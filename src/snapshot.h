/*
 * Copyright (c) 2026-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of (a) the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 *
 *
 * WHAT IS A HYBRID SNAPSHOT?
 * ----------------------
 * A point-in-time snapshot of the keyspace (BGSAVE, full sync, AOF rewrite)
 * that forks a child, as stock does, but keeps the bulk of the keyspace out of
 * the child: small values are serialized by a background thread of the parent
 * instead. Stock pays for the fork twice: the page tables of the whole heap at
 * fork(), and a copy-on-write page for every write the parent makes while the
 * child runs. Typical datasets are many small keys and a few big ones, so the
 * hybrid puts small values where the child cannot see them and serializes them
 * itself; only the big values still come from the child's copy-on-write image.
 * Switched at runtime with `hybrid-snapshot`; design in docs/hybrid-snapshot.md.
 *
 * KEY CLASSES AND THE SNAP_NOFORK ARENA
 * -------------------------------------
 * Every stored value has a class, decided by where its memory IS:
 *
 *   SNAP_NOFORK  a single-allocation value (string, listpack, intset) of at
 *                most `save-small-max-bytes` living in the SNAP_NOFORK arena.
 *                Serialized by the scan thread from the parent's memory.
 *   SNAP_FORK    everything else (multi-allocation encodings, big values).
 *                Serialized by the fork child from its copy-on-write image.
 *
 * The arena is a jemalloc arena over an address range marked MADV_DONTFORK:
 * a fork child does not map it, so it costs nothing at fork() and nothing in
 * copy-on-write. Every kvobj header and every keyspace dict table lives there
 * too, whatever the value's class: the child never touches the parent's dicts.
 * Allocation goes through zmalloc_nofork*() (zmalloc.h); frees and reallocs
 * route by address, so sds, listpack and dict code need not know. Placement
 * is applied on every keyspace insertion and after a value grows or changes
 * encoding (snapPlaceValue): only the value moves, the header never does.
 * Any other fork (module fork, Lua debugger, a stock child while the feature
 * is off) needs the keyspace in the child, so the range is flipped forkable
 * around it (snapArenaBeforeFork / snapArenaChildExited).
 *
 * ONE SAVE, END TO END
 * --------------------
 * 1. Freeze (main thread, the "fork instant"): a guard is installed on every
 *    keyspace store (kvstoreSnapGuardSet), every fork-instant dict gets a
 *    reference and a generation stamp. Nothing is copied.
 * 2. Fork: the child maps the SNAP_FORK values and nothing of the keyspace.
 * 3. Scan (background thread): every frozen dict is read one bucket window
 *    at a time; SNAP_NOFORK keys are serialized into 64 KB buffers, SNAP_FORK
 *    keys are shipped to the child as header copies whose values the child
 *    resolves in its own memory. Two pipes: the small channel of buffers and
 *    the big channel of headers; the child takes the small backlog first.
 * 4. Writes meanwhile (main thread, the write funnel): a write to a frozen
 *    dict is in place once the window has passed the bucket; diverted into the
 *    dict's side table while the window is on it (kvstore.h, KVSTORE OVERLAY);
 *    and ahead of the window the bucket is serialized first (emit-ahead, the
 *    main thread produces it) and then written in place.
 * 5. Completion: as the window moves on, side tables drain into their dicts;
 *    when a dict's scan is complete its overlay is released. The child exits
 *    after END; snapOnChildEnd joins the thread and the stock done handler
 *    runs. Abort (hybrid-snapshot-max-retained-bytes hard mark, FLUSHALL, a
 *    killed child) joins the thread, drains every overlay and leaves the
 *    previous dump untouched.
 *
 * THE BUCKET WINDOW AND WRITE SECTIONS
 * ------------------------------------
 * The scan thread publishes the window (dict, table, [lo, hi)) before it
 * reads it. The main thread writes to frozen buckets only inside a "write
 * section", opened by the first funnel hook of a command and closed when the
 * outermost command returns (also at beforeSleep and when a module thread
 * releases the GIL). The thread waits for an open section to close before
 * reading a new window, so a bucket link obtained inside a section stays
 * valid until the section ends: one seq_cst store-then-load pair each side.
 * A slot (dict) is "owed" to the save until its state is SNAP_SLOT_DONE.
 *
 * MEMORY
 * ------
 * The hybrid keeps three things alive beyond stock: fork-instant originals
 * that were replaced or deleted while owed (retained bytes, bounded by
 * `hybrid-snapshot-max-retained-bytes`, the save aborts above it), records
 * queued for the child (`hybrid-snapshot-max-queued-bytes`, the thread waits
 * above it), and one bit per bucket of every dict the save touched. INFO
 * memory and persistence report all three (snapInfoMemory/Persistence).
 *
 * ENTRY POINTS
 * ------------
 * One decision, snapSaveTryStart(): rdbSaveBackground, the replication full
 * sync (disk or diskless) and rewriteAppendOnlyFileBackground call it; it
 * takes the hybrid path when the switch is on, the request has keyspace
 * content and every loaded module has opted in
 * (REDISMODULE_OPTIONS_HANDLE_HYBRID_SNAPSHOT), else the caller runs the stock
 * child. Functions-only transfers always keep the stock child. An atomic slot
 * migration's slot snapshot is a hybrid save over the migrating slots only:
 * the same scan and child, a RESP record form (RESTORE from DUMP payloads).
 *
 * WHERE TO LOOK
 * -------------
 * snapshot.c: classification and placement; clone-on-write for the overlay;
 * the scan engine (windows, write sections, per-dict state, bitmaps, the
 * thread); the save (queues, channels, stores vs dbs, hard mark); the child
 * writer; the parent orchestration; unit tests (redis-server test snapshot).
 * kvstore.h / kvsoverlay.c: the overlay (frozen dicts, side tables).
 * zmalloc.c: the arena. docs/hybrid-snapshot.md: the design, with figures.
 * All functions here are prefixed "snap".
 */

#ifndef __SNAPSHOT_H
#define __SNAPSHOT_H

#include <stddef.h>
#include "server.h"   /* kvobj, kvstore, listpackEx; server.h includes this header at its end */
#include "sds.h"
#include "intset.h"

/* Key classes: the class of a key's VALUE allocation (headers are always in the arena). */
#define SNAP_NOFORK 0   /* Value in the DONTFORK arena; serialized by the scan thread. */
#define SNAP_FORK   1   /* Value in forkable memory; serialized by the fork child. */

struct listpackEx;
struct aeEventLoop;

/* ---- SNAP_NOFORK arena ---------------------------------------------------- */

/* Create the arena: once, main thread, before the first zmalloc_nofork*(); a no-op without support. */
void snapArenaInit(void);
/* 1 if the arena exists (DONTFORK or currently forkable). */
static inline int snapArenaAvailable(void) { return zmalloc_nofork_arena_index() != UINT_MAX; }
/* Address range reserved for the arena, and the part handed to jemalloc so far (0 if unavailable). */
size_t snapArenaVaReserved(void);
size_t snapArenaVaUsed(void);

/* Fork visibility: DONTFORK for the hybrid child, flipped forkable around any other fork. */
int snapArenaForkable(void);
int snapArenaSetForkable(int forkable);
/* redisFork(), parent side: a non-hybrid child needs the arena mapped. */
void snapArenaBeforeFork(int purpose);
/* After a child of any type was reaped: back to DONTFORK when the feature is on. */
void snapArenaChildExited(void);

/* ---- Classification and placement ---------------------------------------- */

/* Single-allocation encoding that MAY be SNAP_NOFORK (strings, listpacks, intsets). */
int snapEncodingIsNoforkClass(int type, int encoding);
/* Placement policy: the class a value of this encoding and size should have. */
int snapClassify(int type, int encoding, size_t alloc_bytes);
/* Actual class of a stored value, decided by where its memory is. */
int snapKeyClass(const robj *o);
/* Apply the policy to a value: move it into or out of the arena (only the value moves, never the header). */
void snapPlaceValueArena(robj *o);
static inline void snapPlaceValue(robj *o) {
    if (__builtin_expect(zmalloc_nofork_enabled(), 0)) snapPlaceValueArena(o);
}
/* Move a value out of the arena before its buffer is adopted by a multi-allocation value. */
void snapValueDetachFromArena(robj *o);

/* Keyspace placement statistics (DEBUG SNAP-PLACEMENT, SNAP-DRIFT). */
typedef struct snapPlacementStats {
    long long keys;
    long long headers_in_arena;        /* every kvobj header should be here */
    long long headers_outside;
    long long class_nofork;            /* snapKeyClass() == SNAP_NOFORK */
    long long class_fork;
    long long drift_nofork_outside;    /* policy says NOFORK, value outside the arena */
    long long drift_fork_inside;       /* policy says FORK (single-alloc, too big), value inside */
    long long violations;              /* header outside, or multi-allocation value inside */
    long long dict_tables_in_arena;    /* dict structs + bucket arrays of db->keys / db->expires */
    long long dict_tables_outside;
} snapPlacementStats;
void snapPlacementScan(redisDb *db, snapPlacementStats *st);
void snapPlacementDriftReply(struct client *c, redisDb *db, int limit);

/* ---- Per-slot background scan ------------------------------------------- */

/* Slot (dict) states, monotonic; the slot is owed to the save until DONE. */
#define SNAP_SLOT_PENDING  0   /* Not reached by the scan thread yet. */
#define SNAP_SLOT_SCANNING 1   /* Being read by the scan thread. */
#define SNAP_SLOT_SCANNED  2   /* Fully dispatched; waiting for the main thread to complete it. */
#define SNAP_SLOT_DONE     3   /* Completed by the main thread: side table drained, dict unfrozen. */

typedef struct snapScanCtx snapScanCtx;

/* Scan thread, once per fork-instant key of a slot; strictly read-only on the keyspace. */
typedef void (snapScanDispatchFn)(void *privdata, int store_idx, int didx, kvobj *kv);
/* Main thread, when a slot is complete, before its dict is unfrozen ('aborted': not fully scanned). */
typedef void (snapScanSlotDoneFn)(void *privdata, int store_idx, int didx, int aborted);
/* Scan thread, after the last slot or on abort, before the scan is published as finished. */
typedef void (snapScanThreadDoneFn)(void *privdata, int aborted);

/* Create a scan over 'num_stores' kvstores; nothing is frozen or started yet. */
snapScanCtx *snapScanCreate(kvstore **stores, int num_stores, snapScanDispatchFn *dispatch,
                            snapScanSlotDoneFn *slot_done, void *privdata);
/* Before start: the keyspace overlay callbacks (clone, free_orig, enter, prepare, drainable). */
void snapScanSetOverlay(snapScanCtx *ctx, const kvstoreSnapHooks *hooks, void **store_privdata);
/* Before start, cluster mode: freeze and scan only the slots whose bit is set (CLUSTER_SLOTS bits, must outlive the scan). */
void snapScanSetSlotFilter(snapScanCtx *ctx, const uint8_t *bitmap);
/* Optional hooks: scan thread before the first slot / after the last; after every window; main thread when a store's last slot completed. */
void snapScanSetThreadInit(snapScanCtx *ctx, void (*fn)(void *privdata));
void snapScanSetThreadDone(snapScanCtx *ctx, snapScanThreadDoneFn *fn);
void snapScanSetWindowDone(snapScanCtx *ctx, void (*fn)(void *privdata));
void snapScanSetStoreDone(snapScanCtx *ctx, void (*fn)(void *privdata, int s));

/* Main thread: freeze every store (the fork instant) and spawn the thread; C_OK / C_ERR. */
int snapScanStart(snapScanCtx *ctx);
/* The same in two steps: freeze first, spawn the thread later (the fork goes in between). */
void snapScanFreeze(snapScanCtx *ctx);
int snapScanStartThread(snapScanCtx *ctx);
/* Main thread: complete the slots the thread finished since the last call; returns how many. */
int snapScanHandleCompletions(snapScanCtx *ctx);
/* Readable when a slot completes or the thread exits (-1 if none); register the handler on it with ctx as client data. */
int snapScanReadEventFd(snapScanCtx *ctx);
void snapScanAeHandler(struct aeEventLoop *el, int fd, void *privdata, int mask);
/* 1 once the thread has exited (all slots scanned, or aborted). */
int snapScanIsFinished(snapScanCtx *ctx);
/* Main thread: stop and join the thread, complete every slot. */
void snapScanAbort(snapScanCtx *ctx);
/* Main thread: join if needed and free the ctx; every slot must be DONE. */
void snapScanRelease(snapScanCtx *ctx);

/* SNAP_SLOT_* of (store, dict); progress counters for INFO and tests; 1 when called from the scan thread. */
int snapScanSlotState(snapScanCtx *ctx, int store_idx, int didx);
void snapScanProgress(snapScanCtx *ctx, long *slots_total, long *slots_scanned,
                      long *slots_done, long long *keys_dispatched);
int snapOnScanThread(void);

/* ---- Bucket window and write sections ------------------------------------ */

/* Odd while the main thread is inside a write section. */
extern redisAtomic uint64_t snap_funnel_seq;
/* Open the section (main thread, or a module thread holding the GIL): the first funnel hook of a command. */
void snapFunnelOpen(void);
/* Close it: end of the outermost command, beforeSleep, GIL release. */
void snapFunnelCloseSlow(void);
static inline void snapFunnelClose(void) {
    uint64_t v;
    atomicGet(snap_funnel_seq, v);
    if (__builtin_expect(v & 1, 0)) snapFunnelCloseSlow();
}
/* Inside a section: is the published window on the bucket this key maps to. */
int snapWindowCoversKey(dict *d, const void *key);
/* Buckets per published window (DEBUG SNAP-WINDOW-BUCKETS; tests). */
void snapSetWindowBuckets(unsigned long n);
unsigned long snapGetWindowBuckets(void);

/* Per-save bucket bitmaps: a set bit means the main thread already emitted the bucket's chain. */
void snapScanMarkBucketEmitted(snapScanCtx *ctx, int store_idx, int didx, int table, unsigned long idx);
int snapScanBucketEmitted(snapScanCtx *ctx, int store_idx, int didx, int table, unsigned long idx);
/* An attached dict started or finished rehashing: its bitmap follows the new table. */
void snapScanDictRehashStarted(snapScanCtx *ctx, int store_idx, int didx, dict *d);
void snapScanDictRehashCompleted(snapScanCtx *ctx, int store_idx, int didx, dict *d);
/* Bitmap accounting: dicts that grew under the save, dicts retained past their store, bitmap bytes. */
long snapScanDictsGrown(snapScanCtx *ctx);
long snapScanRetainedDicts(snapScanCtx *ctx);
size_t snapScanBitmapBytes(snapScanCtx *ctx);

/* ---- The server keyspace on the scan ------------------------------------- */

/* Overlay callbacks (privdata = the store's snapGuardPriv): clone a kvobj into the arena, release a fork-instant original. */
void *snapOverlayCloneKey(void *privdata, int didx, void *orig, int *value_shared);
void snapOverlayFreeOriginal(void *privdata, int didx, void *orig, int value_shared);

/* The RDB serializer's configuration: live values on the main thread, the save-start copies on the scan thread. */
typedef struct snapRdbCfg {
    int on_scan;                 /* serializing on the scan thread */
    int compression;             /* rdbcompression */
    int maxmemory_policy;
    int apply_key_save_delay;    /* rdb-key-save-delay applies here (stock contexts only) */
} snapRdbCfg;
snapRdbCfg snapRdbCfgGet(void);

/* ---- Hybrid snapshot: the save --------------------------------------------- */

/* The one dispatch decision for rdb.c, replication.c and aof.c: C_OK started, C_ERR taken but failed, SNAP_NOT_TAKEN run the stock child. */
#define SNAP_SAVE_RDB     0   /* BGSAVE or disk-based full sync: a file (filename, rsi, rdbflags) */
#define SNAP_SAVE_SOCKETS 1   /* diskless full sync: the replicas' sockets (rsi) */
#define SNAP_SAVE_AOF     2   /* BGREWRITEAOF: the AOF base file */
#define SNAP_NOT_TAKEN    1
int snapSaveTryStart(int kind, int req, char *filename, rdbSaveInfo *rsi, int rdbflags);
/* The three saves it dispatches to: same contracts as rdbSaveBackground, rdbSaveToSlavesSockets, rewriteAppendOnlyFileBackground. */
int snapSaveBackground(int req, char *filename, rdbSaveInfo *rsi, int rdbflags);
int snapSaveToSlavesSockets(int req, rdbSaveInfo *rsi);
int snapSaveBackgroundAof(void);

/* The runtime switch (hybrid-snapshot) and its CONFIG SET hook (flips the arena, refuses mid-save). */
static inline int snapHybridEnabled(void) { return server.hybrid_snapshot; }
int snapHybridConfigApply(const char **err);
/* A hybrid save is running / this process is its child (no keyspace mapped). */
int snapSaveInProgress(void);
int snapInHybridChild(void);

/* Events during a save: a store the save is scanning; FLUSHDB/SWAPDB hand their dbs to the save. */
int snapSaveOwnsKvstore(kvstore *kvs);
int snapSaveDetachDb(kvstore *keys, kvstore *expires, void *subexpires, dict *stream_idmp_keys, kvstore *blessed);
/* Main thread: stop the scan and complete every slot (not the child, see killRDBChild). */
void snapSaveAbort(void);
/* The one child-end hook, for every child type, before the type's done handler. */
void snapOnChildEnd(int exitcode, int bysignal);
/* serverCron/beforeSleep: act on a pending hard-mark abort. */
void snapSaveCron(void);
/* CONFIG SET rdb-key-save-delay: reaches the running scan thread. */
void snapSaveSetKeySaveDelay(int usec);

/* Owed objects on read paths: a fork-instant original the save has not taken yet (opens the command's write section). */
int snapKvobjIsOwed(redisDb *db, kvobj *kv);
/* A read path about to write 'kv' in place: the object to write (the clone when the key was owed). */
kvobj *snapPrepareMutation(redisDb *db, robj *key, kvobj *kv);
/* Debug-build predicate and assertion at the write funnels. */
int snapKvobjIsFrozenOriginal(redisDb *db, kvobj *kv);
#define snapDebugAssertNotFrozenOriginal(db, kv) \
    debugServerAssert((kv) == NULL || !((robj *)(kv))->iskvobj || !snapKvobjIsFrozenOriginal((db), (kvobj *)(kv)))

/* ---- Observability -------------------------------------------------------- */

/* INFO memory / persistence fields, DEBUG SNAP-* (1 when handled), the INFO sections safe in the hybrid child (else NULL). */
sds snapInfoMemory(sds info);
sds snapInfoPersistence(sds info);
int snapDebugCommand(struct client *c);
const char *const *snapCrashReportInfoSections(void);

/* The save's memory in the parent that stock's child would hold in its own process: excluded from the eviction calculation. */
size_t snapSaveOverheadBytes(void);
size_t snapScanTableBytes(snapScanCtx *ctx);
/* The running save: slots and keys. */
void snapSaveProgress(long *slots_total, long *slots_done, long long *keys_nofork,
                      long long *keys_fork, long long *bytes_sent);
size_t snapSaveKeysProcessed(size_t stock);
/* Retained originals (replaced or tombstoned) and side-table entries; the peak over the last save. */
size_t snapSaveRetainedBytes(void);
long snapSaveSideEntries(void);
size_t snapSaveLastRetainedPeak(void);
/* Bytes queued for the child (main-thread queue plus the thread's outbound queue); the peak over the last save. */
size_t snapSaveQueuedBytes(void);
size_t snapSaveLastQueuedPeak(void);
/* Bitmaps and dicts: bitmap bytes, dicts that grew, fork-instant dicts their store let go of (cumulative). */
size_t snapSaveBitmapBytes(void);
long snapSaveGrownDicts(void);
long snapSaveRetainedDicts(void);
/* Main-thread work: buckets and keys emitted ahead of the window; in-place writes behind it (this save, last save). */
long long snapSaveMainBuckets(void);
long long snapSaveMainKeys(void);
long long snapSaveInplaceWrites(void);
long long snapSaveLastInplaceWrites(void);

/* ---- Tests --------------------------------------------------------------- */
#ifdef REDIS_TEST
int snapshotTest(int argc, char *argv[], int flags);
#endif

#endif /* __SNAPSHOT_H */
