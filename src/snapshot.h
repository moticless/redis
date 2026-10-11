/*
 * Copyright (c) 2026-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of (a) the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 *
 *
 * WHAT IS HYBRID BGSAVE?
 * ----------------------
 * A point-in-time snapshot of the keyspace (BGSAVE, full sync, AOF rewrite)
 * that forks a child, as stock does, but keeps the bulk of the keyspace out of
 * the child: small values are serialized by a background thread of the parent
 * instead. Stock pays for the fork twice: the page tables of the whole heap at
 * fork(), and a copy-on-write page for every write the parent makes while the
 * child runs. Typical datasets are many small keys and a few big ones, so the
 * hybrid puts small values where the child cannot see them and serializes them
 * itself; only the big values still come from the child's copy-on-write image.
 * Switched at runtime with `bgsave-hybrid`; design in docs/hybrid-bgsave.md.
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
 *    runs. Abort (bgsave-hybrid-max-retained-bytes hard mark, FLUSHALL, a
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
 * `bgsave-hybrid-max-retained-bytes`, the save aborts above it), records
 * queued for the child (`bgsave-hybrid-max-queued-bytes`, the thread waits
 * above it), and one bit per bucket of every dict the save touched. INFO
 * memory and persistence report all three (snapInfoMemory/Persistence).
 *
 * ENTRY POINTS
 * ------------
 * One decision, snapSaveTryStart(): rdbSaveBackground, the replication full
 * sync (disk or diskless) and rewriteAppendOnlyFileBackground call it; it
 * takes the hybrid path when the switch is on, the request has keyspace
 * content and every loaded module has opted in
 * (REDISMODULE_OPTIONS_HANDLE_HYBRID_BGSAVE), else the caller runs the stock
 * child. Functions-only and slot-snapshot transfers always keep the stock child.
 *
 * WHERE TO LOOK
 * -------------
 * snapshot.c: classification and placement; clone-on-write for the overlay;
 * the scan engine (windows, write sections, per-dict state, bitmaps, the
 * thread); the save (queues, channels, stores vs dbs, hard mark); the child
 * writer; the parent orchestration; unit tests (redis-server test snapshot).
 * kvstore.h / kvsoverlay.c: the overlay (frozen dicts, side tables).
 * zmalloc.c: the arena. docs/hybrid-bgsave.md: the design, with figures.
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

/* ---- SNAP_NOFORK arena lifecycle ---------------------------------------- */

void snapArenaInit(void);                       /* once, main thread, before the first zmalloc_nofork*(); no-op without support */
static inline int snapArenaAvailable(void) { return zmalloc_nofork_arena_index() != UINT_MAX; }   /* the arena exists */
int snapArenaForkable(void);                    /* 1 while a fork child would map the arena */
int snapArenaSetForkable(int forkable);         /* MADV_DOFORK (1) / MADV_DONTFORK (0) on the range; 1 on success */
void snapArenaBeforeFork(int purpose);          /* redisFork(), parent: a non-hybrid child needs the arena mapped */
void snapArenaChildExited(void);                /* after a child of any type was reaped: back to DONTFORK when due */
size_t snapArenaVaReserved(void);               /* bytes of address range reserved for the arena (0 if unavailable) */
size_t snapArenaVaUsed(void);                   /* bytes of that range handed to jemalloc so far */

/* ---- Classification and placement ---------------------------------------- */

int snapEncodingIsNoforkClass(int type, int encoding);         /* single-allocation encoding that MAY be SNAP_NOFORK */
int snapClassify(int type, int encoding, size_t alloc_bytes);  /* placement policy: the class a value of this size should have */
int snapKeyClass(const robj *o);                               /* actual class of a stored value, by where its memory is */
void snapPlaceValueArena(robj *o);                             /* apply the policy: move the value into or out of the arena */
static inline void snapPlaceValue(robj *o) {
    if (__builtin_expect(zmalloc_nofork_enabled(), 0)) snapPlaceValueArena(o);
}
void snapValueDetachFromArena(robj *o);                        /* before a buffer is adopted by a multi-allocation value */

/* Keyspace placement statistics (DEBUG SNAP-PLACEMENT). */
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
void snapPlacementDriftReply(struct client *c, redisDb *db, int limit);   /* DEBUG SNAP-DRIFT: keys whose placement disagrees with their class */

/* ---- Per-slot background scan ------------------------------------------- */

/* Slot (dict) states, monotonic; the slot is owed to the save until DONE. */
#define SNAP_SLOT_PENDING  0   /* Not reached by the scan thread yet. */
#define SNAP_SLOT_SCANNING 1   /* Being read by the scan thread. */
#define SNAP_SLOT_SCANNED  2   /* Fully dispatched; waiting for the main thread to complete it. */
#define SNAP_SLOT_DONE     3   /* Completed by the main thread: side table drained, dict unfrozen. */

typedef struct snapScanCtx snapScanCtx;
struct aeEventLoop;

typedef void (snapScanDispatchFn)(void *privdata, int store_idx, int didx, kvobj *kv);    /* scan thread, per fork-instant key; read-only */
typedef void (snapScanSlotDoneFn)(void *privdata, int store_idx, int didx, int aborted);  /* main thread, before the slot's dict is unfrozen */
typedef void (snapScanThreadDoneFn)(void *privdata, int aborted);                          /* scan thread, after the last slot or on abort */

snapScanCtx *snapScanCreate(kvstore **stores, int num_stores, snapScanDispatchFn *dispatch,
                            snapScanSlotDoneFn *slot_done, void *privdata);   /* nothing frozen or started yet */
void snapScanSetOverlay(snapScanCtx *ctx, const kvstoreSnapHooks *hooks, void **store_privdata);   /* before start: the overlay callbacks */
void snapScanSetThreadInit(snapScanCtx *ctx, void (*fn)(void *privdata));      /* optional: scan thread, before the first slot */
void snapScanSetThreadDone(snapScanCtx *ctx, snapScanThreadDoneFn *fn);         /* optional: scan thread, before it publishes finished */
void snapScanSetWindowDone(snapScanCtx *ctx, void (*fn)(void *privdata));      /* optional: scan thread, after every window (queue drain) */
void snapScanSetStoreDone(snapScanCtx *ctx, void (*fn)(void *privdata, int s)); /* optional: main thread, a store's last slot completed */
int snapScanStart(snapScanCtx *ctx);              /* main thread: freeze every store and spawn the thread; C_OK / C_ERR */
void snapScanFreeze(snapScanCtx *ctx);            /* two-step start, first half: the fork instant */
int snapScanStartThread(snapScanCtx *ctx);        /* two-step start, second half */
int snapScanHandleCompletions(snapScanCtx *ctx);  /* main thread: complete the slots the thread finished; returns how many */
int snapScanIsFinished(snapScanCtx *ctx);         /* the thread has exited (all slots scanned, or aborted) */
void snapScanAbort(snapScanCtx *ctx);             /* main thread: stop and join the thread, complete every slot */
void snapScanRelease(snapScanCtx *ctx);           /* main thread: join if needed and free; every slot must be DONE */
int snapScanReadEventFd(snapScanCtx *ctx);        /* readable when a slot completes or the thread exits; -1 if none */
void snapScanAeHandler(struct aeEventLoop *el, int fd, void *privdata, int mask);   /* register on that fd with ctx as client data */
int snapScanSlotState(snapScanCtx *ctx, int store_idx, int didx);   /* SNAP_SLOT_* of (store, dict) */
void snapScanProgress(snapScanCtx *ctx, long *slots_total, long *slots_scanned,
                      long *slots_done, long long *keys_dispatched);   /* for INFO and tests */
int snapOnScanThread(void);                       /* 1 when called from the scan thread */

/* ---- Bucket window and write sections ------------------------------------ */

extern redisAtomic uint64_t snap_funnel_seq;    /* odd: the main thread is inside a write section */
void snapFunnelOpen(void);                      /* main thread (or a module thread holding the GIL): open the section */
void snapFunnelCloseSlow(void);
static inline void snapFunnelClose(void) {      /* end of the outermost command, beforeSleep, GIL release */
    uint64_t v;
    atomicGet(snap_funnel_seq, v);
    if (__builtin_expect(v & 1, 0)) snapFunnelCloseSlow();
}
int snapWindowCoversKey(dict *d, const void *key);   /* inside a section: is the published window on this key's bucket */
void snapSetWindowBuckets(unsigned long n);          /* buckets per window (DEBUG SNAP-WINDOW-BUCKETS; tests) */
unsigned long snapGetWindowBuckets(void);

/* Per-save bucket bitmaps: a set bit means the main thread already emitted the bucket's chain. */
void snapScanMarkBucketEmitted(snapScanCtx *ctx, int store_idx, int didx, int table, unsigned long idx);  /* ahead of the window, inside a section */
int snapScanBucketEmitted(snapScanCtx *ctx, int store_idx, int didx, int table, unsigned long idx);
void snapScanDictRehashStarted(snapScanCtx *ctx, int store_idx, int didx, dict *d);     /* the bitmap follows the new table */
void snapScanDictRehashCompleted(snapScanCtx *ctx, int store_idx, int didx, dict *d);
long snapScanDictsGrown(snapScanCtx *ctx);
long snapScanRetainedDicts(snapScanCtx *ctx);
size_t snapScanBitmapBytes(snapScanCtx *ctx);

/* Overlay callbacks for the server keyspace (privdata = the store's snapGuardPriv). */
void *snapOverlayCloneKey(void *privdata, int didx, void *orig, int *value_shared);   /* clone of a kvobj into the arena; NOFORK values deep-copied */
void snapOverlayFreeOriginal(void *privdata, int didx, void *orig, int value_shared); /* release a fork-instant original after the drain */

/* The RDB serializer's configuration: live values on the main thread, the save-start copies on the scan thread. */
typedef struct snapRdbCfg {
    int on_scan;                 /* serializing on the scan thread */
    int compression;             /* rdbcompression */
    int maxmemory_policy;
    int apply_key_save_delay;    /* rdb-key-save-delay applies here (stock contexts only) */
} snapRdbCfg;
snapRdbCfg snapRdbCfgGet(void);

/* ---- Hybrid BGSAVE ------------------------------------------------------- */

/* snapSaveTryStart(): C_OK started, C_ERR taken but failed, SNAP_NOT_TAKEN run the stock child. */
#define SNAP_SAVE_RDB     0   /* BGSAVE or disk-based full sync: a file (filename, rsi, rdbflags) */
#define SNAP_SAVE_SOCKETS 1   /* diskless full sync: the replicas' sockets (rsi) */
#define SNAP_SAVE_AOF     2   /* BGREWRITEAOF: the AOF base file */
#define SNAP_NOT_TAKEN    1
int snapSaveTryStart(int kind, int req, char *filename, rdbSaveInfo *rsi, int rdbflags);   /* the one dispatch decision */
int snapSaveBackground(int req, char *filename, rdbSaveInfo *rsi, int rdbflags);   /* same contract as rdbSaveBackground() */
int snapSaveToSlavesSockets(int req, rdbSaveInfo *rsi);   /* diskless full sync through the hybrid child */
int snapSaveBackgroundAof(void);  /* BGREWRITEAOF: RDB base with the preamble, AOF command form without */
int snapSaveInProgress(void);
int snapInHybridChild(void);      /* 1 in the hybrid save child (no keyspace mapped) */
static inline int snapHybridEnabled(void) { return server.bgsave_hybrid; }   /* the runtime switch (saves take the hybrid path) */
int snapHybridConfigApply(const char **err);   /* CONFIG SET bgsave-hybrid: flips the arena, refuses mid-save */

/* INFO, DEBUG and the crash report: server.c and debug.c carry no field of the feature themselves. */
sds snapInfoMemory(sds info);                     /* appends the arena fields to INFO memory */
sds snapInfoPersistence(sds info);                /* appends the save fields to INFO persistence */
size_t snapSaveKeysProcessed(size_t stock);       /* current_save_keys_processed during a hybrid save */
int snapDebugCommand(struct client *c);           /* DEBUG SNAP-*: 1 when handled */
const char *const *snapCrashReportInfoSections(void);   /* in the hybrid child: the INFO sections that are safe; else NULL */

/* Keyspace events during a save. */
int snapSaveOwnsKvstore(kvstore *kvs);            /* a store the save is scanning */
int snapSaveDetachDb(kvstore *keys, kvstore *expires, void *subexpires, dict *stream_idmp_keys, kvstore *blessed);   /* FLUSHDB/SWAPDB: hand the dbs to the save */
void snapSaveAbort(void);                         /* main thread: stop the scan, complete every slot; not the child */
void snapOnChildEnd(int exitcode, int bysignal);  /* the one child-end hook, before the type's done handler */
void snapSaveCron(void);                          /* serverCron/beforeSleep: act on a pending hard-mark abort */
void snapSaveSetKeySaveDelay(int usec);           /* CONFIG SET rdb-key-save-delay: reaches the running thread */

/* Owed objects on read paths (the predicate opens the command's write section). */
int snapKvobjIsOwed(redisDb *db, kvobj *kv);                      /* a fork-instant original the save has not taken yet */
kvobj *snapPrepareMutation(redisDb *db, robj *key, kvobj *kv);    /* a read path about to write kv in place: the object to write */
int snapKvobjIsFrozenOriginal(redisDb *db, kvobj *kv);            /* debug-build predicate at the write funnels */
#define snapDebugAssertNotFrozenOriginal(db, kv) \
    debugServerAssert((kv) == NULL || !((robj *)(kv))->iskvobj || !snapKvobjIsFrozenOriginal((db), (kvobj *)(kv)))

/* Statistics of the running save (INFO, DEBUG, tests). */
size_t snapSaveRetainedBytes(void);      /* originals the overlay keeps alive (replaced or tombstoned) */
size_t snapSaveLastRetainedPeak(void);   /* peak of the last completed save */
long snapSaveSideEntries(void);          /* clones, adds and tombstones waiting in side tables */
size_t snapSaveQueuedBytes(void);        /* main-thread queue plus the scan thread's outbound queue */
size_t snapSaveLastQueuedPeak(void);     /* peak of the two queues over the last save */
size_t snapSaveBitmapBytes(void);
long snapSaveGrownDicts(void);
long snapSaveRetainedDicts(void);        /* fork-instant dicts their store let go of before the save completed them; cumulative */
long long snapSaveMainBuckets(void);     /* buckets the main thread emitted itself (emit-ahead) */
long long snapSaveMainKeys(void);
long long snapSaveInplaceWrites(void);
long long snapSaveLastInplaceWrites(void);
void snapSaveProgress(long *slots_total, long *slots_done, long long *keys_nofork,
                      long long *keys_fork, long long *bytes_sent);

/* ---- Tests --------------------------------------------------------------- */
#ifdef REDIS_TEST
int snapshotTest(int argc, char *argv[], int flags);
#endif

#endif /* __SNAPSHOT_H */
