/*
 * Copyright (c) 2026-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of (a) the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 *
 *
 * WHAT IS HYBRID SNAPSHOT?
 * --------------------------
 * A point-in-time snapshot (BGSAVE, full sync, AOF rewrite) that cuts fork()
 * page-table and copy-on-write cost. Unlike the classic fork(), small values 
 * and selected structures live in a dedicated jemalloc arena such that pages are 
 * marked MADV_DONTFORK and are serialized by a background thread of the parent. 
 * The fork child serializes only the big ones. Toggled at runtime with `hybrid-snapshot`.
 *
 * Every value has a class, decided by where its memory is:
 *   SNAP_NOFORK  single-allocation value (string, listpack, intset) of at most
 *                `save-small-max-bytes`, in the arena. Scan thread serializes it.
 *   SNAP_FORK    everything else (multi-allocation encodings, big values), in
 *                forkable memory. The fork child serializes it.
 * Every kvobj header and keyspace dict table is in the arena whatever the
 * value's class, so the child never touches the parent's dicts. Placement is
 * applied on each insertion and when a value grows or changes encoding
 * (snapPlaceValue): only the value moves, never the header. Allocation goes
 * through zmalloc_nofork*(); frees and reallocs route by address.
 * Any other fork (module, Lua debugger, a classic child while the feature is
 * off) needs the keyspace in the child, so the arena is flipped forkable around
 * it (snapArenaBeforeFork / snapArenaChildExited).
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
 *    dict is in place once the window has passed the bucket; made on a
 *    duplicate of the bucket's chain while the window is on it (kvstore.h, KVSTORE OVERLAY);
 *    and ahead of the window the bucket is serialized first (emit-ahead, the
 *    main thread produces it) and then written in place.
 * 5. Completion: as the window moves on, the duplicated buckets are written back into their dicts;
 *    when a dict's scan is complete its overlay is released. The child exits
 *    after END; snapOnChildEnd joins the thread and the classic done handler
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
 * The hybrid keeps three things alive beyond the classic save: fork-instant originals
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
 * (REDISMODULE_OPTIONS_HANDLE_HYBRID_SNAPSHOT), else the caller runs the classic
 * child. Functions-only transfers always keep the classic child. An atomic slot
 * migration's slot snapshot is a hybrid save over the migrating slots only:
 * the same scan and child, a RESP record form (RESTORE from DUMP payloads).
 *
 * WHERE TO LOOK
 * -------------
 * snapshot.c: classification and placement; clone-on-write for the overlay;
 * the scan engine (windows, write sections, per-dict state, bitmaps, the
 * thread); the save (queues, channels, stores vs dbs, hard mark); the child
 * writer; the parent orchestration; unit tests (redis-server test snapshot).
 * kvstore.h / kvsoverlay.c: the overlay (frozen dicts, overlayed buckets).
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

/* Fork visibility: DONTFORK for the hybrid child, flipped forkable around any other fork. */
int snapArenaForkable(void);
/* redisFork(), parent side: a non-hybrid child needs the arena mapped. */
void snapArenaBeforeFork(int purpose);
/* After a child of any type was reaped: back to DONTFORK when the feature is on. */
void snapArenaChildExited(void);

/* ---- Classification and placement ---------------------------------------- */

/* Apply the policy to a value: move it into or out of the arena (only the value moves, never the header). */
void snapPlaceValueArena(robj *o);

static inline void snapPlaceValue(robj *o) {
    if (__builtin_expect(zmalloc_nofork_enabled(), 0)) snapPlaceValueArena(o);
}
/* Move a value out of the arena */
void snapValueDetachFromArena(robj *o);

/* ---- Per-slot background scan ------------------------------------------- */

/* ---- Credit window and write sections ------------------------------------ */

/* Odd while the main thread is inside a write section (it caps the scan thread then; see snapshot.c). */
extern redisAtomic uint64_t snap_funnel_seq;
/* Close it: end of the outermost command, beforeSleep, GIL release. */
void snapFunnelCloseSlow(void);

static inline void snapFunnelClose(void) {
    uint64_t v;
    atomicGet(snap_funnel_seq, v);
    if (__builtin_expect(v & 1, 0)) snapFunnelCloseSlow();
}

/* ---- The server keyspace on the scan ------------------------------------- */

/* The RDB serializer's configuration: live values on the main thread, the save-start copies on the scan thread. */
typedef struct snapRdbCfg {
    int on_scan;                 /* serializing on the scan thread */
    int compression;             /* rdbcompression */
    int maxmemory_policy;
    int apply_key_save_delay;    /* rdb-key-save-delay applies here (classic contexts only) */
} snapRdbCfg;
snapRdbCfg snapRdbCfgGet(void);

/* ---- Hybrid snapshot: the save --------------------------------------------- */

/* The one dispatch decision for rdb.c, replication.c and aof.c: C_OK started, C_ERR taken but failed, SNAP_NOT_TAKEN run the classic child. */
#define SNAP_SAVE_RDB     0   /* BGSAVE or disk-based full sync: a file (filename, rsi, rdbflags) */
#define SNAP_SAVE_SOCKETS 1   /* diskless full sync: the replicas' sockets (rsi) */
#define SNAP_SAVE_AOF     2   /* BGREWRITEAOF: the AOF base file */
#define SNAP_NOT_TAKEN    1
int snapSaveTryStart(int kind, int req, char *filename, rdbSaveInfo *rsi, int rdbflags);

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

/* The save's memory in the parent that the classic child would hold in its own process: excluded from the eviction calculation. */
size_t snapSaveOverheadBytes(void);
size_t snapSaveKeysProcessed(size_t classic);

/* ---- Tests --------------------------------------------------------------- */
#ifdef REDIS_TEST
int snapshotTest(int argc, char *argv[], int flags);
#endif

#endif /* __SNAPSHOT_H */
