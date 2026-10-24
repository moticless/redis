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
 * ------------------------
 * A point-in-time snapshot (BGSAVE, full sync, AOF rewrite) that cuts fork()
 * page-table and copy-on-write cost. Unlike the classic fork(), small values
 * and selected structures live in a dedicated jemalloc arena such that pages are
 * marked MADV_DONTFORK and are serialized by a background thread of the parent.
 * The fork child serializes only the big ones. Toggled at runtime with `hybrid-snapshot`.
 *
 * Every value has a class, decided by where its memory is:
 *   HYBSNAP_NOFORK  single-allocation value (string, listpack, intset) of at most
 *                `save-small-max-bytes`, in the arena. Scan thread serializes it.
 *   HYBSNAP_FORK    everything else (multi-allocation encodings, big values), in
 *                forkable memory. The fork child serializes it.
 * Every kvobj header and keyspace dict table is in the arena whatever the
 * value's class, so the child never touches the parent's dicts. Placement is
 * applied on each insertion and when a value grows or changes encoding
 * (hybsnapPlaceValue): only the value moves, never the header. Allocation goes
 * through zmalloc_nofork*(); frees and reallocs route by address.
 * Any other fork (module, Lua debugger, a classic child while the feature is
 * off) needs the keyspace in the child, so the arena is flipped forkable around
 * it (hybsnapArenaBeforeFork / hybsnapArenaChildExited); a module fork that declares
 * REDISMODULE_FORK_NO_KEYSPACE is forked like the hybrid child instead
 * (hybsnapForkNoKeyspace).
 *
 * ONE SAVE, END TO END
 * --------------------
 * 1. Freeze (main thread, the "fork instant"): a guard is installed on every
 *    keyspace store (kvstoreSnapGuardSet), every fork-instant dict gets a
 *    reference and a generation stamp. Nothing is copied.
 * 2. Fork: the child maps the HYBSNAP_FORK values and nothing of the keyspace.
 * 3. Scan (background thread): the frozen dicts are read bucket by bucket in
 *    a fixed order, below a limit the main thread sets (the credit window,
 *    below). HYBSNAP_NOFORK keys are serialized into 64 KB buffers, HYBSNAP_FORK
 *    keys are shipped to the child as header copies whose values the child
 *    resolves in its own memory. Two pipes: the small channel of buffers and
 *    the big channel of headers; the child drains the small backlog first.
 * 4. Writes meanwhile (main thread, the write funnel): what a write to a
 *    frozen dict does depends on where the scan thread is (below).
 * 5. Completion: buckets the thread has passed are written back as it moves
 *    on; when a dict's scan is complete its overlay is released and the dict
 *    unfrozen. The child exits after END; hybsnapOnChildEnd joins the thread and
 *    the classic done handler runs. Abort (hybrid-snapshot-max-retained-bytes
 *    hard mark, FLUSHALL, a killed child) joins the thread, drains every
 *    overlay and leaves the previous dump untouched.
 *
 * THE CREDIT WINDOW AND WRITE SECTIONS
 * ------------------------------------
 * Every bucket has a position in the scan order (store, dict, table, bucket
 * as one integer). The scan thread publishes lo (every position below it is
 * read) and announces in cur the batch it reads next; it never reads at or
 * past hi, which only the main thread moves.
 *
 * The main thread touches frozen dicts only inside a "write section": opened
 * by the first guard or funnel hook of a command, closed when the outermost
 * command returns, at beforeSleep and when a module thread releases the GIL.
 * Outside sections hi is unlimited and the thread runs free. Opening a
 * section caps hi about W buckets past lo (default 1024, DEBUG
 * SNAP-WINDOW-BUCKETS); while it stays open the main thread raises hi each
 * time lo passes half the grant, unless many overlay windows are in use;
 * closing it writes back the overlayed buckets below lo and lifts the cap.
 * Inside a section a bucket the main thread writes is:
 *   behind  below lo: written in place;
 *   inside  in [lo, hi): the thread may be reading it, so the write goes to
 *           a duplicate of the bucket's chain (kvstore.h, KVSTORE OVERLAY);
 *   ahead   hi and above: serialized by the main thread first (emit-ahead,
 *           marked so the thread skips it), then written in place.
 * The thread waits only when it reaches hi during a long section, and once at
 * the end for an open section to close; the main thread never waits. The cap
 * is a seq_cst store-load pair on each side (thread: store cur, load hi;
 * main: store hi, load cur): either the main thread sees where the thread is,
 * or the thread sees the cap. A slot (dict) is "owed" to the save until its
 * state is HYBSNAP_SLOT_DONE. The full argument: docs/hybrid-snapshot.md
 * section 3; the encoding and the code: hybrid_snapshot.c, "Scan positions".
 *
 * MEMORY
 * ------
 * The hybrid keeps three things alive beyond the classic save: fork-instant originals
 * that were replaced or deleted while owed (retained bytes, bounded by
 * `hybrid-snapshot-max-retained-bytes`, the save aborts above it), records
 * queued for the child (`hybrid-snapshot-max-queued-bytes`, the thread waits
 * above it), and one bit per bucket of every dict the save touched. INFO
 * memory and persistence report all three (hybsnapInfoMemory/Persistence).
 *
 * ENTRY POINTS
 * ------------
 * One decision, hybsnapSaveTryStart(): rdbSaveBackground, the replication full
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
 * hybrid_snapshot.c: the implementation (its head lists its sections) and the unit
 * tests (redis-server test snapshot). kvstore.h, KVSTORE OVERLAY: the overlay
 * contract (frozen dicts, overlayed buckets); kvsoverlay.h / kvsoverlay.c: its
 * implementation.
 * zmalloc.c: the arena. docs/hybrid-snapshot.md: the design, with figures.
 * All functions here are prefixed "snap".
 */

#ifndef __HYBRID_SNAPSHOT_H
#define __HYBRID_SNAPSHOT_H

#include <stddef.h>
#include "server.h"   /* kvobj, kvstore, listpackEx; server.h includes this header at its end */
#include "sds.h"
#include "intset.h"

/* Key classes: the class of a key's VALUE allocation (headers are always in the arena). */
#define HYBSNAP_NOFORK 0   /* Value in the DONTFORK arena; serialized by the scan thread. */
#define HYBSNAP_FORK   1   /* Value in forkable memory; serialized by the fork child. */

struct listpackEx;
struct aeEventLoop;

/* ---- HYBSNAP_NOFORK arena ---------------------------------------------------- */

/* Create the arena: once, main thread, before the first zmalloc_nofork*(); a no-op without support. */
void hybsnapArenaInit(void);

/* 1 if the arena exists (DONTFORK or currently forkable). */
static inline int hybsnapArenaAvailable(void) { return zmalloc_nofork_arena_index() != UINT_MAX; }

/* Fork visibility: DONTFORK for a child forked without the keyspace, flipped forkable around any other fork. */
int hybsnapArenaForkable(void);
/* redisFork(), parent side: a child that walks the keyspace needs the arena mapped. */
void hybsnapArenaBeforeFork(int purpose);
/* After a child of any type was reaped: back to DONTFORK when the feature is on. */
void hybsnapArenaChildExited(void);
/* redisFork() for a child that never touches the keyspace (the hybrid save child, a
 * REDISMODULE_FORK_NO_KEYSPACE module fork): the arena stays unmapped in it. */
int hybsnapForkNoKeyspace(int purpose);
/* This process is such a child: the keyspace module APIs are refused, the crash report skips the keyspace. */
int hybsnapChildNoKeyspace(void);

/* ---- Classification and placement ---------------------------------------- */

/* Apply the policy to a value: move it into or out of the arena (only the value moves, never the header). */
void hybsnapPlaceValueArena(robj *o);

static inline void hybsnapPlaceValue(robj *o) {
    if (unlikely(zmalloc_nofork_enabled())) hybsnapPlaceValueArena(o);
}
/* Move a value out of the arena */
void hybsnapValueDetachFromArena(robj *o);

/* ---- Credit window and write sections ------------------------------------ */

/* Odd while the main thread is inside a write section (see THE CREDIT WINDOW AND WRITE SECTIONS above). */
extern redisAtomic uint64_t hybsnap_funnel_seq;
/* Close it: end of the outermost command, beforeSleep, GIL release. */
void hybsnapFunnelCloseSlow(void);

static inline void hybsnapFunnelClose(void) {
    uint64_t v;
    atomicGet(hybsnap_funnel_seq, v);
    if (unlikely(v & 1)) hybsnapFunnelCloseSlow();
}

/* ---- The server keyspace on the scan ------------------------------------- */

/* The RDB serializer's configuration: live values on the main thread, the save-start copies on the scan thread. */
typedef struct hybsnapRdbCfg {
    int on_scan;                 /* serializing on the scan thread */
    int compression;             /* rdbcompression */
    int maxmemory_policy;
    int apply_key_save_delay;    /* rdb-key-save-delay applies here (classic contexts only) */
} hybsnapRdbCfg;
hybsnapRdbCfg hybsnapRdbCfgGet(void);

/* ---- Hybrid snapshot: the save --------------------------------------------- */

/* The one dispatch decision for rdb.c, replication.c and aof.c: C_OK started, C_ERR taken but failed, HYBSNAP_NOT_TAKEN run the classic child. */
#define HYBSNAP_SAVE_RDB     0   /* BGSAVE or disk-based full sync: a file (filename, rsi, rdbflags) */
#define HYBSNAP_SAVE_SOCKETS 1   /* diskless full sync: the replicas' sockets (rsi) */
#define HYBSNAP_SAVE_AOF     2   /* BGREWRITEAOF: the AOF base file */
#define HYBSNAP_NOT_TAKEN    1
int hybsnapSaveTryStart(int kind, int req, char *filename, rdbSaveInfo *rsi, int rdbflags);

static inline int hybsnapHybridEnabled(void) { return server.hybrid_snapshot; }

int hybsnapHybridConfigApply(const char **err);

/* A hybrid save is running. */
int hybsnapSaveInProgress(void);

/* Events during a save: a store the save is scanning; FLUSHDB/SWAPDB hand their dbs to the save. */
int hybsnapSaveOwnsKvstore(kvstore *kvs);

int hybsnapSaveDetachDb(kvstore *keys, kvstore *expires, void *subexpires, dict *stream_idmp_keys, kvstore *blessed);
/* Main thread: stop the scan and complete every slot (not the child, see killRDBChild). */
void hybsnapSaveAbort(void);
/* The one child-end hook, for every child type, before the type's done handler. */
void hybsnapOnChildEnd(int exitcode, int bysignal);
/* serverCron/beforeSleep: act on a pending hard-mark abort. */
void hybsnapSaveCron(void);
/* CONFIG SET rdb-key-save-delay: reaches the running scan thread. */
void hybsnapSaveSetKeySaveDelay(int usec);

/* Owed objects on read paths: a fork-instant original the save has not taken yet (opens the command's write section). */
int hybsnapKvobjIsOwed(redisDb *db, kvobj *kv);
/* A read path about to write 'kv' in place: the object to write (the clone when the key was owed). */
kvobj *hybsnapPrepareMutation(redisDb *db, robj *key, kvobj *kv);
/* Debug-build predicate and assertion at the write funnels. */
int hybsnapKvobjIsFrozenOriginal(redisDb *db, kvobj *kv);
#define hybsnapDebugAssertNotFrozenOriginal(db, kv) \
    debugServerAssert((kv) == NULL || !((robj *)(kv))->iskvobj || !hybsnapKvobjIsFrozenOriginal((db), (kvobj *)(kv)))

/* ---- Observability -------------------------------------------------------- */

/* INFO memory / persistence fields, DEBUG SNAP-* (1 when handled), the INFO sections safe in the hybrid child (else NULL). */
sds hybsnapInfoMemory(sds info);
sds hybsnapInfoPersistence(sds info);
int hybsnapDebugCommand(struct client *c);
const char *const *hybsnapCrashReportInfoSections(void);

/* The save's memory in the parent that the classic child would hold in its own process: excluded from the eviction calculation. */
size_t hybsnapSaveOverheadBytes(void);
size_t hybsnapSaveKeysProcessed(size_t classic);

/* ---- Tests --------------------------------------------------------------- */
#ifdef REDIS_TEST
int hybsnapTest(int argc, char *argv[], int flags);
#endif

#endif /* __HYBRID_SNAPSHOT_H */
