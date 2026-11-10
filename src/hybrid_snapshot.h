/*
 * Copyright (c) 2026-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of (a) the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 *
 *
 * WHAT IS HYBRID SNAPSHOT
 * -----------------------
 * A classic save forks the whole process: every page table is mapped at
 * fork() and every page the parent then writes triggers CoW. A hybrid
 * save (`hybrid-snapshot`, switchable at runtime; BGSAVE, full sync, AOF
 * rewrite and slot migration) keeps most of the keyspace out of the child.
 * Small values held in one allocation (string, listpack, intset), the kvobj
 * headers and the kvstore itself live in a dedicated jemalloc arena marked
 * MADV_DONTFORK, which the child never maps.
 *
 * The work splits by value: a background thread of the parent walks the
 * kvstore, serializes the small values itself and forwards serialization to
 * the child. For each big value it sends the child a copy of the key's header
 * and the child serializes the value from its own fork-instant memory. Fork
 * cost and copy-on-write follow the big values only.
 *
 *
 * HYBRID SNAPSHOT FLOW
 * --------------------
 * Without copy-on-write the parent must keep what the thread has not read yet
 * unchanged while commands keep writing. One save, step by step:
 *
 *   1. Freeze (the fork instant). The main thread installs its hooks on the
 *      keyspace stores (kvstoreSnapGuardSet), takes a reference to every
 *      dict, stamps it with the save's generation and bumps the save epoch.
 *      Nothing is copied.
 *
 *   2. Fork. The child maps the FORK (big) values only; the hybrid arena is
 *      MADV_DONTFORK.
 *
 *   3. Scan. The scan background thread reads buckets in a fixed order. Every
 *      bucket has a position (db, dict, table, bucket, packed as one integer);
 *      the thread publishes lo, every position below it is read, and never
 *      reads at or past a limit hi the main thread sets. NOFORK keys go out in
 *      64 KB buffers (small channel), FORK keys as header copies (big channel);
 *      the child drains the small one first.
 *
 *   4. Write during the scan. The main thread touches the keyspace only
 *      inside a write section: opened by a command's first access to a frozen
 *      dict, closed when the outermost command returns, at beforeSleep and
 *      when a module thread releases the GIL. Opening one caps hi about W
 *      buckets past lo (default 1024, DEBUG SNAP-WINDOW-BUCKETS); hi moves
 *      again only between the statements of EXEC or a script, never in the
 *      middle of a command; outside sections the thread runs free. The zone of
 *      the written bucket decides the write:
 *        behind  below lo      already read: ordinary in-place write.
 *        inside  in [lo, hi)   the thread may be reading it: the write goes to
 *                              a private copy of the bucket (overlay),
 *                              installed once lo passes it; the originals stay
 *                              for the thread (kvstore_overlay.h).
 *        ahead   hi and above  not read yet: the main thread emits the key
 *                              first (NOFORK: DUMP to buffer; FORK: header
 *                              copy), stamps it with the save epoch and writes
 *                              in place; the thread skips stamped keys.
 *      So every fork-instant key is written exactly once, as at a classic
 *      fork, and the main thread never waits. The thread waits only when it
 *      reaches hi during a long section, and once at the end for an open
 *      section to close. Encoding, grants and memory ordering:
 *      hybrid_snapshot.c, "Scan positions".
 *
 *   5. Complete. A dict the thread has read gets its overlay written back and
 *      is released; the child exits after END and hybsnapOnChildEnd joins the
 *      thread.
 *
 *   6. Abort. Retained-bytes hard mark, FLUSHALL or a killed child: the
 *      thread is joined, every overlay drained, the previous dump kept.
 *
 * Memory during a save: the fork-instant originals replaced or deleted while
 * still pending (`hybrid-snapshot-max-retained-bytes`, the save aborts above
 * it) and the records queued for the child (`hybrid-snapshot-max-queued-bytes`,
 * the thread waits above it); INFO memory and persistence report both.
 * Always: up to 4 bytes per key for the save epoch (kvobjGetEpoch,
 * `hybrid-snapshot-epoch-prealloc`).
 *
 * OTHER FORKS
 * -----------
 * A module fork, the Lua debugger or a classic child needs the keyspace, so
 * the arena is made forkable around it (hybsnapArenaBeforeFork,
 * hybsnapArenaChildExited); a module fork that declares
 * REDISMODULE_FORK_NO_KEYSPACE is forked like the hybrid child (redisFork()
 * with REDIS_FORK_NO_KEYSPACE).
 *
 * ENTRY POINTS
 * ------------
 * Usage: if (hybsnapSaveEligible(req))
 *           rdbSaveBackground()                 // opt1
 *           rdbSaveToSlavesSockets()            // opt2
 *           rewriteAppendOnlyFileBackground()   // opt3
 *
 * The check answers yes when the switch is on, the request has keyspace
 * content, the databases fit the scan positions (HYBSNAP_POS_ORD_BITS) and
 * every module has opted in (REDISMODULE_OPTIONS_HANDLE_HYBRID_SNAPSHOT); else
 * the caller runs the classic child, as it always does for functions-only
 * transfers. An atomic slot migration's slot snapshot is a hybrid save over the
 * migrating slots only, with RESTORE commands from DUMP payloads as records.
 *
 * RELEATED FILES
 * --------------
 * hybrid_snapshot.c   the implementation (its head lists its sections) and the
 *                     unit tests (redis-server test hybrid_snapshot)
 * kvstore_overlay.h   the overlay: its contract and design; kvstore_overlay.c
 *                     its implementation
 * zmalloc.c           the arena
 *
 * Functions here are prefixed "hybsnap".
 */

#ifndef __HYBRID_SNAPSHOT_H
#define __HYBRID_SNAPSHOT_H

#include <stddef.h>
#include "server.h"   /* kvobj, kvstore, listpackEx; server.h includes this header at its end */
#include "sds.h"
#include "intset.h"

typedef enum hybsnapValueClass {
    HYBSNAP_NOFORK = 0,   /* Value in the hybrid arena - serialized by the scan thread. */
    HYBSNAP_FORK   = 1,   /* Value in forkable memory  - serialized by the fork child. */
} hybsnapValueClass;

/* ---- hybrid arena (can be toggled forkable or not) ----------------------- */

/* If supported create the arena: once, main thread, before the first zmalloc_hybrid*() */
void hybsnapArenaInit(void);

/* 1 if the arena exists (DONTFORK or currently forkable). */
static inline int hybsnapArenaExists(void) { return zmalloc_hybrid_arena_index() != UINT_MAX; }

/* 1 if hybrid arena exists and set to MADV_DOFORK */
int hybsnapArenaIsForkable(void);

/* 1 when a fork child gets the entire keyspace in its address space */
static inline int hybsnapForkSeesKeyspace(void) {  return !hybsnapArenaExists() || hybsnapArenaIsForkable(); }

/* redisFork(), parent side: a child that walks the keyspace needs the arena mapped. */
void hybsnapArenaBeforeFork(int purpose, int flags);

/* After a child of any type ended: back arena to DONTFORK when the feature is on. */
void hybsnapArenaChildExited(void);

/* On redisFork(,+REDIS_FORK_NO_KEYSPACE) hybrid arena is unmapped: its range is reserved. */
void hybsnapChildNoKeyspaceInit(void);

/* Is this process is a child and keyspace is unavailable (hybrid arena unmapped) */
int hybsnapIsChildNoKeyspace(void);

/* ---- Classification and placement ---------------------------------------- */

/* Apply the policy to a value: move it into or out of the arena (only the value moves, never the header). */
void hybsnapPlaceValueArena(robj *o);

/* move a value into the arena when hybsnapClassify() says HYBSNAP_NOFORK */
static inline void hybsnapPlaceValue(robj *o) {
    if (unlikely(zmalloc_hybrid_arena_enabled())) hybsnapPlaceValueArena(o);
}

/* As obj value increase, move it to "common" arena to make it avail to child */
void hybsnapValueDetachFromArena(robj *o);

/* ---- Save epochs (object.h kvobjGetEpoch) ---------------------------------- */

/* Which kvobjs get a save epoch when they are created (kvobjCreate*): every
 * one while a save runs (a key born after the fork instant must be skipped by
 * the scan), else per hybrid-snapshot-epoch-prealloc: every one (ALWAYS) or
 * those whose allocation has 4 free bytes anyway (IF_FREE); none while the
 * feature is off. New kvobjs are stamped with hybsnap_epoch_stamp: the running
 * save's epoch, else the last save's (older than the next one). Main thread. */
typedef enum hybsnapEpochMode {
    HYBSNAP_EPOCH_NONE    = 0,
    HYBSNAP_EPOCH_IF_FREE = 1,
    HYBSNAP_EPOCH_ALWAYS  = 2,
} hybsnapEpochMode;
extern int hybsnap_epoch_mode;
extern uint32_t hybsnap_epoch_stamp;

/* ---- Credit window and write sections ------------------------------------ */

/* Between two statements of EXEC or a script (no link or kvobj pointer of the
 * outer command is live): grant the thread more of the dict if it is half way
 * through its grant. Never called in the middle of a command. A no-op outside
 * a section. */
void hybsnapSectionGrant(void);

/* End the open write section, if any: write back the overlayed buckets the
 * thread has passed and lift the cap. Called at the end of the outermost
 * command, in beforeSleep, and when a module thread releases the GIL. A no-op
 * outside a section: one relaxed load. */
void hybsnapSectionClose(void);

/* ---- The server keyspace on the scan ------------------------------------- */

/* The RDB serializer's configuration: live values on the main thread, the save-start copies on the scan thread. */
typedef struct hybsnapRdbCfg {
    int on_scan;                 /* serializing on the scan thread */
    int compression;             /* rdbcompression */
    int maxmemory_policy;
    int apply_key_save_delay;    /* rdb-key-save-delay applies here (classic contexts only) */
} hybsnapRdbCfg;
hybsnapRdbCfg hybsnapRdbCfgGet(void);

/* dictType.pausedExpandAllowed of the keyspace: may a dict whose layout a save
 * holds grow into a new table 1 now (hybrid_snapshot.c)? */
int hybsnapDictPausedExpandAllowed(dict *d);

/* ---- Hybrid snapshot: the save --------------------------------------------- */

/* The one decision for rdb.c, replication.c and aof.c: 1 when a hybrid save
 * serves this request (the start functions below), 0 when the caller runs the
 * classic child. */
int hybsnapSaveEligible(int req);

/* Start a hybrid snapshot, verify before hybsnapSaveEligible() */
int hybsnapStartSaveBackground(int req, char *filename, rdbSaveInfo *rsi, int rdbflags);
int hybsnapStartSaveToSlavesSockets(int req, rdbSaveInfo *rsi);
int hybsnapStartRewriteAofBackground(void);

int hybsnapConfigApply(const char **err);
int hybsnapEpochConfigApply(const char **err);

int hybsnapIsEnabled(void);

int hybsnapSaveIsInProgress(void);

/* Events during a save: a store the save is scanning; FLUSHDB/SWAPDB hand their dbs to the save. */
int hybsnapSaveOwnsKvstore(kvstore *kvs);

int hybsnapSaveDetachDb(kvstore *keys, kvstore *expires, void *subexpires, dict *stream_idmp_keys, kvstore *blessed);
/* Main thread: stop the scan (not the child, see killRDBChild). */
void hybsnapSaveAbort(void);
/* The one child-end hook, for every child type, before the type's done handler. */
void hybsnapOnChildEnd(int exitcode, int bysignal);

/* serverCron/beforeSleep: act on a pending hard-mark abort. */
void hybsnapSaveCron(void);

/* Read paths: is 'kv' a fork-instant original the save has not serialized yet (opens the command's write section). */
int hybsnapKvobjIsPendingSave(redisDb *db, kvobj *kv);

/* A read path about to write 'kv' in place: the object to write (the clone when the key was pending save). */
kvobj *hybsnapPrepareMutation(redisDb *db, robj *key, kvobj *kv);
/* Debug-build predicate and assertion at the write funnels: pending save, and the value may move on write. */
int hybsnapKvobjIsPendingSaveMovable(redisDb *db, kvobj *kv);
#define hybsnapDebugAssertNotPendingSaveMovable(db, kv) \
    debugServerAssert((kv) == NULL || !((robj *)(kv))->iskvobj || !hybsnapKvobjIsPendingSaveMovable((db), (kvobj *)(kv)))

/* ---- Observability -------------------------------------------------------- */

/* INFO memory / persistence fields, DEBUG SNAP-* (1 when handled), the INFO sections safe in the hybrid child (else NULL). */
sds hybsnapInfoMemory(sds info);
sds hybsnapInfoPersistence(sds info);
int hybsnapDebugCommand(struct client *c);
const char *const *hybsnapCrashReportInfoSections(void);

/* The save's memory in the parent that the classic child would hold in its own process: excluded from the eviction calculation. */
size_t hybsnapSaveOverheadBytes(void);
size_t hybsnapSaveKeysProcessed(size_t classic);

/* ---- debug and tests ------------------------------------------------------ */

/* CONFIG SET rdb-key-save-delay: reaches the running scan thread (DEBUG) */
void hybsnapSaveSetKeySaveDelay(int usec);

#ifdef REDIS_TEST
int hybsnapTest(int argc, char *argv[], int flags);
#endif

#endif /* __HYBRID_SNAPSHOT_H */
