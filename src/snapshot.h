/*
 * snapshot.h - Hybrid BGSAVE: SNAP_NOFORK / SNAP_FORK key classes and the
 * SNAP_NOFORK jemalloc arena (madvise MADV_DONTFORK).
 *
 * See poc-hybrid-bgsave-spec.md. All functions are prefixed "snap".
 *
 * Copyright (c) 2026-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of (a) the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#ifndef __SNAPSHOT_H
#define __SNAPSHOT_H

#include <stddef.h>
#include "server.h"   /* kvobj, kvstore, listpackEx; server.h includes this header at its end */
#include "sds.h"
#include "intset.h"

/* Key classes. The class describes the VALUE allocation(s) of a key; every
 * kvobj header is always allocated in the SNAP_NOFORK arena. */
#define SNAP_NOFORK 0   /* Value lives in the DONTFORK arena; serialized by a parent bg thread. */
#define SNAP_FORK   1   /* Value lives in forkable memory; serialized by the fork child. */

struct listpackEx;

/* ---- SNAP_NOFORK arena lifecycle ---------------------------------------- */

/* Create the SNAP_NOFORK arena. Must be called once, in the main thread,
 * before any snapNofork* allocation (initServer does it before creating the
 * db kvstores). Only creates the arena when the startup config
 * `bgsave-hybrid` is enabled: with headers in a DONTFORK arena no fork child
 * can read the keyspace, so the stock fork-based serializers cannot be used
 * and every save must go through the hybrid path. On platforms without
 * jemalloc or MADV_DONTFORK this is a no-op and snapArenaAvailable() is 0. */
void snapArenaInit(void);

/* 1 if the DONTFORK arena exists and the hybrid save path may be used. */
static inline int snapArenaAvailable(void) { return server.snap_nofork_arena != UINT_MAX; }

/* Reserved / bump-allocated bytes of the arena's address range (0 if unavailable). */
size_t snapArenaVaReserved(void);
size_t snapArenaVaUsed(void);

/* ---- Fast allocation API for SNAP_NOFORK memory -------------------------- */

/* All of these keep zmalloc's used_memory accounting. They select the arena
 * with MALLOCX_ARENA and a per-thread explicit tcache, so blocks released
 * here can only ever be handed back to SNAP_NOFORK allocations (jemalloc
 * thread caches are not arena-aware, hence plain zfree() must never be used
 * on SNAP_NOFORK memory). When the arena is unavailable they degrade to
 * zmalloc/zrealloc/zfree. Calling any of them from a fork child panics. */
/* Allocation in the SNAP_NOFORK arena, or the default allocator when there is
 * none (zmalloc.h inlines that decision). Never call from a fork child. */
static inline void *snapNoforkAlloc(size_t size) { return zmalloc_nofork(size); }
static inline void *snapNoforkCalloc(size_t size) { return zcalloc_nofork(size); }
static inline void *snapNoforkAllocUsable(size_t size, size_t *usable) { return zmalloc_nofork_usable(size, usable); }
void *snapNoforkRealloc(void *ptr, size_t size);
void snapNoforkFree(void *ptr);
void snapNoforkFreeSize(void *ptr, size_t usable);   /* sized free fast path */
size_t snapNoforkUsableSize(void *ptr);

/* Class-aware free wrappers for the value buffer types that can live in the
 * SNAP_NOFORK arena. The caller must know the class of the buffer (kvobj
 * class bit or context); these are the only correct way to release such
 * buffers. snapListpackExFree() releases both the struct and its listpack;
 * the caller must have unregistered the ExpireMeta first, exactly like
 * listpackExFree() in t_hash.c. */
void snapSdsFree(sds s);
void snapLpFree(unsigned char *lp);
void snapIntsetFree(intset *is);
void snapListpackExFree(struct listpackEx *lpt);

/* 1 if 'ptr' lies in the SNAP_NOFORK arena's reserved address range. A
 * two-compare range check (see zmalloc_is_nofork), cheap enough for hot paths. */
int snapPtrIsNofork(const void *ptr);

/* ---- Classification and placement ---------------------------------------- */

/* 1 if a value with this type/encoding is a single allocation that MAY be
 * SNAP_NOFORK (strings; listpack list/set/zset/hash; intset; listpack-ex).
 * Multi-allocation encodings (hashtable, skiplist, quicklist, stream, module,
 * arrays) are always SNAP_FORK. */
int snapEncodingIsNoforkClass(int type, int encoding);

/* Placement policy: SNAP_NOFORK iff the encoding qualifies and the value's
 * allocation is at most save-small-max-bytes (INT/EMBSTR values have no
 * separate allocation and are always SNAP_NOFORK). */
int snapClassify(int type, int encoding, size_t alloc_bytes);

/* Actual class of a stored key, decided by where its value memory IS: a key is
 * SNAP_NOFORK when its value has no separate allocation or when its single
 * allocation (for listpack-ex: both the struct and its listpack) lies in the
 * SNAP_NOFORK arena; otherwise SNAP_FORK. The background scan serializes
 * SNAP_NOFORK keys itself and ships SNAP_FORK keys to the fork child, so this
 * is correct regardless of policy drift. */
int snapKeyClass(const robj *o);

/* Apply the placement policy to a value: move a single-allocation value into
 * the arena when snapClassify() says SNAP_NOFORK and it is outside, or out of
 * the arena when it says SNAP_FORK and it is inside. Called by kvobjSet() at
 * every keyspace insertion and after unbounded in-place growth of strings.
 * Only the value moves; the header (and thus every pointer to it) stays. */
void snapPlaceValueArena(robj *o);
static inline void snapPlaceValue(robj *o) { if (__builtin_expect(snapArenaAvailable(), 0)) snapPlaceValueArena(o); }

/* Move a single-allocation value out of the arena if it is there. Must be
 * called before such a buffer is ADOPTED by a multi-allocation structure
 * (e.g. a listpack becoming the first quicklist node): every piece of a
 * SNAP_FORK value has to be readable by the fork child. */
void snapValueDetachFromArena(robj *o);

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
/* Reply with up to 'limit' keys whose value placement disagrees with its
 * class: "key type encoding drift" per entry (test aid, DEBUG SNAP-DRIFT). */
void snapPlacementDriftReply(struct client *c, redisDb *db, int limit);

/* ---- Per-slot background scan ------------------------------------------- */

/* Slot (kvstore dict) states, monotonic: PENDING -> SCANNING -> SCANNED -> DONE.
 * A slot is "owed" to the snapshot (main-thread writes must go through
 * clone-on-write into the temp dict) while its state is not DONE. */
#define SNAP_SLOT_PENDING  0   /* Not reached by the scan thread yet. */
#define SNAP_SLOT_SCANNING 1   /* Being read by the scan thread. */
#define SNAP_SLOT_SCANNED  2   /* Fully dispatched; waiting for the main-thread merge. */
#define SNAP_SLOT_DONE     3   /* Merged by the main thread; dict unfrozen. */

typedef struct snapScanCtx snapScanCtx;
struct aeEventLoop;

/* Runs on the scan thread once for every fork-instant key of a slot. It must
 * be strictly read-only on the keyspace: it may read the kvobj (and, for
 * SNAP_NOFORK values, the value) but never write to any dict or object. */
typedef void (snapScanDispatchFn)(void *privdata, int store_idx, int didx, kvobj *kv);

/* Runs on the main thread when a slot is complete, BEFORE its dict is
 * unfrozen and its state becomes DONE. 'aborted' is 1 when the slot was not
 * fully scanned because the scan was aborted. This is where the temp-dict
 * merge (TASK-1.6) hooks in. */
typedef void (snapScanSlotDoneFn)(void *privdata, int store_idx, int didx, int aborted);

/* Create a scan over 'num_stores' kvstores (normally one per db: db->keys).
 * Nothing is frozen or started until snapScanStart(). */
snapScanCtx *snapScanCreate(kvstore **stores, int num_stores,
                            snapScanDispatchFn *dispatch,
                            snapScanSlotDoneFn *slot_done, void *privdata);

/* Main thread. Freezes every existing dict of every store (rehash and
 * auto-resize paused), publishes all slots as PENDING and spawns the scan
 * thread. Returns C_OK or C_ERR (nothing frozen on error). From this point
 * until snapScanRelease() the main thread must not insert into or delete
 * from any of the scanned dicts (writes go to the temp dict). */
int snapScanStart(snapScanCtx *ctx);

/* Main thread. Handles slots the thread finished since the last call: for
 * each, calls slot_done, unfreezes the dict and publishes DONE. Returns the
 * number of slots completed. Also to be called once after
 * snapScanIsFinished() to drain the tail. */
int snapScanHandleCompletions(snapScanCtx *ctx);

/* 1 once the scan thread has exited (all slots scanned, or aborted). */
int snapScanIsFinished(snapScanCtx *ctx);

/* Main thread. Asks the thread to stop (it stops within one cursor step),
 * joins it, drains fully-scanned slots normally and completes every other
 * slot with aborted=1, unfreezing all dicts. */
void snapScanAbort(snapScanCtx *ctx);

/* Main thread. Joins the thread if still running (does not abort it: call
 * snapScanAbort() first if the scan may be in progress) and frees the ctx.
 * Every slot must be DONE by then unless the ctx was never started. */
void snapScanRelease(snapScanCtx *ctx);

/* Attach the keyspace overlay (kvstore.h) to the scan: every dict of every
 * store is frozen at snapScanStart() with these callbacks and merged on the
 * main thread when its slot completes. 'store_privdata[s]' is passed to the
 * callbacks for store s (the server passes the redisDb). Must be called
 * before snapScanStart(). */
void snapScanSetOverlay(snapScanCtx *ctx, kvstoreOverlayCloneFn *clone,
                        kvstoreOverlayFreeOrigFn *free_orig, kvstoreOverlayScannedFn *scanned,
                        kvstoreOverlayTombFn *tomb, void **store_privdata);

/* Overlay callbacks for the server keyspace (privdata = redisDb *). The clone
 * copies the whole kvobj allocation into a fresh arena block; SNAP_NOFORK
 * values (single allocation in the arena) are deep-copied, everything else
 * is shared with the original. db->expires and db->subexpires are pointed
 * at the clone. The free releases a fork-instant original after merge. */
void *snapOverlayCloneKey(void *privdata, int didx, void *orig, int *value_shared);
void snapOverlayFreeOriginal(void *privdata, int didx, void *orig, int value_shared);

/* Cheap, lock-free state queries usable on every write path. */
int snapScanSlotState(snapScanCtx *ctx, int store_idx, int didx);
int snapScanKeyIsOwed(snapScanCtx *ctx, int store_idx, int didx);

/* Readable fd that becomes ready whenever a slot completes or the thread
 * exits; register snapScanAeHandler() on it with AE_READABLE and 'ctx' as
 * client data. -1 if unavailable. */
int snapScanReadEventFd(snapScanCtx *ctx);
void snapScanAeHandler(struct aeEventLoop *el, int fd, void *privdata, int mask);

/* Two-step start: freeze first (fork-instant), spawn the thread later.
 * snapScanStart() == snapScanFreeze() + snapScanStartThread(). */
void snapScanFreeze(snapScanCtx *ctx);
int snapScanStartThread(snapScanCtx *ctx);

/* Optional hook run on the scan thread after the last slot (or on abort),
 * before the scan is published as finished. */
typedef void (snapScanThreadDoneFn)(void *privdata, int aborted);
void snapScanSetThreadInit(snapScanCtx *ctx, void (*fn)(void *privdata));
void snapScanSetThreadDone(snapScanCtx *ctx, snapScanThreadDoneFn *fn);

/* 1 when called from the scan thread. */
int snapOnScanThread(void);

/* Progress for INFO / tests. */
void snapScanProgress(snapScanCtx *ctx, long *slots_total, long *slots_scanned,
                      long *slots_done, long long *keys_dispatched);

/* ---- Hybrid BGSAVE ------------------------------------------------------- */

/* Start a hybrid background save: freeze the keyspace (overlay), fork the
 * writer child, spawn the scan thread. Same contract as rdbSaveBackground(). */
int snapSaveBackground(int req, char *filename, rdbSaveInfo *rsi, int rdbflags);
int snapSaveInProgress(void);
/* Stop the scan thread and merge every slot (main thread). Safe to call when
 * no hybrid save is running. Does not touch the child (see killRDBChild). */
void snapSaveAbort(void);
/* Called by backgroundSaveDoneHandler() when the RDB child exited. */
void snapSaveChildDone(int exitcode, int bysignal);
/* 1 when bgsave-hybrid is on: saves take the hybrid path whether or not the
 * SNAP_NOFORK arena exists (without it the CoW benefit is gone but the
 * mechanics are identical, which is what sanitizer builds exercise). */
static inline int snapHybridEnabled(void) { return server.bgsave_hybrid; }

/* 1 if 'key' of 'db' is still owed to a running hybrid save. */
int snapKeyIsOwed(redisDb *db, sds key);

/* 1 if 'kv' is the frozen (fork-instant) entry of its key in a slot that is
 * still owed: such an object must never reach a mutator, only its clone may.
 * Used by debug-build assertions at the keyspace write funnels. */
int snapKvobjIsFrozenOriginal(redisDb *db, kvobj *kv);

/* Scan generation (kvBits.save_gen, 3 bits). Every hybrid save bumps
 * server.snap_gen; the scan thread stamps each key it finished reading with
 * it (release store), so the main thread can tell an already-scanned frozen
 * original (acquire load, gen == current) from one still owed. Marks are only
 * trusted while fewer than 7 saves ran without one completing: a completed
 * save stamps every key, an aborted one leaves a mix, and after 7 aborts the
 * 8th generation would alias a stale mark. */
#define SNAP_GEN_MAX_SINCE_SUCCESS 7
int snapKvobjScanned(const kvobj *kv);           /* main thread */
void snapKvobjMarkScanned(kvobj *kv);            /* scan thread */
int snapOverlayKeyScanned(void *privdata, int didx, void *orig);   /* kvstore overlay callback */
void *snapOverlayTombstoneKey(void *privdata, int didx, void *orig); /* placeholder for a deleted scanned key */
int snapKvobjIsOwed(redisDb *db, kvobj *kv);     /* owed slot and not yet scanned */
int snapSaveMarksValid(void);
long long snapSaveInplaceWrites(void);
long long snapSaveLastInplaceWrites(void);
#define snapDebugAssertNotFrozenOriginal(db, kv) \
    debugServerAssert((kv) == NULL || !((robj *)(kv))->iskvobj || !snapKvobjIsFrozenOriginal((db), (kvobj *)(kv)))
/* Main thread, serverCron/beforeSleep: act on a pending hard-mark abort. */
void snapSaveCron(void);
/* CONFIG SET rdb-key-save-delay apply hook: reaches the running scan thread. */
void snapSaveSetKeySaveDelay(int usec);
/* Configuration the serializer consults, frozen at save start for the scan
 * thread (the main thread may CONFIG SET them meanwhile). Valid on the scan
 * thread only; rdb.c uses them when snapOnScanThread(). */
int snapScanMaxmemoryPolicy(void);
int snapScanRdbCompression(void);
/* Bytes the overlay currently keeps alive for the running save (replaced or
 * tombstoned originals), the peak of the last completed save, and the
 * hard-mark abort counter. */
size_t snapSaveRetainedBytes(void);
size_t snapSaveLastRetainedPeak(void);

void snapSaveProgress(long *slots_total, long *slots_done, long long *keys_nofork,
                      long long *keys_fork, long long *bytes_sent);

/* ---- Tests --------------------------------------------------------------- */
#ifdef REDIS_TEST
int snapshotTest(int argc, char *argv[], int flags);
#endif

#endif /* __SNAPSHOT_H */
