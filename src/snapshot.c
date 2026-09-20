/*
 * snapshot.c - Hybrid BGSAVE implementation (see poc-hybrid-bgsave-spec.md).
 *
 * This file currently provides the SNAP_NOFORK jemalloc arena: a dedicated
 * arena whose extents are madvise(MADV_DONTFORK)ed so that a BGSAVE fork
 * child never maps them. Every kvobj header, every SNAP_NOFORK value and all
 * keyspace hash tables are meant to live here, which removes copy-on-write
 * amplification on them while the child runs.
 *
 * Allocator notes:
 *  - The arena is created with a copy of jemalloc's default extent hooks in
 *    which 'alloc' and 'commit' are wrapped to (re)apply MADV_DONTFORK.
 *    MADV_DONTFORK is a VMA flag, so purge (MADV_FREE/MADV_DONTNEED), split
 *    and merge leave it intact; decommit+commit re-applies it in 'commit'.
 *  - Allocations use MALLOCX_ARENA(arena) plus a per-thread explicit tcache.
 *    Explicit tcaches may be used by one thread at a time (jemalloc docs), so
 *    each thread lazily creates its own. Only SNAP_NOFORK requests ever use
 *    these tcaches, so a freed SNAP_NOFORK block can never be recycled into a
 *    default-arena (SNAP_FORK) allocation. This is why plain zfree() must not
 *    be used on SNAP_NOFORK memory.
 *  - Only the parent may touch the arena: its range is unmapped in the child.
 *
 * Copyright (c) 2026-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of (a) the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#include "server.h"
#include "snapshot.h"
#include <sys/mman.h>

#if defined(USE_JEMALLOC) && defined(__linux__) && defined(MADV_DONTFORK)
#define SNAP_NOFORK_ARENA_SUPPORTED 1
#endif

/* Guard: the SNAP_NOFORK arena must never be allocated from, freed into, or
 * otherwise touched by a fork child (its virtual range is unmapped there). */
static inline void snapAssertNotChild(void) {
    if (unlikely(server.in_fork_child != CHILD_TYPE_NONE))
        serverPanic("SNAP_NOFORK arena accessed from a fork child");
}

#ifdef SNAP_NOFORK_ARENA_SUPPORTED

/* jemalloc's default hooks, read from arena 0 at init. They ignore the
 * extent_hooks_t pointer they receive and key off 'arena_ind', so wrapping
 * them is safe. */
static extent_hooks_t *snap_default_hooks = NULL;
/* Our hooks: a copy of the defaults with 'alloc' and 'commit' wrapped. The
 * struct must outlive the arena, hence static. */
static extent_hooks_t snap_hooks;
/* Precomputed MIB for "arenas.lookup" (pointer -> arena index). */
static size_t snap_lookup_mib[4];
static size_t snap_lookup_miblen = 4;

static void snapApplyDontfork(void *addr, size_t size) {
    if (madvise(addr, size, MADV_DONTFORK) != 0) {
        static int warned = 0;
        if (!warned) {
            warned = 1;
            serverLog(LL_WARNING, "madvise(MADV_DONTFORK) failed on a SNAP_NOFORK arena extent: %s",
                      strerror(errno));
        }
    }
}

static void *snapExtentAlloc(extent_hooks_t *hooks, void *new_addr, size_t size,
                             size_t alignment, bool *zero, bool *commit, unsigned arena_ind)
{
    UNUSED(hooks);
    void *addr = snap_default_hooks->alloc(snap_default_hooks, new_addr, size,
                                           alignment, zero, commit, arena_ind);
    if (addr) snapApplyDontfork(addr, size);
    return addr;
}

/* Hooks return false on success. Re-apply the flag after a successful commit
 * since a decommit may have replaced the mapping. */
static bool snapExtentCommit(extent_hooks_t *hooks, void *addr, size_t size,
                             size_t offset, size_t length, unsigned arena_ind)
{
    UNUSED(hooks);
    bool err = snap_default_hooks->commit(snap_default_hooks, addr, size, offset, length, arena_ind);
    if (!err) snapApplyDontfork((char*)addr + offset, length);
    return err;
}

void snapArenaInit(void) {
    server.snap_nofork_arena = UINT_MAX;

    size_t sz = sizeof(extent_hooks_t *);
    if (je_mallctl("arena.0.extent_hooks", &snap_default_hooks, &sz, NULL, 0) != 0 ||
        snap_default_hooks == NULL)
    {
        serverLog(LL_WARNING, "SNAP_NOFORK arena disabled: cannot read jemalloc default extent hooks.");
        return;
    }
    snap_hooks = *snap_default_hooks;
    snap_hooks.alloc = snapExtentAlloc;
    snap_hooks.commit = snapExtentCommit;

    if (je_mallctlnametomib("arenas.lookup", snap_lookup_mib, &snap_lookup_miblen) != 0) {
        serverLog(LL_WARNING, "SNAP_NOFORK arena disabled: arenas.lookup is unavailable.");
        return;
    }

    /* Create the arena with "experimental.arenas_create_ext" rather than
     * "arenas.create": the latter allocates the arena's own metadata (arena
     * struct, bins, mutexes, extent records) THROUGH the arena's extent hooks,
     * which would make that metadata DONTFORK too and crash jemalloc's
     * post-fork handler in every child. metadata_use_hooks=false keeps the
     * metadata in normal forkable mappings; only data extents are DONTFORK.
     * The struct mirrors jemalloc's internal arena_config_t (5.3). */
    struct { extent_hooks_t *extent_hooks; bool metadata_use_hooks; } cfg = {
        .extent_hooks = &snap_hooks,
        .metadata_use_hooks = false,
    };
    unsigned arena;
    sz = sizeof(arena);
    int err = je_mallctl("experimental.arenas_create_ext", &arena, &sz, &cfg, sizeof(cfg));
    if (err) {
        serverLog(LL_WARNING, "SNAP_NOFORK arena disabled: experimental.arenas_create_ext failed (err=%d).", err);
        return;
    }
    server.snap_nofork_arena = arena;
    serverLog(LL_NOTICE, "SNAP_NOFORK arena created (jemalloc arena %u, MADV_DONTFORK).", arena);
}

int snapArenaAvailable(void) {
    return server.snap_nofork_arena != UINT_MAX;
}

/* Per-thread mallocx flags: MALLOCX_ARENA(arena) | MALLOCX_TCACHE(tc).
 * 0 means "not initialized yet" (both macros yield non-zero values). Threads
 * are long lived in Redis, so the explicit tcache is never destroyed. */
static __thread int snap_thread_flags = 0;

static int snapThreadFlagsInit(void) {
    unsigned tcache;
    size_t sz = sizeof(tcache);
    int flags = MALLOCX_ARENA(server.snap_nofork_arena);
    if (je_mallctl("tcache.create", &tcache, &sz, NULL, 0) == 0)
        flags |= MALLOCX_TCACHE(tcache);
    else
        flags |= MALLOCX_TCACHE_NONE;
    snap_thread_flags = flags;
    return flags;
}

static inline int snapFlags(void) {
    int flags = snap_thread_flags;
    if (unlikely(flags == 0)) flags = snapThreadFlagsInit();
    return flags;
}

void *snapNoforkAlloc(size_t size) {
    snapAssertNotChild();
    if (unlikely(!snapArenaAvailable())) return zmalloc(size);
    /* mallocx() rejects size 0; mirror zmalloc's minimum allocation. */
    if (unlikely(size == 0)) size = sizeof(long);
    return zmalloc_with_flags(size, snapFlags());
}

void *snapNoforkAllocUsable(size_t size, size_t *usable) {
    void *ptr = snapNoforkAlloc(size);
    if (usable) *usable = zmalloc_size(ptr);
    return ptr;
}

void *snapNoforkRealloc(void *ptr, size_t size) {
    snapAssertNotChild();
    if (unlikely(!snapArenaAvailable())) return zrealloc(ptr, size);
    return zrealloc_with_flags(ptr, size, snapFlags());
}

void snapNoforkFree(void *ptr) {
    if (ptr == NULL) return;
    snapAssertNotChild();
    if (unlikely(!snapArenaAvailable())) { zfree(ptr); return; }
    zfree_with_flags(ptr, snapFlags());
}

void snapNoforkFreeSize(void *ptr, size_t usable) {
    if (ptr == NULL) return;
    snapAssertNotChild();
    if (unlikely(!snapArenaAvailable())) { zfree_with_size(ptr, usable); return; }
    zfree_with_size_and_flags(ptr, usable, snapFlags());
}

int snapPtrIsNofork(const void *ptr) {
    if (ptr == NULL || !snapArenaAvailable()) return 0;
    unsigned ind;
    size_t sz = sizeof(ind);
    if (je_mallctlbymib(snap_lookup_mib, snap_lookup_miblen, &ind, &sz,
                        (void *)&ptr, sizeof(ptr)) != 0)
        return 0;
    return ind == server.snap_nofork_arena;
}

#else /* !SNAP_NOFORK_ARENA_SUPPORTED: degrade to the default allocator. */

void snapArenaInit(void) {
    server.snap_nofork_arena = UINT_MAX;
    serverLog(LL_NOTICE, "SNAP_NOFORK arena unavailable on this platform/allocator; "
                         "hybrid BGSAVE disabled.");
}
int snapArenaAvailable(void) { return 0; }
void *snapNoforkAlloc(size_t size) { snapAssertNotChild(); return zmalloc(size); }
void *snapNoforkAllocUsable(size_t size, size_t *usable) { snapAssertNotChild(); return zmalloc_usable(size, usable); }
void *snapNoforkRealloc(void *ptr, size_t size) { snapAssertNotChild(); return zrealloc(ptr, size); }
void snapNoforkFree(void *ptr) { snapAssertNotChild(); zfree(ptr); }
void snapNoforkFreeSize(void *ptr, size_t usable) { snapAssertNotChild(); zfree_with_size(ptr, usable); }
int snapPtrIsNofork(const void *ptr) { UNUSED(ptr); return 0; }

#endif

size_t snapNoforkUsableSize(void *ptr) {
    return zmalloc_size(ptr);
}

/* ---- Class-aware free wrappers ------------------------------------------- */

void snapSdsFree(sds s) {
    if (s == NULL) return;
    snapNoforkFree(sdsAllocPtr(s));
}

void snapLpFree(unsigned char *lp) {
    snapNoforkFree(lp);
}

void snapIntsetFree(intset *is) {
    snapNoforkFree(is);
}

void snapListpackExFree(struct listpackEx *lpt) {
    if (lpt == NULL) return;
    snapNoforkFree(lpt->lp);
    snapNoforkFree(lpt);
}


/* ============================================================================
 * Per-slot background scan
 * ============================================================================
 *
 * The scan walks every kvstore dict ("slot") of every store in sequence on a
 * dedicated thread and hands each fork-instant kvobj to a dispatch callback.
 *
 * Immutability contract (spec §4, invariant 2): from snapScanStart() until a
 * slot is DONE, its dict is never modified. The main thread guarantees no
 * insert/delete (writes go to the per-slot temp dict) and pauses rehashing
 * and auto-resize; the thread only reads (dictScanNoPause never writes to the
 * dict, unlike dictScan which bumps d->pauserehash). Cron-driven resize and
 * rehash are already skipped by databasesCron while a child process exists.
 *
 * Publication: slot states are atomics written with release semantics by the
 * thread (SCANNING, SCANNED) and by the main thread (PENDING, DONE), and read
 * with acquire semantics. Completed slots are handed to the main thread over
 * a single-producer/single-consumer ring (no locks, no allocation on the
 * thread) plus an eventNotifier so the event loop wakes up.
 *
 * M1 decision on granularity: the scan unit is one kvstore dict. In cluster
 * mode that is one of 16384 slots; in standalone mode a db has a single dict,
 * so the whole db stays owed until fully scanned and the clone-on-write
 * burden does not shrink during the save. The write hooks only depend on
 * snapScanKeyIsOwed(), so refining the unit to bucket ranges later (compare
 * the key's bucket, in dictScan cursor order, with the published cursor; valid
 * because the dict is frozen) is a local change to this file.
 */

#include "atomicvar.h"
#include "ae.h"
#include "eventnotifier.h"
#include <pthread.h>

struct snapScanCtx {
    kvstore **stores;               /* One per db (db->keys). */
    int num_stores;
    int *num_dicts;                 /* kvstoreNumDicts() per store. */
    redisAtomic uint8_t **state;    /* Per store: num_dicts slot states. */
    int *frozen;                    /* Per store: 1 if its dicts were paused by us. */

    snapScanDispatchFn *dispatch;
    snapScanSlotDoneFn *slot_done;
    void *privdata;

    /* SPSC completion ring, capacity == slots_total. */
    uint64_t *ring;
    redisAtomic long ring_wr;       /* Written by the thread. */
    long ring_rd;                   /* Main thread only. */
    eventNotifier *en;

    pthread_t tid;
    int started;
    int joined;
    redisAtomic int abort;          /* Main -> thread. */
    redisAtomic int finished;       /* Thread -> main. */

    long slots_total;
    redisAtomic long slots_scanned;
    long slots_done;                /* Main thread only. */
    redisAtomic long long keys_dispatched;
};

#define SNAP_SCAN_ENC(store_idx, didx) (((uint64_t)(uint32_t)(store_idx) << 32) | (uint32_t)(didx))
#define SNAP_SCAN_DEC_STORE(v) ((int)((v) >> 32))
#define SNAP_SCAN_DEC_DIDX(v) ((int)((v) & 0xffffffffu))

static inline void snapSlotSetState(snapScanCtx *ctx, int s, int didx, uint8_t st) {
    atomicSetWithSync(ctx->state[s][didx], st);
}

static inline uint8_t snapSlotGetState(snapScanCtx *ctx, int s, int didx) {
    uint8_t st;
    atomicGetWithSync(ctx->state[s][didx], st);
    return st;
}

snapScanCtx *snapScanCreate(kvstore **stores, int num_stores,
                            snapScanDispatchFn *dispatch,
                            snapScanSlotDoneFn *slot_done, void *privdata)
{
    snapScanCtx *ctx = zcalloc(sizeof(*ctx));
    ctx->stores = zmalloc(sizeof(kvstore*) * num_stores);
    memcpy(ctx->stores, stores, sizeof(kvstore*) * num_stores);
    ctx->num_stores = num_stores;
    ctx->num_dicts = zmalloc(sizeof(int) * num_stores);
    ctx->state = zmalloc(sizeof(redisAtomic uint8_t*) * num_stores);
    ctx->frozen = zcalloc(sizeof(int) * num_stores);
    for (int s = 0; s < num_stores; s++) {
        ctx->num_dicts[s] = kvstoreNumDicts(stores[s]);
        ctx->state[s] = zcalloc(sizeof(redisAtomic uint8_t) * ctx->num_dicts[s]); /* PENDING */
        ctx->slots_total += ctx->num_dicts[s];
    }
    ctx->ring = zmalloc(sizeof(uint64_t) * (ctx->slots_total ? ctx->slots_total : 1));
    ctx->dispatch = dispatch;
    ctx->slot_done = slot_done;
    ctx->privdata = privdata;
    ctx->en = createEventNotifier();
    return ctx;
}

static void snapScanFreezeStore(snapScanCtx *ctx, int s) {
    for (int didx = 0; didx < ctx->num_dicts[s]; didx++) {
        dict *d = kvstoreGetDict(ctx->stores[s], didx);
        if (d) {
            dictPauseRehashing(d);
            dictPauseAutoResize(d);
        }
    }
    ctx->frozen[s] = 1;
}

static void snapScanUnfreezeDict(snapScanCtx *ctx, int s, int didx) {
    if (!ctx->frozen[s]) return;
    dict *d = kvstoreGetDict(ctx->stores[s], didx);
    if (d) {
        dictResumeRehashing(d);
        dictResumeAutoResize(d);
    }
}

/* dictScan callback on the scan thread: decode the entry and dispatch. */
struct snapScanEntryArgs { snapScanCtx *ctx; int s; int didx; };

static void snapScanEntryCb(void *privdata, const dictEntry *de, dictEntry **plink) {
    UNUSED(plink);
    struct snapScanEntryArgs *a = privdata;
    atomicIncr(a->ctx->keys_dispatched, 1);
    a->ctx->dispatch(a->ctx->privdata, a->s, a->didx, dictGetKV(de));
}

static inline int snapScanAborted(snapScanCtx *ctx) {
    int v;
    atomicGet(ctx->abort, v);
    return v;
}

static void snapScanPublishScanned(snapScanCtx *ctx, int s, int didx) {
    snapSlotSetState(ctx, s, didx, SNAP_SLOT_SCANNED);
    long wr;
    atomicGet(ctx->ring_wr, wr);
    ctx->ring[wr] = SNAP_SCAN_ENC(s, didx);
    atomicSetWithSync(ctx->ring_wr, wr + 1);   /* release: ring[wr] visible before wr+1 */
    atomicIncr(ctx->slots_scanned, 1);
    triggerEventNotifier(ctx->en);
}

static void *snapScanThreadMain(void *arg) {
    snapScanCtx *ctx = arg;
    redis_set_thread_title("snap_scan");
    makeThreadKillable();

    for (int s = 0; s < ctx->num_stores && !snapScanAborted(ctx); s++) {
        for (int didx = 0; didx < ctx->num_dicts[s]; didx++) {
            if (snapScanAborted(ctx)) break;
            dict *d = kvstoreGetDict(ctx->stores[s], didx);
            if (d == NULL || dictSize(d) == 0) {
                snapScanPublishScanned(ctx, s, didx);
                continue;
            }
            snapSlotSetState(ctx, s, didx, SNAP_SLOT_SCANNING);
            struct snapScanEntryArgs a = { ctx, s, didx };
            unsigned long cursor = 0;
            int aborted = 0;
            do {
                cursor = dictScanNoPause(d, cursor, snapScanEntryCb, &a);
                if (snapScanAborted(ctx)) { aborted = 1; break; }
            } while (cursor);
            if (aborted) break;   /* Leave the slot SCANNING; snapScanAbort() completes it. */
            snapScanPublishScanned(ctx, s, didx);
        }
    }
    atomicSetWithSync(ctx->finished, 1);
    triggerEventNotifier(ctx->en);
    return NULL;
}

int snapScanStart(snapScanCtx *ctx) {
    serverAssert(!ctx->started);
    for (int s = 0; s < ctx->num_stores; s++) snapScanFreezeStore(ctx, s);
    if (pthread_create(&ctx->tid, NULL, snapScanThreadMain, ctx) != 0) {
        serverLog(LL_WARNING, "snapScanStart: pthread_create failed: %s", strerror(errno));
        for (int s = 0; s < ctx->num_stores; s++) {
            for (int didx = 0; didx < ctx->num_dicts[s]; didx++) snapScanUnfreezeDict(ctx, s, didx);
            ctx->frozen[s] = 0;
        }
        return C_ERR;
    }
    ctx->started = 1;
    return C_OK;
}

/* Main thread: finish one slot (merge hook, unfreeze, publish DONE). */
static void snapScanCompleteSlot(snapScanCtx *ctx, int s, int didx, int aborted) {
    if (ctx->slot_done) ctx->slot_done(ctx->privdata, s, didx, aborted);
    snapScanUnfreezeDict(ctx, s, didx);
    snapSlotSetState(ctx, s, didx, SNAP_SLOT_DONE);
    ctx->slots_done++;
}

int snapScanHandleCompletions(snapScanCtx *ctx) {
    int n = 0;
    long wr;
    atomicGetWithSync(ctx->ring_wr, wr);   /* acquire */
    while (ctx->ring_rd < wr) {
        uint64_t v = ctx->ring[ctx->ring_rd++];
        snapScanCompleteSlot(ctx, SNAP_SCAN_DEC_STORE(v), SNAP_SCAN_DEC_DIDX(v), 0);
        n++;
    }
    return n;
}

int snapScanIsFinished(snapScanCtx *ctx) {
    int v;
    atomicGetWithSync(ctx->finished, v);
    return v;
}

static void snapScanJoin(snapScanCtx *ctx) {
    if (ctx->started && !ctx->joined) {
        pthread_join(ctx->tid, NULL);
        ctx->joined = 1;
    }
}

void snapScanAbort(snapScanCtx *ctx) {
    if (!ctx->started) return;
    atomicSetWithSync(ctx->abort, 1);
    snapScanJoin(ctx);
    /* Fully scanned slots complete normally. */
    snapScanHandleCompletions(ctx);
    /* Everything else was interrupted. */
    for (int s = 0; s < ctx->num_stores; s++) {
        for (int didx = 0; didx < ctx->num_dicts[s]; didx++) {
            if (snapSlotGetState(ctx, s, didx) != SNAP_SLOT_DONE)
                snapScanCompleteSlot(ctx, s, didx, 1);
        }
    }
}

void snapScanRelease(snapScanCtx *ctx) {
    if (ctx == NULL) return;
    snapScanJoin(ctx);
    if (ctx->started) {
        /* Drain the tail so no slot is left frozen. */
        snapScanHandleCompletions(ctx);
        serverAssert(ctx->slots_done == ctx->slots_total);
    }
    for (int s = 0; s < ctx->num_stores; s++) zfree(ctx->state[s]);
    zfree(ctx->state);
    zfree(ctx->num_dicts);
    zfree(ctx->frozen);
    zfree(ctx->stores);
    zfree(ctx->ring);
    freeEventNotifier(ctx->en);
    zfree(ctx);
}

int snapScanSlotState(snapScanCtx *ctx, int store_idx, int didx) {
    return snapSlotGetState(ctx, store_idx, didx);
}

int snapScanKeyIsOwed(snapScanCtx *ctx, int store_idx, int didx) {
    return snapSlotGetState(ctx, store_idx, didx) != SNAP_SLOT_DONE;
}

int snapScanReadEventFd(snapScanCtx *ctx) {
    return ctx->en ? getReadEventFd(ctx->en) : -1;
}

void snapScanAeHandler(struct aeEventLoop *el, int fd, void *privdata, int mask) {
    UNUSED(el);
    UNUSED(fd);
    UNUSED(mask);
    snapScanCtx *ctx = privdata;
    handleEventNotifier(ctx->en);
    snapScanHandleCompletions(ctx);
}

void snapScanProgress(snapScanCtx *ctx, long *slots_total, long *slots_scanned,
                      long *slots_done, long long *keys_dispatched)
{
    if (slots_total) *slots_total = ctx->slots_total;
    if (slots_scanned) atomicGet(ctx->slots_scanned, *slots_scanned);
    if (slots_done) *slots_done = ctx->slots_done;
    if (keys_dispatched) atomicGet(ctx->keys_dispatched, *keys_dispatched);
}

/* ---- Unit tests ---------------------------------------------------------- */
#ifdef REDIS_TEST
#include "testhelp.h"
#include <sys/wait.h>
#include <time.h>

/* Fork a child that writes one byte to 'p'. The child reports over a pipe
 * that it reached the write ('r') and that it survived it ('d'), so a crash
 * anywhere else (e.g. in jemalloc's post-fork handler) is not mistaken for a
 * fault on 'p'. Returns 1 if the child faulted exactly on the write (SIGSEGV
 * or SIGBUS after 'r' and no 'd'), 0 if it wrote fine and exited 0, and -1 on
 * any other outcome (printed). */
static int snapTestChildWriteFaults(void *p) {
    int fds[2];
    if (pipe(fds) != 0) return -1;
    pid_t pid = fork();
    if (pid == 0) {
        close(fds[0]);
        volatile char *c = p;
        if (write(fds[1], "r", 1) != 1) _exit(2);
        c[0] = 1;
        if (write(fds[1], "d", 1) != 1) _exit(2);
        _exit(0);
    }
    close(fds[1]);
    if (pid < 0) { close(fds[0]); return -1; }
    char buf[4] = {0};
    ssize_t n = 0, r;
    while (n < 2 && (r = read(fds[0], buf + n, 2 - n)) > 0) n += r;
    close(fds[0]);
    int status = 0;
    waitpid(pid, &status, 0);
    int reached = (n >= 1 && buf[0] == 'r');
    int survived = (n >= 2 && buf[1] == 'd');
    if (reached && !survived && WIFSIGNALED(status) &&
        (WTERMSIG(status) == SIGSEGV || WTERMSIG(status) == SIGBUS))
        return 1;
    if (reached && survived && WIFEXITED(status) && WEXITSTATUS(status) == 0)
        return 0;
    printf("  (child outcome: reached=%d survived=%d signaled=%d sig=%d exited=%d code=%d)\n",
           reached, survived, WIFSIGNALED(status), WIFSIGNALED(status) ? WTERMSIG(status) : 0,
           WIFEXITED(status), WIFEXITED(status) ? WEXITSTATUS(status) : -1);
    return -1;
}

/* Fork a child that exercises the allocator (zmalloc + snapNofork are both
 * forbidden/unmapped for the arena, so only the default arena is used) to
 * prove jemalloc's post-fork state is intact with the SNAP_NOFORK arena
 * present. Returns 1 on success. */
static int snapTestChildAllocatorAlive(void) {
    pid_t pid = fork();
    if (pid == 0) {
        for (int i = 0; i < 1000; i++) { void *x = zmalloc(64 + i); zfree(x); }
        void *big = zmalloc(3 * 1024 * 1024); memset(big, 1, 3 * 1024 * 1024); zfree(big);
        _exit(0);
    }
    if (pid < 0) return 0;
    int status = 0;
    waitpid(pid, &status, 0);
    if (!(WIFEXITED(status) && WEXITSTATUS(status) == 0))
        printf("  (allocator child: signaled=%d sig=%d exited=%d code=%d)\n", WIFSIGNALED(status),
               WIFSIGNALED(status) ? WTERMSIG(status) : 0, WIFEXITED(status),
               WIFEXITED(status) ? WEXITSTATUS(status) : -1);
    return WIFEXITED(status) && WEXITSTATUS(status) == 0;
}

/* Look up the mapping containing 'p' in /proc/self/smaps and return 1 if its
 * VmFlags include "dc" (VM_DONTCOPY, i.e. MADV_DONTFORK), 0 if not, -1 if not
 * found. */
static int snapTestMappingIsDontcopy(const void *p) {
    FILE *f = fopen("/proc/self/smaps", "r");
    if (!f) return -1;
    char line[1024];
    int in_range = 0, result = -1;
    unsigned long target = (unsigned long)p;
    while (fgets(line, sizeof(line), f)) {
        unsigned long lo, hi;
        if (sscanf(line, "%lx-%lx ", &lo, &hi) == 2 && strchr(line, '-') && strchr(line, ' ') &&
            (strstr(line, " r") || strstr(line, " -")))
        {
            in_range = (target >= lo && target < hi);
            continue;
        }
        if (in_range && !strncmp(line, "VmFlags:", 8)) {
            result = strstr(line, " dc") != NULL;
            break;
        }
    }
    fclose(f);
    return result;
}

static long long snapTestNowNs(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (long long)ts.tv_sec * 1000000000LL + ts.tv_nsec;
}

static int snapTestCountMaps(void) {
    FILE *f = fopen("/proc/self/maps", "r");
    if (!f) return -1;
    int n = 0;
    char line[1024];
    while (fgets(line, sizeof(line), f)) n++;
    fclose(f);
    return n;
}

static void snapTestPurgeArena(void) {
#ifdef SNAP_NOFORK_ARENA_SUPPORTED
    char buf[64];
    snprintf(buf, sizeof(buf), "arena.%u.purge", server.snap_nofork_arena);
    je_mallctl(buf, NULL, 0, NULL, 0);
    snprintf(buf, sizeof(buf), "arena.%u.decay", server.snap_nofork_arena);
    je_mallctl(buf, NULL, 0, NULL, 0);
#endif
}


/* ---- Scan tests ---- */
extern dictType KvstoreDictTestType;   /* kvstore.c (REDIS_TEST) */
extern kvstoreType KvstoreTestType;

#define SNAP_TEST_STORES 2
#define SNAP_TEST_DICT_BITS 2
#define SNAP_TEST_DICTS (1 << SNAP_TEST_DICT_BITS)
#define SNAP_TEST_MAXKEYS 6000

struct snapScanTestState {
    unsigned char seen[SNAP_TEST_STORES][SNAP_TEST_DICTS][SNAP_TEST_MAXKEYS];
    int inserted[SNAP_TEST_STORES][SNAP_TEST_DICTS];
    redisAtomic long dispatched;
    redisAtomic long bad;             /* unparsable / out-of-range / duplicate */
    int done_calls[SNAP_TEST_STORES][SNAP_TEST_DICTS];
    int done_aborted[SNAP_TEST_STORES][SNAP_TEST_DICTS];
    int dispatch_sleep_us;
    redisAtomic int wrong_state;      /* dispatch saw its slot not SCANNING */
    snapScanCtx *ctx;
    kvstore **stores;
    int rehashing_at_done;
};

static char *snapTestKey(int s, int didx, int i) {
    char buf[64];
    int len = snprintf(buf, sizeof(buf), "k:%d:%d:%d", s, didx, i);
    char *k = zmalloc(len + 1);
    memcpy(k, buf, len + 1);
    return k;
}

static void snapTestDispatch(void *priv, int s, int didx, kvobj *kv) {
    struct snapScanTestState *st = priv;
    const char *key = (const char*)kv;   /* test dict stores plain C strings */
    int ps, pd, pi;
    if (sscanf(key, "k:%d:%d:%d", &ps, &pd, &pi) != 3 || ps != s || pd != didx ||
        pi < 0 || pi >= st->inserted[s][didx] || st->seen[s][didx][pi])
    {
        atomicIncr(st->bad, 1);
    } else {
        st->seen[s][didx][pi] = 1;
    }
    if (st->ctx && snapScanSlotState(st->ctx, s, didx) != SNAP_SLOT_SCANNING) atomicSetWithSync(st->wrong_state, 1);
    atomicIncr(st->dispatched, 1);
    if (st->dispatch_sleep_us) usleep(st->dispatch_sleep_us);
}

static int snapTestKeyCompare(dictCmpCache *cache, const void *k1, const void *k2) {
    UNUSED(cache);
    return strcmp(k1, k2) == 0;
}

static void snapTestSlotDone(void *priv, int s, int didx, int aborted) {
    struct snapScanTestState *st = priv;
    st->done_calls[s][didx]++;
    st->done_aborted[s][didx] = aborted;
    /* The dict is still frozen here: record whether the mid-rehash dict is
     * still mid-rehash, i.e. the scan itself never rehashed it. */
    if (s == 0 && didx == 3 && st->stores)
        st->rehashing_at_done = dictIsRehashing(kvstoreGetDict(st->stores[0], 3));
}

static int snapTestAllUnfrozen(kvstore **stores) {
    for (int s = 0; s < SNAP_TEST_STORES; s++)
        for (int didx = 0; didx < SNAP_TEST_DICTS; didx++) {
            dict *d = kvstoreGetDict(stores[s], didx);
            if (d && (d->pauserehash != 0 || d->pauseAutoResize != 0)) return 0;
        }
    return 1;
}

static int snapTestAllState(snapScanCtx *ctx, int state) {
    for (int s = 0; s < SNAP_TEST_STORES; s++)
        for (int didx = 0; didx < SNAP_TEST_DICTS; didx++)
            if (snapScanSlotState(ctx, s, didx) != state) return 0;
    return 1;
}

static void snapScanTests(int accurate) {
    dictType noval = KvstoreDictTestType;
    noval.no_value = 1;
    noval.keyCompare = snapTestKeyCompare;   /* the kvstore test type compares pointers */
    kvstore *stores[SNAP_TEST_STORES];
    for (int s = 0; s < SNAP_TEST_STORES; s++)
        stores[s] = kvstoreCreate(&KvstoreTestType, &noval, SNAP_TEST_DICT_BITS, KVSTORE_ALLOCATE_DICTS_ON_DEMAND);

    struct snapScanTestState *st = zcalloc(sizeof(*st));
    st->stores = stores;

    /* Population: store 0: d0 = 1 key, d1 = empty (never allocated), d2 = 5000
     * keys, d3 = left mid-rehash. Store 1: d0..d2 = 100 keys each, d3 empty. */
    dictSetResizeEnabled(DICT_RESIZE_ENABLE);
    int plan[SNAP_TEST_STORES][SNAP_TEST_DICTS] = { {1, 0, 5000, -1}, {100, 100, 100, 0} };
    for (int s = 0; s < SNAP_TEST_STORES; s++) {
        for (int didx = 0; didx < SNAP_TEST_DICTS; didx++) {
            int n = plan[s][didx];
            if (n == -1) {
                /* Insert until the dict is rehashing (expand just triggered). */
                int i = 0;
                while (i < SNAP_TEST_MAXKEYS) {
                    serverAssert(kvstoreDictAddRaw(stores[s], didx, snapTestKey(s, didx, i), NULL) != NULL);
                    i++;
                    if (i > 64 && dictIsRehashing(kvstoreGetDict(stores[s], didx))) break;
                }
                n = i;
            } else {
                for (int i = 0; i < n; i++)
                    serverAssert(kvstoreDictAddRaw(stores[s], didx, snapTestKey(s, didx, i), NULL) != NULL);
            }
            st->inserted[s][didx] = n;
        }
    }
    long total_keys = 0;
    for (int s = 0; s < SNAP_TEST_STORES; s++)
        for (int didx = 0; didx < SNAP_TEST_DICTS; didx++) total_keys += st->inserted[s][didx];
    test_cond("scan test: a dict was left mid-rehash at scan start",
              dictIsRehashing(kvstoreGetDict(stores[0], 3)));
    test_cond("scan test: an empty slot has no dict allocated", kvstoreGetDict(stores[0], 1) == NULL);

    /* ---- Full scan with concurrent main-thread reads ---- */
    snapScanCtx *ctx = snapScanCreate(stores, SNAP_TEST_STORES, snapTestDispatch, snapTestSlotDone, st);
    st->ctx = ctx;
    st->dispatch_sleep_us = 0;
    test_cond("scan: all slots PENDING before start", snapTestAllState(ctx, SNAP_SLOT_PENDING));
    test_cond("scan: snapScanStart() succeeds", snapScanStart(ctx) == C_OK);
    {
        dict *d = kvstoreGetDict(stores[0], 0);
        test_cond("scan: dicts are frozen after start", d->pauserehash == 1 && d->pauseAutoResize == 1);
    }
    /* Concurrent reads from the owner thread while the scan runs. */
    long reads = 0, misses = 0;
    unsigned seed = 7;
    int completions = 0;
    while (!snapScanIsFinished(ctx)) {
        seed = seed * 1103515245u + 12345u;
        int s = (seed >> 8) % SNAP_TEST_STORES, didx = (seed >> 12) % SNAP_TEST_DICTS;
        if (st->inserted[s][didx] > 0) {
            int i = (seed >> 16) % st->inserted[s][didx];
            char *k = snapTestKey(s, didx, i);
            if (kvstoreDictFind(stores[s], didx, k) == NULL) misses++;
            zfree(k);
            reads++;
        }
        completions += snapScanHandleCompletions(ctx);
    }
    completions += snapScanHandleCompletions(ctx);
    long dispatched, bad;
    atomicGet(st->dispatched, dispatched);
    atomicGet(st->bad, bad);
    int wrong_state;
    atomicGet(st->wrong_state, wrong_state);
    long all_seen = 1;
    for (int s = 0; s < SNAP_TEST_STORES; s++)
        for (int didx = 0; didx < SNAP_TEST_DICTS; didx++)
            for (int i = 0; i < st->inserted[s][didx]; i++) if (!st->seen[s][didx][i]) all_seen = 0;
    int done_once = 1;
    for (int s = 0; s < SNAP_TEST_STORES; s++)
        for (int didx = 0; didx < SNAP_TEST_DICTS; didx++)
            if (st->done_calls[s][didx] != 1 || st->done_aborted[s][didx] != 0) done_once = 0;
    printf("scan: %ld keys dispatched, %ld concurrent reads (%ld misses), %d completions\n",
           dispatched, reads, misses, completions);
    test_cond("scan: dispatched exactly the inserted key set (count)", dispatched == total_keys);
    test_cond("scan: every inserted key was dispatched exactly once", all_seen && bad == 0);
    test_cond("scan: dispatch always ran with its slot in SCANNING state", wrong_state == 0);
    test_cond("scan: concurrent owner-thread lookups all hit", reads > 0 && misses == 0);
    test_cond("scan: every slot completed exactly once, none aborted",
              done_once && completions == SNAP_TEST_STORES * SNAP_TEST_DICTS);
    test_cond("scan: all slots DONE at the end", snapTestAllState(ctx, SNAP_SLOT_DONE));
    test_cond("scan: no slot is owed after completion", !snapScanKeyIsOwed(ctx, 0, 0) && !snapScanKeyIsOwed(ctx, 1, 3));
    test_cond("scan: all dicts unfrozen at the end", snapTestAllUnfrozen(stores));
    test_cond("scan: mid-rehash dict was still mid-rehash when its slot completed (scan did not rehash)",
              st->rehashing_at_done);
    {
        long t, sc, dn; long long kd;
        snapScanProgress(ctx, &t, &sc, &dn, &kd);
        test_cond("scan: progress counters consistent", t == SNAP_TEST_STORES * SNAP_TEST_DICTS && sc == t && dn == t && kd == total_keys);
    }
    snapScanRelease(ctx);

    /* ---- Abort mid-scan ---- */
    memset(st->seen, 0, sizeof(st->seen));
    memset(st->done_calls, 0, sizeof(st->done_calls));
    memset(st->done_aborted, 0, sizeof(st->done_aborted));
    atomicSet(st->dispatched, 0);
    atomicSet(st->bad, 0);
    st->dispatch_sleep_us = accurate ? 200 : 100;   /* 5000-key slot takes >= 0.5 s */
    ctx = snapScanCreate(stores, SNAP_TEST_STORES, snapTestDispatch, snapTestSlotDone, st);
    st->ctx = ctx;
    test_cond("abort: start", snapScanStart(ctx) == C_OK);
    /* Wait until the thread is inside the first big slot. */
    long d0;
    do { usleep(1000); atomicGet(st->dispatched, d0); } while (d0 < 50 && !snapScanIsFinished(ctx));
    long long t0 = snapTestNowNs();
    snapScanAbort(ctx);
    long long abort_ms = (snapTestNowNs() - t0) / 1000000;
    atomicGet(st->dispatched, dispatched);
    int aborted_slots = 0, normal_slots = 0, calls = 0;
    for (int s = 0; s < SNAP_TEST_STORES; s++)
        for (int didx = 0; didx < SNAP_TEST_DICTS; didx++) {
            calls += st->done_calls[s][didx];
            if (st->done_aborted[s][didx]) aborted_slots++; else normal_slots++;
        }
    printf("abort: after %ld dispatched keys, abort took %lld ms; %d slots aborted, %d completed normally\n",
           dispatched, abort_ms, aborted_slots, normal_slots);
    test_cond("abort: stopped well before the scan would have finished", dispatched < total_keys);
    test_cond("abort: returned promptly (< 500 ms)", abort_ms < 500);
    test_cond("abort: thread finished", snapScanIsFinished(ctx));
    test_cond("abort: every slot completed exactly once", calls == SNAP_TEST_STORES * SNAP_TEST_DICTS);
    test_cond("abort: interrupted slots reported as aborted", aborted_slots >= 1);
    test_cond("abort: slots scanned before the abort completed normally", normal_slots >= 2);
    test_cond("abort: all slots DONE", snapTestAllState(ctx, SNAP_SLOT_DONE));
    test_cond("abort: all dicts unfrozen", snapTestAllUnfrozen(stores));
    snapScanRelease(ctx);

    /* ---- Empty stores ---- */
    {
        kvstore *empty[2];
        empty[0] = kvstoreCreate(&KvstoreTestType, &noval, 0, KVSTORE_ALLOCATE_DICTS_ON_DEMAND);
        empty[1] = kvstoreCreate(&KvstoreTestType, &noval, 3, KVSTORE_ALLOCATE_DICTS_ON_DEMAND);
        atomicSet(st->dispatched, 0);
        st->dispatch_sleep_us = 0;
        snapScanCtx *ectx = snapScanCreate(empty, 2, snapTestDispatch, NULL, st);
        st->ctx = NULL;
        test_cond("empty: start", snapScanStart(ectx) == C_OK);
        while (!snapScanIsFinished(ectx)) usleep(100);
        int n = snapScanHandleCompletions(ectx);
        atomicGet(st->dispatched, dispatched);
        test_cond("empty: all 9 slots completed with zero dispatches", n == 9 && dispatched == 0);
        snapScanRelease(ectx);
        kvstoreRelease(empty[0]);
        kvstoreRelease(empty[1]);
    }

    for (int s = 0; s < SNAP_TEST_STORES; s++) kvstoreRelease(stores[s]);
    zfree(st);
}

int snapshotTest(int argc, char **argv, int flags) {
    UNUSED(argc);
    UNUSED(argv);
    int accurate = flags & REDIS_TEST_ACCURATE;
    int large = flags & REDIS_TEST_LARGE_MEMORY;

    /* Minimal server state so serverLog() works in test mode. */
    server.in_fork_child = CHILD_TYPE_NONE;
    server.logfile = "";
    server.verbosity = LL_DEBUG;
    snapArenaInit();

#ifdef SNAP_NOFORK_ARENA_SUPPORTED
    test_cond("SNAP_NOFORK arena is available on Linux+jemalloc", snapArenaAvailable());
#else
    test_cond("SNAP_NOFORK arena reported unavailable on this platform", !snapArenaAvailable());
    void *fb = snapNoforkAlloc(100);
    test_cond("fallback alloc goes to the default allocator", fb != NULL && !snapPtrIsNofork(fb));
    snapNoforkFree(fb);
    /* The background scan does not depend on the arena. */
    snapScanTests(accurate);
    test_report();
    return 0;
#endif

    /* 1. predicate */
    char *p = snapNoforkAlloc(100);
    char *q = zmalloc(100);
    memset(p, 'x', 100);
    test_cond("snapPtrIsNofork() is true for an arena pointer", snapPtrIsNofork(p));
    test_cond("snapPtrIsNofork() is false for a zmalloc pointer", !snapPtrIsNofork(q));
    test_cond("snapNoforkUsableSize() >= requested size", snapNoforkUsableSize(p) >= 100);

    /* 2. fork visibility */
    test_cond("SNAP_NOFORK mapping carries the VM_DONTCOPY (dc) flag", snapTestMappingIsDontcopy(p) == 1);
    test_cond("zmalloc mapping does not carry the dc flag", snapTestMappingIsDontcopy(q) == 0);
    test_cond("fork child survives and can use the default allocator", snapTestChildAllocatorAlive() == 1);
    test_cond("fork child faults when writing a SNAP_NOFORK allocation", snapTestChildWriteFaults(p) == 1);
    test_cond("fork child can write a zmalloc allocation", snapTestChildWriteFaults(q) == 0);

    /* Large (>= 2MB, multi-page-run) allocation is also DONTFORK. */
    char *big = snapNoforkAlloc(5 * 1024 * 1024);
    memset(big, 1, 5 * 1024 * 1024);
    test_cond("large SNAP_NOFORK allocation is in the arena", snapPtrIsNofork(big));
    test_cond("fork child faults on a large SNAP_NOFORK allocation (first page)", snapTestChildWriteFaults(big) == 1);
    test_cond("fork child faults on a large SNAP_NOFORK allocation (last page)",
              snapTestChildWriteFaults(big + 5 * 1024 * 1024 - 1) == 1);

    /* realloc keeps the class */
    p = snapNoforkRealloc(p, 5000);
    test_cond("snapNoforkRealloc() result stays in the arena", snapPtrIsNofork(p));
    test_cond("fork child faults after realloc", snapTestChildWriteFaults(p) == 1);
    snapNoforkFree(p);
    zfree(q);
    snapNoforkFree(big);

    /* 3. stress: mixed sizes, frees, purge/decay, then re-check the flag. */
    {
        const int N = accurate ? 200000 : 40000;
        void **ptrs = zmalloc(sizeof(void*) * N);
        unsigned seed = 12345;
        int bad_class = 0, bad_fork = 0;
        for (int round = 0; round < 4; round++) {
            for (int i = 0; i < N; i++) {
                seed = seed * 1103515245u + 12345u;
                size_t sz = 8 + (seed >> 8) % 8192;
                if (i % 5000 == 0) sz = 3 * 1024 * 1024 + (seed % 1024) * 1024;
                ptrs[i] = snapNoforkAlloc(sz);
                ((char*)ptrs[i])[0] = 1;
                ((char*)ptrs[i])[sz-1] = 1;
            }
            for (int i = 0; i < N; i += 2) { snapNoforkFree(ptrs[i]); ptrs[i] = NULL; }
            snapTestPurgeArena();
            for (int i = 1; i < N; i += 2) {
                seed = seed * 1103515245u + 12345u;
                ptrs[i] = snapNoforkRealloc(ptrs[i], 8 + (seed >> 8) % 16384);
            }
            snapTestPurgeArena();
            for (int i = 1; i < N; i += (N / 32)) {
                if (!snapPtrIsNofork(ptrs[i])) bad_class++;
                if (snapTestChildWriteFaults(ptrs[i]) != 1) bad_fork++;
            }
            for (int i = 1; i < N; i += 2) snapNoforkFree(ptrs[i]);
            snapTestPurgeArena();
        }
        /* Fresh allocation after purge cycles must still be DONTFORK (commit path). */
        void *fresh = snapNoforkAlloc(4096 * 3);
        memset(fresh, 2, 4096 * 3);
        test_cond("mapping after purge cycles carries the dc flag", snapTestMappingIsDontcopy(fresh) == 1);
        test_cond("stress: sampled live pointers are all in the arena", bad_class == 0);
        test_cond("stress: sampled live pointers all fault in the child after purge/decay/realloc cycles", bad_fork == 0);
        test_cond("allocation after purge cycles is still DONTFORK", snapTestChildWriteFaults(fresh) == 1);
        snapNoforkFree(fresh);
        zfree(ptrs);
    }

    /* 4. tcache isolation: freed SNAP_NOFORK blocks must never be recycled into
     * default-arena allocations, and vice versa. */
    {
        const int N = 2000;
        void **a = zmalloc(sizeof(void*) * N);
        for (int i = 0; i < N; i++) a[i] = snapNoforkAlloc(64);
        for (int i = 0; i < N; i++) snapNoforkFree(a[i]);
        int leaked = 0;
        for (int i = 0; i < N; i++) { a[i] = zmalloc(64); if (snapPtrIsNofork(a[i])) leaked++; }
        for (int i = 0; i < N; i++) zfree(a[i]);
        test_cond("freed SNAP_NOFORK blocks are never handed to zmalloc()", leaked == 0);

        for (int i = 0; i < N; i++) a[i] = zmalloc(64);
        for (int i = 0; i < N; i++) zfree(a[i]);
        int wrong = 0;
        for (int i = 0; i < N; i++) { a[i] = snapNoforkAlloc(64); if (!snapPtrIsNofork(a[i])) wrong++; }
        for (int i = 0; i < N; i++) snapNoforkFree(a[i]);
        test_cond("freed zmalloc blocks are never handed to snapNoforkAlloc()", wrong == 0);
        zfree(a);
    }

    /* 5. accounting */
    {
        size_t before = zmalloc_used_memory();
        void *x = snapNoforkAlloc(10000);
        size_t mid = zmalloc_used_memory();
        snapNoforkFreeSize(x, snapNoforkUsableSize(x));
        size_t after = zmalloc_used_memory();
        test_cond("snapNofork allocations are counted in zmalloc_used_memory()", mid > before && after == before);
    }

    /* 6. microbenchmark (reported, not asserted) */
    {
        const int N = accurate ? 5000000 : 1000000;
        void **a = zmalloc(sizeof(void*) * N);
        long long t0, t1;

        t0 = snapTestNowNs();
        for (int i = 0; i < N; i++) { void *x = zmalloc(64); zfree(x); }
        t1 = snapTestNowNs();
        double z_pair = (double)(t1 - t0) / N;

        t0 = snapTestNowNs();
        for (int i = 0; i < N; i++) { void *x = snapNoforkAlloc(64); snapNoforkFree(x); }
        t1 = snapTestNowNs();
        double s_pair = (double)(t1 - t0) / N;

        t0 = snapTestNowNs();
        for (int i = 0; i < N; i++) a[i] = zmalloc(64);
        for (int i = 0; i < N; i++) zfree(a[i]);
        t1 = snapTestNowNs();
        double z_bulk = (double)(t1 - t0) / N;

        t0 = snapTestNowNs();
        for (int i = 0; i < N; i++) a[i] = snapNoforkAlloc(64);
        for (int i = 0; i < N; i++) snapNoforkFree(a[i]);
        t1 = snapTestNowNs();
        double s_bulk = (double)(t1 - t0) / N;

        t0 = snapTestNowNs();
        for (int i = 0; i < N; i++) a[i] = snapNoforkAlloc(64);
        for (int i = 0; i < N; i++) snapNoforkFreeSize(a[i], 64);
        t1 = snapTestNowNs();
        double s_bulk_sized = (double)(t1 - t0) / N;

        printf("BENCH alloc+free 64B, N=%d: zmalloc/zfree pair %.1f ns, snapNofork pair %.1f ns; "
               "bulk zmalloc %.1f ns/op, bulk snapNofork %.1f ns/op, bulk snapNofork sized-free %.1f ns/op\n",
               N, z_pair, s_pair, z_bulk, s_bulk, s_bulk_sized);
        zfree(a);
        test_cond("microbenchmark ran", 1);
    }

    /* 7. VMA count proxy: many small arena allocations must not explode the
     * number of mappings (MADV_DONTFORK splits VMAs, but adjacent extents
     * with identical flags merge back). */
    {
        const int N = large ? 10000000 : 1000000;
        int maps_before = snapTestCountMaps();
        void **a = zmalloc(sizeof(void*) * N);
        for (int i = 0; i < N; i++) { a[i] = snapNoforkAlloc(96); ((char*)a[i])[0] = 1; }
        int maps_after = snapTestCountMaps();
        for (int i = 0; i < N; i++) snapNoforkFree(a[i]);
        zfree(a);
        printf("VMA maps: before=%d after %d x 96B SNAP_NOFORK allocations=%d (delta %d)\n",
               maps_before, N, maps_after, maps_after - maps_before);
        test_cond("VMA count stays far below vm.max_map_count default (65530)", maps_after < 10000);
    }

    snapScanTests(accurate);

    test_report();
    return 0;
}
#endif
