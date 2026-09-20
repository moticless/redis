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

    test_report();
    return 0;
}
#endif
