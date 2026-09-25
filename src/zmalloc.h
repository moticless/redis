/* zmalloc - total amount of allocated memory aware version of malloc()
 *
 * Copyright (c) 2009-Present, Redis Ltd.
 * All rights reserved.
 * 
 * Copyright (c) 2024-present, Valkey contributors.
 * All rights reserved.
 *
 * Licensed under your choice of (a) the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#ifndef __ZMALLOC_H
#define __ZMALLOC_H

/* Double expansion needed for stringification of macro values. */
#define __xstr(s) __str(s)
#define __str(s) #s

#if defined(USE_TCMALLOC)
#define ZMALLOC_LIB ("tcmalloc-" __xstr(TC_VERSION_MAJOR) "." __xstr(TC_VERSION_MINOR))
#include <google/tcmalloc.h>
#if (TC_VERSION_MAJOR == 1 && TC_VERSION_MINOR >= 6) || (TC_VERSION_MAJOR > 1)
#define HAVE_MALLOC_SIZE 1
#define zmalloc_size(p) tc_malloc_size(p)
#else
#error "Newer version of tcmalloc required"
#endif

#elif defined(USE_JEMALLOC)
#define ZMALLOC_LIB ("jemalloc-" __xstr(JEMALLOC_VERSION_MAJOR) "." __xstr(JEMALLOC_VERSION_MINOR) "." __xstr(JEMALLOC_VERSION_BUGFIX))
#include <jemalloc/jemalloc.h>
#if (JEMALLOC_VERSION_MAJOR == 2 && JEMALLOC_VERSION_MINOR >= 1) || (JEMALLOC_VERSION_MAJOR > 2)
#define HAVE_MALLOC_SIZE 1
#define zmalloc_size(p) je_malloc_usable_size(p)
#else
#error "Newer version of jemalloc required"
#endif

#elif defined(__APPLE__)
#include <malloc/malloc.h>
#define HAVE_MALLOC_SIZE 1
#define zmalloc_size(p) malloc_size(p)
#endif

/* On native libc implementations, we should still do our best to provide a
 * HAVE_MALLOC_SIZE capability. This can be set explicitly as well:
 *
 * NO_MALLOC_USABLE_SIZE disables it on all platforms, even if they are
 *      known to support it.
 * USE_MALLOC_USABLE_SIZE forces use of malloc_usable_size() regardless
 *      of platform.
 */
#ifndef ZMALLOC_LIB
#define ZMALLOC_LIB "libc"

#if !defined(NO_MALLOC_USABLE_SIZE) && \
    (defined(__GLIBC__) || defined(__FreeBSD__) || \
     defined(__DragonFly__) || defined(__HAIKU__) || \
     defined(USE_MALLOC_USABLE_SIZE))

/* Includes for malloc_usable_size() */
#ifdef __FreeBSD__
#include <malloc_np.h>
#else
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include <malloc.h>
#endif

#define HAVE_MALLOC_SIZE 1
#define zmalloc_size(p) malloc_usable_size(p)

#endif
#endif

/* We can enable the Redis defrag capabilities only if we are using Jemalloc
 * and the version used is our special version modified for Redis having
 * the ability to return per-allocation fragmentation hints. */
#if (defined(USE_JEMALLOC) && defined(JEMALLOC_FRAG_HINT)) || defined(DEBUG_DEFRAG_FORCE)
#define HAVE_DEFRAG
#endif

/* We can enable allocation with usable size capabilities only if we are using Jemalloc
 * and the version used is our special version modified for Redis having
 * the ability to return usable size during allocation or deallocation. */
#if defined(USE_JEMALLOC) && defined(JEMALLOC_ALLOC_WITH_USIZE)
#define HAVE_ALLOC_WITH_USIZE
#endif

#include <time.h>

/* 'noinline' attribute is intended to prevent the `-Wstringop-overread` warning
 * when using gcc-12 later with LTO enabled. It may be removed once the
 * bug[https://gcc.gnu.org/bugzilla/show_bug.cgi?id=96503] is fixed. */
__attribute__((malloc,alloc_size(1),noinline)) void *zmalloc(size_t size);
__attribute__((malloc,alloc_size(1),noinline)) void *zcalloc(size_t size);
__attribute__((malloc,alloc_size(1,2),noinline)) void *zcalloc_num(size_t num, size_t size);
__attribute__((alloc_size(2),noinline)) void *zrealloc(void *ptr, size_t size);
__attribute__((malloc,alloc_size(1),noinline)) void *ztrymalloc(size_t size);
__attribute__((malloc,alloc_size(1),noinline)) void *ztrycalloc(size_t size);
__attribute__((alloc_size(2),noinline)) void *ztryrealloc(void *ptr, size_t size);
void zfree(void *ptr);
void zfree_usable(void *ptr, size_t *usable);
void zfree_with_size(void *ptr, size_t size);
void *zmalloc_usable(size_t size, size_t *usable);
void *zcalloc_usable(size_t size, size_t *usable);
void *zrealloc_usable(void *ptr, size_t size, size_t *usable, size_t *old_usable);
void *ztrymalloc_usable(size_t size, size_t *usable);
void *ztrycalloc_usable(size_t size, size_t *usable);
void *ztryrealloc_usable(void *ptr, size_t size, size_t *usable, size_t *old_usable);
__attribute__((malloc)) char *zstrdup(const char *s);
__attribute__((malloc)) char *zstrdup_usable(const char *s, size_t *usable);
size_t zmalloc_used_memory(void);
void zmalloc_reserve_thread_slots(int n);
void zmalloc_register_reserved_slot(void);
size_t zmalloc_get_peak_memory(void);
time_t zmalloc_get_peak_memory_time(void);
void zmalloc_set_oom_handler(void (*oom_handler)(size_t));
size_t zmalloc_get_rss(void);
int zmalloc_get_allocator_info(int refresh_stats, size_t *allocated, size_t *active, size_t *resident,
                               size_t *retained, size_t *muzzy, size_t *frag_smallbins_bytes);
int zmalloc_get_allocator_info_by_arena(unsigned int arena, int refresh_stats, size_t *allocated,
                                        size_t *active, size_t *resident, size_t *frag_smallbins_bytes);
void set_jemalloc_bg_thread(int enable);
int jemalloc_purge(void);
size_t zmalloc_get_private_dirty(long pid);
size_t zmalloc_get_smap_bytes_by_field(char *field, long pid);
size_t zmalloc_get_memory_size(void);
void zlibc_free(void *ptr);
void zmadvise_dontneed(void *ptr);

#if defined(USE_JEMALLOC)
void *zmalloc_with_flags(size_t size, int flags);
void *zrealloc_with_flags(void *ptr, size_t size, int flags);
void zfree_with_flags(void *ptr, int flags);
void zfree_with_size_and_flags(void *ptr, size_t size, int flags);
#endif

/* ---- SNAP_NOFORK arena (hybrid BGSAVE, see snapshot.c) ----------------------
 *
 * A dedicated jemalloc arena whose data extents are carved out of ONE reserved
 * virtual address range that is madvise(MADV_DONTFORK)ed, so a fork child never
 * maps it. Because the range is contiguous, "does this pointer belong to the
 * arena" is a two-compare range check (zmalloc_is_nofork), and the generic
 * zfree()/zrealloc() paths use it to keep arena memory in the arena: a block
 * allocated in the arena stays there across realloc and is never recycled
 * through a default-arena thread cache. New allocations go to the arena only
 * through the explicit zmalloc_nofork*() entry points. */
#if defined(USE_JEMALLOC) && defined(__linux__)
#define HAVE_NOFORK_ARENA 1
extern char *zmalloc_nofork_lo;   /* [lo, hi) reserved range; both NULL when unavailable. */
extern char *zmalloc_nofork_hi;
extern unsigned zmalloc_nofork_arena;   /* jemalloc arena index, UINT_MAX until created */
/* hi is NULL while there is no arena: one load and one compare decide. */
static inline int zmalloc_is_nofork(const void *p) {
    return (const char *)p < zmalloc_nofork_hi && (const char *)p >= zmalloc_nofork_lo;
}
static inline int zmalloc_nofork_enabled(void) { return zmalloc_nofork_arena != UINT_MAX; }
int zmalloc_nofork_arena_init(void);                 /* 1 on success, 0 if unavailable */
int zmalloc_nofork_arena_available(void);
int zmalloc_nofork_arena_set_forkable(int forkable); /* MADV_DOFORK (1) / MADV_DONTFORK (0) on the range; 1 on success */
int zmalloc_nofork_arena_forkable(void);             /* 1 while a fork child would map the arena */
unsigned zmalloc_nofork_arena_index(void);           /* UINT_MAX if unavailable */
/* The arena entry points: with no arena they ARE the default allocators (one
 * predictable branch, inlined), so a server with bgsave-hybrid off pays nothing. */
__attribute__((malloc)) void *zmalloc_nofork_slow(size_t size);
__attribute__((malloc)) void *zcalloc_nofork_slow(size_t size);
void *zmalloc_nofork_usable_slow(size_t size, size_t *usable);
static inline void *zmalloc_nofork(size_t size) {
    return __builtin_expect(zmalloc_nofork_enabled(), 0) ? zmalloc_nofork_slow(size) : zmalloc(size);
}
static inline void *zcalloc_nofork(size_t size) {
    return __builtin_expect(zmalloc_nofork_enabled(), 0) ? zcalloc_nofork_slow(size) : zcalloc(size);
}
static inline void *zmalloc_nofork_usable(size_t size, size_t *usable) {
    return __builtin_expect(zmalloc_nofork_enabled(), 0) ? zmalloc_nofork_usable_slow(size, usable) : zmalloc_usable(size, usable);
}
void *zrealloc_nofork(void *ptr, size_t size);
void zfree_nofork(void *ptr);
void zfree_nofork_size(void *ptr, size_t size);
__attribute__((malloc)) void *zmalloc_nofork_no_tcache(size_t size);
void *ztrycalloc_nofork(size_t size);           /* NULL on OOM */
void zmalloc_nofork_va_stats(size_t *reserved, size_t *used);
/* Allocate 'size' bytes in the same arena as 'like' (default or SNAP_NOFORK),
 * for containers that replace a buffer instead of reallocating it in place. */
static inline void *zmalloc_usable_like(const void *like, size_t size, size_t *usable) {
    return zmalloc_is_nofork(like) ? zmalloc_nofork_usable(size, usable) : zmalloc_usable(size, usable);
}
#else
static inline int zmalloc_is_nofork(const void *p) { (void)p; return 0; }
static inline void *zmalloc_usable_like(const void *like, size_t size, size_t *usable) {
    (void)like;
    return zmalloc_usable(size, usable);
}
/* No arena on this platform: the SNAP_NOFORK entry points are the defaults. */
#define zmalloc_nofork(size) zmalloc(size)
#define zcalloc_nofork(size) zcalloc(size)
#define zmalloc_nofork_usable(size, usable) zmalloc_usable(size, usable)
#define ztrycalloc_nofork(size) ztrycalloc(size)
static inline int zmalloc_nofork_enabled(void) { return 0; }
#endif

#if (defined(USE_JEMALLOC) && defined(HAVE_DEFRAG))
void zfree_no_tcache(void *ptr);
__attribute__((malloc)) void *zmalloc_no_tcache(size_t size);
#endif

#ifndef HAVE_MALLOC_SIZE
size_t zmalloc_size(void *ptr);
size_t zmalloc_usable_size(void *ptr);
#else
/* If we use 'zmalloc_usable_size()' to obtain additional available memory size
 * and manipulate it, we need to call 'extend_to_usable()' afterwards to ensure
 * the compiler recognizes this extra memory. However, if we use the pointer
 * obtained from z[*]_usable() family functions, there is no need for this step. */
#define zmalloc_usable_size(p) zmalloc_size(p)

/* derived from https://github.com/systemd/systemd/pull/25688
 * We use zmalloc_usable_size() everywhere to use memory blocks, but that is an abuse since the
 * malloc_usable_size() isn't meant for this kind of use, it is for diagnostics only. That is also why the
 * behavior is flaky when built with _FORTIFY_SOURCE, the compiler can sense that we reach outside
 * the allocated block and SIGABRT.
 * We use a dummy allocator function to tell the compiler that the new size of ptr is newsize.
 * The implementation returns the pointer as is; the only reason for its existence is as a conduit for the
 * alloc_size attribute. This cannot be a static inline because gcc then loses the attributes on the function.
 * See: https://gcc.gnu.org/bugzilla/show_bug.cgi?id=96503 */
__attribute__((alloc_size(2),noinline)) void *extend_to_usable(void *ptr, size_t size);
#endif

int get_proc_stat_ll(int i, long long *res);

#ifdef REDIS_TEST
int zmalloc_test(int argc, char **argv, int flags);
#endif

#endif /* __ZMALLOC_H */
