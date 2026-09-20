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
 * db kvstores). On platforms without jemalloc or MADV_DONTFORK this is a
 * no-op and snapArenaAvailable() returns 0. */
void snapArenaInit(void);

/* 1 if the DONTFORK arena exists and the hybrid save path may be used. */
int snapArenaAvailable(void);

/* ---- Fast allocation API for SNAP_NOFORK memory -------------------------- */

/* All of these keep zmalloc's used_memory accounting. They select the arena
 * with MALLOCX_ARENA and a per-thread explicit tcache, so blocks released
 * here can only ever be handed back to SNAP_NOFORK allocations (jemalloc
 * thread caches are not arena-aware, hence plain zfree() must never be used
 * on SNAP_NOFORK memory). When the arena is unavailable they degrade to
 * zmalloc/zrealloc/zfree. Calling any of them from a fork child panics. */
void *snapNoforkAlloc(size_t size);
void *snapNoforkAllocUsable(size_t size, size_t *usable);
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

/* 1 if 'ptr' was allocated from the SNAP_NOFORK arena. Costs an allocator
 * metadata lookup; intended for assertions and tests, not hot paths. 'ptr'
 * must be a live allocator pointer (heap memory). */
int snapPtrIsNofork(const void *ptr);

/* ---- Tests --------------------------------------------------------------- */
#ifdef REDIS_TEST
int snapshotTest(int argc, char *argv[], int flags);
#endif

#endif /* __SNAPSHOT_H */
