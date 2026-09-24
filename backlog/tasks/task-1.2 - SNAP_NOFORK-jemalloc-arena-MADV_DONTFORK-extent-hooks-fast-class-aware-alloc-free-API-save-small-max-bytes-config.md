---
id: TASK-1.2
title: >-
  SNAP_NOFORK jemalloc arena: MADV_DONTFORK extent hooks, fast class-aware
  alloc/free API, save-small-max-bytes config
status: Done
assignee:
  - moti.cohen@redis.com
created_date: '2026-09-20 17:18'
updated_date: '2026-09-20 19:05'
labels:
  - bgsave
  - persistence
  - allocator
  - jemalloc
milestone: m-0
dependencies: []
documentation:
  - poc-hybrid-bgsave-spec.md
parent_task_id: TASK-1
priority: high
ordinal: 3000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Spec §2: all kvobj headers, SNAP_NOFORK values and all kvstore hash-table structures must live in memory the fork child never maps. Provide, in src/snapshot.c (functions prefixed 'snap'), a dedicated jemalloc arena whose extents are madvise(MADV_DONTFORK)ed, plus a fast allocation API so the hot object-creation and free paths can target it explicitly.

Scope: create the arena at startup with custom extent hooks that apply MADV_DONTFORK on extent alloc and re-apply it on commit (default commit can drop the flag); handle split/merge/decommit/purge so the flag stays correct. Expose a distinct API (snapNoforkAlloc / snapNoforkRealloc / snapNoforkFree / usable-size, and a predicate 'is this pointer in the SNAP_NOFORK arena' for assertions) built on mallocx/rallocx/sdallocx with MALLOCX_ARENA and a dedicated per-thread tcache, not on switching the thread's default arena. Consider static preallocation of hot structures if it measurably helps. Add the 'save-small-max-bytes' config (default 4096) used by snapClassify. Account the arena in INFO memory with dedicated allocated/resident stats.

free() is the hard part and must be designed here: jemalloc thread caches are not arena-aware, so a SNAP_NOFORK pointer released through plain free()/zfree() lands in the default tcache and can be handed back to a SNAP_FORK allocation, silently placing forkable-class data in DONTFORK memory. SNAP_NOFORK frees must go through sdallocx with the arena's own tcache (or MALLOCX_TCACHE_NONE), which requires callers to know the class. Provide class-aware free wrappers for the value buffer types (sds, listpack, intset, listpackEx) and document how callers obtain the class (the optional kvobj class bit from TASK-1.4, or the caller's context). Measure alloc/free cost versus zmalloc on a microbenchmark and record it.

Constraints: MADV_DONTFORK is Linux-only; on other platforms and non-jemalloc builds the API must degrade to normal allocation and report the hybrid path unavailable so stock BGSAVE is used. Each madvise on a non-VMA-aligned range splits VMAs; check the process VMA count against vm.max_map_count under a large dataset. Verify the flag applies to THP-backed extents. Only the parent may touch this arena; add a cheap child-side assertion hook (a flag set after fork that makes SNAP_NOFORK alloc/free abort in the child).
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 A SNAP_NOFORK arena is created at startup on Linux+jemalloc builds and every extent it maps is MADV_DONTFORK, verified by a test that forks and confirms the child faults (or sees the range unmapped) when touching a SNAP_NOFORK allocation
- [x] #2 The flag survives extent commit/decommit/purge/split/merge cycles, verified by an allocation stress test followed by a fork check
- [x] #3 snapNoforkAlloc/Realloc/Free and usable-size exist in snapshot.c, select the arena via MALLOCX_ARENA with a dedicated tcache, and a microbenchmark comparing them to zmalloc/zfree is recorded in task notes
- [x] #4 A SNAP_NOFORK pointer freed through the class-aware free path never re-enters a default-arena tcache, verified by a test that frees SNAP_NOFORK blocks then allocates default blocks and asserts none land in the arena
- [x] #5 Class-aware free wrappers exist for sds, listpack, intset and listpackEx value buffers
- [x] #6 A predicate exists to test whether a pointer belongs to the SNAP_NOFORK arena, usable in assertions
- [x] #7 Config 'save-small-max-bytes' exists with default 4096, is documented in redis.conf, and is settable via CONFIG SET
- [x] #8 INFO memory reports allocated and resident bytes for the SNAP_NOFORK arena
- [x] #9 On non-Linux platforms or non-jemalloc builds the API falls back to the default allocator and a runtime flag reports the hybrid path as unavailable
- [x] #10 VMA count under a dataset of at least 10M small keys stays well under the default vm.max_map_count, with the measured value recorded in task notes
- [x] #11 Existing tcl test suite passes with the arena present but unused by the keyspace
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
## Implementation plan (TASK-1.2)

Branch: snapshot-hybrid-bgsave (off unstable). Local commits only, no push.

### Design
- New files src/snapshot.h / src/snapshot.c (added to REDIS_SERVER_OBJ in src/Makefile). All functions prefixed `snap`.
- Build guard: `SNAP_NOFORK_ARENA_SUPPORTED` defined when USE_JEMALLOC && __linux__ && MADV_DONTFORK. Otherwise stubs: snapNoforkAlloc→zmalloc, snapNoforkFree→zfree, snapArenaAvailable()→0, snapPtrIsNofork()→0.
- Arena creation (snapArenaInit, called from initServer before db kvstores are created): read default hooks via mallctl "arena.0.extent_hooks"; build a static extent_hooks_t that wraps the defaults: alloc → default alloc then madvise(MADV_DONTFORK); commit → default commit then re-apply madvise; dalloc/destroy/decommit/purge_lazy/purge_forced/split/merge → pass through to defaults (VMA flags survive purge/split/merge; decommit+commit path re-applies in commit). Create the arena with mallctl "arenas.create" passing &hooks. Store index in server.snap_nofork_arena (UINT_MAX when unavailable), mirror of server.lua_arena pattern in script.c.
- Fast API: per-thread explicit tcache created lazily (`tcache.create`) and cached in a __thread flags word: flags = MALLOCX_ARENA(arena) | MALLOCX_TCACHE(tc); fallback MALLOCX_TCACHE_NONE if creation fails. Explicit tcaches are single-thread-at-a-time per jemalloc docs, hence per-thread. Because only SNAP_NOFORK allocations ever use these tcaches, freed NOFORK blocks can only be re-handed to NOFORK allocations (this is the fix for the tcache leak into default-arena allocations).
  - snapNoforkAlloc(size) → zmalloc_with_flags(size, flags) (keeps zmalloc used_memory accounting)
  - snapNoforkRealloc(ptr,size) → zrealloc_with_flags
  - snapNoforkFree(ptr) → zfree_with_flags
  - snapNoforkFreeSize(ptr, usable) → new zfree_with_size_and_flags() added to zmalloc.c (sdallocx fast path)
  - snapNoforkUsableSize(ptr) → zmalloc_size
  - snapPtrIsNofork(ptr) → mallctl "arenas.lookup" via precomputed MIB; returns arena == snap arena.
  - Class-aware free wrappers for value buffers: snapSdsFree(sds), snapLpFree(lp), snapIntsetFree(is), snapListpackExFree(lpt) (frees lpt->lp then lpt).
  - Child guard: every alloc/free checks `server.in_fork_child != CHILD_TYPE_NONE` and panics (one load+branch on hot path). No separate flag needed.
  - snapArenaGetInfo(...) wraps zmalloc_get_allocator_info_by_arena.
- Config: `save-small-max-bytes` (createSizeTConfig, MEMORY_CONFIG, default 4096, range 0..LONG_MAX) → server.save_small_max_bytes; documented in redis.conf SNAPSHOTTING section after rdb-del-sync-files.
- INFO memory: new malloc_stats fields nofork_allocator_{allocated,active,resident,frag_smallbins_bytes} sampled in cronUpdateMemoryStats next to lua; emitted in INFO memory as allocator_allocated_nofork / allocator_active_nofork / allocator_resident_nofork / allocator_frag_bytes_nofork, plus `snap_nofork_arena_available:0|1`.

### Tests
- C unit test `snapshotTest` (REDIS_TEST, registered in redisTests[] in server.c, run via `./src/redis-server test snapshot`):
  1. arena created; snapPtrIsNofork true for arena ptr, false for zmalloc ptr.
  2. fork child writes to an arena pointer → waitpid shows SIGSEGV/SIGBUS; child writing a zmalloc pointer exits 0.
  3. stress: many alloc/free cycles of mixed sizes (incl. >2MB large) + `arena.<i>.purge` + `arena.<i>.decay` to exercise purge/decommit/split/merge, then repeat the fork check on a sample of live pointers.
  4. tcache isolation: free N NOFORK blocks, then zmalloc N blocks of the same size class; assert none is in the arena.
  5. microbenchmark: alloc+free 1M x 64B via zmalloc/zfree vs snapNoforkAlloc/Free; print ns/op (recorded in notes).
  6. VMA proxy: allocate 10M x ~100B arena blocks, count lines of /proc/self/maps before/after; print (recorded in notes; full keyspace measurement re-done in TASK-1.5).
- tcl: tests/unit/snapshot.tcl — CONFIG GET/SET save-small-max-bytes (default 4096, accepts "8kb"), INFO memory contains the nofork fields and availability flag.
- Full tcl suite run once with the change (arena unused by keyspace).

### Files
src/snapshot.h, src/snapshot.c (new); src/Makefile; src/zmalloc.c/.h (zfree_with_size_and_flags); src/server.h (arena idx, config var, malloc_stats fields, snapshotTest proto); src/server.c (initServer call, cron stats, INFO, redisTests entry); src/config.c; redis.conf; tests/unit/snapshot.tcl (new).

### Risks / checks
- jemalloc default hooks are stateless w.r.t. the hooks pointer (they use arena_ind), so wrapping is safe in 5.3.
- Explicit tcache leak for short-lived threads: acceptable (server threads are long-lived); documented.
- THP: MADV_DONTFORK is a VMA flag so it covers huge pages too; verified by the fork test when THP is enabled on the box.
<!-- SECTION:PLAN:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
2026-09-20 implementation notes (TASK-1.2)

Key finding: `arenas.create` allocates the new arena's own metadata (arena_t, bins, mutexes, edata records) THROUGH the arena's extent hooks. With MADV_DONTFORK hooks that metadata became DONTFORK and jemalloc's post-fork child handler (arena_postfork_child touching the arena mutexes) crashed every fork child with SIGSEGV, before the child executed anything. First test version masked this: 'child faults' passed for the wrong reason and 'child can write zmalloc memory' failed. Fix: create the arena with `experimental.arenas_create_ext` and `arena_config_t{extent_hooks, metadata_use_hooks=false}` (jemalloc 5.3), so metadata stays in forkable mappings while data extents are DONTFORK. The test now requires the child to signal over a pipe that it reached the write, and separately proves the allocator works in a child and checks /proc/self/smaps VmFlags for 'dc'.

Hooks: copy of the default extent_hooks_t with alloc and commit wrapped (madvise MADV_DONTFORK after both). Defaults ignore the hooks pointer and key off arena_ind, so pass-through is safe. NULL default hooks (purge_lazy/purge_forced when unsupported) are preserved by copying the struct.

free() path: per-thread explicit tcache (`tcache.create`) cached in a __thread flags word; only SNAP_NOFORK requests use it, so freed NOFORK blocks cannot be recycled into default-arena allocations (tested both directions with 2000 x 64B blocks: 0 leaks). Explicit tcaches are single-thread-at-a-time per jemalloc docs; the tcache is never destroyed (Redis threads are long-lived).

Microbenchmark (this box, 16 cores, gcc 11, -O3 LTO, N=5M, 64B blocks, accurate run):
- zmalloc/zfree pair: 6.0 ns; snapNoforkAlloc/Free pair: 16.9 ns
- bulk (5M live then free all): zmalloc 18.2 ns/op; snapNofork 28.1 ns/op; snapNofork with sized free 27.7 ns/op
A 1M run gave 7.2/20.1 and 21.9/33.7/24.6. So roughly +10 ns per alloc+free pair versus the je_malloc fast path, because mallocx with dynamic flags bypasses jemalloc's static fast path and does an explicit tcaches_get. At ~1M keyspace ops/s this is on the order of 1% CPU. Options if it matters later: (a) make the main thread's default arena the NOFORK arena via `thread.arena` and pay the mallocx cost on SNAP_FORK allocations instead (rejected for now: every main-thread zmalloc, including Lua/function state the child must read, would become DONTFORK); (b) sized frees everywhere the usable size is known (kvobj, listpack). Static preallocation would not help: the cost is in flag dispatch, not in mapping.

VMA count: /proc/self/maps lines before=45, after 10,000,000 x 96B SNAP_NOFORK allocations=45 (delta 0). jemalloc grows the arena in large retained extents that are adjacent and carry identical flags, so the kernel merges them; MADV_DONTFORK splitting is not a concern. vm.max_map_count on this box is 65530.

THP: this box has transparent_hugepage=madvise and jemalloc opt.thp=default, so THP-backed extents were not exercised. VM_DONTCOPY is a VMA flag, so it applies to huge pages too; to be re-checked on a THP=always host during TASK-1.10 (d).

Test notes: the tcl harness cannot run inside the Claude Code sandbox (Unix socket creation is denied), tcl runs were done with the sandbox disabled. Unit test: `./src/redis-server test snapshot [--accurate] [--large-memory]` (REDIS_TEST build). 23/23 pass. tcl: `./runtest --single unit/snapshot` passes.

Full tcl suite (./runtest --clients 16) with the arena present and unused by the keyspace: 5564 tests, all passed, exit 0.
<!-- SECTION:NOTES:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
## Summary

Adds the SNAP_NOFORK jemalloc arena, the first building block of the hybrid BGSAVE design, in new files `src/snapshot.c` / `src/snapshot.h` (all functions `snap`-prefixed). Commit 538391875 on branch `snapshot-hybrid-bgsave` (local only).

### What changed
- **Arena**: created at `initServer()` before the db kvstores via `experimental.arenas_create_ext` with `metadata_use_hooks=false`. Extent hooks are a copy of jemalloc's defaults with `alloc` and `commit` wrapped to (re)apply `madvise(MADV_DONTFORK)`. Data extents are DONTFORK; arena metadata stays forkable.
- **Fast class-aware API**: `snapNoforkAlloc/AllocUsable/Realloc/Free/FreeSize/UsableSize` using `MALLOCX_ARENA` + a lazily created per-thread explicit tcache, keeping zmalloc `used_memory` accounting. Freed SNAP_NOFORK blocks can never be recycled into default-arena allocations. Class-aware free wrappers `snapSdsFree`, `snapLpFree`, `snapIntsetFree`, `snapListpackExFree`. `snapPtrIsNofork()` predicate via `arenas.lookup`. Every entry point panics if called from a fork child (`server.in_fork_child`).
- **Fallback**: on non-Linux or non-jemalloc builds the API degrades to zmalloc and `snapArenaAvailable()` is 0.
- **Config** `save-small-max-bytes` (default 4096, MEMORY units, CONFIG SET-able), documented in redis.conf.
- **INFO memory**: `allocator_allocated_nofork`, `allocator_active_nofork`, `allocator_resident_nofork`, `allocator_frag_bytes_nofork`, `snap_nofork_arena_available`.
- **zmalloc**: `zfree_with_size_and_flags()` for the sized-free fast path.

### Why the ext creation API
`arenas.create` allocates the arena's own metadata through the arena's extent hooks; with DONTFORK hooks jemalloc's post-fork child handler crashed every child. Discovered because the first test version's "child can write zmalloc memory" check failed.

### Tests
- `./src/redis-server test snapshot` (REDIS_TEST build): 23 checks covering arena availability, `/proc/self/smaps` VmFlags `dc`, fork child faulting exactly on DONTFORK writes (pipe handshake) while surviving and allocating normally otherwise, large (5 MB) allocations, realloc, purge/decay/realloc stress with re-checks, tcache isolation in both directions, accounting, microbenchmark, VMA count. Also run with `--accurate --large-memory`.
- `tests/unit/snapshot.tcl`: config default/units and INFO fields.
- Full tcl suite: 5564 passed, 0 failed.

### Measurements (see notes)
- alloc+free 64 B pair: zmalloc 6.0 ns vs snapNofork 16.9 ns; bulk 18.2 vs 28.1 ns/op (27.7 with sized free). About +10 ns per pair, roughly 1% CPU at 1M ops/s.
- 10M x 96 B arena allocations: zero change in `/proc/self/maps` line count.

### Risks / follow-ups
- THP-backed extents not exercised on this host (THP=madvise); VM_DONTCOPY is a VMA flag so it should hold; re-check in TASK-1.10.
- Explicit tcaches are never destroyed (threads are long-lived).
- If the +10 ns matters later, options are recorded in the notes.
<!-- SECTION:FINAL_SUMMARY:END -->
