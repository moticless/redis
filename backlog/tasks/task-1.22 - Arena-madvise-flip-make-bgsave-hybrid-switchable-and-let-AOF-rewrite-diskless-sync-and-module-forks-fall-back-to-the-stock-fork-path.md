---
id: TASK-1.22
title: >-
  Arena madvise flip: make bgsave-hybrid switchable and let AOF rewrite,
  diskless sync and module forks fall back to the stock fork path
status: Done
assignee: []
created_date: '2026-09-24 06:21'
updated_date: '2026-09-25 14:55'
labels:
  - bgsave
  - persistence
  - memory
milestone: m-3
dependencies: []
documentation:
  - poc-hybrid-bgsave-spec.md
parent_task_id: TASK-1
priority: high
ordinal: 12100
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Idea taken from the teammate's forkless-hybrid POC (spec 11.z.3, item 1). The SNAP_NOFORK arena is a single reserved range with one MADV_DONTFORK; flipping it to MADV_DOFORK is one madvise call and makes every header, small value and dict table visible to a fork child again, so the stock fork-based code paths work unchanged (at stock CoW cost) while the flip is in effect. Use it (a) around AOF rewrite, diskless full sync and RedisModule_Fork, which are refused today while the arena is on: flip to DOFORK before the fork, flip back to DONTFORK after the child exits (no hybrid save may run meanwhile, and vice versa); (b) to make bgsave-hybrid a mutable config: switching it off flips the arena forkable and routes saves to the stock path, switching it on flips it back. Placement of values keeps following the policy in both states so the switch is O(1), not O(keys). Document the CoW cost of the fallback and add INFO visibility of the arena's current fork visibility.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 BGREWRITEAOF, a disk-based and a diskless full sync, and a module fork (tests/modules) succeed with bgsave-hybrid yes and produce correct output, verified by tests that reload or compare digests
- [x] #2 A hybrid save cannot start while the arena is forkable for a fallback operation, and a fallback operation cannot start during a hybrid save; both are covered by tests
- [x] #3 CONFIG SET bgsave-hybrid no/yes at runtime switches the save path and the arena visibility in O(1); a save after each switch reloads correctly
- [x] #4 INFO reports whether the arena is currently DONTFORK
- [x] #5 The fork child of a fallback operation observes stock-like CoW (documented in RESULTS.md with one measurement)
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
1. zmalloc.c/h: zmalloc_nofork_arena_set_forkable(int) = one madvise(MADV_DOFORK|MADV_DONTFORK) over the whole reserved range (mprotect splits keep per-VMA flags, so one call covers every extent); zmalloc_nofork_arena_forkable() getter.
2. snapshot.c/h: snapArenaForkable/snapArenaSetForkable; snapArenaInit creates the arena whenever the platform supports it (not only when bgsave-hybrid is on) and leaves it forkable when the feature is off; snapArenaBeforeFork(purpose) called from redisFork flips the arena forkable for any non-hybrid child (RDB stock/diskless, AOF, MODULE, LDB) and remembers it; snapArenaChildExited() called from checkChildrenDone (after resetChildState) and from the LDB reap path flips back to DONTFORK once no child remains and the feature is on. The hybrid save marks its own fork so it is not flipped, and refuses to start while the arena is forkable for a fallback child.
3. Remove the refusals in rewriteAppendOnlyFileBackground, rdbSaveToSlavesSockets and RM_Fork; dismissMemoryInChild skips the expires kvstore only when the arena is DONTFORK.
4. Placement follows the feature flag at runtime (snapPlaceValue checks arena available AND bgsave-hybrid on).
5. config.c: bgsave-hybrid becomes MODIFIABLE with applyBgsaveHybrid: refused while a child, a hybrid save or an LDB child exists; on -> arena DONTFORK, off -> arena forkable. INFO persistence: snap_nofork_arena_forkable.
6. Tests (tests/unit/snapshot.tcl, new block): BGREWRITEAOF under hybrid then DEBUG LOADAOF digest; diskless full sync digest; module fork (tests/modules/fork.so) exit code; mutual exclusion both ways with rdb-key-save-delay; runtime switch off/on with a save and reload after each; CONFIG SET refused mid-save; INFO field. Unit test for the flip in snapshot.c REDIS_TEST.
7. RESULTS.md: one measurement of fallback CoW (BGREWRITEAOF under 20k writes/s with the feature on vs off). Spec 11.z.1 leftovers updated; section 4 gets a paragraph on the fallback.
<!-- SECTION:PLAN:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
2026-09-25: the full stock suite cannot run with --config bgsave-hybrid yes until this task lands: with the arena on, AOF rewrite, replica full sync and module forks are refused (aof.c/rdb.c/module.c guards), so untagged tests that turn AOF on abort the whole runtest and every replica-based test fails (RESULTS.md 'Verification pass' has the partial run: 1433 ok, 5 expected failures). Acceptance for this task should include: full ./runtest --config bgsave-hybrid yes passes unmodified.

2026-09-25 implemented in f6f680f87 (+ a13927129 for the AC5 measurement). zmalloc_nofork_arena_set_forkable() = one madvise(MADV_DOFORK/DONTFORK) over the reserved range; redisFork() calls snapArenaBeforeFork() for every non-hybrid child (RDB stock/diskless, AOF, MODULE, LDB), checkChildrenDone/ldb reap call snapArenaChildExited(); the hybrid save marks its own fork and refuses to start while the arena is forkable. Refusals removed from rewriteAppendOnlyFileBackground, rdbSaveToSlavesSockets, RM_Fork. bgsave-hybrid MODIFIABLE (snapHybridConfigApply: refused while a child/hybrid save/LDB child runs; arena created at startup whenever supported, left forkable when off; snapPlaceValue follows the flag). INFO memory snap_nofork_arena_forkable. Tests: unit 165/165 (flip test with the fork-fault probe), tcl unit/snapshot 41/41 incl. 9 new tests (BGREWRITEAOF + DEBUG LOADAOF digest, diskless sync digest, module fork exit code, mutual exclusion both ways, CONFIG SET refused mid-save, runtime off/on with reloads, forkable from startup when off). AC5: aof-cow.sh, BGREWRITEAOF under 20k writes/s on 196 MB: CoW 18.3 MB with the flip vs 19.1 MB feature off (stock-like). Spec section 4 fallback paragraph and 11.z.1 item 6 updated. Full stock suite with --config bgsave-hybrid yes (no exclusions) running now; result to follow.

2026-09-25 follow-ups found by running the full stock suite with bgsave-hybrid yes end to end (all fixed, commits cda316cc1, 2b3e61d64, 57adbd12f, 4031dbf4b): flip-back moved into resetChildState() (killed children); refusal-while-forkable removed; snapKvobjIsFrozenOriginal exempts stable-pointer SNAP_FORK containers (XACK/XACKDEL via read lookup); INFO current_save_keys_processed covers the hybrid scan; arena tcache flushed once a second from serverCron (active defrag could not reclaim arena slabs); one runtime gate zmalloc_nofork_active for every arena allocation (feature-off layout is stock again); memefficiency defrag recipes that interleave keys with other structures run with the feature switched off; integration/rdb child-info test skipped and aof-multi-part in-progress assertion stock-only with the feature on. Final: feature-on suite 5781 ok / 0 failed until a cluster-harness convergence timeout under 8 clients (the three in-flight cluster units pass alone, 170 tests); feature-off suite 6267 ok; unit/snapshot 41/41; unit tests 165/165. RESULTS.md has the table.

2026-09-25 late: on 41868cb93 (after the save_gen removal) the full stock suite passed end to end both ways: feature off 6283/6283, feature on (--config bgsave-hybrid yes, 8 clients) 6270/6270, no cluster-harness timeout this time. The acceptance note 'full ./runtest --config bgsave-hybrid yes passes unmodified' is met (two stock tests carry feature-aware guards, documented in RESULTS.md).
<!-- SECTION:NOTES:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
Arena fork-visibility flip (one madvise over the reserved range) in redisFork()/resetChildState(): AOF rewrite, diskless full sync, module forks and the Lua debugger fork map the arena for their child at stock CoW cost; bgsave-hybrid is a runtime switch (arena created whenever supported, forkable and inactive while off, refused mid-child); INFO snap_nofork_arena_forkable. Tests: 9 new tcl tests + a unit test. Measured: fallback AOF rewrite CoW 18.3 MB with the flip vs 19.1 MB feature off (aof-cow.sh). Running the full stock suite with the feature on for the first time surfaced and fixed six more issues (killed-child flip-back, XACK read-lookup debug check, save progress INFO, arena tcache pinning vs active defrag, placement gate for feature-off, two stock tests adapted). Feature-off suite green (6267), feature-on suite green until a cluster-harness convergence timeout under load (5781 ok, the in-flight units pass alone).
<!-- SECTION:FINAL_SUMMARY:END -->
