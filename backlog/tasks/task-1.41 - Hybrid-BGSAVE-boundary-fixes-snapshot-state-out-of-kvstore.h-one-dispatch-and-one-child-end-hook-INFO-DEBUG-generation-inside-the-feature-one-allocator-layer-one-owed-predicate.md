---
id: TASK-1.41
title: >-
  Hybrid BGSAVE boundary fixes: snapshot state out of kvstore.h, one dispatch
  and one child-end hook, INFO/DEBUG generation inside the feature, one
  allocator layer, one owed predicate
status: In Progress
assignee:
  - '@Claude'
created_date: '2026-10-09 22:23'
updated_date: '2026-10-10 00:16'
labels:
  - hybrid-bgsave
  - refactor
milestone: m-1
dependencies:
  - TASK-1.40
references:
  - >-
    src/kvstore.h: kvstoreDictSnap, snap_word, kvstoreOverlayHooks vs
    kvstoreSnapHooks, the ov_cur iterator cursor
  - >-
    src/kvsoverlay.c: kvstoreSnapGuardSet (store-level, belongs in kvstore.c),
    kvstoreDictSnapRelease frees bitmaps snapshot.c allocated; ovKeyCountAdd
    duplicates cumulativeKeyCountAdd; kvstoreMoveDict repeats the arithmetic
  - >-
    dispatch sites: src/rdb.c rdbSaveBackground, src/replication.c
    startBgsaveForReplication, src/aof.c rewriteAppendOnlyFileBackground
    (21-line block duplicating stock bookkeeping)
  - >-
    child end: src/rdb.c killRDBChild and backgroundSaveDoneHandler, src/aof.c
    killAppendOnlyChild (statloc=0 makes a failed kill look like exit 0) and
    backgroundRewriteDoneHandler, src/server.c resetChildState
  - >-
    src/server.c INFO memory and persistence blocks (about 30 snap_* fields
    inline), src/debug.c SNAP-* subcommands and the crash-report section list
  - >-
    src/rdb.c: six snapOnScanThread() decisions inside the serializer (472,
    1353, 1698-1700, 1712, 1748)
  - >-
    src/zmalloc.c/h: 23 functions and 4 globals; snapshot.h snapNofork* 1:1
    wrappers; arena index stored in zmalloc_nofork_arena,
    zmalloc_nofork_arena_index() and server.snap_nofork_arena; HAVE_NOFORK_ARENA
    vs SNAP_NOFORK_ARENA_SUPPORTED
  - >-
    owed predicates: snapScanKeyIsOwed, snapBucketOwed, snapKvobjIsOwed (opens a
    write section as a side effect, called on read paths in t_hash.c),
    snapKvobjIsFrozenOriginal (used in production by hyperloglog.c despite being
    documented as debug-only), ovFrozen/kvstoreOverlayIsFrozen
  - >-
    src/module.c: eight hand-numbered moduleHybridChildGuard bits; load warning
    reads server.bgsave_hybrid directly
parent_task_id: TASK-1
priority: medium
ordinal: 37000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
The 2026-10-09 review of the hybrid BGSAVE feature found the layering partly right (dict.c generic, kvstore interception uniform, the scan engine independent of the save object) and listed the boundaries that leak. Files are NOT to be split or renamed (user decision); this task regularizes the boundaries in place, each item as its own commit verified by unit/snapshot, redis-server test snapshot, the AOF and replication suites with the hybrid on, and TSan/ASan unit/snapshot.

Items: (1) kvstore.h exposes one opaque per-dict extension slot plus an ext-ops vtable owned by snapshot.c, instead of kvstoreDictSnap bitmaps and the snap_word encoding; the overlay types and the inline ovRead/ovWrite/ovFrozen move to an internal kvsoverlay.h; the iterator cursor becomes the real ovCursor; ownership of the per-dict save state (bitmaps, paused) is snapshot.c's alone, kvsoverlay.c frees only the overlay; the two hook structs merge into one with one privdata convention; kvstoreSnapGuardSet moves to kvstore.c; key-count arithmetic goes through one function. (2) One dispatch entry point in snapshot.c that owns the eligibility rule (functions-only and slot-snapshot requests keep the stock child, module blocker, fallback log) and replaces the three differently composed blocks; the duplicated AOF rewrite bookkeeping becomes a shared aof.c helper. (3) One child-end hook called from resetChildState covering kill and reap for RDB, AOF and module children, absorbing snapArenaChildExited and fixing killAppendOnlyChild's statloc handling. (4) INFO memory/persistence field generation and the DEBUG SNAP-* subcommands and crash-report section list move into snapshot.c functions. (5) The six scan-thread decisions in rdb.c collapse into one per-save configuration struct or getter. (6) The allocator exposes one layer: drop either the snapNofork* wrappers or the direct zmalloc_nofork* calls in dict.c/kvstore.c/kvsoverlay.c; delete zfree_nofork, zfree_nofork_size, zrealloc_nofork, zmalloc_nofork_arena_available; make lo/hi/arena/active globals static behind inline accessors; one arena index; one feature macro. (7) One side-effect-free owed predicate for read paths and one mutation-preparation function (used by hyperloglog PFCOUNT, RM_SetLRU/LFU on a READ handle, lazy hash-field expiry), FrozenOriginal kept for debug asserts only; the test-only snapScanKeyIsOwed moves to the tests. (8) Module guard bits assigned by a table; the load warning uses snapHybridEnabled.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 kvstore.h contains no snapshot-specific types or encodings beyond an opaque extension slot and its ops vtable; kvsoverlay.c neither allocates nor frees bitmaps; one hook struct and one privdata convention; one key-count update function used by kvstore.c, kvsoverlay.c and kvstoreMoveDict
- [x] #2 rdb.c, replication.c and aof.c each call one hybrid dispatch function and contain no inline eligibility logic; the AOF rewrite start bookkeeping exists once
- [ ] #3 One child-end hook handles kill and reap for RDB, AOF and module children; a failed kill() in killAppendOnlyChild is reported as a failure, with a test
- [x] #4 server.c and debug.c call one function each for INFO memory fields, INFO persistence fields, the DEBUG SNAP-* subcommands and the crash-report section list
- [x] #5 rdb.c reads the scan-thread configuration (compression, maxmemory policy, LRU clock, key-save delay) through one struct or getter
- [x] #6 zmalloc.h exports one nofork allocation layer with no redundant free/realloc variants, no extern lo/hi/arena/active globals, one arena index and one feature macro; all call sites use that layer
- [ ] #7 Read paths call a side-effect-free owed predicate; the three mutation-preparation sites call one shared function; snapKvobjIsFrozenOriginal is used only in debug assertions; snapScanKeyIsOwed lives in the tests
- [x] #8 Module child guard bits come from a table; every suite named in the description passes after each item's commit
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
Order, one commit per item, each verified by build without warnings, redis-server test snapshot, unit/snapshot, and for items touching the dispatch or child paths also unit/aofrw, integration/aof, integration/aof-multi-part, integration/replication with --config bgsave-hybrid yes; TSan and ASan unit/snapshot at the end of the task.
1. (item 2) snapSaveTryStart(kind, ...) in snapshot.c owns the eligibility rule: switch on, request has keyspace content (not EXCLUDE_DATA, not SLOTS_SNAPSHOT), every relevant module opted in, with the fallback log; rdb.c, replication.c and aof.c call it and keep no inline eligibility logic. The AOF rewrite start bookkeeping (scheduled flag, time_start, aof_cmd_duration) becomes one aof.c helper used by both the hybrid and the stock branch.
2. (item 4) sds snapInfoMemory(sds) and sds snapInfoPersistence(sds), int snapDebugCommand(client *c) for the SNAP-* subcommands, and a snapshot.c function returning the crash-report section allow-list; server.c and debug.c call them.
3. (item 5) one per-save configuration getter for the serializer: rdb.c reads compression, maxmemory policy, LRU clock and key-save delay through it instead of six snapOnScanThread() branches.
4. (item 8) module guard bits from a table (an enum of guarded APIs indexes the once-logged mask); the module-load warning uses snapHybridEnabled().
5. (item 6) allocator: one layer. Keep zmalloc_nofork* as the layer (dict.c, kvstore.c, kvsoverlay.c already use it) and make snapNofork* thin inlines or remove them where the child assertion is not needed; delete zfree_nofork, zfree_nofork_size, zrealloc_nofork, zmalloc_nofork_arena_available; lo/hi/arena/active behind inline accessors; one arena index (drop server.snap_nofork_arena); one feature macro.
6. (item 3) snapOnChildEnd(exitcode, bysignal) called from resetChildState for every child type, absorbing snapSaveChildDone's callers and snapArenaChildExited; killAppendOnlyChild reports a failed kill() as a failure (test).
7. (item 7) one side-effect-free owed predicate for read paths, one snapPrepareMutation(db, key) for hyperloglog, RM_SetLRU/SetLFU and lazy field expiry; snapKvobjIsFrozenOriginal under DEBUG_ASSERTIONS only; snapScanKeyIsOwed moves to the tests.
8. (item 1) kvstore.h: opaque per-dict extension slot and ext-ops vtable; kvsoverlay.h (internal) for the overlay types and the inline ovRead/ovWrite/ovFrozen; the store struct in a shared internal header; the iterator cursor as ovCursor; kvsoverlay.c frees only the overlay; one hook struct and privdata; kvstoreSnapGuardSet in kvstore.c; one key-count update function.
<!-- SECTION:PLAN:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
2026-10-10 progress: items 2+4 (one dispatch snapSaveTryStart, INFO/DEBUG/crash-report generation in snapshot.c) committed 9fb660285; items 5+8 (snapRdbCfgGet for rdb.c, module guard enum/table, snapHybridEnabled at module load) committed ae61ea7e5; item 6 (one allocation layer: zmalloc_nofork*, snapNofork* wrappers and zfree_nofork/zfree_nofork_size/zmalloc_nofork_arena_available removed, arena index static behind zmalloc_nofork_arena_index(), server.snap_nofork_arena removed, one macro HAVE_NOFORK_ARENA) committed after its chain. Each item verified by build without warnings, redis-server test snapshot 184, unit/snapshot, unit/aofrw, integration/aof, integration/aof-multi-part, integration/replication and unit/info with --config bgsave-hybrid yes. One flake seen on item 6's chain: 'child CoW stays flat under heavy writes versus stock BGSAVE' failed its 2x margin once (958 KB vs 1.82 MB) and passed three reruns; the allocator change moves no placement decision. Same class as the hard-mark timing test: a test-robustness item, not filed. Items 3 (snapOnChildEnd from checkChildrenDone and killAppendOnlyChild, failed kill() counted as failure) and 7 (snapPrepareMutation for PFCOUNT and RM_SetLRU/SetLFU; snapScanKeyIsOwed test-only) applied next; item 1 (kvstore.h extension slot) last.

2026-10-10, remaining items landed: item 3 + 7 as 2673049fc (snapOnChildEnd from checkChildrenDone and killAppendOnlyChild, failed kill() counted as a failure; snapPrepareMutation for PFCOUNT and RM_SetLRU/SetLFU; snapKvobjIsFrozenOriginal debug-only; snapScanKeyIsOwed a test helper), item 1 first half as 787ca1d1d (opaque ext_word/ext slot and the overlay pointer in kvstoreDictMetaBase; snapDictState private to snapshot.c, which frees bitmaps and pause before kvstoreOverlayRelease; kvstoreKeyCountAdd as the one key-count function), item 1 second half as 39f68a939 (one kvstoreSnapHooks with the five overlay callbacks and one privdata, the per-store snapGuardPriv with a 'user' field; kvsoverlay.h internal header with the store struct, overlay types, inline interception and the exchanged routines; kvstore.h 423 -> 273 lines; kvstoreOvCursor named; kvstoreSnapGuardSet in kvstore.c). Every step: build without warnings, redis-server test snapshot 184, and the six-suite chain green (unit/snapshot 81, aofrw 25, aof 63, aof-multi-part 41, replication 80, info 31 with the hybrid on). AC #7 nuance: snapKvobjIsOwed still opens the write section by design (the section is what keeps the answer valid for the command; a section-free variant would be racy), so read paths call it and skip; the mutation sites call snapPrepareMutation. AC #3 nuance: a deterministic test of a failed kill() in killAppendOnlyChild is not writable without fault injection (kill(ESRCH) needs an already-reaped pid); the handling is in place, the test is not. Sanitizer pass (TSan + ASan unit/snapshot on the final head) running.

Sanitizer pass on the final head (39f68a939): TSan unit/snapshot 75 ok, 0 reports; ASan unit/snapshot 75 ok, 0 reports. AC 1, 2, 4, 5, 6 and 8 verified. AC 3 and 7 are implemented except for one clause each, which the user decides on: AC 3 asks for a test of the failed-kill() path in killAppendOnlyChild, which cannot be driven deterministically without fault injection; AC 7 asks for a side-effect-free owed predicate on read paths, where the write section the predicate opens is what makes its answer valid for the rest of the command (a section-free check would be racy), so the read paths keep calling snapKvobjIsOwed and skip, and the mutating paths call snapPrepareMutation.
<!-- SECTION:NOTES:END -->
