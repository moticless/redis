---
id: TASK-1.41
title: >-
  Hybrid BGSAVE boundary fixes: snapshot state out of kvstore.h, one dispatch
  and one child-end hook, INFO/DEBUG generation inside the feature, one
  allocator layer, one owed predicate
status: To Do
assignee: []
created_date: '2026-10-09 22:23'
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
- [ ] #1 kvstore.h contains no snapshot-specific types or encodings beyond an opaque extension slot and its ops vtable; kvsoverlay.c neither allocates nor frees bitmaps; one hook struct and one privdata convention; one key-count update function used by kvstore.c, kvsoverlay.c and kvstoreMoveDict
- [ ] #2 rdb.c, replication.c and aof.c each call one hybrid dispatch function and contain no inline eligibility logic; the AOF rewrite start bookkeeping exists once
- [ ] #3 One child-end hook handles kill and reap for RDB, AOF and module children; a failed kill() in killAppendOnlyChild is reported as a failure, with a test
- [ ] #4 server.c and debug.c call one function each for INFO memory fields, INFO persistence fields, the DEBUG SNAP-* subcommands and the crash-report section list
- [ ] #5 rdb.c reads the scan-thread configuration (compression, maxmemory policy, LRU clock, key-save delay) through one struct or getter
- [ ] #6 zmalloc.h exports one nofork allocation layer with no redundant free/realloc variants, no extern lo/hi/arena/active globals, one arena index and one feature macro; all call sites use that layer
- [ ] #7 Read paths call a side-effect-free owed predicate; the three mutation-preparation sites call one shared function; snapKvobjIsFrozenOriginal is used only in debug assertions; snapScanKeyIsOwed lives in the tests
- [ ] #8 Module child guard bits come from a table; every suite named in the description passes after each item's commit
<!-- AC:END -->
