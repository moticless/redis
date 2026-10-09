---
id: TASK-1.39
title: >-
  FLUSHALL-class callers that empty every db hand the dbs to a running hybrid
  BGSAVE instead of aborting it (DEBUG RELOAD/LOADAOF, CLUSTER RESET,
  RM_ResetDataset behave as stock)
status: Done
assignee:
  - '@Claude'
created_date: '2026-10-09 10:34'
updated_date: '2026-10-09 10:54'
labels:
  - hybrid-bgsave
  - semantics
milestone: m-1
dependencies: []
references:
  - >-
    src/db.c emptyData (the dbnum == -1 abort branch) and emptyDbStructure /
    emptyDbDetachToSnapshot
  - src/db.c flushAllDataAndResetRDB (stock kills the RDB child after emptying)
  - >-
    src/replication.c readSyncBulkPayload (stock kills the RDB child before the
    full-sync load)
  - >-
    src/debug.c DEBUG RELOAD / LOADAOF, src/cluster_legacy.c clusterReset,
    src/module.c RM_ResetDataset (stock leaves a running BGSAVE child alone)
  - >-
    tests/integration/aof-multi-part.tcl (the stock-only in-progress assertion
    added for the hybrid)
documentation:
  - poc-hybrid-bgsave-spec.md
parent_task_id: TASK-1
priority: medium
ordinal: 35000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Since TASK-1.24, emptyData(-1) aborts a running hybrid BGSAVE (and kills its child) before destroying the keyspace, because the scan thread reads the live dicts. Stock only kills the RDB child in two of emptyData(-1)'s seven callers: FLUSHALL (flushAllDataAndResetRDB, right after emptying) and a replica's full-sync load (before loading). The other four, DEBUG RELOAD, DEBUG LOADAOF, CLUSTER RESET HARD and RedisModule_ResetDataset, let a running BGSAVE finish with its fork-instant data. The hybrid therefore differs from stock in those four, and one stock test had to be adapted (aof-multi-part's rdb_bgsave_in_progress assertion is stock-only).

The mechanism to match stock already exists: a hybrid AOF rewrite (TASK-1.15 stage 1) and FLUSHDB (TASK-1.24) hand the emptied db's structures to the running save (emptyDbDetachToSnapshot), which finishes with the fork-instant data. Outcome wanted: emptyData(-1) does the same for a BGSAVE instead of aborting, so the four callers above behave as in stock, while FLUSHALL and the full-sync load still end the save through their own killRDBChild calls (which the hybrid abort is hooked into). Detached dbs hold their memory until the save ends, as a stock child's copy-on-write pages do.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 DEBUG RELOAD NOSAVE issued while a hybrid BGSAVE runs leaves the save running; it completes with status ok and the dump it writes holds the fork-instant dataset (reload and digest), while the live keyspace holds what DEBUG RELOAD loaded
- [x] #2 DEBUG LOADAOF during a hybrid BGSAVE behaves the same way (save completes, fork-instant dump)
- [x] #3 FLUSHALL (sync and ASYNC) during a hybrid BGSAVE still ends the save, through killRDBChild, and the next save works; a replica's full-sync load still ends the replica's own running save (existing tests unchanged and passing)
- [x] #4 The stock-only adaptation of the aof-multi-part in-progress assertion is removed and the suite passes with --config bgsave-hybrid yes
- [x] #5 unit/snapshot passes; spec section 4/5 and the blog's FLUSHALL bullet describe the new behavior
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
1. db.c emptyData: drop the dbnum == -1 abort branch; every emptied db goes through emptyDbStructure's detach (emptyDbDetachToSnapshot), the path FLUSHDB and the hybrid AOF rewrite already take. FLUSHALL (flushAllDataAndResetRDB) and the full-sync load (readSyncBulkPayload) keep ending the save through their own killRDBChild, which calls snapSaveAbort.
2. Tests (unit/snapshot, 'Hybrid BGSAVE:' block): DEBUG RELOAD NOSAVE during a slow save brings back the previous dump while the save continues, then the save's dump reloads to the fork-instant digest; DEBUG LOADAOF during a save the same way. The AOF for the second test must not contain a FLUSHALL: populate_all_types starts with one, so the data is populated before appendonly is turned on (the rewrite base holds it). A replayed FLUSHALL kills the RDB child in stock too.
3. integration/aof-multi-part: remove the stock-only guard around the rdb_bgsave_in_progress assertion added for the hybrid.
4. Spec section 4 (abort paragraph and the FLUSHALL/FLUSHDB/SWAPDB item) and the blog's FLUSHALL bullet.
5. Verify: unit/snapshot, integration/replication, unit/aofrw, integration/aof, unit/other with --config bgsave-hybrid yes; aof-multi-part with the hybrid on; TSan unit/snapshot on the new head.
<!-- SECTION:PLAN:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
2026-10-09 implemented. First version of the LOADAOF test failed: the save ended between the AOF base and INCR loads with the child receiving SIGUSR1. Cause: populate_all_types begins with FLUSHALL, which was in the INCR file; its replay runs flushAllDataAndResetRDB -> killRDBChild, as stock does. Test restructured so the data is in the rewrite base. Targeted runs: both DEBUG tests, the FLUSHALL/FLUSHDB/SWAPDB/full-sync tests and the AOF FLUSHALL test pass; integration/aof-multi-part with --config bgsave-hybrid yes 41 ok with the stock-only guard removed. Full chain running (unit/snapshot, replication, aofrw, aof, other with the hybrid on, then TSan unit/snapshot on this head).

Verified 2026-10-09: unit/snapshot 81 ok; integration/replication 80, unit/aofrw 25, integration/aof 63, unit/other 43 with --config bgsave-hybrid yes, all clean; integration/aof-multi-part 41 ok with the guard removed; TSan unit/snapshot on this head 75 ok, no ThreadSanitizer report.
<!-- SECTION:NOTES:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
Emptying every db no longer aborts a running hybrid BGSAVE; it hands the dbs to the save, which finishes with the fork-instant data as a stock fork child does from its copy. This removes a behavior difference from stock in DEBUG RELOAD, DEBUG LOADAOF, CLUSTER RESET HARD and RedisModule_ResetDataset, which in stock leave a running BGSAVE alone. FLUSHALL and a replica's full-sync load still end the save, through the same killRDBChild calls stock makes there (the hybrid abort is hooked into killRDBChild).

Change: the dbnum == -1 abort branch in db.c emptyData is removed; emptyDbStructure's per-db detach (emptyDbDetachToSnapshot, the FLUSHDB and AOF-rewrite path since TASK-1.24 and TASK-1.15) now covers every caller.

Tests: two new unit/snapshot tests (DEBUG RELOAD NOSAVE and DEBUG LOADAOF during a slow save: the save keeps running, the live keyspace holds what was loaded, the save's dump reloads to the fork-instant digest). The LOADAOF test populates before turning AOF on so the replayed AOF holds no FLUSHALL, since a replayed FLUSHALL kills the RDB child in stock as well. The stock-only guard added to integration/aof-multi-part for the hybrid is removed. Verified: unit/snapshot 81, replication 80, aofrw 25, aof 63, other 43 (hybrid on), aof-multi-part 41, TSan unit/snapshot 75, no sanitizer report.

Docs: spec section 4 (abort paragraph and the FLUSHALL/FLUSHDB/SWAPDB item) and the blog's FLUSHALL bullet.
<!-- SECTION:FINAL_SUMMARY:END -->
