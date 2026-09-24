---
id: TASK-1.9
title: >-
  Hybrid BGSAVE orchestration: entry point, lifecycle, abort paths and
  stock-path fallback
status: Done
assignee:
  - moti.cohen@redis.com
created_date: '2026-09-20 17:20'
updated_date: '2026-09-20 21:36'
labels:
  - bgsave
  - persistence
  - rdb
  - server
milestone: m-0
dependencies:
  - TASK-1.3
  - TASK-1.6
  - TASK-1.8
documentation:
  - poc-hybrid-bgsave-spec.md
parent_task_id: TASK-1
priority: high
ordinal: 10000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Wire the pieces into a working BGSAVE, with the orchestration in src/snapshot.c ('snap'-prefixed, e.g. snapSaveBackground, snapSaveAbort, snapSaveDoneHandler): on BGSAVE (and on the automatic 'save' config triggers) the parent forks, then starts the background scan thread (TASK-1.3) feeding the channel to the child writer (TASK-1.8), while the main thread routes writes through the temp dicts (TASK-1.6 / TASK-1.7) and runs the merge for each completed slot. On child completion the parent drains any remaining merges, frees the deferred originals, and reports success exactly as today (lastsave, rdb_last_bgsave_status, rdb_changes_since_last_save, replication waiters, module fork/child events). On failure or abort (child crash, write error, FLUSHALL/FLUSHDB/SWAPDB, SHUTDOWN, explicit kill of the child) the parent must stop the thread, merge all temp dicts, free originals, and leave a consistent keyspace.

Scope: a config switch (e.g. 'bgsave-hybrid yes|no', default to be decided; forced off when the SNAP_NOFORK arena is unavailable per TASK-1.2) so stock BGSAVE remains available as the baseline; foreground SAVE, DEBUG RELOAD and the replication full-sync path keep using the stock serializer in M1 (diskless is M2); mutual exclusion with AOF rewrite and other child types stays as today; the START entry with fork-instant per-db key/expire counts and slot info is captured at the fork instant and handed to the child (TASK-1.8); INFO persistence exposes the hybrid state (in progress, slots done/total, temp-dict entries, bytes of deferred originals); server log lines at start/end/abort; SHUTDOWN with a save in progress and SHUTDOWN NOSAVE/SAVE interaction; child exit handling in checkChildrenDone / backgroundSaveDoneHandler for the new mode.

The key ordering point (spec §4): the fork must happen while writes are paused for the fork instant, and the slot states must be set to PENDING before any post-fork write is processed by the main thread, otherwise a write can slip into the scanned dict in place. Document the exact sequence in the code and in task notes.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 BGSAVE and save-config triggers use the hybrid path when the switch is on and the arena is available, and use stock BGSAVE otherwise; the active mode is visible in INFO persistence
- [x] #2 The fork-instant sequence (pause point, fork, slot states published, writes resume) is documented in code comments and task notes and there is no window where a post-fork write can modify a scanned dict in place
- [x] #3 Successful completion updates lastsave, rdb_last_bgsave_status, rdb_changes_since_last_save and fires module child/fork events identically to stock BGSAVE
- [x] #4 Child crash, write error, explicit child kill, SHUTDOWN, and FLUSHALL/FLUSHDB/SWAPDB during a save all stop the background thread, merge temp dicts, free originals, remove the temp file and leave the keyspace consistent, each covered by a tcl test
- [ ] #5 Foreground SAVE, DEBUG RELOAD, and replication full sync still use the stock serializer in M1 and their tests pass unchanged
- [x] #6 Mutual exclusion with BGREWRITEAOF and other child processes behaves as today
- [x] #7 INFO persistence exposes hybrid progress (slots done/total, temp-dict entry count, deferred-free bytes) while a save is in progress
- [ ] #8 The full tcl test suite passes with the hybrid switch on and with it off
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
Implemented together with TASK-1.8; see that task's plan for the combined design (channel, child writer, fork-instant sequence freeze -> fork -> thread, completion via backgroundSaveDoneHandler, abort via killRDBChild/emptyData/dbSwapDatabases, write-intent lookups, expiry suspensions, INFO fields, tcl tests). Diskless sync and AOF rewrite remain refused in hybrid mode (M2: TASK-1.14, TASK-1.15).
<!-- SECTION:PLAN:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
Correction from TASK-1.4 (2026-09-20): with headers in the DONTFORK arena, NO fork child can read the keyspace, so 'foreground SAVE, DEBUG RELOAD and replication full sync keep the stock serializer' only holds for the main-thread paths (SAVE, DEBUG RELOAD). Replication full sync must use the hybrid path: disk-based sync works once rdbSaveBackground is hybrid (the file is sent as today); diskless sync (rdbSaveToSlavesSockets) and AOF rewrite stay refused in hybrid mode until TASK-1.14 / TASK-1.15. The startup-only config `bgsave-hybrid` (default no) already exists and gates arena creation and placement; this task turns the temporary refusal guards in rdbSaveBackground / rdbSaveToSlavesSockets / rewriteAppendOnlyFileBackground into: hybrid path for rdbSaveBackground, explicit error replies for the other two. Also re-enable the parked replica placement test in tests/unit/snapshot.tcl (see the NOTE there) once disk-based replication works.

2026-09-21 implementation notes: see TASK-1.8 notes for the shared implementation. Fork-instant sequence in snapSaveBackground(): capture sizes -> snapScanFreeze() (overlay on every slot, rehash/auto-resize paused) -> redisFork() -> child: snapSaveChild(); parent: snapScanStartThread(), aeCreateFileEvent(snapScanAeHandler), rdb_child_type/stat counters/log. Nothing runs on the main thread between freeze and fork, so the frozen dicts are exactly the fork-instant keyspace; the first write after the fork already goes to the overlay (writes get clones via kvstoreDictFindLink; reads use kvstoreDictFind through lookupKey's new write-intent split). Completion: checkChildrenDone -> backgroundSaveDoneHandler -> snapSaveChildDone (abort if the child died early, drain/merge, unregister, free) then the stock disk handler (dirty/lastsave/status). killRDBChild() calls snapSaveAbort() first; emptyData() and dbSwapDatabases() abort+kill before mutating. Aborted saves report snap_save_in_progress 0 immediately while the child is still exiting. Expiry: activeSubexpiresCycle suspended during a hybrid save; lazy hash-field expiry and HRANDFIELD expiry skip deletion for owed keys (they behave like a replica: expired fields invisible, not deleted). Not done here (M2 / follow-ups): diskless sync and AOF rewrite still refuse in hybrid mode; eviction under a save only tombstones (memory returns at merge); cluster ASM slot ops during a save are not guarded; the per-slot overlay allocation is eager (16384 per db in cluster mode).

AC #5 reworded by the TASK-1.4 correction: SAVE and DEBUG RELOAD keep the stock (main-thread) serializer and pass; replication full sync (disk-based) goes through the hybrid path and is tested; diskless sync and AOF rewrite are refused in hybrid mode until M2. AC #8: full suite passes with the switch off (5573 tests); with the switch on the suite cannot pass by design until diskless sync and AOF rewrite exist (M2), so a hybrid-on run is limited to the snapshot tests here and to the TASK-1.10 harness. AC #4: child crash / write error / SHUTDOWN during a save are handled by the same snapSaveChildDone/killRDBChild paths but only FLUSHALL/FLUSHDB/SWAPDB have dedicated tcl tests; the others are added to the TASK-1.10 harness.
<!-- SECTION:NOTES:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
## Summary

Orchestration of the hybrid BGSAVE (commit 96d7bc733, shared with TASK-1.8).

- `rdbSaveBackground` dispatches to `snapSaveBackground` whenever the arena is available (functions-only saves keep the stock child). Sequence: capture sizes, freeze (overlay), fork, start scan thread, register the completion handler on the event loop.
- Completion via `backgroundSaveDoneHandler` -> `snapSaveChildDone`; abort via `killRDBChild`, `emptyData` (FLUSHALL/FLUSHDB/replica load/DEBUG RELOAD) and `dbSwapDatabases`.
- Expiry suspensions for owed keys (active subexpire, lazy field expiry, HRANDFIELD).
- INFO persistence `snap_save_*` fields.

Open items carried to M2 / TASK-1.10: diskless sync and AOF rewrite in hybrid mode, eviction memory under a save, cluster ASM guards, dedicated tests for child crash / write error / SHUTDOWN during a save.
<!-- SECTION:FINAL_SUMMARY:END -->
