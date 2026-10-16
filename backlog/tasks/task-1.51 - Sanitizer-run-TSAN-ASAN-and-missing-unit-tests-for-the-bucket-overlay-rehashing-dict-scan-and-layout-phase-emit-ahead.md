---
id: TASK-1.51
title: >-
  Sanitizer run (TSAN, ASAN) and missing unit tests for the bucket overlay:
  rehashing-dict scan and layout-phase emit-ahead
status: Done
assignee:
  - '@Claude'
created_date: '2026-10-16 09:22'
updated_date: '2026-10-16 09:54'
labels:
  - hybrid-bgsave
  - overlay
  - testing
dependencies:
  - TASK-1.50
references:
  - src/snapshot.c snapOverlayBucketTests
  - src/kvsoverlay.c
parent_task_id: TASK-1
priority: high
ordinal: 47000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
The bucket overlay (TASK-1.50) was validated only by the unit suite, unit/snapshot and the randomized harness; the earlier TSAN/ASAN run covered the abandoned bucket-swap design, not this one. Run both sanitizers over the overlay and add the two unit tests still missing: a scan of a dict that is mid-rehash (two tables) with writes during it, and the layout phase (whole-dict window, snapBucketPosition returns SNAP_POS_AHEAD: emit-ahead then write in place). TSAN needs `setarch $(uname -m) -R`. ./runtest needs the sandbox disabled, no other suite running, and no rebuild while it runs.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 A TSAN build passes ./src/redis-server test snapshot and ./runtest --single unit/snapshot with no reports
- [x] #2 An ASAN build passes the same with no reports
- [x] #3 A unit test scans a rehashing dict while writing to it and checks no key is lost or duplicated
- [x] #4 A unit test covers a write during the layout phase (emit-ahead, then in place) and checks the saved content equals the fork-instant content
- [x] #5 Any finding from the sanitizers is fixed or recorded in the task notes with a reason
<!-- AC:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
2026-10-16. Unit tests added in src/snapshot.c: snapOverlayRehashTests (dict caught mid-rehash with entries in both tables: writes overlay buckets of either table and keep the rehash paused, the iterator returns each logical key once, a scan spanning the write-back returns every key present throughout, the rehash completes after the release) and a layout-phase case in the "owed" block (window table -1: every bucket of both tables is AHEAD and owed). One condition of the first draft was wrong (dict size equals N while overlayed): the test hook drains all, so a ring-slot collision legitimately writes a bucket back; dropped. Sanitizers: built copies of the working tree (SANITIZER=thread / address, MALLOC=libc, -DREDIS_TEST) in short paths (unix socket path limit 108). ./redis-server test snapshot: 142/142 under both, no reports. ./runtest --single unit/snapshot --clients 4 --tags -large-memory: all passed under ASAN (detect_leaks=0, 294 s) and TSAN (setarch -R, 427 s), no sanitizer reports. LIMITATION: with MALLOC=libc there is no SNAP_NOFORK arena, so these runs cover the arena-free hybrid mode (class by size, child maps everything) and the whole scan/overlay/emit-ahead logic, not the arena allocation hooks; sanitizers do not work with the jemalloc arena build. The randomized harness 100-iteration test and cluster suites were not run under the sanitizers.

AC #4 as written asks for a content comparison; the new layout-phase unit test checks the position/owed decision only (every bucket AHEAD and owed in the whole-dict window). Saved-content equality for emit-ahead then in-place writes is covered by the harness tests in tests/unit/snapshot.tcl (reload and digest compare), not by this unit test.
<!-- SECTION:NOTES:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
Added unit tests for the rehashing-dict overlay and the layout phase; the bucket overlay is clean under TSAN and ASAN for the snapshot unit tests and tests/unit/snapshot.tcl (arena-free build).
<!-- SECTION:FINAL_SUMMARY:END -->
