---
id: TASK-1.66
title: 'Overlay windows owned by their dict''s overlay: the global slot index goes'
status: Done
assignee:
  - '@claude'
created_date: '2026-11-08 07:07'
updated_date: '2026-11-08 07:18'
labels:
  - hybrid-bgsave
  - simplification
dependencies: []
references:
  - src/kvstore_overlay.c
  - src/kvstore_overlay.h
  - src/hybrid_snapshot.c
  - TASK-1.60
documentation:
  - docs/hybrid-snapshot.md
modified_files:
  - src/kvstore_overlay.c
  - src/kvstore_overlay.h
  - src/kvstore.h
  - src/hybrid_snapshot.c
  - docs/hybrid-snapshot.md
parent_task_id: TASK-1
priority: medium
ordinal: 62000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Why: the overlayed buckets of a hybrid save are indexed through one global array of window slots (kvso_index / kvso_nslots in src/kvstore_overlay.c). Every lookup on a frozen dict with overlayed buckets, reads included, walks that whole array (free slots and other dicts' windows too), the array only ever grows, freed slots keep their bucket arrays, and a bucket and the iterator cursor carry a slot index into it. The structure is harder to grasp than the thing it indexes: a dict has at most a few windows open at once.

Outcome: each dict's overlay owns its windows (a short list: the aligned runs of the credit window W the thread is on, at most the table; a run per table when rehashing). Lookup is a check of the overlay's own windows and an array load. The drain finds the overlays holding buckets through a small global list of them and writes back the windows the thread has passed, as before. Windows are allocated when first needed and freed with their last bucket. The slot index, its growth, the per-bucket slot number and kvstoreOverlaySlotsInUse's "slot" vocabulary go away; the count of windows holding buckets stays (kvstoreOverlayWindowsInUse) for the grant limit and tests.

Constraint: the number of windows per table is NOT fixed at two: DEBUG snap-window-buckets allows W down to 1 while the thread announces batches of 128 buckets plus prefetch, so a section's cap can leave several runs inside [lo, hi). Hence a list, not a pair. The credit window, grant and drain timing (section close, cron, the 16-window grant limit) are unchanged.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 Lookup of an overlayed bucket depends only on the number of windows of that dict's overlay, not on other dicts or on past peaks
- [x] #2 No global array of window slots remains; kvstoreOverlaySlotsInUse is replaced by kvstoreOverlayWindowsInUse with the same meaning (windows holding buckets, across all overlays)
- [x] #3 Windows are freed with their last bucket; nothing stays allocated between saves
- [x] #4 kvstoreOverlayDrain(below) still writes back exactly the windows whose last position is below 'below', across every store and detached dicts included
- [x] #5 Unit tests (redis-server test hybrid_snapshot) pass; the window test covers two windows of one table receiving many buckets and draining in order
- [x] #6 tests/unit/hybrid_snapshot.tcl passes with hybrid-snapshot on
- [x] #7 kvstore_overlay.h overview and docs/hybrid-snapshot.md describe the per-overlay windows
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
1. kvstore_overlay.c: kvsoWindow {next, last, table, lo, n, count, b[], bits[]} owned by kvstoreDictOverlay (win list oldest first, nwin); kvsoBucket points at its window; kvsoLookup walks the overlay's windows; kvsoWindowFor matches (table, lo, n) and keeps the later 'last', appends a new window; kvsoWindowRemove frees an emptied window. Global state is one intrusive list of overlays with nb > 0 (kvso_dirty) and the count of windows holding buckets; the drain walks the list, the release walks the overlay's windows. Enumeration cursors count the overlay's windows (kvstoreOvCursor.win) instead of indexing a global array.
2. kvstore_overlay.h / kvstore.h: prose for the per-overlay windows; kvstoreOverlaySlotsInUse -> kvstoreOverlayWindowsInUse; cursor field slot -> win.
3. hybrid_snapshot.c: HYBSNAP_OV_SLOTS_SOFT -> HYBSNAP_OV_WINDOWS_SOFT, call renamed; the 'ten windows' unit test becomes 'ten buckets in the dict's two windows' with an in-between drain.
4. docs/hybrid-snapshot.md: the overlayed-buckets paragraph and the memory table row.
5. Verify: build, redis-server test hybrid_snapshot, ./runtest --single unit/hybrid_snapshot --config hybrid-snapshot yes.
<!-- SECTION:PLAN:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
2026-11-08: implemented and built clean. redis-server test hybrid_snapshot: 227/227 (window tests 211-221 pass, two rewritten). tcl unit run in progress.

2026-11-08 verification: ./runtest --single unit/hybrid_snapshot --config hybrid-snapshot yes: 96 ok, 0 err. Not run: full suite, ASan/TSan (the change is main-thread-only data structure work; the drain and release paths keep their call sites). Working tree also carried unrelated uncommitted work (pausedExpandAllowed, dict.c resize policy); nothing committed.
<!-- SECTION:NOTES:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
Each dict's overlay now owns its overlay windows. kvstore_overlay.c: kvsoWindow (the buckets of one run of one table: pointer array by offset, bitmap, count, last position) lives on a short list in kvstoreDictOverlay, oldest first; a bucket points at its window. Lookup walks the overlay's few windows and loads one array slot. kvsoWindowFor finds a window by (table, lo, n), keeping the later 'last' when a run is reported again, or appends a new one; the window is freed with its last bucket. The drain walks a global intrusive list of the overlays holding buckets (kvso_dirty, entered with a dict's first overlayed bucket, left with its last) and writes back the windows whose last position is below 'below'; the release walks the overlay's windows. Enumeration cursors count the overlay's windows (kvstoreOvCursor.win).

Removed: the global slot array (kvso_index, kvso_nslots, kvso_slots_used, KVSO_SLOT_KEEP), its growth and matching, the per-bucket slot number. Renamed: kvstoreOverlaySlotsInUse -> kvstoreOverlayWindowsInUse, HYBSNAP_OV_SLOTS_SOFT -> HYBSNAP_OV_WINDOWS_SOFT. Unit test 'ten windows' rewritten as ten buckets landing in a dict's two windows with a drain between their last positions. Docs: kvstore_overlay.h overview, docs/hybrid-snapshot.md paragraph and memory table row.

Not changed: the credit window, the grant soft limit of 16 windows, drain timing. The windows per table are not bounded at two (W may be as small as 1 under DEBUG snap-window-buckets while the thread's batches are 128 buckets), hence a list.

Verification: build clean; redis-server test hybrid_snapshot 227/227; ./runtest --single unit/hybrid_snapshot --config hybrid-snapshot yes 96 ok, 0 err.
<!-- SECTION:FINAL_SUMMARY:END -->
