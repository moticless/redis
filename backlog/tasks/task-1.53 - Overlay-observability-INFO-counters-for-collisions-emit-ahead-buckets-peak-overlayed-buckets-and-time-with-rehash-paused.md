---
id: TASK-1.53
title: >-
  Overlay observability: INFO counters for collisions, emit-ahead buckets, peak
  overlayed buckets and time with rehash paused
status: Done
assignee:
  - '@Claude'
created_date: '2026-10-16 09:22'
updated_date: '2026-10-16 09:58'
labels:
  - hybrid-bgsave
  - overlay
  - observability
dependencies:
  - TASK-1.50
references:
  - src/snapshot.c snapInfoPersistence
  - src/kvsoverlay.c
parent_task_id: TASK-1
priority: medium
ordinal: 49000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
A save's overlay cost is invisible on a live instance. Expose, in INFO persistence (snapInfoPersistence), the counters of the last/current hybrid save: writes that created an overlayed bucket (collisions with the window), buckets the main thread emitted ahead, peak number of simultaneously overlayed buckets, keys cloned, and the time rehash/resize was paused by the overlay. Counters are kept in src/kvsoverlay.c and read from the main thread only (no new atomics unless a thread other than main touches them).
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 INFO persistence shows the five counters during and after a hybrid save, reset at the start of the next save
- [x] #2 A unit/snapshot test writes during a save and checks the counters are non-zero and consistent (clones >= collisions)
- [x] #3 docs/hybrid-snapshot.md lists the fields
<!-- AC:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
2026-10-16. kvstoreOverlayStats (kvstore.h) kept in static counters in src/kvsoverlay.c (main thread only): buckets (ovBucketCreate), clones, peak of simultaneously overlayed buckets over every dict, paused_us (per-dict rehash/auto-resize hold; an open pause is added at read time from the open count and the sum of start times, so no list walk). snapSaveCreate calls kvstoreOverlayStatsReset(), so the values describe the current save and stay readable after it. INFO persistence: snap_save_overlay_buckets, snap_save_overlay_clones, snap_save_overlay_peak_buckets, snap_save_overlay_paused_ms. The task's fifth counter (buckets the main thread emitted ahead) already exists as snap_save_main_buckets. Test: the existing tests/unit/snapshot.tcl case "writes inside the window go to overlayed buckets and are written back once the window has moved" asserts buckets > 0, clones >= buckets, peak bounds, paused_ms >= 0, and that the values persist after the save; ./runtest --single unit/snapshot passed (all tests, 206 s). snap_save_side_entries keeps its name (tests and bench probes use it).
<!-- SECTION:NOTES:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
INFO persistence now reports the overlay's collisions, clones, peak overlayed buckets and rehash-paused time for the current/last save; documented in docs/hybrid-snapshot.md.
<!-- SECTION:FINAL_SUMMARY:END -->
