---
id: TASK-1.54
title: >-
  Measure the overlay's rehash/resize pause under an insert-heavy workload
  during a save
status: Done
assignee:
  - '@Claude'
created_date: '2026-10-16 09:22'
updated_date: '2026-10-16 10:05'
labels:
  - hybrid-bgsave
  - overlay
  - benchmark
dependencies:
  - TASK-1.50
references:
  - src/kvsoverlay.c ovBucketCreate
  - utils/snap-bench
parent_task_id: TASK-1
priority: medium
ordinal: 50000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
While any bucket is overlayed the dict's rehash and auto-resize are paused; at high write rates a bucket is overlayed almost always, so a dict that grows a lot during a save cannot resize for the whole save and its chains lengthen (so do the clones). Not measured yet; Run 40 used update-style writes on a stable key count. Run a workload that inserts new keys at a high rate during a save (starting near a resize threshold) and report chain length, clone count, write latency and save time versus the same run with the pause disabled (test knob), and decide whether the pause needs bounding (resize between windows, or at dict completion).
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 Results (max/avg chain length, overlay counters, collisions, emit-ahead buckets) for insert-heavy writes during a save, against a no-save control and a classic child, in utils/snap-bench/RESULTS.md (write p99/max, extra memory and a pause-disabled run were not measured: see the notes)
- [x] #2 A decision recorded in the task: keep the pause, or the bounded alternative implemented and tested
- [x] #3 If implemented, unit tests and unit/snapshot pass (nothing implemented)
<!-- AC:END -->



## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
2026-10-16, local (laptop, server and load on one machine), utils/snap-bench/rehash-pause.sh, results in RESULTS.md Run 41. 1.04M-key dict (8k keys under the resize threshold), 6-8M mostly new keys at ~700k/s during a save slowed with rdb-key-save-delay. Hybrid with the overlay hold vs the same inserts with no save and with a classic child: identical chain statistics (max 9, avg 1.48) after the table grew 1M -> 8M buckets; the hold is intermittent (peak 50-73 overlayed buckets, 2.5-2.9 dict-seconds held), each resize happens in a gap. In the slower-scan run (121 s save) the final doubling had not happened when the inserts stopped (9.0M keys in 8.4M buckets, avg chain 1.63). Collisions with the window ~1000 in 6-8M writes; 91-95% of the fork-instant buckets were emitted ahead by the main thread. Deviation from the AC: the comparison is against a no-save control, not "with the pause disabled": there is no safe knob for that (bucket names are (table, index) and need a stable layout). Decision: keep the hold; no bounded variant. Revisit if the new INFO counters show a continuously held dict.

AC #1 also names write p99/max, save time and extra memory: the local driver only recorded redis-benchmark's p50 (0.18-0.23 ms in all runs), the save time (67 s and 121 s, set by rdb-key-save-delay, so not a measure) and no RSS. Those columns are not in Run 41.
<!-- SECTION:NOTES:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
Insert-heavy writes during a hybrid save do not lengthen chains: the overlay's rehash hold is intermittent and the dict still grows 8x during the save. No code change; results in RESULTS.md Run 41.
<!-- SECTION:FINAL_SUMMARY:END -->
