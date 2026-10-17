---
id: TASK-1.61
title: >-
  Measure the scan thread's quiesce waits and the main thread's overlay
  write-back cost during a save
status: To Do
assignee: []
created_date: '2026-10-17 10:31'
labels:
  - hybrid-bgsave
  - performance
  - observability
dependencies: []
references:
  - src/snapshot.c
  - src/kvsoverlay.c
  - utils/snap-bench/RESULTS.md
  - TASK-1.53
parent_task_id: TASK-1
priority: medium
ordinal: 57000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Why: two follow-ups are gated on numbers nobody has today.

1. The credit-based window protocol (scan thread advances lo, main thread advances hi) exists to remove the scan thread's wait in snapWalkerQuiesce (src/snapshot.c): after publishing a window the scan thread waits for any write section open at that moment to close. A section lasts at most one command, so the cost only matters with long commands (large Lua scripts, big MULTI/EXEC, module threads holding the GIL). Nothing counts how often or how long it waits.
2. Offloading the frees of the overlay's original entries to a background thread only pays off if the main thread's write-back (kvsoverlay.c ovWriteBack, run from the drain at the end of write sections and from cron, and from ovRelease when a dict's scan completes) is a noticeable share of main-thread time. Nothing measures it.

Outcome: counters for both, a benchmark run that exercises them, and a recorded go/no-go for each follow-up.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 INFO (or the snapshot DEBUG stats) reports, for the current or last save: number of quiesce waits, total wait time, longest single wait
- [ ] #2 INFO (or the snapshot DEBUG stats) reports, for the current or last save: buckets written back, total main-thread write-back time and the part spent freeing original entries
- [ ] #3 Counters are reset per save and documented next to the existing overlay counters (TASK-1.53)
- [ ] #4 utils/snap-bench runs the existing matrix plus at least one long-command workload (large Lua script or big MULTI/EXEC) during a save
- [ ] #5 utils/snap-bench/RESULTS.md records quiesce time as a share of save time and write-back time as a share of main-thread CPU per run
- [ ] #6 RESULTS.md states a conclusion for each follow-up: pursue or drop the credit-based window protocol, pursue or drop offloading the frees
<!-- AC:END -->
