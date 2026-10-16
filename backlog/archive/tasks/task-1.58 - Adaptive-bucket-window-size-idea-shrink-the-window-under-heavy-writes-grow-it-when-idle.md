---
id: TASK-1.58
title: >-
  Adaptive bucket window size (idea): shrink the window under heavy writes, grow
  it when idle
status: To Do
assignee: []
created_date: '2026-10-16 09:23'
labels:
  - hybrid-bgsave
  - overlay
  - idea
dependencies:
  - TASK-1.50
references:
  - src/snapshot.c SNAP_WINDOW_BUCKETS_DEFAULT
  - src/kvsoverlay.c OV_RING
parent_task_id: TASK-1
priority: low
ordinal: 54000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
The window is a fixed SNAP_WINDOW_BUCKETS_DEFAULT = 1024 buckets (DEBUG SNAP-WINDOW-BUCKETS overrides it). Collisions are proportional to window size, while a smaller window means more publication/quiesce handshakes. Evaluate a window that adapts to the observed write rate (for example from the collision counter of TASK-1.53), and the ring size (OV_RING = 1024) that must stay at least the window.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 A sweep of window sizes (64..4096) at 20k and 100k writes/s on mixed1m recorded in RESULTS.md with collisions, save time and write latency
- [ ] #2 A decision recorded: keep fixed, or adaptive implemented and tested; the ring size constraint is asserted
<!-- AC:END -->
