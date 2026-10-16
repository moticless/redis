---
id: TASK-1.59
title: >-
  SNAP_NOFORK arena: fall back to regular allocation when the 16 TB reservation
  is full or cannot be reserved (idea)
status: To Do
assignee: []
created_date: '2026-10-16 09:23'
labels:
  - hybrid-bgsave
  - arena
  - idea
dependencies: []
references:
  - src/zmalloc.c
  - src/zmalloc.h
  - src/snapshot.c snapArenaInit
parent_task_id: TASK-1
priority: low
ordinal: 55000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
The arena is a fixed virtual range (16 TB cap). When it is exhausted, or the reservation fails (RLIMIT_AS, containers), allocation should degrade instead of failing: size the range from RLIMIT_AS, add a larger rung, or fall back to ordinary zmalloc for new SNAP_NOFORK requests (those values become SNAP_FORK class, which the classification already handles by address). Keep zmalloc free/realloc routing by address correct for blocks from both sources.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 With the arena range artificially small (test knob), writes keep succeeding and a hybrid save of the resulting mixed placement equals the fork-instant content
- [ ] #2 With RLIMIT_AS below the 16 TB range, startup picks a smaller range or disables the arena with a log line instead of aborting
- [ ] #3 unit tests for the free/realloc routing of both block sources
<!-- AC:END -->
