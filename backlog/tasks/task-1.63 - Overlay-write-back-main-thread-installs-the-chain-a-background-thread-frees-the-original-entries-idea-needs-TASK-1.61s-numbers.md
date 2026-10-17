---
id: TASK-1.63
title: >-
  Overlay write-back: main thread installs the chain, a background thread frees
  the original entries (idea, needs TASK-1.61's numbers)
status: To Do
assignee: []
created_date: '2026-10-17 10:31'
labels:
  - hybrid-bgsave
  - performance
  - idea
dependencies:
  - TASK-1.61
references:
  - src/kvsoverlay.c
  - src/lazyfree.c
  - TASK-1.61
parent_task_id: TASK-1
priority: low
ordinal: 59000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Why: the overlay write-back (src/kvsoverlay.c ovWriteBack) runs on the main thread and must stay there: the main thread may be writing the same overlay chain, the dict and kvstore counters it updates are main-thread only, and main-thread iterators walk the buckets. Per bucket it is a few pointer stores plus freeing the original entries the overlay shadowed; the frees are the only part that may cost.

Idea: split the write-back. The main thread installs the chain into the dict and clears the index entry (O(1) per bucket) and hands the original entries to a background thread (the scan thread when idle, or bio lazyfree) to free. Only entries that lazyfree would free in the background (the freeObjAsync rules: refcount 1, module types with free_effort) go to the background; the rest are freed inline.

Gate: only if TASK-1.61 shows the frees are a noticeable share of main-thread time during a save.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 TASK-1.61's numbers justify the change (recorded in this task's notes); otherwise the task is closed with them
- [ ] #2 Main-thread write-back time per bucket drops measurably on the workload TASK-1.61 used
- [ ] #3 Entries that are not safe to free in the background are still freed on the main thread; a test covers shared objects and a module type without free_effort
- [ ] #4 Memory accounting and the overlay INFO counters stay correct; ASAN and TSAN runs are clean
- [ ] #5 docs/hybrid-snapshot.md describes where original entries are freed
<!-- AC:END -->
