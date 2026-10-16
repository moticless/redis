---
id: TASK-1.57
title: >-
  Parallel scan: split the scan across 2+ threads (per slot in cluster mode,
  table halves otherwise) (idea)
status: To Do
assignee: []
created_date: '2026-10-16 09:23'
labels:
  - hybrid-bgsave
  - performance
  - idea
dependencies:
  - TASK-1.55
parent_task_id: TASK-1
priority: low
ordinal: 53000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
One scan thread walks every frozen dict. A faster scan also shortens the exposure to window collisions and emit-ahead, both proportional to scan duration. Idea: several scan threads, each with its own window (per slot dicts in cluster mode, ranges of the table otherwise), each publishing its window and write section handshake. Needs TASK-1.55's profile to show the scan thread, not the child writer, is the limit.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 Profile evidence that the scan thread is the bottleneck, recorded first
- [ ] #2 If implemented: save time and write latency measured on mixed1m and small, harness and unit/snapshot pass, TSAN clean
<!-- AC:END -->
