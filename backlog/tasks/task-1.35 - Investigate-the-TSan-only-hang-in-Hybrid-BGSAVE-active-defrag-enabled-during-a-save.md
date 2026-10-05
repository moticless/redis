---
id: TASK-1.35
title: >-
  Investigate the TSan-only hang in 'Hybrid BGSAVE: active defrag enabled during
  a save'
status: To Do
assignee: []
created_date: '2026-10-05 10:27'
labels:
  - tsan
  - flaky
  - snapshot
dependencies: []
references:
  - tests/unit/snapshot.tcl (test 'active defrag enabled during a save')
  - src/snapshot.c snapWalkerQuiesce / snapFunnelCloseSlow
parent_task_id: TASK-1
priority: medium
ordinal: 31000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Seen once on 2026-10-05 while verifying TASK-1.34 (a pure code move): the TSan build (.scratch/tsan, MALLOC=libc, setarch -R) of unit/snapshot with bgsave-hybrid yes hung in 'Hybrid BGSAVE: active defrag enabled during a save' right after the allkeys-lru/lfu write tests; the harness reported the client as IN PROGRESS and timed out after 2.5 h; the server logs were cleaned by the harness. The same test alone under TSan passed in 13 s with no ThreadSanitizer report, and a full rerun of the unit (--timeout 600 --dont-clean) passed. A hang (not a failed wait_for_condition) means a client command never got a reply, so the main thread was blocked, most likely waiting on the scan thread (write section / walker quiesce / funnel) under TSan's 10x slowdown with rdb-key-save-delay 300 and 1000 SETs + 500 DELs in flight. The ASan tree and the unsanitized build never showed it. The test exists since 4c62e8f6a (2026-09-21) and TSan was clean on 2026-10-03.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 Reproduce under TSan with --timeout 600 --dont-clean (loop the unit; on a hang attach gdb to the server and record the main thread's and scan thread's backtraces before the harness kills it)
- [ ] #2 Root cause identified: a real main-thread/scan-thread wait cycle, or a TSan artefact (document which); fix or a documented test adjustment
- [ ] #3 unit/snapshot under TSan passes 3 consecutive full runs
<!-- AC:END -->
