---
id: TASK-1.23
title: >-
  Harness scenarios adopted from the teammate's POC: address reuse, read-path
  mutations, failed publication, blocked-helper cancel
status: To Do
assignee: []
created_date: '2026-09-24 06:21'
labels:
  - bgsave
  - testing
milestone: m-3
dependencies: []
documentation:
  - poc-hybrid-bgsave-spec.md
  - tests/unit/snapshot.tcl
parent_task_id: TASK-1
priority: medium
ordinal: 12200
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Spec 11.z.3, item 3: corner cases the teammate's tcl suites exercise that our randomized harness does not reach. Add to tests/unit/snapshot.tcl, each racing a slow hybrid save and checking the reload digest against the fork instant: (1) address reuse: delete keys during the save and create new ones until a new kvobj lands at a freed address (verify with DEBUG OBJECT or a debug command exposing the address); (2) in-place RAW string mutation from a read command (PFCOUNT on a HyperLogLog) and XREADGROUP updating consumer-group state through a read lookup; (3) failed publication: kill the child after it has written part of the file and assert the previous dump.rdb is untouched and loadable; (4) cancellation while the scan thread is blocked on a full pipe (child stopped with SIGSTOP), then FLUSHALL / SHUTDOWN NOSAVE, asserting no hang and a clean next save. The module-metadata case is already covered (commit 4f0046b69).
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 Each of the four scenarios exists as a test in tests/unit/snapshot.tcl and passes on the jemalloc build and under TSan in arena-free mode
- [ ] #2 The address-reuse test demonstrates an actual reuse (asserts the address matched at least once) rather than hoping for it
- [ ] #3 The failed-publication test asserts the old dump file's digest is unchanged after the failed save
<!-- AC:END -->
