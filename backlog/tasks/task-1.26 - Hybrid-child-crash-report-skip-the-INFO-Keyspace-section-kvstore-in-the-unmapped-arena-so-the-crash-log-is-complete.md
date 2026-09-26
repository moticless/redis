---
id: TASK-1.26
title: >-
  Hybrid child crash report: skip the INFO Keyspace section (kvstore in the
  unmapped arena) so the crash log is complete
status: To Do
assignee: []
created_date: '2026-09-26 08:35'
labels:
  - hybrid-bgsave
  - robustness
dependencies: []
references:
  - poc-hybrid-bgsave-spec.md#11.z.4
  - src/debug.c sigsegvHandler / logServerInfo
  - src/server.c dismissMemoryInChild condition
parent_task_id: TASK-1
priority: low
ordinal: 20000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
A crash inside the hybrid BGSAVE child runs sigsegvHandler -> logServerInfo -> genRedisInfoString(everything). The Keyspace section calls kvstoreSize(server.db[j].keys/expires); the kvstore structs are allocated from the MADV_DONTFORK arena and are not mapped in the hybrid child, so the crash handler faults a second time. The re-entry guard (signal_handler_lock EDEADLK check) then prints the reduced report, so the child's crash log is truncated: no INFO, no client/stack details past that point.

Fix: when server.in_fork_child is set and the arena is not forkable for this child (snapArenaAvailable() && !snapArenaForkable(), the same condition dismissMemoryInChild uses at server.c:7955), skip the Keyspace section (and any other section that touches the keyspace kvstores: keysizes, pubsub kvstores are forkable and fine) in genRedisInfoString, or have logServerInfo pass a section set without 'keyspace' in that case. Log one line stating the section was skipped because the process is a hybrid save child.

Found while auditing module behaviour in the child (spec section 11.z.4, side finding).
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 A deliberate crash in the hybrid child (e.g. via a DEBUG subcommand or a test hook such as snapTestChildWriteFaults) produces a full crash report with stack trace and INFO, without the 'Crashed running signal handler' reduced-report line
- [ ] #2 The stock child (arena forkable) and the parent still print the Keyspace section
- [ ] #3 Unit or tcl test covers the child crash report path under bgsave-hybrid yes
<!-- AC:END -->
