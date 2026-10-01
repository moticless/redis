---
id: TASK-1.26
title: >-
  Hybrid child crash report: skip the INFO Keyspace section (kvstore in the
  unmapped arena) so the crash log is complete
status: Done
assignee:
  - '@Claude'
created_date: '2026-09-26 08:35'
updated_date: '2026-10-01 15:04'
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
- [x] #1 A deliberate crash in the hybrid child (e.g. via a DEBUG subcommand or a test hook such as snapTestChildWriteFaults) produces a full crash report with stack trace and INFO, without the 'Crashed running signal handler' reduced-report line
- [x] #2 The stock child (arena forkable) and the parent still print the Keyspace section
- [x] #3 Unit or tcl test covers the child crash report path under bgsave-hybrid yes
<!-- AC:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
The hybrid save child now writes a complete crash report (commit b89355ece). Two faults inside the handler were truncating it, not one:

1. **INFO sections touching the keyspace.** logServerInfo asked for "all"; the memory section (getMemoryOverheadData walks the db kvstores), keyspace and keysizes (kvstoreSize) and hotkeys dereference kvstores allocated in the DONTFORK arena, which is not mapped in the hybrid child. logServerInfo now builds the section list without those four when snapInHybridChild() is set and logs one line saying so; the parent and the stock child still print everything.
2. **killThreads() before the fast memory test.** It cancelled the parent's bio and IO pthread handles, which are stale in any fork child (fork copies one thread); pthread_cancel faulted inside the handler. killThreads() now returns at once when server.in_fork_child is set. The fast memory test itself is fine: it only touches rw anonymous mappings, so the child's PROT_NONE re-reservation of the arena range is skipped.

Test: the harness crashes the hybrid child with SIGSEGV and checks the log has the INFO section, the skip line, no "Crashed running signal handler" and no new "# Keyspace"; then switches to bgsave-hybrid no, crashes the stock child the same way and checks its report does print "# Keyspace"; then a save and reload succeed. Verified in the feature-on unit, stock unit and TSan runs of the whole unit.
<!-- SECTION:FINAL_SUMMARY:END -->
