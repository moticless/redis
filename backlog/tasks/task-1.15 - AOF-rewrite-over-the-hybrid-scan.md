---
id: TASK-1.15
title: AOF rewrite over the hybrid scan
status: To Do
assignee: []
created_date: '2026-09-20 17:22'
updated_date: '2026-09-20 18:28'
labels:
  - bgsave
  - persistence
  - aof
milestone: m-1
dependencies:
  - TASK-1.9
documentation:
  - poc-hybrid-bgsave-spec.md
parent_task_id: TASK-1
priority: medium
ordinal: 16000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Spec §6 and M2: BGREWRITEAOF uses the same slot scan and channel, with the AOF encoders instead of RDB. With aof-use-rdb-preamble yes the base file is an RDB, so this is largely the M1 writer producing the base file inside the multi-part AOF layout; with the preamble off, the background thread's SNAP_NOFORK serializer and the child's SNAP_FORK serializer (working on the shipped kvobj copy) emit AOF command form (rewriteAppendOnlyFileRio per-type rewriters) into the same ~64 KB staging buffers / channel entries, and the child tracks the selected db to emit SELECT only on change.

Scope: hook rewriteAppendOnlyFileBackground into the snapshot.c orchestration; AOF-specific finish (manifest update, base/incr file handling; the incr file captures post-fork writes as today); interaction with the temp-dict path (snapshot content must be fork-instant); the AOF rewrite child's child-info reporting; per-type AOF rewriters must be safe to run off the main thread for SNAP_NOFORK keys. Foreground AOF rewrite on startup / aof-load paths are unchanged.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 BGREWRITEAOF with aof-use-rdb-preamble yes and no both produce a base file via the hybrid scan and the AOF loads to a keyspace identical to the fork-instant reference under the harness race workload
- [ ] #2 With the preamble off, SELECT is emitted only on db change despite interleaved producers, verified with a multi-db dataset
- [ ] #3 Multi-part AOF manifest and base/incr handling behave as today, covered by the existing AOF tcl tests
- [ ] #4 Per-type AOF rewriters used by the background thread for SNAP_NOFORK keys are reviewed for main-thread-only state and the review is recorded in task notes
- [ ] #5 Abort of an AOF rewrite mid-scan leaves the manifest and files consistent and the next rewrite succeeds
- [ ] #6 Existing AOF and aof-multi-part test suites pass with the hybrid switch on
<!-- AC:END -->
