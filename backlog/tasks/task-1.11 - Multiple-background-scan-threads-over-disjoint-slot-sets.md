---
id: TASK-1.11
title: Multiple background scan threads over disjoint slot sets
status: To Do
assignee: []
created_date: '2026-09-20 17:22'
updated_date: '2026-09-20 18:28'
labels:
  - bgsave
  - persistence
  - threads
milestone: m-1
dependencies:
  - TASK-1.9
documentation:
  - poc-hybrid-bgsave-spec.md
parent_task_id: TASK-1
priority: medium
ordinal: 12000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Spec §3, §6 and M2: extend the single scan thread from TASK-1.3 to a pool where each thread owns a disjoint set of slots (or scan units), scans them concurrently, and feeds the same fork channel with FORK_KVOBJ and NOFORK_BUFFER entries; each thread keeps its own per-db ~64 KB staging buffers. The child is the single writer and merge point and already tracks the selected db per entry (TASK-1.8), so interleaving producers is safe. Slot completion signals from multiple threads are consumed by the main thread's merge in any order.

Scope: thread count config (e.g. 'bgsave-threads', default to be decided; 1 preserves M1 behaviour); slot-to-thread assignment (static partition or work-stealing from the shared cursor); channel becomes multi-producer (or one channel per thread multiplexed by the child); ordering guarantees for slot state publication with several writers of the state array; abort must stop all threads; INFO persistence progress aggregated over threads. Standalone mode needs the bucket-range scan unit from TASK-1.3 for parallelism to exist at all, since a db has one dict there. Code in src/snapshot.c, 'snap'-prefixed.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 A config controls the number of background scan threads; with 1 the behaviour and output are the same as M1
- [ ] #2 With N>1 threads, slots are scanned concurrently by disjoint owners and every slot is scanned exactly once, verified by the harness race test under N=2 and N=4
- [ ] #3 The child accepts interleaved FORK_KVOBJ and NOFORK_BUFFER entries from multiple producers with correct SELECTDB tracking and the output reloads correctly (harness static and race tests pass)
- [ ] #4 Abort stops all threads and leaves no dict rehash-paused, covered by a test
- [ ] #5 Parallelism is effective in standalone mode as well as cluster mode (scan wall-time on a large SNAP_NOFORK-only dataset drops measurably from 1 to 4 threads; numbers recorded in task notes)
- [ ] #6 ThreadSanitizer run of the harness with 4 threads reports no races
<!-- AC:END -->
