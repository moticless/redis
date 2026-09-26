---
id: TASK-1.27
title: >-
  Scan thread blocks on the 1 MB pipe while the child serializes a huge
  fork-class value: buffer completed blocks in memory so the two overlap
status: To Do
assignee: []
created_date: '2026-09-26 19:52'
labels:
  - hybrid-bgsave
  - performance
dependencies: []
references:
  - utils/snap-bench/RESULTS.md run 13
  - utils/snap-bench/run13.csv
  - 'src/snapshot.c snapSaveChildWrite, snapSendMsg, staging/mq queue'
parent_task_id: TASK-1
priority: medium
ordinal: 22000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Run 13 (RESULTS.md, mixed1m at 4 GB: 19.2 M small keys plus 24 hashtable hashes of 1M fields, about 90 MB each) shows the hybrid save taking 41-42 s against upstream's 40-46 s, while run 12's mixed (256k hashes of 9 KB) saved in half of upstream's time. The scan thread finished its 2 GB of small values in about 20 s (threads cpu s = 18-20) but the child spent about 40 s serializing the 24 big hashes one at a time via rdbSaveKeyValuePair from their header copies. While the child is inside a 1.5 s hash it does not read the pipe, so the scan thread blocks in write() on the 1 MB pipe buffer and its work is serialized behind the child's instead of overlapping with it. With small fork-class units (9 KB hashes) the child drains the pipe often enough that the two halves overlap.

Options, in order of preference:
1. Scan thread keeps completed 64 KB staging buffers in an in-memory queue (already exists for main-thread emissions: mq) and a writer step drains it to the pipe non-blockingly; the queue is bounded by bgsave-hybrid-max-retained-bytes (its bytes already count as retained). The scan thread never blocks on the child; when the bound is reached it waits (as today) rather than aborting.
2. Alternatively a reader thread in the child that drains the pipe into a memory queue while the main child thread serializes, so the pipe never fills. Costs a thread and a copy in the child; the parent side stays as is.
3. Larger pipe (F_SETPIPE_SZ, up to /proc/sys/fs/pipe-max-size, 1 MB default cap without privileges) is a partial fix only.

Measure with mixed1m (run 13 recipe) before/after: target is save time close to max(scan time, child time) = about 40 s -> the child's 40 s alone is the floor here, so the win shows when the child's share is smaller than the scan's (e.g. mixed1m with fewer hashes, or the small dataset with a few huge hashes added). Also record snap_save_queued_bytes peak.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 On a dataset where the scan thread's work exceeds the child's (e.g. small 4 GB plus 4 hashes of 1M fields), save time is within 10% of max(scan-only time, child-only time) instead of their sum
- [ ] #2 The in-memory queue is bounded by bgsave-hybrid-max-retained-bytes and its peak is visible in INFO (snap_save_queued_bytes or a new peak field)
- [ ] #3 No regression on run 12 small and mixed cells (save time, extra memory, main-thread CPU) within noise
- [ ] #4 Race harness, TSan and the feature-on unit tests pass
<!-- AC:END -->
