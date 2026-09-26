---
id: TASK-1.27
title: >-
  Scan thread blocks on the 1 MB pipe while the child serializes a huge
  fork-class value: buffer completed blocks in memory so the two overlap
status: To Do
assignee: []
created_date: '2026-09-26 20:05'
labels:
  - hybrid-bgsave
  - performance
dependencies: []
references:
  - utils/snap-bench/RESULTS.md run 13
  - utils/snap-bench/run13.csv
  - src/snapshot.c snapSaveChildWrite, snapSendMsg, staging and mq queue
parent_task_id: TASK-1
priority: medium
ordinal: 21000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Run 13 (RESULTS.md, mixed1m at 4 GB: 19.2 M small keys plus 24 hashtable hashes of 1M fields, about 90 MB each) shows the hybrid save taking 41-42 s against upstream's 40-46 s, while run 12's mixed (256k hashes of 9 KB) saved in half of upstream's time. The scan thread finished its 2 GB of small values in about 20 s (threads cpu s = 18-20) but the child spent about 40 s serializing the 24 big hashes one at a time via rdbSaveKeyValuePair from their header copies. While the child is inside a 1.5 s hash it does not read the pipe, so the scan thread blocks in write() on the 1 MB pipe buffer and its work is serialized behind the child's instead of overlapping with it. With small fork-class units (9 KB hashes) the child drains the pipe often enough that the two halves overlap.

Options, in order of preference:

1. The scan thread keeps completed 64 KB staging buffers in an in-memory queue (the mq queue already exists for main-thread emissions) and a writer step drains it to the pipe without blocking; the queue is bounded by bgsave-hybrid-max-retained-bytes (its bytes already count as retained). The scan thread never blocks on the child; at the bound it waits rather than aborting.
2. A reader thread in the child draining the pipe into a memory queue while the child's main thread serializes; costs a thread and a copy in the child.
3. A larger pipe (F_SETPIPE_SZ, capped by /proc/sys/fs/pipe-max-size, 1 MB default) is a partial fix only.

Measure with the run 13 recipe before/after. Target: save time within 10% of max(scan-only time, child-only time) instead of their sum; the win shows when the scan's share exceeds the child's (e.g. the small dataset plus 4 hashes of 1M fields). Record the queue's peak (snap_save_queued_bytes).

Created by hand on 2026-09-26 because the backlog MCP task_create call hung (timed out after 1800 s and then reported another operation in progress).
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 On a dataset where the scan thread's work exceeds the child's (e.g. small 4 GB plus 4 hashes of 1M fields), save time is within 10% of max(scan-only time, child-only time)
- [ ] #2 The in-memory queue is bounded by bgsave-hybrid-max-retained-bytes and its peak is visible in INFO
- [ ] #3 No regression on run 12 small and mixed cells (save time, extra memory, main-thread CPU) within noise
- [ ] #4 Race harness, TSan and the feature-on unit tests pass
<!-- AC:END -->
