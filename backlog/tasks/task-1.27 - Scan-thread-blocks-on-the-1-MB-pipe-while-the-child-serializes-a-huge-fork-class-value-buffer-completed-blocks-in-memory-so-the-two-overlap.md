---
id: TASK-1.27
title: >-
  Scan thread blocks on the 1 MB pipe while the child serializes a huge
  fork-class value: buffer completed blocks in memory so the two overlap
status: In Progress
assignee:
  - Claude
created_date: '2026-09-26 19:52'
updated_date: '2026-09-29 20:57'
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

Evidence from the CSVs (idle cells): run 12 mixed, hybrid: scan thread 18.2 s CPU, child 17.6 s CPU, save 18.4 s = max of the two (full overlap). Run 13 mixed1m, hybrid: scan thread 19.9 s, child 23.3 s, save 41.7 s = their sum (no overlap); upstream's single child needs 40.5 s CPU for the same data. So the child is not the floor on mixed1m: with the two overlapping, the save should take about 24 s instead of 42.

Measure with mixed1m (run 13 recipe) before/after: target is save time within 10% of max(scan CPU, child CPU), about 24 s. Also record the snap_save_queued_bytes peak.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 On a dataset where the scan thread's work exceeds the child's (e.g. small 4 GB plus 4 hashes of 1M fields), save time is within 10% of max(scan-only time, child-only time) instead of their sum
- [ ] #2 The in-memory queue is bounded by bgsave-hybrid-max-retained-bytes and its peak is visible in INFO (snap_save_queued_bytes or a new peak field)
- [ ] #3 No regression on run 12 small and mixed cells (save time, extra memory, main-thread CPU) within noise
- [x] #4 Race harness, TSan and the feature-on unit tests pass
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
Approved 2026-09-29 (option 1 of the description).

1. Outbound queue on the scan thread (snapshot.c): snapSendMsg appends header+payload as one owned buffer to a queue owned by the scan thread (no lock). Main-thread emissions keep using mq and are forwarded into this queue at window boundaries as today. KVOBJ payloads are copied into the queued buffer (small header allocations).
2. Pipe write end set O_NONBLOCK. A drain step after every enqueue and at every window boundary writes as much as the pipe accepts, keeping an offset into the head message for partial writes. The child is unchanged.
3. Bound: when queued bytes exceed bgsave-hybrid-max-retained-bytes the scan thread drains in blocking mode (poll POLLOUT with a short timeout, checking abort) until under the bound. It waits, never aborts. The main-thread hard-mark abort (retained + mq) is unchanged. Documented: worst-case save memory up to twice the configured value. 0 keeps meaning no limit for both.
4. End: before END drain everything blocking, END through the same path. Abort: drop the queue, write ABORT blocking ignoring errors. Pipe errors abort the scan as before.
5. INFO: snap_save_queued_bytes = mq + scan queue; new snap_last_save_queued_peak tracked exactly on the scan thread; retained peak keeps including queued bytes.
6. Tests (tests/unit/snapshot.tcl): (a) bound and wait: small retained limit, SIGSTOP the child via get_child_pid, queued bytes rise to the bound and stop, save still in progress, clients answer, SIGCONT, save completes, reload equal; (b) absorption: several 300k-field hashes plus a few hundred thousand small keys, queued peak > 1 MB, digest equal after reload; (c) rerun the harness tests, feature-on tcl unit under SANITIZER=thread, C unit tests (redis-server test snapshot).
7. Measurement: local scaled-down mixed1m idle before/after; then on the AWS c7i.4xlarge (54.219.8.216, given 2026-09-29): mixed1m idle+20k at 4 GB before/after (AC1) and run 12 small and mixed cells before/after (AC3). Results to RESULTS.md as run 16.

Out of scope: child, funnel, mq, overlay.
<!-- SECTION:PLAN:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
2026-09-29 implementation: snapshot.c gets an outbound queue owned by the scan thread (list of sds, header+payload), the pipe write end is O_NONBLOCK, snapOutDrain() has three modes (TRY after every enqueue, BOUND = wait with poll(100 ms) until oq_bytes <= bgsave-hybrid-max-retained-bytes frozen at save start, ALL = wait until empty for END and ABORT). snapSendMsg() enqueues, drains TRY, then BOUND when over the limit; a broken pipe aborts the scan as before. The unused blocking snapWriteAll() was removed (CI builds with -Werror). INFO: snap_save_queued_bytes = mq + oq, new snap_last_save_queued_peak; snapSaveCron's retained peak includes both queues, its abort still counts retained + mq only. Two tcl tests added to unit/snapshot (stalled child via SIGSTOP fills the queue to the bound and the thread waits; big hashes + 400k strings give a queued peak above the 1 MB pipe). C unit tests: 164/164. AWS c7i.4xlarge 54.219.8.216 prepared: tmpfs 24G, build tools, ~/redis-before built from 444feb8d4 (REDIS_CFLAGS empty); run.sh gained a hybrid-before config (HYB_BEFORE) for A/B runs; run16.sh driver drafted (mixed1m 0/20k x2 reps, small and mixed 0/20k/50k x1, per-dataset cache cleanup).

2026-09-29 run 16 (AWS c7i.4xlarge, hybrid-before 444feb8d4 vs hybrid with the queue, 4 GB, interleaved): mixed1m idle before 38.1/38.3 s -> after 27.3/21.4 s; scan CPU 18-19 s, child CPU 21-25 s, so the save now sits at the child (rep 2: 21.35 vs 20.68 = +3%, rep 1: 27.26 vs 24.56 = +11%). Cost: parent memory grows by the queue bound during the save (parent_growth 225-268 MB, retained peak 235-256 MB reported, since queued bytes count) where before it was 0-1 MB. mixed1m 20k: before 40.9/41.9 s -> after 37.0/37.3 s only; child CPU unchanged (23.5-24.6) and the queue sat at the bound (retained peak 277-283 MB), so under writes the child's wall time is what limits and the 256 MB bound throttles the scan thread; a larger bound would test whether the gap closes. CoW 1.17-1.18 GB vs 1.24-1.25 GB, load p99 unchanged. small idle: before 29.3 s -> after 32.5 s, scan CPU 29.1 -> 32.3 (+11%): the first version copied every 64 KB staging buffer into a fresh sds per message (4 GB of memcpy plus fresh-page faults). Fixed with zero-copy staging: the staging buffer reserves the message header up front and is handed to the queue as-is, drained buffers go back to a 16-entry pool (snapOutTake/snapOutRecycle). tcl unit 45/45 and C unit 164/164 on the zero-copy build; being measured as 'hybrid2' in a second pass appended to run16.csv. Test note: the first version of the stalled-child test used compressible values (rdbcompression shrank 60k records to 1.8 MB, under the pipe plus bound) and left the child SIGSTOPped on failure, which wedged the next test; now rdbcompression is off in both new tests and the child is always continued before any assertion. TSan on this 6.8 kernel needs `setarch $(uname -m) -R ./runtest ...` (ASLR incompatibility: 'FATAL: ThreadSanitizer: unexpected memory mapping').

2026-09-29 AC4: feature-on tcl unit/snapshot 45/45 on the debug build (harness tests included), 39/39 on the TSan build (arena tests skip under libc malloc) with no sanitizer reports, C unit tests 164/164.
<!-- SECTION:NOTES:END -->
