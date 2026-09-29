---
id: TASK-1.27
title: >-
  Scan thread blocks on the 1 MB pipe while the child serializes a huge
  fork-class value: buffer completed blocks in memory so the two overlap
status: Done
assignee:
  - Claude
created_date: '2026-09-26 19:52'
updated_date: '2026-09-29 23:54'
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
- [x] #1 On a dataset where the scan thread's work exceeds the child's (e.g. small 4 GB plus 4 hashes of 1M fields), save time is within 10% of max(scan-only time, child-only time) instead of their sum
- [x] #2 The in-memory queue is bounded by bgsave-hybrid-max-retained-bytes and its peak is visible in INFO (snap_save_queued_bytes or a new peak field)
- [x] #3 No regression on run 12 small and mixed cells (save time, extra memory, main-thread CPU) within noise
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

2026-09-30 AC2: with the default bgsave-hybrid-max-retained-bytes (256 MiB) the zero-copy build's mixed1m saves report snap_last_save_queued_peak = 230-268 MB idle (the queue alone) and 287-290 MB at 20k writes/s (queue at the bound plus the main-thread queue), i.e. the scan queue stops at 268,435,456 bytes plus one message and INFO shows both the live value (snap_save_queued_bytes) and the peak. bench.py now records the peak in impl_stats. Second pass (hybrid2 = zero-copy build) mixed1m: idle 32.6 s (rep after a fresh populate) / 23.8 s (child 20.3 / 22.2 s), 20k 34.0 / 35.2 s (before 40.9 / 41.9, first queue version 37.0 / 37.3). Two false starts of the second pass: run16b.sh did not set CLI, so run.sh's health check used a redis-cli path that does not exist on the host, reported 'server start failed' and left the server running on port 7600 for the next cells; fixed by exporting CLI alongside HYB2.

2026-09-30 memory fix (16fda179b): run 16's second pass (hybrid2) showed used_memory growing 5-20 GB during mixed saves with RSS +0.6 GB: pooled 64 KB staging buffers were handed to 170-byte KVOBJ messages and sat in the queue counted as 170 bytes, and greedy sds doubling made each staging buffer a 160 KB allocation; maxmemory would have seen it. Now the pool serves staging-sized requests only, buffers are allocated exactly (sdsMakeRoomForNonGreedy), the stage flushes when under 8 KB of headroom, and oq_bytes counts allocation sizes. Local check: used_memory during a save tracks the queued peak (26/33 MB, 75/78 MB) and returns to baseline. The stalled-child test now also holds 3000 hashtable hashes and asserts used_memory grows < 8 MB while the queue sits at its bound. Re-verified on the fixed build: tcl 45/45, TSan 39/39 (no reports), C unit 164/164. Third AWS pass (hybrid3 = fixed build) running, appended to run16.csv.

2026-09-30 AC1/AC3 closed with run 16 (RESULTS.md). AC1: mixed1m idle from-cache cells: v1 21.4 s vs child 20.7, v2 23.8 vs 22.2, v3 (final) 24.6 vs 23.7, i.e. within 3-7% of max(scan, child); before 38.1/38.3 = their sum. Caveat recorded: the first cell after a fresh populate is 30-40% slower for every binary (32.2 s for v3, 27-38 for before on other datasets), and at 20k writes/s the gain is only 41-42 -> 36-39 s because the child's wall time exceeds its CPU by 10-15 s; both left as an observation. AC3: pass 4 (3 interleaved reps, before vs v3): mixed 20k 21.3-23.1 vs 20.6-22.5 s, small 20k 31.1-31.5 vs 30.1-31.1 s, main-thread CPU within 0.3 s, mixed 50k 10% faster (30.9-31.9 -> 26.9-28.3) with 1.5 s less main CPU; the +5-7% single-rep cells of passes 1-3 were noise (the unchanged binary itself spreads 21.3-23.1 on the same cell). Extra memory: the queue, up to the bound while the child is behind (231-293 MB on mixed1m, 72-98 on mixed under writes, 0 on small), released at save end.
<!-- SECTION:NOTES:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
## Outbound queue on the scan thread (TASK-1.27)

**Problem.** The scan thread wrote the child's pipe with blocking `write()`. While the child serialized one huge fork-class value (a 90 MB hash takes it about 1.5 s) it did not read the pipe, the 1 MB pipe filled and the scan stalled, so on datasets with a few huge values the save took scan + child instead of max(scan, child): 41 s instead of about 24 on mixed1m (run 13).

**Change (src/snapshot.c, e9dbd7715 + 16fda179b).**
- Every message the scan thread produces or forwards goes to a queue it owns; the pipe's write end is O_NONBLOCK and `snapOutDrain()` writes what the pipe takes after every enqueue, keeping an offset into the head message for partial writes. The child is unchanged.
- Past `bgsave-hybrid-max-retained-bytes` (frozen at save start) the thread waits for the child with poll() instead of growing; it never aborts for that. The main-thread hard mark (retained + mq) is unchanged, so worst-case save memory is up to twice the configured value. END drains everything first; ABORT drops the queue and is delivered blocking.
- The staging buffer is a BUFFER message in the making (header reserved up front, handed to the queue without a copy); drained staging buffers return to a 16-entry pool. Allocations are exact (`sdsMakeRoomForNonGreedy`), the pool serves staging-sized requests only, the stage flushes when under 8 KB of headroom, and the queue's bytes are allocation sizes. (The first zero-copy version let 170-byte KVOBJ messages pin 160 KB pooled buffers and greedy doubling made each 64 KB buffer a 160 KB allocation: `used_memory` grew 5-20 GB during mixed saves. Fixed and covered by a test.)
- INFO: `snap_save_queued_bytes` = main-thread queue + scan queue; new `snap_last_save_queued_peak`; the retained peak includes both queues.

**Tests.** unit/snapshot: a SIGSTOPped child fills the queue to a 256 KB bound and the thread waits with clients answering and `used_memory` growing < 8 MB, then the save completes and reloads equal; 4 big hashtable hashes + 400k strings give a queued peak above the 1 MB pipe. Both switch rdbcompression off for volume and always SIGCONT the child before asserting. Feature-on tcl 45/45, TSan (`setarch -R`) 39/39 with no reports, C unit 164/164.

**Measured (run 16, c7i.4xlarge, 4 GB, RESULTS.md).** mixed1m idle 38 s -> 22-25 s, within 3-7% of the child alone; mixed1m 20k 41-42 -> 36-39 s; mixed 50k 10% faster with 1.5 s less main-thread CPU; small and mixed 20k unchanged over 3 interleaved reps. Cost: parent memory grows by up to the bound while the child is behind (231-293 MB on mixed1m, 72-98 MB on mixed under writes, 0 on small), released at save end; CoW 3-6% lower.

**Also.** snap-bench: `hybrid-before`/`hybrid2`/`hybrid3` configs in run.sh for A/B runs, `snap_last_save_queued_peak` recorded by bench.py, run16*.sh drivers and run16.csv. Spec section 6 and the comparison table describe the queue; blog section 7 and section 9 updated.

**Open observations (not fixed here).** (1) Under writes and in the first cell after a fresh populate the child's wall time exceeds its CPU time by 10-15 s, which caps the gain at 20k writes/s; cause unknown (scan-thread quiesce waits vs child waits). (2) Worst-case save memory is up to twice the configured retained bound; a separate queue bound or counting the scan queue toward the abort are alternatives if that matters.
<!-- SECTION:FINAL_SUMMARY:END -->
