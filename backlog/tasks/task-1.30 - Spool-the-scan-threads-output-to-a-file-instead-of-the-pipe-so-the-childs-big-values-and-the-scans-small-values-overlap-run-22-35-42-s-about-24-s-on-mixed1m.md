---
id: TASK-1.30
title: >-
  Spool the scan thread's output to a file instead of the pipe, so the child's
  big values and the scan's small values overlap (run 22: 35-42 s -> about 24 s
  on mixed1m)
status: Done
assignee:
  - '@Claude'
created_date: '2026-10-01 06:30'
updated_date: '2026-10-01 10:15'
labels:
  - hybrid-bgsave
  - performance
dependencies: []
references:
  - utils/snap-bench/run22-probe-samples.txt
  - 'src/snapshot.c snapSendMsg, snapOutDrain, snapSaveChildWrite'
documentation:
  - utils/snap-bench/RESULTS.md
  - poc-hybrid-bgsave-spec.md
parent_task_id: TASK-1
priority: high
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Run 22 (RESULTS.md, 2026-10-01) probed the child wall-time gap on mixed1m at 4 GB with per-0.5 s samples of both sides' scheduler state. The gap is not a stall: the scan thread's 19.5 s and the child's 21-24 s run one after the other. Idle, the child sleeps on the pipe for 14 s while the scan thread serializes small values (the 24 big hashes come late in scan order), then the child serializes the hashes for 21 s while the scan thread sleeps in poll with its queue pinned at the 16 MB bound. Under writes the first HSET on each hash emits it to the child at once, so the child is busy for 25 s first and the scan thread is parked after 195k keys, then the scan's remaining 19M keys take 17 s with the child idle. Full overlap would give about 24 s against 35-42 today and 40-46 for stock. The 128 MB bound of run 17 (26-29 s) was buying part of this overlap with anonymous parent memory; a bigger queue is the wrong fix (1 GB of scan output to hide 21 s of hashing).

Design (to approve): replace the pipe with a spool file. The scan thread appends its completed staging buffers (BUFFER messages) to a spool file in the save's directory (`dir`, or a configurable spool dir; tmpfs-backed /dev/shm is fine for diskless hosts) with plain write(); no bound, no poll, no backpressure (disk space is the bound; page cache is reclaimable, unlike the queue's anonymous memory). KVOBJ messages (big-value headers) keep going to the child over the pipe, which is now low-volume. The child serializes big values as they arrive and, between them and at the end, copies the spool to its output (RDB file or replica socket) with read()+rioWrite, tracking the spool offset; an END marker (or the pipe's END) tells it the spool is complete; the CRC and the SELECTDB/RESIZEDB/SLOT_INFO tracking stay in the child as today (the spool carries the same message headers as the pipe stream, so snapSaveChildWrite is unchanged apart from the source). Abort: unlink the spool. Memory: the queue, its bound (`bgsave-hybrid-max-queued-bytes`), `snapOutDrain` and the pool go away or shrink to the KVOBJ path; INFO queued bytes become spool bytes. Optional second step: a per-db index of SNAP_FORK values kept at placement time (snapPlaceValueArena), sent to the child at the fork instant, so the child starts on the big values at t = 0 instead of in scan order; the walker then skips SNAP_FORK entries and the main thread never emits them.

Expected: mixed1m idle about 24 s (max of scan 19.5 and child 21), under writes about 25 s; small and mixed unchanged (their child is fast); parent memory during the save back to the side table + KVOBJ queue (a few MB). Risk: an extra 1 GB write+read through the page cache per save on disk-backed dirs (sequential, cheap; measure on the host's root disk as well as tmpfs); diskless sync on a host with no writable dir needs a fallback (pipe as today, or /dev/shm).
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 mixed1m at 4 GB idle and at 20k writes/s saves in at most 1.15 x max(scan thread CPU, child CPU) (run 22 numbers: about 24 s), measured on the c7i.4xlarge against run 21
- [x] #2 Parent memory during the save on mixed1m is the side table plus the KVOBJ queue (under 50 MB), not the spooled output; INFO exposes the spool size
- [x] #3 Small and mixed cells of run 12/16 unchanged within noise; diskless full sync (run 18/21 recipe) unchanged or faster
- [x] #4 Race harness, unit/snapshot both modes, TSan and ASan clean; abort, child killed and FLUSHDB-detach paths unlink the spool
- [x] #5 Spec section 6 (channel) and the comparison table describe the spool; redis.conf documents the spool directory and the removed or narrowed queue bound
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
Approved by the user 2026-10-01 ('do it'). 1. Spool file: parent creates `<dir>/temp-snap-<pid>.spool` at save start (O_RDWR|O_CREAT|O_EXCL), opens a second read-only fd for the child, unlinks the name at once (both fds stay valid, nothing to clean on a crash); the child inherits the read fd, the scan thread owns the write fd. Fallback to the pipe for everything when the file cannot be created. 2. Scan thread: BUFFER messages (staging buffers, header + payload as today) go to the spool with blocking write(); KVOBJ/END/ABORT keep the pipe and the existing outbound queue (now low-volume). Main-thread emissions forwarded at window boundaries follow the same split. 3. Child: between pipe messages, read the spool from its offset and write the chunks to the output through the same message parser (SELECTDB/RESIZEDB/SLOT_INFO tracking unchanged); poll the pipe with a short timeout when the spool is at EOF-so-far; on END drain the spool to its end, then finish; on ABORT stop. 4. INFO: snap_save_spool_bytes (written by the scan thread); queued bytes keep describing the pipe queue. Config `bgsave-hybrid-spool` yes|no (default yes) for the A/B and for hosts without a writable dir. 5. Tests: spool used (INFO > 0, file unlinked), abort and killed child leave no file, diskless sync through the spool, FLUSHDB-detach unaffected. 6. Spec section 6 and comparison table, redis.conf. 7. Measure on the host: mixed1m idle/20k and small, spool vs no-spool, against run 21/22.
<!-- SECTION:PLAN:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
The scan thread's output overflows to an unlinked spool file once its queue is full, so the child's big values and the scan's small values run in parallel instead of in series. mixed1m at 4 GB: 21-26 s at every write rate against 35-42 s before and 40-46 s for stock.

**Why.** Run 22 (probe, RESULTS.md) showed the two halves of a mixed1m save running one after the other: the child cannot read the pipe while it serializes a 90 MB hash, the scan thread blocks after 16 MB of queue, and whichever side starts first finishes before the other begins. Neither side waited on the run queue or page-faulted. The 128 MB bound of run 17 bought part of the overlap with anonymous memory; a bigger queue was the wrong fix.

**What landed (c85968b09, bc33196f5).**
- An unlinked `temp-snap-<pid>.spool` in the working directory, opened for writing and reading before the fork and unlinked at once: a crash of either side leaves nothing behind. `bgsave-hybrid-spool yes` by default; off = the pipe and queue as before (for a working directory that cannot take a file of the dataset's size).
- The queue stays the primary path (the user's refinement, 2026-10-01): a BUFFER message that would push the queue past `bgsave-hybrid-max-queued-bytes` switches the scan thread to appending BUFFER messages to the spool (same framing), and it switches back once the child has drained the queue to half. KVOBJ, END and ABORT always take the pipe and the queue. A dataset the child keeps up with never touches the file (small: 0 bytes spooled in run 24).
- The child alternates: a pipe message when one is waiting (the queue drains, big values start promptly), else the next complete spool message from its own offset with `pread()` (a partially written message is retried from the same offset); it polls the pipe for 5 ms when both are empty, and after END drains the spool to its end. SELECTDB/RESIZEDB/SLOT_INFO tracking unchanged.
- The scan thread closes its spool end when done, so the child's exit frees the pages: the parent's close of a 1-3 GB unlinked file froze its pages on the main thread for 55-155 ms in run 23 (the first, spool-everything version).
- INFO `snap_save_spool_bytes`, `snap_last_save_spool_bytes`; bench.py records the latter; run.sh gained `hybrid-nospool`.

**Measured (c7i.4xlarge, mixed1m 19.2M keys / 4 GB, run 24, overflow spool vs pipe-only):** idle 25.4-26.1 s vs 34.8-37.0 (stock 40-43); 20k writes/s 21.6-24.4 vs 40.4-40.5 (stock 45.6-46.2); 50k 21.2-21.4 vs 40.3-42.3 (stock 39.7-44.1). Child copy-on-write 0.83-0.91 GB vs 1.24 at 20k and 1.41 vs 1.73-1.75 at 50k; main-thread CPU 5-6 s vs 9 at 20k. Queue peak 16 / 23 / 31-33 MB vs 16 / 44-45 / 88-95; parent growth 1-11 MB; 0.8-1.1 GB in the spool (page cache on disk, RAM on the benchmark's tmpfs dir, outside used_memory, freed at child exit). Worst PING 11.5-14.8 ms and worst write 15.7-18.2 ms, same as before (run 23's close spike gone). small: 0 bytes spooled, 31.5 vs 29.9 idle and 31.3 vs 30.7 at 20k (single reps, within the dataset's 29-33 s spread). Full sync (run 25): 16.2 / 16.9-17.0 s either way (replica load is the floor), 0.8-0.9 GB through the spool, memory and latency within noise. AC1: 21-26 s against max(scan 19-20, child 21-25) = within 1.06x.

**Tests.** The queue fills first and nothing is spooled while the child keeps up; with the child SIGSTOPped the queue stops at its bound and the overflow goes to the spool, no file appears in the directory, the save completes and reloads equal; spool off reloads equal with 0 spooled; a child killed while draining the spool leaves no file and the next save works; the two queue-bound tests switch the spool off for their duration. C unit 166/166; unit/snapshot feature on and off; TSan and ASan 58/58 clean on the final build.

**Full suites on the final head (landing prep, worktree .scratch/land):** stock mode all pass except two defrag tests that pass alone (load flakes under 12 clients); feature on: 163/164 units, two failures that pass alone ('replica buffer don't induce eviction', 'Hybrid diskless sync converges under write load' 3/3 alone), the last unit (integration/replication) cut short by my own second runtest in the same tree and rerun separately. Merge with upstream unstable (23 commits behind) is conflict-free.

**Docs.** redis.conf (bgsave-hybrid-spool), spec section 6 (spool paragraph, comparison table row), RESULTS.md runs 22-25.

**Follow-ups.** The optional per-db index of SNAP_FORK values (child starts on big values at t = 0 in the idle case) is not needed for the overlap: the remaining gap to max(scan, child) is 1-6%. On a tmpfs working directory the spool is RAM; a `bgsave-hybrid-spool-dir` knob would let it go to disk. The main-thread queue of emitted buckets is still bounded only by the hard mark (23-33 MB peaks here).

Correction to the suites paragraph: 'replica buffer don't induce eviction' passes with the whole unit/maxmemory run alone with the feature on (43 s, all tests pass); run with `--only` it hits the harness's own 'process was not stopped' assertion, which is a filter artefact, not the feature. integration/replication with the feature on: all tests pass (531 s).

Addendum 2026-10-01 (fd2034f0b, 13f99424d), after the user asked for the true memory consumption. The spool's pages are outside used_memory and RSS; now the child punches holes behind its reads (fallocate PUNCH_HOLE on 4 KB boundaries, 16 MB at a time, posix_fadvise DONTNEED where holes are unsupported) and flushes the spool after every big value, so the file holds about one big value's worth of scan output; INFO snap_save_spool_bytes is that footprint (written minus the first data offset, resumed from the previous frontier), snap_last_save_spool_peak its peak, snap_save_spool_written_bytes what was written; bench.py records the peak. Run 29 (final, c7i.4xlarge, mixed1m at 4 GB): saves 21-26 s at every write rate against 35-42 for the pipe shape, spool footprint peak 61-77 MB while 0.92-1.06 GB pass through, queue 16-33 MB, parent growth 2-14 MB, worst PING 10-16 ms (the fork stall), worst write 15-17 ms; small never spools (idle 32.9 vs 29.6 s single reps, flagged as noise to settle with two interleaved reps). Three defects found and fixed on the way, by the test runs: (1) the first child policy handled queued big values back to back and drained the spool only afterwards (run 26); (2) unaligned hole punches pinned SEEK_DATA at the first straddling block, so the footprint read 'written minus 16 MB' (run 27); (3) two child crashes: a torn spool header read while the scan thread was still writing it (fixed with a committed length in a MAP_SHARED page the child never reads past), and, predating the spool, the arena's 16 TB range being absent in the hybrid child so the kernel reused its addresses for the child's heap and zmalloc_is_nofork routed a realloc of a child buffer to the parent-only arena (fixed: the child re-reserves the range PROT_NONE first thing after the fork). The 'converges under write load' test's occasional second save was that crash's retry; 18/18 in a loop since. Verified on the final build: C unit 166/166, unit/snapshot both modes, ASan 58/58 clean, TSan no report (one timing failure of the bucket-layout test under TSan load, passes alone). RESULTS.md runs 26-29.
<!-- SECTION:FINAL_SUMMARY:END -->
