---
id: TASK-1.30
title: >-
  Spool the scan thread's output to a file instead of the pipe, so the child's
  big values and the scan's small values overlap (run 22: 35-42 s -> about 24 s
  on mixed1m)
status: In Progress
assignee:
  - '@Claude'
created_date: '2026-10-01 06:30'
updated_date: '2026-10-01 06:35'
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
- [ ] #1 mixed1m at 4 GB idle and at 20k writes/s saves in at most 1.15 x max(scan thread CPU, child CPU) (run 22 numbers: about 24 s), measured on the c7i.4xlarge against run 21
- [ ] #2 Parent memory during the save on mixed1m is the side table plus the KVOBJ queue (under 50 MB), not the spooled output; INFO exposes the spool size
- [ ] #3 Small and mixed cells of run 12/16 unchanged within noise; diskless full sync (run 18/21 recipe) unchanged or faster
- [ ] #4 Race harness, unit/snapshot both modes, TSan and ASan clean; abort, child killed and FLUSHDB-detach paths unlink the spool
- [ ] #5 Spec section 6 (channel) and the comparison table describe the spool; redis.conf documents the spool directory and the removed or narrowed queue bound
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
Approved by the user 2026-10-01 ('do it'). 1. Spool file: parent creates `<dir>/temp-snap-<pid>.spool` at save start (O_RDWR|O_CREAT|O_EXCL), opens a second read-only fd for the child, unlinks the name at once (both fds stay valid, nothing to clean on a crash); the child inherits the read fd, the scan thread owns the write fd. Fallback to the pipe for everything when the file cannot be created. 2. Scan thread: BUFFER messages (staging buffers, header + payload as today) go to the spool with blocking write(); KVOBJ/END/ABORT keep the pipe and the existing outbound queue (now low-volume). Main-thread emissions forwarded at window boundaries follow the same split. 3. Child: between pipe messages, read the spool from its offset and write the chunks to the output through the same message parser (SELECTDB/RESIZEDB/SLOT_INFO tracking unchanged); poll the pipe with a short timeout when the spool is at EOF-so-far; on END drain the spool to its end, then finish; on ABORT stop. 4. INFO: snap_save_spool_bytes (written by the scan thread); queued bytes keep describing the pipe queue. Config `bgsave-hybrid-spool` yes|no (default yes) for the A/B and for hosts without a writable dir. 5. Tests: spool used (INFO > 0, file unlinked), abort and killed child leave no file, diskless sync through the spool, FLUSHDB-detach unaffected. 6. Spec section 6 and comparison table, redis.conf. 7. Measure on the host: mixed1m idle/20k and small, spool vs no-spool, against run 21/22.
<!-- SECTION:PLAN:END -->
