---
id: TASK-1.32
title: >-
  Fix the main-thread queue byte count that drifted by the emitted volume (run
  34's false hard-mark abort); decide on priority forwarding at the queue bound
status: Done
assignee:
  - '@Claude'
created_date: '2026-10-04 13:03'
updated_date: '2026-10-04 13:21'
labels:
  - bgsave
milestone: m-3
dependencies:
  - TASK-1.31
documentation:
  - poc-hybrid-bgsave-spec.md
  - utils/snap-bench/RESULTS.md
modified_files:
  - src/snapshot.c
  - tests/unit/snapshot.tcl
  - utils/snap-bench/RESULTS.md
  - utils/snap-bench/run34b.csv
parent_task_id: TASK-1
priority: high
ordinal: 28000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Run 34 (2026-10-04) aborted one hybrid save on the retained-bytes hard mark with "0 bytes retained plus 268450621 queued": small dataset, 50k writes/s of 100-byte strings, 31 s save. The counted queue is the main thread's emit-ahead records; the scan thread forwards them only at window boundaries (snapSaveWindowDone), and when the child is the bottleneck the thread's own records fill the 100 MB queue (bgsave-hybrid-max-queued-bytes) and the thread blocks inside snapOutEnqueue, so the main thread's records accumulate at the write rate for the whole save (50k/s x 31 s x 170 B is about 265 MB, the 256 MiB mark by construction). The same bytes would be serialized by the thread for those buckets later, so forwarding them first changes only the order through the pipe.

User's design (2026-10-04): let the scan thread write the main thread's data to its pipe while it waits at the bound, and suspend its own iteration meanwhile. In the bound wait of snapOutDrain, before each poll, move the main thread's queued records into the thread's small queue at its head (behind a partially written message) and account them under the queue bound; the walk stays suspended until the total drops under the bound, which the child's consumption achieves because the only inflow left is the emit-ahead stream (a few MB/s against the child's hundred). Ordering between the two sources is free (snapSaveWindowDone already relies on it), main-thread BUFFER records carry no big-value dependency, small_seq counts them wherever inserted.

The hard mark must keep counting the forwarded records or a stopped child makes memory unbounded (the thread's queue would grow with forwarded records and the mark would never fire): count retained plus the excess of the total queued bytes over the queue bound. With a stalled child that aborts as today, 100 MB later; with a slow child it never fires.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 mq_bytes counts each emitted record exactly once: after ahead writes stop during a slow save, INFO snap_save_queued_bytes falls to the queue's real content (under 1 MB in the harness test) instead of staying at the emitted volume
- [x] #2 The small 50k cell of the run 34 recipe completes without a hard-mark abort on the same instance (run 34b, 3 reps), with save time and memory as in run 34
- [x] #3 unit/snapshot hybrid and stock pass, C unit tests pass; RESULTS.md run 34 note corrected to the real cause
- [x] #4 Decision recorded on the priority-forwarding design (forward the main thread's records while the walker waits at its bound): adopted or left out, with the reasoning
<!-- AC:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
2026-10-04: the run 34 abort was an accounting bug, not memory. snapSaveEmitBucket incremented mq_bytes per record appended to the staging buffer AND snapSaveQueuePushLocked incremented it again by the whole stage when flushed; the forwarding decremented once, so mq_bytes drifted by the total emitted volume (since 3da2cd28a0, 2026-09-22; run 31's same cell computed to just under the mark, run 34's just over). Reproduced locally: after 100k ahead writes during a slow save, snap_save_queued_bytes stayed at 9.37 MB, the volume of the 132,612 emitted keys, with the queue empty. Consequences besides the false abort: INFO snap_save_queued_bytes and snap_last_save_queued_peak overreported, and freeMemoryGetNotCountedMemory excluded the drifted figure from eviction accounting during saves. Fix: count the stage once, at hand-over. Regression test added (queue figure drops under 1 MB once ahead writes stop). With the counter right, the walker's window-boundary forwarding keeps the main thread's queue small on its own; the priority-forwarding design (forward while waiting at the bound) adds little and is left out unless wanted.

2026-10-04: run 34b on the same instance, hybrid rebuilt with the fix, small 50k x 3 reps: all ok, 31.9 / 33.8 / 32.8 s, no hard-mark abort, memory and latency as before. Local: C unit 184/184, unit/snapshot hybrid 76 and stock 76 ok (new regression test 'the main thread's emit-ahead queue drains as the walker moves; its byte count does not drift'). Decision on the user's priority-forwarding design: left out; with the counter right the window-boundary forwarding keeps the main thread's queue small because each boundary comes at the child's pace, and forwarding at the bound would move the same bytes between two queues without lowering the total. Can be added later as a small change in snapOutDrain's bound wait if main-thread records should jump the thread's own.
<!-- SECTION:NOTES:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
Run 34's single hard-mark abort was an accounting drift: snapSaveEmitBucket counted each emitted record in mq_bytes when appending it to the staging buffer and snapSaveQueuePushLocked counted the whole stage again at hand-over, while forwarding subtracted once, so the figure grew by the total emitted volume (since 3da2cd28a0). Fixed by counting the stage once, at hand-over. Besides the false abort this corrects INFO snap_save_queued_bytes and snap_last_save_queued_peak and eviction's not-counted memory during saves. Regression test added; run 34b (same instance, 3 reps of the failing cell) all ok with unchanged times. The priority-forwarding design discussed for this task was left out with the reasoning recorded.
<!-- SECTION:FINAL_SUMMARY:END -->
