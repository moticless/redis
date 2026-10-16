---
id: TASK-1.55
title: >-
  Profile the scan thread and the child writer; prefetch next buckets and
  headers if memory-latency-bound
status: In Progress
assignee: []
created_date: '2026-10-16 09:22'
updated_date: '2026-10-16 14:07'
labels:
  - hybrid-bgsave
  - performance
dependencies:
  - TASK-1.50
references:
  - src/snapshot.c scan loop
  - utils/snap-bench/RESULTS.md Run 40
parent_task_id: TASK-1
priority: medium
ordinal: 51000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Save time on the all-arena small dataset is ~33 s versus ~36 s for the classic child (Run 40), so something other than the fork is the limit. Profile (perf) the scan thread, the child writer and the main thread during a save on the AWS benchmark host (one new c7i.4xlarge per session) and find the limit: pointer chasing (bucket, header, value), serialization, compression, CRC64, or the pipe/spool to the child. If the scan thread is memory-latency-bound, prefetch the next buckets and their kvobj headers in the scan loop (as dictScan/dictPrefetch do elsewhere). Record the profile and the before/after in RESULTS.md. Ideas conditional on the profile (separate tickets if confirmed): per-buffer CRC64 computed on the scan thread and combined in the child; a compression thread.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 perf profiles of the scan thread, child writer and main thread for mixed1m and small at 0 and 50k writes/s are summarized in the task notes with the identified limit
- [ ] #2 If prefetch is implemented: save time before/after on small and mixed1m recorded in utils/snap-bench/RESULTS.md, with no regression in write latency
- [ ] #3 If the limit is the child's CRC64 or compression, a follow-up task is created with the measured share
<!-- AC:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
Local evaluation (no perf: perf_event_paranoid=4; used per-thread CPU from /proc and gdb stack sampling, i7-1260P, noisy/thermal). 10M keys x 64 B, all arena, default rdbcompression. Scan thread (snap_scan) is the bottleneck: ~8.7 s CPU in a 9 s save. 40 gdb samples of that thread: lzf_compress 16 (40%), snapKeyClass 10 + sdsType 4 (35%, first touch of kvobj header/key = cache miss), rest spread (rio, memcpy, free). Without compression the scan CPU drops from ~11 s to ~4.3 s: LZF is more than half of the scan thread's cost.

Prefetch implemented (dictWalkPrefetchBucket in dict.c, SNAP_SCAN_PREFETCH=16 buckets ahead in snapScanWalkDict, first entry of the bucket, one cache line; two lines measured the same). A/B alternating runs, 10M keys: scan CPU ticks pf0 avg ~1170 vs pf16 ~1000 (-14%), save time ~11.3 s -> ~10.3 s (-9%); without compression ~4.4 s -> ~3.9 s (-10%). After it the sampled miss moves from snapKeyClass to sdsType (key sds header), i.e. part of the latency remains. Local noise is large (same config 8.7-13 s); not yet measured on AWS or with writes/latency (AC #2 open). ./redis-server test snapshot 200/200 and ./runtest --single unit/snapshot --clients 4 pass with it.

Finding for follow-up (AC #3): LZF compression on the single scan thread is the larger cost (about 40% of its samples); a compression thread or skipping LZF for small values is the lever, not prefetch. No follow-up task created yet.

AWS verification (fresh c7i.4xlarge, 10M keys x 64 B, 3 alternating rounds, recorded as Run 42 in RESULTS.md): the local finding holds and the prefetch is stronger there. Scan CPU 926 -> 730 ticks (-21%) with compression, 375 -> 281 (-25%) without; save time 9-10 s -> 7 s, 4 s -> 3 s. perf on the scan thread: LZF 46-61% with compression; without it, before the prefetch 66% of the thread was stalls on the value and the kvobj header, after it ~22% (the cost moves to memcpy of the value).

Prefetch final form: stage 1 first entry of bucket i+16, stage 2 the raw string value of bucket i+8; both only inside the window and only for buckets the scan will read (not the ones emitted ahead by the main thread). Sanitizers found two bugs in earlier forms (ASAN: reading kv->ptr of a kvobj shorter than the field; TSAN: reading buckets the main thread writes in place); the final form passes ./redis-server test snapshot (200/200 main, 142/142 TSAN and ASAN) and ./runtest --single unit/snapshot --clients 4 on main, TSAN and ASAN with no reports.

Not done: AC #1 (child writer and main thread profiles, mixed1m and small, with writes) and the write-latency check of AC #2. Follow-up for AC #3 not created: the limit is LZF on the single scan thread (46-61% of it), not the child's CRC64; user decides whether a compression-thread ticket is wanted.
<!-- SECTION:NOTES:END -->
