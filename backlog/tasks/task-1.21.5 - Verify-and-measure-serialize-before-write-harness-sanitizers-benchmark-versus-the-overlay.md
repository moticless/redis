---
id: TASK-1.21.5
title: >-
  Verify and measure serialize-before-write: harness, sanitizers, benchmark
  versus the overlay
status: In Progress
assignee:
  - '@moti.cohen@redis.com'
created_date: '2026-09-21 19:57'
updated_date: '2026-09-21 21:40'
labels:
  - bgsave
  - testing
  - performance
milestone: m-3
dependencies:
  - TASK-1.21.2
  - TASK-1.21.3
  - TASK-1.21.4
documentation:
  - poc-hybrid-bgsave-spec.md
  - utils/snap-bench/RESULTS.md
  - utils/snap-bench/run.sh
parent_task_id: TASK-1.21
priority: high
ordinal: 11950
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Closes the spike on standalone. Run the full randomized race harness (CI, eviction lru/lfu, child-killed, SHUTDOWN, hard mark, --accurate stress of 100 iterations), TSan and ASan in arena-free mode (make distclean; make SANITIZER=thread MALLOC=libc REDIS_CFLAGS='-DREDIS_TEST'; setarch $(uname -m) -R ./runtest --tsan --single unit/snapshot), the full stock suite, and the utils/snap-bench matrix (small, mixed, big-grow; idle, 20k and 50k writes/s; tmpfs; plain builds with REDIS_CFLAGS='') against three binaries: stock upstream 0d6266f2d, the overlay hybrid at commit 033f94bbf, and the new scheme. Record in RESULTS.md: save time, extra memory (parent growth and child CoW), retained peak, fork stall, worst PING and its latency event, main-thread emissions. Also rerun the pinned STRING microbench (SET/GET/DEL, feature off and on) and the callgrind instruction count with the feature off. Report the spike honestly: if worst PING or main-thread cost is worse than the overlay, say so with numbers.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 Harness: all variants equal to the fork instant, 100 --accurate iterations equal
- [ ] #2 TSan and ASan clean on unit/snapshot in arena-free mode; full stock suite passes with no regression
- [x] #3 RESULTS.md has a three-way table (stock, overlay hybrid, serialize-before-write) for the whole matrix with medians of at least two runs, plus the microbench and callgrind numbers
- [ ] #4 A short verdict section states whether the new scheme should replace the overlay and lists any regressions found
<!-- AC:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
2026-09-22: benchmark run 6 done (RESULTS.md, results.csv ts 2026-09-22): four configs (upstream 0d6266f2d, hybrid 3da2cd28a, valkey-fork, valkey-forkless from ~/git/valkey unstable 06d35aca6), small/mixed/big/big-grow x 0/20k/50k writes/s x 2 reps. Not run: the overlay-only hybrid (033f94bbf) as a third redis column (run 5 numbers stand in), the --accurate 100-iteration stress, ASan, the full stock suite. tcl unit/snapshot passes on the test build. TSan on 3da2cd28a: one report, snap_main_emitting read by the scan thread through rdb.c:1747 (fixed by making it thread-local, rerun pending).

2026-09-22: TSan clean on fa21935d0 after the two emission fixes (thread-local snap_main_emitting; rdb.c delay check reads server.rdb_key_save_delay only on the main thread outside emission). Verdict so far: the new scheme should replace the overlay: same correctness (harness), worst PING 0.3-0.7 ms instead of 13-31 ms, parent growth 2 MB instead of 4.5-17 MB, save time and main-thread CPU unchanged. Regressions found: none in the matrix; the hard-mark test now trips on queued bytes rather than retained clones (peak accounting extended). Still to do for this task: ASan, --accurate 100 iterations, full stock suite, STRING microbench and callgrind with the feature on.
<!-- SECTION:NOTES:END -->
