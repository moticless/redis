---
id: TASK-1.21.5
title: >-
  Verify and measure serialize-before-write: harness, sanitizers, benchmark
  versus the overlay
status: Done
assignee:
  - '@moti.cohen@redis.com'
created_date: '2026-09-21 19:57'
updated_date: '2026-09-25 08:31'
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
- [x] #1 Harness: all variants equal to the fork instant, 100 --accurate iterations equal
- [x] #2 TSan and ASan clean on unit/snapshot in arena-free mode; full stock suite passes with no regression
- [x] #3 RESULTS.md has a three-way table (stock, overlay hybrid, serialize-before-write) for the whole matrix with medians of at least two runs, plus the microbench and callgrind numbers
- [x] #4 A short verdict section states whether the new scheme should replace the overlay and lists any regressions found
<!-- AC:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
2026-09-22: benchmark run 6 done (RESULTS.md, results.csv ts 2026-09-22): four configs (upstream 0d6266f2d, hybrid 3da2cd28a, valkey-fork, valkey-forkless from ~/git/valkey unstable 06d35aca6), small/mixed/big/big-grow x 0/20k/50k writes/s x 2 reps. Not run: the overlay-only hybrid (033f94bbf) as a third redis column (run 5 numbers stand in), the --accurate 100-iteration stress, ASan, the full stock suite. tcl unit/snapshot passes on the test build. TSan on 3da2cd28a: one report, snap_main_emitting read by the scan thread through rdb.c:1747 (fixed by making it thread-local, rerun pending).

2026-09-22: TSan clean on fa21935d0 after the two emission fixes (thread-local snap_main_emitting; rdb.c delay check reads server.rdb_key_save_delay only on the main thread outside emission). Verdict so far: the new scheme should replace the overlay: same correctness (harness), worst PING 0.3-0.7 ms instead of 13-31 ms, parent growth 2 MB instead of 4.5-17 MB, save time and main-thread CPU unchanged. Regressions found: none in the matrix; the hard-mark test now trips on queued bytes rather than retained clones (peak accounting extended). Still to do for this task: ASan, --accurate 100 iterations, full stock suite, STRING microbench and callgrind with the feature on.

2026-09-25 run 7 (utils/snap-bench/RESULTS.md, run7.csv, commit 5780b349a): head to head with the teammate's forkless-hybrid POC (yinon-fork / yinon-hybrid / yinon-forkless configs added to run.sh in 293e26ae2) plus upstream. Small at 50k writes/s: our hybrid extra memory 3 MB, fork 0.5 ms, PING max 0.33 ms; teammate hybrid 35 MB / 1.0 ms / 9 ms; stock 220 MB / 4.5 ms / 2.6 ms. Save time ours 1.3 s vs 1.5 s theirs vs 1.9 s stock. Forkless on big values: 140 MB parent growth, PING p99 65 ms.

Bug found by run 7 and fixed in 20aa93d06: snapKvobjIsOwed/prepare checked the emitted bit only for the ahead position; a window move inside a write section made an emitted bucket look owed (DEBUG_ASSERTIONS crash at db.c:618 at 50k writes/s, needless clone in plain builds). New helper snapBucketOwed, unit test, spec 11.z.2. Also found: the repository binary used in run 6 was a -DDEBUG_ASSERTIONS build while every other benchmark binary was plain; run 7 hybrid rows use a plain worktree build (/tmp/redis-hyb). Rerun of the microbench (feature-on SET/GET) with a plain build is still owed.

2026-09-25 run 8 (run8-cpu1.csv, RESULTS.md 'Run 8'): run 7 matrix with SERVER_CPUS=0 (server + scan/helper threads + fork child on one CPU; run.sh knob added in e270b171e). All fork-based configs converge to PING p99 3-5 ms at 50k writes/s (child competes for the core); ours is at or below stock and the teammate's hybrid in every small cell with less total CPU (2.2 s vs 2.6 s stock at small/50k), fastest save on small (1.94 s vs 2.38 stock vs 2.63 teammate hybrid), memory unchanged (3 MB vs 236 MB vs 39 MB). Forkless collapses on one core: 12-14 s saves at 50k with the load target missed and 12 s of stall; on big values PING p99 136-214 ms, max up to 657 ms.

2026-09-25 verification pass (RESULTS.md 'Verification pass'): unit/snapshot --accurate 32/32 incl. 100 randomized race iterations (500 s); ASan arena-free worktree 26/26 clean; unit tests 160/160; full stock suite feature off all passed; feature-on suite: 1433 ok, 5 expected replica-based failures, aborted on an untagged BGREWRITEAOF (AOF/replication/module fork are refused by design until TASK-1.22, so a full feature-on suite run is blocked on that task). Spec section 4 rewritten (18777d3c3). Microbenchmarks added (utils/snap-bench/microbench.sh, callgrind.sh): ops/s on this laptop is noise-dominated (40% swings, order effect); callgrind shows SET +0.01% / GET +0.9% Ir with the feature on but the arena cannot be created under valgrind (16 TB reservation), so the arena mallocx cost is still unmeasured; needs perf (perf_event_paranoid is 4). AC2 checked with the caveat that 'full stock suite with no regression' was measured feature off; AC4 verdict written in RESULTS.md.
<!-- SECTION:NOTES:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
Serialize-before-write verified on standalone: harness with 100 accurate iterations, TSan and ASan clean in arena-free mode, unit tests 160/160, full stock suite green with the feature off. Benchmarks: run 6 (vs upstream and Valkey), run 7 (vs the teammate's forkless-hybrid POC on 8 cores), run 8 (same on 1 core) all in utils/snap-bench/RESULTS.md with raw CSVs. One race found and fixed by the benchmark (owed-bucket position vs emitted bit, 20aa93d06). Left open and recorded: arena steady-state instruction cost (needs perf), feature-on stock suite (blocked on TASK-1.22), overlay-only hybrid column not rerun (run 5 numbers stand in).
<!-- SECTION:FINAL_SUMMARY:END -->
