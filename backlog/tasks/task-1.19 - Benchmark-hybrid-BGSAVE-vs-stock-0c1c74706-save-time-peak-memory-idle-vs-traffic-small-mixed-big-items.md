---
id: TASK-1.19
title: >-
  Benchmark: hybrid BGSAVE vs stock 0c1c74706 (save time, peak memory, idle vs
  traffic, small/mixed/big items)
status: Done
assignee:
  - '@moti.cohen@redis.com'
created_date: '2026-09-21 09:33'
updated_date: '2026-09-21 09:41'
labels:
  - bgsave
  - persistence
  - benchmark
milestone: m-0
dependencies: []
parent_task_id: TASK-1
priority: high
ordinal: 17500
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Requested 2026-09-21: compare this branch against the merge base 0c1c74706 on (1) BGSAVE time, (2) maximum memory consumed during BGSAVE, (3) traffic vs idle, (4) small items vs mixed vs big items, with datasets of at most 400 MB, modelled on the sibling tree's bench (`~/git/redis/.mfiles/task-234 - Hybrid-BGSAVE/bench`).

Tooling in `utils/snap-bench/`: `bench.py` (datasets small/mixed/big/big-grow, memtier same-size or insert traffic split by bytes per class, 100 ms /proc sampling of server RssAnon growth + child Private_Dirty, 5 ms PING probe, LATENCY LATEST capture, CSV + markdown report), `run.sh` (one server per config, identical flags, cores 0-7 vs 8-15, dump dir on tmpfs), `rss_probe*.sh` (allocator/RSS probes). Results in `utils/snap-bench/RESULTS.md`.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 Both binaries built with the same (release) flags and run with identical server flags, dump directory on tmpfs
- [x] #2 Every dataset under 400 MB used_memory; idle and two traffic levels; 2 reps per cell
- [x] #3 Save time, fork stall, extra memory peak (parent growth + child dirty), absolute peak, CoW report, CPU split, PING and load latency recorded per cell
- [x] #4 Anomalies explained (debug-build flags, disk writeback outliers, arena tcache retention after FLUSHALL) and fixed where they were defects
- [x] #5 Results committed as utils/snap-bench/RESULTS.md with the tool
<!-- AC:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
Results in utils/snap-bench/RESULTS.md (raw results.csv). Headline vs stock 0c1c74706 on 250-300 MB datasets: save time -11% (small), -38% (mixed), equal (big); extra memory during a save under 20k/50k writes/s 14-15x lower on small, 2.5-2.8x lower on mixed, equal on big; fork stall 6 ms -> 0.6 ms on small; the one regression is the overlay merge on the main thread, 12-35 ms worst PING on small under 20-50k writes/s (stock worst is the 5 ms fork). Anomalies fixed: debug-build flags on the hybrid binary, disk writeback outliers (tmpfs), child dismissObject (child CoW 27 -> 13 MB on big-hash inserts), arena tcache retention after FLUSHALL (30 MB). Residual: +17-27 MB RSS at rest after a class shift in one process (arena memory not reusable across classes, slow decay); merge spike to bound in M2 (incremental merge).
<!-- SECTION:FINAL_SUMMARY:END -->
