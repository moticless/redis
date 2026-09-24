---
id: TASK-1.17
title: >-
  Measure hybrid BGSAVE against stock BGSAVE: CoW/RSS, main-thread CPU, fork
  stall, wall-time
status: To Do
assignee: []
created_date: '2026-09-20 17:22'
updated_date: '2026-09-20 18:28'
labels:
  - bgsave
  - persistence
  - benchmark
  - docs
milestone: m-2
dependencies:
  - TASK-1.11
  - TASK-1.12
  - TASK-1.13
documentation:
  - poc-hybrid-bgsave-spec.md
parent_task_id: TASK-1
priority: medium
ordinal: 18000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Spec §9 M3: quantify the redesign against stock BGSAVE on the same hardware and datasets, and record the numbers. The problem statement (spec §0) predicts two wins: a smaller fork page-table copy (kvobj headers, SNAP_NOFORK values and hash tables are DONTFORK) and no CoW amplification for headers and SNAP_NOFORK values under sustained writes. It also introduces new costs that must be measured honestly: clone + original memory for owed slots, background-thread CPU, kvobj copying and channel overhead for SNAP_FORK keys, the class-aware alloc/free path on every object create/free, and any main-thread cost from temp-dict resolution on every lookup.

Scope: a reproducible benchmark script (dataset generators, workload drivers, collection of INFO persistence CoW fields, /proc smaps, main-thread CPU via /proc/<pid>/task, fork() duration from latest_fork_usec, save wall-time). Workloads: write-heavy mix on SNAP_NOFORK keys, key-churn mix (add/delete), mixed SNAP_NOFORK/SNAP_FORK, an EXPIRE-heavy mix on SNAP_FORK keys (header-only writes), and a read-only baseline to show temp-dict overhead on lookups when no save is running and when one is. Datasets: at least a few GB with tens of millions of SNAP_NOFORK keys, plus a SNAP_FORK-heavy dataset. Compare stock vs hybrid with 1 and N threads. Record results as a checked-in document with tables and the exact commands used.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 A reproducible benchmark script and dataset/workload definitions are checked in with instructions to run them
- [ ] #2 Peak RSS and CoW bytes during a save are recorded for stock and hybrid under the write-heavy mix, the key-churn mix and the EXPIRE-heavy SNAP_FORK mix, on a SNAP_NOFORK-heavy dataset and a mixed dataset
- [ ] #3 Parent main-thread CPU during the save is recorded for stock and hybrid, and hybrid is shown to move the keyspace walk off the main thread
- [ ] #4 fork() stall (latest_fork_usec) is recorded for stock and hybrid at the same dataset size
- [ ] #5 Save wall-time is recorded for stock, hybrid with 1 thread, and hybrid with N threads
- [ ] #6 Steady-state (no save running) command latency, throughput and object create/free cost are compared between the hybrid build with the switch on and stock, to quantify placement, class-aware free and lookup-resolution overhead
- [ ] #7 All results, the hardware description and the exact commands are recorded in a checked-in measurements document
<!-- AC:END -->
