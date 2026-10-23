---
id: TASK-1.50
title: >-
  Bucket-granular overlay: writes to a bucket the scan may be reading duplicate
  the bucket's chain, replacing the per-key side table
status: Done
assignee: []
created_date: '2026-10-16 09:22'
updated_date: '2026-10-16 15:12'
labels:
  - hybrid-bgsave
  - overlay
dependencies: []
references:
  - src/kvsoverlay.c
  - src/kvsoverlay.h
  - src/kvstore.c
  - src/kvstore.h
  - src/snapshot.c
  - utils/snap-bench/run40-mixed1m.csv
  - utils/snap-bench/run40-mixed.csv
  - utils/snap-bench/run40-mixed-hot.csv
  - utils/snap-bench/run40-small.csv
parent_task_id: TASK-1
priority: medium
ordinal: 46000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Outcome: while the scan thread may be reading a bucket of a frozen dict, the main thread never writes that dict's slot or chain; a write duplicates the whole bucket chain into an overlayed bucket (ring of 1024 slots indexed by bucket index, src/kvsoverlay.c) and the chain is written back into the dict once the scan has passed. Reads never clone; the main thread checks the overlay first. Replaces the per-key side table of the committed design (ba5f0052d). Prototype lives uncommitted on branch proto-bucket-cow. Benchmarked as Run 40 (c7i.4xlarge, 4 GB datasets, 8-vCPU server): equal to the side table within noise on save time, extra/CoW memory, CPU, write latency and rate (e.g. mixed1m 50k: 27.1 s / 1611 MB vs 26.3 s / 1584 MB; stock upstream 45-49 s / 3284 MB); raw CSVs utils/snap-bench/run40-*.csv. Remaining before it can land: sanitizer run, dedicated tests, docs (see the follow-up tasks that depend on this one), then commit.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 Prototype branch proto-bucket-cow is committed after TSAN/ASAN, the new unit tests and the docs update tasks it depends on are done
- [x] #2 ./src/redis-server test snapshot, ./runtest --single unit/snapshot and the accurate randomized-writers harness test pass on the committed build
- [x] #3 Run 40 recorded in utils/snap-bench/RESULTS.md and SUMMARY.md with the CSVs committed
<!-- AC:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
2026-10-16: Run 40 (AWS) and Run 41 (local insert-heavy) recorded in utils/snap-bench/RESULTS.md and SUMMARY.md; CSVs run40-*.csv, run40.sh and rehash-pause.sh are in utils/snap-bench, uncommitted. TASK-1.51 (TSAN/ASAN clean on the arena-free build, two new unit tests), TASK-1.52 (docs), TASK-1.53 (INFO counters), TASK-1.54 (rehash hold, kept) done; unit/snapshot passes on the main build (206 s) and redis-server test snapshot 200/200. Still open on this task: AC 1 (the commit, only when the user asks) and AC 2 on the committed build; the randomized-writers 100-iteration test was last run before the 1.51-1.53 changes (no overlay logic changed since, only counters, tests and docs). Not run: the accurate harness test and the cluster suites on the current tree.

Committed on proto-bucket-cow: 015213f47 (scan prefetch, TASK-1.55) and 6df646b93 (the overlay, tests, docs, counters, Run 40/41/42 with CSVs and scripts, backlog). Checks on the final tree: ./redis-server test snapshot 200/200, ./runtest --single unit/snapshot --clients 4 (main, TSAN, ASAN), the accurate 100-iteration randomized-writers harness (485 s) and unit/cluster/atomic-slot-migration (107 tests) all pass. Limit: sanitizer builds are arena-free (MALLOC=libc).
<!-- SECTION:NOTES:END -->
