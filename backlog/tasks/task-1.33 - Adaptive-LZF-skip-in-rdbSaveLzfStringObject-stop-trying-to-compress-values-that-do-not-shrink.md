---
id: TASK-1.33
title: >-
  Adaptive LZF skip in rdbSaveLzfStringObject: stop trying to compress values
  that do not shrink
status: To Do
assignee: []
created_date: '2026-10-04 20:14'
labels:
  - bgsave
  - upstream
milestone: m-3
dependencies: []
documentation:
  - utils/snap-bench/RESULTS.md
parent_task_id: TASK-1
priority: medium
ordinal: 29000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Run 36 (2026-10-04, RESULTS.md): on the benchmark datasets (random 50-100 byte strings and fields) saves with rdbcompression yes and no write byte-identical files, yet compression costs about two thirds of each serializer's CPU: mixed1m hybrid 26.9/28.4 s against 9.6 s, stock 42.0/48.4 against 19.3; small hybrid 32.1/34.7 against 10.3, stock 35.3/38.1 against 14.8/13.9. rdbSaveLzfStringObject tries LZF on every string of 20 bytes or more and keeps the raw bytes when the output is not smaller, so on incompressible data every attempt is wasted work, on the thread and on the child alike, for stock and hybrid alike.

Idea (independent of the hybrid, an upstream candidate): make the attempt adaptive. Per object being serialized (a hash, set, list, or the key stream), keep a short failure history: after N consecutive values that did not shrink, skip LZF for the rest of that object (or for the next M values, then retry one). Alternatively sample: try the first few values of an aggregate and decide for the object. Must not change the RDB format (raw strings stay valid), must keep compression for data that does shrink (verify on a compressible dataset that the file size is unchanged or within a small fraction), and the hybrid's two serializers (snapshot.c thread path and the child) both go through the same rio helpers so they inherit it.

Measure before and after with run 36's recipe (idle saves, hybrid and stock, mixed1m and small) plus a compressible dataset to show no size regression.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 On the incompressible benchmark data an idle save with rdbcompression yes takes within 15% of rdbcompression no, for stock and hybrid, mixed1m and small (run 36 recipe)
- [ ] #2 On a compressible dataset (values with repeated content) the RDB size with the adaptive skip is within 2% of today's and loads identically
- [ ] #3 RDB format unchanged; unit/snapshot and integration/rdb pass in hybrid and stock modes
- [ ] #4 RESULTS.md records the before/after and the chosen heuristic
<!-- AC:END -->
