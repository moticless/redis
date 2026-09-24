---
id: TASK-1.13
title: Soft and hard memory watermarks for save-time clones and preserved originals
status: To Do
assignee: []
created_date: '2026-09-20 17:22'
updated_date: '2026-09-20 18:28'
labels:
  - bgsave
  - persistence
  - memory
milestone: m-1
dependencies:
  - TASK-1.9
documentation:
  - poc-hybrid-bgsave-spec.md
parent_task_id: TASK-1
priority: medium
ordinal: 14000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Spec §7.6: memory held by clones (headers and values) plus preserved originals for still-owed slots is bounded by write_rate x save_duration and is the hybrid design's replacement for CoW growth. Add a soft watermark that applies backpressure to the scan (e.g. prioritise slots with the most temp-dict entries; stalling client writes is NOT acceptable) and a hard watermark that aborts the save rather than stalling clients or risking OOM.

Scope, in src/snapshot.c: configs for the two watermarks (absolute bytes and/or fraction of maxmemory), precise accounting of save-time extra memory (clone bytes + original bytes not yet freed + temp dict overhead + in-flight staging buffers and channel entries), the soft-watermark reaction, the hard-watermark abort using the orchestration abort path (TASK-1.9), log lines and INFO persistence fields, and interaction with maxmemory eviction (save-time extra memory must not cause eviction storms; decide whether it counts toward maxmemory and document it).
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 Save-time extra memory (clones, unfreed originals, temp-dict overhead, in-flight staging buffers) is accounted and exposed in INFO persistence
- [ ] #2 Soft and hard watermark configs exist, are documented in redis.conf, and are settable via CONFIG SET
- [ ] #3 Crossing the soft watermark changes scan behaviour without stalling client writes, verified by a test that measures client latency during backpressure
- [ ] #4 Crossing the hard watermark aborts the save cleanly through the standard abort path, is logged, and is reflected in rdb_last_bgsave_status
- [ ] #5 The interaction of save-time extra memory with maxmemory eviction is decided, documented, and covered by a test under maxmemory
<!-- AC:END -->
