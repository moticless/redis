---
id: TASK-1.56
title: >-
  Overlay: clone only the written key of a bucket, sharing the neighbors' kvobjs
  until they are written (idea, needs a profile first)
status: To Do
assignee: []
created_date: '2026-10-16 09:22'
updated_date: '2026-10-31 22:17'
labels:
  - hybrid-bgsave
  - overlay
  - idea
dependencies:
  - TASK-1.50
references:
  - src/snapshot.c snapOverlayCloneKey
  - src/kvsoverlay.c ovBucketCreate
parent_task_id: TASK-1
priority: low
ordinal: 52000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
A collision duplicates every key of the bucket chain (a bucket holding a key averages about 2), including kvobj header and, for SNAP_NOFORK values, a deep copy of the value. Alternative: duplicate only the dict entry nodes, clone the kvobj of the written key, and share the neighbors' kvobjs between the original chain and the overlay chain until a neighbor is itself written (then clone it in the prepare-mutation path; whether a neighbor is still shared is found by comparing against the originals, no stored bit). Roughly halves clone work and retained bytes per collision; costs complexity in snapPrepareMutation and the release path. Only worth doing if a profile shows the clone cost, or retained bytes matter for the hard mark.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 Evidence from a profile or benchmark that clone cost or retained bytes of neighbor keys is significant, recorded before implementing
- [ ] #2 If implemented: harness (randomized writers, 100 iterations) and unit tests pass; retained bytes per collision drop as measured
- [ ] #3 The overlay clones a key lazily on its first write, not the whole chain on DEFER; a test with a multi-key INSIDE bucket and a single-key write shows one clone (moved from TASK-1.65 AC#7)
<!-- AC:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
2026-11-01: TASK-1.65 AC#7 (lazy per-key clone) moved here as AC#3. Evidence so far argues against doing it: run 165 measured 5-50 overlay buckets per save, INSIDE hits 0.001-0.004% of writes (TASK-1.65 notes). It matters again only once a swiss-table hashtable makes buckets ~5 keys and a profile shows the clone cost.
<!-- SECTION:NOTES:END -->
