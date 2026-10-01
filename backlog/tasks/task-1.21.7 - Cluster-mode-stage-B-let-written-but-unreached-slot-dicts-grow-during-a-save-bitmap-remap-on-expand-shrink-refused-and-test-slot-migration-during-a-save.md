---
id: TASK-1.21.7
title: >-
  Cluster mode stage B: let written-but-unreached slot dicts grow during a save
  (bitmap remap on expand, shrink refused) and test slot migration during a save
status: To Do
assignee: []
created_date: '2026-10-01 00:01'
labels:
  - bgsave
  - cluster
milestone: m-3
dependencies:
  - TASK-1.21.6
documentation:
  - poc-hybrid-bgsave-spec.md
parent_task_id: TASK-1.21
priority: low
ordinal: 25000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Split out of TASK-1.21.6 on 2026-10-01 (stage A of the user's on-demand design landed as e7062762b; this is the rest of that design, deferred because nothing measured asks for it yet).

Today a slot dict that receives a write before the scan thread reaches it is paused at attach (snapScanAttach): it keeps its bitmap valid by not changing layout until its slot completes, the same limit every dict had before stage A and close to stock's DICT_RESIZE_AVOID while a child exists. The approved design goes further: an unreached dict may expand during the save. On expand, dictType.rehashingStarted (kvstoreDictRehashingStarted, kvstore.c) calls a snapshot hook that allocates the bitmap of the new table and replicates each old bit into its k children (bit i of size n -> bits i, i+n, ..., k = new/old, expand is not always 2x); rehashingCompleted drops the old table's bitmap. During the incremental rehash of such a dict the emit-ahead unit is the hash class (ht0[i] plus its children in ht1) and all their bits are marked, so keys moved by later rehash steps land in already-marked buckets. Shrink is refused while a bitmap exists (many-to-one fold has no correct bit value): dictShrinkIfNeeded checks through a kvstore-level test, since dictType.resizeAllowed has no dict argument. The walker still claims a dict with the whole-dict window before reading its layout; attach then pauses it as today, so growth is only for dicts the thread has not reached.

Also moved here from TASK-1.21.6 AC6: atomic slot migration (ASM) during a hybrid save is neither refused nor tested; decide by a test (cluster harness with a save in flight during slot import/trim) whether it needs a guard, and document the choice in the spec.

Motivation to build it: a hot slot on a long save (30-40 s mixed1m saves at 20-50k writes/s) whose load factor climbs past 4 while paused. Measure before building: INFO/htstats of a hot slot during a cluster save under load.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 A slot dict written and expanded before the walker reached it (forced by adding keys to one slot during a slow save) reloads equal to the fork instant: the bitmap was remapped and the hash class emitted ahead across both tables (new cluster harness test)
- [ ] #2 Shrink of a dict with a bitmap is refused while the save runs and resumes after; expand of an unreached dict is allowed and visible in debug htstats during the save
- [ ] #3 Slot migration during a save: refused with a clear error or verified safe by a test; the choice is documented in spec section 11.z.1
- [ ] #4 TSan and ASan runs of unit/snapshot clean with the remap in place
<!-- AC:END -->
