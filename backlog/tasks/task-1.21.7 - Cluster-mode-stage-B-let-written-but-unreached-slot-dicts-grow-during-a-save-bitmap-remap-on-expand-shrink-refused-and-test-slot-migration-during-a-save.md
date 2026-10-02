---
id: TASK-1.21.7
title: >-
  Cluster mode stage B: let written-but-unreached slot dicts grow during a save
  (bitmap remap on expand, shrink refused) and test slot migration during a save
status: In Progress
assignee:
  - '@Claude'
created_date: '2026-10-01 00:01'
updated_date: '2026-10-02 19:17'
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
- [x] #1 A slot dict written and expanded before the walker reached it (forced by adding keys to one slot during a slow save) reloads equal to the fork instant: the bitmap was remapped and the hash class emitted ahead across both tables (new cluster harness test)
- [x] #2 Shrink of a dict with a bitmap is refused while the save runs and resumes after; expand of an unreached dict is allowed and visible in debug htstats during the save
- [ ] #3 Slot migration during a save: refused with a clear error or verified safe by a test; the choice is documented in spec section 11.z.1
- [x] #4 TSan and ASan runs of unit/snapshot clean with the remap in place
<!-- AC:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
2026-10-02, stage B implemented as the user described it: a slot dict the scan thread has not reached is attached by its first write (bitmaps, overlay) but NOT paused any more; only the dict under the thread's window (or whose slot is SCANNING) is paused, on the main thread's next access. The kvstore snapshot guard grew into kvstoreSnapHooks {guard, rehash_started, rehash_completed, shrink_allowed}; kvstoreDictRehashingStarted/Completed call them with the dict's index (new kvstoreDictMetaBase.didx). On rehash start the table-1 bitmap is k copies of the table-0 bitmap (old bucket i -> new buckets i + j*n; memcpy per copy when n is a multiple of 8, bit loop otherwise); on completion the table-1 bitmap becomes table 0's. Shrink is refused for attached dicts through a new optional dictType.shrinkAllowed checked by dictShrinkIfNeeded (a merge has no bucket-wise answer); cron resize/rehash and kvstoreExpand of paused dicts were already off during a child. Emit-ahead on a rehashing unreached dict emits and marks the whole bucket class (ht0[i0] if not yet moved, ht1[i0 + j*n] for all j) so entries the rehash moves later land in marked buckets. INFO snap_save_grown_dicts counts the remaps. Tests: C unit (write-intent lookup attaches without pausing; 16x expand replicates the marks into the new table; the carried bitmap is the table's after the rehash; shrink refused), tcl cluster test (a high slot with 4 fork-instant keys takes 600 writes during a slow save, snap_save_grown_dicts >= 1, reload equals the fork instant). C unit 168/168; unit/snapshot both modes running.

Stage B verified and committed as 3c1b14608: unit/snapshot feature on and stock pass (the new cluster test included), TSan and ASan clean, the full feature-on suite passed with no failures at all (164 units). AC2 is observed through INFO snap_save_grown_dicts (the remap counter) rather than debug htstats, which aggregates the per-slot dicts in cluster mode. AC3 (slot migration during a save) is still open and is the only remaining item of this task. Follow-up on the user's suggestion (2026-10-02): overlays now carry the save generation (kvstoreSnapGuardSet(..., gen), ov->gen); an access meeting an overlay of another generation releases it and counts it (INFO snap_overlay_leftovers, C unit test); verification running.
<!-- SECTION:NOTES:END -->
