---
id: TASK-1.21.3
title: 'In-window side table with drain, and removal of the frozen-dict overlay'
status: To Do
assignee: []
created_date: '2026-09-21 19:57'
updated_date: '2026-09-30 20:30'
labels:
  - bgsave
  - db
  - memory
milestone: m-3
dependencies:
  - TASK-1.21.1
  - TASK-1.21.2
documentation:
  - poc-hybrid-bgsave-spec.md
parent_task_id: TASK-1.21
priority: medium
ordinal: 11930
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Spec section 11.z, 'If b is inside W'. Writes to keys whose bucket is inside the walker's current window must not emit (the walker may be emitting the same key) and must not change the chain. They go to a small main-thread side table: duplicate on modify, tombstone on delete, pending entry on add. Lookups consult the side table first while it is non-empty. The main thread drains it at the next funnel entry or cron tick for buckets the window has moved past: replace the entry pointer in the chain and free the original (CoW-free for arena objects), unlink tombstoned originals, add pending keys. Originals kept alive by the side table are counted as retained bytes. Once this and the emission task work, delete the frozen-dict overlay: kvstore overlay structs and functions, per-slot temp dicts, state dict, placeholder tombstones, merge, the hybrid-merge latency event, snapScanSetOverlay and the slot PENDING/SCANNING/DONE machinery no longer needed. Keep the scanned-callback funnel hook and the write-path audit table current.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 Writes to keys inside the window during a save are served from the side table and reload equal to the fork instant (harness variant that pins the window with a slow walker and hammers the keys in it)
- [ ] #2 The side table is drained within one funnel entry or cron tick after the window moves and never grows beyond a few windows' worth of entries under the harness storm (measured, recorded in notes)
- [ ] #3 No overlay code remains: kvstore.c has no temp dict, state dict, merge or tombstone-placeholder paths and there is no hybrid-merge latency event
- [ ] #4 MOVE, RENAME, COPY and RESTORE REPLACE on keys inside and outside the window reload correctly (the cases that broke the header-only clone under TSan)
- [ ] #5 ASan run of unit/snapshot clean after the overlay removal
<!-- AC:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
2026-09-30 reassessment with the user: clones are already created only for keys whose bucket is inside the window; this task changes only when they are freed (today at slot completion in snapScanCompleteSlot via kvstoreOverlayMerge, i.e. end of save in standalone; the window-done hook only flushes the main-to-walker queue). Measured, that difference is immaterial: in run 19 (mixed1m, 40 s save, 20k writes/s) snap_last_save_retained_peak minus the queued peak is at most 1 MB in every hybrid row, so the in-window clones cost about nothing. The per-lookup overlay check (ovMapFrozenEntry: temp-dict find plus state-dict lookup on every access to a frozen dict for the whole save) is not visible either: run 17 main-thread CPU during the save is identical for hybrid and stock (10 s at 20k writes/s, 15 s at 50k). What remains is a cleanup: removal of the temp dict, state dict, tombstones, merge (and the hybrid-merge latency event), overlay-aware scan and two-phase unlink in kvstore.c, plus the slot state machine in snapshot.c that exists to know when to merge; one rule for the header-only-clone corner cases (MOVE, RENAME, COPY, RESTORE REPLACE); less overlay handling to carry into TASK-1.24 (detached kvstore would just be walked) and TASK-1.21.6 (window without per-slot overlays). Decision: no measured benefit on its own; do it only if it makes TASK-1.24 or TASK-1.21.6 easier to build. Priority lowered to low. AC #2 ('drained within one funnel entry...never grows beyond a few windows') is therefore a correctness bound, not a memory win.

2026-09-30, later the same day: back to medium. Still no measured benefit on its own, but it is now the enabler for cluster mode: TASK-1.21.6 (design approved 2026-09-30, plan file wondrous-meandering-sketch.md) attaches per-slot save metadata on demand (generation, bucket bitmaps, in-window side table, paused flag) to kvstore's per-dict metadata, and wants the side table to live there from the start rather than moving it later. Plan of record for this task therefore: implement the side table as part of a per-dict snapSlotMeta struct reachable from kvstoreDictMetaBase (one instance per frozen dict; in standalone one per db), drain at funnel entry / cron for buckets the window has passed, then delete the overlay (temp dict, state dict, tombstones, merge, hybrid-merge latency event, overlay-aware scan and two-phase unlink, snapScanSetOverlay). Keep the freeze semantics of today (all non-empty dicts frozen at start) in this task; the on-demand attach, lazy pause and bitmap remap are TASK-1.21.6.
<!-- SECTION:NOTES:END -->
