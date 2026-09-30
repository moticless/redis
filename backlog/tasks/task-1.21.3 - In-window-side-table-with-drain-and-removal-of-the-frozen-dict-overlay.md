---
id: TASK-1.21.3
title: 'In-window side table with drain, and removal of the frozen-dict overlay'
status: Done
assignee:
  - '@Claude'
created_date: '2026-09-21 19:57'
updated_date: '2026-09-30 21:13'
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
- [x] #1 Writes to keys inside the window during a save are served from the side table and reload equal to the fork instant (harness variant that pins the window with a slow walker and hammers the keys in it)
- [x] #2 The side table is drained within one funnel entry or cron tick after the window moves and never grows beyond a few windows' worth of entries under the harness storm (measured, recorded in notes)
- [x] #3 MOVE, RENAME, COPY and RESTORE REPLACE on keys inside and outside the window reload correctly (the cases that broke the header-only clone under TSan)
- [x] #4 ASan run of unit/snapshot clean after the overlay removal
- [x] #5 No slot-completion merge remains: kvstoreOverlayMerge and the hybrid-merge latency event are gone, slot completion is a release of an already drained side table; reads on a frozen dict take the stock path while its side table is empty; the overlay lives in the dict's metadata (kvstoreDictMetaBase.ov). The temp dict + state dict pair is kept as the side table's representation (decision 2026-09-30: measured cost about nothing, sanitizer-clean)
<!-- AC:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
2026-09-30 reassessment with the user: clones are already created only for keys whose bucket is inside the window; this task changes only when they are freed (today at slot completion in snapScanCompleteSlot via kvstoreOverlayMerge, i.e. end of save in standalone; the window-done hook only flushes the main-to-walker queue). Measured, that difference is immaterial: in run 19 (mixed1m, 40 s save, 20k writes/s) snap_last_save_retained_peak minus the queued peak is at most 1 MB in every hybrid row, so the in-window clones cost about nothing. The per-lookup overlay check (ovMapFrozenEntry: temp-dict find plus state-dict lookup on every access to a frozen dict for the whole save) is not visible either: run 17 main-thread CPU during the save is identical for hybrid and stock (10 s at 20k writes/s, 15 s at 50k). What remains is a cleanup: removal of the temp dict, state dict, tombstones, merge (and the hybrid-merge latency event), overlay-aware scan and two-phase unlink in kvstore.c, plus the slot state machine in snapshot.c that exists to know when to merge; one rule for the header-only-clone corner cases (MOVE, RENAME, COPY, RESTORE REPLACE); less overlay handling to carry into TASK-1.24 (detached kvstore would just be walked) and TASK-1.21.6 (window without per-slot overlays). Decision: no measured benefit on its own; do it only if it makes TASK-1.24 or TASK-1.21.6 easier to build. Priority lowered to low. AC #2 ('drained within one funnel entry...never grows beyond a few windows') is therefore a correctness bound, not a memory win.

2026-09-30, later the same day: back to medium. Still no measured benefit on its own, but it is now the enabler for cluster mode: TASK-1.21.6 (design approved 2026-09-30, plan file wondrous-meandering-sketch.md) attaches per-slot save metadata on demand (generation, bucket bitmaps, in-window side table, paused flag) to kvstore's per-dict metadata, and wants the side table to live there from the start rather than moving it later. Plan of record for this task therefore: implement the side table as part of a per-dict snapSlotMeta struct reachable from kvstoreDictMetaBase (one instance per frozen dict; in standalone one per db), drain at funnel entry / cron for buckets the window has passed, then delete the overlay (temp dict, state dict, tombstones, merge, hybrid-merge latency event, overlay-aware scan and two-phase unlink, snapScanSetOverlay). Keep the freeze semantics of today (all non-empty dicts frozen at start) in this task; the on-demand attach, lazy pause and bitmap remap are TASK-1.21.6.

Implementation 2026-09-30 (uncommitted, verification in progress). Scope as agreed with the user: the temp dict + state dict pair stays as the side table's representation; what changes is lifetime, placement and cost. (1) kvstore.h/kvstore.c: the overlay pointer moved from a kvs->ov[] array to the dict's own metadata (kvstoreDictMetaBase.ov), the home TASK-1.21.6 needs; ovGet is one load when nothing is frozen (kvs->ov_frozen) plus the dict pointer every caller loads anyway. (2) ovActive(): reads, sizes, iterators, scan and random sampling consult the side table only while it is non-empty; writes always go through the overlay of a frozen dict (emission/prepare). (3) Drain: kvstoreOverlayDrain(kvs) walks the store's dirty-overlay list (ovMarkDirty on clone/add/tombstone) and applies every entry whose bucket the new 'drainable' hook accepts (snapOverlayDrainable: snapBucketPosition == BEHIND, i.e. window passed or slot SCANNED); ovApplyTemp replaces the original at its link (or inserts an add), ovApplyTomb unlinks and frees; destructors suppressed as the old merge did. Triggered from snapFunnelCloseSlow (still inside the section, at the outermost command end / beforeSleep, where no caller holds a link) and from snapSaveCron (opens a section when entries are pending); skipped while snap_win_seq has not moved since the last drain (snap_drain_seq). (4) kvstoreOverlayRelease at slot completion applies the rest and unfreezes; kvstoreOverlayMerge and the hybrid-merge latency event are gone. (5) INFO persistence snap_save_side_entries (kvstoreOverlayPending summed over dbs). (6) New tcl test 'writes inside the window go to the side table and drain once the window has moved' (two windows over a 10k-key table, 1 ms/key walker, half the keys written: side entries > 0 and main emissions > 0 during window 1, side entries and retained bytes reach 0 while the save still runs, digest equal after reload); the C unit test hooks gained the drainable member. Results so far: C unit 164/164; unit/snapshot with --config bgsave-hybrid yes all pass (166 s); the new test passed 3/3 standalone after two timing fixes (the first version wrote every key, so the second window had nothing left to walk and the save ended in the same poll as the drain). Two unrelated tests flaked once under full-run load and passed in isolation: 'child CoW stays flat' (1.1 MB vs 1.9 MB, needs 2x) and 'diskless sync rdb-channel yes converges' (snap_saves off by one). Spec section 4 (side table, drain, release paragraphs), 11.z.1 leftover 2 and the write-path audit table updated. Pending: stock-mode unit/snapshot, TSan and ASan runs (sanitizer trees being rebuilt from scratch after an rsync mishap), commit.
<!-- SECTION:NOTES:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
In-window side table with incremental drain; the slot-completion merge is gone and the overlay now lives in the dict's own metadata, where TASK-1.21.6 will attach it on demand.

**What changed**
- kvstore.h/kvstore.c: the per-dict overlay pointer moved from a `kvs->ov[]` array into `kvstoreDictMetaBase.ov`; `ovGet` is one load when nothing is frozen (`kvs->ov_frozen`) plus the dict pointer every caller loads anyway. New `ovActive()`: reads, sizes, iterators, scan and random sampling consult the side table only while it is non-empty; writes always take the overlay path of a frozen dict (enter/prepare/emit). New hook `drainable`; `kvstoreOverlayDrain(kvs)` walks the store's dirty-overlay list and applies every entry the hook accepts (`ovApplyTemp`: clone replaces its original at the link or an add is inserted; `ovApplyTomb`: tombstoned original unlinked and freed), destructors suppressed as the old merge did. `kvstoreOverlayRelease` applies the rest and unfreezes at slot completion; `kvstoreOverlayMerge` removed. `kvstoreOverlayPending` for INFO.
- snapshot.c: `snapOverlayDrainable` (bucket position BEHIND, i.e. window passed or slot SCANNED); `snapSaveDrainSideTables` called from `snapFunnelCloseSlow` (still inside the section, at the outermost command end / beforeSleep, where no caller holds a link) and from `snapSaveCron` (opens a section when entries are pending); skipped while `snap_win_seq` has not moved since the last drain. Slot completion no longer times a merge; the `hybrid-merge` latency event is gone. INFO persistence `snap_save_side_entries`.
- Tests: new 'writes inside the window go to the side table and drain once the window has moved' (two windows over a 10k-key table, 1 ms/key walker, half the keys written: side entries and main emissions > 0 during the first window, side entries and retained bytes reach 0 while the save still runs, digest equal after reload). C unit hooks gained the drainable member (NULL: nothing drains before release, the old semantics).
- Docs: spec section 4 (side table, drain, release paragraphs), 11.z.1 leftover 2, write-path audit table row.

**Measured (AC2)**: 1M keys, `rdb-key-save-delay 2`, redis-benchmark SET storm (50 clients, pipeline 8, random keys) for the whole save: 860 samples of `snap_save_side_entries` at 20 ms, peak 20 entries, non-zero in 93 samples (11 %); 1.43M in-place writes; save ok. The side table holds at most a window's worth of clones and drains within a command end or cron tick.

**Verification**: C unit `redis-server test snapshot` 164/164; unit/snapshot with `--config bgsave-hybrid yes` (166 s) and stock mode (176 s) all pass; TSan (52 tests, no report, 355 s) and ASan (52 tests, no report, 170 s) of unit/snapshot with the feature on, both trees rebuilt from scratch. AC4 (MOVE/RENAME/COPY/RESTORE inside and outside the window) is covered by the randomized race harness, which issues all four, run in both modes and under both sanitizers.

**Scope decision**: AC3 originally asked for the temp dict and state dict to disappear. Agreed with the user on 2026-09-30 that the overlay's only material differences were drain timing and placement (measured: at most 1 MB of clones, no visible main-thread CPU), so the representation stays and the criterion was reworded to what was built. Two unrelated tests flaked once under full-run load ('child CoW stays flat', 'diskless sync rdb-channel yes converges') and passed in isolation and in the two later full runs.

**Follow-ups**: TASK-1.21.6 (on-demand attach, lazy pause, bitmap remap; baseline hybrid-freeze 3-5 ms in cluster mode recorded there). In-window deletes still clone the key only to tombstone it (rare).
<!-- SECTION:FINAL_SUMMARY:END -->
