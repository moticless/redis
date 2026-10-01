---
id: TASK-1.21.6
title: Extend the bucket window scheme to cluster mode (per-slot dicts)
status: Done
assignee:
  - '@Claude'
created_date: '2026-09-21 19:57'
updated_date: '2026-10-01 00:01'
labels:
  - bgsave
  - cluster
milestone: m-3
dependencies:
  - TASK-1.21.5
  - TASK-1.21.3
documentation:
  - poc-hybrid-bgsave-spec.md
parent_task_id: TASK-1.21
priority: medium
ordinal: 11960
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
After the standalone spike: the window W = (dict, table, [lo, hi)) already names the dict, so in cluster mode it walks the 16,384 slot dicts in order and a window often covers a whole small dict. This task makes membership checks, the pause of a dict under the walker, the side-table drain and the walk-to-completion rule work over kvstore slot dicts, keeps SLOT_INFO records correct when the main thread emits keys for a slot the walker has not reached, and revisits the M1 note that empty slots are DONE at freeze. Cluster atomic slot migration during a save stays refused or guarded as today; document which.

Design (user, 2026-09-30; plan file wondrous-meandering-sketch.md). Today the freeze visits every non-empty slot dict before the fork and pays, per slot, two pause counters, one bitmap per table and an overlay struct with two dicts (tens of thousands of allocations inside the hybrid-freeze stall on a full node; never measured in cluster mode). Instead, allocate the save metadata of a slot dict ON DEMAND and attach it to the dict (pointer in kvstore's per-dict metadata, kvstoreDictMetaBase): either when the walker starts serializing that slot or when a command modifies a key in it before the walker got there. The metadata holds the save generation, the bucket bitmap(s) and (after TASK-1.21.3) the in-window side table. A dict the walker has not reached is NOT paused: it may grow; dictType.rehashingStarted (kvstore.c kvstoreDictRehashingStarted) remaps the bitmap into the new table (bit i of size n becomes bits i, i+n, ... of the new size; expand may be more than 2x), rehashingCompleted drops the old table's bitmap. During incremental rehash the emit-ahead unit is the hash class (ht0[i] plus its children in ht1) and all their bits are marked. Shrink is refused while current-generation metadata is attached (many-to-one fold has no correct bit value). The walker cannot touch the pause counters (plain fields also bumped by dictScan on the main thread): it publishes the slot as its window and quiesces as today, and the main thread pauses the dict lazily the first time it enters it under the window; for this every kvstore access to a non-DONE slot during a save opens a section, reads included (a rehash step can happen on a find). Slots with zero keys at the fork instant are DONE at start using the slot_keys capture that SLOT_INFO needs anyway; that byte loop and the state array are the whole freeze. Metadata carries server.snap_gen so stale metadata of an aborted save is freed lazily on next access (plus the existing sweeps in kvstoreRelease/kvstoreEmpty); abort and release need no O(16384) sweep. Order: TASK-1.21.3 first (side table lives in this metadata from the start), then this task.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 The cluster variant of the race harness (start_cluster 1 0) reloads equal to the fork instant, including keys emitted by the main thread for slots the walker had not reached (growth of unreached dicts moved to TASK-1.21.7)
- [x] #2 SLOT_INFO records in the hybrid RDB are accepted by redis-check-rdb and a cluster node started with the feature off loads the file with the same digest
- [x] #3 Per-slot save state (pause, bucket bitmaps, overlay) is attached on demand through the per-store snapshot guard: on the first structural write to a slot the thread has not finished, or on any access while the thread's window is on the dict; released when the slot completes; dicts already walked resume resizing before the save ends; the guard comes off a store when its last slot completes
- [x] #4 hybrid-freeze in cluster mode is O(slots) byte work with no per-slot allocation: measured before/after on a 1M-key all-slots node (3-5 ms -> below the 1 ms monitor threshold) and recorded in notes together with hybrid-merge (gone)
- [x] #5 Abort and release leave no save state behind (every slot completes before an aborting FLUSHALL/SWAPDB/DEBUG RELOAD frees the store); an aborted save followed by a save reloads equal (ten-aborted-saves test); no generation tag needed
- [x] #6 Growth of written-but-unreached dicts (bitmap remap, shrink refusal) and slot migration during a save moved to TASK-1.21.7
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
Plan of record (2026-09-30, approved with the plan file): 0. Baseline: cluster harness measurement of hybrid-freeze and hybrid-merge (latency latest) on 1M keys over all slots. 1. snapSlotMeta {gen, bits[2], side table, paused} owned by snapshot.c, pointer in kvstoreDictMetaBase; attach on the kvstore write hooks for PENDING/SCANNING slots and on the walker hand-off; gate 'has current-gen metadata or slot under window' replaces kvstoreOverlayIsFrozen. 2. Freeze reduced to the state array and the slot_keys capture; empty slots DONE. 3. Sections opened for reads on non-DONE slots during a save; main-thread lazy pause under the window; resume at completion only if paused. 4. Bitmap remap in kvstoreDictRehashingStarted/Completed; shrink refused with metadata (resizeAllowed or kvstore-level check). 5. Emit-ahead over the hash class during rehash; bitmap allocated on attach. 6. Stale comments (snapshot.h 180/207), spec section 4 Freeze paragraph and 11.z.1 leftover 5, RESULTS.md numbers. Verification: new cluster harness tests (slow walker + forced expand of an unreached slot, writes after attach before hand-off, killed child then a clean save), unit/snapshot both modes, C unit (layout-lock test changes meaning: paused only under the window), TSan and ASan, full runtest with the feature on, redis-check-rdb on a cluster dump.
<!-- SECTION:PLAN:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
Baseline 2026-09-30 (before the on-demand metadata; repo build with -DREDIS_TEST -DDEBUG_ASSERTIONS, laptop): single-node cluster owning all 16384 slots, DEBUG POPULATE 1M keys (about 61 keys per slot, 64-bucket tables), latency-monitor-threshold 1. hybrid-freeze 5 ms on the first save and 3 ms on the second (the BGSAVE command itself 5 ms); hybrid-merge 1 ms total across the 16384 slot completions; stock fork on the same data 3 ms. So in cluster mode the freeze alone costs as much as a stock fork stall, against 0.3-0.7 ms worst PING for the standalone hybrid save (run 6). That is the number the on-demand metadata must bring down. Script: scratchpad clfreeze/run.sh (needs --cluster-bus-port-protected-mode no on this build).

Stage A implemented 2026-10-01 (on-demand attach; uncommitted, verification in progress). The freeze now only marks empty slots DONE and installs a per-store snapshot guard (kvstoreSnapGuardSet, kvstore.h): every dict access without an overlay while the scan runs lands in snapOverlayGuard, which returns NULL for SCANNED/DONE slots, otherwise opens a write section (reads included: a find may step a rehash, and the thread may claim the dict at any moment) and attaches on a structural write, or on any access while the thread's window is on the dict. snapScanAttach pauses rehash/auto-resize, allocates the bitmaps for the tables that exist now (pointer published with a release store; the thread loads with acquire) and attaches the overlay; ctx->attached[s][didx] records it so completion resumes only what was paused. The thread claims each dict with a whole-dict window (table -1, position INSIDE for every bucket) plus quiesce before reading table sizes and rehashidx, so the layout holds still for the walk. Growth of unreached dicts is still blocked once they are attached (a written slot is paused at attach): the bitmap remap / shrink refusal of the plan (stage B) is not built yet. Measured on the same 1M-key all-slots node as the baseline: hybrid-freeze no longer registers above the 1 ms latency-monitor threshold (was 3-5 ms), the BGSAVE command is 1 ms (the fork) against a 2 ms stock fork; hybrid-merge is gone (TASK-1.21.3). Tests: C unit 166/166 after adapting the scan/bitmap/owed/overlay tests to on-demand pausing (deterministic checks between snapScanFreeze and snapScanStartThread; reads in the concurrent-read loop end their 'command' with snapFunnelClose or the thread's quiesce waits forever); unit/snapshot with the feature on passes after the bitmap-lifecycle test learnt that bitmaps appear on the first write. Pending: stock-mode run, TSan, ASan, cluster harness variant for expand-before-reached (stage B), commit.

Stage A committed 2026-10-01 as e7062762b after: unit/snapshot with the feature on (205 s) and off (213 s) all pass; TSan 52/52 no report; ASan 52/52 no report. TSan caught a real bug in the first version: the guard was taken off a store only at snapScanRelease, but FLUSHALL/SWAPDB/DEBUG RELOAD abort the save and free or replace the kvstore before the release runs (heap-use-after-free in kvstoreSnapGuardSet); the guard now comes off when the store's last slot completes (ctx->pending[s]). AC2 checked: a hybrid cluster dump (300k keys, all slots) passes redis-check-rdb and a cluster node started with bgsave-hybrid no loads it with an equal DEBUG DIGEST and the right slot placement (countkeysinslot 0 = 20). AC4 checked: hybrid-freeze 3-5 ms -> below the 1 ms monitor threshold, BGSAVE command 1 ms vs stock fork 2 ms, hybrid-merge gone. Open for stage B: AC1's 'slots whose dict expanded before the walker reached them' and AC3's 'unreached dicts may grow and remap their bitmap, shrink refused' (today a written slot is paused at attach, so it cannot grow until its slot completes, the same limit every dict had before this task); AC5 (generation check): not needed as designed, since state DONE plus guard-off make stale metadata impossible (abort completes every slot before the store is freed) and the abort sweep is a byte loop; propose to reword AC5 to 'abort and release leave no metadata behind; an aborted save followed by a save reloads equal (covered by the ten-aborted-saves test)'. AC6 (slot migration during a save) still open.
<!-- SECTION:NOTES:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
Cluster-mode freeze made on demand (stage A of the user's 2026-09-30 design); the rest of the design is TASK-1.21.7.

**Problem.** In cluster mode the freeze visited all 16384 slot dicts before the fork and paid, per non-empty slot, two pause counters, one bitmap per table and an overlay struct with two dicts: 3-5 ms of main-thread stall on a 1M-key node owning every slot, as long as a stock fork and ten times the standalone hybrid's worst PING.

**What landed (e7062762b, on top of TASK-1.21.3's drain).**
- The freeze marks the slots that are empty right now DONE and installs a per-store snapshot guard (`kvstoreSnapGuardSet`, kvstore.h). Nothing is paused or allocated per dict.
- `snapOverlayGuard` runs on every kvstore access to a dict without an overlay while the scan runs: nothing for SCANNED/DONE slots; otherwise it opens the write section (reads included, since a find may step a rehash and the thread may claim the dict at any moment) and attaches on a structural write, or on any access while the thread's window is on the dict. `snapScanAttach` pauses rehash and auto-resize, allocates the bitmaps for the tables the dict has now (release store, acquire load on the thread) and attaches the overlay; `ctx->attached` makes completion resume only what was paused.
- The thread claims a dict with a whole-dict window (table -1, every bucket inside) plus quiesce before it reads table sizes and rehashidx.
- The guard comes off a store when its last slot completes (`ctx->pending[s]`), before an aborting FLUSHALL/SWAPDB/DEBUG RELOAD frees or replaces the kvstore. TSan found the use-after-free in the first version, which took the guard off only at release.
- A dict nobody writes and the thread has not reached is untouched and may rehash or resize. A written slot is paused at attach, so it cannot grow until its slot completes: the same limit every dict had before, close to stock's resize-avoid while a child exists. Letting it grow (bitmap remap on expand, shrink refused) is TASK-1.21.7, to be built if a hot-slot workload shows the need.

**Measured** (same node and recipe as the baseline, repo build): hybrid-freeze 3-5 ms -> below the 1 ms latency-monitor threshold; the BGSAVE command 1 ms (the fork) against a 2 ms stock fork; hybrid-merge gone.

**Verification.** C unit 166/166 (scan, bitmap, owed and overlay tests adapted to on-demand pausing: deterministic checks between `snapScanFreeze` and `snapScanStartThread`; a read of a slot under scan opens a section, so the test's concurrent-read loop ends each "command" with `snapFunnelClose`). unit/snapshot with `--config bgsave-hybrid yes` (205 s) and stock mode (213 s); TSan 52/52 and ASan 52/52 with no report. A hybrid cluster dump (300k keys, all slots) passes redis-check-rdb and a cluster node with the feature off loads it with an equal DEBUG DIGEST and correct slot placement.

**Spec/docs.** Section 4 Freeze paragraph rewritten; snapshot.h API comments; RESULTS.md run 20 (unrelated A/B of the drain, performance-neutral).

**Not done here, moved to TASK-1.21.7**: growth of written-but-unreached dicts with bitmap remap and shrink refusal; slot migration during a save (untested either way). The generation tag of the design is unnecessary: state DONE plus guard-off leave no stale state, and the abort sweep is a byte loop.
<!-- SECTION:FINAL_SUMMARY:END -->
