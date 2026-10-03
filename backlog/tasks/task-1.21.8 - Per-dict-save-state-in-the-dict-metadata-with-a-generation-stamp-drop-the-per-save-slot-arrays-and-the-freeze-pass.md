---
id: TASK-1.21.8
title: >-
  Per-dict save state in the dict metadata with a generation stamp: drop the
  per-save slot arrays and the freeze pass
status: Done
assignee:
  - '@Claude'
created_date: '2026-10-02 22:53'
updated_date: '2026-10-03 09:26'
labels:
  - bgsave
  - cluster
milestone: m-3
dependencies:
  - TASK-1.21.7
documentation:
  - poc-hybrid-bgsave-spec.md
modified_files:
  - src/kvstore.c
  - src/kvstore.h
  - src/snapshot.c
  - src/snapshot.h
  - src/server.c
  - tests/unit/snapshot.tcl
  - poc-hybrid-bgsave-spec.md
parent_task_id: TASK-1.21
priority: medium
ordinal: 26000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
User's design question, 2026-10-03: with one scan at a time, the scan thread does not need the per-save arrays indexed by slot (state[s][didx], attached[s][didx], bits[s][didx*2+t], pending[s] in snapScanCtx) nor the freeze pass that marks the slots empty at the fork instant DONE. The save state of a dict can live in the dict's own kvstore metadata, stamped with the save generation: a dict whose stamp is not the current generation has no save state (never touched by this save, or a leftover of an aborted one). The overlay already works this way since ab94ff647 (ov->gen, kvstore.snap_gen); this task applies the same rule to everything per dict.

Agreed shape: kvstoreDictMetaBase gains an atomic generation, an atomic state byte (PENDING/SCANNING/SCANNED/DONE, valid only when the generation matches) and one pointer to a snapshot-owned struct holding the two bitmaps, the overlay pointer (moved out of the base) and the paused flag. The struct is allocated on first touch, by the main-thread guard or by the scan thread's window, and freed at completion. Fork-instant membership is derived lazily: a dict with a stale stamp that is empty right now was empty at the fork instant (any post-fork add would have stamped it first), so the first toucher stamps it DONE; non-empty stamps PENDING. Store completion no longer uses a precomputed pending count: the thread counts the dicts it stamped SCANNING per store and the main thread declares the store done when the thread finished enumerating it and the drained completions reach that count. The thread still walks the kvstore's dict pointer array skipping NULLs (what the kvstore iterator does), but membership and status no longer live in a parallel structure.

Why: removes three 16384-entry arrays per db and the O(slots) freeze and release loops in cluster mode, makes the generation check the single source of "relevant or leftover", and shrinks the kvstore/snapshot surface reviewers see before landing. Constraint: the dict pointer must stay stable for every dict the thread may read (holds: frozen dicts are not freed during a save; KVSTORE_FREE_EMPTY_DICTS is deferred while a save runs, verify).
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 snapScanCtx has no per-slot arrays (state, attached, bits, pending) and snapScanFreeze does no per-slot pass; the per-dict save state is reachable only through the dict's metadata and is valid only under the current generation
- [x] #2 A dict empty at the fork instant that receives keys during the save is treated as DONE by its first toucher on either thread (no fork-instant key, no serialization), and a dict created during the save (allocate-on-demand) is skipped; covered by unit and cluster harness tests
- [x] #3 Leftover metadata from an aborted save is released on the next touch and at kvstoreEmpty/kvstoreRelease, counted in INFO snap_overlay_leftovers (renamed or kept), with a C unit test for each first-touch order (guard first, thread first)
- [x] #4 unit/snapshot passes in hybrid and stock modes, C unit tests pass, TSan and ASan clean; the full feature-on suite passes
- [x] #5 Spec section 4 (freeze) and the stage B / generation paragraphs describe the metadata layout and the lazy membership rule; stale comments in snapshot.h and kvstore.h updated
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
1. kvstore.h/.c: kvstoreDictMetaBase loses the overlay pointer and gains snap_word ((gen<<8)|state, __atomic builtins, both threads) and a kvstoreDictSnap pointer (bits[2]/bytes[2], ov, paused, list node) allocated by kvstoreDictSnapAttach on first touch and freed by kvstoreDictSnapRelease (overlay drained, bitmaps freed, layout resumed, unlinked). kvs->snap_dicts lists attached dicts (kvstoreSnapFirstDict for the end-of-save sweep); snapCurrent releases and counts a struct whose stamp is not kvs->snap_gen (kvstoreSnapLeftovers); kvstoreEmpty/Release release and count whatever is still attached. New hook dict_created stamps a dict allocated under a save before its pointer is published (release store; kvstoreGetDictAcquire for the thread). freeDictIfNeeded defers while hooks are set, the deferred frees run at kvstoreSnapGuardSet(NULL). Bitmap bytes accounted per store (kvstoreSnapBitmapAccount/Bytes).
2. snapshot.c: scan context drops state/attached/bits/pending; keeps per-store frozen, claimed (thread), enumerated (thread), completed (main), plus cur_slot (the dict under scan, for the abort sweep) and ended. Main thread reads an unstamped dict as PENDING if non-empty, DONE if empty (snapDictState); a write stamps with a CAS (snapDictStamp); the thread claims with a CAS to SCANNING (snapScanClaim) after an acquire load of the dict pointer. Freeze is O(stores): kvstoreSize==0 stores are skipped, slots_total = allocated dicts at freeze bounds the ring. Store done when enumerated and completed==claimed; snapScanFinishAll completes cur_slot and the store's attached list at abort/release.
3. Tests: C unit adapted (PENDING-before-start becomes nothing-owed, completion counts over populated slots, abort counts over reached slots, empty stores visit nothing) plus snapScanLazyStampTest (guard-first and thread-first orders); tcl cluster test: 300 keys into a slot empty at the fork instant during a slow save never reach the RDB. INFO field snap_overlay_leftovers renamed snap_save_leftovers.
4. Verify: C unit, unit/snapshot hybrid and stock, TSan and ASan, full feature-on suite. Spec section 4 freeze paragraph and 11.z notes updated.
<!-- SECTION:PLAN:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
2026-10-03: implemented as planned (kvstore.h/.c, snapshot.c/.h, server.c, tests, spec). C unit 171/171 after three fixes: progress counted scanned slots twice (snapScanPublishScanned still incremented slots_scanned), bitmap bytes could not stay in the scan context once the kvstore frees a leftover's bitmaps (moved to a per-store counter), and the abort test's 'completed normally' count had included slots empty at freeze. unit/snapshot hybrid run in progress.

2026-10-03: C unit 181/181 (new snapScanLazyStampTest, both first-touch orders); unit/snapshot hybrid 75 ok including the new cluster test 'keys written into a slot empty at the fork instant never reach the snapshot'; stock 75 ok. TSan and ASan runs in progress (fresh .scratch/tsan and .scratch/asan trees; first attempt failed to build because the rsync exclude 'redis-check-*' also dropped redis-check-rdb.c and redis-check-aof.c). Full feature-on suite still to run.

2026-10-03: TSan (setarch -R, MALLOC=libc) and ASan runs of unit/snapshot with the feature on: 69 ok each, 'All tests passed without errors', no sanitizer reports. C unit 184/184 after adding the kvstoreEmpty leftover case to snapOverlayLeftoverTest. Full feature-on suite started.

2026-10-03: full feature-on suite (164 units, 8 clients): one failure, 'FLUSHALL while the scan thread waits at the queue bound': wait_for_condition 'queue did not fill' timed out after 5 s under the loaded suite (rdb-key-save-delay sleeps stretch); the test passed alone 3/3 before and after raising the four queue-fill waits to 15 s. Everything else green (unit/snapshot hybrid 75 and stock 75, C unit 184, TSan and ASan clean).
<!-- SECTION:NOTES:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
The per-slot arrays of the scan context (state, attached, bits, pending) and the O(slots) freeze and release loops are gone. A dict carries its own save state in kvstoreDictMetaBase: a word (generation << 8 | state) both threads access with atomics, and a kvstoreDictSnap (bitmaps, overlay, pause flag) attached on first touch and freed at completion. An unstamped dict is untouched since the fork instant: PENDING when non-empty, DONE when empty (main thread), stamped DONE at creation when allocated during the save (dict_created hook), claimed by CAS by the thread. The freeze is O(stores); store completion is "thread passed the last dict and every claimed dict completed"; the abort sweep completes the dict under scan plus the store's list of attached dicts. Empty-dict frees are deferred while a store is under a save. Leftover state of any kind is released and counted on access and at kvstoreEmpty/kvstoreRelease (INFO snap_save_leftovers). Verified: C unit 184/184 (new lazy-stamp test for both first-touch orders, kvstoreEmpty leftover case), unit/snapshot hybrid and stock 75 ok each (new cluster test: 300 keys into a slot empty at the fork instant never reach the RDB), TSan and ASan clean, full feature-on suite green apart from one 5 s timing wait that now has 15 s.
<!-- SECTION:FINAL_SUMMARY:END -->
