---
id: TASK-1.60
title: >-
  Overlay index keyed by the scan window: per-window bucket array and dirty
  bitmap replace the ring, the dirty list and the drainable hook
status: Done
assignee:
  - '@claude'
created_date: '2026-10-17 10:30'
updated_date: '2026-10-17 12:18'
labels:
  - hybrid-bgsave
  - simplification
dependencies: []
references:
  - src/kvsoverlay.c
  - src/snapshot.c
  - TASK-1.50
  - TASK-1.57
documentation:
  - docs/hybrid-snapshot.md
parent_task_id: TASK-1
priority: medium
ordinal: 56000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Why: the bucket overlay (src/kvsoverlay.c) finds overlayed buckets through a per-dict hash ring (OV_RING, 1024 slots) and finds dicts with overlayed buckets through a per-store dirty list (ovDirtyList, kept in kvstoreOverlaySlot). The drain (kvstoreOverlayDrain, called from snapSaveDrainSideTables in src/snapshot.c for every db) walks all ring slots of every dirty dict and asks a per-bucket 'drainable' hook whether the scan thread has passed it.

That machinery is more general than needed. An overlay is only created for a bucket that is SNAP_POS_INSIDE the scan thread's published window (snapshot.c snapFunnel / snapBucketPosition), and the window covers one (dict, table, [lo,hi)) at a time and only moves forward. Hence every overlayed bucket belongs to one of at most two recent windows (the scan thread may publish W+1 while a write section that already overlayed under W is still open), and once the window sequence (snap_win_seq) has moved past a window, all of that window's buckets are behind the scan and can be written back without a per-bucket check.

Outcome: an overlay index that follows the window, double-buffered by sequence parity like snap_win[2]: per window slot the (dict, table, lo, seq), an ovBucket pointer per window offset and a dirty bitmap. Lookup is O(1); drain visits only the set bits of slots the sequence has passed; the dirty list, the ring, the drainable hook and the per-db drain loop go away. The window size gets a hard cap (proposed 64K buckets; today DEBUG snap-window-buckets accepts up to 1<<30, the default is 1024 = SNAP_WINDOW_BUCKETS_DEFAULT).

Decided: write-back stays on the main thread (the scan thread must not write back: concurrent writers on the overlay chain, main-thread-only dict and kvstore counters, non-thread-safe frees, main-thread iterators). Offloading only the frees is a separate idea task.

Constraint: assumes a single scan thread with a single global window. TASK-1.57 (parallel scan) would need one index per scan thread. The structure is meant to carry over to the credit-based window protocol (separate task) as a circular bitmap over [lo, hi).
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 snapWindowPublish asserts that the window only moves forward (same dict and table: lo >= previous hi; otherwise a later table or a later dict), and the full test suite passes with the assertion enabled
- [x] #2 Overlayed-bucket lookup cost does not depend on the number of overlayed dicts or buckets
- [x] #3 A drain touches only overlayed buckets, not every slot of a fixed-size table
- [x] #4 ovDirtyList, the overlay's dirty list node, ovStoreRelease and the 'drainable' hook are removed; the drain is one call per save instead of one per db
- [x] #5 DEBUG snap-window-buckets rejects values above the cap; tests that relied on larger windows are adjusted
- [x] #6 Tests cover: a dict detached or moved from its store while it has overlayed buckets, abort inside a write section, a scan crossing tables while rehashing, and a scan crossing dicts in cluster mode
- [x] #7 The overlay INFO counters (TASK-1.53) keep their meaning; TSAN and ASAN runs are clean
- [x] #8 docs/hybrid-snapshot.md and the kvsoverlay.h overview describe the window-keyed index
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
Approved 2026-10-17.

Findings that shape the design:
- kvsoverlay is generic: the unit tests in snapshot.c (ovTest* hooks) install no 'prepare', so every write is copy-on-write on arbitrary buckets. Without 'prepare' the window defaults to the whole table, seq 0.
- Up to 3 windows can have live overlays (leftovers of the window at the last drain, plus up to two seen inside one section). Use 4 index slots, assert on overflow.
- The window must come back from 'prepare' itself (the scan thread may publish between prepare and a later read).

Steps:
1. kvstore.h: kvstoreOvWindow {seq, table, lo, n}; prepare fills it on DEFER; drop 'drainable'; kvstoreOverlayDrain(uint64_t seq) writes back every bucket of windows older than seq (one call per save); kvstoreOverlayPending(void).
2. kvsoverlay.c: global index of 4 window slots {ov, table, lo, n, seq, ovBucket *b[n], dirty bitmap, count}; O(1) lookup; drain walks set bits; ovBucket carries ov and slot; un-pause in ovWriteBack when a dict's last bucket goes. Remove ring, ovDirtyList, dirty_node, ovMarkDirty/Clean, ovStoreRelease, kvstoreOverlaySlot/kvs->ov_state, mid-section eviction in ovBucketCreate. Enumeration iterates the dict's buckets across slots; iterator cursor = (slot, offset).
3. snapshot.c: snapBucketPosition returns the window; snapOverlayPrepare fills kvstoreOvWindow; snapSaveDrainSideTables makes one call with the current seq; delete snapOverlayDrainable; forward-only assert in snapWindowPublish (NULL resets); SNAP_WINDOW_BUCKETS_MAX = 65536 in DEBUG snap-window-buckets.
4. Tests: adapt unit tests to the new API; add detach-while-overlayed, window across tables mid-rehash, across dicts, slot bound.
5. Docs: kvsoverlay.h overview, kvstore.h KVSTORE OVERLAY, docs/hybrid-snapshot.md.
6. Verify: build, unit snapshot tests, tests/unit/snapshot.tcl (+ cluster variant), TSAN and ASAN.

Decided: detached dicts are drained too once the window has passed their buckets (same ovWriteBack as the release; memory returns sooner).
<!-- SECTION:PLAN:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
2026-10-17 progress: implemented. kvstore.h: kvstoreOvWindow, prepare(..., win), drainable removed, kvstoreOverlayDrain(seq)/kvstoreOverlayPending(void). kvsoverlay.c: global ov_index[4] of window slots (ovBucket array + bitmap, cap kept up to 4096 offsets), ovSlotFor/ovSlotRemove/ovSlotNext/ovSlotWriteBack, ovResume on a dict's last write-back, random walk over the dict's slots for sampling; ring, dirty list, ovStoreRelease, kvs->ov_state removed. snapshot.c: snapBucketPosition/snapBucketOwed fill the window, snapOverlayDrainable removed, one drain call, forward-only assert in snapWindowPublish (snap_win_last), SNAP_WINDOW_BUCKETS_MAX 65536. New unit test snapOverlayWindowTests (9 checks: per-window slots, drain boundary, moved dict drained, four windows at once, scan/iteration across windows). redis-server test snapshot: 209/209.

2026-10-17 verification: build clean (no warnings); redis-server test snapshot 209/209; ./runtest unit/snapshot + unit/cluster/atomic-slot-migration + integration/rdb: 217 ok, 0 err; ASan unit/snapshot 75 ok; TSan unit/snapshot 75 ok, no sanitizer reports. No test needed a window above the 65536 cap (snapshot.tcl uses size/2 = 8192). Open: AC1 wants the full suite with the assertion (only the three suites above were run); AC6 cluster-dict crossing is covered by the existing cluster/ASM tests, not by a new dedicated test.

2026-10-17: full suite ./runtest --config hybrid-snapshot yes: 164/164 units, 6319 ok, 0 err (forward-only assert enabled). AC6: detach/move (new unit test), abort inside a section (existing snapOverlayTests(1) and tcl), crossing tables mid-rehash (snapOverlayRehashTests, tcl rehash tests), crossing dicts in cluster mode (existing cluster tests in unit/snapshot and unit/cluster/atomic-slot-migration); no new dedicated cluster test was added.
<!-- SECTION:NOTES:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
The overlay index now follows the scan window. The prepare hook returns the window (kvstoreOvWindow: seq, table, lo, n) with its DEFER verdict. kvsoverlay.c keeps one index of 4 slots for the whole save; each slot holds the buckets of one window of one dict as an array indexed by idx - lo, plus a bitmap. Lookup is O(1). kvstoreOverlayDrain(seq) writes back every bucket of the windows before seq in one call per save, detached dicts included; rehash and auto-resize resume with a dict's last bucket.

Removed: the 1024-slot ring per dict, the per-store dirty list (ovDirtyList, kvs->ov_state, kvstoreOverlaySlot, ovStoreRelease), the 'drainable' hook and snapOverlayDrainable, and the mid-section write-back of ring occupants in ovBucketCreate.

Added: the forward-only assert in snapWindowPublish, the cap SNAP_WINDOW_BUCKETS_MAX = 65536 on DEBUG snap-window-buckets, and the unit test snapOverlayWindowTests (9 checks). Docs: kvsoverlay.h, kvstore.h and docs/hybrid-snapshot.md.

Verification: redis-server test snapshot 209/209; full suite with hybrid-snapshot on 164/164 units, 6319 ok; ASan and TSan clean on unit/snapshot.
<!-- SECTION:FINAL_SUMMARY:END -->
