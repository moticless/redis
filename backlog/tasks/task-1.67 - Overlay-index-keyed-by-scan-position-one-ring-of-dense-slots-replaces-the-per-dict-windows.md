---
id: TASK-1.67
title: >-
  Overlay index keyed by scan position: one ring of dense slots replaces the
  per-dict windows
status: Done
assignee:
  - '@claude'
created_date: '2026-11-08 07:46'
updated_date: '2026-11-08 08:48'
labels:
  - hybrid-bgsave
  - simplification
dependencies:
  - TASK-1.66
references:
  - src/kvstore_overlay.c
  - src/kvstore_overlay.h
  - src/kvstore.h
  - src/hybrid_snapshot.c
  - TASK-1.66
  - TASK-1.60
documentation:
  - docs/hybrid-snapshot.md
modified_files:
  - src/kvstore_overlay.c
  - src/kvstore_overlay.h
  - src/kvstore.h
  - src/hybrid_snapshot.c
  - docs/hybrid-snapshot.md
parent_task_id: TASK-1
priority: medium
ordinal: 63000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Why: after TASK-1.66 each dict's overlay owns a list of windows (aligned runs of W buckets, each an array and a bitmap). The window is an indirection the scan does not need: every bucket already has a scan position, positions only grow along the scan, and the live overlayed buckets always sit within about two credit windows of positions. A single ring over positions names a bucket's place directly and writes back per bucket exactly when the thread passes it.

Outcome: the overlay module keeps one index for the save, a power-of-two ring of slots keyed by a dense scan index: each dict paused by the save gets the next run of indices (its bucket count in the frozen layout, table 0 from rehashidx then table 1) so consecutive buckets of the scan take consecutive slots. prepare returns the bucket's scan position and dense index; the overlay derives the dict's base once from the first answer and computes every later slot from (table, idx) and the paused dict's stable rehashidx. Slots chain (a collision costs a compare, never an answer), so an out-of-order pause or a thread racing a section's cap is harmless. The drain writes back every bucket whose position is below lo, per bucket. Each overlay keeps the list of its buckets for release and enumeration. Windows, kvsoWindow and kvstoreOverlayWindowsInUse go away.

To keep the live span small (so slots rarely collide), the buckets the thread has passed are written back before every raise of hi: at section open before the cap and before every grant between statements. The 16-window grant limit goes with it.

Ring size: 4 x W rounded up to a power of two, at least 1024 (the thread announces up to 128 + 16 buckets past lo), allocated at scan create and freed at scan release.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 An overlayed-bucket lookup is one slot computation, one load and a compare of (overlay, table, idx) per chained entry; no list or array of windows is walked
- [x] #2 kvstoreOverlayDrain(below) writes back exactly the buckets whose scan position is below 'below', across every store and detached dicts included
- [x] #3 A bucket deferred under any order of pauses or positions is found again and written back (collisions chain; no assertion on them)
- [x] #4 The write-back of passed buckets runs before hi is raised: at section open and before each grant
- [x] #5 Unit tests (redis-server test hybrid_snapshot) pass, including a rewritten index test: positions in several dicts, drain boundaries, a moved dict, ten buckets of one dict drained in position order
- [x] #6 tests/unit/hybrid_snapshot.tcl passes with hybrid-snapshot on
- [x] #7 kvstore_overlay.h overview and docs/hybrid-snapshot.md describe the position-keyed index and the drain-before-grant rule
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
1. kvstore_overlay.h: kvstoreOvPos {pos, slot} replaces kvstoreOvWindow in the prepare hook; kvstoreOverlayIndexInit(slots)/kvstoreOverlayIndexFree(); kvstoreOverlayWindowsInUse removed; prose for the position-keyed index.
2. kvstore_overlay.c: one static index {slot chains, bitmap, mask, count}; kvsoBucket gains hnext (chain), onext/oprev (the overlay's list), pos, slot; kvstoreDictOverlay gains buckets list and base (set from the first DEFER: slot - dense offset); kvsoDenseOffset(d, table, idx) = table 0 from rehashidx then table 1; lookup = slot + chain compare; drain sweeps the bitmap and writes back pos < below; release walks the overlay's list; enumeration and random sampling over the overlay's list; cursor keeps the next bucket pointer (kvstoreOvCursor.ob).
3. hybrid_snapshot.c: hybsnapDictState.index_base and ctx->index_next assigned in hybsnapScanPauseLayout (bucket count of the frozen layout); hybsnapBucketPosition fills pos and slot; index init in hybsnapScanCreate (4W, min 1024, power of two), free in hybsnapScanRelease; hybsnapSectionDrain before the cap in hybsnapSectionOpen and before the grant in hybsnapWindowRefill; HYBSNAP_OV_WINDOWS_SOFT removed; test prepare hook returns positions; index tests adjusted.
4. docs/hybrid-snapshot.md: overlayed-buckets paragraph, grant rule, memory table, glossary.
5. Verify: build, redis-server test hybrid_snapshot, ./runtest --single unit/hybrid_snapshot --config hybrid-snapshot yes.
<!-- SECTION:PLAN:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
2026-11-08: implemented, build clean, redis-server test hybrid_snapshot 227/227. Safety note for the new drain points: at section open the drain runs at a command's first hook, before any lookup of that command; a lookup through the overlay returns the clone for an overlayed bucket, never its original, so a write-back there frees nothing a caller holds (same footing as the cron drain that already did open/drain/close between commands). Before a grant the section's statement boundary gives the same guarantee the grant itself relies on. tcl run in progress.

2026-11-08 verification: ./runtest --single unit/hybrid_snapshot --config hybrid-snapshot yes: 96 ok, 0 err. Not run: full suite, ASan/TSan. Nothing committed; the tree also carries unrelated uncommitted work.

2026-11-08 follow-up (same task): the dense offset formula now lives only in kvstore_overlay.c. The snapshot hands a paused dict its base once, kvstoreOverlayIndexBase(d, base) (stored in kvstoreDictMetaBase.ov_index_base; returns the dict's slot count so the caller advances its counter), and prepare returns only the scan position (uint64_t *): kvstoreOvPos, hybsnapDictState.index_base and the overlay's has_base/base are gone. The index unit test numbers its dicts after the freeze the way the pause would. Build clean, redis-server test hybrid_snapshot 227/227; tcl run in progress.

2026-11-08 follow-up verification: ./runtest --single unit/hybrid_snapshot --config hybrid-snapshot yes after removing the duplicated formula: 96 ok, 0 err.

2026-11-08 ASan: .scratch/asan tree (SANITIZER=address MALLOC=libc, no arena) synced and built clean; ./runtest --single unit/hybrid_snapshot --config hybrid-snapshot yes --timeout 600 --dont-clean: 85 ok, 0 err, no AddressSanitizer/LeakSanitizer report in any server log. Two tests skipped by name: the uncommitted held-dict table-growth tests ('keeps table 0 during a save and grows into a table 1 at 1:1' and 'grows a table 1 under adds, updates, deletes and renames'), which assume the hybrid arena (dictResizePolicy's arena override) and fail in any libc-malloc build; a first run without the skips failed on exactly those two and then cascaded (CONFIG SET hybrid-snapshot refused while the save ran). They need a jemalloc/arena guard, unrelated to this task.
<!-- SECTION:NOTES:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
The overlayed buckets of a hybrid save are indexed by their place in the scan. kvstore_overlay.c keeps one index per save (kvstoreOverlayIndexInit/Free, called by hybsnapScanCreate/Release): a ring of 4W slots (power of two, at least 1024) keyed by a dense scan index, each slot a chain of the buckets that map to it, plus a bitmap of the slots in use. The prepare hook returns kvstoreOvPos {pos, slot}: the bucket's scan position and dense index. The overlay derives its dict's base from the first DEFER (slot minus the bucket's offset in the frozen layout: table 0 from rehashidx, then table 1) and computes every later slot from (table, idx) and the paused dict's stable rehashidx, so a lookup is one slot, one load, one compare. Each dict paused by the save takes the next run of dense indices in hybsnapScanPauseLayout (dicts are paused in scan order by the cap and the grants), so consecutive buckets of the scan take consecutive slots; a pause out of order could only cost collisions, which chain. Each overlay keeps the doubly linked list of its buckets for release, phase-2 enumeration and random sampling. The drain sweeps the bitmap and writes back every bucket whose position is below lo, per bucket.

To keep the live buckets within the credit window, the passed buckets are written back before every raise of hi: hybsnapSectionOpen drains before the cap and hybsnapWindowRefill drains before the grant (both points hold no caller link). The 16-window grant limit (HYBSNAP_OV_WINDOWS_SOFT, kvstoreOverlayWindowsInUse) is gone, as are kvsoWindow and the per-dict window lists of TASK-1.66.

Tests: the index unit test's prepare hook returns positions and a per-dict run of dense slots; the ten-bucket case drains in position order (drain(35) writes back positions 30..34). Docs: kvstore_overlay.h overview, docs/hybrid-snapshot.md (overlayed buckets, grants, memory table, glossary).

Verification: build clean; redis-server test hybrid_snapshot 227/227; ./runtest --single unit/hybrid_snapshot --config hybrid-snapshot yes 96 ok, 0 err.

Follow-up in the same task: the dense offset is computed in one place. kvstoreOverlayIndexBase(d, base), called from hybsnapScanPauseLayout, records the dict's base in its kvstore metadata and returns the run length; prepare returns only the scan position; kvstoreOvPos and the snapshot-side copy of the offset formula are removed.
<!-- SECTION:FINAL_SUMMARY:END -->
