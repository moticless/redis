---
id: TASK-1.21.1
title: >-
  Bucket window publication, quiescence handshake and resize lockout for dicts
  under a hybrid save (standalone)
status: Done
assignee:
  - '@moti.cohen@redis.com'
created_date: '2026-09-21 19:57'
updated_date: '2026-09-21 20:52'
labels:
  - bgsave
  - db
  - concurrency
milestone: m-3
dependencies: []
documentation:
  - poc-hybrid-bgsave-spec.md
parent_task_id: TASK-1.21
priority: high
ordinal: 11910
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Spec section 11.z, invariants 2 and 4 and the handshake paragraph. The walker replaces the frozen-slot state machine with a published window W = (dict, table, [lo, hi)) of K buckets and walks chains sequentially with atomic loads (rehash paused, no reverse-binary cursor). Before touching a new window it stores W and waits for main-thread quiescence: the main thread keeps an atomic counter incremented when a write section opens (odd = inside) and closes, reads W after the opening increment; the walker loads the counter after storing W and waits while it is odd and unchanged. Both store-then-load pairs must be seq_cst. A write section opens at the first write funnel hook and closes when the outermost command returns, at beforeSleep and when a module thread releases the GIL. Dicts under a save have rehashing paused and expand/shrink forbidden for that dict (including the kvstore resize cron and kvstoreExpand) until the save ends; a dict mid-rehash at freeze keeps both tables and the window names the table. The kvstore overlay gets 'enter' and 'covers' hooks: the existing in-place paths (writes to scanned keys, placeholder tombstones) are only taken when the key's bucket is outside the window. Per-save bucket bitmaps are allocated at freeze and the walker skips buckets whose bit is set (nobody sets bits yet in this task). Standalone single-db only.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 The walker never reads a bucket outside its published window and never advances while the main thread is inside a write funnel that started before the publication (TSan clean under the race harness in arena-free mode)
- [x] #2 Dicts under a hybrid save do not expand, shrink or rehash for the whole save and resume normal resizing afterwards, covered by a test that adds many keys during a slow save
- [x] #3 An idle server does not stall the walker: a save with zero client traffic completes as fast as today
- [x] #4 The per-funnel cost when no hybrid save is active is unchanged (callgrind instruction count of the 200k SET microbench within 0.5% of commit 033f94bbf)
- [x] #5 Unit checks in redis-server test snapshot cover window membership for both tables of a rehashing dict
<!-- AC:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
2026-09-21: committed 2e69194bc. dictWalkBucketRange (atomic chain loads); window publish/quiesce with seq_cst pairs; write sections opened by the overlay 'enter' hook, closed in afterCommandEx (nesting 0), beforeSleep, moduleReleaseGIL; walker gives up quiescence on abort (the aborting command may hold the section). kvstoreOverlayHooks {clone, free_orig, scanned, tomb, enter, covers}; in-place scanned-key writes and placeholder tombstones only when the bucket is outside the window. Per-save bucket bitmaps (struct snapBitmap {b, bytes} per table). Resize lockout: kvstoreTryResizeDicts and kvstoreExpand skip dicts with pauseAutoResize > 0. DEBUG SNAP-WINDOW-BUCKETS. Unit: 154/154 (handshake, abort-in-section, window membership both tables, bitmap skip). tcl unit/snapshot: all passed (80 s) incl. new resize-lockout test. Pending: TSan run, callgrind vs 033f94bbf.

2026-09-21 verification: TSan (SANITIZER=thread MALLOC=libc, arena-free) unit/snapshot all passed, no reports, 102 s (AC1). tcl resize-lockout test passes; public dictExpandIfNeeded/dictShrinkIfNeeded now return DICT_ERR while pauseAutoResize > 0 so dictSetKeyAtLink's insert path cannot expand a frozen dict either (AC2). Idle save of 1M x 64B keys, hybrid on, alternating runs: prev 0.99-1.33 s, new 0.90-1.11 s (AC3). callgrind 200k pipelined SETs, feature off: 033f94bbf 1,237,809,681 Ir vs new 1,234,338,612 Ir (-0.28%) (AC4). Unit tests 155/155 incl. window membership on both tables (AC5). Commits 2e69194bc, plus the dict.c follow-up.
<!-- SECTION:NOTES:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
Bucket window layer for the hybrid save (spec 11.z, standalone). The scan thread publishes (dict, table, [lo,hi)) with seq_cst and waits for the main thread's open write section to close; write sections open at the overlay's first write hook and close at outermost command end, beforeSleep and module GIL release; the walker gives up waiting on abort. Chains are read with atomic loads (dictWalkBucketRange). Overlay hooks struct with enter/covers; in-place scanned-key writes and placeholder tombstones only outside the window. Per-save bucket bitmaps (skip emitted buckets). Resize lockout for frozen dicts across cron, kvstoreExpand and the insert path. DEBUG SNAP-WINDOW-BUCKETS. Verified: unit 155/155, tcl suite, TSan clean, callgrind -0.28% vs previous commit with the feature off, idle save time unchanged.
<!-- SECTION:FINAL_SUMMARY:END -->
