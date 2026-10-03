---
id: TASK-1.21.9
title: >-
  Overlay side table as per-bucket chains in a fixed array instead of the temp
  and state dicts
status: To Do
assignee: []
created_date: '2026-10-03 10:26'
labels:
  - bgsave
milestone: m-3
dependencies:
  - TASK-1.21.8
documentation:
  - poc-hybrid-bgsave-spec.md
parent_task_id: TASK-1.21
priority: medium
ordinal: 27000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
User's proposal, 2026-10-03: replace the overlay's two dicts (temp: clones and adds keyed by key; state: frozen key to tombstone/shared flags, both of a cloned dictType with callbacks nulled, which is where the 2026-10-03 onDictRelease sanitizer hit came from) with one plain array of chain heads. An in-window write creates an entry {status (clone deep/shared, add, tombstone), key (the clone or the add, in the no-value dict's key-only entry encoding so links handed to callers keep working with dictGetKV), table, idx, next} in the chain at array[idx & (W-1)], where (table, idx) is the key's frozen bucket, already computed by the prepare hook. W is a constant power of two (1024 to start), independent of snap_window_buckets: entries of the previous window and the current one coexist until the main thread drains, and the whole-dict window at claim time defers writes to any bucket, so the index must be the bucket modulo W, not an offset in the window; W only bounds chain length. The array is allocated on the first deferred write (only the dict under the thread's window ever needs one), not at attach, so dicts attached by writes ahead of the thread keep only their bitmaps.

Why: one structure instead of two dicts and a cloned type, no second hashing or key compare per side-table operation, the mapping to the real dict is the stored bucket so the drain is a splice without rehash, and the iterator/scan phase over the side table becomes an array walk. The dict is paused while entries exist (window or SCANNING), so stored indices stay valid; the release drain runs before the resume.

Scope: kvstore.c overlay module (attach/drain/map/iterators ov_phase/scan callback/two-phase unlink/stats), hooks unchanged in meaning. The existing C overlay tests (snapOverlayTests, leftover tests) and unit/snapshot cover every path.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 The overlay holds no dict: side-table entries live in per-bucket chains of a fixed power-of-two array allocated on the first deferred write and freed at release; kvstoreOverlayAttach allocates nothing but the overlay struct
- [ ] #2 Links returned to callers for side-table entries work with dictGetKV/dictSetKeyAtLink exactly as temp-dict links did (C overlay tests unchanged in expectations: clone on write lookup, read after write returns the clone, tombstones hide originals, adds visible, two-phase unlink)
- [ ] #3 Drain applies each entry to its stored frozen bucket without rehashing; iterators and kvstoreScan see frozen entries then side-table adds once each; counts (size, adds, tombstones, pending) match the dict semantics
- [ ] #4 unit/snapshot hybrid and stock, C unit tests, TSan and ASan clean; full feature-on suite passes; spec section on the side table updated
<!-- AC:END -->
