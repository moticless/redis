---
id: TASK-1.21.3
title: 'In-window side table with drain, and removal of the frozen-dict overlay'
status: To Do
assignee: []
created_date: '2026-09-21 19:57'
updated_date: '2026-09-21 19:58'
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
priority: high
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
