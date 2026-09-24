---
id: TASK-1.12
title: Incremental per-slot merge with mid-save frees of fork-instant originals
status: To Do
assignee: []
created_date: '2026-09-20 17:22'
updated_date: '2026-09-21 19:58'
labels:
  - bgsave
  - persistence
  - db
  - memory
milestone: m-1
dependencies:
  - TASK-1.6
  - TASK-1.9
documentation:
  - poc-hybrid-bgsave-spec.md
parent_task_id: TASK-1
priority: medium
ordinal: 13000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Spec §4 slot-scan-complete and M2: in M1 all frees of replaced fork-instant originals are deferred to save end, so memory held by clones plus originals grows for the whole save. In M2, when a slot becomes DONE the main thread immediately frees the originals it replaced: kvobj headers (all SNAP_NOFORK) and SNAP_NOFORK values, CoW-free because the child never mapped them. SNAP_FORK values of tombstoned or value-replaced keys may optionally still be deferred to save end to avoid paying CoW on their pages (spec §5, a memory-vs-CoW tradeoff) - make this a config or a documented decision. Header-only clones of SNAP_FORK keys share the value with their original: merge frees only the original header.

Scope: free-at-merge in snapSlotMerge, respecting that a bg thread may still be reading a header or SNAP_NOFORK value only while its slot is SCANNING (so the free happens strictly after the completion signal is consumed, with a memory barrier); all frees go through the class-aware free path from TASK-1.2 so nothing enters a default tcache; handling of the lazyfree path (UNLINK / lazyfree-lazy-* configs) so originals freed via the lazyfree thread are also safe and class-aware; the SNAP_FORK value deferral decision; accounting of freed bytes in INFO persistence progress. Prove there is no use-after-free with ASan under the harness race test.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 Original headers and SNAP_NOFORK values replaced by clones or tombstones are freed when their slot becomes DONE, not at save end, and peak memory during the harness race test drops versus M1 (numbers recorded in task notes)
- [ ] #2 No original is freed while its slot is still SCANNING, verified by an ASan run of the harness race test with multiple threads
- [ ] #3 Header-only clones of SNAP_FORK keys never cause the shared value to be freed at merge, covered by an ASan test
- [ ] #4 Lazyfree configurations (lazyfree-lazy-server-del, lazyfree-lazy-user-del, UNLINK) work correctly with mid-save frees and use the class-aware free path, covered by tests
- [ ] #5 The SNAP_FORK value deferral policy is decided, documented and, if configurable, tested in both settings
- [ ] #6 INFO persistence progress reports freed-original bytes during the save
<!-- AC:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
2026-09-21: superseded if TASK-1.21 (serialize-before-write with a bucket window) lands: there is no merge and no retained originals beyond the side table, so incremental merge becomes moot. Keep on hold until the spike verdict (TASK-1.21.5).
<!-- SECTION:NOTES:END -->
