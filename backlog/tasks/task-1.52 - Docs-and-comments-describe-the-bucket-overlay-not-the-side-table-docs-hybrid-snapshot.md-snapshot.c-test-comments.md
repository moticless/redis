---
id: TASK-1.52
title: >-
  Docs and comments describe the bucket overlay, not the side table
  (docs/hybrid-snapshot.md, snapshot.c test comments)
status: Done
assignee:
  - '@Claude'
created_date: '2026-10-16 09:22'
updated_date: '2026-10-16 09:30'
labels:
  - hybrid-bgsave
  - docs
dependencies:
  - TASK-1.50
references:
  - docs/hybrid-snapshot.md
  - src/snapshot.c
  - src/kvsoverlay.c
parent_task_id: TASK-1
priority: medium
ordinal: 48000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
docs/hybrid-snapshot.md and a few test comments in src/snapshot.c still describe the per-key side table that TASK-1.50 replaces. Update them so a reader gets the current design: overlayed buckets in a ring, reads check the overlay first, writes in the window duplicate the bucket chain, write-back after the scan passes, rehash/resize paused while any bucket is overlayed, and a one-table statement of the AHEAD / INSIDE / BEHIND bucket states (who reads, who writes, what a write does in each). Also use the term "classic" for the fork-only path.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 grep -ni 'side table' docs/hybrid-snapshot.md src/snapshot.c src/kvsoverlay.* src/kvstore.* finds no description of the replaced design
- [x] #2 docs/hybrid-snapshot.md has the bucket-state table and the write-back / rehash-pause explanation, with the figures updated or removed if they show the side table
- [x] #3 The layout-phase (whole-dict window) rule is stated: writes are emit-ahead then in place
<!-- AC:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
docs/hybrid-snapshot.md: sections 2 (step 5, figure caption), 3 (renamed "The write funnel and the bucket overlay": position table with the layout-phase row and the duplicate-chain action, "The overlayed buckets", Clone, "Write-back and release"), 4 (memory table), 8 (INFO fields), 9 (glossary) and 10 (invariants 3 and 7) describe the bucket overlay. Source comments in kvstore.c and the test comments in snapshot.c reworded. Remaining: the PNG docs/images/hybrid-snapshot-flow.png still draws per-key "dup"/"tomb"; it cannot be regenerated here, so the caption says it has not been redrawn. The INFO field snap_save_side_entries keeps its name (used by tests/unit/snapshot.tcl, tests/unit/cluster/atomic-slot-migration.tcl and bench probes) and is documented as overlayed buckets pending write-back; a rename belongs with TASK-1.53.
<!-- SECTION:NOTES:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
Docs and comments describe the bucket overlay (ring of overlayed buckets, duplicate chains, write-back, rehash pause, layout phase as AHEAD). No "side table" description of the replaced design is left in docs/hybrid-snapshot.md, src/*.c or src/*.h. The flow figure is a PNG that still shows per-key dup/tomb; the caption says so.
<!-- SECTION:FINAL_SUMMARY:END -->
