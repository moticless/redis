---
id: TASK-1.46
title: >-
  The overlay struct, its slot type and chain constants are private to
  kvsoverlay.c; the shared header carries a forward declaration and prototypes
  only
status: Done
assignee:
  - '@Claude'
created_date: '2026-10-11 07:13'
updated_date: '2026-10-11 07:22'
labels:
  - hybrid-bgsave
  - refactor
milestone: m-1
dependencies:
  - TASK-1.45
references:
  - 'src/kvsoverlay.h (OV_SLOTS, ovSlot, kvstoreDictOverlay)'
  - src/kvstore.c ovActive
modified_files:
  - src/kvstore.c
  - src/kvsoverlay.c
  - src/kvsoverlay.h
parent_task_id: TASK-1
priority: low
ordinal: 42000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Requested by the user on 2026-10-11, following TASK-1.45: kvstore.c reads exactly two fields of kvstoreDictOverlay (entries and tombs, in the inline ovActive), so the struct, the ovSlot type and the OV_* chain constants need not be in the shared header. Outcome: they move into kvsoverlay.c; kvsoverlay.h keeps a forward declaration, the cursor typedef and prototypes, with ovActive out of line (it runs only for a frozen dict during a save; the common path returns at the no-save check before it). No behavior change.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 kvsoverlay.h contains no struct definition, no OV_* constant and no ovSlot; kvstore.c and snapshot.c compile against the forward declaration only
- [x] #2 ovActive is an out-of-line function in kvsoverlay.c; the inline ovRead/ovWrite/ovFrozen in kvstore.c are otherwise unchanged
- [x] #3 Build without warnings; redis-server test kvstore and test snapshot pass; unit/snapshot passes with bgsave-hybrid yes and no
<!-- AC:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
The 'Snapshot overlay' block of kvsoverlay.h (OV_SLOTS, OV_SLOT_MASK, the chain enum, OV_IS_TOMB, ovSlot, struct kvstoreDictOverlay) moved verbatim into kvsoverlay.c after its includes; ovActive became an out-of-line function there. kvsoverlay.h keeps 'typedef struct kvstoreDictOverlay kvstoreDictOverlay;', the ovCursor typedef, the four store accessors and the ov* prototypes: 105 -> 68 lines. kvstore.c lost the inline ovActive and otherwise compiles against the forward declaration (its only overlay field reads were the two counters in ovActive). Header intro comments merged into one accurate one. Verified: build without warnings, redis-server test kvstore and snapshot (184), unit/snapshot hybrid 81 ok, stock 81 ok.
<!-- SECTION:NOTES:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
The overlay struct is private to kvsoverlay.c; the header shared with kvstore.c carries a forward declaration and prototypes only (68 lines), and the one field check kvstore.c needed, ovActive, is an out-of-line call reached only for a frozen dict during a save. Together with TASK-1.45 this leaves kvstore.c and kvsoverlay.c each owning its own struct, with the public API in kvstore.h unchanged. No behavior change; verified by the C kvstore and snapshot tests and unit/snapshot in both modes.
<!-- SECTION:FINAL_SUMMARY:END -->
