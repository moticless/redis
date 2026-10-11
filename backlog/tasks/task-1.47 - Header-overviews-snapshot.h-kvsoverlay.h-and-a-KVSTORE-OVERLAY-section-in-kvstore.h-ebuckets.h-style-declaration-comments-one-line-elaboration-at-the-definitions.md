---
id: TASK-1.47
title: >-
  Header overviews: snapshot.h, kvsoverlay.h and a KVSTORE OVERLAY section in
  kvstore.h, ebuckets.h style; declaration comments one line, elaboration at the
  definitions
status: Done
assignee:
  - '@Claude'
created_date: '2026-10-11 07:54'
updated_date: '2026-10-11 08:02'
labels:
  - hybrid-bgsave
  - docs
milestone: m-1
dependencies: []
references:
  - src/ebuckets.h (style reference)
  - src/snapshot.h
  - src/kvsoverlay.h
  - src/kvstore.h
  - docs/hybrid-bgsave.md
modified_files:
  - src/snapshot.h
  - src/snapshot.c
  - src/kvsoverlay.h
  - src/kvstore.h
  - src/kvstore.c
parent_task_id: TASK-1
priority: medium
ordinal: 43000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Requested by the user on 2026-10-11. The headers of the hybrid BGSAVE feature carry their context as long comments attached to individual declarations (snapshot.h: 409 lines, many 5-12 line comments; kvstore.h: a 20-line overlay block in the middle of the file), which makes a first read hard and the declarations themselves hard to scan. Outcome: each header opens with an overview in the style of ebuckets.h (titled sections with underlined headings, a reader can understand the design from the top of the file), snapshot.h and kvsoverlay.h get one, kvstore.h gets a KVSTORE OVERLAY section right after its KVSTORE section; the comment on every declaration is one concise line; the elaboration the declarations carried today moves to the function definitions in snapshot.c / kvsoverlay.c / kvstore.c, which mostly have no comment yet. No code change; stale references (names that no longer exist) fixed on the way.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 snapshot.h, kvsoverlay.h and kvstore.h each open with an overview in ebuckets.h style (titled, underlined sections); kvstore.h's is a KVSTORE OVERLAY section after KVSTORE
- [x] #2 Every declaration comment in snapshot.h and kvsoverlay.h is one line; the hybrid additions to kvstore.h follow the same rule; nothing the old comments said is lost, it lives at the definition
- [x] #3 No stale names in the three headers (e.g. kvstoreDictSnap); configuration names match config.c
- [x] #4 Build without warnings; redis-server test snapshot passes (comment-only change)
<!-- AC:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
snapshot.h rewritten: overview sections WHAT IS HYBRID BGSAVE, KEY CLASSES AND THE SNAP_NOFORK ARENA, ONE SAVE END TO END (five numbered steps), THE BUCKET WINDOW AND WRITE SECTIONS, MEMORY (the three bounded quantities and their configs), ENTRY POINTS, WHERE TO LOOK; every declaration one line with a trailing comment; same declaration set as before (one dangling comment with no declaration dropped). 30 of its former multi-line comments moved onto the definitions in snapshot.c that had none (a script matched each name to its definition and skipped the ones already commented). kvsoverlay.h: WHAT IS KVSOVERLAY, WHO INCLUDES THIS, THE SIDE TABLE (four chains), LIFECYCLE (attach, write, drain, release, detach); declarations one line. kvstore.h: KVSTORE OVERLAY section after KVSTORE (the guard, frozen dicts, the side table, drain/release, reference counting, where the implementation is); the 90-line hook/reference/overlay block condensed to 39 lines of one-line comments; the cursor typedef comment one line; the detail that left the header (guard semantics, hooks lifetime, rehash/shrink hooks, reference rules, atomic counts) now sits on kvstoreSnapGuardSet and kvstoreDictRetain in kvstore.c. Config names checked against config.c (save-small-max-bytes, bgsave-hybrid, bgsave-hybrid-max-retained-bytes, bgsave-hybrid-max-queued-bytes); no stale identifiers (kvstoreDictSnap, snap_word, snapNofork*, kvstoreOverlayStats) in the three headers. Build without warnings, redis-server test snapshot 184, test kvstore. Commit: see git log for TASK-1.47.
<!-- SECTION:NOTES:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
The three hybrid headers now read top-down in ebuckets.h style: snapshot.h explains the feature (classes, arena, one save end to end, window and write sections, memory bounds, entry points, where to look), kvsoverlay.h explains the overlay's side table and lifecycle, and kvstore.h gained a KVSTORE OVERLAY section next to its KVSTORE one. Every declaration carries a one-line comment; the elaboration moved to the function definitions, which mostly had none. snapshot.h went from 409 to 290 lines and kvstore.h from 273 to 254, with nothing the old comments said lost. Comment-only change, build clean, C tests green.
<!-- SECTION:FINAL_SUMMARY:END -->
