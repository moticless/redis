---
id: TASK-1.34
title: 'kvstore: move the snapshot overlay and per-dict save state to kvsoverlay.c'
status: Done
assignee: []
created_date: '2026-10-05 06:19'
updated_date: '2026-10-05 10:27'
labels:
  - refactor
  - kvstore
  - landing
dependencies: []
modified_files:
  - src/kvsoverlay.c
  - src/kvstore.c
  - src/kvstore.h
  - src/Makefile
parent_task_id: TASK-1
priority: medium
ordinal: 30000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
About 850 of kvstore.c's 2046 lines were snapshot support (TASK-1.21.3/1.21.8/1.21.9): the side-table block (struct, chains, cursor, clone/insert/remove, drain, release, per-dict save-state attach/release, leftovers, bitmap accounting) and the six overlay write paths (ovDictFind .. ovDictDelete). Move them to a new src/kvsoverlay.c so stock kvstore.c keeps only hooks and dispatch (ovGet in the public wrappers, the overlay branch of iterators/scan/random, dict_created/onDictRelease, deferred empty-dict frees). This shrinks the stock-kvstore diff for the landing PR and makes the overlay an additive file.

Decision (2026-10-05): no private kvstore_int.h. struct _kvstore, the overlay types (ovSlot, OV_*, kvstoreDictOverlay, ovCursor), the inline fast paths (ovPeek/ovGet/ovActive/snapCurrent) and the ov* prototypes go at the bottom of kvstore.h under #ifdef KVSTORE_INTERNAL, defined only by kvstore.c and kvsoverlay.c (neither includes server.h, the only other includer of kvstore.h). Pure move: no logic change, reviewed with git diff --color-moved. ov* names stay (they are hidden behind the guard; no clash in the tree).
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 src/kvsoverlay.c holds the side table, drain/release, per-dict save state (attach/release/leftovers/bitmap accounting/guard set) and the ov* write paths; kvstore.c keeps only dispatch and the dict hooks
- [x] #2 kvstore.h gains a KVSTORE_INTERNAL section (struct _kvstore, overlay types, inline fast paths, ov* prototypes) with a comment that it is internal to the two kvstore files; snapshot.c and server.h see only the public API
- [x] #3 src/Makefile builds kvsoverlay.o; no logic change (diff reviewed with --color-moved)
- [x] #4 C unit (redis-server test kvstore, test snapshot), unit/snapshot in hybrid and stock mode, TSan and ASan unit/snapshot pass; full suite with bgsave-hybrid yes green
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
1. Split kvstore.c by line ranges with a checked script (scratchpad split.py): per-dict save state (kvstoreSnapGuardSet, DictSnapAttach/Release, bitmap accounting, GetDictAcquire, leftovers), side table (chains, cursor, clone/insert/remove, apply, drain, release), enumeration helpers (ovScanCb, ovRandomEntry, new ovGetSomeKeys extracted from kvstoreDictGetSomeKeys), the ov* write paths. Exported routines lose `static` and the `noinline` attribute (cross-TU now).
2. kvstore.h: KVSTORE_INTERNAL section at the end (struct _kvstore, overlay types, inline ovPeek/ovGet/ovActive/snapCurrent, prototypes). Both .c files #define it before their includes.
3. Makefile: kvsoverlay.o after kvstore.o. The overlay's dict frees go through the public kvstoreFreeDictIfNeeded.
4. Verify: C unit (kvstore, snapshot), unit/snapshot hybrid + stock, TSan + ASan trees (rsync excluding *.o, *.d, .make-settings, Makefile.dep and the binaries by name), full suite hybrid.
<!-- SECTION:PLAN:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
Pure move, scripted by line ranges with boundary assertions (scratchpad split.py). kvstore.c 2046 -> 1293 lines; kvsoverlay.c 684 lines; kvstore.h +150 (KVSTORE_INTERNAL section: struct _kvstore, ovSlot/OV_*/kvstoreDictOverlay/ovCursor, inline snapAttached/snapGenOf/snapCurrent/ovPeek/ovGet/ovActive, ovGetR/W macros, struct ovScanCtx, prototypes). Lines appearing only once across the diff: dropped `static`/`noinline` on the exported ov* routines, their prototypes, includes and section headers, ovGetSomeKeys extracted from kvstoreDictGetSomeKeys (reindented body), the overlay's two dict frees routed through the public kvstoreFreeDictIfNeeded. kvstoreDictSnapRelease placed after the side-table code (it calls ovRelease). Comment 'see kvstoreDictFindLink in kvstore.c for the contract' on ovDictFindLink; the cursor comment stays with the typedef in the header, a one-line pointer at ovCursorAdvance.

Verification (2026-10-05): build warning-free; C unit kvstore + snapshot (184); unit/snapshot hybrid and stock green; ASan unit/snapshot green; full suite --config bgsave-hybrid yes green (12:09-12:24). TSan unit/snapshot: first run hung in 'Hybrid BGSAVE: active defrag enabled during a save' (client IN PROGRESS, harness TIMEOUT after 2.5 h, logs cleaned by the harness); the test alone under TSan passes in 13 s with no reports; a full rerun with --timeout 600 --dont-clean passed with no ThreadSanitizer output (the two REDIS BUG REPORTs in server .7 are the crash-in-child test killing its child on purpose). The hang is tracked separately (TASK-1.35); this change cannot alter timing-independent semantics.

Sanitizer-tree recipe that worked: rsync src/ excluding *.o, *.d, .make-settings, Makefile.dep and the six binaries BY NAME (not redis-check-*), plus modules/ and tests/ (minus tmp), rm vset objects, make -C src SANITIZER=... MALLOC=libc redis-server (incremental, no distclean since .make-settings is left alone).
<!-- SECTION:NOTES:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
Moved the hybrid-BGSAVE snapshot support out of kvstore.c into src/kvsoverlay.c (per-dict save state, side table, drain/release, overlay enumeration helpers, the six overlay write paths). kvstore.c keeps dispatch and the dict hooks only; kvstore.h carries the shared internals under #ifdef KVSTORE_INTERNAL, defined by the two kvstore files. No logic change. Verified by C unit, unit/snapshot in both modes, ASan, TSan (one unexplained hang on the first run, see TASK-1.35) and the full hybrid suite.
<!-- SECTION:FINAL_SUMMARY:END -->
