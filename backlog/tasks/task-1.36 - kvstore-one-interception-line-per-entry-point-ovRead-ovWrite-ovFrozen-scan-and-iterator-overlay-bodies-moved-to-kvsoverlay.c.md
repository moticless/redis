---
id: TASK-1.36
title: >-
  kvstore: one interception line per entry point (ovRead/ovWrite/ovFrozen), scan
  and iterator overlay bodies moved to kvsoverlay.c
status: Done
assignee: []
created_date: '2026-10-06 05:58'
updated_date: '2026-10-06 07:18'
labels:
  - refactor
  - kvstore
  - landing
dependencies:
  - TASK-1.34
modified_files:
  - src/kvstore.c
  - src/kvstore.h
  - src/kvsoverlay.c
parent_task_id: TASK-1
priority: medium
ordinal: 32000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Follow-up of TASK-1.34 (user, 2026-10-05: "define api wrapper (overlay) that might simplify to separate and track logic"). Decision: no indirection table and no second API (an indirect call per dict access, or renaming upstream functions / sixty call sites); instead make the existing wrapper pattern uniform and auditable. Three inline helpers in the KVSTORE_INTERNAL section replace the three ad-hoc shapes (ovGetR+ovActive, ovGetW, hand-written branches): ovRead (reads and enumeration, the overlay only while its side table is non-empty), ovWrite (structural writes, attach through the guard), ovFrozen (layout decisions). The interleaved overlay bodies of kvstoreScan, kvstoreIteratorNext and kvstoreDictIteratorNext move to kvsoverlay.c as ovScan and ovIteratorNext, so every intercepted public function in kvstore.c starts with exactly one grep-able line and the stock body follows. No hot-path change: the helpers compile to the same load and branch.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 Every public kvstore entry point that touches dict content starts with one of ovRead/ovWrite/ovFrozen (grep lists them); ovGetR/ovGetW macros gone
- [x] #2 kvstoreScan, kvstoreIteratorNext, kvstoreDictIteratorNext contain no overlay branching beyond the one dispatch line; ovScan and ovIteratorNext live in kvsoverlay.c
- [x] #3 C unit kvstore + snapshot, unit/snapshot hybrid and stock green; TSan/ASan unit/snapshot clean
<!-- AC:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
Done as planned: ovRead/ovWrite/ovFrozen inline in the KVSTORE_INTERNAL section (ovGetR/ovGetW macros removed), ovScan and ovIteratorNext in kvsoverlay.c (ovScanCb and struct ovScanCtx static there again), kvstoreScan / kvstoreIteratorNext / kvstoreDictIteratorNext reduced to the dispatch line plus the stock body. Interception points in kvstore.c after the change: ovRead x9 (scan, iterator, dict size, dict iterator, random x2, some keys, find), ovFrozen x1 (expand), ovWrite x6 (find-link, set-at-link, add-raw, two-phase unlink find/free, delete), plus ovPeek in freeDictIfNeeded (no guard side effects on that path). Verified 2026-10-06: build warning-free, C unit kvstore + snapshot (184), unit/snapshot hybrid and stock green. TSan/ASan (AC3's second half) run together with TASK-1.37, which changes the same paths next.
<!-- SECTION:NOTES:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
Uniform interception in kvstore.c: ovRead/ovWrite/ovFrozen inline helpers, one grep-able line per public entry point; the overlay bodies of scan and the two iterators moved to kvsoverlay.c (ovScan, ovIteratorNext). No hot-path change. Verified with TASK-1.37 on top: C unit, unit/snapshot both modes, TSan and ASan clean, full hybrid suite.
<!-- SECTION:FINAL_SUMMARY:END -->
