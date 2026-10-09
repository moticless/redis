---
id: TASK-1.40
title: >-
  Hybrid BGSAVE review cleanup: dead code, stale comments and config docs
  describing the replaced design
status: In Progress
assignee:
  - '@Claude'
created_date: '2026-10-09 22:22'
labels:
  - hybrid-bgsave
  - cleanup
  - docs
milestone: m-1
dependencies: []
references:
  - >-
    src/snapshot.c file header (lines 1-21) and the scan banner (655-674) and
    orchestration banner (1446-1460)
  - >-
    src/snapshot.h stale temp-dict/merge comments (154, 160, 173-174, 267, 324),
    zfree claim (64-69), 84, 118-119, 317-318, 343-346; duplicate
    snapScanSlotState (274, 299)
  - redis.conf bgsave-hybrid and bgsave-hybrid-max-retained-bytes entries
  - >-
    src/config.c:2843, src/keymeta.c:723-728, src/module.c:4162-4168,
    src/rdb.c:1744-1747 (misattached or duplicated comments)
documentation:
  - poc-hybrid-bgsave-spec.md
parent_task_id: TASK-1
priority: high
ordinal: 36000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
A four-way review of the hybrid BGSAVE feature (2026-10-09, HEAD vs 0d6266f2d) found text and code left over from the replaced milestone-1 design (per-slot temp dict and main-thread merge, slot-level granularity, class-aware frees as the only correct free path, startup-only switch). It misleads a reader before any structural change can help. Outcome wanted: every comment, log line and config entry describes the current design (bucket window, write funnel with in-place / side-table / emit-ahead, two channels, runtime switch, overlay drain), dead symbols are gone, and process jargon (task IDs, run numbers, spec section numbers, stage/milestone names) is out of src and redis.conf. No behavior change; no file moves (the user decided against splitting or renaming files).

Candidates identified (verify each before removing): snapSaveIsAofRewrite (no callers since TASK-1.39), snapSdsFree/snapLpFree/snapIntsetFree/snapListpackExFree, kvstoreOverlayStats, dictEncodeKeyAsEntry, the duplicate snapScanSlotState declaration, the dangling ovTombstoneInPlace reference (snapshot.c ~523). Stale text: snapshot.c header says the file "currently provides the arena"; the scan banner's immutability contract and M1 granularity note; the orchestration banner's 4-step sequence (freeze is on demand, two pipes); "merge" in log lines and comments where the overlay is drained; "temp dict" in snapshot.h, snapshot.c, kvsoverlay.c (~523); snapshot.h claims about zfree and about which functions panic in the child; the module-option comment limited to the RDB preamble. redis.conf: bgsave-hybrid is a runtime switch refused while a child runs, with the module fallback; retained bytes are held only while a bucket is inside the window and queued bytes count toward the hard mark. Misattached doc comments in config.c, keymeta.c, module.c; duplicated comment in rdb.c. Blog section 9 still says the window was not extended to per-slot dicts.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 grep for 'temp dict', 'merge', 'M1', 'TASK-', 'run [0-9]', 'spec 11', 'stage [12]', 'Stage B' in src/snapshot.c, src/snapshot.h, src/kvsoverlay.c, src/kvstore.c, src/kvstore.h, src/dict.c, src/dict.h and the core hook sites returns only text that is accurate for the current design, and no task IDs or run numbers remain in src or redis.conf
- [ ] #2 Dead symbols listed in the description are removed where verification confirms no caller, the build has no new warnings, and unit/snapshot plus redis-server test snapshot pass
- [ ] #3 The file header of snapshot.c and the two banners give an accurate map of the current design (classes, on-demand freeze and fork, scan thread with bucket window, funnel outcomes, two channels, child ordering, completion and abort) and point to the design doc
- [ ] #4 redis.conf entries for bgsave-hybrid and bgsave-hybrid-max-retained-bytes match the implementation (runtime switch, refusal while a child runs, module fallback, retention and queued-bytes accounting)
- [ ] #5 Misattached or duplicated doc comments in config.c, keymeta.c, module.c and rdb.c are repaired; the module API doc generator still finds RM_KeyExists's comment
- [ ] #6 Blog section 9 and spec sections that still describe the temp-dict design are corrected or marked superseded (full spec rewrite is the design-doc task)
<!-- AC:END -->
