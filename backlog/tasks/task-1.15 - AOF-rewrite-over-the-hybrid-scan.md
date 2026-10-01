---
id: TASK-1.15
title: AOF rewrite over the hybrid scan
status: In Progress
assignee:
  - '@Claude'
created_date: '2026-09-20 17:22'
updated_date: '2026-10-01 15:54'
labels:
  - bgsave
  - persistence
  - aof
milestone: m-1
dependencies:
  - TASK-1.9
documentation:
  - poc-hybrid-bgsave-spec.md
parent_task_id: TASK-1
priority: medium
ordinal: 16000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Spec §6 and M2: BGREWRITEAOF uses the same slot scan and channel, with the AOF encoders instead of RDB. With aof-use-rdb-preamble yes the base file is an RDB, so this is largely the M1 writer producing the base file inside the multi-part AOF layout; with the preamble off, the background thread's SNAP_NOFORK serializer and the child's SNAP_FORK serializer (working on the shipped kvobj copy) emit AOF command form (rewriteAppendOnlyFileRio per-type rewriters) into the same ~64 KB staging buffers / channel entries, and the child tracks the selected db to emit SELECT only on change.

Scope: hook rewriteAppendOnlyFileBackground into the snapshot.c orchestration; AOF-specific finish (manifest update, base/incr file handling; the incr file captures post-fork writes as today); interaction with the temp-dict path (snapshot content must be fork-instant); the AOF rewrite child's child-info reporting; per-type AOF rewriters must be safe to run off the main thread for SNAP_NOFORK keys. Foreground AOF rewrite on startup / aof-load paths are unchanged.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 BGREWRITEAOF with aof-use-rdb-preamble yes and no both produce a base file via the hybrid scan and the AOF loads to a keyspace identical to the fork-instant reference under the harness race workload
- [ ] #2 With the preamble off, SELECT is emitted only on db change despite interleaved producers, verified with a multi-db dataset
- [x] #3 Multi-part AOF manifest and base/incr handling behave as today, covered by the existing AOF tcl tests
- [ ] #4 Per-type AOF rewriters used by the background thread for SNAP_NOFORK keys are reviewed for main-thread-only state and the review is recorded in task notes
- [x] #5 Abort of an AOF rewrite mid-scan leaves the manifest and files consistent and the next rewrite succeeds
- [x] #6 Existing AOF and aof-multi-part test suites pass with the hybrid switch on
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
Plan of record (2026-10-01, after TASK-1.31; code read: aof.c rewriteAppendOnlyFileBackground 3210, rewriteAppendOnlyFile 3130, rewriteAppendOnlyFileRio 3026, backgroundRewriteDoneHandler 3866; snapshot.c snapSaveBackground/snapSaveCreate/snapSaveChildWrite).

Stage 1, preamble on (the default, most of the value): BGREWRITEAOF with bgsave-hybrid yes and aof-use-rdb-preamble yes runs the hybrid save machinery instead of the stock fork. In rewriteAppendOnlyFileBackground, after flushAppendOnlyFile(1) and openNewIncrAofForAppend() (the INCR file must start at the fork instant, which is the hybrid freeze+fork), call a snapshot.c entry (snapSaveBackgroundAof) that is snapSaveBackground with: CHILD_TYPE_AOF, the temp file name "temp-rewriteaof-bg-<childpid>.aof" (the done handler renames by server.child_pid, unchanged), rdbflags RDBFLAGS_AOF_PREAMBLE for startSaving/rdbSaveRioHeader, sendChildCowInfo(CHILD_INFO_TYPE_AOF_COW_SIZE) in the child, and no fsync-dir rename (the handler does the manifest dance). The arena stays DONTFORK (no snapArenaBeforeFork flip for this child type). The child's exit goes to backgroundRewriteDoneHandler as today; the parent-side hybrid completion (snapSaveChildDone) must run for CHILD_TYPE_AOF too (checkChildrenDone dispatch). Hard-mark abort or scan abort = rewrite failed, as a killed child is today. FLUSHALL/SWAPDB/FLUSHDB behave as for BGSAVE (TASK-1.24).

Stage 2, preamble off (AOF command form): the scan thread serializes SNAP_NOFORK keys with the per-type rewriters (rewriteStringObject, rewriteListObject (listpack), rewriteSetObject (intset/listpack), rewriteSortedSetObject (listpack), rewriteHashObject incl. listpackEx field TTLs) into the staging buffers; the child does SNAP_FORK keys (quicklist, HT, skiplist, stream, module, array) with the same rewriters on its copy, plus rewriteFunctions and the timestamp annotation first. Framing unchanged; snapChildSelect gains a mode that emits "SELECT <db>" on db change instead of SELECTDB/RESIZEDB/SLOT_INFO (headers already carry dbid). Expire: the per-key PEXPIREAT line as rewriteAppendOnlyFileRio writes it, from kvobjGetExpire. AC4 review: the rewriters only touch the value and the rio; rioWriteBulk* and the shared argv-less helpers have no main-thread state; rewriteStreamObject and rewriteModuleObject stay child-side by class; record the review in notes.

Tests: harness race workload with BGREWRITEAOF in both preamble modes, load the resulting AOF into a fresh server and compare the digest to the fork-instant reference; multi-db dataset with preamble off checks one SELECT per db change (count in the base file); abort mid-scan (child killed, hard mark) leaves the manifest consistent and the next rewrite succeeds; existing unit/aof, aof-multi-part and integration/aof* suites with --config bgsave-hybrid yes. Spec section 6 (AOF rewrite paragraph) and the blog's section 9 bullet ("AOF rewrite still takes the stock fork path") updated.
<!-- SECTION:PLAN:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
2026-10-01 stage 1 implemented (preamble on): snapSaveBackgroundAof() in snapshot.c (snapSave.child_type, CHILD_TYPE_AOF: no RDB stats, redisFork(CHILD_TYPE_AOF), the child names temp-rewriteaof-<pid>.aof and renames to temp-rewriteaof-bg-<pid>.aof, aof_rewrite_incremental_fsync, CHILD_INFO_TYPE_AOF_COW_SIZE, no dir fsync); rewriteAppendOnlyFileBackground calls it when aof-use-rdb-preamble and the hybrid are on and every module handles the hybrid, after flushAppendOnlyFile/openNewIncrAofForAppend, and does the parent bookkeeping; killAppendOnlyChild calls snapSaveAbort(); backgroundRewriteDoneHandler calls snapSaveChildDone() first. Tests: the BGREWRITEAOF arena test split into preamble-on (hybrid path, arena DONTFORK, snap_saves +1) and preamble-off (stock fallback, arena forkable); the mutual-exclusion fallback-child test uses preamble off; a harness test runs BGREWRITEAOF under random writes, DEBUG LOADAOF equals the live digest, a killed child fails the rewrite and the next one works. All pass alone; whole unit, stock mode and the AOF suites with the feature on pending. Stage 2 (AOF command form, preamble off) not started.

Stage 1 verified and committed (2026-10-01): unit/snapshot feature on and stock all pass; unit/aofrw, integration/aof and integration/aof-multi-part pass with --config bgsave-hybrid yes (AC3, AC6); the harness test covers the killed-child abort with the manifest intact and the next rewrite succeeding (AC5). AC1 is met for the preamble-on half; AC1's preamble-off half, AC2 and AC4 are stage 2 (AOF command form from the scan thread), left for a later session with the plan of record above.
<!-- SECTION:NOTES:END -->
