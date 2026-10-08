---
id: TASK-1.15
title: AOF rewrite over the hybrid scan
status: Done
assignee:
  - '@Claude'
created_date: '2026-09-20 17:22'
updated_date: '2026-10-08 13:46'
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
- [x] #1 BGREWRITEAOF with aof-use-rdb-preamble yes and no both produce a base file via the hybrid scan and the AOF loads to a keyspace identical to the fork-instant reference under the harness race workload
- [x] #2 With the preamble off, SELECT is emitted only on db change despite interleaved producers, verified with a multi-db dataset
- [x] #3 Multi-part AOF manifest and base/incr handling behave as today, covered by the existing AOF tcl tests
- [x] #4 Per-type AOF rewriters used by the background thread for SNAP_NOFORK keys are reviewed for main-thread-only state and the review is recorded in task notes
- [x] #5 Abort of an AOF rewrite mid-scan leaves the manifest and files consistent and the next rewrite succeeds
- [x] #6 Existing AOF and aof-multi-part test suites pass with the hybrid switch on
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
Plan of record (2026-10-01, after TASK-1.31; code read: aof.c rewriteAppendOnlyFileBackground 3210, rewriteAppendOnlyFile 3130, rewriteAppendOnlyFileRio 3026, backgroundRewriteDoneHandler 3866; snapshot.c snapSaveBackground/snapSaveCreate/snapSaveChildWrite).

Stage 1, preamble on (the default, most of the value): BGREWRITEAOF with bgsave-hybrid yes and aof-use-rdb-preamble yes runs the hybrid save machinery instead of the stock fork. In rewriteAppendOnlyFileBackground, after flushAppendOnlyFile(1) and openNewIncrAofForAppend() (the INCR file must start at the fork instant, which is the hybrid freeze+fork), call a snapshot.c entry (snapSaveBackgroundAof) that is snapSaveBackground with: CHILD_TYPE_AOF, the temp file name "temp-rewriteaof-bg-<childpid>.aof" (the done handler renames by server.child_pid, unchanged), rdbflags RDBFLAGS_AOF_PREAMBLE for startSaving/rdbSaveRioHeader, sendChildCowInfo(CHILD_INFO_TYPE_AOF_COW_SIZE) in the child, and no fsync-dir rename (the handler does the manifest dance). The arena stays DONTFORK (no snapArenaBeforeFork flip for this child type). The child's exit goes to backgroundRewriteDoneHandler as today; the parent-side hybrid completion (snapSaveChildDone) must run for CHILD_TYPE_AOF too (checkChildrenDone dispatch). Hard-mark abort or scan abort = rewrite failed, as a killed child is today. FLUSHALL/SWAPDB/FLUSHDB behave as for BGSAVE (TASK-1.24).

Stage 2, preamble off (AOF command form): the scan thread serializes SNAP_NOFORK keys with the per-type rewriters (rewriteStringObject, rewriteListObject (listpack), rewriteSetObject (intset/listpack), rewriteSortedSetObject (listpack), rewriteHashObject incl. listpackEx field TTLs) into the staging buffers; the child does SNAP_FORK keys (quicklist, HT, skiplist, stream, module, array) with the same rewriters on its copy, plus rewriteFunctions and the timestamp annotation first. Framing unchanged; snapChildSelect gains a mode that emits "SELECT <db>" on db change instead of SELECTDB/RESIZEDB/SLOT_INFO (headers already carry dbid). Expire: the per-key PEXPIREAT line as rewriteAppendOnlyFileRio writes it, from kvobjGetExpire. AC4 review: the rewriters only touch the value and the rio; rioWriteBulk* and the shared argv-less helpers have no main-thread state; rewriteStreamObject and rewriteModuleObject stay child-side by class; record the review in notes.

Tests: harness race workload with BGREWRITEAOF in both preamble modes, load the resulting AOF into a fresh server and compare the digest to the fork-instant reference; multi-db dataset with preamble off checks one SELECT per db change (count in the base file); abort mid-scan (child killed, hard mark) leaves the manifest consistent and the next rewrite succeeds; existing unit/aof, aof-multi-part and integration/aof* suites with --config bgsave-hybrid yes. Spec section 6 (AOF rewrite paragraph) and the blog's section 9 bullet ("AOF rewrite still takes the stock fork path") updated.

Stage 2 refinement (2026-10-08, code read: snapshot.c snapSaveEmitBucket 1672, scan emit 2255, snapChildSelect 2309, snapSaveChildWrite 2330-2545, snapSaveBackgroundAof 2854; aof.c rewriteObject 2984 (already non-static, declared in server.h), rewriteAppendOnlyFileRio 3032, rewriteAppendOnlyFileBackground 3210):
1. snapSave gains aof_cmd_form (CHILD_TYPE_AOF && !aof_use_rdb_preamble, set in snapSaveBackgroundAof). One helper snapSaveWriteRecord(sv, rio, kv, dbid) replaces the three rdbSaveKeyValuePair call sites (main-thread emit-ahead, scan thread, child KVOBJ): rewriteObject() in command form (per-type rewriter + PEXPIREAT + BLESS + module key meta, exactly rewriteAppendOnlyFileRio's per-key sequence), rdbSaveKeyValuePair otherwise. Framing, channels and queues untouched.
2. snapChildSelect in command form: on db change write '*2 SELECT <db>' (no RESIZEDB, no SLOT_INFO); cur_db tracking gives 'SELECT only on db change' across interleaved producers for free.
3. Child prologue/epilogue in command form: new aof.c rewriteAppendOnlyFileHeader(rio) = timestamp annotation + rewriteFunctions (factored out of rewriteAppendOnlyFileRio, which now calls it); no rdbSaveRioHeader/Footer. startSaving(RDBFLAGS_AOF_PREAMBLE) stays (stock rewriteAppendOnlyFile does the same in both modes). Child info label 'AOF rewrite'.
4. rewriteAppendOnlyFileBackground: the hybrid path no longer requires the preamble; the stock child remains only for a module that has not opted in.
5. Keys in slots being trimmed by ASM: stock's command-form rewriter skips them; the hybrid keeps the fork instant (as its RDB does since TASK-1.37). Documented in the spec.
6. Tests (unit/snapshot): the harness AOF test runs for preamble yes and no; new AC2 test: 3 dbs, preamble off, writes across dbs during the rewrite, then parse the base .aof (from the manifest) and assert every SELECT changes the db and the AOF reloads to the live digest; the two fallback-section tests that used 'preamble off' to obtain a stock child are rewritten: 'no preamble' now asserts the hybrid path (arena DONTFORK, snap_saves+1, reload), and the mutual-exclusion test gets its stock child from the fork test module (fork.create <code> <usleep_us>).
7. AC4 review recorded in notes; spec section 6 paragraph and table row, blog section 9 bullet updated.
Verification: unit/snapshot feature on and stock; unit/aofrw, integration/aof, integration/aof-multi-part with --config bgsave-hybrid yes; the TSan tree (already rebuilt for TASK-1.35) reruns unit/snapshot after.
<!-- SECTION:PLAN:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
2026-10-01 stage 1 implemented (preamble on): snapSaveBackgroundAof() in snapshot.c (snapSave.child_type, CHILD_TYPE_AOF: no RDB stats, redisFork(CHILD_TYPE_AOF), the child names temp-rewriteaof-<pid>.aof and renames to temp-rewriteaof-bg-<pid>.aof, aof_rewrite_incremental_fsync, CHILD_INFO_TYPE_AOF_COW_SIZE, no dir fsync); rewriteAppendOnlyFileBackground calls it when aof-use-rdb-preamble and the hybrid are on and every module handles the hybrid, after flushAppendOnlyFile/openNewIncrAofForAppend, and does the parent bookkeeping; killAppendOnlyChild calls snapSaveAbort(); backgroundRewriteDoneHandler calls snapSaveChildDone() first. Tests: the BGREWRITEAOF arena test split into preamble-on (hybrid path, arena DONTFORK, snap_saves +1) and preamble-off (stock fallback, arena forkable); the mutual-exclusion fallback-child test uses preamble off; a harness test runs BGREWRITEAOF under random writes, DEBUG LOADAOF equals the live digest, a killed child fails the rewrite and the next one works. All pass alone; whole unit, stock mode and the AOF suites with the feature on pending. Stage 2 (AOF command form, preamble off) not started.

Stage 1 verified and committed (2026-10-01): unit/snapshot feature on and stock all pass; unit/aofrw, integration/aof and integration/aof-multi-part pass with --config bgsave-hybrid yes (AC3, AC6); the harness test covers the killed-child abort with the manifest intact and the next rewrite succeeding (AC5). AC1 is met for the preamble-on half; AC1's preamble-off half, AC2 and AC4 are stage 2 (AOF command form from the scan thread), left for a later session with the plan of record above.

Bug found by the full feature-on suite (unit/wait 'WAITAOF local if AOFRW was postponed'): killAppendOnlyChild (appendonly no, postponed rewrite, shutdown) waits for the child and resets the child state without any done handler, so the hybrid rewrite's save object stayed behind and the next BGSAVE asserted snap_save == NULL in snapSaveCreate. Fixed by calling snapSaveChildDone after the waitpid there; regression test 'BGREWRITEAOF through the hybrid killed by appendonly no'. unit/wait, the three AOF suites and unit/snapshot pass with the feature on; the full suite and TSan are rerunning on the fixed head.

FLUSHALL semantic (2026-10-01 evening): emptyData(-1) aborted every hybrid save and killed the child; for an AOF rewrite that is a behavior change from stock (which keeps its AOF child; the FLUSHALL lands in the INCR file) and each such kill counted as a failed rewrite, feeding aofRewriteLimited (three failures => retries delayed by minutes), which is how the full suite's aof-multi-part 'temporary INCR AOF' test came to wait 50 s for a retry. Now emptyData skips the abort when snapSaveIsAofRewrite(): every db detaches to the running rewrite as FLUSHDB does and the rewrite completes with the fork-instant data. Test added. The full suite and the sanitizers are rerunning on the fixed head.

2026-10-08 stage 2 implemented (AOF command form, preamble off): snapSave.aof_cmd_form; snapSaveWriteRecord() is the one record writer for the three producers (main-thread emit-ahead, scan thread, child KVOBJ) and switches rdbSaveKeyValuePair -> rewriteObject; snapChildSelect writes SELECT on db change only in command form; the child writes rewriteAppendOnlyFileHeader (new in aof.c: timestamp annotation + functions, factored out of rewriteAppendOnlyFileRio) instead of the RDB header and no footer; rewriteAppendOnlyFileBackground hands every rewrite to the hybrid (the stock child only for a module without the opt-in). Tests: the harness AOF test runs for preamble yes and no (module keys only with the preamble: the datatype test module has no aof_rewrite callback, so a command-form rewrite of its keys fails in stock too); new multi-db test parses the base .aof from the manifest and asserts every SELECT changes the db (AC2); the two fallback-section tests that used preamble-off as a stock child now assert the hybrid path and take their stock child from the fork test module. Spec section 6 and table, blog section 9 bullet updated.

Bug found by the preamble-off harness run (AC4 in action): rewriteHashObject decided its field-TTL branch with hashTypeGetMinExpire(o, 0), which reads the ExpireMeta registered in the db's ebuckets. The overlay clone of a listpackEx hash written inside the window takes over that registration and trashes the original's meta (snapshot.c snapKvobjClone, estoreRemove), so the scan thread serialized such fork-instant originals through the non-TTL branch and the base file lost their field TTLs (2 of 40 HFE hashes per run, the ones written inside the window). Fixed by deciding from the content: hashTypeGetMinExpire(o, 1) (listpackExGetMinExpire / the HT's hfe ebuckets). The RDB form never had this because rdbSaveObject reads TTLs from the listpack entries.

AC4 review, rewriters the scan thread runs on SNAP_NOFORK keys (string: int/embstr/raw; list listpack; set intset/listpack; zset listpack; hash listpack/listpackEx) and the shared helpers: rioWriteBulkCount/String/LongLong/Double (rio.c, pure formatting into the rio), rioWriteBulkObject (reads obj->ptr), rewriteListObject (listTypeInitIterator over the listpack, read-only), rewriteSetObject (setTypeInitIterator, read-only), rewriteSortedSetObject listpack branch (zzl* readers), rewriteHashObject (hashTypeInitIterator/hashTypeNext(skipExpired=0), hashTypeCurrentObject; TTL from hi.expire_time, i.e. the listpack entry, and the branch decision now from content, see above), blessRewrite (reads the kvobj flag), the PEXPIREAT line from kvobjGetExpire (the header, fork-instant). None touches server.* state other than reading configuration constants; none allocates beyond the rio buffer; none calls module code. Child-only by class: rewriteStreamObject, rewriteModuleObject (module aof_rewrite callback), keyMetaOnAof (module key metadata; keys with module metadata are forced SNAP_FORK in snapSaveEmitBucket and the scan path), rewriteHashObject's HT/HFE-dict and template (RESTORE payload via createRawDumpPayload) branches, quicklist/skiplist/array. rewriteFunctions and the timestamp annotation run in the child before any key (rewriteAppendOnlyFileHeader).

Stage 2 verified (2026-10-08): unit/snapshot 79 ok 0 failures (both preamble variants of the harness AOF test, the multi-db SELECT test, the rewritten fallback tests); unit/aofrw (25), integration/aof (63), integration/aof-multi-part (41) and integration/aof-race with --config bgsave-hybrid yes all clean. Standalone reproduction of the field-TTL bug and its fix in the scratchpad (aofdbg.sh). The TSan tree runs TASK-1.35's loop on the pre-stage-2 sources; it reruns unit/snapshot on this head once that loop ends.
<!-- SECTION:NOTES:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
BGREWRITEAOF runs through the hybrid save in both base-file formats.

Stage 1 (2026-10-01, RDB preamble): rewriteAppendOnlyFileBackground hands the rewrite to snapSaveBackgroundAof after flushing the AOF and opening the INCR file, so the freeze and fork are the instant the INCR history starts; the child is CHILD_TYPE_AOF, writes the temp file backgroundRewriteDoneHandler expects, and the stock done handler does the manifest work after snapSaveChildDone released the save. killAppendOnlyChild aborts the scan; FLUSHALL/DEBUG RELOAD detach the dbs to the rewrite instead of killing it (stock keeps its AOF child too).

Stage 2 (2026-10-08, command form): snapSave.aof_cmd_form switches the one record writer shared by the three producers (main-thread emit-ahead, scan thread, child KVOBJ) from rdbSaveKeyValuePair to rewriteObject; the child writes the AOF prologue (rewriteAppendOnlyFileHeader, factored out of rewriteAppendOnlyFileRio) instead of the RDB header, no footer, and emits SELECT on a db change only whatever order the producers' records arrive in. Every rewrite now takes the hybrid path; the stock child remains only for a module without the hybrid opt-in. Keys in slots being trimmed by ASM keep the fork instant, as the hybrid RDB does.

Bug fixed on the way: rewriteHashObject decided its field-TTL branch from the ExpireMeta registered in the db, which the overlay clone of a hash written during the save takes over (the original's meta is trashed); the scan thread then wrote such hashes without field TTLs. It now decides from the hash content (hashTypeGetMinExpire accurate).

Tests: unit/snapshot harness AOF test in both preamble modes (random writes during the rewrite, DEBUG LOADAOF equals the live digest, a killed child fails cleanly and the next rewrite works), a multi-db command-form test that parses the base .aof and asserts every SELECT changes the db, FLUSHALL-during-rewrite, appendonly-no kill, mutual exclusion with a module-fork stock child. unit/snapshot 79 ok; unit/aofrw, integration/aof, integration/aof-multi-part, integration/aof-race clean with --config bgsave-hybrid yes. Spec section 6 and table, blog section 9 updated.

Follow-up: the TSan tree should rerun unit/snapshot on this head once TASK-1.35's loop on the previous head ends (the loop cannot share the tree with a rebuild).
<!-- SECTION:FINAL_SUMMARY:END -->
