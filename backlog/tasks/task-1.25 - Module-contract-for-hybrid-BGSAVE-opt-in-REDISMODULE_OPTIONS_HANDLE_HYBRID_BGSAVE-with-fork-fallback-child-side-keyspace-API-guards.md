---
id: TASK-1.25
title: >-
  Module contract for hybrid BGSAVE: opt-in
  REDISMODULE_OPTIONS_HANDLE_HYBRID_BGSAVE with fork fallback, child-side
  keyspace API guards
status: Done
assignee:
  - Claude
created_date: '2026-09-25 22:51'
updated_date: '2026-09-30 12:27'
labels:
  - hybrid-bgsave
  - modules
dependencies: []
references:
  - poc-hybrid-bgsave-spec.md#11.z.4
  - ~/git/valkey/src/valkeymodule.h VALKEYMODULE_OPTIONS_HANDLE_FORKLESS
  - ~/git/valkey/src/module.c moduleAllModulesHandleForkless
  - ~/git/valkey/src/rdb.c resolveBgsaveType
parent_task_id: TASK-1
priority: medium
ordinal: 19000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
The hybrid child has no keyspace (dict tables, kvobj headers and small values are MADV_DONTFORK), so a module save callback (rdb_save, aux_save, key-metadata save, persistence/fork-child event handlers) that touches a key faults instead of reading a stale copy as in the stock child: the child dies with SIGSEGV and persistence silently fails. Main-thread module semantics are unchanged (all mutating APIs already require a WRITE handle, which goes through the write funnel; module threads mutate only under the GIL, which closes the write section).

Define and enforce the contract written in poc-hybrid-bgsave-spec.md section 11.z.4, following Valkey's VALKEYMODULE_OPTIONS_HANDLE_FORKLESS (opt-in bit, moduleAllModulesHandleForkless(), resolveBgsaveType() falls back to fork with a warning):

- New option bit REDISMODULE_OPTIONS_HANDLE_HYBRID_BGSAVE (1<<4), _REDISMODULE_OPTIONS_FLAGS_NEXT -> 1<<5, documented in redismodule.h and RM_SetModuleOptions doc comment.
- moduleAllModulesHandleHybridBgsave(): every loaded module must set the bit (server-event handlers run in the child too). snapSaveBackground falls back to rdbSaveBackground (stock fork, arena flipped forkable by the TASK-1.22 mechanism) when it returns 0. LL_WARNING once per offending module at load time; INFO persistence field naming the blocking module (snap_hybrid_blocked_by).
- Child-side guards: in a hybrid save child, RM_OpenKey, RM_KeyExists, RM_Scan, RM_ScanKey, RM_DbSize, RM_RandomKey, RM_GetExpire and RM_Call return NULL / REDISMODULE_ERR (RM_Call: error reply) and log once, instead of dereferencing arena memory.
- RM_SetLRU / RM_SetLFU: require REDISMODULE_WRITE on the handle (today they write kv->lru on a READ handle while the scan thread may read it for the IDLE/FREQ opcode).
- Test modules that register data types, key metadata or aux callbacks (tests/modules/datatype.c, datatype2.c, testrdb.c, test_keymeta.c, keymeta_notify.c, mallocsize.c, test_lazyfree.c, blockonkeys.c and any other loaded by the suite) set the bit so the feature-on suite keeps exercising the hybrid path. One new test module violates rule 1 (RM_OpenKey inside rdb_save) to cover both the fallback (bit missing) and the guard (bit set).
- MODULE LOAD during a hybrid save stays allowed (Valkey refuses it; our OnLoad runs on the main thread through the funnel).
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 With a loaded module lacking the bit, BGSAVE under bgsave-hybrid yes runs the stock fork path (INFO: snap_saves unchanged, rdb_last_cow_size > 0 on a dirty dataset), a warning was logged at module load, and INFO names the module
- [x] #2 With every module opted in, BGSAVE runs the hybrid path (snap_saves increments)
- [x] #3 A module calling RM_OpenKey/RM_Call inside rdb_save in the hybrid child gets NULL/error and a log line; the child does not crash; the save result is reported honestly
- [x] #4 Full tests/modules suite passes with --config bgsave-hybrid yes and the test modules opted in (hybrid path exercised, not the fallback)
- [x] #5 Spec section 11.z.4 and redismodule.h documentation match the implementation
- [x] #6 RM_SetLRU/RM_SetLFU on a handle opened without REDISMODULE_WRITE stay allowed (stock misc.c uses READ|NOTOUCH) but hand an owed key to the write funnel first, so the scan thread never reads a header the module is writing; the stock misc lru/lfu tests pass in both modes
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
DRAFT for approval (written 2026-09-30 while the user is OOO; not started).

1. redismodule.h: REDISMODULE_OPTIONS_HANDLE_HYBRID_BGSAVE (1<<4), _REDISMODULE_OPTIONS_FLAGS_NEXT -> (1<<5); doc comment on RM_SetModuleOptions stating the contract (save-side callbacks never touch the keyspace: no RM_OpenKey/KeyExists/Scan/ScanKey/Call/DbSize/RandomKey/GetExpire on any key; value passed in, module memory and the IO context only).
2. module.c: moduleAllModulesHandleHybridBgsave() (every loaded module has the bit; a module with no data types, no aux callbacks and no persistence event handler counts as compliant); LL_WARNING once at load for a module that registers a type/aux/persistence handler without the bit; the name of the first blocking module kept for INFO.
3. Fallback: snapSaveBackground and snapSaveToSlavesSockets check moduleAllModulesHandleHybridBgsave() and route to rdbSaveBackground / rdbSaveToSlavesSockets (stock child, arena flipped forkable by the TASK-1.22 mechanism) when it returns 0; INFO persistence snap_hybrid_blocked_by:<module> (empty when none), snap_saves unchanged on the fallback.
4. Child-side guards: in a hybrid child (a new server flag set in snapSaveChild), RM_OpenKey returns NULL, RM_KeyExists/RM_DbSize/RM_GetExpire return 0 / REDISMODULE_NO_EXPIRE, RM_Scan/RM_ScanKey return 0, RM_RandomKey returns NULL, RM_Call returns an error reply; each logs once ('module X touched the keyspace in the hybrid save child'). The stock child keeps today's behaviour.
5. RM_SetLRU/RM_SetLFU require REDISMODULE_WRITE on the handle (return REDISMODULE_ERR otherwise); document.
6. Test modules that register types, key metadata or aux callbacks set the bit (tests/modules/datatype.c, datatype2.c, testrdb.c, test_keymeta.c, keymeta_notify.c, mallocsize.c, test_lazyfree.c, blockonkeys.c and any other the suite loads with the feature on); one new module violates rule 1 (RM_OpenKey inside rdb_save) to cover the fallback (bit missing: stock path, warning, INFO names it) and the guard (bit set: NULL, log line, child survives, save result honest).
7. Tests (tests/unit/moduleapi/*.tcl and unit/snapshot): AC1-AC4 of the task; the full tests/modules suite with --config bgsave-hybrid yes exercises the hybrid path (snap_saves increments) rather than the fallback.
8. Spec 11.z.4 and redismodule.h match; blog section 5's 'one new rule' paragraph names the option bit.

Risks: modules bundled in Redis 8 (JSON, Search, TimeSeries, Bloom) do not set the bit until they are rebuilt; until then the hybrid save falls back to the stock fork on any server loading them, which is the safe default but means the feature is off for those deployments. Decide whether the bit should default to 'compliant' for modules without save callbacks (proposed yes).

Approved by the user 2026-09-30 ('go'); the DRAFT above is the plan of record. Decision taken: a module that registers no data type, no aux callbacks and no PERSISTENCE/FORK_CHILD server-event handler counts as compliant without the bit.
<!-- SECTION:PLAN:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
Progress 2026-09-30 (implementation done, verification in progress): all code from the plan is in place and built. Findings while testing: (1) the test module's first version called RM_OpenKey through the RM_GetContextFromIO context, which has no client, and the STOCK fork child segfaulted (a pre-existing stock behaviour, not ours); the module now uses a detached thread-safe context and selects the IO's db (the harness runs on db 9). (2) In datatype2.c and test_keymeta.c the SetModuleOptions line had landed inside the failing-Init branch after the return, so those two modules never opted in and every harness test that loads test_keymeta fell back to the stock child (9 [err] in unit/snapshot). Fixed, unit/snapshot passes with --config bgsave-hybrid yes (153 s). (3) The moduleapi suite with the feature on failed only 'test module lru api' / 'test module lfu api' (misc.tcl): the stock misc.c module sets idle time through a READ|NOTOUCH handle, which the WRITE requirement of plan item 5 / AC4 refuses. Requiring WRITE is a stock API break, so the design changed: RM_SetLRU/RM_SetLFU stay allowed on a READ handle and hand an owed key to the write funnel first (moduleKeyPrepareLruWrite: snapKvobjIsOwed + a write lookup with NOTOUCH|NONOTIFY|NOSTATS|NOEXPIRE that emits or clones the bucket, and refreshes key->kv since a clone replaces the frozen original). misc passes in both modes; the snapshot module test now exercises SetLRU/SetLFU on an owed key during a save. Spec 11.z.4 item 3 and the enforcement paragraph updated accordingly. AC4 will be reworded. Remaining: TSan run of unit/snapshot (running), stock-mode unit/snapshot, commit.
<!-- SECTION:NOTES:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
Module contract for hybrid BGSAVE, implemented as planned with one design change (SetLRU/SetLFU, see below).

**Opt-in bit.** `REDISMODULE_OPTIONS_HANDLE_HYBRID_BGSAVE` (1<<4, `_REDISMODULE_OPTIONS_FLAGS_NEXT` -> 1<<5) with the contract documented in redismodule.h and in the RM_SetModuleOptions doc comment. A module is relevant when it registers a data type, a key-metadata class (`keyMetaClassModuleHasAny`) or a PERSISTENCE / FORK_CHILD server-event handler (`moduleHasSaveCallbacks`); modules with none of those count as compliant without the bit (decision recorded in the plan).

**Fallback.** `moduleAllModulesHandleHybridBgsave()` / `moduleHybridBgsaveBlockedBy()` in module.c. `rdbSaveBackground` and `startBgsaveForReplication` route to the stock fork child when a relevant module lacks the bit (the TASK-1.22 arena flip makes it free); `snapHybridFallbackLog` logs at save time when the blocking module changes, `MODULE LOAD` logs an LL_WARNING when bgsave-hybrid is on, and INFO persistence gains `snap_hybrid_blocked_by:<name>` (empty when none). MODULE LOAD during a save stays allowed.

**Child-side guards** (`snapInHybridChild()`, set in `snapSaveChild`): RM_OpenKey NULL, RM_KeyExists/RM_DbSize 0, RM_RandomKey NULL, RM_Scan/RM_ScanKey 0 with EPERM, RM_GetExpire NO_EXPIRE, RM_Call NULL with EPERM or an error reply under the E flag; each logged once per API per child through `moduleHybridChildGuard`. Verified by the violating module: the hybrid save completes ("OpenKey=NULL Call=NULL" logged, status ok, reload equal).

**RM_SetLRU / RM_SetLFU (changed from the plan).** Requiring a WRITE handle broke the stock `misc.c` module tests, which set idle time through READ|NOTOUCH (so the open does not touch the LRU first); that is also what a real module would do. Instead, `moduleKeyPrepareLruWrite` keeps READ handles allowed and hands an owed key to the write funnel first: `snapKvobjIsOwed` then a write lookup with NOTOUCH|NONOTIFY|NOSTATS|NOEXPIRE, which emits or clones the bucket, and the handle's kvobj pointer is refreshed since a clone replaces the frozen original. AC #6 reworded accordingly; spec 11.z.4 item 3 and the enforcement paragraph describe it.

**Modules.** Bundled vector-sets declares the bit (it would otherwise block every hybrid save on a stock build). Test modules with types / metadata / persistence hooks declare it: datatype, datatype2, testrdb, test_keymeta, keymeta_notify, mallocsize, test_lazyfree, blockonkeys, hooks, defragtest, postnotifications, postnotifications_perkey_metadata, atomicslotmigration. New `tests/modules/hybridviolate.c` (type hybridvio; rdb_save opens another key and runs DBSIZE through a detached thread-safe context on the IO's db; hv.set/hv.get/hv.optin/hv.setlru).

**Tests.** unit/snapshot block `snapshot modules` (5 tests): bundled modules do not block; a module without the bit is named in INFO, warned at load, and the save takes the stock child with the probes succeeding there; after hv.optin 1 the hybrid path runs and the child refuses the probes; withdrawing the opt-in blocks again; SetLRU/SetLFU on READ and WRITE handles during a save on an owed key, under LRU and LFU policies, save ok and reload equal.

**Verification.** unit/snapshot with `--config bgsave-hybrid yes` (all pass, 153 s) and in stock mode (155 s); full `runtest-moduleapi --config bgsave-hybrid yes` (48 units, all pass after the SetLRU change); TSan build (`.scratch/tsan`, `setarch -R`) of unit/snapshot with the feature on: 51 tests, no ThreadSanitizer report. Not measured: AC #1's `rdb_last_cow_size > 0` on the fallback (the test asserts the stock path through snap_saves, the fallback log line and the module's own log from the stock child instead).

**Pitfalls hit (memory updated).** Stock `RM_GetContextFromIO` context has no client: RM_OpenKey through it segfaults in the STOCK child too (pre-existing). Two insertions of SetModuleOptions had landed after `return REDISMODULE_ERR;` inside the failing-Init branch (datatype2, test_keymeta), silently disabling the opt-in and sending the whole harness block to the stock child. TSan tree: rsync'd `.make-settings` masked `-fsanitize=thread`, and a stale `src/vset.o` (vector-sets compiled via vpath) without the opt-in made every hybrid save in the TSan run fall back.

**Follow-ups.** Bundled JSON/Search/TimeSeries/Bloom need the bit in their builds (audit in spec 11.z.4 says JSON and Search comply as they are). Side finding in the spec: a crash inside the hybrid child truncates the crash report because `genRedisInfoString` reads the unmapped keyspace; skip the Keyspace section in a hybrid child.
<!-- SECTION:FINAL_SUMMARY:END -->
