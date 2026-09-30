---
id: TASK-1.25
title: >-
  Module contract for hybrid BGSAVE: opt-in
  REDISMODULE_OPTIONS_HANDLE_HYBRID_BGSAVE with fork fallback, child-side
  keyspace API guards
status: To Do
assignee: []
created_date: '2026-09-25 22:51'
updated_date: '2026-09-30 03:50'
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
- [ ] #1 With a loaded module lacking the bit, BGSAVE under bgsave-hybrid yes runs the stock fork path (INFO: snap_saves unchanged, rdb_last_cow_size > 0 on a dirty dataset), a warning was logged at module load, and INFO names the module
- [ ] #2 With every module opted in, BGSAVE runs the hybrid path (snap_saves increments)
- [ ] #3 A module calling RM_OpenKey/RM_Call inside rdb_save in the hybrid child gets NULL/error and a log line; the child does not crash; the save result is reported honestly
- [ ] #4 RM_SetLRU/RM_SetLFU return REDISMODULE_ERR on a handle opened without REDISMODULE_WRITE
- [ ] #5 Full tests/modules suite passes with --config bgsave-hybrid yes and the test modules opted in (hybrid path exercised, not the fallback)
- [ ] #6 Spec section 11.z.4 and redismodule.h documentation match the implementation
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
<!-- SECTION:PLAN:END -->
