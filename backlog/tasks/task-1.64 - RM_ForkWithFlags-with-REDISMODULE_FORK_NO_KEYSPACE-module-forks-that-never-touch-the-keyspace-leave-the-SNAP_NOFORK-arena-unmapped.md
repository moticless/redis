---
id: TASK-1.64
title: >-
  RM_ForkWithFlags with REDISMODULE_FORK_NO_KEYSPACE: module forks that never
  touch the keyspace leave the SNAP_NOFORK arena unmapped
status: Done
assignee: []
created_date: '2026-10-23 09:05'
updated_date: '2026-10-23 09:16'
labels:
  - hybrid-snapshot
  - modules
  - arena
dependencies: []
references:
  - 'src/module.c RM_Fork, moduleHybridChildGuard'
  - >-
    src/snapshot.c snapArenaBeforeFork, snapChildReserveArenaRange,
    snapSaveChild
  - src/server.c redisFork
  - 'https://github.com/RediSearch/RediSearch/blob/master/src/fork_gc/fork_gc.c'
documentation:
  - docs/hybrid-snapshot.md
modified_files:
  - src/snapshot.c
  - src/snapshot.h
  - src/module.c
  - src/redismodule.h
  - src/debug.c
  - tests/modules/fork.c
  - tests/unit/moduleapi/fork.tcl
  - tests/unit/snapshot.tcl
  - docs/hybrid-snapshot.md
parent_task_id: TASK-1
priority: medium
ordinal: 60000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Every module fork (RM_Fork) is a classic child today: redisFork flips the SNAP_NOFORK arena MADV_DOFORK for it, so the fork copies the arena's page tables and the parent pays copy-on-write on headers, small values and dict tables while it runs. Some module children never touch the keyspace. RediSearch's fork GC (src/fork_gc/fork_gc.c, latest master 724e39f) only walks module-owned inverted indexes and pipes deltas back, and it runs periodically on every RAM index. For such children the flip is pure cost.

Outcome: a module can declare, per fork call, that its child does not need the keyspace. That child forks the way the hybrid save child does: the arena stays DONTFORK, the child reserves the arena's address range, the keyspace module APIs are refused there with the existing one-time log (no fault), and the crash report skips the INFO sections that read the keyspace.

Decisions taken with the user (2026-10-23):
- A new API RM_ForkWithFlags(ctx, flags, cb, user_data) with REDISMODULE_FORK_NO_KEYSPACE. RM_Fork is unchanged and equals flags 0.
- Not an extension of REDISMODULE_OPTIONS_HANDLE_HYBRID_SNAPSHOT: that option covers save callbacks only. Not a module-wide option either: RM_Fork takes no ctx, so the server cannot attribute a fork to a module. The ctx argument is what makes attribution possible.
- The flag applies whether or not hybrid-snapshot is on. When the arena is forkable or absent, the child still gets the refusals, so a module sees the same contract everywhere.

Contract for the module: the child does not call the keyspace APIs, and it does not dereference or free pointers it kept into Redis key memory (RM_StringDMA results, RedisModuleKey handles, key names shared with a kvobj). Breaking the second rule crashes only the child.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 RM_ForkWithFlags(ctx, REDISMODULE_FORK_NO_KEYSPACE, cb, ud) forks a child that runs module code and exits with its code; the done handler gets the exit code
- [x] #2 With hybrid-snapshot on and the arena available, INFO snap_nofork_arena_forkable stays 0 for the whole life of a NO_KEYSPACE module child; a plain RM_Fork child still gets it flipped to 1 and restored on reap
- [x] #3 Inside a NO_KEYSPACE child RM_KeyExists / RM_OpenKey / RM_Call are refused (0 / NULL / NULL with EPERM) and logged once per API, both with hybrid-snapshot on and off
- [x] #4 Unknown flag bits are rejected: the call returns -1 with errno EINVAL and no child is forked
- [x] #5 A second fork while a NO_KEYSPACE child runs fails with EEXIST as for RM_Fork (single child slot)
- [x] #6 RM_ForkWithFlags is exported in redismodule.h and documented in module.c; the design doc's module contract and interactions table describe it
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
1. snapshot.c: factor the hybrid child's fork (skip the arena flip, reserve the arena range in the child, mark the process keyspace-less) into snapForkNoKeyspace(purpose); the hybrid save uses it.
2. Rename the child predicate snapInHybridChild -> snapChildNoKeyspace (module guard, crash-report INFO sections, bitmap INFO getter).
3. module.c: moduleFork(module, flags, cb, ud) shared by RM_Fork and the new RM_ForkWithFlags(ctx, flags, cb, ud); unknown flags -> EINVAL; NO_KEYSPACE -> snapForkNoKeyspace(CHILD_TYPE_MODULE). Guard message names the module-fork case.
4. redismodule.h: REDISMODULE_FORK_NO_KEYSPACE, _REDISMODULE_FORK_FLAGS_NEXT, API pointer + GET_API.
5. Tests: tests/modules/fork.c fork.nokeyspace / fork.badflags; tests/unit/moduleapi/fork.tcl (refusals, EINVAL, EEXIST) and unit/snapshot.tcl fallback block (arena stays DONTFORK through the child's life).
6. docs/hybrid-snapshot.md sections 5 and 7.
<!-- SECTION:PLAN:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
Implemented steps 1-6. The arena range is reserved in the child only when the arena was DONTFORK at the fork (snapArenaAvailable && !snapArenaForkable); this also stops the hybrid child from trying to reserve a range it has mapped in the debugger-forkable case. The refusals apply whatever the arena state, so the contract is the same with the feature off.

unit/moduleapi/fork passes with hybrid-snapshot off and with --config hybrid-snapshot yes (6/6 each). In the hybrid-on run the server log shows exactly two 'arena made forkable for a MODULE child' lines, both from the plain RM_Fork tests, and none for the NO_KEYSPACE forks.

Build note: built with REDIS_CFLAGS='-DREDIS_TEST -Werror', which changed .make-settings and forced a full rebuild including deps.

unit/snapshot: 83/83 pass, including the new 'Module fork with REDISMODULE_FORK_NO_KEYSPACE' test (arena sampled 0 ten times over the child's 1.5 s life, no flip log line) and the existing plain-module-fork flip test. redis-server test snapshot: 212/212. Not covered by a test: a crash inside a NO_KEYSPACE module child (the crash-report path is shared with the hybrid child and tested there).
<!-- SECTION:NOTES:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
Added RedisModule_ForkWithFlags(ctx, flags, cb, user_data) with REDISMODULE_FORK_NO_KEYSPACE. A module child declared keyspace-less is forked like the hybrid snapshot child (new snapForkNoKeyspace, now also used by the hybrid save): the SNAP_NOFORK arena is not flipped forkable, its range is reserved in the child, and the keyspace module APIs plus the keyspace INFO sections of a crash report are refused or skipped there (predicate renamed snapInHybridChild -> snapChildNoKeyspace). The refusals apply with the feature on or off. RM_Fork is unchanged. Unknown flags fail with EINVAL. Tests: fork.nokeyspace / fork.badflags in tests/modules/fork.c, three tests in unit/moduleapi/fork (run with the feature off and on), one in unit/snapshot. Docs: docs/hybrid-snapshot.md sections 5 and 7, API docs in module.c and redismodule.h.
<!-- SECTION:FINAL_SUMMARY:END -->
