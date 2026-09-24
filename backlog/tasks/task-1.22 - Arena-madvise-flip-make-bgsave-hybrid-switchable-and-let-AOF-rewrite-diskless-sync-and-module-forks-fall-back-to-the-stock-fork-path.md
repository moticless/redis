---
id: TASK-1.22
title: >-
  Arena madvise flip: make bgsave-hybrid switchable and let AOF rewrite,
  diskless sync and module forks fall back to the stock fork path
status: To Do
assignee: []
created_date: '2026-09-24 06:21'
labels:
  - bgsave
  - persistence
  - memory
milestone: m-3
dependencies: []
documentation:
  - poc-hybrid-bgsave-spec.md
parent_task_id: TASK-1
priority: high
ordinal: 12100
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Idea taken from the teammate's forkless-hybrid POC (spec 11.z.3, item 1). The SNAP_NOFORK arena is a single reserved range with one MADV_DONTFORK; flipping it to MADV_DOFORK is one madvise call and makes every header, small value and dict table visible to a fork child again, so the stock fork-based code paths work unchanged (at stock CoW cost) while the flip is in effect. Use it (a) around AOF rewrite, diskless full sync and RedisModule_Fork, which are refused today while the arena is on: flip to DOFORK before the fork, flip back to DONTFORK after the child exits (no hybrid save may run meanwhile, and vice versa); (b) to make bgsave-hybrid a mutable config: switching it off flips the arena forkable and routes saves to the stock path, switching it on flips it back. Placement of values keeps following the policy in both states so the switch is O(1), not O(keys). Document the CoW cost of the fallback and add INFO visibility of the arena's current fork visibility.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 BGREWRITEAOF, a disk-based and a diskless full sync, and a module fork (tests/modules) succeed with bgsave-hybrid yes and produce correct output, verified by tests that reload or compare digests
- [ ] #2 A hybrid save cannot start while the arena is forkable for a fallback operation, and a fallback operation cannot start during a hybrid save; both are covered by tests
- [ ] #3 CONFIG SET bgsave-hybrid no/yes at runtime switches the save path and the arena visibility in O(1); a save after each switch reloads correctly
- [ ] #4 INFO reports whether the arena is currently DONTFORK
- [ ] #5 The fork child of a fallback operation observes stock-like CoW (documented in RESULTS.md with one measurement)
<!-- AC:END -->
