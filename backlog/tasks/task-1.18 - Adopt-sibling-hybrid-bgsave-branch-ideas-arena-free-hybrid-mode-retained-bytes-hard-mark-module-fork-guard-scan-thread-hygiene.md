---
id: TASK-1.18
title: >-
  Adopt sibling hybrid-bgsave branch ideas: arena-free hybrid mode,
  retained-bytes hard mark, module-fork guard, scan-thread hygiene
status: Done
assignee:
  - '@moti.cohen@redis.com'
created_date: '2026-09-20 22:52'
updated_date: '2026-09-20 23:06'
labels:
  - bgsave
  - persistence
  - correctness
milestone: m-0
dependencies: []
references:
  - /home/moti/git/redis/.mfiles/task-234 - Hybrid-BGSAVE/new_bgsave.md
  - /home/moti/git/redis/src/snapshot.c
documentation:
  - poc-hybrid-bgsave-spec.md
parent_task_id: TASK-1
priority: high
ordinal: 11500
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Comparison of this branch against ~/git/redis (branch hybrid-bgsave, per-key generation + refcount pins + main-thread sweep design) identified ideas worth carrying over into the frozen-slot/overlay design (requested 2026-09-21):

1. Hybrid path independent of the arena: `bgsave-hybrid` alone gates the hybrid save; without the SNAP_NOFORK arena (non-Linux, libc allocator, sanitizer builds) keys are classified by encoding+size and the mechanics are identical, so TSan/ASan can exercise the scan thread, overlay and child.
2. Bounded retention: `bgsave-hybrid-max-retained-bytes` (default 256mb, 0 = off). The overlay accounts every original it keeps alive (kvobj + private value copy); past the cap the save is aborted from serverCron (never inside a write path), reported as a failed save (`rdb_last_bgsave_status err`, consecutive failures), and counted in INFO `snap_hard_mark_aborts`; `snap_save_retained_bytes` / `snap_last_save_retained_peak` expose the retention.
3. `RedisModule_Fork` refused (EPERM) while the arena is on: a module child cannot read DONTFORK memory.
4. Scan-thread hygiene: `rdb-key-save-delay` copied per save into an atomic the scan thread reads (CONFIG SET apply hook updates it; the child keeps its fork-time copy); `hashTypeNext` no longer reads `server.allow_access_expired` unless skipping was requested.
5. Latency monitor events `hybrid-freeze` and `hybrid-merge`; `DEBUG SNAP-DRIFT` lists values whose placement disagrees with their class.

Not adopted (by design difference): per-key generation bits and reset pass (overlay state is per save), main-thread sweep and pin/duplicate scheme (the scan thread walks frozen dicts), child-owned output with writer thread (possible M3 throughput lever), AOF rewrite / diskless sync as hybrid sessions (M2 tasks 1.11-1.15 here).
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 Hybrid saves run and pass the snapshot tcl suite in a build without the arena (MALLOC=libc or sanitizer), with arena-only assertions skipped
- [x] #2 Retained-bytes hard mark aborts a save under write pressure with status err, no client command stalls, and the next save succeeds (test)
- [x] #3 RedisModule_Fork returns -1/EPERM while the arena is on
- [x] #4 No scan-thread read of a main-thread-mutable global remains in the serialization path (rdb_key_save_delay, allow_access_expired)
- [x] #5 INFO persistence exposes retained bytes, last peak and hard-mark abort count
<!-- AC:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
TSan run of unit/snapshot in the arena-free build passed clean on 2026-09-21 after three follow-up fixes it exposed (merge UAF, scan-thread reads of server.hz/maxmemory_policy/rdb_compression, shared single-allocation values without the arena). AC #1 verified by that run, #2 and #5 by the hard-mark test, #3 by code inspection (module.c RM_Fork), #4 by the TSan run.
<!-- SECTION:NOTES:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
Implemented in commit 1b9d645c5 on branch snapshot-hybrid-bgsave together with the harness (TASK-1.10). All ACs covered by tests/unit/snapshot.tcl: the hard-mark test, the `snap_hybrid_supported`/`snap_arena_available` split, INFO fields; the module-fork guard is code-only (no module test forks with the arena on).
<!-- SECTION:FINAL_SUMMARY:END -->
