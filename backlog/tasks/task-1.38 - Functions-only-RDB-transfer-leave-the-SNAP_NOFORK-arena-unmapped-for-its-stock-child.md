---
id: TASK-1.38
title: >-
  Functions-only RDB transfer: leave the SNAP_NOFORK arena unmapped for its
  stock child
status: To Do
assignee: []
created_date: '2026-10-08 15:56'
labels:
  - hybrid-bgsave
  - replication
  - arena
milestone: m-1
dependencies: []
references:
  - >-
    src/rdb.c rdbSaveBackground (the EXCLUDE_DATA exemption, comment
    'Functions-only saves have no keyspace content and keep the stock path') and
    rdbSaveToSlavesSockets
  - >-
    src/replication.c startBgsaveForReplication (SLAVE_REQ_RDB_EXCLUDE_DATA
    keeps the stock child)
  - >-
    src/snapshot.c snapArenaBeforeFork / snapArenaChildExited (the per-child
    MADV_DOFORK flip and its restore)
  - >-
    src/server.c dismissMemoryInChild (already skips the arena when it is
    unmapped)
documentation:
  - poc-hybrid-bgsave-spec.md
parent_task_id: TASK-1
priority: low
ordinal: 34000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
With bgsave-hybrid yes, a functions-only RDB transfer (a replica or redis-cli asking for functions without data, SLAVE_REQ_RDB_EXCLUDE_DATA) is the one save that still forks the stock child by design: it writes header, functions and footer and has no keyspace content, so the hybrid's scan thread and channels would run for nothing. The cost is not the stock child but the blanket rule in redisFork: every non-hybrid child gets the SNAP_NOFORK arena flipped MADV_DOFORK for its lifetime because a stock child normally walks the keyspace. For a functions-only child that is wasted: the fork copies the arena's page tables too (the full-memory fork stall, 30-40 ms on 4 GB instead of about 2 ms) and writes to small values during the transfer pay copy-on-write on the arena.

Outcome wanted: a functions-only transfer with the hybrid on forks with the arena left DONTFORK, so its fork stall and copy-on-write are those of a hybrid child, and nothing in that child ever touches the keyspace. The child does not need the arena because the RDB writer with EXCLUDE_DATA skips every db and the functions live in forkable memory; the child's memory-dismissal path already skips the arena when it is unmapped. Module persistence-event handlers that run in the child are the one path to check: they must not reach for keys, and the hybrid child's existing refusal of keyspace module APIs should apply to this child too.

Decision taken with the user (2026-10-08): skip the flip for this child rather than route the transfer through the hybrid child with an empty scan (same result, more code). The ASM slot snapshot is NOT covered: it walks the keyspace and genuinely needs the flip or a real move to the hybrid child.

Rare path, low priority; nothing depends on it.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 With bgsave-hybrid yes, a functions-only replication transfer (replica advertising the functions-only request) completes and the replica loads the functions; INFO shows snap_nofork_arena_forkable 0 throughout the transfer and the fallback log line is not emitted
- [ ] #2 The functions-only child's fork stall (latest_fork_usec) on a dataset of a few GB of small values is in the range of a hybrid child's, not a stock child's, recorded in RESULTS.md
- [ ] #3 A module whose persistence event handler calls a keyspace API inside that child gets the hybrid child's refusal, not a fault (test with the snapshot test module)
- [ ] #4 The other stock children (module fork, Lua debugger fork, ASM slot snapshot, a module without the hybrid opt-in) still get the arena flipped forkable and restored on reap; the existing fallback tests in unit/snapshot pass
- [ ] #5 Spec section 6 (fallback children list) and the dependency table of children in the blog or spec updated
<!-- AC:END -->
