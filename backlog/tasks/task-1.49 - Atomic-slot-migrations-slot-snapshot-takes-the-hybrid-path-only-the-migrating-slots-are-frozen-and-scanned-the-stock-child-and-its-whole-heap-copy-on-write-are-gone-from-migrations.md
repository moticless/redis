---
id: TASK-1.49
title: >-
  Atomic slot migration's slot snapshot takes the hybrid path: only the
  migrating slots are frozen and scanned, the stock child and its whole-heap
  copy-on-write are gone from migrations
status: To Do
assignee: []
created_date: '2026-10-11 12:35'
labels:
  - hybrid-bgsave
  - cluster
  - asm
milestone: m-2
dependencies:
  - TASK-1.44
references:
  - 'src/snapshot.c snapSaveTryStart: SLAVE_REQ_SLOTS_SNAPSHOT exclusion'
  - >-
    src/cluster_asm.c: startBgsaveForReplication(c->slave_capa, c->slave_req)
    and the transfer states
  - >-
    src/replication.c startBgsaveForReplication, rdb-channel replication (PR
    #13732); ASM PR #14414
  - tests/unit/cluster/atomic-slot-migration.tcl
  - >-
    tests/unit/snapshot.tcl: 'slots trimmed by atomic slot migration during a
    save stay in the dump'
  - >-
    TASK-1.44 (trim-slot skip), TASK-1.37 (dict references), TASK-1.38
    (functions-only child arena flip)
parent_task_id: TASK-1
priority: medium
ordinal: 45000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Filed 2026-10-11 at the user's request. Today the slot snapshot transfer of atomic slot migration (ASM, the rdb-channel based migration, cluster_asm.c) keeps the stock fork child: snapSaveTryStart returns SNAP_NOT_TAKEN for SLAVE_REQ_SLOTS_SNAPSHOT (snapshot.c, next to the functions-only exclusion), so the arena is flipped forkable around the fork and the source node pays stock fork cost and whole-heap copy-on-write for the duration of every migration, even though only a handful of slots are being serialized. Coexistence is correct and tested (TASK-1.44: keys of slots under a trim job at the fork instant are skipped as in stock; TASK-1.37: moved slot dicts stay with a running save by reference), but the hybrid's benefit does not reach migrations.

Outcome wanted: a slot snapshot request takes the hybrid path over the migrating slots only. The scan freezes just those slot dicts (the per-slot dicts of cluster mode are exactly the overlay's unit of freezing), the scan thread serializes their SNAP_NOFORK keys, and SNAP_FORK values reach the destination either through the existing hybrid child protocol or through a child whose copy-on-write exposure is the migrating slots rather than the heap (to be decided in the design step: the child cannot avoid mapping the heap, so the second option only shrinks the written pages, not the fork). Completion, abort and failure must plug into ASM's transfer state machine on the rdb-channel link (the source's slot-snapshot child exit and the 'send-stream' transition, the destination's loading, the trim that follows), not into the BGSAVE done handler. Writes to the migrating slots during the transfer go through the overlay as in any save; writes to other slots are untouched. The functions-only (EXCLUDE_DATA) transfer stays stock (TASK-1.38 covers its arena flip separately).

Design first: a short note (in the task or docs/hybrid-snapshot.md) on how ASM drives the transfer today, where the hybrid's start/end hooks attach, how the slot set is passed (the request carries the slot ranges), what happens on a migration cancel mid-scan, and which of the two SNAP_FORK options is taken. Estimate: a few days of design and implementation plus the cluster suite.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 A slot snapshot transfer with hybrid-snapshot yes freezes and scans only the migrating slots; INFO or a DEBUG probe during a migration shows slots_total equal to the migrating slot count and no arena fork-visibility flip
- [ ] #2 tests/unit/cluster/atomic-slot-migration.tcl passes with hybrid-snapshot yes and no; a new test writes to migrating and non-migrating slots during the transfer and checks the destination holds the fork-instant content of the migrating slots while the source keeps serving
- [ ] #3 Cancelling a migration mid-transfer and a destination failure both end the scan cleanly (every slot DONE, overlays released, no retained bytes) and leave the source's keyspace intact
- [ ] #4 Measured on a cluster node with a multi-GB keyspace: the source's fork time and copy-on-write during a migration of a few slots drop from heap-proportional to slot-proportional, recorded in utils/snap-bench/RESULTS.md
- [ ] #5 A design note explains the handshake with ASM's state machine, the slot set hand-off and the chosen SNAP_FORK delivery option; docs/hybrid-snapshot.md's entry-point and interaction tables are updated
<!-- AC:END -->
