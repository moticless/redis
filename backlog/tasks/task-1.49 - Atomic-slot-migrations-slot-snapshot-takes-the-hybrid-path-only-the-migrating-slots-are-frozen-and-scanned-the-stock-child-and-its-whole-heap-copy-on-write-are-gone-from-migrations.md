---
id: TASK-1.49
title: >-
  Atomic slot migration's slot snapshot takes the hybrid path: only the
  migrating slots are frozen and scanned, the stock child and its whole-heap
  copy-on-write are gone from migrations
status: In Progress
assignee:
  - '@Claude'
created_date: '2026-10-11 12:35'
updated_date: '2026-10-11 15:25'
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
- [ ] #1 A slot snapshot transfer with hybrid-snapshot yes freezes and scans only the migrating slots; INFO or a DEBUG probe during a migration shows slots_total equal to the number of non-empty migrating slots and no arena fork-visibility flip
- [ ] #2 tests/unit/cluster/atomic-slot-migration.tcl passes with hybrid-snapshot yes and no; a new test writes to migrating and non-migrating slots during the transfer and checks the destination holds the fork-instant content of the migrating slots while the source keeps serving
- [ ] #3 Cancelling a migration mid-transfer and a destination failure both end the scan cleanly (every slot DONE, overlays released, no retained bytes) and leave the source's keyspace intact
- [ ] #4 Measured on a cluster node with a multi-GB keyspace: the source's fork stall during a migration drops from the whole heap's page tables to the fork-class values' (as a hybrid BGSAVE), and its copy-on-write from every written page to fork-class values written during the migration; recorded in utils/snap-bench/RESULTS.md (reworded 2026-10-11: slot-proportional CoW is not reachable with a fork child, see the design note)
- [ ] #5 The design note (implementation plan of this task) explains the handshake with ASM's state machine, the slot set hand-off and the record form; docs/hybrid-snapshot.md's entry-point and interaction tables are updated
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
Design note (2026-10-11), from reading cluster_asm.c, replication.c, rdb.c and snapshot.c.

HOW ASM DRIVES THE SLOT SNAPSHOT TODAY
The destination opens an rdb-channel connection to the source and sends a SYNC-style request with SLAVE_REQ_SLOTS_SNAPSHOT | SLAVE_REQ_RDB_CHANNEL (plus NO_COMPRESS). On the source that connection is an ordinary replica client (task->rdb_channel_client, flag CLIENT_ASM_MIGRATING) in SLAVE_STATE_WAIT_BGSAVE_START; replicationCron starts the save at once for such a client (no diskless delay sharing). startBgsaveForReplication(): socket_target is forced for any SLAVE_REQ_RDB_MASK request; it calls snapSaveTryStart(SNAP_SAVE_SOCKETS, req, ...), which returns SNAP_NOT_TAKEN for SLOTS_SNAPSHOT today, so rdbSaveToSlavesSockets() forks the stock child (arena flipped forkable). The child does not write an RDB: slotSnapshotSaveRio() (cluster_asm.c) writes a RESP COMMAND STREAM to the pipe that the parent relays to the rdb-channel socket: FUNCTION RESTORE <payload> REPLACE; per db with keys: SELECT <db>; per migrating slot (task->slots, the one ASM_MIGRATE task's slotRangeArray) before its first key: CLUSTER SYNCSLOTS CONF SLOT-INFO <slot:keys:expires>; per key: slotSnapshotSaveKeyValuePair(): RESTORE <key> <abs-expire-or-0> <DUMP payload without checksum> ABSTTL [plus the AOF-form path for types that need it, e.g. module commands and field expiry]; finally CLUSTER SYNCSLOTS SNAPSHOT-EOF. Compression is forced off, rdb-key-save-delay applies per key. After the fork, startBgsaveForReplication() moves the client to WAIT_BGSAVE_END and, for a slots snapshot, calls asmSlotSnapshotAndStreamStart(task): main_channel_client -> SLAVE_STATE_SEND_BULK_AND_STREAM, task->state = ASM_SEND_BULK_AND_STREAM, rdb_channel_state = ASM_RDBCHANNEL_TRANSFER, and writes +SLOTSSNAPSHOT on the rdb channel. From then the main channel streams the migrating slots' live writes (fork-instant onward, stock replication machinery). When the child exits, updateSlavesWaitingBgsave(): on success the main-channel client (SEND_BULK_AND_STREAM) gets asmSlotSnapshotSucceed(task) (state ASM_SEND_STREAM, rdb_channel_state ASM_COMPLETED) and goes online; the rdb-channel client is simply freed ("Streamed slots snapshot transfer succeeded"). On failure asmSlotSnapshotFailed(task) -> asmTaskSetFailed(). Cancel (CLUSTER MIGRATION CANCEL, failover, invalid tasks) frees the rdb-channel client, which through freeClient() -> killRDBChild() reaches checkChildrenDone() -> snapOnChildEnd() -> the done handler; a hybrid save would be torn down by the same path as a hybrid full sync is today.

WHAT THE HYBRID NEEDS TO ADD
1. Dispatch: snapSaveTryStart() stops excluding SLAVE_REQ_SLOTS_SNAPSHOT (EXCLUDE_DATA stays stock). snapSaveToSlavesSockets(req, rsi) creates the save with a slot filter.
2. Slot set hand-off: snapSaveCreate() takes the migrating slotRangeArray from the one ASM_MIGRATE task (asm API: a getter returning task->slots, or the ranges copied into a CLUSTER_SLOTS bitmap on the save, like trim_slots). The set is fixed at the fork instant, as stock's child reads its CoW copy of task->slots.
3. Scan over a subset: snapScanFreezeStore() skips dicts outside the set (no reference, no stamp, not in ctx->dicts[s]); the guard already returns NULL for a dict that is not the save's table dict, so writes to non-migrating slots are plain writes with no funnel; the thread's pass visits only table dicts. slots_total counts the frozen subset. Cluster mode has one db, but keep the per-db loop (slotSnapshotSaveRio writes SELECT per non-empty db).
4. Record form: a third form next to RDB pairs and AOF commands, sv->record_form = SNAP_REC_RDB | SNAP_REC_AOF | SNAP_REC_RESTORE; snapSaveWriteRecord() calls slotSnapshotSaveKeyValuePair() (exported from cluster_asm.c) for the RESTORE form; it uses createDumpPayload() -> rdbSaveObject, which the scan thread already runs for SNAP_NOFORK values, so the scan thread, the main thread's emit-ahead and the child all produce it. The child's framing (snapSaveChildWrite): no $EOF mark and no RDB header/footer for this form; instead FUNCTION RESTORE + the SELECT per db with keys at the start, SLOT-INFO before the first record of each slot, SNAPSHOT-EOF at the end. SLOT-INFO carries kvstoreDictSize(keys/expires, slot) at the fork instant: the save already records per-slot sizes in snapSaveCreate (sv->sizes[j].slot_keys/slot_expires) for the RDB slot-info opcode, reuse them; the child learns slot boundaries from the records' didx as it does for the RDB form (snapChildSelect / slot bookkeeping). Trim-slot skipping (TASK-1.44) applies unchanged. Compression is irrelevant to the RESTORE form (DUMP payload); rdb-key-save-delay applies on the scan thread as today.
5. Child output: SNAP_SAVE_SOCKETS already streams the child's output to the replicas' sockets through the parent's pipe relay (rdb_pipe_conns), the same path the stock slot-snapshot child uses; nothing new, but verify that the relay's '+SLOTSSNAPSHOT' prelude and the client flags are set before the fork (startBgsaveForReplication does it after the save started, same order as stock).
6. End of transfer: the hybrid child exits after SNAPSHOT-EOF; snapOnChildEnd() joins the thread and releases the overlays, then backgroundSaveDoneHandler -> updateSlavesWaitingBgsave -> asmSlotSnapshotSucceed/Failed, unchanged. Cancel mid-scan: freeClient(rdb_channel_client) -> killRDBChild() -> snapSaveAbort/snapOnChildEnd, as a dropped replica does during a hybrid diskless sync today (covered by the harness tests); add a cluster test that cancels during the transfer and checks the source's keyspace and that no retained bytes or overlays remain.
7. The arena is not flipped forkable for this child (snapArenaBeforeFork sees a hybrid fork), so the migration's fork stall becomes the fork-class page tables, and copy-on-write becomes the fork-class values written anywhere during the migration. A child's copy-on-write cannot be confined to the migrating slots: the child maps all forkable memory whatever it reads, and dismissing non-migrating pages would need a walk of every key. So the honest expectation is 'the same cost profile as a hybrid BGSAVE', not 'slot-proportional'; AC 4 reworded accordingly. A no-fork variant (skip the child when the migrating slots hold no fork-class key) is not possible without a pass over the slots before the fork instant; noted as a later option, not in scope.

ORDER
a. Export slotSnapshotSaveKeyValuePair and a getter for the migrating slot set from cluster_asm.c; the record form enum and the per-form framing in snapshot.c (child header/footer, scan/main record writer).
b. Slot filter in snapSaveCreate/snapScanFreezeStore; dispatch change in snapSaveTryStart.
c. Tests: atomic-slot-migration.tcl in both modes; new tests: writes to migrating and non-migrating slots during the transfer (destination content = fork instant of the migrating slots, source unaffected), cancel mid-transfer, destination drop mid-transfer; unit/snapshot cluster harness check that slots_total equals the migrating slot count.
d. Measurement on the AWS host (cluster of two nodes, multi-GB keyspace, migrate a few hundred slots under writes): fork stall and CoW stock vs hybrid, RESULTS.md.
e. docs/hybrid-snapshot.md: entry-point table (slot snapshot: hybrid), interaction table (ASM row), the design note condensed; redis.conf/README mention.
<!-- SECTION:PLAN:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
2026-10-11 design pass done (see the implementation plan). Key finding: the slot snapshot is not an RDB but a RESP command stream (FUNCTION RESTORE, SELECT, CLUSTER SYNCSLOTS CONF SLOT-INFO per slot, RESTORE ... ABSTTL per key from a DUMP payload, CLUSTER SYNCSLOTS SNAPSHOT-EOF), so the hybrid needs a third record form (after RDB pairs and AOF commands), not a new transport: the existing SNAP_SAVE_SOCKETS child/pipe relay carries it. Second finding: a fork child's copy-on-write cannot be confined to the migrating slots, so the gain is 'hybrid BGSAVE cost profile for migrations' (fork-class page tables and fork-class CoW) rather than slot-proportional; AC 4 reworded. Implementation starts with the record form and the slot filter.
<!-- SECTION:NOTES:END -->
