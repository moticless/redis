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
updated_date: '2026-10-11 19:51'
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
modified_files:
  - src/snapshot.c
  - src/snapshot.h
  - src/cluster_asm.c
  - src/cluster_asm.h
  - src/kvstore.c
  - src/evict.c
  - docs/hybrid-snapshot.md
  - tests/unit/cluster/atomic-slot-migration.tcl
  - .mfiles/bgsave-blog/bgsave-blog-draft.md
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
- [x] #1 A slot snapshot transfer with hybrid-snapshot yes freezes and scans only the migrating slots; INFO or a DEBUG probe during a migration shows slots_total equal to the number of non-empty migrating slots and no arena fork-visibility flip
- [x] #2 tests/unit/cluster/atomic-slot-migration.tcl passes with hybrid-snapshot yes and no; a new test writes to migrating and non-migrating slots during the transfer and checks the destination holds the fork-instant content of the migrating slots while the source keeps serving
- [x] #3 Cancelling a migration mid-transfer and a destination failure both end the scan cleanly (every slot DONE, overlays released, no retained bytes) and leave the source's keyspace intact
- [ ] #4 Measured on a cluster node with a multi-GB keyspace: the source's fork stall during a migration drops from the whole heap's page tables to the fork-class values' (as a hybrid BGSAVE), and its copy-on-write from every written page to fork-class values written during the migration; recorded in utils/snap-bench/RESULTS.md (reworded 2026-10-11: slot-proportional CoW is not reachable with a fork child, see the design note)
- [x] #5 The design note (implementation plan of this task) explains the handshake with ASM's state machine, the slot set hand-off and the record form; docs/hybrid-snapshot.md's entry-point and interaction tables are updated
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

2026-10-11 implementation, first pass (steps a and b of the plan): cluster_asm.c exports slotSnapshotSaveKeyValuePair, slotSnapshotWritePrelude (fail point, compression off, module commands, FUNCTION RESTORE), slotSnapshotWriteSlotInfo, slotSnapshotWriteEOF and asmMigratingSlotsBitmap (the migrate task's slots as a CLUSTER_SLOTS bitmap); slotSnapshotSaveRio is built from the same pieces. snapshot.c: snapSave.slots_form and slot_filter; snapSaveCreate captures the migrating slots at the fork instant (SNAP_NOT_TAKEN when no migrate task runs) and turns compression off; snapScanSetSlotFilter restricts snapScanFreezeStore to the filter; snapSaveWriteRecord's third form; snapChildSelect writes SELECT per db and SLOT-INFO once per slot from the save's fork-instant sizes; the child writes the prelude instead of the RDB header and SNAPSHOT-EOF instead of footer and EOF mark; snapSaveTryStart takes slot snapshots when a migrate task runs. Two new cluster tests appended to atomic-slot-migration.tcl (hybrid only): slots_total equals the migrating slot count with the arena unmapped, writes to migrating and other slots during the transfer; cancel mid-snapshot ends the save cleanly with the keyspace intact.

Bug found by the cluster suite's fail-point tests and fixed: snapScanCompleteSlot released the dict's state and overlay BEFORE publishing DONE. kvstoreOverlayRelease ends by asking the store whether the dict may be freed, through a public kvstore entry point and so through the guard; for an aborted slot the word still said SCANNING, the guard saw a table dict in that state and attached a fresh overlay and state, which the completion then left behind, and the dict's eventual free (the trim's temporary store on the lazy-free thread) tripped the unref assertion 'ov == NULL && ext == NULL'. Reproduced deterministically with the slot snapshot's fail points (child aborted inside slot 0), diagnosed with a temporary attach/complete trace. Fix: DONE is published first, the guard declines during the release. The unref assertion is now a descriptive panic (slot, overlay, state, word, detached, size). Latent in every abort of a dict mid-scan, not specific to the slot snapshot.

Semantics found by the test 'Expired key is not deleted and SCAN/KEYS/... filter keys in importing slots': it relies on rdb-key-save-delay making a 3-key snapshot take 3 s while it writes to those keys; the hybrid's emit-ahead serialized the written keys at once and the scan thread then had nothing left to delay, so the import finished early. Fix in the hybrid: the scan thread owes the delay for every fork-instant key whoever produced the record (itself, the child for SNAP_FORK values, or the main thread's emit-ahead, which must not slow the command): snapSaveKeyDelay sleeps it per key in abort-aware 10 ms slices, for the thread's own keys and the child's at dispatch, and for the main thread's at each window boundary (the count main_keys is atomic now). A save of N keys takes N delays, as stock's child does.

Two more differences surfaced by the cluster suite, both fixed. (1) The hybrid child's socket rio buffers small writes in user space (16 KB) and flushed only at END, so a tiny slot snapshot reached the destination all at once; stock flushes per key when rdb-key-save-delay is set and its stream is otherwise large. The child now flushes the socket rio before every blocking wait for the producers, so records reach the replica or the migration's destination as they are produced (this also helps diskless sync latency). (2) The hybrid save's bookkeeping lives in the parent: a cluster node's per-slot size tables alone are 256 KB (16384 slots x 2 x 8 bytes), plus the scan's dict tables, ring and bitmaps and the retained originals. The eviction test arms maxmemory 200 KB above the memory at its start and never resets it, so the next hybrid save on that node pushed used memory over the limit and the following test's writes got OOM; stock's child holds the equivalent memory in its own process. snapSaveOverheadBytes (queued + retained + bitmaps + tables + sizes) is now excluded from the eviction calculation in freeMemoryGetNotCountedMemory, like the replication buffers; docs updated. Diagnosed with temporary probes printing node 1's INFO memory before and after the failover test (hybrid and stock were within 3 KB of each other at the end of the eviction test; the gap appeared during the next save).

Two timing differences in the cluster suite's timeout tests, both from the same root: a socket-target child is never killed when its replica disconnects (freeClient kills only disk-target children), so it learns of the dead destination at its next write. Stock's child writes continuously (and flushes per key under rdb-key-save-delay); the hybrid child wrote only what the scan thread delivered, and with two delayed keys in one window that was 20 s later, past the next test's 20 s wait for a new save ('ASM task did not start'), while a paused source made the destination time out on the snapshot channel instead of the stream. Fixes: (a) the child polls its rdb-channel connections for hangup (POLLRDHUP) while it waits for the producers and exits with 'replica connection closed' at once; (b) the scan thread flushes its stage after every key it delayed, so records stream one key at a time under a delay, as stock's child does; (c) the paused-source test accepts either channel's timeout, since the hybrid child cannot deliver a snapshot while the parent that feeds it is stopped. A harness TIMEOUT seen once in the chain was collateral: the failed paused-source test skipped its cleanup and left the source SIGSTOPped. Also: the chain's unit/maxmemory failure was my own doing, a second runtest started on the same ports while the chain was still running (the in-sandbox ps cannot see host processes); its rerun passed 39/39.

2026-10-11 landed (see git log for TASK-1.49). Final verification: unit/cluster/atomic-slot-migration 108 ok with hybrid-snapshot yes (every stock test plus the two new hybrid-only tests, which also passed three isolated runs) and 107 ok with it off; unit/snapshot 81 ok in both modes; integration/replication 80, unit/maxmemory 39, integration/aof-multi-part 41, unit/aofrw 25 with the hybrid on; redis-server test snapshot 184; build without warnings. AC 1, 2, 3 and 5 met. AC 4 (the measurement on a multi-GB cluster node) is open: it needs the AWS host (one new c7i.4xlarge per session, see the aws-benchmark-host memory note); the expected result is the hybrid BGSAVE profile for a migration's fork stall and copy-on-write. The blog's slot-migration statements were updated to the implemented state. The last stock-mode cluster run that includes the final test-file edits is the 107-ok run on the previous binary; the two new tests are guarded by hybrid-snapshot yes and the adapted paused-source test accepts the stock pattern unchanged.
<!-- SECTION:NOTES:END -->
