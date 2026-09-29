---
id: TASK-1.14
title: Diskless full sync over the hybrid scan (fork writes replica sockets)
status: To Do
assignee: []
created_date: '2026-09-20 17:22'
updated_date: '2026-09-29 18:07'
labels:
  - bgsave
  - persistence
  - replication
milestone: m-1
dependencies:
  - TASK-1.9
references:
  - >-
    https://github.com/valkey-io/valkey/pull/4797 (Valkey forkless full sync,
    for comparison)
  - >-
    src/replication.c startBgsaveForReplication, rdb-channel design comment near
    replicationSetupSlaveForFullResync
  - src/rdb.c rdbSaveToSlavesSockets
documentation:
  - poc-hybrid-bgsave-spec.md
parent_task_id: TASK-1
priority: high
ordinal: 15000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Spec §6 and M2: replication full sync with repl-diskless-sync uses the same scan and channel (FORK_KVOBJ copies for SNAP_FORK keys, ~64 KB NOFORK_BUFFER entries, child-side SELECTDB tracking), but the child writes the RDB stream to the replica sockets (or the parent-side rdb_pipe used by the current diskless implementation) instead of a temp file. Disk-based full sync (repl-diskless-sync no) reuses the file path from M1 and needs only the replication bookkeeping wired in.

Scope: route rdbSaveToSlavesSockets and the rdbchannel-based replication path through the hybrid writer in snapshot.c; keep the EOF-marker framing used by diskless streams; handle replica disconnect mid-transfer and the multiple-replica fan-out as today; replication offset / repl-id capture at the fork instant must remain exactly consistent with the snapshot (the fork instant is the point at which the backlog offset is recorded, and the same instant at which slot states become PENDING); repl-diskless-sync-delay and max-replicas waiting semantics unchanged; child-info reporting used for replication stats continues to work.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 Full sync with repl-diskless-sync yes uses the hybrid scan and the replica ends with a keyspace identical to the primary's fork-instant state, including under the harness race workload on the primary
- [ ] #2 Full sync with repl-diskless-sync no uses the hybrid file save and works unchanged
- [ ] #3 Replica disconnect mid-transfer and multi-replica fan-out behave as before, covered by the existing replication tcl tests plus new ones for the hybrid path
- [ ] #4 Replication offset and replid recorded for the sync match the fork instant, verified by a test that writes continuously during the sync and checks the replica converges with no missing or duplicate commands
- [ ] #5 Existing replication, rdbchannel and psync test suites pass with the hybrid switch on
- [ ] #6 A full sync to a replica with the rdb-channel capability runs with the hybrid child writing the rdb-channel connections directly while the main channel streams the backlog, and the replica converges (digest equal) under continuous writes
- [ ] #7 A replica that stops reading mid-transfer does not block the main thread; the save ends by repl-timeout or by the retained-bytes hard mark and is reported as failed, covered by a test
<!-- AC:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
2026-09-24: the teammate's forkless-hybrid POC (spec 11.z.3) routes diskless replication through the same child rio as the disk save, with rdb_channel forced off and slot snapshots / NO_COMPRESS rejected. Our child already owns one rio; the socket case should reuse it the same way. Until then TASK-1.22 (arena madvise flip) provides a stock-path fallback for diskless sync.

2026-09-29 design decision (after reviewing Valkey PR 4797, forkless full sync): the child owns the replica sockets in every diskless mode, as it owns the file today; the scan side does not change. Two shapes, both stock: (a) rdb-channel (SLAVE_REQ_RDB_CHANNEL): the child gets the rdb-channel connections in blocking mode via rioInitWithConnset and writes the stream directly, while the parent streams the backlog on the main channel from the fork offset and the replica buffers it; (b) single connection (replica without rdb-channel capability, or TLS continuity): the child writes the stock rdb_pipe and the parent's existing non-blocking fan-out (rdbPipeReadHandler) relays to the sockets. Do not let the scan thread or any parent thread hold a replica connection: cross-thread client ownership is where the Valkey PR's review found its defects. The replication offset is the stock one recorded in replicationSetupSlaveForFullResync just before the fork; our freeze and fork happen in the same main-thread step, so that call goes there. A slow replica blocks the child on the socket, which fills the scan pipe and grows retained bytes: expected outcome is repl-timeout or the hard mark aborting the save, never a blocked main thread; add a test for it. Inline replication (commands spliced into the RDB stream, Valkey's RDB_OPCODE_UPDATE idea) is deliberately out of scope here and filed separately as TASK-1.29; it builds on this task and on the queue in front of the pipe (TASK-1.27). Replica-side deduction of 'bucket complete' from the stream was considered and rejected: the hybrid stream interleaves main-thread emissions, so a slot's records are not contiguous and the replica cannot know completion without new markers.
<!-- SECTION:NOTES:END -->
