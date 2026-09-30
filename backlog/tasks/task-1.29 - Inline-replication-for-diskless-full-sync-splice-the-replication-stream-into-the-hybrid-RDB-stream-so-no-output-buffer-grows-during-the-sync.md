---
id: TASK-1.29
title: >-
  Inline replication for diskless full sync: splice the replication stream into
  the hybrid RDB stream so no output buffer grows during the sync
status: To Do
assignee: []
created_date: '2026-09-29 18:07'
updated_date: '2026-09-30 03:13'
labels:
  - hybrid-bgsave
  - replication
milestone: m-2
dependencies:
  - TASK-1.14
  - TASK-1.27
references:
  - >-
    https://github.com/valkey-io/valkey/pull/4797 (Valkey forkless full sync:
    RDB_OPCODE_UPDATE, capa inband-repl, REPLCONF psync-offset; review found
    lost writes between 'done iterating' and the end marker, and the opcode
    being executed from any RDB file)
  - >-
    src/snapshot.c snapSaveEmitBucket, snapSaveQueuePushLocked, mq forwarding at
    window boundaries
  - src/replication.c replicationFeedSlaves / feedReplicationBuffer
  - >-
    ~/git/dragonfly src/server/snapshot.cc ConsumeJournalChange /
    RDB_OPCODE_JOURNAL_BLOB, FinalizeJournalStream (RDB_OPCODE_FULLSYNC_END,
    RDB_OPCODE_JOURNAL_OFFSET); src/server/rdb_load.cc HandleJournalBlob;
    docs/shard-serialization.md
documentation:
  - poc-hybrid-bgsave-spec.md
parent_task_id: TASK-1
priority: medium
ordinal: 24000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
During a full sync the replication stream produced from the snapshot offset until the replica finishes loading must be buffered somewhere: in the primary's replica output buffer (plain diskless), or on the replica (rdb-channel). On a large dataset under a heavy write rate that buffer is write-rate x sync-duration, gigabytes for a long sync, and it is the classic 'output buffer limit reached during sync' failure. Valkey's forkless full sync (PR 4797) removes it by splicing the replicated commands into the RDB stream as a new opcode carrying RESP, which the replica executes while loading. Their design needs it for correctness because their snapshot is not point in time; for the hybrid it is an optimization that our point-in-time snapshot makes simple.

Design decisions already taken (restated so this task is self-contained):

1. Rule: from the fork instant until the end of the scan, EVERY replicated command is inlined, in replication order. No per-command decision. The stream is exactly 'fork-instant snapshot plus the backlog from the fork offset', delivered inside the RDB frame. (Valkey inlines only commands on already-iterated keys; that rule does not apply to fork-instant records.)
2. Ordering comes from the write funnel for free: when a command is propagated, every key it wrote is either already scanned (record ahead in the stream), ahead of the scan (the main thread emitted its whole bucket into the main-thread queue mq before the write), or inside the current window (the scan thread reads the retained original). The command is appended to the same mq after the funnel handled the write, and the scan thread forwards mq at each window boundary after flushing its own staging buffer, so every command lands after the fork-instant record of every key it touched. MULTI/EXEC blocks and multi-key commands are propagated as a unit after execution and are covered.
3. Inline the exact bytes the replication feed produces (including SELECT), so the replica's offset is the fork offset plus the inlined bytes. No placeholder offset and no new REPLCONF subcommand.
4. The end handoff is atomic on the main thread: the scan thread must NOT send END on its own when the scan finishes. It signals the main thread, which under the mq lock records the current offset, stops inlining, attaches the replica's main channel (or the single connection) to the backlog at that offset, and pushes END. Commands after that instant flow through the backlog as usual. (This is the gap that lost writes in the Valkey PR.)
5. Socket targets only, gated twice: a file must stay a pure snapshot, so disk-based sync never inlines. The primary inlines only when the replica advertised a new capability; the replica executes the opcode only when loading from a primary (RDBFLAGS_REPLICATION), never from a file, DEBUG RELOAD or an AOF preamble.
6. The child owns the socket (TASK-1.14). Two shapes: single connection with the stock rdb_pipe relay, or inline over the rdb channel with the main channel idle until the end; prefer the latter when both capabilities are present.
7. Backpressure: a slow replica fills mq instead of the output buffer; mq is bounded by bgsave-hybrid-max-retained-bytes and the save aborts at the hard mark (counterpart of the output-buffer limit). The bounded queue in front of the pipe (TASK-1.27) is the same queue and must land first.
8. FLUSHALL/SWAPDB during a save still abort it (until TASK-1.24); that applies to inline syncs too.

Considered and rejected: letting the replica deduce when a bucket/slot is complete from the two-channel stream and apply buffered commands early. The hybrid stream interleaves main-thread emissions, so a slot's records are not contiguous, the replica cannot know completion without new markers, in-order application suffers head-of-line blocking, and even with reordering it only halves the buffer.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 With a capable replica and repl-diskless-sync yes, a full sync under continuous writes (two write-load generators) ends with primary and replica equal in dbsize and DEBUG DIGEST, and the replica's offset equals the primary's at the end of the sync with no missing or duplicated commands (PSYNC continues without a second full sync)
- [ ] #2 The primary's replica output buffer (client-output-buffer) does not grow during the sync beyond the post-END live stream; measured and recorded alongside the rdb-channel and plain diskless numbers
- [ ] #3 Writes issued between the end of the scan and the END marker are delivered exactly once (test with rdb-key-save-delay to widen the window)
- [ ] #4 A replica without the capability, a disk-based sync, DEBUG RELOAD and an AOF preamble never see or execute the inline opcode; a file containing the opcode is rejected by the loader outside replication with a logged error
- [ ] #5 A slow replica does not block the main thread; the save aborts at the retained-bytes hard mark or repl-timeout and is reported as failed
- [ ] #6 MULTI/EXEC blocks, multi-key commands (RENAME, SMOVE, MSET across slots in standalone) and commands on keys created after the fork instant replicate correctly during the sync
- [ ] #7 Race harness, TSan and the existing replication, psync2 and rdbchannel tcl suites pass with the feature on
- [ ] #8 Spec section on replication and the blog section 6 status line are updated to describe the implemented behavior
<!-- AC:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
2026-09-29 precedent: Dragonfly has run this exact model in production for years. During full sync the snapshot fiber registers as a journal consumer and appends every journal record for every key inline into the same serializer buffer (RDB_OPCODE_JOURNAL_BLOB), relying on the bucket being serialized before the write (their bucket-version pre-update hook, our funnel). The end of traversal is marked with RDB_OPCODE_FULLSYNC_END; the switch to the live stream happens under a global transaction guard with no commands running, and the stream carries RDB_OPCODE_JOURNAL_OFFSET(LSN) so the replica knows where the live stream resumes: same shape as our atomic main-thread handoff. Replica executes blobs as it parses them; a FLUSHALL/FLUSHDB inside a blob aborts the load and restarts the sync (their issue 1231), which is the behavior to copy until TASK-1.24. Their backpressure lands on writer latency (blocking flush to the socket from the writer's fiber); ours must land on mq bounded by the hard mark instead, never on the main thread.

2026-09-30 evidence from run 18 (RESULTS.md, diskless full sync of 4 GB datasets at 20k writes/s, replica loading the stream directly over the rdb channel): the replication stream buffered on the replica during the sync peaked at 23-24 MB (small, 10 s sync), 29-31 MB (mixed, 13 s) and 32-33 MB (mixed1m, 17 s); Valkey's single-channel fork-based sync held 56-77 MB in the primary's output buffer over 30-39 s. At these sync durations the buffer inline replication would remove is a few percent of the dataset; the case for it is long syncs (tens of GB, minutes) or replicas without the rdb channel. Keep the task, lower its urgency.
<!-- SECTION:NOTES:END -->
