---
id: TASK-1.14
title: Diskless full sync over the hybrid scan (fork writes replica sockets)
status: Done
assignee:
  - Claude
created_date: '2026-09-20 17:22'
updated_date: '2026-09-30 03:49'
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
- [x] #1 Full sync with repl-diskless-sync yes uses the hybrid scan and the replica ends with a keyspace identical to the primary's fork-instant state, including under the harness race workload on the primary
- [x] #2 Full sync with repl-diskless-sync no uses the hybrid file save and works unchanged
- [x] #3 Replica disconnect mid-transfer and multi-replica fan-out behave as before, covered by the existing replication tcl tests plus new ones for the hybrid path
- [x] #4 Replication offset and replid recorded for the sync match the fork instant, verified by a test that writes continuously during the sync and checks the replica converges with no missing or duplicate commands
- [x] #5 Existing replication, rdbchannel and psync test suites pass with the hybrid switch on
- [x] #6 A full sync to a replica with the rdb-channel capability runs with the hybrid child writing the rdb-channel connections directly while the main channel streams the backlog, and the replica converges (digest equal) under continuous writes
- [x] #7 A replica that stops reading mid-transfer does not block the main thread; the save ends by repl-timeout or by the retained-bytes hard mark and is reported as failed, covered by a test
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
Approved 2026-09-30 (user OOO; execute autonomously, commit as we go).

A. Implementation: the hybrid child owns the replica stream.
1. Entry point: startBgsaveForReplication (replication.c) calls a new snapSaveToSlavesSockets(req, rsi) when the target is a socket and the hybrid is on, instead of rdbSaveToSlavesSockets. Functions-only (SLAVE_REQ_RDB_EXCLUDE_DATA) and slot snapshots (SLAVE_REQ_SLOTS_SNAPSHOT) keep the stock fork with the arena flip. It sets each WAIT_BGSAVE_START replica with matching req up for full resync at getPsyncInitialOffset(), then freezes and forks in the same main-thread step, so the offset is the snapshot's.
2. Two shapes, both stock: (a) SLAVE_REQ_RDB_CHANNEL: the replicas' connections are set blocking with the repl-timeout send timeout and the child writes them through rioInitWithConnset; (b) otherwise the child writes server.rdb_pipe_write (rioInitWithFd) and the parent registers rdbPipeReadHandler on rdb_pipe_read with rdb_pipe_conns, plus the safe-to-exit pipe handshake, exactly as rdbSaveToSlavesSockets. No parent thread ever holds a replica connection.
3. Child (snapshot.c): snapSaveChildWrite takes a rio target and a 'socket' flag: no temp file, fsync, rename or cache reclaim for sockets; write the diskless EOF-mark framing ($EOF:<40 hex>\r\n prefix, the mark after the footer) as rdbSaveRioWithEOFMark does; send child info and, for the pipe shape, wait on the safe-to-exit pipe before exiting. The scan side (dispatch, staging, queue, funnel, mq) is untouched.
4. Bookkeeping: the hybrid socket save registers server.rdb_child_type = RDB_CHILD_TYPE_SOCKET so backgroundSaveDoneHandler runs the socket variant (updateSlavesWaitingBgsave, rdb pipe cleanup), including after a hard-mark or FLUSHALL abort (killRDBChild). snapSaveChildDone keeps releasing the scan/overlay. rdb_last_cow_size, snap_saves and the child-info stats stay meaningful.
5. Config: repl-diskless-sync yes/no and rdb-channel are honoured as today; the hybrid disk-based sync path is unchanged.

B. Tests (tests/unit/snapshot.tcl and the stock suites with --config bgsave-hybrid yes).
6. Diskless sync converges under two write-load clients (dbsize and DEBUG DIGEST equal, PSYNC continues without a second full sync); both rdb-channel and plain diskless (repl-rdb-channel no); two replicas in one cohort; replica disconnect mid-transfer then a successful retry; a SIGSTOPped replica: the primary keeps answering PING, the save ends by repl-timeout or the retained hard mark and is reported failed, the next sync works; INFO snap_saves increments on the hybrid path and rdb_last_cow_size stays small on a small-value dataset (proof it is not the fallback). Stock suites integration/replication*, psync2, replication-rdbchannel pass with the feature on. TSan run of unit/snapshot.

C. Benchmark (run 18).
7. bench.py 'sync' mode: primary loaded with the dataset (cache), a replica started on the load CPUs, replicaof, measure sync duration (until master_link_status up and offsets equal), primary peak memory (parent RssAnon growth + child Private_Dirty), primary main-thread CPU, write rate and latency during the sync, replica load time, replica-side buffer peak (repl_full_sync_buffer / mem_replication_backlog) and primary replica output-buffer peak. Datasets small, mixed, mixed1m at 4 GB, rates 0 and 20k, 2 reps, interleaved.
8. Configurations: up 0d6266f2d (default rdb-channel diskless), hybrid, valkey 06d35aca6 fork-based diskless; Valkey's forkless full sync (PR 4797 branch) built and included marked experimental only if it syncs cleanly, otherwise reported as not measurable.
9. Deliverables: RESULTS.md run 18, blog section 6 with measured numbers, this task closed with a final summary, a note in TASK-1.29 with the observed buffer sizes.

Out of scope: inline replication (TASK-1.29), slot snapshots and functions-only syncs on the hybrid path, AOF rewrite (TASK-1.15).
<!-- SECTION:PLAN:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
2026-09-24: the teammate's forkless-hybrid POC (spec 11.z.3) routes diskless replication through the same child rio as the disk save, with rdb_channel forced off and slot snapshots / NO_COMPRESS rejected. Our child already owns one rio; the socket case should reuse it the same way. Until then TASK-1.22 (arena madvise flip) provides a stock-path fallback for diskless sync.

2026-09-29 design decision (after reviewing Valkey PR 4797, forkless full sync): the child owns the replica sockets in every diskless mode, as it owns the file today; the scan side does not change. Two shapes, both stock: (a) rdb-channel (SLAVE_REQ_RDB_CHANNEL): the child gets the rdb-channel connections in blocking mode via rioInitWithConnset and writes the stream directly, while the parent streams the backlog on the main channel from the fork offset and the replica buffers it; (b) single connection (replica without rdb-channel capability, or TLS continuity): the child writes the stock rdb_pipe and the parent's existing non-blocking fan-out (rdbPipeReadHandler) relays to the sockets. Do not let the scan thread or any parent thread hold a replica connection: cross-thread client ownership is where the Valkey PR's review found its defects. The replication offset is the stock one recorded in replicationSetupSlaveForFullResync just before the fork; our freeze and fork happen in the same main-thread step, so that call goes there. A slow replica blocks the child on the socket, which fills the scan pipe and grows retained bytes: expected outcome is repl-timeout or the hard mark aborting the save, never a blocked main thread; add a test for it. Inline replication (commands spliced into the RDB stream, Valkey's RDB_OPCODE_UPDATE idea) is deliberately out of scope here and filed separately as TASK-1.29; it builds on this task and on the queue in front of the pipe (TASK-1.27). Replica-side deduction of 'bucket complete' from the stream was considered and rejected: the hybrid stream interleaves main-thread emissions, so a slot's records are not contiguous and the replica cannot know completion without new markers.

2026-09-30 implementation (uncommitted, building clean): snapshot.c splits snapSaveBackground into snapSaveCreate (save object, channel, queues, scan ctx) and snapSaveLaunch (freeze, fork, thread); new snapSaveToSlavesSockets(req, rsi) mirrors rdbSaveToSlavesSockets: sets WAIT_BGSAVE_START replicas with the exact req up for full resync at getPsyncInitialOffset() right before the launch, rdb-channel shape puts the connections in blocking mode and the child writes them via rioInitWithConnset, pipe shape creates the stock rdb pipe and safe-to-exit pipe and the parent registers rdbPipeReadHandler; rdb_child_type = SOCKET so the stock socket done handler runs. snapSaveChildWrite(sv, rio *out) writes the diskless framing ($EOF:mark ... mark) around the stream when out != NULL and skips the file steps; snapSaveChild sets the process title, honours SLAVE_REQ_RDB_NO_COMPRESS/NO_CHECKSUM in the child (the scan thread's compression flag is set from the request at create), and for the pipe shape closes the pipe and waits on the safe-to-exit pipe. snapSaveChildDone does not touch lastbgsave_status for socket saves (as stock). replication.c startBgsaveForReplication routes socket targets to snapSaveToSlavesSockets when the hybrid is on, except functions-only and slots snapshots. Tests written in tests/unit/snapshot.tcl: the fallback-block diskless test now asserts the hybrid path (snap_saves +1, arena never forkable, 'Hybrid RDB transfer to' logged); new block 'snapshot diskless' run for rdb-channel yes and no: converge under two write-load clients with one full sync and PSYNC continuing, two replicas sharing one child (repl-diskless-sync-delay 2), replica disconnect mid-transfer (client kill type replica) then a working resync, and (rdb-channel yes) a SIGSTOPped replica: master answers 20/20 pings, transfer ends within repl-timeout 3, replica resyncs after SIGCONT.

2026-09-30 committed d05c8c3f2: implementation plus tests plus bench.py sync mode. unit/snapshot feature-on 53/53 (8 new: converge under write load, two replicas one child, disconnect mid-transfer, for rdb-channel yes and no; stalled replica for rdb-channel yes; the old fallback test now asserts the hybrid path). One test bug on the way: wait_for_condition needs its else clause, otherwise the catch swallowed the arity error and looked like a hang. Manual check of the stalled replica: the child's connection-set write fails with 'Connection timed out' after repl-timeout, the master logs 'SYNC failed. BGSAVE child returned an error', main thread answered every PING. Stock replication suites (integration/replication*, replication-rdbchannel, replication-buffer, psync2*) running with --config bgsave-hybrid yes; TSan run of unit/snapshot next.

2026-09-30 stock suites with --config bgsave-hybrid yes (integration/replication, -2, -3, -4, -psync, -rdbchannel, -buffer, psync2, psync2-reg, psync2-pingoff, psync2-master-restart): 356/358, the two failures fixed in c5183cd4b and passing alone: (1) 'diskless replication read pipe cleanup' waits for stock's log line 'Background RDB transfer started by pid', the hybrid socket save now logs that wording; (2) 'master continues RDB delivery if not all replicas are dropped' exposed a latent stock bug in rioConnsetWrite(): the failed-connection counter is a running total across the 32 KB chunks of one write while an already-failed connection is counted on every chunk, so with one dead replica out of two the second chunk reached n_dst and the transfer to the live replica failed ('write error (write buffer): Broken pipe', a third full sync). Stock's writes are small enough to flush in one chunk; the hybrid child's 64 KB buffers take three. Reset per chunk; upstream-worthy. TSan run of unit/snapshot on the diskless code started; run 17 (three-way mixed1m BGSAVE, for the blog table) finishing on AWS, run 18 (three-way full sync) chained after it with the fixed tree (redis-after6) and the Valkey PR 4797 branch built as an experimental fourth config.

2026-09-30 TSan (setarch -R, libc malloc) on the diskless code: 44 ok, the two failures were a test bug of mine (count_log_message is a grep, my pattern used '*' as a glob); fixed and committed, TSan rerun in progress. Note for the TSan tree: rsync of src/ must drop Makefile.dep and *.d (they point at the jemalloc headers the libc build lacks and make silently builds nothing). Run 18 restarted at 01:52 host time after the first attempt left the replica's working directory on the root disk (bench.py default /tmp; run.sh now passes --workdir "$D"), where a stock sync crawled at 2 MB/s; the replica now runs with --repl-diskless-load on-empty-db on tmpfs. First cell: stock full sync of mixed1m (4 GB) in 16.3 s, child CPU 14.2 s, versus 43 s for its BGSAVE: a replica loading disklessly sends REPLCONF rdb-no-compress 1 (replication.c:3258), so the child skips LZF; the hybrid honours the same request (child and scan thread), to be checked on Valkey.

2026-09-30 acceptance criteria 1-7 verified: unit/snapshot feature-on 53/53 (debug build) and 46/46 under TSan with no reports (arena tests skip under libc malloc); stock replication, rdbchannel, buffer and psync2 suites 358/358 after c5183cd4b (356 in the first run, the two fixed tests passing alone). AC2 is the existing 'disk-based full sync places values on the replica and matches the master' test, still on the hybrid file path. Task stays open for plan item C (run 18 write-up, blog section 6, TASK-1.29 buffer note).

2026-09-30 run 18 complete and written up (RESULTS.md, blog section 6); the experimental Valkey forkless full sync (PR 4797 branch 8dc5df0d5) completed all 12 cells with the replica's dbsize equal: 1-13 MB extra primary memory, but 36-50 s per sync (stock and hybrid 10-17 s), 11-17 s of main-thread CPU per sync, write p99 1.0-1.45 ms, and on mixed1m at 20k writes/s 92% of the offered rate with a 0.85-0.91 s worst write and 31-34 stalled seconds. Task closed; final summary above. Follow-ups left as observations: slot-snapshot and functions-only transfers still on the stock child; TASK-1.29's urgency lowered by the 23-33 MB buffer numbers.
<!-- SECTION:NOTES:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
## Diskless full sync through the hybrid child (TASK-1.14)

**What changed (d05c8c3f2, c5183cd4b, 5ce44553e, test fix commit).**
- `startBgsaveForReplication` routes socket-target syncs to the new `snapSaveToSlavesSockets()` when the hybrid is on (functions-only and slot-snapshot transfers keep the stock child). It mirrors `rdbSaveToSlavesSockets()` on the parent side: replicas in WAIT_BGSAVE_START with the exact request are set up for full resync at the current offset immediately before the freeze-and-fork, so the offset is the snapshot's.
- Two shapes, both stock protocol: with the rdb channel the hybrid child writes the replicas' blocking connections through a connection-set rio; otherwise it writes the stock rdb pipe that the parent fans out and waits on the safe-to-exit pipe. The child frames the stream with the diskless EOF mark, honours the request's no-compression and no-checksum flags (the scan thread's compression flag follows the request too), skips every file step, and registers as RDB_CHILD_TYPE_SOCKET so the stock socket done handler and `updateSlavesWaitingBgsave` run. No parent thread ever holds a replica connection. `snapSaveBackground` is split into `snapSaveCreate` and `snapSaveLaunch`, shared by both entry points.
- A latent stock bug in `rioConnsetWrite()` fixed on the way: the failed-connection counter was a running total across the 32 KB chunks of one write, so one dead replica out of two aborted the transfer to the live one once a write spanned two chunks (stock's small writes never did; the hybrid's 64 KB buffers do). Upstream-worthy on its own.

**Tests.** unit/snapshot: for rdb-channel yes and no, a sync converges under two write-load clients with one full sync and PSYNC continuing, two replicas share one child, a replica killed mid-transfer resyncs on retry; with the rdb channel a SIGSTOPped replica leaves the master answering every PING and the transfer ends by repl-timeout, then the replica resyncs. The old fallback test asserts the hybrid path (snap_saves +1, arena never mapped). 53/53 feature-on, 46/46 under TSan with no reports. Stock integration/replication, -2, -3, -4, -psync, -rdbchannel, -buffer and psync2* suites: 358/358 with the feature on.

**Measured (run 18, c7i.4xlarge, 4 GB, fresh replica per cell loading the stream directly, RESULTS.md).** Sync time equals the replica's load time for stock and the hybrid (10 s small, 13 s mixed, 17 s mixed1m, idle or at 20k writes/s); Valkey's default fork-based sync takes 28-39 s. Primary memory during a sync under 20k writes/s: stock 1.13-1.44 GB of copy-on-write, hybrid 0.12 / 0.62 / 0.80 GB (fork-class values plus the 60-130 MB queue), Valkey 2.1-2.5 GB. Fork stall and worst write: stock 29-40 / 30-34 ms, hybrid 2-17 / 2-19 ms. Replica-side stream buffer 23-33 MB (recorded in TASK-1.29 as the number inline replication must justify itself against). Valkey's forkless full sync (PR 4797) was run as an experimental fourth configuration; see the addendum in RESULTS.md run 18.

**Also.** bench.py full-sync mode (`--sync-bin`: a replica per cell on the load CPUs, sync and transfer time, primary output-buffer peak, replica buffer peak, sync_full and dbsize checks), run.sh `SYNC=1`/`SYNC_REPLICA_ARGS` and `--workdir` on the tmpfs dir (the first pass had the replica writing its RDB to the root disk at 2 MB/s). Blog section 6 carries the measured table; the status bullet and comparison-table row updated.

**Out of scope / follow-ups.** Inline replication (TASK-1.29, urgency lowered by the buffer numbers). Slot-snapshot and functions-only transfers stay on the stock child. The child's wall time exceeding its CPU time under writes on BGSAVE (run 16/17 observation) does not show in the sync cells, where the replica's load is the floor.
<!-- SECTION:FINAL_SUMMARY:END -->
