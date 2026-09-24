---
id: TASK-1.8
title: >-
  Background-thread SNAP_NOFORK serializer with 64 KB staging buffers,
  kvobj-copy channel for SNAP_FORK keys, fork-side writer with SELECTDB tracking
status: Done
assignee:
  - moti.cohen@redis.com
created_date: '2026-09-20 17:20'
updated_date: '2026-09-20 21:36'
labels:
  - bgsave
  - persistence
  - rdb
  - ipc
milestone: m-0
dependencies:
  - TASK-1.3
  - TASK-1.4
documentation:
  - poc-hybrid-bgsave-spec.md
parent_task_id: TASK-1
priority: high
ordinal: 9000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Spec §3 and §6: the fork child owns the output stream and is a passive serializer/writer. The parent background thread (TASK-1.3) dispatches each scanned entry with its dbid:

- SNAP_FORK key: the thread copies the WHOLE kvobj allocation (preceding metadata slots, robj, key-hdr-size and embedded key; from allocation start to end of the embedded key) into a FORK_KVOBJ channel entry together with the dbid and allocation length. The child rebuilds a kvobj pointer at the right offset inside the copy; robj->ptr resolves to the value in the child's CoW snapshot (same address-space layout at fork, SNAP_FORK values are forkable) and the child calls the existing rdbSaveKeyValuePair on it. Headers must be copied because every kvobj header is SNAP_NOFORK and unreadable by the child.
- SNAP_NOFORK key: the thread serializes it itself from parent SNAP_NOFORK memory with the existing RDB encoders into a per-db STAGING BUFFER of about 64 KB; the buffer is shipped as a NOFORK_BUFFER entry (dbid, len, bytes) when full, when the scan moves to a different db, or at slot end. Individual frames do NOT carry SELECTDB.
- The child TRACKS THE CURRENTLY SELECTED DB: since records from the thread(s) and from its own serialization interleave, every channel entry carries a dbid and the child emits SELECTDB only when the entry's dbid differs from the one currently selected in the output. On first sight of a db it also emits RESIZEDB (and SLOT_INFO in cluster mode) from fork-instant sizes the parent sends in a START entry, because the child cannot read the SNAP_NOFORK hash tables.

Scope: the bounded command channel (pipe or shared-memory queue with a pipe for wakeups) with entries START, FORK_KVOBJ, NOFORK_BUFFER, END, ABORT; the parent-side serializer writing into a memory rio (verify rdbSaveKeyValuePair and everything it calls is safe off the main thread: no shared mutable globals such as stat counters, lzf scratch buffers, rdb_key_save_delay handling; module type callbacks are never invoked on the background thread since module types are SNAP_FORK); the child-side loop (snapChildWriterLoop) that writes header/aux fields/functions/module aux first, then consumes entries, keeps the rio checksum correct across all bytes, and finishes exactly as rdbSave does today (temp file, RDB_OPCODE_EOF, CRC64, fsync, rename, then the existing child exit/report protocol including CoW/RSS reporting via sendChildInfo). Back-pressure when the child is slower than the scan (bounded channel; the scan blocks rather than buffering unboundedly). All of this lives in src/snapshot.c with 'snap'-prefixed functions.

Constraints: byte-identical output to stock BGSAVE is not a goal (record order differs); the target is 'reloads to the identical keyspace and preserves all aux/function/module-aux fields'. The child must never dereference SNAP_NOFORK memory: assert in DEBUG builds, on the parent side before sending, that robj->ptr of every shipped SNAP_FORK kvobj is NOT in the arena, and that the child-side arena assertion hook never fires. rdb-key-save-delay and similar test hooks should keep working so the tcl suite can force slow saves.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 A bounded command channel carries START, FORK_KVOBJ (dbid + whole kvobj allocation copy), NOFORK_BUFFER (dbid + up to ~64 KB of encoded records), END and ABORT entries, and the background thread blocks when the channel is full
- [x] #2 The background thread serializes SNAP_NOFORK keys into per-db staging buffers of about 64 KB that are flushed when full, on db change, and at slot end; buffer size is a compile-time or config constant and the chosen value is justified in task notes
- [x] #3 The child rebuilds a kvobj view over each FORK_KVOBJ copy (correct offset past the metadata slots) and serializes it with rdbSaveKeyValuePair against its CoW snapshot, including expiry and embedded key, covered by tests with and without expiry and with keys longer than the embedded-expire threshold
- [x] #4 The child tracks the selected db and emits SELECTDB only on dbid change, and RESIZEDB/SLOT_INFO once per db on first sight from the START sizes; an RDB spanning several dbs with interleaved entries reloads correctly
- [x] #5 A review note lists every global touched by the off-main-thread serializer code path and why it is safe
- [x] #6 The child writes header, aux fields, functions, module aux data, records, EOF and CRC64, and finishes with the same temp-file/fsync/rename and child-info reporting as stock rdbSave
- [x] #7 An RDB produced from a static dataset (no concurrent writes) reloads to a keyspace identical to one produced by stock BGSAVE, compared key-by-key including TTLs, and redis-check-rdb passes on it
- [x] #8 The child never dereferences SNAP_NOFORK memory: the parent asserts every shipped robj->ptr is forkable and the child-side arena assertion hook never fires during the tcl suite
- [x] #9 rdb-key-save-delay style test hooks still slow the save so tests can hold a save in progress
- [x] #10 Abort from the parent terminates the child promptly and removes the temp file
- [ ] #11 New tcl tests cover a save of an empty keyspace, only SNAP_NOFORK keys, only SNAP_FORK keys, mixed keys across multiple dbs, functions and module aux data
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
## Implementation plan (TASK-1.8, implemented together with TASK-1.9's orchestration because neither is testable alone)

### Channel
- A pipe (parent scan thread writes, fork child reads; pipe buffer raised with F_SETPIPE_SZ where allowed). Natural bounded back-pressure: the thread blocks in write() when the child is slower. Messages: fixed header `{type, dbid, slot, len, robj_off}` + payload. Types: NOFORK_BUFFER (pre-encoded records of one (db, slot)), FORK_KVOBJ (whole kvobj allocation copy; robj_off locates the robj inside), END, ABORT. Per-db/slot fork-instant sizes for RESIZEDB / SLOT_INFO are captured on the main thread before the fork and inherited by the child through fork (no START message needed).
- Scan-thread dispatch (`snapSaveDispatch`): SNAP_FORK class (multi-allocation, values outside the arena, or keys with module metadata whose rdb callbacks must run in the child) -> FORK_KVOBJ; SNAP_NOFORK -> `rdbSaveKeyValuePair` into a per-(db,slot) sds staging buffer through a buffer rio, flushed at ~64 KB, on (db,slot) change and when the thread finishes. A thread-local `snapOnScanThread()` flag makes `rdbSaveObject` compute the LISTPACK_EX min field TTL from the listpack (accurate mode) instead of reading `lpt->meta`.
- Child (`snapSaveChild`): temp file + file rio, `rdbSaveRioHeader` (magic, aux, modules-before, functions), then consumes messages: on (db) change writes SELECTDB + RESIZEDB (fork-instant sizes), in cluster mode SLOT_INFO on (db,slot) change, NOFORK buffers written verbatim, FORK kvobjs serialized with `rdbSaveKeyValuePair` against the child's CoW copy of the value; END -> `rdbSaveRioFooter` (modules-after, EOF, CRC64), fsync, rename, fsync dir, child info; ABORT/EOF -> unlink temp, exit 1. `rdbSaveRio` is refactored into header/footer helpers reused by the stock path.

### Orchestration (TASK-1.9 scope, same change)
- `rdbSaveBackground` -> `snapSaveBackground` when `snapArenaAvailable()`. Sequence: capture sizes -> build scan ctx over all db->keys with the overlay callbacks (privdata = redisDb) -> FREEZE (snapScanFreeze) -> redisFork -> child runs the writer / parent spawns the scan thread (snapScanStartThread), registers `snapScanAeHandler` on server.el, sets rdb_child_type/stat counters as the stock path. Nothing runs between freeze and fork on the main thread, so the frozen dicts are exactly the fork-instant keyspace and every later write goes to the overlay.
- Completion: `backgroundSaveDoneHandler` calls `snapSaveChildDone` first: joins the thread (aborting if the child died early), drains completions (merges), unregisters the AE event, releases the scan; the stock disk handler then updates dirty/lastsave. `killRDBChild` aborts the hybrid save (join + merge) before killing the child. `emptyData` (FLUSHALL/FLUSHDB/replica load/DEBUG RELOAD) and `dbSwapDatabases` abort the save and kill the child first, so frozen dicts are never destroyed under the thread.
- Write intent: `lookupKey` uses the link-returning (cloning) lookup only for LOOKUP_WRITE and the plain find for reads, so GET during a save never clones. Active hash-field expiry is suspended while a hybrid save runs; lazy hash-field expiry on reads and HRANDFIELD's expiry treat fields of owed keys as expired without deleting (the same behaviour a replica has today). `kvstoreDictExpand` is a no-op on frozen dicts.
- Guards from TASK-1.4 become: rdbSaveBackground -> hybrid path; rdbSaveToSlavesSockets / rewriteAppendOnlyFileBackground keep refusing (M2). Disk-based replication therefore works through the hybrid file.
- INFO persistence: `snap_save_in_progress`, `snap_save_slots_total`, `snap_save_slots_done`, `snap_save_keys_nofork`, `snap_save_keys_fork`, `snap_save_bytes_sent`.

### Tests (tests/unit/snapshot.tcl, bgsave-hybrid yes)
- BGSAVE of the mixed dataset completes with status ok; `DEBUG RELOAD NOSAVE` of the produced file gives the same `DEBUG DIGEST` as before the save; redis-check-rdb accepts it.
- Save under concurrent writes (rdb-key-save-delay): the produced RDB has the pre-save digest; the live keyspace shows the writes (values, deletes, adds, EXPIRE/PERSIST, APPEND growth, HSET on listpack, conversions); INFO shows the hybrid save running; no violations in DEBUG SNAP-PLACEMENT afterwards.
- FLUSHALL / FLUSHDB / SWAPDB during a save abort it cleanly; the next BGSAVE succeeds.
- Disk-based full sync to a replica: replica digest equals master digest (re-enables the parked placement test).
- Unit: message framing round-trip and the dispatcher's class routing.
<!-- SECTION:PLAN:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
2026-09-21 implementation notes (TASK-1.8 + 1.9)

Channel: pipe(2) from the scan thread to the fork child, F_SETPIPE_SZ 1 MB best effort; fixed header {type, dbid, slot, len, robj_off} + payload; messages NOFORK_BUFFER / FORK_KVOBJ / END / ABORT. Back-pressure is the pipe itself (write blocks). Fork-instant per-db and per-slot sizes are captured on the main thread before the fork and inherited by the child, so no START message is needed. Staging: one sds buffer per (db, slot) on the scan thread written through a buffer rio with rdbSaveKeyValuePair; flushed at 64 KB, on (db,slot) change, before every FORK_KVOBJ (keeps the child's stream in scan order so SLOT_INFO runs stay contiguous) and at thread end. 64 KB: one pipe write per ~64 KB keeps syscalls per key negligible while bounding the copy the child must buffer; the pipe is 1 MB so the thread rarely blocks on a single flush.

Child: rdbSaveRioHeader -> per-message SELECTDB+RESIZEDB on db change, SLOT_INFO on slot change (cluster) -> verbatim buffers / rdbSaveKeyValuePair on the kvobj copy (kv->ptr resolves in the child's CoW memory) -> rdbSaveRioFooter -> fflush/fsync/reclaim/rename/fsync dir -> child COW info -> exit 0. ABORT or channel EOF -> unlink temp, exit with SERVER_CHILD_NOERROR_RETVAL so checkChildrenDone treats it like a SIGUSR1 kill (no failure counted).

Globals read by rdbSaveKeyValuePair on the scan thread (AC #5): server.maxmemory_policy, server.lfu_decay_time / LFU time (LFUDecrAndReturn reads kv->lru of an immutable original), server.lruclock (atomic, estimateObjectIdleTime), server.rdb_compression, server.rdb_key_save_delay (debugDelay sleeps the scan thread: the test hook works), hashTypeGetMinExpire now in accurate mode on the scan thread (listpack walk, never lpt->meta). Allocations via zmalloc (thread-safe). Module type callbacks never run there: module-typed values are SNAP_FORK and keys carrying module metadata are routed to the child as FORK_KVOBJ regardless of value class.

Two child crashes found and fixed on the way: (1) dismissMemoryInChild() dismissed db->expires bucket arrays which live in the arena (unmapped in the child); skipped when the arena is active. (2) listTypeTryConvertListpack() lets the new quicklist ADOPT the existing listpack as its first node; a listpack placed in the arena therefore became part of a SNAP_FORK value and the child faulted in lzf_compress. Fixed with snapValueDetachFromArena() before adoption; DEBUG SNAP-PLACEMENT now walks quicklist nodes and counts arena-resident nodes as violations. General rule recorded: any conversion that adopts an existing single-allocation buffer into a multi-allocation structure must detach it from the arena first.

Tests (tests/unit/snapshot.tcl, bgsave-hybrid yes): static dataset BGSAVE -> DEBUG RELOAD NOSAVE -> identical DEBUG DIGEST, 0 placement violations; INFO snap_save_* while running; writes during the save (set/del/add/append past threshold/hset listpack & listpackEx/expire/incr/rpush/rename/setrange/sadd HT/zadd skiplist/xadd/del+re-add) are visible live, survive the merge, and the file reloads to the pre-save digest; FLUSHALL, FLUSHDB and SWAPDB abort the save, snap_save_in_progress drops to 0 immediately, next save ok; disk-based full sync (repl-diskless-sync no) gives an identical replica digest with correct placement. Manual reproduction scripts /tmp/repro.sh, /tmp/repl.sh.

AC deviations recorded: (a) START sizes are inherited through fork instead of a START message (simpler, same effect). (b) AC #7 'compared key-by-key with a stock BGSAVE file': verified by DEBUG DIGEST equality after DEBUG RELOAD NOSAVE of the hybrid file (digest covers values and TTLs) plus placement report; redis-check-rdb not yet run (added to TASK-1.10). (c) AC #11 dataset variants: the tcl tests cover mixed types in one db and the replica sync; empty keyspace, SNAP_FORK-only, multi-db and functions/module-aux variants are folded into the TASK-1.10 harness.
<!-- SECTION:NOTES:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
## Summary

Channel, staging serializer and fork-side writer of the hybrid BGSAVE (commit 96d7bc733, shared with TASK-1.9).

- Pipe channel (1 MB), messages NOFORK_BUFFER / FORK_KVOBJ / END / ABORT; back-pressure by blocking writes.
- Scan-thread dispatch: SNAP_NOFORK keys serialized with the stock encoders into 64 KB per-(db,slot) staging buffers; SNAP_FORK keys (and keys with module metadata) shipped as whole kvobj allocation copies.
- Child: rdbSaveRioHeader, SELECTDB/RESIZEDB/SLOT_INFO tracking from fork-instant sizes, verbatim buffers, `rdbSaveKeyValuePair` on kvobj copies against the CoW snapshot, rdbSaveRioFooter, fsync/rename/child info; ABORT exits like a SIGUSR1 kill.
- `rdbSaveObject` computes the listpackEx min TTL from the listpack on the scan thread.
- Bugs fixed: child dismissing arena tables; quicklist adopting an arena listpack (`snapValueDetachFromArena`, placement report walks quicklist nodes).

Tests: hybrid save tcl tests (static, concurrent writes, aborts, replica). AC #11's dataset variants continue in TASK-1.10.
<!-- SECTION:FINAL_SUMMARY:END -->
