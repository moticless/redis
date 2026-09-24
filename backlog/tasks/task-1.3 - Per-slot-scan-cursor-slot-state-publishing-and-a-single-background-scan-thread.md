---
id: TASK-1.3
title: >-
  Per-slot scan cursor, slot state publishing and a single background scan
  thread
status: Done
assignee:
  - moti.cohen@redis.com
created_date: '2026-09-20 17:18'
updated_date: '2026-09-20 19:11'
labels:
  - bgsave
  - persistence
  - kvstore
  - threads
milestone: m-0
dependencies: []
documentation:
  - poc-hybrid-bgsave-spec.md
parent_task_id: TASK-1
priority: high
ordinal: 4000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Spec §4: a monotonic scan cursor tracks each kvstore slot as PENDING, SCANNING (owned by a background thread) or DONE. Transitions are published so the main thread can read a slot's state cheaply on every write. While a slot is SCANNING its dict is rehash-paused and treated as immutable; the background thread dictScans it and dispatches each entry (SNAP_FORK -> kvobj copy, SNAP_NOFORK -> serialize into staging buffer) but is strictly read-only on the keyspace (invariant 2). Implementation lives in src/snapshot.c with 'snap'-prefixed functions (e.g. snapScanThreadMain, snapSlotState, snapSlotComplete).

Scope for M1: one background thread that walks all dbs and all slots in sequence; per-slot state array with atomic publication; dictPauseRehashing on a slot's dict for the SCANNING window and resume at completion; a completion signal per slot that the main thread consumes to run its merge (merge itself is another task); clean cancellation (save abort must stop the thread promptly and release paused dicts). Dispatch callbacks are pluggable so the channel/serializer task can hook in; each dispatched entry carries its dbid.

Design gap to resolve here: in standalone (non-cluster) mode each db's kvstore has ONE dict, so 'per-slot' collapses to one slot per db and the clone-on-write burden never shrinks during the save. Decide whether the M1 cursor is per-kvstore-dict (simple, acceptable for M1 if documented) or per bucket range within a dict (dictScan cursor ranges, which makes SCANNING granularity independent of cluster mode and is a prerequisite for M2 parallelism in standalone mode). Record the decision and its consequences in task notes.

Also verify that dictScan concurrent with main-thread reads is safe when pauserehash > 0 (main thread must not call _dictRehashStep, dictExpand or dictShrink on a SCANNING dict; inserts/deletes to a SCANNING dict must be impossible because they go to the temp dict).
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 Each kvstore slot (or chosen scan unit) has a published state PENDING/SCANNING/DONE readable from the main thread without locks, and transitions are monotonic
- [x] #2 A single background thread scans every db and slot in sequence, invoking pluggable per-entry dispatch callbacks, and signals slot completion to the main thread
- [x] #3 Rehashing on a slot's dict is paused for the whole SCANNING window and resumed at completion, including on abort
- [x] #4 Abort stops the scan thread promptly (bounded by one slot or bucket range) and leaves no dict rehash-paused
- [x] #5 The background thread performs no writes to any dict or kvobj, verified by code review notes and a ThreadSanitizer run of the scan under concurrent reads
- [x] #6 The standalone-vs-cluster slot granularity decision is recorded in task notes with its impact on capture-burden shrinkage
- [x] #7 Unit or tcl tests cover: full scan enumerates exactly the fork-instant key set; abort mid-scan; scan of empty dbs and of a dict mid-rehash at scan start
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
## Implementation plan (TASK-1.3)

### Findings that shape the design
- `dictScan()` pauses/resumes rehashing on the dict it scans (a non-atomic RMW on `d->pauserehash`). The main thread does the same from SCAN/safe iterators, so a background thread must never call dictScan on a live keyspace dict. Add `dictScanNoPause()` to dict.c (refactor dictScanDefrag into an internal with a `pause` flag) for callers that guarantee the dict is externally frozen.
- The cron resize/rehash block in databasesCron is already skipped while a child process exists, and dictRehashMicroseconds/_dictRehashStep honour `pauserehash`. So a keyspace dict is immutable during the save iff (a) the main thread never inserts/deletes into it (writes go to the temp dict, TASK-1.6) and (b) rehash is paused by the main thread. `dictPauseAutoResize` is added as belt-and-braces.
- Standalone mode has one dict per db. M1 scan unit = one kvstore dict (didx). The single predicate the write hooks will call is `snapKeyIsOwed(db, didx)`; refining the unit to bucket ranges later (cursor-order comparison of the key's bucket against the scan cursor, valid because the dict is frozen) changes only that predicate and the state array, not the hooks. Recorded as the M1 decision.

### Design (src/snapshot.c / snapshot.h, all `snap`-prefixed)
- Slot states (monotonic): `SNAP_SLOT_PENDING` -> `SNAP_SLOT_SCANNING` (bg owns) -> `SNAP_SLOT_SCANNED` (bg finished, main thread has not merged yet) -> `SNAP_SLOT_DONE` (merged, dict unfrozen). Owed = state != DONE. Stored per db as an array of `redisAtomic uint8_t` of length kvstoreNumDicts(db->keys) (1 standalone, 16384 cluster). Writers: bg sets SCANNING/SCANNED with release stores; main sets PENDING at start and DONE at merge.
- `snapScanCtx` (one per save): per-db state arrays, dispatch callback + privdata, completion queue (mutex + list of {dbid, didx}) + eventNotifier for main-thread wakeup, abort flag (atomic), pthread handle, counters (slots total/done) for INFO.
- `snapScanStart(ctx, dispatch, priv)` [main thread]: for every db and every existing dict of db->keys: dictPauseRehashing + dictPauseAutoResize, state=PENDING; creates the eventNotifier and registers an AE read handler on server.el when an event loop exists (unit test drains manually); spawns the thread (makeThreadKillable, redis_set_thread_title "snap_scan").
- Thread main: for db in order, for didx in order: dict = kvstoreGetDict(); if NULL or empty -> mark SCANNED immediately; else state=SCANNING, loop `dictScanNoPause(d, cursor, snapScanEntry, ctx)` until cursor 0; check abort flag between cursor steps; state=SCANNED; push completion; triggerEventNotifier. When all slots are done (or aborted) set `finished` and exit. The per-entry callback decodes `dictGetKV(de)` and calls the pluggable dispatch(priv, dbid, didx, kvobj*) so TASK-1.8 can classify and ship; the thread never writes to any dict or kvobj.
- Main thread completion handling: `snapScanHandleCompletions(ctx, mergeFn)` drains the queue; for each slot calls mergeFn (TASK-1.6; M1 stub) then dictResumeRehashing/ResumeAutoResize and state=DONE.
- `snapScanAbort(ctx)`: set abort flag, join thread, then treat every non-DONE slot as completed (merge + unfreeze). `snapScanFinish(ctx)` joins and frees. Both are main-thread only.
- Accessors: `snapScanSlotState(ctx, dbid, didx)`, `snapKeyIsOwed(ctx, dbid, didx)`, progress counters.

### Tests
- C unit test (extends snapshotTest): build 2 kvstores of 4 dicts each with a no_value dictType keyed by sds (mirrors dbDictType layout for dictGetKV), 1 empty dict, 1 dict left mid-rehash (insert past the expand threshold, then dictRehash one step), then run the full scan from the thread while the main test thread performs concurrent dictFind lookups; assert the dispatched set equals the inserted set exactly (count and membership via a check-off array), each slot is reported exactly once, states end DONE, pauserehash/pauseAutoResize are back to 0. Abort test: dispatch callback sleeps, call abort after the first slot, assert the thread exits promptly and all dicts are unpaused. ThreadSanitizer: build with `make SANITIZER=thread` and run the test (recorded in notes).
- tcl: not yet applicable (no server entry point until TASK-1.9).

### Files
src/dict.c/.h (dictScanNoPause), src/snapshot.c/.h, src/server.h (snap scan ctx pointer field for later tasks, optional).
<!-- SECTION:PLAN:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
2026-09-20 implementation notes (TASK-1.3)

Granularity decision (AC #6): the M1 scan unit is one kvstore dict (didx). Cluster mode: 16384 slots per db, so owed-ness shrinks slot by slot as the spec intends. Standalone mode: a db has ONE dict, so the whole db stays owed until the thread finishes it and the clone-on-write burden does not shrink during the save; the temp dict for that db captures every write of the save. Because the write hooks (TASK-1.7) only call snapScanKeyIsOwed(store, didx), refining the unit to bucket ranges later (compare the key's bucket index in dictScan reverse-cursor order with a published cursor; valid because the dict is frozen) is confined to snapshot.c. M2 parallelism in standalone mode needs that refinement.

Why dictScanNoPause: dictScan() increments/decrements d->pauserehash around each call. That is a plain int16 RMW; the owner (main) thread does the same from SCAN and safe iterators, so a second thread calling dictScan would corrupt the counter. The thread therefore uses the new dictScanNoPause(), and the MAIN thread pauses rehash (and auto-resize) on every existing keyspace dict at snapScanStart() and resumes per slot at completion. Cron-driven resize/rehash in databasesCron are already skipped while a child process exists, so during a real save the only remaining mutators of a frozen dict are inserts/deletes, which TASK-1.6/1.7 route to the temp dict.

Completion path: SPSC ring of (store,didx) sized slots_total, written by the thread with a release store, read by the main thread with an acquire load; plus an eventNotifier so the event loop wakes (snapScanAeHandler ready for TASK-1.9 to register). No allocation and no locks on the thread.

Tests: 26 scan checks in `redis-server test snapshot`: 2 stores x 4 dicts (1, empty/never-allocated, 5000, mid-rehash / 100,100,100, empty) = 5366 keys; every key dispatched exactly once, dispatch always observed its slot SCANNING, ~5-8k concurrent owner-thread dictFind lookups during the scan all hit, the mid-rehash dict was still mid-rehash when its slot completed (the scan never rehashed), all slots DONE and all dicts unfrozen; abort after ~50 keys returned in 0 ms with 2 slots completed normally and 6 reported aborted; two empty stores (9 slots) complete with zero dispatches. ThreadSanitizer build (make SANITIZER=thread MALLOC=libc REDIS_CFLAGS=-DREDIS_TEST): 28 checks pass, 0 ThreadSanitizer warnings. Test bugs found on the way: the kvstore test dictType compares keys by pointer (added a strcmp comparator), and the 'still mid-rehash' check must run inside the slot-done hook because the test's own lookups rehash the dict once it is unfrozen.

Committed as 6871810d4 on snapshot-hybrid-bgsave (dict.c/.h, snapshot.c/.h). jemalloc REDIS_TEST build: 49/49 unit checks pass.
<!-- SECTION:NOTES:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
## Summary

Implements the background per-slot scan of spec §4 in `src/snapshot.c` (commit on branch `snapshot-hybrid-bgsave`, local only), plus a read-only scan primitive in dict.c.

### What changed
- **Slot states** `SNAP_SLOT_PENDING/SCANNING/SCANNED/DONE`, monotonic, per (store, didx), published with atomics. `snapScanKeyIsOwed()` and `snapScanSlotState()` are the lock-free queries the write hooks will use.
- **`snapScanCreate/Start`** (main thread): freezes every existing keyspace dict (`dictPauseRehashing` + `dictPauseAutoResize`), publishes PENDING, spawns the `snap_scan` thread.
- **Scan thread**: walks all stores and dicts in order, dispatches every fork-instant kvobj via a pluggable callback (read-only on the keyspace), publishes SCANNING/SCANNED, hands completed slots to the main thread over an SPSC ring + eventNotifier, checks the abort flag every cursor step.
- **`snapScanHandleCompletions`** (main thread): runs the slot-done hook (temp-dict merge point for TASK-1.6), unfreezes the dict, publishes DONE. `snapScanAeHandler` is ready for event-loop registration in TASK-1.9. `snapScanAbort` joins the thread and completes remaining slots as aborted; `snapScanRelease` frees.
- **`dictScanNoPause()`** in dict.c: `dictScan` without the `pauserehash` RMW, for callers that freeze the dict externally. `dictScanDefrag` refactored into a shared internal.

### Decisions
- M1 scan unit = one kvstore dict; in standalone mode the whole db stays owed until scanned. Bucket-range refinement is confined to snapshot.c (details in notes).

### Tests
- 26 new checks in `redis-server test snapshot` (exact enumeration incl. empty and mid-rehash dicts, concurrent lookups, abort, empty stores); all 49 checks pass on the jemalloc build.
- ThreadSanitizer (`SANITIZER=thread MALLOC=libc`): 28 checks pass, 0 warnings.

### Follow-ups
- Register `snapScanAeHandler` and wire the fork instant in TASK-1.9; merge hook in TASK-1.6; dispatch/classify in TASK-1.8.
<!-- SECTION:FINAL_SUMMARY:END -->
