---
id: TASK-1.7
title: Clone-on-write hooks across every keyspace write path (invariant 1)
status: Done
assignee:
  - moti.cohen@redis.com
created_date: '2026-09-20 17:20'
updated_date: '2026-09-20 21:43'
labels:
  - bgsave
  - persistence
  - db
  - correctness
milestone: m-0
dependencies:
  - TASK-1.1
  - TASK-1.4
  - TASK-1.6
documentation:
  - poc-hybrid-bgsave-spec.md
  - poc-hybrid-bgsave-write-path-audit.md
parent_task_id: TASK-1
priority: high
ordinal: 8000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Spec §4, §5 and §7 invariant 1: during a save, the FIRST modification of a key whose slot is still PENDING or SCANNING must clone (new kvobj header in the SNAP_NOFORK arena, plus a new value in the arena matching its new class if the value changes; header-only for SNAP_FORK header mutations), apply the change to the clone, insert it into the per-slot temp dict (TASK-1.6), and leave the fork-instant original untouched in the scanned dict. Subsequent modifications act on the clone in place. Deletes tombstone. Adds go to the temp dict. Keys in DONE slots are modified in place as today. A single missed path means the background thread copies a torn header or serializes a mutated value: silent snapshot corruption plus a data race.

Because ALL kvobj headers are SNAP_NOFORK and copied live by the background thread, header mutations on owed SNAP_FORK keys (EXPIRE/PERSIST, metadata slots, LRU/LFU clock) must also clone; only SNAP_FORK VALUE mutations may proceed in place (the fork holds their CoW snapshot), and their delete/modify still tombstones/records in the temp dict so the scanned dict entry survives until scanned.

Scope: implement the routing for every path listed in the write-path audit (TASK-1.1), by hooking the smallest set of shared primitives that all commands funnel through (dbAdd/dbOverwrite/setKey/dbDelete/dbGenericDelete, the lookupKeyWrite path, setExpire/removeExpire, the per-type 'get object for write' helpers and encoding conversion functions, hash field TTL updates, RESTORE/COPY/MOVE/RENAME, module key handles opened for write, lazy expiry deletion, active expiry deletion, eviction deletion, DEBUG mutators). Hooks call into snapshot.c 'snap'-prefixed primitives.

Specific gaps to close: (1) lookupKey updates the LRU/LFU clock in the kvobj header on every READ - decide between atomic word access with the background copier reading it atomically, skipping the update for owed keys during a save, or dropping IDLE/FREQ fidelity for the duration of the save; document and cover with the harness under allkeys-lru and allkeys-lfu. (2) Active defrag relocates kvobjs and dict entries in place; suspend it for owed slots (or for the whole save in M1). (3) FLUSHALL, FLUSHDB and SWAPDB abort the save (spec §7.5). (4) Encoding conversion of an owed SNAP_NOFORK value to a SNAP_FORK encoding must produce the clone value in forkable memory and never place a SNAP_FORK value in the SNAP_NOFORK arena.

Prove the coverage with the verification harness (TASK-1.10). Per spec §10, if any path cannot honour the invariant without unacceptable cost, record it as a blocker in task notes and in the M1 report rather than working around it.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 Every path in the write-path audit document is routed through the clone/tombstone/add primitives in snapshot.c, and the audit document is updated with the function where each hook lives
- [ ] #2 A DEBUG-build assertion fires if any code writes to a kvobj header (any class) or a SNAP_NOFORK value in a slot that is PENDING or SCANNING, other than through the clone path; running the full tcl suite with a save forced to stay in progress triggers no assertion
- [x] #3 Header-only mutations (EXPIRE/PERSIST/metadata) of owed SNAP_FORK keys clone the header and share the value, covered by tests
- [x] #4 LRU/LFU clock updates on reads of owed keys are handled per the recorded decision and the produced RDB reloads correctly under allkeys-lru and allkeys-lfu policies
- [x] #5 Active defrag does not relocate any kvobj or dict entry in an owed slot while a save is in progress, verified by a test with activedefrag yes and a forced-slow save
- [x] #6 FLUSHALL, FLUSHDB (including ASYNC variants) and SWAPDB during a save abort it cleanly, the server stays consistent, and the abort is reported in INFO persistence and the server log
- [x] #7 Encoding conversions of owed SNAP_NOFORK values during a save place the clone value in forkable memory and the no-SNAP_FORK-in-arena assertion holds throughout the tcl suite
- [ ] #8 ThreadSanitizer run of the harness workload reports no data races between the main thread and the background scan thread
- [x] #9 Any path that cannot satisfy invariant 1 or 2 is recorded as a blocker in task notes
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
## Implementation plan (TASK-1.7)

Most of the routing was delivered structurally by the kvstore overlay (TASK-1.6) and the save orchestration (TASK-1.9): writers obtain clones through `kvstoreDictFindLink` / `TwoPhaseUnlinkFind` / `AddRaw` / `SetAtLink` / `Delete`, reads never clone (`lookupKey` write-intent split), FLUSH*/SWAPDB abort, active hash-field expiry is suspended and lazy field expiry never mutates an owed key. What remains here:

1. **Debug-build assertion (AC #2)**: `snapAssertNotFrozenOriginal(db, kv)` checked (DEBUG_ASSERTIONS) at the write funnels `dbSetValue`, `setExpireByLink`, `removeExpire`, `dbGenericDelete`, `keyModified`, `dbUnshareStringValueByLink` and in `snapPlaceValue`: a kvobj that is the frozen dict's own entry for its key while the slot is owed must never reach a mutator. Run the tcl snapshot tests with -DDEBUG_ASSERTIONS.
2. **LRU/LFU decision (AC #4)**: NOOP by construction. `lookupKey`'s clock update is already skipped while a child process exists (the hybrid save forks), `updateLRM` runs on the kvobj a WRITER holds (a clone for owed keys), RESTORE/`RM_SetLRU` act on new/cloned objects. Tests: hybrid saves under `maxmemory-policy allkeys-lru` and `allkeys-lfu` with reads during the save; the file reloads to the pre-save digest and reads keep working.
3. **Defrag (AC #5)**: gated by `hasActiveChildProcess()` (activeDefragCycle and the timer proc). Test: `activedefrag yes` + `active-defrag-ignore-bytes 1` + `active-defrag-threshold-lower 0` with a slow save and writes; no crash, digest intact.
4. **Conversions during a save (AC #7)**: extend the writes-during-save test with listpack->hashtable (HSET 600), intset->listpack->hashtable (SADD), listpack->skiplist (ZADD 300), listpack->quicklist (RPUSH 2000, adoption on a clone) and quicklist->listpack (LPOP) on owed keys; placement report stays at 0 violations and the file keeps the fork instant.
5. **Audit document (AC #1)**: append a "Hook locations" section mapping every treatment to the implemented function.
6. TSan of the harness workload (AC #8): not possible with the arena (sanitizer builds force libc); recorded as a limitation, with the kvstore-level TSan run as the substitute.
7. Blockers (AC #9): recorded in notes (eviction under a save, ASM slot ops, diskless/AOF in hybrid mode).
<!-- SECTION:PLAN:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
## Result (commit 4c62e8f6a)

- AC #1: routing is structural (kvstore overlay); the audit document gained a "Hook locations" table mapping every §6 treatment to the implementing function.
- AC #2 (partial): `snapKvobjIsFrozenOriginal` + `snapDebugAssertNotFrozenOriginal` at `dbSetValue`, `setExpireByLink`, `removeExpire`, `dbGenericDelete`, `keyModified`, `dbUnshareStringValueByLink`. Built with `-DDEBUG_ASSERTIONS`; the snapshot tcl suite (which keeps saves in progress with rdb-key-save-delay while exercising all write paths) triggers none. NOT done: the full tcl suite with a save forced to stay in progress (no mechanism to force a save under every test; the harness in TASK-1.10 is the substitute).
- AC #4: LRU/LFU clock is a NOOP by construction (lookupKey skips the clock update while a child exists; writers touch their clone). Tests under allkeys-lru and allkeys-lfu pass with reload-equivalence.
- AC #5: defrag gated by `hasActiveChildProcess()`; test with activedefrag yes + slow save passes (placement report 0 violations, digest intact).
- AC #7: conversions on owed clones tested: HSET->hashtable, SADD intset->hashtable, ZADD->skiplist, RPUSH->quicklist, LPOP quicklist->listpack.
- AC #8 NOT met: sanitizer builds force MALLOC=libc so the SNAP_NOFORK arena is inert and the hybrid path is not taken; only the kvstore-level overlay unit tests ran under TSan. Recorded as an M1 limitation.
- AC #9 blockers: eviction during a save only tombstones (memory reclaimed at slot merge); cluster ASM slot import/trim during a save unguarded; diskless sync and AOF rewrite refused in hybrid mode (M2).
<!-- SECTION:NOTES:END -->
