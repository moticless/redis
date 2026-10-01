---
id: TASK-1.24
title: >-
  FLUSHDB and SWAPDB during a hybrid save: detach the frozen kvstore instead of
  aborting the save
status: Done
assignee:
  - '@Claude'
created_date: '2026-09-25 14:14'
updated_date: '2026-10-01 06:01'
labels:
  - bgsave
  - persistence
  - db
milestone: m-3
dependencies: []
references:
  - >-
    ~/git/dragonfly src/server/snapshot.cc (db_array_ intrusive_ptr copy of the
    table array), src/server/db_slice.cc FlushDbIndexes
documentation:
  - poc-hybrid-bgsave-spec.md
  - utils/snap-bench/RESULTS.md
parent_task_id: TASK-1
priority: high
ordinal: 12150
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Today emptyData() and dbSwapDatabases() abort a running hybrid save (snapSaveAbort + killRDBChild) because the scan thread reads the live dicts. Stock keeps its child alive on FLUSHDB and SWAPDB (only FLUSHALL and a replica's full-sync load kill it), so a hybrid save overlapping a FLUSHDB or SWAPDB produces no dump and a disk-based full sync fed by it fails and is retried. Valkey's forkless save (src/bgiteration.c handleFlushdb) also cancels a consistent iteration on FLUSHDB, but keeps going on SWAPDB by remapping database ids (fullScanIteratorSwapDb).

Do better than both: on FLUSHDB during a save, swap in a fresh empty kvstore for the db in O(1) and hand the frozen kvstore, with its overlay and temp dict, to the save; the scan thread finishes walking it and the save frees it at completion (FLUSHDB ASYNC defers the free to that point). Readers and writers see the empty db immediately, so FLUSHDB stays atomic; the snapshot keeps the fork-instant contents, as stock. On SWAPDB nothing needs detaching: the overlay lives in the kvstore, but the hooks' privdata is a redisDb pointer used to derive the store index and slot state, so after a swap kvstore B would be looked up under the wrong index. Key the hooks and snapKvobjIsOwed by store index (or store it in the overlay) and SWAPDB needs no abort: keys are serialized under their fork-instant db id, which is stock and Valkey semantics. Keep aborting on FLUSHALL, on the replica's full-sync load and on DEBUG RELOAD/LOADAOF (stock kills the child there too). Do not block clients: blocking the issuer breaks FLUSHDB atomicity against other clients, blocking everyone pauses the server for the save duration.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 FLUSHDB during a hybrid save: the save completes, the dump equals the fork-instant digest, the live db is empty right after the command returns, and the detached kvstore's memory is released once the save completes (used_memory back below the pre-flush level, lazyfree queue empty), for both sync and ASYNC
- [x] #2 SWAPDB during a hybrid save: the save completes, keys appear in the dump under their fork-instant db id, live reads and writes after the swap go to the right db (deterministic test: 3000 and 100 keys swapped mid-save, writes through both ids incl. an in-place write on an owed key of the swapped store; the randomized harness keeps covering single-db writes)
- [x] #3 FLUSHALL and a replica full-sync load during a save still abort it as stock kills its child; a test covers each (FLUSHALL ASYNC harness test; new replica-side test in the diskless block)
- [x] #4 The stock aof-multi-part in-progress assertion adapted in 2b3e61d64 stays adapted: it exercises DEBUG LOADAOF, which empties every db and still aborts; nothing in the stock suite asserts a child survives FLUSHDB or SWAPDB
- [x] #5 Spec section 4 and 11.z.1 describe the detach semantics
<!-- AC:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
2026-09-29 precedent: Dragonfly's SliceSnapshot holds its own refcounted copy of the DbTable array; FLUSHALL/FLUSHDB swaps fresh tables into the slice, the snapshot keeps serializing the old, now-frozen tables, and their memory is freed when the snapshot ends. New keys in the new tables carry versions above the cut so the pre-update hook skips them. This is the 'detach the frozen kvstore' design of this task, proven in production; the main difference for us is that the scan thread's window and the funnel must be told the frozen kvstore is no longer the live one (no more emissions for it, all its buckets belong to the scan thread).

Implemented 2026-10-01 (uncommitted, verification in progress), on top of TASK-1.21.3 (overlay in the dict metadata) and TASK-1.21.6 (per-store guard), which made it small. (1) Stores versus dbs: the overlay hooks' privdata is now the store index (snapStorePriv {s}), the db id a kvstore had at the fork instant; the owning redisDb is resolved at call time (snapStoreOwnerDb, used only by the clone hook for expires/subexpires bookkeeping, NULL for a detached store) and snapKvobjIsOwed maps db->keys back to its store (snapStoreIndexOf; -1 for a keyspace created after the fork: nothing owed). (2) SWAPDB: the abort in dbSwapDatabases is gone; a swap moves a scanned kvstore to the other db id, keys are serialized under the fork-instant id (stock and Valkey semantics), writes through the new id take the guard/overlay path of that kvstore. (3) FLUSHDB (sync and ASYNC): emptyDbStructure calls emptyDbDetachToSnapshot (lazyfree.c) when the save owns the db's kvstore: the db gets fresh structures at once (shared helper dbSwapInFreshStructures with emptyDbAsync) and the fork-instant ones go to the save (snapSaveDetachDb); the scan thread keeps walking them, nobody else can reach them; when the store's last slot completes (new scan callback store_done -> snapSaveStoreDone) they are freed on BIO through emptyDbDataAsync, as the ASYNC flush would. A synchronously flushed db therefore releases its memory at the end of the save, not inside the command (documented). emptyData aborts only for dbnum == -1 (FLUSHALL, DEBUG RELOAD/LOADAOF, replica full-sync load), as stock kills its child there. (4) Tests: the old 'FLUSHDB and SWAPDB during the save abort it' became 'FLUSHDB during the save empties the live db at once and the save completes with the fork-instant keys' (sync and async: dbsize 0 immediately, save still in progress, a write after the flush lands in the live db and not in the dump, memory released after the save, reload equals the pre-flush digest) and 'SWAPDB during the save keeps the save running, keys stay under their fork-instant db' (3000 keys in db 9 and 100 in db 10, swap mid-save, writes through both ids incl. an in-place write on an owed key of the swapped store, reload digest equal, sizes back in the fork-instant dbs). Both pass; C unit 166/166. Spec section 4 (new paragraph), 11.z.1 limitation 5, 11.z.4 item 4 and the blog status bullet updated. AC4 (aof-multi-part assertion): DEBUG LOADAOF empties every db, so it still aborts; the assertion cannot be restored for it, nothing to restore for FLUSHDB/SWAPDB (no stock test asserts a child survives them). Pending: full unit/snapshot both modes, TSan, ASan, commit.
<!-- SECTION:NOTES:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
FLUSHDB and SWAPDB no longer abort a running hybrid save; FLUSHALL, DEBUG RELOAD/LOADAOF and a replica's full-sync load still do, as stock kills its child there.

**Design.** The save scans kvstores, indexed by the db id they had at the fork instant. The overlay hooks' privdata became that store index (`snapStorePriv`), the owning redisDb is resolved at call time (`snapStoreOwnerDb`, only the clone hook needs it, for expires/subexpires bookkeeping), and `snapKvobjIsOwed` maps a db's kvstore back to its store (`snapStoreIndexOf`, -1 for a keyspace created after the fork).
- **SWAPDB** needs nothing more: the swap moves a scanned kvstore to the other db id, keys are serialized under the fork-instant id (stock and Valkey semantics), writes through the new id take the guard and overlay path of that kvstore. The abort in `dbSwapDatabases` is gone.
- **FLUSHDB** (sync and ASYNC): `emptyDbStructure` calls `emptyDbDetachToSnapshot` (lazyfree.c) when the save owns the db's kvstore. The db gets fresh, empty structures at once (helper `dbSwapInFreshStructures`, shared with `emptyDbAsync`) and the fork-instant ones go to the save (`snapSaveDetachDb`); the scan thread keeps walking them and nobody else can reach them. When the store's last slot completes, the scan's new `store_done` callback (`snapSaveStoreDone`) frees them on BIO through `emptyDbDataAsync`, as the ASYNC flush would; the store pointer is cleared so INFO and lookups never touch a freed kvstore. A synchronously flushed db therefore releases its memory at the end of the save, not inside the command. FLUSHDB stays atomic for every client and a disk-based full sync fed by the save survives it. Dragonfly does the same with its refcounted table array.
- `emptyData` aborts only for dbnum == -1. `snapSaveAbort` clears the store pointers too (the aborting FLUSHALL ASYNC or full-sync load frees the kvstores on BIO while the save object waits for the child). Both clears were found by the sanitizers: TSan and ASan reported the heap-use-after-free in `kvstoreOverlayPending` from INFO's side-entry counter in the first two versions.

**Tests** (unit/snapshot): 'FLUSHDB during the save empties the live db at once and the save completes with the fork-instant keys' (sync and async: dbsize 0 immediately, save still running, a write after the flush lands in the live db and not in the dump, memory released after the save, reload equals the pre-flush digest); 'SWAPDB during the save keeps the save running, keys stay under their fork-instant db'; 'a replica's own running hybrid save is aborted by the full-sync load' (both rdb-channel shapes); the FLUSHALL ASYNC abort test unchanged.

**Verification.** C unit 166/166; unit/snapshot with `--config bgsave-hybrid yes` (173 s) and stock mode (241 s); TSan 55/55 no report; ASan 55/55 no report (one earlier ASan run hit the harness timeout after 35 tests; the two tests that follow pass in isolation and in the full rerun, so it is recorded as a flake, not reproduced).

**Docs.** Spec section 4 (new FLUSHDB/SWAPDB paragraph, abort list), 11.z.1 limitation 5, 11.z.4 item 4; blog status bullet.

**Also in the same commit.** Run 21 (RESULTS.md, run21.csv, run21s.csv): mixed1m at 4 GB with the 16 MB queue default against upstream, BGSAVE and full sync; the blog's 128 MB footnotes now point at it.
<!-- SECTION:FINAL_SUMMARY:END -->
