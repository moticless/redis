---
id: TASK-1.24
title: >-
  FLUSHDB and SWAPDB during a hybrid save: detach the frozen kvstore instead of
  aborting the save
status: To Do
assignee: []
created_date: '2026-09-25 14:14'
labels:
  - bgsave
  - persistence
  - db
milestone: m-3
dependencies: []
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
- [ ] #1 FLUSHDB during a hybrid save: the save completes, the dump equals the fork-instant digest, the live db is empty right after the command returns, and the detached kvstore's memory is released once the save completes (INFO used_memory back to baseline), for both sync and ASYNC
- [ ] #2 SWAPDB during a hybrid save: the save completes, keys appear in the dump under their fork-instant db id, live reads and writes after the swap go to the right db and the overlay position logic uses the right slot state (randomized harness variant)
- [ ] #3 FLUSHALL and a replica full-sync load during a save still abort it as stock kills its child; a test covers each
- [ ] #4 The stock aof-multi-part in-progress assertion adapted in 2b3e61d64 is restored for the FLUSHDB/SWAPDB cases it can cover
- [ ] #5 Spec section 4 and 11.z.1 describe the detach semantics
<!-- AC:END -->
