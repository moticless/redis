---
id: TASK-1.37
title: >-
  kvstore: dict and store reference counts; the save owns a reference and a
  pointer table per fork-instant dict (FLUSH, slot drop and MoveDict under a
  save become reference drops)
status: In Progress
assignee: []
created_date: '2026-10-06 06:00'
labels:
  - bgsave
  - cluster
  - kvstore
  - design
dependencies:
  - TASK-1.36
documentation:
  - poc-hybrid-bgsave-spec.md
modified_files:
  - src/kvstore.h
  - src/kvstore.c
  - src/kvsoverlay.c
  - src/snapshot.c
  - src/snapshot.h
  - src/server.c
  - tests/unit/snapshot.tcl
  - poc-hybrid-bgsave-spec.md
parent_task_id: TASK-1
priority: high
ordinal: 33000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
User's idea (2026-10-05/06): a refcount in kvstore so BGSAVE, FLUSH and atomic slot migration (ASM) compose; refined: the main thread takes the references up front, at freeze, before the thread starts.

Why now: ASM trim moves slot dicts out with kvstoreMoveDict into temp kvstores freed on BIO (cluster_asm.c ~3190). Under a hybrid save the moved dict carries this save's metadata into a store with no guard; BIO's kvstoreRelease frees it while the scan thread may still be reading it (use-after-free), and onDictRelease's leftover release runs against the temp store's counters. That is TASK-1.21.7 AC3, open since 2026-10-02.

Design:
- kvstoreDictMetaBase.refcount (atomic; owner store holds 1 from creation, a save holds 1 per fork-instant dict). kvstoreDictRelease(d): decrement, dictRelease at zero. Every dict holds a reference on its store (kvs->refcount, atomic; owner holds 1): kvstoreRelease frees arrays and lists at once, the struct (the dtype the dicts point at, the counters the dict callbacks touch) when the last dict is gone. Decrements run on the main thread (save completion) and on BIO (lazyfree kvstoreRelease), hence atomic; never on the per-command path.
- Freeze: one pass per non-empty store over its dicts: non-empty dict -> retain, stamp (gen, PENDING), store the pointer in the save's per-store table ctx->dicts[s][didx]. This is the pass snapSaveCreate already makes for SLOT_INFO's per-slot counts. The thread walks the table, never the live slot table again: kvstoreGetDictAcquire, the dict_created hook, the release-publish of new dicts, snap_free_deferred, the lazy stamp (size-based PENDING/DONE inference, the CAS claim) and the leftover machinery (snapReleaseLeftover, kvstoreSnapLeftovers, INFO snap_save_leftovers) all go. Claim becomes a plain release store PENDING->SCANNING.
- Guard: a live dict that is not the table's entry for its slot (created or swapped in since the fork) holds nothing of the fork instant: NULL, no stamp needed. Otherwise the state word (always this generation).
- Detach = owner reference drop: freeDictIfNeeded and kvstoreEmpty on a dict with refcount > 1 take it out of the slot table and decrement instead of dictRelease/dictEmpty (the save still reads it; its slot gets a fresh dict on the next add). kvstoreMoveDict keeps the dict's save state (bitmaps, pause) with the dict; its side table is detached from the source store (unlinked from ov_dirty, ov->kvs = NULL, logical key counts settled: the source loses the logical size, the destination gains the real size) and, at the save's completion of the slot, is applied to the dict when a store still owns it or freed when the save was the last holder. Overlay code addresses its dict through ov->d, not kvs->dicts[didx].
- Completion by dict: kvstoreDictSnapRelease(dict *) frees bitmaps (accounted in the scan ctx again), releases the overlay, resumes the layout, stamps DONE, then the save drops its reference (the dict is freed here if its owner already let go). Abort sweeps the table (a pointer per slot). Store done = all table entries released (per-store count), replaces kvstoreSnapAttached.
- Kept as is: the FLUSHDB detach-to-snapshot of whole dbs (TASK-1.24), FLUSHALL/DEBUG RELOAD abort, SWAPDB. Memory: a flushed or trimmed fork-instant dict stays allocated until the thread passes it (same bytes as before the flush, released later); INFO snap_save_retained_dicts.
- Not solved here: concurrent modification (overlay, windows, sections unchanged).
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 kvstoreMoveDict of a fork-instant dict during a hybrid save, followed by kvstoreRelease of the destination store (BIO path), is safe: C unit test with a running scan; cluster tcl test with CLUSTER MIGRATION IMPORT trimming slots of the source while the source runs a slow hybrid save, reload equals the fork instant on both sides (TASK-1.21.7 AC3)
- [ ] #2 FLUSHDB sync and kvstoreEmpty under a save detach instead of destroying fork-instant dicts (C unit); FLUSHDB detach-to-snapshot and FLUSHALL abort unchanged (existing tcl tests)
- [ ] #3 Freeze takes the references and fills the table in the SLOT_INFO pass; the thread reads only the save's table; kvstoreGetDictAcquire, dict_created, snap_free_deferred, the lazy stamp CAS and the leftover machinery (snap_save_leftovers) are removed
- [ ] #4 No dict or store struct is freed while referenced: ASan unit/snapshot clean, TSan unit/snapshot clean, full suite with bgsave-hybrid yes green; C unit kvstore + snapshot pass
- [ ] #5 Spec section 4 (Freeze) and 11.z.1/11.z.2 updated; TASK-1.21.7 AC3 closed by reference
<!-- AC:END -->
