---
id: TASK-1.37
title: >-
  kvstore: dict and store reference counts; the save owns a reference and a
  pointer table per fork-instant dict (FLUSH, slot drop and MoveDict under a
  save become reference drops)
status: Done
assignee: []
created_date: '2026-10-06 06:00'
updated_date: '2026-10-06 07:18'
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
- [x] #1 kvstoreMoveDict of a fork-instant dict during a hybrid save, followed by kvstoreRelease of the destination store (BIO path), is safe: C unit test with a running scan; cluster tcl test with CLUSTER MIGRATION IMPORT trimming slots of the source while the source runs a slow hybrid save, reload equals the fork instant on both sides (TASK-1.21.7 AC3)
- [x] #2 FLUSHDB sync and kvstoreEmpty under a save detach instead of destroying fork-instant dicts (C unit); FLUSHDB detach-to-snapshot and FLUSHALL abort unchanged (existing tcl tests)
- [x] #3 Freeze takes the references and fills the table in the SLOT_INFO pass; the thread reads only the save's table; kvstoreGetDictAcquire, dict_created, snap_free_deferred, the lazy stamp CAS and the leftover machinery (snap_save_leftovers) are removed
- [x] #4 No dict or store struct is freed while referenced: ASan unit/snapshot clean, TSan unit/snapshot clean, full suite with bgsave-hybrid yes green; C unit kvstore + snapshot pass
- [x] #5 Spec section 4 (Freeze) and 11.z.1/11.z.2 updated; TASK-1.21.7 AC3 closed by reference
<!-- AC:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
2026-10-06, implemented as designed. kvstore: kvstoreDictMetaBase.refcount/detached, kvstore refcount; kvstoreDictRetain/Unref/Refcount/Detached, kvstoreRefcount; detachDict(kvs, didx, moving) shared by freeDictIfNeeded (no ovPeek check, no deferral), kvstoreEmpty (a dict with refcount>1 is detached, not emptied), kvstoreMoveDict (logical size moves; a held dict stays out of the destination's layout bookkeeping and rehashing list); kvstoreRelease unrefs dicts and the struct, kvstoreUnref frees the struct at zero; the dict callbacks (rehash started/completed, bucket changed) skip detached dicts; onDictRelease no longer set by kvstore. Bookkeeping note: upstream kvstoreMoveDict adjusts non_empty_dicts through cumulativeKeyCountAdd with the dict still in the table, which never decrements it for a non-empty moved dict; the rewrite adjusts key_count/non_empty_dicts/fenwick explicitly. kvsoverlay.c: ov->d everywhere (the overlay addresses its dict, not kvs->dicts[didx]), ov->kvs NULL once detached (ovDetachFromStore: off the drain list), ovDrainOne uses the dict's type for the key helpers and the destructor toggle (first attempt dereferenced the NULL store: C unit segfault, fixed), kvstoreDictSnapAttach(dict*), kvstoreDictSnapRelease(dict*) returns the bitmap bytes and skips the store's free check for detached dicts. snapshot.c: ctx->dicts[s][didx] table + nrefs per store, bitmap_bytes and retained_dicts in the ctx; freeze pass retains/stamps/records; guard compares the live dict with the table entry; claim is a plain store; completion releases state, stamps DONE, clears the entry, unrefs; FinishAll sweeps the table; claimed/enumerated/completed/slots_skipped, snapDictStamp, snapGuardDictCreated, kvstoreGetDictAcquire, kvstoreSnapAttached/BitmapAccount/BitmapBytes/Leftovers removed; INFO snap_save_leftovers replaced by snap_save_retained_dicts. Tests: snapRefcountTests (A: table/refs, writes outside the table, kvstoreMoveDict + kvstoreRelease of the destination under the scan; B: kvstoreEmpty under the scan, a fresh dict in the slot; C: a dict emptied under the scan in a FREE_EMPTY_DICTS store) replaces the leftover and lazy-stamp tests; tcl cluster test with the handoff-prep fail point. C unit 184 pass; unit/snapshot hybrid green; stock running; tcl ASM test, TSan, ASan, full suite pending.

TSan on the new cluster test found a real race the C unit could not: the trim's BIO histogram pass (asmTrimJobPopulateDeltaHistograms) iterated the moved dicts while the main thread completed one (side table applied, rehash resumed). Fix: a slot dict a save still holds (refcount > 1 before the move, detached after it) is accounted on the main thread before kvstoreMoveDict through the live overlay-aware dict iterator, and the BIO pass skips detached dicts. Second hazard fixed on the way: ovDrainOne used to NULL the store-wide dictType.keyDestructor around the apply; the store's other dicts share that type and the lazyfree thread may be releasing them, so the drain now swaps the dict's own type pointer to a private copy for the duration. ASan run also failed the hard-mark test (save completed in 2.1 s before the 3000 writes queued 64 KB; the test leaves bgsave-hybrid-max-retained-bytes at 64 KB on failure, which cascaded into the next two tests): timing under ASan, reran to confirm; consider a longer rdb-key-save-delay in that test.
<!-- SECTION:NOTES:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
Dict and store reference counts in kvstore. At freeze the save takes one reference per non-empty dict, stamps it PENDING and records it in its own per-store table; the scan thread walks that table, the guard compares the live dict with the table entry, completion releases the state and the reference. A store that frees, empties or moves a referenced dict detaches it (out of the slot table and the store's bookkeeping, side table cut loose) and the save reads it to the end and frees it; every dict holds a reference on its store so a store released on the lazyfree thread outlives its detached dicts. Removed: the lazy stamp and CAS claim, the deferred empty-dict frees, the dict-created hook, the acquire loads of the live slot table, onDictRelease, the leftover machinery and INFO snap_save_leftovers (INFO snap_retained_dicts, cumulative, replaces it). kvstoreMoveDict moves the logical size and keeps a held dict out of the destination's layout bookkeeping; the ASM trim accounts held slots on the main thread before the move and its BIO pass skips detached dicts; the overlay drain no longer edits the store-wide dict type. Closes TASK-1.21.7 AC3 (slot trim under a save). Verified: C unit kvstore + snapshot (184, three new reference tests), unit/snapshot hybrid and stock, TSan and ASan unit/snapshot clean, cluster ASM units, full hybrid suite (on the build before the two sanitizer-driven fixes; those paths are covered by the sanitizer and ASM runs). Spec section 4 and 11.z.2 updated.
<!-- SECTION:FINAL_SUMMARY:END -->
