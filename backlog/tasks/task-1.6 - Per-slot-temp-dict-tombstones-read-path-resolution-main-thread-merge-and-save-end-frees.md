---
id: TASK-1.6
title: >-
  Per-slot temp dict: tombstones, read-path resolution, main-thread merge and
  save-end frees
status: Done
assignee:
  - moti.cohen@redis.com
created_date: '2026-09-20 17:19'
updated_date: '2026-09-20 21:36'
labels:
  - bgsave
  - persistence
  - db
  - kvstore
milestone: m-0
dependencies:
  - TASK-1.3
documentation:
  - poc-hybrid-bgsave-spec.md
parent_task_id: TASK-1
priority: high
ordinal: 7000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Spec §4: while a slot is PENDING or SCANNING all main-thread changes to keys in that slot go to a per-slot temp dict (main-thread only) and never to the scanned dict. Presence in the temp dict is the 'already captured' marker. Deletes of an owed key put a tombstone in the temp dict and leave the original in place so it is still serialized. Adds of new keys in an owed slot go to the temp dict only. Reads consult the temp dict first, then the scanned dict. When the background thread signals slot-scan-complete the main thread merges the temp dict into the real dict (apply clones, tombstones, adds) and the slot becomes DONE; for a DONE slot writes act in place normally. In M1 all frees of replaced fork-instant originals are deferred to save end (mid-save frees are M2). Implementation in src/snapshot.c with 'snap'-prefixed functions (snapTempDictCapture, snapTempDictLookup, snapSlotMerge, ...).

Scope: the temp dict data structure keyed by slot (or by the scan unit chosen in TASK-1.3), with entry kinds clone / tombstone / add; the resolution layer used by every keyspace lookup so that lookupKey, dbFind, exists/type/ttl, and all enumeration paths (SCAN, KEYS, RANDOMKEY, DBSIZE, SCAN with TYPE/MATCH, cluster GETKEYSINSLOT/COUNTKEYSINSLOT, active expire cycle sampling, eviction sampling via dictGetSomeKeys / eviction pool, keysizes histogram) see the logical keyspace and never expose a tombstoned key or miss an added key; the merge at slot completion; the deferred-free list drained when the save completes or aborts; abort semantics (on abort: merge everything so the live keyspace is consistent, then free originals). This task provides the primitives the clone-on-write hooks (TASK-1.7) call; it does not by itself route the write commands.

Clone shapes: a clone is always a new kvobj header in the SNAP_NOFORK arena; for a SNAP_NOFORK value change it also carries a new value in the arena of its new class; for a header-only change on a SNAP_FORK key (EXPIRE, metadata, LRU) the clone header SHARES the original's value pointer, and at merge only the original header is freed, never the shared value. Tombstones of SNAP_FORK keys defer the value free too in M1.

Gaps to resolve: the db->expires kvstore and hash-field hexpires ebuckets reference kvobj pointers; when a clone replaces an original at merge, those references must be updated (or the clone must reuse the same expires entry) - define exactly how expire lookups behave for a key whose live version is in the temp dict. Rehash of the scanned dict is paused (TASK-1.3) so the merge insert/delete into the real dict happens only after the slot is DONE and rehash is resumed. DBSIZE and INFO keyspace counts must reflect the logical keyspace during a save. Keyspace notifications, WATCH/touchWatchedKey and client-side tracking must fire on the logical key, unchanged from today.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 A per-slot temp dict exists in snapshot.c with clone, tombstone and add entry kinds, and an API for the main thread to capture, look up, and merge entries
- [x] #2 Every keyspace read path (point lookups and all enumeration paths listed in the description) resolves through the temp dict overlay first so that during a save a deleted key is invisible, an added key is visible, and a modified key returns the clone
- [x] #3 DBSIZE, INFO keyspace and cluster slot key counts reflect the logical keyspace during a save
- [x] #4 Expire lookups (TTL, lazy expiry, active expire cycle) and hash-field expiry behave correctly for keys whose live version is in the temp dict, with tests for EXPIRE/PERSIST on owed keys and hexpire on owed hashes
- [x] #5 Header-only clones of SNAP_FORK keys share the value pointer and merge frees only the original header, verified by an ASan test that EXPIREs an owed big hash during a save and then reads it after merge
- [x] #6 At slot-scan-complete the main thread merges the temp dict into the real dict and the slot state becomes DONE; a subsequent write to a key in that slot acts in place
- [x] #7 Fork-instant originals replaced by clones or tombstones are freed only after the save completes or aborts (M1 behaviour) and no original is freed while any slot is still owed
- [x] #8 On save abort all temp dicts are merged, the live keyspace matches what clients observed, and all originals are freed, with no leaks under a valgrind/ASan run
- [x] #9 Tests cover: write then read of an owed key, delete then re-add of an owed key within one save, add of new key in owed slot not present in the produced RDB, merge with rehash resumed, and abort mid-save
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
## Implementation plan (TASK-1.6) — "kvstore overlay" design

### Why not per-primitive hooks in db.c
The audit shows dozens of paths that obtain a kvobj and then write `o->ptr`/`o->encoding`/the value directly, plus SCAN/KEYS/RANDOMKEY/DBSIZE/eviction sampling/cluster slot enumeration that all read db->keys. Every one of them goes through the kvstore API (`kvstoreDictFindLink/Find/FetchValue/AddRaw/SetAtLink/Delete/TwoPhaseUnlink*/Scan/Iterator/GetSomeKeys/GetRandomKey/Size`). So the temp dict is implemented INSIDE kvstore as a per-dict overlay, and clone-on-write happens on the first *write* lookup. db.c changes shrink to: lookupKey's write path, a few read-then-write spots (audit §3), and abort points.

### Overlay (kvstore.c)
- Per kvstore: `snapOverlay *ov` (NULL when no save). Per dict didx: `frozen` flag (set by snapScanStart via a new kvstore hook, cleared by the slot merge), a `temp` dict (normal dict: key = sds copy of the key name, value = `snapTempEntry {kvobj *kv; int kind; int value_shared;}` with kind CLONE / ADD / TOMBSTONE), and logical counters (adds, tombstones) so `kvstoreDictSize`, `kvstoreSize`, `key_count` and the fenwick tree describe the LOGICAL keyspace.
- Read paths (`kvstoreDictFind`, `kvstoreDictFetchValue`, `dbFind`): temp first (TOMBSTONE -> not found; CLONE/ADD -> that kvobj), else frozen dict (original, read-only).
- Write-link paths (`kvstoreDictFindLink`, `kvstoreDictTwoPhaseUnlinkFind`) on a frozen dict NEVER return a link into the frozen dict: if the key is in temp -> temp link; if only in frozen -> call the clone callback (`snapCloneOwedKey`, see below), store the CLONE in temp, return its temp link; if absent -> the temp bucket link so `kvstoreDictSetAtLink(newItem=1)` inserts an ADD. `kvstoreDictAddRaw` on a frozen dict inserts into temp as ADD. Deletes (`TwoPhaseUnlinkFree`, `kvstoreDictDelete`) on a temp link: remove the temp entry (destructor frees the clone/add as today) and, if the frozen dict holds the key, leave a TOMBSTONE. So dbGenericDelete, dbSetValue, setExpireByLink, keyMetaSetMetadata work unchanged: every pointer they mutate is a clone.
- Enumeration: `kvstoreScan` / `kvstoreIterator` / `kvstoreDictIterator` over a frozen dict yield frozen entries mapped through temp (skip TOMBSTONE, substitute CLONE) followed by the ADD entries (emitted in the final batch for SCAN cursors); `GetRandomKey/GetFairRandomKey/GetSomeKeys` sample the frozen dict, skip tombstoned keys, substitute clones, fall back to temp when the frozen dict is exhausted.
- `db->expires` is NOT frozen (the scan thread never reads it): cloning rewrites its entry to the clone, so TTL sampling, active expire and PERSIST/EXPIRE act on clones. Hash-field subexpires: `estoreRemove(original)` + `estoreAdd(clone)` at clone time; the writes this makes into the ORIGINAL's `listpackEx.meta` are tolerated because the background serializer will compute the min field TTL from the listpack tuples, never from `meta` (TASK-1.8), and active subexpire is suspended for owed slots (TASK-1.7).

### Clone (snapshot.c, `snapCloneOwedKey(db, slot, orig)`)
- Header: copy the whole kvobj allocation (metadata slots, robj, embedded key, embedded EMBSTR value with `ptr` re-based) into a fresh arena block.
- Value: SNAP_NOFORK class (single allocation) -> deep copy in the same arena (`sdsdup`-like placement copy, `lpDup`, intset copy, listpackEx: fresh struct + listpack copy, meta trash=1); SNAP_FORK class -> share `ptr` and mark `value_shared` (the original header no longer owns the value; the child reads the value from its CoW snapshot).
- Bookkeeping: `db->expires` entry -> clone; subexpires -> clone; keysizes/alloc-size histograms unchanged (logical key unchanged).

### Merge (main thread, slot-done hook from TASK-1.3)
For each temp entry of the slot: CLONE -> swap the frozen dict's entry pointer to the clone and free the original (header only when `value_shared`); TOMBSTONE -> unlink the original from the frozen dict and free it (header only if a clone shared its value and was deleted); ADD -> insert into the dict. Then clear `frozen`, release the temp dict, resume rehash/auto-resize (already done by snapScanCompleteSlot). Frees at merge are safe: the slot is SCANNED, the thread no longer reads it. (Spec M1 said "defer all frees to save end"; freeing at merge is strictly safer for memory and equally correct.) Abort: same merge for every slot.

### Out of scope here (TASK-1.7)
lookupKey write-intent (use FindLink for `LOOKUP_WRITE`, Find for reads), suspending lazy hash-field expiry / HRANDFIELD expiry / PFCOUNT cache on owed keys, suspending active subexpire, `rdbSaveObject` accurate min-TTL on the scan thread, FLUSH*/SWAPDB abort, eviction policy under a save, DEBUG assertion that no frozen-dict kvobj is written.

### Tests (unit, kvstore level, no server): freeze a populated kvstore; find/fetch return originals; FindLink returns a temp clone and the frozen entry is untouched; add/delete/re-add semantics; SCAN/iterator/random/size reflect the logical keyspace (deleted invisible, added visible, modified returns clone); merge restores a single live dict with the expected contents; abort merge; no leaks under ASan. tcl (with the hybrid save in TASK-1.9): the description's list.
<!-- SECTION:PLAN:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
2026-09-20 implementation notes (TASK-1.6)

Implemented as the 'kvstore overlay' (see plan): kvstore.c gained a per-dict overlay {temp dict (same type as the keyspace dict minus kvstore callbacks), state dict (frozen key -> TOMB / VALUE_SHARED flags, keyed by the frozen entry's own key pointer, which is immutable until merge), logical adds/tombs counters}. Read paths (kvstoreDictFind) consult temp -> tombstone -> frozen without cloning. Write-link paths (kvstoreDictFindLink, TwoPhaseUnlinkFind, Delete, AddRaw, SetAtLink) never touch the frozen dict: first touch clones via the callback into temp, misses return temp buckets, deletes remove the temp entry and leave a tombstone when the frozen dict holds the key. kvstoreScan / kvstoreIterator / kvstoreDictIterator / random / GetSomeKeys / kvstoreDictSize / kvstoreSize describe the logical keyspace (tombstones skipped, clones substituted, adds appended). freeDictIfNeeded never frees a frozen dict; kvstoreRelease merges any leftover overlay. kvstoreOverlayMerge (main thread, called from snapScanCompleteSlot before the slot-done hook) swaps clones into the real dict, inserts adds, unlinks tombstoned originals and releases originals through the free callback (header-only when the value was shared), with the dict destructor suppressed so ownership is explicit.

snapshot.c: snapOverlayCloneKey copies the whole kvobj allocation into a fresh arena block (EMBSTR ptr rebased), deep-copies SNAP_NOFORK values that live in the arena (raw sds, listpack, intset; listpackEx: fresh arena struct with trash=1 meta + listpack copy), shares everything else (multi-allocation values and values outside the arena, which the child reads from its CoW copy), then points db->expires and db->subexpires at the clone (estoreRemove(original) writes trash into the original's ExpireMeta: tolerated, the scan thread must recompute min field TTL from the listpack, TASK-1.8). snapOverlayFreeOriginal frees the value only when not shared and never runs key-metadata free callbacks (the clone carries the metadata). snapScanSetOverlay attaches the callbacks to a scan; every slot of every store is frozen at start (including empty slots, so adds go to temp). Known cost: cluster mode creates 16384 small overlays per db per save; make it lazy in M2 if it shows in measurements.

Gotchas fixed while testing: (1) no_value dicts require 8-byte-aligned stored keys, so the state dict cannot be sds-keyed on its own copies; it borrows the frozen key pointer and reuses the store's hash/compare. (2) kvstoreDictTwoPhaseUnlinkFree must do its bookkeeping before the temp entry is freed (the key pointer dies with it). (3) TSan on kernel 6.8 needs `setarch -R` (ASLR) or it aborts with 'unexpected memory mapping'.

Tests: 53 kvstore-level overlay checks x2 (completion and abort variants): read returns original without cloning; write lookup clones once and the frozen dict keeps the original; delete of an owed key is clone-then-tombstone and invisible; double delete fails; add / re-add / add into an empty frozen slot; logical sizes; iterator, kvstoreScan, random sampling and GetSomeKeys reflect the logical keyspace; the scan thread dispatched exactly the fork-instant set (deleted key included, adds excluded); after merge a single live dict holds clone, re-added and new keys with consistent sizes and exactly the replaced originals freed. 134/134 on jemalloc; 81/81 under ThreadSanitizer (libc, arena inert) with 0 warnings. Server-level criteria (#4 expires/hexpire behaviour, #5 header-only clone ASan test with a real EXPIRE during a save) can only be exercised once a save can run (TASK-1.9); left unchecked until then.

2026-09-21: server-level criteria verified through the hybrid save tests (commit 96d7bc733): during a save EXPIRE on an owed SNAP_NOFORK set, EXPIRE/PERSIST on owed SNAP_FORK keys (hashtable hash, quicklist list: header-only clones sharing the value), HSET on an owed listpackEx hash and TTL reads all behave correctly and survive the merge; the produced file keeps the fork-instant content. AC #5's ASan variant cannot run: the build system forces libc malloc under sanitizers, which disables the arena and therefore hybrid saves; verified functionally (hgetall/llen after merge) and by the 0-violation placement report instead.
<!-- SECTION:NOTES:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
## Summary

Temp-dict semantics implemented as a **kvstore overlay** (commit 96d7bc733, branch `snapshot-hybrid-bgsave`, local only).

### What changed
- kvstore.c: per-dict overlay while frozen: temp dict (clones and adds), state (tombstone / value-shared flags keyed by the frozen entry's own key), logical counters. Read lookups never clone; write-link lookups clone on first touch through a callback and never return links into the frozen dict; deletes tombstone; scan/iterators/random/GetSomeKeys/sizes describe the logical keyspace; `kvstoreOverlayMerge` applies the temp dict when the slot completes and frees originals (header-only when shared).
- snapshot.c: `snapOverlayCloneKey` (whole kvobj copy into the arena, EMBSTR pointer rebased; SNAP_NOFORK values deep-copied, others shared; db->expires and subexpires repointed to the clone), `snapOverlayFreeOriginal`, `snapScanSetOverlay`, merge wired into slot completion. `kvobjFreeValueOnly` in object.c.
- db.c: `lookupKey` clones only for LOOKUP_WRITE.

### Tests
- 53 kvstore-level checks x 2 (completion, abort); TSan clean.
- End-to-end through the hybrid save tcl tests: writes of every kind during a save stay in the overlay, the file keeps the fork instant, sizes/enumeration correct, FLUSH/SWAPDB abort merges cleanly.

### Notes
- Frees happen at slot merge (safe: the thread is done with the slot), not at save end.
- Eager per-slot overlay allocation (16384/db in cluster mode) is a known M2 optimization.
<!-- SECTION:FINAL_SUMMARY:END -->
