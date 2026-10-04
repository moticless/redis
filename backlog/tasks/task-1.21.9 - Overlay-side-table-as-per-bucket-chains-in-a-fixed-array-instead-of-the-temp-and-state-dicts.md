---
id: TASK-1.21.9
title: >-
  Overlay side table as per-bucket chains in a fixed array instead of the temp
  and state dicts
status: Done
assignee:
  - '@Claude'
created_date: '2026-10-03 10:26'
updated_date: '2026-10-04 18:53'
labels:
  - bgsave
milestone: m-3
dependencies:
  - TASK-1.21.8
documentation:
  - poc-hybrid-bgsave-spec.md
modified_files:
  - src/dict.c
  - src/dict.h
  - src/kvstore.c
  - src/kvstore.h
  - tests/unit/snapshot.tcl
  - poc-hybrid-bgsave-spec.md
parent_task_id: TASK-1.21
priority: medium
ordinal: 27000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
User's proposal, 2026-10-03: replace the overlay's two dicts (temp: clones and adds keyed by key; state: frozen key to tombstone/shared flags, both of a cloned dictType with callbacks nulled, which is where the 2026-10-03 onDictRelease sanitizer hit came from) with one plain array of chain heads. An in-window write creates an entry {status (clone deep/shared, add, tombstone), key (the clone or the add, in the no-value dict's key-only entry encoding so links handed to callers keep working with dictGetKV), table, idx, next} in the chain at array[idx & (W-1)], where (table, idx) is the key's frozen bucket, already computed by the prepare hook. W is a constant power of two (1024 to start), independent of snap_window_buckets: entries of the previous window and the current one coexist until the main thread drains, and the whole-dict window at claim time defers writes to any bucket, so the index must be the bucket modulo W, not an offset in the window; W only bounds chain length. The array is allocated on the first deferred write (only the dict under the thread's window ever needs one), not at attach, so dicts attached by writes ahead of the thread keep only their bitmaps.

Why: one structure instead of two dicts and a cloned type, no second hashing or key compare per side-table operation, the mapping to the real dict is the stored bucket so the drain is a splice without rehash, and the iterator/scan phase over the side table becomes an array walk. The dict is paused while entries exist (window or SCANNING), so stored indices stay valid; the release drain runs before the resume.

Scope: kvstore.c overlay module (attach/drain/map/iterators ov_phase/scan callback/two-phase unlink/stats), hooks unchanged in meaning. The existing C overlay tests (snapOverlayTests, leftover tests) and unit/snapshot cover every path.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 The overlay holds no dict: side-table entries live in per-bucket chains of a fixed power-of-two array allocated on the first deferred write and freed at release; kvstoreOverlayAttach allocates nothing but the overlay struct
- [x] #2 Links returned to callers for side-table entries work with dictGetKV/dictSetKeyAtLink exactly as temp-dict links did (C overlay tests unchanged in expectations: clone on write lookup, read after write returns the clone, tombstones hide originals, adds visible, two-phase unlink)
- [x] #3 Drain applies each entry to its stored frozen bucket without rehashing; iterators and kvstoreScan see frozen entries then side-table adds once each; counts (size, adds, tombstones, pending) match the dict semantics
- [x] #4 unit/snapshot hybrid and stock, C unit tests, TSan and ASan clean; full feature-on suite passes; spec section on the side table updated
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
1. dict.h/.c: dictEntryCreateDetached (no_value -> createEntryNoValue, else a normal entry with v.val NULL), dictEntryFreeDetached (node, key through the destructor when asked), dictEntryNextLink (&next).
2. kvstore.c overlay: kvstoreDictOverlay {kvs, didx, ovSlot *slots (1024, lazy), entries, adds, tombs, hooks, unlink_frozen, dirty_node, privdata}; ovSlot {heads[4]} for LIVE, SHARED, TOMB, TOMB_SHARED. ovSlotOf = hash & 1023 (allocates the array). ovChainFind/ovSideFind (link + chain), ovChainPush (head), ovChainUnlink, ovCursor (LIVE+SHARED, next fetched ahead). ovEnsureClone pushes to SHARED or LIVE per the clone hook; ovSideInsert replaces a tombstone (chain SHARED if TOMB_SHARED) or counts an add; ovSideRemove pushes a tombstone (kind by the chain) or uncounts an add and unlinks; ovApplySide/ovApplyTomb at drain with dictFindLink/dictSetKeyAtLink/dictUnlink as before; ovDrainOne walks the array. Entry points (find, findLink, setAtLink, addRaw, two-phase unlink with the chain as the opaque table index, delete), iterators (ov_cur cursor in kvstoreIterator/kvstoreDictIterator), scan, random and some-keys rewritten over the chains.
3. Spec section 4 side-table paragraph and the leftover-2 note updated; verification: C unit (overlay suite), unit/snapshot hybrid and stock, TSan, ASan, full feature-on suite.
<!-- SECTION:PLAN:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
Refinement (user, 2026-10-03): each array slot holds a real dictEntry chain, the look and feel of a dict bucket, so links handed to callers are ordinary dictEntryLinks and the drain splices entries into the frozen bucket (dict.c must export chain helpers: next-pointer access, splice into bucket, unlink from chain; the entry tag bits 000/XX1/010 are all dict.c's, no per-entry flag fits). A deleted key's item moves to the head of its chain and a per-bucket counter says how many leading items are tombstones (the tombstone's key is the borrowed frozen kvobj pointer, never destructed by us). Same trick for the 'value shared with the original' state: a second counter, shared clones right after the tombstones; a clone that stops sharing moves across that boundary (chains are short). Clone versus add needs no flag: at drain, an original found in the frozen bucket means clone (unlink it, free per shared), none means add. Per-slot struct {dictEntry *head; uint16_t ndel, nshared}, 16 KB for W=1024, allocated on the first deferred write.

Final shape (user, 2026-10-03): three chain heads per slot instead of one chain with counters: {dictEntry *live; dictEntry *shared; dictEntry *deleted}. live = deep clones and adds; shared = clones still sharing the original's value allocation; deleted = tombstones whose key is the borrowed frozen kvobj pointer. A status change is an unlink from one chain and a push on another (shared -> live when the value stops being shared, deleted -> live on re-add, live/shared -> deleted on delete). Entries are allocated no-value dict entries so every chain position is a valid dictEntryLink. 24 KB per array (W=1024), allocated on the first deferred write.

2026-10-03: implemented. Deviation from the three-heads shape: four chains, because a tombstone must remember whether its clone shared the original's value (the async delete path frees the clone, value included, outside the overlay, so the original is then freed header-only at drain) and the tombstone's key is the borrowed original with no room for a flag. C unit 184/184 (57 overlay checks) on the first run; unit/snapshot hybrid and stock, TSan and ASan chain running.

2026-10-03: unit/snapshot hybrid 75 and stock 75 ok; ASan 69 ok clean; TSan 68 ok + one failure, 'keyspace dicts keep their bucket layout during a save': under TSan's slow thread start the 6000 adds ran before the thread claimed the dict, so stage B let the table grow (correct, but not what the test asserted; the same failure appeared on the first TSan run of the day, before any of today's changes). The test now waits for the thread's first dispatched key before the adds; passed 2/2 under TSan. Full feature-on suite running.

2026-10-03: full feature-on suite (164 units + solo tail, 8 clients): all passed. A first run was cut off at 144/164 by a laptop restart with no failures until then.

2026-10-04 benchmark (run 34, RESULTS.md): three-way on a fresh c7i.4xlarge, mixed1m/mixed/small at 4 GB, 0/20k/50k (+ mixed 50k on a 20% hot set), 2 reps. Hybrid 0c4e68488 matches runs 31/32 cell by cell after the instance's 5-10% offset (stock and Valkey slower by the same); hybrid-to-stock ratio unchanged (mixed1m 50k 0.59 vs 0.57, mixed 50k 0.50 vs 0.52, small 50k 0.84 vs 0.87); memory, CPU split, latency and achieved rate the same. No regression from TASK-1.21.8/1.21.9. One hard-mark abort on small 50k rep a: the main thread's emit-ahead queue (about 265 MB = 50k/s x 31 s x 170 B) sits at the 256 MiB mark by construction of that cell; the other reps and run 31 passed under it by a hair. Policy limit noted in RESULTS.md with options (higher default, forward main-thread records while the thread waits at its bound, or defer to the side table instead of aborting).

2026-10-04 run 35 (same instance as run 34, A/B interleaved per cell, mixed1m 20k and 50k, 3 reps): run 31's hybrid binary bfaa21174 25.6 / 25.8 / 24.0 s at 20k and 27.2 s (x3) at 50k against the current d2a464f34 at 23.5 / 27.4 / 26.1 s and 25.8 s. The two binaries are equal within the spread on one host, the old one as slow here as run 34 measured; run 34's 10-13% gap to run 31 under writes is the host (every binary slower, no steal or throttling, identical bytes written per save), not TASK-1.21.8/1.21.9.
<!-- SECTION:NOTES:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
The overlay side table is a fixed array of 1024 slots of chains of ordinary dict entries, a key's slot being its hash modulo 1024; each slot has four chains that stand for the entry's state (LIVE: deep clones and adds; SHARED: clones whose value is still the original's; TOMB and TOMB_SHARED: tombstones whose key is the borrowed frozen original). The temp and state dicts and the cloned dictType are gone, as is the second hash and key compare per side-table operation; a chain position is an ordinary dictEntryLink so callers' links need no translation; the array is allocated on the first deferred write and freed with the overlay. dict.c exports detached entries (dictEntryCreateDetached/FreeDetached/NextLink). The kvstore iterators carry a side-table cursor for their second phase. Four chains instead of the three proposed because a tombstone must remember whether its clone shared the original's value (the async delete path frees the clone outside the overlay). Verified: C unit 184/184 (57 overlay checks), unit/snapshot hybrid 75 and stock 75, TSan and ASan clean (the bucket-layout test now waits for the thread's first dispatched key, a stage B timing exposure under TSan), full feature-on suite green.
<!-- SECTION:FINAL_SUMMARY:END -->
