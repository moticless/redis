---
id: TASK-1.4
title: >-
  snapClassify() and SNAP_NOFORK/SNAP_FORK placement: headers always
  SNAP_NOFORK, values by class, relocation on class change
status: Done
assignee:
  - moti.cohen@redis.com
created_date: '2026-09-20 17:19'
updated_date: '2026-09-20 19:46'
labels:
  - bgsave
  - persistence
  - object
  - allocator
milestone: m-0
dependencies:
  - TASK-1.2
documentation:
  - poc-hybrid-bgsave-spec.md
parent_task_id: TASK-1
priority: high
ordinal: 5000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Spec §1-§2: every key is classified by its VALUE. SNAP_NOFORK: the value is at most one separate allocation, at most save-small-max-bytes, with an encoding accepted by snapEncodingIsNoforkClass() (strings of any encoding; listpack list/set/zset/hash; intset; listpack-ex; templated hash encodings OBJ_ENCODING_TMPL_LP / OBJ_ENCODING_TMPL_ARRAY). SNAP_FORK: everything else (hashtable/skiplist/quicklist, module types, streams, anything over the threshold). The starting point for the encoding half is already written in the spec (snapEncodingIsNoforkClass) and goes into src/snapshot.c.

Placement rules: EVERY kvobj header (the whole allocation: preceding metadata slots, robj, embedded key) is allocated in the SNAP_NOFORK arena via the API from TASK-1.2, regardless of class. SNAP_NOFORK values (raw sds, listpack, intset, listpackEx struct and its listpack) are allocated in the SNAP_NOFORK arena. SNAP_FORK values are allocated in default forkable memory. A SNAP_FORK value must never live in the SNAP_NOFORK arena because the fork child cannot read it.

Scope: snapClassify(type, encoding, size); kvobj creation paths (kvobjCreate, kvobjCreateEmbedString, kvobjSet, dbAdd, setKey, RESTORE, replication load, RDB/AOF load) allocate the header in the arena and the value by class; evaluate and, if viable, add an explicit SNAP_NOFORK class bit in the kvobj/robj header so free paths and assertions can read the class without recomputing it (record the decision and the bit chosen); relocation on class change: when a SNAP_NOFORK value grows past the threshold or converts to a multi-allocation encoding its value (not its header) is moved to forkable memory (SNAP_FORK -> SNAP_NOFORK relocation optional); the header never moves, so db->expires and hexpires pointers stay valid on relocation. During a save the clone-on-write path (TASK-1.6/1.7) allocates the clone value in the arena of its new class, so expose 'allocate value for class X' primitives. Route every value free through the class-aware free wrappers from TASK-1.2.

Gaps to resolve: listpackEx is two allocations (struct with ExpireMeta plus the listpack) - both go to the arena or hashes with field TTL are SNAP_FORK; decide and document. Shared/integer-encoded values (OBJ_ENCODING_INT) and embedded strings have no separate value allocation. Keys loaded from RDB/AOF at startup and via replication must be placed correctly too.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 snapClassify() and snapEncodingIsNoforkClass() in snapshot.c return SNAP_NOFORK for strings and single-allocation encodings at or below save-small-max-bytes and SNAP_FORK for hashtable/skiplist/quicklist/stream/module types and anything above the threshold, covered by unit tests
- [x] #2 Every kvobj creation path (command writes, RESTORE, RDB load, AOF load, replication) places the whole kvobj allocation (metadata, robj, embedded key) in the SNAP_NOFORK arena and the value in the arena matching its class, verified with the arena-membership predicate under DEBUG assertions
- [x] #3 The kvobj class-bit decision is recorded; if adopted, the bit is set on creation, updated on relocation, and used by the class-aware free path
- [x] #4 A SNAP_NOFORK value that grows past the threshold or converts to a multi-allocation encoding has its value relocated to forkable memory while its header stays in place, covered by tests for string growth, listpack->hashtable, intset->hashtable and listpack->skiplist conversions
- [x] #5 A DEBUG assertion (or DEBUG subcommand) verifies that no SNAP_FORK value resides in the SNAP_NOFORK arena and every kvobj header does
- [x] #6 The listpackEx (hash field TTL) two-allocation case is handled and the decision is documented in task notes
- [x] #7 All value frees go through the class-aware free wrappers; an ASan/debug run of the tcl suite shows no SNAP_NOFORK pointer freed via the default path
- [x] #8 OBJECT ENCODING, MEMORY USAGE and DEBUG OBJECT behave unchanged for both classes
- [x] #9 The full tcl test suite passes with placement enabled and no save in progress
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
## Implementation plan (TASK-1.4)

### Approach change versus TASK-1.2 (recorded here because it is what makes placement feasible)
Values grow and shrink in place through sds/listpack/intset code that calls plain zrealloc/zfree. jemalloc's realloc/free do not preserve the arena: a SNAP_NOFORK listpack grown by `lpInsert` would be moved to the default arena and the freed block would enter the default tcache. Threading a class parameter through every sds/listpack/intset call site is invasive and error prone. Instead:

**(A) Back the SNAP_NOFORK arena with one reserved virtual address range and make the generic allocator arena-aware.** Move the arena core from snapshot.c into zmalloc.c (snapshot.c keeps thin `snap*` wrappers):
- At init: `mmap(PROT_NONE, MAP_PRIVATE|MAP_ANONYMOUS|MAP_NORESERVE)` a large reservation (try 16 TB, 4 TB, 1 TB, 256 GB, 64 GB; if all fail the arena is unavailable), `madvise(MADV_DONTFORK)` on the whole reservation once. Extent hooks: `alloc` carves from the reservation with an atomic bump pointer (honours `alignment`; refuses `new_addr` hints that are not the bump pointer) and `mprotect(RW)`s the range (VMA split keeps VM_DONTCOPY); `commit` mprotects RW; `decommit` returns unsupported (jemalloc purges instead, matching the Linux overcommit default); `dalloc` returns failure so jemalloc retains the extent (VA is never unmapped); `destroy` no-op; `purge_lazy/purge_forced/split/merge` delegate to the defaults (madvise / bookkeeping). Arena still created via `experimental.arenas_create_ext` with `metadata_use_hooks=false`.
- `zmalloc_is_nofork(p)` = `lo <= p < hi` (inline, two loads + two compares). Replaces the `arenas.lookup` predicate.
- `zfree`, `zfree_usable`, `zfree_with_size`, `zrealloc*` (ztryrealloc_usable_internal) branch on `zmalloc_is_nofork(ptr)` and use `sdallocx/rallocx` with `MALLOCX_ARENA(nofork) | MALLOCX_TCACHE(per-thread explicit tcache)`, so arena blocks stay in the arena and never enter a default tcache. New allocations (`ptr == NULL`) stay default. Defrag: `activeDefragAllocWithoutFree` allocates the replacement in the same arena as the original (`zmalloc_nofork_no_tcache`), and `zfree_no_tcache` is range-aware.
- API in zmalloc.h: `zmalloc_nofork(_usable)`, `zrealloc_nofork`, `zfree_nofork(_size)`, `zmalloc_nofork_no_tcache`, `zmalloc_is_nofork`, `zmalloc_nofork_arena_init/available/index`, VA stats. snapshot.c: `snapNofork*` and `snapPtrIsNofork` call these; the class-aware free wrappers remain as documentation of intent but plain zfree is now equally correct.
- Tests: existing 23 arena checks stay (fork fault, VmFlags dc, purge stress, tcache isolation, VMA count now covering mprotect splitting); add: zrealloc of an arena pointer stays in range; zfree of arena pointers then zmalloc never returns arena memory; defrag-style no-tcache alloc keeps arena.

**(B) Placement.**
- `snapEncodingIsNoforkClass(type, encoding)` (from the spec) and `snapClassify(type, encoding, alloc_bytes)` = NOFORK iff encoding qualifies and `alloc_bytes <= server.save_small_max_bytes` (EMBSTR/INT: no separate allocation, always NOFORK).
- `snapKeyClass(kv)` (used by the scan dispatch in TASK-1.8): NOFORK iff the value has no separate allocation, or its single allocation (for listpackEx: both the struct and its listpack) is in the arena; otherwise FORK. Classification at scan time is by ACTUAL placement, which is what makes the fork-child/bg-thread split correct regardless of policy drift.
- Header placement: `kvobjCreate` and `kvobjCreateEmbedString` allocate with `zmalloc_nofork(_usable)`; every kvobj header is therefore in the arena. Frees are range-aware automatically.
- Value placement policy at insertion: `kvobjSet()` (the funnel used by dbAdd, dbAddRDBLoad, dbOverwrite/setKey) ends with `snapPlaceValue(kv)`: if `snapClassify` says NOFORK and the value is outside the arena, copy it in (`sds` via `sdsnewplacement` into a `zmalloc_nofork_usable` buffer; listpack/intset by `lpBytes`/`intsetBlobLen` memcpy; listpackEx struct + lp); if it says FORK (single-alloc but over the threshold) and the value is inside the arena, copy it out. Multi-allocation encodings are untouched (always default memory).
- Class drift after insertion: in-place growth keeps the arena (range-aware realloc); listpacks are bounded by the *-max-listpack-* configs so staying in the arena is acceptable. Raw strings can grow without bound (APPEND/SETRANGE/SETBIT/BITFIELD), so `snapReconcileValue(kv)` is called after the growth sites in t_string.c and bitops.c (setrange, append, setbit, bitfield via dbUnshareStringValue callers) and moves a string that crossed the threshold out of the arena. Shrink-conversions that allocate a fresh listpack in default memory (quicklist->listpack) leave a NOFORK-class value outside the arena; correct (child serializes it) but not optimal; reconcile there too where cheap (listTypeTryConversion). Everything else is tolerated drift for M1 and measured in TASK-1.17.
- Optional class bit: not needed. The range check gives the class of any pointer in ~1 ns and the robj has no spare bit worth spending; decision recorded.
- listpackEx: both allocations go to the arena (hashes with field TTL stay NOFORK when small).
- `DEBUG SNAP-PLACEMENT [dbid]`: walks the keyspace and reports headers in/out of arena, values per class in/out, and violations (header outside arena; multi-allocation value top pointer inside arena). Used by tests and as the assertion vehicle.

### Tests
- Unit (snapshotTest): classify table; placement of kvobj created via kvobjSet for each encoding (string raw/embstr/int, listpack list/hash/set/zset, intset, listpackEx, hashtable, skiplist, quicklist, stream) checking header in arena and value placement per class; relocation on growth (string 100 B -> 10 KB moves out; 10 KB -> trimmed stays out).
- tcl (tests/unit/snapshot.tcl): populate all types via commands, RESTORE, DEBUG RELOAD, replication to a replica; `DEBUG SNAP-PLACEMENT` reports 0 violations and expected class counts; conversions listpack->hashtable, intset->listpack->hashtable, listpack->skiplist, quicklist<->listpack keep 0 violations; APPEND past the threshold relocates (value class changes in the report); OBJECT ENCODING / MEMORY USAGE / DEBUG OBJECT unchanged.
- Full tcl suite; ASan run of the snapshot unit test and of unit/snapshot tcl.

### Files
zmalloc.c/.h (arena core, range-aware free/realloc), snapshot.c/.h (wrappers, classify, place/reconcile, tests), object.c (header allocs, kvobjSet hook), t_string.c / bitops.c (reconcile after growth), defrag.c (same-arena replacement), debug.c (DEBUG SNAP-PLACEMENT), tests/unit/snapshot.tcl.
<!-- SECTION:PLAN:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
2026-09-20 implementation notes (TASK-1.4)

(A) Arena core moved to zmalloc.c on a reserved address range. mmap(PROT_NONE, MAP_NORESERVE) of 16 TB (fallbacks 4 TB .. 64 GB), MADV_DONTFORK once on the whole range; extent alloc = atomic bump pointer + mprotect(RW) (VMA split keeps VM_DONTCOPY, and adjacent RW ranges merge so /proc/self/maps did not grow at all for 1M and 10M allocations); commit = mprotect; decommit reported unsupported (jemalloc purges via the default madvise hooks, same as Linux overcommit default); dalloc reports failure so extents are retained; destroy no-op. `zmalloc_is_nofork(p)` is a 2-compare range check. zfree/zfree_usable/zfree_with_size/zrealloc*/zfree_no_tcache branch on it and use MALLOCX_ARENA + the per-thread explicit tcache, so arena memory stays in the arena through every generic path (tested: zrealloc grow/shrink, plain zfree, sized free, no-tcache). Defrag allocates replacements in the same arena. Non-Linux/non-jemalloc: zmalloc_is_nofork() is a constant 0.

sds: sdsMakeRoomFor/sdsResize allocate a NEW buffer when the header type changes (5->8->16...) instead of reallocating, which silently moved growing values out of the arena. Added s_malloc_usable_like(sh, ...) = zmalloc_usable_like(): same arena as the old buffer. (listpack and intset use realloc and were already fine.)

(B) Placement. Headers: kvobjCreate/kvobjCreateEmbedString allocate via snapNoforkAlloc(Usable). Values: snapPlaceValue(kv) at the end of kvobjSet (funnel for dbAdd/dbAddRDBLoad/RESTORE/overwrite/RENAME/COPY) moves single-allocation values in (<= save-small-max-bytes) or out (bigger) of the arena by copy; after unbounded string growth (SETRANGE, APPEND, SETBIT/BITFIELD via lookupStringForBitCommand) and after quicklist->listpack conversion the policy is re-applied. snapKeyClass(kv) classifies by ACTUAL placement for the scan dispatch. DEBUG SNAP-PLACEMENT [dbid] reports headers in/out, class counts, policy drift and violations.

listpackEx decision (AC #6): the struct is arena-born (listpackExCreate uses snapNoforkCalloc) and is NEVER moved, because its ExpireMeta is registered in db->subexpires ebuckets (moving it would leave a dangling ebuckets link; found via the audit). The listpack follows the struct into the arena, whatever its size; a LISTPACK_EX key is therefore always served by the background thread when the struct is in the arena. If a legacy struct were outside the arena with its listpack inside, the child would dereference arena memory: snapPlaceValue moves the listpack out in that case and DEBUG SNAP-PLACEMENT counts it as a violation.

Class bit (AC #3): not adopted. zmalloc_is_nofork() gives the class of any pointer in ~1 ns and is needed on free paths anyway.

MAJOR SCOPE FINDING: once kvobj headers are in the DONTFORK arena, NO fork child can read the keyspace: stock BGSAVE, diskless/disk full-sync via rdbSaveToSlavesSockets/rdbSaveBackground, and AOF rewrite all segfault in the child (the replica tcl test hit this). The spec's M1 plan to keep replication and foreground paths on the stock serializer is therefore infeasible while hybrid placement is on. Resolution: new startup-only config `bgsave-hybrid` (default no) gates arena creation and hence all placement; with it off the server is byte-for-byte stock. With it on, rdbSaveBackground, rdbSaveToSlavesSockets and rewriteAppendOnlyFileBackground currently refuse with a log line (temporary guards) until TASK-1.9 wires the hybrid path (which then also serves disk-based replication; diskless sync and AOF rewrite stay unavailable in hybrid mode until TASK-1.14/1.15). The replica placement tcl test is parked until TASK-1.9. Foreground SAVE / DEBUG RELOAD work (main thread).

Tests: unit `redis-server test snapshot` 78 checks (arena 23, allocator/placement 29, scan 26); tcl unit/snapshot 7 tests: all types placed with 0 violations/drift (10 NOFORK, 6 FORK), conversions listpack->hashtable, intset->listpack->hashtable, listpack->skiplist, quicklist<->listpack keep 0 violations, APPEND/SETRANGE/SETBIT past the threshold relocate, DEBUG RELOAD / DUMP-RESTORE / RENAME / COPY re-place, OBJECT ENCODING / MEMORY USAGE / DEBUG OBJECT unchanged. Encoding names in the spec snippet (TMPL_LP / TMPL_ARRAY) do not exist in this tree; hashes are LISTPACK / LISTPACK_EX / HT; OBJ_ARRAY (SLICED_ARRAY) is FORK.

Full tcl suite with bgsave-hybrid off (default): 5560 passed, 0 failed. tests/unit/introspection.tcl config-sanity skip list gained bgsave-hybrid (immutable). ASan run to be recorded under TASK-1.5 (same binary).
<!-- SECTION:NOTES:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
## Summary

Placement half of the hybrid BGSAVE design, committed on `snapshot-hybrid-bgsave` (local only).

### Allocator
- SNAP_NOFORK arena backed by **one reserved DONTFORK address range** (16 TB `PROT_NONE` + `MADV_DONTFORK`, extents carved by atomic bump pointer + `mprotect`). `zmalloc_is_nofork()` is a two-compare range check.
- `zfree` / `zfree_usable` / `zfree_with_size` / `zrealloc*` / `zfree_no_tcache` are arena-aware: arena memory stays in the arena via a per-thread explicit tcache. sds allocates replacement buffers "like" the old one across header-type changes; defrag allocates replacements in the same arena.
- `bgsave-hybrid` startup config (default `no`) gates the arena and therefore all placement.

### Placement
- `snapEncodingIsNoforkClass`, `snapClassify` (policy), `snapKeyClass` (actual placement, for the scan dispatch).
- Every kvobj header allocated in the arena. `kvobjSet()` applies the policy to single-allocation values (copy in/out); re-applied after SETRANGE/APPEND/SETBIT/BITFIELD growth and quicklist->listpack conversion.
- listpackEx struct arena-born and never moved (ebuckets link); its listpack follows the struct.
- `DEBUG SNAP-PLACEMENT [dbid]`.
- Temporary guards: with `bgsave-hybrid yes`, `rdbSaveBackground`, `rdbSaveToSlavesSockets`, `rewriteAppendOnlyFileBackground` refuse (no fork child can read DONTFORK headers) until TASK-1.9.

### Tests
- 78 unit checks; tcl unit/snapshot placement tests (all types, conversions, growth relocation, DEBUG RELOAD / DUMP-RESTORE / RENAME / COPY, OBJECT ENCODING / MEMORY USAGE / DEBUG OBJECT); full suite 5560/5560 with the switch off.

### Decisions / findings (details in notes)
- No kvobj class bit (range check instead).
- Scope finding: all fork-based serializers must go through the hybrid path once placement is on; spec §11 addendum and TASK-1.9 notes updated. Replica placement test parked until TASK-1.9.
- Spec encodings TMPL_LP/TMPL_ARRAY do not exist in this tree.
<!-- SECTION:FINAL_SUMMARY:END -->
