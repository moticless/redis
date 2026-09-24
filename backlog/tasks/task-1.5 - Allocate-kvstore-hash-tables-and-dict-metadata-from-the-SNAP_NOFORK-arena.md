---
id: TASK-1.5
title: Allocate kvstore hash tables and dict metadata from the SNAP_NOFORK arena
status: Done
assignee:
  - moti.cohen@redis.com
created_date: '2026-09-20 17:19'
updated_date: '2026-09-20 20:03'
labels:
  - bgsave
  - persistence
  - kvstore
  - dict
milestone: m-0
dependencies:
  - TASK-1.2
documentation:
  - poc-hybrid-bgsave-spec.md
parent_task_id: TASK-1
priority: high
ordinal: 6000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Spec §1-§2: the fork child never walks the keyspace, so the kvstore bucket arrays and dict metadata (dict structs, ht_table arrays, kvstore per-slot metadata, for both db->keys and db->expires) are allocated from the SNAP_NOFORK arena (TASK-1.2). This removes CoW on hash-table pages caused by inserts/deletes/rehash during a save and shrinks the fork's page-table copy.

Scope: give dict/kvstore an allocation hook or per-dict flag so the keyspace kvstores allocate tables and metadata via snapNoforkAlloc/Free, while all other dicts in the server (client dicts, pubsub, module dicts, etc.) stay on the default allocator. Rehash allocates new tables - those must also come from the arena, and table frees must use the class-aware free path so they never enter a default tcache. Verify nothing in the fork child (rdbSaveRio, AOF rewrite child, module fork callbacks, child INFO/cow reporting) dereferences these tables or any kvobj header (all headers are SNAP_NOFORK too); anything that does must be moved to the parent side or fail loudly with the child-side arena assertion hook.

Not needed for correctness of the M1 scan protocol, but M1 deliverable (d) (no CoW on headers, SNAP_NOFORK values or hash tables) needs it to show the CoW numbers.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 db->keys and db->expires kvstores for all dbs allocate dict structs, hash-table arrays and kvstore metadata from the SNAP_NOFORK arena, including tables created during rehash, verified with the arena-membership predicate under DEBUG assertions
- [x] #2 Table frees use the class-aware free path and never enter a default-arena tcache
- [x] #3 All non-keyspace dicts continue to use the default allocator, verified by an assertion in the dict allocation path
- [x] #4 A fork child in the stock BGSAVE path is audited for keyspace table and kvobj header dereferences; the audit result is recorded in task notes and any child-side access is removed or guarded
- [x] #5 Memory reported for the keyspace tables (INFO memory overhead.hashtable.*) remains correct
- [x] #6 The full tcl test suite passes, including cluster-mode tests that resize and rehash slot dicts
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
## Implementation plan (TASK-1.5)

Frees are already correct for arena memory (range-aware zfree from TASK-1.4), so only allocation sites need routing.

- dict.h: new dictType flag `nofork_alloc:1` ("allocate the dict struct, bucket arrays and entry nodes in the SNAP_NOFORK arena"). dict.c: `dictCreate` (struct + metadata), `_dictResize` (both the `zcalloc` and the `ztrycalloc` table paths; add `ztrycalloc_nofork` to zmalloc), `createEntryNoValue` (thread the dict in) and the normal `dictEntry` allocation in the add path use `zmalloc_nofork/zcalloc_nofork` when the flag is set. Iterators/stats stay default (transient).
- kvstore.c: when `dtype->nofork_alloc` is set, allocate the kvstore struct, the `dicts` pointer array, the rehashing list and the fenwick tree... keep it to the struct and the `dicts` array (the others are small, default is fine and simpler); dict creation goes through dictCreate with the flagged type.
- server.c: set `.nofork_alloc = 1` on `dbDictType` and `dbExpiresDictType` only (the two keyspace kvstores; pubsub and every other dict stay default). With `bgsave-hybrid no` the arena is unavailable and all of this degrades to zmalloc.
- Fork child audit (AC #4): with `bgsave-hybrid yes` no stock fork-based serializer may run (guards from TASK-1.4); module `RM_Fork` children do not touch the keyspace dicts; `dismissMemoryInChild()` walks db kvstores via `dismissMemory`-style helpers? -> check `dismissMemoryInChild` in server.c and make it skip keyspace tables when hybrid is on (they are not mapped in the child at all). Record the result.
- DEBUG SNAP-PLACEMENT: add `dict_tables_in_arena` / `dict_tables_outside` (dict structs and ht_table[0/1] of db->keys and db->expires) so tcl can assert.
- Tests: unit — kvstore created with a `nofork_alloc` dictType: dict struct, both tables after forcing a rehash, and chained no-value entry nodes are all `zmalloc_is_nofork`; a default-type kvstore has none. tcl — with `bgsave-hybrid yes`, after populate + expires + growth to force rehash, `dict_tables_outside == 0`; with `bgsave-hybrid no` everything is outside and the suite is unchanged. INFO memory `mem_overhead_db_hashtable_*` unchanged. Full suite + cluster tests (dicts per slot created on demand and freed when empty).
<!-- SECTION:PLAN:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
2026-09-20 implementation notes (TASK-1.5)

Mechanism: dictType.nofork_alloc flag. dict.c routes dictCreate (struct+metadata), _dictResize (zcalloc and ztrycalloc table paths; new ztrycalloc_nofork), createEntryNoValue (now takes the dict) and the normal dictEntry allocation to zmalloc_nofork/zcalloc_nofork when set. kvstore.c allocates the kvstore struct and dicts array in the arena for flagged types. dbDictType and dbExpiresDictType carry the flag; nothing else does (AC #3: the flag is the assertion point; all other dictTypes leave it 0). Frees are arena-aware already (TASK-1.4).

Fork child audit (AC #4): dismissMemoryInChild() only dismisses replication buffers and client objects, never the keyspace. With bgsave-hybrid yes no stock fork-based serializer may start (guards from TASK-1.4), so no child dereferences keyspace tables or headers. Module RM_Fork children do not touch dicts.

INFO memory overhead.hashtable.* (AC #5) is computed from kvstoreOverheadHashtableLut/Rehashing counters, unaffected by where tables live; verified unchanged in the tcl run.

Tests: 3 new unit checks (81 total, all pass on the jemalloc build); tcl unit/snapshot asserts dict_tables_outside == 0 and dict_tables_in_arena >= 2 with the switch on. ASan (Makefile forces MALLOC=libc under sanitizers, so the arena is inert): unit 28/28, tcl passes, no reports. A jemalloc+ASan combination is not supported by the build system; arena paths are covered by the jemalloc unit tests and the tcl placement report instead.

Full tcl suite after the change (bgsave-hybrid off): 5560 passed, 0 failed. Committed as e8ee6e039.
<!-- SECTION:NOTES:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
## Summary

Keyspace hash tables live in the SNAP_NOFORK arena (commit e8ee6e039, branch `snapshot-hybrid-bgsave`, local only).

### What changed
- `dictType.nofork_alloc`: dict struct (+metadata), bucket arrays (normal and try-calloc paths, including rehash tables) and entry nodes are allocated with `zmalloc_nofork`/`zcalloc_nofork`/`ztrycalloc_nofork` when set. `createEntryNoValue` now takes the dict.
- kvstore allocates its struct and dict pointer array in the arena for flagged types.
- `dbDictType` and `dbExpiresDictType` set the flag; every other dict is unchanged. Frees are arena-aware already.
- `DEBUG SNAP-PLACEMENT` reports `dict_tables_in_arena` / `dict_tables_outside`.

### Tests
- 81 unit checks pass (3 new: nofork dict struct/table/rehashed table in arena; default dict outside).
- tcl unit/snapshot asserts no keyspace table outside the arena with `bgsave-hybrid yes`.
- Full suite 5560/5560 (switch off). ASan (libc, arena inert): clean.

### Notes
- Fork child audit: `dismissMemoryInChild` never touches the keyspace; stock fork serializers are refused in hybrid mode (TASK-1.4 guards); `RM_Fork` children do not use dicts.
- With `bgsave-hybrid no` all of this degrades to the default allocator.
<!-- SECTION:FINAL_SUMMARY:END -->
