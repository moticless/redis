---
id: TASK-1.1
title: >-
  Write-path audit: enumerate every keyspace mutation path that must go through
  clone-on-write
status: Done
assignee:
  - moti.cohen@redis.com
created_date: '2026-09-20 17:18'
updated_date: '2026-09-20 19:34'
labels:
  - bgsave
  - persistence
  - audit
  - docs
milestone: m-0
dependencies: []
documentation:
  - poc-hybrid-bgsave-spec.md
  - poc-hybrid-bgsave-write-path-audit.md
parent_task_id: TASK-1
priority: high
ordinal: 2000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Spec §7 invariant 1 (total clone-on-write) is the crux of the whole design: while a save is in progress, a background thread reads LIVE parent memory for every kvobj header (all headers are SNAP_NOFORK, including those of SNAP_FORK keys, and are shipped to the fork as copies) and for every SNAP_NOFORK value. The parent must therefore never write a fork-instant header or SNAP_NOFORK value in place while its slot is still PENDING or SCANNING. The spec requires a WRITTEN audit of every write path as proof, alongside the verification harness.

Produce a checked-in document listing every code path that can mutate a key's kvobj header (metadata slots, expiry, robj fields including lru, embedded key) or value, or the kvstore hash tables, with the function(s) involved and how each will be routed through the clone-on-write / temp-dict path implemented in snapshot.c. The audit is the input for the clone-on-write hooks task and the harness task.

Paths to cover at minimum (spec §7.1 plus gaps found in review): SET/APPEND/SETRANGE/GETSET/SETBIT and other string ops, INCR/DECR family, HSET/HDEL and hash field TTL (HEXPIRE etc., including listpackEx in-place mutation), SADD/SREM, ZADD/ZREM, list ops, EXPIRE/PERSIST/EXPIREAT (embedded expire in kvobj AND the db->expires kvstore), encoding conversions (listpack->hashtable, intset->listpack, embstr->raw, tryObjectEncoding, dbUnshareStringValue), DEL/UNLINK/dbDelete/dbOverwrite/setKey, lazy expiry on read, active expire cycle (keys and hash fields), maxmemory eviction, RESTORE-over-existing, COPY, MOVE, RENAME, SWAPDB/FLUSHALL/FLUSHDB (abort), module writes via the module API, DEBUG mutators, scripting and MULTI paths (should reduce to the same primitives), active defrag (relocates kvobjs and dict entries; must be paused for owed slots), and the LRU/LFU clock update in lookupKey (val->lru written on every read; RDB saves it as RDB_OPCODE_IDLE/FREQ under LRU/LFU maxmemory policies). Note that header-only writes on SNAP_FORK keys (EXPIRE on a big hash, LRU touch) are in scope too, since the header is copied live by the background thread.

For each path record: file/function, what it mutates (header/value/expires table/hash table), whether it is main-thread-only, and the proposed treatment (clone into temp dict, tombstone, abort save, atomic access, suspend, or no-op with justification). Flag any path where invariant 1 or 2 cannot be met without unacceptable cost; per spec §10 that is a valid spike-killing result and must be surfaced, not worked around.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 A checked-in audit document lists every write path enumerated in the description, plus any others found while reading db.c, t_*.c, expire.c, evict.c, defrag.c, module.c, cluster.c and debug.c
- [x] #2 Each entry names the entry-point function(s), what it mutates (header, value, expires table, hash table), and the chosen treatment (clone, tombstone, abort, atomic, suspend, or justified no-op)
- [x] #3 Header-only mutations of SNAP_FORK keys (expiry, metadata, LRU/LFU) are explicitly covered, since their headers are SNAP_NOFORK and copied live
- [x] #4 The LRU/LFU clock write in lookupKey and active defrag relocation are explicitly covered with a decision
- [x] #5 The db->expires kvstore and hash-field ebuckets (hexpires) consistency across clone/merge is explicitly covered
- [x] #6 Any path where invariant 1 or 2 cannot be satisfied is listed in a dedicated 'blockers' section (or the section states none were found)
- [x] #7 The document is referenced from the clone-on-write hooks task and the verification harness task
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
## Implementation plan (TASK-1.1)

1. Sweep src/db.c, object.c, t_string.c, t_hash.c, t_set.c, t_zset.c, t_list.c, t_stream.c, expire.c, evict.c, defrag.c, module.c, cluster*.c, debug.c, rdb.c (RESTORE), replication.c (RESTORE-like loads), lazyfree.c, keymeta.c, kvstore.c for every function that mutates (a) a kvobj header: metadata slots, embedded expire, robj type/encoding/lru/refcount/ptr, embedded key; (b) a value buffer in place (sds, listpack, intset, listpackEx, hashtable/skiplist/quicklist/stream/module); (c) db->keys / db->expires hash tables or hexpires ebuckets.
2. Identify the smallest set of shared primitives the commands funnel through (dbAdd, dbOverwrite/setKey, dbGenericDelete, lookupKeyWrite*, setExpire/removeExpire, kvobjSet, dbUnshareStringValue, per-type conversion functions, hash field TTL helpers, module OpenKey-for-write) and map every command family to them.
3. For each entry record: file:function, what it mutates, main-thread-only?, class relevance (all keys vs SNAP_FORK value-only), proposed treatment (clone header / clone header+value / tombstone / add-to-temp / abort save / atomic access / suspend / justified no-op).
4. Explicitly cover: lookupKey LRU/LFU write on read; active defrag relocation; db->expires and hexpires pointer consistency; FLUSHALL/FLUSHDB/SWAPDB abort; lazyfree thread frees.
5. Produce the document (Backlog doc "snapshot-write-path-audit") with a blockers section, then link it from TASK-1.7 and TASK-1.10.

Execution: the sweep is delegated to a research subagent writing a draft to the scratchpad; I review it against the code, correct, and publish via document_create.
<!-- SECTION:PLAN:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
Audit produced by a research subagent (118 tool calls over the tree) and reviewed line by line; corrections and decisions taken since are in the 'Reviewer addendum' section of the document. Checked in as poc-hybrid-bgsave-write-path-audit.md next to the spec (untracked like the spec until the user decides where design docs live).
<!-- SECTION:NOTES:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
## Summary

`poc-hybrid-bgsave-write-path-audit.md`: a line-referenced audit of every keyspace mutation path in this tree, produced as the proof artifact required by spec §7 invariant 1 and as the input for TASK-1.7 (clone-on-write hooks) and TASK-1.10 (harness).

### Contents
1. 32 shared primitives (lookupKey, dbAddInternal, dbSetValue, dbGenericDelete, setExpireByLink, removeExpire, kvobjSet, keyModified/updateLRM, dbUnshareStringValue, per-type conversions, hash field TTL helpers, estore/ebuckets, kvstore/dict, defrag, module APIs) with what each mutates (header word, metadata slots, value, keys/expires tables, subexpires) and on which thread.
2. Command family tables (strings, hashes, sets, zsets, lists, streams, arrays/GCRA, generic, keyspace-wide, server-driven, modules) mapping to primitives and listing every in-place write that bypasses them.
3. Reads that write (LRU/LFU on read, LRM on modify, lazy key and hash-field expiry, PFCOUNT cache, stream PEL updates, quicklist recompression, memory prefetch).
4. Every insert/delete/rehash site of db->keys and db->expires and every ExpireMeta writer.
5. Non-main-thread actors (bio lazy-free, IO threads, module GIL, fork children, defrag, the new scan thread).
6. Treatment proposal per entry: CLONE_HEADER, CLONE_HEADER_VALUE (via `lookupKeyWrite*` returning the temp-dict clone), TOMBSTONE, ADD_TEMP, SUSPEND_DURING_SAVE, ABORT_SAVE, NOOP with justification.
7. 17 blockers/risks, headline: the listpackEx `ExpireMeta` lives inside the value and is written by ebuckets and read by the serializer; kvobjSet steals and frees the source header; dbSetValue rewrites the original header in place; keyMetaTransition writes the old header's metadata; the robj header is one bitfield word.

### Decisions recorded in the addendum
- Blocker 9 (thread frees) resolved by the range-backed arena in zmalloc.c.
- `lookupKeyWrite*` returns the clone: adopted as the central hook design.
- listpackEx struct arena-born, never moved; serializer computes min TTL from the listpack.
- Headers in the arena force all fork-based serializers through the hybrid path (`bgsave-hybrid` gate).
<!-- SECTION:FINAL_SUMMARY:END -->
