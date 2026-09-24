---
id: TASK-1
title: >-
  Hybrid BGSAVE: background per-slot scan, SNAP_NOFORK arena, fork for SNAP_FORK
  values (epic)
status: To Do
assignee: []
created_date: '2026-09-20 17:17'
updated_date: '2026-09-21 12:34'
labels:
  - bgsave
  - persistence
  - epic
dependencies: []
documentation:
  - poc-hybrid-bgsave-spec.md
priority: high
ordinal: 1000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Umbrella task for the BGSAVE redesign spike described in poc-hybrid-bgsave-spec.md. Stock BGSAVE forks and serializes the whole keyspace from a copy-on-write snapshot; the fork page-table copy stalls the main thread and CoW amplification (page-granular copies triggered by small writes) can blow up RSS during a long save.

Terminology: every key is SNAP_NOFORK or SNAP_FORK, classified by its VALUE. All kvobj headers (preceding metadata + robj + embedded key), SNAP_NOFORK values and all kvstore hash tables live in the SNAP_NOFORK arena, a jemalloc arena whose extents are madvise(MADV_DONTFORK). SNAP_FORK values (multi-allocation encodings, module types, streams, anything over save-small-max-bytes) live in normal forkable memory. All snapshot code lives in src/snapshot.c with every function prefixed 'snap'.

The redesign: on BGSAVE the parent forks, but the child holds only SNAP_FORK values and owns the output file/socket. Parent background thread(s) scan the keyspace slot by slot: for a SNAP_FORK key the thread copies the whole kvobj allocation plus dbid into the channel and the child serializes it against its CoW snapshot; for a SNAP_NOFORK key the thread serializes it into a ~64 KB per-db staging buffer that is shipped to the child to write verbatim. The child tracks the currently selected db and emits SELECTDB only on change, because records from the thread(s) and its own serialization interleave. The main thread stays the sole keyspace mutator and, while a slot is still owed to the snapshot, applies changes via clone-on-write into a per-slot temp dict, merging back when the slot's scan completes.

Work is broken into subtasks grouped by milestone (M1 prove the crux, M2 parallelism/watermarks/diskless/AOF, M3 measurement). Spec §10: local commits on a branch are fine; do not push or open PRs without explicit approval; if invariant 1 (total clone-on-write) or invariant 2 (slot immutability) cannot be met on some path, surface it as a spike-killing result rather than working around it.

Known spec gaps found during review that subtasks must resolve: (1) byte-identical RDB is not achievable with interleaved records, target reload-equivalence; (2) in standalone mode a db has a single kvstore dict, so 'per-slot' degenerates to one slot per db unless the scan unit is a bucket range; (3) LRU/LFU clock updates on read write into the fork-instant kvobj header; (4) active defrag relocates entries and objects and must be paused while a slot is owed; (5) listpackEx (hash with field TTL) is two allocations; (6) db->expires references kvobj pointers and must stay consistent across clone/merge; (7) MADV_DONTFORK is Linux-only, fallback to stock BGSAVE required; (8) jemalloc thread caches are not arena-aware, so SNAP_NOFORK frees need a class-aware free path or they leak into default-arena allocations.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 All M1 subtasks are Done and the M1 report subtask records a go/no-go decision on invariants 1 and 2
- [ ] #2 M2 and M3 subtasks are Done or explicitly descoped with the reason recorded in this task
- [ ] #3 No commits from this work are pushed or turned into PRs without explicit approval
<!-- AC:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
2026-09-20 progress and scope finding

Done: TASK-1.2 (arena, commit 538391875), TASK-1.3 (scan thread, commit 6871810d4). In progress: TASK-1.4 (placement, implemented, awaiting full-suite run), TASK-1.1 (audit delivered, being published).

Design refinement: the SNAP_NOFORK arena is backed by ONE reserved DONTFORK address range (16 TB PROT_NONE + MADV_DONTFORK, extents carved by bump pointer + mprotect), so `zmalloc_is_nofork(p)` is a range check and the generic zfree/zrealloc keep arena memory in the arena. No class bit, no class-aware call sites needed on free/realloc paths.

SCOPE FINDING (affects TASK-1.9, 1.14, 1.15 and the spec §9 M1 text): with every kvobj header in the DONTFORK arena, no fork child can read the keyspace. Stock BGSAVE, replication full sync (disk and diskless) and AOF rewrite all crash in the child. The M1 plan to keep those on the stock serializer is infeasible while placement is enabled. Resolution taken: startup-only config `bgsave-hybrid` (default no) gates the arena and all placement; with it off the server behaves exactly as stock. With it on, the three fork-based entry points refuse (log + C_ERR) until TASK-1.9 provides the hybrid BGSAVE, which also serves disk-based replication (the sync sends the produced file). Diskless full sync and AOF rewrite remain unavailable in hybrid mode until M2 (TASK-1.14, 1.15). This is recorded as a correction in the spec (§9 addendum) and in TASK-1.9.

Audit (TASK-1.1) headline for TASK-1.6/1.7: the only viable central hook for clone-on-write is `lookupKeyWrite*` returning the temp-dict clone (eager copy of SNAP_NOFORK values); dbSetValue's in-place header swap, kvobjSet's steal-and-free of the source header and keyMetaTransition's write into the old header's metadata must all be bypassed for owed keys; listpackEx ExpireMeta is written by ebuckets (estoreAdd/Remove, ebExpire, ebSetMetaExpTime) and read by rdbSaveObject, so LISTPACK_EX clones need their own struct, estoreRemove of originals must be deferred to merge, active subexpire must skip owed slots, and the background serializer should compute the min field TTL from the listpack instead of reading meta.

## M1 status (2026-09-21)

Done: 1.1-1.10, 1.18 (adopted ideas from the sibling `hybrid-bgsave` branch). In progress: 1.16 (report written, waiting on the final full-suite and stress runs). To Do: M2 tasks 1.11-1.15, 1.17.

Branch `snapshot-hybrid-bgsave`, 8 local commits, nothing pushed. Report: `poc-hybrid-bgsave-m1-report.md`. Go/no-go: go for M2 with conditions (production-shaped measurement first, eviction-during-save policy, diskless sync and AOF rewrite as the first M2 items).

## Aligned with upstream (2026-09-21)

Branch merged with redis/redis `unstable` at 0d6266f2d (206 commits ahead of the old base 0c1c74706; remote `upstream` added to this clone, `origin` is the filipecosta90 fork and was not behind). Merge commit f93f77adf: three one-hunk conflicts (Makefile object list, server.c cron stats, aof.c refusal), both sides kept. Follow-up fb0cbe13f: upstream's hinted hash templates write a registry before the databases in rdbSaveRio; moved into rdbSaveRioHeader so the hybrid child emits it (templated hashes are SNAP_FORK by default and serialized by the child in REF form). kvBits now exists on this base (2 bits sds type, no_evict, 5 free bits), which is where a per-key scan generation would live if pursued in M2. Snapshot unit tests 134/134, unit/snapshot 29/29 after the merge; full stock suite running.
<!-- SECTION:NOTES:END -->
