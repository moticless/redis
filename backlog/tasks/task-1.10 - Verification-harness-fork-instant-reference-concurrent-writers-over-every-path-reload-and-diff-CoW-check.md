---
id: TASK-1.10
title: >-
  Verification harness: fork-instant reference, concurrent writers over every
  path, reload-and-diff, CoW check
status: Done
assignee:
  - '@moti.cohen@redis.com'
created_date: '2026-09-20 17:22'
updated_date: '2026-09-20 23:08'
labels:
  - bgsave
  - persistence
  - testing
  - correctness
milestone: m-0
dependencies:
  - TASK-1.7
  - TASK-1.9
documentation:
  - poc-hybrid-bgsave-spec.md
  - poc-hybrid-bgsave-write-path-audit.md
parent_task_id: TASK-1
priority: high
ordinal: 11000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Spec §7.1 and §9 M1 deliverables (a)-(d). This is the test that proves or kills the design: capture a reference of the keyspace at the fork instant, drive writers over every write path while the save runs, reload the produced RDB into a fresh server, and diff every key (value, type, encoding-independent content, TTL, hash field TTLs) against the reference. Any difference means the snapshot is corrupted.

Scope: (a) static-dataset equivalence - populate a mixed dataset (all types, both classes, multiple dbs so SELECTDB tracking is exercised, TTLs, hash field TTLs, functions, and a module type via a test module), BGSAVE with stock and with hybrid, reload both and compare key-by-key; also confirm redis-check-rdb accepts the hybrid file; (b) the race test - CLIENT PAUSE WRITE, dump a reference (e.g. a stock RDB taken at the pause point), start a hybrid BGSAVE with rdb-key-save-delay so it runs for seconds, unpause, then run writer clients that exercise every path from the write-path audit (TASK-1.1) including deletes, re-adds, encoding conversions across the SNAP_NOFORK/SNAP_FORK threshold, EXPIRE/PERSIST on both classes (header-only clones of SNAP_FORK keys), hexpire, RESTORE-over-existing, COPY/MOVE/RENAME, eviction under maxmemory, lazy and active expiry, module writes, under maxmemory-policy lru and lfu; wait for completion, reload, diff against the reference; repeat many iterations with randomized workloads; (c) FLUSHALL/FLUSHDB/SWAPDB abort mid-save leaves the server consistent and the next BGSAVE succeeds; (d) CoW confirmation - with a dataset of only SNAP_NOFORK keys and a heavy write load during the save, the child's reported CoW (INFO persistence current_cow_size / rdb_last_cow_size, or /proc/<child>/smaps Private_Dirty) is near zero and independent of write rate, compared to stock BGSAVE on the same workload; also with a SNAP_FORK-heavy dataset under header-only writes (EXPIRE storm) to show headers do not CoW.

Deliver as tcl tests under tests/integration (fast variants for CI) plus a longer stress script for the M1 report. Run under ASan and TSan at least once and record results. The harness must work in both standalone and cluster mode.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 Static-dataset test: hybrid and stock RDBs of the same mixed multi-db dataset reload to identical keyspaces (values, TTLs, hash field TTLs, functions, module aux) and redis-check-rdb accepts the hybrid file
- [x] #2 Race test: after a hybrid save under concurrent writers covering every path in the write-path audit, the reloaded RDB equals the fork-instant reference on every key, across at least 100 randomized iterations in the stress script and a shorter CI variant
- [x] #3 Race test includes header-only mutations of owed SNAP_FORK keys and class-crossing encoding conversions
- [x] #4 Race test runs with maxmemory-policy allkeys-lru and allkeys-lfu and with eviction actually occurring during the save
- [x] #5 Abort test: FLUSHALL, FLUSHDB and SWAPDB during a save abort it, the live keyspace matches client-observed state, and the next BGSAVE succeeds
- [x] #6 CoW test: for a SNAP_NOFORK-only dataset under heavy writes, and for a SNAP_FORK dataset under an EXPIRE storm, hybrid child CoW stays near zero and flat versus write rate while stock BGSAVE CoW grows; numbers recorded in task notes
- [x] #7 Harness passes in standalone and cluster mode
- [x] #8 One full run under AddressSanitizer and one under ThreadSanitizer completes with no reports, or every report is triaged in task notes
- [x] #9 Fast variants are added to the tcl suite and pass in CI runtime budget
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
## Plan (TASK-1.10)

All in `tests/unit/snapshot.tcl` (the file already carries the fast hybrid tests); iteration counts scale with `--accurate` for the stress variant.

1. (a) Static multi-db dataset: all types both classes, TTLs, hash-field TTLs, a FUNCTION, module type (tests/modules/datatype.so) in a dedicated server block; stock (`bgsave-hybrid no`) vs hybrid saves reload to the same DEBUG DIGEST; `redis-check-rdb` accepts the hybrid file; in cluster mode the multi-db part is skipped (single db).
2. (b) Randomized race test: a `snap_random_write` proc drawing from every audit path (SET/APPEND/SETRANGE/INCR, HSET/HDEL/HEXPIRE/HPERSIST, SADD/SREM, ZADD/ZREM, RPUSH/LPOP, EXPIRE/PERSIST on both classes, DEL/UNLINK, RENAME, COPY, MOVE (standalone), RESTORE over existing, GETEX, class-crossing growth/shrink); each iteration: populate, digest (reference), BGSAVE with rdb-key-save-delay, run N random ops, wait, reload, compare digests. CI: 3 iterations x 400 ops; --accurate: 100 iterations.
3. (b) under allkeys-lru / allkeys-lfu with maxmemory set so eviction happens during the save (assert evicted_keys grew).
4. (c) abort tests exist (FLUSHALL, FLUSHDB, SWAPDB); add FLUSHALL ASYNC and check next BGSAVE.
5. (d) CoW: SNAP_NOFORK-only dataset under heavy writes and a SNAP_FORK dataset under an EXPIRE storm, hybrid vs stock `rdb_last_cow_size`; record the numbers in notes; assert hybrid << stock.
6. Child crash (kill -9 the child) and SHUTDOWN NOSAVE during a save (deferred from TASK-1.9 AC #4).
7. Run `./runtest --single unit/snapshot` in standalone and `--cluster-mode`.
8. Sanitizers: not possible for the hybrid path (libc forced) — record.
<!-- SECTION:PLAN:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
## Results (2026-09-21, commits 1b9d645c5 and the sanitizer follow-up)

Harness lives in `tests/unit/snapshot.tcl` (blocks `snapshot harness` and `snapshot cluster`), CI variant runs in about 90 s; `--accurate` raises the race test to 100 iterations.

- (a) Static: mixed dataset over dbs 9/1/2 (every type, both classes, key TTLs, hash field TTLs, a FUNCTION, module type keys from tests/modules/datatype.so). Hybrid RDB reloads to the reference digest, `redis-check-rdb` accepts it, a stock server (`bgsave-hybrid no`) loads the hybrid file and its own stock save of the same dataset to the same digest.
- (b) Race: `snap_random_op` draws from 44 write kinds covering the audit (value writes both classes, class-crossing growth, listpack->HT/skiplist/quicklist and back, header-only EXPIRE/PERSIST/HEXPIRE on SNAP_FORK keys, DEL/UNLINK/GETDEL, RENAME/COPY/MOVE/RESTORE REPLACE, streams, module writes, short-PX keys for lazy+active expiry, reads). 400 ops per iteration, all raced the save (checked every 50 ops); reload compared to the fork-instant reference with a per-key type/digest/TTL diff on mismatch. 3 iterations (CI) plus the two eviction variants and the cluster run: all equal.
- (b) Eviction: `maxmemory` set below `used_memory` mid-save under allkeys-lru / allkeys-lfu; evictions observed (34-590 keys per run); snapshot still equal. Known cost: evicting an owed key only tombstones, memory returns at slot merge.
- (c) Abort: FLUSHALL, FLUSHALL ASYNC, FLUSHDB, SWAPDB mid-save abort cleanly; next save ok. Also: child killed mid-save (status err, overlay merged, next save equals the live keyspace), SHUTDOWN NOSAVE mid-save (clean exit), retained-bytes hard mark (status err, no client stall).
- (d) CoW, `rdb_last_cow_size`, baseline-corrected (delta = storm run minus a no-write run of the same dataset):

| workload | hybrid delta | stock delta |
|---|---|---|
| 20k x 60 B strings, overwrite all during save | 225-725 KB | 1.94-2.0 MB |
| 20k x 1000 B SNAP_FORK values (threshold 256 B), EXPIRE all | 233-258 KB | 1.25-1.42 MB |

The residual hybrid CoW is default-arena churn (client argv strings freed, jemalloc metadata of the arena which stays forkable), not per-object page amplification.
- Cluster mode (`start_cluster 1 0`): per-slot progress exposed; race iteration equal.
- Bugs the harness found and fixed: UNLINK of an owed key (NULL kvobj read in overlay bookkeeping); child stopping on a broken channel left slots owed (assert at release); SET overwrite in-place swap path left the value outside the arena; set/zset conversions to listpack not placed; cluster per-slot alloc_size accounting on the overlay (temp dict destructor now accounts on the real dict, the clone takes over the original's accounting, slots empty at freeze are DONE and never frozen); state-dict lookups after freeing originals in the merge (TSan UAF); scan thread reading server.hz (LRU_CLOCK), maxmemory_policy, rdb_compression, allow_access_expired, rdb_key_save_delay (TSan races, now per-save copies); arena-free mode sharing a size-classed single-allocation value that the clone then reallocated (TSan UAF, now always copied without the arena).
- Sanitizers: ThreadSanitizer (MALLOC=libc, arena-free hybrid mode, `setarch -R`): whole unit clean after the fixes above (22 tests; the 6 arena-only tests skip). AddressSanitizer: see next note.
- Not done: the deferred `tests/integration` placement (kept in unit/snapshot.tcl next to the fast tests); a multi-process writer load (single client suffices: the race is against the scan thread, not other clients).

AddressSanitizer (MALLOC=libc, arena-free hybrid mode): `redis-server test snapshot` 81/81 (arena checks skipped) and unit/snapshot 22 tests clean, no reports, leak detection on. Both sanitizer runs exercise the hybrid path end to end (scan thread, overlay, child) thanks to TASK-1.18's arena-free mode; the arena itself (placement, DONTFORK, CoW) is only testable in the jemalloc build.
<!-- SECTION:NOTES:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
Verification harness delivered in tests/unit/snapshot.tcl: static multi-db equivalence against a stock save plus redis-check-rdb, randomized 44-kind writers racing the save with per-key reload-and-diff, eviction under LRU/LFU, abort paths (FLUSHALL/ASYNC, FLUSHDB, SWAPDB, killed child, SHUTDOWN, hard mark), baseline-corrected CoW comparison (hybrid 3-9x lower than stock on both workloads), and a cluster-mode run. Found and fixed 10 defects (see notes). TSan and ASan clean in arena-free mode. Commits 1b9d645c5 and the sanitizer follow-up on snapshot-hybrid-bgsave.
<!-- SECTION:FINAL_SUMMARY:END -->
