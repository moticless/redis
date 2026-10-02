# Hybrid BGSAVE (this branch) versus Valkey's forkless save

Written 2026-10-02. Our side is `snapshot-hybrid-bgsave` at `dec2dcfa8` (two channels, 100 MB bound, no spool,
AOF rewrite with the RDB preamble through the hybrid). Valkey's side is `~/git/valkey` unstable at `06d35aca6`,
which contains Forkless Save (#4460) and its two follow-ups (#4610, #4643); the forkless full-sync change (#4797)
is a local branch `pr4797` on 45 newer upstream commits and is not merged, so it is treated below as "in flight".
Facts about Valkey come from its code (`src/forkless.c`, `src/bgiteration.c`, `src/blocked.c`, `src/rdb.c`,
`src/replication.c`, `valkey.conf`); numbers come from `utils/snap-bench/RESULTS.md` runs 12, 18, 31, 32 and 33,
all on c7i.4xlarge hosts with both binaries on the same host and day.

## 1. The two designs in one paragraph each

**Hybrid.** Small values (any single-allocation encoding up to 4 KB: strings, listpack and intset collections,
listpack-ex hashes) and every kvobj header and dict table live in a jemalloc arena marked `MADV_DONTFORK`. A save
freezes dict layouts, forks, and runs two writers at once: a parent scan thread walks the live dicts under a
published bucket window and serializes the small values, and the fork child serializes the big values from its
copy-on-write view and owns the output (file or replica socket). The two meet through two pipes fed from bounded
in-memory queues; the child drains the small backlog first and takes a big value when it is low. A write to a key
the scan has not reached serializes that bucket's chain first (emit-ahead) and then proceeds in place; a write
inside the window being read goes to a side table that drains when the window moves. The main thread never waits
and no client is ever blocked by the save. Cost: a fork stall proportional to the fork-class values' page tables,
copy-on-write on big values that are written during the save, one scan thread's worth of CPU.

**Valkey forkless.** No fork. Every key carries a 4-byte iteration epoch (allocated only when
`forkless-infrastructure-enabled yes`). A main-thread timer (every 2 ms, 1 ms budget) scans the hashtables and
pushes dbEntry references to a background thread, which serializes them with the stock RDB code straight into the
file. A write to a key not yet serialized is "expedited": the key jumps to the front of the queue and the client
is blocked (`BLOCKED_INUSE`, no timeout) until the thread has serialized that key, unless the value is a non-INT
string of at most 512 bytes and a 1 MB clone pool has room, in which case it is copied instead. Reads are never
blocked. Serialization is atomic per key, so a writer to a 90 MB hash waits for the whole hash. Memory held: the
queue references, the in-use records and objects deleted while in use. Cost: main-thread CPU for feeding and
cloning, blocked writers on in-use keys, no memory bound.

## 2. Design, side by side

| | Hybrid | Valkey forkless |
|---|---|---|
| fork | yes, with the small values and all headers excluded (`MADV_DONTFORK` arena) | none |
| who serializes | scan thread (small) + fork child (big) in parallel | one background thread, everything |
| output path | child writes the file or socket; small values cross two pipes from bounded queues | the thread writes the file directly, no pipe |
| point-in-time rule | fork instant; a write to an unscanned bucket serializes the bucket first, then proceeds | iterator epoch per key; a write to an un-iterated key expedites it and blocks the writer until done |
| writer blocking | never | per key, for the whole serialization of that key; small strings (≤ 512 B, 1 MB pool) cloned instead |
| big values | serialized by the child from its CoW copy; writers never wait | atomic per key; a 90 MB hash blocks its writers for its whole serialization (20-35 s measured) |
| memory held during a save | child CoW on written big values; queue ≤ 100 MB; side table bounded by `bgsave-hybrid-max-retained-bytes` (abort at the hard mark) | references + in-use records + deleted-in-use objects; no bound, no abort on memory |
| fork stall | proportional to fork-class page tables: 1.6 ms (small), 14-18 ms (mixed, mixed1m), 26-34 ms (big1m) | 0 |
| main-thread cost | emit-ahead serialization under writes; cron drains; 0.3-16 CPU-s per save, which is the write load itself | feeder timer, cloning, blocking bookkeeping: 11-28 CPU-s per save, 1.6-2.7x ours under writes |
| rehash during a save | frozen dicts: rehash paused, resize refused (lazily, on demand per dict) | rehash paused on the table and on HT-encoded values in use |
| consistency across dbs | frozen at the fork instant | epoch; SWAPDB keeps original db ids |
| recovery after a failed save | child dies, overlay merges, nothing outlives the save | iterator terminated; a failed save blocks writes until resolved (`failed bgsave prevents writes` test) |
| allocator requirement | 64-bit Linux + jemalloc for the arena (runs without it, no memory benefit) | none |
| switchable at runtime | `bgsave-hybrid` immutable at startup (arena placement) | `forkless-infrastructure-enabled` immutable; `bgsave-default-method fork|forkless` modifiable |
| knobs | `save-small-max-bytes` 4 KB, `bgsave-hybrid-max-queued-bytes` 100 MB, `bgsave-hybrid-max-retained-bytes` 256 MB | none beyond the two above; pool and queue sizes are compile-time |
| observability | INFO snap_* (in progress, keys, queued bytes, retained bytes, side entries, hard-mark aborts, main-thread emissions) + latency events `hybrid-freeze` | `forkless_estimated_seconds_remaining`, debug section queue metrics, `rdb_current/last_bgsave_type` |

## 3. Functionality coverage today

| Operation | Hybrid | Valkey forkless |
|---|---|---|
| BGSAVE to disk, `save` points, scheduled saves | yes | yes |
| SAVE (foreground) | stock path | stock path; refused while a forkless save runs |
| Disk-based full sync | yes, through the hybrid file | fork |
| Diskless full sync, rdb-channel and single connection | yes (TASK-1.14), child owns the socket | fork; #4797 (unmerged) adds it for replicas advertising `inband-repl`, else fork |
| BGREWRITEAOF, RDB preamble | yes (TASK-1.15 stage 1) | fork, and deferred while a forkless save runs |
| BGREWRITEAOF, command form | fork child | fork |
| Cluster mode | yes; per-slot dicts attach on demand (TASK-1.21.6); slot-migration snapshots fork the stock child | full-scan iterator is slot-aware; the slot iterator is `serverAssert(false); // Not yet implemented`; slot migration forks and fails during a forkless save |
| Module data types | yes if every loaded module sets `REDISMODULE_OPTIONS_HANDLE_HYBRID_BGSAVE`, else stock child; vector-sets opted in, RedisJSON and Search live in their own repos | yes if every module sets `VALKEYMODULE_OPTIONS_HANDLE_FORKLESS`, else fork; MODULE LOAD refused during a save |
| Functions | child writes them | main thread writes them in the header at start |
| FLUSHALL / DEBUG RELOAD during a save | BGSAVE aborts (as stock kills its child); an AOF rewrite keeps running on the detached dbs | iterator terminated, save cancelled |
| FLUSHDB during a save | the db detaches to the save, the save completes | consistent iterators terminated |
| SWAPDB during a save | the save continues, keys under their fork-instant db | the save continues, original db ids kept |
| Writes during a save | never blocked; served in place once the bucket is behind the scan | blocked per in-use key, cloned only for strings ≤ 512 B |
| Scripts | unaffected | undeclared-key scripts busy-wait the main thread (`usleep(100)` loop, logged) |
| Memory bound | retained bytes hard mark aborts the save; queue bound; both excluded from eviction accounting | none |
| 32-bit | builds and passes (no arena, no memory benefit) | n/a |
| Tests | one tcl unit with a randomized race harness, C unit tests, full suite with the feature on, TSan/ASan | fork/forkless parameterized tests across rdb.tcl, info, other, introspection, moduleapi; C++ unit tests |

## 4. Measured, same host and day

Save time, seconds, 4 GB datasets, two reps (ranges):

| dataset | rate | stock fork | hybrid | Valkey forkless |
|---|---|---|---|---|
| small (100 B strings, listpack hashes) | idle / 20k / 50k | 34.8 / 34.8 / 35.4 | 32.0 / 32.1 / 30.8 | 36.5 / 45.8 / 43.8 |
| mixed (9 KB hash tables) | idle / 20k / 50k | 36.9 / 36.5 / 38.8 | 19.6 / 20.1 / 20.2 | 38.1 / 43.8 / 47.1 |
| mixed1m (24 hashes of 1M fields) | idle / 20k / 50k | 39.8 / 44.4-46.0 / 43.6-45.0 | 25.8-26.3 / 21.9-25.2 / 25.1-25.5 | 49.7-53.7 / 44.9-49.6 / 45.3-49.5 |
| big1m (everything fork-class, run 12) | 20k / 50k | 34.2 / 35.9 | 35.6 / 35.2 | 34.7 / 34.9 |
| mixed, one core | idle / 20k / 50k | 46-47 / 52-56 / 68-69 | 47 / 53-54 / 60-61 | 51-55 / 64-66 / 68-70 |

Memory, latency and rate (the columns that decide the operator's question):

| | stock fork | hybrid | Valkey forkless |
|---|---|---|---|
| extra memory, small at 50k | 3.3 GB | 7 MB | 21 MB |
| extra memory, mixed at 50k | 3.2 GB | 1.35 GB (hash tables' CoW) | 16 MB |
| extra memory, mixed1m at 50k | 3.3 GB | 1.55 GB | 8 MB |
| fork stall, small / mixed / mixed1m | 32 / 36 / 31 ms | 1.6 / 18 / 15 ms | 0 |
| worst write, small / mixed / mixed1m | 34 / 37 / 32 ms | 3.9 / 21 / 18 ms | 5 ms / 18 ms / 20.7-23.9 s |
| write p99, mixed at 50k | 0.42 ms | 0.43 ms | 13.2 ms |
| offered rate delivered, mixed1m 50k / big1m 50k | 100% / 100% | 100% / 100% | 76% / 41% |
| clients blocked at the peak, mixed1m | 0 | 0 | 16 |
| main-thread CPU per save, mixed at 50k | 14.6 s | 9.3 s | 25.3 s |
| full sync of mixed1m, 20k writes (run 18): sync time / primary extra | 17 s / 1.4 GB | 17 s / 0.8 GB | 44-47 s / 13 MB, 92% of the rate, 0.9 s worst write (PR 4797) |

## 5. Who wins

**Save time: hybrid.** Half of stock on mixed and mixed1m at every write rate, 10% better on small, parity on
big1m where nothing is small. Forkless is 5-50% slower than stock everywhere because one thread does what stock's
child does, with the feeder's cost on top; on one core all three take turns and the hybrid's win shrinks to the
50k cells.

**Memory: forkless, on datasets with fork-class values.** Its 8-26 MB against our 0.5-1.6 GB of copy-on-write on
the 9 KB hash tables and the million-field hashes is the one column where it is clearly better. On small-value
datasets both hold memory flat (7 MB against 21 MB), and the hybrid's queue (50-100 MB, released at the end, not
counted for eviction) is the only save-time memory it needs there.

**Latency and throughput under writes: hybrid.** Stock's write latency and full rate in every cell. Forkless pays
with the main thread (1.6-2.7x the CPU) and with blocked writers: 7-13 ms p99 on 9 KB hashes, and on million-field
hashes a write waits 20-35 seconds while 41-76% of the offered rate is delivered. That is not a tuning problem, it
is what "serialize each key atomically on one thread" means when a key is a million fields; the hybrid gives
those keys to a fork child precisely so that no writer waits.

**Fork stall: forkless.** Zero against our 1.6-34 ms (stock's 30-60). Ours is bounded by the fork-class values'
page tables, so it is small on small-value datasets and stock-like when everything is fork-class.

**Big values: hybrid.** Fork parity (same save, same memory, same stall as stock) where forkless fails to serve
writes.

**Coverage: hybrid, today.** Disk and diskless full sync, AOF rewrite with the preamble, cluster, FLUSHDB and
SWAPDB continuation all run through the hybrid; forkless covers BGSAVE to disk and defers or forks everything
else, with full sync in flight (#4797) and slot iteration unimplemented.

**Operational simplicity: forkless.** No fork, no jemalloc dependency, no arena, a modifiable method switch,
and the write-time rules are one sentence. The hybrid has a startup-only switch, an arena with placement
rules, and three memory knobs.

## 6. The functionality gap, today

What forkless has that the hybrid does not:
- `bgsave-default-method` switchable at runtime between fork and forkless (ours is fixed at startup because
  values are placed in the arena from the first write).
- No fork at all, so no stall and no copy-on-write on anything; no allocator or address-space requirement.
- Save cancellation (`BGSAVE CANCEL`) as a user command (ours aborts on FLUSHALL, a killed child or the hard
  mark; a CANCEL verb would be a small addition).
- A fork/forkless parameterization across many existing test files.

What the hybrid has that forkless does not:
- Writers never blocked, at any value size; stock's write latency under a save.
- Save time half of stock on mixed datasets, parity on all-big ones.
- Diskless and disk-based full sync, rdb-channel and single connection, through the same mechanism; the
  replication stream buffered on the replica during a sync of this length is 23-33 MB (run 18).
- AOF rewrite with the RDB preamble.
- Cluster mode with per-slot dicts attached on demand (forkless has an unimplemented slot iterator).
- FLUSHDB and SWAPDB during a save without losing the save (forkless cancels on FLUSHDB).
- Bounded save-time memory with an abort at the hard mark, both bounds excluded from eviction accounting.
- Scripts unaffected (forkless busy-waits the main thread for undeclared keys).

## 7. Eventually

What each side can close, and what is structural.

**Forkless can close:** full sync (#4797 is written; it falls back to fork for replicas without the
capability and its review found ordering holes to fix); AOF rewrite with the preamble (same iterator, different
file name, plausible); the slot iterator for slot migration; a bigger and type-wider clone pool (its own TODO
names small listpacks and sorted sets), which would stop blocking writers on small collections; a memory bound
and abort. **Structural:** a write to a large in-use value waits for the value, and the only way out is chunked
serialization of hash tables, skiplists and streams with version tracking, which is a different and much larger
design (their "eventual consistency" iterator for replication is a step in that direction, not a solution);
the main-thread feeder and cloning cost scales with the write rate; the single serializing thread caps save
speed at roughly stock's child.

**The hybrid can close:** the AOF command form from the scan thread (planned, stage 2); inline replication so a
full sync buffers nothing on the primary (designed, the dependency field is in place); per-type opt-in to the
small class so listpack hashes up to 16 KB and small hash tables stop paying copy-on-write, which is the whole
of the mixed dataset's 0.5-1.4 GB gap to forkless; a `BGSAVE CANCEL`; the bundled modules' opt-in bit in their
repositories; a runtime switch is possible with the arena flipped forkable (the teammate's POC did it with an
madvise registry) but costs a stock-like stall until the next restart. **Structural:** values that are not
single allocations (hash tables, skiplists, quicklists, streams, module types) go through a fork, so their
page tables set the stall and their writes set the copy-on-write; a host without jemalloc or 64-bit address
space gets the scan but not the memory benefit.

**Where they end up.** On small-value datasets the two converge: both hold memory flat, the hybrid saves
faster and the forkless has no stall. On datasets with large collections they diverge for good: the hybrid
keeps stock's latency and halves its save time at the price of copy-on-write on the written big values; the
forkless keeps memory flat at the price of blocking the writers of those values. The operator's question that
picks the winner is "do my large keys take writes during a save". If yes, hybrid; if the dataset is small
values only and the fork stall is the complaint, forkless.
