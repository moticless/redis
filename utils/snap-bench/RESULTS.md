# Hybrid BGSAVE vs stock BGSAVE (0c1c74706): save time, peak memory, idle vs traffic, item size

Date: 2026-09-21. Tool: `utils/snap-bench/bench.py` driven by `run.sh` (this directory).

## Setup

| | |
|---|---|
| stock | `0c1c74706` (merge base of this branch, upstream unstable), plain build, `/tmp/redis-base` worktree |
| hybrid | branch `snapshot-hybrid-bgsave` (commit with the benchmark), plain build, `--bgsave-hybrid yes` |
| common flags | `save "" appendonly no activedefrag no disable-thp yes io-threads 1 latency-monitor-threshold 5 jemalloc-bg-thread yes`, dump dir on tmpfs (`/dev/shm`) so save time does not measure disk writeback |
| host | i7-1260P (4P+8E, 16 threads), 31 GB RAM, kernel 6.8, governor powersave, THP madvise, 2 GB swap partly in use; server pinned to CPUs 0-7, memtier and harness to 8-15 |
| reps | 2 per cell, medians reported with [min-max] |

Datasets (each about 250-300 MB `used_memory`, under the 400 MB budget):

| name | contents | class under hybrid |
|---|---|---|
| small | 1.2 M strings x 100 B, 200 k listpack hashes (10 x 40 B) | all SNAP_NOFORK (arena) |
| mixed | 800 k strings x 50 B, 200 k 1-element lists, 200 k 1-element sets, 16 k hashtable hashes (64 x 96 B, about 9 KB) | 45% of bytes NOFORK, 55% in 1.3% big keys (FORK) |
| big | 22 hashtable hashes x 100 k fields x 64 B (about 10 MB each), 2 zsets x 300 k | all SNAP_FORK |
| big-grow | same data as big; the traffic inserts random fields/members instead of overwriting one field | all SNAP_FORK |

Traffic: memtier same-size in-place writes (SET / HSET f0 / LSET 0 / SADD / ZADD existing member) at 20k and 50k ops/s total, spread over the dataset's key classes by the bytes each class occupies, started 3 s before BGSAVE and kept running past its end; a 5 ms PING probe runs throughout.

Metrics: `save s` = BGSAVE issued until `rdb_bgsave_in_progress` and `snap_save_in_progress` are 0. `extra mem peak` = max over 100 ms samples of (server RssAnon+VmSwap growth since just before the save) + (fork child Private_Dirty). `abs peak` = server RssAnon + child Private_Dirty. `rdb_last_cow` = the child's own CoW report. CPU seconds from /proc for the main thread, the other threads (the hybrid scan thread) and the child. Latency events from `LATENCY LATEST`.

## Results

Raw: `results.csv` (baseline rows from run 4, hybrid rows from run 5, both on tmpfs with plain builds). Medians of 2 reps, [min-max].

| dataset | rate | config | n | save s | fork ms | extra mem peak MB | parent growth MB | child dirty MB | abs peak MB | rdb_last_cow MB | main cpu s | threads cpu s | child cpu s | ping p99 ms | ping max ms | load ops/s | load p99 ms | load max ms | stall s |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| big | 0 | base | 2 | 1.46 [1.41-1.51] | 5.585 [5.22-5.95] | 1 [1-1] | 0 [0-0] | 1 [1-1] | 262 [262-262] | 1 [1-1] | 0.06 [0.06-0.06] | 0.005 [0-0.01] | 1.38 [1.36-1.4] | 0.528 [0.429-0.627] | 3.9905 [1.533-6.448] | - | - | - | - |
| big | 0 | hybrid | 2 | 1.44 [1.43-1.45] | 5.895 [5.28-6.51] | 1 [1-1] | 0 [0-0] | 1 [1-1] | 289 [289-289] | 1 [1-1] | 0.055 [0.05-0.06] | 0.01 [0-0.02] | 1.36 [1.35-1.37] | 0.7095 [0.522-0.897] | 5.223 [4.003-6.443] | - | - | - | - |
| big | 20000 | base | 2 | 1.495 [1.47-1.52] | 5.235 [5.14-5.33] | 1.5 [1-2] | 0 [0-0] | 1.5 [1-2] | 258.5 [254-263] | 1.5 [1-2] | 0.445 [0.44-0.45] | 0.01 [0.01-0.01] | 1.4 [1.39-1.41] | 0.3815 [0.356-0.407] | 2.3745 [1.076-3.673] | 19746.5 [19697-19796] | 0.459 [0.455-0.463] | 5.727 [5.695-5.759] | 0 [0-0] |
| big | 20000 | hybrid | 2 | 1.48 [1.41-1.55] | 5.455 [5.16-5.75] | 1.5 [1-2] | 0 [0-0] | 2 [2-2] | 285.5 [281-290] | 2 [2-2] | 0.47 [0.47-0.47] | -0.005 [-0.02-0.01] | 1.375 [1.29-1.46] | 0.3985 [0.373-0.424] | 3.5535 [0.783-6.324] | 19746 [19698-19794] | 0.475 [0.471-0.479] | 6.047 [5.727-6.367] | 0 [0-0] |
| big | 50000 | base | 2 | 1.57 [1.51-1.63] | 3.44 [2.78-4.1] | 1 [1-1] | 0 [0-0] | 1 [1-1] | 251 [251-251] | 1 [1-1] | 0.68 [0.67-0.69] | 0 [0-0] | 1.485 [1.4-1.57] | 0.336 [0.313-0.359] | 0.4225 [0.377-0.468] | 49391 [49389-49393] | 0.215 [0.191-0.239] | 3.751 [3.087-4.415] | 0 [0-0] |
| big | 50000 | hybrid | 2 | 1.47 [1.45-1.49] | 3.875 [3.75-4] | 2 [2-2] | 0 [0-0] | 2 [2-2] | 279 [279-279] | 2 [2-2] | 0.685 [0.67-0.7] | 0.005 [0-0.01] | 1.385 [1.38-1.39] | 0.3375 [0.332-0.343] | 3.51 [2.671-4.349] | 49392 [49391-49393] | 0.227 [0.207-0.247] | 3.703 [3.023-4.383] | 0 [0-0] |
| big-grow | 0 | base | 2 | 1.465 [1.42-1.51] | 4.595 [4.09-5.1] | 1 [1-1] | 0 [0-0] | 1 [1-1] | 262 [262-262] | 1 [1-1] | 0.06 [0.06-0.06] | -0.01 [-0.01--0.01] | 1.39 [1.38-1.4] | 0.542 [0.465-0.619] | 3.947 [3.718-4.176] | - | - | - | - |
| big-grow | 0 | hybrid | 2 | 1.47 [1.43-1.51] | 5.77 [5.05-6.49] | 1 [1-1] | 0 [0-0] | 1 [1-1] | 289 [289-289] | 1 [1-1] | 0.065 [0.06-0.07] | 0 [0-0] | 1.385 [1.37-1.4] | 0.7965 [0.528-1.065] | 4.554 [2.409-6.699] | - | - | - | - |
| big-grow | 20000 | base | 2 | 1.515 [1.45-1.58] | 5.305 [4.41-6.2] | 12 [12-12] | 1 [1-1] | 12 [12-12] | 272.5 [270-275] | 4 [4-4] | 0.485 [0.47-0.5] | -0.005 [-0.01-0] | 1.435 [1.39-1.48] | 0.3895 [0.356-0.423] | 3.1115 [0.594-5.629] | 19749.5 [19704-19795] | 0.451 [0.447-0.455] | 5.455 [4.639-6.271] | 0 [0-0] |
| big-grow | 20000 | hybrid | 2 | 1.605 [1.53-1.68] | 3.975 [3.66-4.29] | 10.5 [9-12] | 0.5 [0-1] | 13 [12-14] | 300 [298-302] | 4.5 [4-5] | 0.5 [0.49-0.51] | 0 [0-0] | 1.48 [1.41-1.55] | 0.411 [0.36-0.462] | 0.6875 [0.383-0.992] | 19710 [19697-19723] | 0.415 [0.367-0.463] | 4.295 [3.983-4.607] | 0 [0-0] |
| big-grow | 50000 | base | 2 | 1.735 [1.72-1.75] | 3.52 [3.18-3.86] | 15 [15-15] | 1 [1-1] | 15 [15-15] | 280.5 [278-283] | 6 [6-6] | 0.765 [0.75-0.78] | 0 [-0.01-0.01] | 1.675 [1.65-1.7] | 0.3755 [0.37-0.381] | 0.5195 [0.447-0.592] | 49148 [49096-49200] | 0.235 [0.231-0.239] | 3.359 [3.247-3.471] | 0 [0-0] |
| big-grow | 50000 | hybrid | 2 | 1.88 [1.84-1.92] | 4.855 [4.37-5.34] | 15.5 [15-16] | 1 [1-1] | 15 [15-15] | 308 [305-311] | 6 [6-6] | 0.855 [0.79-0.92] | -0.01 [-0.01--0.01] | 1.77 [1.75-1.79] | 0.346 [0.334-0.358] | 3.778 [3.054-4.502] | 49228 [49094-49362] | 0.199 [0.175-0.223] | 4.439 [2.991-5.887] | 0 [0-0] |
| mixed | 0 | base | 2 | 1.635 [1.62-1.65] | 4.34 [3.02-5.66] | 2 [2-2] | 0 [0-0] | 5 [2-8] | 263 [260-266] | 5 [2-8] | 0.06 [0.06-0.06] | 0.02 [0.02-0.02] | 1.515 [1.48-1.55] | 1.0015 [0.986-1.017] | 2.03 [1.811-2.249] | - | - | - | - |
| mixed | 0 | hybrid | 2 | 1.01 [0.99-1.03] | 3.27 [3.26-3.28] | 2 [2-2] | 1 [1-1] | 1 [1-1] | 279 [276-282] | 1 [1-1] | 0.06 [0.06-0.06] | 0.915 [0.91-0.92] | 0.795 [0.78-0.81] | 0.945 [0.888-1.002] | 3.8025 [3.678-3.927] | - | - | - | - |
| mixed | 20000 | base | 2 | 1.69 [1.68-1.7] | 4.695 [4-5.39] | 99.5 [98-101] | 0 [0-0] | 99.5 [98-101] | 350.5 [349-352] | 102 [101-103] | 0.565 [0.56-0.57] | 0.005 [0-0.01] | 1.58 [1.56-1.6] | 0.326 [0.295-0.357] | 3.093 [1.531-4.655] | 19709.5 [19709-19710] | 0.267 [0.263-0.271] | 4.607 [3.391-5.823] | 0 [0-0] |
| mixed | 20000 | hybrid | 2 | 1.055 [1.03-1.08] | 2.57 [2.47-2.67] | 35.5 [35-36] | 0.5 [0-1] | 35 [35-35] | 303 [303-303] | 37 [36-38] | 0.475 [0.46-0.49] | 0.965 [0.94-0.99] | 0.875 [0.87-0.88] | 0.4455 [0.371-0.52] | 7.585 [7.29-7.88] | 19709 [19709-19709] | 0.295 [0.295-0.295] | 5.951 [3.903-7.999] | 0 [0-0] |
| mixed | 50000 | base | 2 | 1.715 [1.69-1.74] | 3.9 [3.16-4.64] | 141.5 [141-142] | 0 [0-0] | 141.5 [141-142] | 393 [392-394] | 142 [142-142] | 0.91 [0.87-0.95] | 0 [-0.01-0.01] | 1.62 [1.6-1.64] | 0.313 [0.272-0.354] | 2.971 [2.721-3.221] | 49301.5 [49299-49304] | 0.255 [0.175-0.335] | 4.247 [3.407-5.087] | 0 [0-0] |
| mixed | 50000 | hybrid | 2 | 1.075 [1.06-1.09] | 2.545 [2.18-2.91] | 57 [56-58] | 2 [2-2] | 55 [54-56] | 325.5 [324-327] | 56.5 [56-57] | 0.715 [0.69-0.74] | 0.985 [0.96-1.01] | 0.85 [0.83-0.87] | 0.2935 [0.288-0.299] | 12.355 [11.164-13.546] | 49298.5 [49292-49305] | 0.275 [0.223-0.327] | 15.455 [12.991-17.919] | 0 [0-0] |
| small | 0 | base | 2 | 1.655 [1.63-1.68] | 6.025 [5.91-6.14] | 2 [2-2] | 0 [0-0] | 4 [2-6] | 307 [301-313] | 4 [2-6] | 0.055 [0.05-0.06] | 0 [0-0] | 1.54 [1.52-1.56] | 0.7235 [0.641-0.806] | 4.503 [3.575-5.431] | - | - | - | - |
| small | 0 | hybrid | 2 | 1.475 [1.44-1.51] | 0.785 [0.78-0.79] | 1.5 [1-2] | 0.5 [0-1] | 1 [1-1] | 311 [309-313] | 1 [1-1] | 0.055 [0.04-0.07] | 1.38 [1.36-1.4] | 0.23 [0.23-0.23] | 0.788 [0.559-1.017] | 1.289 [1.166-1.412] | - | - | - | - |
| small | 20000 | base | 2 | 1.61 [1.52-1.7] | 5.46 [4.78-6.14] | 128 [125-131] | 0 [0-0] | 128 [125-131] | 427 [424-430] | 132 [129-135] | 0.55 [0.5-0.6] | 0 [-0.01-0.01] | 1.53 [1.47-1.59] | 0.382 [0.282-0.482] | 1.4765 [1.283-1.67] | 19747 [19745-19749] | 0.351 [0.351-0.351] | 4.679 [2.735-6.623] | 0 [0-0] |
| small | 20000 | hybrid | 2 | 1.48 [1.45-1.51] | 0.58 [0.49-0.67] | 9 [7-11] | 4.5 [1-8] | 4.5 [3-6] | 312.5 [311-314] | 6 [6-6] | 0.5 [0.48-0.52] | 1.41 [1.41-1.41] | 0.23 [0.22-0.24] | 0.407 [0.384-0.43] | 13.683 [11.695-15.671] | 19746.5 [19746-19747] | 0.379 [0.375-0.383] | 4.535 [4.015-5.055] | 0 [0-0] |
| small | 50000 | base | 2 | 1.625 [1.59-1.66] | 5.12 [5.03-5.21] | 209.5 [207-212] | 0 [0-0] | 209.5 [207-212] | 508.5 [506-511] | 209.5 [207-212] | 0.865 [0.83-0.9] | 0 [0-0] | 1.545 [1.5-1.59] | 0.2915 [0.29-0.293] | 2.9265 [2.558-3.295] | 49393 [49393-49393] | 0.243 [0.231-0.255] | 4.535 [3.631-5.439] | 0 [0-0] |
| small | 50000 | hybrid | 2 | 1.515 [1.5-1.53] | 0.62 [0.5-0.74] | 13.5 [7-20] | 9 [1-17] | 4.5 [3-6] | 324.5 [323-326] | 6 [6-6] | 0.805 [0.79-0.82] | 1.43 [1.41-1.45] | 0.235 [0.23-0.24] | 0.3305 [0.329-0.332] | 31.1455 [30.982-31.309] | 49317 [49314-49320] | 0.243 [0.231-0.255] | 30.143 [29.951-30.335] | 0 [0-0] |

### (1) Time to BGSAVE

| dataset | stock idle | hybrid idle | stock @50k writes/s | hybrid @50k |
|---|---|---|---|---|
| small | 1.66 s | 1.48 s (-11%) | 1.63 s | 1.52 s |
| mixed | 1.64 s | 1.01 s (-38%) | 1.72 s | 1.08 s |
| big | 1.46 s | 1.44 s | 1.57 s | 1.47 s |
| big-grow | 1.47 s | 1.47 s | 1.74 s | 1.88 s (+8%) |

The hybrid save is never slower than stock except big-grow at 50k inserts/s (+8%, within the
spread of that cell). On mixed it is 38% faster because serialization is split between the scan
thread (small keys, about 0.95 s CPU) and the child (big hashes, about 0.85 s CPU) and the two run
concurrently; stock does all 1.6 s in one child. On small the scan thread does all the work
(1.4 s CPU) and the child only writes (0.23 s); the gain is the missing page-table copy and the
child's own startup cost. On big everything goes through the child, as in stock.

Fork stall (`latest_fork_usec`): small 6.0 ms -> 0.6-0.8 ms (the child inherits no keyspace
tables and no small values), mixed 4.3 -> 2.6-3.3 ms, big unchanged (4-6 ms: the page tables of
the big values dominate).

### (2) Maximum memory consumed during BGSAVE

`extra mem peak` = parent RSS growth + child private (copy-on-write) pages, sampled every 100 ms.

| dataset, write rate | stock | hybrid | ratio |
|---|---|---|---|
| small, idle | 2 MB | 1.5 MB | - |
| small, 20k/s | 128 MB | 9 MB | 14x |
| small, 50k/s | 210 MB | 13.5 MB | 15x |
| mixed, 20k/s | 100 MB | 36 MB | 2.8x |
| mixed, 50k/s | 142 MB | 57 MB | 2.5x |
| big, any rate | 1-2 MB | 1-2 MB | 1x |
| big-grow, 20k/s | 12 MB | 10.5 MB | 1x |
| big-grow, 50k/s | 15 MB | 15.5 MB | 1x |

Absolute peak (server RSS + child private pages) on small at 50k writes/s: 509 MB stock vs
325 MB hybrid on a 299 MB dataset. The hybrid's remaining extra memory is split between the parent
(overlay clones of the keys written during the save, 4-9 MB, bounded by
`bgsave-hybrid-max-retained-bytes`) and the child (4-6 MB of its own pages). On mixed the residual
35-55 MB is the hashtable hashes, which stay SNAP_FORK and CoW in the child exactly as in stock:
the design removes small-value, header and table CoW, not big-value CoW.

At rest the hybrid server's RSS is a few MB higher (arena metadata: about 5 MB in a fresh
process) and, after a workload shift from a small-key dataset to a big-key dataset in the same
process, 17-27 MB higher (memory freed in the arena is not reusable for values of the other class
and its dirty pages decay slowly). Two contributors were fixed during this benchmark (see below).

### (3) Traffic vs idle

Stock's extra memory grows with the write rate (small: 2 -> 128 -> 210 MB at 0 / 20k / 50k
writes/s); hybrid stays nearly flat (1.5 -> 9 -> 13.5 MB). Save time does not change with traffic
for either. Throughput: both sustain the offered 20k and 50k ops/s with no stalled seconds.

Latency: stock's worst PING during a save is the fork (5-6 ms). Hybrid removes that on small
datasets (0.6 ms) but adds one main-thread event, the overlay merge when a slot's scan completes:
`hybrid-merge` 12-16 ms (small, 20k/s), 31-35 ms (small, 50k/s), 8 ms and 12-18 ms (mixed). It is
proportional to the number of keys written during the save (standalone has one slot per db, so
one merge of the whole write set) and does not occur on big datasets. p99 latencies are equal.

### (4) Small vs mixed vs big items

| | small (all SNAP_NOFORK) | mixed (45% bytes NOFORK) | big (all SNAP_FORK) |
|---|---|---|---|
| save time | -11% | -38% | equal |
| extra memory under writes | 14-15x lower | 2.5-2.8x lower | equal |
| fork stall | 8-10x lower | 1.3-1.7x lower | equal |
| worst PING | 13-31 ms merge vs 5 ms fork | 8-18 ms merge vs 5 ms fork | equal (fork 5 ms) |
| CPU | moved from child to scan thread, total equal | split thread/child, total equal | equal |

The benefit scales with the share of writes (not bytes) that hit SNAP_NOFORK keys; a big-item-only
dataset behaves exactly like stock.

## Anomalies found while benchmarking, and what was done

1. The hybrid binary was first built with `-DDEBUG_ASSERTIONS -DREDIS_TEST` (the test build): that
   made HSET inserts into 100k-field hashes 3x more expensive on the main thread (7.6 s save under
   50k inserts/s vs 1.9 s). Rebuilt with plain flags; every number above is from plain builds.
2. Save times had 6-12 s outliers on both binaries when the dump went to disk (writeback of the
   previous 250-300 MB dump). Dump directory moved to tmpfs.
3. The hybrid child did not `dismissObject` values after writing them, so parent inserts into big
   hashes left it 27 MB of private pages vs 12 MB for stock. Fixed in `snapSaveChildWrite`; now equal.
4. After FLUSHALL the hybrid server kept 30 MB more resident than stock: about 1 MB of regions in the
   main thread's explicit arena tcache pinned 30 MB of slabs that `arena.purge` cannot release.
   `jemalloc_purge()` now flushes that tcache first; resident memory after FLUSHALL matches stock.

## Caveats

Laptop-class host (governor powersave, turbo on), 2 reps, datasets of 250-300 MB so saves last
1-2 s; the memory ratios are robust across reps, the save-time differences below 10% are not.
Traffic is same-size in-place writes (or inserts for big-grow) with uniform key choice; skewed
workloads concentrate CoW on fewer pages and favour stock less than this. Production-shaped data
(as in the sibling branch's t083 measurements) is the next step.

## Addendum: per-key scan generation (kvBits.save_gen), 2026-09-21

With the 3-bit generation mark, a write to a key the scan thread has already
serialized is applied in place instead of being cloned into the overlay. Hybrid
cells rerun on tmpfs with the plain build (`/tmp/snap-bench6.csv`), medians of 2;
the "before" column is the clone-everything hybrid from the main table.

| dataset, rate | save s | extra mem MB | parent growth MB | retained peak MB | worst PING ms (merge) |
|---|---|---|---|---|---|
| small, 20k/s | 1.48 -> 1.58 | 9.0 -> 6.0 | 4.5 -> 2.0 | 7.6 -> 4.0 | 13.7 -> 6.7 |
| small, 50k/s | 1.52 -> 1.52 | 13.5 -> 9.0 | 9.0 -> 4.5 | 18.5 -> 10.0 | 31.1 -> 19.0 |
| mixed, 20k/s | 1.06 -> 1.08 | 35.5 -> 38.0 | 0.5 -> 1.5 | 0.9 -> 0.5 | 7.6 -> 3.1 |
| mixed, 50k/s | 1.08 -> 1.10 | 57.0 -> 58.5 | 2.0 -> 2.0 | 2.0 -> 1.2 | 12.4 -> 12.1 |

As expected for uniform writes during a linear scan, about half of the writes
now bypass the overlay: retained bytes, parent growth and the merge spike all
drop by roughly half on the small dataset. The mixed dataset's extra memory is
dominated by the SNAP_FORK hashes' CoW in the child, which the marks do not
touch. The remaining merge spike is proportional to the keys written before the
scan reached them; bounding it needs an incremental merge.
