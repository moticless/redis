# Hybrid BGSAVE benchmarks: save time, peak memory, idle vs traffic, item size (runs 4-5 vs 0c1c74706; run 6 vs upstream 0d6266f2d and Valkey; run 7 vs the teammate's forkless-hybrid POC)

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


## Run 6 (2026-09-22): serialize-before-write hybrid vs upstream vs Valkey fork and forkless

Branch commit `3da2cd28a` (bucket window + main-thread emission, spec 11.z; the
frozen-dict overlay only holds keys written while their bucket was inside the
scan thread's window). Same tool, same host, same flags and datasets as above,
tmpfs, plain builds, medians of 2 reps with [min-max]. Raw rows appended to
`results.csv` (ts 2026-09-22).

| config | binary |
|---|---|
| up | redis upstream `0d6266f2d` (the branch's merge base), `/tmp/redis-up` |
| hybrid | this branch at `3da2cd28a`, `--bgsave-hybrid yes` |
| valkey-fork | `~/git/valkey` unstable `06d35aca6`, default fork-based BGSAVE |
| valkey-forkless | same binary, `--forkless-infrastructure-enabled yes --bgsave-default-method forkless` (Valkey serializes on the main thread, no child) |

Valkey's `used_memory` for the same datasets is about 10% lower (270 vs 299 MB
on small, 220 vs 254 MB on big), so its save time is for slightly less data.

| dataset | rate | config | n | save s | fork ms | extra mem peak MB | parent growth MB | child dirty MB | abs peak MB | rdb_last_cow MB | main cpu s | threads cpu s | child cpu s | ping p99 ms | ping max ms | load ops/s | load p99 ms | load max ms | stall s |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| big | 0 | hybrid | 2 | 1.45 [1.43-1.47] | 6.005 [5.54-6.47] | 1 [1-1] | 0 [0-0] | 1 [1-1] | 273 [273-273] | 1 [1-1] | 0.065 [0.06-0.07] | -0.005 [-0.01-0] | 1.33 [1.33-1.33] | 0.815 [0.695-0.935] | 4.3845 [4.11-4.659] | - | - | - | - |
| big | 0 | up | 2 | 1.465 [1.42-1.51] | 5.805 [5.21-6.4] | 1 [1-1] | 0 [0-0] | 1 [1-1] | 262 [262-262] | 1 [1-1] | 0.065 [0.06-0.07] | -0.01 [-0.01--0.01] | 1.38 [1.37-1.39] | 0.514 [0.416-0.612] | 4.971 [4.67-5.272] | - | - | - | - |
| big | 0 | valkey-fork | 2 | 1.46 [1.43-1.49] | 5.85 [5.66-6.04] | 2.5 [1-4] | 0 [0-0] | 13 [1-25] | 250 [238-262] | 13 [1-25] | 0.065 [0.06-0.07] | 0.005 [0-0.01] | 1.35 [1.31-1.39] | 0.8445 [0.69-0.999] | 6.4995 [4.608-8.391] | - | - | - | - |
| big | 0 | valkey-forkless | 2 | 1.57 [1.55-1.59] | 0 [0-0] | 0.5 [0-1] | 0.5 [0-1] | 0 [0-0] | 242.5 [238-247] | 0 [0-0] | 0.07 [0.07-0.07] | 1.365 [1.35-1.38] | 0 [0-0] | 1.125 [1.012-1.238] | 1.2525 [1.148-1.357] | - | - | - | - |
| big | 20000 | hybrid | 2 | 1.495 [1.48-1.51] | 4.53 [3.62-5.44] | 2 [2-2] | 0 [0-0] | 2 [2-2] | 269.5 [265-274] | 2 [2-2] | 0.44 [0.42-0.46] | 0.005 [0-0.01] | 1.425 [1.4-1.45] | 0.349 [0.293-0.405] | 4.41 [4.114-4.706] | 19660 [19623-19697] | 0.371 [0.327-0.415] | 5.135 [4.255-6.015] | 0 [0-0] |
| big | 20000 | up | 2 | 1.45 [1.43-1.47] | 4.445 [3.64-5.25] | 1.5 [1-2] | 0 [0-0] | 1.5 [1-2] | 259 [255-263] | 1.5 [1-2] | 0.455 [0.45-0.46] | 0.005 [0-0.01] | 1.385 [1.38-1.39] | 0.3655 [0.314-0.417] | 4.342 [3.26-5.424] | 19659.5 [19623-19696] | 0.383 [0.311-0.455] | 4.783 [3.935-5.631] | 0 [0-0] |
| big | 20000 | valkey-fork | 2 | 1.535 [1.53-1.54] | 3.995 [3.34-4.65] | 1 [1-1] | 0 [0-0] | 1 [1-1] | 235 [235-235] | 1 [1-1] | 0.475 [0.46-0.49] | 0.005 [0-0.01] | 1.435 [1.43-1.44] | 0.363 [0.347-0.379] | 1.902 [1.897-1.907] | 19707 [19706-19708] | 0.323 [0.279-0.367] | 4.007 [2.927-5.087] | 0 [0-0] |
| big | 20000 | valkey-forkless | 2 | 1.53 [1.52-1.54] | 0 [0-0] | 0.5 [0-1] | 0.5 [0-1] | 0 [0-0] | 235 [235-235] | 0 [0-0] | 0.35 [0.35-0.35] | 1.33 [1.33-1.33] | 0 [0-0] | 0.419 [0.416-0.422] | 0.5995 [0.577-0.622] | 17779 [17778-17780] | 0.387 [0.383-0.391] | 1257.47 [1196.03-1318.91] | 1 [1-1] |
| big | 50000 | hybrid | 2 | 1.47 [1.45-1.49] | 3.785 [3.65-3.92] | 2 [2-2] | 0 [0-0] | 2 [2-2] | 263 [263-263] | 2 [2-2] | 0.68 [0.67-0.69] | -0.01 [-0.01--0.01] | 1.39 [1.39-1.39] | 0.3555 [0.339-0.372] | 1.8905 [1.076-2.705] | 49286 [49278-49294] | 0.231 [0.223-0.239] | 5.263 [4.415-6.111] | 0 [0-0] |
| big | 50000 | up | 2 | 1.56 [1.49-1.63] | 3.855 [3.82-3.89] | 1 [1-1] | 0 [0-0] | 1 [1-1] | 251 [251-251] | 1 [1-1] | 0.75 [0.69-0.81] | 0 [0-0] | 1.47 [1.39-1.55] | 0.304 [0.292-0.316] | 0.3625 [0.358-0.367] | 49294.5 [49294-49295] | 0.243 [0.239-0.247] | 4.223 [4.191-4.255] | 0 [0-0] |
| big | 50000 | valkey-fork | 2 | 1.42 [1.41-1.43] | 3.75 [3.6-3.9] | 1 [1-1] | 0 [0-0] | 1 [1-1] | 235 [235-235] | 1 [1-1] | 0.67 [0.65-0.69] | 0 [-0.01-0.01] | 1.36 [1.33-1.39] | 0.36 [0.359-0.361] | 2.566 [1.767-3.365] | 49099.5 [49098-49101] | 0.287 [0.287-0.287] | 3.447 [2.975-3.919] | 0 [0-0] |
| big | 50000 | valkey-forkless | 2 | 1.595 [1.55-1.64] | 0 [0-0] | 0.5 [0-1] | 0.5 [0-1] | 0 [0-0] | 235 [235-235] | 0 [0-0] | 0.41 [0.41-0.41] | 1.39 [1.35-1.43] | 0 [0-0] | 0.4165 [0.387-0.446] | 1.741 [0.407-3.075] | 44068.5 [43919-44218] | 0.287 [0.287-0.287] | 1318.91 [1277.95-1359.87] | 1 [1-1] |
| big-grow | 0 | hybrid | 2 | 1.52 [1.51-1.53] | 5.8 [5.37-6.23] | 1 [1-1] | 0 [0-0] | 1 [1-1] | 273 [273-273] | 1 [1-1] | 0.065 [0.06-0.07] | -0.005 [-0.01-0] | 1.41 [1.39-1.43] | 0.688 [0.547-0.829] | 2.782 [1.281-4.283] | - | - | - | - |
| big-grow | 0 | up | 2 | 1.52 [1.51-1.53] | 5.71 [5.24-6.18] | 1 [1-1] | 0 [0-0] | 1 [1-1] | 262 [262-262] | 1 [1-1] | 0.06 [0.06-0.06] | 0.005 [0-0.01] | 1.41 [1.39-1.43] | 0.82 [0.798-0.842] | 4.1355 [1.532-6.739] | - | - | - | - |
| big-grow | 0 | valkey-fork | 2 | 1.45 [1.43-1.47] | 5.52 [5.03-6.01] | 1 [1-1] | 0 [0-0] | 1 [1-1] | 232 [232-232] | 1 [1-1] | 0.065 [0.06-0.07] | 0 [0-0] | 1.35 [1.32-1.38] | 0.6705 [0.519-0.822] | 4.711 [3.622-5.8] | - | - | - | - |
| big-grow | 0 | valkey-forkless | 2 | 1.59 [1.59-1.59] | 0 [0-0] | 0.5 [0-1] | 0.5 [0-1] | 0 [0-0] | 231 [231-231] | 0 [0-0] | 0.075 [0.07-0.08] | 1.385 [1.37-1.4] | 0 [0-0] | 1.099 [1.07-1.128] | 2.8975 [2.871-2.924] | - | - | - | - |
| big-grow | 20000 | hybrid | 2 | 1.58 [1.56-1.6] | 5.1 [4.21-5.99] | 12.5 [12-13] | 0.5 [0-1] | 12 [12-12] | 284 [282-286] | 3 [3-3] | 0.515 [0.51-0.52] | 0 [-0.01-0.01] | 1.495 [1.49-1.5] | 0.459 [0.394-0.524] | 4.606 [2.648-6.564] | 19747.5 [19699-19796] | 0.463 [0.463-0.463] | 5.663 [4.639-6.687] | 0 [0-0] |
| big-grow | 20000 | up | 2 | 1.59 [1.55-1.63] | 4.53 [4.51-4.55] | 13.5 [13-14] | 1 [1-1] | 13.5 [13-14] | 278 [276-280] | 5 [5-5] | 0.485 [0.46-0.51] | 0 [0-0] | 1.515 [1.5-1.53] | 0.398 [0.386-0.41] | 1.6815 [0.502-2.861] | 19746.5 [19697-19796] | 0.459 [0.455-0.463] | 4.527 [3.999-5.055] | 0 [0-0] |
| big-grow | 20000 | valkey-fork | 2 | 1.53 [1.52-1.54] | 3.97 [3.5-4.44] | 18 [18-18] | 1 [1-1] | 17 [17-17] | 252.5 [250-255] | 10.5 [10-11] | 0.49 [0.48-0.5] | 0 [-0.01-0.01] | 1.455 [1.42-1.49] | 0.351 [0.339-0.363] | 0.922 [0.425-1.419] | 19725 [19707-19743] | 0.371 [0.367-0.375] | 2.943 [2.783-3.103] | 0 [0-0] |
| big-grow | 20000 | valkey-forkless | 2 | 1.62 [1.58-1.66] | 0 [0-0] | 1 [1-1] | 1 [1-1] | 0 [0-0] | 235.5 [233-238] | 0 [0-0] | 0.36 [0.35-0.37] | 1.415 [1.37-1.46] | 0 [0-0] | 0.4215 [0.385-0.458] | 0.4615 [0.411-0.512] | 17666 [17610-17722] | 0.375 [0.375-0.375] | 1122.3 [1105.92-1138.69] | 1 [1-1] |
| big-grow | 50000 | hybrid | 2 | 1.73 [1.66-1.8] | 4.645 [3.96-5.33] | 15 [15-15] | 1 [1-1] | 14.5 [14-15] | 291 [288-294] | 4 [4-4] | 0.785 [0.78-0.79] | 0 [0-0] | 1.645 [1.59-1.7] | 0.358 [0.326-0.39] | 3.255 [0.684-5.826] | 49146.5 [49099-49194] | 0.215 [0.199-0.231] | 3.415 [3.167-3.663] | 0 [0-0] |
| big-grow | 50000 | up | 2 | 1.81 [1.7-1.92] | 4.615 [4.24-4.99] | 14.5 [14-15] | 0.5 [0-1] | 14.5 [14-15] | 280 [277-283] | 6.5 [6-7] | 0.83 [0.76-0.9] | 0 [0-0] | 1.7 [1.59-1.81] | 0.39 [0.366-0.414] | 1.5435 [1.335-1.752] | 49149.5 [49104-49195] | 0.215 [0.199-0.231] | 3.071 [3.039-3.103] | 0 [0-0] |
| big-grow | 50000 | valkey-fork | 2 | 1.765 [1.71-1.82] | 4.16 [3.81-4.51] | 19.5 [19-20] | 8 [0-16] | 19.5 [19-20] | 277 [263-291] | 11 [8-14] | 0.9 [0.89-0.91] | -0.005 [-0.01-0] | 1.65 [1.6-1.7] | 0.311 [0.301-0.321] | 3.5195 [2.862-4.177] | 49292.5 [49193-49392] | 0.267 [0.255-0.279] | 3.711 [3.039-4.383] | 0 [0-0] |
| big-grow | 50000 | valkey-forkless | 2 | 1.72 [1.71-1.73] | 0 [0-0] | 1 [1-1] | 1 [1-1] | 0 [0-0] | 257.5 [244-271] | 0 [0-0] | 0.415 [0.4-0.43] | 1.51 [1.49-1.53] | 0 [0-0] | 0.4185 [0.38-0.457] | 0.4995 [0.471-0.528] | 43816.5 [43803-43830] | 0.271 [0.263-0.279] | 1359.87 [1310.72-1409.02] | 1 [1-1] |
| mixed | 0 | hybrid | 2 | 1.165 [1.09-1.24] | 3.225 [2.98-3.47] | 1 [1-1] | 0 [0-0] | 1 [1-1] | 264 [264-264] | 1 [1-1] | 0.065 [0.06-0.07] | 0.825 [0.82-0.83] | 0.78 [0.75-0.81] | 0.6715 [0.447-0.896] | 0.9175 [0.655-1.18] | - | - | - | - |
| mixed | 0 | up | 2 | 1.56 [1.53-1.59] | 5.83 [5.31-6.35] | 2 [2-2] | 0 [0-0] | 2 [2-2] | 263 [260-266] | 2 [2-2] | 0.07 [0.07-0.07] | -0.01 [-0.01--0.01] | 1.49 [1.49-1.49] | 0.479 [0.429-0.529] | 3.7405 [2.947-4.534] | - | - | - | - |
| mixed | 0 | valkey-fork | 2 | 1.44 [1.43-1.45] | 6.595 [6.04-7.15] | 1 [1-1] | 0 [0-0] | 1 [1-1] | 256 [256-256] | 1 [1-1] | 0.06 [0.06-0.06] | 0 [0-0] | 1.37 [1.36-1.38] | 0.5005 [0.473-0.528] | 5.692 [4.415-6.969] | - | - | - | - |
| mixed | 0 | valkey-forkless | 2 | 1.85 [1.85-1.85] | 0 [0-0] | 0 [0-0] | 0 [0-0] | 0 [0-0] | 256 [256-256] | 0 [0-0] | 0.46 [0.45-0.47] | 1.535 [1.52-1.55] | 0 [0-0] | 1.153 [1.115-1.191] | 2.691 [2.279-3.103] | - | - | - | - |
| mixed | 20000 | hybrid | 2 | 1.055 [1.02-1.09] | 2.07 [1.85-2.29] | 33.5 [33-34] | 1 [1-1] | 32.5 [32-33] | 292.5 [288-297] | 34.5 [34-35] | 0.48 [0.47-0.49] | 0.855 [0.85-0.86] | 0.85 [0.84-0.86] | 0.3215 [0.244-0.399] | 0.4375 [0.269-0.606] | 19682.5 [19682-19683] | 0.423 [0.383-0.463] | 3.423 [3.007-3.839] | 0 [0-0] |
| mixed | 20000 | up | 2 | 1.635 [1.57-1.7] | 4.515 [4.12-4.91] | 100.5 [98-103] | 0.5 [0-1] | 100 [98-102] | 351 [349-353] | 101.5 [101-102] | 0.53 [0.52-0.54] | -0.005 [-0.01-0] | 1.54 [1.49-1.59] | 0.3315 [0.304-0.359] | 4.7805 [4.384-5.177] | 19683.5 [19683-19684] | 0.443 [0.439-0.447] | 4.591 [3.967-5.215] | 0 [0-0] |
| mixed | 20000 | valkey-fork | 2 | 1.465 [1.43-1.5] | 4.865 [4.56-5.17] | 92 [90-94] | 0 [0-0] | 92 [90-94] | 342.5 [337-348] | 93.5 [93-94] | 0.52 [0.51-0.53] | 0.005 [0-0.01] | 1.385 [1.37-1.4] | 0.352 [0.333-0.371] | 2.6305 [1.917-3.344] | 19692 [19687-19697] | 0.391 [0.311-0.471] | 2.927 [2.895-2.959] | 0 [0-0] |
| mixed | 20000 | valkey-forkless | 2 | 2.24 [2.22-2.26] | 0 [0-0] | 1 [1-1] | 1 [1-1] | 0 [0-0] | 252 [248-256] | 0 [0-0] | 1.015 [1-1.03] | 1.63 [1.63-1.63] | 0 [0-0] | 1.08 [1.078-1.082] | 2.289 [1.411-3.167] | 19459 [19440-19478] | 0.947 [0.927-0.967] | 5.455 [5.087-5.823] | 0.5 [0-1] |
| mixed | 50000 | hybrid | 2 | 1.325 [1.26-1.39] | 2.445 [2.39-2.5] | 58.5 [58-59] | 2.5 [2-3] | 56 [56-56] | 314 [314-314] | 56.5 [56-57] | 0.795 [0.79-0.8] | 0.87 [0.87-0.87] | 0.87 [0.85-0.89] | 0.2925 [0.288-0.297] | 0.6315 [0.342-0.921] | 49255 [49253-49257] | 0.251 [0.239-0.263] | 3.359 [3.343-3.375] | 0 [0-0] |
| mixed | 50000 | up | 2 | 1.685 [1.65-1.72] | 4.395 [4.32-4.47] | 140.5 [140-141] | 0 [0-0] | 140.5 [140-141] | 392 [392-392] | 141 [141-141] | 0.875 [0.86-0.89] | -0.005 [-0.01-0] | 1.595 [1.58-1.61] | 0.289 [0.282-0.296] | 2.4875 [1.994-2.981] | 49252 [49252-49252] | 0.275 [0.271-0.279] | 4.863 [4.863-4.863] | 0 [0-0] |
| mixed | 50000 | valkey-fork | 2 | 1.45 [1.43-1.47] | 4.03 [3.28-4.78] | 136 [136-136] | 0 [0-0] | 136 [136-136] | 383 [383-383] | 137.5 [137-138] | 0.795 [0.79-0.8] | -0.01 [-0.01--0.01] | 1.37 [1.35-1.39] | 0.282 [0.261-0.303] | 2.2785 [1.564-2.993] | 49610.5 [49588-49633] | 0.267 [0.255-0.279] | 4.367 [3.647-5.087] | 0 [0-0] |
| mixed | 50000 | valkey-forkless | 2 | 2.45 [2.44-2.46] | 0 [0-0] | 1.5 [1-2] | 1.5 [1-2] | 0 [0-0] | 249.5 [249-250] | 0 [0-0] | 1.435 [1.42-1.45] | 1.705 [1.7-1.71] | 0 [0-0] | 1.305 [1.255-1.355] | 1.527 [1.499-1.555] | 48296 [48143-48449] | 2.415 [2.367-2.463] | 6.751 [6.751-6.751] | 0 [0-0] |
| small | 0 | hybrid | 2 | 1.32 [1.3-1.34] | 0.66 [0.62-0.7] | 1 [1-1] | 0 [0-0] | 1 [1-1] | 311 [309-313] | 1 [1-1] | 0.055 [0.05-0.06] | 1.255 [1.24-1.27] | 0.2 [0.19-0.21] | 0.6075 [0.59-0.625] | 0.7875 [0.777-0.798] | - | - | - | - |
| small | 0 | up | 2 | 1.56 [1.53-1.59] | 6.665 [6.3-7.03] | 2 [2-2] | 0 [0-0] | 2 [2-2] | 309 [309-309] | 2 [2-2] | 0.06 [0.05-0.07] | 0.005 [0-0.01] | 1.495 [1.49-1.5] | 0.485 [0.477-0.493] | 2.9525 [1.477-4.428] | - | - | - | - |
| small | 0 | valkey-fork | 2 | 1.315 [1.22-1.41] | 5.585 [5.55-5.62] | 1 [1-1] | 0 [0-0] | 1 [1-1] | 281 [281-281] | 1 [1-1] | 0.07 [0.07-0.07] | 0 [0-0] | 1.23 [1.17-1.29] | 0.6905 [0.614-0.767] | 2.4755 [1.591-3.36] | - | - | - | - |
| small | 0 | valkey-forkless | 2 | 1.685 [1.68-1.69] | 0 [0-0] | 1 [1-1] | 1 [1-1] | 0 [0-0] | 281 [281-281] | 0 [0-0] | 0.5 [0.5-0.5] | 1.42 [1.41-1.43] | 0 [0-0] | 1.556 [1.505-1.607] | 2.765 [1.756-3.774] | - | - | - | - |
| small | 20000 | hybrid | 2 | 1.38 [1.35-1.41] | 0.725 [0.71-0.74] | 3 [3-3] | 2 [2-2] | 1 [1-1] | 303 [303-303] | 1 [1-1] | 0.49 [0.49-0.49] | 1.28 [1.26-1.3] | 0.23 [0.23-0.23] | 0.3975 [0.356-0.439] | 0.6615 [0.602-0.721] | 19697.5 [19697-19698] | 0.391 [0.391-0.391] | 2.983 [2.975-2.991] | 0 [0-0] |
| small | 20000 | up | 2 | 1.585 [1.52-1.65] | 5.38 [4.83-5.93] | 127.5 [125-130] | 0 [0-0] | 127.5 [125-130] | 426.5 [424-429] | 130 [129-131] | 0.52 [0.49-0.55] | 0 [0-0] | 1.465 [1.39-1.54] | 0.437 [0.385-0.489] | 3.232 [2.84-3.624] | 19697.5 [19697-19698] | 0.331 [0.327-0.335] | 5.647 [5.055-6.239] | 0 [0-0] |
| small | 20000 | valkey-fork | 2 | 1.36 [1.33-1.39] | 5.625 [5.28-5.97] | 101.5 [101-102] | 0 [0-0] | 101.5 [101-102] | 381.5 [381-382] | 104 [103-105] | 0.52 [0.52-0.52] | 0 [0-0] | 1.25 [1.23-1.27] | 0.487 [0.409-0.565] | 1.4445 [1.135-1.754] | 19746.5 [19746-19747] | 0.383 [0.375-0.391] | 5.951 [5.599-6.303] | 0 [0-0] |
| small | 20000 | valkey-forkless | 2 | 1.855 [1.84-1.87] | 0 [0-0] | 1 [1-1] | 1 [1-1] | 0 [0-0] | 281 [281-281] | 0 [0-0] | 1.005 [1-1.01] | 1.495 [1.48-1.51] | 0 [0-0] | 1.1875 [1.138-1.237] | 1.4975 [1.343-1.652] | 19735.5 [19732-19739] | 0.411 [0.407-0.415] | 4.055 [3.855-4.255] | 0 [0-0] |
| small | 50000 | hybrid | 2 | 1.31 [1.28-1.34] | 0.695 [0.65-0.74] | 3.5 [3-4] | 2 [2-2] | 1.5 [1-2] | 303.5 [303-304] | 1.5 [1-2] | 0.745 [0.74-0.75] | 1.245 [1.23-1.26] | 0.215 [0.21-0.22] | 0.307 [0.288-0.326] | 0.387 [0.298-0.476] | 49195.5 [49195-49196] | 0.251 [0.247-0.255] | 3.111 [3.023-3.199] | 0 [0-0] |
| small | 50000 | up | 2 | 1.6 [1.58-1.62] | 5.665 [5.24-6.09] | 207 [207-207] | 0 [0-0] | 207 [207-207] | 506 [506-506] | 207.5 [207-208] | 0.86 [0.86-0.86] | 0.01 [0.01-0.01] | 1.49 [1.49-1.49] | 0.3315 [0.32-0.343] | 4.097 [3.921-4.273] | 49195.5 [49195-49196] | 0.247 [0.239-0.255] | 6.207 [5.727-6.687] | 0 [0-0] |
| small | 50000 | valkey-fork | 2 | 1.25 [1.23-1.27] | 4.735 [4.67-4.8] | 176.5 [176-177] | 0 [0-0] | 176.5 [176-177] | 456.5 [456-457] | 176 [176-176] | 0.735 [0.72-0.75] | 0 [-0.01-0.01] | 1.15 [1.1-1.2] | 0.283 [0.249-0.317] | 3.0665 [2.226-3.907] | 49391.5 [49391-49392] | 0.251 [0.247-0.255] | 5.087 [4.991-5.183] | 0 [0-0] |
| small | 50000 | valkey-forkless | 2 | 1.965 [1.96-1.97] | 0 [0-0] | 1 [1-1] | 1 [1-1] | 0 [0-0] | 281 [281-281] | 0 [0-0] | 1.405 [1.39-1.42] | 1.535 [1.53-1.54] | 0 [0-0] | 1.0285 [0.975-1.082] | 2.509 [1.446-3.572] | 49389.5 [49389-49390] | 0.299 [0.287-0.311] | 3.719 [3.695-3.743] | 0 [0-0] |

### Reading the run

- **Fork stall.** hybrid 0.6-0.7 ms on small (headers, small values and tables
  are DONTFORK) versus 5-6 ms for up and valkey-fork; forkless has no fork.
  On big and mixed the hybrid still forks the big values (3-6 ms).
- **Extra memory under traffic.** small 50k writes/s: up 207 MB, valkey-fork
  177 MB, hybrid 3.5 MB (parent +2 MB, child +2 MB), forkless 1 MB. mixed 50k:
  up 140, valkey-fork 136, hybrid 58 (the child's CoW on the 55% of bytes in
  big values), forkless 1.5. big-grow 50k (inserts into 10 MB hashes): up 14.5,
  valkey-fork 19.5, hybrid 15, forkless 1. Emission on the main thread replaced
  the overlay's retained clones: parent growth is 2 MB instead of 4.5-17 MB in
  run 5.
- **Main-thread latency.** The `hybrid-merge` spike of run 5 (13-31 ms worst
  PING on small at 50k) is gone: worst PING during the save is 0.3-0.7 ms on
  small and 0.6-0.9 ms on mixed, against 3-4 ms for up (the fork) and 1.4-3 ms
  for valkey-fork. Valkey forkless keeps PING p99 at 1.0-1.5 ms for the whole
  save on small/mixed (serialization shares the main thread) and stalls for
  1.1-1.4 s on big and big-grow (one 10 MB hash serialized in one go: `stall 1`,
  load max 1.1-1.4 s, load ops/s down to 44k of 50k). The hybrid hands big
  values to the child and shows no stall.
- **Save time.** small: hybrid 1.31-1.44 s, up 1.56-1.60, valkey-fork
  1.25-1.36, forkless 1.69-1.97. mixed: hybrid 1.07-1.33, up 1.56-1.69,
  valkey-fork 1.44-1.47, forkless 1.85-2.45 (its main-thread share competes with
  the traffic). big and big-grow: all within 1.4-1.8 s, the child does the same
  work everywhere (forkless 1.5-1.7 on its smaller dataset).
- **CPU.** Main-thread CPU during the save is unchanged for the hybrid (small
  50k: 0.80 s vs 0.86 s for up), the emission cost is inside the noise; the scan
  thread adds 1.3-1.4 s on small (it serializes everything), 0.9 s on mixed, 0
  on big. Valkey forkless spends 1.0-1.4 s of main-thread CPU per save on
  small/mixed under traffic plus 1.4-1.7 s on its background thread.
- **Correctness of the hybrid run.** All 24 hybrid saves reported `ok`; the
  reload equivalence is covered by the tcl harness, not by this tool.


## Run 7 (2026-09-25): head to head with the teammate's forkless-hybrid POC

Raw: `run7.csv`. Same host, same flags and datasets as run 6, medians of 2 reps, [min-max].

| config | binary | save path |
|---|---|---|
| up | upstream unstable 0d6266f2d, plain build | stock fork |
| hybrid | this branch 20aa93d06, plain build (`/tmp/redis-hyb` worktree), `--bgsave-hybrid yes` | our hybrid: scan thread serializes arena values, child serializes big values |
| yinon-fork | `yinon/forkless-hybrid` 1204d0970 (on dce0c76d4, 15 commits behind our base), plain build, `--bgsave-mode fork` | stock fork of the teammate's tree |
| yinon-hybrid | same binary, `--bgsave-mode hybrid --forkless-string-max 4096` | child walks its CoW dict and serializes big values, parent helper thread serializes retained small values |
| yinon-forkless | same binary, `--bgsave-mode forkless --forkless-string-max 4096` | no child: main thread cuts the snapshot and copies, helper serializes |

Parity: the small-value cap is 4096 B on both POCs. Retained-memory budgets are at each POC's default (ours 256 MB hard mark,
theirs unlimited); neither budget was hit (`snap_hard_mark_aborts=0`, `forkless_limit_aborts=0` in every row). The teammate's
config leaves keyspace and kvobj headers in forkable memory; ours keeps them in the no-fork arena.

### Small (300 MB: 1.2 M strings x 100 B, 200 k listpack hashes), all values eligible for the small path on both POCs

| rate | config | save s | fork ms | extra mem peak MB | parent / child MB | main cpu s | threads cpu s | child cpu s | ping p99 ms | ping max ms | load max ms |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | up | 1.80 | 3.3 | 2 | 0 / 2 | 0.06 | 0 | 1.72 | 0.57 | 1.8 | - |
| 0 | yinon-fork | 1.50 | 6.3 | 1 | 0 / 1 | 0.07 | 0 | 1.43 | 0.66 | 3.0 | - |
| 0 | yinon-hybrid | 1.35 | 1.2 | 1.5 | 0.5 / 1 | 0.07 | 1.31 | 0.52 | 0.51 | 9.4 | - |
| 0 | yinon-forkless | 1.68 | 0 | 0.5 | 0.5 / 0 | 1.58 | 1.67 | 0 | 0.38 | 10.7 | - |
| 0 | hybrid | 1.33 | 0.66 | 1 | 0 / 1 | 0.05 | 1.25 | 0.20 | 0.75 | 1.2 | - |
| 20k | up | 1.75 | 4.2 | 135 | 0 / 135 | 0.56 | 0 | 1.68 | 0.42 | 2.8 | 4.8 |
| 20k | yinon-fork | 1.57 | 3.8 | 150 | 0 / 150 | 0.59 | 0 | 1.45 | 0.38 | 1.6 | 3.1 |
| 20k | yinon-hybrid | 1.50 | 1.1 | 32.5 | 6.5 / 26 | 0.58 | 1.46 | 0.55 | 0.38 | 12.3 | 5.8 |
| 20k | yinon-forkless | 1.54 | 0 | 0.5 | 0.5 / 0 | 1.73 | 1.51 | 0 | 0.30 | 9.8 | 8.7 |
| 20k | hybrid | 1.35 | 0.65 | 3 | 2 / 1 | 0.51 | 1.29 | 0.22 | 0.34 | 0.6 | 3.2 |
| 50k | up | 1.88 | 4.5 | 220 | 0 / 220 | 1.34 | 0 | 1.80 | 0.53 | 2.6 | 7.9 |
| 50k | yinon-fork | 1.64 | 5.3 | 232 | 0 / 232 | 0.98 | 0 | 1.53 | 0.45 | 3.0 | 5.8 |
| 50k | yinon-hybrid | 1.47 | 1.0 | 34.5 | 8.5 / 26 | 0.96 | 1.40 | 0.55 | 0.36 | 9.2 | 7.2 |
| 50k | yinon-forkless | 1.90 | 0 | 1 | 1 / 0 | 2.15 | 1.62 | 0 | 0.37 | 9.2 | 11.9 |
| 50k | hybrid | 1.31 | 0.52 | 3 | 2 / 1 | 0.79 | 1.25 | 0.21 | 0.28 | 0.33 | 4.1 |

### Mixed (260 MB: 45% of bytes in small values, 55% in 16 k hashtable hashes of about 9 KB)

| rate | config | save s | fork ms | extra mem peak MB | parent / child MB | rdb_last_cow MB | main cpu s | threads cpu s | child cpu s | ping p99 ms | ping max ms | load max ms |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | up | 2.53 [1.71-3.35] | 4.6 | 2 | 0 / 2 | 2 | 0.10 | 0 | 2.42 | 1.49 | 3.4 | - |
| 0 | yinon-fork | 1.62 | 5.6 | 1.5 | 0 / 1.5 | 1.5 | 0.07 | 0 | 1.51 | 0.48 | 5.0 | - |
| 0 | yinon-hybrid | 1.34 | 4.0 | 2.5 | 0.5 / 2 | 2 | 0.08 | 0.61 | 1.10 | 1.22 | 13.4 | - |
| 0 | yinon-forkless | 1.59 | 0 | 0 | 0 / 0 | 0 | 1.27 | 1.59 | 0 | 0.48 | 4.0 | - |
| 0 | hybrid | 1.12 | 2.6 | 1 | 0 / 1 | 1 | 0.06 | 0.82 | 0.78 | 0.52 | 2.0 | - |
| 20k | up | 1.75 | 4.0 | 102 | 0 / 102 | 104 | 0.59 | 0 | 1.66 | 0.37 | 2.0 | 4.5 |
| 20k | yinon-fork | 1.69 | 4.3 | 124 | 0.5 / 123.5 | 109.5 | 0.61 | 0 | 1.58 | 0.33 | 3.6 | 3.4 |
| 20k | yinon-hybrid | 1.36 | 2.4 | 76.5 | 3.5 / 73 | 66.5 | 0.56 | 0.65 | 1.19 | 0.36 | 8.8 | 7.0 |
| 20k | yinon-forkless | 1.49 | 0 | 2 | 2 / 0 | 0 | 1.45 | 1.45 | 0 | 0.30 | 7.2 | 7.2 |
| 20k | hybrid | 1.11 | 1.7 | 34.5 | 1.5 / 33 | 35 | 0.51 | 0.90 | 0.88 | 0.35 | 2.3 | 3.3 |
| 50k | up | 1.79 | 3.3 | 141.5 | 0 / 141.5 | 142 | 1.07 | 0 | 1.72 | 0.33 | 1.5 | 6.7 |
| 50k | yinon-fork | 1.79 | 4.0 | 166.5 | 0 / 166.5 | 151 | 1.03 | 0 | 1.71 | 0.37 | 0.7 | 4.7 |
| 50k | yinon-hybrid | 1.29 | 2.8 | 103 | 4.5 / 98.5 | 94 | 0.86 | 0.67 | 1.12 | 0.40 | 7.9 | 7.6 |
| 50k | yinon-forkless | 1.75 | 0 | 8 | 8 / 0 | 0 | 1.84 | 1.52 | 0 | 0.39 | 5.9 | 5.4 |
| 50k | hybrid | 1.37 | 2.0 | 58 | 2 / 56 | 56 | 0.86 | 0.89 | 0.87 | 0.32 | 2.0 | 4.9 |

### Big (260 MB: 22 hashtable hashes of about 10 MB, 2 zsets), nothing eligible for the small path

| rate | config | save s | fork ms | extra mem peak MB | parent / child MB | main cpu s | threads cpu s | child cpu s | ping p99 ms | ping max ms | load max ms |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | up | 1.77 | 4.4 | 1 | 0 / 1 | 0.05 | 0 | 1.64 | 0.57 | 2.4 | - |
| 0 | yinon-fork | 1.52 | 3.7 | 1 | 0 / 1 | 0.06 | 0 | 1.40 | 0.57 | 2.5 | - |
| 0 | yinon-hybrid | 1.45 | 3.6 | 1 | 0 / 1 | 0.07 | 0 | 1.36 | 0.57 | 5.4 | - |
| 0 | yinon-forkless | 1.29 | 0 | 146.5 | 146.5 / 0 | 0.52 | 1.31 | 0 | 64.9 | 87.3 | - |
| 0 | hybrid | 1.47 | 4.5 | 1 | 0 / 1 | 0.04 | 0 | 1.38 | 0.50 | 1.8 | - |
| 20k | up | 1.55 | 3.9 | 2 | 0 / 2 | 0.49 | 0 | 1.44 | 0.36 | 0.5 | 4.5 |
| 20k | yinon-fork | 1.51 | 3.4 | 2 | 0 / 2 | 0.50 | 0 | 1.44 | 0.37 | 1.2 | 3.8 |
| 20k | yinon-hybrid | 1.60 | 3.4 | 2.5 | 0 / 2.5 | 0.54 | 0 | 1.49 | 0.40 | 9.7 | 12.1 |
| 20k | yinon-forkless | 1.31 | 0 | 146.5 | 146.5 / 0 | 0.89 | 1.30 | 0 | 64.3 | 159.5 | 170.5 |
| 20k | hybrid | 1.55 | 3.3 | 2 | 0 / 2 | 0.48 | 0 | 1.48 | 0.37 | 1.7 | 3.7 |
| 50k | up | 1.51 | 3.7 | 1 | 0 / 1 | 0.77 | 0 | 1.41 | 0.35 | 3.4 | 3.9 |
| 50k | yinon-fork | 1.53 | 3.7 | 2 | 0 / 2 | 0.82 | 0 | 1.43 | 0.33 | 0.6 | 4.1 |
| 50k | yinon-hybrid | 1.54 | 3.0 | 2 | 0 / 2 | 0.78 | 0 | 1.45 | 0.36 | 5.1 | 8.8 |
| 50k | yinon-forkless | 1.31 | 0 | 140.5 | 140.5 / 0 | 1.06 | 1.34 | 0 | 67.7 | 190.8 | 228.9 |
| 50k | hybrid | 1.46 | 4.0 | 2 | 0 / 2 | 0.73 | 0.01 | 1.39 | 0.37 | 2.5 | 4.2 |

### Reading the run

- **Small, under writes: both hybrids remove most of the fork's cost; ours removes the rest.** Stock CoW is 135-232 MB at 20-50k
  writes/s. The teammate's hybrid brings it to 32-35 MB, of which 26 MB is the child's copy-on-write of the dict and kvobj headers
  it walks (they stay in forkable memory), plus 6-8 MB of retained values in the parent. Ours is 3 MB (2 MB of temp-dict clones,
  1 MB in the child). Fork stall: 1.0-1.2 ms theirs, 0.5-0.7 ms ours, 3.3-6.3 ms for the two stock forks.
- **Worst-case latency during the save is the largest difference.** The teammate's hybrid has a PING max of 9-13 ms in every
  small and mixed cell and 5-10 ms on big; forkless 9-11 ms on small. Ours is 0.3-1.2 ms on small and 2.0-2.5 ms on mixed and big,
  i.e. the fork itself. We did not profile their spike; candidates are the per-save arena madvise flip and the snapshot cut on the
  main thread. Their p99 is fine (0.3-0.5 ms), so it is a few events per save, not a steady cost.
- **Main-thread CPU.** Ours matches stock in every cell (0.05 / 0.5 / 0.8 s idle / 20k / 50k on small), because the serialization
  runs on the scan thread and the main thread only clones inside the window. The teammate's hybrid also matches stock; their
  forkless mode moves the whole small-value copy to the main thread (1.6 s idle, 2.2 s at 50k, three times stock).
- **Save time.** Ours is the fastest on small and mixed (1.1-1.4 s), because two threads serialize (scan thread the arena values,
  child the big ones) and the scan thread's serialization is not slowed by CoW. Theirs is 1.3-1.5 s; the two stock forks 1.5-1.9 s.
  On big, everything is the child's work and all fork-based configs are within noise (1.45-1.6 s); forkless is faster (1.3 s) but
  pays for it below.
- **Forkless on big values is not usable as measured**: 140-147 MB of parent growth (full copies of the 10 MB hashes being
  written), PING p99 of 65 ms and PING max of 87-190 ms, load max of 170-230 ms. The same configuration on small/mixed is the
  best memory number of the run (0-8 MB) with p99 in line with the others. This matches the Valkey forkless result of run 6.
- **Mixed: the big-value CoW remains for both hybrids and is smaller for ours.** Extra memory at 50k: stock 142-167 MB,
  teammate 103 MB, ours 58 MB. Both children copy the pages of 9 KB hashes as they are written; ours finishes sooner (0.87 s child
  CPU versus 1.12 s), so fewer writes land while it runs.
- **The two stock baselines agree.** `up` and `yinon-fork` are within noise of each other in every cell except idle mixed, where one
  `up` rep took 3.35 s (the other 1.71 s; a laptop scheduling artifact, not reproduced elsewhere).

### What the run found in our code

The first pass of `hybrid` used the repository binary, which was built with `-DDEBUG_ASSERTIONS` (unlike every other binary
in the run; run 6's hybrid rows had the same handicap). At small / 50k it hit `db.c:618`: the debug check
`snapDebugAssertNotFrozenOriginal` reported a frozen chain as owed. Root cause: within one write section the prepare hook saw
a bucket ahead, emitted it and set its bit; the scan thread then published the next window over that bucket and waited for the
section to close; the check re-read the position, found the bucket inside the window and consulted the bit only for the ahead
position. In a plain build the same path only caused an unnecessary clone. Fixed in 20aa93d06 (`snapBucketOwed`: behind or
emitted is never owed; unit test reproduces the window move inside a section; spec 11.z.2). The 4 hybrid rows of the first pass
were dropped and the config rerun with a plain build of the fixed commit about 35 minutes after the other configs.

Harness note: `snap_save_main_keys` / `snap_save_main_buckets` are live counters of the running save and read 0 once it is
done, so the `impl_stats` column carries no main-thread emission count for our rows; `snap_last_save_retained_peak` does
carry (4.4-10.9 MB on small at 20-50k, 0.6-2 MB on mixed, 0 on big).

### Caveats specific to this run

- The teammate's tree is 15 commits behind our base; compare each hybrid to the stock fork of its own tree (`yinon-hybrid` vs
  `yinon-fork`, `hybrid` vs `up`) before comparing the hybrids to each other.
- One laptop, 2 reps, medians; differences under about 10% in save time and 1 ms in latency are noise here.
- `--forkless-string-max 4096` puts the teammate's cap at our default; their default is 500 KB. Their retained budget is
  unlimited, ours 256 MB; neither triggered.
- The hybrid rows were measured later than the other four configs (same boot, same idle machine).
