# Hybrid BGSAVE benchmarks: save time, peak memory, idle vs traffic, item size (runs 4-5 vs 0c1c74706; run 6 vs upstream 0d6266f2d and Valkey; run 7 vs the teammate's forkless-hybrid POC; run 8 = run 7 on a single core; run 9 = new traffic generator, hot set, vs the teammate's hybrid; run 10 = the same on a c7i.4xlarge with interleaved configurations; run 11 = mixed, our hybrid vs Valkey forkless on the c7i.4xlarge)

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


## Run 8 (2026-09-25): run 7 repeated with the server confined to one CPU

Raw: `run8-cpu1.csv`. Same binaries, flags, datasets and rates as run 7; the only change is `SERVER_CPUS=0` in `run.sh`:
the server, its threads (our scan thread, the teammate's helper) and its fork child all inherit the affinity to CPU 0 (a P-core;
its SMT sibling CPU 1 is idle). memtier and the probes stay on CPUs 8-15. This is the shape of the teammate's `cpu1` profile
(his spec uses a one-CPU cgroup). Medians of 2 reps, [min-max] where it matters.

### Small (300 MB, all small values)

| rate | config | save s | fork ms | extra mem peak MB | parent / child MB | main cpu s | threads cpu s | child cpu s | ping p99 ms | ping max ms | load ops/s | load max ms | stall s |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | up | 1.56 | 5.4 | 2 | 0 / 2 | 0.06 | 0 | 1.47 | 1.33 | 3.6 | - | - | - |
| 0 | yinon-fork | 1.60 | 6.5 | 1 | 0 / 1 | 0.06 | 0 | 1.47 | 1.81 | 6.0 | - | - | - |
| 0 | yinon-hybrid | 1.75 | 0.8 | 2 | 1 / 1 | 0.06 | 1.22 | 0.41 | 0.77 | 10.9 | - | - | - |
| 0 | yinon-forkless | 2.71 [2.43-2.99] | 0 | 1 | 1 / 0 | 1.26 | 1.46 | 0 | 0.83 | 8.4 | - | - | - |
| 0 | hybrid | 1.42 | 0.47 | 1 | 1 / 0 | 0.05 | 1.19 | 0.12 | 1.01 | 2.1 | - | - | - |
| 20k | up | 1.89 | 4.0 | 142.5 | 0 / 142.5 | 0.55 | 0 | 1.51 | 3.28 | 3.6 | 19697 | 5.4 | 0 |
| 20k | yinon-fork | 1.90 | 4.7 | 164 | 0 / 164 | 0.61 | 0 | 1.46 | 4.03 | 5.4 | 19696 | 5.3 | 0 |
| 20k | yinon-hybrid | 2.06 | 1.0 | 32 | 7 / 25 | 0.59 | 1.26 | 0.41 | 2.87 | 9.1 | 19699 | 5.4 | 0 |
| 20k | yinon-forkless | 4.36 [2.78-5.94] | 0 | 1 | 1 / 0 | 2.48 | 2.05 | 0 | 1.32 | 10.9 | 19665 | 12.3 | 0 |
| 20k | hybrid | 1.56 | 0.6 | 2.5 | 1.5 / 1 | 0.48 | 1.15 | 0.12 | 2.83 | 3.7 | 19695 | 5.6 | 0 |
| 50k | up | 2.38 | 4.9 | 236.5 | 0 / 236.5 | 1.10 | 0 | 1.50 | 4.67 | 6.2 | 49182 | 6.4 | 0 |
| 50k | yinon-fork | 2.46 | 4.4 | 261 | 0 / 261 | 1.20 | 0 | 1.51 | 5.44 | 9.4 | 49131 | 12.1 | 0 |
| 50k | yinon-hybrid | 2.63 | 0.8 | 39 | 12.5 / 26.5 | 1.18 | 1.30 | 0.40 | 4.61 | 11.9 | 49148 | 14.9 | 0 |
| 50k | yinon-forkless | 12.14 | 0 | 1 | 1 / 0 | 7.72 | 4.36 | 0 | 2.44 | 14.9 | 36913 (target missed) | 8.2 | 12 |
| 50k | hybrid | 1.94 | 0.32 | 3 | 2 / 1 | 0.92 | 1.14 | 0.12 | 3.95 | 7.4 | 49127 | 11.7 | 0 |

### Mixed (260 MB, 55% of bytes in 9 KB hashtable hashes)

| rate | config | save s | fork ms | extra mem peak MB | parent / child MB | main cpu s | threads cpu s | child cpu s | ping p99 ms | ping max ms | load ops/s | load max ms | stall s |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | up | 1.62 | 4.9 | 2 | 0 / 2 | 0.05 | 0 | 1.53 | 1.28 | 4.4 | - | - | - |
| 0 | yinon-fork | 1.61 | 5.6 | 1 | 0 / 1 | 0.06 | 0 | 1.51 | 2.08 | 3.5 | - | - | - |
| 0 | yinon-hybrid | 1.78 | 4.1 | 2 | 1 / 4 | 0.06 | 0.57 | 1.09 | 1.92 | 13.2 | - | - | - |
| 0 | yinon-forkless | 4.26 | 0 | 4.5 | 4.5 / 0 | 1.86 | 2.39 | 0 | 1.28 | 11.6 | - | - | - |
| 0 | hybrid | 1.68 | 2.2 | 2 | 1 / 1 | 0.06 | 0.81 | 0.77 | 2.98 | 3.3 | - | - | - |
| 20k | up | 1.96 | 3.2 | 112 | 0 / 112 | 0.59 | 0 | 1.57 | 2.91 | 3.8 | 19685 | 4.6 | 0 |
| 20k | yinon-fork | 2.08 | 3.8 | 135 | 0 / 135 | 0.65 | 0 | 1.59 | 3.13 | 6.8 | 19674 | 7.1 | 0 |
| 20k | yinon-hybrid | 2.10 | 2.7 | 91 | 3.5 / 87.5 | 0.63 | 0.58 | 1.07 | 3.54 | 7.3 | 19681 | 11.0 | 0 |
| 20k | yinon-forkless | 5.43 | 0 | 3 | 3 / 0 | 3.09 | 2.52 | 0 | 4.33 | 11.1 | 19575 | 12.6 | 0.5 |
| 20k | hybrid | 1.91 | 2.2 | 49.5 | 1.5 / 48 | 0.57 | 0.77 | 0.74 | 4.53 | 5.2 | 19682 | 7.5 | 0 |
| 50k | up | 2.38 | 4.2 | 149.5 | 0 / 149.5 | 1.08 | 0 | 1.55 | 3.78 | 6.8 | 49247 | 7.1 | 0 |
| 50k | yinon-fork | 2.53 | 3.0 | 172.5 | 0 / 172.5 | 1.15 | 0 | 1.60 | 3.84 | 8.9 | 49230 | 12.8 | 0 |
| 50k | yinon-hybrid | 2.49 | 2.5 | 113 | 5 / 108 | 1.10 | 0.59 | 1.04 | 6.08 | 8.4 | 49238 | 10.7 | 0 |
| 50k | yinon-forkless | 13.76 | 0 | 25.5 | 25.5 / 0 | 8.98 | 4.72 | 0 | 2.15 | 16.4 | 39613 (target missed) | 13.7 | 12.5 |
| 50k | hybrid | 2.45 | 1.6 | 65 | 2.5 / 63 | 1.12 | 0.78 | 0.79 | 5.23 | 8.8 | 49225 | 10.1 | 0 |

### Big (260 MB, 10 MB hashes)

| rate | config | save s | fork ms | extra mem peak MB | main cpu s | threads cpu s | child cpu s | ping p99 ms | ping max ms | load ops/s | load max ms | stall s |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | up | 1.52 | 5.4 | 1 | 0.06 | 0 | 1.39 | 1.64 | 5.2 | - | - | - |
| 0 | yinon-fork | 1.57 | 4.5 | 1 | 0.05 | 0 | 1.47 | 2.47 | 5.2 | - | - | - |
| 0 | yinon-hybrid | 1.56 | 4.8 | 1 | 0.07 | 0 | 1.45 | 2.94 | 11.6 | - | - | - |
| 0 | yinon-forkless | 3.05 | 0 | 143 | 0.85 | 2.20 | 0 | 213.8 | 312.4 | - | - | - |
| 0 | hybrid | 1.51 | 4.8 | 1 | 0.05 | 0 | 1.38 | 1.96 | 3.2 | - | - | - |
| 20k | up | 1.72 | 3.7 | 1.5 | 0.49 | 0 | 1.42 | 2.75 | 3.5 | 19661 | 4.7 | 0 |
| 20k | yinon-fork | 1.69 | 3.6 | 1.5 | 0.48 | 0 | 1.39 | 3.07 | 4.8 | 19657 | 7.0 | 0 |
| 20k | yinon-hybrid | 1.70 | 3.5 | 1.5 | 0.51 | 0 | 1.38 | 3.31 | 7.0 | 19661 | 8.1 | 0 |
| 20k | yinon-forkless | 3.31 | 0 | 147.5 | 1.33 | 2.16 | 0 | 207.1 | 657.3 | 17701 (target missed) | 661.5 | 1 |
| 20k | hybrid | 1.84 | 3.3 | 1.5 | 0.49 | 0 | 1.50 | 2.93 | 3.9 | 19660 | 5.1 | 0 |
| 50k | up | 2.04 | 3.0 | 1 | 0.85 | 0 | 1.45 | 4.67 | 8.1 | 49263 | 9.2 | 0 |
| 50k | yinon-fork | 2.05 | 3.8 | 1 | 0.85 | 0 | 1.41 | 4.43 | 7.4 | 49265 | 7.7 | 0 |
| 50k | yinon-hybrid | 2.08 | 3.3 | 1 | 0.89 | 0 | 1.44 | 4.61 | 6.8 | 49286 | 7.2 | 0 |
| 50k | yinon-forkless | 2.11 | 0 | 170 | 1.10 | 1.29 | 0 | 135.7 | 456.9 | 45989 (target missed) | 486.4 | 1 |
| 50k | hybrid | 2.01 | 3.0 | 2 | 0.82 | 0 | 1.40 | 4.14 | 8.1 | 49266 | 11.5 | 0 |

### Reading the run

- **On one core every fork-based configuration pays the same latency price: the child competes with the main thread.** PING p99
  during the save goes from 0.3-0.5 ms (run 7) to 3-5 ms at 50k writes/s for stock and for both hybrids alike. Our extra thread
  costs nothing on top: at small/50k our p99 (3.95 ms) is below stock (4.67) and the teammate's hybrid (4.61), and total CPU for
  the save is lower (0.92 main + 1.14 scan + 0.12 child = 2.2 s, versus 1.1 + 1.5 = 2.6 s stock, whose child also pays the kernel's
  copy-on-write work). Serializing from a thread instead of a CoW child is cheaper CPU-wise, not just memory-wise.
- **Save time.** Ours is still the fastest on small (1.42 / 1.56 / 1.94 s idle / 20k / 50k against 1.56 / 1.89 / 2.38 stock and
  1.75 / 2.06 / 2.63 for the teammate's hybrid, whose helper thread now slows the save instead of speeding it up). On mixed and
  big all fork-based configs are within noise of each other; the single core is the bottleneck for everyone.
- **Memory is unchanged from run 7**, as expected: ours 3 MB, teammate's hybrid 32-39 MB, stock 143-261 MB on small at 20-50k.
- **Worst-case latency.** Ours: 2.1-7.4 ms on small, 3.3-8.8 on mixed, 3.2-8.1 on big, the same band as stock (3.6-6.2 / 3.8-6.8 /
  3.5-8.1). The teammate's hybrid keeps its 7-13 ms PING max in the small and mixed cells and 7-12 on big. The load's max
  latency at 50k is 10-15 ms for every fork-based config, single-core scheduling noise dominates it.
- **Forkless does not survive one core.** The main thread owns the copy and the cut: saves take 2.7-4.4 s idle, 12-14 s at 50k
  writes/s on small and mixed, with the offered load unmet (37-40k of 50k ops/s achieved, 12 s of stall). On big it is what run
  7 showed, worse: PING p99 136-214 ms, PING max 312-657 ms, load max 486-661 ms. This is the same shape as the Valkey forkless
  result of run 6 and is the strongest argument in these runs for keeping a child for big values.
- Note on the counters: the teammate's `forkless_*` INFO fields are lifetime maxima / cumulative (e.g. `forkless_retained_peak`
  stays at its earlier high-water mark, `forkless_copied_values` grows across saves), ours are per last save; do not compare
  them cell by cell.

### Caveats specific to this run

- One P-core with its SMT sibling idle is a generous "one CPU": a one-CPU cgroup on a busy host would be tighter. The relative
  picture (fork child versus threads versus main thread all sharing one core) is what the run tests.
- Same 2-rep laptop caveats as run 7; the teammate's forkless idle/20k small cells vary 2.4-5.9 s between reps.


## Verification pass (2026-09-25, commit 20aa93d06 and later)

Run after the owed-bucket fix, to close TASK-1.21.5 on standalone.

| check | build | result |
|---|---|---|
| `./runtest --single unit/snapshot --accurate` (randomized writers race the save, 100 iterations, plus eviction lru/lfu, module metadata, templated hashes, FLUSHALL, hard mark, killed child, SHUTDOWN, cluster-mode harness) | repo build, `-DREDIS_TEST -DDEBUG_ASSERTIONS` | 32/32 passed; the 100-iteration race took 500 s, every iteration byte-identical to the fork instant |
| `./runtest --single unit/snapshot` | `SANITIZER=address MALLOC=libc` worktree (arena-free mode) | 26/26 passed, no sanitizer report |
| `./src/redis-server test snapshot` | repo build | 160/160 (5 new checks for the window move inside a section) |
| full stock suite with `--config bgsave-hybrid yes`, minus AOF / replication / cluster / fork-dependent units | repo build | 1433 tests passed; 5 failed, all replica-based (MASTERAUTH x2, bind-source-addr, pubsubshard setup, maxmemory slave buffers): with the arena on, a replica full sync is refused by design until TASK-1.22; the run then aborted on an untagged BGREWRITEAOF (same refusal). The feature-on suite is not runnable end to end before TASK-1.22 |
| full stock suite, feature off (default config), `./runtest --clients 8` | repo build | all passed (6265 tests), no regression from the window layer being compiled in |

### Steady-state cost with no save running (feature on vs off, same plain binary 20aa93d06)

Two tools were added under this directory. `microbench.sh` measures redis-benchmark ops/s (pinned, 2 M ops, pipeline 16);
`callgrind.sh` counts instructions inside `processCommand` for 200 k SET and 200 k GET under callgrind.

- **ops/s on this laptop is too noisy to resolve a few percent.** Alternating on/off/on/off with 5 runs each gave SET
  1.13 M / 0.76 M / 0.76 M / 0.88 M ops/s and GET 1.28 M / 0.97 M / 1.15 M / 1.27 M: the first mode measured is the fastest
  whatever it is, and run-to-run spread within one mode is up to 40% (powersave governor, thermal state after an hour of
  benchmarks). No conclusion about the feature can be drawn from this; the earlier "SET -4..-6%" figure came from the same kind
  of measurement on the debug build and should be treated as unmeasured.
- **Instruction counts: SET +0.01%, GET +0.9% with the feature on, but without the arena.** Under valgrind the server cannot
  reserve the 16 TB DONTFORK range, the arena reports unavailable and values stay in the default allocator, so this measures the
  feature's code path (write sections, overlay checks, placement decisions) and not the arena `mallocx`. It is the number that
  matters for a build with the feature compiled in but the arena unavailable (any non-Linux or non-jemalloc build).
- **The arena's own cost is still unmeasured on a quiet machine.** `perf stat -e instructions` on the server pid would give it in
  one run but needs `kernel.perf_event_paranoid <= 1` (it is 4 here) or root.

### Verdict for TASK-1.21.5

The serialize-before-write scheme replaces the frozen-dict overlay: same correctness on every harness variant including 100
accurate iterations, TSan (2026-09-22) and ASan clean in arena-free mode, the stock suite unchanged with the feature off, and
on the benchmark it beats the overlay, stock, Valkey forkless and the teammate's hybrid on worst-case latency and extra memory
while matching or beating them on save time and main-thread CPU on 8 cores and on 1 core. Regressions found and fixed during
the pass: the owed-bucket position race (20aa93d06). Open: the arena's steady-state cost is unmeasured on a quiet machine; the
feature-on stock suite needs TASK-1.22 before it can run end to end.


## Fallback child copy-on-write (TASK-1.22 AC5, 2026-09-25, commit f6f680f87, plain build)

`utils/snap-bench/aof-cow.sh`: 1.2 M keys x 100 B (196 MB), memtier same-size overwrites at 20k/s, BGREWRITEAOF issued 2 s in.
With the feature on, `redisFork()` flips the arena forkable for the AOF child (one flip logged) and back afterwards
(`snap_nofork_arena_forkable` 0 when done).

| bgsave-hybrid | rewrite s | aof_last_cow_size | latest_fork_usec |
|---|---|---|---|
| on (arena flipped forkable for the child) | 0.98 | 18.3 MB | 3.3 ms |
| off (values in the default allocator) | 0.98 | 19.1 MB | 3.6 ms |

The fallback child pays the same copy-on-write as stock, as designed: the flip only restores stock behaviour for that child.
Nothing about the hybrid save path is involved in this measurement.


## Full stock suite with the feature on (TASK-1.22, 2026-09-25, commits f6f680f87 .. 4031dbf4b)

Before the arena fork-visibility flip the feature-on suite could not run end to end (AOF rewrite, diskless sync and module
forks were refused). After it, `./runtest --config bgsave-hybrid yes --clients 8` on the DEBUG_ASSERTIONS build found, in
order, and each was fixed the same day:

| finding | cause | fix |
|---|---|---|
| bare `ERR` from BGSAVE after a killed AOF child (moduleapi/rdbloadsave) | killed children reach `resetChildState()` without `checkChildrenDone()`, so the arena stayed forkable and the hybrid save refused to start | flip-back moved into `resetChildState()`; the refusal removed (a debugger child may leave the arena forkable, the hybrid child then just maps it) |
| `keyModified` assertion under XACKDEL during a full sync (integration/replication) | XACK/XACKDEL mutate the stream through a read lookup; safe for SNAP_FORK containers (the child serializes its CoW image) but the debug check did not know | `snapKvobjIsFrozenOriginal` exempts stream, module, hashtable, skiplist and quicklist values; audit addendum |
| `current_save_keys_processed` stayed 0 during a hybrid save (integration/rdb) | the child only sees SNAP_FORK keys | INFO reports the scan thread's and the main thread's dispatched keys |
| active defrag could not bring `allocator_frag_ratio` under 1.1 (unit/memefficiency, 12.5 of 12.9 MB of frag in the arena, 325 M key misses) | the arena's explicit tcache is only GC'd by its own allocations; idle keyspace or a defrag pass (tcache bypassed) pins slabs | `serverCron` flushes the arena tcache once a second |
| defrag recipes never reached their fragmentation precondition (11 tests) | they interleave keys with scripts, channels, argv or field-expiry structures in the same slabs; with the keyspace in its own arena that cannot fragment anything | those tests switch the feature off for their duration (runtime switch); keyspace-fragmenting ones run with it on |
| feature-off servers had headers and dict tables in the arena since the arena is created at startup | only value placement followed the flag | one gate, `zmalloc_nofork_active`, for every arena entry point |
| two stock tests assume stock semantics | `DEBUG LOADAOF` during a save aborts a hybrid save (the scan thread reads the dicts being freed); writes to small values create no CoW in the child | integration/aof-multi-part in-progress assertion stock-only; integration/rdb 'Test child sending info' skipped with the feature on |

Final runs (repo build, `-DREDIS_TEST -DDEBUG_ASSERTIONS`, 8 clients):

| suite | result |
|---|---|
| `./runtest --config bgsave-hybrid yes` (run 7 of the day, before the defrag fixes) | 6246 passed, 21 failed, all in unit/memefficiency (the two defrag findings above) |
| unit/memefficiency alone, feature on, after the fixes | 28 of 28 defrag tests pass (standalone and cluster) |
| `./runtest --config bgsave-hybrid yes` (final) | 5781 passed, 0 failed, then the cluster harness aborted with "cluster config did not reach a consistent state" while three cluster units ran in parallel; those three units pass alone with the feature on (170 tests). A convergence timeout under load, seen with the feature off too on this laptop in earlier sessions |
| `./runtest` (feature off, final) | 6267 passed, 0 failed |
| unit/snapshot, feature on | 41 of 41; unit tests 165 of 165 |
| **after the save_gen removal (41868cb93): `./runtest` feature off / `./runtest --config bgsave-hybrid yes`** | **6283 passed, 0 failed / 6270 passed, 0 failed, end to end, no cluster-harness timeout this time** |


## Per-key save generation removed (TASK-1.21.4, 2026-09-25, commit 41868cb93)

Suspicion: the byte-wide atomic on the kvobj byte (one relaxed store per kvobj creation, a release store per key on
the scan thread) costs SET/GET something. Measured with `callgrind.sh` on plain builds, feature off, instructions
inside `processCommand` per command:

| build | SET Ir/cmd | GET Ir/cmd |
|---|---|---|
| 20aa93d06 (generation, atomics) | 2952.8 | 1861.8 |
| 41868cb93 (upstream kvBits, plain field access) | 2949.9 | 1872.0 |

About 3 instructions per SET, 0.1%; GET within run-to-run noise (the two runs differ in dict layout). On x86 a relaxed
atomic byte store is a plain `mov`, so there was no fence to remove; the win of the removal is the upstream layout and
one less mechanism, not throughput. The feature-on GET number under callgrind (arena unavailable there) moved from
+0.9% to +3.2% over feature-off between the two runs, which is the same noise band and not a signal.


## Run 9 (2026-09-25): new traffic generator, uniform vs 20% hot set, our hybrid vs the teammate's hybrid vs upstream

Raw: `run9.csv`. Binaries: `up` (0d6266f2d), `hybrid` (41868cb93, plain worktree build, after the arena flip and the
save_gen removal), `yinon-hybrid` (1204d0970, `--bgsave-mode hybrid --forkless-string-max 4096`). 8 cores, 2 reps per cell
(6 for our two 50k/uniform cells, see below), medians.

**Traffic changed in this run**, so the big-dataset rows are not comparable with runs 6 to 8. `snapload.py` replaces memtier:
writes go to a random EXISTING field of a hash (`HSET key f<rand>`) or member of a zset (`ZINCRBY`), over the first `hot`
fraction of each class's keys (1.0 = uniform over all keys, 0.2 = a 20% hot set). memtier could only write a fixed element,
so on the big dataset every write hit field `f0` of one of 22 hashes, at most 22 pages, and all configurations showed 1-2 MB
of copy-on-write regardless of the rate. Now they show what big values really cost under writes.

### Small (300 MB, all values eligible for the small path)

| rate | hot | config | save s | fork ms | extra mem MB | child dirty MB | main cpu s | ping p99 ms | ping max ms |
|---|---|---|---|---|---|---|---|---|---|
| 0 | - | up | 2.03 | 5.0 | 2 | 2 | 0.08 | 1.43 | 3.9 |
| 0 | - | yinon-hybrid | 1.49 | 1.3 | 2 | 1 | 0.07 | 0.61 | 15.4 |
| 0 | - | hybrid | 1.36 | 0.66 | 1.5 | 1 | 0.06 | 0.72 | 1.9 |
| 20k | 1.0 | up | 2.18 | 4.2 | 154.5 | 154.5 | 0.60 | 0.31 | 1.7 |
| 20k | 1.0 | yinon-hybrid | 1.57 | 0.79 | 32.5 | 26 | 0.44 | 0.29 | 13.2 |
| 20k | 1.0 | hybrid | 1.45 | 0.27 | 3 | 1 | 0.36 | 0.26 | 0.36 |
| 20k | 0.2 | up | 1.79 | 3.4 | 56.5 | 56.5 | 0.40 | 0.27 | 1.4 |
| 20k | 0.2 | yinon-hybrid | 1.87 | 0.72 | 34 | 28 | 0.59 | 0.34 | 19.6 |
| 20k | 0.2 | hybrid | 1.53 | 0.34 | 3 | 1 | 0.37 | 0.31 | 0.66 |
| 50k | 1.0 | up | 2.13 | 4.0 | 229.5 | 229.5 | 0.73 | 0.35 | 1.3 |
| 50k | 1.0 | yinon-hybrid | 2.05 | 0.69 | 36.5 | 26.5 | 0.95 | 0.43 | 14.7 |
| 50k | 1.0 | hybrid (n=6) | 1.77 | 0.30 | 3 | 2 | 0.65 | 0.34 | see caveat |
| 50k | 0.2 | up | 1.96 | 3.4 | 60 | 60 | 0.63 | 0.28 | 1.2 |
| 50k | 0.2 | yinon-hybrid | 2.25 | 0.71 | 38 | 29 | 1.00 | 0.43 | 21.4 |
| 50k | 0.2 | hybrid | 1.70 | 0.32 | 3 | 1.5 | 0.65 | 0.34 | 0.45 |

### Mixed (260 MB, 55% of bytes in 9 KB hashtable hashes)

| rate | hot | config | save s | fork ms | extra mem MB | child dirty MB | main cpu s | ping p99 ms | ping max ms |
|---|---|---|---|---|---|---|---|---|---|
| 0 | - | up | 1.69 | 4.8 | 2 | 2 | 0.08 | 0.66 | 3.3 |
| 0 | - | yinon-hybrid | 1.53 | 4.7 | 2 | 2 | 0.09 | 0.90 | 14.9 |
| 0 | - | hybrid | 0.92 | 3.3 | 2.5 | 2 | 0.04 | 0.53 | 1.9 |
| 20k | 1.0 | up | 2.36 | 4.4 | 137 | 137 | 0.71 | 0.69 | 3.5 |
| 20k | 1.0 | yinon-hybrid | 1.53 | 2.6 | 91 | 87.5 | 0.48 | 0.29 | 10.4 |
| 20k | 1.0 | hybrid | 1.32 | 1.9 | 47 | 46.5 | 0.39 | 0.28 | 0.48 |
| 20k | 0.2 | up | 2.71 | 2.9 | 47 | 47 | 0.73 | 0.40 | 1.8 |
| 20k | 0.2 | yinon-hybrid | 1.63 | 2.5 | 72 | 69 | 0.47 | 0.26 | 8.5 |
| 20k | 0.2 | hybrid | 1.31 | 1.8 | 28 | 27 | 0.38 | 0.27 | 2.6 |
| 50k | 1.0 | up | 2.82 | 3.5 | 191.5 | 191.5 | 1.08 | 0.39 | 1.0 |
| 50k | 1.0 | yinon-hybrid | 2.19 | 2.7 | 143.5 | 138.5 | 1.09 | 0.66 | 15.1 |
| 50k | 1.0 | hybrid (n=6) | 2.57 | 2.0 | 104 | 103 | 1.13 | 0.63 | see caveat |
| 50k | 0.2 | up | 2.66 | 3.2 | 47 | 47 | 0.93 | 0.41 | 1.3 |
| 50k | 0.2 | yinon-hybrid | 1.81 | 2.5 | 85 | 80.5 | 0.88 | 0.34 | 14.3 |
| 50k | 0.2 | hybrid | 1.32 | 1.9 | 33 | 31.5 | 0.63 | 0.33 | 0.67 |

### Big (260 MB, 10 MB hashes and 300k-member zsets; random existing field/member per write)

| rate | hot | config | save s | fork ms | extra mem MB | main cpu s | ping p99 ms | ping max ms | load ops/s |
|---|---|---|---|---|---|---|---|---|---|
| 0 | - | up | 1.59 | 4.7 | 1 | 0.06 | 0.69 | 1.9 | - |
| 0 | - | yinon-hybrid | 1.58 | 5.5 | 1 | 0.07 | 0.57 | 7.9 | - |
| 0 | - | hybrid | 1.62 | 5.9 | 0.5 | 0.06 | 0.64 | 5.0 | - |
| 20k | 1.0 | up | 1.68 | 3.3 | 99.5 | 0.40 | 0.25 | 0.38 | 19998 |
| 20k | 1.0 | yinon-hybrid | 1.78 | 3.4 | 102 | 0.46 | 0.27 | 8.6 | 19998 |
| 20k | 1.0 | hybrid | 1.68 | 3.1 | 98.5 | 0.41 | 0.23 | 1.2 | 19997 |
| 20k | 0.2 | up | 1.85 | 3.1 | 46.5 | 0.43 | 0.31 | 1.7 | 19998 |
| 20k | 0.2 | yinon-hybrid | 1.96 | 3.2 | 44 | 0.46 | 0.34 | 6.8 | 19997 |
| 20k | 0.2 | hybrid | 1.66 | 2.8 | 42 | 0.38 | 0.26 | 0.77 | 19997 |
| 50k | 1.0 | up | 1.77 | 3.2 | 161.5 | 0.53 | 0.32 | 0.88 | 49985 |
| 50k | 1.0 | yinon-hybrid | 2.78 | 3.7 | 171 | 1.03 | 0.73 | 14.4 | 47067 |
| 50k | 1.0 | hybrid | 2.35 | 3.6 | 170 | 0.76 | 0.53 | 3.3 | 48954 |
| 50k | 0.2 | up | 2.44 | 3.0 | 49 | 0.72 | 0.39 | 3.3 | 49398 |
| 50k | 0.2 | yinon-hybrid | 1.84 | 3.0 | 45 | 0.47 | 0.34 | 9.7 | 49659 |
| 50k | 0.2 | hybrid | 1.91 | 2.8 | 45 | 0.43 | 0.33 | 1.7 | 49990 |

### Reading the run

- **Big values now show their real cost, and it is the same for everyone that forks**: 100 MB of child copy-on-write at 20k
  writes/s uniform, 160-170 MB at 50k, 42-49 MB with the 20% hot set (bounded by the hot set's pages). Our hybrid, the
  teammate's hybrid and stock are within noise of each other here because all three serialize big values from a fork child;
  the hybrid designs neither help nor hurt on this dataset. This is the number a forkless design trades against its main-thread
  copies (runs 6 and 8: Valkey forkless 140-170 MB of parent growth plus 65-214 ms PING p99 on the same dataset).
- **Small and mixed repeat run 7** with the new generator: at 20k uniform ours is 3 MB / 0.27 ms fork / 0.36 ms PING max
  against 32.5 MB / 0.79 ms / 13.2 ms for the teammate's hybrid and 154.5 MB / 4.2 ms / 1.7 ms for stock. On mixed the
  big-value CoW remains for all three and is smallest for ours (47 vs 91 vs 137 MB at 20k) because our child finishes sooner.
- **The 20% hot set changes little for the small path** (3 MB either way for us, 32-38 MB for the teammate's hybrid) and
  cuts stock and big-value CoW by 2.5-4x, as expected once repeated writes land on already-copied pages. The teammate's
  hybrid keeps its 7-21 ms PING max in every cell with writes, unchanged from run 7.
- **Caveat, and the reason two cells have n=6: thermal throttling.** Our two 50k/uniform cells each had one rep where the
  CPU seconds of the main thread, the scan thread AND the child doubled at once (small: 4.18 s save, PING max 19.8 ms;
  mixed: 5.24 s, 21.9 ms) while the other rep was normal (1.57 s / 0.36 ms; 2.45 s / 0.8 ms). That signature is a clock drop,
  not a lock. Re-measuring those cells with frequency sampling confirmed it: package at 85-89 C, cores averaging 1.5-2.6 GHz
  with dips to 0.76 GHz (max 4.7), and save time tracking the average clock (1.40 s at 2.5 GHz, 2.25 s at 1.65 GHz). Every
  configuration in this run was affected to an unknown and uneven degree, since they ran back to back for 100 minutes:
  treat save-time and latency differences under about 25% as noise in this run; memory numbers do not depend on the clock.
  bench.py now records `cpu_mhz_min`, `cpu_mhz_avg`, `temp_c_start`, `temp_c_max` per cell and has a thermal gate
  (`--cool-below C`, `COOL=` in run.sh) that waits for the package to cool before each cell. A rerun with the gate, and with
  configurations interleaved per cell now that datasets load from the RDB cache in 2 s, is the clean way to get medians that
  are comparable across configurations.


## Run 10 (2026-09-25/26): run 9 on AWS, configurations interleaved per cell

Raw: `run10.csv`. Host: EC2 **c7i.4xlarge** (Xeon Platinum 8488C, 8 cores / 16 vCPUs, 30 GB, Ubuntu 24.04, kernel
6.8.0-1021-aws, no cpufreq exposed), server on 4 physical cores with both threads (vCPUs 0-3,8-11), load generator on the other
4 (4-7,12-15), dumps and dataset cache on tmpfs. Same binaries as run 9 (upstream 0d6266f2d, hybrid 41868cb93, teammate
1204d0970), plain builds on the instance. `run-interleaved.sh`: for every (dataset, rate, rep) each binary is started, loads
the dataset from the RDB cache, runs one cell and stops, so the three configurations see the same machine state. 2 reps per
cell; rep-to-rep spread is tight everywhere (our save times agree within 0.1 s in 17 of 18 cells), the opposite of run 9.

The run was interrupted three times by the FinOps NoTag-NoRun policy (FINOPS-416): the instance had no `owner`/`team` tags
and was stopped 12-22 minutes after each boot. Cells are self-contained and the CSV was trimmed to complete cells before each
resume, so the interruptions cost time, not data.

### Small (300 MB, all values eligible for the small path)

| rate | hot | config | save s | fork ms | extra mem MB | child dirty MB | main cpu s | ping p99 ms | ping max ms |
|---|---|---|---|---|---|---|---|---|---|
| 0 | - | up | 2.55 | 4.9 | 1.5 | 3.5 | 0.06 | 0.62 | 3.9 |
| 0 | - | yinon-hybrid | 2.34 | 0.84 | 1.5 | 1 | 0.05 | 0.64 | 9.8 |
| 0 | - | hybrid | 2.27 | 0.40 | 1 | 1 | 0.04 | 0.65 | 0.79 |
| 20k | 1.0 | up | 2.51 | 4.4 | 172 | 172 | 0.62 | 0.69 | 3.2 |
| 20k | 1.0 | yinon-hybrid | 2.45 | 0.98 | 35 | 28 | 0.66 | 0.65 | 7.5 |
| 20k | 1.0 | hybrid | 2.23 | 0.41 | 3 | 1 | 0.58 | 0.70 | 0.89 |
| 20k | 0.2 | up | 2.72 | 3.5 | 165.5 | 165.5 | 0.66 | 0.64 | 0.82 |
| 20k | 0.2 | yinon-hybrid | 2.60 | 0.99 | 35 | 28 | 0.69 | 0.65 | 8.8 |
| 20k | 0.2 | hybrid | 2.33 | 0.41 | 3 | 1 | 0.61 | 0.65 | 0.77 |
| 50k | 1.0 | up | 2.56 | 5.2 | 243 | 243 | 1.30 | 0.41 | 4.6 |
| 50k | 1.0 | yinon-hybrid | 2.42 | 0.99 | 41 | 28 | 1.44 | 0.51 | 11.5 |
| 50k | 1.0 | hybrid | 2.12 | 0.40 | 3 | 1 | 1.19 | 0.49 | 0.63 |
| 50k | 0.2 | up | 2.65 | 3.6 | 223 | 223 | 1.35 | 0.41 | 2.3 |
| 50k | 0.2 | yinon-hybrid | 2.57 | 0.98 | 40 | 28 | 1.54 | 0.56 | 10.6 |
| 50k | 0.2 | hybrid | 2.30 | 0.40 | 3 | 1 | 1.29 | 0.39 | 0.53 |

### Mixed (260 MB, 55% of bytes in 9 KB hashtable hashes)

| rate | hot | config | save s | fork ms | extra mem MB | child dirty MB | main cpu s | ping p99 ms | ping max ms |
|---|---|---|---|---|---|---|---|---|---|
| 0 | - | up | 2.30 | 3.5 | 1.5 | 1.5 | 0.05 | 0.63 | 3.6 |
| 0 | - | yinon-hybrid | 1.87 | 3.1 | 2 | 1.5 | 0.06 | 0.62 | 9.2 |
| 0 | - | hybrid | 1.11 | 2.0 | 1 | 1 | 0.03 | 0.66 | 0.83 |
| 20k | 1.0 | up | 2.26 | 4.6 | 143.5 | 143.5 | 0.62 | 0.67 | 2.9 |
| 20k | 1.0 | yinon-hybrid | 1.83 | 4.0 | 108 | 105 | 0.62 | 0.68 | 10.4 |
| 20k | 1.0 | hybrid | 1.42 | 2.7 | 53 | 51 | 0.50 | 0.63 | 1.6 |
| 20k | 0.2 | up | 2.47 | 4.1 | 104.5 | 104.5 | 0.70 | 0.67 | 3.9 |
| 20k | 0.2 | yinon-hybrid | 1.98 | 3.4 | 75.5 | 71.5 | 0.66 | 0.65 | 7.7 |
| 20k | 0.2 | hybrid | 1.36 | 2.3 | 29.5 | 28 | 0.47 | 0.67 | 0.99 |
| 50k | 1.0 | up | 2.31 | 5.2 | 196 | 196 | 1.17 | 0.45 | 2.6 |
| 50k | 1.0 | yinon-hybrid | 1.92 | 4.0 | 152.5 | 147.5 | 1.17 | 0.48 | 7.0 |
| 50k | 1.0 | hybrid | 1.86 | 3.0 | 99 | 96 | 1.04 | 0.48 | 2.2 |
| 50k | 0.2 | up | 2.25 | 2.8 | 88 | 88 | 1.11 | 0.35 | 0.52 |
| 50k | 0.2 | yinon-hybrid | 1.91 | 2.6 | 75 | 70.5 | 0.98 | 0.59 | 6.3 |
| 50k | 0.2 | hybrid | 1.30 | 1.5 | 34 | 32 | 0.78 | 0.50 | 0.61 |

### Big (260 MB, 10 MB hashes and 300k-member zsets; random existing field/member per write)

| rate | hot | config | save s | fork ms | extra mem MB | main cpu s | ping p99 ms | ping max ms |
|---|---|---|---|---|---|---|---|---|
| 0 | - | up | 1.99 | 3.9 | 1 | 0.03 | 0.65 | 3.6 |
| 0 | - | yinon-hybrid | 2.00 | 3.3 | 1 | 0.05 | 0.62 | 7.7 |
| 0 | - | hybrid | 1.97 | 3.7 | 1 | 0.05 | 0.63 | 2.8 |
| 20k | 1.0 | up | 2.09 | 2.9 | 115.5 | 0.64 | 0.67 | 0.82 |
| 20k | 1.0 | yinon-hybrid | 2.25 | 3.0 | 120 | 0.70 | 0.75 | 5.6 |
| 20k | 1.0 | hybrid | 2.34 | 2.7 | 121.5 | 0.65 | 0.73 | 2.7 |
| 20k | 0.2 | up | 1.88 | 2.8 | 43 | 0.49 | 0.66 | 2.1 |
| 20k | 0.2 | yinon-hybrid | 1.98 | 3.1 | 44 | 0.54 | 0.65 | 4.8 |
| 20k | 0.2 | hybrid | 2.02 | 3.1 | 44 | 0.53 | 0.63 | 2.2 |
| 50k | 1.0 | up | 2.23 | 3.4 | 174 | 1.31 | 0.43 | 2.9 |
| 50k | 1.0 | yinon-hybrid | 2.14 | 3.8 | 172 | 1.29 | 0.41 | 5.3 |
| 50k | 1.0 | hybrid | 2.16 | 3.5 | 172.5 | 1.29 | 0.40 | 2.9 |
| 50k | 0.2 | up | 1.96 | 2.8 | 45 | 1.09 | 0.37 | 2.0 |
| 50k | 0.2 | yinon-hybrid | 1.97 | 2.8 | 45 | 1.14 | 0.46 | 6.0 |
| 50k | 0.2 | hybrid | 1.92 | 3.0 | 45 | 1.07 | 0.40 | 1.5 |

### Reading the run

- **This is the clean version of run 9.** No thermal drift, no order effect, rep spreads of a few percent. The conclusions
  of runs 7 and 9 hold on a server CPU: on the small path our hybrid saves in 2.1-2.4 s against 2.3-2.6 s for the teammate's
  hybrid and 2.5-2.7 s for stock, with a 0.4 ms fork (his 1.0, stock 3.5-5.2), 3 MB of extra memory (his 35-41, stock 165-243)
  and a PING max of 0.5-0.9 ms (his 7.5-11.5, stock 0.8-4.6).
- **Mixed is where the two hybrids differ most**: 53 MB vs 108 MB extra memory at 20k uniform (stock 143.5), 99 vs 152.5 at
  50k (stock 196), and save time 1.1-1.9 s vs 1.8-2.0 s vs 2.3 s. Both children copy the pages of the 9 KB hashes as they are
  written; ours finishes sooner and keeps the headers and small values out of the child.
- **Big is a wash for everything that forks**, as it must be: 116-123 MB at 20k uniform, 172-175 MB at 50k, 43-45 MB with the
  hot set, for all three. The only difference on this dataset is the teammate's PING max of 4.8-7.7 ms against 1.5-2.9 ms for
  ours and 0.8-3.6 ms for stock.
- **The 20% hot set** behaves as in run 9: it cuts stock and big-value copy-on-write by 2-4x and changes nothing for our
  small path.
- **PING p99 is 0.4-0.7 ms for every configuration here**, higher than the 0.25-0.35 ms the laptop showed: the probe runs
  from Python on the load cores and a virtualized network stack adds its share. It is the same for all three, so it does not
  affect the comparison; PING max does, and it separates the three designs clearly.
- Main-thread CPU during the save is at or below stock in every small and mixed cell for ours; the teammate's hybrid is
  5-15% above stock at 50k on small (its parent-side helper handshake).


## Copy-on-write sources other than big values (2026-09-26, `cow-sources.py`)

Mixed dataset (260 MB), write traffic at 20k/s on the small classes only (strings, lists, sets; the 9 KB hashes untouched), so
every dirtied page in the child is a parent mechanism other than big-value CoW. Laptop, plain build 41868cb93, one save per
variant, measured 3 s into the traffic.

| variant | our hybrid: child dirty MB | stock: child dirty MB | INFO memory during the save |
|---|---|---|---|
| base (4 pipelined clients) | 3 | 95 | clients 2 KB |
| + replica (diskless sync, 1 MB backlog churning) | 3 | 97 | slaves 37 KB, backlog 1 MB |
| + AOF (appendfsync everysec) | 3 | 97 | aof_buf 3 KB |
| + 64 pipelined clients | 3 | 92 | clients 2 KB |

The 3 MB floor is the same as an idle save's 1-3 MB and is mostly the child's own working set (rio buffer, stack, allocator
metadata), not parent pages. Adding a replica, AOF or 64 clients moved nothing at this rate: the reply buffer is 16 KB per
client and reused in place, the default backlog is 1 MB, the AOF buffer a few KB. On this workload there is nothing left to
mark DONTFORK. Where it can matter in production: a churning replication backlog under a heavy write stream (CoW bounded by
min(backlog size, stream bytes during the save); 50 MB/s x 2 s = 100 MB, the size of the big-value CoW), and thousands of
active clients' reply buffers and reply lists. Both are single allocation sites (`replBufBlock`, `c->buf`/`clientReplyBlock`),
never read by any fork child, and `zfree` is already range-aware, so moving them into the arena is cheap; it should be done
against a measurement with a heavy replication stream first.


## Run 11 (2026-09-26): mixed dataset, our hybrid vs Valkey forkless vs Valkey fork, c7i.4xlarge

Raw: `run11.csv`. Same host and layout as run 10 (server on 4 physical cores with both threads, load on the other 4, tmpfs).
Binaries: `hybrid` 41868cb93 (plain), `valkey-forkless` and `valkey-fork` from Valkey unstable 06d35aca6 built on the instance
(`--forkless-infrastructure-enabled yes --bgsave-default-method forkless` for the forkless rows). Interleaved per cell, 2 reps.
Mixed = 800k strings x 50 B, 200k 1-element lists, 200k 1-element sets, 16k hashtable hashes of 64 x 96 B (about 9 KB); 260 MB;
writes hit a random existing field of the hashes, same-size overwrites elsewhere.

| rate | keys | config | save s | fork ms | extra mem MB | main cpu s | threads cpu s | PING p99 ms | PING max ms | load p99 ms | load max ms | load ops/s |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| idle | - | valkey-fork | 2.18 | 3.3 | 1 | 0.04 | 0 | 0.65 | 2.4 | - | - | - |
| idle | - | valkey-forkless | 2.57 | 0 | 1 | 0.60 | 2.2 | 1.11 | 1.2 | - | - | - |
| idle | - | hybrid | 1.40 | 2.2 | 2 | 0.03 | 1.1 | 0.61 | 1.2 | - | - | - |
| 20k | uniform | valkey-fork | 2.21 | 4.8 | 141 | 0.63 | 0 | 0.59 | 3.8 | 0.64 | 5.7 | 19998 |
| 20k | uniform | valkey-forkless | 3.14 | 0 | 1.5 | 1.07 | 2.6 | 0.98 | 1.1 | 1.74 | 12.4 | 19633 |
| 20k | uniform | hybrid | 1.41 | 2.4 | 52.5 | 0.49 | 1.1 | 0.61 | 1.2 | 0.63 | 3.7 | 19998 |
| 20k | 20% hot | valkey-fork | 2.17 | 4.1 | 100.5 | 0.59 | 0 | 0.60 | 1.5 | 0.62 | 4.8 | 19997 |
| 20k | 20% hot | valkey-forkless | 2.99 | 0 | 1 | 1.09 | 2.5 | 0.96 | 1.1 | 1.14 | 12.2 | 19898 |
| 20k | 20% hot | hybrid | 1.32 | 2.3 | 29 | 0.44 | 1.1 | 0.69 | 2.0 | 0.64 | 3.6 | 19998 |
| 50k | uniform | valkey-fork | 2.31 | 5.0 | 201 | 1.22 | 0 | 0.47 | 2.3 | 0.29 | 6.6 | 49995 |
| 50k | uniform | valkey-forkless | 3.10 | 0 | 1.5 | 1.84 | 2.7 | 1.06 | 1.2 | 1.51 | 17.1 | 48876 |
| 50k | uniform | hybrid | 2.15 | 2.8 | 104.5 | 1.21 | 1.2 | 0.39 | 2.8 | 0.30 | 4.9 | 49993 |
| 50k | 20% hot | valkey-fork | 2.21 | 4.7 | 131 | 1.12 | 0 | 0.39 | 2.9 | 0.31 | 6.2 | 49995 |
| 50k | 20% hot | valkey-forkless | 2.99 | 0 | 2 | 1.90 | 2.6 | 0.68 | 1.0 | 0.56 | 16.4 | 49784 |
| 50k | 20% hot | hybrid | 1.36 | 2.3 | 34.5 | 0.85 | 1.1 | 0.41 | 2.0 | 0.37 | 4.2 | 49994 |

### Reading the run

- **Memory: forkless wins outright on this dataset.** 1-2 MB of extra memory in every cell against 29-105 MB for us and
  100-201 MB for Valkey's fork. The reason is the value sizes: the biggest value here is a 9 KB hash, which Valkey copies on the
  main thread before it is written for a few microseconds, so nothing is ever forked and nothing is copied on write. Our 29-105 MB
  is the child's copy-on-write on exactly those hashes, the class we send to the child.
- **Everything the main thread does, forkless pays for on the main thread.** Its main-thread CPU is 0.6 s with no traffic at all
  (the snapshot cut and the value copies of the walk) and 1.07-1.9 s under writes, 1.5-2.2x ours, and its save takes 2.6-3.1 s
  against our 1.3-2.2 s and Valkey fork's 2.2-2.3 s. Command latency shows the same thing: load p99 1.1-1.7 ms against our
  0.3-0.6 ms, load max 12-17 ms against our 3.6-4.9 ms, PING p99 about 1.0 ms against 0.4-0.7 ms for the two fork-based
  designs, and at 50k writes/s it is the only configuration that misses the offered rate (48.9k of 50k).
- **PING max is where forkless looks good here (1.0-1.2 ms)**: no fork, so no fork stall. Ours is 1.2-2.8 ms, which is the
  fork of the page tables for the 16k hashes. On the big dataset in runs 6 and 8 the same forkless design showed 65-214 ms PING
  p99 because 10 MB values are copied on the main thread; the copy cost is proportional to value size and this dataset's values
  are small enough to hide it.
- **The trade in one line for mixed data**: forkless buys about 50-100 MB of peak memory per save on a 260 MB dataset for a
  permanent 1.5-2x main-thread cost during saves, 3x command latency p99, 3-4x command latency max and 40% longer saves. Our
  design keeps the command path at stock and pays copy-on-write only for values above the small-value cap; where that cap sits
  (4 KB today) is the lever that trades between the two, since 9 KB hashes serialized on the scan thread would take the 29-105 MB
  away at the cost of more scan-thread work.


## Run 12 (2026-09-26): 4 GB datasets, our hybrid vs upstream vs Valkey forkless, c7i.4xlarge

Raw: `run12.csv`, driver `run4g.sh` (per-dataset scale, tmpfs remounted to 24 GB, a watcher deleting the dump files of
configurations not running so three 3 GB dumps plus two caches fit). Host: a fresh c7i.4xlarge (i-00fe113633e1d929e, 30 GB),
server on vCPUs 0-3,8-11, load on 4-7,12-15, dumps and dataset cache on tmpfs. Binaries built on the instance: `up` = upstream
0d6266f2d, `hybrid` = 54b7d4f0c (`--bgsave-hybrid yes`, cap 4 KB), `valkey-forkless` = Valkey unstable 06d35aca6
(`--forkless-infrastructure-enabled yes --bgsave-default-method forkless`). Interleaved per cell, 2 reps, every save `ok`, no
hard-mark aborts. Scales: small 13.5 (18.9 M keys, 4.1 GB), mixed 16 (19.5 M keys, 4.0 GB), big 16 (240 keys, 3.9 GB).
**The big dataset changed for this run**: it now has a class of 1M-field hashes (16 x about 100 MB) beside 192 x 100k-field
hashes (10 MB) and 32 zsets of 300k; runs 4-11 had 22 x 100k-field hashes only. Columns: `peak` = parent RssAnon + child
Private_Dirty (the memory the machine must have), `extra` = peak minus the parent's RSS before the save, `wavg/wp99/wmax` =
load-generator latency per pipelined batch, `ops` = achieved writes/s against the offered rate. Medians of 2 reps; ranges where
the reps differ materially.

### Small (4.1 GB, 18.9 M keys: 100-byte strings and 10-field listpack hashes)

| rate | hot | config | save s | fork ms | peak MB | extra MB | main cpu s | threads cpu s | ping max ms | ops | wavg ms | wp99 ms | wmax ms |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | - | up | 33.8 | 34.4 | 4109 | 2 | 0.46 | 0 | 31.3 | - | - | - | - |
| 0 | - | hybrid | 31.3 | 1.6 | 4030 | 2 | 0.41 | 31.0 | 1.8 | - | - | - | - |
| 0 | - | valkey-forkless | 36.1 | 0 | 3911 | 0.5 | 10.7 | 35.2 | 1.7 | - | - | - | - |
| 20k | 1.0 | up | 35.1 | 33.6 | 6408 | 2380 | 7.7 | 0 | 30.0 | 19994 | 0.218 | 0.60 | 34.5 |
| 20k | 1.0 | hybrid | 31.5 | 1.6 | 4036 | 8 | 6.9 | 31.2 | 1.0 | 19999 | 0.220 | 0.63 | 2.5 |
| 20k | 1.0 | valkey-forkless | 46.0 | 0 | 3904 | 9 | 17.2 | 41.3 | 1.7 | 19980 | 0.302 | 3.41 | 7.0 |
| 50k | 1.0 | up | 35.6 | 38.0 | 7334 | 3306 | 13.9 | 0 | 36.1 | 49982 | 0.109 | 0.45 | 39.5 |
| 50k | 1.0 | hybrid | 29.3 | 1.6 | 4036 | 7.5 | 11.8 | 29.1 | 1.4 | 49998 | 0.134 | 0.49 | 3.1 |
| 50k | 1.0 | valkey-forkless | 41.9 | 0 | 3916 | 20.5 | 25.6 | 40.9 | 2.7 | 49999 | 0.202 | 1.25 | 4.5 |
| 50k | 0.2 | up | 35.3 | 40.2 | 6910 | 2882 | 14.5 | 0 | 38.4 | 49980 | 0.095 | 0.33 | 41.3 |
| 50k | 0.2 | hybrid | 29.7 | 1.6 | 4036 | 8 | 12.8 | 29.5 | 1.2 | 49999 | 0.118 | 0.31 | 3.6 |
| 50k | 0.2 | valkey-forkless | 41.0 | 0 | 3906 | 11.5 | 23.5 | 39.7 | 2.6 | 49999 | 0.183 | 1.12 | 4.9 |

### Mixed (4.0 GB, 19.5 M keys: 45% of bytes in small values, 55% in 256k hashtable hashes of about 9 KB)

| rate | hot | config | save s | fork ms | peak MB | extra MB | main cpu s | threads cpu s | ping max ms | ops | wavg ms | wp99 ms | wmax ms |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | - | up | 36.9 | 36.7 | 3979 | 2.5 | 0.52 | 0 | 35.8 | - | - | - | - |
| 0 | - | hybrid | 18.7 | 16.6 | 3909 | 2 | 0.27 | 18.5 | 17.0 | - | - | - | - |
| 0 | - | valkey-forkless | 40.3 | 0 | 3900 | 1 | 11.0 | 37.7 | 1.6 | - | - | - | - |
| 20k | 1.0 | up | 36.0 | 38.4 | 6223 | 2324 | 8.4 | 0 | 37.5 | 19995 | 0.205 | 0.63 | 39.1 |
| 20k | 1.0 | hybrid | 23.3 | 18.7 | 4772 | 852 | 5.8 | 19.1 | 16.6 | 20000 | 0.194 | 0.65 | 20.1 |
| 20k | 1.0 | valkey-forkless | 44.1 | 0 | 3828 | 7 | 15.8 | 39.2 | 1.7 | 18970 (missed) | 0.346 | 7.1 | 13.7 |
| 50k | 1.0 | up | 37.4 | 41.9 | 7058 | 3148 | 14.8 | 0 | 38.8 | 49981 | 0.088 | 0.50 | 43.6 |
| 50k | 1.0 | hybrid | 31.3 | 17.9 | 5492 | 1576 | 14.3 | 18.7 | 15.5 | 49995 | 0.085 | 0.40 | 20.9 |
| 50k | 1.0 | valkey-forkless | 44.4 | 0 | 3861 | 15.5 | 23.6 | 40.4 | 1.9 | 46490 (missed) | 0.322 | 12.6 | 17.6 |
| 50k | 0.2 | up | 36.0 | 42.8 | 5899 | 1988 | 14.8 | 0 | 40.6 | 49982 | 0.079 | 0.32 | 44.5 |
| 50k | 0.2 | hybrid | 21.8 | 18.7 | 4416 | 506 | 9.6 | 18.8 | 16.9 | 49996 | 0.082 | 0.33 | 20.8 |
| 50k | 0.2 | valkey-forkless | 45.3 | 0 | 3827 | 14 | 26.3 | 42.1 | 1.25 | 49203 | 0.166 | 0.91 | 16.9 |

### Big (3.9 GB, 240 keys: 16 hashes of 1M fields (about 100 MB), 192 hashes of 100k fields (10 MB), 32 zsets of 300k)

| rate | hot | config | save s | fork ms | peak MB | extra MB | main cpu s | threads cpu s | ping max ms | ops | wavg ms | wp99 ms | wmax ms | stall s | blocked clients |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | - | up | 30.2 | 24-39 | 3890 | 1 | 0.43 | 0 | 1.1-35.6 | - | - | - | - | - | 0 |
| 0 | - | hybrid | 31.2 | 24-33 | 3800 | 1.5 | 0.45 | 0 | 24-29 | - | - | - | - | - | 0 |
| 0 | - | valkey-forkless | 31.8 | 0 | 3639 | 1 | 0.68 | 31.7 | 1.2 | - | - | - | - | - | 0 |
| 20k | 1.0 | up | 32.5 | 31-35 | 5628 | 1830 | 7.5 | 0 | 28.9 | 19995 | 0.224 | 0.70 | 33.7 | 0 | 0 |
| 20k | 1.0 | hybrid | 31.3 | 26-28 | 5586 | 1787 | 7.2 | 0 | 25.8 | 19997 | 0.223 | 0.71 | 28.4 | 0 | 0 |
| 20k | 1.0 | valkey-forkless | 32.9 | 0 | 3606 | 1 | 1.1 | 32.8 | 1.4 | 9689 (48%) | 0.94 | 0.65 | 25672-29442 | 9-10 | 7 |
| 50k | 1.0 | up | 33.2 | 26-34 | 6546 | 2748 | 11.9 | 0 | 29.7 | 49986 | 0.133 | 0.56 | 31.1 | 0 | 0 |
| 50k | 1.0 | hybrid | 33.7 | 26-28 | 6557 | 2759 | 12.0 | 0 | 26.7 | 49989 | 0.150 | 0.61 | 28.8 | 0 | 0 |
| 50k | 1.0 | valkey-forkless | 33.4 | 0 | 3606 | 1 | 1.3 | 33.4 | 0.9 | 24723 (49%) | 0.81 | 0.68 | 30136-32759 | 10-11 | 14 |
| 50k | 0.2 | up | 32.0 | 33-34 | 4398 | 599 | 9.5 | 0 | 30.4 | 49984 | 0.142 | 0.65 | 34.8 | 0 | 0 |
| 50k | 0.2 | hybrid | 32.5 | 25-26 | 4398 | 599 | 9.8 | 0 | 22.3 | 49990 | 0.143 | 0.63 | 27.0 | 0 | 0 |
| 50k | 0.2 | valkey-forkless | 33.8 | 0 | 3607 | 2 | 5.9 | 33.8 | 1.2 | 35355 (71%) | 0.31 | 0.47 | 19708-20518 | 6-8 | 14 |

### Reading the run

- **The 300 MB conclusions hold at 4 GB and the absolute numbers are now operator-sized.** Small: upstream needs 7.3 GB of
  memory to save a 4 GB dataset at 50k writes/s (3.3 GB of copy-on-write, over 80% of the data), a 34-40 ms fork stall that
  lands on PING max and write max alike; ours saves 6 s faster with 8 MB extra, a 1.6 ms stall and a 3 ms write max.
  Forkless keeps memory at 9-20 MB but its save is 40-50% longer than ours, its main thread burns 17-26 CPU-seconds against
  7-13, and write p99 is 2-5x (3.4 ms at 20k). Nobody misses the offered rate on small.
- **Mixed is where the cap costs us, now in gigabytes.** The 256k 9 KB hashes are forkable: 2.3 GB of page tables give a
  17-19 ms stall (upstream 37-43) and their copy-on-write is 0.5-1.6 GB (upstream 2.0-3.1). Forkless stays at 7-16 MB but
  misses the offered rate at 20k (94.9%) and 50k uniform (93%), with write p99 of 7-13 ms against 0.4-0.65 for the two
  fork-based designs and 42-45 s saves against our 22-31. Our save is the shortest in every mixed cell because the scan thread
  and the child overlap; idle it is half of upstream (18.7 vs 36.9 s).
- **Big is fork parity for us and a failure mode for forkless.** Upstream and hybrid agree within noise on every column
  (1.8 GB and 2.7 GB of copy-on-write at 20k and 50k, 0.6 GB with the hot set, 25-35 ms stalls). Forkless with 1M-field
  hashes: a write to a hash under serialization waits until the whole 100 MB hash is written, so the load generator saw
  20-33 s maximum write latency, 6-11 s of stalled seconds per save, 7-14 clients blocked at the peak, and delivered
  48%, 49% and 71% of the offered rate at 20k, 50k uniform and 50k hot. Its p99 stays at 0.5-0.7 ms because the blocked
  writes are few; they are just very long.
- **Base RSS differs between binaries for the same data** (small: up 4186 loaded fresh / 4028 from cache, hybrid 4028, Valkey
  3926): compare `extra` across configurations and `peak` within one.
- The scan thread's CPU (`threads cpu s`) equals the save duration on small and mixed: it serializes for the whole save, on a
  core the fork child would otherwise use. Forkless's background thread does the same for longer.


## Run 13 (2026-09-26): 4 GB datasets whose big component is 1M-field hashes, our hybrid vs upstream vs Valkey forkless

Raw: `run13.csv`, driver `run13.sh` (same host, binaries and layout as run 12; caches cleared first). Two new datasets, both at
scale 16 so the earlier names stay comparable: **mixed1m** = 12.8 M strings x 50 B, 3.2 M one-element lists, 3.2 M one-element
sets and 24 hashtable hashes of 1M fields x 64 B (about 90 MB each): 19.2 M keys, 4.0-4.2 GB, roughly half the bytes small and
half in the big hashes; **big1m** = 36 hashes of 1M fields and 32 zsets of 300k members: 68 keys, 3.8-4.0 GB. Same matrix
(idle, 20k, 50k uniform, 50k on a 20% hot set; 2 reps; interleaved per cell), every save `ok`, no hard-mark aborts. Values as
rep a / rep b.

### mixed1m

| rate | hot | config | save s | fork ms | peak MB | extra MB | main cpu s | threads cpu s | ping max ms | ops | wavg ms | wp99 ms | wmax ms | stall s | blocked |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | - | up | 40.6 / 43.9 | 40 / 29 | 4232 / 4073 | 4 / 1 | 0.6 | 0 | 38.5 / 30.0 | - | - | - | - | - | 0 |
| 0 | - | hybrid | 41.7 / 42.6 | 13.6 / 14.1 | 4075 / 4074 | 2 / 1 | 0.6 | 19.9 | 13.4 / 14.0 | - | - | - | - | - | 0 |
| 0 | - | valkey-forkless | 50.7 / 50.8 | 0 | 4106 / 4008 | 1 | 11.9 / 11.2 | 41.0 / 43.9 | 2.1 / 2.3 | - | - | - | - | - | 0 |
| 20k | 1.0 | up | 43.7 / 42.4 | 30 / 29 | 6584 / 6549 | 2512 / 2476 | 9.9 / 9.6 | 0 | 26.7 / 28.8 | 19997 | 0.21 | 0.64 | 30.7 / 30.4 | 0 | 0 |
| 20k | 1.0 | hybrid | 41.1 / 41.6 | 17.6 / 16.9 | 5321 / 5327 | 1248 / 1254 | 9.0 / 9.9 | 18.7 / 18.1 | 15.2 / 17.5 | 19999 | 0.20 / 0.21 | 0.65 | 18.8 | 0 | 0 |
| 20k | 1.0 | valkey-forkless | 49.9 / 46.6 | 0 | 4011 / 4010 | 4 / 3 | 14.4 / 13.5 | 47.4 / 44.4 | 1.9 / 1.8 | 15109 / 14981 (75%) | 0.63 | 1.51 / 1.46 | 22756 / 22375 | 6 / 7 | 9 |
| 50k | 1.0 | up | 46.2 / 40.6 | 31 / 41 | 7352 / 7298 | 3279 / 3225 | 18.7 / 16.2 | 0 | 28.2 / 36.9 | 49988 | 0.08 | 0.36 / 0.41 | 32.2 / 42.4 | 0 | 0 |
| 50k | 1.0 | hybrid | 41.4 / 40.0 | 14.9 / 16.1 | 5819 / 5806 | 1746 / 1733 | 17.0 / 16.8 | 18.2 / 18.4 | 12.7 / 13.3 | 49999 | 0.09 | 0.41 / 0.35 | 16.9 / 17.8 | 0 | 0 |
| 50k | 1.0 | valkey-forkless | 44.5 / 46.7 | 0 | 4014 / 4015 | 7 / 8 | 17.7 / 18.5 | 43.8 / 46.2 | 1.5 | 38028 / 38221 (76%) | 0.38 | 1.41 | 21414 / 22076 | 10 / 11 | 16 |
| 50k | 0.2 | up | 45.4 / 45.5 | 41 / 31 | 5893 / 5894 | 1820 / 1821 | 17.6 / 17.1 | 0 | 41.1 / 30.6 | 49988 | 0.08 | 0.26 / 0.33 | 42.9 / 31.9 | 0 | 0 |
| 50k | 0.2 | hybrid | 41.5 / 42.0 | 14.9 / 17.6 | 4410 / 4409 | 337 / 336 | 16.2 / 16.5 | 18.4 / 18.7 | 14.3 / 14.5 | 49999 | 0.09 | 0.45 / 0.50 | 16.9 / 19.2 | 0 | 0 |
| 50k | 0.2 | valkey-forkless | 45.1 / 45.4 | 0 | 4015 | 8 | 24.1 / 24.6 | 43.3 / 42.4 | 1.5 / 2.1 | 46753 / 46894 (94%) | 0.30 / 0.29 | 1.40 / 1.41 | 3553 / 3311 | 32 / 27 | 14 / 15 |

### big1m

| rate | hot | config | save s | fork ms | peak MB | extra MB | main cpu s | threads cpu s | ping max ms | ops | wavg ms | wp99 ms | wmax ms | stall s | blocked |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | - | up | 35.2 / 34.6 | 37 / 25 | 3930 / 3850 | 1 | 0.5 | 0 | 33.8 / 25.2 | - | - | - | - | - | 0 |
| 0 | - | hybrid | 36.3 / 34.9 | 24 / 33 | 3850 | 1 | 0.5 | 0 | 25.0 / 30.4 | - | - | - | - | - | 0 |
| 0 | - | valkey-forkless | 35.7 / 35.2 | 0 | 3900 / 3754 | 0 / 1 | 0.75 / 0.73 | 35.6 / 35.1 | 1.2 / 0.9 | - | - | - | - | - | 0 |
| 20k | 1.0 | up | 34.6 / 33.7 | 34 / 30 | 5763 / 5735 | 1915 / 1887 | 8.0 / 7.8 | 0 | 34.2 / 29.4 | 19995 | 0.21 | 0.67 | 35.7 / 30.5 | 0 | 0 |
| 20k | 1.0 | hybrid | 35.8 / 35.5 | 26 / 26 | 5796 / 5793 | 1947 / 1944 | 8.0 / 8.2 | 0 | 24.7 / 23.4 | 19996 | 0.22 | 0.68 | 27.0 / 27.0 | 0 | 0 |
| 20k | 1.0 | valkey-forkless | 35.0 / 34.3 | 0 | 3754 | 1 | 0.9 / 1.1 | 35.0 / 34.2 | 0.9 / 0.8 | 8407 / 8656 (43%) | 1.22 / 1.05 | 0.61 / 0.68 | 33834 / 34059 | 7 / 5 | 7 |
| 50k | 1.0 | up | 35.9 / 35.9 | 27 / 36 | 6691 / 6689 | 2843 / 2841 | 16.2 / 16.3 | 0 | 26.8 / 35.8 | 49982 | 0.12 | 0.31 / 0.32 | 28.2 / 37.4 | 0 | 0 |
| 50k | 1.0 | hybrid | 36.0 / 34.4 | 30 / 27 | 6696 / 6661 | 2847 / 2812 | 16.1 / 15.6 | 0 | 28.9 / 26.0 | 49985 | 0.12 | 0.31 / 0.33 | 31.5 / 28.9 | 0 | 0 |
| 50k | 1.0 | valkey-forkless | 33.3 / 36.5 | 0 | 3754 | 0 / 1 | 1.1 / 1.3 | 33.2 / 36.4 | 1.4 / 1.5 | 22055 / 19430 (41%) | 0.79 / 0.89 | 0.30 / 0.33 | 32993 / 35302 | 10 / 9 | 10 |
| 50k | 0.2 | up | 32.7 / 35.8 | 37 / 33 | 4462 / 4463 | 614 | 13.5 / 14.9 | 0 | 35.5 / 31.9 | 49980 | 0.12 | 0.33 / 0.31 | 38.2 / 34.4 | 0 | 0 |
| 50k | 0.2 | hybrid | 31.4 / 34.7 | 26 / 29 | 4463 | 614 | 13.2 / 14.5 | 0 | 25.4 / 28.3 | 49987 | 0.11 | 0.30 / 0.31 | 27.6 / 30.9 | 0 | 0 |
| 50k | 0.2 | valkey-forkless | 35.7 / 36.1 | 0 | 3754 | 1 / 0 | 3.4 / 2.8 | 35.6 / 36.0 | 0.9 | 24087 / 23089 (47%) | 0.73 / 0.74 | 0.18 / 0.31 | 32494 / 31801 | 5 / 3 | 10 |

### Reading the run

- **Forkless with 1M-field hashes cannot serve writes to them.** A write that lands on the hash under serialization is blocked
  until the whole 90 MB hash is written: 20-35 s maximum write latency in every traffic cell of both datasets, 7-16 clients
  blocked at the peak, 5-11 stalled seconds per save (27-32 with the hot set on mixed1m, where 5 hot hashes take most of the
  writes), and 41-47% of the offered rate delivered on big1m, 75-76% on mixed1m uniform. Its p99 stays under 1.5 ms because
  the blocked writes are few, so a p99 dashboard would not show it; the achieved rate and the max do.
- **On big1m upstream and hybrid are the same save**, as on run 12's big: 1.9 GB and 2.8 GB of copy-on-write at 20k and 50k,
  0.6 GB with the hot set, 25-37 ms stalls, full rate, write max 27-38 ms (the stall). Everything is fork-class, so the hybrid
  adds nothing and costs nothing.
- **On mixed1m the hybrid pays copy-on-write for the hashes only**: 1.25 GB vs 2.5 GB (20k), 1.7 GB vs 3.25 GB (50k),
  0.34 GB vs 1.8 GB (hot set) against upstream, with a 14-18 ms stall against 29-41 ms, the same write latency as upstream and
  the full rate. Peak memory to save 4 GB at 50k: upstream 7.3 GB, hybrid 5.8 GB, forkless 4.0 GB.
- **Save time did not halve on mixed1m as it did on run 12's mixed** (41-42 s vs upstream 40-46 s, idle 41.7 vs 40.6). The
  scan thread finished its 2 GB of small values in about 20 s (`threads cpu s` = 18-20) but the child spent about 40 s
  serializing 24 hashes of 1M fields, one at a time, and while it is inside a hash it does not drain the pipe, so the scan
  thread blocks on the 1 MB pipe buffer instead of running ahead. With 9 KB hashes (run 12 mixed) the child's units of work
  were small and the two overlapped. Fix on our side: let the scan thread queue completed 64 KB blocks in memory (bounded by
  the retained-bytes mark) rather than blocking on the pipe, or give the child a reader thread. Filed as TASK-1.27.
- Forkless's idle save on mixed1m is 25% longer than either fork-based one (50.7 s) and its main thread spends 11-25
  CPU-seconds per save against our 0.6-17.


## Run 14 (2026-09-27, lost): our hybrid vs the teammate's hybrid on the 4 GB datasets

Started 00:12 UTC on the run 12/13 host (i-00fe113633e1d929e) with `run14.sh`: `hybrid` (54b7d4f0c) vs `yinon-hybrid`
(1204d0970, `--bgsave-mode hybrid --forkless-string-max 4096`), datasets small (13.5), mixed, mixed1m, big1m (16), rates
0/20k/50k uniform and 50k on the 20% hot set, two passes of one rep each, interleaved per cell. The instance powered off on its
own shutdown timer at 03:58 UTC and was then no longer available, so `~/run14.csv` was never fetched. The one cell seen in the
log before the connection dropped, small idle: yinon-hybrid save 30.3 s, fork 9.3 ms, extra 4 MB, PING p99 0.58 ms, **PING max
147.9 ms** (ours in run 12: 31.3 s, 1.6 ms, 2 MB, 0.60 ms, 1.8 ms; his PING max at 300 MB in run 10 was 7.5-11.5 ms, so the
parent-side helper handshake appears to scale with the dataset). One cell, no conclusion. Rerun with the recipe above on a new
host; copy the CSV off the host after every dataset.


## Run 15 (2026-09-27): single core, mixed at 4 GB, our hybrid vs upstream vs Valkey forkless

Raw: `run15.csv`, driver `run15.sh`. Fresh c7i.4xlarge (i-003628ddea3294b66, same layout as runs 12-13). The only change from
run 12: `SERVER_CPUS=0`, so the server, its scan thread, its fork child and Valkey's background thread all run on vCPU 0 with
its SMT sibling vCPU 8 idle; load generator on vCPUs 4-7,12-15. Mixed dataset at scale 16 (19.5 M keys, 3.9 GB, 256k hashtable
hashes of 9 KB). Binaries: `up` 0d6266f2d, `hybrid` 9e21ef6c3 (src as 54b7d4f0c), `valkey-forkless` 06d35aca6. 2 reps,
interleaved per cell, every save `ok`, no hard-mark aborts. Values as rep a / rep b.

| rate | hot | config | save s | fork ms | peak MB | extra MB | main cpu s | threads cpu s | child cpu s | ping p99 ms | ping max ms | ops | wavg ms | wp99 ms | wmax ms | stall s | blocked |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | - | up | 38.0 / 41.5 | 41 / 32 | 4064 / 3910 | 4 / 1 | 0.35 | 0 | 37.6 / 41.0 | 1.70 / 1.63 | 37.5 / 33.4 | - | - | - | - | - | 0 |
| 0 | - | hybrid | 42.6 / 41.4 | 17 / 19 | 3919 / 3906 | 2 / 1 | 0.34 | 23.0 / 23.1 | 19.2 / 17.9 | 3.37 / 3.04 | 14.9 / 16.4 | - | - | - | - | - | 0 |
| 0 | - | valkey-forkless | 48.1 / 50.9 | 0 | 3943 / 3830 | 1 | 10.8 / 11.0 | 36.4 / 39.1 | 0 | 3.83 / 3.78 | 5.0 / 4.8 | - | - | - | - | - | 0 |
| 20k | 1.0 | up | 50.3 / 53.2 | 35 / 59 | 6601 / 6640 | 2680 / 2735 | 7.2 / 7.6 | 0 | 43.1 / 45.6 | 3.29 / 3.36 | 31.8 / 58.0 | 19996 / 19990 | 0.23 / 0.24 | 3.04 / 3.15 | 35.8 / 60.4 | 0 | 0 |
| 20k | 1.0 | hybrid | 47.7 / 49.3 | 30 / 32 | 5207 / 5241 | 1306 / 1328 | 6.9 / 7.2 | 21.8 / 22.9 | 19.0 / 19.2 | 4.17 / 3.99 | 28.4 / 31.0 | 19996 | 0.33 | 3.98 / 3.96 | 31.8 / 32.7 | 0 | 0 |
| 20k | 1.0 | valkey-forkless | 56.6 / 59.3 | 0 | 3837 / 3853 | 8 | 16.5 / 16.9 | 37.5 / 39.5 | 0 | 2.41 | 3.5 / 3.7 | 17663 / 17548 (88%) | 0.74 / 0.79 | 11.8 / 12.3 | 24.3 / 24.1 | 32 / 33 | 7 |
| 50k | 1.0 | up | 61.8 / 63.9 | 46 / 64 | 7234 / 7222 | 3309 / 3313 | 19.6 / 20.2 | 0 | 42.5 / 43.9 | 3.72 / 3.68 | 43.1 / 64.1 | 49979 / 49971 | 0.32 / 0.30 | 3.76 / 3.72 | 47.9 / 65.7 | 0 | 0 |
| 50k | 1.0 | hybrid | 65.7 / 60.8 | 27 / 33 | 5702 / 5690 | 1777 / 1769 | 24.3 / 19.8 | 21.2 / 21.0 | 20.5 / 20.0 | 3.41 / 3.55 | 25.2 / 33.2 | 49989 / 49983 | 0.23 | 3.19 / 3.14 | 29.2 / 35.0 | 0 | 0 |
| 50k | 1.0 | valkey-forkless | 64.2 / 62.5 | 0 | 3870 / 3838 | 25 | 22.6 / 22.1 | 40.2 / 39.0 | 0 | 2.87 / 2.92 | 158 / 162 | 44001 / 44261 (88%) | 1.04 / 1.02 | 18.5 / 18.2 | 162 / 165 | 32 / 35 | 12 |
| 50k | 0.2 | up | 63.8 / 61.1 | 66 / 43 | 5980 / 5973 | 2063 / 2060 | 19.6 / 18.9 | 0 | 44.4 / 42.4 | 3.76 / 3.72 | 63.8 / 43.7 | 49968 / 49981 | 0.32 / 0.31 | 3.73 / 3.72 | 67.9 / 45.9 | 0 | 0 |
| 50k | 0.2 | hybrid | 55.3 / 56.6 | 19 / 23 | 4445 / 4425 | 532 | 14.1 / 14.5 | 22.5 / 22.6 | 18.9 / 19.7 | 4.53 / 4.74 | 17.9 / 20.8 | 49995 / 49989 | 0.51 / 0.52 | 4.58 / 4.73 | 20.9 / 25.8 | 0 | 0 |
| 50k | 0.2 | valkey-forkless | 66.2 / 66.2 | 0 | 3844 / 3828 | 15 | 24.9 / 24.9 | 39.7 / 39.7 | 0 | 2.63 / 2.65 | 3.8 / 4.8 | 48664 / 48635 (97%) | 0.66 / 0.67 | 6.91 / 7.17 | 27.8 / 25.1 | 6 | 12 |

### Reading the run

- **On one core the hybrid's parallelism is gone, and so is its save-time win on mixed.** Scan thread (21-23 s CPU) and child
  (18-20 s) take turns on the core, so idle saves take 42 s against upstream's 38-41 (four cores: 18.7 vs 36.9). Under traffic
  the two are within noise of each other (48-49 vs 50-53 s at 20k, 56 vs 61-64 with the hot set, 61-66 vs 62-64 at 50k).
- **Memory and stall are unchanged by the core count**, as they must be: 1.3 GB vs 2.7 GB (20k), 1.8 vs 3.3 GB (50k), 0.5 vs
  2.1 GB (hot) of copy-on-write; stalls 17-33 ms vs 32-66 ms (upstream's 59-66 ms outliers are one-core scheduling: the parent
  waits to be rescheduled after fork). Peak memory to save 3.9 GB at 50k: upstream 7.2 GB, hybrid 5.7, forkless 3.9.
- **Command latency on one core: the hybrid costs about 1 ms at p99 when the main thread is light, and nothing when it is
  busy.** Idle: PING p99 3.0-3.4 ms vs 1.6-1.7 for upstream (two workers instead of one share the core with the main thread).
  20k: write p99 3.96-3.98 vs 3.04-3.15. 50k uniform: the order flips, 3.14-3.19 vs 3.72-3.76, and PING p99 3.4-3.5 vs 3.7;
  upstream's child copying 3.3 GB of pages now takes more of the core than our scan thread. 50k hot: 4.6-4.7 vs 3.7 (our
  child has little to copy and the scan thread's 22 s dominate). Write max follows the fork stall for both: ours 21-35 ms,
  upstream 36-68 ms. Run 8 at 300 MB showed the same shape (3.95 vs 4.67 ms at 50k).
- **Forkless does not survive one core at 4 GB either.** It misses the offered rate at 20k (88%) and 50k uniform (88%), with
  32-35 s of stalled seconds per 57-64 s save, 7-12 clients blocked at the peak, write p99 12-18 ms and a 162-165 ms worst
  write and PING max at 50k; with the hot set it delivers 97% with 6 stalled seconds and a 7 ms write p99. Its main thread
  spends 16.5-25 CPU-seconds per save against 7-24 for ours and upstream's, and its saves are the longest in every cell but
  one. Memory stays at 8-25 MB, as on four cores. Its PING p99 (2.4-2.9 ms) is the lowest under traffic because blocked
  writers do not count against PING; the write columns show where the time went.
- Caveat: one P-core with its SMT sibling idle is a generous "one CPU"; a one-vCPU cgroup on a busy host is tighter. The
  relative picture is what this run tests.


## Run 16 (2026-09-29/30): the scan thread's outbound queue (TASK-1.27), before/after on our own branch, c7i.4xlarge

Raw: `run16.csv`, drivers `run16.sh` (pass 1: `hybrid-before` = 444feb8d4 vs `hybrid` = first queue version, interleaved),
`run16b.sh` (pass 2: `hybrid2` = zero-copy staging), `run16c.sh` (pass 3: `hybrid3` = exact allocation, the committed
version 16fda179b), `run16d.sh` (pass 4: `hybrid-before` vs `hybrid3` interleaved, 3 reps, on the cells where pass 3
differed from before by more than a few percent). Fresh c7i.4xlarge at 54.219.8.216, same layout as runs 12-15 (server on
vCPUs 0-3,8-11, load on 4-7,12-15, dumps and dataset cache on a 24 GB tmpfs). Datasets: mixed1m at scale 16 (run 13's
recipe), small 13.5 and mixed 16 (run 12's). All saves `ok`, no hard-mark aborts. New columns: `used_mem +MB` is the peak
growth of `used_memory` during the save, `queued peak MB` is `snap_last_save_queued_peak` (added in this task).

**What changed between the versions.** Before: the scan thread wrote the pipe with blocking `write()`, so while the child
serialized one 90 MB hash the 1 MB pipe filled and the scan stalled. v1: every message is copied into an owned buffer and
queued; the pipe write end is non-blocking; past `bgsave-hybrid-max-retained-bytes` the thread waits. v2: the staging
buffer is handed to the queue without a copy and drained buffers return to a pool (v1 cost 11% scan CPU on small). v3:
exact allocations, pool for staging-sized buffers only, queue bytes are allocation sizes (v2 let 170-byte KVOBJ messages
pin 160 KB pooled buffers and let greedy sds doubling make each 64 KB buffer a 160 KB allocation: `used_memory` grew by
5-20 GB during mixed saves while RSS grew 0.6 GB; maxmemory would have seen it).

**Cell ordering caveat.** The first cell of every dataset in every pass runs right after a fresh populate (the cache is
deleted per dataset) and is 30-40% slower for every binary (mixed1m idle 32 s vs 24 s from cache, mixed idle 27 vs 19),
with the child's wall time far above its CPU time. Compare from-cache cells; the from-populate cells are the first value
in each pair below where a pass has two reps.

### mixed1m

| rate | config | save s | main cpu s | scan cpu s | child cpu s | extra MB | parent +MB | CoW MB | used_mem +MB | queued peak MB | wp99 ms | wmax ms | ops |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | before | 38.1 / 38.3 | 0.5 / 0.5 | 18.7 / 17.4 | 21.0 / 22.4 | 4 / 1 | 1 / 0 | 4 / 1 | 6 / 5 | - / - | - / - | - / - | - / - |
| 0 | queue v1 (copy) | 27.3 / 21.4 | 0.4 / 0.2 | 18.5 / 19.2 | 24.6 / 20.7 | 301 / 278 | 268 / 225 | 33 / 53 | 324 / 299 | - / - | - / - | - / - | - / - |
| 0 | queue v2 (zero-copy) | 32.6 / 23.8 | 0.5 / 0.3 | 18.6 / 17.2 | 20.3 / 22.2 | 241 / 277 | 233 / 248 | 8 / 29 | 551 / 644 | 220 / 256 | - / - | - / - | - / - |
| 0 | queue v3 (exact alloc) | 32.2 / 24.6 | 0.5 / 0.3 | 18.7 / 18.6 | 20.4 / 23.7 | 250 / 246 | 237 / 231 | 13 / 16 | 262 / 260 | 256 / 256 | - / - | - / - | - / - |
| 20000 | before | 40.9 / 41.9 | 9.3 / 10.2 | 18.2 / 18.6 | 24.0 / 24.6 | 1241 / 1256 | 23 / 23 | 1236 / 1251 | 35 / 36 | - / - | 0.57 / 0.55 | 14.8 / 16.0 | 20000 / 19999 |
| 20000 | queue v1 (copy) | 37.0 / 37.3 | 7.9 / 8.4 | 19.4 / 18.1 | 23.5 / 24.6 | 1176 / 1181 | 283 / 283 | 1173 / 1178 | 325 / 327 | - / - | 0.55 / 0.56 | 16.4 / 15.8 | 19999 / 20000 |
| 20000 | queue v2 (zero-copy) | 34.0 / 35.2 | 8.4 / 8.0 | 18.8 / 17.2 | 20.9 / 23.5 | 1120 / 1150 | 289 / 293 | 1120 / 1143 | 1206 / 1207 | 274 / 277 | 0.56 / 0.56 | 16.0 / 15.1 | 19999 / 20000 |
| 20000 | queue v3 (exact alloc) | 36.0 / 39.0 | 8.3 / 9.0 | 17.0 / 18.3 | 22.4 / 24.2 | 1163 / 1217 | 141 / 161 | 1182 / 1207 | 282 / 283 | 280 / 281 | 0.57 / 0.57 | 15.6 / 15.5 | 19999 / 19999 |

### small

| rate | config | save s | main cpu s | scan cpu s | child cpu s | extra MB | parent +MB | CoW MB | used_mem +MB | queued peak MB | wp99 ms | wmax ms | ops |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | before | 29.3 | 0.4 | 29.1 | 3.0 | 5 | 1 | 4 | 6 | - | - | - | - |
| 0 | queue v1 (copy) | 32.5 | 0.4 | 32.3 | 3.0 | 2 | 1 | 1 | 4 | - | - | - | - |
| 0 | queue v2 (zero-copy) | 31.2 | 0.4 | 31.0 | 3.0 | 1 | 0 | 1 | 6 | 0 | - | - | - |
| 0 | queue v3 (exact alloc) | 29.6 | 0.4 | 29.3 | 2.9 | 4 | 0 | 4 | 6 | 0 | - | - | - |
| 20000 | before | 28.9 / 31.3 / 31.5 / 31.1 | 6.3 / 7.0 / 6.9 / 6.8 | 28.7 / 31.1 / 31.4 / 30.9 | 3.3 / 3.3 / 3.3 / 3.3 | 8 / 17 / 8 / 8 | 7 / 8 / 7 / 7 | 1 / 9 / 1 / 1 | 4 / 6 / 4 / 4 | - / - / - / - | 0.54 / 0.56 / 0.54 / 0.54 | 2.2 / 4.3 / 2.7 / 2.6 | 20000 / 19999 / 20000 / 20000 |
| 20000 | queue v1 (copy) | 30.9 | 6.8 | 30.7 | 3.3 | 8 | 7 | 1 | 4 | - | 0.53 | 2.1 | 19999 |
| 20000 | queue v2 (zero-copy) | 30.2 | 6.3 | 30.0 | 3.3 | 8 | 7 | 1 | 4 | 96 | 0.58 | 2.6 | 20000 |
| 20000 | queue v3 (exact alloc) | 30.9 / 31.1 / 30.5 / 30.1 | 6.8 / 6.7 / 6.8 / 6.7 | 30.7 / 30.9 / 30.3 / 30.0 | 3.1 / 3.1 / 3.1 / 3.0 | 8 / 8 / 8 / 8 | 7 / 7 / 7 / 7 | 1 / 1 / 1 / 1 | 4 / 4 / 4 / 4 | 98 / 99 / 97 / 96 | 0.53 / 0.57 / 0.53 / 0.53 | 2.8 / 2.6 / 2.7 / 2.4 | 19999 / 19999 / 20000 / 20000 |
| 50000 | before | 29.3 | 13.0 | 29.1 | 3.0 | 7 | 6 | 1 | 4 | - | 0.31 | 3.0 | 49999 |
| 50000 | queue v1 (copy) | 29.1 | 9.4 | 29.0 | 3.2 | 7 | 6 | 1 | 4 | - | 0.62 | 3.0 | 49998 |
| 50000 | queue v2 (zero-copy) | 28.3 | 12.5 | 28.1 | 3.1 | 8 | 7 | 1 | 4 | 219 | 0.36 | 4.1 | 49999 |
| 50000 | queue v3 (exact alloc) | 29.6 | 13.2 | 29.4 | 2.9 | 8 | 7 | 1 | 4 | 229 | 0.35 | 3.3 | 49999 |

### mixed

| rate | config | save s | main cpu s | scan cpu s | child cpu s | extra MB | parent +MB | CoW MB | used_mem +MB | queued peak MB | wp99 ms | wmax ms | ops |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | before | 26.8 | 0.4 | 19.6 | 16.1 | 5 | 1 | 4 | 6 | - | - | - | - |
| 0 | queue v1 (copy) | 18.8 | 0.3 | 18.6 | 18.0 | 3 | 1 | 2 | 4 | - | - | - | - |
| 0 | queue v2 (zero-copy) | 27.0 | 0.4 | 20.2 | 16.2 | 694 | 624 | 70 | 20763 | 208 | - | - | - |
| 0 | queue v3 (exact alloc) | 27.0 | 0.4 | 20.1 | 16.1 | 39 | 29 | 10 | 262 | 256 | - | - | - |
| 20000 | before | 21.8 / 21.2 / 23.1 / 23.1 | 6.1 / 5.7 / 5.9 / 6.3 | 18.1 / 18.4 / 20.0 / 19.9 | 18.5 / 16.3 / 18.6 / 18.6 | 814 / 791 / 847 / 847 | 7 / 8 / 6 / 7 | 809 / 784 / 843 / 842 | 4 / 6 / 5 / 4 | - / - / - / - | 0.57 / 0.59 / 0.55 / 0.56 | 18.2 / 17.3 / 18.4 / 18.4 | 20000 / 20000 / 19999 / 20000 |
| 20000 | queue v1 (copy) | 19.8 | 5.5 | 19.6 | 18.2 | 838 | 160 | 757 | 147 | - | 0.56 | 18.4 | 19999 |
| 20000 | queue v2 (zero-copy) | 19.5 | 5.2 | 19.3 | 17.4 | 929 | 403 | 747 | 5235 | 110 | 0.55 | 18.2 | 19999 |
| 20000 | queue v3 (exact alloc) | 21.2 / 21.8 / 22.5 / 20.6 | 5.6 / 5.9 / 6.2 / 5.6 | 18.2 / 19.8 / 20.1 / 18.1 | 18.1 / 17.5 / 18.2 / 17.3 | 800 / 817 / 835 / 783 | 79 / 72 / 74 / 76 | 792 / 810 / 828 / 777 | 261 / 261 / 261 / 261 | 264 / 263 / 264 / 264 | 0.55 / 0.56 / 0.56 / 0.55 | 18.6 / 19.1 / 19.2 / 18.8 | 20000 / 19999 / 19999 / 20000 |
| 50000 | before | 28.5 / 31.9 / 30.9 / 30.9 | 12.8 / 14.5 / 14.1 / 14.2 | 17.1 / 19.2 / 19.3 / 19.3 | 17.9 / 19.2 / 18.1 / 18.3 | 1533 / 1589 / 1571 / 1571 | 17 / 18 / 17 / 17 | 1527 / 1581 / 1565 / 1564 | 15 / 17 / 16 / 15 | - / - / - / - | 0.34 / 0.33 / 0.28 / 0.33 | 19.8 / 19.6 / 19.8 / 23.4 | 49996 / 49996 / 49997 / 49992 |
| 50000 | queue v1 (copy) | 20.2 | 8.5 | 19.2 | 18.1 | 1564 | 352 | 1339 | 289 | - | 0.57 | 19.6 | 49996 |
| 50000 | queue v2 (zero-copy) | 20.4 | 9.7 | 19.9 | 17.8 | 1773 | 620 | 1363 | 7930 | 276 | 0.36 | 20.2 | 49996 |
| 50000 | queue v3 (exact alloc) | 29.9 / 28.3 / 26.9 / 27.9 | 13.6 / 13.0 / 12.4 / 12.7 | 20.2 / 19.7 / 18.0 / 18.8 | 19.1 / 18.2 / 18.1 / 18.6 | 1557 / 1532 / 1505 / 1528 | 97 / 96 / 96 / 98 | 1549 / 1525 / 1497 / 1517 | 262 / 262 / 262 / 262 | 285 / 284 / 283 / 284 | 0.36 / 0.36 / 0.32 / 0.35 | 20.2 / 20.2 / 19.8 / 19.9 | 49995 / 49996 / 49996 / 49996 |

### Pass 4: the noisy cells repeated, before vs v3 interleaved, 3 reps

| dataset | rate | config | save s | main cpu s | scan cpu s | child cpu s | parent +MB | CoW MB |
|---|---|---|---|---|---|---|---|---|
| mixed | 20k | before | 21.3 / 23.1 / 23.1 | 5.7 / 5.9 / 6.3 | 18.4 / 20.0 / 19.9 | 16.3 / 18.6 / 18.6 | 8 / 6 / 7 | 784 / 843 / 842 |
| mixed | 20k | queue v3 | 21.8 / 22.5 / 20.6 | 5.9 / 6.2 / 5.6 | 19.8 / 20.1 / 18.1 | 17.5 / 18.2 / 17.3 | 72 / 74 / 76 | 810 / 828 / 777 |
| mixed | 50k | before | 31.9 / 30.9 / 30.9 | 14.5 / 14.1 / 14.2 | 19.2 / 19.3 / 19.3 | 19.2 / 18.2 / 18.3 | 18 / 17 / 17 | 1581 / 1565 / 1564 |
| mixed | 50k | queue v3 | 28.3 / 26.9 / 27.9 | 13.0 / 12.4 / 12.7 | 19.7 / 18.0 / 18.8 | 18.2 / 18.1 / 18.6 | 96 / 96 / 98 | 1525 / 1497 / 1517 |
| small | 20k | before | 31.3 / 31.5 / 31.1 | 7.0 / 6.9 / 6.8 | 31.1 / 31.4 / 30.9 | 3.3 / 3.3 / 3.3 | 8 / 7 / 7 | 9 / 1 / 1 |
| small | 20k | queue v3 | 31.1 / 30.5 / 30.1 | 6.7 / 6.8 / 6.7 | 30.9 / 30.3 / 30.0 | 3.2 / 3.1 / 3.0 | 7 / 7 / 7 | 1 / 1 / 1 |

### Reading the run

- **mixed1m idle: the save now tracks the child.** Before, 38.1 / 38.3 s = scan (17-19 s CPU) + child (21-22 s CPU) in
  series. With the queue, from-cache cells: v1 21.4 s against a 20.7 s child, v2 23.8 against 22.2, v3 24.6 against 23.7,
  i.e. within 3-7% of the slower half. The from-populate cells (27.3 / 32.6 / 32.2 s) are the caveat above, not the queue.
- **mixed1m at 20k writes/s: a smaller gain, 41-42 s -> 36-39 s (v3), 34-35 (v2).** The child's CPU is unchanged at
  21-24 s but its wall time is not: the queue sits at its bound most of the save (queued peak 280 MB = the 256 MiB bound
  plus the main-thread queue), so the scan thread is throttled to the child's pace, and the child is waiting on something
  for 10-15 s that it does not wait on at idle. Not investigated here; the same signature appears in the from-populate
  cells at idle. A larger bound would show whether the gap closes.
- **The cost is the queue's memory.** Parent RSS grows by the bound while the child is behind: 231-293 MB on mixed1m
  (before: 0-23 MB), 72-98 MB on mixed under writes (before: 6-18), nothing on small (the child is never behind there).
  `used_memory` grows by the same amount once v3's accounting is in place (262 MB on mixed1m against a 256 MB queued peak;
  v2 showed 551-1207 MB there and 5-20 GB on mixed). It is released at the end of the save. Copy-on-write is 3-6% lower
  because the save is shorter.
- **No regression on small and mixed (pass 4, 3 interleaved reps).** mixed 20k 21.3-23.1 vs 20.6-22.5 s, small 20k
  31.1-31.5 vs 30.1-31.1 s, main-thread CPU within 0.3 s; mixed 50k is 10% faster (30.9-31.9 -> 26.9-28.3 s) with 1.5 s
  less main-thread CPU, because the shorter save emits fewer buckets. The single-rep differences of passes 1-3 (small 20k
  +7%, mixed 50k v3 29.9 vs 28.5) were noise: the same cells spread 21.3-23.1 and 30.9-31.9 for the unchanged binary.
- **Write latency is unchanged** (wp99 0.55-0.57 ms at 20k on both, wmax 15-16 ms on mixed1m from the fork stall).

### Pass 5: the queue's own bound, 128 MB vs 256 MB (`run16e.csv`, `run16e.sh`, 41527cab3)

`bgsave-hybrid-max-queued-bytes` (default 128 MB, `hybrid` = ~/redis-after4) against the queue bounded by the 256 MB
retained limit (`hybrid3`), mixed1m, interleaved, 2 reps (first rep of each pair from a fresh populate).

| rate | queue bound | save s | scan cpu s | child cpu s | extra MB | parent +MB | used_mem +MB | CoW MB | wp99 ms |
|---|---|---|---|---|---|---|---|---|---|
| 0 | 256 MB | 32.3 / 25.3 | 18.9 / 17.7 | 20.9 / 24.1 | 247 / 247 | 236 / 230 | 258 / 260 | 11 / 17 | - |
| 0 | 128 MB | 26.8 / 23.8 | 19.1 / 17.7 | 23.7 / 21.2 | 126 / 138 | 113 / 115 | 132 / 132 | 13 / 23 | - |
| 20k | 256 MB | 37.0 / 38.4 | 17.1 / 17.3 | 23.4 / 24.6 | 1179 / 1205 | 162 / 165 | 284 / 285 | 1175 / 1196 | 0.57 / 0.56 |
| 20k | 128 MB | 36.2 / 41.0 | 17.1 / 18.2 | 21.5 / 25.3 | 1159 / 1248 | 83 / 69 | 155 / 160 | 1159 / 1276 | 0.57 / 0.56 |

Halving the bound halves the save's own memory (parent growth 113-115 MB vs 230-236 idle, `used_memory` +132 vs +260) and
costs no save time that the noise can show: idle 23.8-26.8 s vs 25.3-32.3, 20k 36.2-41.0 vs 37.0-38.4. The reasoning that
a full 90 MB hash needs about 150 MB of buffered scan output does not bind here because the child drains between hashes
faster than the scan refills, so a 128 MB queue keeps the two overlapped. 128 MB is the default.

## Run 17 (2026-09-30): mixed1m three-way on one host and day, upstream vs hybrid (with the queue) vs Valkey forkless

Raw: `run17.csv`, driver `run17.sh`. Same c7i.4xlarge and layout as run 16. `up` = 0d6266f2d, `hybrid` = the tree of
41527cab3 (outbound queue, 128 MB queue bound), `valkey-forkless` = 06d35aca6 (`--forkless-infrastructure-enabled yes
--bgsave-default-method forkless`). mixed1m at scale 16, rates 0 / 20k / 50k at HOT=1.0, 2 reps, interleaved per cell,
every save `ok`. Values as rep a / rep b.

| rate | config | save s | fork ms | peak MB | extra MB | parent +MB | CoW MB | main cpu s | threads cpu s | child cpu s | ping max ms | ops | wavg ms | wp99 ms | wmax ms | stall s | blocked |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | up | 43.2 / 43.1 | 41 / 28 | 4231 / 4073 | 3 / 1 | 0 | 3 / 1 | 0.6 | 0 | 43.0 | 40.1 / 26.5 | - | - | - | - | - | 0 |
| 0 | hybrid | 28.7 / 26.3 | 14 / 14 | 4262 / 4248 | 158 / 136 | 126 / 96 | 32 / 40 | 0.4 | 19.0 | 24.2 / 23.6 | 11.2 / 12.1 | - | - | - | - | - | 0 |
| 0 | valkey-forkless | 51.0 / 52.1 | 0 | 4106 / 4008 | 1 | 1 | 0 | 12.3 / 11.5 | 40.7 / 45.0 | 0 | 1.9 / 2.0 | - | - | - | - | - | 0 |
| 20k | up | 43.3 / 40.9 | 29 / 29 | 6574 / 6510 | 2501 / 2437 | 1 | 2502 / 2438 | 10.2 / 9.6 | 0 | 43.2 / 40.8 | 26.2 / 25.3 | 19997 | 0.18 | 0.53 | 29.9 / 30.1 | 0 | 0 |
| 20k | hybrid | 39.9 / 40.7 | 14 / 14 | 5311 / 5326 | 1230 / 1245 | 88 / 88 | 1229 / 1243 | 10.1 / 9.6 | 18.8 / 18.7 | 23.6 / 24.5 | 11.7 / 15.4 | 19999 | 0.18 / 0.19 | 0.56 / 0.57 | 15.7 / 15.6 | 0 | 0 |
| 20k | valkey-forkless | 49.3 / 48.1 | 0 | 4010 / 4011 | 3 / 4 | 3 / 4 | 0 | 14.0 / 14.2 | 46.9 / 45.6 | 0 | 1.9 / 1.8 | 14851 / 15242 (75%) | 0.65 / 0.60 | 1.51 / 1.53 | 23609 / 21166 | 5 / 6 | 9 |
| 50k | up | 43.8 / 44.2 | 31 / 31 | 7331 / 7334 | 3258 / 3261 | 2 | 3256 / 3260 | 17.0 / 14.4 | 0 | 43.7 / 44.1 | 31.1 / 30.0 | 49991 | 0.09 / 0.12 | 0.42 / 0.61 | 31.8 / 31.7 | 0 | 0 |
| 50k | hybrid | 38.3 / 37.5 | 15 / 15 | 5796 / 5796 | 1721 / 1713 | 108 / 97 | 1711 | 15.5 / 15.6 | 16.8 / 18.5 | 23.7 / 21.5 | 16.3 / 16.0 | 49998 | 0.10 / 0.09 | 0.53 / 0.40 | 17.4 / 16.9 | 0 | 0 |
| 50k | valkey-forkless | 47.3 / 46.0 | 0 | 4015 | 8 | 8 | 0 | 19.1 / 18.6 | 45.8 / 44.5 | 0 | 1.7 / 1.6 | 38356 / 38252 (77%) | 0.37 / 0.38 | 1.38 / 1.41 | 22207 / 18471 | 8 / 13 | 16 |

### Reading the run

- **Idle: the hybrid now saves mixed1m in 26-29 s against 43 for stock and 51-52 for forkless.** Run 13 had it at 42, tied
  with stock, because the scan thread waited on the pipe while the child was inside a hash; the queue (TASK-1.27) is the
  difference. The cost is the queue, 96-126 MB of parent growth (bounded at 128 MB plus the main-thread queue), 136-158 MB
  extra in total against 1-3 MB for the others; it is released at the end of the save.
- **Under writes the hybrid keeps its memory advantage and gains 10-15% of save time, not the idle 40%.** 20k: 40-41 s
  against 41-43 (stock) and 48-49 (forkless); 50k: 37.5-38.3 against 43.8-44.2 and 46-47. Copy-on-write is the hashes
  only, 1.23-1.71 GB against stock's 2.44-3.26 GB; the fork stall 14-15 ms against 29-31. The child's CPU is 22-25 s in
  every hybrid cell, so under writes the child's wall time exceeds its CPU by 13-16 s; that gap is what caps the gain and
  is still unexplained (run 16 reading).
- **Forkless on 1M-field hashes still cannot serve writes to them**: 75-77% of the offered rate, worst writes of 18-24 s,
  9-16 clients blocked at the peak, 5-13 stalled seconds per save, with 1.4-1.5 ms p99 that hides it. Its idle save is
  20% longer than stock's and twice the hybrid's, and its main thread spends 11.5-19 CPU-seconds per save against the
  hybrid's 0.4-15.6 (the hybrid's main-thread CPU under writes is the write load itself: stock's is 9.6-17.0).
- **Write latency on the hybrid is stock's**: p99 0.40-0.57 ms against 0.42-0.61 (stock) and 1.4-1.5 (forkless); the
  worst write is the fork stall, 16-17 ms against 30-32.

## Run 18 (2026-09-30): diskless full sync to a fresh replica, upstream vs hybrid vs Valkey, c7i.4xlarge

Raw: `run18.csv`, drivers `run18.sh` (three configurations, interleaved per cell) and `run18b.sh` (Valkey's forkless full
sync from PR 4797, appended afterwards as an experimental fourth configuration; see the addendum). Same host and layout as
runs 16-17. The primary runs on vCPUs 0-3,8-11 with the dataset loaded from the RDB cache; for every cell bench.py starts a
fresh replica of the same binary on the load CPUs (4-7,12-15) with `--repl-diskless-load on-empty-db`, so it loads the
stream directly and writes no file, points it at the primary and measures until the replica reports its link up. `sync s`
is that time; `transfer` (the primary's child finishing) equalled it in every cell. `up` = 0d6266f2d with its default
rdb-channel sync (the child writes the replica's second connection, the main channel streams the backlog, the replica
buffers it: `replica buf MB` is `replica_full_sync_buffer_peak`), `hybrid` = c5183cd4b (the hybrid child on the same
rdb-channel connection), `valkey-fork` = 06d35aca6 with its default single-channel diskless sync (the replication stream
waits in the primary's replica output buffer: `primary COB MB` is `mem_clients_slaves`). A replica that loads disklessly
asks Redis for an uncompressed RDB (`REPLCONF rdb-no-compress 1`), which both Redis binaries honour; Valkey has no such
request and compresses, which is most of its extra time. 2 reps, values as rep a / rep b.
### mixed1m

| rate | config | sync s | fork ms | extra MB | parent +MB | CoW MB | primary COB MB | replica buf MB | main cpu s | scan cpu s | child cpu s | ops | wp99 ms | wmax ms | full syncs |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | stock, rdb-channel | 16.3 / 16.2 | 40 / 28 | 3 / 0 | 0 / 0 | 3 / 0 | 0 / 0 | 1 / 1 | 0.3 / 0.2 | 0.0 / 0.0 | 14.2 / 14.3 | - / - | - / - | - / - | 1 / 1 |
| 0 | stock, one connection | 15.6 / 15.7 | 38 / 27 | 3 / 0 | 0 / 0 | 3 / 0 | 0 / 0 | 0 / 0 | 2.2 / 2.1 | -0.0 / 0.0 | 14.0 / 13.8 | - / - | - / - | - / - | 1 / 1 |
| 0 | hybrid, rdb-channel | 16.6 / 16.4 | 14 / 14 | 123 / 124 | 79 / 107 | 44 / 30 | 0 / 0 | 1 / 1 | 0.2 / 0.2 | 8.9 / 8.9 | 6.4 / 6.3 | - / - | - / - | - / - | 1 / 1 |
| 0 | hybrid, one connection | 16.1 / 15.9 | 14 / 14 | 115 / 124 | 94 / 89 | 21 / 42 | 0 / 0 | 0 / 0 | 1.5 / 1.7 | 8.8 / 8.8 | 6.3 / 6.2 | - / - | - / - | - / - | 1 / 1 |
| 0 | Valkey fork | 34.0 / 39.2 | 41 / 28 | 2 / 1 | 1 / 0 | 1 / 1 | 0 / 0 | 0 / 0 | 4.4 / 4.8 | 0.0 / 0.0 | 34.0 / 39.2 | - / - | - / - | - / - | 1 / 1 |
| 0 | Valkey forkless (PR 4797) | 48.3 / 49.6 | 0 / 0 | 1 / 1 | 1 / 1 | 0 / 0 | 0 / 0 | 0 / 0 | 11.5 / 11.4 | 38.6 / 41.1 | 0.0 / 0.0 | - / - | - / - | - / - | 1 / 1 |
| 20000 | stock, rdb-channel | 17.3 / 16.7 | 29 / 29 | 1436 / 1398 | 2 / 2 | 1435 / 1396 | 0 / 0 | 33 / 32 | 4.3 / 4.2 | 0.0 / -0.0 | 14.4 / 14.4 | 19998 / 19998 | 0.52 / 0.54 | 29.4 / 30.0 | 1 / 1 |
| 20000 | stock, one connection | 16.0 / 16.4 | 30 / 29 | 1382 / 1408 | 31 / 32 | 1354 / 1378 | 29 / 29 | 0 / 0 | 4.6 / 4.8 | 0.0 / 0.0 | 13.9 / 14.0 | 19998 / 19998 | 0.51 / 0.53 | 30.7 / 29.8 | 1 / 1 |
| 20000 | hybrid, rdb-channel | 17.2 / 17.5 | 14 / 15 | 799 / 806 | 129 / 124 | 692 / 713 | 0 / 0 | 33 / 33 | 4.3 / 4.5 | 9.0 / 8.9 | 6.3 / 6.4 | 20000 / 20000 | 0.55 / 0.54 | 15.5 / 15.6 | 1 / 1 |
| 20000 | hybrid, one connection | 16.4 / 16.6 | 14 / 14 | 749 / 768 | 123 / 135 | 683 / 674 | 29 / 30 | 0 / 0 | 4.0 / 4.0 | 9.2 / 9.0 | 6.4 / 6.4 | 20000 / 20000 | 0.52 / 0.54 | 15.1 / 15.6 | 1 / 1 |
| 20000 | Valkey fork | 39.4 / 38.2 | 29 / 29 | 2471 / 2431 | 77 / 74 | 2396 / 2361 | 63 / 61 | 0 / 0 | 8.7 / 8.6 | 0.0 / 0.0 | 39.4 / 38.3 | 19998 / 19998 | 0.56 / 0.53 | 29.6 / 29.5 | 1 / 1 |
| 20000 | Valkey forkless (PR 4797) | 47.0 / 43.9 | 0 / 0 | 13 / 13 | 13 / 13 | 0 / 0 | 0 / 0 | 0 / 0 | 15.9 / 15.1 | 44.4 / 40.5 | 0.0 / 0.0 | 18331 / 18503 | 1.39 / 1.45 | 908.8 / 847.1 | 1 / 1 |

### small

| rate | config | sync s | fork ms | extra MB | parent +MB | CoW MB | primary COB MB | replica buf MB | main cpu s | scan cpu s | child cpu s | ops | wp99 ms | wmax ms | full syncs |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | stock, rdb-channel | 10.1 / 10.1 | 39 / 30 | 3 / 0 | 0 / 0 | 3 / 0 | 0 / 0 | 1 / 1 | 0.2 / 0.2 | 0.0 / 0.0 | 9.2 / 9.3 | - / - | - / - | - / - | 1 / 1 |
| 0 | stock, one connection | 10.1 / 10.1 | 37 / 29 | 3 / 0 | 0 / 0 | 3 / 0 | 0 / 0 | 0 / 0 | 2.0 / 2.0 | 0.0 / 0.0 | 9.4 / 9.3 | - / - | - / - | - / - | 1 / 1 |
| 0 | hybrid, rdb-channel | 10.3 / 10.3 | 2 / 2 | 100 / 115 | 99 / 114 | 1 / 1 | 0 / 0 | 1 / 1 | 0.1 / 0.1 | 9.9 / 9.8 | 1.4 / 1.4 | - / - | - / - | - / - | 1 / 1 |
| 0 | hybrid, one connection | 10.2 / 10.1 | 2 / 2 | 102 / 114 | 101 / 113 | 1 / 1 | 0 / 0 | 0 / 0 | 1.5 / 1.5 | 9.9 / 9.7 | 1.2 / 1.2 | - / - | - / - | - / - | 1 / 1 |
| 0 | Valkey fork | 28.0 / 28.1 | 34 / 27 | 1 / 1 | 0 / 0 | 1 / 1 | 0 / 0 | 0 / 0 | 3.9 / 3.9 | 0.0 / 0.0 | 28.0 / 28.1 | - / - | - / - | - / - | 1 / 1 |
| 0 | Valkey forkless (PR 4797) | 36.3 / 36.8 | 0 / 0 | 2 / 1 | 2 / 1 | 0 / 0 | 0 / 0 | 0 / 0 | 11.4 / 11.3 | 35.7 / 36.1 | 0.0 / 0.0 | - / - | - / - | - / - | 1 / 1 |
| 20000 | stock, rdb-channel | 10.3 / 10.7 | 31 / 31 | 1133 / 1164 | 1 / 1 | 1137 / 1164 | 0 / 0 | 23 / 24 | 2.4 / 2.5 | 0.0 / 0.0 | 9.4 / 9.4 | 19997 / 19997 | 0.48 / 0.47 | 30.8 / 31.3 | 1 / 1 |
| 20000 | stock, one connection | 10.3 / 10.5 | 30 / 30 | 1155 / 1179 | 24 / 24 | 1136 / 1154 | 22 / 22 | 0 / 0 | 3.4 / 3.3 | 0.0 / 0.0 | 9.4 / 9.3 | 19997 / 19997 | 0.46 / 0.47 | 31.3 / 30.5 | 1 / 1 |
| 20000 | hybrid, rdb-channel | 10.6 / 10.4 | 2 / 2 | 120 / 118 | 119 / 117 | 1 / 1 | 0 / 0 | 24 / 24 | 2.3 / 2.2 | 10.1 / 10.1 | 1.4 / 1.4 | 20000 / 20000 | 0.52 / 0.51 | 2.0 / 2.8 | 1 / 1 |
| 20000 | hybrid, one connection | 10.5 / 10.3 | 2 / 2 | 154 / 90 | 153 / 89 | 1 / 1 | 22 / 21 | 0 / 0 | 2.6 / 2.6 | 10.0 / 10.1 | 1.2 / 1.1 | 20000 / 20000 | 0.51 / 0.50 | 2.5 / 2.2 | 1 / 1 |
| 20000 | Valkey fork | 30.6 / 30.3 | 28 / 27 | 2124 / 2112 | 68 / 68 | 2057 / 2046 | 57 / 56 | 0 / 0 | 7.0 / 7.1 | 0.0 / 0.0 | 30.6 / 30.3 | 19997 / 19998 | 0.45 / 0.46 | 29.2 / 27.7 | 1 / 1 |
| 20000 | Valkey forkless (PR 4797) | 45.1 / 45.0 | 0 / 0 | 11 / 11 | 11 / 11 | 0 / 0 | 0 / 0 | 0 / 0 | 16.6 / 16.4 | 40.5 / 40.4 | 0.0 / 0.0 | 20000 / 20000 | 0.97 / 0.97 | 5.2 / 3.4 | 1 / 1 |

### mixed

| rate | config | sync s | fork ms | extra MB | parent +MB | CoW MB | primary COB MB | replica buf MB | main cpu s | scan cpu s | child cpu s | ops | wp99 ms | wmax ms | full syncs |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | stock, rdb-channel | 12.6 / 12.4 | 40 / 32 | 4 / 0 | 0 / 0 | 4 / 0 | 0 / 0 | 1 / 1 | 0.2 / 0.2 | 0.0 / 0.0 | 11.1 / 11.2 | - / - | - / - | - / - | 1 / 1 |
| 0 | stock, one connection | 12.4 / 12.5 | 37 / 33 | 2 / 0 | 0 / 0 | 2 / 0 | 0 / 0 | 0 / 0 | 1.9 / 1.9 | 0.0 / 0.0 | 11.4 / 11.2 | - / - | - / - | - / - | 1 / 1 |
| 0 | hybrid, rdb-channel | 13.0 / 12.9 | 17 / 16 | 41 / 44 | 22 / 43 | 20 / 1 | 0 / 0 | 1 / 1 | 0.2 / 0.2 | 9.8 / 9.7 | 3.2 / 3.2 | - / - | - / - | - / - | 1 / 1 |
| 0 | hybrid, one connection | 12.9 / 12.9 | 16 / 16 | 44 / 44 | 43 / 43 | 1 / 1 | 0 / 0 | 0 / 0 | 1.8 / 1.7 | 9.7 / 9.7 | 3.5 / 3.3 | - / - | - / - | - / - | 1 / 1 |
| 0 | Valkey fork | 28.6 / 30.7 | 40 / 28 | 1 / 1 | 0 / 0 | 1 / 1 | 0 / 0 | 0 / 0 | 3.8 / 4.0 | -0.0 / 0.0 | 28.6 / 30.7 | - / - | - / - | - / - | 1 / 1 |
| 0 | Valkey forkless (PR 4797) | 37.6 / 39.9 | 0 / 0 | 1 / 1 | 1 / 1 | 0 / 0 | 0 / 0 | 0 / 0 | 11.4 / 11.8 | 35.6 / 37.9 | 0.0 / 0.0 | - / - | - / - | - / - | 1 / 1 |
| 20000 | stock, rdb-channel | 13.6 / 13.1 | 33 / 33 | 1264 / 1236 | 1 / 1 | 1268 / 1236 | 0 / 0 | 30 / 29 | 3.5 / 3.4 | 0.0 / -0.0 | 11.4 / 11.3 | 19998 / 19998 | 0.52 / 0.52 | 34.1 / 33.9 | 1 / 1 |
| 20000 | stock, one connection | 12.6 / 12.7 | 33 / 33 | 1228 / 1235 | 29 / 29 | 1199 / 1207 | 27 / 27 | 0 / 0 | 3.9 / 3.8 | -0.0 / -0.0 | 11.3 / 11.2 | 19999 / 19998 | 0.52 / 0.53 | 33.7 / 33.8 | 1 / 1 |
| 20000 | hybrid, rdb-channel | 13.4 / 13.6 | 17 / 17 | 621 / 628 | 61 / 62 | 563 / 569 | 0 / 0 | 30 / 31 | 3.6 / 3.6 | 10.0 / 10.0 | 3.1 / 3.3 | 20000 / 20000 | 0.55 / 0.54 | 17.6 / 18.8 | 1 / 1 |
| 20000 | hybrid, one connection | 13.9 / 13.2 | 17 / 17 | 667 / 638 | 91 / 87 | 580 / 554 | 29 / 28 | 0 / 0 | 3.8 / 3.8 | 9.8 / 9.8 | 3.5 / 3.3 | 20000 / 20000 | 0.53 / 0.53 | 18.2 / 18.5 | 1 / 1 |
| 20000 | Valkey fork | 33.1 / 31.2 | 30 / 30 | 2288 / 2211 | 76 / 71 | 2215 / 2142 | 63 / 59 | 0 / 0 | 7.7 / 7.4 | 0.0 / 0.0 | 33.2 / 31.2 | 19998 / 19998 | 0.52 / 0.53 | 30.9 / 31.0 | 1 / 1 |
| 20000 | Valkey forkless (PR 4797) | 42.9 / 41.4 | 0 / 0 | 13 / 13 | 13 / 13 | 0 / 0 | 0 / 0 | 0 / 0 | 16.8 / 16.2 | 39.4 / 38.1 | 0.0 / 0.0 | 20000 / 20000 | 1.01 / 1.01 | 4.0 / 4.2 | 1 / 1 |

### Reading the run

- **Sync time is the replica's load time, and the hybrid matches stock on it.** With the rdb channel and a diskless load
  the 4 GB replica is up in 10 s (small), 13 s (mixed) and 16-17 s (mixed1m) for both Redis binaries, idle or under 20k
  writes/s; the primary's child is never the bottleneck once it skips compression. Valkey's fork-based sync takes 28-31 s
  (small, mixed) and 34-39 s (mixed1m): compressed RDB, single channel.
- **Primary memory during the sync is where the hybrid differs.** Under 20k writes/s stock's child pays copy-on-write on
  the whole keyspace: 1.13-1.16 GB (small), 1.24-1.27 GB (mixed), 1.40-1.44 GB (mixed1m). The hybrid pays it on the
  fork-class values only: 1 MB on small, 0.56-0.57 GB on mixed, 0.69-0.71 GB on mixed1m, plus its queue (60-130 MB): in
  total 0.12, 0.62 and 0.80 GB. Valkey's fork pays 2.1-2.5 GB and holds 56-77 MB of replication stream in its output
  buffer. At idle the hybrid's queue is its only cost (41-124 MB) where the fork children hold 0-4 MB.
- **The fork stall and the worst write follow the page tables.** Stock stalls 29-40 ms at the fork and its worst write
  during the sync is 30-34 ms; the hybrid stalls 2 ms on small (nothing forkable but the buffers), 14-17 ms on mixed and
  mixed1m, with worst writes of 2-3 ms and 15-19 ms. Write p99 is equal (0.47-0.55 ms) and the offered 20k writes/s are
  delivered by all three.
- **Primary CPU is equal in total and moved off the child.** Main-thread CPU during the sync is the write load
  (2.2-4.5 s, same for stock and hybrid); stock's child spends 9.4 (small), 11.3 (mixed) and 14.4 s (mixed1m); the hybrid
  spends 8.9-10.1 s on the scan thread and 1.4, 3.2 and 6.4 s in the child. Valkey's main thread spends 3.8-8.7 s.
- **The replication stream buffered during a sync is small at these durations**: 23-33 MB on the replica for a 10-17 s
  sync at 20k writes/s (rdb-channel), 56-77 MB in Valkey's output buffer for a 30-39 s one. This is the number TASK-1.29
  (inline replication) has to justify itself against: at these sync times it is a few percent of the dataset.

### Addendum: the single-connection shape (`run18c.sh`, `up-1ch` and `hybrid-1ch`)

The same 12 cells with `repl-rdb-channel no` on the primary: the child (stock or hybrid) writes a pipe, the parent relays
it to the replica's one connection, and the replication stream waits in the primary's replica output buffer instead of on
the replica. Their rows are the `one connection` lines above.

- Sync time, copy-on-write, fork stall and worst write are the same as on the rdb channel for both binaries: the shape of
  the connection does not change what the child does or what the parent's pages cost.
- The stream buffer moves, not shrinks: 22-33 MB in the primary's output buffer (`mem_clients_slaves`) instead of the
  replica's full-sync buffer, and it counts in the parent's growth (the stock rows show +24-32 MB of parent growth under
  writes where the rdb-channel rows show 1-2).
- The relay costs the primary's main thread about 2 CPU-seconds per 4 GB sync (idle: 1.9-2.2 s against 0.2-0.3 with the
  rdb channel, stock and hybrid alike): it copies every RDB byte from the pipe to the socket.
- The hybrid's queue growth is a little higher on one connection (mixed1m 123-135 vs 79-129 MB; small 89-153 vs 99-119)
  because the relay drains the child in bursts; within the 128 MB bound plus one message either way.

### Addendum: Valkey forkless full sync (PR 4797, experimental)

`run18b.sh` ran the PR branch (8dc5df0d5 at the time, `--forkless-infrastructure-enabled yes --bgsave-default-method
forkless` on the primary, the infrastructure flag on the replica) over the same cells afterwards; its rows are the
`Valkey forkless (PR 4797)` lines above. Every sync completed with the replica's dbsize equal to the primary's.

- **It delivers what it promises on memory**: 1-2 MB extra at idle and 11-13 MB under 20k writes/s on every dataset, no
  copy-on-write and no replication buffer anywhere, since the commands are spliced into the stream and applied as the
  replica loads.
- **It pays in time and on the main thread.** 36-50 s per sync against 10-17 s for the two Redis binaries (the same
  3-4x as its BGSAVE, single thread, compressed); main-thread CPU 11.3-16.8 s per sync against 0.2-4.5 for stock and the
  hybrid, ping p99 1.1-1.7 ms against 0.5.
- **And the 1M-field hashes are still the wall**: on mixed1m at 20k writes/s the load delivered 92% of the offered rate,
  the worst write was 0.85-0.91 s, 5 clients were blocked at the peak and 31-34 of the 44-47 s were stalled seconds. On
  small and mixed it kept the rate, with write p99 1.0 ms (stock and hybrid 0.5) and a worst write of 3-5 ms.
- The branch is a day old and under review; treat the numbers as its shape, not its final performance.

## Run 19 (2026-09-30): the scan thread's queue bound, 128 MB vs 16 MB vs 2 MB

Raw: `run19.csv`, driver `run19.sh`. Same host as runs 16-18, later in the day and measurably slower (the 128 MB idle
mixed1m cells took 32-38 s here against 26-29 in run 17 with the same binary; compare within rows only). BGSAVE, hybrid
build c5183cd4b, `bgsave-hybrid-max-queued-bytes` 128 MB (the default until this run) vs 16 MB vs 2 MB, mixed1m and small
at 0 and 20k writes/s, interleaved per cell, 2 reps.

| dataset | rate | queue bound | save s | extra MB | queue (parent +MB) | CoW MB | scan cpu s | child cpu s | wp99 ms | wmax ms |
|---|---|---|---|---|---|---|---|---|---|---|
| mixed1m | 0 | 128 MB | 38.5 / 32.5 | 126 / 135 | 98 / 110 | 28 / 25 | 22.2 / 20.9 | 25.5 / 26.7 | - | - |
| mixed1m | 0 | 16 MB | 44.3 / 40.4 | 17 / 17 | 9 / 7 | 8 / 10 | 24.2 / 20.9 | 28.8 / 26.2 | - | - |
| mixed1m | 0 | 2 MB | 47.7 / 40.6 | 4 / 3 | 1 / 0 | 3 / 3 | 23.8 / 17.8 | 26.5 / 25.0 | - | - |
| mixed1m | 20000 | 128 MB | 37.7 / 40.6 | 1193 / 1241 | 91 / 79 | 1184 / 1248 | 18.3 / 18.6 | 21.9 / 24.5 | 0.56 / 0.55 | 15.6 / 16.1 |
| mixed1m | 20000 | 16 MB | 40.7 / 39.6 | 1243 / 1224 | 27 / 29 | 1241 / 1219 | 18.6 / 16.6 | 23.5 / 24.3 | 0.56 / 0.56 | 15.1 / 15.8 |
| mixed1m | 20000 | 2 MB | 42.0 / 38.1 | 1265 / 1197 | 24 / 21 | 1258 / 1190 | 18.0 / 18.4 | 25.2 / 21.0 | 0.56 / 0.54 | 15.7 / 15.6 |
| small | 0 | 128 MB | 30.3 / 31.2 | 6 / 2 | 0 / 1 | 6 / 1 | 30.1 / 31.0 | 2.9 / 2.8 | - | - |
| small | 0 | 16 MB | 31.0 / 31.8 | 2 / 2 | 1 / 1 | 1 / 1 | 30.8 / 31.6 | 2.9 / 2.9 | - | - |
| small | 0 | 2 MB | 30.7 / 31.2 | 2 / 2 | 1 / 1 | 1 / 1 | 30.5 / 31.0 | 2.9 / 2.8 | - | - |
| small | 20000 | 128 MB | 30.1 / 29.6 | 8 / 8 | 7 / 7 | 1 / 1 | 29.8 / 29.5 | 3.1 / 3.1 | 0.51 / 0.54 | 2.3 / 2.5 |
| small | 20000 | 16 MB | 29.5 / 30.9 | 8 / 8 | 7 / 7 | 1 / 1 | 29.4 / 30.6 | 3.1 / 3.1 | 0.52 / 0.49 | 2.6 / 2.6 |
| small | 20000 | 2 MB | 30.3 / 30.5 | 8 / 8 | 7 / 7 | 1 / 1 | 30.1 / 30.4 | 3.1 / 3.0 | 0.52 / 0.52 | 2.4 / 2.2 |

### Reading the run

- **The queue's only measurable benefit is the idle save on datasets with huge fork-class values.** mixed1m idle: 32-38 s
  at 128 MB, 40-44 at 16 MB, 41-48 at 2 MB. The 128 MB queue lets the scan thread run for the whole of one 90 MB hash
  while the child is inside it; 16 MB covers a sixth of it and keeps about half of the gain; 2 MB is the pipe plus a
  little and gives the sum of the two halves back.
- **Under writes the bound does not matter** (37.7-42.0 s for all three, within the run's noise): the child's wall time
  is the limit there, as in runs 16-17.
- **On small the queue never fills in a BGSAVE** (0-7 MB at any bound), so the bound changes nothing. It does fill during
  a full sync (run 18: 90-150 MB, the child drains more slowly into a socket than into a file) while the sync time is the
  replica's load either way, so there the bound is pure memory.
- **Decision (the user's): the default is 16 MB** (config.c, redis.conf, spec section 6, blog). It costs about a tenth of
  the memory of 128 MB where the queue fills, keeps roughly half of the idle mixed1m gain, and the knob remains for
  datasets of hundred-megabyte values. The run 17 and run 18 tables were measured at 128 MB and say so.


## Run 20 (2026-10-01): A/B of the side-table drain (TASK-1.21.3), upstream vs before vs after, c7i.4xlarge

Fresh c7i.4xlarge (18.144.225.10), `run-interleaved.sh run20.csv 0,20000,50000 small,mixed1m 1.0 2`,
`SERVER_CPUS="0-3,8-11" LOAD_CPUS="4-7,12-15"`, HOT=1.0, scale 1.0 (about 300 MB datasets, so the saves are
2 s cells: this is a regression check of one commit, not a capacity run). Binaries: `up` = upstream
0d6266f2d, `hybrid-before` = d42921437 (TASK-1.25, before the drain), `hybrid` = 817c96857 (the drain,
overlay in the dict metadata, reads skip an empty side table). Two reps per cell, values `rep1/rep2`.

| dataset | rate | config | save s | extra MB | parent growth MB | CoW MB | main CPU s | thread CPU s | ping max ms |
|---|---|---|---|---|---|---|---|---|---|
| mixed1m | 0 | up | 2.20/2.12 | 2/1 | 0/0 | 2/1 | 0.05/0.03 | 0 | 0.69/0.97 |
| mixed1m | 0 | hybrid-before | 1.91/1.93 | 18/17 | 4/1 | 14/16 | 0.04/0.04 | 1.09/1.09 | 0.58/0.68 |
| mixed1m | 0 | hybrid | 1.83/1.83 | 17/17 | 1/1 | 16/16 | 0.04/0.04 | 1.03/1.05 | 0.82/0.71 |
| mixed1m | 20000 | up | 2.22/2.22 | 131/130 | 1/0 | 131/130 | 0.61/0.62 | 0 | 0.74/2.50 |
| mixed1m | 20000 | hybrid-before | 1.92/2.01 | 61/64 | 9/11 | 54/53 | 0.59/0.61 | 0.97/1.06 | 0.64/1.27 |
| mixed1m | 20000 | hybrid | 1.93/1.93 | 62/62 | 8/8 | 57/57 | 0.58/0.58 | 1.07/1.04 | 2.83/3.36 |
| mixed1m | 50000 | up | 2.22/2.32 | 168/169 | 0/0 | 168/169 | 0.85/0.87 | 0 | 2.42/3.46 |
| mixed1m | 50000 | hybrid-before | 2.00/1.90 | 83/82 | 5/9 | 81/77 | 0.78/0.81 | 1.12/1.07 | 3.25/1.50 |
| mixed1m | 50000 | hybrid | 2.02/1.92 | 82/83 | 9/9 | 78/77 | 0.85/0.80 | 1.06/1.08 | 1.53/0.70 |
| small | 0 | up | 2.57/2.51 | 2/1 | 0/0 | 2/1 | 0.06/0.05 | 0 | 5.14/0.85 |
| small | 0 | hybrid-before | 2.32/2.22 | 1/1 | 0/0 | 1/1 | 0.03/0.03 | 2.24/2.16 | 0.72/0.65 |
| small | 0 | hybrid | 2.22/2.22 | 1/1 | 0/0 | 1/1 | 0.03/0.04 | 2.15/2.17 | 0.97/0.75 |
| small | 20000 | up | 2.52/2.52 | 171/171 | 0/0 | 173/173 | 0.64/0.65 | 0 | 0.60/0.69 |
| small | 20000 | hybrid-before | 2.13/2.23 | 3/3 | 2/2 | 1/1 | 0.58/0.58 | 2.01/2.10 | 0.68/0.72 |
| small | 20000 | hybrid | 2.12/2.13 | 3/3 | 2/2 | 1/1 | 0.58/0.57 | 2.04/2.06 | 0.72/0.82 |
| small | 50000 | up | 2.50/2.50 | 239/241 | 0/0 | 241/242 | 1.30/1.31 | 0 | 4.38/3.94 |
| small | 50000 | hybrid-before | 2.02/2.12 | 3/3 | 2/2 | 1/1 | 1.15/1.22 | 1.95/2.01 | 0.58/1.05 |
| small | 50000 | hybrid | 2.20/2.12 | 3/3 | 2/2 | 1/1 | 1.24/1.20 | 2.11/2.01 | 0.63/0.83 |

- **The drain is performance-neutral**: before and after agree within the rep-to-rep spread on save time,
  memory, main-thread and thread CPU at every cell. Worst PING is noise at this scale (single 2-3 ms
  outliers on either side).
- Same picture as runs 6-8 for hybrid versus stock at this size: 3 MB of extra memory against 170-240 MB
  on small under writes, 60-80 MB against 130-170 MB on mixed1m.

## Run 21 (2026-10-01): mixed1m at 4 GB with the 16 MB queue default, BGSAVE and full sync, upstream vs hybrid, c7i.4xlarge

Raw: `run21.csv` (BGSAVE) and `run21s.csv` (full sync), driver `~/run21.sh` on the host (recipe: run 17 for part A,
run 18 for part B). Same instance as run 20 (18.144.225.10). `up` = 0d6266f2d, `hybrid` = e7062762b (TASK-1.21.3 drain,
TASK-1.21.6 stage A, `bgsave-hybrid-max-queued-bytes` at its 16 MB default). mixed1m at scale 16 (19.2M keys, 4.0 GB),
HOT=1.0, 2 reps interleaved per cell, every save and sync `ok`. Runs 17 and 18 measured the same cells with the queue
bound at 128 MB; this run replaces their footnote. Values as rep a / rep b.

### Part A: BGSAVE, rates 0 / 20k / 50k writes/s

| rate | config | save s | fork ms | extra MB | parent +MB | CoW MB | main cpu s | threads cpu s | child cpu s | ping max ms | wmax ms | queued peak MB |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | up | 43.5 / 39.6 | 39 / 28 | 4 / 1 | 0 | 4 / 1 | 0.6 | 0 | 43.4 / 39.5 | 39.2 / 26.0 | - | - |
| 0 | hybrid | 36.7 / 37.0 | 14 / 14 | 17 / 17 | 8 / 9 | 9 / 8 | 0.5 / 0.6 | 19.2 / 18.8 | 23.9 / 24.1 | 12.0 / 13.7 | - | 16 / 16 |
| 20k | up | 46.2 / 45.6 | 29 / 29 | 2579 / 2559 | 1 | 2578 / 2560 | 10.6 / 10.5 | 0 | 46.0 / 45.5 | 26.1 / 29.2 | 29.7 / 30.7 | - |
| 20k | hybrid | 37.5 / 43.3 | 14 / 15 | 1184 / 1283 | 26 / 28 | 1183 / 1282 | 8.6 / 9.4 | 17.1 / 19.6 | 21.7 / 25.2 | 11.0 / 13.8 | 15.5 / 16.1 | 43 / 48 |
| 50k | up | 44.1 / 39.7 | 30 / 31 | 3263 / 3215 | 3 / 2 | 3261 / 3213 | 17.8 / 16.1 | 0 | 44.0 / 39.6 | 27.0 / 29.5 | 32.2 / 32.5 | - |
| 50k | hybrid | 41.3 / 38.4 | 15 / 15 | 1745 / 1721 | 51 / 51 | 1744 / 1714 | 17.3 / 16.1 | 18.7 / 16.9 | 23.9 / 22.9 | 12.7 / 13.1 | 18.2 / 17.8 | 90 / 87 |

### Part B: diskless full sync to a fresh replica (rdb-channel yes, replica `--repl-diskless-load on-empty-db`), rates 0 / 20k

| rate | config | sync s | extra MB | parent +MB | CoW MB | main cpu s | threads cpu s | child cpu s | wmax ms |
|---|---|---|---|---|---|---|---|---|---|
| 0 | up | 16.6 / 16.4 | 3 / 0 | 0 | 3 / 0 | 0.3 / 0.2 | 0 | 14.2 / 14.3 | - |
| 0 | hybrid | 16.5 / 16.6 | 16 / 16 | 7 / 13 | 9 / 3 | 0.2 | 9.0 / 9.0 | 6.4 / 6.4 | - |
| 20k | up | 17.6 / 18.4 | 1451 / 1499 | 1 | 1455 / 1499 | 4.1 / 4.4 | 0 | 14.5 / 14.5 | 29.3 / 29.6 |
| 20k | hybrid | 17.6 / 17.2 | 724 / 711 | 26 / 24 | 705 / 693 | 4.2 / 4.1 | 9.0 / 9.0 | 6.8 / 6.7 | 16.1 / 15.0 |

### Reading the run

- **Idle BGSAVE: 36.7-37.0 s against 39.6-43.5 for stock**, with 17 MB of extra memory against the 136-158 MB the 128 MB
  bound cost in run 17 (where the save was 26-29 s). This is the trade the 16 MB default makes: about a third of the idle
  gain for a tenth of the memory, as run 19 predicted (40-44 s there; this host and day land at 37).
- **Under writes the hybrid keeps its memory advantage and 5-15% of the save time**: 20k 37.5-43.3 s against 45.6-46.2,
  50k 38.4-41.3 against 39.7-44.1; copy-on-write 1.18-1.75 GB against 2.56-3.26 GB; parent growth 26-51 MB. The queued
  peak passes the 16 MB scan-thread bound (43-90 MB) because the main-thread queue of emitted buckets is bounded only by
  the hard mark; still a fifth of stock's child copy-on-write.
- **Worst write latency is the fork stall**: 15-18 ms against 30-33 for stock; worst PING 11-14 against 26-39.
- **Full sync: the sync time is the replica's load either way** (16.5-18.4 s both). The hybrid child spends 6.4-6.8 CPU-s
  against 14.2-14.5 (the scan thread does 9.0 s of the work) and under 20k writes/s the primary pays 0.71-0.72 GB
  against 1.45-1.50 GB.
- The child wall-time gap under writes (child CPU 22-25 s inside a 37-43 s save) is unchanged from run 17 and still the
  open observation.

## Run 22 (2026-10-01): probing the child wall-time gap, mixed1m at 4 GB, idle and 20k writes/s

Raw: `run22-probe-samples.txt` (one line per 0.5 s: child process state, utime+stime, schedstat run/wait, wchan;
scan thread state, schedstat, wchan; INFO queue, keys and bytes counters), driver `~/probe.sh` + `~/sampler.sh` on the
host, same c7i.4xlarge and binaries as run 21, hybrid only, `run.sh ~/probe.csv 0,20000 mixed1m 16 1`.

| cell | wall s | child cpu s | child run-queue wait | scan thread cpu s | child sleeping (pipe_read) | scan sleeping (poll at the 16 MB bound) |
|---|---|---|---|---|---|---|
| idle (save 0) | 35.4 | 20.9 | 0.00 s | 19.5 | 23 of 70 samples | 33 of 70 samples |
| idle (save 1) | 36.4 | 21.5 | 0.00 s | 19.5 | 21 of 72 | 31 of 72 |
| 20k writes/s | 42.1 | 23.9 | 0.00 s | 19.5 | 33 of 83 | 44 of 83 |

Timeline, idle: t = 0-14 s the child sleeps in `pipe_read` about half the time while the scan thread serializes small
values at about 1M keys/s (14M of 19.2M keys, 820 MB sent); the first big hash reaches the child at t = 15 s (keys_fork
3) because the 24 hashes sit where scan order puts them; t = 15-35 s the child serializes the hashes (keys_fork 3 -> 24,
about 0.85 s each) and the scan thread sleeps in poll with the queue pinned at 16 MB, finishing its last 5M keys only as
the child drains between hashes. Wall = scan-bound phase (14 s) + child-bound phase (21 s).

Timeline, 20k writes/s: t = 0-25 s the child is running flat out and the scan thread is parked after 195,471 keys
(bytes sent stuck at 12 MB, queue 11 -> 49 MB: the scan queue at its 16 MB bound plus the main-thread queue of emitted
buckets, which is not bounded). The child's work is the 24 hashes, delivered at once by the main thread: the first HSET
on each hash during the save emits it ahead (a KVOBJ header copy), so the child gets all the big values up front. From
t = 25 s the hashes are done, the child sleeps in `pipe_read` and the scan thread serializes the remaining 19M keys in
17 s. Wall = child-bound phase (25 s) + scan-bound phase (17 s). Neither side ever waits on the run queue; neither side
page-faults (child minor faults 5-167 per save).

### Reading the run

- **The gap is serialization of the two halves, not a stall inside either.** The scan thread needs 19.5 s and the child
  21-24 s; a full overlap would give a 24 s save against 35-42 today and 40-46 for stock. The 16 MB queue is what
  prevents it: the child cannot read the pipe while it is inside a 90 MB hash, so the scan thread blocks after 16 MB.
  Run 17's 26-29 s at the 128 MB bound was the partial overlap that bound bought; run 19 measured the same curve.
- **Under writes the main thread hands the child every big value at the start** (the first write to each hash emits it),
  so the child-bound phase comes first and the scan-bound phase after; the total is the same sum.
- **A bigger queue is the wrong fix**: holding 1 GB of scan output in anonymous parent memory to hide 21 s of hashing.
  Two designs give the overlap without that memory: (1) the scan thread spools its output to a file instead of the pipe
  (a spool in the save's directory; page cache, reclaimable, no parent memory, no bound, no poll), and the child copies
  spool -> RDB file or socket between big values and at the end, which also lets the child serialize the big values
  first; (2) a per-db index of SNAP_FORK values maintained at placement time, so the child can start on the big values
  at the fork instant instead of waiting for scan order. (1) alone gives the overlap; (2) additionally removes the
  dependence on scan order and makes the child's part start at t = 0 in the idle case. Proposed as a task.
- **Correction (2026-10-01, after run 29).** "The 24 hashes sit where scan order puts them" undersold what the probe
  showed: the first hash at 12.5-12.8M of 19.2M keys on both saves is not where 24 uniformly hashed keys land. The
  probe's instance was populated live (no cache yet) and its main dict was still mid-rehash from 2^24 to 2^25 when the
  saves ran: the walker visits table 0 from `rehashidx` up and only then table 1 (snapshot.c, the window loop), and the
  24 hashes, inserted last, were all in table 1, behind the 2/3 of the keys still in table 0. A dict loaded from the RDB
  cache (DEBUG RELOAD, RESIZEDB hint, no rehash) spreads them evenly: measured locally with 1M strings + 24 hashes and
  `rdb-key-save-delay`, the first hash is met at 1-8% of the scan, the 24th at 92-99%, after a live populate, after
  DEBUG RELOAD and in a fresh process alike. Consequence: with the pipe the order never mattered (the scan parks while
  the child is inside a hash, so wall = scan + child either way), but with the spool the idle gain depends on the child
  getting big values early. Runs 23-29 loaded from the cache, so the hashes came at a steady rate and the child was
  busy from the start (run 29 idle: child CPU 24.9 s of a 25.5 s save). On the probe's layout the spool alone would
  have given about 13 + 21 = 34 s, no better than the pipe. Design (2), the per-db index of SNAP_FORK values handed to
  the child at the fork instant, is what makes the idle case independent of scan order; under writes the main thread's
  emit-ahead already does this.

## Run 23 (2026-10-01): spool file instead of the pipe (TASK-1.30, first version), mixed1m at 4 GB and small, c7i.4xlarge

Raw: `run23.csv`, driver `~/run23.sh` on the host. Same instance and layout as runs 20-22. One binary, the spool
commit c85968b09 as first built (every BUFFER message to the spool; the scan thread's close of the spool fixed in the
same commit but after this run), `hybrid-nospool` = `--bgsave-hybrid-spool no` (pipe + 16 MB queue, run 21's shape),
`hybrid` = spool on. mixed1m scale 16, rates 0 / 20k / 50k, 2 reps interleaved; small scale 13.5, rates 0 / 20k, 1 rep.
Values as rep a / rep b.

| dataset | rate | config | save s | extra MB | parent +MB | CoW MB | main cpu s | threads cpu s | child cpu s | ping max ms | wmax ms | spooled MB |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| mixed1m | 0 | nospool | 35.8 / 36.9 | 20 / 18 | 8 / 8 | 12 / 10 | 0.5 | 18.5 / 19.3 | 22.0 / 24.5 | 17.6 / 12.0 | - | 0 |
| mixed1m | 0 | spool | 25.8 / 24.9 | 2 / 2 | 1 / 1 | 1 / 1 | 0.4 | 20.3 / 19.2 | 24.1 / 24.1 | 56.1 / 57.2 | - | 1071 |
| mixed1m | 20k | nospool | 43.3 / 43.1 | 1283 / 1282 | 31 / 30 | 1281 / 1277 | 10.2 / 9.4 | 19.0 / 19.2 | 25.7 / 25.3 | 15.3 / 14.8 | 16.2 / 15.4 | 0 |
| mixed1m | 20k | spool | 25.1 / 24.6 | 915 / 906 | 7 / 7 | 913 / 901 | 6.3 / 5.8 | 20.3 / 18.0 | 24.9 / 24.5 | 55.2 / 58.1 | 57.2 / 61.2 | 1072 |
| mixed1m | 50k | nospool | 39.8 / 41.4 | 1733 / 1749 | 51 / 53 | 1729 / 1744 | 16.9 / 14.3 | 17.2 / 18.1 | 24.0 / 24.7 | 14.3 / 20.5 | 17.2 / 22.5 | 0 |
| mixed1m | 50k | spool | 25.1 / 23.5 | 1510 / 1470 | 7 / 7 | 1509 / 1470 | 11.1 / 10.4 | 20.4 / 19.9 | 25.0 / 23.4 | 54.5 / 56.0 | 58.0 / 57.7 | 1072 |
| small | 0 | nospool | 30.1 | 6 | 1 | 5 | 0.4 | 29.9 | 2.9 | 5.0 | - | 0 |
| small | 0 | spool | 33.6 | 2 | 1 | 1 | 0.6 | 33.2 | 2.1 | 154.8 | - | 2987 |
| small | 20k | nospool | 31.5 | 8 | 7 | 1 | 6.6 | 31.2 | 3.2 | 0.8 | 2.7 | 0 |
| small | 20k | spool | 30.1 | 8 | 7 | 1 | 6.5 | 29.8 | 2.2 | 153.3 | 153.8 | 2988 |

### Reading the run

- **mixed1m saves in 24-26 s at every write rate**, where the pipe shape takes 36-43 and stock 40-46 (run 21): the two
  halves overlap as run 22 predicted (max of scan 19-20 s and child 24-25 s). The shorter save also cuts the child's
  copy-on-write under writes (0.90-0.91 GB against 1.28 at 20k, 1.47-1.51 against 1.73-1.75 at 50k) and the main
  thread's CPU (6 s against 10 at 20k: fewer emissions ahead of a faster walker). Parent growth 7 MB against 30-53: the
  queue holds only big-value headers.
- **Two costs of spooling everything, both fixed in the follow-up commit:** (1) worst PING 55-58 ms on mixed1m and
  153-155 ms on small, which is the main thread's close of the unlinked spool freeing its 1-3 GB of pages at save end
  (now the scan thread closes its end and the child's exit frees the pages); (2) on small, where the child keeps up, the
  spool is pure overhead: 3.0 GB written and read for nothing, +3.3 s of scan-thread CPU idle (33.2 against 29.9).
  The user's refinement measured as run 24: the queue stays the primary path and the spool takes only the overflow once
  the queue is at its bound, with the scan thread returning to the queue when the child has drained it to half.
- The benchmark's `dir` is tmpfs, so here the spool's pages are RAM, not reclaimable page cache; they are outside
  `used_memory` and RSS (extra memory 2 MB) and are freed at save end. On a disk-backed `dir` they are page cache.

## Run 24 (2026-10-01): queue first, spool as the overflow (TASK-1.30 final), mixed1m at 4 GB and small, c7i.4xlarge

Raw: `run24.csv`, driver `~/run24.sh`. Same instance and layout as runs 20-23. Binary bc33196f5: `hybrid-nospool` =
`--bgsave-hybrid-spool no` (pipe + 16 MB queue, the scan thread waits at the bound), `hybrid` = the default: the queue
fills first, BUFFER messages overflow to the unlinked spool once the queue is at its 16 MB bound, the scan thread
returns to the queue when the child has drained it to half; the scan thread closes its spool end, the child's exit
frees the pages. mixed1m scale 16, rates 0 / 20k / 50k, 2 reps interleaved; small scale 13.5, rates 0 / 20k, 1 rep.

| dataset | rate | config | save s | extra MB | parent +MB | CoW MB | main cpu s | threads cpu s | child cpu s | ping max ms | wmax ms | spooled MB | queued peak MB |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| mixed1m | 0 | nospool | 34.8 / 37.0 | 20 / 17 | 8 / 5 | 12 / 12 | 0.5 | 19.3 / 19.4 | 21.3 / 24.2 | 17.3 / 13.9 | - | 0 | 16 |
| mixed1m | 0 | overflow spool | 25.4 / 26.1 | 18 / 17 | 1 / 2 | 17 / 15 | 0.4 | 19.1 / 19.1 | 24.5 / 24.6 | 14.3 / 11.5 | - | 951 / 803 | 16 |
| mixed1m | 20k | nospool | 40.5 / 40.4 | 1239 / 1238 | 29 / 26 | 1236 / 1238 | 9.4 / 9.3 | 18.3 / 18.8 | 23.6 / 23.0 | 12.1 / 11.4 | 15.5 / 15.5 | 0 | 45 / 44 |
| mixed1m | 20k | overflow spool | 24.4 / 21.6 | 909 / 835 | 10 / 11 | 902 / 826 | 6.2 / 5.3 | 19.9 / 19.4 | 24.4 / 21.5 | 13.0 / 13.2 | 16.6 / 15.7 | 1063 | 23 |
| mixed1m | 50k | nospool | 40.3 / 42.3 | 1736 / 1752 | 52 / 56 | 1730 / 1745 | 16.8 / 17.6 | 18.2 / 18.2 | 23.4 / 25.4 | 13.0 / 12.3 | 16.9 / 17.0 | 0 | 88 / 95 |
| mixed1m | 50k | overflow spool | 21.4 / 21.2 | 1423 / 1414 | 5 / 11 | 1420 / 1405 | 9.7 / 9.6 | 18.2 / 19.6 | 21.4 / 21.0 | 14.8 / 13.7 | 17.4 / 18.2 | 1064 | 31 / 33 |
| small | 0 | nospool | 29.9 | 6 | 0 | 6 | 0.4 | 29.7 | 3.0 | 4.2 | - | 0 | 0 |
| small | 0 | overflow spool | 31.5 | 2 | 1 | 1 | 0.4 | 31.3 | 3.1 | 1.8 | - | 0 | 0 |
| small | 20k | nospool | 30.7 | 8 | 7 | 1 | 6.3 | 30.5 | 3.1 | 1.0 | 2.3 | 0 | 98 |
| small | 20k | overflow spool | 31.3 | 8 | 7 | 1 | 6.6 | 31.2 | 3.3 | 1.5 | 2.7 | 0 | 100 |

### Reading the run

- **mixed1m: 21-26 s at every write rate**, against 35-42 for the pipe shape and 40-46 for stock (run 21): idle 25-26
  (-30%), 20k 22-24 (-45%), 50k 21 (-49%). Under writes the save is now shorter than idle because the main thread hands
  the child every big value at the first write to it (run 22), so the child starts on them at t = 0 and the spool
  absorbs the scan's output meanwhile. Copy-on-write follows the save time down (0.83-0.91 GB against 1.24 at 20k,
  1.41-1.42 against 1.73-1.75 at 50k), and so does the main thread's CPU (5-6 s against 9 at 20k, 10 against 17 at 50k:
  fewer buckets to emit ahead of a faster walker).
- **Memory: the queue holds at most its 16 MB bound plus big-value headers** (queued peak 16 / 23 / 31-33 MB against
  16 / 44-45 / 88-95 for the pipe shape, whose main-thread queue grows while the scan thread is parked); parent growth
  1-11 MB. The 0.8-1.1 GB that overflowed sits in the spool (page cache on disk, RAM on this tmpfs benchmark dir),
  outside `used_memory` and RSS, freed when the child exits.
- **Latency: back to the fork stall.** Worst PING 11.5-14.8 ms, worst write 15.7-18.2 ms, the same as the pipe shape;
  run 23's 55-155 ms close spike is gone.
- **small never touches the file** (0 MB spooled: the child keeps up and the queue never reaches its bound) and stays
  within one rep's noise of the pipe shape (31.5 against 29.9 idle, 31.3 against 30.7 at 20k, single reps on a dataset
  whose cells spread 29-33 s in earlier runs).
- The remaining gap to max(scan, child) is small: 21-26 s against 19-20 s of scan CPU and 21-25 s of child CPU.
- Idle depends on the big values arriving early in scan order (see the run 22 correction): here the dataset comes
  from the RDB cache and the 24 hashes are spread evenly, so the child is busy from the first seconds.

## Run 25 (2026-10-01): full sync through the overflow spool, mixed1m at 4 GB, c7i.4xlarge

Raw: `run25.csv`, driver `~/run25.sh`. Run 18/21 recipe (`SYNC=1`, rdb-channel yes, replica
`--repl-diskless-load on-empty-db`), binary bc33196f5, 1 rep per cell.

| rate | config | sync s | extra MB | parent +MB | CoW MB | main cpu s | threads cpu s | child cpu s | ping max ms | wmax ms | spooled MB | queued peak MB |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | nospool | 16.2 | 19 | 9 | 10 | 0.24 | 8.8 | 6.6 | 15.5 | - | 0 | 16 |
| 0 | overflow spool | 16.2 | 16 | 0 | 16 | 0.23 | 8.7 | 6.3 | 12.7 | - | 797 | 15 |
| 20k | nospool | 17.0 | 708 | 24 | 691 | 4.0 | 8.9 | 6.5 | 11.7 | 15.4 | 0 | 25 |
| 20k | overflow spool | 16.9 | 692 | 19 | 688 | 3.8 | 9.0 | 6.4 | 14.2 | 15.5 | 916 | 18 |

- **A full sync is unchanged**: 16.2 s idle and 16.9-17.0 s at 20k writes/s either way, the replica's load time as
  in runs 18 and 21 (the uncompressed stream a diskless-loading replica asks for makes the child three times faster
  than the same binary's BGSAVE, so nothing waited on the pipe in the first place). The spool carries 0.8-0.9 GB
  of it (the child drains the spool into the socket between big values), the queue peak drops to 15-18 MB, memory
  and latency are within noise. No regression, as TASK-1.30's AC3 asked.

## Runs 26-28 (2026-10-01): the spool's true footprint (TASK-1.30, "reflect true memory consumption")

The spool's pages are outside `used_memory` and RSS (page cache on disk, RAM on this tmpfs benchmark dir), so run 24's
"extra 17 MB" hid about 1 GB. Three steps, each measured on mixed1m at 4 GB on the same host (`hybrid` only):

| run | child behaviour | footprint measure | idle: spooled / footprint peak MB | 20k: spooled / peak | 50k: spooled / peak |
|---|---|---|---|---|---|
| 26 | pipe-first, holes punched 16 MB behind the reads | written minus first data offset | 258 / 258 | 1060 / 1060 | - |
| 27 | plus: flush the spool after every big value | same | 323-330 / 307-314 | 1060 / 1044 | 1061-1063 / 1045-1047 |
| 28 | same, hole punches aligned to 4 KB blocks | same | 337-350 / 61-62 | 1060-1061 / 60-64 | 1061-1065 / 59-70 |

- **Run 26** measured no reduction because the child, with pipe-first alone, handled the queued big values back to back and
  drained the spool only afterwards: the file held everything written in the meantime.
- **Run 27** flushed the spool after each big value, yet the peak still read "written minus 16 MB": an strace of the child
  showed the drains and punches happening as intended, so the *measure* was wrong. A hole punched to an unaligned byte
  offset leaves the straddling 4 KB block allocated, and `SEEK_DATA` from the start of the file stops at the first such
  block for good.
- **Run 28**, punches aligned to blocks: the footprint peaks at 59-70 MB while 0.34-1.07 GB pass through, about one big
  value's worth of scan output (the hashes take the child 0.85 s each, the scan thread produces about 70 MB/s). Save
  times 21-25 s, as in run 24. Two cells show 34-41 ms worst PING: the main thread's `SEEK_DATA` walked the whole punched
  prefix (a gigabyte of holes, page by page) on every INFO call; fixed by resuming from the previous frontier (run 29).
- Found on the way, by this test coverage rather than by the benchmark: a torn message header read by the child while
  the scan thread was still writing it (fixed: the scan thread publishes the committed spool length through a shared
  page and the child never reads past it), and a crash in the hybrid child that predates the spool: the arena's 16 TB
  range is not mapped in the child, the kernel reused those addresses for the child's own heap, and `zmalloc_is_nofork`
  then routed a realloc of a child buffer to the parent-only arena (fixed: the child re-reserves the range PROT_NONE
  right after the fork). The latter is what the 'converges under write load' test's occasional second save was.

## Run 29 (2026-10-01): final spool (overflow, aligned punches, committed offset, frontier resume) vs pipe only, c7i.4xlarge

Raw: `run29.csv` (header restored from run 28 after an interrupted first start), driver `~/run29.sh`. Binary fd2034f0b.
`hybrid-nospool` = `--bgsave-hybrid-spool no`, `hybrid` = default. mixed1m scale 16, rates 0 / 20k / 50k, 2 reps
interleaved; small scale 13.5, rates 0 / 20k, 1 rep. Every save `ok`. "spool peak" is the file's true footprint
(allocated bytes not yet consumed by the child).

| dataset | rate | config | save s | extra MB | parent +MB | CoW MB | main cpu s | threads cpu s | child cpu s | ping max ms | wmax ms | spooled MB | spool peak MB | queued peak MB |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| mixed1m | 0 | nospool | 34.8 / 35.2 | 20 / 18 | 9 / 1 | 11 / 17 | 0.5 | 17.3 / 19.4 | 21.7 / 21.8 | 15.4 / 13.1 | - | 0 | 0 | 16 |
| mixed1m | 0 | spool | 25.6 / 25.4 | 17 / 17 | 10 / 2 | 7 / 15 | 0.4 / 0.3 | 17.1 / 18.5 | 24.9 / 24.7 | 11.1 / 10.2 | - | 957 / 922 | 77 / 73 | 16 |
| mixed1m | 20k | nospool | 41.1 / 39.3 | 1249 / 1218 | 29 / 27 | 1245 / 1216 | 9.7 / 9.3 | 18.2 / 19.2 | 24.3 / 21.5 | 14.8 / 12.6 | 15.5 / 16.1 | 0 | 0 | 46 / 43 |
| mixed1m | 20k | spool | 24.3 / 21.4 | 904 / 829 | 12 / 7 | 897 / 824 | 6.2 / 5.5 | 18.9 / 19.5 | 24.2 / 21.3 | 15.0 / 11.9 | 15.4 / 15.8 | 1063 | 71 / 61 | 22 / 23 |
| mixed1m | 50k | nospool | 41.9 / 41.5 | 1748 / 1745 | 54 / 54 | 1745 / 1742 | 15.7 / 14.5 | 17.9 / 17.7 | 25.4 / 25.2 | 15.4 / 21.7 | 16.8 / 22.4 | 0 | 0 | 95 / 94 |
| mixed1m | 50k | spool | 25.7 / 24.4 | 1529 / 1507 | 12 / 14 | 1524 / 1494 | 11.5 / 10.3 | 19.6 / 19.4 | 25.6 / 24.4 | 11.9 / 15.8 | 17.4 / 17.0 | 1064 | 73 / 69 | 33 |
| small | 0 | nospool | 29.6 | 4 | 0 | 4 | 0.4 | 29.4 | 2.9 | 4.0 | - | 0 | 0 | 0 |
| small | 0 | spool | 32.9 | 2 | 1 | 1 | 0.5 | 32.7 | 3.1 | 0.9 | - | 0 | 0 | 0 |
| small | 20k | nospool | 31.8 | 8 | 7 | 1 | 6.8 | 31.6 | 3.1 | 1.4 | 2.4 | 0 | 0 | 102 |
| small | 20k | spool | 31.6 | 8 | 7 | 1 | 6.9 | 31.4 | 3.2 | 1.0 | 2.3 | 0 | 0 | 101 |

### Reading the run

- **mixed1m: 21-26 s at every write rate against 35-42 for the pipe shape** (run 21's stock: 40-46), as run 24; the
  three fixes since (committed offset, aligned punches, frontier resume) cost nothing measurable.
- **The spool's true footprint is 61-77 MB** while 0.92-1.06 GB pass through it: one big value's worth of scan output,
  freed as the child consumes it; the queue stays at 16-33 MB; parent growth 2-14 MB. This is the number to put next to
  RSS: the save holds about 90 MB outside `used_memory` at its peak, not a gigabyte.
- **Latency is back to the fork stall**: worst PING 10-16 ms (run 27/28's 34-41 ms SEEK_DATA walks are gone), worst
  write 15-17 ms.
- **small never spools.** Its idle cell reads 32.9 against 29.6 s (single reps; the same cell has read 29.6-33.6 s
  for both shapes across runs 23, 24 and 29), its 20k cell 31.6 against 31.8. Flagged, not established: a two-rep
  interleaved small pass would settle whether the per-buffer drain attempt costs anything on a dataset the child
  keeps up with.

## Run 30 (2026-10-01): two channels and a small-first child (TASK-1.31, first build 3406f4b4c), mixed1m at 4 GB and small, c7i.4xlarge

Raw: `run30.csv`, `run30s.csv` (full sync), driver `~/run30.sh`. The user's design: the scan thread feeds two pipes
(BUFFER messages on a small channel, KVOBJ/END/ABORT on a big one), the child takes the small channel while the
parent reports a small backlog of 1 MB or more and a big value otherwise, `bgsave-hybrid-max-queued-bytes` default
100 MB, spool off by default. `hybrid` = that default; `hybrid-spool` = `--bgsave-hybrid-spool yes` (spool as the
small channel's overflow past the bound, in stream order through SPOOL markers); `hybrid-q16` = the two channels
with run 29's 16 MB bound. mixed1m scale 16, rates 0 / 20k / 50k, 2 reps interleaved; small scale 13.5, rates
0 / 20k, 2 reps (`hybrid`, `hybrid-spool`); full sync (run 25 recipe) rates 0 / 20k, 1 rep, `hybrid`. Values as
rep a / rep b. "queued peak" is oq + mq (both queues plus the main thread's emitted buckets).

| dataset | rate | config | save s | parent +MB | CoW MB | main cpu s | threads cpu s | child cpu s | ping max ms | wmax ms | queued peak MB | spooled MB | spool peak MB |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| mixed1m | 0 | hybrid | 33.2* / 26.2 | 48 / 110 | 9 / 8 | 0.5 / 0.4 | 18.9 / 19.4 | 21.1 / 24.9 | 17.3 / 12.6 | - | 54 / 100 | 0 | 0 |
| mixed1m | 0 | hybrid-spool | 27.2 / 27.0 | 103 / 99 | 2 / 6 | 0.4 | 17.7 / 18.9 | 23.8 / 23.7 | 12.3 / 13.5 | - | 100 | 8 / 16 | 8 / 16 |
| mixed1m | 0 | hybrid-q16 | 32.9 / 35.9 | 7 / 9 | 10 / 8 | 0.5 | 19.0 / 19.4 | 21.0 / 23.3 | 11.4 / 10.6 | - | 16 | 0 | 0 |
| mixed1m | 20k | hybrid | 28.9 / 35.1 | 101 / 85 | 1014 / 1145 | 6.8 / 8.1 | 17.6 / 17.5 | 23.0 / 24.3 | 14.9 / 12.1 | 15.3 / 15.5 | 111 / 113 | 0 | 0 |
| mixed1m | 20k | hybrid-spool | 21.2 / 23.3 | 95 / 97 | 815 / 874 | 5.1 / 5.6 | 17.9 / 17.9 | 21.1 / 23.1 | 13.4 / 11.5 | 16.4 / 16.1 | 106 | 119 / 183 | 119 / 71 |
| mixed1m | 20k | hybrid-q16 | 36.4 / 39.0 | 19 / 18 | 1166 / 1212 | 8.8 / 9.1 | 19.1 / 19.2 | 21.3 / 23.8 | 11.2 / 14.6 | 15.8 / 16.3 | 28 / 30 | 0 | 0 |
| mixed1m | 50k | hybrid | 36.5 / 33.0 | 78 / 93 | 1696 / 1655 | 15.2 / 11.1 | 19.5 / 17.5 | 23.3 / 23.2 | 15.8 / 16.5 | 16.7 / 17.5 | 133 / 131 | 0 | 0 |
| mixed1m | 50k | hybrid-spool | 24.0 / 21.2 | 81 / 96 | 1490 / 1412 | 10.6 / 8.8 | 20.3 / 19.9 | 23.8 / 21.2 | 13.8 / 15.8 | 17.6 / 17.1 | 117 | 577 / 563 | 244 / 162 |
| mixed1m | 50k | hybrid-q16 | 38.9 / 35.8 | 23 / 21 | 1722 / 1691 | 16.2 / 14.9 | 17.4 / 18.0 | 25.0 / 21.5 | 15.9 / 13.8 | 17.7 / 18.8 | 54 / 48 | 0 | 0 |
| small | 0 | hybrid | 29.4* / 31.0 | 0 / 1 | 6 / 1 | 0.4 | 29.2 / 30.7 | 3.0 | 4.0 / 1.1 | - | 0 | 0 | 0 |
| small | 0 | hybrid-spool | 31.6 / 32.3 | 1 | 1 | 0.4 | 31.4 / 32.1 | 3.0 | 0.9 | - | 0 | 0 | 0 |
| small | 20k | hybrid | 31.1 / 31.6 | 7 | 1 | 6.5 / 6.8 | 30.8 / 31.4 | 3.2 | 1.0 / 0.9 | 2.2 / 2.5 | 99 / 101 | 0 | 0 |
| small | 20k | hybrid-spool | 30.7 / 31.0 | 7 | 1 | 6.4 / 6.5 | 30.5 / 30.8 | 3.2 | 1.1 / 0.8 | 2.8 / 2.6 | 98 / 99 | 0 | 0 |
| sync mixed1m | 0 | hybrid | 16.2 | 54 | 3 | 0.2 | 8.9 | 6.5 | 19.6 | - | 57 | 0 | 0 |
| sync mixed1m | 20k | hybrid | 17.0 | 99 | 695 | 3.8 | 9.3 | 6.4 | 15.9 | 15.9 | 104 | 0 | 0 |

\* first cell of the dataset, right after the live populate (run 16 pitfall): for mixed1m that layout also puts the
24 hashes last in scan order (the run 22 correction), so it is not comparable.

### Reading the run

- **Idle, from the cache: the bounded two-channel shape overlaps (26.2 s against run 29's 25.4-25.6 with the spool
  and 34.8-35.2 pipe-only)**, with the queue at its 100 MB bound and parent growth 110 MB: the child is the longer
  side (24.9 s CPU against 19.4 of scan), so whatever the policy, the scan thread's lead accumulates up to the bound.
  The 16 MB bound does not (32.9-35.9 s): one hash's worth of scan output, about 47 MB, has to fit.
- **Under writes the bounded shape did NOT overlap: 28.9-36.5 s against 21.2-24.0 for the spool shape** (run 29's
  spool: 21.4-25.7; pipe-only 39-42). Parent growth 78-101 MB says the queue hit the bound and the scan thread parked.
  Cause (fixed as bfaa21174, run 30b): the child's rule read "small channel while the backlog is 1 MB or more" but
  the child could only act on it when the small *pipe* was readable, and the scan thread pushes the queue into the
  pipe only at its own enqueues (about once a millisecond, a pipe's worth each time). At every such gap the child
  found the pipe empty and the big channel ready, and took the next big value with tens of megabytes waiting in
  the parent's queue; with the 24 hashes handed over at the first write to each, that is the back-to-back shape of
  run 17 again, slightly interleaved. The fix makes the child wait up to 20 ms for the pending backlog before taking
  a big value.
- **The spool shape is unchanged by the two channels** (21.2-27.2 s at every rate), with the spool doing more of the
  work than in run 29 because the queue now fills to 100 MB before anything spills: 8-16 MB spooled idle, 119-183 MB
  at 20k, 563-577 MB at 50k, footprint peak up to 244 MB (the child reads spooled segments at their place in the
  stream, after the queued buffers that precede them, so the footprint is no longer bounded by one big value's
  worth as in run 29).
- **small is noise either way**: 29.4-32.3 idle and 30.7-31.6 at 20k for both configs (two reps each); nothing is
  spooled and the queue never fills (the 98-101 MB peak at 20k is the main thread's emitted buckets, as in runs
  24-29). This settles run 29's single-rep flag: no cost from the spool's drain attempt.
- **Full sync unchanged**: 16.2 / 17.0 s (run 25: 16.2 / 16.9-17.0).
- Latency as before: worst PING 10.6-17.3 ms (the fork stall), worst write 15.3-18.8 ms.

