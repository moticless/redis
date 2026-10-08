# Hybrid BGSAVE vs. stock Redis: current summary

Benchmarks of 2026-09-26 to 2026-10-04 (runs 12, 13, 33, 34, 34b, 36 in `RESULTS.md`; raw CSVs
alongside). Host: c7i.4xlarge, 4 GB datasets, two interleaved reps per cell, the load generator on
separate vCPUs. `stock` is upstream `0d6266f2d`; `hybrid` is the `snapshot-hybrid-bgsave` branch with
default settings (`bgsave-hybrid yes`, 256 MB retained-bytes hard mark, 4 KB small-value cap).

Datasets:

| name | content | share of bytes under the 4 KB cap |
|---|---|---|
| small | 100-byte strings and 10-field listpack hashes, 18.9 M keys | all |
| mixed | 45% small values, 55% in 256k hashtable hashes of about 9 KB | about half |
| mixed1m | half small values, half in 24 hashes of 1M fields (about 90 MB each) | about half |
| big / big1m | 240 or 68 keys: hashes of 10 to 100 MB and zsets of 300k members | none |

Writes are uniform over the keyspace unless marked "20% hot".

## Eight vCPUs for the server (run 34, 2026-10-04)

| dataset | writes/s | save time stock → hybrid | extra memory stock → hybrid | write max stock → hybrid |
|---|---|---|---|---|
| mixed1m | idle | 43 s → 28 s | 5 MB → 85 MB | – |
| mixed1m | 20k | 49 s → 26 s | 2.6 GB → 0.97 GB | 32 ms → 20 ms |
| mixed1m | 50k | 44 s → 28 s | 3.3 GB → 1.6 GB | 34 ms → 20 ms |
| mixed (9 KB) | 50k | 43 s → 21 s | 3.2 GB → 1.4 GB | 39 ms → 22 ms |
| mixed (9 KB) | 50k, 20% hot | 43 s → 20 s | 2.0 GB → 0.5 GB | 47 ms → 20 ms |
| small | 20k | 39 s → 35 s | 2.5 GB → 8 MB | 33 ms → 2 ms |
| small | 50k | 39 s → 31 s | 3.4 GB → 8 MB | 40 ms → 4 ms |

Write p99 is equal in every cell (0.3 to 0.7 ms for both) and every cell reached the offered rate.
Main-thread CPU under writes is 60 to 70% of stock's; the scan thread adds about one core for the
save's duration; the child's CPU drops to a few seconds on small data. Total CPU per save is about
the same as stock, spread over more cores.

## Big dataset, everything above the cap (runs 12 and 13, 2026-09-26)

| dataset | writes/s | save time stock → hybrid | extra memory stock → hybrid | write max stock → hybrid |
|---|---|---|---|---|
| big | 20k | 33 s → 31 s | 1.8 GB → 1.8 GB | 34 ms → 28 ms |
| big | 50k | 33 s → 34 s | 2.7 GB → 2.8 GB | 31 ms → 29 ms |
| big | 50k, 20% hot | 32 s → 33 s | 0.6 GB → 0.6 GB | 35 ms → 27 ms |
| big1m | 50k | 36 s → 36 s | 2.8 GB → 2.8 GB | 28 to 37 ms → 29 to 32 ms |

Fork parity by construction: every value is in the fork class, so the child does the same work as
stock and the scan thread has nothing to serialize. The hybrid adds nothing and costs nothing here.
(These runs used the branch at `54b7d4f0c`; the later queue and channel work does not touch the
path measured on this dataset.)

## One core for server, scan thread and child (run 33, 2026-10-02, mixed)

| writes/s | save time stock → hybrid | extra memory stock → hybrid | write p99 stock → hybrid | write max stock → hybrid |
|---|---|---|---|---|
| idle | 46 s → 47 s | 2 MB → 2 MB | – | – |
| 20k | 52 to 56 s → 53 to 54 s | 2.7 GB → 1.4 GB | 3.1 ms → 4.9 ms | 37 ms → 20 to 27 ms |
| 50k | 68 to 69 s → 60 to 61 s | 3.3 GB → 1.8 GB | 3.9 ms → 6.2 ms | 59 to 63 ms → 36 ms |
| 50k, 20% hot | 67 to 68 s → 60 s | 2.1 GB → 0.5 GB | 3.9 ms → 6.0 ms | 64 to 68 ms → 35 to 39 ms |

On one core the save-time gain mostly disappears: the scan thread and the child take turns on the
same core. The memory and fork-stall gains stay. The cost shows in write p99, up about 50%, because
the scan thread competes with the main thread for the core.

## Full sync with the primary on one core (run 38, 2026-10-07, mixed and mixed1m)

| dataset, writes/s | sync time stock → hybrid | primary extra memory stock → hybrid | write p99 stock → hybrid | write max stock → hybrid |
|---|---|---|---|---|
| mixed, 20k | 16 s → 15 s | 1.4 GB → 0.6 GB | 1.7 to 1.8 ms → 2.5 ms | 36 ms → 18 ms |
| mixed, 50k | 18 to 20 s → 19 s | 2.6 GB → 1.3 GB | 2.4 ms → 3.1 ms | 37 to 39 ms → 21 ms |
| mixed1m, 20k | 19 s → 21 s | 1.6 GB → 0.8 GB | 1.8 ms → 2.3 to 2.6 ms | 32 ms → 17 ms |

Sync time is the replica's load either way, so the hybrid matches stock on it with one core as with
eight. Memory and the fork stall halve as in every other run; write p99 pays the scan thread's share
of the core, about 1 ms. Valkey's single-channel fork sync does not survive the 50k cell on one
core: its output buffer reaches the 256 MB limit, the replica is dropped, and three full syncs take
139 s.

## Takeaways

- **Memory under writes is the main win**: 2 to 3x less copy-on-write on mixed data and near zero on
  small data, independent of the core count. Idle saves on mixed data cost a little more than stock
  (the main thread's emit-ahead buffers, under 100 MB).
- **Saves are 40 to 50% faster on mixed data with spare cores**, 10 to 20% on small data, unchanged
  on big data and on one core. Full syncs take the replica's load time in every configuration.
- **Tail latency improves wherever the fork touches less memory**: write max halves on mixed data
  and drops 10x on small data. Write p99 is unchanged with spare cores and worse on one core.
- **No client is ever blocked** by a save, on any dataset.
- **Robustness**: one hard-mark abort in run 34 was a counter drift in the queue byte count, fixed
  the same day and rerun clean (run 34b, three saves). Three structural changes to the write path
  (per-dict save state, side-chain table, queue fix) cost nothing measurable (run 35).
- **Where the time goes**: on this incompressible benchmark data two thirds of the serializer's CPU
  is failed LZF compression; `rdbcompression no` cuts an idle small save from 32 s to 10 s for stock
  and hybrid alike (run 36). An adaptive compression skip is filed as TASK-1.33.

## Valkey forkless, for context

Valkey `unstable` (`06d35aca6`) with `forkless-infrastructure-enabled yes` and
`bgsave-default-method forkless` was measured in the same cells. It serializes on a background
thread with no fork at all, so its extra memory is 1 to 20 MB in every cell, and it has no fork
stall (PING max 1 to 3 ms against our 12 to 20 ms). The price is paid on the main thread and by
clients: a write to a key the iterator has not reached is expedited, cloned or blocked.

- On mixed and small data its saves are 45 to 56 s against our 20 to 35 s, its main thread spends
  12 to 30 CPU-seconds per save against our 0.3 to 15, write p99 is 2 to 5x ours, and it misses the
  offered rate at 50k writes/s on mixed data (74 to 93% delivered, 9 to 16 clients blocked at the
  peak, 10 to 18 stalled seconds per save).
- On the big datasets a write to a hash under serialization waits until the whole 90 to 100 MB hash
  is written: 20 to 35 s maximum write latency, 41 to 49% of the offered rate delivered.
- On one core it delivers 87 to 89% of the offered rate at this size, with p99 of 12 to 19 ms.

Valkey's own issue (valkey-io/valkey#2878) draws the same line: forkless when memory is the
constraint, fork when speed and latency are. The hybrid sits between the two, close to fork on
latency and CPU, well below fork on memory, and it never blocks a client; its remaining cost is the
copy-on-write of values above the cap and the scan thread's core.

## Two hybrid flavours, one decision (2026-09-25/26)

We evaluated two flavours of the hybrid approach, differing in whether the kvobj headers and the
kvstore dict tables are forkable. In the forkable flavour they stay in normal memory: the fork child
walks its copy-on-write copy of the dicts and sends the fork-instant keys back to a parent helper that
serializes the small values. In the non-forkable flavour they live in the `MADV_DONTFORK` arena with
the small values, so the child never sees the keyspace and a parent scan thread walks the live dicts
under a bucket window and streams records to it. Based on the numbers (run 10, the three binaries
interleaved per cell) we went with the non-forkable flavour: with forkable headers every write to a
small key still dirties the page of its header and its dict bucket, so copy-on-write and the fork
stall stay proportional to the keyspace, and the per-key round trip through the helper shows up as
main-thread waits; with everything in the arena a small-value workload costs a few MB and a
sub-millisecond fork at any write rate, while all-big data is identical for both. We accepted the
engineering cost (the scan thread, the window and write-section protocol, per-dict save state, care
around rehash) and carried over what the forkable prototype did better as follow-ups: the arena
madvise flip, now the fallback for AOF rewrite and module forks, eviction integration and its test
scenarios; runtime mode switching is still open.
