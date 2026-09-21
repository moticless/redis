# Hybrid BGSAVE, M1 report (branch `snapshot-hybrid-bgsave`)

Status: M1 delivered; stop-and-report per spec section 9. Written 2026-09-21.
Commits: `538391875` (arena), `6871810d4` (scan), `e2f367fc9` (placement),
`e8ee6e039` (tables), `96d7bc733` (overlay + hybrid save), `4c62e8f6a`
(assertions, LRU/LFU/defrag tests), `1b9d645c5` (harness, arena-free mode,
hard mark), `0ae712867` (sanitizer fixes). Nothing pushed, no PR.

## 1. What was built

- `src/snapshot.c` / `snapshot.h`: everything prefixed `snap`. SNAP_NOFORK jemalloc
  arena (one reserved VA range, `MADV_DONTFORK` once, extents carved with a bump
  pointer; `experimental.arenas_create_ext` with `metadata_use_hooks=false` so the
  arena's own bookkeeping stays forkable); class-aware alloc/free API; placement
  policy at insertion (`snapPlaceValue`, all kvobj headers plus SNAP_NOFORK
  values plus the keyspace hash tables live in the arena); per-slot scan states
  and a scan thread that walks frozen dicts with `dictScanNoPause`; the hybrid
  save session (pipe to a fork child, 64 KB staging buffer, kvobj copies with
  the metadata prefix and dbid for SNAP_FORK keys, SELECTDB/RESIZEDB/SLOT_INFO
  tracked in the child); abort, child-death and shutdown paths.
- `src/kvstore.c`: the keyspace overlay. Frozen dicts are never written: the first
  write to an owed key clones into a per-slot temp dict, deletes tombstone, adds
  go to the temp dict, lookups consult the temp dict first, iteration and sizes
  are logical, and the overlay merges back when the slot is scanned.
- Configs: `bgsave-hybrid` (immutable, default no), `save-small-max-bytes`
  (default 4096), `bgsave-hybrid-max-retained-bytes` (default 256mb).
- Observability: INFO memory `allocator_*_nofork`, `snap_nofork_arena_*`; INFO
  persistence `snap_save_*`, `snap_last_save_retained_peak`,
  `snap_hard_mark_aborts`; `DEBUG SNAP-PLACEMENT`, `DEBUG SNAP-DRIFT`; latency
  events `hybrid-freeze`, `hybrid-merge`.
- Tests: 134 unit checks (`redis-server test snapshot`), 28 tcl tests in
  `tests/unit/snapshot.tcl` including the verification harness.

## 2. Deliverable (a): static-dataset equivalence

Byte-identical output against stock was replaced by reload-equivalence: record
order differs (scan order, SNAP_FORK records interleaved by the child), and the
child writes SELECTDB/RESIZEDB per db change and SLOT_INFO per slot change
instead of once per db. Evidence, test "mixed multi-db dataset with functions and
module keys reloads identically" and "hybrid RDB loads into a stock server":

- dataset over dbs 9, 1, 2: every type in both classes, key TTLs, hash field TTLs,
  a Lua function library, module type keys (tests/modules/datatype.so);
- hybrid RDB reloads to the pre-save DEBUG DIGEST; `redis-check-rdb` accepts it;
- a stock server loads the hybrid file to the same digest, and its own stock save
  of the same dataset reloads to the same digest as well;
- functions, field TTLs, key TTLs and module values survive.

## 3. Deliverable (b): race test

Test "randomized writers over every path race the save" and its eviction
variants. Each iteration: populate (about 3,600 keys, both classes, TTLs, field
TTLs, module keys, two extra dbs), take the reference digest and per-key
digests, start a save slowed to 400 us per key, run 400 random writes drawn from
44 kinds (all raced the save, checked every 50 ops), let the save finish, reload
and compare. Kinds cover the write-path audit: value writes of both classes,
class-crossing growth (SET/APPEND to 9 KB), listpack to hashtable / skiplist /
quicklist and back, header-only EXPIRE/PERSIST/HEXPIRE on SNAP_FORK keys,
DEL/UNLINK/GETDEL, RENAME/COPY/MOVE/RESTORE REPLACE, HINCRBY/HDEL/HPERSIST,
SADD/SREM/SPOP, ZADD/ZPOPMIN, RPUSH/LPOP/LTRIM/LSET, XADD/XTRIM, module writes,
short-PX keys (lazy and active expiry during the save) and reads.

| run | iterations | result |
|---|---|---|
| CI variant | 3 | equal |
| allkeys-lru, maxmemory below used_memory | 1 (34-103 evictions) | equal |
| allkeys-lfu, maxmemory below used_memory | 1 (1-590 evictions) | equal |
| cluster mode (`start_cluster 1 0`, per-slot scan) | 1 | equal |
| `--accurate` stress | 100 | equal |

Defects the harness found, all fixed on the branch:

1. UNLINK of an owed key read the key from an entry whose value had already been
   detached (crash).
2. A child stopping on a broken channel left slots owed; release asserted.
3. SET overwrite took the in-place swap path and left the new value outside the
   arena (silently SNAP_FORK, no CoW benefit).
4. Set and zset conversions back to listpack allocated outside the arena.
5. Cluster per-slot `alloc_size` accounting on the overlay temp dict (crash under
   memory tracking): the temp dict's destructor now accounts on the real dict, a
   clone takes over its original's accounting, and slots empty at freeze are DONE
   from the start and never frozen.
6. Overlay merge looked up the state dict after freeing originals whose keys it
   borrowed (heap-use-after-free, single-threaded, found by TSan).
7. Scan thread read mutable main-thread globals: `server.hz` through `LRU_CLOCK`,
   `maxmemory_policy`, `rdb_compression`, `allow_access_expired`,
   `rdb_key_save_delay`. All are now per-save copies or thread-locals.
8. Arena-free mode shared a size-classed single-allocation value with the clone,
   which then reallocated it while the scan thread still had to read its
   allocation (heap-use-after-free, found by TSan); such values are now copied.

Sanitizers: ThreadSanitizer and AddressSanitizer runs of the whole unit are clean
in the arena-free hybrid mode (libc allocator; 22 tests, the 6 arena-only tests
skip). The arena itself is only testable with jemalloc.

## 4. Deliverable (c): abort behaviour

FLUSHALL, FLUSHALL ASYNC, FLUSHDB and SWAPDB during a save abort it: the scan
thread stops within one cursor step, every slot merges, the child exits on
SIGUSR1 with no error counted, the next BGSAVE works and reloads correctly. Also
covered: the child killed mid-save (status err, overlay merged, a following save
equals the live keyspace), SHUTDOWN NOSAVE mid-save (clean exit, no crash
report), and the retained-bytes hard mark (status err, `snap_hard_mark_aborts`
incremented, no client command above 50 ms while it aborted).

## 5. Deliverable (d): copy-on-write

`rdb_last_cow_size` of the child, baseline-corrected: the delta is a storm run
minus a no-write run of the same dataset (the child dirties some pages of its
own either way).

| workload | hybrid delta | stock delta | ratio |
|---|---|---|---|
| 20,000 x 60 B strings, all overwritten during the save | 225-725 KB | 1.94-2.00 MB | 2.7-8.7x |
| 20,000 x 1,000 B SNAP_FORK values (threshold 256 B), EXPIRE on all | 233-258 KB | 1.25-1.42 MB | 4.9-5.5x |

The second row is the header test: EXPIRE rewrites the kvobj header of every
SNAP_FORK key; in stock that dirties every header page, with the arena the
headers (and the expires table) are not mapped in the child at all. The residual
hybrid delta is default-arena churn: the client's argv strings freed after the
command, jemalloc metadata of the arena (kept forkable on purpose), reply
buffers. It does not grow with the number of objects touched.

Not measured yet: a production-shaped dataset (the sibling branch used a 15M-key
dump), fork stall, save throughput, main-thread latency under load. These are
the M2 measurements.

## 6. Decisions on the spec gaps

- Slot granularity in standalone mode: one kvstore dict per db, so the whole db
  is owed until scanned; cluster mode uses the 16,384 slot dicts and releases
  them progressively.
- LRU/LFU clock: no-op by construction. `lookupKey` skips the clock update while a
  child exists; writers touch their clone. Saves under allkeys-lru/lfu reload
  correctly.
- Active defrag: paused while a child exists (stock behaviour), covered by a test
  with `activedefrag yes` and a slow save.
- listpackEx: the struct is arena-born and never moved (its ExpireMeta is linked
  in ebuckets); clones get a fresh struct and listpack copy.
- Expires consistency: the expires kvstore is not frozen; a clone replaces its
  original in the expires dict and in the field-TTL ebuckets at clone time.
- Platform fallback: the hybrid path no longer needs the arena. Without it
  (non-Linux, libc, sanitizers) classification is by encoding and size, single-
  allocation values are always copied on clone, and the CoW benefit is gone but
  behaviour and tests are identical.
- kvobj class bit: not needed; class is placement (arena) or size (no arena).
- Class-aware free: generic zfree/zrealloc are range-aware (one comparison); the
  explicit class-aware wrappers exist for hot paths.
- Staging buffer: 64 KB messages from the scan thread; a 1 MB pipe.
- Retention bound (adopted from the sibling branch): the overlay accounts the
  originals it keeps alive; past `bgsave-hybrid-max-retained-bytes` the save is
  aborted from serverCron and reported as failed, never inside a write path.

## 7. Blockers and limitations

None against invariants 1 and 2 were found: no write path reaches a frozen
original (debug-build assertion at the write funnels never fired in the whole
suite), and no slot is modified before it is scanned. Limitations to carry into
M2:

- Eviction of an owed key only tombstones; memory returns at slot merge. Under
  tight maxmemory in standalone mode a long save can evict a large part of the
  keyspace without freeing anything (the hard mark bounds the retained bytes,
  not the eviction count).
- AOF rewrite, diskless full sync and `RedisModule_Fork` are refused while the
  arena is on (M2 tasks 1.11-1.15).
- Cluster atomic slot migration during a save is not guarded.
- The debug assertion could not be exercised against the full stock suite with a
  save forced to stay in progress; the harness is the substitute.
- Templated hash encodings do not exist on this base commit.

## 8. Test status

- `redis-server test snapshot`: 134/134 (jemalloc), 81/81 (libc, arena checks skipped).
- `./runtest --single unit/snapshot`: 28/28 jemalloc, 22/22 under TSan and under
  ASan (arena-free).
- Full stock suite (`./runtest --clients 16 --timeout 1200`, jemalloc build):
  5,577 tests passed, 1 failed. The failure was the hybrid test "writes during
  the save land in the overlay": on the loaded box the save finished before the
  test's own writes did (a test-timing assertion, not a server defect); its save
  delay was raised and the test passes again. No stock test regressed.
- `--accurate` stress: 100 iterations of the randomized race, all equal to the
  fork-instant reference (478 s).

## 9. Comparison with the sibling branch (`~/git/redis`, `hybrid-bgsave`)

Different mechanism, same goal. That branch marks keys with a 3-bit generation,
sweeps the keyspace on the main thread (25% CPU budget), pins SNAP_NOFORK keys by
refcount and duplicates in-flight keys on write; the child owns the output and
also serves AOF rewrite and diskless sync; it is measured on a production dump.
This branch has no main-thread walk and no table CoW, and its invariant is
structural (frozen dicts). Adopted from there: arena-free mode, the retained-bytes
hard mark, the module-fork guard, per-save config copies for the scan thread.
Not adopted: generation bits and reset pass, pin/duplicate scheme, child-owned
output with a writer thread (a possible M3 throughput lever).

## 10. Go / no-go for M2

Go, with conditions. The crux holds: writes of every kind during a save leave the
snapshot at the fork instant, the arena removes header, small-value and table CoW,
and the mechanics survive TSan and ASan. Conditions before M2 work starts:

1. Measure on a production-shaped dataset (CoW, fork stall, save time, main-thread
   p99) against stock, as the sibling branch did.
2. Decide the eviction-during-save policy (tombstone-only vs. abort past a count).
3. M2 order: diskless sync and AOF rewrite through the hybrid session first, since
   both are refused today and the sibling branch shows they fit the same child.

## 11. Addendum (2026-09-22): serialize-before-write replaces the overlay's clone-on-write

After M1 the write side was redesigned (spec §11.z, backlog TASK-1.21): the
scan thread publishes the bucket window it reads, and the main thread emits a
bucket's chain into the snapshot itself before the first post-fork write to it
(per-save bucket bitmaps decide who emits). Clone-on-write survives only for
buckets inside the window being read. Verification and the four-way benchmark
against upstream and Valkey (fork and forkless) are in
`utils/snap-bench/RESULTS.md` run 6; leftovers in spec §11.z.1. Sections 3-5
above describe the M1 state and remain valid as the record of that milestone.
