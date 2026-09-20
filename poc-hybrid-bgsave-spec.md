# BGSAVE redesign — background per-slot scan, SNAP_NOFORK arena for small values, fork for SNAP_FORK values

Self-contained implementation spec. Assumes ONLY a fresh clone of the upstream `redis` repository
(branch `unstable`). Do the work on a new branch. The baseline to beat is stock `BGSAVE`.

## Terminology and code layout
- Every key is either **SNAP_NOFORK** or **SNAP_FORK** (the class of its *value* allocation; see §1).
- All snapshot-related implementation lives in a dedicated `src/snapshot.c` / `src/snapshot.h`.
  Every function in that file is prefixed `snap` (e.g. `snapClassify`, `snapEncodingIsNoforkClass`,
  `snapNoforkAlloc`, `snapScanThreadMain`, `snapSlotMerge`, `snapChildWriterLoop`).
- "SNAP_NOFORK arena" = the jemalloc arena whose extents are `madvise(MADV_DONTFORK)`.
  "SNAP_FORK memory" = the default, forkable allocator.

## 0. Problem
Stock BGSAVE forks and the child serializes the whole keyspace from its copy-on-write (CoW) snapshot
while the parent serves writes. Two independent costs: (a) the `fork()` page-table copy — a latency
stall growing with dataset size; (b) **CoW amplification** — the kernel copies memory at *page*
granularity (4 KB, or 2 MB with transparent huge pages), so a 64-byte write to one small value
duplicates the whole 4 KB page it shares with unrelated neighbours; sustained writes during a long
save multiply into large excess RSS and can OOM the host (`RSS_spike ~= write_rate x save_duration x
amplification_factor`). This design keeps a fork for large values but eliminates CoW amplification for
small values and kvobj headers and moves the keyspace walk off the main thread.

## 1. Core idea
- **Every kvobj header lives in the SNAP_NOFORK arena**, regardless of key class. A kvobj allocation is
  `[metadata slots (expiry first)] [robj] [key-hdr-size] [embedded sds key]`; the kvobj pointer is the
  robj in the middle. The whole allocation is SNAP_NOFORK, so the fork never sees any kvobj header.
- Classify each key at creation by its **value**:
  - **SNAP_NOFORK**: the value is at most one separate allocation, its in-RAM size is <= a new config
    `save-small-max-bytes` (default 4096), and its encoding is in `snapEncodingIsNoforkClass()` below
    (strings of any encoding, listpack list/set/zset/hash, intset, listpack-ex, templated hash
    encodings). The value allocation is made in the SNAP_NOFORK arena. Embedded-string and int-encoded
    values have no separate allocation and are trivially SNAP_NOFORK.
  - **SNAP_FORK**: everything else — multi-allocation encodings (hashtable/skiplist/quicklist), module
    types, streams, or any value over the threshold. Value allocations live in SNAP_FORK memory.
- **The kvstore hash tables (bucket arrays / dict metadata, for `db->keys` and `db->expires`) are also
  allocated in the SNAP_NOFORK arena.**
- Optionally mark each kvobj with an explicit class bit in the robj header (a `SNAP_NOFORK` bit) so the
  class is available without recomputing it, in particular on free paths (§2).
- On BGSAVE the parent forks. The fork holds a frozen CoW snapshot of **SNAP_FORK values only** and
  **owns the output** (temp file -> rename+fsync, or the replica socket for diskless). The fork holds
  no kvobj headers, no hash tables and no SNAP_NOFORK values (all DONTFORK) — it is a passive
  serializer-of-SNAP_FORK-values plus the file writer.
- One or more **parent background threads** drive the save: each takes ownership of a set of kvstore
  **slots**, scans each slot's hash table, and for every key:
  - if **SNAP_FORK**: copies the **whole kvobj allocation** (preceding metadata + robj + embedded key)
    together with the **dbid** into the channel; the fork rebuilds a kvobj view over the copy, whose
    `ptr` resolves to the value in its CoW snapshot, and serializes it with `rdbSaveKeyValuePair`.
  - if **SNAP_NOFORK**: serializes the key itself (reading the parent's SNAP_NOFORK memory) into a
    per-db **staging buffer of ~64 KB**; the buffer is handed to the fork when full, when the db
    changes, or at slot end.
- CoW amplification on SNAP_NOFORK values and on all kvobj headers is avoided two ways at once: they
  are DONTFORK (the fork never maps them, so a parent write can never CoW them), and the parent never
  writes a fork-instant header or SNAP_NOFORK value in place during a save (it clones — see §4).

```c
/* snapshot.c — starting point for the value-encoding half of snapClassify() */
int snapEncodingIsNoforkClass(int type, int encoding) {
    switch (type) {
    case OBJ_STRING:
        return 1; /* int, embstr, raw */
    case OBJ_LIST:
        return encoding == OBJ_ENCODING_LISTPACK;
    case OBJ_SET:
        return encoding == OBJ_ENCODING_INTSET || encoding == OBJ_ENCODING_LISTPACK;
    case OBJ_ZSET:
        return encoding == OBJ_ENCODING_LISTPACK;
    case OBJ_HASH:
        return encoding == OBJ_ENCODING_LISTPACK ||
               encoding == OBJ_ENCODING_LISTPACK_EX ||
               encoding == OBJ_ENCODING_TMPL_LP ||
               encoding == OBJ_ENCODING_TMPL_ARRAY;
    default:
        return 0;
    }
}
```

## 2. Memory placement, classification and allocator API
- Add the SNAP_NOFORK arena: a jemalloc arena created with custom extent hooks that `madvise(...,
  MADV_DONTFORK)` every extent (and re-apply it on commit, since default commit can drop the flag).
  Allocate all kvobj headers, SNAP_NOFORK values and all kvstore bucket/dict structures from it;
  everything else from the default arena. Only the parent ever allocates/frees/dereferences this
  arena; a fork/child must never touch it — its virtual range is unmapped there.
- **Allocation must be fast.** Provide a distinct API (`snapNoforkAlloc/Realloc/Free`, and the plain
  `zmalloc` family for SNAP_FORK) that selects the arena with `MALLOCX_ARENA(...)` and a dedicated
  per-thread tcache, rather than switching the thread's default arena. Consider static preallocation
  of hot structures. **free() is the hard part**: jemalloc thread caches are not arena-aware, so a
  SNAP_NOFORK pointer released through plain `free()` lands in the default tcache and can be handed
  back to a SNAP_FORK allocation. SNAP_NOFORK frees must therefore go through `sdallocx` with the
  arena's own tcache (or `MALLOCX_TCACHE_NONE`), which requires the caller to know the class — hence
  the class bit in the kvobj and class-aware free wrappers for value buffers (sds, listpack, intset).
- `snapClassify(type, encoding, size)` returns SNAP_NOFORK or SNAP_FORK. Placement of the **value** is
  decided when the object is created or when it changes class; the header placement never changes.
- **Relocation on class change (required).** A SNAP_NOFORK value that grows past the threshold or
  converts to a multi-allocation encoding must have its value moved to SNAP_FORK memory (and
  vice-versa is optional). A SNAP_FORK value must never live in the SNAP_NOFORK arena — the fork must
  be able to read it. During a save this is handled naturally by clone-on-write (§4): the new/grown
  value is allocated in the correct arena for its new class while the fork-instant original is
  preserved.

## 3. Actors
- **Fork (child):** owns the output stream. Receives from the channel either a kvobj copy (SNAP_FORK
  key) which it serializes with `rdbSaveKeyValuePair` against its CoW snapshot, or a pre-encoded
  buffer of SNAP_NOFORK records which it writes verbatim. Writes the header/aux fields, functions and
  module aux data first, then records as they arrive, then footer/CRC; fsync + rename (disk) or closes
  the socket (diskless). **Tracks the current SELECTDB**: because records from the background
  thread(s) and the fork's own serialization interleave, every channel entry carries its dbid and the
  fork emits `SELECTDB` (and, on first sight of a db, `RESIZEDB` / `SLOT_INFO` from sizes sent by the
  parent at start) only when the dbid differs from the one currently selected in the output.
- **Background thread(s) (parent):** own disjoint slot sets; scan each owned slot; for SNAP_FORK keys
  copy the kvobj allocation + dbid into the channel; for SNAP_NOFORK keys serialize into the ~64 KB
  per-db staging buffer and ship it; signal slot-scan-complete. **Read-only on the keyspace** (never
  mutate a hash table or an object).
- **Main thread:** serves clients; is the SOLE mutator of the keyspace; during a save it captures
  fork-instant headers and SNAP_NOFORK values via clone-on-write and buffers all changes in a per-slot
  temporary dict, then merges at slot-scan-complete.

## 4. The per-slot scan protocol (the concurrency model — this is the crux, get it exact)
A monotonic scan cursor tracks, for every slot, one of: `PENDING`, `SCANNING` (owned by a bg thread),
`DONE`. Transitions are published so the main thread can read a slot's state cheaply.

**Invariant: while a slot is `SCANNING`, its hash table, the kvobj headers it references and the
fork-instant SNAP_NOFORK values are immutable.** The background thread only reads them. All
main-thread changes go to a per-slot **temp dict** (a normal dict, main-thread-only), never to the
scanned hash table:

- `dictPauseRehashing` the slot's dict for the `SCANNING` window (so bucket layout is stable).
- The bg thread `dictScan`s the slot, classifying each entry and dispatching it (SNAP_FORK -> kvobj
  copy, SNAP_NOFORK -> serialize into the staging buffer). It touches nothing else.
- The main thread, for a key whose slot is `PENDING` or `SCANNING` (i.e. still owed to the snapshot),
  on the FIRST modification of that key during the save: **clone** — allocate a new kvobj (always
  SNAP_NOFORK) and, if the value changes, a new value in the arena matching its new class; apply the
  change to the clone; insert the clone into the temp dict; LEAVE the fork-instant original in place
  in the scanned dict. Subsequent modifications of the same key act on the clone in the temp dict in
  place. Presence in the temp dict is the "already captured" marker.
- **Delete of an owed key:** put a tombstone in the temp dict; leave the original in the scanned dict
  so the bg still serializes it (it existed at fork).
- **Add of a new key to an owed slot:** insert into the temp dict only (never the scanned dict); it is
  post-fork and must NOT be serialized.
- **Reads during a save:** consult the temp dict first (covers modified/added/deleted keys), else the
  scanned dict. Because the scanned dict is immutable while `SCANNING`, concurrent reads by the main
  thread and the bg thread are safe with no lock beyond the rehash pause.
- **Slot-scan-complete:** the bg thread signals done; the **main thread** then merges the temp dict
  into the real dict (apply clones/tombstones/adds) and frees the fork-instant originals it replaced.
  Freeing a header or SNAP_NOFORK value is CoW-free (DONTFORK: the fork never mapped it). After merge
  the slot is `DONE`.
- For a key whose slot is already `DONE`, modifications act in place normally — no clone, no temp
  dict. So the capture burden shrinks as the save advances.

## 5. SNAP_FORK keys
The **value** of a SNAP_FORK key is served entirely by the fork's CoW snapshot; the main thread mutates
the value in place as usual (paying ordinary page-granular CoW on that value — the accepted cost, same
as stock BGSAVE). The **header** of a SNAP_FORK key is SNAP_NOFORK memory that the bg thread copies
live, so header mutations of an owed SNAP_FORK key (EXPIRE/PERSIST, metadata, LRU/LFU clock) must go
through clone-on-write like any other header: the clone shares the value pointer, and at merge only the
original header is freed, never the value it shares. A deleted/modified SNAP_FORK key stays represented
in the scanned dict (tombstoned in the temp dict) so the bg still ships its header copy and the fork
still serializes the fork-instant value; the fork always resolves `ptr` in its own snapshot, so parent
frees/reuses cannot affect it. (Optionally defer SNAP_FORK value frees to save-end to avoid their CoW;
a memory-vs-CoW tradeoff, not needed for correctness.)

## 6. Output and channels
- **bg -> fork command channel** (pipe/shared queue), entries carry a dbid and are one of:
  `START {per-db key/expire counts, slot info}`, `FORK_KVOBJ {dbid, alloc_len, bytes of the whole
  kvobj allocation}`, `NOFORK_BUFFER {dbid, len, ~64 KB of pre-encoded records}`, `END`, `ABORT`.
  `ptr` inside a copied kvobj is valid in the fork because parent and fork share address-space layout
  at `fork()` and SNAP_FORK values are forkable.
- The fork is the single writer and merge point and owns SELECTDB tracking (§3), so multiple bg threads
  may feed it concurrently; the fork serializes writes.
- Diskless full sync: the fork writes the replica socket instead of a file. AOF rewrite: same scan,
  AOF encoders.

## 7. Correctness invariants (worst-first)
1. **Total clone-on-write for owed keys.** A bg thread reads LIVE parent memory for every kvobj header
   and every SNAP_NOFORK value; that read is correct only if the parent never writes a fork-instant
   header or SNAP_NOFORK value in place while its slot is `PENDING`/`SCANNING`. EVERY keyspace write
   path — SET/APPEND/SETRANGE/GETSET, HSET/HDEL and field-TTL, SADD/SREM, ZADD, list ops, INCR/DECR,
   EXPIRE/PERSIST, encoding conversions, DEL/UNLINK, lazy expiry on read, RESTORE-over-existing, COPY,
   MOVE, module writes, DEBUG mutators, the LRU/LFU clock update in `lookupKey`, active defrag — must
   go through the clone-on-write path in §4 (or be suspended). A miss = the bg serializes a
   mutated/torn value = silent snapshot corruption + a data race. Prove with: a written audit of every
   write path, AND a verification harness (capture a fork-instant reference of the keyspace under
   CLIENT PAUSE WRITE, drive background writers over every path during a save, reload the produced
   RDB, diff every key against the reference).
2. **Slot immutability while `SCANNING`.** The scanned dict, its headers and fork-instant SNAP_NOFORK
   values must not be mutated by anyone but through the temp-dict path; the bg thread must be strictly
   read-only on the keyspace.
3. **Consistent point-in-time enumeration.** Deletes tombstone (don't remove) and adds go to the temp
   dict, so the bg sees exactly the fork-instant contents of each slot.
4. **Class-change safety.** A SNAP_NOFORK value that becomes SNAP_FORK during a save is cloned into
   SNAP_FORK memory; its fork-instant SNAP_NOFORK original is what the bg serializes. Never let a
   SNAP_FORK value sit in the SNAP_NOFORK arena (the fork could not read it).
5. **FLUSHALL / FLUSHDB / SWAPDB during a save:** abort the save.
6. **Memory bound.** Clones + preserved originals for still-owed slots are bounded by
   write_rate x save_duration. Add a soft watermark (backpressure the scan) and a hard watermark that
   ABORTS the save rather than stalling clients.
7. **Arena isolation.** The fork/child must never allocate from, free into, or dereference the
   SNAP_NOFORK arena — that now includes every kvobj header, which is why SNAP_FORK keys are shipped
   as copies. Assert this where cheap. Parent side: SNAP_NOFORK frees must never enter a default-arena
   tcache (§2).

## 8. What must be built
`src/snapshot.c`: the SNAP_NOFORK jemalloc arena and the fast class-aware alloc/free API; placement of
all kvobj headers, SNAP_NOFORK values and kvstore tables into it; `snapClassify()`/
`snapEncodingIsNoforkClass()` and relocation-on-class-change (plus the optional kvobj class bit); the
background thread pool with slot ownership + the scan cursor; the per-slot temp dict and the
main-thread merge; the clone-on-write write-path hooks (invariant 1); the bg->fork channel with kvobj
copies and ~64 KB staging buffers; the fork-side writer with SELECTDB tracking; watermarks; the
verification harness.

## 9. Milestones (deliver M1, then stop and report)
- **M1 — prove the crux.** One background thread, whole keyspace as a single logical scan (slots
  scanned in sequence), defer all frees to save-end. Deliver: (a) RDB that reloads to a keyspace
  identical to stock BGSAVE's on a static dataset (byte-identity is not a goal: record order and
  SELECTDB placement differ); (b) under concurrent writes across every write path, reload-and-diff
  equals the fork-instant reference (the corruption/race test — the whole point); (c) FLUSH/SWAPDB
  abort cleanly; (d) confirm no CoW on kvobj headers, SNAP_NOFORK values or hash tables. Do NOT build
  multi-thread slot parallelism, mid-scan frees, diskless, or AOF yet.
- **M2** — multiple bg threads over disjoint slots; per-slot merge + mid-save frees; soft/hard
  watermarks; diskless full sync; AOF rewrite.
- **M3** — measurement vs stock BGSAVE: RSS/CoW under a write-heavy mix and a key-churn mix; parent
  main-thread CPU during the save (should be near zero — the walk is on the bg threads); fork stall;
  save wall-time; steady-state allocation/lookup overhead. Record the measurements.

## 10. Conventions
Local milestone commits are fine; do NOT push or open PRs without explicit approval. Run the tcl test
suite from a normal checkout path. Report honestly: if invariant 1 (total clone-on-write) or invariant
2 (slot immutability) cannot be met on some path, that is a valid spike-killing result and must be
surfaced, not worked around.

## 11. Implementation notes and corrections (added during M1 implementation, 2026-09-20)
- **Arena backing.** The SNAP_NOFORK arena is backed by ONE reserved virtual address range
  (`mmap(PROT_NONE, MAP_NORESERVE)`, `MADV_DONTFORK` applied once; extents are carved with a bump
  pointer and `mprotect(RW)`). Membership is therefore a two-compare range check
  (`zmalloc_is_nofork`), and the generic `zfree`/`zrealloc` keep arena memory in the arena. No class
  bit and no class-aware call sites are needed on free/realloc paths (§2 "free() is the hard part"
  is solved this way). jemalloc's `arenas.create` must not be used: it allocates the arena's own
  metadata through the extent hooks (making it DONTFORK and crashing the child's post-fork
  handler); `experimental.arenas_create_ext` with `metadata_use_hooks=false` is required.
- **All fork-based serializers must be hybrid.** Since every kvobj header is DONTFORK, no fork
  child can walk the keyspace. Stock BGSAVE, replication full sync (disk and diskless) and AOF
  rewrite therefore cannot use the stock serializer while placement is on; §9's M1 statement that
  replication keeps the stock path holds only for main-thread paths (SAVE, DEBUG RELOAD). A
  startup-only config `bgsave-hybrid` (default `no`) gates the arena and all placement. With it on,
  disk-based replication works through the hybrid BGSAVE file; diskless sync and AOF rewrite are
  M2 (§9) and are refused until then.
- **listpackEx.** The `listpackEx` struct is arena-born and never moved (its `ExpireMeta` is linked
  into the db's ebuckets). A LISTPACK_EX key is served by the background thread whenever its struct
  is in the arena, regardless of size. The struct's `meta` is written by ebuckets and read by
  `rdbSaveObject`, so the background serializer must compute the min field TTL from the listpack
  instead (see the write-path audit).
- **Encodings in this tree.** There are no `OBJ_ENCODING_TMPL_*` encodings; hashes are LISTPACK,
  LISTPACK_EX or HT. `OBJ_ARRAY` (sliced array) is SNAP_FORK.

### 11.x Additions after the sibling-branch comparison (2026-09-21)

- The hybrid path is gated by `bgsave-hybrid` alone. Without the SNAP_NOFORK
  arena (non-Linux, libc allocator, sanitizer builds) keys are classified by
  encoding and size, single-allocation values are always copied on clone, and
  the mechanics are identical; TSan and ASan run the whole path this way.
- `bgsave-hybrid-max-retained-bytes` bounds the memory the overlay keeps alive
  (replaced or tombstoned originals). Past it the save is aborted from
  serverCron and reported as a failed save; INFO exposes
  `snap_save_retained_bytes`, `snap_last_save_retained_peak`,
  `snap_hard_mark_aborts`.
- `RedisModule_Fork` is refused (EPERM) while the arena is on.
- The scan thread never reads a mutable main-thread global: `rdb-key-save-delay`
  (CONFIG SET reaches it through an apply hook), `maxmemory-policy` and
  `rdb-compression` are frozen per save; the LRU clock is computed, not read.
- Slots empty at the freeze instant are DONE immediately and never frozen.
- See `poc-hybrid-bgsave-m1-report.md` for the M1 evidence and go/no-go.
