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

Rewritten 2026-09-25 to describe the serialize-before-write scheme that replaced the frozen-dict
overlay (design and rationale in §11.z; the sync argument in §11.z.2). The original overlay text is
kept in §11.y for history.

**States.** Every frozen dict (one per db in standalone, one per slot in cluster) is in one of
`PENDING` (the scan thread has not reached it), `SCANNING` (being read), `SCANNED` (fully dispatched,
waiting for the main thread to release it) or `DONE` (released: side table applied, dict unfrozen).
The scan thread advances the state, the main thread reads it with an atomic load.

**Freeze.** At save start, on the main thread and before the fork: pause rehashing and forbid
auto-resize on every non-empty dict (`dictExpandIfNeeded` / `dictShrinkIfNeeded` return `DICT_ERR`
while paused, `kvstoreExpand` skips paused dicts), allocate one bucket bitmap per table of every
frozen dict, and install the overlay hooks. The bucket layout of a frozen dict is therefore fixed
for the whole save; the chains are not.

**The window.** The scan thread reads a dict one window of `W` consecutive buckets at a time
(`SNAP_WINDOW_BUCKETS_DEFAULT` = 1024, settable only through `DEBUG SNAP-WINDOW-BUCKETS n` for tests; no config option yet). Before reading
a window it publishes `(dict, table, lo, hi)` into a double buffer guarded by a seq_cst sequence
number, then **quiesces**: if a main-thread write section is open it waits until that section
closes (§11.z.2). Only then does it walk the buckets of the window, with atomic loads of the chain
pointers, skipping every bucket whose bitmap bit is set, and dispatches each entry (SNAP_FORK ->
kvobj copy message for the child; SNAP_NOFORK -> serialized on this thread into its staging buffer).
At the end of each window it forwards what the main thread queued (below). When the dict is done it
publishes `SCANNED`; when every dict is done it publishes the empty window, quiesces once more,
drains the queue a last time and sends END.

**Write sections.** The main thread opens a section at the first write hook of a command on a
frozen dict (`snapFunnelOpen`, seq_cst) and closes it when the outermost command ends
(`afterCommandEx` at nesting 0, `beforeSleep`, `moduleReleaseGIL`). Inside a section the window
cannot move past it, so a position computed in the section stays valid until it closes. The scan
thread never blocks the main thread; the main thread blocks the scan thread only for the length
of one command.

**Position and action.** Every write to a key of a frozen dict is preceded by the overlay's
`prepare` hook, which hashes the key to its `(table, bucket)` and, inside the section, classifies
it against the state and the window (`snapBucketOwed`):

| position | condition | action |
|---|---|---|
| behind | dict `SCANNED`/`DONE`, or same dict and `(table, bucket)` before the window | write in place (nothing is owed) |
| emitted | the bucket's bit is set, whatever the position | write in place (the thread skips the bucket) |
| inside | same dict, same table, `lo <= bucket < hi`, bit clear | never touch the chain: clone the key into the per-dict temp dict on first touch and apply the write there; deletes leave a tombstone, adds land in the temp dict |
| ahead | dict `PENDING`, window on another dict or table, or `(table, bucket)` after the window, bit clear | **emit first**: serialize the whole fork-instant chain of the bucket on the main thread (SNAP_NOFORK records into a mutex-guarded stage, SNAP_FORK keys as kvobj copies) into the main->thread queue, set the bit, then write in place |

The bit is set only by the main thread and only while the bucket is outside the window; it is read
by the scan thread only while the bucket is inside; the same bucket is never read by both at once.
Value mutations outside the write hook (in-place `APPEND`, `HSET` on a listpack, `INCR`) need no
protection: their bucket was emitted or walked before the command ran, and the thread never returns
to a bucket. Keys with module metadata, module-type values and everything larger than
`save-small-max-bytes` are SNAP_FORK and serialized by the child from the kvobj copy (§5).

**The side table** (TASK-1.21.3, 2026-09-30; `kvstoreDictOverlay`, reachable from the dict's
metadata `kvstoreDictMetaBase.ov`) holds the clones, adds and tombstones of keys written while their
bucket was inside the window. **Reads during a save** consult it first while it is non-empty and are
plain dict reads otherwise (`ovActive`), so a frozen dict with nothing pending costs a read nothing.
It is tiny: run 7 measured 0.6-11 MB of retained clones at 20-50k writes/s on a 300 MB dataset,
against 5-17 MB for the slot-lifetime overlay and 135-232 MB of child copy-on-write for a stock
fork; run 19 measured at most 1 MB.

**Drain.** An entry stays in the side table only while the walker may still read its bucket. At the
end of every write section (`snapFunnelCloseSlow`, i.e. the outermost command end or beforeSleep;
no caller holds a link there) and once per cron tick, the main thread applies every entry whose
bucket is now behind the window (`snapOverlayDrainable`: position `BEHIND`, or the dict `SCANNED`):
a clone replaces its original at the link and the original is freed (SNAP_NOFORK memory: DONTFORK,
so the free is CoW-free), a tombstoned original is unlinked and freed, an add is inserted
(`kvstoreOverlayDrain`, over the store's list of dirty overlays only). The drain is skipped while the
window has not moved since the last one. **Release.** When the thread publishes `SCANNED` for a
dict, the main thread (from the event loop, `snapScanHandleCompletions`) applies whatever is left
(normally nothing), resumes rehashing and auto-resize, frees the bitmap and marks the dict `DONE`
(`kvstoreOverlayRelease`). There is no merge step and no `hybrid-merge` latency event any more.
From then on writes to it are stock writes. INFO `snap_save_side_entries` counts what is pending.

**Backpressure and abort.** The main thread's queue is bounded only by the hard mark: `snapSaveCron`
aborts the save when retained clones plus queued bytes exceed `bgsave-hybrid-max-retained-bytes`
(default 256 MB, 0 = unlimited). Abort marks the context, joins the thread (which gives up its
quiescence wait when it sees the abort, since the aborting command may itself hold the section
open), kills the child, releases every side table and unfreezes. FLUSHALL, FLUSHDB, SWAPDB, SHUTDOWN
and a killed child take the same path. The previous dump file is untouched.

**Other fork children (fallback).** The arena is one reserved range carrying
`MADV_DONTFORK`. Any child that is not the hybrid save's (stock RDB save while the
feature is off, diskless full sync, AOF rewrite, module fork, Lua debugger) needs
the keyspace mapped, so `redisFork()` first flips the range to `MADV_DOFORK` (one
madvise, O(VMAs), no key is touched) and `resetChildState()` flips it back once
no child is left and the feature is on (reaped or killed children alike). Such a child sees stock copy-on-write on the
arena for its lifetime. Mutual exclusion with the hybrid save is the existing
one-child rule; a `BGSAVE` issued while a fallback child runs is scheduled and
starts, on the hybrid path, after the flip back. A Lua debugger child is not
exclusive: a hybrid save started during a debug session maps the arena in its
child (copy-on-write on it for that save only, still correct). `CONFIG SET bgsave-hybrid` uses
the same flip and is refused while any child or hybrid save runs.

**What the main thread pays.** One hash and one atomic load per write to a frozen dict, one dict
lookup in the temp dict, and, for the first write into each not-yet-walked bucket, the
serialization of that bucket (about 1.3 keys on average at load factor 1). Measured main-thread CPU
during a save equals stock in every cell of runs 7 and 8; worst PING during a save is the fork stall
(0.3-0.7 ms), not the scheme.

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
- **Outbound queue in front of the pipe (TASK-1.27, 2026-09-29).** The child reads one message at a
  time and does not touch the pipe while it serializes a fork-class value, so with a blocking
  `write()` the scan thread stalled behind every big value (mixed1m: save = scan + child instead of
  max, run 13). The scan thread now owns a queue of complete messages and writes the pipe
  non-blockingly after every enqueue (partial writes keep an offset into the head message); the
  staging buffer reserves its header up front and is handed to the queue without a copy, and
  drained buffers return to a small pool. Past `bgsave-hybrid-max-queued-bytes` (its own knob,
  default 16 MB, frozen at save start; 128 MB buys the full overlap on datasets of hundred-megabyte
  values, run 19) the thread waits for the child with `poll()` instead of
  growing; it never aborts for that. The main-thread hard mark (`bgsave-hybrid-max-retained-bytes`,
  256 MB) still counts only what the main thread retains and queues, so the worst-case save memory
  is the sum of the two bounds. `snap_save_queued_bytes` covers both queues;
  `snap_last_save_queued_peak` is their peak over the last save. Cost: the queue is memory the save
  holds while the child is behind, up to its bound (run 16 measured 250 MB on mixed1m with the
  queue bounded by the 256 MB retained limit, where before the change the save held about 1 MB;
  the separate 128 MB default halves that for a few seconds of save time).
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
  arena (non-Linux, libc allocator, sanitizer builds, and 32-bit builds: the
  arena is a 64 GB to 16 TB address-space reservation, so `HAVE_NOFORK_ARENA`
  requires a 64-bit pointer size, 2026-09-28) keys are classified by
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

### 11.y Per-key scan generation (2026-09-21; superseded by 11.z)

`kvBits.save_gen` (3 bits, upstream kvBits byte) records the generation of the
hybrid save whose scan thread serialized the key. `server.snap_gen` bumps at
every hybrid save start before the freeze; the scan thread stamps a key as its
last touch (release store); a write lookup on a frozen slot checks the mark with
an acquire load and, when it matches, hands out the frozen entry itself: the
write is applied in place, no clone, no temp entry, no retention, nothing to
merge. Deletes still tombstone (with a header-only clone that owns and frees
the value at once), adds still go to the temp dict. Marks are trusted only while
fewer than 7 saves ran without one completing; otherwise the save clones
everything as before and its completion resets the counter. kvBits is accessed
only through byte-wide atomics. INFO exposes `snap_gen`,
`snap_gen_since_success`, `snap_save_marks_valid`, `snap_save_inplace_writes`.

### 11.z Serialize-before-write with a bucket window (design for the next spike, 2026-09-21)

Proposed by the user after the generation-mark results; replaces the frozen-dict
overlay of §4 (temp dicts, tombstones, merge) and the per-key generation of
§11.y. Precedent: Dragonfly's bucket versioning (a write to a bucket whose
version is older than the running snapshot first serializes that bucket, then
proceeds in place). Our differences: the walker is a separate thread, so a
published bucket window keeps it off the chains the main thread mutates; the
"already in the snapshot" state is a per-save bitmap with one bit per bucket
rather than a 64-bit version per bucket; and SNAP_FORK values are still owned
by the fork child, so the main thread never serializes a big value.

**Invariants (worst-first).**

1. Every key present at the fork instant is emitted exactly once, by bucket:
   a bucket is emitted either by the walker when the window reaches it or by
   the main thread before the first post-fork write to it, never both, never
   neither. The arbiter is the bucket's "emitted" bit: set only by the main
   thread for buckets ahead of the window, read only by the walker for buckets
   inside it, and the walker's cursor itself for buckets behind it.
2. The walker traverses only the chains of the buckets in the published window
   W = (dict, table, [lo, hi)). The main thread makes structural changes to a
   dict under a save (add, unlink, replace of an entry pointer) only in buckets
   outside W; changes to buckets inside W are deferred to a small side table.
3. The main thread never blocks on the child. Its records go to the walker
   through a queue; the walker drains that queue before its own work.
4. Bucket layout of a dict under a save is stable: rehashing paused, expand
   and shrink forbidden for that dict until the save ends (including the
   kvstore resize cron). A dict mid-rehash at freeze keeps both tables; the
   window names the table and each table has its own bitmap.

**Main thread, at the write funnel** (the kvstore write lookup, add, unlink and
set-at-link entry points, i.e. the hooks of the write-path audit). It knows the
dict, table and bucket b of the key (hash and mask). Under an active hybrid
save:

- Read W (see the handshake below).
- b behind W (already walked): write in place, nothing else.
- b ahead of W: if the bucket's bit is clear, emit the bucket's whole chain
  first (SNAP_NOFORK class: `rdbSaveKeyValuePair` with the key's TTL into the
  main thread's own staging buffer; SNAP_FORK class: a `SNAP_MSG_KVOBJ` header
  copy, the value pointer refers to memory the child owns since the fork), then
  set the bit. Then apply the operation in place: mutate, unlink and free, or
  add. This holds for adds too, so a post-fork key never lands in a chain that
  is still to be emitted. Small values are listpack, intset or short strings,
  which RDB writes as raw blobs, so an emission is a bounded memcpy per key
  (`save-small-max-bytes`); a chain is about one key at load factor 1. Deleting
  a SNAP_FORK value in place is fine, the child has its own copy of the pages.
- b inside W: do not emit (the walker is emitting it) and do not touch the
  chain. Behave as the overlay does today, bounded to the window: duplicate
  into a side table on modify, tombstone on delete, add to the side table on
  add. Lookups consult the side table first while it is non-empty. The main
  thread drains the side table (replace in chain and free the original, unlink
  tombstoned originals, add pending keys) at the next funnel entry or cron tick
  once W has moved past those buckets. The originals kept alive by the side
  table are the only retained bytes left.

Value mutations outside the funnel (the command body writing into the value it
looked up) need no protection: the bucket was emitted or walked before they
start and the walker never reads it again. Only chain writes are structural and
those happen inside the funnel.

**Walker.** For each dict and table, publish W of K buckets, wait for
quiescence, then walk the chains sequentially with atomic loads of the chain
pointers (no reverse-binary cursor is needed with rehash paused): a bucket whose
bit is set is skipped, the others are emitted. Before advancing, drain the
main-thread queue, then publish the next window. Standalone mode thus gets
fine-grained release for free; cluster mode windows span the 16,384 small dicts
(a window is often a whole dict).

**Handshake.** No acknowledgement from the main thread (that stalls the walker
on an idle server). Instead a quiescence sequence: the main thread increments an
atomic counter when a write section opens (odd = inside) and closes, and reads
W after the opening increment; the walker stores the new W, loads the counter,
and if it is odd waits until it changes. Both pairs are seq_cst (store-then-load
on each side is the store-buffer pattern; acquire/release alone is not enough).
A write section opens at the first write funnel of a command and closes when
the outermost command returns, at beforeSleep, and when a module thread
releases the GIL, so a link obtained by a write lookup stays valid until the
matching set-at-link: the window cannot move under it. Cost: two seq_cst
operations per write section, only while a hybrid save is active. Possible
later optimisation: an asymmetric fence (`membarrier`) on the walker side.

**Bitmap lifecycle.** Allocated at freeze for every frozen table (one bit per
bucket, about 256 KB for 2M buckets), written with atomic byte ops so a window
boundary inside a byte is race-free, freed when the save ends or aborts. Nothing
about a save survives it, so there is no generation to wrap and no trust rule.
`kvBits.save_gen` and the byte-wide kvBits atomics of §11.y are removed; kvBits
returns to the upstream layout.

**Queue and backpressure.** Main to walker: a list of filled staging buffers,
pushed when full or at command end. The walker drains it before every window
advance and before END. When the child stalls the list grows on the heap; its
bytes count as retained and `bgsave-hybrid-max-retained-bytes` aborts the save
as today. Expected volume is small: at 50k writes/s over a 1.5 s save on the
small dataset, at most about 75k first-writes of roughly 80 bytes, some 6 MB
against 300 MB of walker output.

**What this removes.** The frozen-dict invariant of §4, the kvstore overlay
(temp dicts, state dict, tombstone placeholders, merge, `hybrid-merge` latency
event), retained accounting beyond the side table and queue, the per-key
generation marks with their atomics and trust counter. Eviction and expiry of
unwalked keys now free memory immediately (M1 limitation 1 disappears). Reads
during a save are plain in-place reads except for the side-table check while it
is non-empty.

**What it keeps.** Arena and placement, scan thread and 64 KB staging protocol,
pipe and child (SELECTDB/RESIZEDB/SLOT_INFO tracking, `dismissObject`), the
funnel hooks in kvstore.c, the write-path audit table, the tcl harness (the
reload step already fails loudly on a duplicate key; a missing key fails the
digest).

**Costs and risks to measure in the spike.** Main-thread emission of a bucket
chain on the first write to it (about a microsecond per key, replacing today's
clone; an add to a fresh bucket pays it too); the seq_cst pair per write section
during saves; load factor drift under the resize lockout for long saves; the
walker's wait on a main thread inside a long write section (bounded by one
command). Order of work: standalone single db first, TSan and the harness on
every step, cluster slots last.

### 11.z.1 Status of the serialize-before-write spike (2026-09-22)

Done on branch `snapshot-hybrid-bgsave` (commits 2e69194bc, f6ee1fcba,
3da2cd28a; backlog TASK-1.21.1 and 1.21.2):

- Bucket window published by the scan thread, write sections on the main
  thread (opened by the overlay's first write hook, closed at outermost
  command end, beforeSleep and module GIL release), seq_cst handshake, chain
  walk with atomic loads, per-save bucket bitmaps, resize lockout for frozen
  dicts (cron, `kvstoreExpand`, insert path), `DEBUG SNAP-WINDOW-BUCKETS`.
- Main-thread emission: the overlay's `prepare` hook places the key's bucket
  relative to the window; behind or already emitted -> in place; ahead ->
  emit the whole chain (NOFORK records serialized on the main thread into a
  locked stage, FORK keys as kvobj header copies), set the bit, in place;
  inside -> temp dict as before. The scan thread forwards the queue at every
  window boundary and before END. Adds, deletes, evictions and expiries of
  unwalked keys are in place, so their memory returns immediately.
- Verified: unit tests, the whole tcl harness (randomized race, eviction
  variants, cluster mode, child killed, hard mark, SHUTDOWN), TSan clean for
  the window layer, callgrind unchanged with the feature off.

Left over, in the order I would do it:

1. TASK-1.21.4: remove `kvBits.save_gen`, the kvBits atomics, the trust
   counter and their INFO fields; the scan thread still stamps every key it
   emits (dead work, about one release store per key). Expose bitmap bytes in
   INFO.
2. TASK-1.21.3 (done 2026-09-30): the side table drains at the end of every
   write section and per cron tick for buckets the window has passed; slot
   completion is a release (no merge, no `hybrid-merge` event); reads take the
   stock path while the side table is empty; the overlay lives in the dict's
   metadata (`kvstoreDictMetaBase.ov`, the home TASK-1.21.6 attaches on
   demand). Kept as is: the temp dict + state dict pair as the side table's
   representation (a working, sanitizer-clean structure; measured cost about
   nothing), and in-window deletes still clone the key only to tombstone it.
3. Cost trims: `dictKeyBucket()` hashes the key a second time per write
   during a save (reuse the lookup's hash); `snapSendMsg` does two `write()`s
   per message (writev); the walker's `snapWalkerQuiesce` spins with
   `sched_yield` then 20 us sleeps (fine, but an asymmetric fence would make
   the main-thread side plain stores); `snapKvobjIsOwed()` opens a write
   section from the hash field-TTL read paths.
4. Not measured yet (TASK-1.21.2 AC 3-5): used_memory dropping during
   eviction under a save; behaviour with a stalled child (queue growth is
   accounted and hard-marked, not exercised by a test); the per-write cost of
   a main-thread emission in isolation (the benchmark's main-thread CPU
   seconds are the proxy).
5. TASK-1.21.6: cluster mode is exercised by the harness's cluster test
   (the window names the dict, SLOT_INFO comes from the message header) but
   has no dedicated verification of main-thread emission per slot.
6. AOF rewrite, diskless sync and `RedisModule_Fork`: no longer refused (TASK-1.22,
   2026-09-25). `redisFork()` flips the arena's reservation to `MADV_DOFORK` for any
   non-hybrid child (one madvise; `snapArenaBeforeFork`), the reaper flips it back
   once no child is left (`snapArenaChildExited`); the fallback child pays stock
   copy-on-write on the arena. `bgsave-hybrid` is a runtime config on the same
   mechanism (off = arena forkable, stock save path, and `zmalloc_nofork_active` = 0 so
   every arena entry point is a plain `zmalloc`: headers, dict tables and values allocated
   while off have the stock layout; blocks already in the arena stay there and are freed
   by address), refused while any child or hybrid save runs. `INFO snap_nofork_arena_forkable`
   shows the state. Atomic slot migration during a save is still untested (cluster
   is TASK-1.21.6).
7. Active defrag and the arena (2026-09-25, found by the full suite with `bgsave-hybrid
   yes`): the arena's explicit per-thread tcache only receives jemalloc GC events through
   its own allocations. With an idle keyspace, or during an active-defrag pass (which
   bypasses tcaches), its cached regions pinned their slabs indefinitely and defrag could
   not bring `allocator_frag_ratio` down (12.5 MB of the 12.9 MB it could not remove were
   arena slabs, 325 M key misses). Fix: `serverCron` flushes the arena tcache once a
   second (`zmalloc_nofork_tcache_flush`), in addition to the flush `jemalloc_purge`
   already did on FLUSHALL/lazyfree/MEMORY PURGE. All 21 defrag tests then pass with the
   feature on. Eleven of them manufacture fragmentation by interleaving keys with
   scripts, channels, argv or field-expiry structures in the same slabs: with the keyspace
   in its own arena that cannot fragment anything (a real property of the design), so those
   tests run with the feature switched off at runtime.
8. Per-key generation removed (TASK-1.21.4, 2026-09-25): `kvBits` is back to the upstream
   layout (2 bits sds type, no_evict, 5 unused) with plain field access; no byte-wide
   atomics remain on the kvobj creation path, `snapKvobjScanned/MarkScanned`, `snap_gen`,
   `snap_gen_since_success` and `snap_save_marks_valid` are gone. The per-save bucket
   bitmap is the only "already in the snapshot" state (INFO `snap_save_bitmap_bytes`
   while a save runs, 0 otherwise); `snap_saves` counts hybrid saves started.

Found while comparing with the teammate's POC (2026-09-24), both fixed and
covered by a harness test with `test_keymeta.so`: (a) placement ignored module
key metadata, so a small value whose key later received metadata stayed in the
arena while both emitters routed the key to the child (which serializes module
metadata through module callbacks); such values now always leave the arena,
including the listpack-ex struct, and the placement scan reports them as
violations. (b) An EMBSTR value shipped to the child as a kvobj copy kept its
parent-side pointer into the copied allocation; KVOBJ messages now carry the
source address and the child relocates it. Both were unreachable before, since
the only route of a small value to the child is module metadata.

### 11.z.2 How the scan thread and the main thread agree on which buckets the thread reads

The unit of coordination is a bucket window: a range of consecutive buckets
`[lo, hi)` of one table of one frozen dict (1024 buckets by default). Three
pieces of shared state, all lock-free:

1. **The window** (`snap_win`, published by the scan thread). Before reading a
   window the thread fills the spare of two window slots and publishes its
   sequence number with a seq_cst store. Sequence 0 or a NULL dict means "no
   window": nothing is being read.
2. **The write-section counter** (`snap_funnel_seq`, owned by the main thread).
   Odd means the main thread is inside a keyspace write section. A section
   opens at the first overlay write hook of a command (seq_cst increment) and
   closes when the outermost command returns, at `beforeSleep`, and when a
   module thread releases the GIL. Links into a frozen dict are never held
   across a section boundary.
3. **The per-save bucket bitmaps**, one bit per bucket of every frozen table,
   allocated at freeze and freed when the slot completes.

**Handshake.** After publishing a new window the thread loads the counter
(seq_cst). If it is odd it waits until the value changes, then reads the
window. Because both sides store then load with seq_cst, a section that started
before the publication is always seen by the thread and waited for, and a
section that starts after it always sees the new window. The thread gives up
the wait only when the save is aborted, since the aborting command may itself
hold the section open while joining the thread. An idle server never stalls
the thread: no section, no wait.

**Bucket position, decided by the main thread inside a section.** For a key it
is about to write structurally (add, unlink, replace) it derives the key's
`(table, bucket)` and compares with the slot state and the window:

| position | condition | action |
|---|---|---|
| behind | slot SCANNED/DONE, or same dict and `(table, bucket)` before the window | write in place, nothing to emit |
| inside | same dict, same table, `lo <= bucket < hi` | if the bucket's bit is set: write in place (see below); else clone into the temp dict (never touch the chain) |
| ahead | slot PENDING, window on another dict, or `(table, bucket)` after the window | if the bucket's bit is clear: serialize its whole chain into the queue and set the bit; then write in place |

The bit is consulted whatever the position (`snapBucketOwed`): within one
section the first look may find a bucket ahead and emit it, then the thread
publishes the next window over that bucket and waits for the section to close;
a second look in the same section (another key of the chain, or the debug
check in `dbSetValue`) finds it inside. The thread will skip it by its bit, so
it is not owed and may be written in place. Run 7 of the benchmark hit exactly
this in a `DEBUG_ASSERTIONS` build at 50k writes/s (`db.c:618`), which is how
the position-only check was found (2026-09-25).

**Why it is race-free.** The thread reads chain pointers only inside its
window, with atomic loads. The main thread writes chains only outside the
window and only inside a section, which the thread cannot overtake. A bucket's
bit is set by the main thread only while the bucket is ahead of the window and
read by the thread only while the bucket is inside it, so the same bit is never
accessed by both at once (byte-wide atomics cover a window edge inside a byte).
Value mutations outside the section need no protection: the bucket was emitted
or walked before they start and the thread never returns to it. The thread
skips buckets whose bit is set and forwards the main thread's queue at every
window boundary; before END it publishes the empty window, waits for the open
section to close, and drains once more, so nothing emitted can be left behind.

Rehashing is paused and expansion forbidden for a frozen dict for the whole
save (cron, `kvstoreExpand` and the insert path), so bucket indices are stable;
a dict mid-rehash at freeze keeps both tables and the window names the table.

### 11.z.3 Comparison with the teammate's forkless-hybrid POC (yinon/forkless-hybrid, 2026-09-23)

Same goal, and the same two tools: a MADV_DONTFORK jemalloc arena for small
values and a parent thread that serializes them while the fork child writes
big values and owns the RDB. The split of responsibilities differs:

| | forkless-hybrid (theirs) | snapshot-hybrid-bgsave (ours) |
|---|---|---|
| Enumerates the fork-time keys | the child, walking its CoW copy of the dicts; sends `{kvobj*, dbid}` back over a work pipe (256 x 16 B batches) | a parent scan thread, walking the live dicts under a published bucket window |
| Dict tables, big-key headers | forkable (default arena / arena A): stock-size fork stall, table and header CoW on every write | DONTFORK arena: 0.7 ms stall on 300 MB, no table or header CoW |
| "Small" | INT/EMBSTR/RAW strings up to 500 KB and listpack(-ex) hashes; small lists, sets, zsets, streams go to the child | any single-allocation encoding up to 4 KB (strings, listpack list/set/zset/hash, intset, listpack-ex) |
| Write to an unsaved small key | pin: retain by refcount (immutable strings), type Dup copy, or pre-encoded bytes; memory held until the helper reaches the key (evicted keys jump the queue); two budgets, abort when exceeded | serialize the bucket's chain into the queue, then write in place; retention only for buckets inside the window being read |
| "Already saved" | 2-bit alternating round in the kvobj header (took 2 of the 8 module metadata classes), recovery mode after a failed save | per-save bucket bitmap plus window position; nothing outlives the save |
| Thread sync | one mutex and condvar; each side publishes the one object it reads; the main thread waits if the helper is reading that object | the main thread never waits; the scan thread waits for the open write section at each window boundary |
| Pipes and framing | three pipes (work, data, status); one `SELECTDB` per record; the child relays parent frames only when its work pipe blocks or at the end; 64 KB pipes, 10 ms poll | one pipe parent to child, 1 MB, fed from a bounded in-memory queue on the scan thread (non-blocking writes); whole 64 KB buffers or header copies; `SELECTDB` only on change |
| Mode | `bgsave-mode fork|hybrid|forkless` switchable at runtime by flipping the arena's madvise over a registry of up to 8192 extents; a fully fork-free mode too | `bgsave-hybrid` immutable at startup |
| Rehash/resize during a save | unconstrained (the child has a frozen copy) | frozen dicts locked (rehash paused, no resize) |
| Replication, AOF | disk and diskless replication through the relay; no AOF in non-fork modes | disk RDB only; diskless sync and AOF rewrite are M2 |
| Tests | 15 tcl suites, C unit tests, a python fault-injection harness for the arena | one tcl suite with a randomized race harness, C unit tests, TSan/ASan runs |

Where theirs is ahead: runtime mode switching, replication coverage,
no resize lockout, O(1) retain-by-refcount instead of main-thread
serialization, eviction integration (release pins before evicting, evicted
keys serialized first), memory budgets with reserve-before-allocate
accounting, richer INFO, test breadth, and the parent publishing the RDB
only after both sides succeed.

Where ours is ahead: fork stall and metadata CoW (their tables and headers are
forked, so their stall and small-key CoW should be stock-like; to be measured
with their binary in `utils/snap-bench`), the main thread never blocks on the
helper, no child-to-parent-to-child round trip per small key, more small
types offloaded, no per-record `SELECTDB`, no O(N) normalization after a
failed save, and the write path serializes instead of retaining.

Ideas to take:

1. **madvise flip as the fallback path.** Our arena is one reservation, so
   `MADV_DOFORK` on it is a single call. Flipping it for the duration of an
   AOF rewrite or a diskless sync makes the stock fork paths work unchanged
   (at stock CoW cost) until the hybrid versions exist; it would also make
   `bgsave-hybrid` mutable at runtime.
2. **Eviction integration**: frozen LRU/LFU clock and policy per save (we have
   it), plus a check of the eviction deficit after freeing snapshot memory.
3. **Their test scenarios as a checklist**: address reuse after DEL, PFCOUNT
   in-place RAW mutation, XREADGROUP through read lookups, cap crossing via
   APPEND/SETRANGE, diskless digest under writes and eviction, cancellation
   while the helper is blocked, failed publication keeps the old RDB.
4. **INFO counters** for main-thread time spent in emission and for waits.
5. **Their benchmark spec and runner** as the shared harness for a three-way
   comparison (see `tools/forkless-benchmark` on their branch).

Not to take: one `SELECTDB` per record, relay only on EAGAIN through small
pipes, the 8192-extent registry cap, giving up module metadata classes for
marks, an O(N) main-thread normalization after failures.

### 11.z.4 Modules under a hybrid save: what runs where, and the contract (2026-09-26)

Precedent: Valkey's `VALKEYMODULE_OPTIONS_HANDLE_FORKLESS` (valkeymodule.h, an
opt-in bit; `moduleAllModulesHandleForkless()` in module.c; `resolveBgsaveType()`
in rdb.c falls back to a fork save with a warning when any loaded module lacks
it). Valkey needs it for two reasons: its forkless save changes main-thread
semantics (`VM_OpenKey` in WRITE mode may return NULL while a key is write-locked)
and its save callbacks may run off the main thread. Neither applies here, so our
contract is narrower, but the child side is stricter than stock.

**What runs where.**

- Module code never runs on the scan thread. `OBJ_MODULE` values and every key
  carrying module key metadata are SNAP_FORK (`snapPlaceValueArena`,
  `snapKeyClass`): the fork child serializes them through the type's `rdb_save`
  and the metadata save callbacks, from the kvobj header copy it receives over
  the channel (`snapSaveChildWrite` -> `rdbSaveKeyValuePair`), exactly as the
  stock child does. `aux_save`/`aux_save2` run in the child from
  `rdbSaveRioHeader`. Persistence events fire as in stock: `RDB_START` in the
  child (`startSaving`) and so are `ENDED`/`FAILED` (`stopSaving` runs in the
  child too); `FORK_CHILD_BORN`/`DIED` fire in the parent only (`redisFork`,
  `receiveChildInfo`). Module timers, keyspace notifications and module threads
  do not exist in the child: fork copies only the calling thread and the child
  never runs the event loop. `pthread_atfork` child handlers of linked libraries
  do run there (libuv, Rust `rand`: they reset locks and counters, no keyspace).
- Module allocations (`RM_Alloc` and friends, `RedisModuleString`s, module dicts)
  are default-arena `zmalloc`: forkable, mapped in the child. Only kvobj headers,
  the keyspace and expires dict tables and entries, and small values live in the
  `MADV_DONTFORK` arena (the `*_nofork` allocators, reachable from object.c,
  dict.c and kvstore.c only). A module cannot allocate there.
- The one difference from stock: **the hybrid child has no keyspace.** Dict
  tables, kvobj headers other than the copy handed to the save callback, and
  small values are unmapped in the child. Dereferencing them is a SIGSEGV: the
  child dies, the save is reported as "terminated by signal 11" and the parent
  is unaffected, but persistence silently stops working until someone reads the
  log. In the stock child the same code reads a stale but valid copy.

**Contract for a module to be compatible with `bgsave-hybrid yes`.**

1. Save-side callbacks (`rdb_save`, `aux_save`, `aux_save2`, key-metadata save,
   and a `PERSISTENCE` event handler, which runs in the child for
   `RDB_START`/`ENDED`/`FAILED`) must not touch the keyspace: no `RM_OpenKey`,
   `RM_KeyExists`, `RM_Scan`, `RM_ScanKey`, `RM_Call`, `RM_DbSize`,
   `RM_RandomKey`, `RM_GetExpire`, `RM_ListPush`... on any key. Allowed: the
   value passed to the callback, module-owned memory, and the IO context
   (`RM_SaveString*`, `RM_GetKeyNameFromIO`, `RM_GetDbIdFromIO`,
   `RM_GetContextFromIO` for `RM_Log`). The key name in the IO context points
   into the header copy and is valid for the callback only, as in stock. (The
   IO context has no client, so `RM_OpenKey` through it dereferences NULL in
   the stock child as well; a module that reaches the keyspace from a save
   callback does so through a detached thread-safe context, which is what the
   test module does.)
2. Module-owned state must not keep pointers into Redis-owned key memory and
   dereference them in a save callback: a `char *` from `RM_StringDMA` or a
   `RedisModuleKey *`. Both are already invalid past the command by the API
   contract; the difference is that in the hybrid child they fault.
3. Main-thread rules are the existing API rules, restated because the funnel
   relies on them: mutate a key only through a handle opened with
   `REDISMODULE_WRITE` (the write lookup emits or clones the owed bucket, so a
   later in-place write through the `RM_StringDMA` pointer, `RM_StringTruncate`,
   `RM_SetExpire`, list/zset/hash/stream APIs and `RM_ModuleTypeReplaceValue` is
   covered: "value mutations outside the section need no protection"), and from
   a module thread only under the GIL (the write section closes in
   `moduleReleaseGIL`). Gap on our side, closed by TASK-1.25: `RM_SetLRU` /
   `RM_SetLFU` are allowed on a READ handle (the stock test module uses
   READ|NOTOUCH so that the open does not touch the idle time first), and
   wrote `kv->lru` while the scan thread may read it for the IDLE/FREQ opcode.
   They now hand an owed key to the write funnel first (a write lookup, which
   emits or clones the bucket and refreshes the handle's pointer), the same
   thing a WRITE handle does at open time.
4. Behaviour a module can observe on the main thread during a hybrid save:
   `RM_ResetDataset` (`emptyData`) aborts the save and kills the child (stock:
   the child finishes the pre-flush dataset); `RM_Call` of FLUSHDB or SWAPDB
   aborts it too (TASK-1.24 will detach the frozen kvstore instead). `RM_Fork`
   during a save is refused by `redisFork`'s child exclusivity as in stock;
   outside a save it works and the arena is flipped forkable for that child
   (§11.z.1 item 6). `REDISMODULE_CTX_FLAGS_ACTIVE_CHILD` is set for the whole
   hybrid save, so `lookupKey` keeps skipping LRU/LFU updates as in stock.
5. Cost, not a restriction: a key with module key metadata is SNAP_FORK whatever
   its value size, so it pays stock copy-on-write and does not benefit from the
   arena. Module values themselves are big-value class by definition.

**Enforcement (implemented, TASK-1.25, 2026-09-30).** Following Valkey: an opt-in
module option `REDISMODULE_OPTIONS_HANDLE_HYBRID_BGSAVE` (`1<<4`,
`_REDISMODULE_OPTIONS_FLAGS_NEXT` -> `1<<5`) meaning "my save-side callbacks obey
items 1 and 2". A module is *relevant* when it registers a data type, a key
metadata class, or a PERSISTENCE / FORK_CHILD server-event handler: those are
the callbacks the save child runs. A relevant module without the bit blocks the
hybrid path: `rdbSaveBackground` and `startBgsaveForReplication` consult
`moduleAllModulesHandleHybridBgsave()` and take the stock fork child instead
(the §11.z.1 item 6 flip makes this free), with `LL_WARNING` at module load
(when `bgsave-hybrid` is on) and again when the blocking module changes at save
time, and INFO persistence `snap_hybrid_blocked_by:<name>` naming it, so the
fallback is never silent. Modules with none of those callbacks need no
declaration (nothing of theirs runs in the child). The bundled vector-sets
module and the test modules with data types, metadata classes or persistence
handlers declare the bit. `MODULE LOAD` during a hybrid save stays allowed.
Defence in depth in the child (`snapInHybridChild()`): `RM_OpenKey` returns
NULL, `RM_KeyExists`/`RM_DbSize` 0, `RM_RandomKey` NULL, `RM_Scan`/`RM_ScanKey`
0 with `EPERM`, `RM_GetExpire` no-expire, `RM_Call` NULL with `EPERM` (or an
error reply with the `E` flag), each logged once per API per child, so a
violation by an opted-in module produces a message and a save that continues
with the module's own data instead of a SIGSEGV. `RM_SetLRU`/`RM_SetLFU` on
a READ handle go through the write funnel (item 3's gap; requiring WRITE was
the plan, but it breaks the stock `misc.c` module tests and any module that
sets idle time through READ|NOTOUCH). Test module
`tests/modules/hybridviolate.c` breaks item 1 on purpose to cover both the
fallback and the guards, and exercises SetLRU/SetLFU on an owed key.

**Audit of RedisJSON and RediSearch against the contract (2026-09-26, trees
~/git/RedisJSON e1c8194 and ~/git/RediSearch c14cf7ffb).** Both satisfy items 1
to 3 as they are; only the opt-in bit would be missing. RedisJSON: one save-side
callback, `rdb_save` (redis_json/src/redisjson.rs), encodes the value it is
given; no aux save, no persistence or fork-child event handlers; writes go
through WRITE handles (`set_value`); no `SetLRU/SetLFU/StringDMA/ResetDataset`.
RediSearch: `Indexes_RdbSave/Indexes_RdbSave2` (aux, BEFORE_RDB) walk the
module-owned spec dictionary and serialize fields, schema rule, stopwords,
synonyms, aliases and the disk spec; `SpellCheckDictAuxSave(2)` and
`TrieType_RdbSave` serialize module-owned tries; `GenericType_DummyRdbSave`
aborts by design. `RedisModule_GetContextFlags` on the IO context is the only
server call. Every `RedisModule_OpenKey/Call/Scan` in the tree is in indexing,
query, notification and suggestion code (spec.c: `IndexSpec_TimedOutProc`,
`Indexes_ScanAndReindexTask`; module.c, document_*.c, suggest.c, redis_index.c,
rlookup.c, notifications.c), none reachable from a save callback. The fork GC
(`fork_gc.c`, `RedisModule_Fork`) reads inverted indexes in its own child; with
the arena flip it behaves as under stock. Method: grep of the keyspace APIs per
file intersected with the save-path call tree; the leaf serializers
(field_spec, rules, stopwords, synonym_map, hiddenstring, search disk) contain
no keyspace calls.

Neither RedisJSON nor RediSearch subscribes to `PERSISTENCE`; RediSearch's
subscriptions (ModuleChange, FlushDB, Loading, Shutdown, Sharding, Config,
ClusterSlotMigration, ReplBackup, ReplicationRoleChanged) and its timers (GC
scheduling, index timeout `IndexSpec_TimedOutProc` which does open keys,
topology refresh, trimming) fire only from the parent's event loop. Its fork GC
child calls only `ThreadSafeContextLock/Unlock`, `ExitFromChild` and
`KillForkChild` besides reading inverted indexes.

Side finding, not module related: a crash inside the hybrid child runs
`logServerInfo` -> `genRedisInfoString`, whose Keyspace section reads
`kvstoreSize(server.db[j].keys)` from the unmapped arena and faults again; the
re-entry guard in `sigsegvHandler` then prints the reduced report, so a child
crash log is truncated but produced. Skipping the keyspace section when
`server.in_fork_child` is a hybrid child would keep the full report.
