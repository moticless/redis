# Hybrid snapshot

Hybrid snapshot is a `fork()`-based snapshot that keeps the fork for the values it is good at and
keeps everything else out of the child. This document describes the design as implemented, for
a reader who knows Redis internals (kvstore, dict, rdb.c, the fork child) but has not seen this
code. It is the reference; `src/snapshot.c` carries a shorter overview in its file header and
`poc-hybrid-bgsave-spec.md` is the design history.

Contents: 1 problem and classes, 2 one save end to end, 3 the write funnel and the bucket overlay,
4 memory model and bounds, 5 entry points, 6 interactions, 7 module contract, 8 observability,
9 glossary, 10 invariants.

## 1. Problem and the two classes

Classic BGSAVE forks and the child serializes the whole keyspace from its copy-on-write snapshot
while the parent serves writes. Two costs scale with the dataset: the `fork()` page-table copy,
a stall on the main thread, and copy-on-write amplification, where a write of a few bytes
duplicates the whole 4 KB page it shares with unrelated neighbours, so that a long save under
writes multiplies into gigabytes of extra RSS.

The hybrid splits every key's **value** into one of two classes:

| class | what | where it lives | who serializes it |
|---|---|---|---|
| SNAP_NOFORK | every kvobj header; the keyspace and expires hash tables; single-allocation values up to `save-small-max-bytes` (default 4096): strings, listpack, intset, listpackEx | the SNAP_NOFORK arena, a jemalloc arena whose extents carry `MADV_DONTFORK`; the child never maps it | a scan thread in the parent, reading the live memory |
| SNAP_FORK | multi-allocation encodings (hash table, skiplist, quicklist, stream), module types, template hashes, any eligible encoding above the cap, any key with module key metadata | the default arena, forkable | the fork child, from its copy-on-write snapshot, exactly as the classic save |

Because headers and tables are in the arena, the child cannot walk the keyspace at all. It is a
serializer of SNAP_FORK values and the single writer of the output. The scan thread serializes
SNAP_NOFORK values itself and ships SNAP_FORK keys to the child as copies of their headers; the
child follows the value pointer into its own snapshot.

The class boundary follows an existing one: Redis already switches a hash from a listpack to a
hash table at a size threshold, and the listpack side is one allocation built to be copied. The
cap is a knob (`save-small-max-bytes`); the encoding list is `snapEncodingIsNoforkClass()`.

**Placement.** The class is applied to the value allocation when a key is inserted (`kvobjSet`,
`dbSetValue`), after an in-place growth of a string, and after an encoding conversion produced a
single-allocation value (`snapPlaceValue`). A value that crosses the cap or converts to a
multi-allocation encoding is moved out of the arena; a listpack adopted as the first node of a
quicklist is detached from the arena first. Only the value moves; the header, and every pointer
to it, stays. `DEBUG SNAP-PLACEMENT` and `DEBUG SNAP-DRIFT` report where values are versus where
their class says they should be.

**The arena.** One address range (16 TB on 64-bit) reserved at startup, `MADV_DONTFORK` applied in
the extent hooks on allocation and recommit. Allocation goes through `zmalloc_nofork*` with
`MALLOCX_ARENA` and a per-thread explicit tcache; `zfree()` and `zrealloc()` recognise arena
blocks by address range, so no caller needs to know the class to free. The arena exists whenever
the platform supports it (64-bit Linux, jemalloc) so the switch can change at runtime; while
`hybrid-snapshot` is off it is forkable and nothing new is placed in it. Without an arena (other
platforms, `MALLOC=libc`) the hybrid still runs, with the class as a pure size decision and the
child mapping everything; only the memory win is lost.

## 2. One save, end to end

Three parties: the **main thread**, the **scan thread** (one per save, started after the fork)
and the **child**. The scan thread and the main thread share the keyspace; the child shares
nothing but the two pipes, a `MAP_SHARED` page and its copy-on-write view of the forkable
memory.

| step | main thread | scan thread | child |
|---|---|---|---|
| 1 | Capture per-db (and per-slot) key and expire counts for `RESIZEDB` / `SLOT_INFO`. | | |
| 2 | **Freeze**: for every non-empty keyspace dict take one reference, stamp it `(generation, PENDING)` and record it in the save's table; install the store's snapshot guard. Nothing is paused, nothing allocated per dict. | | |
| 3 | `redisFork()`. The arena stays `MADV_DONTFORK`. | | Inherits the SNAP_FORK values (copy-on-write), the forkable half of the server, no headers, no tables, no small values. Reserves the arena's address range `PROT_NONE` so its own mappings cannot land there. |
| 4 | Start the scan thread; register the completion notifier with the event loop. Serve commands: every keyspace write goes through the funnel (section 3). | Walk the save's table of dicts in scan order (section 3, the credit window). For each dict: claim it, announce its layout position and read its table sizes, then announce batches of 128 buckets (plus the prefetch lookahead) and read their chains with atomic loads, staying below `hi` and skipping buckets whose bitmap bit is set; publish `lo` behind each bucket. SNAP_NOFORK keys are serialized into a 64 KB staging buffer; SNAP_FORK keys become KVOBJ messages. Every `W` buckets (default 1024) and at the end of each dict forward what the main thread queued. | Write the RDB header (or the AOF prologue). Then loop: take the small channel while the parent reports at least 1 MB of small backlog, waiting up to 20 ms for it to reach the pipe; otherwise take one big value. A BUFFER whose dependency (`robj_off`: big values that must precede it) is unmet is held until they are written. Track `SELECTDB` / `RESIZEDB` / `SLOT_INFO` (or `SELECT`) per message. |
| 5 | Write back overlayed buckets at the end of every write section and once per cron tick: a bucket the thread has passed (its overlay window wholly below `lo`) gets its duplicate chain installed in the dict and its originals released. Complete slots the thread published as SCANNED: apply what is left, resume layout changes, free the bitmaps, mark DONE, drop the reference. Abort at the hard mark. | When a dict is fully read publish SCANNED. When all are read, set `lo` and `cur` past every position, wait once for an open write section to close (it may still be emitting), drain the queue a last time, send END with both channels' message counts. | Finish when both counts are consumed: footer, fsync, rename (or the EOF mark on the sockets). Exit. |
| 6 | `checkChildrenDone` -> the classic done handler -> `snapOnChildEnd`: abort the scan if the child failed or the scan is unfinished, join the thread, release every remaining state, publish the last-save peaks. | Joined. | Reaped. |

![One hybrid save in flight](images/hybrid-snapshot-flow.png)

*Blue memory is the arena, never mapped in the child; pink is forkable. The scan thread reads
the frozen dict through a window (`[lo, hi)` in the credit window's terms). Behind the window writes go in place (kv1). Inside it a write
goes to a duplicate of the bucket's chain (kv3, kv7 are written, kv4, kv8 deleted, on the duplicate)
while the scan reads the fork-instant originals in the dict: small values (kv3-kv5) go into 64 KB buffers, big values (kv6-kv8) travel
as header copies and the child serializes them from its own snapshot. Ahead of the window the
main thread serializes kv9's whole bucket into a buffer, marks the bucket emitted so the scan
skips it, then writes in place. kv8 is deleted during the save and still reaches the file.

The figure draws the overlay per key ("dup", "tomb"). In the implemented bucket overlay a write inside the window duplicates the whole chain of its bucket, a deleted key is absent from the duplicate and no tombstone exists; the figure has not been redrawn.*

**Queues and channels.**

```
 main thread                           scan thread                              child
 ───────────                           ───────────                              ─────
 emit-ahead: serialize a bucket        staging buffer (64 KB) ──┐
   into mstage ──► mq ─────────────────► forwarded every W        │
   (mutex, taken by the thread          buckets and per dict      ▼
    every W buckets)                                      oq: small-channel queue ──► pipe A (BUFFER) ──► small-first loop
                                                                                        shared page: bytes queued on oq
 SNAP_FORK key written ahead:                                                             (child reads it to choose)
   header copy ──► mq ─────────────────► bq: big-channel queue ──► pipe B (KVOBJ, END, ABORT) ──► one big value at a time
                                        one bound over oq + bq:
                                        hybrid-snapshot-max-queued-bytes
                                        (the thread waits past it)
```

Both pipes are written non-blockingly from the scan thread's queues, so a child busy inside one
large value never stalls the scan. The main thread's queue `mq` is the only place the main thread
and the scan thread meet under a mutex, and the thread takes it only every `W` buckets and at the
end of each dict.

**Replication offset.** The snapshot is point in time at the fork instant, so the offset is the
the one a classic save records before forking; the wire protocol and the RDB format are unchanged, and any
replica version loads a hybrid dump.

## 3. The write funnel and the bucket overlay

Every keyspace write to a frozen dict goes through the kvstore overlay's `prepare` hook before it
touches the dict. Reads go through the overlay too, so a key in an overlayed bucket (one whose
chain was duplicated) is read from the duplicate; reads never clone. Every access runs inside a
**write section**, opened at the first guard or overlay hook of a command (reads included) and
closed when the outermost command returns, at `beforeSleep` and when a module thread releases the
GIL. There the main thread derives the key's `(table, bucket)`, hence its scan position `p`, and
compares it with the dict's state and the credit window (below):

| position | condition | action |
|---|---|---|
| behind | dict SCANNED or DONE, or `p < lo` | write in place; nothing is owed |
| emitted | the bucket's bitmap bit is set, whatever the position | write in place; the thread skips the bucket |
| inside | `lo <= p < hi`, bit clear | never touch the dict's slot or chain: on the first write duplicate the whole chain of the bucket (every key cloned) into an overlayed bucket and apply this and later writes to the duplicate; a delete unlinks from the duplicate, an add links into it; the originals stay in the dict for the thread |
| ahead | dict PENDING and not paused (wholly beyond `hi`), or `p >= hi`, bit clear | **emit first**: serialize the bucket's whole fork-instant chain on the main thread (SNAP_NOFORK records into the staging area, SNAP_FORK keys as header copies) into `mq`, set the bit, then write in place |

**The credit window.** The scan visits a fixed order, and each bucket has a position in it: a
64-bit integer packing the dict's ordinal (its store's base plus the slot, so a slot filter's
missing slots are gaps), a phase (0 the dict's layout, 1 table 0, 2 table 1) and the bucket index.
The order is integer order. Three words carry the protocol:

- `cur`, written by the thread (seq_cst): the last position it may read next. It announces a
  batch here before reading it.
- `lo`, written by the thread (release): every position below it is read.
- `hi`, written only by the main thread: the thread does not read at or beyond it.

The thread, per batch: store `cur`, load `hi` (seq_cst), wait while the batch's first position is
at or above `hi`, read up to `min(batch end, hi)`, store `lo` after each bucket. The main thread:

- **Outside sections** `hi` is unlimited. The main thread touches no frozen dict there, so an idle
  server, commands that do not touch the keyspace and the time between commands never slow the
  scan.
- **Section open (the cap).** `H = advance(lo, W)`; store `hi = H` (seq_cst); load `cur` (seq_cst);
  if `cur >= H`, set `hi = cur + 1` and pause the dict `cur` is in. Each side stores then loads, so
  either the main thread sees the thread's announcement, or the thread's next load of `hi` sees
  `H`: after the cap the thread is at most on `cur`, and no later position it reaches can pass the
  cap. The main thread never waits.
- **Inside a section (grants).** Every later hook checks whether `lo` has reached the midpoint
  recorded at the last grant (`advance(lo, W/2)`) and, if so, raises `hi` to `advance(lo, W)`
  (release). A grant is skipped while 16 overlay windows hold buckets: they are written back only
  at the section's end, and the thread then waits for it.
- **Section close.** Write back the overlayed buckets the thread has passed, then set `hi`
  unlimited.

`advance(p, W)` walks the scan order counting only buckets the thread will read (table 0 from
`rehashidx` while rehashing, table 1 only then), skips dicts the thread is done with and dicts the
save does not hold, pauses every dict it enters before `hi` may pass that dict's layout position
(positions name buckets by `(table, index)`, so the layout must hold still first), and stops before
the layout of its 64th dict.

**Why it is race-free.**

- A bucket below `lo` is read: the thread never returns to it.
- A bucket in `[lo, hi)` may be being read: the main thread overlays it and never writes its chain.
  A stale `lo` only makes this conservative.
- A bucket at or beyond `hi` cannot be reached until the main thread raises `hi`, which it does
  only after any emit-ahead and bit it set (the release store of `hi` orders them; the thread reads
  the bit after its load of `hi`). The thread then skips it.
- A bucket the main thread only read while it was ahead may become readable by the thread later in
  the same section: concurrent reads are fine, and a later write in the section goes through
  `prepare` again, which classifies it anew.
- Links the funnel hands out point into an overlayed bucket's chain or into the chain of a bucket
  behind the thread or emitted, which the thread no longer reads. Raising `hi` changes neither.
- A dict claimed by the thread while `hi` was unlimited is paused at the next section's cap (it
  finds the thread there), by `advance` (it enters the dict), or by the guard (an access to a
  SCANNING dict pauses it); the main thread touched no frozen dict in between.
- In-place header writes outside the funnel do not happen during a hybrid save: the LRU/LFU touch
  in `lookupKey` is skipped while a child exists (the save's writer child counts), and the
  in-place writes of read commands (`PFCOUNT`'s cache, `RM_SetLRU` on a READ handle) go through the
  funnel (`snapPrepareMutation`).
- **Abort**: the thread's wait for `hi` checks for an abort, as does its final wait.
- **Module threads** holding the GIL open, grant and close sections exactly as the main thread
  does; the GIL serializes them with it.
- **The end**: the thread stores `lo` and `cur` past every position (seq_cst) and waits once for an
  open section to close, since a section open before that may still be emitting. A section opened
  after sees every bucket behind the thread.

Value mutations that do not go through the hook (`APPEND`, `INCR`, `HSET` on a listpack) need no
protection: their bucket was emitted or walked before the command ran and the thread never returns
to a bucket. The scan thread never blocks the main thread. The main thread holds the scan thread
back only inside a section, and only once the thread reaches `hi`: a section that stops making
keyspace accesses (one access, then a long computation), or one whose overlay windows reached the
soft count.

**Attach and pause.** A dict acquires its per-save state on demand: the store's guard, which runs
on every access to a dict without an overlay while the scan runs, attaches on the first
structural write, or on any access while the thread is on the dict. Attaching allocates one
bitmap per hash table and the overlay. A dict's rehashing and auto-resize are paused before `hi`
may pass its layout (`advance`), or by the guard while the thread is on it. A dict beyond `hi` may
keep growing: when it starts
a rehash the bitmap of the old table is replicated into the new one (bucket `i` of `n` becomes
`i + j*n` of `k*n`), and emit-ahead on such a dict marks the whole bucket class in both tables.
A shrink merges buckets, which a bitmap cannot follow, so it is refused for attached dicts.

**The overlayed buckets.** Indexed by the overlay window they were deferred in: the overlay's
`prepare` hook returns the window with its verdict, the aligned run of `W` buckets (rounded up to
a power of two, at most the table) around the bucket, with the scan position of the run's last
bucket. One index serves the whole save: slots, each the buckets of one window of one dict, a
bucket at offset `index - lo` of its slot's array, with a bitmap of the offsets in use. The index
grows on demand; the write-back at every section's end and the grant limit of 16 windows in use
keep it small. `W` is at most `SNAP_WINDOW_BUCKETS_MAX` (65536), so a slot's array is bounded
too. An overlayed bucket is named by `(table, bucket index)` and
owns the duplicate chain (ordinary dict entries, so a position in it is an ordinary
`dictEntryLink`) and records nothing per original: the originals are found by walking the dict's
own chain at that bucket, and whether a clone took over its original's value is recomputed from
the original's header and the address of its value (`snapOriginalSharesValue`). There are no
tombstones: a deleted key is simply absent from the duplicate while its original stays in the
dict. Enumeration (`SCAN`, `KEYS`, iterators) hands out the duplicate chain at the first original
it meets of an overlayed bucket and skips the other originals of that bucket, and the buckets
the dict has no entry for after the dict's own walk. While any bucket of a dict is overlayed its
rehash and auto-resize are paused, so a `(table, index)` name stays valid; both resume once
the last bucket is written back.

**Clone.** Duplicating a bucket clones each key of its chain: the whole kvobj allocation into a
fresh arena block. A SNAP_NOFORK value
is deep-copied; a SNAP_FORK value is shared with the original, since the child already has its
own copy. The clone takes over the key's entry in `db->expires` and, for a hash with field TTLs,
the registration in the db's sub-expiry buckets: the original's `ExpireMeta` is trashed, which is
why anything that serializes a fork-instant original must read field TTLs from the value, not
from the registered metadata.

**Write-back and release.** A bucket stays overlayed only while the thread may still read it. At
the end of every write section and once per cron tick, if `lo` has moved since the last time, the
main thread writes back every overlayed bucket of the windows whose last position is below `lo`,
detached dicts included, with no per-bucket position check: the dict's slot is set to the duplicate chain, the
dict's entry count is adjusted by the difference, and each original is released (its header; its
value too unless the clone took it over). When the thread publishes SCANNED for a dict, the main
thread writes back what is left, resumes rehashing and auto-resize, frees the bitmaps, marks the
dict DONE and drops the save's reference. From then on writes to it are classic writes.

**References.** A dict is owned by its store (one reference) and held by a running save (one
more, taken at freeze). A store that frees, empties or moves a dict another holder references
detaches it instead: the dict leaves the slot table and the store's bookkeeping, keeps its save
state, and is freed by the last holder. Every dict holds a reference on its store, so a store
released on the lazyfree thread outlives its detached dicts. This is what lets FLUSHDB, an atomic
slot migration's trim and a kvstore release run while the save still reads the dict. A generation
stamp on every dict the save touched identifies its own state; because the save releases every
state it attached before its guard comes off, no state can outlive its save and no leftover
handling exists.

## 4. Memory model and bounds

What a hybrid save holds beyond the dataset, and what bounds it:

| memory | where | bound |
|---|---|---|
| copy-on-write on SNAP_FORK values written during the save | child, kernel | the forkable memory: `allocator_active` minus `allocator_active_nofork`, reached only if every forkable page is written |
| child page tables | child | proportional to the forkable range; sets the fork stall |
| retained originals: fork-instant SNAP_NOFORK values kept alive while their bucket is overlayed (a clone replaced them or they were deleted) and inside the window | parent | released once the thread passes the bucket's overlay window |
| emit-ahead records queued by the main thread (`mq`) | parent | forwarded within the thread's next `W` buckets or at the end of its dict |
| the scan thread's queues to the child (`oq` + `bq`) | parent | `hybrid-snapshot-max-queued-bytes` (default 100 MB): past it the thread waits for the child, never aborts |
| per-dict bitmaps and the overlay index | parent | one bit per bucket per table of attached and paused dicts; per overlay window holding buckets an array of one pointer per bucket of the window (8 KB at the default 1024) and its bitmap, plus the duplicated chains; grants stop at 16 windows in use |

The **hard mark**, `hybrid-snapshot-max-retained-bytes` (default 256 MB, 0 unlimited), covers the
retained originals plus the bytes the main thread has queued and the thread has not yet handed to
its own queues. `snapSaveCron` aborts the save when it is exceeded, and reports the save as failed
(`rdb_last_bgsave_status:err`, `snap_hard_mark_aborts`). The worst-case memory a save holds in the
parent is the sum of the two bounds. The queues, the retained originals, the bitmaps and the save's
fixed bookkeeping (per-slot size tables, dict tables, completion ring) are excluded from the eviction
calculation, like the replication buffers, so a save never causes evictions by itself: that memory
is what the classic child would hold in its own process.

The small channel policy keeps the queue small in practice: the child drains the small backlog
before each big value, so the queue peaks at about one big value's worth of scan output. A single
value longer than `bound / scan rate` parks the thread for the remainder of that value.

## 5. Entry points

Every consumer of a fork-based snapshot goes through the same dispatch rule: `hybrid-snapshot` on,
the request has keyspace content, and every loaded module with save callbacks has opted in
(section 7). Otherwise the classic child runs and `redisFork` flips the arena to `MADV_DOFORK` for
that child's lifetime, flipping it back when no child of any kind is left; such a child pays classic
copy-on-write on the arena.

| entry point | path with the hybrid on | notes |
|---|---|---|
| BGSAVE, save points, BGSAVE SCHEDULE | hybrid, RDB child, `dump.rdb` | the previous dump is untouched until the rename |
| SAVE, DEBUG RELOAD's save, SHUTDOWN's save, FLUSHALL with save points | foreground `rdbSave` in the parent | the parent maps everything, so the classic serializer works unchanged |
| disk-based full sync | hybrid, via `rdbSaveBackground` | the file is then streamed as today |
| diskless full sync, rdb channel | hybrid; the child writes the replicas' sockets | several replicas share one child; the replication stream buffers on the replica |
| diskless full sync, one connection | hybrid; the child writes a pipe, the parent relays it | about 2 CPU-seconds of relay per 4 GB on the main thread |
| functions-only transfer | classic child | no keyspace content |
| atomic slot migration's slot snapshot | hybrid over the migrating slots only; the child writes the rdb-channel connection | a third record form: the stream is RESP (`FUNCTION RESTORE`, `SELECT`, `CLUSTER SYNCSLOTS CONF SLOT-INFO` once per slot, `RESTORE ... ABSTTL` from a `DUMP` payload per key, `CLUSTER SYNCSLOTS SNAPSHOT-EOF`), written by the same producers through `slotSnapshotSaveKeyValuePair`; only the migrating slots' dicts are frozen and scanned (`snapScanSetSlotFilter`), writes to every other slot are plain writes; the child never maps the arena, so the migration pays the fork-class page tables and fork-class copy-on-write, as a hybrid BGSAVE does |
| BGREWRITEAOF, `aof-use-rdb-preamble yes` | hybrid, AOF child, RDB base file | the INCR file opens before the freeze, so the fork instant is where its history starts |
| BGREWRITEAOF, `aof-use-rdb-preamble no` | hybrid, AOF child, command-form base file | the same producers call `rewriteObject` per record; the child writes the AOF prologue and emits `SELECT` only on a db change |
| BACKUP | through BGREWRITEAOF | |
| `RedisModule_Fork`, Lua debugger fork | classic child, arena flipped | |
| `RedisModule_ForkWithFlags` with `REDISMODULE_FORK_NO_KEYSPACE` | module child forked like the hybrid child: arena not flipped, its range reserved in the child, keyspace APIs refused there | for module children that work on module memory only, such as a search index GC; see section 7 |
| `CONFIG SET hybrid-snapshot` | flips the arena | refused while any child or hybrid save runs |

Mutual exclusion is the existing one-child rule. A BGSAVE issued while a classic child runs is
scheduled and starts, on the hybrid path, after the flip back.

## 6. Interactions

What happens when something else touches the keyspace or the child during a hybrid save.

| event | behaviour | same as the classic save? |
|---|---|---|
| FLUSHALL (sync or ASYNC) | the dbs are handed to the save, then the caller's `killRDBChild` ends it; the next save works | yes, the classic save kills the RDB child there |
| FLUSHDB | the db gets fresh empty structures at once; the fork-instant ones stay with the save and are freed on BIO when the store completes; the dump holds the fork instant | the classic child keeps its copy |
| SWAPDB | the save scans kvstores, not db ids: keys stay under their fork-instant db id, writes through the new id take the funnel | yes |
| DEBUG RELOAD, DEBUG LOADAOF, CLUSTER RESET HARD, `RedisModule_ResetDataset` | every db is handed to the save, which finishes with the fork-instant data | yes |
| a replica's full-sync load | the replica's own running save is ended by its `killRDBChild` before the load | yes |
| hybrid AOF rewrite and FLUSHALL | the rewrite finishes on the detached dbs; the FLUSHALL lands in the INCR file | yes, the classic save keeps its AOF child |
| atomic slot migration's slot snapshot | start, success, failure and cancel run through replication's hooks into the migration's state machine (`asmSlotSnapshotAndStreamStart`, `asmSlotSnapshotSucceed`, `asmSlotSnapshotFailed`, `killRDBChild` on cancel), the same path a hybrid full sync takes; the slot set is fixed at the fork instant from the migrate task, like the trim set below | yes |
| atomic slot migration's trim | keys of slots under a trim job at the fork instant are skipped and counted in the "keys skipped" log line, as the classic child skips them; a trim that starts during the save is not in that set, its moved slot dicts stay with the save (references), are serialized to the end and freed when their slots complete | yes |
| rehash and resize | dicts are paused before `hi` may pass their layout, and while the thread is on them; dicts beyond `hi` grow with their bitmap, shrink is refused for attached dicts | the classic save avoids resizing while a child exists |
| active defrag | off while a child exists (classic rule); the arena's tcache is flushed once a second by cron | yes |
| key expiry and eviction | active expiry and eviction continue; deletes go through the funnel; LRU/LFU updates on lookups are skipped while a child exists | yes |
| hash-field expiry | active field expiry is suspended for the save; lazy field expiry and `HRANDFIELD` skip deletion on owed keys (expired fields are invisible, not deleted, as on a replica) | hybrid-specific |
| `RM_SetLRU` / `RM_SetLFU` on a READ handle | routed through the write funnel first | hybrid-specific |
| `PFCOUNT` cache write on an owed key | re-enters through the write path | hybrid-specific |
| SHUTDOWN | kills the child; the hybrid abort runs through `killRDBChild` | yes |
| child killed or crashed | the done handler aborts the scan, joins the thread, releases every state; the previous dump is untouched; a child crash report skips the keyspace sections it cannot read | yes |
| hard mark exceeded | the save is aborted and reported as failed | hybrid-specific |
| `CONFIG SET hybrid-snapshot` | refused while a child or save runs | hybrid-specific |

## 7. Module contract

Module code never runs on the scan thread: module values and every key with module key metadata
are SNAP_FORK, serialized by the child from the header copy through the type's `rdb_save` and the
metadata save callbacks, as the classic save does. `aux_save` runs in the child from the RDB header.
Persistence events fire where the classic save fires them. Module allocations are forkable and mapped in the
child.

The one difference from the classic save: **the hybrid child has no keyspace.** Dict tables, headers other
than the copy handed to the callback, and small values are unmapped there.

A module is compatible with `hybrid-snapshot yes` when:

1. Its save-side callbacks (`rdb_save`, `aux_save`, `aux_save2`, key-metadata save, a PERSISTENCE
   event handler) do not touch the keyspace: no `RM_OpenKey`, `RM_KeyExists`, `RM_Scan`,
   `RM_ScanKey`, `RM_Call`, `RM_DbSize`, `RM_RandomKey`, `RM_GetExpire` on any key. Allowed: the
   value passed to the callback, module-owned memory, the IO context.
2. Module-owned state does not keep pointers into Redis-owned key memory (`RM_StringDMA` results,
   `RedisModuleKey *`) and dereference them in a save callback. Both are invalid past the command
   by the API contract already; in the hybrid child they fault.
3. On the main thread it mutates a key only through a handle opened with `REDISMODULE_WRITE`, and
   from a module thread only under the GIL. The write lookup emits or clones the owed bucket, so a
   later in-place write through a DMA pointer or a type API is covered.

A module declares compliance with `REDISMODULE_OPTIONS_HANDLE_HYBRID_SNAPSHOT`. A module is relevant
when it registers a data type, a key metadata class, or a PERSISTENCE or FORK_CHILD event handler;
a relevant module without the bit sends every save to the classic child, with a warning at load and
at save time, and `INFO persistence` names it in `snap_hybrid_blocked_by`. Modules with none of
those callbacks need no declaration. In the hybrid child the forbidden APIs are refused rather than
faulting (`RM_OpenKey` returns NULL, `RM_Call` fails with EPERM, and so on), logged once per API per
child, so a violation by an opted-in module costs a message, not a crash. The bundled modules
declare the bit.

A module's own forks are a separate declaration, made per fork call. `RedisModule_Fork` is a
classic child: it may walk the keyspace, so the arena is flipped forkable for it. A child that
works on module memory only (an index, a cache) is forked with
`RedisModule_ForkWithFlags(ctx, REDISMODULE_FORK_NO_KEYSPACE, cb, user_data)`. It is forked the way
the hybrid child is (`snapForkNoKeyspace`): the arena stays `MADV_DONTFORK`, the child reserves its
range, the same keyspace APIs are refused there with the same once-per-API log, and the crash
report skips the INFO sections that read the keyspace. The refusals apply with the feature on or
off, so the contract does not depend on the configuration. Rule 2 above applies to the whole
child, not only to callbacks: it must not dereference or free pointers it kept into key memory.
`HANDLE_HYBRID_SNAPSHOT` does not imply the fork flag, and the flag cannot be module-wide: a
module may have compliant save callbacks and a fork that does walk keys, and `RedisModule_Fork`
takes no context, so the server cannot tell which module forked. Unknown flags fail with `EINVAL`;
the single-child rule applies as for any fork.

## 8. Observability

**Configuration**

| config | default | meaning |
|---|---|---|
| `hybrid-snapshot` | no | the switch; runtime, refused while a child or save runs |
| `save-small-max-bytes` | 4096 | the SNAP_NOFORK size cap for single-allocation values; affects values placed from then on |
| `hybrid-snapshot-max-retained-bytes` | 256mb | the hard mark over retained originals plus main-thread queued bytes; 0 unlimited |
| `hybrid-snapshot-max-queued-bytes` | 100mb | the bound over the scan thread's two queues; 0 unbounded |
| `hybrid-snapshot-tag-all` | no | debug: every BUFFER depends on all big values sent before it |

**INFO memory**

| field | meaning |
|---|---|
| `allocator_allocated_nofork`, `allocator_active_nofork`, `allocator_resident_nofork`, `allocator_frag_bytes_nofork` | jemalloc statistics of the arena; forkable memory is the process total minus these |
| `snap_nofork_arena_available` | 1 when the arena exists |
| `snap_nofork_arena_forkable` | 1 while a classic child has it mapped |
| `snap_nofork_va_reserved`, `snap_nofork_va_used` | the arena's address range and how much of it has been handed out |

**INFO persistence**

| field | meaning |
|---|---|
| `snap_save_in_progress` | a hybrid save is running |
| `snap_save_slots_total`, `snap_save_slots_done` | fork-instant dicts to scan and completed |
| `snap_save_keys_nofork`, `snap_save_keys_fork`, `snap_save_bytes_sent` | records produced by the scan thread so far, by class, and bytes sent to the child |
| `snap_save_main_buckets`, `snap_save_main_keys` | buckets and keys the main thread emitted ahead of the window |
| `snap_save_scan_stalls`, `snap_save_scan_stall_ms` | times the scan thread waited at `hi`, and the time it spent waiting, in the current or last save |
| `snap_save_window_grants` | times a write section raised `hi` (the thread was half way through its credit), in the current or last save |
| `snap_save_retained_bytes`, `snap_last_save_retained_peak` | fork-instant originals kept alive by overlayed buckets, now and at the last save's peak |
| `snap_save_queued_bytes`, `snap_last_save_queued_peak` | bytes in the scan thread's queues and the main thread's queue, now and at the last save's peak |
| `snap_save_side_entries` | overlayed buckets pending write-back (the name predates the bucket overlay) |
| `snap_save_overlay_buckets`, `snap_save_overlay_clones` | writes that overlayed a bucket (collisions with the window) and the keys cloned by them, since the current or last save started |
| `snap_save_overlay_peak_buckets` | most buckets overlayed at once, over every dict |
| `snap_save_overlay_paused_ms` | dict-time rehash and auto-resize were held by overlayed buckets (summed over dicts) |
| `snap_save_bitmap_bytes` | bytes of per-save bucket bitmaps |
| `snap_save_grown_dicts` | attached dicts that expanded during the save and had their bitmap remapped |
| `snap_retained_dicts` | across saves, dicts their store let go of before the save completed them |
| `snap_save_inplace_writes`, `snap_last_save_inplace_writes`, `snap_inplace_writes_total` | writes served in place on scanned keys |
| `snap_hard_mark_aborts` | saves aborted at the hard mark |
| `snap_saves` | hybrid saves started |
| `snap_hybrid_blocked_by` | the module sending saves to the classic child, empty when none |

`rdb_last_cow_size` and `current_cow_size` keep their classic meaning: the child's copy-on-write,
now on the forkable half only. `rdb_bgsave_in_progress`, `aof_rewrite_in_progress` and the done
status fields are the classic ones.

**DEBUG**: `SNAP-PLACEMENT [dbid]` (placement statistics of the keyspace, arena versus class),
`SNAP-DRIFT [dbid]` (up to 50 keys whose placement disagrees with their class),
`SNAP-WINDOW-BUCKETS [n]` (get or set the credit `W`, buckets per grant; tests).

**Log lines** worth knowing: `Hybrid background saving started by pid ... (scan thread streams to
the child)`, `Hybrid AOF rewrite started ...`, `Hybrid snapshot done, N SNAP_FORK keys serialized by
the child`, `Hybrid snapshot: ... past hybrid-snapshot-max-retained-bytes ...: aborting the save`,
`Hybrid snapshot: scan aborted, keyspace overlay drained`, `Hybrid snapshot: db N flushed during the
save, its keyspace stays with the save`, `Hybrid snapshot: falling back to the fork child, module
'...' has not declared ...`, `SNAP_NOFORK arena made forkable for a ... child` and `... back to
MADV_DONTFORK`.

**Unit tests**: `redis-server test snapshot` (scan engine, placement, overlay, references),
`tests/unit/snapshot.tcl` (the harness race workload, every entry point, every interaction above).

## 9. Glossary

- **class**: SNAP_NOFORK or SNAP_FORK, a property of a key's value allocation (section 1).
- **arena**: the SNAP_NOFORK jemalloc arena, `MADV_DONTFORK`, never mapped in the hybrid child.
- **fork instant**: the moment of `redisFork()`; the snapshot's point in time. Everything the
  save writes is the state at that instant.
- **store**: a kvstore the save is scanning, identified by the db id it had at the fork instant;
  SWAPDB and FLUSHDB change which db owns it.
- **slot** / **dict**: one kvstore dict, a hash table; one per db in standalone mode, one per
  cluster slot in cluster mode. Slot states: PENDING, SCANNING, SCANNED, DONE.
- **owed**: a bucket (and the keys in it) whose fork-instant content the save has not taken yet;
  a write to it must go through the funnel.
- **position**: a bucket's place in the scan order, one integer: dict ordinal, phase (layout,
  table 0, table 1), bucket index.
- **credit window**: `[lo, hi)`, the positions the scan thread may be reading. The thread moves
  `lo`; the main thread alone moves `hi`, unlimited outside write sections, capped at a section's
  start and raised by `W` buckets inside it (a **grant**).
- **overlay window**: the aligned run of `W` buckets (power of two) an overlayed bucket is indexed
  by; written back once the thread is past its last bucket.
- **write section**: the interval on the main thread from a command's first hook on a frozen
  dict (read or write) to the end of the outermost command; it caps the scan thread at its start
  and lifts the cap at its end.
- **funnel**: the overlay's `prepare` hook and the three outcomes of a write: in place, overlayed
  bucket, emit-ahead.
- **emit-ahead**: serializing a bucket's fork-instant chain on the main thread before writing it
  in place, marking it in the bitmap so the thread skips it.
- **bitmap**: one bit per bucket per table of an attached dict; set means emitted.
- **overlay** / **overlayed bucket**: for each bucket written while it was inside the window, a
  duplicate of its chain, indexed by its overlay window, (clones, adds, no deleted keys) that the main
  thread uses in place of the dict's chain until the write-back.
- **retained**: fork-instant originals kept alive by overlayed buckets; **queued**: records waiting
  for the child; **hard mark**: the bound over both on the parent side.
- **small channel** / **big channel**: the two pipes to the child; BUFFER messages on the first,
  KVOBJ, END and ABORT on the second. "Small" names the channel, not the class: a BUFFER may carry
  SNAP_FORK headers the main thread emitted inline.
- **dependency**: the count of big values that must precede a BUFFER in the output (`robj_off`).

## 10. Invariants

1. The scan thread reads a fork-instant header or SNAP_NOFORK value only while it is unchanged:
   every keyspace write to an owed bucket goes through the funnel, which emits or clones before the
   dict changes.
2. The thread reads chain pointers only below `hi` and after announcing them in `cur`; the main
   thread writes chains only outside `[lo, hi)` and only inside a write section; a bitmap bit is
   set only at or beyond `hi`, before `hi` is raised over it.
3. Every fork-instant key reaches the output exactly once: by the thread, by the main thread's
   emit-ahead (bit set, thread skips), or by the child from a header copy. A key deleted while its
   bucket is inside the window stays in the dict, as an original, until the bucket is written back.
4. The snapshot is the fork instant: a key's record is its state at the fork, under its
   fork-instant db id, with its fork-instant TTL and field TTLs read from the value.
5. Memory held by a save in the parent is bounded by the hard mark plus the queue bound, and is
   released when the save ends; the previous dump is untouched by a failed or aborted save.
6. The hybrid child never dereferences the arena; module APIs that would are refused.
7. No state the save attached to a dict outlives the save: every reference, bitmap, overlay and
   pause is released before the store's guard comes off.
8. The main thread never waits for the scan thread or the child; the scan thread waits for the
   main thread only inside a write section once it reaches `hi` (for at most the rest of that
   section) and once at the end, and for the child only at the queue bound.
