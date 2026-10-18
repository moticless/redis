---
id: doc-1
title: 'TASK-1.62 design: credit-based scan window (reviewed)'
type: specification
created_date: '2026-10-17 12:03'
tags:
  - hybrid-bgsave
  - TASK-1.62
  - design
---
# Credit-based scan window: design draft (TASK-1.62)

Status: reviewed 2026-10-17 (sections 4, 6 and 7 settled with the user). Implementation in progress. Once implemented, the parts that describe the result move into docs/hybrid-snapshot.md (which describes current state only).

## 1. Today's protocol (what we replace)

- The scan thread owns the window. `snapWindowPublish(d, t, lo, hi)` (snapshot.c) publishes `[lo, hi)` (or `(d, -1)` = "about to read d's layout", or NULL = no window), then `snapWalkerQuiesce` waits until no write section that was open at publication time is still open.
- A **write section** opens at the first guard or funnel hook of a command (`snapFunnelOpen`: `snap_funnel_seq` odd, seq_cst) and closes when the **outermost** `call()` returns (server.c:4559, `execution_nesting == 0`), at `beforeSleep`, and on module GIL release (`snapFunnelClose`). EVAL, FCALL, and MULTI/EXEC as a whole are therefore one section.
- **Reads open sections too.** `snapOverlayGuard` calls `snapFunnelOpen()` on every access, read or write, to a dict the save has not finished. A find may step a rehash, and the layout must hold still once the thread claims the dict.
- The section also bounds **link validity**: the overlay drain (write-back, which frees originals) runs only at section close, where no caller holds a link.
- Result: the thread stalls for the full length of any command touching an unfinished frozen dict that is running when it publishes. That includes long read-only commands (KEYS, SORT, read-heavy scripts) and whole scripts and transactions.

## 2. Key observation

Who owns `hi` matters less than **when `hi` may move**:

- If the main thread only refills credit at section close, a long command still stalls the thread once it has used its W buckets. That is barely better than today.
- The gain comes from (a) letting the thread run free whenever no section is open, and (b) letting the main thread **raise `hi` in the middle of a section**. Section 4 argues that (b) is safe.

## 3. Proposed protocol

Shared state:
- `cur`, written by the scan thread: the position it is reading or about to read.
- `lo`, written by the scan thread: everything below it has been read.
- `hi`, written by the main thread: the thread may not read at or beyond it.

Positions are linear over `(store, didx, phase, idx)`, with `phase` ∈ {layout, table 0, table 1}. See section 6.

Scan thread, per bucket (or per small batch):
```
store_seq_cst(cur, p)
while (p >= load_acquire(hi)) stall   // yield, then usleep; check abort
read bucket p (skip it if its emitted bit is set)
store_release(lo, p + 1)
```

Main thread:
- **Outside a section:** `hi = INF`. The thread runs free, which covers an idle server, commands that do not touch frozen dicts, and the time between commands.
- **Section open** (first guard or funnel access of a command; reads included, as today): the **cap**.
  ```
  store_seq_cst(hi, H)          // H = advance(load(lo), W), see section 6
  c = load_seq_cst(cur)
  if (c + 1 > H) store_release(hi, c + 1)   // the thread is at most on c
  ```
  This is the Dekker pairing with the thread's `store cur; load hi`. After it, the thread is at most on bucket `c`, and any later bucket it reaches sees the cap. The main thread never waits.
- **Inside a section:** classify by `lo` (acquire) and `hi` (own value):
  - below `lo`: BEHIND, write in place;
  - in `[lo, hi)`: INSIDE, overlay;
  - at or above `hi`: AHEAD, emit-ahead, mark, write in place.
  A stale `lo` only makes the classification conservative (INSIDE instead of BEHIND).
- **Refill inside a section:** at each guard or funnel hook, if `lo` has reached `mid` (recorded at the last refill as `advance(lo, W/2)`, since packed positions cannot be subtracted either), set `hi = advance(lo, W)` (store_release). Before raising `hi` into a new dict's layout, the main thread pauses that dict's rehash and auto-resize (the main thread now decides when the thread may claim a dict).
- **Section close:** drain (write back the overlayed buckets below `lo`), then `hi = INF`.

## 4. Why raising `hi` mid-section is safe (to be checked in review)

A bucket the main thread has touched during the section is one of:
- **BEHIND:** never read again.
- **INSIDE:** overlayed. The thread reads the originals, and the main thread works on the duplicate.
- **AHEAD and written:** emitted and its bit set **before** any later raise of `hi` (the release store orders it). The thread reads the bit after its acquire load of `hi` and skips the bucket.
- **AHEAD and only read:** the main thread holds read pointers into it. Concurrent reads are fine. A later write in the same section goes through `prepare`, which by then classifies it INSIDE (overlay) or AHEAD (emit). The original stays alive until the drain, which happens only at a point where nothing holds a pointer.

Links handed out by the funnel (`ovDictFindLink`, `SetAtLink`) point either into the overlay's chain (INSIDE) or into the dict's chain of a BEHIND or emitted bucket, which the thread no longer reads. Moving `hi` changes neither.

In-place writes from read commands (resolved):
- **Data written in place** (PFCOUNT's cache, a module setting LRU/LFU through a READ handle) goes through the write funnel via `snapPrepareMutation`, so it is classified when the write happens. That does not depend on when `hi` moved.
- **The LRU/LFU touch in `lookupKey`** (db.c) is skipped while `hasActiveChildProcess()` is true. The hybrid save forks its writer child with `redisFork()` (snapshot.c), which sets `server.child_pid` for the whole save, so read commands do not write `kv->lru` while the scan runs. Cron's incremental rehash is gated the same way (server.c `databasesCron`), so it never steps a frozen dict either. Nothing about this changes when `hi` moves. (Correction, 2026-10-17: an earlier version of this bullet wrongly said the hybrid save has no child.)
- **Valkey forkless, for comparison** (~/git/valkey `bgiteration.c`, `db.c:117`). It has no child, so the same gate does not hold there. Reads never block. Reads that change the stored data in place (PFCOUNT, LINDEX/LRANGE/LPOS on compressed quicklists, SORT_RO) are treated as writes (`isFormatChangingCommand`). The LRU/LFU touch is left racing with the thread's `rdbSaveKeyValuePair`.
- **No hardening needed here.** If the hybrid save ever runs without a child, `lookupKey`'s gate must also check for a running scan.

## 5. Write-back

Unchanged in substance and still on the main thread. Today it drains when `snap_win_seq` moves; it would drain when `lo` moves past a window slot's range. With TASK-1.60's index, a slot whose `[lo, lo + n)` lies below `lo` is written back whole. The index could become a circular bitmap over `[lo, hi)` later, but that is not required.

## 6. Dicts and tables

**Decided: option A, a linear position.** (Rejected, option B: clip `hi` at the end of the current dict or table. Simpler, but while a section is open the thread stalls at every boundary, which means 16384 dicts per db in cluster mode.)

- **Encoding:** `(store, didx, phase, idx)` packed into a 64-bit value, most significant field first, with `phase` 0 = layout, 1 = table 0, 2 = table 1. Example budget (not final): store 2 bits, didx 14 bits, phase 2 bits, idx 46 bits. `didx` is the real dict index (the slot in cluster mode), not an index into the scanned subset, so the main thread computes a bucket's position straight from `(store, didx, table, idx)`.
- **Order:** the scan visits stores, then dicts, then layout, table 0 and table 1, then buckets, all increasing. That is increasing integer order. `cur`, `lo` and `hi` are each one atomic word, and classification is two integer compares. This replaces the seq-tagged, double-buffered `snap_win[2]`.
- **Gaps:** dicts the scan never visits (not frozen, as with ASM's `slot_filter`, a non-contiguous set of migrating slots) occupy no buckets. Their accesses are not the save's and are never classified, so a gap lying inside `[lo, hi)` affects nothing. Trim slots are skipped at record time and do not affect the order.
- **Refill is `hi = advance(lo, W)`, not `lo + W`.** The idx field is wider than any one table, so plain addition never carries into the next phase. It would only point past the end of the current table, which quietly turns this into option B. `advance()` walks the scan order: what is left of the current table, then the next phase, then the next frozen dict's layout. It skips non-frozen dicts and counts only buckets the thread will read. The current dict's sizes are frozen, and the next dict's are read after the main thread pauses it.
- **Rule:** `hi` never passes a frozen dict's layout position unless that dict is paused (no rehash, no auto-resize). Before that, rehash can still move its keys between `(table, idx)` positions. `advance()` pauses each dict it enters before `hi` is stored past that dict's layout point. `hi = INF` outside sections is consistent with the rule, because the main thread is not touching any dict then.
- **Claims while `hi = INF`** (needs checking during implementation): today the guard also lets a PENDING dict keep rehashing (bitmap replication). With `hi = INF`, the thread may claim a dict while the main thread is between commands. The claim must still pause the dict, either from the thread side as today (`snapScanClaim` sets SCANNING; the guard pauses on SCANNING), or by the main thread at the next section open, when the cap sees `cur` inside that dict. The Dekker cap orders this: the main thread loads `cur` before any access.

## 7. End of the scan

The thread stores `cur = END`. The main thread's emitted records must all be queued before the thread's last `window_done` drain. **Decided:** the thread waits once for any open section to close (the existing quiesce), then drains. This is the only quiesce left, once per save. (Rejected: the main thread pushes its staged records at section close when it sees END. That avoids the wait but needs a separate handoff path.)

## 8. Abort, modules, the layout phase

- **Abort:** the thread's stall loop checks abort, as `snapWalkerQuiesce` does.
- **Module threads holding the GIL:** a module thread's accesses open a section the same way, and the GIL release closes it. Unchanged.
- **The layout phase** (`table < 0` today): it becomes the dict's `layout` position. The thread stores `cur = (X, layout)` and the cap handshake orders it against the main thread's accesses to X.

## 9. Expected effect and how to measure it

- The thread stalls only when it overtakes `hi` inside a section that makes no further guard or funnel access, for example a command that touches the keyspace once and then computes for a long time. Read-only long commands no longer stall it.
- Measure with TASK-1.61's counters: stall count, total and maximum per save, today versus after, on the snap-bench matrix plus long EVAL, MULTI/EXEC and KEYS workloads.

## 10. Risks

- This is a new memory-ordering protocol at the core of the save. Every case in section 4 must hold, including the in-place header writes.
- Interactions with the PENDING-dict rehash and bitmap replication path, and with emit-ahead of a whole bucket class.
- Test burden: unit tests for the cap at every phase boundary, plus a TSAN soak under write and read load with sections opening and closing at high frequency.
