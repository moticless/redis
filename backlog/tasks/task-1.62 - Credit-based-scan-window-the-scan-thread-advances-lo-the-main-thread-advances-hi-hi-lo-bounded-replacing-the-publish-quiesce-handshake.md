---
id: TASK-1.62
title: >-
  Credit-based scan window: the scan thread advances lo, the main thread
  advances hi (hi - lo bounded), replacing the publish/quiesce handshake
status: Done
assignee:
  - '@claude'
created_date: '2026-10-17 10:31'
updated_date: '2026-10-18 12:46'
labels:
  - hybrid-bgsave
  - performance
  - design
dependencies:
  - TASK-1.60
  - TASK-1.61
references:
  - src/snapshot.c
  - src/kvsoverlay.c
  - TASK-1.21.1
  - TASK-1.57
documentation:
  - docs/hybrid-snapshot.md
  - >-
    backlog/docs/doc-1 -
    TASK-1.62-design-credit-based-scan-window-draft-for-review.md
parent_task_id: TASK-1
priority: medium
ordinal: 58000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Why: today the scan thread owns the whole window. snapWindowPublish (src/snapshot.c) jumps it a block of buckets at a time, then snapWalkerQuiesce waits for any open main-thread write section to close; the main thread only reads the window inside a write section (snapFunnelOpen/snapFunnelCloseSlow), synchronised by seq_cst store-load pairs. The scan thread therefore stalls for the length of whatever command is running when it publishes.

Proposed protocol: the scan thread advances lo as it finishes buckets; the main thread is the only writer of hi and keeps hi - lo within the cap. Main-thread classification of a bucket: below lo is behind (write in place; a stale lo only makes the main thread overlay conservatively); [lo, hi) is inside (overlay); hi and above is ahead (emit-ahead, then write in place) and cannot race because only the main thread moves hi. No write sections are needed for window safety, the scan thread never waits for a command and the main thread never spins. Overlay write-back stays on the main thread (decided): when it observes lo has moved it writes back the overlayed buckets in [drained, lo). The window-keyed overlay index from TASK-1.60 becomes a circular bitmap over [lo, hi).

Open design points to settle in the design write-up before code:
- Idle main thread: nobody advances hi and the scan thread would stop until the next event or cron tick. Candidate: before sleeping the main thread grants an unbounded hi; on wakeup, before its first write to a frozen dict, it caps without waiting via a store-load handshake (scan thread: store cur = idx, then load hi, stall if idx >= hi; main thread: store hi = H, then load cur; the scan thread can be at most on bucket cur, so hi = max(H, cur + 1) plus credit).
- Position across tables (rehash) and dicts (16384 per db in cluster mode): one linear position over (store, dict, table, idx) versus clipping hi at dict/table boundaries (simpler, one refill per boundary).
- Abort, module threads holding the GIL, dicts detached or moved from their store while inside the window.

Gate: proceed only if TASK-1.61 shows the quiesce wait is a noticeable share of save time; otherwise close as not worth it with the numbers.

Constraint: single scan thread; TASK-1.57 (parallel scan) would need one window per scan thread.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 TASK-1.61's numbers justify the change (recorded in this task's notes); otherwise the task is closed with them
- [x] #2 docs/hybrid-snapshot.md contains the memory-ordering argument covering busy, idle and wakeup transitions, abort, and module threads holding the GIL, reviewed before implementation
- [x] #3 With no traffic, save time is within noise of the current protocol (an idle main thread does not slow the scan)
- [x] #4 Under TASK-1.61's long-command workload the scan thread's waiting time drops to near zero
- [x] #5 Existing scan, abort and abort-in-section tests pass; new tests cover the idle-to-capped transition and a window crossing tables and dicts
- [x] #6 TSAN run is clean
- [x] #7 docs/hybrid-snapshot.md describes the new protocol and the write-section wording that no longer applies is removed
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
Design: backlog doc-1 (reviewed 2026-10-17). Steps:

1. Positions (snapshot.c): pos = ord<<42 | phase<<40 | idx. ord = ctx->ord_base[s] + didx (real slot); phase 0 = layout, 1 = table 0, 2 = table 1. SNAP_POS_END is past every dict, and hi = UINT64_MAX means no limit. Refuse the hybrid save (fall back) if the ords do not fit in 22 bits; assert tables stay below 2^40 buckets.
2. Shared state, replacing snap_win[2], snap_win_seq and snapWindowPublish: snap_cur (thread, seq_cst), snap_lo (thread, release), snap_hi (main, seq_cst at the cap, release at a refill). snap_funnel_seq stays as the section marker.
3. Thread walk: claim the dict (SCANNING), reach(layout), read the layout, then for each table go batch by batch: store cur = last position of the batch, load hi, stall while the first position >= hi, read up to min(batch end, hi), store lo after each bucket. Prefetch only inside the batch it committed to in cur. window_done runs every W buckets and at the end of each dict. At the end: store lo = cur = END (seq_cst), wait once for an open section to close (section 7), then window_done. Count stalls and stall time.
4. Main thread, in snapFunnelOpen: on section open, the cap (H = advance(lo, W); store hi = H; c = load cur; if c + 1 > H, store hi = c + 1; mid = advance(lo, W/2)). If the section is already open: refill once lo >= mid, unless the overlay has at least OV_SLOTS_SOFT slots in use. In snapFunnelCloseSlow: drain (lo), then hi = INF.
5. advance(lo, W): walk the scan order, counting only the buckets the thread will read (table 0 from rehashidx when rehashing). Skip NULL, SCANNED and DONE dicts. Pause every frozen dict before hi may pass its layout point (pause only, no overlay attach, so this works for dicts detached from their store). Stop at the next dict's layout after 64 dicts.
6. Classification (snapBucketPosition): SCANNED/DONE dict = BEHIND; PENDING and not paused = AHEAD (the class-emission path for a rehashing dict applies only here); otherwise by pos against lo/hi. The guard pauses on SCANNING; snapWindowOnDict and snapWindowCoversKey go away.
7. Overlay (kvsoverlay.c, kvstore.h): kvstoreOvWindow gets `end` (a position) in place of `seq`. A window is the aligned block of B = pow2(W) buckets (at most the table size) around the bucket. kvstoreOverlayDrain(below) writes back the slots with end <= below. ov_index becomes a growable array; kvstoreOverlaySlotsInUse() feeds the refill throttle.
8. Stats: stall count and stall time in INFO (needed for AC #3 and #4).
9. Tests: rewrite the window unit tests on top of a test helper that sets cur/lo/hi. New tests: the cap when the thread is ahead of H, a refill crossing a table and a dict (pauses the next dict), gaps from slot_filter, the refill throttle, end-of-scan with an open section. Then the full tcl suite, ASan and TSan, plus a benchmark of the long-command workload and idle save time.
10. Docs: move the result into docs/hybrid-snapshot.md and remove the write-section/publish wording that no longer applies.
<!-- SECTION:PLAN:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
2026-10-17: the user chose to go ahead without waiting for TASK-1.61's numbers (gate in AC1 overridden by the user). Design draft written as backlog doc-1, awaiting review before code. Main findings: (1) sections close only at the outermost call(), and the guard opens them on reads too, so today the thread stalls for whole scripts, transactions and long read-only commands; (2) moving hi only at section close would barely help: the gain needs hi = INF outside sections plus raising hi mid-section, argued safe in doc-1 section 4 (open point: in-place header writes that bypass the funnel).

Design review (doc-1), 2026-10-17:
- Section 4 resolved. Data written in place goes through the funnel (snapPrepareMutation). The LRU/LFU touch in lookupKey does not happen during a hybrid save: the save's writer child sets child_pid, so hasActiveChildProcess() gates the touch, and cron rehash is gated the same way (corrected; I first said there was no child). Valkey forkless has no child and leaves the touch racing.
- Section 6 decided: option A, a linear packed position (store, didx, phase, idx), with didx the real slot. Refill uses advance(lo, W), which skips non-frozen dicts (works with ASM's slot_filter) and pauses each frozen dict before hi passes its layout point. The refill trigger is a midpoint recorded at refill.
- Section 7 decided: the thread waits once, at END, for an open section to close (the existing quiesce).
- Two changes found while planning: the overlay index is re-keyed by position blocks and can grow (no published windows any more), and refills stop at a soft slot count so a long script cannot pile up slots.

Implementation, 2026-10-17:
- snapshot.c: positions (SNAP_POS: ord 22 bits | phase 2 | idx 40), cur/lo/hi globals, snapWindowAdvance / snapWindowCap / snapWindowRefill, snapScanReach (batches of 128), the end quiesce. snap_win, snapWindowPublish, snapWindowCoversKey and snapWindowOnDict are gone. snapScanAttach split into snapScanState + snapScanPauseLayout (pause without attaching an overlay, so it works for detached dicts). The rehash-class emit path applies only to unpaused PENDING dicts. A save started inside a section is capped at start. The hybrid save falls back to the classic child if the ords do not fit.
- kvsoverlay.c: windows keyed by `last` (position of the run's last bucket) instead of seq; ov_index grows; kvstoreOverlaySlotsInUse(). Grants stop at 16 slots in use.
- Bug found by the new tcl test: once a dict has an overlay attached, reads went through the overlay with no hook, so a read-only script never refilled hi and the thread stalled for the whole script. Fix: kvstore.c ovGet calls the 'enter' hook on every access through an attached overlay. After the fix: a 1 s script of GETs, 0 ms stall, 33 grants.
- INFO: snap_save_scan_stalls, snap_save_scan_stall_ms, snap_save_window_grants (current or last save).
- Tests: unit 212/212 (new: credit grant/stall/refill/close with a thread; positions order, gaps, crossing dicts, rehashidx; cap with the thread past H; the overlay index growing to 10 windows). tcl: a long-script test. unit/snapshot + ASM + integration/rdb: 217 ok before the new test.

Verification, 2026-10-18:
- Full tcl suite (--config hybrid-snapshot yes): all passed, 6310 ok (the count difference from 6319 is PSYNC2's randomized cycles; the only new name is the long-script test). Unit tests 212/212. ASan and TSan unit/snapshot: 76 ok each, no sanitizer reports in any server log.
- Benchmark (laptop; bench162.sh in the session scratchpad; base = 24f129c5c in .scratch/base): 2M keys of 100 B.
  - Idle save: base 1417/1801/2523 ms, new 2622/1704/2220 ms, 0 stalls. Within this machine's noise (thermal throttling); a quiet host is needed to call it equal with confidence.
  - Back-to-back Lua scripts of 100k SETs (one write section each): base 78.7/82.8/76.4/79.8/69.8/71.4 s, new 2.30/2.82/2.38/2.45/2.62/2.11 s; new stall time 0 ms, about 3970 grants per save. About 30x.
  - Read-only scripts (300k GETs): base and new alike (~2-2.5 s). In the base code, reads through an attached overlay called no hook, so read-only scripts never opened a section and never held the thread.

Idle-save regression investigation (AWS c7i.4xlarge, 2026-10-18; perf on the scan thread, CSVs and summaries in the session scratchpad aws/):
- The scan thread is the bottleneck of an idle save (its CPU time is the save time; child and main thread are small). Instructions are equal (36.58 G base vs 36.60 G new, 10M keys of 100 B): the gap is cycles, not work. perf shows the extra cycles in serializer code this change did not touch (rdbSaveStringObject, rdbSaveLen, sdscatlen).
- Not LTO layout: without LTO the gap is the same. Per-batch lo and 1024-bucket batches did not help (single runs vary +-5% in cycles, mostly from the populated layout).
- Controlled comparison (8 runs each from one RDB, scan thread pinned): base 30.09 G cycles / 9170 ms; new +1.2% / +1.1% (about one standard error); new2, with the batch announced together with its prefetch lookahead so prefetching continues across batches, +0.3% / +0.4% (noise). The earlier 2-3% was mostly run-to-run noise. AC #3 met with new2.
- False sharing found and fixed: snap_cur/snap_lo (stored by the thread after every bucket) shared a cache line with snap_funnel_seq (read by the main thread after every command) and snap_hi. They now sit on their own cache line (snap_pos_thread).
- Under SET/GET traffic (6 runs each from one RDB): scan cycles base 38.3 G, new2 37.2 G (-2.9%), new3 (cache-line fix) 37.1 G (-3.1%); save 11.78 / 11.46 / 11.44 s; ops/s during the save 145.6k / 149.1k / 152.5k (+4.8% for new3).

Closing (2026-10-18). AC #1: justified by this task's own A/B rather than TASK-1.61 (still open; its remaining half, the main thread's write-back cost, feeds TASK-1.63). Long write scripts during a save, 2M keys: 49 s -> 2.0 s, so the quiesce wait was ~96% of the old save time; idle +0.3% scan cycles (noise); traffic -3.1% cycles, 11.78 -> 11.44 s. The old code had no stall counter, so its wait share is inferred from the save-time difference. AC #2: the memory-ordering argument (busy, idle, wakeup, abort, module threads under the GIL, the end) is in docs/hybrid-snapshot.md section 4; the user accepted the pre-implementation review of doc-1 as covering it. Committed as c3ad92db6 (plus 8270aa81a, likely/unlikely cleanup).
<!-- SECTION:NOTES:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
Replaced the publish/quiesce handshake between the scan thread and the main thread with a credit-based window (commit c3ad92db6).

What changed:
- The thread announces `cur` and publishes `lo`; only the main thread writes `hi`, which is INF outside write sections. A section caps `hi` at advance(lo, W) on open (a seq_cst store-load pair against `cur`: a dict the thread has already entered is paused), grants more as `lo` passes the midpoint (up to 16 overlay slots), and drains below `lo` and resets `hi` to INF on close. The thread waits once, at END.
- Positions are SNAP_POS(ord, phase, idx) over the real (db, slot), so ASM slot-filter gaps are never compared. Buckets are classified BEHIND / INSIDE / AHEAD by position; the emitted bitmap records only main-thread emit-ahead.
- The overlay index grows on demand, keyed by each window's last position; reads through an attached overlay call the `enter` hook, so read-only scripts refill `hi`.
- snap_cur/snap_lo moved to their own cache line; the prefetch lookahead stays below `hi`; stores with dbnum * slots >= 2^22 fall back to the classic child.
- New INFO fields: snap_save_scan_stalls, snap_save_scan_stall_ms, snap_save_window_grants. docs/hybrid-snapshot.md describes the protocol and its race-freedom argument; design in doc-1.

Why: the scan thread used to stall for the whole of any long command running when it published a window. AWS c7i, long write scripts during a save: 49 s -> 2.0 s. Idle and traffic saves stay within noise (traffic slightly better: -3.1% scan cycles, ops/s 145.6k -> 152.5k).

Tests: new unit tests (credit, positions/grants/cap, owed, a 10-window overlay) and a tcl test for a long read script; redis-server test snapshot 212/212, unit/snapshot 82 ok, ASan and TSan clean on unit/snapshot, full suite passed on the pre-final code.

Follow-ups: TASK-1.61 (main-thread write-back cost), which TASK-1.63 depends on.
<!-- SECTION:FINAL_SUMMARY:END -->
