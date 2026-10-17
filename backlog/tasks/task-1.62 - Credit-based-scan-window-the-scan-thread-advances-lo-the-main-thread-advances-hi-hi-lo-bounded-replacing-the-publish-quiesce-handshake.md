---
id: TASK-1.62
title: >-
  Credit-based scan window: the scan thread advances lo, the main thread
  advances hi (hi - lo bounded), replacing the publish/quiesce handshake
status: In Progress
assignee:
  - '@claude'
created_date: '2026-10-17 10:31'
updated_date: '2026-10-17 12:03'
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
- [ ] #1 TASK-1.61's numbers justify the change (recorded in this task's notes); otherwise the task is closed with them
- [ ] #2 docs/hybrid-snapshot.md contains the memory-ordering argument covering busy, idle and wakeup transitions, abort, and module threads holding the GIL, reviewed before implementation
- [ ] #3 With no traffic, save time is within noise of the current protocol (an idle main thread does not slow the scan)
- [ ] #4 Under TASK-1.61's long-command workload the scan thread's waiting time drops to near zero
- [ ] #5 Existing scan, abort and abort-in-section tests pass; new tests cover the idle-to-capped transition and a window crossing tables and dicts
- [ ] #6 TSAN run is clean
- [ ] #7 docs/hybrid-snapshot.md describes the new protocol and the write-section wording that no longer applies is removed
<!-- AC:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
2026-10-17: the user chose to go ahead without waiting for TASK-1.61's numbers (gate in AC1 overridden by the user). Design draft written as backlog doc-1, awaiting review before code. Main findings: (1) sections close only at the outermost call(), and the guard opens them on reads too, so today the thread stalls for whole scripts, transactions and long read-only commands; (2) moving hi only at section close would barely help: the gain needs hi = INF outside sections plus raising hi mid-section, argued safe in doc-1 section 4 (open point: in-place header writes that bypass the funnel).
<!-- SECTION:NOTES:END -->
