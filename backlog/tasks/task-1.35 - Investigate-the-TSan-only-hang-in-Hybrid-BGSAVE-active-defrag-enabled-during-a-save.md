---
id: TASK-1.35
title: >-
  Investigate the TSan-only hang in 'Hybrid BGSAVE: active defrag enabled during
  a save'
status: Done
assignee:
  - '@Claude'
created_date: '2026-10-05 10:27'
updated_date: '2026-10-11 14:55'
labels:
  - tsan
  - flaky
  - snapshot
dependencies: []
references:
  - tests/unit/snapshot.tcl (test 'active defrag enabled during a save')
  - src/snapshot.c snapWalkerQuiesce / snapFunnelCloseSlow
parent_task_id: TASK-1
priority: medium
ordinal: 31000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Seen once on 2026-10-05 while verifying TASK-1.34 (a pure code move): the TSan build (.scratch/tsan, MALLOC=libc, setarch -R) of unit/snapshot with bgsave-hybrid yes hung in 'Hybrid BGSAVE: active defrag enabled during a save' right after the allkeys-lru/lfu write tests; the harness reported the client as IN PROGRESS and timed out after 2.5 h; the server logs were cleaned by the harness. The same test alone under TSan passed in 13 s with no ThreadSanitizer report, and a full rerun of the unit (--timeout 600 --dont-clean) passed. A hang (not a failed wait_for_condition) means a client command never got a reply, so the main thread was blocked, most likely waiting on the scan thread (write section / walker quiesce / funnel) under TSan's 10x slowdown with rdb-key-save-delay 300 and 1000 SETs + 500 DELs in flight. The ASan tree and the unsanitized build never showed it. The test exists since 4c62e8f6a (2026-09-21) and TSan was clean on 2026-10-03.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 Reproduce under TSan with --timeout 600 --dont-clean (loop the unit; on a hang capture the main thread's and scan thread's backtraces). Outcome: not reproduced in 13 further runs (1 hang in 14 overall) plus two sanitizer passes on later heads; closed as not reproducible by the user on 2026-10-11, the watcher recipe kept
- [x] #2 Root cause identified, or a documented test adjustment. Outcome: none established; the write-section protocol and snapFunnelCloseSlow were read without finding a wait cycle, and the only failures seen in the loop were the known hard-mark timing pair, since fixed (TASK-1.44 commit)
- [x] #3 unit/snapshot under TSan passes 3 consecutive full runs
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
Plan (2026-10-08):
1. Rebuild the TSan tree (.scratch/tsan, MALLOC=libc, incremental rsync recipe from the shell-pitfalls memory) at the current head and loop unit/snapshot with bgsave-hybrid yes, setarch -R, --timeout 600 --dont-clean --port 23111, from a background script (scratchpad tsan-loop.sh). A stall watcher polls the harness log every 20 s; when it is silent for 300 s it records every test server's thread states from /proc (comm, state, wchan per task), tries CLIENT LIST, and sends SIGSEGV so the server's crash report (kept by --dont-clean) carries all thread backtraces before the harness timeout kills it. gdb is available and ptrace_scope is 0, so gdb -p is a fallback.
2. On a capture: read the main thread's and scan thread's stacks; decide between a real wait cycle (main thread in a write section / funnel close / walker quiesce waiting on the scan thread, which waits on the main thread or the child) and a TSan artefact (slowdown makes the test's 50x50 ms wait_for_condition or 600 s harness timeout too short); fix or adjust the test and document.
3. AC3: three consecutive full TSan passes of unit/snapshot on the fixed head.
Loop runs so far: three passes at 44a1bb220 + working tree before stage 2 (runs 1-3 on 2026-10-08, 8-9 min each), no hang; reproduction rate 1 in 6 overall. Next: the loop on the stage 2 head (cdef8de18) for up to 8 more runs.
<!-- SECTION:PLAN:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
2026-10-05: three full TSan runs of unit/snapshot on the same binary (.scratch/tsan at 76cab4e6f): run 1 hung in the defrag test (harness default timeout, logs lost), runs 2 and 3 passed with --timeout 600 --dont-clean and no ThreadSanitizer output. Reproduction rate so far 1/3; the loop in AC1 should keep --dont-clean and attach gdb on the IN PROGRESS client report.

2026-10-08: TSan tree rebuilt at 44a1bb220 (plus the working tree of the day); unit/snapshot under TSan passed 3 consecutive runs (16:33, 16:41, and run 3 in progress), no hang, no ThreadSanitizer report. The watcher never fired. Next loop runs on the TASK-1.15 stage 2 head.

2026-10-08, loop 2 on the stage 2 head (cdef8de18, TSan tree rebuilt 16:49): runs 11-15 passed (6-7 min each). Run 16 FAILED without hanging: the known timing-sensitive pair, 'passing the retained-bytes hard mark aborts the save' (the 2 s save finished before the writes crossed the 64 KB mark: status ok, expected err) and then 'the main thread's emit-ahead queue drains as the walker moves' (snap_save_main_keys <= 50000), the same cluster seen under ASan on 2026-10-06; no ThreadSanitizer report, the defrag test passed. Not the hang this task is about; the hard-mark test's determinism is a separate item. Reproduction count for the hang: 0 in 11 today, 1 in 14 overall.

2026-10-08 loop 2 finished: runs 11-15, 17 and 18 passed on the stage 2 head (cdef8de18), run 16 failed on the known hard-mark timing pair; no hang in 11 runs today (about 80 minutes of TSan unit/snapshot), 1 hang in 14 runs overall (2026-10-05). AC3 met (five consecutive passes before run 16, two after). AC1 and AC2 open: the watcher (tsan-loop.sh / tsan-loop2.sh in the session scratchpad: stall detection on a silent harness log, /proc thread states, SIGSEGV for an all-thread crash report) never fired. Options for the next step, for the user: keep the loop running unattended (overnight), or review snapWalkerQuiesce / snapFunnelCloseSlow / the write-section protocol for a wait cycle by reading, or close as not reproducible and keep the watcher recipe.
<!-- SECTION:NOTES:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
Closed as not reproducible (user decision, 2026-10-11). One TSan-only hang in 'active defrag enabled during a save' on 2026-10-05; afterwards 13 loop runs with a stall watcher (2026-10-08) and TSan passes of unit/snapshot on 39f68a939 (2026-10-10) and e6803282e (2026-10-11), each 75 ok with 0 reports, never hung. The only loop failure was the hard-mark timing pair, whose determinism fix landed with TASK-1.44. The watcher recipe (tsan-loop*.sh in the session scratchpad: stall detection on a silent harness log, /proc thread states, SIGSEGV for an all-thread crash report) is described in the plan and the shell-and-test-pitfalls memory note, so a recurrence can be captured. The scratch scripts now need the renamed config hybrid-snapshot.
<!-- SECTION:FINAL_SUMMARY:END -->
