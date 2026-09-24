---
id: TASK-1.16
title: 'M1 report: deliverables (a)-(d) evidence and go/no-go on invariants 1 and 2'
status: Done
assignee:
  - '@moti.cohen@redis.com'
created_date: '2026-09-20 17:22'
updated_date: '2026-09-20 23:31'
labels:
  - bgsave
  - persistence
  - docs
  - report
milestone: m-0
dependencies:
  - TASK-1.5
  - TASK-1.10
documentation:
  - poc-hybrid-bgsave-spec.md
parent_task_id: TASK-1
priority: high
ordinal: 17000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Spec §9 says: deliver M1, then STOP and report before any M2 work starts. This task is the report. It collects the evidence produced by the harness (TASK-1.10) and the other M1 tasks into a single checked-in document and states whether the crux holds.

Contents: (a) static-dataset equivalence result and the explanation of why byte-identical output was replaced by reload-equivalence (record ordering, child-side SELECTDB tracking, RESIZEDB/SLOT_INFO placement from the START entry); (b) race-test results: iterations run, write paths covered (cross-reference the write-path audit from TASK-1.1, including header-only mutations of SNAP_FORK keys), sanitizer results, any failures found and fixed; (c) abort behaviour results; (d) CoW measurements for kvobj headers, SNAP_NOFORK values and hash tables with the SNAP_NOFORK arena in place (TASK-1.5); the decisions taken on the spec gaps (slot granularity in standalone mode, LRU/LFU clock, active defrag, listpackEx, expires consistency, platform fallback, kvobj class bit, class-aware free path and its measured cost, staging buffer size); the list of blockers, if any, where invariant 1 (total clone-on-write) or invariant 2 (slot immutability) could not be met; and an explicit go/no-go recommendation for M2. Per spec §10 a spike-killing result is a valid outcome and must be reported honestly, not worked around. No pushing or PRs without approval; the report is a local commit on the work branch.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 A checked-in M1 report exists with sections for deliverables (a), (b), (c) and (d), each with the concrete evidence (test names, iteration counts, measurement tables) from the M1 tasks
- [x] #2 The report lists every design decision taken on the spec gaps named in the epic and the reason for each
- [x] #3 The report contains a blockers section that either lists paths where invariant 1 or 2 could not be met, with evidence, or states that none were found
- [x] #4 The report ends with an explicit go/no-go recommendation for M2 and the conditions attached to it
- [x] #5 The report has been delivered to the requester for review before any M2 task moves to In Progress
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
Report checked in as `poc-hybrid-bgsave-m1-report.md` (sections: what was built, deliverables (a)-(d) with evidence tables, decisions on the spec gaps, blockers and limitations, test status, comparison with the sibling `hybrid-bgsave` branch, go/no-go). Pending before marking Done: the full stock suite run and the 100-iteration `--accurate` race stress (running in the background, results appended to section 8), then a local commit of the report and the two updated docs. Delivered to the requester in the session summary; no M2 task moves to In Progress before their answer.
<!-- SECTION:PLAN:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
M1 report committed as `poc-hybrid-bgsave-m1-report.md` on branch snapshot-hybrid-bgsave. Evidence: static equivalence with functions/module keys and redis-check-rdb; randomized race harness (3 CI iterations, 100 accurate iterations, eviction under LRU/LFU, cluster mode) equal to the fork instant; abort paths incl. killed child, SHUTDOWN and hard mark; CoW 2.7-8.7x (overwrite storm) and 4.9-5.5x (EXPIRE storm on SNAP_FORK headers) lower than stock, baseline-corrected; TSan and ASan clean in arena-free mode; full stock suite 5577/5578 with the one failure a test-timing issue fixed. Recommendation: go for M2 with conditions (production-shaped measurement, eviction-during-save policy, diskless sync and AOF rewrite first). Delivered to the requester in the session report; no M2 task started.
<!-- SECTION:FINAL_SUMMARY:END -->
