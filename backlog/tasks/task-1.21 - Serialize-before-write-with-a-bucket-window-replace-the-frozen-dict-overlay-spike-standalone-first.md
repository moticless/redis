---
id: TASK-1.21
title: >-
  Serialize-before-write with a bucket window: replace the frozen-dict overlay
  (spike, standalone first)
status: In Progress
assignee:
  - '@moti.cohen@redis.com'
created_date: '2026-09-21 19:56'
updated_date: '2026-09-25 08:31'
labels:
  - bgsave
  - persistence
  - db
  - memory
  - performance
milestone: m-3
dependencies: []
documentation:
  - poc-hybrid-bgsave-spec.md
  - poc-hybrid-bgsave-write-path-audit.md
  - utils/snap-bench/RESULTS.md
parent_task_id: TASK-1
priority: high
ordinal: 11900
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Design agreed 2026-09-21 and recorded in poc-hybrid-bgsave-spec.md section 11.z. Motivation: the current hybrid save (frozen per-slot dicts, clone-on-write into temp dicts, merge at slot completion) has a merge latency spike of 13-31 ms on a 1.4M-key standalone db (stock fork stall is about 5 ms), keeps replaced originals alive until merge, and in standalone mode owes the whole db until scanned.

New scheme (Dragonfly-style bucket versioning adapted to a second thread): the walker publishes a bucket window; the main thread, on the first post-fork write or delete of a key outside the window, emits the key's snapshot record itself (SNAP_NOFORK: serialized into a staging buffer; SNAP_FORK: header message, the child owns the value), stamps kvBits.save_gen with the save generation, then mutates in place. Keys inside the window are duplicated into a small side table drained once the window moves. The walker skips stamped keys. No temp dicts, no merge, no retained originals beyond the side table and the main-to-walker queue. Every save walks to completion (stamping only) even when aborted so marks never alias; the marks trust counter goes away.

Invariants, handshake (seq_cst quiescence counter, no ack from main), queue and backpressure rules, and what is kept and removed are all in the spec addendum. Order: standalone single db first, TSan and the tcl harness on every step, cluster slots last. No pushes or PRs; local commits on snapshot-hybrid-bgsave.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 The randomized race harness (tests/unit/snapshot.tcl) reloads equal to the fork instant with the new scheme, including eviction, cluster-mode and child-killed variants
- [x] #2 No hybrid-merge latency event exists any more and the worst PING during a save on the small dataset at 50k writes/s is at or below stock's fork stall (recorded in utils/snap-bench/RESULTS.md)
- [x] #3 Retained bytes during a save are bounded by the side table plus queue and the peak on the small dataset drops versus the overlay numbers in RESULTS.md
- [x] #4 TSan and ASan runs of unit/snapshot in arena-free mode are clean
- [x] #5 Spec section 4 is rewritten to describe the new protocol and the M1 report gets a pointer to the change
- [ ] #6 Per-save bucket bitmaps replace the per-key generation: kvBits returns to the upstream layout, no kvBits atomics remain, an aborted save leaves no state behind and the next save reloads correctly
<!-- AC:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
2026-09-21: design change agreed before implementation started: the 'already emitted' arbiter is a per-save bitmap with one bit per bucket, not kvBits.save_gen. Emission is per bucket chain (adds included). Spec 11.z rewritten accordingly; 11.y marked superseded.

2026-09-22 PoC status: 1.21.1 and 1.21.2 done (window, write sections, bitmaps, main-thread emission), harness green, benchmark run 6 in RESULTS.md against upstream 0d6266f2d and Valkey fork/forkless. AC2: no hybrid-merge spike, worst PING 0.3-0.7 ms vs 3-4 ms fork stall of stock (small, 50k writes/s). AC3: parent growth 2 MB, extra memory 3.5 MB vs 207 MB stock. Pending: AC4 (TSan rerun after the emission fixes, ASan not run), AC5 (spec section 4 still describes the overlay protocol; 11.z and 11.z.1 hold the new one, M1 report has the pointer), AC6 (kvBits cleanup, TASK-1.21.4), 1.21.3 drain, 1.21.6 cluster verification. The user asked to strive to a PoC and document leftovers: leftovers are in spec 11.z.1.

2026-09-22 final verification on fa21935d0: TSan (arena-free, libc) unit/snapshot all passed, no reports (86 s); test build unit 155/155; tcl unit/snapshot 31 tests passed (79 s). ASan not rerun on the new code (leftover). AC1 checked (harness incl. eviction, cluster mode, child killed).

2026-09-24: comparison with yinon/forkless-hybrid recorded in spec 11.z.3. Two bugs found on the module-metadata path and fixed (commit on 2026-09-24): placement ignored module key metadata (small value left in the arena while routed to the child) and EMBSTR values shipped as kvobj copies kept parent-side pointers (child segfault in rdbSaveStringObject). New harness test with test_keymeta.so. Unit 155/155, tcl suite 32 tests green.

2026-09-25: AC4 closed (TSan clean 2026-09-22, ASan clean in arena-free mode 2026-09-25, harness --accurate 100 iterations green); AC5 closed (spec section 4 rewritten for the bucket-window scheme, 18777d3c3). TASK-1.21.5 Done. Remaining for this task: AC6 kvBits save_gen cleanup (TASK-1.21.4) and the optional overlay removal (TASK-1.21.3); cluster mode is TASK-1.21.6.
<!-- SECTION:NOTES:END -->
