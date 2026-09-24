---
id: TASK-1.20
title: >-
  Per-key scan generation in kvBits (3 bits): in-place writes to keys the scan
  already passed
status: Done
assignee:
  - '@moti.cohen@redis.com'
created_date: '2026-09-21 13:01'
updated_date: '2026-09-21 20:49'
labels:
  - bgsave
  - persistence
  - performance
milestone: m-1
dependencies: []
parent_task_id: TASK-1
priority: high
ordinal: 11800
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Requested 2026-09-21 after the sibling-branch comparison: use a per-key generation (kvBits.save_gen, 3 bits, upstream kvBits layout) so a write to a frozen key the scan thread has already serialized is applied in place instead of cloned into the overlay. Deletes still tombstone (the frozen bucket chain cannot be unlinked under the thread); adds still go to the temp dict.

Design: `server.snap_gen` bumps at every hybrid save start before the freeze; the scan thread stamps each key as its last touch (release store of the kvBits byte); the main thread checks with an acquire load in the overlay write lookup (`kvstoreDictFindLink`) and hands out the frozen entry itself when gen matches. kvBits is accessed only through byte-wide atomics (`kvobjBitsGet/Set`, relaxed; `GetAcquire/SetRelease` for the mark). Marks are trusted only while `snap_gen_since_success < 7`: a completed save stamps every key, an aborted one leaves a mix, and the 8th generation after 7 aborts would alias a stale mark; an untrusted save clones everything (previous behaviour) and its completion resets the counter. A delete of a scanned key clones header-only sharing the value (the clone frees the value when deleted, the original keeps its header until merge), so eviction/expiry of scanned keys returns the value memory immediately. `snapKvobjIsOwed` gives per-key precision to the lazy field-expiry checks. INFO: snap_gen, snap_gen_since_success, snap_save_marks_valid, snap_save_inplace_writes, snap_last_save_inplace_writes, snap_inplace_writes_total.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 Writes during a save to keys the scan already passed are served in place (INFO snap_save_inplace_writes > 0) and the reloaded RDB still equals the fork instant
- [x] #2 After 7 saves without a completion marks are not trusted (all writes clone), a completed save resets the counter
- [x] #3 kvBits accessed only through atomics; TSan run of unit/snapshot in arena-free mode clean
- [x] #4 Unit check pins the kvBits bit layout the atomic mask relies on
- [x] #5 Benchmark on small/mixed shows lower retained bytes and merge cost than the clone-everything baseline
<!-- AC:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
Committed on snapshot-hybrid-bgsave (see git log, 'kvBits, in-place writes'). Effect on the small dataset (all SNAP_NOFORK), hybrid vs clone-everything hybrid, tmpfs, plain build: at 20k writes/s extra memory 9 -> 6 MB, retained peak 7.6 -> 4.0 MB, worst PING 13.7 -> 6.7 ms; at 50k writes/s extra memory 13.5 -> 9 MB, parent growth 9 -> 4.5 MB, retained peak 18.5 -> 10 MB, worst PING 31 -> 19-22 ms (hybrid-merge). Mixed unchanged in memory (dominated by SNAP_FORK CoW in the child), merge spike 7.6 -> 3.1 ms at 20k. Save time unchanged. Roughly half of uniform writes bypass the overlay, as expected for a linear scan.

AC #3 pending: the first TSan run found the header clone's memcpy reading the kvBits byte while the thread stamped it (fixed with an atomic copy) and then a real bug: MOVE/RENAME hold the object across the delete, and deleting a scanned key through a header-only shared clone freed the value under them. Replaced by the placeholder tombstone (ovTombstoneInPlace); unit and tcl suites pass, the TSan rerun of that version has not been done yet (user asked to record the work first).

2026-09-21: follow-up design (spec 11.z, TASK-1.21) turns the marks into the emission arbiter and removes the snap_gen_since_success trust counter in favour of walk-to-completion stamping. AC #3 (TSan rerun of the placeholder-tombstone version) still pending for this task as delivered.

2026-09-21: AC #3 closed by the TSan run of unit/snapshot (arena-free, libc) on commit 2e69194bc, which contains the placeholder-tombstone version: all tests passed, no ThreadSanitizer reports (102 s). Note: TASK-1.21.4 will remove save_gen and the kvBits atomics again in favour of per-save bucket bitmaps.
<!-- SECTION:NOTES:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
Delivered on snapshot-hybrid-bgsave (de1ab4f3a and follow-ups): 3-bit kvBits.save_gen scan generation, in-place writes to keys the scan thread already passed, byte-wide kvBits atomics, marks trust counter (7 saves), placeholder tombstones for deletes of scanned keys, INFO fields. Benchmarks: small dataset at 50k writes/s parent growth 9 to 4.5 MB, retained peak 18.5 to 10 MB, merge spike 31 to 19-22 ms. TSan and ASan clean in arena-free mode. Superseded by the bucket-bitmap design of TASK-1.21 (spec 11.z), which removes save_gen again.
<!-- SECTION:FINAL_SUMMARY:END -->
