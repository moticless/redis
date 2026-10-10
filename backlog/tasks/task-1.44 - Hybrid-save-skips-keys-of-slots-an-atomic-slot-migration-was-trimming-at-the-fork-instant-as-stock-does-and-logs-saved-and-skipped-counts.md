---
id: TASK-1.44
title: >-
  Hybrid save skips keys of slots an atomic slot migration was trimming at the
  fork instant, as stock does, and logs saved and skipped counts
status: Done
assignee:
  - '@Claude'
created_date: '2026-10-10 08:26'
updated_date: '2026-10-10 08:58'
labels:
  - hybrid-bgsave
  - cluster
  - parity
milestone: m-1
dependencies: []
references:
  - >-
    tests/unit/cluster/atomic-slot-migration.tcl 'AOF rewrite during active trim
    should skip keys in trimmed slots' (fails with bgsave-hybrid yes)
  - >-
    src/rdb.c rdbSaveRio and src/aof.c rewriteAppendOnlyFileRio:
    isSlotInTrimJob(curr_slot) skip and the 'keys saved, keys skipped' log lines
  - >-
    src/snapshot.c snapSaveDispatch, snapSaveEmitBucket, snapSaveCreate,
    snapOnChildEnd
  - >-
    docs/hybrid-bgsave.md sections 5 and 6, poc-hybrid-bgsave-spec.md section 6
    (stage 2 paragraph)
modified_files:
  - src/snapshot.c
  - src/zmalloc.c
  - tests/unit/snapshot.tcl
  - docs/hybrid-bgsave.md
  - poc-hybrid-bgsave-spec.md
parent_task_id: TASK-1
priority: high
ordinal: 40000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Found by the first full-suite run with bgsave-hybrid yes after the review refactors (2026-10-10). Stock's RDB writer and AOF command-form rewriter both skip every key whose slot has an atomic-slot-migration trim job (the child evaluates the fork-instant copy of the trim set) and log 'N keys saved, M keys skipped'. The hybrid save kept those keys in both forms (the design document and spec said so for the AOF form, believing stock's RDB kept them too; it does not), so a node restarted from a hybrid base file holds keys of slots it no longer owns, and the cluster test asserting countkeysinslot 0 for the trimmed slots fails.

Outcome wanted: parity with stock. The main thread captures the set of slots in a trim job at the fork instant; the scan thread and the main thread's emit-ahead skip those keys (nothing of them reaches the child), a skipped counter is kept, and the save's end logs stock's 'keys saved, keys skipped' line for BGSAVE and for the AOF rewrite. A trim that starts after the fork instant is not skipped, as in stock (its keys are in the fork-instant snapshot); the existing unit/snapshot cluster test for that case stays valid. The design document and spec state the rule.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 With bgsave-hybrid yes, tests/unit/cluster/atomic-slot-migration.tcl passes, including the AOF rewrite during active trim test (keys of slots 0 and 1 skipped, slot 2 kept, the log line matches stock's pattern)
- [x] #2 unit/snapshot's cluster test 'slots trimmed by atomic slot migration during a save stay in the dump' still passes (trim started after the fork instant)
- [x] #3 The parent logs 'keys saved, keys skipped' at the end of a successful hybrid BGSAVE and AOF rewrite, in stock's wording
- [x] #4 docs/hybrid-bgsave.md and the spec describe the rule: slots in a trim job at the fork instant are skipped, a trim that starts during the save is not
<!-- AC:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
Commit 163494aef. snapSave gains trim_slots (a CLUSTER_SLOTS-bit bitmap, NULL when no trim job exists at save start) and an atomic keys_skipped counter. snapSaveCreate fills the bitmap on the main thread when server.cluster_enabled && asmIsTrimInProgress(), via isSlotInTrimJob per slot; this is the fork-instant set, the same set stock's child reads from its copy-on-write copy of the trim jobs. snapSaveSlotTrimmed(sv, didx) is tested in both producers: snapSaveDispatch (scan thread, SNAP_NOFORK and SNAP_FORK dispatch) returns before staging, snapSaveEmitBucket (main thread emit-ahead at write sections) skips the entry; both count. Nothing of a skipped key reaches the child, so the END counts and the child's per-db bookkeeping are unaffected. snapOnChildEnd, on a clean exit, logs "%s done, %lld keys saved, %lld keys skipped." with "AOF rewrite" or "BGSAVE" as the subject, saved = keys_nofork + keys_fork + main_keys; the cluster test greps that pattern in the parent log. A trim that starts after save start is not in the bitmap: its keys stay in the dump, the existing unit/snapshot test for that case passed unchanged. Docs: the ASM trim row of the interaction table in docs/hybrid-bgsave.md and the stage-2 paragraph of the spec state the rule.

Bundled in the same commit (found by the same full-suite pass): zmalloc's per-arena resident figure read a nonexistent mallctl name ("stats.arenas.<i>.small.resident") so allocator_resident_nofork was always 0, now "stats.arenas.<i>.resident" with the result asserted and the INFO test checking it is populated; the hard-mark test uses an 8 KB mark and a 5 ms per-key delay so the first window crosses the mark whatever the client's speed and restores its configs before asserting; the CoW comparison uses 50000 small keys so stock's copies dominate the child's own page noise.
<!-- SECTION:NOTES:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
Parity gap with stock closed: a hybrid BGSAVE or AOF rewrite now skips the keys of slots an atomic slot migration was trimming at the fork instant, as stock's child does, and the parent logs stock's "N keys saved, M keys skipped" line. The gap was found by the first full-suite run with bgsave-hybrid yes after the review refactors (one real failure out of 6097 tests). The fix is producer-side: the main thread captures the trim-job slot set at save start, and both producers (scan thread, main-thread emit-ahead) drop and count those keys, so the child and the two channels are untouched. A trim starting during the save is not skipped, as in stock.

Verification: unit/cluster/atomic-slot-migration with the hybrid on 107 ok 0 bad; unit/snapshot hybrid 81 ok and stock 81 ok; integration/aof-multi-part hybrid 41 ok; redis-server test snapshot 184 passed. Same commit carries the nofork resident statistic fix (was always 0) and two test-determinism fixes. Follow-up: rerun the whole suite with bgsave-hybrid yes to confirm zero failures end to end (the stock run was already 6309 ok 0 bad).
<!-- SECTION:FINAL_SUMMARY:END -->
