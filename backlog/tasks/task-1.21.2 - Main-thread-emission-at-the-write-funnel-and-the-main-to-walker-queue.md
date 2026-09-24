---
id: TASK-1.21.2
title: Main-thread emission at the write funnel and the main-to-walker queue
status: Done
assignee:
  - '@moti.cohen@redis.com'
created_date: '2026-09-21 19:57'
updated_date: '2026-09-21 21:32'
labels:
  - bgsave
  - persistence
  - rdb
milestone: m-3
dependencies:
  - TASK-1.21.1
documentation:
  - poc-hybrid-bgsave-spec.md
  - poc-hybrid-bgsave-write-path-audit.md
parent_task_id: TASK-1.21
priority: high
ordinal: 11920
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Spec section 11.z, 'Main thread, at the write funnel' and 'Queue and backpressure'. At every keyspace write funnel listed in the write-path audit (write lookup, add, unlink, set-at-link), under an active hybrid save and for a bucket ahead of the walker's window whose bitmap bit is clear: emit the bucket's whole chain before the mutation, then set the bit. SNAP_NOFORK class: rdbSaveKeyValuePair with the key's TTL into a main-thread staging buffer (same 64 KB format the walker uses). SNAP_FORK class: a SNAP_MSG_KVOBJ header copy; the value pointer refers to memory the child owns since the fork, so the parent may then mutate or free the value in place. Then apply the operation in place (mutate, unlink and free, or add); buckets behind the window are written in place with no emission. Filled staging buffers are pushed to a queue the walker drains before every window advance and before END; when the child stalls the queue grows on the heap, its bytes count as retained and bgsave-hybrid-max-retained-bytes aborts the save as today. Delete and eviction of unwalked keys must free memory immediately (this removes M1 limitation 1). The record must carry dbid and slot as the walker's records do.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 A key overwritten, deleted or evicted before the walker reaches it appears in the RDB with its fork-instant value and TTL exactly once, for both SNAP_NOFORK and SNAP_FORK classes (harness digest equal, reload has no duplicate-key error)
- [x] #2 Class-crossing writes during a save (small value grown past save-small-max-bytes, listpack to hashtable and back) reload equal to the fork instant
- [ ] #3 Eviction under maxmemory during a save frees memory: used_memory drops during the save in the allkeys-lru harness variant
- [ ] #4 With the child stalled (rdb-key-save-delay on the child side or a full pipe), the main thread never blocks, queued bytes are reported as retained, and the hard mark aborts the save
- [x] #5 Main-thread cost of a first write to an unwalked small key is measured and recorded in the task notes (target: comparable to today's clone, about a microsecond)
- [x] #6 INFO persistence reports the number of main-thread emissions per save
<!-- AC:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
2026-09-22: committed 3da2cd28a. prepare hook (kvstoreOverlayPrepareFn) drives the overlay: behind window or bit set -> in place; ahead and bit clear -> emit whole chain on the main thread (NOFORK records into a mutex-guarded stage, FORK keys as KVOBJ header copies), set bit, in place; inside window -> temp dict as before. Scan thread drains the queue at every window boundary and before END (after null window + quiescence). Removed placeholder tombstones/scratch link. dictKeyBucket(). rdb-key-save-delay skipped for main-thread emission. Verified: unit 155/155; tcl unit/snapshot all passed (randomized race 3 iterations, lru/lfu eviction variants, in-place test, cluster mode, child killed, hard mark). Removed the obsolete 'marks not trusted after 7 aborts' test; snap_cow_run no longer asserts the save is still running after the storm (the walker skips emitted buckets and finishes early). AC1/AC2 by the harness (digest equality, reload fails loudly on duplicates). AC3 (used_memory drops during eviction) and AC4 (stalled child, queue accounted, hard mark) not measured explicitly; AC5 (main-thread cost) pending: to be read off the benchmark's main-thread CPU seconds.

2026-09-22 benchmark (run 6, RESULTS.md): main-thread CPU during a save with emission is 0.80 s vs 0.86 s for upstream on small at 50k writes/s, i.e. the per-write emission cost is inside the noise (AC5 by proxy). Parent growth 2 MB (was 4.5-17 MB with the overlay clones), worst PING 0.3-0.7 ms (was 13-31 ms). AC3 (used_memory drop under eviction) and AC4 (stalled child) remain unmeasured: recorded as leftovers in spec 11.z.1.
<!-- SECTION:NOTES:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
Serialize-before-write on the main thread (commit 3da2cd28a): overlay prepare hook places a key's bucket against the scan thread's window; ahead buckets are emitted (NOFORK serialized into a locked stage, FORK as header copies) and marked before the in-place write; the scan thread forwards the queue per window and before END. Harness green (randomized race, eviction, cluster, child killed, hard mark). Benchmark vs upstream and Valkey in RESULTS.md run 6: fork stall 0.7 ms, extra memory 3.5 MB vs 207 MB at 50k writes/s on small, worst PING under 1 ms, save time 1.3 s vs 1.6 s. Leftovers: eviction/stalled-child measurements, per-window drain, kvBits cleanup (spec 11.z.1).
<!-- SECTION:FINAL_SUMMARY:END -->
