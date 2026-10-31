---
id: TASK-1.65
title: >-
  Per-key save epoch at the kvobj tail: key-granular emit-ahead replaces the
  per-bucket emitted bitmap (ahead of the swiss-table hashtable)
status: In Progress
assignee:
  - '@claude'
created_date: '2026-10-30 13:35'
updated_date: '2026-10-31 23:25'
labels:
  - bgsave
  - persistence
dependencies: []
documentation:
  - docs/hybrid-snapshot.md
  - src/hybrid_snapshot.c
  - src/kvstore_overlay.h
  - src/object.h
  - src/object.c
  - ~/git/valkey/src/bgiteration.c
parent_task_id: TASK-1
priority: medium
ordinal: 61000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
## Why

Today, when the main thread writes a key whose bucket is ahead of the scan thread (AHEAD), it serializes the whole bucket chain, sets the bucket's bit in a per-dict emitted bitmap, and then writes in place (hybsnapSaveEmitBucket, hybsnapMarkBucketEmitted and the related functions in src/hybrid_snapshot.c). With today's dict a chain holds about one key, so that is close to per-key cost.

Redis is expected to replace dict with a swiss-table hashtable like Valkey's: 7 slots per bucket plus child buckets, so about 5 keys per bucket at the fill limit. Bucket granularity would then serialize about 5x more on the main thread for every first write to an AHEAD bucket. The bitmap also cannot follow keys that move between slots on resize or compaction. That is why it needs the remap on growth and the shrink block (hybsnapScanDictRehashStarted, hybsnapGuardShrinkAllowed).

Goal: make emit-ahead key-granular. The main thread JIT-serializes only the key being written and stamps it. The scan thread later skips stamped keys. The sliding window, write sections, the bucket overlay (DEFER for INSIDE buckets), slot states and invariant 8 (the main thread never waits) stay unchanged.

## Design decided with the user (2026-10-30)

- **Per-key 32-bit save epoch.** The global save id is bumped at the fork instant. A key with epoch > save_id is already in the snapshot or was born after the fork instant, so the scan skips it. A key with epoch <= save_id, or with no epoch at all, is fork-instant and still pending save. Stale stamps from earlier saves are always <= the current id, so they are never cleared.
- **Who stamps.**
  - The main thread stamps a key in the write funnel when its bucket is AHEAD, before the in-place write. A small value is serialized immediately; a fork-class value gets its kvobj header copied, as today.
  - A key created during a save is stamped at creation.
  - BEHIND buckets need no stamp. INSIDE buckets go through the overlay, where the thread reads the frozen original.
  - The scan thread never writes epochs.
- **No atomics.** Main stamps only buckets at or above `hi`. The thread reads a bucket only after acquiring `hi` past it, so the window already orders them. The check stays in the write funnel, not at call() start, so that scripts, modules and writes outside call() are covered.
- **Rehash.** Pausing rehash on the dict being scanned (SCANNING) is sufficient.
  - PENDING dicts haven't been scanned yet, so keys may move freely: stamps travel with the keys.
  - SCANNED/DONE dicts are never revisited.
  - Expansion while rehash is paused only adds new keys, which are stamped.
  - Consequence: the bitmap remap, the shrink block and the rehash new-bucket marking in the prepare hook go away.
- **Layout: a dedicated kvBits bit, not module key metadata.**
  - `has_epoch:1` takes one of the 5 unused kvBits bits.
  - The 4 bytes sit at the END of the kvobj: `[meta slots][robj][kvBits][key sds][embedded value][epoch 4 if has_epoch]`.
  - Reasons: kvobjGetKey() is unchanged (a hot path for every user); attaching into existing allocation slack is a plain 4-byte write and sets no pointer; module metadata slots would cost 8 B and a module class.
  - Without an embedded value the epoch sits at key + sdslen(key) + 1. For EMBSTR, the value's sds currently takes all the slack (object.c, kvobjCreateEmbedString, remaining_size), so with has_epoch its alloc is reduced by 4 and the epoch sits in the last 4 bytes.
  - Unaligned access goes through memcpy. The kvobjSet embed threshold and kvobjAllocSize must account for the 4 bytes.
- **Attach and keep policy.**
  - When the hybrid snapshot is enabled, or when the 4 bytes fit into the allocation's usable size, allocate the epoch at creation.
  - Otherwise attach it on the first write during a save: in place when there is slack, else realloc in the funnel before the lookup returns (AHEAD bucket, so the thread is not there; same pattern as kvobjSetExpire/kvobjSet).
  - The epoch is never dropped just to reclaim memory; no dedicated sweep or extra realloc for that.
- **Keep or drop when another flow reallocates or recreates the kvobj.**
  - These flows already pay for a new allocation and a pointer update: kvobjSet (overwrite, embed decision), kvobjSetExpire adding the expire slot, active defrag, RENAME/MOVE/COPY/RESTORE, and type or encoding conversions that rebuild the kvobj. Each one is a free chance to decide again whether the 4 bytes are still needed.
  - Rule: carry the epoch if the current save stamped it (a save is running and epoch > save_id). Otherwise keep it only if the creation policy above would allocate it (hybrid snapshot enabled, or it fits in the new allocation's slack). In every other case, drop it.
  - This is correct even during a save. No epoch means the same as epoch <= save_id: fork-instant, pending save. So dropping a stale stamp never changes what the scan emits.
  - All recreation paths go through one helper that applies the rule, so a new flow cannot silently lose a stamp the current save needs.
  - Expected cost: about 4 B per touched key on average, at most one jemalloc size-class step.

## Not the same as the removed kvBits.save_gen (TASK-1.21.4)

That design needed byte-wide atomics on every kvobj creation and scan-thread writes, and a small generation that wrapped (hence the trust counter and the clone-everything fallback). This one has no atomics (ordering via the window), no scan-thread writes, a 32-bit id with no practical wrap, and leaves the upstream kvBits fields untouched apart from one bit.

## Companion change (in scope)

The overlay is bucket-granular too. DEFER duplicates the chain and clones every key in it (src/kvstore_overlay.h, near the top), which would be about 5 clones per deferred write on swiss buckets. Make the clone lazy: the duplicate bucket shares pointers to originals that are only read, and clones a key on its first write.

## Prior art

Valkey bgiteration.c uses a uint32_t iterator_epoch per entry, consistent_modification_id = epoch++ at iterator creation, skips entries with a newer epoch, and expedites writes to unpassed keys.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 Emit-ahead serializes only the written key: a unit test with several keys in one AHEAD bucket shows exactly one record emitted by the main thread, and the remaining keys are emitted once by the scan thread
- [x] #2 The per-dict emitted bitmap, its remap on growth and the shrink block are removed; INFO no longer reports bitmap bytes (or reports a replacement metric documented in docs/hybrid-snapshot.md)
- [ ] #3 Every save reloads equal to the fork instant with no missing or duplicate key under a heavy write load, including writes to AHEAD buckets, new keys, deletes of stamped keys, overwrites (SET/RENAME/MOVE/COPY/RESTORE), defrag and slot migration during the save
- [ ] #4 kvobjGetKey() is unchanged; has_epoch uses one kvBits unused bit; EMBSTR objects with an epoch keep a correct sds alloc and kvobjAllocSize/MEMORY USAGE are correct (unit tests cover with/without epoch, EMBSTR and RAW, short and long keys)
- [x] #5 Ten or more consecutive saves (some aborted) followed by a successful one reload equal: stale epochs from earlier saves are treated as fork-instant
- [ ] #6 No new atomics on the kvobj creation path or in the scan thread; the main thread never waits for the scan thread (invariant 8)
- [x] #7 Memory overhead measured on the snap-bench matrix (keys touched during a save vs idle, hybrid on vs off) and save time/latency compared to the bucket-emit baseline; results recorded in the task notes
- [x] #8 docs/hybrid-snapshot.md updated: epoch semantics, layout, attach/keep policy, rehash pause rule, and why it differs from the removed save_gen
- [x] #9 Rehash/resize is paused only on dicts the window covers (SCANNING and those inside the current grant); attached-but-unreached dicts grow and shrink freely; a test grows and shrinks PENDING and DONE dicts during a save and the reload is equal
- [x] #10 Every flow that rebuilds a kvobj through kvobjSet()/kvobjCreate() (TTL attach, keymeta attach, overwrite with new metabits, RENAME/MOVE/COPY/RESTORE, encoding conversions) decides again: during a save it carries the running save's epoch, outside a save the creation policy decides whether to keep the 4 bytes. Defrag and in-place writes keep the bytes as they are. A test rebuilds stamped keys during a save and the reload has no duplicate
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
Approved 2026-10-30. Branch proto-key-epoch (baseline proto-bucket-cow 37dcc99a2).

PHASE 1 (per-key epoch; benchmarked):
1. Layout (object.c/h): kvBits.has_epoch from the unused bits; 4 bytes at the kvobj tail (key + sdslen + 1 without an embedded value; for EMBSTR the value's sds alloc gives up its last 4 bytes, so the epoch is at ptr + sdsalloc + 1 - 4). kvobjGetEpoch/kvobjSetEpoch via memcpy. kvobjAllocSize and the embed threshold count the 4 bytes. kvobjCreate/kvobjCreateEmbedString add the epoch per the creation policy and stamp the current stamp value.
2. Semantics: global epoch bumped at the freeze = E; objects created during the save get E; the scan skips a key with epoch >= E (check in hybsnapScanEntryCb); a missing epoch is fork-instant.
3. Prepare hook per key: AHEAD + key exists + unstamped -> emit only that key (hybsnapSaveEmitKey), stamp, INPLACE; an insert emits nothing; INSIDE -> DEFER (also when the bucket was emitted ahead earlier in the same section); BEHIND -> INPLACE. The unreached mid-rehash special branch goes away. hybsnapKvobjIsPendingSave uses the stamp.
4. Attach: in place when there is slack, else realloc in the funnel, repointing the keys link, expires, subexpires and slot accounting (as the clone hook does). The old header is retired at section close, not freed immediately. Creation policy behind a hidden config: (A) prealloc whenever the hybrid snapshot is enabled; (B) on demand: during a save or when it fits in slack. One keep/drop helper serves kvobjSet, kvobjSetExpire, the clone and defrag: carry the epoch if the current save stamped it, else apply the creation policy.
5. Remove the bitmap and its alloc, remap and shrink block, the rehash_started/rehash_completed/shrink_allowed hooks (kvstore.c, kvstore_overlay.h), and the bitmap checks in prefetch. INFO: drop snap_save_bitmap_bytes and snap_save_grown_dicts; snap_save_main_buckets becomes a key count; add snap_save_keys_skipped and an epoch-realloc counter. Update rehash-pause.sh.
6. Pausing: the window still pauses every dict it covers (positions are (table, idx)); an attached-but-unreached dict is no longer paused and may now also shrink.
7. Tests and docs: rewrite the C unit tests that use the bitmap; epoch layout tests; a multi-key AHEAD bucket test; Tcl tests; docs/hybrid-snapshot.md. Build with -Werror, unit tests, Tcl suite.

PHASE 2 (separate commit, after the AWS run): lazy per-key clone in the overlay (AC8).

BENCHMARK: proto-bucket-cow vs proto-key-epoch with policy A and with policy B, snap-bench matrix on a c7i.4xlarge: save time, p99/p999 during the save, used_memory/RSS idle and during the save, main-thread keys emitted.
<!-- SECTION:PLAN:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
2026-10-30 Phase 1 implemented on proto-key-epoch (uncommitted). Deviations from the plan: (1) the scan checks epochs only when ctx->skip_stamped is set (the save sets it; the scan unit tests use plain string keys). (2) No separate keep/drop helper: every kvobj created while a save runs gets the save's epoch (hybsnap_epoch_mode ALWAYS from freeze to release), which covers kvobjSet/kvobjSetExpire/RENAME/MOVE/COPY/RESTORE recreation; outside a save the creation policy applies; defrag and the overlay clone copy the whole allocation, epoch included. (3) The embed threshold in kvobjSet does not count the epoch (counting it turned 30-byte values RAW: a separate allocation is worse than the epoch spilling past the cache line). (4) An in-place attach updates the slot accounting (kvobjAllocSize grows by 4 except for EMBSTR). INFO: removed snap_save_bitmap_bytes, snap_save_grown_dicts, snap_save_main_buckets; added snap_save_scan_stamped_keys, snap_save_epoch_reallocs. Hidden config hybrid-snapshot-epoch-prealloc (default yes). Removed the rehash_started/rehash_completed/shrink_allowed hooks and dictType.shrinkAllowed. Unit tests 213/213 (new epoch layout + scan skip tests); Tcl unit/hybrid_snapshot: new emit-ahead tests pass for both policies; one failure (OBJECT ENCODING embstr) fixed by (3), rerun pending.

Keep/drop of the 4 epoch bytes on reallocation (decided 2026-10-30): every flow that rebuilds a kvobj through kvobjSet()/kvobjCreate() (TTL attach via kvobjSetExpire, keymeta attach, overwrite with new metabits, RENAME/MOVE/COPY/RESTORE, conversions) re-decides per the current mode, so under prealloc=no with no live save the bytes are dropped unless the new allocation has slack. Defrag (activeDefragKvobj) copies the allocation verbatim and KEEPS the epoch by decision - simpler code, no per-flow helper. In-place writes (APPEND on RAW, HSET) also keep it.

2026-10-30: AC1 unit test hybsnapEpochEmitAheadTests: 6 keys hashed into one bucket ahead of the thread; the real prepare hook emits and stamps only the written key; a second write and an insert emit nothing; the thread emits the other 5 once and skips the stamped one. Unit tests 217/217. Phase 1 committed on proto-key-epoch.

Run 165 (2026-10-30, c7i.4xlarge 54.176.49.7, CSVs utils/snap-bench/run165-*.csv): hybrid-before 37dcc99a2 vs 9f75125c8 policy A (prealloc yes) and B (prealloc no), 4 GB small/mixed/mixed1m, 0/20k/50k x2, mixed 50k HOT=0.2. All saves ok. (1) Main-thread emits 30-37% fewer keys (old build emitted ~1.55 keys per bucket; new key count ~= old bucket count). (2) p99/p999/max latency and main-thread CPU equal. (3) used_memory before save: A +38..47 MB (+0.9..1.2%), B = before. (4) Peak during save: small A +3..5 MB extra (before +8), B +72..79 MB: B reallocs ~25% of the keys it stamps (71k at 20k, 172k at 50k), each rewrites a dict-table pointer shared with the fork child -> ~70 MB child CoW; on mixed the big-value CoW (0.7-1.5 GB) dominates for all. Conclusion: keep A as default. (5) OPEN: mixed1m save time A 25.0-26.5 s vs before 22.4-22.9 s in 3 of 4 cached cells (20k rep1 equal at 26.1 vs 26.5); small and mixed show no difference. Not yet explained; needs more reps and a profile. bench.py change: LIVE_STATS keeps the in-progress snap_save_* counters seen during the save (they read 0 once it ends); counters valid from small r20000 rep 2 on.

Run 165p (2026-10-31, same host; CSVs utils/snap-bench/run165r-mixed1m.csv, run165p-mixed1m.csv): the mixed1m save-time gap was noise. 8 interleaved reps at 50k, rep 1 warm-up dropped: before 24.99 +-1.71 s (22.3-26.7), A 24.79 +-1.39 s (22.8-26.3); scan thread done at 20.48 vs 20.40 s, child tail after the scan 4.51 vs 4.38 s; main CPU 10.99 vs 10.92 s; main-thread emits 317k vs 208k (-34%). perf (system-wide on the server cores, one profiled save each): child 45.8% vs 46.7% of samples, scan thread 30.2% vs 32.6%, main 17.5% vs 15.2%; child top functions identical (lzf_compress 28%, entryGetValue 5%, crc64 3%, fwrite 3%); scan thread hybsnapScanThreadMain 4.6% -> 6.3% (the entry callback, now with the epoch read, is inlined there; hybsnapKeyClass 1.7% no longer shows separately). The save's critical path on mixed1m is the child serializing big values after the scan ends. AC8 done: no save-time or latency regression; A costs about 1% memory idle and has the lowest peak.

2026-11-01: AC#7 (lazy per-key overlay clone, Phase 2) moved to TASK-1.56 (AC#3) and parked: INSIDE hits are 0.001-0.004% of writes (run 165).

2026-11-01: four Tcl tests in tests/unit/hybrid_snapshot.tcl (after the emit-ahead test), each checked against a mutant in a throwaway worktree: (1) 'keys given an epoch in a new allocation keep their TTL and field TTLs, active expiry reaches them after the save' (prealloc no; 1000 volatile keys and 320 hash fields, listpack and hashtable, expire actively without being touched) - mutant: hybsnapKeyRepoint skips db->expires/db->subexpires -> ASSERTION t_hash.c:4157 meta.trash. (2) AC#10 'keys rebuilt during the save (RENAME, MOVE, COPY, RESTORE, SET, EXPIRE) reload with the fork instant, none twice' x prealloc yes/no; stamped keys and pending neighbours, MOVE/COPY into db 12 which is scanned later - mutant: scan skips only epoch > E -> 'Duplicated key found in RDB file'. (3) AC#5 'epochs left by ten earlier saves (seven aborted) are fork-instant to the next one' (policies alternate; stamps, keys born under each epoch, a pending delete; the final save also writes ahead) - mutant: any epoch reads as stamped -> 382 keys missing. (4) AC#9 'a dict the walker finished and one it has not reached grow and shrink during the save' (db 2 DONE, db 12 PENDING, 4 -> >=1024 buckets and back, a stamped seed rehashed) - mutant: pause on first touch -> no growth. All four pass on HEAD.

2026-11-01: REGRESSION found and fixed (uncommitted at time of writing): with the bitmap an ahead bucket handed out for an in-place write was marked emitted, so the thread skipped it even after hi passed it. With per-key epochs the thread walks such a bucket's chain and reads its other keys, but grants still ran at every hook inside a section (hybsnapSectionOpen's in-section branch, incl. kvsoEnter in kvsoDictSetAtLink / kvsoDictTwoPhaseUnlinkFreeKey), so a command could raise hi past a bucket it had just been handed and then write through its link or kvobj pointer (replace, insert, unlink, free) while the thread read it. Probe (throwaway worktree; logs a grant passing an ahead bucket handed out earlier in the same command): 1M keys, save with no delay, 300 MULTI blocks of 200 random SETs -> 1 hit in 243 grants (the grant fired in setKey's kvsoEnter after SET's lookup). Fix: grants only between statements (hybsnapSectionGrant, called before each EXEC statement in multi.c and each script statement in script.c, not for EXEC or scripts run by RM_Call, whose module may hold a key open); hooks inside a command never move hi. Probe on the fix: 285 grants, 0 hits. Unit 218/218, unit/hybrid_snapshot 89 ok, multi/scripting/functions with hybrid on 551 ok. AC#8: docs/hybrid-snapshot.md updated (stale bitmap wording, grants between statements, the race-free argument, why an epoch and not save_gen).
<!-- SECTION:NOTES:END -->
