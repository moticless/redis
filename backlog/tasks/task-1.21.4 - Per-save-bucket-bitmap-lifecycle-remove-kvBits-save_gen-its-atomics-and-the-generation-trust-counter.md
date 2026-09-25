---
id: TASK-1.21.4
title: >-
  Per-save bucket bitmap lifecycle; remove kvBits save_gen, its atomics and the
  generation trust counter
status: Done
assignee: []
created_date: '2026-09-21 19:57'
updated_date: '2026-09-25 14:23'
labels:
  - bgsave
  - persistence
milestone: m-3
dependencies:
  - TASK-1.21.1
documentation:
  - poc-hybrid-bgsave-spec.md
parent_task_id: TASK-1.21
priority: high
ordinal: 11940
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Spec section 11.z, 'Bitmap lifecycle'. The 'already in the snapshot' state is a per-save bitmap with one bit per bucket of every frozen table, allocated at freeze, set by the main thread (atomic byte ops) for buckets it emitted ahead of the window, read by the walker for buckets inside the window, freed when the save ends or aborts. Nothing survives a save, so there is no generation to wrap. Remove kvBits.save_gen and the byte-wide kvBits atomics (kvobjBitsGet/Set/GetAcquire/SetRelease, kvBitsWord) restoring the upstream kvBits layout, remove server.snap_gen, snap_gen_since_success, snap_save_marks_valid and the clone-everything fallback from code, INFO, redis.conf and the tcl tests. Abort of a save frees the bitmaps and the side table after the walker has stopped; FLUSHALL/FLUSHDB/SWAPDB during a save abort it as today.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 Bitmaps exist only while a save runs: INFO shows their bytes during a save and zero after; an aborted save (child killed, hard mark, abort command) leaves none behind
- [x] #2 Ten or more consecutive aborted saves followed by a successful one reload equal to the fork instant with no missing or duplicate key
- [x] #3 kvBits has the upstream layout again (2 bits sds type, no_evict, 5 unused) and no kvBits atomics remain; snap_gen, snap_gen_since_success and snap_save_marks_valid are gone from INFO and code
- [x] #4 FLUSHALL during a save aborts promptly and the next save works
<!-- AC:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
2026-09-25: started on the user's request (suspected steady-state cost of the per-key atomic). Survey: kvBits.save_gen is written with an atomic store at every kvobj creation (object.c:98, 200), read by snapKvobjScanned (no callers left after the bucket-window redesign) and written by the scan thread in snapKvobjMarkScanned; INFO snap_gen / snap_gen_since_success / snap_save_marks_valid; tests use snap_gen as a 'hybrid save happened' indicator.

2026-09-25 done: kvBits back to upstream (2/1/5 bits, plain field access, no atomics), save_gen/snap_gen/snap_gen_since_success/snap_save_marks_valid/snapKvobjScanned/MarkScanned removed; INFO snap_saves and snap_save_bitmap_bytes added; tests: bitmap bytes >0 during a save and 0 after, ten saves aborted by a killed child then a completing one reloads equal (AC1, AC2); FLUSHALL-during-save tests unchanged (AC4). Unit 164/164, unit/snapshot 43/43. Callgrind before/after on the plain build in RESULTS.md.
<!-- SECTION:NOTES:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
Per-key save generation removed: kvBits is the upstream struct with plain field access, no byte-wide atomics remain on the kvobj creation path or the scan thread. The per-save bucket bitmap is the only snapshot state (INFO snap_save_bitmap_bytes), snap_saves counts hybrid saves. New tests cover bitmap lifetime and ten aborted saves followed by a successful reload.
<!-- SECTION:FINAL_SUMMARY:END -->
