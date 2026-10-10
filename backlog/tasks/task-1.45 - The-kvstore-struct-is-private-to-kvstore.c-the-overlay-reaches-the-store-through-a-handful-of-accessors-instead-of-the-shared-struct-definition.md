---
id: TASK-1.45
title: >-
  The kvstore struct is private to kvstore.c; the overlay reaches the store
  through a handful of accessors instead of the shared struct definition
status: Done
assignee:
  - '@Claude'
created_date: '2026-10-10 19:05'
updated_date: '2026-10-10 19:25'
labels:
  - hybrid-bgsave
  - refactor
milestone: m-1
dependencies:
  - TASK-1.41
references:
  - >-
    src/kvsoverlay.h (struct _kvstore,
    ovPeek/ovGet/ovActive/ovRead/ovWrite/ovFrozen)
  - >-
    src/kvsoverlay.c: kvs->dicts (3), kvs->dtype (9), kvs->snap_hooks (2),
    kvs->snap_priv (1), kvs->ov_dirty (9)
  - >-
    src/kvstore.c kvstoreRelease (listRelease of ov_dirty), kvstoreGetDict,
    kvstoreSnapGuardSet
modified_files:
  - src/kvstore.c
  - src/kvsoverlay.c
  - src/kvsoverlay.h
parent_task_id: TASK-1
priority: low
ordinal: 41000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Requested by the user on 2026-10-10 after the question whether to merge kvsoverlay.c/h back into kvstore.c/h (answer: keep the split, tighten the boundary instead). Today kvsoverlay.h, the internal header shared by kvstore.c and kvsoverlay.c, carries the full definition of struct _kvstore so that the overlay can reach the store's dict array, dict type, snapshot hooks and the dirty list of side tables. The overlay touches exactly five store fields (dicts, dtype, snap_hooks, snap_priv, ov_dirty) in about 25 places; kvstore.c touches all nineteen.

Outcome wanted: struct _kvstore is defined in kvstore.c only. kvsoverlay.h keeps the overlay types, the cursor and the ov* prototypes, and declares the few store accessors the overlay needs: the public kvstoreGetDict (exists), the dict's own dictType where the frozen dict is at hand, one accessor for the snapshot hooks and privdata, and one opaque per-store slot that the overlay uses for its dirty list and releases itself when the store is released (kvstore.c then knows nothing about that list). The six inline interception helpers (ovPeek, ovGet, ovActive, ovRead, ovWrite, ovFrozen) move into kvstore.c, the only hot-path user; the two cold uses in kvsoverlay.c read the dict's overlay pointer through kvstoreGetDict. No behavior change.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 struct _kvstore appears in src/kvstore.c only; kvsoverlay.h and kvsoverlay.c contain no 'kvs->' field access
- [x] #2 kvsoverlay.h declares at most four store accessors for the overlay (dict by index, snapshot hooks with privdata, the opaque overlay slot, and the key-count update that already exists); the overlay's dirty list is created and released by kvsoverlay.c
- [x] #3 The inline interception helpers live in kvstore.c; the public header kvstore.h is unchanged in API
- [x] #4 Build without warnings; redis-server test kvstore and test snapshot pass; unit/snapshot passes with bgsave-hybrid yes and no
<!-- AC:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
struct _kvstore moved from kvsoverlay.h to kvstore.c (its ov_dirty list became an opaque void *ov_state), together with the six inline interception helpers, which only kvstore.c's hot paths use. kvsoverlay.h (157 -> ~105 lines) keeps the overlay types, the cursor, the ov* prototypes and declares the accessors: kvstoreSnapHooksGet(kvs, &privdata), kvstoreOverlaySlot(kvs) (the opaque slot, used by the overlay for its drain list via a static ovDirtyList(kvs, create)), ovStoreRelease(kvs) (kvsoverlay.c, called from kvstoreRelease in place of the listRelease), plus the pre-existing kvstoreKeyCountAdd; the dict of a slot is the public kvstoreGetDict. The dict type comes from the frozen dict itself (ovType(ov) = ov->d->type; the drain's quiet type copy keeps the key functions), so ovEntryKey, ovStoredKey2Key, ovSlotOf, ovChainFind, ovChainPush, ovSideFind, ovApplySide and ovApplyTomb take the overlay instead of a store, the drain's tkvs detour is gone, and the now-unused store parameter was dropped from ovMapFrozenEntry, ovSideEntryIsAdd, ovDictFind, ovIteratorNext and ovGetSomeKeys (callers in kvstore.c updated). Two cold uses of ovPeek in kvsoverlay.c (IsFrozen, ovDictSize) read the dict's overlay pointer through kvstoreGetDict. grep 'kvs->' in kvsoverlay.c/h: 0.

One rerun was needed: the first chain's unit/snapshot (hybrid) saw the stock-mode server of the CoW comparison test fail to start twice with an empty stdout after the harness's two-minute wait, right after I had SIGKILLed the previous chain's servers to rebuild; integration/aof-multi-part had one 'aof rewrite did not scheduled' timing failure in the same chain. Both suites passed in full on the rerun with server logs kept (81 ok, 41 ok), and the stock-mode unit/snapshot passed in the first chain, so the start failure is attributed to the environment at that moment, not to the change (which touches no startup path).
<!-- SECTION:NOTES:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
The kvstore struct is now private to kvstore.c. The internal header shared with the overlay no longer exposes the store's layout; the overlay reaches the store through kvstoreGetDict, the frozen dict's own type, one accessor for the snapshot hooks and privdata, and one opaque per-store slot it uses for its drain list and releases itself. The hot-path inline interception helpers moved to kvstore.c, their only user. Side effect of taking the overlay instead of a store in the key helpers: the drain's store detour and seven unused store parameters are gone. No behavior change. Commit: see git log for 'TASK-1.45'. Verified: build without warnings, C kvstore and snapshot tests (184), unit/snapshot with the hybrid on and off (81 ok each), integration/aof-multi-part with the hybrid on (41 ok). This answers the user's question about merging kvsoverlay.c/h into kvstore.c/h: the split stays, with a stricter boundary.
<!-- SECTION:FINAL_SUMMARY:END -->
