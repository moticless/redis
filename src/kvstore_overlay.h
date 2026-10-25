/*
 * Copyright (c) 2026-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of (a) the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 *
 *
 * WHAT IS THE KVSTORE OVERLAY?
 * ----------------------------
 * The contract between a store and the hybrid snapshot (hybrid_snapshot.h),
 * which reads the store's dicts on a background thread while the main thread
 * keeps writing, and its implementation (kvstore_overlay.c). The first part of
 * this header is the contract; the second, INTERNALS, is how it is built.
 *
 * While a scan runs over a store, the snapshot's hooks are installed on it
 * (kvstoreSnapGuardSet): every access to a dict without an overlay asks the
 * guard first, which may attach one. A dict with an overlay is "frozen": its
 * bucket layout is fixed (no resize; rehash steps are reported to the
 * snapshot), and before every structural write the snapshot's 'prepare' hook
 * says whether the bucket's chain may be written in place. If not (DEFER:
 * the scan thread may be reading the bucket), the bucket is "overlayed": its
 * chain is duplicated, every key through 'clone' so in-place mutation by the
 * caller lands on the clone, and the write goes to the duplicate; the dict's
 * own chain stays exactly as the thread reads it. Reads, sizes and
 * enumeration describe the logical keyspace (each overlayed bucket's chain in
 * place of the dict's); reads never overlay anything. A store without hooks
 * pays one load per access.
 *
 * DEFER also reports the window the bucket is in (kvstoreOvWindow: a run of
 * buckets of one table and the scan position of its last bucket; positions
 * only grow along the scan). On the owner thread kvstoreOverlayDrain(below)
 * writes back every window whose last position is below 'below' (the
 * snapshot no longer reads them), and kvstoreOverlayRelease() writes back
 * what is left of a dict and unfreezes it once its scan is complete.
 *
 * The overlay hangs off the dict's metadata (kvstoreDictMetaBase.ov). Dict
 * references (kvstore.h, kvstoreDictRetain) let a save keep reading a dict its
 * store let go of.
 *
 * WHO INCLUDES THIS
 * -----------------
 * The contract: the hybrid snapshot (hybrid_snapshot.c). The INTERNALS:
 * kvstore.c and kvstore_overlay.c only, each after #define
 * INCLUDE_KVSTORE_OVERLAY_H (any other includer does not see them); they are
 * not an API. kvstore.c keeps the public entry points and decides, per access,
 * whether the overlay steps in (its inline kvsoRead/kvsoWrite/kvsoFrozen
 * helpers, one load on the common path); kvstore_overlay.c holds the overlay
 * halves of those entry points and the overlayed buckets themselves. The store
 * struct is private to kvstore.c and the overlay struct is private to
 * kvstore_overlay.c: the two files meet only through the INTERNALS prototypes.
 * The save's own per-dict state (bucket bitmaps, pause) is hybrid_snapshot.c's,
 * in the dict's extension slot next to the overlay, and is not seen here.
 *
 * THE OVERLAYED BUCKETS
 * ----------------------
 * An overlayed bucket is named by (table, index) and holds:
 *
 *   chain   the bucket's logical chain: detached entries (ordinary dict entry
 *           nodes, so a position in it is a dictEntryLink for the callers)
 *           holding the clones of the original keys, in order, and the adds.
 *   origs   the dict's own chain at that slot, untouched: the originals.
 *
 * The dict's slot and chain are not written while the bucket is overlayed:
 * the scan thread reads them exactly as at the freeze. Only the main thread
 * touches the overlay.
 *
 * The buckets are indexed by the window 'prepare' reported for them (the
 * kvstoreOvWindow a DEFER carries). One index serves every store, there being
 * one scan at a time: slots, each the buckets of one window of one dict, a
 * bucket at offset index - lo, with a bitmap of the offsets in use. A slot is
 * written back whole once the scan has passed its last bucket. The index
 * grows on demand; the snapshot keeps it small (it drains at the end of every
 * write section and stops granting the scan more once many slots are in use,
 * kvstoreOverlaySlotsInUse). Lookup is a check of the slots and an array
 * load; a drain visits only the buckets there are.
 *
 * LIFECYCLE
 * ---------
 * attach   kvstoreOverlayAttach, when the guard decides (above): the
 *          overlay copies the store's hooks. An overlay without overlayed
 *          buckets is only the carrier of the hooks.
 * write    on DEFER the bucket is overlayed on its first write (every key of
 *          the chain through 'clone') and the write lands in the duplicate;
 *          later writes to it go there directly. Rehash and auto-resize are
 *          held while any bucket of the dict is overlayed. Logical key counts
 *          are kept as it goes.
 * drain    the write-back of a bucket: the duplicate becomes the slot's
 *          chain, the originals are released through 'free_orig' and the
 *          original chain's nodes are freed. Rehash and auto-resize resume
 *          with a dict's last overlayed bucket.
 * release  kvstoreOverlayRelease drains what is left and the overlay goes.
 * detach   a dict that leaves its store while frozen (freed, emptied, moved)
 *          takes its overlay along; its buckets drain like any others and the
 *          save releases the overlay later wherever the dict is.
 */

#ifndef KVSTORE_OVERLAY_H
#define KVSTORE_OVERLAY_H

#include "kvstore.h"

/* ---- The contract (the hybrid snapshot) -------------------------------------- */

/* Guard: a dict with no overlay is accessed (write = structural): attach one and return it, or NULL. */
typedef struct kvstoreDictOverlay *(kvstoreSnapGuardFn)(void *privdata, kvstore *kvs, int didx, int write);
/* Layout hooks: a dict started / finished an incremental rehash; may the dict shrink right now. */
typedef void (kvstoreSnapRehashFn)(void *privdata, int didx, dict *d);
typedef int (kvstoreSnapShrinkAllowedFn)(void *privdata, int didx);
/* Overlay callbacks: a private copy of a frozen key for the bucket's duplicate, and the release of a frozen original after the write-back. */
typedef void *(kvstoreOverlayCloneFn)(void *privdata, int didx, void *orig);
typedef void (kvstoreOverlayFreeOrigFn)(void *privdata, int didx, void *orig);
/* Start of every main-thread write on a frozen dict, and of every access through an attached overlay (optional). */
typedef void (kvstoreOverlayEnterFn)(void *privdata);
/* The window a deferred bucket belongs to: buckets [lo, lo + n) of one table, 'last' the scan position of its last bucket. */
typedef struct kvstoreOvWindow { uint64_t last; int table; unsigned long lo, n; } kvstoreOvWindow;
/* Before a structural write of 'key': may the frozen chain be written in place; on DEFER, the window the bucket is in
 * (optional: absent = DEFER, the window the whole table with last 0). */
typedef enum kvstoreOverlayPrepareResult {
    KVSTORE_OV_DEFER   = 0,   /* the scan thread is reading the bucket: the write goes to the bucket's duplicate */
    KVSTORE_OV_INPLACE = 1,   /* the snapshot holds the bucket (or just emitted it): write in place */
} kvstoreOverlayPrepareResult;
typedef kvstoreOverlayPrepareResult (kvstoreOverlayPrepareFn)(void *privdata, int didx, dict *d, const void *key, kvstoreOvWindow *win);
/* The snapshot's hooks on a store, one struct and one privdata; it must outlive every overlay attached while installed. */
typedef struct kvstoreSnapHooks {
    kvstoreSnapGuardFn *guard;
    kvstoreSnapRehashFn *rehash_started;
    kvstoreSnapRehashFn *rehash_completed;
    kvstoreSnapShrinkAllowedFn *shrink_allowed;
    kvstoreOverlayCloneFn *clone;
    kvstoreOverlayFreeOrigFn *free_orig;
    kvstoreOverlayEnterFn *enter;
    kvstoreOverlayPrepareFn *prepare;
} kvstoreSnapHooks;
/* Install the hooks on a store; NULL turns the snapshot off for it. */
void kvstoreSnapGuardSet(kvstore *kvs, const kvstoreSnapHooks *hooks, void *privdata);

/* The overlay of a dict: attach (the store's hooks serve it), is it attached, release (scan complete or save aborted: buckets written back). */
void kvstoreOverlayAttach(kvstore *kvs, int didx);
int kvstoreOverlayIsFrozen(kvstore *kvs, int didx);
void kvstoreOverlayRelease(dict *d);
/* Across every store: write back the buckets of the windows whose last position is below 'below' (returns what is left);
 * count the overlayed buckets; count the index slots in use (windows holding buckets). */
long kvstoreOverlayDrain(uint64_t below);
long kvstoreOverlayPending(void);
int kvstoreOverlaySlotsInUse(void);

/* Counters of the overlay's work since the last reset (main thread; a save resets them at its start). */
typedef struct kvstoreOverlayStats {
    long long buckets;      /* buckets overlayed: writes that collided with the window */
    long long clones;       /* keys cloned by them */
    long peak;              /* most overlayed buckets at once, over every dict */
    long long paused_us;    /* dict-time rehash and auto-resize were held by overlayed buckets (a pause still open counts up to now) */
} kvstoreOverlayStats;
void kvstoreOverlayStatsReset(void);
kvstoreOverlayStats kvstoreOverlayStatsGet(void);

#ifdef INCLUDE_KVSTORE_OVERLAY_H
/* ==========================================================================
 * INTERNALS: kvstore.c and kvstore_overlay.c only
 * ========================================================================== */

/* The overlay of a frozen dict (layout in kvstore_overlay.c) and a position in its overlayed buckets' chains. */
typedef struct kvstoreDictOverlay kvstoreDictOverlay;
typedef kvstoreOvCursor kvsoCursor;

/* ---- What the overlay may reach of its store (kvstore.c) ------------------ */

/* The store's snapshot hooks and privdata; NULL outside a save. */
const kvstoreSnapHooks *kvstoreSnapHooksGet(kvstore *kvs, void **privdata);
/* The one key-count update of the store; 'after' is the dict's logical size after the change. */
void kvstoreKeyCountAdd(kvstore *kvs, int didx, long delta, unsigned long after);

/* ---- The overlay for kvstore.c (kvstore_overlay.c) ------------------------- */

/* Some bucket is overlayed: reads must consult the overlay (out of line; reached only for frozen dicts). */
int kvsoActive(kvstoreDictOverlay *ov);
/* Logical size of dict didx, frozen or not. */
unsigned long kvsoDictSize(kvstore *kvs, int didx);
/* The dict leaves its store (detachDict): the overlay follows the dict. */
void kvsoDetachFromStore(dict *d);

/* Enumeration: scan, iterator (phase 0 the dict, 1 a replaced bucket's chain, 2 buckets the dict has no entry for; next is fetched first, the caller may delete what it got), random keys. */
unsigned long long kvsoScan(kvstore *kvs, kvstoreDictOverlay *ov, dict *d, unsigned long long cursor,
                          dictScanFunction *scan_cb, void *privdata);
dictEntry *kvsoIteratorNext(kvstoreDictOverlay *ov, dictIterator *di, int *phase, kvsoCursor *cur);
dictEntry *kvsoRandomEntry(kvstore *kvs, int didx, kvstoreDictOverlay *ov, int fair);
unsigned int kvsoGetSomeKeys(kvstoreDictOverlay *ov, dictEntry **des, unsigned int count);

/* The overlay halves of the hot kvstore entry points (out of line, so the common path stays inlined). */
dictEntry *kvsoDictFind(kvstoreDictOverlay *ov, void *key);
dictEntryLink kvsoDictFindLink(kvstore *kvs, int didx, kvstoreDictOverlay *ov, void *key, dictEntryLink *bucket);
void kvsoDictSetAtLink(kvstore *kvs, int didx, kvstoreDictOverlay *ov, void *kv, dictEntryLink *link, int newItem);
dictEntry *kvsoDictAddRaw(kvstore *kvs, int didx, kvstoreDictOverlay *ov, void *key, dictEntry **existing);
dictEntryLink kvsoDictTwoPhaseUnlinkFind(kvstore *kvs, int didx, kvstoreDictOverlay *ov, const void *key, int *table_index);
void kvsoDictTwoPhaseUnlinkFreeKey(kvstore *kvs, int didx, kvstoreDictOverlay *ov, dictEntryLink link, int table_index, const void *key);
int kvsoDictDelete(kvstore *kvs, int didx, kvstoreDictOverlay *ov, const void *key);

#endif /* INCLUDE_KVSTORE_OVERLAY_H */

#endif /* KVSTORE_OVERLAY_H */
