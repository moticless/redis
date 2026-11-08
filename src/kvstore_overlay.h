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
 * While a scan runs, the snapshot's hooks are installed on the store
 * (kvstoreSnapGuardSet):
 *   guard    every access to a dict without an overlay asks it first; it may
 *            attach one. A dict with an overlay is "frozen".
 *   prepare  before every structural write to a frozen dict: in place, or
 *            DEFER when the scan thread may be reading the bucket.
 *   overlay  on DEFER the bucket is "overlayed": its chain is duplicated, every
 *            key through 'clone' (so the caller's in-place mutation lands on
 *            the clone), and the write goes to the duplicate; the dict's own
 *            chain stays as the thread reads it.
 * Reads, sizes and enumeration see the logical keyspace (each overlayed
 * bucket's chain in place of the dict's) and never overlay anything. A store
 * without hooks pays one load per access.
 *
 * DEFER also reports the bucket's scan position (positions only grow along
 * the scan). On the owner thread kvstoreOverlayDrain(below) writes back every
 * bucket whose position is below 'below' (the snapshot no longer reads it),
 * and kvstoreOverlayRelease() writes back what is left of a dict and
 * unfreezes it once its scan is complete.
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
 * The save's own per-dict state (the pause) is hybrid_snapshot.c's,
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
 * The buckets are indexed by their place in the scan. One index serves the
 * save (kvstoreOverlayIndexInit): a ring of slots, a power of two, keyed by
 * the bucket's dense index: the dict's base, which the snapshot assigns when
 * it pauses the dict's layout (kvstoreOverlayIndexBase, the next run of
 * indices in scan order), plus the bucket's offset among the buckets the scan
 * reads (table 0 from rehashidx, then table 1; stable while paused). So the
 * buckets the scan reads one after the other take slots one after the other,
 * and the buckets alive at once (those the scan is on, within a couple of
 * credit windows: the snapshot writes back the passed ones before it lets the
 * scan further) fit the ring with room to spare. A slot chains the buckets
 * that share it, so a collision costs a compare, never an answer. Lookup is
 * one slot, one load, one compare. Each overlay also keeps the list of its
 * buckets, for its release and enumeration; a bitmap of the slots in use
 * keeps the drain to the buckets there are.
 *
 * LIFECYCLE
 * ---------
 * attach   kvstoreOverlayAttach, when the guard decides (above): the
 *          overlay copies the store's hooks. An overlay without overlayed
 *          buckets is only the carrier of the hooks.
 * write    on DEFER the bucket is overlayed on its first write (every key of
 *          the chain through 'clone') and the write lands in the duplicate;
 *          later writes to it go there directly. Rehash and auto-resize are
 *          held while any bucket of the dict is overlayed, except that the
 *          store's dictType may let the dict grow a new table 1
 *          (pausedExpandAllowed): table 0 keeps its buckets, a key added to
 *          one before stays there (kvstoreOverlayKeyBucket). Logical key
 *          counts are kept as it goes.
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
/* Overlay callbacks: a private copy of a frozen key for the bucket's duplicate, and the release of a frozen original after the write-back. */
typedef void *(kvstoreOverlayCloneFn)(void *privdata, int didx, void *orig);
typedef void (kvstoreOverlayFreeOrigFn)(void *privdata, int didx, void *orig);
/* Start of every main-thread write on a frozen dict, and of every access through an attached overlay (optional). */
typedef void (kvstoreOverlayEnterFn)(void *privdata);
/* Before a structural write of 'key': may the frozen chain be written in place; on DEFER, the bucket's scan position
 * (the drain writes it back once the scan is past it). Optional: absent = DEFER at position 0. */
typedef enum kvstoreOverlayPrepareResult {
    KVSTORE_OV_DEFER   = 0,   /* the scan thread is reading the bucket: the write goes to the bucket's duplicate */
    KVSTORE_OV_INPLACE = 1,   /* the snapshot holds the bucket (or just emitted it): write in place */
} kvstoreOverlayPrepareResult;
typedef kvstoreOverlayPrepareResult (kvstoreOverlayPrepareFn)(void *privdata, int didx, dict *d, const void *key, uint64_t *pos);
/* The snapshot's hooks on a store, one struct and one privdata; it must outlive every overlay attached while installed. */
typedef struct kvstoreSnapHooks {
    kvstoreSnapGuardFn *guard;
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
/* The bucket of 'key' in dict d (dictKeyBucket()) as the dict's overlay, if any, sees it. */
void kvstoreOverlayKeyBucket(dict *d, const void *key, int *table, unsigned long *idx);
/* The index of overlayed buckets, one per save: 'slots' a power of two, sized for the buckets alive at once (a slot chains
 * the buckets that share it). Free once every overlay is released. */
void kvstoreOverlayIndexInit(unsigned long slots);
void kvstoreOverlayIndexFree(void);
/* Give dict d (its layout paused) the dense indices from 'base': returns how many it takes, one per bucket the scan reads
 * (table 0 from rehashidx, then table 1), so the next dict in scan order starts at base + that. Before any DEFER on it. */
uint64_t kvstoreOverlayIndexBase(dict *d, uint64_t base);
/* Across every store: write back the buckets whose scan position is below 'below' (returns what is left);
 * count the overlayed buckets. */
long kvstoreOverlayDrain(uint64_t below);
long kvstoreOverlayPending(void);

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
dictEntry *kvsoIteratorNext(kvstoreDictOverlay *ov, dictIterator *di, kvsoCursor *cur);
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
