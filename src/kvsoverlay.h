/*
 * Copyright (c) 2026-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of (a) the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 *
 *
 * WHAT IS KVSOVERLAY?
 * -------------------
 * The implementation of the kvstore snapshot overlay. What it does for its
 * user (frozen dicts, 'prepare' and DEFER, overlayed buckets, windows, drain
 * and release, dict references) is the KVSTORE OVERLAY section of kvstore.h;
 * read that first. This header covers how it is built.
 *
 * WHO INCLUDES THIS
 * -----------------
 * kvstore.c and kvsoverlay.c only, each after #define INCLUDE_KVSOVERLAY_H
 * (any other includer gets an #error); it is not an API. kvstore.c keeps the
 * public entry points and decides, per access, whether the overlay steps in
 * (its inline ovRead/ovWrite/ovFrozen helpers, one load on the common path);
 * kvsoverlay.c holds the overlay halves of those entry points and the
 * overlayed buckets themselves. The store struct is private to kvstore.c and the overlay
 * struct is private to kvsoverlay.c: the two files meet only through the
 * prototypes below. The save's own per-dict state (bucket bitmaps, pause) is
 * snapshot.c's, in the dict's extension slot, and is not seen here.
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
 * attach   kvstoreOverlayAttach, when the guard decides (kvstore.h): the
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

#ifndef INCLUDE_KVSOVERLAY_H
#error "kvsoverlay.h is private to kvstore.c and kvsoverlay.c: define INCLUDE_KVSOVERLAY_H before including it"
#endif

#ifndef KVSOVERLAY_H
#define KVSOVERLAY_H

#include "kvstore.h"

/* The overlay of a frozen dict (layout in kvsoverlay.c) and a position in its overlayed buckets' chains. */
typedef struct kvstoreDictOverlay kvstoreDictOverlay;
typedef kvstoreOvCursor ovCursor;

/* ---- What the overlay may reach of its store (kvstore.c) ------------------ */

/* The store's snapshot hooks and privdata; NULL outside a save. */
const kvstoreSnapHooks *kvstoreSnapHooksGet(kvstore *kvs, void **privdata);
/* The one key-count update of the store; 'after' is the dict's logical size after the change. */
void kvstoreKeyCountAdd(kvstore *kvs, int didx, long delta, unsigned long after);

/* ---- The overlay for kvstore.c (kvsoverlay.c) ------------------------------ */

/* Some bucket is overlayed: reads must consult the overlay (out of line; reached only for frozen dicts). */
int ovActive(kvstoreDictOverlay *ov);
/* Logical size of dict didx, frozen or not. */
unsigned long ovDictSize(kvstore *kvs, int didx);
/* The dict leaves its store (detachDict): the overlay follows the dict. */
void ovDetachFromStore(dict *d);

/* Enumeration: scan, iterator (phase 0 the dict, 1 a replaced bucket's chain, 2 buckets the dict has no entry for; next is fetched first, the caller may delete what it got), random keys. */
unsigned long long ovScan(kvstore *kvs, kvstoreDictOverlay *ov, dict *d, unsigned long long cursor,
                          dictScanFunction *scan_cb, void *privdata);
dictEntry *ovIteratorNext(kvstoreDictOverlay *ov, dictIterator *di, int *phase, ovCursor *cur);
dictEntry *ovRandomEntry(kvstore *kvs, int didx, kvstoreDictOverlay *ov, int fair);
unsigned int ovGetSomeKeys(kvstoreDictOverlay *ov, dictEntry **des, unsigned int count);

/* The overlay halves of the hot kvstore entry points (out of line, so the common path stays inlined). */
dictEntry *ovDictFind(kvstoreDictOverlay *ov, void *key);
dictEntryLink ovDictFindLink(kvstore *kvs, int didx, kvstoreDictOverlay *ov, void *key, dictEntryLink *bucket);
void ovDictSetAtLink(kvstore *kvs, int didx, kvstoreDictOverlay *ov, void *kv, dictEntryLink *link, int newItem);
dictEntry *ovDictAddRaw(kvstore *kvs, int didx, kvstoreDictOverlay *ov, void *key, dictEntry **existing);
dictEntryLink ovDictTwoPhaseUnlinkFind(kvstore *kvs, int didx, kvstoreDictOverlay *ov, const void *key, int *table_index);
void ovDictTwoPhaseUnlinkFreeKey(kvstore *kvs, int didx, kvstoreDictOverlay *ov, dictEntryLink link, int table_index, const void *key);
int ovDictDelete(kvstore *kvs, int didx, kvstoreDictOverlay *ov, const void *key);

#endif /* KVSOVERLAY_H */
