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
 * The implementation side of the kvstore snapshot overlay (hybrid snapshot, see
 * the KVSTORE OVERLAY section of kvstore.h). While a background scan thread
 * reads a dict of the store, the dict is "frozen": its bucket layout may not
 * change and the chains the thread is reading may not be written. Writes to
 * such a dict go to a per-dict side table instead, and every read, size and
 * enumeration of the dict describes the logical keyspace: the frozen dict
 * minus its tombstones, with clones substituted and adds appended.
 *
 * WHO INCLUDES THIS
 * -----------------
 * kvstore.c and kvsoverlay.c only; it is not an API. kvstore.c keeps the
 * public entry points and decides, per access, whether the overlay steps in
 * (its inline ovRead/ovWrite/ovFrozen helpers, one load on the common path);
 * kvsoverlay.c holds the overlay halves of those entry points and the side
 * table itself. The store struct is private to kvstore.c and the overlay
 * struct is private to kvsoverlay.c: the two files meet only through the
 * prototypes below. The save's own per-dict state (bucket bitmaps, pause) is
 * snapshot.c's, in the dict's extension slot, and is not seen here.
 *
 * THE SIDE TABLE
 * --------------
 * A fixed array of slots, a key's slot being its hash modulo the array size,
 * with four chains of ordinary dict entries per slot:
 *
 *   LIVE         deep clones and adds: at drain they replace their original
 *                (freed in full) or are inserted.
 *   SHARED       clones whose value allocation is still the original's: at
 *                drain they replace the original, which is freed header-only.
 *   TOMB         tombstones: the key is the frozen original itself (borrowed,
 *                never destructed here); at drain the original leaves the
 *                dict and is freed in full.
 *   TOMB_SHARED  tombstones of keys whose clone shared the original's value
 *                (the clone's release took the value with it): header-only.
 *
 * A key is in at most one chain. Entries are allocated with the dict's own
 * entry kind, so a position in a chain is an ordinary dictEntryLink for the
 * callers. The array exists from the first deferred write to the release.
 *
 * LIFECYCLE
 * ---------
 * attach   the store's snapshot guard decides a dict needs an overlay (first
 *          structural write to it while owed, or the window is on it):
 *          kvstoreOverlayAttach, the overlay copies the store's hooks.
 * write    'prepare' asks the snapshot whether the chain may be written in
 *          place (the window has passed, or the bucket was just emitted); if
 *          not, the write lands in the side table: the first write-lookup of a
 *          key calls 'clone' for a private copy, deletes leave a tombstone,
 *          adds go to the LIVE chain. Logical key counts are kept as it goes.
 * drain    on the owner thread, kvstoreOverlayDrain applies every entry whose
 *          bucket the thread will never read again ('drainable'); originals
 *          are released through 'free_orig'.
 * release  when the dict's scan is complete (or the save aborted),
 *          kvstoreOverlayRelease applies what is left and the overlay goes.
 * detach   a dict that leaves its store while frozen (freed, emptied, moved)
 *          takes its overlay along, off the store's drain list; the save
 *          releases it later wherever the dict is.
 */

#ifndef KVSOVERLAY_H
#define KVSOVERLAY_H

#include "kvstore.h"

/* The side table of a frozen dict (layout in kvsoverlay.c) and a position in it (every LIVE and SHARED entry once). */
typedef struct kvstoreDictOverlay kvstoreDictOverlay;
typedef kvstoreOvCursor ovCursor;

/* ---- What the overlay may reach of its store (kvstore.c) ------------------ */

/* The store's snapshot hooks and privdata; NULL outside a save. */
const kvstoreSnapHooks *kvstoreSnapHooksGet(kvstore *kvs, void **privdata);
/* One opaque per-store slot, the overlay's (its drain list); ovStoreRelease empties it from kvstoreRelease. */
void **kvstoreOverlaySlot(kvstore *kvs);
void ovStoreRelease(kvstore *kvs);
/* The one key-count update of the store; 'after' is the dict's logical size after the change. */
void kvstoreKeyCountAdd(kvstore *kvs, int didx, long delta, unsigned long after);

/* ---- The overlay for kvstore.c (kvsoverlay.c) ------------------------------ */

/* Non-empty side table: reads must consult it (out of line; reached only for frozen dicts). */
int ovActive(kvstoreDictOverlay *ov);
/* Logical size of dict didx, frozen or not. */
unsigned long ovDictSize(kvstore *kvs, int didx);
/* A frozen entry's logical counterpart: its clone, NULL (tombstoned) or itself; a side entry whose key the frozen dict lacks. */
dictEntry *ovMapFrozenEntry(kvstoreDictOverlay *ov, dictEntry *de);
int ovSideEntryIsAdd(kvstoreDictOverlay *ov, dictEntry *te);
/* The dict leaves its store (detachDict): off the drain list, the overlay follows the dict. */
void ovDetachFromStore(dict *d);

/* Enumeration: the side-table cursor (next is fetched first, the caller may delete what it got), scan, iterator (phase 0 frozen dict, 1 side table), random keys. */
void ovCursorInit(kvstoreDictOverlay *ov, ovCursor *c);
dictEntry *ovCursorNext(kvstoreDictOverlay *ov, ovCursor *c);
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
