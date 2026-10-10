/*
 * kvsoverlay.h - internal to kvstore.c and kvsoverlay.c, not an API: the
 * store struct itself, the snapshot overlay types (hybrid BGSAVE), the inline
 * fast paths every dict access takes and the overlay routines the two files
 * exchange. Nothing else includes this header; the API is kvstore.h.
 *
 * Copyright (c) 2026-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of (a) the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#ifndef KVSOVERLAY_H
#define KVSOVERLAY_H

#include "kvstore.h"

/* Internal to kvstore.c and kvsoverlay.c: the overlay types, the overlay
 * halves of the kvstore entry points, and the few store accessors the overlay
 * uses. The store struct itself is private to kvstore.c, which also holds
 * the inline interception helpers (ovRead/ovWrite/ovFrozen) of the hot paths. */

/* ==========================================================================
 * Snapshot overlay
 * ========================================================================== */

/* The side table of a frozen dict (public API: kvstoreOverlay* in kvstore.h): OV_SLOTS chains of detached
 * dict entries, a key's slot being its hash modulo OV_SLOTS (the low bits of
 * its bucket index in either table). Four chains per slot:
 *   LIVE         deep clones and adds: at drain they replace their original
 *                (freed in full) or are inserted.
 *   SHARED       clones whose value allocation is still the original's: at
 *                drain they replace the original, which is freed header-only.
 *   TOMB         tombstones: the key is the frozen original itself (borrowed,
 *                never destructed here); at drain the original leaves the
 *                dict and is freed in full.
 *   TOMB_SHARED  tombstones of keys whose clone shared the original's value
 *                (the clone's release took the value with it): header-only.
 * A key is in at most one chain. Entries are allocated with the dict's own
 * entry kind, so a position in a chain is an ordinary dictEntryLink for the
 * callers. The array exists from the first deferred write to the release. */
#define OV_SLOTS 1024
#define OV_SLOT_MASK (OV_SLOTS - 1)
enum { OV_LIVE = 0, OV_SHARED = 1, OV_TOMB = 2, OV_TOMB_SHARED = 3, OV_CHAINS = 4 };
#define OV_IS_TOMB(chain) ((chain) >= OV_TOMB)
typedef struct ovSlot { dictEntry *heads[OV_CHAINS]; } ovSlot;

typedef struct kvstoreDictOverlay {
    kvstore *kvs;               /* the store the dict is in; NULL once the dict left it (detached: no more
                                   accesses through the store, the side table only waits for the release) */
    int didx;                   /* the dict's slot */
    dict *d;                    /* the frozen dict itself (the store's dict of slot didx while owned) */
    ovSlot *slots;              /* OV_SLOTS, NULL until the first side entry */
    long entries;               /* LIVE + SHARED entries (clones and adds) */
    long adds;                  /* LIVE entries whose key is not in the frozen dict */
    long tombs;                 /* TOMB + TOMB_SHARED entries */
    const kvstoreSnapHooks *h;  /* the store's hooks at attach time (kvstore.h kvstoreSnapHooks) */
    int unlink_frozen;          /* the pending two-phase unlink targets the frozen dict */
    listNode *dirty_node;       /* node in the store's drain list while the side table may be non-empty */
    void *privdata;
} kvstoreDictOverlay;

/* Side-table cursor: every LIVE and SHARED entry once, slot by slot. The next
 * entry is fetched before the current one is handed out, so a caller may
 * delete the entry it got (safe-iterator contract). */
typedef kvstoreOvCursor ovCursor;

/* What the overlay may reach of its store (kvstore.c; the store struct is
 * private to kvstore.c). The dict type is the frozen dict's own (ov->d->type,
 * the store's), the dict of a slot is the public kvstoreGetDict(). */
const kvstoreSnapHooks *kvstoreSnapHooksGet(kvstore *kvs, void **privdata);   /* NULL outside a save */
void **kvstoreOverlaySlot(kvstore *kvs);   /* one opaque per-store slot, the overlay's (its drain list); ovStoreRelease() empties it */
void ovStoreRelease(kvstore *kvs);         /* kvsoverlay.c, from kvstoreRelease */

/* kvstore.c: the one place the store's key count, non-empty count and
 * cumulative-size tree change. 'after' is the dict's logical size after the
 * change (the real size, or the overlay's logical size for a frozen dict). */
void kvstoreKeyCountAdd(kvstore *kvs, int didx, long delta, unsigned long after);

/* kvsoverlay.c, for the enumeration and the public wrappers in kvstore.c. */
unsigned long ovDictSize(kvstore *kvs, int didx);
dictEntry *ovMapFrozenEntry(kvstoreDictOverlay *ov, dictEntry *de);
int ovSideEntryIsAdd(kvstoreDictOverlay *ov, dictEntry *te);
void ovDetachFromStore(dict *d);   /* the dict leaves its store: its side table is cut loose (kvstore.c detachDict) */
void ovCursorInit(kvstoreDictOverlay *ov, ovCursor *c);
dictEntry *ovCursorNext(kvstoreDictOverlay *ov, ovCursor *c);
unsigned long long ovScan(kvstore *kvs, kvstoreDictOverlay *ov, dict *d, unsigned long long cursor,
                          dictScanFunction *scan_cb, void *privdata);
dictEntry *ovIteratorNext(kvstoreDictOverlay *ov, dictIterator *di, int *phase, ovCursor *cur);
dictEntry *ovRandomEntry(kvstore *kvs, int didx, kvstoreDictOverlay *ov, int fair);
unsigned int ovGetSomeKeys(kvstoreDictOverlay *ov, dictEntry **des, unsigned int count);
/* The overlay halves of the hot kvstore entry points: out of line so the
 * common path stays as small as it was and keeps being inlined. */
dictEntry *ovDictFind(kvstoreDictOverlay *ov, void *key);
dictEntryLink ovDictFindLink(kvstore *kvs, int didx, kvstoreDictOverlay *ov, void *key, dictEntryLink *bucket);
void ovDictSetAtLink(kvstore *kvs, int didx, kvstoreDictOverlay *ov, void *kv, dictEntryLink *link, int newItem);
dictEntry *ovDictAddRaw(kvstore *kvs, int didx, kvstoreDictOverlay *ov, void *key, dictEntry **existing);
dictEntryLink ovDictTwoPhaseUnlinkFind(kvstore *kvs, int didx, kvstoreDictOverlay *ov, const void *key, int *table_index);
void ovDictTwoPhaseUnlinkFreeKey(kvstore *kvs, int didx, kvstoreDictOverlay *ov, dictEntryLink link, int table_index, const void *key);
int ovDictDelete(kvstore *kvs, int didx, kvstoreDictOverlay *ov, const void *key);

#endif /* KVSOVERLAY_H */
