/*
 * kvsoverlay.h - internal to kvstore.c and kvsoverlay.c, not an API: the
 * snapshot overlay routines (hybrid BGSAVE) the two files exchange and the
 * few store accessors the overlay uses. The store struct is private to
 * kvstore.c, which also holds the inline interception helpers
 * (ovRead/ovWrite/ovFrozen) of the hot paths; the overlay struct is private
 * to kvsoverlay.c. Nothing else includes this header; the API is kvstore.h.
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

/* The side table of a frozen dict: its layout is kvsoverlay.c's (kvstore.h
 * carries the forward declaration for the per-dict metadata pointer). */
typedef struct kvstoreDictOverlay kvstoreDictOverlay;

/* A non-empty side table: reads must consult it. Writes always go through
 * the overlay of a frozen dict, whatever the side table holds. Out of line:
 * reached only for a frozen dict during a save. */
int ovActive(kvstoreDictOverlay *ov);

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
