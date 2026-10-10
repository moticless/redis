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
#include "fwtree.h"

struct _kvstore {
    int flags;
    kvstoreType *type;
    dictType dtype;
    dict **dicts;
    long long num_dicts;
    long long num_dicts_bits;
    list *rehashing;                       /* List of dictionaries in this kvstore that are currently rehashing. */
    int resize_cursor;                     /* Cron job uses this cursor to gradually resize dictionaries (only used if num_dicts > 1). */
    int allocated_dicts;                   /* The number of allocated dicts. */
    int non_empty_dicts;                   /* The number of non-empty dicts. */
    unsigned long long key_count;          /* Total number of keys in this kvstore. */
    unsigned long long bucket_count;       /* Total number of buckets in this kvstore across dictionaries. */
    fenwickTree *dict_sizes;               /* Binary indexed tree (BIT) that describes cumulative key frequencies up until given dict-index. */
    size_t overhead_hashtable_rehashing;   /* The overhead of dictionaries rehashing. */
    int refcount;                          /* the owner plus one per allocated dict (atomic): the struct outlives
                                              its detached dicts, whose dictType it holds */
    list *ov_dirty;                        /* Frozen dicts whose side table is (or was) non-empty (kvstoreDictOverlay *). */
    const kvstoreSnapHooks *snap_hooks;    /* Non-NULL while a snapshot scan runs over this store. */
    void *snap_priv;
    void *metadata[];                      /* conditionally allocated based on "flags" */
};

/* ==========================================================================
 * Snapshot overlay
 * ========================================================================== */

/* The side table of a frozen dict (the overlay API above): OV_SLOTS chains of detached
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
    dict *d;                    /* the frozen dict itself (kvs->dicts[didx] while owned) */
    ovSlot *slots;              /* OV_SLOTS, NULL until the first side entry */
    long entries;               /* LIVE + SHARED entries (clones and adds) */
    long adds;                  /* LIVE entries whose key is not in the frozen dict */
    long tombs;                 /* TOMB + TOMB_SHARED entries */
    const kvstoreSnapHooks *h;  /* the store's hooks at attach time (kvstore.h kvstoreSnapHooks) */
    int unlink_frozen;          /* the pending two-phase unlink targets the frozen dict */
    listNode *dirty_node;       /* node in kvs->ov_dirty while the side table may be non-empty */
    void *privdata;
} kvstoreDictOverlay;

/* Side-table cursor: every LIVE and SHARED entry once, slot by slot. The next
 * entry is fetched before the current one is handed out, so a caller may
 * delete the entry it got (safe-iterator contract). */
typedef kvstoreOvCursor ovCursor;

/* Save state exists on a dict only while its store is under a save (the save
 * releases every state it attached before the hooks come off): no save, one
 * load. ovPeek: the attached overlay or NULL, no side effects. ovGet: what a
 * dict access must go through: the attached overlay, or whatever the guard
 * decides (it may attach one now). */
static inline kvstoreDictOverlay *ovPeek(kvstore *kvs, int didx) {
    if (__builtin_expect(kvs->snap_hooks == NULL, 1)) return NULL;
    dict *d = kvs->dicts[didx];
    return d ? ((kvstoreDictMetaBase *)dictMetadata(d))->ov : NULL;
}

static inline kvstoreDictOverlay *ovGet(kvstore *kvs, int didx, int write) {
    if (__builtin_expect(kvs->snap_hooks == NULL, 1)) return NULL;
    dict *d = kvs->dicts[didx];
    if (!d) return NULL;
    kvstoreDictOverlay *ov = ((kvstoreDictMetaBase *)dictMetadata(d))->ov;
    if (ov) return ov;
    return kvs->snap_hooks->guard(kvs->snap_priv, kvs, didx, write);
}

/* A non-empty side table: reads must consult it. Writes always go through
 * the overlay of a frozen dict (ovGet), whatever the side table holds. */
static inline int ovActive(kvstoreDictOverlay *ov) {
    return ov && (ov->entries || ov->tombs);
}

/* The interception points of kvstore.c. Every public entry point that
 * touches a dict's content starts with exactly one of these three (grep
 * them to list every place the snapshot may step in):
 *   ovRead(kvs, didx)    reads and enumeration: the overlay only while its
 *                        side table is non-empty, a plain dict read otherwise;
 *   ovWrite(kvs, didx)   structural writes: the overlay of a frozen dict,
 *                        attached through the guard on first touch;
 *   ovFrozen(kvs, didx)  layout decisions (expand): is the dict frozen. */
static inline kvstoreDictOverlay *ovRead(kvstore *kvs, int didx) {
    kvstoreDictOverlay *ov = ovGet(kvs, didx, 0);
    return ovActive(ov) ? ov : NULL;
}
static inline kvstoreDictOverlay *ovWrite(kvstore *kvs, int didx) { return ovGet(kvs, didx, 1); }
static inline int ovFrozen(kvstore *kvs, int didx) { return ovGet(kvs, didx, 0) != NULL; }

/* kvstore.c: the one place the store's key count, non-empty count and
 * cumulative-size tree change. 'after' is the dict's logical size after the
 * change (the real size, or the overlay's logical size for a frozen dict). */
void kvstoreKeyCountAdd(kvstore *kvs, int didx, long delta, unsigned long after);

/* kvsoverlay.c, for the enumeration and the public wrappers in kvstore.c. */
unsigned long ovDictSize(kvstore *kvs, int didx);
dictEntry *ovMapFrozenEntry(kvstore *kvs, kvstoreDictOverlay *ov, dictEntry *de);
int ovSideEntryIsAdd(kvstore *kvs, kvstoreDictOverlay *ov, dictEntry *te);
void ovDetachFromStore(dict *d);   /* the dict leaves its store: its side table is cut loose (kvstore.c detachDict) */
void ovCursorInit(kvstoreDictOverlay *ov, ovCursor *c);
dictEntry *ovCursorNext(kvstoreDictOverlay *ov, ovCursor *c);
unsigned long long ovScan(kvstore *kvs, kvstoreDictOverlay *ov, dict *d, unsigned long long cursor,
                          dictScanFunction *scan_cb, void *privdata);
dictEntry *ovIteratorNext(kvstore *kvs, kvstoreDictOverlay *ov, dictIterator *di, int *phase, ovCursor *cur);
dictEntry *ovRandomEntry(kvstore *kvs, int didx, kvstoreDictOverlay *ov, int fair);
unsigned int ovGetSomeKeys(kvstore *kvs, kvstoreDictOverlay *ov, dictEntry **des, unsigned int count);
/* The overlay halves of the hot kvstore entry points: out of line so the
 * common path stays as small as it was and keeps being inlined. */
dictEntry *ovDictFind(kvstore *kvs, kvstoreDictOverlay *ov, void *key);
dictEntryLink ovDictFindLink(kvstore *kvs, int didx, kvstoreDictOverlay *ov, void *key, dictEntryLink *bucket);
void ovDictSetAtLink(kvstore *kvs, int didx, kvstoreDictOverlay *ov, void *kv, dictEntryLink *link, int newItem);
dictEntry *ovDictAddRaw(kvstore *kvs, int didx, kvstoreDictOverlay *ov, void *key, dictEntry **existing);
dictEntryLink ovDictTwoPhaseUnlinkFind(kvstore *kvs, int didx, kvstoreDictOverlay *ov, const void *key, int *table_index);
void ovDictTwoPhaseUnlinkFreeKey(kvstore *kvs, int didx, kvstoreDictOverlay *ov, dictEntryLink link, int table_index, const void *key);
int ovDictDelete(kvstore *kvs, int didx, kvstoreDictOverlay *ov, const void *key);

#endif /* KVSOVERLAY_H */
