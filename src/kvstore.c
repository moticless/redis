/*
 * Copyright (c) 2011-Present, Redis Ltd. and contributors.
 * All rights reserved.
 *
 * Copyright (c) 2024-present, Valkey contributors.
 * All rights reserved.
 *
 * Licensed under your choice of (a) the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 *
 * Portions of this file are available under BSD3 terms; see REDISCONTRIBUTIONS for more information.
 */

#include "fmacros.h"

#include <string.h>
#include <stddef.h>

#include "zmalloc.h"
#include "kvstore.h"
#include "fwtree.h"
#include "redisassert.h"
#include "monotonic.h"

#define UNUSED(V) ((void) V)

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
    list *snap_dicts;                      /* Dicts with save state attached (dict *), see kvstoreDictSnap. */
    list *ov_dirty;                        /* Frozen dicts whose side table is (or was) non-empty (kvstoreDictOverlay *). */
    const kvstoreSnapHooks *snap_hooks;    /* Non-NULL while a snapshot scan runs over this store. */
    void *snap_priv;
    uint64_t snap_gen;                     /* generation of the running save (0: none); the dicts' stamps carry it */
    long snap_leftovers;                   /* save state of an earlier generation met and released */
    int snap_free_deferred;                /* an empty dict was kept while the hooks were set */
    size_t snap_bitmap_bytes;              /* bucket bitmaps attached to the store's dicts (kvstoreDictSnap.bytes) */
    void *metadata[];                      /* conditionally allocated based on "flags" */
};

/* ==========================================================================
 * Snapshot overlay
 * ========================================================================== */

#define OV_STATE_TOMB   1   /* key deleted: hide the frozen original */
#define OV_STATE_SHARED 2   /* the clone shares the original's value allocation */

typedef struct kvstoreDictOverlay {
    kvstore *kvs;               /* owner and slot, for the temp dict's destructor */
    int didx;
    dict *temp;                 /* clones and adds; same type as the real dict minus kvstore callbacks */
    dictType temp_type;
    dict *state;                /* frozen key (borrowed pointer) -> OV_STATE_* flags */
    dictType state_type;
    long adds;                  /* temp entries whose key is not in the frozen dict */
    long tombs;                 /* frozen keys hidden by a tombstone (and not re-added) */
    kvstoreOverlayHooks h;
    int unlink_frozen;          /* the pending two-phase unlink targets the frozen dict */
    listNode *dirty_node;       /* node in kvs->ov_dirty while the side table may be non-empty */
    void *privdata;
} kvstoreDictOverlay;

/* The save state of a dict lives in the dict's metadata (kvstoreDictSnap,
 * the overlay inside it). ovPeek: the attached overlay or NULL, no side
 * effects. ovGet: what a dict access must go through: the attached overlay,
 * or whatever the snapshot guard decides (it may attach one now). One load
 * when no snapshot runs; the dict pointer every caller loads anyway. */
static void freeDictIfNeeded(kvstore *kvs, int didx);

static inline int snapAttached(kvstore *kvs) { return kvs->snap_dicts && listLength(kvs->snap_dicts); }

static inline uint64_t snapGenOf(dict *d) {
    return __atomic_load_n(&((kvstoreDictMetaBase *)dictMetadata(d))->snap_word, __ATOMIC_RELAXED) >> 8;
}

/* The save state attached to dict didx if it belongs to the running save;
 * state under another generation is a leftover (some release path did not
 * run): apply what its side table holds to the real dict and drop it, so no
 * access ever trusts stale snapshot state. */
static kvstoreDictSnap *snapCurrent(kvstore *kvs, dict *d, int didx) {
    kvstoreDictSnap *sn = ((kvstoreDictMetaBase *)dictMetadata(d))->snap;
    if (__builtin_expect(sn == NULL || snapGenOf(d) == kvs->snap_gen, 1)) return sn;
    kvs->snap_leftovers++;
    kvstoreDictSnapRelease(kvs, didx);
    return NULL;
}

static inline kvstoreDictOverlay *ovPeek(kvstore *kvs, int didx) {
    if (__builtin_expect(!snapAttached(kvs), 1)) return NULL;
    dict *d = kvs->dicts[didx];
    kvstoreDictSnap *sn = d ? snapCurrent(kvs, d, didx) : NULL;
    return sn ? sn->ov : NULL;
}

static inline kvstoreDictOverlay *ovGet(kvstore *kvs, int didx, int write) {
    if (__builtin_expect(kvs->snap_hooks == NULL && !snapAttached(kvs), 1)) return NULL;
    dict *d = kvs->dicts[didx];
    if (!d) return NULL;
    kvstoreDictSnap *sn = snapCurrent(kvs, d, didx);
    if (sn && sn->ov) return sn->ov;
    if (kvs->snap_hooks == NULL) return NULL;
    return kvs->snap_hooks->guard(kvs->snap_priv, kvs, didx, write);
}

long kvstoreSnapLeftovers(kvstore *kvs) { return kvs->snap_leftovers; }
#define ovGetR(kvs, didx) ovGet(kvs, didx, 0)
#define ovGetW(kvs, didx) ovGet(kvs, didx, 1)

void kvstoreSnapGuardSet(kvstore *kvs, const kvstoreSnapHooks *hooks, void *privdata, uint64_t gen) {
    kvs->snap_hooks = hooks;
    kvs->snap_priv = privdata;
    kvs->snap_gen = hooks ? gen : 0;
    if (!hooks && kvs->snap_free_deferred) {
        kvs->snap_free_deferred = 0;
        for (int didx = 0; didx < kvs->num_dicts; didx++) freeDictIfNeeded(kvs, didx);
    }
}

kvstoreDictSnap *kvstoreDictSnapAttach(kvstore *kvs, int didx) {
    dict *d = kvs->dicts[didx];
    assert(d != NULL);   /* only an existing dict; it stays alive while the state is attached */
    kvstoreDictMetaBase *meta = (kvstoreDictMetaBase *)dictMetadata(d);
    assert(meta->snap == NULL);
    kvstoreDictSnap *sn = zcalloc(sizeof(*sn));
    if (!kvs->snap_dicts) kvs->snap_dicts = listCreate();
    listAddNodeTail(kvs->snap_dicts, d);
    sn->node = listLast(kvs->snap_dicts);
    __atomic_store_n(&meta->snap, sn, __ATOMIC_RELEASE);
    return sn;
}

void kvstoreSnapBitmapAccount(kvstore *kvs, long delta) { kvs->snap_bitmap_bytes += delta; }
size_t kvstoreSnapBitmapBytes(kvstore *kvs) { return kvs->snap_bitmap_bytes; }

dict *kvstoreSnapFirstDict(kvstore *kvs) {
    if (!snapAttached(kvs)) return NULL;
    return listNodeValue(listFirst(kvs->snap_dicts));
}

dict *kvstoreGetDictAcquire(kvstore *kvs, int didx) {
    return __atomic_load_n(&kvs->dicts[didx], __ATOMIC_ACQUIRE);
}

/* A non-empty side table: reads must consult it. Writes always go through
 * the overlay of a frozen dict (ovGet), whatever the side table holds. */
static inline int ovActive(kvstoreDictOverlay *ov) {
    return ov && (dictSize(ov->temp) || ov->tombs);
}

static void ovMarkDirty(kvstore *kvs, kvstoreDictOverlay *ov) {
    if (ov->dirty_node) return;
    if (!kvs->ov_dirty) kvs->ov_dirty = listCreate();
    listAddNodeTail(kvs->ov_dirty, ov);
    ov->dirty_node = listLast(kvs->ov_dirty);
}

static void ovMarkClean(kvstore *kvs, kvstoreDictOverlay *ov) {
    if (!ov->dirty_node) return;
    listDelNode(kvs->ov_dirty, ov->dirty_node);
    ov->dirty_node = NULL;
}

/* Lookup key (sds) of a stored entry of a keyspace dict. */
static inline const void *ovEntryKey(kvstore *kvs, dictEntry *de) {
    void *stored = dictGetKey(de);
    return kvs->dtype.keyFromStoredKey ? kvs->dtype.keyFromStoredKey(stored) : stored;
}

static inline uintptr_t ovStateGet(kvstoreDictOverlay *ov, const void *key) {
    dictEntry *de = dictFind(ov->state, key);
    return de ? (uintptr_t)dictGetVal(de) : 0;
}

/* State entries exist only for keys of the frozen dict and borrow the frozen
 * entry's own lookup-key pointer, which is immutable until merge. */
static void ovStateSet(kvstore *kvs, int didx, kvstoreDictOverlay *ov, const void *key, uintptr_t flags) {
    dictEntry *de = dictFind(ov->state, key);
    if (de) {
        if (flags) dictSetVal(ov->state, de, (void *)flags);
        else dictDelete(ov->state, key);
    } else if (flags) {
        dict *d = kvs->dicts[didx];
        dictEntry *fe = d ? dictFind(d, key) : NULL;
        assert(fe != NULL);
        de = dictAddRaw(ov->state, (void *)ovEntryKey(kvs, fe), NULL);
        dictSetVal(ov->state, de, (void *)flags);
    }
}

static inline int ovFrozenHas(kvstore *kvs, int didx, const void *key) {
    dict *d = kvs->dicts[didx];
    return d && dictFind(d, key) != NULL;
}

static void ovKeyCountAdd(kvstore *kvs, int didx, long delta);
static dict *createDictIfNeeded(kvstore *kvs, int didx);
static void freeDictIfNeeded(kvstore *kvs, int didx);

/* Key destructor of a temp dict: run the store's destructor against the REAL
 * dict of the slot, so per-dict accounting (kvstore metadata, e.g. slot
 * alloc sizes) lands where adds were counted. The real dict exists whenever
 * a temp entry exists (adds create it, clones come from it). */
static void ovTempKeyDestructor(dict *tempd, void *key) {
    kvstoreDictOverlay *ov = tempd->type->userdata;
    dict *real = ov->kvs->dicts[ov->didx];
    assert(real != NULL);
    ov->kvs->dtype.keyDestructor(real, key);
}

/* Start of a main-thread write on a frozen dict (see kvstoreOverlayEnterFn). */
static inline void ovEnter(kvstoreDictOverlay *ov) {
    if (ov->h.enter) ov->h.enter(ov->privdata);
}

/* May the frozen chain of 'key' be written in place? Must be called after
 * ovEnter() (the answer is only stable inside a write section). Without a
 * prepare hook the overlay is pure clone-on-write. */
static inline int ovPrepare(kvstore *kvs, int didx, kvstoreDictOverlay *ov, const void *key) {
    dict *d = kvs->dicts[didx];
    if (!d || !ov->h.prepare) return KVSTORE_OV_DEFER;
    return ov->h.prepare(ov->privdata, didx, d, key);
}

/* Is 'bucket' (a link handed out as a bucket) one of dict d's own buckets? */
static int ovBucketInDict(dict *d, dictEntryLink bucket) {
    for (int t = 0; t < 2; t++) {
        unsigned long size = DICTHT_SIZE(d->ht_size_exp[t]);
        if (size && bucket >= &d->ht_table[t][0] && bucket < &d->ht_table[t][size]) return 1;
    }
    return 0;
}

void kvstoreOverlayAttach(kvstore *kvs, int didx, const kvstoreOverlayHooks *hooks, void *privdata)
{
    dict *d = kvs->dicts[didx];
    assert(d != NULL);   /* only an existing dict can be frozen; it stays alive while frozen */
    kvstoreDictSnap *sn = ((kvstoreDictMetaBase *)dictMetadata(d))->snap;
    assert(sn != NULL && sn->ov == NULL);
    kvstoreDictOverlay *ov = zcalloc(sizeof(*ov));
    ov->kvs = kvs;
    ov->didx = didx;
    ov->temp_type = kvs->dtype;
    /* The temp dict must not touch kvstore-level accounting (rehash list,
     * bucket counts, per-dict metadata); its destructor accounts on the real dict. */
    ov->temp_type.rehashingStarted = NULL;
    ov->temp_type.rehashingCompleted = NULL;
    ov->temp_type.bucketChanged = NULL;
    ov->temp_type.dictMetadataBytes = NULL;
    ov->temp_type.userdata = ov;
    if (kvs->dtype.keyDestructor) ov->temp_type.keyDestructor = ovTempKeyDestructor;
    ov->temp = dictCreate(&ov->temp_type);
    /* The state dict is keyed by lookup keys (not stored keys) borrowed from
     * the frozen entries: same hash/compare as the store, no ownership. Lives
     * where the store's tables live (arena): it is written during a save. */
    memset(&ov->state_type, 0, sizeof(ov->state_type));
    ov->state_type.hashFunction = kvs->dtype.hashFunction;
    ov->state_type.keyCompare = kvs->dtype.keyCompare;
    ov->state_type.nofork_alloc = kvs->dtype.nofork_alloc;
    ov->state = dictCreate(&ov->state_type);
    ov->h = *hooks;
    ov->privdata = privdata;
    sn->ov = ov;
}

int kvstoreOverlayIsFrozen(kvstore *kvs, int didx) {
    return ovPeek(kvs, didx) != NULL;
}

void kvstoreOverlayStats(kvstore *kvs, int didx, long *temp_entries, long *adds, long *tombstones) {
    kvstoreDictOverlay *ov = ovPeek(kvs, didx);
    if (temp_entries) *temp_entries = ov ? (long)dictSize(ov->temp) : 0;
    if (adds) *adds = ov ? ov->adds : 0;
    if (tombstones) *tombstones = ov ? ov->tombs : 0;
}

/* Ensure 'key' (present in the frozen dict, not tombstoned, not yet in temp)
 * has a clone in the temp dict. Returns the temp entry or NULL when the key is
 * not logically present. */
static dictEntry *ovEnsureClone(kvstore *kvs, int didx, kvstoreDictOverlay *ov, const void *key) {
    dictEntry *te = dictFind(ov->temp, key);
    if (te) return te;
    uintptr_t st = ovStateGet(ov, key);
    if (st & OV_STATE_TOMB) return NULL;
    dict *d = kvs->dicts[didx];
    dictEntry *fe = d ? dictFind(d, key) : NULL;
    if (!fe) return NULL;
    int shared = 0;
    void *clone = ov->h.clone(ov->privdata, didx, dictGetKey(fe), &shared);
    assert(clone != NULL);
    te = dictAddRaw(ov->temp, clone, NULL);
    assert(te != NULL);
    if (shared) ovStateSet(kvs, didx, ov, key, st | OV_STATE_SHARED);
    ovMarkDirty(kvs, ov);
    return te;
}


/* Called after a temp entry for 'key' was inserted (new item). */
static void ovOnTempInsert(kvstore *kvs, int didx, kvstoreDictOverlay *ov, const void *key) {
    if (ovFrozenHas(kvs, didx, key)) {
        /* Re-add of a tombstoned frozen key. */
        uintptr_t st = ovStateGet(ov, key);
        if (st & OV_STATE_TOMB) {
            ovStateSet(kvs, didx, ov, key, st & ~(uintptr_t)OV_STATE_TOMB);
            ov->tombs--;
        }
    } else {
        ov->adds++;
    }
    ovKeyCountAdd(kvs, didx, 1);
    ovMarkDirty(kvs, ov);
}

/* Called before a temp entry for 'key' is removed. */
static void ovOnTempRemove(kvstore *kvs, int didx, kvstoreDictOverlay *ov, const void *key) {
    if (ovFrozenHas(kvs, didx, key)) {
        uintptr_t st = ovStateGet(ov, key);
        ovStateSet(kvs, didx, ov, key, st | OV_STATE_TOMB);
        ov->tombs++;
    } else {
        ov->adds--;
    }
    ovKeyCountAdd(kvs, didx, -1);
    ovMarkDirty(kvs, ov);
}

/* Logical size of dict didx (frozen or not). */
static unsigned long ovDictSize(kvstore *kvs, int didx) {
    dict *d = kvs->dicts[didx];
    unsigned long n = d ? dictSize(d) : 0;
    kvstoreDictOverlay *ov = ovPeek(kvs, didx);
    if (ov) n = n + ov->adds - ov->tombs;
    return n;
}

/* Map a frozen-dict entry to what the logical keyspace holds: the temp clone
 * if any, NULL if tombstoned, or the entry itself. */
static dictEntry *ovMapFrozenEntry(kvstore *kvs, kvstoreDictOverlay *ov, dictEntry *de) {
    const void *key = ovEntryKey(kvs, de);
    dictEntry *te = dictFind(ov->temp, key);
    if (te) return te;
    if (ovStateGet(ov, key) & OV_STATE_TOMB) return NULL;
    return de;
}

/* Is this temp entry an ADD (key absent from the frozen dict)? */
static int ovTempEntryIsAdd(kvstore *kvs, int didx, dictEntry *te) {
    return !ovFrozenHas(kvs, didx, ovEntryKey(kvs, te));
}

/* Owner thread: apply one side-table (temp) entry to the real dict. A clone
 * replaces its original at the link, an add is inserted. The real dict's key
 * destructor is suppressed by the caller (originals are released through
 * free_orig, header-only when the value is shared with the clone). */
static void ovApplyTemp(kvstore *kvs, int didx, kvstoreDictOverlay *ov, dict *d, dictEntry *te) {
    void *kv = dictGetKey(te);
    const void *key = ovEntryKey(kvs, te);
    dictEntryLink link = dictFindLink(d, key, NULL);
    if (link) {
        void *orig = dictGetKey(*link);
        uintptr_t st = ovStateGet(ov, key);
        /* The state dict borrows the original's key pointer: drop the entry
         * before the original is freed, or later lookups walk a chain with a
         * dangling key. */
        dictDelete(ov->state, key);
        dictSetKeyAtLink(d, kv, &link, 0);
        ov->h.free_orig(ov->privdata, didx, orig, (st & OV_STATE_SHARED) != 0);
    } else {
        dictSetKeyAtLink(d, kv, NULL, 1);   /* no expand: auto-resize is paused */
        ov->adds--;
    }
    /* Out of the side table without destroying the key: the real dict owns it now. */
    void (*saved)(dict *, void *) = ov->temp_type.keyDestructor;
    ov->temp_type.keyDestructor = NULL;
    int ret = dictDelete(ov->temp, key);
    assert(ret == DICT_OK);
    ov->temp_type.keyDestructor = saved;
}

/* Owner thread: a tombstoned original (no re-add) leaves the real dict. */
static void ovApplyTomb(int didx, kvstoreDictOverlay *ov, dict *d, const void *key, uintptr_t st) {
    dictDelete(ov->state, key);              /* borrows the original's key pointer: drop it first */
    dictEntry *fe = dictUnlink(d, key);
    assert(fe != NULL);
    void *orig = dictGetKey(fe);
    dictFreeUnlinkedEntry(d, fe);            /* destructor suppressed: frees only a chain node */
    ov->h.free_orig(ov->privdata, didx, orig, (st & OV_STATE_SHARED) != 0);
    ov->tombs--;
}

/* Owner thread: apply the side-table entries the snapshot no longer needs
 * kept out of the chain (the 'drainable' hook decides; every entry when
 * 'force'). Returns what is left. The logical counters were maintained while
 * the entries were created, so the dict is used directly here. */
static long ovDrainOne(kvstore *kvs, kvstoreDictOverlay *ov, int force) {
    int didx = ov->didx;
    dict *d = kvs->dicts[didx];
    assert(d != NULL);
    void (*saved_destructor)(dict *, void *) = kvs->dtype.keyDestructor;
    kvs->dtype.keyDestructor = NULL;
    dictIterator it;
    if (dictSize(ov->temp)) {
        dictInitSafeIterator(&it, ov->temp);
        dictEntry *te;
        while ((te = dictNext(&it)) != NULL) {   /* deleting the returned entry is safe */
            const void *key = ovEntryKey(kvs, te);
            if (!force && !(ov->h.drainable && ov->h.drainable(ov->privdata, didx, d, key))) continue;
            ovApplyTemp(kvs, didx, ov, d, te);
        }
        dictResetIterator(&it);
    }
    if (ov->tombs) {
        dictInitSafeIterator(&it, ov->state);
        dictEntry *se;
        while ((se = dictNext(&it)) != NULL) {
            uintptr_t st = (uintptr_t)dictGetVal(se);
            if (!(st & OV_STATE_TOMB)) continue;
            const void *key = dictGetKey(se);
            if (!force && !(ov->h.drainable && ov->h.drainable(ov->privdata, didx, d, key))) continue;
            ovApplyTomb(didx, ov, d, key, st);
        }
        dictResetIterator(&it);
    }
    kvs->dtype.keyDestructor = saved_destructor;
    long left = (long)dictSize(ov->temp) + ov->tombs;
    if (left == 0) ovMarkClean(kvs, ov);
    return left;
}

long kvstoreOverlayDrain(kvstore *kvs) {
    if (!kvs->ov_dirty) return 0;
    long left = 0;
    listNode *ln = listFirst(kvs->ov_dirty);
    while (ln) {
        listNode *next = listNextNode(ln);   /* ovDrainOne may unlink ln */
        left += ovDrainOne(kvs, listNodeValue(ln), 0);
        ln = next;
    }
    return left;
}

long kvstoreOverlayPending(kvstore *kvs) {
    if (!kvs->ov_dirty) return 0;
    long n = 0;
    listIter li;
    listNode *ln;
    listRewind(kvs->ov_dirty, &li);
    while ((ln = listNext(&li)) != NULL) {
        kvstoreDictOverlay *ov = listNodeValue(ln);
        n += (long)dictSize(ov->temp) + ov->tombs;
    }
    return n;
}

/* Owner thread, after the scan of this dict is complete: apply whatever is
 * left in the side table and drop it. */
static void ovRelease(kvstore *kvs, kvstoreDictOverlay *ov) {
    long left = ovDrainOne(kvs, ov, 1);
    assert(left == 0);
    ovMarkClean(kvs, ov);
    /* The side table now references nothing: release its tables without
     * running destructors. */
    ov->temp_type.keyDestructor = NULL;
    dictRelease(ov->temp);
    dictRelease(ov->state);
    zfree(ov);
}

/* Owner thread: the save is finished with dict didx (its scan completed, the
 * save was aborted, or the state is a leftover): side table applied, bitmaps
 * freed, layout unpaused, state gone. Whatever the generation. */
void kvstoreDictSnapRelease(kvstore *kvs, int didx) {
    dict *d = kvs->dicts[didx];
    kvstoreDictSnap *sn = d ? ((kvstoreDictMetaBase *)dictMetadata(d))->snap : NULL;
    if (!sn) return;
    if (sn->ov) ovRelease(kvs, sn->ov);
    for (int t = 0; t < 2; t++) {
        zfree(sn->bits[t]);
        kvs->snap_bitmap_bytes -= sn->bytes[t];
    }
    if (sn->paused) {
        dictResumeRehashing(d);
        dictResumeAutoResize(d);
    }
    listDelNode(kvs->snap_dicts, sn->node);
    ((kvstoreDictMetaBase *)dictMetadata(d))->snap = NULL;
    zfree(sn);
    /* The real dict may have become empty (all tombstones). */
    freeDictIfNeeded(kvs, didx);
}

/**********************************/
/*** Helpers **********************/
/**********************************/

/* Get the dictionary pointer based on dict-index. */
dict *kvstoreGetDict(kvstore *kvs, int didx) {
    return kvs->dicts[didx];
}

static dict **kvstoreGetDictRef(kvstore *kvs, int didx) {
    return &kvs->dicts[didx];
}

static int kvstoreDictIsRehashingPaused(kvstore *kvs, int didx)
{
    dict *d = kvstoreGetDict(kvs, didx);
    return d ? dictIsRehashingPaused(d) : 0;
}

static void addDictIndexToCursor(kvstore *kvs, int didx, unsigned long long *cursor) {
    if (kvs->num_dicts == 1)
        return;
    /* didx can be -1 when iteration is over and there are no more dicts to visit. */
    if (didx < 0)
        return;
    *cursor = (*cursor << kvs->num_dicts_bits) | didx;
}

static int getAndClearDictIndexFromCursor(kvstore *kvs, unsigned long long *cursor) {
    if (kvs->num_dicts == 1)
        return 0;
    int didx = (int) (*cursor & (kvs->num_dicts-1));
    *cursor = *cursor >> kvs->num_dicts_bits;
    return didx;
}

/* Logical key-count bookkeeping for a frozen dict: the real dict size does not
 * change, so derive the empty/non-empty transition from the logical size. */
static void ovKeyCountAdd(kvstore *kvs, int didx, long delta) {
    kvs->key_count += delta;
    unsigned long lsize = ovDictSize(kvs, didx);
    int non_empty_dicts_delta = (lsize == 1 && delta > 0) ? 1 : (lsize == 0 && delta < 0) ? -1 : 0;
    kvs->non_empty_dicts += non_empty_dicts_delta;
    if (kvs->num_dicts == 1) return;
    fwTreeUpdate(kvs->dict_sizes, didx, delta);
}

/* Updates binary index tree (Fenwick tree), updates key count for a given dict */
static void cumulativeKeyCountAdd(kvstore *kvs, int didx, long delta) {
    kvs->key_count += delta;

    dict *d = kvstoreGetDict(kvs, didx);
    size_t dsize = dictSize(d);
    /* Increment if dsize is 1 and delta is positive (first element inserted, dict becomes non-empty).
     * Decrement if dsize is 0 (dict becomes empty). */
    int non_empty_dicts_delta = (dsize == 1 && delta > 0) ? 1 : (dsize == 0) ? -1 : 0;
    kvs->non_empty_dicts += non_empty_dicts_delta;

    /* BIT does not need to be calculated when there's only one dict. */
    if (kvs->num_dicts == 1)
        return;

    /* Update the BIT */
    fwTreeUpdate(kvs->dict_sizes, didx, delta);
}

/* Create the dict if it does not exist and return it. */
static dict *createDictIfNeeded(kvstore *kvs, int didx) {
    dict *d = kvstoreGetDict(kvs, didx);
    if (d) return d;

    d = dictCreate(&kvs->dtype);
    ((kvstoreDictMetaBase *)dictMetadata(d))->didx = didx;
    /* Under a save: stamp it before the scan thread can see it (it reads the
     * pointer with acquire). */
    if (kvs->snap_hooks && kvs->snap_hooks->dict_created) kvs->snap_hooks->dict_created(kvs->snap_priv, didx, d);
    __atomic_store_n(&kvs->dicts[didx], d, __ATOMIC_RELEASE);
    kvs->allocated_dicts++;
    return d;
}

/* Called when the dict will delete entries, the function will check
 * KVSTORE_FREE_EMPTY_DICTS to determine whether the empty dict needs
 * to be freed.
 *
 * Note that for rehashing dicts, that is, in the case of safe iterators
 * and Scan, we won't delete the dict. We will check whether it needs
 * to be deleted when we're releasing the iterator. */
static void freeDictIfNeeded(kvstore *kvs, int didx) {
    if (!(kvs->flags & KVSTORE_FREE_EMPTY_DICTS) ||
        !kvstoreGetDict(kvs, didx) ||
        ovPeek(kvs, didx) ||
        kvstoreDictSize(kvs, didx) != 0 ||
        kvstoreDictIsRehashingPaused(kvs, didx))
        return;
    /* Under a save the scan thread may be reading this dict's pointer and
     * stamp: keep it; kvstoreSnapGuardSet(NULL) frees what was deferred. */
    if (kvs->snap_hooks) {
        kvs->snap_free_deferred = 1;
        return;
    }

    /* Use callback if provided to check if dict can be freed */
    if (kvs->type->canFreeDict && !kvs->type->canFreeDict(kvs, didx))
        return;

    dictRelease(kvs->dicts[didx]);
    kvs->dicts[didx] = NULL;
    kvs->allocated_dicts--;
}

void kvstoreFreeDictIfNeeded(kvstore *kvs, int didx) {
    freeDictIfNeeded(kvs, didx);
}

/**********************************/
/*** dict callbacks ***************/
/**********************************/

/* Adds dictionary to the rehashing list, which allows us
 * to quickly find rehash targets during incremental rehashing.
 *
 * If there are multiple dicts, updates the bucket count for the given dictionary
 * in a DB, bucket count incremented with the new ht size during the rehashing phase.
 * If there's one dict, bucket count can be retrieved directly from single dict bucket. */
static void kvstoreDictRehashingStarted(dict *d) {
    kvstore *kvs = d->type->userdata;
    kvstoreDictMetaBase *metadata = (kvstoreDictMetaBase *)dictMetadata(d);
    listAddNodeTail(kvs->rehashing, d);
    metadata->rehashing_node = listLast(kvs->rehashing);
    if (kvs->snap_hooks && kvs->snap_hooks->rehash_started)
        kvs->snap_hooks->rehash_started(kvs->snap_priv, metadata->didx, d);

    unsigned long long from, to;
    dictRehashingInfo(d, &from, &to);
    kvs->overhead_hashtable_rehashing += from;
}

/* Remove dictionary from the rehashing list.
 *
 * Updates the bucket count for the given dictionary in a DB. It removes
 * the old ht size of the dictionary from the total sum of buckets for a DB.  */
static void kvstoreDictRehashingCompleted(dict *d) {
    kvstore *kvs = d->type->userdata;
    kvstoreDictMetaBase *metadata = (kvstoreDictMetaBase *)dictMetadata(d);
    if (metadata->rehashing_node) {
        listDelNode(kvs->rehashing, metadata->rehashing_node);
        metadata->rehashing_node = NULL;
    }
    if (kvs->snap_hooks && kvs->snap_hooks->rehash_completed)
        kvs->snap_hooks->rehash_completed(kvs->snap_priv, metadata->didx, d);

    unsigned long long from, to;
    dictRehashingInfo(d, &from, &to);
    kvs->overhead_hashtable_rehashing -= from;
}

/* A dict a running snapshot is attached to may grow (its per-bucket state
 * splits one old bucket into k new ones) but not shrink (a merge has no
 * bucket-wise answer); the snapshot answers per dict. */
static int kvstoreDictShrinkAllowed(dict *d) {
    kvstore *kvs = d->type->userdata;
    if (!kvs->snap_hooks || !kvs->snap_hooks->shrink_allowed) return 1;
    kvstoreDictMetaBase *metadata = (kvstoreDictMetaBase *)dictMetadata(d);
    return kvs->snap_hooks->shrink_allowed(kvs->snap_priv, metadata->didx);
}

/* Updates the bucket count for the given dictionary in a DB. It adds the new ht size
 * of the dictionary or removes the old ht size of the dictionary from the total
 * sum of buckets for a DB. */
static void kvstoreDictBucketChanged(dict *d, long long delta) {
    kvstore *kvs = d->type->userdata;
    kvs->bucket_count += delta;
}

/* Returns the size of the DB dict extended metadata in bytes. */
static size_t kvstoreDictBaseMetaSize(dict *d) {
    UNUSED(d);
    return sizeof(kvstoreDictMetaBase);
}

/**********************************/
/*** API **************************/
/**********************************/

/* Create an array of dictionaries
 * num_dicts_bits is the log2 of the amount of dictionaries needed (e.g. 0 for 1 dict,
 * 3 for 8 dicts, etc.) */
kvstore *kvstoreCreate(kvstoreType *type, dictType *dtype, int num_dicts_bits, int flags) {
    /* We can't support more than 2^16 dicts because we want to save 48 bits
     * for the dict cursor, see kvstoreScan */
    assert(num_dicts_bits <= 16);
    assert(!type->dictMetadataBytes || type->dictMetadataBytes(NULL) >= sizeof(kvstoreDictMetaBase));

    /* Calc kvstore size */   
    size_t kvsize = sizeof(kvstore);
    /* Conditionally calc also histogram size */
    if (type->kvstoreMetadataBytes)
        kvsize += type->kvstoreMetadataBytes(NULL);
    
    kvstore *kvs = dtype->nofork_alloc ? zcalloc_nofork(kvsize) : zcalloc(kvsize);
    memcpy(&kvs->dtype, dtype, sizeof(kvs->dtype));
    kvs->flags = flags;
    kvs->type = type;

    /* kvstore must be the one to set these callbacks, so we make sure the
     * caller didn't do it */
    assert(!dtype->userdata);
    assert(!dtype->dictMetadataBytes);
    assert(!dtype->rehashingStarted);
    assert(!dtype->rehashingCompleted);
    kvs->dtype.userdata = kvs;
    kvs->dtype.dictMetadataBytes = type->dictMetadataBytes ?
        type->dictMetadataBytes : kvstoreDictBaseMetaSize;
    kvs->dtype.rehashingStarted = kvstoreDictRehashingStarted;
    kvs->dtype.rehashingCompleted = kvstoreDictRehashingCompleted;
    kvs->dtype.shrinkAllowed = kvstoreDictShrinkAllowed;
    kvs->dtype.bucketChanged = kvstoreDictBucketChanged;

    kvs->num_dicts_bits = num_dicts_bits;
    kvs->num_dicts = 1 << kvs->num_dicts_bits;
    kvs->dicts = dtype->nofork_alloc ? zcalloc_nofork(sizeof(dict*) * kvs->num_dicts)
                                     : zcalloc(sizeof(dict*) * kvs->num_dicts);
    if (!(kvs->flags & KVSTORE_ALLOCATE_DICTS_ON_DEMAND)) {
        for (int i = 0; i < kvs->num_dicts; i++)
            createDictIfNeeded(kvs, i);
    }

    kvs->rehashing = listCreate();
    kvs->key_count = 0;
    kvs->non_empty_dicts = 0;
    kvs->resize_cursor = 0;
    kvs->dict_sizes = kvs->num_dicts > 1 ? fwTreeCreate(kvs->num_dicts_bits) : NULL;
    kvs->bucket_count = 0;
    kvs->overhead_hashtable_rehashing = 0;
    return kvs;
}

/* Save state still attached when the store is emptied or released is a
 * leftover (the save that owned it is gone): release and count it. */
static void snapReleaseAll(kvstore *kvs) {
    dict *d;
    while ((d = kvstoreSnapFirstDict(kvs)) != NULL) {
        kvs->snap_leftovers++;
        kvstoreDictSnapRelease(kvs, ((kvstoreDictMetaBase *)dictMetadata(d))->didx);
    }
}

void kvstoreEmpty(kvstore *kvs, void(callback)(dict*)) {
    snapReleaseAll(kvs);
    for (int didx = 0; didx < kvs->num_dicts; didx++) {
        dict *d = kvstoreGetDict(kvs, didx);
        if (!d)
            continue;
        kvstoreDictMetaBase *metadata = (kvstoreDictMetaBase *)dictMetadata(d);
        if (metadata->rehashing_node)
            metadata->rehashing_node = NULL;
        dictEmpty(d, callback);
        if (kvs->type->onDictEmpty) kvs->type->onDictEmpty(kvs, didx);
        freeDictIfNeeded(kvs, didx);
    }

    if (kvs->type->onKvstoreEmpty) kvs->type->onKvstoreEmpty(kvs);

    listEmpty(kvs->rehashing);

    kvs->key_count = 0;
    kvs->non_empty_dicts = 0;
    kvs->resize_cursor = 0;
    kvs->bucket_count = 0;
    if (kvs->dict_sizes)
        fwTreeClear(kvs->dict_sizes);
    kvs->overhead_hashtable_rehashing = 0;
}

void kvstoreRelease(kvstore *kvs) {
    snapReleaseAll(kvs);
    for (int didx = 0; didx < kvs->num_dicts; didx++) {
        dict *d = kvstoreGetDict(kvs, didx);
        if (!d)
            continue;
        kvstoreDictMetaBase *metadata = (kvstoreDictMetaBase *)dictMetadata(d);
        if (metadata->rehashing_node)
            metadata->rehashing_node = NULL;
        if (kvs->type->onDictEmpty) kvs->type->onDictEmpty(kvs, didx);
        dictRelease(d);
    }
    zfree(kvs->dicts);

    listRelease(kvs->rehashing);
    if (kvs->ov_dirty) listRelease(kvs->ov_dirty);
    if (kvs->snap_dicts) listRelease(kvs->snap_dicts);
    if (kvs->dict_sizes)
        fwTreeDestroy(kvs->dict_sizes);

    zfree(kvs);
}

unsigned long long int kvstoreSize(kvstore *kvs) {
    return kvs->key_count;
}

/* This method provides the cumulative sum of all the dictionary buckets
 * across dictionaries in a database. */
unsigned long kvstoreBuckets(kvstore *kvs) {
    if (kvs->num_dicts != 1) {
        return kvs->bucket_count;
    } else {
        return kvs->dicts[0]? dictBuckets(kvs->dicts[0]) : 0;
    }
}

size_t kvstoreMemUsage(kvstore *kvs) {
    size_t mem = sizeof(*kvs);
    size_t metaSize = kvs->dtype.dictMetadataBytes(NULL);
    unsigned long long keys_count = kvstoreSize(kvs);
    mem += keys_count * dictEntryMemUsage(kvs->dtype.no_value) +
           kvstoreBuckets(kvs) * sizeof(dictEntry*) +
           kvs->allocated_dicts * (sizeof(dict) + metaSize);

    /* Values are dict* shared with kvs->dicts */
    mem += listLength(kvs->rehashing) * sizeof(listNode);

    return mem;
}

/*
 * This method is used to iterate over the elements of the entire kvstore specifically across dicts.
 * It's a three pronged approach.
 *
 * 1. It uses the provided cursor `cursor` to retrieve the dict index from it.
 * 2. If the dictionary is in a valid state checked through the provided callback `dictScanValidFunction`,
 *    it performs a dictScan over the appropriate `keyType` dictionary of `db`.
 * 3. If the dict is entirely scanned i.e. the cursor has reached 0, the next non empty dict is discovered.
 *    The dict information is embedded into the cursor and returned.
 *
 * To restrict the scan to a single dict, pass a valid dict index as
 * 'onlydidx', otherwise pass -1.
 */
struct ovScanCtx { kvstore *kvs; kvstoreDictOverlay *ov; dictScanFunction *fn; void *priv; };
static void ovScanCb(void *p, const dictEntry *de, dictEntry **plink) {
    (void)plink;
    struct ovScanCtx *c = p;
    dictEntry *m = ovMapFrozenEntry(c->kvs, c->ov, (dictEntry *)de);
    if (m) c->fn(c->priv, m, NULL);
}

unsigned long long kvstoreScan(kvstore *kvs, unsigned long long cursor,
                               int onlydidx, dictScanFunction *scan_cb,
                               kvstoreScanShouldSkipDict *skip_cb,
                               void *privdata)
{
    unsigned long long _cursor = 0;
    /* During dictionary traversal, 48 upper bits in the cursor are used for positioning in the HT.
     * Following lower bits are used for the dict index number, ranging from 0 to 2^num_dicts_bits-1.
     * Dict index is always 0 at the start of iteration and can be incremented only if there are
     * multiple dicts. */
    int didx = getAndClearDictIndexFromCursor(kvs, &cursor);
    if (onlydidx >= 0) {
        if (didx < onlydidx) {
            /* Fast-forward to onlydidx. */
            assert(onlydidx < kvs->num_dicts);
            didx = onlydidx;
            cursor = 0;
        } else if (didx > onlydidx) {
            /* The cursor is already past onlydidx. */
            return 0;
        }
    }

    dict *d = kvstoreGetDict(kvs, didx);
    kvstoreDictOverlay *ov = ovGetR(kvs, didx);
    if (!ovActive(ov)) ov = NULL;   /* empty side table: a plain scan */

    int skip = !d || (skip_cb && skip_cb(d, didx));
    if (!skip && ov) {
        /* Frozen dict: scan it through the overlay (skip tombstones,
         * substitute clones) and emit the ADDs with the final batch. */
        struct ovScanCtx sctx = { kvs, ov, scan_cb, privdata };
        _cursor = (d && dictSize(d)) ? dictScan(d, cursor, ovScanCb, &sctx) : 0;
        if (_cursor == 0 && ov->adds) {
            dictIterator it;
            dictInitSafeIterator(&it, ov->temp);
            dictEntry *te;
            while ((te = dictNext(&it)) != NULL)
                if (ovTempEntryIsAdd(kvs, didx, te)) scan_cb(privdata, te, NULL);
            dictResetIterator(&it);
        }
    } else if (!skip) {
        _cursor = dictScan(d, cursor, scan_cb, privdata);
        /* In dictScan, scan_cb may delete entries (e.g., in active expire case). */
        freeDictIfNeeded(kvs, didx);
    }
    /* scanning done for the current dictionary or if the scanning wasn't possible, move to the next dict index. */
    if (_cursor == 0 || skip) {
        if (onlydidx >= 0)
            return 0;
        didx = kvstoreGetNextNonEmptyDictIndex(kvs, didx);
    }
    if (didx == -1) {
        return 0;
    }
    addDictIndexToCursor(kvs, didx, &_cursor);
    return _cursor;
}

/*
 * This functions increases size of kvstore to match desired number.
 * It resizes all individual dictionaries, unless skip_cb indicates otherwise.
 *
 * Based on the parameter `try_expand`, appropriate dict expand API is invoked.
 * if try_expand is set to 1, `dictTryExpand` is used else `dictExpand`.
 * The return code is either `DICT_OK`/`DICT_ERR` for both the API(s).
 * `DICT_OK` response is for successful expansion. However, `DICT_ERR` response signifies failure in allocation in
 * `dictTryExpand` call and in case of `dictExpand` call it signifies no expansion was performed.
 */
int kvstoreExpand(kvstore *kvs, uint64_t newsize, int try_expand, kvstoreExpandShouldSkipDictIndex *skip_cb) {
    for (int i = 0; i < kvs->num_dicts; i++) {
        if (skip_cb && skip_cb(i)) continue;
        dict *d = createDictIfNeeded(kvs, i);
        if (!d) continue;
        if (d->pauseAutoResize > 0) continue;   /* frozen by a snapshot: layout must not change */

        int result = try_expand ? dictTryExpand(d, newsize) : dictExpand(d, newsize);
        if (try_expand && result == DICT_ERR)
            return 0;
    }

    return 1;
}

/* Returns fair random dict index, probability of each dict being returned is
 * proportional to the number of elements that dictionary holds.
 * This function guarantees that it returns a dict-index of a non-empty dict,
 * unless the entire kvstore is empty or all dicts are skipped.
 *
 * Parameters:
 * - kvs: the kvstore instance
 * - skip_cb: callback to determine if a dict should be skipped (NULL means no skipping)
 * - fair_attempts: number of fair selection attempts before falling back
 * - slow_fallback: if 1, uses systematic search when fair attempts fail
 *
 * Returns:
 * - Valid dict index (>= 0) on success
 * - -1 if no valid dict found (either slow_fallback is 0 or all dicts are skipped)
 *
 * Time complexity: O(fair_attempts * log(kvs->num_dicts)) for fair attempts,
 * plus O(kvs->num_dicts) for systematic fallback if enabled.
 */
int kvstoreGetFairRandomDictIndex(kvstore *kvs, kvstoreRandomShouldSkipDictIndex *skip_cb,
                                  int fair_attempts, int slow_fallback)
{
    if (kvs->num_dicts == 1 || kvstoreSize(kvs) == 0)
        return 0;

    unsigned long long total_size = kvstoreSize(kvs);

    /* Try fair attempts first. If skip_cb is not applicable, execute only once. */
    for (int attempt = 0; attempt < fair_attempts; attempt++) {
        unsigned long target = (randomULong() % total_size) + 1;
        int didx = kvstoreFindDictIndexByKeyIndex(kvs, target);
        if (!skip_cb || !skip_cb(didx)) {
            return didx;
        }
    }

    /* If fair attempts failed and slow fallback is allowed */
    if (slow_fallback) {
        /* systematic check from random start */
        int start = randomULong() % kvs->num_dicts;
        for (int i = 0; i < kvs->num_dicts; i++) {
            int didx = (start + i) % kvs->num_dicts;
            dict *d = kvstoreGetDict(kvs, didx);
            if (d && (!skip_cb || !skip_cb(didx))) {
                return didx;
            }
        }
    }

    /* Failed to find valid dict that has elements */
    return -1;
}

void kvstoreGetStats(kvstore *kvs, char *buf, size_t bufsize, int full) {
    buf[0] = '\0';

    size_t l;
    char *orig_buf = buf;
    size_t orig_bufsize = bufsize;
    dictStats *mainHtStats = NULL;
    dictStats *rehashHtStats = NULL;
    dict *d;
    kvstoreIterator kvs_it;

    kvstoreIteratorInit(&kvs_it, kvs);
    while ((d = kvstoreIteratorNextDict(&kvs_it))) {
        dictStats *stats = dictGetStatsHt(d, 0, full);
        if (!mainHtStats) {
            mainHtStats = stats;
        } else {
            dictCombineStats(stats, mainHtStats);
            dictFreeStats(stats);
        }
        if (dictIsRehashing(d)) {
            stats = dictGetStatsHt(d, 1, full);
            if (!rehashHtStats) {
                rehashHtStats = stats;
            } else {
                dictCombineStats(stats, rehashHtStats);
                dictFreeStats(stats);
            }
        }
    }
    kvstoreIteratorReset(&kvs_it);

    if (mainHtStats && bufsize > 0) {
        l = dictGetStatsMsg(buf, bufsize, mainHtStats, full);
        dictFreeStats(mainHtStats);
        buf += l;
        bufsize -= l;
    }

    if (rehashHtStats && bufsize > 0) {
        l = dictGetStatsMsg(buf, bufsize, rehashHtStats, full);
        dictFreeStats(rehashHtStats);
        buf += l;
        bufsize -= l;
    }
    /* Make sure there is a NULL term at the end. */
    if (orig_bufsize) orig_buf[orig_bufsize - 1] = '\0';
}

/* Finds a dict containing target element in a key space ordered by dict index.
 * Consider this example. Dictionaries are represented by brackets and keys by dots:
 *  #0   #1   #2     #3    #4
 * [..][....][...][.......][.]
 *                    ^
 *                 target
 *
 * In this case dict #3 contains key that we are trying to find.
 *
 * The return value is 0 based dict-index, and the range of the target is [1..kvstoreSize], kvstoreSize inclusive.
 *
 * To find the dict, we start with the root node of the binary index tree and search through its children
 * from the highest index (2^num_dicts_bits in our case) to the lowest index. At each node, we check if the target
 * value is greater than the node's value. If it is, we remove the node's value from the target and recursively
 * search for the new target using the current node as the parent.
 * Time complexity of this function is O(log(kvs->num_dicts))
 */
int kvstoreFindDictIndexByKeyIndex(kvstore *kvs, unsigned long target) {
    if (kvs->num_dicts == 1 || kvstoreSize(kvs) == 0)
        return 0;
    assert(target <= kvstoreSize(kvs));

    return fwTreeFindIndex(kvs->dict_sizes, target);
}

/* Get the first non-empty dict index in the kvstore. Returns -1 if kvstore is empty. */
int kvstoreGetFirstNonEmptyDictIndex(kvstore *kvs) {
    if (kvstoreSize(kvs) == 0)
        return -1;
    if (kvs->num_dicts == 1)
        return 0;
    return fwTreeFindFirstNonEmpty(kvs->dict_sizes);
}

/* Returns next non-empty dict index strictly after given one, or -1 if provided didx is the last one. */
int kvstoreGetNextNonEmptyDictIndex(kvstore *kvs, int didx) {
    if (kvs->num_dicts == 1) {
        assert(didx == 0);
        return -1;
    }
    return fwTreeFindNextNonEmpty(kvs->dict_sizes, didx);
}

int kvstoreNumNonEmptyDicts(kvstore *kvs) {
    return kvs->non_empty_dicts;
}

int kvstoreNumAllocatedDicts(kvstore *kvs) {
    return kvs->allocated_dicts;
}

int kvstoreNumDicts(kvstore *kvs) {
    return kvs->num_dicts;
}

/* Move dict from one kvstore to another. */
void kvstoreMoveDict(kvstore *kvs, kvstore *dst, int didx) {
    assert(kvs->num_dicts > didx);
    assert(kvs->num_dicts == dst->num_dicts);
    assert(dst->dicts[didx] == NULL);

    dict *d = kvs->dicts[didx];
    if (d == NULL) return;

    /* Adjust source kvstore */
    kvs->allocated_dicts -= 1;
    cumulativeKeyCountAdd(kvs, didx, -((long long)dictSize(d)));
    kvstoreDictBucketChanged(d, -((long long) dictBuckets(d)));
    /* If rehashing, stop it. */
    if (dictIsRehashing(d))
        kvstoreDictRehashingCompleted(d);
    /* Clear dict from source kvstore and create a new one if needed */
    kvs->dicts[didx] = NULL;
    if (!(kvs->flags & (KVSTORE_ALLOCATE_DICTS_ON_DEMAND | KVSTORE_FREE_EMPTY_DICTS)))
        createDictIfNeeded(kvs, didx);

    /* Move dict to destination kvstore */
    dst->dicts[didx] = d;
    dst->dicts[didx]->type = &dst->dtype;
    dst->allocated_dicts += 1;
    cumulativeKeyCountAdd(dst, didx, dictSize(d));
    kvstoreDictBucketChanged(d, dictBuckets(d));
    if (dictIsRehashing(dst->dicts[didx]))
        kvstoreDictRehashingStarted(dst->dicts[didx]);
}

/* Returns kvstore iterator that can be used to iterate through sub-dictionaries.
 *
 * The caller should reset kvs_it with kvstoreIteratorReset. */
void kvstoreIteratorInit(kvstoreIterator *kvs_it, kvstore *kvs) {
    kvs_it->kvs = kvs;
    kvs_it->didx = -1;
    kvs_it->ov_phase = 0;
    kvs_it->next_didx = kvstoreGetFirstNonEmptyDictIndex(kvs_it->kvs); /* Finds first non-empty dict index. */
    dictInitSafeIterator(&kvs_it->di, NULL);
}

/* Free the kvs_it returned by kvstoreIteratorInit. */
void kvstoreIteratorReset(kvstoreIterator *kvs_it) {
    dictIterator *iter = &kvs_it->di;
    dictResetIterator(iter);
    /* In the safe iterator context, we may delete entries. */
    if (kvs_it->didx != -1)
        freeDictIfNeeded(kvs_it->kvs, kvs_it->didx);
}

/* Returns next dictionary from the iterator, or NULL if iteration is complete.
 *
 * - Takes care to reset the iter of the previous dict before moved to the next dict.
 */
dict *kvstoreIteratorNextDict(kvstoreIterator *kvs_it) {
    if (kvs_it->next_didx == -1)
        return NULL;

    /* The dict may be deleted during the iteration process, so here need to check for NULL. */
    if (kvs_it->didx != -1 && kvstoreGetDict(kvs_it->kvs, kvs_it->didx)) {
        /* Before we move to the next dict, reset the iter of the previous dict. */
        dictIterator *iter = &kvs_it->di;
        dictResetIterator(iter);
        /* In the safe iterator context, we may delete entries. */
        freeDictIfNeeded(kvs_it->kvs, kvs_it->didx);
    }

    kvs_it->didx = kvs_it->next_didx;
    kvs_it->next_didx = kvstoreGetNextNonEmptyDictIndex(kvs_it->kvs, kvs_it->didx);
    kvs_it->ov_phase = 0;
    return kvs_it->kvs->dicts[kvs_it->didx];
}

int kvstoreIteratorGetCurrentDictIndex(kvstoreIterator *kvs_it) {
    assert(kvs_it->didx >= 0 && kvs_it->didx < kvs_it->kvs->num_dicts);
    return kvs_it->didx;
}

/* Returns next entry. */
dictEntry *kvstoreIteratorNext(kvstoreIterator *kvs_it) {
    for (;;) {
        dictEntry *de = kvs_it->di.d ? dictNext(&kvs_it->di) : NULL;
        kvstoreDictOverlay *ov = (kvs_it->didx >= 0) ? ovGetR(kvs_it->kvs, kvs_it->didx) : NULL;
        if (!ovActive(ov)) ov = NULL;
        if (!de) { /* No current dict or reached the end of the dictionary. */
            if (ov && kvs_it->ov_phase == 0 && kvs_it->di.d) {
                /* Frozen dict exhausted: continue with the ADDs of its temp dict. */
                dictResetIterator(&kvs_it->di);
                dictInitSafeIterator(&kvs_it->di, ov->temp);
                kvs_it->ov_phase = 1;
                continue;
            }
            /* Before we move to the next dict, function kvstoreIteratorNextDict()
             * reset the iter of the previous dict & freeDictIfNeeded(). */
            dict *d = kvstoreIteratorNextDict(kvs_it);
            if (!d)
                return NULL;
            dictInitSafeIterator(&kvs_it->di, d);
            continue;
        }
        if (!ov) return de;
        if (kvs_it->ov_phase == 0) {
            dictEntry *m = ovMapFrozenEntry(kvs_it->kvs, ov, de);
            if (m) return m;
            continue;                                   /* tombstoned */
        }
        if (ovTempEntryIsAdd(kvs_it->kvs, kvs_it->didx, de)) return de;
        /* clone already emitted in phase 0 */
    }
}

/* This method traverses through kvstore dictionaries and triggers a resize,
 * unless skip_cb indicates otherwise. It first tries to shrink if needed, and
 * if it doesn't try to shrink, it tries to expand. */
void kvstoreTryResizeDicts(kvstore *kvs, int limit, kvstoreResizeShouldSkipDictIndex *skip_cb) {
    if (limit > kvs->num_dicts)
        limit = kvs->num_dicts;

    for (int i = 0; i < limit; i++) {
        int didx = kvs->resize_cursor;
        dict *d = kvstoreGetDict(kvs, didx);
        if (d && (!skip_cb || !skip_cb(didx))) {
            if (dictShrinkIfNeeded(d) == DICT_ERR)
                dictExpandIfNeeded(d);
        }
        kvs->resize_cursor = (didx + 1) % kvs->num_dicts;
    }
}

/* Our hash table implementation performs rehashing incrementally while
 * we write/read from the hash table. Still if the server is idle, the hash
 * table will use two tables for a long time. So we try to use threshold_us
 * of CPU time at every call of this function to perform some rehashing.
 *
 * The function returns the amount of microsecs spent if some rehashing was
 * performed, otherwise 0 is returned. */
uint64_t kvstoreIncrementallyRehash(kvstore *kvs, uint64_t threshold_us) {
    if (listLength(kvs->rehashing) == 0)
        return 0;

    /* Our goal is to rehash as many dictionaries as we can before reaching threshold_us,
     * after each dictionary completes rehashing, it removes itself from the list. */
    listNode *node;
    monotime timer;
    uint64_t elapsed_us = 0;
    elapsedStart(&timer);
    while ((node = listFirst(kvs->rehashing))) {
        dictRehashMicroseconds(listNodeValue(node), threshold_us - elapsed_us);

        elapsed_us = elapsedUs(timer);
        if (elapsed_us >= threshold_us) {
            break;  /* Reached the time limit. */
        }
    }
    return elapsed_us;
}

size_t kvstoreOverheadHashtableLut(kvstore *kvs) {
    return kvs->bucket_count * sizeof(dictEntry *);
}

size_t kvstoreOverheadHashtableRehashing(kvstore *kvs) {
    return kvs->overhead_hashtable_rehashing * sizeof(dictEntry *);
}

unsigned long kvstoreDictRehashingCount(kvstore *kvs) {
    return listLength(kvs->rehashing);
}

unsigned long kvstoreDictSize(kvstore *kvs, int didx)
{
    if (ovActive(ovGetR(kvs, didx))) return ovDictSize(kvs, didx);
    dict *d = kvstoreGetDict(kvs, didx);
    if (!d)
        return 0;
    return dictSize(d);
}

void kvstoreInitDictIterator(kvstoreDictIterator *kvs_di, kvstore *kvs, int didx)
{
    kvs_di->kvs = kvs;
    kvs_di->didx = didx;
    kvs_di->ov_phase = 0;
    dict *d = kvstoreGetDict(kvs, didx);
    dictInitIterator(&kvs_di->di, d);
}

void kvstoreInitDictSafeIterator(kvstoreDictIterator *kvs_di, kvstore *kvs, int didx)
{
    kvs_di->kvs = kvs;
    kvs_di->didx = didx;
    kvs_di->ov_phase = 0;
    dict *d = kvstoreGetDict(kvs, didx);
    dictInitSafeIterator(&kvs_di->di, d);
}

/* Free the kvs_di returned by kvstoreGetDictIterator and kvstoreGetDictSafeIterator. */
void kvstoreResetDictIterator(kvstoreDictIterator *kvs_di)
{
    /* The dict may be deleted during the iteration process, so here need to check for NULL. */
    if (kvstoreGetDict(kvs_di->kvs, kvs_di->didx)) {
        dictResetIterator(&kvs_di->di);
        /* In the safe iterator context, we may delete entries. */
        freeDictIfNeeded(kvs_di->kvs, kvs_di->didx);
    }
}

/* Get the next element of the dict through kvstoreDictIterator and dictNext. */
dictEntry *kvstoreDictIteratorNext(kvstoreDictIterator *kvs_di)
{
    kvstoreDictOverlay *ov = ovGetR(kvs_di->kvs, kvs_di->didx);
    if (!ovActive(ov)) ov = NULL;
    for (;;) {
        /* The dict may be deleted during the iteration process, so here need to check for NULL. */
        if (!kvs_di->di.d) return NULL;
        if (!ov && !kvstoreGetDict(kvs_di->kvs, kvs_di->didx)) return NULL;
        dictEntry *de = dictNext(&kvs_di->di);
        if (!de) {
            if (ov && kvs_di->ov_phase == 0) {
                int safe = kvs_di->di.safe;
                dictResetIterator(&kvs_di->di);
                if (safe) dictInitSafeIterator(&kvs_di->di, ov->temp);
                else dictInitIterator(&kvs_di->di, ov->temp);
                kvs_di->ov_phase = 1;
                continue;
            }
            return NULL;
        }
        if (!ov) return de;
        if (kvs_di->ov_phase == 0) {
            dictEntry *m = ovMapFrozenEntry(kvs_di->kvs, ov, de);
            if (m) return m;
            continue;
        }
        if (ovTempEntryIsAdd(kvs_di->kvs, kvs_di->didx, de)) return de;
    }
}

/* Random entry of a frozen dict: sample the real dict (mapping through the
 * overlay, retrying on tombstones) and fall back to the temp dict. */
static dictEntry *ovRandomEntry(kvstore *kvs, int didx, kvstoreDictOverlay *ov, int fair) {
    dict *d = kvs->dicts[didx];
    if (ovDictSize(kvs, didx) == 0) return NULL;
    if (d && dictSize(d)) {
        for (int tries = 0; tries < 16; tries++) {
            dictEntry *de = fair ? dictGetFairRandomKey(d) : dictGetRandomKey(d);
            if (!de) break;
            dictEntry *m = ovMapFrozenEntry(kvs, ov, de);
            if (m) return m;
        }
    }
    if (dictSize(ov->temp)) {
        dictEntry *te = dictGetRandomKey(ov->temp);
        if (te) return te;   /* clones and adds are both logically present */
    }
    /* Only tombstoned frozen keys were sampled: scan for any live entry. */
    kvstoreDictIterator it;
    kvstoreInitDictIterator(&it, kvs, didx);
    dictEntry *de = kvstoreDictIteratorNext(&it);
    kvstoreResetDictIterator(&it);
    return de;
}

dictEntry *kvstoreDictGetRandomKey(kvstore *kvs, int didx)
{
    kvstoreDictOverlay *ov = ovGetR(kvs, didx);
    if (ovActive(ov)) return ovRandomEntry(kvs, didx, ov, 0);
    dict *d = kvstoreGetDict(kvs, didx);
    if (!d)
        return NULL;
    return dictGetRandomKey(d);
}

dictEntry *kvstoreDictGetFairRandomKey(kvstore *kvs, int didx)
{
    kvstoreDictOverlay *ov = ovGetR(kvs, didx);
    if (ovActive(ov)) return ovRandomEntry(kvs, didx, ov, 1);
    dict *d = kvstoreGetDict(kvs, didx);
    if (!d)
        return NULL;
    return dictGetFairRandomKey(d);
}

unsigned int kvstoreDictGetSomeKeys(kvstore *kvs, int didx, dictEntry **des, unsigned int count)
{
    kvstoreDictOverlay *ov = ovGetR(kvs, didx);
    if (ovActive(ov)) {
        unsigned int n = 0;
        dict *d = kvs->dicts[didx];
        if (d && dictSize(d)) {
            unsigned int got = dictGetSomeKeys(d, des, count);
            for (unsigned int i = 0; i < got; i++) {
                dictEntry *m = ovMapFrozenEntry(kvs, ov, des[i]);
                if (m) des[n++] = m;
            }
        }
        if (n < count && dictSize(ov->temp)) {
            dictEntry **tmp = zmalloc(sizeof(dictEntry *) * count);
            unsigned int got = dictGetSomeKeys(ov->temp, tmp, count - n);
            for (unsigned int i = 0; i < got; i++)
                if (ovTempEntryIsAdd(kvs, didx, tmp[i])) des[n++] = tmp[i];
            zfree(tmp);
        }
        return n;
    }
    dict *d = kvstoreGetDict(kvs, didx);
    if (!d)
        return 0;
    return dictGetSomeKeys(d, des, count);
}

int kvstoreDictExpand(kvstore *kvs, int didx, unsigned long size)
{
    if (ovGetR(kvs, didx)) return DICT_OK;   /* frozen: never resize the scanned dict */
    dict *d = createDictIfNeeded(kvs, didx);
    if (!d)
        return DICT_ERR;
    return dictExpand(d, size);
}

unsigned long kvstoreDictScanDefrag(kvstore *kvs, int didx, unsigned long v, dictScanFunction *fn, dictDefragFunctions *defragfns, void *privdata)
{
    dict *d = kvstoreGetDict(kvs, didx);
    if (!d)
        return 0;
    return dictScanDefrag(d, v, fn, defragfns, privdata);
}

/* Unlike kvstoreDictScanDefrag(), this method doesn't defrag the data(keys and values)
 * within dict, it only reallocates the memory used by the dict structure itself using 
 * the provided allocation function. This feature was added for the active defrag feature.
 *
 * With 16k dictionaries for cluster mode with 1 shard, this operation may require substantial time
 * to execute.  A "cursor" is used to perform the operation iteratively.  When first called, a
 * cursor value of 0 should be provided.  The return value is an updated cursor which should be
 * provided on the next iteration.  The operation is complete when 0 is returned.
 *
 * The 'defragfn' callback is called with a reference to the dict that callback can reallocate. */
unsigned long kvstoreDictLUTDefrag(kvstore *kvs, unsigned long cursor, kvstoreDictLUTDefragFunction *defragfn) {
    for (int didx = cursor; didx < kvs->num_dicts; didx++) {
        dict **d = kvstoreGetDictRef(kvs, didx), *newd;
        if (!*d)
            continue;
        if ((newd = defragfn(*d))) {
            *d = newd;

            /* After defragmenting the dict, update its corresponding
             * rehashing node in the kvstore's rehashing list. */
            kvstoreDictMetaBase *metadata = (kvstoreDictMetaBase *)dictMetadata(*d);
            if (metadata->rehashing_node)
                metadata->rehashing_node->value = *d;
        }
        return (didx + 1);
    }
    return 0;
}

void *kvstoreDictFetchValue(kvstore *kvs, int didx, const void *key)
{
    dict *d = kvstoreGetDict(kvs, didx);
    if (!d)
        return NULL;
    assert(d->type->no_value == 0); 
    return dictFetchValue(d, key);
}

/* Overlay (frozen slot) halves of the hot kvstore entry points: out of line so
 * the common path stays as small as it was and keeps being inlined. */
static dictEntry * __attribute__((noinline)) ovDictFind(kvstore *kvs, int didx, kvstoreDictOverlay *ov, void *key) {
    /* Read: the logical keyspace, no cloning. */
    dictEntry *te = dictFind(ov->temp, key);
    if (te) return te;
    if (ovStateGet(ov, key) & OV_STATE_TOMB) return NULL;
    dict *d = kvs->dicts[didx];
    return d ? dictFind(d, key) : NULL;
}

dictEntry *kvstoreDictFind(kvstore *kvs, int didx, void *key) {
    kvstoreDictOverlay *ov = ovGetR(kvs, didx);
    if (__builtin_expect(ovActive(ov), 0)) return ovDictFind(kvs, didx, ov, key);
    dict *d = kvstoreGetDict(kvs, didx);
    if (!d)
        return NULL;
    return dictFind(d, key);
}

/* Find a link to a key in the specified kvstore. If not found return NULL.
 *
 * This function is a wrapper around dictFindLink(), used to locate a key in a dict
 * from a kvstore. 
 *
 * The caller may provide a bucket pointer to receive the reference to the bucket 
 * where the key is stored or need to be added.
 *
 * Returns:
 *   A reference to the dictEntry if found, otherwise NULL.
 *   
 * Important: 
 * After calling kvstoreDictFindLink(), any necessary updates based on returned 
 * link or bucket must be made immediately after, commonly by kvstoreDictSetAtLink() 
 * without any operations in between that might modify the dict. Otherwise, 
 * the link or bucket may become invalid. Example usage:
 *
 *      link = kvstoreDictFindLink(kvs, didx, key, &bucket);
 *      ... Do something, but don't modify kvs->dicts[didx] ...
 *      if (link)
 *          kvstoreDictSetAtLink(kvs, didx, kv, &link, 0);   // Update existing entry
 *      else
 *          kvstoreDictSetAtLink(kvs, didx, kv, &bucket, 1); // Insert new entry
 */
static dictEntryLink __attribute__((noinline)) ovDictFindLink(kvstore *kvs, int didx, kvstoreDictOverlay *ov, void *key, dictEntryLink *bucket) {
    ovEnter(ov);
    /* Write intent. A key that already has a temp entry (clone or add) or a
     * tombstone (its re-add must land in the temp dict) is served there. */
    if (dictFind(ov->temp, key) || (ovStateGet(ov, key) & OV_STATE_TOMB))
        return dictFindLink(ov->temp, key, bucket);
    /* Otherwise the snapshot decides: a bucket it already holds may be
     * written in place (the frozen link, or the frozen bucket for an insert),
     * a bucket it is reading right now is cloned on first touch (a miss
     * returns the temp bucket so the insertion lands in the temp dict). */
    if (ovPrepare(kvs, didx, ov, key) == KVSTORE_OV_INPLACE)
        return dictFindLink(kvs->dicts[didx], key, bucket);
    ovEnsureClone(kvs, didx, ov, key);
    return dictFindLink(ov->temp, key, bucket);
}

dictEntryLink kvstoreDictFindLink(kvstore *kvs, int didx, void *key, dictEntryLink *bucket) {
    if (bucket) *bucket = NULL;    
    kvstoreDictOverlay *ov = ovGetW(kvs, didx);
    if (__builtin_expect(ov != NULL, 0)) return ovDictFindLink(kvs, didx, ov, key, bucket);
    dict *d = kvstoreGetDict(kvs, didx);
    if (!d) return NULL;
    return dictFindLink(d, key, bucket);
}

/* Set a key (or key-value) in the specified kvstore. 
 *
 * This function inserts a new key or updates an existing one, depending on 
 * the `newItem` flag.
 *
 * Parameters:
 * link:      - When `newItem` is set, `link` points to the bucket of the key.
 *            - When `newItem` is not set, `link` points to the link of the key.
 *            - If link is NULL, dictFindLink() will be called to locate the link.
 *          
 * newItem: - If set, add a new key with a new dictEntry.
 *          - If not set, update the key of an existing dictEntry.
 */
static void __attribute__((noinline)) ovDictSetAtLink(kvstore *kvs, int didx, kvstoreDictOverlay *ov, void *kv, dictEntryLink *link, int newItem) {
    ovEnter(ov);
    dict *d = kvs->dicts[didx];
    const void *key = (kv && kvs->dtype.keyFromStoredKey) ? kvs->dtype.keyFromStoredKey(kv) : kv;
    if (!newItem) {
        /* Replacing the key of an existing entry: with a link it is a store
         * through the link (the dict only lends its type); without one, look
         * the key up in whichever dict holds it. */
        dict *target = ov->temp;
        if ((!link || *link == NULL) && !dictFind(ov->temp, key)) target = d;
        dictSetKeyAtLink(target, kv, link, 0);
        return;
    }
    /* New item: a bucket link tells the dict (frozen buckets come from an
     * INPLACE write lookup of this same section); without one decide now. */
    int in_frozen;
    if (link && *link) {
        in_frozen = ovBucketInDict(d, *link);
    } else {
        in_frozen = !dictFind(ov->temp, key) && !(ovStateGet(ov, key) & OV_STATE_TOMB) &&
                    ovPrepare(kvs, didx, ov, key) == KVSTORE_OV_INPLACE;
    }
    if (in_frozen) {
        dictSetKeyAtLink(d, kv, link, 1);   /* no expand: auto-resize is paused */
        ovKeyCountAdd(kvs, didx, 1);
    } else {
        dictSetKeyAtLink(ov->temp, kv, link, 1);
        ovOnTempInsert(kvs, didx, ov, key);
    }
}

void kvstoreDictSetAtLink(kvstore *kvs, int didx, void *kv, dictEntryLink *link, int newItem) {
    dict *d;
    kvstoreDictOverlay *ov = ovGetW(kvs, didx);
    if (__builtin_expect(ov != NULL, 0)) { ovDictSetAtLink(kvs, didx, ov, kv, link, newItem); return; }
    if (newItem) {
        d = createDictIfNeeded(kvs, didx);
        dictSetKeyAtLink(d, kv, link, newItem);
        cumulativeKeyCountAdd(kvs, didx, 1); /* must be called only after updating dict */
    } else {
        d = kvstoreGetDict(kvs, didx);
        dictSetKeyAtLink(d, kv, link, newItem);
    }
}

static dictEntry * __attribute__((noinline)) ovDictAddRaw(kvstore *kvs, int didx, kvstoreDictOverlay *ov, void *key, dictEntry **existing) {
    ovEnter(ov);
    const void *lkey = kvs->dtype.keyFromStoredKey ? kvs->dtype.keyFromStoredKey(key) : key;
    dictEntry *te = dictFind(ov->temp, lkey);
    if (te) { if (existing) *existing = te; return NULL; }
    dictEntry *ret;
    if (!(ovStateGet(ov, lkey) & OV_STATE_TOMB)) {
        if (ovPrepare(kvs, didx, ov, lkey) == KVSTORE_OV_INPLACE) {
            ret = dictAddRaw(kvs->dicts[didx], key, existing);
            if (ret) ovKeyCountAdd(kvs, didx, 1);
            return ret;
        }
        te = ovEnsureClone(kvs, didx, ov, lkey);
        if (te) { if (existing) *existing = te; return NULL; }
    }
    ret = dictAddRaw(ov->temp, key, existing);
    if (ret) ovOnTempInsert(kvs, didx, ov, lkey);
    return ret;
}

dictEntry *kvstoreDictAddRaw(kvstore *kvs, int didx, void *key, dictEntry **existing) {
    kvstoreDictOverlay *ov = ovGetW(kvs, didx);
    if (__builtin_expect(ov != NULL, 0)) return ovDictAddRaw(kvs, didx, ov, key, existing);
    dict *d = createDictIfNeeded(kvs, didx);
    dictEntry *ret = dictAddRaw(d, key, existing);
    if (ret)
        cumulativeKeyCountAdd(kvs, didx, 1);
    return ret;
}

void kvstoreDictSetKey(kvstore *kvs, int didx, dictEntry* de, void *key) {
    dict *d = kvstoreGetDict(kvs, didx);
    dictSetKey(d, de, key);
}

void kvstoreDictSetVal(kvstore *kvs, int didx, dictEntry *de, void *val) {
    dict *d = kvstoreGetDict(kvs, didx);
    assert(d->type->no_value == 0); 
    dictSetVal(d, de, val);
}

static dictEntryLink __attribute__((noinline)) ovDictTwoPhaseUnlinkFind(kvstore *kvs, int didx, kvstoreDictOverlay *ov, const void *key, int *table_index) {
    ovEnter(ov);
    ov->unlink_frozen = 0;
    if (!dictFind(ov->temp, key)) {
        if (ovStateGet(ov, key) & OV_STATE_TOMB) return NULL;
        if (ovPrepare(kvs, didx, ov, key) == KVSTORE_OV_INPLACE) {
            dictEntryLink l = dictTwoPhaseUnlinkFind(kvs->dicts[didx], key, table_index);
            if (l) ov->unlink_frozen = 1;
            return l;
        }
        if (!ovEnsureClone(kvs, didx, ov, key)) return NULL;
    }
    return dictTwoPhaseUnlinkFind(ov->temp, key, table_index);
}

dictEntryLink kvstoreDictTwoPhaseUnlinkFind(kvstore *kvs, int didx, const void *key, int *table_index) {
    kvstoreDictOverlay *ov = ovGetW(kvs, didx);
    if (__builtin_expect(ov != NULL, 0)) return ovDictTwoPhaseUnlinkFind(kvs, didx, ov, key, table_index);
    dict *d = kvstoreGetDict(kvs, didx);
    if (!d)
        return NULL;
    return dictTwoPhaseUnlinkFind(kvstoreGetDict(kvs, didx), key, table_index);
}

void kvstoreDictTwoPhaseUnlinkFree(kvstore *kvs, int didx, dictEntryLink link, int table_index) {
    kvstoreDictTwoPhaseUnlinkFreeKey(kvs, didx, link, table_index, NULL);
}

/* As above, with the key given explicitly: callers that already detached the
 * value from the entry (dbGenericDelete's async path stores NULL at the link
 * before unlinking) cannot have it read back from the entry. 'key' may be
 * NULL when the entry still holds it. */
static void __attribute__((noinline)) ovDictTwoPhaseUnlinkFreeKey(kvstore *kvs, int didx, kvstoreDictOverlay *ov, dictEntryLink link, int table_index, const void *key) {
    ovEnter(ov);
    if (ov->unlink_frozen) {
        /* Second phase of an in-place unlink (see ovDictTwoPhaseUnlinkFind). */
        ov->unlink_frozen = 0;
        dictTwoPhaseUnlinkFree(kvs->dicts[didx], link, table_index);
        ovKeyCountAdd(kvs, didx, -1);
        return;
    }
    if (link && *link) {
        /* Bookkeeping first: the key pointer dies with the temp entry. */
        if (!key) key = ovEntryKey(kvs, *link);
        ovOnTempRemove(kvs, didx, ov, key);
        dictTwoPhaseUnlinkFree(ov->temp, link, table_index);
    }
}

void kvstoreDictTwoPhaseUnlinkFreeKey(kvstore *kvs, int didx, dictEntryLink link, int table_index, const void *key) {
    kvstoreDictOverlay *ov = ovGetW(kvs, didx);
    if (__builtin_expect(ov != NULL, 0)) { ovDictTwoPhaseUnlinkFreeKey(kvs, didx, ov, link, table_index, key); return; }
    dict *d = kvstoreGetDict(kvs, didx);
    dictTwoPhaseUnlinkFree(d, link, table_index);
    cumulativeKeyCountAdd(kvs, didx, -1);
    freeDictIfNeeded(kvs, didx);
}

static int __attribute__((noinline)) ovDictDelete(kvstore *kvs, int didx, kvstoreDictOverlay *ov, const void *key) {
    ovEnter(ov);
    if (!dictFind(ov->temp, key)) {
        if (ovStateGet(ov, key) & OV_STATE_TOMB) return DICT_ERR;
        if (ovPrepare(kvs, didx, ov, key) == KVSTORE_OV_INPLACE) {
            int ret = dictDelete(kvs->dicts[didx], key);
            if (ret == DICT_OK) ovKeyCountAdd(kvs, didx, -1);
            return ret;
        }
        if (!ovEnsureClone(kvs, didx, ov, key)) return DICT_ERR;
    }
    ovOnTempRemove(kvs, didx, ov, key);
    int ret = dictDelete(ov->temp, key);
    assert(ret == DICT_OK);
    return ret;
}

int kvstoreDictDelete(kvstore *kvs, int didx, const void *key) {
    kvstoreDictOverlay *ov = ovGetW(kvs, didx);
    if (__builtin_expect(ov != NULL, 0)) return ovDictDelete(kvs, didx, ov, key);
    dict *d = kvstoreGetDict(kvs, didx);
    if (!d)
        return DICT_ERR;
    int ret = dictDelete(d, key);
    if (ret == DICT_OK) {
        cumulativeKeyCountAdd(kvs, didx, -1);
        freeDictIfNeeded(kvs, didx);
    }
    return ret;
}

void *kvstoreGetDictMeta(kvstore *kvs, int didx, int createIfNeeded) {
    dict *d = kvstoreGetDict(kvs, didx);
    if (!d) {
        if (!createIfNeeded) return NULL;
        d = createDictIfNeeded(kvs, didx);
    }
    return dictMetadata(d);
}

void *kvstoreGetMetadata(kvstore *kvs) {
    if (!kvs->type->kvstoreMetadataBytes)
        return NULL;
    return &kvs->metadata;
}

#ifdef REDIS_TEST
#include <stdio.h>
#include "testhelp.h"

#define TEST(name) printf("test — %s\n", name);

uint64_t hashTestCallback(const void *key) {
    return dictGenHashFunction((unsigned char*)key, strlen((char*)key));
}

void freeTestCallback(dict *d, void *val) {
    UNUSED(d);
    zfree(val);
}

void *defragAllocTest(void *ptr) {
    size_t size = zmalloc_usable_size(ptr);
    void *newptr = zmalloc(size);
    memcpy(newptr, ptr, size);
    zfree(ptr);
    return newptr;
}

dict *defragLUTTestCallback(dict *d) {
    /* handle the dict struct */
    d = defragAllocTest(d);
    /* handle the first hash table */
    d->ht_table[0] = defragAllocTest(d->ht_table[0]);
    /* handle the second hash table */
    if (d->ht_table[1])
        d->ht_table[1] = defragAllocTest(d->ht_table[1]);
    return d; 
}

dictType KvstoreDictTestType = {
    hashTestCallback,
    NULL,
    NULL,
    NULL,
    freeTestCallback,
    NULL,
    NULL
};

kvstoreType KvstoreTestType = {
    NULL, /* kvstore metadata size */
    NULL, /* dict metadata size */
    NULL, /* can free dict */
    NULL, /* on kvstore empty */
    NULL, /* on dict empty */
};

char *stringFromInt(int value) {
    char buf[32];
    int len;
    char *s;

    len = snprintf(buf, sizeof(buf), "%d",value);
    s = zmalloc(len+1);
    memcpy(s, buf, len);
    s[len] = '\0';
    return s;
}

/* ./redis-server test kvstore */
int kvstoreTest(int argc, char **argv, int flags) {
    UNUSED(argc);
    UNUSED(argv);
    UNUSED(flags);

    int i;
    void *key;
    dictEntry *de;
    kvstoreIterator kvs_it;
    kvstoreDictIterator kvs_di;

    /* Test also dictType with no_value=1 */
    dictType KvstoreDictNovalTestType = KvstoreDictTestType;
    KvstoreDictNovalTestType.no_value = 1;

    int didx = 0;
    int curr_slot = 0;
    kvstore *kvs1 = kvstoreCreate(&KvstoreTestType, &KvstoreDictTestType, 0, KVSTORE_ALLOCATE_DICTS_ON_DEMAND);
    kvstore *kvs2 = kvstoreCreate(&KvstoreTestType, &KvstoreDictNovalTestType, 0, KVSTORE_ALLOCATE_DICTS_ON_DEMAND | KVSTORE_FREE_EMPTY_DICTS);

    TEST("Add 16 keys") {
        for (i = 0; i < 16; i++) {
            de = kvstoreDictAddRaw(kvs1, didx, stringFromInt(i), NULL);
            assert(de != NULL);
            de = kvstoreDictAddRaw(kvs2, didx, stringFromInt(i), NULL);
            assert(de != NULL);
        }
        assert(kvstoreDictSize(kvs1, didx) == 16);
        assert(kvstoreSize(kvs1) == 16);
        assert(kvstoreDictSize(kvs2, didx) == 16);
        assert(kvstoreSize(kvs2) == 16);
    }

    TEST("kvstoreIterator creating and releasing without kvstoreIteratorNextDict()") {
        kvstore *kvs = kvstoreCreate(&KvstoreTestType, &KvstoreDictNovalTestType, 0, KVSTORE_ALLOCATE_DICTS_ON_DEMAND | KVSTORE_FREE_EMPTY_DICTS);
        kvstoreIterator kvs_iter;
        kvstoreIteratorInit(&kvs_iter, kvs);
        kvstoreIteratorReset(&kvs_iter);
        kvstoreRelease(kvs);
    }

    TEST("kvstoreIterator case 1: removing all keys does not delete the empty dict") {
        kvstoreIteratorInit(&kvs_it, kvs1);
        while((de = kvstoreIteratorNext(&kvs_it)) != NULL) {
            curr_slot = kvstoreIteratorGetCurrentDictIndex(&kvs_it);
            key = dictGetKey(de);
            assert(kvstoreDictDelete(kvs1, curr_slot, key) == DICT_OK);
        }
        kvstoreIteratorReset(&kvs_it);

        dict *d = kvstoreGetDict(kvs1, didx);
        assert(d != NULL);
        assert(kvstoreDictSize(kvs1, didx) == 0);
        assert(kvstoreSize(kvs1) == 0);
    }

    TEST("kvstoreIterator case 2: removing all keys will delete the empty dict") {
        kvstoreIteratorInit(&kvs_it, kvs2);
        while((de = kvstoreIteratorNext(&kvs_it)) != NULL) {
            curr_slot = kvstoreIteratorGetCurrentDictIndex(&kvs_it);
            key = dictGetKey(de);
            assert(kvstoreDictDelete(kvs2, curr_slot, key) == DICT_OK);
        }
        kvstoreIteratorReset(&kvs_it);

        /* Make sure the dict was removed from the rehashing list. */
        while (kvstoreIncrementallyRehash(kvs2, 1000)) {}

        dict *d = kvstoreGetDict(kvs2, didx);
        assert(d == NULL);
        assert(kvstoreDictSize(kvs2, didx) == 0);
        assert(kvstoreSize(kvs2) == 0);
    }

    TEST("Add 16 keys again") {
        for (i = 0; i < 16; i++) {
            de = kvstoreDictAddRaw(kvs1, didx, stringFromInt(i), NULL);
            assert(de != NULL);
            de = kvstoreDictAddRaw(kvs2, didx, stringFromInt(i), NULL);
            assert(de != NULL);
        }
        assert(kvstoreDictSize(kvs1, didx) == 16);
        assert(kvstoreSize(kvs1) == 16);
        assert(kvstoreDictSize(kvs2, didx) == 16);
        assert(kvstoreSize(kvs2) == 16);
    }

    TEST("kvstoreDictIterator case 1: removing all keys does not delete the empty dict") {
        kvstoreInitDictSafeIterator(&kvs_di, kvs1, didx);
        while((de = kvstoreDictIteratorNext(&kvs_di)) != NULL) {
            key = dictGetKey(de);
            assert(kvstoreDictDelete(kvs1, didx, key) == DICT_OK);
        }
        kvstoreResetDictIterator(&kvs_di);

        dict *d = kvstoreGetDict(kvs1, didx);
        assert(d != NULL);
        assert(kvstoreDictSize(kvs1, didx) == 0);
        assert(kvstoreSize(kvs1) == 0);
    }

    TEST("kvstoreDictIterator case 2: removing all keys will delete the empty dict") {
        kvstoreInitDictSafeIterator(&kvs_di, kvs2, didx);
        while((de = kvstoreDictIteratorNext(&kvs_di)) != NULL) {
            key = dictGetKey(de);
            assert(kvstoreDictDelete(kvs2, didx, key) == DICT_OK);
        }
        kvstoreResetDictIterator(&kvs_di);

        dict *d = kvstoreGetDict(kvs2, didx);
        assert(d == NULL);
        assert(kvstoreDictSize(kvs2, didx) == 0);
        assert(kvstoreSize(kvs2) == 0);
    }

    TEST("Verify that a rehashing dict's node in the rehashing list is correctly updated after defragmentation") {
        unsigned long cursor = 0;
        kvstore *kvs = kvstoreCreate(&KvstoreTestType, &KvstoreDictTestType, 0, KVSTORE_ALLOCATE_DICTS_ON_DEMAND);
        for (i = 0; i < 256; i++) {
            de = kvstoreDictAddRaw(kvs, 0, stringFromInt(i), NULL);
            if (listLength(kvs->rehashing)) break;
        }
        assert(listLength(kvs->rehashing));
        while ((cursor = kvstoreDictLUTDefrag(kvs, cursor, defragLUTTestCallback)) != 0) {}
        while (kvstoreIncrementallyRehash(kvs, 1000)) {}
        kvstoreRelease(kvs);
    }

    TEST("Verify non-empty dict count is correctly updated") {
        kvstore *kvs = kvstoreCreate(&KvstoreTestType, &KvstoreDictTestType, 2, 
                            KVSTORE_ALLOCATE_DICTS_ON_DEMAND);
        for (int idx = 0; idx < 4; idx++) {
            for (i = 0; i < 16; i++) {
                de = kvstoreDictAddRaw(kvs, idx, stringFromInt(i), NULL);
                assert(de != NULL);
                /* When the first element is inserted, the number of non-empty dictionaries is increased by 1. */
                if (i == 0) assert(kvstoreNumNonEmptyDicts(kvs) == idx + 1);
            }
        }

        /* Step by step, clear all dictionaries and ensure non-empty dict count is updated */
        for (int idx = 0; idx < 4; idx++) {
            kvstoreInitDictSafeIterator(&kvs_di, kvs, idx);
            while((de = kvstoreDictIteratorNext(&kvs_di)) != NULL) {
                key = dictGetKey(de);
                assert(kvstoreDictDelete(kvs, idx, key) == DICT_OK);
                /* When the dictionary is emptied, the number of non-empty dictionaries is reduced by 1. */
                if (kvstoreDictSize(kvs, idx) == 0) assert(kvstoreNumNonEmptyDicts(kvs) == 3 - idx);
            }
            kvstoreResetDictIterator(&kvs_di);
        }
        kvstoreRelease(kvs);
    }

    kvstoreRelease(kvs1);
    kvstoreRelease(kvs2);
    return 0;
}
#endif
