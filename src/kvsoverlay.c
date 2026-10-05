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

/* Snapshot overlay of a kvstore (hybrid BGSAVE): the per-dict save state and
 * the side table that keeps main-thread writes out of the buckets the scan
 * thread is reading. kvstore.c holds only the dispatch into this file (the
 * public wrappers, the iterators and the dict hooks). See kvstore.h, the
 * KVSTORE_INTERNAL section, for what the two files share. */

#define KVSTORE_INTERNAL
#include "fmacros.h"

#include <string.h>
#include <stddef.h>

#include "zmalloc.h"
#include "kvstore.h"
#include "fwtree.h"
#include "redisassert.h"

/* ==========================================================================
 * Per-dict save state
 * ========================================================================== */

long kvstoreSnapLeftovers(kvstore *kvs) { return kvs->snap_leftovers; }

void kvstoreSnapGuardSet(kvstore *kvs, const kvstoreSnapHooks *hooks, void *privdata, uint64_t gen) {
    kvs->snap_hooks = hooks;
    kvs->snap_priv = privdata;
    kvs->snap_gen = hooks ? gen : 0;
    if (!hooks && kvs->snap_free_deferred) {
        kvs->snap_free_deferred = 0;
        for (int didx = 0; didx < kvs->num_dicts; didx++) kvstoreFreeDictIfNeeded(kvs, didx);
    }
}

kvstoreDictSnap *kvstoreDictSnapAttach(kvstore *kvs, int didx) {
    dict *d = kvs->dicts[didx];
    assert(d != NULL);   /* only an existing dict; it stays alive while the state is attached */
    kvstoreDictMetaBase *meta = (kvstoreDictMetaBase *)dictMetadata(d);
    assert(meta->snap == NULL);
    kvstoreDictSnap *sn = zcalloc(sizeof(*sn));
    kvs->snap_attached++;
    __atomic_store_n(&meta->snap, sn, __ATOMIC_RELEASE);
    return sn;
}

void kvstoreSnapBitmapAccount(kvstore *kvs, long delta) { kvs->snap_bitmap_bytes += delta; }
size_t kvstoreSnapBitmapBytes(kvstore *kvs) { return kvs->snap_bitmap_bytes; }

int kvstoreSnapAttached(kvstore *kvs) { return kvs->snap_attached; }

dict *kvstoreGetDictAcquire(kvstore *kvs, int didx) {
    return __atomic_load_n(&kvs->dicts[didx], __ATOMIC_ACQUIRE);
}

/* ==========================================================================
 * Side table
 * ========================================================================== */

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

static inline const void *ovStoredKey2Key(kvstore *kvs, const void *stored) {
    return kvs->dtype.keyFromStoredKey ? kvs->dtype.keyFromStoredKey(stored) : stored;
}

static inline int ovFrozenHas(kvstore *kvs, int didx, const void *key) {
    dict *d = kvs->dicts[didx];
    return d && dictFind(d, key) != NULL;
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

/* ---- Side-table chains ---- */

static inline ovSlot *ovSlotOf(kvstore *kvs, kvstoreDictOverlay *ov, const void *key) {
    if (!ov->slots) {
        size_t bytes = sizeof(ovSlot) * OV_SLOTS;
        ov->slots = kvs->dtype.nofork_alloc ? zcalloc_nofork(bytes) : zcalloc(bytes);
    }
    dict *d = kvs->dicts[ov->didx];
    return &ov->slots[dictGetHash(d, key) & OV_SLOT_MASK];
}

/* The link of 'key' in chain 'chain' of 'slot', or NULL. */
static dictEntryLink ovChainFind(kvstore *kvs, ovSlot *slot, int chain, const void *key) {
    dictCmpCache cache = {0};
    dictEntryLink link = &slot->heads[chain];
    while (*link) {
        const void *k = ovEntryKey(kvs, *link);
        if (k == key || (kvs->dtype.keyCompare ? kvs->dtype.keyCompare(&cache, key, k) : 0)) return link;
        link = dictEntryNextLink(*link);
    }
    return NULL;
}

/* The link of 'key' in any chain of its slot (chain id in *chain), or NULL.
 * No allocation: a missing array means an empty side table. */
static dictEntryLink ovSideFind(kvstore *kvs, kvstoreDictOverlay *ov, const void *key, int *chain) {
    if (!ov->slots) return NULL;
    ovSlot *slot = ovSlotOf(kvs, ov, key);
    for (int c = 0; c < OV_CHAINS; c++) {
        dictEntryLink link = ovChainFind(kvs, slot, c, key);
        if (link) {
            if (chain) *chain = c;
            return link;
        }
    }
    return NULL;
}

/* A new entry for stored key 'kv' at the head of chain 'chain' of its slot;
 * returns the link (the head). */
static dictEntryLink ovChainPush(kvstore *kvs, kvstoreDictOverlay *ov, int chain, void *kv) {
    ovSlot *slot = ovSlotOf(kvs, ov, ovStoredKey2Key(kvs, kv));
    dictEntryLink head = &slot->heads[chain];
    *head = dictEntryCreateDetached(kvs->dicts[ov->didx], kv, *head);
    return head;
}

/* Take the entry at 'link' out of its chain (not freed). */
static dictEntry *ovChainUnlink(dictEntryLink link) {
    dictEntry *de = *link;
    *link = dictGetNext(de);
    return de;
}

/* Side-table cursor (ovCursor, kvstore.h): every LIVE and SHARED entry once,
 * slot by slot, the next entry fetched ahead (safe-iterator contract). */
static void ovCursorAdvance(kvstoreDictOverlay *ov, ovCursor *c) {
    if (c->next) { c->next = dictGetNext(c->next); if (c->next) return; c->chain++; }
    for (; c->slot < OV_SLOTS; c->slot++, c->chain = OV_LIVE) {
        for (; c->chain <= OV_SHARED; c->chain++) {
            c->next = ov->slots[c->slot].heads[c->chain];
            if (c->next) return;
        }
    }
    c->next = NULL;
}

void ovCursorInit(kvstoreDictOverlay *ov, ovCursor *c) {
    c->slot = 0;
    c->chain = OV_LIVE;
    c->next = NULL;
    if (ov->slots) ovCursorAdvance(ov, c);
}

dictEntry *ovCursorNext(kvstoreDictOverlay *ov, ovCursor *c) {
    dictEntry *de = c->next;
    if (de) ovCursorAdvance(ov, c);
    return de;
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
    ov->h = *hooks;
    ov->privdata = privdata;
    sn->ov = ov;
}

int kvstoreOverlayIsFrozen(kvstore *kvs, int didx) {
    return ovPeek(kvs, didx) != NULL;
}

void kvstoreOverlayStats(kvstore *kvs, int didx, long *side_entries, long *adds, long *tombstones) {
    kvstoreDictOverlay *ov = ovPeek(kvs, didx);
    if (side_entries) *side_entries = ov ? ov->entries : 0;
    if (adds) *adds = ov ? ov->adds : 0;
    if (tombstones) *tombstones = ov ? ov->tombs : 0;
}

/* Ensure 'key' (present in the frozen dict, not tombstoned, not yet in the
 * side table) has a clone in the side table. Returns its link or NULL when
 * the key is not logically present. */
static dictEntryLink ovEnsureClone(kvstore *kvs, int didx, kvstoreDictOverlay *ov, const void *key) {
    int chain;
    dictEntryLink link = ovSideFind(kvs, ov, key, &chain);
    if (link) return OV_IS_TOMB(chain) ? NULL : link;
    dict *d = kvs->dicts[didx];
    dictEntry *fe = d ? dictFind(d, key) : NULL;
    if (!fe) return NULL;
    int shared = 0;
    void *clone = ov->h.clone(ov->privdata, didx, dictGetKey(fe), &shared);
    assert(clone != NULL);
    link = ovChainPush(kvs, ov, shared ? OV_SHARED : OV_LIVE, clone);
    ov->entries++;
    ovMarkDirty(kvs, ov);
    return link;
}

/* A new side-table entry for stored key 'kv' (an insert that may not go to
 * the frozen chain). A re-add of a tombstoned key takes the tombstone's
 * place, in the chain that remembers whether its original's value is gone. */
static dictEntryLink ovSideInsert(kvstore *kvs, int didx, kvstoreDictOverlay *ov, void *kv) {
    const void *key = ovStoredKey2Key(kvs, kv);
    int chain = OV_LIVE;
    int tchain;
    dictEntryLink tlink = ovSideFind(kvs, ov, key, &tchain);
    if (tlink) {
        assert(OV_IS_TOMB(tchain));   /* a present key is never inserted twice */
        dictEntryFreeDetached(kvs->dicts[didx], ovChainUnlink(tlink), 0);   /* borrowed key */
        ov->tombs--;
        if (tchain == OV_TOMB_SHARED) chain = OV_SHARED;
    } else if (!ovFrozenHas(kvs, didx, key)) {
        ov->adds++;
    }
    dictEntryLink link = ovChainPush(kvs, ov, chain, kv);
    ov->entries++;
    ovKeyCountAdd(kvs, didx, 1);
    ovMarkDirty(kvs, ov);
    return link;
}

/* The side-table entry at 'link' (chain 'chain') leaves: a key the frozen
 * dict holds gets a tombstone in its stead. The entry itself is unlinked and
 * returned for the caller to free (with or without its key). */
static dictEntry *ovSideRemove(kvstore *kvs, int didx, kvstoreDictOverlay *ov, dictEntryLink link, int chain, const void *key) {
    assert(!OV_IS_TOMB(chain));
    dict *d = kvs->dicts[didx];
    if (!key) key = ovEntryKey(kvs, *link);
    dictEntry *fe = d ? dictFind(d, key) : NULL;
    if (fe) {
        ovChainPush(kvs, ov, chain == OV_SHARED ? OV_TOMB_SHARED : OV_TOMB, dictGetKey(fe));
        ov->tombs++;
    } else {
        ov->adds--;
    }
    ov->entries--;
    ovKeyCountAdd(kvs, didx, -1);
    ovMarkDirty(kvs, ov);
    return ovChainUnlink(link);
}

/* Free a side-table entry the store no longer references, running the
 * store's key destructor against the real dict (per-dict accounting lands
 * where the adds were counted). */
static void ovEntryFreeWithKey(kvstore *kvs, int didx, dictEntry *de) {
    dict *d = kvs->dicts[didx];
    assert(d != NULL);
    dictEntryFreeDetached(d, de, kvs->dtype.keyDestructor != NULL);
}

/* Logical size of dict didx (frozen or not). */
unsigned long ovDictSize(kvstore *kvs, int didx) {
    dict *d = kvs->dicts[didx];
    unsigned long n = d ? dictSize(d) : 0;
    kvstoreDictOverlay *ov = ovPeek(kvs, didx);
    if (ov) n = n + ov->adds - ov->tombs;
    return n;
}

/* Map a frozen-dict entry to what the logical keyspace holds: the side-table
 * clone if any, NULL if tombstoned, or the entry itself. */
dictEntry *ovMapFrozenEntry(kvstore *kvs, kvstoreDictOverlay *ov, dictEntry *de) {
    int chain;
    dictEntryLink link = ovSideFind(kvs, ov, ovEntryKey(kvs, de), &chain);
    if (!link) return de;
    return OV_IS_TOMB(chain) ? NULL : *link;
}

/* Is this side-table entry an ADD (key absent from the frozen dict)? */
int ovSideEntryIsAdd(kvstore *kvs, int didx, dictEntry *te) {
    return !ovFrozenHas(kvs, didx, ovEntryKey(kvs, te));
}

/* Owner thread: apply one LIVE or SHARED entry to the real dict. A clone
 * replaces its original at the link, an add is inserted. The real dict's key
 * destructor is suppressed by the caller (originals are released through
 * free_orig, header-only when the value is shared with the clone). The
 * entry is unlinked from 'link' and its node freed; the real dict owns the
 * key now. */
static void ovApplySide(kvstore *kvs, int didx, kvstoreDictOverlay *ov, dict *d, dictEntryLink link, int chain) {
    dictEntry *te = *link;
    void *kv = dictGetKey(te);
    const void *key = ovEntryKey(kvs, te);
    dictEntryLink flink = dictFindLink(d, key, NULL);
    if (flink) {
        void *orig = dictGetKey(*flink);
        dictSetKeyAtLink(d, kv, &flink, 0);
        ov->h.free_orig(ov->privdata, didx, orig, chain == OV_SHARED);
    } else {
        dictSetKeyAtLink(d, kv, NULL, 1);   /* no expand: auto-resize is paused */
        ov->adds--;
    }
    ov->entries--;
    dictEntryFreeDetached(d, ovChainUnlink(link), 0);
}

/* Owner thread: a tombstoned original (no re-add) leaves the real dict. */
static void ovApplyTomb(kvstore *kvs, int didx, kvstoreDictOverlay *ov, dict *d, dictEntryLink link, int chain) {
    const void *key = ovEntryKey(kvs, *link);
    dictEntry *fe = dictUnlink(d, key);
    assert(fe != NULL);
    void *orig = dictGetKey(fe);
    dictFreeUnlinkedEntry(d, fe);            /* destructor suppressed: frees only a chain node */
    ov->h.free_orig(ov->privdata, didx, orig, chain == OV_TOMB_SHARED);
    ov->tombs--;
    dictEntryFreeDetached(d, ovChainUnlink(link), 0);   /* the tombstone borrowed the original's pointer */
}

/* Owner thread: apply the side-table entries the snapshot no longer needs
 * kept out of the chain (the 'drainable' hook decides; every entry when
 * 'force'). Returns what is left. The logical counters were maintained while
 * the entries were created, so the dict is used directly here. */
static long ovDrainOne(kvstore *kvs, kvstoreDictOverlay *ov, int force) {
    int didx = ov->didx;
    dict *d = kvs->dicts[didx];
    assert(d != NULL);
    if (ov->slots && (ov->entries || ov->tombs)) {
        void (*saved_destructor)(dict *, void *) = kvs->dtype.keyDestructor;
        kvs->dtype.keyDestructor = NULL;
        for (int si = 0; si < OV_SLOTS; si++) {
            ovSlot *slot = &ov->slots[si];
            for (int chain = 0; chain < OV_CHAINS; chain++) {
                dictEntryLink link = &slot->heads[chain];
                while (*link) {
                    const void *key = ovEntryKey(kvs, *link);
                    if (!force && !(ov->h.drainable && ov->h.drainable(ov->privdata, didx, d, key))) {
                        link = dictEntryNextLink(*link);
                        continue;
                    }
                    /* The applied entry leaves the chain: 'link' now holds its successor. */
                    if (OV_IS_TOMB(chain)) ovApplyTomb(kvs, didx, ov, d, link, chain);
                    else ovApplySide(kvs, didx, ov, d, link, chain);
                }
            }
        }
        kvs->dtype.keyDestructor = saved_destructor;
    }
    long left = ov->entries + ov->tombs;
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
        n += ov->entries + ov->tombs;
    }
    return n;
}

/* Owner thread, after the scan of this dict is complete: apply whatever is
 * left in the side table and drop it. */
static void ovRelease(kvstore *kvs, kvstoreDictOverlay *ov) {
    long left = ovDrainOne(kvs, ov, 1);
    assert(left == 0);
    ovMarkClean(kvs, ov);
    zfree(ov->slots);
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
    kvs->snap_attached--;
    ((kvstoreDictMetaBase *)dictMetadata(d))->snap = NULL;
    zfree(sn);
    /* The real dict may have become empty (all tombstones). */
    kvstoreFreeDictIfNeeded(kvs, didx);
}

/* ==========================================================================
 * Enumeration through the overlay
 * ========================================================================== */

void ovScanCb(void *p, const dictEntry *de, dictEntry **plink) {
    (void)plink;
    struct ovScanCtx *c = p;
    dictEntry *m = ovMapFrozenEntry(c->kvs, c->ov, (dictEntry *)de);
    if (m) c->fn(c->priv, m, NULL);
}

/* Random entry of a frozen dict: sample the real dict (mapping through the
 * overlay, retrying on tombstones) and fall back to the temp dict. */
dictEntry *ovRandomEntry(kvstore *kvs, int didx, kvstoreDictOverlay *ov, int fair) {
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
    if (ov->entries && ov->slots) {
        /* Clones and adds are both logically present: the first entry at or
         * after a random slot (uniform enough for a fallback). */
        unsigned long start = random() & OV_SLOT_MASK;
        for (unsigned long k = 0; k < OV_SLOTS; k++) {
            ovSlot *slot = &ov->slots[(start + k) & OV_SLOT_MASK];
            if (slot->heads[OV_LIVE]) return slot->heads[OV_LIVE];
            if (slot->heads[OV_SHARED]) return slot->heads[OV_SHARED];
        }
    }
    /* Only tombstoned frozen keys were sampled: scan for any live entry. */
    kvstoreDictIterator it;
    kvstoreInitDictIterator(&it, kvs, didx);
    dictEntry *de = kvstoreDictIteratorNext(&it);
    kvstoreResetDictIterator(&it);
    return de;
}

/* Some keys of a frozen dict: sample the real dict through the overlay, then
 * adds from a random slot onwards (clones were sampled through the originals). */
unsigned int ovGetSomeKeys(kvstore *kvs, int didx, kvstoreDictOverlay *ov, dictEntry **des, unsigned int count) {
    unsigned int n = 0;
    dict *d = kvs->dicts[didx];
    if (d && dictSize(d)) {
        unsigned int got = dictGetSomeKeys(d, des, count);
        for (unsigned int i = 0; i < got; i++) {
            dictEntry *m = ovMapFrozenEntry(kvs, ov, des[i]);
            if (m) des[n++] = m;
        }
    }
    if (n < count && ov->adds && ov->slots) {
        /* Adds from a random slot onwards (clones were sampled through the originals). */
        unsigned long start = random() & OV_SLOT_MASK;
        for (unsigned long k = 0; k < OV_SLOTS && n < count; k++) {
            ovSlot *slot = &ov->slots[(start + k) & OV_SLOT_MASK];
            for (int chain = OV_LIVE; chain <= OV_SHARED && n < count; chain++)
                for (dictEntry *te = slot->heads[chain]; te && n < count; te = dictGetNext(te))
                    if (ovSideEntryIsAdd(kvs, didx, te)) des[n++] = te;
        }
    }
    return n;
}

/* ==========================================================================
 * Writes through the overlay
 * ========================================================================== */

/* Overlay (frozen slot) halves of the hot kvstore entry points: out of line so
 * the common path stays as small as it was and keeps being inlined. */
dictEntry *ovDictFind(kvstore *kvs, int didx, kvstoreDictOverlay *ov, void *key) {
    /* Read: the logical keyspace, no cloning. */
    int chain;
    dictEntryLink link = ovSideFind(kvs, ov, key, &chain);
    if (link) return OV_IS_TOMB(chain) ? NULL : *link;
    dict *d = kvs->dicts[didx];
    return d ? dictFind(d, key) : NULL;
}

/* Write-intent lookup (see kvstoreDictFindLink in kvstore.c for the contract). */
dictEntryLink ovDictFindLink(kvstore *kvs, int didx, kvstoreDictOverlay *ov, void *key, dictEntryLink *bucket) {
    ovEnter(ov);
    /* Write intent. A key that already has a side entry (clone or add) is
     * served there; a tombstoned key's re-add must land in the side table:
     * no link, and the side table's chain head as the bucket. */
    int chain;
    dictEntryLink link = ovSideFind(kvs, ov, key, &chain);
    if (link) {
        if (!OV_IS_TOMB(chain)) return link;
        if (bucket) *bucket = &ovSlotOf(kvs, ov, key)->heads[OV_LIVE];
        return NULL;
    }
    /* Otherwise the snapshot decides: a bucket it already holds may be
     * written in place (the frozen link, or the frozen bucket for an insert),
     * a bucket it is reading right now is cloned on first touch (a miss
     * returns the side table's chain head so the insertion lands there). */
    if (ovPrepare(kvs, didx, ov, key) == KVSTORE_OV_INPLACE)
        return dictFindLink(kvs->dicts[didx], key, bucket);
    link = ovEnsureClone(kvs, didx, ov, key);
    if (link) return link;
    if (bucket) *bucket = &ovSlotOf(kvs, ov, key)->heads[OV_LIVE];
    return NULL;
}

void ovDictSetAtLink(kvstore *kvs, int didx, kvstoreDictOverlay *ov, void *kv, dictEntryLink *link, int newItem) {
    ovEnter(ov);
    dict *d = kvs->dicts[didx];
    const void *key = (kv && kvs->dtype.keyFromStoredKey) ? kvs->dtype.keyFromStoredKey(kv) : kv;
    if (!newItem) {
        /* Replacing the key of an existing entry: with a link it is a store
         * through the link (the dict only lends its type); without one, look
         * the key up in the side table first, then the frozen dict. */
        dictEntryLink l = link ? *link : NULL;
        if (!l) {
            int chain;
            l = ovSideFind(kvs, ov, key, &chain);
            if (l && OV_IS_TOMB(chain)) l = NULL;
            if (l && link) *link = l;
        }
        dictSetKeyAtLink(d, kv, l ? &l : link, 0);
        return;
    }
    /* New item: a bucket link tells the dict (frozen buckets come from an
     * INPLACE write lookup of this same section); without one decide now. */
    int in_frozen;
    if (link && *link) {
        in_frozen = ovBucketInDict(d, *link);
    } else {
        int chain;
        in_frozen = !ovSideFind(kvs, ov, key, &chain) && ovPrepare(kvs, didx, ov, key) == KVSTORE_OV_INPLACE;
    }
    if (in_frozen) {
        dictSetKeyAtLink(d, kv, link, 1);   /* no expand: auto-resize is paused */
        ovKeyCountAdd(kvs, didx, 1);
    } else {
        dictEntryLink l = ovSideInsert(kvs, didx, ov, kv);
        if (link) *link = l;   /* the inserted entry's link, as dictSetKeyAtLink reports it */
    }
}

dictEntry *ovDictAddRaw(kvstore *kvs, int didx, kvstoreDictOverlay *ov, void *key, dictEntry **existing) {
    ovEnter(ov);
    const void *lkey = ovStoredKey2Key(kvs, key);
    int chain;
    dictEntryLink link = ovSideFind(kvs, ov, lkey, &chain);
    if (link && !OV_IS_TOMB(chain)) { if (existing) *existing = *link; return NULL; }
    if (!link) {   /* not tombstoned: the frozen dict may hold it */
        if (ovPrepare(kvs, didx, ov, lkey) == KVSTORE_OV_INPLACE) {
            dictEntry *ret = dictAddRaw(kvs->dicts[didx], key, existing);
            if (ret) ovKeyCountAdd(kvs, didx, 1);
            return ret;
        }
        link = ovEnsureClone(kvs, didx, ov, lkey);
        if (link) { if (existing) *existing = *link; return NULL; }
    }
    return *ovSideInsert(kvs, didx, ov, key);
}

dictEntryLink ovDictTwoPhaseUnlinkFind(kvstore *kvs, int didx, kvstoreDictOverlay *ov, const void *key, int *table_index) {
    ovEnter(ov);
    ov->unlink_frozen = 0;
    /* A side entry: its link, and its chain as the opaque table index for
     * the second phase. */
    int chain;
    dictEntryLink link = ovSideFind(kvs, ov, key, &chain);
    if (!link) {
        if (ovPrepare(kvs, didx, ov, key) == KVSTORE_OV_INPLACE) {
            dictEntryLink l = dictTwoPhaseUnlinkFind(kvs->dicts[didx], key, table_index);
            if (l) ov->unlink_frozen = 1;
            return l;
        }
        link = ovEnsureClone(kvs, didx, ov, key);
        if (!link) return NULL;
        chain = OV_LIVE;
        for (int c = OV_LIVE; c <= OV_SHARED; c++)
            if (link == &ovSlotOf(kvs, ov, key)->heads[c]) chain = c;
    } else if (OV_IS_TOMB(chain)) {
        return NULL;
    }
    *table_index = chain;
    return link;
}

/* As above, with the key given explicitly: callers that already detached the
 * value from the entry (dbGenericDelete's async path stores NULL at the link
 * before unlinking) cannot have it read back from the entry. 'key' may be
 * NULL when the entry still holds it. */
void ovDictTwoPhaseUnlinkFreeKey(kvstore *kvs, int didx, kvstoreDictOverlay *ov, dictEntryLink link, int table_index, const void *key) {
    ovEnter(ov);
    if (ov->unlink_frozen) {
        /* Second phase of an in-place unlink (see ovDictTwoPhaseUnlinkFind). */
        ov->unlink_frozen = 0;
        dictTwoPhaseUnlinkFree(kvs->dicts[didx], link, table_index);
        ovKeyCountAdd(kvs, didx, -1);
        return;
    }
    if (link && *link) {
        /* Bookkeeping first: the key pointer dies with the side entry. */
        dictEntry *de = ovSideRemove(kvs, didx, ov, link, table_index, key);
        ovEntryFreeWithKey(kvs, didx, de);
    }
}

int ovDictDelete(kvstore *kvs, int didx, kvstoreDictOverlay *ov, const void *key) {
    ovEnter(ov);
    int chain;
    dictEntryLink link = ovSideFind(kvs, ov, key, &chain);
    if (!link) {
        if (ovPrepare(kvs, didx, ov, key) == KVSTORE_OV_INPLACE) {
            int ret = dictDelete(kvs->dicts[didx], key);
            if (ret == DICT_OK) ovKeyCountAdd(kvs, didx, -1);
            return ret;
        }
        link = ovEnsureClone(kvs, didx, ov, key);
        if (!link) return DICT_ERR;
        chain = OV_LIVE;
        for (int c = OV_LIVE; c <= OV_SHARED; c++)
            if (link == &ovSlotOf(kvs, ov, key)->heads[c]) chain = c;
    } else if (OV_IS_TOMB(chain)) {
        return DICT_ERR;
    }
    dictEntry *de = ovSideRemove(kvs, didx, ov, link, chain, key);
    ovEntryFreeWithKey(kvs, didx, de);
    return DICT_OK;
}
