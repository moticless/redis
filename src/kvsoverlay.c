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

/* Snapshot overlay of a kvstore (hybrid snapshot): the side table that keeps
 * main-thread writes out of the buckets the scan thread is reading, and the
 * overlay halves of the kvstore entry points. kvstore.c holds only the
 * dispatch into this file (the public wrappers, the iterators and the dict
 * hooks); kvsoverlay.h is what the two files share. The save's own per-dict
 * state (bitmaps, pause) lives in the dict's extension slot and is
 * snapshot.c's. */

/* Opt in to the private overlay header. */
#define INCLUDE_KVSOVERLAY_H

#include "fmacros.h"

#include <string.h>
#include <stddef.h>

#include "zmalloc.h"
#include "kvsoverlay.h"
#include "redisassert.h"

/* ==========================================================================
 * Snapshot overlay
 * ========================================================================== */

/* The side table of a frozen dict (public API: kvstoreOverlay* in kvstore.h; kvstore.c sees only the forward declaration): OV_SLOTS chains of detached
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

int ovActive(kvstoreDictOverlay *ov) {
    return ov && (ov->entries || ov->tombs);
}

/* ==========================================================================
 * Side table
 * ========================================================================== */

/* The store's list of side tables to drain, kept in the store's opaque
 * overlay slot (created on the first dirty side table, released with the
 * store). A detached overlay (ov->kvs NULL) is on no list: nothing reaches it
 * until the save releases it. */
static list *ovDirtyList(kvstore *kvs, int create) {
    list **slot = (list **)kvstoreOverlaySlot(kvs);
    if (!*slot && create) *slot = listCreate();
    return *slot;
}

void ovStoreRelease(kvstore *kvs) {
    list **slot = (list **)kvstoreOverlaySlot(kvs);
    if (*slot) listRelease(*slot);
    *slot = NULL;
}

static void ovMarkDirty(kvstore *kvs, kvstoreDictOverlay *ov) {
    if (ov->dirty_node || !kvs) return;
    list *dirty = ovDirtyList(kvs, 1);
    listAddNodeTail(dirty, ov);
    ov->dirty_node = listLast(dirty);
}

static void ovMarkClean(kvstore *kvs, kvstoreDictOverlay *ov) {
    if (!ov->dirty_node) return;
    listDelNode(ovDirtyList(kvs, 0), ov->dirty_node);
    ov->dirty_node = NULL;
}

/* The key functions are the dict type's, which the frozen dict carries (the
 * store's type; the drain's quiet copy keeps them). */
static inline const dictType *ovType(kvstoreDictOverlay *ov) { return ov->d->type; }

/* Lookup key (sds) of a stored entry of a keyspace dict. */
static inline const void *ovEntryKey(kvstoreDictOverlay *ov, dictEntry *de) {
    void *stored = dictGetKey(de);
    return ovType(ov)->keyFromStoredKey ? ovType(ov)->keyFromStoredKey(stored) : stored;
}

static inline const void *ovStoredKey2Key(kvstoreDictOverlay *ov, const void *stored) {
    return ovType(ov)->keyFromStoredKey ? ovType(ov)->keyFromStoredKey(stored) : stored;
}

static inline int ovFrozenHas(kvstoreDictOverlay *ov, const void *key) {
    return dictFind(ov->d, key) != NULL;
}

/* Logical key-count bookkeeping for a frozen dict: the real dict size does not
 * change, so derive the empty/non-empty transition from the logical size. */
static void ovKeyCountAdd(kvstore *kvs, int didx, long delta) {
    kvstoreKeyCountAdd(kvs, didx, delta, ovDictSize(kvs, didx));
}

/* ---- Side-table chains ---- */

static inline ovSlot *ovSlotOf(kvstoreDictOverlay *ov, const void *key) {
    if (!ov->slots) {
        size_t bytes = sizeof(ovSlot) * OV_SLOTS;
        ov->slots = ovType(ov)->nofork_alloc ? zcalloc_nofork(bytes) : zcalloc(bytes);
    }
    return &ov->slots[dictGetHash(ov->d, key) & OV_SLOT_MASK];
}

/* The link of 'key' in chain 'chain' of 'slot', or NULL. */
static dictEntryLink ovChainFind(kvstoreDictOverlay *ov, ovSlot *slot, int chain, const void *key) {
    dictCmpCache cache = {0};
    dictEntryLink link = &slot->heads[chain];
    while (*link) {
        const void *k = ovEntryKey(ov, *link);
        if (k == key || (ovType(ov)->keyCompare ? ovType(ov)->keyCompare(&cache, key, k) : 0)) return link;
        link = dictEntryNextLink(*link);
    }
    return NULL;
}

/* The link of 'key' in any chain of its slot (chain id in *chain), or NULL.
 * No allocation: a missing array means an empty side table. */
static dictEntryLink ovSideFind(kvstoreDictOverlay *ov, const void *key, int *chain) {
    if (!ov->slots) return NULL;
    ovSlot *slot = ovSlotOf(ov, key);
    for (int c = 0; c < OV_CHAINS; c++) {
        dictEntryLink link = ovChainFind(ov, slot, c, key);
        if (link) {
            if (chain) *chain = c;
            return link;
        }
    }
    return NULL;
}

/* A new entry for stored key 'kv' at the head of chain 'chain' of its slot;
 * returns the link (the head). */
static dictEntryLink ovChainPush(kvstoreDictOverlay *ov, int chain, void *kv) {
    ovSlot *slot = ovSlotOf(ov, ovStoredKey2Key(ov, kv));
    dictEntryLink head = &slot->heads[chain];
    *head = dictEntryCreateDetached(ov->d, kv, *head);
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
    if (ov->h->enter) ov->h->enter(ov->privdata);
}

/* May the frozen chain of 'key' be written in place? Must be called after
 * ovEnter() (the answer is only stable inside a write section). Without a
 * prepare hook the overlay is pure clone-on-write. */
static inline int ovPrepare(kvstore *kvs, int didx, kvstoreDictOverlay *ov, const void *key) {
    (void)kvs;
    if (!ov->h->prepare) return KVSTORE_OV_DEFER;
    return ov->h->prepare(ov->privdata, didx, ov->d, key);
}

/* Is 'bucket' (a link handed out as a bucket) one of dict d's own buckets? */
static int ovBucketInDict(dict *d, dictEntryLink bucket) {
    for (int t = 0; t < 2; t++) {
        unsigned long size = DICTHT_SIZE(d->ht_size_exp[t]);
        if (size && bucket >= &d->ht_table[t][0] && bucket < &d->ht_table[t][size]) return 1;
    }
    return 0;
}

void kvstoreOverlayAttach(kvstore *kvs, int didx)
{
    dict *d = kvstoreGetDict(kvs, didx);
    assert(d != NULL);   /* only an existing dict can be frozen; it stays alive while frozen */
    kvstoreDictMetaBase *meta = (kvstoreDictMetaBase *)dictMetadata(d);
    assert(meta->ov == NULL);
    kvstoreDictOverlay *ov = zcalloc(sizeof(*ov));
    ov->kvs = kvs;
    ov->didx = didx;
    ov->d = d;
    ov->h = kvstoreSnapHooksGet(kvs, &ov->privdata);
    assert(ov->h != NULL);
    __atomic_store_n(&meta->ov, ov, __ATOMIC_RELEASE);
}

/* The overlay of slot didx, if its dict is frozen (cold; kvstore.c has the
 * inline hot-path form). */
static inline kvstoreDictOverlay *ovOf(kvstore *kvs, int didx) {
    dict *d = kvstoreGetDict(kvs, didx);
    return d ? ((kvstoreDictMetaBase *)dictMetadata(d))->ov : NULL;
}

int kvstoreOverlayIsFrozen(kvstore *kvs, int didx) {
    return ovOf(kvs, didx) != NULL;
}

/* Ensure 'key' (present in the frozen dict, not tombstoned, not yet in the
 * side table) has a clone in the side table. Returns its link or NULL when
 * the key is not logically present. */
static dictEntryLink ovEnsureClone(kvstore *kvs, int didx, kvstoreDictOverlay *ov, const void *key) {
    int chain;
    dictEntryLink link = ovSideFind(ov, key, &chain);
    if (link) return OV_IS_TOMB(chain) ? NULL : link;
    dictEntry *fe = dictFind(ov->d, key);
    if (!fe) return NULL;
    int shared = 0;
    void *clone = ov->h->clone(ov->privdata, didx, dictGetKey(fe), &shared);
    assert(clone != NULL);
    link = ovChainPush(ov, shared ? OV_SHARED : OV_LIVE, clone);
    ov->entries++;
    ovMarkDirty(kvs, ov);
    return link;
}

/* A new side-table entry for stored key 'kv' (an insert that may not go to
 * the frozen chain). A re-add of a tombstoned key takes the tombstone's
 * place, in the chain that remembers whether its original's value is gone. */
static dictEntryLink ovSideInsert(kvstore *kvs, int didx, kvstoreDictOverlay *ov, void *kv) {
    const void *key = ovStoredKey2Key(ov, kv);
    int chain = OV_LIVE;
    int tchain;
    dictEntryLink tlink = ovSideFind(ov, key, &tchain);
    if (tlink) {
        assert(OV_IS_TOMB(tchain));   /* a present key is never inserted twice */
        dictEntryFreeDetached(ov->d, ovChainUnlink(tlink), 0);   /* borrowed key */
        ov->tombs--;
        if (tchain == OV_TOMB_SHARED) chain = OV_SHARED;
    } else if (!ovFrozenHas(ov, key)) {
        ov->adds++;
    }
    dictEntryLink link = ovChainPush(ov, chain, kv);
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
    if (!key) key = ovEntryKey(ov, *link);
    dictEntry *fe = dictFind(ov->d, key);
    if (fe) {
        ovChainPush(ov, chain == OV_SHARED ? OV_TOMB_SHARED : OV_TOMB, dictGetKey(fe));
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
static void ovEntryFreeWithKey(kvstoreDictOverlay *ov, dictEntry *de) {
    dictEntryFreeDetached(ov->d, de, ov->d->type->keyDestructor != NULL);
}

/* Logical size of dict didx (frozen or not). */
unsigned long ovDictSize(kvstore *kvs, int didx) {
    dict *d = kvstoreGetDict(kvs, didx);
    unsigned long n = d ? dictSize(d) : 0;
    kvstoreDictOverlay *ov = ovOf(kvs, didx);
    if (ov) n = n + ov->adds - ov->tombs;
    return n;
}

/* Map a frozen-dict entry to what the logical keyspace holds: the side-table
 * clone if any, NULL if tombstoned, or the entry itself. */
dictEntry *ovMapFrozenEntry(kvstoreDictOverlay *ov, dictEntry *de) {
    int chain;
    dictEntryLink link = ovSideFind(ov, ovEntryKey(ov, de), &chain);
    if (!link) return de;
    return OV_IS_TOMB(chain) ? NULL : *link;
}

/* Is this side-table entry an ADD (key absent from the frozen dict)? */
int ovSideEntryIsAdd(kvstoreDictOverlay *ov, dictEntry *te) {
    return !ovFrozenHas(ov, ovEntryKey(ov, te));
}

/* Owner thread: apply one LIVE or SHARED entry to the real dict. A clone
 * replaces its original at the link, an add is inserted. The real dict's key
 * destructor is suppressed by the caller (originals are released through
 * free_orig, header-only when the value is shared with the clone). The
 * entry is unlinked from 'link' and its node freed; the real dict owns the
 * key now. */
static void ovApplySide(int didx, kvstoreDictOverlay *ov, dict *d, dictEntryLink link, int chain) {
    dictEntry *te = *link;
    void *kv = dictGetKey(te);
    const void *key = ovEntryKey(ov, te);
    dictEntryLink flink = dictFindLink(d, key, NULL);
    if (flink) {
        void *orig = dictGetKey(*flink);
        dictSetKeyAtLink(d, kv, &flink, 0);
        ov->h->free_orig(ov->privdata, didx, orig, chain == OV_SHARED);
    } else {
        dictSetKeyAtLink(d, kv, NULL, 1);   /* no expand: auto-resize is paused */
        ov->adds--;
    }
    ov->entries--;
    dictEntryFreeDetached(d, ovChainUnlink(link), 0);
}

/* Owner thread: a tombstoned original (no re-add) leaves the real dict. */
static void ovApplyTomb(int didx, kvstoreDictOverlay *ov, dict *d, dictEntryLink link, int chain) {
    const void *key = ovEntryKey(ov, *link);
    dictEntry *fe = dictUnlink(d, key);
    assert(fe != NULL);
    void *orig = dictGetKey(fe);
    dictFreeUnlinkedEntry(d, fe);            /* destructor suppressed: frees only a chain node */
    ov->h->free_orig(ov->privdata, didx, orig, chain == OV_TOMB_SHARED);
    ov->tombs--;
    dictEntryFreeDetached(d, ovChainUnlink(link), 0);   /* the tombstone borrowed the original's pointer */
}

/* Owner thread: apply the side-table entries the snapshot no longer needs
 * kept out of the chain (the 'drainable' hook decides; every entry when
 * 'force'). Returns what is left. The logical counters were maintained while
 * the entries were created, so the dict is used directly here. */
static long ovDrainOne(kvstore *kvs, kvstoreDictOverlay *ov, int force) {
    int didx = ov->didx;
    dict *d = ov->d;
    if (ov->slots && (ov->entries || ov->tombs)) {
        /* Originals are released through free_orig (header-only when the
         * clone owns the value): the dict's destructor is kept out of the
         * way through a private copy of its type, not by editing the store's
         * type, which its other dicts share (and the lazyfree thread may be
         * releasing them right now). */
        dictType *saved_type = d->type;
        dictType quiet = *saved_type;
        quiet.keyDestructor = NULL;
        d->type = &quiet;
        for (int si = 0; si < OV_SLOTS; si++) {
            ovSlot *slot = &ov->slots[si];
            for (int chain = 0; chain < OV_CHAINS; chain++) {
                dictEntryLink link = &slot->heads[chain];
                while (*link) {
                    const void *key = ovEntryKey(ov, *link);
                    if (!force && !(ov->h->drainable && ov->h->drainable(ov->privdata, didx, d, key))) {
                        link = dictEntryNextLink(*link);
                        continue;
                    }
                    /* The applied entry leaves the chain: 'link' now holds its successor. */
                    if (OV_IS_TOMB(chain)) ovApplyTomb(didx, ov, d, link, chain);
                    else ovApplySide(didx, ov, d, link, chain);
                }
            }
        }
        d->type = saved_type;
    }
    long left = ov->entries + ov->tombs;
    if (left == 0 && kvs) ovMarkClean(kvs, ov);
    return left;
}

/* The dict leaves its store (kvstore.c detachDict: freed while a save holds
 * it, emptied, moved): nothing reaches its side table through the store any
 * more. Off the drain list; the overlay only waits for the save's release. */
void ovDetachFromStore(dict *d) {
    kvstoreDictOverlay *ov = ((kvstoreDictMetaBase *)dictMetadata(d))->ov;
    if (!ov) return;
    if (ov->kvs) ovMarkClean(ov->kvs, ov);
    ov->kvs = NULL;
}

long kvstoreOverlayDrain(kvstore *kvs) {
    list *dirty = ovDirtyList(kvs, 0);
    if (!dirty) return 0;
    long left = 0;
    listNode *ln = listFirst(dirty);
    while (ln) {
        listNode *next = listNextNode(ln);   /* ovDrainOne may unlink ln */
        left += ovDrainOne(kvs, listNodeValue(ln), 0);
        ln = next;
    }
    return left;
}

long kvstoreOverlayPending(kvstore *kvs) {
    list *dirty = ovDirtyList(kvs, 0);
    if (!dirty) return 0;
    long n = 0;
    listIter li;
    listNode *ln;
    listRewind(dirty, &li);
    while ((ln = listNext(&li)) != NULL) {
        kvstoreDictOverlay *ov = listNodeValue(ln);
        n += ov->entries + ov->tombs;
    }
    return n;
}

/* Owner thread, after the scan of this dict is complete: apply whatever is
 * left in the side table and drop it. */
static void ovRelease(kvstoreDictOverlay *ov) {
    long left = ovDrainOne(ov->kvs, ov, 1);   /* applied to the dict itself; its holder decides what becomes of it */
    assert(left == 0);
    if (ov->kvs) ovMarkClean(ov->kvs, ov);
    zfree(ov->slots);
    zfree(ov);
}

/* Owner thread: the save is finished with the dict (its scan completed or
 * the save was aborted) and has released its own state: the side table is
 * applied and the overlay goes. The dict may have left its store meanwhile
 * (detached, or moved to another store): the overlay follows the dict, so
 * this works wherever it is. */
void kvstoreOverlayRelease(dict *d) {
    kvstoreDictMetaBase *meta = (kvstoreDictMetaBase *)dictMetadata(d);
    kvstoreDictOverlay *ov = meta->ov;
    if (!ov) return;
    ovRelease(ov);
    __atomic_store_n(&meta->ov, NULL, __ATOMIC_RELEASE);
    /* The real dict may have become empty (all tombstones): its store, if it
     * still holds it, may drop it now (the save's reference keeps the dict
     * alive until the caller lets go). */
    kvstore *kvs = d->type->userdata;
    if (!meta->detached && kvstoreGetDict(kvs, meta->didx) == d) kvstoreFreeDictIfNeeded(kvs, meta->didx);
}

/* ==========================================================================
 * Enumeration through the overlay
 * ========================================================================== */

struct ovScanCtx { kvstore *kvs; kvstoreDictOverlay *ov; dictScanFunction *fn; void *priv; };
static void ovScanCb(void *p, const dictEntry *de, dictEntry **plink) {
    (void)plink;
    struct ovScanCtx *c = p;
    dictEntry *m = ovMapFrozenEntry(c->ov, (dictEntry *)de);
    if (m) c->fn(c->priv, m, NULL);
}

/* kvstoreScan over a frozen dict with a non-empty side table: the frozen
 * dict through the overlay (tombstones skipped, clones substituted), then
 * the ADDs with the final batch. Returns the dict cursor. */
unsigned long long ovScan(kvstore *kvs, kvstoreDictOverlay *ov, dict *d, unsigned long long cursor,
                          dictScanFunction *scan_cb, void *privdata)
{
    struct ovScanCtx sctx = { kvs, ov, scan_cb, privdata };
    unsigned long long next = (d && dictSize(d)) ? dictScan(d, cursor, ovScanCb, &sctx) : 0;
    if (next == 0 && ov->adds) {
        ovCursor c;
        dictEntry *te;
        ovCursorInit(ov, &c);
        while ((te = ovCursorNext(ov, &c)) != NULL)
            if (ovSideEntryIsAdd(ov, te)) scan_cb(privdata, te, NULL);
    }
    return next;
}

/* One step of kvstoreIteratorNext / kvstoreDictIteratorNext over a frozen
 * dict with a non-empty side table: 'di' iterates the frozen dict (phase 0,
 * tombstoned entries skipped, clones handed out in place of their
 * originals), then 'cur' the side table (phase 1, ADDs only). NULL once
 * both are exhausted. */
dictEntry *ovIteratorNext(kvstoreDictOverlay *ov, dictIterator *di, int *phase, ovCursor *cur) {
    for (;;) {
        dictEntry *de;
        if (*phase == 1) de = ovCursorNext(ov, cur);
        else de = di->d ? dictNext(di) : NULL;
        if (!de) {
            if (*phase == 0 && di->d) {
                ovCursorInit(ov, cur);
                *phase = 1;
                continue;
            }
            return NULL;
        }
        if (*phase == 0) {
            dictEntry *m = ovMapFrozenEntry(ov, de);
            if (m) return m;
            continue;   /* tombstoned */
        }
        if (ovSideEntryIsAdd(ov, de)) return de;   /* a clone was handed out in phase 0 */
    }
}

/* Random entry of a frozen dict: sample the real dict (mapping through the
 * overlay, retrying on tombstones) and fall back to the side table's adds. */
dictEntry *ovRandomEntry(kvstore *kvs, int didx, kvstoreDictOverlay *ov, int fair) {
    dict *d = ov->d;
    if (ovDictSize(kvs, didx) == 0) return NULL;
    if (d && dictSize(d)) {
        for (int tries = 0; tries < 16; tries++) {
            dictEntry *de = fair ? dictGetFairRandomKey(d) : dictGetRandomKey(d);
            if (!de) break;
            dictEntry *m = ovMapFrozenEntry(ov, de);
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
unsigned int ovGetSomeKeys(kvstoreDictOverlay *ov, dictEntry **des, unsigned int count) {
    unsigned int n = 0;
    dict *d = ov->d;
    if (d && dictSize(d)) {
        unsigned int got = dictGetSomeKeys(d, des, count);
        for (unsigned int i = 0; i < got; i++) {
            dictEntry *m = ovMapFrozenEntry(ov, des[i]);
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
                    if (ovSideEntryIsAdd(ov, te)) des[n++] = te;
        }
    }
    return n;
}

/* ==========================================================================
 * Writes through the overlay
 * ========================================================================== */

/* Overlay (frozen slot) halves of the hot kvstore entry points: out of line so
 * the common path stays as small as it was and keeps being inlined. */
dictEntry *ovDictFind(kvstoreDictOverlay *ov, void *key) {
    /* Read: the logical keyspace, no cloning. */
    int chain;
    dictEntryLink link = ovSideFind(ov, key, &chain);
    if (link) return OV_IS_TOMB(chain) ? NULL : *link;
    return dictFind(ov->d, key);
}

/* Write-intent lookup (see kvstoreDictFindLink in kvstore.c for the contract). */
dictEntryLink ovDictFindLink(kvstore *kvs, int didx, kvstoreDictOverlay *ov, void *key, dictEntryLink *bucket) {
    ovEnter(ov);
    /* Write intent. A key that already has a side entry (clone or add) is
     * served there; a tombstoned key's re-add must land in the side table:
     * no link, and the side table's chain head as the bucket. */
    int chain;
    dictEntryLink link = ovSideFind(ov, key, &chain);
    if (link) {
        if (!OV_IS_TOMB(chain)) return link;
        if (bucket) *bucket = &ovSlotOf(ov, key)->heads[OV_LIVE];
        return NULL;
    }
    /* Otherwise the snapshot decides: a bucket it already holds may be
     * written in place (the frozen link, or the frozen bucket for an insert),
     * a bucket it is reading right now is cloned on first touch (a miss
     * returns the side table's chain head so the insertion lands there). */
    if (ovPrepare(kvs, didx, ov, key) == KVSTORE_OV_INPLACE)
        return dictFindLink(ov->d, key, bucket);
    link = ovEnsureClone(kvs, didx, ov, key);
    if (link) return link;
    if (bucket) *bucket = &ovSlotOf(ov, key)->heads[OV_LIVE];
    return NULL;
}

void ovDictSetAtLink(kvstore *kvs, int didx, kvstoreDictOverlay *ov, void *kv, dictEntryLink *link, int newItem) {
    ovEnter(ov);
    dict *d = ov->d;
    const void *key = kv ? ovStoredKey2Key(ov, kv) : kv;
    if (!newItem) {
        /* Replacing the key of an existing entry: with a link it is a store
         * through the link (the dict only lends its type); without one, look
         * the key up in the side table first, then the frozen dict. */
        dictEntryLink l = link ? *link : NULL;
        if (!l) {
            int chain;
            l = ovSideFind(ov, key, &chain);
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
        in_frozen = !ovSideFind(ov, key, &chain) && ovPrepare(kvs, didx, ov, key) == KVSTORE_OV_INPLACE;
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
    const void *lkey = ovStoredKey2Key(ov, key);
    int chain;
    dictEntryLink link = ovSideFind(ov, lkey, &chain);
    if (link && !OV_IS_TOMB(chain)) { if (existing) *existing = *link; return NULL; }
    if (!link) {   /* not tombstoned: the frozen dict may hold it */
        if (ovPrepare(kvs, didx, ov, lkey) == KVSTORE_OV_INPLACE) {
            dictEntry *ret = dictAddRaw(ov->d, key, existing);
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
    dictEntryLink link = ovSideFind(ov, key, &chain);
    if (!link) {
        if (ovPrepare(kvs, didx, ov, key) == KVSTORE_OV_INPLACE) {
            dictEntryLink l = dictTwoPhaseUnlinkFind(ov->d, key, table_index);
            if (l) ov->unlink_frozen = 1;
            return l;
        }
        link = ovEnsureClone(kvs, didx, ov, key);
        if (!link) return NULL;
        chain = OV_LIVE;
        for (int c = OV_LIVE; c <= OV_SHARED; c++)
            if (link == &ovSlotOf(ov, key)->heads[c]) chain = c;
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
        dictTwoPhaseUnlinkFree(ov->d, link, table_index);
        ovKeyCountAdd(kvs, didx, -1);
        return;
    }
    if (link && *link) {
        /* Bookkeeping first: the key pointer dies with the side entry. */
        dictEntry *de = ovSideRemove(kvs, didx, ov, link, table_index, key);
        ovEntryFreeWithKey(ov, de);
    }
}

int ovDictDelete(kvstore *kvs, int didx, kvstoreDictOverlay *ov, const void *key) {
    ovEnter(ov);
    int chain;
    dictEntryLink link = ovSideFind(ov, key, &chain);
    if (!link) {
        if (ovPrepare(kvs, didx, ov, key) == KVSTORE_OV_INPLACE) {
            int ret = dictDelete(ov->d, key);
            if (ret == DICT_OK) ovKeyCountAdd(kvs, didx, -1);
            return ret;
        }
        link = ovEnsureClone(kvs, didx, ov, key);
        if (!link) return DICT_ERR;
        chain = OV_LIVE;
        for (int c = OV_LIVE; c <= OV_SHARED; c++)
            if (link == &ovSlotOf(ov, key)->heads[c]) chain = c;
    } else if (OV_IS_TOMB(chain)) {
        return DICT_ERR;
    }
    dictEntry *de = ovSideRemove(kvs, didx, ov, link, chain, key);
    ovEntryFreeWithKey(ov, de);
    return DICT_OK;
}
