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

/* Snapshot overlay of a kvstore (hybrid snapshot): the contract and the design
 * of this file (who includes what, the overlayed buckets and their index, the
 * lifecycle) are the head of kvstore_overlay.h. */

/* Opt in to the overlay header's INTERNALS. */
#define INCLUDE_KVSTORE_OVERLAY_H

#include "fmacros.h"

#include <string.h>
#include <stddef.h>
#include <time.h>

#include "zmalloc.h"
#include "kvstore_overlay.h"
#include "redisassert.h"

/* ==========================================================================
 * Snapshot overlay
 * ========================================================================== */

/* The overlay of a frozen dict, its overlayed buckets and the one index of
 * them by scan position (kvstore_overlay.h, THE OVERLAYED BUCKETS). kvstore.c
 * sees only the forward declaration. Only the main thread touches any of this. */

/* The overlay's work counters (main thread only), see kvstoreOverlayStats.
 * A pause still open is added at read time from its count and the sum of its
 * start times. */
static struct {
    kvstoreOverlayStats s;
    long cur;                   /* overlayed buckets now */
    int open;                   /* dicts whose pause is open */
    long long open_since_sum;   /* sum of their start times, us */
} kvso_stats;

static long long kvsoNowUs(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (long long)ts.tv_sec * 1000000 + ts.tv_nsec / 1000;
}

void kvstoreOverlayStatsReset(void) {
    long keep = kvso_stats.cur;
    memset(&kvso_stats.s, 0, sizeof(kvso_stats.s));
    kvso_stats.s.peak = keep;
}

kvstoreOverlayStats kvstoreOverlayStatsGet(void) {
    kvstoreOverlayStats r = kvso_stats.s;
    if (kvso_stats.open) r.paused_us += (long long)kvso_stats.open * kvsoNowUs() - kvso_stats.open_since_sum;
    return r;
}

typedef struct kvsoBucket {
    struct kvstoreDictOverlay *ov;   /* the dict's overlay */
    struct kvsoBucket *hnext;   /* the index: the next bucket of the same slot (a collision) */
    struct kvsoBucket *onext, *oprev;   /* the overlay's buckets */
    uint64_t pos;               /* its scan position: written back once the scan is past it */
    unsigned long slot;         /* its index slot */
    int table;
    unsigned long idx;
    dictEntry *chain;           /* the bucket's logical chain: detached entries holding the clones and the adds */
    dictEntry *orig_head;       /* the dict's chain, untouched until the write-back: the originals */
    int norig;                  /* entries of orig_head */
    long nkeys;                 /* entries of chain */
} kvsoBucket;

typedef struct kvstoreDictOverlay {
    kvstore *kvs;               /* the store the dict is in; NULL once the dict left it (detached: no more
                                   accesses through the store, the overlay only waits for the release) */
    int didx;                   /* the dict's slot */
    dict *d;                    /* the frozen dict itself (the store's dict of slot didx while owned) */
    int nb;                     /* overlayed buckets */
    long delta;                 /* logical size - dict size: the adds minus the deletes of the overlayed buckets */
    const kvstoreSnapHooks *h;  /* the store's hooks at attach time (kvstore_overlay.h kvstoreSnapHooks) */
    int paused;                 /* rehash and auto-resize are held while buckets are overlayed */
    long long paused_at;        /* when, us (kvso_stats) */
    int unlink_frozen;          /* the pending two-phase unlink targets the frozen dict */
    kvsoBucket *unlink_ob;        /* ... or this overlayed bucket */
    void *privdata;
    kvsoBucket *buckets;        /* its overlayed buckets, newest first (nb of them) */
} kvstoreDictOverlay;

/* The index of overlayed buckets, one for the save (kvstoreOverlayIndexInit):
 * a ring of slots keyed by the bucket's dense scan index (the dict's base,
 * kvstoreOverlayIndexBase, plus kvsoDenseOffset), each slot the chain of the
 * buckets that map to it. */
static struct {
    kvsoBucket **slot;          /* chains, by index & mask */
    uint64_t *bits;             /* the slots holding a bucket */
    unsigned long mask;         /* slots - 1 (a power of two); no index without one */
    long count;
} kvso_index;

int kvsoActive(kvstoreDictOverlay *ov) {
    return ov && ov->nb > 0;
}

/* The key functions are the dict type's, which the frozen dict carries. */
static inline const dictType *kvsoType(kvstoreDictOverlay *ov) { return ov->d->type; }

/* Lookup key (sds) of a stored entry of a keyspace dict. */
static inline const void *kvsoEntryKey(kvstoreDictOverlay *ov, dictEntry *de) {
    void *stored = dictGetKey(de);
    return kvsoType(ov)->keyFromStoredKey ? kvsoType(ov)->keyFromStoredKey(stored) : stored;
}

static inline const void *kvsoStoredKey2Key(kvstoreDictOverlay *ov, const void *stored) {
    return kvsoType(ov)->keyFromStoredKey ? kvsoType(ov)->keyFromStoredKey(stored) : stored;
}

/* Logical key-count bookkeeping for a frozen dict: the real dict size does not
 * change, so derive the empty/non-empty transition from the logical size. */
static void kvsoKeyCountAdd(kvstore *kvs, int didx, long delta) {
    kvstoreKeyCountAdd(kvs, didx, delta, kvsoDictSize(kvs, didx));
}

/* ---- The index of overlayed buckets ---- */

void kvstoreOverlayIndexInit(unsigned long slots) {
    assert(slots && (slots & (slots - 1)) == 0);
    kvstoreOverlayIndexFree();
    kvso_index.slot = zcalloc(slots * sizeof(*kvso_index.slot));
    kvso_index.bits = zcalloc((slots >> 6 ? slots >> 6 : 1) * sizeof(*kvso_index.bits));
    kvso_index.mask = slots - 1;
}

void kvstoreOverlayIndexFree(void) {
    assert(kvso_index.count == 0);   /* every overlay released its buckets */
    zfree(kvso_index.slot);
    zfree(kvso_index.bits);
    memset(&kvso_index, 0, sizeof(kvso_index));
}

/* The dict's buckets in scan order, from 0: table 0 from rehashidx (the
 * buckets below it were moved to table 1 before the freeze), then table 1.
 * Stable while the dict is paused: rehashidx and the sizes hold still, and a
 * table 1 the dict grows meanwhile (dictType.pausedExpandAllowed) starts
 * with rehashidx 0. */
static inline uint64_t kvsoDenseOffset(dict *d, int table, unsigned long idx) {
    unsigned long r = dictIsRehashing(d) ? (unsigned long)d->rehashidx : 0;
    return table == 0 ? (uint64_t)(idx - r) : (uint64_t)(DICTHT_SIZE(d->ht_size_exp[0]) - r) + idx;
}

uint64_t kvstoreOverlayIndexBase(dict *d, uint64_t base) {
    ((kvstoreDictMetaBase *)dictMetadata(d))->ov_index_base = base;
    /* One past the last bucket the scan reads: table 1's end when rehashing, else table 0's. */
    return dictIsRehashing(d) ? kvsoDenseOffset(d, 1, DICTHT_SIZE(d->ht_size_exp[1])) : kvsoDenseOffset(d, 0, DICTHT_SIZE(d->ht_size_exp[0]));
}

static inline unsigned long kvsoSlotOf(kvstoreDictOverlay *ov, int table, unsigned long idx) {
    uint64_t base = ((kvstoreDictMetaBase *)dictMetadata(ov->d))->ov_index_base;
    return (unsigned long)(base + kvsoDenseOffset(ov->d, table, idx)) & kvso_index.mask;
}

/* The overlayed bucket (table, idx) of the dict, or NULL: its slot, then the
 * bucket named so in the slot's chain (one, normally). */
static kvsoBucket *kvsoLookup(kvstoreDictOverlay *ov, int table, unsigned long idx) {
    if (!ov->nb) return NULL;
    for (kvsoBucket *b = kvso_index.slot[kvsoSlotOf(ov, table, idx)]; b; b = b->hnext)
        if (b->ov == ov && b->table == table && b->idx == idx) return b;
    return NULL;
}

static void kvsoIndexAdd(kvsoBucket *ob) {
    assert(kvso_index.slot != NULL);
    unsigned long s = kvsoSlotOf(ob->ov, ob->table, ob->idx);
    ob->slot = s;
    ob->hnext = kvso_index.slot[s];
    kvso_index.slot[s] = ob;
    kvso_index.bits[s >> 6] |= 1ULL << (s & 63);
    kvso_index.count++;
}

static void kvsoIndexRemove(kvsoBucket *ob) {
    kvsoBucket **link = &kvso_index.slot[ob->slot];
    while (*link != ob) link = &(*link)->hnext;
    *link = ob->hnext;
    if (!kvso_index.slot[ob->slot]) kvso_index.bits[ob->slot >> 6] &= ~(1ULL << (ob->slot & 63));
    kvso_index.count--;
}

/* ---- The overlay's buckets ---- */

static void kvsoBucketsAdd(kvstoreDictOverlay *ov, kvsoBucket *ob) {
    ob->oprev = NULL;
    ob->onext = ov->buckets;
    if (ov->buckets) ov->buckets->oprev = ob;
    ov->buckets = ob;
    ov->nb++;
}

static void kvsoBucketsRemove(kvstoreDictOverlay *ov, kvsoBucket *ob) {
    if (ob->oprev) ob->oprev->onext = ob->onext;
    else ov->buckets = ob->onext;
    if (ob->onext) ob->onext->oprev = ob->oprev;
    ov->nb--;
}

/* The k-th bucket of the overlay (newest first). */
static kvsoBucket *kvsoBucketAt(kvstoreDictOverlay *ov, int k) {
    kvsoBucket *b = ov->buckets;
    while (b && k-- > 0) b = b->onext;
    return b;
}

/* Is 'key' (a stored key pointer) one of the originals of overlayed bucket ob? */
static int kvsoHasOriginal(kvsoBucket *ob, void *key) {
    for (dictEntry *e = ob->orig_head; e; e = dictGetNext(e))
        if (dictGetKey(e) == key) return 1;
    return 0;
}

/* The overlayed bucket whose originals include entry 'de' of the dict, or
 * NULL: for callers that do not know the bucket (scan callbacks, sampling).
 * One hash; the entry is in the table that has it, so both are tried. */
static kvsoBucket *kvsoOriginalOf(kvstoreDictOverlay *ov, dictEntry *de) {
    if (!ov->nb) return NULL;
    dict *d = ov->d;
    void *key = dictGetKey(de);
    uint64_t h = dictGetHash(d, kvsoStoredKey2Key(ov, key));
    for (int t = 0; t < 2; t++) {
        if (!d->ht_table[t]) continue;
        kvsoBucket *ob = kvsoLookup(ov, t, h & DICTHT_SIZE_MASK(d->ht_size_exp[t]));
        if (ob && kvsoHasOriginal(ob, key)) return ob;
    }
    return NULL;
}

/* ---- Chains of an overlayed bucket ---- */

/* The link of 'key' in the bucket's chain, or NULL. */
static dictEntryLink kvsoChainFind(kvstoreDictOverlay *ov, kvsoBucket *ob, const void *key) {
    dictCmpCache cache = {0};
    dictEntryLink link = &ob->chain;
    while (*link) {
        const void *k = kvsoEntryKey(ov, *link);
        if (k == key || (kvsoType(ov)->keyCompare ? kvsoType(ov)->keyCompare(&cache, key, k) : 0)) return link;
        link = dictEntryNextLink(*link);
    }
    return NULL;
}

/* The bucket of 'key' in the frozen dict as the overlay sees it. While the
 * dict is rehashing, dictKeyBucket() maps a key to table 0 when the dict's
 * table-0 chain holds it; for an overlayed bucket the chain that counts is
 * the bucket's own, which may hold a key added to it before the dict grew a
 * table 1 during the save (dictType.pausedExpandAllowed) and the dict's chain
 * lacks. A dict rehashing when it was frozen never adds such a key: new keys
 * go to table 1 from the start. */
static void kvsoKeyBucket(kvstoreDictOverlay *ov, const void *key, int *table, unsigned long *idx) {
    dict *d = ov->d;
    dictKeyBucket(d, key, table, idx);
    if (*table == 0 || !ov->nb) return;
    unsigned long i0 = dictGetHash(d, key) & DICTHT_SIZE_MASK(d->ht_size_exp[0]);
    if ((long)i0 < d->rehashidx) return;
    kvsoBucket *ob = kvsoLookup(ov, 0, i0);
    if (ob && kvsoChainFind(ov, ob, key)) {
        *table = 0;
        *idx = i0;
    }
}

/* A new entry for stored key 'kv' at the head of the bucket's chain; returns it. */
static dictEntry *kvsoChainPush(kvstoreDictOverlay *ov, kvsoBucket *ob, void *kv) {
    ob->chain = dictEntryCreateDetached(ov->d, kv, ob->chain);
    ob->nkeys++;
    return ob->chain;
}

/* Take the entry at 'link' out of its chain (not freed). */
static dictEntry *kvsoChainUnlink(kvsoBucket *ob, dictEntryLink link) {
    dictEntry *de = *link;
    *link = dictGetNext(de);
    ob->nkeys--;
    return de;
}

/* Free a chain entry the store no longer references, running the store's key
 * destructor against the real dict (the clone dies here). */
static void kvsoEntryFreeWithKey(kvstoreDictOverlay *ov, dictEntry *de) {
    dictEntryFreeDetached(ov->d, de, ov->d->type->keyDestructor != NULL);
}

/* The dict's last overlayed bucket is gone: its layout may change again. */
static void kvsoResume(kvstoreDictOverlay *ov) {
    if (!ov->paused) return;
    dictResumeRehashing(ov->d);
    dictResumeAutoResize(ov->d);
    ov->paused = 0;
    kvso_stats.open--;
    kvso_stats.open_since_sum -= ov->paused_at;
    kvso_stats.s.paused_us += kvsoNowUs() - ov->paused_at;
}

/* Owner thread: the bucket goes back to the dict. The scan thread is done
 * with it, so the slot is written plainly; the originals, found on the dict's
 * own chain, are released (the hook knows what each one still owns) and the
 * dict's size follows. */
static void kvsoWriteBack(kvsoBucket *ob) {
    kvstoreDictOverlay *ov = ob->ov;
    dict *d = ov->d;
    assert(d->ht_table[ob->table][ob->idx] == ob->orig_head);
    d->ht_table[ob->table][ob->idx] = ob->chain;
    long diff = ob->nkeys - ob->norig;
    d->ht_used[ob->table] += diff;
    ov->delta -= diff;
    for (dictEntry *e = ob->orig_head; e;) {
        void *orig = dictGetKey(e);
        dictEntry *next = dictGetNext(e);
        ov->h->free_orig(ov->privdata, ov->didx, orig);
        e = next;
    }
    dictChainFreeNodes(d, ob->orig_head);
    kvsoIndexRemove(ob);
    kvsoBucketsRemove(ov, ob);
    kvso_stats.cur--;
    zfree(ob);
    if (ov->nb == 0) kvsoResume(ov);
}

/* Duplicate the chain of bucket (table, idx), deferred at scan position
 * 'pos': every key through the 'clone' hook, same order. The dict is not
 * written. */
static kvsoBucket *kvsoBucketCreate(int didx, kvstoreDictOverlay *ov, int table, unsigned long idx, uint64_t pos) {
    dict *d = ov->d;
    if (!ov->paused) {   /* the buckets are named by (table, idx): the layout holds still while any is overlayed */
        dictPauseRehashing(d);
        dictPauseAutoResize(d);
        ov->paused = 1;
        ov->paused_at = kvsoNowUs();
        kvso_stats.open++;
        kvso_stats.open_since_sum += ov->paused_at;
    }
    kvsoBucket *ob = zcalloc(sizeof(*ob));
    ob->ov = ov;
    ob->pos = pos;
    ob->table = table;
    ob->idx = idx;
    ob->orig_head = d->ht_table[table][idx];
    dictEntryLink tail = &ob->chain;
    for (dictEntry *de = ob->orig_head; de; de = dictGetNext(de)) {
        void *clone = ov->h->clone(ov->privdata, didx, dictGetKey(de));
        assert(clone != NULL);
        dictEntry *node = dictEntryCreateDetached(d, clone, NULL);
        *tail = node;
        tail = dictEntryNextLink(node);
        ob->norig++;
        ob->nkeys++;
        kvso_stats.s.clones++;
    }
    kvsoBucketsAdd(ov, ob);
    kvsoIndexAdd(ob);
    kvso_stats.s.buckets++;
    if (++kvso_stats.cur > kvso_stats.s.peak) kvso_stats.s.peak = kvso_stats.cur;
    return ob;
}

/* ==========================================================================
 * Attach, write sections
 * ========================================================================== */

/* Start of a main-thread write on a frozen dict (see kvstoreOverlayEnterFn). */
static inline void kvsoEnter(kvstoreDictOverlay *ov) {
    if (ov->h->enter) ov->h->enter(ov->privdata);
}

/* May the frozen chain of 'key' be written in place? If not, *pos is the
 * scan position the bucket is deferred at. Must be called after kvsoEnter()
 * (the answer is only stable inside a write section). Without a prepare hook
 * the overlay is pure copy-on-write at position 0. */
static inline kvstoreOverlayPrepareResult kvsoPrepare(int didx, kvstoreDictOverlay *ov, const void *key, uint64_t *pos) {
    if (!ov->h->prepare) {
        *pos = 0;
        return KVSTORE_OV_DEFER;
    }
    return ov->h->prepare(ov->privdata, didx, ov->d, key, pos);
}

/* Is 'bucket' (a link handed out as a bucket) one of dict d's own buckets? */
static int kvsoBucketInDict(dict *d, dictEntryLink bucket) {
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
static inline kvstoreDictOverlay *kvsoOf(kvstore *kvs, int didx) {
    dict *d = kvstoreGetDict(kvs, didx);
    return d ? ((kvstoreDictMetaBase *)dictMetadata(d))->ov : NULL;
}

int kvstoreOverlayIsFrozen(kvstore *kvs, int didx) {
    return kvsoOf(kvs, didx) != NULL;
}

void kvstoreOverlayKeyBucket(dict *d, const void *key, int *table, unsigned long *idx) {
    kvstoreDictOverlay *ov = ((kvstoreDictMetaBase *)dictMetadata(d))->ov;
    if (ov) kvsoKeyBucket(ov, key, table, idx);
    else dictKeyBucket(d, key, table, idx);
}

/* What a write to 'key' of a frozen dict works on. */
typedef enum kvsoWriteTarget {
    KVSO_W_INPLACE = 0,   /* the dict itself: the snapshot holds the bucket */
    KVSO_W_BUCKET  = 1,   /* the bucket's overlay */
    KVSO_W_ABSENT  = 2,   /* neither: the key is not there and no overlay covers it */
} kvsoWriteTarget;

/* Write intent on the bucket of 'key' (inside the write section): the dict
 * itself when the snapshot holds the bucket (KVSO_W_INPLACE), else the bucket's
 * overlay (KVSO_W_BUCKET), created now when the scan thread may be reading the
 * chain. With 'miss_ok' false, a key the dict does not hold and no overlay
 * covers is KVSO_W_ABSENT and nothing is created. */
static kvsoWriteTarget kvsoWriteBucket(kvstore *kvs, int didx, kvstoreDictOverlay *ov, const void *key, int miss_ok, kvsoBucket **out) {
    (void)kvs;
    dict *d = ov->d;
    /* The bucket of the key must not move while it is decided on (a lookup
     * steps a rehash): the dict's layout is held for the whole decision, and
     * for as long as any bucket is overlayed (kvsoBucketCreate). */
    dictPauseRehashing(d);
    int t;
    kvsoWriteTarget w;
    unsigned long i;
    kvsoKeyBucket(ov, key, &t, &i);
    kvsoBucket *ob = kvsoLookup(ov, t, i);
    uint64_t pos;
    if (ob) {
        *out = ob;
        w = KVSO_W_BUCKET;
    } else if (!miss_ok && dictFind(d, key) == NULL) {
        w = KVSO_W_ABSENT;
    } else if (kvsoPrepare(didx, ov, key, &pos) == KVSTORE_OV_INPLACE) {
        w = KVSO_W_INPLACE;
    } else {
        *out = kvsoBucketCreate(didx, ov, t, i, pos);
        w = KVSO_W_BUCKET;
    }
    dictResumeRehashing(d);
    return w;
}

/* Logical size of dict didx (frozen or not). */
unsigned long kvsoDictSize(kvstore *kvs, int didx) {
    dict *d = kvstoreGetDict(kvs, didx);
    unsigned long n = d ? dictSize(d) : 0;
    kvstoreDictOverlay *ov = kvsoOf(kvs, didx);
    if (ov) n = n + ov->delta;
    return n;
}

/* ==========================================================================
 * Write-back, release
 * ========================================================================== */

/* The dict leaves its store (kvstore.c detachDict: freed while a save holds
 * it, emptied, moved): nothing reaches its overlay through the store any
 * more. Its buckets stay in the index and are written back like any other
 * once the window has passed them; the overlay waits for the save's release. */
void kvsoDetachFromStore(dict *d) {
    kvstoreDictOverlay *ov = ((kvstoreDictMetaBase *)dictMetadata(d))->ov;
    if (ov) ov->kvs = NULL;
}

/* Owner thread: the scan has passed every position below 'below', so the
 * buckets there are behind it. The slots in use, by the bitmap. */
long kvstoreOverlayDrain(uint64_t below) {
    if (!kvso_index.count) return 0;
    unsigned long words = (kvso_index.mask >> 6) + 1;
    for (unsigned long w = 0; w < words; w++) {
        for (uint64_t word = kvso_index.bits[w]; word; word &= word - 1) {
            unsigned long s = (w << 6) + (unsigned long)__builtin_ctzll(word);
            for (kvsoBucket *b = kvso_index.slot[s], *next; b; b = next) {
                next = b->hnext;   /* the write-back unlinks b */
                if (b->pos < below) kvsoWriteBack(b);
            }
        }
    }
    return kvso_stats.cur;
}

long kvstoreOverlayPending(void) {
    return kvso_stats.cur;
}

/* Owner thread, after the scan of this dict is complete: write back whatever
 * is left and drop the overlay. */
static void kvsoRelease(kvstoreDictOverlay *ov) {
    while (ov->buckets) kvsoWriteBack(ov->buckets);   /* written to the dict itself; its holder decides what becomes of it */
    assert(ov->nb == 0 && ov->delta == 0 && !ov->paused);
    zfree(ov);
}

/* Owner thread: the save is finished with the dict (its scan completed or
 * the save was aborted) and has released its own state: the overlay is
 * written back and goes. The dict may have left its store meanwhile
 * (detached, or moved to another store): the overlay follows the dict, so
 * this works wherever it is. */
void kvstoreOverlayRelease(dict *d) {
    kvstoreDictMetaBase *meta = (kvstoreDictMetaBase *)dictMetadata(d);
    kvstoreDictOverlay *ov = meta->ov;
    if (!ov) return;
    kvsoRelease(ov);
    __atomic_store_n(&meta->ov, NULL, __ATOMIC_RELEASE);
    /* The real dict may have become empty (every key deleted): its store, if it
     * still holds it, may drop it now (the save's reference keeps the dict
     * alive until the caller lets go). */
    kvstore *kvs = d->type->userdata;
    if (!meta->detached && kvstoreGetDict(kvs, meta->didx) == d) kvstoreFreeDictIfNeeded(kvs, meta->didx);
}

/* ==========================================================================
 * Enumeration through the overlay
 *
 * The logical keyspace is the dict with each overlayed bucket's chain in
 * place of the bucket's own. A chain is handed out where its first original is
 * met, so a scan's cursor never skips a key that was there the whole time; a
 * bucket the dict has no entry for comes last.
 * ========================================================================== */

/* The next overlayed bucket of the dict with no originals, from bucket *it of
 * its list (NULL: none left); advances it. */
static kvsoBucket *kvsoNextBare(kvsoBucket **it) {
    for (kvsoBucket *b = *it; b; b = b->onext) {
        if (!b->norig) {
            *it = b->onext;
            return b;
        }
    }
    *it = NULL;
    return NULL;
}

/* The dict's overlayed buckets one by one from a random one, wrapping around;
 * NULL at the end. */
typedef struct { kvsoBucket *b; int left; } kvsoRandomWalk;
static void kvsoRandomWalkInit(kvstoreDictOverlay *ov, kvsoRandomWalk *rw) {
    rw->left = ov->nb;
    rw->b = ov->nb ? kvsoBucketAt(ov, (int)(random() % ov->nb)) : NULL;
}
static kvsoBucket *kvsoRandomNext(kvstoreDictOverlay *ov, kvsoRandomWalk *rw) {
    if (rw->left-- <= 0) return NULL;
    kvsoBucket *b = rw->b;
    rw->b = b->onext ? b->onext : ov->buckets;
    return b;
}

struct kvsoScanCtx { kvstoreDictOverlay *ov; dictScanFunction *fn; void *priv; kvsoBucket *last; };
static void kvsoScanCb(void *p, const dictEntry *de, dictEntry **plink) {
    (void)plink;
    struct kvsoScanCtx *c = p;
    kvsoBucket *ob = kvsoOriginalOf(c->ov, (dictEntry *)de);
    if (!ob) { c->fn(c->priv, de, NULL); return; }
    if (ob == c->last) return;   /* its chain went out with an earlier original (a bucket's entries come one after the other) */
    c->last = ob;
    for (dictEntry *te = ob->chain; te;) {
        dictEntry *next = dictGetNext(te);
        c->fn(c->priv, te, NULL);
        te = next;
    }
}

/* kvstoreScan over a frozen dict with overlayed buckets: the dict through the
 * overlay, then the buckets it has no entry for with the final batch.
 * Returns the dict cursor. */
unsigned long long kvsoScan(kvstore *kvs, kvstoreDictOverlay *ov, dict *d, unsigned long long cursor,
                          dictScanFunction *scan_cb, void *privdata)
{
    (void)kvs;
    struct kvsoScanCtx sctx = { ov, scan_cb, privdata, NULL };
    unsigned long long next = (d && dictSize(d)) ? dictScan(d, cursor, kvsoScanCb, &sctx) : 0;
    if (next == 0) {
        kvsoBucket *it = ov->buckets;
        for (kvsoBucket *ob; (ob = kvsoNextBare(&it)) != NULL;) {
            for (dictEntry *te = ob->chain; te;) {
                dictEntry *nx = dictGetNext(te);
                scan_cb(privdata, te, NULL);
                te = nx;
            }
        }
    }
    return next;
}

/* Is the key of chain entry 'de' one of the first 'n' originals of the bucket (the iterator handed those out already)? */
static int kvsoHandedOut(kvstoreDictOverlay *ov, kvsoBucket *ob, int n, dictEntry *de) {
    dictCmpCache cache = {0};
    const void *k = kvsoEntryKey(ov, de);
    dictEntry *e = ob->orig_head;
    for (int p = 0; p < n && e; p++, e = dictGetNext(e)) {
        const void *o = kvsoStoredKey2Key(ov, dictGetKey(e));
        if (o == k || (kvsoType(ov)->keyCompare ? kvsoType(ov)->keyCompare(&cache, k, o) : 0)) return 1;
    }
    return 0;
}

/* One step of kvstoreIteratorNext / kvstoreDictIteratorNext over a frozen
 * dict with overlayed buckets. Phase 0: 'di' iterates the dict; an original
 * of an overlayed bucket is not handed out, its bucket's chain is instead
 * (phase 1, 'cur'), once. Phase 2: the buckets the dict has no entry for.
 * NULL once everything is exhausted. */
dictEntry *kvsoIteratorNext(kvstoreDictOverlay *ov, dictIterator *di, kvsoCursor *cur) {
    for (;;) {
        if (cur->phase != 0) {
            dictEntry *de = cur->next;
            if (de) {
                cur->next = dictGetNext(de);   /* fetched ahead: the caller may delete what it got */
                if (cur->phase == 1 && cur->skipn &&
                    kvsoHandedOut(ov, kvsoLookup(ov, (int)(cur->bucket & 1), (unsigned long)cur->bucket >> 1), cur->skipn, de))
                    continue;
                return de;
            }
            if (cur->phase == 1) { cur->phase = 0; continue; }
            /* Phase 2: the next bucket without originals. */
            kvsoBucket *ob = kvsoNextBare((kvsoBucket **)&cur->ob);
            if (!ob) return NULL;
            cur->next = ob->chain;
            continue;
        }
        dictEntry *de = di->d ? dictNext(di) : NULL;
        if (!de) {
            if (!di->d) return NULL;
            cur->phase = 2;
            cur->ob = ov->buckets;
            cur->next = NULL;
            continue;
        }
        /* The dict iterator knows the bucket it is in. */
        kvsoBucket *ob = kvsoLookup(ov, di->table, di->index);
        if (!ob) return de;
        /* An original: its bucket's chain goes out in its place, once (the
         * dict yields a chain's entries one after the other, so a later
         * original of the same dict bucket is skipped). When the bucket was
         * overlayed after some of its originals were handed out, the clones
         * of those are not handed out again ('skipn' leading originals). */
        long bucket = (long)((di->index << 1) | (unsigned long)di->table);
        if (cur->last && cur->bucket == bucket) continue;
        void *key = dictGetKey(de);
        int p = 0;
        for (dictEntry *e = ob->orig_head; e && dictGetKey(e) != key; e = dictGetNext(e)) p++;
        cur->last = 1;
        cur->bucket = bucket;
        cur->skipn = p;
        cur->phase = 1;
        cur->next = ob->chain;
    }
}

/* The chain head of the first non-empty overlayed bucket from a random start, or NULL. */
static dictEntry *kvsoRandomChainHead(kvstoreDictOverlay *ov) {
    kvsoRandomWalk rw;
    kvsoRandomWalkInit(ov, &rw);
    for (kvsoBucket *ob; (ob = kvsoRandomNext(ov, &rw)) != NULL;)
        if (ob->chain) return ob->chain;
    return NULL;
}

/* Random entry of a frozen dict: sample the real dict (skipping the originals
 * of overlayed buckets) and fall back to the overlayed chains. */
dictEntry *kvsoRandomEntry(kvstore *kvs, int didx, kvstoreDictOverlay *ov, int fair) {
    dict *d = ov->d;
    if (kvsoDictSize(kvs, didx) == 0) return NULL;
    if (d && dictSize(d)) {
        for (int tries = 0; tries < 16; tries++) {
            dictEntry *de = fair ? dictGetFairRandomKey(d) : dictGetRandomKey(d);
            if (!de) break;
            if (!kvsoOriginalOf(ov, de)) return de;
        }
    }
    dictEntry *de = kvsoRandomChainHead(ov);
    if (de) return de;
    /* Only originals of overlayed buckets were sampled: scan for any live entry. */
    kvstoreDictIterator it;
    kvstoreInitDictIterator(&it, kvs, didx);
    de = kvstoreDictIteratorNext(&it);
    kvstoreResetDictIterator(&it);
    return de;
}

/* Some keys of a frozen dict: sample the real dict (skipping the originals of
 * overlayed buckets), then fill from the overlayed chains. */
unsigned int kvsoGetSomeKeys(kvstoreDictOverlay *ov, dictEntry **des, unsigned int count) {
    unsigned int n = 0;
    dict *d = ov->d;
    if (d && dictSize(d)) {
        unsigned int got = dictGetSomeKeys(d, des, count);
        for (unsigned int i = 0; i < got; i++)
            if (!kvsoOriginalOf(ov, des[i])) des[n++] = des[i];
    }
    if (n < count && ov->nb) {
        kvsoRandomWalk rw;
        kvsoRandomWalkInit(ov, &rw);
        for (kvsoBucket *ob; n < count && (ob = kvsoRandomNext(ov, &rw)) != NULL;)
            for (dictEntry *te = ob->chain; te && n < count; te = dictGetNext(te)) des[n++] = te;
    }
    return n;
}

/* ==========================================================================
 * Writes through the overlay
 * ========================================================================== */

/* Overlay (frozen slot) halves of the hot kvstore entry points: out of line so
 * the common path stays as small as it was and keeps being inlined. */
dictEntry *kvsoDictFind(kvstoreDictOverlay *ov, void *key) {
    /* Read: the logical keyspace, no copying. */
    int t;
    unsigned long i;
    kvsoKeyBucket(ov, key, &t, &i);
    kvsoBucket *ob = kvsoLookup(ov, t, i);
    if (!ob) return dictFind(ov->d, key);
    dictEntryLink link = kvsoChainFind(ov, ob, key);
    return link ? *link : NULL;
}

/* Write-intent lookup (see kvstoreDictFindLink in kvstore.c for the contract). */
dictEntryLink kvsoDictFindLink(kvstore *kvs, int didx, kvstoreDictOverlay *ov, void *key, dictEntryLink *bucket) {
    kvsoEnter(ov);
    kvsoBucket *ob;
    if (kvsoWriteBucket(kvs, didx, ov, key, 1, &ob) == KVSO_W_INPLACE) return dictFindLink(ov->d, key, bucket);
    /* The overlayed bucket: a miss returns its chain head so the insertion lands there. */
    if (bucket) *bucket = &ob->chain;
    return kvsoChainFind(ov, ob, key);
}

void kvsoDictSetAtLink(kvstore *kvs, int didx, kvstoreDictOverlay *ov, void *kv, dictEntryLink *link, int newItem) {
    kvsoEnter(ov);
    dict *d = ov->d;
    const void *key = kv ? kvsoStoredKey2Key(ov, kv) : kv;
    if (!newItem) {
        /* Replacing the key of an existing entry: with a link it is a store
         * through the link (the dict only lends its type); without one, look
         * the key up in the overlay first, then the dict. */
        dictEntryLink l = link ? *link : NULL;
        if (!l) {
            int t;
            unsigned long i;
            kvsoKeyBucket(ov, key, &t, &i);
            kvsoBucket *ob = kvsoLookup(ov, t, i);
            if (ob) {
                l = kvsoChainFind(ov, ob, key);
                if (l && link) *link = l;
            }
        }
        dictSetKeyAtLink(d, kv, l ? &l : link, 0);
        return;
    }
    /* New item: a bucket link tells the dict (the dict's buckets come from an
     * INPLACE write lookup of this same section); without one decide now. */
    kvsoBucket *ob = NULL;
    int in_dict;
    if (link && *link) {
        in_dict = kvsoBucketInDict(d, *link);
        if (!in_dict) {
            int t;
            unsigned long i;
            kvsoKeyBucket(ov, key, &t, &i);
            ob = kvsoLookup(ov, t, i);
            assert(ob != NULL);
        }
    } else {
        in_dict = kvsoWriteBucket(kvs, didx, ov, key, 1, &ob) == KVSO_W_INPLACE;
    }
    if (in_dict) {
        dictSetKeyAtLink(d, kv, link, 1);   /* may grow a table 1 (dictType.pausedExpandAllowed) */
        kvsoKeyCountAdd(kvs, didx, 1);
    } else {
        kvsoChainPush(ov, ob, kv);
        ov->delta++;
        kvsoKeyCountAdd(kvs, didx, 1);
        if (link) *link = &ob->chain;   /* the inserted entry's link, as dictSetKeyAtLink reports it */
    }
}

dictEntry *kvsoDictAddRaw(kvstore *kvs, int didx, kvstoreDictOverlay *ov, void *key, dictEntry **existing) {
    kvsoEnter(ov);
    const void *lkey = kvsoStoredKey2Key(ov, key);
    kvsoBucket *ob;
    if (kvsoWriteBucket(kvs, didx, ov, lkey, 1, &ob) == KVSO_W_INPLACE) {
        dictEntry *ret = dictAddRaw(ov->d, key, existing);
        if (ret) kvsoKeyCountAdd(kvs, didx, 1);
        return ret;
    }
    dictEntryLink link = kvsoChainFind(ov, ob, lkey);
    if (link) { if (existing) *existing = *link; return NULL; }
    if (ov->d->type->keyDup) key = ov->d->type->keyDup(ov->d, key);
    dictEntry *de = kvsoChainPush(ov, ob, key);
    ov->delta++;
    kvsoKeyCountAdd(kvs, didx, 1);
    return de;
}

dictEntryLink kvsoDictTwoPhaseUnlinkFind(kvstore *kvs, int didx, kvstoreDictOverlay *ov, const void *key, int *table_index) {
    kvsoEnter(ov);
    ov->unlink_frozen = 0;
    ov->unlink_ob = NULL;
    kvsoBucket *ob;
    kvsoWriteTarget w = kvsoWriteBucket(kvs, didx, ov, key, 0, &ob);
    if (w == KVSO_W_ABSENT) return NULL;
    if (w == KVSO_W_INPLACE) {
        dictEntryLink l = dictTwoPhaseUnlinkFind(ov->d, key, table_index);
        if (l) ov->unlink_frozen = 1;
        return l;
    }
    dictEntryLink link = kvsoChainFind(ov, ob, key);
    if (!link) return NULL;
    ov->unlink_ob = ob;
    *table_index = 0;
    return link;
}

/* As above, with the key given explicitly: callers that already detached the
 * value from the entry (dbGenericDelete's async path stores NULL at the link
 * before unlinking) cannot have it read back from the entry. 'key' may be
 * NULL when the entry still holds it. */
void kvsoDictTwoPhaseUnlinkFreeKey(kvstore *kvs, int didx, kvstoreDictOverlay *ov, dictEntryLink link, int table_index, const void *key) {
    (void)key;
    kvsoEnter(ov);
    if (ov->unlink_frozen) {
        /* Second phase of an in-place unlink (see kvsoDictTwoPhaseUnlinkFind). */
        ov->unlink_frozen = 0;
        dictTwoPhaseUnlinkFree(ov->d, link, table_index);
        kvsoKeyCountAdd(kvs, didx, -1);
        return;
    }
    if (link && *link) {
        kvsoBucket *ob = ov->unlink_ob;
        assert(ob != NULL);
        ov->unlink_ob = NULL;
        dictEntry *de = kvsoChainUnlink(ob, link);
        ov->delta--;
        kvsoKeyCountAdd(kvs, didx, -1);
        kvsoEntryFreeWithKey(ov, de);
    }
}

int kvsoDictDelete(kvstore *kvs, int didx, kvstoreDictOverlay *ov, const void *key) {
    kvsoEnter(ov);
    kvsoBucket *ob;
    kvsoWriteTarget w = kvsoWriteBucket(kvs, didx, ov, key, 0, &ob);
    if (w == KVSO_W_ABSENT) return DICT_ERR;
    if (w == KVSO_W_INPLACE) {
        int ret = dictDelete(ov->d, key);
        if (ret == DICT_OK) kvsoKeyCountAdd(kvs, didx, -1);
        return ret;
    }
    dictEntryLink link = kvsoChainFind(ov, ob, key);
    if (!link) return DICT_ERR;
    dictEntry *de = kvsoChainUnlink(ob, link);
    ov->delta--;
    kvsoKeyCountAdd(kvs, didx, -1);
    kvsoEntryFreeWithKey(ov, de);
    return DICT_OK;
}
