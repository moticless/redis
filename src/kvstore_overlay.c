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

/* The overlay of a frozen dict and the one index of overlayed buckets, by
 * window (kvstore_overlay.h, THE OVERLAYED BUCKETS). kvstore.c sees only the
 * forward declaration. Only the main thread touches any of this. */
#define KVSO_SLOT_KEEP 4096   /* offsets a free slot keeps allocated for the next window */

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
    int slot;                   /* its index slot (kvso_index) */
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
} kvstoreDictOverlay;

/* The overlayed buckets of one window of one dict. */
typedef struct kvsoSlot {
    kvstoreDictOverlay *ov;     /* NULL: free */
    uint64_t last;              /* the scan position of the window's last bucket */
    int table;
    unsigned long lo, n;        /* buckets [lo, lo + n) */
    long count;                 /* overlayed buckets in it */
    kvsoBucket **b;               /* by offset idx - lo */
    uint64_t *bits;             /* the offsets in use */
    unsigned long cap;          /* offsets allocated in b and bits */
} kvsoSlot;

static kvsoSlot *kvso_index;     /* kvso_nslots entries */
static int kvso_nslots;
static int kvso_slots_used;

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

static kvsoBucket *kvsoLookup(kvstoreDictOverlay *ov, int table, unsigned long idx) {
    if (!ov->nb) return NULL;
    for (int k = 0; k < kvso_nslots; k++) {
        kvsoSlot *s = &kvso_index[k];
        if (s->ov == ov && s->table == table && idx - s->lo < s->n && s->b[idx - s->lo]) return s->b[idx - s->lo];
    }
    return NULL;
}

/* The first offset at or after 'from' that holds a bucket, s->n if none. */
static unsigned long kvsoSlotNext(const kvsoSlot *s, unsigned long from) {
    if (from >= s->n) return s->n;
    unsigned long w = from >> 6, words = (s->n + 63) >> 6;
    uint64_t word = s->bits[w] & (~0ULL << (from & 63));
    while (!word) {
        if (++w >= words) return s->n;
        word = s->bits[w];
    }
    return (w << 6) + (unsigned long)__builtin_ctzll(word);
}

/* The slot of window 'w' of the dict, taken now if no slot has it yet. */
static kvsoSlot *kvsoSlotFor(kvstoreDictOverlay *ov, const kvstoreOvWindow *w) {
    kvsoSlot *free_slot = NULL;
    for (int k = 0; k < kvso_nslots; k++) {
        kvsoSlot *s = &kvso_index[k];
        if (s->ov == ov && s->last == w->last && s->table == w->table && s->lo == w->lo && s->n == w->n) return s;
        if (!s->ov && !free_slot) free_slot = s;
    }
    if (!free_slot) {
        int n = kvso_nslots ? kvso_nslots * 2 : 4;
        kvso_index = zrealloc(kvso_index, n * sizeof(*kvso_index));
        memset(kvso_index + kvso_nslots, 0, (n - kvso_nslots) * sizeof(*kvso_index));
        free_slot = &kvso_index[kvso_nslots];
        kvso_nslots = n;
    }
    kvsoSlot *s = free_slot;
    kvso_slots_used++;
    if (s->cap < w->n) {
        zfree(s->b);
        zfree(s->bits);
        s->b = zcalloc(w->n * sizeof(*s->b));
        s->bits = zcalloc(((w->n + 63) >> 6) * sizeof(*s->bits));
        s->cap = w->n;
    }
    s->ov = ov;
    s->last = w->last;
    s->table = w->table;
    s->lo = w->lo;
    s->n = w->n;
    return s;
}

/* Take ob out of its slot; a slot left empty is free again. */
static void kvsoSlotRemove(kvsoBucket *ob) {
    kvsoSlot *s = &kvso_index[ob->slot];
    unsigned long off = ob->idx - s->lo;
    s->b[off] = NULL;
    s->bits[off >> 6] &= ~(1ULL << (off & 63));
    if (--s->count) return;
    s->ov = NULL;
    kvso_slots_used--;
    if (s->cap > KVSO_SLOT_KEEP) {
        zfree(s->b);
        zfree(s->bits);
        s->b = NULL;
        s->bits = NULL;
        s->cap = 0;
    }
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
    kvsoSlotRemove(ob);
    kvso_stats.cur--;
    zfree(ob);
    if (--ov->nb == 0) kvsoResume(ov);
}

/* Write back every bucket of slot s. */
static void kvsoSlotWriteBack(kvsoSlot *s) {
    unsigned long off = 0;
    while (s->count) {   /* the last write-back frees the slot */
        off = kvsoSlotNext(s, off);
        kvsoWriteBack(s->b[off]);
        off++;
    }
}

/* Duplicate the chain of bucket (table, idx), deferred in window 'w': every
 * key through the 'clone' hook, same order. The dict is not written. */
static kvsoBucket *kvsoBucketCreate(int didx, kvstoreDictOverlay *ov, int table, unsigned long idx, const kvstoreOvWindow *w) {
    dict *d = ov->d;
    assert(w->table == table && idx - w->lo < w->n);
    if (!ov->paused) {   /* the buckets are named by (table, idx): the layout holds still while any is overlayed */
        dictPauseRehashing(d);
        dictPauseAutoResize(d);
        ov->paused = 1;
        ov->paused_at = kvsoNowUs();
        kvso_stats.open++;
        kvso_stats.open_since_sum += ov->paused_at;
    }
    kvsoSlot *s = kvsoSlotFor(ov, w);
    kvsoBucket *ob = zcalloc(sizeof(*ob));
    ob->ov = ov;
    ob->slot = (int)(s - kvso_index);
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
    unsigned long off = idx - s->lo;
    s->b[off] = ob;
    s->bits[off >> 6] |= 1ULL << (off & 63);
    s->count++;
    ov->nb++;
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

/* May the frozen chain of 'key' (in table 'table') be written in place? If
 * not, *w is the window the bucket is deferred in. Must be called after
 * kvsoEnter() (the answer is only stable inside a write section). Without a
 * prepare hook the overlay is pure copy-on-write, one window per table. */
static inline kvstoreOverlayPrepareResult kvsoPrepare(int didx, kvstoreDictOverlay *ov, const void *key, int table, kvstoreOvWindow *w) {
    if (!ov->h->prepare) {
        w->last = 0;
        w->table = table;
        w->lo = 0;
        w->n = DICTHT_SIZE(ov->d->ht_size_exp[table]);
        return KVSTORE_OV_DEFER;
    }
    return ov->h->prepare(ov->privdata, didx, ov->d, key, w);
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
    dictKeyBucket(d, key, &t, &i);
    kvsoBucket *ob = kvsoLookup(ov, t, i);
    kvstoreOvWindow win;
    if (ob) {
        *out = ob;
        w = KVSO_W_BUCKET;
    } else if (!miss_ok && dictFind(d, key) == NULL) {
        w = KVSO_W_ABSENT;
    } else if (kvsoPrepare(didx, ov, key, t, &win) == KVSTORE_OV_INPLACE) {
        w = KVSO_W_INPLACE;
    } else {
        *out = kvsoBucketCreate(didx, ov, t, i, &win);
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
 * windows whose last bucket is below it are behind it with all their buckets. */
long kvstoreOverlayDrain(uint64_t below) {
    for (int k = 0; k < kvso_nslots; k++)
        if (kvso_index[k].ov && kvso_index[k].last < below) kvsoSlotWriteBack(&kvso_index[k]);
    return kvso_stats.cur;
}

int kvstoreOverlaySlotsInUse(void) {
    return kvso_slots_used;
}

long kvstoreOverlayPending(void) {
    return kvso_stats.cur;
}

/* Owner thread, after the scan of this dict is complete: write back whatever
 * is left and drop the overlay. */
static void kvsoRelease(kvstoreDictOverlay *ov) {
    for (int k = 0; k < kvso_nslots; k++)   /* written to the dict itself; its holder decides what becomes of it */
        if (kvso_index[k].ov == ov) kvsoSlotWriteBack(&kvso_index[k]);
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

/* The next overlayed bucket of the dict with no originals, at or after
 * offset *pos of index slot *slot; advances both. */
static kvsoBucket *kvsoNextBare(kvstoreDictOverlay *ov, int *slot, long *pos) {
    for (; *slot < kvso_nslots; (*slot)++, *pos = 0) {
        kvsoSlot *s = &kvso_index[*slot];
        if (s->ov != ov) continue;
        for (unsigned long off = kvsoSlotNext(s, (unsigned long)*pos); off < s->n; off = kvsoSlotNext(s, off + 1)) {
            if (!s->b[off]->norig) {
                *pos = (long)off + 1;
                return s->b[off];
            }
        }
    }
    return NULL;
}

/* The dict's overlayed buckets one by one, from a random index slot and a
 * random offset in each slot, wrapping around; NULL at the end. */
typedef struct { int k0, k, begun, wrapped; unsigned long start, off; } kvsoRandomWalk;
static void kvsoRandomWalkInit(kvsoRandomWalk *rw) {
    memset(rw, 0, sizeof(*rw));
    rw->k0 = kvso_nslots ? (int)(random() % kvso_nslots) : 0;
}
static kvsoBucket *kvsoRandomNext(kvstoreDictOverlay *ov, kvsoRandomWalk *rw) {
    for (; rw->k < kvso_nslots; rw->k++, rw->begun = 0) {
        kvsoSlot *s = &kvso_index[(rw->k0 + rw->k) % kvso_nslots];
        if (s->ov != ov) continue;
        if (!rw->begun) {
            rw->begun = 1;
            rw->wrapped = 0;
            rw->start = rw->off = (unsigned long)random() % s->n;
        }
        for (;;) {
            unsigned long off = kvsoSlotNext(s, rw->off);
            if (!rw->wrapped && off >= s->n) {   /* to the end, then from offset 0 up to the start */
                rw->wrapped = 1;
                rw->off = 0;
                continue;
            }
            if (rw->wrapped && off >= rw->start) break;
            rw->off = off + 1;
            return s->b[off];
        }
    }
    return NULL;
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
        int slot = 0;
        long pos = 0;
        for (kvsoBucket *ob; (ob = kvsoNextBare(ov, &slot, &pos)) != NULL;) {
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
dictEntry *kvsoIteratorNext(kvstoreDictOverlay *ov, dictIterator *di, int *phase, kvsoCursor *cur) {
    for (;;) {
        if (*phase != 0) {
            dictEntry *de = cur->next;
            if (de) {
                cur->next = dictGetNext(de);   /* fetched ahead: the caller may delete what it got */
                if (*phase == 1 && cur->skipn &&
                    kvsoHandedOut(ov, kvsoLookup(ov, (int)(cur->bucket & 1), (unsigned long)cur->bucket >> 1), cur->skipn, de))
                    continue;
                return de;
            }
            if (*phase == 1) { *phase = 0; continue; }
            /* Phase 2: the next bucket without originals. */
            kvsoBucket *ob = kvsoNextBare(ov, &cur->slot, &cur->bucket);
            if (!ob) return NULL;
            cur->next = ob->chain;
            continue;
        }
        dictEntry *de = di->d ? dictNext(di) : NULL;
        if (!de) {
            if (!di->d) return NULL;
            *phase = 2;
            cur->slot = 0;
            cur->bucket = 0;
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
        *phase = 1;
        cur->next = ob->chain;
    }
}

/* The chain head of the first non-empty overlayed bucket from a random start, or NULL. */
static dictEntry *kvsoRandomChainHead(kvstoreDictOverlay *ov) {
    kvsoRandomWalk rw;
    kvsoRandomWalkInit(&rw);
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
        kvsoRandomWalkInit(&rw);
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
    dictKeyBucket(ov->d, key, &t, &i);
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
            dictKeyBucket(d, key, &t, &i);
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
            dictKeyBucket(d, key, &t, &i);
            ob = kvsoLookup(ov, t, i);
            assert(ob != NULL);
        }
    } else {
        in_dict = kvsoWriteBucket(kvs, didx, ov, key, 1, &ob) == KVSO_W_INPLACE;
    }
    if (in_dict) {
        dictSetKeyAtLink(d, kv, link, 1);   /* no expand: auto-resize is paused */
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
