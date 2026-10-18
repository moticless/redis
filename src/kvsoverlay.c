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

/* Snapshot overlay of a kvstore (hybrid snapshot): the duplicated bucket
 * chains that keep main-thread writes out of the buckets the scan thread is
 * reading, and the overlay halves of the kvstore entry points. kvstore.c holds
 * only the dispatch into this file (the public wrappers, the iterators and
 * the dict hooks); kvsoverlay.h is what the two files share. The save's own
 * per-dict state (bitmaps, pause) lives in the dict's extension slot and is
 * snapshot.c's. */

/* Opt in to the private overlay header. */
#define INCLUDE_KVSOVERLAY_H

#include "fmacros.h"

#include <string.h>
#include <stddef.h>
#include <time.h>

#include "zmalloc.h"
#include "kvsoverlay.h"
#include "redisassert.h"

/* ==========================================================================
 * Snapshot overlay
 * ========================================================================== */

/* The overlay of a frozen dict (public API: kvstoreOverlay* in kvstore.h;
 * kvstore.c sees only the forward declaration): the buckets the main thread
 * wrote while the scan thread was reading them. A written bucket is
 * "overlayed": its chain is duplicated (every key through the 'clone' hook)
 * and the main thread works on the duplicate, the dict's own slot and chain
 * staying exactly as the thread reads them. The duplicate is written back to
 * the slot once the thread has moved past the bucket's window, and the
 * originals are released then. Until then the overlay's chain is the bucket:
 * reads, sizes and enumeration prefer it to the dict's. Only the main thread
 * touches any of this.
 *
 * The overlayed buckets are indexed by the window they were deferred in
 * ('prepare' reports it: a run [lo, lo + n) of one table, and 'last', the
 * scan position of its last bucket): one index slot per window that holds
 * buckets, a bucket at offset idx - lo of its slot, with a bitmap of the
 * offsets in use. Once the scan has passed a slot's last bucket, every bucket
 * of the slot is written back together. The index grows on demand; the
 * owner keeps it small by draining at the end of every write section and by
 * holding the scan back once many slots are in use (kvstoreOverlaySlotsInUse).
 * One index for every store: there is one scan at a time. */
#define OV_SLOT_KEEP 4096   /* offsets a free slot keeps allocated for the next window */

/* The overlay's work counters (main thread only), see kvstoreOverlayStats.
 * A pause still open is added at read time from its count and the sum of its
 * start times. */
static struct {
    kvstoreOverlayStats s;
    long cur;                   /* overlayed buckets now */
    int open;                   /* dicts whose pause is open */
    long long open_since_sum;   /* sum of their start times, us */
} ov_stats;

static long long ovNowUs(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (long long)ts.tv_sec * 1000000 + ts.tv_nsec / 1000;
}

void kvstoreOverlayStatsReset(void) {
    long keep = ov_stats.cur;
    memset(&ov_stats.s, 0, sizeof(ov_stats.s));
    ov_stats.s.peak = keep;
}

kvstoreOverlayStats kvstoreOverlayStatsGet(void) {
    kvstoreOverlayStats r = ov_stats.s;
    if (ov_stats.open) r.paused_us += (long long)ov_stats.open * ovNowUs() - ov_stats.open_since_sum;
    return r;
}

typedef struct ovBucket {
    struct kvstoreDictOverlay *ov;   /* the dict's overlay */
    int slot;                   /* its index slot (ov_index) */
    int table;
    unsigned long idx;
    dictEntry *chain;           /* the bucket's logical chain: detached entries holding the clones and the adds */
    dictEntry *orig_head;       /* the dict's chain, untouched until the write-back: the originals */
    int norig;                  /* entries of orig_head */
    long nkeys;                 /* entries of chain */
} ovBucket;

typedef struct kvstoreDictOverlay {
    kvstore *kvs;               /* the store the dict is in; NULL once the dict left it (detached: no more
                                   accesses through the store, the overlay only waits for the release) */
    int didx;                   /* the dict's slot */
    dict *d;                    /* the frozen dict itself (the store's dict of slot didx while owned) */
    int nb;                     /* overlayed buckets */
    long delta;                 /* logical size - dict size: the adds minus the deletes of the overlayed buckets */
    const kvstoreSnapHooks *h;  /* the store's hooks at attach time (kvstore.h kvstoreSnapHooks) */
    int paused;                 /* rehash and auto-resize are held while buckets are overlayed */
    long long paused_at;        /* when, us (ov_stats) */
    int unlink_frozen;          /* the pending two-phase unlink targets the frozen dict */
    ovBucket *unlink_ob;        /* ... or this overlayed bucket */
    void *privdata;
} kvstoreDictOverlay;

/* The overlayed buckets of one window of one dict. */
typedef struct ovSlot {
    kvstoreDictOverlay *ov;     /* NULL: free */
    uint64_t last;              /* the scan position of the window's last bucket */
    int table;
    unsigned long lo, n;        /* buckets [lo, lo + n) */
    long count;                 /* overlayed buckets in it */
    ovBucket **b;               /* by offset idx - lo */
    uint64_t *bits;             /* the offsets in use */
    unsigned long cap;          /* offsets allocated in b and bits */
} ovSlot;

static ovSlot *ov_index;     /* ov_nslots entries */
static int ov_nslots;
static int ov_slots_used;

int ovActive(kvstoreDictOverlay *ov) {
    return ov && ov->nb > 0;
}

/* The key functions are the dict type's, which the frozen dict carries. */
static inline const dictType *ovType(kvstoreDictOverlay *ov) { return ov->d->type; }

/* Lookup key (sds) of a stored entry of a keyspace dict. */
static inline const void *ovEntryKey(kvstoreDictOverlay *ov, dictEntry *de) {
    void *stored = dictGetKey(de);
    return ovType(ov)->keyFromStoredKey ? ovType(ov)->keyFromStoredKey(stored) : stored;
}

static inline const void *ovStoredKey2Key(kvstoreDictOverlay *ov, const void *stored) {
    return ovType(ov)->keyFromStoredKey ? ovType(ov)->keyFromStoredKey(stored) : stored;
}

/* Logical key-count bookkeeping for a frozen dict: the real dict size does not
 * change, so derive the empty/non-empty transition from the logical size. */
static void ovKeyCountAdd(kvstore *kvs, int didx, long delta) {
    kvstoreKeyCountAdd(kvs, didx, delta, ovDictSize(kvs, didx));
}

/* ---- The index of overlayed buckets ---- */

static ovBucket *ovLookup(kvstoreDictOverlay *ov, int table, unsigned long idx) {
    if (!ov->nb) return NULL;
    for (int k = 0; k < ov_nslots; k++) {
        ovSlot *s = &ov_index[k];
        if (s->ov == ov && s->table == table && idx - s->lo < s->n && s->b[idx - s->lo]) return s->b[idx - s->lo];
    }
    return NULL;
}

/* The first offset at or after 'from' that holds a bucket, s->n if none. */
static unsigned long ovSlotNext(const ovSlot *s, unsigned long from) {
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
static ovSlot *ovSlotFor(kvstoreDictOverlay *ov, const kvstoreOvWindow *w) {
    ovSlot *free_slot = NULL;
    for (int k = 0; k < ov_nslots; k++) {
        ovSlot *s = &ov_index[k];
        if (s->ov == ov && s->last == w->last && s->table == w->table && s->lo == w->lo && s->n == w->n) return s;
        if (!s->ov && !free_slot) free_slot = s;
    }
    if (!free_slot) {
        int n = ov_nslots ? ov_nslots * 2 : 4;
        ov_index = zrealloc(ov_index, n * sizeof(*ov_index));
        memset(ov_index + ov_nslots, 0, (n - ov_nslots) * sizeof(*ov_index));
        free_slot = &ov_index[ov_nslots];
        ov_nslots = n;
    }
    ovSlot *s = free_slot;
    ov_slots_used++;
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
static void ovSlotRemove(ovBucket *ob) {
    ovSlot *s = &ov_index[ob->slot];
    unsigned long off = ob->idx - s->lo;
    s->b[off] = NULL;
    s->bits[off >> 6] &= ~(1ULL << (off & 63));
    if (--s->count) return;
    s->ov = NULL;
    ov_slots_used--;
    if (s->cap > OV_SLOT_KEEP) {
        zfree(s->b);
        zfree(s->bits);
        s->b = NULL;
        s->bits = NULL;
        s->cap = 0;
    }
}

/* Is 'key' (a stored key pointer) one of the originals of overlayed bucket ob? */
static int ovHasOriginal(ovBucket *ob, void *key) {
    for (dictEntry *e = ob->orig_head; e; e = dictGetNext(e))
        if (dictGetKey(e) == key) return 1;
    return 0;
}

/* The overlayed bucket whose originals include entry 'de' of the dict, or
 * NULL: for callers that do not know the bucket (scan callbacks, sampling).
 * One hash; the entry is in the table that has it, so both are tried. */
static ovBucket *ovOriginalOf(kvstoreDictOverlay *ov, dictEntry *de) {
    if (!ov->nb) return NULL;
    dict *d = ov->d;
    void *key = dictGetKey(de);
    uint64_t h = dictGetHash(d, ovStoredKey2Key(ov, key));
    for (int t = 0; t < 2; t++) {
        if (!d->ht_table[t]) continue;
        ovBucket *ob = ovLookup(ov, t, h & DICTHT_SIZE_MASK(d->ht_size_exp[t]));
        if (ob && ovHasOriginal(ob, key)) return ob;
    }
    return NULL;
}

/* ---- Chains of an overlayed bucket ---- */

/* The link of 'key' in the bucket's chain, or NULL. */
static dictEntryLink ovChainFind(kvstoreDictOverlay *ov, ovBucket *ob, const void *key) {
    dictCmpCache cache = {0};
    dictEntryLink link = &ob->chain;
    while (*link) {
        const void *k = ovEntryKey(ov, *link);
        if (k == key || (ovType(ov)->keyCompare ? ovType(ov)->keyCompare(&cache, key, k) : 0)) return link;
        link = dictEntryNextLink(*link);
    }
    return NULL;
}

/* A new entry for stored key 'kv' at the head of the bucket's chain; returns it. */
static dictEntry *ovChainPush(kvstoreDictOverlay *ov, ovBucket *ob, void *kv) {
    ob->chain = dictEntryCreateDetached(ov->d, kv, ob->chain);
    ob->nkeys++;
    return ob->chain;
}

/* Take the entry at 'link' out of its chain (not freed). */
static dictEntry *ovChainUnlink(ovBucket *ob, dictEntryLink link) {
    dictEntry *de = *link;
    *link = dictGetNext(de);
    ob->nkeys--;
    return de;
}

/* Free a chain entry the store no longer references, running the store's key
 * destructor against the real dict (the clone dies here). */
static void ovEntryFreeWithKey(kvstoreDictOverlay *ov, dictEntry *de) {
    dictEntryFreeDetached(ov->d, de, ov->d->type->keyDestructor != NULL);
}

/* The dict's last overlayed bucket is gone: its layout may change again. */
static void ovResume(kvstoreDictOverlay *ov) {
    if (!ov->paused) return;
    dictResumeRehashing(ov->d);
    dictResumeAutoResize(ov->d);
    ov->paused = 0;
    ov_stats.open--;
    ov_stats.open_since_sum -= ov->paused_at;
    ov_stats.s.paused_us += ovNowUs() - ov->paused_at;
}

/* Owner thread: the bucket goes back to the dict. The scan thread is done
 * with it, so the slot is written plainly; the originals, found on the dict's
 * own chain, are released (the hook knows what each one still owns) and the
 * dict's size follows. */
static void ovWriteBack(ovBucket *ob) {
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
    ovSlotRemove(ob);
    ov_stats.cur--;
    zfree(ob);
    if (--ov->nb == 0) ovResume(ov);
}

/* Write back every bucket of slot s. */
static void ovSlotWriteBack(ovSlot *s) {
    unsigned long off = 0;
    while (s->count) {   /* the last write-back frees the slot */
        off = ovSlotNext(s, off);
        ovWriteBack(s->b[off]);
        off++;
    }
}

/* Duplicate the chain of bucket (table, idx), deferred in window 'w': every
 * key through the 'clone' hook, same order. The dict is not written. */
static ovBucket *ovBucketCreate(int didx, kvstoreDictOverlay *ov, int table, unsigned long idx, const kvstoreOvWindow *w) {
    dict *d = ov->d;
    assert(w->table == table && idx - w->lo < w->n);
    if (!ov->paused) {   /* the buckets are named by (table, idx): the layout holds still while any is overlayed */
        dictPauseRehashing(d);
        dictPauseAutoResize(d);
        ov->paused = 1;
        ov->paused_at = ovNowUs();
        ov_stats.open++;
        ov_stats.open_since_sum += ov->paused_at;
    }
    ovSlot *s = ovSlotFor(ov, w);
    ovBucket *ob = zcalloc(sizeof(*ob));
    ob->ov = ov;
    ob->slot = (int)(s - ov_index);
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
        ov_stats.s.clones++;
    }
    unsigned long off = idx - s->lo;
    s->b[off] = ob;
    s->bits[off >> 6] |= 1ULL << (off & 63);
    s->count++;
    ov->nb++;
    ov_stats.s.buckets++;
    if (++ov_stats.cur > ov_stats.s.peak) ov_stats.s.peak = ov_stats.cur;
    return ob;
}

/* ==========================================================================
 * Attach, write sections
 * ========================================================================== */

/* Start of a main-thread write on a frozen dict (see kvstoreOverlayEnterFn). */
static inline void ovEnter(kvstoreDictOverlay *ov) {
    if (ov->h->enter) ov->h->enter(ov->privdata);
}

/* May the frozen chain of 'key' (in table 'table') be written in place? If
 * not, *w is the window the bucket is deferred in. Must be called after
 * ovEnter() (the answer is only stable inside a write section). Without a
 * prepare hook the overlay is pure copy-on-write, one window per table. */
static inline int ovPrepare(int didx, kvstoreDictOverlay *ov, const void *key, int table, kvstoreOvWindow *w) {
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

/* What a write to 'key' of a frozen dict works on. */
enum { OV_W_INPLACE = 0, OV_W_BUCKET = 1, OV_W_ABSENT = 2 };

/* Write intent on the bucket of 'key' (inside the write section): the dict
 * itself when the snapshot holds the bucket (OV_W_INPLACE), else the bucket's
 * overlay (OV_W_BUCKET), created now when the scan thread may be reading the
 * chain. With 'miss_ok' false, a key the dict does not hold and no overlay
 * covers is OV_W_ABSENT and nothing is created. */
static int ovWriteBucket(kvstore *kvs, int didx, kvstoreDictOverlay *ov, const void *key, int miss_ok, ovBucket **out) {
    (void)kvs;
    dict *d = ov->d;
    /* The bucket of the key must not move while it is decided on (a lookup
     * steps a rehash): the dict's layout is held for the whole decision, and
     * for as long as any bucket is overlayed (ovBucketCreate). */
    dictPauseRehashing(d);
    int t, w;
    unsigned long i;
    dictKeyBucket(d, key, &t, &i);
    ovBucket *ob = ovLookup(ov, t, i);
    kvstoreOvWindow win;
    if (ob) {
        *out = ob;
        w = OV_W_BUCKET;
    } else if (!miss_ok && dictFind(d, key) == NULL) {
        w = OV_W_ABSENT;
    } else if (ovPrepare(didx, ov, key, t, &win) == KVSTORE_OV_INPLACE) {
        w = OV_W_INPLACE;
    } else {
        *out = ovBucketCreate(didx, ov, t, i, &win);
        w = OV_W_BUCKET;
    }
    dictResumeRehashing(d);
    return w;
}

/* Logical size of dict didx (frozen or not). */
unsigned long ovDictSize(kvstore *kvs, int didx) {
    dict *d = kvstoreGetDict(kvs, didx);
    unsigned long n = d ? dictSize(d) : 0;
    kvstoreDictOverlay *ov = ovOf(kvs, didx);
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
void ovDetachFromStore(dict *d) {
    kvstoreDictOverlay *ov = ((kvstoreDictMetaBase *)dictMetadata(d))->ov;
    if (ov) ov->kvs = NULL;
}

/* Owner thread: the scan has passed every position below 'below', so the
 * windows whose last bucket is below it are behind it with all their buckets. */
long kvstoreOverlayDrain(uint64_t below) {
    for (int k = 0; k < ov_nslots; k++)
        if (ov_index[k].ov && ov_index[k].last < below) ovSlotWriteBack(&ov_index[k]);
    return ov_stats.cur;
}

int kvstoreOverlaySlotsInUse(void) {
    return ov_slots_used;
}

long kvstoreOverlayPending(void) {
    return ov_stats.cur;
}

/* Owner thread, after the scan of this dict is complete: write back whatever
 * is left and drop the overlay. */
static void ovRelease(kvstoreDictOverlay *ov) {
    for (int k = 0; k < ov_nslots; k++)   /* written to the dict itself; its holder decides what becomes of it */
        if (ov_index[k].ov == ov) ovSlotWriteBack(&ov_index[k]);
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
    ovRelease(ov);
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
static ovBucket *ovNextBare(kvstoreDictOverlay *ov, int *slot, long *pos) {
    for (; *slot < ov_nslots; (*slot)++, *pos = 0) {
        ovSlot *s = &ov_index[*slot];
        if (s->ov != ov) continue;
        for (unsigned long off = ovSlotNext(s, (unsigned long)*pos); off < s->n; off = ovSlotNext(s, off + 1)) {
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
typedef struct { int k0, k, begun, wrapped; unsigned long start, off; } ovRandomWalk;
static void ovRandomWalkInit(ovRandomWalk *rw) {
    memset(rw, 0, sizeof(*rw));
    rw->k0 = ov_nslots ? (int)(random() % ov_nslots) : 0;
}
static ovBucket *ovRandomNext(kvstoreDictOverlay *ov, ovRandomWalk *rw) {
    for (; rw->k < ov_nslots; rw->k++, rw->begun = 0) {
        ovSlot *s = &ov_index[(rw->k0 + rw->k) % ov_nslots];
        if (s->ov != ov) continue;
        if (!rw->begun) {
            rw->begun = 1;
            rw->wrapped = 0;
            rw->start = rw->off = (unsigned long)random() % s->n;
        }
        for (;;) {
            unsigned long off = ovSlotNext(s, rw->off);
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

struct ovScanCtx { kvstoreDictOverlay *ov; dictScanFunction *fn; void *priv; ovBucket *last; };
static void ovScanCb(void *p, const dictEntry *de, dictEntry **plink) {
    (void)plink;
    struct ovScanCtx *c = p;
    ovBucket *ob = ovOriginalOf(c->ov, (dictEntry *)de);
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
unsigned long long ovScan(kvstore *kvs, kvstoreDictOverlay *ov, dict *d, unsigned long long cursor,
                          dictScanFunction *scan_cb, void *privdata)
{
    (void)kvs;
    struct ovScanCtx sctx = { ov, scan_cb, privdata, NULL };
    unsigned long long next = (d && dictSize(d)) ? dictScan(d, cursor, ovScanCb, &sctx) : 0;
    if (next == 0) {
        int slot = 0;
        long pos = 0;
        for (ovBucket *ob; (ob = ovNextBare(ov, &slot, &pos)) != NULL;) {
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
static int ovHandedOut(kvstoreDictOverlay *ov, ovBucket *ob, int n, dictEntry *de) {
    dictCmpCache cache = {0};
    const void *k = ovEntryKey(ov, de);
    dictEntry *e = ob->orig_head;
    for (int p = 0; p < n && e; p++, e = dictGetNext(e)) {
        const void *o = ovStoredKey2Key(ov, dictGetKey(e));
        if (o == k || (ovType(ov)->keyCompare ? ovType(ov)->keyCompare(&cache, k, o) : 0)) return 1;
    }
    return 0;
}

/* One step of kvstoreIteratorNext / kvstoreDictIteratorNext over a frozen
 * dict with overlayed buckets. Phase 0: 'di' iterates the dict; an original
 * of an overlayed bucket is not handed out, its bucket's chain is instead
 * (phase 1, 'cur'), once. Phase 2: the buckets the dict has no entry for.
 * NULL once everything is exhausted. */
dictEntry *ovIteratorNext(kvstoreDictOverlay *ov, dictIterator *di, int *phase, ovCursor *cur) {
    for (;;) {
        if (*phase != 0) {
            dictEntry *de = cur->next;
            if (de) {
                cur->next = dictGetNext(de);   /* fetched ahead: the caller may delete what it got */
                if (*phase == 1 && cur->skipn &&
                    ovHandedOut(ov, ovLookup(ov, (int)(cur->bucket & 1), (unsigned long)cur->bucket >> 1), cur->skipn, de))
                    continue;
                return de;
            }
            if (*phase == 1) { *phase = 0; continue; }
            /* Phase 2: the next bucket without originals. */
            ovBucket *ob = ovNextBare(ov, &cur->slot, &cur->bucket);
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
        ovBucket *ob = ovLookup(ov, di->table, di->index);
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
static dictEntry *ovRandomChainHead(kvstoreDictOverlay *ov) {
    ovRandomWalk rw;
    ovRandomWalkInit(&rw);
    for (ovBucket *ob; (ob = ovRandomNext(ov, &rw)) != NULL;)
        if (ob->chain) return ob->chain;
    return NULL;
}

/* Random entry of a frozen dict: sample the real dict (skipping the originals
 * of overlayed buckets) and fall back to the overlayed chains. */
dictEntry *ovRandomEntry(kvstore *kvs, int didx, kvstoreDictOverlay *ov, int fair) {
    dict *d = ov->d;
    if (ovDictSize(kvs, didx) == 0) return NULL;
    if (d && dictSize(d)) {
        for (int tries = 0; tries < 16; tries++) {
            dictEntry *de = fair ? dictGetFairRandomKey(d) : dictGetRandomKey(d);
            if (!de) break;
            if (!ovOriginalOf(ov, de)) return de;
        }
    }
    dictEntry *de = ovRandomChainHead(ov);
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
unsigned int ovGetSomeKeys(kvstoreDictOverlay *ov, dictEntry **des, unsigned int count) {
    unsigned int n = 0;
    dict *d = ov->d;
    if (d && dictSize(d)) {
        unsigned int got = dictGetSomeKeys(d, des, count);
        for (unsigned int i = 0; i < got; i++)
            if (!ovOriginalOf(ov, des[i])) des[n++] = des[i];
    }
    if (n < count && ov->nb) {
        ovRandomWalk rw;
        ovRandomWalkInit(&rw);
        for (ovBucket *ob; n < count && (ob = ovRandomNext(ov, &rw)) != NULL;)
            for (dictEntry *te = ob->chain; te && n < count; te = dictGetNext(te)) des[n++] = te;
    }
    return n;
}

/* ==========================================================================
 * Writes through the overlay
 * ========================================================================== */

/* Overlay (frozen slot) halves of the hot kvstore entry points: out of line so
 * the common path stays as small as it was and keeps being inlined. */
dictEntry *ovDictFind(kvstoreDictOverlay *ov, void *key) {
    /* Read: the logical keyspace, no copying. */
    int t;
    unsigned long i;
    dictKeyBucket(ov->d, key, &t, &i);
    ovBucket *ob = ovLookup(ov, t, i);
    if (!ob) return dictFind(ov->d, key);
    dictEntryLink link = ovChainFind(ov, ob, key);
    return link ? *link : NULL;
}

/* Write-intent lookup (see kvstoreDictFindLink in kvstore.c for the contract). */
dictEntryLink ovDictFindLink(kvstore *kvs, int didx, kvstoreDictOverlay *ov, void *key, dictEntryLink *bucket) {
    ovEnter(ov);
    ovBucket *ob;
    if (ovWriteBucket(kvs, didx, ov, key, 1, &ob) == OV_W_INPLACE) return dictFindLink(ov->d, key, bucket);
    /* The overlayed bucket: a miss returns its chain head so the insertion lands there. */
    if (bucket) *bucket = &ob->chain;
    return ovChainFind(ov, ob, key);
}

void ovDictSetAtLink(kvstore *kvs, int didx, kvstoreDictOverlay *ov, void *kv, dictEntryLink *link, int newItem) {
    ovEnter(ov);
    dict *d = ov->d;
    const void *key = kv ? ovStoredKey2Key(ov, kv) : kv;
    if (!newItem) {
        /* Replacing the key of an existing entry: with a link it is a store
         * through the link (the dict only lends its type); without one, look
         * the key up in the overlay first, then the dict. */
        dictEntryLink l = link ? *link : NULL;
        if (!l) {
            int t;
            unsigned long i;
            dictKeyBucket(d, key, &t, &i);
            ovBucket *ob = ovLookup(ov, t, i);
            if (ob) {
                l = ovChainFind(ov, ob, key);
                if (l && link) *link = l;
            }
        }
        dictSetKeyAtLink(d, kv, l ? &l : link, 0);
        return;
    }
    /* New item: a bucket link tells the dict (the dict's buckets come from an
     * INPLACE write lookup of this same section); without one decide now. */
    ovBucket *ob = NULL;
    int in_dict;
    if (link && *link) {
        in_dict = ovBucketInDict(d, *link);
        if (!in_dict) {
            int t;
            unsigned long i;
            dictKeyBucket(d, key, &t, &i);
            ob = ovLookup(ov, t, i);
            assert(ob != NULL);
        }
    } else {
        in_dict = ovWriteBucket(kvs, didx, ov, key, 1, &ob) == OV_W_INPLACE;
    }
    if (in_dict) {
        dictSetKeyAtLink(d, kv, link, 1);   /* no expand: auto-resize is paused */
        ovKeyCountAdd(kvs, didx, 1);
    } else {
        ovChainPush(ov, ob, kv);
        ov->delta++;
        ovKeyCountAdd(kvs, didx, 1);
        if (link) *link = &ob->chain;   /* the inserted entry's link, as dictSetKeyAtLink reports it */
    }
}

dictEntry *ovDictAddRaw(kvstore *kvs, int didx, kvstoreDictOverlay *ov, void *key, dictEntry **existing) {
    ovEnter(ov);
    const void *lkey = ovStoredKey2Key(ov, key);
    ovBucket *ob;
    if (ovWriteBucket(kvs, didx, ov, lkey, 1, &ob) == OV_W_INPLACE) {
        dictEntry *ret = dictAddRaw(ov->d, key, existing);
        if (ret) ovKeyCountAdd(kvs, didx, 1);
        return ret;
    }
    dictEntryLink link = ovChainFind(ov, ob, lkey);
    if (link) { if (existing) *existing = *link; return NULL; }
    if (ov->d->type->keyDup) key = ov->d->type->keyDup(ov->d, key);
    dictEntry *de = ovChainPush(ov, ob, key);
    ov->delta++;
    ovKeyCountAdd(kvs, didx, 1);
    return de;
}

dictEntryLink ovDictTwoPhaseUnlinkFind(kvstore *kvs, int didx, kvstoreDictOverlay *ov, const void *key, int *table_index) {
    ovEnter(ov);
    ov->unlink_frozen = 0;
    ov->unlink_ob = NULL;
    ovBucket *ob;
    int w = ovWriteBucket(kvs, didx, ov, key, 0, &ob);
    if (w == OV_W_ABSENT) return NULL;
    if (w == OV_W_INPLACE) {
        dictEntryLink l = dictTwoPhaseUnlinkFind(ov->d, key, table_index);
        if (l) ov->unlink_frozen = 1;
        return l;
    }
    dictEntryLink link = ovChainFind(ov, ob, key);
    if (!link) return NULL;
    ov->unlink_ob = ob;
    *table_index = 0;
    return link;
}

/* As above, with the key given explicitly: callers that already detached the
 * value from the entry (dbGenericDelete's async path stores NULL at the link
 * before unlinking) cannot have it read back from the entry. 'key' may be
 * NULL when the entry still holds it. */
void ovDictTwoPhaseUnlinkFreeKey(kvstore *kvs, int didx, kvstoreDictOverlay *ov, dictEntryLink link, int table_index, const void *key) {
    (void)key;
    ovEnter(ov);
    if (ov->unlink_frozen) {
        /* Second phase of an in-place unlink (see ovDictTwoPhaseUnlinkFind). */
        ov->unlink_frozen = 0;
        dictTwoPhaseUnlinkFree(ov->d, link, table_index);
        ovKeyCountAdd(kvs, didx, -1);
        return;
    }
    if (link && *link) {
        ovBucket *ob = ov->unlink_ob;
        assert(ob != NULL);
        ov->unlink_ob = NULL;
        dictEntry *de = ovChainUnlink(ob, link);
        ov->delta--;
        ovKeyCountAdd(kvs, didx, -1);
        ovEntryFreeWithKey(ov, de);
    }
}

int ovDictDelete(kvstore *kvs, int didx, kvstoreDictOverlay *ov, const void *key) {
    ovEnter(ov);
    ovBucket *ob;
    int w = ovWriteBucket(kvs, didx, ov, key, 0, &ob);
    if (w == OV_W_ABSENT) return DICT_ERR;
    if (w == OV_W_INPLACE) {
        int ret = dictDelete(ov->d, key);
        if (ret == DICT_OK) ovKeyCountAdd(kvs, didx, -1);
        return ret;
    }
    dictEntryLink link = ovChainFind(ov, ob, key);
    if (!link) return DICT_ERR;
    dictEntry *de = ovChainUnlink(ob, link);
    ov->delta--;
    ovKeyCountAdd(kvs, didx, -1);
    ovEntryFreeWithKey(ov, de);
    return DICT_OK;
}
