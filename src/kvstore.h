/*
 * Copyright (c) 2009-Present, Redis Ltd.
 * All rights reserved.
 *
 * Copyright (c) 2024-present, Valkey contributors.
 * All rights reserved.
 *
 * Licensed under your choice of (a) the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 *
 * KVSTORE
 * -------
 * Index-based KV store implementation. This file implements a KV store comprised
 * of an array of dicts (see dict.c) The purpose of this KV store is to have easy
 * access to all keys that belong in the same dict (i.e. are in the same dict-index)
 *
 * For example, when Redis is running in cluster mode, we use kvstore to save
 * all keys that map to the same hash-slot in a separate dict within the kvstore
 * struct.
 * This enables us to easily access all keys that map to a specific hash-slot.
 *
 * Portions of this file are available under BSD3 terms; see REDISCONTRIBUTIONS for more information.
 */

#ifndef DICTARRAY_H_
#define DICTARRAY_H_

#include "dict.h"
#include "adlist.h"

typedef struct _kvstore kvstore;

/* Structure for kvstore iterator that allows iterating across multiple dicts. */
typedef struct _kvstoreIterator {
    kvstore *kvs;
    long long didx;
    long long next_didx;
    dictIterator di;
    int ov_phase;               /* snapshot overlay: 0 = frozen dict, 1 = side table (ADDs) */
    struct { int slot, chain; dictEntry *next; } ov_cur;   /* side-table position in phase 1 */
} kvstoreIterator;

/* Structure for kvstore dict iterator that allows iterating the corresponding dict. */
typedef struct _kvstoreDictIterator {
    kvstore *kvs;
    long long didx;
    dictIterator di;
    int ov_phase;               /* snapshot overlay: 0 = frozen dict, 1 = side table (ADDs) */
    struct { int slot, chain; dictEntry *next; } ov_cur;   /* side-table position in phase 1 */
} kvstoreDictIterator;

/* ---- Snapshot overlay: the in-window side table (hybrid BGSAVE) -------------
 *
 * While a dict is "frozen" (owed to a running snapshot scan) its bucket layout
 * is fixed and every write first asks the snapshot ('prepare') whether the
 * chain may be written in place. When it may not (the scan thread is reading
 * that bucket right now) the write goes to the dict's side table instead: the
 * first write-lookup of such a key calls 'clone' to obtain a private copy that
 * is stored in the side table and returned to the caller, so all in-place
 * mutation done by callers lands on the clone; deletes leave a tombstone; adds
 * land in the side table. While the side table is non-empty, reads, sizes and
 * enumeration describe the logical keyspace (frozen minus tombstones, clones
 * substituted, plus adds); while it is empty they are plain dict reads.
 * kvstoreOverlayDrain() applies the side table to the real dict on the owner
 * thread for every key whose bucket the snapshot no longer reads ('drainable'),
 * kvstoreDictSnapRelease() applies whatever is left and unfreezes the dict
 * once its scan is complete. The overlay hangs off the dict's save state
 * (kvstoreDictSnap.ov). The side table is a fixed array of per-bucket chains
 * of ordinary dict entries (TASK-1.21.9): a key's chain is its hash modulo the
 * array size, four chains per slot tell a clone whose value is still the
 * original's, a deep clone or add, and the tombstones of each kind apart. */
typedef void *(kvstoreOverlayCloneFn)(void *privdata, int didx, void *orig, int *value_shared);
typedef void (kvstoreOverlayFreeOrigFn)(void *privdata, int didx, void *orig, int value_shared);
/* Called at the start of every main-thread write operation on a frozen dict
 * (before the overlay decides between in-place and side-table paths). Optional. */
typedef void (kvstoreOverlayEnterFn)(void *privdata);
/* Called before a structural write (add, unlink, replace) of 'key' in frozen
 * dict 'd', after 'enter'. KVSTORE_OV_INPLACE: the snapshot already holds
 * this bucket (the callee may have just emitted it), the frozen chain may be
 * written in place. KVSTORE_OV_DEFER: the snapshot thread is reading the
 * bucket right now, the write must go to the side table. Optional (absent:
 * always DEFER, i.e. pure clone-on-write). */
#define KVSTORE_OV_DEFER   0
#define KVSTORE_OV_INPLACE 1
typedef int (kvstoreOverlayPrepareFn)(void *privdata, int didx, dict *d, const void *key);
/* May the side-table entry of 'key' be applied to the frozen chain now, i.e.
 * will the snapshot thread never read that bucket again? Optional (absent:
 * nothing drains before kvstoreOverlayRelease). */
typedef int (kvstoreOverlayDrainableFn)(void *privdata, int didx, dict *d, const void *key);
typedef struct kvstoreOverlayHooks {
    kvstoreOverlayCloneFn *clone;
    kvstoreOverlayFreeOrigFn *free_orig;
    kvstoreOverlayEnterFn *enter;
    kvstoreOverlayPrepareFn *prepare;
    kvstoreOverlayDrainableFn *drainable;
} kvstoreOverlayHooks;
/* Snapshot guard: while a scan runs over the store, every access to a dict
 * that has no overlay yet asks the snapshot first (write = 1 for structural
 * writes). The guard may attach an overlay (kvstoreOverlayAttach) and return
 * it, or return NULL when the dict needs none (its scan is complete, or a
 * read on a dict the scan thread is not reading). A NULL fn turns the guard
 * off; the fast path is then one load. */
typedef struct kvstoreDictOverlay *(kvstoreSnapGuardFn)(void *privdata, kvstore *kvs, int didx, int write);
/* The other snapshot hooks of a scanned store (TASK-1.21.7 stage B): a dict
 * started or finished an incremental rehash (main thread, inside the dict
 * operation; both tables are set up at 'started'), and whether a dict may
 * shrink right now (a snapshot bitmap follows a split but not a merge). */
typedef void (kvstoreSnapRehashFn)(void *privdata, int didx, dict *d);
typedef int (kvstoreSnapShrinkAllowedFn)(void *privdata, int didx);
/* A dict was just allocated in a store under a save (main thread, before the
 * dict is published): it holds nothing of the fork instant. */
typedef void (kvstoreSnapDictCreatedFn)(void *privdata, int didx, dict *d);
typedef struct kvstoreSnapHooks {
    kvstoreSnapGuardFn *guard;
    kvstoreSnapRehashFn *rehash_started;
    kvstoreSnapRehashFn *rehash_completed;
    kvstoreSnapShrinkAllowedFn *shrink_allowed;
    kvstoreSnapDictCreatedFn *dict_created;
} kvstoreSnapHooks;
/* NULL hooks turn the snapshot off for the store. 'gen' is the save's
 * generation (never 0): the per-dict save state (kvstoreDictSnap, and the
 * state word next to it) is valid only while the dict's stamp equals it.
 * State met under another generation is a leftover of an earlier save (a
 * release path that did not run): it is released on the spot and counted in
 * kvstoreSnapLeftovers(). While hooks are set, empty dicts are not freed
 * (the scan thread reads dict pointers of the store); the deferred frees run
 * when the hooks come off. */
void kvstoreSnapGuardSet(kvstore *kvs, const kvstoreSnapHooks *hooks, void *privdata, uint64_t gen);
long kvstoreSnapLeftovers(kvstore *kvs);
/* The save state of dict didx: allocate it (stamped by the caller first) or
 * release it (bitmaps, overlay, pause). How many dicts of the store have
 * one, for the sweep that ends an aborted save (and the assertion that a
 * completed store has none). */
struct kvstoreDictSnap *kvstoreDictSnapAttach(kvstore *kvs, int didx);
void kvstoreDictSnapRelease(kvstore *kvs, int didx);
int kvstoreSnapAttached(kvstore *kvs);
/* Bytes of bucket bitmaps attached to the store's dicts: the snapshot
 * accounts what it allocates, the release subtracts what it frees. */
void kvstoreSnapBitmapAccount(kvstore *kvs, long delta);
size_t kvstoreSnapBitmapBytes(kvstore *kvs);
/* The dict pointer with acquire semantics, for the scan thread (the main
 * thread publishes new dicts with release). */
dict *kvstoreGetDictAcquire(kvstore *kvs, int didx);
void kvstoreOverlayAttach(kvstore *kvs, int didx, const kvstoreOverlayHooks *hooks, void *privdata);   /* needs the save state attached */
int kvstoreOverlayIsFrozen(kvstore *kvs, int didx);   /* overlay attached */
long kvstoreOverlayDrain(kvstore *kvs);            /* every frozen dict with a non-empty side table; returns what is left */
long kvstoreOverlayPending(kvstore *kvs);          /* side-table entries (clones, adds, tombstones) across the store */
void kvstoreOverlayStats(kvstore *kvs, int didx, long *side_entries, long *adds, long *tombstones);

typedef int (kvstoreScanShouldSkipDict)(dict *d, int didx);
typedef int (kvstoreExpandShouldSkipDictIndex)(int didx);
typedef int (kvstoreRandomShouldSkipDictIndex)(int didx);
typedef int (kvstoreResizeShouldSkipDictIndex)(int didx);

/* kvstoreType: Callback structure for kvstore-specific operations.
 * Similar to dictType for dict, this allows kvstore to be a generic data structure
 * without hardcoding dependencies on specific subsystems. */
typedef struct kvstoreType {
    /* Allow kvstore to carry extra caller-defined metadata. The
     * extra memory is initialized to 0 when a kvstore is allocated. */
    size_t (*kvstoreMetadataBytes)(kvstore *kvs);

    /* Allow a per slot dicts to carry extra caller-defined metadata. The
     * extra memory is initialized to 0 when each dict is allocated. */
    size_t (*dictMetadataBytes)(dict *d);

    /* Check if a dict at given index can be freed. Used by kvstore when
     * KVSTORE_FREE_EMPTY_DICTS is set. Return 1 if can free, 0 otherwise.
     * Parameters: kvstore pointer, dict index */
    int (*canFreeDict)(kvstore *kvs, int didx);

    /* Called when kvstore becomes empty. */
    void (*onKvstoreEmpty)(kvstore *kvs);

    /* Called when per slot dict becomes empty. Parameters: kvstore pointer,
     * dict index. */
    void (*onDictEmpty)(kvstore *kvs, int didx);
} kvstoreType;

/* What a running snapshot save (hybrid BGSAVE) keeps per dict, allocated the
 * first time the save touches the dict (a write before the scan thread
 * reached it, or the thread's window on it) and freed when the dict's scan
 * completes. Meaningful only while the dict's stamp (kvstoreDictMetaBase
 * snap_word) carries the running save's generation. */
typedef struct kvstoreDictSnap {
    uint8_t *bits[2];           /* per table: one bit per bucket, set when the main thread emitted the
                                   bucket's fork-instant chain ahead of the scan thread (snapshot.c);
                                   main thread stores (release), scan thread loads (acquire) */
    size_t bytes[2];
    struct kvstoreDictOverlay *ov;  /* side table while writes meet the thread's window, else NULL */
    int paused;                 /* rehash and auto-resize paused for the thread's read of the layout */
} kvstoreDictSnap;

/* Basic metadata allocated per dict.
 * If additional metadata needed, embed this structure as the first member
 * in a new, larger structure. */
typedef struct {
    listNode *rehashing_node;   /* list node in rehashing list */
    int didx;                   /* the dict's index in its kvstore (for the snapshot hooks) */
    uint64_t snap_word;         /* (save generation << 8) | per-save state (snapshot.h SNAP_SLOT_*);
                                   written by both threads with __atomic builtins. A stamp of another
                                   generation means this save has not touched the dict. */
    kvstoreDictSnap *snap;      /* NULL until the save touches the dict; main thread stores (release),
                                   scan thread loads (acquire) */
} kvstoreDictMetaBase;

#define KVSTORE_ALLOCATE_DICTS_ON_DEMAND (1<<0)
#define KVSTORE_FREE_EMPTY_DICTS (1<<1)
kvstore *kvstoreCreate(kvstoreType *type, dictType *dtype, int num_dicts_bits, int flags);
void kvstoreEmpty(kvstore *kvs, void(callback)(dict*));
void kvstoreRelease(kvstore *kvs);
unsigned long long kvstoreSize(kvstore *kvs);
unsigned long kvstoreBuckets(kvstore *kvs);
size_t kvstoreMemUsage(kvstore *kvs);
unsigned long long kvstoreScan(kvstore *kvs, unsigned long long cursor,
                               int onlydidx, dictScanFunction *scan_cb,
                               kvstoreScanShouldSkipDict *skip_cb,
                               void *privdata);
int kvstoreExpand(kvstore *kvs, uint64_t newsize, int try_expand, kvstoreExpandShouldSkipDictIndex *skip_cb);
int kvstoreGetFairRandomDictIndex(kvstore *kvs, kvstoreExpandShouldSkipDictIndex *skip_cb,
                                  int fair_attempts, int slow_fallback);
void kvstoreGetStats(kvstore *kvs, char *buf, size_t bufsize, int full);

int kvstoreFindDictIndexByKeyIndex(kvstore *kvs, unsigned long target);
int kvstoreGetFirstNonEmptyDictIndex(kvstore *kvs);
int kvstoreGetNextNonEmptyDictIndex(kvstore *kvs, int didx);
int kvstoreNumNonEmptyDicts(kvstore *kvs);
int kvstoreNumAllocatedDicts(kvstore *kvs);
int kvstoreNumDicts(kvstore *kvs);
void kvstoreMoveDict(kvstore *kvs, kvstore *dst, int didx);

/* kvstore iterator specific functions */
void kvstoreIteratorInit(kvstoreIterator *kvs_it, kvstore *kvs);
void kvstoreIteratorReset(kvstoreIterator *kvs_it);
dict *kvstoreIteratorNextDict(kvstoreIterator *kvs_it);
int kvstoreIteratorGetCurrentDictIndex(kvstoreIterator *kvs_it);
dictEntry *kvstoreIteratorNext(kvstoreIterator *kvs_it);

/* Rehashing */
void kvstoreTryResizeDicts(kvstore *kvs, int limit, kvstoreResizeShouldSkipDictIndex *skip_cb);
uint64_t kvstoreIncrementallyRehash(kvstore *kvs, uint64_t threshold_us);
size_t kvstoreOverheadHashtableLut(kvstore *kvs);
size_t kvstoreOverheadHashtableRehashing(kvstore *kvs);
unsigned long kvstoreDictRehashingCount(kvstore *kvs);

/* Specific dict access by dict-index */
unsigned long kvstoreDictSize(kvstore *kvs, int didx);
void kvstoreInitDictIterator(kvstoreDictIterator *kvs_di, kvstore *kvs, int didx);
void kvstoreInitDictSafeIterator(kvstoreDictIterator *kvs_di, kvstore *kvs, int didx);
void kvstoreResetDictIterator(kvstoreDictIterator *kvs_di);
dictEntry *kvstoreDictIteratorNext(kvstoreDictIterator *kvs_di);
dictEntry *kvstoreDictGetRandomKey(kvstore *kvs, int didx);
dictEntry *kvstoreDictGetFairRandomKey(kvstore *kvs, int didx);
unsigned int kvstoreDictGetSomeKeys(kvstore *kvs, int didx, dictEntry **des, unsigned int count);
int kvstoreDictExpand(kvstore *kvs, int didx, unsigned long size);
unsigned long kvstoreDictScanDefrag(kvstore *kvs, int didx, unsigned long v, dictScanFunction *fn, dictDefragFunctions *defragfns, void *privdata);
typedef dict *(kvstoreDictLUTDefragFunction)(dict *d);
unsigned long kvstoreDictLUTDefrag(kvstore *kvs, unsigned long cursor, kvstoreDictLUTDefragFunction *defragfn);
void *kvstoreDictFetchValue(kvstore *kvs, int didx, const void *key);
dictEntry *kvstoreDictFind(kvstore *kvs, int didx, void *key);
dictEntry *kvstoreDictAddRaw(kvstore *kvs, int didx, void *key, dictEntry **existing);
dictEntryLink kvstoreDictTwoPhaseUnlinkFind(kvstore *kvs, int didx, const void *key, int *table_index);
void kvstoreDictTwoPhaseUnlinkFree(kvstore *kvs, int didx, dictEntryLink plink, int table_index);
void kvstoreDictTwoPhaseUnlinkFreeKey(kvstore *kvs, int didx, dictEntryLink plink, int table_index, const void *key);
int kvstoreDictDelete(kvstore *kvs, int didx, const void *key);
dict *kvstoreGetDict(kvstore *kvs, int didx);
void kvstoreFreeDictIfNeeded(kvstore *kvs, int didx);
void *kvstoreGetDictMeta(kvstore *kvs, int didx, int createIfNeeded);
void *kvstoreGetMetadata(kvstore *kvs);

dictEntryLink kvstoreDictFindLink(kvstore *kvs, int didx, void *key, dictEntryLink *bucket);
void kvstoreDictSetAtLink(kvstore *kvs, int didx, void *kv, dictEntryLink *link, int newItem);

/* dict with distinct key & value (no_value=1) currently is used only by pubsub. */
void kvstoreDictSetKey(kvstore *kvs, int didx, dictEntry* de, void *key);
void kvstoreDictSetVal(kvstore *kvs, int didx, dictEntry *de, void *val);

#ifdef REDIS_TEST
int kvstoreTest(int argc, char *argv[], int flags);
#endif


/* ==========================================================================
 * Internal to kvstore.c and kvsoverlay.c, not an API: the store itself, the
 * snapshot overlay types, the inline fast paths every dict access takes and
 * the overlay routines the two files exchange. Both files define
 * KVSTORE_INTERNAL before including this header; nothing else does.
 * ========================================================================== */
#ifdef KVSTORE_INTERNAL

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
    int snap_attached;                     /* Dicts with save state attached (kvstoreDictSnap in their metadata). */
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
    kvstore *kvs;               /* owner and slot */
    int didx;
    ovSlot *slots;              /* OV_SLOTS, NULL until the first side entry */
    long entries;               /* LIVE + SHARED entries (clones and adds) */
    long adds;                  /* LIVE entries whose key is not in the frozen dict */
    long tombs;                 /* TOMB + TOMB_SHARED entries */
    kvstoreOverlayHooks h;
    int unlink_frozen;          /* the pending two-phase unlink targets the frozen dict */
    listNode *dirty_node;       /* node in kvs->ov_dirty while the side table may be non-empty */
    void *privdata;
} kvstoreDictOverlay;

/* Side-table cursor: every LIVE and SHARED entry once, slot by slot. The next
 * entry is fetched before the current one is handed out, so a caller may
 * delete the entry it got (safe-iterator contract). */
typedef struct ovCursor { int slot; int chain; dictEntry *next; } ovCursor;

/* The save state of a dict lives in the dict's metadata (kvstoreDictSnap,
 * the overlay inside it). ovPeek: the attached overlay or NULL, no side
 * effects. ovGet: what a dict access must go through: the attached overlay,
 * or whatever the snapshot guard decides (it may attach one now). One load
 * when no snapshot runs; the dict pointer every caller loads anyway. */
static inline int snapAttached(kvstore *kvs) { return kvs->snap_attached; }

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
#define ovGetR(kvs, didx) ovGet(kvs, didx, 0)
#define ovGetW(kvs, didx) ovGet(kvs, didx, 1)

/* A non-empty side table: reads must consult it. Writes always go through
 * the overlay of a frozen dict (ovGet), whatever the side table holds. */
static inline int ovActive(kvstoreDictOverlay *ov) {
    return ov && (ov->entries || ov->tombs);
}

/* kvsoverlay.c, for the enumeration and the public wrappers in kvstore.c. */
unsigned long ovDictSize(kvstore *kvs, int didx);
dictEntry *ovMapFrozenEntry(kvstore *kvs, kvstoreDictOverlay *ov, dictEntry *de);
int ovSideEntryIsAdd(kvstore *kvs, int didx, dictEntry *te);
void ovCursorInit(kvstoreDictOverlay *ov, ovCursor *c);
dictEntry *ovCursorNext(kvstoreDictOverlay *ov, ovCursor *c);
struct ovScanCtx { kvstore *kvs; kvstoreDictOverlay *ov; dictScanFunction *fn; void *priv; };
void ovScanCb(void *p, const dictEntry *de, dictEntry **plink);
dictEntry *ovRandomEntry(kvstore *kvs, int didx, kvstoreDictOverlay *ov, int fair);
unsigned int ovGetSomeKeys(kvstore *kvs, int didx, kvstoreDictOverlay *ov, dictEntry **des, unsigned int count);
/* The overlay halves of the hot kvstore entry points: out of line so the
 * common path stays as small as it was and keeps being inlined. */
dictEntry *ovDictFind(kvstore *kvs, int didx, kvstoreDictOverlay *ov, void *key);
dictEntryLink ovDictFindLink(kvstore *kvs, int didx, kvstoreDictOverlay *ov, void *key, dictEntryLink *bucket);
void ovDictSetAtLink(kvstore *kvs, int didx, kvstoreDictOverlay *ov, void *kv, dictEntryLink *link, int newItem);
dictEntry *ovDictAddRaw(kvstore *kvs, int didx, kvstoreDictOverlay *ov, void *key, dictEntry **existing);
dictEntryLink ovDictTwoPhaseUnlinkFind(kvstore *kvs, int didx, kvstoreDictOverlay *ov, const void *key, int *table_index);
void ovDictTwoPhaseUnlinkFreeKey(kvstore *kvs, int didx, kvstoreDictOverlay *ov, dictEntryLink link, int table_index, const void *key);
int ovDictDelete(kvstore *kvs, int didx, kvstoreDictOverlay *ov, const void *key);

#endif /* KVSTORE_INTERNAL */

#endif /* DICTARRAY_H_ */
