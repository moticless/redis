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

/* Snapshot overlay (hybrid BGSAVE): an iterator's position in a frozen dict's
 * side table while it enumerates the keys the overlay added (phase 1). */
typedef struct kvstoreOvCursor { int slot, chain; dictEntry *next; } kvstoreOvCursor;

/* Structure for kvstore iterator that allows iterating across multiple dicts. */
typedef struct _kvstoreIterator {
    kvstore *kvs;
    long long didx;
    long long next_didx;
    dictIterator di;
    int ov_phase;               /* snapshot overlay: 0 = frozen dict, 1 = side table (ADDs) */
    kvstoreOvCursor ov_cur;     /* side-table position in phase 1 */
} kvstoreIterator;

/* Structure for kvstore dict iterator that allows iterating the corresponding dict. */
typedef struct _kvstoreDictIterator {
    kvstore *kvs;
    long long didx;
    dictIterator di;
    int ov_phase;               /* snapshot overlay: 0 = frozen dict, 1 = side table (ADDs) */
    kvstoreOvCursor ov_cur;     /* side-table position in phase 1 */
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
 * kvstoreOverlayRelease() applies whatever is left and unfreezes the dict
 * once its scan is complete. The overlay hangs off the dict's metadata
 * (kvstoreDictMetaBase.ov). The side table is a fixed array of per-bucket chains
 * of ordinary dict entries: a key's chain is its hash modulo the
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
/* Snapshot guard: while a scan runs over the store, every access to a dict
 * that has no overlay yet asks the snapshot first (write = 1 for structural
 * writes). The guard may attach an overlay (kvstoreOverlayAttach) and return
 * it, or return NULL when the dict needs none (its scan is complete, or a
 * read on a dict the scan thread is not reading). A NULL fn turns the guard
 * off; the fast path is then one load. */
typedef struct kvstoreDictOverlay *(kvstoreSnapGuardFn)(void *privdata, kvstore *kvs, int didx, int write);
/* The other snapshot hooks of a scanned store: a dict
 * started or finished an incremental rehash (main thread, inside the dict
 * operation; both tables are set up at 'started'), and whether a dict may
 * shrink right now (a snapshot bitmap follows a split but not a merge). */
typedef void (kvstoreSnapRehashFn)(void *privdata, int didx, dict *d);
typedef int (kvstoreSnapShrinkAllowedFn)(void *privdata, int didx);
/* The snapshot's hooks on a store, one struct and one privdata: the guard and
 * the layout hooks the store consults on every dict, and the overlay
 * callbacks a frozen dict's side table calls (clone, free_orig, enter,
 * prepare, drainable). The struct must outlive every overlay attached while
 * it was installed (a detached dict's overlay keeps pointing at it). */
typedef struct kvstoreSnapHooks {
    kvstoreSnapGuardFn *guard;
    kvstoreSnapRehashFn *rehash_started;
    kvstoreSnapRehashFn *rehash_completed;
    kvstoreSnapShrinkAllowedFn *shrink_allowed;
    kvstoreOverlayCloneFn *clone;
    kvstoreOverlayFreeOrigFn *free_orig;
    kvstoreOverlayEnterFn *enter;
    kvstoreOverlayPrepareFn *prepare;
    kvstoreOverlayDrainableFn *drainable;
} kvstoreSnapHooks;
/* NULL hooks turn the snapshot off for the store. While hooks are set, every
 * access to a dict of the store that meets no overlay asks the guard, and
 * the overlays attached meanwhile call back with the same privdata. */
void kvstoreSnapGuardSet(kvstore *kvs, const kvstoreSnapHooks *hooks, void *privdata);
/* References. A dict is owned by its store (one reference from
 * creation) and may be held by a running save (one more, taken at freeze
 * for every fork-instant dict). A store that frees, empties or moves a dict
 * another holder references detaches it instead: the dict leaves the slot
 * table and the store's bookkeeping, keeps its save state, and is released
 * by the last holder. Every dict holds a reference on its store (its
 * dictType lives there), so a store struct outlives its detached dicts.
 * The counts are atomic: the lazyfree thread releases stores. */
dict *kvstoreDictRetain(kvstore *kvs, int didx);   /* the dict of slot didx with one more reference, NULL if none */
void kvstoreDictUnref(dict *d);                     /* drop one; the dict is released at zero */
int kvstoreDictRefcount(dict *d);
int kvstoreDictDetached(dict *d);                   /* out of its store's slot table, kept by another holder */
int kvstoreRefcount(kvstore *kvs);                  /* the owner plus one per allocated dict */
/* The save state of a dict: allocate it, or release it (side table applied,
 * bitmaps freed and their bytes returned, layout resumed). */
void kvstoreOverlayRelease(dict *d);   /* the dict's scan is complete or the save aborted: side table applied, overlay gone */
void kvstoreOverlayAttach(kvstore *kvs, int didx);   /* the store's hooks and privdata serve the overlay */
int kvstoreOverlayIsFrozen(kvstore *kvs, int didx);   /* overlay attached */
long kvstoreOverlayDrain(kvstore *kvs);            /* every frozen dict with a non-empty side table; returns what is left */
long kvstoreOverlayPending(kvstore *kvs);          /* side-table entries (clones, adds, tombstones) across the store */

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

/* Basic metadata allocated per dict.
 * If additional metadata needed, embed this structure as the first member
 * in a new, larger structure. */
typedef struct {
    listNode *rehashing_node;   /* list node in rehashing list */
    int didx;                   /* the dict's index in its kvstore (for the snapshot hooks) */
    /* The extension slot: owned by whatever extends the store's dicts (today the
     * hybrid BGSAVE's save state, snapshot.c). The store never reads these two
     * fields; the extension stamps the word and allocates and frees the state,
     * with __atomic builtins where two threads meet. 0 / NULL when none. */
    uint64_t ext_word;
    void *ext;
    struct kvstoreDictOverlay *ov;  /* the dict's side table while it is frozen (kvsoverlay.c), else NULL;
                                   main thread stores (release), scan thread loads (acquire) */
    int refcount;               /* holders: the owning store, a running save; atomic */
    int detached;               /* out of the store's slot table, kept by a holder: the dict callbacks
                                   skip the store's bookkeeping for it */
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

#endif /* DICTARRAY_H_ */
