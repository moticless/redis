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
 * KVSTORE OVERLAY
 * ---------------
 * The hybrid snapshot (snapshot.h) serializes the keyspace with a background
 * thread that reads the store's dicts while the main thread keeps writing.
 * The overlay is what makes that safe. While a scan runs over a store, a
 * snapshot guard is installed on it (kvstoreSnapGuardSet): every access to a
 * dict that has no overlay yet asks the guard first, which may attach one.
 *
 * A dict with an overlay is "frozen": its bucket layout is fixed (no resize,
 * rehash steps are reported to the snapshot) and every write first asks the
 * snapshot ('prepare') whether the chain may be written in place. When the
 * scan thread is reading that bucket right now, the write goes to the dict's
 * side table instead: the first write-lookup of such a key calls 'clone' for
 * a private copy that is stored in the side table and returned to the caller,
 * so in-place mutation by callers lands on the clone; deletes leave a
 * tombstone; adds land in the side table. While the side table is non-empty,
 * reads, sizes and enumeration describe the logical keyspace (frozen minus
 * tombstones, clones substituted, plus adds); while it is empty they are
 * plain dict reads, and a store without a guard pays one load per access.
 *
 * kvstoreOverlayDrain() applies the side table to the real dict, on the
 * owner thread, for every key whose bucket the snapshot no longer reads
 * ('drainable'); kvstoreOverlayRelease() applies whatever is left and
 * unfreezes the dict once its scan is complete. The overlay hangs off the
 * dict's metadata (kvstoreDictMetaBase.ov); the save's own per-dict state
 * (bucket bitmaps, pause) sits in the opaque extension slot next to it.
 *
 * Dicts are reference counted so a save can keep reading a dict its store
 * let go of (FLUSHDB, SWAPDB, a freed empty dict): the store detaches the
 * dict instead of freeing it and the last holder releases it. The
 * implementation is kvsoverlay.c (kvsoverlay.h is the private header the two
 * files share); the public entry points are in this file.
 *
 * Portions of this file are available under BSD3 terms; see REDISCONTRIBUTIONS for more information.
 */

#ifndef DICTARRAY_H_
#define DICTARRAY_H_

#include "dict.h"
#include "adlist.h"

typedef struct _kvstore kvstore;

typedef struct kvstoreOvCursor { int slot, chain; dictEntry *next; } kvstoreOvCursor;   /* snapshot overlay: iterator position in a side table (phase 1) */

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

/* ---- Snapshot overlay (hybrid snapshot; see KVSTORE OVERLAY above) ---------- */

/* Guard: a dict with no overlay is accessed (write = structural): attach one and return it, or NULL. */
typedef struct kvstoreDictOverlay *(kvstoreSnapGuardFn)(void *privdata, kvstore *kvs, int didx, int write);
/* Layout hooks: a dict started / finished an incremental rehash; may the dict shrink right now. */
typedef void (kvstoreSnapRehashFn)(void *privdata, int didx, dict *d);
typedef int (kvstoreSnapShrinkAllowedFn)(void *privdata, int didx);
/* Overlay callbacks: a private copy of a frozen key for the side table, and the release of a frozen original after the drain. */
typedef void *(kvstoreOverlayCloneFn)(void *privdata, int didx, void *orig, int *value_shared);
typedef void (kvstoreOverlayFreeOrigFn)(void *privdata, int didx, void *orig, int value_shared);
/* Start of every main-thread write on a frozen dict (optional). */
typedef void (kvstoreOverlayEnterFn)(void *privdata);
/* Before a structural write of 'key': may the frozen chain be written in place (optional: absent = DEFER). */
#define KVSTORE_OV_DEFER   0   /* the scan thread is reading the bucket: the write goes to the side table */
#define KVSTORE_OV_INPLACE 1   /* the snapshot holds the bucket (or just emitted it): write in place */
typedef int (kvstoreOverlayPrepareFn)(void *privdata, int didx, dict *d, const void *key);
/* May the side entry of 'key' be applied to the frozen chain now (optional: absent = only at release). */
typedef int (kvstoreOverlayDrainableFn)(void *privdata, int didx, dict *d, const void *key);
/* The snapshot's hooks on a store, one struct and one privdata; it must outlive every overlay attached while installed. */
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
/* Install the hooks on a store; NULL turns the snapshot off for it. */
void kvstoreSnapGuardSet(kvstore *kvs, const kvstoreSnapHooks *hooks, void *privdata);

/* References: one from the store, one more per holder (a running save); a dict another holder references is detached, not freed. */
dict *kvstoreDictRetain(kvstore *kvs, int didx);
void kvstoreDictUnref(dict *d);
int kvstoreDictRefcount(dict *d);
int kvstoreDictDetached(dict *d);
int kvstoreRefcount(kvstore *kvs);

/* The overlay of a dict: attach (the store's hooks serve it), is it attached, release (scan complete or save aborted: side table applied). */
void kvstoreOverlayAttach(kvstore *kvs, int didx);
int kvstoreOverlayIsFrozen(kvstore *kvs, int didx);
void kvstoreOverlayRelease(dict *d);
/* Across the store: apply every drainable side entry (returns what is left); count the side entries waiting. */
long kvstoreOverlayDrain(kvstore *kvs);
long kvstoreOverlayPending(kvstore *kvs);

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
     * hybrid snapshot's save state, snapshot.c). The store never reads these two
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
