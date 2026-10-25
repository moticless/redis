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
 * The hybrid snapshot (hybrid_snapshot.h) reads a store's dicts on a background
 * thread while the main thread keeps writing; the store's side of that contract,
 * the overlay, is kvstore_overlay.h. What stays here: the iterators' overlay
 * position, the dict metadata the overlay hangs off (kvstoreDictMetaBase) and dict
 * references (kvstoreDictRetain), by which a save keeps reading a dict its store
 * let go of (FLUSHDB, SWAPDB, a freed empty dict): the store detaches the dict
 * instead of freeing it and the last holder releases it.
 *
 * Portions of this file are available under BSD3 terms; see REDISCONTRIBUTIONS for more information.
 */

#ifndef DICTARRAY_H_
#define DICTARRAY_H_

#include "dict.h"
#include "adlist.h"

typedef struct _kvstore kvstore;

typedef struct kvstoreOvCursor {
    int slot, last, skipn;
    long bucket;
    dictEntry *next;
} kvstoreOvCursor;   /* snapshot overlay: iterator position in the overlay's bucket chains (phases 1, 2) */

/* Structure for kvstore iterator that allows iterating across multiple dicts. */
typedef struct _kvstoreIterator {
    kvstore *kvs;
    long long didx;
    long long next_didx;
    dictIterator di;
    int ov_phase;               /* snapshot overlay: 0 = frozen dict, 1 = a replaced bucket's chain, 2 = buckets the dict has no entry for */
    kvstoreOvCursor ov_cur;     /* overlay position in phases 1 and 2 */
} kvstoreIterator;

/* Structure for kvstore dict iterator that allows iterating the corresponding dict. */
typedef struct _kvstoreDictIterator {
    kvstore *kvs;
    long long didx;
    dictIterator di;
    int ov_phase;               /* snapshot overlay: 0 = frozen dict, 1 = a replaced bucket's chain, 2 = buckets the dict has no entry for */
    kvstoreOvCursor ov_cur;     /* overlay position in phases 1 and 2 */
} kvstoreDictIterator;

/* ---- Dict references (see KVSTORE OVERLAY above) ---------------------------- */

/* References: one from the store, one more per holder (a running save); a dict another holder references is detached, not freed. */
dict *kvstoreDictRetain(kvstore *kvs, int didx);
void kvstoreDictUnref(dict *d);
int kvstoreDictRefcount(dict *d);
int kvstoreDictDetached(dict *d);
int kvstoreRefcount(kvstore *kvs);

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
     * hybrid snapshot's save state, hybrid_snapshot.c). The store never reads these two
     * fields; the extension stamps the word and allocates and frees the state,
     * with __atomic builtins where two threads meet. 0 / NULL when none. */
    uint64_t ext_word;
    void *ext;
    struct kvstoreDictOverlay *ov;  /* the dict's overlay while it is frozen (kvstore_overlay.c), else NULL;
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
