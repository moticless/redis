/*
 * snapshot.c - Hybrid BGSAVE implementation (see poc-hybrid-bgsave-spec.md).
 *
 * This file currently provides the SNAP_NOFORK jemalloc arena: a dedicated
 * arena whose extents are madvise(MADV_DONTFORK)ed so that a BGSAVE fork
 * child never maps them. Every kvobj header, every SNAP_NOFORK value and all
 * keyspace hash tables are meant to live here, which removes copy-on-write
 * amplification on them while the child runs.
 *
 * Allocator notes:
 *  - The arena is created with a copy of jemalloc's default extent hooks in
 *    which 'alloc' and 'commit' are wrapped to (re)apply MADV_DONTFORK.
 *    MADV_DONTFORK is a VMA flag, so purge (MADV_FREE/MADV_DONTNEED), split
 *    and merge leave it intact; decommit+commit re-applies it in 'commit'.
 *  - Allocations use MALLOCX_ARENA(arena) plus a per-thread explicit tcache.
 *    Explicit tcaches may be used by one thread at a time (jemalloc docs), so
 *    each thread lazily creates its own. Only SNAP_NOFORK requests ever use
 *    these tcaches, so a freed SNAP_NOFORK block can never be recycled into a
 *    default-arena (SNAP_FORK) allocation. This is why plain zfree() must not
 *    be used on SNAP_NOFORK memory.
 *  - Only the parent may touch the arena: its range is unmapped in the child.
 *
 * Copyright (c) 2026-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of (a) the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#include "server.h"
#include "snapshot.h"
#include <sys/mman.h>

#ifdef HAVE_NOFORK_ARENA
#define SNAP_NOFORK_ARENA_SUPPORTED 1
#endif

/* Guard: the SNAP_NOFORK arena must never be allocated from, freed into, or
 * otherwise touched by a fork child (its virtual range is unmapped there). */
static inline void snapAssertNotChild(void) {
    if (unlikely(server.in_fork_child != CHILD_TYPE_NONE))
        serverPanic("SNAP_NOFORK arena accessed from a fork child");
}

/* The arena itself (reserved DONTFORK address range, extent hooks, per-thread
 * tcache flags, range-aware zfree/zrealloc) lives in zmalloc.c so that sds,
 * listpack, intset and dict code keep arena memory in the arena without
 * knowing about classes. These are the snapshot-facing entry points. */
void snapArenaInit(void) {
    server.snap_nofork_arena = UINT_MAX;
#ifdef SNAP_NOFORK_ARENA_SUPPORTED
    if (!zmalloc_nofork_arena_init()) {
        serverLog(LL_WARNING, "SNAP_NOFORK arena disabled: could not reserve a DONTFORK address "
                              "range or create the jemalloc arena; hybrid BGSAVE runs without it.");
        return;
    }
    server.snap_nofork_arena = zmalloc_nofork_arena_index();
    zmalloc_nofork_set_active(server.bgsave_hybrid);       /* new headers, tables and values go there only when on */
    if (!server.bgsave_hybrid) snapArenaSetForkable(1);   /* nothing is placed in it; stock forks map it */
    serverLog(LL_NOTICE, "SNAP_NOFORK arena created (jemalloc arena %u, %zu GB of address space reserved, "
                         "%s).", server.snap_nofork_arena, snapArenaVaReserved() >> 30,
                         server.bgsave_hybrid ? "MADV_DONTFORK: hybrid BGSAVE on" : "forkable: bgsave-hybrid no");
#else
    serverLog(LL_NOTICE, "SNAP_NOFORK arena unavailable on this platform/allocator (needs 64-bit Linux "
                         "with jemalloc); hybrid BGSAVE, if enabled, runs without it.");
#endif
}

/* ---- Fork visibility of the arena (see snapshot.h) ---------------------- */

static int snap_arena_fallback = 0;   /* flipped forkable for a non-hybrid child, flip back on reap */
static int snap_forking_hybrid = 0;   /* set around the hybrid save's own redisFork() */
static int snap_in_hybrid_child = 0;  /* this process is the hybrid save child: no keyspace mapped */
int snapInHybridChild(void) { return snap_in_hybrid_child; }

/* Main thread: a hybrid save was requested but a loaded module has not
 * declared REDISMODULE_OPTIONS_HANDLE_HYBRID_BGSAVE; the stock child runs
 * instead. Logged when the blocking module changes, not on every save. */
void snapHybridFallbackLog(void) {
    static sds last = NULL;
    const char *who = moduleHybridBgsaveBlockedBy();
    if (last && !strcmp(last, who)) return;
    sdsfree(last);
    last = sdsnew(who);
    serverLog(LL_WARNING, "Hybrid BGSAVE: falling back to the fork child, module '%s' has not declared "
                          "REDISMODULE_OPTIONS_HANDLE_HYBRID_BGSAVE", who);
}

int snapArenaForkable(void) {
#ifdef SNAP_NOFORK_ARENA_SUPPORTED
    return snapArenaAvailable() && zmalloc_nofork_arena_forkable();
#else
    return 0;
#endif
}

int snapArenaSetForkable(int forkable) {
#ifdef SNAP_NOFORK_ARENA_SUPPORTED
    if (!snapArenaAvailable()) return 0;
    if (!zmalloc_nofork_arena_set_forkable(forkable)) {
        serverLog(LL_WARNING, "SNAP_NOFORK arena: madvise(%s) failed: %s",
                  forkable ? "MADV_DOFORK" : "MADV_DONTFORK", strerror(errno));
        return 0;
    }
    return 1;
#else
    UNUSED(forkable);
    return 0;
#endif
}

void snapArenaBeforeFork(int purpose) {
    UNUSED(purpose);
    if (snap_forking_hybrid || !snapArenaAvailable() || snapArenaForkable()) return;
    /* A stock child (RDB with the feature off, diskless sync, AOF rewrite,
     * module or Lua debugger fork) walks the keyspace: it must map the arena. */
    if (snapArenaSetForkable(1)) {
        snap_arena_fallback = 1;
        serverLog(LL_NOTICE, "SNAP_NOFORK arena made forkable for a %s child (stock copy-on-write applies)",
                  strChildType(purpose));
    }
}

void snapArenaChildExited(void) {
    if (!snap_arena_fallback) return;
    if (hasActiveChildProcess() || ldbPendingChildren()) return;   /* another stock child still runs */
    snap_arena_fallback = 0;
    if (server.bgsave_hybrid && snapArenaSetForkable(0))
        serverLog(LL_NOTICE, "SNAP_NOFORK arena back to MADV_DONTFORK");
}

int snapHybridConfigApply(const char **err) {
    if (server.loading) return 1;   /* applied again by the startup path */
    if (hasActiveChildProcess() || snapSaveInProgress() || ldbPendingChildren()) {
        *err = "bgsave-hybrid cannot change while a save, rewrite, module fork or debugger child is running";
        return 0;
    }
    if (snapArenaAvailable()) {
        /* On: children stop mapping the arena; off: they must map it. Values
         * placed so far stay where they are, the switch itself is O(1). */
        if (!snapArenaSetForkable(!server.bgsave_hybrid)) {
            *err = "SNAP_NOFORK arena: madvise failed, see the server log";
            return 0;
        }
        zmalloc_nofork_set_active(server.bgsave_hybrid);
        snap_arena_fallback = 0;
    }
    serverLog(LL_NOTICE, "Hybrid BGSAVE switched %s at runtime%s", server.bgsave_hybrid ? "on" : "off",
              snapArenaAvailable() ? "" : " (no SNAP_NOFORK arena on this platform)");
    return 1;
}


size_t snapArenaVaReserved(void) {
#ifdef SNAP_NOFORK_ARENA_SUPPORTED
    size_t reserved = 0;
    if (snapArenaAvailable()) zmalloc_nofork_va_stats(&reserved, NULL);
    return reserved;
#else
    return 0;
#endif
}

size_t snapArenaVaUsed(void) {
#ifdef SNAP_NOFORK_ARENA_SUPPORTED
    size_t used = 0;
    if (snapArenaAvailable()) zmalloc_nofork_va_stats(NULL, &used);
    return used;
#else
    return 0;
#endif
}




void *snapNoforkRealloc(void *ptr, size_t size) {
    snapAssertNotChild();
#ifdef SNAP_NOFORK_ARENA_SUPPORTED
    return zrealloc_nofork(ptr, size);
#else
    return zrealloc(ptr, size);
#endif
}

void snapNoforkFree(void *ptr) {
    if (ptr == NULL) return;
    snapAssertNotChild();
#ifdef SNAP_NOFORK_ARENA_SUPPORTED
    zfree_nofork(ptr);
#else
    zfree(ptr);
#endif
}

void snapNoforkFreeSize(void *ptr, size_t usable) {
    if (ptr == NULL) return;
    snapAssertNotChild();
#ifdef SNAP_NOFORK_ARENA_SUPPORTED
    zfree_nofork_size(ptr, usable);
#else
    zfree_with_size(ptr, usable);
#endif
}

int snapPtrIsNofork(const void *ptr) {
    return zmalloc_is_nofork(ptr);
}

size_t snapNoforkUsableSize(void *ptr) {
    return zmalloc_size(ptr);
}

/* ---- Class-aware free wrappers ------------------------------------------- */

void snapSdsFree(sds s) {
    if (s == NULL) return;
    snapNoforkFree(sdsAllocPtr(s));
}

void snapLpFree(unsigned char *lp) {
    snapNoforkFree(lp);
}

void snapIntsetFree(intset *is) {
    snapNoforkFree(is);
}

void snapListpackExFree(struct listpackEx *lpt) {
    if (lpt == NULL) return;
    snapNoforkFree(lpt->lp);
    snapNoforkFree(lpt);
}


/* ============================================================================
 * Classification and placement
 * ============================================================================ */

int snapEncodingIsNoforkClass(int type, int encoding) {
    switch (type) {
    case OBJ_STRING:
        return 1; /* int, embstr, raw */
    case OBJ_LIST:
        return encoding == OBJ_ENCODING_LISTPACK;
    case OBJ_SET:
        return encoding == OBJ_ENCODING_INTSET || encoding == OBJ_ENCODING_LISTPACK;
    case OBJ_ZSET:
        return encoding == OBJ_ENCODING_LISTPACK;
    case OBJ_HASH:
        return encoding == OBJ_ENCODING_LISTPACK || encoding == OBJ_ENCODING_LISTPACK_EX;
    default:
        return 0; /* module, stream, array, ... */
    }
}

/* INT and EMBSTR strings carry their value inside the object. */
static inline int snapValueIsInline(int type, int encoding) {
    return type == OBJ_STRING && (encoding == OBJ_ENCODING_INT || encoding == OBJ_ENCODING_EMBSTR);
}

int snapClassify(int type, int encoding, size_t alloc_bytes) {
    if (!snapEncodingIsNoforkClass(type, encoding)) return SNAP_FORK;
    if (snapValueIsInline(type, encoding)) return SNAP_NOFORK;
    return alloc_bytes <= server.save_small_max_bytes ? SNAP_NOFORK : SNAP_FORK;
}

/* The single value allocation of a single-allocation value (the sds
 * allocation start for raw strings), or NULL when the value is inline. */
static void *snapValueAllocPtr(const robj *o) {
    switch (o->encoding) {
    case OBJ_ENCODING_RAW: return sdsAllocPtr(o->ptr);
    case OBJ_ENCODING_LISTPACK:
    case OBJ_ENCODING_INTSET:
    case OBJ_ENCODING_LISTPACK_EX: return o->ptr;
    default: return NULL;
    }
}


int snapKeyClass(const robj *o) {
    if (!snapEncodingIsNoforkClass(o->type, o->encoding)) return SNAP_FORK;
    if (snapValueIsInline(o->type, o->encoding)) return SNAP_NOFORK;
    /* No arena: nothing is placed, the class is a pure size decision. */
    if (!snapArenaAvailable()) {
        void *a = snapValueAllocPtr(o);
        return snapClassify(o->type, o->encoding, a ? zmalloc_size(a) : 0);
    }
    /* LISTPACK_EX: the class follows the struct. The struct holds the
     * ExpireMeta registered in the db's ebuckets, so it is never moved; when
     * it is in the arena the child cannot read it and the key MUST be served
     * by the background thread, wherever the listpack itself lives. */
    void *a = snapValueAllocPtr(o);
    if (a == NULL || !zmalloc_is_nofork(a)) return SNAP_FORK;
    return SNAP_NOFORK;
}

/* Copy an sds into the requested arena. */
static sds snapSdsCopy(sds s, int to_nofork) {
    size_t len = sdslen(s);
    char type = sdsReqType(len);
    size_t need = sdsReqSize(len, type);
    size_t usable = 0;
    char *buf = to_nofork ? snapNoforkAllocUsable(need, &usable) : zmalloc_usable(need, &usable);
    return sdsnewplacement(buf, usable, type, s, len);
}

/* Copy an sds into the requested arena and free the original. */
static sds snapSdsMove(sds s, int to_nofork) {
    sds ns = snapSdsCopy(s, to_nofork);
    sdsfree(s); /* range-aware */
    return ns;
}

/* Copy 'bytes' bytes of a blob (listpack, intset, struct) into the requested arena. */
static void *snapBlobCopy(void *p, size_t bytes, int to_nofork) {
    void *n = to_nofork ? snapNoforkAlloc(bytes) : zmalloc(bytes);
    memcpy(n, p, bytes);
    return n;
}

/* Same, freeing the original. */
static void *snapBlobMove(void *p, size_t bytes, int to_nofork) {
    void *n = snapBlobCopy(p, bytes, to_nofork);
    zfree(p); /* range-aware */
    return n;
}

void snapPlaceValueArena(robj *o) {
    if (!snapEncodingIsNoforkClass(o->type, o->encoding)) return;
    if (snapValueIsInline(o->type, o->encoding)) return;
    /* Module key metadata is serialized through module callbacks, which only
     * run in the fork child: such a key is SNAP_FORK whatever its size, so
     * every piece of its value must be readable by the child. */
    int force_fork = o->iskvobj && getModuleMetaBits(((kvobj *)o)->metabits);

    switch (o->encoding) {
    case OBJ_ENCODING_RAW: {
        sds s = o->ptr;
        void *a = sdsAllocPtr(s);
        int want = force_fork ? SNAP_FORK : snapClassify(o->type, o->encoding, zmalloc_size(a));
        int in = zmalloc_is_nofork(a);
        if (want == SNAP_NOFORK && !in) o->ptr = snapSdsMove(s, 1);
        else if (want == SNAP_FORK && in) o->ptr = snapSdsMove(s, 0);
        break;
    }
    case OBJ_ENCODING_LISTPACK: {
        unsigned char *lp = o->ptr;
        int want = force_fork ? SNAP_FORK : snapClassify(o->type, o->encoding, zmalloc_size(lp));
        int in = zmalloc_is_nofork(lp);
        if (want == SNAP_NOFORK && !in) o->ptr = snapBlobMove(lp, lpBytes(lp), 1);
        else if (want == SNAP_FORK && in) o->ptr = snapBlobMove(lp, lpBytes(lp), 0);
        break;
    }
    case OBJ_ENCODING_INTSET: {
        intset *is = o->ptr;
        int want = force_fork ? SNAP_FORK : snapClassify(o->type, o->encoding, zmalloc_size(is));
        int in = zmalloc_is_nofork(is);
        if (want == SNAP_NOFORK && !in) o->ptr = snapBlobMove(is, intsetBlobLen(is), 1);
        else if (want == SNAP_FORK && in) o->ptr = snapBlobMove(is, intsetBlobLen(is), 0);
        break;
    }
    case OBJ_ENCODING_LISTPACK_EX: {
        /* Two allocations. The struct is arena-born (listpackExCreate) and is
         * NEVER moved: its ExpireMeta may be registered in the db's ebuckets.
         * The listpack follows the struct: struct in the arena -> listpack in
         * the arena (the key is served by the background thread regardless of
         * size, so no "too big" move-out); struct outside (legacy) -> the
         * listpack must be outside too, or the fork child would dereference
         * arena memory. */
        listpackEx *lpt = o->ptr;
        int lp_in = zmalloc_is_nofork(lpt->lp);
        int st_in = zmalloc_is_nofork(lpt);
        if (force_fork) {
            /* Module metadata: both pieces must leave the arena. Moving the
             * struct is only legal while its ExpireMeta is not registered in
             * the db's ebuckets, which holds at every placement call site
             * (kvobjSet insertion funnel, dbSetValue swap, metadata attach:
             * the db unregisters before and re-registers after). */
            if (st_in) {
                lpt = snapBlobMove(lpt, sizeof(*lpt), 0);
                o->ptr = lpt;
            }
            if (zmalloc_is_nofork(lpt->lp)) lpt->lp = snapBlobMove(lpt->lp, lpBytes(lpt->lp), 0);
            break;
        }
        if (st_in && !lp_in) lpt->lp = snapBlobMove(lpt->lp, lpBytes(lpt->lp), 1);
        else if (!st_in && lp_in) lpt->lp = snapBlobMove(lpt->lp, lpBytes(lpt->lp), 0);
        break;
    }
    default:
        break;
    }
}

static void snapPlacementScanTables(kvstore *kvs, snapPlacementStats *st) {
    for (int didx = 0; didx < kvstoreNumDicts(kvs); didx++) {
        dict *d = kvstoreGetDict(kvs, didx);
        if (d == NULL) continue;
        if (zmalloc_is_nofork(d)) st->dict_tables_in_arena++; else st->dict_tables_outside++;
        for (int t = 0; t < 2; t++) {
            if (d->ht_table[t] == NULL) continue;
            if (zmalloc_is_nofork(d->ht_table[t])) st->dict_tables_in_arena++; else st->dict_tables_outside++;
        }
    }
}

/* ---- Clone-on-write for the keyspace overlay ---------------------------- */

/* Duplicate the whole kvobj allocation (metadata slots, robj, embedded key
 * and, for EMBSTR, the embedded value) into a fresh SNAP_NOFORK block. The
 * value pointer is left as-is (the caller decides copy vs share). */
static kvobj *snapKvobjCloneHeader(kvobj *orig) {
    char *alloc = kvobjGetAllocPtr(orig);
    size_t size = zmalloc_size(alloc);
    char *nalloc = snapNoforkAlloc(size);
    memcpy(nalloc, alloc, size);
    kvobj *kv = (kvobj *)(nalloc + ((char *)orig - alloc));
    if (kv->type == OBJ_STRING && kv->encoding == OBJ_ENCODING_EMBSTR)
        kv->ptr = (char *)kv->ptr + (nalloc - alloc);
    kv->refcount = 1;
    return kv;
}

static void snapSaveRetainedAdd(kvobj *orig, int value_shared);
/* Overlay hook privdata of store s: the store index. The redisDb owning the
 * store is resolved at call time (SWAPDB moves a store to another db id; a
 * FLUSHDB leaves it with no owner), the db id in the dump is always s. */
typedef struct snapStorePriv { int s; } snapStorePriv;
typedef struct snapSave snapSave;
static snapSave *snap_save;   /* defined with the save object below */
static redisDb *snapStoreOwnerDb(snapSave *sv, int s);
static void snapSaveRetainedSub(kvobj *orig, int value_shared);
void updateSlotAllocSize(redisDb *db, int didx, kvobj *kv, int64_t oldsize, int64_t newsize);   /* db.c */

void *snapOverlayCloneKey(void *privdata, int didx, void *orig_, int *value_shared) {
    /* The db owning the store right now (NULL once FLUSHDB detached it: no
     * expires/subexpires to keep in step, and no writes reach it anyway). */
    redisDb *db = snap_save ? snapStoreOwnerDb(snap_save, ((snapStorePriv *)privdata)->s) : NULL;
    kvobj *orig = orig_;
    kvobj *kv = snapKvobjCloneHeader(orig);
    *value_shared = 0;

    /* SNAP_NOFORK: the scan thread reads the original live -> the clone gets a
     * private copy of the value (in the arena when there is one). SNAP_FORK:
     * the child reads its CoW copy -> the clone shares the value.
     *
     * Without the arena the class of a single-allocation value is a size
     * decision the scan thread takes by reading the value's allocation, so a
     * shared value that the clone then reallocates would be read after free.
     * Single-allocation values are therefore always copied in that mode;
     * only multi-allocation values (never dereferenced by the scan thread)
     * are shared. */
    int to_arena = snapArenaAvailable();
    int nofork = to_arena ? snapKeyClass(orig) == SNAP_NOFORK
                          : snapEncodingIsNoforkClass(orig->type, orig->encoding);
    if (!snapValueIsInline(orig->type, orig->encoding)) {
        switch (orig->encoding) {
        case OBJ_ENCODING_RAW:
            if (nofork) kv->ptr = snapSdsCopy(orig->ptr, to_arena);
            else *value_shared = 1;
            break;
        case OBJ_ENCODING_LISTPACK:
            if (nofork) kv->ptr = snapBlobCopy(orig->ptr, lpBytes(orig->ptr), to_arena);
            else *value_shared = 1;
            break;
        case OBJ_ENCODING_INTSET:
            if (nofork) kv->ptr = snapBlobCopy(orig->ptr, intsetBlobLen(orig->ptr), to_arena);
            else *value_shared = 1;
            break;
        case OBJ_ENCODING_LISTPACK_EX: {
            /* Private copy (struct + listpack) with a fresh ExpireMeta. */
            listpackEx *o = orig->ptr;
            if (nofork) {
                listpackEx *n = snapNoforkCalloc(sizeof(*n));
                n->meta.trash = 1;
                n->lp = snapBlobCopy(o->lp, lpBytes(o->lp), zmalloc_is_nofork(o->lp));
                kv->ptr = n;
            } else {
                *value_shared = 1;
            }
            break;
        }
        default:
            /* Multi-allocation value (hashtable, skiplist, quicklist, stream,
             * module, array): SNAP_FORK, the child has its own copy. */
            *value_shared = 1;
            break;
        }
    }

    snapSaveRetainedAdd(orig, *value_shared);

    /* The clone takes over the original's per-slot accounting: later in-place
     * updates and the delete of the clone are counted against the slot, the
     * original is released unaccounted at merge time. */
    if (db && server.memory_tracking_enabled)
        updateSlotAllocSize(db, didx, kv, kvobjAllocSize(orig), kvobjAllocSize(kv));

    /* Point the expiry structures at the clone: they are not part of the
     * scanned snapshot, and every later TTL operation must act on the clone. */
    if (db) {
        sds key = kvobjGetKey(orig);
        if (kvobjGetExpire(orig) != -1) {
            dictEntryLink link = kvstoreDictFindLink(db->expires, didx, key, NULL);
            if (link) kvstoreDictSetAtLink(db->expires, didx, kv, &link, 0);
        }
        if (orig->type == OBJ_HASH) {
            /* Writes trash=1 into the original's ExpireMeta; the scan thread
             * never reads meta (it recomputes the min field TTL from the
             * listpack), so this is tolerated. */
            uint64_t sub = estoreRemove(db->subexpires, didx, orig);
            if (sub != EB_EXPIRE_TIME_INVALID) estoreAdd(db->subexpires, didx, kv, sub);
        }
    }
    return kv;
}

/* A deleted key the scan already read keeps a header-only stand-in in its
 * frozen entry until merge (kvstore ovTombstoneInPlace). Released through
 * snapOverlayFreeOriginal with value_shared = 1: header only. */

void snapOverlayFreeOriginal(void *privdata, int didx, void *orig_, int value_shared) {
    UNUSED(privdata);
    UNUSED(didx);
    kvobj *orig = orig_;
    snapSaveRetainedSub(orig, value_shared);
    /* The clone carries the key metadata (no keyMetaOnFree here) and, when
     * shared, owns the value. */
    if (!value_shared) kvobjFreeValueOnly(orig);
    zfree(kvobjGetAllocPtr(orig));
}

void snapValueDetachFromArena(robj *o) {
    if (!snapArenaAvailable()) return;
    switch (o->encoding) {
    case OBJ_ENCODING_RAW: {
        void *a = sdsAllocPtr(o->ptr);
        if (zmalloc_is_nofork(a)) o->ptr = snapSdsMove(o->ptr, 0);
        break;
    }
    case OBJ_ENCODING_LISTPACK:
        if (zmalloc_is_nofork(o->ptr)) o->ptr = snapBlobMove(o->ptr, lpBytes(o->ptr), 0);
        break;
    case OBJ_ENCODING_INTSET:
        if (zmalloc_is_nofork(o->ptr)) o->ptr = snapBlobMove(o->ptr, intsetBlobLen(o->ptr), 0);
        break;
    default:
        break;
    }
}

void snapPlacementDriftReply(client *c, redisDb *db, int limit) {
    kvstoreIterator kvs_it;
    dictEntry *de;
    void *replylen = addReplyDeferredLen(c);
    int n = 0;
    kvstoreIteratorInit(&kvs_it, db->keys);
    while ((de = kvstoreIteratorNext(&kvs_it)) != NULL && n < limit) {
        kvobj *kv = dictGetKV(de);
        const char *drift = NULL;
        if (!zmalloc_is_nofork(kv)) drift = "header-outside";
        else if (!snapEncodingIsNoforkClass(kv->type, kv->encoding)) {
            if (kv->ptr && kv->encoding != OBJ_ENCODING_INT && zmalloc_is_nofork(kv->ptr)) drift = "fork-value-in-arena";
        } else if (snapValueIsInline(kv->type, kv->encoding)) {
            drift = NULL;
        } else if (kv->encoding == OBJ_ENCODING_LISTPACK_EX) {
            listpackEx *lpt = kv->ptr;
            if (zmalloc_is_nofork(lpt) != zmalloc_is_nofork(lpt->lp)) drift = "listpackex-split";
        } else {
            void *a = snapValueAllocPtr(kv);
            int want = snapClassify(kv->type, kv->encoding, zmalloc_size(a));
            int in = zmalloc_is_nofork(a);
            if (want == SNAP_NOFORK && !in) drift = "nofork-outside";
            if (want == SNAP_FORK && in) drift = "fork-inside";
        }
        if (!drift) continue;
        addReplyArrayLen(c, 4);
        addReplyBulkCBuffer(c, kvobjGetKey(kv), sdslen(kvobjGetKey(kv)));
        addReplyBulkCString(c, getObjectTypeName(kv));
        addReplyBulkCString(c, strEncoding(kv->encoding));
        addReplyBulkCString(c, drift);
        n++;
    }
    kvstoreIteratorReset(&kvs_it);
    setDeferredArrayLen(c, replylen, n);
}

void snapPlacementScan(redisDb *db, snapPlacementStats *st) {
    memset(st, 0, sizeof(*st));
    snapPlacementScanTables(db->keys, st);
    snapPlacementScanTables(db->expires, st);
    kvstoreIterator kvs_it;
    dictEntry *de;
    kvstoreIteratorInit(&kvs_it, db->keys);
    while ((de = kvstoreIteratorNext(&kvs_it)) != NULL) {
        kvobj *kv = dictGetKV(de);
        st->keys++;
        int hdr_in = zmalloc_is_nofork(kv);
        if (hdr_in) st->headers_in_arena++; else { st->headers_outside++; st->violations++; }

        int cls = snapKeyClass(kv);
        if (cls == SNAP_NOFORK) st->class_nofork++; else st->class_fork++;

        if (getModuleMetaBits(kv->metabits)) {
            /* Serialized by the child (module callbacks): nothing of the value
             * may be in the arena. */
            if (cls == SNAP_NOFORK && !snapValueIsInline(kv->type, kv->encoding)) st->violations++;
            if (kv->encoding == OBJ_ENCODING_LISTPACK_EX) {
                listpackEx *lpt = kv->ptr;
                if (zmalloc_is_nofork(lpt) || zmalloc_is_nofork(lpt->lp)) st->violations++;
            }
            continue;
        }

        if (!snapEncodingIsNoforkClass(kv->type, kv->encoding)) {
            /* Multi-allocation value: its top-level allocation must be forkable. */
            if (kv->ptr && kv->encoding != OBJ_ENCODING_INT && zmalloc_is_nofork(kv->ptr)) st->violations++;
            /* Quicklists adopt listpacks: every node must be forkable too. */
            if (kv->encoding == OBJ_ENCODING_QUICKLIST) {
                quicklist *ql = kv->ptr;
                for (quicklistNode *node = ql->head; node; node = node->next)
                    if (zmalloc_is_nofork(node->entry)) st->violations++;
            }
            continue;
        }
        if (snapValueIsInline(kv->type, kv->encoding)) continue;
        if (kv->encoding == OBJ_ENCODING_LISTPACK_EX) {
            listpackEx *lpt = kv->ptr;
            int st_in = zmalloc_is_nofork(lpt), lp_in = zmalloc_is_nofork(lpt->lp);
            if (st_in && !lp_in) st->drift_nofork_outside++;
            if (!st_in && lp_in) st->violations++;   /* child would read arena memory */
            if (!st_in && hdr_in) st->drift_nofork_outside++;   /* legacy struct outside */
            continue;
        }
        void *a = snapValueAllocPtr(kv);
        int want = snapClassify(kv->type, kv->encoding, zmalloc_size(a));
        int in = zmalloc_is_nofork(a);
        if (want == SNAP_NOFORK && !in) st->drift_nofork_outside++;
        if (want == SNAP_FORK && in) st->drift_fork_inside++;
    }
    kvstoreIteratorReset(&kvs_it);
}

/* ============================================================================
 * Per-slot background scan
 * ============================================================================
 *
 * The scan walks every kvstore dict ("slot") of every store in sequence on a
 * dedicated thread and hands each fork-instant kvobj to a dispatch callback.
 *
 * Immutability contract (spec §4, invariant 2): from snapScanStart() until a
 * slot is DONE, its dict is never modified. The main thread guarantees no
 * insert/delete (writes go to the per-slot temp dict) and pauses rehashing
 * and auto-resize; the thread only reads (dictScanNoPause never writes to the
 * dict, unlike dictScan which bumps d->pauserehash). Cron-driven resize and
 * rehash are already skipped by databasesCron while a child process exists.
 *
 * Publication: slot states are atomics written with release semantics by the
 * thread (SCANNING, SCANNED) and by the main thread (PENDING, DONE), and read
 * with acquire semantics. Completed slots are handed to the main thread over
 * a single-producer/single-consumer ring (no locks, no allocation on the
 * thread) plus an eventNotifier so the event loop wakes up.
 *
 * M1 decision on granularity: the scan unit is one kvstore dict. In cluster
 * mode that is one of 16384 slots; in standalone mode a db has a single dict,
 * so the whole db stays owed until fully scanned and the clone-on-write
 * burden does not shrink during the save. The write hooks only depend on
 * snapScanKeyIsOwed(), so refining the unit to bucket ranges later (compare
 * the key's bucket, in dictScan cursor order, with the published cursor; valid
 * because the dict is frozen) is a local change to this file.
 */

#include "atomicvar.h"
#include "ae.h"
#include "eventnotifier.h"
#include <pthread.h>
#include <poll.h>

struct snapScanCtx {
    kvstore **stores;               /* One per db (db->keys). */
    int num_stores;
    int *num_dicts;                 /* kvstoreNumDicts() per store. */
    redisAtomic uint8_t **state;    /* Per store: num_dicts slot states. */
    int *frozen;                    /* Per store: 1 while our guard is installed on it (snapScanFreeze() until its last slot completes). */
    long *pending;                  /* Per store: slots not yet DONE; the guard comes off at 0, before anyone may free the store. */
    uint8_t **attached;             /* Per store: 1 per dict we paused and gave a bitmap/overlay (on demand). */
    struct snapGuardPriv { struct snapScanCtx *ctx; int s; } *guards;   /* Per store, the guard's privdata. */

    snapScanDispatchFn *dispatch;
    snapScanSlotDoneFn *slot_done;
    void *privdata;

    /* SPSC completion ring, capacity == slots_total. */
    uint64_t *ring;
    redisAtomic long ring_wr;       /* Written by the thread. */
    long ring_rd;                   /* Main thread only. */
    eventNotifier *en;

    pthread_t tid;
    int started;
    int joined;
    redisAtomic int abort;          /* Main -> thread. */
    redisAtomic int finished;       /* Thread -> main. */

    long slots_total;
    redisAtomic long slots_scanned;
    long slots_done;                /* Main thread only. */
    redisAtomic long long keys_dispatched;

    /* Keyspace overlay (optional). */
    kvstoreOverlayHooks ov_hooks;
    int has_ov;
    void **ov_priv;                 /* Per store. */
    int frozen_all;                 /* snapScanFreeze() done. */
    /* Per-save bucket bitmaps: bits[s][didx*2+table], .b NULL when the slot
     * was not frozen or the table does not exist. */
    struct snapBitmap { uint8_t *b; size_t bytes; } **bits;   /* .b: main thread stores (release), thread loads (acquire) */
    size_t bitmap_bytes;
    unsigned long window_buckets;   /* copied from snap_window_buckets at create */
    redisAtomic long long buckets_skipped;   /* buckets the thread skipped (already emitted) */
    snapScanThreadDoneFn *thread_done;
    void (*thread_init)(void *privdata);   /* first thing on the scan thread */
    void (*window_done)(void *privdata);   /* after every window, and before thread_done */
    void (*store_done)(void *privdata, int s);   /* main thread, when a store's last slot completed */
};

static __thread int snap_on_scan_thread = 0;
static __thread int snap_scan_maxmemory_policy = 0;
static __thread int snap_scan_rdb_compression = 0;
int snapOnScanThread(void) { return snap_on_scan_thread; }
int snapScanMaxmemoryPolicy(void) { return snap_scan_maxmemory_policy; }
int snapScanRdbCompression(void) { return snap_scan_rdb_compression; }

/* ---- Bucket window and write sections (see snapshot.h) ------------------ */

#define SNAP_WINDOW_BUCKETS_DEFAULT 1024
static unsigned long snap_window_buckets = SNAP_WINDOW_BUCKETS_DEFAULT;
void snapSetWindowBuckets(unsigned long n) { snap_window_buckets = n ? n : 1; }
unsigned long snapGetWindowBuckets(void) { return snap_window_buckets; }

typedef struct snapWindow {
    dict *d;                 /* NULL: no window (thread between dicts or finished) */
    int table;               /* -1: the whole dict (the thread is about to read its layout) */
    unsigned long lo, hi;    /* buckets [lo, hi) */
} snapWindow;

/* Double-buffered by sequence parity: the thread fills snap_win[(seq+1)&1]
 * and then publishes seq+1; the main thread reads snap_win[seq&1] inside a
 * write section, which the thread cannot overtake (see snapWalkerQuiesce). */
static snapWindow snap_win[2];
static redisAtomic uint64_t snap_win_seq = 0;
redisAtomic uint64_t snap_funnel_seq = 0;

/* Scan thread: publish the window it is about to read. */
static void snapWindowPublish(dict *d, int table, unsigned long lo, unsigned long hi) {
    uint64_t seq;
    atomicGet(snap_win_seq, seq);
    snapWindow *w = &snap_win[(seq + 1) & 1];
    w->d = d;
    w->table = table;
    w->lo = lo;
    w->hi = hi;
    atomicSetWithSync(snap_win_seq, seq + 1);   /* seq_cst: pairs with snapFunnelOpen */
}

/* Scan thread, after snapWindowPublish(): wait until no write section that
 * may have read the previous window is still open. A section opened after
 * this point has seen the new window (store-buffer argument, both sides
 * seq_cst). Sections are short (one command at most). */
static inline int snapScanAborted(snapScanCtx *ctx);
static void snapWalkerQuiesce(snapScanCtx *ctx) {
    uint64_t v;
    atomicGetWithSync(snap_funnel_seq, v);
    if (!(v & 1)) return;
    int spins = 0;
    for (;;) {
        uint64_t n;
        atomicGetWithSync(snap_funnel_seq, n);
        if (n != v) return;
        /* The command aborting the save may itself hold the section open and
         * be waiting to join us: an aborted walker reads nothing more. */
        if (snapScanAborted(ctx)) return;
        if (++spins < 200) sched_yield();
        else usleep(20);
    }
}

void snapFunnelOpen(void) {
    uint64_t v;
    atomicGet(snap_funnel_seq, v);          /* single writer: relaxed read of our own value */
    if (v & 1) return;
    atomicSetWithSync(snap_funnel_seq, v + 1);   /* seq_cst: pairs with snapWindowPublish */
}

static void snapSaveDrainSideTables(void);

void snapFunnelCloseSlow(void) {
    uint64_t v;
    atomicGet(snap_funnel_seq, v);
    if (v & 1) {
        /* Still inside the section (the window holds still) and at a point
         * where no caller holds a link: apply what the walker has passed. */
        snapSaveDrainSideTables();
        atomicSetWithSync(snap_funnel_seq, v + 1);
    }
}

int snapWindowCoversKey(dict *d, const void *key) {
    uint64_t seq;
    atomicGetWithSync(snap_win_seq, seq);
    if (seq == 0) return 0;
    const snapWindow *w = &snap_win[seq & 1];
    if (w->d != d) return 0;
    if (w->table < 0) return 1;
    unsigned long idx = dictGetHash(d, key) & DICTHT_SIZE_MASK(d->ht_size_exp[w->table]);
    return idx >= w->lo && idx < w->hi;
}

/* Inside a write section: is the thread's published window on dict d? */
static int snapWindowOnDict(dict *d) {
    uint64_t seq;
    atomicGetWithSync(snap_win_seq, seq);
    if (seq == 0) return 0;
    return snap_win[seq & 1].d == d;
}

/* ---- Per-save bucket bitmaps ---- */

static inline uint8_t *snapScanBitmap(snapScanCtx *ctx, int s, int didx, int table) {
    /* Allocated on demand by the main thread while the thread may be
     * reading: acquire pairs with the release store in snapScanBitmapAlloc. */
    return __atomic_load_n(&ctx->bits[s][didx * 2 + table].b, __ATOMIC_ACQUIRE);
}

void snapScanMarkBucketEmitted(snapScanCtx *ctx, int s, int didx, int table, unsigned long idx) {
    uint8_t *bm = snapScanBitmap(ctx, s, didx, table);
    serverAssert(bm != NULL);
    __atomic_fetch_or(&bm[idx >> 3], (uint8_t)(1u << (idx & 7)), __ATOMIC_RELAXED);
}

int snapScanBucketEmitted(snapScanCtx *ctx, int s, int didx, int table, unsigned long idx) {
    uint8_t *bm = snapScanBitmap(ctx, s, didx, table);
    if (!bm) return 0;
    return (__atomic_load_n(&bm[idx >> 3], __ATOMIC_RELAXED) >> (idx & 7)) & 1;
}

size_t snapScanBitmapBytes(snapScanCtx *ctx) { return ctx ? ctx->bitmap_bytes : 0; }

static void snapScanBitmapAlloc(snapScanCtx *ctx, int s, int didx, dict *d) {
    for (int t = 0; t < 2; t++) {
        unsigned long size = DICTHT_SIZE(d->ht_size_exp[t]);
        if (size == 0) continue;
        size_t bytes = (size + 7) / 8;
        struct snapBitmap *bm = &ctx->bits[s][didx * 2 + t];
        bm->bytes = bytes;
        __atomic_store_n(&bm->b, zcalloc(bytes), __ATOMIC_RELEASE);
        ctx->bitmap_bytes += bytes;
    }
}

static void snapScanBitmapFree(snapScanCtx *ctx, int s, int didx) {
    for (int t = 0; t < 2; t++) {
        struct snapBitmap *bm = &ctx->bits[s][didx * 2 + t];
        if (!bm->b) continue;
        ctx->bitmap_bytes -= bm->bytes;
        zfree(bm->b);
        bm->b = NULL;
        bm->bytes = 0;
    }
}

void snapScanSetThreadDone(snapScanCtx *ctx, snapScanThreadDoneFn *fn) {
    ctx->thread_done = fn;
}

void snapScanSetThreadInit(snapScanCtx *ctx, void (*fn)(void *privdata)) {
    ctx->thread_init = fn;
}

void snapScanSetWindowDone(snapScanCtx *ctx, void (*fn)(void *privdata)) {
    ctx->window_done = fn;
}

void snapScanSetStoreDone(snapScanCtx *ctx, void (*fn)(void *privdata, int s)) {
    ctx->store_done = fn;
}

void snapScanSetOverlay(snapScanCtx *ctx, const kvstoreOverlayHooks *hooks, void **store_privdata)
{
    serverAssert(!ctx->started);
    ctx->ov_hooks = *hooks;
    ctx->has_ov = 1;
    ctx->ov_priv = store_privdata;
}

#define SNAP_SCAN_ENC(store_idx, didx) (((uint64_t)(uint32_t)(store_idx) << 32) | (uint32_t)(didx))
#define SNAP_SCAN_DEC_STORE(v) ((int)((v) >> 32))
#define SNAP_SCAN_DEC_DIDX(v) ((int)((v) & 0xffffffffu))

static inline void snapSlotSetState(snapScanCtx *ctx, int s, int didx, uint8_t st) {
    atomicSetWithSync(ctx->state[s][didx], st);
}

static inline uint8_t snapSlotGetState(snapScanCtx *ctx, int s, int didx) {
    uint8_t st;
    atomicGetWithSync(ctx->state[s][didx], st);
    return st;
}

snapScanCtx *snapScanCreate(kvstore **stores, int num_stores,
                            snapScanDispatchFn *dispatch,
                            snapScanSlotDoneFn *slot_done, void *privdata)
{
    snapScanCtx *ctx = zcalloc(sizeof(*ctx));
    ctx->stores = zmalloc(sizeof(kvstore*) * num_stores);
    memcpy(ctx->stores, stores, sizeof(kvstore*) * num_stores);
    ctx->num_stores = num_stores;
    ctx->num_dicts = zmalloc(sizeof(int) * num_stores);
    ctx->state = zmalloc(sizeof(redisAtomic uint8_t*) * num_stores);
    ctx->frozen = zcalloc(sizeof(int) * num_stores);
    ctx->pending = zmalloc(sizeof(long) * num_stores);
    ctx->attached = zmalloc(sizeof(uint8_t *) * num_stores);
    ctx->guards = zmalloc(sizeof(struct snapGuardPriv) * num_stores);
    ctx->bits = zmalloc(sizeof(*ctx->bits) * num_stores);
    for (int s = 0; s < num_stores; s++) {
        ctx->num_dicts[s] = kvstoreNumDicts(stores[s]);
        ctx->pending[s] = ctx->num_dicts[s];
        ctx->state[s] = zcalloc(sizeof(redisAtomic uint8_t) * ctx->num_dicts[s]); /* PENDING */
        ctx->attached[s] = zcalloc(ctx->num_dicts[s]);
        ctx->guards[s].ctx = ctx;
        ctx->guards[s].s = s;
        ctx->bits[s] = zcalloc(sizeof(struct snapBitmap) * ctx->num_dicts[s] * 2);
        ctx->slots_total += ctx->num_dicts[s];
    }
    ctx->window_buckets = snap_window_buckets;
    ctx->ring = zmalloc(sizeof(uint64_t) * (ctx->slots_total ? ctx->slots_total : 1));
    ctx->dispatch = dispatch;
    ctx->slot_done = slot_done;
    ctx->privdata = privdata;
    ctx->en = createEventNotifier();
    return ctx;
}

/* Main thread, inside a write section: give dict (s, didx) what the scan
 * needs from it, on demand (TASK-1.21.6): pause its layout, allocate its
 * bucket bitmaps, attach the overlay. Called by the guard on the first
 * structural write to a slot the thread has not finished, or on the first
 * access of any kind while the thread's window is on the dict. Returns the
 * overlay (NULL without one). */
static struct kvstoreDictOverlay *snapScanAttach(snapScanCtx *ctx, int s, int didx, dict *d) {
    if (!ctx->attached[s][didx]) {
        ctx->attached[s][didx] = 1;
        dictPauseRehashing(d);
        dictPauseAutoResize(d);
        snapScanBitmapAlloc(ctx, s, didx, d);
        if (ctx->has_ov)
            kvstoreOverlayAttach(ctx->stores[s], didx, &ctx->ov_hooks, ctx->ov_priv ? ctx->ov_priv[s] : NULL);
    }
    if (!ctx->has_ov) return NULL;
    kvstoreDictMetaBase *meta = kvstoreGetDictMeta(ctx->stores[s], didx, 0);
    return meta ? meta->ov : NULL;
}

/* kvstore snapshot guard (kvstore.h): every access to a dict without an
 * overlay while the scan runs lands here. */
static struct kvstoreDictOverlay *snapOverlayGuard(void *privdata, kvstore *kvs, int didx, int write) {
    struct snapGuardPriv *g = privdata;
    snapScanCtx *ctx = g->ctx;
    int s = g->s;
    uint8_t st = snapSlotGetState(ctx, s, didx);
    if (st == SNAP_SLOT_DONE || st == SNAP_SLOT_SCANNED) return NULL;   /* the thread is finished with it */
    /* The thread may take this dict at any moment: it publishes a whole-dict
     * window and waits for open sections, so the layout is stable from then
     * on only if every access here runs inside a section, reads included (a
     * find may step a rehash). */
    snapFunnelOpen();
    dict *d = kvstoreGetDict(kvs, didx);
    if (!d) return NULL;
    if (!write && !snapWindowOnDict(d)) return NULL;   /* a read of a dict the thread is not reading */
    return snapScanAttach(ctx, s, didx, d);
}

static void snapScanFreezeStore(snapScanCtx *ctx, int s) {
    for (int didx = 0; didx < ctx->num_dicts[s]; didx++) {
        dict *d = kvstoreGetDict(ctx->stores[s], didx);
        if (d == NULL || dictSize(d) == 0) {
            /* Nothing to snapshot: the slot is DONE from the start, writes go
             * to the real dict as usual and the scan thread never reads it
             * (so creating its dict later races with nobody). */
            snapSlotSetState(ctx, s, didx, SNAP_SLOT_DONE);
            atomicIncr(ctx->slots_scanned, 1);
            if (ctx->slot_done) ctx->slot_done(ctx->privdata, s, didx, 0);
            ctx->slots_done++;
            ctx->pending[s]--;
            continue;
        }
        /* PENDING (zeroed state). Pause, bitmaps and overlay come on demand
         * through the guard: nothing per non-empty dict here. */
    }
    if (ctx->pending[s] == 0) return;   /* nothing to scan in this store */
    kvstoreSnapGuardSet(ctx->stores[s], snapOverlayGuard, &ctx->guards[s]);
    ctx->frozen[s] = 1;
}

/* The store's last slot completed: the guard comes off now, not at release,
 * because the caller of an abort (FLUSHALL, SWAPDB, DEBUG RELOAD) frees or
 * replaces the store right after, before snapScanRelease() runs. */
static void snapScanStoreDone(snapScanCtx *ctx, int s) {
    if (!ctx->frozen[s]) return;
    kvstoreSnapGuardSet(ctx->stores[s], NULL, NULL);
    ctx->frozen[s] = 0;
    if (ctx->store_done) ctx->store_done(ctx->privdata, s);
}

static void snapScanUnfreezeDict(snapScanCtx *ctx, int s, int didx) {
    if (!ctx->frozen[s]) return;
    if (!ctx->attached[s][didx]) return;   /* never paused */
    ctx->attached[s][didx] = 0;
    dict *d = kvstoreGetDict(ctx->stores[s], didx);
    if (d) {
        dictResumeRehashing(d);
        dictResumeAutoResize(d);
    }
}

/* Chain-walk callback on the scan thread: decode the entry and dispatch. */
struct snapScanEntryArgs { snapScanCtx *ctx; int s; int didx; };

static void snapScanEntryCb(void *privdata, const dictEntry *de, dictEntry **plink) {
    UNUSED(plink);
    struct snapScanEntryArgs *a = privdata;
    atomicIncr(a->ctx->keys_dispatched, 1);
    a->ctx->dispatch(a->ctx->privdata, a->s, a->didx, dictGetKV(de));
}

static inline int snapScanAborted(snapScanCtx *ctx) {
    int v;
    atomicGet(ctx->abort, v);
    return v;
}

/* Scan thread: read one frozen dict window by window. Returns 1 if aborted. */
static int snapScanWalkDict(snapScanCtx *ctx, int s, int didx, dict *d) {
    struct snapScanEntryArgs a = { ctx, s, didx };
    int rehashing = dictIsRehashing(d);   /* stable: rehashing is paused */
    for (int t = 0; t < (rehashing ? 2 : 1); t++) {
        unsigned long size = DICTHT_SIZE(d->ht_size_exp[t]);
        /* Buckets of table 0 below rehashidx were moved to table 1 already. */
        unsigned long lo = (t == 0 && rehashing) ? (unsigned long)d->rehashidx : 0;
        while (lo < size) {
            unsigned long hi = lo + ctx->window_buckets;
            if (hi > size || hi < lo) hi = size;
            snapWindowPublish(d, t, lo, hi);
            snapWalkerQuiesce(ctx);
            if (snapScanAborted(ctx)) return 1;
            for (unsigned long i = lo; i < hi; i++) {
                if (snapScanBucketEmitted(ctx, s, didx, t, i)) {
                    atomicIncr(ctx->buckets_skipped, 1);
                    continue;
                }
                dictWalkBucketRange(d, t, i, i + 1, snapScanEntryCb, &a);
            }
            if (snapScanAborted(ctx)) return 1;
            if (ctx->window_done) ctx->window_done(ctx->privdata);
            lo = hi;
        }
    }
    return 0;
}

static void snapScanPublishScanned(snapScanCtx *ctx, int s, int didx) {
    snapSlotSetState(ctx, s, didx, SNAP_SLOT_SCANNED);
    long wr;
    atomicGet(ctx->ring_wr, wr);
    ctx->ring[wr] = SNAP_SCAN_ENC(s, didx);
    atomicSetWithSync(ctx->ring_wr, wr + 1);   /* release: ring[wr] visible before wr+1 */
    atomicIncr(ctx->slots_scanned, 1);
    triggerEventNotifier(ctx->en);
}

static void *snapScanThreadMain(void *arg) {
    snapScanCtx *ctx = arg;
    redis_set_thread_title("snap_scan");
    makeThreadKillable();
    snap_on_scan_thread = 1;
    if (ctx->thread_init) ctx->thread_init(ctx->privdata);

    for (int s = 0; s < ctx->num_stores && !snapScanAborted(ctx); s++) {
        for (int didx = 0; didx < ctx->num_dicts[s]; didx++) {
            if (snapScanAborted(ctx)) break;
            /* Empty at freeze: completed by the main thread already, and its
             * dict pointer may be changing under us. Never touch it. */
            if (snapSlotGetState(ctx, s, didx) == SNAP_SLOT_DONE) continue;
            dict *d = kvstoreGetDict(ctx->stores[s], didx);
            snapSlotSetState(ctx, s, didx, SNAP_SLOT_SCANNING);
            /* Claim the whole dict before reading its layout: once every
             * section open at this point has closed, any main-thread access
             * pauses the dict first (guard), so table sizes and rehashidx
             * hold still for the walk. */
            snapWindowPublish(d, -1, 0, 0);
            snapWalkerQuiesce(ctx);
            if (snapScanAborted(ctx)) break;
            if (snapScanWalkDict(ctx, s, didx, d))
                break;   /* Aborted: leave the slot SCANNING; snapScanAbort() completes it. */
            snapScanPublishScanned(ctx, s, didx);
        }
    }
    /* No window: from here on the main thread may write every bucket. Once
     * the section that may still be emitting has closed, everything the main
     * thread produced is in the queue: drain it before the end marker. */
    snapWindowPublish(NULL, 0, 0, 0);
    snapWalkerQuiesce(ctx);
    if (ctx->window_done) ctx->window_done(ctx->privdata);
    if (ctx->thread_done) ctx->thread_done(ctx->privdata, snapScanAborted(ctx));
    atomicSetWithSync(ctx->finished, 1);
    triggerEventNotifier(ctx->en);
    return NULL;
}

/* Main thread. Freeze every store: from here on the dicts are the fork-instant
 * keyspace and all writes go to the overlay. Nothing else may run on the main
 * thread between this call and the fork of the writer child. */
void snapScanFreeze(snapScanCtx *ctx) {
    serverAssert(!ctx->started && !ctx->frozen_all);
    snapWindowPublish(NULL, 0, 0, 0);   /* no thread yet: drop any stale window of a previous save */
    for (int s = 0; s < ctx->num_stores; s++) snapScanFreezeStore(ctx, s);
    ctx->frozen_all = 1;
}

/* Main thread. Spawn the scan thread over an already frozen keyspace. */
int snapScanStartThread(snapScanCtx *ctx) {
    serverAssert(!ctx->started && ctx->frozen_all);
    if (pthread_create(&ctx->tid, NULL, snapScanThreadMain, ctx) != 0) {
        serverLog(LL_WARNING, "snapScanStartThread: pthread_create failed: %s", strerror(errno));
        return C_ERR;
    }
    ctx->started = 1;
    return C_OK;
}

static void snapScanCompleteSlot(snapScanCtx *ctx, int s, int didx, int aborted);

int snapScanStart(snapScanCtx *ctx) {
    snapScanFreeze(ctx);
    if (snapScanStartThread(ctx) != C_OK) {
        /* Nothing was scanned: complete every slot as aborted (merges an
         * empty overlay, unfreezes). */
        for (int s = 0; s < ctx->num_stores; s++)
            for (int didx = 0; didx < ctx->num_dicts[s]; didx++)
                snapScanCompleteSlot(ctx, s, didx, 1);
        ctx->frozen_all = 0;
        return C_ERR;
    }
    return C_OK;
}

/* Thread-side request to stop (e.g. the channel to the child broke). */
static inline void snapScanRequestAbort(snapScanCtx *ctx) {
    atomicSetWithSync(ctx->abort, 1);
}

/* Main thread: finish one slot (release the side table, slot-done hook,
 * unfreeze, publish DONE). */
static void snapScanCompleteSlot(snapScanCtx *ctx, int s, int didx, int aborted) {
    /* The thread no longer reads this slot: whatever the side table still
     * holds (normally nothing: it drains as the window moves) is applied to
     * the real dict and the dict is unfrozen. */
    snapScanBitmapFree(ctx, s, didx);
    if (ctx->has_ov) kvstoreOverlayRelease(ctx->stores[s], didx);
    if (ctx->slot_done) ctx->slot_done(ctx->privdata, s, didx, aborted);
    snapScanUnfreezeDict(ctx, s, didx);
    snapSlotSetState(ctx, s, didx, SNAP_SLOT_DONE);
    ctx->slots_done++;
    if (--ctx->pending[s] == 0) snapScanStoreDone(ctx, s);
}

int snapScanHandleCompletions(snapScanCtx *ctx) {
    int n = 0;
    long wr;
    atomicGetWithSync(ctx->ring_wr, wr);   /* acquire */
    while (ctx->ring_rd < wr) {
        uint64_t v = ctx->ring[ctx->ring_rd++];
        snapScanCompleteSlot(ctx, SNAP_SCAN_DEC_STORE(v), SNAP_SCAN_DEC_DIDX(v), 0);
        n++;
    }
    return n;
}

int snapScanIsFinished(snapScanCtx *ctx) {
    int v;
    atomicGetWithSync(ctx->finished, v);
    return v;
}

static void snapScanJoin(snapScanCtx *ctx) {
    if (ctx->started && !ctx->joined) {
        pthread_join(ctx->tid, NULL);
        ctx->joined = 1;
    }
}

void snapScanAbort(snapScanCtx *ctx) {
    if (!ctx->started) {
        /* Frozen but no thread (fork failure path): unfreeze everything. */
        if (ctx->frozen_all) {
            for (int s = 0; s < ctx->num_stores; s++)
                for (int didx = 0; didx < ctx->num_dicts[s]; didx++)
                    if (snapSlotGetState(ctx, s, didx) != SNAP_SLOT_DONE)
                        snapScanCompleteSlot(ctx, s, didx, 1);
        }
        return;
    }
    atomicSetWithSync(ctx->abort, 1);
    snapScanJoin(ctx);
    /* Fully scanned slots complete normally. */
    snapScanHandleCompletions(ctx);
    /* Everything else was interrupted. */
    for (int s = 0; s < ctx->num_stores; s++) {
        for (int didx = 0; didx < ctx->num_dicts[s]; didx++) {
            if (snapSlotGetState(ctx, s, didx) != SNAP_SLOT_DONE)
                snapScanCompleteSlot(ctx, s, didx, 1);
        }
    }
}

void snapScanRelease(snapScanCtx *ctx) {
    if (ctx == NULL) return;
    snapScanJoin(ctx);
    if (ctx->started) {
        /* Drain the tail, then complete whatever the thread left owed (it
         * stopped early on a broken channel) so no slot stays frozen. */
        snapScanHandleCompletions(ctx);
        for (int s = 0; s < ctx->num_stores; s++)
            for (int didx = 0; didx < ctx->num_dicts[s]; didx++)
                if (snapSlotGetState(ctx, s, didx) != SNAP_SLOT_DONE)
                    snapScanCompleteSlot(ctx, s, didx, 1);
        serverAssert(ctx->slots_done == ctx->slots_total);
    }
    for (int s = 0; s < ctx->num_stores; s++) {
        serverAssert(!ctx->frozen[s]);   /* every slot completed: the guard is already off */
        for (int didx = 0; didx < ctx->num_dicts[s]; didx++) snapScanBitmapFree(ctx, s, didx);
        zfree(ctx->bits[s]);
        zfree(ctx->state[s]);
        zfree(ctx->attached[s]);
    }
    zfree(ctx->bits);
    zfree(ctx->state);
    zfree(ctx->attached);
    zfree(ctx->guards);
    zfree(ctx->pending);
    zfree(ctx->num_dicts);
    zfree(ctx->frozen);
    zfree(ctx->stores);
    zfree(ctx->ring);
    freeEventNotifier(ctx->en);
    zfree(ctx);
}

int snapScanSlotState(snapScanCtx *ctx, int store_idx, int didx) {
    return snapSlotGetState(ctx, store_idx, didx);
}

int snapScanKeyIsOwed(snapScanCtx *ctx, int store_idx, int didx) {
    return snapSlotGetState(ctx, store_idx, didx) != SNAP_SLOT_DONE;
}

int snapScanReadEventFd(snapScanCtx *ctx) {
    return ctx->en ? getReadEventFd(ctx->en) : -1;
}

void snapScanAeHandler(struct aeEventLoop *el, int fd, void *privdata, int mask) {
    UNUSED(el);
    UNUSED(fd);
    UNUSED(mask);
    snapScanCtx *ctx = privdata;
    handleEventNotifier(ctx->en);
    snapScanHandleCompletions(ctx);
}

void snapScanProgress(snapScanCtx *ctx, long *slots_total, long *slots_scanned,
                      long *slots_done, long long *keys_dispatched)
{
    if (slots_total) *slots_total = ctx->slots_total;
    if (slots_scanned) atomicGet(ctx->slots_scanned, *slots_scanned);
    if (slots_done) *slots_done = ctx->slots_done;
    if (keys_dispatched) atomicGet(ctx->keys_dispatched, *keys_dispatched);
}

/* ============================================================================
 * Hybrid BGSAVE: orchestration, channel, child writer
 * ============================================================================
 *
 * Fork-instant sequence (main thread, nothing interleaves):
 *   1. capture per-db (and per-slot) key/expire counts for RESIZEDB/SLOT_INFO
 *   2. snapScanFreeze(): every keyspace dict is frozen, all writes now go to
 *      the overlay (clone-on-write), reads consult the overlay first
 *   3. redisFork(): the child inherits the frozen keyspace's SNAP_FORK values
 *      (CoW) but none of the DONTFORK memory (headers, NOFORK values, tables)
 *   4. snapScanStartThread(): the parent thread walks the frozen dicts and
 *      streams records to the child over a pipe
 *
 * The child owns the output file: header (aux, functions, module aux),
 * SELECTDB/RESIZEDB/SLOT_INFO tracking per message, records, footer, fsync,
 * rename. Completion and abort both flow through the stock child machinery
 * (checkChildrenDone -> backgroundSaveDoneHandler -> snapSaveChildDone).
 */

#define SNAP_MSG_BUFFER 1     /* pre-encoded records of one (db, slot) */
#define SNAP_MSG_KVOBJ  2     /* whole kvobj allocation copy of a SNAP_FORK key */
#define SNAP_MSG_END    3
#define SNAP_MSG_ABORT  4
#define SNAP_STAGE_BYTES (64 * 1024)   /* staging buffer flush threshold */

typedef struct snapMsgHdr {
    uint32_t type;
    int32_t dbid;
    int32_t slot;
    uint32_t len;        /* payload bytes */
    uint32_t robj_off;   /* KVOBJ: offset of the robj inside the copied allocation */
    uint64_t src_addr;   /* KVOBJ: parent address of the copied allocation, to relocate
                          * pointers into it (an EMBSTR value points inside its kvobj) */
} snapMsgHdr;

typedef struct snapDbSizes {
    unsigned long long keys, expires;
    unsigned long long *slot_keys, *slot_expires;   /* cluster mode */
    int nslots;
} snapDbSizes;

/* A db flushed during the save: its fork-instant structures, kept alive for
 * the scan and freed (on BIO, as FLUSHDB ASYNC does) when the store completes. */
typedef struct snapFlushedDb {
    int set;
    kvstore *keys, *expires, *blessed;
    void *subexpires;
    dict *stream_idmp_keys;
} snapFlushedDb;

struct snapSave {
    snapScanCtx *scan;
    kvstore **stores;          /* per store (db id at the fork instant): the kvstore being scanned */
    void **privs;              /* &store_priv[s] per store */
    snapStorePriv *store_priv;
    snapFlushedDb *flushed;    /* per store */
    int dbnum;
    int pipe_rd, pipe_wr;
    /* Spool (TASK-1.30): the scan thread's BUFFER messages go to an unlinked
     * file instead of the pipe, so the child's big values and the scan's small
     * values overlap without a memory bound. -1: pipe for everything. */
    int spool_wr, spool_rd;
    redisAtomic size_t spool_bytes;   /* written by the scan thread this save */
    int spilling;                     /* scan thread: queue reached its bound, BUFFERs go to the spool until it drains to half */
    snapDbSizes *sizes;
    int req, rdbflags;
    char *filename;
    rdbSaveInfo rsi;
    int has_rsi;
    int ae_registered;
    int aborted;               /* snapSaveAbort() ran; waiting for the child to exit */
    int hard_mark_hit;         /* retained bytes passed the configured maximum */
    int failed;                /* a defined failure of ours (hard mark): report status err */
    size_t retained_bytes;     /* originals kept alive by the overlay (main thread) */
    size_t retained_peak;
    redisAtomic int key_save_delay;   /* rdb-key-save-delay, copied for the scan thread */
    int maxmemory_policy, rdb_compression;   /* config frozen at start for the scan thread */
    long long inplace_writes;  /* writes served in place on scanned keys (main thread) */
    /* Scan-thread staging buffer (single thread in M1). */
    sds stage;
    int stage_dbid, stage_slot;
    redisAtomic long long keys_nofork, keys_fork, bytes_sent, msgs_sent;
    /* Main-thread emission (spec 11.z): records the main thread serialized
     * before writing a bucket the thread had not reached. mq_lock guards
     * mq (ready messages) and mstage (the partial BUFFER of one (db, slot));
     * the scan thread takes both at every window boundary and forwards them. */
    pthread_mutex_t mq_lock;
    list *mq;
    sds mstage;
    int mstage_dbid, mstage_slot;
    sds mscratch;                       /* main thread only: one record at a time */
    redisAtomic size_t mq_bytes;        /* payload bytes queued and not yet sent */
    long long main_buckets, main_keys;  /* main thread only */
    /* Outbound queue (TASK-1.27): every message the scan thread produces or
     * forwards waits here for the pipe, which is written non-blockingly, so a
     * child busy inside one huge fork-class value does not stall the scan.
     * Owned by the scan thread; oq_bytes and queued_peak are read by INFO. */
    list *oq;                           /* sds: header followed by payload */
    list *oq_pool;                      /* drained message buffers kept for reuse (scan thread) */
    size_t oq_head_off;                 /* bytes of the head message already written */
    size_t oq_limit;                    /* bgsave-hybrid-max-queued-bytes at save start, 0 = none */
    redisAtomic size_t oq_bytes;        /* allocation bytes of the messages queued for the pipe */
    redisAtomic size_t queued_peak;     /* max of oq_bytes + mq_bytes seen by the scan thread */
    /* Output target (TASK-1.14). A BGSAVE or disk-based sync writes a temp
     * file renamed at the end. A diskless sync writes the replicas' sockets:
     * the child owns them, either directly (rdb-channel connections, blocking)
     * or through the stock rdb pipe that the parent fans out non-blockingly. */
    int to_sockets;
    int rdb_channel;
    connection **conns;        /* rdb-channel: the replica connections the child writes */
    int numconns;
    int rdb_pipe_write;        /* pipe shape: child's end of server.rdb_pipe_read */
    int safe_to_exit_pipe;     /* pipe shape: the child waits on it before exiting */
};

typedef struct snapQueuedMsg {
    snapMsgHdr h;
    sds payload;
} snapQueuedMsg;
/* Thread-local: set by the main thread around a bucket emission; rdb.c reads
 * it on whichever thread serializes (the scan thread's copy is always 0). */
static __thread int snap_main_emitting = 0;
int snapMainEmitting(void) { return snap_main_emitting; }

static snapSave *snap_save = NULL;   /* the running hybrid save, main thread */
static size_t snap_last_retained_peak = 0;
static size_t snap_last_queued_peak = 0;
static long long snap_last_inplace_writes = 0;

long long snapSaveInplaceWrites(void) { return snap_save ? snap_save->inplace_writes : 0; }
long long snapSaveLastInplaceWrites(void) { return snap_last_inplace_writes; }


int snapSaveInProgress(void) { return snap_save != NULL && !snap_save->aborted; }
size_t snapSaveRetainedBytes(void) { return snap_save ? snap_save->retained_bytes : 0; }
size_t snapSaveLastRetainedPeak(void) { return snap_last_retained_peak; }
size_t snapSaveBitmapBytes(void) { return snap_save ? snapScanBitmapBytes(snap_save->scan) : 0; }

size_t snapSaveQueuedBytes(void) {
    if (!snap_save) return 0;
    size_t n, o;
    atomicGet(snap_save->mq_bytes, n);
    atomicGet(snap_save->oq_bytes, o);
    return n + o;
}
size_t snapSaveLastQueuedPeak(void) { return snap_last_queued_peak; }
static size_t snap_last_spool_bytes = 0;
size_t snapSaveSpoolBytes(void) {
    if (!snap_save) return 0;
    size_t n;
    atomicGet(snap_save->spool_bytes, n);
    return n;
}
size_t snapSaveLastSpoolBytes(void) { return snap_last_spool_bytes; }
long long snapSaveMainBuckets(void) { return snap_save ? snap_save->main_buckets : 0; }
long long snapSaveMainKeys(void) { return snap_save ? snap_save->main_keys : 0; }

/* ---- Bucket position and main-thread emission (spec 11.z) ---- */

static int snapSendMsg(snapSave *sv, uint32_t type, int dbid, int slot,
                       const void *payload, uint32_t len, uint32_t robj_off, uint64_t src_addr);
static void snapStageFlush(snapSave *sv);

#define SNAP_POS_BEHIND 0   /* the thread already read this bucket */
#define SNAP_POS_INSIDE 1   /* the thread is reading it now */
#define SNAP_POS_AHEAD  2   /* the thread has not reached it */

/* Main thread, inside a write section. */
static int snapBucketPosition(snapScanCtx *ctx, int s, int didx, dict *d, int table, unsigned long idx) {
    uint8_t st = snapSlotGetState(ctx, s, didx);
    if (st == SNAP_SLOT_PENDING) return SNAP_POS_AHEAD;
    if (st != SNAP_SLOT_SCANNING) return SNAP_POS_BEHIND;
    uint64_t seq;
    atomicGetWithSync(snap_win_seq, seq);
    if (seq == 0) return SNAP_POS_AHEAD;
    const snapWindow *w = &snap_win[seq & 1];
    /* SCANNING but the window is another dict's (or none): the thread set the
     * state and has not published its first window of this dict yet. */
    if (w->d != d) return SNAP_POS_AHEAD;
    if (w->table < 0) return SNAP_POS_INSIDE;   /* the thread is reading the dict's layout */
    if (table != w->table) return table < w->table ? SNAP_POS_BEHIND : SNAP_POS_AHEAD;
    if (idx < w->lo) return SNAP_POS_BEHIND;
    if (idx >= w->hi) return SNAP_POS_AHEAD;
    return SNAP_POS_INSIDE;
}

static void snapSaveQueuePushLocked(snapSave *sv, uint32_t type, int dbid, int slot, sds payload, uint32_t robj_off, uint64_t src_addr) {
    snapQueuedMsg *m = zmalloc(sizeof(*m));
    m->h.type = type;
    m->h.dbid = dbid;
    m->h.slot = slot;
    m->h.len = (uint32_t)sdslen(payload);
    m->h.robj_off = robj_off;
    m->h.src_addr = src_addr;
    m->payload = payload;
    listAddNodeTail(sv->mq, m);
    atomicIncr(sv->mq_bytes, sdslen(payload));
}

/* Main thread, mq_lock held: hand the partial BUFFER over as a message. */
static void snapSaveMainStageFlushLocked(snapSave *sv) {
    if (sdslen(sv->mstage) == 0) return;
    snapSaveQueuePushLocked(sv, SNAP_MSG_BUFFER, sv->mstage_dbid, sv->mstage_slot, sv->mstage, 0, 0);
    sv->mstage = sdsempty();
}

/* Main thread: serialize the whole chain of bucket (table, idx) of dict d of
 * slot (s, didx) as it is now, i.e. still at its fork-instant state (no key of
 * a not yet emitted bucket ahead of the thread was ever written in place). */
static void snapSaveEmitBucket(snapSave *sv, int s, int didx, dict *d, int table, unsigned long idx) {
    snap_main_emitting = 1;
    for (dictEntry *de = d->ht_table[table][idx]; de; de = dictGetNext(de)) {
        kvobj *kv = dictGetKV(de);
        int cls = snapKeyClass(kv);
        if (cls == SNAP_NOFORK && getModuleMetaBits(kv->metabits)) cls = SNAP_FORK;
        if (cls == SNAP_FORK) {
            /* The child resolves kv->ptr in its own memory: it must be forkable. */
            char *alloc = kvobjGetAllocPtr(kv);
            debugServerAssert(kv->encoding == OBJ_ENCODING_INT || kv->encoding == OBJ_ENCODING_EMBSTR ||
                              !zmalloc_is_nofork(kv->ptr));
            sds copy = sdsnewlen(alloc, zmalloc_size(alloc));
            pthread_mutex_lock(&sv->mq_lock);
            snapSaveQueuePushLocked(sv, SNAP_MSG_KVOBJ, s, didx, copy, (uint32_t)((char *)kv - alloc), (uint64_t)(uintptr_t)alloc);
            pthread_mutex_unlock(&sv->mq_lock);
        } else {
            rio r;
            sdsclear(sv->mscratch);
            rioInitWithBuffer(&r, sv->mscratch);
            robj key;
            initStaticStringObject(key, kvobjGetKey(kv));
            if (rdbSaveKeyValuePair(&r, &key, kv, kvobjGetExpire(kv), s) == -1)
                serverPanic("Hybrid BGSAVE: failed to serialize a key on the main thread");
            sv->mscratch = r.io.buffer.ptr;
            pthread_mutex_lock(&sv->mq_lock);
            if (sv->mstage_dbid != s || sv->mstage_slot != didx) {
                snapSaveMainStageFlushLocked(sv);
                sv->mstage_dbid = s;
                sv->mstage_slot = didx;
            }
            sv->mstage = sdscatlen(sv->mstage, sv->mscratch, sdslen(sv->mscratch));
            atomicIncr(sv->mq_bytes, sdslen(sv->mscratch));
            if (sdslen(sv->mstage) >= SNAP_STAGE_BYTES) snapSaveMainStageFlushLocked(sv);
            pthread_mutex_unlock(&sv->mq_lock);
        }
        sv->main_keys++;
    }
    sv->main_buckets++;
    snap_main_emitting = 0;
}

/* Scan thread, at every window boundary and before END: forward what the
 * main thread emitted. */
static void snapSaveWindowDone(void *priv) {
    snapSave *sv = priv;
    pthread_mutex_lock(&sv->mq_lock);
    list *ready = sv->mq;
    sv->mq = listCreate();
    if (sdslen(sv->mstage)) snapSaveMainStageFlushLocked(sv);
    /* The flush above moved the stage into 'sv->mq': move it over too. */
    if (listLength(sv->mq)) {
        listJoin(ready, sv->mq);
    }
    pthread_mutex_unlock(&sv->mq_lock);
    if (listLength(ready)) snapStageFlush(sv);   /* this thread's partial buffer first (order is free anyway) */
    listNode *ln;
    while ((ln = listFirst(ready)) != NULL) {
        snapQueuedMsg *m = listNodeValue(ln);
        if (!snapScanAborted(sv->scan))
            snapSendMsg(sv, m->h.type, m->h.dbid, m->h.slot, m->payload, m->h.len, m->h.robj_off, m->h.src_addr);
        atomicDecr(sv->mq_bytes, m->h.len);
        sdsfree(m->payload);
        zfree(m);
        listDelNode(ready, ln);
    }
    listRelease(ready);
}

/* Inside a write section: is the frozen chain of bucket (table, idx) of dict
 * d of slot (s, didx) still owed to the snapshot, i.e. neither read by the
 * thread already nor emitted by the main thread? Only an owed chain must not
 * be written in place. *pos receives the bucket's position.
 *
 * The emitted bit is checked whatever the position: one section may see the
 * bucket ahead, emit it and set the bit, while the thread publishes the next
 * window over that bucket and then waits for the section to close. A later
 * look in the same section sees the bucket inside the window, but the thread
 * will skip it (snapScanWalkDict reads the bit), so it is not owed. */
static int snapBucketOwed(snapScanCtx *ctx, int s, int didx, dict *d, int table, unsigned long idx, int *pos) {
    *pos = snapBucketPosition(ctx, s, didx, d, table, idx);
    if (*pos == SNAP_POS_BEHIND) return 0;
    return !snapScanBucketEmitted(ctx, s, didx, table, idx);
}

/* Overlay 'prepare' hook (privdata = redisDb *): may the frozen chain of
 * 'key' be written in place? Emits the bucket first when the thread has not
 * reached it. */
static int snapOverlayPrepare(void *privdata, int didx, dict *d, const void *key) {
    int s = ((snapStorePriv *)privdata)->s;
    snapSave *sv = snap_save;
    if (!sv || sv->aborted) return KVSTORE_OV_INPLACE;
    int table, pos;
    unsigned long idx;
    dictKeyBucket(d, key, &table, &idx);
    if (!snapBucketOwed(sv->scan, s, didx, d, table, idx, &pos)) {
        sv->inplace_writes++;
        server.stat_snap_inplace_writes++;
        return KVSTORE_OV_INPLACE;
    }
    if (pos == SNAP_POS_INSIDE) return KVSTORE_OV_DEFER;
    snapSaveEmitBucket(sv, s, didx, d, table, idx);
    snapScanMarkBucketEmitted(sv->scan, s, didx, table, idx);
    return KVSTORE_OV_INPLACE;
}

/* Overlay 'drainable' hook: may the side-table entry of 'key' be applied to
 * the frozen chain now? Yes once the walker has passed its bucket (the
 * position is stable inside the section the caller holds). */
static int snapOverlayDrainable(void *privdata, int didx, dict *d, const void *key) {
    int s = ((snapStorePriv *)privdata)->s;
    snapSave *sv = snap_save;
    if (!sv) return 1;
    int table;
    unsigned long idx;
    dictKeyBucket(d, key, &table, &idx);
    return snapBucketPosition(sv->scan, s, didx, d, table, idx) == SNAP_POS_BEHIND;
}

/* Main thread, inside a write section: apply the side-table entries whose
 * bucket the walker has passed (spec 11.z, "drained once W has moved").
 * Called at the end of every section and from cron; a no-op until the window
 * has moved since the last drain, since nothing new can be drainable. */
static uint64_t snap_drain_seq = 0;
static void snapSaveDrainSideTables(void) {
    snapSave *sv = snap_save;
    if (!sv || sv->aborted) return;
    uint64_t seq;
    atomicGetWithSync(snap_win_seq, seq);
    if (seq == snap_drain_seq) return;
    snap_drain_seq = seq;
    for (int j = 0; j < sv->dbnum; j++) kvstoreOverlayDrain(server.db[j].keys);
}

/* Side-table entries (clones, adds, tombstones) of the running save. */
long snapSaveSideEntries(void) {
    snapSave *sv = snap_save;
    if (!sv || sv->aborted) return 0;
    long n = 0;
    for (int s = 0; s < sv->dbnum; s++)
        if (sv->stores[s]) n += kvstoreOverlayPending(sv->stores[s]);
    return n;
}

/* ---- Stores versus dbs (SWAPDB, FLUSHDB during a save; TASK-1.24) ---- */

/* Store index of a kvstore being scanned, -1 if the save does not own it
 * (a keyspace created after the fork, e.g. by a FLUSHDB during the save). */
static int snapStoreIndexOf(snapSave *sv, kvstore *kvs) {
    for (int s = 0; s < sv->dbnum; s++)
        if (sv->stores[s] && sv->stores[s] == kvs) return s;
    return -1;
}

/* The db that holds store s right now, NULL when none (FLUSHDB detached it). */
static redisDb *snapStoreOwnerDb(snapSave *sv, int s) {
    if (!sv->stores[s]) return NULL;   /* flushed and already freed */
    for (int j = 0; j < server.dbnum; j++)
        if (server.db[j].keys == sv->stores[s]) return &server.db[j];
    return NULL;
}

int snapSaveOwnsKvstore(kvstore *kvs) {
    snapSave *sv = snap_save;
    if (!sv || sv->aborted) return 0;
    return snapStoreIndexOf(sv, kvs) >= 0;
}

/* FLUSHDB during a save (db.c emptyDbStructure): the db swapped in fresh,
 * empty structures and hands its fork-instant ones to the save. The scan
 * thread keeps walking 'keys' (the live db is already empty for everyone
 * else); everything is freed on BIO when the store completes. Returns 1
 * when adopted, 0 when the caller must free them itself. */
int snapSaveDetachDb(kvstore *keys, kvstore *expires, void *subexpires, dict *stream_idmp_keys, kvstore *blessed) {
    snapSave *sv = snap_save;
    if (!sv || sv->aborted) return 0;
    int s = snapStoreIndexOf(sv, keys);
    if (s < 0 || sv->flushed[s].set) return 0;
    sv->flushed[s] = (snapFlushedDb){ 1, keys, expires, blessed, subexpires, stream_idmp_keys };
    serverLog(LL_NOTICE, "Hybrid BGSAVE: db %d flushed during the save, its keyspace stays with the save until its slots complete", s);
    return 1;
}

static void snapSaveFreeFlushed(snapSave *sv, int s) {
    snapFlushedDb *f = &sv->flushed[s];
    if (!f->set) return;
    f->set = 0;
    /* The store is gone for us from here on (INFO counters, store lookups). */
    sv->stores[s] = NULL;
    emptyDbDataAsync(f->keys, f->expires, f->subexpires, f->stream_idmp_keys, f->blessed, NULL, NULL);
}

/* Scan callback: store s has no slot left owed (completed or aborted). */
static void snapSaveStoreDone(void *privdata, int s) {
    snapSave *sv = privdata;
    snapSaveFreeFlushed(sv, s);
}

int snapKvobjIsOwed(redisDb *db, kvobj *kv) {
    snapSave *sv = snap_save;
    if (!sv || sv->aborted) return 0;
    int s = snapStoreIndexOf(sv, db->keys);
    if (s < 0) return 0;   /* a keyspace created after the fork: nothing in it is owed */
    sds key = kvobjGetKey(kv);
    int slot = server.cluster_enabled ? calculateKeySlot(key) : 0;
    uint8_t st = snapSlotGetState(sv->scan, s, slot);
    if (st == SNAP_SLOT_SCANNED || st == SNAP_SLOT_DONE) return 0;
    dict *d = kvstoreGetDict(db->keys, slot);
    if (!d) return 0;
    snapFunnelOpen();   /* the window must hold still while we look */
    int table, pos;
    unsigned long idx;
    dictKeyBucket(d, key, &table, &idx);
    if (!snapBucketOwed(sv->scan, s, slot, d, table, idx, &pos)) return 0;
    /* Ahead and not emitted, or being read: the frozen entry itself is owed,
     * a temp-dict clone of it is not. */
    dictEntryLink l = dictFindLink(d, key, NULL);
    return l && dictGetKV(*l) == kv;
}

/* Debug-build check at the write funnels. A SNAP_FORK value whose top-level
 * pointer never moves (stream, hashtable, skiplist, quicklist, module type)
 * may be mutated in place even while owed: the child serializes its own
 * copy-on-write image from the fork instant, and the header copy taken later
 * still points at the same object (only its LRU field may be a little stale).
 * XACK/XACKDEL reach such values through a read lookup (write-path audit,
 * streams row), which is why this exemption exists. Single-allocation values
 * (raw strings, listpacks, intsets) may move on write and stay checked. */
int snapKvobjIsFrozenOriginal(redisDb *db, kvobj *kv) {
    if (kv->type == OBJ_STREAM || kv->type == OBJ_MODULE ||
        kv->encoding == OBJ_ENCODING_HT || kv->encoding == OBJ_ENCODING_SKIPLIST ||
        kv->encoding == OBJ_ENCODING_QUICKLIST)
        return 0;
    return snapKvobjIsOwed(db, kv);
}

void snapSaveSetKeySaveDelay(int usec) {
    if (snap_save) atomicSet(snap_save->key_save_delay, usec);
}

/* Bytes of an original the overlay keeps alive: its kvobj allocation plus,
 * when the clone took a private copy, its value allocation(s). */
static size_t snapOriginalRetainedBytes(kvobj *orig, int value_shared) {
    size_t n = zmalloc_size(kvobjGetAllocPtr(orig));
    if (value_shared || snapValueIsInline(orig->type, orig->encoding)) return n;
    switch (orig->encoding) {
    case OBJ_ENCODING_RAW: n += zmalloc_size(sdsAllocPtr(orig->ptr)); break;
    case OBJ_ENCODING_LISTPACK:
    case OBJ_ENCODING_INTSET: n += zmalloc_size(orig->ptr); break;
    case OBJ_ENCODING_LISTPACK_EX: {
        listpackEx *o = orig->ptr;
        n += zmalloc_size(o) + zmalloc_size(o->lp);
        break;
    }
    default: break;
    }
    return n;
}

/* Main thread, from the overlay clone callback: account a retained original
 * and arm the hard-mark abort. The abort itself runs from snapSaveCron():
 * we are inside a keyspace write here and the overlay must stay intact. */
static void snapSaveRetainedAdd(kvobj *orig, int value_shared) {
    if (!snap_save || snap_save->aborted) return;
    snapSave *sv = snap_save;
    sv->retained_bytes += snapOriginalRetainedBytes(orig, value_shared);
    if (sv->retained_bytes > sv->retained_peak) sv->retained_peak = sv->retained_bytes;
    if (server.bgsave_hybrid_max_retained_bytes && !sv->hard_mark_hit &&
        sv->retained_bytes > server.bgsave_hybrid_max_retained_bytes) {
        sv->hard_mark_hit = 1;
        serverLog(LL_WARNING, "Hybrid BGSAVE: overlay retains %zu bytes, past bgsave-hybrid-max-retained-bytes (%zu): aborting the save",
                  sv->retained_bytes, server.bgsave_hybrid_max_retained_bytes);
    }
}

static void snapSaveRetainedSub(kvobj *orig, int value_shared) {
    if (!snap_save) return;
    size_t n = snapOriginalRetainedBytes(orig, value_shared);
    snap_save->retained_bytes = n > snap_save->retained_bytes ? 0 : snap_save->retained_bytes - n;
}

void snapSaveCron(void) {
    if (!snap_save || snap_save->aborted) return;
    snapSave *sv = snap_save;
    /* Writes may have stopped since the window moved: drain from here too,
     * so a side-table entry never outlives the window by more than a tick. */
    if (snapSaveSideEntries()) {
        snapFunnelOpen();
        snapSaveDrainSideTables();
        snapFunnelClose();
    }
    size_t queued;
    atomicGet(sv->mq_bytes, queued);
    /* The peak reported by INFO covers every kind of save-time memory: the
     * overlay's retained originals and the records queued for the child on
     * both threads. The abort below counts only what the main thread holds:
     * the scan thread's queue waits for the child instead of growing. */
    size_t all_queued = snapSaveQueuedBytes();
    if (sv->retained_bytes + all_queued > sv->retained_peak) sv->retained_peak = sv->retained_bytes + all_queued;
    if (server.bgsave_hybrid_max_retained_bytes && !sv->hard_mark_hit) {
        if (sv->retained_bytes + queued > server.bgsave_hybrid_max_retained_bytes) {
            sv->hard_mark_hit = 1;
            serverLog(LL_WARNING, "Hybrid BGSAVE: %zu bytes retained plus %zu queued for the child, past bgsave-hybrid-max-retained-bytes (%zu): aborting the save",
                      sv->retained_bytes, queued, server.bgsave_hybrid_max_retained_bytes);
        }
    }
    if (!sv->hard_mark_hit) return;
    snap_save->failed = 1;
    server.stat_snap_hard_mark_aborts++;
    killRDBChild();   /* -> snapSaveAbort(): merges the overlay; the done handler releases */
}


void snapSaveProgress(long *slots_total, long *slots_done, long long *keys_nofork,
                      long long *keys_fork, long long *bytes_sent)
{
    *slots_total = *slots_done = 0;
    *keys_nofork = *keys_fork = *bytes_sent = 0;
    if (!snap_save || snap_save->aborted) return;
    snapScanProgress(snap_save->scan, slots_total, NULL, slots_done, NULL);
    atomicGet(snap_save->keys_nofork, *keys_nofork);
    atomicGet(snap_save->keys_fork, *keys_fork);
    atomicGet(snap_save->bytes_sent, *bytes_sent);
}

static int snapReadAll(int fd, void *p, size_t n) {
    char *c = p;
    while (n) {
        ssize_t r = read(fd, c, n);
        if (r < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        if (r == 0) { errno = 0; return -1; }   /* EOF */
        c += r;
        n -= r;
    }
    return 0;
}

/* Child: 'n' bytes of the spool at 'off', complete or not at all. 1 = got
 * them, 0 = not (all) written yet (the scan thread appends whole messages
 * with one write each, but a reader may observe a partial one), -1 = error. */
static int snapSpoolPread(int fd, void *p, size_t n, off_t off) {
    char *c = p;
    size_t got = 0;
    while (got < n) {
        ssize_t r = pread(fd, c + got, n - got, off + got);
        if (r < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        if (r == 0) return 0;
        got += r;
    }
    return 1;
}

/* Scan thread: append a whole message to the spool (blocking). */
static int snapSpoolWrite(snapSave *sv, const char *p, size_t n) {
    while (n) {
        ssize_t w = write(sv->spool_wr, p, n);
        if (w < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        p += w;
        n -= w;
    }
    return 0;
}

/* ---- Outbound queue to the child (scan thread only) ---- */

#define SNAP_DRAIN_TRY   0   /* write what the pipe takes, never wait */
#define SNAP_DRAIN_BOUND 1   /* wait until oq_bytes <= target; give up on abort */
#define SNAP_DRAIN_ALL   2   /* wait until the queue is empty, abort or not */

static void snapOutRecycle(snapSave *sv, sds m);
#define SNAP_OUT_POOL_MAX 16   /* staging buffers kept for reuse */
#define SNAP_STAGE_SLACK (8 * 1024)   /* headroom a record may take before the flush */

/* Scan thread: a message buffer with room for 'need' bytes. Allocation is
 * exact (no greedy doubling: these bytes are what the queue holds against the
 * bound and what used_memory reports) and the pool serves staging-sized
 * requests only, so a small KVOBJ or END message never pins a 64 KB buffer
 * in the queue with a few bytes in it. */
static sds snapOutTake(snapSave *sv, size_t need) {
    listNode *ln;
    if (need >= SNAP_STAGE_BYTES && (ln = listFirst(sv->oq_pool)) != NULL) {
        sds m = listNodeValue(ln);
        listDelNode(sv->oq_pool, ln);
        return sdsMakeRoomForNonGreedy(m, need);
    }
    return sdsMakeRoomForNonGreedy(sdsempty(), need);
}

/* Scan thread: a drained message goes back to the pool when it is a staging
 * buffer, is freed otherwise. */
static void snapOutRecycle(snapSave *sv, sds m) {
    size_t a = sdsalloc(m);
    if (a >= SNAP_STAGE_BYTES && a <= 4 * SNAP_STAGE_BYTES && listLength(sv->oq_pool) < SNAP_OUT_POOL_MAX) {
        sdsclear(m);
        listAddNodeTail(sv->oq_pool, m);
    } else {
        sdsfree(m);
    }
}

/* Scan thread: write queued messages to the (non-blocking) pipe. Returns -1
 * when the pipe is broken (child gone) or a BOUND wait saw an abort. */
static int snapOutDrain(snapSave *sv, int mode, size_t target) {
    listNode *ln;
    while ((ln = listFirst(sv->oq)) != NULL) {
        sds m = listNodeValue(ln);
        size_t len = sdslen(m);
        ssize_t w = write(sv->pipe_wr, m + sv->oq_head_off, len - sv->oq_head_off);
        if (w < 0) {
            if (errno == EINTR) continue;
            if (errno != EAGAIN && errno != EWOULDBLOCK) return -1;
            if (mode == SNAP_DRAIN_TRY) return 0;
            if (mode == SNAP_DRAIN_BOUND) {
                size_t q;
                atomicGet(sv->oq_bytes, q);
                if (q <= target) return 0;
                if (snapScanAborted(sv->scan)) return -1;
            }
            struct pollfd pfd = { .fd = sv->pipe_wr, .events = POLLOUT, .revents = 0 };
            int r = poll(&pfd, 1, 100);
            if (r < 0 && errno != EINTR) return -1;
            if (r > 0 && (pfd.revents & (POLLERR | POLLHUP)) && !(pfd.revents & POLLOUT)) return -1;
            continue;
        }
        sv->oq_head_off += w;
        if (sv->oq_head_off == len) {
            atomicDecr(sv->oq_bytes, sdsAllocSize(m));
            listDelNode(sv->oq, ln);
            snapOutRecycle(sv, m);
            sv->oq_head_off = 0;
        }
    }
    return 0;
}

static void snapOutClear(snapSave *sv) {
    listNode *ln;
    while ((ln = listFirst(sv->oq)) != NULL) {
        sdsfree(listNodeValue(ln));
        listDelNode(sv->oq, ln);
    }
    sv->oq_head_off = 0;
    atomicSet(sv->oq_bytes, 0);
}

/* Scan thread: queue a complete message (header + payload in 'm', owned by
 * the queue from here on) and push the pipe. Past the bound the thread waits
 * for the child rather than growing further; it never aborts for that (the
 * main-thread hard mark covers what the main thread retains and queues). A
 * broken pipe (child gone) aborts the scan. */
static int snapOutEnqueue(snapSave *sv, sds m, uint32_t type) {
    size_t n = sdslen(m);
    if (type == SNAP_MSG_BUFFER && sv->spool_wr != -1 && sv->oq_limit) {
        /* Queue first, spool as the overflow: push the pipe, and if the queue
         * is at its bound (the child is inside a big value) send this and the
         * following buffers to the spool, until the child has drained the
         * queue to half. On a dataset the child keeps up with nothing ever
         * touches the file. */
        if (snapOutDrain(sv, SNAP_DRAIN_TRY, 0) != 0) {
            snapOutRecycle(sv, m);
            snapScanRequestAbort(sv->scan);
            return -1;
        }
        size_t q;
        atomicGet(sv->oq_bytes, q);
        if (sv->spilling) {
            if (q <= sv->oq_limit / 2) sv->spilling = 0;
        } else if (q + sdsAllocSize(m) > sv->oq_limit) {
            sv->spilling = 1;
        }
    }
    if (type == SNAP_MSG_BUFFER && sv->spool_wr != -1 && (sv->spilling || !sv->oq_limit)) {
        int rc = snapSpoolWrite(sv, m, n);
        atomicIncr(sv->msgs_sent, 1);
        atomicIncr(sv->bytes_sent, (long long)n);
        atomicIncr(sv->spool_bytes, n);
        snapOutRecycle(sv, m);
        if (rc != 0) {
            snapScanRequestAbort(sv->scan);
            return -1;
        }
        return 0;
    }
    listAddNodeTail(sv->oq, m);
    size_t q, mqb, peak;
    atomicIncrGet(sv->oq_bytes, q, sdsAllocSize(m));   /* what the queue really holds */
    atomicGet(sv->mq_bytes, mqb);
    atomicGet(sv->queued_peak, peak);
    if (q + mqb > peak) atomicSet(sv->queued_peak, q + mqb);
    atomicIncr(sv->msgs_sent, 1);
    atomicIncr(sv->bytes_sent, (long long)n);
    int rc = snapOutDrain(sv, SNAP_DRAIN_TRY, 0);
    if (rc == 0 && sv->oq_limit) {
        atomicGet(sv->oq_bytes, q);
        if (q > sv->oq_limit) rc = snapOutDrain(sv, SNAP_DRAIN_BOUND, sv->oq_limit);
    }
    if (rc != 0) {
        snapScanRequestAbort(sv->scan);
        return -1;
    }
    return 0;
}

/* Scan thread: one message from a header and a payload it does not own. */
static int snapSendMsg(snapSave *sv, uint32_t type, int dbid, int slot,
                       const void *payload, uint32_t len, uint32_t robj_off, uint64_t src_addr)
{
    snapMsgHdr h = { type, dbid, slot, len, robj_off, src_addr };
    sds m = snapOutTake(sv, sizeof(h) + len);
    m = sdscatlen(m, &h, sizeof(h));
    if (len) m = sdscatlen(m, payload, len);
    return snapOutEnqueue(sv, m, type);
}

/* The staging buffer is a BUFFER message in the making: its first
 * sizeof(snapMsgHdr) bytes are reserved for the header, the records follow, and
 * the flush hands the whole buffer to the queue without a copy. */
static void snapStageReset(snapSave *sv) {
    sv->stage = snapOutTake(sv, sizeof(snapMsgHdr) + SNAP_STAGE_BYTES + SNAP_STAGE_SLACK);
    sdsIncrLen(sv->stage, sizeof(snapMsgHdr));
}

static void snapStageFlush(snapSave *sv) {
    if (sdslen(sv->stage) <= sizeof(snapMsgHdr)) return;
    snapMsgHdr h = { SNAP_MSG_BUFFER, sv->stage_dbid, sv->stage_slot,
                     (uint32_t)(sdslen(sv->stage) - sizeof(snapMsgHdr)), 0, 0 };
    memcpy(sv->stage, &h, sizeof(h));
    snapOutEnqueue(sv, sv->stage, SNAP_MSG_BUFFER);
    snapStageReset(sv);
}

/* Scan thread: one fork-instant key. Read-only on the keyspace. */
static void snapSaveDispatch(void *priv, int s, int didx, kvobj *kv) {
    snapSave *sv = priv;
    if (sv->stage_dbid != s || sv->stage_slot != didx) {
        snapStageFlush(sv);
        sv->stage_dbid = s;
        sv->stage_slot = didx;
    }
    int cls = snapKeyClass(kv);
    /* Module key metadata is serialized through module callbacks: child only. */
    if (cls == SNAP_NOFORK && getModuleMetaBits(kv->metabits)) cls = SNAP_FORK;

    if (cls == SNAP_FORK) {
        snapStageFlush(sv);   /* keep the child's stream in scan order */
        char *alloc = kvobjGetAllocPtr(kv);
        size_t len = zmalloc_size(alloc);
        /* The child resolves kv->ptr in its own memory: it must be forkable. */
        debugServerAssert(kv->encoding == OBJ_ENCODING_INT || kv->encoding == OBJ_ENCODING_EMBSTR ||
                          !zmalloc_is_nofork(kv->ptr));
        snapSendMsg(sv, SNAP_MSG_KVOBJ, s, didx, alloc, len, (uint32_t)((char *)kv - alloc), (uint64_t)(uintptr_t)alloc);
        atomicIncr(sv->keys_fork, 1);
    } else {
        rio r;
        rioInitWithBuffer(&r, sv->stage);
        robj key;
        initStaticStringObject(key, kvobjGetKey(kv));
        int delay;
        atomicGet(sv->key_save_delay, delay);
        while (delay > 0 && !snapScanAborted(sv->scan)) {
            int slice = delay > 10000 ? 10000 : delay;
            debugDelay(slice);
            delay -= slice;
        }
        if (rdbSaveKeyValuePair(&r, &key, kv, kvobjGetExpire(kv), s) == -1) {
            serverLog(LL_WARNING, "Hybrid BGSAVE: failed to serialize a key on the scan thread");
            snapScanRequestAbort(sv->scan);
        }
        sv->stage = r.io.buffer.ptr;
        atomicIncr(sv->keys_nofork, 1);
        if (sdslen(sv->stage) + SNAP_STAGE_SLACK > sdsalloc(sv->stage)) snapStageFlush(sv);
    }
}

/* Scan thread, before the first slot: publish the frozen config to this thread. */
static void snapSaveThreadInit(void *priv) {
    snapSave *sv = priv;
    snap_scan_maxmemory_policy = sv->maxmemory_policy;
    snap_scan_rdb_compression = sv->rdb_compression;
}


/* Scan thread: after the last slot (or on abort). */
static void snapSaveThreadDone(void *priv, int aborted) {
    snapSave *sv = priv;
    if (aborted) {
        /* Whatever is still queued is moot; the child must see ABORT (or die
         * first, which breaks the pipe and ends the wait). */
        snapOutClear(sv);
        snapSendMsg(sv, SNAP_MSG_ABORT, -1, -1, NULL, 0, 0, 0);
        snapOutDrain(sv, SNAP_DRAIN_ALL, 0);
    } else {
        snapStageFlush(sv);
        if (snapSendMsg(sv, SNAP_MSG_END, -1, -1, NULL, 0, 0, 0) == 0 &&
            snapOutDrain(sv, SNAP_DRAIN_ALL, 0) != 0)
            snapScanRequestAbort(sv->scan);
    }
    /* Our side of the spool is done: close it here, on this thread. The child
     * holds the other descriptor, so the file's pages (a gigabyte for a large
     * save) are released when the child exits, never by the main thread (the
     * last close of an unlinked file frees them synchronously: run 23 saw a
     * 56 ms PING spike from that in snapSaveFree). */
    if (sv->spool_wr != -1) {
        close(sv->spool_wr);
        sv->spool_wr = -1;
    }
}

/* ---- child ---- */

static int snapChildSelect(rio *rdb, snapSave *sv, int dbid, int slot, int *cur_db, int *cur_slot) {
    if (dbid != *cur_db) {
        if (rdbSaveType(rdb, RDB_OPCODE_SELECTDB) == -1) return -1;
        if (rdbSaveLen(rdb, dbid) == -1) return -1;
        if (rdbSaveType(rdb, RDB_OPCODE_RESIZEDB) == -1) return -1;
        if (rdbSaveLen(rdb, sv->sizes[dbid].keys) == -1) return -1;
        if (rdbSaveLen(rdb, sv->sizes[dbid].expires) == -1) return -1;
        *cur_db = dbid;
        *cur_slot = -1;
    }
    if (server.cluster_enabled && slot != *cur_slot) {
        if (rdbSaveType(rdb, RDB_OPCODE_SLOT_INFO) == -1) return -1;
        if (rdbSaveLen(rdb, slot) == -1) return -1;
        if (rdbSaveLen(rdb, sv->sizes[dbid].slot_keys[slot]) == -1) return -1;
        if (rdbSaveLen(rdb, sv->sizes[dbid].slot_expires[slot]) == -1) return -1;
        *cur_slot = slot;
    }
    return 0;
}

/* Child: write the whole RDB stream. 'out' is the socket rio for a diskless
 * sync (framed with the diskless EOF mark, nothing on disk); NULL means a
 * temp file renamed to sv->filename at the end. */
static int snapSaveChildWrite(snapSave *sv, rio *out) {
    char tmpfile[256] = "";
    char eofmark[RDB_EOF_MARK_SIZE];
    char *err_op = "";
    int error = 0;
    char *payload = NULL;
    size_t payload_cap = 0;
    long key_counter = 0, msgs = 0;
    long long info_updated_time = 0;
    int cur_db = -1, cur_slot = -1;
    int aborted = 0;
    FILE *fp = NULL;
    rio filerio;
    rio *rdb = out;

    startSaving(sv->rdbflags);
    if (!out) {
        snprintf(tmpfile, sizeof(tmpfile), "temp-%d.rdb", (int)getpid());
        fp = fopen(tmpfile, "w");
        if (!fp) {
            serverLog(LL_WARNING, "Hybrid BGSAVE: failed opening the temp RDB file %s: %s", tmpfile, strerror(errno));
            stopSaving(0);
            return C_ERR;
        }
        rioInitWithFile(&filerio, fp);
        rdb = &filerio;
        if (server.rdb_save_incremental_fsync) {
            rioSetAutoSync(rdb, REDIS_AUTOSYNC_BYTES);
            if (!(sv->rdbflags & RDBFLAGS_KEEP_CACHE)) rioSetReclaimCache(rdb, 1);
        }
    } else {
        /* Diskless framing, as rdbSaveRioWithEOFMark(): the replica reads up
         * to the mark that follows the footer. */
        getRandomHexChars(eofmark, RDB_EOF_MARK_SIZE);
        if (rioWrite(rdb, "$EOF:", 5) == 0 || rioWrite(rdb, eofmark, RDB_EOF_MARK_SIZE) == 0 ||
            rioWrite(rdb, "\r\n", 2) == 0) { err_op = "eof mark"; goto werr; }
    }
    if (rdbSaveRioHeader(sv->req, rdb, &error, sv->rdbflags, sv->has_rsi ? &sv->rsi : NULL) == C_ERR) {
        err_op = "rdbSaveRioHeader";
        goto werr;
    }

    /* Two sources: the spool (BUFFER messages, read at our own pace from
     * 'spool_off') and the pipe (KVOBJ, END, ABORT; everything when there is
     * no spool). The spool is drained whenever it has a complete message;
     * the pipe is polled in between, with a short wait only when the spool
     * is caught up. After END the spool is complete: drain it to its end. */
    off_t spool_off = 0;
    int ended = 0;
    for (;;) {
        snapMsgHdr h;
        int from_spool = 0;
        int pipe_ready = (sv->spool_rd == -1);   /* no spool: the pipe is the only source; after END: spool only */
        if (sv->spool_rd != -1 && !ended) {
            /* Round-robin: a pipe message when one is waiting (so the queue
             * drains while we copy the spool and big values start promptly),
             * else a spool message. */
            struct pollfd pfd = { .fd = sv->pipe_rd, .events = POLLIN, .revents = 0 };
            int pr = poll(&pfd, 1, 0);
            pipe_ready = pr > 0 && (pfd.revents & POLLIN);
            if (pr > 0 && !(pfd.revents & POLLIN)) { err_op = "channel closed"; goto werr; }
        }
        if (sv->spool_rd != -1 && !pipe_ready) {
            int r = snapSpoolPread(sv->spool_rd, &h, sizeof(h), spool_off);
            if (r < 0) { err_op = "spool read (header)"; goto werr; }
            if (r == 1) {
                if (h.len > payload_cap) {
                    payload = zrealloc(payload, h.len);
                    payload_cap = h.len;
                }
                r = h.len ? snapSpoolPread(sv->spool_rd, payload, h.len, spool_off + sizeof(h)) : 1;
                if (r < 0) { err_op = "spool read (payload)"; goto werr; }
            }
            if (r == 1) {
                spool_off += sizeof(h) + h.len;
                from_spool = 1;
            } else if (ended) {
                break;   /* the scan thread wrote END after its last spool message: done */
            }
        }
        if (!from_spool) {
            if (sv->spool_rd != -1) {
                /* Spool caught up: wait for the pipe, briefly, then look again. */
                struct pollfd pfd = { .fd = sv->pipe_rd, .events = POLLIN, .revents = 0 };
                int pr = poll(&pfd, 1, 5);
                if (pr < 0 && errno != EINTR) { err_op = "channel poll"; goto werr; }
                if (pr <= 0) continue;
                if (!(pfd.revents & POLLIN)) { err_op = "channel closed"; goto werr; }
            }
            if (snapReadAll(sv->pipe_rd, &h, sizeof(h)) != 0) { err_op = "channel read (header)"; goto werr; }
            if (h.type == SNAP_MSG_END) {
                if (sv->spool_rd == -1) break;
                ended = 1;   /* finish the spool first */
                continue;
            }
            if (h.type == SNAP_MSG_ABORT) { aborted = 1; goto werr; }
            if (h.len > payload_cap) {
                payload = zrealloc(payload, h.len);
                payload_cap = h.len;
            }
            if (h.len && snapReadAll(sv->pipe_rd, payload, h.len) != 0) { err_op = "channel read (payload)"; goto werr; }
        }
        if (snapChildSelect(rdb, sv, h.dbid, h.slot, &cur_db, &cur_slot) != 0) { err_op = "selectdb"; goto werr; }

        if (h.type == SNAP_MSG_BUFFER) {
            if (rioWrite(rdb, payload, h.len) == 0) { err_op = "write buffer"; goto werr; }
        } else if (h.type == SNAP_MSG_KVOBJ) {
            kvobj *kv = (kvobj *)(payload + h.robj_off);
            /* An embedded string value lives inside the kvobj allocation: its
             * pointer is the parent's address, relocate it into our copy. */
            if (kv->encoding == OBJ_ENCODING_EMBSTR)
                kv->ptr = payload + ((char *)kv->ptr - (char *)(uintptr_t)h.src_addr);
            robj key;
            initStaticStringObject(key, kvobjGetKey(kv));
            size_t before = rdb->processed_bytes;
            if (rdbSaveKeyValuePair(rdb, &key, kv, kvobjGetExpire(kv), h.dbid) == -1) { err_op = "rdbSaveKeyValuePair"; goto werr; }
            key_counter++;
            /* As the stock child does after each key: drop the value's pages from
             * this process once written, so later parent writes to them do not
             * leave us a private copy. The value is the parent's forkable
             * allocation, mapped in our CoW view. */
            size_t dump_size = rdb->processed_bytes - before;
            if (dump_size > server.page_size / 2) dismissObject(kv, dump_size);
        } else {
            err_op = "unknown message";
            goto werr;
        }
        if ((msgs++ & 1023) == 0) {
            long long now = mstime();
            if (now - info_updated_time >= 1000) {
                sendChildInfo(CHILD_INFO_TYPE_CURRENT_INFO, key_counter, "RDB");
                info_updated_time = now;
            }
        }
    }
    zfree(payload);
    payload = NULL;

    if (rdbSaveRioFooter(sv->req, rdb, &error) == C_ERR) { err_op = "rdbSaveRioFooter"; goto werr; }
    if (out) {
        if (rioWrite(rdb, eofmark, RDB_EOF_MARK_SIZE) == 0) { err_op = "eof mark"; goto werr; }
        if (rioFlush(rdb) == 0) { err_op = "flush"; goto werr; }
        serverLog(LL_NOTICE, "Hybrid RDB transfer done, %ld SNAP_FORK keys serialized by the child, %zu bytes sent.",
                  key_counter, rdb->processed_bytes);
        stopSaving(1);
        return C_OK;
    }
    if (fflush(fp)) { err_op = "fflush"; goto werr; }
    if (fsync(fileno(fp))) { err_op = "fsync"; goto werr; }
    if (!(sv->rdbflags & RDBFLAGS_KEEP_CACHE) && reclaimFilePageCache(fileno(fp), 0, 0) == -1)
        serverLog(LL_NOTICE, "Unable to reclaim cache after saving RDB: %s", strerror(errno));
    if (fclose(fp)) { fp = NULL; err_op = "fclose"; goto werr; }
    fp = NULL;
    if (rename(tmpfile, sv->filename) == -1) {
        serverLog(LL_WARNING, "Hybrid BGSAVE: error moving temp DB file %s to %s: %s", tmpfile, sv->filename, strerror(errno));
        unlink(tmpfile);
        stopSaving(0);
        return C_ERR;
    }
    if (fsyncFileDir(sv->filename) != 0) {
        serverLog(LL_WARNING, "Hybrid BGSAVE: failed to fsync directory: %s", strerror(errno));
        stopSaving(0);
        return C_ERR;
    }
    serverLog(LL_NOTICE, "Hybrid BGSAVE done, %ld SNAP_FORK keys serialized by the child, %zu bytes written.",
              key_counter, rdb->processed_bytes);
    stopSaving(1);
    return C_OK;

werr:
    if (aborted)
        serverLog(LL_NOTICE, "Hybrid BGSAVE aborted by the parent.");
    else if (errno == 0)
        serverLog(LL_WARNING, "Hybrid BGSAVE: the parent closed the channel (%s)", err_op);
    else
        serverLog(LL_WARNING, "Hybrid BGSAVE: write error (%s): %s", err_op, strerror(errno));
    zfree(payload);
    if (fp) fclose(fp);
    if (tmpfile[0]) unlink(tmpfile);
    stopSaving(0);
    return aborted ? SERVER_CHILD_NOERROR_RETVAL : C_ERR;
}

static void snapSaveChild(snapSave *sv) {
    rio sock;
    snap_in_hybrid_child = 1;
    close(sv->pipe_wr);
    if (sv->spool_wr != -1) { close(sv->spool_wr); sv->spool_wr = -1; }
    if (sv->to_sockets) {
        if (sv->rdb_channel) {
            rioInitWithConnset(&sock, sv->conns, sv->numconns);
        } else {
            rioInitWithFd(&sock, sv->rdb_pipe_write);
            /* Close the reading part, so that if the parent crashes, the child
             * will get a write error and exit. */
            close(server.rdb_pipe_read);
        }
        redisSetProcTitle("redis-rdb-to-slaves");
        /* Disable RDB compression and checksum in the fork child if requested.
         * The parent's configuration is not affected. (The scan thread's copy
         * of the compression flag was set from the same request at start.) */
        if (sv->req & SLAVE_REQ_RDB_NO_COMPRESS) server.rdb_compression = 0;
        if (sv->req & SLAVE_REQ_RDB_NO_CHECKSUM) server.rdb_checksum = 0;
    } else {
        redisSetProcTitle("redis-rdb-bgsave");
    }
    redisSetCpuAffinity(server.bgsave_cpulist);
    int retval = snapSaveChildWrite(sv, sv->to_sockets ? &sock : NULL);
    if (retval == C_OK) sendChildCowInfo(CHILD_INFO_TYPE_RDB_COW_SIZE, "RDB");
    if (sv->to_sockets) {
        if (sv->rdb_channel) {
            rioFreeConnset(&sock);
        } else {
            rioFreeFd(&sock);
            /* wake up the reader, tell it we're done. */
            close(sv->rdb_pipe_write);
            close(server.rdb_child_exit_pipe); /* close write end so that we can detect the close on the parent. */
            /* hold exit until the parent tells us it's safe. we're not expecting
             * to read anything, just get the error when the pipe is closed. */
            char dummy;
            ssize_t r = read(sv->safe_to_exit_pipe, &dummy, 1);
            UNUSED(r);
        }
    }
    if (retval == C_OK) exitFromChild(0, 0);
    /* A parent-requested abort mirrors a SIGUSR1 kill (no error counted). */
    exitFromChild(retval == SERVER_CHILD_NOERROR_RETVAL ? SERVER_CHILD_NOERROR_RETVAL : 1, 0);
}

/* ---- parent ---- */

static void snapSaveFree(snapSave *sv) {
    if (sv->ae_registered)
        aeDeleteFileEvent(server.el, snapScanReadEventFd(sv->scan), AE_READABLE);
    if (sv->scan) snapScanRelease(sv->scan);
    if (sv->oq) {
        snapOutClear(sv);
        listRelease(sv->oq);
    }
    if (sv->oq_pool) {
        listNode *ln;
        while ((ln = listFirst(sv->oq_pool)) != NULL) {
            sdsfree(listNodeValue(ln));
            listDelNode(sv->oq_pool, ln);
        }
        listRelease(sv->oq_pool);
    }
    if (sv->pipe_wr != -1) close(sv->pipe_wr);
    if (sv->pipe_rd != -1) close(sv->pipe_rd);
    if (sv->spool_wr != -1) close(sv->spool_wr);
    if (sv->spool_rd != -1) close(sv->spool_rd);
    if (sv->sizes) {
        for (int j = 0; j < sv->dbnum; j++) {
            zfree(sv->sizes[j].slot_keys);
            zfree(sv->sizes[j].slot_expires);
        }
        zfree(sv->sizes);
    }
    sdsfree(sv->stage);
    if (sv->mq) {
        listNode *ln;
        while ((ln = listFirst(sv->mq)) != NULL) {
            snapQueuedMsg *m = listNodeValue(ln);
            sdsfree(m->payload);
            zfree(m);
            listDelNode(sv->mq, ln);
        }
        listRelease(sv->mq);
        pthread_mutex_destroy(&sv->mq_lock);
    }
    sdsfree(sv->mstage);
    sdsfree(sv->mscratch);
    zfree(sv->stores);
    zfree(sv->privs);
    zfree(sv->store_priv);
    for (int j = 0; j < sv->dbnum; j++) snapSaveFreeFlushed(sv, j);   /* normally freed at store completion */
    zfree(sv->flushed);
    zfree(sv);
}

/* Overlay hooks for the server keyspace (privdata = redisDb *). */
static void snapOverlayEnter(void *privdata) { UNUSED(privdata); snapFunnelOpen(); }
static const kvstoreOverlayHooks snap_server_hooks = {
    snapOverlayCloneKey, snapOverlayFreeOriginal, snapOverlayEnter, snapOverlayPrepare, snapOverlayDrainable,
};

/* Main thread: the save object, its channel, queues and scan context for a
 * frozen-to-be keyspace. NULL on failure (logged). */
static snapSave *snapSaveCreate(int req, char *filename, rdbSaveInfo *rsi, int rdbflags) {
    serverAssert(snap_save == NULL);

    snapSave *sv = zcalloc(sizeof(*sv));
    sv->pipe_rd = sv->pipe_wr = -1;
    sv->spool_rd = sv->spool_wr = -1;
    sv->dbnum = server.dbnum;
    sv->req = req;
    sv->rdbflags = rdbflags;
    sv->filename = filename;
    if (rsi) { sv->rsi = *rsi; sv->has_rsi = 1; }
    sv->stores = zmalloc(sizeof(kvstore *) * sv->dbnum);
    sv->privs = zmalloc(sizeof(void *) * sv->dbnum);
    sv->store_priv = zmalloc(sizeof(snapStorePriv) * sv->dbnum);
    sv->flushed = zcalloc(sizeof(snapFlushedDb) * sv->dbnum);
    sv->sizes = zcalloc(sizeof(snapDbSizes) * sv->dbnum);
    for (int j = 0; j < sv->dbnum; j++) {
        redisDb *db = &server.db[j];
        sv->stores[j] = db->keys;
        sv->store_priv[j].s = j;
        sv->privs[j] = &sv->store_priv[j];
        sv->sizes[j].keys = kvstoreSize(db->keys);
        sv->sizes[j].expires = kvstoreSize(db->expires);
        if (server.cluster_enabled) {
            int n = kvstoreNumDicts(db->keys);
            sv->sizes[j].nslots = n;
            sv->sizes[j].slot_keys = zmalloc(sizeof(unsigned long long) * n);
            sv->sizes[j].slot_expires = zmalloc(sizeof(unsigned long long) * n);
            for (int didx = 0; didx < n; didx++) {
                sv->sizes[j].slot_keys[didx] = kvstoreDictSize(db->keys, didx);
                sv->sizes[j].slot_expires[didx] = kvstoreDictSize(db->expires, didx);
            }
        }
    }
    int fds[2];
    if (pipe(fds) != 0) {
        serverLog(LL_WARNING, "Hybrid BGSAVE: pipe() failed: %s", strerror(errno));
        snapSaveFree(sv);
        return NULL;
    }
    sv->pipe_rd = fds[0];
    sv->pipe_wr = fds[1];
#ifdef F_SETPIPE_SZ
    fcntl(sv->pipe_wr, F_SETPIPE_SZ, 1 << 20);   /* best effort: deeper back-pressure buffer */
#endif
    /* The scan thread writes the pipe non-blockingly and keeps what does not
     * fit in its own queue (bounded by bgsave-hybrid-max-queued-bytes). The
     * child's read end stays blocking. */
    if (fcntl(sv->pipe_wr, F_SETFL, fcntl(sv->pipe_wr, F_GETFL) | O_NONBLOCK) == -1) {
        serverLog(LL_WARNING, "Hybrid BGSAVE: fcntl(O_NONBLOCK) failed: %s", strerror(errno));
        snapSaveFree(sv);
        return NULL;
    }
    sv->oq = listCreate();
    sv->oq_pool = listCreate();
    sv->oq_limit = server.bgsave_hybrid_max_queued_bytes;
    if (server.bgsave_hybrid_spool) {
        /* Unlinked at once: both descriptors keep it alive, a crash leaves
         * nothing behind. In the working directory, next to the temp RDB. */
        char path[64];
        snprintf(path, sizeof(path), "temp-snap-%d.spool", (int)getpid());
        sv->spool_wr = open(path, O_WRONLY | O_CREAT | O_EXCL | O_APPEND, 0600);
        if (sv->spool_wr != -1) {
            sv->spool_rd = open(path, O_RDONLY);
            unlink(path);
            if (sv->spool_rd == -1) { close(sv->spool_wr); sv->spool_wr = -1; }
        }
        if (sv->spool_wr == -1)
            serverLog(LL_WARNING, "Hybrid BGSAVE: cannot create the spool file %s (%s): the pipe carries everything", path, strerror(errno));
    }
    snapStageReset(sv);
    sv->stage_dbid = sv->stage_slot = -1;
    pthread_mutex_init(&sv->mq_lock, NULL);
    sv->mq = listCreate();
    sv->mstage = sdsempty();
    sv->mscratch = sdsempty();
    sv->mstage_dbid = sv->mstage_slot = -1;
    atomicSet(sv->key_save_delay, server.rdb_key_save_delay);
    sv->maxmemory_policy = server.maxmemory_policy;
    sv->rdb_compression = (req & SLAVE_REQ_RDB_NO_COMPRESS) ? 0 : server.rdb_compression;
    server.stat_snap_saves++;
    sv->scan = snapScanCreate(sv->stores, sv->dbnum, snapSaveDispatch, NULL, sv);
    snapScanSetOverlay(sv->scan, &snap_server_hooks, sv->privs);
    snapScanSetStoreDone(sv->scan, snapSaveStoreDone);
    snapScanSetThreadDone(sv->scan, snapSaveThreadDone);
    snapScanSetThreadInit(sv->scan, snapSaveThreadInit);
    snapScanSetWindowDone(sv->scan, snapSaveWindowDone);
    return sv;
}

/* Main thread: FORK INSTANT. Freeze, fork the writer child, start the scan
 * thread. On a fork failure the keyspace is unfrozen and sv is freed; the
 * caller undoes its own state (replica states, pipes). */
static int snapSaveLaunch(snapSave *sv) {
    server.stat_rdb_saves++;
    server.dirty_before_bgsave = server.dirty;
    server.lastbgsave_try = time(NULL);

    /* FORK INSTANT: freeze, then fork. */
    mstime_t freeze_lat;
    latencyStartMonitor(freeze_lat);
    snapScanFreeze(sv->scan);
    latencyEndMonitor(freeze_lat);
    latencyAddSampleIfNeeded("hybrid-freeze", freeze_lat);
    snap_forking_hybrid = 1;
    pid_t childpid = redisFork(CHILD_TYPE_RDB);
    snap_forking_hybrid = 0;
    if (childpid == 0) {
        snapSaveChild(sv);   /* never returns */
    }
    if (childpid == -1) {
        if (!sv->to_sockets) server.lastbgsave_status = C_ERR;
        serverLog(LL_WARNING, "Can't save in background (hybrid): fork: %s", strerror(errno));
        snapScanAbort(sv->scan);   /* unfreeze; nothing was scanned */
        snapSaveFree(sv);
        return C_ERR;
    }
    close(sv->pipe_rd);
    sv->pipe_rd = -1;
    if (sv->spool_rd != -1) { close(sv->spool_rd); sv->spool_rd = -1; }
    snap_save = sv;
    if (snapScanStartThread(sv->scan) != C_OK) {
        /* The child is waiting on the channel: kill it, unfreeze. */
        serverLog(LL_WARNING, "Hybrid BGSAVE: cannot start the scan thread; aborting the save");
        killRDBChild();   /* -> snapSaveAbort() unfreezes; the done handler releases */
    } else {
        aeCreateFileEvent(server.el, snapScanReadEventFd(sv->scan), AE_READABLE, snapScanAeHandler, sv->scan);
        sv->ae_registered = 1;
    }
    serverLog(LL_NOTICE, "Hybrid background saving started by pid %ld (scan thread streams to the child)", (long)childpid);
    server.rdb_save_time_start = time(NULL);
    return C_OK;
}

int snapSaveBackground(int req, char *filename, rdbSaveInfo *rsi, int rdbflags) {
    if (hasActiveChildProcess()) return C_ERR;
    if (snapArenaForkable()) {
        /* Only a Lua debugger session can leave the arena forkable with no
         * exclusive child around. The hybrid child never touches the arena, so
         * mapping it is harmless; it just pays copy-on-write on it meanwhile. */
        serverLog(LL_NOTICE, "Hybrid BGSAVE: the SNAP_NOFORK arena is forkable (debugger child); the save child will map it");
    }
    snapSave *sv = snapSaveCreate(req, filename, rsi, rdbflags);
    if (!sv) return C_ERR;
    if (snapSaveLaunch(sv) != C_OK) return C_ERR;
    server.rdb_child_type = RDB_CHILD_TYPE_DISK;
    return C_OK;
}

/* Diskless full sync through the hybrid save (TASK-1.14): the same scan and
 * channel as a BGSAVE, but the child writes the replicas instead of a file.
 * Mirrors rdbSaveToSlavesSockets(): replicas in WAIT_BGSAVE_START with this
 * exact 'req' are set up for full resync at the current offset just before
 * the freeze-and-fork, so the offset is the snapshot's; with the rdb channel
 * the child writes the (blocking) replica connections directly, otherwise it
 * writes the stock rdb pipe that the parent fans out to the sockets, and
 * waits on the safe-to-exit pipe before exiting. The stock socket done
 * handler (RDB_CHILD_TYPE_SOCKET) does the replica bookkeeping afterwards. */
int snapSaveToSlavesSockets(int req, rdbSaveInfo *rsi) {
    listIter li;
    listNode *ln;
    int pipefds[2];
    int rdb_channel = server.repl_rdb_channel && (req & SLAVE_REQ_RDB_CHANNEL);

    if (hasActiveChildProcess()) return C_ERR;
    /* Even if the previous fork child exited, don't start a new one until we
     * drained the pipe. */
    if (server.rdb_pipe_conns) return C_ERR;
    if (snapArenaForkable())
        serverLog(LL_NOTICE, "Hybrid BGSAVE: the SNAP_NOFORK arena is forkable (debugger child); the save child will map it");

    snapSave *sv = snapSaveCreate(req, NULL, rsi, RDBFLAGS_REPLICATION);
    if (!sv) return C_ERR;
    sv->to_sockets = 1;
    sv->rdb_channel = rdb_channel;
    sv->rdb_pipe_write = sv->safe_to_exit_pipe = -1;

    if (!rdb_channel) {
        /* The child cannot write the sockets itself: with TLS the parent must
         * keep a continuous TLS state when it takes over after the child. */
        if (anetPipe(pipefds, O_NONBLOCK, 0) == -1) {
            snapScanAbort(sv->scan);
            snapSaveFree(sv);
            return C_ERR;
        }
        server.rdb_pipe_read = pipefds[0];
        sv->rdb_pipe_write = pipefds[1];
        /* The parent tells the child through this pipe that it may exit. */
        if (anetPipe(pipefds, 0, 0) == -1) {
            close(sv->rdb_pipe_write);
            close(server.rdb_pipe_read);
            server.rdb_pipe_read = -1;
            snapScanAbort(sv->scan);
            snapSaveFree(sv);
            return C_ERR;
        }
        sv->safe_to_exit_pipe = pipefds[0];
        server.rdb_child_exit_pipe = pipefds[1];
    }

    /* Collect the connections of the replicas we want to transfer the RDB to. */
    int numconns = 0;
    connection **conns = zmalloc(sizeof(*conns) * listLength(server.slaves));
    listRewind(server.slaves, &li);
    while ((ln = listNext(&li))) {
        client *slave = ln->value;
        if (slave->replstate != SLAVE_STATE_WAIT_BGSAVE_START) continue;
        if (slave->slave_req != req) continue;
        replicationSetupSlaveForFullResync(slave, getPsyncInitialOffset());
        conns[numconns++] = slave->conn;
        if (rdb_channel) {
            /* Put the socket in blocking mode to simplify RDB transfer. */
            connSendTimeout(slave->conn, server.repl_timeout * 1000);
            connBlock(slave->conn);
        }
    }
    sv->conns = conns;
    sv->numconns = numconns;
    if (!rdb_channel) {
        server.rdb_pipe_conns = conns;
        server.rdb_pipe_numconns = numconns;
        server.rdb_pipe_numconns_writing = 0;
    }

    if (snapSaveLaunch(sv) != C_OK) {
        /* Undo the state change. The caller will perform cleanup on all the
         * replicas in BGSAVE_START state, but the setup above turned them
         * into BGSAVE_END. */
        listRewind(server.slaves, &li);
        while ((ln = listNext(&li))) {
            client *slave = ln->value;
            if (slave->replstate == SLAVE_STATE_WAIT_BGSAVE_END)
                slave->replstate = SLAVE_STATE_WAIT_BGSAVE_START;
        }
        if (!rdb_channel) {
            close(server.rdb_pipe_read);
            close(server.rdb_child_exit_pipe);
            server.rdb_pipe_read = server.rdb_child_exit_pipe = -1;
            server.rdb_pipe_conns = NULL;
            server.rdb_pipe_numconns = 0;
            server.rdb_pipe_numconns_writing = 0;
        }
        zfree(conns);
        return C_ERR;
    }
    /* sv is live from here (snap_save); it does not own 'conns'. */
    sv->conns = NULL;
    serverLog(LL_NOTICE, "Background RDB transfer started by pid %ld to %s (hybrid: the scan thread streams to the child)",
              (long)server.child_pid, rdb_channel ? "replica socket" : "parent process pipe");
    server.rdb_child_type = RDB_CHILD_TYPE_SOCKET;
    if (!rdb_channel) {
        close(sv->rdb_pipe_write); /* close write in parent so that it can detect the close on the child. */
        sv->rdb_pipe_write = -1;
        close(sv->safe_to_exit_pipe);
        sv->safe_to_exit_pipe = -1;
        if (aeCreateFileEvent(server.el, server.rdb_pipe_read, AE_READABLE, rdbPipeReadHandler, NULL) == AE_ERR)
            serverPanic("Unrecoverable error creating server.rdb_pipe_read file event.");
    } else {
        zfree(conns);
    }
    return C_OK;
}

void snapSaveAbort(void) {
    if (!snap_save || snap_save->aborted) return;
    snapSave *sv = snap_save;
    sv->aborted = 1;
    /* Stops the thread within one cursor step, joins it, merges every slot.
     * A thread blocked on write() returns once the child reads or dies. */
    snapScanAbort(sv->scan);
    /* Every slot is complete and every overlay released: the stores are not
     * ours to look at any more (the aborting FLUSHALL ASYNC or full-sync load
     * frees them on BIO while this save object waits for the child). */
    for (int s = 0; s < sv->dbnum; s++) sv->stores[s] = NULL;
    if (sv->ae_registered) {
        aeDeleteFileEvent(server.el, snapScanReadEventFd(sv->scan), AE_READABLE);
        sv->ae_registered = 0;
    }
    serverLog(LL_NOTICE, "Hybrid BGSAVE: scan aborted, keyspace overlay merged.");
}

void snapSaveChildDone(int exitcode, int bysignal) {
    if (!snap_save) return;
    snapSave *sv = snap_save;
    /* The child is gone. If the scan did not complete (or the thread stopped
     * on its own when the channel broke, leaving slots owed), stop it and
     * merge every remaining slot. Harmless after a clean finish. */
    if (!(!bysignal && exitcode == 0) || !snapScanIsFinished(sv->scan))
        snapScanAbort(sv->scan);
    if (sv->failed && !sv->to_sockets) {
        /* Our own defined failure (hard mark): the child was ended with the
         * whitelisted SIGUSR1, which the plain done handler does not count.
         * A socket transfer reports through the replicas, as stock. */
        server.lastbgsave_status = C_ERR;
        server.stat_rdb_consecutive_failures++;
    }
    snap_last_retained_peak = sv->retained_peak;
    atomicGet(sv->queued_peak, snap_last_queued_peak);
    atomicGet(sv->spool_bytes, snap_last_spool_bytes);
    snap_last_inplace_writes = sv->inplace_writes;
    snap_save = NULL;
    snapSaveFree(sv);   /* drains completions (merges), joins, unregisters */
    serverLog(LL_NOTICE, "Hybrid BGSAVE: scan thread joined and overlay released.");
}

/* ---- Unit tests ---------------------------------------------------------- */
#ifdef REDIS_TEST
#include "testhelp.h"
#include <sys/wait.h>
#include <time.h>

/* Fork a child that writes one byte to 'p'. The child reports over a pipe
 * that it reached the write ('r') and that it survived it ('d'), so a crash
 * anywhere else (e.g. in jemalloc's post-fork handler) is not mistaken for a
 * fault on 'p'. Returns 1 if the child faulted exactly on the write (SIGSEGV
 * or SIGBUS after 'r' and no 'd'), 0 if it wrote fine and exited 0, and -1 on
 * any other outcome (printed). */
static int snapTestChildWriteFaults(void *p) {
    int fds[2];
    if (pipe(fds) != 0) return -1;
    pid_t pid = fork();
    if (pid == 0) {
        close(fds[0]);
        volatile char *c = p;
        if (write(fds[1], "r", 1) != 1) _exit(2);
        c[0] = 1;
        if (write(fds[1], "d", 1) != 1) _exit(2);
        _exit(0);
    }
    close(fds[1]);
    if (pid < 0) { close(fds[0]); return -1; }
    char buf[4] = {0};
    ssize_t n = 0, r;
    while (n < 2 && (r = read(fds[0], buf + n, 2 - n)) > 0) n += r;
    close(fds[0]);
    int status = 0;
    waitpid(pid, &status, 0);
    int reached = (n >= 1 && buf[0] == 'r');
    int survived = (n >= 2 && buf[1] == 'd');
    if (reached && !survived && WIFSIGNALED(status) &&
        (WTERMSIG(status) == SIGSEGV || WTERMSIG(status) == SIGBUS))
        return 1;
    if (reached && survived && WIFEXITED(status) && WEXITSTATUS(status) == 0)
        return 0;
    printf("  (child outcome: reached=%d survived=%d signaled=%d sig=%d exited=%d code=%d)\n",
           reached, survived, WIFSIGNALED(status), WIFSIGNALED(status) ? WTERMSIG(status) : 0,
           WIFEXITED(status), WIFEXITED(status) ? WEXITSTATUS(status) : -1);
    return -1;
}

/* Fork a child that exercises the allocator (zmalloc + snapNofork are both
 * forbidden/unmapped for the arena, so only the default arena is used) to
 * prove jemalloc's post-fork state is intact with the SNAP_NOFORK arena
 * present. Returns 1 on success. */
static int snapTestChildAllocatorAlive(void) {
    pid_t pid = fork();
    if (pid == 0) {
        for (int i = 0; i < 1000; i++) { void *x = zmalloc(64 + i); zfree(x); }
        void *big = zmalloc(3 * 1024 * 1024); memset(big, 1, 3 * 1024 * 1024); zfree(big);
        _exit(0);
    }
    if (pid < 0) return 0;
    int status = 0;
    waitpid(pid, &status, 0);
    if (!(WIFEXITED(status) && WEXITSTATUS(status) == 0))
        printf("  (allocator child: signaled=%d sig=%d exited=%d code=%d)\n", WIFSIGNALED(status),
               WIFSIGNALED(status) ? WTERMSIG(status) : 0, WIFEXITED(status),
               WIFEXITED(status) ? WEXITSTATUS(status) : -1);
    return WIFEXITED(status) && WEXITSTATUS(status) == 0;
}

/* Look up the mapping containing 'p' in /proc/self/smaps and return 1 if its
 * VmFlags include "dc" (VM_DONTCOPY, i.e. MADV_DONTFORK), 0 if not, -1 if not
 * found. */
static int snapTestMappingIsDontcopy(const void *p) {
    FILE *f = fopen("/proc/self/smaps", "r");
    if (!f) return -1;
    char line[1024];
    int in_range = 0, result = -1;
    unsigned long target = (unsigned long)p;
    while (fgets(line, sizeof(line), f)) {
        unsigned long lo, hi;
        if (sscanf(line, "%lx-%lx ", &lo, &hi) == 2 && strchr(line, '-') && strchr(line, ' ') &&
            (strstr(line, " r") || strstr(line, " -")))
        {
            in_range = (target >= lo && target < hi);
            continue;
        }
        if (in_range && !strncmp(line, "VmFlags:", 8)) {
            result = strstr(line, " dc") != NULL;
            break;
        }
    }
    fclose(f);
    return result;
}

static long long snapTestNowNs(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (long long)ts.tv_sec * 1000000000LL + ts.tv_nsec;
}

static int snapTestCountMaps(void) {
    FILE *f = fopen("/proc/self/maps", "r");
    if (!f) return -1;
    int n = 0;
    char line[1024];
    while (fgets(line, sizeof(line), f)) n++;
    fclose(f);
    return n;
}

static void snapTestPurgeArena(void) {
#ifdef SNAP_NOFORK_ARENA_SUPPORTED
    char buf[64];
    snprintf(buf, sizeof(buf), "arena.%u.purge", server.snap_nofork_arena);
    je_mallctl(buf, NULL, 0, NULL, 0);
    snprintf(buf, sizeof(buf), "arena.%u.decay", server.snap_nofork_arena);
    je_mallctl(buf, NULL, 0, NULL, 0);
#endif
}


/* ---- Scan tests ---- */
extern dictType KvstoreDictTestType;   /* kvstore.c (REDIS_TEST) */
extern kvstoreType KvstoreTestType;

#define SNAP_TEST_STORES 2
#define SNAP_TEST_DICT_BITS 2
#define SNAP_TEST_DICTS (1 << SNAP_TEST_DICT_BITS)
#define SNAP_TEST_MAXKEYS 6000

struct snapScanTestState {
    unsigned char seen[SNAP_TEST_STORES][SNAP_TEST_DICTS][SNAP_TEST_MAXKEYS];
    int inserted[SNAP_TEST_STORES][SNAP_TEST_DICTS];
    redisAtomic long dispatched;
    redisAtomic long bad;             /* unparsable / out-of-range / duplicate */
    int done_calls[SNAP_TEST_STORES][SNAP_TEST_DICTS];
    int done_aborted[SNAP_TEST_STORES][SNAP_TEST_DICTS];
    int dispatch_sleep_us;
    redisAtomic int wrong_state;      /* dispatch saw its slot not SCANNING */
    snapScanCtx *ctx;
    kvstore **stores;
    int rehashing_at_done;
};

/* Test keys are aligned C strings owned by the dict (no_value dicts need
 * aligned stored keys; the real keyspace stores kvobj pointers). */
static char *snapTestKey(int s, int didx, int i) {
    char buf[64];
    int len = snprintf(buf, sizeof(buf), "k:%d:%d:%d", s, didx, i);
    char *k = zmalloc(len + 1);
    memcpy(k, buf, len + 1);
    return k;
}

static void snapTestDispatch(void *priv, int s, int didx, kvobj *kv) {
    struct snapScanTestState *st = priv;
    const char *key = (const char*)kv;   /* test dict stores plain C strings */
    int ps, pd, pi;
    if (sscanf(key, "k:%d:%d:%d", &ps, &pd, &pi) != 3 || ps != s || pd != didx ||
        pi < 0 || pi >= st->inserted[s][didx] || st->seen[s][didx][pi])
    {
        atomicIncr(st->bad, 1);
    } else {
        st->seen[s][didx][pi] = 1;
    }
    if (st->ctx && snapScanSlotState(st->ctx, s, didx) != SNAP_SLOT_SCANNING) atomicSetWithSync(st->wrong_state, 1);
    atomicIncr(st->dispatched, 1);
    if (st->dispatch_sleep_us) usleep(st->dispatch_sleep_us);
}

static int snapTestKeyCompare(dictCmpCache *cache, const void *k1, const void *k2) {
    UNUSED(cache);
    return strcmp(k1, k2) == 0;
}

static void snapTestSlotDone(void *priv, int s, int didx, int aborted) {
    struct snapScanTestState *st = priv;
    st->done_calls[s][didx]++;
    st->done_aborted[s][didx] = aborted;
    /* The dict is still frozen here: record whether the mid-rehash dict is
     * still mid-rehash, i.e. the scan itself never rehashed it. */
    if (s == 0 && didx == 3 && st->stores)
        st->rehashing_at_done = dictIsRehashing(kvstoreGetDict(st->stores[0], 3));
}

static int snapTestAllUnfrozen(kvstore **stores) {
    for (int s = 0; s < SNAP_TEST_STORES; s++)
        for (int didx = 0; didx < SNAP_TEST_DICTS; didx++) {
            dict *d = kvstoreGetDict(stores[s], didx);
            if (d && (d->pauserehash != 0 || d->pauseAutoResize != 0)) return 0;
        }
    return 1;
}

static int snapTestAllState(snapScanCtx *ctx, int state) {
    for (int s = 0; s < SNAP_TEST_STORES; s++)
        for (int didx = 0; didx < SNAP_TEST_DICTS; didx++)
            if (snapScanSlotState(ctx, s, didx) != state) return 0;
    return 1;
}

static void ovTestScanCb(void *priv, const dictEntry *de, dictEntry **plink);

static void snapScanTests(int accurate) {
    dictType noval = KvstoreDictTestType;
    noval.no_value = 1;
    noval.keyCompare = snapTestKeyCompare;   /* the kvstore test type compares pointers */
    kvstore *stores[SNAP_TEST_STORES];
    for (int s = 0; s < SNAP_TEST_STORES; s++)
        stores[s] = kvstoreCreate(&KvstoreTestType, &noval, SNAP_TEST_DICT_BITS, KVSTORE_ALLOCATE_DICTS_ON_DEMAND);

    struct snapScanTestState *st = zcalloc(sizeof(*st));
    st->stores = stores;

    /* Population: store 0: d0 = 1 key, d1 = empty (never allocated), d2 = 5000
     * keys, d3 = left mid-rehash. Store 1: d0..d2 = 100 keys each, d3 empty. */
    dictSetResizeEnabled(DICT_RESIZE_ENABLE);
    int plan[SNAP_TEST_STORES][SNAP_TEST_DICTS] = { {1, 0, 5000, -1}, {100, 100, 100, 0} };
    for (int s = 0; s < SNAP_TEST_STORES; s++) {
        for (int didx = 0; didx < SNAP_TEST_DICTS; didx++) {
            int n = plan[s][didx];
            if (n == -1) {
                /* Insert until the dict is rehashing (expand just triggered). */
                int i = 0;
                while (i < SNAP_TEST_MAXKEYS) {
                    serverAssert(kvstoreDictAddRaw(stores[s], didx, snapTestKey(s, didx, i), NULL) != NULL);
                    i++;
                    if (i > 64 && dictIsRehashing(kvstoreGetDict(stores[s], didx))) break;
                }
                n = i;
            } else {
                for (int i = 0; i < n; i++)
                    serverAssert(kvstoreDictAddRaw(stores[s], didx, snapTestKey(s, didx, i), NULL) != NULL);
            }
            st->inserted[s][didx] = n;
        }
    }
    long total_keys = 0;
    for (int s = 0; s < SNAP_TEST_STORES; s++)
        for (int didx = 0; didx < SNAP_TEST_DICTS; didx++) total_keys += st->inserted[s][didx];
    test_cond("scan test: a dict was left mid-rehash at scan start",
              dictIsRehashing(kvstoreGetDict(stores[0], 3)));
    test_cond("scan test: an empty slot has no dict allocated", kvstoreGetDict(stores[0], 1) == NULL);

    /* ---- Full scan with concurrent main-thread reads ---- */
    snapScanCtx *ctx = snapScanCreate(stores, SNAP_TEST_STORES, snapTestDispatch, snapTestSlotDone, st);
    st->ctx = ctx;
    st->dispatch_sleep_us = 0;
    test_cond("scan: all slots PENDING before start", snapTestAllState(ctx, SNAP_SLOT_PENDING));
    snapScanFreeze(ctx);   /* guard installed, no thread yet: the checks below are deterministic */
    {
        /* On-demand freeze (TASK-1.21.6): the freeze pauses nothing; the
         * first write-intent access to a slot the thread has not finished
         * pauses that dict (guard). A read of a dict the thread is not
         * reading does not. */
        dict *d = kvstoreGetDict(stores[0], 0);
        test_cond("scan: no dict is paused right after start", d->pauserehash == 0 && d->pauseAutoResize == 0);
        char *k0 = snapTestKey(0, 0, 0);   /* store 0 dict 0 holds one key */
        dictEntryLink b = NULL;
        dictEntryLink l = kvstoreDictFindLink(stores[0], 0, k0, &b);
        test_cond("scan: a write-intent lookup pauses the dict on demand and finds the key",
                  l != NULL && d->pauserehash == 1 && d->pauseAutoResize == 1);
        zfree(k0);
        /* The layout lock: neither the insert path nor the resize cron may
         * touch a paused table. */
        dict *big = kvstoreGetDict(stores[0], 2);   /* 5000 keys */
        char *kb = snapTestKey(0, 2, 1);
        kvstoreDictFindLink(stores[0], 2, kb, &b);
        zfree(kb);
        signed char exp0 = big->ht_size_exp[0], exp1 = big->ht_size_exp[1];
        long ridx = big->rehashidx;
        int r1 = dictExpandIfNeeded(big), r2 = dictShrinkIfNeeded(big);
        test_cond("scan: dictExpandIfNeeded/dictShrinkIfNeeded are no-ops on a paused dict",
                  r1 == DICT_ERR && r2 == DICT_ERR && big->ht_size_exp[0] == exp0 &&
                  big->ht_size_exp[1] == exp1 && big->rehashidx == ridx);
        snapFunnelClose();   /* the accesses above opened a section: "command end" */
    }
    test_cond("scan: snapScanStartThread() succeeds", snapScanStartThread(ctx) == C_OK);
    /* Concurrent reads from the owner thread while the scan runs. */
    long reads = 0, misses = 0;
    unsigned seed = 7;
    int completions = 0;
    while (!snapScanIsFinished(ctx)) {
        seed = seed * 1103515245u + 12345u;
        int s = (seed >> 8) % SNAP_TEST_STORES, didx = (seed >> 12) % SNAP_TEST_DICTS;
        if (st->inserted[s][didx] > 0) {
            int i = (seed >> 16) % st->inserted[s][didx];
            char *k = snapTestKey(s, didx, i);
            if (kvstoreDictFind(stores[s], didx, k) == NULL) misses++;
            zfree(k);
            reads++;
            snapFunnelClose();   /* a read of a slot under scan opens a section: end the "command" */
        }
        completions += snapScanHandleCompletions(ctx);
    }
    completions += snapScanHandleCompletions(ctx);
    long dispatched, bad;
    atomicGet(st->dispatched, dispatched);
    atomicGet(st->bad, bad);
    int wrong_state;
    atomicGet(st->wrong_state, wrong_state);
    long all_seen = 1;
    for (int s = 0; s < SNAP_TEST_STORES; s++)
        for (int didx = 0; didx < SNAP_TEST_DICTS; didx++)
            for (int i = 0; i < st->inserted[s][didx]; i++) if (!st->seen[s][didx][i]) all_seen = 0;
    int done_once = 1;
    for (int s = 0; s < SNAP_TEST_STORES; s++)
        for (int didx = 0; didx < SNAP_TEST_DICTS; didx++)
            if (st->done_calls[s][didx] != 1 || st->done_aborted[s][didx] != 0) done_once = 0;
    printf("scan: %ld keys dispatched, %ld concurrent reads (%ld misses), %d completions\n",
           dispatched, reads, misses, completions);
    test_cond("scan: dispatched exactly the inserted key set (count)", dispatched == total_keys);
    test_cond("scan: every inserted key was dispatched exactly once", all_seen && bad == 0);
    test_cond("scan: dispatch always ran with its slot in SCANNING state", wrong_state == 0);
    test_cond("scan: concurrent owner-thread lookups all hit", reads > 0 && misses == 0);
    /* Slots empty at freeze complete right there (no ring entry), the rest
     * through the completion ring: together, every slot exactly once. */
    int empty_slots = 0;
    for (int s = 0; s < SNAP_TEST_STORES; s++)
        for (int didx = 0; didx < SNAP_TEST_DICTS; didx++) if (st->inserted[s][didx] == 0) empty_slots++;
    test_cond("scan: every slot completed exactly once, none aborted",
              done_once && completions + empty_slots == SNAP_TEST_STORES * SNAP_TEST_DICTS);
    test_cond("scan: all slots DONE at the end", snapTestAllState(ctx, SNAP_SLOT_DONE));
    test_cond("scan: no slot is owed after completion", !snapScanKeyIsOwed(ctx, 0, 0) && !snapScanKeyIsOwed(ctx, 1, 3));
    test_cond("scan: all dicts unfrozen at the end", snapTestAllUnfrozen(stores));
    /* With the on-demand freeze a dict left mid-rehash at start may finish
     * rehashing through main-thread reads before the thread reaches it (the
     * thread never rehashes: it only reads a layout that holds still). */
    printf("scan: mid-rehash dict %s mid-rehash when its slot completed\n", st->rehashing_at_done ? "still" : "no longer");
    {
        long t, sc, dn; long long kd;
        snapScanProgress(ctx, &t, &sc, &dn, &kd);
        test_cond("scan: progress counters consistent", t == SNAP_TEST_STORES * SNAP_TEST_DICTS && sc == t && dn == t && kd == total_keys);
    }
    snapScanRelease(ctx);

    /* ---- Abort mid-scan ---- */
    memset(st->seen, 0, sizeof(st->seen));
    memset(st->done_calls, 0, sizeof(st->done_calls));
    memset(st->done_aborted, 0, sizeof(st->done_aborted));
    atomicSet(st->dispatched, 0);
    atomicSet(st->bad, 0);
    st->dispatch_sleep_us = accurate ? 200 : 100;   /* 5000-key slot takes >= 0.5 s */
    ctx = snapScanCreate(stores, SNAP_TEST_STORES, snapTestDispatch, snapTestSlotDone, st);
    st->ctx = ctx;
    test_cond("abort: start", snapScanStart(ctx) == C_OK);
    /* Wait until the thread is inside the first big slot. */
    long d0;
    do { usleep(1000); atomicGet(st->dispatched, d0); } while (d0 < 50 && !snapScanIsFinished(ctx));
    long long t0 = snapTestNowNs();
    snapScanAbort(ctx);
    long long abort_ms = (snapTestNowNs() - t0) / 1000000;
    atomicGet(st->dispatched, dispatched);
    int aborted_slots = 0, normal_slots = 0, calls = 0;
    for (int s = 0; s < SNAP_TEST_STORES; s++)
        for (int didx = 0; didx < SNAP_TEST_DICTS; didx++) {
            calls += st->done_calls[s][didx];
            if (st->done_aborted[s][didx]) aborted_slots++; else normal_slots++;
        }
    printf("abort: after %ld dispatched keys, abort took %lld ms; %d slots aborted, %d completed normally\n",
           dispatched, abort_ms, aborted_slots, normal_slots);
    test_cond("abort: stopped well before the scan would have finished", dispatched < total_keys);
    test_cond("abort: returned promptly (< 500 ms)", abort_ms < 500);
    test_cond("abort: thread finished", snapScanIsFinished(ctx));
    test_cond("abort: every slot completed exactly once", calls == SNAP_TEST_STORES * SNAP_TEST_DICTS);
    test_cond("abort: interrupted slots reported as aborted", aborted_slots >= 1);
    test_cond("abort: slots scanned before the abort completed normally", normal_slots >= 2);
    test_cond("abort: all slots DONE", snapTestAllState(ctx, SNAP_SLOT_DONE));
    test_cond("abort: all dicts unfrozen", snapTestAllUnfrozen(stores));
    snapScanRelease(ctx);

    /* ---- Write section handshake: the thread waits for an open section ---- */
    memset(st->seen, 0, sizeof(st->seen));
    memset(st->done_calls, 0, sizeof(st->done_calls));
    memset(st->done_aborted, 0, sizeof(st->done_aborted));
    atomicSet(st->dispatched, 0);
    atomicSet(st->bad, 0);
    st->dispatch_sleep_us = 0;
    ctx = snapScanCreate(stores, SNAP_TEST_STORES, snapTestDispatch, snapTestSlotDone, st);
    st->ctx = ctx;
    snapFunnelOpen();
    test_cond("handshake: start with a write section open", snapScanStart(ctx) == C_OK);
    usleep(30000);
    atomicGet(st->dispatched, dispatched);
    test_cond("handshake: the thread dispatched nothing while the section was open", dispatched == 0 && !snapScanIsFinished(ctx));
    snapFunnelClose();
    while (!snapScanIsFinished(ctx)) usleep(100);
    snapScanHandleCompletions(ctx);
    atomicGet(st->dispatched, dispatched);
    atomicGet(st->bad, bad);
    test_cond("handshake: after the section closed every key was dispatched once", dispatched == total_keys && bad == 0);
    snapScanRelease(ctx);

    /* ---- Abort while the aborting command holds the section open ---- */
    atomicSet(st->dispatched, 0);
    ctx = snapScanCreate(stores, SNAP_TEST_STORES, snapTestDispatch, snapTestSlotDone, st);
    st->ctx = ctx;
    snapFunnelOpen();
    test_cond("abort-in-section: start", snapScanStart(ctx) == C_OK);
    usleep(5000);
    t0 = snapTestNowNs();
    snapScanAbort(ctx);
    abort_ms = (snapTestNowNs() - t0) / 1000000;
    test_cond("abort-in-section: abort returned promptly although the section is still open", abort_ms < 500 && snapScanIsFinished(ctx));
    test_cond("abort-in-section: all slots DONE, dicts unfrozen", snapTestAllState(ctx, SNAP_SLOT_DONE) && snapTestAllUnfrozen(stores));
    snapScanRelease(ctx);
    snapFunnelClose();

    /* ---- Window membership agrees with what the thread would read ---- */
    {
        dict *d = kvstoreGetDict(stores[0], 3);
        /* The earlier concurrent lookups finished its rehash once unfrozen:
         * start a new one so both tables exist (nothing below steps it). */
        if (!dictIsRehashing(d)) dictExpand(d, DICTHT_SIZE(d->ht_size_exp[0]) * 2);
        test_cond("window: test dict has two tables", dictIsRehashing(d) && d->ht_table[1] != NULL);
        for (int t = 0; t < 2; t++) {
            unsigned long size = DICTHT_SIZE(d->ht_size_exp[t]);
            unsigned long lo = size / 4, hi = size / 4 + size / 2;
            /* Keys the thread would read in this window. */
            long nvisit = 0;
            dictWalkBucketRange(d, t, lo, hi, ovTestScanCb, &nvisit);
            snapWindowPublish(d, t, lo, hi);
            snapFunnelOpen();
            long covered = 0, agree = 1;
            for (int i = 0; i < st->inserted[0][3]; i++) {
                char *k = snapTestKey(0, 3, i);
                int cov = snapWindowCoversKey(d, k);
                unsigned long idx = dictGetHash(d, k) & DICTHT_SIZE_MASK(d->ht_size_exp[t]);
                int in_range = idx >= lo && idx < hi;
                if (cov != in_range) agree = 0;
                covered += cov;
                zfree(k);
            }
            snapFunnelClose();
            /* Table 0 windows start at rehashidx in the thread; here [lo,hi) may
             * include moved buckets, so covered >= visited (conservative). */
            test_cond(t == 0 ? "window: table 0 membership is the bucket-range test and covers every key the thread would read"
                             : "window: table 1 membership is the bucket-range test and covers every key the thread would read",
                      agree && covered >= nvisit && covered > 0);
            test_cond(t == 0 ? "window: a different dict is never covered (table 0)" : "window: a different dict is never covered (table 1)",
                      !snapWindowCoversKey(kvstoreGetDict(stores[0], 2), "k:0:2:1"));
        }
        snapWindowPublish(NULL, 0, 0, 0);
        snapFunnelOpen();
        test_cond("window: no window covers nothing", !snapWindowCoversKey(d, "k:0:3:1"));
        snapFunnelClose();
    }

    /* ---- Bucket bitmap: buckets marked emitted are skipped by the thread ---- */
    memset(st->seen, 0, sizeof(st->seen));
    memset(st->done_calls, 0, sizeof(st->done_calls));
    memset(st->done_aborted, 0, sizeof(st->done_aborted));
    atomicSet(st->dispatched, 0);
    atomicSet(st->bad, 0);
    ctx = snapScanCreate(stores, SNAP_TEST_STORES, snapTestDispatch, snapTestSlotDone, st);
    st->ctx = ctx;
    snapScanFreeze(ctx);
    {
        dict *d = kvstoreGetDict(stores[1], 0);
        size_t none = snapScanBitmapBytes(ctx);
        snapScanAttach(ctx, 1, 0, d);   /* what the guard does on the first write to the slot */
        size_t before = snapScanBitmapBytes(ctx);
        for (int t = 0; t < 2; t++)
            for (unsigned long i = 0; i < DICTHT_SIZE(d->ht_size_exp[t]); i++)
                snapScanMarkBucketEmitted(ctx, 1, 0, t, i);
        test_cond("bitmap: none at freeze, allocated for every table of the dict on attach", none == 0 && before > 0 &&
                  snapScanBucketEmitted(ctx, 1, 0, 0, 0) && !snapScanBucketEmitted(ctx, 1, 1, 0, 0));
    }
    test_cond("bitmap: start thread", snapScanStartThread(ctx) == C_OK);
    while (!snapScanIsFinished(ctx)) usleep(100);
    snapScanHandleCompletions(ctx);
    atomicGet(st->dispatched, dispatched);
    atomicGet(st->bad, bad);
    {
        int skipped_ok = 1;
        for (int i = 0; i < st->inserted[1][0]; i++) if (st->seen[1][0][i]) skipped_ok = 0;
        int others_ok = 1;
        for (int s = 0; s < SNAP_TEST_STORES; s++)
            for (int didx = 0; didx < SNAP_TEST_DICTS; didx++) {
                if (s == 1 && didx == 0) continue;
                for (int i = 0; i < st->inserted[s][didx]; i++) if (!st->seen[s][didx][i]) others_ok = 0;
            }
        test_cond("bitmap: keys of emitted buckets were not dispatched, everything else once",
                  skipped_ok && others_ok && bad == 0 && dispatched == total_keys - st->inserted[1][0]);
        test_cond("bitmap: freed when the slots completed", snapScanBitmapBytes(ctx) == 0);
    }
    snapScanRelease(ctx);

    /* ---- Owed: a bucket emitted ahead of the window stays not-owed once the
     * window moves over it (the thread skips it by its bit). Simulates one
     * write section spanning a window publish, with no thread running. ---- */
    ctx = snapScanCreate(stores, SNAP_TEST_STORES, snapTestDispatch, snapTestSlotDone, st);
    st->ctx = ctx;
    snapScanFreeze(ctx);
    {
        dict *d = kvstoreGetDict(stores[0], 3);   /* populated (see the window test) */
        unsigned long size = DICTHT_SIZE(d->ht_size_exp[0]), mid = size / 2;
        int pos, p_ahead, p_after, p_other, p_behind;
        test_cond("owed: test dict exists and has at least 8 buckets", d != NULL && size >= 8);
        snapScanAttach(ctx, 0, 3, d);               /* bitmap and pause, as the guard would */
        snapSlotSetState(ctx, 0, 3, SNAP_SLOT_SCANNING);
        snapWindowPublish(d, 0, 0, mid);            /* the thread reads [0, mid) */
        snapFunnelOpen();                           /* a command opens its write section */
        int owed_ahead = snapBucketOwed(ctx, 0, 3, d, 0, mid + 1, &pos); p_ahead = pos;
        snapScanMarkBucketEmitted(ctx, 0, 3, 0, mid + 1);   /* ... and emits the bucket */
        snapWindowPublish(d, 0, mid, size);         /* the thread moves on; it would quiesce now */
        int owed_after = snapBucketOwed(ctx, 0, 3, d, 0, mid + 1, &pos); p_after = pos;
        int owed_other = snapBucketOwed(ctx, 0, 3, d, 0, mid + 2, &pos); p_other = pos;
        int owed_behind = snapBucketOwed(ctx, 0, 3, d, 0, 0, &pos); p_behind = pos;
        snapFunnelClose();
        snapWindowPublish(NULL, 0, 0, 0);
        test_cond("owed: a bucket ahead of the window and not emitted is owed", owed_ahead && p_ahead == SNAP_POS_AHEAD);
        test_cond("owed: the same bucket, emitted, is not owed after the window moved over it", !owed_after && p_after == SNAP_POS_INSIDE);
        test_cond("owed: an unemitted bucket inside the window is owed", owed_other && p_other == SNAP_POS_INSIDE);
        test_cond("owed: a bucket behind the window is not owed", !owed_behind && p_behind == SNAP_POS_BEHIND);
    }
    snapScanAbort(ctx);   /* frozen, never started: unfreezes */
    snapScanRelease(ctx);

    /* ---- Empty stores ---- */
    {
        kvstore *empty[2];
        empty[0] = kvstoreCreate(&KvstoreTestType, &noval, 0, KVSTORE_ALLOCATE_DICTS_ON_DEMAND);
        empty[1] = kvstoreCreate(&KvstoreTestType, &noval, 3, KVSTORE_ALLOCATE_DICTS_ON_DEMAND);
        atomicSet(st->dispatched, 0);
        st->dispatch_sleep_us = 0;
        snapScanCtx *ectx = snapScanCreate(empty, 2, snapTestDispatch, NULL, st);
        st->ctx = NULL;
        test_cond("empty: start", snapScanStart(ectx) == C_OK);
        while (!snapScanIsFinished(ectx)) usleep(100);
        snapScanHandleCompletions(ectx);
        atomicGet(st->dispatched, dispatched);
        long et, esc, edn; long long ekd;
        snapScanProgress(ectx, &et, &esc, &edn, &ekd);
        test_cond("empty: all 9 slots completed (at freeze) with zero dispatches", et == 9 && edn == 9 && esc == 9 && dispatched == 0);
        snapScanRelease(ectx);
        kvstoreRelease(empty[0]);
        kvstoreRelease(empty[1]);
    }

    for (int s = 0; s < SNAP_TEST_STORES; s++) kvstoreRelease(stores[s]);
    zfree(st);
}

/* ---- Placement / range-aware allocator tests ---- */
static void snapPlacementTests(void) {
    if (!snapArenaAvailable()) return;
    server.save_small_max_bytes = 4096;

    /* Generic allocator paths keep arena memory in the arena. */
    {
        char *p = snapNoforkAlloc(100);
        p = zrealloc(p, 5000);
        test_cond("zrealloc() of an arena pointer stays in the arena (grow)", zmalloc_is_nofork(p));
        p = zrealloc(p, 40);
        test_cond("zrealloc() of an arena pointer stays in the arena (shrink)", zmalloc_is_nofork(p));
        zfree(p);
        char *q = zmalloc(100);
        q = zrealloc(q, 5000);
        test_cond("zrealloc() of a default pointer stays outside the arena", !zmalloc_is_nofork(q));
        zfree(q);

        const int N = 2000;
        void **a = zmalloc(sizeof(void*) * N);
        for (int i = 0; i < N; i++) a[i] = snapNoforkAlloc(64);
        for (int i = 0; i < N; i++) zfree(a[i]);                     /* generic zfree */
        int leaked = 0;
        for (int i = 0; i < N; i++) { a[i] = zmalloc(64); if (zmalloc_is_nofork(a[i])) leaked++; }
        for (int i = 0; i < N; i++) zfree(a[i]);
        test_cond("arena blocks freed with plain zfree() never come back from zmalloc()", leaked == 0);
        for (int i = 0; i < N; i++) a[i] = snapNoforkAlloc(64);
        for (int i = 0; i < N; i++) zfree_with_size(a[i], 64);       /* sized generic free */
        leaked = 0;
        for (int i = 0; i < N; i++) { a[i] = zmalloc(64); if (zmalloc_is_nofork(a[i])) leaked++; }
        for (int i = 0; i < N; i++) zfree(a[i]);
        test_cond("arena blocks freed with zfree_with_size() never come back from zmalloc()", leaked == 0);
        zfree(a);
#if defined(SNAP_NOFORK_ARENA_SUPPORTED) && defined(HAVE_DEFRAG)
        void *nt = zmalloc_nofork_no_tcache(300);   /* the no-tcache entry points exist only with the arena and defrag */
        test_cond("zmalloc_nofork_no_tcache() allocates in the arena", zmalloc_is_nofork(nt));
        zfree_no_tcache(nt);
#endif
    }

    /* sds growth through the sds API keeps the arena. */
    {
        sds s = sdsnewlen("x", 1);
        sds t = snapSdsMove(s, 1);
        test_cond("sds moved into the arena", zmalloc_is_nofork(sdsAllocPtr(t)));
        t = sdsMakeRoomFor(t, 20000);
        test_cond("sdsMakeRoomFor() on an arena sds stays in the arena", zmalloc_is_nofork(sdsAllocPtr(t)));
        sdsfree(t);
    }

    /* Classification table. */
    test_cond("classify: small raw string is NOFORK", snapClassify(OBJ_STRING, OBJ_ENCODING_RAW, 100) == SNAP_NOFORK);
    test_cond("classify: 4096-byte raw string is NOFORK", snapClassify(OBJ_STRING, OBJ_ENCODING_RAW, 4096) == SNAP_NOFORK);
    test_cond("classify: 4097-byte raw string is FORK", snapClassify(OBJ_STRING, OBJ_ENCODING_RAW, 4097) == SNAP_FORK);
    test_cond("classify: INT / EMBSTR are NOFORK regardless of size",
              snapClassify(OBJ_STRING, OBJ_ENCODING_INT, 0) == SNAP_NOFORK &&
              snapClassify(OBJ_STRING, OBJ_ENCODING_EMBSTR, 100000) == SNAP_NOFORK);
    test_cond("classify: small listpack hash/set/zset/list are NOFORK",
              snapClassify(OBJ_HASH, OBJ_ENCODING_LISTPACK, 100) == SNAP_NOFORK &&
              snapClassify(OBJ_HASH, OBJ_ENCODING_LISTPACK_EX, 100) == SNAP_NOFORK &&
              snapClassify(OBJ_SET, OBJ_ENCODING_LISTPACK, 100) == SNAP_NOFORK &&
              snapClassify(OBJ_SET, OBJ_ENCODING_INTSET, 100) == SNAP_NOFORK &&
              snapClassify(OBJ_ZSET, OBJ_ENCODING_LISTPACK, 100) == SNAP_NOFORK &&
              snapClassify(OBJ_LIST, OBJ_ENCODING_LISTPACK, 100) == SNAP_NOFORK);
    test_cond("classify: multi-allocation encodings and other types are FORK",
              snapClassify(OBJ_HASH, OBJ_ENCODING_HT, 10) == SNAP_FORK &&
              snapClassify(OBJ_SET, OBJ_ENCODING_HT, 10) == SNAP_FORK &&
              snapClassify(OBJ_ZSET, OBJ_ENCODING_SKIPLIST, 10) == SNAP_FORK &&
              snapClassify(OBJ_LIST, OBJ_ENCODING_QUICKLIST, 10) == SNAP_FORK &&
              snapClassify(OBJ_STREAM, OBJ_ENCODING_STREAM, 10) == SNAP_FORK &&
              snapClassify(OBJ_MODULE, OBJ_ENCODING_RAW, 10) == SNAP_FORK &&
              snapClassify(OBJ_ARRAY, OBJ_ENCODING_SLICED_ARRAY, 10) == SNAP_FORK);

    /* Placement through kvobjSet(). */
    sds key = sdsnew("placement-key");
    char buf[20000];
    memset(buf, 'v', sizeof(buf));
    {
        kvobj *kv = kvobjSet(key, createRawStringObject(buf, 100), 0);
        test_cond("kvobj header (raw string) is in the arena", zmalloc_is_nofork(kv));
        test_cond("small raw string value moved into the arena", zmalloc_is_nofork(sdsAllocPtr(kv->ptr)) && snapKeyClass(kv) == SNAP_NOFORK);
        test_cond("value content preserved after the move", sdslen(kv->ptr) == 100 && memcmp(kv->ptr, buf, 100) == 0);
        /* Unbounded in-place growth keeps the arena; the policy hook moves it out. */
        kv->ptr = sdscatlen(kv->ptr, buf, 10000);
        test_cond("grown string still in the arena before the policy hook", zmalloc_is_nofork(sdsAllocPtr(kv->ptr)));
        snapPlaceValue(kv);
        test_cond("snapPlaceValue() moves a string over the threshold out of the arena",
                  !zmalloc_is_nofork(sdsAllocPtr(kv->ptr)) && zmalloc_is_nofork(kv) && snapKeyClass(kv) == SNAP_FORK);
        test_cond("content preserved after moving out", sdslen(kv->ptr) == 10100 && memcmp(kv->ptr, buf, 100) == 0);
        decrRefCount(kv);
    }
    {
        kvobj *kv = kvobjSet(key, createRawStringObject(buf, 10000), 0);
        test_cond("large raw string: header in arena, value outside, class FORK",
                  zmalloc_is_nofork(kv) && !zmalloc_is_nofork(sdsAllocPtr(kv->ptr)) && snapKeyClass(kv) == SNAP_FORK);
        decrRefCount(kv);
    }
    {
        kvobj *kv = kvobjSet(key, createStringObject("tiny", 4), 0);
        test_cond("embstr value: header in arena, class NOFORK",
                  kv->encoding == OBJ_ENCODING_EMBSTR && zmalloc_is_nofork(kv) && snapKeyClass(kv) == SNAP_NOFORK);
        decrRefCount(kv);
        kv = kvobjSet(key, createStringObjectFromLongLongForValue(1234567), 0);
        test_cond("int value: header in arena, class NOFORK",
                  kv->encoding == OBJ_ENCODING_INT && zmalloc_is_nofork(kv) && snapKeyClass(kv) == SNAP_NOFORK);
        decrRefCount(kv);
    }
    {
        robj *o = createObject(OBJ_HASH, lpNew(0));
        o->encoding = OBJ_ENCODING_LISTPACK;
        o->ptr = lpAppend(o->ptr, (unsigned char*)"field", 5);
        o->ptr = lpAppend(o->ptr, (unsigned char*)"value", 5);
        kvobj *kv = kvobjSet(key, o, 0);
        test_cond("small listpack hash moved into the arena", zmalloc_is_nofork(kv->ptr) && snapKeyClass(kv) == SNAP_NOFORK);
        unsigned char *p = lpFirst(kv->ptr);
        int64_t l; unsigned char *sv = lpGet(p, &l, NULL);
        test_cond("listpack content preserved", sv && l == 5 && memcmp(sv, "field", 5) == 0);
        /* Grow in place past the threshold: stays in the arena, class stays NOFORK by placement. */
        kv->ptr = lpAppend(kv->ptr, (unsigned char*)buf, 6000);
        test_cond("listpack grown in place stays in the arena", zmalloc_is_nofork(kv->ptr) && snapKeyClass(kv) == SNAP_NOFORK);
        snapPlaceValue(kv);
        test_cond("policy hook moves an oversized listpack out", !zmalloc_is_nofork(kv->ptr) && snapKeyClass(kv) == SNAP_FORK);
        decrRefCount(kv);
    }
    {
        robj *o = createIntsetObject();
        uint8_t ok;
        for (long long i = 0; i < 50; i++) o->ptr = intsetAdd(o->ptr, i * 7, &ok);
        kvobj *kv = kvobjSet(key, o, 0);
        test_cond("intset moved into the arena", zmalloc_is_nofork(kv->ptr) && snapKeyClass(kv) == SNAP_NOFORK && intsetLen(kv->ptr) == 50);
        decrRefCount(kv);
    }
    {
        robj *o = createSetObject();   /* hashtable encoding */
        kvobj *kv = kvobjSet(key, o, 0);
        test_cond("hashtable set: header in arena, dict outside, class FORK",
                  zmalloc_is_nofork(kv) && !zmalloc_is_nofork(kv->ptr) && snapKeyClass(kv) == SNAP_FORK);
        decrRefCount(kv);
    }
    sdsfree(key);

    /* Keyspace-style dicts (nofork_alloc) put struct, tables and entry nodes in the arena. */
    {
        dictType nofork = KvstoreDictTestType;
        nofork.no_value = 1;
        nofork.nofork_alloc = 1;
        dictType plain = KvstoreDictTestType;
        plain.no_value = 1;
        kvstore *ks = kvstoreCreate(&KvstoreTestType, &nofork, 0, KVSTORE_ALLOCATE_DICTS_ON_DEMAND);
        kvstore *kp = kvstoreCreate(&KvstoreTestType, &plain, 0, KVSTORE_ALLOCATE_DICTS_ON_DEMAND);
        dictSetResizeEnabled(DICT_RESIZE_ENABLE);
        for (int i = 0; i < 3000; i++) {
            serverAssert(kvstoreDictAddRaw(ks, 0, snapTestKey(0, 0, i), NULL) != NULL);
            serverAssert(kvstoreDictAddRaw(kp, 0, snapTestKey(0, 0, i), NULL) != NULL);
        }
        dict *ds = kvstoreGetDict(ks, 0), *dp = kvstoreGetDict(kp, 0);
        while (dictIsRehashing(ds)) dictRehash(ds, 100);
        test_cond("nofork_alloc dict: kvstore struct, dict struct and bucket array are in the arena",
                  zmalloc_is_nofork(ks) && zmalloc_is_nofork(ds) && zmalloc_is_nofork(ds->ht_table[0]));
        test_cond("default dict: struct and bucket array are outside the arena",
                  !zmalloc_is_nofork(kp) && !zmalloc_is_nofork(dp) && !zmalloc_is_nofork(dp->ht_table[0]));
        /* Force another expand (rehash allocates a new table) and check it too. */
        for (int i = 3000; i < 9000; i++)
            serverAssert(kvstoreDictAddRaw(ks, 0, snapTestKey(0, 0, i), NULL) != NULL);
        test_cond("nofork_alloc dict: table allocated by rehash is in the arena",
                  zmalloc_is_nofork(ds->ht_table[0]) && (ds->ht_table[1] == NULL || zmalloc_is_nofork(ds->ht_table[1])));
        kvstoreRelease(ks);
        kvstoreRelease(kp);
    }
}

/* ---- Overlay tests (kvstore level: entries are plain C strings) ---- */
struct ovTestCounters { int clones; int frees; int frees_shared; };

static void *ovTestClone(void *priv, int didx, void *orig, int *shared) {
    UNUSED(didx);
    struct ovTestCounters *c = priv;
    c->clones++;
    *shared = 0;
    return zstrdup(orig);
}

static void ovTestFree(void *priv, int didx, void *orig, int shared) {
    UNUSED(didx);
    struct ovTestCounters *c = priv;
    c->frees++;
    if (shared) c->frees_shared++;
    zfree(orig);
}

static long ovTestCountIter(kvstore *ks, int *has_new, void **entry_for_5) {
    long n = 0;
    kvstoreIterator it;
    dictEntry *de;
    kvstoreIteratorInit(&it, ks);
    while ((de = kvstoreIteratorNext(&it)) != NULL) {
        const char *k = dictGetKey(de);
        if (!strcmp(k, "k:0:0:new")) *has_new = 1;
        if (!strcmp(k, "k:0:0:5") && entry_for_5) *entry_for_5 = dictGetKey(de);
        n++;
    }
    kvstoreIteratorReset(&it);
    return n;
}

static void ovTestScanCb(void *priv, const dictEntry *de, dictEntry **plink) {
    UNUSED(plink); UNUSED(de);
    (*(long *)priv)++;
}

static void snapOverlayTests(int abort_variant) {
    dictType noval = KvstoreDictTestType;
    noval.no_value = 1;
    noval.keyCompare = snapTestKeyCompare;
    kvstore *stores[1];
    stores[0] = kvstoreCreate(&KvstoreTestType, &noval, SNAP_TEST_DICT_BITS, KVSTORE_ALLOCATE_DICTS_ON_DEMAND);
    kvstore *ks = stores[0];
    const int N = 1000;
    char *orig5 = NULL, *orig7 = NULL;
    for (int i = 0; i < N; i++) {
        char *k = snapTestKey(0, 0, i);
        if (i == 5) orig5 = k;
        if (i == 7) orig7 = k;
        serverAssert(kvstoreDictAddRaw(ks, 0, k, NULL) != NULL);
    }
    struct snapScanTestState *st = zcalloc(sizeof(*st));
    st->inserted[0][0] = N;
    st->dispatch_sleep_us = abort_variant ? 50 : 0;
    struct ovTestCounters cnt = {0, 0, 0};
    void *privs[1] = { &cnt };

    snapScanCtx *ctx = snapScanCreate(stores, 1, snapTestDispatch, snapTestSlotDone, st);
    st->ctx = ctx;
    kvstoreOverlayHooks hooks = { ovTestClone, ovTestFree, snapOverlayEnter, NULL, NULL };
    snapScanSetOverlay(ctx, &hooks, privs);
    test_cond("overlay: start", snapScanStart(ctx) == C_OK);
    test_cond("overlay: nothing is attached at start, the empty slot is DONE",
              !kvstoreOverlayIsFrozen(ks, 0) && !kvstoreOverlayIsFrozen(ks, 3) && !snapScanKeyIsOwed(ctx, 0, 3));
    dict *frozen = kvstoreGetDict(ks, 0);

    /* Read: original, no clone. */
    dictEntry *de = kvstoreDictFind(ks, 0, "k:0:0:5");
    test_cond("overlay: read lookup returns the original without cloning", de && dictGetKey(de) == orig5 && cnt.clones == 0);

    /* Write lookup: clone in temp, frozen untouched. */
    dictEntryLink bucket = NULL;
    dictEntryLink link = kvstoreDictFindLink(ks, 0, "k:0:0:5", &bucket);
    char *clone5 = link ? dictGetKey(*link) : NULL;
    test_cond("overlay: write lookup returns a clone", link && clone5 && clone5 != orig5 && !strcmp(clone5, "k:0:0:5") && cnt.clones == 1);
    test_cond("overlay: the write attached the overlay to the populated slot only",
              kvstoreOverlayIsFrozen(ks, 0) && !kvstoreOverlayIsFrozen(ks, 3));
    test_cond("overlay: frozen dict still holds the original", dictGetKey(dictFind(frozen, "k:0:0:5")) == orig5);
    de = kvstoreDictFind(ks, 0, "k:0:0:5");
    test_cond("overlay: subsequent read returns the clone", de && dictGetKey(de) == clone5);
    link = kvstoreDictFindLink(ks, 0, "k:0:0:5", &bucket);
    test_cond("overlay: second write lookup reuses the clone", link && dictGetKey(*link) == clone5 && cnt.clones == 1);

    /* Delete an untouched key: clone-then-delete, tombstone. */
    int table;
    link = kvstoreDictTwoPhaseUnlinkFind(ks, 0, "k:0:0:7", &table);
    test_cond("overlay: two-phase unlink find on an owed key returns a temp link", link != NULL && dictGetKey(*link) != orig7);
    kvstoreDictTwoPhaseUnlinkFree(ks, 0, link, table);
    test_cond("overlay: deleted key is invisible", kvstoreDictFind(ks, 0, "k:0:0:7") == NULL);
    test_cond("overlay: frozen dict still holds the deleted original", dictGetKey(dictFind(frozen, "k:0:0:7")) == orig7);
    test_cond("overlay: logical size dropped to N-1", kvstoreDictSize(ks, 0) == (unsigned long)N - 1 && kvstoreSize(ks) == (unsigned long long)N - 1);
    test_cond("overlay: deleting a deleted key fails", kvstoreDictTwoPhaseUnlinkFind(ks, 0, "k:0:0:7", &table) == NULL &&
              kvstoreDictDelete(ks, 0, "k:0:0:7") == DICT_ERR);

    /* Add a new key and re-add the deleted one. */
    link = kvstoreDictFindLink(ks, 0, "k:0:0:new", &bucket);
    test_cond("overlay: miss returns no link", link == NULL);
    kvstoreDictSetAtLink(ks, 0, zstrdup("k:0:0:new"), &bucket, 1);
    char *readd7 = zstrdup("k:0:0:7");
    kvstoreDictSetAtLink(ks, 0, readd7, NULL, 1);
    test_cond("overlay: added and re-added keys are visible",
              kvstoreDictFind(ks, 0, "k:0:0:new") && dictGetKey(kvstoreDictFind(ks, 0, "k:0:0:7")) == readd7);
    test_cond("overlay: logical size is N+1", kvstoreDictSize(ks, 0) == (unsigned long)N + 1 && kvstoreSize(ks) == (unsigned long long)N + 1);
    test_cond("overlay: frozen dict does not contain the added key", dictFind(frozen, "k:0:0:new") == NULL);
    /* Add to a slot that was empty at freeze: not frozen, goes to a real dict. */
    kvstoreDictSetAtLink(ks, 3, zstrdup("k:0:3:0"), NULL, 1);
    test_cond("overlay: add into a slot empty at freeze goes to its real dict", kvstoreDictFind(ks, 3, "k:0:3:0") != NULL && kvstoreDictSize(ks, 3) == 1 && kvstoreGetDict(ks, 3) != NULL);

    /* Enumeration sees the logical keyspace. */
    int has_new = 0; void *e5 = NULL;
    long n = ovTestCountIter(ks, &has_new, &e5);
    test_cond("overlay: kvstore iterator yields the logical keyspace (N+2 keys, add included, clone substituted)",
              n == N + 2 && has_new && e5 == clone5);
    long scanned = 0;
    unsigned long long cursor = 0;
    do { cursor = kvstoreScan(ks, cursor, -1, ovTestScanCb, NULL, &scanned); } while (cursor);
    test_cond("overlay: kvstoreScan yields the logical keyspace", scanned == N + 2);
    int bad_random = 0;
    for (int i = 0; i < 200; i++) {
        dictEntry *r = kvstoreDictGetFairRandomKey(ks, 0);
        if (!r) { bad_random++; continue; }
        const char *k = dictGetKey(r);
        if (!strcmp(k, "k:0:0:5") && dictGetKey(r) != clone5) bad_random++;
    }
    test_cond("overlay: random sampling never returns NULL or a shadowed original", bad_random == 0);
    dictEntry *some[8];
    unsigned int got = kvstoreDictGetSomeKeys(ks, 0, some, 8);
    test_cond("overlay: GetSomeKeys returns entries", got > 0);

    /* The scan thread must have dispatched exactly the fork-instant set. The
     * writes above opened a write section (overlay 'enter' hook), which in the
     * server ends with the command: end it here or the thread waits forever. */
    test_cond("overlay: the writes above opened a write section", (snap_funnel_seq & 1) == 1);
    snapFunnelClose();
    if (abort_variant) {
        long d0;
        do { usleep(500); atomicGet(st->dispatched, d0); } while (d0 < 10 && !snapScanIsFinished(ctx));
        snapScanAbort(ctx);
    } else {
        while (!snapScanIsFinished(ctx)) usleep(200);
        snapScanHandleCompletions(ctx);
    }
    long dispatched, bad;
    atomicGet(st->dispatched, dispatched);
    atomicGet(st->bad, bad);
    if (!abort_variant) {
        int all_seen = 1;
        for (int i = 0; i < N; i++) if (!st->seen[0][0][i]) all_seen = 0;
        test_cond("overlay: scan dispatched exactly the fork-instant set (deleted key included, added keys excluded)",
                  dispatched == N && bad == 0 && all_seen);
    }

    /* After merge: a single live dict with the logical contents. */
    test_cond("overlay: slots are unfrozen after completion", !kvstoreOverlayIsFrozen(ks, 0) && !kvstoreOverlayIsFrozen(ks, 3));
    frozen = kvstoreGetDict(ks, 0);
    test_cond("overlay: merged dict holds the clone, the re-added key and the new key",
              dictGetKey(dictFind(frozen, "k:0:0:5")) == clone5 &&
              dictGetKey(dictFind(frozen, "k:0:0:7")) == readd7 &&
              dictFind(frozen, "k:0:0:new") != NULL &&
              kvstoreGetDict(ks, 3) && dictFind(kvstoreGetDict(ks, 3), "k:0:3:0") != NULL);
    test_cond("overlay: merged sizes are consistent",
              dictSize(frozen) == (unsigned long)N + 1 && kvstoreDictSize(ks, 0) == (unsigned long)N + 1 &&
              kvstoreSize(ks) == (unsigned long long)N + 2);
    test_cond("overlay: exactly the two replaced originals were freed", cnt.frees == 2 && cnt.frees_shared == 0 && cnt.clones == 2);
    has_new = 0; e5 = NULL;
    n = ovTestCountIter(ks, &has_new, &e5);
    test_cond("overlay: iteration after merge matches", n == N + 2 && has_new && e5 == clone5);

    snapScanRelease(ctx);
    kvstoreRelease(ks);
    zfree(st);
}

int snapshotTest(int argc, char **argv, int flags) {
    UNUSED(argc);
    {
        union { uint8_t byte; kvBits bits; } w;
        w.byte = 0; w.bits.key_sds_type = 3; w.bits.no_evict = 1;
        test_cond("kvBits: key_sds_type and no_evict occupy bits 0-2", w.byte == 0x07);
    }
    UNUSED(argv);
    int accurate = flags & REDIS_TEST_ACCURATE;
    int large = flags & REDIS_TEST_LARGE_MEMORY;

    /* Minimal server state so serverLog() works in test mode. */
    server.in_fork_child = CHILD_TYPE_NONE;
    server.logfile = "";
    server.verbosity = LL_DEBUG;
    server.bgsave_hybrid = 1;
    snapArenaInit();

#ifdef SNAP_NOFORK_ARENA_SUPPORTED
    test_cond("SNAP_NOFORK arena is available on Linux+jemalloc", snapArenaAvailable());
#else
    test_cond("SNAP_NOFORK arena reported unavailable on this platform", !snapArenaAvailable());
    void *fb = snapNoforkAlloc(100);
    test_cond("fallback alloc goes to the default allocator", fb != NULL && !snapPtrIsNofork(fb));
    snapNoforkFree(fb);
    /* The background scan and the keyspace overlay do not depend on the arena. */
    snapScanTests(accurate);
    snapOverlayTests(0);
    snapOverlayTests(1);
    test_report();
    return 0;
#endif

    /* 1. predicate */
    char *p = snapNoforkAlloc(100);
    char *q = zmalloc(100);
    memset(p, 'x', 100);
    test_cond("snapPtrIsNofork() is true for an arena pointer", snapPtrIsNofork(p));
    test_cond("snapPtrIsNofork() is false for a zmalloc pointer", !snapPtrIsNofork(q));
    test_cond("snapNoforkUsableSize() >= requested size", snapNoforkUsableSize(p) >= 100);

    /* 2. fork visibility */
    test_cond("SNAP_NOFORK mapping carries the VM_DONTCOPY (dc) flag", snapTestMappingIsDontcopy(p) == 1);
    test_cond("zmalloc mapping does not carry the dc flag", snapTestMappingIsDontcopy(q) == 0);
    test_cond("fork child survives and can use the default allocator", snapTestChildAllocatorAlive() == 1);
    test_cond("fork child faults when writing a SNAP_NOFORK allocation", snapTestChildWriteFaults(p) == 1);
    test_cond("fork child can write a zmalloc allocation", snapTestChildWriteFaults(q) == 0);
    /* Fork visibility flip (TASK-1.22): one madvise over the reservation. */
    test_cond("arena is DONTFORK by default", !snapArenaForkable());
    test_cond("flip: arena made forkable", snapArenaSetForkable(1) && snapArenaForkable());
    test_cond("flip: fork child can write a SNAP_NOFORK allocation while forkable", snapTestChildWriteFaults(p) == 0);
    test_cond("flip: arena back to DONTFORK", snapArenaSetForkable(0) && !snapArenaForkable());
    test_cond("flip: fork child faults again on the same allocation", snapTestChildWriteFaults(p) == 1);

    /* Large (>= 2MB, multi-page-run) allocation is also DONTFORK. */
    char *big = snapNoforkAlloc(5 * 1024 * 1024);
    memset(big, 1, 5 * 1024 * 1024);
    test_cond("large SNAP_NOFORK allocation is in the arena", snapPtrIsNofork(big));
    test_cond("fork child faults on a large SNAP_NOFORK allocation (first page)", snapTestChildWriteFaults(big) == 1);
    test_cond("fork child faults on a large SNAP_NOFORK allocation (last page)",
              snapTestChildWriteFaults(big + 5 * 1024 * 1024 - 1) == 1);

    /* realloc keeps the class */
    p = snapNoforkRealloc(p, 5000);
    test_cond("snapNoforkRealloc() result stays in the arena", snapPtrIsNofork(p));
    test_cond("fork child faults after realloc", snapTestChildWriteFaults(p) == 1);
    snapNoforkFree(p);
    zfree(q);
    snapNoforkFree(big);

    /* 3. stress: mixed sizes, frees, purge/decay, then re-check the flag. */
    {
        const int N = accurate ? 200000 : 40000;
        void **ptrs = zmalloc(sizeof(void*) * N);
        unsigned seed = 12345;
        int bad_class = 0, bad_fork = 0;
        for (int round = 0; round < 4; round++) {
            for (int i = 0; i < N; i++) {
                seed = seed * 1103515245u + 12345u;
                size_t sz = 8 + (seed >> 8) % 8192;
                if (i % 5000 == 0) sz = 3 * 1024 * 1024 + (seed % 1024) * 1024;
                ptrs[i] = snapNoforkAlloc(sz);
                ((char*)ptrs[i])[0] = 1;
                ((char*)ptrs[i])[sz-1] = 1;
            }
            for (int i = 0; i < N; i += 2) { snapNoforkFree(ptrs[i]); ptrs[i] = NULL; }
            snapTestPurgeArena();
            for (int i = 1; i < N; i += 2) {
                seed = seed * 1103515245u + 12345u;
                ptrs[i] = snapNoforkRealloc(ptrs[i], 8 + (seed >> 8) % 16384);
            }
            snapTestPurgeArena();
            for (int i = 1; i < N; i += (N / 32)) {
                if (!snapPtrIsNofork(ptrs[i])) bad_class++;
                if (snapTestChildWriteFaults(ptrs[i]) != 1) bad_fork++;
            }
            for (int i = 1; i < N; i += 2) snapNoforkFree(ptrs[i]);
            snapTestPurgeArena();
        }
        /* Fresh allocation after purge cycles must still be DONTFORK (commit path). */
        void *fresh = snapNoforkAlloc(4096 * 3);
        memset(fresh, 2, 4096 * 3);
        test_cond("mapping after purge cycles carries the dc flag", snapTestMappingIsDontcopy(fresh) == 1);
        test_cond("stress: sampled live pointers are all in the arena", bad_class == 0);
        test_cond("stress: sampled live pointers all fault in the child after purge/decay/realloc cycles", bad_fork == 0);
        test_cond("allocation after purge cycles is still DONTFORK", snapTestChildWriteFaults(fresh) == 1);
        snapNoforkFree(fresh);
        zfree(ptrs);
    }

    /* 4. tcache isolation: freed SNAP_NOFORK blocks must never be recycled into
     * default-arena allocations, and vice versa. */
    {
        const int N = 2000;
        void **a = zmalloc(sizeof(void*) * N);
        for (int i = 0; i < N; i++) a[i] = snapNoforkAlloc(64);
        for (int i = 0; i < N; i++) snapNoforkFree(a[i]);
        int leaked = 0;
        for (int i = 0; i < N; i++) { a[i] = zmalloc(64); if (snapPtrIsNofork(a[i])) leaked++; }
        for (int i = 0; i < N; i++) zfree(a[i]);
        test_cond("freed SNAP_NOFORK blocks are never handed to zmalloc()", leaked == 0);

        for (int i = 0; i < N; i++) a[i] = zmalloc(64);
        for (int i = 0; i < N; i++) zfree(a[i]);
        int wrong = 0;
        for (int i = 0; i < N; i++) { a[i] = snapNoforkAlloc(64); if (!snapPtrIsNofork(a[i])) wrong++; }
        for (int i = 0; i < N; i++) snapNoforkFree(a[i]);
        test_cond("freed zmalloc blocks are never handed to snapNoforkAlloc()", wrong == 0);
        zfree(a);
    }

    /* 5. accounting */
    {
        size_t before = zmalloc_used_memory();
        void *x = snapNoforkAlloc(10000);
        size_t mid = zmalloc_used_memory();
        snapNoforkFreeSize(x, snapNoforkUsableSize(x));
        size_t after = zmalloc_used_memory();
        test_cond("snapNofork allocations are counted in zmalloc_used_memory()", mid > before && after == before);
    }

    /* 6. microbenchmark (reported, not asserted) */
    {
        const int N = accurate ? 5000000 : 1000000;
        void **a = zmalloc(sizeof(void*) * N);
        long long t0, t1;

        t0 = snapTestNowNs();
        for (int i = 0; i < N; i++) { void *x = zmalloc(64); zfree(x); }
        t1 = snapTestNowNs();
        double z_pair = (double)(t1 - t0) / N;

        t0 = snapTestNowNs();
        for (int i = 0; i < N; i++) { void *x = snapNoforkAlloc(64); snapNoforkFree(x); }
        t1 = snapTestNowNs();
        double s_pair = (double)(t1 - t0) / N;

        t0 = snapTestNowNs();
        for (int i = 0; i < N; i++) a[i] = zmalloc(64);
        for (int i = 0; i < N; i++) zfree(a[i]);
        t1 = snapTestNowNs();
        double z_bulk = (double)(t1 - t0) / N;

        t0 = snapTestNowNs();
        for (int i = 0; i < N; i++) a[i] = snapNoforkAlloc(64);
        for (int i = 0; i < N; i++) snapNoforkFree(a[i]);
        t1 = snapTestNowNs();
        double s_bulk = (double)(t1 - t0) / N;

        t0 = snapTestNowNs();
        for (int i = 0; i < N; i++) a[i] = snapNoforkAlloc(64);
        for (int i = 0; i < N; i++) snapNoforkFreeSize(a[i], 64);
        t1 = snapTestNowNs();
        double s_bulk_sized = (double)(t1 - t0) / N;

        printf("BENCH alloc+free 64B, N=%d: zmalloc/zfree pair %.1f ns, snapNofork pair %.1f ns; "
               "bulk zmalloc %.1f ns/op, bulk snapNofork %.1f ns/op, bulk snapNofork sized-free %.1f ns/op\n",
               N, z_pair, s_pair, z_bulk, s_bulk, s_bulk_sized);
        zfree(a);
        test_cond("microbenchmark ran", 1);
    }

    /* 7. VMA count proxy: many small arena allocations must not explode the
     * number of mappings (MADV_DONTFORK splits VMAs, but adjacent extents
     * with identical flags merge back). */
    {
        const int N = large ? 10000000 : 1000000;
        int maps_before = snapTestCountMaps();
        void **a = zmalloc(sizeof(void*) * N);
        for (int i = 0; i < N; i++) { a[i] = snapNoforkAlloc(96); ((char*)a[i])[0] = 1; }
        int maps_after = snapTestCountMaps();
        for (int i = 0; i < N; i++) snapNoforkFree(a[i]);
        zfree(a);
        printf("VMA maps: before=%d after %d x 96B SNAP_NOFORK allocations=%d (delta %d)\n",
               maps_before, N, maps_after, maps_after - maps_before);
        test_cond("VMA count stays far below vm.max_map_count default (65530)", maps_after < 10000);
    }

    snapPlacementTests();
    snapScanTests(accurate);
    snapOverlayTests(0);
    snapOverlayTests(1);

    test_report();
    return 0;
}
#endif
