/*
 * hybrid_snapshot.c - Hybrid snapshot: the implementation.
 *
 * The design overview (value classes, one save end to end, the credit window
 * and write sections, memory, entry points) is the comment at the top of
 * hybrid_snapshot.h. This file keeps the implementation details next to
 * the code they describe; each section below opens with its own.
 *
 * Sections, in order:
 *   - fork visibility of the arena;
 *   - classification and placement;
 *   - clone-on-write for the keyspace overlay;
 *   - the scan engine: scan positions, the credit window and write sections,
 *     per-dict save state, save epochs, the thread;
 *   - the save: INFO/DEBUG, bucket position and emit-ahead, stores versus
 *     dbs, the outbound queue to the child;
 *   - the child writer, then the parent orchestration;
 *   - unit tests (redis-server test hybrid_snapshot).
 *
 * Copyright (c) 2026-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of (a) the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#include "server.h"
#include "hybrid_snapshot.h"
#include "kvstore_overlay.h"
#include "cluster.h"
#include "cluster_asm.h"
#include "fmtargs.h"
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include "atomicvar.h"
#include "ae.h"
#include "eventnotifier.h"
#include <pthread.h>
#include <poll.h>

/* ---- Constants -------------------------------------------------------- */

/* Slot (dict) states, monotonic; the save holds the slot until DONE. The
 * order is relied on (st >= HYBSNAP_SLOT_SCANNED: the thread is finished with
 * it); stored in the low byte of the dict's word (HYBSNAP_WORD). */
typedef enum hybsnapSlotState {
    HYBSNAP_SLOT_PENDING  = 0,   /* Not reached by the scan thread yet. */
    HYBSNAP_SLOT_SCANNING = 1,   /* Being read by the scan thread. */
    HYBSNAP_SLOT_SCANNED  = 2,   /* Fully dispatched; waiting for the main thread to complete it. */
    HYBSNAP_SLOT_DONE     = 3,   /* Completed by the main thread: overlay written back, dict unfrozen. */
} hybsnapSlotState;

/* The thread's credit W (buckets per grant); per-dict word: save generation and slot state. */
#define HYBSNAP_WINDOW_BUCKETS_DEFAULT 1024
#define HYBSNAP_WINDOW_BUCKETS_MAX 65536   /* overlay windows are W rounded up to a power of two: an array of that many buckets each */
#ifndef HYBSNAP_SCAN_PREFETCH2
#define HYBSNAP_SCAN_PREFETCH2 8
#endif
#ifndef HYBSNAP_SCAN_PREFETCH
#define HYBSNAP_SCAN_PREFETCH 16   /* buckets ahead of the scan whose first entry is prefetched */
#endif
#define HYBSNAP_WORD(gen, st) (((uint64_t)(gen) << 8) | (uint64_t)(st))
#define HYBSNAP_WORD_GEN(w) ((w) >> 8)
#define HYBSNAP_WORD_STATE(w) ((uint8_t)((w) & 0xff))
/* A (store, dict) pair packed in one atomic. */
#define HYBSNAP_SCAN_ENC(store_idx, didx) (((uint64_t)(uint32_t)(store_idx) << 32) | (uint32_t)(didx))
#define HYBSNAP_SCAN_DEC_STORE(v) ((int)((v) >> 32))
#define HYBSNAP_SCAN_DEC_DIDX(v) ((int)((v) & 0xffffffffu))

/* Messages to the child and the staging buffers. */
#define HYBSNAP_MSG_BUFFER 1     /* pre-encoded records of one (db, slot) */
#define HYBSNAP_MSG_KVOBJ  2     /* whole kvobj allocation copy of a HYBSNAP_FORK key */
#define HYBSNAP_MSG_END    3     /* big channel: robj_off = KVOBJ messages sent, src_addr = small-channel messages sent */
#define HYBSNAP_MSG_ABORT  4
#define HYBSNAP_SMALL_BACKLOG (1024 * 1024)   /* child: take a big value only below this much small backlog in the parent */
#define HYBSNAP_SMALL_WAIT_MS 20              /* child: how long to wait for that backlog to reach the pipe before taking a big value */
#define HYBSNAP_STAGE_BYTES (64 * 1024)   /* staging buffer flush threshold */

/* Where the scan thread is relative to a bucket. */
typedef enum hybsnapBucketPos {
    HYBSNAP_POS_BEHIND = 0,   /* the thread already read this bucket */
    HYBSNAP_POS_INSIDE = 1,   /* the thread is reading it now */
    HYBSNAP_POS_AHEAD  = 2,   /* the thread has not reached it */
} hybsnapBucketPos;

/* Outbound queue drain modes and buffer pool. */
typedef enum hybsnapDrainMode {
    HYBSNAP_DRAIN_TRY   = 0,   /* write what the pipe takes, never wait */
    HYBSNAP_DRAIN_BOUND = 1,   /* wait until oq_bytes <= target; give up on abort */
    HYBSNAP_DRAIN_ALL   = 2,   /* wait until the queue is empty, abort or not */
} hybsnapDrainMode;
#define HYBSNAP_OUT_POOL_MAX 16   /* staging buffers kept for reuse */
#define HYBSNAP_STAGE_SLACK (8 * 1024)   /* headroom a record may take before the flush */

/* ---- Types ------------------------------------------------------------ */

typedef struct hybsnapScanCtx hybsnapScanCtx;
/* Scan thread, once per fork-instant key of a slot; strictly read-only on the keyspace. */
typedef void (hybsnapScanDispatchFn)(void *privdata, int store_idx, int didx, kvobj *kv);
/* Main thread, when a slot is complete, before its dict is unfrozen ('aborted': not fully scanned). */
typedef void (hybsnapScanSlotDoneFn)(void *privdata, int store_idx, int didx, int aborted);
/* Scan thread, after the last slot or on abort, before the scan is published as finished. */
typedef void (hybsnapScanThreadDoneFn)(void *privdata, int aborted);
typedef struct hybsnapSave hybsnapSave;
/* What a hybrid save writes. */
typedef enum hybsnapSaveKind {
    HYBSNAP_SAVE_RDB     = 0,   /* BGSAVE or disk-based full sync: a file (filename, rsi, rdbflags) */
    HYBSNAP_SAVE_SOCKETS = 1,   /* diskless full sync: the replicas' sockets (rsi) */
    HYBSNAP_SAVE_AOF     = 2,   /* BGREWRITEAOF: the AOF base file */
} hybsnapSaveKind;

/* Keyspace placement statistics (DEBUG SNAP-PLACEMENT, SNAP-DRIFT). */
typedef struct hybsnapPlacementStats {
    long long keys;
    long long headers_in_arena;        /* every kvobj header should be here */
    long long headers_outside;
    long long class_nofork;            /* hybsnapKeyClass() == HYBSNAP_NOFORK */
    long long class_fork;
    long long drift_nofork_outside;    /* policy says NOFORK, value outside the arena */
    long long drift_fork_inside;       /* policy says FORK (single-alloc, too big), value inside */
    long long violations;              /* header outside, or multi-allocation value inside */
    long long dict_tables_in_arena;    /* dict structs + bucket arrays of db->keys / db->expires */
    long long dict_tables_outside;
} hybsnapPlacementStats;

/* The privdata of every kvstore hook on store s (guard, layout and overlay
 * callbacks alike, one convention): the scan and the store index, plus what
 * the overlay's user handed to hybsnapScanSetOverlay for that store. The redisDb
 * owning a store is resolved at call time (SWAPDB moves a store to another db
 * id; a FLUSHDB leaves it with no owner), the db id in the dump is always s. */
struct hybsnapGuardPriv { struct hybsnapScanCtx *ctx; int s; void *user; };

/* What the scan keeps per store (one per db: db->keys). */
typedef struct hybsnapScanStore {
    kvstore *kvs;
    dict **dicts;                 /* the fork-instant dicts by slot, one reference each; NULL once completed (allocated at freeze) */
    long nrefs;                   /* dicts not completed yet; the guard comes off at zero. Main thread. */
    int num_dicts;                /* kvstoreNumDicts() */
    int ord_base;                 /* the ordinal of its dict 0 in the scan positions */
    int frozen;                   /* 1 while our guard is installed (hybsnapScanFreeze() until its last dict completes) */
    struct hybsnapGuardPriv guard;   /* the privdata of every hook on the store */
} hybsnapScanStore;

/* What the save keeps per dict in the kvstore extension slot, allocated the
 * first time the save touches the dict (a write before the scan thread
 * reached it, a pause before hi may pass its layout, or any access while the
 * thread is on it) and freed when the dict's scan
 * completes. Meaningful only while the dict's word carries this save's
 * generation. The overlay lives next to it (kvstoreDictMetaBase.ov). */
typedef struct hybsnapDictState {
    int paused;                 /* rehash and auto-resize paused: hi may pass the layout (positions are stable) */
} hybsnapDictState;


/* The scan thread's chain-walk callback arguments. */
struct hybsnapScanEntryArgs { hybsnapScanCtx *ctx; int s; int didx; };


struct hybsnapScanCtx {
    hybsnapScanStore *store;           /* num_stores entries, one per db. */
    int num_stores;
    long retained_dicts;            /* Table dicts their store let go of before the save completed them (freed here). Main thread. */
    redisAtomic int64_t cur_slot;   /* The slot the thread is scanning (claimed, not yet SCANNED), HYBSNAP_SCAN_ENC, -1 none: the abort sweep completes it. */
    int ended;                      /* Main thread: every store completed; the save holds no slot any more. */
    uint64_t gen;                   /* this save's generation (never 0): stamps every dict it touches (kvstoreDictMetaBase.ext_word) */
    uint32_t epoch;                 /* this save's epoch (set at freeze): a key whose epoch is at least this was serialized
                                       by the main thread or born after the fork instant, and the thread skips it */
    int skip_stamped;               /* the keys are kvobjs and the thread skips the stamped ones (set by the owner before
                                       start; the server keyspace; the unit tests' plain string keys have no epoch) */
    const uint8_t *slot_filter;     /* cluster mode, optional: freeze and scan only the slots whose bit is set (a slot snapshot) */

    hybsnapScanDispatchFn *dispatch;
    hybsnapScanSlotDoneFn *slot_done;
    void *privdata;

    /* SPSC completion ring, capacity == slots_total (allocated at freeze). */
    uint64_t *ring;
    redisAtomic long ring_wr;       /* Written by the thread. */
    long ring_rd;                   /* Main thread only. */
    eventNotifier *en;

    pthread_t tid;
    int started;
    int joined;
    redisAtomic int abort;          /* Main -> thread. */
    redisAtomic int finished;       /* Thread -> main. */

    long slots_total;               /* fork-instant dicts (non-empty at freeze) in the scanned stores */
    redisAtomic long slots_scanned; /* dicts the thread passed */
    long slots_done;                /* claimed dicts completed. Main thread only. */
    redisAtomic long long keys_dispatched;

    /* Keyspace overlay (optional). */
    kvstoreSnapHooks hooks;         /* installed on every store: the scan's guard and layout hooks plus the user's overlay callbacks */
    int has_ov;
    void **ov_priv;                 /* Per store. */
    int frozen_all;                 /* hybsnapScanFreeze() done. */
    unsigned long window_buckets;   /* copied from hybsnap_window_buckets at create: the thread's credit W */
    unsigned long ov_block;         /* overlay windows: aligned runs of this many buckets (W rounded up to a power of two) */
    uint64_t pos_end;               /* the position past every dict: the thread's lo and cur at the end */
    uint64_t hi;                    /* main thread: its own last store of hybsnap_hi */
    uint64_t mid;                   /* main thread: the next grant is due once lo reaches this */
    long long grants;               /* main thread: hi raised inside a section */
    redisAtomic long long stalls;   /* thread: times it waited for hi */
    redisAtomic long long stall_us; /* thread: time spent waiting */
    redisAtomic long long keys_stamped;      /* keys the thread skipped (epoch: already emitted, or newer than the fork instant) */
    /* Optional hooks, set by the owner before start. */
    hybsnapScanThreadDoneFn *thread_done;   /* scan thread, after the last slot or on abort, before the scan is published as finished */
    void (*thread_init)(void *privdata);   /* first thing on the scan thread */
    void (*window_done)(void *privdata);   /* after every window, and before thread_done */
    void (*store_done)(void *privdata, int s);   /* main thread, when a store's last slot completed */
};


typedef struct hybsnapMsgHdr {
    uint32_t type;
    int32_t dbid;
    int32_t slot;
    uint32_t len;        /* payload bytes */
    uint32_t robj_off;   /* KVOBJ: offset of the robj inside the copied allocation.
                          * BUFFER: dependency, the number of big values (KVOBJ messages) that
                          * must precede this buffer in the output (0 = none; reserved for inline
                          * replication, whose buffers carry commands on big keys) */
    uint64_t src_addr;   /* KVOBJ: parent address of the copied allocation, to relocate
                          * pointers into it (an EMBSTR value points inside its kvobj) */
} hybsnapMsgHdr;

/* records the key and expire counts at the fork instant. RDB stream must announce 
 * those counts before the keys they describe, and by then the live keyspace has changed. */
typedef struct hybsnapDbSizes {
    unsigned long long keys, expires;
    unsigned long long *slot_keys, *slot_expires;   /* cluster mode */
    int nslots;
} hybsnapDbSizes;

/* A db flushed during the save: its fork-instant structures, kept alive for
 * the scan and freed (on BIO, as FLUSHDB ASYNC does) when the store completes. */
typedef struct hybsnapFlushedDb {
    int set;
    kvstore *keys, *expires, *blessed;
    void *subexpires;
    dict *stream_idmp_keys;
} hybsnapFlushedDb;

/* One page shared with the child (MAP_SHARED, mapped before the fork). */
typedef struct hybsnapShared {
    redisAtomic size_t small_queued;   /* small-channel bytes queued in the parent, for the child's choice of channel */
} hybsnapShared;

struct hybsnapSave {
    hybsnapScanCtx *scan;
    void *tables;              /* one block: stores, flushed and sizes below, each dbnum entries */
    void *bitmaps;             /* cluster mode, one block: slot_filter then trim_slots, CLUSTER_SLOTS bits each; NULL when neither is used */
    kvstore **stores;          /* per store (db id at the fork instant): the kvstore being scanned */
    hybsnapFlushedDb *flushed;    /* per store */
    int dbnum;
    /* Two channels to the child. The small channel carries the
     * BUFFER messages, the big channel KVOBJ, END and
     * ABORT, so the child can drain the small backlog first and take a big
     * value only when that backlog is low: a bounded queue then never parks
     * the scan thread while the child is inside a big value. */
    int pipe_rd, pipe_wr;             /* small channel */
    int big_rd, big_wr;               /* big channel */
    hybsnapShared *shared;
    int tag_all;                      /* debug (hybrid-snapshot-tag-all): every BUFFER depends on all big values sent before it */
    hybsnapDbSizes *sizes;
    int req, rdbflags;
    hybsnapSaveKind kind;
    int aof_cmd_form;          /* CHILD_TYPE_AOF without the RDB preamble: records are AOF commands, not RDB pairs */
    int slots_form;            /* atomic slot migration's slot snapshot (SLAVE_REQ_SLOTS_SNAPSHOT): records are
                                  RESTORE commands from DUMP payloads (cluster_asm.c), the stream is RESP, no RDB framing */
    uint8_t *slot_filter;      /* slots_form: the migrating slots at the fork instant (CLUSTER_SLOTS bits); only they are frozen and scanned */
    char *filename;
    rdbSaveInfo rsi;
    int has_rsi;
    int ae_registered;
    int aborted;               /* hybsnapSaveAbort() ran; waiting for the child to exit */
    int hard_mark_hit;         /* retained bytes passed the configured maximum */
    int failed;                /* a defined failure of ours (hard mark): report status err */
    size_t retained_bytes;     /* originals kept alive by the overlay (main thread) */
    size_t retained_peak;
    redisAtomic int key_save_delay;   /* rdb-key-save-delay, copied for the scan thread */
    int maxmemory_policy, rdb_compression;   /* config frozen at start for the scan thread */
    long long inplace_writes;  /* writes served in place on scanned keys (main thread) */
    /* Scan-thread staging buffer. */
    sds stage;
    int stage_dbid, stage_slot;
    redisAtomic long long keys_nofork, keys_fork, bytes_sent;
    /* Cluster mode: the slots an atomic slot migration was trimming at the
     * fork instant, one bit each (NULL when none). Their keys are leaving the
     * node: the classic child skips them, reading the fork-instant copy of the
     * trim jobs, and so do the two producers here. A trim that starts during
     * the save is not in the set, as in the classic save. */
    uint8_t *trim_slots;
    redisAtomic long long keys_skipped;
    /* Main-thread emission (emit-ahead): records the main thread serialized
     * before writing a bucket the thread had not reached. mq_lock guards
     * mq (ready messages) and mstage (the partial BUFFER of one (db, slot));
     * the scan thread takes both at every window boundary and forwards them. */
    pthread_mutex_t mq_lock;
    list *mq;                           /* complete messages: sds, header followed by payload, as on the pipe */
    list *mq_ready;                     /* scan thread: the batch swapped out of mq, being forwarded */
    sds mstage;                         /* a message in the making: header space reserved, records appended */
    int mstage_dbid, mstage_slot;
    sds mscratch;                       /* main thread only: one record at a time */
    redisAtomic size_t mq_bytes;        /* allocation bytes of the messages in mq, not yet forwarded */
    long long epoch_reallocs;           /* main thread only: keys given an epoch in a new allocation (no room in theirs) */
    redisAtomic long long main_keys;    /* keys the main thread emitted ahead (read by the scan thread: their save delay is due on it) */
    long long delayed_main_keys;        /* scan thread only: main-emitted keys it has slept the key-save delay for */
    /* Outbound queue: every message the scan thread produces or
     * forwards waits here for the pipe, which is written non-blockingly, so a
     * child busy inside one huge fork-class value does not stall the scan.
     * Owned by the scan thread; oq_bytes and queued_peak are read by INFO. */
    list *oq;                           /* small channel: sds, header followed by payload */
    list *bq;                           /* big channel */
    list *oq_pool;                      /* drained message buffers kept for reuse (scan thread) */
    size_t oq_head_off, bq_head_off;    /* bytes of each head message already written */
    size_t oq_limit;                    /* hybrid-snapshot-max-queued-bytes at save start, 0 = none */
    redisAtomic size_t oq_bytes;        /* allocation bytes of the messages queued for both pipes */
    size_t sq_bytes;                    /* of which the small channel's (scan thread; published to the child) */
    uint32_t kv_seq;                    /* KVOBJ messages sent so far (scan thread) */
    uint64_t small_seq;                 /* small-channel messages sent so far (scan thread) */
    redisAtomic size_t queued_peak;     /* max of oq_bytes + mq_bytes seen by the scan thread */
    /* Output target. A BGSAVE or disk-based sync writes a temp
     * file renamed at the end. A diskless sync writes the replicas' sockets:
     * the child owns them, either directly (rdb-channel connections, blocking)
     * or through the classic rdb pipe that the parent fans out non-blockingly. */
    int rdb_channel;
    connection **conns;        /* rdb-channel: the replica connections the child writes */
    int numconns;
    int rdb_pipe_write;        /* pipe shape: child's end of server.rdb_pipe_read */
    int safe_to_exit_pipe;     /* pipe shape: the child waits on it before exiting */
};
static hybsnapSave *hybsnap_save = NULL;   /* the running hybrid save, main thread */


#define hybsnapIsAof(sv) ((sv)->kind == HYBSNAP_SAVE_AOF)
#define hybsnapToSockets(sv) ((sv)->kind == HYBSNAP_SAVE_SOCKETS)

/* ---- Function declarations (used only in this file) ------------------- */

/* ---- hybrid arena ------------------------------------------------ */

/* Address range reserved for the arena, and the part handed to jemalloc so far (0 if unavailable). */
static size_t hybsnapArenaVaReserved(void);
static size_t hybsnapArenaVaUsed(void);
static int hybsnapArenaSetForkable(int forkable);

/* ---- Classification and placement ------------------------------------- */

/* Single-allocation encoding that MAY be HYBSNAP_NOFORK (strings, listpacks, intsets). */
static int hybsnapEncodingIsNoforkClass(int type, int encoding);
/* Placement policy: the class a value of this encoding and size should have. */
static hybsnapValueClass hybsnapClassify(int type, int encoding, size_t alloc_bytes);
/* Actual class of a stored value, decided by where its memory is. */
static hybsnapValueClass hybsnapKeyClass(const robj *o);
static void hybsnapPlacementScan(redisDb *db, hybsnapPlacementStats *st);
static void hybsnapPlacementDriftReply(struct client *c, redisDb *db, int limit);

/* ---- Scan engine ------------------------------------------------------ */

/* Create a scan over 'num_stores' kvstores; nothing is frozen or started yet. */
static hybsnapScanCtx *hybsnapScanCreate(kvstore **stores, int num_stores, hybsnapScanDispatchFn *dispatch,
                            hybsnapScanSlotDoneFn *slot_done, void *privdata);
/* Before start: the keyspace overlay callbacks (clone, free_orig, enter, prepare). */
static void hybsnapScanSetOverlay(hybsnapScanCtx *ctx, const kvstoreSnapHooks *hooks, void **store_privdata);
/* Before start, cluster mode: freeze and scan only the slots whose bit is set (CLUSTER_SLOTS bits, must outlive the scan). */
static void hybsnapScanSetSlotFilter(hybsnapScanCtx *ctx, const uint8_t *bitmap);
/* Main thread: freeze every store (the fork instant), then, after the fork, spawn the thread; C_OK / C_ERR. */
static void hybsnapScanFreeze(hybsnapScanCtx *ctx);
static int hybsnapScanStartThread(hybsnapScanCtx *ctx);
/* Main thread: complete the slots the thread finished since the last call; returns how many. */
static int hybsnapScanHandleCompletions(hybsnapScanCtx *ctx);
/* Readable when a slot completes or the thread exits (-1 if none); register the handler on it with ctx as client data. */
static int hybsnapScanReadEventFd(hybsnapScanCtx *ctx);
static void hybsnapScanAeHandler(struct aeEventLoop *el, int fd, void *privdata, int mask);
/* 1 once the thread has exited (all slots scanned, or aborted). */
static int hybsnapScanIsFinished(hybsnapScanCtx *ctx);
/* Main thread: stop and join the thread, complete every slot. */
static void hybsnapScanAbort(hybsnapScanCtx *ctx);
/* Main thread: join if needed and free the ctx; every slot must be DONE. */
static void hybsnapScanRelease(hybsnapScanCtx *ctx);
/* The scan was aborted; complete every slot of every store (main thread). */
static inline int hybsnapScanAborted(hybsnapScanCtx *ctx);
static void hybsnapScanFinishAll(hybsnapScanCtx *ctx);
/* Progress counters for INFO and tests. */
static void hybsnapScanProgress(hybsnapScanCtx *ctx, long *slots_total, long *slots_scanned,
                      long *slots_done, long long *keys_dispatched);

/* ---- Credit window and write sections --------------------------------- */

/* Every guard or funnel hook (main thread, or a module thread holding the GIL): the first opens the section and caps the thread, later ones may raise the cap. */
static void hybsnapSectionOpen(void);
/* The thread's credit W, buckets per grant (DEBUG SNAP-WINDOW-BUCKETS; tests). */
static void hybsnapSetWindowBuckets(unsigned long n);
static unsigned long hybsnapGetWindowBuckets(void);

/* ---- Save epochs ------------------------------------------------------- */

/* Begin and end a scan's epoch (freeze, release); give a frozen key the save's epoch before an in-place write. */
static void hybsnapEpochBegin(hybsnapScanCtx *ctx);
static void hybsnapEpochEnd(void);
static void hybsnapEpochModeUpdate(void);
static kvobj *hybsnapKvobjStamp(hybsnapSave *sv, int s, int didx, dict *d, dictEntryLink link);
static void hybsnapRetiredFree(void);

/* ---- Keyspace overlay ------------------------------------------------- */

/* Overlay callbacks (privdata = the store's hybsnapGuardPriv): clone a kvobj into the arena, release a fork-instant original. */
static void *hybsnapOverlayCloneKey(void *privdata, int didx, void *orig);
static void hybsnapOverlayFreeOriginal(void *privdata, int didx, void *orig);
/* Does the clone of an original take over its value (1) or copy it (0)? Pure: the unchanged header and the value's address. */
static int hybsnapOriginalSharesValue(const kvobj *orig);
/* Retained-originals accounting, the db that owns a store, the write-back of overlayed buckets. */
static void hybsnapSaveRetainedAdd(kvobj *orig, int value_shared);
static void hybsnapSaveRetainedSub(kvobj *orig, int value_shared);
static redisDb *hybsnapStoreOwnerDb(hybsnapSave *sv, int s);
static void hybsnapSectionDrain(void);
/* A replacement kvobj (clone, copy with an epoch) takes over db->expires, db->subexpires and the slot accounting. */
static void hybsnapKeyRepoint(redisDb *db, int didx, kvobj *from, kvobj *to);

/* ---- Messages to the child -------------------------------------------- */

static int hybsnapSendMsg(hybsnapSave *sv, uint32_t type, int dbid, int slot,
                       const void *payload, uint32_t len, uint32_t robj_off, uint64_t src_addr);
static void hybsnapStageFlush(hybsnapSave *sv);
static int hybsnapOutEnqueue(hybsnapSave *sv, sds m, uint32_t type);
static void hybsnapOutRecycle(hybsnapSave *sv, sds m);

/* ---- Observability ---------------------------------------------------- */

static size_t hybsnapScanTableBytes(hybsnapScanCtx *ctx);
/* The running save: slots and keys. */
static void hybsnapSaveProgress(long *slots_total, long *slots_done, long long *keys_nofork,
                      long long *keys_fork, long long *bytes_sent);
/* Retained originals (replaced or deleted) and overlayed buckets; the peak over the last save. */
static size_t hybsnapSaveRetainedBytes(void);
static long hybsnapSaveSideEntries(void);
static size_t hybsnapSaveLastRetainedPeak(void);
/* Bytes queued for the child (main-thread queue plus the thread's outbound queue); the peak over the last save. */
static size_t hybsnapSaveQueuedBytes(void);
static size_t hybsnapSaveLastQueuedPeak(void);
/* Fork-instant dicts their store let go of (cumulative). */
static long hybsnapSaveRetainedDicts(void);
/* Main-thread work: keys emitted ahead of the window, of which given an epoch in a new allocation; keys the thread
 * skipped for their epoch; in-place writes behind it (this save, last save). */
static long long hybsnapSaveMainKeys(void);
static long long hybsnapSaveEpochReallocs(void);
static long long hybsnapSaveKeysStamped(void);
static long long hybsnapSaveInplaceWrites(void);
static long long hybsnapSaveLastInplaceWrites(void);

/* ---- Defined elsewhere ------------------------------------------------ */

void updateSlotAllocSize(redisDb *db, int didx, kvobj *kv, int64_t oldsize, int64_t newsize);   /* db.c */

/* ----------------------------------------------------------------------- */

/* The arena itself (reserved DONTFORK address range, extent hooks, per-thread
 * tcache flags, range-aware zfree/zrealloc) lives in zmalloc.c so that sds,
 * listpack, intset and dict code keep arena memory in the arena without
 * knowing about classes. These are the snapshot-facing entry points. */
void hybsnapArenaInit(void) {
    hybsnapEpochModeUpdate();
#ifdef HAVE_HYBRID_ARENA
    if (!zmalloc_hybrid_arena_init()) {
        serverLog(LL_WARNING, "Hybrid arena disabled: could not reserve a DONTFORK address "
                              "range or create the jemalloc arena; hybrid snapshot runs without it.");
        return;
    }
    zmalloc_hybrid_arena_set_active(server.hybrid_snapshot);       /* new headers, tables and values go there only when on */
    if (!server.hybrid_snapshot) hybsnapArenaSetForkable(1);   /* nothing is placed in it; classic forks map it */
    serverLog(LL_NOTICE, "Hybrid arena created (jemalloc arena %u, %zu GB of address space reserved, "
                         "%s).", zmalloc_hybrid_arena_index(), hybsnapArenaVaReserved() >> 30,
                         server.hybrid_snapshot ? "MADV_DONTFORK: hybrid snapshot on" : "forkable: hybrid-snapshot no");
#else
    serverLog(LL_NOTICE, "Hybrid arena unavailable on this platform/allocator (needs 64-bit Linux "
                         "with jemalloc); hybrid snapshot, if enabled, runs without it.");
#endif
}

/* ---- Fork visibility of the arena (see hybrid_snapshot.h) ---------------------- */

static int hybsnap_arena_fallback = 0;        /* flipped forkable for a non-hybrid child, flip back on reap */
static int hybsnap_child_no_keyspace = 0;     /* Is this process is a child forked without hybrid arena */
int hybsnapIsChildNoKeyspace(void) { return hybsnap_child_no_keyspace; }

/* Main thread: a hybrid save was requested but a loaded module has not
 * declared REDISMODULE_OPTIONS_HANDLE_HYBRID_SNAPSHOT; the classic child runs
 * instead. Logged when the blocking module changes, not on every save. */
static void hybsnapFallbackLog(void) {
    static sds last = NULL;
    const char *who = moduleHybridSnapshotBlockedBy();
    if (last && !strcmp(last, who)) return;
    sdsfree(last);
    last = sdsnew(who);
    serverLog(LL_WARNING, "Hybrid snapshot: falling back to the fork child, module '%s' has not declared "
                          "REDISMODULE_OPTIONS_HANDLE_HYBRID_SNAPSHOT", who);
}

/* Fork visibility of the arena. It is DONTFORK, so a child forked without the
 * keyspace (redisFork() with REDIS_FORK_NO_KEYSPACE) never maps it. Any other
 * fork (classic save, diskless sync, AOF rewrite, module, Lua debugger) needs
 * the keyspace: redisFork() makes the arena DOFORK (hybsnapArenaBeforeFork) and
 * the reap path restores DONTFORK once no child is left
 * (hybsnapArenaChildExited). One madvise each way; that child pays classic
 * copy-on-write.
 *
 * Returns 1 when the arena exists and is DOFORK, else 0. */
int hybsnapArenaIsForkable(void) {
#ifdef HAVE_HYBRID_ARENA
    return hybsnapArenaExists() && zmalloc_hybrid_arena_is_forkable();
#else
    return 0;
#endif
}

static int hybsnapArenaSetForkable(int forkable) {
#ifdef HAVE_HYBRID_ARENA
    if (!hybsnapArenaExists()) return 0;
    if (!zmalloc_hybrid_arena_set_forkable(forkable)) {
        serverLog(LL_WARNING, "Hybrid arena: madvise(%s) failed: %s",
                  forkable ? "MADV_DOFORK" : "MADV_DONTFORK", strerror(errno));
        return 0;
    }
    return 1;
#else
    UNUSED(forkable);
    return 0;
#endif
}

void hybsnapArenaBeforeFork(int purpose, int flags) {
    if ((flags & REDIS_FORK_NO_KEYSPACE) || hybsnapForkSeesKeyspace()) return;

    /* A classic child (RDB with the feature off, diskless sync, AOF rewrite,
     * module or Lua debugger fork) walks the keyspace: it must map the arena. */
    if (hybsnapArenaSetForkable(1)) {
        hybsnap_arena_fallback = 1;
        serverLog(LL_NOTICE, "Hybrid arena made forkable for a %s child (classic copy-on-write applies)",
                  strChildType(purpose));
    }
}

void hybsnapArenaChildExited(void) {
    if (!hybsnap_arena_fallback) return;
    if (hasActiveChildProcess() || ldbPendingChildren()) return;   /* another classic child still runs */
    hybsnap_arena_fallback = 0;
    if (server.hybrid_snapshot && hybsnapArenaSetForkable(0))
        serverLog(LL_NOTICE, "Hybrid arena back to MADV_DONTFORK");
}

/* redisFork(), child side, first thing in a child forked without the keyspace
 * (REDIS_FORK_NO_KEYSPACE: the hybrid save child, a REDISMODULE_FORK_NO_KEYSPACE
 * module fork). The keyspace module APIs are refused from here on
 * (hybsnapIsChildNoKeyspace), feature on or off.
 *
 * The DONTFORK arena is not mapped here, so the kernel could hand its
 * addresses to our own heap; zmalloc routes frees and reallocs by address
 * (zmalloc_in_hybrid_arena) and would treat such a block as an arena block.
 * Reserve the range PROT_NONE. A forkable arena (feature off, a debugger
 * child) is mapped: nothing to reserve. */
void hybsnapChildNoKeyspaceInit(void) {
    hybsnap_child_no_keyspace = 1;
#ifdef HAVE_HYBRID_ARENA
    void *lo; size_t len;
    if (hybsnapForkSeesKeyspace() || !zmalloc_hybrid_arena_range(&lo, &len)) return;
    int flags = MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE;
#ifdef MAP_FIXED_NOREPLACE
    flags |= MAP_FIXED_NOREPLACE;
#else
    flags |= MAP_FIXED;
#endif
    void *p = mmap(lo, len, PROT_NONE, flags, -1, 0);
    if (p == MAP_FAILED)
        serverLog(LL_WARNING, "Child without keyspace: cannot re-reserve the arena range (%s); allocations may collide with it", strerror(errno));
#endif
}

/* CONFIG SET hybrid-snapshot apply hook: flips the arena, refuses mid-save.
 * Saves take the hybrid path whether or not the hybrid arena exists
 * (without it the CoW benefit is gone but the mechanics are identical, which
 * is what sanitizer builds exercise). Switching is O(1): the arena is flipped
 * forkable when off, and values allocated meanwhile simply live in the
 * default allocator. */
int hybsnapConfigApply(const char **err) {
    if (server.loading) {   /* applied again by the startup path */
        hybsnapEpochModeUpdate();
        return 1;
    }
    if (hasActiveChildProcess() || hybsnapSaveIsInProgress() || ldbPendingChildren()) {
        *err = "hybrid-snapshot cannot change while a save, rewrite, module fork or debugger child is running";
        return 0;
    }
    if (hybsnapArenaExists()) {
        /* On: children stop mapping the arena; off: they must map it. Values
         * placed so far stay where they are, the switch itself is O(1). */
        if (!hybsnapArenaSetForkable(!server.hybrid_snapshot)) {
            *err = "Hybrid arena: madvise failed, see the server log";
            return 0;
        }
        zmalloc_hybrid_arena_set_active(server.hybrid_snapshot);
        hybsnap_arena_fallback = 0;
    }
    hybsnapEpochModeUpdate();
    serverLog(LL_NOTICE, "Hybrid snapshot switched %s at runtime%s", server.hybrid_snapshot ? "on" : "off",
              hybsnapArenaExists() ? "" : " (no hybrid arena on this platform)");
    return 1;
}


static size_t hybsnapArenaVaReserved(void) {
#ifdef HAVE_HYBRID_ARENA
    size_t reserved = 0;
    if (hybsnapArenaExists()) zmalloc_hybrid_arena_va_stats(&reserved, NULL);
    return reserved;
#else
    return 0;
#endif
}

static size_t hybsnapArenaVaUsed(void) {
#ifdef HAVE_HYBRID_ARENA
    size_t used = 0;
    if (hybsnapArenaExists()) zmalloc_hybrid_arena_va_stats(NULL, &used);
    return used;
#else
    return 0;
#endif
}


/* ============================================================================
 * Classification and placement
 * ============================================================================ */

/* 1 if a value with this type/encoding is a single allocation that MAY be
 * HYBSNAP_NOFORK (strings; listpack list/set/zset/hash; intset; listpack-ex).
 * Multi-allocation encodings (hashtable, skiplist, quicklist, stream, module,
 * arrays) are always HYBSNAP_FORK. */
static int hybsnapEncodingIsNoforkClass(int type, int encoding) {
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
static inline int hybsnapValueIsInline(int type, int encoding) {
    return type == OBJ_STRING && (encoding == OBJ_ENCODING_INT || encoding == OBJ_ENCODING_EMBSTR);
}

/* Placement policy: HYBSNAP_NOFORK iff the encoding qualifies and the value's
 * allocation is at most save-small-max-bytes (INT/EMBSTR values have no
 * separate allocation and are always HYBSNAP_NOFORK). */
static hybsnapValueClass hybsnapClassify(int type, int encoding, size_t alloc_bytes) {
    if (!hybsnapEncodingIsNoforkClass(type, encoding)) return HYBSNAP_FORK;
    if (hybsnapValueIsInline(type, encoding)) return HYBSNAP_NOFORK;
    return alloc_bytes <= server.save_small_max_bytes ? HYBSNAP_NOFORK : HYBSNAP_FORK;
}

/* The single value allocation of a single-allocation value (the sds
 * allocation start for raw strings), or NULL when the value is inline. */
static void *hybsnapValueAllocPtr(const robj *o) {
    switch (o->encoding) {
    case OBJ_ENCODING_RAW: return sdsAllocPtr(o->ptr);
    case OBJ_ENCODING_LISTPACK:
    case OBJ_ENCODING_INTSET:
    case OBJ_ENCODING_LISTPACK_EX: return o->ptr;
    default: return NULL;
    }
}


/* Actual class of a stored key, decided by where its value memory IS: a key is
 * HYBSNAP_NOFORK when its value has no separate allocation or when its single
 * allocation (for listpack-ex: both the struct and its listpack) lies in the
 * hybrid arena; otherwise HYBSNAP_FORK. The background scan serializes
 * HYBSNAP_NOFORK keys itself and ships HYBSNAP_FORK keys to the fork child, so this
 * is correct regardless of policy drift. */
static hybsnapValueClass hybsnapKeyClass(const robj *o) {
    if (!hybsnapEncodingIsNoforkClass(o->type, o->encoding)) return HYBSNAP_FORK;
    if (hybsnapValueIsInline(o->type, o->encoding)) return HYBSNAP_NOFORK;
    /* No arena: nothing is placed, the class is a pure size decision. */
    if (!hybsnapArenaExists()) {
        void *a = hybsnapValueAllocPtr(o);
        return hybsnapClassify(o->type, o->encoding, a ? zmalloc_size(a) : 0);
    }
    /* LISTPACK_EX: the class follows the struct. The struct holds the
     * ExpireMeta registered in the db's ebuckets, so it is never moved; when
     * it is in the arena the child cannot read it and the key MUST be served
     * by the background thread, wherever the listpack itself lives. */
    /* The arena is a range: the value pointer is as good as its allocation
     * start, and is never dereferenced (hybsnapOriginalSharesValue relies on it). */
    if (o->ptr == NULL || !zmalloc_in_hybrid_arena(o->ptr)) return HYBSNAP_FORK;
    return HYBSNAP_NOFORK;
}

/* Copy an sds into the requested arena. */
static sds hybsnapSdsCopy(sds s, int to_arena) {
    size_t len = sdslen(s);
    char type = sdsReqType(len);
    size_t need = sdsReqSize(len, type);
    size_t usable = 0;
    char *buf = to_arena ? zmalloc_hybrid_usable(need, &usable) : zmalloc_usable(need, &usable);
    return sdsnewplacement(buf, usable, type, s, len);
}

/* Copy an sds into the requested arena and free the original. */
static sds hybsnapSdsMove(sds s, int to_arena) {
    sds ns = hybsnapSdsCopy(s, to_arena);
    sdsfree(s); /* range-aware */
    return ns;
}

/* Copy 'bytes' bytes of a blob (listpack, intset, struct) into the requested arena. */
static void *hybsnapBlobCopy(void *p, size_t bytes, int to_arena) {
    void *n = to_arena ? zmalloc_hybrid(bytes) : zmalloc(bytes);
    memcpy(n, p, bytes);
    return n;
}

/* Same, freeing the original. */
static void *hybsnapBlobMove(void *p, size_t bytes, int to_arena) {
    void *n = hybsnapBlobCopy(p, bytes, to_arena);
    zfree(p); /* range-aware */
    return n;
}

/* Apply the placement policy to a value: move a single-allocation value into
 * the arena when hybsnapClassify() says HYBSNAP_NOFORK and it is outside, or out of
 * the arena when it says HYBSNAP_FORK and it is inside. Called (through the
 * inline hybsnapPlaceValue) at every keyspace insertion (kvobjSet, dbSetValue),
 * after in-place growth of a string and after an encoding conversion produced
 * a single-allocation value. Only the value moves; the header (and thus every
 * pointer to it) stays. */
void hybsnapPlaceValueArena(robj *o) {
    if (!hybsnapEncodingIsNoforkClass(o->type, o->encoding)) return;
    /* nothing to do if inline. kvobj already in no_fork arena */
    if (hybsnapValueIsInline(o->type, o->encoding)) return;
    /* Module key metadata is serialized through module callbacks, which only
     * run in the fork child: such a key is HYBSNAP_FORK whatever its size, so
     * every piece of its value must be readable by the child. */
    int force_fork = o->iskvobj && getModuleMetaBits(((kvobj *)o)->metabits);

    switch (o->encoding) {
    case OBJ_ENCODING_RAW: {
        sds s = o->ptr;
        void *a = sdsAllocPtr(s);
        hybsnapValueClass want = force_fork ? HYBSNAP_FORK : hybsnapClassify(o->type, o->encoding, zmalloc_size(a));
        int in = zmalloc_in_hybrid_arena(a);
        if (want == HYBSNAP_NOFORK && !in) o->ptr = hybsnapSdsMove(s, 1);
        else if (want == HYBSNAP_FORK && in) o->ptr = hybsnapSdsMove(s, 0);
        break;
    }
    case OBJ_ENCODING_LISTPACK: {
        unsigned char *lp = o->ptr;
        hybsnapValueClass want = force_fork ? HYBSNAP_FORK : hybsnapClassify(o->type, o->encoding, zmalloc_size(lp));
        int in = zmalloc_in_hybrid_arena(lp);
        if (want == HYBSNAP_NOFORK && !in) o->ptr = hybsnapBlobMove(lp, lpBytes(lp), 1);
        else if (want == HYBSNAP_FORK && in) o->ptr = hybsnapBlobMove(lp, lpBytes(lp), 0);
        break;
    }
    case OBJ_ENCODING_INTSET: {
        intset *is = o->ptr;
        hybsnapValueClass want = force_fork ? HYBSNAP_FORK : hybsnapClassify(o->type, o->encoding, zmalloc_size(is));
        int in = zmalloc_in_hybrid_arena(is);
        if (want == HYBSNAP_NOFORK && !in) o->ptr = hybsnapBlobMove(is, intsetBlobLen(is), 1);
        else if (want == HYBSNAP_FORK && in) o->ptr = hybsnapBlobMove(is, intsetBlobLen(is), 0);
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
        int lp_in = zmalloc_in_hybrid_arena(lpt->lp);
        int st_in = zmalloc_in_hybrid_arena(lpt);
        if (force_fork) {
            /* Module metadata: both pieces must leave the arena. Moving the
             * struct is only legal while its ExpireMeta is not registered in
             * the db's ebuckets, which holds at every placement call site
             * (kvobjSet insertion funnel, dbSetValue swap, metadata attach:
             * the db unregisters before and re-registers after). */
            if (st_in) {
                lpt = hybsnapBlobMove(lpt, sizeof(*lpt), 0);
                o->ptr = lpt;
            }
            if (zmalloc_in_hybrid_arena(lpt->lp)) lpt->lp = hybsnapBlobMove(lpt->lp, lpBytes(lpt->lp), 0);
            break;
        }
        if (st_in && !lp_in) lpt->lp = hybsnapBlobMove(lpt->lp, lpBytes(lpt->lp), 1);
        else if (!st_in && lp_in) lpt->lp = hybsnapBlobMove(lpt->lp, lpBytes(lpt->lp), 0);
        break;
    }
    default:
        break;
    }
}

static void hybsnapPlacementScanTables(kvstore *kvs, hybsnapPlacementStats *st) {
    for (int didx = 0; didx < kvstoreNumDicts(kvs); didx++) {
        dict *d = kvstoreGetDict(kvs, didx);
        if (d == NULL) continue;
        if (zmalloc_in_hybrid_arena(d)) st->dict_tables_in_arena++; else st->dict_tables_outside++;
        for (int t = 0; t < 2; t++) {
            if (d->ht_table[t] == NULL) continue;
            if (zmalloc_in_hybrid_arena(d->ht_table[t])) st->dict_tables_in_arena++; else st->dict_tables_outside++;
        }
    }
}

/* ---- Clone-on-write for the keyspace overlay ---------------------------- */

/* Duplicate the whole kvobj allocation (metadata slots, robj, embedded key
 * and, for EMBSTR, the embedded value) into a fresh HYBSNAP_NOFORK block. The
 * value pointer is left as-is (the caller decides copy vs share). */
static kvobj *hybsnapKvobjCloneHeader(kvobj *orig) {
    char *alloc = kvobjGetAllocPtr(orig);
    size_t size = zmalloc_size(alloc);
    char *nalloc = zmalloc_hybrid(size);
    memcpy(nalloc, alloc, size);
    kvobj *kv = (kvobj *)(nalloc + ((char *)orig - alloc));
    if (kv->type == OBJ_STRING && kv->encoding == OBJ_ENCODING_EMBSTR)
        kv->ptr = (char *)kv->ptr + (nalloc - alloc);
    kv->refcount = 1;
    return kv;
}


/* Overlay callbacks for the server keyspace (privdata = the store's
 * hybsnapGuardPriv). The clone copies the whole kvobj allocation into a fresh
 * arena block; HYBSNAP_NOFORK values (single allocation in the arena) are
 * deep-copied, everything else is shared with the original. db->expires and
 * db->subexpires are pointed at the clone. hybsnapOverlayFreeOriginal releases a
 * fork-instant original after the drain. */
static void *hybsnapOverlayCloneKey(void *privdata, int didx, void *orig_) {
    /* The db owning the store right now (NULL once FLUSHDB detached it: no
     * expires/subexpires to keep in step, and no writes reach it anyway). */
    redisDb *db = hybsnap_save ? hybsnapStoreOwnerDb(hybsnap_save, ((struct hybsnapGuardPriv *)privdata)->s) : NULL;
    kvobj *orig = orig_;
    kvobj *kv = hybsnapKvobjCloneHeader(orig);

    /* HYBSNAP_NOFORK: the scan thread reads the original live -> the clone gets a
     * private copy of the value (in the arena when there is one). HYBSNAP_FORK:
     * the child reads its CoW copy -> the clone shares the value.
     *
     * Without the arena the class of a single-allocation value is a size
     * decision the scan thread takes by reading the value's allocation, so a
     * shared value that the clone then reallocates would be read after free.
     * Single-allocation values are therefore always copied in that mode;
     * only multi-allocation values (never dereferenced by the scan thread)
     * are shared. */
    int to_arena = hybsnapArenaExists();
    int shared = hybsnapOriginalSharesValue(orig);
    int nofork = !shared;
    if (!hybsnapValueIsInline(orig->type, orig->encoding)) {
        switch (orig->encoding) {
        case OBJ_ENCODING_RAW:
            if (nofork) kv->ptr = hybsnapSdsCopy(orig->ptr, to_arena);
            else shared = 1;
            break;
        case OBJ_ENCODING_LISTPACK:
            if (nofork) kv->ptr = hybsnapBlobCopy(orig->ptr, lpBytes(orig->ptr), to_arena);
            else shared = 1;
            break;
        case OBJ_ENCODING_INTSET:
            if (nofork) kv->ptr = hybsnapBlobCopy(orig->ptr, intsetBlobLen(orig->ptr), to_arena);
            else shared = 1;
            break;
        case OBJ_ENCODING_LISTPACK_EX: {
            /* Private copy (struct + listpack) with a fresh ExpireMeta. */
            listpackEx *o = orig->ptr;
            if (nofork) {
                listpackEx *n = zcalloc_hybrid(sizeof(*n));
                n->meta.trash = 1;
                n->lp = hybsnapBlobCopy(o->lp, lpBytes(o->lp), zmalloc_in_hybrid_arena(o->lp));
                kv->ptr = n;
            } else {
                shared = 1;
            }
            break;
        }
        default:
            /* Multi-allocation value (hashtable, skiplist, quicklist, stream,
             * module, array): HYBSNAP_FORK, the child has its own copy. */
            shared = 1;
            break;
        }
    }
    serverAssert(shared == hybsnapOriginalSharesValue(orig));

    hybsnapSaveRetainedAdd(orig, shared);

    /* The clone takes over the original's per-slot accounting (later in-place
     * updates and the delete of the clone are counted against the slot, the
     * original is released unaccounted when the overlay is drained) and the
     * expiry structures: they are not part of the scanned snapshot, and every
     * later TTL operation must act on the clone. */
    if (db) hybsnapKeyRepoint(db, didx, orig, kv);
    return kv;
}

/* Does the clone of 'orig' take over its value (1) or copy it (0)? Used when
 * the clone is made and again when the original is released, so it depends
 * only on the original's header (never written) and on its value's address
 * (never dereferenced: a shared value may be gone by then):
 *   - a value inside the header allocation is copied with it;
 *   - a single-allocation value (string, listpack, intset, listpack with field
 *     TTLs) is copied when the scan thread reads it: in the arena, and always
 *     without the arena (the thread would read a value the clone reallocates);
 *   - a multi-allocation value is the fork child's and is taken over. */
static int hybsnapOriginalSharesValue(const kvobj *orig) {
    if (hybsnapValueIsInline(orig->type, orig->encoding)) return 0;
    switch (orig->encoding) {
    case OBJ_ENCODING_RAW:
    case OBJ_ENCODING_LISTPACK:
    case OBJ_ENCODING_INTSET:
    case OBJ_ENCODING_LISTPACK_EX:
        if (!hybsnapEncodingIsNoforkClass(orig->type, orig->encoding)) return 1;
        return hybsnapArenaExists() ? !zmalloc_in_hybrid_arena(orig->ptr) : 0;
    default:
        return 1;
    }
}

/* A key deleted while its bucket is still pending save keeps its fork-instant original
 * for the scan, in its overlayed bucket, until the bucket is written back;
 * when the clone took over its value, only the header is released.
 * Released through hybsnapOverlayFreeOriginal. */
static void hybsnapOverlayFreeOriginal(void *privdata, int didx, void *orig_) {
    UNUSED(privdata);
    UNUSED(didx);
    kvobj *orig = orig_;
    int shared = hybsnapOriginalSharesValue(orig);
    hybsnapSaveRetainedSub(orig, shared);
    /* The clone carries the key metadata (no keyMetaOnFree here) and, when
     * shared, owns the value. */
    if (!shared) kvobjFreeValueOnly(orig);
    zfree(kvobjGetAllocPtr(orig));
}

/* Move a single-allocation value out of the arena if it is there. Must be
 * called before such a buffer is ADOPTED by a multi-allocation structure
 * (e.g. a listpack becoming the first quicklist node): every piece of a
 * HYBSNAP_FORK value has to be readable by the fork child. */
void hybsnapValueDetachFromArena(robj *o) {
    if (!hybsnapArenaExists()) return;
    switch (o->encoding) {
    case OBJ_ENCODING_RAW: {
        void *a = sdsAllocPtr(o->ptr);
        if (zmalloc_in_hybrid_arena(a)) o->ptr = hybsnapSdsMove(o->ptr, 0);
        break;
    }
    case OBJ_ENCODING_LISTPACK:
        if (zmalloc_in_hybrid_arena(o->ptr)) o->ptr = hybsnapBlobMove(o->ptr, lpBytes(o->ptr), 0);
        break;
    case OBJ_ENCODING_INTSET:
        if (zmalloc_in_hybrid_arena(o->ptr)) o->ptr = hybsnapBlobMove(o->ptr, intsetBlobLen(o->ptr), 0);
        break;
    default:
        break;
    }
}

/* Reply with up to 'limit' keys whose value placement disagrees with its
 * class: "key type encoding drift" per entry (test aid, DEBUG SNAP-DRIFT). */
static void hybsnapPlacementDriftReply(client *c, redisDb *db, int limit) {
    kvstoreIterator kvs_it;
    dictEntry *de;
    void *replylen = addReplyDeferredLen(c);
    int n = 0;
    kvstoreIteratorInit(&kvs_it, db->keys);
    while ((de = kvstoreIteratorNext(&kvs_it)) != NULL && n < limit) {
        kvobj *kv = dictGetKV(de);
        const char *drift = NULL;
        if (!zmalloc_in_hybrid_arena(kv)) drift = "header-outside";
        else if (!hybsnapEncodingIsNoforkClass(kv->type, kv->encoding)) {
            if (kv->ptr && kv->encoding != OBJ_ENCODING_INT && zmalloc_in_hybrid_arena(kv->ptr)) drift = "fork-value-in-arena";
        } else if (hybsnapValueIsInline(kv->type, kv->encoding)) {
            drift = NULL;
        } else if (kv->encoding == OBJ_ENCODING_LISTPACK_EX) {
            listpackEx *lpt = kv->ptr;
            if (zmalloc_in_hybrid_arena(lpt) != zmalloc_in_hybrid_arena(lpt->lp)) drift = "listpackex-split";
        } else {
            void *a = hybsnapValueAllocPtr(kv);
            hybsnapValueClass want = hybsnapClassify(kv->type, kv->encoding, zmalloc_size(a));
            int in = zmalloc_in_hybrid_arena(a);
            if (want == HYBSNAP_NOFORK && !in) drift = "nofork-outside";
            if (want == HYBSNAP_FORK && in) drift = "fork-inside";
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

static void hybsnapPlacementScan(redisDb *db, hybsnapPlacementStats *st) {
    memset(st, 0, sizeof(*st));
    hybsnapPlacementScanTables(db->keys, st);
    hybsnapPlacementScanTables(db->expires, st);
    kvstoreIterator kvs_it;
    dictEntry *de;
    kvstoreIteratorInit(&kvs_it, db->keys);
    while ((de = kvstoreIteratorNext(&kvs_it)) != NULL) {
        kvobj *kv = dictGetKV(de);
        st->keys++;
        int hdr_in = zmalloc_in_hybrid_arena(kv);
        if (hdr_in) st->headers_in_arena++; else { st->headers_outside++; st->violations++; }

        hybsnapValueClass cls = hybsnapKeyClass(kv);
        if (cls == HYBSNAP_NOFORK) st->class_nofork++; else st->class_fork++;

        if (getModuleMetaBits(kv->metabits)) {
            /* Serialized by the child (module callbacks): nothing of the value
             * may be in the arena. */
            if (cls == HYBSNAP_NOFORK && !hybsnapValueIsInline(kv->type, kv->encoding)) st->violations++;
            if (kv->encoding == OBJ_ENCODING_LISTPACK_EX) {
                listpackEx *lpt = kv->ptr;
                if (zmalloc_in_hybrid_arena(lpt) || zmalloc_in_hybrid_arena(lpt->lp)) st->violations++;
            }
            continue;
        }

        if (!hybsnapEncodingIsNoforkClass(kv->type, kv->encoding)) {
            /* Multi-allocation value: its top-level allocation must be forkable. */
            if (kv->ptr && kv->encoding != OBJ_ENCODING_INT && zmalloc_in_hybrid_arena(kv->ptr)) st->violations++;
            /* Quicklists adopt listpacks: every node must be forkable too. */
            if (kv->encoding == OBJ_ENCODING_QUICKLIST) {
                quicklist *ql = kv->ptr;
                for (quicklistNode *node = ql->head; node; node = node->next)
                    if (zmalloc_in_hybrid_arena(node->entry)) st->violations++;
            }
            continue;
        }
        if (hybsnapValueIsInline(kv->type, kv->encoding)) continue;
        if (kv->encoding == OBJ_ENCODING_LISTPACK_EX) {
            listpackEx *lpt = kv->ptr;
            int st_in = zmalloc_in_hybrid_arena(lpt), lp_in = zmalloc_in_hybrid_arena(lpt->lp);
            if (st_in && !lp_in) st->drift_nofork_outside++;
            if (!st_in && lp_in) st->violations++;   /* child would read arena memory */
            if (!st_in && hdr_in) st->drift_nofork_outside++;   /* legacy struct outside */
            continue;
        }
        void *a = hybsnapValueAllocPtr(kv);
        hybsnapValueClass want = hybsnapClassify(kv->type, kv->encoding, zmalloc_size(a));
        int in = zmalloc_in_hybrid_arena(a);
        if (want == HYBSNAP_NOFORK && !in) st->drift_nofork_outside++;
        if (want == HYBSNAP_FORK && in) st->drift_fork_inside++;
    }
    kvstoreIteratorReset(&kvs_it);
}

/* ============================================================================
 * Per-slot background scan
 * ============================================================================
 *
 * The scan walks every kvstore dict ("slot") of every store in sequence on a
 * dedicated thread and hands each fork-instant kvobj to a dispatch callback.
 * It is generic: nothing here knows about the save object (hybsnapSave); the
 * save layer plugs in through the dispatch, slot-done, window-done and
 * store-done callbacks.
 *
 * Reading contract: the credit window of hybrid_snapshot.h (behind lo in place,
 * [lo, hi) through the overlay, hi and beyond emit-ahead). Beyond it: a
 * dict's layout is paused before hi may pass it; a dict beyond hi may grow
 * and shrink (what was emitted ahead is in the keys' epochs). The thread reads the
 * chains with atomic loads (dictWalkBucketRange) and never writes to a dict.
 *
 * Publication: slot states are atomics written with release semantics by the
 * thread (SCANNING, SCANNED) and by the main thread (PENDING, DONE), and read
 * with acquire semantics. Completed slots are handed to the main thread over
 * a single-producer/single-consumer ring (no locks, no allocation on the
 * thread) plus an eventNotifier so the event loop wakes up; the main thread
 * completes a slot by writing back its overlayed buckets and releasing its state.
 */


static __thread int hybsnap_on_scan_thread = 0;
static __thread int hybsnap_scan_maxmemory_policy = 0;
static __thread int hybsnap_scan_rdb_compression = 0;
static __thread int hybsnap_main_emitting = 0;   /* main thread: inside an emit-ahead serialization */

/* The RDB serializer's configuration, read through one call (rdb.c). On the
 * main thread these are the live server values; on the scan thread the copies
 * frozen at save start, since the main thread may change the live ones while
 * the thread runs, and the LRU clock is computed by the thread itself
 * (LRU_CLOCK reads server.hz, which dynamic-hz adjusts on the main thread).
 * The key-save delay applies only in classic contexts: the scan thread applies
 * its own atomic copy and the main thread's emit-ahead must not slow the
 * command that triggered it. */
hybsnapRdbCfg hybsnapRdbCfgGet(void) {
    hybsnapRdbCfg c;
    c.on_scan = hybsnap_on_scan_thread;
    c.compression = c.on_scan ? hybsnap_scan_rdb_compression : server.rdb_compression;
    c.maxmemory_policy = c.on_scan ? hybsnap_scan_maxmemory_policy : server.maxmemory_policy;
    c.apply_key_save_delay = !c.on_scan && !hybsnap_main_emitting;
    return c;
}

/* ---- Scan positions, the credit window and write sections (see hybrid_snapshot.h) ---- */

static unsigned long hybsnap_window_buckets = HYBSNAP_WINDOW_BUCKETS_DEFAULT;
static void hybsnapSetWindowBuckets(unsigned long n) { hybsnap_window_buckets = n ? n : 1; }
static unsigned long hybsnapGetWindowBuckets(void) { return hybsnap_window_buckets; }

static inline dict *hybsnapTableDict(hybsnapScanCtx *ctx, int s, int didx);
static inline hybsnapSlotState hybsnapDictStateOf(hybsnapScanCtx *ctx, dict *d);

/* The scan order as integers. A position packs (ord, phase, idx): ord is the
 * dict's ordinal (its store's base plus didx, the real slot), phase 0 the
 * dict's layout (the thread reads table sizes and rehashidx there), 1 and 2
 * its tables, idx the bucket. The thread visits stores, dicts, phases and
 * buckets in increasing order, so it only ever moves up; dicts it never
 * visits (empty at the freeze, outside a slot filter) are gaps nobody
 * compares against. */
#define HYBSNAP_POS_IDX_BITS 40
#define HYBSNAP_POS_ORD_BITS 22
#define HYBSNAP_POS(ord, phase, idx) (((uint64_t)(ord) << (HYBSNAP_POS_IDX_BITS + 2)) | ((uint64_t)(phase) << HYBSNAP_POS_IDX_BITS) | (uint64_t)(idx))
#define HYBSNAP_POS_ORD(p) ((p) >> (HYBSNAP_POS_IDX_BITS + 2))
#define HYBSNAP_POS_PHASE(p) ((int)(((p) >> HYBSNAP_POS_IDX_BITS) & 3))
#define HYBSNAP_POS_IDX(p) ((unsigned long)((p) & ((1ULL << HYBSNAP_POS_IDX_BITS) - 1)))
#define HYBSNAP_POS_INF UINT64_MAX            /* hi: no limit */
#define HYBSNAP_SCAN_BATCH 128                /* buckets the thread reads per announcement */
#define HYBSNAP_SCAN_LOOKAHEAD (HYBSNAP_SCAN_PREFETCH > HYBSNAP_SCAN_PREFETCH2 ? HYBSNAP_SCAN_PREFETCH : HYBSNAP_SCAN_PREFETCH2)   /* announced past the batch for the prefetches */
#define HYBSNAP_ADVANCE_DICTS 64              /* dicts one grant may open */
#define HYBSNAP_OV_SLOTS_SOFT 16              /* overlay windows in use past which a section stops granting */

/* The credit window (hybrid_snapshot.h). Here:
 * cur is the last position of the batch the thread reads next (its prefetch
 * lookahead included), lo is stored after every bucket. hi is capped by
 * hybsnapWindowCap at section open and raised by hybsnapWindowRefill, at the
 * statement boundaries of EXEC and scripts only (hybsnapSectionGrant), each
 * time lo passes half the grant, unless HYBSNAP_OV_SLOTS_SOFT overlay windows
 * are in use. Never in the middle of a command: a key it handed out ahead of
 * the thread (a link, a kvobj pointer) is written in place until the command
 * ends, so its bucket must stay out of the thread's reach until then.
 * The cap is a seq_cst store-load pair on each side (thread: store cur,
 * load hi; main: store hi, load cur): either the main thread sees where the
 * thread is, or the thread sees the cap. After the cap the thread is at most
 * on cur, and any later position it reaches sees it. */
/* The thread's words on a cache line of their own: it stores lo after every
 * bucket, and the main thread reads hybsnap_section_seq after every command. */
static struct {
    uint64_t cur;                          /* thread: seq_cst stores */
    uint64_t lo;                           /* thread: release stores (seq_cst at the end) */
} __attribute__((aligned(CACHE_LINE_SIZE))) hybsnap_pos_thread;
#define hybsnap_cur (hybsnap_pos_thread.cur)
#define hybsnap_lo (hybsnap_pos_thread.lo)
static uint64_t hybsnap_hi __attribute__((aligned(CACHE_LINE_SIZE))) = HYBSNAP_POS_INF;   /* main thread: seq_cst at the cap, release otherwise */
static hybsnapScanCtx *hybsnap_live = NULL;      /* main thread: the scan whose thread runs (the cap and grants apply) */
/* Write-section counter: odd while the main thread is inside a write section,
 * +1 at each open and close. The scan thread reads it once, at the end, to
 * wait for an open section to close (hybsnapWalkerQuiesce). */
static redisAtomic uint64_t hybsnap_section_seq = 0;

static inline uint64_t hybsnapPosLoLoad(void) { return __atomic_load_n(&hybsnap_lo, __ATOMIC_ACQUIRE); }

/* A dict's ordinal and back. */
static inline uint64_t hybsnapOrd(hybsnapScanCtx *ctx, int s, int didx) { return (uint64_t)ctx->store[s].ord_base + (uint64_t)didx; }
static void hybsnapOrdDecode(hybsnapScanCtx *ctx, uint64_t ord, int *s, int *didx) {
    int k = 0;
    while (k + 1 < ctx->num_stores && ord >= (uint64_t)ctx->store[k + 1].ord_base) k++;
    *s = k;
    *didx = (int)(ord - (uint64_t)ctx->store[k].ord_base);
}

static void hybsnapScanPauseLayout(hybsnapScanCtx *ctx, dict *d);

/* Main thread: the position 'budget' readable buckets after 'from' in the
 * scan order, and in *mid the one half way. Only buckets the thread will read
 * count (table 0 from rehashidx while rehashing, table 1 only then); dicts
 * the thread is done with, or that the save does not hold, are skipped. Every
 * dict the walk enters is paused first: positions name buckets by (table,
 * idx), so a dict's layout must hold still before hi may pass it. Stops
 * before the layout of the HYBSNAP_ADVANCE_DICTS-th dict. */
static uint64_t hybsnapWindowAdvance(hybsnapScanCtx *ctx, uint64_t from, unsigned long budget, uint64_t *mid) {
    unsigned long half = budget / 2, used = 0;
    *mid = HYBSNAP_POS_INF;
    if (from >= ctx->pos_end) return ctx->pos_end;
    int s, didx, dicts = 0;
    hybsnapOrdDecode(ctx, HYBSNAP_POS_ORD(from), &s, &didx);
    int phase = HYBSNAP_POS_PHASE(from);
    unsigned long idx = HYBSNAP_POS_IDX(from);
    for (; s < ctx->num_stores; s++, didx = 0) {
        for (; didx < ctx->store[s].num_dicts; didx++, phase = 0, idx = 0) {
            dict *d = ctx->store[s].frozen ? hybsnapTableDict(ctx, s, didx) : NULL;
            if (!d) continue;
            hybsnapSlotState st = hybsnapDictStateOf(ctx, d);
            if (st == HYBSNAP_SLOT_SCANNED || st == HYBSNAP_SLOT_DONE) continue;
            uint64_t ord = hybsnapOrd(ctx, s, didx);
            if (++dicts > HYBSNAP_ADVANCE_DICTS) {
                if (*mid == HYBSNAP_POS_INF) *mid = HYBSNAP_POS(ord, 0, 0);
                return HYBSNAP_POS(ord, 0, 0);
            }
            hybsnapScanPauseLayout(ctx, d);
            if (phase == 0) phase = 1, idx = 0;
            int rehashing = dictIsRehashing(d);
            for (; phase <= (rehashing ? 2 : 1); phase++, idx = 0) {
                unsigned long size = DICTHT_SIZE(d->ht_size_exp[phase - 1]);
                if (phase == 1 && rehashing && idx < (unsigned long)d->rehashidx) idx = (unsigned long)d->rehashidx;
                if (idx >= size) continue;
                unsigned long avail = size - idx;
                if (*mid == HYBSNAP_POS_INF && used + avail >= half) *mid = HYBSNAP_POS(ord, phase, idx + (half - used));
                if (used + avail >= budget) return HYBSNAP_POS(ord, phase, idx + (budget - used));
                used += avail;
            }
        }
    }
    return ctx->pos_end;
}

/* Main thread, opening a write section: cap the thread. After the store of
 * hi and the load of cur, the thread is at most on cur (it announces before
 * it reads, and reads hi after announcing), so hi = max(H, cur + 1). A
 * thread found past H ran free outside sections; the dicts between are done
 * with, and the one it is in is paused here. */
static void hybsnapWindowCap(hybsnapScanCtx *ctx) {
    uint64_t lo = __atomic_load_n(&hybsnap_lo, __ATOMIC_SEQ_CST);
    uint64_t mid, h = hybsnapWindowAdvance(ctx, lo, ctx->window_buckets, &mid);
    __atomic_store_n(&hybsnap_hi, h, __ATOMIC_SEQ_CST);
    uint64_t c = __atomic_load_n(&hybsnap_cur, __ATOMIC_SEQ_CST);
    if (c >= h && c < ctx->pos_end) {
        int s, didx;
        hybsnapOrdDecode(ctx, HYBSNAP_POS_ORD(c), &s, &didx);
        dict *d = ctx->store[s].frozen ? hybsnapTableDict(ctx, s, didx) : NULL;
        if (d && hybsnapDictStateOf(ctx, d) == HYBSNAP_SLOT_SCANNING) hybsnapScanPauseLayout(ctx, d);
        h = c + 1;
        __atomic_store_n(&hybsnap_hi, h, __ATOMIC_RELEASE);
        mid = h;
    }
    ctx->hi = h;
    ctx->mid = mid;
}

/* Main thread, at a statement boundary inside a section (hybsnapSectionGrant):
 * once the thread is half way through its grant, grant the next W buckets
 * (pausing the dicts they open). Not while many overlay windows are in use:
 * the section's drain has to come first. */
static void hybsnapWindowRefill(hybsnapScanCtx *ctx) {
    if (hybsnapPosLoLoad() < ctx->mid) return;
    if (kvstoreOverlaySlotsInUse() >= HYBSNAP_OV_SLOTS_SOFT) return;
    uint64_t mid, h = hybsnapWindowAdvance(ctx, hybsnapPosLoLoad(), ctx->window_buckets, &mid);
    if (h > ctx->hi) {
        ctx->hi = h;
        __atomic_store_n(&hybsnap_hi, h, __ATOMIC_RELEASE);
        ctx->grants++;
    }
    ctx->mid = mid;
}

/* Scan thread: may it read positions [p, last]? Announces 'last' in cur,
 * then waits while p is at or above hi. Returns the end (exclusive) it may
 * read to, at most last + 1, or 0 when the scan was aborted meanwhile. */
static uint64_t hybsnapScanReach(hybsnapScanCtx *ctx, uint64_t p, uint64_t last) {
    __atomic_store_n(&hybsnap_cur, last, __ATOMIC_SEQ_CST);
    uint64_t hi = __atomic_load_n(&hybsnap_hi, __ATOMIC_SEQ_CST);
    if (p >= hi) {
        monotime t;
        elapsedStart(&t);
        atomicIncr(ctx->stalls, 1);
        int spins = 0;
        while (p >= hi) {
            if (hybsnapScanAborted(ctx)) return 0;
            if (++spins < 200) sched_yield();
            else usleep(20);
            hi = __atomic_load_n(&hybsnap_hi, __ATOMIC_SEQ_CST);
        }
        atomicIncr(ctx->stall_us, (long long)elapsedUs(t));
    }
    return hi > last ? last + 1 : hi;
}

static inline void hybsnapScanSetLo(uint64_t lo) { __atomic_store_n(&hybsnap_lo, lo, __ATOMIC_RELEASE); }

/* Scan thread, at the end: wait until no write section is open. A section
 * opened after the seq_cst store of lo = END sees every bucket behind it, so
 * nothing more is emitted; one open before may still be emitting. This is
 * the only time the thread waits for a section. */
static void hybsnapWalkerQuiesce(hybsnapScanCtx *ctx) {
    uint64_t v;
    atomicGetWithSync(hybsnap_section_seq, v);
    if (!(v & 1)) return;
    int spins = 0;
    for (;;) {
        uint64_t n;
        atomicGetWithSync(hybsnap_section_seq, n);
        if (n != v) return;
        /* The command aborting the save may itself hold the section open and
         * be waiting to join us: an aborted scan thread reads nothing more. */
        if (hybsnapScanAborted(ctx)) return;
        if (++spins < 200) sched_yield();
        else usleep(20);
    }
}

/* Write sections (hybrid_snapshot.h, "The credit window"). Opened by the first
 * guard or funnel hook of a command, reads included: caps the thread
 * (hybsnapWindowCap). hi moves inside the section only at a statement boundary
 * (hybsnapSectionGrant). hybsnapSectionClose writes back the overlayed buckets
 * the thread has passed and lifts the cap. */
static void hybsnapSectionOpen(void) {
    uint64_t v;
    atomicGet(hybsnap_section_seq, v);          /* single writer: relaxed read of our own value */
    if (v & 1) return;
    atomicSetWithSync(hybsnap_section_seq, v + 1);   /* seq_cst: pairs with hybsnapWalkerQuiesce */
    if (hybsnap_live) hybsnapWindowCap(hybsnap_live);
}

/* Main thread, between two statements of EXEC or a script, inside the
 * command's section: no link or kvobj pointer of the outer command is live, so
 * the thread may be granted the next W buckets. */
void hybsnapSectionGrant(void) {
    uint64_t v;
    atomicGet(hybsnap_section_seq, v);
    if ((v & 1) && hybsnap_live) hybsnapWindowRefill(hybsnap_live);
}

void hybsnapSectionClose(void) {
    uint64_t v;
    atomicGet(hybsnap_section_seq, v);
    if (likely(!(v & 1))) return;    /* outside a section */
    /* At a point where no caller holds a link: write back what the
     * thread has passed, then let it run free. */
    hybsnapSectionDrain();
    hybsnapRetiredFree();
    if (hybsnap_live) {
        hybsnap_live->hi = HYBSNAP_POS_INF;
        __atomic_store_n(&hybsnap_hi, HYBSNAP_POS_INF, __ATOMIC_RELEASE);
    }
    atomicSetWithSync(hybsnap_section_seq, v + 1);
}

/* The scan thread's waits for hi and the grants raised inside sections, of
 * the running save, else of the last one (kept at its release). */
static struct { long long stalls, stall_us, grants; } hybsnap_last_window;
static void hybsnapScanWindowStats(hybsnapScanCtx *ctx, long long *stalls, long long *stall_us, long long *grants) {
    if (!ctx) {
        *stalls = hybsnap_last_window.stalls;
        *stall_us = hybsnap_last_window.stall_us;
        *grants = hybsnap_last_window.grants;
        return;
    }
    atomicGet(ctx->stalls, *stalls);
    atomicGet(ctx->stall_us, *stall_us);
    *grants = ctx->grants;
}

/* Main thread: the scan thread is about to start (positions from the first
 * dict, no limit), or failed to (NULL). */
static void hybsnapWindowReset(hybsnapScanCtx *live) {
    __atomic_store_n(&hybsnap_cur, 0, __ATOMIC_SEQ_CST);
    __atomic_store_n(&hybsnap_lo, 0, __ATOMIC_SEQ_CST);
    __atomic_store_n(&hybsnap_hi, HYBSNAP_POS_INF, __ATOMIC_SEQ_CST);
    if (live) {
        live->hi = HYBSNAP_POS_INF;
        live->mid = 0;
    }
    hybsnap_live = live;
}

/* ---- Save epochs (object.h kvobjGetEpoch, hybrid_snapshot.h) ----
 *
 * Every save takes the next epoch at its freeze (the fork instant). A key
 * whose epoch is at least the save's is not the save's to serialize by the
 * scan thread: the main thread emitted it ahead of the thread (and stamped
 * it before writing it in place), or it was created after the fork instant
 * (every kvobj created while a save runs gets the save's epoch). A key with
 * an older epoch, or none, is fork-instant. Stamps of earlier saves are
 * never cleared: they are older than the next save's epoch. 2^32 saves
 * would wrap it.
 *
 * Only the main thread writes epochs, and only of keys in buckets at or
 * beyond hi, inside a write section: the thread reads a bucket only after
 * loading a hi past it, which orders the stamp before its read. */

int hybsnap_epoch_mode = HYBSNAP_EPOCH_NONE;
uint32_t hybsnap_epoch_stamp = 0;
static int hybsnap_epoch_live = 0;   /* a scan holds the current epoch (freeze to release) */

/* Main thread: which new kvobjs get an epoch (hybsnapEpochMode). */
static void hybsnapEpochModeUpdate(void) {
    if (hybsnap_epoch_live) hybsnap_epoch_mode = HYBSNAP_EPOCH_ALWAYS;
    else if (!server.hybrid_snapshot) hybsnap_epoch_mode = HYBSNAP_EPOCH_NONE;
    else hybsnap_epoch_mode = server.hybrid_snapshot_epoch_prealloc ? HYBSNAP_EPOCH_ALWAYS : HYBSNAP_EPOCH_IF_FREE;
}

/* Main thread, at the freeze: the scan's epoch, the stamp of new kvobjs until it is released. */
static void hybsnapEpochBegin(hybsnapScanCtx *ctx) {
    ctx->epoch = ++hybsnap_epoch_stamp;
    hybsnap_epoch_live = 1;
    hybsnapEpochModeUpdate();
}

static void hybsnapEpochEnd(void) {
    hybsnap_epoch_live = 0;
    hybsnapEpochModeUpdate();
}

/* CONFIG SET hybrid-snapshot-epoch-prealloc. */
int hybsnapEpochConfigApply(const char **err) {
    UNUSED(err);
    hybsnapEpochModeUpdate();
    return 1;
}

/* kvobj allocations replaced by a copy with an epoch (hybsnapKvobjStamp),
 * freed when the write section closes: a caller of the command that still
 * holds the old pointer reads valid memory until then. Main thread. */
static void **hybsnap_retired = NULL;
static size_t hybsnap_retired_n = 0, hybsnap_retired_cap = 0;

static void hybsnapRetire(void *alloc) {
    if (hybsnap_retired_n == hybsnap_retired_cap) {
        hybsnap_retired_cap = hybsnap_retired_cap ? hybsnap_retired_cap * 2 : 64;
        hybsnap_retired = zrealloc(hybsnap_retired, hybsnap_retired_cap * sizeof(void *));
    }
    hybsnap_retired[hybsnap_retired_n++] = alloc;
}

static void hybsnapRetiredFree(void) {
    for (size_t i = 0; i < hybsnap_retired_n; i++) zfree(hybsnap_retired[i]);
    hybsnap_retired_n = 0;
}

/* ---- Per-dict save state (the kvstore extension slot: kvstoreDictMetaBase.ext_word / ext) ----
 *
 * At freeze the main thread takes one reference on every non-empty dict of
 * a scanned store, stamps it (generation, PENDING) and records it in the
 * save's own per-store table. That table is what the thread
 * walks and what the main thread compares a live dict against: a dict that
 * is not the table's entry for its slot (created or swapped in since the
 * fork instant, or the slot completed) holds nothing of the save. The store
 * may free, empty or move a table dict meanwhile: it detaches it and the
 * save, holding its reference, reads it to the end and releases it. The
 * rest of the state (overlay, pause) is attached on first touch
 * and freed when the dict completes. */

static inline kvstoreDictMetaBase *hybsnapMeta(dict *d) { return (kvstoreDictMetaBase *)dictMetadata(d); }
static inline uint64_t hybsnapWordLoad(dict *d) { return __atomic_load_n(&hybsnapMeta(d)->ext_word, __ATOMIC_ACQUIRE); }
static inline void hybsnapWordStore(dict *d, uint64_t w) { __atomic_store_n(&hybsnapMeta(d)->ext_word, w, __ATOMIC_RELEASE); }

/* The save's table of fork-instant dicts (filled at freeze, one reference
 * each). Entries are cleared at completion, on the main thread, after the
 * thread published SCANNED for the slot or was joined. */
static inline dict *hybsnapTableDict(hybsnapScanCtx *ctx, int s, int didx) {
    return ctx->store[s].dicts ? __atomic_load_n(&ctx->store[s].dicts[didx], __ATOMIC_RELAXED) : NULL;
}

/* Either thread: the state of a table dict (its word carries this save's generation). */
static inline hybsnapSlotState hybsnapDictStateOf(hybsnapScanCtx *ctx, dict *d) {
    uint64_t w = hybsnapWordLoad(d);
    serverAssert(HYBSNAP_WORD_GEN(w) == ctx->gen);
    return (hybsnapSlotState)HYBSNAP_WORD_STATE(w);
}

/* Main thread: the state of the live dict d of slot (s, didx): DONE for a
 * store that is not scanned (empty at the fork instant, or completed), after
 * the end, and for a dict that is not the save's. */
static hybsnapSlotState hybsnapScanDictStateMain(hybsnapScanCtx *ctx, int s, int didx, dict *d) {
    if (ctx->ended || !ctx->store[s].frozen) return HYBSNAP_SLOT_DONE;
    if (d != hybsnapTableDict(ctx, s, didx)) return HYBSNAP_SLOT_DONE;
    return hybsnapDictStateOf(ctx, d);
}

/* Scan thread: claim dict d for the walk (PENDING -> SCANNING; the main
 * thread changes no word before SCANNED). */
static void hybsnapScanClaim(hybsnapScanCtx *ctx, dict *d) {
    serverAssert(hybsnapDictStateOf(ctx, d) == HYBSNAP_SLOT_PENDING);
    hybsnapWordStore(d, HYBSNAP_WORD(ctx->gen, HYBSNAP_SLOT_SCANNING));
}

/* The scan's own tables: the per-store dict tables and the completion ring. */
static size_t hybsnapScanTableBytes(hybsnapScanCtx *ctx) {
    size_t n = ctx->slots_total ? ctx->slots_total : 1;
    size_t bytes = n * sizeof(uint64_t);   /* ring */
    for (int s = 0; s < ctx->num_stores; s++)
        if (ctx->store[s].dicts) bytes += (size_t)ctx->store[s].num_dicts * sizeof(dict *);
    return bytes;
}

/* Attach the keyspace overlay (kvstore_overlay.h) to the scan: a dict gets an
 * overlay the first time the guard attaches it; its overlayed buckets are written back on
 * the main thread once the thread has passed them and it is released when its slot
 * completes. Only the overlay callbacks of 'hooks' (clone, free_orig, enter,
 * prepare) are taken; the guard and layout hooks are the scan's
 * own. Every hook of store s receives the store's hybsnapGuardPriv, whose 'user'
 * field is 'store_privdata[s]' (NULL array: NULL). Must be called before
 * hybsnapScanFreeze(). */
/* Before start: restrict the scan to the slots whose bit is set (cluster
 * mode; the bitmap must outlive the scan). Dicts outside the set are never
 * frozen: the guard treats them as not the save's, so their writes are plain
 * writes, and the thread's pass never visits them. */
static void hybsnapScanSetSlotFilter(hybsnapScanCtx *ctx, const uint8_t *bitmap) {
    serverAssert(!ctx->started);
    ctx->slot_filter = bitmap;
}

static void hybsnapScanSetOverlay(hybsnapScanCtx *ctx, const kvstoreSnapHooks *hooks, void **store_privdata)
{
    serverAssert(!ctx->started);
    ctx->hooks.clone = hooks->clone;
    ctx->hooks.free_orig = hooks->free_orig;
    ctx->hooks.enter = hooks->enter;
    ctx->hooks.prepare = hooks->prepare;
    ctx->has_ov = 1;
    ctx->ov_priv = store_privdata;
}

static uint64_t hybsnap_scan_gen = 0;   /* generation of the last scan context created */
static long hybsnap_stat_retained_dicts = 0;   /* table dicts freed by a save, across saves (INFO snap_retained_dicts) */

/* Create a scan over 'num_stores' kvstores (normally one per db: db->keys).
 * Nothing is frozen or started until hybsnapScanFreeze() and
 * hybsnapScanStartThread(). 'dispatch' runs on the
 * scan thread once for every fork-instant key of a slot and must be strictly
 * read-only on the keyspace (it may read the kvobj and, for HYBSNAP_NOFORK
 * values, the value, never write to any dict or object). 'slot_done' runs on
 * the main thread when a slot is complete, BEFORE its dict is unfrozen and
 * its state becomes DONE ('aborted' = 1 when the slot was not fully scanned);
 * the dict's overlayed buckets are written back and its save state released right after. */
static hybsnapScanCtx *hybsnapScanCreate(kvstore **stores, int num_stores,
                            hybsnapScanDispatchFn *dispatch,
                            hybsnapScanSlotDoneFn *slot_done, void *privdata)
{
    hybsnapScanCtx *ctx = zcalloc(sizeof(*ctx));
    ctx->store = zcalloc(sizeof(hybsnapScanStore) * num_stores);
    ctx->num_stores = num_stores;
    long long ords = 0;
    for (int s = 0; s < num_stores; s++) {
        hybsnapScanStore *st = &ctx->store[s];
        st->kvs = stores[s];
        st->num_dicts = kvstoreNumDicts(stores[s]);
        st->ord_base = (int)ords;
        ords += st->num_dicts;
        st->guard.ctx = ctx;
        st->guard.s = s;
    }
    serverAssert(ords < (1LL << HYBSNAP_POS_ORD_BITS));   /* hybsnapScanFits() */
    ctx->pos_end = HYBSNAP_POS(ords, 0, 0);
    ctx->hi = 0;   /* no grant until the thread starts: every bucket is ahead */
    ctx->window_buckets = hybsnap_window_buckets;
    ctx->ov_block = 1;
    while (ctx->ov_block < ctx->window_buckets) ctx->ov_block <<= 1;
    ctx->gen = ++hybsnap_scan_gen;
    atomicSet(ctx->cur_slot, -1);
    ctx->dispatch = dispatch;
    ctx->slot_done = slot_done;
    ctx->privdata = privdata;
    ctx->en = createEventNotifier();
    return ctx;
}

/* Main thread: the dict's save state (the pause), allocated on first use. */
static hybsnapDictState *hybsnapScanState(hybsnapScanCtx *ctx, dict *d) {
    serverAssert(hybsnapDictStateOf(ctx, d) != HYBSNAP_SLOT_DONE);
    hybsnapDictState *sn = hybsnapMeta(d)->ext;   /* main thread: the only writer of the pointer */
    if (!sn) {
        sn = zcalloc(sizeof(*sn));
        __atomic_store_n(&hybsnapMeta(d)->ext, sn, __ATOMIC_RELEASE);
    }
    return sn;
}

/* Main thread: hold the dict's layout (table sizes, rehashidx) until its slot
 * completes. Done before the thread may read the layout (hybsnapWindowAdvance,
 * hybsnapWindowCap) and on any access while it is on the dict (guard). Works for
 * a dict that left its store: no overlay is attached here. */
static void hybsnapScanPauseLayout(hybsnapScanCtx *ctx, dict *d) {
    hybsnapDictState *sn = hybsnapScanState(ctx, d);
    if (sn->paused) return;
    sn->paused = 1;
    dictPauseRehashing(d);
    dictPauseAutoResize(d);
}

static inline int hybsnapDictPaused(dict *d) {
    hybsnapDictState *sn = hybsnapMeta(d)->ext;
    return sn && sn->paused;
}

/* Main thread, inside a write section: attach dict (s, didx) to the scan on
 * demand: its overlay, and a layout pause when the thread is on it. Called by
 * the guard on the first structural write to a dict the thread has not
 * finished, or on any access while the thread is on it. A dict attached but
 * not reached yet (beyond hi, unpaused) may still grow and shrink: emit-ahead
 * lives in the keys' epochs, which move with them. Returns the overlay (NULL
 * without one). */
static struct kvstoreDictOverlay *hybsnapScanAttach(hybsnapScanCtx *ctx, int s, int didx, dict *d, int pause) {
    hybsnapScanState(ctx, d);
    if (ctx->has_ov && !hybsnapMeta(d)->ov) kvstoreOverlayAttach(ctx->store[s].kvs, didx);
    if (pause) hybsnapScanPauseLayout(ctx, d);
    return hybsnapMeta(d)->ov;
}

/* kvstore snapshot guard (kvstore_overlay.h): every access to a dict without an
 * overlay while the scan runs lands here. */
static struct kvstoreDictOverlay *hybsnapOverlayGuard(void *privdata, kvstore *kvs, int didx, int write) {
    struct hybsnapGuardPriv *g = privdata;
    hybsnapScanCtx *ctx = g->ctx;
    int s = g->s;
    dict *d = kvstoreGetDict(kvs, didx);
    /* Not the save's dict (created or swapped into the slot since the fork
     * instant, or the save is done with the slot): nothing of the fork
     * instant in it, a plain access. */
    if (!d || d != hybsnapTableDict(ctx, s, didx)) return NULL;
    if (hybsnapDictStateOf(ctx, d) >= HYBSNAP_SLOT_SCANNED) return NULL;   /* the thread is finished with it */
    /* Every access runs inside a section, reads included (a find may step a
     * rehash): opening one caps the thread, and the cap pauses any dict the
     * thread may be reading or may enter before the section ends. */
    hybsnapSectionOpen();
    /* The state after the cap: the thread may have claimed the dict while it
     * ran free. A write to a dict the thread has not reached attaches it
     * without pausing it. */
    hybsnapSlotState st = hybsnapDictStateOf(ctx, d);
    if (st >= HYBSNAP_SLOT_SCANNED) return NULL;
    int pause = (st == HYBSNAP_SLOT_SCANNING);
    if (!write && !pause) return NULL;   /* a read of a dict the thread is not reading */
    return hybsnapScanAttach(ctx, s, didx, d, pause);
}

/* Freeze one store: every dict non-empty now becomes the save's (a reference,
 * its word stamped PENDING, its pointer in the save's table). The thread walks
 * that table, never the live slot table, so the store may free, empty or move
 * these dicts meanwhile: it detaches them and the save releases them when
 * their slot completes. A dict empty now, or allocated later, holds nothing of
 * the fork instant: the guard leaves it alone (an empty store is never scanned
 * nor guarded). */
static void hybsnapScanFreezeStore(hybsnapScanCtx *ctx, int s) {
    hybsnapScanStore *st = &ctx->store[s];
    kvstore *kvs = st->kvs;
    if (kvstoreSize(kvs) == 0) return;
    int n = st->num_dicts;
    st->dicts = zcalloc(sizeof(dict *) * n);
    for (int didx = 0; didx < n; didx++) {
        if (ctx->slot_filter && !(ctx->slot_filter[didx >> 3] & (1u << (didx & 7)))) continue;   /* not ours: a plain dict throughout */
        dict *d = kvstoreGetDict(kvs, didx);
        if (!d || dictSize(d) == 0) continue;
        kvstoreDictRetain(kvs, didx);
        hybsnapWordStore(d, HYBSNAP_WORD(ctx->gen, HYBSNAP_SLOT_PENDING));
        st->dicts[didx] = d;
        st->nrefs++;
    }
    ctx->slots_total += st->nrefs;
    ctx->hooks.guard = hybsnapOverlayGuard;
    st->guard.user = ctx->ov_priv ? ctx->ov_priv[s] : NULL;
    kvstoreSnapGuardSet(kvs, &ctx->hooks, &st->guard);
    st->frozen = 1;
}

/* The store's last dict completed: the guard comes off now, not at release,
 * because the caller of an abort (FLUSHALL, SWAPDB, DEBUG RELOAD) frees or
 * replaces the store right after, before hybsnapScanRelease() runs. */
static void hybsnapScanStoreDone(hybsnapScanCtx *ctx, int s) {
    hybsnapScanStore *st = &ctx->store[s];
    if (!st->frozen) return;
    serverAssert(st->nrefs == 0);
    kvstoreSnapGuardSet(st->kvs, NULL, NULL);
    st->frozen = 0;
    if (ctx->store_done) ctx->store_done(ctx->privdata, s);
}


/* Chain-walk callback on the scan thread: decode the entry and dispatch,
 * unless the key's epoch says the main thread emitted it ahead of the thread
 * or it was born after the fork instant (the stamp was written before hi
 * passed the bucket: the acquire of hi orders it before this read). */
static void hybsnapScanEntryCb(void *privdata, const dictEntry *de, dictEntry **plink) {
    UNUSED(plink);
    struct hybsnapScanEntryArgs *a = privdata;
    kvobj *kv = dictGetKV(de);
    if (a->ctx->skip_stamped && kvobjGetEpoch(kv) >= a->ctx->epoch) {
        atomicIncr(a->ctx->keys_stamped, 1);
        return;
    }
    atomicIncr(a->ctx->keys_dispatched, 1);
    a->ctx->dispatch(a->ctx->privdata, a->s, a->didx, kv);
}

static inline int hybsnapScanAborted(hybsnapScanCtx *ctx) {
    int v;
    atomicGet(ctx->abort, v);
    return v;
}

#if HYBSNAP_SCAN_PREFETCH2
/* Scan thread: prefetch the value allocation of a raw string at the head of a
 * bucket ('ptr' exists only for encodings with a separate allocation). Not
 * instrumented for TSan, as kvobjEpochPtr(): the main thread may drop a
 * reference to the kvobj meanwhile, which rewrites the header word with only
 * refcount changed. */
REDIS_NO_SANITIZE("thread")
static void hybsnapScanPrefetchValue(dictEntry *de) {
    if (!de) return;
    kvobj *kv = dictGetKV(de);
    if (kv->type == OBJ_STRING && kv->encoding == OBJ_ENCODING_RAW) __builtin_prefetch(kv->ptr);
}
#endif

/* Scan thread: read one frozen dict, a batch of buckets at a time, each
 * batch announced and kept below hi (hybsnapScanReach). Returns 1 if aborted. */
static int hybsnapScanWalkDict(hybsnapScanCtx *ctx, int s, int didx, dict *d, unsigned long *since_done) {
    struct hybsnapScanEntryArgs a = { ctx, s, didx };
    uint64_t ord = hybsnapOrd(ctx, s, didx);
    /* The layout: hi past it means the main thread paused the dict first
     * (hybsnapWindowAdvance), and with no limit no section is open. */
    uint64_t lp = HYBSNAP_POS(ord, 0, 0);
    if (!hybsnapScanReach(ctx, lp, lp)) return 1;
    int rehashing = dictIsRehashing(d);   /* stable: rehashing is paused */
    hybsnapScanSetLo(lp + 1);
    for (int t = 0; t < (rehashing ? 2 : 1); t++) {
        unsigned long size = DICTHT_SIZE(d->ht_size_exp[t]);
        serverAssert(size < (1UL << HYBSNAP_POS_IDX_BITS));
        uint64_t base = HYBSNAP_POS(ord, 1 + t, 0);
        /* Buckets of table 0 below rehashidx were moved to table 1 already. */
        unsigned long lo = (t == 0 && rehashing) ? (unsigned long)d->rehashidx : 0;
        int primed = 0;   /* the first buckets of the table are prefetched once, later ones by the lookahead */
        while (lo < size) {
            unsigned long hi = lo + HYBSNAP_SCAN_BATCH;
            if (hi > size || hi < lo) hi = size;
            /* The batch is announced with its prefetch lookahead: the thread
             * touches no bucket head beyond cur, nor at or beyond hi (the main
             * thread may be writing those, emitted ahead and then in place). */
            unsigned long ahead = hi + HYBSNAP_SCAN_LOOKAHEAD;
            if (ahead > size || ahead < hi) ahead = size;
            uint64_t end = hybsnapScanReach(ctx, base + lo, base + ahead - 1);
            if (!end) return 1;
            unsigned long lim = (unsigned long)(end - base);   /* bucket heads below lim may be touched */
            if (hi > lim) hi = lim;
            /* The first touch of each key's header is a cache miss: have the
             * first entries of the next buckets on their way, then the values
             * of the ones after. */
            if (!primed) {
                for (unsigned long k = lo; k < lo + HYBSNAP_SCAN_PREFETCH && k < lim; k++)
                    dictWalkPrefetchBucket(d, t, k);
                primed = 1;
            }
            for (unsigned long i = lo; i < hi; i++) {
                if (i + HYBSNAP_SCAN_PREFETCH < lim)
                    dictWalkPrefetchBucket(d, t, i + HYBSNAP_SCAN_PREFETCH);
#if HYBSNAP_SCAN_PREFETCH2
                /* The header of that bucket is in cache by now: a raw string's
                 * value allocation is the next miss. */
                if (i + HYBSNAP_SCAN_PREFETCH2 < lim)
                    hybsnapScanPrefetchValue(dictWalkBucketHead(d, t, i + HYBSNAP_SCAN_PREFETCH2));
#endif
                dictWalkBucketRange(d, t, i, i + 1, hybsnapScanEntryCb, &a);
                hybsnapScanSetLo(base + i + 1);
            }
            if (hybsnapScanAborted(ctx)) return 1;
            *since_done += hi - lo;
            if (*since_done >= ctx->window_buckets) {
                *since_done = 0;
                if (ctx->window_done) ctx->window_done(ctx->privdata);
            }
            lo = hi;
        }
    }
    return 0;
}

static void hybsnapScanPublishScanned(hybsnapScanCtx *ctx, int s, int didx, dict *d) {
    hybsnapWordStore(d, HYBSNAP_WORD(ctx->gen, HYBSNAP_SLOT_SCANNED));
    atomicSet(ctx->cur_slot, -1);
    long wr;
    atomicGet(ctx->ring_wr, wr);
    ctx->ring[wr] = HYBSNAP_SCAN_ENC(s, didx);
    atomicSetWithSync(ctx->ring_wr, wr + 1);   /* release: ring[wr] visible before wr+1 */
    triggerEventNotifier(ctx->en);
}

static void *hybsnapScanThreadMain(void *arg) {
    hybsnapScanCtx *ctx = arg;
    redis_set_thread_title("snap_scan");
    makeThreadKillable();
    hybsnap_on_scan_thread = 1;
    if (ctx->thread_init) ctx->thread_init(ctx->privdata);

    unsigned long since_done = 0;   /* buckets read since the last window_done */
    for (int s = 0; s < ctx->num_stores && !hybsnapScanAborted(ctx); s++) {
        if (!ctx->store[s].frozen) continue;   /* nothing of the fork instant in it (hybsnapScanFreezeStore) */
        for (int didx = 0; didx < ctx->store[s].num_dicts; didx++) {
            if (hybsnapScanAborted(ctx)) break;
            /* The save's own table: the fork-instant dicts, referenced,
             * whatever the store did with them since. */
            dict *d = hybsnapTableDict(ctx, s, didx);
            if (!d) continue;
            atomicIncr(ctx->slots_scanned, 1);
            /* Claim before announcing the layout: a cap that finds the
             * thread on the dict sees SCANNING and pauses it, as does any
             * access of the dict from then on (guard). */
            hybsnapScanClaim(ctx, d);
            atomicSet(ctx->cur_slot, (int64_t)HYBSNAP_SCAN_ENC(s, didx));
            if (hybsnapScanWalkDict(ctx, s, didx, d, &since_done))
                break;   /* Aborted: leave the slot SCANNING; hybsnapScanAbort() completes it. */
            hybsnapScanPublishScanned(ctx, s, didx, d);
            if (since_done && ctx->window_done) {
                since_done = 0;
                ctx->window_done(ctx->privdata);
            }
        }
    }
    /* Past every position: from here on the main thread may write every
     * bucket. Once the section that may still be emitting has closed,
     * everything the main thread produced is in the queue: drain it before
     * the end marker. */
    __atomic_store_n(&hybsnap_cur, ctx->pos_end, __ATOMIC_SEQ_CST);
    __atomic_store_n(&hybsnap_lo, ctx->pos_end, __ATOMIC_SEQ_CST);
    hybsnapWalkerQuiesce(ctx);
    if (ctx->window_done) ctx->window_done(ctx->privdata);
    if (ctx->thread_done) ctx->thread_done(ctx->privdata, hybsnapScanAborted(ctx));
    atomicSetWithSync(ctx->finished, 1);
    triggerEventNotifier(ctx->en);
    return NULL;
}

/* Main thread. Freeze every store: from here on the dicts are the fork-instant
 * keyspace and every write to one goes through the guard first. Nothing else
 * may run on the main thread between this call and the fork of the writer
 * child. One pass per non-empty store: a reference and a pointer per
 * fork-instant dict, nothing allocated per dict. */
static void hybsnapScanFreeze(hybsnapScanCtx *ctx) {
    serverAssert(!ctx->started && !ctx->frozen_all);
    hybsnapWindowReset(NULL);   /* no thread yet: drop the positions of a previous save */
    hybsnapEpochBegin(ctx);
    for (int s = 0; s < ctx->num_stores; s++) hybsnapScanFreezeStore(ctx, s);
    ctx->ring = zmalloc(sizeof(uint64_t) * (ctx->slots_total ? ctx->slots_total : 1));
    ctx->frozen_all = 1;
}

/* Main thread. Spawn the scan thread over an already frozen keyspace. */
static int hybsnapScanStartThread(hybsnapScanCtx *ctx) {
    serverAssert(!ctx->started && ctx->frozen_all);
    hybsnapWindowReset(ctx);
    uint64_t v;
    atomicGet(hybsnap_section_seq, v);
    if (v & 1) hybsnapWindowCap(ctx);   /* started from inside a section: the thread begins capped */
    if (pthread_create(&ctx->tid, NULL, hybsnapScanThreadMain, ctx) != 0) {
        serverLog(LL_WARNING, "hybsnapScanStartThread: pthread_create failed: %s", strerror(errno));
        hybsnapWindowReset(NULL);
        return C_ERR;
    }
    ctx->started = 1;
    return C_OK;
}

/* Thread-side request to stop (e.g. the channel to the child broke). */
static inline void hybsnapScanRequestAbort(hybsnapScanCtx *ctx) {
    atomicSetWithSync(ctx->abort, 1);
}

/* Owner thread: the save is finished with the dict (its scan completed or
 * the save was aborted). The dict's overlayed buckets are written back and released by the
 * kvstore overlay; the layout is unpaused here. The dict may have left its
 * store meanwhile: the state follows the dict, so this works wherever it is. */
static void hybsnapScanStateRelease(dict *d) {
    hybsnapDictState *sn = hybsnapMeta(d)->ext;
    if (sn) {
        if (sn->paused) {
            dictResumeRehashing(d);
            dictResumeAutoResize(d);
        }
        __atomic_store_n(&hybsnapMeta(d)->ext, NULL, __ATOMIC_RELEASE);
        zfree(sn);
    }
    kvstoreOverlayRelease(d);
}

/* Main thread: finish one slot (release the overlay, resume
 * the layout, slot-done hook, publish DONE). */
static void hybsnapScanCompleteSlot(hybsnapScanCtx *ctx, int s, int didx, int aborted) {
    /* The thread no longer reads this slot: whatever buckets are still
     * overlayed (normally none: they are written back as the window moves) go back to
     * the dict, the dict is unfrozen, and the save's reference goes: a dict
     * its store let go of meanwhile (freed, emptied, moved and released) is
     * freed here. */
    dict *d = hybsnapTableDict(ctx, s, didx);
    serverAssert(d != NULL);
    /* DONE first: the release below writes the overlay back and then asks the
     * store whether the dict may be freed, through the public kvstore entry
     * points, i.e. through the guard. An aborted slot is still SCANNING at
     * this point, and the guard would attach a fresh overlay and state to a
     * table dict in that state, which this completion would then leave
     * behind for the dict's eventual free to trip over (seen with the slot
     * snapshot's fail-point aborts). With DONE published the guard declines. */
    hybsnapWordStore(d, HYBSNAP_WORD(ctx->gen, HYBSNAP_SLOT_DONE));
    hybsnapScanStateRelease(d);
    if (ctx->slot_done) ctx->slot_done(ctx->privdata, s, didx, aborted);
    if (kvstoreDictRefcount(d) == 1) { ctx->retained_dicts++; hybsnap_stat_retained_dicts++; }   /* the save outlived the store's hold on it */
    __atomic_store_n(&ctx->store[s].dicts[didx], NULL, __ATOMIC_RELAXED);
    kvstoreDictUnref(d);
    ctx->store[s].nrefs--;
    ctx->slots_done++;
    if (ctx->store[s].nrefs == 0) hybsnapScanStoreDone(ctx, s);
}

/* Main thread. Handles slots the thread finished since the last call: for
 * each, calls slot_done, unfreezes the dict and publishes DONE. Returns the
 * number of slots completed. Also to be called once after
 * hybsnapScanIsFinished() to drain the tail. */
static int hybsnapScanHandleCompletions(hybsnapScanCtx *ctx) {
    int n = 0;
    long wr;
    atomicGetWithSync(ctx->ring_wr, wr);   /* acquire */
    while (ctx->ring_rd < wr) {
        uint64_t v = ctx->ring[ctx->ring_rd++];
        hybsnapScanCompleteSlot(ctx, HYBSNAP_SCAN_DEC_STORE(v), HYBSNAP_SCAN_DEC_DIDX(v), 0);
        n++;
    }
    return n;
}

/* Main thread, the thread joined or never started: complete what the save
 * still holds as aborted (the dict the thread was reading, then every
 * remaining entry of the save's table: a pointer per slot), then every
 * store. Every slot is DONE after this. */
static void hybsnapScanFinishAll(hybsnapScanCtx *ctx) {
    int64_t cur;
    atomicGet(ctx->cur_slot, cur);
    if (cur >= 0) {
        atomicSet(ctx->cur_slot, -1);
        hybsnapScanCompleteSlot(ctx, HYBSNAP_SCAN_DEC_STORE((uint64_t)cur), HYBSNAP_SCAN_DEC_DIDX((uint64_t)cur), 1);
    }
    for (int s = 0; s < ctx->num_stores; s++) {
        hybsnapScanStore *st = &ctx->store[s];
        if (!st->frozen) continue;
        for (int didx = 0; st->nrefs > 0 && didx < st->num_dicts; didx++)
            if (st->dicts[didx]) hybsnapScanCompleteSlot(ctx, s, didx, 1);
        hybsnapScanStoreDone(ctx, s);   /* a no-op when the last completion did it */
    }
    ctx->ended = 1;
}

static int hybsnapScanIsFinished(hybsnapScanCtx *ctx) {
    int v;
    atomicGetWithSync(ctx->finished, v);
    return v;
}

static void hybsnapScanJoin(hybsnapScanCtx *ctx) {
    if (ctx->started && !ctx->joined) {
        pthread_join(ctx->tid, NULL);
        ctx->joined = 1;
        if (hybsnap_live == ctx) hybsnap_live = NULL;   /* lo stays where the thread left it */
    }
}

/* Main thread. Asks the thread to stop (it stops within one cursor step),
 * joins it, drains fully-scanned slots normally and completes every other
 * slot with aborted=1, unfreezing all dicts. */
static void hybsnapScanAbort(hybsnapScanCtx *ctx) {
    if (!ctx->started) {
        /* Frozen but no thread (fork failure path): unfreeze everything. */
        if (ctx->frozen_all) hybsnapScanFinishAll(ctx);
        return;
    }
    atomicSetWithSync(ctx->abort, 1);
    hybsnapScanJoin(ctx);
    /* Fully scanned slots complete normally. */
    hybsnapScanHandleCompletions(ctx);
    /* Everything else was interrupted. */
    hybsnapScanFinishAll(ctx);
}

/* Main thread. Joins the thread if still running (does not abort it: call
 * hybsnapScanAbort() first if the scan may be in progress) and frees the ctx.
 * Every slot must be DONE by then unless the ctx was never started. */
static void hybsnapScanRelease(hybsnapScanCtx *ctx) {
    if (ctx == NULL) return;
    hybsnapScanJoin(ctx);
    if (ctx->started)
        hybsnapScanWindowStats(ctx, &hybsnap_last_window.stalls, &hybsnap_last_window.stall_us, &hybsnap_last_window.grants);
    if (ctx->started) {
        /* Drain the tail, then complete the slots the thread left unfinished (it
         * stopped early on a broken channel) so no slot stays frozen. */
        hybsnapScanHandleCompletions(ctx);
        hybsnapScanFinishAll(ctx);
    }
    for (int s = 0; s < ctx->num_stores; s++)
        serverAssert(!ctx->store[s].frozen);   /* every slot completed: the guard is already off */
    for (int s = 0; s < ctx->num_stores; s++) zfree(ctx->store[s].dicts);
    if (ctx->frozen_all) hybsnapEpochEnd();
    zfree(ctx->store);
    zfree(ctx->ring);
    freeEventNotifier(ctx->en);
    zfree(ctx);
}

/* Readable fd that becomes ready whenever a slot completes or the thread
 * exits; register hybsnapScanAeHandler() on it with AE_READABLE and 'ctx' as
 * client data. -1 if unavailable. */
static int hybsnapScanReadEventFd(hybsnapScanCtx *ctx) {
    return ctx->en ? getReadEventFd(ctx->en) : -1;
}

static void hybsnapScanAeHandler(struct aeEventLoop *el, int fd, void *privdata, int mask) {
    UNUSED(el);
    UNUSED(fd);
    UNUSED(mask);
    hybsnapScanCtx *ctx = privdata;
    handleEventNotifier(ctx->en);
    hybsnapScanHandleCompletions(ctx);
}

/* Progress for INFO / tests: dicts allocated at freeze in the scanned
 * stores, dicts the thread passed, dicts finished (completed or skipped). */
static void hybsnapScanProgress(hybsnapScanCtx *ctx, long *slots_total, long *slots_scanned,
                      long *slots_done, long long *keys_dispatched)
{
    if (slots_total) *slots_total = ctx->slots_total;
    if (slots_scanned) atomicGet(ctx->slots_scanned, *slots_scanned);
    if (slots_done) *slots_done = ctx->slots_done;
    if (keys_dispatched) atomicGet(ctx->keys_dispatched, *keys_dispatched);
}

/* ============================================================================
 * Hybrid snapshot: orchestration, channel, child writer
 * ============================================================================
 *
 * Fork-instant sequence (main thread, nothing interleaves):
 *   1. capture per-db (and per-slot) key/expire counts for RESIZEDB/SLOT_INFO
 *   2. hybsnapScanFreeze(): one reference and a generation stamp per non-empty
 *      keyspace dict, a guard on every store; nothing is paused and nothing
 *      is allocated per dict yet (overlays attach on demand, at the first
 *      write that needs them); the save's epoch is taken
 *   3. redisFork(): the child inherits the HYBSNAP_FORK values (copy-on-write)
 *      but none of the DONTFORK memory (headers, NOFORK values, tables)
 *   4. hybsnapScanStartThread(): the scan thread walks the save's table of
 *      fork-instant dicts below hi and streams records to the child
 *
 * Two pipes to the child. The small channel carries BUFFER messages: 64 KB
 * staging buffers of pre-encoded records, produced by the scan thread and by
 * the main thread's emit-ahead (forwarded by the thread every W buckets
 * and at the end of each dict, in order). The big channel carries one KVOBJ message per
 * HYBSNAP_FORK key (a copy of the kvobj header; the child follows the value
 * pointer into its own snapshot), then END with both channels' message
 * counts. A BUFFER may depend on big values that must precede it in the
 * output (robj_off); the child holds it until they are written.
 *
 * The child owns the output: RDB header, SELECTDB/RESIZEDB/SLOT_INFO (or
 * SELECT in AOF command form) tracked per message, records, footer, fsync,
 * rename; or the replica sockets for a diskless sync. Completion and abort
 * both flow through the classic child machinery (checkChildrenDone -> the done
 * handler -> hybsnapOnChildEnd).
 */

static size_t hybsnap_last_retained_peak = 0;
static size_t hybsnap_last_queued_peak = 0;
static long long hybsnap_last_inplace_writes = 0;

static long long hybsnapSaveInplaceWrites(void) { return hybsnap_save ? hybsnap_save->inplace_writes : 0; }
static long long hybsnapSaveLastInplaceWrites(void) { return hybsnap_last_inplace_writes; }

int hybsnapIsEnabled(void) { return server.hybrid_snapshot; }

int hybsnapSaveIsInProgress(void) {
    return hybsnap_save != NULL && !hybsnap_save->aborted;
}

/* Bytes the overlay currently keeps alive for the running save (replaced or
 * deleted originals); the peak of the last completed save and the
 * hard-mark abort counter are reported next to it. */
static size_t hybsnapSaveRetainedBytes(void) { return hybsnap_save ? hybsnap_save->retained_bytes : 0; }
static size_t hybsnapSaveLastRetainedPeak(void) { return hybsnap_last_retained_peak; }
/* Fork-instant dicts their store let go of (freed, flushed, moved out by a
 * slot trim) before the save completed them: the save read them to the end
 * and freed them. */
static long hybsnapSaveRetainedDicts(void) { return hybsnap_stat_retained_dicts; }

/* Main-thread emission (emit-ahead): bytes queued for the scan thread to
 * forward to the child (main-thread queue plus the thread's outbound queue),
 * and the peak of the two over the last save. */
/* What the running save holds in the parent that the classic child would hold
 * in its own process, for the eviction calculation (evict.c): the records
 * queued for the child, the fork-instant originals the overlay keeps alive
 * while it reads them, and the fixed bookkeeping (the
 * per-slot size tables, the dict tables, the completion ring). Memory the
 * save holds for its duration and releases, not the dataset's: counting it
 * would evict keys during every save on an instance near its limit. */
size_t hybsnapSaveOverheadBytes(void) {
    hybsnapSave *sv = hybsnap_save;
    if (!sv) return 0;
    size_t bytes = hybsnapSaveQueuedBytes() + sv->retained_bytes + hybsnapScanTableBytes(sv->scan);
    bytes += (size_t)sv->dbnum * sizeof(hybsnapDbSizes);
    for (int j = 0; j < sv->dbnum; j++)
        bytes += (size_t)sv->sizes[j].nslots * 2 * sizeof(unsigned long long);
    if (sv->trim_slots) bytes += CLUSTER_SLOTS / 8;
    if (sv->slot_filter) bytes += CLUSTER_SLOTS / 8;
    return bytes;
}

static size_t hybsnapSaveQueuedBytes(void) {
    if (!hybsnap_save) return 0;
    size_t n, o;
    atomicGet(hybsnap_save->mq_bytes, n);
    atomicGet(hybsnap_save->oq_bytes, o);
    return n + o;
}
static size_t hybsnapSaveLastQueuedPeak(void) { return hybsnap_last_queued_peak; }
static long long hybsnapSaveEpochReallocs(void) { return hybsnap_save ? hybsnap_save->epoch_reallocs : 0; }
static long long hybsnapSaveKeysStamped(void) { long long n = 0; if (hybsnap_save) atomicGet(hybsnap_save->scan->keys_stamped, n); return n; }
static long long hybsnapSaveMainKeys(void) { long long n = 0; if (hybsnap_save) atomicGet(hybsnap_save->main_keys, n); return n; }


/* ---- INFO, DEBUG and the crash report (server.c, debug.c call these) ---- */

/* INFO memory: the arena's allocator statistics (sampled by cron next to the
 * default arena's) and its fork visibility. */
sds hybsnapInfoMemory(sds info) {
    return sdscatprintf(info, FMTARGS(
        "allocator_allocated_hybrid_arena:%zu\r\n", server.cron_malloc_stats.hybrid_arena_allocated,
        "allocator_active_hybrid_arena:%zu\r\n", server.cron_malloc_stats.hybrid_arena_active,
        "allocator_resident_hybrid_arena:%zu\r\n", server.cron_malloc_stats.hybrid_arena_resident,
        "allocator_frag_bytes_hybrid_arena:%zu\r\n", server.cron_malloc_stats.hybrid_arena_frag_smallbins_bytes,
        "snap_hybrid_arena_available:%d\r\n", hybsnapArenaExists(),
        "snap_hybrid_arena_forkable:%d\r\n", hybsnapArenaIsForkable(),
        "snap_hybrid_arena_va_reserved:%zu\r\n", hybsnapArenaVaReserved(),
        "snap_hybrid_arena_va_used:%zu\r\n", hybsnapArenaVaUsed()));
}

/* current_save_keys_processed during a hybrid save: the child sees only the
 * HYBSNAP_FORK keys, so the figure is what the scan thread and the main thread
 * dispatched so far; 'classic' otherwise. */
size_t hybsnapSaveKeysProcessed(size_t classic) {
    if (!hybsnapSaveIsInProgress()) return classic;
    long slots_total, slots_done;
    long long keys_nofork, keys_fork, bytes_sent;
    hybsnapSaveProgress(&slots_total, &slots_done, &keys_nofork, &keys_fork, &bytes_sent);
    return (size_t)(keys_nofork + keys_fork + hybsnapSaveMainKeys());
}

/* INFO persistence: the running save's progress and queues, the last save's
 * peaks and the counters across saves. */
sds hybsnapInfoPersistence(sds info) {
    long slots_total = 0, slots_done = 0;
    long long keys_nofork = 0, keys_fork = 0, bytes_sent = 0;
    hybsnapSaveProgress(&slots_total, &slots_done, &keys_nofork, &keys_fork, &bytes_sent);
    kvstoreOverlayStats os = kvstoreOverlayStatsGet();
    long long stalls, stall_us, grants;
    hybsnapScanWindowStats(hybsnap_save ? hybsnap_save->scan : NULL, &stalls, &stall_us, &grants);
    return sdscatprintf(info, FMTARGS(
        "snap_save_in_progress:%d\r\n", hybsnapSaveIsInProgress(),
        "snap_save_slots_total:%ld\r\n", slots_total,
        "snap_save_slots_done:%ld\r\n", slots_done,
        "snap_save_keys_nofork:%lld\r\n", keys_nofork,
        "snap_save_keys_fork:%lld\r\n", keys_fork,
        "snap_save_bytes_sent:%lld\r\n", bytes_sent,
        "snap_save_retained_bytes:%zu\r\n", hybsnapSaveRetainedBytes(),
        "snap_save_side_entries:%ld\r\n", hybsnapSaveSideEntries(),
        "snap_save_overlay_buckets:%lld\r\n", os.buckets,
        "snap_save_overlay_clones:%lld\r\n", os.clones,
        "snap_save_overlay_peak_buckets:%ld\r\n", os.peak,
        "snap_save_overlay_paused_ms:%lld\r\n", os.paused_us / 1000,
        "snap_save_scan_stalls:%lld\r\n", stalls,
        "snap_save_scan_stall_ms:%lld\r\n", stall_us / 1000,
        "snap_save_window_grants:%lld\r\n", grants,
        "snap_last_save_retained_peak:%zu\r\n", hybsnapSaveLastRetainedPeak(),
        "snap_save_queued_bytes:%zu\r\n", hybsnapSaveQueuedBytes(),
        "snap_last_save_queued_peak:%zu\r\n", hybsnapSaveLastQueuedPeak(),
        "snap_retained_dicts:%ld\r\n", hybsnapSaveRetainedDicts(),
        "snap_save_main_keys:%lld\r\n", hybsnapSaveMainKeys(),
        "snap_save_scan_stamped_keys:%lld\r\n", hybsnapSaveKeysStamped(),
        "snap_save_epoch_reallocs:%lld\r\n", hybsnapSaveEpochReallocs(),
        "snap_hard_mark_aborts:%lld\r\n", server.stat_hybsnap_hard_mark_aborts,
        "snap_saves:%lld\r\n", server.stat_hybsnap_saves,
        "snap_hybrid_blocked_by:%s\r\n", moduleHybridSnapshotBlockedBy(),
        "snap_save_inplace_writes:%lld\r\n", hybsnapSaveInplaceWrites(),
        "snap_last_save_inplace_writes:%lld\r\n", hybsnapSaveLastInplaceWrites(),
        "snap_inplace_writes_total:%lld\r\n", server.stat_hybsnap_inplace_writes));
}

/* The INFO sections a crash report may generate inside a child forked without
 * the keyspace (the hybrid save child, a REDISMODULE_FORK_NO_KEYSPACE module
 * fork: memory, keyspace, keysizes and hotkeys would fault); NULL in every
 * other process, meaning "all". */
const char *const *hybsnapCrashReportInfoSections(void) {
    static const char *const keep[] = {"server", "clients", "persistence", "stats", "replication", "threads",
                                       "cpu", "module_list", "errorstats", "cluster", NULL};
    return hybsnapIsChildNoKeyspace() ? keep : NULL;
}

static redisDb *hybsnapDebugDbArg(client *c) {
    if (c->argc == 2) return c->db;
    long long dbid;
    if (getLongLongFromObjectOrReply(c, c->argv[2], &dbid, NULL) != C_OK) return NULL;
    if (dbid < 0 || dbid >= server.dbnum) {
        addReplyError(c, "Out of range database");
        return NULL;
    }
    return &server.db[dbid];
}

/* DEBUG SNAP-PLACEMENT [dbid] | SNAP-DRIFT [dbid] | SNAP-WINDOW-BUCKETS [n].
 * Returns 1 when the subcommand was one of these (and replied), 0 otherwise. */
int hybsnapDebugCommand(client *c) {
    const char *sub = c->argv[1]->ptr;
    if (!strcasecmp(sub, "snap-placement") && (c->argc == 2 || c->argc == 3)) {
        redisDb *db = hybsnapDebugDbArg(c);
        if (!db) return 1;
        hybsnapPlacementStats st;
        hybsnapPlacementScan(db, &st);
        addReplyMapLen(c, 11);
        addReplyBulkCString(c, "keys"); addReplyLongLong(c, st.keys);
        addReplyBulkCString(c, "dict_tables_in_arena"); addReplyLongLong(c, st.dict_tables_in_arena);
        addReplyBulkCString(c, "dict_tables_outside"); addReplyLongLong(c, st.dict_tables_outside);
        addReplyBulkCString(c, "arena_available"); addReplyLongLong(c, hybsnapArenaExists());
        addReplyBulkCString(c, "headers_in_arena"); addReplyLongLong(c, st.headers_in_arena);
        addReplyBulkCString(c, "headers_outside"); addReplyLongLong(c, st.headers_outside);
        addReplyBulkCString(c, "class_nofork"); addReplyLongLong(c, st.class_nofork);
        addReplyBulkCString(c, "class_fork"); addReplyLongLong(c, st.class_fork);
        addReplyBulkCString(c, "drift_nofork_outside"); addReplyLongLong(c, st.drift_nofork_outside);
        addReplyBulkCString(c, "drift_fork_inside"); addReplyLongLong(c, st.drift_fork_inside);
        addReplyBulkCString(c, "violations"); addReplyLongLong(c, st.violations);
        return 1;
    }
    if (!strcasecmp(sub, "snap-drift") && (c->argc == 2 || c->argc == 3)) {
        redisDb *db = hybsnapDebugDbArg(c);
        if (db) hybsnapPlacementDriftReply(c, db, 50);
        return 1;
    }
    if (!strcasecmp(sub, "snap-window-buckets") && (c->argc == 2 || c->argc == 3)) {
        if (c->argc == 3) {
            long long n;
            if (getLongLongFromObjectOrReply(c, c->argv[2], &n, NULL) != C_OK) return 1;
            if (n < 1 || n > HYBSNAP_WINDOW_BUCKETS_MAX) {
                addReplyError(c, "Out of range");
                return 1;
            }
            hybsnapSetWindowBuckets((unsigned long)n);
        }
        addReplyLongLong(c, (long long)hybsnapGetWindowBuckets());
        return 1;
    }
    return 0;
}

/* ---- Bucket position and main-thread emission (emit-ahead) ---- */

/* Main thread, inside a write section: where the thread is relative to
 * bucket (table, idx) of dict d of slot (s, didx). Below lo it has read the
 * bucket; at or above hi it cannot reach it before the main thread raises hi;
 * in between it may be reading it. A dict the save holds but has not paused
 * is beyond hi as a whole. For an inside bucket and a non-NULL 'win', *win is
 * its overlay window: the aligned run of ov_block buckets around it (at most
 * the table), tagged with the position of its last bucket. */
static hybsnapBucketPos hybsnapBucketPosition(hybsnapScanCtx *ctx, int s, int didx, dict *d, int table, unsigned long idx, kvstoreOvWindow *win) {
    hybsnapSlotState st = hybsnapScanDictStateMain(ctx, s, didx, d);
    if (st >= HYBSNAP_SLOT_SCANNED) return HYBSNAP_POS_BEHIND;
    if (st == HYBSNAP_SLOT_PENDING && !hybsnapDictPaused(d)) return HYBSNAP_POS_AHEAD;
    uint64_t base = HYBSNAP_POS(hybsnapOrd(ctx, s, didx), 1 + table, 0);
    uint64_t p = base + idx;
    if (p < hybsnapPosLoLoad()) return HYBSNAP_POS_BEHIND;
    if (p >= ctx->hi) return HYBSNAP_POS_AHEAD;
    if (win) {
        unsigned long size = DICTHT_SIZE(d->ht_size_exp[table]);
        unsigned long n = ctx->ov_block < size ? ctx->ov_block : size;
        win->table = table;
        win->lo = idx & ~(n - 1);
        win->n = n;
        win->last = base + win->lo + n - 1;
    }
    return HYBSNAP_POS_INSIDE;
}

/* A message buffer holding only the header space; the payload is appended
 * and the header filled in when the message is handed over. */
static sds hybsnapMsgNew(size_t payload) {
    sds m = sdsMakeRoomForNonGreedy(sdsempty(), sizeof(hybsnapMsgHdr) + payload);
    sdsIncrLen(m, sizeof(hybsnapMsgHdr));
    return m;
}

/* mq_lock held: complete message 'm' (header space + payload) and queue it as
 * it will go on the pipe, without another copy. */
static void hybsnapSaveQueuePushLocked(hybsnapSave *sv, uint32_t type, int dbid, int slot, sds m, uint32_t robj_off, uint64_t src_addr) {
    hybsnapMsgHdr h = { type, dbid, slot, (uint32_t)(sdslen(m) - sizeof(hybsnapMsgHdr)), robj_off, src_addr };
    memcpy(m, &h, sizeof(h));
    listAddNodeTail(sv->mq, m);
    atomicIncr(sv->mq_bytes, sdsAllocSize(m));
}

/* Main thread, mq_lock held: hand the partial BUFFER over as a message. */
static void hybsnapSaveMainStageFlushLocked(hybsnapSave *sv) {
    if (sdslen(sv->mstage) <= sizeof(hybsnapMsgHdr)) return;
    hybsnapSaveQueuePushLocked(sv, HYBSNAP_MSG_BUFFER, sv->mstage_dbid, sv->mstage_slot, sv->mstage, 0, 0);
    sv->mstage = hybsnapMsgNew(0);
}

/* Cluster mode: is slot didx one an atomic slot migration was trimming at the
 * fork instant? Its keys are skipped by every producer (classic parity). */
static inline int hybsnapSaveSlotTrimmed(hybsnapSave *sv, int didx) {
    return sv->trim_slots && (sv->trim_slots[didx >> 3] & (1u << (didx & 7)));
}

/* One record of 'kv' in the save's format, from any producer (main thread,
 * scan thread, child): the RDB key-value pair, or for an AOF rewrite without
 * the preamble the command form rewriteAppendOnlyFileRio writes per key
 * (per-type rewriter, PEXPIREAT, BLESS, module key metadata). 0 on success. */
static int hybsnapSaveWriteRecord(hybsnapSave *sv, rio *r, kvobj *kv, int dbid) {
    robj key;
    if (sv->slots_form) return slotSnapshotSaveKeyValuePair(r, kv, dbid) == C_OK ? 0 : -1;
    initStaticStringObject(key, kvobjGetKey(kv));
    if (sv->aof_cmd_form) return rewriteObject(r, &key, kv, dbid, kvobjGetExpire(kv)) == C_OK ? 0 : -1;
    return rdbSaveKeyValuePair(r, &key, kv, kvobjGetExpire(kv), dbid) == -1 ? -1 : 0;
}

/* Main thread: serialize fork-instant key 'kv' of slot (s, didx) as it is
 * now, i.e. still at its fork-instant state (a key pending save is never
 * written in place before this). */
static void hybsnapSaveEmitKey(hybsnapSave *sv, int s, int didx, kvobj *kv) {
    if (hybsnapSaveSlotTrimmed(sv, didx)) {
        atomicIncr(sv->keys_skipped, 1);
        return;
    }
    hybsnap_main_emitting = 1;
    hybsnapValueClass cls = hybsnapKeyClass(kv);
    if (cls == HYBSNAP_NOFORK && getModuleMetaBits(kv->metabits)) cls = HYBSNAP_FORK;
    if (cls == HYBSNAP_FORK) {
        /* The child resolves kv->ptr in its own memory: it must be forkable. */
        char *alloc = kvobjGetAllocPtr(kv);
        debugServerAssert(kv->encoding == OBJ_ENCODING_INT || kv->encoding == OBJ_ENCODING_EMBSTR ||
                          !zmalloc_in_hybrid_arena(kv->ptr));
        size_t len = zmalloc_size(alloc);
        sds copy = hybsnapMsgNew(len);
        copy = sdscatlen(copy, alloc, len);
        pthread_mutex_lock(&sv->mq_lock);
        hybsnapSaveQueuePushLocked(sv, HYBSNAP_MSG_KVOBJ, s, didx, copy, (uint32_t)((char *)kv - alloc), (uint64_t)(uintptr_t)alloc);
        pthread_mutex_unlock(&sv->mq_lock);
    } else {
        /* The record is written to mscratch (a rio over the sds appends to
         * it) and joins the stage under the lock. */
        sdsclear(sv->mscratch);
        rio r;
        rioInitWithBuffer(&r, sv->mscratch);
        if (hybsnapSaveWriteRecord(sv, &r, kv, s) != 0)
            serverPanic("Hybrid snapshot: failed to serialize a key on the main thread");
        sv->mscratch = r.io.buffer.ptr;
        pthread_mutex_lock(&sv->mq_lock);
        if (sv->mstage_dbid != s || sv->mstage_slot != didx) {
            hybsnapSaveMainStageFlushLocked(sv);
            sv->mstage_dbid = s;
            sv->mstage_slot = didx;
        }
        sv->mstage = sdscatlen(sv->mstage, sv->mscratch, sdslen(sv->mscratch));
        /* mq_bytes counts the stage once, when it is handed over as a
         * message (hybsnapSaveQueuePushLocked), not per record. */
        if (sdslen(sv->mstage) - sizeof(hybsnapMsgHdr) >= HYBSNAP_STAGE_BYTES) hybsnapSaveMainStageFlushLocked(sv);
        pthread_mutex_unlock(&sv->mq_lock);
    }
    atomicIncr(sv->main_keys, 1);
    hybsnap_main_emitting = 0;
}

/* Scan thread: rdb-key-save-delay for 'keys' fork-instant keys. The classic
 * child sleeps it per key, so a save of N keys takes N delays; here the
 * thread sleeps it for every key of the fork instant whoever produced the
 * record (itself, the child it shipped a big value to, or the main thread's
 * emit-ahead, which must not slow the command that triggered it, so its
 * share is slept before END), in slices so an abort or CONFIG SET
 * rdb-key-save-delay 0 is seen within 10 ms. */
static int hybsnapSaveKeyDelay(hybsnapSave *sv, long long keys) {
    int slept = 0;
    while (keys-- > 0 && !hybsnapScanAborted(sv->scan)) {
        int delay;
        atomicGet(sv->key_save_delay, delay);
        if (delay <= 0) return slept;
        slept = 1;
        while (delay > 0 && !hybsnapScanAborted(sv->scan)) {
            int slice = delay > 10000 ? 10000 : delay;
            debugDelay(slice);
            delay -= slice;
        }
    }
    return slept;
}

/* Scan thread, every W buckets, at the end of each dict and before END:
 * forward what the main thread emitted. */
static void hybsnapSaveWindowDone(void *priv) {
    hybsnapSave *sv = priv;
    list *ready = sv->mq_ready;   /* empty: drained by the previous call */
    pthread_mutex_lock(&sv->mq_lock);
    hybsnapSaveMainStageFlushLocked(sv);
    sv->mq_ready = sv->mq;
    sv->mq = ready;
    ready = sv->mq_ready;
    pthread_mutex_unlock(&sv->mq_lock);
    if (listLength(ready)) hybsnapStageFlush(sv);   /* this thread's partial buffer first (order is free anyway) */
    listNode *ln;
    while ((ln = listFirst(ready)) != NULL) {
        sds m = listNodeValue(ln);
        size_t a = sdsAllocSize(m);
        listDelNode(ready, ln);
        if (hybsnapScanAborted(sv->scan)) {
            sdsfree(m);
            atomicDecr(sv->mq_bytes, a);
        } else {
            uint32_t type;
            memcpy(&type, m, sizeof(type));
            hybsnapOutEnqueue(sv, m, type);   /* the queue owns m now: it may be sent and freed already */
            atomicDecr(sv->mq_bytes, a);
        }
    }
}

/* Main thread: the keyspace's other references to a kvobj now go to 'to', its
 * replacement for the same key in slot didx of db (a clone, or a copy with an
 * epoch): db->expires, db->subexpires and the slot's size accounting. The
 * db->keys entry is the caller's. */
static void hybsnapKeyRepoint(redisDb *db, int didx, kvobj *from, kvobj *to) {
    if (server.memory_tracking_enabled)
        updateSlotAllocSize(db, didx, to, kvobjAllocSize(from), kvobjAllocSize(to));
    sds key = kvobjGetKey(from);
    if (kvobjGetExpire(from) != -1) {
        dictEntryLink link = kvstoreDictFindLink(db->expires, didx, key, NULL);
        if (link) kvstoreDictSetAtLink(db->expires, didx, to, &link, 0);
    }
    if (from->type == OBJ_HASH) {
        /* Writes trash=1 into the original's ExpireMeta; the scan thread
         * never reads meta (it recomputes the min field TTL from the
         * listpack), so this is tolerated. */
        uint64_t sub = estoreRemove(db->subexpires, didx, from);
        if (sub != EB_EXPIRE_TIME_INVALID) estoreAdd(db->subexpires, didx, to, sub);
    }
}

/* Main thread, inside a write section: the frozen key at 'link' of dict d,
 * slot (s, didx), in a bucket ahead of the thread and just emitted, gets the
 * save's epoch, so the thread will skip it. In place when its allocation has
 * room; else the key moves to a copy with the epoch (the value is not
 * copied), every reference is repointed and the old allocation is freed at
 * the end of the section, so a caller still holding the old pointer reads
 * valid memory meanwhile (a write through it would be lost, as with a clone:
 * writes go through the funnel). Returns the kvobj at the link now. */
static kvobj *hybsnapKvobjStamp(hybsnapSave *sv, int s, int didx, dict *d, dictEntryLink link) {
    kvobj *kv = dictGetKV(*link);
    if (kvobjBits(kv)->has_epoch) {
        kvobjSetEpoch(kv, sv->scan->epoch);
        return kv;
    }
    redisDb *db = hybsnapStoreOwnerDb(sv, s);
    size_t oldsize = (db && server.memory_tracking_enabled) ? kvobjAllocSize(kv) : 0;
    kvobj *nkv = kvobjAddEpoch(kv, sv->scan->epoch);
    if (nkv == kv) {
        /* In place: the accounted size grew by the epoch (not for an EMBSTR, whose value gave up the bytes). */
        if (db && server.memory_tracking_enabled) updateSlotAllocSize(db, didx, kv, oldsize, kvobjAllocSize(kv));
        return kv;
    }
    sv->epoch_reallocs++;
    dictSetKeyAtLink(d, nkv, &link, 0);
    if (db) hybsnapKeyRepoint(db, didx, kv, nkv);
    hybsnapRetire(kvobjGetAllocPtr(kv));
    return nkv;
}

/* Overlay 'prepare' hook (privdata = the store's hybsnapGuardPriv): may the
 * frozen chain of 'key' be written in place? Defers the bucket, in the window
 * *win, when the thread may be reading it. Ahead of the thread, a key the
 * save has not serialized yet is emitted first and gets the save's epoch, so
 * the thread skips it; the other keys of the bucket stay for the thread, and
 * a key the write adds is newer than the fork instant (stamped at creation).
 * Behind the thread, and for a key already stamped, the write is in place. */
static kvstoreOverlayPrepareResult hybsnapOverlayPrepare(void *privdata, int didx, dict *d, const void *key, kvstoreOvWindow *win) {
    int s = ((struct hybsnapGuardPriv *)privdata)->s;
    hybsnapSave *sv = hybsnap_save;
    if (!sv || sv->aborted) return KVSTORE_OV_INPLACE;
    int table;
    unsigned long idx;
    dictKeyBucket(d, key, &table, &idx);
    hybsnapBucketPos pos = hybsnapBucketPosition(sv->scan, s, didx, d, table, idx, win);
    if (pos == HYBSNAP_POS_INSIDE) return KVSTORE_OV_DEFER;
    if (pos == HYBSNAP_POS_AHEAD) {
        /* The layout holds still for the decision (kvsoWriteBucket pauses
         * rehashing), so the link stays valid. */
        dictEntryLink link = dictFindLink(d, key, NULL);
        if (link && kvobjGetEpoch(dictGetKV(*link)) < sv->scan->epoch) {
            hybsnapSaveEmitKey(sv, s, didx, dictGetKV(*link));
            hybsnapKvobjStamp(sv, s, didx, d, link);
            return KVSTORE_OV_INPLACE;
        }
    }
    sv->inplace_writes++;
    server.stat_hybsnap_inplace_writes++;
    return KVSTORE_OV_INPLACE;
}

/* Main thread, inside a write section at a point where no caller holds a
 * link: write back the overlayed buckets the scan thread has passed, i.e.
 * those of the overlay windows wholly below lo. Called at the end of every
 * section and from cron; a no-op until lo has moved since the last drain. */
static uint64_t hybsnap_drain_lo = 0;
static void hybsnapSectionDrain(void) {
    hybsnapSave *sv = hybsnap_save;
    if (!sv || sv->aborted) return;
    uint64_t lo = hybsnapPosLoLoad();
    if (lo == hybsnap_drain_lo) return;
    hybsnap_drain_lo = lo;
    kvstoreOverlayDrain(lo);
}

/* Overlayed buckets of the running save. */
static long hybsnapSaveSideEntries(void) {
    hybsnapSave *sv = hybsnap_save;
    if (!sv || sv->aborted) return 0;
    return kvstoreOverlayPending();
}

/* ---- Stores versus dbs (SWAPDB, FLUSHDB during a save) ---- */

/* Store index of a kvstore being scanned, -1 if the save does not own it
 * (a keyspace created after the fork, e.g. by a FLUSHDB during the save). */
static int hybsnapStoreIndexOf(hybsnapSave *sv, kvstore *kvs) {
    for (int s = 0; s < sv->dbnum; s++)
        if (sv->stores[s] && sv->stores[s] == kvs) return s;
    return -1;
}

/* The db that holds store s right now, NULL when none (FLUSHDB detached it). */
static redisDb *hybsnapStoreOwnerDb(hybsnapSave *sv, int s) {
    if (!sv->stores[s]) return NULL;   /* flushed and already freed */
    for (int j = 0; j < server.dbnum; j++)
        if (server.db[j].keys == sv->stores[s]) return &server.db[j];
    return NULL;
}

/* FLUSHDB and SWAPDB during a save. A store is a kvstore the save is
 * scanning, identified by the db id it had at the fork instant. */
int hybsnapSaveOwnsKvstore(kvstore *kvs) {
    hybsnapSave *sv = hybsnap_save;
    if (!sv || sv->aborted) return 0;
    return hybsnapStoreIndexOf(sv, kvs) >= 0;
}

/* FLUSHDB during a save (db.c emptyDbStructure): the db swapped in fresh,
 * empty structures and hands its fork-instant ones to the save. The scan
 * thread keeps walking 'keys' (the live db is already empty for everyone
 * else); everything is freed on BIO when the store completes. Returns 1
 * when adopted, 0 when the caller must free them itself. */
int hybsnapSaveDetachDb(kvstore *keys, kvstore *expires, void *subexpires, dict *stream_idmp_keys, kvstore *blessed) {
    hybsnapSave *sv = hybsnap_save;
    if (!sv || sv->aborted) return 0;
    int s = hybsnapStoreIndexOf(sv, keys);
    if (s < 0 || sv->flushed[s].set) return 0;
    sv->flushed[s] = (hybsnapFlushedDb){ 1, keys, expires, blessed, subexpires, stream_idmp_keys };
    serverLog(LL_NOTICE, "Hybrid snapshot: db %d flushed during the save, its keyspace stays with the save until its slots complete", s);
    return 1;
}

static void hybsnapSaveFreeFlushed(hybsnapSave *sv, int s) {
    hybsnapFlushedDb *f = &sv->flushed[s];
    if (!f->set) return;
    f->set = 0;
    /* The store is gone for us from here on (INFO counters, store lookups). */
    sv->stores[s] = NULL;
    emptyDbDataAsync(f->keys, f->expires, f->subexpires, f->stream_idmp_keys, f->blessed, NULL, NULL);
}

/* Scan callback: every slot of store s is DONE (completed or aborted). */
static void hybsnapSaveStoreDone(void *privdata, int s) {
    hybsnapSave *sv = privdata;
    hybsnapSaveFreeFlushed(sv, s);
}

/* Is 'kv' pending save (1): a fork-instant object the save has not serialized
 * yet, so it must not be mutated in place? True when its bucket is ahead of the
 * thread and it does not carry this save's epoch, or when the thread may be
 * reading the bucket; never for a clone in an overlayed bucket. Per object,
 * unlike the slot state PENDING.
 *
 * Opens the command's write section, so the answer holds until the command
 * ends: a 0 stays 0, a 1 can only turn stale in the safe direction. A read path
 * that may skip its mutation (lazy field expiry) skips it on 1; one that must
 * mutate calls hybsnapPrepareMutation() instead. */
int hybsnapKvobjIsPendingSave(redisDb *db, kvobj *kv) {
    hybsnapSave *sv = hybsnap_save;
    if (!sv || sv->aborted) return 0;
    int s = hybsnapStoreIndexOf(sv, db->keys);
    if (s < 0) return 0;   /* a keyspace created after the fork: nothing in it is pending save */
    sds key = kvobjGetKey(kv);
    int slot = server.cluster_enabled ? calculateKeySlot(key) : 0;
    dict *d = kvstoreGetDict(db->keys, slot);
    if (!d) return 0;
    hybsnapSlotState st = hybsnapScanDictStateMain(sv->scan, s, slot, d);
    if (st == HYBSNAP_SLOT_SCANNED || st == HYBSNAP_SLOT_DONE) return 0;
    hybsnapSectionOpen();   /* the cap: positions below lo stay behind, hi moves only by our hand */
    int table;
    unsigned long idx;
    dictKeyBucket(d, key, &table, &idx);
    if (hybsnapBucketPosition(sv->scan, s, slot, d, table, idx, NULL) == HYBSNAP_POS_BEHIND) return 0;
    /* Emitted ahead of the thread (whatever the position now: hi may have
     * passed it since), or newer than the fork instant: the thread skips it. */
    if (kvobjGetEpoch(kv) >= sv->scan->epoch) return 0;
    /* Ahead and not emitted, or being read: the frozen entry itself is pending
     * save, its clone in an overlayed bucket is not. */
    dictEntryLink l = dictFindLink(d, key, NULL);
    return l && dictGetKV(*l) == kv;
}

/* A read-path command about to write 'kv' in place (PFCOUNT's cache, a module
 * setting LRU/LFU through a READ handle): if the object is pending save, take the key
 * through the write funnel (a write lookup that emits or clones its bucket)
 * and return the object to write, which may be the clone; else 'kv'. NULL
 * only if the key is gone. */
kvobj *hybsnapPrepareMutation(redisDb *db, robj *key, kvobj *kv) {
    if (!hybsnapKvobjIsPendingSave(db, kv)) return kv;
    return lookupKeyWriteWithFlags(db, key, LOOKUP_NOTOUCH|LOOKUP_NONOTIFY|LOOKUP_NOSTATS|LOOKUP_NOEXPIRE);
}

/* Debug-build check at the write funnels: is 'kv' pending save with a value
 * that may move on write? A HYBSNAP_FORK value whose top-level pointer never
 * moves (stream, hashtable, skiplist, quicklist, module type) may be written in
 * place while pending save: the child serializes its own fork-instant image,
 * and a later header copy still points at the same object (its LRU may be a
 * little stale). XACK/XACKDEL rely on this: they reach streams through a read
 * lookup. Single-allocation values (raw strings, listpacks, intsets) may move
 * and stay checked. */
int hybsnapKvobjIsPendingSaveMovable(redisDb *db, kvobj *kv) {
    if (kv->type == OBJ_STREAM || kv->type == OBJ_MODULE ||
        kv->encoding == OBJ_ENCODING_HT || kv->encoding == OBJ_ENCODING_SKIPLIST ||
        kv->encoding == OBJ_ENCODING_QUICKLIST)
        return 0;
    return hybsnapKvobjIsPendingSave(db, kv);
}

/* CONFIG SET rdb-key-save-delay apply hook: reaches the running scan thread. */
void hybsnapSaveSetKeySaveDelay(int usec) {
    if (hybsnap_save) atomicSet(hybsnap_save->key_save_delay, usec);
}

/* Bytes of an original the overlay keeps alive: its kvobj allocation plus,
 * when the clone took a private copy, its value allocation(s). */
static size_t hybsnapOriginalRetainedBytes(kvobj *orig, int value_shared) {
    size_t n = zmalloc_size(kvobjGetAllocPtr(orig));
    if (value_shared || hybsnapValueIsInline(orig->type, orig->encoding)) return n;
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
 * and arm the hard-mark abort. The abort itself runs from hybsnapSaveCron():
 * we are inside a keyspace write here and the overlay must stay intact. */
static void hybsnapSaveRetainedAdd(kvobj *orig, int value_shared) {
    if (!hybsnap_save || hybsnap_save->aborted) return;
    hybsnapSave *sv = hybsnap_save;
    sv->retained_bytes += hybsnapOriginalRetainedBytes(orig, value_shared);
    if (sv->retained_bytes > sv->retained_peak) sv->retained_peak = sv->retained_bytes;
    if (server.hybrid_snapshot_max_retained_bytes && !sv->hard_mark_hit &&
        sv->retained_bytes > server.hybrid_snapshot_max_retained_bytes) {
        sv->hard_mark_hit = 1;
        serverLog(LL_WARNING, "Hybrid snapshot: overlay retains %zu bytes, past hybrid-snapshot-max-retained-bytes (%zu): aborting the save",
                  sv->retained_bytes, server.hybrid_snapshot_max_retained_bytes);
    }
}

static void hybsnapSaveRetainedSub(kvobj *orig, int value_shared) {
    if (!hybsnap_save) return;
    size_t n = hybsnapOriginalRetainedBytes(orig, value_shared);
    hybsnap_save->retained_bytes = n > hybsnap_save->retained_bytes ? 0 : hybsnap_save->retained_bytes - n;
}

/* Main thread, serverCron/beforeSleep: act on a pending hard-mark abort. */
void hybsnapSaveCron(void) {
    if (!hybsnap_save || hybsnap_save->aborted) return;
    hybsnapSave *sv = hybsnap_save;
    /* Writes may have stopped since the thread passed a bucket: drain from
     * here too, so an overlayed bucket never outlives that by more than a tick. */
    if (hybsnapSaveSideEntries()) {
        hybsnapSectionOpen();
        hybsnapSectionDrain();
        hybsnapSectionClose();
    }
    size_t queued;
    atomicGet(sv->mq_bytes, queued);
    /* The peak reported by INFO covers every kind of save-time memory: the
     * overlay's retained originals and the records queued for the child on
     * both threads. The abort below counts only what the main thread holds:
     * the scan thread's queue waits for the child instead of growing. */
    size_t all_queued = hybsnapSaveQueuedBytes();
    if (sv->retained_bytes + all_queued > sv->retained_peak) sv->retained_peak = sv->retained_bytes + all_queued;
    if (server.hybrid_snapshot_max_retained_bytes && !sv->hard_mark_hit) {
        if (sv->retained_bytes + queued > server.hybrid_snapshot_max_retained_bytes) {
            sv->hard_mark_hit = 1;
            serverLog(LL_WARNING, "Hybrid snapshot: %zu bytes retained plus %zu queued for the child, past hybrid-snapshot-max-retained-bytes (%zu): aborting the save",
                      sv->retained_bytes, queued, server.hybrid_snapshot_max_retained_bytes);
        }
    }
    if (!sv->hard_mark_hit) return;
    hybsnap_save->failed = 1;
    server.stat_hybsnap_hard_mark_aborts++;
    killRDBChild();   /* -> hybsnapSaveAbort(): drains the overlays; the done handler releases */
}

static void hybsnapSaveProgress(long *slots_total, long *slots_done, long long *keys_nofork,
                      long long *keys_fork, long long *bytes_sent)
{
    *slots_total = *slots_done = 0;
    *keys_nofork = *keys_fork = *bytes_sent = 0;
    if (!hybsnap_save || hybsnap_save->aborted) return;
    hybsnapScanProgress(hybsnap_save->scan, slots_total, NULL, slots_done, NULL);
    atomicGet(hybsnap_save->keys_nofork, *keys_nofork);
    atomicGet(hybsnap_save->keys_fork, *keys_fork);
    atomicGet(hybsnap_save->bytes_sent, *bytes_sent);
}

static int hybsnapReadAll(int fd, void *p, size_t n) {
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

/* ---- Outbound queue to the child (scan thread only) ---- */

/* Scan thread: a message buffer with room for 'need' bytes. Allocation is
 * exact (no greedy doubling: these bytes are what the queue holds against the
 * bound and what used_memory reports) and the pool serves staging-sized
 * requests only, so a small KVOBJ or END message never pins a 64 KB buffer
 * in the queue with a few bytes in it. */
static sds hybsnapOutTake(hybsnapSave *sv, size_t need) {
    listNode *ln;
    if (need >= HYBSNAP_STAGE_BYTES && (ln = listFirst(sv->oq_pool)) != NULL) {
        sds m = listNodeValue(ln);
        listDelNode(sv->oq_pool, ln);
        return sdsMakeRoomForNonGreedy(m, need);
    }
    return sdsMakeRoomForNonGreedy(sdsempty(), need);
}

/* Scan thread: a drained message goes back to the pool when it is a staging
 * buffer, is freed otherwise. */
static void hybsnapOutRecycle(hybsnapSave *sv, sds m) {
    size_t a = sdsalloc(m);
    if (a >= HYBSNAP_STAGE_BYTES && a <= 4 * HYBSNAP_STAGE_BYTES && listLength(sv->oq_pool) < HYBSNAP_OUT_POOL_MAX) {
        sdsclear(m);
        listAddNodeTail(sv->oq_pool, m);
    } else {
        sdsfree(m);
    }
}

static void hybsnapOutPublish(hybsnapSave *sv) {
    atomicSet(sv->shared->small_queued, sv->sq_bytes);
}

/* Scan thread: write one channel's queued messages to its (non-blocking)
 * pipe. 0 = the list is empty, 1 = the pipe is full, -1 = broken (child gone). */
static int hybsnapOutDrainList(hybsnapSave *sv, list *l, int fd, size_t *head_off, int small) {
    listNode *ln;
    while ((ln = listFirst(l)) != NULL) {
        sds m = listNodeValue(ln);
        size_t len = sdslen(m);
        ssize_t w = write(fd, m + *head_off, len - *head_off);
        if (w < 0) {
            if (errno == EINTR) continue;
            if (errno == EAGAIN || errno == EWOULDBLOCK) return 1;
            return -1;
        }
        *head_off += w;
        if (*head_off == len) {
            size_t a = sdsAllocSize(m);
            atomicDecr(sv->oq_bytes, a);
            if (small) {
                sv->sq_bytes -= a;
                hybsnapOutPublish(sv);
            }
            listDelNode(l, ln);
            hybsnapOutRecycle(sv, m);
            *head_off = 0;
        }
    }
    return 0;
}

/* Scan thread: push both pipes. Returns -1 when a pipe is broken (child
 * gone) or a BOUND wait saw an abort. */
static int hybsnapOutDrain(hybsnapSave *sv, hybsnapDrainMode mode, size_t target) {
    for (;;) {
        int rs = hybsnapOutDrainList(sv, sv->oq, sv->pipe_wr, &sv->oq_head_off, 1);
        int rb = hybsnapOutDrainList(sv, sv->bq, sv->big_wr, &sv->bq_head_off, 0);
        if (rs < 0 || rb < 0) return -1;
        if (rs == 0 && rb == 0) return 0;
        if (mode == HYBSNAP_DRAIN_TRY) return 0;
        if (mode == HYBSNAP_DRAIN_BOUND) {
            size_t q;
            atomicGet(sv->oq_bytes, q);
            if (q <= target) return 0;
        }
        /* An aborted save must not wait for the child: the parent aborts
         * before it kills the child (killRDBChild), and a child blocked on
         * an rdb pipe nobody reads any more never drains our pipes (the
         * 'all replicas drop during rdb pipe' test hung here once the big
         * channel's 64 KB pipe was full of KVOBJ headers). What did not fit
         * is moot; the child dies on SIGUSR1 or sees the pipes close. */
        if (hybsnapScanAborted(sv->scan)) return -1;
        struct pollfd pfd[2];
        int n = 0;
        if (rs == 1) pfd[n++] = (struct pollfd){ .fd = sv->pipe_wr, .events = POLLOUT, .revents = 0 };
        if (rb == 1) pfd[n++] = (struct pollfd){ .fd = sv->big_wr, .events = POLLOUT, .revents = 0 };
        int r = poll(pfd, n, 100);
        if (r < 0 && errno != EINTR) return -1;
        for (int i = 0; r > 0 && i < n; i++)
            if ((pfd[i].revents & (POLLERR | POLLHUP)) && !(pfd[i].revents & POLLOUT)) return -1;
    }
}

static void hybsnapOutClearList(list *l) {
    listNode *ln;
    while ((ln = listFirst(l)) != NULL) {
        sdsfree(listNodeValue(ln));
        listDelNode(l, ln);
    }
}

static void hybsnapOutClear(hybsnapSave *sv) {
    hybsnapOutClearList(sv->oq);
    hybsnapOutClearList(sv->bq);
    sv->oq_head_off = sv->bq_head_off = 0;
    sv->sq_bytes = 0;
    atomicSet(sv->oq_bytes, 0);
    hybsnapOutPublish(sv);
}

/* Scan thread: queue a complete message (header + payload in 'm', owned by
 * the queue from here on) on its channel and push the pipes. Past the bound
 * the thread waits for the child rather than growing further; it never aborts
 * for that (the main-thread hard mark covers what the main thread retains and
 * queues). A broken pipe (child gone) aborts the scan. */
static int hybsnapOutEnqueue(hybsnapSave *sv, sds m, uint32_t type) {
    size_t n = sdslen(m);
    int big = (type == HYBSNAP_MSG_KVOBJ || type == HYBSNAP_MSG_END || type == HYBSNAP_MSG_ABORT);
    if (type == HYBSNAP_MSG_BUFFER) {
        if (sv->tag_all) {
            /* Debug: depend on every big value sent so far (inline replication
             * would set the exact dependency per command). */
            hybsnapMsgHdr h;
            memcpy(&h, m, sizeof(h));
            h.robj_off = sv->kv_seq;
            memcpy(m, &h, sizeof(h));
        }
    }
    if (big) {
        listAddNodeTail(sv->bq, m);
        if (type == HYBSNAP_MSG_KVOBJ) sv->kv_seq++;
    } else {
        listAddNodeTail(sv->oq, m);
        sv->sq_bytes += sdsAllocSize(m);
        sv->small_seq++;
        hybsnapOutPublish(sv);
    }
    size_t q, mqb, peak;
    atomicIncrGet(sv->oq_bytes, q, sdsAllocSize(m));   /* what the queues really hold */
    atomicGet(sv->mq_bytes, mqb);
    atomicGet(sv->queued_peak, peak);
    if (q + mqb > peak) atomicSet(sv->queued_peak, q + mqb);
    atomicIncr(sv->bytes_sent, (long long)n);
    int rc = hybsnapOutDrain(sv, HYBSNAP_DRAIN_TRY, 0);
    if (rc == 0 && sv->oq_limit) {
        atomicGet(sv->oq_bytes, q);
        if (q > sv->oq_limit) rc = hybsnapOutDrain(sv, HYBSNAP_DRAIN_BOUND, sv->oq_limit);
    }
    if (rc != 0) {
        hybsnapScanRequestAbort(sv->scan);
        return -1;
    }
    return 0;
}

/* Scan thread: one message from a header and a payload it does not own. */
static int hybsnapSendMsg(hybsnapSave *sv, uint32_t type, int dbid, int slot,
                       const void *payload, uint32_t len, uint32_t robj_off, uint64_t src_addr)
{
    hybsnapMsgHdr h = { type, dbid, slot, len, robj_off, src_addr };
    sds m = hybsnapOutTake(sv, sizeof(h) + len);
    m = sdscatlen(m, &h, sizeof(h));
    if (len) m = sdscatlen(m, payload, len);
    return hybsnapOutEnqueue(sv, m, type);
}

/* The staging buffer is a BUFFER message in the making: its first
 * sizeof(hybsnapMsgHdr) bytes are reserved for the header, the records follow, and
 * the flush hands the whole buffer to the queue without a copy. */
static void hybsnapStageReset(hybsnapSave *sv) {
    sv->stage = hybsnapOutTake(sv, sizeof(hybsnapMsgHdr) + HYBSNAP_STAGE_BYTES + HYBSNAP_STAGE_SLACK);
    sdsIncrLen(sv->stage, sizeof(hybsnapMsgHdr));
}

static void hybsnapStageFlush(hybsnapSave *sv) {
    if (sdslen(sv->stage) <= sizeof(hybsnapMsgHdr)) return;
    hybsnapMsgHdr h = { HYBSNAP_MSG_BUFFER, sv->stage_dbid, sv->stage_slot,
                     (uint32_t)(sdslen(sv->stage) - sizeof(hybsnapMsgHdr)), 0, 0 };
    memcpy(sv->stage, &h, sizeof(h));
    hybsnapOutEnqueue(sv, sv->stage, HYBSNAP_MSG_BUFFER);
    hybsnapStageReset(sv);
}

/* Scan thread: one fork-instant key. Read-only on the keyspace. */
static void hybsnapSaveDispatch(void *priv, int s, int didx, kvobj *kv) {
    hybsnapSave *sv = priv;
    if (hybsnapSaveSlotTrimmed(sv, didx)) {
        atomicIncr(sv->keys_skipped, 1);
        return;
    }
    if (sv->stage_dbid != s || sv->stage_slot != didx) {
        hybsnapStageFlush(sv);
        sv->stage_dbid = s;
        sv->stage_slot = didx;
    }
    hybsnapValueClass cls = hybsnapKeyClass(kv);
    /* Module key metadata is serialized through module callbacks: child only. */
    if (cls == HYBSNAP_NOFORK && getModuleMetaBits(kv->metabits)) cls = HYBSNAP_FORK;

    if (cls == HYBSNAP_FORK) {
        hybsnapStageFlush(sv);   /* keep the child's stream in scan order */
        char *alloc = kvobjGetAllocPtr(kv);
        size_t len = zmalloc_size(alloc);
        /* The child resolves kv->ptr in its own memory: it must be forkable. */
        debugServerAssert(kv->encoding == OBJ_ENCODING_INT || kv->encoding == OBJ_ENCODING_EMBSTR ||
                          !zmalloc_in_hybrid_arena(kv->ptr));
        hybsnapSendMsg(sv, HYBSNAP_MSG_KVOBJ, s, didx, alloc, len, (uint32_t)((char *)kv - alloc), (uint64_t)(uintptr_t)alloc);
        atomicIncr(sv->keys_fork, 1);
        hybsnapSaveKeyDelay(sv, 1);
    } else {
        rio r;
        rioInitWithBuffer(&r, sv->stage);
        if (hybsnapSaveWriteRecord(sv, &r, kv, s) != 0) {
            serverLog(LL_WARNING, "Hybrid snapshot: failed to serialize a key on the scan thread");
            hybsnapScanRequestAbort(sv->scan);
        }
        sv->stage = r.io.buffer.ptr;
        atomicIncr(sv->keys_nofork, 1);
        /* Under rdb-key-save-delay the records stream one key at a time and
         * the delay follows the key, as the classic child does (write, flush,
         * sleep): tests pace a transfer with it and cancel it after the first
         * key arrived. */
        int delay;
        atomicGet(sv->key_save_delay, delay);
        if (delay > 0) {
            hybsnapStageFlush(sv);
            hybsnapSaveKeyDelay(sv, 1);
        } else if (sdslen(sv->stage) + HYBSNAP_STAGE_SLACK > sdsalloc(sv->stage)) {
            hybsnapStageFlush(sv);
        }
    }
}

/* Scan thread, before the first slot: publish the frozen config to this thread. */
static void hybsnapSaveThreadInit(void *priv) {
    hybsnapSave *sv = priv;
    hybsnap_scan_maxmemory_policy = sv->maxmemory_policy;
    hybsnap_scan_rdb_compression = sv->rdb_compression;
}


/* Scan thread: after the last slot (or on abort). */
static void hybsnapSaveThreadDone(void *priv, int aborted) {
    hybsnapSave *sv = priv;
    if (aborted) {
        /* Whatever is still queued is moot; the child must see ABORT (or die
         * first, which breaks the pipe and ends the wait). */
        hybsnapOutClear(sv);
        hybsnapSendMsg(sv, HYBSNAP_MSG_ABORT, -1, -1, NULL, 0, 0, 0);
        hybsnapOutDrain(sv, HYBSNAP_DRAIN_ALL, 0);
    } else {
        /* The delay due for the keys the main thread emitted ahead (their
         * records were forwarded as the thread went, so the drain was never
         * held back): slept here, before END, so a save of N fork-instant keys
         * still lasts N delays whoever produced the records. */
        long long mk;
        atomicGet(sv->main_keys, mk);
        if (mk > sv->delayed_main_keys) {
            hybsnapSaveKeyDelay(sv, mk - sv->delayed_main_keys);
            sv->delayed_main_keys = mk;
        }
        if (hybsnapScanAborted(sv->scan)) return;
        hybsnapStageFlush(sv);
        /* END carries both channels' message counts: the child finishes when
         * it has consumed them. */
        if (hybsnapSendMsg(sv, HYBSNAP_MSG_END, -1, -1, NULL, 0, sv->kv_seq, sv->small_seq) == 0 &&
            hybsnapOutDrain(sv, HYBSNAP_DRAIN_ALL, 0) != 0)
            hybsnapScanRequestAbort(sv->scan);
    }
}

/* ---- child ---- */

static int hybsnapChildSelect(rio *rdb, hybsnapSave *sv, int dbid, int slot, int *cur_db, int *cur_slot) {
    if (sv->slots_form) {
        /* Slot snapshot stream: SELECT on a db change; the slot info once per
         * slot, before its first record, whichever producer delivers it (the
         * fork-instant sizes the save recorded, as the classic child reads them). */
        static uint8_t announced[CLUSTER_SLOTS / 8];
        if (dbid != *cur_db) {
            static const char selectcmd[] = "*2\r\n$6\r\nSELECT\r\n";
            if (rioWrite(rdb, selectcmd, sizeof(selectcmd) - 1) == 0) return -1;
            if (rioWriteBulkLongLong(rdb, dbid) == 0) return -1;
            *cur_db = dbid;
        }
        if (!(announced[slot >> 3] & (1u << (slot & 7)))) {
            if (slotSnapshotWriteSlotInfo(rdb, slot, sv->sizes[dbid].slot_keys[slot],
                                          sv->sizes[dbid].slot_expires[slot]) == C_ERR) return -1;
            announced[slot >> 3] |= 1u << (slot & 7);
        }
        *cur_slot = slot;
        return 0;
    }
    if (sv->aof_cmd_form) {
        /* AOF command form: SELECT on a db change only, whatever the order the
         * producers' records arrive in; no sizes, no slot info. */
        if (dbid != *cur_db) {
            static const char selectcmd[] = "*2\r\n$6\r\nSELECT\r\n";
            if (rioWrite(rdb, selectcmd, sizeof(selectcmd) - 1) == 0) return -1;
            if (rioWriteBulkLongLong(rdb, dbid) == 0) return -1;
            *cur_db = dbid;
        }
        *cur_slot = slot;
        return 0;
    }
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
static int hybsnapSaveChildWrite(hybsnapSave *sv, rio *out) {
    char tmpfile[256] = "";
    char eofmark[RDB_EOF_MARK_SIZE];
    char *err_op = "";
    int error = 0;
    char *payload = NULL;
    size_t payload_cap = 0;
    char *bigbuf = NULL;               /* KVOBJ copies: apart from 'payload', which may hold a held buffer */
    size_t bigbuf_cap = 0;
    long key_counter = 0, msgs = 0;
    long long info_updated_time = 0;
    int cur_db = -1, cur_slot = -1;
    int aborted = 0;
    FILE *fp = NULL;
    rio filerio;
    rio *rdb = out;

    startSaving(sv->rdbflags);
    if (!out) {
        if (hybsnapIsAof(sv))
            snprintf(tmpfile, sizeof(tmpfile), "temp-rewriteaof-%d.aof", (int)getpid());   /* as rewriteAppendOnlyFile() */
        else
            snprintf(tmpfile, sizeof(tmpfile), "temp-%d.rdb", (int)getpid());
        fp = fopen(tmpfile, "w");
        if (!fp) {
            serverLog(LL_WARNING, "Hybrid snapshot: failed opening the temp RDB file %s: %s", tmpfile, strerror(errno));
            stopSaving(0);
            return C_ERR;
        }
        rioInitWithFile(&filerio, fp);
        rdb = &filerio;
        if (hybsnapIsAof(sv) ? server.aof_rewrite_incremental_fsync : server.rdb_save_incremental_fsync) {
            rioSetAutoSync(rdb, REDIS_AUTOSYNC_BYTES);
            if (!(sv->rdbflags & RDBFLAGS_KEEP_CACHE)) rioSetReclaimCache(rdb, 1);
        }
    } else if (!sv->slots_form) {
        /* Diskless framing, as rdbSaveRioWithEOFMark(): the replica reads up
         * to the mark that follows the footer. */
        getRandomHexChars(eofmark, RDB_EOF_MARK_SIZE);
        if (rioWrite(rdb, "$EOF:", 5) == 0 || rioWrite(rdb, eofmark, RDB_EOF_MARK_SIZE) == 0 ||
            rioWrite(rdb, "\r\n", 2) == 0) { err_op = "eof mark"; goto werr; }
    }
    if (sv->slots_form) {
        /* A RESP stream, as slotSnapshotSaveRio() starts: module commands and FUNCTION RESTORE. */
        if (slotSnapshotWritePrelude(rdb) == C_ERR) { err_op = "slotSnapshotWritePrelude"; goto werr; }
    } else if (sv->aof_cmd_form) {
        /* Timestamp annotation and the functions, as rewriteAppendOnlyFileRio starts. */
        if (rewriteAppendOnlyFileHeader(rdb) == C_ERR) { err_op = "rewriteAppendOnlyFileHeader"; goto werr; }
    } else if (rdbSaveRioHeader(sv->req, rdb, &error, sv->rdbflags, sv->has_rsi ? &sv->rsi : NULL) == C_ERR) {
        err_op = "rdbSaveRioHeader";
        goto werr;
    }

    /* Two channels. Small: BUFFER messages in production order.
     * Big: KVOBJ (one big value each), then END (or ABORT).
     * Policy: the small channel while the parent reports a backlog of at
     * least HYBSNAP_SMALL_BACKLOG on it (or nothing waits on the big channel), a
     * big value otherwise. The bounded queue then never parks the scan thread
     * while we are inside a big value, and the big values come up as soon as
     * the small side is caught up (a single FIFO took the big values handed
     * ahead by the main thread back to back and lost the overlap). A
     * BUFFER whose dependency (header robj_off: big values that must precede
     * it) is not met waits for the big channel. END carries both channels'
     * message counts: we are done when both are consumed. */
    uint64_t small_total = 0, small_seen = 0, kv_total = 0, kv_written = 0;
    int ended = 0;
    hybsnapMsgHdr held;
    int have_held = 0;                 /* a BUFFER (in 'payload') waiting for big values */
    /* rdb channel: replicas seen hanging up. Like the classic connset writer the
     * transfer goes on for the others and fails only when none is left; the
     * rio skips a dead connection by itself at its next write. */
    char *conn_dead = zcalloc(sv->rdb_channel && sv->numconns ? sv->numconns : 1);
    int ndead = 0;
    for (;;) {
        hybsnapMsgHdr h;
        int src;   /* 0 = small channel, 1 = big channel */
        if (have_held) {
            if (held.robj_off <= kv_written) {
                h = held;
                have_held = 0;
                goto write_small;
            }
            if (ended) { errno = EPROTO; err_op = "buffer depends on a big value that never comes"; goto werr; }
            src = 1;
        } else if (ended && small_seen == small_total) {
            break;
        } else {
            int small_open = !(ended && small_seen == small_total);
            /* The two channels, then the replica connections we write
             * directly (rdb channel): a replica that hangs up is noticed here,
             * while we wait for the producers, not at our next write, which
             * may be a key-save delay away. The classic child learns it at its
             * next write too, but writes continuously. */
            int nconns = sv->rdb_channel ? sv->numconns : 0;
            struct pollfd pfd[2 + nconns];
            pfd[0] = (struct pollfd){ .fd = sv->pipe_rd, .events = small_open ? POLLIN : 0, .revents = 0 };
            pfd[1] = (struct pollfd){ .fd = sv->big_rd, .events = ended ? 0 : POLLIN, .revents = 0 };
#ifdef POLLRDHUP
            const short hup = POLLRDHUP;
#else
            const short hup = 0;
#endif
            for (int i = 0; i < nconns; i++)
                pfd[2 + i] = (struct pollfd){ .fd = conn_dead[i] ? -1 : sv->conns[i]->fd, .events = hup, .revents = 0 };
            /* About to wait for the producers: push what the socket rio
             * buffers in user space out now, so records reach the replica
             * (or a migration's destination) as they are produced, as the classic
             * child does when it flushes per key, rather than at END. */
            if (out && rioFlush(rdb) == 0) { err_op = "flush"; goto werr; }
            int pr = poll(pfd, 2 + nconns, -1);
            if (pr < 0) {
                if (errno == EINTR) continue;
                err_op = "channel poll";
                goto werr;
            }
            int newly_dead = 0;
            for (int i = 0; i < nconns; i++) {
                if (!conn_dead[i] && (pfd[2 + i].revents & (hup | POLLHUP | POLLERR))) {
                    conn_dead[i] = 1;
                    newly_dead = 1;
                    if (++ndead == nconns) {
                        errno = ECONNRESET;
                        err_op = "replica connection closed";
                        goto werr;
                    }
                }
            }
            int small_ready = small_open && (pfd[0].revents & POLLIN);
            int big_ready = !ended && (pfd[1].revents & POLLIN);
            if (!small_ready && !big_ready) {
                if (newly_dead) continue;   /* woken by a replica hanging up only */
                errno = 0; err_op = "channel closed"; goto werr;
            }
            size_t backlog;
            atomicGet(sv->shared->small_queued, backlog);
            if (!small_ready && big_ready && small_open && backlog >= HYBSNAP_SMALL_BACKLOG) {
                /* The parent holds a small backlog that the scan thread has
                 * not pushed into the pipe yet (it pushes at its own
                 * enqueues, about once a millisecond, a pipe's worth each
                 * time). Wait for it briefly rather than start a big value:
                 * measurements showed the child falling to the next big value at
                 * every such gap, the backlog growing to the bound for the
                 * whole value and the scan thread parked (29-36 s under
                 * writes against 21-24 with an unbounded overflow). */
                struct pollfd one = { .fd = sv->pipe_rd, .events = POLLIN, .revents = 0 };
                int r = poll(&one, 1, HYBSNAP_SMALL_WAIT_MS);
                if (r > 0 && (one.revents & POLLIN)) small_ready = 1;
            }
            src = (small_ready && (backlog >= HYBSNAP_SMALL_BACKLOG || !big_ready)) ? 0 : 1;
        }
        if (src == 1) {
            if (hybsnapReadAll(sv->big_rd, &h, sizeof(h)) != 0) { err_op = "channel read (header)"; goto werr; }
            if (h.type == HYBSNAP_MSG_END) {
                ended = 1;
                kv_total = h.robj_off;
                small_total = h.src_addr;
                if (kv_written != kv_total) { errno = EPROTO; err_op = "big value count"; goto werr; }
                continue;
            }
            if (h.type == HYBSNAP_MSG_ABORT) { aborted = 1; goto werr; }
            if (h.type != HYBSNAP_MSG_KVOBJ) { errno = EPROTO; err_op = "unknown message"; goto werr; }
            if (h.len > bigbuf_cap) {
                bigbuf = zrealloc(bigbuf, h.len);
                bigbuf_cap = h.len;
            }
            if (h.len && hybsnapReadAll(sv->big_rd, bigbuf, h.len) != 0) { err_op = "channel read (payload)"; goto werr; }
            if (hybsnapChildSelect(rdb, sv, h.dbid, h.slot, &cur_db, &cur_slot) != 0) { err_op = "selectdb"; goto werr; }
            kvobj *kv = (kvobj *)(bigbuf + h.robj_off);
            /* An embedded string value lives inside the kvobj allocation: its
             * pointer is the parent's address, relocate it into our copy. */
            if (kv->encoding == OBJ_ENCODING_EMBSTR)
                kv->ptr = bigbuf + ((char *)kv->ptr - (char *)(uintptr_t)h.src_addr);
            size_t before = rdb->processed_bytes;
            if (hybsnapSaveWriteRecord(sv, rdb, kv, h.dbid) != 0) { err_op = "record"; goto werr; }
            key_counter++;
            kv_written++;
            /* As the classic child does after each key: drop the value's pages from
             * this process once written, so later parent writes to them do not
             * leave us a private copy. The value is the parent's forkable
             * allocation, mapped in our CoW view. */
            size_t dump_size = rdb->processed_bytes - before;
            if (dump_size > server.page_size / 2) dismissObject(kv, dump_size);
            goto progress;
        }
        /* Small channel, from the pipe. */
        if (hybsnapReadAll(sv->pipe_rd, &h, sizeof(h)) != 0) { err_op = "channel read (header)"; goto werr; }
        small_seen++;
        if (h.type != HYBSNAP_MSG_BUFFER) { errno = EPROTO; err_op = "unknown message"; goto werr; }
        if (h.len > payload_cap) {
            payload = zrealloc(payload, h.len);
            payload_cap = h.len;
        }
        if (h.len && hybsnapReadAll(sv->pipe_rd, payload, h.len) != 0) { err_op = "channel read (payload)"; goto werr; }
        if (h.robj_off > kv_written) {
            held = h;
            have_held = 1;
            continue;
        }
    write_small:
        if (hybsnapChildSelect(rdb, sv, h.dbid, h.slot, &cur_db, &cur_slot) != 0) { err_op = "selectdb"; goto werr; }
        if (rioWrite(rdb, payload, h.len) == 0) { err_op = "write buffer"; goto werr; }
    progress:
        if ((msgs++ & 1023) == 0) {
            long long now = mstime();
            if (now - info_updated_time >= 1000) {
                sendChildInfo(CHILD_INFO_TYPE_CURRENT_INFO, key_counter, hybsnapIsAof(sv) ? "AOF rewrite" : "RDB");
                info_updated_time = now;
            }
        }
    }
    zfree(bigbuf);
    bigbuf = NULL;
    zfree(payload);
    payload = NULL;

    if (sv->slots_form) {
        if (slotSnapshotWriteEOF(rdb) == C_ERR) { err_op = "slotSnapshotWriteEOF"; goto werr; }
    } else if (!sv->aof_cmd_form && rdbSaveRioFooter(sv->req, rdb, &error) == C_ERR) { err_op = "rdbSaveRioFooter"; goto werr; }
    if (out) {
        if (!sv->slots_form && rioWrite(rdb, eofmark, RDB_EOF_MARK_SIZE) == 0) { err_op = "eof mark"; goto werr; }
        if (rioFlush(rdb) == 0) { err_op = "flush"; goto werr; }
        serverLog(LL_NOTICE, "Hybrid %s transfer done, %ld HYBSNAP_FORK keys serialized by the child, %zu bytes sent.",
                  sv->slots_form ? "slot snapshot" : "RDB", key_counter, rdb->processed_bytes);
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
        serverLog(LL_WARNING, "Hybrid snapshot: error moving temp DB file %s to %s: %s", tmpfile, sv->filename, strerror(errno));
        unlink(tmpfile);
        stopSaving(0);
        return C_ERR;
    }
    if (hybsnapIsAof(sv)) {
        /* The parent's done handler renames it into the manifest and fsyncs the directory. */
        serverLog(LL_NOTICE, "Hybrid AOF rewrite done, %ld HYBSNAP_FORK keys serialized by the child, %zu bytes written to %s.",
                  key_counter, rdb->processed_bytes, sv->filename);
        stopSaving(1);
        return C_OK;
    }
    if (fsyncFileDir(sv->filename) != 0) {
        serverLog(LL_WARNING, "Hybrid snapshot: failed to fsync directory: %s", strerror(errno));
        stopSaving(0);
        return C_ERR;
    }
    serverLog(LL_NOTICE, "Hybrid snapshot done, %ld HYBSNAP_FORK keys serialized by the child, %zu bytes written.",
              key_counter, rdb->processed_bytes);
    stopSaving(1);
    return C_OK;

werr:
    if (aborted)
        serverLog(LL_NOTICE, "Hybrid snapshot aborted by the parent.");
    else if (errno == 0)
        serverLog(LL_WARNING, "Hybrid snapshot: the parent closed the channel (%s)", err_op);
    else
        serverLog(LL_WARNING, "Hybrid snapshot: write error (%s): %s", err_op, strerror(errno));
    zfree(payload);
    zfree(bigbuf);
    if (fp) fclose(fp);
    if (tmpfile[0]) unlink(tmpfile);
    stopSaving(0);
    return aborted ? SERVER_CHILD_NOERROR_RETVAL : C_ERR;
}

static void hybsnapSaveChild(hybsnapSave *sv) {
    rio sock;
    char aofname[256];
    close(sv->pipe_wr);
    close(sv->big_wr);
    if (hybsnapToSockets(sv)) {
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
    } else if (hybsnapIsAof(sv)) {
        /* The name backgroundRewriteDoneHandler and aofRemoveTempFile expect. */
        snprintf(aofname, sizeof(aofname), "temp-rewriteaof-bg-%d.aof", (int)getpid());
        sv->filename = aofname;
        redisSetProcTitle("redis-aof-rewrite");
        redisSetCpuAffinity(server.aof_rewrite_cpulist);
    } else {
        redisSetProcTitle("redis-rdb-bgsave");
    }
    if (!hybsnapIsAof(sv)) redisSetCpuAffinity(server.bgsave_cpulist);
    int retval = hybsnapSaveChildWrite(sv, hybsnapToSockets(sv) ? &sock : NULL);
    if (retval == C_OK) {
        if (hybsnapIsAof(sv)) sendChildCowInfo(CHILD_INFO_TYPE_AOF_COW_SIZE, "AOF rewrite");
        else sendChildCowInfo(CHILD_INFO_TYPE_RDB_COW_SIZE, "RDB");
    }
    if (hybsnapToSockets(sv)) {
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

static void hybsnapSaveFree(hybsnapSave *sv) {
    if (sv->ae_registered)
        aeDeleteFileEvent(server.el, hybsnapScanReadEventFd(sv->scan), AE_READABLE);
    if (sv->scan) hybsnapScanRelease(sv->scan);
    if (sv->oq) {
        hybsnapOutClearList(sv->oq);
        listRelease(sv->oq);
    }
    if (sv->bq) {
        hybsnapOutClearList(sv->bq);
        listRelease(sv->bq);
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
    if (sv->big_wr != -1) close(sv->big_wr);
    if (sv->big_rd != -1) close(sv->big_rd);
    if (sv->shared) munmap(sv->shared, sizeof(hybsnapShared));
    if (sv->sizes) {
        for (int j = 0; j < sv->dbnum; j++) zfree(sv->sizes[j].slot_keys);   /* slot_expires lives in the same block */
    }
    sdsfree(sv->stage);
    if (sv->mq) {
        hybsnapOutClearList(sv->mq);
        listRelease(sv->mq);
        hybsnapOutClearList(sv->mq_ready);
        listRelease(sv->mq_ready);
        pthread_mutex_destroy(&sv->mq_lock);
    }
    sdsfree(sv->mstage);
    sdsfree(sv->mscratch);
    /* Flushed dbs still held (every db of an AOF rewrite that saw FLUSHALL,
     * when the child ended before their slots completed): free them while
     * sv->stores is still there, hybsnapSaveFreeFlushed clears the entry. */
    for (int j = 0; j < sv->dbnum; j++) hybsnapSaveFreeFlushed(sv, j);   /* normally freed at store completion */
    zfree(sv->bitmaps);
    zfree(sv->tables);
    zfree(sv);
}

/* Overlay callbacks for the server keyspace (privdata = the store's hybsnapGuardPriv). */
static void hybsnapOverlayEnter(void *privdata) { UNUSED(privdata); hybsnapSectionOpen(); }
static const kvstoreSnapHooks hybsnap_server_hooks = {
    .clone = hybsnapOverlayCloneKey, .free_orig = hybsnapOverlayFreeOriginal, .enter = hybsnapOverlayEnter,
    .prepare = hybsnapOverlayPrepare,
};

/* Main thread: the save object, its channel, queues and scan context for a
 * frozen-to-be keyspace. NULL on failure (logged). */
static hybsnapSave *hybsnapSaveCreate(hybsnapSaveKind kind, int req, char *filename, rdbSaveInfo *rsi, int rdbflags) {
    serverAssert(hybsnap_save == NULL);

    kvstoreOverlayStatsReset();   /* the overlay counters of INFO describe this save */
    hybsnapSave *sv = zcalloc(sizeof(*sv));
    sv->pipe_rd = sv->pipe_wr = sv->big_rd = sv->big_wr = -1;
    sv->dbnum = server.dbnum;
    sv->kind = kind;
    sv->req = req;
    sv->rdbflags = rdbflags;
    sv->filename = filename;
    if (rsi) { sv->rsi = *rsi; sv->has_rsi = 1; }
    sv->tables = zcalloc((sizeof(kvstore *) + sizeof(hybsnapFlushedDb) + sizeof(hybsnapDbSizes)) * sv->dbnum);
    sv->stores = sv->tables;
    sv->flushed = (hybsnapFlushedDb *)(sv->stores + sv->dbnum);
    sv->sizes = (hybsnapDbSizes *)(sv->flushed + sv->dbnum);
    if (req & SLAVE_REQ_SLOTS_SNAPSHOT) {
        /* Atomic slot migration's slot snapshot: only the migrating slots, as
         * the classic child reads them from its copy of the migrate task. */
        sv->slots_form = 1;
        sv->bitmaps = zcalloc(2 * (CLUSTER_SLOTS / 8));
        sv->slot_filter = sv->bitmaps;
        if (asmMigratingSlotsBitmap(sv->slot_filter) == 0) {
            serverLog(LL_WARNING, "Hybrid snapshot: slot snapshot requested with no migrate task running");
            zfree(sv->bitmaps); zfree(sv->tables); zfree(sv);
            return NULL;
        }
    }
    if (server.cluster_enabled && asmIsTrimInProgress()) {
        /* The fork-instant set of slots under a trim job (the classic child reads
         * its copy-on-write copy of the same jobs). */
        if (!sv->bitmaps) sv->bitmaps = zcalloc(2 * (CLUSTER_SLOTS / 8));
        sv->trim_slots = (uint8_t *)sv->bitmaps + CLUSTER_SLOTS / 8;
        for (int slot = 0; slot < CLUSTER_SLOTS; slot++)
            if (isSlotInTrimJob(slot)) sv->trim_slots[slot >> 3] |= 1u << (slot & 7);
    }
    for (int j = 0; j < sv->dbnum; j++) {
        redisDb *db = &server.db[j];
        sv->stores[j] = db->keys;
        sv->sizes[j].keys = kvstoreSize(db->keys);
        sv->sizes[j].expires = kvstoreSize(db->expires);
        if (server.cluster_enabled) {
            int n = kvstoreNumDicts(db->keys);
            sv->sizes[j].nslots = n;
            sv->sizes[j].slot_keys = zmalloc(sizeof(unsigned long long) * 2 * n);   /* both tables, one block */
            sv->sizes[j].slot_expires = sv->sizes[j].slot_keys + n;
            for (int didx = 0; didx < n; didx++) {
                sv->sizes[j].slot_keys[didx] = kvstoreDictSize(db->keys, didx);
                sv->sizes[j].slot_expires[didx] = kvstoreDictSize(db->expires, didx);
            }
        }
    }
    /* Two pipes. The scan thread writes them non-blockingly and keeps what
     * does not fit in its own queues (bounded by hybrid-snapshot-max-queued-bytes);
     * the child's read ends stay blocking. Both pipes are deepened to 1 MB
     * (best effort). */
    int *ends[2] = { &sv->pipe_rd, &sv->big_rd };
    int *wends[2] = { &sv->pipe_wr, &sv->big_wr };
    for (int c = 0; c < 2; c++) {
        int fds[2];
        if (pipe(fds) != 0) {
            serverLog(LL_WARNING, "Hybrid snapshot: pipe() failed: %s", strerror(errno));
            hybsnapSaveFree(sv);
            return NULL;
        }
        *ends[c] = fds[0];
        *wends[c] = fds[1];
#ifdef F_SETPIPE_SZ
        fcntl(fds[1], F_SETPIPE_SZ, 1 << 20);   /* best effort, both channels: a dataset of many fork-class keys
                                                  * sends many small KVOBJ headers between two child reads */
#endif
        if (fcntl(fds[1], F_SETFL, fcntl(fds[1], F_GETFL) | O_NONBLOCK) == -1) {
            serverLog(LL_WARNING, "Hybrid snapshot: fcntl(O_NONBLOCK) failed: %s", strerror(errno));
            hybsnapSaveFree(sv);
            return NULL;
        }
    }
    sv->shared = mmap(NULL, sizeof(hybsnapShared), PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANONYMOUS, -1, 0);
    if (sv->shared == MAP_FAILED) {
        sv->shared = NULL;
        serverLog(LL_WARNING, "Hybrid snapshot: mmap(MAP_SHARED) failed: %s", strerror(errno));
        hybsnapSaveFree(sv);
        return NULL;
    }
    atomicSet(sv->shared->small_queued, 0);
    sv->oq = listCreate();
    sv->bq = listCreate();
    sv->oq_pool = listCreate();
    sv->oq_limit = server.hybrid_snapshot_max_queued_bytes;
    sv->tag_all = server.hybrid_snapshot_tag_all;
    hybsnapStageReset(sv);
    sv->stage_dbid = sv->stage_slot = -1;
    pthread_mutex_init(&sv->mq_lock, NULL);
    sv->mq = listCreate();
    sv->mq_ready = listCreate();
    sv->mstage = hybsnapMsgNew(0);
    sv->mscratch = sdsempty();
    sv->mstage_dbid = sv->mstage_slot = -1;
    atomicSet(sv->key_save_delay, server.rdb_key_save_delay);
    sv->maxmemory_policy = server.maxmemory_policy;
    sv->rdb_compression = (req & (SLAVE_REQ_RDB_NO_COMPRESS | SLAVE_REQ_SLOTS_SNAPSHOT)) ? 0 : server.rdb_compression;   /* a slot snapshot never compresses (slotSnapshotSaveRio) */
    server.stat_hybsnap_saves++;
    sv->scan = hybsnapScanCreate(sv->stores, sv->dbnum, hybsnapSaveDispatch, NULL, sv);
    sv->scan->skip_stamped = 1;
    if (sv->slot_filter) hybsnapScanSetSlotFilter(sv->scan, sv->slot_filter);
    hybsnapScanSetOverlay(sv->scan, &hybsnap_server_hooks, NULL);
    sv->scan->store_done = hybsnapSaveStoreDone;
    sv->scan->thread_done = hybsnapSaveThreadDone;
    sv->scan->thread_init = hybsnapSaveThreadInit;
    sv->scan->window_done = hybsnapSaveWindowDone;
    return sv;
}

/* Main thread: FORK INSTANT. Freeze, fork the writer child, start the scan
 * thread. On a fork failure the keyspace is unfrozen and sv is freed; the
 * caller undoes its own state (replica states, pipes). */
static int hybsnapSaveLaunch(hybsnapSave *sv) {
    if (!hybsnapIsAof(sv)) {
        server.stat_rdb_saves++;
        server.dirty_before_bgsave = server.dirty;
        server.lastbgsave_try = time(NULL);
    }

    /* FORK INSTANT: freeze, then fork. */
    mstime_t freeze_lat;
    latencyStartMonitor(freeze_lat);
    hybsnapScanFreeze(sv->scan);
    latencyEndMonitor(freeze_lat);
    latencyAddSampleIfNeeded("hybrid-freeze", freeze_lat);
    pid_t childpid = redisFork(hybsnapIsAof(sv) ? CHILD_TYPE_AOF : CHILD_TYPE_RDB, REDIS_FORK_NO_KEYSPACE);
    if (childpid == 0) {
        hybsnapSaveChild(sv);   /* never returns */
    }
    if (childpid == -1) {
        if (hybsnapIsAof(sv)) server.aof_lastbgrewrite_status = C_ERR;
        else if (!hybsnapToSockets(sv)) server.lastbgsave_status = C_ERR;
        serverLog(LL_WARNING, "Can't save in background (hybrid): fork: %s", strerror(errno));
        hybsnapScanAbort(sv->scan);   /* unfreeze; nothing was scanned */
        hybsnapSaveFree(sv);
        return C_ERR;
    }
    close(sv->pipe_rd);
    sv->pipe_rd = -1;
    close(sv->big_rd);
    sv->big_rd = -1;
    hybsnap_save = sv;
    /* The classic done handlers branch on it; an AOF rewrite leaves it NONE. */
    if (!hybsnapIsAof(sv)) server.rdb_child_type = hybsnapToSockets(sv) ? RDB_CHILD_TYPE_SOCKET : RDB_CHILD_TYPE_DISK;
    if (hybsnapScanStartThread(sv->scan) != C_OK) {
        /* The child is waiting on the channel: kill it, unfreeze. */
        serverLog(LL_WARNING, "Hybrid snapshot: cannot start the scan thread; aborting the save");
        killRDBChild();   /* -> hybsnapSaveAbort() unfreezes; the done handler releases */
    } else {
        aeCreateFileEvent(server.el, hybsnapScanReadEventFd(sv->scan), AE_READABLE, hybsnapScanAeHandler, sv->scan);
        sv->ae_registered = 1;
    }
    if (hybsnapIsAof(sv)) {
        serverLog(LL_NOTICE, "Hybrid AOF rewrite started by pid %ld (scan thread streams the %s base file to the child)",
                  (long)childpid, sv->aof_cmd_form ? "AOF" : "RDB");
    } else {
        serverLog(LL_NOTICE, "Hybrid background saving started by pid %ld (scan thread streams to the child)", (long)childpid);
        server.rdb_save_time_start = time(NULL);
    }
    return C_OK;
}

/* BGREWRITEAOF through the hybrid save: the scan and child write the base file,
 * an RDB with aof-use-rdb-preamble, else AOF commands (records in command form,
 * the child emitting SELECT on db change). The caller has flushed the AOF and
 * opened the new INCR file, so the fork instant is where the INCR history
 * starts; backgroundRewriteDoneHandler handles the child's temp file as for the
 * classic child. */
int hybsnapStartRewriteAofBackground(void) {
    if (hasActiveChildProcess()) return C_ERR;
    hybsnapSave *sv = hybsnapSaveCreate(HYBSNAP_SAVE_AOF, SLAVE_REQ_NONE, NULL, NULL, RDBFLAGS_AOF_PREAMBLE);
    if (!sv) return C_ERR;
    sv->aof_cmd_form = !server.aof_use_rdb_preamble;
    return hybsnapSaveLaunch(sv);   /* the child names its own temp file */
}

/* Whether a save takes the hybrid path: the one decision for every fork-based
 * snapshot (rdbSaveBackground, startBgsaveForReplication,
 * rewriteAppendOnlyFileBackground). 1 when the switch is on, the request has
 * keyspace content (functions-only transfers keep the classic child) and every
 * relevant module has opted in (a blocking module is logged once); 0 when the
 * caller must run the classic child. */
int hybsnapSaveEligible(int req) {
    if (!hybsnapIsEnabled()) return 0;
    /* A functions-only transfer has no keyspace content for the scan thread;
     * the classic child serves it (redisFork maps the arena). A slot snapshot
     * (atomic slot migration) is a hybrid save over the migrating slots. */
    if (req & SLAVE_REQ_RDB_EXCLUDE_DATA) return 0;
    if ((req & SLAVE_REQ_SLOTS_SNAPSHOT) && !asmMigrateInProgress()) return 0;
    /* Scan positions number every dict of every db in HYBSNAP_POS_ORD_BITS bits. */
    if ((long long)server.dbnum * (server.cluster_enabled ? CLUSTER_SLOTS : 1) >= (1LL << HYBSNAP_POS_ORD_BITS)) {
        static int logged = 0;
        if (!logged) serverLog(LL_NOTICE, "Hybrid snapshot: too many databases for the scan positions, the classic child serves the save");
        logged = 1;
        return 0;
    }
    if (!moduleAllModulesHandleHybridSnapshot()) {
        hybsnapFallbackLog();   /* a module's save callbacks need the keyspace */
        return 0;
    }
    return 1;
}

/* Start a hybrid background save: freeze the keyspace (overlay), fork the
 * writer child, spawn the scan thread. Same contract as rdbSaveBackground(). */
int hybsnapStartSaveBackground(int req, char *filename, rdbSaveInfo *rsi, int rdbflags) {
    if (hasActiveChildProcess()) return C_ERR;
    if (hybsnapArenaIsForkable()) {
        /* Only a Lua debugger session can leave the arena forkable with no
         * exclusive child around. The hybrid child never touches the arena, so
         * mapping it is harmless; it just pays copy-on-write on it meanwhile. */
        serverLog(LL_NOTICE, "Hybrid snapshot: the hybrid arena is forkable (debugger child); the save child will map it");
    }
    hybsnapSave *sv = hybsnapSaveCreate(HYBSNAP_SAVE_RDB, req, filename, rsi, rdbflags);
    if (!sv) return C_ERR;
    return hybsnapSaveLaunch(sv);
}

/* Diskless full sync through the hybrid save: a BGSAVE's scan and channels,
 * the child writing the replicas instead of a file. As in
 * rdbSaveToSlavesSockets(), replicas in WAIT_BGSAVE_START with this 'req' are
 * set up for full resync at the current offset just before the freeze, so the
 * offset is the snapshot's. With the rdb channel the child writes the replica
 * connections; otherwise it writes the rdb pipe the parent fans out and waits
 * on the safe-to-exit pipe. The classic socket done handler does the replica
 * bookkeeping. */
int hybsnapStartSaveToSlavesSockets(int req, rdbSaveInfo *rsi) {
    listIter li;
    listNode *ln;
    int pipefds[2];
    int rdb_channel = server.repl_rdb_channel && (req & SLAVE_REQ_RDB_CHANNEL);

    if (hasActiveChildProcess()) return C_ERR;
    /* Even if the previous fork child exited, don't start a new one until we
     * drained the pipe. */
    if (server.rdb_pipe_conns) return C_ERR;
    if (hybsnapArenaIsForkable())
        serverLog(LL_NOTICE, "Hybrid snapshot: the hybrid arena is forkable (debugger child); the save child will map it");

    hybsnapSave *sv = hybsnapSaveCreate(HYBSNAP_SAVE_SOCKETS, req, NULL, rsi, RDBFLAGS_REPLICATION);
    if (!sv) return C_ERR;
    sv->rdb_channel = rdb_channel;
    sv->rdb_pipe_write = sv->safe_to_exit_pipe = -1;

    if (!rdb_channel) {
        /* The child cannot write the sockets itself: with TLS the parent must
         * keep a continuous TLS state when it takes over after the child. */
        if (anetPipe(pipefds, O_NONBLOCK, 0) == -1) {
            hybsnapScanAbort(sv->scan);
            hybsnapSaveFree(sv);
            return C_ERR;
        }
        server.rdb_pipe_read = pipefds[0];
        sv->rdb_pipe_write = pipefds[1];
        /* The parent tells the child through this pipe that it may exit. */
        if (anetPipe(pipefds, 0, 0) == -1) {
            close(sv->rdb_pipe_write);
            close(server.rdb_pipe_read);
            server.rdb_pipe_read = -1;
            hybsnapScanAbort(sv->scan);
            hybsnapSaveFree(sv);
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

    if (hybsnapSaveLaunch(sv) != C_OK) {
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
    /* sv is live from here (hybsnap_save); it does not own 'conns'. */
    sv->conns = NULL;
    serverLog(LL_NOTICE, "Background RDB transfer started by pid %ld to %s (hybrid: the scan thread streams to the child)",
              (long)server.child_pid, rdb_channel ? "replica socket" : "parent process pipe");
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

/* Stop the scan thread and complete every slot (main thread). Safe to call
 * when no hybrid save is running. Does not touch the child (see killRDBChild). */
void hybsnapSaveAbort(void) {
    if (!hybsnap_save || hybsnap_save->aborted) return;
    hybsnapSave *sv = hybsnap_save;
    sv->aborted = 1;
    /* Stops the thread within one cursor step, joins it, completes every slot.
     * A thread blocked on write() returns once the child reads or dies. */
    hybsnapScanAbort(sv->scan);
    /* Every slot is complete and every overlay released: the stores are not
     * ours to look at any more (the aborting FLUSHALL ASYNC or full-sync load
     * frees them on BIO while this save object waits for the child). */
    for (int s = 0; s < sv->dbnum; s++) sv->stores[s] = NULL;
    if (sv->ae_registered) {
        aeDeleteFileEvent(server.el, hybsnapScanReadEventFd(sv->scan), AE_READABLE);
        sv->ae_registered = 0;
    }
    serverLog(LL_NOTICE, "Hybrid snapshot: scan aborted, keyspace overlay drained.");
}

/* The one child-end hook: called once for every child that ended, whatever
 * its type and whether it was reaped (checkChildrenDone) or killed
 * (killAppendOnlyChild), before the type's done handler runs. A no-op when
 * no hybrid save owns the child; otherwise stops the scan if the child failed
 * or the scan is unfinished, joins the thread, releases every state and
 * publishes the last save's peaks. */
void hybsnapOnChildEnd(int exitcode, int bysignal) {
    if (!hybsnap_save) return;
    hybsnapSave *sv = hybsnap_save;
    /* The child is gone. If the scan did not complete (or the thread stopped
     * on its own when the channel broke, leaving slots unfinished), stop it and
     * complete every remaining slot. Harmless after a clean finish. */
    if (!(!bysignal && exitcode == 0) || !hybsnapScanIsFinished(sv->scan))
        hybsnapScanAbort(sv->scan);
    if (sv->failed && !hybsnapToSockets(sv) && !hybsnapIsAof(sv)) {
        /* Our own defined failure (hard mark): the child was ended with the
         * whitelisted SIGUSR1, which the plain done handler does not count.
         * A socket transfer reports through the replicas, as the classic save. */
        server.lastbgsave_status = C_ERR;
        server.stat_rdb_consecutive_failures++;
    }
    hybsnap_last_retained_peak = sv->retained_peak;
    atomicGet(sv->queued_peak, hybsnap_last_queued_peak);
    hybsnap_last_inplace_writes = sv->inplace_writes;
    if (!bysignal && exitcode == 0 && !sv->failed) {
        /* The classic end-of-save line, from the side that knows the counts: the
         * keys the scan thread and the main thread produced, and the keys of
         * slots under a trim job at the fork instant that were skipped. */
        long long nofork, fork, skipped, main;
        atomicGet(sv->keys_nofork, nofork);
        atomicGet(sv->keys_fork, fork);
        atomicGet(sv->keys_skipped, skipped);
        atomicGet(sv->main_keys, main);
        serverLog(LL_NOTICE, "%s done, %lld keys saved, %lld keys skipped.",
                  hybsnapIsAof(sv) ? "AOF rewrite" : sv->slots_form ? "Slot snapshot" : "BGSAVE",
                  nofork + fork + main, skipped);
    }
    hybsnap_save = NULL;
    hybsnapSaveFree(sv);   /* drains completions, joins, unregisters */
    serverLog(LL_NOTICE, "Hybrid snapshot: scan thread joined and overlay released.");
}

/* ---- Unit tests ---------------------------------------------------------- */
#ifdef REDIS_TEST
/* Test helper: HYBSNAP_SLOT_* of (store, dict), from either thread. DONE for a
 * slot the save holds nothing of (empty at the fork instant, created since, or
 * completed). */
static hybsnapSlotState hybsnapScanSlotState(hybsnapScanCtx *ctx, int store_idx, int didx) {
    if (ctx->ended || !ctx->store[store_idx].frozen) return HYBSNAP_SLOT_DONE;
    dict *d = hybsnapTableDict(ctx, store_idx, didx);
    if (!d) return HYBSNAP_SLOT_DONE;
    return hybsnapDictStateOf(ctx, d);
}

/* Test helper: table dicts this save retained past their store. */
static long hybsnapScanRetainedDicts(hybsnapScanCtx *ctx) { return ctx ? ctx->retained_dicts : 0; }

/* Test helper: freeze and spawn the thread in one call, with no fork in
 * between. On a failed spawn nothing was scanned: complete whatever was
 * attached as aborted and take the guards off (the save instead kills its
 * waiting child). */
static int hybsnapScanStart(hybsnapScanCtx *ctx) {
    hybsnapScanFreeze(ctx);
    if (hybsnapScanStartThread(ctx) != C_OK) {
        hybsnapScanFinishAll(ctx);
        ctx->frozen_all = 0;
        return C_ERR;
    }
    return C_OK;
}

/* Test helper: 1 while slot (store, dict) is not DONE. */
static int hybsnapScanSlotNotDone(hybsnapScanCtx *ctx, int store_idx, int didx) {
    return hybsnapScanSlotState(ctx, store_idx, didx) != HYBSNAP_SLOT_DONE;
}
#include "testhelp.h"
#include <sys/wait.h>
#include <time.h>

/* Test declarations. */
extern dictType KvstoreDictTestType;   /* kvstore.c (REDIS_TEST) */
extern kvstoreType KvstoreTestType;

#define HYBSNAP_TEST_STORES 2
#define HYBSNAP_TEST_DICT_BITS 2
#define HYBSNAP_TEST_DICTS (1 << HYBSNAP_TEST_DICT_BITS)
#define HYBSNAP_TEST_MAXKEYS 6000

struct hybsnapScanTestState {
    unsigned char seen[HYBSNAP_TEST_STORES][HYBSNAP_TEST_DICTS][HYBSNAP_TEST_MAXKEYS];
    int inserted[HYBSNAP_TEST_STORES][HYBSNAP_TEST_DICTS];
    redisAtomic long dispatched;
    redisAtomic long bad;             /* unparsable / out-of-range / duplicate */
    int done_calls[HYBSNAP_TEST_STORES][HYBSNAP_TEST_DICTS];
    int done_aborted[HYBSNAP_TEST_STORES][HYBSNAP_TEST_DICTS];
    int dispatch_sleep_us;
    redisAtomic int wrong_state;      /* dispatch saw its slot not SCANNING */
    hybsnapScanCtx *ctx;
    kvstore **stores;
    int rehashing_at_done;
};

struct ovTestCounters { int clones; int frees; };
static void ovTestScanCb(void *priv, const dictEntry *de, dictEntry **plink);

/* Fork a child that writes one byte to 'p'. The child reports over a pipe
 * that it reached the write ('r') and that it survived it ('d'), so a crash
 * anywhere else (e.g. in jemalloc's post-fork handler) is not mistaken for a
 * fault on 'p'. Returns 1 if the child faulted exactly on the write (SIGSEGV
 * or SIGBUS after 'r' and no 'd'), 0 if it wrote fine and exited 0, and -1 on
 * any other outcome (printed). */
static int hybsnapTestChildWriteFaults(void *p) {
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

/* Fork a child that exercises the allocator (zmalloc + zmalloc_hybrid are both
 * forbidden/unmapped for the arena, so only the default arena is used) to
 * prove jemalloc's post-fork state is intact with the hybrid arena
 * present. Returns 1 on success. */
static int hybsnapTestChildAllocatorAlive(void) {
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
static int hybsnapTestMappingIsDontcopy(const void *p) {
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

static long long hybsnapTestNowNs(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (long long)ts.tv_sec * 1000000000LL + ts.tv_nsec;
}

static int hybsnapTestCountMaps(void) {
    FILE *f = fopen("/proc/self/maps", "r");
    if (!f) return -1;
    int n = 0;
    char line[1024];
    while (fgets(line, sizeof(line), f)) n++;
    fclose(f);
    return n;
}

static void hybsnapTestPurgeArena(void) {
#ifdef HAVE_HYBRID_ARENA
    char buf[64];
    snprintf(buf, sizeof(buf), "arena.%u.purge", zmalloc_hybrid_arena_index());
    je_mallctl(buf, NULL, 0, NULL, 0);
    snprintf(buf, sizeof(buf), "arena.%u.decay", zmalloc_hybrid_arena_index());
    je_mallctl(buf, NULL, 0, NULL, 0);
#endif
}


/* ---- Scan tests ---- */


/* Test keys are aligned C strings owned by the dict (no_value dicts need
 * aligned stored keys; the real keyspace stores kvobj pointers). */
static char *hybsnapTestKey(int s, int didx, int i) {
    char buf[64];
    int len = snprintf(buf, sizeof(buf), "k:%d:%d:%d", s, didx, i);
    char *k = zmalloc(len + 1);
    memcpy(k, buf, len + 1);
    return k;
}

static void hybsnapTestDispatch(void *priv, int s, int didx, kvobj *kv) {
    struct hybsnapScanTestState *st = priv;
    const char *key = (const char*)kv;   /* test dict stores plain C strings */
    int ps, pd, pi;
    if (sscanf(key, "k:%d:%d:%d", &ps, &pd, &pi) != 3 || ps != s || pd != didx ||
        pi < 0 || pi >= st->inserted[s][didx] || st->seen[s][didx][pi])
    {
        atomicIncr(st->bad, 1);
    } else {
        st->seen[s][didx][pi] = 1;
    }
    if (st->ctx && hybsnapScanSlotState(st->ctx, s, didx) != HYBSNAP_SLOT_SCANNING) atomicSetWithSync(st->wrong_state, 1);
    atomicIncr(st->dispatched, 1);
    if (st->dispatch_sleep_us) usleep(st->dispatch_sleep_us);
}

static int hybsnapTestKeyCompare(dictCmpCache *cache, const void *k1, const void *k2) {
    UNUSED(cache);
    return strcmp(k1, k2) == 0;
}

static void hybsnapTestSlotDone(void *priv, int s, int didx, int aborted) {
    struct hybsnapScanTestState *st = priv;
    st->done_calls[s][didx]++;
    st->done_aborted[s][didx] = aborted;
    /* The dict is still frozen here: record whether the mid-rehash dict is
     * still mid-rehash, i.e. the scan itself never rehashed it. */
    if (s == 0 && didx == 3 && st->stores) {
        dict *d = kvstoreGetDict(st->stores[0], 3);
        st->rehashing_at_done = d ? dictIsRehashing(d) : 0;   /* NULL once the store let go of it */
    }
}

static int hybsnapTestAllUnfrozen(kvstore **stores) {
    for (int s = 0; s < HYBSNAP_TEST_STORES; s++)
        for (int didx = 0; didx < HYBSNAP_TEST_DICTS; didx++) {
            dict *d = kvstoreGetDict(stores[s], didx);
            if (d && (d->pauserehash != 0 || d->pauseAutoResize != 0)) return 0;
        }
    return 1;
}

static int hybsnapTestAllState(hybsnapScanCtx *ctx, hybsnapSlotState state) {
    for (int s = 0; s < HYBSNAP_TEST_STORES; s++)
        for (int didx = 0; didx < HYBSNAP_TEST_DICTS; didx++)
            if (hybsnapScanSlotState(ctx, s, didx) != state) return 0;
    return 1;
}


/* Tests without a scan thread: put the window where a thread and its
 * grants would have it. */
static void hybsnapTestWindow(hybsnapScanCtx *ctx, uint64_t lo, uint64_t hi) {
    __atomic_store_n(&hybsnap_lo, lo, __ATOMIC_SEQ_CST);
    __atomic_store_n(&hybsnap_hi, hi, __ATOMIC_SEQ_CST);
    ctx->hi = hi;
}

static void hybsnapScanTests(int accurate) {
    dictType noval = KvstoreDictTestType;
    noval.no_value = 1;
    noval.keyCompare = hybsnapTestKeyCompare;   /* the kvstore test type compares pointers */
    kvstore *stores[HYBSNAP_TEST_STORES];
    for (int s = 0; s < HYBSNAP_TEST_STORES; s++)
        stores[s] = kvstoreCreate(&KvstoreTestType, &noval, HYBSNAP_TEST_DICT_BITS, KVSTORE_ALLOCATE_DICTS_ON_DEMAND);

    struct hybsnapScanTestState *st = zcalloc(sizeof(*st));
    st->stores = stores;

    /* Population: store 0: d0 = 1 key, d1 = empty (never allocated), d2 = 5000
     * keys, d3 = left mid-rehash. Store 1: d0..d2 = 100 keys each, d3 empty. */
    dictSetResizeEnabled(DICT_RESIZE_ENABLE);
    int plan[HYBSNAP_TEST_STORES][HYBSNAP_TEST_DICTS] = { {1, 0, 5000, -1}, {100, 100, 100, 0} };
    for (int s = 0; s < HYBSNAP_TEST_STORES; s++) {
        for (int didx = 0; didx < HYBSNAP_TEST_DICTS; didx++) {
            int n = plan[s][didx];
            if (n == -1) {
                /* Insert until the dict is rehashing (expand just triggered). */
                int i = 0;
                while (i < HYBSNAP_TEST_MAXKEYS) {
                    serverAssert(kvstoreDictAddRaw(stores[s], didx, hybsnapTestKey(s, didx, i), NULL) != NULL);
                    i++;
                    if (i > 64 && dictIsRehashing(kvstoreGetDict(stores[s], didx))) break;
                }
                n = i;
            } else {
                for (int i = 0; i < n; i++)
                    serverAssert(kvstoreDictAddRaw(stores[s], didx, hybsnapTestKey(s, didx, i), NULL) != NULL);
            }
            st->inserted[s][didx] = n;
        }
    }
    long total_keys = 0;
    for (int s = 0; s < HYBSNAP_TEST_STORES; s++)
        for (int didx = 0; didx < HYBSNAP_TEST_DICTS; didx++) total_keys += st->inserted[s][didx];
    test_cond("scan test: a dict was left mid-rehash at scan start",
              dictIsRehashing(kvstoreGetDict(stores[0], 3)));
    test_cond("scan test: an empty slot has no dict allocated", kvstoreGetDict(stores[0], 1) == NULL);

    /* ---- Full scan with concurrent main-thread reads ---- */
    hybsnapScanCtx *ctx = hybsnapScanCreate(stores, HYBSNAP_TEST_STORES, hybsnapTestDispatch, hybsnapTestSlotDone, st);
    st->ctx = ctx;
    st->dispatch_sleep_us = 0;
    test_cond("scan: every slot is DONE before start", hybsnapTestAllState(ctx, HYBSNAP_SLOT_DONE));
    hybsnapScanFreeze(ctx);   /* guard installed, no thread yet: the checks below are deterministic */
    {
        /* On-demand freeze: the freeze pauses nothing; the
         * first write-intent access to a slot the thread has not finished
         * pauses that dict (guard). A read of a dict the thread is not
         * reading does not. */
        dict *d = kvstoreGetDict(stores[0], 0);
        test_cond("scan: no dict is paused right after start", d->pauserehash == 0 && d->pauseAutoResize == 0);
        char *k0 = hybsnapTestKey(0, 0, 0);   /* store 0 dict 0 holds one key */
        dictEntryLink b = NULL;
        dictEntryLink l = kvstoreDictFindLink(stores[0], 0, k0, &b);
        test_cond("scan: a write-intent lookup on an unreached dict finds the key without pausing it",
                  l != NULL && d->pauserehash == 0 && d->pauseAutoResize == 0);
        zfree(k0);
        /* An unreached dict the save holds may grow and shrink: what the main
         * thread emits ahead is recorded in the keys, which move with them. */
        dict *big = kvstoreGetDict(stores[0], 2);   /* 5000 keys */
        while (dictIsRehashing(big)) dictRehash(big, 1000);   /* start from one table */
        char *kb = hybsnapTestKey(0, 2, 1);
        kvstoreDictFindLink(stores[0], 2, kb, &b);
        zfree(kb);
        unsigned long n0 = DICTHT_SIZE(big->ht_size_exp[0]);
        int r1 = dictExpand(big, n0 * 16);   /* allowed: not paused */
        unsigned long n1 = DICTHT_SIZE(big->ht_size_exp[1]);
        test_cond("scan: an unreached dict expands", r1 == DICT_OK && dictIsRehashing(big) && n1 == n0 * 16);
        while (dictIsRehashing(big)) dictRehash(big, 1000);
        /* 5000 keys in 16 x their table: the fill ratio asks for a shrink. */
        int r2 = dictShrinkIfNeeded(big);
        while (dictIsRehashing(big)) dictRehash(big, 1000);
        test_cond("scan: an unreached dict the save holds shrinks", r2 == DICT_OK && DICTHT_SIZE(big->ht_size_exp[0]) < n1 &&
                  dictSize(big) == (unsigned long)st->inserted[0][2]);
        hybsnapSectionClose();   /* the accesses above opened a section: "command end" */
    }
    test_cond("scan: hybsnapScanStartThread() succeeds", hybsnapScanStartThread(ctx) == C_OK);
    /* Concurrent reads from the owner thread while the scan runs. */
    long reads = 0, misses = 0;
    unsigned seed = 7;
    int completions = 0;
    while (!hybsnapScanIsFinished(ctx)) {
        seed = seed * 1103515245u + 12345u;
        int s = (seed >> 8) % HYBSNAP_TEST_STORES, didx = (seed >> 12) % HYBSNAP_TEST_DICTS;
        if (st->inserted[s][didx] > 0) {
            int i = (seed >> 16) % st->inserted[s][didx];
            char *k = hybsnapTestKey(s, didx, i);
            if (kvstoreDictFind(stores[s], didx, k) == NULL) misses++;
            zfree(k);
            reads++;
            hybsnapSectionClose();   /* a read of a slot under scan opens a section: end the "command" */
        }
        completions += hybsnapScanHandleCompletions(ctx);
    }
    completions += hybsnapScanHandleCompletions(ctx);
    long dispatched, bad;
    atomicGet(st->dispatched, dispatched);
    atomicGet(st->bad, bad);
    int wrong_state;
    atomicGet(st->wrong_state, wrong_state);
    long all_seen = 1;
    for (int s = 0; s < HYBSNAP_TEST_STORES; s++)
        for (int didx = 0; didx < HYBSNAP_TEST_DICTS; didx++)
            for (int i = 0; i < st->inserted[s][didx]; i++) if (!st->seen[s][didx][i]) all_seen = 0;
    /* Slots empty at the fork instant have no dict (allocate on demand) and
     * are never completed; every populated one completes once through the ring. */
    int done_once = 1, empty_slots = 0;
    for (int s = 0; s < HYBSNAP_TEST_STORES; s++)
        for (int didx = 0; didx < HYBSNAP_TEST_DICTS; didx++) {
            int expect = st->inserted[s][didx] > 0 ? 1 : 0;
            if (st->inserted[s][didx] == 0) empty_slots++;
            if (st->done_calls[s][didx] != expect || st->done_aborted[s][didx] != 0) done_once = 0;
        }
    printf("scan: %ld keys dispatched, %ld concurrent reads (%ld misses), %d completions\n",
           dispatched, reads, misses, completions);
    test_cond("scan: dispatched exactly the inserted key set (count)", dispatched == total_keys);
    test_cond("scan: every inserted key was dispatched exactly once", all_seen && bad == 0);
    test_cond("scan: dispatch always ran with its slot in SCANNING state", wrong_state == 0);
    test_cond("scan: concurrent owner-thread lookups all hit", reads > 0 && misses == 0);
    test_cond("scan: every populated slot completed exactly once, none aborted",
              done_once && completions == HYBSNAP_TEST_STORES * HYBSNAP_TEST_DICTS - empty_slots);
    test_cond("scan: all slots DONE at the end", hybsnapTestAllState(ctx, HYBSNAP_SLOT_DONE));
    test_cond("scan: every slot is DONE after completion", !hybsnapScanSlotNotDone(ctx, 0, 0) && !hybsnapScanSlotNotDone(ctx, 1, 3));
    test_cond("scan: all dicts unfrozen at the end", hybsnapTestAllUnfrozen(stores));
    /* With the on-demand freeze a dict left mid-rehash at start may finish
     * rehashing through main-thread reads before the thread reaches it (the
     * thread never rehashes: it only reads a layout that holds still). */
    printf("scan: mid-rehash dict %s mid-rehash when its slot completed\n", st->rehashing_at_done ? "still" : "no longer");
    {
        long t, sc, dn; long long kd;
        hybsnapScanProgress(ctx, &t, &sc, &dn, &kd);
        printf("scan: progress total %ld scanned %ld done %ld keys %lld (empty slots %d)\n", t, sc, dn, kd, empty_slots);
        test_cond("scan: progress counters consistent (the allocated dicts)",
                  t == HYBSNAP_TEST_STORES * HYBSNAP_TEST_DICTS - empty_slots && sc == t && dn == t && kd == total_keys);
    }
    hybsnapScanRelease(ctx);

    /* ---- Abort mid-scan ---- */
    memset(st->seen, 0, sizeof(st->seen));
    memset(st->done_calls, 0, sizeof(st->done_calls));
    memset(st->done_aborted, 0, sizeof(st->done_aborted));
    atomicSet(st->dispatched, 0);
    atomicSet(st->bad, 0);
    st->dispatch_sleep_us = accurate ? 200 : 100;   /* 5000-key slot takes >= 0.5 s */
    ctx = hybsnapScanCreate(stores, HYBSNAP_TEST_STORES, hybsnapTestDispatch, hybsnapTestSlotDone, st);
    st->ctx = ctx;
    test_cond("abort: start", hybsnapScanStart(ctx) == C_OK);
    /* Wait until the thread is inside the first big slot. */
    long d0;
    do { usleep(1000); atomicGet(st->dispatched, d0); } while (d0 < 50 && !hybsnapScanIsFinished(ctx));
    long long t0 = hybsnapTestNowNs();
    hybsnapScanAbort(ctx);
    long long abort_ms = (hybsnapTestNowNs() - t0) / 1000000;
    atomicGet(st->dispatched, dispatched);
    int aborted_slots = 0, normal_slots = 0, calls = 0, twice = 0;
    for (int s = 0; s < HYBSNAP_TEST_STORES; s++)
        for (int didx = 0; didx < HYBSNAP_TEST_DICTS; didx++) {
            calls += st->done_calls[s][didx];
            if (st->done_calls[s][didx] > 1) twice++;
            if (!st->done_calls[s][didx]) continue;   /* not reached: the save never touched it */
            if (st->done_aborted[s][didx]) aborted_slots++; else normal_slots++;
        }
    printf("abort: after %ld dispatched keys, abort took %lld ms; %d slots aborted, %d completed normally\n",
           dispatched, abort_ms, aborted_slots, normal_slots);
    test_cond("abort: stopped well before the scan would have finished", dispatched < total_keys);
    test_cond("abort: returned promptly (< 500 ms)", abort_ms < 500);
    test_cond("abort: thread finished", hybsnapScanIsFinished(ctx));
    test_cond("abort: no slot completed twice, the reached ones once", twice == 0 && calls == aborted_slots + normal_slots && calls <= HYBSNAP_TEST_STORES * HYBSNAP_TEST_DICTS);
    test_cond("abort: interrupted slots reported as aborted", aborted_slots >= 1);
    test_cond("abort: slots scanned before the abort completed normally", normal_slots >= 1);
    test_cond("abort: all slots DONE", hybsnapTestAllState(ctx, HYBSNAP_SLOT_DONE));
    test_cond("abort: all dicts unfrozen", hybsnapTestAllUnfrozen(stores));
    hybsnapScanRelease(ctx);

    /* ---- Credit: inside a section the thread reads only its grant ---- */
    memset(st->seen, 0, sizeof(st->seen));
    memset(st->done_calls, 0, sizeof(st->done_calls));
    memset(st->done_aborted, 0, sizeof(st->done_aborted));
    atomicSet(st->dispatched, 0);
    atomicSet(st->bad, 0);
    st->dispatch_sleep_us = 0;
    unsigned long saved_w = hybsnapGetWindowBuckets();
    hybsnapSetWindowBuckets(64);
    ctx = hybsnapScanCreate(stores, HYBSNAP_TEST_STORES, hybsnapTestDispatch, hybsnapTestSlotDone, st);
    st->ctx = ctx;
    hybsnapSectionOpen();
    test_cond("credit: start with a write section open", hybsnapScanStart(ctx) == C_OK);
    long long stalls = 0;
    for (int i = 0; i < 2000 && stalls == 0; i++) { usleep(1000); atomicGet(ctx->stalls, stalls); }
    long first;
    atomicGet(st->dispatched, first);
    usleep(20000);
    atomicGet(st->dispatched, dispatched);
    test_cond("credit: the thread read its grant and waits at hi (64 buckets: d0, then part of d2)",
              stalls == 1 && first > 0 && dispatched == first && first < total_keys && !hybsnapScanIsFinished(ctx));
    hybsnapSectionOpen();   /* a later hook of the same command: no grant */
    usleep(20000);
    long same;
    atomicGet(st->dispatched, same);
    test_cond("credit: a later hook of the same command does not raise hi", ctx->grants == 0 && same == first);
    hybsnapSectionGrant();  /* the next statement of EXEC or a script: the thread is past mid, the next 64 buckets are granted */
    long long stalls2 = stalls;
    for (int i = 0; i < 2000 && stalls2 == stalls; i++) { usleep(1000); atomicGet(ctx->stalls, stalls2); }
    atomicGet(st->dispatched, dispatched);
    test_cond("credit: a statement boundary inside the section raised hi, the thread read on and waits again",
              ctx->grants == 1 && stalls2 == 2 && dispatched > first && dispatched < total_keys);
    hybsnapSectionClose();   /* no limit outside sections */
    while (!hybsnapScanIsFinished(ctx)) usleep(100);
    hybsnapScanHandleCompletions(ctx);
    atomicGet(st->dispatched, dispatched);
    atomicGet(st->bad, bad);
    atomicGet(ctx->stalls, stalls);
    test_cond("credit: after the section closed the thread ran to the end, every key once, no further wait",
              dispatched == total_keys && bad == 0 && stalls == 2);
    hybsnapScanRelease(ctx);
    hybsnapSetWindowBuckets(saved_w);

    /* ---- Abort while the aborting command holds the section open ---- */
    atomicSet(st->dispatched, 0);
    ctx = hybsnapScanCreate(stores, HYBSNAP_TEST_STORES, hybsnapTestDispatch, hybsnapTestSlotDone, st);
    st->ctx = ctx;
    hybsnapSectionOpen();
    test_cond("abort-in-section: start", hybsnapScanStart(ctx) == C_OK);
    usleep(5000);
    t0 = hybsnapTestNowNs();
    hybsnapScanAbort(ctx);
    abort_ms = (hybsnapTestNowNs() - t0) / 1000000;
    test_cond("abort-in-section: abort returned promptly although the section is still open", abort_ms < 500 && hybsnapScanIsFinished(ctx));
    test_cond("abort-in-section: all slots DONE, dicts unfrozen", hybsnapTestAllState(ctx, HYBSNAP_SLOT_DONE) && hybsnapTestAllUnfrozen(stores));
    hybsnapScanRelease(ctx);
    hybsnapSectionClose();

    /* ---- Positions and grants (no thread) ---- */
    ctx = hybsnapScanCreate(stores, HYBSNAP_TEST_STORES, hybsnapTestDispatch, hybsnapTestSlotDone, st);
    st->ctx = ctx;
    for (int s2 = 0; s2 < HYBSNAP_TEST_STORES; s2++)
        for (int didx = 0; didx < HYBSNAP_TEST_DICTS; didx++) {
            dict *d = kvstoreGetDict(stores[s2], didx);
            while (d && dictIsRehashing(d)) dictRehash(d, 1000);   /* one table each, then one rehash set up below */
        }
    dict *r12 = kvstoreGetDict(stores[1], 2);   /* 100 keys */
    unsigned long r12n = DICTHT_SIZE(r12->ht_size_exp[0]);
    dictExpand(r12, r12n * 2);
    dictRehash(r12, 5);
    hybsnapScanFreeze(ctx);
    {
        uint64_t o00 = hybsnapOrd(ctx, 0, 0), o02 = hybsnapOrd(ctx, 0, 2), o03 = hybsnapOrd(ctx, 0, 3);
        uint64_t o10 = hybsnapOrd(ctx, 1, 0), o11 = hybsnapOrd(ctx, 1, 1), o12 = hybsnapOrd(ctx, 1, 2);
        test_cond("positions: the scan order is integer order (layout, table 0, table 1; dicts; stores)",
                  HYBSNAP_POS(o02, 0, 0) < HYBSNAP_POS(o02, 1, 0) && HYBSNAP_POS(o02, 1, (1UL << HYBSNAP_POS_IDX_BITS) - 1) < HYBSNAP_POS(o02, 2, 0) &&
                  HYBSNAP_POS(o02, 2, (1UL << HYBSNAP_POS_IDX_BITS) - 1) < HYBSNAP_POS(o03, 0, 0) && HYBSNAP_POS(o03, 2, 0) < HYBSNAP_POS(o10, 0, 0) &&
                  ctx->pos_end > HYBSNAP_POS(hybsnapOrd(ctx, 1, 3), 2, 0));
        dict *d00 = kvstoreGetDict(stores[0], 0), *d02 = kvstoreGetDict(stores[0], 2), *d03 = kvstoreGetDict(stores[0], 3);
        unsigned long n00 = DICTHT_SIZE(d00->ht_size_exp[0]), n02 = DICTHT_SIZE(d02->ht_size_exp[0]);
        uint64_t mid, h = hybsnapWindowAdvance(ctx, HYBSNAP_POS(o00, 1, 0), n00 + 2, &mid);
        test_cond("grants: the empty slot (no dict) is a gap: a grant past dict 0 goes on in dict 2",
                  h == HYBSNAP_POS(o02, 1, 2) && mid == HYBSNAP_POS(o00, 1, (n00 + 2) / 2) &&
                  d00->pauserehash == 1 && d02->pauserehash == 1 && d03->pauserehash == 0);
        h = hybsnapWindowAdvance(ctx, HYBSNAP_POS(o02, 1, n02 - 3), 5, &mid);
        test_cond("grants: a grant crossing into the next dict pauses it first and counts from its table 0",
                  h == HYBSNAP_POS(o03, 1, 2) && d03->pauserehash == 1 && d02->pauserehash == 1);
        unsigned long rehashidx = (unsigned long)r12->rehashidx;
        h = hybsnapWindowAdvance(ctx, HYBSNAP_POS(o12, 0, 0), (r12n - rehashidx) + 3, &mid);
        test_cond("grants: a rehashing dict counts table 0 from rehashidx, then table 1",
                  rehashidx > 0 && dictIsRehashing(r12) && h == HYBSNAP_POS(o12, 2, 3) && r12->pauserehash == 1);
        /* The cap, with the thread found past the grant (it ran free outside
         * sections and claimed store 1's dict 1). */
        dict *d11 = kvstoreGetDict(stores[1], 1);
        hybsnapWordStore(d11, HYBSNAP_WORD(ctx->gen, HYBSNAP_SLOT_SCANNING));
        __atomic_store_n(&hybsnap_lo, HYBSNAP_POS(o00, 1, 0), __ATOMIC_SEQ_CST);
        __atomic_store_n(&hybsnap_cur, HYBSNAP_POS(o11, 1, 5), __ATOMIC_SEQ_CST);
        hybsnapWindowCap(ctx);
        uint64_t c1 = HYBSNAP_POS(o11, 1, 5) + 1;
        test_cond("cap: a thread found past H gets hi = cur + 1, and the dict it is in is paused",
                  ctx->hi == c1 && __atomic_load_n(&hybsnap_hi, __ATOMIC_SEQ_CST) == c1 && d11->pauserehash == 1);
        hybsnapSectionOpen();
        hybsnapBucketPos p_in = hybsnapBucketPosition(ctx, 1, 1, d11, 0, 3, NULL);
        hybsnapBucketPos p_ahead = hybsnapBucketPosition(ctx, 1, 1, d11, 0, 6, NULL);
        hybsnapSectionClose();
        test_cond("cap: below cur + 1 the bucket may be being read, at it the thread cannot be",
                  p_in == HYBSNAP_POS_INSIDE && p_ahead == HYBSNAP_POS_AHEAD);
        (void)o10;
    }
    hybsnapScanAbort(ctx);   /* frozen, never started: unfreezes and unpauses */
    hybsnapScanRelease(ctx);
    hybsnapWindowReset(NULL);

    /* ---- Bucket positions: ahead, inside and behind the window, and the
     * layout phase. Simulates one write section spanning a window publish,
     * with no thread running. ---- */
    ctx = hybsnapScanCreate(stores, HYBSNAP_TEST_STORES, hybsnapTestDispatch, hybsnapTestSlotDone, st);
    st->ctx = ctx;
    hybsnapScanFreeze(ctx);
    {
        dict *d = kvstoreGetDict(stores[0], 3);   /* populated */
        unsigned long size = DICTHT_SIZE(d->ht_size_exp[0]), mid = size / 2;
        uint64_t o = hybsnapOrd(ctx, 0, 3), b0 = HYBSNAP_POS(o, 1, 0);
        test_cond("positions: test dict exists and has at least 8 buckets", d != NULL && size >= 8);
        hybsnapScanAttach(ctx, 0, 3, d, 1);            /* the pause, as the guard would */
        hybsnapWordStore(d, HYBSNAP_WORD(ctx->gen, HYBSNAP_SLOT_SCANNING));   /* as the thread's claim would */
        hybsnapTestWindow(ctx, b0, b0 + mid);          /* the thread may read [0, mid) */
        hybsnapSectionOpen();                           /* a command opens its write section */
        hybsnapBucketPos p_ahead = hybsnapBucketPosition(ctx, 0, 3, d, 0, mid + 1, NULL);
        hybsnapTestWindow(ctx, b0 + mid, b0 + size);   /* a grant at a later statement of the section moves the window over it */
        hybsnapBucketPos p_after = hybsnapBucketPosition(ctx, 0, 3, d, 0, mid + 1, NULL);
        hybsnapBucketPos p_behind = hybsnapBucketPosition(ctx, 0, 3, d, 0, 0, NULL);
        hybsnapSectionClose();
        test_cond("positions: a bucket at or beyond hi is ahead", p_ahead == HYBSNAP_POS_AHEAD);
        test_cond("positions: the same bucket is inside once a grant moved the window over it", p_after == HYBSNAP_POS_INSIDE);
        test_cond("positions: a bucket below lo is behind", p_behind == HYBSNAP_POS_BEHIND);

        /* The layout phase: the thread has claimed the dict and reads its table
         * sizes, no chain yet (hi just past the layout position). Every bucket
         * of every table is ahead of it. */
        hybsnapTestWindow(ctx, HYBSNAP_POS(o, 0, 0), HYBSNAP_POS(o, 0, 0) + 1);
        hybsnapSectionOpen();
        int layout_ahead = 1;
        for (int t = 0; t < 2; t++) {
            if (!d->ht_table[t]) continue;
            unsigned long tsize = DICTHT_SIZE(d->ht_size_exp[t]);
            unsigned long probe[3] = { 0, tsize / 2, tsize - 1 };
            for (int j = 0; j < 3; j++)
                if (hybsnapBucketPosition(ctx, 0, 3, d, t, probe[j], NULL) != HYBSNAP_POS_AHEAD) layout_ahead = 0;
        }
        hybsnapSectionClose();
        hybsnapWindowReset(NULL);
        test_cond("positions: in the layout phase every bucket of every table is ahead of the window", layout_ahead);
    }
    hybsnapScanAbort(ctx);   /* frozen, never started: unfreezes */
    hybsnapScanRelease(ctx);

    /* ---- Empty stores ---- */
    {
        kvstore *empty[2];
        empty[0] = kvstoreCreate(&KvstoreTestType, &noval, 0, KVSTORE_ALLOCATE_DICTS_ON_DEMAND);
        empty[1] = kvstoreCreate(&KvstoreTestType, &noval, 3, KVSTORE_ALLOCATE_DICTS_ON_DEMAND);
        atomicSet(st->dispatched, 0);
        st->dispatch_sleep_us = 0;
        hybsnapScanCtx *ectx = hybsnapScanCreate(empty, 2, hybsnapTestDispatch, NULL, st);
        st->ctx = NULL;
        test_cond("empty: start", hybsnapScanStart(ectx) == C_OK);
        while (!hybsnapScanIsFinished(ectx)) usleep(100);
        hybsnapScanHandleCompletions(ectx);
        atomicGet(st->dispatched, dispatched);
        long et, esc, edn; long long ekd;
        hybsnapScanProgress(ectx, &et, &esc, &edn, &ekd);
        test_cond("empty: nothing to scan, nothing visited, zero dispatches", et == 0 && edn == 0 && esc == 0 && dispatched == 0);
        hybsnapScanRelease(ectx);
        kvstoreRelease(empty[0]);
        kvstoreRelease(empty[1]);
    }

    for (int s = 0; s < HYBSNAP_TEST_STORES; s++) kvstoreRelease(stores[s]);
    zfree(st);
}

/* ---- Placement / range-aware allocator tests ---- */
static void hybsnapPlacementTests(void) {
    if (!hybsnapArenaExists()) return;
    server.save_small_max_bytes = 4096;

    /* Generic allocator paths keep arena memory in the arena. */
    {
        char *p = zmalloc_hybrid(100);
        p = zrealloc(p, 5000);
        test_cond("zrealloc() of an arena pointer stays in the arena (grow)", zmalloc_in_hybrid_arena(p));
        p = zrealloc(p, 40);
        test_cond("zrealloc() of an arena pointer stays in the arena (shrink)", zmalloc_in_hybrid_arena(p));
        zfree(p);
        char *q = zmalloc(100);
        q = zrealloc(q, 5000);
        test_cond("zrealloc() of a default pointer stays outside the arena", !zmalloc_in_hybrid_arena(q));
        zfree(q);

        const int N = 2000;
        void **a = zmalloc(sizeof(void*) * N);
        for (int i = 0; i < N; i++) a[i] = zmalloc_hybrid(64);
        for (int i = 0; i < N; i++) zfree(a[i]);                     /* generic zfree */
        int leaked = 0;
        for (int i = 0; i < N; i++) { a[i] = zmalloc(64); if (zmalloc_in_hybrid_arena(a[i])) leaked++; }
        for (int i = 0; i < N; i++) zfree(a[i]);
        test_cond("arena blocks freed with plain zfree() never come back from zmalloc()", leaked == 0);
        for (int i = 0; i < N; i++) a[i] = zmalloc_hybrid(64);
        for (int i = 0; i < N; i++) zfree_with_size(a[i], 64);       /* sized generic free */
        leaked = 0;
        for (int i = 0; i < N; i++) { a[i] = zmalloc(64); if (zmalloc_in_hybrid_arena(a[i])) leaked++; }
        for (int i = 0; i < N; i++) zfree(a[i]);
        test_cond("arena blocks freed with zfree_with_size() never come back from zmalloc()", leaked == 0);
        zfree(a);
#if defined(HAVE_HYBRID_ARENA) && defined(HAVE_DEFRAG)
        void *nt = zmalloc_hybrid_arena_no_tcache(300);   /* the no-tcache entry points exist only with the arena and defrag */
        test_cond("zmalloc_hybrid_arena_no_tcache() allocates in the arena", zmalloc_in_hybrid_arena(nt));
        zfree_no_tcache(nt);
#endif
    }

    /* sds growth through the sds API keeps the arena. */
    {
        sds s = sdsnewlen("x", 1);
        sds t = hybsnapSdsMove(s, 1);
        test_cond("sds moved into the arena", zmalloc_in_hybrid_arena(sdsAllocPtr(t)));
        t = sdsMakeRoomFor(t, 20000);
        test_cond("sdsMakeRoomFor() on an arena sds stays in the arena", zmalloc_in_hybrid_arena(sdsAllocPtr(t)));
        sdsfree(t);
    }

    /* Classification table. */
    test_cond("classify: small raw string is NOFORK", hybsnapClassify(OBJ_STRING, OBJ_ENCODING_RAW, 100) == HYBSNAP_NOFORK);
    test_cond("classify: 4096-byte raw string is NOFORK", hybsnapClassify(OBJ_STRING, OBJ_ENCODING_RAW, 4096) == HYBSNAP_NOFORK);
    test_cond("classify: 4097-byte raw string is FORK", hybsnapClassify(OBJ_STRING, OBJ_ENCODING_RAW, 4097) == HYBSNAP_FORK);
    test_cond("classify: INT / EMBSTR are NOFORK regardless of size",
              hybsnapClassify(OBJ_STRING, OBJ_ENCODING_INT, 0) == HYBSNAP_NOFORK &&
              hybsnapClassify(OBJ_STRING, OBJ_ENCODING_EMBSTR, 100000) == HYBSNAP_NOFORK);
    test_cond("classify: small listpack hash/set/zset/list are NOFORK",
              hybsnapClassify(OBJ_HASH, OBJ_ENCODING_LISTPACK, 100) == HYBSNAP_NOFORK &&
              hybsnapClassify(OBJ_HASH, OBJ_ENCODING_LISTPACK_EX, 100) == HYBSNAP_NOFORK &&
              hybsnapClassify(OBJ_SET, OBJ_ENCODING_LISTPACK, 100) == HYBSNAP_NOFORK &&
              hybsnapClassify(OBJ_SET, OBJ_ENCODING_INTSET, 100) == HYBSNAP_NOFORK &&
              hybsnapClassify(OBJ_ZSET, OBJ_ENCODING_LISTPACK, 100) == HYBSNAP_NOFORK &&
              hybsnapClassify(OBJ_LIST, OBJ_ENCODING_LISTPACK, 100) == HYBSNAP_NOFORK);
    test_cond("classify: multi-allocation encodings and other types are FORK",
              hybsnapClassify(OBJ_HASH, OBJ_ENCODING_HT, 10) == HYBSNAP_FORK &&
              hybsnapClassify(OBJ_SET, OBJ_ENCODING_HT, 10) == HYBSNAP_FORK &&
              hybsnapClassify(OBJ_ZSET, OBJ_ENCODING_SKIPLIST, 10) == HYBSNAP_FORK &&
              hybsnapClassify(OBJ_LIST, OBJ_ENCODING_QUICKLIST, 10) == HYBSNAP_FORK &&
              hybsnapClassify(OBJ_STREAM, OBJ_ENCODING_STREAM, 10) == HYBSNAP_FORK &&
              hybsnapClassify(OBJ_MODULE, OBJ_ENCODING_RAW, 10) == HYBSNAP_FORK &&
              hybsnapClassify(OBJ_ARRAY, OBJ_ENCODING_SLICED_ARRAY, 10) == HYBSNAP_FORK);

    /* Placement through kvobjSet(). */
    sds key = sdsnew("placement-key");
    char buf[20000];
    memset(buf, 'v', sizeof(buf));
    {
        kvobj *kv = kvobjSet(key, createRawStringObject(buf, 100), 0);
        test_cond("kvobj header (raw string) is in the arena", zmalloc_in_hybrid_arena(kv));
        test_cond("small raw string value moved into the arena", zmalloc_in_hybrid_arena(sdsAllocPtr(kv->ptr)) && hybsnapKeyClass(kv) == HYBSNAP_NOFORK);
        test_cond("value content preserved after the move", sdslen(kv->ptr) == 100 && memcmp(kv->ptr, buf, 100) == 0);
        /* Unbounded in-place growth keeps the arena; the policy hook moves it out. */
        kv->ptr = sdscatlen(kv->ptr, buf, 10000);
        test_cond("grown string still in the arena before the policy hook", zmalloc_in_hybrid_arena(sdsAllocPtr(kv->ptr)));
        hybsnapPlaceValue(kv);
        test_cond("hybsnapPlaceValue() moves a string over the threshold out of the arena",
                  !zmalloc_in_hybrid_arena(sdsAllocPtr(kv->ptr)) && zmalloc_in_hybrid_arena(kv) && hybsnapKeyClass(kv) == HYBSNAP_FORK);
        test_cond("content preserved after moving out", sdslen(kv->ptr) == 10100 && memcmp(kv->ptr, buf, 100) == 0);
        decrRefCount(kv);
    }
    {
        kvobj *kv = kvobjSet(key, createRawStringObject(buf, 10000), 0);
        test_cond("large raw string: header in arena, value outside, class FORK",
                  zmalloc_in_hybrid_arena(kv) && !zmalloc_in_hybrid_arena(sdsAllocPtr(kv->ptr)) && hybsnapKeyClass(kv) == HYBSNAP_FORK);
        decrRefCount(kv);
    }
    {
        kvobj *kv = kvobjSet(key, createStringObject("tiny", 4), 0);
        test_cond("embstr value: header in arena, class NOFORK",
                  kv->encoding == OBJ_ENCODING_EMBSTR && zmalloc_in_hybrid_arena(kv) && hybsnapKeyClass(kv) == HYBSNAP_NOFORK);
        decrRefCount(kv);
        kv = kvobjSet(key, createStringObjectFromLongLongForValue(1234567), 0);
        test_cond("int value: header in arena, class NOFORK",
                  kv->encoding == OBJ_ENCODING_INT && zmalloc_in_hybrid_arena(kv) && hybsnapKeyClass(kv) == HYBSNAP_NOFORK);
        decrRefCount(kv);
    }
    {
        robj *o = createObject(OBJ_HASH, lpNew(0));
        o->encoding = OBJ_ENCODING_LISTPACK;
        o->ptr = lpAppend(o->ptr, (unsigned char*)"field", 5);
        o->ptr = lpAppend(o->ptr, (unsigned char*)"value", 5);
        kvobj *kv = kvobjSet(key, o, 0);
        test_cond("small listpack hash moved into the arena", zmalloc_in_hybrid_arena(kv->ptr) && hybsnapKeyClass(kv) == HYBSNAP_NOFORK);
        unsigned char *p = lpFirst(kv->ptr);
        int64_t l; unsigned char *sv = lpGet(p, &l, NULL);
        test_cond("listpack content preserved", sv && l == 5 && memcmp(sv, "field", 5) == 0);
        /* Grow in place past the threshold: stays in the arena, class stays NOFORK by placement. */
        kv->ptr = lpAppend(kv->ptr, (unsigned char*)buf, 6000);
        test_cond("listpack grown in place stays in the arena", zmalloc_in_hybrid_arena(kv->ptr) && hybsnapKeyClass(kv) == HYBSNAP_NOFORK);
        hybsnapPlaceValue(kv);
        test_cond("policy hook moves an oversized listpack out", !zmalloc_in_hybrid_arena(kv->ptr) && hybsnapKeyClass(kv) == HYBSNAP_FORK);
        decrRefCount(kv);
    }
    {
        robj *o = createIntsetObject();
        uint8_t ok;
        for (long long i = 0; i < 50; i++) o->ptr = intsetAdd(o->ptr, i * 7, &ok);
        kvobj *kv = kvobjSet(key, o, 0);
        test_cond("intset moved into the arena", zmalloc_in_hybrid_arena(kv->ptr) && hybsnapKeyClass(kv) == HYBSNAP_NOFORK && intsetLen(kv->ptr) == 50);
        decrRefCount(kv);
    }
    {
        robj *o = createSetObject();   /* hashtable encoding */
        kvobj *kv = kvobjSet(key, o, 0);
        test_cond("hashtable set: header in arena, dict outside, class FORK",
                  zmalloc_in_hybrid_arena(kv) && !zmalloc_in_hybrid_arena(kv->ptr) && hybsnapKeyClass(kv) == HYBSNAP_FORK);
        decrRefCount(kv);
    }
    sdsfree(key);

    /* Keyspace-style dicts (hybrid_arena_alloc) put struct, tables and entry nodes in the arena. */
    {
        dictType nofork = KvstoreDictTestType;
        nofork.no_value = 1;
        nofork.hybrid_arena_alloc = 1;
        dictType plain = KvstoreDictTestType;
        plain.no_value = 1;
        kvstore *ks = kvstoreCreate(&KvstoreTestType, &nofork, 0, KVSTORE_ALLOCATE_DICTS_ON_DEMAND);
        kvstore *kp = kvstoreCreate(&KvstoreTestType, &plain, 0, KVSTORE_ALLOCATE_DICTS_ON_DEMAND);
        dictSetResizeEnabled(DICT_RESIZE_ENABLE);
        for (int i = 0; i < 3000; i++) {
            serverAssert(kvstoreDictAddRaw(ks, 0, hybsnapTestKey(0, 0, i), NULL) != NULL);
            serverAssert(kvstoreDictAddRaw(kp, 0, hybsnapTestKey(0, 0, i), NULL) != NULL);
        }
        dict *ds = kvstoreGetDict(ks, 0), *dp = kvstoreGetDict(kp, 0);
        while (dictIsRehashing(ds)) dictRehash(ds, 100);
        test_cond("hybrid_arena_alloc dict: kvstore struct, dict struct and bucket array are in the arena",
                  zmalloc_in_hybrid_arena(ks) && zmalloc_in_hybrid_arena(ds) && zmalloc_in_hybrid_arena(ds->ht_table[0]));
        test_cond("default dict: struct and bucket array are outside the arena",
                  !zmalloc_in_hybrid_arena(kp) && !zmalloc_in_hybrid_arena(dp) && !zmalloc_in_hybrid_arena(dp->ht_table[0]));
        /* Force another expand (rehash allocates a new table) and check it too. */
        for (int i = 3000; i < 9000; i++)
            serverAssert(kvstoreDictAddRaw(ks, 0, hybsnapTestKey(0, 0, i), NULL) != NULL);
        test_cond("hybrid_arena_alloc dict: table allocated by rehash is in the arena",
                  zmalloc_in_hybrid_arena(ds->ht_table[0]) && (ds->ht_table[1] == NULL || zmalloc_in_hybrid_arena(ds->ht_table[1])));
        kvstoreRelease(ks);
        kvstoreRelease(kp);
    }
}

/* ---- Overlay tests (kvstore level: entries are plain C strings) ---- */

static void *ovTestClone(void *priv, int didx, void *orig) {
    UNUSED(didx);
    struct ovTestCounters *c = ((struct hybsnapGuardPriv *)priv)->user;
    c->clones++;
    return zstrdup(orig);
}

static void ovTestFree(void *priv, int didx, void *orig) {
    UNUSED(didx);
    struct ovTestCounters *c = ((struct hybsnapGuardPriv *)priv)->user;
    c->frees++;
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

/* The save holds a reference on every fork-instant dict and walks
 * its own table of them. A store that frees, empties or moves one of them
 * during the save detaches it; the save reads it to the end and frees it. */
static void hybsnapRefcountTests(void) {
    dictType noval = KvstoreDictTestType;
    noval.no_value = 1;
    noval.keyCompare = hybsnapTestKeyCompare;
    kvstore *stores[1];
    stores[0] = kvstoreCreate(&KvstoreTestType, &noval, HYBSNAP_TEST_DICT_BITS, KVSTORE_ALLOCATE_DICTS_ON_DEMAND);
    kvstore *ks = stores[0];
    /* dict 0 and 3: fork-instant content; dict 1: allocated but empty; dict 2: not allocated. */
    for (int i = 0; i < 50; i++) {
        serverAssert(kvstoreDictAddRaw(ks, 0, hybsnapTestKey(0, 0, i), NULL) != NULL);
        serverAssert(kvstoreDictAddRaw(ks, 3, hybsnapTestKey(0, 3, i), NULL) != NULL);
    }
    serverAssert(kvstoreDictAddRaw(ks, 1, hybsnapTestKey(0, 1, 0), NULL) != NULL);
    serverAssert(kvstoreDictDelete(ks, 1, "k:0:1:0") == DICT_OK);
    struct hybsnapScanTestState *st = zcalloc(sizeof(*st));
    st->inserted[0][0] = st->inserted[0][3] = 50;
    st->stores = stores;
    struct ovTestCounters cnt = {0, 0};
    void *privs[1] = { &cnt };
    kvstoreSnapHooks hooks = { .clone = ovTestClone, .free_orig = ovTestFree, .enter = hybsnapOverlayEnter };
    long dispatched, bad, t, sc, dn;
    long long kd;

    /* A credit of one bucket: each save below starts inside a section, so the
     * thread reads a bucket of dict 0 per guard hook and never reaches dict 3
     * before the section closes. */
    unsigned long saved_w = hybsnapGetWindowBuckets();
    hybsnapSetWindowBuckets(1);

    /* ---- A: the table and the references; a dict moved out during the save ---- */
    hybsnapScanCtx *ctx = hybsnapScanCreate(stores, 1, hybsnapTestDispatch, hybsnapTestSlotDone, st);
    st->ctx = ctx;
    hybsnapScanSetOverlay(ctx, &hooks, privs);
    hybsnapSectionOpen();   /* the thread starts capped */
    test_cond("refs: start with a section open", hybsnapScanStart(ctx) == C_OK);
    dict *d0 = kvstoreGetDict(ks, 0), *d3 = kvstoreGetDict(ks, 3);
    test_cond("refs: the save holds one reference on every non-empty dict, none on an empty one",
              kvstoreDictRefcount(d0) == 2 && kvstoreDictRefcount(d3) == 2 && kvstoreDictRefcount(kvstoreGetDict(ks, 1)) == 1);
    test_cond("refs: an empty or missing dict holds nothing of the save: DONE from the start",
              hybsnapScanSlotState(ctx, 0, 3) == HYBSNAP_SLOT_PENDING && hybsnapScanSlotState(ctx, 0, 1) == HYBSNAP_SLOT_DONE &&
              !hybsnapScanSlotNotDone(ctx, 0, 2) && kvstoreRefcount(ks) == 1 + 3);
    serverAssert(kvstoreDictAddRaw(ks, 1, hybsnapTestKey(0, 1, 5), NULL) != NULL);   /* post-fork keys the dispatch would flag as bad */
    serverAssert(kvstoreDictAddRaw(ks, 2, hybsnapTestKey(0, 2, 5), NULL) != NULL);
    test_cond("refs: writes to dicts outside the table attach nothing",
              hybsnapScanSlotState(ctx, 0, 1) == HYBSNAP_SLOT_DONE && hybsnapScanSlotState(ctx, 0, 2) == HYBSNAP_SLOT_DONE &&
              !kvstoreOverlayIsFrozen(ks, 1) && !kvstoreOverlayIsFrozen(ks, 2) &&
              hybsnapMeta(kvstoreGetDict(ks, 1))->ext == NULL && hybsnapMeta(kvstoreGetDict(ks, 2))->ext == NULL);
    /* A write to a table dict the thread has not reached attaches it (an overlayed bucket whose duplicate chain lacks the key). */
    serverAssert(kvstoreDictDelete(ks, 3, "k:0:3:7") == DICT_OK);
    test_cond("refs: the write attached dict 3", kvstoreOverlayIsFrozen(ks, 3) && kvstoreDictSize(ks, 3) == 49);
    /* Move dict 3 to another store while the save holds it (the slot-trim path
     * of atomic slot migration), then release that store the way BIO does. */
    kvstore *tmp = kvstoreCreate(&KvstoreTestType, &noval, HYBSNAP_TEST_DICT_BITS, KVSTORE_ALLOCATE_DICTS_ON_DEMAND);
    kvstoreMoveDict(ks, tmp, 3);
    test_cond("refs: the moved dict left the slot with its logical size; the save still holds it",
              kvstoreGetDict(ks, 3) == NULL && kvstoreGetDict(tmp, 3) == d3 && kvstoreDictRefcount(d3) == 2 &&
              kvstoreSize(ks) == 52 && kvstoreSize(tmp) == 49 && kvstoreDictDetached(d3) &&
              hybsnapScanSlotState(ctx, 0, 3) == HYBSNAP_SLOT_PENDING);
    kvstoreRelease(tmp);
    test_cond("refs: the released store left the dict to the save", kvstoreDictRefcount(d3) == 1 && d3->type->userdata == tmp);
    hybsnapSectionClose();
    while (!hybsnapScanIsFinished(ctx)) { hybsnapScanHandleCompletions(ctx); usleep(100); }
    hybsnapScanHandleCompletions(ctx);
    atomicGet(st->dispatched, dispatched);
    atomicGet(st->bad, bad);
    hybsnapScanProgress(ctx, &t, &sc, &dn, &kd);
    test_cond("refs: the fork-instant keys of both dicts were dispatched, the moved one included; the post-fork ones never",
              dispatched == 100 && bad == 0 && kd == 100 && st->seen[0][3][7]);
    test_cond("refs: two table entries, both completed, the moved dict freed by the save",
              t == 2 && sc == 2 && dn == 2 && st->done_calls[0][0] == 1 && st->done_calls[0][3] == 1 &&
              hybsnapScanRetainedDicts(ctx) == 1 && !hybsnapScanSlotNotDone(ctx, 0, 3));
    hybsnapScanRelease(ctx);

    /* ---- B: the store emptied (FLUSH) while the save reads it ---- */
    memset(st->seen, 0, sizeof(st->seen));
    memset(st->done_calls, 0, sizeof(st->done_calls));
    atomicSet(st->dispatched, 0);
    atomicSet(st->bad, 0);
    st->inserted[0][1] = st->inserted[0][2] = 6;   /* k:0:1:5 and k:0:2:5 are fork-instant now */
    ctx = hybsnapScanCreate(stores, 1, hybsnapTestDispatch, hybsnapTestSlotDone, st);
    st->ctx = ctx;
    hybsnapScanSetOverlay(ctx, &hooks, privs);
    hybsnapSectionOpen();
    test_cond("refs: second save starts", hybsnapScanStart(ctx) == C_OK);
    d0 = kvstoreGetDict(ks, 0);
    kvstoreEmpty(ks, NULL);
    test_cond("refs: emptying the store hands its fork-instant dicts to the save",
              kvstoreSize(ks) == 0 && kvstoreGetDict(ks, 0) == NULL && kvstoreDictRefcount(d0) == 1 && kvstoreDictDetached(d0) &&
              hybsnapScanSlotState(ctx, 0, 0) != HYBSNAP_SLOT_DONE);
    serverAssert(kvstoreDictAddRaw(ks, 0, hybsnapTestKey(0, 0, 60), NULL) != NULL);   /* a fresh dict in the slot */
    test_cond("refs: the slot's new dict is not the save's", kvstoreGetDict(ks, 0) != d0 && !kvstoreOverlayIsFrozen(ks, 0));
    hybsnapSectionClose();
    while (!hybsnapScanIsFinished(ctx)) { hybsnapScanHandleCompletions(ctx); usleep(100); }
    hybsnapScanHandleCompletions(ctx);
    atomicGet(st->dispatched, dispatched);
    atomicGet(st->bad, bad);
    hybsnapScanProgress(ctx, &t, &sc, &dn, &kd);
    test_cond("refs: the detached dicts were walked to the end and freed by the save; the new key never dispatched",
              dispatched == 52 && bad == 0 && t == 3 && dn == 3 && hybsnapScanRetainedDicts(ctx) == 3 && kvstoreSize(ks) == 1);
    hybsnapScanRelease(ctx);

    /* ---- C: a table dict that empties under the save, in a store that frees empty dicts ---- */
    kvstore *fs = kvstoreCreate(&KvstoreTestType, &noval, HYBSNAP_TEST_DICT_BITS, KVSTORE_ALLOCATE_DICTS_ON_DEMAND | KVSTORE_FREE_EMPTY_DICTS);
    kvstore *fstores[1] = { fs };
    serverAssert(kvstoreDictAddRaw(fs, 3, hybsnapTestKey(0, 3, 0), NULL) != NULL);
    for (int i = 0; i < 20; i++) serverAssert(kvstoreDictAddRaw(fs, 0, hybsnapTestKey(0, 0, i), NULL) != NULL);
    memset(st->seen, 0, sizeof(st->seen));
    memset(st->done_calls, 0, sizeof(st->done_calls));
    atomicSet(st->dispatched, 0);
    atomicSet(st->bad, 0);
    st->inserted[0][0] = 20;
    st->inserted[0][1] = st->inserted[0][2] = 0;
    st->inserted[0][3] = 1;
    st->stores = fstores;
    ctx = hybsnapScanCreate(fstores, 1, hybsnapTestDispatch, hybsnapTestSlotDone, st);
    st->ctx = ctx;
    hybsnapScanSetOverlay(ctx, &hooks, privs);
    hybsnapSectionOpen();
    test_cond("refs: third save starts", hybsnapScanStart(ctx) == C_OK);
    dict *f3 = kvstoreGetDict(fs, 3);
    serverAssert(kvstoreDictDelete(fs, 3, "k:0:3:0") == DICT_OK);   /* deferred: an overlayed bucket without the key, the dict logically empty */
    kvstoreFreeDictIfNeeded(fs, 3);
    /* The overlayed bucket holds the dict's layout, so the emptied dict is not freed yet: it goes at the release. */
    test_cond("refs: the emptied dict is logically empty and stays with the save until the overlay is released",
              kvstoreGetDict(fs, 3) == f3 && kvstoreDictSize(fs, 3) == 0 && kvstoreDictRefcount(f3) == 2 &&
              !kvstoreDictDetached(f3) && hybsnapScanSlotState(ctx, 0, 3) == HYBSNAP_SLOT_PENDING && kvstoreSize(fs) == 20);
    hybsnapSectionClose();
    while (!hybsnapScanIsFinished(ctx)) { hybsnapScanHandleCompletions(ctx); usleep(100); }
    hybsnapScanHandleCompletions(ctx);
    atomicGet(st->dispatched, dispatched);
    atomicGet(st->bad, bad);
    test_cond("refs: its fork-instant key was still dispatched; the delete written back at release, the dict freed",
              dispatched == 21 && bad == 0 && hybsnapScanRetainedDicts(ctx) == 1 && kvstoreGetDict(fs, 3) == NULL);
    hybsnapScanRelease(ctx);
    hybsnapSetWindowBuckets(saved_w);
    kvstoreRelease(fs);
    kvstoreRelease(ks);
    zfree(st);
}

static void hybsnapOverlayTests(int abort_variant) {
    dictType noval = KvstoreDictTestType;
    noval.no_value = 1;
    noval.keyCompare = hybsnapTestKeyCompare;
    kvstore *stores[1];
    stores[0] = kvstoreCreate(&KvstoreTestType, &noval, HYBSNAP_TEST_DICT_BITS, KVSTORE_ALLOCATE_DICTS_ON_DEMAND);
    kvstore *ks = stores[0];
    const int N = 1000;
    char *orig5 = NULL, *orig7 = NULL;
    for (int i = 0; i < N; i++) {
        char *k = hybsnapTestKey(0, 0, i);
        if (i == 5) orig5 = k;
        if (i == 7) orig7 = k;
        serverAssert(kvstoreDictAddRaw(ks, 0, k, NULL) != NULL);
    }
    struct hybsnapScanTestState *st = zcalloc(sizeof(*st));
    st->inserted[0][0] = N;
    st->dispatch_sleep_us = abort_variant ? 50 : 0;
    struct ovTestCounters cnt = {0, 0};
    void *privs[1] = { &cnt };

    hybsnapScanCtx *ctx = hybsnapScanCreate(stores, 1, hybsnapTestDispatch, hybsnapTestSlotDone, st);
    st->ctx = ctx;
    kvstoreSnapHooks hooks = { .clone = ovTestClone, .free_orig = ovTestFree, .enter = hybsnapOverlayEnter };
    hybsnapScanSetOverlay(ctx, &hooks, privs);
    test_cond("overlay: start", hybsnapScanStart(ctx) == C_OK);
    test_cond("overlay: nothing is attached at start, the empty slot is DONE",
              !kvstoreOverlayIsFrozen(ks, 0) && !kvstoreOverlayIsFrozen(ks, 3) && !hybsnapScanSlotNotDone(ctx, 0, 3));
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

    /* Delete an untouched key: duplicate the bucket, then delete from the duplicate. */
    int table;
    link = kvstoreDictTwoPhaseUnlinkFind(ks, 0, "k:0:0:7", &table);
    test_cond("overlay: two-phase unlink find on a key pending save returns a temp link", link != NULL && dictGetKey(*link) != orig7);
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
    test_cond("overlay: the writes above opened a write section", (hybsnap_section_seq & 1) == 1);
    hybsnapSectionClose();
    if (abort_variant) {
        long d0;
        do { usleep(500); atomicGet(st->dispatched, d0); } while (d0 < 10 && !hybsnapScanIsFinished(ctx));
        hybsnapScanAbort(ctx);
    } else {
        while (!hybsnapScanIsFinished(ctx)) usleep(200);
        hybsnapScanHandleCompletions(ctx);
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

    /* After the drain: a single live dict with the logical contents. */
    test_cond("overlay: slots are unfrozen after completion", !kvstoreOverlayIsFrozen(ks, 0) && !kvstoreOverlayIsFrozen(ks, 3));
    frozen = kvstoreGetDict(ks, 0);
    test_cond("overlay: drained dict holds the clone, the re-added key and the new key",
              dictGetKey(dictFind(frozen, "k:0:0:5")) == clone5 &&
              dictGetKey(dictFind(frozen, "k:0:0:7")) == readd7 &&
              dictFind(frozen, "k:0:0:new") != NULL &&
              kvstoreGetDict(ks, 3) && dictFind(kvstoreGetDict(ks, 3), "k:0:3:0") != NULL);
    test_cond("overlay: drained sizes are consistent",
              dictSize(frozen) == (unsigned long)N + 1 && kvstoreDictSize(ks, 0) == (unsigned long)N + 1 &&
              kvstoreSize(ks) == (unsigned long long)N + 2);
    test_cond("overlay: every original of the overlayed buckets was freed", cnt.frees == cnt.clones && cnt.clones >= 2);
    has_new = 0; e5 = NULL;
    n = ovTestCountIter(ks, &has_new, &e5);
    test_cond("overlay: iteration after the drain matches", n == N + 2 && has_new && e5 == clone5);

    hybsnapScanRelease(ctx);
    kvstoreRelease(ks);
    zfree(st);
}

static int ovTestKeyIndex(const char *k) {
    int i = -1;
    return sscanf(k, "k:0:0:%d", &i) == 1 ? i : -1;
}

static void ovTestScanSeen(void *priv, const dictEntry *de, dictEntry **plink) {
    UNUSED(plink);
    int i = ovTestKeyIndex(dictGetKey(de));
    if (i >= 0 && i < 300) ((int *)priv)[i]++;
}

/* The overlay's bucket chains: enumeration while a bucket is overlayed mid-walk,
 * an add into a bucket the dict has no entry for, a scan that spans a
 * write-back, and the write-back itself. No scan thread: the guard is
 * installed (frozen) and every write defers (no 'prepare' hook), so each
 * write overlays its bucket. */
static void hybsnapOverlayBucketTests(void) {
    dictType noval = KvstoreDictTestType;
    noval.no_value = 1;
    noval.keyCompare = hybsnapTestKeyCompare;
    kvstore *stores[1];
    stores[0] = kvstoreCreate(&KvstoreTestType, &noval, HYBSNAP_TEST_DICT_BITS, KVSTORE_ALLOCATE_DICTS_ON_DEMAND);
    kvstore *ks = stores[0];
    const int N = 300;
    for (int i = 0; i < N; i++) serverAssert(kvstoreDictAddRaw(ks, 0, hybsnapTestKey(0, 0, i), NULL) != NULL);
    dict *d = kvstoreGetDict(ks, 0);
    while (dictIsRehashing(d)) dictRehash(d, 1000);
    struct hybsnapScanTestState *st = zcalloc(sizeof(*st));
    st->inserted[0][0] = N;
    st->stores = stores;
    struct ovTestCounters cnt = {0, 0};
    void *privs[1] = { &cnt };
    kvstoreSnapHooks hooks = { .clone = ovTestClone, .free_orig = ovTestFree, .enter = hybsnapOverlayEnter };
    hybsnapScanCtx *ctx = hybsnapScanCreate(stores, 1, hybsnapTestDispatch, hybsnapTestSlotDone, st);
    st->ctx = ctx;
    hybsnapScanSetOverlay(ctx, &hooks, privs);
    hybsnapScanFreeze(ctx);

    /* A bucket of at least three entries (300 keys in a table of 512). */
    unsigned long size0 = DICTHT_SIZE(d->ht_size_exp[0]), b = size0;
    for (unsigned long i = 0; i < size0 && b == size0; i++) {
        int n = 0;
        for (dictEntry *e = d->ht_table[0][i]; e; e = dictGetNext(e)) n++;
        if (n >= 3) b = i;
    }
    test_cond("overlay buckets: the table has a bucket of three entries", b < size0);
    serverAssert(b < size0);
    char k1name[32];
    int idx0 = ovTestKeyIndex(dictGetKey(d->ht_table[0][b]));
    int idx1 = ovTestKeyIndex(dictGetKey(dictGetNext(d->ht_table[0][b])));
    snprintf(k1name, sizeof(k1name), "k:0:0:%d", idx1);

    /* ---- 1. the bucket is overlayed after its first entry was handed out ---- */
    int seen[300] = {0};
    kvstoreDictIterator it;
    kvstoreInitDictIterator(&it, ks, 0);
    dictEntry *de;
    long returned = 0;
    int overlayed = 0;
    while ((de = kvstoreDictIteratorNext(&it)) != NULL) {
        int idx = ovTestKeyIndex(dictGetKey(de));
        serverAssert(idx >= 0 && idx < N);
        seen[idx]++;
        returned++;
        if (idx == idx0 && !overlayed) {
            overlayed = 1;
            serverAssert(kvstoreDictDelete(ks, 0, k1name) == DICT_OK);   /* overlays the bucket mid-walk */
        }
    }
    kvstoreResetDictIterator(&it);
    int once = 1;
    for (int i = 0; i < N; i++) if (i != idx1 && seen[i] != 1) once = 0;
    test_cond("overlay buckets: a bucket overlayed mid-walk loses nothing and repeats nothing",
              once && seen[idx1] == 0 && returned == N - 1);
    test_cond("overlay buckets: the delete shows in the logical size and not in the dict",
              kvstoreDictFind(ks, 0, k1name) == NULL && kvstoreDictSize(ks, 0) == (unsigned long)N - 1 &&
              dictSize(d) == (unsigned long)N && dictFind(d, k1name) != NULL);

    /* ---- 2. an add into a bucket the dict has no entry for ---- */
    unsigned long empty_b = size0;
    for (unsigned long i = 0; i < size0 && empty_b == size0; i++) if (!d->ht_table[0][i]) empty_b = i;
    char *newkey = NULL;
    for (int t = 0; empty_b < size0 && !newkey && t < 1000000; t++) {   /* a key that hashes into it */
        char buf[32];
        snprintf(buf, sizeof(buf), "k:0:0:%d", N + 1 + t);
        if ((dictGetHash(d, buf) & DICTHT_SIZE_MASK(d->ht_size_exp[0])) == empty_b) newkey = zstrdup(buf);
    }
    test_cond("overlay buckets: found a key for an empty bucket", newkey != NULL);
    serverAssert(newkey != NULL);
    serverAssert(kvstoreDictAddRaw(ks, 0, newkey, NULL) != NULL);
    test_cond("overlay buckets: the add is in the overlay, not in the dict",
              kvstoreDictFind(ks, 0, newkey) != NULL && dictFind(d, newkey) == NULL && kvstoreDictSize(ks, 0) == (unsigned long)N);
    int new_seen = 0;
    long n = 0;
    kvstoreIterator ki;
    kvstoreIteratorInit(&ki, ks);
    while ((de = kvstoreIteratorNext(&ki)) != NULL) { n++; if (!strcmp(dictGetKey(de), newkey)) new_seen++; }
    kvstoreIteratorReset(&ki);
    test_cond("overlay buckets: the iterator hands out the add once", new_seen == 1 && n == N);

    /* ---- 3. a scan that spans a write-back loses no key that was there throughout ---- */
    for (int i = 0; i < N; i += 11) {   /* overlay more buckets */
        char *k = hybsnapTestKey(0, 0, i);
        dictEntryLink bucket = NULL;
        kvstoreDictFindLink(ks, 0, k, &bucket);
        zfree(k);
    }
    memset(seen, 0, sizeof(seen));
    unsigned long long cursor = 0;
    int steps = 0;
    do {
        cursor = kvstoreScan(ks, cursor, 0, ovTestScanSeen, NULL, seen);
        if (++steps == 3) kvstoreOverlayDrain(UINT64_MAX);   /* the buckets go back to the dict mid-scan */
    } while (cursor);
    int all = 1;
    for (int i = 0; i < N; i++) if (i != idx1 && seen[i] == 0) all = 0;
    test_cond("overlay buckets: a scan spanning the write-back returns every key that was there throughout", all && seen[idx1] == 0);
    test_cond("overlay buckets: the write-back moved the chains into the dict",
              kvstoreOverlayPending() == 0 && dictSize(d) == (unsigned long)N && kvstoreDictSize(ks, 0) == (unsigned long)N &&
              dictFind(d, newkey) != NULL && dictFind(d, k1name) == NULL && cnt.frees == cnt.clones);

    hybsnapSectionClose();
    hybsnapScanAbort(ctx);
    hybsnapScanRelease(ctx);
    test_cond("overlay buckets: after the release the dict holds the logical keys and every original was freed",
              !kvstoreOverlayIsFrozen(ks, 0) && kvstoreDictSize(ks, 0) == (unsigned long)N && cnt.frees == cnt.clones);
    kvstoreRelease(ks);
    zfree(st);
}

/* The window the test 'prepare' hook defers every write in: half a table
 * (the half the bucket is in), its last position ov_test_seq. */
static uint64_t ov_test_seq;
static kvstoreOverlayPrepareResult ovTestPrepareHalves(void *priv, int didx, dict *d, const void *key, kvstoreOvWindow *w) {
    UNUSED(priv); UNUSED(didx);
    int t;
    unsigned long i;
    dictKeyBucket(d, key, &t, &i);
    unsigned long half = DICTHT_SIZE(d->ht_size_exp[t]) / 2;
    w->last = ov_test_seq;
    w->table = t;
    w->lo = i < half ? 0 : half;
    w->n = half;
    return KVSTORE_OV_DEFER;
}

/* A key of dict didx (k:0:didx:i, i < n) in a table-0 bucket of the lower or
 * upper half not used yet; its name in 'name'. */
static int ovTestKeyInHalf(dict *d, int didx, int n, int upper, char *used, char *name, size_t len) {
    unsigned long size = DICTHT_SIZE(d->ht_size_exp[0]);
    for (int i = 0; i < n; i++) {
        snprintf(name, len, "k:0:%d:%d", didx, i);
        unsigned long b = dictGetHash(d, name) & DICTHT_SIZE_MASK(d->ht_size_exp[0]);
        if ((b >= size / 2) == upper && !used[b]) {
            used[b] = 1;
            return 1;
        }
    }
    return 0;
}

/* The index of overlayed buckets: one slot per (dict, window), a drain writes
 * back exactly the windows whose last position is below the one it is given,
 * a dict that left its store is drained like the others, and the index grows
 * past its first four slots.
 * No scan thread: a test 'prepare' hook defers every write in a window it
 * chooses. */
static void hybsnapOverlayWindowTests(void) {
    dictType noval = KvstoreDictTestType;
    noval.no_value = 1;
    noval.keyCompare = hybsnapTestKeyCompare;
    kvstore *stores[1];
    stores[0] = kvstoreCreate(&KvstoreTestType, &noval, HYBSNAP_TEST_DICT_BITS, KVSTORE_ALLOCATE_DICTS_ON_DEMAND);
    kvstore *ks = stores[0];
    const int N = 200;
    for (int didx = 0; didx < HYBSNAP_TEST_DICTS; didx++) {
        for (int i = 0; i < N; i++) serverAssert(kvstoreDictAddRaw(ks, didx, hybsnapTestKey(0, didx, i), NULL) != NULL);
        dict *d = kvstoreGetDict(ks, didx);
        while (dictIsRehashing(d)) dictRehash(d, 1000);
    }
    dict *d0 = kvstoreGetDict(ks, 0), *d1 = kvstoreGetDict(ks, 1), *d2 = kvstoreGetDict(ks, 2);
    struct hybsnapScanTestState *st = zcalloc(sizeof(*st));
    for (int didx = 0; didx < HYBSNAP_TEST_DICTS; didx++) st->inserted[0][didx] = N;
    st->stores = stores;
    struct ovTestCounters cnt = {0, 0};
    void *privs[1] = { &cnt };
    kvstoreSnapHooks hooks = { .clone = ovTestClone, .free_orig = ovTestFree, .enter = hybsnapOverlayEnter,
                               .prepare = ovTestPrepareHalves };
    hybsnapScanCtx *ctx = hybsnapScanCreate(stores, 1, hybsnapTestDispatch, hybsnapTestSlotDone, st);
    st->ctx = ctx;
    hybsnapScanSetOverlay(ctx, &hooks, privs);
    hybsnapScanFreeze(ctx);
    char *used[HYBSNAP_TEST_DICTS];
    for (int didx = 0; didx < HYBSNAP_TEST_DICTS; didx++)
        used[didx] = zcalloc(DICTHT_SIZE(kvstoreGetDict(ks, didx)->ht_size_exp[0]));
    char lo0[32], hi0[32], k1[32], lo2[32], hi2[32];
    serverAssert(ovTestKeyInHalf(d0, 0, N, 0, used[0], lo0, sizeof(lo0)) && ovTestKeyInHalf(d0, 0, N, 1, used[0], hi0, sizeof(hi0)) &&
                 ovTestKeyInHalf(d1, 1, N, 0, used[1], k1, sizeof(k1)));

    /* Window 10 over both halves of dict 0, window 11 over dict 1: three slots. */
    ov_test_seq = 10;
    serverAssert(kvstoreDictDelete(ks, 0, lo0) == DICT_OK && kvstoreDictDelete(ks, 0, hi0) == DICT_OK);
    ov_test_seq = 11;
    serverAssert(kvstoreDictDelete(ks, 1, k1) == DICT_OK);
    test_cond("overlay window: three buckets in three windows, the deletes logical only",
              kvstoreOverlayPending() == 3 && kvstoreDictFind(ks, 0, lo0) == NULL && kvstoreDictFind(ks, 0, hi0) == NULL &&
              kvstoreDictFind(ks, 1, k1) == NULL && dictFind(d0, lo0) && dictFind(d0, hi0) && dictFind(d1, k1) &&
              kvstoreDictSize(ks, 0) == (unsigned long)N - 2 && kvstoreDictSize(ks, 1) == (unsigned long)N - 1);
    test_cond("overlay window: a drain at the current window writes back nothing", kvstoreOverlayDrain(10) == 3);
    test_cond("overlay window: a drain past window 10 writes back dict 0's buckets only",
              kvstoreOverlayDrain(11) == 1 && !dictFind(d0, lo0) && !dictFind(d0, hi0) && dictSize(d0) == (unsigned long)N - 2 &&
              dictFind(d1, k1) != NULL && kvstoreDictFind(ks, 1, k1) == NULL);

    /* Dict 1 leaves its store (the slot-trim move) with its bucket still overlayed: drained all the same. */
    kvstore *tmp = kvstoreCreate(&KvstoreTestType, &noval, HYBSNAP_TEST_DICT_BITS, KVSTORE_ALLOCATE_DICTS_ON_DEMAND);
    kvstoreMoveDict(ks, tmp, 1);
    test_cond("overlay window: the moved dict keeps its overlayed bucket", kvstoreDictDetached(d1) && kvstoreOverlayPending() == 1);
    test_cond("overlay window: a drain past window 11 writes back the moved dict's bucket",
              kvstoreOverlayDrain(12) == 0 && !dictFind(d1, k1) && dictSize(d1) == (unsigned long)N - 1 && cnt.frees == cnt.clones);
    kvstoreRelease(tmp);

    /* Four windows at once: both halves of dict 0 (window 20) and of dict 2 (window 21). */
    serverAssert(ovTestKeyInHalf(d0, 0, N, 0, used[0], lo0, sizeof(lo0)) && ovTestKeyInHalf(d0, 0, N, 1, used[0], hi0, sizeof(hi0)) &&
                 ovTestKeyInHalf(d2, 2, N, 0, used[2], lo2, sizeof(lo2)) && ovTestKeyInHalf(d2, 2, N, 1, used[2], hi2, sizeof(hi2)));
    ov_test_seq = 20;
    serverAssert(kvstoreDictDelete(ks, 0, lo0) == DICT_OK && kvstoreDictDelete(ks, 0, hi0) == DICT_OK);
    ov_test_seq = 21;
    serverAssert(kvstoreDictDelete(ks, 2, lo2) == DICT_OK && kvstoreDictDelete(ks, 2, hi2) == DICT_OK);
    /* A second write into an overlayed bucket goes to the same bucket, and an add lands in it too. */
    dictEntryLink bucket = NULL;
    kvstoreDictFindLink(ks, 0, lo0, &bucket);
    kvstoreDictSetAtLink(ks, 0, zstrdup(lo0), &bucket, 1);
    test_cond("overlay window: four windows hold buckets at once; a re-add lands in the overlayed bucket",
              kvstoreOverlayPending() == 4 && kvstoreDictFind(ks, 0, lo0) != NULL && kvstoreDictSize(ks, 0) == (unsigned long)N - 3);
    long scanned = 0;
    unsigned long long cursor = 0;
    do { cursor = kvstoreScan(ks, cursor, -1, ovTestScanCb, NULL, &scanned); } while (cursor);
    int has_new = 0;
    long iterated = ovTestCountIter(ks, &has_new, NULL);
    unsigned long long logical = kvstoreSize(ks);
    test_cond("overlay window: scan and iteration see the logical keyspace across the windows",
              (unsigned long long)scanned == logical && (unsigned long long)iterated == logical && logical == 3ULL * N - 5);
    test_cond("overlay window: windows 20 and 21 drain in turn",
              kvstoreOverlaySlotsInUse() == 4 &&
              kvstoreOverlayDrain(21) == 2 && dictFind(d0, lo0) != NULL && dictFind(d0, hi0) == NULL && dictFind(d2, lo2) != NULL &&
              kvstoreOverlayDrain(22) == 0 && dictFind(d2, lo2) == NULL && dictFind(d2, hi2) == NULL && cnt.frees == cnt.clones &&
              kvstoreOverlaySlotsInUse() == 0);

    /* Ten windows at once (one bucket each in dict 3): the index grows, every
     * bucket stays reachable, and the drain takes them in order. */
    char k3[10][32];
    dict *d3 = kvstoreGetDict(ks, 3);
    for (int w = 0; w < 10; w++) {
        serverAssert(ovTestKeyInHalf(d3, 3, N, w & 1, used[3], k3[w], sizeof(k3[w])));
        ov_test_seq = 30 + w;
        serverAssert(kvstoreDictDelete(ks, 3, k3[w]) == DICT_OK);
    }
    int gone_all = 1;
    for (int w = 0; w < 10; w++) if (kvstoreDictFind(ks, 3, k3[w]) != NULL || dictFind(d3, k3[w]) == NULL) gone_all = 0;
    test_cond("overlay window: ten windows hold buckets, the index grew, each delete is visible through it",
              kvstoreOverlaySlotsInUse() == 10 && kvstoreOverlayPending() == 10 && gone_all &&
              kvstoreDictSize(ks, 3) == (unsigned long)N - 10);
    test_cond("overlay window: the grown index drains window by window",
              kvstoreOverlayDrain(35) == 5 && dictFind(d3, k3[4]) == NULL && dictFind(d3, k3[5]) != NULL &&
              kvstoreOverlayDrain(40) == 0 && kvstoreOverlaySlotsInUse() == 0 && dictSize(d3) == (unsigned long)N - 10 &&
              cnt.frees == cnt.clones);

    hybsnapSectionClose();
    hybsnapScanAbort(ctx);
    hybsnapScanRelease(ctx);
    test_cond("overlay window: after the release nothing is overlayed and nothing is frozen",
              kvstoreOverlayPending() == 0 && !kvstoreOverlayIsFrozen(ks, 0) && !kvstoreOverlayIsFrozen(ks, 2) &&
              kvstoreSize(ks) == 3ULL * N - 15);
    for (int didx = 0; didx < HYBSNAP_TEST_DICTS; didx++) zfree(used[didx]);
    kvstoreRelease(ks);
    zfree(st);
}

/* The overlay over a dict caught mid-rehash (entries in both tables): writes
 * overlay buckets of either table and pause the rehash, enumeration and scan
 * return every key that was there throughout, and the rehash resumes once the
 * overlay is gone. */
static void hybsnapOverlayRehashTests(void) {
    dictType noval = KvstoreDictTestType;
    noval.no_value = 1;
    noval.keyCompare = hybsnapTestKeyCompare;
    kvstore *stores[1];
    stores[0] = kvstoreCreate(&KvstoreTestType, &noval, HYBSNAP_TEST_DICT_BITS, KVSTORE_ALLOCATE_DICTS_ON_DEMAND);
    kvstore *ks = stores[0];
    dict *d = NULL;
    int N = 0;
    /* Stop right after an expansion of a table of at least 64 buckets starts: the
     * rehash then needs about that many more steps, none of which run below. */
    while (N < 250) {
        serverAssert(kvstoreDictAddRaw(ks, 0, hybsnapTestKey(0, 0, N), NULL) != NULL);
        N++;
        d = kvstoreGetDict(ks, 0);
        if (dictIsRehashing(d) && DICTHT_SIZE(d->ht_size_exp[0]) >= 64) break;
    }
    int two_tables = dictIsRehashing(d) && d->ht_used[0] > 0 && d->ht_used[1] > 0;
    test_cond("overlay rehash: the dict is mid-rehash with entries in both tables", two_tables);
    serverAssert(two_tables);
    struct hybsnapScanTestState *st = zcalloc(sizeof(*st));
    st->inserted[0][0] = N;
    st->stores = stores;
    struct ovTestCounters cnt = {0, 0};
    void *privs[1] = { &cnt };
    kvstoreSnapHooks hooks = { .clone = ovTestClone, .free_orig = ovTestFree, .enter = hybsnapOverlayEnter };
    hybsnapScanCtx *ctx = hybsnapScanCreate(stores, 1, hybsnapTestDispatch, hybsnapTestSlotDone, st);
    st->ctx = ctx;
    hybsnapScanSetOverlay(ctx, &hooks, privs);
    hybsnapScanFreeze(ctx);

    /* Delete every 7th key, add 20 new ones: buckets of both tables overlay. */
    int gone[300] = {0}, added = 0;
    for (int i = 0; i < N; i += 7) {
        char *k = hybsnapTestKey(0, 0, i);
        serverAssert(kvstoreDictDelete(ks, 0, k) == DICT_OK);
        gone[i] = 1;
        zfree(k);
    }
    for (int j = 0; j < 20; j++) {
        serverAssert(kvstoreDictAddRaw(ks, 0, hybsnapTestKey(0, 0, N + j), NULL) != NULL);
        added++;
    }
    int ndel = 0;
    for (int i = 0; i < N; i++) ndel += gone[i];
    test_cond("overlay rehash: writes overlayed buckets and the rehash stayed paused",
              kvstoreOverlayPending() > 0 && dictIsRehashing(d) &&
              kvstoreDictSize(ks, 0) == (unsigned long)(N - ndel + added));

    /* Enumeration: each logical key once. */
    int seen[300] = {0};
    kvstoreDictIterator it;
    kvstoreInitDictIterator(&it, ks, 0);
    dictEntry *de;
    long returned = 0;
    while ((de = kvstoreDictIteratorNext(&it)) != NULL) {
        int idx = ovTestKeyIndex(dictGetKey(de));
        serverAssert(idx >= 0 && idx < 300);
        seen[idx]++;
        returned++;
    }
    kvstoreResetDictIterator(&it);
    int exact = 1;
    for (int i = 0; i < N + added; i++) {
        int want = (i < N) ? !gone[i] : 1;
        if (seen[i] != want) exact = 0;
    }
    test_cond("overlay rehash: the iterator returns each logical key exactly once",
              exact && returned == N - ndel + added);

    /* A scan that spans the write-back returns every key that was there throughout. */
    memset(seen, 0, sizeof(seen));
    unsigned long long cursor = 0;
    int steps = 0;
    do {
        cursor = kvstoreScan(ks, cursor, 0, ovTestScanSeen, NULL, seen);
        if (++steps == 3) kvstoreOverlayDrain(UINT64_MAX);
    } while (cursor);
    int all = 1;
    for (int i = 0; i < N; i++) if (!gone[i] && seen[i] == 0) all = 0;
    test_cond("overlay rehash: a scan spanning the write-back returns every key that was there throughout", all);
    test_cond("overlay rehash: the write-back left the dict with exactly the logical keys",
              kvstoreOverlayPending() == 0 && dictSize(d) == (unsigned long)(N - ndel + added) && cnt.frees == cnt.clones);

    hybsnapSectionClose();
    hybsnapScanAbort(ctx);
    hybsnapScanRelease(ctx);
    d = kvstoreGetDict(ks, 0);
    int finished = 1;
    while (dictIsRehashing(d)) { dictRehash(d, 1000); }
    for (int i = 0; i < N + added; i++) {
        char *k = hybsnapTestKey(0, 0, i);
        int present = dictFind(d, k) != NULL;
        int want = (i < N) ? !gone[i] : 1;
        if (present != want) finished = 0;
        zfree(k);
    }
    test_cond("overlay rehash: the rehash completes after the release and the dict holds the logical keys", finished);
    kvstoreRelease(ks);
    zfree(st);
}

/* ---- Save epochs: the kvobj layout, and the scan skipping stamped keys ---- */

/* Is the epoch where the layout says, within the allocation, the key and the
 * embedded value intact, and the accounted size consistent? */
static int hybsnapTestEpochLayoutOk(kvobj *kv, const char *key, const char *val, uint32_t epoch) {
    char *alloc = kvobjGetAllocPtr(kv);
    size_t usable = zmalloc_size(alloc);
    if (strcmp(kvobjGetKey(kv), key) != 0) return 0;
    if (val && (kv->type != OBJ_STRING || strcmp(kv->ptr, val) != 0)) return 0;
    if (kvobjGetEpoch(kv) != (kvobjBits(kv)->has_epoch ? epoch : 0)) return 0;
    if (kvobjBits(kv)->has_epoch) {
        char *end;
        if (kv->encoding == OBJ_ENCODING_EMBSTR) end = (char *)kv->ptr + sdsalloc(kv->ptr) + 1 + sizeof(uint32_t);
        else end = kvobjGetKey(kv) + sdslen(kvobjGetKey(kv)) + 1 + sizeof(uint32_t);
        if (end > alloc + usable) return 0;
    }
    /* kvobjAllocSize adds the value's own allocation for a raw string: compare the header part only. */
    size_t hdr = kvobjAllocSize(kv) - (kv->encoding == OBJ_ENCODING_RAW ? sdsAllocSize(kv->ptr) : 0);
    return hdr <= usable;
}

static void hybsnapEpochLayoutTests(void) {
    int saved_mode = hybsnap_epoch_mode;
    uint32_t saved_stamp = hybsnap_epoch_stamp;
    hybsnap_epoch_stamp = 77;
    char longkey[201];
    memset(longkey, 'L', 200);
    longkey[200] = '\0';
    const char *keys[2] = { "k", longkey };
    int modes[3] = { HYBSNAP_EPOCH_NONE, HYBSNAP_EPOCH_ALWAYS, HYBSNAP_EPOCH_IF_FREE };
    int ok_layout = 1, ok_mode = 1, ok_add = 1, saw_embstr_epoch = 0, saw_raw_epoch = 0;
    int add_in_place = 0, add_moved = 0;
    char val[41];
    for (int m = 0; m < 3; m++) {
        hybsnap_epoch_mode = modes[m];
        for (int k = 0; k < 2; k++) {
            for (int vlen = 1; vlen <= 40; vlen++) {
                memset(val, 'a' + (vlen % 26), vlen);
                val[vlen] = '\0';
                sds key = sdsnew(keys[k]);
                /* EMBSTR when the key and value fit a cache line, else RAW. */
                kvobj *kv = kvobjSet(key, createStringObject(val, vlen), 0);
                int has = kvobjBits(kv)->has_epoch;
                if (modes[m] == HYBSNAP_EPOCH_NONE && has) ok_mode = 0;
                if (modes[m] == HYBSNAP_EPOCH_ALWAYS && !has) ok_mode = 0;
                if (has && kv->encoding == OBJ_ENCODING_EMBSTR) saw_embstr_epoch = 1;
                if (has && kv->encoding == OBJ_ENCODING_RAW) saw_raw_epoch = 1;
                if (!hybsnapTestEpochLayoutOk(kv, keys[k], val, 77)) ok_layout = 0;
                /* Without an epoch: add one, in place or into a new allocation. */
                if (!has) {
                    kvobj *nkv = kvobjAddEpoch(kv, 99);
                    if (nkv == kv) {
                        add_in_place++;
                    } else {
                        add_moved++;
                        zfree(kvobjGetAllocPtr(kv));   /* the old allocation alone: the value is the new one's */
                    }
                    kv = nkv;
                    if (!kvobjBits(kv)->has_epoch || !hybsnapTestEpochLayoutOk(kv, keys[k], val, 99)) ok_add = 0;
                }
                kvobjSetEpoch(kv, 123);
                if (kvobjGetEpoch(kv) != 123 || strcmp(kv->ptr, val) != 0) ok_add = 0;
                decrRefCount(kv);
                sdsfree(key);
            }
        }
    }
    test_cond("epoch: NONE creates no epoch, ALWAYS always one, for EMBSTR and RAW, short and long keys", ok_mode && saw_embstr_epoch && saw_raw_epoch);
    test_cond("epoch: the epoch, the key and the embedded value fit the allocation; the accounted size too", ok_layout);
    test_cond("epoch: kvobjAddEpoch works in place and into a new allocation, keeping key and value", ok_add && add_in_place > 0 && add_moved > 0);
    hybsnap_epoch_mode = saved_mode;
    hybsnap_epoch_stamp = saved_stamp;
}

#define HYBSNAP_TEST_EPOCH_KEYS 300
struct hybsnapEpochTestState { int seen[HYBSNAP_TEST_EPOCH_KEYS + 1]; redisAtomic int bad, dispatched; };

static void hybsnapTestEpochDispatch(void *priv, int s, int didx, kvobj *kv) {
    UNUSED(s);
    UNUSED(didx);
    struct hybsnapEpochTestState *et = priv;
    int i;
    if (sscanf(kvobjGetKey(kv), "e:%d", &i) != 1 || i < 0 || i > HYBSNAP_TEST_EPOCH_KEYS || et->seen[i]) atomicIncr(et->bad, 1);
    else et->seen[i] = 1;
    atomicIncr(et->dispatched, 1);
}

/* The scan over kvobj keys: those with no epoch or an older one are
 * dispatched, those stamped with the save's epoch (emitted ahead, in place or
 * moved to a new allocation) and those created after the freeze are not. */
static void hybsnapEpochScanTests(void) {
    int saved_mode = hybsnap_epoch_mode;
    dictType kvt = dbDictType;
    kvt.keyDestructor = NULL;   /* the keys are freed below */
    kvt.resizeAllowed = NULL;
    kvt.prefetchEntryKey = NULL;
    kvt.prefetchEntryValue = NULL;
    kvstore *ks[1] = { kvstoreCreate(&KvstoreTestType, &kvt, 0, KVSTORE_ALLOCATE_DICTS_ON_DEMAND) };
    kvobj *kv[HYBSNAP_TEST_EPOCH_KEYS + 1];
    hybsnap_epoch_mode = HYBSNAP_EPOCH_ALWAYS;   /* the first half: an earlier save's epoch */
    for (int i = 0; i < HYBSNAP_TEST_EPOCH_KEYS; i++) {
        if (i == HYBSNAP_TEST_EPOCH_KEYS / 2) hybsnap_epoch_mode = HYBSNAP_EPOCH_NONE;   /* the rest: none */
        sds key = sdscatfmt(sdsempty(), "e:%i", i);
        kv[i] = kvobjCreate(OBJ_STRING, key, sdsnew("v"), 0);
        sdsfree(key);
        serverAssert(kvstoreDictAddRaw(ks[0], 0, kv[i], NULL) != NULL);
    }
    struct hybsnapEpochTestState *et = zcalloc(sizeof(*et));
    hybsnapScanCtx *ctx = hybsnapScanCreate(ks, 1, hybsnapTestEpochDispatch, NULL, et);
    ctx->skip_stamped = 1;
    hybsnapScanFreeze(ctx);
    int fresh_stamped = hybsnap_epoch_mode == HYBSNAP_EPOCH_ALWAYS;
    /* Every third key is emitted ahead and stamped, as the prepare hook does. */
    dict *d = kvstoreGetDict(ks[0], 0);
    int stamped = 0, moved = 0;
    for (int i = 0; i < HYBSNAP_TEST_EPOCH_KEYS; i += 3) {
        dictEntryLink link = dictFindLink(d, kvobjGetKey(kv[i]), NULL);
        serverAssert(link && dictGetKV(*link) == kv[i]);
        kvobj *n = kvobjAddEpoch(kv[i], ctx->epoch);
        if (n != kv[i]) {
            dictSetKeyAtLink(d, n, &link, 0);
            zfree(kvobjGetAllocPtr(kv[i]));
            moved++;
        }
        kv[i] = n;
        stamped++;
    }
    /* A key born after the fork instant. */
    sds key = sdscatfmt(sdsempty(), "e:%i", HYBSNAP_TEST_EPOCH_KEYS);
    kv[HYBSNAP_TEST_EPOCH_KEYS] = kvobjCreate(OBJ_STRING, key, sdsnew("v"), 0);
    sdsfree(key);
    serverAssert(dictAdd(d, kv[HYBSNAP_TEST_EPOCH_KEYS], NULL) == DICT_OK);
    test_cond("epoch: while a scan holds an epoch every new kvobj gets it", fresh_stamped &&
              kvobjGetEpoch(kv[HYBSNAP_TEST_EPOCH_KEYS]) == ctx->epoch);
    serverAssert(hybsnapScanStartThread(ctx) == C_OK);
    while (!hybsnapScanIsFinished(ctx)) usleep(100);
    hybsnapScanHandleCompletions(ctx);
    int ok = 1;
    for (int i = 0; i <= HYBSNAP_TEST_EPOCH_KEYS; i++) {
        int expect = i < HYBSNAP_TEST_EPOCH_KEYS && i % 3 != 0;
        if (et->seen[i] != expect) ok = 0;
    }
    int bad, dispatched;
    long long skipped;
    atomicGet(et->bad, bad);
    atomicGet(et->dispatched, dispatched);
    atomicGet(ctx->keys_stamped, skipped);
    test_cond("epoch: the thread dispatched the unstamped keys once (older epochs and none), skipped the stamped and the new",
              ok && bad == 0 && dispatched == HYBSNAP_TEST_EPOCH_KEYS - stamped && skipped == stamped + 1 && moved > 0);
    hybsnapScanRelease(ctx);
    test_cond("epoch: released with the scan (new kvobjs per the mode again)", hybsnap_epoch_mode != HYBSNAP_EPOCH_ALWAYS ||
              saved_mode == HYBSNAP_EPOCH_ALWAYS || server.hybrid_snapshot_epoch_prealloc);
    kvstoreRelease(ks[0]);
    for (int i = 0; i <= HYBSNAP_TEST_EPOCH_KEYS; i++) decrRefCount(kv[i]);
    zfree(et);
    hybsnap_epoch_mode = saved_mode;
}

static uint64_t hybsnapTestOneBucketHash(const void *key) {
    UNUSED(key);
    return 0;
}

/* Emit-ahead is per key: several keys share one bucket ahead of the thread
 * (every key hashes to bucket 0, as in a swiss-table bucket), a write to one
 * of them makes the prepare hook emit and stamp only that key; a second write
 * to it and an insert into the bucket emit nothing; the thread then emits the
 * others, once each, and skips the stamped one. */
#define HYBSNAP_TEST_BUCKET_KEYS 6
static void hybsnapEpochEmitAheadTests(void) {
    int saved_mode = hybsnap_epoch_mode, saved_dbnum = server.dbnum;
    server.dbnum = 0;   /* no db owns the test store: hybsnapStoreOwnerDb() finds none */
    dictType kvt = dbDictType;
    kvt.hashFunction = hybsnapTestOneBucketHash;
    kvt.keyDestructor = NULL;   /* the keys are freed below */
    kvt.resizeAllowed = NULL;
    kvt.prefetchEntryKey = NULL;
    kvt.prefetchEntryValue = NULL;
    kvstore *ks[1] = { kvstoreCreate(&KvstoreTestType, &kvt, 0, KVSTORE_ALLOCATE_DICTS_ON_DEMAND) };
    hybsnap_epoch_mode = HYBSNAP_EPOCH_NONE;   /* fork-instant keys without an epoch */
    for (int i = 0; i < HYBSNAP_TEST_BUCKET_KEYS; i++) {
        sds key = sdscatfmt(sdsempty(), "e:%i", i);
        kvobj *kv = kvobjCreate(OBJ_STRING, key, sdsnew("v"), 0);
        sdsfree(key);
        serverAssert(kvstoreDictAddRaw(ks[0], 0, kv, NULL) != NULL);
    }
    dict *d = kvstoreGetDict(ks[0], 0);
    while (dictIsRehashing(d)) dictRehash(d, 100);
    sds k[HYBSNAP_TEST_BUCKET_KEYS + 1];   /* lookup keys: the dict compares sds */
    for (int i = 0; i < HYBSNAP_TEST_BUCKET_KEYS; i++) k[i] = sdscatfmt(sdsempty(), "e:%i", i);
    k[HYBSNAP_TEST_BUCKET_KEYS] = sdsnew("e:new");
    int one_bucket = 1;
    for (int i = 0; i < HYBSNAP_TEST_BUCKET_KEYS; i++) {
        int table;
        unsigned long idx;
        dictKeyBucket(d, k[i], &table, &idx);
        if (table != 0 || idx != 0) one_bucket = 0;
    }
    struct hybsnapEpochTestState *et = zcalloc(sizeof(*et));
    hybsnapScanCtx *ctx = hybsnapScanCreate(ks, 1, hybsnapTestEpochDispatch, NULL, et);
    ctx->skip_stamped = 1;
    hybsnapScanFreeze(ctx);
    /* The save's state the prepare hook touches: the main-thread stage. */
    hybsnapSave *sv = zcalloc(sizeof(*sv));
    sv->scan = ctx;
    sv->stores = ks;
    sv->dbnum = 1;
    pthread_mutex_init(&sv->mq_lock, NULL);
    sv->mq = listCreate();
    sv->mstage = hybsnapMsgNew(0);
    sv->mstage_dbid = sv->mstage_slot = -1;
    sv->mscratch = sdsempty();
    hybsnap_save = sv;
    struct hybsnapGuardPriv gp = { ctx, 0, NULL };
    kvstoreOvWindow win;
    const int w = 2;   /* the written key */
    kvstoreOverlayPrepareResult r1 = hybsnapOverlayPrepare(&gp, 0, d, k[w], &win);
    long long emitted;
    atomicGet(sv->main_keys, emitted);
    /* One record: in the stage (a small value) or a header copy message (a fork-class value). */
    size_t staged = sdslen(sv->mstage);
    unsigned long msgs = listLength(sv->mq);
    int one_record = (staged > sizeof(hybsnapMsgHdr)) + (int)msgs == 1;
    int stamped_only_w = 1;
    for (int i = 0; i < HYBSNAP_TEST_BUCKET_KEYS; i++) {
        dictEntryLink link = dictFindLink(d, k[i], NULL);
        serverAssert(link);
        int stamped = kvobjGetEpoch(dictGetKV(*link)) >= ctx->epoch;
        if (stamped != (i == w)) stamped_only_w = 0;
    }
    test_cond("emit-ahead: the keys share one bucket, ahead of the thread", one_bucket &&
              hybsnapBucketPosition(ctx, 0, 0, d, 0, 0, NULL) == HYBSNAP_POS_AHEAD);
    test_cond("emit-ahead: a write to one key of a multi-key bucket emits that key alone and stamps it",
              r1 == KVSTORE_OV_INPLACE && emitted == 1 && one_record && stamped_only_w);
    long long inplace0 = sv->inplace_writes;
    kvstoreOverlayPrepareResult r2 = hybsnapOverlayPrepare(&gp, 0, d, k[w], &win);
    kvstoreOverlayPrepareResult r3 = hybsnapOverlayPrepare(&gp, 0, d, k[HYBSNAP_TEST_BUCKET_KEYS], &win);   /* an insert */
    atomicGet(sv->main_keys, emitted);
    test_cond("emit-ahead: a second write to the stamped key and an insert into the bucket emit nothing",
              r2 == KVSTORE_OV_INPLACE && r3 == KVSTORE_OV_INPLACE && emitted == 1 &&
              sdslen(sv->mstage) == staged && listLength(sv->mq) == msgs && sv->inplace_writes == inplace0 + 2);
    hybsnapRetiredFree();   /* as at the end of the section */
    serverAssert(hybsnapScanStartThread(ctx) == C_OK);
    while (!hybsnapScanIsFinished(ctx)) usleep(100);
    hybsnapScanHandleCompletions(ctx);
    int ok = 1, bad, dispatched;
    for (int i = 0; i < HYBSNAP_TEST_BUCKET_KEYS; i++)
        if (et->seen[i] != (i != w)) ok = 0;
    long long skipped;
    atomicGet(et->bad, bad);
    atomicGet(et->dispatched, dispatched);
    atomicGet(ctx->keys_stamped, skipped);
    test_cond("emit-ahead: the thread emits the bucket's other keys once each and skips the stamped one",
              ok && bad == 0 && dispatched == HYBSNAP_TEST_BUCKET_KEYS - 1 && skipped == 1);
    hybsnap_save = NULL;
    hybsnapScanRelease(ctx);
    kvobj *kv[HYBSNAP_TEST_BUCKET_KEYS];
    for (int i = 0; i < HYBSNAP_TEST_BUCKET_KEYS; i++) {
        dictEntryLink link = dictFindLink(d, k[i], NULL);
        kv[i] = dictGetKV(*link);
    }
    kvstoreRelease(ks[0]);
    for (int i = 0; i < HYBSNAP_TEST_BUCKET_KEYS; i++) decrRefCount(kv[i]);
    for (int i = 0; i <= HYBSNAP_TEST_BUCKET_KEYS; i++) sdsfree(k[i]);
    sdsfree(sv->mstage);
    sdsfree(sv->mscratch);
    listSetFreeMethod(sv->mq, sdsfreegeneric);
    listRelease(sv->mq);
    pthread_mutex_destroy(&sv->mq_lock);
    zfree(sv);
    zfree(et);
    hybsnap_epoch_mode = saved_mode;
    server.dbnum = saved_dbnum;
}

int hybsnapTest(int argc, char **argv, int flags) {
    UNUSED(argc);
    {
        union { uint8_t byte; kvBits bits; } w;
        w.byte = 0; w.bits.key_sds_type = 3; w.bits.no_evict = 1;
        test_cond("kvBits: key_sds_type and no_evict occupy bits 0-2", w.byte == 0x07);
        w.byte = 0; w.bits.has_epoch = 1;
        test_cond("kvBits: has_epoch is bit 3", w.byte == 0x08);
    }
    UNUSED(argv);
    int accurate = flags & REDIS_TEST_ACCURATE;
    int large = flags & REDIS_TEST_LARGE_MEMORY;

    /* Minimal server state so serverLog() works in test mode. */
    server.in_fork_child = CHILD_TYPE_NONE;
    server.logfile = "";
    server.verbosity = LL_DEBUG;
    server.hybrid_snapshot = 1;
    hybsnapArenaInit();

#ifdef HAVE_HYBRID_ARENA
    test_cond("Hybrid arena is available on Linux+jemalloc", hybsnapArenaExists());
#else
    test_cond("Hybrid arena reported unavailable on this platform", !hybsnapArenaExists());
    void *fb = zmalloc_hybrid(100);
    test_cond("fallback alloc goes to the default allocator", fb != NULL && !zmalloc_in_hybrid_arena(fb));
    zfree(fb);
    /* The background scan and the keyspace overlay do not depend on the arena. */
    hybsnapScanTests(accurate);
    hybsnapEpochLayoutTests();
    hybsnapEpochScanTests();
    hybsnapEpochEmitAheadTests();
    hybsnapRefcountTests();
    hybsnapOverlayTests(0);
    hybsnapOverlayTests(1);
    hybsnapOverlayBucketTests();
    hybsnapOverlayWindowTests();
    hybsnapOverlayRehashTests();
    test_report();
    return 0;
#endif

    /* 1. predicate */
    char *p = zmalloc_hybrid(100);
    char *q = zmalloc(100);
    memset(p, 'x', 100);
    test_cond("zmalloc_in_hybrid_arena() is true for an arena pointer", zmalloc_in_hybrid_arena(p));
    test_cond("zmalloc_in_hybrid_arena() is false for a zmalloc pointer", !zmalloc_in_hybrid_arena(q));
    test_cond("zmalloc_size() >= requested size", zmalloc_size(p) >= 100);

    /* 2. fork visibility */
    test_cond("HYBSNAP_NOFORK mapping carries the VM_DONTCOPY (dc) flag", hybsnapTestMappingIsDontcopy(p) == 1);
    test_cond("zmalloc mapping does not carry the dc flag", hybsnapTestMappingIsDontcopy(q) == 0);
    test_cond("fork child survives and can use the default allocator", hybsnapTestChildAllocatorAlive() == 1);
    test_cond("fork child faults when writing a HYBSNAP_NOFORK allocation", hybsnapTestChildWriteFaults(p) == 1);
    test_cond("fork child can write a zmalloc allocation", hybsnapTestChildWriteFaults(q) == 0);
    /* Fork visibility flip: one madvise over the reservation. */
    test_cond("arena is DONTFORK by default", !hybsnapArenaIsForkable());
    test_cond("flip: arena made forkable", hybsnapArenaSetForkable(1) && hybsnapArenaIsForkable());
    test_cond("flip: fork child can write a HYBSNAP_NOFORK allocation while forkable", hybsnapTestChildWriteFaults(p) == 0);
    test_cond("flip: arena back to DONTFORK", hybsnapArenaSetForkable(0) && !hybsnapArenaIsForkable());
    test_cond("flip: fork child faults again on the same allocation", hybsnapTestChildWriteFaults(p) == 1);

    /* Large (>= 2MB, multi-page-run) allocation is also DONTFORK. */
    char *big = zmalloc_hybrid(5 * 1024 * 1024);
    memset(big, 1, 5 * 1024 * 1024);
    test_cond("large HYBSNAP_NOFORK allocation is in the arena", zmalloc_in_hybrid_arena(big));
    test_cond("fork child faults on a large HYBSNAP_NOFORK allocation (first page)", hybsnapTestChildWriteFaults(big) == 1);
    test_cond("fork child faults on a large HYBSNAP_NOFORK allocation (last page)",
              hybsnapTestChildWriteFaults(big + 5 * 1024 * 1024 - 1) == 1);

    /* realloc keeps the class */
    p = zrealloc_hybrid(p, 5000);
    test_cond("zrealloc_hybrid() result stays in the arena", zmalloc_in_hybrid_arena(p));
    test_cond("fork child faults after realloc", hybsnapTestChildWriteFaults(p) == 1);
    zfree(p);
    zfree(q);
    zfree(big);

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
                ptrs[i] = zmalloc_hybrid(sz);
                ((char*)ptrs[i])[0] = 1;
                ((char*)ptrs[i])[sz-1] = 1;
            }
            for (int i = 0; i < N; i += 2) { zfree(ptrs[i]); ptrs[i] = NULL; }
            hybsnapTestPurgeArena();
            for (int i = 1; i < N; i += 2) {
                seed = seed * 1103515245u + 12345u;
                ptrs[i] = zrealloc_hybrid(ptrs[i], 8 + (seed >> 8) % 16384);
            }
            hybsnapTestPurgeArena();
            for (int i = 1; i < N; i += (N / 32)) {
                if (!zmalloc_in_hybrid_arena(ptrs[i])) bad_class++;
                if (hybsnapTestChildWriteFaults(ptrs[i]) != 1) bad_fork++;
            }
            for (int i = 1; i < N; i += 2) zfree(ptrs[i]);
            hybsnapTestPurgeArena();
        }
        /* Fresh allocation after purge cycles must still be DONTFORK (commit path). */
        void *fresh = zmalloc_hybrid(4096 * 3);
        memset(fresh, 2, 4096 * 3);
        test_cond("mapping after purge cycles carries the dc flag", hybsnapTestMappingIsDontcopy(fresh) == 1);
        test_cond("stress: sampled live pointers are all in the arena", bad_class == 0);
        test_cond("stress: sampled live pointers all fault in the child after purge/decay/realloc cycles", bad_fork == 0);
        test_cond("allocation after purge cycles is still DONTFORK", hybsnapTestChildWriteFaults(fresh) == 1);
        zfree(fresh);
        zfree(ptrs);
    }

    /* 4. tcache isolation: freed HYBSNAP_NOFORK blocks must never be recycled into
     * default-arena allocations, and vice versa. */
    {
        const int N = 2000;
        void **a = zmalloc(sizeof(void*) * N);
        for (int i = 0; i < N; i++) a[i] = zmalloc_hybrid(64);
        for (int i = 0; i < N; i++) zfree(a[i]);
        int leaked = 0;
        for (int i = 0; i < N; i++) { a[i] = zmalloc(64); if (zmalloc_in_hybrid_arena(a[i])) leaked++; }
        for (int i = 0; i < N; i++) zfree(a[i]);
        test_cond("freed HYBSNAP_NOFORK blocks are never handed to zmalloc()", leaked == 0);

        for (int i = 0; i < N; i++) a[i] = zmalloc(64);
        for (int i = 0; i < N; i++) zfree(a[i]);
        int wrong = 0;
        for (int i = 0; i < N; i++) { a[i] = zmalloc_hybrid(64); if (!zmalloc_in_hybrid_arena(a[i])) wrong++; }
        for (int i = 0; i < N; i++) zfree(a[i]);
        test_cond("freed zmalloc blocks are never handed to zmalloc_hybrid()", wrong == 0);
        zfree(a);
    }

    /* 5. accounting */
    {
        size_t before = zmalloc_used_memory();
        void *x = zmalloc_hybrid(10000);
        size_t mid = zmalloc_used_memory();
        zfree_with_size(x, zmalloc_size(x));
        size_t after = zmalloc_used_memory();
        test_cond("zmalloc_hybrid allocations are counted in zmalloc_used_memory()", mid > before && after == before);
    }

    /* 6. microbenchmark (reported, not asserted) */
    {
        const int N = accurate ? 5000000 : 1000000;
        void **a = zmalloc(sizeof(void*) * N);
        long long t0, t1;

        t0 = hybsnapTestNowNs();
        for (int i = 0; i < N; i++) { void *x = zmalloc(64); zfree(x); }
        t1 = hybsnapTestNowNs();
        double z_pair = (double)(t1 - t0) / N;

        t0 = hybsnapTestNowNs();
        for (int i = 0; i < N; i++) { void *x = zmalloc_hybrid(64); zfree(x); }
        t1 = hybsnapTestNowNs();
        double s_pair = (double)(t1 - t0) / N;

        t0 = hybsnapTestNowNs();
        for (int i = 0; i < N; i++) a[i] = zmalloc(64);
        for (int i = 0; i < N; i++) zfree(a[i]);
        t1 = hybsnapTestNowNs();
        double z_bulk = (double)(t1 - t0) / N;

        t0 = hybsnapTestNowNs();
        for (int i = 0; i < N; i++) a[i] = zmalloc_hybrid(64);
        for (int i = 0; i < N; i++) zfree(a[i]);
        t1 = hybsnapTestNowNs();
        double s_bulk = (double)(t1 - t0) / N;

        t0 = hybsnapTestNowNs();
        for (int i = 0; i < N; i++) a[i] = zmalloc_hybrid(64);
        for (int i = 0; i < N; i++) zfree_with_size(a[i], 64);
        t1 = hybsnapTestNowNs();
        double s_bulk_sized = (double)(t1 - t0) / N;

        printf("BENCH alloc+free 64B, N=%d: zmalloc/zfree pair %.1f ns, zmalloc_hybrid pair %.1f ns; "
               "bulk zmalloc %.1f ns/op, bulk zmalloc_hybrid %.1f ns/op, bulk zmalloc_hybrid sized-free %.1f ns/op\n",
               N, z_pair, s_pair, z_bulk, s_bulk, s_bulk_sized);
        zfree(a);
        test_cond("microbenchmark ran", 1);
    }

    /* 7. VMA count proxy: many small arena allocations must not explode the
     * number of mappings (MADV_DONTFORK splits VMAs, but adjacent extents
     * with identical flags merge back). */
    {
        const int N = large ? 10000000 : 1000000;
        int maps_before = hybsnapTestCountMaps();
        void **a = zmalloc(sizeof(void*) * N);
        for (int i = 0; i < N; i++) { a[i] = zmalloc_hybrid(96); ((char*)a[i])[0] = 1; }
        int maps_after = hybsnapTestCountMaps();
        for (int i = 0; i < N; i++) zfree(a[i]);
        zfree(a);
        printf("VMA maps: before=%d after %d x 96B HYBSNAP_NOFORK allocations=%d (delta %d)\n",
               maps_before, N, maps_after, maps_after - maps_before);
        test_cond("VMA count stays far below vm.max_map_count default (65530)", maps_after < 10000);
    }

    hybsnapPlacementTests();
    hybsnapScanTests(accurate);
    hybsnapEpochLayoutTests();
    hybsnapEpochScanTests();
    hybsnapEpochEmitAheadTests();
    hybsnapRefcountTests();
    hybsnapOverlayTests(0);
    hybsnapOverlayTests(1);
    hybsnapOverlayBucketTests();
    hybsnapOverlayWindowTests();
    hybsnapOverlayRehashTests();

    test_report();
    return 0;
}
#endif
