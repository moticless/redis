/*
 * snapshot.c - Hybrid snapshot (design: docs/hybrid-snapshot.md; API: snapshot.h).
 *
 * A hybrid save splits the keyspace by value class. SNAP_NOFORK values (every
 * kvobj header, the keyspace hash tables and single-allocation values up to
 * save-small-max-bytes) live in a jemalloc arena the fork child never maps
 * (MADV_DONTFORK, zmalloc.c) and are serialized by a scan thread in the
 * parent; SNAP_FORK values (hash tables, skiplists, quicklists, streams,
 * module types, large encodings) stay in forkable memory and the child
 * serializes them from its copy-on-write snapshot. The child is the single
 * writer of the output (RDB file, replica sockets or an AOF base file).
 *
 * One save, in order: the main thread captures the per-db sizes, freezes the
 * keyspace (one reference and a generation stamp per fork-instant dict, a
 * guard on every store), forks the child, then starts the scan thread. The
 * thread reads the frozen dicts bucket by bucket in a fixed order, below a
 * limit (hi) that the main thread sets only while it works on the keyspace
 * (the credit window); the main thread keeps serving writes through the write
 * funnel: in place behind the thread, made on a duplicate of the bucket's
 * chain between the thread and hi, and beyond hi the bucket is serialized
 * first (emit-ahead) and then written in place. Records reach the child over two pipes: a small
 * channel of 64 KB buffers of pre-encoded records, a big channel of kvobj
 * header copies whose values the child resolves in its own memory. The child
 * takes the small backlog first, then one big value, until END; the parent
 * writes back each dict's duplicated buckets once the thread has passed them and completes
 * the save when the child exits (checkChildrenDone -> snapOnChildEnd ->
 * the classic done handler). Abort (hard mark, FLUSHALL, killed child) joins
 * the thread, drains every overlay and leaves the previous dump untouched.
 *
 * Sections of this file: arena fork visibility; classification and
 * placement; clone-on-write for the overlay; the scan engine (positions,
 * the credit window, write sections, per-dict state, bitmaps, the thread); the save
 * (queues, channels, stores vs dbs, hard mark); the child writer; the
 * parent orchestration; unit tests (redis-server test snapshot).
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

/* Slot (dict) states, monotonic; the slot is owed to the save until DONE. */
#define SNAP_SLOT_PENDING  0   /* Not reached by the scan thread yet. */
#define SNAP_SLOT_SCANNING 1   /* Being read by the scan thread. */
#define SNAP_SLOT_SCANNED  2   /* Fully dispatched; waiting for the main thread to complete it. */
#define SNAP_SLOT_DONE     3   /* Completed by the main thread: overlay written back, dict unfrozen. */

/* The thread's credit W (buckets per grant); per-dict word: save generation and slot state. */
#define SNAP_WINDOW_BUCKETS_DEFAULT 1024
#define SNAP_WINDOW_BUCKETS_MAX 65536   /* overlay windows are W rounded up to a power of two: an array of that many buckets each */
#ifndef SNAP_SCAN_PREFETCH2
#define SNAP_SCAN_PREFETCH2 8
#endif
#ifndef SNAP_SCAN_PREFETCH
#define SNAP_SCAN_PREFETCH 16   /* buckets ahead of the scan whose first entry is prefetched */
#endif
#define SNAP_WORD(gen, st) (((uint64_t)(gen) << 8) | (uint64_t)(st))
#define SNAP_WORD_GEN(w) ((w) >> 8)
#define SNAP_WORD_STATE(w) ((uint8_t)((w) & 0xff))
/* A (store, dict) pair packed in one atomic. */
#define SNAP_SCAN_ENC(store_idx, didx) (((uint64_t)(uint32_t)(store_idx) << 32) | (uint32_t)(didx))
#define SNAP_SCAN_DEC_STORE(v) ((int)((v) >> 32))
#define SNAP_SCAN_DEC_DIDX(v) ((int)((v) & 0xffffffffu))

/* Messages to the child and the staging buffers. */
#define SNAP_MSG_BUFFER 1     /* pre-encoded records of one (db, slot) */
#define SNAP_MSG_KVOBJ  2     /* whole kvobj allocation copy of a SNAP_FORK key */
#define SNAP_MSG_END    3     /* big channel: robj_off = KVOBJ messages sent, src_addr = small-channel messages sent */
#define SNAP_MSG_ABORT  4
#define SNAP_SMALL_BACKLOG (1024 * 1024)   /* child: take a big value only below this much small backlog in the parent */
#define SNAP_SMALL_WAIT_MS 20              /* child: how long to wait for that backlog to reach the pipe before taking a big value */
#define SNAP_STAGE_BYTES (64 * 1024)   /* staging buffer flush threshold */

/* Where the scan thread is relative to a bucket. */
#define SNAP_POS_BEHIND 0   /* the thread already read this bucket */
#define SNAP_POS_INSIDE 1   /* the thread is reading it now */
#define SNAP_POS_AHEAD  2   /* the thread has not reached it */

/* Outbound queue drain modes and buffer pool. */
#define SNAP_DRAIN_TRY   0   /* write what the pipe takes, never wait */
#define SNAP_DRAIN_BOUND 1   /* wait until oq_bytes <= target; give up on abort */
#define SNAP_DRAIN_ALL   2   /* wait until the queue is empty, abort or not */
#define SNAP_OUT_POOL_MAX 16   /* staging buffers kept for reuse */
#define SNAP_STAGE_SLACK (8 * 1024)   /* headroom a record may take before the flush */

/* ---- Types ------------------------------------------------------------ */

typedef struct snapScanCtx snapScanCtx;
/* Scan thread, once per fork-instant key of a slot; strictly read-only on the keyspace. */
typedef void (snapScanDispatchFn)(void *privdata, int store_idx, int didx, kvobj *kv);
/* Main thread, when a slot is complete, before its dict is unfrozen ('aborted': not fully scanned). */
typedef void (snapScanSlotDoneFn)(void *privdata, int store_idx, int didx, int aborted);
/* Scan thread, after the last slot or on abort, before the scan is published as finished. */
typedef void (snapScanThreadDoneFn)(void *privdata, int aborted);
typedef struct snapSave snapSave;

/* Keyspace placement statistics (DEBUG SNAP-PLACEMENT, SNAP-DRIFT). */
typedef struct snapPlacementStats {
    long long keys;
    long long headers_in_arena;        /* every kvobj header should be here */
    long long headers_outside;
    long long class_nofork;            /* snapKeyClass() == SNAP_NOFORK */
    long long class_fork;
    long long drift_nofork_outside;    /* policy says NOFORK, value outside the arena */
    long long drift_fork_inside;       /* policy says FORK (single-alloc, too big), value inside */
    long long violations;              /* header outside, or multi-allocation value inside */
    long long dict_tables_in_arena;    /* dict structs + bucket arrays of db->keys / db->expires */
    long long dict_tables_outside;
} snapPlacementStats;

/* The privdata of every kvstore hook on store s (guard, layout and overlay
 * callbacks alike, one convention): the scan and the store index, plus what
 * the overlay's user handed to snapScanSetOverlay for that store. The redisDb
 * owning a store is resolved at call time (SWAPDB moves a store to another db
 * id; a FLUSHDB leaves it with no owner), the db id in the dump is always s. */
struct snapGuardPriv { struct snapScanCtx *ctx; int s; void *user; };

/* What the save keeps per dict in the kvstore extension slot, allocated the
 * first time the save touches the dict (a write before the scan thread
 * reached it, a pause before hi may pass its layout, or any access while the
 * thread is on it) and freed when the dict's scan
 * completes. Meaningful only while the dict's word carries this save's
 * generation. The overlay lives next to it (kvstoreDictMetaBase.ov). */
typedef struct snapDictState {
    uint8_t *bits[2];           /* per table: one bit per bucket, set when the main thread emitted the
                                   bucket's fork-instant chain ahead of the scan thread; main thread
                                   stores (release), scan thread loads (acquire) */
    size_t bytes[2];
    int paused;                 /* rehash and auto-resize paused: hi may pass the layout (positions are stable) */
} snapDictState;


/* The scan thread's chain-walk callback arguments. */
struct snapScanEntryArgs { snapScanCtx *ctx; int s; int didx; };


struct snapScanCtx {
    kvstore **stores;               /* One per db (db->keys). */
    int num_stores;
    int *num_dicts;                 /* kvstoreNumDicts() per store. */
    int *frozen;                    /* Per store: 1 while our guard is installed on it (snapScanFreeze() until its last dict completes). */
    dict ***dicts;                  /* Per store: the fork-instant dicts by slot, one reference each; NULL once completed. */
    long *nrefs;                    /* Per store: table entries not completed yet; the guard comes off at zero. Main thread. */
    size_t bitmap_bytes;            /* Bucket bitmaps attached to the table dicts. Main thread. */
    long retained_dicts;            /* Table dicts their store let go of before the save completed them (freed here). Main thread. */
    redisAtomic int64_t cur_slot;   /* The slot the thread is scanning (claimed, not yet SCANNED), SNAP_SCAN_ENC, -1 none: the abort sweep completes it. */
    int ended;                      /* Main thread: every store completed; nothing is owed any more. */
    long dicts_grown;               /* attached dicts that expanded during the save (bitmap remapped), main thread */
    uint64_t gen;                   /* this save's generation (never 0): stamps every dict it touches (kvstoreDictMetaBase.ext_word) */
    const uint8_t *slot_filter;     /* cluster mode, optional: freeze and scan only the slots whose bit is set (a slot snapshot) */
    struct snapGuardPriv *guards;   /* Per store: the privdata of every hook on that store. */

    snapScanDispatchFn *dispatch;
    snapScanSlotDoneFn *slot_done;
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
    int frozen_all;                 /* snapScanFreeze() done. */
    unsigned long window_buckets;   /* copied from snap_window_buckets at create: the thread's credit W */
    unsigned long ov_block;         /* overlay windows: aligned runs of this many buckets (W rounded up to a power of two) */
    int *ord_base;                  /* Per store: the ordinal of its dict 0 in the scan positions. */
    uint64_t pos_end;               /* the position past every dict: the thread's lo and cur at the end */
    uint64_t hi;                    /* main thread: its own last store of snap_hi */
    uint64_t mid;                   /* main thread: the next grant is due once lo reaches this */
    long long grants;               /* main thread: hi raised inside a section */
    redisAtomic long long stalls;   /* thread: times it waited for hi */
    redisAtomic long long stall_us; /* thread: time spent waiting */
    redisAtomic long long buckets_skipped;   /* buckets the thread skipped (already emitted) */
    /* Optional hooks, set by the owner before start. */
    snapScanThreadDoneFn *thread_done;   /* scan thread, after the last slot or on abort, before the scan is published as finished */
    void (*thread_init)(void *privdata);   /* first thing on the scan thread */
    void (*window_done)(void *privdata);   /* after every window, and before thread_done */
    void (*store_done)(void *privdata, int s);   /* main thread, when a store's last slot completed */
};


typedef struct snapMsgHdr {
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
} snapMsgHdr;

/* records the key and expire counts at the fork instant. RDB stream must announce 
 * those counts before the keys they describe, and by then the live keyspace has changed. */
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

/* One page shared with the child (MAP_SHARED, mapped before the fork). */
typedef struct snapShared {
    redisAtomic size_t small_queued;   /* small-channel bytes queued in the parent, for the child's choice of channel */
} snapShared;

struct snapSave {
    snapScanCtx *scan;
    void *tables;              /* one block: stores, flushed and sizes below, each dbnum entries */
    void *bitmaps;             /* cluster mode, one block: slot_filter then trim_slots, CLUSTER_SLOTS bits each; NULL when neither is used */
    kvstore **stores;          /* per store (db id at the fork instant): the kvstore being scanned */
    snapFlushedDb *flushed;    /* per store */
    int dbnum;
    /* Two channels to the child. The small channel carries the
     * BUFFER messages, the big channel KVOBJ, END and
     * ABORT, so the child can drain the small backlog first and take a big
     * value only when that backlog is low: a bounded queue then never parks
     * the scan thread while the child is inside a big value. */
    int pipe_rd, pipe_wr;             /* small channel */
    int big_rd, big_wr;               /* big channel */
    snapShared *shared;
    int tag_all;                      /* debug (hybrid-snapshot-tag-all): every BUFFER depends on all big values sent before it */
    snapDbSizes *sizes;
    int req, rdbflags;
    int kind;                  /* SNAP_SAVE_RDB/SOCKETS/AOF */
    int aof_cmd_form;          /* CHILD_TYPE_AOF without the RDB preamble: records are AOF commands, not RDB pairs */
    int slots_form;            /* atomic slot migration's slot snapshot (SLAVE_REQ_SLOTS_SNAPSHOT): records are
                                  RESTORE commands from DUMP payloads (cluster_asm.c), the stream is RESP, no RDB framing */
    uint8_t *slot_filter;      /* slots_form: the migrating slots at the fork instant (CLUSTER_SLOTS bits); only they are frozen and scanned */
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
    /* Scan-thread staging buffer. */
    sds stage;
    int stage_dbid, stage_slot;
    redisAtomic long long keys_nofork, keys_fork, bytes_sent, msgs_sent;
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
    long long main_buckets;             /* main thread only */
    redisAtomic long long main_keys;    /* keys the main thread emitted ahead (read by the scan thread: it owes their save delay) */
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
static snapSave *snap_save = NULL;   /* the running hybrid save, main thread */


#define snapIsAof(sv) ((sv)->kind == SNAP_SAVE_AOF)
#define snapToSockets(sv) ((sv)->kind == SNAP_SAVE_SOCKETS)

/* ---- Function declarations (used only in this file) ------------------- */

/* ---- SNAP_NOFORK arena ------------------------------------------------ */

/* Address range reserved for the arena, and the part handed to jemalloc so far (0 if unavailable). */
static size_t snapArenaVaReserved(void);
static size_t snapArenaVaUsed(void);
static int snapArenaSetForkable(int forkable);

/* ---- Classification and placement ------------------------------------- */

/* Single-allocation encoding that MAY be SNAP_NOFORK (strings, listpacks, intsets). */
static int snapEncodingIsNoforkClass(int type, int encoding);
/* Placement policy: the class a value of this encoding and size should have. */
static int snapClassify(int type, int encoding, size_t alloc_bytes);
/* Actual class of a stored value, decided by where its memory is. */
static int snapKeyClass(const robj *o);
static void snapPlacementScan(redisDb *db, snapPlacementStats *st);
static void snapPlacementDriftReply(struct client *c, redisDb *db, int limit);

/* ---- Scan engine ------------------------------------------------------ */

/* Create a scan over 'num_stores' kvstores; nothing is frozen or started yet. */
static snapScanCtx *snapScanCreate(kvstore **stores, int num_stores, snapScanDispatchFn *dispatch,
                            snapScanSlotDoneFn *slot_done, void *privdata);
/* Before start: the keyspace overlay callbacks (clone, free_orig, enter, prepare). */
static void snapScanSetOverlay(snapScanCtx *ctx, const kvstoreSnapHooks *hooks, void **store_privdata);
/* Before start, cluster mode: freeze and scan only the slots whose bit is set (CLUSTER_SLOTS bits, must outlive the scan). */
static void snapScanSetSlotFilter(snapScanCtx *ctx, const uint8_t *bitmap);
/* Main thread: freeze every store (the fork instant) and spawn the thread; C_OK / C_ERR. */
static int snapScanStart(snapScanCtx *ctx);
/* The same in two steps: freeze first, spawn the thread later (the fork goes in between). */
static void snapScanFreeze(snapScanCtx *ctx);
static int snapScanStartThread(snapScanCtx *ctx);
/* Main thread: complete the slots the thread finished since the last call; returns how many. */
static int snapScanHandleCompletions(snapScanCtx *ctx);
/* Readable when a slot completes or the thread exits (-1 if none); register the handler on it with ctx as client data. */
static int snapScanReadEventFd(snapScanCtx *ctx);
static void snapScanAeHandler(struct aeEventLoop *el, int fd, void *privdata, int mask);
/* 1 once the thread has exited (all slots scanned, or aborted). */
static int snapScanIsFinished(snapScanCtx *ctx);
/* Main thread: stop and join the thread, complete every slot. */
static void snapScanAbort(snapScanCtx *ctx);
/* Main thread: join if needed and free the ctx; every slot must be DONE. */
static void snapScanRelease(snapScanCtx *ctx);
/* The scan was aborted; complete every slot of every store (main thread). */
static inline int snapScanAborted(snapScanCtx *ctx);
static void snapScanFinishAll(snapScanCtx *ctx);
/* SNAP_SLOT_* of (store, dict); progress counters for INFO and tests. */
static int snapScanSlotState(snapScanCtx *ctx, int store_idx, int didx);
static void snapScanProgress(snapScanCtx *ctx, long *slots_total, long *slots_scanned,
                      long *slots_done, long long *keys_dispatched);

/* ---- Credit window and write sections --------------------------------- */

/* Every guard or funnel hook (main thread, or a module thread holding the GIL): the first opens the section and caps the thread, later ones may raise the cap. */
static void snapFunnelOpen(void);
/* The thread's credit W, buckets per grant (DEBUG SNAP-WINDOW-BUCKETS; tests). */
static void snapSetWindowBuckets(unsigned long n);
static unsigned long snapGetWindowBuckets(void);

/* ---- Bucket bitmaps and accounting ------------------------------------ */

/* Per-save bucket bitmaps: a set bit means the main thread already emitted the bucket's chain. */
static void snapScanMarkBucketEmitted(snapScanCtx *ctx, int store_idx, int didx, int table, unsigned long idx);
static int snapScanBucketEmitted(snapScanCtx *ctx, int store_idx, int didx, int table, unsigned long idx);
/* An attached dict started or finished rehashing: its bitmap follows the new table. */
static void snapScanDictRehashStarted(snapScanCtx *ctx, int store_idx, int didx, dict *d);
static void snapScanDictRehashCompleted(snapScanCtx *ctx, int store_idx, int didx, dict *d);
/* Bitmap accounting: dicts that grew under the save, dicts retained past their store, bitmap bytes. */
static long snapScanDictsGrown(snapScanCtx *ctx);
static long snapScanRetainedDicts(snapScanCtx *ctx);
static size_t snapScanBitmapBytes(snapScanCtx *ctx);

/* ---- Keyspace overlay ------------------------------------------------- */

/* Overlay callbacks (privdata = the store's snapGuardPriv): clone a kvobj into the arena, release a fork-instant original. */
static void *snapOverlayCloneKey(void *privdata, int didx, void *orig);
static void snapOverlayFreeOriginal(void *privdata, int didx, void *orig);
/* Does the clone of an original take over its value (1) or copy it (0)? Pure: the unchanged header and the value's address. */
static int snapOriginalSharesValue(const kvobj *orig);
/* Retained-originals accounting, the db that owns a store, the write-back of overlayed buckets. */
static void snapSaveRetainedAdd(kvobj *orig, int value_shared);
static void snapSaveRetainedSub(kvobj *orig, int value_shared);
static redisDb *snapStoreOwnerDb(snapSave *sv, int s);
static void snapSaveDrainSideTables(void);

/* ---- The save --------------------------------------------------------- */

/* The three saves it dispatches to: same contracts as rdbSaveBackground, rdbSaveToSlavesSockets, rewriteAppendOnlyFileBackground. */
static int snapSaveBackground(int req, char *filename, rdbSaveInfo *rsi, int rdbflags);
static int snapSaveToSlavesSockets(int req, rdbSaveInfo *rsi);
static int snapSaveBackgroundAof(void);

/* ---- Messages to the child -------------------------------------------- */

static int snapSendMsg(snapSave *sv, uint32_t type, int dbid, int slot,
                       const void *payload, uint32_t len, uint32_t robj_off, uint64_t src_addr);
static void snapStageFlush(snapSave *sv);
static int snapOutEnqueue(snapSave *sv, sds m, uint32_t type);
static void snapOutRecycle(snapSave *sv, sds m);

/* ---- Observability ---------------------------------------------------- */

static size_t snapScanTableBytes(snapScanCtx *ctx);
/* The running save: slots and keys. */
static void snapSaveProgress(long *slots_total, long *slots_done, long long *keys_nofork,
                      long long *keys_fork, long long *bytes_sent);
/* Retained originals (replaced or deleted) and overlayed buckets; the peak over the last save. */
static size_t snapSaveRetainedBytes(void);
static long snapSaveSideEntries(void);
static size_t snapSaveLastRetainedPeak(void);
/* Bytes queued for the child (main-thread queue plus the thread's outbound queue); the peak over the last save. */
static size_t snapSaveQueuedBytes(void);
static size_t snapSaveLastQueuedPeak(void);
/* Bitmaps and dicts: bitmap bytes, dicts that grew, fork-instant dicts their store let go of (cumulative). */
static size_t snapSaveBitmapBytes(void);
static long snapSaveGrownDicts(void);
static long snapSaveRetainedDicts(void);
/* Main-thread work: buckets and keys emitted ahead of the window; in-place writes behind it (this save, last save). */
static long long snapSaveMainBuckets(void);
static long long snapSaveMainKeys(void);
static long long snapSaveInplaceWrites(void);
static long long snapSaveLastInplaceWrites(void);

/* ---- Defined elsewhere ------------------------------------------------ */

void updateSlotAllocSize(redisDb *db, int didx, kvobj *kv, int64_t oldsize, int64_t newsize);   /* db.c */

/* ----------------------------------------------------------------------- */

/* The arena itself (reserved DONTFORK address range, extent hooks, per-thread
 * tcache flags, range-aware zfree/zrealloc) lives in zmalloc.c so that sds,
 * listpack, intset and dict code keep arena memory in the arena without
 * knowing about classes. These are the snapshot-facing entry points. */
void snapArenaInit(void) {
#ifdef HAVE_NOFORK_ARENA
    if (!zmalloc_nofork_arena_init()) {
        serverLog(LL_WARNING, "SNAP_NOFORK arena disabled: could not reserve a DONTFORK address "
                              "range or create the jemalloc arena; hybrid snapshot runs without it.");
        return;
    }
    zmalloc_nofork_set_active(server.hybrid_snapshot);       /* new headers, tables and values go there only when on */
    if (!server.hybrid_snapshot) snapArenaSetForkable(1);   /* nothing is placed in it; classic forks map it */
    serverLog(LL_NOTICE, "SNAP_NOFORK arena created (jemalloc arena %u, %zu GB of address space reserved, "
                         "%s).", zmalloc_nofork_arena_index(), snapArenaVaReserved() >> 30,
                         server.hybrid_snapshot ? "MADV_DONTFORK: hybrid snapshot on" : "forkable: hybrid-snapshot no");
#else
    serverLog(LL_NOTICE, "SNAP_NOFORK arena unavailable on this platform/allocator (needs 64-bit Linux "
                         "with jemalloc); hybrid snapshot, if enabled, runs without it.");
#endif
}

/* ---- Fork visibility of the arena (see snapshot.h) ---------------------- */

static int snap_arena_fallback = 0;        /* flipped forkable for a non-hybrid child, flip back on reap */
static int snap_forking_no_keyspace = 0;   /* set around snapForkNoKeyspace()'s redisFork() */
static int snap_child_no_keyspace = 0;     /* this process is a child forked without the keyspace */
int snapChildNoKeyspace(void) { return snap_child_no_keyspace; }

/* Main thread: a hybrid save was requested but a loaded module has not
 * declared REDISMODULE_OPTIONS_HANDLE_HYBRID_SNAPSHOT; the classic child runs
 * instead. Logged when the blocking module changes, not on every save. */
static void snapHybridFallbackLog(void) {
    static sds last = NULL;
    const char *who = moduleHybridSnapshotBlockedBy();
    if (last && !strcmp(last, who)) return;
    sdsfree(last);
    last = sdsnew(who);
    serverLog(LL_WARNING, "Hybrid snapshot: falling back to the fork child, module '%s' has not declared "
                          "REDISMODULE_OPTIONS_HANDLE_HYBRID_SNAPSHOT", who);
}

/* Fork visibility of the arena: DONTFORK, so a child forked without the
 * keyspace (the hybrid save child, a REDISMODULE_FORK_NO_KEYSPACE module fork:
 * snapForkNoKeyspace) never maps it. Any other fork (classic save, diskless
 * sync, AOF rewrite, module, Lua debugger) needs the keyspace, so redisFork()
 * flips it to DOFORK (snapArenaBeforeFork) and the child reap path flips it
 * back once no child is left (snapArenaChildExited). One madvise each way,
 * O(1) in keys; that child pays classic copy-on-write. */
int snapArenaForkable(void) {
#ifdef HAVE_NOFORK_ARENA
    return snapArenaAvailable() && zmalloc_nofork_arena_forkable();
#else
    return 0;
#endif
}

static int snapArenaSetForkable(int forkable) {
#ifdef HAVE_NOFORK_ARENA
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
    if (snap_forking_no_keyspace || !snapArenaAvailable() || snapArenaForkable()) return;
    /* A classic child (RDB with the feature off, diskless sync, AOF rewrite,
     * module or Lua debugger fork) walks the keyspace: it must map the arena. */
    if (snapArenaSetForkable(1)) {
        snap_arena_fallback = 1;
        serverLog(LL_NOTICE, "SNAP_NOFORK arena made forkable for a %s child (classic copy-on-write applies)",
                  strChildType(purpose));
    }
}

void snapArenaChildExited(void) {
    if (!snap_arena_fallback) return;
    if (hasActiveChildProcess() || ldbPendingChildren()) return;   /* another classic child still runs */
    snap_arena_fallback = 0;
    if (server.hybrid_snapshot && snapArenaSetForkable(0))
        serverLog(LL_NOTICE, "SNAP_NOFORK arena back to MADV_DONTFORK");
}

/* Child forked without the keyspace, first thing after the fork: the arena's
 * range is not mapped here (MADV_DONTFORK), so the kernel would hand its
 * addresses to our own allocations as the heap grows; zmalloc routes frees and
 * reallocs by address (zmalloc_is_nofork), so such a block would be
 * reallocated in the parent-only arena and the write would fault (seen as a
 * crash in the hybrid child's pipe reader). Reserve the whole range PROT_NONE
 * so nothing of ours can land in it. */
static void snapChildReserveArenaRange(void) {
#ifdef HAVE_NOFORK_ARENA
    void *lo; size_t len;
    if (!zmalloc_nofork_arena_range(&lo, &len)) return;
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

/* redisFork() for a child that never touches the keyspace: the hybrid save
 * child and a REDISMODULE_FORK_NO_KEYSPACE module fork. The arena is left
 * DONTFORK (no page tables copied for it, no copy-on-write on it while the
 * child runs); in the child the arena's range is reserved and the keyspace
 * module APIs are refused (snapChildNoKeyspace). When the arena is forkable
 * anyway (feature off, a debugger child) the child has it mapped and needs no
 * reservation, but the refusals apply all the same: one contract whatever the
 * configuration. Same return as redisFork(). */
int snapForkNoKeyspace(int purpose) {
    snap_forking_no_keyspace = 1;
    int childpid = redisFork(purpose);
    snap_forking_no_keyspace = 0;
    if (childpid == 0) {
        snap_child_no_keyspace = 1;
        if (snapArenaAvailable() && !snapArenaForkable()) snapChildReserveArenaRange();
    }
    return childpid;
}

/* CONFIG SET hybrid-snapshot apply hook: flips the arena, refuses mid-save.
 * Saves take the hybrid path whether or not the SNAP_NOFORK arena exists
 * (without it the CoW benefit is gone but the mechanics are identical, which
 * is what sanitizer builds exercise). Switching is O(1): the arena is flipped
 * forkable when off, and values allocated meanwhile simply live in the
 * default allocator. */
int snapHybridConfigApply(const char **err) {
    if (server.loading) return 1;   /* applied again by the startup path */
    if (hasActiveChildProcess() || snapSaveInProgress() || ldbPendingChildren()) {
        *err = "hybrid-snapshot cannot change while a save, rewrite, module fork or debugger child is running";
        return 0;
    }
    if (snapArenaAvailable()) {
        /* On: children stop mapping the arena; off: they must map it. Values
         * placed so far stay where they are, the switch itself is O(1). */
        if (!snapArenaSetForkable(!server.hybrid_snapshot)) {
            *err = "SNAP_NOFORK arena: madvise failed, see the server log";
            return 0;
        }
        zmalloc_nofork_set_active(server.hybrid_snapshot);
        snap_arena_fallback = 0;
    }
    serverLog(LL_NOTICE, "Hybrid snapshot switched %s at runtime%s", server.hybrid_snapshot ? "on" : "off",
              snapArenaAvailable() ? "" : " (no SNAP_NOFORK arena on this platform)");
    return 1;
}


static size_t snapArenaVaReserved(void) {
#ifdef HAVE_NOFORK_ARENA
    size_t reserved = 0;
    if (snapArenaAvailable()) zmalloc_nofork_va_stats(&reserved, NULL);
    return reserved;
#else
    return 0;
#endif
}

static size_t snapArenaVaUsed(void) {
#ifdef HAVE_NOFORK_ARENA
    size_t used = 0;
    if (snapArenaAvailable()) zmalloc_nofork_va_stats(NULL, &used);
    return used;
#else
    return 0;
#endif
}


/* ============================================================================
 * Classification and placement
 * ============================================================================ */

/* 1 if a value with this type/encoding is a single allocation that MAY be
 * SNAP_NOFORK (strings; listpack list/set/zset/hash; intset; listpack-ex).
 * Multi-allocation encodings (hashtable, skiplist, quicklist, stream, module,
 * arrays) are always SNAP_FORK. */
static int snapEncodingIsNoforkClass(int type, int encoding) {
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

/* Placement policy: SNAP_NOFORK iff the encoding qualifies and the value's
 * allocation is at most save-small-max-bytes (INT/EMBSTR values have no
 * separate allocation and are always SNAP_NOFORK). */
static int snapClassify(int type, int encoding, size_t alloc_bytes) {
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


/* Actual class of a stored key, decided by where its value memory IS: a key is
 * SNAP_NOFORK when its value has no separate allocation or when its single
 * allocation (for listpack-ex: both the struct and its listpack) lies in the
 * SNAP_NOFORK arena; otherwise SNAP_FORK. The background scan serializes
 * SNAP_NOFORK keys itself and ships SNAP_FORK keys to the fork child, so this
 * is correct regardless of policy drift. */
static int snapKeyClass(const robj *o) {
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
    /* The arena is a range: the value pointer is as good as its allocation
     * start, and is never dereferenced (snapOriginalSharesValue relies on it). */
    if (o->ptr == NULL || !zmalloc_is_nofork(o->ptr)) return SNAP_FORK;
    return SNAP_NOFORK;
}

/* Copy an sds into the requested arena. */
static sds snapSdsCopy(sds s, int to_nofork) {
    size_t len = sdslen(s);
    char type = sdsReqType(len);
    size_t need = sdsReqSize(len, type);
    size_t usable = 0;
    char *buf = to_nofork ? zmalloc_nofork_usable(need, &usable) : zmalloc_usable(need, &usable);
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
    void *n = to_nofork ? zmalloc_nofork(bytes) : zmalloc(bytes);
    memcpy(n, p, bytes);
    return n;
}

/* Same, freeing the original. */
static void *snapBlobMove(void *p, size_t bytes, int to_nofork) {
    void *n = snapBlobCopy(p, bytes, to_nofork);
    zfree(p); /* range-aware */
    return n;
}

/* Apply the placement policy to a value: move a single-allocation value into
 * the arena when snapClassify() says SNAP_NOFORK and it is outside, or out of
 * the arena when it says SNAP_FORK and it is inside. Called (through the
 * inline snapPlaceValue) at every keyspace insertion (kvobjSet, dbSetValue),
 * after in-place growth of a string and after an encoding conversion produced
 * a single-allocation value. Only the value moves; the header (and thus every
 * pointer to it) stays. */
void snapPlaceValueArena(robj *o) {
    if (!snapEncodingIsNoforkClass(o->type, o->encoding)) return;
    /* nothing to do if inline. kvobj already in no_fork arena */
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
    char *nalloc = zmalloc_nofork(size);
    memcpy(nalloc, alloc, size);
    kvobj *kv = (kvobj *)(nalloc + ((char *)orig - alloc));
    if (kv->type == OBJ_STRING && kv->encoding == OBJ_ENCODING_EMBSTR)
        kv->ptr = (char *)kv->ptr + (nalloc - alloc);
    kv->refcount = 1;
    return kv;
}


/* Overlay callbacks for the server keyspace (privdata = the store's
 * snapGuardPriv). The clone copies the whole kvobj allocation into a fresh
 * arena block; SNAP_NOFORK values (single allocation in the arena) are
 * deep-copied, everything else is shared with the original. db->expires and
 * db->subexpires are pointed at the clone. snapOverlayFreeOriginal releases a
 * fork-instant original after the drain. */
static void *snapOverlayCloneKey(void *privdata, int didx, void *orig_) {
    /* The db owning the store right now (NULL once FLUSHDB detached it: no
     * expires/subexpires to keep in step, and no writes reach it anyway). */
    redisDb *db = snap_save ? snapStoreOwnerDb(snap_save, ((struct snapGuardPriv *)privdata)->s) : NULL;
    kvobj *orig = orig_;
    kvobj *kv = snapKvobjCloneHeader(orig);

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
    int shared = snapOriginalSharesValue(orig);
    int nofork = !shared;
    if (!snapValueIsInline(orig->type, orig->encoding)) {
        switch (orig->encoding) {
        case OBJ_ENCODING_RAW:
            if (nofork) kv->ptr = snapSdsCopy(orig->ptr, to_arena);
            else shared = 1;
            break;
        case OBJ_ENCODING_LISTPACK:
            if (nofork) kv->ptr = snapBlobCopy(orig->ptr, lpBytes(orig->ptr), to_arena);
            else shared = 1;
            break;
        case OBJ_ENCODING_INTSET:
            if (nofork) kv->ptr = snapBlobCopy(orig->ptr, intsetBlobLen(orig->ptr), to_arena);
            else shared = 1;
            break;
        case OBJ_ENCODING_LISTPACK_EX: {
            /* Private copy (struct + listpack) with a fresh ExpireMeta. */
            listpackEx *o = orig->ptr;
            if (nofork) {
                listpackEx *n = zcalloc_nofork(sizeof(*n));
                n->meta.trash = 1;
                n->lp = snapBlobCopy(o->lp, lpBytes(o->lp), zmalloc_is_nofork(o->lp));
                kv->ptr = n;
            } else {
                shared = 1;
            }
            break;
        }
        default:
            /* Multi-allocation value (hashtable, skiplist, quicklist, stream,
             * module, array): SNAP_FORK, the child has its own copy. */
            shared = 1;
            break;
        }
    }
    serverAssert(shared == snapOriginalSharesValue(orig));

    snapSaveRetainedAdd(orig, shared);

    /* The clone takes over the original's per-slot accounting: later in-place
     * updates and the delete of the clone are counted against the slot, the
     * original is released unaccounted when the overlay is drained. */
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

/* Does the clone of 'orig' take over its value (1) or copy it (0)? The one
 * definition, used when the clone is made and again when the original is
 * released, so nothing is remembered between the two: it depends on the
 * original's header, which is never written, and on the numeric address of its
 * value, which is never dereferenced (a shared value may be gone by then).
 *   - a value inside the header allocation is copied with it;
 *   - a single-allocation value (string, listpack, intset, listpack with
 *     field TTLs) is copied when the scan thread reads it, i.e. when it is in
 *     the arena (the class is the address, see snapKeyClass), and without the
 *     arena always: the thread would read a shared value the clone then
 *     reallocates;
 *   - a multi-allocation value is the fork child's and is taken over. */
static int snapOriginalSharesValue(const kvobj *orig) {
    if (snapValueIsInline(orig->type, orig->encoding)) return 0;
    switch (orig->encoding) {
    case OBJ_ENCODING_RAW:
    case OBJ_ENCODING_LISTPACK:
    case OBJ_ENCODING_INTSET:
    case OBJ_ENCODING_LISTPACK_EX:
        if (!snapEncodingIsNoforkClass(orig->type, orig->encoding)) return 1;
        return snapArenaAvailable() ? !zmalloc_is_nofork(orig->ptr) : 0;
    default:
        return 1;
    }
}

/* A key deleted while its bucket is still owed keeps its fork-instant original
 * for the scan, in its overlayed bucket, until the bucket is written back;
 * when the clone took over its value, only the header is released.
 * Released through snapOverlayFreeOriginal. */
static void snapOverlayFreeOriginal(void *privdata, int didx, void *orig_) {
    UNUSED(privdata);
    UNUSED(didx);
    kvobj *orig = orig_;
    int shared = snapOriginalSharesValue(orig);
    snapSaveRetainedSub(orig, shared);
    /* The clone carries the key metadata (no keyMetaOnFree here) and, when
     * shared, owns the value. */
    if (!shared) kvobjFreeValueOnly(orig);
    zfree(kvobjGetAllocPtr(orig));
}

/* Move a single-allocation value out of the arena if it is there. Must be
 * called before such a buffer is ADOPTED by a multi-allocation structure
 * (e.g. a listpack becoming the first quicklist node): every piece of a
 * SNAP_FORK value has to be readable by the fork child. */
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

/* Reply with up to 'limit' keys whose value placement disagrees with its
 * class: "key type encoding drift" per entry (test aid, DEBUG SNAP-DRIFT). */
static void snapPlacementDriftReply(client *c, redisDb *db, int limit) {
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

static void snapPlacementScan(redisDb *db, snapPlacementStats *st) {
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
 * It is generic: nothing here knows about the save object (snapSave); the
 * save layer plugs in through the dispatch, slot-done, window-done and
 * store-done callbacks.
 *
 * Reading contract: the thread reads dicts in a fixed order (store, dict,
 * layout, table 0, table 1, bucket: one integer position) and stays below hi,
 * which only the main thread moves; it announces each batch in cur before
 * reading it and publishes lo behind it. The main thread never writes a chain
 * at a position in [lo, hi) (such writes go to a duplicate of the bucket
 * through the kvstore overlay) and waits for no one; a write at or beyond hi
 * serializes the bucket first (emit-ahead, marked in the per-save bitmap so
 * the thread skips it) and then goes in place; a write below lo is a plain
 * write. A dict's layout is paused before hi may pass it; a dict beyond hi
 * may grow (its bitmap follows the new table) but not shrink. The thread
 * reads the chains with atomic loads (dictWalkBucketRange) and never writes
 * to a dict.
 *
 * Publication: slot states are atomics written with release semantics by the
 * thread (SCANNING, SCANNED) and by the main thread (PENDING, DONE), and read
 * with acquire semantics. Completed slots are handed to the main thread over
 * a single-producer/single-consumer ring (no locks, no allocation on the
 * thread) plus an eventNotifier so the event loop wakes up; the main thread
 * completes a slot by writing back its overlayed buckets and releasing its state.
 */


static __thread int snap_on_scan_thread = 0;
static __thread int snap_scan_maxmemory_policy = 0;
static __thread int snap_scan_rdb_compression = 0;
static __thread int snap_main_emitting = 0;   /* main thread: inside an emit-ahead serialization */

/* The RDB serializer's configuration, read through one call (rdb.c). On the
 * main thread these are the live server values; on the scan thread the copies
 * frozen at save start, since the main thread may change the live ones while
 * the thread runs, and the LRU clock is computed by the thread itself
 * (LRU_CLOCK reads server.hz, which dynamic-hz adjusts on the main thread).
 * The key-save delay applies only in classic contexts: the scan thread applies
 * its own atomic copy and the main thread's emit-ahead must not slow the
 * command that triggered it. */
snapRdbCfg snapRdbCfgGet(void) {
    snapRdbCfg c;
    c.on_scan = snap_on_scan_thread;
    c.compression = c.on_scan ? snap_scan_rdb_compression : server.rdb_compression;
    c.maxmemory_policy = c.on_scan ? snap_scan_maxmemory_policy : server.maxmemory_policy;
    c.apply_key_save_delay = !c.on_scan && !snap_main_emitting;
    return c;
}

/* ---- Scan positions, the credit window and write sections (see snapshot.h) ---- */

static unsigned long snap_window_buckets = SNAP_WINDOW_BUCKETS_DEFAULT;
static void snapSetWindowBuckets(unsigned long n) { snap_window_buckets = n ? n : 1; }
static unsigned long snapGetWindowBuckets(void) { return snap_window_buckets; }

static inline dict *snapTableDict(snapScanCtx *ctx, int s, int didx);
static inline uint8_t snapDictStateOf(snapScanCtx *ctx, dict *d);

/* The scan order as integers. A position packs (ord, phase, idx): ord is the
 * dict's ordinal (its store's base plus didx, the real slot), phase 0 the
 * dict's layout (the thread reads table sizes and rehashidx there), 1 and 2
 * its tables, idx the bucket. The thread visits stores, dicts, phases and
 * buckets in increasing order, so it only ever moves up; dicts it never
 * visits (empty at the freeze, outside a slot filter) are gaps nobody
 * compares against. */
#define SNAP_POS_IDX_BITS 40
#define SNAP_POS_ORD_BITS 22
#define SNAP_POS(ord, phase, idx) (((uint64_t)(ord) << (SNAP_POS_IDX_BITS + 2)) | ((uint64_t)(phase) << SNAP_POS_IDX_BITS) | (uint64_t)(idx))
#define SNAP_POS_ORD(p) ((p) >> (SNAP_POS_IDX_BITS + 2))
#define SNAP_POS_PHASE(p) ((int)(((p) >> SNAP_POS_IDX_BITS) & 3))
#define SNAP_POS_IDX(p) ((unsigned long)((p) & ((1ULL << SNAP_POS_IDX_BITS) - 1)))
#define SNAP_POS_INF UINT64_MAX            /* hi: no limit */
#define SNAP_SCAN_BATCH 128                /* buckets the thread reads per announcement */
#define SNAP_SCAN_LOOKAHEAD (SNAP_SCAN_PREFETCH > SNAP_SCAN_PREFETCH2 ? SNAP_SCAN_PREFETCH : SNAP_SCAN_PREFETCH2)   /* announced past the batch for the prefetches */
#define SNAP_ADVANCE_DICTS 64              /* dicts one grant may open */
#define SNAP_OV_SLOTS_SOFT 16              /* overlay windows in use past which a section stops granting */

/* The credit window (doc: "Scan window"). The thread announces in cur the
 * last position it may read next, then reads hi and stays below it; it
 * publishes lo once every position below lo is read. The main thread alone
 * writes hi: unlimited outside write sections, capped when a section opens
 * (snapWindowCap) and raised inside one as the thread advances
 * (snapWindowRefill). cur/hi form a store-load pair on each side (seq_cst):
 * after the cap, the thread is at most on cur, and any later position it
 * reaches sees the cap. */
/* The thread's words on a cache line of their own: it stores lo after every
 * bucket, and the main thread reads snap_funnel_seq after every command. */
static struct {
    uint64_t cur;                          /* thread: seq_cst stores */
    uint64_t lo;                           /* thread: release stores (seq_cst at the end) */
} __attribute__((aligned(CACHE_LINE_SIZE))) snap_pos_thread;
#define snap_cur (snap_pos_thread.cur)
#define snap_lo (snap_pos_thread.lo)
static uint64_t snap_hi __attribute__((aligned(CACHE_LINE_SIZE))) = SNAP_POS_INF;   /* main thread: seq_cst at the cap, release otherwise */
static snapScanCtx *snap_live = NULL;      /* main thread: the scan whose thread runs (the cap and grants apply) */
redisAtomic uint64_t snap_funnel_seq = 0;

static inline uint64_t snapPosLoLoad(void) { return __atomic_load_n(&snap_lo, __ATOMIC_ACQUIRE); }

/* A dict's ordinal and back. */
static inline uint64_t snapOrd(snapScanCtx *ctx, int s, int didx) { return (uint64_t)ctx->ord_base[s] + (uint64_t)didx; }
static void snapOrdDecode(snapScanCtx *ctx, uint64_t ord, int *s, int *didx) {
    int k = 0;
    while (k + 1 < ctx->num_stores && ord >= (uint64_t)ctx->ord_base[k + 1]) k++;
    *s = k;
    *didx = (int)(ord - (uint64_t)ctx->ord_base[k]);
}

static void snapScanPauseLayout(snapScanCtx *ctx, dict *d);

/* Main thread: the position 'budget' readable buckets after 'from' in the
 * scan order, and in *mid the one half way. Only buckets the thread will read
 * count (table 0 from rehashidx while rehashing, table 1 only then); dicts
 * the thread is done with, or that the save does not hold, are skipped. Every
 * dict the walk enters is paused first: positions name buckets by (table,
 * idx), so a dict's layout must hold still before hi may pass it. Stops
 * before the layout of the SNAP_ADVANCE_DICTS-th dict. */
static uint64_t snapWindowAdvance(snapScanCtx *ctx, uint64_t from, unsigned long budget, uint64_t *mid) {
    unsigned long half = budget / 2, used = 0;
    *mid = SNAP_POS_INF;
    if (from >= ctx->pos_end) return ctx->pos_end;
    int s, didx, dicts = 0;
    snapOrdDecode(ctx, SNAP_POS_ORD(from), &s, &didx);
    int phase = SNAP_POS_PHASE(from);
    unsigned long idx = SNAP_POS_IDX(from);
    for (; s < ctx->num_stores; s++, didx = 0) {
        for (; didx < ctx->num_dicts[s]; didx++, phase = 0, idx = 0) {
            dict *d = ctx->frozen[s] ? snapTableDict(ctx, s, didx) : NULL;
            if (!d) continue;
            uint8_t st = snapDictStateOf(ctx, d);
            if (st == SNAP_SLOT_SCANNED || st == SNAP_SLOT_DONE) continue;
            uint64_t ord = snapOrd(ctx, s, didx);
            if (++dicts > SNAP_ADVANCE_DICTS) {
                if (*mid == SNAP_POS_INF) *mid = SNAP_POS(ord, 0, 0);
                return SNAP_POS(ord, 0, 0);
            }
            snapScanPauseLayout(ctx, d);
            if (phase == 0) phase = 1, idx = 0;
            int rehashing = dictIsRehashing(d);
            for (; phase <= (rehashing ? 2 : 1); phase++, idx = 0) {
                unsigned long size = DICTHT_SIZE(d->ht_size_exp[phase - 1]);
                if (phase == 1 && rehashing && idx < (unsigned long)d->rehashidx) idx = (unsigned long)d->rehashidx;
                if (idx >= size) continue;
                unsigned long avail = size - idx;
                if (*mid == SNAP_POS_INF && used + avail >= half) *mid = SNAP_POS(ord, phase, idx + (half - used));
                if (used + avail >= budget) return SNAP_POS(ord, phase, idx + (budget - used));
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
static void snapWindowCap(snapScanCtx *ctx) {
    uint64_t lo = __atomic_load_n(&snap_lo, __ATOMIC_SEQ_CST);
    uint64_t mid, h = snapWindowAdvance(ctx, lo, ctx->window_buckets, &mid);
    __atomic_store_n(&snap_hi, h, __ATOMIC_SEQ_CST);
    uint64_t c = __atomic_load_n(&snap_cur, __ATOMIC_SEQ_CST);
    if (c >= h && c < ctx->pos_end) {
        int s, didx;
        snapOrdDecode(ctx, SNAP_POS_ORD(c), &s, &didx);
        dict *d = ctx->frozen[s] ? snapTableDict(ctx, s, didx) : NULL;
        if (d && snapDictStateOf(ctx, d) == SNAP_SLOT_SCANNING) snapScanPauseLayout(ctx, d);
        h = c + 1;
        __atomic_store_n(&snap_hi, h, __ATOMIC_RELEASE);
        mid = h;
    }
    ctx->hi = h;
    ctx->mid = mid;
}

/* Main thread, inside a section: once the thread is half way through its
 * grant, grant the next W buckets (pausing the dicts they open). Not while
 * many overlay windows are in use: the section's drain has to come first. */
static void snapWindowRefill(snapScanCtx *ctx) {
    if (snapPosLoLoad() < ctx->mid) return;
    if (kvstoreOverlaySlotsInUse() >= SNAP_OV_SLOTS_SOFT) return;
    uint64_t mid, h = snapWindowAdvance(ctx, snapPosLoLoad(), ctx->window_buckets, &mid);
    if (h > ctx->hi) {
        ctx->hi = h;
        __atomic_store_n(&snap_hi, h, __ATOMIC_RELEASE);
        ctx->grants++;
    }
    ctx->mid = mid;
}

/* Scan thread: may it read positions [p, last]? Announces 'last' in cur,
 * then waits while p is at or above hi. Returns the end (exclusive) it may
 * read to, at most last + 1, or 0 when the scan was aborted meanwhile. */
static uint64_t snapScanReach(snapScanCtx *ctx, uint64_t p, uint64_t last) {
    __atomic_store_n(&snap_cur, last, __ATOMIC_SEQ_CST);
    uint64_t hi = __atomic_load_n(&snap_hi, __ATOMIC_SEQ_CST);
    if (p >= hi) {
        monotime t;
        elapsedStart(&t);
        atomicIncr(ctx->stalls, 1);
        int spins = 0;
        while (p >= hi) {
            if (snapScanAborted(ctx)) return 0;
            if (++spins < 200) sched_yield();
            else usleep(20);
            hi = __atomic_load_n(&snap_hi, __ATOMIC_SEQ_CST);
        }
        atomicIncr(ctx->stall_us, (long long)elapsedUs(t));
    }
    return hi > last ? last + 1 : hi;
}

static inline void snapScanSetLo(uint64_t lo) { __atomic_store_n(&snap_lo, lo, __ATOMIC_RELEASE); }

/* Scan thread, at the end: wait until no write section is open. A section
 * opened after the seq_cst store of lo = END sees every bucket behind it, so
 * nothing more is emitted; one open before may still be emitting. This is
 * the only time the thread waits for a section. */
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
         * be waiting to join us: an aborted scan thread reads nothing more. */
        if (snapScanAborted(ctx)) return;
        if (++spins < 200) sched_yield();
        else usleep(20);
    }
}

/* Write sections. The main thread works on frozen dicts inside a "write
 * section": opened by the first guard or funnel hook of a command (reads
 * included), closed when the outermost command returns, at beforeSleep and
 * when a module thread releases the GIL (snapFunnelClose). Opening caps the
 * thread (snapWindowCap); every later hook of the section may raise the cap
 * (snapWindowRefill); closing writes back the overlayed buckets the thread
 * has passed and lifts the cap. Outside sections the thread runs free: the
 * main thread touches no frozen dict there. */
static void snapFunnelOpen(void) {
    uint64_t v;
    atomicGet(snap_funnel_seq, v);          /* single writer: relaxed read of our own value */
    if (v & 1) {
        if (snap_live) snapWindowRefill(snap_live);
        return;
    }
    atomicSetWithSync(snap_funnel_seq, v + 1);   /* seq_cst: pairs with snapWalkerQuiesce */
    if (snap_live) snapWindowCap(snap_live);
}

void snapFunnelCloseSlow(void) {
    uint64_t v;
    atomicGet(snap_funnel_seq, v);
    if (v & 1) {
        /* At a point where no caller holds a link: write back what the
         * thread has passed, then let it run free. */
        snapSaveDrainSideTables();
        if (snap_live) {
            snap_live->hi = SNAP_POS_INF;
            __atomic_store_n(&snap_hi, SNAP_POS_INF, __ATOMIC_RELEASE);
        }
        atomicSetWithSync(snap_funnel_seq, v + 1);
    }
}

/* The scan thread's waits for hi and the grants raised inside sections, of
 * the running save, else of the last one (kept at its release). */
static struct { long long stalls, stall_us, grants; } snap_last_window;
static void snapScanWindowStats(snapScanCtx *ctx, long long *stalls, long long *stall_us, long long *grants) {
    if (!ctx) {
        *stalls = snap_last_window.stalls;
        *stall_us = snap_last_window.stall_us;
        *grants = snap_last_window.grants;
        return;
    }
    atomicGet(ctx->stalls, *stalls);
    atomicGet(ctx->stall_us, *stall_us);
    *grants = ctx->grants;
}

/* Main thread: the scan thread is about to start (positions from the first
 * dict, no limit), or failed to (NULL). */
static void snapWindowReset(snapScanCtx *live) {
    __atomic_store_n(&snap_cur, 0, __ATOMIC_SEQ_CST);
    __atomic_store_n(&snap_lo, 0, __ATOMIC_SEQ_CST);
    __atomic_store_n(&snap_hi, SNAP_POS_INF, __ATOMIC_SEQ_CST);
    if (live) {
        live->hi = SNAP_POS_INF;
        live->mid = 0;
    }
    snap_live = live;
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
 * rest of the state (bitmaps, overlay, pause) is attached on first touch
 * and freed when the dict completes. */

static inline kvstoreDictMetaBase *snapMeta(dict *d) { return (kvstoreDictMetaBase *)dictMetadata(d); }
static inline uint64_t snapWordLoad(dict *d) { return __atomic_load_n(&snapMeta(d)->ext_word, __ATOMIC_ACQUIRE); }
static inline void snapWordStore(dict *d, uint64_t w) { __atomic_store_n(&snapMeta(d)->ext_word, w, __ATOMIC_RELEASE); }
/* The attached state, either thread (the main thread publishes it with release). */
static inline snapDictState *snapStateOf(dict *d) { return __atomic_load_n(&snapMeta(d)->ext, __ATOMIC_ACQUIRE); }

/* The save's table of fork-instant dicts (filled at freeze, one reference
 * each). Entries are cleared at completion, on the main thread, after the
 * thread published SCANNED for the slot or was joined. */
static inline dict *snapTableDict(snapScanCtx *ctx, int s, int didx) {
    return ctx->dicts[s] ? __atomic_load_n(&ctx->dicts[s][didx], __ATOMIC_RELAXED) : NULL;
}

/* Either thread: the state of a table dict (its word carries this save's generation). */
static inline uint8_t snapDictStateOf(snapScanCtx *ctx, dict *d) {
    uint64_t w = snapWordLoad(d);
    serverAssert(SNAP_WORD_GEN(w) == ctx->gen);
    return SNAP_WORD_STATE(w);
}

/* Main thread: the state of the live dict d of slot (s, didx): DONE for a
 * store that is not scanned (empty at the fork instant, or completed), after
 * the end, and for a dict that is not the save's. */
static uint8_t snapScanDictStateMain(snapScanCtx *ctx, int s, int didx, dict *d) {
    if (ctx->ended || !ctx->frozen[s]) return SNAP_SLOT_DONE;
    if (d != snapTableDict(ctx, s, didx)) return SNAP_SLOT_DONE;
    return snapDictStateOf(ctx, d);
}

/* Scan thread: claim dict d for the walk (PENDING -> SCANNING; the main
 * thread changes no word before SCANNED). */
static void snapScanClaim(snapScanCtx *ctx, dict *d) {
    serverAssert(snapDictStateOf(ctx, d) == SNAP_SLOT_PENDING);
    snapWordStore(d, SNAP_WORD(ctx->gen, SNAP_SLOT_SCANNING));
}

/* ---- Per-save bucket bitmaps (in snapDictState) ---- */

static inline uint8_t *snapScanBitmap(snapScanCtx *ctx, int s, int didx, int table) {
    /* Attached on demand by the main thread while the thread may be
     * reading: acquire pairs with the release stores in snapScanBitmapAlloc. */
    dict *d = snapTableDict(ctx, s, didx);
    snapDictState *sn = d ? snapStateOf(d) : NULL;
    if (!sn) return NULL;
    return __atomic_load_n(&sn->bits[table], __ATOMIC_ACQUIRE);
}

/* Per-save bucket bitmaps: one bit per bucket of every table of a dict the
 * save touched, allocated when the dict is attached (snapDictState in the
 * kvstore extension slot), freed when the slot completes. A set bit means the
 * bucket's chain was already emitted into the snapshot by the main thread
 * (at or beyond hi); the scan thread skips such buckets. Mark only buckets at
 * or beyond hi, inside a write section, before hi is raised over them. */
static void snapScanMarkBucketEmitted(snapScanCtx *ctx, int s, int didx, int table, unsigned long idx) {
    uint8_t *bm = snapScanBitmap(ctx, s, didx, table);
    serverAssert(bm != NULL);
    __atomic_fetch_or(&bm[idx >> 3], (uint8_t)(1u << (idx & 7)), __ATOMIC_RELAXED);
}

static int snapScanBucketEmitted(snapScanCtx *ctx, int s, int didx, int table, unsigned long idx) {
    uint8_t *bm = snapScanBitmap(ctx, s, didx, table);
    if (!bm) return 0;
    return (__atomic_load_n(&bm[idx >> 3], __ATOMIC_RELAXED) >> (idx & 7)) & 1;
}

/* Bucket bitmaps of this save (they live in the dicts' metadata; accounted
 * here, main thread). */
/* The scan's own tables: the per-store dict tables and the completion ring. */
static size_t snapScanTableBytes(snapScanCtx *ctx) {
    size_t n = ctx->slots_total ? ctx->slots_total : 1;
    size_t bytes = n * sizeof(uint64_t);   /* ring */
    for (int s = 0; s < ctx->num_stores; s++)
        if (ctx->dicts[s]) bytes += (size_t)ctx->num_dicts[s] * sizeof(dict *);
    return bytes;
}

static size_t snapScanBitmapBytes(snapScanCtx *ctx) {
    return ctx ? ctx->bitmap_bytes : 0;
}

static void snapScanBitmapAlloc(snapScanCtx *ctx, snapDictState *sn, dict *d) {
    for (int t = 0; t < 2; t++) {
        unsigned long size = DICTHT_SIZE(d->ht_size_exp[t]);
        if (size == 0) continue;
        size_t bytes = (size + 7) / 8;
        sn->bytes[t] = bytes;
        __atomic_store_n(&sn->bits[t], zcalloc(bytes), __ATOMIC_RELEASE);
        ctx->bitmap_bytes += bytes;
    }
}

/* Attach the keyspace overlay (kvstore.h) to the scan: a dict gets an
 * overlay the first time the guard attaches it; its overlayed buckets are written back on
 * the main thread once the thread has passed them and it is released when its slot
 * completes. Only the overlay callbacks of 'hooks' (clone, free_orig, enter,
 * prepare) are taken; the guard and layout hooks are the scan's
 * own. Every hook of store s receives the store's snapGuardPriv, whose 'user'
 * field is 'store_privdata[s]' (NULL array: NULL). Must be called before
 * snapScanStart(). */
/* Before start: restrict the scan to the slots whose bit is set (cluster
 * mode; the bitmap must outlive the scan). Dicts outside the set are never
 * frozen: the guard treats them as not the save's, so their writes are plain
 * writes, and the thread's pass never visits them. */
static void snapScanSetSlotFilter(snapScanCtx *ctx, const uint8_t *bitmap) {
    serverAssert(!ctx->started);
    ctx->slot_filter = bitmap;
}

static void snapScanSetOverlay(snapScanCtx *ctx, const kvstoreSnapHooks *hooks, void **store_privdata)
{
    serverAssert(!ctx->started);
    ctx->hooks.clone = hooks->clone;
    ctx->hooks.free_orig = hooks->free_orig;
    ctx->hooks.enter = hooks->enter;
    ctx->hooks.prepare = hooks->prepare;
    ctx->has_ov = 1;
    ctx->ov_priv = store_privdata;
}

static uint64_t snap_scan_gen = 0;   /* generation of the last scan context created */
static long snap_stat_retained_dicts = 0;   /* table dicts freed by a save, across saves (INFO snap_retained_dicts) */

/* Create a scan over 'num_stores' kvstores (normally one per db: db->keys).
 * Nothing is frozen or started until snapScanStart(). 'dispatch' runs on the
 * scan thread once for every fork-instant key of a slot and must be strictly
 * read-only on the keyspace (it may read the kvobj and, for SNAP_NOFORK
 * values, the value, never write to any dict or object). 'slot_done' runs on
 * the main thread when a slot is complete, BEFORE its dict is unfrozen and
 * its state becomes DONE ('aborted' = 1 when the slot was not fully scanned);
 * the dict's overlayed buckets are written back and its save state released right after. */
static snapScanCtx *snapScanCreate(kvstore **stores, int num_stores,
                            snapScanDispatchFn *dispatch,
                            snapScanSlotDoneFn *slot_done, void *privdata)
{
    snapScanCtx *ctx = zcalloc(sizeof(*ctx));
    ctx->stores = zmalloc(sizeof(kvstore*) * num_stores);
    memcpy(ctx->stores, stores, sizeof(kvstore*) * num_stores);
    ctx->num_stores = num_stores;
    ctx->num_dicts = zmalloc(sizeof(int) * num_stores);
    ctx->ord_base = zmalloc(sizeof(int) * num_stores);
    ctx->frozen = zcalloc(sizeof(int) * num_stores);
    ctx->dicts = zcalloc(sizeof(dict **) * num_stores);
    ctx->nrefs = zcalloc(sizeof(long) * num_stores);
    ctx->guards = zmalloc(sizeof(struct snapGuardPriv) * num_stores);
    long long ords = 0;
    for (int s = 0; s < num_stores; s++) {
        ctx->num_dicts[s] = kvstoreNumDicts(stores[s]);
        ctx->ord_base[s] = (int)ords;
        ords += ctx->num_dicts[s];
        ctx->guards[s].ctx = ctx;
        ctx->guards[s].s = s;
        ctx->guards[s].user = NULL;
    }
    serverAssert(ords < (1LL << SNAP_POS_ORD_BITS));   /* snapScanFits() */
    ctx->pos_end = SNAP_POS(ords, 0, 0);
    ctx->hi = 0;   /* no grant until the thread starts: every bucket is ahead */
    ctx->window_buckets = snap_window_buckets;
    ctx->ov_block = 1;
    while (ctx->ov_block < ctx->window_buckets) ctx->ov_block <<= 1;
    ctx->gen = ++snap_scan_gen;
    atomicSet(ctx->cur_slot, -1);
    ctx->dispatch = dispatch;
    ctx->slot_done = slot_done;
    ctx->privdata = privdata;
    ctx->en = createEventNotifier();
    return ctx;
}

/* Main thread: the dict's save state (bucket bitmaps), allocated on first use. */
static snapDictState *snapScanState(snapScanCtx *ctx, dict *d) {
    serverAssert(snapDictStateOf(ctx, d) != SNAP_SLOT_DONE);
    snapDictState *sn = snapMeta(d)->ext;   /* main thread: the only writer of the pointer */
    if (!sn) {
        sn = zcalloc(sizeof(*sn));
        __atomic_store_n(&snapMeta(d)->ext, sn, __ATOMIC_RELEASE);
        snapScanBitmapAlloc(ctx, sn, d);
    }
    return sn;
}

/* Main thread: hold the dict's layout (table sizes, rehashidx) until its slot
 * completes. Done before the thread may read the layout (snapWindowAdvance,
 * snapWindowCap) and on any access while it is on the dict (guard). Works for
 * a dict that left its store: no overlay is attached here. */
static void snapScanPauseLayout(snapScanCtx *ctx, dict *d) {
    snapDictState *sn = snapScanState(ctx, d);
    if (sn->paused) return;
    sn->paused = 1;
    dictPauseRehashing(d);
    dictPauseAutoResize(d);
}

static inline int snapDictPaused(dict *d) {
    snapDictState *sn = snapMeta(d)->ext;
    return sn && sn->paused;
}

/* Main thread, inside a write section: give dict (s, didx) what the scan
 * needs from it, on demand: allocate its bucket bitmaps and attach the
 * overlay; pause its layout when the thread is on it. Called by the guard on
 * the first structural write to a dict the thread has not finished, or on
 * any access while the thread is on the dict. A dict attached by a write but
 * not reached yet (beyond hi, not paused) keeps growing: the rehash hooks
 * replicate its bitmap into the new table (bucket i of a table of n becomes
 * buckets i + j*n of the table of k*n), and a shrink is refused for it
 * (kvstore shrinkAllowed). Returns the overlay (NULL without one). */
static struct kvstoreDictOverlay *snapScanAttach(snapScanCtx *ctx, int s, int didx, dict *d, int pause) {
    snapScanState(ctx, d);
    if (ctx->has_ov && !snapMeta(d)->ov) kvstoreOverlayAttach(ctx->stores[s], didx);
    if (pause) snapScanPauseLayout(ctx, d);
    return snapMeta(d)->ov;
}

/* kvstore snapshot guard (kvstore.h): every access to a dict without an
 * overlay while the scan runs lands here. */
static struct kvstoreDictOverlay *snapOverlayGuard(void *privdata, kvstore *kvs, int didx, int write) {
    struct snapGuardPriv *g = privdata;
    snapScanCtx *ctx = g->ctx;
    int s = g->s;
    dict *d = kvstoreGetDict(kvs, didx);
    /* Not the save's dict (created or swapped into the slot since the fork
     * instant, or the save is done with the slot): nothing of the fork
     * instant in it, a plain access. */
    if (!d || d != snapTableDict(ctx, s, didx)) return NULL;
    if (snapDictStateOf(ctx, d) >= SNAP_SLOT_SCANNED) return NULL;   /* the thread is finished with it */
    /* Every access runs inside a section, reads included (a find may step a
     * rehash): opening one caps the thread, and the cap pauses any dict the
     * thread may be reading or may enter before the section ends. */
    snapFunnelOpen();
    /* The state after the cap: the thread may have claimed the dict while it
     * ran free. A write to a dict the thread has not reached attaches it
     * without pausing it. */
    uint8_t st = snapDictStateOf(ctx, d);
    if (st >= SNAP_SLOT_SCANNED) return NULL;
    int pause = (st == SNAP_SLOT_SCANNING);
    if (!write && !pause) return NULL;   /* a read of a dict the thread is not reading */
    return snapScanAttach(ctx, s, didx, d, pause);
}

/* The store-level snapshot hooks (kvstore.h): the guard above and the stage
 * B rehash and shrink hooks of attached dicts. */
static void snapGuardRehashStarted(void *privdata, int didx, dict *d) {
    struct snapGuardPriv *g = privdata;
    snapScanDictRehashStarted(g->ctx, g->s, didx, d);
}
static void snapGuardRehashCompleted(void *privdata, int didx, dict *d) {
    struct snapGuardPriv *g = privdata;
    snapScanDictRehashCompleted(g->ctx, g->s, didx, d);
}
static int snapGuardShrinkAllowed(void *privdata, int didx) {
    struct snapGuardPriv *g = privdata;
    dict *d = kvstoreGetDict(g->ctx->stores[g->s], didx);
    return !d || snapMeta(d)->ext == NULL;   /* a bitmap cannot follow a merge of buckets */
}

/* One pass over the store's dicts (a store empty right now holds nothing of
 * the fork instant and is never scanned nor guarded): every non-empty dict
 * is the save's from here on: one reference taken, its word
 * stamped PENDING, its pointer in the save's table. The thread walks that
 * table, never the live slot table, so the store may free, empty or move
 * any of these dicts meanwhile: it detaches them and the save releases them
 * when their slot completes. A dict empty now, or allocated later, is not in
 * the table: nothing of the fork instant is in it, the guard leaves it alone. */
static void snapScanFreezeStore(snapScanCtx *ctx, int s) {
    kvstore *kvs = ctx->stores[s];
    if (kvstoreSize(kvs) == 0) return;
    int n = ctx->num_dicts[s];
    ctx->dicts[s] = zcalloc(sizeof(dict *) * n);
    for (int didx = 0; didx < n; didx++) {
        if (ctx->slot_filter && !(ctx->slot_filter[didx >> 3] & (1u << (didx & 7)))) continue;   /* not ours: a plain dict throughout */
        dict *d = kvstoreGetDict(kvs, didx);
        if (!d || dictSize(d) == 0) continue;
        kvstoreDictRetain(kvs, didx);
        snapWordStore(d, SNAP_WORD(ctx->gen, SNAP_SLOT_PENDING));
        ctx->dicts[s][didx] = d;
        ctx->nrefs[s]++;
    }
    ctx->slots_total += ctx->nrefs[s];
    ctx->hooks.guard = snapOverlayGuard;
    ctx->hooks.rehash_started = snapGuardRehashStarted;
    ctx->hooks.rehash_completed = snapGuardRehashCompleted;
    ctx->hooks.shrink_allowed = snapGuardShrinkAllowed;
    ctx->guards[s].user = ctx->ov_priv ? ctx->ov_priv[s] : NULL;
    kvstoreSnapGuardSet(kvs, &ctx->hooks, &ctx->guards[s]);
    ctx->frozen[s] = 1;
}

/* The store's last dict completed: the guard comes off now, not at release,
 * because the caller of an abort (FLUSHALL, SWAPDB, DEBUG RELOAD) frees or
 * replaces the store right after, before snapScanRelease() runs. */
static void snapScanStoreDone(snapScanCtx *ctx, int s) {
    if (!ctx->frozen[s]) return;
    serverAssert(ctx->nrefs[s] == 0);
    kvstoreSnapGuardSet(ctx->stores[s], NULL, NULL);
    ctx->frozen[s] = 0;
    if (ctx->store_done) ctx->store_done(ctx->privdata, s);
}


/* Main thread, inside the dict operation that started an incremental
 * rehash of an attached, unpaused dict: the new table
 * has k times the buckets of the old one and the entries of old bucket i
 * land in i + j*n, so its bitmap is k copies of the old one. */
/* The dict's state if this save attached one (a leftover of another save is
 * not ours to touch; the main thread's access releases it). */
static snapDictState *snapScanOwnState(snapScanCtx *ctx, dict *d) {
    if (!ctx) return NULL;
    snapDictState *sn = snapMeta(d)->ext;
    if (!sn || SNAP_WORD_GEN(snapWordLoad(d)) != ctx->gen) return NULL;
    return sn;
}

/* An attached, unreached dict grew or finished rehashing: its bitmap follows
 * the table. */
static void snapScanDictRehashStarted(snapScanCtx *ctx, int s, int didx, dict *d) {
    UNUSED(didx);
    UNUSED(s);
    snapDictState *sn = snapScanOwnState(ctx, d);
    if (!sn) return;
    unsigned long size0 = DICTHT_SIZE(d->ht_size_exp[0]), size1 = DICTHT_SIZE(d->ht_size_exp[1]);
    if (sn->bits[1]) {   /* should not happen: a rehash started over an unfinished one */
        ctx->bitmap_bytes -= sn->bytes[1];
        zfree(sn->bits[1]);
        __atomic_store_n(&sn->bits[1], NULL, __ATOMIC_RELEASE);
        sn->bytes[1] = 0;
    }
    if (size1 == 0) return;
    size_t bytes1 = (size1 + 7) / 8;
    uint8_t *nb = zcalloc(bytes1);
    uint8_t *b0 = sn->bits[0];
    if (b0 && size0) {
        if (size1 >= size0 && (size1 % size0) == 0) {
            if (size0 % 8 == 0) {
                size_t bytes0 = size0 / 8;
                for (size_t off = 0; off < bytes1; off += bytes0) memcpy(nb + off, b0, bytes0);
            } else {
                for (unsigned long i = 0; i < size1; i++) {
                    unsigned long src = i & (size0 - 1);
                    if ((b0[src >> 3] >> (src & 7)) & 1) nb[i >> 3] |= (uint8_t)(1u << (i & 7));
                }
            }
        } else {
            /* A shrink is refused for attached dicts (kvstore shrinkAllowed);
             * a non-multiple growth cannot happen with power-of-two tables. */
            debugServerAssert(0);
        }
    }
    sn->bytes[1] = bytes1;
    __atomic_store_n(&sn->bits[1], nb, __ATOMIC_RELEASE);
    ctx->bitmap_bytes += bytes1;
    ctx->dicts_grown++;
}

/* The rehash completed: the new table is the table. */
static void snapScanDictRehashCompleted(snapScanCtx *ctx, int s, int didx, dict *d) {
    UNUSED(didx);
    UNUSED(s);
    snapDictState *sn = snapScanOwnState(ctx, d);
    if (!sn || !sn->bits[1]) return;   /* the dict was not attached when this rehash started: nothing to carry */
    if (sn->bits[0]) {
        ctx->bitmap_bytes -= sn->bytes[0];
        zfree(sn->bits[0]);
    }
    __atomic_store_n(&sn->bits[0], sn->bits[1], __ATOMIC_RELEASE);
    sn->bytes[0] = sn->bytes[1];
    __atomic_store_n(&sn->bits[1], NULL, __ATOMIC_RELEASE);
    sn->bytes[1] = 0;
}

static long snapScanDictsGrown(snapScanCtx *ctx) { return ctx ? ctx->dicts_grown : 0; }
static long snapScanRetainedDicts(snapScanCtx *ctx) { return ctx ? ctx->retained_dicts : 0; }

/* Chain-walk callback on the scan thread: decode the entry and dispatch. */
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

/* Scan thread: read one frozen dict, a batch of buckets at a time, each
 * batch announced and kept below hi (snapScanReach). Returns 1 if aborted. */
static int snapScanWalkDict(snapScanCtx *ctx, int s, int didx, dict *d, unsigned long *since_done) {
    struct snapScanEntryArgs a = { ctx, s, didx };
    uint64_t ord = snapOrd(ctx, s, didx);
    /* The layout: hi past it means the main thread paused the dict first
     * (snapWindowAdvance), and with no limit no section is open. */
    uint64_t lp = SNAP_POS(ord, 0, 0);
    if (!snapScanReach(ctx, lp, lp)) return 1;
    int rehashing = dictIsRehashing(d);   /* stable: rehashing is paused */
    snapScanSetLo(lp + 1);
    for (int t = 0; t < (rehashing ? 2 : 1); t++) {
        unsigned long size = DICTHT_SIZE(d->ht_size_exp[t]);
        serverAssert(size < (1UL << SNAP_POS_IDX_BITS));
        uint64_t base = SNAP_POS(ord, 1 + t, 0);
        /* Buckets of table 0 below rehashidx were moved to table 1 already. */
        unsigned long lo = (t == 0 && rehashing) ? (unsigned long)d->rehashidx : 0;
        int primed = 0;   /* the first buckets of the table are prefetched once, later ones by the lookahead */
        while (lo < size) {
            unsigned long hi = lo + SNAP_SCAN_BATCH;
            if (hi > size || hi < lo) hi = size;
            /* The batch is announced with its prefetch lookahead: the thread
             * touches no bucket head beyond cur, nor at or beyond hi (the main
             * thread may be writing those, emitted ahead and then in place). */
            unsigned long ahead = hi + SNAP_SCAN_LOOKAHEAD;
            if (ahead > size || ahead < hi) ahead = size;
            uint64_t end = snapScanReach(ctx, base + lo, base + ahead - 1);
            if (!end) return 1;
            unsigned long lim = (unsigned long)(end - base);   /* bucket heads below lim may be touched */
            if (hi > lim) hi = lim;
            /* The first touch of each key's header is a cache miss: have the
             * first entries of the next buckets on their way, then the values
             * of the ones after. */
            if (!primed) {
                for (unsigned long k = lo; k < lo + SNAP_SCAN_PREFETCH && k < lim; k++)
                    if (!snapScanBucketEmitted(ctx, s, didx, t, k)) dictWalkPrefetchBucket(d, t, k);
                primed = 1;
            }
            for (unsigned long i = lo; i < hi; i++) {
                if (i + SNAP_SCAN_PREFETCH < lim && !snapScanBucketEmitted(ctx, s, didx, t, i + SNAP_SCAN_PREFETCH))
                    dictWalkPrefetchBucket(d, t, i + SNAP_SCAN_PREFETCH);
#if SNAP_SCAN_PREFETCH2
                /* The header of that bucket is in cache by now: a raw string's
                 * value allocation is the next miss ('ptr' exists only for
                 * encodings that have a separate allocation). */
                if (i + SNAP_SCAN_PREFETCH2 < lim && !snapScanBucketEmitted(ctx, s, didx, t, i + SNAP_SCAN_PREFETCH2)) {
                    dictEntry *de2 = dictWalkBucketHead(d, t, i + SNAP_SCAN_PREFETCH2);
                    if (de2) {
                        kvobj *kv2 = dictGetKV(de2);
                        if (kv2->type == OBJ_STRING && kv2->encoding == OBJ_ENCODING_RAW) __builtin_prefetch(kv2->ptr);
                    }
                }
#endif
                if (snapScanBucketEmitted(ctx, s, didx, t, i)) {
                    atomicIncr(ctx->buckets_skipped, 1);
                } else {
                    dictWalkBucketRange(d, t, i, i + 1, snapScanEntryCb, &a);
                }
                snapScanSetLo(base + i + 1);
            }
            if (snapScanAborted(ctx)) return 1;
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

static void snapScanPublishScanned(snapScanCtx *ctx, int s, int didx, dict *d) {
    snapWordStore(d, SNAP_WORD(ctx->gen, SNAP_SLOT_SCANNED));
    atomicSet(ctx->cur_slot, -1);
    long wr;
    atomicGet(ctx->ring_wr, wr);
    ctx->ring[wr] = SNAP_SCAN_ENC(s, didx);
    atomicSetWithSync(ctx->ring_wr, wr + 1);   /* release: ring[wr] visible before wr+1 */
    triggerEventNotifier(ctx->en);
}

static void *snapScanThreadMain(void *arg) {
    snapScanCtx *ctx = arg;
    redis_set_thread_title("snap_scan");
    makeThreadKillable();
    snap_on_scan_thread = 1;
    if (ctx->thread_init) ctx->thread_init(ctx->privdata);

    unsigned long since_done = 0;   /* buckets read since the last window_done */
    for (int s = 0; s < ctx->num_stores && !snapScanAborted(ctx); s++) {
        if (!ctx->frozen[s]) continue;   /* nothing of the fork instant in it (snapScanFreezeStore) */
        for (int didx = 0; didx < ctx->num_dicts[s]; didx++) {
            if (snapScanAborted(ctx)) break;
            /* The save's own table: the fork-instant dicts, referenced,
             * whatever the store did with them since. */
            dict *d = snapTableDict(ctx, s, didx);
            if (!d) continue;
            atomicIncr(ctx->slots_scanned, 1);
            /* Claim before announcing the layout: a cap that finds the
             * thread on the dict sees SCANNING and pauses it, as does any
             * access of the dict from then on (guard). */
            snapScanClaim(ctx, d);
            atomicSet(ctx->cur_slot, (int64_t)SNAP_SCAN_ENC(s, didx));
            if (snapScanWalkDict(ctx, s, didx, d, &since_done))
                break;   /* Aborted: leave the slot SCANNING; snapScanAbort() completes it. */
            snapScanPublishScanned(ctx, s, didx, d);
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
    __atomic_store_n(&snap_cur, ctx->pos_end, __ATOMIC_SEQ_CST);
    __atomic_store_n(&snap_lo, ctx->pos_end, __ATOMIC_SEQ_CST);
    snapWalkerQuiesce(ctx);
    if (ctx->window_done) ctx->window_done(ctx->privdata);
    if (ctx->thread_done) ctx->thread_done(ctx->privdata, snapScanAborted(ctx));
    atomicSetWithSync(ctx->finished, 1);
    triggerEventNotifier(ctx->en);
    return NULL;
}

/* Main thread. Freeze every store: from here on the dicts are the fork-instant
 * keyspace and every write to one goes through the guard first. Nothing else
 * may run on the main thread between this call and the fork of the writer
 * child. One pass per non-empty store: a reference and a pointer per
 * fork-instant dict, nothing allocated per dict. */
static void snapScanFreeze(snapScanCtx *ctx) {
    serverAssert(!ctx->started && !ctx->frozen_all);
    snapWindowReset(NULL);   /* no thread yet: drop the positions of a previous save */
    for (int s = 0; s < ctx->num_stores; s++) snapScanFreezeStore(ctx, s);
    ctx->ring = zmalloc(sizeof(uint64_t) * (ctx->slots_total ? ctx->slots_total : 1));
    ctx->frozen_all = 1;
}

/* Main thread. Spawn the scan thread over an already frozen keyspace. */
static int snapScanStartThread(snapScanCtx *ctx) {
    serverAssert(!ctx->started && ctx->frozen_all);
    snapWindowReset(ctx);
    uint64_t v;
    atomicGet(snap_funnel_seq, v);
    if (v & 1) snapWindowCap(ctx);   /* started from inside a section: the thread begins capped */
    if (pthread_create(&ctx->tid, NULL, snapScanThreadMain, ctx) != 0) {
        serverLog(LL_WARNING, "snapScanStartThread: pthread_create failed: %s", strerror(errno));
        snapWindowReset(NULL);
        return C_ERR;
    }
    ctx->started = 1;
    return C_OK;
}

/* Main thread. Installs the snapshot guard on every non-empty store
 * (kvstore.h) and spawns the scan thread. Nothing is visited, paused or
 * allocated per dict: a dict is stamped with the save's generation, gets its
 * bucket bitmaps and overlay, and is paused, on demand: at the first
 * structural write to it, before hi may pass its layout, or on an access while
 * the thread is on it. Returns C_OK or C_ERR (nothing frozen on error).
 * From this point until snapScanRelease() every main-thread write to an owed
 * dict goes through the overlay hooks: in place once the snapshot holds the
 * bucket, to a duplicate of its chain while the thread reads it.
 * snapScanStart() == snapScanFreeze() + snapScanStartThread(). */
static int snapScanStart(snapScanCtx *ctx) {
    snapScanFreeze(ctx);
    if (snapScanStartThread(ctx) != C_OK) {
        /* Nothing was scanned: complete whatever was attached as aborted
         * (drains an empty overlay, unfreezes) and take the guards off. */
        snapScanFinishAll(ctx);
        ctx->frozen_all = 0;
        return C_ERR;
    }
    return C_OK;
}

/* Thread-side request to stop (e.g. the channel to the child broke). */
static inline void snapScanRequestAbort(snapScanCtx *ctx) {
    atomicSetWithSync(ctx->abort, 1);
}

/* Owner thread: the save is finished with the dict (its scan completed or
 * the save was aborted). The dict's overlayed buckets are written back and released by the
 * kvstore overlay; the bitmaps are freed (their bytes returned) and the
 * layout unpaused here. The dict may have left its store meanwhile: the state
 * follows the dict, so this works wherever it is. */
static size_t snapScanStateRelease(dict *d) {
    snapDictState *sn = snapMeta(d)->ext;
    size_t bytes = 0;
    if (sn) {
        for (int t = 0; t < 2; t++) {
            zfree(sn->bits[t]);
            bytes += sn->bytes[t];
        }
        if (sn->paused) {
            dictResumeRehashing(d);
            dictResumeAutoResize(d);
        }
        __atomic_store_n(&snapMeta(d)->ext, NULL, __ATOMIC_RELEASE);
        zfree(sn);
    }
    kvstoreOverlayRelease(d);
    return bytes;
}

/* Main thread: finish one slot (release the overlay and bitmaps, resume
 * the layout, slot-done hook, publish DONE). */
static void snapScanCompleteSlot(snapScanCtx *ctx, int s, int didx, int aborted) {
    /* The thread no longer reads this slot: whatever buckets are still
     * overlayed (normally none: they are written back as the window moves) go back to
     * the dict, the dict is unfrozen, and the save's reference goes: a dict
     * its store let go of meanwhile (freed, emptied, moved and released) is
     * freed here. */
    dict *d = snapTableDict(ctx, s, didx);
    serverAssert(d != NULL);
    /* DONE first: the release below writes the overlay back and then asks the
     * store whether the dict may be freed, through the public kvstore entry
     * points, i.e. through the guard. An aborted slot is still SCANNING at
     * this point, and the guard would attach a fresh overlay and state to a
     * table dict in that state, which this completion would then leave
     * behind for the dict's eventual free to trip over (seen with the slot
     * snapshot's fail-point aborts). With DONE published the guard declines. */
    snapWordStore(d, SNAP_WORD(ctx->gen, SNAP_SLOT_DONE));
    ctx->bitmap_bytes -= snapScanStateRelease(d);
    if (ctx->slot_done) ctx->slot_done(ctx->privdata, s, didx, aborted);
    if (kvstoreDictRefcount(d) == 1) { ctx->retained_dicts++; snap_stat_retained_dicts++; }   /* the save outlived the store's hold on it */
    __atomic_store_n(&ctx->dicts[s][didx], NULL, __ATOMIC_RELAXED);
    kvstoreDictUnref(d);
    ctx->nrefs[s]--;
    ctx->slots_done++;
    if (ctx->nrefs[s] == 0) snapScanStoreDone(ctx, s);
}

/* Main thread. Handles slots the thread finished since the last call: for
 * each, calls slot_done, unfreezes the dict and publishes DONE. Returns the
 * number of slots completed. Also to be called once after
 * snapScanIsFinished() to drain the tail. */
static int snapScanHandleCompletions(snapScanCtx *ctx) {
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

/* Main thread, the thread joined or never started: complete what the save
 * still holds as aborted (the dict the thread was reading, then every
 * remaining entry of the save's table: a pointer per slot), then every
 * store. Nothing is owed after this. */
static void snapScanFinishAll(snapScanCtx *ctx) {
    int64_t cur;
    atomicGet(ctx->cur_slot, cur);
    if (cur >= 0) {
        atomicSet(ctx->cur_slot, -1);
        snapScanCompleteSlot(ctx, SNAP_SCAN_DEC_STORE((uint64_t)cur), SNAP_SCAN_DEC_DIDX((uint64_t)cur), 1);
    }
    for (int s = 0; s < ctx->num_stores; s++) {
        if (!ctx->frozen[s]) continue;
        for (int didx = 0; ctx->nrefs[s] > 0 && didx < ctx->num_dicts[s]; didx++)
            if (ctx->dicts[s][didx]) snapScanCompleteSlot(ctx, s, didx, 1);
        snapScanStoreDone(ctx, s);   /* a no-op when the last completion did it */
    }
    ctx->ended = 1;
}

static int snapScanIsFinished(snapScanCtx *ctx) {
    int v;
    atomicGetWithSync(ctx->finished, v);
    return v;
}

static void snapScanJoin(snapScanCtx *ctx) {
    if (ctx->started && !ctx->joined) {
        pthread_join(ctx->tid, NULL);
        ctx->joined = 1;
        if (snap_live == ctx) snap_live = NULL;   /* lo stays where the thread left it */
    }
}

/* Main thread. Asks the thread to stop (it stops within one cursor step),
 * joins it, drains fully-scanned slots normally and completes every other
 * slot with aborted=1, unfreezing all dicts. */
static void snapScanAbort(snapScanCtx *ctx) {
    if (!ctx->started) {
        /* Frozen but no thread (fork failure path): unfreeze everything. */
        if (ctx->frozen_all) snapScanFinishAll(ctx);
        return;
    }
    atomicSetWithSync(ctx->abort, 1);
    snapScanJoin(ctx);
    /* Fully scanned slots complete normally. */
    snapScanHandleCompletions(ctx);
    /* Everything else was interrupted. */
    snapScanFinishAll(ctx);
}

/* Main thread. Joins the thread if still running (does not abort it: call
 * snapScanAbort() first if the scan may be in progress) and frees the ctx.
 * Every slot must be DONE by then unless the ctx was never started. */
static void snapScanRelease(snapScanCtx *ctx) {
    if (ctx == NULL) return;
    snapScanJoin(ctx);
    if (ctx->started)
        snapScanWindowStats(ctx, &snap_last_window.stalls, &snap_last_window.stall_us, &snap_last_window.grants);
    if (ctx->started) {
        /* Drain the tail, then complete whatever the thread left owed (it
         * stopped early on a broken channel) so no slot stays frozen. */
        snapScanHandleCompletions(ctx);
        snapScanFinishAll(ctx);
    }
    for (int s = 0; s < ctx->num_stores; s++)
        serverAssert(!ctx->frozen[s]);   /* every slot completed: the guard is already off */
    zfree(ctx->guards);
    for (int s = 0; s < ctx->num_stores; s++) zfree(ctx->dicts[s]);
    zfree(ctx->dicts);
    zfree(ctx->nrefs);
    zfree(ctx->num_dicts);
    zfree(ctx->ord_base);
    zfree(ctx->frozen);
    zfree(ctx->stores);
    zfree(ctx->ring);
    freeEventNotifier(ctx->en);
    zfree(ctx);
}

/* Either thread. DONE for a slot the save holds nothing of (empty at the
 * fork instant, created since, or completed). */
static int snapScanSlotState(snapScanCtx *ctx, int store_idx, int didx) {
    if (ctx->ended || !ctx->frozen[store_idx]) return SNAP_SLOT_DONE;
    dict *d = snapTableDict(ctx, store_idx, didx);
    if (!d) return SNAP_SLOT_DONE;
    return snapDictStateOf(ctx, d);
}

/* Readable fd that becomes ready whenever a slot completes or the thread
 * exits; register snapScanAeHandler() on it with AE_READABLE and 'ctx' as
 * client data. -1 if unavailable. */
static int snapScanReadEventFd(snapScanCtx *ctx) {
    return ctx->en ? getReadEventFd(ctx->en) : -1;
}

static void snapScanAeHandler(struct aeEventLoop *el, int fd, void *privdata, int mask) {
    UNUSED(el);
    UNUSED(fd);
    UNUSED(mask);
    snapScanCtx *ctx = privdata;
    handleEventNotifier(ctx->en);
    snapScanHandleCompletions(ctx);
}

/* Progress for INFO / tests: dicts allocated at freeze in the scanned
 * stores, dicts the thread passed, dicts finished (completed or skipped). */
static void snapScanProgress(snapScanCtx *ctx, long *slots_total, long *slots_scanned,
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
 *   2. snapScanFreeze(): one reference and a generation stamp per non-empty
 *      keyspace dict, a guard on every store; nothing is paused and nothing
 *      is allocated per dict yet (bitmaps and overlays attach on demand,
 *      at the first write that needs them)
 *   3. redisFork(): the child inherits the SNAP_FORK values (copy-on-write)
 *      but none of the DONTFORK memory (headers, NOFORK values, tables)
 *   4. snapScanStartThread(): the scan thread walks the save's table of
 *      fork-instant dicts below hi and streams records to the child
 *
 * Two pipes to the child. The small channel carries BUFFER messages: 64 KB
 * staging buffers of pre-encoded records, produced by the scan thread and by
 * the main thread's emit-ahead (forwarded by the thread every W buckets
 * and at the end of each dict, in order). The big channel carries one KVOBJ message per
 * SNAP_FORK key (a copy of the kvobj header; the child follows the value
 * pointer into its own snapshot), then END with both channels' message
 * counts. A BUFFER may depend on big values that must precede it in the
 * output (robj_off); the child holds it until they are written.
 *
 * The child owns the output: RDB header, SELECTDB/RESIZEDB/SLOT_INFO (or
 * SELECT in AOF command form) tracked per message, records, footer, fsync,
 * rename; or the replica sockets for a diskless sync. Completion and abort
 * both flow through the classic child machinery (checkChildrenDone -> the done
 * handler -> snapOnChildEnd).
 */

static size_t snap_last_retained_peak = 0;
static size_t snap_last_queued_peak = 0;
static long long snap_last_inplace_writes = 0;

static long long snapSaveInplaceWrites(void) { return snap_save ? snap_save->inplace_writes : 0; }
static long long snapSaveLastInplaceWrites(void) { return snap_last_inplace_writes; }


int snapSaveInProgress(void) {
    return snap_save != NULL && !snap_save->aborted;
}

/* Bytes the overlay currently keeps alive for the running save (replaced or
 * deleted originals); the peak of the last completed save and the
 * hard-mark abort counter are reported next to it. */
static size_t snapSaveRetainedBytes(void) { return snap_save ? snap_save->retained_bytes : 0; }
static size_t snapSaveLastRetainedPeak(void) { return snap_last_retained_peak; }
static size_t snapSaveBitmapBytes(void) {
    if (!snap_save || snap_child_no_keyspace) return 0;   /* the stores live in the arena, not mapped in the child (crash report INFO) */
    return snapScanBitmapBytes(snap_save->scan);
}
/* Fork-instant dicts their store let go of (freed, flushed, moved out by a
 * slot trim) before the save completed them: the save read them to the end
 * and freed them. */
static long snapSaveRetainedDicts(void) { return snap_stat_retained_dicts; }
static long snapSaveGrownDicts(void) { return snap_save ? snapScanDictsGrown(snap_save->scan) : 0; }

/* Main-thread emission (emit-ahead): bytes queued for the scan thread to
 * forward to the child (main-thread queue plus the thread's outbound queue),
 * and the peak of the two over the last save. */
/* What the running save holds in the parent that the classic child would hold
 * in its own process, for the eviction calculation (evict.c): the records
 * queued for the child, the fork-instant originals the overlay keeps alive
 * while it reads them, the bucket bitmaps, and the fixed bookkeeping (the
 * per-slot size tables, the dict tables, the completion ring). Memory the
 * save holds for its duration and releases, not the dataset's: counting it
 * would evict keys during every save on an instance near its limit. */
size_t snapSaveOverheadBytes(void) {
    snapSave *sv = snap_save;
    if (!sv) return 0;
    size_t bytes = snapSaveQueuedBytes() + sv->retained_bytes + snapScanBitmapBytes(sv->scan) + snapScanTableBytes(sv->scan);
    bytes += (size_t)sv->dbnum * sizeof(snapDbSizes);
    for (int j = 0; j < sv->dbnum; j++)
        bytes += (size_t)sv->sizes[j].nslots * 2 * sizeof(unsigned long long);
    if (sv->trim_slots) bytes += CLUSTER_SLOTS / 8;
    if (sv->slot_filter) bytes += CLUSTER_SLOTS / 8;
    return bytes;
}

static size_t snapSaveQueuedBytes(void) {
    if (!snap_save) return 0;
    size_t n, o;
    atomicGet(snap_save->mq_bytes, n);
    atomicGet(snap_save->oq_bytes, o);
    return n + o;
}
static size_t snapSaveLastQueuedPeak(void) { return snap_last_queued_peak; }
static long long snapSaveMainBuckets(void) { return snap_save ? snap_save->main_buckets : 0; }
static long long snapSaveMainKeys(void) { long long n = 0; if (snap_save) atomicGet(snap_save->main_keys, n); return n; }


/* ---- INFO, DEBUG and the crash report (server.c, debug.c call these) ---- */

/* INFO memory: the arena's allocator statistics (sampled by cron next to the
 * default arena's) and its fork visibility. */
sds snapInfoMemory(sds info) {
    return sdscatprintf(info, FMTARGS(
        "allocator_allocated_nofork:%zu\r\n", server.cron_malloc_stats.nofork_allocator_allocated,
        "allocator_active_nofork:%zu\r\n", server.cron_malloc_stats.nofork_allocator_active,
        "allocator_resident_nofork:%zu\r\n", server.cron_malloc_stats.nofork_allocator_resident,
        "allocator_frag_bytes_nofork:%zu\r\n", server.cron_malloc_stats.nofork_allocator_frag_smallbins_bytes,
        "snap_nofork_arena_available:%d\r\n", snapArenaAvailable(),
        "snap_nofork_arena_forkable:%d\r\n", snapArenaForkable(),
        "snap_nofork_va_reserved:%zu\r\n", snapArenaVaReserved(),
        "snap_nofork_va_used:%zu\r\n", snapArenaVaUsed()));
}

/* current_save_keys_processed during a hybrid save: the child sees only the
 * SNAP_FORK keys, so the figure is what the scan thread and the main thread
 * dispatched so far; 'classic' otherwise. */
size_t snapSaveKeysProcessed(size_t classic) {
    if (!snapSaveInProgress()) return classic;
    long slots_total, slots_done;
    long long keys_nofork, keys_fork, bytes_sent;
    snapSaveProgress(&slots_total, &slots_done, &keys_nofork, &keys_fork, &bytes_sent);
    return (size_t)(keys_nofork + keys_fork + snapSaveMainKeys());
}

/* INFO persistence: the running save's progress and queues, the last save's
 * peaks and the counters across saves. */
sds snapInfoPersistence(sds info) {
    long slots_total = 0, slots_done = 0;
    long long keys_nofork = 0, keys_fork = 0, bytes_sent = 0;
    snapSaveProgress(&slots_total, &slots_done, &keys_nofork, &keys_fork, &bytes_sent);
    kvstoreOverlayStats os = kvstoreOverlayStatsGet();
    long long stalls, stall_us, grants;
    snapScanWindowStats(snap_save ? snap_save->scan : NULL, &stalls, &stall_us, &grants);
    return sdscatprintf(info, FMTARGS(
        "snap_save_in_progress:%d\r\n", snapSaveInProgress(),
        "snap_save_slots_total:%ld\r\n", slots_total,
        "snap_save_slots_done:%ld\r\n", slots_done,
        "snap_save_keys_nofork:%lld\r\n", keys_nofork,
        "snap_save_keys_fork:%lld\r\n", keys_fork,
        "snap_save_bytes_sent:%lld\r\n", bytes_sent,
        "snap_save_retained_bytes:%zu\r\n", snapSaveRetainedBytes(),
        "snap_save_side_entries:%ld\r\n", snapSaveSideEntries(),
        "snap_save_overlay_buckets:%lld\r\n", os.buckets,
        "snap_save_overlay_clones:%lld\r\n", os.clones,
        "snap_save_overlay_peak_buckets:%ld\r\n", os.peak,
        "snap_save_overlay_paused_ms:%lld\r\n", os.paused_us / 1000,
        "snap_save_scan_stalls:%lld\r\n", stalls,
        "snap_save_scan_stall_ms:%lld\r\n", stall_us / 1000,
        "snap_save_window_grants:%lld\r\n", grants,
        "snap_last_save_retained_peak:%zu\r\n", snapSaveLastRetainedPeak(),
        "snap_save_queued_bytes:%zu\r\n", snapSaveQueuedBytes(),
        "snap_last_save_queued_peak:%zu\r\n", snapSaveLastQueuedPeak(),
        "snap_save_bitmap_bytes:%zu\r\n", snapSaveBitmapBytes(),
        "snap_save_grown_dicts:%ld\r\n", snapSaveGrownDicts(),
        "snap_retained_dicts:%ld\r\n", snapSaveRetainedDicts(),
        "snap_save_main_buckets:%lld\r\n", snapSaveMainBuckets(),
        "snap_save_main_keys:%lld\r\n", snapSaveMainKeys(),
        "snap_hard_mark_aborts:%lld\r\n", server.stat_snap_hard_mark_aborts,
        "snap_saves:%lld\r\n", server.stat_snap_saves,
        "snap_hybrid_blocked_by:%s\r\n", moduleHybridSnapshotBlockedBy(),
        "snap_save_inplace_writes:%lld\r\n", snapSaveInplaceWrites(),
        "snap_last_save_inplace_writes:%lld\r\n", snapSaveLastInplaceWrites(),
        "snap_inplace_writes_total:%lld\r\n", server.stat_snap_inplace_writes));
}

/* The INFO sections a crash report may generate inside a child forked without
 * the keyspace (the hybrid save child, a REDISMODULE_FORK_NO_KEYSPACE module
 * fork: memory, keyspace, keysizes and hotkeys would fault); NULL in every
 * other process, meaning "all". */
const char *const *snapCrashReportInfoSections(void) {
    static const char *const keep[] = {"server", "clients", "persistence", "stats", "replication", "threads",
                                       "cpu", "module_list", "errorstats", "cluster", NULL};
    return snapChildNoKeyspace() ? keep : NULL;
}

static redisDb *snapDebugDbArg(client *c) {
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
int snapDebugCommand(client *c) {
    const char *sub = c->argv[1]->ptr;
    if (!strcasecmp(sub, "snap-placement") && (c->argc == 2 || c->argc == 3)) {
        redisDb *db = snapDebugDbArg(c);
        if (!db) return 1;
        snapPlacementStats st;
        snapPlacementScan(db, &st);
        addReplyMapLen(c, 11);
        addReplyBulkCString(c, "keys"); addReplyLongLong(c, st.keys);
        addReplyBulkCString(c, "dict_tables_in_arena"); addReplyLongLong(c, st.dict_tables_in_arena);
        addReplyBulkCString(c, "dict_tables_outside"); addReplyLongLong(c, st.dict_tables_outside);
        addReplyBulkCString(c, "arena_available"); addReplyLongLong(c, snapArenaAvailable());
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
        redisDb *db = snapDebugDbArg(c);
        if (db) snapPlacementDriftReply(c, db, 50);
        return 1;
    }
    if (!strcasecmp(sub, "snap-window-buckets") && (c->argc == 2 || c->argc == 3)) {
        if (c->argc == 3) {
            long long n;
            if (getLongLongFromObjectOrReply(c, c->argv[2], &n, NULL) != C_OK) return 1;
            if (n < 1 || n > SNAP_WINDOW_BUCKETS_MAX) {
                addReplyError(c, "Out of range");
                return 1;
            }
            snapSetWindowBuckets((unsigned long)n);
        }
        addReplyLongLong(c, (long long)snapGetWindowBuckets());
        return 1;
    }
    return 0;
}

/* ---- Bucket position and main-thread emission (emit-ahead) ---- */

/* Main thread, inside a write section: where the thread is relative to
 * bucket (table, idx) of dict d of slot (s, didx). Below lo it has read the
 * bucket; at or above hi it cannot reach it before the main thread raises hi
 * (and anything emitted by then is marked first); in between it may be
 * reading it. A dict the save holds but has not paused is beyond hi as a
 * whole. When the bucket is inside and 'win' is given, *win receives its
 * overlay window: the aligned run of ov_block buckets around it (at most the
 * table), tagged with the position of its last bucket. */
static int snapBucketPosition(snapScanCtx *ctx, int s, int didx, dict *d, int table, unsigned long idx, kvstoreOvWindow *win) {
    uint8_t st = snapScanDictStateMain(ctx, s, didx, d);
    if (st >= SNAP_SLOT_SCANNED) return SNAP_POS_BEHIND;
    if (st == SNAP_SLOT_PENDING && !snapDictPaused(d)) return SNAP_POS_AHEAD;
    uint64_t base = SNAP_POS(snapOrd(ctx, s, didx), 1 + table, 0);
    uint64_t p = base + idx;
    if (p < snapPosLoLoad()) return SNAP_POS_BEHIND;
    if (p >= ctx->hi) return SNAP_POS_AHEAD;
    if (win) {
        unsigned long size = DICTHT_SIZE(d->ht_size_exp[table]);
        unsigned long n = ctx->ov_block < size ? ctx->ov_block : size;
        win->table = table;
        win->lo = idx & ~(n - 1);
        win->n = n;
        win->last = base + win->lo + n - 1;
    }
    return SNAP_POS_INSIDE;
}

/* A message buffer holding only the header space; the payload is appended
 * and the header filled in when the message is handed over. */
static sds snapMsgNew(size_t payload) {
    sds m = sdsMakeRoomForNonGreedy(sdsempty(), sizeof(snapMsgHdr) + payload);
    sdsIncrLen(m, sizeof(snapMsgHdr));
    return m;
}

/* mq_lock held: complete message 'm' (header space + payload) and queue it as
 * it will go on the pipe, without another copy. */
static void snapSaveQueuePushLocked(snapSave *sv, uint32_t type, int dbid, int slot, sds m, uint32_t robj_off, uint64_t src_addr) {
    snapMsgHdr h = { type, dbid, slot, (uint32_t)(sdslen(m) - sizeof(snapMsgHdr)), robj_off, src_addr };
    memcpy(m, &h, sizeof(h));
    listAddNodeTail(sv->mq, m);
    atomicIncr(sv->mq_bytes, sdsAllocSize(m));
}

/* Main thread, mq_lock held: hand the partial BUFFER over as a message. */
static void snapSaveMainStageFlushLocked(snapSave *sv) {
    if (sdslen(sv->mstage) <= sizeof(snapMsgHdr)) return;
    snapSaveQueuePushLocked(sv, SNAP_MSG_BUFFER, sv->mstage_dbid, sv->mstage_slot, sv->mstage, 0, 0);
    sv->mstage = snapMsgNew(0);
}

/* Cluster mode: is slot didx one an atomic slot migration was trimming at the
 * fork instant? Its keys are skipped by every producer (classic parity). */
static inline int snapSaveSlotTrimmed(snapSave *sv, int didx) {
    return sv->trim_slots && (sv->trim_slots[didx >> 3] & (1u << (didx & 7)));
}

/* One record of 'kv' in the save's format, from any producer (main thread,
 * scan thread, child): the RDB key-value pair, or for an AOF rewrite without
 * the preamble the command form rewriteAppendOnlyFileRio writes per key
 * (per-type rewriter, PEXPIREAT, BLESS, module key metadata). 0 on success. */
static int snapSaveWriteRecord(snapSave *sv, rio *r, kvobj *kv, int dbid) {
    robj key;
    if (sv->slots_form) return slotSnapshotSaveKeyValuePair(r, kv, dbid) == C_OK ? 0 : -1;
    initStaticStringObject(key, kvobjGetKey(kv));
    if (sv->aof_cmd_form) return rewriteObject(r, &key, kv, dbid, kvobjGetExpire(kv)) == C_OK ? 0 : -1;
    return rdbSaveKeyValuePair(r, &key, kv, kvobjGetExpire(kv), dbid) == -1 ? -1 : 0;
}

/* Main thread: serialize the whole chain of bucket (table, idx) of dict d of
 * slot (s, didx) as it is now, i.e. still at its fork-instant state (no key of
 * a not yet emitted bucket ahead of the thread was ever written in place). */
static void snapSaveEmitBucket(snapSave *sv, int s, int didx, dict *d, int table, unsigned long idx) {
    snap_main_emitting = 1;
    long long keys = 0;
    sdsclear(sv->mscratch);
    for (dictEntry *de = d->ht_table[table][idx]; de; de = dictGetNext(de)) {
        kvobj *kv = dictGetKV(de);
        if (snapSaveSlotTrimmed(sv, didx)) {
            atomicIncr(sv->keys_skipped, 1);
            continue;
        }
        int cls = snapKeyClass(kv);
        if (cls == SNAP_NOFORK && getModuleMetaBits(kv->metabits)) cls = SNAP_FORK;
        if (cls == SNAP_FORK) {
            /* The child resolves kv->ptr in its own memory: it must be forkable. */
            char *alloc = kvobjGetAllocPtr(kv);
            debugServerAssert(kv->encoding == OBJ_ENCODING_INT || kv->encoding == OBJ_ENCODING_EMBSTR ||
                              !zmalloc_is_nofork(kv->ptr));
            size_t len = zmalloc_size(alloc);
            sds copy = snapMsgNew(len);
            copy = sdscatlen(copy, alloc, len);
            pthread_mutex_lock(&sv->mq_lock);
            snapSaveQueuePushLocked(sv, SNAP_MSG_KVOBJ, s, didx, copy, (uint32_t)((char *)kv - alloc), (uint64_t)(uintptr_t)alloc);
            pthread_mutex_unlock(&sv->mq_lock);
        } else {
            /* The chain's records pile up in mscratch (appended: a rio over
             * the sds adds to it) and join the stage under one lock below. */
            rio r;
            rioInitWithBuffer(&r, sv->mscratch);
            if (snapSaveWriteRecord(sv, &r, kv, s) != 0)
                serverPanic("Hybrid snapshot: failed to serialize a key on the main thread");
            sv->mscratch = r.io.buffer.ptr;
        }
        keys++;
    }
    if (sdslen(sv->mscratch)) {
        pthread_mutex_lock(&sv->mq_lock);
        if (sv->mstage_dbid != s || sv->mstage_slot != didx) {
            snapSaveMainStageFlushLocked(sv);
            sv->mstage_dbid = s;
            sv->mstage_slot = didx;
        }
        sv->mstage = sdscatlen(sv->mstage, sv->mscratch, sdslen(sv->mscratch));
        /* mq_bytes counts the stage when it is handed over as a message
         * (snapSaveQueuePushLocked); counting the records here as well
         * made the figure drift by the whole emitted volume and produced
         * a false hard-mark abort. */
        if (sdslen(sv->mstage) - sizeof(snapMsgHdr) >= SNAP_STAGE_BYTES) snapSaveMainStageFlushLocked(sv);
        pthread_mutex_unlock(&sv->mq_lock);
    }
    if (keys) atomicIncr(sv->main_keys, keys);
    sv->main_buckets++;
    snap_main_emitting = 0;
}

/* Scan thread, every W buckets, at the end of each dict and before END:
 * forward what the main thread emitted. */
/* Scan thread: rdb-key-save-delay for 'keys' fork-instant keys. The classic
 * child sleeps it per key, so a save of N keys takes N delays; here the
 * thread sleeps it for every key of the fork instant whoever produced the
 * record (itself, the child it shipped a big value to, or the main thread's
 * emit-ahead, which must not slow the command that triggered it, so its
 * share is slept before END), in slices so an abort or CONFIG SET
 * rdb-key-save-delay 0 is seen within 10 ms. */
static int snapSaveKeyDelay(snapSave *sv, long long keys) {
    int slept = 0;
    while (keys-- > 0 && !snapScanAborted(sv->scan)) {
        int delay;
        atomicGet(sv->key_save_delay, delay);
        if (delay <= 0) return slept;
        slept = 1;
        while (delay > 0 && !snapScanAborted(sv->scan)) {
            int slice = delay > 10000 ? 10000 : delay;
            debugDelay(slice);
            delay -= slice;
        }
    }
    return slept;
}

static void snapSaveWindowDone(void *priv) {
    snapSave *sv = priv;
    list *ready = sv->mq_ready;   /* empty: drained by the previous call */
    pthread_mutex_lock(&sv->mq_lock);
    snapSaveMainStageFlushLocked(sv);
    sv->mq_ready = sv->mq;
    sv->mq = ready;
    ready = sv->mq_ready;
    pthread_mutex_unlock(&sv->mq_lock);
    if (listLength(ready)) snapStageFlush(sv);   /* this thread's partial buffer first (order is free anyway) */
    listNode *ln;
    while ((ln = listFirst(ready)) != NULL) {
        sds m = listNodeValue(ln);
        size_t a = sdsAllocSize(m);
        listDelNode(ready, ln);
        if (snapScanAborted(sv->scan)) {
            sdsfree(m);
            atomicDecr(sv->mq_bytes, a);
        } else {
            uint32_t type;
            memcpy(&type, m, sizeof(type));
            snapOutEnqueue(sv, m, type);   /* the queue owns m now: it may be sent and freed already */
            atomicDecr(sv->mq_bytes, a);
        }
    }
}

/* Inside a write section: is the frozen chain of bucket (table, idx) of dict
 * d of slot (s, didx) still owed to the snapshot, i.e. neither read by the
 * thread already nor emitted by the main thread? Only an owed chain must not
 * be written in place. *pos receives the bucket's position.
 *
 * The emitted bit is checked whatever the position: a section may see the
 * bucket ahead, emit it and set the bit, and later raise hi over it. A later
 * look in the same section sees the bucket inside the window, but the thread
 * will skip it (it reads the bit after reading hi), so it is not owed. */
static int snapBucketOwed(snapScanCtx *ctx, int s, int didx, dict *d, int table, unsigned long idx, int *pos, kvstoreOvWindow *win) {
    *pos = snapBucketPosition(ctx, s, didx, d, table, idx, win);
    if (*pos == SNAP_POS_BEHIND) return 0;
    return !snapScanBucketEmitted(ctx, s, didx, table, idx);
}

/* Overlay 'prepare' hook (privdata = the store's snapGuardPriv): may the frozen chain of
 * 'key' be written in place? Emits the bucket first when the thread has not
 * reached it; defers it, in the window *win, when the thread is reading it. */
static int snapOverlayPrepare(void *privdata, int didx, dict *d, const void *key, kvstoreOvWindow *win) {
    int s = ((struct snapGuardPriv *)privdata)->s;
    snapSave *sv = snap_save;
    if (!sv || sv->aborted) return KVSTORE_OV_INPLACE;
    int table, pos;
    unsigned long idx;
    dictKeyBucket(d, key, &table, &idx);
    if (dictIsRehashing(d) && snapScanDictStateMain(sv->scan, s, didx, d) == SNAP_SLOT_PENDING && !snapDictPaused(d)) {
        /* An unreached dict mid-rehash, beyond hi (not paused), keeps rehashing: entries of
         * old bucket i0 move to new buckets i0 + j*n at any step, so the unit
         * the main thread emits and marks is the whole class, both tables.
         * Marking a new bucket the old chain may still move into is what
         * keeps the scan thread from reading those moved entries later. */
        unsigned long n = DICTHT_SIZE(d->ht_size_exp[0]), size1 = DICTHT_SIZE(d->ht_size_exp[1]);
        unsigned long i0 = (table == 0) ? idx : (idx & (n - 1));
        int owed = !snapScanBucketEmitted(sv->scan, s, didx, 0, i0);
        for (unsigned long j = i0; !owed && j < size1; j += n) owed = !snapScanBucketEmitted(sv->scan, s, didx, 1, j);
        if (!owed) {
            sv->inplace_writes++;
            server.stat_snap_inplace_writes++;
            return KVSTORE_OV_INPLACE;
        }
        if (!snapScanBucketEmitted(sv->scan, s, didx, 0, i0)) {
            if ((long)i0 >= d->rehashidx) snapSaveEmitBucket(sv, s, didx, d, 0, i0);   /* below rehashidx the chain moved already */
            snapScanMarkBucketEmitted(sv->scan, s, didx, 0, i0);
        }
        for (unsigned long j = i0; j < size1; j += n) {
            if (snapScanBucketEmitted(sv->scan, s, didx, 1, j)) continue;
            snapSaveEmitBucket(sv, s, didx, d, 1, j);
            snapScanMarkBucketEmitted(sv->scan, s, didx, 1, j);
        }
        return KVSTORE_OV_INPLACE;
    }
    if (!snapBucketOwed(sv->scan, s, didx, d, table, idx, &pos, win)) {
        sv->inplace_writes++;
        server.stat_snap_inplace_writes++;
        return KVSTORE_OV_INPLACE;
    }
    if (pos == SNAP_POS_INSIDE) return KVSTORE_OV_DEFER;
    snapSaveEmitBucket(sv, s, didx, d, table, idx);
    snapScanMarkBucketEmitted(sv->scan, s, didx, table, idx);
    return KVSTORE_OV_INPLACE;
}

/* Main thread, inside a write section at a point where no caller holds a
 * link: write back the overlayed buckets the scan thread has passed, i.e.
 * those of the overlay windows wholly below lo. Called at the end of every
 * section and from cron; a no-op until lo has moved since the last drain. */
static uint64_t snap_drain_lo = 0;
static void snapSaveDrainSideTables(void) {
    snapSave *sv = snap_save;
    if (!sv || sv->aborted) return;
    uint64_t lo = snapPosLoLoad();
    if (lo == snap_drain_lo) return;
    snap_drain_lo = lo;
    kvstoreOverlayDrain(lo);
}

/* Overlayed buckets of the running save. */
static long snapSaveSideEntries(void) {
    snapSave *sv = snap_save;
    if (!sv || sv->aborted) return 0;
    return kvstoreOverlayPending();
}

/* ---- Stores versus dbs (SWAPDB, FLUSHDB during a save) ---- */

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

/* FLUSHDB and SWAPDB during a save. A store is a kvstore the save is
 * scanning, identified by the db id it had at the fork instant. */
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
    serverLog(LL_NOTICE, "Hybrid snapshot: db %d flushed during the save, its keyspace stays with the save until its slots complete", s);
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

/* 1 if 'kv' is a fork-instant original the running save has not taken yet:
 * its bucket is ahead of the scan thread and not emitted by the main thread,
 * or the thread is reading it right now. Such an object must not be mutated
 * in place. Opens the command's write section. The answer stays valid until
 * the command ends: a bucket behind the thread or emitted stays so, and one
 * still owed is only treated as owed for longer than needed; a read path that would mutate (lazy field expiry) skips the mutation when
 * this returns 1, a read path that must mutate calls snapPrepareMutation. */
int snapKvobjIsOwed(redisDb *db, kvobj *kv) {
    snapSave *sv = snap_save;
    if (!sv || sv->aborted) return 0;
    int s = snapStoreIndexOf(sv, db->keys);
    if (s < 0) return 0;   /* a keyspace created after the fork: nothing in it is owed */
    sds key = kvobjGetKey(kv);
    int slot = server.cluster_enabled ? calculateKeySlot(key) : 0;
    dict *d = kvstoreGetDict(db->keys, slot);
    if (!d) return 0;
    uint8_t st = snapScanDictStateMain(sv->scan, s, slot, d);
    if (st == SNAP_SLOT_SCANNED || st == SNAP_SLOT_DONE) return 0;
    snapFunnelOpen();   /* the cap: positions below lo stay behind, hi moves only by our hand */
    int table, pos;
    unsigned long idx;
    dictKeyBucket(d, key, &table, &idx);
    if (!snapBucketOwed(sv->scan, s, slot, d, table, idx, &pos, NULL)) return 0;
    /* Ahead and not emitted, or being read: the frozen entry itself is owed,
     * its clone in an overlayed bucket is not. */
    dictEntryLink l = dictFindLink(d, key, NULL);
    return l && dictGetKV(*l) == kv;
}

/* A read-path command about to write 'kv' in place (PFCOUNT's cache, a module
 * setting LRU/LFU through a READ handle): if the object is owed, take the key
 * through the write funnel (a write lookup that emits or clones its bucket)
 * and return the object to write, which may be the clone; else 'kv'. NULL
 * only if the key is gone. */
kvobj *snapPrepareMutation(redisDb *db, robj *key, kvobj *kv) {
    if (!snapKvobjIsOwed(db, kv)) return kv;
    return lookupKeyWriteWithFlags(db, key, LOOKUP_NOTOUCH|LOOKUP_NONOTIFY|LOOKUP_NOSTATS|LOOKUP_NOEXPIRE);
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

/* CONFIG SET rdb-key-save-delay apply hook: reaches the running scan thread. */
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
    if (server.hybrid_snapshot_max_retained_bytes && !sv->hard_mark_hit &&
        sv->retained_bytes > server.hybrid_snapshot_max_retained_bytes) {
        sv->hard_mark_hit = 1;
        serverLog(LL_WARNING, "Hybrid snapshot: overlay retains %zu bytes, past hybrid-snapshot-max-retained-bytes (%zu): aborting the save",
                  sv->retained_bytes, server.hybrid_snapshot_max_retained_bytes);
    }
}

static void snapSaveRetainedSub(kvobj *orig, int value_shared) {
    if (!snap_save) return;
    size_t n = snapOriginalRetainedBytes(orig, value_shared);
    snap_save->retained_bytes = n > snap_save->retained_bytes ? 0 : snap_save->retained_bytes - n;
}

/* Main thread, serverCron/beforeSleep: act on a pending hard-mark abort. */
void snapSaveCron(void) {
    if (!snap_save || snap_save->aborted) return;
    snapSave *sv = snap_save;
    /* Writes may have stopped since the thread passed a bucket: drain from
     * here too, so an overlayed bucket never outlives that by more than a tick. */
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
    if (server.hybrid_snapshot_max_retained_bytes && !sv->hard_mark_hit) {
        if (sv->retained_bytes + queued > server.hybrid_snapshot_max_retained_bytes) {
            sv->hard_mark_hit = 1;
            serverLog(LL_WARNING, "Hybrid snapshot: %zu bytes retained plus %zu queued for the child, past hybrid-snapshot-max-retained-bytes (%zu): aborting the save",
                      sv->retained_bytes, queued, server.hybrid_snapshot_max_retained_bytes);
        }
    }
    if (!sv->hard_mark_hit) return;
    snap_save->failed = 1;
    server.stat_snap_hard_mark_aborts++;
    killRDBChild();   /* -> snapSaveAbort(): drains the overlays; the done handler releases */
}

static void snapSaveProgress(long *slots_total, long *slots_done, long long *keys_nofork,
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

/* ---- Outbound queue to the child (scan thread only) ---- */

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

static void snapOutPublish(snapSave *sv) {
    atomicSet(sv->shared->small_queued, sv->sq_bytes);
}

/* Scan thread: write one channel's queued messages to its (non-blocking)
 * pipe. 0 = the list is empty, 1 = the pipe is full, -1 = broken (child gone). */
static int snapOutDrainList(snapSave *sv, list *l, int fd, size_t *head_off, int small) {
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
                snapOutPublish(sv);
            }
            listDelNode(l, ln);
            snapOutRecycle(sv, m);
            *head_off = 0;
        }
    }
    return 0;
}

/* Scan thread: push both pipes. Returns -1 when a pipe is broken (child
 * gone) or a BOUND wait saw an abort. */
static int snapOutDrain(snapSave *sv, int mode, size_t target) {
    for (;;) {
        int rs = snapOutDrainList(sv, sv->oq, sv->pipe_wr, &sv->oq_head_off, 1);
        int rb = snapOutDrainList(sv, sv->bq, sv->big_wr, &sv->bq_head_off, 0);
        if (rs < 0 || rb < 0) return -1;
        if (rs == 0 && rb == 0) return 0;
        if (mode == SNAP_DRAIN_TRY) return 0;
        if (mode == SNAP_DRAIN_BOUND) {
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
        if (snapScanAborted(sv->scan)) return -1;
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

static void snapOutClearList(list *l) {
    listNode *ln;
    while ((ln = listFirst(l)) != NULL) {
        sdsfree(listNodeValue(ln));
        listDelNode(l, ln);
    }
}

static void snapOutClear(snapSave *sv) {
    snapOutClearList(sv->oq);
    snapOutClearList(sv->bq);
    sv->oq_head_off = sv->bq_head_off = 0;
    sv->sq_bytes = 0;
    atomicSet(sv->oq_bytes, 0);
    snapOutPublish(sv);
}

/* Scan thread: queue a complete message (header + payload in 'm', owned by
 * the queue from here on) on its channel and push the pipes. Past the bound
 * the thread waits for the child rather than growing further; it never aborts
 * for that (the main-thread hard mark covers what the main thread retains and
 * queues). A broken pipe (child gone) aborts the scan. */
static int snapOutEnqueue(snapSave *sv, sds m, uint32_t type) {
    size_t n = sdslen(m);
    int big = (type == SNAP_MSG_KVOBJ || type == SNAP_MSG_END || type == SNAP_MSG_ABORT);
    if (type == SNAP_MSG_BUFFER) {
        if (sv->tag_all) {
            /* Debug: depend on every big value sent so far (inline replication
             * would set the exact dependency per command). */
            snapMsgHdr h;
            memcpy(&h, m, sizeof(h));
            h.robj_off = sv->kv_seq;
            memcpy(m, &h, sizeof(h));
        }
    }
    if (big) {
        listAddNodeTail(sv->bq, m);
        if (type == SNAP_MSG_KVOBJ) sv->kv_seq++;
    } else {
        listAddNodeTail(sv->oq, m);
        sv->sq_bytes += sdsAllocSize(m);
        sv->small_seq++;
        snapOutPublish(sv);
    }
    size_t q, mqb, peak;
    atomicIncrGet(sv->oq_bytes, q, sdsAllocSize(m));   /* what the queues really hold */
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
    if (snapSaveSlotTrimmed(sv, didx)) {
        atomicIncr(sv->keys_skipped, 1);
        return;
    }
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
        snapSaveKeyDelay(sv, 1);
    } else {
        rio r;
        rioInitWithBuffer(&r, sv->stage);
        if (snapSaveWriteRecord(sv, &r, kv, s) != 0) {
            serverLog(LL_WARNING, "Hybrid snapshot: failed to serialize a key on the scan thread");
            snapScanRequestAbort(sv->scan);
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
            snapStageFlush(sv);
            snapSaveKeyDelay(sv, 1);
        } else if (sdslen(sv->stage) + SNAP_STAGE_SLACK > sdsalloc(sv->stage)) {
            snapStageFlush(sv);
        }
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
        /* The delay owed for the keys the main thread emitted ahead (their
         * records were forwarded as the thread went, so the drain was never
         * held back): slept here, before END, so a save of N fork-instant keys
         * still lasts N delays whoever produced the records. */
        long long mk;
        atomicGet(sv->main_keys, mk);
        if (mk > sv->delayed_main_keys) {
            snapSaveKeyDelay(sv, mk - sv->delayed_main_keys);
            sv->delayed_main_keys = mk;
        }
        if (snapScanAborted(sv->scan)) return;
        snapStageFlush(sv);
        /* END carries both channels' message counts: the child finishes when
         * it has consumed them. */
        if (snapSendMsg(sv, SNAP_MSG_END, -1, -1, NULL, 0, sv->kv_seq, sv->small_seq) == 0 &&
            snapOutDrain(sv, SNAP_DRAIN_ALL, 0) != 0)
            snapScanRequestAbort(sv->scan);
    }
}

/* ---- child ---- */

static int snapChildSelect(rio *rdb, snapSave *sv, int dbid, int slot, int *cur_db, int *cur_slot) {
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
static int snapSaveChildWrite(snapSave *sv, rio *out) {
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
        if (snapIsAof(sv))
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
        if (snapIsAof(sv) ? server.aof_rewrite_incremental_fsync : server.rdb_save_incremental_fsync) {
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
     * least SNAP_SMALL_BACKLOG on it (or nothing waits on the big channel), a
     * big value otherwise. The bounded queue then never parks the scan thread
     * while we are inside a big value, and the big values come up as soon as
     * the small side is caught up (a single FIFO took the big values handed
     * ahead by the main thread back to back and lost the overlap). A
     * BUFFER whose dependency (header robj_off: big values that must precede
     * it) is not met waits for the big channel. END carries both channels'
     * message counts: we are done when both are consumed. */
    uint64_t small_total = 0, small_seen = 0, kv_total = 0, kv_written = 0;
    int ended = 0;
    snapMsgHdr held;
    int have_held = 0;                 /* a BUFFER (in 'payload') waiting for big values */
    /* rdb channel: replicas seen hanging up. Like the classic connset writer the
     * transfer goes on for the others and fails only when none is left; the
     * rio skips a dead connection by itself at its next write. */
    char *conn_dead = zcalloc(sv->rdb_channel && sv->numconns ? sv->numconns : 1);
    int ndead = 0;
    for (;;) {
        snapMsgHdr h;
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
            if (!small_ready && big_ready && small_open && backlog >= SNAP_SMALL_BACKLOG) {
                /* The parent holds a small backlog that the scan thread has
                 * not pushed into the pipe yet (it pushes at its own
                 * enqueues, about once a millisecond, a pipe's worth each
                 * time). Wait for it briefly rather than start a big value:
                 * measurements showed the child falling to the next big value at
                 * every such gap, the backlog growing to the bound for the
                 * whole value and the scan thread parked (29-36 s under
                 * writes against 21-24 with an unbounded overflow). */
                struct pollfd one = { .fd = sv->pipe_rd, .events = POLLIN, .revents = 0 };
                int r = poll(&one, 1, SNAP_SMALL_WAIT_MS);
                if (r > 0 && (one.revents & POLLIN)) small_ready = 1;
            }
            src = (small_ready && (backlog >= SNAP_SMALL_BACKLOG || !big_ready)) ? 0 : 1;
        }
        if (src == 1) {
            if (snapReadAll(sv->big_rd, &h, sizeof(h)) != 0) { err_op = "channel read (header)"; goto werr; }
            if (h.type == SNAP_MSG_END) {
                ended = 1;
                kv_total = h.robj_off;
                small_total = h.src_addr;
                if (kv_written != kv_total) { errno = EPROTO; err_op = "big value count"; goto werr; }
                continue;
            }
            if (h.type == SNAP_MSG_ABORT) { aborted = 1; goto werr; }
            if (h.type != SNAP_MSG_KVOBJ) { errno = EPROTO; err_op = "unknown message"; goto werr; }
            if (h.len > bigbuf_cap) {
                bigbuf = zrealloc(bigbuf, h.len);
                bigbuf_cap = h.len;
            }
            if (h.len && snapReadAll(sv->big_rd, bigbuf, h.len) != 0) { err_op = "channel read (payload)"; goto werr; }
            if (snapChildSelect(rdb, sv, h.dbid, h.slot, &cur_db, &cur_slot) != 0) { err_op = "selectdb"; goto werr; }
            kvobj *kv = (kvobj *)(bigbuf + h.robj_off);
            /* An embedded string value lives inside the kvobj allocation: its
             * pointer is the parent's address, relocate it into our copy. */
            if (kv->encoding == OBJ_ENCODING_EMBSTR)
                kv->ptr = bigbuf + ((char *)kv->ptr - (char *)(uintptr_t)h.src_addr);
            size_t before = rdb->processed_bytes;
            if (snapSaveWriteRecord(sv, rdb, kv, h.dbid) != 0) { err_op = "record"; goto werr; }
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
        if (snapReadAll(sv->pipe_rd, &h, sizeof(h)) != 0) { err_op = "channel read (header)"; goto werr; }
        small_seen++;
        if (h.type != SNAP_MSG_BUFFER) { errno = EPROTO; err_op = "unknown message"; goto werr; }
        if (h.len > payload_cap) {
            payload = zrealloc(payload, h.len);
            payload_cap = h.len;
        }
        if (h.len && snapReadAll(sv->pipe_rd, payload, h.len) != 0) { err_op = "channel read (payload)"; goto werr; }
        if (h.robj_off > kv_written) {
            held = h;
            have_held = 1;
            continue;
        }
    write_small:
        if (snapChildSelect(rdb, sv, h.dbid, h.slot, &cur_db, &cur_slot) != 0) { err_op = "selectdb"; goto werr; }
        if (rioWrite(rdb, payload, h.len) == 0) { err_op = "write buffer"; goto werr; }
    progress:
        if ((msgs++ & 1023) == 0) {
            long long now = mstime();
            if (now - info_updated_time >= 1000) {
                sendChildInfo(CHILD_INFO_TYPE_CURRENT_INFO, key_counter, snapIsAof(sv) ? "AOF rewrite" : "RDB");
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
        serverLog(LL_NOTICE, "Hybrid %s transfer done, %ld SNAP_FORK keys serialized by the child, %zu bytes sent.",
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
    if (snapIsAof(sv)) {
        /* The parent's done handler renames it into the manifest and fsyncs the directory. */
        serverLog(LL_NOTICE, "Hybrid AOF rewrite done, %ld SNAP_FORK keys serialized by the child, %zu bytes written to %s.",
                  key_counter, rdb->processed_bytes, sv->filename);
        stopSaving(1);
        return C_OK;
    }
    if (fsyncFileDir(sv->filename) != 0) {
        serverLog(LL_WARNING, "Hybrid snapshot: failed to fsync directory: %s", strerror(errno));
        stopSaving(0);
        return C_ERR;
    }
    serverLog(LL_NOTICE, "Hybrid snapshot done, %ld SNAP_FORK keys serialized by the child, %zu bytes written.",
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

static void snapSaveChild(snapSave *sv) {
    rio sock;
    char aofname[256];
    close(sv->pipe_wr);
    close(sv->big_wr);
    if (snapToSockets(sv)) {
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
    } else if (snapIsAof(sv)) {
        /* The name backgroundRewriteDoneHandler and aofRemoveTempFile expect. */
        snprintf(aofname, sizeof(aofname), "temp-rewriteaof-bg-%d.aof", (int)getpid());
        sv->filename = aofname;
        redisSetProcTitle("redis-aof-rewrite");
        redisSetCpuAffinity(server.aof_rewrite_cpulist);
    } else {
        redisSetProcTitle("redis-rdb-bgsave");
    }
    if (!snapIsAof(sv)) redisSetCpuAffinity(server.bgsave_cpulist);
    int retval = snapSaveChildWrite(sv, snapToSockets(sv) ? &sock : NULL);
    if (retval == C_OK) {
        if (snapIsAof(sv)) sendChildCowInfo(CHILD_INFO_TYPE_AOF_COW_SIZE, "AOF rewrite");
        else sendChildCowInfo(CHILD_INFO_TYPE_RDB_COW_SIZE, "RDB");
    }
    if (snapToSockets(sv)) {
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
        snapOutClearList(sv->oq);
        listRelease(sv->oq);
    }
    if (sv->bq) {
        snapOutClearList(sv->bq);
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
    if (sv->shared) munmap(sv->shared, sizeof(snapShared));
    if (sv->sizes) {
        for (int j = 0; j < sv->dbnum; j++) zfree(sv->sizes[j].slot_keys);   /* slot_expires lives in the same block */
    }
    sdsfree(sv->stage);
    if (sv->mq) {
        snapOutClearList(sv->mq);
        listRelease(sv->mq);
        snapOutClearList(sv->mq_ready);
        listRelease(sv->mq_ready);
        pthread_mutex_destroy(&sv->mq_lock);
    }
    sdsfree(sv->mstage);
    sdsfree(sv->mscratch);
    /* Flushed dbs still held (every db of an AOF rewrite that saw FLUSHALL,
     * when the child ended before their slots completed): free them while
     * sv->stores is still there, snapSaveFreeFlushed clears the entry. */
    for (int j = 0; j < sv->dbnum; j++) snapSaveFreeFlushed(sv, j);   /* normally freed at store completion */
    zfree(sv->bitmaps);
    zfree(sv->tables);
    zfree(sv);
}

/* Overlay callbacks for the server keyspace (privdata = the store's snapGuardPriv). */
static void snapOverlayEnter(void *privdata) { UNUSED(privdata); snapFunnelOpen(); }
static const kvstoreSnapHooks snap_server_hooks = {
    .clone = snapOverlayCloneKey, .free_orig = snapOverlayFreeOriginal, .enter = snapOverlayEnter,
    .prepare = snapOverlayPrepare,
};

/* Main thread: the save object, its channel, queues and scan context for a
 * frozen-to-be keyspace. NULL on failure (logged). */
static snapSave *snapSaveCreate(int kind, int req, char *filename, rdbSaveInfo *rsi, int rdbflags) {
    serverAssert(snap_save == NULL);

    kvstoreOverlayStatsReset();   /* the overlay counters of INFO describe this save */
    snapSave *sv = zcalloc(sizeof(*sv));
    sv->pipe_rd = sv->pipe_wr = sv->big_rd = sv->big_wr = -1;
    sv->dbnum = server.dbnum;
    sv->kind = kind;
    sv->req = req;
    sv->rdbflags = rdbflags;
    sv->filename = filename;
    if (rsi) { sv->rsi = *rsi; sv->has_rsi = 1; }
    sv->tables = zcalloc((sizeof(kvstore *) + sizeof(snapFlushedDb) + sizeof(snapDbSizes)) * sv->dbnum);
    sv->stores = sv->tables;
    sv->flushed = (snapFlushedDb *)(sv->stores + sv->dbnum);
    sv->sizes = (snapDbSizes *)(sv->flushed + sv->dbnum);
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
            snapSaveFree(sv);
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
            snapSaveFree(sv);
            return NULL;
        }
    }
    sv->shared = mmap(NULL, sizeof(snapShared), PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANONYMOUS, -1, 0);
    if (sv->shared == MAP_FAILED) {
        sv->shared = NULL;
        serverLog(LL_WARNING, "Hybrid snapshot: mmap(MAP_SHARED) failed: %s", strerror(errno));
        snapSaveFree(sv);
        return NULL;
    }
    atomicSet(sv->shared->small_queued, 0);
    sv->oq = listCreate();
    sv->bq = listCreate();
    sv->oq_pool = listCreate();
    sv->oq_limit = server.hybrid_snapshot_max_queued_bytes;
    sv->tag_all = server.hybrid_snapshot_tag_all;
    snapStageReset(sv);
    sv->stage_dbid = sv->stage_slot = -1;
    pthread_mutex_init(&sv->mq_lock, NULL);
    sv->mq = listCreate();
    sv->mq_ready = listCreate();
    sv->mstage = snapMsgNew(0);
    sv->mscratch = sdsempty();
    sv->mstage_dbid = sv->mstage_slot = -1;
    atomicSet(sv->key_save_delay, server.rdb_key_save_delay);
    sv->maxmemory_policy = server.maxmemory_policy;
    sv->rdb_compression = (req & (SLAVE_REQ_RDB_NO_COMPRESS | SLAVE_REQ_SLOTS_SNAPSHOT)) ? 0 : server.rdb_compression;   /* a slot snapshot never compresses (slotSnapshotSaveRio) */
    server.stat_snap_saves++;
    sv->scan = snapScanCreate(sv->stores, sv->dbnum, snapSaveDispatch, NULL, sv);
    if (sv->slot_filter) snapScanSetSlotFilter(sv->scan, sv->slot_filter);
    snapScanSetOverlay(sv->scan, &snap_server_hooks, NULL);
    sv->scan->store_done = snapSaveStoreDone;
    sv->scan->thread_done = snapSaveThreadDone;
    sv->scan->thread_init = snapSaveThreadInit;
    sv->scan->window_done = snapSaveWindowDone;
    return sv;
}

/* Main thread: FORK INSTANT. Freeze, fork the writer child, start the scan
 * thread. On a fork failure the keyspace is unfrozen and sv is freed; the
 * caller undoes its own state (replica states, pipes). */
static int snapSaveLaunch(snapSave *sv) {
    if (!snapIsAof(sv)) {
        server.stat_rdb_saves++;
        server.dirty_before_bgsave = server.dirty;
        server.lastbgsave_try = time(NULL);
    }

    /* FORK INSTANT: freeze, then fork. */
    mstime_t freeze_lat;
    latencyStartMonitor(freeze_lat);
    snapScanFreeze(sv->scan);
    latencyEndMonitor(freeze_lat);
    latencyAddSampleIfNeeded("hybrid-freeze", freeze_lat);
    pid_t childpid = snapForkNoKeyspace(snapIsAof(sv) ? CHILD_TYPE_AOF : CHILD_TYPE_RDB);
    if (childpid == 0) {
        snapSaveChild(sv);   /* never returns */
    }
    if (childpid == -1) {
        if (snapIsAof(sv)) server.aof_lastbgrewrite_status = C_ERR;
        else if (!snapToSockets(sv)) server.lastbgsave_status = C_ERR;
        serverLog(LL_WARNING, "Can't save in background (hybrid): fork: %s", strerror(errno));
        snapScanAbort(sv->scan);   /* unfreeze; nothing was scanned */
        snapSaveFree(sv);
        return C_ERR;
    }
    close(sv->pipe_rd);
    sv->pipe_rd = -1;
    close(sv->big_rd);
    sv->big_rd = -1;
    snap_save = sv;
    /* The classic done handlers branch on it; an AOF rewrite leaves it NONE. */
    if (!snapIsAof(sv)) server.rdb_child_type = snapToSockets(sv) ? RDB_CHILD_TYPE_SOCKET : RDB_CHILD_TYPE_DISK;
    if (snapScanStartThread(sv->scan) != C_OK) {
        /* The child is waiting on the channel: kill it, unfreeze. */
        serverLog(LL_WARNING, "Hybrid snapshot: cannot start the scan thread; aborting the save");
        killRDBChild();   /* -> snapSaveAbort() unfreezes; the done handler releases */
    } else {
        aeCreateFileEvent(server.el, snapScanReadEventFd(sv->scan), AE_READABLE, snapScanAeHandler, sv->scan);
        sv->ae_registered = 1;
    }
    if (snapIsAof(sv)) {
        serverLog(LL_NOTICE, "Hybrid AOF rewrite started by pid %ld (scan thread streams the %s base file to the child)",
                  (long)childpid, sv->aof_cmd_form ? "AOF" : "RDB");
    } else {
        serverLog(LL_NOTICE, "Hybrid background saving started by pid %ld (scan thread streams to the child)", (long)childpid);
        server.rdb_save_time_start = time(NULL);
    }
    return C_OK;
}

/* BGREWRITEAOF: the hybrid scan and child write the base file, an
 * RDB with aof-use-rdb-preamble or AOF commands without it (same producers
 * and channels, each record in command form and the child
 * emits SELECT on db change). The caller (rewriteAppendOnlyFileBackground)
 * has flushed the AOF and opened the new INCR file, so the fork instant below
 * is where the INCR file's history starts; backgroundRewriteDoneHandler does
 * the manifest work on the temp file the child leaves behind, as for the classic
 * child. */
static int snapSaveBackgroundAof(void) {
    if (hasActiveChildProcess()) return C_ERR;
    snapSave *sv = snapSaveCreate(SNAP_SAVE_AOF, SLAVE_REQ_NONE, NULL, NULL, RDBFLAGS_AOF_PREAMBLE);
    if (!sv) return C_ERR;
    sv->aof_cmd_form = !server.aof_use_rdb_preamble;
    return snapSaveLaunch(sv);   /* the child names its own temp file */
}

/* The one decision whether a save takes the hybrid path, for every consumer
 * of a fork-based snapshot (rdbSaveBackground, startBgsaveForReplication,
 * rewriteAppendOnlyFileBackground): the switch is on, the request has
 * keyspace content (functions-only transfers keep the classic child; a slot
 * snapshot is a hybrid save over the migrating slots) and every relevant module has opted in (otherwise the fallback
 * is logged once per blocking module). Returns C_OK when the hybrid save
 * started, C_ERR when it was taken and failed to start, SNAP_NOT_TAKEN when
 * the caller must run the classic fork child. */
int snapSaveTryStart(int kind, int req, char *filename, rdbSaveInfo *rsi, int rdbflags) {
    if (!snapHybridEnabled()) return SNAP_NOT_TAKEN;
    /* A functions-only transfer has no keyspace content for the scan thread;
     * the classic child serves it (redisFork maps the arena). A slot snapshot
     * (atomic slot migration) is a hybrid save over the migrating slots. */
    if (req & SLAVE_REQ_RDB_EXCLUDE_DATA) return SNAP_NOT_TAKEN;
    if ((req & SLAVE_REQ_SLOTS_SNAPSHOT) && !asmMigrateInProgress()) return SNAP_NOT_TAKEN;
    /* Scan positions number every dict of every db in SNAP_POS_ORD_BITS bits. */
    if ((long long)server.dbnum * (server.cluster_enabled ? CLUSTER_SLOTS : 1) >= (1LL << SNAP_POS_ORD_BITS)) {
        static int logged = 0;
        if (!logged) serverLog(LL_NOTICE, "Hybrid snapshot: too many databases for the scan positions, the classic child serves the save");
        logged = 1;
        return SNAP_NOT_TAKEN;
    }
    if (!moduleAllModulesHandleHybridSnapshot()) {
        snapHybridFallbackLog();   /* a module's save callbacks need the keyspace */
        return SNAP_NOT_TAKEN;
    }
    switch (kind) {
    case SNAP_SAVE_RDB:     return snapSaveBackground(req, filename, rsi, rdbflags);
    case SNAP_SAVE_SOCKETS: return snapSaveToSlavesSockets(req, rsi);
    case SNAP_SAVE_AOF:     return snapSaveBackgroundAof();
    }
    serverPanic("unknown hybrid save kind %d", kind);
}

/* Start a hybrid background save: freeze the keyspace (overlay), fork the
 * writer child, spawn the scan thread. Same contract as rdbSaveBackground(). */
static int snapSaveBackground(int req, char *filename, rdbSaveInfo *rsi, int rdbflags) {
    if (hasActiveChildProcess()) return C_ERR;
    if (snapArenaForkable()) {
        /* Only a Lua debugger session can leave the arena forkable with no
         * exclusive child around. The hybrid child never touches the arena, so
         * mapping it is harmless; it just pays copy-on-write on it meanwhile. */
        serverLog(LL_NOTICE, "Hybrid snapshot: the SNAP_NOFORK arena is forkable (debugger child); the save child will map it");
    }
    snapSave *sv = snapSaveCreate(SNAP_SAVE_RDB, req, filename, rsi, rdbflags);
    if (!sv) return C_ERR;
    return snapSaveLaunch(sv);
}

/* Diskless full sync through the hybrid save: the same scan and
 * channel as a BGSAVE, but the child writes the replicas instead of a file.
 * Mirrors rdbSaveToSlavesSockets(): replicas in WAIT_BGSAVE_START with this
 * exact 'req' are set up for full resync at the current offset just before
 * the freeze-and-fork, so the offset is the snapshot's; with the rdb channel
 * the child writes the (blocking) replica connections directly, otherwise it
 * writes the classic rdb pipe that the parent fans out to the sockets, and
 * waits on the safe-to-exit pipe before exiting. The classic socket done
 * handler (RDB_CHILD_TYPE_SOCKET) does the replica bookkeeping afterwards. */
static int snapSaveToSlavesSockets(int req, rdbSaveInfo *rsi) {
    listIter li;
    listNode *ln;
    int pipefds[2];
    int rdb_channel = server.repl_rdb_channel && (req & SLAVE_REQ_RDB_CHANNEL);

    if (hasActiveChildProcess()) return C_ERR;
    /* Even if the previous fork child exited, don't start a new one until we
     * drained the pipe. */
    if (server.rdb_pipe_conns) return C_ERR;
    if (snapArenaForkable())
        serverLog(LL_NOTICE, "Hybrid snapshot: the SNAP_NOFORK arena is forkable (debugger child); the save child will map it");

    snapSave *sv = snapSaveCreate(SNAP_SAVE_SOCKETS, req, NULL, rsi, RDBFLAGS_REPLICATION);
    if (!sv) return C_ERR;
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
void snapSaveAbort(void) {
    if (!snap_save || snap_save->aborted) return;
    snapSave *sv = snap_save;
    sv->aborted = 1;
    /* Stops the thread within one cursor step, joins it, completes every slot.
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
    serverLog(LL_NOTICE, "Hybrid snapshot: scan aborted, keyspace overlay drained.");
}

/* The one child-end hook: called once for every child that ended, whatever
 * its type and whether it was reaped (checkChildrenDone) or killed
 * (killAppendOnlyChild), before the type's done handler runs. A no-op when
 * no hybrid save owns the child; otherwise stops the scan if the child failed
 * or the scan is unfinished, joins the thread, releases every state and
 * publishes the last save's peaks. */
void snapOnChildEnd(int exitcode, int bysignal) {
    if (!snap_save) return;
    snapSave *sv = snap_save;
    /* The child is gone. If the scan did not complete (or the thread stopped
     * on its own when the channel broke, leaving slots owed), stop it and
     * complete every remaining slot. Harmless after a clean finish. */
    if (!(!bysignal && exitcode == 0) || !snapScanIsFinished(sv->scan))
        snapScanAbort(sv->scan);
    if (sv->failed && !snapToSockets(sv) && !snapIsAof(sv)) {
        /* Our own defined failure (hard mark): the child was ended with the
         * whitelisted SIGUSR1, which the plain done handler does not count.
         * A socket transfer reports through the replicas, as the classic save. */
        server.lastbgsave_status = C_ERR;
        server.stat_rdb_consecutive_failures++;
    }
    snap_last_retained_peak = sv->retained_peak;
    atomicGet(sv->queued_peak, snap_last_queued_peak);
    snap_last_inplace_writes = sv->inplace_writes;
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
                  snapIsAof(sv) ? "AOF rewrite" : sv->slots_form ? "Slot snapshot" : "BGSAVE",
                  nofork + fork + main, skipped);
    }
    snap_save = NULL;
    snapSaveFree(sv);   /* drains completions, joins, unregisters */
    serverLog(LL_NOTICE, "Hybrid snapshot: scan thread joined and overlay released.");
}

/* ---- Unit tests ---------------------------------------------------------- */
#ifdef REDIS_TEST
/* Test helper: 1 while slot (store, dict) is not DONE. */
static int snapScanKeyIsOwed(snapScanCtx *ctx, int store_idx, int didx) {
    return snapScanSlotState(ctx, store_idx, didx) != SNAP_SLOT_DONE;
}
#include "testhelp.h"
#include <sys/wait.h>
#include <time.h>

/* Test declarations. */
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

struct ovTestCounters { int clones; int frees; };
static void ovTestScanCb(void *priv, const dictEntry *de, dictEntry **plink);

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

/* Fork a child that exercises the allocator (zmalloc + zmalloc_nofork are both
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
#ifdef HAVE_NOFORK_ARENA
    char buf[64];
    snprintf(buf, sizeof(buf), "arena.%u.purge", zmalloc_nofork_arena_index());
    je_mallctl(buf, NULL, 0, NULL, 0);
    snprintf(buf, sizeof(buf), "arena.%u.decay", zmalloc_nofork_arena_index());
    je_mallctl(buf, NULL, 0, NULL, 0);
#endif
}


/* ---- Scan tests ---- */


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
    if (s == 0 && didx == 3 && st->stores) {
        dict *d = kvstoreGetDict(st->stores[0], 3);
        st->rehashing_at_done = d ? dictIsRehashing(d) : 0;   /* NULL once the store let go of it */
    }
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


/* Tests without a scan thread: put the window where a thread and its
 * grants would have it. */
static void snapTestWindow(snapScanCtx *ctx, uint64_t lo, uint64_t hi) {
    __atomic_store_n(&snap_lo, lo, __ATOMIC_SEQ_CST);
    __atomic_store_n(&snap_hi, hi, __ATOMIC_SEQ_CST);
    ctx->hi = hi;
}

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
    test_cond("scan: nothing is owed before start", snapTestAllState(ctx, SNAP_SLOT_DONE));
    snapScanFreeze(ctx);   /* guard installed, no thread yet: the checks below are deterministic */
    {
        /* On-demand freeze: the freeze pauses nothing; the
         * first write-intent access to a slot the thread has not finished
         * pauses that dict (guard). A read of a dict the thread is not
         * reading does not. */
        dict *d = kvstoreGetDict(stores[0], 0);
        test_cond("scan: no dict is paused right after start", d->pauserehash == 0 && d->pauseAutoResize == 0);
        char *k0 = snapTestKey(0, 0, 0);   /* store 0 dict 0 holds one key */
        dictEntryLink b = NULL;
        dictEntryLink l = kvstoreDictFindLink(stores[0], 0, k0, &b);
        test_cond("scan: a write-intent lookup on an unreached dict attaches it (bitmap) without pausing it and finds the key",
                  l != NULL && d->pauserehash == 0 && d->pauseAutoResize == 0 && snapScanBitmapBytes(ctx) > 0);
        zfree(k0);
        /* An attached, unreached dict may grow, and its bitmap
         * follows (bucket i of n -> i + j*n of k*n); it may not shrink. */
        dict *big = kvstoreGetDict(stores[0], 2);   /* 5000 keys */
        while (dictIsRehashing(big)) dictRehash(big, 1000);   /* start from one table (it is not attached yet) */
        char *kb = snapTestKey(0, 2, 1);
        kvstoreDictFindLink(stores[0], 2, kb, &b);
        zfree(kb);
        unsigned long n0 = DICTHT_SIZE(big->ht_size_exp[0]);
        snapScanMarkBucketEmitted(ctx, 0, 2, 0, 3);
        snapScanMarkBucketEmitted(ctx, 0, 2, 0, n0 - 1);
        size_t bm_before = snapScanBitmapBytes(ctx);
        int r1 = dictExpand(big, n0 * 16);   /* allowed: not paused */
        unsigned long n1 = DICTHT_SIZE(big->ht_size_exp[1]);
        int ok = r1 == DICT_OK && dictIsRehashing(big) && n1 == n0 * 16 && snapScanBitmapBytes(ctx) > bm_before;
        for (unsigned long j = 0; ok && j < 16; j++)
            ok = snapScanBucketEmitted(ctx, 0, 2, 1, 3 + j * n0) && snapScanBucketEmitted(ctx, 0, 2, 1, n0 - 1 + j * n0) &&
                 !snapScanBucketEmitted(ctx, 0, 2, 1, 4 + j * n0);
        test_cond("scan: an unreached dict expands and its bitmap is replicated into the new table", ok);
        while (dictIsRehashing(big)) dictRehash(big, 1000);   /* finish the rehash: the new bitmap becomes the table's */
        test_cond("scan: after the rehash the carried bitmap is the dict's only one",
                  snapScanBucketEmitted(ctx, 0, 2, 0, 3 + n0) && snapScanBucketEmitted(ctx, 0, 2, 0, n0 - 1 + 2 * n0) &&
                  !snapScanBucketEmitted(ctx, 0, 2, 0, 4) && snapScanBitmapBytes(ctx) == bm_before - (n0 + 7) / 8 + (n1 + 7) / 8);
        /* 5000 keys in 16 x their table: the fill ratio asks for a shrink, the snapshot refuses it. */
        signed char exp_before = big->ht_size_exp[0];
        int r2 = dictShrinkIfNeeded(big);
        test_cond("scan: an attached dict refuses to shrink", r2 == DICT_ERR && !dictIsRehashing(big) && big->ht_size_exp[0] == exp_before);
        /* The marks above named buckets that hold keys: clear them so the walk
         * below dispatches every key (the thread skips marked buckets). */
        snapDictState *bsn = ((kvstoreDictMetaBase *)dictMetadata(big))->ext;
        memset(bsn->bits[0], 0, bsn->bytes[0]);
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
    /* Slots empty at the fork instant have no dict (allocate on demand) and
     * are never completed; every populated one completes once through the ring. */
    int done_once = 1, empty_slots = 0;
    for (int s = 0; s < SNAP_TEST_STORES; s++)
        for (int didx = 0; didx < SNAP_TEST_DICTS; didx++) {
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
              done_once && completions == SNAP_TEST_STORES * SNAP_TEST_DICTS - empty_slots);
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
        printf("scan: progress total %ld scanned %ld done %ld keys %lld (empty slots %d)\n", t, sc, dn, kd, empty_slots);
        test_cond("scan: progress counters consistent (the allocated dicts)",
                  t == SNAP_TEST_STORES * SNAP_TEST_DICTS - empty_slots && sc == t && dn == t && kd == total_keys);
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
    int aborted_slots = 0, normal_slots = 0, calls = 0, twice = 0;
    for (int s = 0; s < SNAP_TEST_STORES; s++)
        for (int didx = 0; didx < SNAP_TEST_DICTS; didx++) {
            calls += st->done_calls[s][didx];
            if (st->done_calls[s][didx] > 1) twice++;
            if (!st->done_calls[s][didx]) continue;   /* not reached: the save never touched it */
            if (st->done_aborted[s][didx]) aborted_slots++; else normal_slots++;
        }
    printf("abort: after %ld dispatched keys, abort took %lld ms; %d slots aborted, %d completed normally\n",
           dispatched, abort_ms, aborted_slots, normal_slots);
    test_cond("abort: stopped well before the scan would have finished", dispatched < total_keys);
    test_cond("abort: returned promptly (< 500 ms)", abort_ms < 500);
    test_cond("abort: thread finished", snapScanIsFinished(ctx));
    test_cond("abort: no slot completed twice, the reached ones once", twice == 0 && calls == aborted_slots + normal_slots && calls <= SNAP_TEST_STORES * SNAP_TEST_DICTS);
    test_cond("abort: interrupted slots reported as aborted", aborted_slots >= 1);
    test_cond("abort: slots scanned before the abort completed normally", normal_slots >= 1);
    test_cond("abort: all slots DONE", snapTestAllState(ctx, SNAP_SLOT_DONE));
    test_cond("abort: all dicts unfrozen", snapTestAllUnfrozen(stores));
    snapScanRelease(ctx);

    /* ---- Credit: inside a section the thread reads only its grant ---- */
    memset(st->seen, 0, sizeof(st->seen));
    memset(st->done_calls, 0, sizeof(st->done_calls));
    memset(st->done_aborted, 0, sizeof(st->done_aborted));
    atomicSet(st->dispatched, 0);
    atomicSet(st->bad, 0);
    st->dispatch_sleep_us = 0;
    unsigned long saved_w = snapGetWindowBuckets();
    snapSetWindowBuckets(64);
    ctx = snapScanCreate(stores, SNAP_TEST_STORES, snapTestDispatch, snapTestSlotDone, st);
    st->ctx = ctx;
    snapFunnelOpen();
    test_cond("credit: start with a write section open", snapScanStart(ctx) == C_OK);
    long long stalls = 0;
    for (int i = 0; i < 2000 && stalls == 0; i++) { usleep(1000); atomicGet(ctx->stalls, stalls); }
    long first;
    atomicGet(st->dispatched, first);
    usleep(20000);
    atomicGet(st->dispatched, dispatched);
    test_cond("credit: the thread read its grant and waits at hi (64 buckets: d0, then part of d2)",
              stalls == 1 && first > 0 && dispatched == first && first < total_keys && !snapScanIsFinished(ctx));
    snapFunnelOpen();   /* a later hook of the same section: the thread is past mid, the next 64 buckets are granted */
    long long stalls2 = stalls;
    for (int i = 0; i < 2000 && stalls2 == stalls; i++) { usleep(1000); atomicGet(ctx->stalls, stalls2); }
    atomicGet(st->dispatched, dispatched);
    test_cond("credit: a hook inside the section raised hi, the thread read on and waits again",
              ctx->grants == 1 && stalls2 == 2 && dispatched > first && dispatched < total_keys);
    snapFunnelClose();   /* no limit outside sections */
    while (!snapScanIsFinished(ctx)) usleep(100);
    snapScanHandleCompletions(ctx);
    atomicGet(st->dispatched, dispatched);
    atomicGet(st->bad, bad);
    atomicGet(ctx->stalls, stalls);
    test_cond("credit: after the section closed the thread ran to the end, every key once, no further wait",
              dispatched == total_keys && bad == 0 && stalls == 2);
    snapScanRelease(ctx);
    snapSetWindowBuckets(saved_w);

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

    /* ---- Positions and grants (no thread) ---- */
    ctx = snapScanCreate(stores, SNAP_TEST_STORES, snapTestDispatch, snapTestSlotDone, st);
    st->ctx = ctx;
    for (int s2 = 0; s2 < SNAP_TEST_STORES; s2++)
        for (int didx = 0; didx < SNAP_TEST_DICTS; didx++) {
            dict *d = kvstoreGetDict(stores[s2], didx);
            while (d && dictIsRehashing(d)) dictRehash(d, 1000);   /* one table each, then one rehash set up below */
        }
    dict *r12 = kvstoreGetDict(stores[1], 2);   /* 100 keys */
    unsigned long r12n = DICTHT_SIZE(r12->ht_size_exp[0]);
    dictExpand(r12, r12n * 2);
    dictRehash(r12, 5);
    snapScanFreeze(ctx);
    {
        uint64_t o00 = snapOrd(ctx, 0, 0), o02 = snapOrd(ctx, 0, 2), o03 = snapOrd(ctx, 0, 3);
        uint64_t o10 = snapOrd(ctx, 1, 0), o11 = snapOrd(ctx, 1, 1), o12 = snapOrd(ctx, 1, 2);
        test_cond("positions: the scan order is integer order (layout, table 0, table 1; dicts; stores)",
                  SNAP_POS(o02, 0, 0) < SNAP_POS(o02, 1, 0) && SNAP_POS(o02, 1, (1UL << SNAP_POS_IDX_BITS) - 1) < SNAP_POS(o02, 2, 0) &&
                  SNAP_POS(o02, 2, (1UL << SNAP_POS_IDX_BITS) - 1) < SNAP_POS(o03, 0, 0) && SNAP_POS(o03, 2, 0) < SNAP_POS(o10, 0, 0) &&
                  ctx->pos_end > SNAP_POS(snapOrd(ctx, 1, 3), 2, 0));
        dict *d00 = kvstoreGetDict(stores[0], 0), *d02 = kvstoreGetDict(stores[0], 2), *d03 = kvstoreGetDict(stores[0], 3);
        unsigned long n00 = DICTHT_SIZE(d00->ht_size_exp[0]), n02 = DICTHT_SIZE(d02->ht_size_exp[0]);
        uint64_t mid, h = snapWindowAdvance(ctx, SNAP_POS(o00, 1, 0), n00 + 2, &mid);
        test_cond("grants: the empty slot (no dict) is a gap: a grant past dict 0 goes on in dict 2",
                  h == SNAP_POS(o02, 1, 2) && mid == SNAP_POS(o00, 1, (n00 + 2) / 2) &&
                  d00->pauserehash == 1 && d02->pauserehash == 1 && d03->pauserehash == 0);
        h = snapWindowAdvance(ctx, SNAP_POS(o02, 1, n02 - 3), 5, &mid);
        test_cond("grants: a grant crossing into the next dict pauses it first and counts from its table 0",
                  h == SNAP_POS(o03, 1, 2) && d03->pauserehash == 1 && d02->pauserehash == 1);
        unsigned long rehashidx = (unsigned long)r12->rehashidx;
        h = snapWindowAdvance(ctx, SNAP_POS(o12, 0, 0), (r12n - rehashidx) + 3, &mid);
        test_cond("grants: a rehashing dict counts table 0 from rehashidx, then table 1",
                  rehashidx > 0 && dictIsRehashing(r12) && h == SNAP_POS(o12, 2, 3) && r12->pauserehash == 1);
        /* The cap, with the thread found past the grant (it ran free outside
         * sections and claimed store 1's dict 1). */
        dict *d11 = kvstoreGetDict(stores[1], 1);
        snapWordStore(d11, SNAP_WORD(ctx->gen, SNAP_SLOT_SCANNING));
        __atomic_store_n(&snap_lo, SNAP_POS(o00, 1, 0), __ATOMIC_SEQ_CST);
        __atomic_store_n(&snap_cur, SNAP_POS(o11, 1, 5), __ATOMIC_SEQ_CST);
        snapWindowCap(ctx);
        uint64_t c1 = SNAP_POS(o11, 1, 5) + 1;
        test_cond("cap: a thread found past H gets hi = cur + 1, and the dict it is in is paused",
                  ctx->hi == c1 && __atomic_load_n(&snap_hi, __ATOMIC_SEQ_CST) == c1 && d11->pauserehash == 1);
        int pos;
        snapFunnelOpen();
        int in = snapBucketOwed(ctx, 1, 1, d11, 0, 3, &pos, NULL);
        int p_in = pos;
        int ahead = snapBucketOwed(ctx, 1, 1, d11, 0, 6, &pos, NULL);
        int p_ahead = pos;
        snapFunnelClose();
        test_cond("cap: below cur + 1 the bucket may be being read, at it the thread cannot be",
                  in && p_in == SNAP_POS_INSIDE && ahead && p_ahead == SNAP_POS_AHEAD);
        (void)o10;
    }
    snapScanAbort(ctx);   /* frozen, never started: unfreezes and unpauses */
    snapScanRelease(ctx);
    snapWindowReset(NULL);

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
        snapScanAttach(ctx, 1, 0, d, 0);   /* what the guard does on the first write to an unreached slot */
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
        dict *d = kvstoreGetDict(stores[0], 3);   /* populated */
        unsigned long size = DICTHT_SIZE(d->ht_size_exp[0]), mid = size / 2;
        uint64_t o = snapOrd(ctx, 0, 3), b0 = SNAP_POS(o, 1, 0);
        int pos, p_ahead, p_after, p_other, p_behind;
        test_cond("owed: test dict exists and has at least 8 buckets", d != NULL && size >= 8);
        snapScanAttach(ctx, 0, 3, d, 1);            /* bitmap and pause, as the guard would */
        snapWordStore(d, SNAP_WORD(ctx->gen, SNAP_SLOT_SCANNING));   /* as the thread's claim would */
        snapTestWindow(ctx, b0, b0 + mid);          /* the thread may read [0, mid) */
        snapFunnelOpen();                           /* a command opens its write section */
        int owed_ahead = snapBucketOwed(ctx, 0, 3, d, 0, mid + 1, &pos, NULL); p_ahead = pos;
        snapScanMarkBucketEmitted(ctx, 0, 3, 0, mid + 1);   /* ... and emits the bucket */
        snapTestWindow(ctx, b0 + mid, b0 + size);   /* a grant later in the same section moves the window over it */
        int owed_after = snapBucketOwed(ctx, 0, 3, d, 0, mid + 1, &pos, NULL); p_after = pos;
        int owed_other = snapBucketOwed(ctx, 0, 3, d, 0, mid + 2, &pos, NULL); p_other = pos;
        int owed_behind = snapBucketOwed(ctx, 0, 3, d, 0, 0, &pos, NULL); p_behind = pos;
        snapFunnelClose();
        test_cond("owed: a bucket ahead of the window and not emitted is owed", owed_ahead && p_ahead == SNAP_POS_AHEAD);
        test_cond("owed: the same bucket, emitted, is not owed after the window moved over it", !owed_after && p_after == SNAP_POS_INSIDE);
        test_cond("owed: an unemitted bucket inside the window is owed", owed_other && p_other == SNAP_POS_INSIDE);
        test_cond("owed: a bucket behind the window is not owed", !owed_behind && p_behind == SNAP_POS_BEHIND);

        /* The layout phase: the thread has claimed the dict and reads its table
         * sizes, no chain yet (hi just past the layout position). Every bucket
         * of every table is ahead of it, so a write emits the bucket first and
         * then goes in place. */
        snapTestWindow(ctx, SNAP_POS(o, 0, 0), SNAP_POS(o, 0, 0) + 1);
        snapFunnelOpen();
        int layout_ahead = 1, layout_owed = 1;
        for (int t = 0; t < 2; t++) {
            if (!d->ht_table[t]) continue;
            unsigned long tsize = DICTHT_SIZE(d->ht_size_exp[t]);
            unsigned long probe[3] = { 0, tsize / 2, tsize - 1 };
            for (int j = 0; j < 3; j++) {
                int owed = snapBucketOwed(ctx, 0, 3, d, t, probe[j], &pos, NULL);
                if (pos != SNAP_POS_AHEAD) layout_ahead = 0;
                if (!owed) layout_owed = 0;
            }
        }
        snapFunnelClose();
        snapWindowReset(NULL);
        test_cond("owed: in the layout phase every bucket of every table is ahead of the window and owed",
                  layout_ahead && layout_owed);
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
        test_cond("empty: nothing to scan, nothing visited, zero dispatches", et == 0 && edn == 0 && esc == 0 && dispatched == 0);
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
        char *p = zmalloc_nofork(100);
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
        for (int i = 0; i < N; i++) a[i] = zmalloc_nofork(64);
        for (int i = 0; i < N; i++) zfree(a[i]);                     /* generic zfree */
        int leaked = 0;
        for (int i = 0; i < N; i++) { a[i] = zmalloc(64); if (zmalloc_is_nofork(a[i])) leaked++; }
        for (int i = 0; i < N; i++) zfree(a[i]);
        test_cond("arena blocks freed with plain zfree() never come back from zmalloc()", leaked == 0);
        for (int i = 0; i < N; i++) a[i] = zmalloc_nofork(64);
        for (int i = 0; i < N; i++) zfree_with_size(a[i], 64);       /* sized generic free */
        leaked = 0;
        for (int i = 0; i < N; i++) { a[i] = zmalloc(64); if (zmalloc_is_nofork(a[i])) leaked++; }
        for (int i = 0; i < N; i++) zfree(a[i]);
        test_cond("arena blocks freed with zfree_with_size() never come back from zmalloc()", leaked == 0);
        zfree(a);
#if defined(HAVE_NOFORK_ARENA) && defined(HAVE_DEFRAG)
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

static void *ovTestClone(void *priv, int didx, void *orig) {
    UNUSED(didx);
    struct ovTestCounters *c = ((struct snapGuardPriv *)priv)->user;
    c->clones++;
    return zstrdup(orig);
}

static void ovTestFree(void *priv, int didx, void *orig) {
    UNUSED(didx);
    struct ovTestCounters *c = ((struct snapGuardPriv *)priv)->user;
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
static void snapRefcountTests(void) {
    dictType noval = KvstoreDictTestType;
    noval.no_value = 1;
    noval.keyCompare = snapTestKeyCompare;
    kvstore *stores[1];
    stores[0] = kvstoreCreate(&KvstoreTestType, &noval, SNAP_TEST_DICT_BITS, KVSTORE_ALLOCATE_DICTS_ON_DEMAND);
    kvstore *ks = stores[0];
    /* dict 0 and 3: fork-instant content; dict 1: allocated but empty; dict 2: not allocated. */
    for (int i = 0; i < 50; i++) {
        serverAssert(kvstoreDictAddRaw(ks, 0, snapTestKey(0, 0, i), NULL) != NULL);
        serverAssert(kvstoreDictAddRaw(ks, 3, snapTestKey(0, 3, i), NULL) != NULL);
    }
    serverAssert(kvstoreDictAddRaw(ks, 1, snapTestKey(0, 1, 0), NULL) != NULL);
    serverAssert(kvstoreDictDelete(ks, 1, "k:0:1:0") == DICT_OK);
    struct snapScanTestState *st = zcalloc(sizeof(*st));
    st->inserted[0][0] = st->inserted[0][3] = 50;
    st->stores = stores;
    struct ovTestCounters cnt = {0, 0};
    void *privs[1] = { &cnt };
    kvstoreSnapHooks hooks = { .clone = ovTestClone, .free_orig = ovTestFree, .enter = snapOverlayEnter };
    long dispatched, bad, t, sc, dn;
    long long kd;

    /* A credit of one bucket: each save below starts inside a section, so the
     * thread reads a bucket of dict 0 per guard hook and never reaches dict 3
     * before the section closes. */
    unsigned long saved_w = snapGetWindowBuckets();
    snapSetWindowBuckets(1);

    /* ---- A: the table and the references; a dict moved out during the save ---- */
    snapScanCtx *ctx = snapScanCreate(stores, 1, snapTestDispatch, snapTestSlotDone, st);
    st->ctx = ctx;
    snapScanSetOverlay(ctx, &hooks, privs);
    snapFunnelOpen();   /* the thread starts capped */
    test_cond("refs: start with a section open", snapScanStart(ctx) == C_OK);
    dict *d0 = kvstoreGetDict(ks, 0), *d3 = kvstoreGetDict(ks, 3);
    test_cond("refs: the save holds one reference on every non-empty dict, none on an empty one",
              kvstoreDictRefcount(d0) == 2 && kvstoreDictRefcount(d3) == 2 && kvstoreDictRefcount(kvstoreGetDict(ks, 1)) == 1);
    test_cond("refs: an empty or missing dict holds nothing of the save: DONE from the start",
              snapScanSlotState(ctx, 0, 3) == SNAP_SLOT_PENDING && snapScanSlotState(ctx, 0, 1) == SNAP_SLOT_DONE &&
              !snapScanKeyIsOwed(ctx, 0, 2) && kvstoreRefcount(ks) == 1 + 3);
    serverAssert(kvstoreDictAddRaw(ks, 1, snapTestKey(0, 1, 5), NULL) != NULL);   /* post-fork keys the dispatch would flag as bad */
    serverAssert(kvstoreDictAddRaw(ks, 2, snapTestKey(0, 2, 5), NULL) != NULL);
    test_cond("refs: writes to dicts outside the table attach nothing",
              snapScanSlotState(ctx, 0, 1) == SNAP_SLOT_DONE && snapScanSlotState(ctx, 0, 2) == SNAP_SLOT_DONE &&
              !kvstoreOverlayIsFrozen(ks, 1) && !kvstoreOverlayIsFrozen(ks, 2) &&
              snapMeta(kvstoreGetDict(ks, 1))->ext == NULL && snapMeta(kvstoreGetDict(ks, 2))->ext == NULL);
    /* A write to a table dict the thread has not reached attaches it (an overlayed bucket whose duplicate chain lacks the key). */
    serverAssert(kvstoreDictDelete(ks, 3, "k:0:3:7") == DICT_OK);
    test_cond("refs: the write attached dict 3", kvstoreOverlayIsFrozen(ks, 3) && kvstoreDictSize(ks, 3) == 49);
    /* Move dict 3 to another store while the save holds it (the slot-trim path
     * of atomic slot migration), then release that store the way BIO does. */
    kvstore *tmp = kvstoreCreate(&KvstoreTestType, &noval, SNAP_TEST_DICT_BITS, KVSTORE_ALLOCATE_DICTS_ON_DEMAND);
    kvstoreMoveDict(ks, tmp, 3);
    test_cond("refs: the moved dict left the slot with its logical size; the save still holds it",
              kvstoreGetDict(ks, 3) == NULL && kvstoreGetDict(tmp, 3) == d3 && kvstoreDictRefcount(d3) == 2 &&
              kvstoreSize(ks) == 52 && kvstoreSize(tmp) == 49 && kvstoreDictDetached(d3) &&
              snapScanSlotState(ctx, 0, 3) == SNAP_SLOT_PENDING);
    kvstoreRelease(tmp);
    test_cond("refs: the released store left the dict to the save", kvstoreDictRefcount(d3) == 1 && d3->type->userdata == tmp);
    snapFunnelClose();
    while (!snapScanIsFinished(ctx)) { snapScanHandleCompletions(ctx); usleep(100); }
    snapScanHandleCompletions(ctx);
    atomicGet(st->dispatched, dispatched);
    atomicGet(st->bad, bad);
    snapScanProgress(ctx, &t, &sc, &dn, &kd);
    test_cond("refs: the fork-instant keys of both dicts were dispatched, the moved one included; the post-fork ones never",
              dispatched == 100 && bad == 0 && kd == 100 && st->seen[0][3][7]);
    test_cond("refs: two table entries, both completed, the moved dict freed by the save",
              t == 2 && sc == 2 && dn == 2 && st->done_calls[0][0] == 1 && st->done_calls[0][3] == 1 &&
              snapScanRetainedDicts(ctx) == 1 && !snapScanKeyIsOwed(ctx, 0, 3));
    snapScanRelease(ctx);

    /* ---- B: the store emptied (FLUSH) while the save reads it ---- */
    memset(st->seen, 0, sizeof(st->seen));
    memset(st->done_calls, 0, sizeof(st->done_calls));
    atomicSet(st->dispatched, 0);
    atomicSet(st->bad, 0);
    st->inserted[0][1] = st->inserted[0][2] = 6;   /* k:0:1:5 and k:0:2:5 are fork-instant now */
    ctx = snapScanCreate(stores, 1, snapTestDispatch, snapTestSlotDone, st);
    st->ctx = ctx;
    snapScanSetOverlay(ctx, &hooks, privs);
    snapFunnelOpen();
    test_cond("refs: second save starts", snapScanStart(ctx) == C_OK);
    d0 = kvstoreGetDict(ks, 0);
    kvstoreEmpty(ks, NULL);
    test_cond("refs: emptying the store hands its fork-instant dicts to the save",
              kvstoreSize(ks) == 0 && kvstoreGetDict(ks, 0) == NULL && kvstoreDictRefcount(d0) == 1 && kvstoreDictDetached(d0) &&
              snapScanSlotState(ctx, 0, 0) != SNAP_SLOT_DONE);
    serverAssert(kvstoreDictAddRaw(ks, 0, snapTestKey(0, 0, 60), NULL) != NULL);   /* a fresh dict in the slot */
    test_cond("refs: the slot's new dict is not the save's", kvstoreGetDict(ks, 0) != d0 && !kvstoreOverlayIsFrozen(ks, 0));
    snapFunnelClose();
    while (!snapScanIsFinished(ctx)) { snapScanHandleCompletions(ctx); usleep(100); }
    snapScanHandleCompletions(ctx);
    atomicGet(st->dispatched, dispatched);
    atomicGet(st->bad, bad);
    snapScanProgress(ctx, &t, &sc, &dn, &kd);
    test_cond("refs: the detached dicts were walked to the end and freed by the save; the new key never dispatched",
              dispatched == 52 && bad == 0 && t == 3 && dn == 3 && snapScanRetainedDicts(ctx) == 3 && kvstoreSize(ks) == 1);
    snapScanRelease(ctx);

    /* ---- C: a table dict that empties under the save, in a store that frees empty dicts ---- */
    kvstore *fs = kvstoreCreate(&KvstoreTestType, &noval, SNAP_TEST_DICT_BITS, KVSTORE_ALLOCATE_DICTS_ON_DEMAND | KVSTORE_FREE_EMPTY_DICTS);
    kvstore *fstores[1] = { fs };
    serverAssert(kvstoreDictAddRaw(fs, 3, snapTestKey(0, 3, 0), NULL) != NULL);
    for (int i = 0; i < 20; i++) serverAssert(kvstoreDictAddRaw(fs, 0, snapTestKey(0, 0, i), NULL) != NULL);
    memset(st->seen, 0, sizeof(st->seen));
    memset(st->done_calls, 0, sizeof(st->done_calls));
    atomicSet(st->dispatched, 0);
    atomicSet(st->bad, 0);
    st->inserted[0][0] = 20;
    st->inserted[0][1] = st->inserted[0][2] = 0;
    st->inserted[0][3] = 1;
    st->stores = fstores;
    ctx = snapScanCreate(fstores, 1, snapTestDispatch, snapTestSlotDone, st);
    st->ctx = ctx;
    snapScanSetOverlay(ctx, &hooks, privs);
    snapFunnelOpen();
    test_cond("refs: third save starts", snapScanStart(ctx) == C_OK);
    dict *f3 = kvstoreGetDict(fs, 3);
    serverAssert(kvstoreDictDelete(fs, 3, "k:0:3:0") == DICT_OK);   /* deferred: an overlayed bucket without the key, the dict logically empty */
    kvstoreFreeDictIfNeeded(fs, 3);
    /* The overlayed bucket holds the dict's layout, so the emptied dict is not freed yet: it goes at the release. */
    test_cond("refs: the emptied dict is logically empty and stays with the save until the overlay is released",
              kvstoreGetDict(fs, 3) == f3 && kvstoreDictSize(fs, 3) == 0 && kvstoreDictRefcount(f3) == 2 &&
              !kvstoreDictDetached(f3) && snapScanSlotState(ctx, 0, 3) == SNAP_SLOT_PENDING && kvstoreSize(fs) == 20);
    snapFunnelClose();
    while (!snapScanIsFinished(ctx)) { snapScanHandleCompletions(ctx); usleep(100); }
    snapScanHandleCompletions(ctx);
    atomicGet(st->dispatched, dispatched);
    atomicGet(st->bad, bad);
    test_cond("refs: its fork-instant key was still dispatched; the delete written back at release, the dict freed",
              dispatched == 21 && bad == 0 && snapScanRetainedDicts(ctx) == 1 && kvstoreGetDict(fs, 3) == NULL);
    snapScanRelease(ctx);
    snapSetWindowBuckets(saved_w);
    kvstoreRelease(fs);
    kvstoreRelease(ks);
    zfree(st);
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
    struct ovTestCounters cnt = {0, 0};
    void *privs[1] = { &cnt };

    snapScanCtx *ctx = snapScanCreate(stores, 1, snapTestDispatch, snapTestSlotDone, st);
    st->ctx = ctx;
    kvstoreSnapHooks hooks = { .clone = ovTestClone, .free_orig = ovTestFree, .enter = snapOverlayEnter };
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

    /* Delete an untouched key: duplicate the bucket, then delete from the duplicate. */
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

    snapScanRelease(ctx);
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
static void snapOverlayBucketTests(void) {
    dictType noval = KvstoreDictTestType;
    noval.no_value = 1;
    noval.keyCompare = snapTestKeyCompare;
    kvstore *stores[1];
    stores[0] = kvstoreCreate(&KvstoreTestType, &noval, SNAP_TEST_DICT_BITS, KVSTORE_ALLOCATE_DICTS_ON_DEMAND);
    kvstore *ks = stores[0];
    const int N = 300;
    for (int i = 0; i < N; i++) serverAssert(kvstoreDictAddRaw(ks, 0, snapTestKey(0, 0, i), NULL) != NULL);
    dict *d = kvstoreGetDict(ks, 0);
    while (dictIsRehashing(d)) dictRehash(d, 1000);
    struct snapScanTestState *st = zcalloc(sizeof(*st));
    st->inserted[0][0] = N;
    st->stores = stores;
    struct ovTestCounters cnt = {0, 0};
    void *privs[1] = { &cnt };
    kvstoreSnapHooks hooks = { .clone = ovTestClone, .free_orig = ovTestFree, .enter = snapOverlayEnter };
    snapScanCtx *ctx = snapScanCreate(stores, 1, snapTestDispatch, snapTestSlotDone, st);
    st->ctx = ctx;
    snapScanSetOverlay(ctx, &hooks, privs);
    snapScanFreeze(ctx);

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
        char *k = snapTestKey(0, 0, i);
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

    snapFunnelClose();
    snapScanAbort(ctx);
    snapScanRelease(ctx);
    test_cond("overlay buckets: after the release the dict holds the logical keys and every original was freed",
              !kvstoreOverlayIsFrozen(ks, 0) && kvstoreDictSize(ks, 0) == (unsigned long)N && cnt.frees == cnt.clones);
    kvstoreRelease(ks);
    zfree(st);
}

/* The window the test 'prepare' hook defers every write in: half a table
 * (the half the bucket is in), its last position ov_test_seq. */
static uint64_t ov_test_seq;
static int ovTestPrepareHalves(void *priv, int didx, dict *d, const void *key, kvstoreOvWindow *w) {
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
static void snapOverlayWindowTests(void) {
    dictType noval = KvstoreDictTestType;
    noval.no_value = 1;
    noval.keyCompare = snapTestKeyCompare;
    kvstore *stores[1];
    stores[0] = kvstoreCreate(&KvstoreTestType, &noval, SNAP_TEST_DICT_BITS, KVSTORE_ALLOCATE_DICTS_ON_DEMAND);
    kvstore *ks = stores[0];
    const int N = 200;
    for (int didx = 0; didx < SNAP_TEST_DICTS; didx++) {
        for (int i = 0; i < N; i++) serverAssert(kvstoreDictAddRaw(ks, didx, snapTestKey(0, didx, i), NULL) != NULL);
        dict *d = kvstoreGetDict(ks, didx);
        while (dictIsRehashing(d)) dictRehash(d, 1000);
    }
    dict *d0 = kvstoreGetDict(ks, 0), *d1 = kvstoreGetDict(ks, 1), *d2 = kvstoreGetDict(ks, 2);
    struct snapScanTestState *st = zcalloc(sizeof(*st));
    for (int didx = 0; didx < SNAP_TEST_DICTS; didx++) st->inserted[0][didx] = N;
    st->stores = stores;
    struct ovTestCounters cnt = {0, 0};
    void *privs[1] = { &cnt };
    kvstoreSnapHooks hooks = { .clone = ovTestClone, .free_orig = ovTestFree, .enter = snapOverlayEnter,
                               .prepare = ovTestPrepareHalves };
    snapScanCtx *ctx = snapScanCreate(stores, 1, snapTestDispatch, snapTestSlotDone, st);
    st->ctx = ctx;
    snapScanSetOverlay(ctx, &hooks, privs);
    snapScanFreeze(ctx);
    char *used[SNAP_TEST_DICTS];
    for (int didx = 0; didx < SNAP_TEST_DICTS; didx++)
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
    kvstore *tmp = kvstoreCreate(&KvstoreTestType, &noval, SNAP_TEST_DICT_BITS, KVSTORE_ALLOCATE_DICTS_ON_DEMAND);
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

    snapFunnelClose();
    snapScanAbort(ctx);
    snapScanRelease(ctx);
    test_cond("overlay window: after the release nothing is overlayed and nothing is frozen",
              kvstoreOverlayPending() == 0 && !kvstoreOverlayIsFrozen(ks, 0) && !kvstoreOverlayIsFrozen(ks, 2) &&
              kvstoreSize(ks) == 3ULL * N - 15);
    for (int didx = 0; didx < SNAP_TEST_DICTS; didx++) zfree(used[didx]);
    kvstoreRelease(ks);
    zfree(st);
}

/* The overlay over a dict caught mid-rehash (entries in both tables): writes
 * overlay buckets of either table and pause the rehash, enumeration and scan
 * return every key that was there throughout, and the rehash resumes once the
 * overlay is gone. */
static void snapOverlayRehashTests(void) {
    dictType noval = KvstoreDictTestType;
    noval.no_value = 1;
    noval.keyCompare = snapTestKeyCompare;
    kvstore *stores[1];
    stores[0] = kvstoreCreate(&KvstoreTestType, &noval, SNAP_TEST_DICT_BITS, KVSTORE_ALLOCATE_DICTS_ON_DEMAND);
    kvstore *ks = stores[0];
    dict *d = NULL;
    int N = 0;
    /* Stop right after an expansion of a table of at least 64 buckets starts: the
     * rehash then needs about that many more steps, none of which run below. */
    while (N < 250) {
        serverAssert(kvstoreDictAddRaw(ks, 0, snapTestKey(0, 0, N), NULL) != NULL);
        N++;
        d = kvstoreGetDict(ks, 0);
        if (dictIsRehashing(d) && DICTHT_SIZE(d->ht_size_exp[0]) >= 64) break;
    }
    int two_tables = dictIsRehashing(d) && d->ht_used[0] > 0 && d->ht_used[1] > 0;
    test_cond("overlay rehash: the dict is mid-rehash with entries in both tables", two_tables);
    serverAssert(two_tables);
    struct snapScanTestState *st = zcalloc(sizeof(*st));
    st->inserted[0][0] = N;
    st->stores = stores;
    struct ovTestCounters cnt = {0, 0};
    void *privs[1] = { &cnt };
    kvstoreSnapHooks hooks = { .clone = ovTestClone, .free_orig = ovTestFree, .enter = snapOverlayEnter };
    snapScanCtx *ctx = snapScanCreate(stores, 1, snapTestDispatch, snapTestSlotDone, st);
    st->ctx = ctx;
    snapScanSetOverlay(ctx, &hooks, privs);
    snapScanFreeze(ctx);

    /* Delete every 7th key, add 20 new ones: buckets of both tables overlay. */
    int gone[300] = {0}, added = 0;
    for (int i = 0; i < N; i += 7) {
        char *k = snapTestKey(0, 0, i);
        serverAssert(kvstoreDictDelete(ks, 0, k) == DICT_OK);
        gone[i] = 1;
        zfree(k);
    }
    for (int j = 0; j < 20; j++) {
        serverAssert(kvstoreDictAddRaw(ks, 0, snapTestKey(0, 0, N + j), NULL) != NULL);
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

    snapFunnelClose();
    snapScanAbort(ctx);
    snapScanRelease(ctx);
    d = kvstoreGetDict(ks, 0);
    int finished = 1;
    while (dictIsRehashing(d)) { dictRehash(d, 1000); }
    for (int i = 0; i < N + added; i++) {
        char *k = snapTestKey(0, 0, i);
        int present = dictFind(d, k) != NULL;
        int want = (i < N) ? !gone[i] : 1;
        if (present != want) finished = 0;
        zfree(k);
    }
    test_cond("overlay rehash: the rehash completes after the release and the dict holds the logical keys", finished);
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
    server.hybrid_snapshot = 1;
    snapArenaInit();

#ifdef HAVE_NOFORK_ARENA
    test_cond("SNAP_NOFORK arena is available on Linux+jemalloc", snapArenaAvailable());
#else
    test_cond("SNAP_NOFORK arena reported unavailable on this platform", !snapArenaAvailable());
    void *fb = zmalloc_nofork(100);
    test_cond("fallback alloc goes to the default allocator", fb != NULL && !zmalloc_is_nofork(fb));
    zfree(fb);
    /* The background scan and the keyspace overlay do not depend on the arena. */
    snapScanTests(accurate);
    snapRefcountTests();
    snapOverlayTests(0);
    snapOverlayTests(1);
    snapOverlayBucketTests();
    snapOverlayWindowTests();
    snapOverlayRehashTests();
    test_report();
    return 0;
#endif

    /* 1. predicate */
    char *p = zmalloc_nofork(100);
    char *q = zmalloc(100);
    memset(p, 'x', 100);
    test_cond("zmalloc_is_nofork() is true for an arena pointer", zmalloc_is_nofork(p));
    test_cond("zmalloc_is_nofork() is false for a zmalloc pointer", !zmalloc_is_nofork(q));
    test_cond("zmalloc_size() >= requested size", zmalloc_size(p) >= 100);

    /* 2. fork visibility */
    test_cond("SNAP_NOFORK mapping carries the VM_DONTCOPY (dc) flag", snapTestMappingIsDontcopy(p) == 1);
    test_cond("zmalloc mapping does not carry the dc flag", snapTestMappingIsDontcopy(q) == 0);
    test_cond("fork child survives and can use the default allocator", snapTestChildAllocatorAlive() == 1);
    test_cond("fork child faults when writing a SNAP_NOFORK allocation", snapTestChildWriteFaults(p) == 1);
    test_cond("fork child can write a zmalloc allocation", snapTestChildWriteFaults(q) == 0);
    /* Fork visibility flip: one madvise over the reservation. */
    test_cond("arena is DONTFORK by default", !snapArenaForkable());
    test_cond("flip: arena made forkable", snapArenaSetForkable(1) && snapArenaForkable());
    test_cond("flip: fork child can write a SNAP_NOFORK allocation while forkable", snapTestChildWriteFaults(p) == 0);
    test_cond("flip: arena back to DONTFORK", snapArenaSetForkable(0) && !snapArenaForkable());
    test_cond("flip: fork child faults again on the same allocation", snapTestChildWriteFaults(p) == 1);

    /* Large (>= 2MB, multi-page-run) allocation is also DONTFORK. */
    char *big = zmalloc_nofork(5 * 1024 * 1024);
    memset(big, 1, 5 * 1024 * 1024);
    test_cond("large SNAP_NOFORK allocation is in the arena", zmalloc_is_nofork(big));
    test_cond("fork child faults on a large SNAP_NOFORK allocation (first page)", snapTestChildWriteFaults(big) == 1);
    test_cond("fork child faults on a large SNAP_NOFORK allocation (last page)",
              snapTestChildWriteFaults(big + 5 * 1024 * 1024 - 1) == 1);

    /* realloc keeps the class */
    p = zrealloc_nofork(p, 5000);
    test_cond("zrealloc_nofork() result stays in the arena", zmalloc_is_nofork(p));
    test_cond("fork child faults after realloc", snapTestChildWriteFaults(p) == 1);
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
                ptrs[i] = zmalloc_nofork(sz);
                ((char*)ptrs[i])[0] = 1;
                ((char*)ptrs[i])[sz-1] = 1;
            }
            for (int i = 0; i < N; i += 2) { zfree(ptrs[i]); ptrs[i] = NULL; }
            snapTestPurgeArena();
            for (int i = 1; i < N; i += 2) {
                seed = seed * 1103515245u + 12345u;
                ptrs[i] = zrealloc_nofork(ptrs[i], 8 + (seed >> 8) % 16384);
            }
            snapTestPurgeArena();
            for (int i = 1; i < N; i += (N / 32)) {
                if (!zmalloc_is_nofork(ptrs[i])) bad_class++;
                if (snapTestChildWriteFaults(ptrs[i]) != 1) bad_fork++;
            }
            for (int i = 1; i < N; i += 2) zfree(ptrs[i]);
            snapTestPurgeArena();
        }
        /* Fresh allocation after purge cycles must still be DONTFORK (commit path). */
        void *fresh = zmalloc_nofork(4096 * 3);
        memset(fresh, 2, 4096 * 3);
        test_cond("mapping after purge cycles carries the dc flag", snapTestMappingIsDontcopy(fresh) == 1);
        test_cond("stress: sampled live pointers are all in the arena", bad_class == 0);
        test_cond("stress: sampled live pointers all fault in the child after purge/decay/realloc cycles", bad_fork == 0);
        test_cond("allocation after purge cycles is still DONTFORK", snapTestChildWriteFaults(fresh) == 1);
        zfree(fresh);
        zfree(ptrs);
    }

    /* 4. tcache isolation: freed SNAP_NOFORK blocks must never be recycled into
     * default-arena allocations, and vice versa. */
    {
        const int N = 2000;
        void **a = zmalloc(sizeof(void*) * N);
        for (int i = 0; i < N; i++) a[i] = zmalloc_nofork(64);
        for (int i = 0; i < N; i++) zfree(a[i]);
        int leaked = 0;
        for (int i = 0; i < N; i++) { a[i] = zmalloc(64); if (zmalloc_is_nofork(a[i])) leaked++; }
        for (int i = 0; i < N; i++) zfree(a[i]);
        test_cond("freed SNAP_NOFORK blocks are never handed to zmalloc()", leaked == 0);

        for (int i = 0; i < N; i++) a[i] = zmalloc(64);
        for (int i = 0; i < N; i++) zfree(a[i]);
        int wrong = 0;
        for (int i = 0; i < N; i++) { a[i] = zmalloc_nofork(64); if (!zmalloc_is_nofork(a[i])) wrong++; }
        for (int i = 0; i < N; i++) zfree(a[i]);
        test_cond("freed zmalloc blocks are never handed to zmalloc_nofork()", wrong == 0);
        zfree(a);
    }

    /* 5. accounting */
    {
        size_t before = zmalloc_used_memory();
        void *x = zmalloc_nofork(10000);
        size_t mid = zmalloc_used_memory();
        zfree_with_size(x, zmalloc_size(x));
        size_t after = zmalloc_used_memory();
        test_cond("zmalloc_nofork allocations are counted in zmalloc_used_memory()", mid > before && after == before);
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
        for (int i = 0; i < N; i++) { void *x = zmalloc_nofork(64); zfree(x); }
        t1 = snapTestNowNs();
        double s_pair = (double)(t1 - t0) / N;

        t0 = snapTestNowNs();
        for (int i = 0; i < N; i++) a[i] = zmalloc(64);
        for (int i = 0; i < N; i++) zfree(a[i]);
        t1 = snapTestNowNs();
        double z_bulk = (double)(t1 - t0) / N;

        t0 = snapTestNowNs();
        for (int i = 0; i < N; i++) a[i] = zmalloc_nofork(64);
        for (int i = 0; i < N; i++) zfree(a[i]);
        t1 = snapTestNowNs();
        double s_bulk = (double)(t1 - t0) / N;

        t0 = snapTestNowNs();
        for (int i = 0; i < N; i++) a[i] = zmalloc_nofork(64);
        for (int i = 0; i < N; i++) zfree_with_size(a[i], 64);
        t1 = snapTestNowNs();
        double s_bulk_sized = (double)(t1 - t0) / N;

        printf("BENCH alloc+free 64B, N=%d: zmalloc/zfree pair %.1f ns, zmalloc_nofork pair %.1f ns; "
               "bulk zmalloc %.1f ns/op, bulk zmalloc_nofork %.1f ns/op, bulk zmalloc_nofork sized-free %.1f ns/op\n",
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
        for (int i = 0; i < N; i++) { a[i] = zmalloc_nofork(96); ((char*)a[i])[0] = 1; }
        int maps_after = snapTestCountMaps();
        for (int i = 0; i < N; i++) zfree(a[i]);
        zfree(a);
        printf("VMA maps: before=%d after %d x 96B SNAP_NOFORK allocations=%d (delta %d)\n",
               maps_before, N, maps_after, maps_after - maps_before);
        test_cond("VMA count stays far below vm.max_map_count default (65530)", maps_after < 10000);
    }

    snapPlacementTests();
    snapScanTests(accurate);
    snapRefcountTests();
    snapOverlayTests(0);
    snapOverlayTests(1);
    snapOverlayBucketTests();
    snapOverlayWindowTests();
    snapOverlayRehashTests();

    test_report();
    return 0;
}
#endif
