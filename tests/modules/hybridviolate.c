/* Test module for the hybrid BGSAVE module contract (TASK-1.25).
 *
 * A data type whose rdb_save deliberately reaches for the keyspace (RM_OpenKey
 * on another key and RM_Call) and logs what it got back, so a test can verify
 * (a) without REDISMODULE_OPTIONS_HANDLE_HYBRID_BGSAVE the server falls back
 * to the stock fork child, and (b) with it the hybrid child refuses those
 * calls (NULL / error) instead of faulting. hv.optin toggles the option at
 * runtime; hv.setlru exercises RM_SetLRU/RM_SetLFU on READ and WRITE handles. */
#include "redismodule.h"
#include <string.h>
#include <stdlib.h>

static RedisModuleType *HVType;
/* Detached context taken at load time, as modules that touch the keyspace
 * from a save callback do (the IO context has no client to look keys up with). */
static RedisModuleCtx *detached;

typedef struct { char *s; size_t len; } hvval;

static void *hv_load(RedisModuleIO *io, int encver) {
    REDISMODULE_NOT_USED(encver);
    hvval *v = RedisModule_Alloc(sizeof(*v));
    v->s = RedisModule_LoadStringBuffer(io, &v->len);
    return v;
}

static void hv_save(RedisModuleIO *io, void *value) {
    hvval *v = value;
    RedisModuleCtx *ctx = detached;
    RedisModule_SelectDb(ctx, RedisModule_GetDbIdFromIO(io));
    /* Rule 1 violations: open another key and run a command from a save callback. */
    RedisModuleString *probe = RedisModule_CreateString(ctx, "hv:probe", 8);
    RedisModuleKey *k = RedisModule_OpenKey(ctx, probe, REDISMODULE_READ);
    RedisModuleCallReply *r = RedisModule_Call(ctx, "DBSIZE", "");
    RedisModule_Log(ctx, "notice", "hv_save: OpenKey=%s Call=%s", k ? "key" : "NULL", r ? "reply" : "NULL");
    if (k) RedisModule_CloseKey(k);
    if (r) RedisModule_FreeCallReply(r);
    RedisModule_FreeString(ctx, probe);
    RedisModule_SaveStringBuffer(io, v->s, v->len);
}

static void hv_free(void *value) {
    hvval *v = value;
    RedisModule_Free(v->s);
    RedisModule_Free(v);
}

static size_t hv_mem_usage(const void *value) {
    const hvval *v = value;
    return sizeof(*v) + v->len;
}

/* HV.SET key value */
static int hv_set(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc != 3) return RedisModule_WrongArity(ctx);
    RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_WRITE);
    size_t len;
    const char *s = RedisModule_StringPtrLen(argv[2], &len);
    hvval *v = RedisModule_Alloc(sizeof(*v));
    v->s = RedisModule_Alloc(len + 1); memcpy(v->s, s, len); v->s[len] = 0; v->len = len;
    RedisModule_ModuleTypeSetValue(key, HVType, v);
    RedisModule_CloseKey(key);
    RedisModule_ReplicateVerbatim(ctx);
    return RedisModule_ReplyWithSimpleString(ctx, "OK");
}

/* HV.GET key */
static int hv_get(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc != 2) return RedisModule_WrongArity(ctx);
    RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ);
    if (RedisModule_ModuleTypeGetType(key) != HVType) {
        RedisModule_CloseKey(key);
        return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
    }
    hvval *v = RedisModule_ModuleTypeGetValue(key);
    RedisModule_ReplyWithStringBuffer(ctx, v->s, v->len);
    RedisModule_CloseKey(key);
    return REDISMODULE_OK;
}

/* HV.OPTIN 0|1: declare (or withdraw) REDISMODULE_OPTIONS_HANDLE_HYBRID_BGSAVE at runtime. */
static int hv_optin(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc != 2) return RedisModule_WrongArity(ctx);
    long long on;
    if (RedisModule_StringToLongLong(argv[1], &on) != REDISMODULE_OK) return RedisModule_ReplyWithError(ctx, "ERR not an integer");
    RedisModule_SetModuleOptions(ctx, on ? REDISMODULE_OPTIONS_HANDLE_HYBRID_BGSAVE : 0);
    return RedisModule_ReplyWithSimpleString(ctx, "OK");
}

/* HV.SETLRU key: [result on a READ handle, result on a WRITE handle] for SetLRU, then SetLFU. */
static int hv_setlru(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc != 2) return RedisModule_WrongArity(ctx);
    RedisModuleKey *rk = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ);
    RedisModuleKey *wk = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_WRITE);
    RedisModule_ReplyWithArray(ctx, 4);
    RedisModule_ReplyWithLongLong(ctx, RedisModule_SetLRU(rk, 1000) == REDISMODULE_OK);
    RedisModule_ReplyWithLongLong(ctx, RedisModule_SetLRU(wk, 1000) == REDISMODULE_OK);
    RedisModule_ReplyWithLongLong(ctx, RedisModule_SetLFU(rk, 5) == REDISMODULE_OK);
    RedisModule_ReplyWithLongLong(ctx, RedisModule_SetLFU(wk, 5) == REDISMODULE_OK);
    RedisModule_CloseKey(rk);
    RedisModule_CloseKey(wk);
    return REDISMODULE_OK;
}

int RedisModule_OnUnload(RedisModuleCtx *ctx) {
    REDISMODULE_NOT_USED(ctx);
    if (detached) RedisModule_FreeThreadSafeContext(detached);
    detached = NULL;
    return REDISMODULE_OK;
}

int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    REDISMODULE_NOT_USED(argv);
    REDISMODULE_NOT_USED(argc);
    if (RedisModule_Init(ctx, "hybridviolate", 1, REDISMODULE_APIVER_1) == REDISMODULE_ERR) return REDISMODULE_ERR;
    RedisModuleTypeMethods tm = {
        .version = REDISMODULE_TYPE_METHOD_VERSION,
        .rdb_load = hv_load,
        .rdb_save = hv_save,
        .free = hv_free,
        .mem_usage = hv_mem_usage,
    };
    HVType = RedisModule_CreateDataType(ctx, "hybridvio", 0, &tm);
    if (HVType == NULL) return REDISMODULE_ERR;
    detached = RedisModule_GetDetachedThreadSafeContext(ctx);
    RedisModule_SelectDb(detached, 0);
    if (RedisModule_CreateCommand(ctx, "hv.set", hv_set, "write deny-oom", 1, 1, 1) == REDISMODULE_ERR) return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx, "hv.get", hv_get, "readonly", 1, 1, 1) == REDISMODULE_ERR) return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx, "hv.optin", hv_optin, "", 0, 0, 0) == REDISMODULE_ERR) return REDISMODULE_ERR;
    if (RedisModule_CreateCommand(ctx, "hv.setlru", hv_setlru, "write", 1, 1, 1) == REDISMODULE_ERR) return REDISMODULE_ERR;
    return REDISMODULE_OK;
}
