
/* define macros for having usleep */
#define _BSD_SOURCE
#define _DEFAULT_SOURCE

#include "redismodule.h"
#include <string.h>
#include <assert.h>
#include <errno.h>
#include <unistd.h>

#define UNUSED(V) ((void) V)

int child_pid = -1;
int exited_with_code = -1;

void done_handler(int exitcode, int bysignal, void *user_data) {
    child_pid = -1;
    exited_with_code = exitcode;
    assert(user_data==(void*)0xdeadbeef);
    UNUSED(bysignal);
}

int fork_create(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    long long code_to_exit_with;
    long long usleep_us;
    if (argc != 3) {
        RedisModule_WrongArity(ctx);
        return REDISMODULE_OK;
    }

    if(!RMAPI_FUNC_SUPPORTED(RedisModule_Fork)){
        RedisModule_ReplyWithError(ctx, "Fork api is not supported in the current redis version");
        return REDISMODULE_OK;
    }

    RedisModule_StringToLongLong(argv[1], &code_to_exit_with);
    RedisModule_StringToLongLong(argv[2], &usleep_us);
    exited_with_code = -1;
    int fork_child_pid = RedisModule_Fork(done_handler, (void*)0xdeadbeef);
    if (fork_child_pid < 0) {
        RedisModule_ReplyWithError(ctx, "Fork failed");
        return REDISMODULE_OK;
    } else if (fork_child_pid > 0) {
        /* parent */
        child_pid = fork_child_pid;
        RedisModule_ReplyWithLongLong(ctx, child_pid);
        return REDISMODULE_OK;
    }

    /* child */
    RedisModule_Log(ctx, "notice", "fork child started");
    usleep(usleep_us);
    RedisModule_Log(ctx, "notice", "fork child exiting");
    RedisModule_ExitFromChild(code_to_exit_with);
    /* unreachable */
    return 0;
}

/* Module-owned memory a REDISMODULE_FORK_NO_KEYSPACE child may read. */
static char *module_buf = NULL;

/* Exit code bits of the fork.nokeyspace child: 0 when every keyspace call was
 * refused and the module's own memory was readable. */
#define NK_KEYEXISTS_REACHED 1    /* RM_KeyExists saw the key */
#define NK_OPENKEY_REACHED   2    /* RM_OpenKey returned a handle */
#define NK_CALL_REACHED      4    /* RM_Call returned a reply */
#define NK_CALL_ERRNO        8    /* RM_Call failed with an errno other than EPERM */
#define NK_MODULE_MEM_BAD    16   /* the module's own buffer did not hold what the parent wrote */

/* FORK.NOKEYSPACE <key> <usleep>: fork with REDISMODULE_FORK_NO_KEYSPACE; the
 * child probes the keyspace APIs on <key>, reads module memory, sleeps and
 * exits with the NK_* bits of what it saw (fork.exitcode). */
int fork_nokeyspace(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    long long usleep_us;
    if (argc != 3) {
        RedisModule_WrongArity(ctx);
        return REDISMODULE_OK;
    }
    if (!RMAPI_FUNC_SUPPORTED(RedisModule_ForkWithFlags)) {
        RedisModule_ReplyWithError(ctx, "ForkWithFlags api is not supported in the current redis version");
        return REDISMODULE_OK;
    }
    RedisModule_StringToLongLong(argv[2], &usleep_us);

    if (!module_buf) module_buf = RedisModule_Alloc(64);
    strcpy(module_buf, "module-owned");
    exited_with_code = -1;
    int fork_child_pid = RedisModule_ForkWithFlags(ctx, REDISMODULE_FORK_NO_KEYSPACE, done_handler, (void*)0xdeadbeef);
    if (fork_child_pid < 0) {
        RedisModule_ReplyWithErrorFormat(ctx, "ERR Fork failed: %s", strerror(errno));
        return REDISMODULE_OK;
    } else if (fork_child_pid > 0) {
        /* parent */
        child_pid = fork_child_pid;
        RedisModule_ReplyWithLongLong(ctx, child_pid);
        return REDISMODULE_OK;
    }

    /* child */
    int code = 0;
    RedisModule_Log(ctx, "notice", "nokeyspace child started");
    if (RedisModule_KeyExists(ctx, argv[1])) code |= NK_KEYEXISTS_REACHED;
    RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ);
    if (key) {
        code |= NK_OPENKEY_REACHED;
        RedisModule_CloseKey(key);
    }
    errno = 0;
    RedisModuleCallReply *reply = RedisModule_Call(ctx, "DBSIZE", "");
    if (reply) {
        code |= NK_CALL_REACHED;
        RedisModule_FreeCallReply(reply);
    } else if (errno != EPERM) {
        code |= NK_CALL_ERRNO;
    }
    if (strcmp(module_buf, "module-owned") != 0) code |= NK_MODULE_MEM_BAD;
    usleep(usleep_us);
    RedisModule_Log(ctx, "notice", "nokeyspace child exiting");
    RedisModule_ExitFromChild(code);
    /* unreachable */
    return 0;
}

/* FORK.BADFLAGS: RM_ForkWithFlags with an unknown flag; replies with the errno name. */
int fork_badflags(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    UNUSED(argv);
    UNUSED(argc);
    errno = 0;
    int pid = RedisModule_ForkWithFlags(ctx, 1 << 30, done_handler, (void*)0xdeadbeef);
    if (pid == 0) RedisModule_ExitFromChild(1);   /* must not happen: the test sees no child */
    if (pid > 0) {
        child_pid = pid;
        return RedisModule_ReplyWithError(ctx, "ERR forked with an unknown flag");
    }
    return RedisModule_ReplyWithSimpleString(ctx, errno == EINVAL ? "EINVAL" : strerror(errno));
}

int fork_exitcode(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    UNUSED(argv);
    UNUSED(argc);
    RedisModule_ReplyWithLongLong(ctx, exited_with_code);
    return REDISMODULE_OK;
}

int fork_kill(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    UNUSED(argv);
    UNUSED(argc);
    if (RedisModule_KillForkChild(child_pid) != REDISMODULE_OK)
        RedisModule_ReplyWithError(ctx, "KillForkChild failed");
    else
        RedisModule_ReplyWithLongLong(ctx, 1);
    child_pid = -1;
    return REDISMODULE_OK;
}

int RedisModule_OnUnload(RedisModuleCtx *ctx) {
    UNUSED(ctx);
    if (module_buf) RedisModule_Free(module_buf);
    module_buf = NULL;
    return REDISMODULE_OK;
}

int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    UNUSED(argv);
    UNUSED(argc);
    if (RedisModule_Init(ctx,"fork",1,REDISMODULE_APIVER_1)== REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx,"fork.create", fork_create,"",0,0,0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx,"fork.nokeyspace", fork_nokeyspace,"",0,0,0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx,"fork.badflags", fork_badflags,"",0,0,0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx,"fork.exitcode", fork_exitcode,"",0,0,0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx,"fork.kill", fork_kill,"",0,0,0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    return REDISMODULE_OK;
}
