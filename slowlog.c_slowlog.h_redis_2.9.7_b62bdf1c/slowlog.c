#include "redis.h"
#include "slowlog.h"

/* Slowlog implements a system that is able to remember the latest N
 * queries that took more than M microseconds to execute.
 *
 * The execution time to reach to be logged in the slow log is set
 * using the 'slowlog-log-slower-than' config directive, that is also
 * readable and writable using the CONFIG SET/GET command.
 *
 * The slow queries log is actually not "logged" in the Redis log file
 * but is accessible thanks to the SLOWLOG command. */

/* Create a new slowlog entry.
 * Incrementing the ref count of all the objects retained is up to
 * this function. */
// 创建新 slowlog 记录
// 增加引用计数以保持对象的任务由这个函数完成
// 参数：
//  argv 传入命令和参数
//  argc 参数个数
//  duration 执行插叙所用的时长，以纳秒（十亿分之一秒）为单位
slowlogEntry *slowlogCreateEntry(robj **argv, int argc, long long duration) {
    slowlogEntry *se = zmalloc(sizeof(*se));
    int j, slargc = argc;

    // 对参数进行减少和截断，节省内存
    if (slargc > SLOWLOG_ENTRY_MAX_ARGC) slargc = SLOWLOG_ENTRY_MAX_ARGC;
    se->argc = slargc;
    se->argv = zmalloc(sizeof(robj*)*slargc);
    for (j = 0; j < slargc; j++) {
        /* Logging too many arguments is a useless memory waste, so we stop
         * at SLOWLOG_ENTRY_MAX_ARGC, but use the last argument to specify
         * how many remaining arguments there were in the original command. */
        if (slargc != argc && j == slargc-1) {
            se->argv[j] = createObject(REDIS_STRING,
                sdscatprintf(sdsempty(),"... (%d more arguments)",
                argc-slargc+1));
        } else {
            /* Trim too long strings as well... */
            if (argv[j]->type == REDIS_STRING &&
                argv[j]->encoding == REDIS_ENCODING_RAW &&
                sdslen(argv[j]->ptr) > SLOWLOG_ENTRY_MAX_STRING)
            {
                sds s = sdsnewlen(argv[j]->ptr, SLOWLOG_ENTRY_MAX_STRING);

                s = sdscatprintf(s,"... (%lu more bytes)",
                    (unsigned long)
                    sdslen(argv[j]->ptr) - SLOWLOG_ENTRY_MAX_STRING);
                se->argv[j] = createObject(REDIS_STRING,s);
            } else {
                se->argv[j] = argv[j];
                incrRefCount(argv[j]);
            }
        }
    }

    se->time = time(NULL);
    se->duration = duration;
    se->id = server.slowlog_entry_id++; // 更新唯一 id

    return se;
}

/* Free a slow log entry. The argument is void so that the prototype of this
 * function matches the one of the 'free' method of adlist.c.
 *
 * This function will take care to release all the retained object. */
// 释放给定的 slowlog
void slowlogFreeEntry(void *septr) {
    slowlogEntry *se = septr;
    int j;

    // 释放参数
    for (j = 0; j < se->argc; j++)
        decrRefCount(se->argv[j]);
    // 释放 slowlog Entry
    zfree(se->argv);
    zfree(se);
}

/* Initialize the slow log. This function should be called a single time
 * at server startup. */
// 初始化 slowlog ，在服务器启动时执行
void slowlogInit(void) {
    server.slowlog = listCreate();
    server.slowlog_entry_id = 0;
    listSetFreeMethod(server.slowlog,slowlogFreeEntry);
}

/* Push a new entry into the slow log.
 * This function will make sure to trim the slow log accordingly to the
 * configured max length. */
// 将一个新的 slowlog 推入 slow log
// 这个函数会确保 slowlog 的数量不超过配置设置的最大 slow log 数量
void slowlogPushEntryIfNeeded(robj **argv, int argc, long long duration) {
    if (server.slowlog_log_slower_than < 0) return; /* Slowlog disabled */
    // 推入 slow log
    if (duration >= server.slowlog_log_slower_than)
        listAddNodeHead(server.slowlog,slowlogCreateEntry(argv,argc,duration));

    /* Remove old entries if needed. */
    // 保持 slowlog 的数量不超过设置的数量
    while (listLength(server.slowlog) > server.slowlog_max_len)
        listDelNode(server.slowlog,listLast(server.slowlog));
}

/* Remove all the entries from the current slow log. */
// 移除当前所有 slow log
void slowlogReset(void) {
    while (listLength(server.slowlog) > 0)
        listDelNode(server.slowlog,listLast(server.slowlog));
}

/* The SLOWLOG command. Implements all the subcommands needed to handle the
 * Redis slow log. */
// SLOWLOG 命令的实现
void slowlogCommand(redisClient *c) {
    // SLOWLOG reset
    if (c->argc == 2 && !strcasecmp(c->argv[1]->ptr,"reset")) {
        slowlogReset();
        addReply(c,shared.ok);
    // SLOWLOG len
    } else if (c->argc == 2 && !strcasecmp(c->argv[1]->ptr,"len")) {
        addReplyLongLong(c,listLength(server.slowlog));
    // SLOWLOG GET 打印全部 slow log
    // 或 SLOWLOG GET number 打印给定数量的 slow log
    } else if ((c->argc == 2 || c->argc == 3) &&
               !strcasecmp(c->argv[1]->ptr,"get"))
    {
        long count = 10, sent = 0;
        listIter li;
        void *totentries;
        listNode *ln;
        slowlogEntry *se;

        if (c->argc == 3 &&
            getLongFromObjectOrReply(c,c->argv[2],&count,NULL) != REDIS_OK)
            return;

        listRewind(server.slowlog,&li);
        totentries = addDeferredMultiBulkLength(c);
        while(count-- && (ln = listNext(&li))) {
            int j;

            se = ln->value;
            addReplyMultiBulkLen(c,4);
            addReplyLongLong(c,se->id);
            addReplyLongLong(c,se->time);
            addReplyLongLong(c,se->duration);
            addReplyMultiBulkLen(c,se->argc);
            for (j = 0; j < se->argc; j++)
                addReplyBulk(c,se->argv[j]);
            sent++;
        }
        setDeferredMultiBulkLength(c,totentries,sent);
    } else {
        addReplyError(c,
            "Unknown SLOWLOG subcommand or wrong # of args. Try GET, RESET, LEN.");
    }
}
