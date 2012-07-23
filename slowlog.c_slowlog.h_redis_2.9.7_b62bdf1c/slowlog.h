#define SLOWLOG_ENTRY_MAX_ARGC 32
#define SLOWLOG_ENTRY_MAX_STRING 128

/* redisServer 结构中和 slow log 有关的属性

struct redisServer {
    // 其他域 ...
    list *slowlog;                  // SLOWLOG list of commands
    long long slowlog_entry_id;     // SLOWLOG current entry ID
    long long slowlog_log_slower_than; // SLOWLOG time limit (to get logged)
    unsigned long slowlog_max_len;     // SLOWLOG max number of items logged
    // 其他域 ...
};

*/

/* This structure defines an entry inside the slow log list */
typedef struct slowlogEntry {
    robj **argv;        // 被执行的命令和参数
    int argc;           // 参数个数
    long long id;       // 唯一 id （ Unique entry identifier ）
    long long duration; // 执行查询所用的时长，以纳秒（十亿份之一秒）为单位
    time_t time;        // 命令执行是的 UNIX 时间戳
} slowlogEntry;

/* Exported API */
void slowlogInit(void);
void slowlogPushEntryIfNeeded(robj **argv, int argc, long long duration);

/* Exported commands */
void slowlogCommand(redisClient *c);
