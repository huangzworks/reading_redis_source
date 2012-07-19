#include "redis.h"

/* redis.h 中和 pubsub 有关的结构

struct redisServer {
    // 省略 ...
    dict *pubsub_channels;  // Map channels to list of subscribed clients
    list *pubsub_patterns;  // A list of pubsub_patterns
    // 省略 ... 
};

typedef struct pubsubPattern {
    redisClient *client;
    robj *pattern;
} pubsubPattern;

typedef struct redisClient {
    // 省略 ...
    dict *pubsub_channels;  // channels a client is interested in (SUBSCRIBE)
    list *pubsub_patterns;  // patterns a client is interested in (SUBSCRIBE)
    // 省略 ...
} redisClient;


*/

/*-----------------------------------------------------------------------------
 * Pubsub low level API
 *----------------------------------------------------------------------------*/

void freePubsubPattern(void *p) {
    pubsubPattern *pat = p;

    decrRefCount(pat->pattern);
    zfree(pat);
}

int listMatchPubsubPattern(void *a, void *b) {
    pubsubPattern *pa = a, *pb = b;

    return (pa->client == pb->client) &&
           (equalStringObjects(pa->pattern,pb->pattern));
}

/* Subscribe a client to a channel. Returns 1 if the operation succeeded, or
 * 0 if the client was already subscribed to that channel. */
// 订阅指定频道
// 订阅成功返回 1 ，如果已经订阅过，返回 0 
int pubsubSubscribeChannel(redisClient *c, robj *channel) {
    struct dictEntry *de;
    list *clients = NULL;
    int retval = 0;

    /* Add the channel to the client -> channels hash table */
    // dictAdd 在添加新元素成功时返回 DICT_OK
    // 因此这个判断句表示，如果新订阅 channel 成功，那么 。。。
    if (dictAdd(c->pubsub_channels,channel,NULL) == DICT_OK) {
        retval = 1;
        incrRefCount(channel);

        /* Add the client to the channel -> list of clients hash table */
        // 将 client 添加到订阅给定 channel 的链表中
        // 这个链表是一个哈希表的值，哈希表的键是给定 channel
        // 这个哈希表保存在 server.pubsub_channels 里
        de = dictFind(server.pubsub_channels,channel);
        if (de == NULL) {
            // 如果 de 等于 NULL 
            // 表示这个客户端是首个订阅这个 channel 的客户端
            // 那么创建一个新的列表， 并将它加入到哈希表中
            clients = listCreate();
            dictAdd(server.pubsub_channels,channel,clients);
            incrRefCount(channel);
        } else {
            // 如果 de 不为空，就取出这个 clients 链表
            clients = dictGetVal(de);
        }
        // 将客户端加入到链表中
        listAddNodeTail(clients,c); 
    }
    /* Notify the client */
    addReply(c,shared.mbulkhdr[3]);
    addReply(c,shared.subscribebulk);
    // 返回订阅的频道
    addReplyBulk(c,channel);    
    // 返回客户端当前已订阅的频道和模式数量的总和
    addReplyLongLong(c,dictSize(c->pubsub_channels)+listLength(c->pubsub_patterns));    

    return retval;
}

/* Unsubscribe a client from a channel. Returns 1 if the operation succeeded, or
 * 0 if the client was not subscribed to the specified channel. */
// 退订频道
// 退订成功返回 1 ，退订失败返回 0 （没有订阅过这个频道）
int pubsubUnsubscribeChannel(redisClient *c, robj *channel, int notify) {
    struct dictEntry *de;
    list *clients;
    listNode *ln;
    int retval = 0;

    /* Remove the channel from the client -> channels hash table */
    incrRefCount(channel); /* channel may be just a pointer to the same object
                            we have in the hash tables. Protect it... */

    // 移除客户端中的订阅信息
    if (dictDelete(c->pubsub_channels,channel) == DICT_OK) {
        retval = 1;
        /* Remove the client from the channel -> clients list hash table */
        // 移除服务器端中的订阅信息
        de = dictFind(server.pubsub_channels,channel);
        redisAssertWithInfo(c,NULL,de != NULL);
        clients = dictGetVal(de);
        ln = listSearchKey(clients,c);
        redisAssertWithInfo(c,NULL,ln != NULL);
        listDelNode(clients,ln);
        // 如果服务器端的频道链表为空， 说明已经没有任何客户端订阅这个频道
        // 那么删除它
        if (listLength(clients) == 0) {
            /* Free the list and associated hash entry at all if this was
             * the latest client, so that it will be possible to abuse
             * Redis PUBSUB creating millions of channels. */
            dictDelete(server.pubsub_channels,channel);
        }
    }
    /* Notify the client */
    if (notify) {
        addReply(c,shared.mbulkhdr[3]);
        addReply(c,shared.unsubscribebulk);
        // 返回被退订的频道
        addReplyBulk(c,channel);
        // 返回客户端目前仍在订阅的频道和模式数量之和
        addReplyLongLong(c,dictSize(c->pubsub_channels)+
                       listLength(c->pubsub_patterns));

    }
    decrRefCount(channel); /* it is finally safe to release it */

    return retval;
}

/* Subscribe a client to a pattern. Returns 1 if the operation succeeded, or 0 if the clinet was already subscribed to that pattern. */
// 订阅指定模式
// 订阅成功返回 1 ，如果已经订阅过，返回 0 
int pubsubSubscribePattern(redisClient *c, robj *pattern) {
    int retval = 0;

    // 向 c->pubsub_patterns 中查找指定 pattern
    // 如果返回值为 NULL ，说明这个 pattern 还没被这个客户端订阅过
    if (listSearchKey(c->pubsub_patterns,pattern) == NULL) {
        retval = 1;

        // 添加 pattern 到客户端 pubsub_patterns
        listAddNodeTail(c->pubsub_patterns,pattern);    
        incrRefCount(pattern);

        // 将 pattern 添加到服务器
        pubsubPattern *pat;
        pat = zmalloc(sizeof(*pat));
        pat->pattern = getDecodedObject(pattern);
        pat->client = c;

        listAddNodeTail(server.pubsub_patterns,pat);
    }
    /* Notify the client */
    addReply(c,shared.mbulkhdr[3]);
    addReply(c,shared.psubscribebulk);
    // 返回被订阅的模式
    addReplyBulk(c,pattern);    
    // 返回客户端当前已订阅的频道和模式数量的总和
    addReplyLongLong(c,dictSize(c->pubsub_channels)+listLength(c->pubsub_patterns));    

    return retval;
}

/* Unsubscribe a client from a channel. Returns 1 if the operation succeeded, or
 * 0 if the client was not subscribed to the specified channel. */
int pubsubUnsubscribePattern(redisClient *c, robj *pattern, int notify) {
    listNode *ln;
    pubsubPattern pat;
    int retval = 0;

    incrRefCount(pattern); /* Protect the object. May be the same we remove */
    // 如果给定模式存在于 c->pubsub_patterns ，那么。。。
    if ((ln = listSearchKey(c->pubsub_patterns,pattern)) != NULL) {
        retval = 1;
        // 从客户端模式链表中移除给定模式
        listDelNode(c->pubsub_patterns,ln); 

        // 从服务器端模式链表中移除给定模式
        pat.client = c;
        pat.pattern = pattern;
        ln = listSearchKey(server.pubsub_patterns,&pat);
        listDelNode(server.pubsub_patterns,ln);
    }
    /* Notify the client */
    if (notify) {
        addReply(c,shared.mbulkhdr[3]);
        addReply(c,shared.punsubscribebulk);
        // 返回被退订的模式
        addReplyBulk(c,pattern);
        // 返回客户端目前仍在订阅的频道和模式数量之和
        addReplyLongLong(c,dictSize(c->pubsub_channels)+
                       listLength(c->pubsub_patterns));
    }
    decrRefCount(pattern);

    return retval;
}

/* Unsubscribe from all the channels. Return the number of channels the
 * client was subscribed from. */
// 退订所有频道，返回值为退订频道的数量
int pubsubUnsubscribeAllChannels(redisClient *c, int notify) {
    dictIterator *di = dictGetSafeIterator(c->pubsub_channels);
    dictEntry *de;
    int count = 0;

    while((de = dictNext(di)) != NULL) {
        robj *channel = dictGetKey(de);

        count += pubsubUnsubscribeChannel(c,channel,notify);
    }
    dictReleaseIterator(di);
    return count;
}

/* Unsubscribe from all the patterns. Return the number of patterns the
 * client was subscribed from. */
// 退订所有模式，返回值为退订模式的数量
int pubsubUnsubscribeAllPatterns(redisClient *c, int notify) {
    listNode *ln;
    listIter li;
    int count = 0;

    listRewind(c->pubsub_patterns,&li);
    while ((ln = listNext(&li)) != NULL) {
        robj *pattern = ln->value;

        count += pubsubUnsubscribePattern(c,pattern,notify);
    }
    return count;
}

/* Publish a message */
// 发送消息
int pubsubPublishMessage(robj *channel, robj *message) {
    int receivers = 0;
    struct dictEntry *de;
    listNode *ln;
    listIter li;

    /* Send to clients listening for that channel */
    // 向所有频道的订阅者发送消息
    de = dictFind(server.pubsub_channels,channel);
    if (de) {
        list *list = dictGetVal(de);    // 取出所有订阅者
        listNode *ln;
        listIter li;

        // 遍历所有订阅者， 向它们发送消息
        listRewind(list,&li);
        while ((ln = listNext(&li)) != NULL) {
            redisClient *c = ln->value;

            addReply(c,shared.mbulkhdr[3]);
            addReply(c,shared.messagebulk);
            addReplyBulk(c,channel);    // 打印频道名
            addReplyBulk(c,message);    // 打印消息
            receivers++;    // 更新接收者数量
        }
    }
    /* Send to clients listening to matching channels */
    // 向所有被匹配模式的订阅者发送消息
    if (listLength(server.pubsub_patterns)) {
        listRewind(server.pubsub_patterns,&li); // 取出所有模式
        channel = getDecodedObject(channel);
        while ((ln = listNext(&li)) != NULL) {
            pubsubPattern *pat = ln->value; // 取出模式

            // 如果模式和 channel 匹配的话
            // 向这个模式的订阅者发送消息
            if (stringmatchlen((char*)pat->pattern->ptr,
                                sdslen(pat->pattern->ptr),
                                (char*)channel->ptr,
                                sdslen(channel->ptr),0)) {
                addReply(pat->client,shared.mbulkhdr[4]);
                addReply(pat->client,shared.pmessagebulk);
                addReplyBulk(pat->client,pat->pattern); // 打印被匹配的模式
                addReplyBulk(pat->client,channel);      // 打印频道名
                addReplyBulk(pat->client,message);      // 打印消息
                receivers++;    // 更新接收者数量
            }
        }
        decrRefCount(channel);  // 释放用过的 channel
    }
    return receivers;   // 返回接收者数量
}

/*-----------------------------------------------------------------------------
 * Pubsub commands implementation
 *----------------------------------------------------------------------------*/

// 订阅频道
void subscribeCommand(redisClient *c) {
    int j;

    for (j = 1; j < c->argc; j++)
        pubsubSubscribeChannel(c,c->argv[j]);
}

// 退订频道
void unsubscribeCommand(redisClient *c) {
    if (c->argc == 1) {
        pubsubUnsubscribeAllChannels(c,1);
        return;
    } else {
        int j;

        for (j = 1; j < c->argc; j++)
            pubsubUnsubscribeChannel(c,c->argv[j],1);
    }
}

// 订阅模式
void psubscribeCommand(redisClient *c) {
    int j;

    for (j = 1; j < c->argc; j++)
        pubsubSubscribePattern(c,c->argv[j]);
}

// 退订频道
void punsubscribeCommand(redisClient *c) {
    // 如果输入没有指定模式，那么退订所有模式
    if (c->argc == 1) {
        pubsubUnsubscribeAllPatterns(c,1);
        return;
    } else {
        int j;

        for (j = 1; j < c->argc; j++)
            pubsubUnsubscribePattern(c,c->argv[j],1);
    }
}

// 发送信息
void publishCommand(redisClient *c) {
    // 发送信息
    int receivers = pubsubPublishMessage(c->argv[1],c->argv[2]);
    // 向集群传播
    if (server.cluster_enabled) clusterPropagatePublish(c->argv[1],c->argv[2]);
    // 返回信息接收者数量
    addReplyLongLong(c,receivers);
}
