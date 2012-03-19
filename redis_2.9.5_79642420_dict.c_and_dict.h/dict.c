/* Hash Tables Implementation.
 *
 * This file implements in memory hash tables with insert/del/replace/find/
 * get-random-element operations. Hash tables will auto resize if needed
 * tables of power of two in size are used, collisions are handled by
 * chaining. See the source code for more information... :)
 *
 * Copyright (c) 2006-2010, Salvatore Sanfilippo <antirez at gmail dot com>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#include "fmacros.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#include <assert.h>
#include <limits.h>
#include <sys/time.h>
#include <ctype.h>

#include "dict.h"
#include "zmalloc.h"

/* Using dictEnableResize() / dictDisableResize() we make possible to
 * enable/disable resizing of the hash table as needed. This is very important
 * for Redis, as we use copy-on-write and don't want to move too much memory
 * around when there is a child performing saving operations.
 *
 * Note that even when dict_can_resize is set to 0, not all resizes are
 * prevented: an hash table is still allowed to grow if the ratio between
 * the number of elements and the buckets > dict_force_resize_ratio. */
static int dict_can_resize = 1;
static unsigned int dict_force_resize_ratio = 5;

/* -------------------------- private prototypes ---------------------------- */

static int _dictExpandIfNeeded(dict *ht);
static unsigned long _dictNextPower(unsigned long size);
static int _dictKeyIndex(dict *ht, const void *key);
static int _dictInit(dict *ht, dictType *type, void *privDataPtr);

/* -------------------------- hash functions -------------------------------- */

/* Thomas Wang 的 32 位 Mix 函数。
 * 
 * 给出分布均匀的哈希值，计算速度也很快。
 * 当 table size 为 2 的次方时，可以用很快的速度计算出地址。
 * (Redis 也的确是这样做的, 参见 _dictNextPower 函数)
 * 
 * 详情参见： http://www.concentric.net/~ttwang/tech/inthash.htm
 *
 */
unsigned int dictIntHashFunction(unsigned int key)
{
    key += ~(key << 15);
    key ^=  (key >> 10);
    key +=  (key << 3);
    key ^=  (key >> 6);
    key += ~(key << 11);
    key ^=  (key >> 16);

    return key;
}

/* Identity hash function for integer keys */
unsigned int dictIdentityHashFunction(unsigned int key)
{
    return key;
}

static int dict_hash_function_seed = 5381;

void dictSetHashFunctionSeed(unsigned int seed) {
    dict_hash_function_seed = seed;
}

unsigned int dictGetHashFunctionSeed(void) {
    return dict_hash_function_seed;
}

/* Generic hash function (a popular one from Bernstein).
 * I tested a few and this was the best. */
unsigned int dictGenHashFunction(const unsigned char *buf, int len) {
    unsigned int hash = dict_hash_function_seed;

    while (len--)
        hash = ((hash << 5) + hash) + (*buf++); /* hash * 33 + c */
    return hash;
}

/* And a case insensitive version */
unsigned int dictGenCaseHashFunction(const unsigned char *buf, int len) {
    unsigned int hash = dict_hash_function_seed;

    while (len--)
        hash = ((hash << 5) + hash) + (tolower(*buf++)); /* hash * 33 + c */
    return hash;
}

/* ----------------------------- API implementation ------------------------- */

/* 重置哈希表 */
static void _dictReset(dictht *ht)
{
    ht->table = NULL;
    ht->size = 0;
    ht->sizemask = 0;
    ht->used = 0;
}

/* 创建一个新字典 */
dict *dictCreate(dictType *type, void *privDataPtr)
{
    dict *d = zmalloc(sizeof(*d));

    _dictInit(d,type,privDataPtr);
    return d;
}

/* 初始化字典 */
int _dictInit(dict *d, dictType *type, void *privDataPtr)
{
    _dictReset(&d->ht[0]);      // 初始化字典内的两个哈希表
    _dictReset(&d->ht[1]);

    d->type = type;             // 设置函数指针
    d->privdata = privDataPtr; 
    d->rehashidx = -1;          // -1 表示没有在进行 rehash
    d->iterators = 0;           // 0 表示没有迭代器在进行迭代

    return DICT_OK;             // 返回成功信号
}


/* 将哈希表的大小缩小到仅仅足以容纳当前已有的元素
 * 将 USED/BUCKETS 的 ratio 保持在 <= 1.0 的范围
 */
int dictResize(dict *d)
{
    int minimal;

    // 不能在 !dict_can_resize 或 rehash 时进行
    if (!dict_can_resize || dictIsRehashing(d)) return DICT_ERR;

    minimal = d->ht[0].used;
    if (minimal < DICT_HT_INITIAL_SIZE)
        minimal = DICT_HT_INITIAL_SIZE;

    return dictExpand(d, minimal);
}

/* 对字典进行扩展
 *
 * 这个函数完成以下两个工作的其中之一：
 * 1) 如果字典的 0 号哈希表不存在，那么创建它
 * 2) 如果字典的 0 号哈希表存在，那么创建字典的 1 号哈希表
 *
 * 这个函数的作用，对于第一件任务来说，是创建字典(的哈希表)。
 * 而对于第二个任务来说，则是扩展字典(的哈希表)。
 */
int dictExpand(dict *d, unsigned long size)
{
    // 一般调用函数已经进行了检查，为了安全起见再次对参数进行检查
    if (dictIsRehashing(d) || d->ht[0].used > size)
        return DICT_ERR;

    // 计算哈希表的(真正)大小
    unsigned long realsize = _dictNextPower(size);

    // 创建新哈希表
    dictht n; 
    n.size = realsize;
    n.sizemask = realsize-1;
    n.table = zcalloc(realsize*sizeof(dictEntry*));
    n.used = 0;

    // 字典的 0 号哈希表是否已经初始化？
    // 如果没有的话，我们将新建哈希表作为字典的 0 号哈希表
    if (d->ht[0].table == NULL) {
        d->ht[0] = n;
    } else {
    // 否则，将新建哈希表作为字典的 1 号哈希表，并将它用于 rehash
        d->ht[1] = n;
        d->rehashidx = 0;
    }

    return DICT_OK;
}

/* 字典(的哈希表) rehash 函数
 *
 * Args：
 *  d
 *  n 要执行 rehash 的元素数量
 *
 * Returns:
 *  0 所有元素 rehash 完毕
 *  1 还有元素没有 rehash
 */
int dictRehash(dict *d, int n) {
    if (!dictIsRehashing(d))
        return 0;

    while(n--) {
        dictEntry *de, *nextde;

        // 0 号哈希表的所有元素 rehash 完毕？
        if (d->ht[0].used == 0) {
            zfree(d->ht[0].table);  // 替换 1 号为 0 号
            d->ht[0] = d->ht[1];

            _dictReset(&d->ht[1]);  // 重置 1 号哈希表

            d->rehashidx = -1;      // 重置 rehash flag

            return 0;
        }

        /* Note that rehashidx can't overflow as we are sure there are more
         * elements because ht[0].used != 0 */
        assert(d->ht[0].size > (unsigned)d->rehashidx);

        // 略过所有空链
        while(d->ht[0].table[d->rehashidx] == NULL)
            d->rehashidx++;

        // 指向链头
        de = d->ht[0].table[d->rehashidx];
        // 将链表内的所有节点移动到 1 号哈希表
        while(de) {
            unsigned int h;

            nextde = de->next;

            // 计算新的地址(用于 1 号哈希表)
            h = dictHashKey(d, de->key) & d->ht[1].sizemask;

            de->next = d->ht[1].table[h];   // 更新 next 指针
            d->ht[1].table[h] = de;         // 移动
            d->ht[0].used--;                // 更新 0 号表计算器
            d->ht[1].used++;                // 更新 1 号表计算器

            de = nextde;
        }

        d->ht[0].table[d->rehashidx] = NULL;    // 清空链头
        d->rehashidx++; // 更新索引
    }

    return 1;
}

long long timeInMilliseconds(void) {
    struct timeval tv;

    gettimeofday(&tv,NULL);
    return (((long long)tv.tv_sec)*1000)+(tv.tv_usec/1000);
}

/* 在指定的时间内，进行 rehash 操作。
 *
 * Args: 
 *  d 
 *  ms 进行 rehash 的时间,以毫秒为单位 
 *
 * Returns:
 *  rehashes 完成 rehash 的元素的数量
 */
int dictRehashMilliseconds(dict *d, int ms) {
    long long start = timeInMilliseconds();
    int rehashes = 0;

    while(dictRehash(d,100)) {
        rehashes += 100;
        if (timeInMilliseconds()-start > ms) break;
    }
    return rehashes;
}

/* 在字典没有迭代器的情况下，rehash 一个元素
 * 通常被查找和更新函数所调用，作为平摊 rehash 操作
 *
 * 不能有迭代器(严格来说是不能有 safe iterator)的原因是
 * rehash 过程中不允许进行删除和插入，否则会造成元素的丢失或重复
 */
static void _dictRehashStep(dict *d) {
    if (d->iterators == 0) dictRehash(d,1);
}

/* 将元素添加到目标哈希表中
 *
 * Add an element to the target hash table
 *
 * Args:
 *  d 字典指针
 *  key 新元素的关键字
 *  val 新元素的值
 *
 * Returns:
 *  DICT_ERR 添加出错
 *  DICT_OK 添加成功
 *
 */
int dictAdd(dict *d, void *key, void *val)
{
    dictEntry *entry = dictAddRaw(d,key);

    if (!entry) return DICT_ERR;
    dictSetVal(d, entry, val);
    return DICT_OK;
}

/* 添加元素的底层实现函数(由 dictAdd 调用)
 * 
 * 新元素的添加操作被分为两步：
 * 1) 创建节点并设置节点的 key ，然后返回节点
 * 2) 设置节点的值
 * 这个函数执行第一步，第二步由函数 dictSetVal 进行
 * 
 * Args:
 *  d 字典
 *  key 新节点的关键字
 *
 * Returns:
 *  NULL 关键字已经存在
 *  entry 设置了关键字的新节点，返回给调用者进行进一步的处理
 */
dictEntry *dictAddRaw(dict *d, void *key)
{
    int index;
    dictEntry *entry;
    dictht *ht;

    // 检查字典(的哈希表)能否执行 rehash 操作
    // 如果可以的话，执行平摊 rehash 操作
    if (dictIsRehashing(d)) _dictRehashStep(d);

    // 计算 key 的 index 值
    // 如果 key 已经存在，_dictKeyIndex 返回 -1 
    if ((index = _dictKeyIndex(d, key)) == -1)
        return NULL;

    // 如果字典正在进行 rehash ，那么将新元素添加到 1 号哈希表，
    // 否则，使用 0 号哈希表
    ht = dictIsRehashing(d) ? &d->ht[1] : &d->ht[0];

    entry = zmalloc(sizeof(*entry));    // 为新节点分配内存
    entry->next = ht->table[index];     // 调整节点的 next 指针
    ht->table[index] = entry;           // 然后将新节点设为链头
    ht->used++;                         // 更新正在使用的节点数量

    // 设置节点的 key 域
    dictSetKey(d, entry, key);

    return entry;   // 返回新节点
}


/* 将新元素添加到字典，如果 key 已经存在，那么新元素覆盖旧元素。
 *
 * Args:
 *  d 
 *  key 新元素的关键字
 *  val 新元素的值
 *
 * Return:
 *  1 key 不存在，新建元素添加成功
 *  0 key 已经存在，旧元素被新元素覆盖
 */
int dictReplace(dict *d, void *key, void *val)
{
    dictEntry *entry, auxentry;

    // 尝试添加元素，如果 key 不存在， 那么 dictAdd 完成工作
    if (dictAdd(d, key, val) == DICT_OK)
        return 1;

    // dictAdd 不成功，说明 key 已经存在
    // 找出这个元素并更新它的值
    entry = dictFind(d, key);
    auxentry = *entry;          // 用变量保存 entry 的引用
    dictSetVal(d, entry, val);  // 设置新值
    dictFreeVal(d, &auxentry);  // 释放旧值
    return 0;
}

/* dictReplaceRaw() is simply a version of dictAddRaw() that always
 * returns the hash entry of the specified key, even if the key already
 * exists and can't be added (in that case the entry of the already
 * existing key is returned.)
 *
 * See dictAddRaw() for more information. */
dictEntry *dictReplaceRaw(dict *d, void *key) {
    dictEntry *entry = dictFind(d,key);

    return entry ? entry : dictAddRaw(d,key);
}

/* 删除指定元素的底层实现代码
 *
 * Args:
 *  d
 *  key
 *  nofree 指示是否释放被删除元素的键和值
 *
 * Returns:
 *  DICT_ERR 字典为空，删除失败
 *  DICT_OK 删除成功
 */
static int dictGenericDelete(dict *d, const void *key, int nofree)
{
    unsigned int h, idx;
    dictEntry *he, *prevHe;
    int table;

    // 字典为空，删除失败
    if (d->ht[0].size == 0)
        return DICT_ERR;

    // 平摊 rehash
    if (dictIsRehashing(d))
        _dictRehashStep(d);

    // 哈希值
    h = dictHashKey(d, key);

    // 遍历
    for (table = 0; table <= 1; table++) {
        idx = h & d->ht[table].sizemask;    // 地址索引
        he = d->ht[table].table[idx];       // 表头节点
        prevHe = NULL;
        while(he) {
            if (dictCompareKeys(d, key, he->key)) {
                /* Unlink the element from the list */
                if (prevHe)
                    prevHe->next = he->next;
                else
                    d->ht[table].table[idx] = he->next;

                if (!nofree) {
                    dictFreeKey(d, he);
                    dictFreeVal(d, he);
                }

                zfree(he);
                d->ht[table].used--;

                return DICT_OK;
            }
            // 推进指针
            prevHe = he;
            he = he->next;
        }
        if (!dictIsRehashing(d)) break;
    }

    return DICT_ERR; /* not found */
}

int dictDelete(dict *ht, const void *key) {
    return dictGenericDelete(ht,key,0);
}

int dictDeleteNoFree(dict *ht, const void *key) {
    return dictGenericDelete(ht,key,1);
}

/* 删除字典中指定的哈希表
 *
 * Destroy an entire dictionary
 *
 * Args:
 *  d 被删除的哈希表所属的字典
 *  ht 被删除的哈希表
 *
 * Returns:
 *  DICT_OK 删除成功(这个函数不可能失败)
 */
int _dictClear(dict *d, dictht *ht)
{
    unsigned long i;

    // 遍历整个哈希表，删除所有节点链
    /* Free all the elements */
    for (i = 0; i < ht->size && ht->used > 0; i++) {
        dictEntry *he, *nextHe;

        // 碰到空节点链，跳到下一个节点链去 
        if ((he = ht->table[i]) == NULL) continue;

        // 如果节点链非空，就遍历删除所有节点
        while(he) {
            nextHe = he->next;

            dictFreeKey(d, he); // 释放 key 空间
            dictFreeVal(d, he); // 释放 value 空间
            zfree(he);          // 释放节点

            ht->used--;         // 减少计数器

            he = nextHe;
        }
    }

    // 释放哈希表节点指针数组的空间
    /* Free the table and the allocated cache structure */
    zfree(ht->table);
    // 并重置(清空)哈希表各项属性
    /* Re-initialize the table */
    _dictReset(ht);

    return DICT_OK; /* never fails */
}

/* 删除字典
 *
 * Clear & Release the hash table
 *
 * Args:
 *  d 被删除的字典
 */
void dictRelease(dict *d)
{
    // 删除 0 号和 1 号哈希表
    _dictClear(d,&d->ht[0]);
    _dictClear(d,&d->ht[1]);

    // 释放字典结构
    zfree(d);
}

/* 从字典中查找给定 key 
 *
 * 查找过程是典型的 separate chaining find 操作
 * 具体参见：http://en.wikipedia.org/wiki/Hash_table#Separate_chaining
 */
dictEntry *dictFind(dict *d, const void *key)
{
    dictEntry *he;
    unsigned int h, idx, table;

    // 哈希表为空，直接返回 NULL
    if (d->ht[0].size == 0) return NULL; /* We don't have a table at all */

    // 检查字典(的哈希表)能否执行 rehash 操作
    // 如果可以的话，执行平摊 rehash 操作
    if (dictIsRehashing(d)) _dictRehashStep(d);

    // 查找 
    h = dictHashKey(d, key);                     // 计算哈希值
    for (table = 0; table <= 1; table++) {       // 遍历两个哈希表
        idx = h & d->ht[table].sizemask;         // 计算地址
        he = d->ht[table].table[idx];            // he 指向链表头
        while(he) {                              // 遍历链查找 key
            if (dictCompareKeys(d, key, he->key))
                return he;
            he = he->next;
        }

        if (!dictIsRehashing(d)) return NULL;
    }

    return NULL;
}

/* 查找给定 key 在字典 d 中的值
 *
 * 如果 key 不存在，返回 NULL
 */
void *dictFetchValue(dict *d, const void *key) {
    dictEntry *he;

    he = dictFind(d,key);
    return he ? dictGetVal(he) : NULL;
}

/* 创建一个迭代器，用于遍历哈希表节点。
 *
 * safe 属性指示迭代器是否安全
 * 如果迭代器是安全的，那么它可以在遍历的过程中进行增删操作
 * 反之，如果迭代器是不安全的，那么它只能执行 dictNext 操作
 *
 * 因为迭代进行的时候可以对列表的当前节点进行修改，
 * 为了避免修改造成指针丢失，
 * 所以不仅要有指向当前节点的 entry 属性，
 * 还需要指向下一节点的 nextEntry 属性 
 */
dictIterator *dictGetIterator(dict *d)
{
    dictIterator *iter = zmalloc(sizeof(*iter));

    iter->d = d;                // 字典
    iter->table = 0;            // 被遍历的哈希表的编号
    iter->index = -1;           // 索引
    iter->safe = 0;             // 是否一个安全哈希表？
    iter->entry = NULL;         // 当前节点指针
    iter->nextEntry = NULL;     // 下一个节点指针

    return iter;
}

/* 创建一个安全迭代器 */
dictIterator *dictGetSafeIterator(dict *d) {
    dictIterator *i = dictGetIterator(d);

    i->safe = 1;
    return i;
}

/* 迭代器的推进函数 */
dictEntry *dictNext(dictIterator *iter)
{
    while (1) {
        // 如果迭代器是新的(未使用过),那么初始化新迭代器
        if (iter->entry == NULL) {
            dictht *ht = &iter->d->ht[iter->table];

            // 在第一次迭代的时候增加字典 iterators 属性的计数
            if (iter->safe && iter->index == -1 && iter->table == 0)
                iter->d->iterators++;

            iter->index++;

            // 当迭代器的 index 值超过哈希表的 index 值时。。。
            if (iter->index >= (signed) ht->size) {
                // 如果字典正在进行 rehash ，那么转移阵地
                // 到 1 号哈希表继续迭代
                if (dictIsRehashing(iter->d) && iter->table == 0) {
                    iter->table++;
                    iter->index = 0;
                    ht = &iter->d->ht[1];
                // 否则，退出迭代
                } else {
                    break;
                }
            }

            // 设置指针，方便下一次迭代
            iter->entry = ht->table[iter->index];
        } else {
        // 这个不是新迭代器
            iter->entry = iter->nextEntry;
        }

        if (iter->entry) {
            /* We need to save the 'next' here, the iterator user
             * may delete the entry we are returning. */
            iter->nextEntry = iter->entry->next;
            return iter->entry;
        }
    }

    return NULL;
}

/* 删除迭代器 */
void dictReleaseIterator(dictIterator *iter)
{
    if (iter->safe && !(iter->index == -1 && iter->table == 0))
        iter->d->iterators--;
    zfree(iter);
}

/* Return a random entry from the hash table. Useful to
 * implement randomized algorithms */
dictEntry *dictGetRandomKey(dict *d)
{
    dictEntry *he, *orighe;
    unsigned int h;
    int listlen, listele;

    if (dictSize(d) == 0) return NULL;
    if (dictIsRehashing(d)) _dictRehashStep(d);
    if (dictIsRehashing(d)) {
        do {
            h = random() % (d->ht[0].size+d->ht[1].size);
            he = (h >= d->ht[0].size) ? d->ht[1].table[h - d->ht[0].size] :
                                      d->ht[0].table[h];
        } while(he == NULL);
    } else {
        do {
            h = random() & d->ht[0].sizemask;
            he = d->ht[0].table[h];
        } while(he == NULL);
    }

    /* Now we found a non empty bucket, but it is a linked
     * list and we need to get a random element from the list.
     * The only sane way to do so is counting the elements and
     * select a random index. */
    listlen = 0;
    orighe = he;
    while(he) {
        he = he->next;
        listlen++;
    }
    listele = random() % listlen;
    he = orighe;
    while(listele--) he = he->next;
    return he;
}

/* ------------------------- private functions ------------------------------ */

/* Expand the hash table if needed */
static int _dictExpandIfNeeded(dict *d)
{
    /* Incremental rehashing already in progress. Return. */
    if (dictIsRehashing(d)) return DICT_OK;

    /* If the hash table is empty expand it to the intial size. */
    if (d->ht[0].size == 0) return dictExpand(d, DICT_HT_INITIAL_SIZE);

    /* If we reached the 1:1 ratio, and we are allowed to resize the hash
     * table (global setting) or we should avoid it but the ratio between
     * elements/buckets is over the "safe" threshold, we resize doubling
     * the number of buckets. */
    // 当 0 号哈希表的已用节点数大于等于它的桶数量，
    // 且以下两个条件的其中之一被满足时，执行 expand 操作：
    // 1) dict_can_resize 变量为真，正常 expand
    // 2) 已用节点数除以桶数量的比率超过变量 dict_force_resize_ratio ，强制 expand
    // (目前版本中 dict_force_resize_ratio = 5)
    if (d->ht[0].used >= d->ht[0].size &&
        (dict_can_resize ||
         d->ht[0].used/d->ht[0].size > dict_force_resize_ratio))
    {
        return dictExpand(d, ((d->ht[0].size > d->ht[0].used) ?
                                    d->ht[0].size : d->ht[0].used)*2);
    }
    return DICT_OK;
}

/* 计算 2 的次方,用作表的大小
 *
 * Our hash table capability is a power of two
 * 
 * Args:
 *  size 当前表大小(一个 2 次方幂)
 *
 * Returns:
 *  i 新的表大小(比 size 大的 2 次方幂)
 *
 */
static unsigned long _dictNextPower(unsigned long size)
{
    unsigned long i = DICT_HT_INITIAL_SIZE;

    if (size >= LONG_MAX) return LONG_MAX;
    while(1) {
        if (i >= size)
            return i;
        i *= 2;
    }
}

/* Returns the index of a free slot that can be populated with
 * an hash entry for the given 'key'.
 * If the key already exists, -1 is returned.
 *
 * Note that if we are in the process of rehashing the hash table, the
 * index is always returned in the context of the second (new) hash table. */
static int _dictKeyIndex(dict *d, const void *key)
{
    unsigned int h, idx, table;
    dictEntry *he;

    /* Expand the hashtable if needed */
    if (_dictExpandIfNeeded(d) == DICT_ERR)
        return -1;
    /* Compute the key hash value */
    h = dictHashKey(d, key);
    for (table = 0; table <= 1; table++) {
        idx = h & d->ht[table].sizemask;
        /* Search if this slot does not already contain the given key */
        he = d->ht[table].table[idx];
        while(he) {
            if (dictCompareKeys(d, key, he->key))
                return -1;
            he = he->next;
        }
        if (!dictIsRehashing(d)) break;
    }
    return idx;
}

/* 清空字典
 *
 * 此操作删除字典的两个哈希表
 * 并重置 rehashidx 和 iterators 属性
 * 但并不删除(释放)字典本身
 *
 * 删除字典的代码在 dictRelease 函数
 *
 */
void dictEmpty(dict *d) {
    _dictClear(d,&d->ht[0]);
    _dictClear(d,&d->ht[1]);
    d->rehashidx = -1;
    d->iterators = 0;
}

#define DICT_STATS_VECTLEN 50
static void _dictPrintStatsHt(dictht *ht) {
    unsigned long i, slots = 0, chainlen, maxchainlen = 0;
    unsigned long totchainlen = 0;
    unsigned long clvector[DICT_STATS_VECTLEN];

    if (ht->used == 0) {
        printf("No stats available for empty dictionaries\n");
        return;
    }

    for (i = 0; i < DICT_STATS_VECTLEN; i++) clvector[i] = 0;
    for (i = 0; i < ht->size; i++) {
        dictEntry *he;

        if (ht->table[i] == NULL) {
            clvector[0]++;
            continue;
        }
        slots++;
        /* For each hash entry on this slot... */
        chainlen = 0;
        he = ht->table[i];
        while(he) {
            chainlen++;
            he = he->next;
        }
        clvector[(chainlen < DICT_STATS_VECTLEN) ? chainlen : (DICT_STATS_VECTLEN-1)]++;
        if (chainlen > maxchainlen) maxchainlen = chainlen;
        totchainlen += chainlen;
    }
    printf("Hash table stats:\n");
    printf(" table size: %ld\n", ht->size);
    printf(" number of elements: %ld\n", ht->used);
    printf(" different slots: %ld\n", slots);
    printf(" max chain length: %ld\n", maxchainlen);
    printf(" avg chain length (counted): %.02f\n", (float)totchainlen/slots);
    printf(" avg chain length (computed): %.02f\n", (float)ht->used/slots);
    printf(" Chain length distribution:\n");
    for (i = 0; i < DICT_STATS_VECTLEN-1; i++) {
        if (clvector[i] == 0) continue;
        printf("   %s%ld: %ld (%.02f%%)\n",(i == DICT_STATS_VECTLEN-1)?">= ":"", i, clvector[i], ((float)clvector[i]/ht->size)*100);
    }
}

void dictPrintStats(dict *d) {
    _dictPrintStatsHt(&d->ht[0]);
    if (dictIsRehashing(d)) {
        printf("-- Rehashing into ht[1]:\n");
        _dictPrintStatsHt(&d->ht[1]);
    }
}

void dictEnableResize(void) {
    dict_can_resize = 1;
}

void dictDisableResize(void) {
    dict_can_resize = 0;
}

#if 0

/* The following are just example hash table types implementations.
 * Not useful for Redis so they are commented out.
 */

/* ----------------------- StringCopy Hash Table Type ------------------------*/

static unsigned int _dictStringCopyHTHashFunction(const void *key)
{
    return dictGenHashFunction(key, strlen(key));
}

static void *_dictStringDup(void *privdata, const void *key)
{
    int len = strlen(key);
    char *copy = zmalloc(len+1);
    DICT_NOTUSED(privdata);

    memcpy(copy, key, len);
    copy[len] = '\0';
    return copy;
}

static int _dictStringCopyHTKeyCompare(void *privdata, const void *key1,
        const void *key2)
{
    DICT_NOTUSED(privdata);

    return strcmp(key1, key2) == 0;
}

static void _dictStringDestructor(void *privdata, void *key)
{
    DICT_NOTUSED(privdata);

    zfree(key);
}

dictType dictTypeHeapStringCopyKey = {
    _dictStringCopyHTHashFunction, /* hash function */
    _dictStringDup,                /* key dup */
    NULL,                          /* val dup */
    _dictStringCopyHTKeyCompare,   /* key compare */
    _dictStringDestructor,         /* key destructor */
    NULL                           /* val destructor */
};

/* This is like StringCopy but does not auto-duplicate the key.
 * It's used for intepreter's shared strings. */
dictType dictTypeHeapStrings = {
    _dictStringCopyHTHashFunction, /* hash function */
    NULL,                          /* key dup */
    NULL,                          /* val dup */
    _dictStringCopyHTKeyCompare,   /* key compare */
    _dictStringDestructor,         /* key destructor */
    NULL                           /* val destructor */
};

/* This is like StringCopy but also automatically handle dynamic
 * allocated C strings as values. */
dictType dictTypeHeapStringCopyKeyValue = {
    _dictStringCopyHTHashFunction, /* hash function */
    _dictStringDup,                /* key dup */
    _dictStringDup,                /* val dup */
    _dictStringCopyHTKeyCompare,   /* key compare */
    _dictStringDestructor,         /* key destructor */
    _dictStringDestructor,         /* val destructor */
};
#endif
