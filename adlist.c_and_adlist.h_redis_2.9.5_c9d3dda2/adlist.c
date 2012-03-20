/* adlist.c - A generic doubly linked list implementation
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


#include <stdlib.h>
#include "adlist.h"
#include "zmalloc.h"

/* Create a new list. The created list can be freed with
 * AlFreeList(), but private value of every node need to be freed
 * by the user before to call AlFreeList().
 *
 * On error, NULL is returned. Otherwise the pointer to the new list. */
/* 创建一个新列表
 *
 * 这个列表可以通过 AlFreeList() 来释放，记得释放列表前用户必须
 * 先释放(每个)节点里的私有值(private value)。
 *
 * Returns:
 *  list
 *  NULL 创建失败
 */
list *listCreate(void)
{
    struct list *list;

    if ((list = zmalloc(sizeof(*list))) == NULL)
        return NULL;

    list->head = list->tail = NULL;
    list->len = 0;
    list->dup = NULL;
    list->free = NULL;
    list->match = NULL;

    return list;
}

/* Free the whole list.
 *
 * This function can't fail. */
/* 释放整个列表
 *
 * 这个函数不会失败。
 */
void listRelease(list *list)
{
    unsigned long len;
    listNode *current, *next;

    current = list->head;
    len = list->len;
    while(len--) {
        next = current->next;
        // 如果有给定列表的 free 函数
        // 用它释放节点的值
        if (list->free) list->free(current->value);
        zfree(current);
        current = next;
    }
    zfree(list);
}

/* Add a new node to the list, to head, contaning the specified 'value'
 * pointer as value.
 *
 * On error, NULL is returned and no operation is performed (i.e. the
 * list remains unaltered).
 * On success the 'list' pointer you pass to the function is returned. */
/* 将包含给定值 value 的节点添加到表头。
 *
 * 如果添加成功，返回输入的列表指针 list
 * 如果添加失败，返回 NULL ，且没有操作被执行
 */
list *listAddNodeHead(list *list, void *value)
{
    listNode *node;

    if ((node = zmalloc(sizeof(*node))) == NULL)
        return NULL;

    node->value = value;

    // 空表
    if (list->len == 0) {
        list->head = list->tail = node;
        node->prev = node->next = NULL;
    } else {
    // 非空表
        node->prev = NULL;
        node->next = list->head;
        list->head->prev = node;
        list->head = node;
    }

    list->len++;

    return list;
}

/* Add a new node to the list, to tail, contaning the specified 'value'
 * pointer as value.
 *
 * On error, NULL is returned and no operation is performed (i.e. the
 * list remains unaltered).
 * On success the 'list' pointer you pass to the function is returned. */
/* 将带有给定值的 value 的节点添加到列表的表尾
 *
 * 如果添加成功，返回输入的列表指针 list 
 * 如果添加失败，返回 NULL ，并且没有操作被执行
 */
list *listAddNodeTail(list *list, void *value)
{
    listNode *node;

    if ((node = zmalloc(sizeof(*node))) == NULL)
        return NULL;

    node->value = value;
    // 空表
    if (list->len == 0) {
        list->head = list->tail = node;
        node->prev = node->next = NULL;
    } else {
    // 非空表
        node->prev = list->tail;
        node->next = NULL;
        list->tail->next = node;
        list->tail = node;
    }

    list->len++;

    return list;
}

/* 将带有给定值 value 的节点添加到给定节点 old_node 的之前或之后
 * 添加的位置（前或后）由参数 after 决定
 */
list *listInsertNode(list *list, listNode *old_node, void *value, int after) {
    listNode *node;

    if ((node = zmalloc(sizeof(*node))) == NULL)
        return NULL;

    node->value = value;

    // 添加到节点之后
    if (after) {
        node->prev = old_node;
        node->next = old_node->next;
        // 处理表尾 case
        if (list->tail == old_node) {
            list->tail = node;
        }
    } else {
    // 添加到节点之前
        node->next = old_node;
        node->prev = old_node->prev;
        // 处理表头 case
        if (list->head == old_node) {
            list->head = node;
        }
    }
    
    // 处理前节点和后节点
    if (node->prev != NULL) {
        node->prev->next = node;
    }
    if (node->next != NULL) {
        node->next->prev = node;
    }

    // 更新 len
    list->len++;

    return list;
}

/* Remove the specified node from the specified list.
 * It's up to the caller to free the private value of the node.
 *
 * This function can't fail. */
/* 从列表中移除给定的节点
 * 释放节点的私有值由调用者进行
 *
 * 这个函数不可能失败
 */
void listDelNode(list *list, listNode *node)
{   
    // 调整被删节点的前节点指针
    if (node->prev)
        node->prev->next = node->next;
    else
        list->head = node->next;
    
    // 调整被删节点的后节点指针
    if (node->next)
        node->next->prev = node->prev;
    else
        list->tail = node->prev;

    if (list->free) list->free(node->value);

    zfree(node);

    list->len--;
}

/* Returns a list iterator 'iter'. After the initialization every
 * call to listNext() will return the next element of the list.
 *
 * This function can't fail. */
/* 创建并返回列表的迭代器
 * 之后每次调用 listNext() 将返回列表的下一个元素
 * 
 * 迭代的方向（从前到后或从后到前）由参数 direction 决定
 *
 * 这个函数不可能失败
 */
listIter *listGetIterator(list *list, int direction)
{
    listIter *iter;
    
    if ((iter = zmalloc(sizeof(*iter))) == NULL) return NULL;

    // 根据迭代的方向选择指向头节点还是尾节点
    if (direction == AL_START_HEAD)
        iter->next = list->head;
    else
        iter->next = list->tail;

    iter->direction = direction;

    return iter;
}

/* Release the iterator memory */
void listReleaseIterator(listIter *iter) {
    zfree(iter);
}

/* Create an iterator in the list private iterator structure */
/* 重置迭代器的指针到表头 */
void listRewind(list *list, listIter *li) {
    li->next = list->head;
    li->direction = AL_START_HEAD;
}

/* 重置迭代器的指针到表尾 */
void listRewindTail(list *list, listIter *li) {
    li->next = list->tail;
    li->direction = AL_START_TAIL;
}

/* Return the next element of an iterator.
 * It's valid to remove the currently returned element using
 * listDelNode(), but not to remove other elements.
 *
 * The function returns a pointer to the next element of the list,
 * or NULL if there are no more elements, so the classical usage patter
 * is:
 *
 * iter = listGetIterator(list,<direction>);
 * while ((node = listNext(iter)) != NULL) {
 *     doSomethingWith(listNodeValue(node));
 * }
 *
 * */
// 这个函数的名字容易让人误解
// next 是对于迭代器本身来说的
// 实际上它返回的是当前(current)被迭代到的节点
// (虽然很多语言的迭代器都是这样用的，唉)
//
// 可以考虑将这个函数分为 getCurrent 和 iterNext
// 这样的话，代码会更好看：
//
// iter = listGetIterator(list, <direction>):
// while ((node = getCurrent(iter)) != NULL) {
//     doSomethingWith(listNodeValue(node));
//     iterNext(iter);
// }
listNode *listNext(listIter *iter)
{
    listNode *current = iter->next;

    if (current != NULL) {
        if (iter->direction == AL_START_HEAD)
            iter->next = current->next;
        else
            iter->next = current->prev;
    }

    return current;
}

/* Duplicate the whole list. On out of memory NULL is returned.
 * On success a copy of the original list is returned.
 *
 * The 'Dup' method set with listSetDupMethod() function is used
 * to copy the node value. Otherwise the same pointer value of
 * the original node is used as value of the copied node.
 *
 * The original list both on success or error is never modified. */
// 复制列表，输入的列表不会被修改
// 成功返回 copy 列表，失败返回 NULL
list *listDup(list *orig)
{
    list *copy;
    listIter *iter;
    listNode *node;

    if ((copy = listCreate()) == NULL)
        return NULL;

    copy->dup = orig->dup;
    copy->free = orig->free;
    copy->match = orig->match;

    // 使用迭代器，从表头开始复制
    iter = listGetIterator(orig, AL_START_HEAD);
    while((node = listNext(iter)) != NULL) {
        void *value;

        // 复制节点
        if (copy->dup) {
            value = copy->dup(node->value);
            // 复制失败，释放 copy 和 iter ，返回 NULL
            if (value == NULL) {
                listRelease(copy);
                listReleaseIterator(iter);
                return NULL;
            }
        } else
            value = node->value;
    
        // 将复制的节点加到表尾
        if (listAddNodeTail(copy, value) == NULL) {
            // 添加失败
            listRelease(copy);
            listReleaseIterator(iter);
            return NULL;
        }
    }

    listReleaseIterator(iter);

    return copy;
}

/* Search the list for a node matching a given key.
 * The match is performed using the 'match' method
 * set with listSetMatchMethod(). If no 'match' method
 * is set, the 'value' pointer of every node is directly
 * compared with the 'key' pointer.
 *
 * On success the first matching node pointer is returned
 * (search starts from head). If no matching node exists
 * NULL is returned. */
// 查找带有给定值 value 的节点
// 查找成功返回节点的指针，没找到返回 NULL
listNode *listSearchKey(list *list, void *key)
{
    listIter *iter;
    listNode *node;

    iter = listGetIterator(list, AL_START_HEAD);
    while((node = listNext(iter)) != NULL) {

        // 有 match 函数就使用 match 函数比对
        if (list->match) {
            if (list->match(node->value, key)) {
                listReleaseIterator(iter);
                return node;
            }
        } else {
        // 否则直接用值对比
            if (key == node->value) {
                listReleaseIterator(iter);
                return node;
            }
        }
    }

    listReleaseIterator(iter);
    return NULL;
}

/* Return the element at the specified zero-based index
 * where 0 is the head, 1 is the element next to head
 * and so on. Negative integers are used in order to count
 * from the tail, -1 is the last element, -2 the penultimante
 * and so on. If the index is out of range NULL is returned. */
// 根据给定的索引值返回相应的节点
// 以 0 为起始索引，索引可以是负数，-1 代表表尾
listNode *listIndex(list *list, long index) {
    listNode *n;

    // 负数索引
    if (index < 0) {
        index = (-index)-1;
        n = list->tail;
        while(index-- && n) n = n->prev;
    } else {
    // 非负数索引
        n = list->head;
        while(index-- && n) n = n->next;
    }

    return n;
}

/* Rotate the list removing the tail node and inserting it to the head. */
/* 翻转(rotate)列表，将表尾节点移动到表头（成为新的表头节点） */
void listRotate(list *list) {
    listNode *tail = list->tail;

    // 只有零个/一个节点有什么好翻转的，扯淡，return 得了
    if (listLength(list) <= 1) return;

    /* Detatch current tail */
    list->tail = tail->prev;
    list->tail->next = NULL;

    /* Move it as head */
    list->head->prev = tail;
    tail->prev = NULL;
    tail->next = list->head;
    list->head = tail;
}
