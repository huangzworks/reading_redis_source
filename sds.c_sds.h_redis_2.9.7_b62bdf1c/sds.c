/* SDSLib, A C dynamic strings library
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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <assert.h>
#include "sds.h"
#include "zmalloc.h"

// 根据给定初始化值和初始化长度
// 创建或重分配一个 sds
sds sdsnewlen(const void *init, size_t initlen) {
    struct sdshdr *sh;

    if (init) {
        // 创建
        sh = zmalloc(sizeof(struct sdshdr)+initlen+1);  
    } else {
        // 重分配
        sh = zcalloc(sizeof(struct sdshdr)+initlen+1);  
    }

    if (sh == NULL) return NULL;

    sh->len = initlen;
    sh->free = 0;   // 刚开始时 free 为 0

    // 设置字符串值
    if (initlen && init)
        memcpy(sh->buf, init, initlen); 
    sh->buf[initlen] = '\0';

    // 只返回 sh->buf 这个字符串部分
    return (char*)sh->buf;  
}

// 创建一个空 sds
sds sdsempty(void) {
    return sdsnewlen("",0);
}

// 根据给定字符串，创建一个新 sds 
sds sdsnew(const char *init) {
    size_t initlen = (init == NULL) ? 0 : strlen(init);
    return sdsnewlen(init, initlen);
}

// 复制给定 sds
sds sdsdup(const sds s) {
    return sdsnewlen(s, sdslen(s));
}

// 释放给定 sds
void sdsfree(sds s) {
    if (s == NULL) return;
    zfree(s-sizeof(struct sdshdr));
}

// 计算 sds 中字符串的实际长度
// 并更新 free 属性和 len 属性
void sdsupdatelen(sds s) {
    struct sdshdr *sh = (void*) (s-(sizeof(struct sdshdr)));

    int reallen = strlen(s);

    sh->free += (sh->len-reallen);
    sh->len = reallen;
}

// 将给定 sds 的字符串设为空字符串
// 并将 sds 的长度归零
void sdsclear(sds s) {
    struct sdshdr *sh = (void*) (s-(sizeof(struct sdshdr)));

    sh->free += sh->len;
    sh->len = 0;
    sh->buf[0] = '\0';  // 设为空字符串
}

/* Enlarge the free space at the end of the sds string so that the caller
 * is sure that after calling this function can overwrite up to addlen
 * bytes after the end of the string, plus one more byte for nul term.
 * 
 * Note: this does not change the *size* of the sds string as returned
 * by sdslen(), but only the free buffer space we have. */
// 扩展 sds 的预留空间， 确保在调用这个函数之后，
// sds 字符串后的 addlen + 1 bytes（for NULL） 可写
sds sdsMakeRoomFor(sds s, size_t addlen) {
    struct sdshdr *sh, *newsh;
    size_t free = sdsavail(s);
    size_t len, newlen;

    // 预留空间可以满足本次拼接
    if (free >= addlen) return s;

    len = sdslen(s);
    sh = (void*) (s-(sizeof(struct sdshdr)));

    // 设置新 sds 的字符串长度
    // 这个长度比完成本次拼接实际所需的长度要大
    // 通过预留空间优化下次拼接操作
    newlen = (len+addlen);
    if (newlen < SDS_MAX_PREALLOC)
        newlen *= 2;
    else
        newlen += SDS_MAX_PREALLOC;

    // 重分配 sdshdr
    newsh = zrealloc(sh, sizeof(struct sdshdr)+newlen+1);
    if (newsh == NULL) return NULL;

    newsh->free = newlen - len;

    // 只返回字符串部分
    return newsh->buf;
}

/* Reallocate the sds string so that it has no free space at the end. The
 * contained string remains not altered, but next concatenation operations
 * will require a reallocation. */
// 对 sds 进行紧缩，将字符串之后的预留空间全部释放
// 在执行此操作之后，下一次对这个 sds 进行拼接操作必然会导致重分配
sds sdsRemoveFreeSpace(sds s) {
    struct sdshdr *sh;

    sh = (void*) (s-(sizeof(struct sdshdr)));
    sh = zrealloc(sh, sizeof(struct sdshdr)+sh->len+1);
    sh->free = 0;

    return sh->buf;
}

// 返回 sds 字符串已占用的空间（包括已用和预留的，以及 sdshdr 结构的）
size_t sdsAllocSize(sds s) {
    struct sdshdr *sh = (void*) (s-(sizeof(struct sdshdr)));

    return sizeof(*sh) + sh->len + sh->free + 1;
}

/* Increment the sds length and decrements the left free space at the
 * end of the string accordingly to 'incr'. Also set the null term
 * in the new end of the string.
 *
 * This function is used in order to fix the string length after the
 * user calls sdsMakeRoomFor(), writes something after the end of
 * the current string, and finally needs to set the new length.
 *
 * Note: it is possible to use a negative increment in order to
 * right-trim the string.
 *
 * Using sdsIncrLen() and sdsMakeRoomFor() it is possible to mount the
 * following schema to cat bytes coming from the kerenl to the end of an
 * sds string new things without copying into an intermediate buffer:
 *
 * oldlen = sdslen(s);
 * s = sdsMakeRoomFor(s, BUFFER_SIZE);
 * nread = read(fd, s+oldlen, BUFFER_SIZE);
 * ... check for nread <= 0 and handle it ...
 * sdsIncrLen(s, nhread);
 */
// 根据 incr 参数，增大 sds 的长度，并缩减预留（free）空间的长度
// 并将 NULL 放到新字符串的结尾
//
// 这个函数通常用于执行 sdsMakeRoomFor() ，并且对字符串进行了写入之后，
// 对字符串现有的实际长度进行设置
//
// 注意：将 incr 参数设为负数可以对字符串进行向右截断
// 
// 以下是一个sdsMakeRoomFor 和 sdsIncrLen 的常见组合使用方法：
//
// oldlen = sdslen(s);
// s = sdsMakeRoomFor(s, BUFFER_SIZE);
// nread = read(fd, s+oldlen, BUFFER_SIZE);
// ... check for nread <= 0 and handle it ...
// sdsIncrLen(s, nhread);
//
void sdsIncrLen(sds s, int incr) {
    struct sdshdr *sh = (void*) (s-(sizeof(struct sdshdr)));

    // 确保有足够空间进行操作
    assert(sh->free >= incr);

    // 更新长度和预留空间长度
    sh->len += incr;
    sh->free -= incr;

    // 确保更新长度之后预留长度正确
    assert(sh->free >= 0);

    // 设置新 NULL
    s[sh->len] = '\0';
}

/* Grow the sds to have the specified length. Bytes that were not part of
 * the original length of the sds will be set to zero. */
// 扩展 sds 至指定长度
// 新增长度的空间用 0 来填充
sds sdsgrowzero(sds s, size_t len) {
    struct sdshdr *sh = (void*)(s-(sizeof(struct sdshdr)));
    size_t totlen, curlen = sh->len;

    // 长度不能过短
    if (len <= curlen) return s;
    // 扩展 sds
    s = sdsMakeRoomFor(s,len-curlen);
    if (s == NULL) return NULL;

    /* Make sure added region doesn't contain garbage */
    // 设置新 sds
    sh = (void*)(s-(sizeof(struct sdshdr)));
    // 用 0 进行填充
    memset(s+curlen,0,(len-curlen+1)); /* also set trailing \0 byte */
    totlen = sh->len + sh->free;
    sh->len = len;
    sh->free = totlen - sh->len;

    return s;
}

// 将长度为 len 的值 t 追加到 sds 之后
sds sdscatlen(sds s, const void *t, size_t len) {
    struct sdshdr *sh;
    size_t curlen = sdslen(s);

    s = sdsMakeRoomFor(s,len);
    if (s == NULL) return NULL;

    sh = (void*) (s-(sizeof(struct sdshdr)));

    // 在 sds 原来的尾部追加值 t
    memcpy(s+curlen, t, len);
    sh->len = curlen+len;
    sh->free = sh->free-len;
    s[curlen+len] = '\0';

    return s;
}

// 将值 t 追加到 sds 之后
sds sdscat(sds s, const char *t) {
    return sdscatlen(s, t, strlen(t));
}

// 将 sds 值 t 追加到 sds 值 s 之后
sds sdscatsds(sds s, const sds t) {
    return sdscatlen(s, t, sdslen(t));
}

// 将长度为 len 的字符串 t 复制到 sds
sds sdscpylen(sds s, const char *t, size_t len) {
    struct sdshdr *sh = (void*) (s-(sizeof(struct sdshdr)));
    size_t totlen = sh->free+sh->len;

    if (totlen < len) {
        // 如果长度不够就扩展
        s = sdsMakeRoomFor(s,len-sh->len);
        if (s == NULL) return NULL;
        sh = (void*) (s-(sizeof(struct sdshdr)));
        totlen = sh->free+sh->len;
    }
    // 复制
    memcpy(s, t, len);
    s[len] = '\0';
    sh->len = len;
    sh->free = totlen-len;
    return s;
}

// 将字符串 t 复制到 sds
sds sdscpy(sds s, const char *t) {
    return sdscpylen(s, t, strlen(t));
}

// 打印函数
sds sdscatvprintf(sds s, const char *fmt, va_list ap) {
    va_list cpy;
    char *buf, *t;
    size_t buflen = 16;

    while(1) {
        buf = zmalloc(buflen);
        if (buf == NULL) return NULL;
        buf[buflen-2] = '\0';
        va_copy(cpy,ap);
        vsnprintf(buf, buflen, fmt, cpy);
        if (buf[buflen-2] != '\0') {
            zfree(buf);
            buflen *= 2;
            continue;
        }
        break;
    }
    t = sdscat(s, buf);
    zfree(buf);
    return t;
}

// 打印函数
sds sdscatprintf(sds s, const char *fmt, ...) {
    va_list ap;
    char *t;
    va_start(ap, fmt);
    t = sdscatvprintf(s,fmt,ap);
    va_end(ap);
    return t;
}

// 截断函数
sds sdstrim(sds s, const char *cset) {
    struct sdshdr *sh = (void*) (s-(sizeof(struct sdshdr)));
    char *start, *end, *sp, *ep;
    size_t len;

    sp = start = s;
    ep = end = s+sdslen(s)-1;
    while(sp <= end && strchr(cset, *sp)) sp++;
    while(ep > start && strchr(cset, *ep)) ep--;
    len = (sp > ep) ? 0 : ((ep-sp)+1);
    if (sh->buf != sp) memmove(sh->buf, sp, len);
    sh->buf[len] = '\0';
    sh->free = sh->free+(sh->len-len);
    sh->len = len;
    return s;
}

// 返回 sds 指定区间内的值
sds sdsrange(sds s, int start, int end) {
    struct sdshdr *sh = (void*) (s-(sizeof(struct sdshdr)));
    size_t newlen, len = sdslen(s);

    if (len == 0) return s;
    if (start < 0) {
        start = len+start;
        if (start < 0) start = 0;
    }
    if (end < 0) {
        end = len+end;
        if (end < 0) end = 0;
    }
    newlen = (start > end) ? 0 : (end-start)+1;
    if (newlen != 0) {
        if (start >= (signed)len) {
            newlen = 0;
        } else if (end >= (signed)len) {
            end = len-1;
            newlen = (start > end) ? 0 : (end-start)+1;
        }
    } else {
        start = 0;
    }
    if (start && newlen) memmove(sh->buf, sh->buf+start, newlen);
    sh->buf[newlen] = 0;
    sh->free = sh->free+(sh->len-newlen);
    sh->len = newlen;
    return s;
}

// 将 sds 的内容全部转换为小写
void sdstolower(sds s) {
    int len = sdslen(s), j;

    for (j = 0; j < len; j++) s[j] = tolower(s[j]);
}

// 将 sds 的内容全部转换为大写
void sdstoupper(sds s) {
    int len = sdslen(s), j;

    for (j = 0; j < len; j++) s[j] = toupper(s[j]);
}

// 对两个 sds 进行对比 
int sdscmp(const sds s1, const sds s2) {
    size_t l1, l2, minlen;
    int cmp;

    l1 = sdslen(s1);
    l2 = sdslen(s2);
    minlen = (l1 < l2) ? l1 : l2;
    cmp = memcmp(s1,s2,minlen);
    if (cmp == 0) return l1-l2;
    return cmp;
}

/* Split 's' with separator in 'sep'. An array
 * of sds strings is returned. *count will be set
 * by reference to the number of tokens returned.
 *
 * On out of memory, zero length string, zero length
 * separator, NULL is returned.
 *
 * Note that 'sep' is able to split a string using
 * a multi-character separator. For example
 * sdssplit("foo_-_bar","_-_"); will return two
 * elements "foo" and "bar".
 *
 * This version of the function is binary-safe but
 * requires length arguments. sdssplit() is just the
 * same function but for zero-terminated strings.
 */
// TODO: 注释
// 使用分隔符 sep 去分割字符串 s
// 分割的结果会作为 sds 数组返回， count 参数是返回 token 的数量
//
// 内存越界，空字符串，空分隔符，以上情况都返回 NULL 
//
// 注意， sep 参数本身也可以是一个字符串（不仅仅是单个字符），
// 比如 sdssplit("foo_-_bar", "_-_"); 将返回 foo 和 bar
//
// 这个函数是 binary-safe 的，但是它要求给出长度参数。
// sdssplit() 函数和这个函数作用类似，但它以 \0 为分隔符
sds *sdssplitlen(const char *s, int len, const char *sep, int seplen, int *count) {
    int elements = 0, slots = 5, start = 0, j;
    sds *tokens;

    if (seplen < 1 || len < 0) return NULL;

    tokens = zmalloc(sizeof(sds)*slots);
    if (tokens == NULL) return NULL;

    if (len == 0) {
        *count = 0;
        return tokens;
    }
    for (j = 0; j < (len-(seplen-1)); j++) {
        /* make sure there is room for the next element and the final one */
        if (slots < elements+2) {
            sds *newtokens;

            slots *= 2;
            newtokens = zrealloc(tokens,sizeof(sds)*slots);
            if (newtokens == NULL) goto cleanup;
            tokens = newtokens;
        }
        /* search the separator */
        if ((seplen == 1 && *(s+j) == sep[0]) || (memcmp(s+j,sep,seplen) == 0)) {
            tokens[elements] = sdsnewlen(s+start,j-start);
            if (tokens[elements] == NULL) goto cleanup;
            elements++;
            start = j+seplen;
            j = j+seplen-1; /* skip the separator */
        }
    }
    /* Add the final element. We are sure there is room in the tokens array. */
    tokens[elements] = sdsnewlen(s+start,len-start);
    if (tokens[elements] == NULL) goto cleanup;
    elements++;
    *count = elements;
    return tokens;

cleanup:
    {
        int i;
        for (i = 0; i < elements; i++) sdsfree(tokens[i]);
        zfree(tokens);
        *count = 0;
        return NULL;
    }
}

// 释放给定 sds 数组内指定数量的 sds
void sdsfreesplitres(sds *tokens, int count) {
    if (!tokens) return;
    while(count--)
        sdsfree(tokens[count]);
    zfree(tokens);
}

// 从 long long 类型的值生成 sds
sds sdsfromlonglong(long long value) {
    char buf[32], *p;
    unsigned long long v;

    v = (value < 0) ? -value : value;
    p = buf+31; /* point to the last character */
    do {
        *p-- = '0'+(v%10);
        v /= 10;
    } while(v);
    if (value < 0) *p-- = '-';
    p++;
    return sdsnewlen(p,32-(p-buf));
}

// 对特殊字符进行转义
sds sdscatrepr(sds s, const char *p, size_t len) {
    s = sdscatlen(s,"\"",1);
    while(len--) {
        switch(*p) {
        case '\\':
        case '"':
            s = sdscatprintf(s,"\\%c",*p);
            break;
        case '\n': s = sdscatlen(s,"\\n",2); break;
        case '\r': s = sdscatlen(s,"\\r",2); break;
        case '\t': s = sdscatlen(s,"\\t",2); break;
        case '\a': s = sdscatlen(s,"\\a",2); break;
        case '\b': s = sdscatlen(s,"\\b",2); break;
        default:
            if (isprint(*p))
                s = sdscatprintf(s,"%c",*p);
            else
                s = sdscatprintf(s,"\\x%02x",(unsigned char)*p);
            break;
        }
        p++;
    }
    return sdscatlen(s,"\"",1);
}

/* Helper function for sdssplitargs() that returns non zero if 'c'
 * is a valid hex digit. */
// 检查字符 c 是否为十六进制字符
int is_hex_digit(char c) {
    return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') ||
           (c >= 'A' && c <= 'F');
}

/* Helper function for sdssplitargs() that converts an hex digit into an
 * integer from 0 to 15 */
// 将字符串 c 转换为十六进制字符
int hex_digit_to_int(char c) {
    switch(c) {
    case '0': return 0;
    case '1': return 1;
    case '2': return 2;
    case '3': return 3;
    case '4': return 4;
    case '5': return 5;
    case '6': return 6;
    case '7': return 7;
    case '8': return 8;
    case '9': return 9;
    case 'a': case 'A': return 10;
    case 'b': case 'B': return 11;
    case 'c': case 'C': return 12;
    case 'd': case 'D': return 13;
    case 'e': case 'E': return 14;
    case 'f': case 'F': return 15;
    default: return 0;
    }
}

/* Split a line into arguments, where every argument can be in the
 * following programming-language REPL-alike form:
 *
 * foo bar "newline are supported\n" and "\xff\x00otherstuff"
 *
 * The number of arguments is stored into *argc, and an array
 * of sds is returned. The caller should sdsfree() all the returned
 * strings and finally zfree() the array itself.
 *
 * Note that sdscatrepr() is able to convert back a string into
 * a quoted string in the same format sdssplitargs() is able to parse.
 */
// parse 输入字符串为参数，返回的结果是一个 sds 数组
// 参数的数量通过 argc 返回
//
// 调用者应该执行 sdsfree() 来释放 sds 数组中的字符串
// 并且调用 zfree() 释放数组本身
//
// 经过 sdscatrepr() 函数处理的字符串可以被这个函数处理
sds *sdssplitargs(const char *line, int *argc) {
    const char *p = line;
    char *current = NULL;
    char **vector = NULL;

    *argc = 0;
    while(1) {
        /* skip blanks */
        while(*p && isspace(*p)) p++;
        if (*p) {
            /* get a token */
            int inq=0;  /* set to 1 if we are in "quotes" */
            int insq=0; /* set to 1 if we are in 'single quotes' */
            int done=0;

            if (current == NULL) current = sdsempty();
            while(!done) {
                if (inq) {
                    if (*p == '\\' && *(p+1) == 'x' &&
                                             is_hex_digit(*(p+2)) &&
                                             is_hex_digit(*(p+3)))
                    {
                        unsigned char byte;

                        byte = (hex_digit_to_int(*(p+2))*16)+
                                hex_digit_to_int(*(p+3));
                        current = sdscatlen(current,(char*)&byte,1);
                        p += 3;
                    } else if (*p == '\\' && *(p+1)) {
                        char c;

                        p++;
                        switch(*p) {
                        case 'n': c = '\n'; break;
                        case 'r': c = '\r'; break;
                        case 't': c = '\t'; break;
                        case 'b': c = '\b'; break;
                        case 'a': c = '\a'; break;
                        default: c = *p; break;
                        }
                        current = sdscatlen(current,&c,1);
                    } else if (*p == '"') {
                        /* closing quote must be followed by a space or
                         * nothing at all. */
                        if (*(p+1) && !isspace(*(p+1))) goto err;
                        done=1;
                    } else if (!*p) {
                        /* unterminated quotes */
                        goto err;
                    } else {
                        current = sdscatlen(current,p,1);
                    }
                } else if (insq) {
                    if (*p == '\\' && *(p+1) == '\'') {
                        p++;
                        current = sdscatlen(current,"'",1);
                    } else if (*p == '\'') {
                        /* closing quote must be followed by a space or
                         * nothing at all. */
                        if (*(p+1) && !isspace(*(p+1))) goto err;
                        done=1;
                    } else if (!*p) {
                        /* unterminated quotes */
                        goto err;
                    } else {
                        current = sdscatlen(current,p,1);
                    }
                } else {
                    switch(*p) {
                    case ' ':
                    case '\n':
                    case '\r':
                    case '\t':
                    case '\0':
                        done=1;
                        break;
                    case '"':
                        inq=1;
                        break;
                    case '\'':
                        insq=1;
                        break;
                    default:
                        current = sdscatlen(current,p,1);
                        break;
                    }
                }
                if (*p) p++;
            }
            /* add the token to the vector */
            vector = zrealloc(vector,((*argc)+1)*sizeof(char*));
            vector[*argc] = current;
            (*argc)++;
            current = NULL;
        } else {
            return vector;
        }
    }

err:
    while((*argc)--)
        sdsfree(vector[*argc]);
    zfree(vector);
    if (current) sdsfree(current);
    return NULL;
}

// 释放 sds 数组
void sdssplitargs_free(sds *argv, int argc) {
    int j;

    for (j = 0 ;j < argc; j++) sdsfree(argv[j]);
    zfree(argv);
}

/* Modify the string substituting all the occurrences of the set of
 * characters specifed in the 'from' string to the corresponding character
 * in the 'to' array.
 *
 * For instance: sdsmapchars(mystring, "ho", "01", 2)
 * will have the effect of turning the string "hello" into "0ell1".
 *
 * The function returns the sds string pointer, that is always the same
 * as the input pointer since no resize is needed. */
// 对字符串进行替换（substitue）
// 例子：
// char* mystring = "hello";
// sdsmapchars(mystring, "ho", 01", 2);
// 返回：
// "0ell1"
//
// 函数的返回值是 sds 指针
sds sdsmapchars(sds s, const char *from, const char *to, size_t setlen) {
    size_t j, i, l = sdslen(s);

    for (j = 0; j < l; j++) {
        for (i = 0; i < setlen; i++) {
            if (s[j] == from[i]) {
                s[j] = to[i];
                break;
            }
        }
    }
    return s;
}

#ifdef SDS_TEST_MAIN
#include <stdio.h>
#include "testhelp.h"

int main(void) {
    {
        struct sdshdr *sh;
        sds x = sdsnew("foo"), y;

        test_cond("Create a string and obtain the length",
            sdslen(x) == 3 && memcmp(x,"foo\0",4) == 0)

        sdsfree(x);
        x = sdsnewlen("foo",2);
        test_cond("Create a string with specified length",
            sdslen(x) == 2 && memcmp(x,"fo\0",3) == 0)

        x = sdscat(x,"bar");
        test_cond("Strings concatenation",
            sdslen(x) == 5 && memcmp(x,"fobar\0",6) == 0);

        x = sdscpy(x,"a");
        test_cond("sdscpy() against an originally longer string",
            sdslen(x) == 1 && memcmp(x,"a\0",2) == 0)

        x = sdscpy(x,"xyzxxxxxxxxxxyyyyyyyyyykkkkkkkkkk");
        test_cond("sdscpy() against an originally shorter string",
            sdslen(x) == 33 &&
            memcmp(x,"xyzxxxxxxxxxxyyyyyyyyyykkkkkkkkkk\0",33) == 0)

        sdsfree(x);
        x = sdscatprintf(sdsempty(),"%d",123);
        test_cond("sdscatprintf() seems working in the base case",
            sdslen(x) == 3 && memcmp(x,"123\0",4) ==0)

        sdsfree(x);
        x = sdstrim(sdsnew("xxciaoyyy"),"xy");
        test_cond("sdstrim() correctly trims characters",
            sdslen(x) == 4 && memcmp(x,"ciao\0",5) == 0)

        y = sdsrange(sdsdup(x),1,1);
        test_cond("sdsrange(...,1,1)",
            sdslen(y) == 1 && memcmp(y,"i\0",2) == 0)

        sdsfree(y);
        y = sdsrange(sdsdup(x),1,-1);
        test_cond("sdsrange(...,1,-1)",
            sdslen(y) == 3 && memcmp(y,"iao\0",4) == 0)

        sdsfree(y);
        y = sdsrange(sdsdup(x),-2,-1);
        test_cond("sdsrange(...,-2,-1)",
            sdslen(y) == 2 && memcmp(y,"ao\0",3) == 0)

        sdsfree(y);
        y = sdsrange(sdsdup(x),2,1);
        test_cond("sdsrange(...,2,1)",
            sdslen(y) == 0 && memcmp(y,"\0",1) == 0)

        sdsfree(y);
        y = sdsrange(sdsdup(x),1,100);
        test_cond("sdsrange(...,1,100)",
            sdslen(y) == 3 && memcmp(y,"iao\0",4) == 0)

        sdsfree(y);
        y = sdsrange(sdsdup(x),100,100);
        test_cond("sdsrange(...,100,100)",
            sdslen(y) == 0 && memcmp(y,"\0",1) == 0)

        sdsfree(y);
        sdsfree(x);
        x = sdsnew("foo");
        y = sdsnew("foa");
        test_cond("sdscmp(foo,foa)", sdscmp(x,y) > 0)

        sdsfree(y);
        sdsfree(x);
        x = sdsnew("bar");
        y = sdsnew("bar");
        test_cond("sdscmp(bar,bar)", sdscmp(x,y) == 0)

        sdsfree(y);
        sdsfree(x);
        x = sdsnew("aar");
        y = sdsnew("bar");
        test_cond("sdscmp(bar,bar)", sdscmp(x,y) < 0)

        {
            int oldfree;

            sdsfree(x);
            x = sdsnew("0");
            sh = (void*) (x-(sizeof(struct sdshdr)));
            test_cond("sdsnew() free/len buffers", sh->len == 1 && sh->free == 0);
            x = sdsMakeRoomFor(x,1);
            sh = (void*) (x-(sizeof(struct sdshdr)));
            test_cond("sdsMakeRoomFor()", sh->len == 1 && sh->free > 0);
            oldfree = sh->free;
            x[1] = '1';
            sdsIncrLen(x,1);
            test_cond("sdsIncrLen() -- content", x[0] == '0' && x[1] == '1');
            test_cond("sdsIncrLen() -- len", sh->len == 2);
            test_cond("sdsIncrLen() -- free", sh->free == oldfree-1);
        }
    }
    test_report()
    return 0;
}
#endif
