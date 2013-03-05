Redis 源码阅读
=================

对 Redis 源码的注释工作已经转移到 `annotated_redis_source <https://github.com/huangz1990/annotated_redis_source>`_ 项目，欢迎意见、 star 和 fork 。

huangz

2013.3.5


项目说明
-----------

学习 Redis 的源码，并加上相关的注释。

因为 Redis 的版本升级比较快，对源码的注释是滚动地在不同的版本上进行的，当然，注释之后会对源码进行测试，确保被注释的代码的正确性。
 
文件名说明了被注释的源码的文件名，所属的版本和 commit 哈希值。

比如：

dict.c_and_dict.h_redis_2.9.5_79642420

就是说：

文件 dict.c 、 dict.h

版本 redis 2.9.5

哈希 79642420

就是这样。
