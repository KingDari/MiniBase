# MiniBase
实习项目架构与HBase整体架构有相似之处，因此希望通过编写一个MiniBase来对其整体架构与实现方法有进一步的理解与掌握。
基础框架参照《HBase原理与实践》一书。
# 已实现
1. 单机、KV存储
2. 基于LSM树的读写压缩
3. 支持布隆过滤器
4. 支持SKIP/ASYNC/SYNC/FSYNC级别WAL
5. MVCC
6. LRU Block Cache
7. BinarySearch seek
# TODO
1. WAL GC
2. Block Storage: KV, KV, KV => KKKK, VVVV: Cache Friendly
4. Region Server: KV -> Column Family; Row Transaction
5. Distributed MiniBase: 6.824Raft as MetaCenter
