////===================================================================
 //
 // ob_query_engine.h / hash / common / Oceanbase
 //
 // Copyright (C) 2010, 2012 Taobao.com, Inc.
 //
 // Created on 2012-03-28 by Yubai (yubai.lk@taobao.com)
 //
 // -------------------------------------------------------------------
 //
 // Description
 //
 //
 // -------------------------------------------------------------------
 //
 // Change Log
 //
////====================================================================

#ifndef  OCEANBASE_UPDATESERVER_QUERY_ENGINE_H_
#define  OCEANBASE_UPDATESERVER_QUERY_ENGINE_H_
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <pthread.h>
#include <new>
#include <algorithm>
#include "common/ob_define.h"
#include "common/btree/key_btree.h"
#include "common/hash/ob_hashmap.h"
#include "common/page_arena.h"
#include "common/ob_list.h"
#include "ob_table_engine.h"
#include "ob_memtank.h"
#include "ob_btree_engine_alloc.h"
#include "ob_lighty_hash.h"

namespace oceanbase
{
  namespace updateserver
  {
    class HashEngineAllocator
    {
      public:
        HashEngineAllocator() : mt_(NULL) {};
        ~HashEngineAllocator() {};
      public:
        void *alloc(const int64_t size)
        {
          return mt_ ? mt_->hash_engine_alloc(static_cast<int32_t>(size)) : NULL;
        };
        void free(void *ptr)
        {
          UNUSED(ptr);
        };
        void set_mem_tank(MemTank *mt)
        {
          mt_ = mt;
        };
      private:
        MemTank *mt_;
    };

    class QueryEngineIterator;
    class QueryEngine
    {
      friend class QueryEngineIterator;
      typedef common::KeyBtree<TEKey, TEValue*> keybtree_t;
      typedef lightyhash::LightyHashMap<TEHashKey, TEValue*, HashEngineAllocator, HashEngineAllocator> keyhash_t;
      static const int64_t HASH_SIZE = 50000000;
      public:
        QueryEngine();
        ~QueryEngine();
      public:
        int init(MemTank *allocer);
        int destroy();
      public:
        // 插入key-value对
        // @param [in] key 待插入的key
        // @param [in] value 待插入的value
        int set(const TEKey &key, TEValue *value);

        // 获取指定key的value
        // @param [in] key 要查询的key
        // @param [out] value 查询返回的value
        TEValue *get(const TEKey &key);

        // 范围查询，返回一个iterator
        // @param [in] 查询范围的start key
        // @param [in] 是否由min key开始查询
        // @param [in] 查询范围是否包含start key本身, 0为包含, 非0为不包含;
        // @param [in] 查询范围的end key
        // @param [in] 是否由max key开始查询
        // @param [in] 查询范围是否包含end key本身, 0为包含, 非0为不包含;
        // @param [out] iter 查询结果的迭代器
        int scan(const TEKey &start_key,
                const int min_key,
                const int start_exclude,
                const TEKey &end_key,
                const int max_key,
                const int end_exclude,
                const bool reverse,
                QueryEngineIterator &iter);

        int clear();

        void dump2text(const char *fname);

        int64_t btree_size();
        int64_t hash_size() const;
        int64_t hash_bucket_using() const;
        int64_t hash_uninit_unit_num() const;
      private:
        bool inited_;
        UpsBtreeEngineAlloc btree_key_alloc_;
        UpsBtreeEngineAlloc btree_node_alloc_;
        HashEngineAllocator hash_bucket_alloc_;
        HashEngineAllocator hash_node_alloc_;
        keybtree_t keybtree_;
        keyhash_t keyhash_;
    };

    class QueryEngineIterator
    {
      friend class QueryEngine;
      public:
        QueryEngineIterator();
        ~QueryEngineIterator();
      public:
        // 迭代下一个元素
        int next();
        // 将迭代器指向的TEValue返回
        // @return  0  成功
        const TEKey &get_key() const;
        TEValue *get_value();
        // 重置迭代器
        void reset();
      private:
        void set_(QueryEngine::keybtree_t *keybtree);
        common::BtreeReadHandle &get_read_handle_();
      private:
        QueryEngine::keybtree_t *keybtree_;
        common::BtreeReadHandle read_handle_;
        TEKey key_;
        TEValue *pvalue_;
    };
  }
}

#endif //OCEANBASE_UPDATESERVER_QUERY_ENGINE_H_
