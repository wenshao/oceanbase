////===================================================================
 //
 // ob_hashmap.cpp / hash / common / Oceanbase
 //
 // Copyright (C) 2010 Taobao.com, Inc.
 //
 // Created on 2010-08-05 by Yubai (yubai.lk@taobao.com) 
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

#ifndef  OCEANBASE_COMMON_HASH_HASHMAP_H_
#define  OCEANBASE_COMMON_HASH_HASHMAP_H_
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <pthread.h>
#include <new>
#include "ob_hashutils.h"
#include "ob_hashtable.h"
#include "ob_serialization.h"

namespace oceanbase
{
  namespace common
  {
    namespace hash
    {
      template <class _key_type, class _value_type>
      struct HashMapTypes
      {
        typedef HashMapPair<_key_type, _value_type> pair_type;
        typedef typename HashTableTypes<pair_type>::AllocType AllocType;
      };

      template <class _key_type, class _value_type>
      struct hashmap_preproc
      {
        typedef typename HashMapTypes<_key_type, _value_type>::pair_type pair_type;
        _value_type &operator () (pair_type &pair) const
        {
          return pair.second;
        };
        char buf[0];
      };

      template <class _key_type, class _value_type,
                class _defendmode = ReadWriteDefendMode,
                class _hashfunc = hash_func<_key_type>,
                class _equal = equal_to<_key_type>,
                //class _allocer = SimpleAllocer<typename HashMapTypes<_key_type, _value_type>::AllocType, 256, NoPthreadDefendMode> >
                class _allocer = SimpleAllocer<typename HashMapTypes<_key_type, _value_type>::AllocType>,
                template <class> class _bucket_array = NormalPointer>
      class ObHashMap
      {
        typedef typename HashMapTypes<_key_type, _value_type>::pair_type pair_type;
        typedef ObHashMap<_key_type, _value_type, _hashfunc, _equal, _allocer, _defendmode> hashmap;
        typedef ObHashTable<_key_type, pair_type, _hashfunc, _equal, pair_first<pair_type>, _allocer, _defendmode, _bucket_array> hashtable;
        typedef hashmap_preproc<_key_type, _value_type> preproc;
      public:
        typedef typename hashtable::iterator iterator;
        typedef typename hashtable::const_iterator const_iterator;
      private:
        ObHashMap(const hashmap &);
        hashmap operator= (const hashmap &);
      public:
        ObHashMap() : ht_()
        {
        };
        ~ObHashMap()
        {
        };
      public:
        iterator begin()
        {
          return ht_.begin();
        };
        const_iterator begin() const
        {
          return ht_.begin();
        };
        iterator end()
        {
          return ht_.end();
        };
        const_iterator end() const
        {
          return ht_.end();
        };
        int64_t size() const
        {
          return ht_.size();
        };
        int create(int64_t bucket_num)
        {
          return ht_.create(cal_next_prime(bucket_num), &allocer_);
        };
        int create(int64_t bucket_num, _allocer *allocer)
        {
          return ht_.create(cal_next_prime(bucket_num), allocer);
        };
        int destroy()
        {
          return ht_.destroy();
        };
        int clear()
        {
          return ht_.clear();
        };
        // 返回  -1表示有错误发生
        // 返回  HASH_EXIST表示结点存在
        // 返回  HASH_NOEXIST表示结点不存在
        inline int get(const _key_type &key, _value_type &value, const int64_t timeout_us = 0) const
        {
          int ret = 0;
          pair_type pair(key, value);
          if (HASH_EXIST == (ret = const_cast<hashtable&>(ht_).get(key, pair, timeout_us)))
          {
            value = pair.second;
          }
          return ret;
        };
        // flag默认为0表示不覆盖已有元素 非0表示覆盖已有元素
        // 返回  -1  表示set调用出错, (无法分配新结点等)
        // 其他均表示插入成功：插入成功分下面三个状态
        // 返回  HASH_OVERWRITE  表示覆盖旧结点成功(在flag非0的时候返回）
        // 返回  HASH_INSERT_SEC 表示插入新结点成功
        // 返回  HASH_EXIST  表示hash表结点存在（在flag为0的时候返回)
        inline int set(const _key_type &key, const _value_type &value, int flag = 0, 
                int broadcast = 0, int overwrite_key = 0)
        {
          pair_type pair(key, value);
          return ht_.set(key, pair, flag, broadcast, overwrite_key);
        };
        template <class _callback>
        // 返回  -1表示有错误发生
        // 返回  HASH_EXIST表示结点存在
        // 返回  HASH_NOEXIST表示结点不存在
        int atomic(const _key_type &key, _callback &callback)
        {
          //return ht_.atomic(key, callback, preproc_);
          return ht_.atomic(key, callback);
        };
        // 返回  -1表示有错误发生
        // 返回  HASH_EXIST表示结点存在并删除成功
        // 返回  HASH_NOEXIST表示结点不存在不用删除
        // 如果value指针不为空并且删除成功则将值存入value指向的空间
        int erase(const _key_type &key, _value_type *value = NULL)
        {
          int ret = 0;
          pair_type pair;
          if (NULL != value)
          {
            ret = ht_.erase(key, &pair);
            *value = pair.second;
          }
          else
          {
            ret = ht_.erase(key);
          }
          return ret;
        };
        template <class _archive>
        int serialization(_archive &archive)
        {
          return ht_.serialization(archive);
        };
        template <class _archive>
        int deserialization(_archive &archive)
        {
          return ht_.deserialization(archive, &allocer_);
        };
      private:
        preproc preproc_;
        _allocer allocer_;
        hashtable ht_;
      };
    }
  }
}

#endif //OCEANBASE_COMMON_HASH_HASHMAP_H_

