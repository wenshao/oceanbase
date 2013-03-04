/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_join_cache.h for join cache. 
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */
#ifndef  OCEANBASE_MERGESERVER_JOIN_CACHE_H_
#define  OCEANBASE_MERGESERVER_JOIN_CACHE_H_

#include "common/ob_define.h"
#include "common/ob_string.h"
#include "common/ob_kv_storecache.h"
#include "ob_row_cell_vec.h"

namespace oceanbase
{
  namespace mergeserver
  {
    struct ObJoinCacheValue
    {
      const ObRowCellVec* row_cell_vec_;
      int64_t size_;
      char* buf_;

      ObJoinCacheValue() : row_cell_vec_(NULL), size_(0), buf_(NULL)
      {

      }
    };

    struct ObJoinCacheKey
    {
      uint64_t table_id_;
      /// @note there won't exist data of 2^32 versions in the cache
      int32_t  end_version_;
      int32_t  row_key_size_;
      char     *row_key_;

      ObJoinCacheKey()
      {
        table_id_  = common::OB_INVALID_ID;
        end_version_ = 0;
        row_key_size_ = 0;
        row_key_ = NULL;
      }

      ObJoinCacheKey(const int64_t end_version_in, const uint64_t table_id_in, 
                     common::ObString & row_key_in)
      {
        end_version_ = static_cast<int32_t>(end_version_in);
        table_id_ = table_id_in;
        row_key_size_ = row_key_in.length();
        row_key_ = row_key_in.ptr();
      }

      int64_t hash()const
      {
        uint32_t hash_val = 0;
        hash_val = oceanbase::common::murmurhash2(&table_id_,sizeof(table_id_),hash_val);
        hash_val = oceanbase::common::murmurhash2(&end_version_,sizeof(end_version_),hash_val);
        if (NULL != row_key_ && 0 < row_key_size_)
        {
          hash_val = oceanbase::common::murmurhash2(row_key_,row_key_size_,hash_val);
        }
        return hash_val;
      }

      bool operator==(const ObJoinCacheKey &other) const
      {
        bool ret = true;
        ret = ((table_id_ == other.table_id_)
               && (end_version_ == other.end_version_)
               && (row_key_size_ == other.row_key_size_));
        if (ret && row_key_size_ > 0)
        {
          if (NULL != row_key_ && NULL != other.row_key_)
          {
            ret = (memcmp(row_key_,other.row_key_,row_key_size_) == 0);
          }
          else
          {
            ret = false;
          }
        }
        return ret;
      }
    };
  }

  namespace mergeserver
  {
    class ObJoinCache
    {
      static const int64_t KVCACHE_ITEM_SIZE = 1024;         //1k
      static const int64_t KVCACHE_BLOCK_SIZE = 1024 * 1024; //1M

    public:
      typedef common::KeyValueCache<ObJoinCacheKey, ObJoinCacheValue, 
      KVCACHE_ITEM_SIZE, KVCACHE_BLOCK_SIZE> KVCache;
      typedef common::CacheHandle Handle;

    public:
      ObJoinCache();
      ~ObJoinCache();

      int init(const int64_t max_mem_size);

      inline bool is_inited() const
      {
        return inited_;
      }

      inline const int64_t get_cache_mem_size() const 
      {
        return kv_cache_.size();
      }

      //clear all items in join cache
      int clear();

      //destroy this cache in order to reuse it
      int destroy();

      /**
       * get one row from join cache
       * 
       * @param key table_id + row_key
       * @param row_cells cells array of row
       * 
       * @return int if success, return OB_SUCCESS, else return 
       *         OB_ERROR, OB_ENTRY_NOT_EXIST, OB_NOT_INIT,
       *         OB_INVALID_ARGUMENT
       */
      int get_row(const ObJoinCacheKey& key, ObRowCellVec& row_cells);

      /**
       * put one row into join cache
       * 
       * @param key table_id + row_key
       * @param row_cells [out] cells array of row to return
       * 
       * @return int if success, return OB_SUCCESS, else return 
       *         OB_ERROR
       */
      int put_row(const ObJoinCacheKey& key, const ObRowCellVec& row_cells);

    private:
      DISALLOW_COPY_AND_ASSIGN(ObJoinCache);

      bool inited_;
      KVCache kv_cache_;
    };
  }
}

#endif //OCEANBASE_MERGESERVER_JOIN_CACHE_H_
