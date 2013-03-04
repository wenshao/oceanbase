#ifndef OB_MERGER_BTREE_TABLE_H_
#define OB_MERGER_BTREE_TABLE_H_

#include "ob_ms_btreemap.h"
//#include "ob_ms_stlmap.h"
#include "common/ob_range.h"
#include "common/utility.h"
#include "common/ob_cache.h"
#include "common/ob_define.h"


namespace oceanbase
{
  namespace mergeserver
  {
    // Search Engine for ob_cache replacement of ObCHashTable
    // Waring must be a template class as the cache template param
    // because of the hash implemention has already been template class
    template <class _key, class _value>
    class ObCBtreeTable
    {
    public:
      ObCBtreeTable()
      {
      }

      virtual ~ObCBtreeTable()
      {
      }

    // cache engine interface
    public:
      // create btree map
      int init(int32_t slot_num);
      // add a new item and return the old item for mem destory
      // the search btree's key is the item.key_
      int add(common::CacheItemHead & item, common::CacheItemHead *& old_item);
      // get the value through string key 
      // the key is constructed with (uint64_t table_id + ObString row_key)
      // at first convert the key to obRange type for btree compare with nodes
      common::CacheItemHead * get(const common::ObString & key);

      // remove a item after it's replaced
      void remove(common::CacheItemHead & item);

      // remove the value through string key
      common::CacheItemHead * remove(const common::ObString & key);
      
      // get item num
      int64_t get_item_num(void) const;
    public: 
      // search mode for compare  
      enum SearchMode
      {
        OB_SEARCH_MODE_EQUAL = 0x00,
        OB_SEARCH_MODE_GREATER_EQUAL,
        OB_SEARCH_MODE_GREATER_THAN,
        OB_SEARCH_MODE_LESS_THAN,
        OB_SEARCH_MODE_LESS_EQUAL
      };

      // the KeyBtree key type for ob_cache
      // it only the wrapper of (CacheItemHead *) for operator - definition
      struct MapKey
      {
        common::CacheItemHead * key_;          
        int compare_range_with_key(const uint64_t table_id, const common::ObString & row_key, 
            const common::ObBorderFlag row_key_flag, const int64_t search_mode, const common::ObRange& range) const
        {
          int cmp = 0;
          if (table_id != range.table_id_)
          {
            cmp = table_id < range.table_id_ ? -1 : 1;
          }
          else if (row_key_flag.is_min_value())
          {
            if (OB_SEARCH_MODE_EQUAL != search_mode && range.border_flag_.is_min_value())
            {
              cmp = 0;
            }
            else
            {
              cmp = -1;
            }
          }
          else if (row_key_flag.is_max_value())
          {
            if (OB_SEARCH_MODE_GREATER_THAN != search_mode && range.border_flag_.is_max_value())
            {
              cmp = 0;
            }
            else
            {
              cmp = 1;
            }
          }
          else
          {
            // the tablet range: (range.start_key_, range.end_key_]
            int cmp_endkey = 0;
            if (range.border_flag_.is_max_value())
            {
              cmp_endkey = -1;
            }
            else
            {
              cmp_endkey = row_key.compare(range.end_key_);
            }

            if (cmp_endkey < 0) // row_key < end_key
            {
              if (range.border_flag_.is_min_value())
              {
                cmp = 0;
              }
              else
              {
                int tmp = row_key.compare(range.start_key_);
                if (tmp > 0)
                {
                  cmp = 0;
                }
                else if (tmp < 0)
                {
                  cmp = -1;
                }
                else
                {
                  if (OB_SEARCH_MODE_GREATER_THAN == search_mode)
                  {
                    cmp = 0;
                  }
                  else
                  {
                    cmp = -1;
                  }
                }
              }
            }
            else if (cmp_endkey > 0) // row_key > end_key
            {
              cmp = 1;
            }
            else                      // row_key == end_key
            {
              if (OB_SEARCH_MODE_GREATER_THAN == search_mode)
              {
                cmp = 1;
              }
              else
              {
                cmp = 0;
              }
            }
          }
          return cmp;
        }
        
        bool operator < (const MapKey & key) const
        {
          return (*this - (key)) < 0;
        }

        // overload operator - for key comparison
        int operator - (const MapKey & key) const
        {
          int64_t pos = 0;
          common::ObRange range1;
          common::ObRange range2;
          int ret = range1.deserialize(key_->key_, key_->key_size_, pos);
          if (ret != common::OB_SUCCESS)
          {
            TBSYS_LOG(ERROR, "%s", "deserialize range1 failed");
          }
          else
          {
            pos = 0;
            ret = range2.deserialize(key.key_->key_, key.key_->key_size_, pos);
            if (ret != common::OB_SUCCESS)
            {
              TBSYS_LOG(ERROR, "%s", "deserialize range2 failed");
            }
          }

          if (common::OB_SUCCESS == ret)
          {
            // table id is equal 
            if (range1.start_key_ == range1.end_key_)
            {
              ret = compare_range_with_key(range1.table_id_, range1.start_key_, 
                  range1.border_flag_, OB_SEARCH_MODE_EQUAL, range2);
            }
            else if (range2.start_key_ == range2.end_key_)
            {
              ret = 0 - compare_range_with_key(range2.table_id_, range2.start_key_, 
                  range2.border_flag_, OB_SEARCH_MODE_EQUAL, range1);
            }
            else
            {
              ret = range1.compare_with_endkey(range2);
            }
          }
          return ret;
        }
      };

      private:
        // construct MapKey
        int construct_mapkey(const common::ObString & key, MapKey & Key, common::CacheItemHead *& local);

      private:
        DISALLOW_COPY_AND_ASSIGN(ObCBtreeTable);
        /// lock for cache logic
        mutable tbsys::CThreadMutex cache_lock_;
        /// treemap implemention of common::ob_cache search engine
        ObBtreeMap<MapKey, common::CacheItemHead *> tree_map_;
        //ObStlMap<MapKey, common::CacheItemHead *> tree_map_;
    };

    template <class _key, class _value>
    int ObCBtreeTable<_key, _value>::init(int32_t slot_num)
    {
      tbsys::CThreadGuard lock(&cache_lock_); 
      return tree_map_.create(slot_num);
    }

    template <class _key, class _value>
    int ObCBtreeTable<_key, _value>::add(common::CacheItemHead & item, common::CacheItemHead *& old_item)
    {
      int ret = common::OB_SUCCESS;
      MapKey Key;
      Key.key_ = &item;
      old_item = NULL;
      tbsys::CThreadGuard lock(&cache_lock_); 
      ret = tree_map_.set(Key, &item, old_item);
      if (ret != common::OB_SUCCESS)
      {
        TBSYS_LOG(ERROR, "check btree set failed:ret[%d]", ret);
      }
      else if (old_item != NULL)
      {
        old_item->get_mother_block()->inc_ref();
      }
      return ret;
    }

    template <class _key, class _value>
    common::CacheItemHead * ObCBtreeTable<_key, _value>::get(const common::ObString & key)
    {
      common::CacheItemHead * result = NULL;
      MapKey Key;
      common::CacheItemHead * local = NULL;
      int ret = construct_mapkey(key, Key, local);
      if (ret != common::OB_SUCCESS)
      {
        TBSYS_LOG(ERROR, "construct MapKey failed through key:ret[%d]", ret);
      }
      else
      {
        Key.key_ = local;
        tbsys::CThreadGuard lock(&cache_lock_); 
        ret = tree_map_.get(Key, result);
        if (result != NULL)
        {
          result->get_mother_block()->inc_ref();
        }
      }
      
      if (local != NULL)
      {
        ob_free(local);
        local = NULL;
      }
      return result;
    }

    template <class _key, class _value>
    int64_t ObCBtreeTable<_key, _value>::get_item_num() const
    {
      tbsys::CThreadGuard lock(&cache_lock_); 
      return tree_map_.size();
    }

    template <class _key, class _value>
    void ObCBtreeTable<_key, _value>::remove(common::CacheItemHead & item)
    {
      MapKey Key;
      Key.key_ = &item;
      common::CacheItemHead * result = NULL;
      tbsys::CThreadGuard lock(&cache_lock_); 
      tree_map_.erase(Key, result);
      // do not dec ref of result because of cache recycle procedure will do
    }
    
    template <class _key, class _value>
    int ObCBtreeTable<_key, _value>::construct_mapkey(const common::ObString & key, MapKey & Key,
      common::CacheItemHead *& local)
    {
      int ret = common::OB_SUCCESS;
      uint64_t table_id = 0;
      common::ObString RowKey;
      if ((NULL == key.ptr()) || (0 == key.length()))
      {
        TBSYS_LOG(ERROR, "check key ptr failed:key[%p]", key.ptr());
        ret = common::OB_INPUT_PARAM_ERROR;
      }
      else
      {
        // deserialize the table_id and rowkey
        table_id = *(uint64_t *) key.ptr();
        RowKey.assign(const_cast<common::ObString&>(key).ptr() + sizeof(uint64_t), 
            static_cast<int32_t>(key.length() - sizeof(uint64_t)));
        if ((RowKey.ptr() == NULL) || (0 == RowKey.length()))
        {
          TBSYS_LOG(ERROR, "check rowkey ptr failed:rowkey[%p]", RowKey.ptr());
          ret = common::OB_INNER_STAT_ERROR;
        }
      }
      
      // construct the range and CacheItemHead for btree search
      common::ObRange Range;
      int64_t range_size = 0;
      if (common::OB_SUCCESS == ret)
      {
        Range.table_id_ = table_id;
        Range.start_key_ = RowKey;
        Range.end_key_ = RowKey;
        range_size = Range.get_serialize_size();
        if (0 == range_size)
        {
          TBSYS_LOG(ERROR, "check range size failed:table_id[%lu], size[%lu]", 
              table_id, range_size);
          ret = common::OB_INNER_STAT_ERROR;
        }
        else
        {
          local = (common::CacheItemHead *)common::ob_malloc(sizeof(common::CacheItemHead) + range_size,
            oceanbase::common::ObModIds::OB_MS_LOCATION_CACHE);
          if (NULL == local)
          {
            TBSYS_LOG(ERROR, "ob_malloc failed:table_id[%lu]", table_id);
            ret = common::OB_ALLOCATE_MEMORY_FAILED;
          }
        }
      }

      // serialize range as key 
      if (common::OB_SUCCESS == ret)
      {
        int64_t pos = 0;
        ret = Range.serialize(local->key_, range_size, pos);
        if (common::OB_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "serialize key range failed:ret[%d]", ret);
        }
        else
        {
          local->key_size_ = static_cast<int32_t>(range_size);
          Key.key_ = local;
        }
      }
      return ret;
    }

    template <class _key, class _value>
    common::CacheItemHead * ObCBtreeTable<_key, _value>::remove(const common::ObString & key)
    {
      common::CacheItemHead * result = NULL;
      MapKey Key;
      common::CacheItemHead * local = NULL;
      int ret = construct_mapkey(key, Key, local);
      if (ret != common::OB_SUCCESS)
      {
        TBSYS_LOG(ERROR, "construct MapKey failed through key:ret[%d]", ret);
      }
      else
      {
        tbsys::CThreadGuard lock(&cache_lock_); 
        tree_map_.erase(Key, result);
        if (result != NULL)
        {
          result->get_mother_block()->inc_ref();
        }
      }
      
      // free local data buffer
      if (local != NULL)
      {
        ob_free(local);
        local = NULL;
      }
      return result;
    }
  }
}


#endif //OB_MERGER_BTREE_TABLE_H_

