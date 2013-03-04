/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: ob_ms_stlmap.h,v 0.1 2010/09/26 14:01:30 zhidong Exp $
 *
 * Authors:
 *   chuanhui <xielun.szd@taobao.com>
 *     - some work details if you want
 *
 */

#ifndef OB_MERGER_STL_MAP_H_
#define OB_MERGER_STL_MAP_H_

#include <map>
#include "tblog.h"
#include "common/ob_define.h"

namespace oceanbase
{
  namespace mergeserver
  {
    // this class is btree and hash data engine's abstract class
    // adaptor the lrucache engine interface for obvarcache
    // all the interfaces must be same with hashengine
    // and the real implementions are in btree class
    template <class _key, class _value>
    class ObStlMap
    {
    public:
      ObStlMap()
      {
        tree_map_ = NULL;
      }

      virtual ~ObStlMap()
      {
        if (tree_map_)
        {
          delete tree_map_;
          tree_map_ = NULL;
        }
      }

    public:
      // create the btreemap with init node count
      // not thread-safe if write_lock is not true
      int create(int64_t num, const bool write_lock = false);
      // get key value if exist
      int get(const _key & key, _value & value) const;
      // insert or update the key value
      int set(const _key & key, const _value & new_value, _value & old_value);
      // del the key
      int erase(const _key & key, _value & value);
      // get data node count
      int64_t size() const;
    
    private:
      std::map<_key, _value> * tree_map_;
    };
    
    template <class _key, class _value>
    int ObStlMap<_key, _value>::create(int64_t num, const bool write_lock)
    {
      int ret = common::OB_SUCCESS;
      if (num <= 0)
      {
        TBSYS_LOG(ERROR, "check create num failed:num[%ld]", num);
        ret = common::OB_INPUT_PARAM_ERROR;
      }
      else if (tree_map_)
      {
        TBSYS_LOG(ERROR, "check tree map not null:map[%p]", tree_map_);
        ret = common::OB_INNER_STAT_ERROR; 
      }
      else
      {
        UNUSED(write_lock);
        tree_map_ = new(std::nothrow) std::map<_key, _value>;
        if (NULL == tree_map_)
        {
          TBSYS_LOG(ERROR, "new KeyBtree failed:map[%p]", tree_map_);
          ret = common::OB_ALLOCATE_MEMORY_FAILED;
        }
      }
      return ret;
    }

    template <class _key, class _value>
    int64_t ObStlMap<_key, _value>::size() const
    {
      int64_t ret = 0;
      if (tree_map_)
      {
        ret = tree_map_->size();
      }
      else
      {
        TBSYS_LOG(ERROR, "check tree map is null:map[%p]", tree_map_);
      }
      return ret;
    }

    template <class _key, class _value>
    int ObStlMap<_key, _value>::erase(const _key & key, _value & value)
    {
      int ret = common::OB_SUCCESS;
      if (tree_map_)
      {
        typename std::map<_key, _value>::const_iterator it;
        it = tree_map_->find(key);
        if (it != tree_map_->end())
        {
          value = it->second;
          tree_map_->erase(key);
        }
        else
        {
          ret = common::OB_ENTRY_NOT_EXIST;
          TBSYS_LOG(ERROR, "not find this key");
        }
      }
      else
      {
        TBSYS_LOG(ERROR, "check tree map is null:map[%p]", tree_map_);
        ret = common::OB_INNER_STAT_ERROR;
      }
      return ret;
    }

    template <class _key, class _value>
    int ObStlMap<_key, _value>::get(const _key & key, _value & value) const
    {
      int ret = common::OB_SUCCESS;
      if (tree_map_)
      {
        typename std::map<_key, _value>::const_iterator it;
        it = tree_map_->find(key);
        if (it != tree_map_->end())
        {
          value = it->second;
        }
        else
        {
          ret = common::OB_ENTRY_NOT_EXIST;
          TBSYS_LOG(ERROR, "not find this key");
        }
      }
      else
      {
        TBSYS_LOG(ERROR, "check tree map is null:map[%p]", tree_map_);
        ret = common::OB_INNER_STAT_ERROR;
      }
      return ret;
    }
    
    template <class _key, class _value>
    int ObStlMap<_key, _value>::set(const _key & key, const _value & new_value,
        _value & old_value)
    {
      int ret = common::OB_SUCCESS;
      if (tree_map_)
      {
        typename std::map<_key, _value>::iterator it;
        it = tree_map_->find(key);
        if (it != tree_map_->end())
        {
          old_value = it->second;
          it->second = new_value;
        }
        else
        {
          std::pair<_key, _value> pair(key, new_value);
          tree_map_->insert(pair); 
        }
      }
      else
      {
        TBSYS_LOG(ERROR, "check tree map is null:map[%p]", tree_map_);
        ret = common::OB_INNER_STAT_ERROR;
      }
      return ret;
    }
  }
}


#endif // OB_MERGER_BTREE_MAP_H_


