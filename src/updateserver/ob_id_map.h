////===================================================================
 //
 // ob_id_map.h updateserver / Oceanbase
 //
 // Copyright (C) 2010, 2012 Taobao.com, Inc.
 //
 // Created on 2012-03-23 by Yubai (yubai.lk@taobao.com)
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

#ifndef  OCEANBASE_UPDATESERVER_ID_MAP_H_
#define  OCEANBASE_UPDATESERVER_ID_MAP_H_
#include "common/ob_define.h"
#include "common/ob_malloc.h"
#include "common/ob_fixed_queue.h"

namespace oceanbase
{
  namespace updateserver
  {
#define ATOMIC_CAS(val, cmpv, newv) __sync_val_compare_and_swap((val), (cmpv), (newv))
    template <typename T>
    class ObIDMap
    {
      enum Stat
      {
        ST_FREE = 0,
        ST_USING = 1,
      };
      struct Node
      {
        volatile uint64_t id;
        volatile Stat stat;
        T *data;
      };
      public:
        ObIDMap();
        ~ObIDMap();
      public:
        int init(const uint64_t num);
        void destroy();
      public:
        int assign(T *value, uint64_t &id);
        int get(const uint64_t id, T *&value) const;
        int erase(const uint64_t id);
        int size() const;
      public:
        // callback::operator()(const uint64_t id)
        template <typename Callback>
        void traverse(Callback &cb) const
        {
          if (NULL != array_)
          {
            for (uint64_t i = 0; i < num_; i++)
            {
              if (ST_USING == array_[i].stat)
              {
                cb(array_[i].id);
              }
            }
          }
        };
      private:
        uint64_t num_;
        Node *array_;
        common::ObFixedQueue<Node> free_list_;
    };

    template <typename T>
    ObIDMap<T>::ObIDMap() : num_(0),
                            array_(NULL),
                            free_list_()
    {
    }

    template <typename T>
    ObIDMap<T>::~ObIDMap()
    {
      destroy();
    }

    template <typename T>
    int ObIDMap<T>::init(const uint64_t num)
    {
      int ret = common::OB_SUCCESS;
      if (NULL != array_)
      {
        TBSYS_LOG(WARN, "have inited");
        ret = common::OB_INIT_TWICE;
      }
      else if (0 >= num)
      {
        TBSYS_LOG(WARN, "invalid param num=%ld", num);
        ret = common::OB_INVALID_ARGUMENT;
      }
      else if (NULL == (array_ = (Node*)common::ob_malloc(num * sizeof(Node))))
      {
        TBSYS_LOG(WARN, "malloc array fail num=%ld", num);
        ret = common::OB_MEM_OVERFLOW;
      }
      else if (common::OB_SUCCESS != (ret = free_list_.init(num)))
      {
        TBSYS_LOG(WARN, "free list init fail ret=%d", ret);
      }
      else
      {
        memset(array_, 0, num * sizeof(Node));
        for (uint64_t i = 0; i < num; i++)
        {
          array_[i].id = i;
          if (common::OB_SUCCESS != (ret = free_list_.push(&(array_[i]))))
          {
            TBSYS_LOG(WARN, "push to free list fail ret=%d i=%ld", ret, i);
            break;
          }
        }
        num_ = num;
      }
      if (common::OB_SUCCESS != ret)
      {
        destroy();
      }
      return ret;
    }

    template <typename T>
    void ObIDMap<T>::destroy()
    {
      free_list_.destroy();
      if (NULL != array_)
      {
        common::ob_free(array_);
        array_ = NULL;
      }
      num_ = 0;
    }

    template <typename T>
    int ObIDMap<T>::assign(T *value, uint64_t &id)
    {
      int ret = common::OB_SUCCESS;
      Node *node = NULL;
      if (NULL == array_)
      {
        TBSYS_LOG(WARN, "have not inited");
        ret = common::OB_NOT_INIT;
      }
      else if (common::OB_SUCCESS != (ret = free_list_.pop(node))
              || NULL == node)
      {
        TBSYS_LOG(WARN, "no more id free list size=%ld", free_list_.get_total());
        ret = (common::OB_SUCCESS == ret) ? common::OB_MEM_OVERFLOW : ret;
      }
      else
      {
        id = node->id;
        node->data = value;
        node->stat = ST_USING;
      }
      return ret;
    }

    template <typename T>
    int ObIDMap<T>::get(const uint64_t id, T *&value) const
    {
      int ret = common::OB_SUCCESS;
      uint64_t pos = id % num_;
      if (NULL == array_)
      {
        TBSYS_LOG(WARN, "have not inited");
        ret = common::OB_NOT_INIT;
      }
      else
      {
        T *retv = NULL;
        if (id != array_[pos].id)
        {
          ret = common::OB_ENTRY_NOT_EXIST;
        }
        else
        {
          retv = array_[pos].data;
          // double check
          if (id != array_[pos].id)
          {
            ret = common::OB_ENTRY_NOT_EXIST;
            retv = NULL;
          }
          else
          {
            value = retv;
          }
        }
      }
      return ret;
    }

    template <typename T>
    int ObIDMap<T>::erase(const uint64_t id)
    {
      int ret = common::OB_SUCCESS;
      uint64_t pos = id % num_;
      if (NULL == array_)
      {
        TBSYS_LOG(WARN, "have not inited");
        ret = common::OB_NOT_INIT;
      }
      else
      {
        uint64_t oldv = array_[pos].id;
        uint64_t newv = array_[pos].id + num_;
        if (id != oldv)
        {
          TBSYS_LOG(WARN, "id do not match param_id=%lu node_id=%ld pos=%lu", id, array_[pos].id, pos);
          ret = common::OB_ENTRY_NOT_EXIST;
        }
        else if (oldv != ATOMIC_CAS(&(array_[pos].id), oldv, newv))
        {
          TBSYS_LOG(WARN, "id=%lu has already erased", id);
          ret = common::OB_ENTRY_NOT_EXIST;
        }
        else
        {
          array_[pos].data = NULL;
          array_[pos].stat = ST_FREE;
          int tmp_ret = common::OB_SUCCESS;
          if (common::OB_SUCCESS != (tmp_ret = free_list_.push(&(array_[pos]))))
          {
            TBSYS_LOG(ERROR, "push to free list fail ret=%d free list size=%ld", tmp_ret, free_list_.get_total());
          }
        }
      }
      return ret;
    }

    template <typename T>
    int ObIDMap<T>::size() const
    {
      return static_cast<int>(num_ - free_list_.get_total());
    }
  } // namespace updateserver
} // namespace oceanbase

#endif //OCEANBASE_UPDATESERVER_ID_MAP_H_
