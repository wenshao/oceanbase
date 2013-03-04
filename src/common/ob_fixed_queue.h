////===================================================================
 //
 // ob_fixed_queue.h common / Oceanbase
 //
 // Copyright (C) 2010 Taobao.com, Inc.
 //
 // Created on 2012-03-04 by Yubai (yubai.lk@taobao.com) 
 //
 // -------------------------------------------------------------------
 //
 // Description
 // 
 // 无锁的环形队列
 //
 // -------------------------------------------------------------------
 // 
 // Change Log
 //
////====================================================================
#ifndef  OCEANBASE_COMMON_FIXED_QUEUE_H_
#define  OCEANBASE_COMMON_FIXED_QUEUE_H_
#include "common/hash/ob_hashutils.h"
#include "common/ob_atomic.h"

namespace oceanbase
{
  namespace common
  {
#define ATOMIC_CAS(val, cmpv, newv) __sync_val_compare_and_swap((val), (cmpv), (newv))
    template <typename T>
    class ObFixedQueue
    {
      struct ArrayItem
      {
        T *data;
        int64_t cur_pos;
      };
      static const int64_t ARRAY_BLOCK_SIZE = 128L * 1024L;
      public:
        ObFixedQueue();
        ~ObFixedQueue();
      public:
        int init(const int64_t max_num);
        void destroy();
      public:
        int push(T *ptr);
        int pop(T *&ptr);
        inline int64_t get_total() const;
        inline int64_t get_free() const;
      private:
        inline int64_t get_total_(const int64_t consumer, const int64_t producer) const;
        inline int64_t get_free_(const int64_t consumer, const int64_t producer) const;
      private:
        bool inited_;
        int64_t max_num_;
        hash::BigArray<ArrayItem> array_;
        volatile int64_t consumer_;
        volatile int64_t producer_;
    };

    template <typename T>
    ObFixedQueue<T>::ObFixedQueue() : inited_(false),
                                      max_num_(0),
                                      array_(),
                                      consumer_(0),
                                      producer_(0)
    {
    }

    template <typename T>
    ObFixedQueue<T>::~ObFixedQueue()
    {
      destroy();
    }

    template <typename T>
    int ObFixedQueue<T>::init(const int64_t max_num)
    {
      int ret = OB_SUCCESS;
      if (inited_)
      {
        ret = OB_INIT_TWICE;
      }
      else if (0 >= max_num)
      {
        ret = OB_INVALID_ARGUMENT;
      }
      else if (0 != array_.create(max_num, ARRAY_BLOCK_SIZE))
      {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      }
      else
      {
        max_num_ = max_num;
        consumer_ = 0;
        producer_ = 0;
        inited_ = true;
      }
      return ret;
    }

    template <typename T>
    void ObFixedQueue<T>::destroy()
    {
      if (inited_)
      {
        array_.destroy();
        inited_ = false;
      }
    }

    template <typename T>
    inline int64_t ObFixedQueue<T>::get_total() const
    {
      return get_total_(consumer_, producer_);
    }

    template <typename T>
    inline int64_t ObFixedQueue<T>::get_free() const
    {
      return get_free_(consumer_, producer_);
    }

    template <typename T>
    inline int64_t ObFixedQueue<T>::get_total_(const int64_t consumer, const int64_t producer) const
    {
      return (producer - consumer);
    }

    template <typename T>
    inline int64_t ObFixedQueue<T>::get_free_(const int64_t consumer, const int64_t producer) const
    {
      return max_num_ - get_total_(consumer, producer);
    }

    template <typename T>
    int ObFixedQueue<T>::push(T *ptr)
    {
      int ret = OB_SUCCESS;
      if (!inited_)
      {
        ret = OB_NOT_INIT;
      }
      else if (NULL == ptr)
      {
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        volatile int64_t old_pos = 0;
        volatile int64_t new_pos = 0;
        while (true)
        {
          old_pos = producer_;
          new_pos = old_pos;

          if (0 >= get_free_(consumer_, old_pos))
          {
            ret = OB_SIZE_OVERFLOW;
            break;
          }

          new_pos++;
          if (old_pos == ATOMIC_CAS(&producer_, old_pos, new_pos))
          {
            break;
          }
        }
        if (OB_SUCCESS == ret)
        {
          array_[old_pos % max_num_].data = ptr;
          array_[old_pos % max_num_].cur_pos = old_pos;
        }
      }
      return ret;
    }

    template <typename T>
    int ObFixedQueue<T>::pop(T *&ptr)
    {
      int ret = OB_SUCCESS;
      if (!inited_)
      {
        ret = OB_NOT_INIT;
      }
      else
      {
        T *tmp_ptr = NULL;
        volatile int64_t old_pos = 0;
        volatile int64_t new_pos = 0;
        while (true)
        {
          old_pos = consumer_;
          new_pos = old_pos;

          if (0 >= get_total_(old_pos, producer_))
          {
            ret = OB_ENTRY_NOT_EXIST;
            break;
          }
          
          if (old_pos != array_[old_pos % max_num_].cur_pos)
          {
            continue;
          }
          tmp_ptr = array_[old_pos % max_num_].data;

          new_pos++;
          if (old_pos == ATOMIC_CAS(&consumer_, old_pos, new_pos))
          {
            break;
          }
        }
        if (OB_SUCCESS == ret)
        {
          ptr = tmp_ptr;
        }
      }
      return ret;
    }
  }
}

#endif //OCEANBASE_COMMON_FIXED_QUEUE_H_

