/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_array.h
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#ifndef _OB_ARRAY_H
#define _OB_ARRAY_H 1
#include "ob_malloc.h"

namespace oceanbase
{
  namespace common
  {
    template<typename T>
    class ObArray
    {
      public:
        ObArray(int64_t block_size = 64*1024);
        virtual ~ObArray();

        int push_back(const T &obj);
        void pop_back();

        int at(int64_t idx, T &obj) const;
        T& at(int64_t idx);     // dangerous
        const T& at(int64_t idx) const; // dangerous

        int64_t count() const;
        void clear();
        void reserve(int64_t capacity);

        // deep copy
        ObArray(const ObArray &other);
        ObArray& operator=(const ObArray &other);
      private:
        void extend_buf();
        void extend_buf(int64_t new_size);
      private:
        // data members
        T* data_;
        int64_t count_;
        int64_t data_size_;
        int64_t block_size_;
        int32_t error_;
        int32_t reserve_;
    };

    template<typename T>
    ObArray<T>::ObArray(int64_t block_size)
      :data_(NULL), count_(0), data_size_(0), block_size_(block_size),
       error_(0), reserve_(0)
    {
      block_size_ = std::max(static_cast<int64_t>(sizeof(T)), block_size);
    }

    template<typename T>
    ObArray<T>::~ObArray()
    {
      if (NULL != data_)
      {
        for (int i = 0; i < count_; ++i)
        {
          data_[i].~T();
        }
        ob_free(data_);
        data_ = NULL;
      }
      count_ = data_size_ = 0;
      error_ = 0;
      reserve_ = 0;
    }

    template<typename T>
    int ObArray<T>::at(int64_t idx, T &obj) const
    {
      int ret = OB_SUCCESS;
      if (error_)
      {
        ret = OB_ERROR;
        TBSYS_LOG(ERROR, "array in error state");
      }
      else if (0 > idx || idx >= count_)
      {
        ret = OB_ARRAY_OUT_OF_RANGE;
      }
      else
      {
        obj = data_[idx];
      }
      return ret;
    }

    template<typename T>
    T& ObArray<T>::at(int64_t idx)
    {
      OB_ASSERT(0 <= idx && idx < count_ && 0 == error_);
      return data_[idx];
    }

    template<typename T>
    const T& ObArray<T>::at(int64_t idx) const
    {
      OB_ASSERT(0 <= idx && idx < count_ && 0 == error_);
      return data_[idx];
    }

    template<typename T>
    void ObArray<T>::extend_buf(int64_t new_size)
    {
      OB_ASSERT(new_size > data_size_);
      T* new_data = (T*)ob_malloc(new_size);
      if (NULL != new_data)
      {
        int64_t max_obj_count = new_size/(int64_t)sizeof(T);
        new_data = new(new_data) T[max_obj_count];
        data_size_ = new_size;
        if (NULL != data_)
        {
          for (int i = 0; i < count_; ++i)
          {
            new_data[i] = data_[i];
            data_[i].~T();
          }
          ob_free(data_);
        }
        data_ = new_data;
      }
      else
      {
        TBSYS_LOG(ERROR, "no memory");
      }
    }

    template<typename T>
    void ObArray<T>::extend_buf()
    {
      int64_t new_size = data_size_ + block_size_;
      extend_buf(new_size);
    }

    template<typename T>
    void ObArray<T>::reserve(int64_t capacity)
    {
      if (capacity > data_size_/(int64_t)sizeof(T))
      {
        int64_t new_size = capacity * sizeof(T);
        int64_t plus = new_size % block_size_;
        new_size += (0 == plus) ? 0 : (block_size_ - plus);
        extend_buf(new_size);
      }
    }

    template<typename T>
    int ObArray<T>::push_back(const T &obj)
    {
      int ret = OB_SUCCESS;
      if (error_)
      {
        ret = OB_ERROR;
        TBSYS_LOG(ERROR, "array in error state");
      }
      else if (count_ >= data_size_/(int64_t)sizeof(T))
      {
        extend_buf();
      }
      if (OB_SUCCESS == ret && (count_ < data_size_/(int64_t)sizeof(T)))
      {
        data_[count_++] = obj;
      }
      else
      {
        TBSYS_LOG(WARN, "count_=%ld, data_size_=%ld, (int64_t)sizeof(T)=%ld, data_size_/(int64_t)sizeof(T)=%ld, ret=%d", 
            count_, data_size_, static_cast<int64_t>(sizeof(T)), data_size_/static_cast<int64_t>(sizeof(T)), ret);
        ret = OB_ALLOCATE_MEMORY_FAILED;
      }
      return ret;
    }

    template<typename T>
    void ObArray<T>::pop_back()
    {
      if (0 < count_)
      {
        --count_;
      }
    }

    template<typename T>
    int64_t ObArray<T>::count() const
    {
      return count_;
    }

    template<typename T>
    void ObArray<T>::clear()
    {
      count_ = 0;
      error_ = 0;
    }

    template<typename T>
    ObArray<T>::ObArray(const ObArray<T> &other)
    {
      *this = other;
    }

    template<typename T>
    ObArray<T>& ObArray<T>::operator=(const ObArray<T> &other)
    {
      if (this != &other)
      {
        this->clear();
        this->reserve(other.count());
        if (data_size_ < other.data_size_)
        {
          TBSYS_LOG(ERROR, "no memory");
          error_ = 1;
        }
        else
        {
          memcpy(data_, other.data_, sizeof(T)*other.count_);
          count_ = other.count_;
        }
      }
      return *this;
    }

  } // end namespace common
} // end namespace oceanbase

#endif /* _OB_ARRAY_H */
