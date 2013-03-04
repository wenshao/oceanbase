/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * ob_malloc.cpp is for what ...
 *
 * Version: $id: ob_malloc.cpp,v 0.1 8/19/2010 9:57a wushi Exp $
 *
 * Authors:
 *   wushi <wushi.ly@taobao.com>
 *     - some work details if you want
 *
 */
#include "ob_malloc.h"
#include <errno.h>
#include "ob_memory_pool.h"
#include "ob_mod_define.h"
#include "ob_thread_mempool.h"
#include "ob_tsi_factory.h"
namespace 
{
  /// @warning 这里使用了gcc的扩展属性，以及一些不常使用的语言特征，目的是解决
  ///          static变量的析构顺序问题，如果把全局的内存池申明为static对象，那么
  ///          它可能在使用他的其他static对象析构前就被析构了，这样程序在退出的
  ///          时候就会出core

  /// @note 提前在数据区分配内存，这样就不需要处理new调用失败的情况
  /// @property 全局的内存池对象的存放空间
  char g_fixed_memory_pool_buffer[sizeof(oceanbase::common::ObFixedMemPool)];
  char g_mod_set_buffer[sizeof(oceanbase::common::ObModSet)];

  /// @note 为了解决析构顺序的问题，必须使用指针
  /// @property global memory pool
  oceanbase::common::ObFixedMemPool *g_memory_pool = NULL;
  oceanbase::common::ObModSet *g_mod_set = NULL;

  /// @note 在main函数执行前初始化全局的内存池
  /// @fn initialize global memory pool
  void  __attribute__((constructor)) init_global_memory_pool()
  {
    g_memory_pool = new(g_fixed_memory_pool_buffer)oceanbase::common::ObFixedMemPool;
    g_mod_set = new (g_mod_set_buffer) oceanbase::common::ObModSet;
    oceanbase::common::thread_mempool_init();
    oceanbase::common::tsi_factory_init();
  }

  /// @note 在main函数退出后析构全局内存池
  /// @fn deinitialize global memory pool
  void  __attribute__((destructor)) deinit_global_memory_pool()
  {
    oceanbase::common::tsi_factory_destroy();
    oceanbase::common::thread_mempool_destroy();
    g_memory_pool->~ObBaseMemPool();
    g_mod_set->~ObModSet();
    g_memory_pool = NULL;
    g_mod_set = NULL;
  }

  /// @fn get global memory pool
  oceanbase::common::ObFixedMemPool & get_fixed_memory_pool_instance()
  {
    return *g_memory_pool;
  }

}


int oceanbase::common::ob_init_memory_pool(int64_t block_size)
{
  return get_fixed_memory_pool_instance().init(block_size,1, g_mod_set);
}
void * oceanbase::common::ob_malloc(const int64_t nbyte, const int32_t mod_id, int64_t *got_size)
{
  void *result = NULL;
  if (nbyte <= 0)
  {
    TBSYS_LOG(WARN, "cann't allocate memory less than 0 byte [nbyte:%ld]",nbyte);
    errno = EINVAL;
  }
  else
  {
    result = get_fixed_memory_pool_instance().malloc(nbyte,mod_id, got_size);
  }
  return  result;
}

void * oceanbase::common::ob_malloc_emergency(const int64_t nbyte, const int32_t mod_id, int64_t *got_size)
{
  void *result = NULL;
  if (nbyte <= 0)
  {
    TBSYS_LOG(WARN, "cann't allocate memory less than 0 byte [nbyte:%ld]",nbyte);
    errno = EINVAL;
  }
  else
  {
    result = get_fixed_memory_pool_instance().malloc_emergency(nbyte,mod_id, got_size);
  }
  return  result;
}


void oceanbase::common::ob_free(void *ptr, const int32_t mod_id)
{
  UNUSED(mod_id);
  get_fixed_memory_pool_instance().free(ptr);
}

void oceanbase::common::ob_safe_free(void *&ptr, const int32_t mod_id)
{
  UNUSED(mod_id);
  get_fixed_memory_pool_instance().free(ptr);
  ptr = NULL;
}

void oceanbase::common::ob_print_mod_memory_usage(bool print_to_std)
{
  g_memory_pool->print_mod_memory_usage(print_to_std);
}

int64_t oceanbase::common::ob_get_mod_memory_usage(int32_t mod_id)
{
  return g_memory_pool->get_mod_memory_usage(mod_id);
}

int64_t oceanbase::common::ob_get_memory_size_direct_allocated()
{
  return g_memory_pool->get_memory_size_direct_allocated();
}

int64_t oceanbase::common::ob_set_memory_size_limit(const int64_t mem_size_limit)
{
  return get_fixed_memory_pool_instance().set_memory_size_limit(mem_size_limit);
}

int64_t oceanbase::common::ob_get_memory_size_limit()
{
  return get_fixed_memory_pool_instance().get_memory_size_limit();
}

int64_t oceanbase::common::ob_get_memory_size_handled()
{
  return get_fixed_memory_pool_instance().get_memory_size_handled();
}

oceanbase::common::ObMemBuffer::ObMemBuffer()
{
  buf_size_ = 0;
  buf_ptr_ = NULL;
}

oceanbase::common::ObMemBuffer::ObMemBuffer(const int64_t nbyte)
{
  buf_size_ = 0;
  buf_ptr_ = NULL;
  buf_ptr_ = ob_malloc(nbyte);
  if (buf_ptr_ != NULL)
  {
    buf_size_ = nbyte;
  }
}

oceanbase::common::ObMemBuffer::~ObMemBuffer()
{
  ob_free(buf_ptr_,mod_id_);
  buf_size_ = 0;
  buf_ptr_ = NULL;
}

void *oceanbase::common::ObMemBuffer::malloc(const int64_t nbyte, const int32_t mod_id)
{
  void *result = NULL;
  if (nbyte <= buf_size_ && buf_ptr_ != NULL)
  {
    result = buf_ptr_;
  }
  else
  {
    ob_free(buf_ptr_,mod_id_);
    buf_size_ = 0;
    buf_ptr_ = NULL;
    result = ob_malloc(nbyte,mod_id);
    if (result != NULL)
    {
      mod_id_ = mod_id;
      buf_size_ = nbyte;
      buf_ptr_ = result;
    }
  }
  return result;
}

void * oceanbase::common::ObMemBuffer::get_buffer()
{
  return buf_ptr_;
}

int64_t oceanbase::common::ObMemBuffer::get_buffer_size()
{
  return buf_size_;
}

int oceanbase::common::ObMemBuf::ensure_space(const int64_t size, const int32_t mod_id)
{
  int ret         = OB_SUCCESS;
  char* new_buf   = NULL;
  int64_t buf_len = size > buf_size_ ? size : buf_size_;

  if (size <= 0 || (NULL != buf_ptr_ && buf_size_ <= 0))
  {
    TBSYS_LOG(WARN, "invalid param, size=%ld, buf_ptr_=%p, "
                    "buf_size_=%ld",
              size, buf_ptr_, buf_size_);
    ret = OB_ERROR;
  }
  else if (NULL == buf_ptr_ || (NULL != buf_ptr_ && size > buf_size_))
  {
    new_buf = static_cast<char*>(ob_malloc(buf_len, mod_id));
    if (NULL == new_buf)
    {
      TBSYS_LOG(ERROR, "Problem allocate memory for buffer");
      ret = OB_ERROR;
    }
    else
    {
      if (NULL != buf_ptr_)
      {
        ob_free(buf_ptr_, mod_id_);
        buf_ptr_ = NULL;
      }
      buf_size_ = buf_len;
      buf_ptr_ = new_buf;
      mod_id_ = mod_id;
    }
  }

  return ret;
}

