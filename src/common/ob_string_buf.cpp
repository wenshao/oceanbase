/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: ob_string_buf.cpp,v 0.1 2010/08/19 16:19:47 chuanhui Exp $
 *
 * Authors:
 *   chuanhui <rizhao.ych@taobao.com>
 *     - some work details if you want
 *
 */

#include "ob_string_buf.h"
#include "common/ob_object.h"
#include "common/ob_malloc.h"

namespace oceanbase
{
  namespace common
  {
    ObStringBuf :: ObStringBuf(const int32_t mod_id, const int64_t block_size) : mem_block_size_(block_size)
    {
      block_head_ = NULL;
      block_tail_ = NULL;
      total_virt_ = 0;
      total_res_ = 0;
      mod_id_ = mod_id;
      if (mem_block_size_ < MIN_DEF_MEM_BLOCK_SIZE)
      {
        mem_block_size_ = MIN_DEF_MEM_BLOCK_SIZE;
      }
    }

    ObStringBuf :: ~ObStringBuf()
    {
      clear();
      block_head_ = NULL;
      block_tail_ = NULL;
    }

    int ObStringBuf :: clear()
    {
      int err = OB_SUCCESS;

      MemBlock* tmp = NULL;
      while (NULL != block_head_)
      {
        tmp = block_head_->next;
        err = free_mem_(block_head_);
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN, "failed to free mem, ptr=%p", block_head_);
        }
        block_head_ = tmp;
      }
      block_head_ = NULL;
      block_tail_ = NULL;
      total_virt_ = 0;
      total_res_ = 0;

      return err;
    }

    int ObStringBuf :: reset()
    {
      int err       = OB_SUCCESS;
      MemBlock* tmp = NULL;

      if (NULL != block_head_)
      {
        while (NULL != block_head_->next)
        {
          tmp = block_head_->next;
          block_head_->next = tmp->next;
          err = free_mem_(tmp);
          if (OB_SUCCESS != err)
          {
            TBSYS_LOG(WARN, "failed to free mem, ptr=%p", tmp);
          }
        }
        if (NULL != block_head_)
        {
          block_head_->next = NULL;
          block_head_->cur_pos = 0;
          // block_size does not need to reset, never updated after init
          //block_head_->block_size = mem_block_size_ - sizeof(MemBlock);
          block_tail_ = block_head_;
          total_virt_ = block_head_->block_size + static_cast<int64_t>(sizeof(MemBlock));
        }
        else
        {
          total_virt_ = 0;
        }
      }
      else
      {
        block_head_ = NULL;
        block_tail_ = NULL;
        total_virt_ = 0;
      }
      total_res_ = 0;

      return err;
    }

    int ObStringBuf :: write_string(const ObString& str, ObString* stored_str)
    {
      int err = OB_SUCCESS;

      if (0 == str.length() || NULL == str.ptr())
      {
        if (NULL != stored_str)
        {
          stored_str->assign(NULL, 0);
        }
      }
      else
      {
        int64_t str_length = str.length();
        if (NULL == block_tail_ ||
            (NULL != block_tail_ && 
             block_tail_->block_size - block_tail_->cur_pos <= str_length))
        {
          err = alloc_a_block_(str_length);
          if (OB_SUCCESS != err)
          {
            TBSYS_LOG(WARN, "failed to alloc_a_block_, err=%d", err);
          }
        }

        if (OB_SUCCESS == err)
        {
          if (NULL == block_tail_ ||
              (NULL != block_tail_ && 
               block_tail_->block_size - block_tail_->cur_pos <= str_length))
          {
            // buffer still not enough
            err = OB_ERROR;
          }
          else
          {
            memcpy(block_tail_->data + block_tail_->cur_pos, str.ptr(), str.length());
            if (NULL != stored_str)
            {
              stored_str->assign(block_tail_->data + block_tail_->cur_pos, str.length());
            }
            block_tail_->cur_pos = static_cast<int32_t>(block_tail_->cur_pos + str_length);
            total_res_ += str_length;
          }
        }
      }

      return err;
    }

    int ObStringBuf :: write_obj(const ObObj& obj, ObObj* stored_obj)
    {
      int err = OB_SUCCESS;

      if (NULL != stored_obj)
      {
        *stored_obj = obj;
      }

      ObObjType type = obj.get_type();
      if (ObVarcharType == type)
      {
        ObString value;
        ObString new_value;
        obj.get_varchar(value);
        err = write_string(value, &new_value);
        if (OB_SUCCESS == err)
        {
          if (NULL != stored_obj)
          {
            stored_obj->set_varchar(new_value);
          }
        }
      }

      return err;
    }

    int ObStringBuf :: alloc_a_block_(const int64_t ref_size)
    {
      int err = OB_SUCCESS;
      void* tmp_ptr = NULL;
      MemBlock* cur_block = NULL;
      int64_t cur_mem_block_size = mem_block_size_;
      if (ref_size > cur_mem_block_size - static_cast<int64_t>(sizeof(MemBlock)))
      {
        cur_mem_block_size = DEF_MEM_BLOCK_SIZE;
      }
      err = alloc_mem_(cur_mem_block_size, tmp_ptr);
      if (OB_SUCCESS != err || NULL == tmp_ptr)
      {
        TBSYS_LOG(WARN, "failed to alloc mem, mem_size=%ld, err=%d",
            cur_mem_block_size, err);
        err = OB_ERROR;
      }
      else
      {
        cur_block = static_cast<MemBlock*> (tmp_ptr);
        cur_block->next = NULL;
        cur_block->cur_pos = 0;
        cur_block->block_size = static_cast<int32_t>(cur_mem_block_size) - static_cast<int32_t>(sizeof(MemBlock));
        if (NULL == block_head_)
        {
          block_head_ = cur_block;
          block_tail_ = cur_block;
        }
        else
        {
          if (NULL == block_tail_)
          {
            TBSYS_LOG(WARN, "block_tail_ is NULL");
            err = OB_ERROR;
          }
          else
          {
            block_tail_->next = cur_block;
            block_tail_ = cur_block;
          }
        }
      }

      return err;
    }

    int ObStringBuf :: alloc_mem_(const int64_t size, void*& ptr)
    {
      int err = OB_SUCCESS;

      if (size <= 0)
      {
        TBSYS_LOG(WARN, "invalid param, size=%ld", size);
        err = OB_INVALID_ARGUMENT;
      }
      else
      {
        ptr = ob_malloc(size, static_cast<int32_t>(mod_id_));
        if (NULL == ptr)
        {
          TBSYS_LOG(WARN, "failed to alloc mem, size=%ld", size);
          err = OB_ERROR;
        }
        else
        {
          total_virt_ += size;
        }
      }

      return err;
    }

    int ObStringBuf :: free_mem_(void* ptr)
    {
      int err = OB_SUCCESS;

      if (NULL == ptr)
      {
        TBSYS_LOG(WARN, "invalid param, ptr=%p", ptr);
        err = OB_INVALID_ARGUMENT;
      }
      else
      {
        ob_free(ptr);
      }

      return err;
    }
  }
}

