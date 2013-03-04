////===================================================================
 //
 // ob_query_engine.cpp / hash / common / Oceanbase
 //
 // Copyright (C) 2010 Taobao.com, Inc.
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

#include "common/hash/ob_hashutils.h"
#include "ob_query_engine.h"
#include "ob_memtable.h"

namespace oceanbase
{
  namespace updateserver
  {
    using namespace common;

    QueryEngine::QueryEngine() : inited_(false),
                                 keybtree_(sizeof(TEKey), &btree_key_alloc_, &btree_node_alloc_),
                                 keyhash_(hash_bucket_alloc_, hash_node_alloc_)
    {
    }

    QueryEngine::~QueryEngine()
    {
      if (inited_)
      {
        destroy();
      }
    }

    int QueryEngine::init(MemTank *allocer)
    {
      int ret = OB_SUCCESS;
      if (inited_)
      {
        TBSYS_LOG(WARN, "have already inited");
        ret = OB_ERROR;
      }
      else if (NULL == allocer)
      {
        TBSYS_LOG(WARN, "allocer null pointer");
        ret = OB_ERROR;
      }
      else
      {
        btree_key_alloc_.set_mem_tank(allocer);
        btree_node_alloc_.set_mem_tank(allocer);
        hash_bucket_alloc_.set_mem_tank(allocer);
        hash_node_alloc_.set_mem_tank(allocer);
        if (g_conf.using_hash_index
            && OB_SUCCESS != (ret = keyhash_.create(hash::cal_next_prime(HASH_SIZE))))
        {
          TBSYS_LOG(WARN, "keyhash create fail");
        }
        else
        {
          inited_ = true;
        }
      }
      return ret;
    }

    int QueryEngine::destroy()
    {
      int ret = OB_SUCCESS;
      if (!inited_)
      {
        TBSYS_LOG(WARN, "have not inited");
      }
      else
      {
        keybtree_.destroy();
        keyhash_.destroy();
        inited_ = false;
      }
      return ret;
    }

    int QueryEngine::clear()
    {
      int ret = OB_SUCCESS;
      if (!inited_)
      {
        TBSYS_LOG(WARN, "have not inited");
      }
      else
      {
        keybtree_.destroy();
        keyhash_.clear();
      }
      return ret;
    }

    int QueryEngine::set(const TEKey &key, TEValue *value)
    {
      int ret = OB_SUCCESS;
      int btree_ret = ERROR_CODE_OK;
      int hash_ret = OB_SUCCESS;
      TEHashKey hash_key;
      hash_key.table_id = static_cast<uint32_t>(key.table_id);
      hash_key.rk_length = key.row_key.length();
      hash_key.row_key = key.row_key.ptr();
      if (!inited_)
      {
        TBSYS_LOG(WARN, "have not inited");
        ret = OB_ERROR;
      }
      else if (NULL == value)
      {
        TBSYS_LOG(INFO, "value null pointer");
        ret = OB_ERROR;
      }
      else if (ERROR_CODE_OK != (btree_ret = keybtree_.put(key, value, true)))
      {
        TBSYS_LOG(WARN, "put to keybtree fail btree_ret=%d [%s] [%s]",
                  btree_ret, key.log_str(), value->log_str());
        ret = (ERROR_CODE_ALLOC_FAIL == btree_ret) ? OB_MEM_OVERFLOW : OB_ERROR;
      }
      else if (g_conf.using_hash_index
              && OB_SUCCESS != (hash_ret = keyhash_.insert(hash_key, value)))
      {
        TBSYS_LOG(WARN, "put to keyhash fail hash_ret=%d value=%p [%s] [%s]",
                  hash_ret, value, key.log_str(), value->log_str());
        ret = hash_ret;
      }
      else
      {
        TBSYS_LOG(DEBUG, "insert to hash and btree succ %s %s", key.log_str(), value->log_str());
      }
      return ret;
    }

    TEValue *QueryEngine::get(const TEKey &key)
    {
      TEValue *ret = NULL;
      if (!inited_)
      {
        TBSYS_LOG(WARN, "have not inited");
      }
      else if (g_conf.using_hash_index)
      {
        TEHashKey hash_key;
        hash_key.table_id = static_cast<uint32_t>(key.table_id);
        hash_key.rk_length = key.row_key.length();
        hash_key.row_key = key.row_key.ptr();
        int hash_ret = OB_SUCCESS;
        if (OB_SUCCESS != (hash_ret = keyhash_.get(hash_key, ret))
            || NULL == ret)
        {
          if (OB_ENTRY_NOT_EXIST != hash_ret)
          {
            TBSYS_LOG(WARN, "get from keyhash fail hash_ret=%d %s", hash_ret, key.log_str());
          }
        }
      }
      else
      {
        int btree_ret = ERROR_CODE_OK;
        if (ERROR_CODE_OK != (btree_ret = keybtree_.get(key, ret))
            || NULL == ret)
        {
          if (ERROR_CODE_NOT_FOUND != btree_ret)
          {
            TBSYS_LOG(WARN, "get from keybtree fail btree_ret=%d %s", btree_ret, key.log_str());
          }
        }
      }
      return ret;
    }

    int QueryEngine::scan(const TEKey &start_key,
                          const int min_key,
                          const int start_exclude,
                          const TEKey &end_key,
                          const int max_key,
                          const int end_exclude,
                          const bool reverse,
                          QueryEngineIterator &iter)
    {
      int ret = OB_SUCCESS;
      int btree_ret = ERROR_CODE_OK;
      if (!inited_)
      {
        TBSYS_LOG(WARN, "have not inited");
        ret = OB_ERROR;
      }
      else if (ERROR_CODE_OK != (btree_ret = keybtree_.get_read_handle(iter.get_read_handle_())))
      {
        TBSYS_LOG(WARN, "btree get read handle fail btree_ret=%d", btree_ret);
        ret = OB_ERROR;
      }
      else
      {
        const TEKey *btree_start_key_ptr = NULL;
        const TEKey *btree_end_key_ptr = NULL;
        int start_exclude_ = start_exclude;
        int end_exclude_ = end_exclude;
        if (0 != min_key)
        {
          btree_start_key_ptr = keybtree_.get_min_key();
        }
        else
        {
          btree_start_key_ptr = &start_key;
        }
        if (0 != max_key)
        {
          btree_end_key_ptr = keybtree_.get_max_key();
        }
        else
        {
          btree_end_key_ptr = &end_key;
        }
        if (reverse)
        {
          std::swap(btree_start_key_ptr, btree_end_key_ptr);
          std::swap(start_exclude_, end_exclude_);
        }
        keybtree_.set_key_range(iter.get_read_handle_(),
                                btree_start_key_ptr, start_exclude_,
                                btree_end_key_ptr, end_exclude_);
        iter.set_(&keybtree_);
      }
      return ret;
    }

    void QueryEngine::dump2text(const char *fname)
    {
      const int64_t BUFFER_SIZE = 1024;
      char buffer[BUFFER_SIZE];
      snprintf(buffer, BUFFER_SIZE, "%s.btree.main", fname);
      FILE *fd = fopen(buffer, "w");
      if (NULL != fd)
      {
        BtreeReadHandle handle;
        if (ERROR_CODE_OK == keybtree_.get_read_handle(handle))
        {
          keybtree_.set_key_range(handle, keybtree_.get_min_key(), 0, keybtree_.get_max_key(), 0);
          TEKey key;
          TEValue *value = NULL;
          MemTableGetIter get_iter;
          int64_t num = 0;
          while (ERROR_CODE_OK == keybtree_.get_next(handle, key, value)
                && NULL != value)
          {
            fprintf(fd, "[ROW_INFO][%ld] btree_key=[%s] btree_value=[%s] ptr=%p\n",
                    num, key.log_str(), value->log_str(), value);
            int64_t pos = 0;
            ObCellInfo *ci = NULL;
            get_iter.set_(key, value, NULL, NULL);
            while (OB_SUCCESS == get_iter.next_cell()
                  && OB_SUCCESS == get_iter.get_cell(&ci))
            {
              fprintf(fd, "          [CELL_INFO][%ld][%ld] column_id=%lu value=[%s]\n",
                      num, pos++, ci->column_id_, print_obj(ci->value_));
            }
            ++num;
          }
        }
        fclose(fd);
      }
    }

    int64_t QueryEngine::btree_size()
    {
      return keybtree_.get_object_count();
    }

    int64_t QueryEngine::hash_size() const
    {
      return keyhash_.size();
    }

    int64_t QueryEngine::hash_bucket_using() const
    {
      return keyhash_.bucket_using();
    }

    int64_t QueryEngine::hash_uninit_unit_num() const
    {
      return keyhash_.uninit_unit_num();
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////

    QueryEngineIterator::QueryEngineIterator() : keybtree_(NULL), read_handle_(), key_(), pvalue_(NULL)
    {
    }

    QueryEngineIterator::~QueryEngineIterator()
    {
    }

    int QueryEngineIterator::next()
    {
      int ret = OB_SUCCESS;
      int btree_ret = ERROR_CODE_OK;
      if (NULL == keybtree_)
      {
        ret = OB_ITER_END;
      }
      else
      {
        while (true)
        {
          if (ERROR_CODE_OK != (btree_ret = keybtree_->get_next(read_handle_, key_, pvalue_))
              || NULL == pvalue_)
          {
            if (ERROR_CODE_NOT_FOUND != btree_ret)
            {
              TBSYS_LOG(WARN, "get_next from keybtree fail btree_ret=%d pvalue_=%p", btree_ret, pvalue_);
            }
            ret = OB_ITER_END;
            break;
          }
          if (pvalue_->is_empty())
          {
            // 数据链表为空时 直接跳过这记录
            continue;
          }
          else
          {
            break;
          }
        }
      }
      return ret;
    }

    TEValue *QueryEngineIterator::get_value()
    {
      return pvalue_;
    }

    const TEKey &QueryEngineIterator::get_key() const
    {
      return key_;
    }

    void QueryEngineIterator::reset()
    {
      keybtree_ = NULL;
      read_handle_.clear();
      new(&read_handle_) BtreeReadHandle;
      pvalue_ = NULL;
    }

    void QueryEngineIterator::set_(QueryEngine::keybtree_t *keybtree)
    {
      keybtree_ = keybtree;
      pvalue_ = NULL;
    }

    common::BtreeReadHandle &QueryEngineIterator::get_read_handle_()
    {
      return read_handle_;
    }

  }
}

