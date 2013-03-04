////===================================================================
 //
 // ob_memtank.h updateserver / Oceanbase
 //
 // Copyright (C) 2010 Taobao.com, Inc.
 //
 // Created on 2010-11-23 by Yubai (yubai.lk@taobao.com) 
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

#ifndef  OCEANBASE_UPDATESERVER_MEMTANK_H_
#define  OCEANBASE_UPDATESERVER_MEMTANK_H_
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <pthread.h>
#include <new>
#include <bitset>
#include <algorithm>
#include "common/ob_atomic.h"
#include "common/ob_define.h"
#include "common/ob_string_buf.h"
#include "common/page_arena.h"
#include "common/utility.h"
#include "ob_ups_utils.h"

namespace oceanbase
{
  namespace updateserver
  {
    class IExternMemTotal
    {
      public:
        IExternMemTotal() {};
        virtual ~IExternMemTotal() {};
      public:
        virtual int64_t get_extern_mem_total() = 0;
    };

    class DefaultExternMemTotal : public IExternMemTotal
    {
      public:
        virtual int64_t get_extern_mem_total()
        {
          return 0;
        }
    };

    class MemTank
    {
      static const int64_t PAGE_SIZE = 2 * 1024L * 1024L;
      static const int64_t AVAILABLE_WARN_SIZE = 2L * 1024L * 1024L * 1024L; //2G
      public:
        MemTank(const int32_t mod_id = common::ObModIds::OB_UPS_MEMTABLE)
          : total_limit_(INT64_MAX),
            string_buf_(mod_id),
            mod_(mod_id),
            allocer_(PAGE_SIZE, mod_),
            tevalue_allocer_(PAGE_SIZE, mod_),
            btree_engine_allocer_(PAGE_SIZE, mod_),
            hash_engine_allocer_(PAGE_SIZE, mod_),
            extern_mem_total_ptr_(&default_extern_mem_total_)
        {
        };
        ~MemTank()
        {
          clear();
        };
      public:
        int write_string(const common::ObString& str, common::ObString* stored_str)
        {
          int ret = common::OB_MEM_OVERFLOW;
          if (!mem_over_limit())
          {
            ret = string_buf_.write_string(str, stored_str);
          }
          else
          {
            log_error_(__FUNCTION__);
          }
          return ret;
        };
        int write_obj(const common::ObObj& obj, common::ObObj* stored_obj)
        {
          int ret = common::OB_MEM_OVERFLOW;
          if (!mem_over_limit())
          {
            ret = string_buf_.write_obj(obj, stored_obj);
          }
          else
          {
            log_error_(__FUNCTION__);
          }
          return ret;
        };
        void *node_alloc(const int32_t sz)
        {
          void *ret = NULL;
          if (!mem_over_limit())
          {
            void *tmp = allocer_.alloc(common::upper_align(sz, sizeof(void*)));
            if (NULL != tmp)
            {
              ret = (void*)common::upper_align((int64_t)tmp, sizeof(void*));
            }
          }
          else
          {
            log_error_(__FUNCTION__);
          }
          return ret;
        };
        void *tevalue_alloc(const int32_t sz)
        {
          void *ret = NULL;
          if (!mem_over_limit())
          {
            void *tmp = tevalue_allocer_.alloc(common::upper_align(sz, sizeof(void*)));
            if (NULL != tmp)
            {
              ret = (void*)common::upper_align((int64_t)tmp, sizeof(void*));
            }
          }
          else
          {
            log_error_(__FUNCTION__);
          }
          return ret;
        };
        void *btree_engine_alloc(const int32_t sz)
        {
          void *ret = NULL;
          if (!mem_over_limit())
          {
            ret = btree_engine_allocer_.alloc(sz);
          }
          else
          {
            log_error_(__FUNCTION__);
          }
          return ret;
        };
        void *hash_engine_alloc(const int32_t sz)
        {
          void *ret = NULL;
          if (!mem_over_limit())
          {
            ret = hash_engine_allocer_.alloc(sz);
          }
          else
          {
            log_error_(__FUNCTION__);
          }
          return ret;
        };
        void clear()
        {
          string_buf_.clear();
          allocer_.free();
          tevalue_allocer_.free();
          btree_engine_allocer_.free();
          hash_engine_allocer_.free();
        };
      public:
        bool mem_over_limit() const
        {
          int64_t table_total = total() + extern_mem_total_ptr_->get_extern_mem_total();
          int64_t table_available = total_limit_ - table_total;
          int64_t table_available_warn_size = get_table_available_warn_size();
          if (table_available_warn_size >= table_available)
          {
            ups_available_memory_warn_callback(table_available);
          }
          return (table_total >= total_limit_);
        };
        int64_t used() const
        {
          return (string_buf_.used() + allocer_.used() + tevalue_allocer_.used() + btree_engine_allocer_.used() + hash_engine_allocer_.used());
        };
        int64_t total() const
        {
          return (string_buf_.total() + allocer_.total() + tevalue_allocer_.total() + btree_engine_allocer_.total() + hash_engine_allocer_.total());
        };
        void set_extern_mem_total(IExternMemTotal *extern_mem_total_ptr)
        {
          if (NULL != extern_mem_total_ptr)
          {
            extern_mem_total_ptr_ = extern_mem_total_ptr;
          }
        };
        IExternMemTotal *get_extern_mem_total()
        {
          return extern_mem_total_ptr_;
        };
        int64_t set_total_limit(const int64_t limit)
        {
          if (0 < limit)
          {
            total_limit_ = limit;
          }
          return total_limit_;
        };
        int64_t get_total_limit() const
        {
          return total_limit_;
        };
        void log_info() const
        {
          TBSYS_LOG(INFO, "MemTank report used=%ld total=%ld extern=%ld limit=%ld "
                    "string_buf_used=%ld string_buf_total=%ld "
                    "allocer_used=%ld allocer_total=%ld "
                    "indep_allocer_used=%ld indep_allocer_total=%ld "
                    "btree_engine_allocer_used=%ld btree_engine_allocer_total=%ld "
                    "hash_engine_allocer_used=%ld hash_engine_allocer_total=%ld",
                    used(), total(), extern_mem_total_ptr_->get_extern_mem_total(), get_total_limit(),
                    string_buf_.used(), string_buf_.total(),
                    allocer_.used(), allocer_.total(),
                    tevalue_allocer_.used(), tevalue_allocer_.total(),
                    btree_engine_allocer_.used(), btree_engine_allocer_.total(),
                    hash_engine_allocer_.used(), hash_engine_allocer_.total());
        };
      private:
        void log_error_(const char *caller) const
        {
          TBSYS_LOG(ERROR, "memory over limited, caller=[%s] used=%ld total=%ld extern=%ld limit=%ld "
                    "string_buf_used=%ld string_buf_total=%ld "
                    "allocer_used=%ld allocer_total=%ld "
                    "indep_allocer_used=%ld indep_allocer_total=%ld "
                    "btree_engine_allocer_used=%ld btree_engine_allocer_total=%ld "
                    "hash_engine_allocer_used=%ld hash_engine_allocer_total=%ld",
                    caller, used(), total(), extern_mem_total_ptr_->get_extern_mem_total(), get_total_limit(),
                    string_buf_.used(), string_buf_.total(),
                    allocer_.used(), allocer_.total(),
                    tevalue_allocer_.used(), tevalue_allocer_.total(),
                    btree_engine_allocer_.used(), btree_engine_allocer_.total(),
                    hash_engine_allocer_.used(), hash_engine_allocer_.total());
        };
      private:
        int64_t total_limit_;
        common::ObStringBuf string_buf_;
        common::ModulePageAllocator mod_;
        common::ModuleArena allocer_;
        common::ModuleArena tevalue_allocer_;
        common::ModuleArena btree_engine_allocer_;
        common::ModuleArena hash_engine_allocer_;
        IExternMemTotal *extern_mem_total_ptr_;
        DefaultExternMemTotal default_extern_mem_total_;
    };
  }
}

#endif //OCEANBASE_UPDATESERVER_MEMTANK_H_

