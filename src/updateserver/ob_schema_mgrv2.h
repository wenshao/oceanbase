////===================================================================
 //
 // ob_schema_mgr.h updateserver / Oceanbase
 //
 // Copyright (C) 2010 Taobao.com, Inc.
 //
 // Created on 2010-10-08 by Yubai (yubai.lk@taobao.com) 
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

#ifndef  OCEANBASE_UPDATESERVER_SCHEMA_MGRV2_H_
#define  OCEANBASE_UPDATESERVER_SCHEMA_MGRV2_H_
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <pthread.h>
#include <new>
#include <algorithm>
#include "common/ob_atomic.h"
#include "common/ob_define.h"
#include "common/hash/ob_hashmap.h"
#include "common/ob_schema.h"
#include "common/page_arena.h"
#include "common/ob_spin_rwlock.h"
#include "sstable/ob_sstable_schema.h"
#include "ob_ups_utils.h"

#define DEFAULT_COLUMN_GROUP_ID 0 

namespace oceanbase
{
  namespace updateserver
  {

    class CommonSchemaManagerWrapper
    {
      public:
        CommonSchemaManagerWrapper();
        explicit CommonSchemaManagerWrapper(const CommonSchemaManager &other);
        ~CommonSchemaManagerWrapper();
        CommonSchemaManagerWrapper &operator= (const CommonSchemaManager &other);
        DISALLOW_COPY_AND_ASSIGN(CommonSchemaManagerWrapper);
      public:
        int64_t get_version() const;
        bool parse_from_file(const char* file_name, tbsys::CConfig& config);
        const CommonSchemaManager *get_impl() const;
        int set_impl(const CommonSchemaManager &schema_impl) const;
        NEED_SERIALIZE_AND_DESERIALIZE;
      public:
        void *schema_mgr_buffer_;
        CommonSchemaManager *schema_mgr_impl_;
    };

    class UpsSchemaMgrImp
    {
      public:
        UpsSchemaMgrImp() : ref_cnt_(0), schema_mgr_()
        {
        };
        ~UpsSchemaMgrImp()
        {
        };
      public:
        inline int64_t inc_ref_cnt()
        {
          return common::atomic_inc((uint64_t*)&ref_cnt_);
        };
        inline int64_t dec_ref_cnt()
        {
          return common::atomic_dec((uint64_t*)&ref_cnt_);
        };
        inline const CommonSchemaManager &get_schema_mgr() const
        {
          return schema_mgr_;
        };
        inline CommonSchemaManager &get_schema_mgr()
        {
          return schema_mgr_;
        };
      private:
        int64_t ref_cnt_;
        CommonSchemaManager schema_mgr_;
    };

    class UpsSchemaMgrGuard;
    class UpsSchemaMgr
    {
      public:
        typedef UpsSchemaMgrImp *SchemaHandle;
        static const SchemaHandle INVALID_SCHEMA_HANDLE;
      public:
        UpsSchemaMgr();
        ~UpsSchemaMgr();
        DISALLOW_COPY_AND_ASSIGN(UpsSchemaMgr);
      public:
        int set_schema_mgr(const CommonSchemaManagerWrapper &schema_manager);
        int get_schema_mgr(CommonSchemaManagerWrapper &schema_manager) const;
        int get_schema_handle(SchemaHandle &schema_handle) const;
        void revert_schema_handle(SchemaHandle &schema_handle) const;

        uint64_t get_create_time_column_id(const uint64_t table_id) const;
        uint64_t get_modify_time_column_id(const uint64_t table_id) const;
        uint64_t get_create_time_column_id(const SchemaHandle &schema_handle, const uint64_t table_id) const;
        uint64_t get_modify_time_column_id(const SchemaHandle &schema_handle, const uint64_t table_id) const;
        const CommonTableSchema *get_table_schema(const SchemaHandle &schema_handle, const uint64_t table_id) const;
        const CommonTableSchema *get_table_schema(const SchemaHandle &schema_handle, const common::ObString &table_name) const;
        const CommonColumnSchema *get_column_schema(const SchemaHandle &schema_handle,
                                                    const common::ObString &table_name,
                                                    const common::ObString &column_name) const;
        const CommonSchemaManager *get_schema_mgr(UpsSchemaMgrGuard &guard) const;

        int build_sstable_schema(const SchemaHandle schema_handle, sstable::ObSSTableSchema &sstable_schema) const;
        void dump2text() const;

        bool has_schema() const {return has_schema_;}
      private:
        mutable UpsSchemaMgrImp *cur_schema_mgr_imp_;
        mutable common::SpinRWLock rwlock_;
        bool has_schema_;
    };
    
    class UpsSchemaMgrGuard
    {
      public:
        UpsSchemaMgrGuard();
        ~UpsSchemaMgrGuard();
      public:
        void set_host(const UpsSchemaMgr *host, const UpsSchemaMgr::SchemaHandle &handle);
      private:
        void deref_();
        DISALLOW_COPY_AND_ASSIGN(UpsSchemaMgrGuard);
      private:
        const UpsSchemaMgr *host_;
        UpsSchemaMgr::SchemaHandle handle_;
    };
  }
}

#endif //OCEANBASE_UPDATESERVER_SCHEMA_MGRV2_H_

