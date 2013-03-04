/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * ob_mod_define.h is for what ...
 *
 * Version: $id: ob_mod_define.h,v 0.1 11/23/2010 5:57p wushi Exp $
 *
 * Authors:
 *   wushi <wushi.ly@taobao.com>
 *     - some work details if you want
 *
 */
#include "ob_memory_pool.h"
#ifndef OCEANBASE_COMMON_OB_MOD_DEFINE_H_
#define OCEANBASE_COMMON_OB_MOD_DEFINE_H_
namespace oceanbase
{
  namespace common
  {
    struct ObModInfo
    {
      ObModInfo()
      {
        mod_id_  = 0;
        mod_name_ = NULL;
      }
      int32_t mod_id_;
      const char * mod_name_;
    };


    static const int32_t G_MAX_MOD_NUM = 2048;
    extern ObModInfo OB_MOD_SET[G_MAX_MOD_NUM];


#define DEFINE_MOD(mod_name) mod_name

#define ADD_MOD(mod_name)\
  do \
  {\
    if (ObModIds::mod_name <= ObModIds::OB_MOD_END\
    && ObModIds::mod_name >= ObModIds::OB_MOD_DEFAULT \
    && ObModIds::mod_name < G_MAX_MOD_NUM) \
    {\
      OB_MOD_SET[ObModIds::mod_name].mod_id_ = ObModIds::mod_name; \
      OB_MOD_SET[ObModIds::mod_name].mod_name_ = # mod_name;\
    }\
  }while (0)

    /// 使用方法：
    /// 1. 在ObModIds中定义自己的模块名称, e.x. OB_MS_CELL_ARRAY
    /// 2. 在init_ob_mod_set中添加之前定义的模块名称，e.x. ADD_MOD(OB_MS_CELL_ARRAY)
    /// 3. 在调用ob_malloc的时候，使用定义的模块名称作为第二个参数，e.x. ob_malloc(512, ObModIds::OB_MS_CELL_ARRAY)
    /// 4. 发现内存泄露，调用ob_print_memory_usage()打印每个模块的内存使用量，以发现内存泄露模块
    /// 5. 也可以通过调用ob_get_mod_memory_usage(ObModIds::OB_MS_CELL_ARRAY)获取单个模块的内存使用量
    class ObModIds
    {
    public:
      enum
      {
        OB_MOD_DEFAULT,
        /// define modules here
        // common modules
        OB_COMMON_NETWORK,
        OB_THREAD_BUFFER,
        OB_VARCACHE,
        OB_KVSTORE_CACHE,
        OB_TSI_FACTORY,
        OB_THREAD_OBJPOOL,
        OB_ROW_COMPACTION,
        OB_SQL_RPC_SCAN,

        OB_FILE_DIRECTOR_UTIL,
        OB_FETCH_RUNABLE,
        OB_GET_PARAM,
        OB_SCAN_PARAM,
        OB_MUTATOR,
        OB_SCANNER,
        OB_LOG_WRITER,
        OB_SINGLE_LOG_READER,
        OB_BUFFER,
        OB_THREAD_STORE,
        OB_REGEX,
        OB_SLAB,
        OB_SLAVE_MGR,
        OB_THREAD_MEM_POOL,
        OB_HASH_BUCKET,
        OB_HASH_NODE,
        OB_PAGE_ARENA,
        OB_NB_ACCESSOR,

       // mergeserver modules
        OB_MS_CELL_ARRAY,
        OB_MS_LOCATION_CACHE,
        OB_MS_BTREE,
        OB_MS_RPC,
        OB_MS_JOIN_CACHE,
        OB_MS_REQUEST_EVENT,
        OB_MS_RPC_EVENT,
        OB_MS_GET_EVENT,

        // updateserver modules
        OB_UPS_ENGINE,
        OB_UPS_MEMTABLE,
        OB_UPS_LOG,
        OB_UPS_SCHEMA,
        OB_UPS_RESOURCE_POOL_NODE,
        OB_UPS_RESOURCE_POOL_ARRAY,

        // chunkserver modules
        OB_CS_SSTABLE_READER,
        OB_CS_TABLET_IMAGE,
        OB_CS_IMPORT_SSTABLE,

        // sstable modules
        OB_SSTABLE_AIO,
        OB_SSTABLE_EGT_SCAN,
        OB_SSTABLE_WRITER,
        OB_SSTABLE_READER,

        OB_COMPACTSSTABLE_WRITER,

        OB_MOD_END
      };
    };

    inline void init_ob_mod_set()
    {
      ADD_MOD(OB_MOD_DEFAULT);
      /// add modules here, modules must be first defined
      ADD_MOD(OB_COMMON_NETWORK);
      ADD_MOD(OB_THREAD_BUFFER);
      ADD_MOD(OB_VARCACHE);
      ADD_MOD(OB_KVSTORE_CACHE);
      ADD_MOD(OB_TSI_FACTORY);
      ADD_MOD(OB_THREAD_OBJPOOL);
      ADD_MOD(OB_ROW_COMPACTION);
      ADD_MOD(OB_SQL_RPC_SCAN);
      ADD_MOD(OB_FILE_DIRECTOR_UTIL);
      ADD_MOD(OB_FETCH_RUNABLE);
      ADD_MOD(OB_GET_PARAM);
      ADD_MOD(OB_SCAN_PARAM);
      ADD_MOD(OB_MUTATOR);
      ADD_MOD(OB_SCANNER);
      ADD_MOD(OB_BUFFER);
      ADD_MOD(OB_THREAD_STORE);
      ADD_MOD(OB_LOG_WRITER);
      ADD_MOD(OB_SINGLE_LOG_READER);
      ADD_MOD(OB_REGEX);
      ADD_MOD(OB_SLAB);
      ADD_MOD(OB_SLAVE_MGR);
      ADD_MOD(OB_THREAD_MEM_POOL);
      ADD_MOD(OB_HASH_BUCKET);
      ADD_MOD(OB_HASH_NODE);
      ADD_MOD(OB_PAGE_ARENA);
      ADD_MOD(OB_NB_ACCESSOR);
      ADD_MOD(OB_BUFFER);
      ADD_MOD(OB_THREAD_STORE);

      ADD_MOD(OB_MS_CELL_ARRAY);
      ADD_MOD(OB_MS_LOCATION_CACHE);
      ADD_MOD(OB_MS_BTREE);
      ADD_MOD(OB_MS_RPC);
      ADD_MOD(OB_MS_JOIN_CACHE);
      ADD_MOD(OB_MS_REQUEST_EVENT);
      ADD_MOD(OB_MS_RPC_EVENT);
      ADD_MOD(OB_MS_GET_EVENT);

      ADD_MOD(OB_UPS_ENGINE);
      ADD_MOD(OB_UPS_MEMTABLE);
      ADD_MOD(OB_UPS_LOG);
      ADD_MOD(OB_UPS_SCHEMA);
      ADD_MOD(OB_UPS_RESOURCE_POOL_NODE);
      ADD_MOD(OB_UPS_RESOURCE_POOL_ARRAY);

      ADD_MOD(OB_CS_SSTABLE_READER);
      ADD_MOD(OB_CS_TABLET_IMAGE);
      ADD_MOD(OB_CS_IMPORT_SSTABLE);

      ADD_MOD(OB_SSTABLE_AIO);
      ADD_MOD(OB_SSTABLE_EGT_SCAN);
      ADD_MOD(OB_SSTABLE_WRITER);
      ADD_MOD(OB_SSTABLE_READER);
      ADD_MOD(OB_COMPACTSSTABLE_WRITER);

      ADD_MOD(OB_MOD_END);
    }
    class ObModSet : public oceanbase::common::ObMemPoolModSet
    {
    public:
      ObModSet();
      virtual ~ObModSet()
      {
      }
      virtual int32_t get_mod_id(const char * mod_name)const;
      virtual const char * get_mod_name(const int32_t mod_id) const;
      virtual int32_t get_max_mod_num()const;
    private:
      int32_t mod_num_;
    };
  }
}

#endif /* OCEANBASE_COMMON_OB_MOD_DEFINE_H_ */
