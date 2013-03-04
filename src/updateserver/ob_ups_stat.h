////===================================================================
 //
 // ob_ups_stat.h / hash / common / Oceanbase
 //
 // Copyright (C) 2010 Taobao.com, Inc.
 //
 // Created on 2010-12-07 by Yubai (yubai.lk@taobao.com) 
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

#ifndef  OCEANBASE_UPDATESERVER_UPS_STAT_H_
#define  OCEANBASE_UPDATESERVER_UPS_STAT_H_
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <pthread.h>
#include <new>
#include <algorithm>
#include <tblog.h>
#include "common/ob_define.h"
#include "common/ob_statistics.h"
#include "sstable/ob_sstable_stat.h"

#define INC_STAT_INFO(stat_key, inc_value) \
  { \
    ObUpdateServerMain *main = ObUpdateServerMain::get_instance(); \
    if (NULL == main) \
    { \
      TBSYS_LOG(ERROR, "get updateserver main null pointer"); \
    } \
    else \
    { \
      UpsStatMgr &stat_mgr = main->get_update_server().get_stat_mgr(); \
      stat_mgr.inc(UpsStatMgr::UPS_STAT_TOTAL, stat_key, inc_value); \
    } \
  }

#define SET_STAT_INFO(stat_key, inc_value) \
  { \
    ObUpdateServerMain *main = ObUpdateServerMain::get_instance(); \
    if (NULL == main) \
    { \
      TBSYS_LOG(ERROR, "get updateserver main null pointer"); \
    } \
    else \
    { \
      UpsStatMgr &stat_mgr = main->get_update_server().get_stat_mgr(); \
      stat_mgr.set_value(UpsStatMgr::UPS_STAT_TOTAL, stat_key, inc_value); \
    } \
  }

namespace oceanbase
{
  namespace updateserver
  {
    enum
    {
      UPS_STAT_MIN = 0,

      UPS_STAT_GET_COUNT = 1,
      UPS_STAT_SCAN_COUNT = 2,
      UPS_STAT_APPLY_COUNT = 3, 
      UPS_STAT_BATCH_COUNT = 4,
      UPS_STAT_MERGE_COUNT = 5,

      UPS_STAT_GET_TIMEU = 6,
      UPS_STAT_SCAN_TIMEU = 7,
      UPS_STAT_APPLY_TIMEU = 8, 
      UPS_STAT_BATCH_TIMEU = 9,
      UPS_STAT_MERGE_TIMEU = 10,

      UPS_STAT_MEMORY_TOTAL = 11,
      UPS_STAT_MEMORY_LIMIT = 12,
      UPS_STAT_MEMTABLE_TOTAL = 13,
      UPS_STAT_MEMTABLE_USED = 14,
      UPS_STAT_TOTAL_LINE = 15,

      UPS_STAT_ACTIVE_MEMTABLE_LIMIT = 16,
      UPS_STAT_ACTICE_MEMTABLE_TOTAL = 17,
      UPS_STAT_ACTIVE_MEMTABLE_USED = 18,
      UPS_STAT_ACTIVE_TOTAL_LINE = 19,

      UPS_STAT_FROZEN_MEMTABLE_LIMIT = 20,
      UPS_STAT_FROZEN_MEMTABLE_TOTAL = 21,
      UPS_STAT_FROZEN_MEMTABLE_USED = 22,
      UPS_STAT_FROZEN_TOTAL_LINE = 23,

      UPS_STAT_APPLY_FAIL_COUNT = 24,
      UPS_STAT_TBSYS_DROP_COUNT = 25,
      UPS_STAT_PACKET_LONG_WAIT_COUNT = 26,

      UPS_STAT_MAX = 27,
    };
    class UpsStatMgr : public common::ObStatManager
    {
      const static int SSTABLE_STAT_NUM = 6;
      public:
        enum
        {
          UPS_STAT_TOTAL = 1,
          SSTABLE_STAT_TOTAL = 2,
        };
      public:
        UpsStatMgr() : ObStatManager(common::ObStatManager::SERVER_TYPE_UPDATE)
        {
        };
        inline void print_info() const
        {
          ObStatManager::const_iterator iter;
          for (iter = begin(); iter != end(); ++iter)
          {
            if (UPS_STAT_TOTAL == iter->get_table_id())
            {
              for (int stat_key = UPS_STAT_MIN + 1; stat_key < UPS_STAT_MAX; ++stat_key)
              {
                TBSYS_LOG(INFO, "table_id=%lu\tstat_key=%s\tstat_value=%ld",
                          iter->get_table_id(), get_ups_stat_str(stat_key), iter->get_value(stat_key));
              }
            }
            else if (SSTABLE_STAT_TOTAL == iter->get_table_id())
            {
              for (int stat_key = 0; stat_key < SSTABLE_STAT_NUM; ++stat_key)
              {
                TBSYS_LOG(INFO, "table_id=%lu\tstat_key=%s\tstat_value=%ld",
                          iter->get_table_id(), get_sstable_stat_str(stat_key), iter->get_value(stat_key));
              }
            }
          }
        };
        inline static const char *get_sstable_stat_str(const int stat_key)
        {
          static const char *stat_str_set[] = {
            "block_index_cache_hit",
            "block_index_cache_miss",
            "block_cache_hit",
            "block_cache_miss",
            "disk_io_num",
            "disk_io_bytes",
          };
          const char *ret = NULL;
          if (0 <= stat_key
              && SSTABLE_STAT_NUM > stat_key)
          {
            ret = stat_str_set[stat_key];
          }
          else
          {
            ret = "nil";
          }
          return ret;
        };
        inline static const char *get_ups_stat_str(const int stat_key)
        {
          static const char *stat_str_set[] = {
            "nil",
            
            "get_count",
            "scan_count",
            "apply_count",
            "batch_count",
            "merge_count",

            "get_timeu",
            "scan_timeu",
            "apply_timeu",
            "batch_timeu",
            "merge_timeu",

            "memory_total",
            "memory_limit",
            "memtable_total",
            "memtable_used",
            "total_line",

            "active_memtable_limit",
            "active_memtable_total",
            "active_memtable_used",
            "active_total_line",

            "frozen_memtable_limit",
            "frozen_memtable_total",
            "frozen_memtable_used",
            "frozen_total_line",

            "apply_fail_count",
            "tbsys_drop_count",
            "pakcet_long_wait_count",
          };
          const char *ret = NULL;
          if (UPS_STAT_MIN < stat_key
              && UPS_STAT_MAX > stat_key)
          {
            ret = stat_str_set[stat_key];
          }
          else
          {
            ret = stat_str_set[0];
          }
          return ret;
        };
    };
  }
}

#endif //OCEANBASE_UPDATESERVER_UPS_STAT_H_

