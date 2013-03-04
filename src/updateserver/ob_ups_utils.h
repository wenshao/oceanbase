////===================================================================
 //
 // ob_ups_utils.h / hash / common / Oceanbase
 //
 // Copyright (C) 2010, 2011 Taobao.com, Inc.
 //
 // Created on 2010-10-13 by Yubai (yubai.lk@taobao.com) 
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

#ifndef  OCEANBASE_UPDATESERVER_UPS_UTILS_H_
#define  OCEANBASE_UPDATESERVER_UPS_UTILS_H_
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <pthread.h>
#include <new>
#include <algorithm>
#include "common/ob_define.h"
#include "common/ob_read_common_data.h"
#include "common/ob_object.h"
#include "common/serialization.h"
#include "common/ob_schema.h"
#include "common/page_arena.h"
#include "common/hash/ob_hashmap.h"
#include "common/ob_file.h"
#include "common/ob_timer.h"
#include "common/ob_scanner.h"
#include "common/ob_packet.h"
#include "common/utility.h"
#include "common/ob_tablet_info.h"

namespace oceanbase
{
  namespace updateserver
  {
    typedef common::ObSchemaManagerV2 CommonSchemaManager;
    typedef common::ObColumnSchemaV2 CommonColumnSchema;
    typedef common::ObTableSchema CommonTableSchema;

    class TEKey;
    class TEValue;
    struct CacheWarmUpConf;
    struct SSTableID;
    extern bool is_in_range(const int64_t key, const common::ObVersionRange &version_range);
    extern bool is_in_range(const common::ObString &key, const common::ObRange &range);
    extern bool is_range_valid(const common::ObVersionRange &version_range);
    extern int precise_sleep(const int64_t microsecond);
    extern const char *inet_ntoa_r(const uint64_t ipport);
    extern int64_t get_max_row_cell_num();
    extern int64_t get_table_available_warn_size();
    extern int64_t get_table_available_error_size();
    extern int64_t get_table_memory_limit();
    extern bool ups_available_memory_warn_callback(const int64_t mem_size_available);
    extern const CacheWarmUpConf &get_warm_up_conf();
    extern void set_warm_up_percent(const int64_t warm_up_percent);
    extern void submit_force_drop();
    extern void schedule_warm_up_duty();
    extern bool using_memtable_bloomfilter();
    extern bool sstable_dio_writing();
    extern void log_scanner(common::ObScanner *scanner);
    extern const char *print_scanner_info(common::ObScanner *scanner);
    extern int64_t get_active_mem_limit();
    extern int64_t get_oldest_memtable_size();
    extern void submit_load_bypass(const common::ObPacket *packet = NULL);
    extern void submit_immediately_drop();
    extern uint64_t get_create_time_column_id(const uint64_t table_id);
    extern uint64_t get_modify_time_column_id(const uint64_t table_id);
    extern void set_client_mgr_err(const int err);

    struct GConf
    {
      bool using_static_cm_column_id;
      volatile int64_t cur_time;
      volatile int64_t global_schema_version;
      bool using_hash_index;
    };
    extern GConf g_conf;

#define OB_UPS_CREATE_TIME_COLUMN_ID(table_id) \
    ({ \
      uint64_t ret = OB_CREATE_TIME_COLUMN_ID; \
      if (!g_conf.using_static_cm_column_id) \
      { \
        ret = get_create_time_column_id(table_id); \
      } \
      ret; \
    })
#define OB_UPS_MODIFY_TIME_COLUMN_ID(table_id) \
    ({ \
      uint64_t ret = OB_MODIFY_TIME_COLUMN_ID; \
      if (!g_conf.using_static_cm_column_id) \
      { \
        ret = get_modify_time_column_id(table_id); \
      } \
      ret; \
    })

    class IObjIterator
    {
      public:
        virtual ~IObjIterator() {};
      public:
        virtual int next_obj() = 0;
        virtual int get_obj(common::ObObj **obj) = 0;
    };

    template <class T>
    int ups_serialize(const T &data, char *buf, const int64_t data_len, int64_t& pos)
    {
      return data.serialize(buf, data_len, pos);
    };

    template <class T>
    int ups_deserialize(T &data, char *buf, const int64_t data_len, int64_t& pos)
    {
      return data.deserialize(buf, data_len, pos);
    };

    template <>
    int ups_serialize<uint64_t>(const uint64_t &data, char *buf, const int64_t data_len, int64_t& pos);
    template <>
    int ups_serialize<int64_t>(const int64_t &data, char *buf, const int64_t data_len, int64_t& pos);

    template <>
    int ups_deserialize<uint64_t>(uint64_t &data, char *buf, const int64_t data_len, int64_t& pos);
    template <>
    int ups_deserialize<int64_t>(int64_t &data, char *buf, const int64_t data_len, int64_t& pos);

    template <>
    int ups_serialize<uint32_t>(const uint32_t &data, char *buf, const int64_t data_len, int64_t& pos);
    template <>
    int ups_serialize<int32_t>(const int32_t &data, char *buf, const int64_t data_len, int64_t& pos);

    template <>
    int ups_deserialize<uint32_t>(uint32_t &data, char *buf, const int64_t data_len, int64_t& pos);
    template <>
    int ups_deserialize<int32_t>(int32_t &data, char *buf, const int64_t data_len, int64_t& pos);    

    struct Dummy
    {
      int serialize(char* buf, int64_t len, int64_t& pos) const
      {
        UNUSED(buf); UNUSED(len); UNUSED(pos);
        return common::OB_SUCCESS;
      }
      int deserialize(char* buf, int64_t len, int64_t& pos)
      {
        UNUSED(buf); UNUSED(len); UNUSED(pos);
        return common::OB_SUCCESS;
      }
    };
    extern Dummy __dummy__;

    class SwitchSKeyDuty : public common::ObTimerTask
    {
      public:
        SwitchSKeyDuty() {};
        virtual ~SwitchSKeyDuty() {};
        virtual void runTimerTask();
    };

    class TimeUpdateDuty : public common::ObTimerTask
    {
      public:
        static const int64_t SCHEDULE_PERIOD = 2000;
        TimeUpdateDuty() {};
        virtual ~TimeUpdateDuty() {};
        virtual void runTimerTask();
    };

    struct TableMemInfo
    {
      int64_t memtable_used;
      int64_t memtable_total;
      int64_t memtable_limit;
      TableMemInfo()
      {
        memtable_used = 0;
        memtable_total = 0;
        memtable_limit = INT64_MAX;
      };
    };

    struct UpsPrivQueueConf
    {
      int64_t low_priv_network_lower_limit;
      int64_t low_priv_network_upper_limit;
      int64_t low_priv_adjust_flag;
      int64_t low_priv_cur_percent;
      int64_t low_priv_max_percent;

      int serialize(char* buf, const int64_t buf_len, int64_t& pos) const
      {
        int ret = common::OB_SUCCESS;
        if ((pos + (int64_t)sizeof(*this)) > buf_len)
        {
          ret = common::OB_ERROR;
        }
        else
        {
          memcpy(buf + pos, this, sizeof(*this));
          pos += sizeof(*this);
        }
        return ret;
      }

      int deserialize(const char* buf, const int64_t buf_len, int64_t& pos)
      {
        int ret = common::OB_SUCCESS;
        if ((pos + (int64_t)sizeof(*this)) > buf_len)
        {
          ret = common::OB_ERROR;
        }
        else
        {
          memcpy(this, buf + pos, sizeof(*this));
          pos += sizeof(*this);
        }
        return ret;
      };

      int64_t get_serialize_size(void) const
      {
        return sizeof(*this);
      };
    };

    struct UpsMemoryInfo
    {
      const int64_t version;
      int64_t total_size;
      int64_t cur_limit_size;
      TableMemInfo table_mem_info;
      UpsMemoryInfo() : version(1), table_mem_info()
      {
        total_size = 0;
        cur_limit_size = INT64_MAX;
      };
      int serialize(char* buf, const int64_t buf_len, int64_t& pos) const
      {
        int ret = common::OB_SUCCESS;
        if ((pos + (int64_t)sizeof(*this)) > buf_len)
        {
          ret = common::OB_ERROR;
        }
        else
        {
          memcpy(buf + pos, this, sizeof(*this));
          pos += sizeof(*this);
        }
        return ret;
      };
      int deserialize(const char* buf, const int64_t buf_len, int64_t& pos)
      {
        int ret = common::OB_SUCCESS;
        if ((pos + (int64_t)sizeof(*this)) > buf_len)
        {
          ret = common::OB_ERROR;
        }
        else
        {
          memcpy(this, buf + pos, sizeof(*this));
          pos += sizeof(*this);
        }
        return ret;
      };
      int64_t get_serialize_size(void) const
      {
        return sizeof(*this);
      };
    };

    struct CacheWarmUpConf
    {
      static const int64_t STOP_PERCENT = 100; // 100%
      static const int64_t STEP_PERCENT = 1; // 1%
      static const int64_t DEFAULT_WARM_UP_TIME_S = 600; //10min
      static const int64_t MIN_WARM_UP_TIME_S = 10; // 10s
      static const int64_t MAX_WARM_UP_TIME_S = 1800; // 30min

      // 预热时间(默认10分钟)
      int64_t warm_up_time_s;
      // 读取sstable的比例两次步进的时间间隔(根据warm_up_time_us计算出)
      int64_t warm_up_step_interval_us;

      CacheWarmUpConf() : warm_up_time_s(DEFAULT_WARM_UP_TIME_S),
                          warm_up_step_interval_us(DEFAULT_WARM_UP_TIME_S * 1000L * 1000L / (STOP_PERCENT / STEP_PERCENT))
      {
      };

      bool check()
      {
        bool bret = false;
        if (0 == warm_up_time_s)
        {
          warm_up_step_interval_us = 0;
          bret = true;
        }
        else if (MIN_WARM_UP_TIME_S > warm_up_time_s)
        {
          TBSYS_LOG(WARN, "warm_up_time_s=%ld cannot smaller than %ld", warm_up_time_s, MIN_WARM_UP_TIME_S);
        }
        else if (MAX_WARM_UP_TIME_S < warm_up_time_s)
        {
          TBSYS_LOG(WARN, "warm_up_time_s=%ld cannot larger than %ld", warm_up_time_s, MAX_WARM_UP_TIME_S);
        }
        else
        {
          warm_up_step_interval_us = warm_up_time_s * 1000L * 1000L / (STOP_PERCENT / STEP_PERCENT);
          bret = true;
        }
        return bret;
      };
    };

    struct TabletInfoList
    {
      common::ObStringBuf allocator;
      common::ObTabletInfoList inst;
    };
  }

  namespace common
  {
    template <>
    struct ob_vector_traits<ObTabletInfo>
    {
    public:
      typedef ObTabletInfo& pointee_type;
      typedef ObTabletInfo value_type;
      typedef const ObTabletInfo const_value_type;
      typedef value_type* iterator;
      typedef const value_type* const_iterator;
      typedef int32_t difference_type;
    };

    struct ObTableInfoEndkeyComp
    {
      bool operator() (const ObTabletInfo &a, const ObTabletInfo &b) const
      {
        return (a.range_.end_key_ < b.range_.end_key_);
      };
    };
  }
}

#endif //OCEANBASE_UPDATESERVER_UPS_UTILS_H_

