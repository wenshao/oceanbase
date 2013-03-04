/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or 
 * counterify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_ms_counter_infos.h for 
 *
 * Authors:
 *   wushi <wushi.ly@taobao.com>
 *
 */
#ifndef MERGESERVER_OB_MS_COUNTER_INFOS_H_ 
#define MERGESERVER_OB_MS_COUNTER_INFOS_H_
#include "common/ob_define.h"
#include "common/ob_counter.h"
#include "tbsys.h"

namespace oceanbase
{
  namespace mergeserver
  {
    struct ObMergerCounterInfo
    {
      ObMergerCounterInfo()
      {
        counter_id_  = 0;
        counter_name_ = NULL;
        static_interval_ = 0;
      }
      int32_t counter_id_;
      const char * counter_name_;
      int64_t static_interval_;
    };


    static const int32_t G_MAX_COUNTER_NUM = 1024;
    extern ObMergerCounterInfo OB_COUNTER_SET[G_MAX_COUNTER_NUM]; 


#define DEFINE_COUNTER(counter_name) counter_name

#define ADD_TIME_COUNTER(counter_name, static_interval)\
  do \
  {\
    if (ObMergerCounterIds::counter_name < ObMergerCounterIds::C_MS_COUNTER_END\
    && ObMergerCounterIds::counter_name > ObMergerCounterIds::C_MS_COUNTER_BEG\
    && ObMergerCounterIds::counter_name < G_MAX_COUNTER_NUM) \
    {\
      OB_COUNTER_SET[ObMergerCounterIds::counter_name].counter_id_ = ObMergerCounterIds::counter_name; \
      OB_COUNTER_SET[ObMergerCounterIds::counter_name].counter_name_ = # counter_name;\
      OB_COUNTER_SET[ObMergerCounterIds::counter_name].static_interval_ = (static_interval);\
    }\
    else \
    {\
      TBSYS_LOG(ERROR,"unknow counter id [counter_id:%d]", ObMergerCounterIds::counter_name);\
    }\
  }while (0);

#define ADD_COUNTER(counter_name) \
  ADD_TIME_COUNTER(counter_name, 1000000);



    /// 使用方法：
    /// 1. 在ObMergerCounterIds中定义自己的模块名称, e.x. OB_MS_CELL_ARRAY
    /// 2. 在init_ob_counter_set中添加之前定义的模块名称，e.x. ADD_COUNTER(OB_MS_CELL_ARRAY), ADD_TIME_COUNTER(OB_MS_CELL_ARRAY, 1000000)
    /// 3. 在调用ob_malloc的时候，使用定义的模块名称作为第二个参数，e.x. ob_malloc(512, ObMergerCounterIds::OB_MS_CELL_ARRAY)
    /// 4. 发现内存泄露，调用ob_print_memory_usage()打印每个模块的内存使用量，以发现内存泄露模块
    /// 5. 也可以通过调用ob_get_counter_memory_usage(ObMergerCounterIds::OB_MS_CELL_ARRAY)获取单个模块的内存使用量
    class ObMergerCounterIds
    {
    public:
      enum 
      {
        C_MS_COUNTER_BEG = -1,
        C_CLIENT_GET,
        C_CLIENT_SCAN,
        C_CLIENT_DROP,
        C_CS_GET,
        C_CS_SCAN,
        /// define other counters here
        C_REGISTER_MS,
        C_HEART_BEAT,
        C_FETCH_SCHEMA,
        C_FETCH_SCHEMA_VERSION,
        C_SCAN_ROOT_TABLE,
        C_FETCH_STATUS,
        C_CLEAR_REQUEST,
        C_QUERY_LESS_THAN_10MS      ,
        C_QUERY_LESS_THAN_50MS      ,
        C_QUERY_LESS_THAN_100MS     ,
        C_QUERY_LESS_THAN_200MS     ,
        C_QUERY_LESS_THAN_500MS     ,
        C_QUERY_LESS_THAN_1000MS    ,
        C_QUERY_LESS_THAN_2000MS    ,
        C_QUERY_LESS_THAN_5000MS    ,
        C_QUERY_LESS_THAN_10000MS   ,
        C_QUERY_LESS_THAN_GT10S     ,

        C_MS_TEST,
        C_MS_COUNTER_END
      };
    };
    inline int64_t get_counter_id_for_proc_time(const int64_t process_time)
    {
      int64_t res = -1;
      if (process_time <= 10000)
      {
        res = ObMergerCounterIds::C_QUERY_LESS_THAN_10MS;
      }
      else if (process_time <= 50000)
      {
        res = ObMergerCounterIds::C_QUERY_LESS_THAN_50MS;
      }
      else if (process_time <= 100000)
      {
        res = ObMergerCounterIds::C_QUERY_LESS_THAN_100MS;
      }
      else if (process_time <= 200000)
      {
        res = ObMergerCounterIds::C_QUERY_LESS_THAN_200MS;
      }
      else if (process_time <= 500000)
      {
        res = ObMergerCounterIds::C_QUERY_LESS_THAN_500MS;
      }
      else if (process_time <= 1000000)
      {
        res = ObMergerCounterIds::C_QUERY_LESS_THAN_1000MS;
      }
      else if (process_time <= 2000000)
      {
        res = ObMergerCounterIds::C_QUERY_LESS_THAN_2000MS;
      }
      else if (process_time <= 5000000)
      {
        res = ObMergerCounterIds::C_QUERY_LESS_THAN_5000MS;
      }
      else if (process_time <= 10000000)
      {
        res = ObMergerCounterIds::C_QUERY_LESS_THAN_10000MS;
      }
      else
      {
        res = ObMergerCounterIds::C_QUERY_LESS_THAN_GT10S;
      }
      return res;
    }

    inline void init_ob_counter_set()
    {
      ADD_COUNTER(C_CLIENT_GET);
      ADD_COUNTER(C_CLIENT_SCAN);
      ADD_COUNTER(C_CLIENT_DROP);
      ADD_COUNTER(C_CS_GET);
      ADD_COUNTER(C_CS_SCAN);
      /// add other counters here
      ADD_COUNTER(C_REGISTER_MS);
      ADD_COUNTER(C_HEART_BEAT);
      ADD_COUNTER(C_FETCH_SCHEMA);
      ADD_COUNTER(C_FETCH_SCHEMA_VERSION);
      ADD_COUNTER(C_SCAN_ROOT_TABLE);
      ADD_COUNTER(C_FETCH_STATUS);
      ADD_COUNTER(C_CLEAR_REQUEST);

      ADD_COUNTER(C_QUERY_LESS_THAN_10MS      );
      ADD_COUNTER(C_QUERY_LESS_THAN_50MS      );
      ADD_COUNTER(C_QUERY_LESS_THAN_100MS     );
      ADD_COUNTER(C_QUERY_LESS_THAN_200MS     );
      ADD_COUNTER(C_QUERY_LESS_THAN_500MS     );
      ADD_COUNTER(C_QUERY_LESS_THAN_1000MS    );
      ADD_COUNTER(C_QUERY_LESS_THAN_2000MS    );
      ADD_COUNTER(C_QUERY_LESS_THAN_5000MS    );
      ADD_COUNTER(C_QUERY_LESS_THAN_10000MS   );
      ADD_COUNTER(C_QUERY_LESS_THAN_GT10S     );

      /// counter for test
      ADD_TIME_COUNTER(C_MS_TEST, 10000);
    }
    class ObMergerCounterInfoSet : public oceanbase::common::ObCounterInfos
    {
    public:
      virtual const char * get_counter_name(const int64_t counter_id) const;
      virtual int64_t get_counter_static_interval(const int64_t counter_id) const;
      virtual int64_t get_max_counter_size()const;
    };

    ObMergerCounterInfoSet &ms_get_counter_infos();
    oceanbase::common::ObCounterSet &ms_get_counter_set();
  }
}


#endif /* MERGESERVER_OB_MS_COUNTER_INFOS_H_ */
