/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: ob_ms_timer_task.h,v 0.1 2010/10/29 10:25:10 zhidong Exp $
 *
 * Authors:
 *   chuanhui <xielun.szd@taobao.com>
 *     - some work details if you want
 *
 */

#ifndef OB_MERGER_SCHEMA_TIMER_TASK_H_
#define OB_MERGER_SCHEMA_TIMER_TASK_H_


#include "common/ob_timer.h"

namespace oceanbase
{
  namespace mergeserver
  {
    class ObMergerRpcProxy;
    class ObMergerSchemaManager;

    /// @brief check and fetch new schema timer task
    class ObMergerSchemaTask : public common::ObTimerTask
    {
    public:
      ObMergerSchemaTask();
      ~ObMergerSchemaTask();
    
    public:
      /// init set rpc and schema manager
      void init(ObMergerRpcProxy * rpc, ObMergerSchemaManager * schema);
      
      /// set fetch new version
      void set_version(const int64_t local, const int64_t remote);

      // main routine
      void runTimerTask(void);
    
    private:
      bool check_inner_stat(void) const; 
    
    public:
      volatile int64_t local_version_;
      volatile int64_t remote_version_;
      ObMergerRpcProxy * rpc_proxy_;
      ObMergerSchemaManager * schema_;
    };
    
    inline void ObMergerSchemaTask::init(ObMergerRpcProxy * rpc, ObMergerSchemaManager * schema)
    {
      local_version_ = 0;
      remote_version_ = 0;
      rpc_proxy_ = rpc;
      schema_ = schema;
    }
    
    inline void ObMergerSchemaTask::set_version(const int64_t local, const int64_t server)
    {
      local_version_ = local;
      remote_version_ = server;
    }

    inline bool ObMergerSchemaTask::check_inner_stat(void) const
    {
      return ((NULL != rpc_proxy_) && (NULL != schema_));
    }
  }
}



#endif //OB_MERGER_SCHEMA_TIMER_TASK_H_

