/*
 * (C) 2007-2012 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.3: ob_ms_config_task.h,v 0.3 2012/7/20 10:25:10 Exp $
 *
 * Authors:
 *   yuhuang <xiaochu.yh@taobao.com>
 *     - some work details if you want
 *
 */

#ifndef OB_MERGER_CONFIG_TIMER_TASK_H_
#define OB_MERGER_CONFIG_TIMER_TASK_H_


#include "common/ob_timer.h"
#include "common/ob_server.h"
#include "ob_merge_server_params.h"

namespace oceanbase
{
  namespace mergeserver
  {
    class ObMergerConfigProxy;
    class ObMergerRpcStub;
    /// @brief check and fetch new config timer task
    class ObMergerConfigTask : public common::ObTimerTask
    {
    public:
      ObMergerConfigTask();
      ~ObMergerConfigTask();
    
    public:
      /// init set rpc and config manager
      void init(const ObMergerRpcStub *rpc_stub, ObMergerConfigProxy *config_proxy, 
          const common::ObServer &server, const char *filename, const int64_t timeout);
      
      /// set fetch new version
      void set_version(const int64_t remote);

      // main routine
      void runTimerTask(void);
    
    private:
      bool check_inner_stat(void) const; 
    
    public:
      volatile int64_t remote_version_;
      const ObMergerRpcStub *rpc_stub_;
      ObMergerConfigProxy * config_proxy_;
      common::ObServer server_;
      const char *filename_;
      int64_t timeout_;
    };
  }
}



#endif //OB_MERGER_SCHEMA_TIMER_TASK_H_

