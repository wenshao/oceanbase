/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: ob_ms_monitor_task.h,v 0.1 2010/10/29 10:25:10 zhidong Exp $
 *
 * Authors:
 *   chuanhui <xielun.szd@taobao.com>
 *     - some work details if you want
 *
 */

#ifndef OB_MERGER_MONITOR_TIMER_TASK_H_
#define OB_MERGER_MONITOR_TIMER_TASK_H_


#include "common/ob_server.h"
#include "common/ob_timer.h"

namespace oceanbase
{
  namespace mergeserver
  {
    class ObMergeServer;
    class ObMergerLocationCacheProxy;
    class ObMergerMonitorTask : public common::ObTimerTask
    {
    public:
      ObMergerMonitorTask();
      ~ObMergerMonitorTask();

    public:
      int init(const ObMergeServer * server, const int64_t threshold);

      void set_cache(const ObMergerLocationCacheProxy * proxy);

      void runTimerTask(void);

    private:
      bool check_inner_stat(void) const;

    private:
      uint64_t old_drop_counter_;
      int64_t min_drop_value_;
      const ObMergeServer * server_;
      const ObMergerLocationCacheProxy * location_proxy_;
    };

    inline bool ObMergerMonitorTask::check_inner_stat(void) const
    {
      return (server_ != NULL);
    }
  }
}


#endif //OB_MERGER_MONITOR_TIMER_TASK_H_

