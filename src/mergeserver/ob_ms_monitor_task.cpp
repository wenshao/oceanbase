/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: ob_ms_monitor_task.h,v 0.1 2011/05/25 15:08:32 zhidong Exp $
 *
 * Authors:
 *   xielun <xielun.szd@taobao.com>
 *     - some work details if you want
 *
 */

#include "ob_merge_server.h"
#include "ob_ms_monitor_task.h"
#include "ob_ms_tablet_location_proxy.h"
#include "ob_ms_counter_infos.h"
#include "common/ob_malloc.h"

using namespace oceanbase::common;
using namespace oceanbase::mergeserver;

ObMergerMonitorTask::ObMergerMonitorTask()
{
  location_proxy_ = NULL;
  old_drop_counter_ = 0;
  min_drop_value_ = 1;
}

ObMergerMonitorTask::~ObMergerMonitorTask()
{
}

void ObMergerMonitorTask::set_cache(const ObMergerLocationCacheProxy * proxy)
{
  location_proxy_ = proxy;
}

int ObMergerMonitorTask::init(const ObMergeServer * server, const int64_t threshold)
{
  int ret = OB_SUCCESS;
  if (NULL == server)
  {
    ret = OB_INPUT_PARAM_ERROR;
    TBSYS_LOG(WARN, "check merge server failed:server[%p]", server);
  }
  else
  {
    server_ = server;
    if (threshold > 0)
    {
      min_drop_value_ = threshold;
    }
    else
    {
      TBSYS_LOG(WARN, "check drop error log threshold failed:count[%ld]", threshold);
    }
  }
  return ret;
}

void ObMergerMonitorTask::runTimerTask(void)
{
  if (true != check_inner_stat())
  {
    TBSYS_LOG(ERROR, "%s", "check monitor timer task inner stat failed");
  }
  else
  {
    if (location_proxy_ != NULL)
    {
      location_proxy_->dump();
    }
    ms_get_counter_set().print_static_info(TBSYS_LOG_LEVEL_INFO);
    uint64_t new_drop_counter = server_->get_drop_packet_count();
    if (new_drop_counter >= (old_drop_counter_ + min_drop_value_))
    {
      TBSYS_LOG(ERROR, "check dropped packet count:drop[%lu], queue[%ld], threshold[%ld], "
          "total[%lu], old[%lu]", new_drop_counter - old_drop_counter_,
          server_->get_current_queue_size(), min_drop_value_, new_drop_counter, old_drop_counter_);
    }
    old_drop_counter_ = new_drop_counter;
    ob_print_mod_memory_usage();
  }
}



