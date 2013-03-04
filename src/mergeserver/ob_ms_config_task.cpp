/*
 * (C) 2007-2012 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.3: ob_ms_config_task.h,v 0.3 2012/7/20 10:25:10 zhidong Exp $
 *
 * Authors:
 *   yuhuang <xiaochu.yh@taobao.com>
 *     - some work details if you want
 *
 */

#include "ob_ms_config_task.h"
#include "ob_ms_config_proxy.h"
#include "ob_ms_rpc_stub.h"
#include "common/ob_malloc.h"

using namespace oceanbase::common;
using namespace oceanbase::mergeserver;

ObMergerConfigTask::ObMergerConfigTask() :
  remote_version_(0), rpc_stub_(NULL), config_proxy_ (NULL), server_(), filename_(NULL), timeout_(-1)
{
}

ObMergerConfigTask::~ObMergerConfigTask()
{
  remote_version_ = 0;
  rpc_stub_ = NULL;
  config_proxy_ = NULL;
  filename_ = NULL;
  timeout_ = -1;
}

void ObMergerConfigTask::init(const ObMergerRpcStub *rpc_stub, 
    ObMergerConfigProxy *config_proxy, 
    const ObServer &server, 
    const char *filename, 
    const int64_t timeout)
{
  rpc_stub_ = rpc_stub;
  config_proxy_ = config_proxy;
  filename_ = filename;
  server_ = server;
  timeout_ = timeout;
}
  
void ObMergerConfigTask::set_version(const int64_t new_version)
{
  remote_version_ = new_version;
}

bool ObMergerConfigTask::check_inner_stat(void) const
{
  return ((NULL != rpc_stub_) && (NULL != config_proxy_) && (NULL != filename_));
}

void ObMergerConfigTask::runTimerTask(void)
{
  int ret = OB_SUCCESS;
  UNUSED(ret);
  if (true != check_inner_stat())
  {
    TBSYS_LOG(ERROR, "%s", "check config timer task inner stat failed");
  }
  else 
  {
    if (OB_SUCCESS != (ret = config_proxy_->fetch_config(remote_version_)))
    {
      TBSYS_LOG(WARN, "fail to fetch config. ret=%d" ,ret);
    }
    else if (OB_SUCCESS != (ret = rpc_stub_->reload_self_config(timeout_, server_, filename_)))
    {
      TBSYS_LOG(WARN, "fail to reload config. ret=%d" ,ret);
    }
    else
    {
      TBSYS_LOG(INFO, "reload config success. version = %ld", remote_version_);
    }
  }
}



