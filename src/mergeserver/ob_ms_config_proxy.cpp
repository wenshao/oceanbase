/*
 * (C) 2007-2012 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.3: ob_ms_config_proxy.cpp,v 0.3 2012/7/20 10:25:10 Exp $
 *
 * Authors:
 *   yuhuang <xiaochu.yh@taobao.com>
 *     - some work details if you want
 *
 */



#include "common/ob_define.h"
#include "common/ob_nb_accessor.h"
#include "ob_ms_config_proxy.h"
#include "ob_ms_config_manager.h"

using namespace oceanbase::common;
using namespace oceanbase::mergeserver;

ObMergerConfigProxy::ObMergerConfigProxy() :
  rpc_(NULL), config_manager_(NULL)
{
}

ObMergerConfigProxy::ObMergerConfigProxy(common::ObNbAccessor * rpc, ObMergerConfigManager * config) :
  rpc_(rpc), config_manager_(config)
{
}

ObMergerConfigProxy::~ObMergerConfigProxy()
{
  rpc_ = NULL;
  config_manager_ = NULL;
}

int ObMergerConfigProxy::fetch_config(const int64_t version)
{
  int ret = OB_SUCCESS;
  QueryRes *query_res = NULL;
  UNUSED(version);
  if (true == check_inner_stat())
  {
    // scan __all_sys_param with condition
    if (OB_SUCCESS == (ret = rpc_->scan(query_res, 
        config_manager_->get_table_name(), 
        config_manager_->get_query_range(), 
        config_manager_->get_select_columns(), 
        config_manager_->get_scan_condition())))
    {
      if(OB_SUCCESS == (ret = config_manager_->update_config(query_res)))
      {
        TBSYS_LOG(INFO, "mergeserver config update success");
      }
      else
      {
        TBSYS_LOG(WARN, "fail to update ms config. ret=%d", ret);
      }
    }
    else
    {
      TBSYS_LOG(WARN, "fail to scan new config for ms. table[%s], ret=%d", 
          config_manager_->get_table_name(), ret);
    }
  }
  else
  {
    TBSYS_LOG(WARN, "config proxy not inited");
    ret = OB_NOT_INIT;
  }
  return ret;
}

