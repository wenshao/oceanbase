/*
 * (C) 2007-2012 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.3: ob_ms_config_manager.h,v 0.3 2012/7/20 10:25:10 Exp $
 *
 * Authors:
 *   yuhuang <xiaochu.yh@taobao.com>
 *     - some work details if you want
 *
 */


#include "common/ob_define.h"
#include "common/ob_nb_accessor.h"
#include "ob_merge_server_params.h"
#include "ob_ms_config_manager.h"

using namespace oceanbase::common;
using namespace oceanbase::mergeserver;


const char *ObMergerConfigManager::table_name_ = "__all_sys_param";
const char *ObMergerConfigManager::item_name_str_ = "name";
const char *ObMergerConfigManager::item_value_str_ = "value";

ObMergerConfigManager::ObMergerConfigManager() : 
  init_(false), config_version_(0)
{

}


ObMergerConfigManager::~ObMergerConfigManager()
{
  init_ = false;
  config_version_ = 0;
}


// init in main thread
int ObMergerConfigManager::init(const ObMergeServerParams *param)
{
  int ret = OB_SUCCESS;
  if (true == init_)
  {
    TBSYS_LOG(WARN, "%s", "init config manager twice");
    ret = OB_INIT_TWICE;
  }
  else if (NULL == param)
  {
    TBSYS_LOG(WARN, "fail to init. param=null");
    ret = OB_INVALID_ARGUMENT;
  }
  else
  {
    //
    // TODO: add init here
    //

    // range
    query_range_.table_id_ = 3;
    query_range_.border_flag_.set_min_value();
    query_range_.border_flag_.set_max_value();
    query_range_.start_key_.assign(NULL, 0);
    query_range_.end_key_.assign(NULL, 0);
    // TODO: columns
    select_columns_("name")("value")("data_type");
    // TODO: filter
    /* scan_condition = */
    
    param_ = param;
    TBSYS_LOG(INFO, "init config manager succ:timestamp[%ld]", config_version_);

    init_ = true;
  }
  return ret;
}


int ObMergerConfigManager::update_config(common::QueryRes *res)
{
  /* basic idea:
   * 1. create a temp Params object, init it with in-using Params object
   * 2. then modify temp Params' value according to QueryRes
   * 3. then dump temp Params to cache
   * 4. in-using Params reload the cache
   */
  int ret = OB_SUCCESS;
  TableRow *table_row = NULL;
  ObCellInfo *item_name = NULL;
  ObCellInfo *item_value = NULL;
  ObMergeServerParams new_param;
  if (NULL == res)
  {
    TBSYS_LOG(WARN, "null res!");
    ret = OB_INVALID_ARGUMENT;
  }
  else if (!init_)
  {
    TBSYS_LOG(WARN, "must call init() first!");
    ret = OB_NOT_INIT;
  }
  else
  {
    OB_ASSERT(NULL != param_);
    param_->copy_to(new_param);
    while (OB_SUCCESS == (ret = res->next_row()))
    {
      if (OB_SUCCESS != (ret = res->get_row(&table_row)))
      {
        TBSYS_LOG(WARN, "fail to get row.ret=%d", ret);
        break;
      }
      OB_ASSERT(table_row);
      if (OB_SUCCESS == ret)
      {
        if (NULL == (item_name = table_row->get_cell_info(item_name_str_)))
        {
          TBSYS_LOG(WARN, "fail to get cell info for item_name");
        }
        else
        {
          item_name->value_.dump();
        }
      }
      if (OB_SUCCESS == ret)
      {
        if (NULL == (item_value = table_row->get_cell_info(item_value_str_)))
        {
          TBSYS_LOG(WARN, "fail to get cell info for item_value");
        }
        else
        {
          item_value->value_.dump();
        }
      }
      if (NULL != item_name && NULL != item_value)
      {
        if (OB_SUCCESS != (ret = new_param.update_item(item_name->value_, item_value->value_)))
        {
          TBSYS_LOG(WARN, "fail to update config item. ret=%d", ret);
          break;
        }
      }
      else
      {
        break;
      }
    }
    if (OB_ITER_END == ret)
    {
      if (OB_SUCCESS != (ret = new_param.dump_config_to_file(param_->get_config_cache_file_name())))
      {
        TBSYS_LOG(WARN, "fail to dump config to cache file. ret=%d", ret);
      }
    }
    if (OB_ITER_END != ret && OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "fail to update config. ret=%d", ret);
    }
  }
  return ret;
}

