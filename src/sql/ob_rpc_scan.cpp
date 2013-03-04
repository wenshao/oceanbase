/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_rpc_scan.cpp
 *
 * ObRpcScan operator
 *
 * Authors:
 *   Yu Huang <xiaochu.yh@taobao.com>
 *
 */
#include "ob_rpc_scan.h"
#include "common/ob_tsi_factory.h"


using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::mergeserver;
using namespace oceanbase::sql;

ObRpcScan::ObRpcScan() :
  timeout_us_(0), sql_scan_event_(NULL), merger_scan_param_(),
  scan_param_(), cur_row_(), cur_row_desc_(), table_id_(0)
{
// mergeserver::ObMsSqlScanEvent sql_scan_event_;
//        mergeserver::ObMergerScanParam scan_param_;
}


ObRpcScan::~ObRpcScan()
{

}


int ObRpcScan::init(mergeserver::ObMergerLocationCacheProxy * cache_proxy, mergeserver::ObMergerAsyncRpcStub * async_rpc)
{
  int ret = OB_SUCCESS;
  if (NULL == cache_proxy || NULL == async_rpc)
  {
    ret = OB_INVALID_ARGUMENT;
  }
  else if (NULL == (sql_scan_event_ = GET_TSI_ARGS(ObMsSqlScanEvent, TSI_MS_SCAN_EVENT_1, cache_proxy, async_rpc)))
  {
    TBSYS_LOG(WARN, "fail to allocate ObMsSqlScanEvent object");
    ret = OB_ERROR;
  }
  else if (OB_SUCCESS != (ret = sql_scan_event_->init(REQUEST_EVENT_QUEUE_SIZE, ObModIds::OB_SQL_RPC_SCAN)))
  {
    TBSYS_LOG(WARN, "fail to init sql_scan_event. ret=%d", ret);
  }
  return ret;
}


int ObRpcScan::create_scan_param(mergeserver::ObMergerScanParam &merger_param)
{
  int ret = OB_SUCCESS;
  cur_row_.set_row_desc(cur_row_desc_);
  if (OB_SUCCESS != (ret = merger_param.set_param(scan_param_)))
  {
    TBSYS_LOG(WARN, "fail to set param. ret=%d", ret);
  }
  return ret;
}


int64_t ObRpcScan::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  databuff_printf(buf, buf_len, pos, "TableScan(tid=%lu)", scan_param_.get_table_id());
  return pos;
}


int ObRpcScan::open()
{
  int ret = OB_SUCCESS;
  int max_parallel = 3;
  int max_tablet_per_req = 3;

  timeout_us_ = 10000000;
  if (NULL == sql_scan_event_)
  {
    TBSYS_LOG(WARN, "sql_scan_event_ is null");
    ret = OB_ERROR;
  }
  else if (OB_SUCCESS != (ret = create_scan_param(merger_scan_param_)))
  {
    TBSYS_LOG(WARN, "fail to create scan param. ret=%d", ret);
  }
  else if(OB_SUCCESS != (ret = sql_scan_event_->set_request_param(merger_scan_param_, timeout_us_, max_parallel, max_tablet_per_req)))
  {
    TBSYS_LOG(WARN, "fail to set request param. max_parellel=%d, max_tablet_per_req=%d, ret=%d", max_parallel, max_tablet_per_req, ret);
  }
  return ret;
}


int ObRpcScan::close()
{
  int ret = OB_SUCCESS;
  //释放资源等;
  return ret;
}

int ObRpcScan::get_row_desc(const common::ObRowDesc *&row_desc) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(table_id_ <= 0 || 0 >= cur_row_desc_.get_column_num()))
  {
    TBSYS_LOG(ERROR, "not init, tid=%lu", table_id_);
    ret = OB_NOT_INIT;
  }
  else
  {
    row_desc = &cur_row_desc_;
  }
  return ret;
}

int ObRpcScan::get_next_row(const common::ObRow *&row)
{
  int ret = OB_SUCCESS;
  ret = get_next_compact_row(row); // 可能需要等待CS返回
  return ret;
}


/**
 * 函数功能： 从scan_event中获取一行数据
 * 说明：
 * wait的功能：从finish_queue中阻塞地pop出一个事件（如果没有事件则阻塞）， 然后调用process_result()处理事件
 */
int ObRpcScan::get_next_compact_row(const common::ObRow *&row)
{
  int ret = OB_SUCCESS;
  bool can_break = false;
  row = NULL;
  do
  {
    ret = sql_scan_event_->get_next_row(cur_row_);
    if (OB_ITER_END == ret && sql_scan_event_->is_finish())
    {
      // finish all data
      // can break;
      can_break = true;
    }
    else if (OB_ITER_END == ret)
    {
      // need to wait for incomming data
      can_break = false;
      // if timeout, can_break = true
      if( OB_SUCCESS != (ret = sql_scan_event_->wait_single_event(timeout_us_)))
      {
        if (timeout_us_ <= 0)
        {
          TBSYS_LOG(WARN, "wait timeout. timeout_us_=%ld", timeout_us_);
        }
        can_break = true;
      }
      else
      {
        TBSYS_LOG(DEBUG, "got a scan event. timeout_us_=%ld", timeout_us_);
      }
    }
    else if (OB_SUCCESS == ret)
    {
      // got a row without block
      can_break = true;
    }
    else
    {
      // encounter an unexpected error or
      TBSYS_LOG(WARN, "Unexprected error. ret=%d", ret);
      can_break = true;
    }
  }while(false == can_break);
  if (OB_SUCCESS == ret)
  {
    row = &cur_row_;
  }
  return ret;
}


int ObRpcScan::add_output_column(const ObSqlExpression& expr)
{
  int ret = OB_SUCCESS;
  const ObObj *obj_expr = NULL;
  int size = 0;
  bool is_cid = false;
  if (table_id_ <= 0 || table_id_ != expr.get_table_id())
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(WARN, "must call set_table() first. or table_id not identical with expr.table_id. table_id_=%lu, expr.table_id_=%lu",
        table_id_,  expr.get_table_id());
  }
  else if ((OB_SUCCESS == (ret = expr.get_decoded_expression().is_column_index_expr(is_cid)))  && (true == is_cid))
  {
    // 添加基本列
    if (OB_SUCCESS != (ret = scan_param_.add_column(expr.get_column_id(), true)))
    {
      TBSYS_LOG(WARN, "fail to add column to scan param. ret=%d, column_id=%lu", ret, expr.get_column_id());
    }
    else if (OB_SUCCESS != (ret = cur_row_desc_.add_column_desc(table_id_, expr.get_column_id())))
    {
      TBSYS_LOG(WARN, "fail to add column to scan param. ret=%d, tid_=%lu, cid=%lu", ret, table_id_, expr.get_column_id());
    }
  }
  else
  {
    // 添加复合列
    if (OB_SUCCESS != (ret = expr.get_decoded_expression().get_expression(obj_expr, size)))
    {
      TBSYS_LOG(WARN, "fail to get expression. ret=%d", ret);
    }
    else if ((size > 0) && (NULL != obj_expr))
    {
      if (OB_SUCCESS != (ret = scan_param_.add_column(obj_expr, true)))
      {
        TBSYS_LOG(WARN, "fail to add composite column to scan param. ret=%d", ret);
      }
      else if (OB_SUCCESS != (ret = cur_row_desc_.add_column_desc(table_id_, expr.get_column_id())))
      {
        TBSYS_LOG(WARN, "fail to add column to scan param. ret=%d, tid_=%lu, cid=%lu", ret, table_id_, expr.get_column_id());
      }
    }
    else
    {
      TBSYS_LOG(WARN, "fail to get expression. Unexpected expression size: size = %d", size);
      ret = OB_ERROR;
    }
  }
  return ret;
}



int ObRpcScan::set_table(const uint64_t table_id)
{
  int ret = OB_SUCCESS;
  ObRange range;
  ObString table_name;
  if (0 >= table_id)
  {
    TBSYS_LOG(WARN, "invalid table id: %lu", table_id);
    ret = OB_INVALID_ARGUMENT;
  }
  else
  {
    range.reset();
    range.table_id_ = table_id;
    table_id_ = table_id;
#if 0
    range.border_flag_.set_min_value();
    range.border_flag_.set_max_value();
#else
    /* FIXME: for debug */
    char * start_key = (char*)"row_100";
    char * end_key = (char*)"row_200";
    ObString key;
    range.border_flag_.unset_inclusive_start();
    range.border_flag_.set_inclusive_end();
    key.assign(start_key, static_cast<int32_t>(strlen(start_key)));
    range.start_key_ = key;
    key.assign(end_key, static_cast<int32_t>(strlen(end_key)));
    range.end_key_ = key;
    //scan_param.add_column(101);
    /* end for debug */
#endif
    ret = scan_param_.set(table_id, table_name, range, true);
  }
  return ret;
}


int ObRpcScan::add_filter(const ObSqlExpression& expr)
{
  int ret = OB_SUCCESS;
  const ObObj *obj_expr = NULL;
  int size = 0;
  // 添加复合列
  if (OB_SUCCESS != (ret = expr.get_decoded_expression().get_expression(obj_expr, size)))
  {
    TBSYS_LOG(WARN, "fail to get expression. ret=%d", ret);
  }
  else if ((size > 0) && (NULL != obj_expr))
  {
    if (OB_SUCCESS != (ret = scan_param_.add_column(obj_expr, true)))
    {
      TBSYS_LOG(WARN, "fail to add composite column to scan param. ret=%d", ret);
    }
  }
  else
  {
    TBSYS_LOG(WARN, "fail to get expression. Unexpected expression size: size = %d", size);
    ret= OB_ERROR;
  }
  return ret;
}


int ObRpcScan::set_limit(const int64_t limit, const int64_t offset)
{
  return scan_param_.set_limit_info(offset, limit);
}
