/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or 
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_merger_operator.cpp for 
 *
 * Authors:
 *   wushi <wushi.ly@taobao.com>
 *
 */
#include "ob_ms_sql_operator.h"
#include "common/ob_scan_param.h"
using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::mergeserver;
oceanbase::mergeserver::ObMsSqlOperator::ObMsSqlOperator()
{
  scan_param_ = NULL;
  status_ = NOT_INIT;
  last_sharding_res_ = NULL;
  sharding_res_count_ = 0;
}

oceanbase::mergeserver::ObMsSqlOperator::~ObMsSqlOperator()
{
  reset();
  status_ = NOT_INIT;
}



void oceanbase::mergeserver::ObMsSqlOperator::reset()
{
  scan_param_ = NULL;
  cells_.reset();
  sorted_operator_.reset();
  last_sharding_res_ = NULL;
  sharding_res_count_ = 0;
}

int oceanbase::mergeserver::ObMsSqlOperator::set_param(const ObScanParam & scan_param)
{
  int err = OB_SUCCESS;
  reset();
  scan_param_ = &scan_param;
  if ((scan_param_->get_group_by_param().get_aggregate_row_width() == 0)
    && (scan_param_->get_orderby_column_size() == 0))
  {
    status_ = USE_SORTED_OPERATOR;
    if (OB_SUCCESS != (err = sorted_operator_.set_param(*scan_param_)))
    {
      TBSYS_LOG(WARN,"fail to set scan param to sorted_operator_ [err:%d]", err);
    }
  }
  if (OB_SUCCESS != err)
  {
    reset();
  }
  return err;
}


int oceanbase::mergeserver::ObMsSqlOperator::add_sharding_result(ObNewScanner & sharding_res, 
  const ObRange & query_range, const int64_t limit_offset, bool &is_finish, bool &can_free_res)
{
  int err = OB_SUCCESS;
  UNUSED(limit_offset);
  can_free_res = false;
  if (OB_NOT_INIT == status_)
  {
    TBSYS_LOG(WARN,"please set param first");
    err = OB_INVALID_ARGUMENT;
  }
  if (OB_SUCCESS == err)
  {
    switch (status_)
    {
    case USE_SORTED_OPERATOR:
      err = sorted_operator_.add_sharding_result(sharding_res,query_range,is_finish);
      break;
   default:
      TBSYS_LOG(ERROR, "status error [status_:%d]", status_);
      err = OB_ERR_UNEXPECTED;
    }
    if (OB_SUCCESS != err)
    {
      TBSYS_LOG(WARN,"fail to add sharding result [err:%d,status_:%d]", err, status_);
    }
  }
  if (OB_SUCCESS == err)
  {
    last_sharding_res_ = &sharding_res;
    sharding_res_count_ ++;
  }
  return err;
}

int oceanbase::mergeserver::ObMsSqlOperator::get_mem_size_used()const
{
  int64_t res = 0;
  switch (status_)
  {
  case USE_SORTED_OPERATOR:
    res = sorted_operator_.get_mem_size_used();
    break;
  default:
    TBSYS_LOG(WARN, "status error [status_:%d]", status_);
  }
  return static_cast<int32_t>(res);
}

int64_t oceanbase::mergeserver::ObMsSqlOperator::get_result_row_width()const
{
  int64_t res = 0;
  if (NULL != scan_param_)
  {
    res = (scan_param_->get_group_by_param().get_aggregate_row_width() > 0) ?
      (scan_param_->get_group_by_param().get_aggregate_row_width() ) : 
      (scan_param_->get_column_id_size() + scan_param_->get_composite_columns_size());
  }
  return res;
}


//////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////

int64_t oceanbase::mergeserver::ObMsSqlOperator::get_sharding_result_count() const
{
  int64_t res = 0;   
  if(USE_SORTED_OPERATOR == status_)
  { 
    res = sorted_operator_.get_sharding_result_count();      
  }
  return (res);

}

int64_t oceanbase::mergeserver::ObMsSqlOperator::get_cur_sharding_result_idx() const
{
  int64_t res = 0;   
  if(USE_SORTED_OPERATOR == status_)
  { 
    res = sorted_operator_.get_cur_sharding_result_idx();      
  }
  return (res);
}

int oceanbase::mergeserver::ObMsSqlOperator::get_next_row(oceanbase::common::ObRow &row)
{
  int err = OB_SUCCESS;

  // xiaochu.yh 2012-6-18
  if (NULL == scan_param_)
  {
    TBSYS_LOG(WARN,"please set request param first");
    err = OB_INVALID_ARGUMENT;
  }
  if (OB_SUCCESS == err)
  {
    if (OB_SUCCESS == (err = sorted_operator_.get_next_row(row)))
    {
    }
    else if (OB_ITER_END == err)
    {
    }
    else
    {
      TBSYS_LOG(WARN,"fail to get next_cell from result [err:%d,status_:%d]", err, status_);
    }
  }
  return err;
}

