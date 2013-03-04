/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or 
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_merger_sorted_operator.cpp for 
 *
 * Authors:
 *   wushi <wushi.ly@taobao.com>
 *
 */
//#include "ob_merger_sorted_operator.h"
#include "ob_ms_sql_sorted_operator.h"
#include <algorithm>
#include "common/ob_scan_param.h"
#include "common/ob_new_scanner.h"
#include "common/ob_range.h"
using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::mergeserver;
using namespace std;

void oceanbase::mergeserver::ObMsSqlSortedOperator::sharding_result_t::init(ObNewScanner & sharding_res, const ObRange & query_range, 
  const ObScanParam &param, ObString & last_process_rowkey, const int64_t fullfilled_item_num)
{
  sharding_res_ = &sharding_res;
  sharding_query_range_ = &query_range;
  param_ = &param;
  last_row_key_ = last_process_rowkey;
  fullfilled_item_num_ = fullfilled_item_num;
}

bool oceanbase::mergeserver::ObMsSqlSortedOperator::sharding_result_t::operator <(const sharding_result_t &other) const
{
  bool res = false;
  if (param_->get_scan_direction() == ObScanParam::BACKWARD)
  {
    res = (sharding_query_range_->compare_with_endkey2(*other.sharding_query_range_) > 0);
  }
  else
  {
    res = (sharding_query_range_->compare_with_startkey2(*other.sharding_query_range_) < 0);
  }
  return res;
}

oceanbase::mergeserver::ObMsSqlSortedOperator::ObMsSqlSortedOperator()
{
  reset();
}


oceanbase::mergeserver::ObMsSqlSortedOperator::~ObMsSqlSortedOperator()
{
}

void oceanbase::mergeserver::ObMsSqlSortedOperator::reset()
{
  sharding_result_count_ = 0;
  cur_sharding_result_idx_ = -1;
  scan_param_ = NULL;
  seamless_result_count_  = 0;
}

int oceanbase::mergeserver::ObMsSqlSortedOperator::set_param(const ObScanParam & scan_param)
{
  int err = OB_SUCCESS;
  reset();
  scan_param_ = &scan_param;
  scan_range_ = *scan_param_->get_range();
  return err;
}

void oceanbase::mergeserver::ObMsSqlSortedOperator::sort(bool &is_finish, ObNewScanner * last_sharding_res)
{
  int64_t result_size = 0;
  bool seamless = true;
  int64_t seamless_result_idx = 0;
  int64_t seamless_row_count = 0;
  std::sort(sharding_result_arr_, sharding_result_arr_ + sharding_result_count_);
  if ((ObScanParam::FORWARD == scan_param_->get_scan_direction())
    && (sharding_result_arr_[0].sharding_query_range_->start_key_  != scan_range_.start_key_)
    && (!sharding_result_arr_[0].sharding_query_range_->border_flag_.is_min_value()))
  {
    seamless = false;
  }
  else if ((ObScanParam::BACKWARD == scan_param_->get_scan_direction())
    && (sharding_result_arr_[0].sharding_query_range_->end_key_  != scan_range_.end_key_)
    && (!sharding_result_arr_[0].sharding_query_range_->border_flag_.is_max_value()))
  {
    seamless = false;
  }
  int64_t limit_offset = 0;
  int64_t limit_count = 0;
  scan_param_->get_limit_info(limit_offset, limit_count);
  if (seamless)
  {
    seamless_result_count_  = 1;
    seamless_row_count += sharding_result_arr_[0].fullfilled_item_num_;
    if (seamless_row_count > limit_offset)
    {
      result_size += sharding_result_arr_[0].sharding_res_->get_serialize_size();
    }
  }
  for (seamless_result_idx = 0; 
    (seamless_result_idx < sharding_result_count_ - 1) && (seamless); 
    seamless_result_idx++)
  {
    if ((ObScanParam::FORWARD == scan_param_->get_scan_direction())
      && (sharding_result_arr_[seamless_result_idx].last_row_key_
      != sharding_result_arr_[seamless_result_idx+1].sharding_query_range_->start_key_))
    {
      seamless = false;
    }
    else if ((ObScanParam::BACKWARD == scan_param_->get_scan_direction())
      && (sharding_result_arr_[seamless_result_idx].last_row_key_
      != sharding_result_arr_[seamless_result_idx+1].sharding_query_range_->end_key_))
    {
      seamless = false;
    }
    if (seamless)
    {
      seamless_result_count_ ++;
      seamless_row_count += sharding_result_arr_[seamless_result_idx + 1].fullfilled_item_num_;
      if (seamless_row_count > limit_offset)
      {
        result_size += sharding_result_arr_[seamless_result_idx + 1].sharding_res_->get_serialize_size();
      }
    }
  }

  if ((limit_count > 0) || (limit_offset > 0))
  {
    is_finish = (seamless_row_count >= limit_offset + limit_count);
  }
  else
  {
    bool is_fullfilled = true;
    int64_t item_num = 0;
    if ((NULL != last_sharding_res) && (OB_SUCCESS != (last_sharding_res->get_is_req_fullfilled(is_fullfilled,item_num))))
    {
      is_fullfilled = true;
    }
    is_finish = (result_size >= OB_MAX_PACKET_LENGTH - FULL_SCANNER_RESERVED_BYTE_COUNT) ||
      ((sharding_result_arr_[seamless_result_count_-1].sharding_res_ == last_sharding_res) && !is_fullfilled);
  }
}

int oceanbase::mergeserver::ObMsSqlSortedOperator::add_sharding_result(ObNewScanner & sharding_res, const ObRange &query_range, 
  bool &is_finish)
{
  int err = OB_SUCCESS;
  if ((OB_SUCCESS == err) && (NULL == scan_param_))
  {
    TBSYS_LOG(WARN,"operator was not initialized yet [scan_param_:%p]", scan_param_);
    err = OB_INVALID_ARGUMENT;
  }
  if ((OB_SUCCESS == err) && (sharding_result_count_ >= MAX_SHARDING_RESULT_COUNT))
  {
    TBSYS_LOG(WARN,"array is full [MAX_SHARDING_RESULT_COUNT:%ld,sharding_result_count_:%ld]", 
      MAX_SHARDING_RESULT_COUNT, sharding_result_count_);
    err = OB_ARRAY_OUT_OF_RANGE;
  }
  /// @todo (wushi wushi.ly@taobao.com) check fullfill item number and size property
  bool is_fullfilled = false;
  int64_t fullfilled_item_num = 0;
  ObString last_row_key;
  ObRange cs_tablet_range;
  if ((OB_SUCCESS == err) && (OB_SUCCESS != (err = sharding_res.get_is_req_fullfilled(is_fullfilled,fullfilled_item_num))))
  {
    TBSYS_LOG(WARN,"fail to get fullfilled info from sharding result [err:%d]", err);
  }
  if (OB_SUCCESS == err)
  {
    if (OB_SUCCESS == (err = sharding_res.get_last_row_key(last_row_key)))
    {
    }
    else if (OB_ENTRY_NOT_EXIST == err)
    {
      err = OB_SUCCESS;
    }
    else
    {
      TBSYS_LOG(WARN,"fail to get last rowkey from sharding result [err:%d]", err);
    }
  }


  if (OB_SUCCESS == err)
  {
    if (is_fullfilled)
    {
      if ((OB_SUCCESS == err) && (OB_SUCCESS != (err = sharding_res.get_range(cs_tablet_range))))
      {
        TBSYS_LOG(WARN,"fail to get tablet range from sharding result [err:%d]", err);
      }
      if (OB_SUCCESS == err)
      {
        if (ObScanParam::FORWARD == scan_param_->get_scan_direction())
        {
          last_row_key = cs_tablet_range.end_key_;
        }
        else
        {
          last_row_key = cs_tablet_range.start_key_;
        }
      }
    }
    if (OB_SUCCESS == err)
    {
      fullfilled_item_num = sharding_res.get_row_num();
      sharding_result_arr_[sharding_result_count_].init(sharding_res,query_range,*scan_param_, last_row_key, fullfilled_item_num);
      sharding_result_count_ ++;
    }
  }
  is_finish = false;
  if (OB_SUCCESS == err)
  {
    sort(is_finish, &sharding_res);
  }
  return err;
}


///////////////////////////////////////
//////////// Row inferface ////////////
///////////////////////////////////////

int oceanbase::mergeserver::ObMsSqlSortedOperator::get_next_row(oceanbase::common::ObRow &row)
{
  int err = OB_SUCCESS;
  if ((OB_SUCCESS == err) && (cur_sharding_result_idx_ < 0))
  {
    cur_sharding_result_idx_ = 0;
  }
  while (OB_SUCCESS == err)
  {
    if (cur_sharding_result_idx_ >= seamless_result_count_)
    {
      err = OB_ITER_END;
    }
    if (OB_SUCCESS == err)
    {
      if (OB_SUCCESS ==(err = sharding_result_arr_[cur_sharding_result_idx_].sharding_res_->get_next_row(row)))
      {
        break;
      }
      else if (OB_ITER_END == err)
      {
        err = OB_SUCCESS;
        cur_sharding_result_idx_ ++;
      }
      else
      {
        TBSYS_LOG(WARN,"fail to get next cell from ObNewScanner [idx:%ld,err:%d]", cur_sharding_result_idx_, err);
      }
    }
  }
  return err;
}


