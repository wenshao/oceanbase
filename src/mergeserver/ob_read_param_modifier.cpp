/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * ob_read_param_modifier.cpp is for what ...
 *
 * Version: $id: ob_read_param_modifier.cpp,v 0.1 10/25/2010 6:23p wushi Exp $
 *
 * Authors:
 *   wushi <wushi.ly@taobao.com>
 *     - some work details if you want
 *
 */
#include "ob_read_param_modifier.h"
#include "common/ob_define.h"
#include "common/ob_range.h"

using namespace oceanbase::common;
using namespace oceanbase::mergeserver;

int oceanbase::mergeserver::get_next_param(const oceanbase::common::ObGetParam & org_param,
  const oceanbase::common::ObScanner & result,
  int64_t & got_cell, bool & finish,
  oceanbase::common::ObGetParam *get_param)
{
  ObReadParam *read_param = get_param;
  finish = false;
  int64_t item_count = 0;
  bool cur_req_finish = false;
  int64_t cell_size = org_param.get_cell_size();
  int ret = const_cast<ObScanner&>(result).get_is_req_fullfilled(cur_req_finish, item_count);
  if (ret != OB_SUCCESS)
  {
    TBSYS_LOG(WARN, "get is req fullfilled failed:ret[%d]", ret);
  }
  // check the result cell size must be equal with fullfilled item count for get
  else if (result.get_cell_num() != item_count)
  {
    TBSYS_LOG(WARN, "check result cell size not coincident with fullfill num:"
      "size[%ld], fullfill[%ld]", result.get_cell_num(), item_count);
    ret = OB_INNER_STAT_ERROR;
  }
  else
  {
    TBSYS_LOG(INFO, "get next param:finish[%d], total[%ld], org[%ld], cur_count[%ld], cur_finish[%d]",
      finish, got_cell, cell_size, item_count, cur_req_finish);
    got_cell += item_count;
    if (got_cell < cell_size)
    {
      finish = false;
    }
    else if ((got_cell == cell_size) && (true == cur_req_finish))
    {
      finish = true;
    }
    else
    {
      TBSYS_LOG(ERROR, "check cell size or finish status failed:total[%ld], org[%ld], "
        "cur_count[%ld], cur_finish[%d]", got_cell, cell_size, item_count, cur_req_finish);
      ret = OB_ERROR;
    }
  }

  if ((OB_SUCCESS == ret) && (false == finish) && (NULL != get_param))
  {
    get_param->reset();
    *read_param = org_param;
    const ObCellInfo * cell_info = NULL;
    int64_t count = got_cell;
    while (count < cell_size)
    {
      cell_info = org_param[count];
      if (NULL == cell_info)
      {
        ret = OB_ERROR;
        TBSYS_LOG(ERROR, "check get param cell failed:cell[%ld], item[%ld], index[%ld]",
          cell_size, item_count, count);
        break;
      }
      /// add the cell to new get param
      ret = get_param->add_cell(*cell_info);
      if (ret != OB_SUCCESS)
      {
        TBSYS_LOG(WARN, "add cell to new get param failed:cell[%ld], item[%ld], "
          "index[%ld], ret[%d]", cell_size, item_count, count, ret);
        if (OB_SIZE_OVERFLOW == ret)
        {
          if (NULL != get_param)
          {
            ret = get_param->rollback();
          }
          break;
        }
      }
      else
      {
        ++count;
      }
    }

    if (0 == get_param->get_cell_size())
    {
      ret = OB_ERROR;
      TBSYS_LOG(WARN, "check add any cell failed:cell[%ld], item[%ld], index[%ld], add[%ld]",
        cell_size, item_count, count, get_param->get_cell_size());
    }
  }
  return ret;
}

bool oceanbase::mergeserver::is_finish_scan(const ObScanParam::Direction scan_direction,  const ObRange & org_range,
  const ObRange & result_range)
{
  bool ret = false;
  if (ObScanParam::FORWARD == scan_direction)
  {
    if (result_range.compare_with_endkey2(org_range) >= 0)
    {
      ret = true;
    }
  }
  else
  {
    if (result_range.compare_with_startkey2(org_range) <= 0)
    {
      ret = true;
    }
  }
  return ret;
}

bool oceanbase::mergeserver::is_finish_scan(const ObScanParam & param, const ObRange & result_range)
{
  bool ret = false;
  if (ObScanParam::FORWARD == param.get_scan_direction())
  {
    if (result_range.compare_with_endkey2(*param.get_range()) >= 0)
    {
      ret = true;
    }
  }
  else
  {
    if (result_range.compare_with_startkey2(*param.get_range()) <= 0)
    {
      ret = true;
    }
  }
  return ret;
}


int oceanbase::mergeserver::get_next_param(const ObReadParam & org_read_param, 
  const ObMSGetCellArray &org_get_cells, 
  const int64_t got_cell_num, ObGetParam *get_param)
{
  int ret = OB_SUCCESS;
  int64_t surplus = org_get_cells.get_cell_size() - got_cell_num;
  int64_t cur_row_beg = got_cell_num;
  int64_t cur_row_end = got_cell_num;
  ObReadParam *read_param = get_param;
  if (NULL != get_param)
  {
    get_param->reset();
    *read_param = org_read_param;
  }
  if (surplus <= 0)
  {
    ret = OB_ITER_END;
  }
  if (OB_SUCCESS == ret)
  {
    while (cur_row_end < org_get_cells.get_cell_size() 
      && OB_SUCCESS == ret)
    {
      while (OB_SUCCESS == ret 
        && cur_row_end < org_get_cells.get_cell_size()
        && org_get_cells[cur_row_end].row_key_ == org_get_cells[cur_row_beg].row_key_
        && org_get_cells[cur_row_end].table_id_ == org_get_cells[cur_row_beg].table_id_)
      {
        if (NULL != get_param)
        {
          ret = get_param->add_cell(org_get_cells[cur_row_end]);
        }
        if (OB_SIZE_OVERFLOW == ret)
        {
          break; 
        }
        if (OB_SUCCESS == ret)
        {
          cur_row_end ++;
        }
      } 
      if (OB_SIZE_OVERFLOW == ret)
      {
        if (NULL != get_param)
        {
          ret = get_param->rollback();
        }
        break;
      }
      if (OB_SUCCESS == ret)
      {
        cur_row_beg = cur_row_end;
      }
    }
  }
  return ret;
}

namespace 
{
  int allocate_range_buffer(ObRange &range, ObMemBuffer &buffer)
  {
    int err = OB_SUCCESS;
    int64_t buf_len = range.start_key_.length() + range.end_key_.length();
    if (0 < buf_len)
    {
      if (NULL == buffer.malloc(buf_len))
      {
        TBSYS_LOG(WARN, "%s", "fail to allocate memory for range key");
        err = OB_ALLOCATE_MEMORY_FAILED;
      }
      else
      {
        memcpy(buffer.get_buffer(), range.start_key_.ptr(), 
          range.start_key_.length());
        range.start_key_.assign(reinterpret_cast<char*>(buffer.get_buffer()),
          range.start_key_.length());
        memcpy(reinterpret_cast<char*>(buffer.get_buffer()) + range.start_key_.length(), 
          range.end_key_.ptr(), range.end_key_.length());
        range.end_key_.assign(reinterpret_cast<char*>(buffer.get_buffer()) 
          + range.start_key_.length(),
          range.end_key_.length());
      }
    }
    return err;
  }
}

int oceanbase::mergeserver::get_next_param(const ObScanParam &org_scan_param, 
  const oceanbase::common::ObScanner &prev_scan_result,
  ObScanParam *scan_param, ObMemBuffer &range_buffer)
{
  int err = OB_SUCCESS;
  const ObReadParam &org_read_param = org_scan_param;
  ObReadParam *read_param = scan_param;
  ObRange tablet_range; 
  const ObRange *org_scan_range = NULL;
  ObRange cur_range;
  if (NULL != scan_param)
  {
    scan_param->reset();
    *read_param = org_read_param;
  }
  bool request_fullfilled = false;
  int64_t fullfilled_row_num = 0;
  err = prev_scan_result.get_is_req_fullfilled(request_fullfilled,fullfilled_row_num);
  if (OB_SUCCESS == err)
  {
    org_scan_range = org_scan_param.get_range();
    if (NULL == org_scan_range)
    {
      TBSYS_LOG(WARN, "%s", "unexpected error, can't get range from org scan param");
      err  = OB_INVALID_ARGUMENT;
    }
  }

  ObString last_row_key;
  if (OB_SUCCESS == err)
  {
    if (request_fullfilled)
    {
      err = prev_scan_result.get_range(tablet_range);
      if (OB_SUCCESS == err)
      {
        if (true == is_finish_scan(org_scan_param.get_scan_direction(), *org_scan_param.get_range(), tablet_range))
        {
          err = OB_ITER_END;
        }
        else if (ObScanParam::FORWARD == org_scan_param.get_scan_direction())
        {
          last_row_key = tablet_range.end_key_;
        }
        else
        {
          last_row_key = tablet_range.start_key_;
        }
      }
    }
    else
    {
      err = prev_scan_result.get_last_row_key(last_row_key);
      if (OB_ENTRY_NOT_EXIST == err)
      {
        /// first time, remain unchanged
      }
      else if (OB_SUCCESS != err)
      {
        TBSYS_LOG(WARN, "%s", "unexpected error, scanner should contain at least one cell");
      }
    }
  }

  if (OB_SUCCESS == err && NULL != scan_param)
  {
    cur_range =*org_scan_range;
    // forward
    if (ObScanParam::FORWARD == org_scan_param.get_scan_direction())
    {
      cur_range.start_key_ = last_row_key;
      cur_range.border_flag_.unset_min_value();
      cur_range.border_flag_.unset_inclusive_start();
    }
    else
    {
      cur_range.end_key_ = last_row_key;
      cur_range.border_flag_.unset_max_value();
      if (request_fullfilled)
      {
        // tablet range start key
        cur_range.border_flag_.set_inclusive_end();
      }
      else
      {
        // the smallest rowkey
        cur_range.border_flag_.unset_inclusive_end();
      }
    }
  }

  // first time remain unchanged
  if (OB_ENTRY_NOT_EXIST == err)
  {
    cur_range =*org_scan_range;
    err = OB_SUCCESS;
  }

  if (OB_SUCCESS == err && NULL != scan_param)
  {
    err = allocate_range_buffer(cur_range, range_buffer);
  }

  if (OB_SUCCESS == err && NULL != scan_param)
  {
    scan_param->set(org_scan_param.get_table_id(), org_scan_param.get_table_name(), cur_range);
    for (int32_t cell_idx = 0; 
      cell_idx < org_scan_param.get_column_id_size() && OB_SUCCESS == err;
      cell_idx ++)
    {
      err = scan_param->add_column(org_scan_param.get_column_id()[cell_idx]);
    }
    if (OB_SUCCESS == err)
    {
      scan_param->set_scan_flag(org_scan_param.get_scan_flag());
    }
  }
  return err;
}

namespace
{
  int get_ups_read_param(ObReadParam & param,  const ObScanner & cs_result)
  {
    int err = OB_SUCCESS;
    ObVersionRange org_version_range;
    ObVersionRange cur_version_range;
    int64_t cs_version = 0;
    org_version_range = param.get_version_range();
    cur_version_range = org_version_range;

    cs_version = cs_result.get_data_version();
    if (OB_SUCCESS == err)
    {
      if (cur_version_range.border_flag_.is_max_value() 
        || org_version_range.end_version_ > cs_version)
      {
        cur_version_range.border_flag_.unset_min_value();
        cur_version_range.border_flag_.set_inclusive_start();
        cur_version_range.start_version_ = cs_version + 1;
      }
      else if (org_version_range.end_version_ <= cs_version)
      {
        TBSYS_LOG(DEBUG, "%s", "chunk server return all data needed");
        err = OB_ITER_END;
      }
    }
    if (OB_SUCCESS == err)
    {
      param.set_version_range(cur_version_range);
    }
    return err;
  }
}

int oceanbase::mergeserver::get_ups_param(oceanbase::common::ObScanParam & param, 
  const oceanbase::common::ObScanner & cs_result,
  ObMemBuffer &range_buffer)
{
  int err = OB_SUCCESS;
  bool is_fullfill = false;
  int64_t fullfilled_row_num = 0;
  ObReadParam &read_param = param;
  ObRange cs_range;
  ObString cs_max_rowkey;
  bool use_max_rowkey = false;
  err =  get_ups_read_param(read_param,cs_result);
  if (OB_SUCCESS == err)
  {
    err = cs_result.get_is_req_fullfilled(is_fullfill,fullfilled_row_num);
  }

  if (OB_SUCCESS == err )
  {
    if (is_fullfill)
    {
      err = cs_result.get_range(cs_range);
      if (OB_SUCCESS == err)
      {
        /// need change the new scan param range rowkey
        if (false == is_finish_scan(param.get_scan_direction(), *param.get_range(), cs_range))
        {
          use_max_rowkey = true;
          if (ObScanParam::FORWARD == param.get_scan_direction())
          {
            cs_max_rowkey = cs_range.end_key_;
          }
          else
          {
            // the smallest rowkey
            cs_max_rowkey = cs_range.start_key_;
          }
        }
      }
    }
    else
    {
      err = cs_result.get_last_row_key(cs_max_rowkey);
      if (OB_SUCCESS != err)
      {
        TBSYS_LOG(WARN, "%s", "unexpected error, there should be at least one cell in the scanner");
      }
      use_max_rowkey = true;
    }
  }

  if (OB_SUCCESS == err && use_max_rowkey)
  {
    ObRange new_range = *param.get_range();
    if (ObScanParam::FORWARD == param.get_scan_direction())
    {
      new_range.end_key_ = cs_max_rowkey;
      new_range.border_flag_.unset_max_value();
      new_range.border_flag_.set_inclusive_end();
    }
    else
    {
      // the smallest row key
      new_range.start_key_ = cs_max_rowkey;
      new_range.border_flag_.unset_min_value();
      if (is_fullfill)
      {
        // not inclusive the tablet range start key
        new_range.border_flag_.unset_inclusive_start();
      }
      else
      {
        new_range.border_flag_.set_inclusive_start();
      }
    }

    err = allocate_range_buffer(new_range,range_buffer);
    if (OB_SUCCESS == err)
    {
      param.set(param.get_table_id(),param.get_table_name(),new_range);
    }
  }
  return err;
}

int oceanbase::mergeserver::get_ups_param(ObGetParam & param, const ObScanner & cs_result)
{
  int err = OB_SUCCESS;
  ObReadParam &read_param = param;
  err =  get_ups_read_param(read_param,cs_result); 
  if (OB_SUCCESS == err)
  {
    bool is_fullfilled = false;
    int64_t fullfilled_cell_num  = 0;
    err = cs_result.get_is_req_fullfilled(is_fullfilled, fullfilled_cell_num);
    if (OB_SUCCESS == err && fullfilled_cell_num < param.get_cell_size())
    {
      err = param.rollback(param.get_cell_size() - fullfilled_cell_num);
      if ((err != OB_SUCCESS) || (param.get_cell_size() != fullfilled_cell_num))
      {
        TBSYS_LOG(WARN, "check param rollback failed:full_fill[%ld], param_size[%ld]",
          fullfilled_cell_num, param.get_cell_size());
      }
    }
  }
  return err;
}



int get_next_range_for_trivail_scan(const ObRange &org_scan_range,
  const ObScanner &prev_scan_result,
  const ObScanParam::Direction scan_direction,
  ObRange &cur_range)
{
  int err = OB_SUCCESS;
  ObRange tablet_range;
  bool request_fullfilled = false;
  int64_t fullfilled_row_num = 0;
  err = prev_scan_result.get_is_req_fullfilled(request_fullfilled,fullfilled_row_num);

  ObString last_row_key;
  if (OB_SUCCESS == err)
  {
    if (request_fullfilled)
    {
      err = prev_scan_result.get_range(tablet_range);
      if (OB_SUCCESS == err)
      {
        if (true == is_finish_scan(scan_direction, org_scan_range, tablet_range))
        {
          err = OB_ITER_END; 
        }
        else if (ObScanParam::FORWARD == scan_direction)
        {
          last_row_key = tablet_range.end_key_;
        }
        else
        {
          last_row_key = tablet_range.start_key_;
        }
      }
    }
    else
    {
      if (OB_SUCCESS != (err = prev_scan_result.get_last_row_key(last_row_key)))
      {
        TBSYS_LOG(WARN,"fail to get last rowkey from prev result [err:%d]", err);
      }
    }
  }

  if (OB_SUCCESS == err)
  {
    cur_range = org_scan_range;
    // forward
    if (ObScanParam::FORWARD == scan_direction)
    {
      cur_range.start_key_ = last_row_key;
      cur_range.border_flag_.unset_min_value();
      cur_range.border_flag_.unset_inclusive_start();
    }
    else
    {
      cur_range.end_key_ = last_row_key;
      cur_range.border_flag_.unset_max_value();
      if (request_fullfilled)
      {
        // tablet range start key
        cur_range.border_flag_.set_inclusive_end();
      }
      else
      {
        // the smallest rowkey
        cur_range.border_flag_.unset_inclusive_end();
      }
    }
  }
  return err;
}

int oceanbase::mergeserver::get_next_range(const oceanbase::common::ObScanParam &org_scan_param,
  const ObScanner &prev_scan_result,
  const int64_t prev_limit_offset,
  ObRange &cur_range,
  int64_t & cur_limit_offset, 
  ObStringBuf &buf)
{
  int err = OB_SUCCESS;
  cur_limit_offset = 0;
  cur_range.reset();
  if ((OB_SUCCESS == err) 
    && (org_scan_param.get_group_by_param().get_aggregate_row_width() > 0 ) 
    && (org_scan_param.get_orderby_column_size() <= 0))
  {
    TBSYS_LOG(WARN,"argument error [org_scan_param.get_group_by_param().get_aggregate_row_width():%ld,"
      "org_scan_param.get_orderby_column_size():%ld]", org_scan_param.get_group_by_param().get_aggregate_row_width(), 
      org_scan_param.get_orderby_column_size());
    err = OB_INVALID_ARGUMENT;
  }
  if ((OB_SUCCESS == err)
    && (org_scan_param.get_scan_direction() == ObScanParam::BACKWARD)
    && ((org_scan_param.get_orderby_column_size() > 0) || (org_scan_param.get_group_by_param().get_aggregate_row_width() > 0)))
  {
    TBSYS_LOG(WARN,"argument error, backward scan are limited only for backward page browsing, "
      "it doesn't support groupby or orderby [org_scan_param.get_group_by_param().get_aggregate_row_width():%ld,"
      "org_scan_param.get_orderby_column_size():%ld]", org_scan_param.get_group_by_param().get_aggregate_row_width(), 
      org_scan_param.get_orderby_column_size());
    err = OB_INVALID_ARGUMENT;
  }
  ObRange tablet_range;
  if ((OB_SUCCESS == err) && (OB_SUCCESS != (err = prev_scan_result.get_range(tablet_range))))
  {
    TBSYS_LOG(WARN,"fail to get tablet range from prev result [err:%d]", err);
  }
  if ((OB_SUCCESS == err)  && (0 < prev_limit_offset))
  {
    /// check tablet split
    if ((tablet_range.compare_with_startkey2(*org_scan_param.get_range()) > 0)
      || (tablet_range.compare_with_endkey2(*org_scan_param.get_range()) < 0))
    {
      TBSYS_LOG(WARN,"cs tablet splitted during the whole request, please try this request again");
      err = OB_NEED_RETRY;
    }
  }

  if ((OB_SUCCESS == err) && (org_scan_param.get_orderby_column_size() <= 0))
  {
    err = get_next_range_for_trivail_scan(*org_scan_param.get_range(),prev_scan_result,
      org_scan_param.get_scan_direction(),cur_range);
    if ((OB_SUCCESS != err) && (OB_ITER_END != err))
    {
      TBSYS_LOG(WARN,"fail to get next range [err:%d]", err);
    }
  }
  else if ((OB_SUCCESS == err) && (org_scan_param.get_orderby_column_size() > 0))
  {
    int64_t fullfilled_item_num = 0;
    bool is_fullfill = false;
    if (OB_SUCCESS != (err = prev_scan_result.get_is_req_fullfilled(is_fullfill,fullfilled_item_num)))
    {
      TBSYS_LOG(WARN,"fail to get fullfill info from prev result [err:%d]", err);
    }
    if ((OB_SUCCESS == err) && (fullfilled_item_num != prev_scan_result.get_row_num()))
    {
      /// TBSYS_LOG(ERROR,"unexpected error [prev_result::fullfilled_item_num:%ld, prev_result::get_row_num():%ld]",
      ///   fullfilled_item_num, prev_scan_result.get_row_num());
      /// err = OB_ERR_UNEXPECTED;
      fullfilled_item_num = prev_scan_result.get_row_num();
    }
    if ((OB_SUCCESS == err) && is_fullfill)
    {
      if (is_finish_scan(org_scan_param.get_scan_direction(),*org_scan_param.get_range(),tablet_range))
      {
        err = OB_ITER_END;
      }
      else
      {
        cur_range = *org_scan_param.get_range();
        cur_range.start_key_ = tablet_range.end_key_;
        cur_range.border_flag_.unset_min_value();
        cur_range.border_flag_.unset_inclusive_start();
      }
    }
    if ((OB_SUCCESS == err) && !is_fullfill)
    {
      cur_range = *org_scan_param.get_range();
      cur_limit_offset = prev_limit_offset + fullfilled_item_num;
    }
  }
  if (OB_SUCCESS == err)
  {
    ObString str;
    str = cur_range.start_key_;
    if ((OB_SUCCESS == err) && (str.length() > 0))
    {
      if (OB_SUCCESS != (err = buf.write_string(str,&(cur_range.start_key_))))
      {
        TBSYS_LOG(WARN,"fail to deep copy cur_range.start_key_ [err:%d]", err);
      }
    }
    str = cur_range.end_key_;
    if ((OB_SUCCESS == err) && (str.length() > 0))
    {
      if (OB_SUCCESS != (err = buf.write_string(str,&(cur_range.end_key_))))
      {
        TBSYS_LOG(WARN,"fail to deep copy cur_range.end_key_ [err:%d]", err);
      }
    }
  }
  return err;
}


int get_next_range_for_trivail_scan(const ObRange &org_scan_range,
  const ObNewScanner &prev_scan_result,
  const ObScanParam::Direction scan_direction,
  ObRange &cur_range)
{
  int err = OB_SUCCESS;
  ObRange tablet_range;
  bool request_fullfilled = false;
  int64_t fullfilled_row_num = 0;
  err = prev_scan_result.get_is_req_fullfilled(request_fullfilled,fullfilled_row_num);

  ObString last_row_key;
  if (OB_SUCCESS == err)
  {
    if (request_fullfilled)
    {
      err = prev_scan_result.get_range(tablet_range);
      if (OB_SUCCESS == err)
      {
        if (true == is_finish_scan(scan_direction, org_scan_range, tablet_range))
        {
          err = OB_ITER_END; 
        }
        else if (ObScanParam::FORWARD == scan_direction)
        {
          last_row_key = tablet_range.end_key_;
        }
        else
        {
          last_row_key = tablet_range.start_key_;
        }
      }
    }
    else
    {
      if (OB_SUCCESS != (err = prev_scan_result.get_last_row_key(last_row_key)))
      {
        TBSYS_LOG(WARN,"fail to get last rowkey from prev result [err:%d]", err);
      }
    }
  }

  if (OB_SUCCESS == err)
  {
    cur_range = org_scan_range;
    // forward
    if (ObScanParam::FORWARD == scan_direction)
    {
      cur_range.start_key_ = last_row_key;
      cur_range.border_flag_.unset_min_value();
      cur_range.border_flag_.unset_inclusive_start();
    }
    else
    {
      cur_range.end_key_ = last_row_key;
      cur_range.border_flag_.unset_max_value();
      if (request_fullfilled)
      {
        // tablet range start key
        cur_range.border_flag_.set_inclusive_end();
      }
      else
      {
        // the smallest rowkey
        cur_range.border_flag_.unset_inclusive_end();
      }
    }
  }
  return err;
}


int oceanbase::mergeserver::get_next_range(const oceanbase::common::ObScanParam &org_scan_param,
  const ObNewScanner &prev_scan_result,
  const int64_t prev_limit_offset,
  ObRange &cur_range,
  int64_t & cur_limit_offset, 
  ObStringBuf &buf)
{
  int err = OB_SUCCESS;
  cur_limit_offset = 0;
  cur_range.reset();
  if ((OB_SUCCESS == err) 
    && (org_scan_param.get_group_by_param().get_aggregate_row_width() > 0 ) 
    && (org_scan_param.get_orderby_column_size() <= 0))
  {
    TBSYS_LOG(WARN,"argument error [org_scan_param.get_group_by_param().get_aggregate_row_width():%ld,"
      "org_scan_param.get_orderby_column_size():%ld]", org_scan_param.get_group_by_param().get_aggregate_row_width(), 
      org_scan_param.get_orderby_column_size());
    err = OB_INVALID_ARGUMENT;
  }
  if ((OB_SUCCESS == err)
    && (org_scan_param.get_scan_direction() == ObScanParam::BACKWARD)
    && ((org_scan_param.get_orderby_column_size() > 0) || (org_scan_param.get_group_by_param().get_aggregate_row_width() > 0)))
  {
    TBSYS_LOG(WARN,"argument error, backward scan are limited only for backward page browsing, "
      "it doesn't support groupby or orderby [org_scan_param.get_group_by_param().get_aggregate_row_width():%ld,"
      "org_scan_param.get_orderby_column_size():%ld]", org_scan_param.get_group_by_param().get_aggregate_row_width(), 
      org_scan_param.get_orderby_column_size());
    err = OB_INVALID_ARGUMENT;
  }
  ObRange tablet_range;
  if ((OB_SUCCESS == err) && (OB_SUCCESS != (err = prev_scan_result.get_range(tablet_range))))
  {
    TBSYS_LOG(WARN,"fail to get tablet range from prev result [err:%d]", err);
  }
  if ((OB_SUCCESS == err)  && (0 < prev_limit_offset))
  {
    /// check tablet split
    if ((tablet_range.compare_with_startkey2(*org_scan_param.get_range()) > 0)
      || (tablet_range.compare_with_endkey2(*org_scan_param.get_range()) < 0))
    {
      TBSYS_LOG(WARN,"cs tablet splitted during the whole request, please try this request again");
      err = OB_NEED_RETRY;
    }
  }

  if ((OB_SUCCESS == err) && (org_scan_param.get_orderby_column_size() <= 0))
  {
    err = get_next_range_for_trivail_scan(*org_scan_param.get_range(),prev_scan_result,
      org_scan_param.get_scan_direction(),cur_range);
    if ((OB_SUCCESS != err) && (OB_ITER_END != err))
    {
      TBSYS_LOG(WARN,"fail to get next range [err:%d]", err);
    }
  }
  else if ((OB_SUCCESS == err) && (org_scan_param.get_orderby_column_size() > 0))
  {
    int64_t fullfilled_item_num = 0;
    bool is_fullfill = false;
    if (OB_SUCCESS != (err = prev_scan_result.get_is_req_fullfilled(is_fullfill,fullfilled_item_num)))
    {
      TBSYS_LOG(WARN,"fail to get fullfill info from prev result [err:%d]", err);
    }
    if ((OB_SUCCESS == err) && (fullfilled_item_num != prev_scan_result.get_row_num()))
    {
      /// TBSYS_LOG(ERROR,"unexpected error [prev_result::fullfilled_item_num:%ld, prev_result::get_row_num():%ld]",
      ///   fullfilled_item_num, prev_scan_result.get_row_num());
      /// err = OB_ERR_UNEXPECTED;
      fullfilled_item_num = prev_scan_result.get_row_num();
    }
    if ((OB_SUCCESS == err) && is_fullfill)
    {
      if (is_finish_scan(org_scan_param.get_scan_direction(),*org_scan_param.get_range(),tablet_range))
      {
        err = OB_ITER_END;
      }
      else
      {
        cur_range = *org_scan_param.get_range();
        cur_range.start_key_ = tablet_range.end_key_;
        cur_range.border_flag_.unset_min_value();
        cur_range.border_flag_.unset_inclusive_start();
      }
    }
    if ((OB_SUCCESS == err) && !is_fullfill)
    {
      cur_range = *org_scan_param.get_range();
      cur_limit_offset = prev_limit_offset + fullfilled_item_num;
    }
  }
  if (OB_SUCCESS == err)
  {
    ObString str;
    str = cur_range.start_key_;
    if ((OB_SUCCESS == err) && (str.length() > 0))
    {
      if (OB_SUCCESS != (err = buf.write_string(str,&(cur_range.start_key_))))
      {
        TBSYS_LOG(WARN,"fail to deep copy cur_range.start_key_ [err:%d]", err);
      }
    }
    str = cur_range.end_key_;
    if ((OB_SUCCESS == err) && (str.length() > 0))
    {
      if (OB_SUCCESS != (err = buf.write_string(str,&(cur_range.end_key_))))
      {
        TBSYS_LOG(WARN,"fail to deep copy cur_range.end_key_ [err:%d]", err);
      }
    }
  }
  return err;
}
