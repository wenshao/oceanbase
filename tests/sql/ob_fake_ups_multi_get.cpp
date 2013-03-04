/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_fake_ups_multi_get.cpp
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@taobao.com>
 *
 */

#include "ob_fake_ups_multi_get.h"

using namespace oceanbase;
using namespace sql;
using namespace test;
using namespace common;

ObFakeUpsMultiGet::ObFakeUpsMultiGet(const char *file_name)
  :ups_scan_(file_name)
{
}

void ObFakeUpsMultiGet::reset()
{
  ObUpsMultiGet::reset();
  ups_scan_.reset();
}

int ObFakeUpsMultiGet::open()
{
  int ret = OB_SUCCESS;

  if(NULL == get_param_)
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "get param is null");
    return ret;
  }

  if(NULL == curr_row_.get_row_desc())
  {
    TBSYS_LOG(WARN, "set row desc first");
    return OB_ERROR;
  }

  row_key_array_.clear();
  int64_t column_num = curr_row_.get_column_num();

  ObString row_key;
  for(int i=0;OB_SUCCESS == ret && i<get_param_->get_cell_size();i+=column_num)
  {
    for(int j=0;OB_SUCCESS == ret && j<column_num;j++)
    {
      ObCellInfo *cell = (*get_param_)[i + j];
      if(0 != j)
      {
        if(row_key != cell->row_key_)
        {
          ret = OB_ERROR;
        }
      }
      else
      {
        row_key = cell->row_key_;
        row_key_array_.push_back(row_key);
      }
    }
  }
  
  if(OB_SUCCESS == ret)
  {
    if(OB_SUCCESS != (ret = ups_scan_.open()))
    {
      TBSYS_LOG(WARN, "file table open fail:ret[%d]", ret);
    }
  }

  return ret;
}

int ObFakeUpsMultiGet::close()
{
  return ups_scan_.close();
}


int ObFakeUpsMultiGet::get_next_row(const ObRow *&row)
{
  int ret = OB_SUCCESS;
  UNUSED(row);
  return ret;
}

int ObFakeUpsMultiGet::get_next_row(const ObString *&rowkey, const ObRow *&row)
{
  int ret = OB_SUCCESS;
  const ObRow *tmp_row = NULL;
  const ObUpsRow *ups_row = NULL;
  const ObObj *cell = NULL;

  while(OB_SUCCESS == ret)
  {
    ret = ups_scan_.get_next_row(rowkey, tmp_row);
    if(OB_ITER_END == ret)
    {
      break;
    }
    else if(OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "get next row fail:ret[%d]", ret);
    }

    if(OB_SUCCESS == ret)
    {
      ups_row = dynamic_cast<const ObUpsRow *>(tmp_row);
      if(NULL == ups_row)
      {
        ret = OB_ERROR;
        TBSYS_LOG(WARN, "should be ups row");
      }
    }

    if(OB_SUCCESS == ret)
    {
      bool flag = false;
      for(int32_t i=0;OB_SUCCESS == ret && i<row_key_array_.count();i++)
      {
        if(*rowkey == row_key_array_.at(i))
        {
          if(OB_SUCCESS != (ret = convert(*ups_row, curr_row_)))
          {
            TBSYS_LOG(WARN, "convert fail:ret[%d]", ret);
          }
          else
          {
            row = &curr_row_;
            flag = true;
            break;
          }
        }
      }
      if(flag)
      {
        break;
      }
    }
  }

  
  return ret;
}

//就是做投影操作
int ObFakeUpsMultiGet::convert(const ObUpsRow &from, ObUpsRow &to)
{
  int ret = OB_SUCCESS;
  uint64_t table_id = OB_INVALID_ID;
  uint64_t column_id = OB_INVALID_ID;
  const ObObj *cell = NULL;

  to.set_delete_row(from.is_delete_row());

  for(int64_t i=0;OB_SUCCESS == ret && i<to.get_column_num();i++)
  {
    if(OB_SUCCESS != (ret = to.get_row_desc()->get_tid_cid(i, table_id, column_id)))
    {
      TBSYS_LOG(WARN, "get_tid_cid fail:ret[%d]", ret);
    }
    else if(OB_SUCCESS != (ret = from.get_cell(table_id, column_id, cell)))
    {
      TBSYS_LOG(WARN, "get_cell fail:ret[%d]", ret);
    }
    else if(OB_SUCCESS != (ret = to.set_cell(table_id, column_id, *cell)))
    {
      TBSYS_LOG(WARN, "set cell fail:ret[%d]", ret);
    }
  }
  return ret;
}



