/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_fake_ups_rpc_stub.cpp
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@taobao.com>
 *
 */

#include "ob_fake_ups_rpc_stub.h"

using namespace oceanbase;
using namespace common;

ObFakeUpsRpcStub::ObFakeUpsRpcStub()
  :column_count_(0)
{
}

ObFakeUpsRpcStub::~ObFakeUpsRpcStub()
{
}

int ObFakeUpsRpcStub::get(const int64_t timeout, const ObServer & server, const ObGetParam & get_param, ObNewScanner & new_scanner) 
{
  int ret = OB_SUCCESS;
  ObUpsRow ups_row;
  ObRowDesc row_desc;

  if( 0 == column_count_ )
  {
    ret = OB_INVALID_ARGUMENT;
  }

  for(int i=1;OB_SUCCESS == ret && i<=column_count_;i++)
  {
    if(OB_SUCCESS != (ret = row_desc.add_column_desc(TABLE_ID, i)))
    {
      TBSYS_LOG(WARN, "add column desc fail:ret[%d]", ret);
    }
  }
  if(OB_SUCCESS == ret)
  {
    ups_row.set_row_desc(row_desc);
  }

  if(OB_SUCCESS == ret)
  {
    if(get_param.get_cell_size() / column_count_ > MAX_ROW_COUNT)
    {
      if(OB_SUCCESS != (ret = new_scanner.set_is_req_fullfilled(false, MAX_ROW_COUNT)))
      {
        TBSYS_LOG(WARN, "set is req fullfilled fail:ret[%d]", ret);
      }
    }
    else
    {
      if(OB_SUCCESS != (ret = new_scanner.set_is_req_fullfilled(true, get_param.get_cell_size())))
      {
        TBSYS_LOG(WARN, "set is req fullfilled fail:ret[%d]", ret);
      }
    }
  }

  int64_t row_count = 0;
  for(int64_t i=0;OB_SUCCESS == ret && i<get_param.get_cell_size();i+=column_count_)
  {
    if( row_count >= MAX_ROW_COUNT )
    {
      break;
    }

    ObString rowkey;

    for(int64_t j=0;OB_SUCCESS == ret && j<column_count_;j++)
    {
      ObCellInfo *cell = get_param[i + j];
      rowkey = cell->row_key_;
      if(OB_SUCCESS != (ret = ups_row.raw_set_cell(j, cell->value_)))
      {
        TBSYS_LOG(WARN, "set cell fail:ret[%d]", ret);
      }
    }

    if(OB_SUCCESS == ret)
    {
      row_count ++;
      if(OB_SUCCESS != (ret = new_scanner.add_row(rowkey, ups_row)))
      {
        TBSYS_LOG(WARN, "add row fail:ret[%d]", ret);
      }
    }
  }
  return ret;
}

int ObFakeUpsRpcStub::get_int(ObString key) 
{
  int ret = 0;
  int num = strlen("rowkey_");
  int pow = 1;
  for(int i=5-1;i>=0;i--)
  {
    char c = key.ptr()[i+num];
    ret += (c - '0') * pow;
    pow *= 10;
  }
  return ret;
}

int ObFakeUpsRpcStub::gen_new_scanner(const ObScanParam & scan_param, ObNewScanner & new_scanner) 
{
  int ret = OB_SUCCESS;
  const ObRange *const range = scan_param.get_range();
  int start = get_int(range->start_key_);
  int end = get_int(range->end_key_);

  ObBorderFlag border_flag;
  border_flag = range->border_flag_;

  if (start+100 >= end)
  {
    if(OB_SUCCESS != (ret = gen_new_scanner(start, end, border_flag, new_scanner, true)))
    {
      TBSYS_LOG(WARN, "gen new scanner fail:ret[%d]", ret);
    }
  }
  else
  {
    border_flag.set_inclusive_end();
    if(OB_SUCCESS != (ret = gen_new_scanner(start, start+100, border_flag, new_scanner, false)))
    {
      TBSYS_LOG(WARN, "gen new scanner fail:ret[%d]", ret);
    }
  }
  return ret;
}

int ObFakeUpsRpcStub::gen_new_scanner(int64_t start_rowkey, int64_t end_rowkey, ObBorderFlag border_flag, ObNewScanner &new_scanner, bool is_fullfilled)
{
  int ret = OB_SUCCESS;
  char start_key[100];
  char end_key[100];
  char rowkey_buf[100];
  ObString rowkey;

  sprintf(start_key, "rowkey_%05d", start_rowkey);
  sprintf(end_key, "rowkey_%05d", end_rowkey);

  ObRange range;
  range.border_flag_ = border_flag;

  ObString start_key_str;
  ObString end_key_str;

  start_key_str.assign_ptr(start_key, strlen(start_key));
  end_key_str.assign_ptr(end_key, strlen(end_key));

  str_buf_.write_string(start_key_str, &(range.start_key_));
  str_buf_.write_string(end_key_str, &(range.end_key_));
  
  if(OB_SUCCESS != (ret = new_scanner.set_range(range)))
  {
    TBSYS_LOG(WARN, "set range:ret[%d]", ret);
  }

  ObUpsRow ups_row;
  ObRowDesc row_desc;
  ObObj value;

  if(OB_SUCCESS == ret)
  {
    for(uint64_t i = 0;OB_SUCCESS == ret && i<COLUMN_NUMS;i++)
    {
      if(OB_SUCCESS != (ret = row_desc.add_column_desc(TABLE_ID, i+OB_APP_MIN_COLUMN_ID)))
      {
        TBSYS_LOG(WARN, "add column desc fail:ret[%d]", ret);
      }
    }
    ups_row.set_row_desc(row_desc);
  }

  int64_t start = border_flag.inclusive_start() ? start_rowkey : start_rowkey + 1;
  int64_t end = border_flag.inclusive_end() ? end_rowkey : end_rowkey - 1;

  if(OB_SUCCESS == ret)
  {
    for(int64_t i=start;OB_SUCCESS == ret && i<=end;i++)
    {
      for(int64_t j=0;OB_SUCCESS == ret && j<COLUMN_NUMS;j++)
      {
        value.set_int(i * 1000 + j);
        if(OB_SUCCESS != (ret = ups_row.raw_set_cell(j, value)))
        {
          TBSYS_LOG(WARN, "raw set cell fail:ret[%d]", ret);
        }
      }

      sprintf(rowkey_buf, "rowkey_%05d", i);
      rowkey.assign_ptr(rowkey_buf, (int32_t)strlen(rowkey_buf));
      if(OB_SUCCESS == ret)
      {
        if(OB_SUCCESS != (ret = new_scanner.add_row(rowkey, ups_row)))
        {
          TBSYS_LOG(WARN, "new scanner add row fail:ret[%d]", ret);
        }
      }
    }
  }

  if(OB_SUCCESS == ret)
  {
    if(OB_SUCCESS != (ret = new_scanner.set_is_req_fullfilled(is_fullfilled, end_rowkey - start_rowkey)))
    {
      TBSYS_LOG(WARN, "set fullfilled fail:ret[%d]", ret);
    }
  }

  return ret;
}

int ObFakeUpsRpcStub::scan(const int64_t timeout, const ObServer & server, const ObScanParam & scan_param, ObNewScanner & new_scanner) 
{
  int ret = OB_SUCCESS;
  UNUSED(timeout);
  UNUSED(server);
  new_scanner.reset();
  if(OB_SUCCESS != (ret = gen_new_scanner(scan_param, new_scanner)))
  {
    TBSYS_LOG(WARN, "gen new scanner fail:ret[%d]", ret);
  }
  return ret;
}

