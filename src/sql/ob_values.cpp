/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_values.cpp
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#include "ob_values.h"
#include "common/utility.h"
using namespace oceanbase::sql;
using namespace oceanbase::common;

ObValues::ObValues()
{
}

ObValues::~ObValues()
{
}

int ObValues::set_row_desc(const common::ObRowDesc &row_desc)
{
  row_desc_ = row_desc;
  return OB_SUCCESS;
}

int ObValues::add_values(const common::ObRow &value)
{
  const ObRowStore::StoredRow *stored_row = NULL;
  return row_store_.add_row(value, stored_row);
}

int ObValues::open()
{
  curr_row_.set_row_desc(row_desc_);
  return OB_SUCCESS;
}

int ObValues::close()
{
  row_store_.clear();
  return OB_SUCCESS;
}

int ObValues::get_next_row(const common::ObRow *&row)
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = row_store_.get_next_row(curr_row_)))
  {
    if (OB_ITER_END != ret)
    {
      TBSYS_LOG(WARN, "failed to get next row from row store, err=%d", ret);
    }
  }
  else
  {
    row = &curr_row_;
  }
  return ret;
}

int ObValues::get_row_desc(const common::ObRowDesc *&row_desc) const
{
  row_desc = &row_desc_;
  return OB_SUCCESS;
}

int64_t ObValues::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  databuff_printf(buf, buf_len, pos, "Values()\n");
  return pos;
}
