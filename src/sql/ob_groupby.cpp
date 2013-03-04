/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_groupby.cpp
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#include "ob_groupby.h"
#include "common/utility.h"
using namespace oceanbase::sql;
using namespace oceanbase::common;
ObGroupBy::ObGroupBy()
  :mem_size_limit_(0)
{
}

ObGroupBy::~ObGroupBy()
{
}

void ObGroupBy::set_mem_size_limit(const int64_t limit)
{
  TBSYS_LOG(INFO, "groupby mem limit=%ld", limit);
  mem_size_limit_ = limit;
}

int ObGroupBy::add_group_column(const uint64_t tid, const uint64_t cid)
{
  int ret = OB_SUCCESS;
  ObGroupColumn group_column;
  group_column.table_id_ = tid;
  group_column.column_id_ = cid;
  if (OB_SUCCESS != (ret = group_columns_.push_back(group_column)))
  {
    TBSYS_LOG(WARN, "failed to push back, err=%d", ret);
  }
  return ret;
}

int ObGroupBy::add_aggr_column(const ObSqlExpression& expr)
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = aggr_columns_.push_back(expr)))
  {
    TBSYS_LOG(WARN, "failed to push back, err=%d", ret);
  }
  return ret;
}

int64_t ObGroupBy::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  databuff_printf(buf, buf_len, pos, "GroupBy(group_cols=[");
  for (int64_t i = 0; i < group_columns_.count(); ++i)
  {
    const ObGroupColumn &g = group_columns_.at(i);
    databuff_printf(buf, buf_len, pos, "<%lu,%lu>", g.table_id_, g.column_id_);
    if (i != group_columns_.count() -1)
    {
      databuff_printf(buf, buf_len, pos, ",");
    }
  } // end for
  databuff_printf(buf, buf_len, pos, "], aggr_cols=[");
  for (int64_t i = 0; i < aggr_columns_.count(); ++i)
  {
    const ObSqlExpression &expr = aggr_columns_.at(i);
    pos += expr.to_string(buf+pos, buf_len-pos);
    if (i != aggr_columns_.count() -1)
    {
      databuff_printf(buf, buf_len, pos, ",");
    }
  } // end for
  databuff_printf(buf, buf_len, pos, "])\n");
  if (NULL != child_op_)
  {
    int64_t pos2 = child_op_->to_string(buf+pos, buf_len-pos);
    pos += pos2;
  }
  return pos;
}
