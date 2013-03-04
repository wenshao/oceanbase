/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_scalar_aggregate.cpp
 *
 * Authors:
 *   Guibin Du <tianguan.dgb@taobao.com>
 *
 */
#include "ob_scalar_aggregate.h"
#include "common/utility.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;

ObScalarAggregate::ObScalarAggregate()
{
}

ObScalarAggregate::~ObScalarAggregate()
{
}

int ObScalarAggregate::open()
{
  return merge_groupby_.open();
}

int ObScalarAggregate::close()
{
  return merge_groupby_.close();
}

int ObScalarAggregate::get_next_row(const ObRow *&row)
{
  return merge_groupby_.get_next_row(row);
}

int ObScalarAggregate::get_row_desc(const common::ObRowDesc *&row_desc) const
{
  return merge_groupby_.get_row_desc(row_desc);
}

int ObScalarAggregate::add_aggr_column(ObSqlExpression& expr)
{
  return merge_groupby_.add_aggr_column(expr);
}

int64_t ObScalarAggregate::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  databuff_printf(buf, buf_len, pos, "ScalarAggr(aggr_cols=[");
  for (int64_t i = 0; i < merge_groupby_.aggr_columns_.count(); ++i)
  {
    const ObSqlExpression &expr = merge_groupby_.aggr_columns_.at(i);
    pos += expr.to_string(buf+pos, buf_len-pos);
    if (i != merge_groupby_.aggr_columns_.count() -1)
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
