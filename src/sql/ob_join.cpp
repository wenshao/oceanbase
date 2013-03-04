/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_join.cpp
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#include "ob_join.h"
#include "common/utility.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;

ObJoin::ObJoin()
{
}

ObJoin::~ObJoin()
{
}

int ObJoin::set_join_type(const JoinType join_type)
{
  join_type_ = join_type;
  return OB_SUCCESS;
}

int ObJoin::add_equijoin_condition(const ObSqlExpression& expr)
{
  return equal_join_conds_.push_back(expr);
}

int ObJoin::add_other_join_condition(const ObSqlExpression& expr)
{
  return other_join_conds_.push_back(expr);
}

int ObJoin::get_next_row(const ObRow *&row)
{
  row = NULL;
  return OB_ERROR;
}

int64_t ObJoin::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  databuff_printf(buf, buf_len, pos, "Join(Join Type: ");
  switch(join_type_)
  {
    case INNER_JOIN:
      databuff_printf(buf, buf_len, pos, "INNER JOIN\n");
      break;
    case LEFT_OUTER_JOIN:
      databuff_printf(buf, buf_len, pos, "LEFT OUTER JOIN\n");
      break;
    case RIGHT_OUTER_JOIN:
      databuff_printf(buf, buf_len, pos, "RIGHT OUTER JOIN\n");
      break;
    case FULL_OUTER_JOIN:
      databuff_printf(buf, buf_len, pos, "FULL OUTER JOIN\n");
      break;
    case LEFT_SEMI_JOIN:
      databuff_printf(buf, buf_len, pos, "LEFT SEMI JOIN\n");
      break;
    case RIGHT_SEMI_JOIN:
      databuff_printf(buf, buf_len, pos, "RIGHT SEMI JOIN\n");
      break;
    case LEFT_ANTI_SEMI_JOIN:
      databuff_printf(buf, buf_len, pos, "LEFT ANTI SEMI JOIN JOIN\n");
      break;
    case RIGHT_ANTI_SEMI_JOIN:
      databuff_printf(buf, buf_len, pos, "RIGHT ANTI SEMI JOIN JOIN\n");
      break;
    default:
      break;
  }
  databuff_printf(buf, buf_len, pos, "(equal_join_conds=[");
  for (int64_t i = 0; i < equal_join_conds_.count(); ++i)
  {
    const ObSqlExpression &expr = equal_join_conds_.at(i);
    pos += expr.to_string(buf+pos, buf_len-pos);
    if (i != equal_join_conds_.count() -1)
    {
      databuff_printf(buf, buf_len, pos, ",");
    }
  } // end for
  databuff_printf(buf, buf_len, pos, "], other_join_conds=[");
  for (int64_t i = 0; i < other_join_conds_.count(); ++i)
  {
    const ObSqlExpression &expr = other_join_conds_.at(i);
    pos += expr.to_string(buf+pos, buf_len-pos);
    if (i != other_join_conds_.count() -1)
    {
      databuff_printf(buf, buf_len, pos, ",");
    }
  } // end for
  databuff_printf(buf, buf_len, pos, "])\n");
  if (NULL != left_op_)
  {
    databuff_printf(buf, buf_len, pos, "Left Join Table:\n");
    int64_t pos2 = left_op_->to_string(buf+pos, buf_len-pos);
    pos += pos2;
  }
  if (NULL != right_op_)
  {
    databuff_printf(buf, buf_len, pos, "right Join Table:\n");
    int64_t pos2 = right_op_->to_string(buf+pos, buf_len-pos);
    pos += pos2;
  }
  return pos;
}


