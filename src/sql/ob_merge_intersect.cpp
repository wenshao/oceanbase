/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_merge_iterset.cpp
 *
 * Authors:
 *   TIAN GUAN <tianguan.dgb@taobao.com>
 *
 */

#include "ob_merge_intersect.h"
#include "common/utility.h"
using namespace oceanbase::common;
using namespace oceanbase::sql;

ObMergeIntersect::ObMergeIntersect()
{
}

ObMergeIntersect::~ObMergeIntersect()
{
}

int ObMergeIntersect::get_row_desc(const common::ObRowDesc *&row_desc) const
{
  row_desc = NULL;
  return OB_NOT_IMPLEMENT;
}

int ObMergeIntersect::get_next_row(const ObRow *&row)
{
  row = NULL;
/*
  while (!first_query_end_ && !second_query_end_)
  {
    if (!cur_first_query_row_)
    {
      ret  = first_op_->get_next_row(cur_first_query_row_);
      if (ret == OB_ITER_END)
      {
        first_query_end_ = true;
        return OB_ITER_END;
      }
      错误返回值检查;
    }
    if (!cur_second_query_row_)
    {
      ret  = second_op_->get_next_row(cur_second_query_row_);
      if (ret == OB_ITER_END)
      {
        second_query_end_ = true;
        return OB_ITER_END;
      }
      错误返回值检查;
    }

    if (cur_first_query_row_ < cur_second_query_row_)
    {
      ret  = first_op_->get_next_row(cur_first_query_row_);
      if (ret == OB_ITER_END)
      {
        first_query_end_ = true;
        return OB_ITER_END;
      }
      错误返回值检查;
    }
    else if (cur_first_query_row_ > cur_second_query_row_)
    {
      ret  = second_op_->get_next_row(cur_second_query_row_);
      if (ret == OB_ITER_END)
      {
        second_query_end_ = true;
        return OB_ITER_END;
      }
      错误返回值检查;
    }
    else // equal
    {
      break;
    }
  }

  if (first_query_end_ || second_query_end_)
    return OB_ITER_END;

  if (*cur_first_query_row_ != *last_row_)
  {
    last_row_.assign(cur_first_query_row_);
    处理last_row_中的指针，把偏移处理到last_copressive_row_中;
    row = cur_first_query_row_;
  }
*/
  /*
   * INTERSECT ALL 只会跳过已经有结果的当前行
   * INTERSECT DISTINCT 会跳过所有和当前结果行相等的行
   */
/*
  do
  {
    ret = first_op_->get_next_row(cur_first_query_row_);
    if (ret == OB_ITER_END)
    {
      first_query_end_ = true;
      break;
    }
    错误返回值检查;
  } while (*cur_first_query_row_ == *last_row_ && distinct_);
  do
  {
    ret = first_op_->get_next_row(cur_first_query_row_);
    if (ret == OB_ITER_END)
    {
      first_query_end_ = true;
      break;
    }
    错误返回值检查;
  } while (*cur_first_query_row_ == *last_row_ && distinct);

  row = last_row_;
*/

  return OB_NOT_IMPLEMENT;
}

int64_t ObMergeIntersect::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  databuff_printf(buf, buf_len, pos, "MergeIntersect()\n");
  if (NULL != left_op_)
  {
    databuff_printf(buf, buf_len, pos, "IntersectLeftChild=\n");
    pos += left_op_->to_string(buf+pos, buf_len-pos);
  }
  if (NULL != right_op_)
  {
    databuff_printf(buf, buf_len, pos, "IntersectRightChild=\n");
    pos += right_op_->to_string(buf+pos, buf_len-pos);
  }
  return pos;
}
