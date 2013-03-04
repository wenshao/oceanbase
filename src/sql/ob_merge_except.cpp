/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_merge_except.cpp
 *
 * Authors:
 *   TIAN GUAN <tianguan.dgb@taobao.com>
 *
 */

#include "ob_merge_except.h"
#include "common/utility.h"
using namespace oceanbase::common;
using namespace oceanbase::sql;

ObMergeExcept::ObMergeExcept()
{
}

ObMergeExcept::~ObMergeExcept()
{
}

int ObMergeExcept::get_row_desc(const common::ObRowDesc *&row_desc) const
{
  row_desc = NULL;
  return OB_NOT_IMPLEMENT;
}

int ObMergeExcept::get_next_row(const ObRow *&row)
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
        break;
      }
      错误返回值检查;
    }

    if (cur_first_query_row_ == cur_second_query_row_)
    {
      ret  = first_op_->get_next_row(cur_first_query_row_);
      if (ret == OB_ITER_END)
      {
        first_query_end_ = true;
        return OB_ITER_END;
      }
      错误返回值检查;
      ret  = second_op_->get_next_row(cur_second_query_row_);
      if (ret == OB_ITER_END)
      {
        second_query_end_ = true;
        break;
      }
      错误返回值检查;
    }
    else if (cur_first_query_row_ > cur_second_query_row_)
    {
      ret  = second_op_->get_next_row(cur_second_query_row_);
      if (ret == OB_ITER_END)
      {
        first_query_end_ = true;
        return OB_ITER_END;
      }
      错误返回值检查;
    }
    else // less than
    {
      break;
    }
  }

  if (first_query_end_)
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
    ret = first_op_->get_next_row(cur_first_query_row);
    if (ret == OB_ITER_END)
    {
      first_query_end_ = true;
      break;
    }
    错误返回值检查;
  } while (*cur_first_query_row_ == *last_row_ && distinct_);

  row = last_row_;
*/

  return OB_NOT_IMPLEMENT;
}

int64_t ObMergeExcept::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  databuff_printf(buf, buf_len, pos, "MergeExcept()\n");
  if (NULL != left_op_)
  {
    databuff_printf(buf, buf_len, pos, "ExceptLeftChild=\n");
    pos += left_op_->to_string(buf+pos, buf_len-pos);
  }
  if (NULL != right_op_)
  {
    databuff_printf(buf, buf_len, pos, "ExceptRightChild=\n");
    pos += right_op_->to_string(buf+pos, buf_len-pos);
  }
  return pos;
}
