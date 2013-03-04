/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_merge_union.cpp
 *
 * Authors:
 *   TIAN GUAN <tianguan.dgb@taobao.com>
 *
 */

#include "ob_merge_union.h"
#include "common/utility.h"
using namespace oceanbase::common;
using namespace oceanbase::sql;

ObMergeUnion::ObMergeUnion()
{
}

ObMergeUnion::~ObMergeUnion()
{
}

int ObMergeUnion::get_row_desc(const common::ObRowDesc *&row_desc) const
{
  row_desc = NULL;
  return OB_NOT_IMPLEMENT;
}

int ObMergeUnion::get_next_row(const ObRow *&row)
{
  ObRow* res_row = NULL;
  row = res_row;
  /*
   * When UNION ALL, we get results from two query one by one
   */
/*
  if (!distinct_)
  {
    if (!first_query_end_)
    {
      ret = first_op_->get_next_row(res_row);
      if (ret == OB_SUCCESS)
      {
        row = res_row;
        return ret;
      }
      else if (ret == OB_ITER_END)
        first_query_end_ = true;
      else
        错误处理;
    }
    if (!second_query_end_)
    {
      ret = second_op_->get_next_row(res_row);
      if (ret == OB_SUCCESS)
      {
        row = res_row;
        return ret;
      }
      else if (ret == OB_ITER_END)
      {
        second_query_end_ = true;
        return OB_ITER_END;
      }
      else
        错误处理;
    }
    else
      return OB_ITER_END;
  }
*/

  /*
   * When UNION DISTINCT, we need the two query already ordered.
   */
/*
  while ((!cur_first_query_row_ || *cur_first_query_row_ == last_row_)
    && !first_query_end_)
  {
    ret = first_op_->get_next_row(cur_first_query_row_);
    if (ret == OB_ITER_END)
      first_query_end_ = true;
    错误返回值检查;
  }
  while ((!cur_second_query_row_ || *cur_second_query_row_ == last_row_)
    && !second_query_end_)
  {
    ret = second_op_->get_next_row(cur_second_query_row_);
    if (ret == OB_ITER_END)
      second_query_end_ = true;
    检查返回结果;
  }
  if (!first_query_end_ && !second_query_end_)
  {
    if (cur_first_query_row_ <= cur_second_query_row_)
    {
      row = cur_first_query_row_;
      last_row_.assign(cur_first_query_row_);
      处理last_row_中的指针，把偏移处理到last_copressive_row_中;
      return OB_SUCCESS;
    }
    else
    {
      row = cur_second_query_row_;
      last_row_.assign(cur_second_query_row_);
      处理last_row_中的指针，把偏移处理到last_copressive_row_中;
      return OB_SUCCESS;
    }
  }
  else if (!first_query_end_)
  {
    row = cur_first_query_row_;
    last_row_.assign(cur_first_query_row_);
    处理last_row_中的指针，把偏移处理到last_copressive_row_中;
    return OB_SUCCESS;
  }
  else if (!second_query_end_)
  {
    row = cur_second_query_row_;
    last_row_.assign(cur_second_query_row_);
    处理last_row_中的指针，把偏移处理到last_copressive_row_中;
    return OB_SUCCESS;
  }
  else
  {
    return OB_ITER_END;
  }
*/
  // won't be here
  return OB_NOT_IMPLEMENT;

}

int64_t ObMergeUnion::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  databuff_printf(buf, buf_len, pos, "MergeUnion()\n");
  if (NULL != left_op_)
  {
    databuff_printf(buf, buf_len, pos, "UnionLeftChild=\n");
    pos += left_op_->to_string(buf+pos, buf_len-pos);
  }
  if (NULL != right_op_)
  {
    databuff_printf(buf, buf_len, pos, "UnionRightChild=\n");
    pos += right_op_->to_string(buf+pos, buf_len-pos);
  }
  return pos;
}
