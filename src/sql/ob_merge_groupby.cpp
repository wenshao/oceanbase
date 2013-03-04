/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_merge_groupby.cpp
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#include "ob_merge_groupby.h"
using namespace oceanbase::sql;
using namespace oceanbase::common;

ObMergeGroupBy::ObMergeGroupBy()
  :last_input_row_(NULL)
{
}

ObMergeGroupBy::~ObMergeGroupBy()
{
  last_input_row_ = NULL;
}

int ObMergeGroupBy::open()
{
  int ret = OB_SUCCESS;
  last_input_row_ = NULL;
  const ObRowDesc *child_row_desc = NULL;
  if (OB_SUCCESS != (ret = ObGroupBy::open()))
  {
    TBSYS_LOG(WARN, "failed to open child op, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = child_op_->get_row_desc(child_row_desc)))
  {
    TBSYS_LOG(WARN, "failed to get child row desc, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = aggr_func_.init(*child_row_desc, aggr_columns_)))
  {
    TBSYS_LOG(WARN, "failed to construct row desc, err=%d", ret);
  }
  return ret;
}

int ObMergeGroupBy::close()
{
  int ret = OB_SUCCESS;
  last_input_row_ = NULL;
  aggr_func_.destroy();
  ret = ObGroupBy::close();
  return ret;
}

int ObMergeGroupBy::get_row_desc(const common::ObRowDesc *&row_desc) const
{
  const ObRowDesc &r = aggr_func_.get_row_desc();
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(0 >= r.get_column_num()))
  {
    TBSYS_LOG(ERROR, "not init");
    ret = OB_NOT_INIT;
  }
  else
  {
    row_desc = &r;
  }
  return ret;
}

// if there is no group columns, is_same_group returns true
int ObMergeGroupBy::is_same_group(const ObRow &row1, const ObRow &row2, bool &result)
{
  int ret = OB_SUCCESS;
  result = true;
  const ObObj *cell1 = NULL;
  const ObObj *cell2 = NULL;
  for (int64_t i = 0; i < group_columns_.count(); ++i)
  {
    const ObGroupColumn &group_col = group_columns_.at(i);
    if (OB_SUCCESS != (ret = row1.get_cell(group_col.table_id_, group_col.column_id_, cell1)))
    {
      TBSYS_LOG(WARN, "failed to get cell, err=%d tid=%lu cid=%lu",
                ret, group_col.table_id_, group_col.column_id_);
      break;
    }
    else if (OB_SUCCESS != (ret = row2.get_cell(group_col.table_id_, group_col.column_id_, cell2)))
    {
      TBSYS_LOG(WARN, "failed to get cell, err=%d tid=%lu cid=%lu",
                ret, group_col.table_id_, group_col.column_id_);
      break;
    }
    else if (*cell1 != *cell2)
    {
      result = false;
      break;
    }
  } // end for
  return ret;
}

int ObMergeGroupBy::get_next_row(const ObRow *&row)
{
  int ret = OB_SUCCESS;
  if (NULL == last_input_row_)
  {
    // get the first input row of one group
    if (OB_SUCCESS != (ret = child_op_->get_next_row(last_input_row_)))
    {
      if (OB_ITER_END != ret)
      {
        TBSYS_LOG(WARN, "failed to get next row, err=%d", ret);
      }
    }
  }
  if (OB_SUCCESS == ret && NULL != last_input_row_)
  {
    if (OB_SUCCESS != (ret = aggr_func_.prepare(*last_input_row_))) // the first row of this group
    {
      TBSYS_LOG(WARN, "failed to init aggr cells, err=%d", ret);
    }
    else
    {
      last_input_row_ = NULL;
    }
  }

  if (OB_SUCCESS == ret)
  {
    bool same_group = false;
    const ObRow *input_row = NULL;
    while (OB_SUCCESS == (ret = child_op_->get_next_row(input_row)))
    {
      if (OB_SUCCESS != (ret = is_same_group(aggr_func_.get_curr_row(), *input_row, same_group)))
      {
        TBSYS_LOG(WARN, "failed to check group, err=%d", ret);
        break;
      }
      else if (same_group)
      {
        if (OB_SUCCESS != (ret = aggr_func_.process(*input_row)))
        {
          TBSYS_LOG(WARN, "failed to calc aggr, err=%d", ret);
          break;
        }
        else if (0 < mem_size_limit_ && mem_size_limit_ < aggr_func_.get_used_mem_size())
        {
          TBSYS_LOG(WARN, "merge group by has exceeded the mem limit, limit=%ld used=%ld",
                    mem_size_limit_, aggr_func_.get_used_mem_size());
          ret = OB_EXCEED_MEM_LIMIT;
          break;
        }
      }
      else if (OB_SUCCESS != (ret = aggr_func_.get_result(row)))
      {
        TBSYS_LOG(WARN, "failed to calculate avg, err=%d", ret);
      }
      else
      {
        last_input_row_ = input_row;
        break;
      }
    } // end while
    if (OB_ITER_END == ret)
    {
      // the last group
      if (OB_SUCCESS != (ret = aggr_func_.get_result(row)))
      {
        TBSYS_LOG(WARN, "failed to calculate avg, err=%d", ret);
      }
      ret = OB_SUCCESS;
    }
  }
  return ret;
}
