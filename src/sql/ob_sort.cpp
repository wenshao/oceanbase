/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_sort.cpp
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#include "ob_sort.h"
#include "common/utility.h"
using namespace oceanbase::sql;
using namespace oceanbase::common;

ObSort::ObSort()
  :mem_size_limit_(0), sort_reader_(&in_mem_sort_)
{
}

ObSort::~ObSort()
{
}

void ObSort::set_mem_size_limit(const int64_t limit)
{
  TBSYS_LOG(INFO, "sort mem limit=%ld", limit);
  mem_size_limit_ = limit;
}

int ObSort::set_run_filename(const common::ObString &filename)
{
  TBSYS_LOG(INFO, "sort run file=%.*s", filename.length(), filename.ptr());
  return merge_sort_.set_run_filename(filename);
}

int ObSort::add_sort_column(const uint64_t tid, const uint64_t cid, bool is_ascending)
{
  int ret = OB_SUCCESS;
  ObSortColumn sort_column;
  sort_column.table_id_ = tid;
  sort_column.column_id_ = cid;
  sort_column.is_ascending_ = is_ascending;
  if (OB_SUCCESS != (ret = sort_columns_.push_back(sort_column)))
  {
    TBSYS_LOG(WARN, "failed to push back to array, err=%d", ret);
  }
  return ret;
}

int ObSort::open()
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = ObSingleChildPhyOperator::open()))
  {
    TBSYS_LOG(WARN, "failed to open child_op, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = do_sort()))
  {
    TBSYS_LOG(WARN, "failed to sort input data, err=%d", ret);
  }
  return ret;
}

int ObSort::close()
{
  int ret = OB_SUCCESS;
  in_mem_sort_.reset();
  merge_sort_.reset();
  sort_reader_ = &in_mem_sort_;
  ret = ObSingleChildPhyOperator::close();
  return ret;
}

int ObSort::get_row_desc(const common::ObRowDesc *&row_desc) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == child_op_))
  {
    TBSYS_LOG(ERROR, "child op is NULL");
    ret = OB_NOT_INIT;
  }
  else
  {
    ret = child_op_->get_row_desc(row_desc);
  }
  return ret;
}

int ObSort::do_sort()
{
  int ret = OB_SUCCESS;
  in_mem_sort_.set_sort_columns(sort_columns_);
  merge_sort_.set_sort_columns(sort_columns_);
  bool need_merge = false;
  const common::ObRow *input_row = NULL;
  while(OB_SUCCESS == ret
        && OB_SUCCESS == (ret = child_op_->get_next_row(input_row)))
  {
    if (OB_SUCCESS != (ret = in_mem_sort_.add_row(*input_row)))
    {
      TBSYS_LOG(WARN, "failed to add row, err=%d", ret);
    }
    else if (need_dump())
    {
      if (OB_SUCCESS != (ret = in_mem_sort_.sort_rows()))
      {
        TBSYS_LOG(WARN, "failed to sort, err=%d", ret);
      }
      else if (OB_SUCCESS != (ret = merge_sort_.dump_run(in_mem_sort_)))
      {
        TBSYS_LOG(WARN, "failed to dump, err=%d", ret);
      }
      else
      {
        TBSYS_LOG(INFO, "need merge sort");
        in_mem_sort_.reset();
        need_merge = true;
        sort_reader_ = &merge_sort_;
      }
    }
  } // end while
  if (OB_ITER_END == ret)
  {
    ret = OB_SUCCESS;
  }
  if (OB_SUCCESS == ret)
  {
    // sort the last run
    if (OB_SUCCESS != (ret = in_mem_sort_.sort_rows()))
    {
      TBSYS_LOG(WARN, "failed to sort, err=%d", ret);
    }
    else if (need_merge && 0 < in_mem_sort_.get_row_count())
    {
      merge_sort_.set_final_run(in_mem_sort_);
      if (OB_SUCCESS != (ret = merge_sort_.build_merge_heap()))
      {
        TBSYS_LOG(WARN, "failed to build heap, err=%d", ret);
      }
    }
  }
  return ret;
}

inline bool ObSort::need_dump() const
{
  return mem_size_limit_ <= 0 ? false : (in_mem_sort_.get_used_mem_size() >= mem_size_limit_);
}

int ObSort::get_next_row(const common::ObRow *&row)
{
  return sort_reader_->get_next_row(row);
}

int64_t ObSort::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  databuff_printf(buf, buf_len, pos, "Sort(columns=[");
  for (int64_t i = 0; i < sort_columns_.count(); ++i)
  {
    if (OB_INVALID_ID != sort_columns_.at(i).table_id_)
    {
      databuff_printf(buf, buf_len, pos, "<%lu,%lu,%s>",
                      sort_columns_.at(i).table_id_, sort_columns_.at(i).column_id_,
                      sort_columns_.at(i).is_ascending_?"ASC":"DESC");
    }
    else
    {
      databuff_printf(buf, buf_len, pos, "<NULL,%lu,%s>",
                      sort_columns_.at(i).column_id_,
                      sort_columns_.at(i).is_ascending_?"ASC":"DESC");
    }
    if (i != sort_columns_.count() -1)
    {
      databuff_printf(buf, buf_len, pos, ",");
    }
  }
  databuff_printf(buf, buf_len, pos, "], mem_size_limit=%ld)\n", mem_size_limit_);
  if (NULL != child_op_)
  {
    int64_t pos2 = child_op_->to_string(buf+pos, buf_len-pos);
    pos += pos2;
  }
  return pos;
}
