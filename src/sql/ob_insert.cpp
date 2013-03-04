/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_insert.cpp
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#include "ob_insert.h"
#include "common/utility.h"
using namespace oceanbase::sql;
using namespace oceanbase::common;

ObInsert::ObInsert()
  :table_id_(OB_INVALID_ID)
{
}

ObInsert::~ObInsert()
{
}

int ObInsert::open()
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_ID == table_id_
      || 0 >= row_desc_.get_column_num())
  {
    TBSYS_LOG(WARN, "not init, tid=%lu", table_id_);
  }
  else if (OB_SUCCESS != (ret = ObSingleChildPhyOperator::open()))
  {
    TBSYS_LOG(WARN, "failed to open child op, err=%d", ret);
  }
  else
  {
    mutator_.use_db_sem();
    ret = mutator_.reset();
  }
  return ret;
}

int ObInsert::close()
{
  mutator_.reset();
  return ObSingleChildPhyOperator::close();
}

int ObInsert::insert_by_mutator()
{
  int ret = OB_SUCCESS;
  OB_ASSERT(child_op_);
  const ObRow *row = NULL;
  uint64_t tid = OB_INVALID_ID;
  uint64_t cid = OB_INVALID_ID;
  const ObObj *cell = NULL;
  ObString rowkey;              // @bug
  while(OB_SUCCESS == ret
        && OB_SUCCESS == (ret = child_op_->get_next_row(row)))
  {
    for (int64_t i = 0; i < row->get_column_num(); ++i)
    {
      if (OB_SUCCESS != (ret = row->raw_get_cell(i, cell, tid, cid)))
      {
        TBSYS_LOG(WARN, "failed to get cell, err=%d i=%ld", ret, i);
        break;
      }
      else if (OB_SUCCESS != (ret = row_desc_.get_tid_cid(i, tid, cid)))
      {
        TBSYS_LOG(WARN, "failed to get cid from row desc, err=%d", ret);
        break;
      }
      else if (OB_SUCCESS != (ret = mutator_.insert(table_id_, rowkey, cid, *cell)))
      {
        TBSYS_LOG(WARN, "failed to insert cell, err=%d", ret);
        break;
      }
    }
  }
  if (OB_ITER_END != ret)
  {
    TBSYS_LOG(WARN, "failed to cons mutator, err=%d", ret);
  }
  else
  {
    ret = OB_SUCCESS;
  }
  // @todo send mutator
  return ret;
}

int64_t ObInsert::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  databuff_printf(buf, buf_len, pos, "Insert(tid=%lu cols_num=%ld)", table_id_, row_desc_.get_column_num());
  if (NULL != child_op_)
  {
    pos += child_op_->to_string(buf+pos, buf_len-pos);
  }
  return pos;
}
