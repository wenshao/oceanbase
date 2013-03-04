/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_rename.cpp
 *
 * Authors:
 *   Yu Huang <xiaochu.yh@taobao.com>
 *
 */
#include "ob_rename.h"
#include "ob_sql_expression.h"
#include "common/utility.h"
using namespace oceanbase::sql;
using namespace oceanbase::common;

ObRename::ObRename():
  table_id_(0), base_table_id_(0),
  org_row_(NULL), org_row_desc_(NULL), row_desc_()
{
}

ObRename::~ObRename()
{
}

int ObRename::set_table(const uint64_t table_id, const uint64_t base_table_id)
{
  int ret = OB_SUCCESS;
  if (table_id <= 0 || base_table_id <= 0)
  {
    TBSYS_LOG(WARN, "invalid id: table_id=%ld, base_table_id=%ld", table_id, base_table_id);
    ret = OB_INVALID_ARGUMENT;
  }
  else
  {
    table_id_ = table_id;
    base_table_id_ = base_table_id;
  }
  return ret;
}

int ObRename::open()
{
  int ret = OB_SUCCESS;
  org_row_ = NULL;
  org_row_desc_ = NULL;
  if (table_id_ <= 0 || base_table_id_ <= 0)
  {
    TBSYS_LOG(WARN, "invalid id: table_id=%ld, base_table_id=%ld", table_id_, base_table_id_);
    ret = OB_NOT_INIT;
  }
  else if (OB_SUCCESS != (ret = ObSingleChildPhyOperator::open()))
  {
    TBSYS_LOG(WARN, "failed to open child_op, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = cons_row_desc()))
  {
    TBSYS_LOG(WARN, "failed to cons row desc, err=%d", ret);
  }
  return ret;
}

int ObRename::close()
{
  org_row_ = NULL;
  org_row_desc_ = NULL;
  row_desc_.reset();
  return ObSingleChildPhyOperator::close();
}

int ObRename::cons_row_desc()
{
  int ret = OB_SUCCESS;
  if (NULL == child_op_)
  {
    TBSYS_LOG(ERROR, "child op is NULL");
    ret = OB_NOT_INIT;
  }
  else if (OB_SUCCESS != (ret = child_op_->get_row_desc(org_row_desc_)))
  {
    TBSYS_LOG(WARN, "failed to get original row desc, err=%d", ret);
  }
  else
  {
    OB_ASSERT(org_row_desc_);
    for (int64_t idx = 0; idx < org_row_desc_->get_column_num(); idx++)
    {
      if (OB_SUCCESS != (ret = row_desc_.add_column_desc(table_id_, OB_APP_MIN_COLUMN_ID+idx)))
      {
        TBSYS_LOG(WARN, "fail to add tid and cid to row desc. idx=%ld, tid=%lu, ret=%d",
                  idx, table_id_, ret);
        break;
      }
    }
  }
  return ret;
}

int ObRename::get_row_desc(const common::ObRowDesc *&row_desc) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(0 >= row_desc_.get_column_num()))
  {
    TBSYS_LOG(ERROR, "not init, call open() first");
    ret = OB_NOT_INIT;
  }
  else
  {
    row_desc = &row_desc_;
  }
  return ret;
}

int ObRename::get_next_row(const common::ObRow *&row)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == child_op_))
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(ERROR, "child_op_ is NULL");
  }
  else
  {
    // reset row desc for under-layer operator
    if (NULL != org_row_desc_ && NULL != org_row_)
    {
      org_row_->set_row_desc(*org_row_desc_);
    }
    if (OB_SUCCESS == (ret = child_op_->get_next_row(row)))
    {
      OB_ASSERT(row);
      org_row_ = const_cast<ObRow *>(row);
      const_cast<ObRow *>(row)->set_row_desc(row_desc_);
    }
  }
  return ret;
}

int64_t ObRename::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  databuff_printf(buf, buf_len, pos, "Rename(out_tid=%lu, base_tid=%lu)\n",
                  table_id_, base_table_id_);
  if (NULL != child_op_)
  {
    pos += child_op_->to_string(buf+pos, buf_len-pos);
  }
  return pos;
}
