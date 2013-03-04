/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_add_project.cpp
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#include "ob_add_project.h"
using namespace oceanbase::common;
using namespace oceanbase::sql;

ObAddProject::ObAddProject()
{
}

ObAddProject::~ObAddProject()
{
}

int ObAddProject::open()
{
  int ret = OB_SUCCESS;
  const ObRowDesc *child_row_desc = NULL;
  if (OB_SUCCESS != (ret = ObSingleChildPhyOperator::open()))
  {
    TBSYS_LOG(WARN, "failed to open child_op, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = child_op_->get_row_desc(child_row_desc)))
  {
    TBSYS_LOG(WARN, "failed to get child row desc, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = cons_row_desc(*child_row_desc)))
  {
    TBSYS_LOG(WARN, "failed to cons row desc, err=%d", ret);
  }
  else
  {
    row_.set_row_desc(row_desc_);
  }
  return ret;
}

int ObAddProject::cons_row_desc(const ObRowDesc &input_row_desc)
{
  int ret = OB_SUCCESS;
  // copy row desc
  row_desc_ = input_row_desc;
  // add aggr columns
  for (int64_t i = 0; i < columns_.count(); ++i)
  {
    const ObSqlExpression &cexpr = columns_.at(i);
    if (OB_SUCCESS != (ret = row_desc_.add_column_desc(cexpr.get_table_id(),
                                                       cexpr.get_column_id())))
    {
      TBSYS_LOG(WARN, "failed to add column desc, err=%d", ret);
      break;
    }
  } // end for
  if (0 >= columns_.count())
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(ERROR, "no column for output");
  }
  return ret;
}

inline int ObAddProject::shallow_copy_input_row(const common::ObRow &input_row)
{
  int ret = OB_SUCCESS;
  const ObObj *cell = NULL;
  uint64_t tid = OB_INVALID_ID;
  uint64_t cid = OB_INVALID_ID;
  for (int64_t i = 0; i < input_row.get_column_num(); ++i)
  {
    if (OB_SUCCESS != (ret = input_row.raw_get_cell(i, cell, tid, cid)))
    {
      TBSYS_LOG(WARN, "failed to get cell, err=%d idx=%ld", ret, i);
      break;
    }
    else if (OB_SUCCESS != (ret = row_.raw_set_cell(i, *cell)))
    {
      TBSYS_LOG(INFO, "failed to set cell, err=%d idx=%ld", ret, i);
      break;
    }
  } // end for
  return ret;
}

int ObAddProject::get_next_row(const common::ObRow *&row)
{
  int ret = OB_SUCCESS;
  const common::ObRow *input_row = NULL;
  if (NULL == child_op_)
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(ERROR, "child_op_ is NULL");
  }
  else if (OB_SUCCESS != (ret = child_op_->get_next_row(input_row)))
  {
    if (OB_ITER_END != ret)
    {
      TBSYS_LOG(WARN, "failed to get next row, err=%d", ret);
    }
  }
  else if (OB_SUCCESS != (ret = shallow_copy_input_row(*input_row)))
  {
    TBSYS_LOG(WARN, "failed to copy row, err=%d", ret);
  }
  else
  {
    const ObObj *result = NULL;
    for (int64_t i = 0; i < columns_.count(); ++i)
    {
      ObSqlExpression &expr = columns_.at(i);
      if (OB_SUCCESS != (ret = expr.calc(*input_row, result)))
      {
        TBSYS_LOG(WARN, "failed to calculate, err=%d", ret);
        break;
      }
      else if (OB_SUCCESS != (ret = row_.set_cell(expr.get_table_id(), expr.get_column_id(), *result)))
      {
        TBSYS_LOG(WARN, "failed to set row cell, err=%d", ret);
        break;
      }
    } // end for
    if (OB_LIKELY(OB_SUCCESS == ret))
    {
      row = &row_;
    }
  }
  return ret;
}
