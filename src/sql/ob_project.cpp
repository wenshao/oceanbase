/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_project.cpp
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#include "ob_project.h"
#include "ob_sql_expression.h"
#include "common/utility.h"
using namespace oceanbase::sql;
using namespace oceanbase::common;

ObProject::ObProject()
{
}

ObProject::~ObProject()
{
}

int ObProject::add_output_column(const ObSqlExpression& expr)
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = columns_.push_back(expr)))
  {
    // @todo to_cstring(expr)
    TBSYS_LOG(WARN, "failed to add column, err=%d", ret);
  }
  return ret;
}

int ObProject::cons_row_desc()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < columns_.count(); ++i)
  {
    const ObSqlExpression &expr = columns_.at(i);
    if (OB_SUCCESS != (ret = row_desc_.add_column_desc(expr.get_table_id(), expr.get_column_id())))
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

int ObProject::open()
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = ObSingleChildPhyOperator::open()))
  {
    TBSYS_LOG(WARN, "failed to open child_op, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = cons_row_desc()))
  {
    TBSYS_LOG(WARN, "failed to construct row desc, err=%d", ret);
  }
  else
  {
    row_.set_row_desc(row_desc_);
  }
  return ret;
}

int ObProject::close()
{
  row_desc_.reset();
  return ObSingleChildPhyOperator::close();
}

int ObProject::get_row_desc(const common::ObRowDesc *&row_desc) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(0 >= row_desc_.get_column_num()))
  {
    TBSYS_LOG(ERROR, "not init");
    ret = OB_NOT_INIT;
  }
  else
  {
    row_desc = &row_desc_;
  }
  return ret;
}

int ObProject::get_next_row(const common::ObRow *&row)
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
    if (OB_SUCCESS == ret)
    {
      row = &row_;
    }
  }
  return ret;
}

int64_t ObProject::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  databuff_printf(buf, buf_len, pos, "Project(columns=[");
  for (int64_t i = 0; i < columns_.count(); ++i)
  {
    int64_t pos2 = columns_.at(i).to_string(buf+pos, buf_len-pos);
    pos += pos2;
    if (i != columns_.count() -1)
    {
      databuff_printf(buf, buf_len, pos, ",");
    }
  }
  databuff_printf(buf, buf_len, pos, "])\n");
  if (NULL != child_op_)
  {
    int64_t pos2 = child_op_->to_string(buf+pos, buf_len-pos);
    pos += pos2;
  }
  return pos;
}


DEFINE_SERIALIZE(ObProject)
{
  int ret = OB_SUCCESS;
  ObObj obj;

  obj.set_int(columns_.count());
  if (OB_SUCCESS != (ret = obj.serialize(buf, buf_len, pos)))
  {
    TBSYS_LOG(WARN, "fail to serialize expr count. ret=%d", ret);
  }
  else
  {
    for (int64_t i = 0; i < columns_.count(); ++i)
    {
      const ObSqlExpression &expr = columns_.at(i);
      if (ret == OB_SUCCESS && (OB_SUCCESS != (ret = expr.serialize(buf, buf_len, pos))))
      {
        TBSYS_LOG(WARN, "serialize fail. ret=%d", ret);
        break;
      }
    } // end for
  }
  if (0 >= columns_.count())
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(ERROR, "no column for output");
  }
  return ret;
}

DEFINE_DESERIALIZE(ObProject)
{
  int ret = OB_SUCCESS;
  ObObj obj;
  int64_t expr_count = 0, i = 0;
  //reset();
  if (OB_SUCCESS != (ret = obj.deserialize(buf, data_len, pos)))
  {
    TBSYS_LOG(WARN, "fail to deserialize expr count. ret=%d", ret);
  }
  else if (OB_SUCCESS != (ret = obj.get_int(expr_count)))
  {
    TBSYS_LOG(WARN, "fail to get expr_count. ret=%d", ret);
  }
  else
  {
    for (i = 0; i < expr_count; i++)
    {
      ObSqlExpression expr;
      if (OB_SUCCESS != (ret = expr.deserialize(buf, data_len, pos)))
      {
        TBSYS_LOG(WARN, "fail to deserialize expression. ret=%d", ret);
        break;
      }
      else
      {
        if (OB_SUCCESS != (ret = add_output_column(expr)))
        {
          TBSYS_LOG(DEBUG, "fail to add expr to project ret=%d. buf=%p, data_len=%ld, pos=%ld", ret, buf, data_len, pos);
          break;
        }
      }
    }
  }
  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObProject)
{
  int64_t size = 0;
  ObObj obj;
  obj.set_int(columns_.count());
  size += obj.get_serialize_size();
  for (int64_t i = 0; i < columns_.count(); ++i)
  {
    const ObSqlExpression &expr = columns_.at(i);
    size += expr.get_serialize_size();
  }
  return size;
}



void ObProject::assign(const ObProject &other)
{
  columns_ = other.columns_;
}
