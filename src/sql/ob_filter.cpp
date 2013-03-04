/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_filter.cpp
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#include "ob_filter.h"
#include "common/utility.h"
using namespace oceanbase::sql;
using namespace oceanbase::common;

ObFilter::ObFilter()
{
}

ObFilter::~ObFilter()
{
}

int ObFilter::add_filter(const ObSqlExpression& expr)
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = filters_.push_back(expr)))
  {
    //@todo to_cstring(expr)
    TBSYS_LOG(WARN, "failed to add column, err=%d", ret);
  }
  return ret;
}

int ObFilter::open()
{
  return ObSingleChildPhyOperator::open();
}

int ObFilter::close()
{
  return ObSingleChildPhyOperator::close();
}

int ObFilter::get_row_desc(const common::ObRowDesc *&row_desc) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == child_op_))
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(ERROR, "child_op_ is NULL");
  }
  else
  {
    ret = child_op_->get_row_desc(row_desc);
  }
  return ret;
}

int ObFilter::get_next_row(const common::ObRow *&row)
{
  int ret = OB_SUCCESS;
  const common::ObRow *input_row = NULL;
  if (OB_UNLIKELY(NULL == child_op_))
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(ERROR, "child_op_ is NULL");
  }
  else
  {
    const ObObj *result = NULL;
    bool did_output = true;
    while(OB_SUCCESS == ret
          && OB_SUCCESS == (ret = child_op_->get_next_row(input_row)))
    {
      did_output = true;
      for (int64_t i = 0; i < filters_.count(); ++i)
      {
        ObSqlExpression &expr = filters_.at(i);
        if (OB_SUCCESS != (ret = expr.calc(*input_row, result)))
        {
          TBSYS_LOG(WARN, "failed to calc expression, err=%d", ret);
          break;
        }
        else if (OB_SUCCESS != (ret = result->get_bool(did_output)))
        {
          TBSYS_LOG(WARN, "failed to get expression result as a bool value, err=%d", ret);
          break;
        }
        else if (false == did_output)
        {
          break;
        }
      } // end for
      if (did_output)
      {
        row = input_row;
        break;
      }
    } // end while
  }
  return ret;
}

int64_t ObFilter::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  databuff_printf(buf, buf_len, pos, "Filter(filters=[");
  for (int64_t i = 0; i < filters_.count(); ++i)
  {
    int64_t pos2 = filters_.at(i).to_string(buf+pos, buf_len-pos);
    pos += pos2;
    if (i != filters_.count() -1)
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


DEFINE_SERIALIZE(ObFilter)
{
  int ret = OB_SUCCESS;
  ObObj obj;

  obj.set_int(filters_.count());
  if (OB_SUCCESS != (ret = obj.serialize(buf, buf_len, pos)))
  {
    TBSYS_LOG(WARN, "fail to serialize filter expr count. ret=%d", ret);
  }
  else
  {
    for (int64_t i = 0; i < filters_.count(); ++i)
    {
      const ObSqlExpression &expr = filters_.at(i);
      if (ret == OB_SUCCESS && (OB_SUCCESS != (ret = expr.serialize(buf, buf_len, pos))))
      {
        TBSYS_LOG(WARN, "filter expr serialize fail. ret=%d", ret);
        break;
      }
    } // end for
  }
  if (0 >= filters_.count())
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(ERROR, "no column for output");
  }
  return ret;
}


DEFINE_GET_SERIALIZE_SIZE(ObFilter)
{
  int64_t size = 0;
  ObObj obj;
  obj.set_int(filters_.count());
  size += obj.get_serialize_size();
  for (int64_t i = 0; i < filters_.count(); ++i)
  {
    const ObSqlExpression &expr = filters_.at(i);
    size += expr.get_serialize_size();
  }
  return size;
}

DEFINE_DESERIALIZE(ObFilter)
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
        if (OB_SUCCESS != (ret = add_filter(expr)))
        {
          TBSYS_LOG(WARN, "fail to add expression to filter.ret=%d, buf=%p, data_len=%ld, pos=%ld", ret, buf, data_len, pos);
          break;
        }
      }
    }
  }
  return ret;
}


void ObFilter::assign(const ObFilter &other)
{
  filters_ = other.filters_;
}
