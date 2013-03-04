/*
 * (C) 2007-2012 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version:  ob_sql_expression.cpp,  05/28/2012 03:46:40 PM xiaochu Exp $
 *
 * Author:
 *   xiaochu.yh <xiaochu.yh@taobao.com>
 * Description:
 *
 *
 */

#include "ob_sql_expression.h"
#include "common/utility.h"
#include "sql/ob_item_type_str.h"
using namespace oceanbase::sql;
using namespace oceanbase::common;

ObSqlExpression::ObSqlExpression()
  : column_id_(0), table_id_(0), is_aggr_func_(false), is_distinct_(false), aggr_func_(T_INVALID)
{
}

ObSqlExpression::~ObSqlExpression()
{
}

ObSqlExpression& ObSqlExpression::operator=(const ObSqlExpression &other)
{
  post_expr_ = other.post_expr_;
  column_id_ = other.column_id_;
  table_id_ = other.table_id_;
  is_aggr_func_ = other.is_aggr_func_;
  is_distinct_ = other.is_distinct_;
  aggr_func_ = other.aggr_func_;
  return *this;
}

int ObSqlExpression::add_expr_item(const ExprItem &item)
{
  return post_expr_.add_expr_item(item);
}

int ObSqlExpression::add_expr_item_end()
{
  return post_expr_.add_expr_item_end();
}

static ObObj OBJ_ZERO;
static struct obj_zero_init
{
  obj_zero_init()
  {
    OBJ_ZERO.set_int(0);
  }
} obj_zero_init;

int ObSqlExpression::calc(const common::ObRow &row, const common::ObObj *&result)
{
  int err = OB_SUCCESS;
  if (OB_UNLIKELY(is_aggr_func_ && T_FUN_COUNT == aggr_func_ && post_expr_.is_empty()))
  {
    // COUNT(*)
    // point the result to an arbitray non-null cell
    result = &OBJ_ZERO;
  }
  else
  {
    err = post_expr_.calc(row, result);
  }
  return err;
}

int64_t ObSqlExpression::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  if (OB_INVALID_ID == table_id_)
  {
    databuff_printf(buf, buf_len, pos, "expr<NULL,%lu>=", column_id_);
  }
  else
  {
    databuff_printf(buf, buf_len, pos, "expr<%lu,%lu>=", table_id_, column_id_);
  }
  if (is_aggr_func_)
  {
	databuff_printf(buf, buf_len, pos, "%s(%s", ob_aggr_func_str(aggr_func_), is_distinct_ ? "DISTINCT " : "");
  }
  if (post_expr_.is_empty())
  {
    databuff_printf(buf, buf_len, pos, "*");
  }
  else
  {
    databuff_printf(buf, buf_len, pos, "[");
    pos += post_expr_.to_string(buf+pos, buf_len-pos);
    databuff_printf(buf, buf_len, pos, "]");
  }
  if (is_aggr_func_)
  {
	databuff_printf(buf, buf_len, pos, ")");
  }
  return pos;
}

DEFINE_SERIALIZE(ObSqlExpression)
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = serialize_basic_param(buf, buf_len, pos)))
  {
	TBSYS_LOG(WARN, "fail to serialize basic param. ret=%d", ret);
  }
  else if (OB_SUCCESS != (ret = post_expr_.serialize(buf, buf_len, pos)))
  {
	TBSYS_LOG(WARN, "fail to serialize postfix expression. ret=%d", ret);
  }
  else
  {
	// success
	//TBSYS_LOG(INFO, "success serialize one ObSqlExpression. pos=%ld", pos);
  }
  return ret;
}

DEFINE_DESERIALIZE(ObSqlExpression)
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = deserialize_basic_param(buf, data_len, pos)))
  {
	TBSYS_LOG(WARN, "fail to serialize basic param. ret=%d", ret);
  }
  else if (OB_SUCCESS != (ret = post_expr_.deserialize(buf, data_len, pos)))
  {
	TBSYS_LOG(WARN, "fail to serialize postfix expression. ret=%d", ret);
  }
  else
  {
	// success
  }
  return ret;
}

int ObSqlExpression::serialize_basic_param(char* buf, const int64_t buf_len, int64_t& pos) const
{
  int ret = OB_SUCCESS;
  ObObj obj;
  if (OB_SUCCESS == ret)
  {
	obj.set_int((int64_t)column_id_);
	if (OB_SUCCESS != (ret = obj.serialize(buf, buf_len, pos)))
	{
	  TBSYS_LOG(WARN, "fail to serialize obj. ret=%d", ret);
	}
  }
  if (OB_SUCCESS == ret)
  {
	obj.set_int((int64_t)table_id_);
	if (OB_SUCCESS != (ret = obj.serialize(buf, buf_len, pos)))
	{
	  TBSYS_LOG(WARN, "fail to serialize obj. ret=%d", ret);
	}
  }
  if (OB_SUCCESS == ret)
  {
	obj.set_bool(is_aggr_func_);
	if (OB_SUCCESS != (ret = obj.serialize(buf, buf_len, pos)))
	{
	  TBSYS_LOG(WARN, "fail to serialize obj. ret=%d", ret);
	}
  }
  if (OB_SUCCESS == ret)
  {
	obj.set_bool(is_distinct_);
	if (OB_SUCCESS != (ret = obj.serialize(buf, buf_len, pos)))
	{
	  TBSYS_LOG(WARN, "fail to serialize obj. ret=%d", ret);
	}
  }
  if (OB_SUCCESS == ret)
  {
	obj.set_int((int64_t)aggr_func_);
	if (OB_SUCCESS != (ret = obj.serialize(buf, buf_len, pos)))
	{
	  TBSYS_LOG(WARN, "fail to serialize obj. ret=%d", ret);
	}
  }
  return ret;
}

int ObSqlExpression::deserialize_basic_param(const char* buf, const int64_t data_len, int64_t& pos)
{
  int ret = OB_SUCCESS;
  ObObj obj;
  int64_t val = 0;
  if (OB_SUCCESS == ret)
  {
	if (OB_SUCCESS != (ret = obj.deserialize(buf, data_len, pos)))
	{
	  TBSYS_LOG(WARN, "fail to serialize obj. ret=%d", ret);
	}
	if (OB_SUCCESS != (ret = obj.get_int(val)))
	{
	  TBSYS_LOG(WARN, "fail to get int value. ret=%d, column_id_=%lu", ret, column_id_);
	}
	else
	{
	  column_id_ = (uint64_t)val;
	}
  }
  if (OB_SUCCESS == ret)
  {
	if (OB_SUCCESS != (ret = obj.deserialize(buf, data_len, pos)))
	{
	  TBSYS_LOG(WARN, "fail to serialize obj. ret=%d", ret);
	}
	if (OB_SUCCESS != (ret = obj.get_int(val)))
	{
	  TBSYS_LOG(WARN, "fail to get int value. ret=%d, table_id_=%lu", ret, table_id_);
	}
	else
	{
	  table_id_ = val;
	}
  }
  if (OB_SUCCESS == ret)
  {
	if (OB_SUCCESS != (ret = obj.deserialize(buf, data_len, pos)))
	{
	  TBSYS_LOG(WARN, "fail to serialize obj. ret=%d", ret);
	}
	if (OB_SUCCESS != (ret = obj.get_bool(is_aggr_func_)))
	{
	  TBSYS_LOG(WARN, "fail to get int value. ret=%d, is_aggr_func_=%d", ret, is_aggr_func_);
	}
  }
  if (OB_SUCCESS == ret)
  {
	if (OB_SUCCESS != (ret = obj.deserialize(buf, data_len, pos)))
	{
	  TBSYS_LOG(WARN, "fail to serialize obj. ret=%d", ret);
	}
	if (OB_SUCCESS != (ret = obj.get_bool(is_distinct_)))
	{
	  TBSYS_LOG(WARN, "fail to get int value. ret=%d, is_distinct_=%d", ret, is_distinct_);
	}
  }
  if (OB_SUCCESS == ret)
  {
	if (OB_SUCCESS != (ret = obj.deserialize(buf, data_len, pos)))
	{
	  TBSYS_LOG(WARN, "fail to serialize obj. ret=%d", ret);
	}
	if (OB_SUCCESS != (ret = obj.get_int(val)))
	{
	  TBSYS_LOG(WARN, "fail to get int value. ret=%d, aggr_func_=%d", ret, aggr_func_);
	}
	else
	{
	  aggr_func_ = (ObItemType)val;
	}
  }
  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObSqlExpression)
{
  int64_t size = 0;
  size += get_basic_param_serialize_size();
  size += post_expr_.get_serialize_size();
  return size;
}

int64_t ObSqlExpression::get_basic_param_serialize_size() const
{
  int64_t size = 0;
  ObObj obj;
	obj.set_int((int64_t)column_id_);
	size += obj.get_serialize_size();
  obj.set_int((int64_t)table_id_);
	size += obj.get_serialize_size();
	obj.set_bool(is_aggr_func_);
	size += obj.get_serialize_size();
	obj.set_bool(is_distinct_);
	size += obj.get_serialize_size();
	obj.set_int((int64_t)aggr_func_);
	size += obj.get_serialize_size();
  return size;
}
