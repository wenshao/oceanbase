/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_expr_obj.cpp
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#include <cmath>
#include "ob_expr_obj.h"
#include "ob_string_search.h"

using namespace oceanbase::common;

void ObExprObj::assign(const ObObj &obj)
{
  type_ = obj.get_type();
  switch(obj.get_type())
  {
    case ObNullType:
      break;
    case ObIntType:
      obj.get_int(v_.int_);
      break;
    case ObDateTimeType:
      obj.get_datetime(v_.datetime_);
      break;
    case ObPreciseDateTimeType:
      obj.get_precise_datetime(v_.precisedatetime_);
      break;
    case ObVarcharType:
      obj.get_varchar(varchar_);
      break;
    case ObCreateTimeType:
      obj.get_createtime(v_.createtime_);
      break;
    case ObModifyTimeType:
      obj.get_modifytime(v_.modifytime_);
      break;
    case ObBoolType:
      obj.get_bool(v_.bool_);
      break;
    case ObDecimalType:
      obj.get_decimal(num_);
      break;
    case ObFloatType:
      obj.get_float(v_.float_);
      break;
    case ObDoubleType:
      obj.get_double(v_.double_);
      break;
    default:
      TBSYS_LOG(ERROR, "invalid value type=%d", obj.get_type());
      break;
  }
}

int ObExprObj::to(ObObj &obj) const
{
  int ret = OB_SUCCESS;
  switch(get_type())
  {
    case ObNullType:
      obj.set_null();
      break;
    case ObIntType:
      obj.set_int(v_.int_);
      break;
    case ObDateTimeType:
      obj.set_datetime(v_.datetime_);
      break;
    case ObPreciseDateTimeType:
      obj.set_precise_datetime(v_.precisedatetime_);
      break;
    case ObVarcharType:
      obj.set_varchar(varchar_);
      break;
    case ObCreateTimeType:
      obj.set_createtime(v_.createtime_);
      break;
    case ObModifyTimeType:
      obj.set_modifytime(v_.modifytime_);
      break;
    case ObBoolType:
      obj.set_bool(v_.bool_);
      break;
    case ObDecimalType:
      ret = obj.set_decimal(num_);
      break;
    case ObFloatType:
      obj.set_float(v_.float_);
      break;
    case ObDoubleType:
      obj.set_double(v_.double_);
      break;
    default:
      TBSYS_LOG(ERROR, "invalid value type=%d", obj.get_type());
      ret = OB_ERR_UNEXPECTED;
      break;
  }
  return ret;
}

inline bool ObExprObj::is_datetime() const
{
  return ((type_ == ObDateTimeType)
          || (type_ == ObPreciseDateTimeType)
          || (type_ == ObCreateTimeType)
          || (type_ == ObModifyTimeType));
}

bool ObExprObj::can_compare(const ObExprObj & other) const
{
  bool ret = false;
  if ((get_type() == ObNullType) || (other.get_type() == ObNullType)
      || (get_type() == other.get_type()) || (is_datetime() && other.is_datetime())
      || (is_numeric() && other.is_numeric()))
  {
    ret = true;
  }
  return ret;
}

inline bool ObExprObj::is_numeric() const
{
  return ((type_ == ObIntType)
          || (type_ == ObDecimalType)
          || (type_ == ObFloatType)
          || (type_ == ObDoubleType));
}

inline int ObExprObj::get_timestamp(int64_t & timestamp) const
{
  int ret = OB_SUCCESS;
  switch(type_)
  {
    case ObDateTimeType:
      timestamp = v_.datetime_ * 1000 * 1000L;
      break;
    case ObPreciseDateTimeType:
      timestamp = v_.precisedatetime_;
      break;
    case ObModifyTimeType:
      timestamp = v_.modifytime_;
      break;
    case ObCreateTimeType:
      timestamp = v_.createtime_;
      break;
    default:
      TBSYS_LOG(ERROR, "unexpected branch");
      ret = OB_OBJ_TYPE_ERROR;
  }
  return ret;
}

int ObExprObj::compare_same_type(const ObExprObj &other) const
{
  int cmp = 0;
  OB_ASSERT(get_type() == other.get_type()
            || (is_datetime() && other.is_datetime()));
  switch(get_type())
  {
    case ObIntType:
      if (this->v_.int_ < other.v_.int_)
      {
        cmp = -1;
      }
      else if (this->v_.int_ == other.v_.int_)
      {
        cmp = 0;
      }
      else
      {
        cmp = 1;
      }
      break;
    case ObDecimalType:
      cmp = this->num_.compare(other.num_);
      break;
    case ObVarcharType:
      cmp = this->varchar_.compare(other.varchar_);
      break;
    case ObFloatType:
    {
      bool float_eq = fabsf(v_.float_ - other.v_.float_) < FLOAT_EPSINON;
      if (float_eq)
      {
        cmp = 0;
      }
      else if (this->v_.float_ < other.v_.float_)
      {
        cmp = -1;
      }
      else
      {
        cmp = 1;
      }
      break;
    }
    case ObDoubleType:
    {
      bool double_eq = fabs(v_.double_ - other.v_.double_) < DOUBLE_EPSINON;
      if (double_eq)
      {
        cmp = 0;
      }
      else if (this->v_.double_ < other.v_.double_)
      {
        cmp = -1;
      }
      else
      {
        cmp = 1;
      }
      break;
    }
    case ObDateTimeType:
    case ObPreciseDateTimeType:
    case ObCreateTimeType:
    case ObModifyTimeType:
    {
      int64_t ts1 = 0;
      int64_t ts2 = 0;
      get_timestamp(ts1);
      other.get_timestamp(ts2);
      if (ts1 < ts2)
      {
        cmp = -1;
      }
      else if (ts1 == ts2)
      {
        cmp = 0;
      }
      else
      {
        cmp = 1;
      }
      break;
    }
    case ObBoolType:
      cmp = this->v_.bool_ - other.v_.bool_;
      break;
    default:
      TBSYS_LOG(ERROR, "invalid type=%d", get_type());
      break;
  }
  return cmp;
}

inline int ObExprObj::type_promotion(const ObExprObj &this_obj, const ObExprObj &other,
                                     ObExprObj &promoted_obj, const ObExprObj *&p_this, const ObExprObj *&p_other)
{
  int ret = OB_SUCCESS;
  ObObjType this_type = this_obj.get_type();
  ObObjType other_type = other.get_type();
  switch(this_type)
  {
    case ObNullType:
      ret = OB_RESULT_UNKNOWN;
      break;
    case ObIntType:
    {
      switch(other_type)
      {
        case ObNullType:
          ret = OB_RESULT_UNKNOWN;
          break;
        case ObIntType:
          // same type
          p_this = &this_obj;
          p_other = &other;
          break;
        case ObFloatType:
          // int vs float
          promoted_obj.type_ = ObFloatType;
          promoted_obj.v_.float_ = static_cast<float>(this_obj.v_.int_);
          p_this = &promoted_obj;
          p_other = &other;
          break;
        case ObDoubleType:
          // int vs double
          promoted_obj.type_ = ObDoubleType;
          promoted_obj.v_.double_ = static_cast<double>(this_obj.v_.int_);
          p_this = &promoted_obj;
          p_other = &other;
          break;
        case ObDecimalType:
          // int vs decimal
          promoted_obj.type_ = ObDecimalType;
          promoted_obj.num_.from(this_obj.v_.int_);
          p_this = &promoted_obj;
          p_other = &other;
          break;
        default:
          ret = OB_INVALID_ARGUMENT;
          TBSYS_LOG(WARN, "cannot compare or do arithmetic, this_type=%d other_type=%d", this_type, other_type);
          break;
      }
      break;
    }
    case ObFloatType:
    {
      switch(other_type)
      {
        case ObNullType:
          ret = OB_RESULT_UNKNOWN;
          break;
        case ObIntType:
          // float vs int
          promoted_obj.type_ = ObFloatType;
          promoted_obj.v_.float_ = static_cast<float>(other.v_.int_);
          p_this = &this_obj;
          p_other = &promoted_obj;
          break;
        case ObFloatType:
          // same type
          p_this = &this_obj;
          p_other = &other;
          break;
        case ObDoubleType:
          // float vs double
          promoted_obj.type_ = ObDoubleType;
          promoted_obj.v_.double_ = static_cast<double>(this_obj.v_.float_);
          p_this = &promoted_obj;
          p_other = &other;
          break;
        case ObDecimalType:
          // float vs decimal
          ret = OB_NOT_IMPLEMENT;
          break;
        default:
          ret = OB_INVALID_ARGUMENT;
          TBSYS_LOG(WARN, "cannot compare or do arithmetic, this_type=%d other_type=%d", this_type, other_type);
          break;
      }
      break;
    }
    case ObDoubleType:
    {
      switch(other_type)
      {
        case ObNullType:
          ret = OB_RESULT_UNKNOWN;
          break;
        case ObIntType:
          // double vs int
          promoted_obj.type_ = ObDoubleType;
          promoted_obj.v_.double_ = static_cast<double>(other.v_.int_);
          p_this = &this_obj;
          p_other = &promoted_obj;
          break;
        case ObFloatType:
          // double vs float
          promoted_obj.type_ = ObDoubleType;
          promoted_obj.v_.double_ = static_cast<double>(other.v_.float_);
          p_this = &this_obj;
          p_other = &promoted_obj;
          break;
        case ObDoubleType:
          // same type
          p_this = &this_obj;
          p_other = &other;
          break;
        case ObDecimalType:
          // double vs decimal
          ret = OB_NOT_IMPLEMENT;
          break;
        default:
          ret = OB_INVALID_ARGUMENT;
          TBSYS_LOG(WARN, "cannot compare or do arithmetic, this_type=%d other_type=%d", this_type, other_type);
          break;
      }
      break;
    }
    case ObDecimalType:
    {
      switch(other_type)
      {
        case ObNullType:
          ret = OB_RESULT_UNKNOWN;
          break;
        case ObIntType:
          // decimal vs int
          promoted_obj.type_ = ObDecimalType;
          promoted_obj.num_.from(other.v_.int_);
          p_this = &this_obj;
          p_other = &promoted_obj;
          break;
        case ObFloatType:
          // decimal vs float
          ret = OB_NOT_IMPLEMENT;
          break;
        case ObDoubleType:
          // decimal vs double
          ret = OB_NOT_IMPLEMENT;
          break;
        case ObDecimalType:
          // same type
          p_this = &this_obj;
          p_other = &other;
          break;
        default:
          ret = OB_INVALID_ARGUMENT;
          TBSYS_LOG(WARN, "cannot compare or do arithmetic, this_type=%d other_type=%d", this_type, other_type);
          break;
      }
      break;
    }
    default:
      ret = OB_INVALID_ARGUMENT;
      TBSYS_LOG(WARN, "cannot compare or do arithmetic, this_type=%d other_type=%d", this_type, other_type);
      break;
  }
  return ret;
}

int ObExprObj::compare(const ObExprObj &other, int &cmp) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!can_compare(other)))
  {
    TBSYS_LOG(ERROR, "can not be compared, this_type=%d other_type=%d",
              get_type(), other.get_type());
    ret = OB_OBJ_TYPE_ERROR;
  }
  else
  {
    ObObjType this_type = get_type();
    ObObjType other_type = other.get_type();
    if (this_type == ObNullType || other_type == ObNullType)
    {
      ret = OB_RESULT_UNKNOWN;
    }
    // both are not null
    else if (this_type == other_type || (this->is_datetime() && other.is_datetime()))
    {
      cmp = this->compare_same_type(other);
    }
    else
    {
      OB_ASSERT(is_numeric() && other.is_numeric());
      ObExprObj promoted_value;
      const ObExprObj *p_this = NULL;
      const ObExprObj *p_other = NULL;
      if (OB_SUCCESS != (ret = type_promotion(*this, other, promoted_value, p_this, p_other)))
      {
        TBSYS_LOG(WARN, "failed to promote type, err=%d", ret);
      }
      else
      {
        cmp = p_this->compare_same_type(*p_other);
      }
    }
  }
  return ret;
}

int ObExprObj::lt(const ObExprObj &other, ObExprObj &res) const
{
  int ret = OB_SUCCESS;
  int cmp = 0;
  if (OB_SUCCESS == (ret = this->compare(other, cmp)))
  {
    res.set_bool(cmp < 0);
  }
  else
  {
    res.set_null();
  }
  return ret;
}

int ObExprObj::gt(const ObExprObj &other, ObExprObj &res) const
{
  int ret = OB_SUCCESS;
  int cmp = 0;
  if (OB_SUCCESS == (ret = this->compare(other, cmp)))
  {
    res.set_bool(cmp > 0);
  }
  else
  {
    res.set_null();
  }
  return ret;
}

int ObExprObj::ge(const ObExprObj &other, ObExprObj &res) const
{
  int ret = OB_SUCCESS;
  int cmp = 0;
  if (OB_SUCCESS == (ret = this->compare(other, cmp)))
  {
    res.set_bool(cmp >= 0);
  }
  else
  {
    res.set_null();
  }
  return ret;
}

int ObExprObj::le(const ObExprObj &other, ObExprObj &res) const
{
  int ret = OB_SUCCESS;
  int cmp = 0;
  if (OB_SUCCESS == (ret = this->compare(other, cmp)))
  {
    res.set_bool(cmp <= 0);
  }
  else
  {
    res.set_null();
  }
  return ret;
}

int ObExprObj::eq(const ObExprObj &other, ObExprObj &res) const
{
  int ret = OB_SUCCESS;
  int cmp = 0;
  if (OB_SUCCESS == (ret = this->compare(other, cmp)))
  {
    res.set_bool(cmp == 0);
  }
  else
  {
    res.set_null();
  }
  return ret;
}

int ObExprObj::ne(const ObExprObj &other, ObExprObj &res) const
{
  int ret = OB_SUCCESS;
  int cmp = 0;
  if (OB_SUCCESS == (ret = this->compare(other, cmp)))
  {
    res.set_bool(cmp != 0);
  }
  else
  {
    res.set_null();
  }
  return ret;
}

int ObExprObj::btw(const ObExprObj &v1, const ObExprObj &v2, ObExprObj &res) const
{
  int ret = OB_SUCCESS;
  int cmp = 0;
  if (OB_SUCCESS == (ret = this->compare(v1, cmp)))
  {
    if (cmp >= 0)
    {
      if (OB_SUCCESS == (ret = this->compare(v2, cmp)))
      {
        res.set_bool(cmp <= 0);
      }
      else
      {
        res.set_null();
      }
    }
    else
    {
      res.set_bool(false);
    }
  }
  else
  {
    res.set_null();
  }
  return ret;
}

int ObExprObj::not_btw(const ObExprObj &v1, const ObExprObj &v2, ObExprObj &res) const
{
  int ret = OB_SUCCESS;
  int cmp = 0;
  if (OB_SUCCESS == (ret = this->compare(v1, cmp)))
  {
    if (cmp >= 0)
    {
      if (OB_SUCCESS == (ret = this->compare(v2, cmp)))
      {
        res.set_bool(cmp > 0);
      }
      else
      {
        res.set_null();
      }
    }
    else
    {
      res.set_bool(true);
    }
  }
  else
  {
    res.set_null();
  }
  return ret;
}

int ObExprObj::is(const ObExprObj &other, ObExprObj &res) const
{
  int ret = OB_SUCCESS;
  bool left_bool = false;
  if (other.get_type() == ObNullType)
  {
    res.set_bool((get_type() == ObNullType));
  }
  else if (ObBoolType != other.get_type())
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "invalid type for is operator, type=%d", other.get_type());
    res.set_bool(false);        // res cannot be UNKNOWN according to the SQL standard
  }
  else
  {
    if (OB_SUCCESS != get_bool(left_bool))
    {
      res.set_bool(false);
    }
    else
    {
      res.set_bool(left_bool == other.v_.bool_);
    }
  }
  return ret;
}

int ObExprObj::is_not(const ObExprObj &other, ObExprObj &res) const
{
  int ret = OB_SUCCESS;
  bool left_bool = false;
  if (other.get_type() == ObNullType)
  {
    res.set_bool((get_type() != ObNullType));
  }
  else if (ObBoolType != other.get_type())
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "invalid type for is operator, type=%d", other.get_type());
    res.set_bool(false);        // res cannot be UNKNOWN according to the SQL standard
  }
  else
  {
    if (OB_SUCCESS != get_bool(left_bool))
    {
      res.set_bool(true);       // NULL is not TRUE/FALSE
    }
    else
    {
      res.set_bool(left_bool != other.v_.bool_);
    }
  }
  return ret;
}

int ObExprObj::get_bool(bool &value) const
{
  int res = OB_SUCCESS;
  switch (type_)
  {
    case ObBoolType:
      value = v_.bool_;
      break;
    case ObVarcharType:
      value = (varchar_.length() > 0);
      break;
    case ObIntType:
      value = (v_.int_ != 0);
      break;
    case ObDecimalType:
      value = !num_.is_zero();
      break;
    case ObFloatType:
      value = (fabsf(v_.float_) > FLOAT_EPSINON);
      break;
    case ObDoubleType:
      value = (fabs(v_.double_) > DOUBLE_EPSINON);
      break;
    case ObDateTimeType:
    case ObPreciseDateTimeType:
    case ObCreateTimeType:
    case ObModifyTimeType:
    {
      int64_t ts1 = 0;
      get_timestamp(ts1);
      value = (0 != ts1);
      break;
    }
    default:
      res = OB_OBJ_TYPE_ERROR;
      break;
  }
  return res;
}

int ObExprObj::land(const ObExprObj &other, ObExprObj &res) const
{
  int ret = OB_SUCCESS;
  bool left = false;
  bool right = false;
  int err1 = get_bool(left);
  int err2 = other.get_bool(right);
  switch (err1)
  {
    case OB_SUCCESS:
    {
      switch(err2)
      {
        case OB_SUCCESS:
          res.set_bool(left && right);
          break;
        default:
          if (left)
          {
            // TRUE and UNKNOWN
            res.set_null();
          }
          else
          {
            // FALSE and UNKNOWN
            res.set_bool(false);
          }
          break;
      }
      break;
    }
    default:
    {
      switch(err2)
      {
        case OB_SUCCESS:
          if (right)
          {
            // UNKNOWN and TRUE
            res.set_null();
          }
          else
          {
            // UNKNOWN and FALSE
            res.set_bool(false);
          }
          break;
        default:
          res.set_null();
          break;
      }
      break;
    }
  }
  return ret;
}

int ObExprObj::lor(const ObExprObj &other, ObExprObj &res) const
{
  int ret = OB_SUCCESS;
  bool left = false;
  bool right = false;
  int err1 = get_bool(left);
  int err2 = other.get_bool(right);
  switch (err1)
  {
    case OB_SUCCESS:
    {
      switch(err2)
      {
        case OB_SUCCESS:
          res.set_bool(left || right);
          break;
        default:
          if (left)
          {
            // TRUE or UNKNOWN
            res.set_bool(true);
          }
          else
          {
            // FALSE or UNKNOWN
            res.set_null();
          }
          break;
      }
      break;
    }
    default:
    {
      switch(err2)
      {
        case OB_SUCCESS:
          if (right)
          {
            // UNKNOWN or TRUE
            res.set_bool(true);
          }
          else
          {
            // UNKNOWN or FALSE
            res.set_null();
          }
          break;
        default:
          res.set_null();
          break;
      }
      break;
    }
  }
  return ret;
}

int ObExprObj::lnot(ObExprObj &res) const
{
  int ret = OB_SUCCESS;
  bool val = false;
  int err1 = get_bool(val);
  if (OB_SUCCESS == err1)
  {
    res.set_bool(!val);
  }
  else
  {
    res.set_null();
  }
  return ret;
}

inline int ObExprObj::add_same_type(const ObExprObj &other, ObExprObj &res) const
{
  int ret = OB_SUCCESS;
  OB_ASSERT(get_type() == other.get_type()
            && this->is_numeric());
  res.type_ = get_type();
  switch(get_type())
  {
    case ObIntType:
      res.v_.int_ = this->v_.int_ + other.v_.int_; // overflow is allowed
      break;
    case ObFloatType:
      res.v_.float_ = this->v_.float_ + other.v_.float_;
      break;
    case ObDoubleType:
      res.v_.double_ = this->v_.double_ + other.v_.double_;
      break;
    case ObDecimalType:
      ret = this->num_.add(other.num_, res.num_);
      break;
    default:
      ret = OB_ERR_UNEXPECTED;
      TBSYS_LOG(ERROR, "unexpected branch");
      break;
  }
  return ret;
}

int ObExprObj::add(ObExprObj &other, ObExprObj &res)
{
  int ret = OB_SUCCESS;
  ObExprObj promoted_value;
  const ObExprObj *p_this = NULL;
  const ObExprObj *p_other = NULL;
  if (OB_SUCCESS != (ret = type_promotion(*this, other, promoted_value, p_this, p_other)))
  {
    if (OB_RESULT_UNKNOWN == ret)
    {
      ret = OB_SUCCESS;
    }
    res.set_null();
  }
  else
  {
    ret = p_this->add_same_type(*p_other, res);
  }
  return ret;
}

inline int ObExprObj::sub_same_type(const ObExprObj &other, ObExprObj &res) const
{
  int ret = OB_SUCCESS;
  OB_ASSERT(get_type() == other.get_type()
            && this->is_numeric());
  res.type_ = get_type();
  switch(get_type())
  {
    case ObIntType:
      res.v_.int_ = this->v_.int_ - other.v_.int_; // overflow is allowed
      break;
    case ObFloatType:
      res.v_.float_ = this->v_.float_ - other.v_.float_;
      break;
    case ObDoubleType:
      res.v_.double_ = this->v_.double_ - other.v_.double_;
      break;
    case ObDecimalType:
      ret = this->num_.sub(other.num_, res.num_);
      break;
    default:
      ret = OB_ERR_UNEXPECTED;
      TBSYS_LOG(ERROR, "unexpected branch");
      break;
  }
  return ret;
}

int ObExprObj::sub(ObExprObj &other, ObExprObj &res)
{
  int ret = OB_SUCCESS;
  ObExprObj promoted_value;
  const ObExprObj *p_this = NULL;
  const ObExprObj *p_other = NULL;
  if (OB_SUCCESS != (ret = type_promotion(*this, other, promoted_value, p_this, p_other)))
  {
    if (OB_RESULT_UNKNOWN == ret)
    {
      ret = OB_SUCCESS;
    }
    res.set_null();
  }
  else
  {
    ret = p_this->sub_same_type(*p_other, res);
  }
  return ret;
}

inline int ObExprObj::mul_same_type(const ObExprObj &other, ObExprObj &res) const
{
  int ret = OB_SUCCESS;
  OB_ASSERT(get_type() == other.get_type()
            && this->is_numeric());
  res.type_ = get_type();
  switch(get_type())
  {
    case ObIntType:
      res.v_.int_ = this->v_.int_ * other.v_.int_; // overflow is allowed
      break;
    case ObFloatType:
      res.v_.float_ = this->v_.float_ * other.v_.float_;
      break;
    case ObDoubleType:
      res.v_.double_ = this->v_.double_ * other.v_.double_;
      break;
    case ObDecimalType:
      ret = this->num_.mul(other.num_, res.num_);
      break;
    default:
      ret = OB_ERR_UNEXPECTED;
      TBSYS_LOG(ERROR, "unexpected branch");
      break;
  }
  return ret;
}

int ObExprObj::mul(ObExprObj &other, ObExprObj &res)
{
  int ret = OB_SUCCESS;
  ObExprObj promoted_value;
  const ObExprObj *p_this = NULL;
  const ObExprObj *p_other = NULL;
  if (OB_SUCCESS != (ret = type_promotion(*this, other, promoted_value, p_this, p_other)))
  {
    if (OB_RESULT_UNKNOWN == ret)
    {
      ret = OB_SUCCESS;
    }
    res.set_null();
  }
  else
  {
    ret = p_this->mul_same_type(*p_other, res);
  }
  return ret;
}

inline int ObExprObj::type_promotion_for_div(const ObExprObj &this_obj, const ObExprObj &other,
                                             ObExprObj &promoted_obj1, ObExprObj &promoted_obj2,
                                             const ObExprObj *&p_this, const ObExprObj *&p_other,
                                             bool int_div_as_double)
{
  int ret = OB_SUCCESS;
  ObObjType this_type = this_obj.get_type();
  ObObjType other_type = other.get_type();
  switch(this_type)
  {
    case ObNullType:
      ret = OB_RESULT_UNKNOWN;
      break;
    case ObIntType:
    {
      switch(other_type)
      {
        case ObNullType:
          ret = OB_RESULT_UNKNOWN;
          break;
        case ObIntType:
          // same type
          if (int_div_as_double)
          {
            promoted_obj1.type_ = ObDoubleType;
            promoted_obj1.v_.double_ = static_cast<double>(this_obj.v_.int_);
            promoted_obj2.type_ = ObDoubleType;
            promoted_obj2.v_.double_ = static_cast<double>(other.v_.int_);
            p_this = &promoted_obj1;
            p_other = &promoted_obj2;
          }
          else
          {
            promoted_obj1.type_ = ObDecimalType;
            promoted_obj1.num_.from(this_obj.v_.int_);
            promoted_obj2.type_ = ObDecimalType;
            promoted_obj2.num_.from(other.v_.int_);
            p_this = &promoted_obj1;
            p_other = &promoted_obj2;
          }
          break;
        case ObFloatType:
          // int vs float
          promoted_obj1.type_ = ObFloatType;
          promoted_obj1.v_.float_ = static_cast<float>(this_obj.v_.int_);
          p_this = &promoted_obj1;
          p_other = &other;
          break;
        case ObDoubleType:
          // int vs double
          promoted_obj1.type_ = ObDoubleType;
          promoted_obj1.v_.double_ = static_cast<double>(this_obj.v_.int_);
          p_this = &promoted_obj1;
          p_other = &other;
          break;
        case ObDecimalType:
          // int vs decimal
          promoted_obj1.type_ = ObDecimalType;
          promoted_obj1.num_.from(this_obj.v_.int_);
          p_this = &promoted_obj1;
          p_other = &other;
          break;
        default:
          ret = OB_INVALID_ARGUMENT;
          TBSYS_LOG(WARN, "cannot compare or do arithmetic, this_type=%d other_type=%d", this_type, other_type);
          break;
      }
      break;
    }
    case ObFloatType:
    {
      switch(other_type)
      {
        case ObNullType:
          ret = OB_RESULT_UNKNOWN;
          break;
        case ObIntType:
          // float vs int
          promoted_obj1.type_ = ObFloatType;
          promoted_obj1.v_.float_ = static_cast<float>(other.v_.int_);
          p_this = &this_obj;
          p_other = &promoted_obj1;
          break;
        case ObFloatType:
          // same type
          p_this = &this_obj;
          p_other = &other;
          break;
        case ObDoubleType:
          // float vs double
          promoted_obj1.type_ = ObDoubleType;
          promoted_obj1.v_.double_ = static_cast<double>(this_obj.v_.float_);
          p_this = &promoted_obj1;
          p_other = &other;
          break;
        case ObDecimalType:
          // float vs decimal
          ret = OB_NOT_IMPLEMENT;
          break;
        default:
          ret = OB_INVALID_ARGUMENT;
          TBSYS_LOG(WARN, "cannot compare or do arithmetic, this_type=%d other_type=%d", this_type, other_type);
          break;
      }
      break;
    }
    case ObDoubleType:
    {
      switch(other_type)
      {
        case ObNullType:
          ret = OB_RESULT_UNKNOWN;
          break;
        case ObIntType:
          // double vs int
          promoted_obj1.type_ = ObDoubleType;
          promoted_obj1.v_.double_ = static_cast<double>(other.v_.int_);
          p_this = &this_obj;
          p_other = &promoted_obj1;
          break;
        case ObFloatType:
          // double vs float
          promoted_obj1.type_ = ObDoubleType;
          promoted_obj1.v_.double_ = static_cast<double>(other.v_.float_);
          p_this = &this_obj;
          p_other = &promoted_obj1;
          break;
        case ObDoubleType:
          // same type
          p_this = &this_obj;
          p_other = &other;
          break;
        case ObDecimalType:
          // double vs decimal
          ret = OB_NOT_IMPLEMENT;
          break;
        default:
          ret = OB_INVALID_ARGUMENT;
          TBSYS_LOG(WARN, "cannot compare or do arithmetic, this_type=%d other_type=%d", this_type, other_type);
          break;
      }
      break;
    }
    case ObDecimalType:
    {
      switch(other_type)
      {
        case ObNullType:
          ret = OB_RESULT_UNKNOWN;
          break;
        case ObIntType:
          // decimal vs int
          promoted_obj1.type_ = ObDecimalType;
          promoted_obj1.num_.from(other.v_.int_);
          p_this = &this_obj;
          p_other = &promoted_obj1;
          break;
        case ObFloatType:
          // decimal vs float
          ret = OB_NOT_IMPLEMENT;
          break;
        case ObDoubleType:
          // decimal vs double
          ret = OB_NOT_IMPLEMENT;
          break;
        case ObDecimalType:
          // same type
          p_this = &this_obj;
          p_other = &other;
          break;
        default:
          ret = OB_INVALID_ARGUMENT;
          TBSYS_LOG(WARN, "cannot compare or do arithmetic, this_type=%d other_type=%d", this_type, other_type);
          break;
      }
      break;
    }
    default:
      ret = OB_INVALID_ARGUMENT;
      TBSYS_LOG(WARN, "cannot compare or do arithmetic, this_type=%d other_type=%d", this_type, other_type);
      break;
  }
  return ret;
}

inline int ObExprObj::div_same_type(const ObExprObj &other, ObExprObj &res) const
{
  int ret = OB_SUCCESS;
  OB_ASSERT(get_type() == other.get_type()
            && this->is_numeric());
  res.type_ = get_type();
  switch(get_type())
  {
    case ObFloatType:
      res.v_.float_ = this->v_.float_ / other.v_.float_;
      break;
    case ObDoubleType:
      res.v_.double_ = this->v_.double_ / other.v_.double_;
      break;
    case ObDecimalType:
      ret = this->num_.div(other.num_, res.num_);
      break;
    default:
      ret = OB_ERR_UNEXPECTED;
      TBSYS_LOG(ERROR, "unexpected branch, type=%d", get_type());
      break;
  }
  return ret;
}

int ObExprObj::div(ObExprObj &other, ObExprObj &res, bool int_div_as_double)
{
  int ret = OB_SUCCESS;
  if(OB_UNLIKELY(other.is_zero()))
  {
    res.set_null();
    ret = OB_DIVISION_BY_ZERO;
  }
  else
  {
    ObExprObj promoted_value1;
    ObExprObj promoted_value2;
    const ObExprObj *p_this = NULL;
    const ObExprObj *p_other = NULL;
    if (OB_SUCCESS != (ret = type_promotion_for_div(*this, other, promoted_value1,
                                                    promoted_value2, p_this, p_other, int_div_as_double)))
    {
      if (OB_RESULT_UNKNOWN == ret)
      {
        ret = OB_SUCCESS;
      }
      res.set_null();
    }
    else
    {
      ret = p_this->div_same_type(*p_other, res);
    }
  }
  return ret;
}

int ObExprObj::mod(const ObExprObj &other, ObExprObj &res) const
{
  int err = OB_SUCCESS;

  /* 取模运算必须在整数之间进行，结果为整数 */
  if (ObIntType != get_type() || ObIntType != other.get_type())
  {
    err = OB_INVALID_ARGUMENT;
    res.set_null();
  }
  else if(other.is_zero())
  {
    res.set_null();
    err = OB_DIVISION_BY_ZERO;
  }
  else
  {
    res.type_ = ObIntType;
    res.v_.int_ = this->v_.int_ % other.v_.int_;
  }
  return err;
}

int ObExprObj::negate(ObExprObj &res) const
{
  int ret = OB_SUCCESS;
  res.type_ = get_type();
  switch(get_type())
  {
    case ObIntType:
      res.v_.int_ = -this->v_.int_; // overflow is allowd
      break;
    case ObFloatType:
      res.v_.float_ = -this->v_.float_;
      break;
    case ObDoubleType:
      res.v_.double_ = -this->v_.double_;
      break;
    case ObDecimalType:
      ret = this->num_.negate(res.num_);
      break;
    default:
      res.set_null();
      ret = OB_INVALID_ARGUMENT;
      break;
  }
  return ret;
}

int ObExprObj::old_like(const ObExprObj &pattern, ObExprObj &result) const
{
  int err = OB_SUCCESS;
  if (this->get_type() == ObNullType || pattern.get_type() == ObNullType)
  {
    result.set_null();
  }
  else if (ObVarcharType != this->get_type() || ObVarcharType != pattern.get_type())
  {
    err = OB_INVALID_ARGUMENT;
    result.set_null();
  }
  else if (pattern.varchar_.length() <= 0)
  {
    result.set_bool(true);
  }
  else
  {
    // TODO: 对于常量字符串，此处可以优化。无需每次计算sign
    uint64_t pattern_sign = ObStringSearch::cal_print(pattern.varchar_);
    int64_t pos = ObStringSearch::kr_search(pattern.varchar_, pattern_sign, this->varchar_);
    result.set_bool(pos >= 0);
  }
  return err;
}

int ObExprObj::like(const ObExprObj &pattern, ObExprObj &result) const
{
  int err = OB_SUCCESS;
  if (this->get_type() == ObNullType || pattern.get_type() == ObNullType)
  {
    result.set_null();
  }
  else if (ObVarcharType != this->get_type() || ObVarcharType != pattern.get_type())
  {
    err = OB_INVALID_ARGUMENT;
    result.set_null();
  }
  else if (pattern.varchar_.length() <= 0 && varchar_.length() <= 0)
  {
    // empty string
    result.set_bool(true);
  }
  else
  {
    // TODO: 对于常量字符串，此处可以优化。无需每次计算sign
    uint64_t pattern_sign = ObStringSearch::cal_print(pattern.varchar_);
    int64_t pos = ObStringSearch::kr_search(pattern.varchar_, pattern_sign, this->varchar_);
    result.set_bool(pos >= 0);
  }
  return err;
}

int ObExprObj::not_like(const ObExprObj &pattern, ObExprObj &result) const
{
  int err = OB_SUCCESS;
  if (this->get_type() == ObNullType || pattern.get_type() == ObNullType)
  {
    result.set_null();
  }
  else if (ObVarcharType != this->get_type() || ObVarcharType != pattern.get_type())
  {
    err = OB_INVALID_ARGUMENT;
    result.set_null();
  }
  else if (pattern.varchar_.length() <= 0 && varchar_.length() <= 0)
  {
    // empty string
    result.set_bool(false);
  }
  else
  {
    // TODO: 对于常量字符串，此处可以优化。无需每次计算sign
    uint64_t pattern_sign = ObStringSearch::cal_print(pattern.varchar_);
    int64_t pos = ObStringSearch::kr_search(pattern.varchar_, pattern_sign, this->varchar_);
    result.set_bool(pos < 0);
  }
  return err;
}

void ObExprObj::set_int(const int64_t value)
{
  ObObj obj;
  obj.set_int(value);
  this->assign(obj);
}

void ObExprObj::set_datetime(const ObDateTime& value)
{
  ObObj obj;
  obj.set_datetime(value);
  this->assign(obj);
}

void ObExprObj::set_precise_datetime(const ObPreciseDateTime& value)
{
  ObObj obj;
  obj.set_precise_datetime(value);
  this->assign(obj);
}

void ObExprObj::set_varchar(const ObString& value)
{
  ObObj obj;
  obj.set_varchar(value);
  this->assign(obj);
}

void ObExprObj::set_modifytime(const ObModifyTime& value)
{
  ObObj obj;
  obj.set_modifytime(value);
  this->assign(obj);
}

void ObExprObj::set_createtime(const ObCreateTime& value)
{
  ObObj obj;
  obj.set_createtime(value);
  this->assign(obj);
}

int ObExprObj::set_decimal(const char* dec_str)
{
  int ret = OB_SUCCESS;
  ObObj obj;
  ObNumber num;
  static const int8_t TEST_PREC = 38;
  static const int8_t TEST_SCALE = 4;
  if (OB_SUCCESS != (ret = num.from(dec_str)))
  {
    TBSYS_LOG(WARN, "failed to construct decimal from string, err=%d str=%s", ret, dec_str);
  }
  else if (OB_SUCCESS != (ret = obj.set_decimal(num, TEST_PREC, TEST_SCALE)))
  {
    TBSYS_LOG(WARN, "obj failed to set decimal, err=%d", ret);
  }
  else
  {
    this->assign(obj);
  }
  return ret;
}

void ObExprObj::set_float(const float value)
{
  ObObj obj;
  obj.set_float(value);
  this->assign(obj);
}

void ObExprObj::set_double(const double value)
{
  ObObj obj;
  obj.set_double(value);
  this->assign(obj);
}

void ObExprObj::set_varchar(const char* value)
{
  ObString str;
  str.assign_ptr(const_cast<char*>(value), static_cast<int32_t>(strlen(value)));
  ObObj obj;
  obj.set_varchar(str);
  this->assign(obj);
}

int ObExprObj::get_int(int64_t& value) const
{
  int ret = OB_SUCCESS;
  ObObj obj;
  if (OB_SUCCESS != (ret = this->to(obj)))
  {
    TBSYS_LOG(WARN, "failed to convert to obj, err=%d", ret);
  }
  else
  {
    ret = obj.get_int(value);
  }
  return ret;
}

int ObExprObj::get_datetime(ObDateTime& value) const
{
  int ret = OB_SUCCESS;
  ObObj obj;
  if (OB_SUCCESS != (ret = this->to(obj)))
  {
    TBSYS_LOG(WARN, "failed to convert to obj, err=%d", ret);
  }
  else
  {
    ret = obj.get_datetime(value);
  }
  return ret;
}

int ObExprObj::get_precise_datetime(ObPreciseDateTime& value) const
{
  int ret = OB_SUCCESS;
  ObObj obj;
  if (OB_SUCCESS != (ret = this->to(obj)))
  {
    TBSYS_LOG(WARN, "failed to convert to obj, err=%d", ret);
  }
  else
  {
    ret = obj.get_precise_datetime(value);
  }
  return ret;
}

int ObExprObj::get_varchar(ObString& value) const
{
  int ret = OB_SUCCESS;
  ObObj obj;
  if (OB_SUCCESS != (ret = this->to(obj)))
  {
    TBSYS_LOG(WARN, "failed to convert to obj, err=%d", ret);
  }
  else
  {
    ret = obj.get_varchar(value);
  }
  return ret;
}

int ObExprObj::get_float(float &f) const
{
  int ret = OB_SUCCESS;
  ObObj obj;
  if (OB_SUCCESS != (ret = this->to(obj)))
  {
    TBSYS_LOG(WARN, "failed to convert to obj, err=%d", ret);
  }
  else
  {
    ret = obj.get_float(f);
  }
  return ret;
}

int ObExprObj::get_double(double &d) const
{
  int ret = OB_SUCCESS;
  ObObj obj;
  if (OB_SUCCESS != (ret = this->to(obj)))
  {
    TBSYS_LOG(WARN, "failed to convert to obj, err=%d", ret);
  }
  else
  {
    ret = obj.get_double(d);
  }
  return ret;
}

int ObExprObj::get_decimal(char * buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  if (type_ != ObDecimalType)
  {
    ret = OB_OBJ_TYPE_ERROR;
  }
  else
  {
    num_.to_string(buf, buf_len);
  }
  return ret;
}

bool ObExprObj::is_null() const
{
  bool ret = false;
  int err = OB_SUCCESS;
  ObObj obj;
  if (OB_SUCCESS != (err = this->to(obj)))
  {
    TBSYS_LOG(WARN, "failed to convert to obj, err=%d", err);
  }
  else
  {
    ret = obj.is_null();
  }
  return ret;
}

int ObExprObj::varchar_length(ObExprObj &res) const
{
  int ret = OB_SUCCESS;
  if (ObVarcharType != get_type())
  {
    ret = OB_INVALID_ARGUMENT;
    res.set_null();
  }
  else
  {
    res.type_ = ObIntType;
    res.v_.int_ = varchar_.length();
  }
  return ret;
}

ObObj ObExprObj::type_arithmetic(const ObObj& t1, const ObObj& t2)
{
  ObObj ret;
  ret.meta_.type_ = ObNullType;
  int err = OB_SUCCESS;
  ObExprObj v1;
  v1.type_ = t1.get_type();
  ObExprObj v2;
  v2.type_ = t2.get_type();
  ObExprObj promoted_value;
  const ObExprObj *p_this = NULL;
  const ObExprObj *p_other = NULL;
  if (OB_SUCCESS != (err = type_promotion(v1, v2, promoted_value, p_this, p_other)))
  {
    TBSYS_LOG(WARN, "failed to promote type, err=%d", err);
  }
  else
  {
    ret.meta_.type_ = p_this->type_;
    if (ObDecimalType == p_this->type_)
    {
      // @todo decimal precision and scale
    }
  }
  return ret;
}

ObObj ObExprObj::type_add(const ObObj& t1, const ObObj& t2)
{
  return type_arithmetic(t1, t2);
}

ObObj ObExprObj::type_sub(const ObObj& t1, const ObObj& t2)
{
  return type_arithmetic(t1, t2);
}

ObObj ObExprObj::type_mul(const ObObj& t1, const ObObj& t2)
{
  return type_arithmetic(t1, t2);
}

ObObj ObExprObj::type_div(const ObObj& t1, const ObObj& t2, bool int_div_as_double)
{
  ObObj ret;
  ret.meta_.type_ = ObNullType;
  int err = OB_SUCCESS;
  ObExprObj v1;
  v1.type_ = t1.get_type();
  ObExprObj v2;
  v2.type_ = t2.get_type();
  ObExprObj promoted_value1;
  ObExprObj promoted_value2;
  const ObExprObj *p_this = NULL;
  const ObExprObj *p_other = NULL;
  if (OB_SUCCESS != (err = type_promotion_for_div(v1, v2, promoted_value1, promoted_value2,
                                                  p_this, p_other, int_div_as_double)))
  {
    TBSYS_LOG(WARN, "failed to promote type, err=%d", err);
  }
  else
  {
    ret.meta_.type_ = p_this->type_;
    if (ObDecimalType == p_this->type_)
    {
      // @todo decimal precision and scale
    }
  }
  return ret;
}

ObObj ObExprObj::type_mod(const ObObj& t1, const ObObj& t2)
{
  ObObj ret;
  ret.meta_.type_ = ObNullType;
  if (ObIntType == t1.get_type() && ObIntType == t2.get_type())
  {
    ret.meta_.type_ = ObIntType;
  }
  else
  {
    TBSYS_LOG(WARN, "not supported type for mod, t1=%d t2=%d", t1.get_type(), t2.get_type());
  }
  return ret;
}

ObObj ObExprObj::type_negate(const ObObj& t1)
{
  ObObj ret;
  ret.meta_.type_ = ObNullType;
  switch(t1.get_type())
  {
    case ObIntType:
    case ObFloatType:
    case ObDoubleType:
    case ObDecimalType:
      ret.meta_.type_ = static_cast<uint8_t>(t1.get_type());
      break;
    default:
      TBSYS_LOG(WARN, "not supported type for negate, type=%d", t1.get_type());
      break;
  }
  return ret;
}

ObObj ObExprObj::type_varchar_length(const ObObj& t1)
{
  ObObj ret;
  ret.meta_.type_ = ObNullType;
  if (ObVarcharType != t1.get_type())
  {
    TBSYS_LOG(WARN, "not supported type for varchar_length, type=%d", t1.get_type());
  }
  else
  {
    ret.meta_.type_ = ObIntType;
  }
  return ret;
}
