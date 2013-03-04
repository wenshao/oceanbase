/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_expr_obj.h
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#ifndef _OB_EXPR_OBJ_H
#define _OB_EXPR_OBJ_H 1
#include "ob_object.h"
class ObExprObj_Math_Test;
class ObExprObj_negate_test_Test;
class ObExprObjTest_get_bool_Test;
class ObExprObjTest_compare_Test;
class ObExprObjTest_like_Test;
class ObExprObjTest_others_Test;
namespace oceanbase
{
  namespace common
  {
    class ObExprObj
    {
      public:
        ObExprObj();
        ~ObExprObj();

        void assign(const ObObj &obj);
        int to(ObObj &obj) const;

        // getters & setters
        void set_int(const int64_t value); // for ob_aggragate_function
        void set_varchar(const ObString& value); // for ob_aggragate_function
        void set_bool(const bool value);

        ObObjType get_type() const;
        bool is_zero() const;
        bool is_null() const;
        bool is_true() const;
        bool is_false() const;
        int get_int(int64_t& value) const;
        int get_varchar(ObString& value) const;

        int compare(const ObExprObj &other, int &cmp) const;

        // compare operators
        ///@note the return code of these functions can be ignored
        int lt(const ObExprObj &other, ObExprObj &res) const;
        int gt(const ObExprObj &other, ObExprObj &res) const;
        int le(const ObExprObj &other, ObExprObj &res) const;
        int ge(const ObExprObj &other, ObExprObj &res) const;
        int eq(const ObExprObj &other, ObExprObj &res) const;
        int ne(const ObExprObj &other, ObExprObj &res) const;
        int btw(const ObExprObj &v1, const ObExprObj &v2, ObExprObj &res) const;
        int not_btw(const ObExprObj &v1, const ObExprObj &v2, ObExprObj &res) const;
        /// is true/false/null(unknown)
        int is(const ObExprObj &other, ObExprObj &res) const;
        /// is not true/false/null(unknown)
        int is_not(const ObExprObj &other, ObExprObj &res) const;

        /// logical and
        int land(const ObExprObj &other, ObExprObj &res) const;
        /// logical or
        int lor(const ObExprObj &other, ObExprObj &res) const;
        /// logical not
        int lnot(ObExprObj &res) const;

        // numeric arithmetic operators
        int add(ObExprObj &other, ObExprObj &res);
        int sub(ObExprObj &other, ObExprObj &res);
        int mul(ObExprObj &other, ObExprObj &res);
        int div(ObExprObj &other, ObExprObj &res, bool int_div_as_double);
        int mod(const ObExprObj &other, ObExprObj &res) const;
        int negate(ObExprObj &res) const;

        /// string like
        int old_like(const ObExprObj &other, ObExprObj &result) const; // compatible with oceanbase 0.3
        int like(const ObExprObj &other, ObExprObj &result) const;
        int not_like(const ObExprObj &other, ObExprObj &result) const;
        int varchar_length(ObExprObj &res) const;

        // result type of calculations
        // all supported operations not listed here return ObBoolType
        static ObObj type_add(const ObObj& t1, const ObObj& t2);
        static ObObj type_sub(const ObObj& t1, const ObObj& t2);
        static ObObj type_mul(const ObObj& t1, const ObObj& t2);
        static ObObj type_div(const ObObj& t1, const ObObj& t2, bool int_div_as_double);
        static ObObj type_mod(const ObObj& t1, const ObObj& t2);
        static ObObj type_negate(const ObObj& t1);
        static ObObj type_varchar_length(const ObObj& t1);
      private:
        // function members
        int get_bool(bool &value) const;
        int get_timestamp(int64_t & timestamp) const;
        bool is_datetime() const;
        bool is_numeric() const;
        bool can_compare(const ObExprObj & other) const;
        int compare_same_type(const ObExprObj &other) const;
        static int type_promotion(const ObExprObj &this_obj, const ObExprObj &other,
                                  ObExprObj &promoted_obj, const ObExprObj *&p_this_obj, const ObExprObj *&p_other);
        int add_same_type(const ObExprObj &other, ObExprObj &res) const;
        int sub_same_type(const ObExprObj &other, ObExprObj &res) const;
        int mul_same_type(const ObExprObj &other, ObExprObj &res) const;
        static int type_promotion_for_div(const ObExprObj &this_obj, const ObExprObj &other,
                                          ObExprObj &promoted_obj1, ObExprObj &promoted_obj2,
                                          const ObExprObj *&p_this, const ObExprObj *&p_other,
                                          bool int_div_as_double);
        int div_same_type(const ObExprObj &other, ObExprObj &res) const;
        void set_null();
        static ObObj type_arithmetic(const ObObj& t1, const ObObj& t2);
        // functions for testing only
        friend class ::ObExprObj_Math_Test;
        friend class ::ObExprObj_negate_test_Test;
        friend class ::ObExprObjTest_get_bool_Test;
        friend class ::ObExprObjTest_compare_Test;
        friend class ::ObExprObjTest_like_Test;
        friend class ::ObExprObjTest_others_Test;
        void set_null(int null); // for testing only
        void set_varchar(const char* value);
        void set_datetime(const ObDateTime& value);
        void set_precise_datetime(const ObPreciseDateTime& value);
        void set_modifytime(const ObModifyTime& value);
        void set_createtime(const ObCreateTime& value);
        int set_decimal(const char* dec_str);
        void set_float(const float value);
        void set_double(const double value);
        int get_datetime(ObDateTime& value) const;
        int get_precise_datetime(ObPreciseDateTime& value) const;
        int get_decimal(char * buf, const int64_t buf_len) const;
        int get_float(float &f) const;
        int get_double(double &d) const;
      private:
        // data members
        int8_t type_;           // ObObjType
        ObNumber num_;
        ObString varchar_;
        union
        {
          int64_t int_;
          ObDateTime datetime_;
          ObPreciseDateTime precisedatetime_;
          ObCreateTime createtime_;
          ObModifyTime modifytime_;
          bool bool_;
          float float_;
          double double_;
        } v_;
    };

    inline ObExprObj::ObExprObj()
      :type_(ObNullType)
    {
      v_.int_ = 0;
    }

    inline ObExprObj::~ObExprObj()
    {
    }

    inline void ObExprObj::set_bool(const bool value)
    {
      type_ = ObBoolType;
      v_.bool_ = value;
    }

    inline void ObExprObj::set_null()
    {
      type_ = ObNullType;
    }

    inline void ObExprObj::set_null(int ignore)
    {
      UNUSED(ignore);
      type_ = ObNullType;
    }

    inline bool ObExprObj::is_zero() const
    {
      bool result = false;
      if (((type_ == ObIntType) && (v_.int_ == 0))
          || ((type_ == ObDecimalType) && num_.is_zero())
          || (type_ == ObFloatType && v_.float_ == 0.0f)
          || (type_ == ObDoubleType && v_.double_ == 0.0))
      {
        result = true;
      }
      return result;
    }

    inline ObObjType ObExprObj::get_type() const
    {
      return static_cast<ObObjType>(type_);
    }

    inline bool ObExprObj::is_true() const
    {
      return type_ == ObBoolType && v_.bool_;
    }

    inline bool ObExprObj::is_false() const
    {
      return type_ == ObBoolType && !v_.bool_;
    }
  } // end namespace common
} // end namespace oceanbase

#endif /* _OB_EXPR_OBJ_H */
