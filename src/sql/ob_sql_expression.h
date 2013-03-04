/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_sql_expression.h
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#ifndef _OB_SQL_EXPRESSION_H
#define _OB_SQL_EXPRESSION_H 1
#include "common/ob_object.h"
#include "ob_postfix_expression.h"
#include "common/ob_row.h"
class ObAggregateFunctionTest;

namespace oceanbase
{
  namespace sql
  {
    class ObSqlExpression
    {
      public:
        ObSqlExpression();
        virtual ~ObSqlExpression();

        ObSqlExpression(const ObSqlExpression &other);
        ObSqlExpression& operator=(const ObSqlExpression &other);

        void set_int_div_as_double(bool did);

        void set_tid_cid(const uint64_t tid, const uint64_t cid);
        const uint64_t get_column_id() const;
        const uint64_t get_table_id() const;

        void set_aggr_func(ObItemType aggr_fun, bool is_distinct);
        int get_aggr_column(ObItemType &aggr_fun, bool &is_distinct) const;
        /**
         * 设置表达式
         * @param expr [in] 表达式，表达方式与实现相关，目前定义为后缀表达式
         *
         * @return error code
         */
        int add_expr_item(const ExprItem &item);
        int add_expr_item_end();

        /**
         * 获取解码后的表达式
         */
        inline const ObPostfixExpression &get_decoded_expression() const;
        inline bool is_equijoin_cond(ExprItem::SqlCellInfo &c1, ExprItem::SqlCellInfo &c2) const;
        /**
         * 根据表达式语义对row的值进行计算
         *
         * @param row [in] 输入行
         * @param result [out] 计算结果
         *
         * @return error code
         */
        int calc(const common::ObRow &row, const common::ObObj *&result);
        /// 打印表达式
        int64_t to_string(char* buf, const int64_t buf_len) const;

        NEED_SERIALIZE_AND_DESERIALIZE;
      private:
        friend class ::ObAggregateFunctionTest;
        // data members
        ObPostfixExpression post_expr_;
        uint64_t column_id_;
        uint64_t table_id_;
        bool is_aggr_func_;
        bool is_distinct_;
        ObItemType aggr_func_;
      private:
        // method
        int serialize_basic_param(char* buf, const int64_t buf_len, int64_t& pos) const;
        int deserialize_basic_param(const char* buf, const int64_t data_len, int64_t& pos);
        int64_t get_basic_param_serialize_size(void) const;
    };

    inline void ObSqlExpression::set_int_div_as_double(bool did)
    {
      post_expr_.set_int_div_as_double(did);
    }

    inline void ObSqlExpression::set_tid_cid(const uint64_t tid, const uint64_t cid)
    {
      table_id_ = tid;
      column_id_ = cid;
    }

    inline const uint64_t ObSqlExpression::get_column_id() const
    {
      return column_id_;
    }

    inline const uint64_t ObSqlExpression::get_table_id() const
    {
      return table_id_;
    }

    inline int ObSqlExpression::get_aggr_column(ObItemType &aggr_fun, bool &is_distinct) const
    {
      int ret = OB_SUCCESS;
      if (!is_aggr_func_)
      {
        ret = OB_ERR_UNEXPECTED;
        TBSYS_LOG(ERROR, "this expression is not an aggr function");
      }
      else
      {
        aggr_fun = aggr_func_;
        is_distinct = is_distinct_;
      }
      return ret;
    }

    inline void ObSqlExpression::set_aggr_func(ObItemType aggr_func, bool is_distinct)
    {
      is_aggr_func_ = true;
      aggr_func_ = aggr_func;
      is_distinct_ = is_distinct;
    }

    inline const ObPostfixExpression &ObSqlExpression::get_decoded_expression() const
    {
      return post_expr_;
    }

    inline bool ObSqlExpression::is_equijoin_cond(ExprItem::SqlCellInfo &c1, ExprItem::SqlCellInfo &c2) const
    {
      return post_expr_.is_equijoin_cond(c1, c2);
    }

  } // end namespace sql
} // end namespace oceanbase

#endif /* _OB_SQL_EXPRESSION_H */
