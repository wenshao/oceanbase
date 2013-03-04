/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_scalar_aggregate.h
 *
 * Authors:
 *   Guibin Du <tianguan.dgb@taobao.com>
 *
 */
#ifndef _OB_SCALAR_AGGREGATE_H
#define _OB_SCALAR_AGGREGATE_H
#include "sql/ob_merge_groupby.h"
#include "sql/ob_single_child_phy_operator.h"
namespace oceanbase
{
  namespace sql
  {
    class ObScalarAggregate: public ObSingleChildPhyOperator
    {
      public:
        ObScalarAggregate();
        virtual ~ObScalarAggregate();

        virtual int open();
        virtual int close();
        virtual int get_next_row(const ObRow *&row);
        virtual int get_row_desc(const common::ObRowDesc *&row_desc) const;
        /**
         * 添加一个聚集表达式
         *
         * @param expr [in] 聚集函数表达式
         *
         * @return OB_SUCCESS或错误码
         */
        virtual int add_aggr_column(ObSqlExpression& expr);
        virtual int64_t to_string(char* buf, const int64_t buf_len) const;
      private:
        // disallow copy
        ObScalarAggregate(const ObScalarAggregate &other);
        ObScalarAggregate& operator=(const ObScalarAggregate &other);
      private:
        ObMergeGroupBy merge_groupby_; // use MergeGroupBy to implement this operator
    };
  } // end namespace sql
} // end namespace oceanbase

#endif /* _OB_SCALAR_AGGREGATE_H */
