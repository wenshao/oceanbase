/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_filter.h
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#ifndef _OB_FILTER_H
#define _OB_FILTER_H 1
#include "ob_single_child_phy_operator.h"
#include "ob_sql_expression.h"
#include "common/ob_array.h"

namespace oceanbase
{
  namespace sql
  {
    class ObFilter: public ObSingleChildPhyOperator
    {
      public:
        ObFilter();
        virtual ~ObFilter();
        void reset(){};
        /**
         * 添加一个filter
         * 多个filter之间为AND关系
         * @param expr [in] 过滤表达式
         *
         * @return OB_SUCCESS或错误码
         */
        int add_filter(const ObSqlExpression& expr);
        virtual int open();
        virtual int close();
        virtual int get_next_row(const common::ObRow *&row);
        virtual int get_row_desc(const common::ObRowDesc *&row_desc) const;
        virtual int64_t to_string(char* buf, const int64_t buf_len) const;
        void assign(const ObFilter &other);

        NEED_SERIALIZE_AND_DESERIALIZE;
      private:
        // disallow copy
        ObFilter(const ObFilter &other);
        ObFilter& operator=(const ObFilter &other);
      private:
        // data members
        common::ObArray<ObSqlExpression> filters_;
    };
  } // end namespace sql
} // end namespace oceanbase

#endif /* _OB_FILTER_H */
