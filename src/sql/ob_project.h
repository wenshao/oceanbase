/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_project.h
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#ifndef _OB_PROJECT_H
#define _OB_PROJECT_H 1
#include "ob_single_child_phy_operator.h"
#include "ob_sql_expression.h"
#include "common/ob_array.h"

namespace oceanbase
{
  namespace sql
  {
    class ObProject: public ObSingleChildPhyOperator
    {
      public:
        ObProject();
        virtual ~ObProject();
        void reset(){};

        int add_output_column(const ObSqlExpression& expr);
        virtual int open();
        virtual int close();
        virtual int get_next_row(const common::ObRow *&row);
        virtual int get_row_desc(const common::ObRowDesc *&row_desc) const;
        virtual int64_t to_string(char* buf, const int64_t buf_len) const;
        void assign(const ObProject &other);

        NEED_SERIALIZE_AND_DESERIALIZE;
      private:
        int cons_row_desc();
        // disallow copy
        ObProject(const ObProject &other);
        ObProject& operator=(const ObProject &other);
      protected:
        // data members
        common::ObArray<ObSqlExpression> columns_;
        common::ObRowDesc row_desc_;
        common::ObRow row_;
    };
    /*
    inline const common::ObArray<ObSqlExpression> &ObProject::get_output_columns() const
    {
      return columns_;
    }
    */
  } // end namespace sql
} // end namespace oceanbase

#endif /* _OB_PROJECT_H */
