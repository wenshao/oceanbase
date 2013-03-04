/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_values.h
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#ifndef _OB_VALUES_H
#define _OB_VALUES_H 1

#include "sql/ob_no_children_phy_operator.h"
#include "common/ob_row_store.h"
namespace oceanbase
{
  namespace sql
  {
    class ObValues: public ObNoChildrenPhyOperator
    {
      public:
        ObValues();
        virtual ~ObValues();

        int set_row_desc(const common::ObRowDesc &row_desc);
        int add_values(const common::ObRow &value);

        virtual int open();
        virtual int close();
        virtual int get_next_row(const common::ObRow *&row);
        virtual int get_row_desc(const common::ObRowDesc *&row_desc) const;
        virtual int64_t to_string(char* buf, const int64_t buf_len) const;
      private:
        // types and constants
      private:
        // disallow copy
        ObValues(const ObValues &other);
        ObValues& operator=(const ObValues &other);
        // function members
      private:
        // data members
        common::ObRowDesc row_desc_;
        common::ObRow curr_row_;
        common::ObRowStore row_store_;
    };
  } // end namespace sql
} // end namespace oceanbase

#endif /* _OB_VALUES_H */
