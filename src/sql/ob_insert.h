/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_insert.h
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#ifndef _OB_INSERT_H
#define _OB_INSERT_H 1
#include "sql/ob_single_child_phy_operator.h"
#include "sql/ob_no_rows_phy_operator.h"
#include "common/ob_mutator.h"
namespace oceanbase
{
  namespace sql
  {
    class ObInsert: public ObSingleChildPhyOperator, public ObNoRowsPhyOperator
    {
      public:
        ObInsert();
        virtual ~ObInsert();

        void set_table_id(const uint64_t tid);
        // set insert columns
        int set_row_desc(const common::ObRowDesc &row_desc);
        // @todo add this interface after merging the formed_rowkey branch
        // int set_rowkey_info(const common::ObRowkeyInfo &rowkey_info);

        /// execute the insert statement
        virtual int open();
        virtual int close();
        virtual int64_t to_string(char* buf, const int64_t buf_len) const;
      private:
        // types and constants
      private:
        // disallow copy
        ObInsert(const ObInsert &other);
        ObInsert& operator=(const ObInsert &other);
        // function members
        int insert_by_mutator();
      private:
        // data members
        uint64_t table_id_;
        common::ObRowDesc row_desc_;
        common::ObMutator mutator_;
    };

    inline void ObInsert::set_table_id(const uint64_t tid)
    {
      table_id_ = tid;
    }

    inline int ObInsert::set_row_desc(const common::ObRowDesc &row_desc)
    {
      row_desc_ = row_desc;
      return common::OB_SUCCESS;
    }

  } // end namespace sql
} // end namespace oceanbase

#endif /* _OB_INSERT_H */
