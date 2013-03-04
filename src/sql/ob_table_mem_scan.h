/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_table_scan.h
 *
 * Authors:
 *   Yu Huang <xiaochu.yh@taobao.com>
 *
 */
#ifndef _OB_TABLE_MEM_SCAN_H
#define _OB_TABLE_MEM_SCAN_H 1
#include "ob_table_scan.h"
#include "ob_sql_expression.h"
#include "ob_rename.h"
#include "ob_project.h"
#include "ob_filter.h"
#include "ob_limit.h"
#include "common/ob_row.h"
namespace oceanbase
{
  namespace sql
  {
    class ObTableMemScan: public ObTableScan
    {
      public:
        ObTableMemScan();
        virtual ~ObTableMemScan();

        virtual int open();
        virtual int close();
        virtual int get_next_row(const common::ObRow *&row);
        virtual int get_row_desc(const common::ObRowDesc *&row_desc) const;
        virtual int64_t to_string(char* buf, const int64_t buf_len) const;

        /**
         * 添加一个需输出的column
         *
         * @note 只有通过复合列结算新生成的列才需要new_column_id
         * @param expr [in] 需输出的列（这个列可能是个复合列的结果）
         *
         * @return OB_SUCCESS或错误码
         */
        int add_output_column(const ObSqlExpression& expr);

        /**
         * 设置table_id
         * @note 只有基本表被重命名的情况才会使两个不相同id，其实两者相同时base_table_id可以给个默认值。
         * @param table_id [in] 输出的table_id
         * @param base_table_id [in] 被访问表的id
         *
         * @return OB_SUCCESS或错误码
         */
        int set_table(const uint64_t table_id, const uint64_t base_table_id);

        /**
         * 添加一个filter
         *
         * @param expr [in] 过滤表达式
         *
         * @return OB_SUCCESS或错误码
         */
        int add_filter(const ObSqlExpression& expr);

        /**
         * 指定limit/offset
         *
         * @param limit [in]
         * @param offset [in]
         *
         * @return OB_SUCCESS或错误码
         */
        int set_limit(const int64_t limit, const int64_t offset);
      private:
        // disallow copy
        ObTableMemScan(const ObTableMemScan &other);
        ObTableMemScan& operator=(const ObTableMemScan &other);
      private:
        // data members
        ObRename rename_;
        ObProject project_;
        ObFilter filter_;
        ObLimit limit_;
        bool has_rename_;
        bool has_project_;
        bool has_filter_;
        bool has_limit_;
    };
  } // end namespace sql
} // end namespace oceanbase

#endif /* _OB_TABLE_MEM_SCAN_H */
