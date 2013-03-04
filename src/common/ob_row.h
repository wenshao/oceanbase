/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_row.h
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#ifndef _OB_ROW_H
#define _OB_ROW_H 1
#include "common/ob_array.h"
#include "ob_raw_row.h"
#include "ob_row_desc.h"

namespace oceanbase
{
  namespace common
  {
    /// 行
    class ObRow
    {
      public:
        ObRow();
        virtual ~ObRow();

        /// 赋值。浅拷贝，特别的，对于varchar类型不拷贝串内容
        void assign(const ObRow &other);
        ObRow(const ObRow &other);
        ObRow &operator= (const ObRow &other);
        /**
         * 设置行描述
         * 由物理操作符在构造一个新行对象时初始化。ObRow的功能要使用到ObRowDesc的功能，多个ObRow对象可以共享一个ObRowDesc对象。
         * @param row_desc
         */
        void set_row_desc(const ObRowDesc &row_desc);
        /**
         * 获取行描述
         */
        const ObRowDesc* get_row_desc() const;
        /**
         * 根据表ID和列ID获得cell
         *
         * @param table_id
         * @param column_id
         * @param cell [out]
         *
         * @return
         */
        int get_cell(const uint64_t table_id, const uint64_t column_id, const common::ObObj *&cell) const;
        int get_cell(const uint64_t table_id, const uint64_t column_id, common::ObObj *&cell);
        /**
         * 设置指定列的值
         */
        int set_cell(const uint64_t table_id, const uint64_t column_id, const common::ObObj &cell);
        /**
         * 组成本行的列元素数
         */
        int64_t get_column_num() const;
        /**
         * 获得第cell_idx个cell
         */
        int raw_get_cell(const int64_t cell_idx, const common::ObObj *&cell, uint64_t &table_id, uint64_t &column_id) const;
        /// 设置第cell_idx个cell
        int raw_set_cell(const int64_t cell_idx, const common::ObObj &cell);

        /**
         * 比较两个row的大小
         * @param row
         * @return
         * (1) if (this == row) return 0;
         * (2) if (this >  row) return POSITIVE_VALUE;
         * (3) if (this <  row) return NEGATIVE_VALUE;
         */
        int compare(const ObRow &row) const;

        /* dump row data */
        void dump() const;

        friend class ObRowFuse;
      private:
        ObRawRow raw_row_;
        const ObRowDesc *row_desc_;
    };

    inline const ObRowDesc* ObRow::get_row_desc() const
    {
      return row_desc_;
    }

  } // end namespace common
} // end namespace oceanbase

#endif /* _OB_ROW_H */

