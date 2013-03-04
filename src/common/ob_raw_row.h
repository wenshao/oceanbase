/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_raw_row.h
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#ifndef _OB_RAW_ROW_H
#define _OB_RAW_ROW_H 1
#include "common/ob_define.h"
#include "common/ob_object.h"

namespace oceanbase
{
  namespace common
  {
    class ObRawRow
    {
      public:
        ObRawRow();
        ~ObRawRow();

        void assign(const ObRawRow &other);
        void clear();
        int add_cell(const common::ObObj &cell);

        int64_t get_cells_count() const;
        int get_cell(const int64_t i, const common::ObObj *&cell) const;
        int get_cell(const int64_t i, common::ObObj *&cell);
        int set_cell(const int64_t i, const common::ObObj &cell);
      private:
        // types and constants
        static const int64_t MAX_COLUMNS_COUNT = common::OB_ROW_MAX_COLUMNS_COUNT;
      private:
        // disallow copy
        ObRawRow(const ObRawRow &other);
        ObRawRow& operator=(const ObRawRow &other);
        // function members
      private:
        // data members
        common::ObObj cells_[MAX_COLUMNS_COUNT];
        int16_t cells_count_;
        int16_t reserved1_;
        int32_t reserved2_;
    };

    inline int64_t ObRawRow::get_cells_count() const
    {
      return cells_count_;
    }

    inline void ObRawRow::clear()
    {
      cells_count_ = 0;
    }
  } // end namespace common
} // end namespace oceanbase

#endif /* _OB_RAW_ROW_H */

