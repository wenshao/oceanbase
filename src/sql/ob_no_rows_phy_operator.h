/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_no_rows_phy_operator.h
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#ifndef _OB_NO_ROWS_PHY_OPERATOR_H
#define _OB_NO_ROWS_PHY_OPERATOR_H 1
#include "sql/ob_phy_operator.h"
namespace oceanbase
{
  namespace sql
  {
    class ObNoRowsPhyOperator: public ObPhyOperator
    {
      public:
        ObNoRowsPhyOperator();
        virtual ~ObNoRowsPhyOperator();

        /// @note always return OB_ITER_END
        virtual int get_next_row(const common::ObRow *&row);
        /// @note always return OB_NOT_SUPPORTED
        virtual int get_row_desc(const common::ObRowDesc *&row_desc) const;
      private:
        // types and constants
      private:
        // disallow copy
        ObNoRowsPhyOperator(const ObNoRowsPhyOperator &other);
        ObNoRowsPhyOperator& operator=(const ObNoRowsPhyOperator &other);
        // function members
      private:
        // data members
    };

    inline int ObNoRowsPhyOperator::get_next_row(const common::ObRow *&row)
    {
      row = NULL;
      return common::OB_ITER_END;
    }

    inline int ObNoRowsPhyOperator::get_row_desc(const common::ObRowDesc *&row_desc) const
    {
      row_desc = NULL;
      return common::OB_NOT_SUPPORTED;
    }

  } // end namespace sql
} // end namespace oceanbase

#endif /* _OB_NO_ROWS_PHY_OPERATOR_H */
