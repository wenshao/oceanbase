/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_single_child_phy_operator.h
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#ifndef _OB_SINGLE_CHILD_PHY_OPERATOR_H
#define _OB_SINGLE_CHILD_PHY_OPERATOR_H 1
#include "ob_phy_operator.h"
namespace oceanbase
{
  namespace sql
  {
    class ObSingleChildPhyOperator: public ObPhyOperator
    {
      public:
        ObSingleChildPhyOperator();
        virtual ~ObSingleChildPhyOperator();
        /// set the only one child
        virtual int set_child(int32_t child_idx, ObPhyOperator &child_operator);
        /// open child_op_
        virtual int open();
        /// close child_op_
        virtual int close();
      private:
        // disallow copy
        ObSingleChildPhyOperator(const ObSingleChildPhyOperator &other);
        ObSingleChildPhyOperator& operator=(const ObSingleChildPhyOperator &other);
      protected:
        // data members
        ObPhyOperator *child_op_;
    };
  } // end namespace sql
} // end namespace oceanbase

#endif /* _OB_SINGLE_CHILD_PHY_OPERATOR_H */

