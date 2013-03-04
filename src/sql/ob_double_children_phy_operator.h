/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_double_children_phy_operator.h
 *
 * Authors:
 *   Guibin Du <tianguan.dgb@taobao.com>
 *
 */
#ifndef _OB_DOUBLE_CHILDREN_PHY_OPERATOR_H
#define _OB_DOUBLE_CHILDREN_PHY_OPERATOR_H 

#include "ob_phy_operator.h"

namespace oceanbase
{
  namespace sql
  {
    class ObDoubleChildrenPhyOperator: public ObPhyOperator
    {
      public:
        ObDoubleChildrenPhyOperator();
        virtual ~ObDoubleChildrenPhyOperator();
        /// Just two children are allowed to set
        virtual int set_child(int32_t child_idx, ObPhyOperator &child_operator);
        /// open children operators
        virtual int open();
        /// close children operators
        virtual int close();
      private:
        // disallow copy
        ObDoubleChildrenPhyOperator(const ObDoubleChildrenPhyOperator &other);
        ObDoubleChildrenPhyOperator& operator=(const ObDoubleChildrenPhyOperator &other);
      protected:
        // data members
        ObPhyOperator *left_op_;
        ObPhyOperator *right_op_;
    };
  } // end namespace sql
} // end namespace oceanbase

#endif /* _OB_DOUBLE_CHILDREN_PHY_OPERATOR_H */


