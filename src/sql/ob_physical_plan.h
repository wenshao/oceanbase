/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_physical_plan.h
 *
 * Authors:
 *   Guibin Du <tianguan.dgb@taobao.com>
 *
 */
#ifndef _OB_PHYSICAL_PLAN_H
#define _OB_PHYSICAL_PLAN_H
#include "ob_phy_operator.h"

namespace oceanbase
{
  namespace sql
  {
    class ObPhysicalPlan
    {
      public:
        ObPhysicalPlan() {}
        virtual ~ObPhysicalPlan();

        int add_phy_query(ObPhyOperator *phy_query, int64_t& idx);
        int add_phy_operator(ObPhyOperator *op);
        int64_t get_query_size() const { return phy_querys_.count(); }
        int64_t to_string(char* buf, const int64_t buf_len) const;
        ObPhyOperator* get_phy_query(int64_t index) const;
        void clear() 
        {
          phy_querys_.clear();
          operators_store_.clear();
        }

      private:
        DISALLOW_COPY_AND_ASSIGN(ObPhysicalPlan);


      private:
        oceanbase::common::ObArray<ObPhyOperator *> phy_querys_;
        oceanbase::common::ObArray<ObPhyOperator *> operators_store_;
    };
    
  } // end namespace sql
} // end namespace oceanbase

#endif /* _OB_PHYSICAL_PLAN_H */



