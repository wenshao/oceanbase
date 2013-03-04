/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_transformer.h
 *
 * Authors:
 *   Guibin Du <tianguan.dgb@taobao.com>
 *
 */
#ifndef _OB_TRANSFORMER_H
#define _OB_TRANSFORMER_H

#include "ob_phy_operator.h"
#include "ob_logical_plan.h"
#include "ob_physical_plan.h"
#include "ob_multi_plan.h"
#include "common/ob_list.h"

namespace oceanbase
{
  namespace sql
  {
    class ObTransformer
    {
      public:
        ObTransformer(oceanbase::common::ObStringBuf *mem_pool);
        virtual ~ObTransformer();

        int add_logical_plan(ObLogicalPlan *logical_plan)
        {
          return logical_plans_.push_back(logical_plan);
        }

        int add_logical_plans(ObMultiPlan *logical_plans);
        
        ObPhysicalPlan* get_physical_plan(int32_t index);
        
        int64_t gen_phy_mono_select(
            ObLogicalPlan *logical_plan,
            ObPhysicalPlan *physical_plan,
            uint64_t query_id = oceanbase::common::OB_INVALID_ID);

      private:
        DISALLOW_COPY_AND_ASSIGN(ObTransformer);

        int generate_physical_plans();
        ObPhysicalPlan* generate_physical_plan(ObLogicalPlan *logical_plan);
        ObPhysicalPlan* gen_physical_select(ObLogicalPlan *logical_plan);
        ObPhysicalPlan* gen_physical_delete(ObLogicalPlan *logical_plan);
        ObPhysicalPlan* gen_physical_insert(ObLogicalPlan *logical_plan);
        ObPhysicalPlan* gen_physical_update(ObLogicalPlan *logical_plan);
        int gen_phy_tables(
            ObLogicalPlan *logical_plan,
            ObSelectStmt *select_stmt,
            ObPhysicalPlan *physical_plan,
            oceanbase::common::ObList<ObPhyOperator*>& phy_table_list,
            oceanbase::common::ObList<ObBitSet>& bitset_list,
            oceanbase::common::ObList<ObSqlRawExpr*>& remainder_cnd_list);
        ObPhyOperator* gen_phy_table(
            ObLogicalPlan *logical_plan,
            ObStmt *stmt,
            ObPhysicalPlan *physical_plan,
            uint64_t table_id);
        int gen_phy_joins(
            ObLogicalPlan *logical_plan,
            ObSelectStmt *select_stmt,
            ObPhysicalPlan *physical_plan,
            oceanbase::common::ObList<ObPhyOperator*>& phy_table_list,
            oceanbase::common::ObList<ObBitSet>& bitset_list,
            oceanbase::common::ObList<ObSqlRawExpr*>& remainder_cnd_list);
        ObPhyOperator* gen_phy_group_by(
            ObLogicalPlan *logical_plan,
            ObSelectStmt *select_stmt,
            ObPhysicalPlan *physical_plan,
            ObPhyOperator *in_op);
        ObPhyOperator* gen_phy_scalar_aggregate(
            ObLogicalPlan *logical_plan,
            ObSelectStmt *select_stmt,
            ObPhysicalPlan *physical_plan,
            ObPhyOperator *in_op);
        ObPhyOperator* gen_phy_distinct(
            ObLogicalPlan *logical_plan,
            ObSelectStmt *select_stmt,
            ObPhysicalPlan *physical_plan,
            ObPhyOperator *in_op);
        ObPhyOperator* gen_phy_order_by(
            ObLogicalPlan *logical_plan,
            ObSelectStmt *select_stmt,
            ObPhysicalPlan *physical_plan,
            ObPhyOperator *in_op);

      private:
        oceanbase::common::ObStringBuf *mem_pool_;
        oceanbase::common::ObArray<ObLogicalPlan *> logical_plans_;
        oceanbase::common::ObArray<ObPhysicalPlan *> physical_plans_;
        bool  is_transformed_;
    };
    
  } // end namespace sql
} // end namespace oceanbase

#endif /* _OB_TRANSFORMER_H */


