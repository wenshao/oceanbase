/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_transformer.cpp
 *
 * Authors:
 *   Guibin Du <tianguan.dgb@taobao.com>
 *
 */
#include "ob_transformer.h"
#include "ob_table_rpc_scan.h"
#include "ob_table_mem_scan.h"
#include "ob_merge_join.h"
#include "ob_sql_expression.h"
#include "ob_filter.h"
#include "ob_project.h"
#include "ob_set_operator.h"
#include "ob_merge_union.h"
#include "ob_merge_intersect.h"
#include "ob_merge_except.h"
#include "ob_sort.h"
#include "ob_merge_distinct.h"
#include "ob_merge_groupby.h"
#include "ob_merge_join.h"
#include "ob_scalar_aggregate.h"
#include "ob_limit.h"
#include "ob_physical_plan.h"
#include "parse_malloc.h"
#include "ob_add_project.h"
using namespace oceanbase::common;
using namespace oceanbase::sql;

#define CREATE_PHY_OPERRATOR(op, type_name, physical_plan)    \
do {    \
  op = (type_name*)parse_malloc(sizeof(type_name), mem_pool_);   \
  op = new(op) type_name();    \
  physical_plan->add_phy_operator(op);    \
} while (0)

ObTransformer::ObTransformer(ObStringBuf *mem_pool)
{
  mem_pool_ = mem_pool;
  is_transformed_ = false;
}

ObTransformer::~ObTransformer()
{
  // logical plan is generated outside, which are not release here
  logical_plans_.clear();
}

int ObTransformer::add_logical_plans(ObMultiPlan *logical_plans)
{
  int ret = OB_SUCCESS;
  for (int32_t i = 0; i < logical_plans->size(); i++)
  {
    if ((ret = logical_plans_.push_back(logical_plans->at(i))) != OB_SUCCESS)
      break;
  }
  return ret;
}

ObPhysicalPlan* ObTransformer::get_physical_plan(int32_t index)
{
  ObPhysicalPlan *op = NULL;
  if (index >= 0 && index < logical_plans_.count())
  {
    if (!is_transformed_)
    {
      if (generate_physical_plans() == OB_SUCCESS)
      {
        is_transformed_ = true;
        op = physical_plans_.at(index);
      }
    }
    else
    {
      op = physical_plans_.at(index);
    }
  }
  return op;
}

int ObTransformer::generate_physical_plans()
{
  int ret = OB_SUCCESS;
  ObLogicalPlan *logical_plan = NULL;
  ObPhysicalPlan *physical_plan = NULL;
  for (int32_t i = 0; i < logical_plans_.count(); i++)
  {
    logical_plan = logical_plans_.at(i);
    physical_plan = generate_physical_plan(logical_plan);
    if (!physical_plan)
    {
      ret = OB_ERROR;
      break;
    }
    physical_plans_.push_back(physical_plan);
  }
  return ret;
}

ObPhysicalPlan* ObTransformer::generate_physical_plan(ObLogicalPlan *logical_plan)
{
  ObPhysicalPlan *physical_plan = NULL;
  if (logical_plan)
  {
    switch (logical_plan->get_main_stmt()->get_stmt_type())
    {
      case ObStmt::T_SELECT:
        physical_plan = gen_physical_select(logical_plan);
        break;
      case ObStmt::T_DELETE:
        physical_plan = gen_physical_delete(logical_plan);
        break;
      case ObStmt::T_INSERT:
        physical_plan = gen_physical_insert(logical_plan);
        break;
      case ObStmt::T_UPDATE:
        physical_plan = gen_physical_update(logical_plan);
        break;
      default:
        break;
    }
  }
  return physical_plan;
}

int64_t ObTransformer::gen_phy_mono_select(
    ObLogicalPlan *logical_plan,
    ObPhysicalPlan *physical_plan,
    uint64_t query_id)
{
  //int err = OB_SUCCESS;
  int64_t idx = OB_INVALID_INDEX;
  ObSelectStmt *select_stmt = NULL;
  if (query_id == OB_INVALID_ID)
    select_stmt = dynamic_cast<ObSelectStmt*>(logical_plan->get_main_stmt());
  else
    select_stmt = dynamic_cast<ObSelectStmt*>(logical_plan->get_query(query_id));
  if (!select_stmt)
    return OB_INVALID_INDEX;

  ObSelectStmt::SetOperator set_type = select_stmt->get_set_op();
  if (set_type != ObSelectStmt::NONE)
  {
    ObPhysicalPlan sub_phy_plan;
    int64_t lidx = gen_phy_mono_select(logical_plan, &sub_phy_plan, select_stmt->get_left_query_id());
    if (lidx == OB_INVALID_INDEX)
    {
      return OB_INVALID_INDEX;
    }
    int64_t ridx = gen_phy_mono_select(logical_plan, &sub_phy_plan, select_stmt->get_right_query_id());
    if (ridx == OB_INVALID_INDEX)
    {
      return OB_INVALID_INDEX;
    }

    ObSetOperator *set_op = NULL;
    switch (set_type)
    {
      case ObSelectStmt::UNION :
      {
        ObMergeUnion *union_op = NULL;
        CREATE_PHY_OPERRATOR(union_op, ObMergeUnion, physical_plan);
        set_op = union_op;
        break;
      }
      case ObSelectStmt::INTERSECT :
      {
        ObMergeIntersect *intersect_op = NULL;
        CREATE_PHY_OPERRATOR(intersect_op, ObMergeIntersect, physical_plan);
        set_op = intersect_op;
        break;
      }
      case ObSelectStmt::EXCEPT :
      {
        ObMergeExcept *except_op = NULL;
        CREATE_PHY_OPERRATOR(except_op, ObMergeExcept, physical_plan);
        set_op = except_op;
        break;
      }
      default:
        break;
    }
    set_op->set_distinct(select_stmt->is_set_distinct() ? true : false);
    physical_plan->add_phy_query(set_op, idx);

    ObPhyOperator *left_op = NULL;
    ObPhyOperator *right_op = NULL;
    ObSelectStmt *lselect = dynamic_cast<ObSelectStmt*>(logical_plan->get_query(select_stmt->get_left_query_id()));
    ObSelectStmt *rselect = dynamic_cast<ObSelectStmt*>(logical_plan->get_query(select_stmt->get_right_query_id()));
    if (set_type == ObSelectStmt::UNION && !select_stmt->is_set_distinct())
    {
      left_op = sub_phy_plan.get_phy_query(lidx);
      right_op = sub_phy_plan.get_phy_query(ridx);
    }
    else
    {
      // 1
      // select c1+c2 from tbl
      // union
      // select c3+c4 rom tbl
      // order by 1;

      // 2
      // select c1+c2 as cc from tbl
      // union
      // select c3+c4 from tbl
      // order by cc;

      // there must be a Project operator on union part,
      // so do not worry non-column expr appear in sot operator

      //CREATE sort operators
      ObSort *left_sort = NULL;
      CREATE_PHY_OPERRATOR(left_sort, ObSort, physical_plan);
      left_sort->set_child(0 /* first child */, *(sub_phy_plan.get_phy_query(lidx)));
      ObSqlRawExpr *raw_expr = NULL;
      for (int32_t i = 0; i < lselect->get_select_item_size(); i++)
      {
        raw_expr = logical_plan->get_expr(select_stmt->get_select_item(i).expr_id_);
        if (raw_expr->get_expr()->get_expr_type() == T_REF_COLUMN)
        {
          ObBinaryRefRawExpr *col_expr = dynamic_cast<ObBinaryRefRawExpr*>(raw_expr->get_expr());
          left_sort->add_sort_column(col_expr->get_first_ref_id(), col_expr->get_second_ref_id(), true);
        }
        else
        {
          left_sort->add_sort_column(OB_INVALID_ID, raw_expr->get_column_id(), true);
        }
      }
      ObSort *right_sort = NULL;
      CREATE_PHY_OPERRATOR(right_sort, ObSort, physical_plan);
      right_sort->set_child(0 /* first child */, *(sub_phy_plan.get_phy_query(ridx)));
      for (int32_t i = 0; i < rselect->get_select_item_size(); i++)
      {
        raw_expr = logical_plan->get_expr(select_stmt->get_select_item(i).expr_id_);
        if (raw_expr->get_expr()->get_expr_type() == T_REF_COLUMN)
        {
          ObBinaryRefRawExpr *col_expr = dynamic_cast<ObBinaryRefRawExpr*>(raw_expr->get_expr());
          right_sort->add_sort_column(col_expr->get_first_ref_id(), col_expr->get_second_ref_id(), true);
        }
        else
        {
          right_sort->add_sort_column(OB_INVALID_ID, raw_expr->get_column_id(), true);
        }
      }
      left_op = left_sort;
      right_op = right_sort;
    }
    sub_phy_plan.clear();
    set_op->set_child(0 /* first child */, *left_op);
    set_op->set_child(1 /* second child */, *right_op);
  }
  else
  {
    /* Normal Select Statement */

    ObPhyOperator *result_op = NULL;

    // 1. generate physical plan for base-table/outer-join-table/temporary table
    ObList<ObPhyOperator*> phy_table_list;
    ObList<ObBitSet> bitset_list;
    ObList<ObSqlRawExpr*> remainder_cnd_list;
    gen_phy_tables(
        logical_plan,
        select_stmt,
        physical_plan,
        phy_table_list,
        bitset_list,
        remainder_cnd_list);

    // 2. Join all tables
    if (phy_table_list.size() > 1)
      gen_phy_joins(
          logical_plan,
          select_stmt,
          physical_plan,
          phy_table_list,
          bitset_list,
          remainder_cnd_list);
    phy_table_list.pop_front(result_op);

    // 3. add filter(s) to the join-op/table-scan-op result
    if (remainder_cnd_list.size() >= 1)
    {
      ObFilter *filter_op = NULL;
      CREATE_PHY_OPERRATOR(filter_op, ObFilter, physical_plan);
      filter_op->set_child(0, *result_op);
      oceanbase::common::ObList<ObSqlRawExpr*>::iterator cnd_it;
      for (cnd_it = remainder_cnd_list.begin(); cnd_it != remainder_cnd_list.end(); cnd_it++)
      {
        ObSqlExpression filter;
        (*cnd_it)->fill_sql_expression(filter, this, logical_plan, physical_plan);
        filter_op->add_filter(filter);
      }
      result_op = filter_op;
    }

    // 4. generate physical plan for group by/aggregate
    if (select_stmt->get_group_expr_size() > 0)
      result_op = gen_phy_group_by(logical_plan, select_stmt, physical_plan, result_op);
    else if (select_stmt->get_agg_fun_size() > 0)
      result_op = gen_phy_scalar_aggregate(logical_plan, select_stmt, physical_plan, result_op);

    // 5. generate physical plan for having
    if (select_stmt->get_having_expr_size() > 0)
    {
      ObFilter *having_op = NULL;
      CREATE_PHY_OPERRATOR(having_op, ObFilter, physical_plan);
      ObSqlRawExpr *having_expr;
      int32_t num = select_stmt->get_having_expr_size();
      for (int32_t i = 0; i < num; i++)
      {
        having_expr = logical_plan->get_expr(select_stmt->get_having_expr_id(i));
        ObSqlExpression having_filter;
        having_expr->fill_sql_expression(having_filter, this, logical_plan, physical_plan);
        having_op->add_filter(having_filter);
      }
      having_op->set_child(0, *result_op);
      result_op = having_op;
    }

    // 6. generate physical plan for distinct
    if (select_stmt->is_distinct())
      result_op = gen_phy_distinct(logical_plan, select_stmt, physical_plan, result_op);

    // 7. generate physical plan for order by
    if (select_stmt->get_order_item_size() > 0)
      result_op = gen_phy_order_by(logical_plan, select_stmt, physical_plan, result_op);

    // 8. generate physical plan for limit
    if (select_stmt->get_limit() != -1 || select_stmt->get_offset() != 0)
    {
      ObLimit *limit_op = NULL;
      CREATE_PHY_OPERRATOR(limit_op, ObLimit, physical_plan);
      limit_op->set_limit(select_stmt->get_limit(), select_stmt->get_offset());
      limit_op->set_child(0, *result_op);
      result_op = limit_op;
    }

    // 8. generate physical plan for select clause
    if (select_stmt->get_select_item_size() > 0)
    {
      ObProject *project_op = NULL;
      CREATE_PHY_OPERRATOR(project_op, ObProject, physical_plan);
      project_op->set_child(0, *result_op);

      ObSqlRawExpr *select_expr;
      int32_t num = select_stmt->get_select_item_size();
      for (int32_t i = 0; i < num; i++)
      {
        const SelectItem& select_item = select_stmt->get_select_item(i);
        select_expr = logical_plan->get_expr(select_item.expr_id_);
        if (select_item.is_real_alias_)
        {
          ObBinaryRefRawExpr col_raw(OB_INVALID_ID, select_expr->get_column_id(), T_REF_COLUMN);
          ObSqlRawExpr col_sql_raw(*select_expr);
          col_sql_raw.set_expr(&col_raw);
          ObSqlExpression  col_expr;
          col_sql_raw.fill_sql_expression(col_expr);
          project_op ->add_output_column(col_expr);
        }
        else
        {
          ObSqlExpression  col_expr;
          select_expr->fill_sql_expression(col_expr, this, logical_plan, physical_plan);
          project_op ->add_output_column(col_expr);
        }
      }
      result_op = project_op;
    }

    physical_plan->add_phy_query(result_op, idx);
  }

  return idx;
}

ObPhyOperator* ObTransformer::gen_phy_order_by(
    ObLogicalPlan *logical_plan,
    ObSelectStmt *select_stmt,
    ObPhysicalPlan *physical_plan,
    ObPhyOperator *in_op)
{
  ObSort *sort_op = NULL;
  ObProject *project_op = NULL;;
  CREATE_PHY_OPERRATOR(sort_op, ObSort, physical_plan);

  ObSqlRawExpr *order_expr;
  int32_t num = select_stmt->get_order_item_size();
  for (int32_t i = 0; i < num; i++)
  {
    const OrderItem& order_item = select_stmt->get_order_item(i);
    order_expr = logical_plan->get_expr(order_item.expr_id_);
    if (order_expr->get_expr()->is_const())
    {
      // do nothing, const column is of no usage for sorting
    }
    else if (order_expr->get_expr()->get_expr_type() == T_REF_COLUMN)
    {
      ObBinaryRefRawExpr *col_expr = dynamic_cast<ObBinaryRefRawExpr*>(order_expr->get_expr());
      sort_op->add_sort_column(
          col_expr->get_first_ref_id(),
          col_expr->get_second_ref_id(),
          order_item.order_type_ == OrderItem::ASC ? true : false);
    }
    else
    {
      if (!project_op)
      {
        CREATE_PHY_OPERRATOR(project_op, ObAddProject, physical_plan);
        project_op->set_child(0, *in_op);
      }
      ObSqlExpression col_expr;
      order_expr->fill_sql_expression(col_expr, this, logical_plan, physical_plan);
      project_op->add_output_column(col_expr);
      sort_op->add_sort_column(
          order_expr->get_table_id(),
          order_expr->get_column_id(),
          order_item.order_type_ == OrderItem::ASC ? true : false);
    }
  }
  if (project_op)
    sort_op->set_child(0, *project_op);
  else
    sort_op->set_child(0, *in_op);

  return sort_op;
}

ObPhyOperator* ObTransformer::gen_phy_distinct(
    ObLogicalPlan *logical_plan,
    ObSelectStmt *select_stmt,
    ObPhysicalPlan *physical_plan,
    ObPhyOperator *in_op)
{
  ObMergeDistinct *distinct_op;
  ObSort *sort_op = NULL;
  ObProject *project_op = NULL;
  CREATE_PHY_OPERRATOR(sort_op, ObSort, physical_plan);
  CREATE_PHY_OPERRATOR(distinct_op, ObMergeDistinct, physical_plan);
  distinct_op->set_child(0, *sort_op);

  ObSqlRawExpr *select_expr;
  int32_t num = select_stmt->get_select_item_size();
  for (int32_t i = 0; i < num; i++)
  {
    const SelectItem& select_item = select_stmt->get_select_item(i);
    select_expr = logical_plan->get_expr(select_item.expr_id_);
    if (select_expr->get_expr()->is_const())
    {
      // do nothing, const column is of no usage for sorting
    }
    else if (select_item.is_real_alias_)
    {
      sort_op->add_sort_column(select_expr->get_table_id(), select_expr->get_column_id(), true);
      distinct_op->add_distinct_column(select_expr->get_table_id(), select_expr->get_column_id());
    }
    else if (select_expr->get_expr()->get_expr_type() == T_REF_COLUMN)
    {
      ObBinaryRefRawExpr *col_expr = dynamic_cast<ObBinaryRefRawExpr*>(select_expr->get_expr());
      sort_op->add_sort_column(col_expr->get_first_ref_id(), col_expr->get_second_ref_id(), true);
      distinct_op->add_distinct_column(col_expr->get_first_ref_id(), col_expr->get_second_ref_id());
    }
    else
    {
      if (!project_op)
      {
        CREATE_PHY_OPERRATOR(project_op, ObAddProject, physical_plan);
        project_op->set_child(0, *in_op);
      }
      ObSqlExpression col_expr;
      select_expr->fill_sql_expression(col_expr, this, logical_plan, physical_plan);
      project_op->add_output_column(col_expr);
      sort_op->add_sort_column(select_expr->get_table_id(), select_expr->get_column_id(), true);
      distinct_op->add_distinct_column(select_expr->get_table_id(), select_expr->get_column_id());
    }
  }
  if (project_op)
    sort_op->set_child(0, *project_op);
  else
    sort_op->set_child(0, *in_op);

  return distinct_op;
}

ObPhyOperator* ObTransformer::gen_phy_group_by(
    ObLogicalPlan *logical_plan,
    ObSelectStmt *select_stmt,
    ObPhysicalPlan *physical_plan,
    ObPhyOperator *in_op)
{
  ObMergeGroupBy *group_op;
  ObSort *sort_op = NULL;
  ObProject *project_op = NULL;
  CREATE_PHY_OPERRATOR(sort_op, ObSort, physical_plan);
  CREATE_PHY_OPERRATOR(group_op, ObMergeGroupBy, physical_plan);
  group_op->set_child(0, *sort_op);

  ObSqlRawExpr *group_expr;
  int32_t num = select_stmt->get_group_expr_size();
  for (int32_t i = 0; i < num; i++)
  {
    group_expr = logical_plan->get_expr(select_stmt->get_group_expr_id(i));
    if (group_expr->get_expr()->get_expr_type() == T_REF_COLUMN)
    {
      ObBinaryRefRawExpr *col_expr = dynamic_cast<ObBinaryRefRawExpr*>(group_expr->get_expr());
      sort_op->add_sort_column(col_expr->get_first_ref_id(), col_expr->get_second_ref_id(), true);
      group_op->add_group_column(col_expr->get_first_ref_id(), col_expr->get_second_ref_id());
    }
    else if (group_expr->get_expr()->is_const())
    {
      // do nothing, const column is of no usage for sorting
    }
    else
    {
      if (!project_op)
      {
        CREATE_PHY_OPERRATOR(project_op, ObAddProject, physical_plan);
        project_op->set_child(0, *in_op);
      }
      ObSqlExpression col_expr;
      group_expr->fill_sql_expression(col_expr, this, logical_plan, physical_plan);
      project_op->add_output_column(col_expr);
      sort_op->add_sort_column(group_expr->get_table_id(), group_expr->get_column_id(), true);
      group_op->add_group_column(group_expr->get_table_id(), group_expr->get_column_id());
    }
  }
  if (project_op)
    sort_op->set_child(0, *project_op);
  else
    sort_op->set_child(0, *in_op);

  num = select_stmt->get_agg_fun_size();
  ObSqlRawExpr *agg_expr;
  for (int32_t i = 0; i < num; i++)
  {
    agg_expr = logical_plan->get_expr(select_stmt->get_agg_expr_id(i));
    if (agg_expr->get_expr()->is_aggr_fun())
    {
      ObSqlExpression new_agg_expr;
      agg_expr->fill_sql_expression(new_agg_expr, this, logical_plan, physical_plan);
      group_op->add_aggr_column(new_agg_expr);
    }
    else
    {
      TBSYS_LOG(ERROR, "wrong aggregate function, exp_id = %ld", agg_expr->get_expr_id());
    }
  }

  return group_op;
}

ObPhyOperator* ObTransformer::gen_phy_scalar_aggregate(
    ObLogicalPlan *logical_plan,
    ObSelectStmt *select_stmt,
    ObPhysicalPlan *physical_plan,
    ObPhyOperator *in_op)
{
  ObScalarAggregate *scalar_agg_op = NULL;
  CREATE_PHY_OPERRATOR(scalar_agg_op, ObScalarAggregate, physical_plan);
  scalar_agg_op->set_child(0, *in_op);

  int32_t num = select_stmt->get_agg_fun_size();
  ObSqlRawExpr *agg_expr;
  for (int32_t i = 0; i < num; i++)
  {
    agg_expr = logical_plan->get_expr(select_stmt->get_agg_expr_id(i));
    if (agg_expr->get_expr()->is_aggr_fun())
    {
      ObSqlExpression new_agg_expr;
      agg_expr->fill_sql_expression(new_agg_expr, this, logical_plan, physical_plan);
      scalar_agg_op->add_aggr_column(new_agg_expr);
    }
    else
    {
      TBSYS_LOG(ERROR, "wrong aggregate function, exp_id = %ld", agg_expr->get_expr_id());
    }
  }

  return scalar_agg_op;
}

int ObTransformer::gen_phy_joins(
    ObLogicalPlan *logical_plan,
    ObSelectStmt *select_stmt,
    ObPhysicalPlan *physical_plan,
    oceanbase::common::ObList<ObPhyOperator*>& phy_table_list,
    oceanbase::common::ObList<ObBitSet>& bitset_list,
    oceanbase::common::ObList<ObSqlRawExpr*>& remainder_cnd_list)
{
  int ret = OB_SUCCESS;
  while (phy_table_list.size() > 1)
  {
    ObMergeJoin *join_op = NULL;
    CREATE_PHY_OPERRATOR(join_op, ObMergeJoin, physical_plan);
    join_op->set_join_type(ObJoin::INNER_JOIN);

    ObBitSet join_table_bitset;
    ObBitSet left_table_bitset;
    ObBitSet right_table_bitset;
    ObSort *left_sort = NULL;
    ObSort *right_sort = NULL;
    oceanbase::common::ObList<ObSqlRawExpr*>::iterator cnd_it;
    oceanbase::common::ObList<ObSqlRawExpr*>::iterator del_it;
    for (cnd_it = remainder_cnd_list.begin(); cnd_it != remainder_cnd_list.end(); )
    {
      if ((*cnd_it)->get_expr()->is_join_cond() && join_table_bitset.is_empty())
      {
        ObBinaryOpRawExpr *join_cnd = dynamic_cast<ObBinaryOpRawExpr*>((*cnd_it)->get_expr());
        ObBinaryRefRawExpr *lexpr = dynamic_cast<ObBinaryRefRawExpr*>(join_cnd->get_first_op_expr());
        ObBinaryRefRawExpr *rexpr = dynamic_cast<ObBinaryRefRawExpr*>(join_cnd->get_second_op_expr());
        int32_t left_bit_idx = select_stmt->get_table_bit_index(lexpr->get_first_ref_id());
        int32_t right_bit_idx = select_stmt->get_table_bit_index(rexpr->get_first_ref_id());
        // join_table_bitset->add_member(left_bit_idx);
        // join_table_bitset->add_member(right_bit_idx);
        CREATE_PHY_OPERRATOR(left_sort, ObSort, physical_plan);
        left_sort->add_sort_column(lexpr->get_first_ref_id(), lexpr->get_second_ref_id(), true);
        CREATE_PHY_OPERRATOR(right_sort, ObSort, physical_plan);
        right_sort->add_sort_column(rexpr->get_first_ref_id(), rexpr->get_second_ref_id(), true);

        oceanbase::common::ObList<ObPhyOperator*>::iterator table_it = phy_table_list.begin();
        oceanbase::common::ObList<ObPhyOperator*>::iterator del_table_it;
        oceanbase::common::ObList<ObBitSet>::iterator bitset_it = bitset_list.begin();
        oceanbase::common::ObList<ObBitSet>::iterator del_bitset_it;
        ObPhyOperator *left_table_op = NULL;
        ObPhyOperator *right_table_op = NULL;
        while ((!left_table_op || !right_table_op)
            && table_it != phy_table_list.end()
            && bitset_it != bitset_list.end())
        {
          if (bitset_it->has_member(left_bit_idx))
          {
            left_table_op = *table_it;
            left_table_bitset = *bitset_it;
            del_table_it = table_it;
            del_bitset_it = bitset_it;
            table_it++;
            bitset_it++;
            phy_table_list.erase(del_table_it);
            bitset_list.erase(del_bitset_it);
          }
          else if (bitset_it->has_member(right_bit_idx))
          {
            right_table_op = *table_it;
            right_table_bitset = *bitset_it;
            del_table_it = table_it;
            del_bitset_it = bitset_it;
            table_it++;
            bitset_it++;
            phy_table_list.erase(del_table_it);
            bitset_list.erase(del_bitset_it);
          }
          else
          {
            table_it++;
            bitset_it++;
          }
        }
        // Two columns must from different table, that expression from one table has been erased in gen_phy_table()
        OB_ASSERT(left_table_op && right_table_op);
        left_sort->set_child(0, *left_table_op);
        right_sort->set_child(0, *right_table_op);
        ObSqlExpression join_op_cnd;
        (*cnd_it)->fill_sql_expression(join_op_cnd, this, logical_plan, physical_plan);
        join_op->add_equijoin_condition(join_op_cnd);
        join_table_bitset.add_members(left_table_bitset);
        join_table_bitset.add_members(right_table_bitset);

        del_it = cnd_it;
        cnd_it++;
        remainder_cnd_list.erase(del_it);
      }
      else if ((*cnd_it)->get_expr()->is_join_cond()
        && (*cnd_it)->get_tables_set().is_subset(join_table_bitset))
      {
        ObBinaryOpRawExpr *join_cnd = dynamic_cast<ObBinaryOpRawExpr*>((*cnd_it)->get_expr());
        ObBinaryRefRawExpr *expr1 = dynamic_cast<ObBinaryRefRawExpr*>(join_cnd->get_first_op_expr());
        ObBinaryRefRawExpr *expr2 = dynamic_cast<ObBinaryRefRawExpr*>(join_cnd->get_second_op_expr());
        int32_t bit_idx1 = select_stmt->get_table_bit_index(expr1->get_first_ref_id());
        int32_t bit_idx2 = select_stmt->get_table_bit_index(expr2->get_first_ref_id());
        if (left_table_bitset.has_member(bit_idx1))
          left_sort->add_sort_column(expr1->get_first_ref_id(), expr1->get_second_ref_id(), true);
        else
          right_sort->add_sort_column(expr1->get_first_ref_id(), expr1->get_second_ref_id(), true);
        if (right_table_bitset.has_member(bit_idx2))
          right_sort->add_sort_column(expr2->get_first_ref_id(), expr2->get_second_ref_id(), true);
        else
          left_sort->add_sort_column(expr2->get_first_ref_id(), expr2->get_second_ref_id(), true);
        ObSqlExpression join_op_cnd;
        (*cnd_it)->fill_sql_expression(join_op_cnd, this, logical_plan, physical_plan);
        join_op->add_equijoin_condition(join_op_cnd);
        del_it = cnd_it;
        cnd_it++;
        remainder_cnd_list.erase(del_it);
      }
      else if ((*cnd_it)->get_tables_set().is_subset(join_table_bitset))
      {
        ObSqlExpression join_other_cnd;
        (*cnd_it)->fill_sql_expression(join_other_cnd, this, logical_plan, physical_plan);
        join_op->add_other_join_condition(join_other_cnd);
        del_it = cnd_it;
        cnd_it++;
        remainder_cnd_list.erase(del_it);
      }
      else
      {
        cnd_it++;
      }
    }

    if (join_table_bitset.is_empty() == false)
    {
      // find a join condition, a merge join will be used here
      OB_ASSERT(left_sort != NULL);
      OB_ASSERT(right_sort != NULL);
      join_op->set_child(0, *left_sort);
      join_op->set_child(1, *right_sort);
    }
    else
    {
      // Can not find a join condition, a product join will be used here
      // FIX me, should be ObJoin, it will be fixed when Join is supported
      ObPhyOperator *op = NULL;
      phy_table_list.pop_front(op);
      join_op->set_child(0, *op);
      phy_table_list.pop_front(op);
      join_op->set_child(1, *op);

      ObBitSet op_bitset;
      bitset_list.pop_front(op_bitset);
      join_table_bitset.add_members(op_bitset);
      bitset_list.pop_front(op_bitset);
      join_table_bitset.add_members(op_bitset);
    }

    // add other join conditions
    for (cnd_it = remainder_cnd_list.begin(); cnd_it != remainder_cnd_list.end(); )
    {
      if ((*cnd_it)->get_tables_set().is_subset(join_table_bitset))
      {
        ObSqlExpression other_cnd;
        (*cnd_it)->fill_sql_expression(other_cnd, this, logical_plan, physical_plan);
        join_op->add_other_join_condition(other_cnd);
        del_it = cnd_it;
        cnd_it++;
        remainder_cnd_list.erase(del_it);
      }
      else
      {
        cnd_it++;
      }
    }

    phy_table_list.push_back(join_op);
    bitset_list.push_back(join_table_bitset);
    join_table_bitset.clear();
  }

  return ret;
}

int ObTransformer::gen_phy_tables(
    ObLogicalPlan *logical_plan,
    ObSelectStmt *select_stmt,
    ObPhysicalPlan *physical_plan,
    oceanbase::common::ObList<ObPhyOperator*>& phy_table_list,
    oceanbase::common::ObList<ObBitSet>& bitset_list,
    oceanbase::common::ObList<ObSqlRawExpr*>& remainder_cnd_list)
{
  int ret = OB_SUCCESS;
  ObPhyOperator *table_op;
  ObBitSet bit_set;
  int32_t num_table = select_stmt->get_from_item_size();
  for(int32_t i = 0; i < num_table; i++)
  {
    const FromItem& from_item = select_stmt->get_from_item(i);
    if (from_item.is_joined_ == false)
    {
      /* base-table or temporary table */
      table_op = gen_phy_table(logical_plan, select_stmt, physical_plan, from_item.table_id_);
      bit_set.add_member(select_stmt->get_table_bit_index(from_item.table_id_));
    }
    else
    {
      /* Outer Join */
      JoinedTable *joined_table = select_stmt->get_joined_table(from_item.table_id_);
      OB_ASSERT(joined_table->table_ids_.count() >= 2);
      OB_ASSERT(joined_table->table_ids_.count() - 1 == joined_table->join_types_.count());
      OB_ASSERT(joined_table->join_types_.count() == joined_table->expr_ids_.count());

      table_op = gen_phy_table(logical_plan, select_stmt, physical_plan, joined_table->table_ids_.at(0));
      if (!table_op)
      {
        return OB_ERROR;
      }
      bit_set.add_member(select_stmt->get_table_bit_index(joined_table->table_ids_.at(0)));

      ObPhyOperator *right_op;
      ObMergeJoin *join_op;
      ObSqlRawExpr *join_expr;
      for (int32_t j = 1; j < joined_table->table_ids_.count(); j++)
      {
        right_op = gen_phy_table(logical_plan, select_stmt, physical_plan, joined_table->table_ids_.at(j));
        if (!right_op)
        {
          delete table_op;
          return OB_ERROR;
        }
        bit_set.add_member(select_stmt->get_table_bit_index(joined_table->table_ids_.at(j)));

        // Now we don't optimize outer join, so set all join type to product join
        // we may pick out equal conditions later
        CREATE_PHY_OPERRATOR(join_op, ObMergeJoin, physical_plan);
        join_op->set_child(0 /* left join table */, *table_op);
        join_op->set_child(1 /* right join table */, *right_op);
        switch (joined_table->join_types_.at(j - 1))
        {
          case JoinedTable::T_FULL:
            join_op->set_join_type(ObJoin::FULL_OUTER_JOIN);
            break;
          case JoinedTable::T_LEFT:
            join_op->set_join_type(ObJoin::LEFT_OUTER_JOIN);
            break;
          case JoinedTable::T_RIGHT:
            join_op->set_join_type(ObJoin::RIGHT_OUTER_JOIN);
            break;
          case JoinedTable::T_INNER:
            join_op->set_join_type(ObJoin::INNER_JOIN);
            break;
          default:
            /* won't be here */
            join_op->set_join_type(ObJoin::INNER_JOIN);
            break;
        }

        join_expr = logical_plan->get_expr(joined_table->expr_ids_.at(j - 1));
        if (join_expr)
        {
          ObSqlExpression join_cnd;
          join_expr->fill_sql_expression(join_cnd, this, logical_plan, physical_plan);
          join_op->add_other_join_condition(join_cnd);
        }
        table_op = join_op;
      }
    }
    phy_table_list.push_back(table_op);
    bitset_list.push_back(bit_set);
    bit_set.clear();
  }

  int32_t num = select_stmt->get_condition_size();
  for (int32_t i = 0; i < num; i++)
  {
    uint64_t expr_id = select_stmt->get_condition_id(i);
    ObSqlRawExpr *where_expr = logical_plan->get_expr(expr_id);
    if (where_expr && where_expr->is_apply() == false)
    {
      remainder_cnd_list.push_back(where_expr);
    }
  }

  return ret;
}

ObPhyOperator* ObTransformer::gen_phy_table(
    ObLogicalPlan *logical_plan,
    ObStmt *stmt,
    ObPhysicalPlan *physical_plan,
    uint64_t table_id)
{
  if (table_id == OB_INVALID_ID)
    return NULL;

  TableItem* table_item = stmt->get_table_item_by_id(table_id);
  if (!table_item)
    return NULL;
  ObTableScan* table_scan_op = NULL;

  switch (table_item->type_)
  {
    case TableItem::BASE_TABLE:
      /* get through */
    case TableItem::ALIAS_TABLE:
      // set table
      CREATE_PHY_OPERRATOR(table_scan_op, ObTableRpcScan, physical_plan);
      table_scan_op->set_table(table_item->table_id_, table_item->ref_id_);
      break;
    case TableItem::GENERATED_TABLE:
    {
      ObPhysicalPlan sub_phy_plan;
      int64_t idx = gen_phy_mono_select(logical_plan, &sub_phy_plan, table_item->ref_id_);
      CREATE_PHY_OPERRATOR(table_scan_op, ObTableMemScan, physical_plan);
      // the sub-query's physical plan is set directly, so base_table_id is no need to set
      table_scan_op->set_table(table_item->table_id_, OB_INVALID_ID);
      table_scan_op->set_child(0, *(sub_phy_plan.get_phy_query(idx)));
      sub_phy_plan.clear();
      break;
    }
    default:
      // won't be here
      OB_ASSERT(0);
      break;
  }

  // set filters
  int32_t num = stmt->get_condition_size();
  ObBitSet table_bitset;
  int32_t bit_index = stmt->get_table_bit_index(table_item->table_id_);
  table_bitset.add_member(bit_index);
  for (int32_t i = 0; i < num; i++)
  {
    ObSqlRawExpr *cnd_expr = logical_plan->get_expr(stmt->get_condition_id(i));
    if (table_bitset.is_superset(cnd_expr->get_tables_set()))
    {
      cnd_expr->set_applied(true);
      ObSqlExpression filter;
      cnd_expr->fill_sql_expression(filter, this, logical_plan, physical_plan);
      table_scan_op->add_filter(filter);
    }
  }

  // add output columns
  num = stmt->get_column_size();
  for (int32_t i = 0; i < num; i++)
  {
    const ColumnItem *col_item = stmt->get_column_item(i);
    if (col_item->table_id_ == table_item->table_id_)
    {
      ObBinaryRefRawExpr col_expr(col_item->table_id_, col_item->column_id_, T_REF_COLUMN);
      ObSqlRawExpr col_raw_expr(
          common::OB_INVALID_ID,
          col_item->table_id_,
          col_item->column_id_,
          &col_expr);
      ObSqlExpression output_expr;
      col_raw_expr.fill_sql_expression(output_expr, this, logical_plan, physical_plan);
      table_scan_op->add_output_column(output_expr);
    }
  }
  ObSelectStmt *select_stmt = dynamic_cast<ObSelectStmt*>(stmt);
  if (select_stmt)
  {
    num = select_stmt->get_select_item_size();
    for (int32_t i = 0; i < num; i++)
    {
      const SelectItem& select_item = select_stmt->get_select_item(i);
      if (select_item.is_real_alias_)
      {
        ObSqlRawExpr *alias_expr = logical_plan->get_expr(select_item.expr_id_);
        if (table_bitset.is_superset(alias_expr->get_tables_set()))
        {
          ObSqlExpression output_expr;
          alias_expr->fill_sql_expression(output_expr, this, logical_plan, physical_plan);
          table_scan_op->add_output_column(output_expr);
        }
      }
    }
  }

  return table_scan_op;
}

ObPhysicalPlan* ObTransformer::gen_physical_select(ObLogicalPlan *logical_plan)
{
  if (!logical_plan)
    return NULL;
  ObSelectStmt *select_stmt = dynamic_cast<ObSelectStmt*>(logical_plan->get_main_stmt());
  assert(select_stmt != NULL);
  UNUSED(select_stmt);

  ObPhysicalPlan* ret_phy_plan = (ObPhysicalPlan*)parse_malloc(sizeof(ObPhysicalPlan), mem_pool_);
  ret_phy_plan = new(ret_phy_plan) ObPhysicalPlan();

  if (gen_phy_mono_select(logical_plan, ret_phy_plan) == OB_INVALID_INDEX)
  {
    delete ret_phy_plan;
    ret_phy_plan = NULL;
  }
  return ret_phy_plan;
}

ObPhysicalPlan* ObTransformer::gen_physical_delete(ObLogicalPlan *logical_plan)
{
  OB_ASSERT(logical_plan);
  return NULL;
}

ObPhysicalPlan* ObTransformer::gen_physical_insert(ObLogicalPlan *logical_plan)
{
  OB_ASSERT(logical_plan);
  return NULL;
}

ObPhysicalPlan* ObTransformer::gen_physical_update(ObLogicalPlan *logical_plan)
{
  OB_ASSERT(logical_plan);
  return NULL;
}
