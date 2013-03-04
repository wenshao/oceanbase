/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_sql.cpp
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#include "sql/ob_sql.h"
#include "sql/parse_node.h"
#include "sql/build_plan.h"
#include "sql/ob_transformer.h"
#include "sql/ob_schema_checker.h"
using namespace oceanbase::common;
using namespace oceanbase::sql;

int ObSql::direct_execute(const common::ObString &stmt, ObResultSet &result)
{
  int ret = OB_SUCCESS;
  ParseResult parse_res;
  common::ObStringBuf name_pool;
  // Step 1: init
  parse_res.malloc_pool_ = &name_pool;
  if (0 != (ret = parse_init(&parse_res)))
  {
    TBSYS_LOG(WARN, "parser init err, err=%s", strerror(errno));
    ret = OB_ERR_PARSER_INIT;
  }
  else
  {
    if (OB_UNLIKELY(TBSYS_LOGGER._level >= TBSYS_LOG_LEVEL_DEBUG))
    {
      TBSYS_LOG(DEBUG, "execute sql statement=%.*s", stmt.length(), stmt.ptr());
    }
    // Step 2: parse the sql statement
    // @todo parse_sql should return error code
    parse_sql(&parse_res, stmt.ptr(), static_cast<size_t>(stmt.length()));
    if (NULL == parse_res.result_tree_)
    {
      TBSYS_LOG(WARN, "failed to parse sql, stmt=%.*s err=%s line=%d pos=%d",
                stmt.length(), stmt.ptr(), parse_res.error_msg_, parse_res.line_, parse_res.start_col_);
      result.set_message(parse_res.error_msg_);
      ret = OB_ERR_PARSE_SQL;
    }
    else
    {
      if (OB_UNLIKELY(TBSYS_LOGGER._level >= TBSYS_LOG_LEVEL_DEBUG))
      {
        print_tree(parse_res.result_tree_, 4);
      }
      // Step 3: resolve the node trees
      ResultPlan logical_plan;
      ObSchemaChecker schema_checker;
      logical_plan.name_pool_ = &name_pool;
      logical_plan.schema_checker_ = &schema_checker;
      logical_plan.plan_tree_ = NULL;
      // @todo resolve's return code don't follow the convention
      if (1 != (ret = resolve(&logical_plan, parse_res.result_tree_)))
      {
        TBSYS_LOG(WARN, "failed to resolve sql, stmt=%.*s",
                  stmt.length(), stmt.ptr());
        ret = OB_ERR_RESOLVE_SQL;
      }
      else
      {
        ObMultiPlan* multi_plan = static_cast<ObMultiPlan*>(logical_plan.plan_tree_);
        if (OB_UNLIKELY(TBSYS_LOGGER._level >= TBSYS_LOG_LEVEL_DEBUG))
        {
          multi_plan->print();
        }
        // Step 4: generate the physical execution plan
        ObTransformer trans(&name_pool);
        ObPhysicalPlan *physical_plan = NULL;
        if (OB_SUCCESS != (ret = trans.add_logical_plans(multi_plan)))
        {
          TBSYS_LOG(WARN, "failed to add logical plan, err=%d", ret);
        }
        else if (NULL == (physical_plan = trans.get_physical_plan(0)))
        {
          TBSYS_LOG(WARN, "failed to transform to physical plan");
          ret = OB_ERR_GEN_PLAN;
        }
        else
        {
          ObPhyOperator *exec_plan = physical_plan->get_phy_query(0);
          OB_ASSERT(exec_plan);
          if (OB_UNLIKELY(TBSYS_LOGGER._level >= TBSYS_LOG_LEVEL_DEBUG))
          {
            char buff[1024];
            exec_plan->to_string(buff, 1024);
            TBSYS_LOG(DEBUG, "ExecutionPlan: \n%s", buff);
          }
          // Step 5: start execution
          if (OB_SUCCESS != (ret = exec_plan->open()))
          {
            TBSYS_LOG(WARN, "failed to start execution, err=%d", ret);
          }
          else
          {
            result.set_execution_plan(exec_plan);
          }
        }
        destroy_plan(&logical_plan);
      }
      destroy_tree(parse_res.result_tree_);
    }
    parse_terminate(&parse_res);
  }
  return ret;
}
