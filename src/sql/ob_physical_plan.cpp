/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_physical_plan.cpp
 *
 * Authors:
 *   Guibin Du <tianguan.dgb@taobao.com>
 *
 */
#include "ob_physical_plan.h"
#include "common/utility.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;

ObPhysicalPlan::~ObPhysicalPlan()
{
  for(int32_t i = 0; i < operators_store_.count(); i++)
  {
    // we can not delete, because it will release space which is not allocated
    // delete operators_store_.at(i);
    operators_store_.at(i)->~ObPhyOperator();
  }
}

int ObPhysicalPlan::add_phy_query(ObPhyOperator *phy_query, int64_t& idx)
{
  int ret = OB_SUCCESS;
  if (phy_querys_.push_back(phy_query) == OB_SUCCESS)
  {
    idx = phy_querys_.count() - 1;
  }
  else
  {
    ret = OB_ERROR;
  }
  return ret;
}

int ObPhysicalPlan::add_phy_operator(ObPhyOperator *op)
{
  return operators_store_.push_back(op);
}

ObPhyOperator* ObPhysicalPlan::get_phy_query(int64_t index) const
{
  ObPhyOperator *op = NULL;
  if (index >= 0 && index < phy_querys_.count())
    op = phy_querys_.at(index);
  return op;
}

int64_t ObPhysicalPlan::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  databuff_printf(buf, buf_len, pos, "\nObPhysicalPlan Begin\n");
  for (int64_t i = 0; i < phy_querys_.count(); ++i)
  {
    if (i == phy_querys_.count() - 1)
      databuff_printf(buf, buf_len, pos, "Main Query:\n");
    else
      databuff_printf(buf, buf_len, pos, "Sub-Query: %ld\n", i);
    int64_t pos2 = phy_querys_.at(i)->to_string(buf + pos, buf_len-pos);
    pos += pos2;
  }
  databuff_printf(buf, buf_len, pos, "ObPhysicalPlan End\n");
  return pos;
}

