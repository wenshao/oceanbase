/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_rowkey_phy_operator.cpp
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@taobao.com>
 *
 */

#include "ob_rowkey_phy_operator.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

int ObRowkeyPhyOperator::get_next_row(const common::ObRow *&row)
{
  int ret = common::OB_NOT_IMPLEMENT;
  UNUSED(row);
  TBSYS_LOG(ERROR, "This function should not be used in ObRowkeyPhyOperator");
  return ret;
}

int ObRowkeyPhyOperator::get_row_desc(const common::ObRowDesc *&row_desc) const
{
  row_desc = NULL;
  TBSYS_LOG(ERROR, "This function should not be used in ObRowkeyPhyOperator");
  return OB_NOT_IMPLEMENT;
}
