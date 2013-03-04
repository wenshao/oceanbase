/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_set_operator.cpp
 *
 * Authors:
 *   TIAN GUAN <tianguan.dgb@taobao.com>
 *
 */

#include "ob_set_operator.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;

ObSetOperator::ObSetOperator()
{
}

 ObSetOperator::~ObSetOperator()
{
}

int ObSetOperator::get_next_row(const ObRow *&row)
{
  row = NULL;
  return OB_ERROR;
}

int ObSetOperator::set_distinct(bool is_distinct)
{
  distinct_ = is_distinct;
  return OB_SUCCESS;
}

