/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_raw_row.cpp
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#include "ob_raw_row.h"
using namespace oceanbase::common;
ObRawRow::ObRawRow()
  :cells_count_(0), reserved1_(0), reserved2_(0)
{
}

ObRawRow::~ObRawRow()
{
}

void ObRawRow::assign(const ObRawRow &other)
{
  if (this != &other)
  {
    for (int16_t i = 0; i < other.cells_count_; ++i)
    {
      this->cells_[i] = other.cells_[i];
    }
    this->cells_count_ = other.cells_count_;
  }
}

int ObRawRow::add_cell(const common::ObObj &cell)
{
  int ret = OB_SUCCESS;
  if (cells_count_ >= MAX_COLUMNS_COUNT)
  {
    TBSYS_LOG(WARN, "array overflow, cells_count=%hd", cells_count_);
    ret = OB_SIZE_OVERFLOW;
  }
  else
  {
    cells_[cells_count_++] = cell;
  }
  return ret;
}

int ObRawRow::get_cell(const int64_t i, const common::ObObj *&cell) const
{
  int ret = OB_SUCCESS;
  if (0 > i || i >= MAX_COLUMNS_COUNT)
  {
    TBSYS_LOG(WARN, "invalid index, count=%hd i=%ld", cells_count_, i);
    ret = OB_INVALID_ARGUMENT;
  }
  else
  {
    cell = &cells_[i];
  }
  return ret;
}

int ObRawRow::get_cell(const int64_t i, common::ObObj *&cell)
{
  int ret = OB_SUCCESS;
  if (0 > i || i >= MAX_COLUMNS_COUNT)
  {
    TBSYS_LOG(WARN, "invalid index, count=%hd i=%ld", cells_count_, i);
    ret = OB_INVALID_ARGUMENT;
  }
  else
  {
    cell = &cells_[i];
  }
  return ret;
}

int ObRawRow::set_cell(const int64_t i, const common::ObObj &cell)
{
  int ret = OB_SUCCESS;
  if (0 > i || i >= MAX_COLUMNS_COUNT)
  {
    TBSYS_LOG(WARN, "invalid index, count=%ld i=%ld", MAX_COLUMNS_COUNT, i);
    ret = OB_INVALID_ARGUMENT;
  }
  else
  {
    cells_[i] = cell;
    if (i >= cells_count_)
    {
      cells_count_ = static_cast<int16_t>(1+i);
    }
  }
  return ret;
}
