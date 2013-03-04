/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_row.cpp
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#include "ob_row.h"
using namespace oceanbase::common;

ObRow::ObRow()
  :row_desc_(NULL)
{
}

ObRow::~ObRow()
{
}

void ObRow::assign(const ObRow &other)
{
  this->raw_row_.assign(other.raw_row_);
  this->row_desc_ = other.row_desc_;
}

ObRow::ObRow(const ObRow &other)
{
  this->assign(other);
}

ObRow &ObRow::operator= (const ObRow &other)
{
  this->assign(other);
  return *this;
}

void ObRow::set_row_desc(const ObRowDesc &row_desc)
{
  row_desc_ = &row_desc;
}

int ObRow::get_cell(const uint64_t table_id, const uint64_t column_id, const common::ObObj *&cell) const
{
  int ret = OB_SUCCESS;
  int64_t cell_idx = OB_INVALID_INDEX;
  if (NULL == row_desc_
      || OB_INVALID_INDEX == (cell_idx = row_desc_->get_idx(table_id, column_id)))
  {
    TBSYS_LOG(WARN, "failed to find cell, tid=%lu cid=%lu", table_id, column_id);
    ret = OB_INVALID_ARGUMENT;
  }
  else
  {
    ret = raw_row_.get_cell(cell_idx, cell);
  }
  return ret;
}

int ObRow::get_cell(const uint64_t table_id, const uint64_t column_id, common::ObObj *&cell)
{
  int ret = OB_SUCCESS;
  int64_t cell_idx = OB_INVALID_INDEX;
  if (NULL == row_desc_
      || OB_INVALID_INDEX == (cell_idx = row_desc_->get_idx(table_id, column_id)))
  {
    TBSYS_LOG(WARN, "failed to find cell, tid=%lu cid=%lu", table_id, column_id);
    ret = OB_INVALID_ARGUMENT;
  }
  else
  {
    ret = raw_row_.get_cell(cell_idx, cell);
  }
  return ret;
}

int ObRow::set_cell(const uint64_t table_id, const uint64_t column_id, const common::ObObj &cell)
{
  int ret = OB_SUCCESS;
  int64_t cell_idx = OB_INVALID_INDEX;
  if (NULL == row_desc_
      || OB_INVALID_INDEX == (cell_idx = row_desc_->get_idx(table_id, column_id)))
  {
    TBSYS_LOG(WARN, "failed to find cell, tid=%lu cid=%lu row_desc=%p", table_id, column_id, row_desc_);
    ret = OB_INVALID_ARGUMENT;
  }
  else if (OB_SUCCESS != (ret = raw_row_.set_cell(cell_idx, cell)))
  {
    TBSYS_LOG(WARN, "failed to get cell, err=%d", ret);
  }
  return ret;
}

int64_t ObRow::get_column_num() const
{
  int64_t ret = -1;
  if (NULL == row_desc_)
  {
    TBSYS_LOG(ERROR, "row_desc_ is NULL");
  }
  else
  {
    ret = row_desc_->get_column_num();
  }
  return ret;
}

int ObRow::raw_get_cell(const int64_t cell_idx, const common::ObObj *&cell, uint64_t &table_id, uint64_t &column_id) const
{
  int ret = OB_SUCCESS;
  int64_t cells_count = get_column_num();
  if (NULL == row_desc_)
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(ERROR, "row_desc_ is NULL");
  }
  else if (cell_idx >= cells_count)
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "invalid cell_idx=%ld cells_count=%ld", cell_idx, cells_count);
  }
  else if (OB_SUCCESS != (ret = row_desc_->get_tid_cid(cell_idx, table_id, column_id)))
  {
    TBSYS_LOG(WARN, "failed to get tid and cid, err=%d", ret);
  }
  else
  {
    ret = raw_row_.get_cell(cell_idx, cell);
  }
  return ret;
}

int ObRow::raw_set_cell(const int64_t cell_idx, const common::ObObj &cell)
{
  int ret = OB_SUCCESS;
  int64_t cells_count = get_column_num();
  if (NULL == row_desc_)
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(ERROR, "row_desc_ is NULL");
  }
  else if (cell_idx >= cells_count)
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "invalid cell_idx=%ld cells_count=%ld", cell_idx, cells_count);
  }
  else if (OB_SUCCESS != (ret = raw_row_.set_cell(cell_idx, cell)))
  {
    TBSYS_LOG(WARN, "failed to get cell, err=%d idx=%ld", ret, cell_idx);
  }
  return ret;
}

void ObRow::dump() const
{
  int64_t cells_count = get_column_num();
  int64_t cell_idx = 0;
  uint64_t tid = 0, cid = 0;
  const ObObj *cell = NULL;
  TBSYS_LOG(DEBUG, "[obrow.dump begin]");
  for (cell_idx = 0; cell_idx < cells_count; cell_idx++)
  {
    if (OB_SUCCESS != raw_get_cell(cell_idx, cell, tid, cid))
    {
      TBSYS_LOG(WARN, "fail to dump ObRow");
      break;
    }
    if (NULL != cell)
    {
      TBSYS_LOG(DEBUG, "-------  tid=%lu, cid=%lu  -------", tid, cid);
      cell->dump();
    }
  }
  TBSYS_LOG(DEBUG, "[obrow.dump end]");
}
