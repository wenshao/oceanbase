/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * ob_groupby_operator.cpp is for what ...
 *
 * Version: $id: ob_groupby_operator.cpp,v 0.1 3/28/2011 10:35a wushi Exp $
 *
 * Authors:
 *   wushi <wushi.ly@taobao.com>
 *     - some work details if you want
 *
 */
#include "ob_groupby_operator.h"
#include "ob_ms_define.h"
#include <algorithm>
using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::common::hash;
using namespace oceanbase::mergeserver;

oceanbase::mergeserver::ObGroupByOperator::ObGroupByOperator()
{
  param_ = NULL;
  inited_ = false;
  max_avail_mem_size_ = -1;
}

oceanbase::mergeserver::ObGroupByOperator::~ObGroupByOperator()
{
  param_ = NULL;
  inited_ = false;
  max_avail_mem_size_ = -1;
}


void oceanbase::mergeserver::ObGroupByOperator::clear()
{
  int64_t memory_size_water_mark = OB_MS_THREAD_MEM_CACHE_LOWER_WATER_MARK;
  if (max_avail_mem_size_ > 0)
  {
    memory_size_water_mark = 2 * max_avail_mem_size_;
  }
  if (get_memory_size_used() > memory_size_water_mark)
  {
    ObCellArray::clear();
  }
  else
  {
    ObCellArray::reset();
  }
  if (group_hash_map_.size() > 0)
  {
    group_hash_map_.clear();
  }
  max_avail_mem_size_ = -1;
}

int oceanbase::mergeserver::ObGroupByOperator::init(const ObGroupByParam & param, 
                                                    const int64_t max_avail_mem_size)
{
  int err = OB_SUCCESS;
  param_ = &param;
  if (!inited_)
  {
    err = group_hash_map_.create(HASH_SLOT_NUM);
    if (OB_SUCCESS != err)
    {
      TBSYS_LOG(WARN,"fail to create hash table");
    }
    else
    {
      inited_ = true;
      max_avail_mem_size_ = max_avail_mem_size;
    }
  }
  return err;
}


int oceanbase::mergeserver::ObGroupByOperator::init_all_in_one_group_row()
{
  int err = OB_SUCCESS;
  if ((0 >= param_->get_aggregate_columns().get_array_index())
      ||(0 != param_->get_groupby_columns().get_array_index())
      ||(0 != this->get_cell_size()))
  {
    TBSYS_LOG(WARN,"aggregate function must act all records and result must be empty "
              "[param_->get_aggregate_columns().size():%ld,"
              "param_->get_groupby_columns().size():%ld,"
              "this->get_cell_size():%ld]", param_->get_aggregate_columns().get_array_index(),
              param_->get_groupby_columns().get_array_index(),
              this->get_cell_size());
    err = OB_INVALID_ARGUMENT;
  }
  ObCellInfo empty_cell;
  ObInnerCellInfo *cell_out = NULL;
  for (int64_t i = 0;  OB_SUCCESS == err && i < param_->get_return_columns().get_array_index(); i++)
  {
    empty_cell.value_.reset();
    err = this->append(empty_cell, cell_out);
    if (OB_SUCCESS != err)
    {
      TBSYS_LOG(WARN,"fail to append return columns into aggregate cell array [err:%d]", err);
    }
  }
  for (int64_t i = 0; 
      (i < static_cast<int64_t>(param_->get_aggregate_columns().get_array_index())) && (OB_SUCCESS == err); 
      i++)
  {
    empty_cell.value_.reset();
    err = param_->get_aggregate_columns().at(i)->init_aggregate_obj(empty_cell.value_);
    if (OB_SUCCESS == err)
    {
      err = this->append(empty_cell, cell_out);
      if (OB_SUCCESS != err)
      {
        TBSYS_LOG(WARN,"fail to append cell to cell array [err:%d]", err);
      }
    }
    else
    {
      TBSYS_LOG(WARN,"fail to init aggregate object [err:%d, idx:%ld]", err, i);
    }
  }
  if (OB_SUCCESS == err)
  {
    ObGroupKey agg_key;
    int64_t agg_row_beg = 0;
    err = agg_key.init(*this,*param_, agg_row_beg, ObCellArray::get_cell_size() - 1, ObGroupKey::AGG_KEY);
    if (OB_SUCCESS == err)
    {
      int32_t hash_err = group_hash_map_.set(agg_key,agg_row_beg);
      if (HASH_INSERT_SUCC != hash_err)
      {
        err = OB_ERROR;
        TBSYS_LOG(WARN,"fail to set hash value of current group [err:%d]", hash_err);
      }
    }
    else
    {
      TBSYS_LOG(WARN,"fail to init aggregate group key [err:%d]", err);
    }
  }
  return err;
}

int oceanbase::mergeserver::ObGroupByOperator::add_row(const ObCellArray & org_cells, 
                                                       const int64_t row_beg, const int64_t row_end)
{
  int err = OB_SUCCESS;
  ObGroupKey org_key;
  ObGroupKey agg_key;
  ObInnerCellInfo * cell_out = NULL;
  if (NULL == param_)
  {
    TBSYS_LOG(WARN,"initialize first [param_:%p]", param_);
    err = OB_INVALID_ARGUMENT;
  }
  if (OB_SUCCESS == err)
  {
    if (row_beg < 0
        || row_end < 0
        || row_beg > row_end
        || row_end >= org_cells.get_cell_size())
    {
      TBSYS_LOG(WARN,"param error [row_beg:%ld,row_end:%ld,org_cells.get_cell_size():%ld]", 
                row_beg, row_end, org_cells.get_cell_size());
      err = OB_INVALID_ARGUMENT;
    }
  }
  if (OB_SUCCESS == err)
  {
    /// need not aggregate
    if (param_->get_aggregate_row_width() == 0)
    {
      for (int64_t i = row_beg; i <= row_end && OB_SUCCESS == err; i++)
      {
        err = ObCellArray::append(org_cells[i],cell_out);
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN,"fail to append cell to cell array [err:%d]", err);
        }
      }
    }
    else
    {
      err = org_key.init(org_cells,*param_,row_beg,row_end, ObGroupKey::ORG_KEY);
      if (OB_SUCCESS != err)
      {
        TBSYS_LOG(WARN,"fail to initialize group key [err:%d]", err);
      }
      if (OB_SUCCESS == err)
      {
        int64_t agg_row_beg = -1;
        int hash_err = OB_SUCCESS;
        hash_err = group_hash_map_.get(org_key,agg_row_beg);
        if (HASH_EXIST == hash_err)
        {
          err = param_->aggregate(org_cells,row_beg, row_end, *this, agg_row_beg, 
                                  agg_row_beg + param_->get_aggregate_row_width() - 1);
          if (OB_SUCCESS != err)
          {
            TBSYS_LOG(WARN,"fail to aggregate org row [err:%d]", err);
          }
        }
        else
        {
          agg_row_beg = ObCellArray::get_cell_size();
          err = param_->aggregate(org_cells,row_beg, row_end, *this, agg_row_beg, 
                                  agg_row_beg + param_->get_aggregate_row_width() - 1);
          if (OB_SUCCESS != err)
          {
            TBSYS_LOG(WARN,"fail to aggregate org row [err:%d]", err);
          }
          if (OB_SUCCESS == err)
          {
            err = agg_key.init(*this,*param_, agg_row_beg, ObCellArray::get_cell_size() - 1, ObGroupKey::AGG_KEY);
            if (OB_SUCCESS != err)
            {
              TBSYS_LOG(WARN,"fail to init aggregate group key [err:%d]", err);
            }
          }
          if (OB_SUCCESS == err)
          {
            hash_err = group_hash_map_.set(agg_key,agg_row_beg);
            if (HASH_INSERT_SUCC != hash_err)
            {
              err = OB_ERROR;
              TBSYS_LOG(WARN,"fail to set hash value of current group [err:%d]", hash_err);
            }
          }
        }
      }
    }
  }
  return err;
}
