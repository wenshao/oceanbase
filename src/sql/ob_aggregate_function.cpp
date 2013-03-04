/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_aggregate_function.cpp
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#include "ob_aggregate_function.h"
using namespace oceanbase::sql;
using namespace oceanbase::common;

ObAggregateFunction::ObAggregateFunction()
  :aggr_columns_(NULL), varchar_buffs_count_(0), did_int_div_as_double_(false)
{
  memset(varchar_buffs_, 0, sizeof(varchar_buffs_));
}

ObAggregateFunction::~ObAggregateFunction()
{
  destroy();
}

int ObAggregateFunction::init(const ObRowDesc &input_row_desc, common::ObArray<ObSqlExpression> &aggr_columns)
{
  int ret = OB_SUCCESS;
  // copy reference of aggr_column
  aggr_columns_ = &aggr_columns;
  // copy row desc
  row_desc_ = input_row_desc;
  // add aggr columns
  for (int64_t i = 0; i < aggr_columns_->count(); ++i)
  {
    const ObSqlExpression &cexpr = aggr_columns_->at(i);
    if (OB_SUCCESS != (ret = row_desc_.add_column_desc(cexpr.get_table_id(),
                                                       cexpr.get_column_id())))
    {
      TBSYS_LOG(WARN, "failed to add column desc, err=%d", ret);
      break;
    }
  } // end for
  curr_row_.set_row_desc(row_desc_);
  ret = init_dedup_sets();
  return ret;
}

void ObAggregateFunction::destroy()
{
  //free_varchar_mem
  for (int64_t i = 0; i < varchar_buffs_count_; ++i)
  {
    ob_free(varchar_buffs_[i]);
    varchar_buffs_[i] = NULL;
  }
  varchar_buffs_count_ = 0;
  row_desc_.reset();
  aggr_columns_ = NULL;
  destroy_dedup_sets();
}

int ObAggregateFunction::clone_expr_cell(const ObExprObj &cell, ObExprObj &cell_clone)
{
  int ret = OB_SUCCESS;
  if (ObVarcharType == cell.get_type())
  {
    ObString varchar;
    cell.get_varchar(varchar);
    ObString varchar_clone;
    if (varchar.length() > OB_MAX_VARCHAR_LENGTH)
    {
      ret = OB_SIZE_OVERFLOW;
      TBSYS_LOG(ERROR, "varchar too long, length=%d", varchar.length());
    }
    else if (ObVarcharType == cell_clone.get_type())
    {
      cell_clone.get_varchar(varchar_clone);
      OB_ASSERT(varchar_clone.ptr());
      varchar_clone.assign_buffer(varchar_clone.ptr(), OB_MAX_VARCHAR_LENGTH);
    }
    else
    {
      char* buff = static_cast<char*>(ob_malloc(OB_MAX_VARCHAR_LENGTH));
      if (NULL == buff)
      {
        TBSYS_LOG(ERROR, "no memory");
        ret = OB_ALLOCATE_MEMORY_FAILED;
      }
      else
      {
        varchar_clone.assign_buffer(buff, OB_MAX_VARCHAR_LENGTH);
        OB_ASSERT(varchar_buffs_count_ < common::OB_ROW_MAX_COLUMNS_COUNT);
        varchar_buffs_[varchar_buffs_count_++] = buff;
      }
    }
    if (OB_SUCCESS == ret)
    {
      if (varchar.length() != varchar_clone.write(varchar.ptr(), varchar.length()))
      {
        ret = OB_ERR_UNEXPECTED;
        TBSYS_LOG(ERROR, "failed to write varchar, length=%d", varchar.length());
      }
      else
      {
        cell_clone.set_varchar(varchar_clone);
      }
    }
  }
  else
  {
    cell_clone = cell;
  }
  return ret;
}

int ObAggregateFunction::clone_cell(const ObObj &cell, ObObj &cell_clone)
{
  int ret = OB_SUCCESS;
  if (ObVarcharType == cell.get_type())
  {
    ObString varchar;
    cell.get_varchar(varchar);
    ObString varchar_clone;
    if (varchar.length() > OB_MAX_VARCHAR_LENGTH)
    {
      ret = OB_SIZE_OVERFLOW;
      TBSYS_LOG(ERROR, "varchar too long, length=%d", varchar.length());
    }
    else if (ObVarcharType == cell_clone.get_type())
    {
      cell_clone.get_varchar(varchar_clone);
      OB_ASSERT(varchar_clone.ptr());
      varchar_clone.assign_buffer(varchar_clone.ptr(), OB_MAX_VARCHAR_LENGTH);
    }
    else
    {
      char* buff = static_cast<char*>(ob_malloc(OB_MAX_VARCHAR_LENGTH));
      if (NULL == buff)
      {
        TBSYS_LOG(ERROR, "no memory");
        ret = OB_ALLOCATE_MEMORY_FAILED;
      }
      else
      {
        varchar_clone.assign_buffer(buff, OB_MAX_VARCHAR_LENGTH);
        OB_ASSERT(varchar_buffs_count_ < common::OB_ROW_MAX_COLUMNS_COUNT);
        varchar_buffs_[varchar_buffs_count_++] = buff;
      }
    }
    if (OB_SUCCESS == ret)
    {
      if (varchar.length() != varchar_clone.write(varchar.ptr(), varchar.length()))
      {
        ret = OB_ERR_UNEXPECTED;
        TBSYS_LOG(ERROR, "failed to write varchar, length=%d", varchar.length());
      }
      else
      {
        cell_clone.set_varchar(varchar_clone);
      }
    }
  }
  else
  {
    cell_clone = cell;
  }
  return ret;
}

int ObAggregateFunction::prepare(const ObRow &input_row)
{
  int ret = OB_SUCCESS;
  const ObObj *cell1 = NULL;
  ObObj *cell2 = NULL;
  uint64_t tid = OB_INVALID_ID;
  uint64_t cid = OB_INVALID_ID;
  for (int64_t i = 0; OB_SUCCESS == ret && i < input_row.get_column_num(); ++i)
  {
    if (OB_SUCCESS != (ret = input_row.raw_get_cell(i, cell1, tid, cid)))
    {
      TBSYS_LOG(WARN, "failed to get cell, err=%d i=%ld", ret, i);
    }
    else if (OB_SUCCESS != (ret = curr_row_.get_cell(tid, cid, cell2)))
    {
      TBSYS_LOG(WARN, "failed to get cell, err=%d", ret);
    }
    else if (OB_SUCCESS != (ret = clone_cell(*cell1, *cell2)))
    {
      TBSYS_LOG(WARN, "failed to clone cell, err=%d", ret);
    }
  } // end for
  OB_ASSERT(aggr_columns_);
  ObItemType aggr_fun;
  bool is_distinct = false;
  const ObObj *input_cell = NULL;
  ObExprObj *aggr_cell = NULL;
  ObExprObj *aux_cell = NULL;
  ObRow dedup_row;
  dedup_row.set_row_desc(dedup_row_desc_);
  bool has_distinct = false;
  for (int64_t i = 0; OB_SUCCESS == ret && i < aggr_columns_->count(); ++i)
  {
    ObSqlExpression &cexpr = aggr_columns_->at(i);
    tid = cexpr.get_table_id();
    cid = cexpr.get_column_id();
    if (OB_SUCCESS != (ret = cexpr.get_aggr_column(aggr_fun, is_distinct)))
    {
      TBSYS_LOG(WARN, "failed to get aggr column, err=%d", ret);
    }
    else if (OB_SUCCESS != (ret = cexpr.calc(input_row, input_cell)))
    {
      TBSYS_LOG(WARN, "failed to get calc cell, err=%d", ret);
    }
    else if (OB_SUCCESS != (ret = aggr_get_cell(tid, cid, aggr_cell)))
    {
      TBSYS_LOG(WARN, "failed to get cell, err=%d", ret);
    }
    else if (OB_SUCCESS != (ret = aux_get_cell(tid, cid, aux_cell)))
    {
      TBSYS_LOG(WARN, "failed to get cell, err=%d", ret);
    }
    else if (OB_SUCCESS != (ret = init_aggr_cell(aggr_fun, *input_cell, *aggr_cell, *aux_cell)))
    {
      TBSYS_LOG(WARN, "failed to init cell, err=%d", ret);
    }
    else if (is_distinct)
    {
      has_distinct = true;
      if (OB_SUCCESS != (ret = dedup_row.set_cell(tid, cid, *input_cell))) // collect distinct cells
      {
        TBSYS_LOG(WARN, "failed to set row cell, err=%d tid=%lu cid=%lu", ret, tid, cid);
      }
    }
  } // end for
  if (OB_SUCCESS == ret && has_distinct)
  {
    // store the cells and insert into dedup sets
    row_store_.clear();
    const ObRowStore::StoredRow *stored_row = NULL;
    if (OB_SUCCESS != (ret = row_store_.add_row(dedup_row, stored_row)))
    {
      TBSYS_LOG(WARN, "failed to add row into store, err=%d", ret);
    }
    else
    {
      for (int64_t i = 0; i < dedup_row.get_column_num(); ++i)
      {
        if (OB_SUCCESS != (ret = dedup_sets_[i].clear()))
        {
          TBSYS_LOG(WARN, "failed to clear hash set, err=%d idx=%ld", ret, i);
        }
        else if (hash::HASH_INSERT_SUCC != (ret = dedup_sets_[i].set(&stored_row->reserved_cells_[i])))
        {
          TBSYS_LOG(WARN, "failed to insert into hash set, err=%d", ret);
        }
        else
        {
          ret = OB_SUCCESS;
        }
      } // end for
    }
  }
  return ret;
}

int ObAggregateFunction::aggr_get_cell(const uint64_t table_id, const uint64_t column_id, common::ObExprObj *&cell)
{
  int ret = OB_SUCCESS;
  int64_t cell_idx = OB_INVALID_INDEX;
  if (OB_INVALID_INDEX == (cell_idx = curr_row_.get_row_desc()->get_idx(table_id, column_id)))
  {
    TBSYS_LOG(WARN, "failed to find cell, tid=%lu cid=%lu", table_id, column_id);
    ret = OB_INVALID_ARGUMENT;
  }
  else
  {
    cell = &aggr_cells_[cell_idx];
  }
  return ret;
}

int ObAggregateFunction::aux_get_cell(const uint64_t table_id, const uint64_t column_id, common::ObExprObj *&cell)
{
  int ret = OB_SUCCESS;
  int64_t cell_idx = OB_INVALID_INDEX;
  if (OB_INVALID_INDEX == (cell_idx = curr_row_.get_row_desc()->get_idx(table_id, column_id)))
  {
    TBSYS_LOG(WARN, "failed to find cell, tid=%lu cid=%lu", table_id, column_id);
    ret = OB_INVALID_ARGUMENT;
  }
  else
  {
    cell = &aux_cells_[cell_idx];
  }
  return ret;
}

int ObAggregateFunction::process(const ObRow &input_row)
{
  int ret = OB_SUCCESS;
  OB_ASSERT(aggr_columns_);
  const ObObj *input_cell = NULL;
  ObExprObj *aggr_cell = NULL;
  ObExprObj *aux_cell = NULL;
  ObItemType aggr_fun;
  bool is_distinct = false;
  uint64_t tid = OB_INVALID_ID;
  uint64_t cid = OB_INVALID_ID;
  bool is_qualified[common::OB_ROW_MAX_COLUMNS_COUNT];
  memset(is_qualified, 0, sizeof(is_qualified));
  ObRow dedup_row;
  dedup_row.set_row_desc(dedup_row_desc_);
  if (0 < dedup_row_desc_.get_column_num())
  {
    // has distinct
    int64_t dedup_cell_idx = 0;
    bool has_qualified = false;
    for (int64_t i = 0; OB_SUCCESS == ret && i < aggr_columns_->count(); ++i)
    {
      // for each distinct aggr column
      ObSqlExpression &cexpr = aggr_columns_->at(i);
      tid = cexpr.get_table_id();
      cid = cexpr.get_column_id();
      if (OB_SUCCESS != (ret = cexpr.get_aggr_column(aggr_fun, is_distinct)))
      {
        TBSYS_LOG(WARN, "failed to get aggr column, err=%d", ret);
      }
      else if (is_distinct)
      {
        if (OB_SUCCESS != (ret = cexpr.calc(input_row, input_cell)))
        {
          TBSYS_LOG(WARN, "failed to calc cell, err=%d", ret);
        }
        else if (OB_SUCCESS != (ret = dedup_row.set_cell(tid, cid, *input_cell))) // collect distinct cells
        {
          TBSYS_LOG(WARN, "failed to set row cell, err=%d tid=%lu cid=%lu", ret, tid, cid);
        }
        else if (hash::HASH_EXIST == (ret = dedup_sets_[i].exist(input_cell)))
        {
          is_qualified[dedup_cell_idx++] = false;
          ret = OB_SUCCESS;
        }
        else if (hash::HASH_NOT_EXIST == ret)
        {
          is_qualified[dedup_cell_idx++] = true;
          ret = OB_SUCCESS;
          has_qualified = true;
        }
      }
    } // end for
    if (OB_SUCCESS == ret && has_qualified)
    {
      // store the cells and insert into dedup sets
      const ObRowStore::StoredRow *stored_row = NULL;
      if (OB_SUCCESS != (ret = row_store_.add_row(dedup_row, stored_row)))
      {
        TBSYS_LOG(WARN, "failed to add row into store, err=%d", ret);
      }
      else
      {
        for (int64_t i = 0; i < dedup_row.get_column_num(); ++i)
        {
          if (is_qualified[i])
          {
            if (hash::HASH_INSERT_SUCC != (ret = dedup_sets_[i].set(&stored_row->reserved_cells_[i])))
            {
              TBSYS_LOG(WARN, "failed to insert into hash set, err=%d", ret);
              break;
            }
            else
            {
              ret = OB_SUCCESS;
            }
          }
        } // end for
      }
    }
  } // end if has_distinct

  int64_t dedup_cell_idx = 0;
  for (int64_t i = 0; OB_SUCCESS == ret && i < aggr_columns_->count(); ++i)
  {
    ObSqlExpression &cexpr = aggr_columns_->at(i);
    tid = cexpr.get_table_id();
    cid = cexpr.get_column_id();
    if (OB_SUCCESS != (ret = cexpr.get_aggr_column(aggr_fun, is_distinct)))
    {
      TBSYS_LOG(WARN, "failed to get aggr column, err=%d", ret);
    }
    else if (OB_SUCCESS != (ret = aggr_get_cell(tid, cid, aggr_cell)))
    {
      TBSYS_LOG(WARN, "failed to get cell, err=%d", ret);
    }
    else if (OB_SUCCESS != (ret = aux_get_cell(tid, cid, aux_cell)))
    {
      TBSYS_LOG(WARN, "failed to get cell, err=%d", ret);
    }
    else if (is_distinct)
    {
      // we already know whether the cell is qualified
      if (is_qualified[dedup_cell_idx])
      {
        if (OB_SUCCESS != (ret = dedup_row.raw_get_cell(dedup_cell_idx, input_cell, tid, cid)))
        {
          TBSYS_LOG(WARN, "failed to get raw cell, err=%d", ret);
        }
        else if (OB_SUCCESS != (ret = calc_aggr_cell(aggr_fun, *input_cell, *aggr_cell, *aux_cell)))
        {
          TBSYS_LOG(WARN, "failed to calculate aggr cell, err=%d", ret);
        }
        else
        {
          ++dedup_cell_idx;
        }
      }
    }
    else
    {
      // not distinct aggr column
      if (OB_SUCCESS != (ret = cexpr.calc(input_row, input_cell)))
      {
        TBSYS_LOG(WARN, "failed to calc cell, err=%d", ret);
      }
      else if (OB_SUCCESS != (ret = calc_aggr_cell(aggr_fun, *input_cell, *aggr_cell, *aux_cell)))
      {
        TBSYS_LOG(WARN, "failed to calculate aggr cell, err=%d", ret);
      }
    }
  } // end for
  return ret;
}

int ObAggregateFunction::init_aggr_cell(const ObItemType aggr_fun, const ObObj &oprand, ObExprObj &res1, ObExprObj &res2)
{
  int ret = OB_SUCCESS;
  ObExprObj oprand_clone;
  oprand_clone.assign(oprand);
  res2.set_int(0);  // count
  switch(aggr_fun)
  {
    case T_FUN_COUNT:
      if (!oprand.is_null())
      {
        res1.set_int(1);
        res2.set_int(1);
      }
      else
      {
        res1.set_int(0);
      }
      break;
    case T_FUN_MAX:
    case T_FUN_MIN:
    case T_FUN_SUM:
    case T_FUN_AVG:
      ret = clone_expr_cell(oprand_clone, res1);
      if (!oprand.is_null())
      {
        res2.set_int(1);
      }
      break;
    default:
      ret = OB_ERR_UNEXPECTED;
      TBSYS_LOG(ERROR, "unknown aggr function type, t=%d", aggr_fun);
      break;
  }
  return ret;
}

int ObAggregateFunction::calc_aggr_cell(const ObItemType aggr_fun, const ObObj &oprand, ObExprObj &res1, ObExprObj &res2)
{
  int ret = OB_SUCCESS;
  if (!oprand.is_null())
  {
    ObExprObj oprand_clone;
    oprand_clone.assign(oprand);
    ObExprObj one;
    ObExprObj result;
    one.set_int(1);
    ret = res2.add(one, result);
    if (OB_SUCCESS == ret)
    {
      res2 = result;
      switch(aggr_fun)
      {
        case T_FUN_COUNT:
          ret = res1.add(one, result);
          if (OB_SUCCESS == ret)
          {
            res1 = result;
          }
          break;
        case T_FUN_MAX:
          res1.lt(oprand_clone, result);
          if (result.is_true())
          {
            ret = clone_expr_cell(oprand_clone, res1);
          }
          break;
        case T_FUN_MIN:
          oprand_clone.lt(res1, result);
          if (result.is_true())
          {
            ret = clone_expr_cell(oprand_clone, res1);
          }
          break;
        case T_FUN_SUM:
          ret = res1.add(oprand_clone, result);
          if (OB_SUCCESS == ret)
          {
            res1 = result;
          }
          break;
        case T_FUN_AVG:
          ret = res1.add(oprand_clone, result);
          if (OB_SUCCESS == ret)
          {
            res1 = result;
          }
          break;
        default:
          ret = OB_ERR_UNEXPECTED;
          TBSYS_LOG(ERROR, "unknown aggr function type, t=%d", aggr_fun);
          break;
      }
    }
  }
  return ret;
}

int ObAggregateFunction::get_result(const ObRow *&row)
{
  int ret = OB_SUCCESS;
  OB_ASSERT(aggr_columns_);
  ObObj *res_cell = NULL;
  ObExprObj *aggr_cell = NULL;
  ObExprObj *aux_cell = NULL;
  ObExprObj result;
  for (int64_t i = 0; OB_SUCCESS == ret && i < aggr_columns_->count(); ++i)
  {
    const ObSqlExpression &cexpr = aggr_columns_->at(i);
    ObItemType aggr_fun;
    bool is_distinct = false;
    if (OB_SUCCESS != (ret = cexpr.get_aggr_column(aggr_fun, is_distinct)))
    {
      TBSYS_LOG(WARN, "failed to get aggr column, err=%d", ret);
    }
    else if (OB_SUCCESS != (ret = aggr_get_cell(cexpr.get_table_id(), cexpr.get_column_id(), aggr_cell)))
    {
      TBSYS_LOG(WARN, "failed to get cell, err=%d", ret);
    }
    else if (OB_SUCCESS != (ret = aux_get_cell(cexpr.get_table_id(), cexpr.get_column_id(), aux_cell)))
    {
      TBSYS_LOG(WARN, "failed to get cell, err=%d", ret);
    }
    else if (OB_SUCCESS != (ret = curr_row_.get_cell(cexpr.get_table_id(), cexpr.get_column_id(), res_cell)))
    {
      TBSYS_LOG(WARN, "failed to get cell, err=%d", ret);
    }
    else
    {
      switch(aggr_fun)
      {
        case T_FUN_COUNT:
          ret = aggr_cell->to(*res_cell);
          break;
        case T_FUN_MAX:
        case T_FUN_MIN:
        case T_FUN_SUM:
          if (aux_cell->is_zero())
          {
            res_cell->set_null();
          }
          else
          {
            ret = aggr_cell->to(*res_cell);
          }
          break;
        case T_FUN_AVG:
          if (aux_cell->is_zero())
          {
            res_cell->set_null();
          }
          else
          {
            ret = aggr_cell->div(*aux_cell, result, did_int_div_as_double_);
            if (OB_SUCCESS == ret)
            {
              ret = result.to(*res_cell);
            }
          }
          break;
        default:
          ret = OB_ERR_UNEXPECTED;
          TBSYS_LOG(ERROR, "unknown aggr function type, t=%d", aggr_fun);
          break;
      } // end switch
    }
  } // end for
  if (OB_SUCCESS == ret)
  {
    row = &curr_row_;
  }
  return ret;
}

int ObAggregateFunction::init_dedup_sets()
{
  int ret = OB_SUCCESS;
  uint64_t tid = OB_INVALID_ID;
  uint64_t cid = OB_INVALID_ID;
  ObItemType aggr_fun;
  bool is_distinct = false;
  for (int64_t i = 0; OB_SUCCESS == ret && i < aggr_columns_->count(); ++i)
  {
    ObSqlExpression &cexpr = aggr_columns_->at(i);
    tid = cexpr.get_table_id();
    cid = cexpr.get_column_id();
    if (OB_SUCCESS != (ret = cexpr.get_aggr_column(aggr_fun, is_distinct)))
    {
      TBSYS_LOG(WARN, "failed to get aggr column, err=%d", ret);
    }
    else if (i >= common::OB_ROW_MAX_COLUMNS_COUNT)
    {
      ret = OB_SIZE_OVERFLOW;
      TBSYS_LOG(WARN, "too many aggr column, i=%ld max=%ld", i, common::OB_ROW_MAX_COLUMNS_COUNT);
    }
    else if (is_distinct)
    {
      // init the cell_idx'th dedup set
      if (OB_SUCCESS != (ret = row_store_.add_reserved_column(tid, cid)))
      {
        TBSYS_LOG(WARN, "failed to add reserved column, err=%d", ret);
      }
      else if (OB_SUCCESS != (ret = dedup_row_desc_.add_column_desc(tid, cid)))
      {
        TBSYS_LOG(WARN, "failed to add column desc, err=%d", ret);
      }
      else if (OB_SUCCESS != (ret = dedup_sets_[i].create(DEDUP_HASH_SET_SIZE)))
      {
        TBSYS_LOG(WARN, "failed to create hash set, err=%d", ret);
      }
    }
  } // end for
  return ret;
}

void ObAggregateFunction::destroy_dedup_sets()
{
  row_store_.clear();
  for (int64_t i = 0; i < dedup_row_desc_.get_column_num(); ++i)
  {
    if (OB_SUCCESS != dedup_sets_[i].destroy())
    {
      TBSYS_LOG(WARN, "failed to destroy hashset, i=%ld", i);
    }
  }
  dedup_row_desc_.reset();
}
