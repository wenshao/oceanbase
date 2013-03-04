/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_merge_join.cpp
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#include "ob_merge_join.h"
#include "common/utility.h"
#include "common/ob_row_util.h"
using namespace oceanbase::sql;
using namespace oceanbase::common;

ObMergeJoin::ObMergeJoin()
  :get_next_row_func_(NULL),
   last_left_row_(NULL),
   last_right_row_(NULL),
   right_cache_is_valid_(false),
   is_right_iter_end_(false)
{
}

ObMergeJoin::~ObMergeJoin()
{
  char *store_buf = last_join_left_row_store_.ptr();
  if (NULL != store_buf)
  {
    ob_free(store_buf);
    last_join_left_row_store_.assign_ptr(NULL, 0);
  }
}

int ObMergeJoin::set_join_type(const ObJoin::JoinType join_type)
{
  int ret = OB_SUCCESS;
  ObJoin::set_join_type(join_type);
  switch(join_type)
  {
    case INNER_JOIN:
      get_next_row_func_ = &ObMergeJoin::inner_get_next_row;
      break;
    case LEFT_OUTER_JOIN:
      get_next_row_func_ = &ObMergeJoin::left_outer_get_next_row;
      break;
    case RIGHT_OUTER_JOIN:
      get_next_row_func_ = &ObMergeJoin::right_outer_get_next_row;
      break;
    case FULL_OUTER_JOIN:
      get_next_row_func_ = &ObMergeJoin::full_outer_get_next_row;
      break;
    case LEFT_SEMI_JOIN:
      get_next_row_func_ = &ObMergeJoin::left_semi_get_next_row;
      break;
    case RIGHT_SEMI_JOIN:
      get_next_row_func_ = &ObMergeJoin::right_semi_get_next_row;
      break;
    case LEFT_ANTI_SEMI_JOIN:
      get_next_row_func_ = &ObMergeJoin::left_anti_semi_get_next_row;
      break;
    case RIGHT_ANTI_SEMI_JOIN:
      get_next_row_func_ = &ObMergeJoin::right_anti_semi_get_next_row;
      break;
    default:
      ret = OB_ERR_UNEXPECTED;
      break;
  }
  return ret;
}

int ObMergeJoin::get_next_row(const ObRow *&row)
{
  OB_ASSERT(get_next_row_func_);
  return (this->*(this->ObMergeJoin::get_next_row_func_))(row);
}

int ObMergeJoin::open()
{
  int ret = OB_SUCCESS;
  const ObRowDesc *left_row_desc = NULL;
  const ObRowDesc *right_row_desc = NULL;
  char *store_buf = NULL;
  if (OB_SUCCESS != (ret = ObJoin::open()))
  {
    TBSYS_LOG(WARN, "failed to open child ops, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = left_op_->get_row_desc(left_row_desc)))
  {
    TBSYS_LOG(WARN, "failed to get child row desc, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = right_op_->get_row_desc(right_row_desc)))
  {
    TBSYS_LOG(WARN, "failed to get child row desc, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = cons_row_desc(*left_row_desc, *right_row_desc)))
  {
    TBSYS_LOG(WARN, "failed to cons row desc, err=%d", ret);
  }
  // allocate memory for last_join_left_row_store_
  else if (NULL == (store_buf = static_cast<char*>(ob_malloc(MAX_SINGLE_ROW_SIZE))))
  {
    TBSYS_LOG(ERROR, "no memory");
    ret = OB_ALLOCATE_MEMORY_FAILED;
  }
  else
  {
    OB_ASSERT(left_row_desc);
    OB_ASSERT(right_row_desc);
    curr_row_.set_row_desc(row_desc_);
    curr_cached_right_row_.set_row_desc(*right_row_desc);

    last_left_row_ = NULL;
    last_right_row_ = NULL;
    right_cache_is_valid_ = false;
    is_right_iter_end_ = false;
    last_join_left_row_store_.assign_buffer(store_buf, MAX_SINGLE_ROW_SIZE);
  }
  return ret;
}

int ObMergeJoin::close()
{
  int ret = OB_SUCCESS;
  char *store_buf = last_join_left_row_store_.ptr();
  if (NULL != store_buf)
  {
    ob_free(store_buf);
    last_join_left_row_store_.assign_ptr(NULL, 0);
  }
  last_left_row_ = NULL;
  last_right_row_ = NULL;
  right_cache_is_valid_ = false;
  is_right_iter_end_ = false;
  row_desc_.reset();
  ret = ObJoin::close();
  return ret;
}

int ObMergeJoin::get_row_desc(const common::ObRowDesc *&row_desc) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(0 >= row_desc_.get_column_num()))
  {
    TBSYS_LOG(ERROR, "not init");
    ret = OB_NOT_INIT;
  }
  else
  {
    row_desc = &row_desc_;
  }
  return ret;
}

int ObMergeJoin::compare_equijoin_cond(const ObRow& r1, const ObRow& r2, int &cmp) const
{
  int ret = OB_SUCCESS;
  cmp = 0;
  const ObObj *res1 = NULL;
  const ObObj *res2 = NULL;
  ObExprObj obj1;
  ObExprObj obj2;
  for (int64_t i = 0; i < equal_join_conds_.count(); ++i)
  {
    const ObSqlExpression &expr = equal_join_conds_.at(i);
    ExprItem::SqlCellInfo c1;
    ExprItem::SqlCellInfo c2;
    if (expr.is_equijoin_cond(c1, c2))
    {
      if (OB_SUCCESS != (ret = r1.get_cell(c1.tid, c1.cid, res1)))
      {
        TBSYS_LOG(ERROR, "failed to get cell, err=%d tid=%lu cid=%lu", ret, c1.tid, c1.cid);
        break;
      }
      else if (OB_SUCCESS != (ret = r2.get_cell(c2.tid, c2.cid, res2)))
      {
        TBSYS_LOG(ERROR, "failed to get cell, err=%d tid=%lu cid=%lu", ret, c2.tid, c2.cid);
        break;
      }
      else
      {
        obj1.assign(*res1);
        obj2.assign(*res2);
        if (OB_SUCCESS != obj1.compare(obj2, cmp))
        {
          cmp = -10;            // @todo NULL
        }
        else if (0 != cmp)
        {
          break;
        }
      }
    }
    else
    {
      TBSYS_LOG(ERROR, "invalid equijoin condition");
      ret = OB_ERR_UNEXPECTED;
      break;
    }
  }
  return ret;
}

int ObMergeJoin::left_row_compare_equijoin_cond(const ObRow& r1, const ObRow& r2, int &cmp) const
{
  int ret = OB_SUCCESS;
  cmp = 0;
  const ObObj *res1 = NULL;
  const ObObj *res2 = NULL;
  ObExprObj obj1;
  ObExprObj obj2;
  for (int64_t i = 0; i < equal_join_conds_.count(); ++i)
  {
    const ObSqlExpression &expr = equal_join_conds_.at(i);
    ExprItem::SqlCellInfo c1;
    ExprItem::SqlCellInfo c2;
    if (expr.is_equijoin_cond(c1, c2))
    {
      if (OB_SUCCESS != (ret = r1.get_cell(c1.tid, c1.cid, res1)))
      {
        TBSYS_LOG(ERROR, "failed to get cell, err=%d tid=%lu cid=%lu", ret, c1.tid, c1.cid);
        break;
      }
      else if (OB_SUCCESS != (ret = r2.get_cell(c1.tid, c1.cid, res2)))
      {
        TBSYS_LOG(ERROR, "failed to get cell, err=%d tid=%lu cid=%lu", ret, c2.tid, c2.cid);
        break;
      }
      else
      {
        obj1.assign(*res1);
        obj2.assign(*res2);
        if (OB_SUCCESS != obj1.compare(obj2, cmp))
        {
          cmp = -10;            // @todo NULL
        }
        else if (0 != cmp)
        {
          break;
        }
      }
    }
    else
    {
      TBSYS_LOG(ERROR, "invalid equijoin condition");
      ret = OB_ERR_UNEXPECTED;
      break;
    }
  }
  return ret;
}

int ObMergeJoin::curr_row_is_qualified(bool &is_qualified)
{
  int ret = OB_SUCCESS;
  is_qualified = true;
  const ObObj *res = NULL;
  for (int64_t i = 0; i < other_join_conds_.count(); ++i)
  {
    ObSqlExpression &expr = other_join_conds_.at(i);
    if (OB_SUCCESS != (ret = expr.calc(curr_row_, res)))
    {
      TBSYS_LOG(WARN, "failed to calc expr, err=%d", ret);
    }
    else if (!res->is_true())
    {
      is_qualified = false;
      break;
    }
  }
  return ret;
}

int ObMergeJoin::cons_row_desc(const ObRowDesc &rd1, const ObRowDesc &rd2)
{
  int ret = OB_SUCCESS;
  uint64_t tid = OB_INVALID_ID;
  uint64_t cid = OB_INVALID_ID;
  for (int64_t i = 0; i < rd1.get_column_num(); ++i)
  {
    if (OB_SUCCESS != (ret = rd1.get_tid_cid(i, tid, cid)))
    {
      TBSYS_LOG(ERROR, "unexpected branch");
      ret = OB_ERR_UNEXPECTED;
      break;
    }
    else if (OB_SUCCESS != (ret = row_desc_.add_column_desc(tid, cid)))
    {
      TBSYS_LOG(WARN, "failed to add column desc, err=%d", ret);
      break;
    }
  }
  for (int64_t i = 0; OB_SUCCESS == ret && i < rd2.get_column_num(); ++i)
  {
    if (OB_SUCCESS != (ret = rd2.get_tid_cid(i, tid, cid)))
    {
      TBSYS_LOG(ERROR, "unexpected branch");
    }
    else if (OB_SUCCESS != (ret = row_desc_.add_column_desc(tid, cid)))
    {
      TBSYS_LOG(WARN, "failed to add column desc, err=%d", ret);
    }
  }
  return ret;
}

int ObMergeJoin::join_rows(const ObRow& r1, const ObRow& r2)
{
  int ret = OB_SUCCESS;
  const ObObj *cell = NULL;
  uint64_t tid = OB_INVALID_ID;
  uint64_t cid = OB_INVALID_ID;
  int64_t i = 0;
  for (; i < r1.get_column_num(); ++i)
  {
    if (OB_SUCCESS != (ret = r1.raw_get_cell(i, cell, tid, cid)))
    {
      TBSYS_LOG(ERROR, "unexpected branch, err=%d", ret);
      ret = OB_ERR_UNEXPECTED;
      break;
    }
    else if (OB_SUCCESS != (ret = curr_row_.raw_set_cell(i, *cell)))
    {
      TBSYS_LOG(WARN, "failed to set cell, err=%d i=%ld", ret, i);
      break;
    }
  } // end for
  for (int64_t j = 0; OB_SUCCESS == ret && j < r2.get_column_num(); ++j)
  {
    if (OB_SUCCESS != (ret = r2.raw_get_cell(j, cell, tid, cid)))
    {
      TBSYS_LOG(ERROR, "unexpected branch, err=%d", ret);
      ret = OB_ERR_UNEXPECTED;
    }
    else if (OB_SUCCESS != (ret = curr_row_.raw_set_cell(i+j, *cell)))
    {
      TBSYS_LOG(WARN, "failed to set cell, err=%d j=%ld", ret, j);
    }
  } // end for
  return ret;
}

// INNER_JOIN
int ObMergeJoin::inner_get_next_row(const common::ObRow *&row)
{
  int ret = OB_SUCCESS;
  const ObRow *left_row = NULL;
  const ObRow *right_row = NULL;
  // fetch the next left row
  if (NULL != last_left_row_)
  {
    left_row = last_left_row_;
    last_left_row_ = NULL;
  }
  else
  {
    ret = left_op_->get_next_row(left_row);
  }

  while(OB_SUCCESS == ret)
  {
    if (right_cache_is_valid_)
    {
      OB_ASSERT(!right_cache_.is_empty());
      int cmp = 0;
      if (OB_SUCCESS != (ret = left_row_compare_equijoin_cond(*left_row, last_join_left_row_, cmp)))
      {
        TBSYS_LOG(WARN, "failed to compare, err=%d", ret);
        break;
      }
      if (0 == cmp)
      {
        // fetch the next right row from right_cache
        if (OB_SUCCESS != (ret = right_cache_.get_next_row(curr_cached_right_row_)))
        {
          if (OB_UNLIKELY(OB_ITER_END != ret))
          {
            TBSYS_LOG(WARN, "failed to get next row from right_cache, err=%d", ret);
          }
          else
          {
            right_cache_.reset_iterator(); // continue
            // fetch the next left row
            ret = left_op_->get_next_row(left_row);
          }
        }
        else
        {
          bool is_qualified = false;
          if (OB_SUCCESS != (ret = join_rows(*left_row, curr_cached_right_row_)))
          {
            TBSYS_LOG(WARN, "failed to join rows, err=%d", ret);
          }
          else if (OB_SUCCESS != (ret = curr_row_is_qualified(is_qualified)))
          {
            TBSYS_LOG(WARN, "failed to test qualification, err=%d", ret);
          }
          else if (is_qualified)
          {
            // output
            row = &curr_row_;
            last_left_row_ = left_row;
            break;
          }
          else
          {
            // continue with the next cached right row
            OB_ASSERT(NULL == last_left_row_);
            OB_ASSERT(NULL != left_row);
          }
        }
      }
      else
      {
        // left_row > last_join_left_row_ on euqijoin conditions
        right_cache_is_valid_ = false;
        right_cache_.clear();
      }
    }
    else
    {
      // fetch the next right row
      if (OB_UNLIKELY(is_right_iter_end_))
      {
        ret = OB_ITER_END;
        break;
      }
      else if (NULL != last_right_row_)
      {
        right_row = last_right_row_;
        last_right_row_ = NULL;
      }
      else
      {
        ret = right_op_->get_next_row(right_row);
        if (OB_SUCCESS != ret)
        {
          if (OB_ITER_END == ret)
          {
            TBSYS_LOG(INFO, "end of right child op");
            is_right_iter_end_ = true;
            if (!right_cache_.is_empty())
            {
              // no more right rows and the right cache is not empty, we SHOULD look at the next left row
              right_cache_is_valid_ = true;
              OB_ASSERT(NULL == last_right_row_);
              OB_ASSERT(NULL == last_left_row_);
              ret = left_op_->get_next_row(left_row);
            }
            continue;
          }
          else
          {
            TBSYS_LOG(WARN, "failed to get next row from right child, err=%d", ret);
            break;
          }
        }
      }
      OB_ASSERT(left_row);
      OB_ASSERT(right_row);
      int cmp = 0;
      if (OB_SUCCESS != (ret = compare_equijoin_cond(*left_row, *right_row, cmp)))
      {
        TBSYS_LOG(WARN, "failed to compare, err=%d", ret);
        break;
      }
      if (0 == cmp)
      {
        if (right_cache_.is_empty())
        {
          // store the joined left row
          last_join_left_row_store_.assign_buffer(last_join_left_row_store_.ptr(), MAX_SINGLE_ROW_SIZE);
          if (OB_SUCCESS != (ret = ObRowUtil::convert(*left_row, last_join_left_row_store_, last_join_left_row_)))
          {
            TBSYS_LOG(WARN, "failed to store left row, err=%d", ret);
            break;
          }
        }
        bool is_qualified = false;
        const ObRowStore::StoredRow *stored_row = NULL;
        if (OB_SUCCESS != (ret = right_cache_.add_row(*right_row, stored_row)))
        {
          TBSYS_LOG(WARN, "failed to store the row, err=%d", ret);
        }
        else if (OB_SUCCESS != (ret = join_rows(*left_row, *right_row)))
        {
          TBSYS_LOG(WARN, "failed to join rows, err=%d", ret);
        }
        else if (OB_SUCCESS != (ret = curr_row_is_qualified(is_qualified)))
        {
          TBSYS_LOG(WARN, "failed to test qualification, err=%d", ret);
        }
        else if (is_qualified)
        {
          // output
          row = &curr_row_;
          last_left_row_ = left_row;
          OB_ASSERT(NULL == last_right_row_);
          break;
        }
        else
        {
          // continue with the next right row
          OB_ASSERT(NULL != left_row);
          OB_ASSERT(NULL == last_left_row_);
          OB_ASSERT(NULL == last_right_row_);
        }
      } // end 0 == cmp
      else if (cmp < 0)
      {
        // left_row < right_row on equijoin conditions
        if (!right_cache_.is_empty())
        {
          right_cache_is_valid_ = true;
        }
        last_right_row_ = right_row;
        OB_ASSERT(NULL == last_left_row_);
        ret = left_op_->get_next_row(left_row);
      }
      else
      {
        // left_row > right_row on euqijoin conditions
        // continue with the next right row
        OB_ASSERT(NULL != left_row);
        OB_ASSERT(NULL == last_left_row_);
        OB_ASSERT(NULL == last_right_row_);
      }
    }
  } // end while
  return ret;
}

int ObMergeJoin::left_join_rows(const ObRow& r1)
{
  int ret = OB_SUCCESS;
  const ObObj *cell = NULL;
  uint64_t tid = OB_INVALID_ID;
  uint64_t cid = OB_INVALID_ID;
  int64_t i = 0;
  for (; i < r1.get_column_num(); ++i)
  {
    if (OB_SUCCESS != (ret = r1.raw_get_cell(i, cell, tid, cid)))
    {
      TBSYS_LOG(ERROR, "unexpected branch, err=%d", ret);
      ret = OB_ERR_UNEXPECTED;
      break;
    }
    else if (OB_SUCCESS != (ret = curr_row_.raw_set_cell(i, *cell)))
    {
      TBSYS_LOG(WARN, "failed to set cell, err=%d i=%ld", ret, i);
      break;
    }
  } // end for
  int64_t right_row_column_num = row_desc_.get_column_num() - r1.get_column_num();
  ObObj null_cell;
  null_cell.set_null();
  for (int64_t j = 0; OB_SUCCESS == ret && j < right_row_column_num; ++j)
  {
    if (OB_SUCCESS != (ret = curr_row_.raw_set_cell(i+j, null_cell)))
    {
      TBSYS_LOG(WARN, "failed to set cell, err=%d j=%ld", ret, j);
    }
  } // end for
  return ret;
}

int ObMergeJoin::left_outer_get_next_row(const common::ObRow *&row)
{
  int ret = OB_SUCCESS;
  const ObRow *left_row = NULL;
  const ObRow *right_row = NULL;
  // fetch the next left row
  if (NULL != last_left_row_)
  {
    left_row = last_left_row_;
    last_left_row_ = NULL;
  }
  else
  {
    ret = left_op_->get_next_row(left_row);
  }

  while(OB_SUCCESS == ret)
  {
    if (right_cache_is_valid_)
    {
      OB_ASSERT(!right_cache_.is_empty());
      int cmp = 0;
      if (OB_SUCCESS != (ret = left_row_compare_equijoin_cond(*left_row, last_join_left_row_, cmp)))
      {
        TBSYS_LOG(WARN, "failed to compare, err=%d", ret);
        break;
      }
      if (0 == cmp)
      {
        // fetch the next right row from right_cache
        if (OB_SUCCESS != (ret = right_cache_.get_next_row(curr_cached_right_row_)))
        {
          if (OB_UNLIKELY(OB_ITER_END != ret))
          {
            TBSYS_LOG(WARN, "failed to get next row from right_cache, err=%d", ret);
          }
          else
          {
            right_cache_.reset_iterator(); // continue
            // fetch the next left row
            ret = left_op_->get_next_row(left_row);
          }
        }
        else
        {
          bool is_qualified = false;
          if (OB_SUCCESS != (ret = join_rows(*left_row, curr_cached_right_row_)))
          {
            TBSYS_LOG(WARN, "failed to join rows, err=%d", ret);
          }
          else if (OB_SUCCESS != (ret = curr_row_is_qualified(is_qualified)))
          {
            TBSYS_LOG(WARN, "failed to test qualification, err=%d", ret);
          }
          else if (is_qualified)
          {
            // output
            row = &curr_row_;
            last_left_row_ = left_row;
            break;
          }
          else
          {
            // continue with the next cached right row
            OB_ASSERT(NULL == last_left_row_);
            OB_ASSERT(NULL != left_row);
          }
        }
      }
      else
      {
        // left_row > last_join_left_row_ on euqijoin conditions
        right_cache_is_valid_ = false;
        right_cache_.clear();
      }
    }
    else
    {
      // fetch the next right row
      if (OB_UNLIKELY(is_right_iter_end_))
      {
        // no more right rows, but there are left rows left
        OB_ASSERT(left_row);
        bool is_qualified = false;
        if (OB_SUCCESS != (ret = left_join_rows(*left_row)))
        {
          TBSYS_LOG(WARN, "failed to join rows, err=%d", ret);
        }
        else if (OB_SUCCESS != (ret = curr_row_is_qualified(is_qualified)))
        {
          TBSYS_LOG(WARN, "failed to test qualification, err=%d", ret);
        }
        else if (is_qualified)
        {
          // output
          row = &curr_row_;
          OB_ASSERT(NULL == last_left_row_);
          OB_ASSERT(NULL == last_right_row_);
          break;
        }
        else
        {
          // continue with the next left row
          OB_ASSERT(NULL == last_left_row_);
          OB_ASSERT(NULL == last_right_row_);
          ret = left_op_->get_next_row(left_row);
          continue;
        }
      }
      else if (NULL != last_right_row_)
      {
        right_row = last_right_row_;
        last_right_row_ = NULL;
      }
      else
      {
        ret = right_op_->get_next_row(right_row);
        if (OB_SUCCESS != ret)
        {
          if (OB_ITER_END == ret)
          {
            TBSYS_LOG(INFO, "end of right child op");
            is_right_iter_end_ = true;
            if (!right_cache_.is_empty())
            {
              // no more right rows and the right cache is not empty, we SHOULD look at the next left row
              right_cache_is_valid_ = true;
              OB_ASSERT(NULL == last_right_row_);
              OB_ASSERT(NULL == last_left_row_);
              ret = left_op_->get_next_row(left_row);
            }
            else
            {
              ret = OB_SUCCESS;
            }
            continue;
          }
          else
          {
            TBSYS_LOG(WARN, "failed to get next row from right child, err=%d", ret);
            break;
          }
        }
      }
      OB_ASSERT(left_row);
      OB_ASSERT(right_row);
      int cmp = 0;
      if (OB_SUCCESS != (ret = compare_equijoin_cond(*left_row, *right_row, cmp)))
      {
        TBSYS_LOG(WARN, "failed to compare, err=%d", ret);
        break;
      }
      if (0 == cmp)
      {
        if (right_cache_.is_empty())
        {
          // store the joined left row
          last_join_left_row_store_.assign_buffer(last_join_left_row_store_.ptr(), MAX_SINGLE_ROW_SIZE);
          if (OB_SUCCESS != (ret = ObRowUtil::convert(*left_row, last_join_left_row_store_, last_join_left_row_)))
          {
            TBSYS_LOG(WARN, "failed to store left row, err=%d", ret);
            break;
          }
        }
        bool is_qualified = false;
        const ObRowStore::StoredRow *stored_row = NULL;
        if (OB_SUCCESS != (ret = right_cache_.add_row(*right_row, stored_row)))
        {
          TBSYS_LOG(WARN, "failed to store the row, err=%d", ret);
        }
        else if (OB_SUCCESS != (ret = join_rows(*left_row, *right_row)))
        {
          TBSYS_LOG(WARN, "failed to join rows, err=%d", ret);
        }
        else if (OB_SUCCESS != (ret = curr_row_is_qualified(is_qualified)))
        {
          TBSYS_LOG(WARN, "failed to test qualification, err=%d", ret);
        }
        else if (is_qualified)
        {
          // output
          row = &curr_row_;
          last_left_row_ = left_row;
          OB_ASSERT(NULL == last_right_row_);
          break;
        }
        else
        {
          // continue with the next right row
          OB_ASSERT(NULL != left_row);
          OB_ASSERT(NULL == last_left_row_);
          OB_ASSERT(NULL == last_right_row_);
        }
      } // end 0 == cmp
      else if (cmp < 0)
      {
        // left_row < right_row on equijoin conditions
        if (!right_cache_.is_empty())
        {
          right_cache_is_valid_ = true;
          OB_ASSERT(NULL == last_left_row_);
          last_right_row_ = right_row;
          ret = left_op_->get_next_row(left_row);
        }
        else
        {
          bool is_qualified = false;
          if (OB_SUCCESS != (ret = left_join_rows(*left_row)))
          {
            TBSYS_LOG(WARN, "failed to join rows, err=%d", ret);
          }
          else if (OB_SUCCESS != (ret = curr_row_is_qualified(is_qualified)))
          {
            TBSYS_LOG(WARN, "failed to test qualification, err=%d", ret);
          }
          else if (is_qualified)
          {
            // output
            row = &curr_row_;
            OB_ASSERT(NULL == last_left_row_);
            last_right_row_ = right_row;
            break;
          }
          else
          {
            // continue with the next left row
            OB_ASSERT(NULL == last_left_row_);
            ret = left_op_->get_next_row(left_row);
            last_right_row_ = right_row;
          }
        }
      }
      else
      {
        // left_row > right_row on euqijoin conditions
        // continue with the next right row
        OB_ASSERT(NULL != left_row);
        OB_ASSERT(NULL == last_left_row_);
        OB_ASSERT(NULL == last_right_row_);
      }
    }
  } // end while
  return ret;
}

int ObMergeJoin::right_join_rows(const ObRow& r2)
{
  int ret = OB_SUCCESS;
  const ObObj *cell = NULL;
  uint64_t tid = OB_INVALID_ID;
  uint64_t cid = OB_INVALID_ID;
  int64_t left_row_column_num = row_desc_.get_column_num() - r2.get_column_num();
  ObObj null_cell;
  null_cell.set_null();
  for (int64_t i = 0; i < left_row_column_num; ++i)
  {
    if (OB_SUCCESS != (ret = curr_row_.raw_set_cell(i, null_cell)))
    {
      TBSYS_LOG(WARN, "failed to set cell, err=%d i=%ld", ret, i);
      break;
    }
  } // end for
  for (int64_t j = 0; OB_SUCCESS == ret && j < r2.get_column_num(); ++j)
  {
    if (OB_SUCCESS != (ret = r2.raw_get_cell(j, cell, tid, cid)))
    {
      TBSYS_LOG(ERROR, "unexpected branch, err=%d", ret);
      ret = OB_ERR_UNEXPECTED;
    }
    else if (OB_SUCCESS != (ret = curr_row_.raw_set_cell(left_row_column_num+j, *cell)))
    {
      TBSYS_LOG(WARN, "failed to set cell, err=%d j=%ld", ret, j);
    }
  } // end for
  return ret;
}

int ObMergeJoin::right_outer_get_next_row(const common::ObRow *&row)
{
  int ret = OB_SUCCESS;
  const ObRow *left_row = NULL;
  const ObRow *right_row = NULL;
  // fetch the next left row
  if (NULL != last_left_row_)
  {
    left_row = last_left_row_;
    last_left_row_ = NULL;
  }
  else
  {
    ret = left_op_->get_next_row(left_row);
  }

  while(OB_SUCCESS == ret)
  {
    if (right_cache_is_valid_)
    {
      OB_ASSERT(!right_cache_.is_empty());
      int cmp = 0;
      if (OB_SUCCESS != (ret = left_row_compare_equijoin_cond(*left_row, last_join_left_row_, cmp)))
      {
        TBSYS_LOG(WARN, "failed to compare, err=%d", ret);
        break;
      }
      if (0 == cmp)
      {
        // fetch the next right row from right_cache
        if (OB_SUCCESS != (ret = right_cache_.get_next_row(curr_cached_right_row_)))
        {
          if (OB_UNLIKELY(OB_ITER_END != ret))
          {
            TBSYS_LOG(WARN, "failed to get next row from right_cache, err=%d", ret);
          }
          else
          {
            right_cache_.reset_iterator(); // continue
            // fetch the next left row
            ret = left_op_->get_next_row(left_row);
          }
        }
        else
        {
          bool is_qualified = false;
          if (OB_SUCCESS != (ret = join_rows(*left_row, curr_cached_right_row_)))
          {
            TBSYS_LOG(WARN, "failed to join rows, err=%d", ret);
          }
          else if (OB_SUCCESS != (ret = curr_row_is_qualified(is_qualified)))
          {
            TBSYS_LOG(WARN, "failed to test qualification, err=%d", ret);
          }
          else if (is_qualified)
          {
            // output
            row = &curr_row_;
            last_left_row_ = left_row;
            break;
          }
          else
          {
            // continue with the next cached right row
            OB_ASSERT(NULL == last_left_row_);
            OB_ASSERT(NULL != left_row);
          }
        }
      }
      else
      {
        // left_row > last_join_left_row_ on euqijoin conditions
        right_cache_is_valid_ = false;
        right_cache_.clear();
      }
    }
    else
    {
      // fetch the next right row
      if (OB_UNLIKELY(is_right_iter_end_))
      {
        ret = OB_ITER_END;
        break;
      }
      else if (NULL != last_right_row_)
      {
        right_row = last_right_row_;
        last_right_row_ = NULL;
      }
      else
      {
        ret = right_op_->get_next_row(right_row);
        if (OB_SUCCESS != ret)
        {
          if (OB_ITER_END == ret)
          {
            TBSYS_LOG(INFO, "end of right child op");
            is_right_iter_end_ = true;
            if (!right_cache_.is_empty())
            {
              // no more right rows and the right cache is not empty, we SHOULD look at the next left row
              right_cache_is_valid_ = true;
              OB_ASSERT(NULL == last_right_row_);
              OB_ASSERT(NULL == last_left_row_);
              ret = left_op_->get_next_row(left_row);
            }
            continue;
          }
          else
          {
            TBSYS_LOG(WARN, "failed to get next row from right child, err=%d", ret);
            break;
          }
        }
      }
      OB_ASSERT(left_row);
      OB_ASSERT(right_row);
      int cmp = 0;
      if (OB_SUCCESS != (ret = compare_equijoin_cond(*left_row, *right_row, cmp)))
      {
        TBSYS_LOG(WARN, "failed to compare, err=%d", ret);
        break;
      }
      if (0 == cmp)
      {
        if (right_cache_.is_empty())
        {
          // store the joined left row
          last_join_left_row_store_.assign_buffer(last_join_left_row_store_.ptr(), MAX_SINGLE_ROW_SIZE);
          if (OB_SUCCESS != (ret = ObRowUtil::convert(*left_row, last_join_left_row_store_, last_join_left_row_)))
          {
            TBSYS_LOG(WARN, "failed to store left row, err=%d", ret);
            break;
          }
        }
        bool is_qualified = false;
        const ObRowStore::StoredRow *stored_row = NULL;
        if (OB_SUCCESS != (ret = right_cache_.add_row(*right_row, stored_row)))
        {
          TBSYS_LOG(WARN, "failed to store the row, err=%d", ret);
        }
        else if (OB_SUCCESS != (ret = join_rows(*left_row, *right_row)))
        {
          TBSYS_LOG(WARN, "failed to join rows, err=%d", ret);
        }
        else if (OB_SUCCESS != (ret = curr_row_is_qualified(is_qualified)))
        {
          TBSYS_LOG(WARN, "failed to test qualification, err=%d", ret);
        }
        else if (is_qualified)
        {
          // output
          row = &curr_row_;
          last_left_row_ = left_row;
          OB_ASSERT(NULL == last_right_row_);
          break;
        }
        else
        {
          // continue with the next right row
          OB_ASSERT(NULL != left_row);
          OB_ASSERT(NULL == last_left_row_);
          OB_ASSERT(NULL == last_right_row_);
        }
      } // end 0 == cmp
      else if (cmp < 0)
      {
        // left_row < right_row on equijoin conditions
        if (!right_cache_.is_empty())
        {
          right_cache_is_valid_ = true;
        }
        last_right_row_ = right_row;
        OB_ASSERT(NULL == last_left_row_);
        ret = left_op_->get_next_row(left_row);
      }
      else
      {
        // left_row > right_row on euqijoin conditions
        OB_ASSERT(NULL != left_row);
        OB_ASSERT(NULL != right_row);
        OB_ASSERT(NULL == last_left_row_);
        OB_ASSERT(NULL == last_right_row_);
        bool is_qualified = false;
        if (OB_SUCCESS != (ret = right_join_rows(*right_row)))
        {
          TBSYS_LOG(WARN, "failed to join rows, err=%d", ret);
        }
        else if (OB_SUCCESS != (ret = curr_row_is_qualified(is_qualified)))
        {
          TBSYS_LOG(WARN, "failed to test qualification, err=%d", ret);
        }
        else if (is_qualified)
        {
          // output
          row = &curr_row_;
          last_left_row_ = left_row;
          break;
        }
        else
        {
          // continue with the next right row
        }
      }
    }
  } // end while
  if (OB_ITER_END == ret && !is_right_iter_end_)
  {
    OB_ASSERT(NULL == last_left_row_);
    // left row finished but we still have right rows left
    do
    {
      if (NULL != last_right_row_)
      {
        right_row = last_right_row_;
        last_right_row_ = NULL;
      }
      else
      {
        ret = right_op_->get_next_row(right_row);
        if (OB_SUCCESS != ret)
        {
          if (OB_ITER_END == ret)
          {
            TBSYS_LOG(INFO, "end of right child op");
            break;
          }
          else
          {
            TBSYS_LOG(WARN, "failed to get next row from right child, err=%d", ret);
            break;
          }
        }
      }
      OB_ASSERT(right_row);
      OB_ASSERT(NULL == last_right_row_);
      bool is_qualified = false;
      if (OB_SUCCESS != (ret = right_join_rows(*right_row)))
      {
        TBSYS_LOG(WARN, "failed to join rows, err=%d", ret);
      }
      else if (OB_SUCCESS != (ret = curr_row_is_qualified(is_qualified)))
      {
        TBSYS_LOG(WARN, "failed to test qualification, err=%d", ret);
      }
      else if (is_qualified)
      {
        // output
        row = &curr_row_;
        break;
      }
      else
      {
        // continue with the next right row
      }
    }
    while (OB_SUCCESS == ret);
  }
  return ret;
}

int ObMergeJoin::full_outer_get_next_row(const common::ObRow *&row)
{
  int ret = OB_SUCCESS;
  const ObRow *left_row = NULL;
  const ObRow *right_row = NULL;
  // fetch the next left row
  if (NULL != last_left_row_)
  {
    left_row = last_left_row_;
    last_left_row_ = NULL;
  }
  else
  {
    ret = left_op_->get_next_row(left_row);
  }

  while(OB_SUCCESS == ret)
  {
    if (right_cache_is_valid_)
    {
      OB_ASSERT(!right_cache_.is_empty());
      int cmp = 0;
      if (OB_SUCCESS != (ret = left_row_compare_equijoin_cond(*left_row, last_join_left_row_, cmp)))
      {
        TBSYS_LOG(WARN, "failed to compare, err=%d", ret);
        break;
      }
      if (0 == cmp)
      {
        // fetch the next right row from right_cache
        if (OB_SUCCESS != (ret = right_cache_.get_next_row(curr_cached_right_row_)))
        {
          if (OB_UNLIKELY(OB_ITER_END != ret))
          {
            TBSYS_LOG(WARN, "failed to get next row from right_cache, err=%d", ret);
          }
          else
          {
            right_cache_.reset_iterator(); // continue
            // fetch the next left row
            ret = left_op_->get_next_row(left_row);
          }
        }
        else
        {
          bool is_qualified = false;
          if (OB_SUCCESS != (ret = join_rows(*left_row, curr_cached_right_row_)))
          {
            TBSYS_LOG(WARN, "failed to join rows, err=%d", ret);
          }
          else if (OB_SUCCESS != (ret = curr_row_is_qualified(is_qualified)))
          {
            TBSYS_LOG(WARN, "failed to test qualification, err=%d", ret);
          }
          else if (is_qualified)
          {
            // output
            row = &curr_row_;
            last_left_row_ = left_row;
            break;
          }
          else
          {
            // continue with the next cached right row
            OB_ASSERT(NULL == last_left_row_);
            OB_ASSERT(NULL != left_row);
          }
        }
      }
      else
      {
        // left_row > last_join_left_row_ on euqijoin conditions
        right_cache_is_valid_ = false;
        right_cache_.clear();
      }
    }
    else
    {
      // fetch the next right row
      if (OB_UNLIKELY(is_right_iter_end_))
      {
        // no more right rows, but there are left rows left
        OB_ASSERT(left_row);
        bool is_qualified = false;
        if (OB_SUCCESS != (ret = left_join_rows(*left_row)))
        {
          TBSYS_LOG(WARN, "failed to join rows, err=%d", ret);
        }
        else if (OB_SUCCESS != (ret = curr_row_is_qualified(is_qualified)))
        {
          TBSYS_LOG(WARN, "failed to test qualification, err=%d", ret);
        }
        else if (is_qualified)
        {
          // output
          row = &curr_row_;
          OB_ASSERT(NULL == last_left_row_);
          OB_ASSERT(NULL == last_right_row_);
          break;
        }
        else
        {
          // continue with the next left row
          OB_ASSERT(NULL == last_left_row_);
          OB_ASSERT(NULL == last_right_row_);
          ret = left_op_->get_next_row(left_row);
          continue;
        }
      }
      else if (NULL != last_right_row_)
      {
        right_row = last_right_row_;
        last_right_row_ = NULL;
      }
      else
      {
        ret = right_op_->get_next_row(right_row);
        if (OB_SUCCESS != ret)
        {
          if (OB_ITER_END == ret)
          {
            TBSYS_LOG(INFO, "end of right child op");
            is_right_iter_end_ = true;
            if (!right_cache_.is_empty())
            {
              // no more right rows and the right cache is not empty, we SHOULD look at the next left row
              right_cache_is_valid_ = true;
              OB_ASSERT(NULL == last_right_row_);
              OB_ASSERT(NULL == last_left_row_);
              ret = left_op_->get_next_row(left_row);
            }
            else
            {
              ret = OB_SUCCESS;
            }
            continue;
          }
          else
          {
            TBSYS_LOG(WARN, "failed to get next row from right child, err=%d", ret);
            break;
          }
        }
      }
      OB_ASSERT(left_row);
      OB_ASSERT(right_row);
      int cmp = 0;
      if (OB_SUCCESS != (ret = compare_equijoin_cond(*left_row, *right_row, cmp)))
      {
        TBSYS_LOG(WARN, "failed to compare, err=%d", ret);
        break;
      }
      if (0 == cmp)
      {
        if (right_cache_.is_empty())
        {
          // store the joined left row
          last_join_left_row_store_.assign_buffer(last_join_left_row_store_.ptr(), MAX_SINGLE_ROW_SIZE);
          if (OB_SUCCESS != (ret = ObRowUtil::convert(*left_row, last_join_left_row_store_, last_join_left_row_)))
          {
            TBSYS_LOG(WARN, "failed to store left row, err=%d", ret);
            break;
          }
        }
        bool is_qualified = false;
        const ObRowStore::StoredRow *stored_row = NULL;
        if (OB_SUCCESS != (ret = right_cache_.add_row(*right_row, stored_row)))
        {
          TBSYS_LOG(WARN, "failed to store the row, err=%d", ret);
        }
        else if (OB_SUCCESS != (ret = join_rows(*left_row, *right_row)))
        {
          TBSYS_LOG(WARN, "failed to join rows, err=%d", ret);
        }
        else if (OB_SUCCESS != (ret = curr_row_is_qualified(is_qualified)))
        {
          TBSYS_LOG(WARN, "failed to test qualification, err=%d", ret);
        }
        else if (is_qualified)
        {
          // output
          row = &curr_row_;
          last_left_row_ = left_row;
          OB_ASSERT(NULL == last_right_row_);
          break;
        }
        else
        {
          // continue with the next right row
          OB_ASSERT(NULL != left_row);
          OB_ASSERT(NULL == last_left_row_);
          OB_ASSERT(NULL == last_right_row_);
        }
      } // end 0 == cmp
      else if (cmp < 0)
      {
        // left_row < right_row on equijoin conditions
        if (!right_cache_.is_empty())
        {
          right_cache_is_valid_ = true;
          OB_ASSERT(NULL == last_left_row_);
          last_right_row_ = right_row;
          ret = left_op_->get_next_row(left_row);
        }
        else
        {
          bool is_qualified = false;
          if (OB_SUCCESS != (ret = left_join_rows(*left_row)))
          {
            TBSYS_LOG(WARN, "failed to join rows, err=%d", ret);
          }
          else if (OB_SUCCESS != (ret = curr_row_is_qualified(is_qualified)))
          {
            TBSYS_LOG(WARN, "failed to test qualification, err=%d", ret);
          }
          else if (is_qualified)
          {
            // output
            row = &curr_row_;
            OB_ASSERT(NULL == last_left_row_);
            last_right_row_ = right_row;
            break;
          }
          else
          {
            // continue with the next left row
            OB_ASSERT(NULL == last_left_row_);
            ret = left_op_->get_next_row(left_row);
            last_right_row_ = right_row;
          }
        }
      }
      else
      {
        // left_row > right_row on euqijoin conditions
        OB_ASSERT(NULL != left_row);
        OB_ASSERT(NULL != right_row);
        OB_ASSERT(NULL == last_left_row_);
        OB_ASSERT(NULL == last_right_row_);
        bool is_qualified = false;
        if (OB_SUCCESS != (ret = right_join_rows(*right_row)))
        {
          TBSYS_LOG(WARN, "failed to join rows, err=%d", ret);
        }
        else if (OB_SUCCESS != (ret = curr_row_is_qualified(is_qualified)))
        {
          TBSYS_LOG(WARN, "failed to test qualification, err=%d", ret);
        }
        else if (is_qualified)
        {
          // output
          row = &curr_row_;
          last_left_row_ = left_row;
          break;
        }
        else
        {
          // continue with the next right row
        }
      }
    }
  } // end while
  if (OB_ITER_END == ret && !is_right_iter_end_)
  {
    OB_ASSERT(NULL == last_left_row_);
    // left row finished but we still have right rows left
    do
    {
      if (NULL != last_right_row_)
      {
        right_row = last_right_row_;
        last_right_row_ = NULL;
      }
      else
      {
        ret = right_op_->get_next_row(right_row);
        if (OB_SUCCESS != ret)
        {
          if (OB_ITER_END == ret)
          {
            TBSYS_LOG(INFO, "end of right child op");
            break;
          }
          else
          {
            TBSYS_LOG(WARN, "failed to get next row from right child, err=%d", ret);
            break;
          }
        }
      }
      OB_ASSERT(right_row);
      OB_ASSERT(NULL == last_right_row_);
      bool is_qualified = false;
      if (OB_SUCCESS != (ret = right_join_rows(*right_row)))
      {
        TBSYS_LOG(WARN, "failed to join rows, err=%d", ret);
      }
      else if (OB_SUCCESS != (ret = curr_row_is_qualified(is_qualified)))
      {
        TBSYS_LOG(WARN, "failed to test qualification, err=%d", ret);
      }
      else if (is_qualified)
      {
        // output
        row = &curr_row_;
        break;
      }
      else
      {
        // continue with the next right row
      }
    }
    while (OB_SUCCESS == ret);
  }
  return ret;
}

// INNER_JOIN, LEFT_OUTER_JOIN, RIGHT_OUTER_JOIN, FULL_OUTER_JOIN
int ObMergeJoin::normal_get_next_row(const ObRow *&row)
{
  row = NULL;
/*
  // 省略边界条件
  const ObRow *left_row = NULL;
  const ObRow *right_row = NULL;
  if (last_left_row_有效)
  {
    left_row = last_left_row_;
    last_left_row_ = NULL;
  }
  else
  {
    left_op_->get_next_row(left_row);
  }

  while(两个子运算符都还有数据没有迭代完)
  {
    if (left_row在等值条件上等于last_join_left_row_
        && right_cache_is_valid_)
    {
      从right_cache_中取下一条数据到right_row;
      if (right_cache_全部取完了一遍)
      {
        重置right_cache_中的迭代器到第一行;
        left_op_->get_next_row(left_row);
      }
      else
      {
        join left_row和right_row两行数据，输出到curr_row_;
        row = &curr_row_;
        break;
      }
    }
    else
    {
      if (last_right_row_有效)
      {
        right_row_ = last_right_row_;
        last_right_row_ = NULL;
      }
      else
      {
        right_op_->get_next_row(right_row);
      }

      if (left_row和right_row在等值join条件上相等)
      {
        join两行数据，输出到curr_row_;
        if (left_row在等值条件上不等于last_join_left_row_)
        {
          清空right_cache_和last_join_left_row_;
          right_cache_is_valid_ = false; // 初始时为false
          拷贝left_row到last_join_left_row_;
        }
        right_row保存添加到right_cache_中;
        last_left_row_ = left_row;
        row = &curr_row_;
        break;
      }
      else if(left_row在等值join条件上 < right_row)
      {
        if (LEFT_OUTER_JOIN == join_type_ || FULL_OUTER_JOIN == join_type_)
        {
          用left_row产生curr_row_，不足的值用NULL补齐;
          last_right_row_ = right_row;
          row = &curr_row_;
          break;
        }
        else
        {
          // INNER_JOIN or RIGHT_OUTER_JOIN
          right_cache_is_valid_ = true;
          last_right_row_ = right_row;
          left_op_->get_next_row(left_row);
        }
      }
      else
      {
        // left_row在等值join条件上 > right_row
        if (RIGHT_OUTER_JOIN == join_type_ || FULL_OUTER_JOIN == join_type_)
        {
          用right_row产生curr_row_, 不足的值用NULL补齐;
          last_left_row_ = left_row;
          row = &curr_row_;
          break;
        }
        else
        {
          // INNER_JOIN or LEFT_OUTER_JOIN
          right_op_->get_next_row(right_row);
        }
      }
    }
  } // end while
  if (没有新行产生
      && (RIGHT_OUTER_JOIN == join_type_ || FULL_OUTER_JOIN == join_type_)
      && right_row有效)
  {
    用right_row产生curr_row_，不足的值用NULL补齐;
    row = &curr_row_;
  }
  if (没有新行产生
      && (LEFT_OUTER_JOIN == join_type_ || FULL_OUTER_JOIN == join_type_)
      && right_row有效)
  {
    用left_row产生curr_row_，不足的值用NULL补齐;
    row = &curr_row_;
  }
*/
  return OB_SUCCESS;
}

// LEFT_SEMI_JOIN
int ObMergeJoin::left_semi_get_next_row(const ObRow *&row)
{
  row = NULL;
/*
  // 省略边界条件
  const ObRow *left_row = NULL;
  const ObRow *right_row = NULL;
  left_op_->get_next_row(left_row);
  if (last_right_row_有效)
  {
    right_row = last_right_row_;
    last_right_row_ = NULL;
  }
  else
  {
    right_op_->get_next_row(right_row);
  }
  while(两个子运算符还有数据没有迭代完)
  {
    if (left_row和right_row在等值join条件上相等)
    {
      row = left_row;
      last_right_row_ = right_row;
      break;
    }
    else if(left_row在等值join条件上 < right_row)
    {
      left_op_->get_next_row(left_row);
    }
    else
    {
      // left_row在等值join条件上 > right_row
      right_op_->get_next_row(right_row);
    }
  }
*/
  return OB_SUCCESS;

}

// RIGHT_SEMI_JOIN
int ObMergeJoin::right_semi_get_next_row(const ObRow *&row)
{
  row = NULL;
/*
  // 省略边界条件
  const ObRow *left_row = NULL;
  const ObRow *right_row = NULL;
  right_op_->get_next_row(right_row);
  if (last_left_row_有效)
  {
    left_row = last_left_row_;
    last_left_row_ = NULL;
  }
  else
  {
    left_op_->get_next_row(left_row);
  }
  while(两个子运算符还有数据没有迭代完)
  {
    if (left_row和right_row在等值join条件上相等)
    {
      row = right_row;
      last_left_row_ = left_row;
      break;
    }
    else if(right_row在等值join条件上 < left_row)
    {
      right_op_->get_next_row(right_row);
    }
    else
    {
      // right_row在等值join条件上 > left_row
      left_op_->get_next_row(left_row);
    }
  }
*/
  return OB_SUCCESS;

}

// LEFT_ANTI_SEMI_JOIN
int ObMergeJoin::left_anti_semi_get_next_row(const ObRow *&row)
{
  row = NULL;
/*
  // 省略边界条件
  const ObRow *left_row = NULL;
  const ObRow *right_row = NULL;
  left_op_->get_next_row(left_row);
  if (last_right_row_有效)
  {
    right_row = last_right_row_;
    last_right_row_ = NULL;
  }
  else
  {
    right_op_->get_next_row(right_row);
  }
  while(两个子运算符还有数据没有迭代完，既left_row和right_row都有效)
  {
    if (left_row和right_row在等值join条件上相等)
    {
      left_op_->get_next_row(left_row);
    }
    else if(left_row在等值join条件上 < right_row)
    {
      row = left_row_;
      last_right_row_ = right_row;
      break;
    }
    else
    {
      // left_row在等值join条件上 > right_row
      right_op_->get_next_row(right_row);
    }
  }
  if (没有找到行要输出
      && left_row有效)
  {
    row = left_row;             // left_op_还有数据
  }
*/
  return OB_SUCCESS;

}

// RIGHT_ANTI_SEMI_JOIN
int ObMergeJoin::right_anti_semi_get_next_row(const ObRow *&row)
{
  row = NULL;
/*
  // 省略边界条件
  const ObRow *left_row = NULL;
  const ObRow *right_row = NULL;
  right_op_->get_next_row(right_row);
  if (last_left_row_有效)
  {
    left_row = last_left_row_;
    last_left_row_ = NULL;
  }
  else
  {
    left_op_->get_next_row(left_row);
  }
  while(两个子运算符还有数据没有迭代完，既left_row和right_row都有效)
  {
    if (left_row和right_row在等值join条件上相等)
    {
      right_op_->get_next_row(right_row);
    }
    else if(right_row在等值join条件上 < left_row)
    {
      row = right_row_;
      last_left_row_ = left_row;
      break;
    }
    else
    {
      // right_row在等值join条件上 > left_row
      left_op_->get_next_row(left_row);
    }
  }
  if (没有找到行要输出
      && right_row有效)
  {
    row = right_row;            // right_op_还有数据
  }
*/
  return OB_SUCCESS;

}

int64_t ObMergeJoin::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  databuff_printf(buf, buf_len, pos, "Merge ");
  pos += ObJoin::to_string(buf + pos, buf_len - pos);
  return pos;
}
