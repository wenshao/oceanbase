/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_row_util.cpp
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#include "ob_row_util.h"
#include "common/ob_compact_cell_writer.h"
#include "common/ob_compact_cell_iterator.h"

using namespace oceanbase::common;

int ObRowUtil::convert(const ObRow &row, ObString &compact_row)
{
  int ret = OB_SUCCESS;
  ObCompactCellWriter cell_writer;
  cell_writer.init(compact_row.ptr(), compact_row.size());

  const ObObj *cell = NULL;
  uint64_t table_id = OB_INVALID_ID;
  uint64_t column_id = OB_INVALID_ID;
  ObObj cell_clone;
  for (int64_t i = 0; i < row.get_column_num(); ++i)
  {
    if (OB_SUCCESS != (ret = row.raw_get_cell(i, cell, table_id, column_id)))
    {
      TBSYS_LOG(WARN, "failed to get cell, err=%d", ret);
      break;
    }
    else if (OB_SUCCESS != (ret = cell_writer.append(column_id, *cell)))
    {
      if (OB_SIZE_OVERFLOW != ret)
      {
        TBSYS_LOG(WARN, "failed to append cell, err=%d", ret);
      }
      break;
    }
  } // end for i
  if (OB_SUCCESS == ret)
  {
    if (OB_SUCCESS != (ret = cell_writer.row_finish()))
    {
      if (OB_SIZE_OVERFLOW != ret)
      {
        TBSYS_LOG(WARN, "failed to append cell, err=%d", ret);
      }
    }
    else
    {
      compact_row.assign_ptr(compact_row.ptr(), static_cast<int32_t>(cell_writer.size()));
    }
  }
  return ret;
}

int ObRowUtil::convert(const ObRow &row, ObString &compact_row, ObRow &out_row)
{
  int ret = OB_SUCCESS;
  ObCompactCellWriter cell_writer;
  cell_writer.init(compact_row.ptr(), compact_row.size());

  const ObObj *cell = NULL;
  const ObObj *clone_cell = NULL;
  uint64_t table_id = OB_INVALID_ID;
  uint64_t column_id = OB_INVALID_ID;
  ObObj cell_clone;

  // shallow copy
  out_row = row;

  for (int64_t i = 0; i < row.get_column_num(); ++i)
  {
    if (OB_SUCCESS != (ret = row.raw_get_cell(i, cell, table_id, column_id)))
    {
      TBSYS_LOG(WARN, "failed to get cell, err=%d", ret);
      break;
    }
    else if (OB_SUCCESS != (ret = out_row.raw_get_cell(i, clone_cell, table_id, column_id)))
    {
      TBSYS_LOG(WARN, "failed to get cell, err=%d", ret);
      break;
    }
    else if (OB_SUCCESS != (ret = cell_writer.append(column_id, *cell, const_cast<ObObj *>(clone_cell))))
    {
      if (OB_SIZE_OVERFLOW != ret)
      {
        TBSYS_LOG(WARN, "failed to append cell, err=%d size=%ld", ret, cell_writer.size());
      }
      break;
    }
  } // end for i
  if (OB_SUCCESS == ret)
  {
    if (OB_SUCCESS != (ret = cell_writer.row_finish()))
    {
      if (OB_SIZE_OVERFLOW != ret)
      {
        TBSYS_LOG(WARN, "failed to append cell, err=%d", ret);
      }
    }
    else
    {
      compact_row.assign_ptr(compact_row.ptr(), static_cast<int32_t>(cell_writer.size()));
    }
  }
  return ret;
}

int ObRowUtil::convert(const ObString &compact_row, ObRow &row)
{
  return convert(compact_row, row, NULL);
}

int ObRowUtil::convert(const ObString &compact_row, ObRow &row, ObString *rowkey)
{
  int ret = OB_SUCCESS;
  ObCompactCellIterator cell_reader;
  ObString compact_row2 = compact_row;
  uint64_t column_id = OB_INVALID_ID;
  ObObj cell;
  bool is_row_finished = false;
  int64_t cell_idx = 0;

  const ObObj *rowkey_obj = NULL;
  ObString tmp_rowkey;

  if(NULL == rowkey)
  {
    cell_reader.init(compact_row2, SPARSE);
  }
  else
  {
    cell_reader.init(compact_row2, DENSE_SPARSE);
  }


  if(OB_SUCCESS == ret && NULL != rowkey)
  {
    if(OB_SUCCESS != (ret = cell_reader.next_cell()))
    {
      TBSYS_LOG(WARN, "next cell fail:ret[%d]", ret);
    }
    else if(OB_SUCCESS != (ret = cell_reader.get_cell(rowkey_obj, &is_row_finished)))
    {
      TBSYS_LOG(WARN, "get cell fail:ret[%d]", ret);
    }

    if(OB_SUCCESS == ret)
    {
      rowkey_obj->get_varchar(*rowkey);
    }
  }

  while(OB_SUCCESS == (ret = cell_reader.next_cell()))
  {
    if (OB_SUCCESS != (ret = cell_reader.get_cell(column_id, cell, &is_row_finished)))
    {
      TBSYS_LOG(WARN, "failed to get cell, err=%d", ret);
      break;
    }
    else if (is_row_finished)
    {
      ret = OB_SUCCESS;
      break;
    }
    else if (OB_SUCCESS != (ret = row.raw_set_cell(cell_idx++, cell)))
    {
      TBSYS_LOG(WARN, "failed to set cell, err=%d", ret);
      break;
    }
  }
  if (cell_idx != row.get_column_num())
  {
    TBSYS_LOG(ERROR, "corrupted row data, col=%ld cell_num=%ld", row.get_column_num(), cell_idx);
    ret = OB_ERR_UNEXPECTED;
  }
  return ret;
}
