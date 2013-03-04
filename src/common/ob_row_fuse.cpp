/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_row_fuse.c
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@taobao.com>
 *
 */

#include "ob_row_fuse.h"

using namespace oceanbase::common;

/*
 * 将一个ups的行付给一个普通行
 * 主要是把OP_NOP转换成null
 * 没有处理is_add的情况，如果原来的ObObj带有is_add，转换后还将带有is_add
 */
int ObRowFuse::assign(const ObUpsRow &incr_row, ObRow &result)
{
  int ret = OB_SUCCESS;
  result.raw_row_.clear();
  result.set_row_desc(*(incr_row.get_row_desc()));

  const ObObj *cell = NULL;
  ObObj null_cell;
  null_cell.set_null();

  uint64_t table_id = OB_INVALID_ID;
  uint64_t column_id = OB_INVALID_ID;

  for(int64_t i=0;(OB_SUCCESS == ret) && i<incr_row.get_column_num();i++)
  {
    if(OB_SUCCESS != (ret = incr_row.raw_get_cell(i, cell, table_id, column_id)))
    {
      TBSYS_LOG(WARN, "incr row get cell fail:ret[%d]", ret);
    }
    else 
    {
      if(ObExtendType == cell->get_type() && cell->get_ext() == ObActionFlag::OP_NOP)
      {
        cell = &null_cell;
      }

      if(OB_SUCCESS != (ret = result.raw_set_cell(i, *cell)))
      {
        TBSYS_LOG(WARN, "result set_cell fail:ret[%d]", ret);
      }
    }
  }
  return ret;
}

/*
 * 把从ups获取的增量数据合并到sstable数据上，按照行合并
 * sstable_row包含的cell必须是incr_row包含cell的超集,否则会出错
 * 如果incr_row含有delete标记, sstable_row中所有incr_row对应的cell将被清空
 * 并设置为incr_row中的值，其他的cell不受影响
 */
int ObRowFuse::fuse_row(const ObUpsRow *incr_row, const ObRow *sstable_row, ObRow *result)
{
  int ret = OB_SUCCESS;
  if(NULL == incr_row || NULL == sstable_row || NULL == result)
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "incr_row[%p], sstable_row[%p], result[%p]", incr_row, sstable_row, result);
  }

  const ObObj *tmp_sstable_cell = NULL;
  ObObj sstable_cell;
  const ObObj *incr_cell = NULL;
  uint64_t table_id = 0;
  uint64_t column_id = 0;

  ObObj null_cell;
  null_cell.set_null();

  if(OB_SUCCESS == ret)
  {
    result->assign(*sstable_row);
    for(int64_t i=0;(OB_SUCCESS == ret) && (i<incr_row->get_column_num());i++)
    {
      if(OB_SUCCESS != (ret = incr_row->raw_get_cell(i, incr_cell, table_id, column_id)))
      {
        TBSYS_LOG(WARN, "incr raw get cell fail:ret[%d] ,index[%ld]", ret, i);
        break;
      }

      if(OB_SUCCESS == ret)
      {
        if(ObExtendType == incr_cell->get_type())
        {
          if(ObActionFlag::OP_NOP == incr_cell->get_ext())
          {
            if(incr_row->is_delete_row())
            {
              if(OB_SUCCESS != (ret = result->set_cell(table_id, column_id, null_cell)))
              {
                TBSYS_LOG(WARN, "set cell fail:ret[%d]", ret);
              }
            }
          }
          else
          {
            ret = OB_ERR_UNEXPECTED;
            TBSYS_LOG(WARN, "invalid extend type:[%ld]", incr_cell->get_ext());
          }
        }
        else
        {
          if(incr_row->is_delete_row())
          {
            if(OB_SUCCESS != (ret = result->set_cell(table_id, column_id, *incr_cell)))
            {
              TBSYS_LOG(WARN, "set cell fail:ret[%d]", ret);
            }
          }
          else
          {
            ret = result->get_cell(table_id, column_id, tmp_sstable_cell);
            if(OB_SUCCESS == ret && NULL != tmp_sstable_cell && NULL != incr_cell)
            {
              sstable_cell = *tmp_sstable_cell;
              sstable_cell.apply(*incr_cell);
              ret = result->set_cell(table_id, column_id, sstable_cell);
              if(OB_SUCCESS != ret)
              {
                TBSYS_LOG(WARN, "result->set cell fail:ret[%d]", ret);
              }
            }
            else
            {
              TBSYS_LOG(WARN, "result->get cell fail:ret[%d], sstable_cell[%p], incr_cell[%p]", ret, tmp_sstable_cell, incr_cell);
            }
          }
        }
      }
    }
  }
  return ret;
}

