/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_tablet_join.cpp
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#include "ob_tablet_join.h"
#include "common/ob_compact_cell_iterator.h"
#include "common/ob_compact_cell_writer.h"
#include "common/ob_ups_row_util.h"
#include "common/ob_row_fuse.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

ObTabletJoin::JoinInfo::JoinInfo()
  :right_table_id_(OB_INVALID_ID),
  left_column_id_(OB_INVALID_ID),
  right_column_id_(OB_INVALID_ID)
{
}

ObTabletJoin::TableJoinInfo::TableJoinInfo()
  :left_table_id_(OB_INVALID_ID)
{
}

ObTabletJoin::ObTabletJoin()
  :batch_count_(0),
  fused_scan_(NULL),
  ups_multi_get_(NULL)
{
}

ObTabletJoin::~ObTabletJoin()
{
}

int ObTabletJoin::add_column_id(uint64_t column_id)
{
  int ret = OB_SUCCESS;
  if(OB_INVALID_ID == table_join_info_.left_table_id_)
  {
    ret = OB_ERROR;
    TBSYS_LOG(WARN, "please set tablet_join_info first");
  }
  else
  {
    if(OB_SUCCESS != (ret = curr_row_desc_.add_column_desc(table_join_info_.left_table_id_, column_id)))
    {
      TBSYS_LOG(WARN, "add column desc fail:ret[%d]", ret);
    }
  }
  return ret;
}


bool ObTabletJoin::check_inner_stat()
{
  bool ret = true;
  if(NULL == fused_scan_ || 0 == batch_count_ || NULL == ups_multi_get_)
  {
    ret = false;
    TBSYS_LOG(WARN, "check inner stat fail:fused_scan_[%p], batch_count_[%ld], ups_multi_get_[%p]", fused_scan_, batch_count_, ups_multi_get_);
  }
  return ret;
}

int ObTabletJoin::set_child(int32_t child_idx, ObPhyOperator &child_operator)
{
  int ret = OB_SUCCESS;
  switch(child_idx)
  {
    case 0:
      fused_scan_ = &child_operator;
      break;
    default:
      ret = OB_ERR_UNEXPECTED;
      break;
  }
  return ret;
}

int ObTabletJoin::open()
{
  int ret = OB_SUCCESS;
  if(!check_inner_stat())
  {
    ret = OB_ERROR;
    TBSYS_LOG(WARN, "check inner stat fail");
  }
  else if(OB_SUCCESS != (ret = fused_scan_->open()))
  {
    TBSYS_LOG(WARN, "open fused scan fail:ret[%d]", ret);
  }
  else if(OB_SUCCESS != (ret = ups_row_cache_.init(KVCACHE_SIZE)))
  {
    TBSYS_LOG(WARN, "init join cache fail:ret[%d]", ret);
  }
  else if(OB_SUCCESS != (ret = gen_ups_row_desc()))
  {
    TBSYS_LOG(WARN, "gen ups row desc:ret[%d]", ret);
  }
  else
  {
    ups_row_cache_.clear();
    fused_row_store_.clear();
  }
  return ret;
}

int ObTabletJoin::close()
{
  int ret = OB_SUCCESS;
  if(NULL != fused_scan_)
  {
    if(OB_SUCCESS != (ret = fused_scan_->close()))
    {
      TBSYS_LOG(WARN, "close fused scan fail:ret[%d]", ret);
    }
  }
  return ret;
}

int ObTabletJoin::compose_get_param(uint64_t table_id, const ObString &rowkey, ObGetParam &get_param)
{
  int ret = OB_SUCCESS;

  ObCellInfo cell_info;
  cell_info.table_id_ = table_id;
  cell_info.row_key_ = rowkey;
  JoinInfo join_info;

  for(int64_t i=0; (OB_SUCCESS == ret) && i<table_join_info_.join_column_.count();i++)
  {
    if(OB_SUCCESS != (ret = table_join_info_.join_column_.at(i, join_info)))
    {
      TBSYS_LOG(WARN, "get join info fail:ret[%d], i[%ld]", ret, i);
    }
    else
    {
      cell_info.column_id_ = join_info.right_column_id_;
      if(OB_SUCCESS != (ret = get_param.add_cell(cell_info)))
      {
        TBSYS_LOG(WARN, "add cell info to get_param fail:ret[%d]", ret);
      }
    }
  }
  return ret;
}

int ObTabletJoin::get_right_table_rowkey(const ObRow &row, uint64_t &right_table_id, ObString &rowkey) const
{
  int ret = OB_SUCCESS;
  const ObObj *cell = NULL;
  JoinInfo join_info;

  if(OB_SUCCESS != (ret = table_join_info_.join_condition_.at(0, join_info)))
  {
    TBSYS_LOG(WARN, "get join condition fail:ret[%d]", ret);
  }

  if(OB_SUCCESS == ret)
  {
    right_table_id = join_info.right_table_id_;

    if(OB_SUCCESS != (ret = row.get_cell(table_join_info_.left_table_id_, join_info.left_column_id_, cell)))
    {
      TBSYS_LOG(WARN, "row get cell fail:ret[%d]", ret);
    }
    else if(OB_SUCCESS != (ret = cell->get_varchar(rowkey)))
    {
      TBSYS_LOG(WARN, "get_varchar fail:ret[%d]", ret);
    }
  }

  return ret;
}

int ObTabletJoin::fetch_fused_row(ObGetParam *get_param)
{
  int ret = OB_SUCCESS;
  const ObRow *scan_row = NULL;
  bool is_end = true;
  const ObRowStore::StoredRow *stored_row = NULL;
  ObString rowkey;
  ObString value;
  uint64_t right_table_id = OB_INVALID_ID;

  if(NULL == get_param)
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "get param is null");
  }

  for(int64_t i=0;(OB_SUCCESS == ret) && i < batch_count_;i++)
  {
    ret = fused_scan_->get_next_row(scan_row);
    if(OB_SUCCESS == ret)
    {
      is_end = false;
      if(OB_SUCCESS != (ret = fused_row_store_.add_row(*scan_row, stored_row)))
      {
        TBSYS_LOG(WARN, "add row to row store fail:ret[%d]", ret);
      }
      else if(OB_SUCCESS != (ret = get_right_table_rowkey(*scan_row, right_table_id, rowkey)))
      {
        TBSYS_LOG(WARN, "get rowkey from tmp_row fail:ret[%d]", ret);
      }

      if(OB_SUCCESS == ret)
      {
        ret = ups_row_cache_.get(rowkey, value);
        if(OB_ENTRY_NOT_EXIST == ret)
        {
          if(OB_SUCCESS != (ret = compose_get_param(right_table_id, rowkey, *get_param)))
          {
            TBSYS_LOG(WARN, "compose get param fail:ret[%d]", ret);
          }
        }
        else if(OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "join cache get fail:ret[%d]", ret);
        }
      }
    }
    else if(OB_ITER_END == ret)
    {
      if(!is_end)
      {
        ret = OB_SUCCESS;
      }
      break;
    }
    else
    {
      TBSYS_LOG(WARN, "get next row from fused scan fail:ret[%d]", ret);
    }
  }

  return ret;
}

int ObTabletJoin::gen_ups_row_desc()
{
  int ret = OB_SUCCESS;
  JoinInfo join_info;
  for(int64_t i=0;(OB_SUCCESS == ret) && i<table_join_info_.join_column_.count();i++)
  {
    if(OB_SUCCESS != (ret = table_join_info_.join_column_.at(i, join_info)))
    {
      TBSYS_LOG(WARN, "get join column fail:ret[%d], i[%ld]", ret, i);
    }
    else if(OB_SUCCESS != (ret = ups_row_desc_.add_column_desc(join_info.right_table_id_, join_info.right_column_id_)))
    {
      TBSYS_LOG(WARN, "ups_row_desc_ add column desc fail:ret[%d]", ret);
    }
    else if(OB_SUCCESS != (ret = ups_row_desc_for_join_.add_column_desc(table_join_info_.left_table_id_, join_info.left_column_id_)))
    {
      TBSYS_LOG(WARN, "ups_row_desc_for_join_ add column desc fail:ret[%d]", ret);
    }
  }
  return ret;
}

int ObTabletJoin::fetch_ups_row(ObGetParam *get_param)
{
  int ret = OB_SUCCESS;
  const ObUpsRow *ups_row = NULL;
  ObString compact_row; //用于存储ObRow的紧缩格式，内存已经分配好
  const ObString *rowkey = NULL;
  ThreadSpecificBuffer::Buffer *buffer = NULL;
  if(NULL == get_param)
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "get param is null");
  }

  if(OB_SUCCESS == ret)
  {
    buffer = thread_buffer_.get_buffer();
    if(NULL == buffer)
    {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      TBSYS_LOG(WARN, "get thread specific buffer fail:ret[%d]", ret);
    }
    else
    {
      buffer->reset();
    }
  }

  if(OB_SUCCESS == ret && get_param->get_cell_size() > 0)
  {
    ups_multi_get_->reset();
    ups_multi_get_->set_get_param(*get_param);
    ups_multi_get_->set_row_desc(ups_row_desc_);
    
    if(OB_SUCCESS != (ret = ups_multi_get_->open()))
    {
      TBSYS_LOG(WARN, "open ups multi get fail:ret[%d]", ret);
    }

    while(OB_SUCCESS == ret)
    {
      const ObRow *tmp_row_ptr = NULL;
      ret = ups_multi_get_->get_next_row(rowkey, tmp_row_ptr);
      if(OB_ITER_END == ret)
      {
        ret = OB_SUCCESS;
        break;
      }
      else if(OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "ups multi get next row fail:ret[%d]", ret);
      }
      else
      {
        ups_row = dynamic_cast<const ObUpsRow *>(tmp_row_ptr);
        if(NULL == ups_row)
        {
          ret = OB_ERR_UNEXPECTED;
          TBSYS_LOG(WARN, "shoud be ObUpsRow");
        }
      }

      if(OB_SUCCESS == ret)
      {
        compact_row.assign_buffer(buffer->current(), buffer->remain());
        if(OB_SUCCESS != (ret = ObUpsRowUtil::convert(*ups_row, compact_row)))
        {
          TBSYS_LOG(WARN, "convert ups row to compact row fail:ret[%d]", ret);
        }
        else if(OB_SUCCESS != (ret = ups_row_cache_.put(*rowkey, compact_row)))
        {
          TBSYS_LOG(WARN, "join cache put fail:ret[%d]", ret);
        }
      }
    }

    if(OB_SUCCESS == ret && NULL != ups_multi_get_)
    {
      if(OB_SUCCESS != (ret = ups_multi_get_->close()))
      {
        TBSYS_LOG(WARN, "close ups multi get fail:ret[%d]", ret);
      }
    }
  }

  return ret;
}


int ObTabletJoin::get_next_row(const ObRow *&row)
{
  int ret = OB_SUCCESS;
  ObString rowkey;
  ObString value;
  ObRow tmp_row;
  ObUpsRow tmp_ups_row;

  tmp_row.set_row_desc(curr_row_desc_);
  tmp_ups_row.set_row_desc(ups_row_desc_);

  ObGetParam *get_param = NULL;
  uint64_t right_table_id = OB_INVALID_ID;

  if(NULL == (get_param = GET_TSI_MULT(ObGetParam, common::TSI_SQL_GET_PARAM_1)))
  {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    TBSYS_LOG(WARN, "get tsi get_param fail:ret[%d]", ret);
  }
  else
  {
    get_param->reset(true);
  }

  bool row_valid = false;

  if(OB_SUCCESS == ret)
  {
    ret = fused_row_store_.get_next_row(tmp_row);
    /* 如果fused cache里面的行已经使用完 */
    if(OB_ITER_END == ret)
    {
      fused_row_store_.clear();
      if(OB_SUCCESS != (ret = fetch_fused_row(get_param)))
      {
        if(OB_ITER_END != ret)
        {
          TBSYS_LOG(WARN, "fetch_fused_row fail :ret[%d]", ret);
        }
      }
      else if(OB_SUCCESS != (ret = fetch_ups_row(get_param)))
      {
        TBSYS_LOG(WARN, "fetch_ups_row fail:ret[%d]", ret);
      }
    }
    else if(OB_SUCCESS == ret)
    {
      row_valid = true;
    }
    else
    {
      TBSYS_LOG(WARN, "fused row store next row fail:ret[%d]", ret);
    }
  }

  if(OB_SUCCESS == ret)
  {
    if(!row_valid)
    {
      ret = fused_row_store_.get_next_row(tmp_row);
      if(OB_SUCCESS != ret || OB_ITER_END == ret)
      {
        TBSYS_LOG(WARN, "fused row store next row fail:ret[%d]", ret);
      }
    }

    if(OB_SUCCESS == ret)
    {
      if(OB_SUCCESS != (ret = get_right_table_rowkey(tmp_row, right_table_id, rowkey)))
      {
        TBSYS_LOG(WARN, "get rowkey from tmp_row fail:ret[%d]", ret);
      }
    }

    if(OB_SUCCESS == ret)
    {
      ret = ups_row_cache_.get(rowkey, value);
      if(OB_SUCCESS == ret)
      {
        if(OB_SUCCESS != (ret = ObUpsRowUtil::convert(right_table_id, value, tmp_ups_row)))
        {
          TBSYS_LOG(WARN, "convert to ups row fail:ret[%d]", ret);
        }
        else
        {
          tmp_ups_row.set_row_desc(ups_row_desc_for_join_);
          if(OB_SUCCESS != (ret = ObRowFuse::fuse_row(&tmp_ups_row, &tmp_row, &curr_row_)))
          {
            TBSYS_LOG(WARN, "fused ups row to row fail:ret[%d]", ret);
          }
          else
          {
            row = &curr_row_;
          }
        }
      }
      else if(OB_ENTRY_NOT_EXIST == ret)
      {
        curr_row_.assign(tmp_row);
        row = &curr_row_;
        ret = OB_SUCCESS;
      }
      else
      {
        TBSYS_LOG(WARN, "ups_row_cache_ get fail:ret[%d] rowkey[%.*s]", ret, rowkey.length(), rowkey.ptr());
      }
    }
  }

  return ret;
}

int64_t ObTabletJoin::to_string(char* buf, const int64_t buf_len) const
{
  UNUSED(buf);
  UNUSED(buf_len);
  return 0;
}


