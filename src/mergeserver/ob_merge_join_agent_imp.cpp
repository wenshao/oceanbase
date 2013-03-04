/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * ob_merge_join_agent.cpp is for what ...
 *
 * Version: $id: ob_merge_join_agent.cpp,v 0.1 9/19/2010 9:46a wushi Exp $
 *
 * Authors:
 *   wushi <wushi.ly@taobao.com>
 *     - some work details if you want
 *
 */
#include "ob_merge_join_agent_imp.h"
#include "common/ob_malloc.h"
#include "common/utility.h"
#include "common/ob_cache.h"
#include "common/ob_scanner.h"
#include "common/ob_action_flag.h"
#include "common/ob_tsi_factory.h"
#include "common/ob_trace_log.h"
#include "ob_merge_server_main.h"
#include "ob_read_param_modifier.h"
#include "ob_ms_tsi.h"
#include "ob_ms_define.h"
#include <iostream>
#include <algorithm>
#include <poll.h>
#include "common/ob_simple_right_join_cell.h"
using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::mergeserver;

void ObMergeJoinOperator::initialize()
{
  ups_scan_stream_ = NULL;
  ups_get_stream_ = NULL;
  ups_join_stream_ = NULL;
  request_finished_ = false;
  cs_request_finished_ = false;
  scan_param_ = NULL;
  get_param_ = NULL;
  req_param_ = NULL;
  cur_get_param_.reset();
  got_cell_num_ = 0;
  left_join_column_count_ = 0;
  cur_scan_param_.reset();
  cur_cs_result_.clear();
  is_scan_query_ = false;
  is_need_query_ups_ = true;
  is_need_join_ = true;
  merger_iterator_moved_ = false;
  merger_.reset();
}

ObMergeJoinOperator::ObMergeJoinOperator():join_cell_vec_(128),join_row_width_vec_(64), join_offset_vec_(128)
{
  rpc_proxy_ = NULL;
  initialize();
}

ObMergeJoinOperator::~ObMergeJoinOperator()
{
  clear();
}


bool ObMergeJoinOperator::is_request_finished()const
{
  return request_finished_;
}

void ObMergeJoinOperator::clear()
{
  int64_t memory_size_water_mark = OB_MS_THREAD_MEM_CACHE_LOWER_WATER_MARK;
  if (max_memory_size_ > 0)
  {
    memory_size_water_mark = 2 * max_memory_size_;
  }
  if (get_memory_size_used() > memory_size_water_mark)
  {
    ObCellArray::clear();
  }
  else
  {
    ObCellArray::reset();
  }
  //join_param_array_.clear();
  join_cell_vec_.clear();
  join_offset_vec_.clear();
  join_row_width_vec_.clear();
  initialize();
}

void ObMergeJoinOperator::prepare_next_round()
{
  ObCellArray::clear();
}

/// @note 这是一个很丑陋的实现，ObMergeJoinOperator中使用了fake_get_param_来实现get与scan逻辑的相同,
///       但是ObGetParam对于add的cell必须保证rowkey的合法性，于是这里就有了这个丑陋的实现
char ObMergeJoinOperator::ugly_fake_get_param_rowkey_ = 'a';

int ObMergeJoinOperator::set_request_param(const int64_t timeout_us, const ObScanParam &scan_param,
    ObMSScanCellStream &ups_stream, ObMSGetCellStream &ups_join_stream,
    const ObSchemaManagerV2 &schema_mgr, const int64_t max_memory_size)
{
  int err = OB_SUCCESS;
  ObMSSchemaDecoderAssis *schema_assis = GET_TSI_MULT(ObMSSchemaDecoderAssis, SCHEMA_DECODER_ASSIS_ID);
  if (NULL == schema_assis)
  {
    TBSYS_LOG(WARN,"fail to allocate memory for ObMSSchemaDecoderAssis");
    err = OB_ALLOCATE_MEMORY_FAILED;
  }
  else
  {
    schema_assis->init();
    is_scan_query_ = true;
    is_need_join_ = false;
    ups_get_stream_ = NULL;
    ups_scan_stream_ = &ups_stream;
    ups_join_stream_ = &ups_join_stream;
    schema_mgr_ = &schema_mgr;
    max_memory_size_ = max_memory_size;
    fake_get_param_.reset();
    int32_t column_idx = 0;
    int32_t first_join_cell_idx = 0;
    param_contain_duplicated_columns_ = false;
    memset(column_id_join_map_, 0, sizeof(column_id_join_map_));
    memset(column_id_idx_map_, -1, sizeof(column_id_idx_map_));
    memset(left_column_idx_map_, -1, sizeof(left_column_idx_map_));
    int32_t cell_size = static_cast<int32_t>(scan_param.get_column_id_size());
    ObCellInfo fake_cell;
    join_only_one_table_ = true;
    uint64_t first_join_table_id = OB_INVALID_ID;
    fake_cell.table_id_ = scan_param.get_table_id();
    fake_cell.row_key_.assign(&ugly_fake_get_param_rowkey_,sizeof(ugly_fake_get_param_rowkey_));
    const ObColumnSchemaV2 * column_schema = NULL;
    const ObColumnSchemaV2::ObJoinInfo *join_info = NULL;
    for (int32_t cell_idx = 0; cell_idx < cell_size && OB_SUCCESS == err; cell_idx++)
    {
      fake_cell.column_id_ = scan_param.get_column_id()[cell_idx];
      column_schema = schema_mgr.get_column_schema(fake_cell.table_id_,fake_cell.column_id_, &column_idx);
      if (NULL == column_schema || fake_cell.column_id_ >= OB_MAX_VALID_COLUMN_ID)
      {
        TBSYS_LOG(ERROR, "unexpected error, fail to get column schema [table_id:%lu,column_id:%lu,"
            "max_valid_column_id:%lu, column_schema:%p]",
            fake_cell.table_id_, fake_cell.column_id_, OB_MAX_VALID_COLUMN_ID, column_schema);
        err = OB_ERR_UNEXPECTED;
      }
      else
      {
        column_id_idx_map_[fake_cell.column_id_] = cell_idx;
        err = fake_get_param_.add_cell(fake_cell);
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN,"fail to add cell to fake_cell [err:%d]", err);
        }
        else
        {
          ++schema_assis->column_idx_in_org_param_[column_idx];
          if (schema_assis->column_idx_in_org_param_[column_idx] > ObMSSchemaDecoderAssis::INVALID_IDX + 1)
          {
            TBSYS_LOG(DEBUG, "query request contail duplicated column");
            param_contain_duplicated_columns_ = true;
          }
          join_info = column_schema->get_join_info();
          if (join_info != NULL)
          {
            column_id_join_map_[fake_cell.column_id_] = join_info;
            ++left_join_column_count_;
            // first join cell idx
            if (!is_need_join_)
            {
              first_join_cell_idx = cell_idx;
              is_need_join_ = true;
              first_join_table_id = join_info->join_table_;
            }
            else if (join_info->join_table_ != first_join_table_id)
            {
              join_only_one_table_ = false;
            }
            if (!param_contain_duplicated_columns_)
            {
              left_column_idx_map_[join_info->correlated_column_] = cell_idx - first_join_cell_idx;
              TBSYS_LOG(DEBUG, "left[%lu], offset[%d], right[%lu], first_join[%d]",
                  fake_cell.column_id_, cell_idx, join_info->correlated_column_, first_join_cell_idx);
            }
          }
        }
      }
    }
    if (OB_SUCCESS == err)
    {
      scan_param_ = &scan_param;
      req_param_ = &fake_get_param_;
    }
  }
  if(OB_SUCCESS == err)
  {
    timeout_us_ = timeout_us;
    start_time_us_ =  tbsys::CTimeUtil::getTime();
  }
  if (OB_SUCCESS == err)
  {
    err = get_next_rpc_result();
    if (OB_ITER_END == err)
    {
      ObCellArray::clear();
      err = OB_SUCCESS;
    }
  }
  return err;
}

int ObMergeJoinOperator::set_request_param(const int64_t timeout_us, const ObGetParam &get_param,
    ObMSGetCellStream &ups_stream, ObMSGetCellStream &ups_join_stream,
    const ObSchemaManagerV2 &schema_mgr, const int64_t max_memory_size)
{
  ups_scan_stream_ = NULL;
  ups_get_stream_ = &ups_stream;
  ups_join_stream_ = &ups_join_stream;
  schema_mgr_ = &schema_mgr;
  max_memory_size_ = max_memory_size;
  /// @note just ignore max_memory_size
  max_memory_size_ = -1;
  req_param_ = &get_param;
  get_param_ = &get_param;
  is_scan_query_ = false;
  param_contain_duplicated_columns_ = true;
  timeout_us_ = timeout_us;
  start_time_us_ =  tbsys::CTimeUtil::getTime();
  int err = get_next_rpc_result();
  if (OB_ITER_END == err)
  {
    ObCellArray::clear();
    err = OB_SUCCESS;
  }
  return err;
}

void ObMergeJoinOperator::move_to_next_row(const ObGetParam & row_spec_arr,
    const int64_t row_spec_arr_size, int64_t &cur_row_beg, int64_t &cur_row_end)
{
  if (cur_row_end != row_spec_arr_size)
  {
    cur_row_beg = cur_row_end;
    while (cur_row_end < row_spec_arr_size
        && row_spec_arr[cur_row_end]->table_id_ == row_spec_arr[cur_row_beg]->table_id_
        && row_spec_arr[cur_row_end]->row_key_ == row_spec_arr[cur_row_beg]->row_key_
        )
    {
      cur_row_end ++;
    }
  }
}

int ObMergeJoinOperator::prepare_join_param_()
{
  int err = OB_SUCCESS;
  const ObColumnSchemaV2::ObJoinInfo *join_info = NULL;
  ObInnerCellInfo *join_left_cell = NULL;
  int32_t start_pos = 0;
  int32_t end_pos = 0;
  ObRowCellVec *join_cache_row = NULL;
  uint64_t prev_cache_row_table_id = OB_INVALID_ID;
  ObString prev_cache_row_key;
  bool prev_row_searched_cache = false;
  bool prev_row_hit_cache = false;
  bool is_row_changed = false;
  bool need_change_row_key = true;
  uint64_t prev_join_right_table_id = OB_INVALID_ID;
  ObCellArray::reset_iterator();
  int64_t cell_num = 0;
  uint64_t prev_table_id = 0;
  ObString prev_row_key;
  int64_t           last_join_row_width = 0;
  int64_t           join_row_width_processed_cell_num = 0;
  ObCellInfo        apply_join_right_cell;
  ObCellInfo        apply_cell_adjusted;
  ObSimpleRightJoinCell cell;
  while (OB_SUCCESS == err)
  {
    err = ObCellArray::next_cell();
    if (OB_SUCCESS == err)
    {
      // if scan only using the first join cell for prepare the join table rowkey
      err = ObCellArray::get_cell(&join_left_cell,&is_row_changed);
      if (OB_SUCCESS == err)
      {
        cell_num ++;
        if (join_left_cell->value_.get_ext() == ObActionFlag::OP_ROW_DOES_NOT_EXIST
            || join_left_cell->value_.get_ext() == ObActionFlag::OP_DEL_ROW
            || join_left_cell->value_.get_ext() == ObActionFlag::OP_DEL_TABLE)
        {
          continue;
        }
        if (is_row_changed)
        {
          need_change_row_key = true;
        }
      }
    }
    else if (OB_ITER_END == err)
    {
      err = OB_SUCCESS;
      break;
    }
    if (OB_SUCCESS == err)
    {
      join_info = get_join_info(join_left_cell->table_id_, join_left_cell->column_id_);
    }
    if (OB_SUCCESS == err && NULL != join_info)
    {
      apply_join_right_cell.table_id_ = join_info->join_table_;
      apply_join_right_cell.column_id_  = join_info->correlated_column_;
      if (OB_INVALID_ID == apply_join_right_cell.column_id_
          || OB_INVALID_ID == apply_join_right_cell.table_id_)
      {
        TBSYS_LOG(WARN,"unepxected error, fail to get ids from join info "
            "[left_table_id:%lu,left_join_columnid:%lu]",
            join_left_cell->table_id_,  join_left_cell->column_id_);
        err = OB_ERR_UNEXPECTED;
      }
      if (OB_SUCCESS == err)
      {
        if (apply_join_right_cell.table_id_ != prev_join_right_table_id)
        {
          start_pos = join_info->start_pos_;
          end_pos = join_info->end_pos_;
          /// @note both equal -1, means the whole rowkey is join rowkey
          if (-1 == start_pos && -1 == end_pos)
          {
            start_pos = 0;
            end_pos = join_left_cell->row_key_.length();
          }
          else if (start_pos >= 0 && end_pos >= 0)
          {
            end_pos ++;
          }
          if (end_pos < 0  || start_pos < 0  || end_pos < start_pos
              || end_pos > join_left_cell->row_key_.length())
          {
            TBSYS_LOG(WARN,"unepxected error, join key error [start_pos:%d,end_pos:%d,"
                "left_rowkey_len:%d]", start_pos, end_pos,  join_left_cell->row_key_.length());
            hex_dump(join_left_cell->row_key_.ptr(),join_left_cell->row_key_.length());
            err = OB_ERR_UNEXPECTED;
          }
        }
        if (OB_SUCCESS == err
            && (apply_join_right_cell.table_id_ != prev_join_right_table_id || need_change_row_key))
        {
          apply_join_right_cell.row_key_.assign(join_left_cell->row_key_.ptr() + start_pos,
              end_pos - start_pos);
          need_change_row_key = false;
        }
      }
      if (OB_SUCCESS == err)
      {
        /// search in cache first
        int get_cache_err = OB_SUCCESS;
        if (prev_row_searched_cache
            && prev_cache_row_table_id == apply_join_right_cell.table_id_
            && prev_cache_row_key == apply_join_right_cell.row_key_
           )
        {
          if (prev_row_hit_cache)
          {
            get_cache_err = OB_SUCCESS;
            join_cache_row->reset_iterator();
          }
          else
          {
            get_cache_err = OB_ENTRY_NOT_EXIST;
          }
        }
        else
        {
          get_cache_err = ups_join_stream_->get_cache_row(apply_join_right_cell,join_cache_row);
          prev_row_searched_cache = true;
          prev_cache_row_table_id = apply_join_right_cell.table_id_;
          prev_cache_row_key = apply_join_right_cell.row_key_;
          prev_row_hit_cache = (OB_SUCCESS == get_cache_err);
          prev_join_right_table_id = apply_join_right_cell.table_id_;
        }
        if (OB_SUCCESS == get_cache_err)
        {
          ObCellInfo *cur_cell = NULL;
          while (OB_SUCCESS == err)
          {
            err = join_cache_row->next_cell();
            if (OB_ITER_END == err)
            {
              err = OB_SUCCESS;
              break;
            }
            if (OB_SUCCESS == err)
            {
              err = join_cache_row->get_cell(&cur_cell);
              if (OB_SUCCESS == err
                  && (cur_cell->column_id_ == apply_join_right_cell.column_id_
                    || cur_cell->value_.get_ext() == ObActionFlag::OP_DEL_ROW
                    || cur_cell->value_.get_ext() == ObActionFlag::OP_DEL_TABLE
                    || cur_cell->value_.get_ext() == ObActionFlag::OP_NOP
                    )
                 )
              {
                // apply_cell_adjusted.table_id_ = join_left_cell->table_id_;
                // apply_cell_adjusted.column_id_ = join_left_cell->column_id_;
                // apply_cell_adjusted.row_key_ = join_left_cell->row_key_;
                // apply_cell_adjusted.value_ = join_left_cell->value_;
                err = ob_get_join_value(apply_cell_adjusted.value_, join_left_cell->value_, cur_cell->value_);
                if (OB_SUCCESS == err)
                {
                  err = ObCellArray::apply(apply_cell_adjusted, join_left_cell);
                }
                else
                {
                  TBSYS_LOG(WARN,"fail to get join mutation [tableid:%lu,rowkey:%.*s,"
                      "column_id:%lu,type:%d,ext:%ld]",cur_cell->table_id_,
                      cur_cell->row_key_.length(), cur_cell->row_key_.ptr(),
                      cur_cell->column_id_, cur_cell->value_.get_type(),
                      cur_cell->value_.get_ext());
                  hex_dump(cur_cell->row_key_.ptr(),cur_cell->row_key_.length(),
                      true,TBSYS_LOG_LEVEL_WARN);
                }
              }
            }
          }///end while
        }
        /// get according to rpc
        if (OB_SUCCESS != get_cache_err && OB_SUCCESS == err)
        {
          /// @warning every join get the whole row
          /// @todo better method : get the whole row just
          /// when cache was setted in ObUPSCellStream
          if ( apply_join_right_cell.table_id_ == prev_table_id
              && apply_join_right_cell.row_key_ == prev_row_key
             )
          {
            /// @note 去重，合并同行的多个请求，只get一次，防止连续一行的
            ///   cell数量超过OB_MAX_COLUMN_NUM
          }
          else
          {
            if (join_cell_vec_.size() != 0)
            {
              err = join_row_width_vec_.push_back(last_join_row_width);
              join_row_width_processed_cell_num += last_join_row_width;
            }
            last_join_row_width = 0;
            cell.table_id = apply_join_right_cell.table_id_;
            cell.rowkey = apply_join_right_cell.row_key_;
            err = join_cell_vec_.push_back(cell);
            prev_table_id = apply_join_right_cell.table_id_;
            prev_row_key = apply_join_right_cell.row_key_;
          }
          if (OB_SUCCESS == err)
          {
            last_join_row_width ++;
            err = join_offset_vec_.push_back(cell_num - 1);
          }
        }
      }
    }
  }
  if (OB_SUCCESS == err && join_offset_vec_.size() > 0)
  {
    if (join_cell_vec_.size() != join_row_width_vec_.size() + 1
        || join_offset_vec_.size() -  join_row_width_processed_cell_num != last_join_row_width)
    {
      TBSYS_LOG(ERROR, "%s", "unexpected implementation algorithm error");
      err = OB_ERR_UNEXPECTED;
    }
    if (OB_SUCCESS == err)
    {
      err = join_row_width_vec_.push_back(last_join_row_width);
    }
  }
  if (OB_SUCCESS== err)
  {
    ObCellArray::reset_iterator();
  }
  return err;
}

namespace
{
  class ObGetParamCellIterator
  {
  public:
    ObGetParamCellIterator();
    ~ObGetParamCellIterator();
    int init(const ObGetParam & get_param, int64_t cell_idx);
    ObGetParamCellIterator &operator++();
    ObGetParamCellIterator operator++(int);
    bool operator==(const ObGetParamCellIterator& other);
    bool operator!=(const ObGetParamCellIterator& other);
    ObCellInfo *operator->();
    ObCellInfo &operator*();
  private:
    const ObGetParam *get_param_;
    int64_t           cell_size_;
    int64_t cell_idx_;
    static ObCellInfo fake_cell_;
  };
  ObCellInfo ObGetParamCellIterator::fake_cell_;

  ObGetParamCellIterator::ObGetParamCellIterator()
  {
    get_param_ = NULL;
    cell_idx_ = -1;
    cell_size_ = 0;
  }
  ObGetParamCellIterator::~ObGetParamCellIterator()
  {
    get_param_ = NULL;
    cell_idx_ = -1;
    cell_size_ = 0;
  }

  int ObGetParamCellIterator::init(const ObGetParam & get_param, int64_t cell_idx)
  {
    int err = OB_SUCCESS;
    if (cell_idx < 0 || cell_idx > get_param.get_cell_size())
    {
      TBSYS_LOG(ERROR, "logic error, cell index out of range [cell_idx:%ld,"
        "get_param.get_cell_size:%ld]", cell_idx, get_param.get_cell_size());
      err = OB_SIZE_OVERFLOW;
    }
    if (OB_SUCCESS == err)
    {
      get_param_ = &get_param;
      cell_size_ = get_param_->get_cell_size();
      cell_idx_ = cell_idx;
    }
    else
    {
      get_param_ = NULL;
      cell_idx_ = -1;
      cell_size_ = 0;
    }
    return err;
  }

  ObGetParamCellIterator &ObGetParamCellIterator::operator ++()
  {
    cell_idx_ ++;
    return *this;
  }

  ObGetParamCellIterator ObGetParamCellIterator::operator ++(int)
  {
    ObGetParamCellIterator res;
    res = *this;
    cell_idx_ ++;
    return res;
  }

  bool ObGetParamCellIterator::operator==(const ObGetParamCellIterator& other)
  {
    return((get_param_ == other.get_param_) && (cell_idx_ == other.cell_idx_));
  }

  bool ObGetParamCellIterator::operator!=(const ObGetParamCellIterator& other)
  {
    return !(*this == other);
  }

  ObCellInfo *ObGetParamCellIterator::operator->()
  {
    ObCellInfo *result = NULL;
    if (cell_idx_ < 0 || cell_idx_ >= cell_size_)
    {
      TBSYS_LOG(ERROR, "logic error, try to access cell index out of range"
        "[get_param_:%p,cell_idex_:%ld,get_param_->get_cell_size:%ld]",
        get_param_, cell_idx_, get_param_ != NULL ? get_param_->get_cell_size() : 0);
      result = &fake_cell_;
    }
    else
    {
      result = get_param_->operator [](cell_idx_);
      if (NULL == result)
      {
        TBSYS_LOG(ERROR, "logic error, within range cell is null"
          "[get_param_:%p,cell_idex_:%ld,get_param_->get_cell_size:%ld]",
          get_param_, cell_idx_, get_param_ != NULL ? get_param_->get_cell_size() : 0);
        result = &fake_cell_;
      }
    }
    return result;
  }

  ObCellInfo &ObGetParamCellIterator::operator*()
  {
    return *(this->operator->());
  }
}


int ObMergeJoinOperator::apply_whole_row(const common::ObCellInfo &cell, const int64_t row_beg)
{
  int err = OB_SUCCESS;
  int adjusted_non_exist_cell_num = 0;
  int64_t cell_idx = row_beg;
  ObInnerCellInfo *dst_cell = NULL;
  int64_t dest_idx = -1;
  if (!param_contain_duplicated_columns_
    && OB_INVALID_ID != cell.column_id_ )
  {
    dest_idx = column_id_idx_map_[cell.column_id_];
    if (dest_idx >= ObCellArray::get_cell_size() - row_beg || dest_idx < 0)
    {
      TBSYS_LOG(WARN,"dest idx err [dest_idx:%ld,column_id:%ld,get_cell_size():%ld,row_beg:%ld]",
          dest_idx, cell.column_id_, ObCellArray::get_cell_size(),row_beg);
      err = OB_ERR_UNEXPECTED;
    }
    else
    {
      err = ObCellArray::get_cell(row_beg + dest_idx,dst_cell);
      if (OB_SUCCESS == err)
      {
        if (dst_cell->value_.get_ext() == ObActionFlag::OP_ROW_DOES_NOT_EXIST)
        {
          dst_cell->value_.set_null();
          adjusted_non_exist_cell_num ++;
        }
        if (cell.column_id_ == dst_cell->column_id_)
        {
          err = ObCellArray::apply(cell,dst_cell);
          if (OB_SUCCESS != err)
          {
            TBSYS_LOG(WARN,"fail to apply mutation [err:%d]", err);
          }
        }
        else
        {
          TBSYS_LOG(WARN,"unexpected error, column id not coincident");
          err = OB_ERR_UNEXPECTED;
        }
      }
      else
      {
        TBSYS_LOG(WARN,"fail to get dest cell from cell array [err:%d]", err);
      }
    }
  }
  else
  {
    for (;OB_SUCCESS == err && cell_idx < ObCellArray::get_cell_size();cell_idx ++)
    {
      err = ObCellArray::get_cell(cell_idx,dst_cell);
      if (OB_SUCCESS == err)
      {
        if (dst_cell->value_.get_ext() == ObActionFlag::OP_ROW_DOES_NOT_EXIST)
        {
          dst_cell->value_.set_null();
          adjusted_non_exist_cell_num ++;
        }
        if (cell.column_id_ == dst_cell->column_id_)
        {
          err = ObCellArray::apply(cell,dst_cell);
          if (OB_SUCCESS != err)
          {
            TBSYS_LOG(WARN,"fail to apply mutation [err:%d]", err);
          }
        }
      }
      else
      {
        TBSYS_LOG(WARN,"fail to get dest cell from cell array [err:%d]", err);
      }
    }
  }
  if (OB_SUCCESS == err)
  {
    err = adjusted_non_exist_cell_num;
  }
  return err;
}

int ObMergeJoinOperator::merge_next_row(int64_t &cur_row_beg, int64_t &cur_row_end)
{
  int err = OB_SUCCESS;
  bool keep_deleted_row = (NULL != get_param_);
  bool first_cell_got = false;
  bool row_changed = false;
  bool keep_move = true;
  bool iterator_end = false;
  move_to_next_row(*req_param_, req_param_->get_cell_size(), cur_row_beg, cur_row_end);
  int64_t src_idx = cur_row_beg;
  ObCellInfo *cur_cell = NULL;
  ObCellInfo cur_cell_initializer;
  ObInnerCellInfo *cell_out = NULL;
  int64_t not_exist_cell_num  = 0;
  int64_t exist_cell_num_got = 0;
  int64_t ext_val = 0;
  if (OB_SUCCESS == err)
  {
    if (cur_row_beg < req_param_->get_cell_size())
    {
      cur_cell_initializer.table_id_ = req_param_->operator [](cur_row_beg)->table_id_;
    }
    else
    {
      TBSYS_LOG(WARN,"unexpected error [cur_row_beg:%ld,req_param_->get_cell_size():%ld]",
        cur_row_beg, req_param_->get_cell_size());
      err = OB_ERR_UNEXPECTED;
    }
  }
  while (OB_SUCCESS == err)
  {
    /// move iterator
    if (!merger_iterator_moved_ && keep_move)
    {
      err  = merger_.next_cell();
      if (OB_SUCCESS != err && OB_ITER_END != err)
      {
        TBSYS_LOG(WARN,"fail to move to next cell in merger [err:%d]", err);
      }
      else if (OB_SUCCESS == err)
      {
        merger_iterator_moved_ = true;
      }
    }
    /// get current cell
    if (OB_SUCCESS == err && keep_move)
    {
      err = merger_.get_cell(&cur_cell, &row_changed);
      if (OB_SUCCESS == err)
      {
        /// ext_val = cur_cell->value_.get_ext();
        if (row_changed && !first_cell_got)
        {
          /// cur_row_key_buf_.reset();
          cur_cell_initializer.value_.set_ext(ObActionFlag::OP_ROW_DOES_NOT_EXIST);
          cur_cell_initializer.row_key_ = cur_cell->row_key_;
        }
        if (row_changed && first_cell_got)
        {
          /// this row process was completed
          break;
        }
        else
        {
          merger_iterator_moved_ = false;
          /// first_cell_got = true;
          if (first_cell_got)
          {
            cur_cell->row_key_ = cur_cell_initializer.row_key_;
          }
        }
      }
      else
      {
        TBSYS_LOG(WARN,"fail to get cell from merger [err:%d]", err);
      }
    }

    /// bugfix by xielun.szd 20110805
    if (OB_SUCCESS == err)
    {
      ext_val = cur_cell->value_.get_ext();
    }

    /// process current cell
    if (OB_SUCCESS == err
      && (((ext_val != ObActionFlag::OP_DEL_ROW)
      && (ext_val != ObActionFlag::OP_DEL_TABLE)
      && (ext_val != ObActionFlag::OP_ROW_DOES_NOT_EXIST))
      || keep_deleted_row))
    {
      if ((ext_val != ObActionFlag::OP_DEL_ROW)
        && (ext_val != ObActionFlag::OP_DEL_TABLE)
        && (ext_val != ObActionFlag::OP_ROW_DOES_NOT_EXIST)
        )
      {
        if (keep_move)
        {
          exist_cell_num_got ++;
        }
      }
      else
      {
        cur_cell->column_id_ = OB_INVALID_ID;
      }
      if (src_idx < cur_row_end) /// initialize stage
      {
        if (!param_contain_duplicated_columns_
          && req_param_->operator [](src_idx)->column_id_ == cur_cell->column_id_)
        {/// 进行二路归并
          err = ObCellArray::append(*cur_cell, cell_out);
          if (OB_SUCCESS == err)
          {
            /// when data only exist on ups, OB semantic
            if (ObActionFlag::OP_NOP == cell_out->value_.get_ext())
            {
              cell_out->value_.set_null();
            }
            else if (cell_out->value_.get_add())
            {
              cell_out->value_.set_add(false);
            }
            keep_move = true;
          }
          else
          {
            TBSYS_LOG(WARN,"fail to append cell to cell array [err:%d]", err);
          }
        }
        else
        {/// append一个empty cell
          keep_move = false;
          cur_cell_initializer.column_id_  = req_param_->operator [](src_idx)->column_id_;
          err = ObCellArray::append(cur_cell_initializer, cell_out);
          if (OB_SUCCESS == err)
          {
            if (exist_cell_num_got > 0)
            {
              cell_out->value_.set_null();
            }
            else
            {
              not_exist_cell_num ++;
            }
          }
          else
          {
            TBSYS_LOG(WARN,"fail to append cell to cell array [err:%d]", err);
          }
        }
        if (OB_SUCCESS == err)
        {
          src_idx ++;
        }
        if (!first_cell_got && OB_SUCCESS == err)
        {
          first_cell_got = true;
          cur_cell_initializer.row_key_ = cell_out->row_key_;
        }
      }
      else
      { /// 当前行的每个cell都已经初始化了
        if ((ext_val == ObActionFlag::OP_DEL_ROW)
          || (ext_val == ObActionFlag::OP_DEL_TABLE)
          || (ext_val == ObActionFlag::OP_ROW_DOES_NOT_EXIST))
        {
          /// just jump
          keep_move = true;
        }
        else
        {
          err = apply_whole_row(*cur_cell, ObCellArray::get_cell_size() - (cur_row_end - cur_row_beg));
          if (err >= 0)
          {
            not_exist_cell_num -= err;
            keep_move = true;
            err = OB_SUCCESS;
          }
          else
          {
            TBSYS_LOG(WARN,"fail to apply whole row [err:%d]", err);
          }
        }
      }
    }
    else
    {
      /// do nothing
      keep_move = true;
    }
  }/// end while
  if (OB_SUCCESS == err || OB_ITER_END == err)
  {
    if (OB_ITER_END == err)
    {
      err = OB_SUCCESS;
      iterator_end = true;
    }
    if (first_cell_got
      && (exist_cell_num_got > 0 || keep_deleted_row)
      && src_idx < cur_row_end)
    {
      for (;src_idx < cur_row_end && OB_SUCCESS == err; src_idx ++)
      {
        cur_cell_initializer.column_id_  = req_param_->operator [](src_idx)->column_id_;
        err = ObCellArray::append(cur_cell_initializer, cell_out);
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN,"fail to append initialize cell [err:%d]", err);
        }
        else
        {
          if (exist_cell_num_got > 0)
          {
            cell_out->value_.set_null();
          }
          else
          {
            not_exist_cell_num ++;
          }
        }
      }
    }
  }

  if (OB_SUCCESS == err || OB_ITER_END == err)
  {
    if (not_exist_cell_num > 0 && exist_cell_num_got > 0)
    {
      cur_cell_initializer.value_.set_ext(ObActionFlag::OP_NOP);
      cur_cell_initializer.column_id_ = OB_INVALID_ID;
      err = apply_whole_row(cur_cell_initializer,ObCellArray::get_cell_size() - (cur_row_end - cur_row_beg));
      if (err >= 0)
      {
        not_exist_cell_num -= err;
        if (0 != not_exist_cell_num)
        {
          TBSYS_LOG(ERROR, "fatal error, algorithm error [not_exist_cell_num:%ld]", not_exist_cell_num);
          err = OB_ERR_UNEXPECTED;
        }
        else
        {
          err = OB_SUCCESS;
        }
      }
      else
      {
        TBSYS_LOG(WARN,"fail to apply whole row [err:%d]", err);
      }
    }
  }
  if (OB_SUCCESS == err && iterator_end)
  {
    err = OB_ITER_END;
  }
  return err;
}

int ObMergeJoinOperator::merge()
{
  int err = OB_SUCCESS;
  int64_t cur_row_beg = 0;
  int64_t cur_row_end = 0;
  int64_t got_row_count = 0;
  int64_t limit_count = 0;
  int64_t limit_offset = 0;
  int64_t aggreagate_row_width = 0;
  int64_t orderby_column_size = 0;
  if (scan_param_ != NULL)
  {
    scan_param_->get_limit_info(limit_offset, limit_count);
    limit_count += limit_count / 10;
    aggreagate_row_width = scan_param_->get_group_by_param().get_aggregate_row_width();
    orderby_column_size = scan_param_->get_orderby_column_size();
  }
  while (OB_SUCCESS == err)
  {
    err = merge_next_row(cur_row_beg, cur_row_end);
    if (NULL != scan_param_ && OB_SUCCESS == err)
    {
      got_row_count++;
      if (!((aggreagate_row_width > 0) || (orderby_column_size > 0))
          && ((limit_count > 0) && (limit_count + limit_offset <= got_row_count)))
      {
        break;
      }
    }
    if (OB_SUCCESS == err && max_memory_size_ > 0 && get_real_memory_used() > max_memory_size_)
    {
      break;
    }
  }
  if (OB_ITER_END == err)
  {
    err = OB_SUCCESS;
  }
  if (OB_SUCCESS == err
      && is_need_join_
      && is_need_query_ups_
      && ObCellArray::get_cell_size() > 0)
  {
    err = prepare_join_param_();
  }
  if (OB_SUCCESS == err && !merger_iterator_moved_ && cs_request_finished_)
  {
    request_finished_ = true;
  }
  return err;
}

// join apply all the columns in a row
template<typename IteratorT>
int ObMergeJoinOperator::join_apply_all_cell(const ObCellInfo & cell, IteratorT & dst_off_beg, IteratorT & dst_off_end)
{
  int err = OB_SUCCESS;
  ObInnerCellInfo *affected_cell = NULL;
  const ObColumnSchemaV2::ObJoinInfo * join_info = NULL;
  for (;dst_off_beg != dst_off_end && OB_SUCCESS == err; ++dst_off_beg)
  {
    err = ObCellArray::get_cell(*dst_off_beg, affected_cell);
    if (OB_SUCCESS == err)
    {
      join_info = get_join_info(affected_cell->table_id_, affected_cell->column_id_);
      if (NULL != join_info)
      {
        if (cell.table_id_ != join_info->join_table_)
        {
          TBSYS_LOG(ERROR,"unexpected error, ups return unwanted result [expected_tableid:%lu,real_tableid:%lu]",
              join_info->join_table_, cell.table_id_);
          err = OB_ERR_UNEXPECTED;
          break;
        }
        affected_cell->value_.set_null();
      }
    }
  }
  return err;
}

// not check input param
int ObMergeJoinOperator::join_apply_one_cell(const ObCellInfo & right_cell, const int64_t offset)
{
  ObInnerCellInfo * left_cell = NULL;
  int err = ObCellArray::get_cell(offset, left_cell);
  if ((err != OB_SUCCESS) || (NULL == left_cell))
  {
    TBSYS_LOG(WARN, "get cell failed [offset:%ld, right_table:%lu, right_column:%lu]",
        offset, right_cell.table_id_, right_cell.column_id_);
  }
  else
  {
    //join_apply_cell_adjusted_.table_id_ = left_cell->table_id_;
    //join_apply_cell_adjusted_.column_id_ = left_cell->column_id_;
    //join_apply_cell_adjusted_.row_key_ = left_cell->row_key_;
    err = ob_get_join_value(join_apply_cell_adjusted_.value_, left_cell->value_, right_cell.value_);
    if (OB_SUCCESS != err)
    {
      TBSYS_LOG(WARN,"fail to get join value [left_value.type:%d,right_value.type:%d,err:%d]",
          left_cell->value_.get_type(), right_cell.value_.get_type(), err);
    }
    else if (left_cell->value_.get_ext() != ObActionFlag::OP_ROW_DOES_NOT_EXIST)/// left table row exists
    {
      err = ObCellArray::apply(join_apply_cell_adjusted_, left_cell);
      if (OB_SUCCESS != err)
      {
        TBSYS_LOG(WARN,"apply error [affect_cell->table_id:%lu,affect_cell->rowkey:%.*s,"
            "affect_cell->column_id:%lu,affect_cell->type:%d,affect_cell->ext:%ld,"
            "mutation->table_id:%lu,mutation->rowkey->%.*s,"
            "mutation->column_id:%lu,mutation->type:%d,mutation->ext:%ld",
            left_cell->table_id_, left_cell->row_key_.length(),
            left_cell->row_key_.ptr(), left_cell->column_id_,
            left_cell->value_.get_type(), left_cell->value_.get_ext(),
            right_cell.table_id_, right_cell.row_key_.length(),
            right_cell.row_key_.ptr(), right_cell.column_id_,
            right_cell.value_.get_type(), right_cell.value_.get_ext());
        hex_dump(left_cell->row_key_.ptr(), left_cell->row_key_.length(),
            true,TBSYS_LOG_LEVEL_WARN);
        hex_dump(right_cell.row_key_.ptr(),right_cell.row_key_.length(),
            true,TBSYS_LOG_LEVEL_WARN);
      }
    }
    else
    {
      err = OB_ERR_UNEXPECTED;
      TBSYS_LOG(ERROR, "can not goto this path find left table rowkey not exist"
          "[left_table:%lu, left_col:%lu, right_table:%lu, right_col:%lu]",
          left_cell->table_id_, left_cell->column_id_,
          right_cell.table_id_, right_cell.column_id_);
    }
  }
  return err;
}

template<typename IteratorT>
int ObMergeJoinOperator::join_apply_directly(const ObCellInfo & cell, const int64_t cell_count,
    IteratorT & dst_off_beg, IteratorT & dst_off_end)
{
  int err = OB_SUCCESS;
  /// 在join的时候，如果碰到delete row，那么所有参与join的cell都必须设置为null
  if ((cell.value_.get_ext() == ObActionFlag::OP_DEL_ROW)
      || (cell.value_.get_ext() == ObActionFlag::OP_DEL_TABLE))
  {
    err = join_apply_all_cell(cell, dst_off_beg, dst_off_end);
  }
  else
  {
    // find the join left table offset must be join column
    int32_t idx = left_column_idx_map_[cell.column_id_];
    if (idx >= 0 && left_join_column_count_ > 0)
    {
      if (cell_count % left_join_column_count_ != 0)
      {
        TBSYS_LOG(ERROR, "check cell count not be divisible by join count:cell[%ld], join_count[%ld]",
            cell_count, left_join_column_count_);
        err = OB_ERR_UNEXPECTED;
      }
      else
      {
        int64_t row_count = cell_count / left_join_column_count_;
        for (int64_t i = 0; (OB_SUCCESS == err) && (i < row_count); ++i)
        {
          // get the left cell and check the right join cell ids
          err = join_apply_one_cell(cell, *(dst_off_beg + i * left_join_column_count_) + idx);
          if (err != OB_SUCCESS)
          {
            TBSYS_LOG(WARN, "join apply one cell failed:right_table[%lu], right_column[%lu], "
                "left_row[%ld], left_idx[%d], cur_row[%ld], row_count[%ld], err[%d]",
                cell.table_id_, cell.column_id_, *dst_off_beg, idx, i, row_count, err);
            break;
          }
        }
      }
    }
  }
  return err;
}

template<typename IteratorT>
int ObMergeJoinOperator::join_apply(const ObCellInfo & cell, IteratorT & dst_off_beg, IteratorT & dst_off_end)
{
  int err = OB_SUCCESS;
  ObInnerCellInfo *affected_cell = NULL;
  const ObColumnSchemaV2::ObJoinInfo *join_info = NULL;
  uint64_t right_cell_table_id = 0;
  for (;dst_off_beg != dst_off_end && OB_SUCCESS == err; ++dst_off_beg)
  {
    err = ObCellArray::get_cell(*dst_off_beg, affected_cell);
    if (OB_SUCCESS != err)
    {
      TBSYS_LOG(WARN, "get cell failed [offset:%ld, err:%d]", *dst_off_beg, err);
      break;
    }
    else
    {
      join_info = get_join_info(affected_cell->table_id_, affected_cell->column_id_);
      if (NULL != join_info)
      {
        right_cell_table_id = join_info->join_table_;
        if (cell.table_id_ != right_cell_table_id)
        {
          TBSYS_LOG(ERROR,"unexpected error, ups return unwanted result [expected_tableid:%lu,real_tableid:%lu]",
              right_cell_table_id, cell.table_id_);
          err = OB_ERR_UNEXPECTED;
          break;
        }
      }
    }
    // apply the right table cell to the left column cell
    if (OB_SUCCESS == err && join_info != NULL)
    {
      /// 在join的时候，如果碰到delete row，那么所有参与join的cell都必须设置为null
      if (cell.value_.get_ext() == ObActionFlag::OP_DEL_ROW
          || cell.value_.get_ext() == ObActionFlag::OP_DEL_TABLE
         )
      {
        affected_cell->value_.set_null();
      }
      else if (join_info->correlated_column_ == cell.column_id_)
      {
        join_apply_cell_adjusted_.table_id_ = affected_cell->table_id_;
        join_apply_cell_adjusted_.column_id_ = affected_cell->column_id_;
        // join_apply_cell_adjusted_.row_key_ = affected_cell->row_key_;
        err = ob_get_join_value(join_apply_cell_adjusted_.value_,affected_cell->value_,cell.value_);
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN,"fail to get join value [left_value.type:%d,right_value.type:%d,err:%d]",
              affected_cell->value_.get_type(), cell.value_.get_type(), err);
        }
        else if (affected_cell->value_.get_ext() != ObActionFlag::OP_ROW_DOES_NOT_EXIST)/// left table row exists
        {
          err = ObCellArray::apply(join_apply_cell_adjusted_,affected_cell);
          if (OB_SUCCESS != err)
          {
            TBSYS_LOG(WARN,"apply error [affect_cell->table_id:%lu,affect_cell->rowkey:%.*s,"
                "affect_cell->column_id:%lu,affect_cell->type:%d,affect_cell->ext:%ld,"
                "mutation->table_id:%lu,mutation->rowkey->%.*s,"
                "mutation->column_id:%lu,mutation->type:%d,mutation->ext:%ld",
                affected_cell->table_id_, affected_cell->row_key_.length(),
                affected_cell->row_key_.ptr(), affected_cell->column_id_,
                affected_cell->value_.get_type(), affected_cell->value_.get_ext(),
                join_apply_cell_adjusted_.table_id_, join_apply_cell_adjusted_.row_key_.length(),
                join_apply_cell_adjusted_.row_key_.ptr(), join_apply_cell_adjusted_.column_id_,
                join_apply_cell_adjusted_.value_.get_type(), join_apply_cell_adjusted_.value_.get_ext());
            hex_dump(affected_cell->row_key_.ptr(),affected_cell->row_key_.length(),
                true,TBSYS_LOG_LEVEL_WARN);
            hex_dump(join_apply_cell_adjusted_.row_key_.ptr(),join_apply_cell_adjusted_.row_key_.length(),
                true,TBSYS_LOG_LEVEL_WARN);
          }
        }
        else
        {
          err = OB_ERR_UNEXPECTED;
          TBSYS_LOG(ERROR, "can not goto this path find left table rowkey not exist"
              "[left_table:%lu, left_col:%lu, right_table:%lu, right_col:%lu]",
              affected_cell->table_id_, affected_cell->column_id_,
              join_apply_cell_adjusted_.table_id_, join_apply_cell_adjusted_.column_id_);
          break;
        }
      }
    }
  }
  return err;
}

int ObMergeJoinOperator::join()
{
  int err = OB_SUCCESS;
  ObCellInfo *cur_cell = NULL;
  bool is_row_changed = false;
  ObMSGetCellArray get_cells(join_cell_vec_);
  common::ObVector<common::ObSimpleRightJoinCell>::iterator src_cell_it_beg = join_cell_vec_.begin();
  common::ObVector<common::ObSimpleRightJoinCell>::iterator join_param_end = join_cell_vec_.end();
  ObVector<int64_t>::iterator dst_width_it = join_row_width_vec_.begin();
  int64_t processed_dst_cell_num = 0;
  int64_t src_cell_idx = 0;
  bool is_first_row = true;
  err = ups_join_stream_->get(cur_join_read_param_, get_cells, cur_cs_addr_);
  int64_t size = join_cell_vec_.size();
  while (OB_SUCCESS == err
    && size > 0
    &&  src_cell_it_beg != join_param_end)
  {
    err = ups_join_stream_->next_cell();
    if (OB_ITER_END == err)
    {
      src_cell_it_beg ++;
      break;
    }
    if (OB_SUCCESS == err)
    {
      err = ups_join_stream_->get_cell(&cur_cell, &is_row_changed);
    }
    if (OB_SUCCESS == err && is_row_changed && !is_first_row)
    {
      processed_dst_cell_num += *dst_width_it;
      ++src_cell_it_beg;
      ++dst_width_it;
      ++src_cell_idx;
      if ((OB_SUCCESS == err) &&
        (src_cell_it_beg->rowkey != cur_cell->row_key_||src_cell_it_beg->table_id != cur_cell->table_id_))
      {
        TBSYS_LOG(ERROR, "updateserver return result not wanted [src_cell_idx:%ld,"
          "get_param_size:%d]", src_cell_idx,join_cell_vec_.size());
        hex_dump(src_cell_it_beg->rowkey.ptr(), src_cell_it_beg->rowkey.length(),
          true,TBSYS_LOG_LEVEL_ERROR);
        hex_dump(cur_cell->row_key_.ptr(),cur_cell->row_key_.length(),true,
          TBSYS_LOG_LEVEL_ERROR);
        err = OB_ERR_UNEXPECTED;
        break;
      }
    }
    if (OB_SUCCESS == err)
    {
      ObVector<int64_t>::iterator dst_off_it_beg = join_offset_vec_.begin() + processed_dst_cell_num;
      ObVector<int64_t>::iterator dst_off_it_end = dst_off_it_beg + *dst_width_it;
      if (!param_contain_duplicated_columns_ && join_only_one_table_)
      {
        err = join_apply_directly(*cur_cell, *dst_width_it, dst_off_it_beg, dst_off_it_end);
      }
      else
      {
        err = join_apply(*cur_cell, dst_off_it_beg, dst_off_it_end);
      }
      is_first_row = false;
    }
    if (OB_SUCCESS == err && get_real_memory_used() > OB_MS_THREAD_MEM_CACHE_UPPER_WATER_MARK)
    {
      while (true)
      {
        poll(NULL,0,5000);
        TBSYS_LOG(ERROR, "this thread using too much memory [memory_used:%ld, total:%ld]",
            get_real_memory_used(), get_memory_size_used());
      }
    }
  }
  if ((join_offset_vec_.size() > 0)
    && ((OB_ITER_END != err)
    || (src_cell_it_beg  != join_cell_vec_.end())))
  {
    if (OB_SUCCESS == err)
    {
      TBSYS_LOG(ERROR, "%s", "unxpected error, update server return result not coincident with request");
      err = OB_ERR_UNEXPECTED;
    }
  }
  else if (OB_ITER_END == err)
  {
    err = OB_SUCCESS;
  }
  if (OB_SUCCESS == err)
  {
    join_cell_vec_.clear();
    join_offset_vec_.clear();
    join_row_width_vec_.clear();
  }
  return err;
}

int ObMergeJoinOperator::get_next_rpc_result()
{
  int err = OB_SUCCESS;
  int64_t now = tbsys::CTimeUtil::getTime();
  if ((timeout_us_ >= 0) && (start_time_us_ + timeout_us_ < now))
  {
    TBSYS_LOG(WARN, "request timeout [start_time_us_:%ld, now:%ld, timeout_us_:%ld]",
        start_time_us_, now, timeout_us_);
    err = OB_PROCESS_TIMEOUT;
  }
  else if (NULL != get_param_)
  {
    err = get_next_get_rpc_result();
  }
  else if (NULL != scan_param_)
  {
    err = get_next_scan_rpc_result();
  }
  else
  {
    TBSYS_LOG(WARN, "%s", "cell stream was not initilized");
    err = OB_INNER_STAT_ERROR;
  }
  now = tbsys::CTimeUtil::getTime();
  if ((timeout_us_ >= 0) && (start_time_us_ + timeout_us_ < now) && (OB_SUCCESS == err))
  {
    TBSYS_LOG(WARN,"request timeout [start_time_us_:%ld,now:%ld,timeout_us_:%ld]",
        start_time_us_, now, timeout_us_);
    err = OB_PROCESS_TIMEOUT;
  }
  if (OB_SUCCESS == err)
  {
    err  = do_merge_join();
  }
  return err;
}

int ObMergeJoinOperator::get_next_get_rpc_result()
{
  int err = OB_SUCCESS;
  ObReadParam &cur_read_param = cur_get_param_;
  ObMSGetCellArray cur_get_cells(cur_get_param_);
  const ObReadParam &org_read_param = *get_param_;
  ObMSGetCellArray get_cells(*get_param_);
  err = get_next_param(org_read_param,get_cells,got_cell_num_,&cur_get_param_);
  oceanbase::common::ObIterator        *cur_cs_cell_it = NULL;
  /// get result from cs
  if (OB_SUCCESS == err)
  {
    err = rpc_proxy_->cs_get(cur_get_param_, cur_cs_addr_, cur_cs_result_, cur_cs_cell_it);
    TBSYS_LOG(DEBUG, "OP:cs_get [got_cell_num:%ld,cur_get_cell_num:%ld,data_version:%ld,res:%d]",
      got_cell_num_, cur_get_param_.get_cell_size(),cur_cs_result_.get_data_version(), err);
    if (OB_SUCCESS == err)
    {
      int64_t now_got_cell_num = 0;
      bool fullfilled = false;
      err = cur_cs_result_.get_is_req_fullfilled(fullfilled,now_got_cell_num);
      now_got_cell_num += got_cell_num_;
      if (OB_SUCCESS == err &&
        (get_next_param(org_read_param,get_cells,now_got_cell_num,NULL) == OB_ITER_END))
      {
        cs_request_finished_ = true;
      }
    }
  }
  if (OB_SUCCESS == err)
  {
    err = get_ups_param(cur_get_param_, cur_cs_result_);
    if (OB_SUCCESS == err)
    {
      is_need_query_ups_ = true;
      cur_join_read_param_ = cur_read_param;
      err = ups_get_stream_->get(cur_read_param,cur_get_cells,cur_cs_addr_);
    }
    if (OB_ITER_END == err)
    {
      err = OB_SUCCESS;
      is_need_query_ups_ = false;
      ups_get_stream_->reset();
    }
  }
  if (OB_SUCCESS == err)
  {
    merger_.reset();
    if (NULL != scan_param_)
    {
      merger_.set_asc(scan_param_->get_scan_direction() == ObScanParam::FORWARD);
    }
    err = merger_.add_iterator(cur_cs_cell_it);
  }
  if (OB_SUCCESS == err)
  {
    err = merger_.add_iterator(ups_get_stream_);
  }
  if (OB_SUCCESS == err)
  {
    bool is_fullfilled = false;
    int64_t fullfilled_cell_num  = 0;
    err = cur_cs_result_.get_is_req_fullfilled(is_fullfilled, fullfilled_cell_num);
    if (OB_SUCCESS == err)
    {
      got_cell_num_ += fullfilled_cell_num;
    }
    req_param_ = &cur_get_param_;
  }
  return err;
}

int ObMergeJoinOperator::get_next_scan_rpc_result()
{
  int err = OB_SUCCESS;
  ObReadParam &cur_read_param = cur_scan_param_;
  const ObRange *org_scan_range = NULL;
  org_scan_range = scan_param_->get_range();
  oceanbase::common::ObIterator        *cur_cs_cell_it = NULL;
  if (NULL == org_scan_range)
  {
    TBSYS_LOG(WARN, "%s", "unexpected error, fail to get range from scan param");
    err = OB_INVALID_ARGUMENT;
  }

  if (OB_SUCCESS == err)
  {
    err = get_next_param(*scan_param_,cur_cs_result_, &cur_scan_param_, cs_scan_buffer_);
  }

  if (OB_SUCCESS == err)
  {
    err = rpc_proxy_->cs_scan(cur_scan_param_, cur_cs_addr_, cur_cs_result_, cur_cs_cell_it);
    TBSYS_LOG(DEBUG, "OP:cs_scan [data_version:%ld,res:%d]", cur_cs_result_.get_data_version(), err);
    if (OB_SUCCESS == err
      && (get_next_param(*scan_param_,cur_cs_result_,NULL,cs_scan_buffer_) == OB_ITER_END))
    {
      cs_request_finished_ = true;
    }
  }

  if (OB_SUCCESS == err)
  {
    err = get_ups_param(cur_scan_param_,cur_cs_result_, ups_scan_buffer_);
    if (OB_SUCCESS == err)
    {
      is_need_query_ups_ = true;
      cur_join_read_param_ = cur_read_param;
      err = ups_scan_stream_->scan(cur_scan_param_, cur_cs_addr_);
    }
    if (OB_ITER_END == err)
    {
      is_need_query_ups_ = false;
      err  = OB_SUCCESS;
      ups_scan_stream_->reset();
    }
  }

  if (OB_SUCCESS == err)
  {
    merger_.reset();
    if (NULL != scan_param_)
    {
      merger_.set_asc(scan_param_->get_scan_direction() == ObScanParam::FORWARD);
    }
    merger_.add_iterator(cur_cs_cell_it);
  }
  if (OB_SUCCESS == err)
  {
    merger_.add_iterator(ups_scan_stream_);
  }
  return err;
}

int ObMergeJoinOperator::do_merge_join()
{
  int err = OB_SUCCESS;
  if (NULL == schema_mgr_)
  {
    TBSYS_LOG(WARN, "%s", "please set argument first");
    err = OB_INVALID_ARGUMENT;
  }
  else
  {
    err = merge();
    FILL_TRACE_LOG("after one merge operation result_cell_num[%ld] cell_num_need_join[%ld] err[%d]",
      get_cell_size(), join_cell_vec_.size(), err);
  }
  if (OB_SUCCESS == err)
  {
    // do join
    if (is_need_query_ups_ && join_cell_vec_.size() > 0)
    {
      err = join();
      FILL_TRACE_LOG("after one join operation err[%d]", err);
    }
  }
  return err;
}

ObGetMergeJoinAgentImp::~ObGetMergeJoinAgentImp()
{
  clear();
}

int ObGetMergeJoinAgentImp::set_request_param(const int64_t timeout_us, const ObGetParam &get_param,
  ObMSGetCellStream &ups_stream, ObMSGetCellStream &ups_join_stream,
  const ObSchemaManagerV2 &schema_mgr, const int64_t max_memory_size)
{
  int err = OB_SUCCESS;
  int real_memory_size = static_cast<int>(max_memory_size);
  /// get request must be fullfilled
  real_memory_size = -1;
  err = merge_join_operator_.set_request_param(timeout_us, get_param,ups_stream, ups_join_stream,
    schema_mgr,real_memory_size);
  return err;
}

bool ObScanMergeJoinAgentImp::return_uncomplete_result_ = false;

ObScanMergeJoinAgentImp::ObScanMergeJoinAgentImp()
{
  clear();
}

ObScanMergeJoinAgentImp::~ObScanMergeJoinAgentImp()
{
  clear();
}

int ObScanMergeJoinAgentImp::set_request_param(const int64_t timeout_us, const ObScanParam &scan_param,
  ObMSScanCellStream &ups_stream, ObMSGetCellStream &ups_join_stream,
  const ObSchemaManagerV2 &schema_mgr, const int64_t max_memory_size)
{
  int err = OB_SUCCESS;
  scan_param.get_limit_info(limit_offset_,limit_count_);
  if (OB_SUCCESS == err)
  {
    err = merge_join_operator_.set_request_param(timeout_us, scan_param, ups_stream, ups_join_stream,
      schema_mgr,max_memory_size);
  }
  if (OB_SUCCESS == err)
  {
    max_avail_mem_size_ = max_memory_size;
    param_ = &scan_param;
  }
  return err;
}

int ObScanMergeJoinAgentImp::filter_org_row_(const ObCellArray &cells, const int64_t row_beg,
  const int64_t row_end, const ObSimpleFilter & filter, bool &result)
{
  int err = filter.check(cells, row_beg,row_end,result);
  if (OB_SUCCESS != err)
  {
    TBSYS_LOG(WARN,"check row fail [err:%d]", err);
  }
  else
  {
    if (result
      && param_->get_group_by_param().get_aggregate_row_width() == 0
      && param_->get_orderby_column_size() == 0
      && limit_offset_ > 0)
    {
      /// @note optimization for request has only filter and limit info
      result = false;
      limit_offset_ --;
    }
  }
  return err;
}

int ObScanMergeJoinAgentImp::jump_limit_offset_(ObCellArray & cells, const int64_t jump_cell_num)
{
  int err = OB_SUCCESS;
  for (int64_t i = 0; i < jump_cell_num && OB_SUCCESS == err; i++)
  {
    err = cells.next_cell();
  }
  return err;
}

int ObScanMergeJoinAgentImp::prepare_final_result_()
{
  int err  = OB_SUCCESS;
  int64_t filter_count = param_->get_filter_info().get_count();
  int64_t aggregate_row_width = param_->get_group_by_param().get_aggregate_row_width();
  int64_t org_row_width = param_->get_column_id_size();
  int64_t orderby_count = param_->get_orderby_column_size();
  ObScanParam::Direction   scan_direction = param_->get_scan_direction();
  if (merge_join_operator_.get_cell_size()%org_row_width != 0)
  {
    TBSYS_LOG(ERROR,"unexpected error [org_row_width:%ld,merge_join_operator_.get_cell_size():%ld]", org_row_width,
      merge_join_operator_.get_cell_size());
    err = OB_ERR_UNEXPECTED;
  }
  if (OB_SUCCESS == err )
  {
    bool can_jump_limit_offset_directly = ((0 == filter_count) &&  (0 == aggregate_row_width)
      &&  (0 == orderby_count));
    if (can_jump_limit_offset_directly)
    {
      /// @note optimization for request has no filter or group by or order by
      pfinal_result_ = &merge_join_operator_;
      if (limit_offset_ > 0)
      {
        err = jump_limit_offset_(merge_join_operator_, limit_offset_ * org_row_width);
        if (OB_SUCCESS == err)
        {
          limit_offset_ = 0;
        }
      }
    }
  }
  bool need_copy = ((0 < filter_count)  || (0 < aggregate_row_width)
    || (0 < orderby_count) || (ObScanParam::BACKWARD == scan_direction));
  if (OB_SUCCESS == err && need_copy)
  {
    err = prepare_final_result_process_intermediate_result_();
    if (err != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "prepare final result failed:err[%d]", err);
    }
  }
  if (OB_SUCCESS == err)
  {
    bool all_record_is_one_group = ((0 < aggregate_row_width)
      && (0 == param_->get_group_by_param().get_groupby_columns().get_array_index()));
    if ( all_record_is_one_group && (0 == groupby_operator_.get_cell_size()))
    {
      /// if there is no result, and aggregate functions act on all rows, i.e. all rows belong to
      /// one group
      err = groupby_operator_.init_all_in_one_group_row();
      if (OB_SUCCESS == err)
      {
        pfinal_result_ = &groupby_operator_;
      }
      else
      {
        TBSYS_LOG(WARN,"fail to init all in one group row [err:%d]", err);
      }
    }
  }
  int64_t real_row_width = aggregate_row_width>0 ? aggregate_row_width : org_row_width;
  if (OB_SUCCESS == err && orderby_count > 0)
  {
    int64_t const * orderby_idxs = NULL;
    uint8_t const * orders = NULL;
    param_->get_orderby_column(orderby_idxs,orders,orderby_count);
    for (int64_t i = 0; i < orderby_count; i++)
    {
      orderby_desc_[i].cell_idx_ = static_cast<int32_t>(orderby_idxs[i]);
      orderby_desc_[i].order_ = orders[i];
    }
    err = pfinal_result_->orderby(real_row_width, orderby_desc_, orderby_count);
    if (OB_SUCCESS != err)
    {
      TBSYS_LOG(WARN,"fail to orderby result [err:%d]", err);
    }
  }
  if (OB_SUCCESS == err && limit_offset_ > 0)
  {
    err = jump_limit_offset_(*pfinal_result_,limit_offset_ * real_row_width);
    if (OB_SUCCESS == err)
    {
      limit_offset_ = 0;
    }
  }
  if (OB_SUCCESS == err && limit_count_ > 0)
  {
    max_avail_cell_num_ = real_row_width * limit_count_;
  }
  if (OB_SUCCESS == err
    && (0 == aggregate_row_width)
    && (0 == orderby_count)
    && (ObScanParam::BACKWARD == scan_direction))
  {
    /// reverse rows
    pfinal_result_->reverse_rows(real_row_width);
  }
  return err;
}

int ObScanMergeJoinAgentImp::prepare_final_result_process_intermediate_result_()
{
  int err = OB_SUCCESS;
  bool pass_filter = false;
  int64_t got_row_count = 0;
  int64_t aggregate_row_width = param_->get_group_by_param().get_aggregate_row_width();
  int64_t org_row_width = param_->get_column_id_size();
  int64_t orderby_count = param_->get_orderby_column_size();
  pfinal_result_ = &groupby_operator_;
  err = groupby_operator_.init(param_->get_group_by_param(),max_avail_mem_size_);
  if (OB_SUCCESS != err)
  {
    TBSYS_LOG(WARN,"fail to init groupby operator [err:%d]", err);
  }
  bool need_fetch_all_result = ((aggregate_row_width > 0) || (orderby_count > 0));
  const ObSimpleFilter & filter = param_->get_filter_info();
  while (OB_SUCCESS == err)
  {
    bool size_over_flow = false;
    /// @note at this place, iterator access and direct access both exist,
    ///   must be careful, it is a bad experience
    for (int64_t i = merge_join_operator_.get_consumed_cell_num();
      i < merge_join_operator_.get_cell_size() && OB_SUCCESS == err;
      i += org_row_width)
    {
      err = filter_org_row_(merge_join_operator_, i, i+org_row_width - 1, filter, pass_filter);
      if ((OB_SUCCESS == err) && pass_filter)
      {
        err = groupby_operator_.add_row(merge_join_operator_, i, i + org_row_width - 1);
        if (OB_SUCCESS == err)
        {
          got_row_count ++;
          if (!need_fetch_all_result)
          {
            if (((limit_count_ > 0)  && (limit_count_ + limit_offset_ <= got_row_count))
              || ((max_avail_mem_size_ > 0 )
              && (groupby_operator_.get_real_memory_used() > max_avail_mem_size_)))
            {
              size_over_flow = true;
              TBSYS_LOG(WARN, "groupby result take too much memory [used:%ld, total:%ld, max_avail:%ld, row_count:%ld]",
                groupby_operator_.get_real_memory_used(), groupby_operator_.get_memory_size_used(),
                max_avail_mem_size_, got_row_count);
              break;
            }
          }
          else
          {
            if (max_avail_mem_size_ > 0
              && groupby_operator_.get_real_memory_used() > max_avail_mem_size_)
            {
              if (return_uncomplete_result_ && (param_->get_group_by_param().get_aggregate_row_width() == 0))
              {
                TBSYS_LOG(WARN, "groupby result take too much memory [used:%ld, total:%ld, max_avail:%ld, row_count:%ld]",
                  groupby_operator_.get_real_memory_used(), groupby_operator_.get_memory_size_used(),
                  max_avail_mem_size_, got_row_count);
                size_over_flow = true;
                break;
              }
              else
              {
                TBSYS_LOG(WARN, "groupby result take too much memory [used:%ld, max_avail:%ld, row_count:%ld]",
                  groupby_operator_.get_memory_size_used(), max_avail_mem_size_, got_row_count);
                err  = OB_MEM_OVERFLOW;
              }
            }
          }
        }
        else
        {
          TBSYS_LOG(WARN,"fail to add row to group by result [err:%d]", err);
        }
      }
      else if (OB_SUCCESS != err)
      {
        TBSYS_LOG(WARN,"filter org row fail [err:%d]", err);
      }
      else /// if (OB_SUCCESS == err && !pass_filter)
      {
        /// do nothing
      }
    }///endfor
    if (OB_SUCCESS == err && size_over_flow)
    {
      break;
    }
    else if (OB_SUCCESS == err)
    {
      merge_join_operator_.consume_all_cell();
      err = merge_join_operator_.next_cell();
      if (OB_ITER_END == err)
      {
        err = OB_SUCCESS;
        break;
      }
      else if (OB_SUCCESS != err)
      {
        TBSYS_LOG(WARN,"fail to get next cell from merge join operator [err:%d]", err);
      }
      else
      {
        if (merge_join_operator_.get_cell_size()%org_row_width != 0)
        {
          TBSYS_LOG(WARN,"merge_join_operator result error [cell_size:%ld, org_row_width:%ld]",
            merge_join_operator_.get_cell_size(), org_row_width);
          err = OB_ERR_UNEXPECTED;
        }
        else
        {
          err = merge_join_operator_.unget_cell();
          if (OB_SUCCESS != err)
          {
            TBSYS_LOG(WARN,"fail to unget one cell [err:%d]", err);
          }
        }
      }
    }
  }/// end while
  return err;
}
