/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_row_store.cpp
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#include "ob_row_store.h"
#include "ob_row_util.h"
#include "common/ob_compact_cell_writer.h"
using namespace oceanbase::common;

////////////////////////////////////////////////////////////////
struct ObRowStore::BlockInfo
{
  BlockInfo *next_block_;
  /**
   * cur_data_pos_ must be set when BlockInfo deserialized
   */
  int64_t curr_data_pos_;
  char data_[0];

  BlockInfo()
    :next_block_(NULL), curr_data_pos_(0)
  {
  }
  inline int64_t get_remain_size() const
  {
    return ObRowStore::BLOCK_SIZE - curr_data_pos_ - sizeof(BlockInfo);
  }
  inline int64_t get_remain_size_for_read(int64_t pos) const
  {
    TBSYS_LOG(DEBUG, "cur=%ld, pos=%ld, remain=%ld", curr_data_pos_, pos, curr_data_pos_ - pos);
    return curr_data_pos_ - pos;
  }
  inline char* get_buffer()
  {
    return data_ + curr_data_pos_;
  }
  inline const char* get_buffer_head() const
  {
    return data_;
  }

  inline void advance(const int64_t length)
  {
    curr_data_pos_ += length;
  }
};

////////////////////////////////////////////////////////////////
ObRowStore::ObRowStore()
  :block_list_head_(NULL), block_list_tail_(NULL),
  block_count_(0), cur_size_counter_(0), got_first_next_(false),
  cur_iter_pos_(0), cur_iter_block_(NULL)
{
}

ObRowStore::~ObRowStore()
{
  clear();
}

int ObRowStore::add_reserved_column(uint64_t tid, uint64_t cid)
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = reserved_columns_.push_back(std::make_pair(tid, cid))))
  {
    TBSYS_LOG(WARN, "failed to push into array, err=%d", ret);
  }
  return ret;
}

void ObRowStore::clear()
{
  while(NULL != block_list_tail_)
  {
    BlockInfo *tmp = block_list_tail_->next_block_;
    ob_free(block_list_tail_);
    block_list_tail_ = tmp;
  }
  block_list_head_ = NULL;
  block_count_ = 0;
  cur_size_counter_ = 0;
  got_first_next_ = false;
  cur_iter_pos_ = 0;
  cur_iter_block_ = NULL;
}

int ObRowStore::new_block()
{
  int ret = OB_SUCCESS;
  BlockInfo *block = static_cast<BlockInfo*>(ob_malloc(BLOCK_SIZE));
  if (NULL == block)
  {
    TBSYS_LOG(ERROR, "no memory");
    ret = OB_ALLOCATE_MEMORY_FAILED;
  }
  else
  {
    block->next_block_ = NULL;
    block->curr_data_pos_ = 0;
    if (NULL == block_list_tail_)
    {
      block_list_tail_ = block;  // this is the first block allocated
    }
    if (NULL == block_list_head_)
    {
      block_list_head_ = block;
    }
    else
    {
      block_list_head_->next_block_ = block;
      block_list_head_ = block;
    }
    ++block_count_;
  }
  return ret;
}

int ObRowStore::append_row(const ObString *rowkey, const ObRow &row, BlockInfo &block, StoredRow &stored_row)
{
  int ret = OB_SUCCESS;

  const int64_t reserved_columns_count = reserved_columns_.count();
  ObCompactCellWriter cell_writer;

  if(NULL == rowkey)
  {
    cell_writer.init(block.get_buffer(), block.get_remain_size(), SPARSE);
  }
  else
  {
    cell_writer.init(block.get_buffer(), block.get_remain_size(), DENSE_SPARSE);
  }

  const ObObj *cell = NULL;
  uint64_t table_id = OB_INVALID_ID;
  uint64_t column_id = OB_INVALID_ID;
  ObObj cell_clone;

  const ObUpsRow *ups_row = dynamic_cast<const ObUpsRow *>(&row);
  if(OB_SUCCESS == ret && NULL != rowkey)
  {
    ObObj rowkey_obj;
    rowkey_obj.set_varchar(*rowkey);
    if(OB_SUCCESS != (ret = cell_writer.append(rowkey_obj)))
    {
      TBSYS_LOG(WARN, "append rowkey fail:ret[%d]", ret);
    }
    else if(OB_SUCCESS != (ret = cell_writer.rowkey_finish()))
    {
      TBSYS_LOG(WARN, "add rowkey finish flag fail:ret[%d]", ret);
    }
  }

  if(NULL != ups_row)
  {
    if( ups_row->is_delete_row() )
    {
      if(OB_SUCCESS != (ret = cell_writer.row_delete()))
      {
        TBSYS_LOG(WARN, "append row delete flag fail:ret[%d]", ret);
      }
    }
  }

  for (int64_t i = 0; OB_SUCCESS == ret && i < row.get_column_num(); ++i)
  {
    if (OB_SUCCESS != (ret = row.raw_get_cell(i, cell, table_id, column_id)))
    {
      TBSYS_LOG(WARN, "failed to get cell, err=%d", ret);
      break;
    }
    else if (OB_SUCCESS != (ret = cell_writer.append(column_id, *cell, &cell_clone)))
    {
      if (OB_BUF_NOT_ENOUGH != ret)
      {
        TBSYS_LOG(WARN, "failed to append cell, err=%d", ret);
      }
      break;
    }
    else
    {
      // whether reserve this cell
      for (int64_t j = 0; j < reserved_columns_count; ++j)
      {
        const std::pair<uint64_t,uint64_t> &tid_cid = reserved_columns_.at(j);
        if (table_id == tid_cid.first && column_id == tid_cid.second)
        {
          stored_row.reserved_cells_[j] = cell_clone;
          break;
        }
      } // end for j
    }
  } // end for i
  if (OB_SUCCESS == ret)
  {
    if (OB_SUCCESS != (ret = cell_writer.row_finish()))
    {
      if (OB_BUF_NOT_ENOUGH != ret)
      {
        TBSYS_LOG(WARN, "failed to append cell, err=%d", ret);
      }
    }
    else
    {
      stored_row.compact_row_size_ = static_cast<int32_t>(cell_writer.size());
      block.advance(cell_writer.size());
      cur_size_counter_ += cell_writer.size();
    }
  }
  return ret;
}


int ObRowStore::add_ups_row(const ObUpsRow &row, const StoredRow *&stored_row)
{
  return add_row(row, stored_row);
}

int ObRowStore::add_ups_row(const ObUpsRow &row, int64_t &cur_size_counter)
{
  return add_row(row, cur_size_counter);
}

int ObRowStore::add_row(const ObRow &row, const StoredRow *&stored_row)
{
  int64_t cur_size_counter = 0; // value ignored
  return add_row(NULL, row, stored_row, cur_size_counter);
}

int ObRowStore::add_row(const ObRow &row, int64_t &cur_size_counter)
{
  const StoredRow *stored_row = NULL; // value ignored
  return add_row(NULL, row, stored_row, cur_size_counter);
}


int ObRowStore::add_row(const ObString *rowkey, const ObRow &row, const StoredRow *&stored_row, int64_t &cur_size_counter)
{
  int ret = OB_SUCCESS;
  stored_row = NULL;
  const int64_t reserved_columns_count = reserved_columns_.count();
  if (NULL == block_list_head_
      || block_list_head_->get_remain_size() <= get_compact_row_min_size(row.get_column_num())
      + get_reserved_cells_size(reserved_columns_count))
  {
    ret = new_block();
  }

  int64_t retry = 0;
  while(OB_SUCCESS == ret && retry < 2)
  {
    // append OrderByCells
    OB_ASSERT(block_list_head_);
    StoredRow *reserved_cells = reinterpret_cast<StoredRow*>(block_list_head_->get_buffer());
    reserved_cells->reserved_cells_count_ = static_cast<int32_t>(reserved_columns_count);
    block_list_head_->advance(get_reserved_cells_size(reserved_columns_count));
    if (OB_SUCCESS != (ret = append_row(rowkey, row, *block_list_head_, *reserved_cells)))
    {
      if (OB_BUF_NOT_ENOUGH == ret)
      {
        // buffer not enough
        block_list_head_->advance( -get_reserved_cells_size(reserved_columns_count) );
        TBSYS_LOG(DEBUG, "block buffer not enough, buff=%p remain_size=%ld block_count=%ld",
                  block_list_head_, block_list_head_->get_remain_size(), block_count_);
        ret = OB_SUCCESS;
        ++retry;
        ret = new_block();
      }
      else
      {
        TBSYS_LOG(WARN, "failed to append row, err=%d", ret);
      }
    }
    else
    {
      cur_size_counter_ += get_reserved_cells_size(reserved_columns_count);
      stored_row = reserved_cells;
      cur_size_counter = cur_size_counter_;
      break;                  // done
    }
  } // end while
  if (2 <= retry)
  {
    ret = OB_ERR_UNEXPECTED;
    TBSYS_LOG(ERROR, "unexpected branch");
  }
  return ret;
}

int ObRowStore::next_iter_pos(BlockInfo *&iter_block, int64_t &iter_pos)
{
  int ret = OB_SUCCESS;
  if (NULL == iter_block)
  {
    ret = OB_ITER_END;
  }
  else
  {
    while (0 >= iter_block->get_remain_size_for_read(iter_pos))
    {
      iter_block = iter_block->next_block_;
      iter_pos = 0;
      if (NULL == iter_block)
      {
        ret = OB_ITER_END;
        break;
      }
    }
  }
  return ret;
}

int ObRowStore::get_next_ups_row(ObString *rowkey, ObUpsRow &row, common::ObString *compact_row /* = NULL */)
{
  int ret = OB_SUCCESS;
  if(OB_SUCCESS != (ret = get_next_row(rowkey, row, compact_row)))
  {
    TBSYS_LOG(WARN, "get next ups row fail:ret[%d]", ret);
  }
  return ret;
}

int ObRowStore::get_next_ups_row(ObUpsRow &row, common::ObString *compact_row /* = NULL */)
{
  return get_next_row(NULL, row, compact_row);
}

void ObRowStore::reset_iterator()
{
  cur_iter_block_ = block_list_tail_;
  cur_iter_pos_ = 0;
  got_first_next_ = false;
}

int ObRowStore::get_next_row(ObRow &row, common::ObString *compact_row /* = NULL */)
{
  return get_next_row(NULL, row, compact_row);
}

int ObRowStore::get_next_row(ObString *rowkey, ObRow &row, common::ObString *compact_row)
{
  int ret = OB_SUCCESS;
  const StoredRow *stored_row = NULL;

  if (!got_first_next_)
  {
    cur_iter_block_ = block_list_tail_;
    cur_iter_pos_ = 0;
    got_first_next_ = true;
  }

  if (OB_ITER_END == (ret = next_iter_pos(cur_iter_block_, cur_iter_pos_)))
  {
    TBSYS_LOG(DEBUG, "iter end.block=%p, pos=%ld", cur_iter_block_, cur_iter_pos_);
  }
  else if (OB_SUCCESS == ret)
  {
    const char *buffer = cur_iter_block_->get_buffer_head() + cur_iter_pos_;
    stored_row = reinterpret_cast<const StoredRow *>(buffer);
    cur_iter_pos_ += (get_reserved_cells_size(stored_row->reserved_cells_count_) + stored_row->compact_row_size_);
    TBSYS_LOG(DEBUG, "stored_row->reserved_cells_count_=%d, stored_row->compact_row_size_=%d, sizeof(ObObj)=%lu, next_pos_=%ld",
        stored_row->reserved_cells_count_, stored_row->compact_row_size_, sizeof(ObObj), cur_iter_pos_);

    if (OB_SUCCESS == ret && NULL != stored_row)
    {
      ObUpsRow *ups_row = dynamic_cast<ObUpsRow*>(&row);
      if (NULL != ups_row)
      {
        const ObRowDesc *row_desc = row.get_row_desc();
        uint64_t table_id = OB_INVALID_ID;
        uint64_t column_id = OB_INVALID_ID;

        if(NULL == row_desc)
        {
          ret = OB_INVALID_ARGUMENT;
          TBSYS_LOG(WARN, "row should set row desc first");
        }
        else if(OB_SUCCESS != (ret = row_desc->get_tid_cid(0, table_id, column_id)))
        {
          TBSYS_LOG(WARN, "get tid cid fail:ret[%d]", ret);
        }
        else if (OB_SUCCESS != (ret = ObUpsRowUtil::convert(table_id, stored_row->get_compact_row(), *ups_row, rowkey)))
        {
          TBSYS_LOG(WARN, "fail to convert compact row to ObUpsRow. ret=%d", ret);
        }
      }
      else
      {
        if(OB_SUCCESS != (ret = ObRowUtil::convert(stored_row->get_compact_row(), row, rowkey)))
        {
          TBSYS_LOG(WARN, "fail to convert compact row to ObRow:ret[%d]", ret);
        }
      }
      if (OB_SUCCESS == ret && NULL != compact_row)
      {
        *compact_row = stored_row->get_compact_row();
      }
    }
    else
    {
      TBSYS_LOG(WARN, "fail to get next row. stored_row=%p, ret=%d", stored_row, ret);
    }
  }
  return ret;
}


int64_t ObRowStore::get_used_mem_size() const
{
  return block_count_ * BLOCK_SIZE;
}


DEFINE_SERIALIZE(ObRowStore)
{
  int ret = OB_SUCCESS;
  ObObj obj;
  BlockInfo *block = block_list_tail_;
  while(NULL != block)
  {
    // serialize block size
    obj.set_int(block->curr_data_pos_);
    if (OB_SUCCESS != (ret = obj.serialize(buf, buf_len, pos)))
    {
      TBSYS_LOG(WARN, "fail to serialize block size. ret=%d", ret);
      break;
    }
    // serialize block data
    else
    {
      if (buf_len - pos < block->curr_data_pos_)
      {
        TBSYS_LOG(WARN, "buffer not enough.");
        ret = OB_BUF_NOT_ENOUGH;
      }
      else
      {
        memcpy(buf + pos, block->get_buffer_head(), block->curr_data_pos_);
        pos += block->curr_data_pos_;
      }
    }
    // serialize next block
    block = block->next_block_;
    TBSYS_LOG(DEBUG, "serialize next block");
  }
  return ret;
}


DEFINE_DESERIALIZE(ObRowStore)
{
  int ret = OB_SUCCESS;
  int64_t old_pos = pos;
  int64_t block_size = 0;
  ObObj obj;

  clear();

  while((OB_SUCCESS == (ret = obj.deserialize(buf, data_len, pos)))
              && (ObExtendType != obj.get_type()))
  {
    // get block data size
    if (ObIntType != obj.get_type())
    {
         TBSYS_LOG(WARN, "ObObj deserialize type error, ret=%d buf=%p data_len=%ld pos=%ld",
                       ret, buf, data_len, pos);
         ret = OB_ERROR;
         break;
    }
    if (OB_SUCCESS != (ret = obj.get_int(block_size)))
    {
      TBSYS_LOG(WARN, "ObObj deserialize error, ret=%d buf=%p data_len=%ld pos=%ld",
          ret, buf, data_len, pos);
      break;
    }
    // copy data to store
    if (OB_SUCCESS != (ret = this->new_block()))
    {
      TBSYS_LOG(WARN, "fail to allocate new block. ret=%d", ret);
      break;
    }
    if (NULL != block_list_head_ && block_size <= block_list_head_->get_remain_size())
    {
      TBSYS_LOG(DEBUG, "copy data from scanner to row_store. buf=%p, pos=%ld, block_size=%ld",
          buf, pos, block_size);
      memcpy(block_list_head_->get_buffer(), buf + pos, block_size);
      block_list_head_->advance(block_size);
      pos += block_size;
      cur_size_counter_ += block_size;
    }
    else
    {
      TBSYS_LOG(WARN, "fail to deserialize scanner data into new block. block_list_head_=%p, block_size=%ld",
          block_list_head_, block_size);
      ret = OB_ERROR;
      break;
    }
    // update pos
    old_pos = pos;
  }
  pos = old_pos;

  return ret;
}


int ObRowStore::add_row(const ObString &rowkey, const ObRow &row, const StoredRow *&stored_row)
{
  int ret = OB_SUCCESS;
  int64_t cur_size_counter = 0;
  if(OB_SUCCESS != (ret = add_row(&rowkey, row, stored_row, cur_size_counter)))
  {
    TBSYS_LOG(WARN, "add row fail:ret[%d]", ret);
  }
  return ret;
}

int ObRowStore::add_ups_row(const ObString &rowkey, const ObUpsRow &row, const StoredRow *&stored_row)
{
  int ret = OB_SUCCESS;
  if(OB_SUCCESS != (ret = add_row(rowkey, row, stored_row)))
  {
    TBSYS_LOG(WARN, "add ups row fail:ret[%d]", ret);
  }
  return ret;
}
