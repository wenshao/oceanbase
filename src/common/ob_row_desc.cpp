/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_row_desc.cpp
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#include "ob_row_desc.h"
#include "common/murmur_hash.h"
#include "common/ob_atomic.h"
#include <tbsys.h>

using namespace oceanbase::common;

volatile uint64_t ObRowDesc::HASH_COLLISIONS_COUNT = 0;

ObRowDesc::ObRowDesc()
  :cells_desc_count_(0), overflow_backets_count_(0)
{
  memset(cells_desc_, 0, sizeof(cells_desc_));
  memset(hash_backets_, 0, sizeof(hash_backets_));
  memset(overflow_backets_, 0, sizeof(overflow_backets_));
}

ObRowDesc::~ObRowDesc()
{
}

int64_t ObRowDesc::get_idx(const uint64_t table_id, const uint64_t column_id) const
{
  int64_t ret = OB_INVALID_INDEX;
  if (0 != table_id && 0 != column_id)
  {
    const DescIndex *desc_index = NULL;
    if (OB_SUCCESS == hash_find(table_id, column_id, desc_index))
    {
      if (desc_index->idx_ < cells_desc_count_)
      {
        ret = desc_index->idx_;
      }
    }
  }
  return ret;
}

int ObRowDesc::get_tid_cid(const int64_t idx, uint64_t &table_id, uint64_t &column_id) const
{
  int ret = OB_SUCCESS;
  if (idx < 0
      || idx >= cells_desc_count_)
  {
    ret = OB_INVALID_ARGUMENT;
  }
  else
  {
    table_id = cells_desc_[idx].table_id_;
    column_id = cells_desc_[idx].column_id_;
  }
  return ret;
}

int ObRowDesc::add_column_desc(const uint64_t table_id, const uint64_t column_id)
{
  int ret = OB_SUCCESS;
  if (cells_desc_count_ >= MAX_COLUMNS_COUNT)
  {
    TBSYS_LOG(ERROR, "too many column for a row, tid=%lu cid=%lu",
              table_id, column_id);
    ret = OB_BUF_NOT_ENOUGH;
  }
  else
  {
    cells_desc_[cells_desc_count_].table_id_ = table_id;
    cells_desc_[cells_desc_count_].column_id_ = column_id;
    if (OB_SUCCESS != (ret = hash_insert(table_id, column_id, cells_desc_count_)))
    {
      TBSYS_LOG(WARN, "failed to insert column desc, err=%d", ret);
    }
    else
    {
      ++cells_desc_count_;
    }
  }
  return ret;
}


void ObRowDesc::reset()
{
  cells_desc_count_ = 0;
  overflow_backets_count_ = 0;
  memset(cells_desc_, 0, sizeof(cells_desc_));
  memset(hash_backets_, 0, sizeof(hash_backets_));
  memset(overflow_backets_, 0, sizeof(overflow_backets_));
}

inline bool ObRowDesc::Desc::is_invalid() const
{
  return (0 == table_id_ || 0 == column_id_);
}

inline bool ObRowDesc::Desc::operator== (const Desc &other) const
{
  return table_id_ == other.table_id_ && column_id_ == other.column_id_;
}

int ObRowDesc::hash_find(const uint64_t table_id, const uint64_t column_id, const DescIndex *&desc_idx) const
{
  int ret = OB_SUCCESS;
  Desc desc;
  desc.table_id_ = table_id;
  desc.column_id_ = column_id;
  uint32_t pos = murmurhash2(&desc, sizeof(desc), 0);
  pos %= HASH_BACKETS_COUNT;
  if (desc == hash_backets_[pos].desc_)
  {
    desc_idx = &hash_backets_[pos];
  }
  else if (hash_backets_[pos].desc_.is_invalid())
  {
    ret = OB_ENTRY_NOT_EXIST;
  }
  else
  {
    ret = slow_find(table_id, column_id, desc_idx);
  }
  return ret;
}

int ObRowDesc::hash_insert(const uint64_t table_id, const uint64_t column_id, const int64_t index)
{
  int ret = OB_SUCCESS;
  Desc desc;
  desc.table_id_ = table_id;
  desc.column_id_ = column_id;
  uint32_t pos = murmurhash2(&desc, sizeof(desc), 0);
  pos %= HASH_BACKETS_COUNT;
  if (hash_backets_[pos].desc_.is_invalid())
  {
    hash_backets_[pos].desc_ = desc;
    hash_backets_[pos].idx_ = index;
  }
  else if (desc == hash_backets_[pos].desc_)
  {
    // already exists
    if (index == hash_backets_[pos].idx_)
    {
      ret = OB_ENTRY_EXIST;
    }
    else
    {
      ret = OB_ERR_UNEXPECTED;
      TBSYS_LOG(ERROR, "duplicated cell desc, tid=%lu cid=%lu old_idx=%ld new_idx=%ld",
                table_id, column_id, hash_backets_[pos].idx_, index);
    }
  }
  else
  {
    // hash collision
    atomic_inc(&HASH_COLLISIONS_COUNT);
    ret = slow_insert(table_id, column_id, index);
  }
  return ret;
}

int ObRowDesc::slow_find(const uint64_t table_id, const uint64_t column_id, const DescIndex *&desc_idx) const
{
  int ret = OB_ENTRY_NOT_EXIST;
  for (int64_t i = 0; i < overflow_backets_count_; ++i)
  {
    if (overflow_backets_[i].desc_.table_id_ == table_id
        && overflow_backets_[i].desc_.column_id_ == column_id)
    {
      desc_idx = &overflow_backets_[i];
      ret = OB_SUCCESS;
      break;
    }
  }
  return ret;
}

int ObRowDesc::slow_insert(const uint64_t table_id, const uint64_t column_id, const int64_t index)
{
  int ret = OB_SUCCESS;
  if (overflow_backets_count_ >= MAX_COLUMNS_COUNT)
  {
    ret = OB_BUF_NOT_ENOUGH;
  }
  else
  {
    overflow_backets_[overflow_backets_count_].desc_.table_id_ = table_id;
    overflow_backets_[overflow_backets_count_].desc_.column_id_ = column_id;
    overflow_backets_[overflow_backets_count_].idx_ = index;
    ++overflow_backets_count_;
  }
  return ret;
}
