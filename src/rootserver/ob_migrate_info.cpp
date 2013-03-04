/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 * 
 * Version: $Id$
 *
 * ob_migrate_info.cpp
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#include "ob_migrate_info.h"
#include "common/ob_define.h"
using namespace oceanbase::rootserver;
using namespace oceanbase::common;

ObMigrateInfo::ObMigrateInfo()
  :cs_idx_(OB_INVALID_INDEX),
   stat_(STAT_INIT), keep_src_(0), 
   reserve_(0), next_(NULL)
{
}

ObMigrateInfo::~ObMigrateInfo()
{
  cs_idx_ = OB_INVALID_INDEX;
  stat_ = STAT_INIT;
  next_ = NULL;
}

void ObMigrateInfo::reset(common::CharArena &allocator)
{
  cs_idx_ = OB_INVALID_INDEX;
  stat_ = STAT_INIT;
  next_ = NULL;
  if (NULL != range_.start_key_.ptr())
  {
    allocator.free(range_.start_key_.ptr());
    range_.start_key_.assign_buffer(NULL, 0);
  }
  if (NULL != range_.end_key_.ptr())
  {
    allocator.free(range_.end_key_.ptr());
    range_.end_key_.assign_buffer(NULL, 0);
  }
}

bool ObMigrateInfo::is_init() const
{
  return NULL == next_
    && OB_INVALID_INDEX == cs_idx_
    && STAT_INIT == stat_
    && NULL == range_.start_key_.ptr()
    && NULL == range_.end_key_.ptr();
}

const char* ObMigrateInfo::get_stat_str() const
{
  const char* ret = "ERR";
  switch(stat_)
  {
    case STAT_INIT:
      ret = "INIT";
      break;
    case STAT_SENT:
      ret = "SENT";
      break;
    case STAT_DONE:
      ret = "DONE";
      break;
    default:
      break;
  }
  return ret;
}

////////////////////////////////////////////////////////////////
ObMigrateInfos::ObMigrateInfos()
  :infos_(NULL), size_(0), alloc_idx_(0)
{
}

ObMigrateInfos::~ObMigrateInfos()
{
  if (NULL != infos_)
  {
    for (int i = 0; i < size_; ++i)
    {
      infos_[i].reset(key_allocator_);
    }
    delete [] infos_;
    infos_ = NULL;
  }
  size_ = 0;
  alloc_idx_ = 0;
}

void ObMigrateInfos::reset(int32_t cs_num, int32_t max_migrate_out_per_cs)
{
  if (NULL == infos_)
  {
    size_ = cs_num * max_migrate_out_per_cs;
    infos_ = new(std::nothrow) ObMigrateInfo[size_];
    if (NULL == infos_)
    {
      TBSYS_LOG(ERROR, "no memory");
    }
  }
  else if (size_ < cs_num * max_migrate_out_per_cs)
  {
    for (int i = 0; i < size_; ++i)
    {
      infos_[i].reset(key_allocator_);
    }
    delete [] infos_;
    size_ = cs_num * max_migrate_out_per_cs;
    infos_ = new(std::nothrow) ObMigrateInfo[size_];
    if (NULL == infos_)
    {
      TBSYS_LOG(ERROR, "no memory");
    }
  }
  else
  {
    for (int i = 0; i < size_; ++i)
    {
      infos_[i].reset(key_allocator_);
    }
  }
  alloc_idx_ = 0;
}

ObMigrateInfo *ObMigrateInfos::alloc_info()
{
  ObMigrateInfo *ret = NULL;
  if (alloc_idx_ < size_)
  {
    ret = infos_ + alloc_idx_;
    alloc_idx_++;
  }
  return ret;
}

CharArena& ObMigrateInfos::get_allocator()
{
  return key_allocator_;
}

bool ObMigrateInfos::is_full() const
{
  return alloc_idx_ >= size_;
}

////////////////////////////////////////////////////////////////    
ObCsMigrateTo::ObCsMigrateTo()
  :info_head_(NULL), info_tail_(NULL), count_(0), reserve_(0)
{
}

ObCsMigrateTo::~ObCsMigrateTo()
{
  reset();
}

void ObCsMigrateTo::reset()
{
  info_head_ = NULL;
  info_tail_ = NULL;
  count_ = 0;
}

ObMigrateInfo *ObCsMigrateTo::head()
{
  return info_head_;
}

ObMigrateInfo *ObCsMigrateTo::tail()
{
  return info_tail_;
}

const ObMigrateInfo *ObCsMigrateTo::head() const
{
  return info_head_;
}

const ObMigrateInfo *ObCsMigrateTo::tail() const
{
  return info_tail_;
}

int32_t ObCsMigrateTo::count() const
{
  return count_;
}

int ObCsMigrateTo::add_migrate_info(const common::ObRange &range, int32_t dest_cs_idx, int8_t keep_src, ObMigrateInfos &infos)
{
  int ret = OB_SUCCESS;
  ObMigrateInfo* minfo = NULL;
  if (OB_INVALID_INDEX == dest_cs_idx)
  {
    TBSYS_LOG(ERROR, "BUG invalid cs index");
    ret = OB_INVALID_ARGUMENT;
  }
  else if (NULL == (minfo = infos.alloc_info()))
  {
    ret = OB_ARRAY_OUT_OF_RANGE;
  }
  else if (!minfo->is_init())
  {
    TBSYS_LOG(ERROR, "BUG element is not init");
    ret = OB_ERROR;
  }
  else if (OB_SUCCESS != (ret = clone_string(infos.get_allocator(), range.start_key_, minfo->range_.start_key_)))
  {
    TBSYS_LOG(ERROR, "clone start key error");
  }
  else if (OB_SUCCESS != (ret = clone_string(infos.get_allocator(), range.end_key_, minfo->range_.end_key_)))
  {
    if (NULL != minfo->range_.start_key_.ptr())
    {
      infos.get_allocator().free(minfo->range_.start_key_.ptr());
      minfo->range_.start_key_.assign(NULL, 0);
    }
  }
  else
  {
    minfo->cs_idx_ = dest_cs_idx;
    minfo->range_.table_id_ = range.table_id_;
    minfo->range_.border_flag_ = range.border_flag_;
    minfo->keep_src_ = keep_src;
    count_++;
    if (NULL == info_head_)
    {
      info_head_ = info_tail_ = minfo;
    }
    else
    {
      info_tail_->next_ = minfo;
      info_tail_ = minfo;
    }
  }
  return ret;
}

int ObCsMigrateTo::clone_string(common::CharArena &allocator, const common::ObString &in, common::ObString &out)
{
  int ret = OB_SUCCESS;
  ObString::obstr_size_t len = in.length();
  if (0 < len)
  {
    char *key = allocator.alloc(len);
    if (NULL == key)
    {
      TBSYS_LOG(ERROR, "no memory");
      ret = OB_ALLOCATE_MEMORY_FAILED;
    }
    else
    {
      memcpy(key, in.ptr(), len);
      out.assign(key, len);
    }
  }
  return ret;
}

int ObCsMigrateTo::set_migrate_done(const ObRange &range, int32_t dest_cs_idx)
{
  int ret = OB_ENTRY_NOT_EXIST;

  ObMigrateInfo *minfo = info_head_;
  for (int i = 0; i < count_ && NULL != minfo; ++i, minfo = minfo->next_)
  {
    if (ObMigrateInfo::STAT_SENT == minfo->stat_
        && minfo->cs_idx_ == dest_cs_idx
        && minfo->range_.equal(range))
    {
      minfo->stat_ = ObMigrateInfo::STAT_DONE;
      ret = OB_SUCCESS;
      break;
    }
  }
  return ret;
}

int ObCsMigrateTo::add_migrate_info(const common::ObRange &range, int32_t dest_cs_idx, ObMigrateInfos &infos)
{
  return add_migrate_info(range, dest_cs_idx, 0, infos);
}

int ObCsMigrateTo::add_copy_info(const common::ObRange &range, int32_t dest_cs_idx, ObMigrateInfos &infos)
{
  return add_migrate_info(range, dest_cs_idx, 1, infos);
}
