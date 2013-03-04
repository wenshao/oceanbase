/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_tablet_fuse.cpp
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#include "ob_tablet_fuse.h"
#include "common/ob_row_fuse.h"
using namespace oceanbase::sql;

ObTabletFuse::ObTabletFuse()
  :sstable_scan_(NULL),
  incremental_scan_(NULL),
  last_sstable_row_(NULL),
  last_incr_row_(NULL),
  sstable_rowkey_(NULL),
  incremental_rowkey_(NULL)
{
}

ObTabletFuse::~ObTabletFuse()
{
}

bool ObTabletFuse::check_inner_stat()
{
  bool ret = true;
  if(NULL == sstable_scan_ || NULL == incremental_scan_)
  {
    ret = false;
    TBSYS_LOG(WARN, "check inner stat fail:sstable_scan_[%p], incremental_scan_[%p]", sstable_scan_, incremental_scan_);
  }
  return ret;
}

int ObTabletFuse::set_child(int32_t child_idx, ObPhyOperator &child_operator)
{
  int ret = OB_NOT_IMPLEMENT;
  UNUSED(child_idx);
  UNUSED(child_operator);
  TBSYS_LOG(ERROR, "not implement");
  return ret;
}

int ObTabletFuse::set_sstable_scan(ObRowkeyPhyOperator *sstable_scan)
{
  int ret = OB_SUCCESS;
  if(NULL == sstable_scan)
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "argument sstable_scan is null");
  }
  else
  {
    sstable_scan_ = sstable_scan;
  }
  return ret;
}

int ObTabletFuse::set_incremental_scan(ObRowkeyPhyOperator *incremental_scan)
{
  int ret = OB_SUCCESS;
  if(NULL == incremental_scan)
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "argument incremental_scan is null");
  }
  else
  {
    incremental_scan_ = incremental_scan;
  }
  return ret;
}

int ObTabletFuse::open()
{
  int ret = OB_SUCCESS;

  if(!check_inner_stat())
  {
    ret = OB_ERROR;
    TBSYS_LOG(WARN, "check inner stat fail");
  }
  else if(OB_SUCCESS != (ret = sstable_scan_->open()))
  {
    TBSYS_LOG(WARN, "open sstable scan fail:ret[%d]", ret);
  }
  else if(OB_SUCCESS != (ret = incremental_scan_->open()))
  {
    TBSYS_LOG(WARN, "open incremental scan fail:ret[%d]", ret);
  }
  else
  {
    last_incr_row_ = NULL;
    last_sstable_row_ = NULL;
  }

  return ret;
}

int ObTabletFuse::close()
{
  int ret = OB_SUCCESS;

  if(NULL != sstable_scan_)
  {
    if(OB_SUCCESS != (ret = sstable_scan_->close()))
    {
      TBSYS_LOG(WARN, "close sstable scan fail:ret[%d]", ret);
    }
  }

  if(NULL != incremental_scan_)
  {
    if(OB_SUCCESS != (ret = incremental_scan_->close()))
    {
      TBSYS_LOG(WARN, "close incremental scan fail:ret[%d]", ret);
    }
  }

  return ret;
}

// 分别从sstable_scan_和incremental_scan_读取数据，按照rowkey归并排序输出
int ObTabletFuse::get_next_row(const ObRow *&row)
{
  int ret = OB_SUCCESS;
  
  if(!check_inner_stat())
  {
    ret = OB_ERROR;
    TBSYS_LOG(WARN, "check inner stat fail");
  }

  if(OB_SUCCESS == ret)
  {
    if (NULL == last_incr_row_)
    {
      const ObRow *tmp_last_incr_row = NULL;
      ret = incremental_scan_->get_next_row(incremental_rowkey_, tmp_last_incr_row);
      if(OB_SUCCESS == ret)
      {
        last_incr_row_ = dynamic_cast<const ObUpsRow *>(tmp_last_incr_row);
        if(NULL == last_incr_row_)
        {
          ret = OB_ERROR;
          TBSYS_LOG(WARN, "incremntal scan should return ObUpsRow");
        }
      }
      else if(OB_ITER_END == ret)
      {
        last_incr_row_ = NULL;
        ret = OB_SUCCESS;
      }
      else
      {
        TBSYS_LOG(WARN, "incremental scan get next row fail:ret[%d]", ret);
      }
    }

    if(OB_SUCCESS == ret && NULL == last_sstable_row_)
    {
      ret = sstable_scan_->get_next_row(sstable_rowkey_, last_sstable_row_);
      if(OB_ITER_END == ret)
      {
        last_sstable_row_ = NULL;
        ret = OB_SUCCESS;
      }
      else if(OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "sstable scan get next row fail:ret[%d]", ret);
      }
    }

    if(OB_SUCCESS == ret)
    {
      if(NULL != last_sstable_row_)
      {
        if(NULL != last_incr_row_)
        {
          int cmp_result = compare_rowkey(*incremental_rowkey_, *sstable_rowkey_);
          if (cmp_result < 0)
          {
            if(OB_SUCCESS != (ret = ObRowFuse::assign(*last_incr_row_, curr_row_)))
            {
              TBSYS_LOG(WARN, "assign last incr row to curr_row fail:ret[%d]", ret);
            }
            else
            {
              row = &curr_row_;
              last_incr_row_ = NULL;
            }
          }
          else if (cmp_result == 0)
          {
            if(OB_SUCCESS != (ret = ObRowFuse::fuse_row(last_incr_row_, last_sstable_row_, &curr_row_)))
            {
              TBSYS_LOG(WARN, "fuse row fail:ret[%d]", ret);
            }
            else
            {
              row = &curr_row_;
              last_incr_row_ = NULL;
              last_sstable_row_ = NULL;
            }
          }
          else
          {
            row = last_sstable_row_;
            last_sstable_row_ = NULL;
          }
        }
        else
        {
          row = last_sstable_row_;
          last_sstable_row_ = NULL;
        }
     }
     else
     {
       if(NULL != last_incr_row_)
       {
          if(OB_SUCCESS != (ret = ObRowFuse::assign(*last_incr_row_, curr_row_)))
          {
            TBSYS_LOG(WARN, "assign last incr row to curr_row fail:ret[%d]", ret);
          }
          else
          {
            row = &curr_row_;
            last_incr_row_ = NULL;
          }       
       }
       else
       {
         ret = OB_ITER_END;
       }
     }
   }
  }
  return ret;
}

int ObTabletFuse::compare_rowkey(const ObString &rowkey1, const ObString &rowkey2)
{
  return rowkey1.compare(rowkey2);
}

int64_t ObTabletFuse::to_string(char* buf, const int64_t buf_len) const
{
  UNUSED(buf);
  UNUSED(buf_len);
  return 0;
}



