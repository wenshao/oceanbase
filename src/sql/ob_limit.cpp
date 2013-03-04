/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_limit.cpp
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#include "ob_limit.h"
#include "common/utility.h"
using namespace oceanbase::sql;
using namespace oceanbase::common;

ObLimit::ObLimit()
  :limit_(-1), offset_(0), input_count_(0), output_count_(0)
{
}

ObLimit::~ObLimit()
{
}

void ObLimit::reset()
{
  limit_ = -1;
  offset_ = input_count_ = output_count_ = 0;
  return;
}

int ObLimit::set_limit(const int64_t limit, const int64_t offset)
{
  int ret = OB_SUCCESS;
  if (0 > offset)
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "invalid arguments, limit=%ld offset=%ld", limit, offset);
  }
  else
  {
    limit_ = limit;
    offset_ = offset;
  }
  return ret;
}

int ObLimit::get_limit(int64_t &limit, int64_t &offset) const
{
  int ret = OB_SUCCESS;
  limit = limit_;
  offset = offset_;
  return ret;
}

int ObLimit::open()
{
  input_count_ = 0;
  output_count_ = 0;
  return ObSingleChildPhyOperator::open();
}

int ObLimit::close()
{
  return ObSingleChildPhyOperator::close();
}

int ObLimit::get_row_desc(const common::ObRowDesc *&row_desc) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == child_op_ || 0 > offset_))
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(ERROR, "child_op_ is NULL or invalid offset=%ld", offset_);
  }
  else
  {
    ret = child_op_->get_row_desc(row_desc);
  }
  return ret;
}

int ObLimit::get_next_row(const common::ObRow *&row)
{
  int ret = OB_SUCCESS;
  const common::ObRow *input_row = NULL;
  if (OB_UNLIKELY(NULL == child_op_ || 0 > offset_))
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(ERROR, "child_op_ is NULL or invalid offset=%ld", offset_);
  }
  else
  {
    while (input_count_ < offset_)
    {
      if (OB_SUCCESS != (ret = child_op_->get_next_row(input_row)))
      {
        TBSYS_LOG(WARN, "child_op failed to get next row, err=%d", ret);
        break;
      }
      else
      {
        ++input_count_;
      }
    } // end while
    if (OB_SUCCESS == ret)
    {
      if (output_count_ < limit_ || 0 > limit_)
      {
        if (OB_SUCCESS != (ret = child_op_->get_next_row(input_row)))
        {
          if (OB_ITER_END != ret)
          {
            TBSYS_LOG(WARN, "child_op failed to get next row, err=%d, limit_=%ld, offset_=%ld, input_count_=%ld, output_count=%ld",
              ret, limit_, offset_, input_count_, output_count_);
          }
        }
        else
        {
          ++output_count_;
          row = input_row;
        }
      }
      else
      {
        ret = OB_ITER_END;
      }
    }
  }
  return ret;
}

int64_t ObLimit::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  databuff_printf(buf, buf_len, pos, "Limit(limit=%ld, offset=%ld)\n", limit_, offset_);
  if (NULL != child_op_)
  {
    int64_t pos2 = child_op_->to_string(buf+pos, buf_len-pos);
    pos += pos2;
  }
  return pos;
}


DEFINE_SERIALIZE(ObLimit)
{
  int ret = OB_SUCCESS;
  ObObj obj;
  if (OB_SUCCESS == ret)
  {
    obj.set_int(limit_);
    if (OB_SUCCESS != (ret = obj.serialize(buf, buf_len, pos)))
    {
      TBSYS_LOG(WARN, "fail to serialize obj. ret=%d", ret);
    }
  }
  if (OB_SUCCESS == ret)
  {
    obj.set_int(offset_);
    if (OB_SUCCESS != (ret = obj.serialize(buf, buf_len, pos)))
    {
      TBSYS_LOG(WARN, "fail to serialize obj. ret=%d", ret);
    }
  }
  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObLimit)
{
  int64_t size = 0;
  ObObj obj;
  obj.set_int(limit_);
  size += obj.get_serialize_size();
  obj.set_int(offset_);
  size += obj.get_serialize_size();
  return size;
}


DEFINE_DESERIALIZE(ObLimit)
{
  int ret = OB_SUCCESS;
  ObObj obj;
  reset();
  if (OB_SUCCESS == ret)
  {
    if (OB_SUCCESS != (ret = obj.deserialize(buf, data_len, pos)))
    {
      TBSYS_LOG(WARN, "fail to serialize obj. ret=%d", ret);
    }
    if (OB_SUCCESS != (ret = obj.get_int(limit_)))
    {
      TBSYS_LOG(WARN, "fail to get int value. ret=%d, limit_=%ld", ret, limit_);
    }
  }
  if (OB_SUCCESS == ret)
  {
    if (OB_SUCCESS != (ret = obj.deserialize(buf, data_len, pos)))
    {
      TBSYS_LOG(WARN, "fail to serialize obj. ret=%d", ret);
    }
    if (OB_SUCCESS != (ret = obj.get_int(offset_)))
    {
      TBSYS_LOG(WARN, "fail to get int value. ret=%d, offset_=%ld", ret, offset_);
    }
  }
  return ret;
}

void ObLimit::assign(const ObLimit &other)
{
  limit_ = other.limit_;
  offset_ = other.offset_;
}
