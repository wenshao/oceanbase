/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_ups_multi_get.cpp
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@taobao.com>
 *
 */

#include "ob_ups_multi_get.h"

using namespace oceanbase;
using namespace sql;

ObUpsMultiGet::ObUpsMultiGet()
  :get_param_(NULL),
  rpc_stub_(NULL),
  got_cell_count_(0),
  row_desc_(NULL)
{
}

ObUpsMultiGet::~ObUpsMultiGet()
{
}

bool ObUpsMultiGet::check_inner_stat()
{
  bool ret = false;
  ret = NULL != get_param_ && NULL != rpc_stub_ && NULL != row_desc_ && network_timeout_ > 0;
  if(!ret)
  {
    TBSYS_LOG(WARN, "get_param_[%p], rpc_stub_[%p], row_desc_[%p], network_timeout_[%ld]",
      get_param_, rpc_stub_, row_desc_, network_timeout_);
  }
  return ret;
}

int ObUpsMultiGet::set_child(int32_t child_idx, ObPhyOperator &child_operator)
{
  int ret = OB_NOT_IMPLEMENT;
  UNUSED(child_idx);
  UNUSED(child_operator);
  TBSYS_LOG(WARN, "not implement");
  return ret;
}

int64_t ObUpsMultiGet::to_string(char* buf, const int64_t buf_len) const
{
  int ret = OB_NOT_IMPLEMENT;
  UNUSED(buf);
  UNUSED(buf_len);
  TBSYS_LOG(WARN, "not implement");
  return ret;
}

int ObUpsMultiGet::open()
{
  int ret = OB_SUCCESS;
  bool is_fullfilled = false;
  int64_t fullfilled_item_num = 0;

  if(!check_inner_stat())
  {
    ret = OB_ERROR;
    TBSYS_LOG(WARN, "check inner stat fail");
  }
  else if(OB_SUCCESS != (ret = next_get_param()))
  {
    TBSYS_LOG(WARN, "construct next get param fail:ret[%d]", ret);
  }
  else if(OB_SUCCESS != (ret = rpc_stub_->get(1000, ups_server, cur_get_param_, cur_new_scanner_)))
  {
    TBSYS_LOG(WARN, "ups get rpc fail:ret[%d]", ret);
  }
  else if(OB_SUCCESS != (ret = cur_new_scanner_.get_is_req_fullfilled(is_fullfilled, fullfilled_item_num)))
  {
    TBSYS_LOG(WARN, "get is req fillfulled fail:ret[%d]", ret);
  }
  else
  {
    if(fullfilled_item_num > 0)
    {
      got_cell_count_ += fullfilled_item_num;
    }
    else if(!is_fullfilled)
    {
      ret = OB_ERR_UNEXPECTED;
      TBSYS_LOG(WARN, "get no item:ret[%d]", ret);
    }
  }
  return ret;
}

int ObUpsMultiGet::close()
{
  int ret = OB_SUCCESS;
  return ret;
}

int ObUpsMultiGet::get_next_row(const ObString *&rowkey, const ObRow *&row)
{
  int ret = OB_SUCCESS;
  bool is_fullfilled = false;
  int64_t fullfilled_item_num = 0;

  if(!check_inner_stat())
  {
    ret = OB_ERROR;
    TBSYS_LOG(WARN, "check inner stat fail");
  }

  while(OB_SUCCESS == ret)
  {
    ret = cur_new_scanner_.get_next_row(cur_rowkey_, cur_ups_row_);
    if(OB_ITER_END == ret)
    {
      if(OB_SUCCESS != (ret = cur_new_scanner_.get_is_req_fullfilled(is_fullfilled, fullfilled_item_num)))
      {
        TBSYS_LOG(WARN, "get is fullfilled fail:ret[%d]", ret);
      }
      else if(is_fullfilled)
      {
        ret = OB_ITER_END;
      }
      else
      {
        if(OB_SUCCESS != (ret = next_get_param()))
        {
          TBSYS_LOG(WARN, "construct next get param fail:ret[%d]", ret);
        }
        else if(OB_SUCCESS != (ret = rpc_stub_->get(1000, ups_server, cur_get_param_, cur_new_scanner_)))
        {
          TBSYS_LOG(WARN, "ups get rpc fail:ret[%d]", ret);
        }
        else if(OB_SUCCESS != (ret = cur_new_scanner_.get_is_req_fullfilled(is_fullfilled, fullfilled_item_num)))
        {
          TBSYS_LOG(WARN, "get is req fillfulled fail:ret[%d]", ret);
        }
        else
        {
          if(fullfilled_item_num > 0)
          {
            got_cell_count_ += fullfilled_item_num;
          }
          else if(!is_fullfilled)
          {
            ret = OB_ERR_UNEXPECTED;
            TBSYS_LOG(WARN, "get no item:ret[%d]", ret);
          }
        }
      }
    }
    else if(OB_SUCCESS == ret)
    {
      break;
    }
    else
    {
      TBSYS_LOG(WARN, "get next row fail:ret[%d]", ret);
    }
  }

  if(OB_SUCCESS == ret)
  {
    rowkey = &cur_rowkey_;
    row = &cur_ups_row_;
  }
  return ret;
}


/*
 * 如果一次rpc调用不能返回所以结果数据，通过next_get_param构造下次请求的
 * get_param
 * 这个函数隐含一个假设，ObNewScanner返回的数据必然在get_param的前面，且
 * 个数一样，这个在服务器端保证
 */
int ObUpsMultiGet::next_get_param()
{
  int ret = OB_SUCCESS;
  cur_get_param_.reset();
  for(int64_t i=got_cell_count_;OB_SUCCESS == ret && i<get_param_->get_cell_size();i++)
  {
    if(OB_SUCCESS != (ret = cur_get_param_.add_cell((*(*get_param_)[i]))))
    {
      TBSYS_LOG(WARN, "get param add cell fail:ret[%d]", ret);
    }
  }
  return ret;
}


void ObUpsMultiGet::reset()
{
  get_param_ = NULL;
  cur_new_scanner_.reset();
  got_cell_count_ = 0;
  row_desc_ = false;
}

void ObUpsMultiGet::set_row_desc(const ObRowDesc &row_desc)
{
  row_desc_ = &row_desc;
  cur_ups_row_.set_row_desc(*row_desc_);
}

