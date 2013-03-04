/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_table_rpc_scan.cpp
 *
 * Authors:
 *   Yu Huang <xiaochu.yh@taobao.com>
 *
 */
#include "ob_table_rpc_scan.h"
#include "common/utility.h"

namespace oceanbase
{
  namespace sql
  {
    ObTableRpcScan::ObTableRpcScan() :
      rpc_scan_(), rename_(), limit_(),
      has_rpc_(false), has_rename_(false), has_limit_(false)
    {
    }

    ObTableRpcScan::~ObTableRpcScan()
    {
    }

    int ObTableRpcScan::open()
    {
      int ret = OB_SUCCESS;
      // rpc_scan_ is the leaf operator
      if (OB_SUCCESS == ret && has_rpc_)
      {
        child_op_ = &rpc_scan_;
      }
      else
      {
        ret = OB_NOT_INIT;
        TBSYS_LOG(WARN, "must call init() before call open(). ret=%d", ret);
      }
      // more operation over the leaf
      if (OB_SUCCESS == ret && has_rename_)
      {
        if (OB_SUCCESS != (ret = rename_.set_child(0, *child_op_)))
        {
          TBSYS_LOG(WARN, "fail to set rename child. ret=%d", ret);
        }
        else
        {
          child_op_ = &rename_;
        }
      }
      // limit
      if (OB_SUCCESS == ret && has_limit_)
      {
        if (OB_SUCCESS != (ret = limit_.set_child(0, *child_op_)))
        {
          TBSYS_LOG(WARN, "fail to set limit child. ret=%d", ret);
        }
        else
        {
          child_op_ = &limit_;
        }
      }

      // open the operation chain
      if (OB_SUCCESS == ret)
      {
        ret = child_op_->open();
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "fail to open table scan in mem operator. ret=%d", ret);
        }
      }
      return ret;
    }

    int ObTableRpcScan::close()
    {
      int ret = OB_SUCCESS;
      if (NULL == child_op_)
      {
        ret = OB_NOT_INIT;
      }
      else
      {
        ret = child_op_->close();
      }
      return ret;
    }

    int ObTableRpcScan::get_next_row(const common::ObRow *&row)
    {
      int ret = OB_SUCCESS;
      if (OB_UNLIKELY(NULL == child_op_))
      {
        ret = OB_NOT_INIT;
      }
      else
      {
        ret = child_op_->get_next_row(row);
      }
      return ret;
    }

    int ObTableRpcScan::get_row_desc(const common::ObRowDesc *&row_desc) const
    {
      int ret = OB_SUCCESS;
      if (OB_UNLIKELY(NULL == child_op_))
      {
        ret = OB_NOT_INIT;
      }
      else
      {
        ret = child_op_->get_row_desc(row_desc);
      }
      return ret;
    }

    int ObTableRpcScan::init(mergeserver::ObMergerLocationCacheProxy * cache_proxy, mergeserver::ObMergerAsyncRpcStub * async_rpc)
    {
      int ret = rpc_scan_.init(cache_proxy, async_rpc);
      if (OB_SUCCESS == ret)
      {
        has_rpc_ = true;
      }
      return ret;
    }

    int ObTableRpcScan::add_output_column(const ObSqlExpression& expr)
    {
      int ret = OB_SUCCESS;
      if (OB_SUCCESS == ret)
      {
        // add output column to scan param
        ret = rpc_scan_.add_output_column(expr);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "fail to add column to rpc scan operator. ret=%d", ret);
        }
      }
      return ret;
    }

    int ObTableRpcScan::set_table(const uint64_t table_id, const uint64_t base_table_id)
    {
      int ret = OB_SUCCESS;
      if (table_id != base_table_id)
      {
        ret = rename_.set_table(table_id, base_table_id);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "fail to rename table id. ret=%d", ret);
        }
        else
        {
          has_rename_ = true;
        }
      }
      if (OB_SUCCESS == ret)
      {
        // add table id to scan param
        ret = rpc_scan_.set_table(base_table_id);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "fail to add table id to rpc scan operator. table_id=%lu, ret=%d", base_table_id, ret);
        }
      }
      return ret;
    }

    int ObTableRpcScan::add_filter(const ObSqlExpression& expr)
    {
      int ret = OB_SUCCESS;
      // add filter to scan param
      ret = rpc_scan_.add_filter(expr);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "fail to add filter to rpc scan operator. ret=%d", ret);
      }
      return ret;
    }

    int ObTableRpcScan::set_limit(const int64_t limit, const int64_t offset)
    {
      int ret = OB_SUCCESS;
      ret = limit_.set_limit(limit, offset);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "fail to set limit. ret=%d", ret);
      }
      else
      {
        has_limit_ = true;
        // add limit to scan param
        ret = rpc_scan_.set_limit(limit, offset);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "fail to add limit/offset to rpc scan operator. limit=%ld, offset=%ld, ret=%d", limit, offset, ret);
        }
      }
      return ret;
    }

    int64_t ObTableRpcScan::to_string(char* buf, const int64_t buf_len) const
    {
      int64_t pos = 0;
      databuff_printf(buf, buf_len, pos, "TableRpcScan(");
      if (has_limit_)
      {
        databuff_printf(buf, buf_len, pos, "limit=<");
        pos += limit_.to_string(buf+pos, buf_len-pos);
        databuff_printf(buf, buf_len, pos, ">, ");
      }
      if (has_rename_)
      {
        databuff_printf(buf, buf_len, pos, "rename=<");
        pos += rename_.to_string(buf+pos, buf_len-pos);
        databuff_printf(buf, buf_len, pos, ">, ");
      }
      databuff_printf(buf, buf_len, pos, "rpc_scan=<");
      pos += rpc_scan_.to_string(buf+pos, buf_len-pos);
      databuff_printf(buf, buf_len, pos, ">)\n");
      return pos;
    }
  } // end namespace sql
} // end namespace oceanbase
