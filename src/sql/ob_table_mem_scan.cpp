/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_table_scan.cpp
 *
 * Authors:
 *   Yu Huang <xiaochu.yh@taobao.com>
 *
 */
#include "ob_table_mem_scan.h"
#include "common/utility.h"

namespace oceanbase
{
  namespace sql
  {
    ObTableMemScan::ObTableMemScan() :
      rename_(), project_(), filter_(), limit_(),
      has_rename_(false), has_project_(false), has_filter_(false), has_limit_(false)
    {
    }

    ObTableMemScan::~ObTableMemScan()
    {
    }

    int ObTableMemScan::open()
    {
      int ret = OB_SUCCESS;
      if (NULL == child_op_)
      {
        ret = OB_NOT_INIT;
      }
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
      else
      {
        ret = OB_NOT_INIT;
        TBSYS_LOG(WARN, "must call set_table() before call open(). ret=%d", ret);
      }
      if (OB_SUCCESS == ret && has_project_)
      {
        if (OB_SUCCESS != (ret = project_.set_child(0, *child_op_)))
        {
          TBSYS_LOG(WARN, "fail to set project child. ret=%d", ret);
        }
        else
        {
          child_op_ = &project_;
        }
      }
      if (OB_SUCCESS == ret && has_filter_)
      {
        if (OB_SUCCESS != (ret = filter_.set_child(0, *child_op_)))
        {
          TBSYS_LOG(WARN, "fail to set filter child. ret=%d", ret);
        }
        else
        {
          child_op_ = &filter_;
        }
      }
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

    int ObTableMemScan::close()
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

    int ObTableMemScan::get_next_row(const common::ObRow *&row)
    {
      int ret = OB_SUCCESS;
      if (OB_UNLIKELY(NULL == child_op_))
      {
        TBSYS_LOG(ERROR, "child op is NULL");
        ret = OB_NOT_INIT;
      }
      else
      {
        ret = child_op_->get_next_row(row);
      }
      return ret;
    }

    int ObTableMemScan::get_row_desc(const common::ObRowDesc *&row_desc) const
    {
      int ret = OB_SUCCESS;
      if (OB_UNLIKELY(NULL == child_op_))
      {
        TBSYS_LOG(ERROR, "child op is NULL");
        ret = OB_NOT_INIT;
      }
      else
      {
        ret = child_op_->get_row_desc(row_desc);
      }
      return ret;
    }

    int ObTableMemScan::add_output_column(const ObSqlExpression& expr)
    {
      int ret = OB_SUCCESS;
      ret = project_.add_output_column(expr);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "fail to add project. ret=%d", ret);
      }
      else
      {
        has_project_ = true;
      }
      return ret;
    }

    int ObTableMemScan::set_table(const uint64_t table_id, const uint64_t base_table_id)
    {
      int ret = OB_SUCCESS;
      ret = rename_.set_table(table_id, base_table_id);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "fail to rename table id. ret=%d", ret);
      }
      else
      {
        has_rename_ = true;
      }
      return ret;
    }


    int ObTableMemScan::add_filter(const ObSqlExpression& expr)
    {
      int ret = OB_SUCCESS;
      ret = filter_.add_filter(expr);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "fail to add filter. ret=%d", ret);
      }
      else
      {
        has_filter_ = true;
      }
      return ret;
    }

    int ObTableMemScan::set_limit(const int64_t limit, const int64_t offset)
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
      }
      return ret;
    }

    int64_t ObTableMemScan::to_string(char* buf, const int64_t buf_len) const
    {
      int64_t pos = 0;
      databuff_printf(buf, buf_len, pos, "TableMemScan(");
      if (has_project_)
      {
        databuff_printf(buf, buf_len, pos, "project=<");
        pos += project_.to_string(buf+pos, buf_len-pos);
        databuff_printf(buf, buf_len, pos, ">, ");
      }
      if (has_limit_)
      {
        databuff_printf(buf, buf_len, pos, "limit=<");
        pos += limit_.to_string(buf+pos, buf_len-pos);
        databuff_printf(buf, buf_len, pos, ">, ");
      }
      if (has_filter_)
      {
        databuff_printf(buf, buf_len, pos, "filter=<");
        pos += filter_.to_string(buf+pos, buf_len-pos);
        databuff_printf(buf, buf_len, pos, ">, ");
      }
      if (has_rename_)
      {
        databuff_printf(buf, buf_len, pos, "rename=<");
        pos += rename_.to_string(buf+pos, buf_len-pos);
        databuff_printf(buf, buf_len, pos, ">)\n");
      }
      if (NULL != child_op_)
      {
        pos += child_op_->to_string(buf+pos, buf_len-pos);
      }
      return pos;
    }
  } // end namespace sql
} // end namespace oceanbase
