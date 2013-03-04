/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: ob_update_condition.cpp,v 0.1 2011/03/31 15:42:58 chuanhui Exp $
 *
 * Authors:
 *   chuanhui <rizhao.ych@taobao.com>
 *     - some work details if you want
 *
 */
#include "ob_update_condition.h"

namespace oceanbase
{
  namespace common
  {
    ObUpdateCondition::ObUpdateCondition()
    {
    }

    ObUpdateCondition::~ObUpdateCondition()
    {
    }

    void ObUpdateCondition::reset()
    {
      conditions_.clear();
      string_buf_.reset();
    }

    int ObUpdateCondition::add_cond(const ObString& table_name, const ObString& row_key,
        const ObString& column_name, const ObLogicOperator op_type, const ObObj& value)
    {
      int err = OB_SUCCESS;
      ObCondInfo cond_info;
      cond_info.set(op_type, table_name, row_key, column_name, value);
      err = add_cond(cond_info);
      if (OB_SUCCESS != err)
      {
        TBSYS_LOG(WARN, "failed to add cond info, err=%d", err);
      }
      return err;
    }

    int ObUpdateCondition::add_cond(const ObString& table_name, const ObString& row_key,
        const bool is_exist)
    {
      int err = OB_SUCCESS;
      ObCondInfo cond_info;
      cond_info.set(table_name, row_key, is_exist);
      err = add_cond(cond_info);
      if (OB_SUCCESS != err)
      {
        TBSYS_LOG(WARN, "failed to add cond info, err=%d", err);
      }
      return err;
    }

    int ObUpdateCondition::add_cond(const ObCondInfo& cond_info)
    {
      int err = OB_SUCCESS;
      ObCondInfo dst_cond_info;
      err = cond_info.deep_copy(dst_cond_info, string_buf_);
      if (OB_SUCCESS != err)
      {
        TBSYS_LOG(WARN, "failed to copy cond info, err=%d", err);
      }
      else
      {
        err = conditions_.push_back(dst_cond_info);
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN, "failed to push back cond info, err=%d", err);
        }
      }
      return err;
    }

    const ObCondInfo* ObUpdateCondition::operator[] (const int64_t index) const
    {
      ObCondInfo* ret = NULL;
      if ((index < 0) || (index >= conditions_.size()))
      {
        TBSYS_LOG(WARN, "invalid param, index=%ld, size=%ld", index, (int64_t) conditions_.size());
      }
      else
      {
        ret = &conditions_[static_cast<int32_t>(index)];
      }
      return ret;
    }

    ObCondInfo* ObUpdateCondition::operator[] (const int64_t index)
    {
      ObCondInfo* ret = NULL;
      if ((index < 0) || (index >= conditions_.size()))
      {
        TBSYS_LOG(WARN, "invalid param, index=%ld, size=%ld", index, (int64_t) conditions_.size());
      }
      else
      {
        ret = &conditions_[static_cast<int32_t>(index)];
      }
      return ret;
    }

    DEFINE_SERIALIZE(ObUpdateCondition)
    {
      int err = OB_SUCCESS;
      int64_t count = conditions_.size();
      for (int64_t i = 0; i < count; ++i)
      {
        err = conditions_[static_cast<int32_t>(i)].serialize(buf, buf_len, pos);
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN, "failed to serialize cond info, i=%ld, err=%d", i, err);
          break;
        }
      }
      return err;
    }

    DEFINE_DESERIALIZE(ObUpdateCondition)
    {
      int err = OB_SUCCESS;
      reset();
      ObObj temp;
      int64_t temp_pos = pos;
      ObCondInfo cond_info;
      while ((temp_pos < data_len) 
          && (OB_SUCCESS == (err = temp.deserialize(buf, data_len, temp_pos)))
          && (ObExtendType != temp.get_type()))
      {
        err = cond_info.deserialize(buf, data_len, pos);
        if (err != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "the condition deserialize failed, err=%d", err);
          break;
        }
        err = add_cond(cond_info);
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN, "failed to add cond info, err=%d", err);
          break;
        }
        cond_info.reset();
        temp_pos = pos;
      }
      return err;
    }

    DEFINE_GET_SERIALIZE_SIZE(ObUpdateCondition)
    {
      int64_t store_size = 0;
      int64_t count = conditions_.size();
      for (int64_t i = 0; i < count; ++i)
      {
        store_size += conditions_[static_cast<int32_t>(i)].get_serialize_size();
      }
      return store_size;
    }
  }
}


