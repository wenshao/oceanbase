/**
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * Authors:
 *   yubai <yubai.lk@taobao.com>
 *     - some work details if you want
 *   yanran <yanran.hfs@taobao.com> 2010-10-27
 *     - new serialization format(ObObj composed, extented version)
 *   xiaochu.yh <xiaochu.yh@taobao.com> 2012-6-14
 *     - derived from ob_scanner.cpp and make it row interface only
 */

#include "ob_new_scanner.h"

#include <new>

#include "tblog.h"
#include "ob_malloc.h"
#include "ob_define.h"
#include "serialization.h"
#include "ob_action_flag.h"
#include "utility.h"
#include "ob_row.h"

using namespace oceanbase::common;

///////////////////////////////////////////////////////////////////////////////////////////////////

ObNewScanner::ObNewScanner() : 
row_store_(), cur_size_counter_(0), cur_table_name_(), cur_table_id_(OB_INVALID_ID), cur_row_key_(),
tmp_table_name_(), tmp_table_id_(OB_INVALID_ID), tmp_row_key_(),
mem_size_limit_(DEFAULT_MAX_SERIALIZE_SIZE)
{
  data_version_ = 0;
  has_range_ = false;
  is_request_fullfilled_ = false;
  fullfilled_item_num_ = 0;
  id_name_type_ = UNKNOWN;
  has_row_key_flag_ = false;
  row_num_ = 0;
  cell_num_ = 0;
  whole_result_row_num_ = -1;
}

ObNewScanner::~ObNewScanner()
{
  // memory auto released in row_store_;
  // TODO: nop
}

void ObNewScanner::reset()
{
  //TODO: need reset store?  
  // row_store_.reset();
  row_store_.clear();
  cur_size_counter_ = 0;
  cur_table_name_.assign(NULL, 0);
  cur_table_id_ = OB_INVALID_ID;
  cur_row_key_.assign(NULL, 0);
  tmp_table_name_.assign(NULL, 0);
  tmp_table_id_ = OB_INVALID_ID;
  tmp_row_key_.assign(NULL, 0);

  range_.reset();
  data_version_ = 0;
  has_range_ = false;
  is_request_fullfilled_ = false;
  fullfilled_item_num_ = 0;
  id_name_type_ = UNKNOWN;
  last_row_key_.assign(NULL, 0);
  id_name_type_ = UNKNOWN;
  has_row_key_flag_ = false;

  row_num_ = cell_num_ = 0;
  whole_result_row_num_ = -1;
}

void ObNewScanner::clear()
{
  row_store_.clear();
  cur_size_counter_ = 0;
  cur_table_name_.assign(NULL, 0);
  cur_table_id_ = OB_INVALID_ID;
  cur_row_key_.assign(NULL, 0);
  tmp_table_name_.assign(NULL, 0);
  tmp_table_id_ = OB_INVALID_ID;
  tmp_row_key_.assign(NULL, 0);

  range_.reset();
  data_version_ = 0;
  has_range_ = false;
  is_request_fullfilled_ = false;
  fullfilled_item_num_ = 0;
  id_name_type_ = UNKNOWN;
  last_row_key_.assign(NULL, 0);
  id_name_type_ = UNKNOWN;
  has_row_key_flag_ = false;

  row_num_ = cell_num_  = 0;
  whole_result_row_num_ = -1;
}

int64_t ObNewScanner::set_mem_size_limit(const int64_t limit)
{
  if (0 > limit
      || OB_MAX_PACKET_LENGTH < limit)
  {
    TBSYS_LOG(WARN, "invlaid limit_size=%ld cur_limit_size=%ld",
              limit, mem_size_limit_);
  }
  else if (0 == limit)
  {
    // 0 means using the default value
  }
  else
  {
    mem_size_limit_ = limit;
  }
  return mem_size_limit_;
}


int ObNewScanner::add_row(const ObRow &row)
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = row_store_.add_row(row, cur_size_counter_)))
  {
    TBSYS_LOG(WARN, "fail to add_row to row store. ret=%d", ret);
  }
  return ret;
}


DEFINE_SERIALIZE(ObNewScanner)
//int ObNewScanner::serialize(char* buf, const int64_t buf_len, int64_t& pos) const
{
  int ret = OB_SUCCESS;
  ObObj obj;
  int64_t next_pos = pos;

  if (OB_SUCCESS == ret)
  {
    ///obj.reset();
    obj.set_ext(ObActionFlag::BASIC_PARAM_FIELD);
    ret = obj.serialize(buf, buf_len, next_pos);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "ObObj serialize error, ret=%d buf=%p data_len=%ld next_pos=%ld",
                ret, buf, buf_len, next_pos);
    }
  }

  if (OB_SUCCESS == ret)
  {
    ///obj.reset();
    obj.set_int(is_request_fullfilled_ ? 1 : 0);
    ret = obj.serialize(buf, buf_len, next_pos);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "ObObj serialize error, ret=%d buf=%p data_len=%ld next_pos=%ld",
                ret, buf, buf_len, next_pos);
    }
  }

  if (OB_SUCCESS == ret)
  {
    ///obj.reset();
    obj.set_int(fullfilled_item_num_);
    ret = obj.serialize(buf, buf_len, next_pos);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "ObObj serialize error, ret=%d buf=%p data_len=%ld next_pos=%ld",
                ret, buf, buf_len, next_pos);
    }
  }

  if (OB_SUCCESS == ret)
  {
    ///obj.reset();
    obj.set_int(data_version_);
    ret = obj.serialize(buf, buf_len, next_pos);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "ObObj serialize error, ret=%d buf=%p data_len=%ld next_pos=%ld",
                ret, buf, buf_len, next_pos);
    }
  }

  if (OB_SUCCESS == ret)
  {
    obj.set_int(whole_result_row_num_);
    ret = obj.serialize(buf, buf_len, next_pos);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "ObObj serialize error, ret=%d buf=%p data_len=%ld next_pos=%ld",
                ret, buf, buf_len, next_pos);
    }
  }


  if (has_range_)
  {
    if (OB_SUCCESS == ret)
    {
      ///obj.reset();
      obj.set_ext(ObActionFlag::TABLET_RANGE_FIELD);
      ret = obj.serialize(buf, buf_len, next_pos);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "ObObj serialize error, ret=%d buf=%p data_len=%ld next_pos=%ld",
                  ret, buf, buf_len, next_pos);
      }
    }

    if (OB_SUCCESS == ret)
    {
      ///obj.reset();
      obj.set_int(range_.border_flag_.get_data());
      ret = obj.serialize(buf, buf_len, next_pos);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "ObObj serialize error, ret=%d buf=%p data_len=%ld next_pos=%ld",
                  ret, buf, buf_len, next_pos);
      }
    }

    if (OB_SUCCESS == ret)
    {
      ///obj.reset();
      obj.set_varchar(range_.start_key_);
      ret = obj.serialize(buf, buf_len, next_pos);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "ObObj serialize error, ret=%d buf=%p data_len=%ld next_pos=%ld",
                  ret, buf, buf_len, next_pos);
      }
    }

    if (OB_SUCCESS == ret)
    {
      ///obj.reset();
      obj.set_varchar(range_.end_key_);
      ret = obj.serialize(buf, buf_len, next_pos);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "ObObj serialize error, ret=%d buf=%p data_len=%ld next_pos=%ld",
                  ret, buf, buf_len, next_pos);
      }
    }
  }

  if (cur_size_counter_ > 0)
  {
    if (OB_SUCCESS == ret)
    {
      ///obj.reset();
      obj.set_ext(ObActionFlag::TABLE_PARAM_FIELD);
      ret = obj.serialize(buf, buf_len, next_pos);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "ObObj serialize error, ret=%d buf=%p data_len=%ld next_pos=%ld",
                  ret, buf, buf_len, next_pos);
      }
    }

    if (OB_SUCCESS == ret)
    {
      ret = row_store_.serialize(buf, buf_len, next_pos);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "ObRowStore serialize error, ret=%d buf=%p data_len=%ld next_pos=%ld",
            ret, buf, buf_len, next_pos);
      }
    }
  }

  if (OB_SUCCESS == ret)
  {
    ///obj.reset();
    TBSYS_LOG(DEBUG, "set  END_PARAM_FIELD to scanner");
    obj.set_ext(ObActionFlag::END_PARAM_FIELD);
    ret = obj.serialize(buf, buf_len, next_pos);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "ObObj serialize error, ret=%d buf=%p data_len=%ld next_pos=%ld",
                ret, buf, buf_len, next_pos);
    }
  }

  if (OB_SUCCESS == ret)
  {
    pos = next_pos;
  }
  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObNewScanner)
//int64_t ObNewScanner::get_serialize_size(void) const
{
  int64_t ret = 0;
  TBSYS_LOG(WARN, "SER size false" );
  return ret;
}

DEFINE_DESERIALIZE(ObNewScanner)
//int ObNewScanner::deserialize(const char* buf, const int64_t data_len, int64_t& pos)
{
  int ret = OB_SUCCESS;
  TBSYS_LOG(DEBUG, "buf=%p", buf);

  if (NULL == buf || 0 >= data_len - pos)
  {
    TBSYS_LOG(WARN, "invalid param buf=%p data_len=%ld pos=%ld", buf, data_len, pos);
    ret = OB_ERROR;
  }
  else
  {
    ObObj param_id;
    ObObjType obj_type;
    int64_t param_id_type;
    bool is_end = false;
    int64_t new_pos = pos;

    reset();

    ret = param_id.deserialize(buf, data_len, new_pos);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "ObObj deserialize error, ret=%d", ret);
    }

    while (OB_SUCCESS == ret && new_pos <= data_len && !is_end)
    {
      obj_type = param_id.get_type();
      // read until reaching a ObExtendType ObObj
      while (OB_SUCCESS == ret && ObExtendType != obj_type)
      {
        ret = param_id.deserialize(buf, data_len, new_pos);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "ObObj deserialize error, ret=%d", ret);
        }
        else
        {
          obj_type = param_id.get_type();
        }
      }

      if (OB_SUCCESS == ret)
      {
        ret = param_id.get_ext(param_id_type);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "ObObj type is not ObExtendType, invalid status, ret=%d", ret);
        }
        else
        {
          switch (param_id_type)
          {
          case ObActionFlag::BASIC_PARAM_FIELD:
            ret = deserialize_basic_(buf, data_len, new_pos, param_id);
            if (OB_SUCCESS != ret)
            {
              TBSYS_LOG(WARN, "deserialize_basic_ error, ret=%d buf=%p data_len=%ld new_pos=%ld",
                        ret, buf, data_len, new_pos);
            }
            break;
          case ObActionFlag::TABLE_PARAM_FIELD:
            ret = deserialize_table_(buf, data_len, new_pos, param_id);
            if (OB_SUCCESS != ret)
            {
              TBSYS_LOG(WARN, "deserialize_table_ error, ret=%d buf=%p data_len=%ld new_pos=%ld",
                        ret, buf, data_len, new_pos);
            }
            break;
          case ObActionFlag::END_PARAM_FIELD:
            is_end = true;
            break;
          default:
            ret = param_id.deserialize(buf, data_len, new_pos);
            if (OB_SUCCESS != ret)
            {
              TBSYS_LOG(WARN, "ObObj deserialize error, ret=%d", ret);
            }
            break;
          }
        }
      }
    }

    if (OB_SUCCESS == ret)
    {
      pos = new_pos;
    }
  }
  return ret;
}


int ObNewScanner::deserialize_basic_(const char* buf, const int64_t data_len, int64_t& pos, ObObj &last_obj)
{
  int ret = OB_SUCCESS;

  int64_t value = 0;

  // deserialize isfullfilled
  ret = deserialize_int_(buf, data_len, pos, value, last_obj);
  if (OB_SUCCESS != ret)
  {
    TBSYS_LOG(WARN, "deserialize_int_ error, ret=%d buf=%p data_len=%ld pos=%ld",
              ret, buf, data_len, pos);
  }
  else
  {
    is_request_fullfilled_ = (value == 0 ? false : true);
  }

  // deserialize filled item number
  if (OB_SUCCESS == ret)
  {
    ret = deserialize_int_(buf, data_len, pos, value, last_obj);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "deserialize_int_ error, ret=%d buf=%p data_len=%ld pos=%ld",
                ret, buf, data_len, pos);
    }
    else
    {
      fullfilled_item_num_ = value;
    }
  }

  // deserialize data_version
  if (OB_SUCCESS == ret)
  {
    ret = deserialize_int_(buf, data_len, pos, value, last_obj);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "deserialize_int_ error, ret=%d buf=%p data_len=%ld pos=%ld",
                ret, buf, data_len, pos);
    }
    else
    {
      data_version_ = value;
    }
  }

  // deserialize whole_result_row_num
  if (OB_SUCCESS == ret)
  {
    ret = deserialize_int_(buf, data_len, pos, value, last_obj);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "deserialize_int_ error, ret=%d buf=%p data_len=%ld pos=%ld",
          ret, buf, data_len, pos);
    }
    else
    {
      whole_result_row_num_ = value;
    }
  }

  if (OB_SUCCESS == ret)
  {
    ret = last_obj.deserialize(buf, data_len, pos);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "ObObj deserialize error, ret=%d buf=%p data_len=%ld pos=%ld",
                ret, buf, data_len, pos);
    }
    else
    {
      if (ObExtendType != last_obj.get_type())
      {
        // maybe new added field
      }
      else
      {
        int64_t type = last_obj.get_ext();
        if (ObActionFlag::TABLET_RANGE_FIELD == type)
        {
          int64_t border_flag = 0;
          ObString start_row;
          ObString end_row;
          ret = deserialize_int_(buf, data_len, pos, border_flag, last_obj);
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "deserialize_int_ error, ret=%d buf=%p data_len=%ld pos=%ld",
                      ret, buf, data_len, pos);
          }

          if (OB_SUCCESS == ret)
          {
            ret = deserialize_varchar_(buf, data_len, pos, start_row, last_obj);
            if (OB_SUCCESS != ret)
            {
              TBSYS_LOG(WARN, "deserialize_varchar_ error, ret=%d buf=%p data_len=%ld pos=%ld",
                        ret, buf, data_len, pos);
            }
          }

          if (OB_SUCCESS == ret)
          {
            ret = deserialize_varchar_(buf, data_len, pos, end_row, last_obj);
            if (OB_SUCCESS != ret)
            {
              TBSYS_LOG(WARN, "deserialize_varchar_ error, ret=%d buf=%p data_len=%ld pos=%ld",
                        ret, buf, data_len, pos);
            }
          }

          if (OB_SUCCESS == ret)
          {
            range_.table_id_ = 0;
            range_.border_flag_.set_data(static_cast<int8_t>(border_flag));
            string_buf_.write_string(start_row, &(range_.start_key_));
            string_buf_.write_string(end_row, &(range_.end_key_));
            has_range_ = true;
          }
        }
        else
        {
          // maybe another param field
        }
      }
    }
  }

  // after reading all neccessary fields,
  // filter unknown ObObj
  if (OB_SUCCESS == ret && ObExtendType != last_obj.get_type())
  {
    ret = last_obj.deserialize(buf, data_len, pos);
    while (OB_SUCCESS == ret && ObExtendType != last_obj.get_type())
    {
      ret = last_obj.deserialize(buf, data_len, pos);
    }
  }

  return ret;
}

int ObNewScanner::deserialize_table_(const char* buf, const int64_t data_len, int64_t& pos, ObObj &last_obj)
{
  int ret = OB_SUCCESS;
  TBSYS_LOG(DEBUG, "deserialize_table_ begin, buf=%p data_len=%ld new_pos=%ld",
      buf, data_len, pos);
  ret = row_store_.deserialize(buf, data_len, pos);
  TBSYS_LOG(DEBUG, "deserialize_table_ end, buf=%p data_len=%ld new_pos=%ld",
      buf, data_len, pos);
  if (OB_SUCCESS != ret)
  {
    TBSYS_LOG(WARN, "deserialize_table_ error, ret=%d buf=%p data_len=%ld new_pos=%ld",
        ret, buf, data_len, pos);
  }
  if (OB_SUCCESS == ret)
  {
    ret = last_obj.deserialize(buf, data_len, pos);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "ObObj deserialize error, ret=%d", ret);
    }
  }
  return ret;
}

int ObNewScanner::deserialize_int_(const char* buf, const int64_t data_len, int64_t& pos,
    int64_t &value, ObObj &last_obj)
{
  int ret = OB_SUCCESS;

  ret = last_obj.deserialize(buf, data_len, pos);
  if (OB_SUCCESS != ret)
  {
    TBSYS_LOG(WARN, "ObObj deserialize error, ret=%d buf=%p data_len=%ld pos=%ld",
              ret, buf, data_len, pos);
  }
  else
  {
    if (ObIntType != last_obj.get_type())
    {
      TBSYS_LOG(WARN, "ObObj type is not Int, Type=%d", last_obj.get_type());
    }
    else
    {
      ret = last_obj.get_int(value);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "ObObj get_int error, ret=%d", ret);
      }
    }
  }

  return ret;
}

int ObNewScanner::deserialize_varchar_(const char* buf, const int64_t data_len, int64_t& pos,
                                    ObString &value, ObObj &last_obj)
{
  int ret = OB_SUCCESS;

  ret = last_obj.deserialize(buf, data_len, pos);
  if (OB_SUCCESS != ret)
  {
    TBSYS_LOG(WARN, "ObObj deserialize error, ret=%d buf=%p data_len=%ld pos=%ld",
              ret, buf, data_len, pos);
  }
  else
  {
    if (ObVarcharType != last_obj.get_type())
    {
      TBSYS_LOG(WARN, "ObObj type is not Varchar, Type=%d", last_obj.get_type());
    }
    else
    {
      ret = last_obj.get_varchar(value);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "ObObj get_varchar error, ret=%d", ret);
      }
    }
  }

  return ret;
}

int ObNewScanner::deserialize_int_or_varchar_(const char* buf, const int64_t data_len, int64_t& pos,
                                           int64_t& int_value, ObString &varchar_value, ObObj &last_obj)
{
  int ret = OB_SUCCESS;

  ret = last_obj.deserialize(buf, data_len, pos);
  if (OB_SUCCESS != ret)
  {
    TBSYS_LOG(WARN, "ObObj deserialize error, ret=%d buf=%p data_len=%ld pos=%ld",
              ret, buf, data_len, pos);
  }
  else
  {
    if (ObIntType == last_obj.get_type())
    {
      ret = last_obj.get_int(int_value);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "ObObj get_int error, ret=%d", ret);
      }
    }
    else if (ObVarcharType == last_obj.get_type())
    {
      ret = last_obj.get_varchar(varchar_value);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "ObObj get_varchar error, ret=%d", ret);
      }
    }
    else
    {
      TBSYS_LOG(WARN, "ObObj type is not int or varchar, Type=%d", last_obj.get_type());
      ret = OB_ERROR;
    }
  }

  return ret;
}


int ObNewScanner::add_row(const ObString &rowkey, const ObRow &row)
{
  int ret = OB_SUCCESS;
  const ObRowStore::StoredRow *stored_row = NULL;
  ret = row_store_.add_row(rowkey, row, stored_row);
  return ret;
}

int ObNewScanner::get_next_row(ObString &rowkey, ObRow &row)
{
  int ret = OB_SUCCESS;
  ret = row_store_.get_next_row(&rowkey, row);
  return ret;
}

int ObNewScanner::get_next_row(ObRow &row)
{
  int ret = OB_SUCCESS;
  ret = row_store_.get_next_row(row);
  return ret;
}

bool ObNewScanner::is_empty() const
{
  return row_store_.is_empty();
}

int ObNewScanner::get_last_row_key(ObString &row_key) const
{
  int ret = OB_SUCCESS;

  if (last_row_key_.length() == 0)
  {
    ret = OB_ENTRY_NOT_EXIST;
  }
  else
  {
    row_key = last_row_key_;
  }

  return ret;
}

int ObNewScanner::set_is_req_fullfilled(const bool &is_fullfilled, const int64_t fullfilled_item_num)
{
  int err = OB_SUCCESS;
  if (0 > fullfilled_item_num)
  {
    TBSYS_LOG(WARN,"param error [fullfilled_item_num:%ld]", fullfilled_item_num);
    err = OB_INVALID_ARGUMENT;
  }
  if (OB_SUCCESS == err)
  {
    fullfilled_item_num_ = fullfilled_item_num;
    is_request_fullfilled_ = is_fullfilled;
  }
  return err;
}

int ObNewScanner::get_is_req_fullfilled(bool &is_fullfilled, int64_t &fullfilled_item_num) const
{
  int err = OB_SUCCESS;
  is_fullfilled = is_request_fullfilled_;
  fullfilled_item_num = fullfilled_item_num_;
  return err;
}

int ObNewScanner::set_range(const ObRange &range)
{
  int ret = OB_SUCCESS;

  if (has_range_)
  {
    TBSYS_LOG(WARN, "range has been setted before");
    ret = OB_ERROR;
  }
  else
  {
    range_.table_id_ = range.table_id_;
    range_.border_flag_.set_data(range.border_flag_.get_data());

    ret = string_buf_.write_string(range.start_key_, &(range_.start_key_));
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "write_string error, ret=%d", ret);
    }
    else
    {
      ret = string_buf_.write_string(range.end_key_, &(range_.end_key_));
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "write_string error, ret=%d", ret);
      }
    }
    has_range_ = true;
  }

  return ret;
}

int ObNewScanner::set_range_shallow_copy(const ObRange &range)
{
  int ret = OB_SUCCESS;

  if (has_range_)
  {
    TBSYS_LOG(WARN, "range has been setted before");
    ret = OB_ERROR;
  }
  else
  {
    range_ = range;
    has_range_ = true;
  }

  return ret;
}

int ObNewScanner::get_range(ObRange &range) const
{
  int ret = OB_SUCCESS;

  if (!has_range_)
  {
    TBSYS_LOG(WARN, "range has not been setted");
    ret = OB_ENTRY_NOT_EXIST;
  }
  else
  {
    range = range_;
  }

  return ret;
}

void ObNewScanner::dump(void) const
{
  TBSYS_LOG(INFO, "[dump] scanner range info:");
  if (has_range_)
  {
    range_.hex_dump();
  }
  else
  {
    TBSYS_LOG(INFO, "    no scanner range found!");
  }
  if (has_row_key_flag_)
  {
    TBSYS_LOG(INFO, "[dump] last_row_key_=%.*s", last_row_key_.length(), last_row_key_.ptr());
  }
  else
  {
    TBSYS_LOG(INFO, "[dump] no last_row_key_ found!");
  }
 
  TBSYS_LOG(INFO, "[dump] is_request_fullfilled_=%d", is_request_fullfilled_);
  TBSYS_LOG(INFO, "[dump] fullfilled_item_num_=%ld", fullfilled_item_num_);
  TBSYS_LOG(INFO, "[dump] whole_result_row_num_=%ld", whole_result_row_num_);
  TBSYS_LOG(INFO, "[dump] row_num_=%ld", row_num_);
  TBSYS_LOG(INFO, "[dump] cell_num__=%ld", cell_num_);
  TBSYS_LOG(INFO, "[dump] cur_size_counter_=%ld", cur_size_counter_);
  TBSYS_LOG(INFO, "[dump] data_version_=%ld", data_version_);
}

void ObNewScanner::dump_all(int log_level) const
{
  int err = OB_SUCCESS;
  /// dump all result that will be send to ob client
  if (OB_SUCCESS == err && log_level >= TBSYS_LOG_LEVEL_DEBUG)
  {
    this->dump();
    // TODO: write new Scanner iterator
#if 0
    ObCellInfo *cell = NULL;
    bool row_change = false;
    ObNewScannerIterator iter = this->begin(); 
    while((iter != this->end()) && 
        (OB_SUCCESS == (err = iter.get_cell(&cell, &row_change))))
    {
      if (NULL == cell)
      {
        err = OB_ERROR;
        break;
      }
      if (row_change)
      {
        TBSYS_LOG(DEBUG, "dump cellinfo rowkey with hex format, length(%d)", cell->row_key_.length());
        hex_dump(cell->row_key_.ptr(), cell->row_key_.length(), true, TBSYS_LOG_LEVEL_DEBUG);
      }
      cell->value_.dump(TBSYS_LOG_LEVEL_DEBUG);
      ++iter;
    }
#endif
    
    if (err != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "fail to get scanner cell as debug output. ");
    }
  }
}
