/*
 * Copyright (C) 2007-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * Description here
 *
 * Version: $Id$
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *     - some work details here
 */

#include "ob_obi_config.h"
#include "common/serialization.h"
#include "common/utility.h"
#include "tblog.h"

using namespace oceanbase::common;
DEFINE_SERIALIZE(ObiConfig)
{
  int ret = OB_ERROR;
  if (OB_SUCCESS != (ret = serialization::encode_vi32(buf, buf_len, pos, static_cast<int32_t>(read_percentage_))))
  {
    TBSYS_LOG(ERROR, "serailization error, read_percentage_=%d", read_percentage_);
  }
  else if (OB_SUCCESS != (ret = serialization::encode_vi32(buf, buf_len, pos, static_cast<int32_t>(flag_))))
  {
    TBSYS_LOG(ERROR, "serialization error");
  }
  else if (OB_SUCCESS != (ret = serialization::encode_vi64(buf, buf_len, pos, static_cast<int32_t>(reserve2_))))
  {
    TBSYS_LOG(ERROR, "serialization error");
  }  
  
  return ret;
}

DEFINE_DESERIALIZE(ObiConfig)
{
  int ret = serialization::decode_vi32(buf, data_len, pos, &read_percentage_);
  if (OB_SUCCESS != ret)
  {
    TBSYS_LOG(ERROR, "deserialization error");
  }
  else if (OB_SUCCESS != (ret = serialization::decode_vi32(buf, data_len, pos, &flag_)))
  {
    TBSYS_LOG(ERROR, "deserialization error");
  }
  else if (OB_SUCCESS != (ret = serialization::decode_vi64(buf, data_len, pos, &reserve2_)))
  {
    TBSYS_LOG(ERROR, "deserialization error");
  }
  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObiConfig)
{
  return serialization::encoded_length_vi32(read_percentage_)
    + serialization::encoded_length_vi32(flag_)
    + serialization::encoded_length_vi64(reserve2_);
}

int ObiConfigEx::serialize(char* buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = ObiConfig::serialize(buf, buf_len, pos)))
  {
    TBSYS_LOG(ERROR, "serialize error, len=%ld pos=%ld", buf_len, pos);
  }
  else if (OB_SUCCESS != (ret = rs_addr_.serialize(buf, buf_len, pos)))
  {
    TBSYS_LOG(ERROR, "serialize error, len=%ld pos=%ld", buf_len, pos);
  }
  else if (OB_SUCCESS != (ret = serialization::encode_vi64(buf, buf_len, pos, reserve_)))
  {
    TBSYS_LOG(ERROR, "serialize error, len=%ld pos=%ld", buf_len, pos);
  }
  return ret;
}

int ObiConfigEx::deserialize(const char* buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = ObiConfig::deserialize(buf, data_len, pos)))
  {
    TBSYS_LOG(ERROR, "deserialize error, len=%ld pos=%ld", data_len, pos);
  }
  else if (OB_SUCCESS != (ret = rs_addr_.deserialize(buf, data_len, pos)))
  {
    TBSYS_LOG(ERROR, "deserialize error, len=%ld pos=%ld", data_len, pos);
  }
  else if (OB_SUCCESS != (ret = serialization::decode_vi64(buf, data_len, pos, &reserve_)))
  {
    TBSYS_LOG(ERROR, "deserialize error, len=%ld pos=%ld", data_len, pos);
  }
  return ret;
}

void ObiConfigEx::print() const
{
  char addr_buf[OB_IP_STR_BUFF];
  rs_addr_.to_string(addr_buf, OB_IP_STR_BUFF);
  TBSYS_LOG(INFO, "rs=%s read_percentage=%d rand_ms=%c", 
            addr_buf, read_percentage_, is_rand_ms()?'Y':'N');
}

void ObiConfigEx::print(char* buf, const int64_t buf_len, int64_t &pos) const
{
  char addr_buf[OB_IP_STR_BUFF];
  rs_addr_.to_string(addr_buf, OB_IP_STR_BUFF);
  databuff_printf(buf, buf_len, pos, "rs=%s read_percentage=%d rand_ms=%c|", 
                  addr_buf, read_percentage_, is_rand_ms()?'Y':'N');
}

int ObiConfigList::serialize(char* buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = serialization::encode_vi32(buf, buf_len, pos, obi_count_)))
  {
    TBSYS_LOG(ERROR, "serialize error, len=%ld pos=%ld", buf_len, pos);
  }
  else if (OB_SUCCESS != (ret = serialization::encode_vi32(buf, buf_len, pos, reserve_)))
  {
    TBSYS_LOG(ERROR, "serialize error, len=%ld pos=%ld", buf_len, pos);
  }
  else
  {
    for (int32_t i = 0; i < obi_count_; ++i)
    {
      if (OB_SUCCESS != (ret = conf_array_[i].serialize(buf, buf_len, pos)))
      {
        TBSYS_LOG(ERROR, "serialize error, len=%ld pos=%ld", buf_len, pos);
        break;
      }
    }
  }
  return ret;
}

int ObiConfigList::deserialize(const char* buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = serialization::decode_vi32(buf, data_len, pos, &obi_count_)))
  {
    TBSYS_LOG(ERROR, "deserialize error, len=%ld pos=%ld", data_len, pos);
  }
  else if (OB_SUCCESS != (ret = serialization::decode_vi32(buf, data_len, pos, &reserve_)))
  {
    TBSYS_LOG(ERROR, "deserialize error, len=%ld pos=%ld", data_len, pos);
  }
  else
  {
    for (int32_t i = 0; i < obi_count_ && i < MAX_OBI_COUNT; ++i)
    {
      if (OB_SUCCESS != (ret = conf_array_[i].deserialize(buf, data_len, pos)))
      {
        TBSYS_LOG(ERROR, "deserialize error, len=%ld pos=%ld", data_len, pos);
        break;
      }
    }
  }
  return ret;
}

void ObiConfigList::print() const
{
  TBSYS_LOG(INFO, "obi_count=%d", obi_count_);
  for (int i = 0; i < obi_count_; ++i)
  {
    conf_array_[i].print();
  }
}

void ObiConfigList::print(char* buf, const int64_t buf_len, int64_t &pos) const
{
  databuff_printf(buf, buf_len, pos, "obi_count_=%d, obi_list: \n", obi_count_);
  for (int i = 0; i < obi_count_; ++i)
  {
    conf_array_[i].print(buf, buf_len, pos);
  }
}
