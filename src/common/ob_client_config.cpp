/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 * 
 * Version: $Id$
 *
 * ob_client_config.cpp
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#include "ob_client_config.h"
#include "utility.h"
#include <tbsys.h>
using namespace oceanbase::common;
int ObClientConfig::serialize(char* buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = serialization::encode_vstr(buf, buf_len, pos, app_name_, strlen(app_name_))))
  {
    TBSYS_LOG(ERROR, "serialize error, len=%ld pos=%ld", buf_len, pos);
  }
  else if (OB_SUCCESS != (ret = obi_list_.serialize(buf, buf_len, pos)))
  {
    TBSYS_LOG(ERROR, "serialize error, len=%ld pos=%ld", buf_len, pos);
  }
  else if (OB_SUCCESS != (ret = serialization::encode_vi64(buf, buf_len, pos, BNL_alpha_)))
  {
    TBSYS_LOG(ERROR, "serialize error, len=%ld pos=%ld", buf_len, pos);
  }
  else if (OB_SUCCESS != (ret = serialization::encode_vi64(buf, buf_len, pos, BNL_alpha_denominator_)))
  {
    TBSYS_LOG(ERROR, "serialize error, len=%ld pos=%ld", buf_len, pos);
  }
  else if (OB_SUCCESS != (ret = serialization::encode_vi64(buf, buf_len, pos, BNL_threshold_)))
  {
    TBSYS_LOG(ERROR, "serialize error, len=%ld pos=%ld", buf_len, pos);
  }
  else if (OB_SUCCESS != (ret = serialization::encode_vi64(buf, buf_len, pos, BNL_threshold_denominator_)))
  {
    TBSYS_LOG(ERROR, "serialize error, len=%ld pos=%ld", buf_len, pos);
  }
  else if (OB_SUCCESS != (ret = serialization::encode_vi64(buf, buf_len, pos, reserve1_)))
  {
    TBSYS_LOG(ERROR, "serialize error, len=%ld pos=%ld", buf_len, pos);
  }
  else if (OB_SUCCESS != (ret = serialization::encode_vi64(buf, buf_len, pos, reserve2_)))
  {
    TBSYS_LOG(ERROR, "serialize error, len=%ld pos=%ld", buf_len, pos);
  }
  return ret;
}

int ObClientConfig::deserialize(const char* buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  memset(app_name_, 0, sizeof(app_name_));
  int64_t name_len = 0;
  if (NULL == serialization::decode_vstr(buf, data_len, pos, app_name_, OB_MAX_APP_NAME_LENGTH, &name_len))
  {
    TBSYS_LOG(ERROR, "deserialize error, len=%ld pos=%ld", data_len, pos);
    ret = OB_INVALID_ARGUMENT;
  }
  else if (OB_SUCCESS != (ret = obi_list_.deserialize(buf, data_len, pos)))
  {
    TBSYS_LOG(ERROR, "deserialize error, len=%ld pos=%ld", data_len, pos);
  }
  else if (OB_SUCCESS != (ret = serialization::decode_vi64(buf, data_len, pos, &BNL_alpha_)))
  {
    TBSYS_LOG(ERROR, "deserialize error, len=%ld pos=%ld", data_len, pos);
  }
  else if (OB_SUCCESS != (ret = serialization::decode_vi64(buf, data_len, pos, &BNL_alpha_denominator_)))
  {
    TBSYS_LOG(ERROR, "deserialize error, len=%ld pos=%ld", data_len, pos);
  }
  else if (OB_SUCCESS != (ret = serialization::decode_vi64(buf, data_len, pos, &BNL_threshold_)))
  {
    TBSYS_LOG(ERROR, "deserialize error, len=%ld pos=%ld", data_len, pos);
  }
  else if (OB_SUCCESS != (ret = serialization::decode_vi64(buf, data_len, pos, &BNL_threshold_denominator_)))
  {
    TBSYS_LOG(ERROR, "deserialize error, len=%ld pos=%ld", data_len, pos);
  }
  else if (OB_SUCCESS != (ret = serialization::decode_vi64(buf, data_len, pos, &reserve1_)))
  {
    TBSYS_LOG(ERROR, "deserialize error, len=%ld pos=%ld", data_len, pos);
  }
  else if (OB_SUCCESS != (ret = serialization::decode_vi64(buf, data_len, pos, &reserve2_)))
  {
    TBSYS_LOG(ERROR, "deserialize error, len=%ld pos=%ld", data_len, pos);
  }
  else 
  {
    app_name_[OB_MAX_APP_NAME_LENGTH - 1] = '\0';
  }
  return ret;
}

void ObClientConfig::print() const
{
  obi_list_.print();
  TBSYS_LOG(INFO, "app_name=%s", app_name_);
  TBSYS_LOG(INFO, "BNL_alpha=%ld", BNL_alpha_);
  TBSYS_LOG(INFO, "BNL_alpha_denominator=%ld", BNL_alpha_denominator_);
  TBSYS_LOG(INFO, "BNL_threshold=%ld", BNL_threshold_);
  TBSYS_LOG(INFO, "BNL_threshold_denominator=%ld", BNL_threshold_denominator_);
}

void ObClientConfig::print(char* buf, const int64_t buf_len, int64_t &pos) const 
{
  databuff_printf(buf, buf_len, pos, "app_name=%s\n", app_name_);
  databuff_printf(buf, buf_len, pos, "BNL_alpha=%ld\n", BNL_alpha_);
  databuff_printf(buf, buf_len, pos, "BNL_alpha_denominator=%ld\n", BNL_alpha_denominator_);
  databuff_printf(buf, buf_len, pos, "BNL_threshold=%ld\n", BNL_threshold_);
  databuff_printf(buf, buf_len, pos, "BNL_threshold_denominator=%ld\n", BNL_threshold_denominator_);
  obi_list_.print(buf, buf_len, pos);
}

