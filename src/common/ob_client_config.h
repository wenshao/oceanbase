/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 * 
 * Version: $Id$
 *
 * ob_client_config.h
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#ifndef _OB_CLIENT_CONFIG_H
#define _OB_CLIENT_CONFIG_H 1
#include "common/ob_define.h"
#include "common/ob_obi_config.h"

namespace oceanbase
{
  namespace common
  {
    struct ObClientConfig
    {
      char app_name_[OB_MAX_APP_NAME_LENGTH];
      ObiConfigList obi_list_;
      int64_t BNL_alpha_;
      int64_t BNL_alpha_denominator_;
      int64_t BNL_threshold_;
      int64_t BNL_threshold_denominator_;
      int64_t reserve1_;
      int64_t reserve2_;

      ObClientConfig();
      void print() const;
      void print(char* buf, const int64_t buf_len, int64_t &pos) const;
      NEED_SERIALIZE_AND_DESERIALIZE;
      static const int64_t DEFAULT_BNL_ALPHA = 999900LL;
      static const int64_t DEFAULT_BNL_ALPHA_DENOMINATOR = 1000000LL;
      static const int64_t DEFAULT_BNL_THRESHOLD = 47630LL;
      static const int64_t DEFAULT_BNL_THRESHOLD_DENOMINATOR = 1000LL;
    };

    inline ObClientConfig::ObClientConfig()
      :BNL_alpha_(DEFAULT_BNL_ALPHA), 
       BNL_alpha_denominator_(DEFAULT_BNL_ALPHA_DENOMINATOR),
       BNL_threshold_(DEFAULT_BNL_THRESHOLD), 
       BNL_threshold_denominator_(DEFAULT_BNL_THRESHOLD_DENOMINATOR),
       reserve1_(0), reserve2_(0)
    {
      memset(app_name_, 0, sizeof(app_name_));
    }
  } // end namespace common
} // end namespace oceanbase

#endif /* _OB_CLIENT_CONFIG_H */

