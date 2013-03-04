/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 * 
 * Version: $Id$
 *
 * ob_rs_ups_message.h
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#ifndef _OB_RS_UPS_MESSAGE_H
#define _OB_RS_UPS_MESSAGE_H 1
#include "ob_server.h"
#include "data_buffer.h"
#include "ob_result.h"
#include "ob_obi_role.h"
namespace oceanbase
{
  namespace common
  {
    struct ObMsgUpsHeartbeat
    {
      static const int MY_VERSION = 2;
      ObServer ups_master_;
      int64_t self_lease_;
      ObiRole obi_role_;
      int serialize(char* buf, const int64_t buf_len, int64_t& pos) const;
      int deserialize(const char* buf, const int64_t data_len, int64_t& pos);
    };

    struct ObMsgUpsHeartbeatResp // aka UPS renew message
    {
      static const int MY_VERSION = 2;
      ObServer addr_;
      enum UpsSyncStatus
      {
        SYNC = 0,
        NOTSYNC = 1
      } status_;
      ObiRole obi_role_;
      int serialize(char* buf, const int64_t buf_len, int64_t& pos) const;
      int deserialize(const char* buf, const int64_t data_len, int64_t& pos);
    };

    struct ObMsgUpsRegister
    {
      static const int MY_VERSION = 1;
      ObServer addr_;
      int32_t inner_port_;
      int64_t log_seq_num_;
      int64_t lease_;
      int serialize(char* buf, const int64_t buf_len, int64_t& pos) const;
      int deserialize(const char* buf, const int64_t data_len, int64_t& pos);
    };

    struct ObMsgUpsSlaveFailure
    {
      static const int MY_VERSION = 1;
      ObServer my_addr_;
      ObServer slave_addr_;
      int serialize(char* buf, const int64_t buf_len, int64_t& pos) const;
      int deserialize(const char* buf, const int64_t data_len, int64_t& pos);
    };

    struct ObMsgRevokeLease
    {
      static const int MY_VERSION = 1;
      int64_t lease_;
      ObServer ups_master_;      
      int serialize(char* buf, const int64_t buf_len, int64_t& pos) const;
      int deserialize(const char* buf, const int64_t data_len, int64_t& pos);
    };
  } // end namespace common
} // end namespace oceanbase

#endif /* _OB_RS_UPS_MESSAGE_H */

