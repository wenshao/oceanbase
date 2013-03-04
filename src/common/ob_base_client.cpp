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

#include "ob_base_client.h"
using namespace oceanbase::common;

ObBaseClient::ObBaseClient()
  :init_(false)
{
}

ObBaseClient::~ObBaseClient()
{
  this->destroy();
}

int ObBaseClient::initialize(const ObServer& server)
{
  int ret = OB_ERROR;
  if (init_)
  {
    TBSYS_LOG(WARN, "already init");
    ret = OB_INIT_TWICE;
  }
  else
  {
    server_ = server;
    streamer_.setPacketFactory(&factory_);
    if (OB_SUCCESS != (ret = client_mgr_.initialize(&transport_, &streamer_)))
    {
      TBSYS_LOG(ERROR, "failed to init client_mgr, err=%d", ret);
    }
    else if (!transport_.start())
    {
      TBSYS_LOG(ERROR, "failed to start transport");
    }
    else
    {
      init_ = true;
    }
  }
  return ret;
}

void ObBaseClient::destroy()
{
  if (init_)
  { 
    if (!transport_.stop())
    {
      TBSYS_LOG(WARN, "failed to stop transport");
    }
    else if (!transport_.wait())
    {
      TBSYS_LOG(WARN, "failed to wait transport");
    }
  }
}

int ObBaseClient::send_recv(const int32_t pcode, const int32_t version, const int64_t timeout, 
                            ObDataBuffer& message_buff)
{
  return client_mgr_.send_request(server_, pcode, version, timeout, message_buff);
}

