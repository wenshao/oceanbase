/*
 *  (C) 2007-2010 Taobao Inc.
 *  
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License version 2 as
 *  published by the Free Software Foundation.
 *
 *         base_client.cpp is for what ...
 *
 *  Version: $Id: base_client.cpp 2010年11月16日 13时29分40秒 qushan Exp $
 *
 *  Authors:
 *     qushan < qushan@taobao.com >
 *        - some work details if you want
 */


#include "base_client.h"
using namespace oceanbase::common;

int BaseClient::initialize()
{
  streamer_.setPacketFactory(&factory_);
  int ret = client_.initialize(&transport_, &streamer_);
  if (OB_SUCCESS != ret)
  {
    TBSYS_LOG(ERROR, "initialize client manager failed.");
  }
  return ret;
}

int BaseClient::start()
{
  transport_.start();
  return 0;
}

int BaseClient::stop()
{
  transport_.stop();
  return 0;
}

int BaseClient::wait()
{
  transport_.wait();
  return 0;
}

