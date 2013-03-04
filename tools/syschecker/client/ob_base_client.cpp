/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_base_client.cpp for define base oceanbase client. 
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */
#include "ob_base_client.h"

namespace oceanbase 
{ 
  namespace client 
  {
    using namespace common;

    int ObBaseClient::init()
    {
      int ret = OB_SUCCESS;

      streamer_.setPacketFactory(&factory_);
      ret = client_.initialize(&transport_, &streamer_);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "initialize client manager failed.");
      }

      return ret;
    }
    
    int ObBaseClient::start()
    {
      return (transport_.start() ? OB_SUCCESS : OB_ERROR);
    }
    
    int ObBaseClient::stop()
    {
      return (transport_.stop() ? OB_SUCCESS : OB_ERROR);
    }
    
    int ObBaseClient::wait()
    {
      return (transport_.wait() ? OB_SUCCESS : OB_ERROR);
    }
  } // end namespace client
} // end namespace oceanbase
