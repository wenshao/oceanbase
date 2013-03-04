/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_base_client.h for define base oceanbase client. 
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */
#ifndef OCEANBASE_CLIENT_OB_BASE_CLIENT_H_
#define OCEANBASE_CLIENT_OB_BASE_CLIENT_H_

#include <tbnet.h>
#include "common/ob_packet.h"
#include "common/ob_packet_factory.h"
#include "common/ob_client_manager.h"

namespace oceanbase 
{
  namespace client 
  {
    class ObBaseClient
    {
    public:
      ObBaseClient()
      {

      }

      virtual ~ObBaseClient()
      {

      }

      virtual int init();
      virtual int start();
      virtual int stop();
      virtual int wait();

      inline const common::ObClientManager& get_client_manager() const
      {
        return client_; 
      }

    private:
      DISALLOW_COPY_AND_ASSIGN(ObBaseClient);

      tbnet::DefaultPacketStreamer streamer_;
      tbnet::Transport transport_;
      common::ObPacketFactory factory_;
      common::ObClientManager client_;
    };
  } // namespace oceanbase::client
} // namespace Oceanbase

#endif //OCEANBASE_CLIENT_OB_BASE_CLIENT_H_
