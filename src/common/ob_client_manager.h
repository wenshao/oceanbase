/*
 *  (C) 2007-2010 Taobao Inc.
 *  
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License version 2 as
 *  published by the Free Software Foundation.
 *
 *         ????.cpp is for what ...
 *
 *  Version: $Id: ipvsadm.c,v 1.27 2005/12/10 16:00:07 wensong Exp $
 *
 *  Authors:
 *     qushan <qushan@taobao.com>
 *        - some work details if you want
 */
#ifndef OCEANBASE_COMMON_CLIENT_MANAGER_H_
#define OCEANBASE_COMMON_CLIENT_MANAGER_H_

#include "tbnet.h"

#include "data_buffer.h"
#include "ob_server.h"
#include "ob_packet.h"

namespace oceanbase 
{ 

  namespace common
  {
    class WaitObjectManager;
    class ObClientManager : public tbnet::IPacketHandler
    {
      public:
        ObClientManager();
        ~ObClientManager();
      public:
        int initialize(tbnet::Transport *transport, tbnet::IPacketStreamer* streamer, 
            const int64_t max_request_timeout = 5000000);

        void set_error(const int err);
        /**
         * post request (%in_buffer) to %server, and do not care repsonse from %server.
         */
        int post_request(const ObServer& server,  const int32_t pcode, const int32_t version,
            const ObDataBuffer& in_buffer) const;

        /**
         * post request (%in_buffer) to %server, and handle response to %handler
         */
        int post_request(const ObServer& server, const int32_t pcode, const int32_t version,
            const int64_t timeout, const ObDataBuffer& in_buffer, tbnet::IPacketHandler* handler, void* args) const;

        /**
         * send request (%in_buffer) to %server, and wait until %server response
         * (parse to %out_buffer) or timeout.
         */
        int send_request(const ObServer& server, const int32_t pcode, const int32_t version, 
            const int64_t timeout, ObDataBuffer& in_buffer, ObDataBuffer& out_buffer) const;

        /**
         * send request (%in_out_buffer) to %server, and wait until %server response 
         * (parse to %in_out_buffer) or timeout.
         * use one buffer %in_out_buffer for store request and response packet;
         */
        int send_request(const ObServer& server, const int32_t pcode, const int32_t version, 
            const int64_t timeout, ObDataBuffer& in_out_buffer) const;

        /**
         * same as above send_packet, but server may response several times. 
         * client can get following response by use get_next on this %session_id
         */
        int send_request(const ObServer& server, const int32_t pcode, const int32_t version, 
            const int64_t timeout, ObDataBuffer& in_buffer, ObDataBuffer& out_buffer, 
            int64_t& session_id) const;
        /**
         * use one buffer %in_out_buffer for store request and response packet;
         */
        int send_request(const ObServer& server, const int32_t pcode, const int32_t version, 
            const int64_t timeout, ObDataBuffer& in_out_buffer, int64_t& session_id) const;

        /**
         * send a special NEXT packet to server for get following response on %session_id
         */
        int get_next(const ObServer& server, const int64_t session_id, 
            const int64_t timeout, ObDataBuffer& in_buffer, ObDataBuffer& out_buffer) const;

        int post_next(const ObServer& server, const int64_t session_id, const int64_t timeout, 
            ObDataBuffer& in_buffer, tbnet::IPacketHandler* handler, void* args) const;
        int post_end_next(const ObServer& server, const int64_t session_id, const int64_t timeout, 
            ObDataBuffer& in_buffer, tbnet::IPacketHandler* handler, void* args) const;

        tbnet::IPacketHandler::HPRetCode handlePacket(tbnet::Packet* packet, void * args);
      private:
        int do_post_request(const ObServer& server, 
            const int32_t pcode, const int32_t version, 
            const int64_t session_id, const int64_t timeout,
            const ObDataBuffer& in_buffer, 
            tbnet::IPacketHandler* handler, void* args) const;
        int do_post_packet(const ObServer& server, ObPacket* packet, 
            tbnet::IPacketHandler* handler, void* args) const;
        int do_send_packet(const ObServer& server, ObPacket* packet, 
            const int64_t timeout, ObPacket* &response) const;
        int do_send_request(const ObServer& server, const int32_t pcode, const int32_t version, 
            const int64_t timeout, ObDataBuffer& in_buffer, ObPacket* &response) const;
        void destroy();
      private:
        int error_;
        int32_t inited_;
        mutable int64_t max_request_timeout_;
        tbnet::ConnectionManager* connmgr_;
        WaitObjectManager* waitmgr_;
    };

  } // end namespace chunkserver
} // end namespace oceanbase


#endif //OCEANBASE_COMMON_CLIENT_MANAGER_H_

