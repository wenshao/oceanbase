/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_server_rpc.h for define API of merge server, 
 * chunk server, update server, root server rpc. 
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */
#ifndef OCEANBASE_CLIENT_OB_SERVER_RPC_H_
#define OCEANBASE_CLIENT_OB_SERVER_RPC_H_

#include "common/ob_schema.h"
#include "common/ob_server.h"
#include "common/ob_tablet_info.h"
#include "common/thread_buffer.h"
#include "common/data_buffer.h"
#include "common/ob_client_manager.h"
#include "common/ob_packet.h"
#include "common/ob_read_common_data.h"
#include "common/thread_buffer.h"
#include "common/ob_scanner.h"
#include "common/ob_mutator.h"

namespace oceanbase 
{
  namespace client 
  {
    class ObServerRpc
    {
    public:
      static const int64_t FRAME_BUFFER_SIZE = 2 * 1024 * 1024L;
  
      ObServerRpc();
      ~ObServerRpc();
  
    public:
      // warning: rpc_buff should be only used by rpc stub for reset
      int init(const common::ObClientManager* rpc_frame);

      int fetch_schema(const common::ObServer& root_server,
                       const int64_t timestap, 
                       common::ObSchemaManagerV2& schema_mgr,
                       const int64_t timeout);

      int fetch_update_server(const common::ObServer& root_server,
                              common::ObServer& update_server,
                              const int64_t timeout);
  
      int scan(const common::ObServer& remote_server, 
               const common::ObScanParam& scan_param,
               common::ObScanner& scanner,
               const int64_t timeout);
  
      int get(const common::ObServer& remote_server, 
              const common::ObGetParam& get_param,
              common::ObScanner& scanner,
              const int64_t timeout);

      int ups_apply(const common::ObServer& update_server,
                    const common::ObMutator &mutator, 
                    const int64_t timeout);
  
    private:
      static const int32_t DEFAULT_VERSION = 1;
  
      int get_frame_buffer(common::ObDataBuffer& data_buffer) const;
  
    private:
      common::ThreadSpecificBuffer frame_buffer_;
      const common::ObClientManager* rpc_frame_;  // rpc frame for send request
    };
  } // namespace oceanbase::client
} // namespace Oceanbase

#endif // OCEANBASE_CLIENT_OB_SERVER_RPC_H_
