/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or 
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_rpc_stub.h for rpc among chunk server, update server and 
 * root server. 
 *
 * Authors:
 *   xielun <xielun.szd@taobao.com>
 *   huating <huating.zmq@taobao.com>
 *
 */
#ifndef OCEANBASE_CHUNKSERVER_RPC_STUB_H_
#define OCEANBASE_CHUNKSERVER_RPC_STUB_H_

#include "common/ob_server.h"
#include "common/ob_ups_info.h"

namespace oceanbase
{
  namespace common
  {
    class ObString;
    class ObScanner;
    class ObGetParam;
    class ObScanParam;
    class ObDataBuffer;
    class ObOperateResult;
    class ObSchemaManagerV2;
    class ObClientManager;
    class ThreadSpecificBuffer;
  }

  namespace chunkserver
  {
    // this class encapsulates network rpc interface as bottom layer,
    // and it only take charge of "one" rpc call. 
    // if u need other operational work, please use rpc_proxy for interaction
    class ObMergerRpcStub 
    {
    public:
      ObMergerRpcStub();
      virtual ~ObMergerRpcStub();
    
    public:
      // warning: rpc_buff should be only used by rpc stub for reset
      // param  @rpc_buff rpc send and response buff
      //        @rpc_frame client manger for network interaction
      int init(const common::ThreadSpecificBuffer * rpc_buffer, 
          const common::ObClientManager * rpc_frame);
      
      // get update server addr through root server rpc call
      // param  @timeout  action timeout
      //        @root_server root server addr
      //        @update_server output server addr
      //        @for_merge if get update server for daily merge
      int fetch_update_server(const int64_t timeout, const common::ObServer & root_server, 
          common::ObServer & update_server, bool for_merge = false) const;

      // get update server list for read through root server rpc call
      // param  @timeout  action timeout
      //        @root_server root server addr
      //        @ups_list output server addr list info
      int fetch_server_list(const int64_t timeout, const common::ObServer & root_server,
          common::ObUpsList & server_list) const;

      // get frozen time from update server according to frozen version
      // param  @timeout  action timeout
      //        @update_server output server addr
      //        @frozen_version frozen version to query
      //        @frozen_time frozen time which the frozen version creates
      int fetch_frozen_time(const int64_t timeout, common::ObServer & update_server, 
          int64_t frozen_version, int64_t& frozen_time) const;

      // get tables schema info through root server rpc call
      // param  @timeout  action timeout
      //        @root_server root server addr
      //        @timestamp  fetch cmd input param
      //        @schema fetch cmd output schema data
      int fetch_schema(const int64_t timeout, const common::ObServer & root_server, 
          const int64_t timestamp, common::ObSchemaManagerV2 & schema) const;

      // get tables schema newest version through root server rpc call
      // param  @timeout  action timeout
      //        @root_server root server addr
      //        @timestamp output new version 
      int fetch_schema_version(const int64_t timeout, const common::ObServer & root_server, 
          int64_t & timestamp) const;

    public: 
      // get row info through rpc call
      // param  @timeout  action timeout
      //        @server server addr
      //        @get_param get parameter
      //        @scanner  return result
      int get(const int64_t timeout, const common::ObServer & server, 
          const common::ObGetParam & get_param, common::ObScanner & scanner) const; 
            
      // scan row info through rpc call
      // param  @timeout  action timeout
      //        @server server addr
      //        @scan_get scan parameter
      //        @scanner  return result
      //        @end_flag is_end flag result
      int scan(const int64_t timeout, const common::ObServer & server, 
          const common::ObScanParam & scan_param, common::ObScanner & scanner) const;
          
    protected:
      // default cmd version
      static const int32_t DEFAULT_VERSION = 1;
      // for heartbeat cmd version
      static const int32_t NEW_VERSION = 2;
      
      // check inner stat
      virtual bool check_inner_stat(void) const;
      
      // get frame rpc data buffer from the thread buffer
      // waring:reset the buffer before serialize any packet
      int get_rpc_buffer(common::ObDataBuffer & data_buffer) const;
    
    private:
      bool init_;                                         // init stat for inner check
      const common::ThreadSpecificBuffer * rpc_buffer_;   // rpc thread buffer
      const common::ObClientManager * rpc_frame_;         // rpc frame for send request
    };

    // check inner stat
    inline bool ObMergerRpcStub::check_inner_stat(void) const
    {
      // check server and packet version
      return (init_ && (NULL != rpc_buffer_) && (NULL != rpc_frame_)); 
    }
  }
}

#endif // OCEANBASE_CHUNKSERVER_RPC_STUB_H_
