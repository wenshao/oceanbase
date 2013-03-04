/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: ob_ms_rpc_stub.h,v 0.1 2010/09/17 14:01:30 zhidong Exp $
 *
 * Authors:
 *   zhidong <xielun.szd@taobao.com>
 *     - some work details if you want
 *
 */

#ifndef OCEANBASE_MERGER_RPC_STUB_H_
#define OCEANBASE_MERGER_RPC_STUB_H_

#include "common/ob_server.h"
#include "common/ob_ups_info.h"

namespace oceanbase
{
  namespace common
  {
    class ObString;
    class ObMutator;
    class ObScanner;
    class ObGetParam;
    class ObScanParam;
    class ObDataBuffer;
    class ObOperateResult;
    class ObSchemaManagerV2;
    class ObClientManager;
    class ThreadSpecificBuffer;
  }

  namespace mergeserver
  {
    // this class encapsulates network rpc interface as bottom layer,
    // and it only take charge of "one" rpc call. 
    // if u need other operational work, please use rpc_proxy for interaction
    class ObMergerTabletLocation;
    class ObMergerTabletLocationList;
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
      
      // register to root server as a merge server by rpc call
      // param  @timeout  action timeout
      //        @root_server root server addr
      //        @merge_server merge server addr
      //        @is_merger merge server status
      int register_server(const int64_t timeout, const common::ObServer & root_server, 
          const common::ObServer & merge_server, const bool is_merger) const;
      
      // heartbeat to root server for alive
      // param  @timeout  action timeout
      //        @root_server root server addr
      //        @merge_server merge server addr
      //        @server_role server role
      int heartbeat_server(const int64_t timeout, const common::ObServer & root_server,
          const common::ObServer & merge_server, const common::ObRole server_role) const;

      // get update server vip addr through root server rpc call
      // param  @timeout  action timeout
      //        @root_server root server addr
      //        @update_server output server addr
      int find_server(const int64_t timeout, const common::ObServer & root_server,
          common::ObServer & update_server) const;

      // get update server list for read through root server rpc call
      // param  @timeout  action timeout
      //        @root_server root server addr
      //        @ups_list output server addr list info
      int fetch_server_list(const int64_t timeout, const common::ObServer & root_server,
          common::ObUpsList & server_list) const;

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
      //
      int fetch_frozen_version(const int64_t timeout, const common::ObServer & ups_server,
          int64_t & version) const;

    public:
      // get tablet location info through root server rpc call
      // param  @timeout  action timeout
      //        @root_server root server addr
      //        @root_table root table name
      //        @table_id look up table id
      //        @row_key look up row key
      //        @scanner scaned tablets location result set
      int fetch_tablet_location(const int64_t timeout, const common::ObServer & root_server, 
          const uint64_t root_table_id, const uint64_t table_id,
          const common::ObString & row_key, common::ObScanner & scanner) const;
      
      // get row info through rpc call
      // param  @timeout  action timeout
      //        @server server addr
      //        @get_param get parameter
      //        @scanner  return result
      int get(const int64_t timeout, const common::ObServer & server, 
          const common::ObGetParam & get_param, common::ObScanner & scanner) const; 
      
      // sort the locationlist asc by distance between input server ip
      // retry another server in the list when the previous one failed
      // waring: timeout is every rpc call time
      // param  @timeout  action timeout
      //        @list sorted location list
      //        @get_param get parameter
      //        @succ_addr get succ response server
      //        @scanner return result
      //        @update_list location list modified status
      int get(const int64_t timeout, ObMergerTabletLocationList & list,
          const common::ObGetParam & get_param, ObMergerTabletLocation & succ_addr,
          common::ObScanner & scanner, bool & update_list) const;

      // scan row info through rpc call
      // param  @timeout  action timeout
      //        @server server addr
      //        @scan_param scan parameter
      //        @scanner  return result
      int scan(const int64_t timeout, const common::ObServer & server,
          const common::ObScanParam & scan_param, common::ObScanner & scanner) const;

      // sort the locationlist asc by distance between input server ip
      // retry another server in the list when the previous one failed
      // waring: timeout is every rpc call time
      // param  @timeout  action timeout
      //        @server input server addr
      //        @list sorted location list
      //        @scan_param scan parameter
      //        @succ_addr scan succ response server
      //        @scanner return result
      //        @update_list location list modified status
      int scan(const int64_t timeout, ObMergerTabletLocationList & list,
          const common::ObScanParam & scan_param, ObMergerTabletLocation & succ_addr,
          common::ObScanner & scanner, bool & update_list) const;

      // apply mutator through rpc call
      // param  @timeout rpc timeout
      //        @server server addr
      //        @mutator parameter
      //        @has_data return data
      //        @scanner return result
      int mutate(const int64_t timeout, const common::ObServer & server,
          const common::ObMutator & mutate_param, const bool has_data, common::ObScanner & scanner) const;

      // reload config for server it self
      // param  @timeout rpc timeout
      //        @ merge_server  server addr
      //        @ filename config file name to reload
      int reload_self_config(const int64_t timeout, const common::ObServer & merge_server, 
          const char *filename) const;
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


#endif // OCEANBASE_MERGER_RPC_STUB_H_
