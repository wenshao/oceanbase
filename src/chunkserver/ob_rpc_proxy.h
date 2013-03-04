/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or 
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_rpc_proxy.h for rpc among chunk server, update server and 
 * root server. 
 *
 * Authors:
 *   xielun <xielun.szd@taobao.com>
 *   huating <huating.zmq@taobao.com>
 *
 */
#ifndef OCEANBASE_CHUNKSERVER_RPC_PROXY_H_
#define OCEANBASE_CHUNKSERVER_RPC_PROXY_H_

#include "tbsys.h"
#include "common/ob_string.h"
#include "common/ob_server.h"
#include "common/ob_iterator.h"
#include "ob_read_ups_balance.h"
#include "ob_ups_blacklist.h"

namespace oceanbase
{
  namespace common
  {
    class ObGetParam;
    class ObScanner;
    class ObRange;
    class ObScanParam;
    class ObSchemaManagerV2;
    class ColumnFilter;
  }

  namespace chunkserver
  {
    class ObMergerRpcStub;
    class ObMergerSchemaManager;

    // this class is the outer interface class based on rpc stub class,
    // which is the real network interactor,
    // it encapsulates all interface for data getting operation, 
    // that sometime need rpc calling to get the right version data.
    // if u want to add a new interactive interface, 
    // please at first add the real detail at rpc stub class
    class ObMergerRpcProxy
    {
    public:
      //
      ObMergerRpcProxy();
      // construct func
      // param  @retry_times rpc call retry times when error occured
      //        @timeout every rpc call timeout
      //        @root_server root server addr for some interface
      //        @update_server update server addr for some interface
      ObMergerRpcProxy(const int64_t retry_times, const int64_t timeout, 
          const common::ObServer & root_server);
      
      virtual ~ObMergerRpcProxy();

    public:
      // param  @rpc_buff rpc send and response buff
      //        @rpc_frame client manger for network interaction
      int init(ObMergerRpcStub * rpc_stub, ObMergerSchemaManager * schema);

      // set retry times and timeout
      int set_rpc_param(const int64_t retry_times, const int64_t timeout);

      // set black list params
      int set_blacklist_param(const int64_t timeout, const int64_t fail_count);

      static const int64_t LOCAL_NEWEST = 0;    // local cation newest version

      // get the scheam data according to the timetamp, in some cases depend on the timestamp value
      // if timestamp is LOCAL_NEWEST, it meanse only get the local latest version
      // otherwise, it means that at first find in the local versions, if not exist then need rpc
      // waring: after using manager, u must release this schema version for washout
      // param  @timeout every rpc call timeout
      //        @manager the real schema pointer returned
      int get_schema(const int64_t timestamp, const common::ObSchemaManagerV2 ** manager);

      // fetch new schema if find new version
      int fetch_schema_version(int64_t & timestamp);

      // fetch update server list
      int fetch_update_server_list(int32_t & count);

      // waring: release schema after using for dec the manager reference count
      //        @manager the real schema pointer returned
      int release_schema(const common::ObSchemaManagerV2 * manager);

      // get master update server
      int get_update_server(const bool renew, common::ObServer & server);

      // some get func as temperary interface
      const ObMergerRpcStub * get_rpc_stub() const
      {
        return rpc_stub_;
      }
      int64_t get_rpc_timeout(void) const
      {
        return rpc_timeout_;
      }
      const common::ObServer & get_root_server(void) const
      {
        return root_server_;
      }

    public:  
      // retry interval time
      static const int64_t RETRY_INTERVAL_TIME = 20; // 20 ms usleep

      // least fetch schema time interval
      static const int64_t LEAST_FETCH_SCHEMA_INTERVAL = 1000 * 1000; // 1s 
      
      // get data from one chunk server, which is positioned according to the param
      // param  @get_param get param
      //        @list tablet location list as output
      //        @scanner return result
      virtual int cs_get(const common::ObGetParam & get_param, 
                         common::ObScanner & scanner, common::ObIterator *it_out[],int64_t& it_size);

      // scan data from one chunk server, which is positioned according to the param
      // the outer class cellstream can give the whole data according to scan range param
      // param  @scan_param scan param
      //        @list tablet location list as output
      //        @scanner return result

      virtual int cs_scan(const common::ObScanParam & scan_param, 
                          common::ObScanner & scanner, common::ObIterator *it_out[],
                          int64_t& it_size);

      // get data from update server
      // param  @get_param get param
      //        @scanner return result
      virtual int ups_get(const common::ObGetParam & get_param, 
                          common::ObScanner & scanner,
                          const common::ObServerType server_type = common::MERGE_SERVER,
                          const int64_t time_out = 0);

      // scan data from update server
      // param  @scan_param get param
      //        @scanner return result
      virtual int ups_scan(const common::ObScanParam & scan_param, 
                           common::ObScanner & scanner,
                           const common::ObServerType server_type = common::MERGE_SERVER,
                           const int64_t time_out = 0);

      // lock and check whether need fetch new schema
      // param  @timestamp new schema timestamp
      //        @manager the new schema pointer
      int fetch_new_schema(const int64_t timestamp, const common::ObSchemaManagerV2 ** manager);

      // get frozen time from update server
      // param  @frozen_version frozen version to query
      //        @frozen_time returned forzen time 
      int get_frozen_time(const int64_t frozen_version, int64_t& forzen_time);

      // get frozen schema from update server
      // param  @frozen_version frozen version to query
      //        @schema returned forzen schema 
      int get_frozen_schema(const int64_t frozen_version, common::ObSchemaManagerV2& schema);
      
      // output scanner result for debug
      static void output(common::ObScanner & result);

    private:
      // get data from master update server
      // param  @get_param get param
      //        @scanner return result
      int master_ups_get(const common::ObGetParam & get_param, 
                         common::ObScanner & scanner,
                         const int64_t time_out = 0);

      // get data from update server list
      // param  @get_param get param
      //        @scanner return result
      int slave_ups_get(const common::ObGetParam & get_param, 
                        common::ObScanner & scanner,
                        const common::ObServerType server_type,
                        const int64_t time_out = 0);

      // scan data from master update server
      // param  @get_param get param
      //        @scanner return result
      int master_ups_scan(const common::ObScanParam & scan_param, 
                          common::ObScanner & scanner,
                          const int64_t time_out = 0);

      // scan data from update server list
      // param  @get_param get param
      //        @scanner return result
      int slave_ups_scan(const common::ObScanParam & scan_param, 
                         common::ObScanner & scanner,
                         const common::ObServerType server_type,
                         const int64_t time_out = 0);

      // find master update server from server list
      void update_ups_info(const common::ObUpsList & list);

      // check and modify update server list
      void modify_ups_list(common::ObUpsList & list);

      // check the server is ok to select for read
      // not in blacklist and read percentage is gt 0
      bool check_server(const int32_t index, const common::ObServerType server_type);

    private:
      // check inner stat
      bool check_inner_stat(void) const; 
      
      // check scan param
      // param  @scan_param scan parameter
      static bool check_scan_param(const common::ObScanParam & scan_param);

      // check range param
      // param  @range_param range parameter 
      static bool check_range_param(const common::ObRange & range_param);

      // get new schema through root server rpc call
      // param  @timestamp old schema timestamp
      //        @manager the new schema pointer
      int get_new_schema(const int64_t timestamp, const common::ObSchemaManagerV2 ** manager);
      
      /// max len
      static const int64_t MAX_RANGE_LEN = 128;
      static const int64_t MAX_ROWKEY_LEN = 8;
    
    private:
      bool init_;                                   // rpc proxy init stat
      int64_t rpc_timeout_;                         // rpc call timeout
      int64_t rpc_retry_times_;                     // rpc retry times
      char max_range_[MAX_RANGE_LEN];               // for debug print range string
      common::ObServer root_server_;                // root server addr

      // update server
      int64_t min_fetch_interval_;                  // min fetch update server interval
      int64_t fetch_ups_timestamp_;                 // last fetch update server timestamp
      common::ObServer update_server_;              // update server addr
      common::ObServer inconsistency_update_server_;// inconsistency update server
      tbsys::CThreadMutex update_lock_;             // lock for fetch update server info
      const ObMergerRpcStub * rpc_stub_;            // rpc stub bottom module
      ObMergerSchemaManager * schema_manager_;      // merge server schema cache
      int64_t fetch_schema_timestamp_;              // last fetch schema from root timestamp
      tbsys::CThreadMutex schema_lock_;             // lock for update schema manager

      // update server list
      tbsys::CRWLock ups_list_lock_;                // lock for update server list
      uint64_t cur_finger_print_;                   // server list finger print
      int64_t fail_count_threshold_;                // pull to black list threshold times
      int64_t black_list_timeout_;                  // black list timeout for alive
      ObUpsBlackList black_list_;                   // black list of update server
      ObUpsBlackList ups_black_list_for_merge_;     // black list of update server
      common::ObUpsList update_server_list_;        // update server list for read
    };
  }
}

#endif // OCEANBASE_CHUNKSERVER_RPC_PROXY_H_
