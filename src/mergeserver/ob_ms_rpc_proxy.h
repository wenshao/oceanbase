/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: ob_ms_schema_manager.h,v 0.1 2010/09/17 14:01:30 zhidong Exp $
 *
 * Authors:
 *   chuanhui <xielun.szd@taobao.com>
 *     - some work details if you want
 *
 */

#ifndef OB_MERGER_RPC_PROXY_H_
#define OB_MERGER_RPC_PROXY_H_

#include "tbsys.h"
#include "common/ob_string.h"
#include "common/ob_server.h"
#include "common/ob_ups_info.h"
#include "common/ob_iterator.h"
#include "ob_ms_read_balance.h"
#include "ob_ms_server_blacklist.h"

namespace oceanbase
{
  namespace common
  {
    class ObGetParam;
    class ObScanner;
    class ObMutator;
    class ObRange;
    class ObScanParam;
    class ObSchemaManagerV2;
  }

  namespace mergeserver
  {
    class ObMergerRpcStub;
    class ObMergerSchemaManager;
    class ObMergerTabletLocation;
    class ObMergerServiceMonitor;
    class ObMergerTabletLocationList;
    class ObMergerTabletLocationCache;
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
      ObMergerRpcProxy(const common::ObServerType type = common::MERGE_SERVER);
      // construct func
      // param  @retry_times rpc call retry times when error occured
      //        @timeout every rpc call timeout
      //        @root_server root server addr for some interface
      //        @mege_server merge server local host addr for input
      ObMergerRpcProxy(const int64_t retry_times, const int64_t timeout,
          const common::ObServer & root_server, const common::ObServer & merge_server,
          const common::ObServerType type = common::MERGE_SERVER);

      virtual ~ObMergerRpcProxy();

    public:
      // param  @rpc_buff rpc send and response buff
      //        @rpc_frame client manger for network interaction
      int init(ObMergerRpcStub * rpc_stub, ObMergerSchemaManager * schema,
               ObMergerTabletLocationCache * cache, ObMergerServiceMonitor * monitor = NULL);

      // set retry times and timeout
      int set_rpc_param(const int64_t retry_times, const int64_t timeout);

      // set black list params
      int set_blacklist_param(const int64_t timeout, const int64_t fail_count);

      // set min fetch update server list interval
      void set_min_interval(const int64_t interval);

      // register merge server
      // param  @merge_server localhost merge server addr
      int register_merger(const common::ObServer & merge_server);

      // merge server heartbeat with root server
      // param  @merge_server localhost merge server addr
      int async_heartbeat(const common::ObServer & merge_server);

      static const int64_t LOCAL_NEWEST = 0;    // local newest version

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
      void set_root_server(const common::ObServer & server)
      {
        root_server_ = server;
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
          ObMergerTabletLocation & addr, common::ObScanner & scanner, common::ObIterator * &it_out);

      // scan data from one chunk server, which is positioned according to the param
      // the outer class cellstream can give the whole data according to scan range param
      // param  @scan_param scan param
      //        @list tablet location list as output
      //        @scanner return result
      virtual int cs_scan(const common::ObScanParam & scan_param,
          ObMergerTabletLocation & addr, common::ObScanner & scanner, common::ObIterator * &it_out);

      // mutate to update server and fetch return data
      // param  @mutate_param mutate param
      // param  @has_data has data for scanner deserialize
      //        @scanner for return value
      virtual int ups_mutate(const common::ObMutator & mutate_param, const bool has_data, common::ObScanner & scanner);

      // get data from update server
      // param  @get_param get param
      //        @scanner return result
      virtual int ups_get(const ObMergerTabletLocation & addr,
          const common::ObGetParam & get_param, common::ObScanner & scanner);

      // scan data from update server
      // param  @scan_param get param
      //        @scanner return result
      virtual int ups_scan(const ObMergerTabletLocation & addr,
          const common::ObScanParam & scan_param, common::ObScanner & scanner);

      // lock and check whether need fetch new schema
      // param  @timestamp new schema timestamp
      //        @manager the new schema pointer
      int fetch_new_schema(const int64_t timestamp, const common::ObSchemaManagerV2 ** manager);
      //
      int fetch_new_version(int64_t & frozen_version);

      // get master update server
      int get_master_ups(const bool renew, common::ObServer & server);
      // WARNING: be cautious of this interface
      // set master update server if using vip for protocol compatible for cs merge
      void set_master_ups(const common::ObServer & server);

      // output scanner result for debug
      static void output(common::ObScanner & result);

    protected:
      // check inner stat
      bool check_inner_stat(void) const;

    private:
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

      // get search cache key
      // warning: the seach_key buffer is allocated in this function if it's not range.start_key_,
      // after use must dealloc search_key.ptr() in user space if temp is not null
      int get_search_key(const common::ObScanParam & scan_param, common::ObString & search_key, char ** new_buffer);

      // resest the search key's buffer to null after using it
      void reset_search_key(common::ObString & search_key, char * buffer);

      // del cache item according to tablet range
      int del_cache_item(const common::ObScanParam & scan_param);
      // del cache item according to table id + search rowkey
      int del_cache_item(const uint64_t table_id, const common::ObString & search_key);
      
      // update cache item according to tablet range
      int update_cache_item(const common::ObScanParam & scan_param, const ObMergerTabletLocationList & list);
      // update cache item according to table id + search rowkey
      int update_cache_item(const uint64_t table_id, const common::ObString & search_key, 
          const ObMergerTabletLocationList & list);

      // set item addr invalid according to tablet range
      int set_item_invalid(const common::ObScanParam & scan_param, const ObMergerTabletLocation & addr);
    public:
      // set item addr invalid according to table id + search rowkey
      int set_item_invalid(const uint64_t table_id, const common::ObString & search_key, 
          const ObMergerTabletLocation & list);

    private:
      // get tablet location according the table_id and row_key [include mode]
      // at first search the cache, then ask the root server, at last update the cache
      // param  @table_id table id of tablet
      //        @row_key row key included in tablet range
      //        @location tablet location
      int get_tablet_location(const uint64_t table_id, const common::ObString & row_key,
          ObMergerTabletLocationList & location);

    private:
      // get data from master update server
      // param  @get_param get param
      //        @scanner return result
      int master_ups_get(const ObMergerTabletLocation & addr,
          const common::ObGetParam & get_param, common::ObScanner & scanner);

      // get data from update server list
      // param  @get_param get param
      //        @scanner return result
      int slave_ups_get(const ObMergerTabletLocation & addr,
          const common::ObGetParam & get_param, common::ObScanner & scanner);

      // scan data from master update server
      // param  @get_param get param
      //        @scanner return result
      int master_ups_scan(const ObMergerTabletLocation & addr,
          const common::ObScanParam & scan_param, common::ObScanner & scanner);

      // scan data from update server list
      // param  @get_param get param
      //        @scanner return result
      int slave_ups_scan(const ObMergerTabletLocation & addr,
          const common::ObScanParam & scan_param, common::ObScanner & scanner);

      // find master update server from server list
      void find_master_ups(const common::ObUpsList & list, common::ObServer & master);

      // check and modify update server list
      void modify_ups_list(common::ObUpsList & list);

      // check the server is ok to select for read
      // not in blacklist and read percentage is gt 0
      bool check_server(const int32_t index);

    private:
      // get the first tablet location according range's first table_id.row_key [range.mode]
      // param  @range scan range
      //        @location the first range tablet location
      int get_first_tablet_location(const common::ObScanParam & param, common::ObString & search_key,
          char ** temp_buffer, ObMergerTabletLocationList & location);

      // scan tablet location through root_server rpc call
      // param  @table_id table id of root table 
      //        @row_key row key included in tablet range
      //        @location tablet location
      int scan_root_table(const uint64_t table_id, const common::ObString & row_key,
          ObMergerTabletLocationList & location);
      
      /// max len
      static const int64_t MAX_RANGE_LEN = 128;
      static const int64_t MAX_ROWKEY_LEN = 8;
    
    private:
      bool init_;                                   // rpc proxy init stat
      int64_t rpc_timeout_;                         // rpc call timeout
      int64_t rpc_retry_times_;                     // rpc retry times
      char max_range_[MAX_RANGE_LEN];               // for debug print range string
      char max_rowkey_[MAX_ROWKEY_LEN];             // 8 0xFF as max rowkey
      common::ObServer root_server_;                // root server addr
      common::ObServer merge_server_;               // merge server addr
      // update server list
      int64_t min_fetch_interval_;                  // min fetch update server interval
      int64_t fetch_ups_timestamp_;                 // last fetch update server timestamp
      common::ObServer master_update_server_;       // update server vip addr
      tbsys::CRWLock ups_list_lock_;                // lock for update server list
      tbsys::CThreadMutex update_lock_;             // lock for fetch update server info
      common::ObServerType server_type_;            // server type for different load balance
      uint64_t cur_finger_print_;                   // server list finger print
      int64_t fail_count_threshold_;                // pull to black list threshold times
      int64_t black_list_timeout_;                  // black list timeout for alive
      ObMergerServerBlackList black_list_;          // black list of update server
      common::ObUpsList update_server_list_;        // update server list for read
      // schema manager
      tbsys::CThreadMutex schema_lock_;             // lock for update schema manager
      int64_t fetch_schema_timestamp_;              // last fetch schema from root timestamp
      ObMergerSchemaManager * schema_manager_;      // merge server schema cache
      // location cache manager
      tbsys::CThreadMutex cache_lock_;              // lock for update chunk server cache
      ObMergerTabletLocationCache * tablet_cache_;  // merge server tablet location cache
      // system monitor
      ObMergerServiceMonitor * monitor_;            // service monitor data
      const ObMergerRpcStub * rpc_stub_;            // rpc stub bottom module
    };
  }
}



#endif // OB_MERGER_RPC_PROXY_H_
