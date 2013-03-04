#ifndef _OB_MS_TABLET_LOCATION_PROXY_H_
#define _OB_MS_TABLET_LOCATION_PROXY_H_

#include "tbsys.h"
#include "ob_chunk_server.h"
#include "common/ob_array_lock.h"
#include "common/ob_scan_param.h"
#include "common/ob_server.h"

namespace oceanbase
{
  namespace common
  {
    class ObString;
    class ObScanParam;
  }

  namespace mergeserver
  {
    class ObMergerRootRpcProxy;
    class ObMergerTabletLocation;
    class ObMergerServiceMonitor;
    class ObMergerTabletLocationList;
    class ObMergerTabletLocationCache;
    class ObMergerLocationCacheProxy
    {
    public:
      ObMergerLocationCacheProxy();

      ObMergerLocationCacheProxy(const common::ObServer & server,
        ObMergerRootRpcProxy * rpc, ObMergerTabletLocationCache * cache,
        ObMergerServiceMonitor * monitor = NULL);

      virtual ~ObMergerLocationCacheProxy();

    public:
      // init for lock holder
      int init(const uint64_t lock_count);

      // get server for localhost query statistic
      const common::ObServer & get_server(void) const
      {
        return merge_server_;
      }

      // get tablet location according the table_id and row_key [include mode]
      // at first search the cache, then ask the root server, at last update the cache
      // param  @table_id table id of tablet
      //        @row_key row key included in tablet range
      //        @location tablet location
      int get_tablet_location(const uint64_t table_id, const common::ObString & row_key,
        ObMergerTabletLocationList & location);

      // get the first tablet location according range's first table_id.row_key [range.mode]
      // param  @range scan range
      //        @location the first range tablet location
      int get_tablet_location(const common::ObScanParam & param,
        ObMergerTabletLocationList & location);

      int get_tablet_location(const common::ObScanParam::Direction scan_direction,
        const common::ObRange * range, ObMergerTabletLocationList & location);

    public:
      // del cache item according to tablet range
      int del_cache_item(const common::ObScanParam & scan_param);

      // del cache item according to table id + search rowkey
      int del_cache_item(const uint64_t table_id, const common::ObString & search_key);

      // update cache item according to tablet range
      int update_cache_item(const common::ObScanParam & scan_param,
        const ObMergerTabletLocationList & list);

      // update cache item according to table id + search rowkey
      int update_cache_item(const uint64_t table_id, const common::ObString & search_key, 
        const ObMergerTabletLocationList & list);

      // set item addr invalid according to tablet range
      int set_item_invalid(const common::ObScanParam & scan_param,
        const ObMergerTabletLocation & addr, ObMergerTabletLocationList & list);

      // set item addr invalid according to table id + search rowkey
      int set_item_invalid(const uint64_t table_id, const common::ObString & search_key,
        const ObMergerTabletLocation & addr, ObMergerTabletLocationList & list);

      int server_fail(const common::ObScanParam & scan_param,
        ObMergerTabletLocationList & list, const oceanbase::common::ObServer & server);

      int server_fail(const uint64_t table_id, const common::ObString & search_key,
        ObMergerTabletLocationList & list, const oceanbase::common::ObServer & server);
      // dump info
      void dump(void) const;
    private:
      // check inner stat
      bool check_inner_stat(void) const;

      // collect hit cache ratio monitor statistic data
      void inc_cache_monitor(const uint64_t table_id, const bool hit_cache);

      // get search cache key
      // warning: the seach_key buffer is allocated if the search key != range.start_key_,
      // after use must dealloc search_key.ptr() in user space if temp is not null
      int get_search_key(const common::ObScanParam & scan_param, common::ObString & search_key,
        char ** new_buffer);

      // get search key for range and scan param
      int get_search_key(const common::ObScanParam::Direction scan_direction, const common::ObRange * range,
        common::ObString & search_key, char ** new_buffer);

      // resest the search key's buffer to null after using it
      void reset_search_key(common::ObString & search_key, char * buffer);

      // get and fetch new item if root server is ok and insert the items
      int renew_location_item(const uint64_t table_id, const common::ObString & row_key,
        ObMergerTabletLocationList & location);

      // update timetout cache item if root server is ok and can refresh the items
      int update_timeout_item(const uint64_t table_id, const common::ObString & row_key,
        ObMergerTabletLocationList & location);

      /// max len
      static const int64_t MAX_ROWKEY_LEN = 8;

    private:
      // monitor info
      mutable uint64_t cur_succ_count_;
      mutable uint64_t cur_fail_count_;
      mutable uint64_t old_succ_count_;
      mutable uint64_t old_fail_count_;
    private:
      char max_rowkey_[MAX_ROWKEY_LEN];             // 8 0xFF as max rowkey
      common::ObServer merge_server_;               // merge server addr for sort
      ObMergerRootRpcProxy * root_rpc_;             // root server rpc proxy
      ObMergerServiceMonitor * cache_monitor_;      // cache hit ratio monitor
      ObMergerTabletLocationCache * tablet_cache_;  // merge server tablet location cache
      common::ObSequenceLock lock_holder_;          // sequence lock holder
    };

    inline bool ObMergerLocationCacheProxy::check_inner_stat(void) const
    {
      return((tablet_cache_ != NULL) && (root_rpc_ != NULL));
    }

    inline int ObMergerLocationCacheProxy::init(const uint64_t count)
    {
      return lock_holder_.init(count);
    }
  }
}



#endif // _OB_MS_TABLET_LOCATION_PROXY_H_


