#ifndef _OB_MERGER_RS_RPC_PROXY_H_
#define _OB_MERGER_RS_RPC_PROXY_H_

#include "tbsys.h"
#include "common/ob_string.h"
#include "common/ob_server.h"

namespace oceanbase
{
  namespace common
  {
    class ObString;
    class ObServer;
    class ObSchemaManagerV2;
  };

  namespace mergeserver
  {
    // root server rpc proxy
    class ObMergerRpcStub;
    class ObMergerSchemaManager;
    class ObMergerTabletLocationList;
    class ObMergerTabletLocationCache;
    class ObMergerRootRpcProxy
    {
    public:
      ObMergerRootRpcProxy(const int64_t retry_times, const int64_t timeout,
          const common::ObServer & root);
      virtual ~ObMergerRootRpcProxy();
    
    public:
      // retry interval time
      static const int64_t RETRY_INTERVAL_TIME = 20; // 20 ms usleep
      ///
      int init(ObMergerRpcStub * rpc_stub);
    
      // register merge server
      // param  @merge_server localhost merge server addr
      int register_merger(const common::ObServer & merge_server);
      
      // merge server heartbeat with root server
      // param  @merge_server localhost merge server addr
      int async_heartbeat(const common::ObServer & merge_server);
    
      // fetch newest schema
      int fetch_newest_schema(ObMergerSchemaManager * schem_manager, 
          const common::ObSchemaManagerV2 ** manager);

      // fetch current schema version
      int fetch_schema_version(int64_t & timestamp);
    
    public:
      // scan tablet location through root_server rpc call
      // param  @table_id table id of root table 
      //        @row_key row key included in tablet range
      //        @location tablet location
      int scan_root_table(ObMergerTabletLocationCache * cache, 
          const uint64_t table_id, const common::ObString & row_key,
          const common::ObServer & server, ObMergerTabletLocationList & location);
    
    private:
      // check inner stat
      bool check_inner_stat(void) const;
    
    private:
      int64_t rpc_timeout_;                         // rpc call timeout
      int64_t rpc_retry_times_;                     // rpc retry times
      const ObMergerRpcStub * rpc_stub_;            // rpc stub bottom module
      common::ObServer root_server_;
    };

    inline bool ObMergerRootRpcProxy::check_inner_stat(void) const
    {
      return (rpc_stub_ != NULL);
    }
  }
}

#endif //_OB_MERGER_RS_RPC_PROXY_H_

