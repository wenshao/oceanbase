#ifndef _OB_MERGER_SCHEMA_PROXY_H_
#define _OB_MERGER_SCHEMA_PROXY_H_

#include "tbsys.h"

namespace oceanbase
{
  namespace common
  {
    class ObSchemaManagerV2;
  }

  namespace mergeserver
  {
    class ObMergerRootRpcProxy;
    class ObMergerSchemaManager;
    class ObMergerSchemaProxy
    {
    public:
      //
      ObMergerSchemaProxy();
      ObMergerSchemaProxy(ObMergerRootRpcProxy * rpc, ObMergerSchemaManager * schema);
      virtual ~ObMergerSchemaProxy();
    
    public:
      // check inner stat
      bool check_inner_stat(void) const;
      
      // least fetch schema time interval
      static const int64_t LEAST_FETCH_SCHEMA_INTERVAL = 1000 * 1000; // 1s 
      
      // local location newest version
      static const int64_t LOCAL_NEWEST = 0;
      // get the scheam data according to the version, in some cases depend on version value
      // if version is LOCAL_NEWEST, it meanse only get the local latest version
      // otherwise, it means that at first find in the local versions, if not exist then need rpc
      // waring: after using manager, u must release this schema version for washout
      // param  @version schema version
      //        @manager the real schema pointer returned
      int get_schema(const int64_t version, const common::ObSchemaManagerV2 ** manager);

      // waring: release schema after using for dec the manager reference count
      //        @manager the real schema pointer returned
      int release_schema(const common::ObSchemaManagerV2 * manager);
      
      // fetch new schema according to the version, if the last fetch timestamp is to nearly
      // return fail to avoid fetch too often from root server
      // param  @version schema version
      //        @manager the real schema pointer returned
      int fetch_schema(const int64_t version, const common::ObSchemaManagerV2 ** manager);

    private:
      int64_t fetch_schema_timestamp_;              // last fetch schema timestamp from root server
      tbsys::CThreadMutex schema_lock_;             // mutex lock for update schema manager
      ObMergerRootRpcProxy * root_rpc_;             // root server rpc proxy
      ObMergerSchemaManager * schema_manager_;      // merge server schema cache
    };
    
    inline bool ObMergerSchemaProxy::check_inner_stat(void) const
    {
      return ((root_rpc_ != NULL) && (schema_manager_ != NULL));
    }
  }
}


#endif // _OB_MERGER_SCHEMA_PROXY_H_
