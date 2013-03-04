#ifndef OCEANBASE_MERGER_VERSION_PROXY_H_
#define OCEANBASE_MERGER_VERSION_PROXY_H_

#include "tbsys.h"
#include "common/ob_define.h"
#include "common/ob_server.h"

namespace oceanbase
{
  namespace mergeserver
  {
    class ObMergerRpcProxy;
    class ObMergerVersionProxy
    {
    public:
      ObMergerVersionProxy(const int64_t timeout);
      virtual ~ObMergerVersionProxy();

    public:
      static const int64_t DEFAULT_TIMEOUT = 10 * 60 * 1000 * 1000L; // 10 mins
      /// set data version timeout
      void set_timeout(const int64_t timeout);

      /// init the update server and rpc proxy
      int init(ObMergerRpcProxy * rpc);

      /// get frozen version if timeout fetch again
      int get_version(int64_t & version);

    private:
      int64_t timeout_;
      tbsys::CThreadMutex lock_;
      int64_t last_timestamp_;
      int64_t last_frozen_version_;
      ObMergerRpcProxy * rpc_proxy_;
    };
  }
}


#endif //OCEANBASE_MERGER_VERSION_PROXY_H_

