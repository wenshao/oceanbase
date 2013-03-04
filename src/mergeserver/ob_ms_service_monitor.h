
#ifndef OB_MS_SERVICE_MONITOR_H_
#define OB_MS_SERVICE_MONITOR_H_

#include "common/ob_statistics.h"

namespace oceanbase
{
  namespace mergeserver
  {
    class ObMergerServiceMonitor : public common::ObStatManager
    {
    public:
      ObMergerServiceMonitor(const int64_t timestamp);
      virtual ~ObMergerServiceMonitor();
    
    public:
      enum
      {
        // get
        SUCC_GET_OP_COUNT = 0,
        SUCC_GET_OP_TIME,
        FAIL_GET_OP_COUNT,
        FAIL_GET_OP_TIME,

        // scan
        SUCC_SCAN_OP_COUNT,
        SUCC_SCAN_OP_TIME,
        FAIL_SCAN_OP_COUNT,
        FAIL_SCAN_OP_TIME,

        // cache hit
        HIT_CS_CACHE_COUNT,
        MISS_CS_CACHE_COUNT,
        
        // cs version error
        FAIL_CS_VERSION_COUNT,

        // local query
        LOCAL_CS_QUERY_COUNT,
        REMOTE_CS_QUERY_COUNT,

        // scan counter for non-precision query
        SUCC_SCAN_NOT_PRECISION_OP_COUNT,
      };

    private:
      int64_t startup_timestamp_;
    };
  }
}


#endif //OB_MS_SERVICE_MONITOR_H_


