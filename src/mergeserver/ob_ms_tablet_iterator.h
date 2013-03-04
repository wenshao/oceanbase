
#ifndef _OB_TABLET_LOCATION_RANGE_ITERATOR_H_
#define _OB_TABLET_LOCATION_RANGE_ITERATOR_H_

#include "ob_ms_tablet_location_proxy.h"
#include "common/ob_define.h"
#include "common/ob_range.h"
#include "common/ob_scan_param.h"
#include "common/ob_server.h"
#include "ob_ms_tablet_location_item.h"
#include "ob_chunk_server.h"
#include "common/ob_string_buf.h"

using namespace oceanbase::common;

namespace oceanbase
{
  namespace mergeserver
  {
    // get all the tablet according to given scan param
    class ObTabletLocationRangeIterator
    {
    public:
      ObTabletLocationRangeIterator();
      virtual ~ObTabletLocationRangeIterator();
    
    public:
      int initialize(ObMergerLocationCacheProxy * cache_proxy, const ObScanParam * scan_param_in,
        ObStringBuf * range_rowkey_buf);
      int next(ObChunkServer * replica_out, int32_t & replica_count_in_out, 
        ObRange & tablet_range_out);
      inline bool end() const
      {
        return is_iter_end_;
      }

    public:
      // intersect two range r1, r2. intersect result is r3
      // if the intersect result is empty. return OB_EMPTY_RANGE, r3 will NOT be changed
      // @ObRange r1 range for intersect
      // @ObRange r2 range for intersect
      // @ObRange r3 range for intersect result
      // @ObStringBuf range_rowkey_buf rowkey buf for r3
      int range_intersect(const ObRange& r1, const ObRange& r2, 
        ObRange& r3, ObStringBuf& range_rowkey_buf) const;

    private:
      bool init_;
      bool is_iter_end_;
      const ObScanParam * org_scan_param_;        //origin scan parameter
      ObRange next_range_;
      ObMergerLocationCacheProxy * cache_proxy_;
      ObStringBuf * range_rowkey_buf_;
    };
  }
}

#endif

