/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: ob_ms_scan_cell_stream.h,v 0.1 2010/09/17 14:01:30 zhidong Exp $
 *
 * Authors:
 *   chuanhui <xielun.szd@taobao.com>
 *     - some work details if you want
 *
 */


#ifndef OB_MERGER_SCAN_CELL_STREAM_H_
#define OB_MERGER_SCAN_CELL_STREAM_H_

#include "common/ob_range.h"
#include "ob_ms_tablet_location_item.h"
#include "common/ob_read_common_data.h"
#include "ob_ms_cell_stream.h"

namespace oceanbase
{
  namespace mergeserver
  {
    // this is one of ObMSGetCellStream subclasses, it can provide cell stream through scan 
    // operation from chunkserver or update server according scan param.
    // it encapsulates many rpc calls when one server not serving all the required data or the
    // packet is too large
    class ObMSScanCellStream : public ObMSCellStream
    {
    public:
      ObMSScanCellStream(ObMergerRpcProxy * rpc_proxy);
      virtual ~ObMSScanCellStream();

    public: 
      // get next cell
      int next_cell(void);
      // scan init
      int scan(const common::ObScanParam & param,
               const ObMergerTabletLocation &cs_addr);

    private:

      // check whether finish scan, if finished return server's servering tablet range
      // param  @param current scan param
      //        @is_fullfill last rpc call return is fullfill
      //        @range current result ext info of tablet range
      int check_finish_scan(const common::ObScanParam & param, bool & is_fullfill); 

      // scan for get next cell
      int get_next_cell(void);

      // scan data
      // param @param scan data param
      int scan_row_data(common::ObScanParam & param);

      // check inner stat
      bool check_inner_stat(void) const; 

      // reset inner stat
      void reset_inner_stat(void);
    
    private:
      bool finish_;                             // finish all scan routine status
      common::ObMemBuffer range_buffer_;        // for modify param range
      const common::ObScanParam * scan_param_;  // orignal scan param
    };

    // check inner stat
    inline bool ObMSScanCellStream::check_inner_stat(void) const
    {
      return(ObMSCellStream::check_inner_stat() && (NULL != scan_param_));
    }

    // reset inner stat
    inline void ObMSScanCellStream::reset_inner_stat(void)
    {
      ObMSCellStream::reset_inner_stat();
      finish_ = false;
      scan_param_ = NULL;
    }
  }
}


#endif //OB_MERGER_SCAN_CELL_STRRAM_H_
