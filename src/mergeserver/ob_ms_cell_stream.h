/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: ob_ms_cell_stream.h,v 0.1 2010/09/17 14:01:30 zhidong Exp $
 *
 * Authors:
 *   chuanhui <xielun.szd@taobao.com>
 *     - some work details if you want
 *
 */

#ifndef OB_MERGER_CELL_STREAM_H_
#define OB_MERGER_CELL_STREAM_H_


#include "common/ob_scanner.h"
#include "common/ob_iterator.h"
#include "common/ob_read_common_data.h"
#include "common/ob_tablet_info.h"
#include "ob_cell_stream.h"
#include "ob_ms_rpc_proxy.h"

namespace oceanbase
{
  namespace mergeserver
  {
    // ObCellStream class is a virtual class as the interface
    // this is the parent class of GetCellStream and ScanCellStream for two different stream
    // types, it only provide common interfaces.
    class ObMSCellStream:public common::ObIterator
    {
    public:
      // construct
      // param  @rpc rpc proxy to provide endless stream
      ObMSCellStream(ObMergerRpcProxy * rpc);
      virtual ~ObMSCellStream();

    public:
      virtual void reset();

      // get the current cell info 
      // the same as parent interface
      virtual int get_cell(common::ObCellInfo** cell);

      // the same as parent interface
      virtual int get_cell(common::ObCellInfo** cell, bool * is_row_changed);

      // as the parent class, not implement the parent virtual function
      // the same as parent interface
      virtual int get(const common::ObReadParam & read_param,
          ObMSGetCellArray & get_cells, const ObMergerTabletLocation & cs_addr);

      // as the parent class, not implement the parent virtual function
      // the same as parent interface
      virtual int scan(const common::ObScanParam & scan_param, 
          const ObMergerTabletLocation & cs_addr); 

      // move to next for get cell
      virtual int next_cell(void) = 0;
    
    protected:
      // check init and scan or get
      virtual bool check_inner_stat(void) const;

      // reset inner stat
      virtual void reset_inner_stat(void);

      // one rpc call for scan the row data from server_type_ server
      // param  @scanner server returned data
      int rpc_scan_row_data(const common::ObScanParam & param);

      // one rpc call for get cell data from server_type_ server
      // param  @param get param 
      int rpc_get_cell_data(const common::ObGetParam & param);
      
    private:
      // check scanner result valid
      static int check_scanner_result(const common::ObScanner & result);

    protected:
      bool first_rpc_;                            // is the first rpc call
      common::ObCellInfo *last_cell_;             // last iterator cell
      common::ObCellInfo *cur_cell_;              // current iterator cell
      common::ObScanner cur_result_;              // rpc call returned temp result for iterator
      ObMergerRpcProxy * rpc_proxy_;              // rpc proxy for sys rpc call
      ObMergerTabletLocation cs_addr_;
    };

    // check inner stat
    inline bool ObMSCellStream::check_inner_stat(void) const
    {
      return(NULL != rpc_proxy_);
    }

    // reset innser stat 
    inline void ObMSCellStream::reset_inner_stat(void)
    {
      first_rpc_ = true;
      ///server_type_ = UNKNOW_SERVER;
      cur_result_.reset();
      // clear the data
      last_cell_ = NULL;
      cur_cell_ = NULL;
    }
  }
}


#endif //OB_MERGER_CELL_STREAM_H_
