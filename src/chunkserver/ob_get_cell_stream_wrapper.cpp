/**
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or 
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_get_cell_stream_wrapper.cpp is for concealing 
 * ObMergerRpcProxy initialization details from chunkserver 
 *
 * Authors:
 *   wushi <wushi.ly@taobao.com>
 *   huating <huating.zmq@taobao.com>
 *
 */
#include "ob_get_cell_stream_wrapper.h"
#include "common/ob_define.h"
#include "tbsys.h"

namespace oceanbase
{
  namespace chunkserver
  {
    using namespace oceanbase::common;

    ObGetCellStreamWrapper::ObGetCellStreamWrapper(
        ObMergerRpcProxy& rpc_proxy, const int64_t time_out)
    : get_cell_stream_(&rpc_proxy, CHUNK_SERVER, time_out), 
      scan_cell_stream_(&rpc_proxy, CHUNK_SERVER, time_out)
    {
    }
    
    ObGetCellStreamWrapper::~ObGetCellStreamWrapper()
    {

    }
    
    ObJoinGetCellStream *ObGetCellStreamWrapper::get_ups_get_cell_stream()
    {
      return &get_cell_stream_;
    }
    
    ObScanCellStream *ObGetCellStreamWrapper::get_ups_scan_cell_stream()
    {
      return &scan_cell_stream_;
    }
  } // end namespace chunkserver
} // end namespace oceanbase
