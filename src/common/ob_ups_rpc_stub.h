/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_ups_rpc_stub.h
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@taobao.com>
 *
 */

#ifndef _OB_UPS_RPC_STUB_H
#define _OB_UPS_RPC_STUB_H 1

#include "ob_server.h"
#include "ob_scan_param.h"
#include "ob_get_param.h"
#include "ob_new_scanner.h"

namespace oceanbase
{
  namespace common
  {
    class ObUpsRpcStub
    {
      public:
        virtual ~ObUpsRpcStub() {}
        virtual int get(const int64_t timeout, const ObServer & server, const ObGetParam & get_param, ObNewScanner & new_scanner) = 0;
        virtual int scan(const int64_t timeout, const ObServer & server, const ObScanParam & scan_param, ObNewScanner & new_scanner) = 0;
    };
  }
}

#endif /* _OB_UPS_RPC_STUB_H */

