/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_fake_ups_rpc_stub2.h
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@taobao.com>
 *
 */

#ifndef _OB_FAKE_UPS_RPC_STUB2_H
#define _OB_FAKE_UPS_RPC_STUB2_H 1

#include "common/ob_ups_rpc_stub.h"
#include "ob_fake_ups_scan.h"
#include "ob_fake_ups_multi_get.h"

namespace oceanbase
{
  using namespace sql::test;

  namespace common
  {
    class ObFakeUpsRpcStub2 : public ObUpsRpcStub
    {
      public:
        ObFakeUpsRpcStub2();
        virtual ~ObFakeUpsRpcStub2();

        int get(const int64_t timeout, const ObServer & server, const ObGetParam & get_param, ObNewScanner & new_scanner);
        int scan(const int64_t timeout, const ObServer & server, const ObScanParam & scan_param, ObNewScanner & new_scanner);

        void set_ups_scan(const char *ups_scan_file)
        {
          ups_scan_file_ = ups_scan_file;
        }

        void set_ups_multi_get(const char *ups_multi_get_file)
        {
          ups_multi_get_file_ = ups_multi_get_file;
        }

      private:
        const char *ups_scan_file_;
        const char *ups_multi_get_file_;
    };
  }
}

#endif /* _OB_FAKE_UPS_RPC_STUB2_H */

