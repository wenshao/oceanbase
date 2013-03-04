/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_fake_ups_rpc_stub.h
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@taobao.com>
 *
 */

#ifndef _OB_FAKE_UPS_RPC_STUB_H
#define _OB_FAKE_UPS_RPC_STUB_H 1

#include "common/ob_ups_rpc_stub.h"
#include "common/ob_string_buf.h"

#define TABLE_ID 1000
#define COLUMN_NUMS 8

namespace test
{
  class ObFakeUpsRpcStubTest_get_int_test_Test;
  class ObFakeUpsRpcStubTest_gen_new_scanner_test1_Test;
  class ObFakeUpsRpcStubTest_gen_new_scanner_test2_Test;
}

namespace oceanbase
{
  namespace common
  {
    class ObFakeUpsRpcStub : public ObUpsRpcStub 
    {
      public:
        ObFakeUpsRpcStub();
        virtual ~ObFakeUpsRpcStub();

        int get(const int64_t timeout, const ObServer & server, const ObGetParam & get_param, ObNewScanner & new_scanner);
        int scan(const int64_t timeout, const ObServer & server, const ObScanParam & scan_param, ObNewScanner & new_scanner);

        inline void set_column_count(int64_t column_count)
        {
          column_count_ = column_count;
        }

        friend class test::ObFakeUpsRpcStubTest_get_int_test_Test;
        friend class test::ObFakeUpsRpcStubTest_gen_new_scanner_test1_Test;
        friend class test::ObFakeUpsRpcStubTest_gen_new_scanner_test2_Test;

      private:
        // disallow copy
        ObFakeUpsRpcStub(const ObFakeUpsRpcStub &other);
        ObFakeUpsRpcStub& operator=(const ObFakeUpsRpcStub &other);

        int gen_new_scanner(const ObScanParam & scan_param, ObNewScanner & new_scanner);
        int gen_new_scanner(int64_t start_rowkey, int64_t end_rowkey, ObBorderFlag border_flag, ObNewScanner &new_scanner, bool is_fullfilled);
        int get_int(ObString key);

      private:
        // data members
        ObStringBuf str_buf_;
        int64_t column_count_;
        static const int64_t MAX_ROW_COUNT = 100;
    };
  }
}

#endif /* _OB_FAKE_UPS_RPC_STUB_H */

