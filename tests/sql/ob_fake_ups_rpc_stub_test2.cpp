/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_fake_ups_rpc_stub_test2.cpp
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@taobao.com>
 *
 */

#include "common/ob_malloc.h"
#include <gtest/gtest.h>
#include "ob_fake_ups_rpc_stub2.h"

using namespace oceanbase;
using namespace common;

#define OK(value) ASSERT_EQ(OB_SUCCESS, (value))

class ObUpsRpcStub2Test: public ::testing::Test
{
  public:
    ObUpsRpcStub2Test();
    virtual ~ObUpsRpcStub2Test();
    virtual void SetUp();
    virtual void TearDown();
  private:
    // disallow copy
    ObUpsRpcStub2Test(const ObUpsRpcStub2Test &other);
    ObUpsRpcStub2Test& operator=(const ObUpsRpcStub2Test &other);
  protected:
    // data members
};

ObUpsRpcStub2Test::ObUpsRpcStub2Test()
{
}

ObUpsRpcStub2Test::~ObUpsRpcStub2Test()
{
}

void ObUpsRpcStub2Test::SetUp()
{
}

void ObUpsRpcStub2Test::TearDown()
{
}

TEST_F(ObUpsRpcStub2Test, basic_test)
{
  ObUpsRpcStub2 rpc_stub;
  rpc_stub.set_ups_scan("./tablet_scan_test_data/ups_table1.ini");

  ObServer server;
  ObScanParam scan_param;

  rpc_stub.scan(1000, server, scan_param, new_scanner);


}

int main(int argc, char **argv)
{
  ob_init_memory_pool();
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}

