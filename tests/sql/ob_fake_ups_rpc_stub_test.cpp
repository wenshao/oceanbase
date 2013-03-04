/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_fake_ups_rpc_stub_test.cpp
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@taobao.com>
 *
 */

#include "common/ob_malloc.h"
#include "common/ob_string_buf.h"
#include <gtest/gtest.h>
#include "ob_fake_ups_rpc_stub.h"

using namespace oceanbase;
using namespace common;

#define OK(value) ASSERT_EQ(OB_SUCCESS, (value))

namespace test
{
  class ObFakeUpsRpcStubTest: public ::testing::Test
  {
    public:
      ObFakeUpsRpcStubTest();
      virtual ~ObFakeUpsRpcStubTest();
      virtual void SetUp();
      virtual void TearDown();
    private:
      // disallow copy
      ObFakeUpsRpcStubTest(const ObFakeUpsRpcStubTest &other);
      ObFakeUpsRpcStubTest& operator=(const ObFakeUpsRpcStubTest &other);
    protected:
      // data members
      ObStringBuf str_buf_;
  };

  ObFakeUpsRpcStubTest::ObFakeUpsRpcStubTest()
  {
  }

  ObFakeUpsRpcStubTest::~ObFakeUpsRpcStubTest()
  {
  }

  void ObFakeUpsRpcStubTest::SetUp()
  {
  }

  void ObFakeUpsRpcStubTest::TearDown()
  {
  }

  TEST_F(ObFakeUpsRpcStubTest, get_test)
  {
    ObFakeUpsRpcStub rpc_stub;
    ObServer ups_server;

    int64_t column_count = 3;

    ObGetParam get_param;
    char rowkey_buf[100];
    ObString tmp_rowkey;
    ObString rowkey;

    rpc_stub.set_column_count(column_count);
    ObCellInfo cell;
    for(int64_t i=0;i<200;i++)
    {
      sprintf(rowkey_buf, "rowkey_%05d", i);
      tmp_rowkey.assign_ptr(rowkey_buf, strlen(rowkey_buf));
      str_buf_.write_string(tmp_rowkey, &rowkey);

      for(int64_t j=0;j<column_count;j++)
      {
        cell.table_id_ = TABLE_ID;
        cell.row_key_ = rowkey;
        cell.column_id_ = j + 1;
        cell.value_.set_int(i * 1000 + j);

        OK(get_param.add_cell(cell));
      }
    }

    ObNewScanner new_scanner;
    OK(rpc_stub.get(1000, ups_server, get_param, new_scanner));

    ObUpsRow ups_row;
    ObRowDesc row_desc;
    for(int i=1;i<=column_count;i++)
    {
      OK(row_desc.add_column_desc(TABLE_ID, i));
    }
    ups_row.set_row_desc(row_desc);

    const ObObj *value = NULL;
    uint64_t table_id = OB_INVALID_ID;
    uint64_t column_id = OB_INVALID_ID;
    int64_t int_value = 0;

    for(int i=0;i<100;i++)
    {
      OK(new_scanner.get_next_row(rowkey, ups_row));
      for(int j=0;j<column_count;j++)
      {
        OK(ups_row.raw_get_cell(j, value, table_id, column_id));
        OK(value->get_int(int_value));
        ASSERT_EQ(i * 1000 + j, int_value);
      }
    }
    ASSERT_EQ(OB_ITER_END, new_scanner.get_next_row(rowkey, ups_row));

    bool is_fullfilled = false;
    int64_t fullfilled_num = 0;
    OK(new_scanner.get_is_req_fullfilled(is_fullfilled, fullfilled_num));
    ASSERT_FALSE(is_fullfilled);
    ASSERT_EQ(100, fullfilled_num);
  }

  TEST_F(ObFakeUpsRpcStubTest, gen_new_scanner_test2)
  {
    ObFakeUpsRpcStub rpc_stub;
    ObNewScanner new_scanner;
    ObScanParam scan_param;
    ObString rowkey;


    int start = 5;
    int end = 1000;
    char t1[100];
    char t2[100];
    sprintf(t1, "rowkey_%05d", start);
    sprintf(t2, "rowkey_%05d", end);

    ObRange range;
    range.start_key_.assign_ptr(const_cast<char*>(t1), strlen(t1));
    range.end_key_.assign_ptr(const_cast<char*>(t2), strlen(t2));
    scan_param.set_range(range);

    rpc_stub.gen_new_scanner(scan_param, new_scanner);
    ObUpsRow ups_row;
    const ObObj *cell = NULL;
    uint64_t table_id = OB_INVALID_ID;
    uint64_t column_id = OB_INVALID_ID;
    int64_t int_value = 0;

    ObRange res_range;
    OK(new_scanner.get_range(res_range));

    ObRowDesc row_desc;
    for(uint64_t i = 0;i<COLUMN_NUMS;i++)
    {
      row_desc.add_column_desc(TABLE_ID, i+OB_APP_MIN_COLUMN_ID);
    }
    ups_row.set_row_desc(row_desc);

    for(int i=start + 1;i<=start + 100;i++)
    {
      OK(new_scanner.get_next_row(rowkey, ups_row));
      for(int j=0;j<COLUMN_NUMS;j++)
      {
        OK(ups_row.raw_get_cell(j, cell, table_id, column_id));
        cell->get_int(int_value);
        ASSERT_EQ(i * 1000 + j, int_value);
      }
    }

    ASSERT_EQ(OB_ITER_END, new_scanner.get_next_row(rowkey, ups_row));

  }

  TEST_F(ObFakeUpsRpcStubTest, gen_new_scanner_test1)
  {
    ObFakeUpsRpcStub rpc_stub;
    ObNewScanner new_scanner;
    char rowkey_buf[100];

    int64_t start = 100;
    int64_t end = 1000;

    ObBorderFlag border_flag;
    border_flag.set_inclusive_start();
    border_flag.set_inclusive_end();

    rpc_stub.gen_new_scanner(start, end, border_flag, new_scanner, true);
    ObUpsRow ups_row;
    const ObObj *cell = NULL;
    uint64_t table_id = OB_INVALID_ID;
    uint64_t column_id = OB_INVALID_ID;
    int64_t int_value = 0;
    ObString rowkey;

    ObRowDesc row_desc;
    for(uint64_t i = 0;i<COLUMN_NUMS;i++)
    {
      row_desc.add_column_desc(TABLE_ID, i+OB_APP_MIN_COLUMN_ID);
    }
    ups_row.set_row_desc(row_desc);

    for(int i=start;i<=end;i++)
    {
      OK(new_scanner.get_next_row(rowkey, ups_row));
      sprintf(rowkey_buf, "rowkey_%05d", i);
      ASSERT_EQ(0, strncmp(rowkey_buf, rowkey.ptr(), rowkey.length())); 

      for(int j=0;j<COLUMN_NUMS;j++)
      {
        OK(ups_row.raw_get_cell(j, cell, table_id, column_id));
        cell->get_int(int_value);
        ASSERT_EQ(i * 1000 + j, int_value);
      }
    }

    ASSERT_EQ(OB_ITER_END, new_scanner.get_next_row(rowkey, ups_row));
  }

  TEST_F(ObFakeUpsRpcStubTest, get_int_test)
  {
    ObFakeUpsRpcStub rpc_stub;
    //const char *t = "rowkey_00058";
    ObString str;

    char t[100];

    for(int i=0;i<5000;i++)
    {
      sprintf(t, "rowkey_%05d", i);
      str.assign_ptr(const_cast<char *>(t), strlen(t));
      ASSERT_EQ(i, rpc_stub.get_int(str));
    }
  }
}

int main(int argc, char **argv)
{
  ob_init_memory_pool();
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}

