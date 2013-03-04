/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_ups_scan_test.cpp
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@taobao.com>
 *
 */


#include "common/ob_row.h"
#include "common/ob_malloc.h"
#include <gtest/gtest.h>
#include "ob_fake_ups_rpc_stub.h"
#include "sql/ob_ups_scan.h"

using namespace oceanbase;
using namespace common;
using namespace sql;

#define OK(value) ASSERT_EQ(OB_SUCCESS, (value))

class ObUpsScanTest: public ::testing::Test
{
  public:
    ObUpsScanTest();
    virtual ~ObUpsScanTest();
    virtual void SetUp();
    virtual void TearDown();
  private:
    // disallow copy
    ObUpsScanTest(const ObUpsScanTest &other);
    ObUpsScanTest& operator=(const ObUpsScanTest &other);
  protected:
    // data members
};

ObUpsScanTest::ObUpsScanTest()
{
}

ObUpsScanTest::~ObUpsScanTest()
{
}

void ObUpsScanTest::SetUp()
{
}

void ObUpsScanTest::TearDown()
{
}

TEST_F(ObUpsScanTest, basic_test)
{
  ObUpsScan ups_scan;
  ObFakeUpsRpcStub fake_ups_rpc_stub;

  OK(ups_scan.set_ups_rpc_stub(&fake_ups_rpc_stub));

  const ObRow *ups_row = NULL;

  int start = 12;
  int end = 1000;
  char t1[100];
  char t2[100];
  sprintf(t1, "rowkey_%05d", start);
  sprintf(t2, "rowkey_%05d", end);

  ObRange range;
  range.table_id_ = TABLE_ID;
  range.start_key_.assign_ptr(const_cast<char*>(t1), strlen(t1));
  range.end_key_.assign_ptr(const_cast<char*>(t2), strlen(t2));
  range.border_flag_.unset_inclusive_start();
  range.border_flag_.unset_inclusive_end();

  ups_scan.set_range(range);
  for(uint64_t i = 0;i<COLUMN_NUMS;i++)
  {
    OK(ups_scan.add_column(i + OB_APP_MIN_COLUMN_ID));
  }

  OK(ups_scan.open());

  const ObObj *cell = NULL;
  uint64_t table_id = OB_INVALID_ID;
  uint64_t column_id = OB_INVALID_ID;
  int64_t int_value = 0;
  const ObString *rowkey = NULL;

  //int err = 0;
  for(int i=start + 1;i<=end - 1;i++)
  {
    OK(ups_scan.get_next_row(rowkey, ups_row));
    for(int j=0;j<COLUMN_NUMS;j++)
    {
      OK(ups_row->raw_get_cell(j, cell, table_id, column_id));
      cell->get_int(int_value);
      ASSERT_EQ(i * 1000 + j, int_value);
    }
  }
  ASSERT_EQ(OB_ITER_END, ups_scan.get_next_row(rowkey, ups_row));
  OK(ups_scan.close());
}

int main(int argc, char **argv)
{
  ob_init_memory_pool();
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}


