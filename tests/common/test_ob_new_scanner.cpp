/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * test_ob_new_scanner.cpp
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@taobao.com>
 *
 */


#include "common/ob_new_scanner.h"
#include "common/ob_malloc.h"
#include <gtest/gtest.h>

using namespace oceanbase;
using namespace common;

#define OK(value) ASSERT_EQ(OB_SUCCESS, (value))

#define TABLE_ID 1000LU

class ObNewScannerTest: public ::testing::Test
{
  public:
    ObNewScannerTest();
    virtual ~ObNewScannerTest();
    virtual void SetUp();
    virtual void TearDown();
  private:
    // disallow copy
    ObNewScannerTest(const ObNewScannerTest &other);
    ObNewScannerTest& operator=(const ObNewScannerTest &other);
  protected:
    // data members
};

ObNewScannerTest::ObNewScannerTest()
{
}

ObNewScannerTest::~ObNewScannerTest()
{
}

void ObNewScannerTest::SetUp()
{
}

void ObNewScannerTest::TearDown()
{
}

TEST_F(ObNewScannerTest, basic_test)
{
  ObNewScanner new_scanner;

  char rowkey_buf[100];
  ObString rowkey;

  ObRowDesc row_desc;
  for(int64_t i=0;i<10;i++)
  {
    row_desc.add_column_desc(TABLE_ID, i + OB_APP_MIN_COLUMN_ID);
  }

  ObRow row;
  row.set_row_desc(row_desc);

  ObObj cell;
  const ObObj *value = NULL;
  uint64_t table_id = OB_INVALID_ID;
  uint64_t column_id = OB_INVALID_ID;
  int64_t int_value = 0;

  for(int64_t j=0;j<9;j++)
  {
    sprintf(rowkey_buf, "rowkey_%05ld", j);
    rowkey.assign_ptr(rowkey_buf, (int32_t)strlen(rowkey_buf));
    for(int64_t i=0;i<10;i++)
    {
      cell.set_int(j * 1000 + i);
      OK(row.raw_set_cell(i, cell));
    }
    OK(new_scanner.add_row(rowkey, row));
  }

  
  for(int64_t j=0;j<9;j++)
  {
    OK(new_scanner.get_next_row(rowkey, row));
    sprintf(rowkey_buf, "rowkey_%05ld", j);
    ASSERT_EQ(0, strncmp(rowkey_buf, rowkey.ptr(), rowkey.length()));

    for(int64_t i=0;i<10;i++)
    {
      OK(row.raw_get_cell(i, value, table_id, column_id));
      value->get_int(int_value);
      ASSERT_EQ( j * 1000 + i, int_value);
    }
  }

}

int main(int argc, char **argv)
{
  ob_init_memory_pool();
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}

