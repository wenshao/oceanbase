/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_fake_ups_multi_get_test.cpp
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@taobao.com>
 *
 */

#include "common/ob_malloc.h"
#include <gtest/gtest.h>
#include "ob_fake_ups_multi_get.h"

using namespace oceanbase;
using namespace common;
using namespace sql;
using namespace test;

#define OK(value) ASSERT_EQ(OB_SUCCESS, (value))

class ObFakeUpsMultiGetTest: public ::testing::Test
{
  public:
    ObFakeUpsMultiGetTest();
    virtual ~ObFakeUpsMultiGetTest();
    virtual void SetUp();
    virtual void TearDown();
  private:
    // disallow copy
    ObFakeUpsMultiGetTest(const ObFakeUpsMultiGetTest &other);
    ObFakeUpsMultiGetTest& operator=(const ObFakeUpsMultiGetTest &other);
  protected:
    // data members
};

ObFakeUpsMultiGetTest::ObFakeUpsMultiGetTest()
{
}

ObFakeUpsMultiGetTest::~ObFakeUpsMultiGetTest()
{
}

void ObFakeUpsMultiGetTest::SetUp()
{
}

void ObFakeUpsMultiGetTest::TearDown()
{
}

TEST_F(ObFakeUpsMultiGetTest, basic_test)
{
  ObRowDesc row_desc;
  row_desc.add_column_desc(1000, 1);
  row_desc.add_column_desc(1000, 2);

  ObFakeUpsMultiGet fake_ups_multi_get("test_cases/ob_fake_ups_multi_get.ini");

  char str_buf[100];
  ObString rowkey;
  const ObString *get_rowkey = NULL;
  ObGetParam get_param(true);
  ObCellInfo cell_info;

  for(int i=0;i<3;i++)
  {
    sprintf(str_buf, "chen%d", i+1);
    rowkey.assign_ptr(str_buf, strlen(str_buf));
    cell_info.table_id_ = 1000;
    cell_info.row_key_ = rowkey;
    cell_info.column_id_ = 1;
    get_param.add_cell(cell_info);
    cell_info.column_id_ = 2;
    get_param.add_cell(cell_info);
  }

  const ObRow *row = NULL;

  fake_ups_multi_get.set_get_param(get_param);
  fake_ups_multi_get.set_row_desc(row_desc);

  OK(fake_ups_multi_get.open());
  const ObObj *cell = NULL;
  uint64_t table_id = OB_INVALID_ID;
  uint64_t column_id = OB_INVALID_ID;

  ObString str;
  for(int i=0;i<3;i++)
  {
    OK(fake_ups_multi_get.get_next_row(get_rowkey, row));
    OK(row->raw_get_cell(0, cell, table_id, column_id));
    cell->get_varchar(str);
    sprintf(str_buf, "chen%d", i+1);
    printf("str:%.*s\n", str.length(), str.ptr());
    ASSERT_EQ(0, strncmp(str.ptr(), str_buf, str.length()));
    ASSERT_EQ(0, strncmp(get_rowkey->ptr(), str_buf, get_rowkey->length()));
  }

  OK(fake_ups_multi_get.close());
}

int main(int argc, char **argv)
{
  ob_init_memory_pool();
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}


