/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * test_row_util.cpp
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@taobao.com>
 *
 */

#include "common/ob_compact_cell_writer.h"
#include "common/ob_row_util.h"
#include "common/ob_malloc.h"
#include <gtest/gtest.h>

using namespace oceanbase;
using namespace common;

#define TABLE_ID 1000
#define OK(value) ASSERT_EQ(OB_SUCCESS, (value))

class ObRowUtilTest: public ::testing::Test
{
  public:
    ObRowUtilTest();
    virtual ~ObRowUtilTest();
    virtual void SetUp();
    virtual void TearDown();
  private:
    // disallow copy
    ObRowUtilTest(const ObRowUtilTest &other);
    ObRowUtilTest& operator=(const ObRowUtilTest &other);
  protected:
    // data members
};

ObRowUtilTest::ObRowUtilTest()
{
}

ObRowUtilTest::~ObRowUtilTest()
{
}

void ObRowUtilTest::SetUp()
{
}

void ObRowUtilTest::TearDown()
{
}


TEST_F(ObRowUtilTest, basic_test1)
{
  char buf[1024];
  ObCompactCellWriter cell_writer;
  OK(cell_writer.init(buf, 1024, SPARSE));

  ObObj value;
  ObRowDesc row_desc;
  for(int64_t i=0;i<10;i++)
  {
    row_desc.add_column_desc(TABLE_ID, i);
    value.set_int(i);
    OK(cell_writer.append(i, value));
  }
  OK(cell_writer.row_finish());

  ObString compact_row;
  compact_row.assign_ptr(cell_writer.get_buf(), (int32_t)cell_writer.size());

  ObRow row;

  row.set_row_desc(row_desc);
  OK(ObRowUtil::convert(compact_row, row));

  const ObObj *cell = NULL;
  uint64_t column_id = OB_INVALID_ID;
  uint64_t table_id = OB_INVALID_ID;
  int64_t int_value = 0;
  for(int64_t i=0;i<10;i++)
  {
    OK(row.raw_get_cell(i, cell, table_id, column_id));
    cell->get_int(int_value);
    ASSERT_EQ(i, int_value);
  }
}

TEST_F(ObRowUtilTest, basic_test)
{
  char buf[1024];
  ObCompactCellWriter cell_writer;
  OK(cell_writer.init(buf, 1024, DENSE_SPARSE));

  const char *rowkey = "rowkey_00001";
  ObString rowkey_str;
  rowkey_str.assign_ptr(const_cast<char *>(rowkey), (int32_t)(strlen(rowkey)));
  ObObj rowkey_obj;
  rowkey_obj.set_varchar(rowkey_str);

  OK(cell_writer.append(rowkey_obj));
  OK(cell_writer.rowkey_finish());

  ObObj value;
  ObRowDesc row_desc;
  for(int64_t i=0;i<10;i++)
  {
    row_desc.add_column_desc(TABLE_ID, i);
    value.set_int(i);
    OK(cell_writer.append(i, value));
  }
  OK(cell_writer.row_finish());

  ObString compact_row;
  compact_row.assign_ptr(cell_writer.get_buf(), (int32_t)cell_writer.size());

  ObRow row;
  ObString rk;

  row.set_row_desc(row_desc);
  OK(ObRowUtil::convert(compact_row, row, &rk));

  ASSERT_EQ(0, strncmp(rowkey, rk.ptr(), rk.length()));

  const ObObj *cell = NULL;
  uint64_t column_id = OB_INVALID_ID;
  uint64_t table_id = OB_INVALID_ID;
  int64_t int_value = 0;
  for(int64_t i=0;i<10;i++)
  {
    OK(row.raw_get_cell(i, cell, table_id, column_id));
    cell->get_int(int_value);
    ASSERT_EQ(i, int_value);
  }
}

int main(int argc, char **argv)
{
  ob_init_memory_pool();
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}

