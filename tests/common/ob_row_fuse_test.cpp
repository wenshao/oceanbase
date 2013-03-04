/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_row_fuse_test.cpp
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@taobao.com>
 *
 */

#include "common/ob_malloc.h"
#include <gtest/gtest.h>
#include "common/ob_row_fuse.h"
#include "common/ob_ups_row.h"
#include "common/ob_row.h"
#include "common/ob_row_desc.h"

using namespace oceanbase;
using namespace common;

#define OK(value) ASSERT_EQ(OB_SUCCESS, (value))

class ObRowFuseTest: public ::testing::Test
{
  public:
    ObRowFuseTest();
    virtual ~ObRowFuseTest();
    virtual void SetUp();
    virtual void TearDown();
  private:
    // disallow copy
    ObRowFuseTest(const ObRowFuseTest &other);
    ObRowFuseTest& operator=(const ObRowFuseTest &other);
  protected:
    // data members
};

ObRowFuseTest::ObRowFuseTest()
{
}

ObRowFuseTest::~ObRowFuseTest()
{
}

void ObRowFuseTest::SetUp()
{
}

void ObRowFuseTest::TearDown()
{
}

TEST_F(ObRowFuseTest, assign)
{
  uint64_t TABLE_ID = 1000;

  ObRowDesc row_desc;
  for(int i=0;i<8;i++)
  {
    row_desc.add_column_desc(TABLE_ID, OB_APP_MIN_COLUMN_ID + i);
  }

  ObRow row;
  row.set_row_desc(row_desc);
  ObObj value;

  for(int i=0;i<8;i++)
  {
    value.set_int(i);
    OK(row.raw_set_cell(i, value));
  }

  ObRowDesc ups_row_desc;
  for(int i=0;i<4;i++)
  {
    ups_row_desc.add_column_desc(TABLE_ID, OB_APP_MIN_COLUMN_ID + i);
  }

  ObUpsRow ups_row;
  ups_row.set_row_desc(ups_row_desc);

  for(int i=0;i<4;i++)
  {
    value.set_ext(ObActionFlag::OP_NOP);
    OK(ups_row.raw_set_cell(i, value));
  }

  ups_row.set_delete_row(true);
  OK(ObRowFuse::assign(ups_row, row));

  const ObObj *cell = NULL;
  uint64_t table_id = OB_INVALID_ID;
  uint64_t column_id = OB_INVALID_ID;

  for(int i=0;i<4;i++)
  {
    OK(row.raw_get_cell(i, cell, table_id, column_id));
    ASSERT_EQ(ObNullType, cell->get_type());
  }

  ASSERT_TRUE(OB_SUCCESS != row.raw_get_cell(4, cell, table_id, column_id));
}

TEST_F(ObRowFuseTest, test2)
{
  uint64_t TABLE_ID = 1000;

  ObRowDesc row_desc;
  for(int i=0;i<8;i++)
  {
    row_desc.add_column_desc(TABLE_ID, OB_APP_MIN_COLUMN_ID + i);
  }

  ObRow row;
  row.set_row_desc(row_desc);
  ObObj value;

  for(int i=0;i<8;i++)
  {
    value.set_int(i);
    row.raw_set_cell(i, value);
  }

  ObRowDesc ups_row_desc;
  for(int i=0;i<4;i++)
  {
    ups_row_desc.add_column_desc(TABLE_ID, OB_APP_MIN_COLUMN_ID + i);
  }

  ObUpsRow ups_row;
  ups_row.set_row_desc(ups_row_desc);

  ups_row.set_delete_row(true);

  for(int i=0;i<4;i++)
  {
    value.set_ext(ObActionFlag::OP_NOP);
    ups_row.raw_set_cell(i, value); 
  }

  ObRow result;
  OK(ObRowFuse::fuse_row(&ups_row, &row, &result));

  uint64_t table_id = OB_INVALID_ID;
  uint64_t column_id = OB_INVALID_ID;
  
  const ObObj *cell = NULL;

  for(int i=0;i<4;i++)
  {
    OK(result.raw_get_cell(i, cell, table_id, column_id));
    ASSERT_EQ(ObNullType, cell->get_type());
  }

  for(int i=4;i<8;i++)
  {
    int64_t int_value = 0;
    OK(result.raw_get_cell(i, cell, table_id, column_id));
    cell->get_int(int_value);
    ASSERT_EQ(int_value, i);
  }


  for(int i=0;i<4;i++)
  {
    value.set_int(i+100);
    ups_row.raw_set_cell(i, value);
  }

  OK(ObRowFuse::fuse_row(&ups_row, &row, &result));
  for(int i=0;i<4;i++)
  {
    int64_t int_value = 0;
    OK(result.raw_get_cell(i, cell, table_id, column_id));
    cell->get_int(int_value);
    ASSERT_EQ(int_value, i+100);
  }

  for(int i=4;i<8;i++)
  {
    int64_t int_value = 0;
    OK(result.raw_get_cell(i, cell, table_id, column_id));
    cell->get_int(int_value);
    ASSERT_EQ(int_value, i);
  }

  ups_row_desc.add_column_desc(TABLE_ID, OB_APP_MIN_COLUMN_ID + 10);
  ASSERT_TRUE(OB_SUCCESS != ObRowFuse::fuse_row(&ups_row, &row, &result));
}

TEST_F(ObRowFuseTest, basic_test)
{
  ObRowDesc row_desc;
  uint64_t table_id = 1000;
  for(int i=0;i<8;i++)
  {
    row_desc.add_column_desc(table_id, OB_APP_MIN_COLUMN_ID + i);
  }

  ObUpsRow ups_row;
  ups_row.set_row_desc(row_desc);
  ups_row.set_delete_row(false);

  ObObj cell;
  for(int i=0;i<8;i++)
  {
    cell.set_int(i, true);
    ups_row.raw_set_cell(i, cell);
  }

  ObRow row;
  row.set_row_desc(row_desc);
  
  for(int i=0;i<8;i++)
  {
    cell.set_int(i);
    row.raw_set_cell(i, cell);
  }

  ObRow result;

  ObRowFuse::fuse_row(&ups_row, &row, &result);

  const ObObj *result_cell = NULL;
  uint64_t result_table_id = 0;
  uint64_t result_column_id = 0;
  int64_t int_value = 0;
  for(int i=0;i<8;i++)
  {
    result.raw_get_cell(i, result_cell, result_table_id, result_column_id);
    result_cell->get_int(int_value);
    ASSERT_EQ(i * 2, int_value);
  }

  ups_row.set_delete_row(true);

  ObRowFuse::fuse_row(&ups_row, &row, &result);

  for(int i=0;i<8;i++)
  {
    result.raw_get_cell(i, result_cell, result_table_id, result_column_id);
    result_cell->get_int(int_value);
    ASSERT_EQ(i, int_value);
  }

  for(int i=0;i<8;i++)
  {
    cell.set_int(i, false);
    ups_row.raw_set_cell(i, cell);
  }

  ups_row.set_delete_row(false);
  ObRowFuse::fuse_row(&ups_row, &row, &result);

  for(int i=0;i<8;i++)
  {
    result.raw_get_cell(i, result_cell, result_table_id, result_column_id);
    result_cell->get_int(int_value);
    ASSERT_EQ(i, int_value);
  }
}

int main(int argc, char **argv)
{
  ob_init_memory_pool();
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}


