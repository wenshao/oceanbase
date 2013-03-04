/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * test_ob_row_store.cpp
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@taobao.com>
 *
 */



#include "common/ob_compact_cell_iterator.h"
#include "common/ob_row_store.h"
#include "common/ob_malloc.h"
#include <gtest/gtest.h>

using namespace oceanbase;
using namespace common;

#define OK(value) ASSERT_EQ(OB_SUCCESS, (value))

#define TABLE_ID 1000LU

class ObRowStoreTest: public ::testing::Test
{
  public:
    ObRowStoreTest();
    virtual ~ObRowStoreTest();
    virtual void SetUp();
    virtual void TearDown();
  private:
    // disallow copy
    ObRowStoreTest(const ObRowStoreTest &other);
    ObRowStoreTest& operator=(const ObRowStoreTest &other);
  protected:
    // data members
};

ObRowStoreTest::ObRowStoreTest()
{
}

ObRowStoreTest::~ObRowStoreTest()
{
}

void ObRowStoreTest::SetUp()
{
}

void ObRowStoreTest::TearDown()
{
}

ObObj gen_int_obj(int64_t int_value, bool is_add = false)
{
  ObObj value;
  value.set_int(int_value, is_add);
  return value;
}

TEST_F(ObRowStoreTest, rowkey)
{
  ObRowStore row_store;

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
  const ObRowStore::StoredRow *stored_row = NULL;
  const ObObj *value = NULL;
  uint64_t table_id = OB_INVALID_ID;
  uint64_t column_id = OB_INVALID_ID;
  int64_t int_value = 0;

  ObCompactCellIterator cell_reader;


  for(int64_t j=0;j<9;j++)
  {
    sprintf(rowkey_buf, "rowkey_%05ld", j);
    rowkey.assign_ptr(rowkey_buf, (int32_t)strlen(rowkey_buf));
    for(int64_t i=0;i<10;i++)
    {
      cell.set_int(j * 1000 + i);
      OK(row.raw_set_cell(i, cell));
    }
    OK(row_store.add_row(rowkey, row, stored_row));

    OK(cell_reader.init(stored_row->get_compact_row(), DENSE_SPARSE));

    OK(cell_reader.next_cell());
    OK(cell_reader.get_cell(value));
    value->get_varchar(rowkey);
    ASSERT_EQ(0, strncmp(rowkey_buf, rowkey.ptr(), rowkey.length()));

    for(int64_t i=0;i<10;i++)
    {
      OK(cell_reader.next_cell());
      OK(cell_reader.get_cell(column_id, value));
      value->get_int(int_value);
      ASSERT_EQ(j * 1000 + i, int_value);
    }
  }

  
  for(int64_t j=0;j<9;j++)
  {
    OK(row_store.get_next_row(&rowkey, row));
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

TEST_F(ObRowStoreTest, ups_row_test)
{
  ObRowStore row_store;
  int64_t cur_size_counter = 0;

  ObRowDesc row_desc;
  row_desc.add_column_desc(TABLE_ID, 1);
  row_desc.add_column_desc(TABLE_ID, 2);
  row_desc.add_column_desc(TABLE_ID, 3);
  row_desc.add_column_desc(TABLE_ID, 4);
  row_desc.add_column_desc(TABLE_ID, 5);

  ObUpsRow row;
  row.set_row_desc(row_desc);

  #define ADD_ROW(is_delete, num1, num2, num3, num4, num5) \
  row.set_delete_row(is_delete); \
  row.set_cell(TABLE_ID, 1, gen_int_obj(num1, false)); \
  row.set_cell(TABLE_ID, 2, gen_int_obj(num2, false)); \
  row.set_cell(TABLE_ID, 3, gen_int_obj(num3, false)); \
  row.set_cell(TABLE_ID, 4, gen_int_obj(num4, false)); \
  row.set_cell(TABLE_ID, 5, gen_int_obj(num5, false)); \
  row_store.add_row(row, cur_size_counter);


  ObUpsRow get_row;
  get_row.set_row_desc(row_desc);

  #define CHECK_CELL(column_id, num) \
  { \
    const ObObj *cell = NULL; \
    int64_t int_value = 0; \
    get_row.get_cell(TABLE_ID, column_id, cell); \
    cell->get_int(int_value); \
    ASSERT_EQ(num, int_value); \
  }

  #define CHECK_ROW(is_delete, num1, num2, num3, num4, num5) \
  row_store.get_next_row(get_row); \
  ASSERT_TRUE( get_row.is_delete_row() == is_delete ); \
  CHECK_CELL(1, num1); \
  CHECK_CELL(2, num2); \
  CHECK_CELL(3, num3); \
  CHECK_CELL(4, num4); \
  CHECK_CELL(5, num5);

  ADD_ROW(true, 1, 2, 4, 5, 3);
  ADD_ROW(false, 1, 2, 4, 5, 3);
  ADD_ROW(true, 1, 2, 4, 5, 3);
  ADD_ROW(false, 1, 2, 4, 5, 3);
  ADD_ROW(false, 1, 2, 4, 5, 3);

  CHECK_ROW(true, 1, 2, 4, 5, 3);
  CHECK_ROW(false, 1, 2, 4, 5, 3);
  CHECK_ROW(true, 1, 2, 4, 5, 3);
  CHECK_ROW(false, 1, 2, 4, 5, 3);
  CHECK_ROW(false, 1, 2, 4, 5, 3);

  #undef ADD_ROW
  #undef CHECK_ROW
}

TEST_F(ObRowStoreTest, basic_test)
{
  ObRowStore row_store;
  int64_t cur_size_counter = 0;

  ObRowDesc row_desc;
  row_desc.add_column_desc(TABLE_ID, 1);
  row_desc.add_column_desc(TABLE_ID, 2);
  row_desc.add_column_desc(TABLE_ID, 3);
  row_desc.add_column_desc(TABLE_ID, 4);
  row_desc.add_column_desc(TABLE_ID, 5);

  ObRow row;
  row.set_row_desc(row_desc);

  #define ADD_ROW(num1, num2, num3, num4, num5) \
  row.set_cell(TABLE_ID, 1, gen_int_obj(num1, false)); \
  row.set_cell(TABLE_ID, 2, gen_int_obj(num2, false)); \
  row.set_cell(TABLE_ID, 3, gen_int_obj(num3, false)); \
  row.set_cell(TABLE_ID, 4, gen_int_obj(num4, false)); \
  row.set_cell(TABLE_ID, 5, gen_int_obj(num5, false)); \
  row_store.add_row(row, cur_size_counter);


  ObRow get_row;
  get_row.set_row_desc(row_desc);

  #define CHECK_CELL(column_id, num) \
  { \
    const ObObj *cell = NULL; \
    int64_t int_value = 0; \
    get_row.get_cell(TABLE_ID, column_id, cell); \
    cell->get_int(int_value); \
    ASSERT_EQ(num, int_value); \
  }

  #define CHECK_ROW(num1, num2, num3, num4, num5) \
  row_store.get_next_row(get_row); \
  CHECK_CELL(1, num1); \
  CHECK_CELL(2, num2); \
  CHECK_CELL(3, num3); \
  CHECK_CELL(4, num4); \
  CHECK_CELL(5, num5);

  ADD_ROW(1, 2, 4, 5, 3);
  ADD_ROW(1, 2, 4, 5, 3);
  ADD_ROW(1, 2, 4, 5, 3);
  ADD_ROW(1, 2, 4, 5, 3);
  ADD_ROW(1, 2, 4, 5, 3);

  CHECK_ROW(1, 2, 4, 5, 3);
  CHECK_ROW(1, 2, 4, 5, 3);
  CHECK_ROW(1, 2, 4, 5, 3);
  CHECK_ROW(1, 2, 4, 5, 3);
  CHECK_ROW(1, 2, 4, 5, 3);
}

int main(int argc, char **argv)
{
  ob_init_memory_pool();
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}

