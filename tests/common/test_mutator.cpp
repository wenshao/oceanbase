/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: test_mutator.cpp,v 0.1 2010/09/16 15:57:42 chuanhui Exp $
 *
 * Authors:
 *   chuanhui <rizhao.ych@taobao.com>
 *     - some work details if you want
 *
 */
#include <iostream>
#include <sstream>
#include <algorithm>
#include <tblog.h>
#include <gtest/gtest.h>
#include "test_helper.h"
#include "common/ob_mutator.h"

#define TRUE 1

using namespace std;
using namespace oceanbase::common;

int init_mem_pool()
{
  ob_init_memory_pool(2 * 1024L * 1024L);
  return 0;
}
static int init_mem_pool_ret = init_mem_pool();

namespace oceanbase
{
namespace tests
{
namespace common
{

class TestMutatorHelper
{
  public:
    TestMutatorHelper(ObMutator& mutator)
      : mutator_(mutator)
    {
    }

    ~TestMutatorHelper()
    {
    }

    int add_cell(const ObMutatorCellInfo cell)
    {
      return mutator_.add_cell(cell);
    }

  private:
    ObMutator& mutator_;
};

class TestMutator : public ::testing::Test
{
public:
  virtual void SetUp()
  {

  }

  virtual void TearDown()
  {

  }
};

TEST_F(TestMutator, test_add_cell)
{
  static const int64_t ROW_NUM = 100;
  static const int64_t COL_NUM = 5;

  int err = OB_SUCCESS;
  ObCellInfo cell_infos[ROW_NUM][COL_NUM];
  char row_key_strs[ROW_NUM][50];
  char column_strs[ROW_NUM][COL_NUM][50];
  ObString table_name;
  table_name.assign((char*)"oceanbase_table", static_cast<int32_t>(strlen("oceanbase_table")));
  // init cell infos
  for (int64_t i = 0; i < ROW_NUM; ++i)
  {
    sprintf(row_key_strs[i], "row_key_%08ld", i);
    for (int64_t j = 0; j < COL_NUM; ++j)
    {
      cell_infos[i][j].table_name_ = table_name;
      cell_infos[i][j].row_key_.assign(row_key_strs[i], static_cast<int32_t>(strlen(row_key_strs[i])));
      //cell_infos[i][j].op_info_.set_update();

      sprintf(column_strs[i][j],"column_name_%08ld_%08ld", i, j);
      cell_infos[i][j].column_name_.assign(column_strs[i][j], static_cast<int32_t>(strlen(column_strs[i][j])));

      cell_infos[i][j].value_.set_int(1000 + i * COL_NUM + j);
    }
  }

  ObMutator mutator;
  TestMutatorHelper helper(mutator);
  // add cell to array
  for (int64_t i = 0; i < ROW_NUM; ++i)
  {
    for (int64_t j = 0; j < COL_NUM; ++j)
    {
      ObMutatorCellInfo mutation;
      mutation.cell_info = cell_infos[i][j];
      mutation.op_type.set_ext(ObActionFlag::OP_UPDATE);

      err = helper.add_cell(mutation);
      EXPECT_EQ(0, err);
    }
  }

  // check result
  int64_t count = 0;
  ObMutatorCellInfo* cur_cell = NULL;
  while (OB_SUCCESS == mutator.next_cell())
  {
    err = mutator.get_cell(&cur_cell);
    EXPECT_EQ(0, err);
    EXPECT_NE((ObMutatorCellInfo*) NULL, cur_cell);
    check_cell_with_name(cell_infos[count / COL_NUM][count % COL_NUM], cur_cell->cell_info);
    int64_t ext_val = 0;
    cur_cell->op_type.get_ext(ext_val);
    EXPECT_TRUE(ObActionFlag::OP_UPDATE ==ext_val);
    ++count;
  }
  EXPECT_EQ(ROW_NUM * COL_NUM, count);
}

TEST_F(TestMutator, test_serialize)
{
  static const int64_t ROW_NUM = 100;
  static const int64_t COL_NUM = 5;

  int err = OB_SUCCESS;
  ObCellInfo cell_infos[ROW_NUM][COL_NUM];
  char row_key_strs[ROW_NUM][50];
  char column_strs[ROW_NUM][COL_NUM][50];
  ObString table_name;
  table_name.assign((char*)"oceanbase_table", static_cast<int32_t>(strlen("oceanbase_table")));
  // init cell infos
  for (int64_t i = 0; i < ROW_NUM; ++i)
  {
    sprintf(row_key_strs[i], "row_key_%08ld", i);
    for (int64_t j = 0; j < COL_NUM; ++j)
    {
      cell_infos[i][j].table_name_ = table_name;
      cell_infos[i][j].row_key_.assign(row_key_strs[i], static_cast<int32_t>(strlen(row_key_strs[i])));
      //cell_infos[i][j].op_info_.set_update();

      sprintf(column_strs[i][j],"column_name_%08ld_%08ld", i, j);
      cell_infos[i][j].column_name_.assign(column_strs[i][j], static_cast<int32_t>(strlen(column_strs[i][j])));

      cell_infos[i][j].value_.set_int(1000 + i * COL_NUM + j);
    }
  }

  ObMutator mutator;
  TestMutatorHelper helper(mutator);
  // add cell to array
  for (int64_t i = 0; i < ROW_NUM; ++i)
  {
    for (int64_t j = 0; j < COL_NUM; ++j)
    {
      ObMutatorCellInfo mutation;
      mutation.cell_info = cell_infos[i][j];
      mutation.op_type.set_ext(ObActionFlag::OP_UPDATE);

      err = helper.add_cell(mutation);
      EXPECT_EQ(0, err);
    }
  }

  int64_t serialize_size = mutator.get_serialize_size();
  ASSERT_TRUE(serialize_size > 0);
  char* buf = new char[serialize_size + 1024];
  memset(buf, 0xfe, serialize_size + 1024);
  int64_t pos = 0;
  err = mutator.serialize(buf, serialize_size, pos);
  EXPECT_EQ(0, err);
  EXPECT_EQ(serialize_size, pos);

  ObMutator new_mutator;
  pos = 0;
  err = new_mutator.deserialize(buf, serialize_size, pos);
  EXPECT_EQ(serialize_size, pos);

  // check result
  int64_t count = 0;
  ObMutatorCellInfo* cur_cell = NULL;
  while (OB_SUCCESS == new_mutator.next_cell())
  {
    err = new_mutator.get_cell(&cur_cell);
    EXPECT_EQ(0, err);
    ASSERT_TRUE(NULL != cur_cell);
    check_cell_with_name(cell_infos[count / COL_NUM][count % COL_NUM], cur_cell->cell_info);
    int64_t ext_val = 0;
    cur_cell->op_type.get_ext(ext_val);
    EXPECT_TRUE(ObActionFlag::OP_UPDATE ==ext_val);
    ++count;
  }
  EXPECT_EQ(ROW_NUM * COL_NUM, count);

  delete[] buf;
}

TEST_F(TestMutator, test_serialize_id)
{
  static const int64_t ROW_NUM = 100;
  static const int64_t COL_NUM = 5;

  int err = OB_SUCCESS;
  ObCellInfo cell_infos[ROW_NUM][COL_NUM];
  char row_key_strs[ROW_NUM][50];
  char column_strs[ROW_NUM][COL_NUM][50];
  ObString table_name;
  table_name.assign((char*)"oceanbase_table", static_cast<int32_t>(strlen("oceanbase_table")));
  uint64_t table_id = 10;
  // init cell infos
  for (int64_t i = 0; i < ROW_NUM; ++i)
  {
    sprintf(row_key_strs[i], "row_key_%08ld", i);
    for (int64_t j = 0; j < COL_NUM; ++j)
    {
      //cell_infos[i][j].table_name_ = table_name;
      cell_infos[i][j].table_name_.assign(NULL, 0);
      cell_infos[i][j].table_id_ = table_id;
      cell_infos[i][j].row_key_.assign(row_key_strs[i], static_cast<int32_t>(strlen(row_key_strs[i])));

      sprintf(column_strs[i][j],"column_name_%08ld_%08ld", i, j);
      //cell_infos[i][j].column_name_.assign(column_strs[i][j], strlen(column_strs[i][j]));
      cell_infos[i][j].column_name_.assign(NULL, 0);
      cell_infos[i][j].column_id_ = j + 1;

      cell_infos[i][j].value_.set_int(1000 + i * COL_NUM + j);
    }
  }

  ObMutator mutator;
  TestMutatorHelper helper(mutator);
  // add cell to array
  for (int64_t i = 0; i < ROW_NUM; ++i)
  {
    for (int64_t j = 0; j < COL_NUM; ++j)
    {
      ObMutatorCellInfo mutation;
      mutation.cell_info = cell_infos[i][j];
      mutation.op_type.set_ext(ObActionFlag::OP_UPDATE);

      err = helper.add_cell(mutation);
      EXPECT_EQ(0, err);
    }
  }

  int64_t serialize_size = mutator.get_serialize_size();
  ASSERT_TRUE(serialize_size > 0);
  char* buf = new char[serialize_size + 1024];
  memset(buf, 0xfe, serialize_size + 1024);
  int64_t pos = 0;
  err = mutator.serialize(buf, serialize_size, pos);
  EXPECT_EQ(0, err);
  EXPECT_EQ(serialize_size, pos);

  ObMutator new_mutator;
  EXPECT_EQ(0, err);
  pos = 0;
  err = new_mutator.deserialize(buf, serialize_size, pos);
  EXPECT_EQ(serialize_size, pos);

  // check result
  int64_t count = 0;
  ObMutatorCellInfo* cur_cell = NULL;
  while (OB_SUCCESS == new_mutator.next_cell())
  {
    err = new_mutator.get_cell(&cur_cell);
    EXPECT_EQ(0, err);
    ASSERT_TRUE(NULL != cur_cell);
    //check_cell_with_name(cell_infos[count / COL_NUM][count % COL_NUM], cur_cell->cell_info);
    check_cell(cell_infos[count / COL_NUM][count % COL_NUM], cur_cell->cell_info);
    int64_t ext_val = 0;
    cur_cell->op_type.get_ext(ext_val);
    EXPECT_TRUE(ObActionFlag::OP_UPDATE == ext_val);
    ++count;
  }
  EXPECT_EQ(ROW_NUM * COL_NUM, count);

  // reuse new_mutator to deserialize
  pos = 0;
  err = new_mutator.deserialize(buf, serialize_size, pos);
  EXPECT_EQ(0, err);
  EXPECT_EQ(serialize_size, pos);
  count = 0;
  cur_cell = NULL;
  while (OB_SUCCESS == new_mutator.next_cell())
  {
    err = new_mutator.get_cell(&cur_cell);
    EXPECT_EQ(0, err);
    ASSERT_TRUE(NULL != cur_cell);
    //check_cell_with_name(cell_infos[count / COL_NUM][count % COL_NUM], cur_cell->cell_info);
    check_cell(cell_infos[count / COL_NUM][count % COL_NUM], cur_cell->cell_info);
    int64_t ext_val = 0;
    cur_cell->op_type.get_ext(ext_val);
    EXPECT_TRUE(ObActionFlag::OP_UPDATE == ext_val);
    ++count;
  }
  EXPECT_EQ(ROW_NUM * COL_NUM, count);

  // serialize new_mutator
  int64_t new_serialize_size = new_mutator.get_serialize_size();
  //ASSERT_TRUE(new_serialize_size == serialize_size);
  EXPECT_EQ(serialize_size, new_serialize_size);
  char* buf2 = new char[new_serialize_size + 1024];

  pos = 0;
  err = new_mutator.serialize(buf2, new_serialize_size, pos);
  EXPECT_EQ(0, err);
  EXPECT_EQ(new_serialize_size, pos);

  pos = 0;
  err = mutator.deserialize(buf2, new_serialize_size, pos);
  EXPECT_EQ(0, err);
  EXPECT_EQ(new_serialize_size, pos);

  count = 0;
  cur_cell = NULL;
  while (OB_SUCCESS == mutator.next_cell())
  {
    err = mutator.get_cell(&cur_cell);
    EXPECT_EQ(0, err);
    ASSERT_TRUE(NULL != cur_cell);
    check_cell_with_name(cell_infos[count / COL_NUM][count % COL_NUM], cur_cell->cell_info);
    check_cell(cell_infos[count / COL_NUM][count % COL_NUM], cur_cell->cell_info);
    int64_t ext_val = 0;
    cur_cell->op_type.get_ext(ext_val);
    EXPECT_TRUE(ObActionFlag::OP_UPDATE == ext_val);
    ++count;
  }
  EXPECT_EQ(ROW_NUM * COL_NUM, count);

  delete[] buf;
  delete[] buf2;
}

TEST_F(TestMutator, test_db_ob_sem)
{
  int err = OB_SUCCESS;
  ObMutator mutator;
  ObString table_name;
  table_name.assign((char*)"oceanbase", static_cast<int32_t>(strlen("oceanbase")));
  ObString row_key;
  row_key.assign((char*)"row_key", static_cast<int32_t>(strlen("row_key")));

  err = mutator.use_db_sem();
  EXPECT_EQ(0, err);

  err = mutator.use_ob_sem();
  EXPECT_EQ(0, err);

  char buf[1024];
  int64_t pos = 0;
  err = mutator.serialize(buf, sizeof(buf), pos);
  EXPECT_EQ(0, err);
  int64_t new_pos = 0;
  err = mutator.deserialize(buf, pos, new_pos);
  EXPECT_EQ(0, err);
  EXPECT_EQ(pos, new_pos);

  ObMutatorCellInfo* mutation = NULL;
  err = mutator.next_cell();
  EXPECT_EQ(0, err);
  err = mutator.get_cell(&mutation);
  EXPECT_EQ(0, err);
  ASSERT_TRUE(NULL != mutation);

  int64_t ext_val = 0;
  mutation->cell_info.value_.get_ext(ext_val);
  EXPECT_TRUE(ObActionFlag::OP_USE_DB_SEM == ext_val);

  err = mutator.next_cell();
  EXPECT_EQ(0, err);
  err = mutator.get_cell(&mutation);
  EXPECT_EQ(0, err);
  ASSERT_TRUE(NULL != mutation);

  mutation->cell_info.value_.get_ext(ext_val);
  EXPECT_TRUE(ObActionFlag::OP_USE_OB_SEM == ext_val);

  err = mutator.next_cell();
  EXPECT_EQ(OB_ITER_END, err);
}

TEST_F(TestMutator, test_del_row)
{
  int err = OB_SUCCESS;
  ObMutator mutator;
  ObString table_name;
  table_name.assign((char*)"oceanbase", static_cast<int32_t>(strlen("oceanbase")));
  ObString row_key;
  row_key.assign((char*)"row_key", static_cast<int32_t>(strlen("row_key")));

  ObMutatorCellInfo update_op;
  update_op.op_type.set_ext(ObActionFlag::OP_UPDATE);
  update_op.cell_info.table_name_ = table_name;
  update_op.cell_info.row_key_ = row_key;
  update_op.cell_info.column_name_.assign((char*)"column_name", static_cast<int32_t>(strlen("column_name")));
  ObString value;
  value.assign((char*)"test", static_cast<int32_t>(strlen("test")));
  update_op.cell_info.value_.set_varchar(value);

  err = mutator.add_cell(update_op);
  EXPECT_EQ(0, err);

  err = mutator.del_row(table_name, row_key);
  EXPECT_EQ(0, err);

  char buf[1024];
  int64_t pos = 0;
  err = mutator.serialize(buf, sizeof(buf), pos);
  EXPECT_EQ(0, err);
  int64_t new_pos = 0;
  err = mutator.deserialize(buf, pos, new_pos);
  EXPECT_EQ(0, err);
  EXPECT_EQ(pos, new_pos);

  ObMutatorCellInfo* mutation = NULL;
  // omit update op
  err = mutator.next_cell();
  EXPECT_EQ(0, err);
  err = mutator.get_cell(&mutation);
  EXPECT_EQ(0, err);
  ASSERT_TRUE(NULL != mutation);

  err = mutator.next_cell();
  EXPECT_EQ(0, err);
  err = mutator.get_cell(&mutation);
  EXPECT_EQ(0, err);
  ASSERT_TRUE(NULL != mutation);

  check_string(table_name, mutation->cell_info.table_name_);
  check_string(row_key, mutation->cell_info.row_key_);
  int64_t ext_val = 0;
  mutation->cell_info.value_.get_ext(ext_val);
  EXPECT_TRUE(ObActionFlag::OP_DEL_ROW == ext_val);
  EXPECT_EQ((uint64_t) oceanbase::common::OB_INVALID_ID, mutation->cell_info.column_id_);
  EXPECT_TRUE(mutation->cell_info.column_name_.ptr() == NULL);
  EXPECT_TRUE(mutation->cell_info.column_name_.length() == 0);

  err = mutator.next_cell();
  EXPECT_EQ(OB_ITER_END, err);
}

TEST_F(TestMutator, test_del_row_id)
{
  int err = OB_SUCCESS;
  ObMutator mutator;
  uint64_t table_id = 10;
  ObString row_key;
  row_key.assign((char*)"row_key", static_cast<int32_t>(strlen("row_key")));

  ObMutatorCellInfo update_op;
  update_op.op_type.set_ext(ObActionFlag::OP_UPDATE);
  update_op.cell_info.table_name_.assign(NULL, 0);
  update_op.cell_info.table_id_ = table_id;
  update_op.cell_info.row_key_ = row_key;
  update_op.cell_info.column_id_ = 2;
  update_op.cell_info.column_name_.assign(NULL, 0);
  ObString value;
  value.assign((char*)"test", static_cast<int32_t>(strlen("test")));
  update_op.cell_info.value_.set_varchar(value);

  err = mutator.add_cell(update_op);
  EXPECT_EQ(0, err);

  ObMutatorCellInfo del_op;
  del_op.cell_info.table_id_ = table_id;
  del_op.cell_info.row_key_ = row_key;
  del_op.cell_info.value_.set_ext(ObActionFlag::OP_DEL_ROW);
  err = mutator.add_cell(del_op);
  EXPECT_EQ(0, err);

  char buf[1024];
  int64_t pos = 0;
  err = mutator.serialize(buf, sizeof(buf), pos);
  EXPECT_EQ(0, err);
  int64_t new_pos = 0;
  err = mutator.deserialize(buf, pos, new_pos);
  EXPECT_EQ(0, err);
  EXPECT_EQ(pos, new_pos);

  ObMutatorCellInfo* mutation = NULL;
  // omit update op
  err = mutator.next_cell();
  EXPECT_EQ(0, err);
  err = mutator.get_cell(&mutation);
  EXPECT_EQ(0, err);
  ASSERT_TRUE(NULL != mutation);

  err = mutator.next_cell();
  EXPECT_EQ(0, err);
  err = mutator.get_cell(&mutation);
  EXPECT_EQ(0, err);
  ASSERT_TRUE(NULL != mutation);

  EXPECT_EQ(table_id, mutation->cell_info.table_id_);
  check_string(row_key, mutation->cell_info.row_key_);
  int64_t ext_val = 0;
  mutation->cell_info.value_.get_ext(ext_val);
  EXPECT_TRUE(ObActionFlag::OP_DEL_ROW == ext_val);
  EXPECT_EQ((uint64_t) oceanbase::common::OB_INVALID_ID, mutation->cell_info.column_id_);
  EXPECT_TRUE(mutation->cell_info.column_name_.ptr() == NULL);
  EXPECT_TRUE(mutation->cell_info.column_name_.length() == 0);

  err = mutator.next_cell();
  EXPECT_EQ(OB_ITER_END, err);
}


TEST_F(TestMutator, test_update)
{
  int err = OB_SUCCESS;
  ObMutator mutator;
  ObString table_name;
  table_name.assign((char*)"oceanbase", static_cast<int32_t>(strlen("oceanbase")));
  ObString row_key;
  row_key.assign((char*)"row_key", static_cast<int32_t>(strlen("row_key")));
  ObString column_name;
  column_name.assign((char*)"column_name", static_cast<int32_t>(strlen("column_name")));
  int64_t int_val = 9999;
  ObObj value;
  value.set_int(int_val);

  err = mutator.update(table_name, row_key, column_name, value);
  EXPECT_EQ(0, err);

  char buf[1024];
  int64_t pos = 0;
  err = mutator.serialize(buf, sizeof(buf), pos);
  EXPECT_EQ(0, err);
  int64_t new_pos = 0;
  err = mutator.deserialize(buf, pos, new_pos);
  EXPECT_EQ(0, err);
  EXPECT_EQ(pos, new_pos);

  ObMutatorCellInfo* mutation = NULL;
  err = mutator.next_cell();
  EXPECT_EQ(0, err);
  err = mutator.get_cell(&mutation);
  EXPECT_EQ(0, err);
  ASSERT_TRUE(NULL != mutation);

  check_string(table_name, mutation->cell_info.table_name_);
  check_string(row_key, mutation->cell_info.row_key_);
  check_string(column_name, mutation->cell_info.column_name_);
  int64_t tmp_int = 0;
  mutation->cell_info.value_.get_int(tmp_int);
  EXPECT_EQ(int_val, tmp_int);
  int64_t ext_val = 0;
  mutation->op_type.get_ext(ext_val);
  EXPECT_TRUE(ObActionFlag::OP_UPDATE == ext_val);

  err = mutator.next_cell();
  EXPECT_EQ(OB_ITER_END, err);
}

TEST_F(TestMutator, test_insert)
{
  int err = OB_SUCCESS;
  ObMutator mutator;
  ObString table_name;
  table_name.assign((char*)"oceanbase", static_cast<int32_t>(strlen("oceanbase")));
  ObString row_key;
  row_key.assign((char*)"row_key", static_cast<int32_t>(strlen("row_key")));
  ObString column_name;
  column_name.assign((char*)"column_name", static_cast<int32_t>(strlen("column_name")));
  int64_t int_val = 9999;
  ObObj value;
  value.set_int(int_val);

  err = mutator.insert(table_name, row_key, column_name, value);
  EXPECT_EQ(0, err);

  char buf[1024];
  int64_t pos = 0;
  err = mutator.serialize(buf, sizeof(buf), pos);
  EXPECT_EQ(0, err);
  int64_t new_pos = 0;
  err = mutator.deserialize(buf, pos, new_pos);
  EXPECT_EQ(0, err);
  EXPECT_EQ(pos, new_pos);

  ObMutatorCellInfo* mutation = NULL;
  err = mutator.next_cell();
  EXPECT_EQ(0, err);
  err = mutator.get_cell(&mutation);
  EXPECT_EQ(0, err);
  ASSERT_TRUE(NULL != mutation);

  check_string(table_name, mutation->cell_info.table_name_);
  check_string(row_key, mutation->cell_info.row_key_);
  check_string(column_name, mutation->cell_info.column_name_);
  int64_t tmp_int = 0;
  mutation->cell_info.value_.get_int(tmp_int);
  EXPECT_EQ(int_val, tmp_int);
  int64_t ext_val = 0;
  mutation->op_type.get_ext(ext_val);
  EXPECT_TRUE(ObActionFlag::OP_INSERT == ext_val);

  err = mutator.next_cell();
  EXPECT_EQ(OB_ITER_END, err);
}

TEST_F(TestMutator, test_add)
{
  int err = OB_SUCCESS;
  ObMutator mutator;
  ObString table_name;
  table_name.assign((char*)"oceanbase", static_cast<int32_t>(strlen("oceanbase")));
  ObString row_key;
  row_key.assign((char*)"row_key", static_cast<int32_t>(strlen("row_key")));
  ObString column_name;
  column_name.assign((char*)"column_name", static_cast<int32_t>(strlen("column_name")));
  int64_t int_val = 9999;

  err = mutator.add(table_name, row_key, column_name, int_val);
  EXPECT_EQ(0, err);

  char buf[1024];
  int64_t pos = 0;
  err = mutator.serialize(buf, sizeof(buf), pos);
  EXPECT_EQ(0, err);
  int64_t new_pos = 0;
  err = mutator.deserialize(buf, pos, new_pos);
  EXPECT_EQ(0, err);
  EXPECT_EQ(pos, new_pos);

  ObMutatorCellInfo* mutation = NULL;
  err = mutator.next_cell();
  EXPECT_EQ(0, err);
  err = mutator.get_cell(&mutation);
  EXPECT_EQ(0, err);
  ASSERT_TRUE(NULL != mutation);

  check_string(table_name, mutation->cell_info.table_name_);
  check_string(row_key, mutation->cell_info.row_key_);
  check_string(column_name, mutation->cell_info.column_name_);
  int64_t tmp_int = 0;
  bool is_add = false;
  mutation->cell_info.value_.get_int(tmp_int, is_add);
  EXPECT_EQ(int_val, tmp_int);
  EXPECT_TRUE(is_add);
  int64_t ext_val = 0;
  mutation->op_type.get_ext(ext_val);
  EXPECT_TRUE(ObActionFlag::OP_UPDATE == ext_val);

  err = mutator.next_cell();
  EXPECT_EQ(OB_ITER_END, err);
}

TEST_F(TestMutator, test_modify_id)
{
  int err = OB_SUCCESS;
  ObMutator mutator;
  ObString table_name;
  table_name.assign((char*)"oceanbase", static_cast<int32_t>(strlen("oceanbase")));
  ObString row_key;
  row_key.assign((char*)"row_key", static_cast<int32_t>(strlen("row_key")));
  ObString column_name;
  column_name.assign((char*)"column_name", static_cast<int32_t>(strlen("column_name")));
  uint64_t table_id = 1000;
  uint64_t column_id = 10;
  int64_t int_val = 9999;
  ObObj value;
  value.set_int(int_val);

  err = mutator.update(table_name, row_key, column_name, value);
  EXPECT_EQ(0, err);

  ObMutatorCellInfo* mutation = NULL;
  err = mutator.next_cell();
  EXPECT_EQ(0, err);
  err = mutator.get_cell(&mutation);
  EXPECT_EQ(0, err);
  ASSERT_TRUE(NULL != mutation);

  // set table_id and column_id
  mutation->cell_info.column_id_ = column_id;
  mutation->cell_info.table_id_ = table_id;

  check_string(table_name, mutation->cell_info.table_name_);
  check_string(row_key, mutation->cell_info.row_key_);
  check_string(column_name, mutation->cell_info.column_name_);
  int64_t tmp_int = 0;
  mutation->cell_info.value_.get_int(tmp_int);
  EXPECT_EQ(int_val, tmp_int);
  int64_t ext_val = 0;
  mutation->op_type.get_ext(ext_val);
  EXPECT_TRUE(ObActionFlag::OP_UPDATE == ext_val);

  err = mutator.next_cell();
  EXPECT_EQ(OB_ITER_END, err);

  mutator.reset_iter();
  err = mutator.next_cell();
  EXPECT_EQ(0, err);
  err = mutator.get_cell(&mutation);
  EXPECT_EQ(0, err);
  ASSERT_TRUE(NULL != mutation);
  EXPECT_EQ(table_id, mutation->cell_info.table_id_);
  check_string(row_key, mutation->cell_info.row_key_);
  EXPECT_EQ(column_id, mutation->cell_info.column_id_);
}

TEST_F(TestMutator, test_combination)
{
  static const int64_t ROW_NUM = 100;
  static const int64_t COL_NUM = 5;

  int err = OB_SUCCESS;
  //ObCellInfo cell_infos[ROW_NUM][COL_NUM];
  ObMutatorCellInfo cell_infos[ROW_NUM][COL_NUM];
  char row_key_strs[ROW_NUM][50];
  char column_strs[ROW_NUM][COL_NUM][50];
  ObString table_name;
  table_name.assign((char*)"oceanbase_table", static_cast<int32_t>(strlen("oceanbase_table")));
  int64_t op_array_size = 6;
  int64_t op_array[100] = {ObActionFlag::OP_UPDATE, ObActionFlag::OP_INSERT,
    ObActionFlag::OP_DEL_ROW, ObActionFlag::OP_USE_OB_SEM,
    ObActionFlag::OP_USE_DB_SEM};
  // init cell infos
  for (int64_t i = 0; i < ROW_NUM; ++i)
  {
    sprintf(row_key_strs[i], "row_key_%08ld", i);
    for (int64_t j = 0; j < COL_NUM; ++j)
    {
      cell_infos[i][j].cell_info.table_name_ = table_name;
      cell_infos[i][j].cell_info.row_key_.assign(row_key_strs[i], static_cast<int32_t>(strlen(row_key_strs[i])));
      cell_infos[i][j].op_type.set_null();
      int64_t op_array_idx = rand() % op_array_size;

      if (ObActionFlag::OP_UPDATE == op_array[op_array_idx]
          || ObActionFlag::OP_INSERT == op_array[op_array_idx])
      {
        sprintf(column_strs[i][j],"column_name_%08ld_%08ld", i, j);
        cell_infos[i][j].cell_info.column_name_.assign(column_strs[i][j], static_cast<int32_t>(strlen(column_strs[i][j])));

        cell_infos[i][j].cell_info.value_.set_int(1000 + i * COL_NUM + j);
        cell_infos[i][j].op_type.set_ext(op_array[op_array_idx]);
      }
      else if (ObActionFlag::OP_DEL_ROW == op_array[op_array_idx])
      {
        cell_infos[i][j].cell_info.value_.set_ext(op_array[op_array_idx]);
      }
      else if (ObActionFlag::OP_USE_OB_SEM == op_array[op_array_idx]
          || ObActionFlag::OP_USE_DB_SEM == op_array[op_array_idx])
      {
        cell_infos[i][j].cell_info.reset();
        cell_infos[i][j].cell_info.value_.set_ext(op_array[op_array_idx]);
      }
      else
      {
        sprintf(column_strs[i][j],"column_name_%08ld_%08ld", i, j);
        cell_infos[i][j].cell_info.column_name_.assign(column_strs[i][j], static_cast<int32_t>(strlen(column_strs[i][j])));

        cell_infos[i][j].cell_info.value_.set_int(1000 + i * COL_NUM + j, true);
      }
    }
  }

  ObMutator mutator;
  TestMutatorHelper helper(mutator);
  // add cell to array
  for (int64_t i = 0; i < ROW_NUM; ++i)
  {
    for (int64_t j = 0; j < COL_NUM; ++j)
    {
      err = helper.add_cell(cell_infos[i][j]);
      EXPECT_EQ(0, err);
    }
  }

  int64_t serialize_size = mutator.get_serialize_size();
  ASSERT_TRUE(serialize_size > 0);
  char* buf = new char[serialize_size + 1024];
  memset(buf, 0xfe, serialize_size + 1024);
  int64_t pos = 0;
  err = mutator.serialize(buf, serialize_size + 1024, pos);
  EXPECT_EQ(0, err);
  EXPECT_EQ(serialize_size, pos);

  ObMutator new_mutator;
  pos = 0;
  err = new_mutator.deserialize(buf, serialize_size, pos);
  EXPECT_EQ(serialize_size, pos);

  // check result
  int64_t count = 0;
  ObMutatorCellInfo* cur_cell = NULL;
  while (OB_SUCCESS == new_mutator.next_cell())
  {
    err = new_mutator.get_cell(&cur_cell);
    EXPECT_EQ(0, err);
    ASSERT_TRUE(NULL != cur_cell);
    //check_cell_with_name(cell_infos[count / COL_NUM][count % COL_NUM].cell_info, cur_cell->cell_info);
    int64_t row_idx = count / COL_NUM;
    int64_t col_idx = count % COL_NUM;
    int64_t ext_val = 0;
    cur_cell->cell_info.value_.get_ext(ext_val);
    int64_t real_op_int = 0;
    cur_cell->op_type.get_ext(real_op_int);

    if (ObActionFlag::OP_USE_OB_SEM == ext_val || ObActionFlag::OP_USE_DB_SEM == ext_val)
    {
      int64_t tmp_int = 0;
      cell_infos[row_idx][col_idx].cell_info.value_.get_ext(tmp_int);
      EXPECT_EQ(tmp_int, ext_val);
    }
    else if (ObActionFlag::OP_DEL_ROW == ext_val)
    {
      check_string(cell_infos[row_idx][col_idx].cell_info.row_key_, cur_cell->cell_info.row_key_);
      check_string(cell_infos[row_idx][col_idx].cell_info.table_name_, cur_cell->cell_info.table_name_);
      int64_t tmp_int = 0;
      cell_infos[row_idx][col_idx].cell_info.value_.get_ext(tmp_int);
      EXPECT_EQ(tmp_int, ext_val);
    }
    else if (ObActionFlag::OP_UPDATE == real_op_int || ObActionFlag::OP_INSERT == real_op_int)
    {
      check_string(cell_infos[row_idx][col_idx].cell_info.row_key_, cur_cell->cell_info.row_key_);
      check_string(cell_infos[row_idx][col_idx].cell_info.table_name_, cur_cell->cell_info.table_name_);
      check_string(cell_infos[row_idx][col_idx].cell_info.column_name_, cur_cell->cell_info.column_name_);
      int64_t tmp_int = 0;
      cell_infos[row_idx][col_idx].cell_info.value_.get_ext(tmp_int);
      EXPECT_EQ(tmp_int, ext_val);
      int64_t op_int = 0;
      cell_infos[row_idx][col_idx].op_type.get_ext(op_int);
      EXPECT_EQ(op_int, real_op_int);
    }
    else
    {
      // add op
      check_string(cell_infos[row_idx][col_idx].cell_info.row_key_, cur_cell->cell_info.row_key_);
      check_string(cell_infos[row_idx][col_idx].cell_info.table_name_, cur_cell->cell_info.table_name_);
      check_string(cell_infos[row_idx][col_idx].cell_info.column_name_, cur_cell->cell_info.column_name_);

      bool is_add = false;
      cur_cell->cell_info.value_.get_int(ext_val, is_add);
      int64_t tmp_int = 0;
      cell_infos[row_idx][col_idx].cell_info.value_.get_int(tmp_int);
      EXPECT_EQ(tmp_int, ext_val);
      EXPECT_TRUE(is_add);
    }
    ++count;
  }
  EXPECT_EQ(ROW_NUM * COL_NUM, count);

  delete[] buf;
}

TEST_F(TestMutator, test_combination_id)
{
  static const int64_t ROW_NUM = 100;
  static const int64_t COL_NUM = 5;

  int err = OB_SUCCESS;
  //ObCellInfo cell_infos[ROW_NUM][COL_NUM];
  ObMutatorCellInfo cell_infos[ROW_NUM][COL_NUM];
  char row_key_strs[ROW_NUM][50];
  //char column_strs[ROW_NUM][COL_NUM][50];
  ObString table_name;
  uint64_t table_id = 10;
  table_name.assign((char*)"oceanbase_table", static_cast<int32_t>(strlen("oceanbase_table")));
  int64_t op_array_size = 6;
  int64_t op_array[100] = {ObActionFlag::OP_UPDATE, ObActionFlag::OP_INSERT,
    ObActionFlag::OP_DEL_ROW, ObActionFlag::OP_USE_OB_SEM,
    ObActionFlag::OP_USE_DB_SEM};
  // init cell infos
  for (int64_t i = 0; i < ROW_NUM; ++i)
  {
    sprintf(row_key_strs[i], "row_key_%08ld", i);
    for (int64_t j = 0; j < COL_NUM; ++j)
    {
      //cell_infos[i][j].cell_info.table_name_ = table_name;
      cell_infos[i][j].cell_info.table_id_ = table_id;
      cell_infos[i][j].cell_info.row_key_.assign(row_key_strs[i], static_cast<int32_t>(strlen(row_key_strs[i])));
      cell_infos[i][j].op_type.set_null();

      int64_t op_array_idx = rand() % op_array_size;

      if (ObActionFlag::OP_UPDATE == op_array[op_array_idx]
          || ObActionFlag::OP_INSERT == op_array[op_array_idx])
      {
        //sprintf(column_strs[i][j],"column_name_%08ld_%08ld", i, j);
        //cell_infos[i][j].cell_info.column_name_.assign(column_strs[i][j], strlen(column_strs[i][j]));
        cell_infos[i][j].cell_info.column_id_ = j + 1;

        cell_infos[i][j].cell_info.value_.set_int(1000 + i * COL_NUM + j);
        cell_infos[i][j].op_type.set_ext(op_array[op_array_idx]);
      }
      else if (ObActionFlag::OP_DEL_ROW == op_array[op_array_idx])
      {
        cell_infos[i][j].cell_info.value_.set_ext(op_array[op_array_idx]);
      }
      else if (ObActionFlag::OP_USE_OB_SEM == op_array[op_array_idx]
          || ObActionFlag::OP_USE_DB_SEM == op_array[op_array_idx])
      {
        cell_infos[i][j].cell_info.reset();
        cell_infos[i][j].cell_info.value_.set_ext(op_array[op_array_idx]);
      }
      else
      {
        //sprintf(column_strs[i][j],"column_name_%08ld_%08ld", i, j);
        //cell_infos[i][j].cell_info.column_name_.assign(column_strs[i][j], strlen(column_strs[i][j]));
        cell_infos[i][j].cell_info.column_id_ = j + 1;

        cell_infos[i][j].cell_info.value_.set_int(1000 + i * COL_NUM + j, true);
      }
    }
  }

  ObMutator mutator;
  TestMutatorHelper helper(mutator);
  // add cell to array
  for (int64_t i = 0; i < ROW_NUM; ++i)
  {
    for (int64_t j = 0; j < COL_NUM; ++j)
    {
      err = helper.add_cell(cell_infos[i][j]);
      EXPECT_EQ(0, err);
    }
  }

  int64_t serialize_size = mutator.get_serialize_size();
  ASSERT_TRUE(serialize_size > 0);
  char* buf = new char[serialize_size + 1024];
  memset(buf, 0xfe, serialize_size + 1024);
  int64_t pos = 0;
  err = mutator.serialize(buf, serialize_size + 1024, pos);
  EXPECT_EQ(0, err);
  EXPECT_EQ(serialize_size, pos);

  ObMutator new_mutator;
  pos = 0;
  err = new_mutator.deserialize(buf, serialize_size, pos);
  EXPECT_EQ(serialize_size, pos);

  // check result
  int64_t count = 0;
  ObMutatorCellInfo* cur_cell = NULL;
  while (OB_SUCCESS == new_mutator.next_cell())
  {
    err = new_mutator.get_cell(&cur_cell);
    EXPECT_EQ(0, err);
    ASSERT_TRUE(NULL != cur_cell);
    //check_cell_with_name(cell_infos[count / COL_NUM][count % COL_NUM].cell_info, cur_cell->cell_info);
    int64_t row_idx = count / COL_NUM;
    int64_t col_idx = count % COL_NUM;
    int64_t ext_val = 0;
    cur_cell->cell_info.value_.get_ext(ext_val);
    int64_t real_op_int = 0;
    cur_cell->op_type.get_ext(real_op_int);

    if (ObActionFlag::OP_USE_OB_SEM == ext_val || ObActionFlag::OP_USE_DB_SEM == ext_val)
    {
      int64_t tmp_int = 0;
      cell_infos[row_idx][col_idx].cell_info.value_.get_ext(tmp_int);
      EXPECT_EQ(tmp_int, ext_val);
    }
    else if (ObActionFlag::OP_DEL_ROW == ext_val)
    {
      check_string(cell_infos[row_idx][col_idx].cell_info.row_key_, cur_cell->cell_info.row_key_);
      //check_string(cell_infos[row_idx][col_idx].cell_info.table_name_, cur_cell->cell_info.table_name_);
      EXPECT_EQ(cell_infos[row_idx][col_idx].cell_info.table_id_, cur_cell->cell_info.table_id_);
      int64_t tmp_int = 0;
      cell_infos[row_idx][col_idx].cell_info.value_.get_ext(tmp_int);
      EXPECT_EQ(tmp_int, ext_val);
    }
    else if (ObActionFlag::OP_UPDATE == real_op_int || ObActionFlag::OP_INSERT == real_op_int)
    {
      check_string(cell_infos[row_idx][col_idx].cell_info.row_key_, cur_cell->cell_info.row_key_);
      //check_string(cell_infos[row_idx][col_idx].cell_info.table_name_, cur_cell->cell_info.table_name_);
      //check_string(cell_infos[row_idx][col_idx].cell_info.column_name_, cur_cell->cell_info.column_name_);
      EXPECT_EQ(cell_infos[row_idx][col_idx].cell_info.table_id_, cur_cell->cell_info.table_id_);
      EXPECT_EQ(cell_infos[row_idx][col_idx].cell_info.column_id_, cur_cell->cell_info.column_id_);
      int64_t tmp_int = 0;
      cell_infos[row_idx][col_idx].cell_info.value_.get_ext(tmp_int);
      EXPECT_EQ(tmp_int, ext_val);
      int64_t op_int = 0;
      cell_infos[row_idx][col_idx].op_type.get_ext(op_int);
      EXPECT_EQ(op_int, real_op_int);
    }
    else
    {
      // add op
      check_string(cell_infos[row_idx][col_idx].cell_info.row_key_, cur_cell->cell_info.row_key_);
      //check_string(cell_infos[row_idx][col_idx].cell_info.table_name_, cur_cell->cell_info.table_name_);
      //check_string(cell_infos[row_idx][col_idx].cell_info.column_name_, cur_cell->cell_info.column_name_);
      EXPECT_EQ(cell_infos[row_idx][col_idx].cell_info.table_id_, cur_cell->cell_info.table_id_);
      EXPECT_EQ(cell_infos[row_idx][col_idx].cell_info.column_id_, cur_cell->cell_info.column_id_);

      bool is_add = false;
      cur_cell->cell_info.value_.get_int(ext_val, is_add);
      int64_t tmp_int = 0;
      cell_infos[row_idx][col_idx].cell_info.value_.get_int(tmp_int);
      EXPECT_EQ(tmp_int, ext_val);
      EXPECT_TRUE(is_add);
    }
    ++count;
  }
  EXPECT_EQ(ROW_NUM * COL_NUM, count);

  delete[] buf;
}

TEST_F(TestMutator, test_name_and_id)
{
  int err = 0;
  ObMutatorCellInfo cell_info1;
  ObMutatorCellInfo cell_info2;

  cell_info1.cell_info.table_name_.assign((char*)"table", static_cast<int32_t>(strlen("table")));
  cell_info1.cell_info.column_name_.assign((char*)"column", static_cast<int32_t>(strlen("column")));
  cell_info1.cell_info.row_key_.assign((char*)"row_key", static_cast<int32_t>(strlen("row_key")));
  cell_info1.cell_info.value_.set_int(100);

  cell_info2.cell_info.table_id_ = 10;
  cell_info2.cell_info.column_id_ = 2;
  cell_info2.cell_info.row_key_.assign((char*)"row_key", static_cast<int32_t>(strlen("row_key")));
  cell_info2.cell_info.value_.set_int(100);

  ObMutator mutator;
  err = mutator.add_cell(cell_info1);
  EXPECT_EQ(0, err);
  err = mutator.add_cell(cell_info2);
  EXPECT_NE(0, err);

  ObMutatorCellInfo cell_info3;
  cell_info3.cell_info.table_name_.assign((char*)"table", static_cast<int32_t>(strlen("table")));
  cell_info3.cell_info.column_id_ = 2;
  cell_info3.cell_info.row_key_.assign((char*)"row_key", static_cast<int32_t>(strlen("row_key")));
  cell_info3.cell_info.value_.set_int(100);

  ObMutator mutator1;
  err = mutator1.add_cell(cell_info3);
  EXPECT_NE(0, err);
}

TEST_F(TestMutator, test_mutator_with_exist_condition)
{
  static const int64_t ROW_NUM = 100;
  static const int64_t COL_NUM = 5;

  int err = OB_SUCCESS;
  ObCellInfo cell_infos[ROW_NUM][COL_NUM];
  char row_key_strs[ROW_NUM][50];
  char column_strs[ROW_NUM][COL_NUM][50];
  ObString table_name;
  table_name.assign((char*)"oceanbase_table", static_cast<int32_t>(strlen("oceanbase_table")));
  // init cell infos
  for (int64_t i = 0; i < ROW_NUM; ++i)
  {
    sprintf(row_key_strs[i], "row_key_%08ld", i);
    for (int64_t j = 0; j < COL_NUM; ++j)
    {
      cell_infos[i][j].table_name_ = table_name;
      cell_infos[i][j].row_key_.assign(row_key_strs[i], static_cast<int32_t>(strlen(row_key_strs[i])));
      //cell_infos[i][j].op_info_.set_update();

      sprintf(column_strs[i][j],"column_name_%08ld_%08ld", i, j);
      cell_infos[i][j].column_name_.assign(column_strs[i][j], static_cast<int32_t>(strlen(column_strs[i][j])));

      cell_infos[i][j].value_.set_int(1000 + i * COL_NUM + j);
    }
  }

  ObMutator mutator;
  TestMutatorHelper helper(mutator);
  // add cell to array
  for (int64_t i = 0; i < ROW_NUM; ++i)
  {
    for (int64_t j = 0; j < COL_NUM; ++j)
    {
      ObMutatorCellInfo mutation;
      mutation.cell_info = cell_infos[i][j];
      mutation.op_type.set_ext(ObActionFlag::OP_UPDATE);

      err = helper.add_cell(mutation);
      EXPECT_EQ(0, err);
    }
  }
  
  // add update condition
  ObUpdateCondition& update_condition = mutator.get_update_condition();
  char table_name_str[128];
  strcpy(table_name_str, "table_name");
  char row_key_str[128];
  strcpy(row_key_str, "row_key");
  char column_name_str[128];
  strcpy(column_name_str, "column_name");
  
  table_name.assign(table_name_str, static_cast<int32_t>(strlen(table_name_str)));
  ObString row_key;
  row_key.assign(row_key_str, static_cast<int32_t>(strlen(row_key_str)));
  ObString column_name;
  column_name.assign(column_name_str, static_cast<int32_t>(strlen(column_name_str)));
  ObObj value;
  value.set_ext(ObActionFlag::OP_ROW_DOES_NOT_EXIST);
  
  ObCondInfo temp_cond;
  temp_cond.set(table_name, row_key, false);
  err = update_condition.add_cond(table_name, row_key, false);
  EXPECT_EQ(0, err);
  EXPECT_EQ(1, update_condition.get_count());
  check_exist_cond_info(temp_cond, *update_condition[0]);
  
  ObCondInfo cond_info;
  cond_info.set(NE, table_name, row_key, column_name, value);
  err = update_condition.add_cond(table_name, row_key, column_name, NE, value);
  EXPECT_EQ(0, err);
  EXPECT_EQ(2, update_condition.get_count());
  check_exist_cond_info(cond_info, *update_condition[1]);

  int64_t serialize_size = mutator.get_serialize_size();
  ASSERT_TRUE(serialize_size > 0);
  char* buf = new char[serialize_size + 1024];
  memset(buf, 0xfe, serialize_size + 1024);
  int64_t pos = 0;
  err = mutator.serialize(buf, serialize_size, pos);
  EXPECT_EQ(0, err);
  EXPECT_EQ(serialize_size, pos);

  ObMutator new_mutator;
  pos = 0;
  err = new_mutator.deserialize(buf, serialize_size, pos);
  EXPECT_EQ(serialize_size, pos);
  // check condition
  const ObUpdateCondition& new_condition = new_mutator.get_update_condition();
  EXPECT_EQ(2, new_condition.get_count());
  check_exist_cond_info(cond_info, *new_condition[1]);

  // check result
  int64_t count = 0;
  ObMutatorCellInfo* cur_cell = NULL;
  while (OB_SUCCESS == new_mutator.next_cell())
  {
    err = new_mutator.get_cell(&cur_cell);
    EXPECT_EQ(0, err);
    ASSERT_TRUE(NULL != cur_cell);
    check_cell_with_name(cell_infos[count / COL_NUM][count % COL_NUM], cur_cell->cell_info);
    int64_t ext_val = 0;
    cur_cell->op_type.get_ext(ext_val);
    EXPECT_TRUE(ObActionFlag::OP_UPDATE ==ext_val);
    ++count;
  }
  EXPECT_EQ(ROW_NUM * COL_NUM, count);

  delete[] buf;
}

TEST_F(TestMutator, test_mutator_with_cell_condition)
{
  static const int64_t ROW_NUM = 100;
  static const int64_t COL_NUM = 5;

  int err = OB_SUCCESS;
  ObCellInfo cell_infos[ROW_NUM][COL_NUM];
  char row_key_strs[ROW_NUM][50];
  char column_strs[ROW_NUM][COL_NUM][50];
  ObString table_name;
  table_name.assign((char*)"oceanbase_table", static_cast<int32_t>(strlen("oceanbase_table")));
  // init cell infos
  for (int64_t i = 0; i < ROW_NUM; ++i)
  {
    sprintf(row_key_strs[i], "row_key_%08ld", i);
    for (int64_t j = 0; j < COL_NUM; ++j)
    {
      cell_infos[i][j].table_name_ = table_name;
      cell_infos[i][j].row_key_.assign(row_key_strs[i], static_cast<int32_t>(strlen(row_key_strs[i])));
      //cell_infos[i][j].op_info_.set_update();

      sprintf(column_strs[i][j],"column_name_%08ld_%08ld", i, j);
      cell_infos[i][j].column_name_.assign(column_strs[i][j], static_cast<int32_t>(strlen(column_strs[i][j])));

      cell_infos[i][j].value_.set_int(1000 + i * COL_NUM + j);
    }
  }

  ObMutator mutator;
  TestMutatorHelper helper(mutator);
  // add cell to array
  for (int64_t i = 0; i < ROW_NUM; ++i)
  {
    for (int64_t j = 0; j < COL_NUM; ++j)
    {
      ObMutatorCellInfo mutation;
      mutation.cell_info = cell_infos[i][j];
      mutation.op_type.set_ext(ObActionFlag::OP_UPDATE);

      err = helper.add_cell(mutation);
      EXPECT_EQ(0, err);
    }
  }
  
  // add update condition
  ObUpdateCondition& update_condition = mutator.get_update_condition();
  char table_name_str[128];
  strcpy(table_name_str, "table_name");
  char row_key_str[128];
  strcpy(row_key_str, "row_key");
  char column_name_str[128];
  strcpy(column_name_str, "column_name");
  ObCondInfo cond_info;
  
  table_name.assign(table_name_str, static_cast<int32_t>(strlen(table_name_str)));
  ObString row_key;
  row_key.assign(row_key_str, static_cast<int32_t>(strlen(row_key_str)));
  ObString column_name;
  column_name.assign(column_name_str, static_cast<int32_t>(strlen(column_name_str)));
  ObObj value;
  value.set_int(100);
  cond_info.set(EQ, table_name, row_key, column_name, value);

  err = update_condition.add_cond(table_name, row_key, column_name, EQ, value);
  EXPECT_EQ(0, err);
  EXPECT_EQ(1, update_condition.get_count());
  check_cond_info(cond_info, *update_condition[0]);

  int64_t serialize_size = mutator.get_serialize_size();
  ASSERT_TRUE(serialize_size > 0);
  char* buf = new char[serialize_size + 1024];
  memset(buf, 0xfe, serialize_size + 1024);
  int64_t pos = 0;
  err = mutator.serialize(buf, serialize_size, pos);
  EXPECT_EQ(0, err);
  EXPECT_EQ(serialize_size, pos);

  ObMutator new_mutator;
  pos = 0;
  err = new_mutator.deserialize(buf, serialize_size, pos);
  EXPECT_EQ(serialize_size, pos);
  // check condition
  const ObUpdateCondition& new_condition = new_mutator.get_update_condition();
  EXPECT_EQ(1, new_condition.get_count());
  check_cond_info(cond_info, *new_condition[0]);

  // check result
  int64_t count = 0;
  ObMutatorCellInfo* cur_cell = NULL;
  while (OB_SUCCESS == new_mutator.next_cell())
  {
    err = new_mutator.get_cell(&cur_cell);
    EXPECT_EQ(0, err);
    ASSERT_TRUE(NULL != cur_cell);
    check_cell_with_name(cell_infos[count / COL_NUM][count % COL_NUM], cur_cell->cell_info);
    int64_t ext_val = 0;
    cur_cell->op_type.get_ext(ext_val);
    EXPECT_TRUE(ObActionFlag::OP_UPDATE ==ext_val);
    ++count;
  }
  EXPECT_EQ(ROW_NUM * COL_NUM, count);

  delete[] buf;
}

TEST_F(TestMutator, test_mutator_with_prefetch_data)
{
  static const int64_t ROW_NUM = 100;
  static const int64_t COL_NUM = 5;

  int err = OB_SUCCESS;
  ObCellInfo cell_infos[ROW_NUM][COL_NUM];
  char row_key_strs[ROW_NUM][50];
  char column_strs[ROW_NUM][COL_NUM][50];
  ObString table_name;
  table_name.assign((char*)"oceanbase_table", static_cast<int32_t>(strlen("oceanbase_table")));
  // init cell infos
  for (int64_t i = 0; i < ROW_NUM; ++i)
  {
    sprintf(row_key_strs[i], "row_key_%08ld", i);
    for (int64_t j = 0; j < COL_NUM; ++j)
    {
      cell_infos[i][j].table_name_ = table_name;
      cell_infos[i][j].row_key_.assign(row_key_strs[i], static_cast<int32_t>(strlen(row_key_strs[i])));

      sprintf(column_strs[i][j],"column_name_%08ld_%08ld", i, j);
      cell_infos[i][j].column_name_.assign(column_strs[i][j], static_cast<int32_t>(strlen(column_strs[i][j])));

      cell_infos[i][j].value_.set_int(1000 + i * COL_NUM + j);
    }
  }

  ObMutator mutator;
  EXPECT_TRUE(ObMutator::NORMAL_UPDATE == mutator.get_mutator_type());
  mutator.set_mutator_type(ObMutator::CORRECTION_UPDATE);
  EXPECT_TRUE(ObMutator::CORRECTION_UPDATE == mutator.get_mutator_type());
  TestMutatorHelper helper(mutator);
  // add cell to array
  for (int64_t i = 0; i < ROW_NUM; ++i)
  {
    for (int64_t j = 0; j < COL_NUM; ++j)
    {
      ObMutatorCellInfo mutation;
      mutation.cell_info = cell_infos[i][j];
      mutation.op_type.set_ext(ObActionFlag::OP_UPDATE);

      err = helper.add_cell(mutation);
      EXPECT_EQ(0, err);
    }
  }
  
  // temp result
  #if TRUE
  {
    ObCellInfo cell;
    char * rowkey = (char*)"test_rowkey";
    for (int64_t i = 0; i < 10; ++i)
    {
      cell.table_id_ = i;
      cell.row_key_.assign(rowkey, static_cast<int32_t>(strlen(rowkey)));
      cell.column_id_ = i + 10;
      cell.value_.set_null();
      EXPECT_TRUE(mutator.get_prefetch_data().get().add_cell(cell) == OB_SUCCESS);
    }
  }
  #endif

  int64_t serialize_size = mutator.get_serialize_size();
  ASSERT_TRUE(serialize_size > 0);
  char* buf = new char[serialize_size + 1024];
  memset(buf, 0xfe, serialize_size + 1024);
  int64_t pos = 0;
  err = mutator.serialize(buf, serialize_size, pos);
  EXPECT_EQ(0, err);
  EXPECT_EQ(serialize_size, pos);

  ObMutator new_mutator;
  pos = 0;
  err = new_mutator.deserialize(buf, serialize_size, pos);
  EXPECT_EQ(serialize_size, pos);
  EXPECT_TRUE(ObMutator::CORRECTION_UPDATE == new_mutator.get_mutator_type());
  
  #if TRUE
  {
    ObScanner & result = new_mutator.get_prefetch_data().get();
    ObCellInfo *cur_cell = NULL;
    uint64_t count = 0;
    char * row_key = (char*)"test_rowkey";
    ObString rowkey;
    rowkey.assign(row_key, static_cast<int32_t>(strlen(row_key)));
    while (result.next_cell() == OB_SUCCESS)
    {
      EXPECT_TRUE(result.get_cell(&cur_cell) == OB_SUCCESS);
      EXPECT_TRUE(cur_cell->table_id_ == count);
      EXPECT_TRUE(cur_cell->column_id_ == (count + 10));
      EXPECT_TRUE(cur_cell->row_key_ == rowkey);
      ++count;
    }
    EXPECT_TRUE(count == 10);
    result.reset_iter();
  }
  #endif

  // check result
  int64_t count = 0;
  ObMutatorCellInfo* cur_cell = NULL;
  while (OB_SUCCESS == new_mutator.next_cell())
  {
    err = new_mutator.get_cell(&cur_cell);
    EXPECT_EQ(0, err);
    ASSERT_TRUE(NULL != cur_cell);
    check_cell_with_name(cell_infos[count / COL_NUM][count % COL_NUM], cur_cell->cell_info);
    int64_t ext_val = 0;
    cur_cell->op_type.get_ext(ext_val);
    EXPECT_TRUE(ObActionFlag::OP_UPDATE ==ext_val);
    ++count;
  }
  EXPECT_EQ(ROW_NUM * COL_NUM, count);

  delete[] buf;
}

TEST_F(TestMutator, test_mutator_different_tablename_and_same_rowkey)
{
  ObMutator mutator;

  ObString table_name1;
  table_name1.assign_ptr("table1", 6);
  ObString table_name2;
  table_name2.assign_ptr("table2", 6);
  ObString rowkey;
  rowkey.assign_ptr("rowkey", 6);
  ObString column_name1;
  column_name1.assign_ptr("column1", 7);
  ObString column_name2;
  column_name2.assign_ptr("column2", 7);

  mutator.update(table_name1, rowkey, column_name1, ObObj());
  mutator.update(table_name2, rowkey, column_name2, ObObj());

  int64_t pos = 0;
  int64_t size = 2 * 1024 * 1024;
  char *buffer = new char[size];
  mutator.serialize(buffer, size, pos);
  pos = 0;
  mutator.deserialize(buffer, size, pos);
  delete[] buffer;

  int ret = mutator.next_cell();
  EXPECT_EQ(OB_SUCCESS, ret);
  ObMutatorCellInfo *nil = NULL;
  ObMutatorCellInfo *ci = NULL;
  bool irc = false;
  bool irf = false;
  ret = mutator.get_cell(&ci, &irc, &irf);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_NE(nil, ci);
  EXPECT_EQ(true, irc);
  EXPECT_EQ(true, irf);

  ret = mutator.next_cell();
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = mutator.get_cell(&ci, &irc, &irf);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_NE(nil, ci);
  EXPECT_EQ(true, irc);
  EXPECT_EQ(true, irf);

  ret = mutator.next_cell();
  EXPECT_EQ(OB_ITER_END, ret);
}

} // end namespace common
} // end namespace tests
} // end namespace oceanbase


int main(int argc, char** argv)
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

