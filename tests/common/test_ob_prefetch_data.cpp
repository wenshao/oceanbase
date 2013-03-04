#include "gtest/gtest.h"
#include "ob_malloc.h"
#include "ob_range.h"
#include "ob_common_param.h"
#include "ob_read_common_data.h"
#include "ob_scanner.h"
#include "ob_prefetch_data.h"

using namespace oceanbase;
using namespace common;

int main(int argc, char **argv)
{
  ob_init_memory_pool();
  testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}

class TestPrefetchData: public ::testing::Test
{
public:
  
  virtual void SetUp()
  {
  }

  virtual void TearDown()
  {
  }
};

TEST_F(TestPrefetchData, add_cell)
{
  ObPrefetchData data;
  EXPECT_TRUE(true == data.is_empty());
  ObCellInfo cell;
  char * row_key = (char*)"test_row_key";
  ObString rowkey;
  rowkey.assign(row_key, static_cast<int32_t>(strlen(row_key)));
  cell.row_key_ = rowkey;
  ObObj value;
  value.set_int(1234);
  cell.table_name_ = rowkey;
  cell.column_name_ = rowkey;
  EXPECT_TRUE(OB_SUCCESS != data.add_cell(cell));
  EXPECT_TRUE(true == data.is_empty());
  // set rowkey to null
  rowkey.assign(NULL, 0);
  cell.table_name_ = rowkey;
  cell.column_name_ = rowkey;
  const int64_t count = 100;
  for (int64_t i = 0; i < count; ++i)
  {
    cell.table_id_ = i;
    cell.column_id_ = 0;
    cell.value_ = value;
    EXPECT_TRUE(OB_SUCCESS == data.add_cell(cell));
  }
  EXPECT_TRUE(false == data.is_empty());
  data.reset();
  EXPECT_TRUE(true == data.is_empty());
}

TEST_F(TestPrefetchData, scanner)
{
  ObPrefetchData data;
  ObCellInfo cell;
  char * row_key = (char*)"test_row_key";
  ObString rowkey;
  rowkey.assign(row_key, static_cast<int32_t>(strlen(row_key)));
  cell.row_key_ = rowkey;
  ObObj value;
  const int64_t count = 100;
  for (int64_t i = 0; i < count; ++i)
  {
    cell.table_id_ = i;
    cell.column_id_ = i + 10;
    value.set_int(i * 2);
    cell.value_ = value;
    EXPECT_TRUE(OB_SUCCESS == data.add_cell(cell));
  }
  EXPECT_TRUE(false == data.is_empty());
  
  ObScanner & scanner = data.get();
  ObCellInfo * pcell = NULL;
  int64_t i = 0;
  int64_t int_value = 0;
  int ret = OB_SUCCESS;
  while (OB_SUCCESS == (ret = scanner.next_cell()))
  {
    EXPECT_TRUE(OB_SUCCESS == scanner.get_cell(&pcell));
    EXPECT_TRUE(pcell->table_id_ == (uint64_t)i);
    EXPECT_TRUE(pcell->column_id_ == ((uint64_t)i + 10));
    EXPECT_TRUE(pcell->row_key_ == rowkey);
    EXPECT_TRUE(OB_SUCCESS == pcell->value_.get_int(int_value));
    EXPECT_TRUE(int_value == i * 2);
    ++i;
  }

  EXPECT_TRUE(ret == OB_ITER_END);
  EXPECT_TRUE(i == count);
}


TEST_F(TestPrefetchData, serialize)
{
  ObPrefetchData data;
  EXPECT_TRUE(true == data.is_empty());
  // empty serialize
  int64_t len = data.get_serialize_size();
  char * buffer = new char[len];
  EXPECT_TRUE(buffer != NULL);
  int64_t pos = 0;
  EXPECT_TRUE(OB_SUCCESS == data.serialize(buffer, len, pos));
  EXPECT_TRUE(pos == data.get_serialize_size());
  
  pos = 0;
  ObPrefetchData temp_data;
  EXPECT_TRUE(OB_SUCCESS == temp_data.deserialize(buffer, len, pos));
  EXPECT_TRUE(pos == temp_data.get_serialize_size());
  EXPECT_TRUE(pos == len);
  EXPECT_TRUE(true == temp_data.is_empty());
  delete []buffer;
  buffer = NULL;

  // add new cells
  ObCellInfo cell;
  char * row_key = (char*)"test_row_key";
  ObString rowkey;
  rowkey.assign(row_key, static_cast<int32_t>(strlen(row_key)));
  cell.row_key_ = rowkey;
  ObObj value;
  const int64_t count = 100;
  for (int64_t i = 0; i < count; ++i)
  {
    cell.table_id_ = i;
    cell.column_id_ = i + 10;
    value.set_int(i * 2);
    cell.value_ = value;
    EXPECT_TRUE(OB_SUCCESS == data.add_cell(cell));
  }
  EXPECT_TRUE(false == data.is_empty());
  
  // empty serialize
  buffer = new char[data.get_serialize_size()];
  EXPECT_TRUE(buffer != NULL);
  pos = 0;
  EXPECT_TRUE(OB_SUCCESS != data.serialize(buffer, data.get_serialize_size()- 1, pos));
  pos = 0;
  EXPECT_TRUE(OB_SUCCESS == data.serialize(buffer, data.get_serialize_size(), pos));
  EXPECT_TRUE(pos == data.get_serialize_size());
  
  pos = 0;
  EXPECT_TRUE(OB_SUCCESS == temp_data.deserialize(buffer, data.get_serialize_size(), pos));
  EXPECT_TRUE(pos == temp_data.get_serialize_size());
  EXPECT_TRUE(false == temp_data.is_empty());
  // not reset 
  // deserialize size again
  pos = 0;
  EXPECT_TRUE(OB_SUCCESS == temp_data.deserialize(buffer, data.get_serialize_size(), pos));
  EXPECT_TRUE(pos == temp_data.get_serialize_size());
  EXPECT_TRUE(false == temp_data.is_empty());
  
  ObScanner & scanner = temp_data.get();
  ObCellInfo * pcell = NULL;
  int64_t i = 0;
  int ret = OB_SUCCESS;
  int64_t int_value = 0;
  while (OB_SUCCESS == (ret = scanner.next_cell()))
  {
    EXPECT_TRUE(OB_SUCCESS == scanner.get_cell(&pcell));
    EXPECT_TRUE(pcell->table_id_ == (uint64_t)i);
    EXPECT_TRUE(pcell->column_id_ == ((uint64_t)i + 10));
    EXPECT_TRUE(pcell->row_key_ == rowkey);
    EXPECT_TRUE(OB_SUCCESS == pcell->value_.get_int(int_value));
    EXPECT_TRUE(int_value == i * 2);
    ++i;
  }

  EXPECT_TRUE(ret == OB_ITER_END);
  EXPECT_TRUE(i == count);
  
  delete []buffer;
  buffer = NULL;
}



