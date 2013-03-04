/**
 *  (C) 2010-2011 Taobao Inc.
 *  
 *  This program is free software; you can redistribute it
 *  and/or modify it under the terms of the GNU General Public
 *  License version 2 as published by the Free Software
 *  Foundation.
 *
 *  test_sstable_block_scanner.cpp is for what ...
 *
 *  Authors:
 *     qushan<qushan@taobao.com>
 *        
 */

#include <gtest/gtest.h>
#include "common/ob_malloc.h"
#include "common/ob_object.h"
#include "common/ob_define.h"
#include "common/page_arena.h"
#include "sstable/ob_sstable_block_reader.h"
#include "sstable/ob_sstable_block_scanner.h"
#include "sstable/ob_scan_column_indexes.h"
#include "sstable/ob_sstable_block_index_v2.h"
#include "sstable/ob_sstable_row.h"
#include "sstable/ob_sstable_block_builder.h"
#include "sstable/ob_sstable_trailer.h"
using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::sstable;

const int64_t TABLE_ID = 1;
static const char* g_key[] = { "aoo", "boo", "coo", "doo", "foo", "koo" };
const int64_t block_internal_bufsiz = 1024;
static char block_internal_buffer[block_internal_bufsiz];


void create_row_key(
    CharArena& allocator,
    ObString& rowkey,
    const char* sk)
{
  int64_t sz = strlen(sk);
  char* msk = allocator.alloc(sz);
  memcpy(msk, sk, sz);
  rowkey.assign_ptr(msk, static_cast<int32_t>(sz));
}

void create_row(
    CharArena& allocator,
    ObSSTableRow& row,
    const char* key,
    int64_t f1,
    int64_t f2,
    char* f3
    )
{
  ObString row_key;
  create_row_key(allocator, row_key, key);
  row.set_row_key(row_key);
  ObObj obj1;
  obj1.set_int(f1);
  ObObj obj2;
  obj2.set_int(f2);
  ObObj obj3;
  ObString temp;
  temp.assign_ptr(f3, static_cast<int32_t>(strlen(f3)));
  obj3.set_varchar(temp);

  row.add_obj(obj1);
  row.add_obj(obj2);
  row.add_obj(obj3);
  row.set_table_id(TABLE_ID);
  row.set_column_group_id(0);
}

int create_block( CharArena& allocator, ObSSTableBlockBuilder& builder, const int32_t row_count)
{
  int ret = builder.init();
  if (ret) return ret;

  for (int i = 0; i < row_count; ++i)
  {
    ObSSTableRow row;
    create_row(allocator, row, g_key[i], i, i, (char*)g_key[i]);
    ret = builder.add_row(row);
    if (ret) return ret;
  }

  ret = builder.build_block();
  return ret;
}

void create_schema(ObSSTableSchema& schema)
{
  ObSSTableSchemaColumnDef def1, def2, def3;
  def1.table_id_ = TABLE_ID;
  def1.column_group_id_ = 0;
  def1.column_name_id_ = 5;
  def1.column_value_type_ = ObIntType;

  def2.table_id_ = TABLE_ID;
  def2.column_group_id_ = 0;
  def2.column_name_id_ = 6;
  def2.column_value_type_ = ObIntType;

  def3.table_id_ = TABLE_ID;
  def3.column_group_id_ = 0;
  def3.column_name_id_ = 7;
  def3.column_value_type_ = ObVarcharType;

  schema.add_column_def(def1);
  schema.add_column_def(def2);
  schema.add_column_def(def3);
}

void create_query_columns(
    ObScanColumnIndexes& indexes, 
    const ObSSTableSchema& schema,
    const int32_t* column_ids,
    const int32_t  column_count
    )
{
  for (int32_t i = 0; i < column_count; ++i)
  {
    int64_t offset = schema.find_offset_column_group_schema(TABLE_ID, 0, column_ids[i]);
    if (offset < 0) offset = ObScanColumnIndexes::NOT_EXIST_COLUMN;
    indexes.add_column_id(offset, column_ids[i]);
  }
}

void create_scan_param(ObRange& range, const char* start_key, const char* end_key,
    bool inclusive_start = true, bool inclusive_end = true, 
    bool is_min_value = false, bool is_max_value = false)
{
  const char* table_name = "nothing";
  ObString ob_table_name(0, static_cast<int32_t>(strlen(table_name)), (char*)table_name);
  //ObRange range;
  range.table_id_ = TABLE_ID;
  ObString ob_start_key(0, static_cast<int32_t>(strlen(start_key)), (char*)start_key);
  ObString ob_end_key(0, static_cast<int32_t>(strlen(end_key)), (char*)end_key);
  range.start_key_ = ob_start_key;
  range.end_key_ = ob_end_key;
  if (inclusive_start) range.border_flag_.set_inclusive_start();
  if (inclusive_end) range.border_flag_.set_inclusive_end();
  if (is_min_value) range.border_flag_.set_min_value();
  if (is_max_value) range.border_flag_.set_max_value();

  //scan_param.set(TABLE_ID, ob_table_name, range);
  /*
  scan_param.add_column(5);
  scan_param.add_column(6);
  scan_param.add_column(7);
  scan_param.add_column(10);
  */
  //scan_param.set_scan_size(2000);
}
    


TEST(ObTestObSSTableBlockScanner, query_one_valid_column)
{

  CharArena allocator;
  ObSSTableBlockBuilder builder;
  ObRange scan_param;
  ObSSTableSchema schema;
  ObScanColumnIndexes indexes;

  const int32_t row_count = 6;
  create_block(allocator, builder, row_count);
  create_scan_param(scan_param, g_key[0], g_key[row_count-1]);
  create_schema(schema);
  const int32_t query_columns[] = {5};
  create_query_columns(indexes, schema, query_columns, 1);

  ObSSTableBlockScanner scanner(indexes);

  bool need_looking_forward = false;
  int format = OB_SSTABLE_STORE_DENSE;
  bool is_reverse_scan = false;
  ObSSTableBlockScanner::BlockData block_data(block_internal_buffer, block_internal_bufsiz,
      builder.block_buf(), builder.get_block_data_size(), format);
  int ret = scanner.set_scan_param(scan_param,  is_reverse_scan, block_data, need_looking_forward);
  EXPECT_EQ(0, ret);

  ObCellInfo *cell_info = 0;
  int count = 0;
  int query_row_count = 0;
  while (scanner.next_cell() == 0)
  {
    bool is_row_changed = false;
    scanner.get_cell(&cell_info, &is_row_changed);
    ++count;
    if (is_row_changed) 
    {
      ++query_row_count;
      int cmp = strncmp(g_key[query_row_count-1], 
          cell_info->row_key_.ptr(), cell_info->row_key_.length());
      EXPECT_EQ(0, cmp);
    }
    /*
    printf("new row:%d, cell.table_id:%ld, cell.rowkey:%.*s\n", 
        is_row_changed, cell_info->table_id_, 
        cell_info->row_key_.length(), cell_info->row_key_.ptr());
    printf("cell val type:%d\n", cell_info->value_.get_type());
    */
    EXPECT_EQ(ObIntType, cell_info->value_.get_type());
    int64_t val = 0;
    EXPECT_EQ(0, cell_info->value_.get_int(val));
    EXPECT_EQ(query_row_count-1, val);
  }

  EXPECT_EQ(row_count, count);

}

TEST(ObTestObSSTableBlockScanner, query_all_valid_column)
{

  CharArena allocator;
  ObSSTableBlockBuilder builder;
  ObRange scan_param;
  ObSSTableSchema schema;
  ObScanColumnIndexes indexes;

  const int32_t row_count = 6;
  create_block(allocator, builder, row_count);
  create_scan_param(scan_param, g_key[0], g_key[5]);
  create_schema(schema);
  const int32_t query_columns[] = {5, 6, 7};
  const int32_t column_count = 3;
  create_query_columns(indexes, schema, query_columns, column_count);

  ObSSTableBlockScanner scanner(indexes);

  bool need_looking_forward = false;
  int format = OB_SSTABLE_STORE_DENSE;
  bool is_reverse_scan = false;
  ObSSTableBlockScanner::BlockData block_data(block_internal_buffer, block_internal_bufsiz,
      builder.block_buf(), builder.get_block_data_size(), format);
  int ret = scanner.set_scan_param(scan_param,  is_reverse_scan, block_data, need_looking_forward);
  EXPECT_EQ(0, ret);

  ObCellInfo *cell_info = 0;
  int count = 0;
  int query_row_count = 0;
  while (scanner.next_cell() == 0)
  {
    bool is_row_changed = false;
    scanner.get_cell(&cell_info, &is_row_changed);
    ++count;
    if (is_row_changed) 
    {
      ++query_row_count;
      int cmp = strncmp(g_key[query_row_count-1], 
          cell_info->row_key_.ptr(), cell_info->row_key_.length());
      EXPECT_EQ(0, cmp);
    }
    if ((count-1) % column_count == 0)
    {
      EXPECT_EQ(ObIntType, cell_info->value_.get_type());
      int64_t val = 0;
      EXPECT_EQ(0, cell_info->value_.get_int(val));
      EXPECT_EQ(query_row_count-1, val);
    }
    else if ((count-1) % column_count == 1)
    {
      EXPECT_EQ(ObIntType, cell_info->value_.get_type());
      int64_t val = 0;
      EXPECT_EQ(0, cell_info->value_.get_int(val));
      EXPECT_EQ(query_row_count-1, val);
    }
    else if ((count-1) % column_count == 2)
    {
      EXPECT_EQ(ObVarcharType, cell_info->value_.get_type());
      ObString val;
      EXPECT_EQ(0, cell_info->value_.get_varchar(val));
      EXPECT_EQ(0, strncmp(val.ptr(), g_key[query_row_count-1], val.length()));
    }
  }

  EXPECT_EQ(column_count*row_count, count);

}

TEST(ObTestObSSTableBlockScanner, query_random_valid_column)
{

  CharArena allocator;
  ObSSTableBlockBuilder builder;
  ObRange scan_param;
  ObSSTableSchema schema;
  ObScanColumnIndexes indexes;

  const int32_t row_count = 6;
  create_block(allocator, builder, row_count);
  create_scan_param(scan_param, g_key[0], g_key[5]);
  create_schema(schema);
  const int32_t query_columns[] = {5, 7};
  const int32_t column_count = 2;
  create_query_columns(indexes, schema, query_columns, column_count);

  ObSSTableBlockScanner scanner(indexes);

  bool need_looking_forward = false;
  int format = OB_SSTABLE_STORE_DENSE;
  bool is_reverse_scan = false;
  ObSSTableBlockScanner::BlockData block_data(block_internal_buffer, block_internal_bufsiz,
      builder.block_buf(), builder.get_block_data_size(), format);
  int ret = scanner.set_scan_param(scan_param,  is_reverse_scan, block_data, need_looking_forward);
  EXPECT_EQ(0, ret);

  ObCellInfo *cell_info = 0;
  int count = 0;
  int query_row_count = 0;
  while (scanner.next_cell() == 0)
  {
    bool is_row_changed = false;
    scanner.get_cell(&cell_info, &is_row_changed);
    ++count;
    if (is_row_changed) 
    {
      ++query_row_count;
      int cmp = strncmp(g_key[query_row_count-1], 
          cell_info->row_key_.ptr(), cell_info->row_key_.length());
      EXPECT_EQ(0, cmp);
    }
    if ((count-1) % column_count == 0)
    {
      EXPECT_EQ(ObIntType, cell_info->value_.get_type());
      int64_t val = 0;
      EXPECT_EQ(0, cell_info->value_.get_int(val));
      EXPECT_EQ(query_row_count-1, val);
    }
    else if ((count-1) % column_count == 1)
    {
      EXPECT_EQ(ObVarcharType, cell_info->value_.get_type());
      ObString val;
      EXPECT_EQ(0, cell_info->value_.get_varchar(val));
      EXPECT_EQ(0, strncmp(val.ptr(), g_key[query_row_count-1], val.length()));
    }
  }

  EXPECT_EQ(column_count*row_count, count);

}

TEST(ObTestObSSTableBlockScanner, query_one_invalid_column)
{

  CharArena allocator;
  ObSSTableBlockBuilder builder;
  ObRange scan_param;
  ObSSTableSchema schema;
  ObScanColumnIndexes indexes;

  const int32_t row_count = 6;
  create_block(allocator, builder, row_count);
  create_scan_param(scan_param, g_key[0], g_key[5]);
  create_schema(schema);
  const int32_t query_columns[] = {15};
  const int32_t column_count = 1;
  create_query_columns(indexes, schema, query_columns, column_count);

  ObSSTableBlockScanner scanner(indexes);

  bool need_looking_forward = false;
  int format = OB_SSTABLE_STORE_DENSE;
  bool is_reverse_scan = false;
  ObSSTableBlockScanner::BlockData block_data(block_internal_buffer, block_internal_bufsiz,
      builder.block_buf(), builder.get_block_data_size(), format);
  int ret = scanner.set_scan_param(scan_param,  is_reverse_scan, block_data, need_looking_forward);
  EXPECT_EQ(0, ret);

  ObCellInfo *cell_info = 0;
  int count = 0;
  int query_row_count = 0;
  while (scanner.next_cell() == 0)
  {
    bool is_row_changed = false;
    scanner.get_cell(&cell_info, &is_row_changed);
    ++count;
    if (is_row_changed) 
    {
      ++query_row_count;
      int cmp = strncmp(g_key[query_row_count-1], 
          cell_info->row_key_.ptr(), cell_info->row_key_.length());
      EXPECT_EQ(0, cmp);
    }
    if ((count-1) % column_count == 0)
    {
      EXPECT_EQ(ObNullType, cell_info->value_.get_type());
    }
  }

  EXPECT_EQ(column_count*row_count, count);

}

TEST(ObTestObSSTableBlockScanner, query_some_invalid_column)
{

  CharArena allocator;
  ObSSTableBlockBuilder builder;
  ObRange scan_param;
  ObSSTableSchema schema;
  ObScanColumnIndexes indexes;

  const int32_t row_count = 6;
  create_block(allocator, builder, row_count);
  create_scan_param(scan_param, g_key[0], g_key[5]);
  create_schema(schema);
  const int32_t query_columns[] = {15, 24, 22};
  const int32_t column_count = 3;
  create_query_columns(indexes, schema, query_columns, column_count);

  ObSSTableBlockScanner scanner(indexes);

  bool need_looking_forward = false;
  int format = OB_SSTABLE_STORE_DENSE;
  bool is_reverse_scan = false;
  ObSSTableBlockScanner::BlockData block_data(block_internal_buffer, block_internal_bufsiz,
      builder.block_buf(), builder.get_block_data_size(), format);
  int ret = scanner.set_scan_param(scan_param,  is_reverse_scan, block_data, need_looking_forward);
  EXPECT_EQ(0, ret);

  ObCellInfo *cell_info = 0;
  int count = 0;
  int query_row_count = 0;
  while (scanner.next_cell() == 0)
  {
    bool is_row_changed = false;
    scanner.get_cell(&cell_info, &is_row_changed);
    ++count;
    if (is_row_changed) 
    {
      ++query_row_count;
      int cmp = strncmp(g_key[query_row_count-1], 
          cell_info->row_key_.ptr(), cell_info->row_key_.length());
      EXPECT_EQ(0, cmp);
    }
    if ((count-1) % column_count == 0)
    {
      EXPECT_EQ(ObNullType, cell_info->value_.get_type());
    }
    else if ((count-1) % column_count == 1)
    {
      EXPECT_EQ(ObNullType, cell_info->value_.get_type());
    }
    else if ((count-1) % column_count == 2)
    {
      EXPECT_EQ(ObNullType, cell_info->value_.get_type());
    }
  }

  EXPECT_EQ(column_count*row_count, count);

}

TEST(ObTestObSSTableBlockScanner, query_mix_columns)
{

  CharArena allocator;
  ObSSTableBlockBuilder builder;
  ObRange scan_param;
  ObSSTableSchema schema;
  ObScanColumnIndexes indexes;

  const int32_t row_count = 6;
  create_block(allocator, builder, row_count);
  create_scan_param(scan_param, g_key[0], g_key[5]);
  create_schema(schema);
  const int32_t query_columns[] = {5, 17, 6, 7};
  const int32_t column_count = 4;
  create_query_columns(indexes, schema, query_columns, column_count);

  ObSSTableBlockScanner scanner(indexes);

  bool need_looking_forward = false;
  int format = OB_SSTABLE_STORE_DENSE;
  bool is_reverse_scan = false;
  ObSSTableBlockScanner::BlockData block_data(block_internal_buffer, block_internal_bufsiz,
      builder.block_buf(), builder.get_block_data_size(), format);
  int ret = scanner.set_scan_param(scan_param,  is_reverse_scan, block_data, need_looking_forward);
  EXPECT_EQ(0, ret);

  ObCellInfo *cell_info = 0;
  int count = 0;
  int query_row_count = 0;
  while (scanner.next_cell() == 0)
  {
    bool is_row_changed = false;
    scanner.get_cell(&cell_info, &is_row_changed);
    ++count;
    if (is_row_changed) 
    {
      ++query_row_count;
      int cmp = strncmp(g_key[query_row_count-1], 
          cell_info->row_key_.ptr(), cell_info->row_key_.length());
      EXPECT_EQ(0, cmp);
    }
    if ((count-1) % column_count == 0)
    {
      EXPECT_EQ(ObIntType, cell_info->value_.get_type());
      int64_t val = 0;
      EXPECT_EQ(0, cell_info->value_.get_int(val));
      EXPECT_EQ(query_row_count-1, val);
    }
    else if ((count-1) % column_count == 1)
    {
      EXPECT_EQ(ObNullType, cell_info->value_.get_type());
    }
    else if ((count-1) % column_count == 2)
    {
      EXPECT_EQ(ObIntType, cell_info->value_.get_type());
      int64_t val = 0;
      EXPECT_EQ(0, cell_info->value_.get_int(val));
      EXPECT_EQ(query_row_count-1, val);
    }
    else if ((count-1) % column_count == 3)
    {
      EXPECT_EQ(ObVarcharType, cell_info->value_.get_type());
      ObString val;
      EXPECT_EQ(0, cell_info->value_.get_varchar(val));
      EXPECT_EQ(0, strncmp(val.ptr(), g_key[query_row_count-1], val.length()));
    }
  }

  EXPECT_EQ(column_count*row_count, count);

}


TEST(ObTestObSSTableBlockScanner, query_one_single_row_block)
{

  CharArena allocator;
  ObSSTableBlockBuilder builder;
  ObRange scan_param;
  ObSSTableSchema schema;
  ObScanColumnIndexes indexes;

  const int32_t row_count = 1;
  create_block(allocator, builder, row_count);
  create_scan_param(scan_param, g_key[0], g_key[0]);
  create_schema(schema);
  const int32_t query_columns[] = {5};
  const int32_t column_count = 1;
  create_query_columns(indexes, schema, query_columns, column_count);

  ObSSTableBlockScanner scanner(indexes);

  bool need_looking_forward = false;
  int format = OB_SSTABLE_STORE_DENSE;
  bool is_reverse_scan = false;
  ObSSTableBlockScanner::BlockData block_data(block_internal_buffer, block_internal_bufsiz,
      builder.block_buf(), builder.get_block_data_size(), format);
  int ret = scanner.set_scan_param(scan_param,  is_reverse_scan, block_data, need_looking_forward);
  EXPECT_EQ(0, ret);

  ObCellInfo *cell_info = 0;
  int all_query_count = 0;
  int query_row_count = 0;
  while (scanner.next_cell() == 0)
  {
    bool is_row_changed = false;
    scanner.get_cell(&cell_info, &is_row_changed);
    ++all_query_count;
    if (is_row_changed) 
    {
      ++query_row_count;
      int cmp = strncmp(g_key[query_row_count-1], 
          cell_info->row_key_.ptr(), cell_info->row_key_.length());
      EXPECT_EQ(0, cmp);
    }
    if ((all_query_count-1) % column_count == 0)
    {
      EXPECT_EQ(ObIntType, cell_info->value_.get_type());
      int64_t val = 0;
      EXPECT_EQ(0, cell_info->value_.get_int(val));
      EXPECT_EQ(all_query_count-1, val);
    }
  }

  EXPECT_EQ(column_count*row_count, all_query_count);

}

TEST(ObTestObSSTableBlockScanner, query_not_in_blocks)
{

  CharArena allocator;
  ObSSTableBlockBuilder builder;
  ObRange scan_param;
  ObSSTableSchema schema;
  ObScanColumnIndexes indexes;

  const int32_t row_count = 1;
  create_block(allocator, builder, row_count);
  create_scan_param(scan_param, "ao", "ao");
  create_schema(schema);
  const int32_t query_columns[] = {5};
  const int32_t column_count = 1;
  create_query_columns(indexes, schema, query_columns, column_count);

  ObSSTableBlockScanner scanner(indexes);

  bool need_looking_forward = false;
  int format = OB_SSTABLE_STORE_DENSE;
  bool is_reverse_scan = false;
  ObSSTableBlockScanner::BlockData block_data(block_internal_buffer, block_internal_bufsiz,
      builder.block_buf(), builder.get_block_data_size(), format);
  int ret = scanner.set_scan_param(scan_param,  is_reverse_scan, block_data, need_looking_forward);
  EXPECT_EQ(OB_BEYOND_THE_RANGE, ret);
  EXPECT_FALSE(need_looking_forward);

}

TEST(ObTestObSSTableBlockScanner, query_not_inclsive_start)
{

  CharArena allocator;
  ObSSTableBlockBuilder builder;
  ObRange scan_param;
  ObSSTableSchema schema;
  ObScanColumnIndexes indexes;

  const int32_t row_count = 6;
  create_block(allocator, builder, row_count);
  create_scan_param(scan_param, g_key[0], g_key[5], false, true);
  create_schema(schema);
  const int32_t query_columns[] = {5, 6, 7};
  const int32_t column_count = 3;
  create_query_columns(indexes, schema, query_columns, column_count);

  ObSSTableBlockScanner scanner(indexes);

  bool need_looking_forward = false;
  int format = OB_SSTABLE_STORE_DENSE;
  bool is_reverse_scan = false;
  ObSSTableBlockScanner::BlockData block_data(block_internal_buffer, block_internal_bufsiz,
      builder.block_buf(), builder.get_block_data_size(), format);
  int ret = scanner.set_scan_param(scan_param,  is_reverse_scan, block_data, need_looking_forward);
  EXPECT_EQ(0, ret);

  ObCellInfo *cell_info = 0;
  int count = 0;
  int query_row_count = 0;
  while (scanner.next_cell() == 0)
  {
    bool is_row_changed = false;
    scanner.get_cell(&cell_info, &is_row_changed);
    ++count;
    if (is_row_changed) 
    {
      ++query_row_count;
      int cmp = strncmp(g_key[query_row_count], 
          cell_info->row_key_.ptr(), cell_info->row_key_.length());
      EXPECT_EQ(0, cmp);
    }
    if ((count-1) % column_count == 0)
    {
      EXPECT_EQ(ObIntType, cell_info->value_.get_type());
      int64_t val = 0;
      EXPECT_EQ(0, cell_info->value_.get_int(val));
      // start from row 2.
      // query_row_count - 1 + 1 == query_row_count
      EXPECT_EQ(query_row_count, val);
    }
    else if ((count-1) % column_count == 1)
    {
      EXPECT_EQ(ObIntType, cell_info->value_.get_type());
      int64_t val = 0;
      EXPECT_EQ(0, cell_info->value_.get_int(val));
      EXPECT_EQ(query_row_count, val);
    }
    else if ((count-1) % column_count == 2)
    {
      EXPECT_EQ(ObVarcharType, cell_info->value_.get_type());
      ObString val;
      EXPECT_EQ(0, cell_info->value_.get_varchar(val));
      EXPECT_EQ(0, strncmp(val.ptr(), g_key[query_row_count], val.length()));
    }
  }

  EXPECT_EQ(column_count*(row_count-1), count);

}

TEST(ObTestObSSTableBlockScanner, query_not_inclsive_end)
{

  CharArena allocator;
  ObSSTableBlockBuilder builder;
  ObRange scan_param;
  ObSSTableSchema schema;
  ObScanColumnIndexes indexes;

  const int32_t row_count = 6;
  create_block(allocator, builder, row_count);
  create_scan_param(scan_param, g_key[0], g_key[5], true, false);
  create_schema(schema);
  const int32_t query_columns[] = {5, 6, 7};
  const int32_t column_count = 3;
  create_query_columns(indexes, schema, query_columns, column_count);

  ObSSTableBlockScanner scanner(indexes);

  bool need_looking_forward = false;
  int format = OB_SSTABLE_STORE_DENSE;
  bool is_reverse_scan = false;
  ObSSTableBlockScanner::BlockData block_data(block_internal_buffer, block_internal_bufsiz,
      builder.block_buf(), builder.get_block_data_size(), format);
  int ret = scanner.set_scan_param(scan_param,  is_reverse_scan, block_data, need_looking_forward);
  EXPECT_EQ(0, ret);

  ObCellInfo *cell_info = 0;
  int count = 0;
  int query_row_count = 0;
  while (scanner.next_cell() == 0)
  {
    bool is_row_changed = false;
    scanner.get_cell(&cell_info, &is_row_changed);
    ++count;
    if (is_row_changed) 
    {
      ++query_row_count;
      int cmp = strncmp(g_key[query_row_count-1], 
          cell_info->row_key_.ptr(), cell_info->row_key_.length());
      EXPECT_EQ(0, cmp);
    }
    if ((count-1) % column_count == 0)
    {
      EXPECT_EQ(ObIntType, cell_info->value_.get_type());
      int64_t val = 0;
      EXPECT_EQ(0, cell_info->value_.get_int(val));
      EXPECT_EQ(query_row_count -1, val);
    }
    else if ((count-1) % column_count == 1)
    {
      EXPECT_EQ(ObIntType, cell_info->value_.get_type());
      int64_t val = 0;
      EXPECT_EQ(0, cell_info->value_.get_int(val));
      EXPECT_EQ(query_row_count-1, val);
    }
    else if ((count-1) % column_count == 2)
    {
      EXPECT_EQ(ObVarcharType, cell_info->value_.get_type());
      ObString val;
      EXPECT_EQ(0, cell_info->value_.get_varchar(val));
      EXPECT_EQ(0, strncmp(val.ptr(), g_key[query_row_count-1], val.length()));
    }
  }

  EXPECT_EQ(column_count*(row_count-1), count);

}

TEST(ObTestObSSTableBlockScanner, query_not_inclsive_start_end)
{

  CharArena allocator;
  ObSSTableBlockBuilder builder;
  ObRange scan_param;
  ObSSTableSchema schema;
  ObScanColumnIndexes indexes;

  const int32_t row_count = 6;
  create_block(allocator, builder, row_count);
  create_scan_param(scan_param, g_key[0], g_key[5], false, false);
  create_schema(schema);
  const int32_t query_columns[] = {5, 6, 7};
  const int32_t column_count = 3;
  create_query_columns(indexes, schema, query_columns, column_count);

  ObSSTableBlockScanner scanner(indexes);

  bool need_looking_forward = false;
  int format = OB_SSTABLE_STORE_DENSE;
  bool is_reverse_scan = false;
  ObSSTableBlockScanner::BlockData block_data(block_internal_buffer, block_internal_bufsiz,
      builder.block_buf(), builder.get_block_data_size(), format);
  int ret = scanner.set_scan_param(scan_param,  is_reverse_scan, block_data, need_looking_forward);
  EXPECT_EQ(0, ret);

  ObCellInfo *cell_info = 0;
  int count = 0;
  int query_row_count = 0;
  while (scanner.next_cell() == 0)
  {
    bool is_row_changed = false;
    scanner.get_cell(&cell_info, &is_row_changed);
    ++count;
    if (is_row_changed) 
    {
      ++query_row_count;
      int cmp = strncmp(g_key[query_row_count], 
          cell_info->row_key_.ptr(), cell_info->row_key_.length());
      EXPECT_EQ(0, cmp);
    }
    if ((count-1) % column_count == 0)
    {
      EXPECT_EQ(ObIntType, cell_info->value_.get_type());
      int64_t val = 0;
      EXPECT_EQ(0, cell_info->value_.get_int(val));
      // start from row 2.
      // query_row_count - 1 + 1 == query_row_count
      EXPECT_EQ(query_row_count, val);
    }
    else if ((count-1) % column_count == 1)
    {
      EXPECT_EQ(ObIntType, cell_info->value_.get_type());
      int64_t val = 0;
      EXPECT_EQ(0, cell_info->value_.get_int(val));
      EXPECT_EQ(query_row_count, val);
    }
    else if ((count-1) % column_count == 2)
    {
      EXPECT_EQ(ObVarcharType, cell_info->value_.get_type());
      ObString val;
      EXPECT_EQ(0, cell_info->value_.get_varchar(val));
      EXPECT_EQ(0, strncmp(val.ptr(), g_key[query_row_count], val.length()));
    }
  }

  EXPECT_EQ(column_count*(row_count-2), count);

}

TEST(ObTestObSSTableBlockScanner, query_header_part)
{

  CharArena allocator;
  ObSSTableBlockBuilder builder;
  ObRange scan_param;
  ObSSTableSchema schema;
  ObScanColumnIndexes indexes;

  const int32_t row_count = 6;
  create_block(allocator, builder, row_count);
  create_scan_param(scan_param, "ac", g_key[2], true, true);
  create_schema(schema);
  const int32_t query_columns[] = {5, 6, 7};
  const int32_t column_count = 3;
  create_query_columns(indexes, schema, query_columns, column_count);

  ObSSTableBlockScanner scanner(indexes);

  bool need_looking_forward = false;
  int format = OB_SSTABLE_STORE_DENSE;
  bool is_reverse_scan = false;
  ObSSTableBlockScanner::BlockData block_data(block_internal_buffer, block_internal_bufsiz,
      builder.block_buf(), builder.get_block_data_size(), format);
  int ret = scanner.set_scan_param(scan_param,  is_reverse_scan, block_data, need_looking_forward);
  EXPECT_EQ(0, ret);

  ObCellInfo *cell_info = 0;
  int count = 0;
  int query_row_count = 0;
  while (scanner.next_cell() == 0)
  {
    bool is_row_changed = false;
    scanner.get_cell(&cell_info, &is_row_changed);
    ++count;
    if (is_row_changed) 
    {
      ++query_row_count;
      int cmp = strncmp(g_key[query_row_count-1], 
          cell_info->row_key_.ptr(), cell_info->row_key_.length());
      EXPECT_EQ(0, cmp);
    }
    if ((count-1) % column_count == 0)
    {
      EXPECT_EQ(ObIntType, cell_info->value_.get_type());
      int64_t val = 0;
      EXPECT_EQ(0, cell_info->value_.get_int(val));
      EXPECT_EQ(query_row_count -1, val);
    }
    else if ((count-1) % column_count == 1)
    {
      EXPECT_EQ(ObIntType, cell_info->value_.get_type());
      int64_t val = 0;
      EXPECT_EQ(0, cell_info->value_.get_int(val));
      EXPECT_EQ(query_row_count-1, val);
    }
    else if ((count-1) % column_count == 2)
    {
      EXPECT_EQ(ObVarcharType, cell_info->value_.get_type());
      ObString val;
      EXPECT_EQ(0, cell_info->value_.get_varchar(val));
      EXPECT_EQ(0, strncmp(val.ptr(), g_key[query_row_count-1], val.length()));
    }
  }

  EXPECT_EQ(column_count*3, count);

}

TEST(ObTestObSSTableBlockScanner, query_tailer_part)
{

  CharArena allocator;
  ObSSTableBlockBuilder builder;
  ObRange scan_param;
  ObSSTableSchema schema;
  ObScanColumnIndexes indexes;

  const int32_t row_count = 6;
  create_block(allocator, builder, row_count);
  create_scan_param(scan_param, g_key[3], "zoo", true, true);
  create_schema(schema);
  const int32_t query_columns[] = {5, 6, 7};
  const int32_t column_count = 3;
  create_query_columns(indexes, schema, query_columns, column_count);

  ObSSTableBlockScanner scanner(indexes);

  bool need_looking_forward = false;
  int format = OB_SSTABLE_STORE_DENSE;
  bool is_reverse_scan = false;
  ObSSTableBlockScanner::BlockData block_data(block_internal_buffer, block_internal_bufsiz,
      builder.block_buf(), builder.get_block_data_size(), format);
  int ret = scanner.set_scan_param(scan_param,  is_reverse_scan, block_data, need_looking_forward);
  EXPECT_EQ(0, ret);

  ObCellInfo *cell_info = 0;
  int count = 0;
  int query_row_count = 0;
  while (scanner.next_cell() == 0)
  {
    bool is_row_changed = false;
    scanner.get_cell(&cell_info, &is_row_changed);
    ++count;
    if (is_row_changed) 
    {
      ++query_row_count;
      int cmp = strncmp(g_key[query_row_count+2], 
          cell_info->row_key_.ptr(), cell_info->row_key_.length());
      EXPECT_EQ(0, cmp);
    }
    if ((count-1) % column_count == 0)
    {
      EXPECT_EQ(ObIntType, cell_info->value_.get_type());
      int64_t val = 0;
      EXPECT_EQ(0, cell_info->value_.get_int(val));
      EXPECT_EQ(query_row_count +2, val);
    }
    else if ((count-1) % column_count == 1)
    {
      EXPECT_EQ(ObIntType, cell_info->value_.get_type());
      int64_t val = 0;
      EXPECT_EQ(0, cell_info->value_.get_int(val));
      EXPECT_EQ(query_row_count+2, val);
    }
    else if ((count-1) % column_count == 2)
    {
      EXPECT_EQ(ObVarcharType, cell_info->value_.get_type());
      ObString val;
      EXPECT_EQ(0, cell_info->value_.get_varchar(val));
      EXPECT_EQ(0, strncmp(val.ptr(), g_key[query_row_count+2], val.length()));
    }
  }

  EXPECT_EQ(column_count*3, count);

}

TEST(ObTestObSSTableBlockScanner, query_with_part_key)
{

  CharArena allocator;
  ObSSTableBlockBuilder builder;
  ObRange scan_param;
  ObSSTableSchema schema;
  ObScanColumnIndexes indexes;

  const int32_t row_count = 4;
  create_block(allocator, builder, row_count);
  create_scan_param(scan_param, "ao", "ko", true, true);
  create_schema(schema);
  const int32_t query_columns[] = {5};
  create_query_columns(indexes, schema, query_columns, 1);

  ObSSTableBlockScanner scanner(indexes);

  bool need_looking_forward = false;
  int format = OB_SSTABLE_STORE_DENSE;
  bool is_reverse_scan = false;
  ObSSTableBlockScanner::BlockData block_data(block_internal_buffer, block_internal_bufsiz,
      builder.block_buf(), builder.get_block_data_size(), format);
  int ret = scanner.set_scan_param(scan_param,  is_reverse_scan, block_data, need_looking_forward);
  EXPECT_EQ(0, ret);

  ObCellInfo *cell_info = 0;
  int count = 0;
  int query_row_count = 0;
  while (scanner.next_cell() == 0)
  {
    bool is_row_changed = false;
    scanner.get_cell(&cell_info, &is_row_changed);
    ++count;
    if (is_row_changed) 
    {
      ++query_row_count;
      int cmp = strncmp(g_key[query_row_count-1], 
          cell_info->row_key_.ptr(), cell_info->row_key_.length());
      EXPECT_EQ(0, cmp);
    }
    /*
    printf("new row:%d, cell.table_id:%ld, cell.rowkey:%.*s\n", 
        is_row_changed, cell_info->table_id_, 
        cell_info->row_key_.length(), cell_info->row_key_.ptr());
    printf("cell val type:%d\n", cell_info->value_.get_type());
    */
    EXPECT_EQ(ObIntType, cell_info->value_.get_type());
    int64_t val = 0;
    EXPECT_EQ(0, cell_info->value_.get_int(val));
    EXPECT_EQ(query_row_count-1, val);
  }

  EXPECT_EQ(row_count, count);

}

TEST(ObTestObSSTableBlockScanner, query_with_part_key_reverse_scan)
{
  CharArena allocator;
  ObSSTableBlockBuilder builder;
  ObRange scan_param;
  ObSSTableSchema schema;
  ObScanColumnIndexes indexes;

  const int32_t row_count = 4;
  create_block(allocator, builder, row_count);
  create_scan_param(scan_param, "ao", "ko", true, true);
  create_schema(schema);
  const int32_t query_columns[] = {5};
  create_query_columns(indexes, schema, query_columns, 1);

  ObSSTableBlockScanner scanner(indexes);

  bool need_looking_forward = false;
  int format = OB_SSTABLE_STORE_DENSE;
  bool is_reverse_scan = true;
  ObSSTableBlockScanner::BlockData block_data(block_internal_buffer, block_internal_bufsiz,
      builder.block_buf(), builder.get_block_data_size(), format);
  int ret = scanner.set_scan_param(scan_param,  is_reverse_scan, block_data, need_looking_forward);
  EXPECT_EQ(0, ret);

  ObCellInfo *cell_info = 0;
  int count = 0;
  int query_row_count = 0;
  while (scanner.next_cell() == 0)
  {
    bool is_row_changed = false;
    scanner.get_cell(&cell_info, &is_row_changed);
    ++count;
    if (is_row_changed) 
    {
      ++query_row_count;
      int cmp = strncmp(g_key[4-query_row_count], 
          cell_info->row_key_.ptr(), cell_info->row_key_.length());
      EXPECT_EQ(0, cmp);
    }
    /*
    printf("query_row_count:%d,new row:%d, cell.table_id:%ld, cell.rowkey:%.*s\n", 
        query_row_count, is_row_changed, cell_info->table_id_, 
        cell_info->row_key_.length(), cell_info->row_key_.ptr());
    printf("cell val type:%d\n", cell_info->value_.get_type());
    */
    EXPECT_EQ(ObIntType, cell_info->value_.get_type());
    int64_t val = 0;
    EXPECT_EQ(0, cell_info->value_.get_int(val));
    EXPECT_EQ(4-query_row_count, val);
  }

  EXPECT_EQ(row_count, count);

}

TEST(ObTestObSSTableBlockScanner, query_one_valid_column_with_min_value)
{

  CharArena allocator;
  ObSSTableBlockBuilder builder;
  ObRange scan_param;
  ObSSTableSchema schema;
  ObScanColumnIndexes indexes;

  const int32_t row_count = 6;
  create_block(allocator, builder, row_count);
  create_scan_param(scan_param, "null", g_key[row_count-1], false, true, true, false);
  create_schema(schema);
  const int32_t query_columns[] = {5};
  create_query_columns(indexes, schema, query_columns, 1);

  ObSSTableBlockScanner scanner(indexes);

  bool need_looking_forward = false;
  int format = OB_SSTABLE_STORE_DENSE;
  bool is_reverse_scan = false;
  ObSSTableBlockScanner::BlockData block_data(block_internal_buffer, block_internal_bufsiz,
      builder.block_buf(), builder.get_block_data_size(), format);
  int ret = scanner.set_scan_param(scan_param,  is_reverse_scan, block_data, need_looking_forward);
  EXPECT_EQ(0, ret);

  ObCellInfo *cell_info = 0;
  int count = 0;
  int query_row_count = 0;
  while (scanner.next_cell() == 0)
  {
    bool is_row_changed = false;
    scanner.get_cell(&cell_info, &is_row_changed);
    ++count;
    if (is_row_changed) 
    {
      ++query_row_count;
      int cmp = strncmp(g_key[query_row_count-1], 
          cell_info->row_key_.ptr(), cell_info->row_key_.length());
      EXPECT_EQ(0, cmp);
    }
    EXPECT_EQ(ObIntType, cell_info->value_.get_type());
    int64_t val = 0;
    EXPECT_EQ(0, cell_info->value_.get_int(val));
    EXPECT_EQ(query_row_count-1, val);
  }

  EXPECT_EQ(row_count, count);

}

TEST(ObTestObSSTableBlockScanner, query_one_valid_column_with_max_value)
{

  CharArena allocator;
  ObSSTableBlockBuilder builder;
  ObRange scan_param;
  ObSSTableSchema schema;
  ObScanColumnIndexes indexes;

  const int32_t row_count = 6;
  create_block(allocator, builder, row_count);
  // not include start key "aoo"
  create_scan_param(scan_param, g_key[0], "null",  false, true, false, true);
  create_schema(schema);
  const int32_t query_columns[] = {5};
  create_query_columns(indexes, schema, query_columns, 1);

  ObSSTableBlockScanner scanner(indexes);

  bool need_looking_forward = false;
  int format = OB_SSTABLE_STORE_DENSE;
  bool is_reverse_scan = false;
  ObSSTableBlockScanner::BlockData block_data(block_internal_buffer, block_internal_bufsiz,
      builder.block_buf(), builder.get_block_data_size(), format);
  int ret = scanner.set_scan_param(scan_param,  is_reverse_scan, block_data, need_looking_forward);
  EXPECT_EQ(0, ret);

  ObCellInfo *cell_info = 0;
  int count = 0;
  int query_row_count = 0;
  while (scanner.next_cell() == 0)
  {
    bool is_row_changed = false;
    scanner.get_cell(&cell_info, &is_row_changed);
    ++count;
    if (is_row_changed) 
    {
      ++query_row_count;
      int cmp = strncmp(g_key[query_row_count], 
          cell_info->row_key_.ptr(), cell_info->row_key_.length());
      EXPECT_EQ(0, cmp);
    }
    EXPECT_EQ(ObIntType, cell_info->value_.get_type());
    int64_t val = 0;
    EXPECT_EQ(0, cell_info->value_.get_int(val));
    EXPECT_EQ(query_row_count, val);
  }

  EXPECT_EQ(row_count-1, count);

}

TEST(ObTestObSSTableBlockScanner, query_one_valid_column_with_min_max_value)
{

  CharArena allocator;
  ObSSTableBlockBuilder builder;
  ObRange scan_param;
  ObSSTableSchema schema;
  ObScanColumnIndexes indexes;

  const int32_t row_count = 6;
  create_block(allocator, builder, row_count);
  // not include start key "aoo"
  create_scan_param(scan_param, "null", "null",  false, false, true, true);
  create_schema(schema);
  const int32_t query_columns[] = {5};
  create_query_columns(indexes, schema, query_columns, 1);

  ObSSTableBlockScanner scanner(indexes);

  bool need_looking_forward = false;
  int format = OB_SSTABLE_STORE_DENSE;
  bool is_reverse_scan = false;
  ObSSTableBlockScanner::BlockData block_data(block_internal_buffer, block_internal_bufsiz,
      builder.block_buf(), builder.get_block_data_size(), format);
  int ret = scanner.set_scan_param(scan_param,  is_reverse_scan, block_data, need_looking_forward);
  EXPECT_EQ(0, ret);

  ObCellInfo *cell_info = 0;
  int count = 0;
  int query_row_count = 0;
  while (scanner.next_cell() == 0)
  {
    bool is_row_changed = false;
    scanner.get_cell(&cell_info, &is_row_changed);
    ++count;
    if (is_row_changed) 
    {
      ++query_row_count;
      int cmp = strncmp(g_key[query_row_count-1], 
          cell_info->row_key_.ptr(), cell_info->row_key_.length());
      EXPECT_EQ(0, cmp);
    }
    EXPECT_EQ(ObIntType, cell_info->value_.get_type());
    int64_t val = 0;
    EXPECT_EQ(0, cell_info->value_.get_int(val));
    EXPECT_EQ(query_row_count-1, val);
  }

  EXPECT_EQ(row_count, count);

}

int main(int argc, char **argv)
{
  ob_init_memory_pool();
  TBSYS_LOGGER.setLogLevel("WARN");
  testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}

