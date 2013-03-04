/**
 *  (C) 2010-2011 Taobao Inc.
 *  
 *  This program is free software; you can redistribute it
 *  and/or modify it under the terms of the GNU General Public
 *  License version 2 as published by the Free Software
 *  Foundation.
 *
 *  test_sstable_reader.cpp is for what ...
 *
 *  Authors:
 *     qushan<qushan@taobao.com>
 *        
 */
#include <gtest/gtest.h>
#include "common/file_directory_utils.h"
#include "common/ob_malloc.h"
#include "common/ob_object.h"
#include "common/ob_define.h"
#include "common/page_arena.h"
#include "sstable/ob_sstable_reader.h"
#include "sstable/ob_sstable_writer.h"
#include "sstable/ob_sstable_row.h"
#include "chunkserver/ob_fileinfo_cache.h"
#include "sstable/ob_disk_path.h"

using namespace oceanbase::common;
using namespace oceanbase::chunkserver;
using namespace oceanbase::sstable;

static const char* g_key[] = { "aoo", "boo", "coo", "doo", "foo", "koo" };
const int64_t TABLE_ID = 1;

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

  row.set_column_group_id(0);
  row.set_table_id(TABLE_ID);
  row.add_obj(obj1);
  row.add_obj(obj2);
  row.add_obj(obj3);
}

void create_schema(ObSSTableSchema& schema)
{
  ObSSTableSchemaColumnDef def1, def2, def3;
  def1.column_name_id_ = 5;
  def1.column_value_type_ = ObIntType;
  def1.column_group_id_ = 0;
  def1.table_id_ = TABLE_ID;

  def2.column_name_id_ = 6;
  def2.column_value_type_ = ObIntType;
  def2.column_group_id_ = 0;
  def2.table_id_ = TABLE_ID;

  def3.column_name_id_ = 7;
  def3.column_value_type_ = ObVarcharType;
  def3.column_group_id_ = 0;
  def3.table_id_ = TABLE_ID;

  schema.add_column_def(def1);
  schema.add_column_def(def2);
  schema.add_column_def(def3);
}

int write_sstable_file(const ObString& path, const ObString& compress, const int row_count)
{
  CharArena allocator;
  ObSSTableWriter writer;
  ObSSTableSchema schema;
  create_schema(schema);
  remove(path.ptr());
  int ret = writer.create_sstable(schema, path, compress, 0);
  if (ret) return ret;
  for (int i = 0; i < row_count; ++i)
  {
    ObSSTableRow row;
    create_row(allocator, row, g_key[i], i, i, (char*)g_key[i]);
    int64_t approx_usage = 0;
    ret = writer.append_row(row, approx_usage);
    if (ret) return ret;
  }

  int64_t trailer_offset = 0;
  ret = writer.close_sstable(trailer_offset);
  return ret;
}


TEST(ObTestObSSTableReader, read)
{
  ObSSTableId sstable_id;
  sstable_id.sstable_file_id_ = 1234;
  sstable_id.sstable_file_offset_ = 0;
   
  char path[OB_MAX_FILE_NAME_LENGTH];
  char* compress = (char*)"lzo_1.0";

  int ret = 0;
  char sstable_file_dir[OB_MAX_FILE_NAME_LENGTH];
  ret = get_sstable_directory((sstable_id.sstable_file_id_ & 0xFF), 
                              sstable_file_dir, OB_MAX_FILE_NAME_LENGTH);
  EXPECT_EQ(0, ret);
  bool ok = FileDirectoryUtils::exists(sstable_file_dir);
  if (!ok)
  {
    ok = FileDirectoryUtils::create_full_path(sstable_file_dir);
    if (!ok)
      TBSYS_LOG(ERROR, "create sstable path:%s failed", sstable_file_dir); 
  }

  get_sstable_path(sstable_id, path, OB_MAX_FILE_NAME_LENGTH);
  ObString ob_path(0, static_cast<int32_t>(strlen(path)), path);
  ObString ob_compress(0, static_cast<int32_t>(strlen(compress)), compress);

  ret = write_sstable_file(ob_path, ob_compress, 6);
  EXPECT_EQ(0, ret);

  FileInfoCache fileinfo_cache;
  fileinfo_cache.init(10);

  ModulePageAllocator mod(0);
  ModuleArena allocator(ModuleArena::DEFAULT_PAGE_SIZE, mod);
  ObSSTableReader reader(allocator, fileinfo_cache);

  ret = reader.open(sstable_id);
  EXPECT_EQ(0, ret);
  unlink(path);
  allocator.free();
  fileinfo_cache.destroy();
}

int main(int argc, char** argv)
{
  ob_init_memory_pool();
  TBSYS_LOGGER.setLogLevel("WARN");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

