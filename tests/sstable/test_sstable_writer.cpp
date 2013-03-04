/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * test_sstable_writer.cpp for test sstable writer
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */

#include <iostream>
#include <sstream>
#include <tblog.h>
#include <gtest/gtest.h>
#include "common/file_utils.h"
#include "sstable/ob_sstable_writer.h"
#include "key.h"
#include "file_directory_utils.h"
#include "sstable/ob_disk_path.h"
#include "sstable/ob_sstable_row.h"

using namespace std;
using namespace oceanbase::common;
using namespace oceanbase::sstable;

namespace oceanbase
{
  namespace tests
  {
    namespace sstable
    {
      #define COMPRESSOR_NAME "lzo_1.0"

      static char sstable_path[OB_MAX_FILE_NAME_LENGTH];
      static const int64_t table_id = 128;
      static const int64_t column_scheme_def_size = 100;
      static const int64_t sstable_id = 1026;
      static const int32_t objs_per_row = 3;
      static const int64_t TABLE_COUNT = 4;
      static const int64_t ROWS_PER_TABLE = 5000;
      static const uint64_t TABLE_ID_BASE = 1025;

      class TestObSSTableWriter: public ::testing::Test
      {
      public:
        virtual void SetUp()
        {
          ObSSTableId sst_id(sstable_id);
          get_sstable_path(sst_id, sstable_path, OB_MAX_FILE_NAME_LENGTH);
        }

        virtual void TearDown()
        {

        }

        void build_row(int64_t index, ObSSTableRow &row, int type, uint64_t table_id, uint64_t column_group_id)
        {
          ObObj tmp_obj;
          Key tmp_key;
          ObString key;
          row.set_table_id(table_id);
          row.set_column_group_id(column_group_id);

          tmp_key.assign(index, 10, 1000);
          key.assign(tmp_key.get_ptr(), tmp_key.key_len());
          row.set_row_key(key);

          for (int i = 0; i < OB_MAX_COLUMN_NUMBER; ++i)
          {
            switch (type)
            {
            case ObNullType:
              tmp_obj.set_null();
              break;
            case ObIntType:
              tmp_obj.set_int(i);
              break;
            default:
              break;
            }
            row.add_obj(tmp_obj);
          }
        }

        void build_row(int64_t index, ObSSTableRow &row, int type, int column_count, 
                       uint64_t table_id, uint64_t column_group_id)
        {
          ObObj tmp_obj;
          ObObj id_obj;
          Key tmp_key;
          ObString key;

          row.set_table_id(table_id);
          row.set_column_group_id(column_group_id);
          tmp_key.assign(index, 10, 1000);
          key.assign(tmp_key.get_ptr(), tmp_key.key_len());
          row.set_row_key(key);

          for (int i = 0; i < column_count; ++i)
          {
            switch (type)
            {
            case ObNullType:
              tmp_obj.set_null();
              break;
            case ObIntType:
              tmp_obj.set_int(i);
              break;
            default:
              break;
            }
            row.add_obj(tmp_obj);
            //row.add_obj(tmp_obj, i + 2);
          }
        }
        
        void build_large_row(int64_t index, ObSSTableRow &row,
                             uint64_t table_id, uint64_t column_group_id, int64_t size_in_kb)
        {
          ObObj tmp_obj;
          int32_t value_size = static_cast<int32_t>(1024 * size_in_kb + 1);
          char value_data[value_size];
          char *ptr;

          //build data
          ptr = value_data;
          for (int i = 0; i < 128 * size_in_kb; ++i) {
            memcpy(ptr, "testing ", 8);
            ptr += 8;
          }
          ObString value_str(value_size, value_size, value_data);

          Key tmp_key(index, 10, 1000);
          ObString key(tmp_key.key_len(), tmp_key.key_len(), tmp_key.get_ptr());
          row.set_row_key(key);
          row.set_table_id(table_id);
          row.set_column_group_id(column_group_id);

          for (int i = 0; i < OB_MAX_COLUMN_NUMBER; ++i)
          {
            tmp_obj.set_varchar(value_str);
            row.add_obj(tmp_obj);
          }
        }

        void read_sstable_and_check(char *file_name, int64_t trailer_addr, bool compressed)
        {
          FileUtils file_util;
          int type;
          int types[3] = {ObIntType, ObIntType, ObVarcharType};
          char *file_buf = NULL;
          int64_t file_len = FileDirectoryUtils::get_size(file_name);
          int64_t read_len = 0;
          const char *compressor_name = COMPRESSOR_NAME;
          ObCompressor *compressor = NULL;
          int64_t uncompressed_size = 0;
          ObTrailerOffset trailer_offset;
          ObSSTableTrailer trailer;
          ObRecordHeader record_header;
          ObSSTableSchema schema;
          const ObSSTableSchemaColumnDef *column = NULL;
          int64_t schema_offset = 0;
          char *schema_buf = NULL;
          char *schema_data = NULL;
          int64_t schema_len = 0;
          int64_t block_count = 0;
          BloomFilter bloom_filter;
          int64_t filter_offset = 0;
          int64_t filter_len = 0;
          ObSSTableBlockIndexHeader index_header;
          ObSSTableBlockIndexItem index_item;
          int64_t index_offset = 0;
          int64_t index_len = 0;
          char *index_buf = NULL;
          char *index_block = NULL;
          int64_t index_block_len = 0;
          ObSSTableBlockBuilder block_builder;
          ObSSTableBlockHeader block_header;
          int64_t block_offset = 0;
          int64_t block_size = 0;
          char *block_buf = 0;
          char *block_data = NULL;
          char *block_index = NULL;
          char *row_data = NULL;
          int64_t row_data_len = 0;
          int64_t row_count = 0;
          ObSSTableRow new_row;
          ObString row_key;
          const ObObj *obj = NULL;
          Key tmp_key;
          ObString key;
          ObString string_value;
          int64_t index = 0;
          int64_t val64 = 0;
          int64_t pos = 0;
          // uint64_t cur_table_id = TABLE_ID_BASE;
          int ret;

          if (compressed)
          {
            compressor = create_compressor(compressor_name);
            EXPECT_TRUE(compressor != NULL);
          }

          file_buf = new char[file_len];
          EXPECT_TRUE(file_buf != NULL);

          ret = file_util.open(file_name, O_RDWR, 0644);
          EXPECT_TRUE(ret != -1);

          read_len = file_util.read(file_buf, file_len);
          EXPECT_EQ(read_len, file_len);

          //read trailer offset
          ret = trailer_offset.deserialize(file_buf + file_len - 8, 8, pos);
          EXPECT_TRUE(OB_SUCCESS == ret);
          EXPECT_EQ(trailer_offset.trailer_record_offset_, trailer_addr);

          //read record header of trailer
          pos = 0;
          ret = record_header.deserialize(file_buf + trailer_addr,
                                          sizeof(ObRecordHeader), pos);
          EXPECT_TRUE(OB_SUCCESS == ret);
          EXPECT_EQ(record_header.data_length_, record_header.data_zlength_);
          EXPECT_TRUE(record_header.magic_ == ObSSTableWriter::TRAILER_MAGIC);

          //read trailer
          pos = 0;
          ret = trailer.deserialize(file_buf + trailer_addr + sizeof(ObRecordHeader),
              file_len - trailer_addr, pos);
          EXPECT_TRUE(OB_SUCCESS == ret);
          block_count = trailer.get_block_count();
          EXPECT_TRUE(memcmp(trailer.get_compressor_name(),
                             compressor_name, strlen(compressor_name)) == 0);
          EXPECT_EQ(50000, trailer.get_row_count());

          //read schema
          schema_offset = trailer.get_schema_record_offset();
          schema_len = trailer.get_schema_record_size();
          // read record header of schema
          pos = 0;
          ret = record_header.deserialize(file_buf + schema_offset,
                                          sizeof(ObRecordHeader), pos);
          EXPECT_TRUE(OB_SUCCESS == ret);
          EXPECT_TRUE(record_header.magic_ == ObSSTableWriter::SCHEMA_MAGIC);
          schema_data = file_buf + schema_offset + sizeof(ObRecordHeader);
          if (record_header.data_length_ > record_header.data_zlength_)
          {
            schema_buf = (char *)ob_malloc(record_header.data_length_);
            EXPECT_TRUE(schema_buf != NULL);
            ret = compressor->decompress(schema_data, record_header.data_zlength_,
                                         schema_buf, record_header.data_length_,
                                         uncompressed_size);
            EXPECT_TRUE(OB_SUCCESS == ret);
            EXPECT_EQ(record_header.data_length_, uncompressed_size);
            //deserialize schema
            pos = 0;
            ret = schema.deserialize(schema_buf, uncompressed_size, pos);
            EXPECT_TRUE(OB_SUCCESS == ret);
            ob_free(schema_buf);
          }
          else
          {
            //deserialize schema
            pos = 0;
            ret = schema.deserialize(schema_data, schema_len - sizeof(ObRecordHeader), pos);
            EXPECT_TRUE(OB_SUCCESS == ret);
          }
          EXPECT_TRUE(OB_SUCCESS == ret);

          EXPECT_EQ(150, schema.get_column_count());
          for (int i = 0; i < 150; ++i)
          {
            column = schema.get_column_def(i);
            EXPECT_EQ(2+i%3, (int32_t)column->column_name_id_);
            EXPECT_EQ(i/30+1025, (int32_t)column->table_id_);
            EXPECT_EQ(i%30/3, (int32_t)column->column_group_id_);
            EXPECT_EQ(types[i%3], column->column_value_type_);
          }

          //read bloom filter
          filter_offset = trailer.get_bloom_filter_record_offset();
          filter_len = trailer.get_bloom_filter_record_size();
          if (filter_offset > 0 && filter_len > 0)
          {
            //read record header of bloom filter
            pos = 0;
            ret = record_header.deserialize(file_buf + filter_offset,
                                            sizeof(ObRecordHeader), pos);
            EXPECT_TRUE(OB_SUCCESS == ret);
            EXPECT_TRUE(record_header.magic_ == ObSSTableWriter::BLOOM_FILTER_MAGIC);
          }

          //read index
          index_offset = trailer.get_block_index_record_offset();
          index_len = trailer.get_block_index_record_size();

          //read record header of block index
          pos = 0;
          ret = record_header.deserialize(file_buf + index_offset,
                                          sizeof(ObRecordHeader), pos);
          EXPECT_TRUE(OB_SUCCESS == ret);
          EXPECT_TRUE(record_header.magic_ == ObSSTableWriter::BLOCK_INDEX_MAGIC);
          index_block = file_buf + index_offset + sizeof(ObRecordHeader);
          index_block_len = index_len - sizeof(ObRecordHeader);
          if (record_header.data_length_ > record_header.data_zlength_)
          {
            index_buf = (char *)ob_malloc(record_header.data_length_);
            EXPECT_TRUE(index_buf != NULL);
            ret = compressor->decompress(index_block, record_header.data_zlength_,
                                         index_buf, record_header.data_length_,
                                         uncompressed_size);
            EXPECT_TRUE(OB_SUCCESS == ret);
            EXPECT_EQ(record_header.data_length_, uncompressed_size);
            index_block = index_buf;
          }
          //read block index header
          pos = 0;
          ret = index_header.deserialize(index_block, sizeof(ObSSTableBlockIndexHeader), pos);
          EXPECT_TRUE(OB_SUCCESS == ret);
          EXPECT_EQ(block_count, index_header.sstable_block_count_);
          int32_t empty_index_item = 0;
          for (int i = 0; i < block_count; ++i)
          {
            //read one block index item
            pos = 0;
            ret = index_item.deserialize(index_block + sizeof(ObSSTableBlockIndexHeader)
                                         + sizeof(ObSSTableBlockIndexItem) * i,
                                         sizeof(ObSSTableBlockIndexItem), pos);
            EXPECT_TRUE(OB_SUCCESS == ret);

            // skip index_item whose block_record_size_ is 0
            if (0 != index_item.block_record_size_)
            {
              block_size = index_item.block_record_size_;

              //read record header of one block (first block offset is 0)
              pos = 0;
              ret = record_header.deserialize(file_buf + block_offset,
                                              sizeof(ObRecordHeader), pos);
              EXPECT_TRUE(OB_SUCCESS == ret);
              EXPECT_TRUE(record_header.magic_ == ObSSTableWriter::DATA_BLOCK_MAGIC);
              block_data = file_buf + block_offset + sizeof(ObRecordHeader);
              if (record_header.data_length_ > record_header.data_zlength_)
              {
                block_buf = (char *)ob_malloc(record_header.data_length_);
                EXPECT_TRUE(block_buf != NULL);
                ret = compressor->decompress(block_data, record_header.data_zlength_,
                                             block_buf, record_header.data_length_,
                                             uncompressed_size);
                EXPECT_TRUE(OB_SUCCESS == ret);
                EXPECT_EQ(record_header.data_length_, uncompressed_size);
                block_data = block_buf;
              }
              //read block header of one block
              pos = 0;
              ret = block_header.deserialize(block_data, sizeof(ObSSTableBlockHeader), pos);
              EXPECT_TRUE(OB_SUCCESS == ret);
              EXPECT_TRUE(record_header.data_length_ > block_header.row_index_array_offset_);
              row_count = block_header.row_count_;
              block_index = block_data + block_header.row_index_array_offset_;
              row_data = block_data + sizeof(ObSSTableBlockHeader);
              row_data_len = block_header.row_index_array_offset_ - sizeof(ObSSTableBlockHeader);

              //parse rows of one block
              pos = 0;
              for (int j = 0; j < row_count; ++j)
              {
                new_row.clear();
                new_row.set_obj_count(objs_per_row);
                ret = new_row.deserialize(row_data, row_data_len, pos);
                EXPECT_TRUE(OB_SUCCESS == ret);
                row_key = new_row.get_row_key();

                //check row key
                tmp_key.assign(index, 0, 0);
                key.assign(tmp_key.get_ptr(), tmp_key.key_len());
                EXPECT_EQ(tmp_key.key_len(), row_key.length());
                EXPECT_TRUE(key == row_key);

                //check objs of row
                for (int k = 0; k < objs_per_row; ++k)
                {
                  obj = new_row.get_obj(k);
                  EXPECT_TRUE(NULL != obj);
                  type = obj->get_type();
                  switch (type)
                  {
                  case ObIntType:
                    obj->get_int(val64);
                    EXPECT_EQ(index, val64);
                    break;
                  case ObVarcharType:
                    obj->get_varchar(string_value);
                    EXPECT_TRUE(memcmp(string_value.ptr(), "testing", string_value.length()));
                    break;
                  default:
                    break;
                  }
                }
                index++;
              }
              if (block_buf != NULL)
              {
                ob_free(block_buf);
              }
              block_offset += index_item.block_record_size_;
            }
            else
            {
              empty_index_item++;
            }
          }
          
          //There are 50 empty block index item
          EXPECT_EQ(0, empty_index_item);
          
          if (compressed)
          {
            destroy_compressor(compressor);
          }

          file_util.close();
          if (index_buf != NULL)
          {
            ob_free(index_buf);
          }
          delete [] file_buf;
        }
      };

      TEST_F(TestObSSTableWriter, test_create_sstable)
      {
        ObSSTableWriter writer;
        ObSSTableSchema schema;
        ObString file_name;
        ObString compressor;
        int ret;
        ObSSTableSchemaColumnDef column_def;
        int64_t trailer_offset = 0;

        char cmd[256];
        sprintf(cmd, "mkdir -p %s", sstable_path);
        system(cmd);

        //null path, null compressor, null schema, table version 0
        ret = writer.create_sstable(schema, file_name, compressor, 0);
        EXPECT_TRUE(OB_ERROR == ret);

        file_name.assign(sstable_path, static_cast<int32_t>(strlen(sstable_path)));
        remove(sstable_path);
        //not null path, null compressor, null schema, table version 0
        ret = writer.create_sstable(schema, file_name, compressor, 0);
        EXPECT_TRUE(OB_ERROR == ret);

        char *compressor_name = (char*)COMPRESSOR_NAME;
        compressor.assign(compressor_name, static_cast<int32_t>(strlen(compressor_name) + 1));
        //not null path, not null compressor, null schema, table version 0
        ret = writer.create_sstable(schema, file_name, compressor, 0);
        EXPECT_TRUE(OB_ERROR == ret);

        column_def.table_id_ = 1025;
        column_def.column_group_id_ = 1;
        column_def.column_name_id_ = 2;
        column_def.column_value_type_ = ObIntType;
        schema.add_column_def(column_def);
        //not null path, not null compressor, not null schema,table version 0
        ret = writer.create_sstable(schema, file_name, compressor, 0);
        EXPECT_TRUE(OB_SUCCESS == ret);

        ret = writer.close_sstable(trailer_offset);
        EXPECT_TRUE(OB_SUCCESS == ret);
        EXPECT_TRUE(trailer_offset == -1);
        remove(sstable_path);

        column_def.table_id_ = 1025;
        column_def.column_group_id_ = 2;
        column_def.column_name_id_ = 3;
        column_def.column_value_type_ = ObIntType;
        schema.add_column_def(column_def);

        column_def.table_id_ = 1025;
        column_def.column_group_id_ = 2;
        column_def.column_name_id_ = 4;
        column_def.column_value_type_ = ObVarcharType;
        schema.add_column_def(column_def);

        //not null path, not null compressor, not null schema, create again
        ret = writer.create_sstable(schema, file_name, compressor, 0);
        EXPECT_TRUE(OB_SUCCESS == ret);

        //create sstable again
        ret = writer.create_sstable(schema, file_name, compressor, 0);
        EXPECT_TRUE(OB_ERROR == ret);

        ret = writer.close_sstable(trailer_offset);
        EXPECT_TRUE(OB_SUCCESS == ret);
        EXPECT_TRUE(trailer_offset == -1);
        remove(sstable_path);
      }

      TEST_F(TestObSSTableWriter, test_close_null_sstable)
      {
        ObSSTableWriter writer;
        int ret;
        int64_t trailer_offset = 0;
        int64_t sstable_size = 0;

        ret = writer.close_sstable(trailer_offset);
        EXPECT_TRUE(OB_SUCCESS == ret);
        EXPECT_TRUE(trailer_offset == -1);

        //close ssatble again
        ret = writer.close_sstable(trailer_offset);
        EXPECT_TRUE(OB_SUCCESS == ret);
        EXPECT_TRUE(trailer_offset == -1);

        //close ssatble again
        ret = writer.close_sstable(trailer_offset, sstable_size);
        EXPECT_TRUE(OB_SUCCESS == ret);
        EXPECT_TRUE(trailer_offset == -1);
        EXPECT_TRUE(sstable_size == 0);
      }

      TEST_F(TestObSSTableWriter, test_append_row)
      {
        ObSSTableWriter writer;
        ObSSTableSchema schema;
        ObSSTableRow row;
        ObString file_name;
        ObString compressor;
        int ret;
        ObSSTableSchemaColumnDef column_def;
        int64_t trailer_offset = 0;
        int64_t space_usage = 0;
        ObObj tmp_obj;
        ObString row_key;
        Key tmp_key;
        ObString key;

        file_name.assign(sstable_path, static_cast<int32_t>(strlen(sstable_path)));
        char *compressor_name = (char*)COMPRESSOR_NAME;
        compressor.assign(compressor_name, static_cast<int32_t>(strlen(compressor_name) + 1));
        column_def.reserved_ = 0;
        column_def.table_id_ = 10086;
        column_def.column_group_id_ = 2;
        column_def.column_name_id_ = 2;
        column_def.column_value_type_ = ObIntType;
        schema.add_column_def(column_def);

        ret = writer.create_sstable(schema, file_name, compressor, 0x200);
        EXPECT_TRUE(OB_SUCCESS == ret);

        //append null row
        ret = writer.append_row(row, space_usage);
        EXPECT_EQ(OB_INVALID_ID, row.get_table_id());
        EXPECT_EQ(OB_INVALID_ID, row.get_column_group_id());
        EXPECT_TRUE(OB_ERROR == ret);
        EXPECT_TRUE(space_usage > 0);

        tmp_obj.set_int(10);
        row.add_obj(tmp_obj);

        //append row without row key buf with objs
        ret = writer.append_row(row, space_usage);
        EXPECT_TRUE(OB_ERROR == ret);
        EXPECT_TRUE(space_usage > 0);

        tmp_key.assign(12345, 10, 1000);
        key.assign(tmp_key.get_ptr(), tmp_key.key_len());
        row.set_row_key(key);

        //can not set table id with  invalid table id
        ret = row.set_table_id(OB_INVALID_ID);
        EXPECT_TRUE(OB_ERROR == ret);
       
        //can not set column group id with invalid column group id
        row.set_table_id(10086);
        ret = row.set_column_group_id(OB_INVALID_ID);
        EXPECT_TRUE(OB_ERROR == ret);
        

        //append legal row
        row.set_column_group_id(2);
        ret = writer.append_row(row, space_usage);
        EXPECT_TRUE(OB_SUCCESS == ret);
        EXPECT_TRUE(space_usage > 0);

        row.clear();
        row.set_table_id(10086);
        row.set_column_group_id(2);
        tmp_key.assign(12346, 10, 1000);
        key.assign(tmp_key.get_ptr(), tmp_key.key_len());
        row.set_row_key(key);
        tmp_obj.set_int(1000);
        row.add_obj(tmp_obj);
        //append second legal row  in order (table_id, column_group_id, rowkey)
        ret = writer.append_row(row, space_usage);
        EXPECT_TRUE(OB_SUCCESS == ret);
        EXPECT_TRUE(space_usage > 0);

        //row is not consistent with schema, row has 2 objs, but schema has 1 column
        tmp_obj.set_int(100);
        row.add_obj(tmp_obj);
        EXPECT_EQ(2, row.get_obj_count());
        ret = writer.append_row(row, space_usage);
        EXPECT_TRUE(OB_ERROR == ret);
        EXPECT_TRUE(space_usage > 0);

        //row with inconsistent type with schema
        row.clear();
        tmp_key.assign(12347, 10, 1000);
        key.assign(tmp_key.get_ptr(), tmp_key.key_len());
        row.set_row_key(key);
        tmp_obj.set_modifytime(0);
        row.add_obj(tmp_obj);

        ret = writer.append_row(row, space_usage);
        EXPECT_TRUE(OB_ERROR == ret);
        EXPECT_TRUE(space_usage > 0);

        //append row not in order (table_id, column_group_id, row_key)
        //table id not in order
        row.clear();
        tmp_key.assign(12348, 10, 1000);
        key.assign(tmp_key.get_ptr(), tmp_key.key_len());
        row.set_row_key(key);
        row.set_table_id(1000);
        row.set_column_group_id(2);
        tmp_obj.set_int(100);
        row.add_obj(tmp_obj);
        ret = writer.append_row(row, space_usage);
        EXPECT_TRUE(OB_ERROR == ret);
        EXPECT_TRUE(space_usage > 0);
        
        //column group id not in order
        row.clear();
        tmp_key.assign(12348, 10, 1000);
        key.assign(tmp_key.get_ptr(), tmp_key.key_len());
        row.set_row_key(key);
        row.set_table_id(10086);
        row.set_column_group_id(1);
        tmp_obj.set_int(100);
        row.add_obj(tmp_obj);
        ret = writer.append_row(row, space_usage);
        EXPECT_TRUE(OB_ERROR == ret);
        EXPECT_TRUE(space_usage > 0);
        
        //rowkey not in order
        row.clear();
        tmp_key.assign(12340, 10, 1000);
        key.assign(tmp_key.get_ptr(), tmp_key.key_len());
        row.set_row_key(key);
        row.set_table_id(10086);
        row.set_column_group_id(2);
        tmp_obj.set_int(100);
        row.add_obj(tmp_obj);
        ret = writer.append_row(row, space_usage);
        EXPECT_TRUE(OB_ERROR == ret);
        EXPECT_TRUE(space_usage > 0);
        
        ret = writer.close_sstable(trailer_offset);
        EXPECT_TRUE(OB_SUCCESS == ret);
        EXPECT_TRUE(trailer_offset > 0);

        
        remove(sstable_path);
      }

      TEST_F(TestObSSTableWriter, test_append_many_rows_null_objs)
      {
        ObSSTableWriter writer;
        ObSSTableSchema schema;
        ObSSTableRow row;
        ObString file_name;
        ObString compressor;
        int ret;
        ObSSTableSchemaColumnDef column_def;
        int64_t trailer_offset = 0;
        int64_t space_usage = 0;
        ObObj tmp_obj;
        ObString row_key;
        Key tmp_key;
        ObString key;
        int j = 0;

        file_name.assign(sstable_path, static_cast<int32_t>(strlen(sstable_path)));
        char *compressor_name = (char*)COMPRESSOR_NAME;
        compressor.assign(compressor_name, static_cast<int32_t>(strlen(compressor_name) + 1));

        for (int i = 0; i < OB_MAX_COLUMN_NUMBER; ++i)
        {
          column_def.table_id_ = 1025;
          column_def.column_group_id_ = 2;
          column_def.column_name_id_ = i + 2;
          column_def.column_value_type_ = ObNullType;
          schema.add_column_def(column_def);
        }
        EXPECT_EQ(OB_MAX_COLUMN_NUMBER, schema.get_column_count());

        ret = writer.create_sstable(schema, file_name, compressor, 2);
        EXPECT_TRUE(OB_SUCCESS == ret);

        while (true)
        {
          row.clear();
          build_row(j, row, ObNullType, 1025, 2);
          EXPECT_EQ(OB_MAX_COLUMN_NUMBER, row.get_obj_count());
          ret = writer.append_row(row, space_usage);
          EXPECT_TRUE(OB_SUCCESS == ret);
          EXPECT_TRUE(space_usage > 0);
          if (space_usage > 1024 * 1024)
          {
            break;
          }
          j++;
        }

        ret = writer.close_sstable(trailer_offset);
        EXPECT_TRUE(OB_SUCCESS == ret);
        EXPECT_TRUE(trailer_offset > 0);
        remove(sstable_path);
      }

      TEST_F(TestObSSTableWriter, test_append_many_rows_int_objs)
      {
        ObSSTableWriter writer;
        ObSSTableSchema schema;
        ObSSTableRow row;
        ObString file_name;
        ObString compressor;
        int ret;
        ObSSTableSchemaColumnDef column_def;
        int64_t trailer_offset = 0;
        int64_t space_usage = 0;
        ObObj tmp_obj;
        ObString row_key;
        Key tmp_key;
        ObString key;
        int j = 0;

        file_name.assign(sstable_path, static_cast<int32_t>(strlen(sstable_path)));
        char *compressor_name = (char*)COMPRESSOR_NAME;
        compressor.assign(compressor_name, static_cast<int32_t>(strlen(compressor_name) + 1));

        for (int i = 0; i < OB_MAX_COLUMN_NUMBER; ++i)
        {
          column_def.table_id_ = 1025;
          column_def.column_group_id_ = 2;
          column_def.column_name_id_ = i + 2;
          column_def.column_value_type_ = ObIntType;
          schema.add_column_def(column_def);
        }
        EXPECT_EQ(OB_MAX_COLUMN_NUMBER, schema.get_column_count());
        ret = writer.create_sstable(schema, file_name, compressor, 0);
        EXPECT_TRUE(OB_SUCCESS == ret);

        while (true)
        {
          row.clear();
          build_row(j, row, ObIntType, 1025, 2);
          ret = writer.append_row(row, space_usage);
          EXPECT_TRUE(OB_SUCCESS == ret);
          EXPECT_TRUE(space_usage > 0);
          if (space_usage > 1024 * 1024)
          {
            break;
          }
          j++;
        }

        ret = writer.close_sstable(trailer_offset);
        EXPECT_TRUE(OB_SUCCESS == ret);
        EXPECT_TRUE(trailer_offset > 0);
        remove(sstable_path);
      }

      TEST_F(TestObSSTableWriter, test_append_many_rows_int_objs2)
      {
        ObSSTableWriter writer;
        ObSSTableSchema schema;
        ObSSTableRow row;
        ObString file_name;
        ObString compressor;
        int ret;
        ObSSTableSchemaColumnDef column_def;
        int64_t trailer_offset = 0;
        int64_t sstable_size = 0;
        int64_t space_usage = 0;
        uint64_t table_id = 0;
        uint64_t column_group_id = 0;
        ObObj tmp_obj;
        ObString row_key;
        Key tmp_key;
        ObString key;
        int j = 0;

        file_name.assign(sstable_path, static_cast<int32_t>(strlen(sstable_path)));
        char *compressor_name = (char*)COMPRESSOR_NAME;
        compressor.assign(compressor_name, static_cast<int32_t>(strlen(compressor_name) + 1));
        for (int t = 0; t < 5; ++t)
        {
          column_def.table_id_ = static_cast<uint32_t>(TABLE_ID_BASE + t);
          for (int c = 0; c < 10; ++c)
          {
            column_def.column_group_id_ = static_cast<uint16_t>(c);
            for (int i = 0; i < 12 /*OB_MAX_COLUMN_NUMBER*/; ++i)
            {
              column_def.column_name_id_ = i + 2;
              column_def.column_value_type_ = ObIntType;
              column_def.reserved_ = 0;
              schema.add_column_def(column_def);
            }
          }
        }

        ret = writer.create_sstable(schema, file_name, compressor, 1);
        EXPECT_TRUE(OB_SUCCESS == ret);

        while ( 100000 > j)
        {
          row.clear();
          table_id = j/20000 + TABLE_ID_BASE;
          column_group_id = j%20000/2000;
          build_row(j, row, ObIntType, 12, table_id, column_group_id);
          ret = writer.append_row(row, space_usage);
          EXPECT_TRUE(OB_SUCCESS == ret);
          EXPECT_TRUE(space_usage > 0);
          j++;
        }

        ret = writer.close_sstable(trailer_offset, sstable_size);
        EXPECT_TRUE(OB_SUCCESS == ret);
        EXPECT_TRUE(trailer_offset > 0);
        EXPECT_TRUE(sstable_size > 0);

        remove(sstable_path);
      }

      TEST_F(TestObSSTableWriter, test_write_one_file)
      {
        ObSSTableSchemaColumnDef column_def;
        ObSSTableSchema schema;
        ObSSTableRow row;
        char *compressor_name = (char*)COMPRESSOR_NAME;
        ObSSTableWriter writer;
        ObString file_name(static_cast<int32_t>(strlen(sstable_path) + 1),
                      static_cast<int32_t>(strlen(sstable_path) + 1), sstable_path);
        ObString compressor(static_cast<int32_t>(strlen(compressor_name) + 1),
                            static_cast<int32_t>(strlen(compressor_name) + 1), compressor_name);
        int64_t disk_usage = 0;
        int64_t trailer_offset = 0;
        ObObj obj;
        uint64_t table_id = 0;
        uint64_t column_group_id = 0;
        char value_data[1024 + 1];
        char *ptr;
        int ret;

        // init schema
        for (int i = 0; i < 5; ++i)
        {
          column_def.table_id_ = 1025 + i;
          for ( int j = 0; j < 10 ; ++j)
          {
            column_def.column_group_id_ = static_cast<uint16_t>(j);
            column_def.column_name_id_ = 2;
            column_def.column_value_type_ = ObIntType;
            schema.add_column_def(column_def);

            column_def.column_name_id_ = 3;
            column_def.column_value_type_ = ObIntType;
            schema.add_column_def(column_def);

            column_def.column_name_id_ = 4;
            column_def.column_value_type_ = ObVarcharType;
            schema.add_column_def(column_def);
          }
        }
        // create sstable file
        if (OB_ERROR == (ret = writer.create_sstable(schema, file_name,
                                  compressor, 2)))
        {
          TBSYS_LOG(ERROR, "Failed to create sstable file: %s", sstable_path);
        }
        EXPECT_TRUE(OB_SUCCESS == ret);

        //build data
        ptr = value_data;
        for (int i = 0; i < 128; ++i) {
          memcpy(ptr, "testing ", 8);
          ptr += 8;
        }
        ObString value_str(1025, 1025, value_data);

        for (int i = 0; i < 500000; ++i) {
          row.clear();
          table_id = i / 100000 + 1025;
          column_group_id = i%100000/10000;
          row.set_table_id(table_id);
          row.set_column_group_id(column_group_id);
          Key tmp_key(i, 0, 0);
          ObString row_key(tmp_key.key_len(), tmp_key.key_len(), tmp_key.get_ptr());
          row.set_row_key(row_key);

          obj.set_int(i);
          row.add_obj(obj);
          obj.set_int(i);
          row.add_obj(obj);
          obj.set_varchar(value_str);
          row.add_obj(obj);

          if (OB_ERROR == (ret = writer.append_row(row, disk_usage)))
          {
            cout << "add row failed" << endl;
            return;
          }
          EXPECT_TRUE(OB_SUCCESS == ret);
        }

        if (OB_ERROR == (ret = writer.close_sstable(trailer_offset)))
        {
          cout << "close sstable failed ------------------" << endl;
        }
        EXPECT_TRUE(OB_SUCCESS == ret);
        remove(sstable_path);
      }

      TEST_F(TestObSSTableWriter, test_write_patch_file_with_check)
      {
        ObSSTableWriter writer;
        ObSSTableSchema schema;
        ObSSTableRow row;
        ObString file_name;
        ObString compressor;
        int ret;
        ObSSTableSchemaColumnDef column_def;
        int64_t trailer_offset = 0;
        int64_t space_usage = 0;
        int64_t sstable_size = 0;
        uint64_t table_id = 0;
        uint64_t column_group_id = 0;
        ObObj tmp_obj;
        ObString row_key;
        Key tmp_key;
        ObString key;
        ObObj obj;
        char value_data[1024 + 1];
        char *ptr;
        file_name.assign(sstable_path, static_cast<int32_t>(strlen(sstable_path)));
        char *compressor_name = (char*)COMPRESSOR_NAME;
        compressor.assign(compressor_name, static_cast<int32_t>(strlen(compressor_name) + 1));

        //init schema
        for (int i = 0; i < 5; ++i)
        {
          column_def.table_id_ = 1025 + i;
          for ( int j = 0; j < 10 ; ++j)
          {
            column_def.column_group_id_ = static_cast<uint16_t>(j);
            column_def.column_name_id_ = 2;
            column_def.column_value_type_ = ObIntType;
            schema.add_column_def(column_def);
            
            column_def.column_name_id_ = 3;
            column_def.column_value_type_ = ObIntType;
            schema.add_column_def(column_def);

            column_def.column_name_id_ = 4;
            column_def.column_value_type_ = ObVarcharType;
            schema.add_column_def(column_def);
          }
        }
       
        ret = writer.create_sstable(schema, file_name, compressor, 2, 
                                    OB_SSTABLE_STORE_SPARSE);
        EXPECT_TRUE(OB_SUCCESS == ret);

        //build data
        ptr = value_data;
        for (int i = 0; i < 128; ++i) {
          memcpy(ptr, "testing ", 8);
          ptr += 8;
        }
        ObString value_str(1025, 1025, value_data);

        for (int i = 0; i < 500000; ++i) {
          row.clear();
          table_id = i / 100000 + 1025;
          column_group_id = i%100000/10000;
          row.set_table_id(table_id);
          row.set_column_group_id(column_group_id);
          Key tmp_key(i, 0, 0);
          ObString row_key(tmp_key.key_len(), tmp_key.key_len(), tmp_key.get_ptr());
          row.set_row_key(row_key);

          obj.set_int(i);
          row.add_obj(obj);
          obj.set_int(i);
          row.add_obj(obj);
          obj.set_varchar(value_str);
          row.add_obj(obj);

          if (OB_ERROR == (ret = writer.append_row(row, space_usage)))
          {
            cout << "add row failed" << endl;
            return;
          }
          EXPECT_TRUE(OB_SUCCESS == ret);
        }

        if (OB_ERROR == (ret = writer.close_sstable(trailer_offset, sstable_size)))
        {
          cout << "close sstable failed ------------------" << endl;
        }
        EXPECT_TRUE(OB_SUCCESS == ret);
        EXPECT_TRUE(trailer_offset > 0);
        EXPECT_TRUE(sstable_size > 0);
        
        read_sstable_and_check(sstable_path, trailer_offset, true);
        remove(sstable_path);
      }

      TEST_F(TestObSSTableWriter, test_write_two_file_with_check)
      {
        ObSSTableWriter writer;
        ObSSTableSchema schema;
        ObSSTableRow row;
        ObString file_name;
        ObString compressor;
        int ret;
        ObSSTableSchemaColumnDef column_def;
        int64_t trailer_offset = 0;
        int64_t space_usage = 0;
        int64_t sstable_size = 0;
        uint64_t table_id = 0;
        uint64_t column_group_id = 0;
        ObObj tmp_obj;
        ObString row_key;
        Key tmp_key;
        ObString key;
        ObObj obj;
        char value_data[1024 + 1];
        char *ptr;
        file_name.assign(sstable_path, static_cast<int32_t>(strlen(sstable_path)));
        char *compressor_name = (char*)COMPRESSOR_NAME;
        compressor.assign(compressor_name, static_cast<int32_t>(strlen(compressor_name) + 1));

        //init schema
        for (int i = 0; i < 5; ++i)
        {
          column_def.table_id_ = 1025 + i;
          for ( int j = 0; j < 10 ; ++j)
          {
            column_def.column_group_id_ = static_cast<uint16_t>(j);
            column_def.column_name_id_ = 2;
            column_def.column_value_type_ = ObIntType;
            schema.add_column_def(column_def);
            
            column_def.column_name_id_ = 3;
            column_def.column_value_type_ = ObIntType;
            schema.add_column_def(column_def);

            column_def.column_name_id_ = 4;
            column_def.column_value_type_ = ObVarcharType;
            schema.add_column_def(column_def);
          }
        }
       
        ret = writer.create_sstable(schema, file_name, compressor, 2);
        EXPECT_TRUE(OB_SUCCESS == ret);

        //build data
        ptr = value_data;
        for (int i = 0; i < 128; ++i) {
          memcpy(ptr, "testing ", 8);
          ptr += 8;
        }
        ObString value_str(1025, 1025, value_data);

        for (int i = 0; i < 500000; ++i) {
          row.clear();
          table_id = i / 100000 + 1025;
          column_group_id = i%100000/10000;
          row.set_table_id(table_id);
          row.set_column_group_id(column_group_id);
          Key tmp_key(i, 0, 0);
          ObString row_key(tmp_key.key_len(), tmp_key.key_len(), tmp_key.get_ptr());
          row.set_row_key(row_key);

          obj.set_int(i);
          row.add_obj(obj);
          obj.set_int(i);
          row.add_obj(obj);
          obj.set_varchar(value_str);
          row.add_obj(obj);

          if (OB_ERROR == (ret = writer.append_row(row, space_usage)))
          {
            cout << "add row failed" << endl;
            return;
          }
          EXPECT_TRUE(OB_SUCCESS == ret);
        }

        if (OB_ERROR == (ret = writer.close_sstable(trailer_offset, sstable_size)))
        {
          cout << "close sstable failed ------------------" << endl;
        }
        EXPECT_TRUE(OB_SUCCESS == ret);
        EXPECT_TRUE(trailer_offset > 0);
        EXPECT_TRUE(sstable_size > 0);
        
        read_sstable_and_check(sstable_path, trailer_offset, true);
        remove(sstable_path);

        schema.reset();
        //write second file
      
        //init schema
        for (int i = 0; i < 5; ++i)
        {
          column_def.table_id_ = 1025 + i;
          for ( int j = 0; j < 10 ; ++j)
          {
            column_def.column_group_id_ = static_cast<uint16_t>(j);
            column_def.column_name_id_ = 2;
            column_def.column_value_type_ = ObIntType;
            schema.add_column_def(column_def);
            
            column_def.column_name_id_ = 3;
            column_def.column_value_type_ = ObIntType;
            schema.add_column_def(column_def);

            column_def.column_name_id_ = 4;
            column_def.column_value_type_ = ObVarcharType;
            schema.add_column_def(column_def);
          }
        }
       
        ret = writer.create_sstable(schema, file_name, compressor, 2);
        EXPECT_TRUE(OB_SUCCESS == ret);

        for (int i = 0; i < 500000; ++i) {
          row.clear();
          table_id = i / 100000 + 1025;
          column_group_id = i%100000/10000;
          row.set_table_id(table_id);
          row.set_column_group_id(column_group_id);
          Key tmp_key(i, 0, 0);
          ObString row_key(tmp_key.key_len(), tmp_key.key_len(), tmp_key.get_ptr());
          row.set_row_key(row_key);

          obj.set_int(i);
          row.add_obj(obj);
          obj.set_int(i);
          row.add_obj(obj);
          obj.set_varchar(value_str);
          row.add_obj(obj);

          if (OB_ERROR == (ret = writer.append_row(row, space_usage)))
          {
            cout << "add row failed" << endl;
            return;
          }
          EXPECT_TRUE(OB_SUCCESS == ret);
        }

        if (OB_ERROR == (ret = writer.close_sstable(trailer_offset, sstable_size)))
        {
          cout << "close sstable failed ------------------" << endl;
        }
        EXPECT_TRUE(OB_SUCCESS == ret);
        EXPECT_TRUE(trailer_offset > 0);
        EXPECT_TRUE(sstable_size > 0);
        
        read_sstable_and_check(sstable_path, trailer_offset, true);
        remove(sstable_path);
      }
      
      TEST_F(TestObSSTableWriter, read_sstable_and_check)
      {
        ObString file_name;
        
        //read_sstable_and_check(sstable_path, trailer_offset, true);

      }
      
      TEST_F(TestObSSTableWriter, append_large_row)
      {
        ObSSTableWriter writer;
        ObSSTableSchema schema;
        ObSSTableRow row;
        ObString file_name;
        ObString compressor;
        int ret;
        ObSSTableSchemaColumnDef column_def;
        int64_t trailer_offset = 0;
        int64_t space_usage = 0;
        ObObj tmp_obj;
        ObString row_key;
        Key tmp_key;
        ObString key;
        int j = 0;

        file_name.assign(sstable_path, static_cast<int32_t>(strlen(sstable_path)));
        char *compressor_name = (char*)COMPRESSOR_NAME;
        compressor.assign(compressor_name, static_cast<int32_t>(strlen(compressor_name) + 1));

        for (int i = 0; i < OB_MAX_COLUMN_NUMBER; ++i)
        {
          column_def.table_id_ = 1025;
          column_def.column_group_id_ = 2;
          column_def.column_name_id_ = i + 2;
          column_def.column_value_type_ = ObVarcharType;
          schema.add_column_def(column_def);
        }
        EXPECT_EQ(OB_MAX_COLUMN_NUMBER, schema.get_column_count());

        ret = writer.create_sstable(schema, file_name, compressor, 2);
        EXPECT_TRUE(OB_SUCCESS == ret);

        while (true)
        {
          if(j > 100)
          {
            break;
          }
          row.clear();
          //build row row size form 1M--5M
          build_large_row(j, row, 1025, 2, 8 * (j%8+1));
          EXPECT_EQ(OB_MAX_COLUMN_NUMBER, row.get_obj_count());
          //fprintf(stderr, "row serialize size is %ld\n", row.get_serialize_size());
          ret = writer.append_row(row, space_usage);
          EXPECT_TRUE(OB_SUCCESS == ret);
          EXPECT_TRUE(space_usage > 0);
          //   fprintf(stderr, "space_usage=%ld\n", space_usage);
          if (space_usage > 32 * 1024 * 1024)
          {
            break;
          }
          j++;
        }

        ret = writer.close_sstable(trailer_offset);
        EXPECT_TRUE(OB_SUCCESS == ret);
        EXPECT_TRUE(trailer_offset > 0);
        remove(sstable_path);        
      }
    }//end namespace sstable
  }//end namespace tests
}//end namespace oceanbase

int main(int argc, char** argv)
{
  ob_init_memory_pool();
  TBSYS_LOGGER.setLogLevel("ERROR");
  printf("sizeof obj=%ld", sizeof(ObObj));
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
