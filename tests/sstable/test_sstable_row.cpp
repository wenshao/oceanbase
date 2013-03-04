/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * test_sstable_row.cpp for test sstable row structure
 *
 * Authors: 
 *   huating <huating.zmq@taobao.com>
 *
 */

#include <tblog.h>
#include <gtest/gtest.h>
#include "sstable/ob_sstable_schema.h"
#include "common/ob_object.h"
#include "sstable/ob_sstable_row.h"
#include "key.h"

using namespace oceanbase::common;
using namespace oceanbase::sstable;

namespace oceanbase
{
  namespace tests
  {
    namespace sstable 
    {
      class TestObSSTableRow: public ::testing::Test
      {
      public:
        virtual void SetUp()
        {
      
        }
      
        virtual void TearDown()
        {
      
        }
      };
      
      TEST_F(TestObSSTableRow, test_init)
      {
        ObSSTableRow row;
        const ObObj *obj = NULL;
        ObString row_key;

        EXPECT_EQ(0, row.get_obj_count());

        for (int i = 0; i < OB_MAX_COLUMN_NUMBER; ++i)
        {
          obj = row.get_obj(i);
          EXPECT_TRUE(NULL == obj);
        }

        row_key = row.get_row_key();
        EXPECT_EQ(0, row_key.length());
        EXPECT_TRUE(NULL == row_key.ptr());
      }

      TEST_F(TestObSSTableRow, test_add_objs)
      {
        ObSSTableRow row;
        ObObj tmp_obj;
        const ObObj *obj = NULL;
        ObString row_key;
        char value_data[1024 + 1];
        char *ptr;
        uint64_t table_id = 1000;
        uint32_t column_group_id = 5;
        //build data
        ptr = value_data;
        for (int i = 0; i < 128; ++i) {
          memcpy(ptr, "testing ", 8);
          ptr += 8;
        }
        ObString value_str(1025, 1025, value_data);

        Key tmp_key(10, 0, 0);
        ObString key(tmp_key.key_len(), tmp_key.key_len(), tmp_key.get_ptr());
        row.set_row_key(key);
        row.set_table_id(table_id);
        row.set_column_group_id(column_group_id);

        tmp_obj.set_int(10);
        row.add_obj(tmp_obj);
        tmp_obj.set_int(100);
        row.add_obj(tmp_obj);
        tmp_obj.set_varchar(value_str);
        row.add_obj(tmp_obj);

        EXPECT_EQ(3, row.get_obj_count());

        obj = row.get_obj(0);
        int64_t val1 = 0;
        obj->get_int(val1);
        EXPECT_EQ(10, val1);

        obj = row.get_obj(1);
        int64_t val2 = 0;
        obj->get_int(val2);
        EXPECT_EQ(100, val2);

        obj = row.get_obj(2);
        ObString val_str;
        obj->get_varchar(val_str);
        EXPECT_EQ(value_str.length(), val_str.length());
        EXPECT_TRUE(value_str == val_str);

        row_key = row.get_row_key();
        EXPECT_EQ(tmp_key.key_len(), row_key.length());
        EXPECT_TRUE(key == row_key);
        EXPECT_EQ((uint64_t)1000, row.get_table_id());
        EXPECT_EQ((uint64_t)5, row.get_column_group_id());
      }

      TEST_F(TestObSSTableRow, test_add_max_objs)
      {
        ObSSTableRow row;
        ObObj tmp_obj;
        const ObObj *obj = NULL;
        ObString row_key;

        Key tmp_key(12345, 10, 1000);
        ObString key(tmp_key.key_len(), tmp_key.key_len(), tmp_key.get_ptr());
        row.set_row_key(key);
        row.set_table_id(1000);
        row.set_column_group_id(5);

        row_key = row.get_row_key();
        EXPECT_EQ(tmp_key.key_len(), row_key.length());
        EXPECT_TRUE(key == row_key);
        EXPECT_EQ((uint64_t)1000, row.get_table_id());
        EXPECT_EQ((uint64_t)5, row.get_column_group_id());

        for (int i = 0; i < OB_MAX_COLUMN_NUMBER; ++i)
        {
          tmp_obj.set_int(i);
          row.add_obj(tmp_obj);
        }
        EXPECT_EQ(OB_MAX_COLUMN_NUMBER, row.get_obj_count());

        for (int i = 0; i < OB_MAX_COLUMN_NUMBER; ++i)
        {
          obj = row.get_obj(i);
          EXPECT_TRUE(NULL != obj);
          int64_t val = 0;
          obj->get_int(val);
          EXPECT_EQ(i, val);
        }
      }

      TEST_F(TestObSSTableRow, test_add_too_many_objs)
      {
        ObSSTableRow row;
        ObObj tmp_obj;
        const ObObj *obj = NULL;
        ObString row_key;
        int  ret;

        Key tmp_key(123456, 10, 1000);
        ObString key(tmp_key.key_len(), tmp_key.key_len(), tmp_key.get_ptr());
        row.set_row_key(key);
        row.set_table_id(1000);
        row.set_column_group_id(5);
        
        row_key = row.get_row_key();
        EXPECT_EQ(tmp_key.key_len(), row_key.length());
        EXPECT_TRUE(key == row_key);
        EXPECT_EQ((uint64_t)1000, row.get_table_id());
        EXPECT_EQ((uint64_t)5, row.get_column_group_id());

        for (int i = 0; i < OB_MAX_COLUMN_NUMBER + 5; ++i)
        {
          tmp_obj.set_int(i);
          ret = row.add_obj(tmp_obj);
          if (i >= OB_MAX_COLUMN_NUMBER)
          {
            EXPECT_TRUE(ret == OB_ERROR);
          }
        }
        EXPECT_EQ(OB_MAX_COLUMN_NUMBER, row.get_obj_count());

        for (int i = 0; i < OB_MAX_COLUMN_NUMBER; ++i)
        {
          obj = row.get_obj(i);
          EXPECT_TRUE(NULL != obj);
          int64_t val = 0;
          obj->get_int(val);
          EXPECT_EQ(i, val);
        }

        //get obj with index non existent
        obj = row.get_obj(OB_MAX_COLUMN_NUMBER + 10);
        EXPECT_TRUE(NULL == obj);
        obj = row.get_obj(OB_MAX_COLUMN_NUMBER + 100);
        EXPECT_TRUE(NULL == obj);
        obj = row.get_obj(-1);
        EXPECT_TRUE(NULL == obj);
      }

      TEST_F(TestObSSTableRow, test_add_objs_with_id)
      {
        ObSSTableRow row;
        ObObj tmp_obj;
        const ObObj *obj = NULL;
        int64_t column_count = OB_MAX_COLUMN_NUMBER * 2;
        int64_t cur_idx = 0;
        ObString row_key;
        int  ret;

        Key tmp_key(123456, 10, 1000);
        ObString key(tmp_key.key_len(), tmp_key.key_len(), tmp_key.get_ptr());
        row.set_row_key(key);
        row.set_table_id(1000);
        row.set_column_group_id(5);

        row_key = row.get_row_key();
        EXPECT_EQ(tmp_key.key_len(), row_key.length());
        EXPECT_TRUE(key == row_key);
        EXPECT_EQ((uint64_t)1000, row.get_table_id());
        EXPECT_EQ((uint64_t)5, row.get_column_group_id());

        for (int i = 0; i < column_count; ++i)
        {
          tmp_obj.set_int(i);
          ret = row.add_obj(tmp_obj, i + 2);
          EXPECT_TRUE(ret == OB_SUCCESS);
        }
        EXPECT_EQ(column_count * 2, row.get_obj_count());

        for (int i = 0; i < column_count; ++i)
        {
          obj = row.get_obj(static_cast<int32_t>(cur_idx++));
          EXPECT_TRUE(NULL != obj);
          EXPECT_EQ(ObIntType, obj->get_type());
          int64_t ival = 0;
          obj->get_int(ival);
          EXPECT_EQ((uint64_t)i + 2, (uint64_t)ival);

          obj = row.get_obj(static_cast<int32_t>(cur_idx++));
          EXPECT_TRUE(NULL != obj);
          int64_t val = 0;
          obj->get_int(val);
          EXPECT_EQ(i, val);
        }

        //get obj with index non existent
        obj = row.get_obj(static_cast<int32_t>(column_count * 2 + 10));
        EXPECT_TRUE(NULL == obj);
        obj = row.get_obj(static_cast<int32_t>(column_count * 2 + 100));
        EXPECT_TRUE(NULL == obj);
        obj = row.get_obj(-1);
        EXPECT_TRUE(NULL == obj);
      }

      TEST_F(TestObSSTableRow, test_set_large_size_key)
      {
        ObSSTableRow row;
        ObString row_key;

        char tmp_key[1024];
        char *ptr = tmp_key;
        for (int i = 0; i < 128; ++i) {
          memcpy(ptr, "testing ", 8);
          ptr += 8;
        }
        ObString key(1024, 1024, tmp_key);
        row.set_row_key(key);
        row.set_table_id(1000);
        row.set_column_group_id(5);

        row_key = row.get_row_key();
        EXPECT_EQ(key.length(), row_key.length());
        EXPECT_TRUE(key == row_key);
        EXPECT_EQ((uint64_t)1000, row.get_table_id());
        EXPECT_EQ((uint64_t)5, row.get_column_group_id());
      }

      TEST_F(TestObSSTableRow, test_set_null_key)
      {
        ObSSTableRow row;
        ObString row_key;
        int ret;

        ObString key;
        ret = row.set_row_key(key);
        EXPECT_TRUE(ret == OB_ERROR);

        row_key = row.get_row_key();
        EXPECT_EQ(0, row_key.length());
        EXPECT_TRUE(key == row_key);
      }

      TEST_F(TestObSSTableRow, test_get_nonexistent_obj)
      {
        ObSSTableRow row;
        const ObObj *obj = NULL;

        obj = row.get_obj(0);
        EXPECT_TRUE(NULL == obj);

        obj = row.get_obj(-1);
        EXPECT_TRUE(NULL == obj);

        obj = row.get_obj(12345678);
        EXPECT_TRUE(NULL == obj);
      }

      TEST_F(TestObSSTableRow, test_set_obj_count)
      {
        ObSSTableRow row;
        ObString row_key;
        const ObObj *obj = NULL;
        int ret;

        ret = row.set_obj_count(1);
        EXPECT_TRUE(ret == OB_SUCCESS);
        EXPECT_EQ(1, row.get_obj_count());
        ret = row.set_obj_count(10);
        EXPECT_TRUE(ret == OB_SUCCESS);
        EXPECT_EQ(10, row.get_obj_count());
        ret = row.set_obj_count(OB_MAX_COLUMN_NUMBER);
        EXPECT_TRUE(ret == OB_SUCCESS);
        EXPECT_EQ(OB_MAX_COLUMN_NUMBER, row.get_obj_count());

        //test clear 
        row.clear();
        EXPECT_EQ(0, row.get_obj_count());
        for (int i = 0; i < OB_MAX_COLUMN_NUMBER; ++i)
        {
          obj = row.get_obj(i);
          EXPECT_TRUE(NULL == obj);
        }
        row_key = row.get_row_key();
        EXPECT_EQ(0, row_key.length());
        EXPECT_TRUE(NULL == row_key.ptr());

        ret = row.set_obj_count(0);
        EXPECT_TRUE(ret == OB_ERROR);
        EXPECT_EQ(0, row.get_obj_count());
        ret = row.set_obj_count(OB_MAX_COLUMN_NUMBER + 1);
        EXPECT_TRUE(ret == OB_ERROR);
        EXPECT_EQ(0, row.get_obj_count());
        ret = row.set_obj_count(OB_MAX_COLUMN_NUMBER + 10);
        EXPECT_TRUE(ret == OB_ERROR);
        EXPECT_EQ(0, row.get_obj_count());
      }

      TEST_F(TestObSSTableRow, test_check_schema_one_obj)
      {
        ObSSTableSchema schema;
        ObSSTableSchemaColumnDef column_def;
        ObSSTableRow row;
        ObObj tmp_obj;
        ObString row_key;
        int ret;

        //both schema and row are null
        ret = row.check_schema(schema);
        EXPECT_TRUE(ret == OB_ERROR);
        
        row.set_table_id(1000);
        row.set_column_group_id(2);
        EXPECT_EQ((uint64_t)1000, row.get_table_id());
        EXPECT_EQ((uint64_t)2, row.get_column_group_id());

        column_def.column_group_id_ = 2;
        column_def.column_name_id_ = 2;
        column_def.column_value_type_ = ObIntType;
        column_def.table_id_ = 1000;
        schema.add_column_def(column_def);
        
        EXPECT_EQ(1, schema.get_column_count());

        //schema is not null, row is null
        ret = row.check_schema(schema);
        EXPECT_TRUE(ret == OB_ERROR);

        tmp_obj.set_int(10);
        row.add_obj(tmp_obj);

        //both row and schema are not null
        ret = row.check_schema(schema);
        EXPECT_TRUE(ret == OB_SUCCESS);

        //schema is null, row is not null
        schema.reset();
        ret = row.check_schema(schema);
        EXPECT_TRUE(ret == OB_ERROR);

        row.clear();
      }

      TEST_F(TestObSSTableRow, test_check_schema_four_objs)
      {
        ObSSTableSchema schema;
        ObSSTableSchemaColumnDef column_def;
        ObSSTableRow row;
        ObObj tmp_obj;
        ObString row_key;
        int ret;
        char value_data[1024 + 1];
        char *ptr;

        //both schema and row are null
        ret = row.check_schema(schema);
        EXPECT_TRUE(ret == OB_ERROR);

        row.set_table_id(1000);
        row.set_column_group_id(2);
        EXPECT_EQ((uint64_t)1000, row.get_table_id());
        EXPECT_EQ((uint64_t)2, row.get_column_group_id());

        column_def.column_group_id_ =2;
        column_def.column_name_id_ = 2;
        column_def.column_value_type_ = ObIntType;
        column_def.table_id_ = 1000;
        schema.add_column_def(column_def);

        column_def.column_group_id_ = 2;
        column_def.column_name_id_ = 3;
        column_def.column_value_type_ = ObIntType;
        column_def.table_id_ = 1000;
        schema.add_column_def(column_def);
      
        column_def.column_group_id_ = 2;
        column_def.column_name_id_ = 4;
        column_def.column_value_type_ = ObVarcharType;
        column_def.table_id_ = 1000;
        schema.add_column_def(column_def);

        column_def.column_name_id_ = 5;
        column_def.column_value_type_ = ObNullType;
        schema.add_column_def(column_def);

        //schema is not null, row is null
        ret = row.check_schema(schema);
        EXPECT_TRUE(ret == OB_ERROR);

        //build data
        ptr = value_data;
        for (int i = 0; i < 128; ++i) {
          memcpy(ptr, "testing ", 8);
          ptr += 8;
        }
        ObString value_str(1025, 1025, value_data);
        
        tmp_obj.set_int(10);
        row.add_obj(tmp_obj);

        //row and schema with different column
        ret = row.check_schema(schema);
        EXPECT_TRUE(ret == OB_ERROR);

        tmp_obj.set_int(100);
        row.add_obj(tmp_obj);

        //row and schema with different column
        ret = row.check_schema(schema);
        EXPECT_TRUE(ret == OB_ERROR);

        tmp_obj.set_varchar(value_str);
        row.add_obj(tmp_obj);

        //row and schema with different column
        ret = row.check_schema(schema);
        EXPECT_TRUE(ret == OB_ERROR);

        tmp_obj.set_null();
        row.add_obj(tmp_obj);

        //both row and schema are not null, with consistent column
        ret = row.check_schema(schema);
        EXPECT_TRUE(ret == OB_SUCCESS);

        //schema is null, row is not null
        schema.reset();
        ret = row.check_schema(schema);
        EXPECT_TRUE(ret == OB_ERROR);

        row.clear();
      }

      TEST_F(TestObSSTableRow, test_check_schema_max_objs)
      {
        ObSSTableSchema schema;
        ObSSTableSchemaColumnDef column_def;
        ObSSTableRow row;
        ObObj tmp_obj;
        ObString row_key;
        int ret;

        //both schema and row are null
        ret = row.check_schema(schema);
        EXPECT_TRUE(ret == OB_ERROR);
        
        row.set_table_id(1000);
        row.set_column_group_id(2);
        EXPECT_EQ((uint64_t)1000, row.get_table_id());
        EXPECT_EQ((uint64_t)2, row.get_column_group_id());

        for (int i = 0; i < OB_MAX_COLUMN_NUMBER; ++i)
        {
          column_def.column_group_id_ = 2;
          column_def.column_name_id_ = i + 2;
          column_def.column_value_type_ = ObIntType;
          column_def.table_id_ = 1000;
          schema.add_column_def(column_def);
        }
        EXPECT_EQ(OB_MAX_COLUMN_NUMBER, schema.get_column_count());

        //schema is not null, row is null
        ret = row.check_schema(schema);
        EXPECT_TRUE(ret == OB_ERROR);

        for (int i = 0; i < OB_MAX_COLUMN_NUMBER; ++i)
        {
          tmp_obj.set_int(i);
          row.add_obj(tmp_obj);
        }
        EXPECT_EQ(OB_MAX_COLUMN_NUMBER, row.get_obj_count());

        //both row and schema are not null
        ret = row.check_schema(schema);
        EXPECT_TRUE(ret == OB_SUCCESS);

        //schema is null, row is not null
        schema.reset();
        ret = row.check_schema(schema);
        EXPECT_TRUE(ret == OB_ERROR);

        row.clear();
      }

      TEST_F(TestObSSTableRow, test_check_schema_with_multi_table)
      {
        ObSSTableSchema schema;
        ObSSTableSchemaColumnDef column_def;
        ObSSTableRow row;
        ObObj tmp_obj;
        ObString row_key;
        int ret;

        //both schema and row are null
        ret = row.check_schema(schema);
        EXPECT_TRUE(ret == OB_ERROR);
        row.set_table_id(1000);
        row.set_column_group_id(2);

        EXPECT_EQ((uint64_t)1000, row.get_table_id());
        EXPECT_EQ((uint64_t)2, row.get_column_group_id());

        for(int j = 995; j < 1002; ++j)
        {
          for (int i = 0; i < OB_MAX_COLUMN_NUMBER; ++i)
          {
            column_def.column_group_id_ = 2;
            column_def.column_name_id_ = i + 2;
            column_def.column_value_type_ = ObIntType;
            column_def.table_id_ = j;
            schema.add_column_def(column_def);
          }
        }
        
        EXPECT_EQ(OB_MAX_COLUMN_NUMBER * 7, schema.get_column_count());

        //schema is not null, row is null
        ret = row.check_schema(schema);
        EXPECT_TRUE(ret == OB_ERROR);

        for (int i = 0; i < OB_MAX_COLUMN_NUMBER; ++i)
        {
          tmp_obj.set_int(i);
          row.add_obj(tmp_obj);
        }
        EXPECT_EQ(OB_MAX_COLUMN_NUMBER, row.get_obj_count());

        //both row and schema are not null
        ret = row.check_schema(schema);
        EXPECT_TRUE(ret == OB_SUCCESS);

        //schema is null, row is not null
        schema.reset();
        ret = row.check_schema(schema);
        EXPECT_TRUE(ret == OB_ERROR);

        row.clear();
      }

      TEST_F(TestObSSTableRow, test_serialize_and_deserialize)
      {
        ObSSTableRow row;
        ObSSTableRow deserialized_row;
        ObObj tmp_obj;
        const ObObj *obj = NULL;
        ObString row_key;
        int64_t buf_size = 0;
        int64_t pos = 0;

        Key tmp_key(12345, 10, 1000);
        ObString key(tmp_key.key_len(), tmp_key.key_len(), tmp_key.get_ptr());
        row.set_row_key(key);
        row.set_table_id(1000);
        row.set_column_group_id(5);

        row_key = row.get_row_key();
        EXPECT_EQ(tmp_key.key_len(), row_key.length());
        EXPECT_TRUE(key == row_key);
        EXPECT_EQ((uint64_t)1000, row.get_table_id());
        EXPECT_EQ((uint64_t)5, row.get_column_group_id());

        for (int i = 0; i < OB_MAX_COLUMN_NUMBER; ++i)
        {
          tmp_obj.set_int(i);
          row.add_obj(tmp_obj);
        }
        EXPECT_EQ(OB_MAX_COLUMN_NUMBER, row.get_obj_count());

        for (int i = 0; i < OB_MAX_COLUMN_NUMBER; ++i)
        {
          obj = row.get_obj(i);
          EXPECT_TRUE(NULL != obj);
          int64_t val = 0;
          obj->get_int(val);
          EXPECT_EQ(i, val);
        }

        buf_size = row.get_serialize_size();
        char serialize_buf[buf_size];

        row.serialize(serialize_buf, buf_size, pos);
        pos = 0;
        deserialized_row.set_obj_count(OB_MAX_COLUMN_NUMBER);
        EXPECT_EQ(OB_MAX_COLUMN_NUMBER, deserialized_row.get_obj_count());
        deserialized_row.deserialize(serialize_buf, buf_size, pos);

        row_key = deserialized_row.get_row_key();
        EXPECT_EQ(tmp_key.key_len(), row_key.length());
        EXPECT_TRUE(key == row_key);
        EXPECT_EQ(OB_INVALID_ID, deserialized_row.get_table_id());
        EXPECT_EQ(OB_INVALID_ID, deserialized_row.get_column_group_id());

        for (int i = 0; i < OB_MAX_COLUMN_NUMBER; ++i)
        {
          obj = deserialized_row.get_obj(i);
          EXPECT_TRUE(NULL != obj);
          EXPECT_EQ(ObIntType, obj->get_type());
          int64_t val = 0;
          obj->get_int(val);
          EXPECT_EQ(i, val);
        }
      }
    }//end namespace sstable
  }//end namespace tests
}//end namespace oceanbase

int main(int argc, char** argv)
{
  ob_init_memory_pool();
  TBSYS_LOGGER.setLogLevel("ERROR");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
