/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_tablet_join_test.cpp
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@taobao.com>
 *
 */

#include "common/ob_malloc.h"
#include <gtest/gtest.h>
#include "sql/ob_tablet_join.h"
#include "ob_file_table.h"
#include "common/utility.h"
#include "ob_fake_ups_multi_get.h"
#include "common/ob_ups_row_util.h"

using namespace oceanbase;
using namespace common;
using namespace sql;
using namespace test;

#define OK(value) ASSERT_EQ(OB_SUCCESS, (value))

namespace oceanbase
{
  namespace sql
  {
    namespace test
    {
      class ObTabletJoinTest: public ::testing::Test
      {
        public:
          ObTabletJoinTest();
          virtual ~ObTabletJoinTest();
          virtual void SetUp();
          virtual void TearDown();
          int read_join_info(ObTabletJoin::TableJoinInfo &tablet_join_info, const char *file_name);
          int get_rowkey(const ObRow &row, ObString &rowkey);

        private:
          // disallow copy
          ObTabletJoinTest(const ObTabletJoinTest &other);
          ObTabletJoinTest& operator=(const ObTabletJoinTest &other);
        protected:
          // data members
          ObTabletJoin::TableJoinInfo tablet_join_info_;
          static const uint64_t LEFT_TABLE_ID = 1000;
          static const uint64_t RIGHT_TABLE_ID = 1001;
      };

      ObTabletJoinTest::ObTabletJoinTest()
      {
      }

      ObTabletJoinTest::~ObTabletJoinTest()
      {
      }

      int ObTabletJoinTest::get_rowkey(const ObRow &row, ObString &rowkey)
      {
        int ret = OB_SUCCESS;
        const ObObj *cell = NULL;
        uint64_t table_id = OB_INVALID_ID;
        uint64_t column_id = OB_INVALID_ID;

        if(OB_SUCCESS != (ret = row.raw_get_cell(0, cell, table_id, column_id)))
        {
          TBSYS_LOG(WARN, "raw get cell fail:ret[%d]", ret);
        }
        else if(OB_SUCCESS != (ret = cell->get_varchar(rowkey)))
        {
          TBSYS_LOG(WARN, "get varchar:ret[%d]", ret);
        }

        return ret;
      }

      void ObTabletJoinTest::SetUp()
      {
        tablet_join_info_.left_table_id_ = LEFT_TABLE_ID;

        ObTabletJoin::JoinInfo join_info;
        join_info.right_table_id_ = RIGHT_TABLE_ID;

        join_info.left_column_id_ = 1;
        join_info.right_column_id_ = 1;
        OK(tablet_join_info_.join_condition_.push_back(join_info));

        join_info.left_column_id_ = 4;
        join_info.right_column_id_ = 2;
        OK(tablet_join_info_.join_column_.push_back(join_info));

        join_info.left_column_id_ = 5;
        join_info.right_column_id_ = 3;
        OK(tablet_join_info_.join_column_.push_back(join_info));

        join_info.left_column_id_ = 6;
        join_info.right_column_id_ = 4;
        OK(tablet_join_info_.join_column_.push_back(join_info));
      }

      void ObTabletJoinTest::TearDown()
      {
      }

      int ObTabletJoinTest::read_join_info(ObTabletJoin::TableJoinInfo &tablet_join_info, const char *file_name)
      {
        int ret = OB_SUCCESS;
        FILE *fp = NULL; 
        char buf[1024];
        char *tokens[100];
        int32_t count = 0;
        ObTabletJoin::JoinInfo join_info;

        join_info.right_table_id_ = RIGHT_TABLE_ID;
        if(NULL == file_name)
        {
          ret = OB_ERROR;
          TBSYS_LOG(WARN, "file_name is null");
        }
        else if(NULL == (fp = fopen(file_name, "r")))
        {
          ret = OB_ERROR;
          TBSYS_LOG(WARN, "open file[%s] fail", file_name);
        }
        else if(NULL == fgets(buf, 1024, fp))
        {
          ret = OB_ERROR;
          TBSYS_LOG(WARN, "fgets fail");
        }
        else
        {
          int num = atoi(buf);
          for(int i=0;i<num;i++)
          {
            if(NULL == fgets(buf, 1024, fp))
            {
              ret = OB_ERROR;
              TBSYS_LOG(WARN, "fgets fail");
            }
            else
            {
              split(buf, " ", tokens, count);
              if(2 != count)
              {
                ret = OB_ERROR;
                TBSYS_LOG(WARN, "count[%d]", count);
              }
              else
              {
                join_info.left_column_id_ = atoi(tokens[0]);
                join_info.right_column_id_ = atoi(tokens[1]);
                if(OB_SUCCESS != (ret = tablet_join_info.join_column_.push_back(join_info)))
                {
                  TBSYS_LOG(WARN, "add join column fail:ret[%d]", ret);
                }
              }
            }
          }
        }

        if(NULL != fp)
        {
          fclose(fp);
        }
        return ret;
      }


      TEST_F(ObTabletJoinTest, big_data_test)
      {
        system("python gen_test_data.py join");
        ObTabletJoin tablet_join;

        ObTabletJoin::TableJoinInfo tablet_join_info;
        tablet_join_info.left_table_id_ = LEFT_TABLE_ID;
        ObTabletJoin::JoinInfo join_info;
        join_info.right_table_id_ = RIGHT_TABLE_ID;

        join_info.left_column_id_ = 1;
        join_info.right_column_id_ = 1;
        OK(tablet_join_info.join_condition_.push_back(join_info));

        OK(read_join_info(tablet_join_info, "tablet_join_test_data/join_info.ini"));

        tablet_join.set_table_join_info(tablet_join_info);

        ObFakeUpsMultiGet fake_ups_multi_get("tablet_join_test_data/big_ups_scan2.ini");
        ObFileTable file_table("tablet_join_test_data/big_sstable2.ini");
        ObFileTable result("tablet_join_test_data/big_result2.ini");

        tablet_join.set_child(0, file_table);
        tablet_join.set_batch_count(3);
        tablet_join.set_ups_multi_get(&fake_ups_multi_get);

        
        OK(file_table.open());
        OK(file_table.close());

        for(int i=0;i<file_table.column_count_;i++)
        {
          tablet_join.add_column_id(file_table.column_ids_[i]);
        }

        ObGetParam get_param(true);

        const ObRow *row = NULL;
        const ObRow *result_row = NULL;
        const ObObj *value = NULL;
        const ObObj *result_value = NULL;
        uint64_t table_id = OB_INVALID_ID;
        uint64_t column_id = OB_INVALID_ID;

        OK(result.open());
        OK(tablet_join.open());

        int err = OB_SUCCESS;
        int64_t count = 0;
        
        while(OB_SUCCESS == (err = tablet_join.get_next_row(row)))
        {
          OK(result.get_next_row(result_row));
          count ++;

          for(int64_t i=0;i<row->get_column_num();i++)
          {
            OK(row->raw_get_cell(i, value, table_id, column_id));
            OK(result_row->get_cell(table_id, column_id, result_value));

            if( *value != *result_value )
            {
              printf("row:[%ld], column[%ld]===========\n", count, i);

              ObString rowkey;
              get_rowkey(*row, rowkey);
              printf("row rowkey: %.*s\n", rowkey.length(), rowkey.ptr());
              printf("row: %s\n", print_obj(*value));
              get_rowkey(*result_row, rowkey);
              printf("result rowkey: %.*s\n", rowkey.length(), rowkey.ptr());
              printf("result: %s\n", print_obj(*result_value));
            }

            ASSERT_TRUE((*value) == (*result_value));
          }
        }
        ASSERT_TRUE(OB_SUCCESS == err || OB_ITER_END == err);

        OK(result.close());
        OK(tablet_join.close());
      }

      TEST_F(ObTabletJoinTest, get_next_row)
      {
        ObTabletJoin tablet_join;
        tablet_join.set_table_join_info(tablet_join_info_);

        ObFakeUpsMultiGet fake_ups_multi_get("tablet_join_test_data/fetch_ups_row_ups_row.ini");

        ObFileTable file_table("tablet_join_test_data/fetch_ups_row_fused_row.ini");
        tablet_join.set_child(0, file_table);
        tablet_join.set_batch_count(3);
        tablet_join.set_ups_multi_get(&fake_ups_multi_get);

        tablet_join.add_column_id(1);
        tablet_join.add_column_id(4);
        tablet_join.add_column_id(5);
        tablet_join.add_column_id(6);

        ObGetParam get_param(true);

        const ObRow *row = NULL;
        const ObRow *result_row = NULL;
        const ObObj *cell = NULL;
        const ObObj *result_cell = NULL;
        uint64_t table_id = OB_INVALID_ID;
        uint64_t column_id = OB_INVALID_ID;

        ObFileTable result("tablet_join_test_data/fetch_ups_row_result.ini");
        OK(result.open());
        OK(tablet_join.open());

        for(int i=0;i<7;i++)
        {
          OK(tablet_join.get_next_row(row));
          OK(result.get_next_row(result_row));

          for(int j=1;j<4;j++)
          {
            OK(row->raw_get_cell(j, cell, table_id, column_id));
            OK(result_row->raw_get_cell(j, result_cell, table_id, column_id));
            ASSERT_TRUE((*cell) == (*result_cell));
          }
        }
        OK(result.close());
        OK(tablet_join.close());
      }

      TEST_F(ObTabletJoinTest, fetch_ups_row)
      {
        ObTabletJoin tablet_join;
        tablet_join.set_table_join_info(tablet_join_info_);

        ObFakeUpsMultiGet fake_ups_multi_get("tablet_join_test_data/fetch_ups_row_ups_row.ini");

        ObFileTable file_table("tablet_join_test_data/fetch_ups_row_fused_row.ini");
        tablet_join.set_child(0, file_table);
        tablet_join.set_batch_count(3);
        tablet_join.set_ups_multi_get(&fake_ups_multi_get);

        ObGetParam get_param(true);

        OK(tablet_join.open());

        OK(tablet_join.fetch_fused_row(&get_param));
        OK(tablet_join.fetch_ups_row(&get_param));

        uint64_t right_table_id = get_param[0]->table_id_;

        char rowkey_buf[100];
        ObString rowkey;
        ObString value;
        ObUpsRow ups_row;
        const ObObj *cell = NULL;
        uint64_t table_id = OB_INVALID_ID;
        uint64_t column_id = OB_INVALID_ID;
        int64_t int_value = 0;

        ups_row.set_row_desc(tablet_join.ups_row_desc_);

        for(int i=0;i<3;i++)
        {
          sprintf(rowkey_buf, "chen%d", i+1);
          printf("rowkey:%s\n", rowkey_buf);
          rowkey.assign_ptr(rowkey_buf, strlen(rowkey_buf));
          OK(tablet_join.ups_row_cache_.get(rowkey, value));
          OK(ObUpsRowUtil::convert(right_table_id, value, ups_row));
          ASSERT_EQ(3, ups_row.get_column_num());

          for(int j=0;j<3;j++)
          {
            OK(ups_row.raw_get_cell(j, cell, table_id, column_id));
            cell->get_int(int_value);
            ASSERT_EQ(2, int_value);
          }
        }

        OK(tablet_join.close());
      }

      /*
      TEST_F(ObTabletJoinTest, fetch_fused_row)
      {
        ObTabletJoin tablet_join;
        tablet_join.set_table_join_info(tablet_join_info_);

        ObGetParam get_param(true);

        ObRowDesc row_desc;
        row_desc.add_column_desc(LEFT_TABLE_ID, 1);
        row_desc.add_column_desc(LEFT_TABLE_ID, 2);
        row_desc.add_column_desc(LEFT_TABLE_ID, 3);
        row_desc.add_column_desc(LEFT_TABLE_ID, 4);

        ObFileTable file_table("tablet_join_test_data/fetch_fused_row.ini");
        tablet_join.set_child(0, file_table);
        tablet_join.set_batch_count(3);

        OK(tablet_join.open());
        OK(tablet_join.fetch_fused_row(&get_param));

        ObRow row;
        row.set_row_desc(row_desc);

        int err = OB_SUCCESS;
        char str_buf[100];
        const ObObj *cell = NULL;
        uint64_t table_id = OB_INVALID_ID;
        uint64_t column_id = OB_INVALID_ID;
        ObString rowkey;

        for(int64_t k=0;k<3;k++)
        {
          err = tablet_join.fused_row_store_.next_row();
          OK(err);
          OK(tablet_join.fused_row_store_.get_row(row));

          ASSERT_EQ(4, row.get_column_num());
          for(int64_t i=0;i<row.get_column_num();i++)
          {
            OK(row.raw_get_cell(i, cell, table_id, column_id));
            if(0 == i)
            {
              sprintf(str_buf, "chen%ld", k+1);
              cell->get_varchar(rowkey);
              ASSERT_EQ(0, strncmp(rowkey.ptr(), str_buf, rowkey.length()));
            }
          }
        }

        ASSERT_EQ(9, get_param.get_cell_size());

        for(int i=0;i<9;i++)
        {
          sprintf(str_buf, "chen%d", 1);
          printf("rowkey:%.*s\n", get_param[i]->row_key_.length(), get_param[i]->row_key_.ptr());
        }


        ASSERT_EQ(0, strncmp(get_param[0]->row_key_.ptr(), str_buf, get_param[0]->row_key_.length()));

        err = tablet_join.fused_row_store_.next_row();
        ASSERT_EQ(OB_ITER_END, err);

        OK(tablet_join.close());
      }
      */

      TEST_F(ObTabletJoinTest, get_right_table_rowkey)
      {
        ObRowDesc row_desc;
        row_desc.add_column_desc(LEFT_TABLE_ID, 1);

        const char *rowkey_str = "oceanbase";
        ObString row_key;
        row_key.assign_ptr(const_cast<char *>(rowkey_str), strlen(rowkey_str));

        ObObj value;
        value.set_varchar(row_key);

        ObRow row;
        row.set_row_desc(row_desc);
        row.raw_set_cell(0, value);

        ObString rowkey2;

        ObTabletJoin tablet_join;
        tablet_join.set_table_join_info(tablet_join_info_);

        uint64_t right_table_id = OB_INVALID_ID;

        OK(tablet_join.get_right_table_rowkey(row, right_table_id, rowkey2));

        uint64_t right_table_id2 = RIGHT_TABLE_ID;
        ASSERT_EQ(right_table_id2, right_table_id);
        ASSERT_TRUE(row_key == rowkey2);
      }

      TEST_F(ObTabletJoinTest, gen_ups_row_desc)
      {
        ObTabletJoin tablet_join;
        tablet_join.set_table_join_info(tablet_join_info_);

        tablet_join.gen_ups_row_desc();

        ASSERT_EQ(0, tablet_join.ups_row_desc_.get_idx(RIGHT_TABLE_ID, 2));
        ASSERT_EQ(1, tablet_join.ups_row_desc_.get_idx(RIGHT_TABLE_ID, 3));
        ASSERT_EQ(2, tablet_join.ups_row_desc_.get_idx(RIGHT_TABLE_ID, 4));

      }

      TEST_F(ObTabletJoinTest, compose_get_param)
      {
        ObTabletJoin tablet_join;
        tablet_join.set_table_join_info(tablet_join_info_);

        const char *rowkey_str = "oceanbase";

        ObString row_key;
        row_key.assign_ptr(const_cast<char *>(rowkey_str), strlen(rowkey_str));

        ObGetParam get_param;
        tablet_join.compose_get_param(RIGHT_TABLE_ID, row_key, get_param);
        ASSERT_EQ(3, get_param.get_cell_size());

        tablet_join.compose_get_param(RIGHT_TABLE_ID, row_key, get_param);
        ASSERT_EQ(6, get_param.get_cell_size());
      }
    }
  }
}


int main(int argc, char **argv)
{
  ob_init_memory_pool();
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}


