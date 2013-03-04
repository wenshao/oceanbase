/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_tablet_scan_test.cpp
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@taobao.com>
 *
 */


#include "common/utility.h"
#include "sql/ob_tablet_scan.h"
#include "common/ob_malloc.h"
#include "common/ob_schema.h"
#include "ob_operator_factory_impl.h"
#include "ob_fake_ups_rpc_stub2.h"
#include <gtest/gtest.h>
#include "ob_file_table.h"

using namespace oceanbase;
using namespace common;
using namespace sql;
using namespace test;

#define TABLE_ID 1001
#define OK(value) ASSERT_EQ(OB_SUCCESS, (value))

int int64_cmp(const void *a1, const void *a2)
{
  int64_t &b1 = (*(int64_t *)a1);
  int64_t &b2 = (*(int64_t *)a2);
  if(b1 > b2)
  {
    return 1;
  }
  else if(b1 < b2)
  {
    return -1;
  }
  else
  {
    return 0;
  }
}

namespace oceanbase
{
  namespace sql
  {
    namespace test
    {
      class ObTabletScanImpl : public ObTabletScan
      {
        public:
          int get_join_cond(ObTabletJoin::TableJoinInfo &tablet_join_info);
      };

      int ObTabletScanImpl::get_join_cond(ObTabletJoin::TableJoinInfo &tablet_join_info)
      {
        int ret = OB_SUCCESS;
        ObTabletJoin::JoinInfo join_cond;
        join_cond.right_table_id_ = 1002;
        join_cond.left_column_id_ = 3;
        join_cond.right_column_id_ = 2;

        if(OB_SUCCESS != (ret = tablet_join_info.join_condition_.push_back(join_cond)))
        {
          TBSYS_LOG(WARN, "add join condition fail:ret[%d]", ret);
        }
        return ret;
      }

      class ObTabletScanTest: public ::testing::Test
      {
        public:
          ObTabletScanTest();
          virtual ~ObTabletScanTest();
          virtual void SetUp();
          virtual void TearDown();
        private:
          // disallow copy
          ObTabletScanTest(const ObTabletScanTest &other);
          ObTabletScanTest& operator=(const ObTabletScanTest &other);
        protected:
          // data members
      };

      ObTabletScanTest::ObTabletScanTest()
      {
      }

      ObTabletScanTest::~ObTabletScanTest()
      {
      }

      void ObTabletScanTest::SetUp()
      {
        system("python gen_tablet_scan_test_data.py");
      }

      void ObTabletScanTest::TearDown()
      {
      }

      TEST_F(ObTabletScanTest, create_plan2)
      {
        ObTabletScanImpl tablet_scan;

        ObOperatorFactoryImpl op_factory;
        tablet_scan.set_operator_factory(&op_factory);

        ObFakeUpsRpcStub2 rpc_stub;
        rpc_stub.set_ups_scan("./tablet_scan_test_data/ups_table1.ini");
        rpc_stub.set_ups_multi_get("./tablet_scan_test_data/ups_table2.ini");

        tablet_scan.set_ups_rpc_stub(&rpc_stub);

        ObSchemaManagerV2 schema_mgr;
        tbsys::CConfig cfg;
        schema_mgr.parse_from_file("./tablet_scan_test_data/schema.ini", cfg);

        tablet_scan.set_schema_manager(&schema_mgr);
        tablet_scan.set_join_batch_count(3);

        ObSqlExpression expr;
        expr.set_tid_cid(TABLE_ID, 4);
        ExprItem item_a;
        item_a.type_ = T_REF_COLUMN;
        item_a.value_.cell_.tid = TABLE_ID;
        item_a.value_.cell_.cid = 4;
        expr.add_expr_item(item_a);
        expr.add_expr_item_end();

        OK(tablet_scan.add_output_column(expr));

        int64_t start = 100;
        int64_t end = 1000;
        char buf1[100];
        char buf2[100];
        sprintf(buf1, "rowkey_%05ld", start);
        sprintf(buf2, "rowkey_%05ld", end);

        ObRange range;
        range.table_id_ = TABLE_ID;
        range.start_key_.assign_ptr(buf1, strlen(buf1));
        range.end_key_.assign_ptr(buf2, strlen(buf2));

        tablet_scan.set_range(range);

        OK(tablet_scan.open());

        ASSERT_TRUE(NULL == tablet_scan.op_tablet_join_);

        ObScanParam *scan_param = &(tablet_scan.op_ups_scan_->cur_scan_param_);
        
        int id_size = 1;
        ASSERT_EQ(id_size, scan_param->get_column_id_size());
        
        uint64_t id_array[id_size];
        for(int i=0;i<id_size;i++)
        {
          id_array[i] = scan_param->get_column_id()[i];
        }

        qsort(id_array, id_size, sizeof(uint64_t), int64_cmp);

        uint64_t result[] = {4};
        for(int i=0;i<id_size;i++)
        {
          ASSERT_EQ(id_array[i], result[i]);
        }
      }

      TEST_F(ObTabletScanTest, create_plan)
      {
        ObTabletScanImpl tablet_scan;

        ObOperatorFactoryImpl op_factory;
        tablet_scan.set_operator_factory(&op_factory);

        ObFakeUpsRpcStub2 rpc_stub;
        rpc_stub.set_ups_scan("./tablet_scan_test_data/ups_table1.ini");
        rpc_stub.set_ups_multi_get("./tablet_scan_test_data/ups_table2.ini");
        tablet_scan.set_ups_rpc_stub(&rpc_stub);

        ObSchemaManagerV2 schema_mgr;
        tbsys::CConfig cfg;
        schema_mgr.parse_from_file("./tablet_scan_test_data/schema.ini", cfg);

        tablet_scan.set_schema_manager(&schema_mgr);
        tablet_scan.set_join_batch_count(3);

        ObSqlExpression expr1, expr2;
        expr1.set_tid_cid(TABLE_ID, 4);

        ExprItem item_a;
        item_a.type_ = T_REF_COLUMN;
        item_a.value_.cell_.tid = TABLE_ID;
        item_a.value_.cell_.cid = 4;
        expr1.add_expr_item(item_a);
        expr1.add_expr_item_end();

        OK(tablet_scan.add_output_column(expr1));
        expr2.set_tid_cid(TABLE_ID, 5);

        ExprItem item_b;
        item_b.type_ = T_REF_COLUMN;
        item_b.value_.cell_.tid = TABLE_ID;
        item_b.value_.cell_.cid = 5;
        expr2.add_expr_item(item_b);
        expr2.add_expr_item_end();

        OK(tablet_scan.add_output_column(expr2));

        int64_t start = 100;
        int64_t end = 1000;
        char buf1[100];
        char buf2[100];
        sprintf(buf1, "rowkey_%05ld", start);
        sprintf(buf2, "rowkey_%05ld", end);

        ObRange range;
        range.table_id_ = TABLE_ID;
        range.start_key_.assign_ptr(buf1, strlen(buf1));
        range.end_key_.assign_ptr(buf2, strlen(buf2));

        tablet_scan.set_range(range);

        OK(tablet_scan.open());

        ASSERT_TRUE(NULL != tablet_scan.op_tablet_join_);
        ObTabletJoin::TableJoinInfo &table_join_info = tablet_scan.op_tablet_join_->table_join_info_;

        ASSERT_EQ(1, table_join_info.join_column_.count());
        ASSERT_EQ(1002, table_join_info.join_column_.at(0).right_table_id_);
        ASSERT_EQ(5, table_join_info.join_column_.at(0).left_column_id_);
        ASSERT_EQ(3, table_join_info.join_column_.at(0).right_column_id_);

        ASSERT_EQ(1, table_join_info.join_condition_.count());
        ASSERT_EQ(1002, table_join_info.join_condition_.at(0).right_table_id_);
        ASSERT_EQ(3, table_join_info.join_condition_.at(0).left_column_id_);
        ASSERT_EQ(2, table_join_info.join_condition_.at(0).right_column_id_);

        ObScanParam *scan_param = &(tablet_scan.op_ups_scan_->cur_scan_param_);
        
        int id_size = 3;
        ASSERT_EQ(id_size, scan_param->get_column_id_size());
        
        uint64_t id_array[id_size];
        for(int i=0;i<id_size;i++)
        {
          id_array[i] = scan_param->get_column_id()[i];
        }

        qsort(id_array, id_size, sizeof(uint64_t), int64_cmp);

        uint64_t result[] = {3 , 4, 5};
        for(int i=0;i<id_size;i++)
        {
          ASSERT_EQ(id_array[i], result[i]);
        }
      }

      TEST_F(ObTabletScanTest, get_next_row)
      {
        ObTabletScanImpl tablet_scan;

        ObOperatorFactoryImpl op_factory;
        tablet_scan.set_operator_factory(&op_factory);

        ObFakeUpsRpcStub2 rpc_stub;
        rpc_stub.set_ups_scan("./tablet_scan_test_data/ups_table1.ini");
        rpc_stub.set_ups_multi_get("./tablet_scan_test_data/ups_table2.ini");
        tablet_scan.set_ups_rpc_stub(&rpc_stub);

        ObSchemaManagerV2 schema_mgr;
        tbsys::CConfig cfg;
        schema_mgr.parse_from_file("./tablet_scan_test_data/schema.ini", cfg);

        tablet_scan.set_schema_manager(&schema_mgr);
        tablet_scan.set_join_batch_count(3);

        ObSqlExpression expr1, expr2, expr3;

        expr1.set_tid_cid(TABLE_ID, 4);
        ExprItem item_a;
        item_a.type_ = T_REF_COLUMN;
        item_a.value_.cell_.tid = TABLE_ID;
        item_a.value_.cell_.cid = 4;
        expr1.add_expr_item(item_a);
        expr1.add_expr_item_end();
        OK(tablet_scan.add_output_column(expr1));

        expr2.set_tid_cid(TABLE_ID, 5);
        ExprItem item_b;
        item_b.type_ = T_REF_COLUMN;
        item_b.value_.cell_.tid = TABLE_ID;
        item_b.value_.cell_.cid = 5;
        expr2.add_expr_item(item_b);
        expr2.add_expr_item_end();
        OK(tablet_scan.add_output_column(expr2));


        int64_t start = 100;
        int64_t end = 1000;
        char buf1[100];
        char buf2[100];
        sprintf(buf1, "rowkey_%05ld", start);
        sprintf(buf2, "rowkey_%05ld", end);

        ObRange range;
        range.table_id_ = TABLE_ID;
        range.start_key_.assign_ptr(buf1, strlen(buf1));
        range.end_key_.assign_ptr(buf2, strlen(buf2));

        tablet_scan.set_range(range);

        ObFileTable result("./tablet_scan_test_data/result.ini");
        
        int err = OB_SUCCESS;
        int64_t count = 0;
        const ObRow *row = NULL;
        const ObRow *result_row = NULL;
        const ObObj *value = NULL;
        const ObObj *result_value = NULL;
        uint64_t table_id = 0;
        uint64_t column_id = 0;

        OK(tablet_scan.open());
        OK(result.open());

        while(OB_SUCCESS == err)
        {
          err = tablet_scan.get_next_row(row);
          if(OB_SUCCESS != err)
          {
            ASSERT_TRUE(OB_ITER_END == err);
            break;
          }

          OK(result.get_next_row(result_row));
          count ++;

          ASSERT_TRUE(row->get_column_num() > 0);

          for(int64_t i=0;i<row->get_column_num();i++)
          {
            OK(row->raw_get_cell(i, value, table_id, column_id));
            OK(result_row->get_cell(table_id, column_id, result_value));

            if( *value != *result_value )
            {
              printf("row:[%ld], column[%ld]===========\n", count, i);
              printf("row: %s\n", print_obj(*value));
              printf("result: %s\n", print_obj(*result_value));
            }

            ASSERT_TRUE((*value) == (*result_value));
          }
        }

        ASSERT_TRUE(count > 0);

        OK(result.close());
        OK(tablet_scan.close());

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


