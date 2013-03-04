/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: test_scan_large_scale.cpp,v 0.1 2010/08/27 09:51:44 chuanhui Exp $
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
#include "common/utility.h"
#include "common/page_arena.h"
#include "test_helper.h"
#include "sstable/ob_column_group_scanner.h"
#include "sstable/ob_sstable_reader.h"

using namespace std;
using namespace oceanbase::common;
using namespace oceanbase::sstable;

namespace oceanbase
{
  namespace tests
  {
    namespace sstable
    {

      static const ObString table_name;
      static const int64_t sstable_file_id = 1234;
      static const int64_t sstable_file_offset = 0;
      static const ObSSTableId sstable_id(sstable_file_id);
      static ModulePageAllocator mod(0);
      static ModuleArena allocator(ModuleArena::DEFAULT_PAGE_SIZE, mod);
      static ObSSTableReader sstable_reader_(allocator, GFactory::get_instance().get_fi_cache());
      static SSTableBuilder sstable_builder_;

      class TestObColumnGroupScanner : public ::testing::Test
      {
        public:
          TestObColumnGroupScanner()
            : scanner_ ()
        {
        }
        protected:
          int test_single_query_helper(const ObRange& range, 
              const int32_t row_start_index , const int32_t row_expect_count, 
              const int32_t column_count );
          int test_single_query_helper(
              const ObBorderFlag& border_flag, 
              const int32_t row_start_index , const int32_t row_end_index, 
              const int32_t row_expect_count, const int32_t column_count );

        public:
          static void SetUpTestCase()
          {
            TBSYS_LOGGER.setLogLevel("ERROR");
            int err = sstable_builder_.generate_sstable_file(write_sstable, sstable_id);
            EXPECT_EQ(0, err);
          }

          static void TearDownTestCase()
          {
          }

          virtual void SetUp()
          {
            ModuleArena * arena = GET_TSI_MULT(ModuleArena, TSI_SSTABLE_MODULE_ARENA_1);
            arena->set_page_size(1024*1024*2);
            arena->reuse();
            printf("memory usage SetUp: total=%ld,pages=%ld,used=%ld\n",
                arena->total(), arena->pages(), arena->used());
            scanner_.initialize(
                &GFactory::get_instance().get_block_index_cache(),
                &GFactory::get_instance().get_block_cache()
              );
            sstable_reader_.reset();
            int err = sstable_reader_.open(sstable_id);
            EXPECT_EQ(0, err);
            EXPECT_TRUE(sstable_reader_.is_opened());
          }

          virtual void TearDown()
          {
            sstable_reader_.reset();
            ModuleArena * arena = GET_TSI_MULT(ModuleArena, TSI_SSTABLE_MODULE_ARENA_1);
            printf("memory usage TearDown: total=%ld,pages=%ld,used=%ld\n",
                arena->total(), arena->pages(), arena->used());
          }
          ObColumnGroupScanner scanner_;
      };

      int TestObColumnGroupScanner::test_single_query_helper(const ObRange& range, 
          const int32_t row_start_index , const int32_t row_expect_count, 
          const int32_t column_count )
      {
        ObScanParam scan_param;

        scan_param.set_scan_direction(ObScanParam::FORWARD);
        scan_param.set(SSTableBuilder::table_id, table_name, range);
        ObCellInfo** const cell_infos = sstable_builder_.get_cell_infos();
        for (int64_t i = 0; i < column_count; ++i)
        {
          scan_param.add_column(cell_infos[0][i].column_id_);
        }

        ObSSTableScanParam sstable_scan_param;
        sstable_scan_param.assign(scan_param);
        int err = scanner_.set_scan_param(0, 0, 1, &sstable_scan_param, &sstable_reader_);
        if (err) return err;

        // check result
        int64_t row_index = 0;
        int64_t col_index = 0;
        int64_t total = 0;
        ObCellInfo* cur_cell = NULL;
        bool is_row_changed = false;
        bool first_row = true;
        while (OB_SUCCESS == scanner_.next_cell())
        {
          err = scanner_.get_cell(&cur_cell, &is_row_changed);
          EXPECT_EQ(0, err);
          EXPECT_NE((ObCellInfo*) NULL, cur_cell); 
          if (is_row_changed)
          {
            if (col_index > 0) EXPECT_EQ(column_count, col_index+1);
            // reset column
            col_index = 0;
            if (!first_row) ++row_index;
            if (first_row) first_row = false;
          }
          else
          {
            ++col_index;
          }
          check_cell(cell_infos[row_index + row_start_index][col_index], *cur_cell);
          ++total;
        }

        int row_count = 0;
        if (first_row) row_count = 0;
        else row_count = static_cast<int32_t>(row_index + 1);

        EXPECT_EQ(row_count , row_expect_count);
        EXPECT_EQ(row_count * column_count, total);
        return 0;
      }

      int TestObColumnGroupScanner::test_single_query_helper(
          const ObBorderFlag& border_flag, 
          const int32_t row_start_index , const int32_t row_end_index, 
          const int32_t row_expect_count, const int32_t column_count )
      {
        int err = OB_SUCCESS;
        ObCellInfo** const cell_infos = sstable_builder_.get_cell_infos();
        ObRange range;
        range.table_id_ = SSTableBuilder::table_id;

        range.start_key_ = cell_infos[row_start_index][0].row_key_;//start key
        range.end_key_ = cell_infos[row_end_index][0].row_key_; //end key
        range.border_flag_ = border_flag;

        err = test_single_query_helper(range, row_start_index, row_expect_count, column_count);
        return err;
      }

      TEST_F(TestObColumnGroupScanner, test_query_case_1)
      {
        int err = OB_SUCCESS;

        ObBorderFlag border_flag;
        border_flag.set_inclusive_start(); //inclusive end
        border_flag.set_inclusive_end(); //inclusive end

        err = test_single_query_helper(border_flag, 0, 0, 1, 2);
        EXPECT_EQ(0, err);
      }

      TEST_F(TestObColumnGroupScanner, test_query_case_2)
      {
        int err = OB_SUCCESS;

        ObBorderFlag border_flag;

        err = test_single_query_helper(border_flag, 0, 0, 0, 2);
        EXPECT_EQ(OB_INVALID_ARGUMENT, err);
      }

      TEST_F(TestObColumnGroupScanner, test_query_case_3)
      {
        int err = OB_SUCCESS;

        ObBorderFlag border_flag;
        border_flag.set_inclusive_start(); //inclusive end

        err = test_single_query_helper(border_flag, 0, 0, 0, 2);
        EXPECT_EQ(OB_INVALID_ARGUMENT, err);
      }

      TEST_F(TestObColumnGroupScanner, test_query_case_4)
      {
        int err = OB_SUCCESS;


        ObBorderFlag border_flag;
        border_flag.set_inclusive_end(); //inclusive end
        err = test_single_query_helper(border_flag, 0, 0, 0, 2);
        EXPECT_EQ(OB_INVALID_ARGUMENT, err);
      }

      TEST_F(TestObColumnGroupScanner, test_query_case_5)
      {
        int err = OB_SUCCESS;

        ObCellInfo** const cell_infos = sstable_builder_.get_cell_infos();
        ObRange range;
        range.table_id_ = SSTableBuilder::table_id;

        ///range.start_key_ = cell_infos[row_start_index][0].row_key_;//do not set start key
        ObBorderFlag border_flag;
        border_flag.set_inclusive_end(); //inclusive end
        border_flag.set_min_value(); //min
        range.end_key_ = cell_infos[0][0].row_key_; //end key
        range.border_flag_ = border_flag;

        //err = test_single_query_helper(range, row_start_index, row_expect_count, column_count);
        err = test_single_query_helper(range, 0, 1, 3);
        EXPECT_EQ(0, err);
      }

      TEST_F(TestObColumnGroupScanner, test_query_case_6)
      {
        int err = OB_SUCCESS;

        ObCellInfo** const cell_infos = sstable_builder_.get_cell_infos();
        ObRange range;
        range.table_id_ = SSTableBuilder::table_id;

        ///range.start_key_ = cell_infos[row_start_index][0].row_key_;//do not set start key
        ObBorderFlag border_flag;
        border_flag.set_inclusive_end(); //inclusive end
        border_flag.set_min_value(); //min
        range.end_key_ = cell_infos[10][0].row_key_; //end key
        range.border_flag_ = border_flag;

        //err = test_single_query_helper(range, row_start_index, row_expect_count, column_count);
        err = test_single_query_helper(range, 0, 11, 3);
        EXPECT_EQ(0, err);
      }

      TEST_F(TestObColumnGroupScanner, test_query_case_7)
      {
        int err = OB_SUCCESS;

        ObCellInfo** const cell_infos = sstable_builder_.get_cell_infos();
        ObRange range;
        range.table_id_ = SSTableBuilder::table_id;

        ///range.start_key_ = cell_infos[row_start_index][0].row_key_;//do not set start key
        ObBorderFlag border_flag;
        border_flag.set_inclusive_start(); //inclusive end
        border_flag.set_max_value();//max
        range.start_key_ = cell_infos[10][0].row_key_; //end key
        range.border_flag_ = border_flag;

        //err = test_single_query_helper(range, row_start_index, row_expect_count, column_count);
        err = test_single_query_helper(range, 10, SSTableBuilder::ROW_NUM -10, 3);
        EXPECT_EQ(0, err);
      }

      TEST_F(TestObColumnGroupScanner, test_query_case_8)
      {
        int err = OB_SUCCESS;

        ObCellInfo** const cell_infos = sstable_builder_.get_cell_infos();
        ObRange range;
        range.table_id_ = SSTableBuilder::table_id;

        ///range.start_key_ = cell_infos[row_start_index][0].row_key_;//do not set start key
        ObBorderFlag border_flag;
        border_flag.set_inclusive_start(); //inclusive end
        border_flag.set_max_value();//max
        range.start_key_ = cell_infos[10][0].row_key_; //end key
        range.end_key_ = cell_infos[20][0].row_key_; //end key
        range.border_flag_ = border_flag;

        //err = test_single_query_helper(range, row_start_index, row_expect_count, column_count);
        err = test_single_query_helper(range, 10, SSTableBuilder::ROW_NUM -10, 3);
        EXPECT_EQ(0, err);
      }

      TEST_F(TestObColumnGroupScanner, test_query_case_9)
      {
        int err = OB_SUCCESS;

        ObCellInfo** const cell_infos = sstable_builder_.get_cell_infos();
        ObRange range;
        range.table_id_ = SSTableBuilder::table_id;

        ///range.start_key_ = cell_infos[row_start_index][0].row_key_;//do not set start key
        ObBorderFlag border_flag;
        border_flag.set_min_value(); //min
        border_flag.set_max_value();//max
        range.start_key_ = cell_infos[10][0].row_key_; //end key
        range.end_key_ = cell_infos[20][0].row_key_; //end key
        range.border_flag_ = border_flag;

        //err = test_single_query_helper(range, row_start_index, row_expect_count, column_count);
        err = test_single_query_helper(range, 0, SSTableBuilder::ROW_NUM, 5);
        EXPECT_EQ(0, err);
      }

      TEST_F(TestObColumnGroupScanner, test_query_case_10)
      {
        int err = OB_SUCCESS;

        ObBorderFlag border_flag;
        border_flag.set_inclusive_start(); //inclusive end
        border_flag.set_inclusive_end(); //not inclusive end

        // start_index = 10
        // end_index = 20
        err = test_single_query_helper(border_flag, 10, 20, 11, 2);
        EXPECT_EQ(0, err);
      }

      TEST_F(TestObColumnGroupScanner, test_query_case_11)
      {
        int err = OB_SUCCESS;

        ObCellInfo** const cell_infos = sstable_builder_.get_cell_infos();
        ObRange range;
        range.table_id_ = SSTableBuilder::table_id;

        ///range.start_key_ = cell_infos[row_start_index][0].row_key_;//do not set start key
        ObBorderFlag border_flag;
        border_flag.set_inclusive_end(); //inclusive end
        range.start_key_ = cell_infos[10][0].row_key_; //end key
        range.end_key_ = cell_infos[20][0].row_key_; //end key
        range.border_flag_ = border_flag;

        //err = test_single_query_helper(range, row_start_index, row_expect_count, column_count);
        err = test_single_query_helper(range, 11, 10, SSTableBuilder::COL_NUM);
        EXPECT_EQ(0, err);
      }

      TEST_F(TestObColumnGroupScanner, test_query_case_12)
      {
        int err = OB_SUCCESS;

        ObBorderFlag border_flag;
        border_flag.set_inclusive_start(); //inclusive end

        // start_index = 10
        // end_index = 20
        err = test_single_query_helper(border_flag, 10, 20, 10, 2);
      }

      TEST_F(TestObColumnGroupScanner, test_query_case_13)
      {
        int err = OB_SUCCESS;
        ObCellInfo** const cell_infos = sstable_builder_.get_cell_infos();
        ObRange range;
        range.table_id_ = SSTableBuilder::table_id;

        ///range.start_key_ = cell_infos[row_start_index][0].row_key_;//do not set start key
        ObBorderFlag border_flag;
        range.start_key_ = cell_infos[10][0].row_key_; //end key
        range.end_key_ = cell_infos[20][0].row_key_; //end key
        range.border_flag_ = border_flag;

        //err = test_single_query_helper(range, row_start_index, row_expect_count, column_count);
        err = test_single_query_helper(range, 11, 9, SSTableBuilder::COL_NUM);
        EXPECT_EQ(0, err);

      }



      TEST_F(TestObColumnGroupScanner, test_query_case_14)
      {
        int err = OB_SUCCESS;
        ObCellInfo** const cell_infos = sstable_builder_.get_cell_infos();
        ObRange range;
        range.table_id_ = SSTableBuilder::table_id;

        int row_start_index = SSTableBuilder::ROW_NUM-1;
        ///range.start_key_ = cell_infos[row_start_index][0].row_key_;//do not set start key
        ObBorderFlag border_flag;
        border_flag.set_inclusive_start(); //inclusive end
        border_flag.set_max_value();//max
        range.start_key_ = cell_infos[row_start_index][0].row_key_; //end key
        range.border_flag_ = border_flag;

        //err = test_single_query_helper(range, row_start_index, row_expect_count, column_count);
        err = test_single_query_helper(range, row_start_index, 1, SSTableBuilder::COL_NUM);
        EXPECT_EQ(0, err);

      }

      TEST_F(TestObColumnGroupScanner, test_query_case_15)
      {
        int err = OB_SUCCESS;
        ObCellInfo** const cell_infos = sstable_builder_.get_cell_infos();
        ObRange range;
        range.table_id_ = SSTableBuilder::table_id;

        ///range.start_key_ = cell_infos[row_start_index][0].row_key_;//do not set start key
        ObBorderFlag border_flag;
        border_flag.set_inclusive_start(); //inclusive end
        range.start_key_ = cell_infos[10][0].row_key_; //end key
        range.border_flag_ = border_flag;

        //err = test_single_query_helper(range, row_start_index, row_expect_count, column_count);
        err = test_single_query_helper(range, 10, 0, SSTableBuilder::COL_NUM);
        EXPECT_EQ(OB_INVALID_ARGUMENT, err);

      }



      TEST_F(TestObColumnGroupScanner, test_query_case_16)
      {
        int err = OB_SUCCESS;
        ObCellInfo** const cell_infos = sstable_builder_.get_cell_infos();
        ObRange range;
        range.table_id_ = SSTableBuilder::table_id;

        ///range.start_key_ = cell_infos[row_start_index][0].row_key_;//do not set start key
        ObBorderFlag border_flag;
        range.start_key_ = cell_infos[10][0].row_key_; //end key
        range.border_flag_ = border_flag;

        //err = test_single_query_helper(range, row_start_index, row_expect_count, column_count);
        err = test_single_query_helper(range, 10, 0, SSTableBuilder::COL_NUM);
        EXPECT_EQ(OB_INVALID_ARGUMENT, err);

      }

      TEST_F(TestObColumnGroupScanner, test_query_case_17)
      {
        int err = OB_SUCCESS;
        ObCellInfo** const cell_infos = sstable_builder_.get_cell_infos();
        ObRange range;
        range.table_id_ = SSTableBuilder::table_id;

        ///range.start_key_ = cell_infos[row_start_index][0].row_key_;//do not set start key
        ObBorderFlag border_flag;
        border_flag.set_inclusive_start(); //inclusive end
        char* start_key = (char*)"row_key_0"; // less than start_key in sstable.
        range.start_key_.assign_ptr(start_key, static_cast<int32_t>(strlen(start_key)));
        range.end_key_ = cell_infos[20][0].row_key_; //end key
        range.border_flag_ = border_flag;

        //err = test_single_query_helper(range, row_start_index, row_expect_count, column_count);
        err = test_single_query_helper(range, 0, 20, SSTableBuilder::COL_NUM);
        EXPECT_EQ(0, err);

      }

      TEST_F(TestObColumnGroupScanner, test_query_case_18)
      {
        int err = OB_SUCCESS;
        ObCellInfo** const cell_infos = sstable_builder_.get_cell_infos();
        ObRange range;
        range.table_id_ = SSTableBuilder::table_id;

        ///range.start_key_ = cell_infos[row_start_index][0].row_key_;//do not set start key
        ObBorderFlag border_flag;
        range.start_key_ = cell_infos[SSTableBuilder::ROW_NUM-10][0].row_key_; //end key
        char* end_key = (char*)"row_key_a";
        range.end_key_.assign_ptr(end_key, static_cast<int32_t>(strlen(end_key))); //end key
        range.border_flag_ = border_flag;

        //err = test_single_query_helper(range, row_start_index, row_expect_count, column_count);
        err = test_single_query_helper(range, SSTableBuilder::ROW_NUM-9, 9, SSTableBuilder::COL_NUM);
        EXPECT_EQ(0, err);

      }

      TEST_F(TestObColumnGroupScanner, test_query_case_19)
      {
        int err = OB_SUCCESS;
        ObRange range;
        range.table_id_ = SSTableBuilder::table_id;

        ///range.start_key_ = cell_infos[row_start_index][0].row_key_;//do not set start key
        ObBorderFlag border_flag;
        char* start_key = (char*)"row_key_a";
        border_flag.set_inclusive_start();
        border_flag.set_inclusive_end();
        range.start_key_.assign_ptr(start_key, static_cast<int32_t>(strlen(start_key))); //end key
        char* end_key = (char*)"row_key_a";
        range.end_key_.assign_ptr(end_key, static_cast<int32_t>(strlen(end_key))); //end key
        range.border_flag_ = border_flag;

        //err = test_single_query_helper(range, row_start_index, row_expect_count, column_count);
        err = test_single_query_helper(range, 0, 0, SSTableBuilder::COL_NUM);
        EXPECT_EQ(0, err);

      }


      TEST_F(TestObColumnGroupScanner, test_query_case_20)
      {
        int err = OB_SUCCESS;
        ObRange range;
        range.table_id_ = SSTableBuilder::table_id;

        ///range.start_key_ = cell_infos[row_start_index][0].row_key_;//do not set start key
        ObBorderFlag border_flag;
        char* start_key = (char*)"row_key_0";
        border_flag.set_inclusive_start();
        border_flag.set_inclusive_end();
        range.start_key_.assign_ptr(start_key, static_cast<int32_t>(strlen(start_key))); //end key
        char* end_key = (char*)"row_key_0";
        range.end_key_.assign_ptr(end_key, static_cast<int32_t>(strlen(end_key))); //end key
        range.border_flag_ = border_flag;

        //err = test_single_query_helper(range, row_start_index, row_expect_count, column_count);
        err = test_single_query_helper(range, 0, 0, SSTableBuilder::COL_NUM);
        EXPECT_EQ(0, err);

      }

      TEST_F(TestObColumnGroupScanner, test_query_case_21)
      {
        int err = OB_SUCCESS;
        ObRange range;
        range.table_id_ = SSTableBuilder::table_id;

        ///range.start_key_ = cell_infos[row_start_index][0].row_key_;//do not set start key
        ObBorderFlag border_flag;
        char* start_key = (char*)"row_key_0";
        border_flag.set_inclusive_start();
        border_flag.set_inclusive_end();
        range.start_key_.assign_ptr(start_key, static_cast<int32_t>(strlen(start_key))); //end key
        char* end_key = (char*)"row_key_1";
        range.end_key_.assign_ptr(end_key, static_cast<int32_t>(strlen(end_key))); //end key
        range.border_flag_ = border_flag;

        //err = test_single_query_helper(range, row_start_index, row_expect_count, column_count);
        err = test_single_query_helper(range, 0, SSTableBuilder::ROW_NUM, SSTableBuilder::COL_NUM);
        EXPECT_EQ(0, err);

      }


      TEST_F(TestObColumnGroupScanner, test_query_case_22)
      {
        int err = OB_SUCCESS;
        ObCellInfo** const cell_infos = sstable_builder_.get_cell_infos();
        ObRange range;
        range.table_id_ = SSTableBuilder::table_id;

        ///range.start_key_ = cell_infos[row_start_index][0].row_key_;//do not set start key
        ObBorderFlag border_flag;
        char* start_key = (char*)"row_key_0";
        border_flag.set_inclusive_start();
        border_flag.set_inclusive_end();
        range.start_key_.assign_ptr(start_key, static_cast<int32_t>(strlen(start_key))); //end key
        range.end_key_ = cell_infos[10][0].row_key_; //end key
        range.border_flag_ = border_flag;


        ObScanParam scan_param;

        scan_param.set(SSTableBuilder::table_id, table_name, range);
        scan_param.add_column((uint64_t)0);

        ObSSTableScanParam sstable_scan_param;
        sstable_scan_param.assign(scan_param);
        err = scanner_.set_scan_param(0, 0, 1, &sstable_scan_param, &sstable_reader_);
        EXPECT_EQ(0, err);

        // check result
        int64_t row_expect_count = 11;
        int64_t row_start_index = 0;
        int64_t row_index = 0;
        int64_t col_index = 0;
        int64_t column_count = SSTableBuilder::COL_NUM;
        int64_t total = 0;
        ObCellInfo* cur_cell = NULL;
        bool is_row_changed = false;
        bool first_row = true;
        while (OB_SUCCESS == scanner_.next_cell())
        {
          err = scanner_.get_cell(&cur_cell, &is_row_changed);
          EXPECT_EQ(0, err);
          EXPECT_NE((ObCellInfo*) NULL, cur_cell); 
          if (is_row_changed)
          {
            if (col_index > 0) EXPECT_EQ(column_count, col_index+1);
            // reset column
            col_index = 0;
            if (!first_row) ++row_index;
            if (first_row) first_row = false;
          }
          else
          {
            ++col_index;
          }
          check_cell(cell_infos[row_index + row_start_index][col_index], *cur_cell);
          ++total;
        }

        int row_count = 0;
        if (first_row) row_count = 0;
        else row_count = static_cast<int32_t>(row_index + 1);

        EXPECT_EQ(row_count , row_expect_count);
        EXPECT_EQ(row_count * column_count, total);
      }

      TEST_F(TestObColumnGroupScanner, test_query_case_min_to_max)
      {
        int err = OB_SUCCESS;
        ObCellInfo** const cell_infos = sstable_builder_.get_cell_infos();
        ObRange range;
        range.table_id_ = SSTableBuilder::table_id;

        ///range.start_key_ = cell_infos[row_start_index][0].row_key_;//do not set start key
        ObBorderFlag border_flag;
        //char* start_key = "row_key_0";
        border_flag.set_min_value();
        border_flag.set_max_value();
        //border_flag.set_inclusive_start();
        //border_flag.set_inclusive_end();
        //range.start_key_.assign_ptr(start_key, strlen(start_key)); //end key
        //range.end_key_ = cell_infos[10][0].row_key_; //end key
        range.border_flag_ = border_flag;


        ObScanParam scan_param;

        scan_param.set(SSTableBuilder::table_id, table_name, range);
        scan_param.add_column((uint64_t)0);

        ObSSTableScanParam sstable_scan_param;
        sstable_scan_param.assign(scan_param);
        err = scanner_.set_scan_param(0, 0, 1, &sstable_scan_param, &sstable_reader_);
        EXPECT_EQ(0, err);

        // check result
        int64_t row_expect_count = SSTableBuilder::ROW_NUM;
        int64_t row_start_index = 0;
        int64_t row_index = 0;
        int64_t col_index = 0;
        int64_t column_count = SSTableBuilder::COL_NUM;
        int64_t total = 0;
        ObCellInfo* cur_cell = NULL;
        bool is_row_changed = false;
        bool first_row = true;
        while (OB_SUCCESS == scanner_.next_cell())
        {
          err = scanner_.get_cell(&cur_cell, &is_row_changed);
          EXPECT_EQ(0, err);
          EXPECT_NE((ObCellInfo*) NULL, cur_cell); 
          if (is_row_changed)
          {
            if (col_index > 0) EXPECT_EQ(column_count, col_index+1);
            // reset column
            col_index = 0;
            if (!first_row) ++row_index;
            if (first_row) first_row = false;
          }
          else
          {
            ++col_index;
          }
          check_cell(cell_infos[row_index + row_start_index][col_index], *cur_cell);
          ++total;
        }

        int row_count = 0;
        if (first_row) row_count = 0;
        else row_count = static_cast<int32_t>(row_index + 1);

        EXPECT_EQ(row_count , row_expect_count);
        EXPECT_EQ(row_count * column_count, total);
      }

      TEST_F(TestObColumnGroupScanner, test_query_case_with_no_cache)
      {
        int err = OB_SUCCESS;
        ObCellInfo** const cell_infos = sstable_builder_.get_cell_infos();
        ObRange range;
        range.table_id_ = SSTableBuilder::table_id;

        ///range.start_key_ = cell_infos[row_start_index][0].row_key_;//do not set start key
        ObBorderFlag border_flag;
        char* start_key = (char*)"row_key_0";
        border_flag.set_inclusive_start();
        border_flag.set_inclusive_end();
        range.start_key_.assign_ptr(start_key, static_cast<int32_t>(strlen(start_key))); //end key
        range.end_key_ = cell_infos[10][0].row_key_; //end key
        range.border_flag_ = border_flag;


        ObScanParam scan_param;

        scan_param.set(SSTableBuilder::table_id, table_name, range);
        scan_param.set_is_result_cached(false);
        scan_param.add_column((uint64_t)0);

        ObSSTableScanParam sstable_scan_param;
        sstable_scan_param.assign(scan_param);
        err = scanner_.set_scan_param(0, 0, 1, &sstable_scan_param, &sstable_reader_);
        EXPECT_EQ(0, err);

        // check result
        int64_t row_expect_count = 11;
        int64_t row_start_index = 0;
        int64_t row_index = 0;
        int64_t col_index = 0;
        int64_t column_count = SSTableBuilder::COL_NUM;
        int64_t total = 0;
        ObCellInfo* cur_cell = NULL;
        bool is_row_changed = false;
        bool first_row = true;
        while (OB_SUCCESS == scanner_.next_cell())
        {
          err = scanner_.get_cell(&cur_cell, &is_row_changed);
          EXPECT_EQ(0, err);
          EXPECT_NE((ObCellInfo*) NULL, cur_cell); 
          if (is_row_changed)
          {
            if (col_index > 0) EXPECT_EQ(column_count, col_index+1);
            // reset column
            col_index = 0;
            if (!first_row) ++row_index;
            if (first_row) first_row = false;
          }
          else
          {
            ++col_index;
          }
          check_cell(cell_infos[row_index + row_start_index][col_index], *cur_cell);
          ++total;
        }

        int row_count = 0;
        if (first_row) row_count = 0;
        else row_count = static_cast<int32_t>(row_index + 1);

        EXPECT_EQ(row_count , row_expect_count);
        EXPECT_EQ(row_count * column_count, total);
      }

      TEST_F(TestObColumnGroupScanner, test_query_case_sync_read)
      {
        int err = OB_SUCCESS;
        ObCellInfo** const cell_infos = sstable_builder_.get_cell_infos();
        ObRange range;
        range.table_id_ = SSTableBuilder::table_id;

        ///range.start_key_ = cell_infos[row_start_index][0].row_key_;//do not set start key
        ObBorderFlag border_flag;
        char* start_key = (char*)"row_key_0";
        border_flag.set_inclusive_start();
        border_flag.set_inclusive_end();
        range.start_key_.assign_ptr(start_key, static_cast<int32_t>(strlen(start_key))); //end key
        range.end_key_ = cell_infos[10][0].row_key_; //end key
        range.border_flag_ = border_flag;


        ObScanParam scan_param;

        scan_param.set(SSTableBuilder::table_id, table_name, range);
        scan_param.set_is_result_cached(false);
        scan_param.set_scan_direction(ObScanParam::FORWARD);
        scan_param.set_read_mode(ObScanParam::SYNCREAD);
        scan_param.add_column((uint64_t)0);

        ObSSTableScanParam sstable_scan_param;
        sstable_scan_param.assign(scan_param);
        err = scanner_.set_scan_param(0, 0, 1, &sstable_scan_param, &sstable_reader_);
        EXPECT_EQ(0, err);

        // check result
        int64_t row_expect_count = 11;
        int64_t row_start_index = 0;
        int64_t row_index = 0;
        int64_t col_index = 0;
        int64_t column_count = SSTableBuilder::COL_NUM;
        int64_t total = 0;
        ObCellInfo* cur_cell = NULL;
        bool is_row_changed = false;
        bool first_row = true;
        while (OB_SUCCESS == scanner_.next_cell())
        {
          err = scanner_.get_cell(&cur_cell, &is_row_changed);
          EXPECT_EQ(0, err);
          EXPECT_NE((ObCellInfo*) NULL, cur_cell); 
          if (is_row_changed)
          {
            if (col_index > 0) EXPECT_EQ(column_count, col_index+1);
            // reset column
            col_index = 0;
            if (!first_row) ++row_index;
            if (first_row) first_row = false;
          }
          else
          {
            ++col_index;
          }
          check_cell(cell_infos[row_index + row_start_index][col_index], *cur_cell);
          ++total;
        }

        int row_count = 0;
        if (first_row) row_count = 0;
        else row_count = static_cast<int32_t>(row_index + 1);

        EXPECT_EQ(row_count , row_expect_count);
        EXPECT_EQ(row_count * column_count, total);
      }

      TEST_F(TestObColumnGroupScanner, test_query_case_block_border)
      {
        int err = OB_SUCCESS;

        ObCellInfo** const cell_infos = sstable_builder_.get_cell_infos();
        ObRange range;
        range.table_id_ = SSTableBuilder::table_id;

        ///range.start_key_ = cell_infos[row_start_index][0].row_key_;//do not set start key
        ObBorderFlag border_flag;
        //border_flag.set_inclusive_start(); //inclusive end
        //border_flag.set_max_value();//max
        border_flag.set_inclusive_end();
        range.start_key_ = cell_infos[1770][0].row_key_; //end key
        range.end_key_ = cell_infos[1780][0].row_key_; //end key
        range.border_flag_ = border_flag;

        //err = test_single_query_helper(range, row_start_index, row_expect_count, column_count);
        err = test_single_query_helper(range, 1771, 10, SSTableBuilder::COL_NUM);
        EXPECT_EQ(0, err);
      }

      TEST_F(TestObColumnGroupScanner, test_query_case_block_border2)
      {
        int err = OB_SUCCESS;

        ObCellInfo** const cell_infos = sstable_builder_.get_cell_infos();
        ObRange range;
        range.table_id_ = SSTableBuilder::table_id;

        ///range.start_key_ = cell_infos[row_start_index][0].row_key_;//do not set start key
        ObBorderFlag border_flag;
        //border_flag.set_inclusive_start(); //inclusive end
        //border_flag.set_max_value();//max
        //border_flag.set_inclusive_end();
        range.start_key_ = cell_infos[1770][0].row_key_; //end key
        range.end_key_ = cell_infos[3541][0].row_key_; //end key
        range.border_flag_ = border_flag;

        //err = test_single_query_helper(range, row_start_index, row_expect_count, column_count);
        err = test_single_query_helper(range, 1771, 3541-1770-1, SSTableBuilder::COL_NUM);
        EXPECT_EQ(0, err);
      }

      TEST_F(TestObColumnGroupScanner, DISABLED_test_query_case_user)
      {
        int err = OB_SUCCESS;
        ObRange range;
        range.table_id_ = 1001;

        ///range.start_key_ = cell_infos[row_start_index][0].row_key_;//do not set start key
        ObBorderFlag border_flag;
        char *sk = (char*)"000000000AE19ED60100000000000000";
        char *ek = (char*)"000000000AE19ED601FFFFFFFFFFFFFF";
        char start_hex_key[50];
        int sk_len = str_to_hex(sk, static_cast<int32_t>(strlen(sk)), start_hex_key, 50);
        char end_hex_key[50];
        int ek_len = str_to_hex(ek, static_cast<int32_t>(strlen(ek)), end_hex_key, 50);
        border_flag.unset_inclusive_start();
        border_flag.unset_inclusive_end();
        range.start_key_.assign_ptr(start_hex_key, sk_len/2); //end key
        range.end_key_.assign_ptr(end_hex_key, ek_len/2); 
        range.border_flag_ = border_flag;


        ObScanParam scan_param;

        scan_param.set(1001, table_name, range);
        scan_param.set_is_result_cached(false);
        scan_param.add_column((uint64_t)0);

        ObSSTableReader sstable_reader(allocator, GFactory::get_instance().get_fi_cache()) ;
        ObSSTableId id ;
        id.sstable_file_id_ = 281345;
        id.sstable_file_offset_ = 0;
        sstable_reader.open(id);

        ObSSTableScanParam sstable_scan_param;
        sstable_scan_param.assign(scan_param);
        err = scanner_.set_scan_param(0, 0, 1, &sstable_scan_param, &sstable_reader);
        EXPECT_EQ(0, err);

        // check result
        //int64_t row_expect_count = 11;
        //int64_t row_start_index = 0;
        int64_t row_index = 0;
        int64_t col_index = 0;
        int64_t column_count = SSTableBuilder::COL_NUM;
        int64_t total = 0;
        ObCellInfo* cur_cell = NULL;
        bool is_row_changed = false;
        bool first_row = true;
        while (OB_SUCCESS == scanner_.next_cell())
        {
          err = scanner_.get_cell(&cur_cell, &is_row_changed);
          hex_dump(cur_cell->row_key_.ptr(), cur_cell->row_key_.length());
          printf("get_cell = err =%d, is_row_changed=%d, type=%d, id=%ld\n", 
              err, is_row_changed, cur_cell->value_.get_type(), cur_cell->column_id_);
          if (is_row_changed)
          {
            if (col_index > 0) EXPECT_EQ(column_count, col_index+1);
            // reset column
            col_index = 0;
            if (!first_row) ++row_index;
            if (first_row) first_row = false;
          }
          else
          {
            ++col_index;
          }
          ++total;
        }

        int row_count = 0;
        if (first_row) row_count = 0;
        else row_count = static_cast<int32_t>(row_index + 1);

      }

      TEST_F(TestObColumnGroupScanner, test_reverse_case_1)
      {
        int err = OB_SUCCESS;
        ObCellInfo** const cell_infos = sstable_builder_.get_cell_infos();
        ObRange range;
        range.table_id_ = SSTableBuilder::table_id;

        ///range.start_key_ = cell_infos[row_start_index][0].row_key_;//do not set start key
        ObBorderFlag border_flag;
        char* start_key = (char*)"row_key_0";
        border_flag.set_inclusive_start();
        border_flag.set_inclusive_end();
        border_flag.set_max_value();
        border_flag.set_min_value();
        range.start_key_.assign_ptr(start_key, static_cast<int32_t>(strlen(start_key))); //end key
        range.end_key_ = cell_infos[3000][0].row_key_; //end key
        range.border_flag_ = border_flag;


        ObScanParam scan_param;

        scan_param.set(SSTableBuilder::table_id, table_name, range);
        scan_param.set_is_result_cached(false);
        scan_param.set_scan_direction(ObScanParam::BACKWARD);
        scan_param.add_column((uint64_t)0);

        ObSSTableScanParam sstable_scan_param;
        sstable_scan_param.assign(scan_param);
        err = scanner_.set_scan_param(0, 0, 1, &sstable_scan_param, &sstable_reader_);
        EXPECT_EQ(0, err);

        // check result
        int64_t row_expect_count = SSTableBuilder::ROW_NUM;
        //int64_t row_start_index = 0;
        int64_t row_index = 0;
        int64_t col_index = 0;
        int64_t column_count = SSTableBuilder::COL_NUM;
        int64_t total = 0;
        ObCellInfo* cur_cell = NULL;
        bool is_row_changed = false;
        bool first_row = true;
        while (OB_SUCCESS == scanner_.next_cell())
        {
          err = scanner_.get_cell(&cur_cell, &is_row_changed);
          EXPECT_EQ(0, err);
          EXPECT_NE((ObCellInfo*) NULL, cur_cell); 
          if (is_row_changed)
          {
            if (col_index > 0) EXPECT_EQ(column_count, col_index+1);
            // reset column
            col_index = 0;
            if (!first_row) ++row_index;
            if (first_row) first_row = false;
            //printf("rowkey=%.*s\n", cur_cell->row_key_.length(), cur_cell->row_key_.ptr());
          }
          else
          {
            ++col_index;
          }
          check_cell(cell_infos[SSTableBuilder::ROW_NUM-row_index-1][col_index], *cur_cell);
          ++total;
        }

        int row_count = 0;
        if (first_row) row_count = 0;
        else row_count = static_cast<int32_t>(row_index + 1);

        EXPECT_EQ(row_count , row_expect_count);
        EXPECT_EQ(row_count * column_count, total);
      }

      TEST_F(TestObColumnGroupScanner, test_query_case_less_than)
      {
        int err = OB_SUCCESS;
        ObCellInfo** const cell_infos = sstable_builder_.get_cell_infos();
        ObRange range;
        range.table_id_ = SSTableBuilder::table_id;

        ///range.start_key_ = cell_infos[row_start_index][0].row_key_;//do not set start key
        ObBorderFlag border_flag;
        char* start_key = (char*)"row_key_0";
        range.start_key_.assign_ptr(start_key, static_cast<int32_t>(strlen(start_key))); //end key
        range.end_key_ = cell_infos[0][0].row_key_; //end key
        range.border_flag_ = border_flag;


        ObScanParam scan_param;

        scan_param.set(SSTableBuilder::table_id, table_name, range);
        scan_param.set_scan_direction(ObScanParam::FORWARD);
        scan_param.add_column((uint64_t)0);

        ObSSTableScanParam sstable_scan_param;
        sstable_scan_param.assign(scan_param);
        err = scanner_.set_scan_param(0, 0, 1, &sstable_scan_param, &sstable_reader_);
        EXPECT_EQ(0, err);
        EXPECT_EQ(OB_ITER_END, scanner_.next_cell());
      }

      TEST_F(TestObColumnGroupScanner, test_query_case_less_than_reverse)
      {
        int err = OB_SUCCESS;
        ObCellInfo** const cell_infos = sstable_builder_.get_cell_infos();
        ObRange range;
        range.table_id_ = SSTableBuilder::table_id;

        ObBorderFlag border_flag;
        char* start_key = (char*)"row_key_0";
        range.start_key_.assign_ptr(start_key, static_cast<int32_t>(strlen(start_key))); //end key
        range.end_key_ = cell_infos[0][0].row_key_; //end key
        range.border_flag_ = border_flag;


        ObScanParam scan_param;

        scan_param.set(SSTableBuilder::table_id, table_name, range);
        scan_param.set_scan_direction(ObScanParam::BACKWARD);
        scan_param.add_column((uint64_t)0);

        ObSSTableScanParam sstable_scan_param;
        sstable_scan_param.assign(scan_param);
        err = scanner_.set_scan_param(0, 0, 1, &sstable_scan_param, &sstable_reader_);
        EXPECT_EQ(0, err);
        EXPECT_EQ(OB_ITER_END, scanner_.next_cell());
      }

      TEST_F(TestObColumnGroupScanner, test_query_case_greater_than)
      {
        int err = OB_SUCCESS;
        ObCellInfo** const cell_infos = sstable_builder_.get_cell_infos();
        ObRange range;
        range.table_id_ = SSTableBuilder::table_id;

        ///range.start_key_ = cell_infos[row_start_index][0].row_key_;//do not set start key
        ObBorderFlag border_flag;
        border_flag.set_max_value();
        char* start_key = (char*)"row_key_x";
        range.start_key_.assign_ptr(start_key, static_cast<int32_t>(strlen(start_key))); //end key
        range.end_key_ = cell_infos[0][0].row_key_; //end key, not used.
        range.border_flag_ = border_flag;


        ObScanParam scan_param;

        scan_param.set(SSTableBuilder::table_id, table_name, range);
        scan_param.set_scan_direction(ObScanParam::FORWARD);
        scan_param.add_column((uint64_t)0);

        ObSSTableScanParam sstable_scan_param;
        sstable_scan_param.assign(scan_param);
        err = scanner_.set_scan_param(0, 0, 1, &sstable_scan_param, &sstable_reader_);
        EXPECT_EQ(0, err);
        EXPECT_EQ(OB_ITER_END, scanner_.next_cell());

      }

      TEST_F(TestObColumnGroupScanner, test_twice_end)
      {
        int err = OB_SUCCESS;
        ObCellInfo** const cell_infos = sstable_builder_.get_cell_infos();
        ObRange range;
        range.table_id_ = SSTableBuilder::table_id;

        ///range.start_key_ = cell_infos[row_start_index][0].row_key_;//do not set start key
        ObBorderFlag border_flag;
        char* start_key = (char*)"row_key_0";
        border_flag.set_inclusive_start();
        border_flag.set_inclusive_end();
        border_flag.set_max_value();
        border_flag.set_min_value();
        range.start_key_.assign_ptr(start_key, static_cast<int32_t>(strlen(start_key))); //end key
        range.end_key_ = cell_infos[3000][0].row_key_; //end key
        range.border_flag_ = border_flag;


        ObScanParam scan_param;

        scan_param.set(SSTableBuilder::table_id, table_name, range);
        scan_param.set_is_result_cached(false);
        scan_param.set_scan_direction(ObScanParam::FORWARD);
        scan_param.add_column((uint64_t)0);

        ObSSTableScanParam sstable_scan_param;
        sstable_scan_param.assign(scan_param);
        err = scanner_.set_scan_param(0, 0, 1, &sstable_scan_param, &sstable_reader_);
        EXPECT_EQ(0, err);

        // check result
        int64_t row_expect_count = SSTableBuilder::ROW_NUM;
        //int64_t row_start_index = 0;
        int64_t row_index = 0;
        int64_t col_index = 0;
        int64_t column_count = SSTableBuilder::COL_NUM;
        int64_t total = 0;
        ObCellInfo* cur_cell = NULL;
        bool is_row_changed = false;
        bool first_row = true;
        while (OB_SUCCESS == scanner_.next_cell())
        {
          err = scanner_.get_cell(&cur_cell, &is_row_changed);
          EXPECT_EQ(0, err);
          EXPECT_NE((ObCellInfo*) NULL, cur_cell); 
          if (is_row_changed)
          {
            if (col_index > 0) EXPECT_EQ(column_count, col_index+1);
            // reset column
            col_index = 0;
            if (!first_row) ++row_index;
            if (first_row) first_row = false;
            //printf("rowkey=%.*s\n", cur_cell->row_key_.length(), cur_cell->row_key_.ptr());
          }
          else
          {
            ++col_index;
          }
          //check_cell(cell_infos[row_index][col_index], *cur_cell);
          ++total;
        }

        printf("scanner_.next_cell=%d\n", scanner_.next_cell());

        int row_count = 0;
        if (first_row) row_count = 0;
        else row_count = static_cast<int32_t>(row_index + 1);

        EXPECT_EQ(row_count , row_expect_count);
        EXPECT_EQ(row_count * column_count, total);
      }

    } // end namespace sstable
  } // end namespace tests
} // end namespace oceanbase


int main(int argc, char** argv)
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

