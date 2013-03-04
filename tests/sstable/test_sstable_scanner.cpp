/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * test_sstable_scanner.cpp for what ... 
 *
 * Authors:
 *   chuanhui <rizhao.ych@taobao.com>
 *   qushan<qushan@taobao.com>
 *
 */
#include <iostream>
#include <sstream>
#include <algorithm>
#include <tblog.h>
#include <gtest/gtest.h>
#include "common/utility.h"
#include "common/page_arena.h"
#include "chunkserver/ob_fileinfo_cache.h"
#include "test_helper.h"
#include "sstable/ob_sstable_reader.h"
#include "sstable/ob_sstable_scanner.h"

using namespace std;
using namespace oceanbase::common;
using namespace oceanbase::chunkserver;
using namespace oceanbase::sstable;

namespace oceanbase
{
  namespace tests
  {
    namespace sstable
    {

      static const ObString table_name;
      static const int64_t sstable_file_id = 1001;
      static const int64_t sstable_file_offset = 0;
      static const ObSSTableId sstable_id(sstable_file_id);
      static ModulePageAllocator mod(0);
      static ModuleArena allocator(ModuleArena::DEFAULT_PAGE_SIZE, mod);
      static ObSSTableReader sstable_reader_(allocator, GFactory::get_instance().get_fi_cache());
      static SSTableBuilder sstable_builder_;

      class TestObSSTableScanner : public ::testing::Test
      {
        public:
          TestObSSTableScanner()
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
            int err = sstable_builder_.generate_sstable_file(write_cg_sstable, sstable_id);
            EXPECT_EQ(0, err);
          }

          static void TearDownTestCase()
          {
          }

          virtual void SetUp()
          {
            sstable_reader_.reset();
            int err = sstable_reader_.open(sstable_id);
            EXPECT_EQ(0, err);
            EXPECT_TRUE(sstable_reader_.is_opened());
          }

          virtual void TearDown()
          {

          }

          ObSSTableScanner scanner_;
      };

      int TestObSSTableScanner::test_single_query_helper(const ObRange& range, 
          const int32_t row_start_index , const int32_t row_expect_count, 
          const int32_t column_count )
      {
        ObScanParam scan_param;

        scan_param.set(SSTableBuilder::table_id, table_name, range);
        ObCellInfo** const cell_infos = sstable_builder_.get_cell_infos();
        for (int64_t i = 0; i < column_count; ++i)
        {
          scan_param.add_column(cell_infos[0][i].column_id_);
        }

        int err = scanner_.set_scan_param(scan_param, &sstable_reader_,
                GFactory::get_instance().get_block_cache(),
                GFactory::get_instance().get_block_index_cache()
            );
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

      int TestObSSTableScanner::test_single_query_helper(
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

      TEST_F(TestObSSTableScanner, test_query_case_1)
      {
        int err = OB_SUCCESS;

        ObBorderFlag border_flag;
        border_flag.set_inclusive_start(); //inclusive end
        border_flag.set_inclusive_end(); //inclusive end

        err = test_single_query_helper(border_flag, 0, 0, 1, 2);
        EXPECT_EQ(0, err);
      }

      TEST_F(TestObSSTableScanner, test_query_case_2)
      {
        int err = OB_SUCCESS;

        ObBorderFlag border_flag;

        err = test_single_query_helper(border_flag, 0, 0, 0, 2);
        EXPECT_EQ(OB_INVALID_ARGUMENT, err);
      }

      TEST_F(TestObSSTableScanner, test_query_case_3)
      {
        int err = OB_SUCCESS;

        ObBorderFlag border_flag;
        border_flag.set_inclusive_start(); //inclusive end

        err = test_single_query_helper(border_flag, 0, 0, 0, 2);
        EXPECT_EQ(OB_INVALID_ARGUMENT, err);
      }

      TEST_F(TestObSSTableScanner, test_query_case_4)
      {
        int err = OB_SUCCESS;


        ObBorderFlag border_flag;
        border_flag.set_inclusive_end(); //inclusive end
        err = test_single_query_helper(border_flag, 0, 0, 0, 2);
        EXPECT_EQ(OB_INVALID_ARGUMENT, err);
      }

      TEST_F(TestObSSTableScanner, test_query_case_5)
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

      TEST_F(TestObSSTableScanner, test_query_case_6)
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
        err = test_single_query_helper(range, 0, 11, SSTableBuilder::COL_NUM);
        EXPECT_EQ(0, err);
      }

      TEST_F(TestObSSTableScanner, test_query_case_7)
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
        err = test_single_query_helper(range, 10, SSTableBuilder::ROW_NUM -10, SSTableBuilder::COL_NUM);
        EXPECT_EQ(0, err);
      }

      TEST_F(TestObSSTableScanner, test_query_case_8)
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
        err = test_single_query_helper(range, 10, SSTableBuilder::ROW_NUM -10, SSTableBuilder::COL_NUM);
        EXPECT_EQ(0, err);
      }

      TEST_F(TestObSSTableScanner, test_query_case_9)
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
        err = test_single_query_helper(range, 0, SSTableBuilder::ROW_NUM, SSTableBuilder::COL_NUM);
        EXPECT_EQ(0, err);
      }

      TEST_F(TestObSSTableScanner, test_query_case_10)
      {
        int err = OB_SUCCESS;

        ObBorderFlag border_flag;
        border_flag.set_inclusive_start(); //inclusive end
        border_flag.set_inclusive_end(); //not inclusive end

        // start_index = 10
        // end_index = 20
        err = test_single_query_helper(border_flag, 10, 20, 11, SSTableBuilder::COL_NUM);
        EXPECT_EQ(0, err);
      }

      TEST_F(TestObSSTableScanner, test_query_case_11)
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

        err = test_single_query_helper(range, 11, 10, SSTableBuilder::COL_NUM);
        EXPECT_EQ(0, err);
      }

      TEST_F(TestObSSTableScanner, test_query_case_12)
      {
        int err = OB_SUCCESS;

        ObBorderFlag border_flag;
        border_flag.set_inclusive_start(); //inclusive end

        // start_index = 10
        // end_index = 20
        err = test_single_query_helper(border_flag, 10, 20, 10, 1);
        EXPECT_EQ(0, err);
      }

      TEST_F(TestObSSTableScanner, test_query_case_13)
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



      TEST_F(TestObSSTableScanner, test_query_case_14)
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

      TEST_F(TestObSSTableScanner, test_query_case_15)
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



      TEST_F(TestObSSTableScanner, test_query_case_16)
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

      TEST_F(TestObSSTableScanner, test_query_case_17)
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

      TEST_F(TestObSSTableScanner, test_query_case_18)
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

      TEST_F(TestObSSTableScanner, test_query_case_19)
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


      TEST_F(TestObSSTableScanner, test_query_case_20)
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

      TEST_F(TestObSSTableScanner, test_query_case_21)
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


      TEST_F(TestObSSTableScanner, test_query_case_22)
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

        err = scanner_.set_scan_param(scan_param, &sstable_reader_,
                GFactory::get_instance().get_block_cache(),
                GFactory::get_instance().get_block_index_cache()
            );
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

      TEST_F(TestObSSTableScanner, test_query_case_min_to_max)
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

        err = scanner_.set_scan_param(scan_param, &sstable_reader_,
                GFactory::get_instance().get_block_cache(),
                GFactory::get_instance().get_block_index_cache()
            );
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

      TEST_F(TestObSSTableScanner, test_query_case_with_no_cache)
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

        err = scanner_.set_scan_param(scan_param, &sstable_reader_,
                GFactory::get_instance().get_block_cache(),
                GFactory::get_instance().get_block_index_cache()
            );
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

      TEST_F(TestObSSTableScanner, test_query_case_sync_read)
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
        scan_param.set_read_mode(ObScanParam::SYNCREAD);
        scan_param.add_column((uint64_t)0);

        err = scanner_.set_scan_param(scan_param, &sstable_reader_,
                GFactory::get_instance().get_block_cache(),
                GFactory::get_instance().get_block_index_cache()
            );
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

      TEST_F(TestObSSTableScanner, test_query_case_no_exist_column)
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
        scan_param.set_read_mode(ObScanParam::SYNCREAD);
        scan_param.add_column(50);
        scan_param.add_column(60);

        err = scanner_.set_scan_param(scan_param, &sstable_reader_,
                GFactory::get_instance().get_block_cache(),
                GFactory::get_instance().get_block_index_cache()
            );
        EXPECT_EQ(0, err);

        // check result
        int64_t row_expect_count = 11;
        int64_t row_start_index = 0;
        int64_t row_index = 0;
        int64_t col_index = 0;
        int64_t column_count = 2;
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
            EXPECT_EQ(0, cur_cell->row_key_.compare(cell_infos[row_index+row_start_index][0].row_key_));
          }
          else
          {
            ++col_index;
          }
          EXPECT_EQ(cur_cell->value_.get_type(), ObNullType);
          ++total;
        }

        int row_count = 0;
        if (first_row) row_count = 0;
        else row_count = static_cast<int32_t>(row_index + 1);

        EXPECT_EQ(row_count , row_expect_count);
        EXPECT_EQ(row_count * column_count, total);
      }

      TEST_F(TestObSSTableScanner, test_query_case_partial_exist_column)
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
        scan_param.set_read_mode(ObScanParam::SYNCREAD);
        scan_param.add_column(50);
        scan_param.add_column(3);

        err = scanner_.set_scan_param(scan_param, &sstable_reader_,
                GFactory::get_instance().get_block_cache(),
                GFactory::get_instance().get_block_index_cache()
            );
        EXPECT_EQ(0, err);

        // check result
        int64_t row_expect_count = 11;
        int64_t row_start_index = 0;
        int64_t row_index = 0;
        int64_t col_index = 0;
        int64_t column_count = 2;
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
            EXPECT_EQ(0, cur_cell->row_key_.compare(cell_infos[row_index+row_start_index][0].row_key_));
          }
          else
          {
            ++col_index;
          }
          /*
          fprintf(stderr, "row:%ld,col:%ld,id:%ld,type:%d\n", 
              row_index, col_index, cur_cell->column_id_, cur_cell->value_.get_type());
              */
          ++total;
        }

        int row_count = 0;
        if (first_row) row_count = 0;
        else row_count = static_cast<int32_t>(row_index + 1);

        EXPECT_EQ(row_count , row_expect_count);
        EXPECT_EQ(row_count * column_count, total);
      }

      TEST_F(TestObSSTableScanner, test_query_case_no_exist_rows)
      {
        int err = OB_SUCCESS;
        ObCellInfo** const cell_infos = sstable_builder_.get_cell_infos();
        ObRange range;
        range.table_id_ = SSTableBuilder::table_id;

        ///range.start_key_ = cell_infos[row_start_index][0].row_key_;//do not set start key
        ObBorderFlag border_flag;
        char* start_key = (char*)"row_key_000000001";
        border_flag.set_inclusive_start();
        border_flag.set_inclusive_end();
        range.start_key_.assign_ptr(start_key, static_cast<int32_t>(strlen(start_key))); //end key
        char* end_key = (char*)"row_key_000000001";
        range.end_key_.assign_ptr(end_key, static_cast<int32_t>(strlen(end_key)) );//end key
        range.border_flag_ = border_flag;


        ObScanParam scan_param;

        scan_param.set(SSTableBuilder::table_id, table_name, range);
        scan_param.set_is_result_cached(false);
        scan_param.set_read_mode(ObScanParam::SYNCREAD);
        scan_param.add_column(3);

        err = scanner_.set_scan_param(scan_param, &sstable_reader_,
                GFactory::get_instance().get_block_cache(),
                GFactory::get_instance().get_block_index_cache()
            );
        EXPECT_EQ(0, err);

        // check result
        int64_t row_expect_count = 0;
        int64_t row_start_index = 0;
        int64_t row_index = 0;
        int64_t col_index = 0;
        int64_t column_count = 2;
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
            EXPECT_EQ(0, cur_cell->row_key_.compare(cell_infos[row_index+row_start_index][0].row_key_));
          }
          else
          {
            ++col_index;
          }
          EXPECT_EQ(cur_cell->value_.get_type(), ObNullType);
          ++total;
        }

        int row_count = 0;
        if (first_row) row_count = 0;
        else row_count = static_cast<int32_t>(row_index + 1);

        EXPECT_EQ(row_count , row_expect_count);
        EXPECT_EQ(row_count * column_count, total);
      }

      TEST_F(TestObSSTableScanner, test_no_data_to_query)
      {
        int err = OB_SUCCESS;
        ObScanParam scan_param;

        err = scanner_.set_scan_param(scan_param, NULL,
                GFactory::get_instance().get_block_cache(),
                GFactory::get_instance().get_block_index_cache()
            );
        EXPECT_EQ(OB_SUCCESS, err);
        err = scanner_.next_cell();
        EXPECT_EQ(OB_ITER_END, err);

        ObRange range;
        scan_param.set(SSTableBuilder::table_id + 1, table_name, range);
        err = scanner_.set_scan_param(scan_param, &sstable_reader_,
                GFactory::get_instance().get_block_cache(),
                GFactory::get_instance().get_block_index_cache()
            );
        EXPECT_EQ(OB_SUCCESS, err);
        err = scanner_.next_cell();
        EXPECT_EQ(OB_ITER_END, err);
      }

    } // end namespace sstable
  } // end namespace tests
} // end namespace oceanbase


int main(int argc, char** argv)
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
