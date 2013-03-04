/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * test_import_sstable.cpp for test import sstable
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */
#include <gtest/gtest.h>
#include "common/file_directory_utils.h"
#include "common/ob_malloc.h"
#include "sstable/ob_disk_path.h"
#include "ob_disk_manager.h"
#include "ob_import_sstable.h"
#include "test_helper.h"

using namespace oceanbase::common;
using namespace oceanbase::chunkserver;
using namespace oceanbase::sstable;

namespace oceanbase
{
  namespace tests
  {
    namespace chunkserver
    {
      static const char* APP_NAME = "test";
      static const char* TMP_DATA_DIR = "./tmp";
      static char range_file_name[OB_MAX_FILE_NAME_LENGTH];
      static char range_file_path[OB_MAX_FILE_NAME_LENGTH];
      static MultSSTableBuilder mult_builder_;

      class TestObImportSSTable : public ::testing::Test
      {
      public:
        static void SetUpTestCase()
        {
          prepare_import_data();
        }
      
        static void TearDownTestCase()
        {
        }

        virtual void SetUp()
        {
        }

        virtual void TearDown()
        {
        }

        static void prepare_import_data()
        {
          int ret = OB_SUCCESS;
          ObCellInfo** cell_info = NULL;
          char sstable_path[OB_MAX_FILE_NAME_LENGTH];
          char import_sstable_dir[OB_MAX_FILE_NAME_LENGTH];
          char import_sstable_path[OB_MAX_FILE_NAME_LENGTH];
          char sstable_name[OB_MAX_FILE_NAME_LENGTH];
          char range_file_line[256];
          int32_t line_len = 0;
          char hexstr_key[128];
          int32_t hexstr_key_len = 0;
          ObSSTableId sst_id;
          FILE* fp = NULL;
          int wrote_len = 0;
          char cmd[256];

          sprintf(cmd, "rm -rf ./tmp");
          system(cmd);

          ret = mult_builder_.generate_sstable_files();
          EXPECT_EQ(OB_SUCCESS, ret);

          for (int64_t i = 0; i < MultSSTableBuilder::SSTABLE_NUM; ++i)
          {
            ret = mult_builder_.get_sstable_id(sst_id, i);
            EXPECT_EQ(OB_SUCCESS, ret);
            ret = get_sstable_path(sst_id, sstable_path, OB_MAX_FILE_NAME_LENGTH);
            EXPECT_EQ(OB_SUCCESS, ret);

            ret = get_import_sstable_directory(sst_id.sstable_file_id_ & 0xFF, 
              import_sstable_dir, OB_MAX_FILE_NAME_LENGTH);
            EXPECT_EQ(OB_SUCCESS, ret);
            if (!FileDirectoryUtils::exists(import_sstable_dir))
            {
              EXPECT_TRUE(FileDirectoryUtils::create_full_path(import_sstable_dir));
            }

            snprintf(sstable_name, OB_MAX_FILE_NAME_LENGTH, "%lu-%06ld", 
              SSTableBuilder::table_id, i);
            ret = get_import_sstable_path(sst_id.sstable_file_id_ & 0xFF, sstable_name,
              import_sstable_path, OB_MAX_FILE_NAME_LENGTH);
            EXPECT_EQ(OB_SUCCESS, ret);

            EXPECT_TRUE(FileDirectoryUtils::rename(sstable_path, import_sstable_path));
            if (0 == i)
            {
              snprintf(range_file_name, OB_MAX_FILE_NAME_LENGTH, 
                "%s-table_name-%lu-range.ini", 
                APP_NAME, SSTableBuilder::table_id);
              snprintf(range_file_path, OB_MAX_FILE_NAME_LENGTH, 
                "%s/%s", import_sstable_dir, range_file_name);
              fp = fopen(range_file_path, "w+");
              EXPECT_TRUE(NULL != fp);
            }
            cell_info = mult_builder_.get_cell_infos(i);
            EXPECT_TRUE(NULL != cell_info);
            hexstr_key_len = hex_to_str(cell_info[SSTableBuilder::ROW_NUM - 1][0].row_key_.ptr(), 
              cell_info[SSTableBuilder::ROW_NUM - 1][0].row_key_.length(), 
              hexstr_key, 128);
            if (MultSSTableBuilder::SSTABLE_NUM - 1 == i)
            {
              // set the last hexstr rowkey with char 'F'
              memset(hexstr_key, 'F', hexstr_key_len * 2);
            }
            line_len = snprintf(range_file_line, 256, 
              "%.*s %lu-%06ld %.*s 10.232.36.38 10.232.36.39\n",
              cell_info[SSTableBuilder::ROW_NUM - 1][0].row_key_.length(),
              cell_info[SSTableBuilder::ROW_NUM - 1][0].row_key_.ptr(),
              SSTableBuilder::table_id, i, hexstr_key_len * 2, hexstr_key);
            wrote_len = static_cast<int32_t>(fwrite(range_file_line, 1, line_len, fp));
            EXPECT_EQ(line_len, wrote_len);
          }

          fclose(fp);
        }

        void not_init_check(ObImportSSTable& import_sstable)
        {
          ObRange invalid_range;
          ObRange valid_range;
          ObCellInfo** cell_info = NULL;
  
          valid_range.table_id_ = SSTableBuilder::table_id;
          valid_range.border_flag_.set_min_value();
          cell_info = mult_builder_.get_cell_infos(0);
          EXPECT_TRUE(NULL != cell_info);
          valid_range.end_key_.assign_ptr(cell_info[SSTableBuilder::ROW_NUM - 1][0].row_key_.ptr(),
            cell_info[SSTableBuilder::ROW_NUM - 1][0].row_key_.length());

          not_init_check(import_sstable, invalid_range, valid_range);
        }

        void not_init_check(ObImportSSTable& import_sstable,
          ObRange& invalid_range, ObRange& valid_range)
        {
          int ret = OB_SUCCESS;
          ObFilePathInfo sstable_info;

          EXPECT_FALSE(import_sstable.need_import_sstable(0));
          EXPECT_FALSE(import_sstable.need_import_sstable(OB_INVALID_ID));
          EXPECT_FALSE(import_sstable.need_import_sstable(SSTableBuilder::table_id));
          ret = import_sstable.get_import_sstable_info(invalid_range, sstable_info);
          EXPECT_EQ(OB_IMPORT_SSTABLE_NOT_EXIST, ret);
          ret = import_sstable.get_import_sstable_info(valid_range, sstable_info);
          EXPECT_EQ(OB_IMPORT_SSTABLE_NOT_EXIST, ret);
        }

        void init_import_sstable_succ(ObImportSSTable& import_sstable, 
          ObDiskManager& disk_manager)
        {
          int ret = OB_SUCCESS;
          ObRange invalid_range;
          ObFilePathInfo sstable_info;
          ObRange valid_range;
          ObCellInfo** cell_info = NULL;
          char file_name[OB_MAX_FILE_NAME_LENGTH];
          ObString start_key;

          ret = disk_manager.scan(TMP_DATA_DIR, 256 * 1024 * 1024);
          EXPECT_EQ(OB_SUCCESS, ret);
          ret = import_sstable.init(disk_manager, APP_NAME);
          EXPECT_EQ(OB_SUCCESS, ret);

          EXPECT_FALSE(import_sstable.need_import_sstable(0));
          EXPECT_FALSE(import_sstable.need_import_sstable(OB_INVALID_ID));
          EXPECT_TRUE(import_sstable.need_import_sstable(SSTableBuilder::table_id));
          ret = import_sstable.get_import_sstable_info(invalid_range, sstable_info);
          EXPECT_EQ(OB_IMPORT_SSTABLE_NOT_EXIST, ret);

          for (int64_t i = 0; i < MultSSTableBuilder::SSTABLE_NUM; ++i)
          {
            valid_range.reset();
            valid_range.table_id_ = SSTableBuilder::table_id;
            if (0 == i)
            {
              valid_range.border_flag_.set_min_value();
            }
            else
            {
              cell_info = mult_builder_.get_cell_infos(i);
              EXPECT_TRUE(NULL != cell_info);
              valid_range.start_key_= start_key;
              valid_range.border_flag_.unset_inclusive_start();
            }

            if (MultSSTableBuilder::SSTABLE_NUM - 1 == i)
            {
              valid_range.border_flag_.set_max_value();
            }
            else 
            {
              cell_info = mult_builder_.get_cell_infos(i);
              EXPECT_TRUE(NULL != cell_info);
              valid_range.end_key_.assign_ptr(
                cell_info[SSTableBuilder::ROW_NUM - 1][0].row_key_.ptr(),
                cell_info[SSTableBuilder::ROW_NUM - 1][0].row_key_.length());
              valid_range.border_flag_.set_inclusive_end();
            }

            ret = import_sstable.get_import_sstable_info(valid_range, sstable_info);
            EXPECT_EQ(OB_SUCCESS, ret);
            EXPECT_EQ(sstable_info.disk_no_, i % MultSSTableBuilder::DISK_NUM + 1);
            snprintf(file_name, OB_MAX_FILE_NAME_LENGTH, "%lu-%06ld", 
              SSTableBuilder::table_id, i);
            EXPECT_EQ(0, strcmp(sstable_info.file_name_, file_name));

            start_key = valid_range.end_key_;
          }
        }
      };

      TEST_F(TestObImportSSTable, test_init_import_sstable_fail)
      {
        int ret = OB_SUCCESS;
        ObImportSSTable import_sstable;
        ObRange invalid_range;
        ObFilePathInfo sstable_info;
        ObRange valid_range;
        ObDiskManager disk_manager;
        const char* wrong_data_dir = "./TMP";
        ObCellInfo** cell_info = NULL;

        valid_range.table_id_ = SSTableBuilder::table_id;
        valid_range.border_flag_.set_min_value();
        cell_info = mult_builder_.get_cell_infos(0);
        EXPECT_TRUE(NULL != cell_info);
        valid_range.end_key_.assign_ptr(cell_info[SSTableBuilder::ROW_NUM - 1][0].row_key_.ptr(),
          cell_info[SSTableBuilder::ROW_NUM - 1][0].row_key_.length());

        // not init
        EXPECT_FALSE(import_sstable.need_import_sstable(0));
        EXPECT_FALSE(import_sstable.need_import_sstable(OB_INVALID_ID));
        EXPECT_FALSE(import_sstable.need_import_sstable(SSTableBuilder::table_id));
        ret = import_sstable.get_import_sstable_info(invalid_range, sstable_info);
        EXPECT_EQ(OB_NOT_INIT, ret);
        ret = import_sstable.get_import_sstable_info(valid_range, sstable_info);
        EXPECT_EQ(OB_NOT_INIT, ret);

        // wrong init param
        ret = import_sstable.init(disk_manager, APP_NAME);
        EXPECT_EQ(OB_ERROR, ret);
        ret = import_sstable.init(disk_manager, NULL);
        EXPECT_EQ(OB_ERROR, ret);
        ret = disk_manager.scan(wrong_data_dir, 256 * 1024 * 1024);
        EXPECT_EQ(OB_ERROR, ret);
        import_sstable.clear();
        ret = import_sstable.init(disk_manager, APP_NAME);
        EXPECT_EQ(OB_ERROR, ret);
        not_init_check(import_sstable, invalid_range, valid_range);

        // no range file
        char tmp_range_file_path[OB_MAX_FILE_NAME_LENGTH];
        snprintf(tmp_range_file_path, OB_MAX_FILE_NAME_LENGTH, 
          "%s/%s", TMP_DATA_DIR, range_file_name);
        EXPECT_TRUE(FileDirectoryUtils::rename(range_file_path, tmp_range_file_path));
        ret = disk_manager.scan(TMP_DATA_DIR, 256 * 1024 * 1024);
        EXPECT_EQ(OB_SUCCESS, ret);
        import_sstable.clear();
        ret = import_sstable.init(disk_manager, APP_NAME);
        EXPECT_EQ(OB_SUCCESS, ret);
        not_init_check(import_sstable, invalid_range, valid_range);

        // wrong range file name
        char wrong_range_file_path[OB_MAX_FILE_NAME_LENGTH];
        char import_sstable_dir[OB_MAX_FILE_NAME_LENGTH];
        ret = get_import_sstable_directory(1, import_sstable_dir, 
          OB_MAX_FILE_NAME_LENGTH);
        EXPECT_EQ(OB_SUCCESS, ret);
        snprintf(wrong_range_file_path, OB_MAX_FILE_NAME_LENGTH, 
          "%s/range.ini", import_sstable_dir);
        ret = FileDirectoryUtils::cp("", tmp_range_file_path, 
          "", wrong_range_file_path);
        EXPECT_EQ(OB_SUCCESS, ret);
        ret = disk_manager.scan(TMP_DATA_DIR, 256 * 1024 * 1024);
        EXPECT_EQ(OB_SUCCESS, ret);
        import_sstable.clear();
        ret = import_sstable.init(disk_manager, APP_NAME);
        EXPECT_EQ(OB_ERROR, ret);
        not_init_check(import_sstable, invalid_range, valid_range);

        // one correct range file, one wrong range file
        EXPECT_TRUE(FileDirectoryUtils::rename(tmp_range_file_path, range_file_path));
        ret = disk_manager.scan(TMP_DATA_DIR, 256 * 1024 * 1024);
        EXPECT_EQ(OB_SUCCESS, ret);
        import_sstable.clear();
        ret = import_sstable.init(disk_manager, APP_NAME);
        EXPECT_EQ(OB_ERROR, ret);
        not_init_check(import_sstable, invalid_range, valid_range);

        EXPECT_TRUE(FileDirectoryUtils::delete_file(wrong_range_file_path));
      }

      TEST_F(TestObImportSSTable, test_init_import_sstable_succ)
      {
        ObImportSSTable import_sstable;
        ObDiskManager disk_manager;

        init_import_sstable_succ(import_sstable, disk_manager);

        // clear and reuse it
        import_sstable.clear(false);
        init_import_sstable_succ(import_sstable, disk_manager);

        // clear 
        import_sstable.clear();
        not_init_check(import_sstable);
      }
    }//end namespace chunkserver
  }//end namespace tests
}//end namespace oceanbase

int main(int argc, char** argv)
{
  ob_init_memory_pool();
  TBSYS_LOGGER.setLogLevel("WARN");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
