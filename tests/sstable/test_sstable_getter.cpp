/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * test_sstable_getter.cpp for test sstble getter 
 *
 * Authors: 
 *   huating <huating.zmq@taobao.com>
 *
 */
#include <tblog.h>
#include <gtest/gtest.h>
#include "common/ob_action_flag.h"
#include "common/thread_buffer.h"
#include "common/page_arena.h"
#include "sstable/ob_sstable_reader.h"
#include "sstable/ob_sstable_writer.h"
#include "sstable/ob_sstable_getter.h"
#include "sstable/ob_disk_path.h"
#include "chunkserver/ob_tablet_manager.h"

using namespace std;
using namespace oceanbase::common;
using namespace oceanbase::sstable;
using namespace oceanbase::chunkserver;

namespace oceanbase
{
  namespace tests
  {
    namespace sstable 
    {
      static const int64_t table_id = 100;
      static const int64_t sstable_file_id = 1001;
      static const int64_t sstable_file_offset = 0;
      static const int64_t DISK_NUM = 12;
      static const int64_t SSTABLE_NUM = DISK_NUM * 2;
      static const int64_t SSTABLE_ROW_NUM = 100;
      static const int64_t ROW_NUM = SSTABLE_NUM * SSTABLE_ROW_NUM;
      static const int64_t COL_NUM = 5;
      static const int64_t NON_EXISTENT_ROW_NUM = 100;
      static const ObString table_name(strlen("sstable") + 1, strlen("sstable") + 1, (char*)"sstable");
      static const int64_t OB_MAX_GET_COLUMN_NUMBER = 128;
      static const int64_t OB_MAX_GET_COLUMN_PER_ROW = ObGetParam::MAX_CELLS_PER_ROW;

      static char sstable_file_path[OB_MAX_FILE_NAME_LENGTH];
      static ObCellInfo** cell_infos;
      static char* row_key_strs[ROW_NUM + NON_EXISTENT_ROW_NUM][COL_NUM];
      static ObTabletManager tablet_mgr;

      static ModulePageAllocator mod(0);
      static ModuleArena allocator(ModuleArena::DEFAULT_PAGE_SIZE, mod);
      static ObSSTableReader sstable(allocator, tablet_mgr.get_fileinfo_cache());

      class TestObSSTableGetter : public ::testing::Test
      {
      public:
        void check_string(const ObString& expected, const ObString& real)
        {
          EXPECT_EQ(expected.length(), real.length());
          if (NULL != expected.ptr() && NULL != real.ptr())
          {
            EXPECT_EQ(0, memcmp(expected.ptr(), real.ptr(), expected.length()));
          }
          else
          {
            EXPECT_EQ((const char*) NULL, expected.ptr());
            EXPECT_EQ((const char*) NULL, real.ptr());
          }
        }
        
        void check_obj(const ObObj& real, int type, int64_t exp_val)
        {
          int64_t real_val;

          if (ObMinType != type)
          {
            EXPECT_EQ(type, real.get_type());
          }
          if (ObIntType == type)
          {
            real.get_int(real_val);
            EXPECT_EQ(exp_val, real_val);
          }
        }
      
        void check_cell(const ObCellInfo& expected, const ObCellInfo& real, 
                        int type=ObIntType, uint64_t column_id = UINT64_MAX)
        {
          int64_t exp_val = 0;

          if (UINT64_MAX == column_id)
          {
            ASSERT_EQ(expected.column_id_, real.column_id_);
          }
          else
          {
            ASSERT_EQ(column_id, real.column_id_);
          }
          ASSERT_EQ(expected.table_id_, real.table_id_);
          check_string(expected.row_key_, real.row_key_);

          if (ObIntType == type)
          {
            expected.value_.get_int(exp_val);
          }
          check_obj(real.value_, type, exp_val);
        }

        int reset_thread_local_buffer()
        {
          int ret = OB_SUCCESS;

          static ModulePageAllocator mod_allocator(ObModIds::OB_THREAD_BUFFER);
          static const int64_t QUERY_INTERNAL_PAGE_SIZE = 2 * 1024 * 1024;

          ModuleArena* internal_buffer_arena = GET_TSI_MULT(ModuleArena, TSI_SSTABLE_MODULE_ARENA_1);
          if (NULL == internal_buffer_arena)
          {
            TBSYS_LOG(ERROR, "cannot get tsi object of PageArena");
            ret = OB_ALLOCATE_MEMORY_FAILED;
          }
          else
          {
            internal_buffer_arena->set_page_size(QUERY_INTERNAL_PAGE_SIZE);
            internal_buffer_arena->set_page_alloctor(mod_allocator);
            internal_buffer_arena->reuse();
          }

          return ret;
        }
        
        void test_adjacent_row_query(const int32_t row_index = 0, 
                                     const int32_t row_count = 1,
                                     const int32_t column_count = COL_NUM,
                                     bool same_row = false)
        {
          int ret = OB_SUCCESS;
          ObSSTableGetter getter;
          ObGetParam get_param;
          ObSSTableReader* readers[OB_MAX_GET_COLUMN_NUMBER];
          int64_t readers_size = row_count;
          ObCellInfo *cell = NULL;
          bool row_change = false;
          int index = 0;

          for (int i = row_index; i < row_index + row_count; i++)
          {
            index = same_row ? row_index : i;
            for (int j = 0; j < column_count; j++)
            {
              ret = get_param.add_cell(cell_infos[index][j]);
              EXPECT_EQ(OB_SUCCESS, ret);
            }
            readers[index - row_index] = &sstable;
          }

          if (same_row)
          {
            readers_size = row_count * column_count / OB_MAX_GET_COLUMN_PER_ROW;
            if (row_count * column_count % OB_MAX_GET_COLUMN_PER_ROW > 0)
            {
              readers_size += 1;
            }

            for (int64_t i =0; i < readers_size; ++i)
            {
              readers[i] = &sstable;
            }
          }

          ret = reset_thread_local_buffer();
          ASSERT_EQ(OB_SUCCESS, ret);

          ret = getter.init(tablet_mgr.get_serving_block_cache(), 
                            tablet_mgr.get_serving_block_index_cache(), 
                            get_param, readers, readers_size);
          ASSERT_EQ(OB_SUCCESS, ret);

          if (same_row)
          {
            int32_t start_row_index = row_index;
            int32_t end_row_index = row_index;
            for (int64_t k = 0; k < readers_size; ++k)
            {
              start_row_index = end_row_index;
              end_row_index = 
                static_cast<int32_t>(row_index + (k + 1) * OB_MAX_GET_COLUMN_PER_ROW / column_count);
              if (end_row_index > row_index + row_count)
              {
                end_row_index = row_index + row_count;
              }
              for (int i = start_row_index; i < end_row_index && column_count >= 2; i++)
              {
                for (int j = 0; j < 2; j++)
                {
                  ret = getter.next_cell();
                  EXPECT_EQ(OB_SUCCESS, ret);
                  ret = getter.get_cell(&cell, &row_change);
                  EXPECT_EQ(OB_SUCCESS, ret);
                  EXPECT_NE((ObCellInfo*)NULL, cell);
                  check_cell(cell_infos[row_index][j], *cell);
                  if (start_row_index == i && 0 == j)
                  {
                    EXPECT_TRUE(row_change);
                  }
                  else
                  {
                    EXPECT_TRUE(!row_change);
                  }
                }
              }
              for (int i = start_row_index; i < end_row_index && column_count > 2; i++)
              {
                for (int j = 2; j < column_count; j++)
                {
                  ret = getter.next_cell();
                  EXPECT_EQ(OB_SUCCESS, ret);
                  ret = getter.get_cell(&cell, &row_change);
                  EXPECT_EQ(OB_SUCCESS, ret);
                  EXPECT_NE((ObCellInfo*)NULL, cell);
                  check_cell(cell_infos[row_index][j], *cell);
                  EXPECT_TRUE(!row_change);
                }
              }
            }
          }
          else
          {
            for (int i = row_index; i < row_index + row_count; i++)
            {
              for (int j = 0; j < column_count; j++)
              {
                ret = getter.next_cell();
                EXPECT_EQ(OB_SUCCESS, ret);
                ret = getter.get_cell(&cell, &row_change);
                EXPECT_EQ(OB_SUCCESS, ret);
                EXPECT_NE((ObCellInfo*)NULL, cell);
                check_cell(cell_infos[i][j], *cell);
                if (0 == (i * column_count + j) % column_count)
                {
                  EXPECT_TRUE(row_change);
                }
                else
                {
                  EXPECT_TRUE(!row_change);
                }
              }
            }
          }

          ret = getter.next_cell();
          ASSERT_EQ(OB_ITER_END, ret);
          ret = getter.next_cell();
          ASSERT_EQ(OB_ITER_END, ret);
        }

        void test_full_row_query(const int32_t row_index = 0, 
                                 const int32_t row_count = 1)
        {
          int ret = OB_SUCCESS;
          ObSSTableGetter getter;
          ObGetParam get_param;
          ObSSTableReader* readers[OB_MAX_GET_COLUMN_NUMBER];
          int64_t readers_size = row_count;
          ObCellInfo *cell = NULL;
          ObCellInfo param_cell;
          bool row_change = false;

          for (int i = row_index; i < row_index + row_count; i++)
          {
            param_cell = cell_infos[i][0];
            param_cell.column_id_ = 0;
            ret = get_param.add_cell(param_cell);
            EXPECT_EQ(OB_SUCCESS, ret);
            readers[i - row_index] = &sstable;
          }

          ret = reset_thread_local_buffer();
          ASSERT_EQ(OB_SUCCESS, ret);

          ret = getter.init(tablet_mgr.get_serving_block_cache(), 
                            tablet_mgr.get_serving_block_index_cache(), 
                            get_param, readers, readers_size);
            ASSERT_EQ(OB_SUCCESS, ret);

          for (int i = row_index; i < row_index + row_count; i++)
          {
            for (int j = 0; j < COL_NUM; j++)
            {
              ret = getter.next_cell();
              EXPECT_EQ(OB_SUCCESS, ret);
              ret = getter.get_cell(&cell, &row_change);
              EXPECT_EQ(OB_SUCCESS, ret);
              EXPECT_NE((ObCellInfo*)NULL, cell);
              check_cell(cell_infos[i][j], *cell);
              if (0 == (i * COL_NUM + j) % COL_NUM)
              {
                EXPECT_TRUE(row_change);
              }
              else
              {
                EXPECT_TRUE(!row_change);
              }
            }
          }

          ret = getter.next_cell();
          EXPECT_EQ(OB_ITER_END, ret);
          ret = getter.next_cell();
          EXPECT_EQ(OB_ITER_END, ret);
        }

        void test_nonexistent_row_query(const int32_t row_index = 0, 
                                        const int32_t row_count = 1,
                                        const int32_t column_count = COL_NUM,
                                        const int32_t nonexistent_row_index = 0, 
                                        const int32_t nonexistent_row_count = 1,
                                        const int32_t nonexistent_column_count = COL_NUM)
        {
          int ret = OB_SUCCESS;
          ObSSTableGetter getter;
          ObGetParam get_param;
          ObSSTableReader* readers[OB_MAX_GET_COLUMN_NUMBER];
          int64_t readers_size = row_count + nonexistent_row_count;
          ObCellInfo *cell = NULL;
          ObCellInfo param_cell;
          int64_t flag;
          bool row_change = false;

          for (int i = nonexistent_row_index; 
               i < nonexistent_row_index + nonexistent_row_count; i++)
          {
            for (int j = 0; j < nonexistent_column_count; j++)
            {
              ret = get_param.add_cell(cell_infos[i][j]);
              EXPECT_EQ(OB_SUCCESS, ret);
            }
            readers[row_count + i - nonexistent_row_index] = &sstable;
          }

          for (int i = row_index; i < row_index + row_count; i++)
          {
            for (int j = 0; j < column_count; j++)
            {
              ret = get_param.add_cell(cell_infos[i][j]);
              EXPECT_EQ(OB_SUCCESS, ret);
            }
            readers[i - row_index] = &sstable;
          }

          ret = reset_thread_local_buffer();
          ASSERT_EQ(OB_SUCCESS, ret);

          ret = getter.init(tablet_mgr.get_serving_block_cache(), 
                            tablet_mgr.get_serving_block_index_cache(), 
                            get_param, readers, readers_size);
          ASSERT_EQ(OB_SUCCESS, ret);

          for (int i = nonexistent_row_index; 
               i < nonexistent_row_index + nonexistent_row_count; i++)
          {
            ret = getter.next_cell();
            EXPECT_EQ(OB_SUCCESS, ret);
            ret = getter.get_cell(&cell, &row_change);
            EXPECT_EQ(OB_SUCCESS, ret);
            EXPECT_NE((ObCellInfo*)NULL, cell);
            param_cell = cell_infos[i][0];
            param_cell.column_id_ = 0;
            check_cell(param_cell, *cell, ObMinType);
            ret = cell->value_.get_ext(flag);
            EXPECT_EQ(OB_SUCCESS, ret);
            EXPECT_EQ((int64_t)ObActionFlag::OP_ROW_DOES_NOT_EXIST, flag);
            EXPECT_TRUE(row_change);
          }

          for (int i = row_index; i < row_index + row_count; i++)
          {
            for (int j = 0; j < column_count; j++)
            {
              ret = getter.next_cell();
              EXPECT_EQ(OB_SUCCESS, ret);
              ret = getter.get_cell(&cell, &row_change);
              EXPECT_EQ(OB_SUCCESS, ret);
              EXPECT_NE((ObCellInfo*)NULL, cell);
              check_cell(cell_infos[i][j], *cell);
              if (0 == (i * column_count + j) % column_count)
              {
                EXPECT_TRUE(row_change);
              }
              else
              {
                EXPECT_TRUE(!row_change);
              }
            }
          }

          ret = getter.next_cell();
          EXPECT_EQ(OB_ITER_END, ret);
          ret = getter.next_cell();
          EXPECT_EQ(OB_ITER_END, ret);
        }

        void test_nonexistent_full_row_query(const int32_t row_index = 0, 
                                             const int32_t row_count = 1,
                                             const int32_t column_count = COL_NUM,
                                             const int32_t nonexistent_row_index = 0, 
                                             const int32_t nonexistent_row_count = 1)
        {
          int ret = OB_SUCCESS;
          ObSSTableGetter getter;
          ObGetParam get_param;
          ObSSTableReader* readers[OB_MAX_GET_COLUMN_NUMBER];
          int64_t readers_size = row_count + nonexistent_row_count;
          ObCellInfo *cell = NULL;
          ObCellInfo param_cell;
          int64_t flag;
          bool row_change = false;

          for (int i = row_index; i < row_index + row_count; i++)
          {
            for (int j = 0; j < column_count; j++)
            {
              ret = get_param.add_cell(cell_infos[i][j]);
              EXPECT_EQ(OB_SUCCESS, ret);
            }
            readers[i - row_index] = &sstable;
          }

          for (int i = nonexistent_row_index; 
               i < nonexistent_row_index + nonexistent_row_count; i++)
          {
            param_cell = cell_infos[i][0];
            param_cell.column_id_ = 0;
            ret = get_param.add_cell(param_cell);
            EXPECT_EQ(OB_SUCCESS, ret);
            readers[row_count + i - nonexistent_row_index] = &sstable;
          }

          ret = reset_thread_local_buffer();
          ASSERT_EQ(OB_SUCCESS, ret);

          ret = getter.init(tablet_mgr.get_serving_block_cache(), 
                            tablet_mgr.get_serving_block_index_cache(), 
                            get_param, readers, readers_size);
          ASSERT_EQ(OB_SUCCESS, ret);

          for (int i = row_index; i < row_index + row_count; i++)
          {
            for (int j = 0; j < column_count; j++)
            {
              ret = getter.next_cell();
              EXPECT_EQ(OB_SUCCESS, ret);
              ret = getter.get_cell(&cell, &row_change);
              EXPECT_EQ(OB_SUCCESS, ret);
              EXPECT_NE((ObCellInfo*)NULL, cell);
              check_cell(cell_infos[i][j], *cell);
              if (0 == (i * column_count + j) % column_count)
              {
                EXPECT_TRUE(row_change);
              }
              else
              {
                EXPECT_TRUE(!row_change);
              }
            }
          }

          for (int i = nonexistent_row_index; 
               i < nonexistent_row_index + nonexistent_row_count; i++)
          {
            ret = getter.next_cell();
            EXPECT_EQ(OB_SUCCESS, ret);
            ret = getter.get_cell(&cell, &row_change);
            EXPECT_EQ(OB_SUCCESS, ret);
            EXPECT_NE((ObCellInfo*)NULL, cell);
            param_cell = cell_infos[i][0];
            param_cell.column_id_ = 0;
            check_cell(param_cell, *cell, ObMinType);
            ret = cell->value_.get_ext(flag);
            EXPECT_EQ(OB_SUCCESS, ret);
            EXPECT_EQ((int64_t)ObActionFlag::OP_ROW_DOES_NOT_EXIST, flag);
            EXPECT_TRUE(row_change);
          }

          ret = getter.next_cell();
          EXPECT_EQ(OB_ITER_END, ret);
          ret = getter.next_cell();
          EXPECT_EQ(OB_ITER_END, ret);
        }

        void test_nonexistent_column_query(const int32_t row_index = 0, 
                                           const int32_t row_count = 1,
                                           const int32_t column_count = 0,
                                           const int32_t nonexistent_column_count = COL_NUM)
        {
          int ret = OB_SUCCESS;
          ObSSTableGetter getter;
          ObGetParam get_param;
          ObSSTableReader* readers[OB_MAX_GET_COLUMN_NUMBER];
          int64_t readers_size = row_count;
          ObCellInfo *cell = NULL;
          ObCellInfo param_cell;
          bool row_change = false;

          for (int i = row_index; i < row_index + row_count; i++)
          {
            for (int j = 0; j < column_count; j++)
            {
              ret = get_param.add_cell(cell_infos[i][j]);
              EXPECT_EQ(OB_SUCCESS, ret);
            }
            for (int j = column_count; j < column_count + nonexistent_column_count; j++)
            {
              param_cell = cell_infos[i][j];
              param_cell.column_id_ = COL_NUM + 10; 
              ret = get_param.add_cell(param_cell);
              EXPECT_EQ(OB_SUCCESS, ret);
            }
            readers[i - row_index] = &sstable;
          }

          ret = reset_thread_local_buffer();
          ASSERT_EQ(OB_SUCCESS, ret);

          ret = getter.init(tablet_mgr.get_serving_block_cache(), 
                            tablet_mgr.get_serving_block_index_cache(), 
                            get_param, readers, readers_size);
          ASSERT_EQ(OB_SUCCESS, ret);

          for (int i = row_index; i < row_index + row_count; i++)
          {
            for (int j = 0; j < column_count + nonexistent_column_count; j++)
            {
              ret = getter.next_cell();
              EXPECT_EQ(OB_SUCCESS, ret);
              ret = getter.get_cell(&cell, &row_change);
              EXPECT_EQ(OB_SUCCESS, ret);
              EXPECT_NE((ObCellInfo*)NULL, cell);
              //when reading the first column group, handle non-existent column
              if (column_count <= 2 && j < column_count)
              {
                check_cell(cell_infos[i][j], *cell);
              }
              else if (column_count > 2 && j >= 2 && j < 2 + nonexistent_column_count)
              {
                check_cell(cell_infos[i][j], *cell, ObNullType, COL_NUM + 10);
              }
              else if (column_count > 2 && j >= 2 + nonexistent_column_count)
              {
                check_cell(cell_infos[i][j - nonexistent_column_count], *cell);
              }

              if (0 == (i * (column_count + nonexistent_column_count) + j) 
                  % (column_count + nonexistent_column_count))
              {
                EXPECT_TRUE(row_change);
              }
              else
              {
                EXPECT_TRUE(!row_change);
              }
            }
          }

          ret = getter.next_cell();
          EXPECT_EQ(OB_ITER_END, ret);
          ret = getter.next_cell();
          EXPECT_EQ(OB_ITER_END, ret);
        }

      public:
        static int init_mgr()
        {
          int err = OB_SUCCESS;
  
          ObBlockCacheConf conf;
          conf.block_cache_memsize_mb = 1024;
          conf.ficache_max_num = 1024;
  
          ObBlockIndexCacheConf bic_conf;
          bic_conf.cache_mem_size = 128 * 1024 * 1024;
  
          err = tablet_mgr.init(conf, bic_conf);
          EXPECT_EQ(OB_SUCCESS, err);
  
          return err;
        }
  
        static int init_sstable(ObSSTableReader& sstable, const ObCellInfo** cell_infos,
            const int64_t row_num, const int64_t col_num, const int64_t sst_id = 0L)
        {
          int err = OB_SUCCESS;
  
          ObSSTableSchema sstable_schema;
          ObSSTableSchemaColumnDef column_def;
  
          EXPECT_TRUE(NULL != cell_infos);
          EXPECT_TRUE(row_num > 0);
          EXPECT_TRUE(col_num > 0);

          uint64_t table_id = cell_infos[0][0].table_id_;
          ObString path;
          int64_t sstable_file_id = 0;
          ObString compress_name;
          char* path_str = sstable_file_path;
          int64_t path_len = OB_MAX_FILE_NAME_LENGTH;

          for (int64_t i = 0; i < col_num; ++i)
          {
            column_def.reserved_ = 0;
            if (i >=2)
            {
              column_def.column_group_id_= 2;
            }
            else
            {
              column_def.column_group_id_= 0;
            }
            column_def.column_name_id_ = static_cast<uint32_t>(cell_infos[0][i].column_id_);
            column_def.column_value_type_ = cell_infos[0][i].value_.get_type();
            column_def.table_id_ = static_cast<uint32_t>(table_id);
            sstable_schema.add_column_def(column_def);
          }
  
          if (0 == sst_id)
          {
            sstable_file_id = 100;
          }
          else
          {
            sstable_file_id = sst_id;
          }
  
          ObSSTableId sstable_id(sst_id);
          get_sstable_path(sstable_id, path_str, path_len);
          char cmd[256];
          sprintf(cmd, "mkdir -p %s", path_str);
          system(cmd);
          path.assign((char*)path_str, static_cast<int32_t>(strlen(path_str)));
          compress_name.assign((char*)"lzo_1.0", static_cast<int32_t>(strlen("lzo_1.0")));
          remove(path.ptr());

          ObSSTableWriter writer;
          err = writer.create_sstable(sstable_schema, path, compress_name, 0);
          EXPECT_EQ(OB_SUCCESS, err);
  
          for (int64_t i = 0; i < row_num; ++i)
          {
            ObSSTableRow row;
            row.set_table_id(table_id);
            row.set_column_group_id(0);
            err = row.set_row_key(cell_infos[i][0].row_key_);
            EXPECT_EQ(OB_SUCCESS, err);
            for (int64_t j = 0; j < 2; ++j)
            {
              err = row.add_obj(cell_infos[i][j].value_);
              EXPECT_EQ(OB_SUCCESS, err);
            }
  
            int64_t space_usage = 0;
            err = writer.append_row(row, space_usage);
            EXPECT_EQ(OB_SUCCESS, err);
          }

          for (int64_t i = 0; i < row_num; ++i)
          {
            ObSSTableRow row;
            row.set_table_id(table_id);
            row.set_column_group_id(2);
            err = row.set_row_key(cell_infos[i][0].row_key_);
            EXPECT_EQ(OB_SUCCESS, err);
            for (int64_t j = 2; j < col_num; ++j)
            {
              err = row.add_obj(cell_infos[i][j].value_);
              EXPECT_EQ(OB_SUCCESS, err);
            }
  
            int64_t space_usage = 0;
            err = writer.append_row(row, space_usage);
            EXPECT_EQ(OB_SUCCESS, err);
          }
  
          int64_t offset = 0;
          err = writer.close_sstable(offset);
          EXPECT_EQ(OB_SUCCESS, err);
  
          err = sstable.open(sstable_id);
          EXPECT_EQ(OB_SUCCESS, err);
          EXPECT_TRUE(sstable.is_opened());
  
          return err;
        }
  
      public:
        static void SetUpTestCase()
        {
          int err = OB_SUCCESS;

          TBSYS_LOGGER.setLogLevel("ERROR");
          err = ob_init_memory_pool();
          ASSERT_EQ(OB_SUCCESS, err);
          err = init_mgr();
          ASSERT_EQ(OB_SUCCESS, err);
      
          //malloc
          cell_infos = new ObCellInfo*[ROW_NUM + NON_EXISTENT_ROW_NUM];
          for (int64_t i = 0; i < ROW_NUM + NON_EXISTENT_ROW_NUM; ++i)
          {
            cell_infos[i] = new ObCellInfo[COL_NUM];
          }
      
          for (int64_t i = 0; i < ROW_NUM + NON_EXISTENT_ROW_NUM; ++i)
          {
            for (int64_t j = 0; j < COL_NUM; ++j)
            {
              row_key_strs[i][j] = new char[50];
            }
          }
      
          // init cell infos
          for (int64_t i = 0; i < ROW_NUM + NON_EXISTENT_ROW_NUM; ++i)
          {
            for (int64_t j = 0; j < COL_NUM; ++j)
            {
              cell_infos[i][j].table_id_ = table_id;
              sprintf(row_key_strs[i][j], "row_key_%08ld", i);
              cell_infos[i][j].row_key_.assign(row_key_strs[i][j], static_cast<int32_t>(strlen(row_key_strs[i][j])));
              cell_infos[i][j].column_id_ = j + 2;
              cell_infos[i][j].value_.set_int(1000 + i * COL_NUM + j);
            }
          }
      
          //init sstable
          err = init_sstable(sstable, (const ObCellInfo**)cell_infos, 
                             ROW_NUM, COL_NUM, sstable_file_id);
          EXPECT_EQ(OB_SUCCESS, err);
        }
      
        static void TearDownTestCase()
        {
          for (int64_t i = 0; i < ROW_NUM + NON_EXISTENT_ROW_NUM; ++i)
          {
            for (int64_t j = 0; j < COL_NUM; ++j)
            {
              if (NULL != row_key_strs[i][j])
              {
                delete[] row_key_strs[i][j];
                row_key_strs[i][j] = NULL;
              }
            }
          }
      
          for (int64_t i = 0; i < ROW_NUM + NON_EXISTENT_ROW_NUM; ++i)
          {
            if (NULL != cell_infos[i])
            {
              delete[] cell_infos[i];
              cell_infos[i] = NULL;
            }
          }
          if (NULL != cell_infos)
          {
            delete[] cell_infos;
          }

          sstable.reset();
        }
      
        virtual void SetUp()
        {
  
        }
      
        virtual void TearDown()
        {
      
        }
      };

      TEST_F(TestObSSTableGetter, test_get_first_row)
      {
        test_adjacent_row_query(0, 1, 1);
        test_adjacent_row_query(0, 1, 2);
        test_adjacent_row_query(0, 1, 3);
        test_adjacent_row_query(0, 1, 4);
        test_adjacent_row_query(0, 1, 5);
      }

      TEST_F(TestObSSTableGetter, test_get_middle_row)
      {
        test_adjacent_row_query((ROW_NUM - 1) / 2, 1, 1);
        test_adjacent_row_query((ROW_NUM - 1) / 2, 1, 2);
        test_adjacent_row_query((ROW_NUM - 1) / 2, 1, 3);
        test_adjacent_row_query((ROW_NUM - 1) / 2, 1, 4);
        test_adjacent_row_query((ROW_NUM - 1) / 2, 1, 5);
      }

      TEST_F(TestObSSTableGetter, test_get_last_row)
      {
        test_adjacent_row_query(ROW_NUM - 1, 1, 1);
        test_adjacent_row_query(ROW_NUM - 1, 1, 2);
        test_adjacent_row_query(ROW_NUM - 1, 1, 3);
        test_adjacent_row_query(ROW_NUM - 1, 1, 4);
        test_adjacent_row_query(ROW_NUM - 1, 1, 5);
      }

      TEST_F(TestObSSTableGetter, test_get_x_columns_two_rows)
      {
        test_adjacent_row_query(0, 2, 1);
        test_adjacent_row_query(0, 2, 2);
        test_adjacent_row_query(0, 2, 3);
        test_adjacent_row_query(0, 2, 4);
        test_adjacent_row_query(0, 2, 5);
      }

      TEST_F(TestObSSTableGetter, test_get_x_columns_ten_rows)
      {
        test_adjacent_row_query(0, 10, 1);
        test_adjacent_row_query(0, 10, 2);
        test_adjacent_row_query(0, 10, 3);
        test_adjacent_row_query(0, 10, 4);
        test_adjacent_row_query(0, 10, 5);
      }

      TEST_F(TestObSSTableGetter, test_get_x_columns_hundred_rows)
      {
        test_adjacent_row_query(0, 100, 1);
      }

      TEST_F(TestObSSTableGetter, test_get_max_columns)
      {
        test_adjacent_row_query(0, OB_MAX_GET_COLUMN_NUMBER, 1);
      }

      TEST_F(TestObSSTableGetter, test_get_one_row)
      {
        test_full_row_query(0, 1);
        test_full_row_query(ROW_NUM / 2, 1);
        test_full_row_query(ROW_NUM - 1, 1);
      }

      TEST_F(TestObSSTableGetter, test_get_two_rows)
      {
        test_full_row_query(0, 2);
        test_full_row_query(ROW_NUM / 2, 2);
        test_full_row_query(ROW_NUM - 2, 2);
      }

      TEST_F(TestObSSTableGetter, test_get_ten_rows)
      {
        test_full_row_query(0, 10);
        test_full_row_query(ROW_NUM / 2, 10);
        test_full_row_query(ROW_NUM - 10, 10);
      }

      TEST_F(TestObSSTableGetter, test_get_one_nonexistent_row)
      {
        test_nonexistent_row_query(0, 0, 0, ROW_NUM, 1, 1);
        test_nonexistent_row_query(0, 1, COL_NUM, ROW_NUM, 1, 1);
        test_nonexistent_row_query(ROW_NUM / 2, 1, COL_NUM, ROW_NUM, 1, 1);
        test_nonexistent_row_query(ROW_NUM - 1, 1, COL_NUM, ROW_NUM, 1, 1);
        test_nonexistent_row_query(0, 2, COL_NUM, ROW_NUM, 1, 1);
        test_nonexistent_row_query(0, 10, COL_NUM, ROW_NUM, 1, 1);

        test_nonexistent_row_query(0, 0, 0, ROW_NUM, 1, COL_NUM);
        test_nonexistent_row_query(0, 1, COL_NUM, ROW_NUM, 1, COL_NUM);
        test_nonexistent_row_query(ROW_NUM / 2, 1, COL_NUM, ROW_NUM, 1, COL_NUM);
        test_nonexistent_row_query(ROW_NUM - 1, 1, COL_NUM, ROW_NUM, 1, COL_NUM);
        test_nonexistent_row_query(0, 2, COL_NUM, ROW_NUM, 1, COL_NUM);
        test_nonexistent_row_query(0, 10, COL_NUM, ROW_NUM, 1, COL_NUM);
      }

      TEST_F(TestObSSTableGetter, test_get_two_nonexistent_rows)
      {
        test_nonexistent_row_query(0, 0, 0, ROW_NUM, 2, 1);
        test_nonexistent_row_query(0, 1, COL_NUM, ROW_NUM, 2, 1);
        test_nonexistent_row_query(ROW_NUM / 2, 1, COL_NUM, ROW_NUM, 2, 1);
        test_nonexistent_row_query(ROW_NUM - 1, 1, COL_NUM, ROW_NUM, 2, 1);
        test_nonexistent_row_query(0, 2, COL_NUM, ROW_NUM, 2, 1);
        test_nonexistent_row_query(0, 10, COL_NUM, ROW_NUM, 2, 1);

        test_nonexistent_row_query(0, 0, 0, ROW_NUM, 2, COL_NUM);
        test_nonexistent_row_query(0, 1, COL_NUM, ROW_NUM, 2, COL_NUM);
        test_nonexistent_row_query(ROW_NUM / 2, 1, COL_NUM, ROW_NUM, 2, COL_NUM);
        test_nonexistent_row_query(ROW_NUM - 1, 1, COL_NUM, ROW_NUM, 2, COL_NUM);
        test_nonexistent_row_query(0, 2, COL_NUM, ROW_NUM, 2, COL_NUM);
        test_nonexistent_row_query(0, 10, COL_NUM, ROW_NUM, 2, COL_NUM);
      }

      TEST_F(TestObSSTableGetter, test_get_ten_nonexistent_rows)
      {
        test_nonexistent_row_query(0, 0, 0, ROW_NUM, 10, 1);
        test_nonexistent_row_query(0, 1, COL_NUM, ROW_NUM, 10, 1);
        test_nonexistent_row_query(ROW_NUM / 2, 1, COL_NUM, ROW_NUM, 10, 1);
        test_nonexistent_row_query(ROW_NUM - 1, 1, COL_NUM, ROW_NUM, 10, 1);
        test_nonexistent_row_query(0, 2, COL_NUM, ROW_NUM, 10, 1);
        test_nonexistent_row_query(0, 10, COL_NUM, ROW_NUM, 10, 1);

        test_nonexistent_row_query(0, 0, 0, ROW_NUM, 10, COL_NUM);
        test_nonexistent_row_query(0, 1, COL_NUM, ROW_NUM, 10, COL_NUM);
        test_nonexistent_row_query(ROW_NUM / 2, 1, COL_NUM, ROW_NUM, 10, COL_NUM);
        test_nonexistent_row_query(ROW_NUM - 1, 1, COL_NUM, ROW_NUM, 10, COL_NUM);
        test_nonexistent_row_query(0, 2, COL_NUM, ROW_NUM, 10, COL_NUM);
        test_nonexistent_row_query(0, 10, COL_NUM, ROW_NUM, 10, COL_NUM);
      }

      TEST_F(TestObSSTableGetter, test_get_one_nonexistent_full_row)
      {
        test_nonexistent_full_row_query(0, 0, 0, ROW_NUM, 1);
        test_nonexistent_full_row_query(0, 1, COL_NUM, ROW_NUM);
        test_nonexistent_full_row_query(ROW_NUM / 2, 1, COL_NUM, ROW_NUM, 1);
        test_nonexistent_full_row_query(ROW_NUM - 1, 1, COL_NUM, ROW_NUM, 1);
        test_nonexistent_full_row_query(0, 2, COL_NUM, ROW_NUM, 1);
        test_nonexistent_full_row_query(0, 10, COL_NUM, ROW_NUM, 1);
      }

      TEST_F(TestObSSTableGetter, test_get_two_nonexistent_full_rows)
      {
        test_nonexistent_full_row_query(0, 0, 0, ROW_NUM, 2);
        test_nonexistent_full_row_query(0, 1, COL_NUM, ROW_NUM, 2);
        test_nonexistent_full_row_query(ROW_NUM / 2, 1, COL_NUM, ROW_NUM, 2);
        test_nonexistent_full_row_query(ROW_NUM - 1, 1, COL_NUM, ROW_NUM, 2);
        test_nonexistent_full_row_query(0, 2, COL_NUM, ROW_NUM, 2);
        test_nonexistent_full_row_query(0, 10, COL_NUM, ROW_NUM, 2);
      }

      TEST_F(TestObSSTableGetter, test_get_ten_nonexistent_full_row)
      {
        test_nonexistent_full_row_query(0, 0, 0, ROW_NUM, 10);
        test_nonexistent_full_row_query(0, 1, COL_NUM, ROW_NUM, 10);
        test_nonexistent_full_row_query(ROW_NUM / 2, 1, COL_NUM, ROW_NUM, 10);
        test_nonexistent_full_row_query(ROW_NUM - 1, 1, COL_NUM, ROW_NUM, 10);
        test_nonexistent_full_row_query(0, 2, COL_NUM, ROW_NUM, 10);
        test_nonexistent_full_row_query(0, 10, COL_NUM, ROW_NUM, 10);
      }

      TEST_F(TestObSSTableGetter, test_get_one_nonexistent_column)
      {
        test_nonexistent_column_query(0, 1, 0, 1);
        test_nonexistent_column_query(0, 1, 1, 1);
        test_nonexistent_column_query(0, 1, 4, 1);
        test_nonexistent_column_query(0, 2, 0, 1);
        test_nonexistent_column_query(0, 2, 1, 1);
        test_nonexistent_column_query(0, 2, 4, 1);
        test_nonexistent_column_query(0, 10, 0, 1);
        test_nonexistent_column_query(0, 10, 1, 1);
        test_nonexistent_column_query(0, 10, 4, 1);
      }

      TEST_F(TestObSSTableGetter, test_get_two_nonexistent_columns)
      {
        test_nonexistent_column_query(0, 1, 0, 2);
        test_nonexistent_column_query(0, 1, 1, 2);
        test_nonexistent_column_query(0, 1, 3, 2);
        test_nonexistent_column_query(0, 2, 0, 2);
        test_nonexistent_column_query(0, 2, 1, 2);
        test_nonexistent_column_query(0, 2, 3, 2);
        test_nonexistent_column_query(0, 10, 0, 2);
        test_nonexistent_column_query(0, 10, 1, 2);
        test_nonexistent_column_query(0, 10, 3, 2);
      }

      TEST_F(TestObSSTableGetter, test_get_max_nonexistent_columns)
      {
        test_nonexistent_column_query(0, 1, 0, COL_NUM);
        test_nonexistent_column_query(0, 2, 0, COL_NUM);
        test_nonexistent_column_query(0, 10, 0, COL_NUM);
      }

      TEST_F(TestObSSTableGetter, test_get_all_nonexistent_columns)
      {
        int32_t row_count = 20;

        for (int i = 0; i < ROW_NUM / row_count; i++)
        {
          test_nonexistent_column_query(row_count * i, row_count, 0, COL_NUM);
        }
      }
      
      TEST_F(TestObSSTableGetter, test_get_all_columns)
      {
        int32_t row_count = 20;

        for (int i = 0; i < ROW_NUM / row_count; i++)
        {
          test_adjacent_row_query(row_count * i, row_count, COL_NUM);
        }
      }

      TEST_F(TestObSSTableGetter, test_get_all_columns_with_full_row_mark)
      {
        int32_t row_count = 20;

        for (int i = 0; i < ROW_NUM / row_count; i++)
        {
          test_full_row_query(row_count * i, row_count);
        }
      }

      TEST_F(TestObSSTableGetter, test_get_all_columns_with_nonexistent_rows)
      {
        int32_t row_count = 10;

        for (int i = 0; i < ROW_NUM / row_count; i++)
        {
          test_nonexistent_row_query(row_count * i, row_count, COL_NUM, ROW_NUM, 10, COL_NUM);
        }
      }

      TEST_F(TestObSSTableGetter, test_get_all_columns_with_nonexistent_full_rows)
      {
        int32_t row_count = 10;

        for (int i = 0; i < ROW_NUM / row_count; i++)
        {
          test_nonexistent_full_row_query(row_count * i, row_count, COL_NUM, ROW_NUM, 10);
        }
      }

      TEST_F(TestObSSTableGetter, test_get_same_row_xtimes)
      {
        int32_t max_row = OB_MAX_GET_COLUMN_PER_ROW + 10;

        for (int i = 0; i < max_row; i++)
        {
          test_adjacent_row_query(0, i + 1, COL_NUM - 1, true);
        }
      }

    }//end namespace sstable
  }//end namespace tests
}//end namespace oceanbase

int main(int argc, char** argv)
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
