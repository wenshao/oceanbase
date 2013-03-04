/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_import_sstable.cpp for import sstable. 
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */
#include "common/file_directory_utils.h"
#include "common/utility.h"
#include "common/ob_file.h"
#include "common/ob_mod_define.h"
#include "sstable/ob_disk_path.h"
#include "ob_chunk_server_main.h"
#include "ob_import_sstable.h"

namespace oceanbase
{
  namespace chunkserver
  {
    using namespace oceanbase::common;
    using namespace oceanbase::common::hash;
    using namespace oceanbase::sstable;

    ObImportSSTable::ObImportSSTable()
    : inited_(false), disk_manager_(NULL), app_name_(NULL),
      range_file_cnt_(0), mod_allocator_(ObModIds::OB_CS_IMPORT_SSTABLE)
    {

    }

    ObImportSSTable::~ObImportSSTable()
    {
      destroy();
    }

    int ObImportSSTable::init(const ObDiskManager& disk_manager, 
      const char* app_name)
    {
      int ret = OB_SUCCESS;

      if (NULL == app_name)
      {
        TBSYS_LOG(WARN, "invalid param, app_name=%p", app_name);
        ret = OB_ERROR;
      }
      else 
      {
        if (!inited_)
        {
          ret = range2file_hash_map_.create(HASH_BUCKET_NUM);
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "fail to create range2file hash table");
          }
          else 
          {
            ret = file2range_hash_map_.create(HASH_BUCKET_NUM);
            if (OB_SUCCESS != ret)
            {
              TBSYS_LOG(WARN, "fail to create file2range hash table");
            }
          }

          if (OB_SUCCESS == ret)
          {
            ret = tableid_hash_map_.create(TABLEID_HASH_BUCKET_NUM);
            if (OB_SUCCESS != ret)
            {
              TBSYS_LOG(WARN, "fail to create table id hash table");
            }
          }
  
          if (OB_SUCCESS == ret)
          {
            range_arena_.set_page_alloctor(mod_allocator_);
            inited_ = true;
          }
        }
        else
        {
          clear(false);
        }

        if (OB_SUCCESS == ret)
        {
          disk_manager_ = &disk_manager;
          app_name_ = app_name;
          ret = build_sstable_hash_map();
        }
      }

      return ret;
    }

    int ObImportSSTable::build_sstable_hash_map()
    {
      int ret = OB_SUCCESS;

      if (!inited_)
      {
        TBSYS_LOG(WARN, "import sstable instance doesn't initialize");
        ret = OB_NOT_INIT;
      }
      else
      {
        ret = scan_range_files(&range_file_name_filter);
        if (OB_NO_IMPORT_SSTABLE == ret)
        {
          // no import sstable
          ret = OB_SUCCESS;
        }
        else if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "failed to scan range files");
        }
        else if (range2file_hash_map_.size() == file2range_hash_map_.size()
                 && file2range_hash_map_.size() > 0)
        {
          ret = scan_sstable_files(&sstable_file_name_filter);
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "failed to scan import sstable files");
          }
        }
      }

      return ret;
    }

    int ObImportSSTable::get_import_sstable_info(const ObRange& range, 
      ObFilePathInfo& sstable_info) const
    {
      int ret = OB_SUCCESS;

      if (!inited_)
      {
        TBSYS_LOG(WARN, "import sstable instance doesn't initialize");
        ret = OB_NOT_INIT;
      }
      else
      {
        ret = range2file_hash_map_.get(range, sstable_info);
        if (HASH_EXIST != ret)
        {
          ret = OB_IMPORT_SSTABLE_NOT_EXIST;
        }
        else 
        {
          ret = OB_SUCCESS;
        }
      }

      return ret;
    }

    bool ObImportSSTable::need_import_sstable(const uint64_t table_id) const
    {
      bool ret = false;
      int err  = OB_SUCCESS;
      int64_t value = 0;

      if (!inited_)
      {
        TBSYS_LOG(WARN, "import sstable instance doesn't initialize");
      }
      else if (OB_INVALID_ID == table_id || 0 == table_id)
      {
        TBSYS_LOG(WARN, "invalid param, table_id=%lu", table_id);
      }
      else if (tableid_hash_map_.size() > 0)
      {
        err = tableid_hash_map_.get(table_id, value);
        if (HASH_EXIST == err)
        {
          ret = true;
        }
        else
        {
          ret = false;
        }
      }

      return ret;
    }

    void ObImportSSTable::clear(bool delete_range_files)
    {
      disk_manager_ = NULL;
      app_name_ = NULL;
      if (inited_)
      {
        if (delete_range_files && range_file_cnt_ > 0)
        {
          for (int64_t i = 0; i < range_file_cnt_; ++i)
          {
            if (!FileDirectoryUtils::delete_file(range_files_[i]))
            {
              TBSYS_LOG(WARN, "failed to delete range file=%s", range_files_[i]);
            }
          }
        }
        range_file_cnt_ = 0;
        memset(range_files_, 0, sizeof(char*) * MAX_TABLE_RANGE_CONFIG_FILES);
        tableid_hash_map_.clear();
        range2file_hash_map_.clear();
        file2range_hash_map_.clear();
        range_arena_.reuse();
      }
    }

    void ObImportSSTable::destroy()
    {
      if (inited_)
      {
        tableid_hash_map_.destroy();
        range2file_hash_map_.destroy();
        file2range_hash_map_.destroy();
      }
      range_arena_.free();
      inited_ = false;
      disk_manager_ = NULL;
      app_name_ = NULL;
      range_file_cnt_ = 0;
    }

    int ObImportSSTable::sstable_file_name_filter(const struct dirent* d)
    {
      uint64_t table_id = OB_INVALID_ID;
      int64_t seq_no = -1;

      /**
       * import sstable name format: 
       *    ex: 1001-000001
       *    1001    table id
       *    -       delimeter '-'
       *    000001  range sequence number, 6 chars
       */
      int num = sscanf(d->d_name, "%lu-%06ld", &table_id, &seq_no);

      return (2 == num && OB_INVALID_ID != table_id && seq_no >= 0) ? 1 : 0;
    }

    int ObImportSSTable::range_file_name_filter(const struct dirent* d)
    {
      int ret = 0;

      /**
       * tablet range file name format: 
       *     ex: app_name-table_name-talbe_id-range.ini
       *     app_name application name
       *     - delimeter '-'
       *     table_name
       *     table_id
       *     range.ini
       */
      if (NULL != d && NULL != d->d_name)
      {
        ret = (NULL != strstr(d->d_name, "range.ini")) ? 1 : 0;
      }

      return ret;
    }

    int ObImportSSTable::scan_range_files(Filter filter)
    {
      int ret                       = OB_SUCCESS;
      int64_t file_num              = 0;
      struct dirent** file_dirent   = NULL;
      bool parse_file               = true;
      int32_t size                  = 0;
      const int32_t* disk_no_array  = NULL;
      char directory[OB_MAX_FILE_NAME_LENGTH];

      /**
       * the disk 1 includes the range files for each table to import, 
       * each table has one rang file to describle the mapping of 
       * tablet range to import sstable file. 
       */
      disk_no_array = disk_manager_->get_disk_no_array(size);
      if (NULL == disk_no_array || size <= 0)
      {
        TBSYS_LOG(WARN, "invalid disk manager, get disk no array failed, "
                        "disk_no_array=%p, size=%d",
          disk_no_array, size);
        ret = OB_ERROR;
      }
      else if (OB_SUCCESS != (ret = get_import_sstable_directory(1, directory, 
        OB_MAX_FILE_NAME_LENGTH)))
      {
        TBSYS_LOG(ERROR, "get sstable import directory error.");
      }
      else if (!FileDirectoryUtils::is_directory(directory))
      {
        TBSYS_LOG(INFO, "import dir doesn't exist, no import sstable, dir=%s", 
          directory);
        ret = OB_NO_IMPORT_SSTABLE;
      }
      else if ((file_num = ::scandir(directory, 
                &file_dirent, filter, ::versionsort)) <= 0 
               || NULL == file_dirent)
      {
        TBSYS_LOG(INFO, "directory=%s doesn't have range files.", directory);
        ret = OB_NO_IMPORT_SSTABLE; 
      }
      else
      {
        for (int64_t n = 0; n < file_num; ++n)
        {
          if (NULL == file_dirent[n])
          {
            TBSYS_LOG(ERROR, "scandir return null dirent[%ld]. directory=%s", 
              n, directory);
            ret = OB_IO_ERROR;
          }
          else
          {
            if (parse_file)
            {
              ret = parse_range_file(directory, file_dirent[n]->d_name);
              if (OB_SUCCESS != ret)
              {
                TBSYS_LOG(WARN, "parse_range_file failed, dir=%s, file_name=%s",
                  directory, file_dirent[n]->d_name);
                parse_file = false;
              }
            }

            ::free(file_dirent[n]);
          }
        }
      }

      if (NULL != file_dirent) 
      {
        ::free(file_dirent);
        file_dirent = NULL;
      }

      return ret;
    }

    int ObImportSSTable::parse_range_file(const char* dir, const char* file_name)
    {
      int ret               = OB_SUCCESS;
      char* file_buf        = NULL;
      int64_t read_size     = 0;
      int64_t pos           = 0;
      char* sstable_name    = NULL;
      char* prev_hexstr_key = NULL;
      char* end_hexstr_key  = NULL;

      TBSYS_LOG(DEBUG, "parse table range file, dir=%s, file_name=%s", 
        dir, file_name);
      ret = read_range_file(dir, file_name, file_buf, read_size);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "failed to read rang file, dir=%s, file_name=%s",
          dir, file_name);
      }
      else
      {
        if (NULL != file_buf && read_size > 0)
        {
          while (pos < read_size && OB_SUCCESS == ret 
                 && OB_SUCCESS == (ret = read_line(file_buf, read_size, pos, 
                   sstable_name, end_hexstr_key)))
          {
            ret = process_line(sstable_name, prev_hexstr_key, end_hexstr_key);
            if (OB_SUCCESS == ret)
            {
              prev_hexstr_key = end_hexstr_key;
            }
          }
        }
      }

      return ret;
    }

    int ObImportSSTable::read_range_file(const char* dir, const char* file_name,
      char*& file_buf, int64_t& read_size)
    {
      int ret               = OB_SUCCESS;
      char* file_path_ptr   = NULL;
      int64_t file_size     = 0;
      int64_t file_path_len = 0;
      char file_path[OB_MAX_FILE_NAME_LENGTH];
      FileComponent::BufferFileReader reader;

      if (NULL == dir || NULL == file_name)
      {
        TBSYS_LOG(WARN, "invalid dir=%p or range file_name=%p", dir, file_name);
        ret = OB_INVALID_ARGUMENT;
      }
      else if (!is_range_file_name_valid(file_name))
      {
        TBSYS_LOG(WARN, "invlid range file name format, file_name=%s", file_name);
        ret = OB_ERROR;
      }
      else {
        file_path_len = snprintf(file_path, OB_MAX_FILE_NAME_LENGTH, 
          "%s/%s", dir, file_name);
        if (!FileDirectoryUtils::exists(file_path))
        {
          TBSYS_LOG(WARN, "range file %s doesn't exist", file_path);
          ret = OB_FILE_NOT_EXIST;
        }
        else 
        {
          file_size = get_file_size(file_path);
          if (file_size < 0)
          {
            TBSYS_LOG(ERROR, "get file size error, file_size=%ld", file_size);
            ret = OB_IO_ERROR;
          }
          else if (0 == file_size)
          {
            TBSYS_LOG(ERROR, "invalid import sstable size error, file_size=%ld", 
              file_size);
            ret = OB_ERROR;
          }
          else
          {
            ObString fname(0, static_cast<int32_t>(strlen(file_path)), file_path);
            if (OB_SUCCESS != (ret = reader.open(fname)))
            {
              TBSYS_LOG(ERROR, "open %s for read error, error=%s.", 
                file_path, strerror(errno));
            }
          }

          if (OB_SUCCESS == ret)
          {
            file_path_ptr = range_arena_.alloc(file_path_len + 1);
            if (NULL == file_path_ptr)
            {
              TBSYS_LOG(ERROR, "failed to allocate memory for range file path");
              ret = OB_ALLOCATE_MEMORY_FAILED;
            }
            else {
              strncpy(file_path_ptr, file_path, file_path_len + 1);
              if (range_file_cnt_ < MAX_TABLE_RANGE_CONFIG_FILES)
              {
                range_files_[range_file_cnt_++] = file_path_ptr;
              }
              else
              {
                TBSYS_LOG(WARN, "too many range files, range_file_cnt=%ld, max_file=%ld",
                  range_file_cnt_, MAX_TABLE_RANGE_CONFIG_FILES);
                ret = OB_SIZE_OVERFLOW;
              }
            }
          }

          if (OB_SUCCESS == ret 
              && OB_SUCCESS == (ret = file_buf_.ensure_space(file_size)))
          {
            file_buf = file_buf_.get_buffer();
          }

          if (OB_SUCCESS == ret)
          {
            ret = reader.pread(file_buf, file_size, 0, read_size);
            if (OB_SUCCESS != ret || read_size < file_size)
            {
              TBSYS_LOG(ERROR, "read range file = %s , ret = %d, "
                               "read_size = %ld, file_size = %ld, error=%s.",
                  file_path, ret, read_size, file_size, strerror(errno));
              ret = OB_IO_ERROR;
            }
            else
            {
              ret = OB_SUCCESS;
            }
          }          
        }
      }

      return ret;
    }

    bool ObImportSSTable::is_range_file_name_valid(const char* file_name)
    {
      bool ret                  = true;
      char* token               = NULL;
      char* dup_file_name       = NULL;
      uint64_t table_id         = OB_INVALID_ID;
      int64_t i                 = 0;
      static const char* suffix = "range.ini";

      /**
       * tablet range file name format: 
       *     ex: app_name-table_name-talbe_id-range.ini
       *     app_name application name
       *     - delimeter '-'
       *     table_name
       *     table_id
       *     range.ini
       */
      if (NULL != file_name)
      {
        dup_file_name = strdup(file_name);
        if (NULL == dup_file_name)
        {
          TBSYS_LOG(WARN, "failed dup str file_name=%s", file_name);
          ret = false;
        }
        else
        {
          while((token = strsep(&dup_file_name, "-")) != NULL) 
          {
            if (token[0] == '\0') continue;
            switch (i)
            {
              case 0:
                if (0 != strncmp(token, app_name_, strlen(app_name_)))
                {
                  TBSYS_LOG(WARN, "this table range file is not for "
                                  "current application, file_name=%s, app_name=%s",
                    file_name, app_name_);
                  ret = false;
                }
                break;
  
              case 1:
                break;
  
              case 2:
                table_id = strtoull(token, NULL, 10);
                if (0 == table_id || OB_INVALID_ID == table_id)
                {
                  TBSYS_LOG(WARN, "invalid table id in range file name, "
                                  "file_name=%s, table_id=%lu",
                    file_name, table_id);
                  ret = false;
                }
                break;
  
              case 3:
                if (0 != strncmp(token, suffix, strlen(suffix)))
                {
                  TBSYS_LOG(WARN, "invalid suffix in range file name, "
                                  "file_name=%s, expected_suffix=%s",
                    file_name, "range.ini");
                  ret = false;
                }
                break;
  
              default:
                TBSYS_LOG(WARN, "invlid range file name format, it includes "
                                "more than 4 parts wihich separated by '-', "
                                "file_name=%s", file_name);
                ret = false;
                break;
            }
  
            if (!ret)
            {
              break;
            }
            i++;
          }
        }
  
        if (4 != i)
        {
          ret = false;
        }
  
        if (NULL != dup_file_name)
        {
          free(dup_file_name);
        }
      }

      return ret;
    }

    int ObImportSSTable::read_line(char* input, const int64_t input_size, 
      int64_t& pos, char*& sstable_name, char*& end_hexstr_key)
    {
      int ret           = OB_SUCCESS;
      char* line_start  = NULL;
      char* phead       = NULL;
      char* ptail       = NULL;
      int i             = 0;
      sstable_name      = NULL;
      end_hexstr_key    = NULL;

      /*
       * range file line format:
       *  1999^A-1 1001-000000 00000000000007CFFFFFFFFFFFFFFFFF 10.232.36.39 10.232.36.38
       *   1999^A-1  rowkey column separate by special delimeter, ex: \1
       *   1001-000001 table id and tablet range number, sstable name
       *   00000000000007CFFFFFFFFFFFFFFFFF binary rowkey for hex format
       *   10.232.36.39  the first chunkserver ip
       *   10.232.36.38  the second chunkserver ip
       *   ...           the third chunkserver ip if it exist
       */
      if (NULL == input || input_size <= 0 || pos >= input_size || pos < 0)
      {
        TBSYS_LOG(WARN, "invalid param, input=%p, input_size=%ld, pos=%ld",
          input, input_size, pos);
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        line_start = input + pos;
        phead = line_start;
        ptail = phead;
        while (*ptail != '\n')
        {
          while((*ptail != ' ') && (*ptail != '\n'))
          {
            ++ptail;
          }

          if (1 == i)
          {
            sstable_name = phead;
          }
          else if (2 == i)
          {
            end_hexstr_key = phead;
          }
          i++;

          if ('\n' == *ptail)
          {
            *ptail= '\0';
            pos += ptail - line_start + 1;
            break;
          }
          else
          {
            *ptail++ = '\0';
          }
          phead = ptail;
        }

        if ('\n' == *ptail)
        {
          pos += ptail - line_start + 1;
        }

        //check 
        if (i < 3)
        {
          TBSYS_LOG(WARN, "invalid range file format, each line must "
                          "have 4 columns at least, actually it has %d columns", i);
          ret = OB_ERROR;
        }
        else if (pos > input_size)
        {
          TBSYS_LOG(WARN, "after read this line, pos overflow, "
                          "pos=%ld, input_size=%ld", 
            pos, input_size);
          ret = OB_ERROR;
        }
      }

      return ret;
    }

    int ObImportSSTable::process_line(const char* sstable_name,  
      const char* start_hexstr_key, const char* end_hexstr_key)
    {
      int ret             = OB_SUCCESS;
      uint64_t table_id   = OB_INVALID_ID;
      int64_t seq_no      = -1;
      const char* end_key = NULL;

      /**
       * needn't check start_hexstr_key and end_hexstr_key, if 
       * start_hexstr_key is NULL, it means the start rowkey is 'min',
       * if end_hexstr_key is NULL, it means the end rowkey is 
       * 'max' 
       */
      if (NULL == sstable_name)
      {
        TBSYS_LOG(WARN, "invalid param, sstable_name=%p", sstable_name);
        ret = OB_ERROR;
      }
      else 
      {
        /**
         * import sstable name format: 
         *    ex: 1001-000001
         *    1001    table id
         *    -       delimeter '-'
         *    000001  range sequence number, 6 chars
         */
        int num = sscanf(sstable_name, "%lu-%06ld", &table_id, &seq_no);
        if (2 != num)
        {
          TBSYS_LOG(WARN, "failed parse sstable name to get table id, num=%d, "
                          "table_id=%lu, seq_no=%ld",
            num, table_id, seq_no);
          ret = OB_ERROR;
        }
        else
        {
          end_key = end_hexstr_key;
          if (is_max_hexstr_key(end_hexstr_key))
          {
            end_key = NULL;
          }
          ret = add_range_file_pair(table_id, sstable_name, 
            start_hexstr_key, end_key);
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "failed to add range file pair, table_id=%lu, "
                            "sstable_name=%s, start_key=%s, end_key=%s",
              table_id, sstable_name, 
              start_hexstr_key != NULL ? start_hexstr_key : "NULL",
              end_key != NULL ? end_key : "NULL");
          }
        }
      }

      return ret;
    }

    bool ObImportSSTable::is_max_hexstr_key(const char* hexstr_key)
    {
      bool ret = true;

      if (NULL != hexstr_key)
      {
        int64_t key_len = strlen(hexstr_key);
        for (int64_t i = 0; i < key_len; ++i)
        {
          if (hexstr_key[i] != 'F')
          {
            ret = false;
            break;
          }
        }
      }

      return ret;
    }

    int ObImportSSTable::add_range_file_pair(const uint64_t table_id, 
      const char* file_name, const char* start_hexstr_key, const char* end_hexstr_key)
    {
      int ret = OB_SUCCESS;
      ObFilePathInfo file_path_info;
      ObRange range;

      /**
       * needn't check start_hexstr_key and end_hexstr_key, if 
       * start_hexstr_key is NULL, it means the start rowkey is 'min',
       * if end_hexstr_key is NULL, it means the end rowkey is 
       * 'max', that both start_hexstr_key and end_hexstr_key are 
       * NULL is not allowed.
       */
      if (OB_INVALID_ID == table_id || NULL == file_name 
          || (NULL == start_hexstr_key && NULL == end_hexstr_key))
      {
        TBSYS_LOG(WARN, "invalid param, table_id=%lu, file_name=%p, "
                        "start_hexstr_key=%p, end_hexstr_key=%p",
          table_id, file_name, start_hexstr_key, end_hexstr_key);
        ret = OB_ERROR;
      }
      else if (static_cast<int64_t>(strlen(file_name)) >= MAX_IMPORT_SSTABLE_NAME_SIZE)
      {
        TBSYS_LOG(WARN, "file name length is too large, "
                        "file_name_len=%d, max_len=%ld",
          (int)strlen(file_name), MAX_IMPORT_SSTABLE_NAME_SIZE);
        ret = OB_ERROR;
      }
      else
      {
        ret = build_range(start_hexstr_key, end_hexstr_key, range);
        if (OB_SUCCESS == ret)
        {
          strncpy(file_path_info.file_name_, file_name, strlen(file_name));
          range.table_id_ = table_id;

          char range_buf[OB_RANGE_STR_BUFSIZ];
          range.to_string(range_buf, OB_RANGE_STR_BUFSIZ);
          TBSYS_LOG(INFO, "add_range_file_pair, table_id=%ld, sstable_name=%s, "
                          "range=%s, start_hexstr_key=%s, end_hexstr_key=%s", 
            table_id, file_name, range_buf, start_hexstr_key, end_hexstr_key);

          ret = range2file_hash_map_.set(range, file_path_info);
          if (HASH_INSERT_SUCC != ret)
          {
            TBSYS_LOG(WARN, "failed to set range and file path info into the "
                            "range2file_hash_map, table_id=%lu, file_name=%s, "
                            "start_hexstr_key=%s, end_hexstr_key=%s, ret=%d",
              table_id, file_name, start_hexstr_key, end_hexstr_key, ret);
            ret = OB_ERROR;
          }
          else
          {
            ret = file2range_hash_map_.set(file_path_info, range);
            if (HASH_INSERT_SUCC != ret)
            {
              TBSYS_LOG(WARN, "failed to set range and file path info into the "
                              "file2range_hash_map, table_id=%lu, file_name=%s, "
                              "start_hexstr_key=%s, end_hexstr_key=%s, ret=%d",
                table_id, file_name, start_hexstr_key, end_hexstr_key, ret);
              ret = OB_ERROR;
            }
            else
            {
              ret = OB_SUCCESS;
            }
          }
        }

        /**
         * only when adding the first range of table, we add the table 
         * id into the table id hash map, it ensure add table id once.
         */
        if (OB_SUCCESS == ret && NULL == start_hexstr_key)
        {
          ret = tableid_hash_map_.set(table_id, 1);
          if (HASH_INSERT_SUCC != ret)
          {
            TBSYS_LOG(WARN, "failed to set table id into the "
                            "tableid_hash_map, table_id=%lu, file_name=%s, "
                            "start_hexstr_key=%s, end_hexstr_key=%s, ret=%d",
              table_id, file_name, start_hexstr_key, end_hexstr_key, ret);
            ret = OB_ERROR;
          }
          else
          {
            ret = OB_SUCCESS;
          }
        }
      }

      return ret;
    }

    int ObImportSSTable::build_range(const char* start_hexstr_key, 
      const char* end_hexstr_key, ObRange& range)
    {
      int ret                       = OB_SUCCESS;
      char start_key_buf[OB_RANGE_STR_BUFSIZ];
      int64_t start_key_len         = 0;
      int64_t start_hexstr_key_len  = 0;
      char end_key_buf[OB_RANGE_STR_BUFSIZ];
      int64_t end_key_len           = 0;
      int64_t end_hexstr_key_len    = 0;
      ObRange new_range;

      if (NULL != start_hexstr_key)
      {
        start_hexstr_key_len = strlen(start_hexstr_key);
        start_key_len = str_to_hex(start_hexstr_key, static_cast<int32_t>(start_hexstr_key_len), 
          start_key_buf, OB_RANGE_STR_BUFSIZ);
        if (start_key_len != start_hexstr_key_len || start_key_len % 2 != 0)
        {
          TBSYS_LOG(WARN, "failed to parse start key from hex format to "
                          "binary format, start_hexstr_key=%s, "
                          "start_hexstr_key_len=%ld, start_key_len=%ld", 
            start_hexstr_key, start_hexstr_key_len, start_key_len);
          ret = OB_ERROR;
        }
        else
        {
          new_range.border_flag_.unset_inclusive_start();
          new_range.start_key_.assign_ptr(start_key_buf, static_cast<int32_t>(start_key_len / 2));
        }
      }
      else
      {
        new_range.border_flag_.set_min_value();
      }

      if (OB_SUCCESS == ret)
      {
        if (NULL != end_hexstr_key)
        {
          end_hexstr_key_len = strlen(end_hexstr_key);
          end_key_len = str_to_hex(end_hexstr_key, static_cast<int32_t>(end_hexstr_key_len), 
            end_key_buf, OB_RANGE_STR_BUFSIZ);
          if (end_key_len != end_hexstr_key_len || end_key_len % 2 != 0)
          {
            TBSYS_LOG(WARN, "failed to parse end key from hex format to "
                            "binary format, end_hexstr_key=%s, "
                            "end_hexstr_key_len=%ld, end_key_len=%ld", 
              end_hexstr_key, end_hexstr_key_len, end_key_len);
            ret = OB_ERROR;
          }
          else
          {
            new_range.border_flag_.set_inclusive_end();
            new_range.end_key_.assign_ptr(end_key_buf, static_cast<int32_t>(end_key_len / 2));
          }
        }
        else
        {
          new_range.border_flag_.set_max_value();
        }
      }

      if (OB_SUCCESS == ret)
      {
        ret = deep_copy_range(range_arena_, new_range, range);
        if (OB_SUCCESS != ret)
        {
          char range_buf[OB_RANGE_STR_BUFSIZ];
          new_range.to_string(range_buf, OB_RANGE_STR_BUFSIZ);
          TBSYS_LOG(WARN, "failed deep copy range=%s", range_buf);
        }
      }

      return ret;
    }

    int ObImportSSTable::scan_sstable_files(Filter filter)
    {
      int ret                       = OB_SUCCESS;
      int32_t size                  = 0;
      const int32_t* disk_no_array  = NULL;

      disk_no_array = disk_manager_->get_disk_no_array(size);
      if (NULL != disk_no_array && size > 0)
      {
        for (int64_t i = 0; i < size && OB_SUCCESS == ret; ++i)
        {
          ret = scan_sstable_files(disk_no_array[i], filter);
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "failed to scan import sstable file in disk no=%d",
              disk_no_array[i]);
          }
        }
      }
      else
      {
        TBSYS_LOG(WARN, "get disk no array failed.");
        ret = OB_ERROR;
      }
      
      return ret;
    }

    int ObImportSSTable::scan_sstable_files(const int64_t disk_no, Filter filter)
    {
      int ret                     = OB_SUCCESS;
      int64_t file_num            = 0;
      struct dirent** file_dirent = NULL;
      bool build_map              = true;
      char directory[OB_MAX_FILE_NAME_LENGTH];

      //each disk has one import dirctory
      ret = get_import_sstable_directory(static_cast<int32_t>(disk_no), directory, OB_MAX_FILE_NAME_LENGTH);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "get sstable import directory error, disk_no=%ld.", disk_no);
      }
      else if (!FileDirectoryUtils::is_directory(directory))
      {
        TBSYS_LOG(ERROR, "import sstable dir doesn't exist, dir=%s", directory);
        ret = OB_DIR_NOT_EXIST;
      }
      else if ((file_num = ::scandir(directory, 
                &file_dirent, filter, ::versionsort)) <= 0 
               || NULL == file_dirent)
      {
        TBSYS_LOG(INFO, "import directory=%s doesn't have sstable files.", directory);
      }
      else
      {
        for (int64_t n = 0; n < file_num; ++n)
        {
          if (NULL == file_dirent[n])
          {
            TBSYS_LOG(WARN, "scandir return null dirent[%ld]. directory=%s", 
              n, directory);
            ret = OB_IO_ERROR;
          }
          else
          {
            if (build_map)
            {
              ret = build_range2file_hash_map(disk_no, file_dirent[n]->d_name);
              if (OB_SUCCESS != ret)
              {
                TBSYS_LOG(WARN, "build_range2file_hash_map failed, dir=%s, file_name=%s",
                  directory, file_dirent[n]->d_name);
                build_map = false;
              }
            }

            ::free(file_dirent[n]);
          }
        }
      }

      if (NULL != file_dirent) 
      {
        ::free(file_dirent);
        file_dirent = NULL;
      }

      return ret;
    }

    int ObImportSSTable::build_range2file_hash_map(
      const int64_t disk_no, const char* file_name)
    {
      int ret = OB_SUCCESS;
      ObFilePathInfo file_path_info;
      ObRange range;

      if (disk_no <= 0 || NULL == file_name)
      {
        TBSYS_LOG(WARN, "invalid import sstable file name or disk no, "
                        "file_name=%p, disk_no=%ld", 
          file_name, disk_no);
        ret = OB_ERROR;
      }
      else if (0 == range2file_hash_map_.size() || 0 == file2range_hash_map_.size()
               || range2file_hash_map_.size() != file2range_hash_map_.size())
      {
        TBSYS_LOG(WARN, "invalid range file hash map size, range2file_hash_map_.size=%ld, "
                        "file2range_hash_map_.size=%ld", 
          range2file_hash_map_.size() , file2range_hash_map_.size());
        ret = OB_ERROR;
      }
      else
      {
        strncpy(file_path_info.file_name_, file_name, strlen(file_name));
        ret = file2range_hash_map_.get(file_path_info, range);
        if (HASH_EXIST == ret)
        {
          file_path_info.disk_no_ = disk_no;
          ret = range2file_hash_map_.set(range, file_path_info, 1);
          if (HASH_OVERWRITE_SUCC != ret)
          {
            char range_buf[OB_RANGE_STR_BUFSIZ];
            range.to_string(range_buf, OB_RANGE_STR_BUFSIZ);
            TBSYS_LOG(WARN, "failed to overwrite range2file_hash_map, file_name=%s, "
                            "disk_no=%ld, range=%s, ret=%d", 
              file_name, disk_no, range_buf, ret);
            ret = OB_ERROR;
          }
          else 
          {
            ret = OB_SUCCESS;
          }
        }
        else 
        {
          TBSYS_LOG(WARN, "disk_no=%ld, file_name=%s isn't existent "
                          "in file2range_hash_map", 
            disk_no, file_name);
          ret = OB_ERROR;
        }
      }

      return ret;
    }
  } // end namespace chunkserver
} // end namespace oceanbase
