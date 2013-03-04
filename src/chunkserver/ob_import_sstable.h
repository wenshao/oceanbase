/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_import_sstable.h for import sstable. 
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */
#ifndef OCEANBASE_CHUNKSERVER_IMPORT_SSTABLE_H_
#define OCEANBASE_CHUNKSERVER_IMPORT_SSTABLE_H_

#include <dirent.h>
#include "common/hash/ob_hashmap.h"
#include "common/ob_range.h"
#include "common/page_arena.h"

namespace oceanbase
{
  namespace chunkserver
  {
    static const int64_t MAX_IMPORT_SSTABLE_NAME_SIZE = 32;

    struct ObFilePathInfo
    {
      int64_t disk_no_;
      char file_name_[MAX_IMPORT_SSTABLE_NAME_SIZE];

      ObFilePathInfo()
      {
        disk_no_ = 0;
        memset(file_name_, 0, MAX_IMPORT_SSTABLE_NAME_SIZE);
      }

      int64_t hash() const
      {
        uint32_t hash_val = 0;

        hash_val = common::murmurhash2(&disk_no_, static_cast<uint32_t>(sizeof(int64_t)), 0);
        hash_val = common::murmurhash2(file_name_, static_cast<uint32_t>(strlen(file_name_)), hash_val);

        return hash_val;
      }

      bool operator == (const ObFilePathInfo &other) const
      {
        return (disk_no_ == other.disk_no_
                && ::strcmp(file_name_, other.file_name_) == 0);
      }     
    };

    /**
     * this class is used to query import sstable file info. it scan
     * the import sstable directory and build the range to file hash
     * map, when chunserver do daily merge, the process check each
     * tablet need import new version to instean of old version
     * tablet, if the merge tablet has import sstable, then use the
     * import sstable as the new version sstable, and never do merge
     * for this tablet. in other words, use importing to instead of
     * merging for tablet. 
     *  
     * WARNING: this class is not thred safe 
     */
    class ObImportSSTable
    {
    public:
      ObImportSSTable();
      ~ObImportSSTable();

    public:
      /**
       * initialzie this import sstable instance, scan the import 
       * sstable directory and build the internal hash map. then user 
       * can query the import sstable info by tablet range. 
       * 
       * @param disk_manager disk manager, use to get the disk array
       * @param app_name application name
       * 
       * @return int if success, return OB_SUCCESS, else return 
       *         OB_ERROR
       */
      int init(const ObDiskManager& disk_manager, const char* app_name);

      /**
       * after init, user can use this function to get import sstable 
       * file info by tablet range. 
       * 
       * @param range tablet range to query
       * @param sstable_info [out] the returned import sstable file 
       *                     information
       * 
       * @return int if success, return OB_SUCCESS, else return 
       *         OB_ERROR
       */
      int get_import_sstable_info(const common::ObRange& range, 
        ObFilePathInfo& sstable_info) const;

      /**
       * check if the table id need import, if there are some sstable 
       * for this table, it need import, otherwisze it need not. if 
       * one table need import new version sstable, each tablet of 
       * this table must own one import sstable, otherwisze something 
       * wrong. this instance builds a tableid hash map to do the 
       * query. 
       * 
       * @param table_id table id to check if need import
       * 
       * @return bool if need, return true, else return false
       */
      bool need_import_sstable(const uint64_t table_id) const;

      /**
       * clear the internal hash maps and variables to reuse it. 
       * @param delete_range_files if delete range files  
       */
      void clear(bool delete_range_files = true);

    private:
      typedef int (*Filter)(const struct dirent* d);
      static int sstable_file_name_filter(const struct dirent* d);
      static int range_file_name_filter(const struct dirent* d);

      int build_sstable_hash_map();
      int scan_range_files(Filter filter);
      int parse_range_file(const char* dir, const char* file_name);
      int read_range_file(const char* dir, const char* file_name, 
        char*& file_buf, int64_t& read_size);
      bool is_range_file_name_valid(const char* file_name);
      int read_line(char* input, const int64_t input_size, 
        int64_t& pos, char*& sstable_name, char*& end_hexstr_key);
      int process_line(const char* sstable_name, const char* start_hexstr_key, 
        const char* end_hexstr_key);
      bool is_max_hexstr_key(const char* hexstr_key);
      int add_range_file_pair(const uint64_t table_id, const char* file_name, 
        const char* start_hexstr_key, const char* end_hexstr_key);
      int build_range(const char* start_hexstr_key, const char* end_hexstr_key,
        common::ObRange& range);

      int scan_sstable_files(Filter filter);
      int scan_sstable_files(const int64_t disk_no, Filter filter);
      int build_range2file_hash_map(const int64_t disk_no, const char* file_name);
      
      void destroy();

    private:
      static const int64_t MAX_TABLE_RANGE_CONFIG_FILES = 128;
      static const int64_t HASH_BUCKET_NUM = 1024 * 16;
      static const int64_t TABLEID_HASH_BUCKET_NUM = 128;

    private:
      DISALLOW_COPY_AND_ASSIGN(ObImportSSTable);

      bool inited_;
      const ObDiskManager* disk_manager_;
      const char* app_name_;
      common::ObMemBuf file_buf_;

      char* range_files_[MAX_TABLE_RANGE_CONFIG_FILES];
      int64_t range_file_cnt_;

      common::hash::ObHashMap<uint64_t, int64_t, 
        common::hash::NoPthreadDefendMode> tableid_hash_map_;

      common::hash::ObHashMap<common::ObRange, ObFilePathInfo,
        common::hash::NoPthreadDefendMode> range2file_hash_map_;

      common::hash::ObHashMap<ObFilePathInfo, common::ObRange, 
        common::hash::NoPthreadDefendMode> file2range_hash_map_;

      common::ModulePageAllocator mod_allocator_;
      common::ModuleArena range_arena_;
    };
  } /* chunkserver */
} /* oceanbase */

#endif // OCEANBASE_CHUNKSERVER_IMPORT_SSTABLE_H_
