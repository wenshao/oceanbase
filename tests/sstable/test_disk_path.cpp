/**
 *  (C) 2010-2011 Taobao Inc.
 *  
 *  This program is free software; you can redistribute it
 *  and/or modify it under the terms of the GNU General Public
 *  License version 2 as published by the Free Software
 *  Foundation.
 *
 *  test_disk_path.cpp is for what ...
 *
 *  Authors:
 *     qushan < qushan@taobao.com >
 *        
 */
#include <tbsys.h>
#include "common/ob_define.h"
#include "sstable/ob_disk_path.h"
#include "sstable/ob_sstable_stat.h"

using namespace oceanbase::common;

namespace oceanbase 
{
  namespace sstable
  {
    uint64_t get_sstable_disk_no(const uint64_t sstable_file_id)
    {
      return (sstable_file_id & DISK_NO_MASK);
    }

    int get_sstable_directory(const int32_t disk_no, char *path, const int64_t path_len)
    {
      int ret = OB_SUCCESS;
      if (disk_no < 0 || NULL == path || path_len < 0)
      {
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        // read from configure file
        snprintf(path, path_len, "%s/%d", "./tmp", disk_no);
      }
      return ret;
    }

    int get_sstable_path(const ObSSTableId& sstable_id, char *path, const int64_t path_len)
    {
      int ret = OB_SUCCESS;
      if (sstable_id.sstable_file_offset_ < 0 || NULL == path || path_len < 0)
      {
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        // read from configure file
        snprintf(path, path_len, "%s/%lu/%ld", "./tmp", 
                 get_sstable_disk_no(sstable_id.sstable_file_id_), sstable_id.sstable_file_id_);
      }
      return ret;
    }

    int get_meta_path(const int32_t disk_no, const bool current, char *path, const int32_t path_len)
    {
      int ret = OB_SUCCESS;
      UNUSED(current);
      if (disk_no <= 0 || NULL == path ||  path_len <= 0)
      {
        ret = OB_INVALID_ARGUMENT;
      }
      if (OB_SUCCESS == ret)
      {
        snprintf(path, path_len, "idx_%d", disk_no);
      }
      return ret;
    }

    int get_meta_path(const int64_t version, const int32_t disk_no, 
        const bool current, char *path, const int32_t path_len)
    {
      int ret = OB_SUCCESS;
      UNUSED(current);
      if (disk_no <= 0 || NULL == path ||  path_len <= 0)
      {
        ret = OB_INVALID_ARGUMENT;
      }
      if (OB_SUCCESS == ret)
      {
        snprintf(path, path_len, "./tmp/%d/idx_%ld_%d", disk_no, version, disk_no);
      }
      return ret;
    }

    int idx_file_name_filter(const struct dirent *d)
    {
      int ret = 0;
      if (NULL != d)
      {
        ret = (0 == strncmp(d->d_name,"idx_",4)) ? 1 : 0;
      }
      return ret;
    }

    int bak_idx_file_name_filter(const struct dirent *d)
    {
      int ret = 0;
      if (NULL != d)
      {
        ret = (0 == strncmp(d->d_name,"bak_idx_",8)) ? 1 : 0;
      }
      return ret;
    }

    int get_recycle_directory(const int32_t disk_no, char *path, const int64_t path_len)
    {
      int ret = OB_SUCCESS;
      if (disk_no < 0 || NULL == path || path_len < 0)
      {
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        // read from configure file
        int bufsiz = snprintf(path, path_len, "./tmp/%d/Recycle",  disk_no);
        if (bufsiz + 1 > path_len)
        {
          ret = OB_SIZE_OVERFLOW;
          }
      }


      return ret;
    }

    int get_tmp_meta_path(const int32_t disk_no, char *path, const int32_t path_len)
    {
      int ret = OB_SUCCESS;
      if (disk_no <= 0 || NULL == path ||  path_len <= 0)
      {
        ret = OB_INVALID_ARGUMENT;
      }

      if (OB_SUCCESS == ret)
      {
        int64_t current_time = tbsys::CTimeUtil::getTime();
        int bufsiz = snprintf(path, path_len, "./tmp/tmp_idx_%d_%ld", disk_no, current_time);

        if (bufsiz + 1 > path_len)
        {
          ret = OB_SIZE_OVERFLOW;
        }
      }

      return ret;
    }

    int get_import_sstable_directory(const int32_t disk_no, char *path, 
      const int64_t path_len)
    {
      int ret = OB_SUCCESS;
      if (disk_no < 0 || NULL == path || path_len < 0)
      {
        TBSYS_LOG(WARN, "get_sstable_import_directory invalid arguments, "
            "disk_no=%d, path=%p, path_len=%ld", disk_no, path, path_len);
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        int bufsiz = snprintf(path, path_len, "./tmp/%d/import", disk_no);
        if (bufsiz + 1 > path_len)
        {
          TBSYS_LOG(WARN, "get_sstable_import_directory, path_len=%ld <= bufsiz=%d", path_len, bufsiz);
          ret = OB_SIZE_OVERFLOW;
        }
      }

      return ret;
    }

    int get_import_sstable_path(const int32_t disk_no, 
      const char* sstable_name, char *path, const int64_t path_len)
    {
      int ret = OB_SUCCESS;
      if (disk_no < 0 || NULL == sstable_name || NULL == path || path_len < 0)
      {
        TBSYS_LOG(WARN, "get_import_sstable_path invalid arguments, "
            "disk_no=%d, sstable_name=%p, path=%p,path_len=%ld", 
            disk_no, sstable_name, path, path_len);
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        int bufsiz = snprintf(path, path_len, "./tmp/%d/import/%s",
            disk_no, sstable_name);
        if (bufsiz + 1 > path_len)
        {
          TBSYS_LOG(WARN, "get_import_sstable_path, path_len=%ld <= bufsiz=%d", 
              path_len, bufsiz);
          ret = OB_SIZE_OVERFLOW;
        }
      }

      return ret;
    }
  }
}
