/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * ob_disk_manager.cpp is for what ...
 *
 * Version: $id$
 *
 * Authors:
 *   maoqi <maoqi@taobao.com>
 *
 */

#include <sys/types.h>
#include <sys/statvfs.h>
#include <dirent.h>
#include <unistd.h>
#include "sstable/ob_disk_path.h"
#include "ob_disk_manager.h"
#include "ob_chunk_server_main.h"
#include "common/file_directory_utils.h"

namespace oceanbase 
{
  namespace chunkserver 
  {
    const char *ObDiskManager::PROC_MOUNTS_FILE = "/proc/mounts";
    const int32_t ObDiskManager::PROC_MOUNTS_FILE_SIZE = 4096;

    using namespace common;
    ObDiskManager::ObDiskManager():disk_num_(0)
    {
      reset();
    }

    void ObDiskManager::reset()
    {
      disk_num_ = 0;
      memset(&pending_files_,0,sizeof(pending_files_));
      memset(&disk_no_array_,0,sizeof(disk_no_array_));
      memset(&sstable_files_,0,sizeof(sstable_files_));
      memset(&disk_,0,sizeof(disk_));
    }

    int ObDiskManager::scan(const char *data_dir, const int64_t max_sstable_size)
    {
      int ret = OB_SUCCESS;
      if (NULL == data_dir || '\0' == *data_dir || max_sstable_size <= 0)
      {
        ret = OB_ERROR;
      }
      else
      {
        char disk_dir[common::OB_MAX_FILE_NAME_LENGTH];
        char sstable_dir[common::OB_MAX_FILE_NAME_LENGTH];
        struct statvfs svfs;

        tbsys::CWLockGuard guard(lock_);
        max_sstable_size_ = max_sstable_size;
        reset();
        
        for(int n = 1; n <= OB_MAX_DISK_NUMBER; ++n) //disk index start from 1
        {
          snprintf (disk_dir, sizeof(disk_dir), "%s%c%d", data_dir, '/', n);
          if (FileDirectoryUtils::is_directory(disk_dir)/* && is_mount_point(disk_dir)*/)
          {
            ret = sstable::get_sstable_directory(n, sstable_dir, OB_MAX_FILE_NAME_LENGTH);
            if (OB_SUCCESS != ret)
            {
              TBSYS_LOG(ERROR, "get sstable directory error, disk_no=%d", n);
              break;
            }
            else if ( 0 != statvfs(disk_dir,&svfs))
            {
              TBSYS_LOG(WARN, "stat vfs error, disk_dir=%s", disk_dir);
              continue;
            }
            else if (!FileDirectoryUtils::is_directory(sstable_dir))
            {
              TBSYS_LOG(WARN, "sstable dir =%s is not a directory.", sstable_dir);
              continue;
            }
            else
            {
              disk_[disk_num_].disk_no_ = n;
              disk_[disk_num_].capacity_ = svfs.f_blocks * svfs.f_bsize;
              disk_[disk_num_].avail_ = svfs.f_bavail * svfs.f_bsize;
              disk_no_array_[disk_num_] = n;
              ++disk_num_;
            }
          }
        }
        if (disk_num_ > 0)
        {
          TBSYS_LOG(DEBUG,"scan disk success.");
          dump();
        }
        else
        {
          TBSYS_LOG(ERROR,"no disk in dir %s, pls check configure item.", data_dir);
          ret = OB_ERROR;
        }
      }
      return ret;
    }

    
    /** 
     * @brief these two functions is for migrate,
     *        migration is not very often,so we just select the most free disk
     *        when migration is done,invoke the shrink_space
     * @return  on success disk_no is returned,-1 otherwise
     */
    int32_t ObDiskManager::get_disk_for_migrate()
    {
      tbsys::CRLockGuard guard(lock_);
      int32_t disk_no = -1;
      if (disk_num_ <= 0)
      {
        disk_no = -1;
      }
      else
      {
        int32_t avail_index = -1;
        int64_t avail = 0;
        int64_t this_avail = 0;

        for(int i=0;i < disk_num_; ++i)
        {
          if (disk_[i].status_ != DISK_NORMAL)
          {
            continue;
          }

          this_avail = (disk_[i].avail_ - pending_files_[i] * max_sstable_size_);
          
          if (this_avail > avail)
          {
            avail = this_avail;
            avail_index = i;
          }
        }

        if (disk_[avail_index].avail_ < max_sstable_size_)
        {
          TBSYS_LOG(ERROR,"disk space not enough");
        }
        else
        {
          disk_no = disk_[avail_index].disk_no_;
        }
      }
      return disk_no;
    }

    void ObDiskManager::shrink_space(const int32_t disk_no,const int64_t used)
    {
      tbsys::CWLockGuard guard(lock_);
      if (disk_no > 0 && disk_no <= OB_MAX_DISK_NUMBER)
      {
        int32_t index = find_disk(disk_no);
        if (index != -1)
        {
          disk_[index].avail_ -= used;
        }
      }
      return;
    }

    int32_t ObDiskManager::get_dest_disk()
    {
      tbsys::CRLockGuard guard(lock_);
      int32_t disk_no = -1;
      if (disk_num_ <= 0)
      {
        disk_no = -1;
      }
      else
      {
        int64_t avail_index = -1;
        int32_t sstable_num = INT32_MAX;
        
        int64_t avail = 0;
        int64_t this_avail = 0;

        int64_t no_pending_avail = 0;
        int64_t no_pending_index = -1;

        int64_t best_avail = 0;
        int64_t best_index = -1;

        int64_t concurrent_writer =  ObChunkServerMain::get_instance()->get_chunk_server()
                                                            .get_param().get_merge_thread_per_disk();

        if (concurrent_writer <= 0)
          concurrent_writer = 1; //for test

        concurrent_writer <<= 1;

        for(int i=0;i < disk_num_; ++i)
        {
          if (disk_[i].status_ != DISK_NORMAL)
          {
            continue;
          }

          this_avail = (disk_[i].avail_ - pending_files_[i] * max_sstable_size_);
          if (this_avail < max_sstable_size_)
          {
            continue;
          }
          
          //find the most available disk
          if (this_avail > avail)
          {
            avail = this_avail;
            avail_index = i;
          }

          //find the most available disk that no more than concurrent_writer pending files 
          if ( pending_files_[i] < concurrent_writer && (this_avail > no_pending_avail))
          {
            no_pending_avail = this_avail;
            no_pending_index = i;
          }

          //find the disk that no more than concurrent_writer pending files & have the least sstable files
          if (sstable_files_[i] <= sstable_num 
              && pending_files_[i] < concurrent_writer)
          {
            if (sstable_files_[i] < sstable_num ||
                (sstable_files_[i] == sstable_num && this_avail > best_avail))
            {
              best_index = i;
              best_avail = this_avail;
              sstable_num = sstable_files_[i];
            }
          }
        }

        if (best_index != -1)
        {
          avail_index = best_index;
        }
        else if (no_pending_index != -1)
        {
          avail_index = no_pending_index;
        }

        if (-1 == avail_index || disk_[avail_index].avail_ < max_sstable_size_)
        {
          TBSYS_LOG(ERROR,"disk space not enough,avail_index:%ld,pending_files_ is %d",avail_index,
              pending_files_[avail_index]);
        }
        else
        {
          disk_no = disk_[avail_index].disk_no_;
          pending_files_[avail_index] += 1;
        }
      }
      return disk_no;
    }

    void ObDiskManager::add_used_space(const int32_t disk_no,const int64_t used,
      const bool decr_pending_cnt)
    {
      tbsys::CWLockGuard guard(lock_);
      if (disk_no > 0 && disk_no <= OB_MAX_DISK_NUMBER)
      {
        int32_t index = find_disk(disk_no);
        if (index != -1)
        {
          disk_[index].avail_ -= used;
          if (decr_pending_cnt)
          {
            pending_files_[index] -= 1;
          }
          if (used > 0)
          {
            sstable_files_[index] += 1;
          }
        }
      }
      return;
    }

    void ObDiskManager::add_sstable_num(const int32_t disk_no,const int32_t num)
    {
      tbsys::CWLockGuard guard(lock_);
      if (disk_no > 0 && disk_no <= OB_MAX_DISK_NUMBER)
      {
        int32_t index = find_disk(disk_no);
        if (index != -1)
        {
          sstable_files_[index] += num;
        }
      }
      return;
    }
        
    void ObDiskManager::set_disk_status(const int32_t disk_no,const ObDiskStatus stat)
    {
      tbsys::CWLockGuard guard(lock_);
      if (disk_no > 0 && disk_no <= OB_MAX_DISK_NUMBER)
      {
        int32_t index = find_disk(disk_no);
        if (index != -1)
        {
          disk_[index].status_ = stat;
        }
      }
      return;
    }

    void ObDiskManager::release_space(const int32_t disk_no,const int64_t size)
    {
      tbsys::CWLockGuard guard(lock_);
      if (disk_no > 0 && disk_no <= OB_MAX_DISK_NUMBER)
      {
        int32_t index = find_disk(disk_no);
        if (index != -1)
        {
          disk_[index].avail_ += size;
        }
      }
      return;
    }

    int64_t ObDiskManager::get_total_capacity(void) const
    {
      int64_t capacity = 0;
      {
        tbsys::CRLockGuard guard(lock_);
        for(int i=0;i < disk_num_; ++i)
        {
          capacity += disk_[i].capacity_;
        }
      }
      return capacity;
    }

    int64_t ObDiskManager::get_total_used(void) const
    {
      int64_t used = 0;
      {
        tbsys::CRLockGuard guard(lock_);
        for(int i=0;i < disk_num_; ++i)
        {
          used += disk_[i].capacity_ - disk_[i].avail_;
          used += pending_files_[i] * max_sstable_size_;
        }
      }
      return used;
    }
        
    const int32_t* ObDiskManager::get_disk_no_array(int32_t& disk_num) const
    {
      tbsys::CRLockGuard guard(lock_);
      disk_num = disk_num_;
      return disk_no_array_;
    }

    bool ObDiskManager::is_mount_point(const char *path) const
    {
      FILE *fp = NULL;
      bool ret = false;
      int err = OB_SUCCESS;
      struct stat st;
      char f_path[common::OB_MAX_FILE_NAME_LENGTH];
      const char *path_check = path;
      
      if (NULL == path || '\0' == *path)
      {
        TBSYS_LOG(WARN,"path is null");
        err = OB_ERROR;
      }
      
      if ((OB_SUCCESS == err) && NULL == (fp = fopen(PROC_MOUNTS_FILE,"r")))
      {
        TBSYS_LOG(WARN,"open %s failed",PROC_MOUNTS_FILE);
        err = OB_ERROR;
      }

      if (OB_SUCCESS == err)
      {
        if (lstat(path,&st) != 0)
        {
          TBSYS_LOG(WARN,"stat path failed");
          err = OB_ERROR;
        }
        else
        {
          if (S_ISLNK(st.st_mode))
          {
            int64_t size = 0;
            if ((size = readlink(path,f_path,sizeof(f_path) - 1)) < 0)
            {
              TBSYS_LOG(WARN,"readlink failed");
              ret = OB_ERROR;
            }
            else
            {
              if ('/' == f_path[size - 1])
                f_path[size - 1] = '\0';
              else
                f_path[size] = '\0';
              path_check = f_path;
            }
          }
        }
      }

      if (OB_SUCCESS == err)
      {
        char *buf = static_cast<char *>(ob_malloc(PROC_MOUNTS_FILE_SIZE)); //st.st_size in proc is 0
        int64_t size = 0;
        if (buf != NULL && (size = read(fileno(fp),buf,PROC_MOUNTS_FILE_SIZE - 1)) <= 0)
        {
          TBSYS_LOG(WARN,"read %s failed",PROC_MOUNTS_FILE);
        }
        else
        {
          buf[size] = '\0';
          ret = (strstr(buf,path_check) != NULL);
        }

        if (buf != NULL)
        {
          ob_free(buf);
          buf = NULL;
        }
      }
      return ret;
    }

    int32_t ObDiskManager::find_disk(const int32_t disk_no)
    {
      int32_t index = disk_no - 1;
      int32_t ret = -1;
      for(;index >= 0;--index)
      {
        if (disk_no == disk_[index].disk_no_)
        {
          ret = index;
          break;
        }
      }
      return ret;
    }

    void ObDiskManager::dump()
    {
      for(int i=0;i<disk_num_;++i)
      {
        TBSYS_LOG(DEBUG,"DISK %d: capacity:%ld,avail:%ld",disk_[i].disk_no_,disk_[i].capacity_,disk_[i].avail_);
      }
      return;
    }
  } /* chunkserver */
} /* oceanbase */

