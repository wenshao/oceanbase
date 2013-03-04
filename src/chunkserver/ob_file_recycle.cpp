#include <sys/types.h>

#include "ob_file_recycle.h"
#include "ob_tablet_manager.h"
#include "ob_chunk_server_main.h"
#include "common/file_directory_utils.h"
#include <tbsys.h>
namespace oceanbase
{
  using namespace common;
  using namespace tbsys;
  using namespace sstable;
  namespace chunkserver
  {
    //--------------------------------------------------------------------
    // class ObRegularRecycler 
    //--------------------------------------------------------------------
    ObRegularRecycler::ObRegularRecycler(ObTabletManager& manager) 
      : manager_(manager)
    {
    }

    ObRegularRecycler::~ObRegularRecycler()
    {
    }

    int ObRegularRecycler::recycle(const int64_t version)
    {
      int ret = OB_SUCCESS;
      ret = prepare_recycle(version);
      if (OB_SUCCESS == ret)
      {
        ret = check_current_status(true);
      }
      return ret;
    }

    int ObRegularRecycler::prepare_recycle(const int64_t version)
    {
      int ret = OB_SUCCESS;
      if (0 >= version)
      {
        TBSYS_LOG(ERROR, "input invalid version = %ld ", version);
        ret = OB_INVALID_ARGUMENT;
      }
      // still not initialized
      else if (0 == expired_image_.get_data_version() 
          && OB_SUCCESS != (ret = load_all_tablets(version)))
      {
        TBSYS_LOG(WARN, "cannot load tablets in image, version=%ld", version);
      }
      else if (version != expired_image_.get_data_version())
      {
        check_current_status(false);
        // destroy old image;
        ret = expired_image_.destroy();
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "cannot destroy previous recycle tablet "
              "image version =%ld", expired_image_.get_data_version());
        }
        else
        {
          ret = load_all_tablets(version);
        }
      }
      return ret;
    }

    int ObRegularRecycler::check_current_status(const bool do_recycle)
    {
      ObTablet* tablet = NULL;
      int ret = OB_SUCCESS;

      ret = expired_image_.begin_scan_tablets();
      if (OB_ITER_END == ret)
      {
        TBSYS_LOG(INFO, "expired_image_ has no tablets.");
      }
      else if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "begin_scan_tablets error, ret = %d", ret);
      }
      else 
      {
        while (OB_SUCCESS == (ret = expired_image_.get_next_tablet(tablet)) )
        {
          if (NULL != tablet && tablet->get_merge_count() == 0) 
          {
            TBSYS_LOG(WARN, "tablet not recycle..");
            if (do_recycle)
            {
              do_recycle_tablet(tablet->get_range());
            }
          }

          if (NULL != tablet) 
          {
            expired_image_.release_tablet(tablet);
          }
        }
      }

      expired_image_.end_scan_tablets();

      return ret;
    }

    int ObRegularRecycler::recycle_tablet(const common::ObRange & range, const int64_t version)
    {
      int ret = OB_SUCCESS;

      if (0 >= version)
      {
        TBSYS_LOG(ERROR, "input invalid version = %ld ", version);
        ret = OB_INVALID_ARGUMENT;
      }
      // still not initialized
      else if ( OB_SUCCESS != (ret = prepare_recycle(version)) )
      {
        TBSYS_LOG(WARN, "cannot prepare recycle tablet, version=%ld", version);
      }
      else if ( OB_SUCCESS != (ret = do_recycle_tablet(range)) )
      {
        TBSYS_LOG(WARN, "do recycle tablet error.");
      }

      return ret;
    }

    int ObRegularRecycler::do_recycle_tablet(const common::ObRange & range)
    {
      int ret = OB_SUCCESS;
      ObTablet * tablet = NULL;
      ObSSTableId sstable_id(0);
      char sstable_file_path[OB_MAX_FILE_NAME_LENGTH];
      char range_buf[OB_RANGE_STR_BUFSIZ];

      FileInfoCache* serving_fileinfo_cache = dynamic_cast<FileInfoCache*>(
        &manager_.get_serving_tablet_image().get_fileinfo_cache());

      if (NULL == serving_fileinfo_cache)
      {
        TBSYS_LOG(WARN, "failed to dynamic cast file info cache for recycle sstable");
        ret = OB_ERROR;
      }
      if ( OB_SUCCESS !=  (ret = expired_image_.acquire_tablet(range, 
              ObMultiVersionTabletImage::SCAN_FORWARD, tablet)) )
      {
        range.to_string(range_buf, OB_RANGE_STR_BUFSIZ);
        TBSYS_LOG(WARN, "cannot acuqire tablet range = %s", range_buf);
      }
      else 
      {
        const common::ObArrayHelper<sstable::ObSSTableId>& 
          sstable_id_list = tablet->get_sstable_id_list();
        int64_t sstable_file_size = 0;
        for (int64_t i = 0; i < sstable_id_list.get_array_index(); ++i)
        {
          sstable_id = *sstable_id_list.at(i);
          
          // destroy file info cache if exist.
          const FileInfo * fileinfo = 
            serving_fileinfo_cache->get_cache_fileinfo(sstable_id.sstable_file_id_);
          if (NULL != fileinfo) 
          {
            const_cast<FileInfo*>(fileinfo)->destroy();
            serving_fileinfo_cache->revert_fileinfo(fileinfo);
          }

          ret = get_sstable_path(sstable_id, sstable_file_path, OB_MAX_FILE_NAME_LENGTH);
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "get sstable file path error, id = %ld", 
                sstable_id.sstable_file_id_);
            break;
          }
          else if (!FileDirectoryUtils::exists(sstable_file_path))
          {
            TBSYS_LOG(INFO, "sstable file = %s not exist.", sstable_file_path);
          }
          else if ( (sstable_file_size = FileDirectoryUtils::get_size(sstable_file_path)) <= 0)
          {
            TBSYS_LOG(WARN, "sstable file %s not exist or invalid",sstable_file_path);
          }
          else if (0 != ::unlink(sstable_file_path)) 
          {
            TBSYS_LOG(WARN, "recycle sstable file = %s failed, error=%d", 
                sstable_file_path, errno);
            ret = OB_IO_ERROR;
          }
          else
          {
            manager_.get_disk_manager().release_space(static_cast<int32_t>(get_sstable_disk_no(sstable_id.sstable_file_id_)),
                                                      sstable_file_size);
            TBSYS_LOG(INFO, "recycle tablet version = %ld, recycle sstable file = %s", 
              tablet->get_data_version(), sstable_file_path);
          }
        }
        
        if (OB_SUCCESS == ret)
        {
          // use merge count represents if recycled tablet.
          tablet->inc_merge_count();
        }
      }

      if ( NULL != tablet && OB_SUCCESS != (ret = 
            expired_image_.release_tablet(tablet)) )
      {
        TBSYS_LOG(ERROR, "release tablet error.");
      }

      return ret;
    }

    int ObRegularRecycler::load_all_tablets(const int64_t version)
    {
      int ret = OB_SUCCESS;

      int32_t disk_no_size = OB_MAX_DISK_NUMBER;
      const int32_t *disk_no_array = 
        manager_.get_disk_manager().get_disk_no_array(disk_no_size);

      for (int32_t i = 0; i < disk_no_size && OB_SUCCESS == ret; ++i)
      {
        int32_t disk_no = disk_no_array[i];
        ret = load_tablet(version, disk_no);
      }

      return ret;
    }

    int ObRegularRecycler::load_tablet(const int64_t version, const int32_t disk_no)
    {
      int ret = OB_SUCCESS;
      char idx_file[OB_MAX_FILE_NAME_LENGTH];

      expired_image_.set_data_version(version);

      if ( OB_SUCCESS != (ret = get_meta_path(
              version, disk_no, false, idx_file, OB_MAX_FILE_NAME_LENGTH)) )
      {
        TBSYS_LOG(ERROR, " get meta path error, version=%ld, disk_no=%d",
            version, disk_no);
      }
      else if (!FileDirectoryUtils::exists(idx_file))
      {
        TBSYS_LOG(INFO, "meta file = %s not exist, ignore.", idx_file);
        ret = OB_SUCCESS;
      }
      // do not load sstable files, important!
      else if ( OB_SUCCESS != (ret = expired_image_.read(idx_file, disk_no, false) ) )
      {
        TBSYS_LOG(WARN, " read idx file =%s error", idx_file);
      }

      return ret;
    }

    int ObRegularRecycler::backup_meta_files(const int64_t version)
    {
      int ret = OB_SUCCESS;
      char src_meta_name[OB_MAX_FILE_NAME_LENGTH];
      char dst_meta_name[OB_MAX_FILE_NAME_LENGTH];

      int32_t disk_no = 0;
      int32_t disk_no_size = OB_MAX_DISK_NUMBER;
      const int32_t *disk_no_array = 
        manager_.get_disk_manager().get_disk_no_array(disk_no_size);


      for (int32_t i = 0; i < disk_no_size && OB_SUCCESS == ret; ++i)
      {
        disk_no = disk_no_array[i];
        if ( OB_SUCCESS != (ret = get_meta_path(
                version, disk_no, true, src_meta_name, OB_MAX_FILE_NAME_LENGTH)) )
        {
          TBSYS_LOG(WARN, "get src meta name error, version = %ld, disk = %d",
              version, disk_no);
        }
        else if ( OB_SUCCESS != (ret = get_meta_path(
                version, disk_no, false, dst_meta_name, OB_MAX_FILE_NAME_LENGTH)) )
        {
          TBSYS_LOG(WARN, "get dst meta name error, version = %ld, disk = %d",
              version, disk_no);
        }
        else if (!FileDirectoryUtils::exists(src_meta_name))
        {
          // ignore idx file not exists, maybe restart on new host.
          TBSYS_LOG(INFO, "meta file = %s not exist.", src_meta_name);
          ret = OB_SUCCESS;
        }
        else if (!FileDirectoryUtils::rename(src_meta_name, dst_meta_name))
        {
          TBSYS_LOG(WARN, "rename src meta = %s to dst meta =%s error.", src_meta_name, dst_meta_name);
          ret = OB_IO_ERROR;
        }
      }

      return ret;
    }

    //--------------------------------------------------------------------
    // class ObScanRecycler
    //--------------------------------------------------------------------

    ObScanRecycler::ObScanRecycler(ObTabletManager& manager)
      : manager_(manager)
    {
    }

    ObScanRecycler::~ObScanRecycler()
    {
    }

    int ObScanRecycler::recycle()
    {
      int ret = OB_SUCCESS;

      int32_t disk_no_size = OB_MAX_DISK_NUMBER;
      const int32_t *disk_no_array = 
        manager_.get_disk_manager().get_disk_no_array(disk_no_size);

      for (int32_t i = 0; i < disk_no_size && OB_SUCCESS == ret; ++i)
      {
        int32_t disk_no = disk_no_array[i];
        recycle_sstable(disk_no);
        recycle_meta_file(disk_no);
      }

      return ret;
    }

    int ObScanRecycler::get_version(const char* idx_file, int64_t& data_version)
    {
      int32_t disk_no = 0;

      int ret = OB_SUCCESS;

      data_version = 0;
      if (0 == strncmp(idx_file, "idx_", 4) )
      {
        ret = sscanf(idx_file, "idx_%ld_%d", &data_version, &disk_no);
      }
      else if (0 == strncmp(idx_file, "bak_idx_", 8) )
      {
        ret = sscanf(idx_file, "bak_idx_%ld_%d", &data_version, &disk_no);
      }

      if (ret < 2)
      {
        ret = OB_ERROR;
        data_version = 0;
      }
      else
      {
        ret = OB_SUCCESS;
      }


      return ret;
    }

    int ObScanRecycler::recycle_sstable(int32_t disk_no)
    {
      return do_scan(disk_no, 
          &sstable_file_name_filter, 
          &ObScanRecycler::check_if_expired_sstable, 
          &do_recycle_file);
    }

    int ObScanRecycler::recycle_meta_file(int32_t disk_no)
    {
      return do_scan(disk_no, 
          &meta_file_name_filter, 
          &ObScanRecycler::check_if_expired_meta_file, 
          &do_recycle_file);
    }

    int ObScanRecycler::sstable_file_name_filter(const struct dirent* d)
    {
      int64_t id = strtoll(d->d_name, NULL, 10);
      return id > 0 ? 1 : 0;
    }

    int ObScanRecycler::meta_file_name_filter(const struct dirent* d)
    {
      return idx_file_name_filter(d) || bak_idx_file_name_filter(d);
    }
    
    bool ObScanRecycler::check_if_expired_meta_file(
        int32_t disk_no, const char* dir, const char* filename)
    {
      UNUSED(disk_no);
      UNUSED(dir);
      bool is_expired_meta = false;
      int64_t data_version = 0;

      int ret = get_version(filename, data_version);
      TBSYS_LOG(DEBUG, "filename=%s,ret=%d,data_version=%ld, eldest v=%ld", 
          filename, ret, data_version, manager_.get_serving_tablet_image().get_eldest_version());
      if (OB_SUCCESS == ret)
      {
        if (data_version < manager_.get_serving_tablet_image().get_eldest_version() - 1)
        {
          is_expired_meta = true;
        }
      }

      return is_expired_meta;
    }

    bool ObScanRecycler::check_if_expired_sstable(
        int32_t disk_no, const char* dir, const char* filename)
    {
      UNUSED(disk_no);
      UNUSED(dir);
      bool is_expired_sstable = false;

      int64_t sstable_file_id = ::strtoll(filename, NULL, 10);
      ObSSTableId id(sstable_file_id);

      if (OB_SUCCESS != manager_.get_serving_tablet_image().include_sstable(id))
      {
        is_expired_sstable = true;
      }


      return is_expired_sstable;
    }

    int ObScanRecycler::do_recycle_file(
        int32_t disk_no, const char* dir, const char* filename)
    {
      int ret = OB_SUCCESS;
      char file_path[OB_MAX_FILE_NAME_LENGTH];
      snprintf(file_path, OB_MAX_FILE_NAME_LENGTH, "%s/%s", dir, filename);

      char dest_path[OB_MAX_FILE_NAME_LENGTH];
      get_recycle_directory(disk_no, dest_path, OB_MAX_FILE_NAME_LENGTH);

      int32_t length = static_cast<int32_t>(strlen(dest_path));
      snprintf(dest_path + length, OB_MAX_FILE_NAME_LENGTH, "/%s", filename);

      TBSYS_LOG(INFO, "rename file = %s to %s.", file_path, dest_path);
      if (0 != ::rename(file_path, dest_path))
      {
        TBSYS_LOG(ERROR, "rename file error %d, %s ", errno, strerror(errno));
        ret = OB_IO_ERROR;
      }
      return ret;
    }

    int ObScanRecycler::do_scan(int32_t disk_no, Filter filter, Predicate pred, Operate op)
    {
      int ret = OB_SUCCESS;
      int64_t file_num = 0;
      struct dirent **file_dirent = NULL;
      const char* filename = NULL;

      char directory[OB_MAX_FILE_NAME_LENGTH];
      ret = get_sstable_directory(disk_no, directory, OB_MAX_FILE_NAME_LENGTH);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "get sstable directory error.");
      }
      else if ( (file_num = ::scandir(directory, 
              &file_dirent, filter, ::versionsort)) <= 0 
          || NULL == file_dirent )
      {
        TBSYS_LOG(INFO, "directory=%s has nothing.", directory);
      }
      else
      {
        for (int32_t n = 0; n < file_num && OB_SUCCESS == ret; ++n)
        {
          if (NULL == file_dirent[n])
          {
            TBSYS_LOG(ERROR, "scandir return null dirent[%d]. directory=%s", n, directory);
            ret = OB_ERROR;
          }
          else
          {

            filename = file_dirent[n]->d_name;
            TBSYS_LOG(DEBUG, "check file %s,%s", directory, filename);
            if ((this->*pred)(disk_no, directory, filename)) 
            {
              ret = op(disk_no, directory, filename);
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

    //--------------------------------------------------------------------
    // class ObExpiredSSTablePool
    //--------------------------------------------------------------------
    ObExpiredSSTablePool::ObExpiredSSTablePool():inited_(false),files_list_(NULL)
    {
    }
    
    ObExpiredSSTablePool::~ObExpiredSSTablePool()
    {
      CThreadGuard guard(&lock_);
      if (files_list_ != NULL)
      {
        ob_free(files_list_);
        files_list_ = NULL;
        inited_ = false;
      }
    }

    int ObExpiredSSTablePool::init()
    {
      int ret = OB_SUCCESS;
      if (!inited_)
      {
        CThreadGuard guard(&lock_);
        if (!inited_)
        {
          files_list_ = static_cast<FileList *>(ob_malloc(OB_MAX_DISK_NUMBER * sizeof(*files_list_)));
          if (NULL == files_list_)
          {
            ret = OB_ERROR;
          }
          else
          {
            inited_ = true;
          }
        }
      }
      return ret;
    }
    
    int ObExpiredSSTablePool::scan()
    {
      int ret = OB_SUCCESS;
      if (!inited_)
      {
        ret = OB_ERROR;
      }
      else
      {
        CThreadGuard guard(&lock_);
        for(int32_t i=0;i<OB_MAX_DISK_NUMBER;++i)
        {
          ret = scan_disk(i + 1); //disk no start from 1
        }
      }
      return ret;
    }

    int ObExpiredSSTablePool::get_expired_sstable(int32_t disk_no,char *path,int32_t size)
    {
      int ret = OB_SUCCESS;
      if (!inited_ || disk_no <= 0 || NULL == path || size <= 0)
      {
        TBSYS_LOG(WARN,"expired sstable pool not init");
        ret = OB_ERROR;
      }
      else
      {
        CThreadGuard guard(&lock_);
        if ( files_list_[disk_no - 1].files_num <= 0)
        {
          scan_disk(disk_no);
        }

        if (files_list_[disk_no - 1].files_num > 0)
        {
          snprintf(path,size,"%s",files_list_[disk_no - 1].files[ files_list_[disk_no -1].current_idx++ ]);
          TBSYS_LOG(DEBUG,"file [%s] will be reused",path);
        }
      }
      return ret;
    }

    int ObExpiredSSTablePool::scan_disk(int32_t disk_no)
    {
      int ret = OB_SUCCESS;
      if (disk_no <= 0 || disk_no > OB_MAX_DISK_NUMBER)
      {
        ret = OB_ERROR;
      }
      else
      {
        FileList *list = &files_list_[disk_no - 1];
        list->files_num = 0;
        list->current_idx = 0;

        const char *data_dir = ObChunkServerMain::get_instance()->get_chunk_server().get_param().get_datadir_path();
        char path[OB_MAX_FILE_NAME_LENGTH];
        snprintf(path,sizeof(path),"%s/%d/%s",data_dir,disk_no,"Recycle");

        if (FileDirectoryUtils::is_directory(path))
        {
          DIR* dir = NULL;
          if ( NULL == (dir= opendir(path) ))
          {
            TBSYS_LOG(WARN,"open %s failed",path);
            ret = OB_ERROR;
          }
          else
          {
            struct dirent dp;
            struct dirent *result = NULL;
            while( 0 == (readdir_r(dir,&dp,&result)) && result)
            {
              char *name = result->d_name;
              if (((name[0] == '.') && (name[1] == '\0')) //ignore . & ..
                  || ((name[0] == '.') && (name[1] == '.') && (name[2] == '\0')))
              {
                continue;
              }
              snprintf (list->files[list->files_num], sizeof(list->files[list->files_num]), "%s/%s", path, name);
              if (++list->files_num >= MAX_FILES_NUM)
              {
                break;
              }
            }

            if (dir != NULL)
            {
              closedir(dir);
              dir = NULL;
            }
          }
        }
      }
      return ret;
    }
  } /* chunkserver */
} /* oceanbase */
