/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: ob_tablet_manager.cpp,v 0.1 2010/08/19 17:42:33 chuanhui Exp $
 *
 * Authors:
 *   chuanhui <rizhao.ych@taobao.com>
 *     - some work details if you want
 *
 */

#include "ob_tablet_manager.h"
#include "profiler.h"
#include "common/ob_malloc.h"
#include "common/ob_range.h"
#include "common/utility.h"
#include "common/ob_schema.h"
#include "common/ob_read_common_data.h"
#include "common/ob_tablet_info.h"
#include "common/ob_scanner.h"
#include "common/ob_atomic.h"
#include "sstable/ob_sstable_getter.h"
#include "sstable/ob_disk_path.h"
#include "ob_tablet.h"
#include "ob_root_server_rpc.h"
#include "ob_chunk_server.h"
#include "ob_chunk_server_main.h"
#include "ob_chunk_merge.h"
#include "ob_file_recycle.h"
#include "ob_chunk_server_stat.h"

#define LOG_CACHE_MEMORY_USAGE(header) \
  do  { \
    TBSYS_LOG(INFO, "%s cur_serving_idx_ =%ld, mgr_status_ =%ld," \
        "table memory usage=%ld," \
        "serving block index cache=%ld, block cache=%ld," \
        "unserving block index cache=%ld, block cache=%ld", \
        header, cur_serving_idx_, mgr_status_, \
        ob_get_mod_memory_usage(ObModIds::OB_CS_TABLET_IMAGE), \
        get_serving_block_index_cache().get_cache_mem_size(), \
        get_serving_block_cache().size(), \
        get_unserving_block_index_cache().get_cache_mem_size(), \
        get_unserving_block_cache().size()); \
  } while(0);

#define LOG_CACHE_MEMORY_USAGE_E(header, manager) \
  do  { \
    TBSYS_LOG(INFO, "%s cur_serving_idx_ =%ld, mgr_status_ =%ld," \
        "table memory usage=%ld," \
        "serving block index cache=%ld, block cache=%ld," \
        "unserving block index cache=%ld, block cache=%ld", \
        header, manager->cur_serving_idx_, manager->mgr_status_, \
        ob_get_mod_memory_usage(ObModIds::OB_CS_TABLET_IMAGE), \
        manager->get_serving_block_index_cache().get_cache_mem_size(), \
        manager->get_serving_block_cache().size(), \
        manager->get_unserving_block_index_cache().get_cache_mem_size(), \
        manager->get_unserving_block_cache().size()); \
  } while(0);

namespace oceanbase
{
  namespace chunkserver
  {
    using namespace oceanbase::common;
    using namespace oceanbase::sstable;

    ObTabletManager::ObTabletManager()
    : is_init_(false), 
      cur_serving_idx_(0), 
      mgr_status_(NORMAL), 
      max_sstable_file_seq_(0),
      sstable_row_cache_(NULL),
      regular_recycler_(*this),
      scan_recycler_(*this),
      tablet_image_(fileinfo_cache_),
      param_(NULL)
    {
    }

    ObTabletManager::~ObTabletManager()
    {
      destroy();
    }

    int ObTabletManager::init(const ObBlockCacheConf& bc_conf,
                              const ObBlockIndexCacheConf& bic_conf, 
                              const int64_t sstable_row_cache_size,
                              const char* data_dir, 
                              const int64_t max_sstable_size)
    {
      int err = OB_SUCCESS;

      if (NULL == data_dir || max_sstable_size <= 0)
      {
        TBSYS_LOG(WARN, "invalid parameter, data_dir=%s, max_sstable_size=%ld", 
                  data_dir, max_sstable_size);
        err = OB_INVALID_ARGUMENT;
      }
      else if (!is_init_)
      {
        if (OB_SUCCESS == err)
        {
          err = fileinfo_cache_.init(bc_conf.ficache_max_num);
        }
        if (OB_SUCCESS == err)
        {
          block_cache_[cur_serving_idx_].set_fileinfo_cache(fileinfo_cache_);
          err = block_cache_[cur_serving_idx_].init(bc_conf);
        }
        if (OB_SUCCESS == err)
        {
          block_index_cache_[cur_serving_idx_].set_fileinfo_cache(fileinfo_cache_);
          err = block_index_cache_[cur_serving_idx_].init(bic_conf);
        }

        if (OB_SUCCESS == err)
        {
          if (sstable_row_cache_size > 0)
          {
            sstable_row_cache_ = new (std::nothrow) ObSSTableRowCache();
            if (NULL != sstable_row_cache_)
            {
              if (OB_SUCCESS != (err = sstable_row_cache_->init(sstable_row_cache_size)))
              {
                TBSYS_LOG(ERROR, "init sstable row cache failed");
              }
            }
            else 
            {
              TBSYS_LOG(WARN, "failed to new sstable row cache");
              err = OB_ERROR;
            }
          }
        }

        if (OB_SUCCESS == err)
        {
          err = disk_manager_.scan(data_dir, max_sstable_size);
        }


        if (OB_SUCCESS == err)
        {
          is_init_ = true;
        }
      }

      return err;
    }

    int ObTabletManager::init(const ObChunkServerParam* param)
    {
      int err = OB_SUCCESS;

      if (NULL == param)
      {
        TBSYS_LOG(ERROR, "invalid parameter, param is NULL");
        err = OB_INVALID_ARGUMENT;
      }
      else 
      {
        err = init(param->get_block_cache_conf(), param->get_block_index_cache_conf(), 
                   param->get_sstable_row_cache_size(), param->get_datadir_path(), 
                   param->get_max_sstable_size());
      }

      if (OB_SUCCESS == err)
      {
        param_ = param;
      }

      if (OB_SUCCESS == err)
      {
        if (param_->get_join_cache_conf().cache_mem_size > 0) // <= 0 will disable the join cache
        {
          if ( (err = join_cache_.init(param_->get_join_cache_conf().cache_mem_size)) != OB_SUCCESS)
          {
            TBSYS_LOG(ERROR, "init join cache failed");
          }
        }
      }

      return err;
    }

    int ObTabletManager::start_merge_thread()
    {
      return chunk_merge_.init(this);
    }

    int ObTabletManager::start_cache_thread()
    {
      return cache_thread_.init(this);
    }

    ObChunkMerge & ObTabletManager::get_chunk_merge() 
    {
      return chunk_merge_;
    }

    ObCompactSSTableMemThread& ObTabletManager::get_cache_thread()
    {
      return cache_thread_;
    }

    void ObTabletManager::destroy()
    {
      if ( is_init_ )
      {
        is_init_ = false;

        chunk_merge_.destroy();
        cache_thread_.destroy();
        fileinfo_cache_.destroy();
        for (uint64_t i = 0; i < TABLET_ARRAY_NUM; ++i)
        {
          block_cache_[i].destroy();
          block_index_cache_[i].destroy();
        }
        join_cache_.destroy();
        if (NULL != sstable_row_cache_)
        {
          sstable_row_cache_->destroy();
          delete sstable_row_cache_;
          sstable_row_cache_ = NULL;
        }
      }
    }

    int ObTabletManager::migrate_tablet(const common::ObRange& range, 
                                        const common::ObServer& dest_server,
                                        char (*src_path)[OB_MAX_FILE_NAME_LENGTH],
                                        char (*dest_path)[OB_MAX_FILE_NAME_LENGTH], 
                                        int64_t& num_file,
                                        int64_t& tablet_version,
                                        int32_t& dest_disk_no,
                                        uint64_t & crc_sum)
    {
      int rc = OB_SUCCESS;
      ObMultiVersionTabletImage & tablet_image = get_serving_tablet_image();
      ObTablet * tablet = NULL;
      char dest_dir_buf[OB_MAX_FILE_NAME_LENGTH];

      if (NULL == src_path || NULL == dest_path)
      {
        TBSYS_LOG(ERROR, "error parameters, src_path=%p, dest_path=%p", src_path, dest_path);
        rc = OB_INVALID_ARGUMENT;
      }

      if (OB_SUCCESS == rc)
      {
        // TODO , get newest tablet.
        rc = tablet_image.acquire_tablet(range, ObMultiVersionTabletImage::SCAN_FORWARD, 0, tablet);
        if (OB_SUCCESS != rc || NULL == tablet)
        {
          TBSYS_LOG(ERROR, "acquire tablet error.");
        }
        else if ( range.compare_with_startkey2(tablet->get_range()) != 0
            || range.compare_with_endkey2(tablet->get_range()) != 0)
        {
          char range_buf[OB_RANGE_STR_BUFSIZ];
          range.to_string(range_buf, OB_RANGE_STR_BUFSIZ);
          TBSYS_LOG(INFO, "migrate tablet range = <%s>", range_buf);
          tablet->get_range().to_string(range_buf, OB_RANGE_STR_BUFSIZ);
          TBSYS_LOG(INFO, "not equal to local tablet range = <%s>", range_buf);
          rc = OB_ERROR;
        }
        else
        {
          TBSYS_LOG(INFO, "acquire tablet  success, tablet=%p", tablet);
        }
      }

      // get destination disk no and dest directory for store sstable files.
      dest_disk_no = 0;
      memset(dest_dir_buf, 0, OB_MAX_FILE_NAME_LENGTH);
      // buffer size set to OB_MAX_FILE_NAME_LENGTH, 
      // make deserialize ObString copy to dest_dir_buf
      ObString dest_directory(OB_MAX_FILE_NAME_LENGTH, 0, dest_dir_buf);
      if (OB_SUCCESS == rc && NULL != tablet)
      {
        ObRootServerRpcStub cs_rpc_stub; 
        rc = cs_rpc_stub.init(dest_server, &(THE_CHUNK_SERVER.get_client_manager()));
        if (OB_SUCCESS != rc)
        {
          TBSYS_LOG(ERROR, "init cs_rpc_stub error.");
        }
        else
        {
          rc = cs_rpc_stub.get_migrate_dest_location(
              tablet->get_occupy_size(), dest_disk_no, dest_directory);
          if (OB_SUCCESS == rc && dest_disk_no > 0)
          {
            TBSYS_LOG(INFO, "get_migrate_dest_location succeed, dest_disk_no=%d, %s",
                dest_disk_no, dest_dir_buf);
          }
          else
          {
            TBSYS_LOG(ERROR, "get_migrate_dest_location failed, rc = %d, dest_disk_no=%d", 
                rc, dest_disk_no);
          }
        }
      }

      if (OB_SUCCESS == rc && NULL != tablet)
      {
        num_file = tablet->get_sstable_id_list().get_array_index();
        tablet_version = tablet->get_data_version();
        crc_sum = tablet->get_checksum();
        TBSYS_LOG(INFO, "migrate_tablet sstable file num =%ld , version=%ld, checksum=%lu",
            num_file, tablet_version, crc_sum);
      }

      // copy sstable files path & generate dest path
      if (OB_SUCCESS == rc && NULL != tablet && num_file > 0)
      {
        int64_t now = 0;
        now = tbsys::CTimeUtil::getTime();
        for(int64_t idx = 0; idx < num_file && OB_SUCCESS == rc; idx++)
        {
          ObSSTableId * sstable_id = tablet->get_sstable_id_list().at(idx);
          if ( NULL != sstable_id)
          {
            rc = get_sstable_path(*sstable_id, src_path[idx], OB_MAX_FILE_NAME_LENGTH);
            if ( OB_SUCCESS != rc )
            {
              TBSYS_LOG(WARN, "get sstable path error, rc=%d, sstable_id=%ld", 
                  rc, sstable_id->sstable_file_id_);
            }
            else
            {
              // generate dest dir
              rc = snprintf(dest_path[idx], 
                  OB_MAX_FILE_NAME_LENGTH, 
                  "%s/%ld.%ld", dest_dir_buf,
                  sstable_id->sstable_file_id_, now);  
              if (rc > 0) rc = OB_SUCCESS;
              else rc = OB_ERROR;
            }
          }
          else
          {
            TBSYS_LOG(ERROR, "sstable not exist, cannot happen.");
            rc = OB_ERROR;
          }
        }
      }

      // now we can release tablet no need wait to rsync complete.
      if ( NULL != tablet )
      {
        int ret = tablet_image.release_tablet(tablet);
        if ( OB_SUCCESS != ret )
        {
          TBSYS_LOG(WARN, "release tablet error.");
          rc = OB_ERROR;
        }
        else
        {
          tablet = NULL;
        }
      }

      if (OB_SUCCESS == rc && NULL != src_path && num_file > 0)
      {
        int64_t rsync_band_limit = THE_CHUNK_SERVER.get_param().get_rsync_band_limit(); 
        char cmd[MAX_COMMAND_LENGTH];
        char ip_addr[OB_IP_STR_BUFF];
        //send all sstable files of this tablet using scp
        for(int64_t idx = 0; idx < num_file && OB_SUCCESS == rc; idx++)
        {
          uint32_t ipv4 = dest_server.get_ipv4();

          rc = snprintf(ip_addr, OB_IP_STR_BUFF, 
              "%d.%d.%d.%d",
              (ipv4& 0xFF),
              (ipv4>> 8) & 0xFF,
              (ipv4>> 16) & 0xFF,
              (ipv4 >> 24) & 0xFF);

          if ( rc > 0)
          { 
            rc = snprintf(cmd, MAX_COMMAND_LENGTH, 
                "scp -oStrictHostKeyChecking=no -c arcfour -l %ld %s %s:%s",
                rsync_band_limit * 8, src_path[idx], ip_addr, dest_path[idx]);  
          }

          if( rc > 0)
          {
            TBSYS_LOG(INFO, "copy sstable file, idx=%ld, cmd=%s", idx, cmd);
            rc = system(cmd);
            if ( 0 != rc)
            {
              TBSYS_LOG(ERROR, "transfer sstable file[%ld]=[%s] failed, rc=%d", idx, (char*)src_path, rc);
              rc = OB_ERROR;
            }
            else
            {
              TBSYS_LOG(DEBUG, "transfer sstable file[%ld]=[%s] success.", idx, (char*)src_path);
              rc = OB_SUCCESS;
            }
          }
        }//end of send all sstable file
      }

      return rc;
    }

    int ObTabletManager::dest_load_tablet(const common::ObRange& range,
                                          char (*dest_path)[OB_MAX_FILE_NAME_LENGTH],
                                          const int64_t num_file, 
                                          const int64_t tablet_version,
                                          const int32_t dest_disk_no,
                                          const uint64_t crc_sum)
    {
      int rc = OB_SUCCESS;
      int idx = 0;
      //construct a new tablet and then add all sstable file to it 
      ObMultiVersionTabletImage & tablet_image = get_serving_tablet_image();

      ObTablet * tablet = NULL;
      ObTablet * old_tablet = NULL;
      int64_t serving_version = get_serving_data_version();

      char path[OB_MAX_FILE_NAME_LENGTH];
      char input_range_buf[OB_RANGE_STR_BUFSIZ];
      range.to_string(input_range_buf, OB_RANGE_STR_BUFSIZ);

      int tablet_exist = OB_ENTRY_NOT_EXIST;

      if (chunk_merge_.is_pending_in_upgrade())
      {
        TBSYS_LOG(WARN, "local merge in upgrade, cannot migrate "
            "new tablet = %s, version=%ld", input_range_buf, tablet_version);
        rc = OB_CS_EAGAIN;
      }
      if (tablet_version < serving_version 
          || (serving_version > 0 && tablet_version > serving_version + 1))
      {
        /**
         * 1. if server version is 0, it means cs is empty, it allow to 
         * migrate any tablet with any version 
         * 2. migrate in tablet version can't be greater than 
         * serving_vervion + 1 
         * 3. migrate in tablet version can't be less than serving 
         * version 
         */
        TBSYS_LOG(WARN, "migrate in tablet = %s version =%ld, local serving version=%ld",
            input_range_buf, tablet_version, serving_version);
        rc = OB_ERROR;
      }
      else if (OB_SUCCESS == ( tablet_exist = 
            tablet_image.acquire_tablet_all_version(
              range,  ObMultiVersionTabletImage::SCAN_FORWARD, 
              ObMultiVersionTabletImage::FROM_NEWEST_INDEX, 0, old_tablet)) )
      {
        char exist_range_buf[OB_RANGE_STR_BUFSIZ];
        old_tablet->get_range().to_string(exist_range_buf, OB_RANGE_STR_BUFSIZ);
        TBSYS_LOG(INFO, "tablet intersect with exist, input <%s> and exist <%s>",
            input_range_buf, exist_range_buf);
        if (tablet_version < old_tablet->get_data_version())
        {
          TBSYS_LOG(WARN, "migrate in tablet's version =%ld < local tablet's version=%ld",
              tablet_version, old_tablet->get_data_version());
          rc = OB_ERROR;
        }
        else if (tablet_version == old_tablet->get_data_version())
        {
          // equal to local tablet's version, verify checksum
          uint64_t exist_crc_sum = old_tablet->get_checksum();
          if (crc_sum == exist_crc_sum)
          {
            TBSYS_LOG(INFO, "migrate in tablet's version=%ld equal "
                "local tablet's version=%ld, and in crc=%lu equal local crc=%lu",
                tablet_version, old_tablet->get_data_version(),
                crc_sum, exist_crc_sum);
            rc = OB_CS_MIGRATE_IN_EXIST;
          }
          else
          {
            TBSYS_LOG(ERROR, "migrate in tablet's version=%ld equal "
                "local tablet's version=%ld, but in crc=%lu <> local crc=%lu",
                tablet_version, old_tablet->get_data_version(),
                crc_sum, exist_crc_sum);
            rc = OB_ERROR;
          }
        }
        else if (tablet_version == old_tablet->get_data_version() + 1)
        {
          // equal to local tablet's version + 1, add it;
          TBSYS_LOG(INFO, "migrate in tablet's version=%ld == "
              "local tablet's version+1, local_version=%ld, add it.",
              tablet_version, old_tablet->get_data_version());
          rc = OB_SUCCESS;
        }
        else
        {
          /**
           * if tablet is existent and migrate in tablet's version is 
           * greater than local version + 1, can't add this tablet. maybe 
           * this chunkserver merge too slow. because chunkserver must 
           * daily merge version by version and chunkserver must keep two 
           * continious tablet versions, if add a tablet whose version is 
           * larger than local versoin + 1, the tablet version isn't 
           * continious, then it can't destroy one tablet image to store 
           * the next tablet version when do next daily merge. 
           */
          TBSYS_LOG(WARN, "migrate in tablet's version=%ld > "
              "local tablet's version+1, local_version=%ld, can't load.",
              tablet_version, old_tablet->get_data_version());
          rc = OB_ERROR;
        }
      }

      if (OB_SUCCESS == rc)
      {
        rc = tablet_image.alloc_tablet_object(range, tablet_version, tablet);
        if (OB_SUCCESS != rc)
        {
          TBSYS_LOG(ERROR, "alloc tablet failed.");
        }
        else
        {
          tablet->set_disk_no(dest_disk_no);
          tablet->set_data_version(tablet_version);
        }
 
        for( ; idx < num_file && OB_SUCCESS == rc; idx++)
        {
          ObSSTableId sstable_id;
          sstable_id.sstable_file_id_     = allocate_sstable_file_seq(); 
          sstable_id.sstable_file_offset_ = 0;
          sstable_id.sstable_file_id_     = (sstable_id.sstable_file_id_ << 8) | (dest_disk_no & 0xff);
          rc = get_sstable_path(sstable_id, path, OB_MAX_FILE_NAME_LENGTH);
          if ( OB_SUCCESS != rc )
          {
            TBSYS_LOG(ERROR, "get sstable path error, rc=%d", rc);
          }
          else
          {
            TBSYS_LOG(INFO, "dest_load_tablet, rename %s -> %s", dest_path[idx], path);
            rc = rename(dest_path[idx], path);
            if (OB_SUCCESS != rc)
            {
              TBSYS_LOG(ERROR, "rename %s -> %s failed, error: %d, %s", 
                  dest_path[idx], path, errno, strerror(errno));
              rc = OB_IO_ERROR;
            }

            if (OB_SUCCESS == rc)
            {
              rc = tablet->add_sstable_by_id(sstable_id);
              if ( OB_SUCCESS != rc )
              {
                TBSYS_LOG(ERROR, "add sstable file  error.");
              }
            }
          }
        }
        
        if ( OB_SUCCESS == rc )
        {              
          bool for_create = get_serving_data_version() == 0 ? true : false;
          rc = tablet_image.add_tablet(tablet, true, for_create);  
          if(OB_SUCCESS != rc)
          {
            TBSYS_LOG(ERROR, "add tablet <%s> failed, rc=%d", input_range_buf, rc);
          }
          else
          {
            // save image file.
            rc = tablet_image.write(tablet->get_data_version(), dest_disk_no);
            disk_manager_.shrink_space(dest_disk_no, tablet->get_occupy_size());
          }
        }

        if ( OB_SUCCESS == rc && OB_SUCCESS == tablet_exist && NULL != old_tablet)
        {
          // if we have old version tablet, no need to merge.
          old_tablet->set_merged();
          tablet_image.write(old_tablet->get_data_version(), old_tablet->get_disk_no());
        }

      }

      if (OB_CS_MIGRATE_IN_EXIST == rc && NULL != old_tablet)
      {
        // migrate in tablet, need remerge..
        if (old_tablet->is_merged())
        {
          old_tablet->set_merged(0);
          old_tablet->set_removed(0);
          tablet_image.write(old_tablet->get_data_version(), old_tablet->get_disk_no());
        }
      }

      // cleanup, delete all migrated sstable files in /tmp;
      for ( int64_t index = 0; index < num_file; index++)
      {
        unlink(dest_path[index]);
      }

      if (NULL != old_tablet) tablet_image.release_tablet(old_tablet);

      return rc;
    }

    void ObTabletManager::start_gc(const int64_t recycle_version) 
    {
      TBSYS_LOG(INFO, "start gc");
      UNUSED(recycle_version);
      if (chunk_merge_.is_merge_stoped())
      {
        scan_recycler_.recycle();
      }
      return;
    }

    int ObTabletManager::merge_multi_tablets(ObTabletReportInfoList& tablet_list)
    {
      int err = OB_SUCCESS;
      ObMultiTabletMerger* multi_tablet_merger = NULL;

      if (NULL == (multi_tablet_merger = GET_TSI_MULT(ObMultiTabletMerger, 
        TSI_CS_MULTI_TABLET_MERGER_1))) 
      {
        TBSYS_LOG(ERROR, "cannot get ObMultiTabletMerger object");
        err = OB_ALLOCATE_MEMORY_FAILED;
      }
      else
      {
        err = multi_tablet_merger->merge_tablets(*this, tablet_list, get_serving_data_version());
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN, "failed to merge multi-tablets");
        }
        multi_tablet_merger->cleanup();
      }

      return err;
    }

    int ObTabletManager::sync_all_tablet_images()
    {
      int ret = OB_SUCCESS;
      int32_t disk_num = 0;
      const int32_t* disk_no_array = disk_manager_.get_disk_no_array(disk_num);

      /**
       * after daily merge, in order to ensure the tablet image in 
       * memroy is consistent with the meta file in disk, we flush the
       * meta file of each disk with two version, ignore the return 
       * value. this function must be called before 
       * tablet_image.upgrade_service() 
       *  
       * the cs_admin tool also can force sync all tablet images by 
       * rpc command 
       */
      if (disk_num > 0 && NULL != disk_no_array)
      {
        /**
         * ignore the write fail status, we just ensure flush meta file 
         * of each disk once 
         */
        ret = tablet_image_.sync_all_images(disk_no_array, disk_num);
      }
      else
      {
        TBSYS_LOG(WARN, "sync all talbet images invalid argument, "
            "disk_no_array=%p is NULL or size=%d < 0", disk_no_array, disk_num);
        ret = OB_INVALID_ARGUMENT;
      }

      return ret;
    }

    int ObTabletManager::create_tablet(const ObRange& range, const int64_t data_version)
    {
      int err = OB_SUCCESS;
      ObTablet* tablet = NULL;

      if (range.empty() || 0 >= data_version || 
          (get_serving_data_version() > 0 && data_version != get_serving_data_version()))
      {
        TBSYS_LOG(ERROR, "create_tablet error, input range is empty "
            "or data_version=%ld or != serving data version %ld", 
            data_version, get_serving_data_version());
        err = OB_INVALID_ARGUMENT;
      }
      else
      {
        // find tablet if exist?
        err = tablet_image_.acquire_tablet(range, 
            ObMultiVersionTabletImage::SCAN_FORWARD, 0, tablet);
        if (OB_SUCCESS == err && NULL != tablet 
            && tablet->get_data_version() >= data_version )
        {
          char range_buf[OB_RANGE_STR_BUFSIZ];
          char exist_range_buf[OB_RANGE_STR_BUFSIZ];
          range.to_string(range_buf, sizeof(range_buf));
          tablet->get_range().to_string(exist_range_buf, sizeof(exist_range_buf));
          TBSYS_LOG(ERROR, "tablet (%ld) >= input version(%ld) "
              "already exists! dump input and exist, create_range=%s, exist_range=%s", 
              tablet->get_data_version(), data_version, range_buf, exist_range_buf);
          err = OB_ENTRY_EXIST;
        }
        else
        {
          err = OB_SUCCESS;
        }


        if (NULL != tablet)
        {
          // release tablet acquired 
          tablet_image_.release_tablet(tablet);
          tablet = NULL;
        }
      }

      if ( OB_SUCCESS != err)
      {
        // error return
      }
      else if ( OB_SUCCESS != (err = 
            tablet_image_.alloc_tablet_object(range, data_version, tablet)) )
      {
        TBSYS_LOG(ERROR, "allocate tablet object failed, ret=%d, version=%ld", 
            err, data_version);
      }
      else
      {
        // add empty tablet, there is no sstable files in it.
        // if scan or get query on this tablet, scan will return empty dataset.
        // TODO, create_tablet need send the %memtable_frozen_version 
        // as first version of this new tablet.
        tablet->set_data_version(data_version);
        // assign a disk for new tablet.
        tablet->set_disk_no(get_disk_manager().get_dest_disk());
        // load empty sstable files on first time, and create new tablet.
        if (OB_SUCCESS != (err = tablet_image_.add_tablet(tablet, true, true)))
        {
          TBSYS_LOG(ERROR, "create table failed add tablet to image error"
              ", version=%ld, disk=%d", data_version, tablet->get_disk_no());
        }
        else if (OB_SUCCESS != (err = tablet_image_.write(data_version, tablet->get_disk_no())))
        {
          // save new meta file in disk
          TBSYS_LOG(ERROR, "create table failed write meta file error"
              ", version=%ld, disk=%d", data_version, tablet->get_disk_no());
        } 
      }

      return err;
    }

    int ObTabletManager::load_tablets(const int32_t* disk_no_array, const int32_t size)
    {
      int err = OB_SUCCESS;

      bool load_sstable = THE_CHUNK_SERVER.get_param().get_lazy_load_sstable() == 0 ;

      if (NULL == disk_no_array || size <= 0)
      {
        TBSYS_LOG(WARN, "invalid param, disk_no_array=%p, size=%d", disk_no_array, size);
        err = OB_INVALID_ARGUMENT;
      }
      else if (OB_SUCCESS != (err = 
            get_serving_tablet_image().load_tablets(disk_no_array, size, load_sstable)) )
      {
        TBSYS_LOG(ERROR, "read tablets from disk error, ret=%d", err);
      }
      else
      {
        //get_serving_tablet_image().dump(true);
        max_sstable_file_seq_ = 
          get_serving_tablet_image().get_max_sstable_file_seq();
        TBSYS_LOG(INFO, "load tablets, sstable seq:%ld, load sstable=%d", max_sstable_file_seq_, load_sstable);
      }
      return err;
    }

    int ObTabletManager::get(const ObGetParam& get_param, ObScanner& scanner)
    {
      int err           = OB_SUCCESS;
      int64_t cell_size = get_param.get_cell_size();

      if (cell_size <= 0)
      {
        TBSYS_LOG(WARN, "invalid param, cell_size=%ld", cell_size);
        err = OB_INVALID_ARGUMENT;
      }
      else if (NULL == get_param.get_row_index() || get_param.get_row_size() <= 0)
      {
        TBSYS_LOG(WARN, "invalid get param, row_index=%p, row_size=%ld",
                  get_param.get_row_index(), get_param.get_row_size());
        err = OB_INVALID_ARGUMENT;
      }
      else
      {
        err = internal_get(get_param, scanner);
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN, "failed to get_, err=%d", err);
        }
      }

      return err;
    }

    int ObTabletManager::internal_get(const ObGetParam& get_param, 
                                      ObScanner& scanner)
    {
      int err                           = OB_SUCCESS;
      int64_t cell_size                 = get_param.get_cell_size();
      int64_t row_size                  = get_param.get_row_size();
      int64_t tablet_version            = 0;
      ObMultiVersionTabletImage& image  = get_serving_tablet_image();
      ObSSTableGetter* sstable_getter   = GET_TSI_MULT(ObSSTableGetter, TSI_CS_SSTABLE_GETTER_1);
      ObGetThreadContext* get_context   = GET_TSI_MULT(ObTabletManager::ObGetThreadContext, TSI_CS_GET_THREAD_CONTEXT_1);
      
      if (NULL == sstable_getter || NULL == get_context)
      {
        TBSYS_LOG(WARN, "get thread local instance of sstable getter failed");
        err = OB_ERROR;
      }
      else if (row_size <= 0 || row_size > OB_MAX_GET_ROW_NUMBER)
      {
        TBSYS_LOG(WARN, "no cell or too many row to get, row_size=%ld", row_size);
        err = OB_INVALID_ARGUMENT;
      }

#ifdef OB_PROFILER
      PROFILER_START("profiler_get");
      PROFILER_BEGIN("internal_get");
#endif

      if (OB_SUCCESS == err)
      {
#ifdef OB_PROFILER
        PROFILER_BEGIN("acquire_tablet");
#endif
        get_context->tablets_count_ = OB_MAX_GET_ROW_NUMBER;
        err = acquire_tablet(get_param, image, &get_context->tablets_[0], 
                             get_context->tablets_count_, tablet_version,
                             &(get_context->min_compactsstable_version_));
        FILL_TRACE_LOG("acquire_tablet:tablets_count_:%ld,version:%ld,err=%d",
                       get_context->tablets_count_,tablet_version,err);
#ifdef OB_PROFILER
        PROFILER_END();
#endif
      }

      //if can't find the first row in all the tablets, just exit get 
      if (OB_SUCCESS == err && 0 == get_context->tablets_count_)
      {
        scanner.set_data_version(tablet_version);
        scanner.set_is_req_fullfilled(true, 0);
        err = OB_CS_TABLET_NOT_EXIST;
      }
      else if (OB_SUCCESS != err || get_context->tablets_count_ <= 0 
               || cell_size < get_context->tablets_count_)
      {
        TBSYS_LOG(WARN, "failed to acquire tablet, cell size=%ld, "
                        "tablets size=%ld, err=%d", 
                  cell_size, get_context->tablets_count_, err);
        err = OB_ERROR;
      }
      else
      {
        ObGetThreadContext*& thread_get_context = get_cur_thread_get_contex();
        if (get_context != NULL)
        {
          thread_get_context = get_context;
        }
        
#ifdef OB_PROFILER
        PROFILER_BEGIN("init_sstable_getter");
#endif
        err = init_sstable_getter(get_param, &get_context->tablets_[0], 
                                  get_context->tablets_count_, *sstable_getter);
#ifdef OB_PROFILER
        PROFILER_END();
#endif
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN, "failed to init sstable getter, err=%d", err);
        }
      }

      // fill result to result objects of ObScanner 
      if (OB_SUCCESS == err)
      {
        scanner.set_data_version(tablet_version);
        err = fill_get_data(*sstable_getter, scanner);
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN, "failed to store cell array to scanner, err=%d", err);
        }
        FILL_TRACE_LOG("finish fill_get_data,err=%d",err);
      }
      /*
#ifdef OB_PROFILER
      PROFILER_BEGIN("release_tablet");
#endif

      if (get_context->tablets_count_ > 0 
          && OB_SUCCESS != release_tablet(image, &get_context->tablets_[0], 
                                          get_context->tablets_count_))
      {
        TBSYS_LOG(WARN, "failed to release tablets");
        err = OB_ERROR;
      }
        */
#ifdef OB_PROFILER
      PROFILER_END();
      PROFILER_DUMP();
      PROFILER_STOP();
#endif

      return err;
    }

    int ObTabletManager::end_get()
    {
      int ret = OB_SUCCESS;
      ObGetThreadContext*& get_context = get_cur_thread_get_contex();
      ObMultiVersionTabletImage& image  = get_serving_tablet_image();
      
      if (get_context != NULL && get_context->tablets_count_ > 0 
          && OB_SUCCESS != release_tablet(image, &get_context->tablets_[0], 
                                          get_context->tablets_count_))
      {
        TBSYS_LOG(WARN, "failed to release tablets");
        ret = OB_ERROR;
      }
      return ret;
    }

    int ObTabletManager::scan(const ObScanParam& scan_param, ObScanner& scanner)
    {
      int err = OB_SUCCESS;

      err = reset_query_thread_local_buffer();

      // suppose table_name, column_name already 
      // translated to table_id, column_id by MergeServer
      if (OB_SUCCESS == err)
      {
        scanner.set_mem_size_limit(scan_param.get_scan_size());
        err = internal_scan(scan_param, scanner);
      }


      return err;
    }

    int64_t ObTabletManager::get_serving_data_version(void) const
    {
      return tablet_image_.get_serving_version();
    }

    int ObTabletManager::prepare_tablet_image(const int64_t memtable_frozen_version)
    {
      int ret = OB_SUCCESS;
      int64_t retry_times = THE_CHUNK_SERVER.get_param().get_retry_times();
      int64_t sleep_interval = THE_CHUNK_SERVER.get_param().get_network_time_out();
      int64_t i = 0;
      while (i++ < retry_times)
      {
        ret = tablet_image_.prepare_for_merge(memtable_frozen_version);
        if (OB_SUCCESS == ret) 
        {
          break;
        }
        else if (OB_CS_EAGAIN == ret)
        {
          usleep(static_cast<useconds_t>(sleep_interval));
        }
        else
        {
          TBSYS_LOG(ERROR, "prepare image version = %ld error ret = %d", 
              memtable_frozen_version, ret);
          break;
        }
      }

      return ret;
    }

    int ObTabletManager::prepare_merge_tablets(const int64_t memtable_frozen_version)
    {
      int err = OB_SUCCESS;

      if ( OB_SUCCESS != (err = prepare_tablet_image(memtable_frozen_version)) ) 
      {
        TBSYS_LOG(WARN, "prepare_for_merge version = %ld error, err = %d", 
            memtable_frozen_version, err);
      }
      else if ( OB_SUCCESS != (err = drop_unserving_cache()) )
      {
        TBSYS_LOG(ERROR, "drop unserving cache for migrate cache failed,"
            "cannot launch new merge process, new version=%ld", memtable_frozen_version);
      }
      else
      {
        int64_t recycle_version = memtable_frozen_version
          - ObMultiVersionTabletImage::MAX_RESERVE_VERSION_COUNT;
        TBSYS_LOG(INFO, "prepare_recycle forzen version=%ld, recycle version=%ld",
            memtable_frozen_version, recycle_version);
        if (recycle_version > 0)
        {
          TBSYS_LOG(INFO, "prepare recycle version = %ld", recycle_version);
          regular_recycler_.backup_meta_files(recycle_version);
          regular_recycler_.prepare_recycle(recycle_version);
        }
      }
      return err;
    }

    int ObTabletManager::merge_tablets(const int64_t memtable_frozen_version)
    {
      int ret = OB_SUCCESS;

      TBSYS_LOG(INFO,"start merge");

      if (OB_SUCCESS == ret)
      {
        //update schema,
        //new version coming,clear join cache
        join_cache_.destroy();
        if (param_->get_join_cache_conf().cache_mem_size > 0) // <= 0 will disable the join cache
        {
          if ( (ret = join_cache_.init(param_->get_join_cache_conf().cache_mem_size)) != OB_SUCCESS)
          {
            TBSYS_LOG(ERROR,"init join cache failed");
          }
        }
        disk_manager_.scan(param_->get_datadir_path(),param_->get_max_sstable_size());
        chunk_merge_.schedule(memtable_frozen_version);
      }
      return ret;
    }

    int ObTabletManager::report_capacity_info()
    {
      int err = OB_SUCCESS;
      if (!is_init_)
      {
        TBSYS_LOG(ERROR, "report_capacity_info not init");
        err = OB_NOT_INIT;
      }
      else if (cur_serving_idx_ >= TABLET_ARRAY_NUM)
      {
        TBSYS_LOG(ERROR, "report_capacity_info invalid status, cur_serving_idx=%ld", cur_serving_idx_);
        err = OB_ERROR;
      }
      else
      {
        const ObServer& root_server = THE_CHUNK_SERVER.get_root_server();
        ObRootServerRpcStub root_stub;
        err = root_stub.init(root_server, &(THE_CHUNK_SERVER.get_client_manager()));
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN, "failed to init root stub, err=%d", err);
        }
        else
        {
          err = root_stub.report_capacity_info(THE_CHUNK_SERVER.get_self(), 
              disk_manager_.get_total_capacity(), disk_manager_.get_total_used());
          if (OB_SUCCESS != err)
          {
            TBSYS_LOG(WARN, "failed to report capacity info, err=%d", err);
          }
        }
      }

      return err;
    } 

    int ObTabletManager::report_tablets()
    {
      int err = OB_SUCCESS;
      ObTablet* tablet = NULL;
      ObTabletReportInfo tablet_info;
      char range_buf[OB_RANGE_STR_BUFSIZ];
      // since report_tablets process never use block_uncompressed_buffer_
      // so borrow it to store ObTabletReportInfoList
      int64_t num = OB_MAX_TABLET_LIST_NUMBER;
      int64_t max_serialize_size = OB_MAX_PACKET_LENGTH - 1024;
      int64_t serialize_size = 0;
      int64_t cur_tablet_brother_cnt = 0;
      ObTabletReportInfoList *report_info_list_first = GET_TSI_MULT(ObTabletReportInfoList, TSI_CS_TABLET_REPORT_INFO_LIST_1);
      ObTabletReportInfoList *report_info_list_second = GET_TSI_MULT(ObTabletReportInfoList, TSI_CS_TABLET_REPORT_INFO_LIST_2);
      ObTabletReportInfoList *report_info_list = report_info_list_first;
      ObTabletReportInfoList *report_info_list_rollback = NULL;
      if (NULL == report_info_list_first || NULL == report_info_list_second)
      {
        err = OB_ALLOCATE_MEMORY_FAILED;
      }
      else
      {
        report_info_list_first->reset();
        report_info_list_second->reset();
      }

      ObMultiVersionTabletImage& image = get_serving_tablet_image();
      if (OB_SUCCESS == err) err = image.begin_scan_tablets();
      if (OB_ITER_END == err)
      {
        image.end_scan_tablets();
        // iter ends
      }
      else if (OB_SUCCESS != err)
      {
        TBSYS_LOG(WARN, "failed to scan tablets, tablet=%p, err=%d",
                  tablet, err);
        err = OB_ERROR;
      }
      else
      {
        while (OB_SUCCESS == err)
        {
          while (OB_SUCCESS == err && num > 0)
          {
            err = image.get_next_tablet(tablet);
            if (OB_ITER_END == err)
            {
              // iter ends
            }
            else if (OB_SUCCESS != err || NULL == tablet)
            {
              TBSYS_LOG(WARN, "failed to get next tablet, err=%d, tablet=%p",
                  err, tablet);
              err = OB_ERROR;
            }
            else if (tablet->get_data_version() != image.get_serving_version())
            {
              image.release_tablet(tablet);
              continue;
            }
            else if (tablet->is_removed())
            {
              tablet->get_range().to_string(range_buf, OB_RANGE_STR_BUFSIZ);
              TBSYS_LOG(WARN, "report: ignore removed range = <%s>", range_buf);
              image.release_tablet(tablet);
              continue;
            }
            else
            {

              if (!tablet->get_range().border_flag_.is_left_open_right_closed())
              {
                tablet->get_range().to_string(range_buf, OB_RANGE_STR_BUFSIZ);
                TBSYS_LOG(WARN, "report illegal tablet range = <%s>", range_buf);
              }

              fill_tablet_info(*tablet, tablet_info);
              err = report_info_list->add_tablet(tablet_info);
              if (OB_SUCCESS != err)
              {
                TBSYS_LOG(WARN, "failed to add tablet info, num=%ld, err=%d", num, err);
              }
              else
              {
                serialize_size += tablet_info.get_serialize_size();
                if (!tablet->is_with_next_brother() 
                    && serialize_size <= max_serialize_size)
                {
                  cur_tablet_brother_cnt = 0;
                }
                else
                {
                  cur_tablet_brother_cnt ++;
                }
                --num;
              }

              if (OB_SUCCESS != image.release_tablet(tablet))
              {
                TBSYS_LOG(WARN, "failed to release tablet, tablet=%p", tablet);
                err = OB_ERROR;
              }

              if (serialize_size > max_serialize_size)
              {
                break;
              }
            }
          }

          if (OB_SUCCESS == err && cur_tablet_brother_cnt > 0)
          {
            if (serialize_size > max_serialize_size)
            {
              /**
               * FIXME: it's better to ensure the tablets splited from one 
               * tablet are reported in one packet, in this case, one tablet 
               * splits more than 1024 tablets, after add the last tablet, the
               * serialize size is greater than packet size, we need rollback 
               * the last tablet, and report the tablets more than one packet,
               * rootserver is also handle this case. 
               */
              TBSYS_LOG(WARN, "one tablet splited more than %ld tablets, "
                              "and the serialize size is greater than packet size, "
                              "rollback the last tablet, and can't report in one packet "
                              "atomicly, serialize_size=%ld, max_serialize_size=%ld",
                OB_MAX_TABLET_LIST_NUMBER, serialize_size, max_serialize_size);
              cur_tablet_brother_cnt = 1;
            }
            if (cur_tablet_brother_cnt < OB_MAX_TABLET_LIST_NUMBER)
            {
              if (report_info_list == report_info_list_first)
              {
                report_info_list_rollback = report_info_list_second;
              }
              else
              {
                report_info_list_rollback = report_info_list_first;
              }
              report_info_list_rollback->reset();
              err = report_info_list->rollback(*report_info_list_rollback, cur_tablet_brother_cnt);
              if (OB_SUCCESS != err)
              {
                TBSYS_LOG(WARN, "failed to rollback tablet info report list, err=%d", err);
              }
              else if (report_info_list->get_serialize_size() > max_serialize_size)
              {
                TBSYS_LOG(ERROR, "report_info_list serialize_size: %ld still greater than %ld", 
                    report_info_list->get_serialize_size(), max_serialize_size);
                err = OB_ERROR;
              }
            }
            else
            {
              /**
               * FIXME: it's better to ensure the tablets splited from one 
               * tablet are reported in one packet, in this case, rootserver 
               * can ensure the atomicity. we only report 1024 tablets to 
               * rootserver each time, if one tablet splits more than 1024 
               * tablets, we can't report all the tablets in one packet, we 
               * report the tablets more than one packet, rootserver is also 
               * handle this case. 
               */
              TBSYS_LOG(WARN, "one tablet splited more than %ld tablets, "
                              "can't report in one packet atomicly",
                OB_MAX_TABLET_LIST_NUMBER);
              cur_tablet_brother_cnt = 0;
            }
          }

          if (OB_SUCCESS == err)
          {
            err = send_tablet_report(*report_info_list, true);
            if (OB_SUCCESS != err)
            {
              TBSYS_LOG(WARN, "failed to send tablet info report, err=%d", err);
            }
            // ignore timeout error, continue report remain tablets;
            if (OB_RESPONSE_TIME_OUT == err)
            {
              err = OB_SUCCESS;
            }
          }

          if (OB_SUCCESS == err)
          {
            serialize_size = 0 ;
            num = OB_MAX_TABLET_LIST_NUMBER;
            report_info_list->reset();
          }

          if (OB_SUCCESS == err && cur_tablet_brother_cnt > 0)
          {
            num = OB_MAX_TABLET_LIST_NUMBER - cur_tablet_brother_cnt;
            report_info_list = report_info_list_rollback;
            serialize_size = report_info_list->get_serialize_size();
          }
        }
        image.end_scan_tablets();
      }

      if (OB_ITER_END == err)
      {
        err = send_tablet_report(*report_info_list, false);
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN, "failed to send tablet report in last round, ret=%d", err);
        }
      }


      return err;
    }

    int ObTabletManager::dump()
    {
      return OB_SUCCESS;
    }

    int ObTabletManager::init_sstable_scanner(const ObScanParam& scan_param, 
        const ObTablet* tablet, ObSSTableScanner& sstable_scanner)
    {
      int err = OB_SUCCESS;
      ObSSTableReader* sstable_reader = NULL;
      int32_t size = 1;

      if (NULL == tablet)
      {
        TBSYS_LOG(WARN, "invalid param, tablet=%p", tablet);
        err = OB_INVALID_ARGUMENT;
      }
      else if (OB_SUCCESS != (err = tablet->find_sstable(
              *scan_param.get_range(), &sstable_reader, size)) )
      {
        TBSYS_LOG(WARN, "find_sstable err=%d, size=%d", err, size);
      }
      else
      {
        err = sstable_scanner.set_scan_param(scan_param, sstable_reader,
          get_serving_block_cache(), get_serving_block_index_cache());
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN, "sstable scanner set scan parameter error.");
        }
      }
      return err;
    }

    ObTablet*& ObTabletManager::get_cur_thread_scan_tablet()
    {
      static __thread ObTablet* cur_thread_scan_tablet = NULL;
      
      return cur_thread_scan_tablet;
    }

    ObTabletManager::ObGetThreadContext*& ObTabletManager::get_cur_thread_get_contex()
    {
      static __thread ObGetThreadContext* cur_thread_get_context = NULL;

      return cur_thread_get_context;
    }

    int ObTabletManager::internal_scan(const ObScanParam& scan_param, ObScanner& scanner)
    {
      int err = OB_SUCCESS;
      ObTablet* tablet = NULL;
      ObTablet*& scan_tablet = get_cur_thread_scan_tablet();
      ObSSTableScanner *sstable_scanner = GET_TSI_MULT(ObSSTableScanner, TSI_CS_SSTABLE_SCANNER_1);

      int64_t query_version = 0;
      ObMultiVersionTabletImage::ScanDirection scan_direction = 
        scan_param.get_scan_direction() == ObScanParam::FORWARD ? 
        ObMultiVersionTabletImage::SCAN_FORWARD : ObMultiVersionTabletImage::SCAN_BACKWARD;

      const ObVersionRange & version_range = scan_param.get_version_range();
      if (!version_range.border_flag_.is_max_value() && version_range.end_version_ != 0)
      {
        query_version = version_range.end_version_;
      }

      if (NULL == sstable_scanner)
      {
        TBSYS_LOG(ERROR, "failed to get thread local sstable scanner, sstable_scanner=%p", 
                  sstable_scanner);
        err = OB_ALLOCATE_MEMORY_FAILED;
      }
      else if (OB_SUCCESS != (err = 
            tablet_image_.acquire_tablet(*scan_param.get_range(), 
              scan_direction, query_version, tablet)))
      {
        TBSYS_LOG(WARN, "failed to acquire tablet, tablet=%p, version=%ld, err=%d", 
            tablet, query_version, err);
      }
      else if (OB_SUCCESS != (err = init_sstable_scanner(scan_param, tablet, *sstable_scanner)))
      {
        TBSYS_LOG(ERROR, "init_sstable_scanner error=%d", err);
      }
      else
      {
        scanner.set_data_version(tablet->get_data_version());
        ObRange copy_range;
        deep_copy_range(*GET_TSI_MULT(ModuleArena, TSI_SSTABLE_MODULE_ARENA_1), 
          tablet->get_range(), copy_range);
        scanner.set_range_shallow_copy(copy_range);

        /**
         * the scan data will be merged and joined by merge join agent, 
         * so it doesn't fill scan data into obscanner, we just add som 
         * meta data into obscanner for merge join agent. 
         */
        scanner.set_is_req_fullfilled(true,1);

        ObCellInfo cell_ext;
        cell_ext.column_id_ = 2; //meaninglessness,just to suppress the scanner warn log
        cell_ext.table_id_ = scan_param.get_table_id();
        cell_ext.row_key_ = copy_range.end_key_;
        if ((err = scanner.add_cell(cell_ext)) != OB_SUCCESS)
        {
          TBSYS_LOG(DEBUG,"add cell failed(%d)",err);
        }

        if (TBSYS_LOGGER._level >= TBSYS_LOG_LEVEL_DEBUG)
        {
          char range_buf[OB_RANGE_STR_BUFSIZ];
          tablet->get_range().to_string(range_buf, OB_RANGE_STR_BUFSIZ);
          TBSYS_LOG(DEBUG, "scan result: tablet's data version=%ld, range=%s",
              tablet->get_data_version(), range_buf);

          common::ModuleArena* internal_buffer_arena = GET_TSI_MULT(common::ModuleArena, TSI_SSTABLE_MODULE_ARENA_1);
          TBSYS_LOG(DEBUG, "thread local page arena hold memory usage,"
              "total=%ld,used=%ld,pages=%ld", internal_buffer_arena->total(),
              internal_buffer_arena->used(), internal_buffer_arena->pages());
        }
      }

      //store the current scan tablet in order to release it when not use it.
      if (NULL != tablet)
      {
        scan_tablet = tablet;
      }

      return err;
    }

    int ObTabletManager::end_scan(bool release_tablet /*=true*/)
    {
      int err = OB_SUCCESS;
      ObSSTableScanner *sstable_scanner = GET_TSI_MULT(ObSSTableScanner, TSI_CS_SSTABLE_SCANNER_1);
      ObTablet*& scan_tablet = get_cur_thread_scan_tablet();

      if (NULL == sstable_scanner)
      {
        TBSYS_LOG(ERROR, "failed to get thread local sstable scanner, sstable_scanner=%p", 
                  sstable_scanner);
        err = OB_ALLOCATE_MEMORY_FAILED;
      }
      else
      {
        sstable_scanner->cleanup();
      }

      if (NULL != scan_tablet && release_tablet)
      {
        err = tablet_image_.release_tablet(scan_tablet);
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(ERROR, "failed to release tablet, tablet=%p,range:%s", scan_tablet,scan_range2str(scan_tablet->get_range()));
        }
        else
        {
          scan_tablet = NULL;
        }
      }

      return err;
    }

    int ObTabletManager::acquire_tablet(const ObGetParam& get_param, 
                                        ObMultiVersionTabletImage& image,
                                        ObTablet* tablets[], int64_t& size,
                                        int64_t& tablet_version,
                                        int64_t* compactsstable_version /*=NULL*/)
    {
      int err                 = OB_SUCCESS;
      int64_t cell_size       = get_param.get_cell_size();
      const ObGetParam::ObRowIndex* row_index = NULL;
      int64_t row_size        = 0;
      int64_t i               = 0;
      int64_t cur_tablet_ver  = 0;
      int64_t tablets_count   = 0;
      int64_t cache_version   = 0;
      int64_t tmp_version     = 0;
      ObRange range;

      if (cell_size <= 0)
      {
        TBSYS_LOG(WARN, "invalid param, cell_size=%ld", cell_size);
        err = OB_INVALID_ARGUMENT;
      }
      else if (NULL == tablets || size <= 0)
      {
        TBSYS_LOG(WARN, "invalid param, tablets=%p size=%ld", tablets, size);
        err = OB_INVALID_ARGUMENT;
      }
      else if (cur_serving_idx_ >= TABLET_ARRAY_NUM)
      {
        TBSYS_LOG(WARN, "invalid status, cur_serving_idx_=%ld", cur_serving_idx_);
        err = OB_ERROR;
      }
      else if (NULL == get_param.get_row_index() || get_param.get_row_size() <= 0)
      {
        TBSYS_LOG(WARN, "invalid get param");
        err = OB_ERROR;
      }
      else
      {
        row_index = get_param.get_row_index();
        row_size = get_param.get_row_size();
        for (i = 0; i < row_size && OB_SUCCESS == err; i++)
        {
          if (i >= size)
          {
            err = OB_SIZE_OVERFLOW;
            size = i - 1;
            break;
          }

          range.table_id_ = get_param[row_index[i].offset_]->table_id_;
          range.start_key_ = get_param[row_index[i].offset_]->row_key_;
          range.end_key_ = get_param[row_index[i].offset_]->row_key_;
          range.border_flag_.set_inclusive_start(); 
          range.border_flag_.set_inclusive_end();
          err = image.acquire_tablet(range, ObMultiVersionTabletImage::SCAN_FORWARD, 0, tablets[i]);
          if (OB_SUCCESS != err)
          {
            TBSYS_LOG(DEBUG, "the tablet does not exist, err=%d, rowkey: ", err);
            hex_dump(range.start_key_.ptr(), range.start_key_.length(), 
                     true, TBSYS_LOG_LEVEL_DEBUG);
            tablets[i] = NULL;
            err = OB_SUCCESS;
            /**
             * don't get the next row after the first non-existent row 
             * WARNING: please don't optimize this, mergeserver rely on this 
             * feature, when chunkserver can't find tablet for this rowkey, 
             * chunserver will return special error 
             * code(OB_CS_TABLET_NOT_EXIST) for this case. 
             */
            break;
          }
          else if (NULL != tablets[i])
          {
            if (0 == cur_tablet_ver)
            {
              cur_tablet_ver = tablets[i]->get_data_version();
            }
            else if (cur_tablet_ver != tablets[i]->get_data_version())
            {
              //release the tablet we don't need
              err = image.release_tablet(tablets[i]);
              if (OB_SUCCESS != err)
              {
                TBSYS_LOG(WARN, "failed to release tablet, tablet=%p, err=%d", 
                          tablets[i], err);
              }
              //tablet version change, break acquire tablet, skip current tablet
              break;
            }

            tmp_version = tablets[i]->get_cache_data_version();
            if (0 == cache_version)
            {
              cache_version = tmp_version;
            }
            else if (ObVersion::compare(tmp_version,cache_version) < 0)
            {
              cache_version = tmp_version;
            }
            
            tablets_count++;
          }
        }
      }

      if (OB_SUCCESS == err)
      {
        size = tablets_count;
        if (compactsstable_version != NULL)
        {
          *compactsstable_version = cache_version;
        }
      }

      //assign table_verion even though error happens
      if (0 == size || 0 == cur_tablet_ver)
      {
        /**
         * not found tablet or error happens, use the data verion of 
         * current tablet image instead 
         */
        cur_tablet_ver = get_serving_data_version();
      }
      tablet_version = cur_tablet_ver;

      return err;
    }

    int ObTabletManager::release_tablet(ObMultiVersionTabletImage& image,
                                        ObTablet* tablets[], const int64_t size)
    {
      int err = OB_SUCCESS;
      int ret = OB_SUCCESS;

      if (NULL == tablets || size <= 0)
      {
        TBSYS_LOG(WARN, "invalid param, tablets=%p size=%ld", tablets, size);
        ret = OB_INVALID_ARGUMENT;
      }
      else if (cur_serving_idx_ >= TABLET_ARRAY_NUM)
      {
        TBSYS_LOG(WARN, "invalid status, cur_serving_idx_=%ld", cur_serving_idx_);
        ret = OB_ERROR;
      }
      else
      {
        for (int64_t i = 0; i < size; ++i)
        {
          if (NULL != tablets[i])
          {
            err = image.release_tablet(tablets[i]);
            if (OB_SUCCESS != err)
            {
              TBSYS_LOG(WARN, "failed to release tablet, tablet=%p, err=%d", 
                        tablets[i], err);
              ret = err;
            }
          }
        }
      }

      return ret;
    }

    int ObTabletManager::init_sstable_getter(const ObGetParam& get_param, 
                                             ObTablet* tablets[],
                                             const int64_t size, 
                                             ObSSTableGetter& sstable_getter)
    {
      int err                   = OB_SUCCESS;
      ObSSTableReader* reader   = NULL;
      int32_t reader_size       = 1;
      const ObGetParam::ObRowIndex* row_index = NULL;
      ObGetThreadContext* get_context         = GET_TSI_MULT(ObGetThreadContext, TSI_CS_GET_THREAD_CONTEXT_1);
      
      if (NULL == get_context)
      {
        TBSYS_LOG(WARN, "get thread local instance of sstable getter failed");
        err = OB_ERROR;
      }
      else if (NULL == tablets || size <= 0)
      {
        TBSYS_LOG(WARN, "invalid param, tablets=%p size=%ld", tablets, size);
        err = OB_INVALID_ARGUMENT;
      }
      else if (NULL == get_param.get_row_index() || get_param.get_row_size() <= 0)
      {
        TBSYS_LOG(WARN, "invalid get param");
        err = OB_ERROR;
      }
      else
      {
        row_index = get_param.get_row_index();

        for (int64_t i = 0; i < size && OB_SUCCESS == err; ++i)
        {
          if (NULL == tablets[i])
          {
            get_context->readers_[i] = NULL;
            continue;
          }

          reader_size = 1;  //reset reader size, find_sstable will modify it
          /** FIXME: find sstable may return more than one reader */
          err = tablets[i]->find_sstable(get_param[row_index[i].offset_]->row_key_, 
                                         &reader, reader_size);
          if (OB_SUCCESS == err && 1 == reader_size)
          {
            TBSYS_LOG(DEBUG, "find_sstable reader=%p, reader_size=%d", 
                      reader, reader_size);
            get_context->readers_[i] = reader;

          }
          else if (OB_SIZE_OVERFLOW == err)
          {
            TBSYS_LOG(WARN, "find sstable reader by rowkey return more than"
                      "one reader, tablet=%p", tablets[i]);
            err = OB_ERROR;
            break;
          }
          else
          {
            TBSYS_LOG(DEBUG, "tablet find sstable reader failed, "
                             "tablet=%p, index=%ld, reader_size=%d",
                      tablets[i], i, reader_size);
            get_context->readers_[i] = NULL;
          }
        }

        if (OB_SUCCESS == err)
        {
          get_context->readers_count_ = size;
          err = sstable_getter.init(get_serving_block_cache() , 
                                    get_serving_block_index_cache(), 
                                    get_param, &get_context->readers_[0], 
                                    get_context->readers_count_,
                                    false, sstable_row_cache_);
          if (OB_SUCCESS != err)
          {
            TBSYS_LOG(WARN, "failed to set_get_param, err=%d", err);
          }
        }
      }

      return err;
    }

    int ObTabletManager::fill_get_data(ObIterator& iterator, ObScanner& scanner)
    {
      int err = OB_SUCCESS;
      ObCellInfo* cell = NULL;
      int64_t cells_count = 0;
      bool is_row_changed = false;

      while (OB_SUCCESS == (err = iterator.next_cell()))
      {
        err = iterator.get_cell(&cell, &is_row_changed);
        if (OB_SUCCESS != err || NULL == cell)
        {
          TBSYS_LOG(WARN, "failed to get cell, cell=%p, err=%d", cell, err);
          err = OB_ERROR;
        }
        else
        {
          err = scanner.add_cell(*cell, false, is_row_changed);
          if (OB_SIZE_OVERFLOW == err)
          {
            TBSYS_LOG(INFO, "ObScanner size full, cannot add any cell.");
            scanner.rollback();
            cells_count = dynamic_cast<ObSSTableGetter&>(iterator).get_handled_cells_in_param();
            scanner.set_is_req_fullfilled(false, cells_count);
            err = OB_SUCCESS;
            break;
          }
          else if (OB_SUCCESS != err)
          {
            TBSYS_LOG(WARN, "failed to add cell to scanner, err=%d", err);
            err = OB_ERROR;
          }
        }

        if (OB_SUCCESS != err)
        {
          break;
        }
      }

      if (OB_ITER_END == err)
      {
        cells_count = dynamic_cast<ObSSTableGetter&>(iterator).get_handled_cells_in_param();
        scanner.set_is_req_fullfilled(true, cells_count);
        err = OB_SUCCESS;
      }
      else if (OB_SUCCESS != err)
      {
        TBSYS_LOG(WARN, "error occurs while iterating, err=%d", err);
      }
      return err;
    }

    int ObTabletManager::fill_tablet_info(const ObTablet& tablet,
                                          ObTabletReportInfo& report_tablet_info)
    {
      int err = OB_SUCCESS;

      report_tablet_info.tablet_info_.range_ = tablet.get_range();
      report_tablet_info.tablet_info_.occupy_size_ = tablet.get_occupy_size();
      report_tablet_info.tablet_info_.row_count_ = tablet.get_row_count();
      uint64_t tablet_checksum = tablet.get_checksum();
      report_tablet_info.tablet_info_.crc_sum_ = tablet_checksum;

      const ObServer& self = THE_CHUNK_SERVER.get_self();
      report_tablet_info.tablet_location_.chunkserver_ = self;
      report_tablet_info.tablet_location_.tablet_version_ = tablet.get_data_version();
      report_tablet_info.tablet_location_.tablet_seq_ = tablet.get_sequence_num();

      return err;
    }

    int ObTabletManager::send_tablet_report(const ObTabletReportInfoList& tablets, bool has_more)
    {
      int err = OB_SUCCESS;

      const ObServer& root_server = THE_CHUNK_SERVER.get_root_server();
      int64_t retry_times = THE_CHUNK_SERVER.get_param().get_retry_times();
      ObRootServerRpcStub root_stub;

      if (OB_SUCCESS != (err = 
            root_stub.init(root_server, &(THE_CHUNK_SERVER.get_client_manager()))) )
      {
        TBSYS_LOG(WARN, "failed to init root stub, err=%d", err);
      }
      else
      {
        rpc_retry_wait(is_init_,  retry_times, err, 
            root_stub.report_tablets(tablets, 0 /*not used*/, has_more));
      }

      return err;
    }

    FileInfoCache&  ObTabletManager::get_fileinfo_cache()
    {
       return fileinfo_cache_;
    }

    ObBlockCache& ObTabletManager::get_serving_block_cache()
    {
       return block_cache_[cur_serving_idx_];
    }

    ObBlockCache& ObTabletManager::get_unserving_block_cache()
    {
      return block_cache_[(cur_serving_idx_ + 1) % TABLET_ARRAY_NUM];
    }

    ObBlockIndexCache& ObTabletManager::get_serving_block_index_cache()
    {
      return block_index_cache_[cur_serving_idx_];
    }

    ObBlockIndexCache& ObTabletManager::get_unserving_block_index_cache()
    {
      return block_index_cache_[(cur_serving_idx_ + 1) % TABLET_ARRAY_NUM];
    }

    ObMultiVersionTabletImage& ObTabletManager::get_serving_tablet_image()
    {
      return tablet_image_;
    }

    const ObMultiVersionTabletImage& ObTabletManager::get_serving_tablet_image() const
    {
      return tablet_image_;
    }

    ObDiskManager& ObTabletManager::get_disk_manager()
    {
      return disk_manager_;
    }

    ObRegularRecycler& ObTabletManager::get_regular_recycler()
    {
      return regular_recycler_;
    }

    ObScanRecycler& ObTabletManager::get_scan_recycler()
    {
      return scan_recycler_;
    }

    ObJoinCache& ObTabletManager::get_join_cache()
    {
      return join_cache_;
    }

    int ObTabletManager::build_unserving_cache()
    {
      int ret = OB_SUCCESS;

      if (NULL == param_)
      {
        TBSYS_LOG(WARN, "param is NULL");
        ret = OB_ERROR;
      }
      else 
      {
        ret = build_unserving_cache(param_->get_block_cache_conf(), 
                                    param_->get_block_index_cache_conf());
      }

      return ret;
    }

    int ObTabletManager::build_unserving_cache(const ObBlockCacheConf& bc_conf,
                                               const ObBlockIndexCacheConf& bic_conf)
    {
      int ret                             = OB_SUCCESS;
      ObBlockCache& dst_block_cache       = get_unserving_block_cache();
      ObBlockCache& src_block_cache       = get_serving_block_cache();
      ObBlockIndexCache& dst_index_cache  = get_unserving_block_index_cache();
      int64_t src_tablet_version          = tablet_image_.get_eldest_version();
      int64_t dst_tablet_version          = tablet_image_.get_newest_version();

      if (OB_SUCCESS == ret)
      {
        //initialize unserving block cache
        dst_block_cache.set_fileinfo_cache(fileinfo_cache_);
        ret = dst_block_cache.init(bc_conf);
      }

      if (OB_SUCCESS == ret)
      {
        //initialize unserving block index cache
        dst_index_cache.set_fileinfo_cache(fileinfo_cache_);
        ret = dst_index_cache.init(bic_conf);
      }

      if (OB_SUCCESS == ret && NULL != param_
          && param_->get_switch_cache_after_merge() > 0)
      {
        ret = switch_cache_utility_.switch_cache(tablet_image_, src_tablet_version,
                                                 dst_tablet_version, 
                                                 src_block_cache, dst_block_cache,
                                                 dst_index_cache);
      }

      return ret;
    }

    int ObTabletManager::drop_unserving_cache()
    {
      int ret                               = OB_SUCCESS;
      ObBlockCache& block_cache             = get_unserving_block_cache();
      ObBlockIndexCache& block_index_cache  = get_unserving_block_index_cache();

      ret = switch_cache_utility_.destroy_cache(block_cache, block_index_cache);

      return ret;
    }

    int64_t ObTabletManager::allocate_sstable_file_seq()
    {
      // TODO lock?
      return atomic_inc(&max_sstable_file_seq_);
    }

    /**
     * call by merge_tablets() after load all new tablets in Merging.
     * from this point, all (scan & get) request from client will be
     * serviced by new tablets.
     * now merge_tablets() can report tablets to RootServer.
     * then waiting RootServer for drop old tablets.
     */
    int ObTabletManager::switch_cache()
    {
      // TODO lock?
      atomic_exchange(&cur_serving_idx_ , (cur_serving_idx_ + 1) % TABLET_ARRAY_NUM);
      return OB_SUCCESS;
    }
  }
}
