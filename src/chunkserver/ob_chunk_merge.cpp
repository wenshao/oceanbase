/*
 * (C) 2007-2010 TaoBao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * ob_chunk_merge.cpp is for what ...
 *
 * Version: $id$
 *
 * Authors:
 *   MaoQi maoqi@taobao.com
 *
 */

#include "ob_chunk_server_main.h"
#include "common/ob_read_common_data.h"
#include "ob_tablet_image.h"
#include "common/utility.h"
#include "ob_chunk_merge.h"
#include "sstable/ob_disk_path.h"
#include "common/ob_trace_log.h"
#include "ob_tablet_manager.h"
#include "common/ob_atomic.h"
#include "common/file_directory_utils.h"

namespace oceanbase
{
  namespace chunkserver
  {
    using namespace tbutil;
    using namespace common;
    using namespace sstable;

    /*-----------------------------------------------------------------------------
     *  ObChunkMerge
     *-----------------------------------------------------------------------------*/

    ObChunkMerge::ObChunkMerge() : inited_(false),tablets_num_(0),
                                   tablet_index_(0),thread_num_(0),
                                   tablets_have_got_(0), active_thread_num_(0),
                                   frozen_version_(0), frozen_timestamp_(0),newest_frozen_version_(0),
                                   merge_start_time_(0),merge_last_end_time_(0),
                                   tablet_manager_(NULL),
                                   last_end_key_buffer_(common::OB_MAX_ROW_KEY_LENGTH),
                                   cur_row_key_buffer_(common::OB_MAX_ROW_KEY_LENGTH),
                                   round_start_(true),round_end_(false), pending_in_upgrade_(false),
                                   merge_load_high_(0),request_count_high_(0), merge_adjust_ratio_(0),
                                   merge_load_adjust_(0), merge_pause_row_count_(0), merge_pause_sleep_time_(0),
                                   merge_highload_sleep_time_(0)
    {
      //memset(reinterpret_cast<void *>(&pending_merge_),0,sizeof(pending_merge_));
      for(uint32_t i=0; i < sizeof(pending_merge_) / sizeof(pending_merge_[0]); ++i)
      {
        pending_merge_[i] = 0;
      }
    }

    void ObChunkMerge::set_config_param()
    {
      ObChunkServer& chunk_server = ObChunkServerMain::get_instance()->get_chunk_server();
      merge_load_high_ = chunk_server.get_param().get_merge_threshold_load_high();
      request_count_high_ = chunk_server.get_param().get_merge_threshold_request_high();
      merge_adjust_ratio_ = chunk_server.get_param().get_merge_adjust_ratio();
      merge_load_adjust_ = (merge_load_high_ * merge_adjust_ratio_) / 100;
      merge_pause_row_count_ = chunk_server.get_param().get_merge_pause_row_count();
      merge_pause_sleep_time_ = chunk_server.get_param().get_merge_pause_sleep_time();
      merge_highload_sleep_time_ = chunk_server.get_param().get_merge_highload_sleep_time();
    }

    int ObChunkMerge::create_merge_threads(const int32_t max_merge_thread)
    {
      int ret = OB_SUCCESS;

      active_thread_num_ = max_merge_thread;
      if (0 == thread_num_)
      {
        for (int32_t i=0; i < max_merge_thread; ++i)
        {
          if ( 0 != pthread_create(&tid_[thread_num_],NULL,ObChunkMerge::run,this))
          {
            TBSYS_LOG(ERROR, "create merge thread failed, exit.");
            ret = OB_ERROR;
            break;
          }
          TBSYS_LOG(INFO,"create merge thread [%d] success.",thread_num_);
          ++thread_num_;
        }
      }

      if (max_merge_thread != thread_num_)
      {
        ret = OB_ERROR;
        TBSYS_LOG(ERROR,"merge : create merge thread failed, create(%d) != need(%d)",
            thread_num_, max_merge_thread);
      }

      if (OB_SUCCESS == ret)
      {
        min_merge_thread_num_ = thread_num_ / 3;
        if (min_merge_thread_num_ == 0) min_merge_thread_num_ = 1;
      }

      return ret;
    }

    int ObChunkMerge::init(ObTabletManager *manager)
    {
      int ret = OB_SUCCESS;
      ObChunkServer& chunk_server = ObChunkServerMain::get_instance()->get_chunk_server();

      if (NULL == manager)
      {
        TBSYS_LOG(ERROR,"input error,manager is null");
        ret = OB_ERROR;
      }
      else if (!inited_)
      {
        inited_ = true;

        tablet_manager_ = manager;
        frozen_version_ = manager->get_serving_data_version();
        newest_frozen_version_ = frozen_version_; 

        pthread_mutex_init(&mutex_,NULL);
        pthread_cond_init(&cond_,NULL);

        int64_t max_merge_thread = chunk_server.get_param().get_max_merge_thread(); 
        if (max_merge_thread <= 0 || max_merge_thread > MAX_MERGE_THREAD)
          max_merge_thread = MAX_MERGE_THREAD;

        set_config_param();
        ret = create_merge_threads(static_cast<int32_t>(max_merge_thread));
      }
      else
      {
        TBSYS_LOG(WARN,"ObChunkMerge have been inited");
      }

      if (OB_SUCCESS != ret && inited_)
      {
        pthread_mutex_destroy(&mutex_);
        pthread_cond_destroy(&cond_);
        inited_ = false;
      }
      return ret;
    }

    void ObChunkMerge::destroy()
    {
      if (inited_)
      {
        if (0 == THE_CHUNK_SERVER.get_param().get_each_tablet_sync_meta()
            && !is_merge_stoped())
        {
          tablet_manager_->sync_all_tablet_images();
        }
        inited_ = false;
        pthread_cond_broadcast(&cond_);
        usleep(50);

        for(int32_t i = 0; i < thread_num_; ++i)
        {
          pthread_join(tid_[i],NULL);
        }
        pthread_cond_destroy(&cond_);
        pthread_mutex_destroy(&mutex_);
      }
    }

    bool ObChunkMerge::can_launch_next_round(const int64_t frozen_version)
    {
      bool ret = false;
      int64_t now = tbsys::CTimeUtil::getTime();

      if (inited_ && frozen_version > frozen_version_ && is_merge_stoped()
          && frozen_version > tablet_manager_->get_serving_data_version() 
          && now - merge_last_end_time_ > THE_CHUNK_SERVER.get_param().get_min_merge_interval())
      {
        ret = true;
      }

      return ret;
    }

    int ObChunkMerge::schedule(const int64_t frozen_version)
    {

      int ret = OB_SUCCESS;

      if (frozen_version > newest_frozen_version_)
      {
        newest_frozen_version_ = frozen_version;
      }

      // empty chunkserver, reset current frozen_version_ if 
      // chunkserver got new tablet by migrate in or create new table;
      if ( 0 == frozen_version_ )
      {
        frozen_version_ = tablet_manager_->get_serving_data_version();
      }

      if (1 >= frozen_version || (!can_launch_next_round(frozen_version)))
      {
        // do nothing
        TBSYS_LOG(INFO, "frozen_version=%ld, current frozen version = %ld, "
            "serving data version=%ld cannot launch next round.",
            frozen_version, frozen_version_, tablet_manager_->get_serving_data_version());
        ret = OB_CS_EAGAIN;
      }
      else if (0 == tablet_manager_->get_serving_data_version()) //new chunkserver
      {
        // empty chunkserver, no need to merge.
        TBSYS_LOG(INFO, "empty chunkserver , wait for migrate in.");
        ret = OB_CS_EAGAIN;
      }
      else
      {
        if (frozen_version - frozen_version_ > 1)
        {
          TBSYS_LOG(WARN, "merge is too slow,[%ld:%ld]", frozen_version_, frozen_version);
        }
        // only plus 1 in one merge process.
        frozen_version_ += 1;

        ret = start_round(frozen_version_);
        if (OB_SUCCESS != ret)
        {
          // start merge failed, maybe rootserver or updateserver not in serving.
          // restore old frozen_version_ for next merge process.
          frozen_version_ -= 1;
        }
      }

      if (inited_ && OB_SUCCESS == ret && thread_num_ > 0)
      {
        TBSYS_LOG(INFO, "wake up all merge threads, "
            "run new merge process with version=%ld", frozen_version_);
        merge_start_time_ = tbsys::CTimeUtil::getTime();
        round_start_ = true;
        round_end_ = false;
        pthread_cond_broadcast(&cond_);
      }

      return ret;
    }

    void* ObChunkMerge::run(void *arg)
    {
      if (NULL == arg)
      {
        TBSYS_LOG(ERROR,"internal merge : ObChunkServerMerge get a null arg");
      }
      else
      {
        ObChunkMerge *merge = reinterpret_cast<ObChunkMerge*>(arg);
        merge->merge_tablets();
      }
      return NULL;
    }

    void ObChunkMerge::merge_tablets()
    {
      int ret = OB_SUCCESS;
      const int64_t sleep_interval = 5000000;
      ObTablet *tablet = NULL;
      ObTabletMerger *merger = NULL;
      RowKeySwap  swap;
      int64_t merge_fail_count = 0;

      ObChunkServer&  chunk_server = ObChunkServerMain::get_instance()->get_chunk_server();

      if (OB_SUCCESS == ret)
      {
        merger = new (std::nothrow) ObTabletMerger(*this,*tablet_manager_);
      }

      if ( NULL == merger)
      {
        TBSYS_LOG(ERROR,"alloc ObTabletMerger failed");
        ret = OB_ERROR;
      }
      else
      { 
        swap.last_end_key_buf     = last_end_key_buffer_.get_buffer()->current(); 
        swap.last_end_key_buf_len = last_end_key_buffer_.get_buffer()->capacity();
        swap.start_key_buf        = cur_row_key_buffer_.get_buffer()->current();
        swap.start_key_buf_len    = cur_row_key_buffer_.get_buffer()->capacity();
        if ( (ret = merger->init(&swap)) != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR,"init merger failed [%d]",ret);
        }
      }
          

      while(OB_SUCCESS == ret)
      {
        if (!inited_)
        {
          break;
        }

        if ( !check_load())
        {
          TBSYS_LOG(INFO,"load is too high, go to sleep");
          pthread_mutex_lock(&mutex_);

          if (1 == active_thread_num_) 
          {
            pthread_mutex_unlock(&mutex_);
            usleep(sleep_interval); //5s
          }
          else
          {
            --active_thread_num_;
            pthread_cond_wait(&cond_,&mutex_);
            TBSYS_LOG(INFO,"to merge,active_thread_num_ :%ld",active_thread_num_);
            ++active_thread_num_;
            pthread_mutex_unlock(&mutex_);
          }

          if (!inited_)
          {
            break;
          }
        }

        pthread_mutex_lock(&mutex_);
        ret = get_tablets(tablet);
        while (true)
        {
          if (!inited_)
          {
            break;
          }
          if (OB_SUCCESS != ret)
          {
            pthread_mutex_unlock(&mutex_);
            usleep(sleep_interval); 
            // retry to get tablet until got one or got nothing.
            pthread_mutex_lock(&mutex_);
          }
          else if (NULL == tablet) // got nothing
          {
            --active_thread_num_;
            TBSYS_LOG(INFO,"there is no tablet need merge, sleep wait for new merge proecess.");
            pthread_cond_wait(&cond_,&mutex_);
            TBSYS_LOG(INFO,"awake by signal, active_thread_num_:%ld",active_thread_num_);
            // retry to get new tablet for merge.
            ++active_thread_num_;
          }
          else // got tablet for merge
          {
            break;
          }
          ret = get_tablets(tablet);
        }
        pthread_mutex_unlock(&mutex_);

        int64_t retry_times = chunk_server.get_param().get_retry_times();
        int64_t merge_per_disk = chunk_server.get_param().get_merge_thread_per_disk();

        // okay , we got a tablet for merge finally.
        if (NULL != tablet)
        {
          if (tablet->get_data_version() > frozen_version_)
          {
            //impossible
            TBSYS_LOG(ERROR,"local tablet version (%ld) > frozen_version_(%ld)",tablet->get_data_version(),frozen_version_);
            kill(getpid(),2);
          }
          else if ((tablet->get_merge_count() > retry_times) && (have_new_version_in_othercs(tablet)))
          {
            TBSYS_LOG(WARN,"too many times(%d),discard this tablet,wait for migrate copy.", tablet->get_merge_count());
            if (OB_SUCCESS == delete_tablet_on_rootserver(tablet))
            {
              TBSYS_LOG(INFO, "delete tablet (version=%ld) on rs succeed. ", tablet->get_data_version());
              tablet->set_merged();
              tablet->set_removed();
            }
          }
          else
          {
            if ( ((newest_frozen_version_ - tablet->get_data_version()) > chunk_server.get_param().get_max_version_gap()) )
            {
              TBSYS_LOG(ERROR,"this tablet version (%ld : %ld) is too old,maybe don't need to merge",
                  tablet->get_data_version(),newest_frozen_version_);
            }

            volatile uint32_t *ref = &pending_merge_[ tablet->get_disk_no() ];
            int err = OB_SUCCESS;
            if (*ref < merge_per_disk)
            {
              atomic_inc(ref);
              TBSYS_LOG(DEBUG,"get a tablet, start merge");
              if ((err = merger->merge_or_import(tablet,tablet->get_data_version() + 1)) != OB_SUCCESS
                  && OB_CS_TABLE_HAS_DELETED != err)
              {
                TBSYS_LOG(WARN,"merge tablet failed");
                if (++merge_fail_count > 5)
                {
                  usleep(sleep_interval);
                }
              }
              else
              {
                merge_fail_count = 0;
              }
              tablet->inc_merge_count();
              atomic_dec(ref);
            }
          }

          if (tablet_manager_->get_serving_tablet_image().release_tablet(tablet) != OB_SUCCESS)
          {
            TBSYS_LOG(WARN,"release tablet failed");
          }

          pthread_mutex_lock(&mutex_);
          ++tablets_have_got_;
          pthread_mutex_unlock(&mutex_);

        }

        if ( tablet_manager_->is_stoped() )
        {
          TBSYS_LOG(WARN,"stop in merging");
          ret = OB_CS_MERGE_CANCELED;
        }
      }

      if (NULL != merger)
      {
        delete merger;
        merger = NULL;
      }
    }

    int ObChunkMerge::fetch_frozen_time_busy_wait(const int64_t frozen_version, int64_t &frozen_time)
    {
      int ret = OB_SUCCESS;
      if (0 == frozen_version) 
      {
        TBSYS_LOG(ERROR,"invalid argument, frozen_version is 0");
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        ObMergerRpcProxy* rpc_proxy = THE_CHUNK_SERVER.get_rpc_proxy();
        if (NULL != rpc_proxy)
        {
          int64_t retry_times  = THE_CHUNK_SERVER.get_param().get_retry_times();
          rpc_retry_wait(inited_, retry_times, ret, 
            rpc_proxy->get_frozen_time(frozen_version, frozen_time));
        }
        else 
        {
          TBSYS_LOG(WARN, "get rpc proxy from chunkserver failed");
          ret = OB_ERROR;
        }
      }
      return ret;
    }

    int ObChunkMerge::fetch_frozen_schema_busy_wait(
      const int64_t frozen_version, ObSchemaManagerV2& schema)
    {
      int ret = OB_SUCCESS;
      if (0 == frozen_version) 
      {
        TBSYS_LOG(ERROR,"invalid argument, frozen_version is 0");
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        ObMergerRpcProxy* rpc_proxy = THE_CHUNK_SERVER.get_rpc_proxy();
        if (NULL != rpc_proxy)
        {
          int64_t retry_times  = THE_CHUNK_SERVER.get_param().get_retry_times();
          rpc_retry_wait(inited_, retry_times, ret, 
            rpc_proxy->get_frozen_schema(frozen_version, schema));
        }
        else 
        {
          TBSYS_LOG(WARN, "get rpc proxy from chunkserver failed");
          ret = OB_ERROR;
        }
      }
      return ret;
    }

    /**
     * luanch next merge round, do something before doing actual 
     * merge stuff .
     * 1. fetch new schema with current frozen version; must be 
     * compatible with last schema;
     * 2. fetch new freeze timestamp with current frozen version,
     * for TTL (filter expired line);
     * 3. prepare for next merge, drop block cache used in pervoius 
     * merge round, drop image slot used in prevoius version; 
     * 4. initialize import sstable instance;
     */
    int ObChunkMerge::start_round(const int64_t frozen_version)
    {
      int ret = OB_SUCCESS;
      // save schema used by last merge process .
      if (current_schema_.get_version() > 0)
      {
        last_schema_ = current_schema_;
      }

      const ObDiskManager& disk_manager = tablet_manager_->get_disk_manager();
      const char* app_name = THE_CHUNK_SERVER.get_param().get_application_name();

      // fetch frozen schema with frozen_version_;
      ret = fetch_frozen_schema_busy_wait(frozen_version, current_schema_);
      if (OB_SUCCESS == ret)
      {
        if(current_schema_.get_version() > 0 && last_schema_.get_version() > 0)
        {
          if (current_schema_.get_version() < last_schema_.get_version())
          {
            TBSYS_LOG(ERROR,"the new schema old than last schema, current=%ld, last=%ld", 
                current_schema_.get_version(), last_schema_.get_version());
            ret = OB_CS_SCHEMA_INCOMPATIBLE;
          }
          else if (!last_schema_.is_compatible(current_schema_)) 
          {
            TBSYS_LOG(ERROR,"the new schema and old schema is not compatible");
            ret = OB_CS_SCHEMA_INCOMPATIBLE;
          }
        }
      }

      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "cannot luanch next merge round cause schema issue.");
      }
      else if (OB_SUCCESS != (ret = fetch_frozen_time_busy_wait(frozen_version, frozen_timestamp_)))
      {
        TBSYS_LOG(ERROR, "cannot fetch frozen %ld timestamp from updateserver.", frozen_version);
      }
      else if (OB_SUCCESS != (ret = tablet_manager_->prepare_merge_tablets(frozen_version)))
      {
        TBSYS_LOG(ERROR, "not prepared for new merge process, version=%ld", frozen_version);
      }
      else if (OB_SUCCESS != (ret = import_sstable_.init(disk_manager, app_name)))
      {
        TBSYS_LOG(ERROR, "can't initialize import sstable instance, "
                         "app_name=%s, version=%ld, ret=%d",
          app_name, frozen_version, ret);
      }
      else
      {
        TBSYS_LOG(INFO, "new merge process, version=%ld, frozen_timestamp_=%ld", 
            frozen_version, frozen_timestamp_);
      }

      return ret;
    }

    int ObChunkMerge::finish_round(const int64_t frozen_version)
    {
      int ret = OB_SUCCESS;
      pending_in_upgrade_ = true;
      tablet_manager_->sync_all_tablet_images();  //ignore err
      if ( OB_SUCCESS != (ret = tablet_manager_->build_unserving_cache()) )
      {
        TBSYS_LOG(WARN,"switch cache failed");
      }
      else if ( OB_SUCCESS != (ret = tablet_manager_->get_serving_tablet_image().upgrade_service()) )
      {
        TBSYS_LOG(ERROR, "upgrade_service to version = %ld failed", frozen_version);
      }
      else
      {
        TBSYS_LOG(INFO,"this version (%ld) is merge done,to switch cache and upgrade",frozen_version);

        // wait for other worker threads release old cache, 
        // switch to use new cache.
        tablet_manager_->get_join_cache().destroy();
        tablet_manager_->switch_cache(); //switch cache
        tablet_manager_->get_disk_manager().scan(THE_CHUNK_SERVER.get_param().get_datadir_path(),
            THE_CHUNK_SERVER.get_param().get_max_sstable_size());
        // report tablets
        tablet_manager_->report_tablets();
        tablet_manager_->report_capacity_info();

        // clear import sstable instance
        import_sstable_.clear();

        // upgrade complete, no need pending, for migrate in.
        pending_in_upgrade_ = false;

        ::usleep(static_cast<useconds_t>(THE_CHUNK_SERVER.get_param().get_min_drop_cache_wait_time()));
        // now we suppose old cache not used by others,
        // drop it.
        tablet_manager_->drop_unserving_cache(); //ignore err
        // re scan all local disk to recycle sstable 
        // left by RegularRecycler. e.g. migrated sstable.
        tablet_manager_->get_scan_recycler().recycle();

        tablet_manager_->get_serving_tablet_image().drop_compactsstable();
        tablet_manager_->get_join_compactsstable().clear();
        tablet_manager_->get_cache_thread().clear_full_flag();        

        round_end_ = true;
        merge_last_end_time_ = tbsys::CTimeUtil::getTime();
      }
      return ret;
    }

    /** 
     * @brief get a tablet to merge,get lock first
     * 
     * @return 
     */
    int ObChunkMerge::get_tablets(ObTablet* &tablet)
    {
      tablet = NULL;
      int err = OB_SUCCESS;
      int64_t print_step = thread_num_ > 0 ? thread_num_ : 10;
      if (tablets_num_ > 0 && tablet_index_ < tablets_num_)
      {
        TBSYS_LOG(DEBUG,"get tablet from local list,tablet_index_:%d,tablets_num_:%d",tablet_index_,tablets_num_);
        tablet = tablet_array_[tablet_index_++];
        if (NULL == tablet) 
        {
          TBSYS_LOG(WARN,"tablet that get from tablet image is null");
        }
        if (0 == tablet_index_ % print_step)
        {
          int64_t seconds = (tbsys::CTimeUtil::getTime() - merge_start_time_) / 1000L / 1000L;
          TBSYS_LOG(INFO, "merge consume seconds:%lds, minutes:%.2fm, hours:%.2fh, merge process:%s",
            seconds, (double)seconds / 60.0, (double)seconds / 3600.0,
            tablet_manager_->get_serving_tablet_image().print_tablet_image_stat());
        }
      }
      else if ( (tablets_have_got_ == tablets_num_) && 
          (frozen_version_ > tablet_manager_->get_serving_data_version()) )
      {

        while(OB_SUCCESS == err)
        {
          if (round_start_)
          {
            round_start_ = false;
          }

          tablets_num_ = sizeof(tablet_array_) / sizeof(tablet_array_[0]);
          tablet_index_ = 0;
          tablets_have_got_ = 0;

          TBSYS_LOG(INFO,"get tablet from tablet image, frozen_version_=%ld, tablets_num_=%d",
              frozen_version_, tablets_num_);
          err = tablet_manager_->get_serving_tablet_image().get_tablets_for_merge(
              frozen_version_, tablets_num_,tablet_array_);

          if (err != OB_SUCCESS)
          {
            TBSYS_LOG(WARN,"get tablets failed : [%d]",err);
          }
          else if (OB_SUCCESS == err && tablets_num_ > 0)
          {
            TBSYS_LOG(INFO,"get %d tablets from tablet image",tablets_num_);
            tablet = tablet_array_[tablet_index_++];
            break; //got it
          }
          else if (!round_end_)
          {
            if (OB_SUCCESS == (err = finish_round(frozen_version_)))
            {
              break;
            }
          }
          else
          {
            //impossible
            TBSYS_LOG(WARN,"can't get tablets and is not round end");
            break;
          }
        }
      }
      else
      {
        TBSYS_LOG(DEBUG,"tablets_num_:%d,tablets_have_got_:%d,"
                       "frozen_version_:%ld,serving data version:%ld",
                       tablets_num_,tablets_have_got_,
                       frozen_version_,tablet_manager_->get_serving_data_version());
      }

      return err;
    }

    bool ObChunkMerge::have_new_version_in_othercs(const ObTablet* tablet)
    {
      bool ret = false;
      
      if (tablet != NULL)
      {
        ObChunkServer&  chunk_server = ObChunkServerMain::get_instance()->get_chunk_server();
        ObTabletLocation list[OB_SAFE_COPY_COUNT];
        int32_t size = OB_SAFE_COPY_COUNT;
        int32_t new_copy = 0;
        int err = chunk_server.get_rs_rpc_stub().get_tablet_info(current_schema_, tablet->get_range().table_id_,
                                                                 tablet->get_range(),list,size);
        if (OB_SUCCESS == err)
        {
          for(int32_t i=0; i<size; ++i)
          {
            TBSYS_LOG(INFO,"version:%ld",list[i].tablet_version_); //for test
            if (list[i].tablet_version_ > tablet->get_data_version())
              ++new_copy;
          }
        }
        if (OB_SAFE_COPY_COUNT - 1 == new_copy)
          ret = true;
      }
      return ret;
    }

    int ObChunkMerge::delete_tablet_on_rootserver(const ObTablet* tablet)
    {
      int ret = OB_SUCCESS;
      ObTabletReportInfoList *discard_tablet_list =  GET_TSI_MULT(ObTabletReportInfoList, TSI_CS_TABLET_REPORT_INFO_LIST_1);

      if (NULL == tablet || NULL == discard_tablet_list)
      {
      }
      else
      {
        discard_tablet_list->reset();
        ObTabletReportInfo tablet_info;
        if (OB_SUCCESS != (ret = tablet_manager_->fill_tablet_info(*tablet,  tablet_info)))
        {
          TBSYS_LOG(WARN, "fill_tablet_info error.");
        }
        else if (OB_SUCCESS != (ret = discard_tablet_list->add_tablet(tablet_info)))
        {
          TBSYS_LOG(WARN, "add tablet(version=%ld) info to list error. ", tablet->get_data_version());
        }
        else if (OB_SUCCESS != (ret = THE_CHUNK_SERVER.get_rs_rpc_stub().delete_tablets(*discard_tablet_list)))
        {
          TBSYS_LOG(WARN, "delete tablets rpc error, ret= %d", ret);
        }
      }
      return ret;
    }

    bool ObChunkMerge::check_load()
    {
      bool ret = false;
      double loadavg[3];
      
      ObChunkServer&  chunk_server = ObChunkServerMain::get_instance()->get_chunk_server();
      volatile int64_t current_request_count_ = 0;

      if (getloadavg(loadavg,sizeof(loadavg)/sizeof(loadavg[0])) < 0)
      {
        TBSYS_LOG(WARN,"getloadavg failed");
        loadavg[0] = 0;
      }

      ObStat *stat = NULL;

      chunk_server.get_stat_manager().get_stat(ObChunkServerStatManager::META_TABLE_ID,stat);
      if (NULL == stat)
      {
        //TBSYS_LOG(WARN,"get stat failed");
        current_request_count_  = 0;
      }
      else
      {
        current_request_count_ = stat->get_value(ObChunkServerStatManager::INDEX_META_REQUEST_COUNT_PER_SECOND);
      }
      

      pthread_mutex_lock(&mutex_);
      if (active_thread_num_ <= min_merge_thread_num_)
      {
        TBSYS_LOG(INFO, "current active thread :%ld < min merge thread: %ld, continue run.",
            active_thread_num_, min_merge_thread_num_);
        ret = true;
      }
      // check load and request if match the merge conditions.
      if ((loadavg[0]  < merge_load_high_)
          && (current_request_count_ < request_count_high_))
      {
        ret = true;
        int64_t sleep_thread = thread_num_ - active_thread_num_;
        int64_t remain_load = merge_load_high_ - static_cast<int64_t>(loadavg[0]) - 1; //loadavg[0] double to int
        int64_t remain_tablet = tablets_num_ - tablets_have_got_;
        if ((loadavg[0] < merge_load_adjust_) &&
            (remain_tablet > active_thread_num_) &&
            (sleep_thread > 0) && (remain_load > 0) )
        {
          TBSYS_LOG(INFO,"wake up %ld thread(s)",sleep_thread > remain_load ? remain_load : sleep_thread);
          while(sleep_thread-- > 0 && remain_load-- > 0)
          {
            pthread_cond_signal(&cond_);
          }
        }
      }
      else
      {
        TBSYS_LOG(INFO,"loadavg[0] : %f,merge_load_high_:%ld,current_request_count_:%ld,request_count_high_:%ld",
            loadavg[0],merge_load_high_,current_request_count_,request_count_high_);
      }
      pthread_mutex_unlock(&mutex_);
      return ret;
    }

    /*-----------------------------------------------------------------------------
     *  ObTabletMerger
     *-----------------------------------------------------------------------------*/

    ObTabletMerger::ObTabletMerger(ObChunkMerge& chunk_merge,ObTabletManager& manager) :
      chunk_merge_(chunk_merge),
      manager_(manager),
      row_key_swap_(NULL),
      cell_(NULL),
      old_tablet_(NULL),
      new_table_schema_(NULL),
      max_sstable_size_(0),
      frozen_version_(0),
      current_sstable_size_(0),
      row_num_(0),
      pre_column_group_row_num_(0),
      cs_proxy_(manager),
      ms_wrapper_(*(ObChunkServerMain::get_instance()->get_chunk_server().get_rpc_proxy()),
          ObChunkServerMain::get_instance()->get_chunk_server().get_param().get_merge_timeout()),
      merge_join_agent_(cs_proxy_)
    {}

    int ObTabletMerger::init(ObChunkMerge::RowKeySwap *swap)
    {
      int ret = OB_SUCCESS;
      ObChunkServer&  chunk_server = ObChunkServerMain::get_instance()->get_chunk_server();

      if (NULL == swap)
      {
        TBSYS_LOG(ERROR,"interal error,swap is null");
        ret = OB_ERROR;
      }

      if (OB_SUCCESS == ret)
      {
        row_key_swap_ = swap;
        max_sstable_size_ = chunk_server.get_param().get_max_sstable_size();
      }

      if (OB_SUCCESS == ret && chunk_server.get_param().get_join_cache_conf().cache_mem_size > 0)
      {
        ms_wrapper_.get_ups_get_cell_stream()->set_cache(manager_.get_join_cache());
      }

      if (OB_SUCCESS == ret)
      {
        int64_t dio_type = chunk_server.get_param().get_write_sstable_io_type();
        bool dio = false;
        if (dio_type > 0)
        {
          dio = true;
        }
        writer_.set_dio(dio);
      }

      return ret;
    }

    int ObTabletMerger::check_row_count_in_column_group()
    {
      int ret = OB_SUCCESS;
      if (pre_column_group_row_num_ != 0)
      {
        if (row_num_ != pre_column_group_row_num_)
        {
          TBSYS_LOG(ERROR,"the row num between two column groups is difference,[%ld,%ld]",
              row_num_,pre_column_group_row_num_);
          ret = OB_ERROR;
        }
      }
      else
      {
        pre_column_group_row_num_ = row_num_;
      }
      return ret;
    }

    void ObTabletMerger::reset_for_next_column_group()
    {
      row_.clear();
      reset_local_proxy();
      merge_join_agent_.clear(); 
      scan_param_.reset();
      row_num_ = 0;
    }

    int ObTabletMerger::save_current_row(const bool current_row_expired)
    {
      int ret = OB_SUCCESS;
      if (current_row_expired)
      {
        //TBSYS_LOG(DEBUG, "current row expired.");
        //hex_dump(row_.get_row_key().ptr(), row_.get_row_key().length(), false, TBSYS_LOG_LEVEL_DEBUG);
      }
      else if ((ret = writer_.append_row(row_,current_sstable_size_)) != OB_SUCCESS )
      {
        TBSYS_LOG(ERROR, "Merge : append row failed [%d], this row_, obj count:%ld, "
                         "table:%lu, group:%lu, rowkey:%s",
            ret, row_.get_obj_count(), row_.get_table_id(), row_.get_column_group_id(), 
            print_string(row_.get_row_key()));
        for(int32_t i=0; i<row_.get_obj_count(); ++i)
        {
          row_.get_obj(i)->dump(TBSYS_LOG_LEVEL_ERROR);
        }
      }
      return ret;
    }
    
    int ObTabletMerger::wait_aio_buffer() const
    {
      int ret = OB_SUCCESS;
      int status = 0;

      ObThreadAIOBufferMgrArray* aio_buf_mgr_array = GET_TSI_MULT(ObThreadAIOBufferMgrArray, TSI_SSTABLE_THREAD_AIO_BUFFER_MGR_ARRAY_1);
      if (NULL == aio_buf_mgr_array)
      {
        ret = OB_ERROR;
      }
      else if (OB_AIO_TIMEOUT == (status = 
            aio_buf_mgr_array->wait_all_aio_buf_mgr_free(10 * 1000000)))
      {    
        TBSYS_LOG(WARN, "failed to wait all aio buffer manager free, stop current thread");
        ret = OB_ERROR;
      }    

      return ret;

    }

    int ObTabletMerger::reset_local_proxy() const
    {
      int ret = OB_SUCCESS;

      /**
       * if this function fails, it means that some critical problems 
       * happen, don't kill chunk server here, just output some error
       * info, then we can restart this chunk server manualy. 
       */
      ret = manager_.end_scan();
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "failed to end scan to release resources");
      }

      ret = reset_query_thread_local_buffer();
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "failed to reset query thread local buffer");
      }

      return ret;
    }

    int ObTabletMerger::merge_column_group(
        const int64_t column_group_idx,
        const uint64_t column_group_id, 
        int64_t& split_row_pos,
        const int64_t max_sstable_size,
        const bool is_need_join,
        bool& is_tablet_splited,
        bool& is_tablet_unchanged)
    {
      int ret = OB_SUCCESS;
      RowStatus row_status = ROW_START;
      ObOperatorMemLimit mem_limit;

      bool is_row_changed = false;
      bool current_row_expired = false;
      bool need_filter = tablet_merge_filter_.need_filter();
      /**
       * there are 4 cases that we cann't do "unmerge_if_unchanged" 
       * optimization 
       * 1. the config of chunkserver doesn't enable this function 
       * 2. need expire some data in this tablet 
       * 3. the table need join another tables 
       * 4. the sub range of this tablet is splited
       */
      bool unmerge_if_unchanged =
        (THE_CHUNK_SERVER.get_param().get_unmerge_if_unchanged() > 0 
         && !need_filter && !is_need_join && !is_tablet_splited);
      int64_t expire_row_num = 0;
      mem_limit.merge_mem_size_ = THE_CHUNK_SERVER.get_param().get_merge_mem_limit();
      // in order to reuse the internal cell array of merge join agent
      mem_limit.max_merge_mem_size_ = mem_limit.merge_mem_size_ + 1024 * 1024;

      is_tablet_splited = false;
      is_tablet_unchanged = false;

      if (OB_SUCCESS != (ret = wait_aio_buffer()))
      {
        TBSYS_LOG(ERROR, "wait aio buffer error, column_group_id = %ld", column_group_id);
      }
      else if (OB_SUCCESS != (ret = fill_scan_param( column_group_id )))
      {
        TBSYS_LOG(ERROR,"prepare scan param failed : [%d]",ret);
      }
      else if (OB_SUCCESS != (ret = tablet_merge_filter_.adjust_scan_param(
        column_group_idx, column_group_id, scan_param_)))
      {
        TBSYS_LOG(ERROR, "tablet merge filter adjust scan param failed : [%d]", ret);
      }
      else if (OB_SUCCESS != (ret = merge_join_agent_.start_agent(scan_param_,
              *ms_wrapper_.get_ups_scan_cell_stream(),
              *ms_wrapper_.get_ups_get_cell_stream(),
              chunk_merge_.current_schema_,
              mem_limit,0, unmerge_if_unchanged)))
      {
        TBSYS_LOG(ERROR,"set request param for merge_join_agent failed [%d]",ret);
        merge_join_agent_.clear();
        scan_param_.reset();
      }
      else if (merge_join_agent_.is_unchanged())
      {
        is_tablet_unchanged = true;
      }

      if (OB_SUCCESS == ret && 0 == column_group_idx && !is_tablet_unchanged
          && (ret = create_new_sstable()) != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR,"create sstable failed.");
      }

      while(OB_SUCCESS == ret && !is_tablet_unchanged)
      {
        cell_ = NULL; //in case
        if ( manager_.is_stoped() )
        {
          TBSYS_LOG(WARN,"stop in merging column_group_id=%ld", column_group_id);
          ret = OB_CS_MERGE_CANCELED;
        } 
        else if ((ret = merge_join_agent_.next_cell()) != OB_SUCCESS ||
            (ret = merge_join_agent_.get_cell(&cell_, &is_row_changed)) != OB_SUCCESS)
        {
          //end or error
          if (OB_ITER_END == ret)
          {
            TBSYS_LOG(DEBUG,"Merge : end of file");
            ret = OB_SUCCESS;
            if (need_filter && ROW_GROWING == row_status)
            {
              ret = tablet_merge_filter_.check_and_trim_row(column_group_idx, row_num_, 
                row_, current_row_expired);
              if (OB_SUCCESS != ret)
              {
                TBSYS_LOG(ERROR, "tablet merge filter do check_and_trim_row failed, "
                                 "row_num_=%ld", row_num_);
              }
              else if (OB_SUCCESS == ret && current_row_expired)
              {
                ++expire_row_num;
              }
            }
            if (OB_SUCCESS == ret && ROW_GROWING == row_status 
                && OB_SUCCESS == (ret = save_current_row(current_row_expired)))
            {
              ++row_num_;
              row_.clear();
              current_row_expired = false;
              row_status = ROW_END;
            }
          }
          else
          {
            TBSYS_LOG(ERROR,"Merge : get data error,ret : [%d]", ret);
          }

          // end of file
          break;
        }
        else if ( cell_ != NULL )
        {
          /**
           * there are some data in scanner
           */
          if ((ROW_GROWING == row_status && is_row_changed))
          {
            if (0 == (row_num_ % chunk_merge_.merge_pause_row_count_) )
            {
              if (chunk_merge_.merge_pause_sleep_time_ > 0)
              {
                if (0 == (row_num_ % (chunk_merge_.merge_pause_row_count_ * 20)))
                {
                  // print log every sleep 20 times;
                  TBSYS_LOG(INFO,"pause in merging,sleep %ld", 
                      chunk_merge_.merge_pause_sleep_time_);
                }
                usleep(static_cast<useconds_t>(chunk_merge_.merge_pause_sleep_time_));
              }

              /* donot sleep while merge in progress.
              while( !chunk_merge_.check_load() )
              {
                TBSYS_LOG(INFO,"load is too high in merging,sleep %ld", 
                    chunk_merge_.merge_highload_sleep_time_);
                usleep(static_cast<useconds_t>(chunk_merge_.merge_highload_sleep_time_));
              }
              */
            }

            if (need_filter)
            {
              ret = tablet_merge_filter_.check_and_trim_row(column_group_idx, row_num_, 
                row_, current_row_expired);
              if (OB_SUCCESS != ret)
              {
                TBSYS_LOG(ERROR, "tablet merge filter do check_and_trim_row failed, "
                                 "row_num_=%ld", row_num_);
              }
              else if (OB_SUCCESS == ret && current_row_expired)
              {
                ++expire_row_num;
              }
            }

            // we got new row, write last row we build.
            if (OB_SUCCESS == ret 
                && OB_SUCCESS == (ret = save_current_row(current_row_expired)) )
            {
              ++row_num_;
              current_row_expired = false;
              // start new row.
              row_status = ROW_START;
            }

            // this split will use current %row_, so we clear row below.
            if (OB_SUCCESS == ret 
                && ((split_row_pos > 0 && row_num_ > split_row_pos)
                    || (0 == column_group_idx && 0 == split_row_pos 
                        && current_sstable_size_ > max_sstable_size)) 
                && maybe_change_sstable())
            {
              /**
               * if the merging tablet is no data, we can't set a proper 
               * split_row_pos, so we try to merge the first column group, and 
               * the first column group split by sstable size, if the first 
               * column group size is larger than max_sstable_size, we set the 
               * current row count as the split_row_pos, the next clolumn 
               * groups of the first splited tablet use this split_row_pos, 
               * after merge the first splited tablet, we use the first 
               * splited tablet as sampler, re-caculate the average row size 
               * of tablet and split_row_pos, the next splited tablet of the 
               * tablet will use the new split_row_pos. 
               */
              if (0 == column_group_idx && 0 == split_row_pos 
                  && current_sstable_size_ > max_sstable_size)
              {
                split_row_pos = row_num_;
                TBSYS_LOG(INFO, "column group %ld splited by sstable size, "
                                "current_sstable_size_=%ld, max_sstable_size=%ld, "
                                "split_row_pos=%ld",
                  column_group_idx, current_sstable_size_, 
                  max_sstable_size, split_row_pos);
              }
              /**
               * record the end key
               */
              if ( (ret = update_range_end_key()) != OB_SUCCESS)
              {
                TBSYS_LOG(ERROR,"update end range error [%d]",ret);
              }
              else
              {
                is_tablet_splited = true;
              }
              // entercount split point, skip to merge next column group
              // we reset all status whenever start new column group, so
              // just break it, leave it to reset_for_next_column_group();
              break;
            }

            // before we build new row, must clear last row in use.
            row_.clear();
          }

          if ( OB_SUCCESS == ret && ROW_START == row_status)
          { 
            // first cell in current row, set rowkey and table info.
            row_.set_row_key(cell_->row_key_);
            row_.set_table_id(cell_->table_id_);
            row_.set_column_group_id(column_group_id);
            row_status = ROW_GROWING;
          }

          if (OB_SUCCESS == ret && ROW_GROWING == row_status)
          {
            // no need to add current cell to %row_ when current row expired.
            if ((ret = row_.add_obj(cell_->value_)) != OB_SUCCESS)
            {
              TBSYS_LOG(ERROR,"Merge : add_cell_to_row failed [%d]",ret);
            }
          }
        }
        else
        {
          TBSYS_LOG(ERROR,"get cell return success but cell is null");
          ret = OB_ERROR;
        }
      }

      char range_buf[OB_RANGE_STR_BUFSIZ];
      new_range_.to_string(range_buf,sizeof(range_buf));
      TBSYS_LOG(INFO, "column group %ld merge completed, ret = %d, version_range=%s, "
          "total row count =%ld, expire row count = %ld, split_row_pos=%ld, "
          "max_sstable_size=%ld, is_tablet_splited=%d, is_tablet_unchanged=%d, range=%s", 
          column_group_id, ret, range2str(scan_param_.get_version_range()),
          row_num_, expire_row_num, split_row_pos, 
          max_sstable_size, is_tablet_splited, is_tablet_unchanged, range_buf);

      return ret;
    }

    int ObTabletMerger::merge_or_import(ObTablet *tablet,int64_t frozen_version)
    {
      int ret = OB_SUCCESS;

      if (NULL == tablet || frozen_version <= 0)
      {
        TBSYS_LOG(ERROR,"merge : interal error ,param invalid frozen_version:[%ld]",frozen_version);
        ret = OB_ERROR;
      }
      else if (chunk_merge_.get_import_sstable().need_import_sstable(tablet->get_range().table_id_))
      {
        ret = import(tablet, frozen_version);
      }
      else 
      {
        ret = merge(tablet, frozen_version);
      }

      return ret;
    }

    bool ObTabletMerger::is_table_need_join(const uint64_t table_id)
    {
      bool ret = false;
      const ObColumnSchemaV2* column = NULL;
      int32_t column_size = 0;

      column = chunk_merge_.current_schema_.get_table_schema(table_id, column_size);
      if (NULL != column && column_size > 0)
      {
        for (int32_t i = 0; i < column_size; ++i)
        {
          if (NULL != column[i].get_join_info())
          {
            ret = true;
            break;
          }
        }
      }

      return ret;
    }

    int ObTabletMerger::merge(ObTablet *tablet,int64_t frozen_version)
    {
      int   ret = OB_SUCCESS;
      int64_t split_row_pos = 0;
      int64_t init_split_row_pos = 0; 
      bool    is_tablet_splited = false;
      bool    is_first_column_group_splited = false;
      bool    is_tablet_unchanged = false;
      int64_t max_sstable_size = max_sstable_size_;
      ObTablet* new_tablet = NULL;
    
      FILL_TRACE_LOG("start merge tablet");
      if (NULL == tablet || frozen_version <= 0)
      {
        TBSYS_LOG(ERROR,"merge : interal error ,param invalid frozen_version:[%ld]",frozen_version);
        ret = OB_ERROR;
      }
      else if ( OB_SUCCESS != (ret = reset_local_proxy()) )      
      {
        TBSYS_LOG(ERROR, "reset query thread local buffer error.");
      }
      else if ( NULL == (new_table_schema_ = 
            chunk_merge_.current_schema_.get_table_schema(tablet->get_range().table_id_)) )
      {
        //This table has been deleted
        TBSYS_LOG(INFO,"table (%lu) has been deleted",tablet->get_range().table_id_);
        tablet->set_merged();
        ret = OB_CS_TABLE_HAS_DELETED;
      }
      else if (OB_SUCCESS != (ret = fill_sstable_schema(*new_table_schema_,sstable_schema_)))
      {
        TBSYS_LOG(ERROR, "convert table schema to sstable schema failed, table=%ld", 
            tablet->get_range().table_id_);
      }

      if (OB_SUCCESS == ret)
      {
        // parse compress name from schema, may be NULL;
        const char *compressor_name = new_table_schema_->get_compress_func_name();
        if (NULL == compressor_name || '\0' == *compressor_name )
        {
          TBSYS_LOG(WARN,"no compressor with this sstable."); 
        }
        compressor_string_.assign_ptr(const_cast<char *>(compressor_name),static_cast<int32_t>(strlen(compressor_name)));

        // if schema define max sstable size for table, use it
        if (new_table_schema_->get_max_sstable_size() > 0)
        {
          max_sstable_size = new_table_schema_->get_max_sstable_size();
        }
        // according to sstable size, compute the average row count of each sstable
        split_row_pos = tablet->get_row_count();
        int64_t over_size_percent = THE_CHUNK_SERVER.get_param().get_over_size_percent_to_split();
        int64_t split_size = (over_size_percent > 0) ? (max_sstable_size * (100 + over_size_percent) / 100) : 0;
        int64_t occupy_size = tablet->get_occupy_size();
        int64_t row_count = tablet->get_row_count();
        if (occupy_size > split_size && row_count > 0) 
        {
          int64_t avg_row_size = occupy_size / row_count;
          if (avg_row_size > 0)
          {
            split_row_pos = max_sstable_size / avg_row_size;
          }
        }
        /**
         * if user specify over_size_percent, and the tablet size is 
         * greater than 0 and not greater than split_size threshold, we 
         * don't split this tablet. if tablet size is 0, use the default
         * split algorithm 
         */
        if (over_size_percent > 0 && occupy_size > 0 && occupy_size <= split_size)
        {
          // set split_row_pos = -1, this tablet will not split
          split_row_pos = -1;
        }
        init_split_row_pos = split_row_pos;
      }


      if (OB_SUCCESS == ret)
      {
        new_range_ = tablet->get_range();
        old_tablet_ = tablet;
        frozen_version_ = frozen_version; 
        int64_t sstable_id = 0;
        char range_buf[OB_RANGE_STR_BUFSIZ];
        ObSSTableId sst_id;
        char path[OB_MAX_FILE_NAME_LENGTH];
        path[0] = '\0';
       
        if ((tablet->get_sstable_id_list()).get_array_index() > 0)
          sstable_id = (tablet->get_sstable_id_list()).at(0)->sstable_file_id_;
        
        tablet->get_range().to_string(range_buf,sizeof(range_buf));
        if (0 != sstable_id)
        {
          sst_id.sstable_file_id_ = sstable_id;
          get_sstable_path(sst_id, path, sizeof(path));
        }
        TBSYS_LOG(INFO, "start merge sstable_id:%ld, old_version=%ld, "
            "new_version=%ld, split_row_pos=%ld, table_row_count=%ld, "
            "tablet_occupy_size=%ld, compressor=%.*s, path=%s, range=%s",
            sstable_id, tablet->get_data_version(), frozen_version,
            split_row_pos, tablet->get_row_count(), 
            tablet->get_occupy_size(), compressor_string_.length(), 
            compressor_string_.ptr(), path, range_buf);

        uint64_t column_group_ids[OB_MAX_COLUMN_GROUP_NUMBER];
        int32_t column_group_num = sizeof(column_group_ids) / sizeof(column_group_ids[0]);

        if ( OB_SUCCESS != (ret = chunk_merge_.current_schema_.get_column_groups(
                new_table_schema_->get_table_id(), column_group_ids,column_group_num)) )
        {
          TBSYS_LOG(ERROR,"get column groups failed : [%d]",ret);
        }
        bool is_need_join = is_table_need_join(new_table_schema_->get_table_id());

        if (OB_SUCCESS == ret &&
            OB_SUCCESS != (ret = tablet_merge_filter_.init(chunk_merge_.current_schema_, 
              tablet, frozen_version, chunk_merge_.frozen_timestamp_)))
        {
          TBSYS_LOG(ERROR, "failed to initialize tablet merge filter, table=%ld", 
              tablet->get_range().table_id_);
        }

        FILL_TRACE_LOG("after prepare merge tablet, column_group_num=%d, ret=%d", 
          column_group_num, ret);
        while(OB_SUCCESS == ret)
        {
          if ( manager_.is_stoped() )
          {
            TBSYS_LOG(WARN,"stop in merging");
            ret = OB_CS_MERGE_CANCELED;
          }

          // clear all bits while start merge new tablet.
          // generally in table split.
          tablet_merge_filter_.clear_expire_bitmap();
          pre_column_group_row_num_ = 0;
          is_first_column_group_splited  = false;

          for(int32_t group_index = 0; (group_index < column_group_num) && (OB_SUCCESS == ret); ++group_index)
          {
            if ( manager_.is_stoped() )
            {
              TBSYS_LOG(WARN,"stop in merging");
              ret = OB_CS_MERGE_CANCELED;
            }
            // %expire_row_filter will be set in first column group,
            // and test in next merge column group.
            else if ( OB_SUCCESS != ( ret = merge_column_group(
                  group_index, column_group_ids[group_index],  
                  split_row_pos, max_sstable_size, is_need_join, 
                  is_tablet_splited, is_tablet_unchanged)) )
            {
              TBSYS_LOG(ERROR, "merge column group[%d] = %lu , group num = %d,"
                  "split_row_pos = %ld error.", 
                  group_index, column_group_ids[group_index], 
                  column_group_num, split_row_pos);
            }
            else if ( OB_SUCCESS != (ret = check_row_count_in_column_group()) )
            {
              TBSYS_LOG(ERROR, "check row count column group[%d] = %lu", 
                  group_index, column_group_ids[group_index]);
            }

            // only when merging the first column group, the function merge_column_group()
            // set the is_tablet_splited flag
            if (OB_SUCCESS == ret && 0 == group_index)
            {
              is_first_column_group_splited = is_tablet_splited;
            }

            FILL_TRACE_LOG("finish merge column group %d, is_splited=%d, "
                           "row_num=%ld, ret=%d", 
              group_index, is_tablet_splited, row_num_, ret);

            // reset for next column group..
            // page arena reuse avoid memory explosion
            reset_for_next_column_group();

            // if the first column group is unchaged, the tablet is unchanged,
            // break the merge loop
            if (OB_SUCCESS == ret && 0 == group_index && is_tablet_unchanged)
            {
              break;
            }
          }

          // all column group has been written,finish this sstable

          if (OB_SUCCESS == ret 
              && (ret = finish_sstable(is_tablet_unchanged, new_tablet))!= OB_SUCCESS)
          {
            TBSYS_LOG(ERROR,"close sstable failed [%d]",ret);
          }

          // not split, break
          if (OB_SUCCESS == ret && !is_first_column_group_splited)
          {
            break;
          }
          
          FILL_TRACE_LOG("start merge new splited tablet");

          /**
           * if the merging tablet is no data, we can't set a proper 
           * split_row_pos, so we try to merge the first column group, and 
           * the first column group split by sstable size, if the first 
           * column group size is larger than max_sstable_size, we set the 
           * current row count as the split_row_pos, the next clolumn 
           * groups of the first splited tablet use this split_row_pos, 
           * after merge the first splited tablet, we use the first 
           * splited tablet as sampler, re-caculate the average row size 
           * of tablet and split_row_pos, the next splited tablet of the 
           * tablet will use the new split_row_pos. 
           */
          if (OB_SUCCESS == ret && 0 == init_split_row_pos 
              && split_row_pos > 0 && NULL != new_tablet 
              && new_tablet->get_occupy_size() > 0
              && new_tablet->get_row_count() > 0)
          {
            int64_t avg_row_size = new_tablet->get_occupy_size() / new_tablet->get_row_count();
            if (avg_row_size > 0)
            {
              split_row_pos = max_sstable_size / avg_row_size;
            }
            init_split_row_pos = split_row_pos;
          }

          if (OB_SUCCESS == ret)
          {
            update_range_start_key();

            //prepare the next request param
            new_range_.end_key_ = tablet->get_range().end_key_;
            if (tablet->get_range().border_flag_.is_max_value())
            {
              TBSYS_LOG(INFO,"this tablet has max flag,reset it");
              new_range_.border_flag_.set_max_value();
              new_range_.border_flag_.unset_inclusive_end();
            }
          }
        } // while (OB_SUCCESS == ret) //finish one tablet
        
        if (OB_SUCCESS == ret)
        {
          ret = update_meta();
        }
        else
        {
          TBSYS_LOG(ERROR,"merge failed,don't add these tablet");
          
          int64_t t_off = 0;
          int64_t s_size = 0;
          writer_.close_sstable(t_off,s_size);
          if (sstable_id_.sstable_file_id_ != 0)
          {
            if (strlen(path_) > 0)
            {
              unlink(path_); //delete 
              TBSYS_LOG(WARN,"delete %s",path_);
            }
            manager_.get_disk_manager().add_used_space((sstable_id_.sstable_file_id_ & DISK_NO_MASK),0);
          }

          int64_t sstable_id = 0;

          for(ObVector<ObTablet *>::iterator it = tablet_array_.begin(); it != tablet_array_.end(); ++it)
          {
            if ( ((*it) != NULL) && ((*it)->get_sstable_id_list().get_array_index() > 0))
            {
              sstable_id = (*it)->get_sstable_id_list().at(0)->sstable_file_id_;
              if (OB_SUCCESS == get_sstable_path(sstable_id,path_,sizeof(path_)))
              {
                unlink(path_);
              }
            }
          }
          tablet_array_.clear();
          path_[0] = '\0';
          current_sstable_size_ = 0;
        }
      }
      FILL_TRACE_LOG("finish merge tablet, ret=%d", ret);
      PRINT_TRACE_LOG();
      CLEAR_TRACE_LOG();

      return ret;
    }

    int ObTabletMerger::import(ObTablet *tablet, int64_t frozen_version)
    {
      int ret = OB_SUCCESS;
      int64_t sstable_id = 0;
      int32_t disk_no = 0;
      int64_t sstable_size = 0;
      ObTablet *new_tablet = NULL;
      int64_t fail_sleep_interval = 5000000; //5s
      char range_buf[OB_RANGE_STR_BUFSIZ];
      char import_sstable_path[OB_MAX_FILE_NAME_LENGTH];
      ObFilePathInfo sstable_info;

      if (NULL == tablet || frozen_version <= 0)
      {
        TBSYS_LOG(ERROR,"merge : interal error, param invalid frozen_version:[%ld]",
          frozen_version);
        ret = OB_ERROR;
      }
      else if ( OB_SUCCESS != (ret = reset_query_thread_local_buffer()) )
      {
        TBSYS_LOG(ERROR, "reset query thread local buffer error.");
      }
      else
      {
        TBSYS_LOG(INFO, "import sstable, table=%ld, frozen_version=%ld", 
          tablet->get_range().table_id_, frozen_version);
      }

      if (OB_SUCCESS == ret)
      {
        new_range_ = tablet->get_range();
        old_tablet_ = tablet;
        frozen_version_ = frozen_version; 
        
        if ((tablet->get_sstable_id_list()).get_array_index() > 0)
        {
          sstable_id = (tablet->get_sstable_id_list()).at(0)->sstable_file_id_;
        }
        tablet->get_range().to_string(range_buf,sizeof(range_buf));
        TBSYS_LOG(INFO," start import src_sstable_id:%ld, range:%s", 
          sstable_id , range_buf);

        sstable_id_.sstable_file_id_     = manager_.allocate_sstable_file_seq();
        sstable_id_.sstable_file_offset_ = 0;

        ret = chunk_merge_.get_import_sstable().get_import_sstable_info(
          tablet->get_range(), sstable_info);
        if (OB_SUCCESS != ret || sstable_info.disk_no_ <= 0)
        {
          TBSYS_LOG(ERROR, "don't find the import sstable for range=%s", range_buf);
          if (sstable_info.disk_no_ <= 0)
          {
            ret = OB_ERROR;
          }
          usleep(static_cast<useconds_t>(fail_sleep_interval)); //5s
        }
        else 
        {
          disk_no = static_cast<int32_t>(sstable_info.disk_no_);
        }
      }
        
      if (OB_SUCCESS == ret)
      {
        sstable_id_.sstable_file_id_ = (sstable_id_.sstable_file_id_ << 8) | (disk_no & 0xff);
        if ((OB_SUCCESS == ret) && (ret = get_sstable_path(sstable_id_,
          path_,sizeof(path_))) != OB_SUCCESS )
        {
          TBSYS_LOG(ERROR, "import : can't get the path of new sstable");
          ret = OB_ERROR;
        }
      }

      if (OB_SUCCESS == ret)
      {
        ret = get_import_sstable_path(disk_no, sstable_info.file_name_, 
          import_sstable_path, OB_MAX_FILE_NAME_LENGTH);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "import: can't get import sstable path, disk_no=%d, "
                           "sstable_name=%s", disk_no, sstable_info.file_name_);
        }
        else 
        {
          sstable_size = get_file_size(import_sstable_path);
          if (sstable_size < 0)
          {
            TBSYS_LOG(ERROR, "get file size error, sstable_size=%ld, import_sstable_path=%s, err=%s", 
                sstable_size, import_sstable_path, strerror(errno));
            ret = OB_IO_ERROR;
          }
          else if (0 == sstable_size)
          {
            TBSYS_LOG(INFO, "empty import sstable, just delete it, import sstable size=%ld", 
                sstable_size);
            if (::remove(import_sstable_path) != 0)
            {
              TBSYS_LOG(WARN, "remove import sstable file=%s failed", import_sstable_path);
            }
          }
        }
      }

      if (OB_SUCCESS == ret)
      {
        ObMultiVersionTabletImage& tablet_image = manager_.get_serving_tablet_image();
        if ((ret = tablet_image.alloc_tablet_object(new_range_, 
          frozen_version_, new_tablet)) != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "alloc_tablet_object failed.");
        }
        else
        {
          //set new data version
          new_tablet->set_data_version(frozen_version_);
          new_tablet->set_disk_no(disk_no);
          if (sstable_size > 0 && (ret = new_tablet->add_sstable_by_id(sstable_id_)) != OB_SUCCESS)
          {
            TBSYS_LOG(ERROR, "import : add sstable to tablet failed.");
          }
        }
       
        if (OB_SUCCESS == ret)
        {
          ret = tablet_array_.push_back(new_tablet);
        }
      }

      // rename import sstable name
      if (OB_SUCCESS == ret)
      {
        if (sstable_size > 0 && !FileDirectoryUtils::rename(import_sstable_path, path_))
        {
          TBSYS_LOG(ERROR, "rename import sstable=%s to dst sstable=%s error.", 
            import_sstable_path, path_);
          ret = OB_IO_ERROR;
        }
        else 
        {
          manager_.get_disk_manager().add_used_space(disk_no, sstable_size);
          TBSYS_LOG(INFO, "rename import sstble=%s to dst sstable=%s",
            import_sstable_path, path_);
        }
      }
      
      if (OB_SUCCESS == ret)
      {
        ret = update_meta();
      }
      else
      {
        tablet_array_.clear();
      }

      return ret;
    }

    int ObTabletMerger::fill_sstable_schema(const ObTableSchema& common_schema,ObSSTableSchema& sstable_schema)
    {
      int ret = OB_SUCCESS;
      int32_t cols = 0;
      int32_t size = 0;
      ObSSTableSchemaColumnDef column_def;

      sstable_schema.reset();
      
      const ObColumnSchemaV2 *col = chunk_merge_.current_schema_.get_table_schema( common_schema.get_table_id(),size);

      if (NULL == col || size <= 0)
      {
        TBSYS_LOG(ERROR,"cann't find this table:%lu",common_schema.get_table_id());
        ret = OB_ERROR;
      }
      else
      {
        for(int col_index = 0; col_index < size && OB_SUCCESS == ret; ++col_index)
        {
          memset(&column_def,0,sizeof(column_def));
          column_def.table_id_ = static_cast<uint32_t>(common_schema.get_table_id());
          column_def.column_group_id_ = static_cast<uint16_t>((col + col_index)->get_column_group_id());
          column_def.column_name_id_ = static_cast<uint16_t>((col + col_index)->get_id());
          column_def.column_value_type_ = (col + col_index)->get_type();
          if ( (ret = sstable_schema.add_column_def(column_def)) != OB_SUCCESS )
          {
            TBSYS_LOG(ERROR,"add column_def(%u,%u,%u) failed col_index : %d",column_def.table_id_,
                column_def.column_group_id_,column_def.column_name_id_,col_index);
          }
          ++cols;
        }
      }
     
      if ( 0 == cols && OB_SUCCESS == ret ) //this table has moved to updateserver
      {
        ret = OB_CS_TABLE_HAS_DELETED;
      }
      return ret;
    }
   
    int ObTabletMerger::create_new_sstable()
    {
      int ret                          = OB_SUCCESS;
      sstable_id_.sstable_file_id_     = manager_.allocate_sstable_file_seq();
      sstable_id_.sstable_file_offset_ = 0;
      int32_t disk_no                  = manager_.get_disk_manager().get_dest_disk();
      int64_t sstable_block_size       = THE_CHUNK_SERVER.get_param().get_sstable_block_size();
      bool is_sstable_exist            = false;

      if (disk_no < 0)
      {
        TBSYS_LOG(ERROR,"does't have enough disk space");
        sstable_id_.sstable_file_id_ = 0;
        ret = OB_CS_OUTOF_DISK_SPACE;
      }

      // if schema define sstable block size for table, use it
      // for the schema with version 2, the default block size is 64(KB),
      // we skip this case and use the config of chunkserver
      if (NULL != new_table_schema_ && new_table_schema_->get_block_size() > 0
          && 64 != new_table_schema_->get_block_size())
      {
        sstable_block_size = new_table_schema_->get_block_size();
      }
      
      if (OB_SUCCESS == ret)
      {
        /**
         * mustn't use the same sstable id in the the same disk, because 
         * if tablet isn't changed, we just add a hard link pointer to 
         * the old sstable file, so maybe different sstable file pointer 
         * the same content in disk. if we cancle daily merge, maybe 
         * some tablet meta isn't synced into index file, then we 
         * restart chunkserver will do daily merge again, it may reuse 
         * the same sstable id, if the sstable id is existent and it 
         * pointer to a share disk content, the sstable will be 
         * truncated if we create sstable with the sstable id. 
         */
        do
        {
          sstable_id_.sstable_file_id_     = (sstable_id_.sstable_file_id_ << 8) | (disk_no & 0xff);

          if ((OB_SUCCESS == ret) && (ret = get_sstable_path(sstable_id_,path_,sizeof(path_))) != OB_SUCCESS )
          {
            TBSYS_LOG(ERROR,"Merge : can't get the path of new sstable");
            ret = OB_ERROR;
          }

          if (OB_SUCCESS == ret)
          {
            is_sstable_exist = FileDirectoryUtils::exists(path_);
            if (is_sstable_exist)
            {
              sstable_id_.sstable_file_id_ = manager_.allocate_sstable_file_seq();
            }
          }
        } while (OB_SUCCESS == ret && is_sstable_exist);

        if (OB_SUCCESS == ret)
        {
          TBSYS_LOG(INFO,"create new sstable, sstable_path:%s, compressor:%.*s, "
                         "version=%ld, block_size=%ld\n",
              path_, compressor_string_.length(), compressor_string_.ptr(), 
              frozen_version_, sstable_block_size);
          path_string_.assign_ptr(path_,static_cast<int32_t>(strlen(path_) + 1));

          if ((ret = writer_.create_sstable(sstable_schema_,
                  path_string_, compressor_string_, frozen_version_, 
                  OB_SSTABLE_STORE_DENSE, sstable_block_size)) != OB_SUCCESS)
          {
            if (OB_IO_ERROR == ret)
              manager_.get_disk_manager().set_disk_status(disk_no,DISK_ERROR);
            TBSYS_LOG(ERROR,"Merge : create sstable failed : [%d]",ret);
          }
        }
      }
      return ret;
    }

    int ObTabletMerger::fill_scan_param(const uint64_t column_group_id) 
    {
      int ret = OB_SUCCESS;

      ObString table_name_string;

      //scan_param_.reset();
      scan_param_.set(new_range_.table_id_, table_name_string, new_range_);
      /**
       * in merge,we do not use cache,
       * and just read frozen mem table.
       */
      scan_param_.set_is_result_cached(false); 
      scan_param_.set_is_read_consistency(false);

      int64_t preread_mode = THE_CHUNK_SERVER.get_param().get_merge_scan_use_preread();
      if ( 0 == preread_mode )
      {
        scan_param_.set_read_mode(ObScanParam::SYNCREAD);
      }
      else
      {
        scan_param_.set_read_mode(ObScanParam::PREREAD);
      }

      ObVersionRange version_range;
      version_range.border_flag_.set_inclusive_end();

      version_range.start_version_ =  old_tablet_->get_data_version();
      version_range.end_version_   =  old_tablet_->get_data_version() + 1; 

      scan_param_.set_version_range(version_range);

      int32_t size = 0;
      const ObColumnSchemaV2 *col = chunk_merge_.current_schema_.get_group_schema(
          new_table_schema_->get_table_id(), column_group_id, size);

      if (NULL == col || size <= 0)
      {
        TBSYS_LOG(ERROR,"cann't find this column group:%lu",column_group_id);
        ret = OB_ERROR;
      }
      else
      {
        for(int32_t i = 0; i < size; ++i)
        {
          if ( (ret = scan_param_.add_column( (col + i)->get_id() ) ) != OB_SUCCESS)
          {
            TBSYS_LOG(ERROR,"add column id [%lu] to scan_param_ error[%d]",(col + i)->get_id(),ret);
            break;
          }
        }
      }
      return ret;
    }

    int ObTabletMerger::update_range_start_key()
    {
      std::swap(row_key_swap_->last_end_key_buf,row_key_swap_->start_key_buf);
      row_key_swap_->start_key_len = row_key_swap_->last_end_key_len;
      new_range_.start_key_.assign_ptr(row_key_swap_->start_key_buf,row_key_swap_->start_key_len);
      new_range_.border_flag_.unset_min_value();
      new_range_.border_flag_.unset_inclusive_start();
      return OB_SUCCESS;
    }

    int ObTabletMerger::update_range_end_key()
    {
      int ret = OB_SUCCESS;
      int32_t split_pos = new_table_schema_->get_split_pos();

      memcpy(row_key_swap_->last_end_key_buf, row_.get_row_key().ptr(),row_.get_row_key().length());
      row_key_swap_->last_end_key_len = row_.get_row_key().length();

      if (split_pos > 0)
      {
        if ( row_key_swap_->last_end_key_len < split_pos)
        {
          TBSYS_LOG(ERROR,"rowkey is too short");
          ret = OB_ERROR;
        }
        else
        {
          memset(row_key_swap_->last_end_key_buf + split_pos,0xFF,row_key_swap_->last_end_key_len - split_pos);
        }
      }

      if (OB_SUCCESS == ret)
      {
        new_range_.end_key_.assign_ptr(row_key_swap_->last_end_key_buf,row_key_swap_->last_end_key_len);
        new_range_.border_flag_.unset_max_value();
        new_range_.border_flag_.set_inclusive_end();
      }
      return ret;
    }

    int ObTabletMerger::create_hard_link_sstable(int64_t& sstable_size)
    {
      int ret = OB_SUCCESS;
      ObSSTableId old_sstable_id;
      char old_path[OB_MAX_FILE_NAME_LENGTH];
      sstable_size = 0;

      /**
       * when do daily merge, the old tablet is unchanged, there is no
       * dynamic data in update server for this old tablet. we needn't 
       * merge this tablet, just create a hard link for the unchanged 
       * tablet at the same disk. althrough the hard link only add the 
       * reference count of inode, and both sstable names refer to the
       * same sstable file, there is oly one copy on disk. 
       *  
       * after create a hard link, we also add the disk usage space. 
       * so the disk usage space statistic is not very correct, but 
       * when the sstable is recycled, the disk space of the sstable 
       * will be decreased. so we can ensure the recycle logical is 
       * correct. 
       */
      if (old_tablet_->get_sstable_id_list().get_array_index() > 0)
      {
        int32_t disk_no = old_tablet_->get_disk_no();

        /**
         * mustn't use the same sstable id in the the same disk, because 
         * if tablet isn't changed, we just add a hard link pointer to 
         * the old sstable file, so maybe different sstable file pointer 
         * the same content in disk. if we cancle daily merge, maybe 
         * some tablet meta isn't synced into index file, then we 
         * restart chunkserver will do daily merge again, it may reuse 
         * the same sstable id, if the sstable id is existent and it 
         * pointer to a share disk content, the sstable will be 
         * truncated if we create sstable with the sstable id. 
         */
        do
        {
          sstable_id_.sstable_file_id_ = manager_.allocate_sstable_file_seq();
          sstable_id_.sstable_file_id_ = (sstable_id_.sstable_file_id_ << 8) | (disk_no & 0xff);

          if ((ret = get_sstable_path(sstable_id_,path_,sizeof(path_))) != OB_SUCCESS )
          {
            TBSYS_LOG(ERROR, "create_hard_link_sstable: can't get the path of hard link sstable");
            ret = OB_ERROR;
          }
        } while (OB_SUCCESS == ret && FileDirectoryUtils::exists(path_));

        if (OB_SUCCESS == ret)
        {
          /**
           * FIXME: current we just support one tablet with only one 
           * sstable file 
           */
          sstable_size = (*old_tablet_->get_sstable_reader_list().at(0))->get_sstable_size();
          old_sstable_id = *old_tablet_->get_sstable_id_list().at(0);
          if ((ret = get_sstable_path(old_sstable_id, old_path, sizeof(old_path))) != OB_SUCCESS )
          {
            TBSYS_LOG(ERROR, "create_hard_link_sstable: can't get the path of old sstable");
            ret = OB_ERROR;
          }
          else if (0 != ::link(old_path, path_))
          {
            TBSYS_LOG(ERROR, "failed create hard link for unchanged sstable, "
                             "old_sstable=%s, new_sstable=%s",
              old_path, path_);
            ret = OB_ERROR;
          }
        }
      }

      return ret;
    }
    
    int ObTabletMerger::finish_sstable(const bool is_tablet_unchanged, 
      ObTablet*& new_tablet)
    {
      int64_t trailer_offset = 0;
      int64_t sstable_size = 0;
      int ret = OB_ERROR;
      new_tablet = NULL;
      TBSYS_LOG(DEBUG,"Merge : finish_sstable");
      if (!is_tablet_unchanged 
          && ((ret = writer_.close_sstable(trailer_offset,sstable_size)) != OB_SUCCESS 
              || sstable_size < 0))
      {
        TBSYS_LOG(ERROR,"Merge : close sstable failed.");
      }
      else
      {
        ObMultiVersionTabletImage& tablet_image = manager_.get_serving_tablet_image();
        const ObSSTableTrailer* trailer = NULL;
        ObTablet *tablet = NULL;
        ObTabletExtendInfo extend_info;
        row_num_ = 0;
        pre_column_group_row_num_ = 0;
        if ((ret = tablet_image.alloc_tablet_object(new_range_,frozen_version_,tablet)) != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR,"alloc_tablet_object failed.");
        }
        else
        {
          if (is_tablet_unchanged)
          {
            tablet->set_disk_no(old_tablet_->get_disk_no());
            ret = create_hard_link_sstable(sstable_size);
            if (OB_SUCCESS == ret && sstable_size > 0
                && old_tablet_->get_sstable_id_list().get_array_index() > 0)
            {
              trailer = &(*old_tablet_->get_sstable_reader_list().at(0))->get_trailer();
            }
          }
          else
          {
            trailer = &writer_.get_trailer();
            tablet->set_disk_no( (sstable_id_.sstable_file_id_) & DISK_NO_MASK);
          }

          if (OB_SUCCESS == ret)
          {
            //set new data version
            tablet->set_data_version(frozen_version_);
            if (tablet_merge_filter_.need_filter())
            {
              extend_info.last_do_expire_version_ = frozen_version_;
            }
            else 
            {
              extend_info.last_do_expire_version_ = old_tablet_->get_last_do_expire_version();
            }
            if (sstable_size > 0)
            {
              if (NULL == trailer)
              {
                TBSYS_LOG(ERROR, "after close sstable, trailer=NULL, "
                                 "is_tablet_unchanged=%d, sstable_size=%ld",
                  is_tablet_unchanged, sstable_size);
                ret = OB_ERROR;
              }
              else if ((ret = tablet->add_sstable_by_id(sstable_id_)) != OB_SUCCESS)
              {
                TBSYS_LOG(ERROR,"Merge : add sstable to tablet failed.");
              }
              else 
              {
                int64_t pos = 0;
                int64_t checksum_len = sizeof(uint64_t);
                char checksum_buf[checksum_len];

                extend_info.occupy_size_ = sstable_size;
                extend_info.row_count_ = trailer->get_row_count();
                //inherit sequence number from parent tablet
                extend_info.sequence_num_ = old_tablet_->get_sequence_num();
                ret = serialization::encode_i64(checksum_buf, 
                    checksum_len, pos, trailer->get_sstable_checksum());
                if (OB_SUCCESS == ret)
                {
                  extend_info.check_sum_ = ob_crc64(checksum_buf, checksum_len);
                }
              }
            }
            else if (0 == sstable_size)
            {
              /**
               * the tablet doesn't include sstable, the occupy_size_, 
               * row_count_ and check_sum_ in extend_info is 0, needn't set 
               * them again 
               */
            }
          }

          /**
           * we set the extent info of tablet here. when we write the 
           * index file, we needn't load the sstable of this tablet to get 
           * the extend info. 
           */
          if (OB_SUCCESS == ret)
          {
            tablet->set_extend_info(extend_info);
          }
        }
       
        if (OB_SUCCESS == ret)
        {
          ret = tablet_array_.push_back(tablet);
        }

        if (OB_SUCCESS == ret)
        {
          manager_.get_disk_manager().add_used_space(
            (sstable_id_.sstable_file_id_ & DISK_NO_MASK),
            sstable_size, !is_tablet_unchanged);
        }

        if (OB_SUCCESS == ret && NULL != tablet)
        {
          new_tablet = tablet;
          
          if (is_tablet_unchanged)
          {
            char range_buf[OB_RANGE_STR_BUFSIZ];
            new_range_.to_string(range_buf,sizeof(range_buf));
            TBSYS_LOG(INFO, "finish_sstable, create hard link sstable=%s, range=%s, "
                            "sstable_size=%ld, row_count=%ld", 
              path_, range_buf, sstable_size, extend_info.row_count_);
          }
        }
      }
      if (OB_SUCCESS == ret)
      {
        path_[0] = '\0';
        current_sstable_size_ = 0;
      }
      return ret;
    }
    
    int ObTabletMerger::update_meta()
    {
      int ret = OB_SUCCESS;
      ObMultiVersionTabletImage& tablet_image = manager_.get_serving_tablet_image();
      ObTablet *new_tablet_list[ tablet_array_.size() ];
      int32_t idx = 0;
      bool sync_meta = THE_CHUNK_SERVER.get_param().get_each_tablet_sync_meta() == 0 ? false : true;
      for(ObVector<ObTablet *>::iterator it = tablet_array_.begin(); it != tablet_array_.end(); ++it)
      {
        if (NULL == (*it)) //in case
        {
          ret = OB_ERROR;
          break;
        }
        new_tablet_list[idx++] = (*it);
      }

      if (tablet_array_.size() > 1)
      {
        TBSYS_LOG(INFO, "tablet splits to %d tablets", tablet_array_.size());
      }

      if (OB_SUCCESS == ret)
      {
        // in case we have migrated tablets, discard current merge tablet
        if (!old_tablet_->is_merged())
        {
          if (OB_SUCCESS != (ret = tablet_image.upgrade_tablet(
                  old_tablet_, new_tablet_list, idx, false)))
          {
            TBSYS_LOG(WARN,"upgrade new merged tablets error [%d]",ret);
          }
          //else if (OB_SUCCESS != (ret = report_tablets(new_tablet_list,idx)))
          //{
          //  TBSYS_LOG(WARN,"report tablets to rootserver error.");
          //}
          else
          {
            if (sync_meta)
            {
              // sync new tablet meta files;
              for(int32_t i = 0; i < idx; ++i)
              {
                if (OB_SUCCESS != (ret = tablet_image.write(
                        new_tablet_list[i]->get_data_version(),
                        new_tablet_list[i]->get_disk_no())))
                {
                  TBSYS_LOG(WARN,"write new meta failed i=%d, version=%ld, disk_no=%d", i , 
                      new_tablet_list[i]->get_data_version(), new_tablet_list[i]->get_disk_no());
                }
              }

              // sync old tablet meta files;
              if (OB_SUCCESS == ret 
                  && OB_SUCCESS != (ret = tablet_image.write(
                      old_tablet_->get_data_version(), old_tablet_->get_disk_no())))
              {
                TBSYS_LOG(WARN,"write old meta failed version=%ld, disk_no=%d", 
                    old_tablet_->get_data_version(), old_tablet_->get_disk_no());
              }
            }

            if (OB_SUCCESS == ret)
            {
              int64_t recycle_version = old_tablet_->get_data_version() 
                - (ObMultiVersionTabletImage::MAX_RESERVE_VERSION_COUNT - 1);
              if (recycle_version > 0)
              {
                manager_.get_regular_recycler().recycle_tablet(
                    old_tablet_->get_range(), recycle_version);
              }
            }

          }
        }
        else
        {
          TBSYS_LOG(INFO, "current tablet covered by migrated tablets, discard.");
        }
      }

      tablet_array_.reset();
      return ret;
    }

    int ObTabletMerger::report_tablets(ObTablet* tablet_list[],int32_t tablet_size)
    {
      int  ret = OB_SUCCESS;
      int64_t num = OB_MAX_TABLET_LIST_NUMBER;
      ObTabletReportInfoList *report_info_list =  GET_TSI_MULT(ObTabletReportInfoList, TSI_CS_TABLET_REPORT_INFO_LIST_1);

      if (tablet_size < 0)
      {
        ret = OB_INVALID_ARGUMENT;
      }
      else if (NULL == report_info_list)
      {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      }
      else
      {
        report_info_list->reset();
        ObTabletReportInfo tablet_info;
        for(int32_t i=0; i < tablet_size && num > 0 && OB_SUCCESS == ret; ++i)
        {
          manager_.fill_tablet_info(*tablet_list[i], tablet_info);
          if (OB_SUCCESS != (ret = report_info_list->add_tablet(tablet_info)) )
          {
            TBSYS_LOG(WARN, "failed to add tablet info, num=%ld, err=%d", num, ret);
          }
          else
          {
            --num;
          }
        }
        if (OB_SUCCESS != (ret = manager_.send_tablet_report(*report_info_list,true))) //always has more
        {
          TBSYS_LOG(WARN, "failed to send request report to rootserver err = %d",ret);
        }
      }
      return ret;
    }

    bool ObTabletMerger::maybe_change_sstable() const
    {
      bool ret = false;
      int64_t rowkey_cmp_size = new_table_schema_->get_split_pos();

      if (0 != rowkey_cmp_size)
      {
        if ( memcmp(row_.get_row_key().ptr(),cell_->row_key_.ptr(),rowkey_cmp_size) != 0)
        {
          TBSYS_LOG(DEBUG,"may be change sstable");
          ret = true;
        }
        else
        {
          TBSYS_LOG(DEBUG,"the same user,can't split,%d,prev_rowkey=%s, cur_rowkey=%s",
            new_table_schema_->get_split_pos(), print_string(row_.get_row_key()), 
            print_string(cell_->row_key_));
        }
      }
      else
      {
        ret = true;
      }
      return ret;
    }
  } /* chunkserver */
} /* oceanbase */
