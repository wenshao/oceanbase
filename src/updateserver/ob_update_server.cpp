/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: ob_update_server.cpp,v 0.1 2010/09/28 13:52:43 chuanhui Exp $
 *
 * Authors:
 *   chuanhui <rizhao.ych@taobao.com>
 *     - some work details if you want
 *
 */

#include "common/ob_trace_log.h"
#include "common/serialization.h"
#include "common/utility.h"
#include "common/ob_log_dir_scanner.h"
#include "common/ob_tsi_factory.h"
#include "common/ob_rs_ups_message.h"
#include "common/ob_token.h"
#include "common/ob_log_cursor.h"
#include "sstable/ob_aio_buffer_mgr.h"
#include "ob_update_server.h"
#include "ob_ups_utils.h"
#include "ob_update_server_main.h"
#include "ob_ups_clog_status.h"
#include <pthread.h>
#include "ob_client_wrapper.h"
#define __ups_debug__
#include "common/debug.h"

using namespace oceanbase::common;

#define RPC_CALL_WITH_RETRY(function, retry_times, timeout, server, args...) \
  ({ \
    int err = OB_RESPONSE_TIME_OUT; \
    for (int64_t i = 0; ObUpsRoleMgr::STOP != role_mgr_.get_state() \
        && ObUpsRoleMgr::FATAL != role_mgr_.get_state() \
        && OB_SUCCESS != err && i < retry_times; ++i) \
    { \
      int64_t timeu = tbsys::CTimeUtil::getMonotonicTime(); \
      err = ups_rpc_stub_.function(server, args, timeout);               \
      TBSYS_LOG(INFO, "%s, server=%s, retry_times=%ld, err=%d", #function, server.to_cstring(), i, err); \
      timeu = tbsys::CTimeUtil::getMonotonicTime() - timeu; \
      if (OB_SUCCESS != err \
          && timeu < timeout) \
      { \
        TBSYS_LOG(INFO, "timeu=%ld not match timeout=%ld, will sleep %ld", timeu, timeout, timeout - timeu); \
        int sleep_ret = precise_sleep(timeout - timeu); \
        if (OB_SUCCESS != sleep_ret) \
        { \
          TBSYS_LOG(ERROR, "precise_sleep ret=%d", sleep_ret); \
        } \
      } \
    } \
    err; \
  })

namespace oceanbase
{
  namespace updateserver
  {
    static const int32_t ADDR_BUF_LEN = 64;

    ObUpdateServer::ObUpdateServer(ObUpdateServerParam& param)
      :param_(param),
      rpc_buffer_(RPC_BUFFER_SIZE),
      read_task_queue_size_(DEFAULT_TASK_READ_QUEUE_SIZE),
      write_task_queue_size_(DEFAULT_TASK_WRITE_QUEUE_SIZE),
      lease_task_queue_size_(DEFAULT_TASK_LEASE_QUEUE_SIZE),
      log_task_queue_size_(DEFAULT_TASK_LOG_QUEUE_SIZE),
      store_thread_queue_size_(DEFAULT_STORE_THREAD_QUEUE_SIZE),
      preprocess_task_queue_size_(DEFAULT_TASK_PREPROCESS_QUEUE_SIZE),
      table_mgr_(ups_cache_),
      sstable_query_(sstable_mgr_),
      client_wrapper_(NULL),
      merger_schema_(NULL)
    {
    }

    ObUpdateServer::~ObUpdateServer()
    {
      if (NULL != merger_schema_)
      {
        delete merger_schema_;
        merger_schema_ = NULL;
      }

      if (NULL != client_wrapper_)
      {
        delete client_wrapper_;
        client_wrapper_ = NULL;
      }
    }

    int ObUpdateServer::initialize()
    {
      int err = OB_SUCCESS;
      __debug_init__();
      // do not handle batch packet.
      // process packet one by one.
      set_batch_process(false);
      is_log_mgr_start_ = false;
      lease_expire_time_us_ = 0;
      //last_keep_alive_time_ = 0;
      ups_renew_reserved_us_ = 0;

      if (OB_SUCCESS == err)
      {
        read_task_queue_size_ = static_cast<int32_t>(param_.get_read_task_queue_size());
        write_task_queue_size_ = static_cast<int32_t>(param_.get_write_task_queue_size());
        // TODO (rizhao)
        lease_task_queue_size_ = static_cast<int32_t>(param_.get_lease_task_queue_size());
        log_task_queue_size_ = static_cast<int32_t>(param_.get_log_task_queue_size());
        store_thread_queue_size_ = static_cast<int32_t>(param_.get_store_thread_queue_size());
      }

      if (OB_SUCCESS == err)
      {
        lease_timeout_in_advance_ = param_.get_lease_timeout_in_advance();
        keep_alive_valid_interval_ = param_.get_keep_alive_timeout();
        TBSYS_LOG(INFO, "load param: keep_alive_valid_interval=%ld", param_.get_keep_alive_timeout());
        slave_type_.set_sync_type(param_.get_slave_type());
        TBSYS_LOG(INFO, "load param: slave type =%s", slave_type_.get_type_str());
      }

      if (OB_SUCCESS == err)
      {
        err = client_manager_.initialize(get_transport(), get_packet_streamer());
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN, "failed to init client manager, err=%d", err);
        }
      }

      if (OB_SUCCESS == err)
      {
        root_server_.set_ipv4_addr(param_.get_root_server_ip(), param_.get_root_server_port());
        TBSYS_LOG(INFO, "load param: root_server addr=%s", root_server_.to_cstring());
      }

      if (OB_SUCCESS == err)
      {
        err = ups_rpc_stub_.init(&client_manager_);
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN, "fail to init rpc stub, err=%d", err);
        }
      }

      if (OB_SUCCESS == err)
      {
        err = set_listen_port(param_.get_ups_port());
      }

      if (OB_SUCCESS == err)
      {
        err = set_self_(param_.get_dev_name(), param_.get_ups_port());
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN, "fail to set self.");
        }
      }

      if (OB_SUCCESS == err)
      {
        if (strlen(param_.get_lsync_ip()) > 0)
        {
          lsync_server_.set_ipv4_addr(param_.get_lsync_ip(), param_.get_lsync_port());
          obi_slave_stat_ = STANDALONE_SLAVE;

          char addr_buf[ADDR_BUF_LEN];
          lsync_server_.to_string(addr_buf, sizeof(addr_buf));
          addr_buf[ADDR_BUF_LEN - 1] = '\0';
          TBSYS_LOG(INFO, "Slave stat set to STANDALONE_SLAVE, lsync_server=%s", addr_buf);
        }
        else
        {
          //obi_slave_stat_ = UNKNOWN_SLAVE;
          obi_slave_stat_ = FOLLOWED_SLAVE; // 去HA之后ups不用配置主instance ups的地址
        }
      }

      // set packet factory object
      if (OB_SUCCESS == err)
      {
        err = set_packet_factory(&packet_factory_);
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN, "fail to set packet factory.");
        }
      }

      // init ups cache
      if (OB_SUCCESS == err)
      {
        err = ups_cache_.init();
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN, "failed to init ups cache, err=%d", err);
        }
      }

      // init mgr
      if (OB_SUCCESS == err)
      {
        err = table_mgr_.init();
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN, "failed to init table mgr, err=%d", err);
        }
        else if (OB_SUCCESS != (err = user_table_cache_.init(&table_mgr_)))
        {
          TBSYS_LOG(WARN, "init user table cache fail err=%d", err);
        }
        else if (OB_SUCCESS != (err = skey_table_cache_.init(&table_mgr_)))
        {
          TBSYS_LOG(WARN, "init skey table cache fail err=%d", err);
        }
        else if (OB_SUCCESS != (err = perm_table_cache_.init(&table_mgr_)))
        {
          TBSYS_LOG(WARN, "init perm table cache fail err=%d", err);
        }
        else if (OB_SUCCESS != (err = login_mgr_.init(&skey_table_cache_, &user_table_cache_)))
        {
          TBSYS_LOG(WARN, "init login mgr fail err=%d", err);
        }
        else
        {
          table_mgr_.set_replay_checksum_flag(param_.get_replay_checksum_flag());
          ob_set_memory_size_limit(param_.get_total_memory_limit());
          MemTableAttr memtable_attr;
          if (OB_SUCCESS == table_mgr_.get_memtable_attr(memtable_attr))
          {
            memtable_attr.total_memlimit = param_.get_table_memory_limit();
            table_mgr_.set_memtable_attr(memtable_attr);
          }
          else
          {
            TBSYS_LOG(WARN, "fail in table_mgr");
          }
        }
      }

      if (OB_SUCCESS == err)
      {
        sstable::ObBlockCacheConf bc_conf;
        sstable::ObBlockIndexCacheConf bic_conf;
        bc_conf.block_cache_memsize_mb = param_.get_blockcache_size_mb();
        bic_conf.cache_mem_size = param_.get_blockindex_cache_size_mb() * 1024L * 1024L;
        err = sstable_query_.init(bc_conf, bic_conf);
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN, "failed to init sstable query, err=%d", err);
        }
        else
        {
          TBSYS_LOG(INFO, "init sstable query success");
        }
      }

      if (OB_SUCCESS == err)
      {
        const char *store_root = param_.get_store_root();
        const char *raid_regex = param_.get_raid_regex();
        const char *dir_regex = param_.get_dir_regex();
        err = sstable_mgr_.init(store_root, raid_regex, dir_regex);
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN, "failed to init sstable mgr, err=%d", err);
        }
        else
        {
          table_mgr_.reg_table_mgr(sstable_mgr_);
          if (!sstable_mgr_.load_new())
          {
            TBSYS_LOG(WARN, "sstable mgr load new fail");
            err = OB_ERROR;
          }
          else if (OB_SUCCESS != (err = table_mgr_.check_sstable_id()))
          {
            TBSYS_LOG(WARN, "check sstable id fail err=%d", err);
          }
          else
          {
            table_mgr_.log_table_info();
          }
        }
      }
      if (OB_SUCCESS == err)
      {
        err = slave_mgr_.init(&role_mgr_, &ups_rpc_stub_,
            param_.get_log_sync_timeout_us());
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN, "failed to init slave mgr, err=%d", err);
        }
        else
        {
          TBSYS_LOG(INFO, "slave_mgr init");
        }
      }
      if (OB_SUCCESS == err)
      {
        int64_t timeout_delta = 50 * 1000;
        int64_t n_blocks = 4;
        int64_t block_size_shift = 24;
        int64_t log_file_max_size = param_.get_log_size_mb() * 1000L * 1000L;
        if (OB_SUCCESS != (err = ups_log_server_getter_.init(this)))
        {
          TBSYS_LOG(ERROR, "master_getter.init()=>%d", err);
        }
        else if (OB_SUCCESS != (err = prefetch_log_task_submitter_.init(param_.get_lsync_fetch_timeout_us() + timeout_delta, this)))
        {
          TBSYS_LOG(ERROR, "prefetch_log_task_submitter_.init(this)=>%d", err);
        }
        else if (OB_SUCCESS != (err = replay_log_src_.init(&log_mgr_.get_log_buffer(), &prefetch_log_task_submitter_,
                &ups_log_server_getter_, &ups_rpc_stub_,
                param_.get_lsync_fetch_timeout_us(),
                n_blocks, block_size_shift)))
        {
          TBSYS_LOG(ERROR, "replay_log_src.init()=>%d", err);
        }
        else if (OB_SUCCESS != (err = log_mgr_.init(param_.get_log_dir_path(), log_file_max_size, &replay_log_src_,
                &table_mgr_, &slave_mgr_, &obi_role_, &role_mgr_,
                param_.get_log_sync_type())))
        {
          TBSYS_LOG(WARN, "failed to init log mgr, path=%s, log_file_size=%ld, err=%d",
              param_.get_log_dir_path(), log_file_max_size, err);
        }
      }
      if (OB_SUCCESS == err)
      {
        set_log_sync_delay_stat_param();
      }

      //add:

      if (OB_SUCCESS == err)
      {
        err = init_client_wrapper();
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN, "failed to init client wrapper, err=%d", err);
        }
      }

      if (OB_SUCCESS == err)
      {
        int64_t read_thread_count = param_.get_read_thread_count();
        int64_t store_thread_count = param_.get_store_thread_count();
        log_thread_queue_.setThreadParameter(1, this, NULL);
        read_thread_queue_.setThreadParameter(static_cast<int32_t>(read_thread_count), this, NULL);
        write_thread_queue_.setThreadParameter(1, this, NULL);
        int64_t preprocess_thread_count = param_.get_pre_process() ? 20 : 1;
        preprocess_thread_queue_.setThreadParameter(static_cast<int32_t>(preprocess_thread_count), this, NULL);
        lease_thread_queue_.setThreadParameter(1, this, NULL);
        store_thread_.setThreadParameter(static_cast<int32_t>(store_thread_count), this, NULL);
      }

      if (OB_SUCCESS == err)
      {
        err = log_replay_thread_.init(&log_mgr_, &obi_role_, &role_mgr_);
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN, "failed to start log replay thread, err=%d", err);
        }
        else
        {
          set_log_replay_thread_param();
        }
      }
      return err;
    }

    int ObUpdateServer::init_client_wrapper()
    {
      int err = OB_SUCCESS;

      // init client mgr
      client_wrapper_ = new(std::nothrow) ObClientWrapper(RPC_RETRY_TIMES, RPC_TIMEOUT,
          get_root_server(),
          get_self(),
          get_table_mgr(), get_ups_cache());
      if (NULL == client_wrapper_)
      {
        TBSYS_LOG(ERROR, "not enough memory");
        err = OB_MEM_OVERFLOW;
      }
      else
      {
        merger_schema_ = new(std::nothrow) ObMergerSchemaManager;
        if (NULL == merger_schema_)
        {
          TBSYS_LOG(ERROR, "not enough memory");
          err = OB_MEM_OVERFLOW;
        }
      }

      if (OB_SUCCESS == err)
      {
        err = merger_stub_.init(&merger_rpc_buffer_, &client_manager_);
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN, "failed to init merger stub, err=%d", err);
        }
      }

      if (OB_SUCCESS == err)
      {
        // init tablet location cache
        // TODO (rizhao): move to conf
        err = tablet_cache_.init(5000 * 5, 1000, 1000);
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN, "failed to init tablet cache, err=%d", err);
        }
      }

      if (OB_SUCCESS == err)
      {
        err = client_wrapper_->init(&merger_stub_, merger_schema_, &tablet_cache_);
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN, "failed to init client wrapper, err=%d", err);
        }
      }

      return err;
    }

    void ObUpdateServer::wait_for_queue()
    {
      /*
         log_thread_queue_.wait();
         read_thread_queue_.wait();
         write_thread_queue_.wait();
       */
    }

    void ObUpdateServer::destroy()
    {
      role_mgr_.set_state(ObUpsRoleMgr::STOP);
      log_mgr_.signal_stop();
    }

    void ObUpdateServer::cleanup()
    {
      /// 写线程
      write_thread_queue_.stop();

      /// 读线程
      read_thread_queue_.stop();

      // preprocess thread
      preprocess_thread_queue_.stop();

      /// Lease线程
      lease_thread_queue_.stop();

      /// Check线程
      //check_thread_.stop();

      /// 转储线程
      store_thread_.stop();

      ///日志回放线程
      log_replay_thread_.stop();

      /// 写线程
      write_thread_queue_.wait();

      /// 读线程
      read_thread_queue_.wait();

      /// preprocess thread
      preprocess_thread_queue_.wait();

      /// Lease线程
      lease_thread_queue_.wait();

      /// 转储线程
      store_thread_.wait();

      ///日志回放线程

      timer_.destroy();
      transport_.stop();
      transport_.wait();
    }

    int ObUpdateServer::start_timer_schedule()
    {
      int err = OB_SUCCESS;
      err = set_timer_check_keep_alive();
      if (OB_SUCCESS != err)
      {
        TBSYS_LOG(WARN, "fail to set timer to check_keep_alive. err=%d", err);
      }
      if (OB_SUCCESS == err)
      {
        if (OB_SUCCESS != (err = set_timer_check_lease()))
        {
          TBSYS_LOG(WARN, "fail to set timer to check lease. err=%d", err);
        }
      }
      if (OB_SUCCESS == err)
      {
        if (OB_SUCCESS != (err = set_timer_grant_keep_alive()))
        {
          TBSYS_LOG(WARN, "fail to set timer to grant keep_alive, err=%d", err);
        }
      }
      if (OB_SUCCESS == err)
      {
        if (OB_SUCCESS != (err = set_timer_handle_fronzen()))
        {
          TBSYS_LOG(WARN, "fail to set timer to handle frozen, err=%d", err);
        }
      }
      if (OB_SUCCESS == err)
      {
        if (OB_SUCCESS != (err = set_timer_refresh_lsync_addr()))
        {
          TBSYS_LOG(WARN, "fail to set timer to refresh_lsync, err=%d", err);
        }
      }
      if (OB_SUCCESS == err)
      {
        if (OB_SUCCESS != (err = set_timer_switch_skey()))
        {
          TBSYS_LOG(WARN, "fail to set timer to switch skey. err=%d", err);
        }
      }
      if (OB_SUCCESS == err)
      {
        if (OB_SUCCESS != (err = set_timer_major_freeze()))
        {
          TBSYS_LOG(WARN, "fail to set timer to major freeze. err=%d", err);
        }
      }
      if (OB_SUCCESS == err)
      {
        if (OB_SUCCESS != (err = set_timer_time_update()))
        {
          TBSYS_LOG(WARN, "fail to set timer to time udpate. err=%d", err);
        }
      }

      return err;
    }

    int ObUpdateServer::start_service()
    {
      int err = OB_SUCCESS;
      obi_role_.set_role(ObiRole::SLAVE);

      err = start_threads();
      if (OB_SUCCESS == err)
      {
        err = timer_.init();
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(ERROR, "ObTimer init error, err=%d", err);
        }
        else
        {
          TBSYS_LOG(INFO, "init timer success");
        }
      }

      //提交一次本地日志回放任务
      if (OB_SUCCESS == err)
      {
        err = submit_replay_commit_log();
      }

      //获取本地最大日志号去找RS注册
      int64_t log_id = 0;
      if (OB_SUCCESS == err)
      {
        err = log_mgr_.get_max_log_seq_replayable(log_id);
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN, "fail to get max log seq replayable. err=%d", err);
        }
      }
      if (OB_SUCCESS == err)
      {
        register_to_rootserver(log_id);
        err = set_schema();
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN, "fail to set schema. err=%d", err);
        }
      }

      if (OB_SUCCESS == err)
      {
        err = start_timer_schedule();
      }

      while (ObUpsRoleMgr::STOP != role_mgr_.get_state())
      {
        if (!log_mgr_.is_log_replay_started())
        {
          submit_replay_commit_log();
        }
        if (OB_SUCCESS == err)
        {
          tbsys::CThreadGuard guard(&mutex_);
          if (ObiRole::MASTER == obi_role_.get_role()
              && ObUpsRoleMgr::MASTER == role_mgr_.get_role()
              && ObUpsRoleMgr::REPLAYING_LOG == role_mgr_.get_state()
              && log_mgr_.is_log_replay_finished())
          {
            if (!is_log_mgr_start_)
            {
              is_log_mgr_start_ = true;
              if (OB_SUCCESS != (err = log_mgr_.start_log_for_master_write()))
              {
                TBSYS_LOG(INFO, "log_mgr.start_log_for_master_write()=>%d", err);
              }
              else if (OB_SUCCESS!= (err = table_mgr_.write_start_log()))
              {
                TBSYS_LOG(WARN, "fail to start log");
              }
              if (OB_SUCCESS == err)
              {
                if (OB_SUCCESS != (err = table_mgr_.sstable_scan_finished(param_.get_minor_num_limit())))
                {
                  TBSYS_LOG(ERROR, "sstable_scan_finished error, err=%d", err);
                }
                else
                {
                  table_mgr_.log_table_info();
                }
              }
            }
            if (OB_SUCCESS == err)
            {
              bool write_log = true;
              if (OB_SUCCESS != (err = update_schema(true, write_log)))
              {
                TBSYS_LOG(WARN, "updata_schema fail ,err=%d", err);
              }
            }
            if (OB_SUCCESS != err)
            {
              role_mgr_.set_state(ObUpsRoleMgr::FATAL);
            }
            else
            {
              role_mgr_.set_state(ObUpsRoleMgr::ACTIVE);
            }
          }
        }
        usleep(10 * 1000);
      }

      if (ObUpsRoleMgr::STOP == role_mgr_.get_state())
      {
        TBSYS_LOG(INFO, "UPS server exist");
      }

      cleanup();
      TBSYS_LOG(INFO, "server stoped.");
      return err;
    }

    void ObUpdateServer::stop()
    {
      ObUpsRoleMgr::State server_stat = role_mgr_.get_state();
      UNUSED(server_stat);
      //if (ObUpsRoleMgr::HOLD == server_stat)
      //{
      //  TBSYS_LOG(WARN, "server stat will switch to HOLD");
      //  while (true)
      //  {
      //    usleep(10 * 1000); // sleep 10ms
      //  }
      //}

      TBSYS_LOG(INFO, "ups stop.");
      stoped_ = true;
      destroy();
      transport_.stop();
      transport_.wait();
    }

    int ObUpdateServer::start_threads()
    {
      int ret = OB_SUCCESS;

      /// 写线程
      write_thread_queue_.start();

      /// 读线程
      read_thread_queue_.start();

      /// preprocess thread
      preprocess_thread_queue_.start();

      /// Lease线程
      lease_thread_queue_.start();

      /// 转储线程
      store_thread_.start();

      ///日志回放线程
      log_replay_thread_.start();

      return ret;
    }

    int ObUpdateServer::check_frozen_version()
    {
      int err = OB_SUCCESS;
      int64_t rs_last_frozen_version = -1;
      int64_t ups_last_frozen_version = -1;
      err = RPC_CALL_WITH_RETRY(get_rs_last_frozen_version, INT64_MAX, RPC_TIMEOUT, root_server_, rs_last_frozen_version);
      if (err != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR, "get_last_frozen_version from root_server failed, err=%d", err);
      }
      else if (rs_last_frozen_version < 0)
      {
        TBSYS_LOG(WARN, "get_last_frozen_version()=>%ld", rs_last_frozen_version);
      }
      else if (OB_SUCCESS != (err = table_mgr_.get_last_frozen_memtable_version((uint64_t&)ups_last_frozen_version)))
      {
        TBSYS_LOG(ERROR, "table_mgr.get_last_frozen_version()=>%d", err);
      }
      else if (rs_last_frozen_version == ups_last_frozen_version)
      {
        TBSYS_LOG(INFO, "check_frozen_version(rs_last_frozen_version[%ld] == ups_last_frozen_version)",
                  rs_last_frozen_version);
      }
      else if (rs_last_frozen_version < ups_last_frozen_version)
      {
        TBSYS_LOG(WARN, "rs_last_frozen_version[%ld] < ups_last_frozen_version[%ld]",
                  rs_last_frozen_version, ups_last_frozen_version);
      }
      else
      {
        err = OB_ERROR;
        TBSYS_LOG(ERROR, "last_frozen_version[%ld] > ups_frozen_version[%ld].", rs_last_frozen_version, ups_last_frozen_version);
      }
      return err;
    }

    int ObUpdateServer::switch_to_master_master()
    {
      int err = OB_SUCCESS;
      int64_t wait_us = 10000;

      TBSYS_LOG(INFO, "SWITCHING state happen");
      tbsys::CThreadGuard guard(&mutex_);
      //等待log_replay_thread的完成
      while(!stoped_ && !log_replay_thread_.wait_stop())
      {
        usleep(static_cast<useconds_t>(wait_us));
      }
      if (stoped_)
      {
        err = OB_CANCELED;
      }
      if (OB_SUCCESS == err)
      {
        if (log_mgr_.is_log_replay_finished())
        {
          ObLogCursor replayed_cursor;
          int64_t log_id = 0;
          if (OB_SUCCESS != (err = log_mgr_.get_replayed_cursor(replayed_cursor)))
          {
            TBSYS_LOG(ERROR, "get_replayed_cursor()=>%d", err);
          }
          else if (OB_SUCCESS != (err = log_mgr_.get_max_log_seq_replayable(log_id)))
          {
            TBSYS_LOG(ERROR, "get_max_log_seq_replayable()=>%d", err);
          }
          else if (replayed_cursor.log_id_ != log_id)
          {
            //role_mgr_.set_state(ObUpsRoleMgr::FATAL);
            TBSYS_LOG(ERROR, "replayed_cursor.log_id[%ld] != log_id[%ld]", replayed_cursor.log_id_, log_id);
          }
          if (OB_SUCCESS == err)
          {
            TBSYS_LOG(INFO, "switch to master_master succ[replayed_cursor=%s].", replayed_cursor.to_str());
            obi_role_.set_role(ObiRole::MASTER);
            role_mgr_.set_role(ObUpsRoleMgr::MASTER);
          }
        }
        else
        {
          TBSYS_LOG(INFO, "switch to master_master succ");
          obi_role_.set_role(ObiRole::MASTER);
          role_mgr_.set_role(ObUpsRoleMgr::MASTER);
        }
      }
      else
      {
        TBSYS_LOG(ERROR, "switch to master_master fail.");
      }
      return err;
    }
    int ObUpdateServer::master_switch_to_slave(const bool is_obi_change, const bool is_role_change)
    {
      int err = OB_SUCCESS;
      int64_t wait_us = 10000;
      tbsys::CThreadGuard guard(&mutex_);
      ObLogCursor replayed_cursor;
      if (is_obi_change)
      {
        obi_role_.set_role(ObiRole::SLAVE);
      }
      if (is_role_change)
      {
        role_mgr_.set_role(ObUpsRoleMgr::SLAVE);
      }
      write_thread_queue_.notify_state_change();
      while(!stoped_ && !write_thread_queue_.wait_state_change_ack())
      {
        usleep(static_cast<useconds_t>(wait_us));
      }
      log_mgr_.get_replayed_cursor(replayed_cursor);
      TBSYS_LOG(INFO, "wait write_thread ack succ[replayed_cursor=%s].", replayed_cursor.to_str());
      while(!stoped_ && !log_replay_thread_.wait_start())
      {
        usleep(static_cast<useconds_t>(wait_us));
      }
      TBSYS_LOG(INFO, "wait log_replay_thread start to work succ.");
      if (stoped_)
      {
        err = OB_CANCELED;
      }
      if (OB_SUCCESS == err)
      {
        slave_mgr_.reset_slave_list();
      }
      return err;
    }
    int ObUpdateServer::slave_change_role(const bool is_obi_change, const bool is_role_change)
    {
      int err= OB_SUCCESS;
      tbsys::CThreadGuard guard(&mutex_);
      if (ObiRole::MASTER == obi_role_.get_role()
          && ObUpsRoleMgr::SLAVE == role_mgr_.get_role()
          && is_obi_change && is_role_change)
      {
        TBSYS_LOG(INFO, "switch happen, master_slave ====> slave_master");
        obi_role_.set_role(ObiRole::SLAVE);
        role_mgr_.set_role(ObUpsRoleMgr::MASTER);
      }
      else if (ObiRole::MASTER == obi_role_.get_role()
          && ObUpsRoleMgr::SLAVE == role_mgr_.get_role()
          && is_obi_change && (!is_role_change))
      {
        TBSYS_LOG(INFO, "switch happen, master_slave ====> slave_salve");
        obi_role_.set_role(ObiRole::SLAVE);
      }
      else if (ObiRole::SLAVE == obi_role_.get_role()
          && ObUpsRoleMgr::MASTER == role_mgr_.get_role()
          && is_obi_change && is_role_change)
      {
        TBSYS_LOG(INFO, "switch happen, slave_master ====> master_slave");
        slave_report_quit();
        slave_mgr_.reset_slave_list();
        obi_role_.set_role(ObiRole::MASTER);
        role_mgr_.set_role(ObUpsRoleMgr::SLAVE);
      }
      else if (ObiRole::SLAVE == obi_role_.get_role()
          && ObUpsRoleMgr::MASTER == role_mgr_.get_role()
          && !is_obi_change && is_role_change)
      {
        TBSYS_LOG(INFO, "switch happen, slave_master ====> slave_slave");
        slave_report_quit();
        slave_mgr_.reset_slave_list();
        role_mgr_.set_role(ObUpsRoleMgr::SLAVE);
      }
      else if (ObiRole::SLAVE == obi_role_.get_role()
          && ObUpsRoleMgr::SLAVE == role_mgr_.get_role()
          && is_obi_change && !is_role_change)
      {
        TBSYS_LOG(INFO, "switch happen, slave_slave ====> master_slave");
        obi_role_.set_role(ObiRole::MASTER);
      }
      else if (ObiRole::SLAVE == obi_role_.get_role()
          && ObUpsRoleMgr::SLAVE == role_mgr_.get_role()
          && !is_obi_change && is_role_change)
      {
        TBSYS_LOG(INFO, "switch happen, slave_slave ====> slave_master");
        role_mgr_.set_role(ObUpsRoleMgr::MASTER);
      }
      else
      {
        TBSYS_LOG(WARN, "invalid switch case. cur_obi_role=%s, cur_role=%s, is_obi_change=%s, is_role_change=%s", 
            obi_role_.get_role_str(), role_mgr_.get_role_str(), 
            is_obi_change ? "TRUE" : "FALSE", is_role_change ? "TRUE" : "FALSE");
      }

      return err;
    }

    //int ObUpdateServer::reregister_standalone()
    //{
    //  int err = OB_SUCCESS;

    //  TBSYS_LOG(INFO, "reregister STANDALONE SLAVE");
    //  is_registered_to_ups_ = false;

    //  // slave register
    //  if (OB_SUCCESS == err)
    //  {
    //    uint64_t log_id_start = 0;
    //    uint64_t log_seq_start = 0;
    //    err = slave_register_standalone(log_id_start, log_seq_start);
    //    if (OB_SUCCESS != err)
    //    {
    //      TBSYS_LOG(WARN, "failed to register, err=%d", err);
    //    }
    //  }

    //  if (OB_SUCCESS != err)
    //  {
    //    TBSYS_LOG(INFO, "REREGISTER err");
    //  }
    //  else
    //  {
    //    TBSYS_LOG(INFO, "REREGISTER succ");
    //  }
    //  return err;
    //}

    int ObUpdateServer::set_schema()
    {
      int err = OB_SUCCESS;

      if (!table_mgr_.get_schema_mgr().has_schema())
      {
        bool write_log = false;
        err = update_schema(true, write_log);
      }

      return err;
    }

    int ObUpdateServer::set_timer_switch_skey()
    {
      int err = OB_SUCCESS;

      bool repeat = true;
      err = timer_.schedule(switch_skey_duty_, SKEY_UPDATE_PERIOD, repeat);
      if (OB_SUCCESS != err)
      {
        TBSYS_LOG(WARN, "schedule switch_skey_duty fail err=%d", err);
      }
      else
      {
        submit_switch_skey();
      }
      return err;
    }

    int ObUpdateServer::set_timer_check_keep_alive()
    {
      int err = OB_SUCCESS;
      bool repeat = true;
      err = timer_.schedule(check_keep_alive_duty_, param_.get_state_check_period_us(), repeat);
      if (OB_SUCCESS != err)
      {
        TBSYS_LOG(WARN, "schedule check_keep_alive_duty_ fail .err=%d", err);
      }
      else
      {
        submit_check_keep_alive();
      }
      return err;
    }

    int ObUpdateServer::set_timer_check_lease()
    {
      int err = OB_SUCCESS;
      bool repeat = true;
      err = timer_.schedule(ups_lease_task_, DEFAULT_CHECK_LEASE_INTERVAL, repeat);
      if (OB_SUCCESS != err)
      {
        TBSYS_LOG(WARN, "schedule check_keep_alive_duty_ fail .err=%d", err);
      }
      else
      {
        submit_lease_task();
      }
      return err;
    }

    int ObUpdateServer::set_timer_grant_keep_alive()
    {
      int err = OB_SUCCESS;
      bool repeat = true;
      err = timer_.schedule(grant_keep_alive_duty_, GRANT_KEEP_ALIVE_PERIOD, repeat);
      if (OB_SUCCESS != err)
      {
        TBSYS_LOG(WARN, "schedule check_keep_alive_duty_ fail .err=%d", err);
      }
      else
      {
        submit_grant_keep_alive();
      }
      return err;
    }

    int ObUpdateServer::set_timer_refresh_lsync_addr()
    {
      int err = OB_SUCCESS;

      bool repeat = true;
      err = timer_.schedule(refresh_lsync_addr_duty_, param_.get_refresh_lsync_addr_interval_us(), repeat);
      if (OB_SUCCESS != err)
      {
        TBSYS_LOG(WARN, "schedule refresh_lsync_addr_duty fail err=%d", err);
      }

      return err;
    }

    int ObUpdateServer::set_timer_major_freeze()
    {
      int err = OB_SUCCESS;

      bool repeat = true;
      err = timer_.schedule(major_freeze_duty_, MajorFreezeDuty::SCHEDULE_PERIOD, repeat);
      if (OB_SUCCESS != err)
      {
        TBSYS_LOG(WARN, "schedule major_freeze_duty fail err=%d", err);
      }

      return err;
    }

    int ObUpdateServer::set_timer_handle_fronzen()
    {
      int err = OB_SUCCESS;

      bool repeat = true;
      err = timer_.schedule(handle_frozen_duty_, HandleFrozenDuty::SCHEDULE_PERIOD, repeat);
      if (OB_SUCCESS != err)
      {
        TBSYS_LOG(WARN, "schedule handle_frozen_duty fail err=%d", err);
      }

      return err;
    }

    int ObUpdateServer::set_timer_time_update()
    {
      int err = OB_SUCCESS;

      bool repeat = true;
      err = timer_.schedule(time_update_duty_, TimeUpdateDuty::SCHEDULE_PERIOD, repeat);
      if (OB_SUCCESS != err)
      {
        TBSYS_LOG(WARN, "schedule time_update_duty fail err=%d", err);
      }

      return err;
    }

    int ObUpdateServer::register_to_master_ups(const ObServer &master)
    {
      int err = OB_SUCCESS;
      ObUpsFetchParam fetch_param;
      uint64_t max_log_seq;
      ObServer null_server;
      if (null_server == master)
      {
        TBSYS_LOG(INFO, "master ups is %s, need wait new master ups", null_server.to_cstring());
      }
      else
      {
        if (OB_SUCCESS == err)
        {
          err = slave_register_followed(master, fetch_param, max_log_seq);
          if (OB_SUCCESS != err)
          {
            TBSYS_LOG(WARN, "failed to register, err=%d, master_addr=%s", err, master.to_cstring());
          }
          else
          {
            TBSYS_LOG(INFO, "register to master ups succ. master_ups=%s", master.to_cstring());
          }
        }
        if (OB_SUCCESS == err)
        {
          log_mgr_.set_master_log_id(max_log_seq);
          TBSYS_LOG(INFO, "log_mgr_.set_master_log_id=%ld", max_log_seq);
        }
      }
      return err;
    }

    //int ObUpdateServer::register_and_start_fetch(const ObServer &master, uint64_t &replay_point)
    //{
    //  int err = OB_SUCCESS;
    //  ObUpsFetchParam fetch_param;
    //  if (OB_SUCCESS == err)
    //  {
    //    err = slave_register_followed(master, fetch_param);
    //    if (OB_SUCCESS != err)
    //    {
    //      TBSYS_LOG(WARN, "failed to register");
    //    }
    //  }
    //  if (OB_SUCCESS == err)
    //  {
    //    if (fetch_param.max_log_id_ < static_cast<uint64_t>(replay_start_log_id_))
    //    {
    //      err = OB_ERROR;
    //      TBSYS_LOG(ERROR, "slave ups has bigger log id than master ups, fetch_param.max_log_id=%ld, replay_start_log_id=%ld", fetch_param.max_log_id_, replay_start_log_id_);
    //    }
    //  }
    //  if (OB_SUCCESS == err)
    //  {
    //    //fetch_thread_.clear();
    //    //fetch_thread_.set_fetch_param(fetch_param);
    //    //fetch_thread_.set_master(master);
    //    //fetch_thread_.start();
    //  }
    //  if (OB_SUCCESS == err)
    //  {
    //    replay_point = fetch_param.min_log_id_;
    //    // compare the max log file and the fetch replay_point
    //    if (replay_point > static_cast<uint64_t>(replay_start_log_id_))
    //    {
    //      //slave' log too old. destory the memetable.
    //      TableMgr *table_mgr = table_mgr_.get_table_mgr();
    //      table_mgr->destroy();
    //      table_mgr->init();
    //      replay_start_log_id_ = replay_point;
    //      replay_start_log_seq_ = 0;
    //    }
    //    TBSYS_LOG(INFO, "set log replay point to %lu", replay_start_log_id_);
    //  }
    //  if (OB_SUCCESS != err)
    //  {
    //    TBSYS_LOG(WARN, "fail to register and start fetch, err=%d", err);
    //  }
    //  else
    //  {
    //    TBSYS_LOG(INFO, "register to master ups succ.");
    //  }
    //  return err;
    //}

    int ObUpdateServer::renew_master_inst_ups()
    {
      int err = OB_SUCCESS;
      err = ups_rpc_stub_.get_inst_master_ups(root_server_, ups_inst_master_, DEFAULT_NETWORK_TIMEOUT);
      if (OB_SUCCESS == err)
      {
        TBSYS_LOG(INFO, "renew master inst ups =%s", ups_inst_master_.to_cstring());
      }
      else
      {
        TBSYS_LOG(WARN, "fail to get inst master ups. err=%d", err);
      }
      return err;
    }

    int ObUpdateServer::slave_standalone_prepare(uint64_t &log_id_start, uint64_t &log_seq_start)
    {
      int err = OB_SUCCESS;

      if (OB_SUCCESS == err)
      {
        if (OB_SUCCESS != (err = slave_register_standalone(log_id_start, log_seq_start)))
        {
          TBSYS_LOG(ERROR, "failed to register to lsync");
        }
        else if (0 == log_id_start)
        {
          TBSYS_LOG(ERROR, "failed to register to lsync, lsync returned log_id_start=0");
          err = OB_ERROR;
        }
       // else
       // {
       //   TBSYS_LOG(INFO, "set log_id_start=%lu log_seq_start=%lu", log_id_start, log_seq_start);
       //   log_mgr_.set_replay_point(log_id_start);
       //   err = log_mgr_.start_log(log_id_start, log_seq_start);
       //   if (OB_SUCCESS != err)
       //   {
       //     TBSYS_LOG(ERROR, "start log error, err=%d", err);
       //   }
       // }
      }

     // if (OB_SUCCESS == err)
     // {
     //   err = fetch_lsync_.init(get_lsync_server(), log_id_start, log_seq_start,
     //       &ups_rpc_stub_, &clog_receiver_, param_.get_lsync_fetch_timeout_us(), &role_mgr_);
     //   if (OB_SUCCESS != err)
     //   {
     //     TBSYS_LOG(ERROR, "ObUpsFetchLsync init error, err=%d", err);
     //   }
     //   else
     //   {
     //     fetch_lsync_.start();
     //     TBSYS_LOG(INFO, "Lsync fetch thread start");
     //   }
     // }

      return err;
    }

   // int ObUpdateServer::set_replay_thread(const uint64_t log_id_start, const uint64_t log_seq_start)
   // {
   //   int err = OB_SUCCESS;
   //   UNUSED(log_id_start);
   //   UNUSED(log_seq_start);
   //   //fetch_thread_.wait();
   //   log_replay_thread_.clear();
   //   //log_replay_thread_.set_max_log_file_id(log_id_start);
   //   TBSYS_LOG(INFO, "log replay thread reset file id=%ld, log seq=%ld", log_id_start, log_seq_start);
   //   //err = log_replay_thread_.reset_file_id(log_id_start, log_seq_start);
   //   //if (OB_SUCCESS == err)
   //   //{
   //   //  TBSYS_LOG(INFO, "start log replay thread");
   //   //  log_replay_thread_.start();
   //   //}
   //   //else
   //   //{
   //   //  TBSYS_LOG(WARN, "fail to reset file id. err=%d", err);
   //   //}
   //   return err;
   // }


    int ObUpdateServer::start_standalone_()
    {
      int err = OB_SUCCESS;
      CommonSchemaManagerWrapper schema_mgr;
      tbsys::CConfig config;

      // load schema from schema ini file
      bool parse_ret = schema_mgr.parse_from_file(param_.get_standalone_schema(), config);
      if (!parse_ret)
      {
        TBSYS_LOG(WARN, "failed to load schema");
        err = OB_ERROR;
      }
      else
      {
        err = table_mgr_.set_schemas(schema_mgr);
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN, "failed to set schema, err=%d", err);
        }
      }

      // start read and write thread
      if (OB_SUCCESS == err)
      {
        read_thread_queue_.start();
        write_thread_queue_.start();
      }

      // modify standalone status
      if (OB_SUCCESS == err)
      {
        role_mgr_.set_state(ObUpsRoleMgr::ACTIVE);
      }

      TBSYS_LOG(INFO, "start service, role=standalone, err=%d", err);
      // wait finish
      if (OB_SUCCESS == err)
      {
        read_thread_queue_.wait();
        write_thread_queue_.wait();
      }

      return err;
    }

    int ObUpdateServer::set_self_(const char* dev_name, const int32_t port)
    {
      int ret = OB_SUCCESS;
      int32_t ip = tbsys::CNetUtil::getLocalAddr(dev_name);
      if (0 == ip)
      {
        TBSYS_LOG(ERROR, "cannot get valid local addr on dev:%s.", dev_name);
        ret = OB_ERROR;
      }
      if (OB_SUCCESS == ret)
      {
        bool res = self_addr_.set_ipv4_addr(ip, port);
        if (!res)
        {
          TBSYS_LOG(ERROR, "chunk server dev:%s, port:%d is invalid.",
              dev_name, port);
          ret = OB_ERROR;
        }
        else
        {
          TBSYS_LOG(INFO, "update server addr =%s", self_addr_.to_cstring());
        }
      }

      return ret;
    }

    int ObUpdateServer::report_frozen_version_()
    {
      int ret = OB_SUCCESS;
      int64_t num_times = param_.get_resp_root_times();
      int64_t timeout = param_.get_resp_root_timeout_us();
      uint64_t last_frozen_version = 0;
      ret = table_mgr_.get_last_frozen_memtable_version(last_frozen_version);
      if (OB_SUCCESS == ret)
      {
        ret = RPC_CALL_WITH_RETRY(report_freeze, num_times, timeout, root_server_, ups_master_, last_frozen_version);
      }
      if (OB_RESPONSE_TIME_OUT == ret)
      {
        TBSYS_LOG(ERROR, "report fronzen version timeout, num_times=%ld, timeout=%ldus", num_times, timeout);
      }
      else if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "error occurs when report frozen version, ret=%d", ret);
      }
      else
      {
        TBSYS_LOG(INFO, "report succ frozen version=%ld", last_frozen_version);
      }
      return ret;
    }

    int ObUpdateServer::replay_commit_log_()
    {
      int err = OB_SUCCESS;
      if (log_mgr_.is_log_replay_started())
      {
        TBSYS_LOG(WARN, "replay process already started, refuse to replay twice");
      }
      else
      {
        err = log_mgr_.do_replay_local_log_task();
      }
      return err;
    }

    int ObUpdateServer::prefetch_remote_log_(ObDataBuffer& in_buf)
    {
      int err = OB_SUCCESS;
      ObPrefetchLogTaskSubmitter::Task task;
      if (in_buf.get_position() + (int64_t)sizeof(task) > in_buf.get_capacity())
      {
        err = OB_ERR_UNEXPECTED;
        TBSYS_LOG(ERROR, "pos[%ld] + sizeof(task)[%ld] > capacity[%ld]",
                  in_buf.get_position(), sizeof(task), in_buf.get_capacity());
      }
      else
      {
        task = *((typeof(task)*)(in_buf.get_data() + in_buf.get_position()));
      }
      if (OB_SUCCESS != err)
      {}
      else if (OB_SUCCESS != (err = replay_log_src_.prefetch_log())
          && OB_NEED_RETRY != err)
      {
        TBSYS_LOG(WARN, "replay_log_src.prefetch_log()=>%d", err);
      }
      else
      {
        err = OB_SUCCESS;
      }
      prefetch_log_task_submitter_.done(task);
      return err;
    }

    int ObUpdateServer::update_schema(const bool always_try, const bool write_log)
    {
      int err = OB_SUCCESS;
      int64_t num_times = always_try ? INT64_MAX : param_.get_fetch_schema_times();
      int64_t timeout = param_.get_fetch_schema_timeout_us();

      CommonSchemaManagerWrapper schema_mgr;
      err = RPC_CALL_WITH_RETRY(fetch_schema, num_times, timeout, root_server_, 0, schema_mgr);

      if (OB_RESPONSE_TIME_OUT == err)
      {
        TBSYS_LOG(ERROR, "fetch schema timeout, num_times=%ld, timeout=%ldus",
            num_times, timeout);
        err = OB_RESPONSE_TIME_OUT;
      }
      else if (OB_SUCCESS != err)
      {
        TBSYS_LOG(WARN, "Error occurs when fetching schema, err=%d", err);
      }
      else
      {
        TBSYS_LOG(INFO, "Fetching schema succeed version=%ld", schema_mgr.get_version());
      }

      if (OB_SUCCESS == err)
      {
        if (write_log)
        {
          TBSYS_LOG(INFO, "start to switch schemas");
          err = table_mgr_.switch_schemas(schema_mgr);
        }
        else
        {
          err = table_mgr_.set_schemas(schema_mgr);
        }
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(ERROR, "failed to set schema, err=%d", err);
        }
      }

      return err;
    }
    int ObUpdateServer::slave_register_followed(const ObServer &master, ObUpsFetchParam & fetch_param, uint64_t &max_log_seq)
    {
      int err = OB_SUCCESS;
      const ObServer& self_addr = get_self();
      ObSlaveInfo slave_info;
      slave_info.self = self_addr;
      slave_info.min_sstable_id = sstable_mgr_.get_min_sstable_id();
      slave_info.max_sstable_id = sstable_mgr_.get_max_sstable_id();
      int64_t timeout = param_.get_register_timeout_us();
      err = ups_rpc_stub_.slave_register_followed(master, slave_info, slave_type_, fetch_param, max_log_seq, timeout);
      if (OB_SUCCESS != err && OB_ALREADY_REGISTERED != err)
      {
        TBSYS_LOG(WARN, "fail to register to master ups[%s]. err=%d", master.to_cstring(), err);
      }
      return err;
    }

    //int ObUpdateServer::slave_register_followed(const ObServer &master, ObUpsFetchParam& fetch_param)
    //{
    //  int err = OB_SUCCESS;
    //  int64_t num_times = INT64_MAX;
    //  int64_t timeout = param_.get_register_timeout_us();
    //  const ObServer& self_addr = get_self();

    //  ObSlaveInfo slave_info;
    //  slave_info.self = self_addr;
    //  slave_info.min_sstable_id = sstable_mgr_.get_min_sstable_id();
    //  slave_info.max_sstable_id = sstable_mgr_.get_max_sstable_id();

    //  err = RPC_CALL_WITH_RETRY(slave_register_followed, num_times, timeout, master, slave_info, fetch_param);

    //  if (ObUpsRoleMgr::STOP == role_mgr_.get_state())
    //  {
    //    TBSYS_LOG(INFO, "The Update Server Slave is stopped manually.");
    //    err = OB_ERROR;
    //  }
    //  else if (OB_RESPONSE_TIME_OUT == err)
    //  {
    //    TBSYS_LOG(WARN, "slave register timeout, num_times=%d, timeout=%ldus",
    //        num_times, timeout);
    //    err = OB_RESPONSE_TIME_OUT;
    //  }
    //  else if (OB_SUCCESS != err)
    //  {
    //    TBSYS_LOG(WARN, "Error occurs when slave register, err=%d", err);
    //  }

    //  char addr_buf[ADDR_BUF_LEN];
    //  self_addr.to_string(addr_buf, sizeof(addr_buf));
    //  addr_buf[ADDR_BUF_LEN - 1] = '\0';

    //  if (OB_SUCCESS == err)
    //  {
    //    TBSYS_LOG(INFO, "slave register succ, self=[%s], min_log_id=%ld, max_log_id=%ld, err=%d",
    //        addr_buf, fetch_param.min_log_id_, fetch_param.max_log_id_, err);
    //    is_registered_to_ups_ = true;
    //  }

    //  return err;
    //}

    int ObUpdateServer::slave_register_standalone(uint64_t &log_id_start, uint64_t &log_seq_start)
    {
      int err = OB_SUCCESS;
      int64_t num_times = INT64_MAX;
      int64_t timeout = param_.get_register_timeout_us();

      log_seq_start = log_mgr_.get_cur_log_seq() == 0 ? 0 : log_mgr_.get_cur_log_seq();
      log_id_start = log_seq_start == 0 ? 0 : log_mgr_.get_max_log_id();
      if (OB_SUCCESS != (err = refresh_lsync_server_addr()))
      {
        TBSYS_LOG(ERROR, "refresh_lsync_server_addr()=>%d", err);
      }
      if (OB_RESPONSE_TIME_OUT == err)
      {
        err = OB_SUCCESS;
      }

      if (OB_SUCCESS == err)
      {
        err = RPC_CALL_WITH_RETRY(slave_register_standalone, num_times, timeout, get_lsync_server(),
            log_id_start, log_seq_start, log_id_start, log_seq_start);
      }
      if (OB_SUCCESS == err)
      {
        TBSYS_LOG(INFO, "slave_register_standalone(%s): SUCCESS", get_lsync_server().to_cstring());
      }

      if (ObUpsRoleMgr::STOP == role_mgr_.get_state())
      {
        TBSYS_LOG(INFO, "The Update Server Slave is stopped manually.");
        err = OB_ERROR;
      }
      else if (OB_RESPONSE_TIME_OUT == err)
      {
        TBSYS_LOG(WARN, "slave register timeout, num_times=%ld, timeout=%ldus",
            num_times, timeout);
        err = OB_RESPONSE_TIME_OUT;
      }
      else if (OB_SUCCESS != err)
      {
        TBSYS_LOG(WARN, "Error occurs when slave register, err=%d", err);
      }

      char addr_buf[ADDR_BUF_LEN];
      get_lsync_server().to_string(addr_buf, sizeof(addr_buf));
      addr_buf[ADDR_BUF_LEN - 1] = '\0';
      TBSYS_LOG(INFO, "slave register, lsync_server=[%s], log_id_start=%ld, log_seq_start=%ld, err=%d",
          addr_buf, log_id_start, log_seq_start, err);

      return err;
    }

    tbnet::IPacketHandler::HPRetCode ObUpdateServer::handlePacket(tbnet::Connection *connection, tbnet::Packet *packet)
    {
      tbnet::IPacketHandler::HPRetCode rc = tbnet::IPacketHandler::FREE_CHANNEL;
      if (!packet->isRegularPacket())
      {
        TBSYS_LOG(WARN, "control packet, packet code: %d", ((tbnet::ControlPacket*)packet)->getCommand());
      }
      else
      {
        ObPacket* req = (ObPacket*) packet;
        req->set_connection(connection);
        bool ps = true;
        int packet_code = req->get_packet_code();
        int32_t priority = req->get_packet_priority();
        TBSYS_LOG(DEBUG,"get packet code is %d, priority=%d", packet_code, priority);
        switch (packet_code)
        {
          case OB_SET_OBI_ROLE:
            ps = read_thread_queue_.push(req, read_task_queue_size_, false,
                (NORMAL_PRI == priority)
                ? PriorityPacketQueueThread::NORMAL_PRIV
                : PriorityPacketQueueThread::LOW_PRIV);
            break;
          case OB_UPS_CLEAR_FATAL_STATUS:
            ps = lease_thread_queue_.push(req, lease_task_queue_size_, false);
            break;
          case OB_SEND_LOG:
            if ((ObUpsRoleMgr::SLAVE == role_mgr_.get_role()
                  || ObiRole::SLAVE == obi_role_.get_role())
                && ObUpsRoleMgr::FATAL != role_mgr_.get_state())
            {
              ps = write_thread_queue_.push(req, log_task_queue_size_, false);
            }
            else
            {
              ps = false;
            }
            break;
          case OB_FETCH_LOG:
            ps = read_thread_queue_.push(req, read_task_queue_size_, false,
                (NORMAL_PRI == priority)
                ? PriorityPacketQueueThread::NORMAL_PRIV
                : PriorityPacketQueueThread::LOW_PRIV);
            break;
          case OB_FILL_LOG_CURSOR:
            ps = read_thread_queue_.push(req, read_task_queue_size_, false,
                (NORMAL_PRI == priority)
                ? PriorityPacketQueueThread::NORMAL_PRIV
                : PriorityPacketQueueThread::LOW_PRIV);
            break;
          case OB_GET_CLOG_STATUS:
            ps = read_thread_queue_.push(req, read_task_queue_size_, false,
                (NORMAL_PRI == priority)
                ? PriorityPacketQueueThread::NORMAL_PRIV
                : PriorityPacketQueueThread::LOW_PRIV);
            break;
          case OB_WRITE:
          case OB_INTERNAL_WRITE:
          case OB_MS_MUTATE:
          case OB_FAKE_WRITE_FOR_KEEP_ALIVE:
            if (ObUpsRoleMgr::FATAL != role_mgr_.get_state())
            {
              if (ObiRole::MASTER != obi_role_.get_role()
                  || ObUpsRoleMgr::MASTER != role_mgr_.get_role())
              {
                TBSYS_LOG(WARN, "ups not master.obi_role=%s, role=%s", obi_role_.get_role_str(), role_mgr_.get_role_str());
                response_result_(OB_NOT_MASTER, OB_WRITE_RES, 1, connection, packet->getChannelId());
                ps = false;
              }
              else if (ObUpsRoleMgr::ACTIVE != role_mgr_.get_state())
              {
                TBSYS_LOG(WARN, "master ups state not ACTIVE. refuse write. role_state=%s", role_mgr_.get_state_str());
                response_result_(OB_NOT_MASTER, OB_WRITE_RES, 1, connection, packet->getChannelId());
                ps = false;
              }
              else if (!is_lease_valid())
              {
                TBSYS_LOG(WARN, "master ups lease is nearly to timeout. refuse write.");
                response_result_(OB_NOT_MASTER, OB_WRITE_RES, 1, connection, packet->getChannelId());
                ps = false;
              }
              else if (OB_INTERNAL_WRITE == packet_code || OB_FAKE_WRITE_FOR_KEEP_ALIVE == packet_code)
              {
                ps = write_thread_queue_.push(req, write_task_queue_size_, false);
              }
              else
              {
                if (0 == param_.get_pre_process())
                {
                  ps = write_thread_queue_.push(req, write_task_queue_size_, false);
                }
                else
                {
                  ps = preprocess_thread_queue_.push(req, preprocess_task_queue_size_, false);
                }
              }
            }
            else
            {
              TBSYS_LOG(DEBUG, "ups state is FATAL, refuse write. state=%s", role_mgr_.get_state_str());
              response_result_(OB_NOT_MASTER, OB_WRITE_RES, 1, connection, packet->getChannelId());
              ps = false;
            }
            break;
          case OB_SLAVE_REG:
            if (ObUpsRoleMgr::MASTER == role_mgr_.get_role())
            {
              ps = write_thread_queue_.push(req, write_task_queue_size_, false);
            }
            else
            {
              response_result_(OB_NOT_MASTER, OB_SLAVE_REG_RES, 1, connection, packet->getChannelId());
              ps = false;
            }
            break;
          case OB_FREEZE_MEM_TABLE:
          case OB_UPS_MINOR_FREEZE_MEMTABLE:
          case OB_UPS_MINOR_LOAD_BYPASS:
          case OB_UPS_MAJOR_LOAD_BYPASS:
          case OB_UPS_CLEAR_ACTIVE_MEMTABLE:
          case OB_SWITCH_SCHEMA:
          case OB_UPS_FORCE_FETCH_SCHEMA:
          case OB_UPS_SWITCH_COMMIT_LOG:
          case OB_UPS_ASYNC_MAJOR_FREEZE_MEMTABLE:
          case OB_UPS_ASYNC_CHECK_CUR_VERSION:
            if (ObUpsRoleMgr::MASTER == role_mgr_.get_role()
                && ObiRole::MASTER == obi_role_.get_role()
                && ObUpsRoleMgr::FATAL != role_mgr_.get_state())
            {
              ps = write_thread_queue_.push(req, write_task_queue_size_, false);
            }
            else
            {
              ps = false;
            }
            break;
          case OB_GET_REQUEST:
          case OB_SCAN_REQUEST:
            if (!get_service_state())
            {
              ps = false;
            }
            else
            {
              ps = read_thread_queue_.push(req, read_task_queue_size_, false,
                  (NORMAL_PRI == priority)
                  ? PriorityPacketQueueThread::NORMAL_PRIV
                  : PriorityPacketQueueThread::LOW_PRIV);
            }
            break;
          case OB_UPS_GET_BLOOM_FILTER:
          case OB_UPS_DUMP_TEXT_MEMTABLE:
          case OB_UPS_DUMP_TEXT_SCHEMAS:
          case OB_UPS_MEMORY_WATCH:
          case OB_UPS_MEMORY_LIMIT_SET:
          case OB_UPS_PRIV_QUEUE_CONF_SET:
          case OB_UPS_RELOAD_CONF:
          case OB_UPS_GET_LAST_FROZEN_VERSION:
          case OB_UPS_GET_TABLE_TIME_STAMP:
          case OB_UPS_ENABLE_MEMTABLE_CHECKSUM:
          case OB_UPS_DISABLE_MEMTABLE_CHECKSUM:
          case OB_FETCH_STATS:
          case OB_FETCH_SCHEMA:
          case OB_RS_FETCH_SPLIT_RANGE:
          case OB_UPS_STORE_MEM_TABLE:
          case OB_UPS_DROP_MEM_TABLE:
          case OB_UPS_ASYNC_FORCE_DROP_MEMTABLE:
          case OB_UPS_DELAY_DROP_MEMTABLE:
          case OB_UPS_IMMEDIATELY_DROP_MEMTABLE:
          case OB_UPS_ASYNC_LOAD_BYPASS:
          case OB_UPS_ERASE_SSTABLE:
          case OB_UPS_LOAD_NEW_STORE:
          case OB_UPS_RELOAD_ALL_STORE:
          case OB_UPS_RELOAD_STORE:
          case OB_UPS_UMOUNT_STORE:
          case OB_UPS_FORCE_REPORT_FROZEN_VERSION:
          case OB_GET_CLOG_CURSOR:
          case OB_GET_CLOG_MASTER:
          case OB_GET_LOG_SYNC_DELAY_STAT:
          case OB_RS_GET_MAX_LOG_SEQ:
            ps = read_thread_queue_.push(req, read_task_queue_size_, false,
                (NORMAL_PRI == priority)
                ? PriorityPacketQueueThread::NORMAL_PRIV
                : PriorityPacketQueueThread::LOW_PRIV);
            break;
          case OB_SLAVE_QUIT:
          case OB_UPS_GET_SLAVE_INFO:
            if (ObUpsRoleMgr::MASTER != role_mgr_.get_role())
            {
              TBSYS_LOG(WARN, "server is not master, refuse to get slave info");
              response_result_(OB_NOT_MASTER, OB_UPS_GET_SLAVE_INFO_RESPONSE, 1, connection, packet->getChannelId());
              ps = false;
            }
            else
            {
              ps = read_thread_queue_.push(req, read_task_queue_size_, false,
                  (NORMAL_PRI == priority)
                  ? PriorityPacketQueueThread::NORMAL_PRIV
                  : PriorityPacketQueueThread::LOW_PRIV);
            }
            break;
            //master send to slave, to keep alive
          case OB_UPS_KEEP_ALIVE:
            TBSYS_LOG(WARN, "not need receive keep_alive anymore");
            ps = false;
            break;
          case OB_RS_UPS_REVOKE_LEASE:
            if (ObUpsRoleMgr::MASTER == role_mgr_.get_role())
            {
              ps = lease_thread_queue_.push(req, lease_task_queue_size_, false);
            }
            else
            {
              TBSYS_LOG(WARN, "server not master, refuse to revoke lease.");
              response_result_(OB_NOT_MASTER, OB_RENEW_LEASE_RESPONSE, 1, connection, packet->getChannelId());

              ps = false;
            }
            break;
          case OB_SET_SYNC_LIMIT_REQUEST:
          case OB_PING_REQUEST:
            //case OB_UPS_CHANGE_VIP_REQUEST:
            if (ObUpsRoleMgr::MASTER == role_mgr_.get_role())
            {
              ps = lease_thread_queue_.push(req, lease_task_queue_size_, false);
            }
            else if (ObUpsRoleMgr::SLAVE == role_mgr_.get_role())
            {
              ps = lease_thread_queue_.push(req, log_task_queue_size_, false);
            }
            else
            {
              ps = false;
            }
            break;
          case OB_RS_UPS_HEARTBEAT:
            if (ObUpsRoleMgr::FATAL != role_mgr_.get_state())
            {
            ps = lease_thread_queue_.push(req, lease_task_queue_size_, false);
            }
            else
            {
              TBSYS_LOG(DEBUG, "UPS become FATAL, refuse to receive heartbeat from rs");
            }
            break;
          case OB_CHANGE_LOG_LEVEL:
              ps = read_thread_queue_.push(req, read_task_queue_size_, false);
              break;
          case OB_STOP_SERVER:
              ps = read_thread_queue_.push(req, read_task_queue_size_, false);
              break;
          default:
              TBSYS_LOG(ERROR, "UNKNOWN packet %d, ignore this", packet_code);
              ps = false;
              break;
        }
        if (!ps)
        {
          TBSYS_LOG(WARN, "packet %d can not be distribute to queue", packet_code);
          rc = tbnet::IPacketHandler::KEEP_CHANNEL;
          INC_STAT_INFO(UPS_STAT_TBSYS_DROP_COUNT, 1);
        }
      }
      return rc;
    }
    bool ObUpdateServer::handleBatchPacket(tbnet::Connection *connection, tbnet::PacketQueue &packetQueue)
    {
      UNUSED(connection);
      UNUSED(packetQueue);
      TBSYS_LOG(ERROR, "you should not reach this, not supported");
      return true;
    }
    bool ObUpdateServer::handlePacketQueue(tbnet::Packet *packet, void *args)
    {
      UNUSED(args);
      bool ret = true;
      int return_code = OB_SUCCESS;

      ObPacket* ob_packet = reinterpret_cast<ObPacket*>(packet);
      int packet_code = ob_packet->get_packet_code();
      int version = ob_packet->get_api_version();
      int32_t priority = ob_packet->get_packet_priority();
      return_code = ob_packet->deserialize();
      uint32_t channel_id = ob_packet->getChannelId();//tbnet need this
      if (OB_SUCCESS != return_code)
      {
        TBSYS_LOG(ERROR, "packet deserialize error packet code is %d", packet_code);
      }
      else
      {
        int64_t packet_timewait = (0 == ob_packet->get_source_timeout()) ?
          param_.get_packet_max_timewait() : ob_packet->get_source_timeout();
        ObDataBuffer* in_buf = ob_packet->get_buffer();
        in_buf->get_limit() = in_buf->get_position() + ob_packet->get_data_length();
        if ((ob_packet->get_receive_ts() + packet_timewait) < tbsys::CTimeUtil::getTime())
        {
          INC_STAT_INFO(UPS_STAT_PACKET_LONG_WAIT_COUNT, 1);
          TBSYS_LOG(WARN, "packet wait too long time, receive_time=%ld cur_time=%ld packet_max_timewait=%ld packet_code=%d "
              "priority=%d last_log_network_elapse=%ld last_log_disk_elapse=%ld "
              "read_task_queue_size=%zu write_task_queue_size=%zu lease_task_queue_size=%zu log_task_queue_size=%zu",
              ob_packet->get_receive_ts(), tbsys::CTimeUtil::getTime(), packet_timewait, packet_code, priority,
              log_mgr_.get_last_net_elapse(), log_mgr_.get_last_disk_elapse(),
              read_thread_queue_.size(), write_thread_queue_.size(), lease_thread_queue_.size(), log_thread_queue_.size());
        }
        else if (in_buf == NULL)
        {
          TBSYS_LOG(ERROR, "in_buff is NUll should not reach this");
        }
        else
        {
          packet_timewait -= DEFAULT_REQUEST_TIMEOUT_RESERVE;
          tbnet::Connection* conn = ob_packet->get_connection();
          ThreadSpecificBuffer::Buffer* my_buffer = my_thread_buffer_.get_buffer();
          if (my_buffer != NULL)
          {
            my_buffer->reset();
            ObDataBuffer thread_buff(my_buffer->current(), my_buffer->remain());
            TBSYS_LOG(DEBUG, "handle packet, packe code is %d", packet_code);
            switch(packet_code)
            {
              case OB_SET_OBI_ROLE:
                return_code = set_obi_role(version, *in_buf, conn, channel_id, thread_buff);
                break;
              case OB_FETCH_LOG:
                return_code = ups_fetch_log_for_slave(version, *in_buf, conn, channel_id, thread_buff, ob_packet);
                break;
              case OB_FILL_LOG_CURSOR:
                return_code = ups_fill_log_cursor_for_slave(version, *in_buf, conn, channel_id, thread_buff);
                break;
              case OB_GET_CLOG_STATUS:
                return_code = ups_get_clog_status(version, *in_buf, conn, channel_id, thread_buff);
                break;
              case OB_SEND_LOG:
                return_code = ups_slave_write_log(version, *in_buf, conn, channel_id, thread_buff);
                break;
              case OB_SET_SYNC_LIMIT_REQUEST:
                return_code = ups_set_sync_limit(version, *in_buf, conn, channel_id, thread_buff);
                break;
              case OB_PING_REQUEST:
                return_code = ups_ping(version, conn, channel_id);
                break;
              case OB_LOGIN:
                return_code = ob_login(version, *in_buf, conn, channel_id, thread_buff);
                break;
              case OB_UPS_ASYNC_SWITCH_SKEY:
                return_code = ups_switch_skey();
                break;
              case OB_GET_CLOG_CURSOR:
                return_code = ups_get_clog_cursor(version, conn, channel_id, thread_buff);
                break;
              case OB_GET_CLOG_MASTER:
                return_code = ups_get_clog_master(version, conn, channel_id, thread_buff);
                break;
              case OB_GET_LOG_SYNC_DELAY_STAT:
                return_code = ups_get_log_sync_delay_stat(version, conn, channel_id, thread_buff);
                break;
              case OB_GET_REQUEST:
                CLEAR_TRACE_LOG();
                FILL_TRACE_LOG("start handle get, packet wait=%ld start_time=%ld timeout=%ld src=%s priority=%d",
                              tbsys::CTimeUtil::getTime() - ob_packet->get_receive_ts(),
                              ob_packet->get_receive_ts(), packet_timewait, NULL == conn ? NULL : inet_ntoa_r(conn->getPeerId()), priority);
                return_code = ups_get(version, *in_buf, conn, channel_id, thread_buff, ob_packet->get_receive_ts(), packet_timewait, priority);
                break;
              case OB_SCAN_REQUEST:
                CLEAR_TRACE_LOG();
                FILL_TRACE_LOG("start handle scan, packet wait=%ld start_time=%ld timeout=%ld src=%s priority=%d",
                              tbsys::CTimeUtil::getTime() - ob_packet->get_receive_ts(),
                              ob_packet->get_receive_ts(), packet_timewait, NULL == conn ? NULL : inet_ntoa_r(conn->getPeerId()), priority);
                return_code = ups_scan(version, *in_buf, conn, channel_id, thread_buff, ob_packet->get_receive_ts(), packet_timewait, priority);
                break;
              case OB_WRITE:
              case OB_MS_MUTATE:
                CLEAR_TRACE_LOG();
                FILL_TRACE_LOG("start preprocess write, packet wait=%ld, start_time=%ld, timeout=%ld, src=%s, packet_code=%d",
                    tbsys::CTimeUtil::getTime() - ob_packet->get_receive_ts(),
                    ob_packet->get_receive_ts(), packet_timewait, NULL == conn ? NULL : inet_ntoa_r(conn->getPeerId()), packet_code);
                return_code = ups_preprocess(version, packet_code, *in_buf, conn, channel_id, thread_buff, ob_packet->get_receive_ts(), packet_timewait);

                break;
              case OB_UPS_GET_BLOOM_FILTER:
                return_code = ups_get_bloomfilter(version, *in_buf, conn, channel_id, thread_buff);
                break;
              case OB_CREATE_MEMTABLE_INDEX:
                return_code = ups_create_memtable_index();
                break;
              case OB_UPS_DUMP_TEXT_MEMTABLE:
                return_code = ups_dump_text_memtable(version, *in_buf, conn, channel_id);
                break;
              case OB_UPS_DUMP_TEXT_SCHEMAS:
                return_code = ups_dump_text_schemas(version, conn, channel_id);
                break;
              case OB_UPS_MEMORY_WATCH:
                return_code = ups_memory_watch(version, conn, channel_id, thread_buff);
                break;
              case OB_UPS_MEMORY_LIMIT_SET:
                return_code = ups_memory_limit_set(version, *in_buf, conn, channel_id, thread_buff);
                break;
              case OB_UPS_PRIV_QUEUE_CONF_SET:
                return_code = ups_priv_queue_conf_set(version, *in_buf, conn, channel_id, thread_buff);
                break;
              case OB_UPS_RELOAD_CONF:
                return_code = ups_reload_conf(version, *in_buf, conn, channel_id);
                break;
              case OB_SLAVE_QUIT:
                return_code = ups_slave_quit(version, *in_buf, conn, channel_id, thread_buff);
                break;
              //case OB_RENEW_LEASE_REQUEST:
              //  return_code = ups_renew_lease(version, *in_buf, conn, channel_id, thread_buff);
              //  break;
              //case OB_GRANT_LEASE_REQUEST:
              //  return_code = ups_grant_lease(version, *in_buf, conn, channel_id, thread_buff);
              //  break;
                //add ：心跳处理函数
              case OB_RS_UPS_HEARTBEAT:
                return_code = ups_rs_lease(version, *in_buf, conn, channel_id, thread_buff);
                break;
              case OB_RS_UPS_REVOKE_LEASE:
                return_code = ups_rs_revoke_lease(version, *in_buf, conn, channel_id, thread_buff);
                break;
              case OB_UPS_GET_LAST_FROZEN_VERSION:
                return_code = ups_get_last_frozen_version(version, conn, channel_id, thread_buff);
                break;
              case OB_UPS_GET_TABLE_TIME_STAMP:
                return_code = ups_get_table_time_stamp(version, *in_buf, conn, channel_id, thread_buff);
                break;
              case OB_UPS_GET_SLAVE_INFO:
                return_code = ups_get_slave_info(version, conn, channel_id, thread_buff);
                break;
              case OB_UPS_ENABLE_MEMTABLE_CHECKSUM:
                return_code = ups_enable_memtable_checksum(version, conn, channel_id);
                break;
              case OB_UPS_DISABLE_MEMTABLE_CHECKSUM:
                return_code = ups_disable_memtable_checksum(version, conn, channel_id);
                break;
              case OB_FETCH_STATS:
                return_code = ups_fetch_stat_info(version, conn, channel_id, thread_buff);
                break;
              case OB_FETCH_SCHEMA:
                return_code = ups_get_schema(version, *in_buf, conn, channel_id, thread_buff);
                break;
              case OB_RS_FETCH_SPLIT_RANGE:
                return_code = ups_get_sstable_range_list(version, *in_buf, conn, channel_id, thread_buff);
                break;
                /* case OB_UPS_CHANGE_VIP_REQUEST:
                   return_code = ups_change_vip(version, *in_buf, conn, channel_id);
                   break; */
              case OB_UPS_STORE_MEM_TABLE:
                return_code = ups_store_memtable(version, *in_buf, conn, channel_id);
                break;
              case OB_UPS_DROP_MEM_TABLE:
                return_code = ups_drop_memtable(version, conn, channel_id);
                break;
              case OB_UPS_DELAY_DROP_MEMTABLE:
                return_code = ups_delay_drop_memtable(version, conn, channel_id);
                break;
              case OB_UPS_IMMEDIATELY_DROP_MEMTABLE:
                return_code = ups_immediately_drop_memtable(version, conn, channel_id);
                break;
              case OB_UPS_ASYNC_FORCE_DROP_MEMTABLE:
                return_code = ups_drop_memtable();
                break;
              case OB_UPS_ASYNC_LOAD_BYPASS:
                return_code = ups_load_bypass(version, conn, channel_id, thread_buff, packet_code);
                break;
              case OB_UPS_ERASE_SSTABLE:
                return_code = ups_erase_sstable(version, conn, channel_id);
                break;
              case OB_UPS_ASYNC_HANDLE_FROZEN:
                return_code = ups_handle_frozen();
                break;
              case OB_UPS_ASYNC_REPORT_FREEZE:
                return_code = report_frozen_version_();
                break;
              case OB_UPS_ASYNC_REPLAY_LOG:
                return_code = replay_commit_log_();
                break;
              case OB_PREFETCH_LOG:
                return_code = prefetch_remote_log_(*in_buf);
                break;
              case OB_UPS_ASYNC_CHECK_KEEP_ALIVE:
                return_code = check_keep_alive_();
                break;
              case OB_UPS_ASYNC_GRANT_KEEP_ALIVE:
                return_code = grant_keep_alive_();
                break;
              case OB_UPS_ASYNC_CHECK_LEASE:
                return_code = check_lease_();
                break;
              case OB_UPS_LOAD_NEW_STORE:
                return_code = ups_load_new_store(version, conn, channel_id);
                break;
              case OB_UPS_RELOAD_ALL_STORE:
                return_code = ups_reload_all_store(version, conn, channel_id);
                break;
              case OB_UPS_RELOAD_STORE:
                return_code = ups_reload_store(version, *in_buf, conn, channel_id);
                break;
              case OB_UPS_UMOUNT_STORE:
                return_code = ups_umount_store(version, *in_buf, conn, channel_id);
                break;
              case OB_UPS_FORCE_REPORT_FROZEN_VERSION:
                return_code = ups_froce_report_frozen_version(version, conn, channel_id);
                break;
              case OB_UPS_KEEP_ALIVE:
                return_code = slave_ups_receive_keep_alive(version, conn, channel_id);
                break;
              case OB_UPS_CLEAR_FATAL_STATUS:
                return_code = ups_clear_fatal_status(version, conn, channel_id);
                break;
              case OB_RS_GET_MAX_LOG_SEQ:
                return_code = ups_rs_get_max_log_seq(version, conn, channel_id, thread_buff);
                break;
              case OB_CHANGE_LOG_LEVEL:
                return_code = ups_change_log_level(version, *in_buf, conn, channel_id);
                break;
              case OB_STOP_SERVER:
                return_code = ups_stop_server(version, *in_buf, conn, channel_id);
                break;
              default:
                return_code = OB_ERROR;
                break;
            }

            if (OB_SUCCESS != return_code)
            {
              TBSYS_LOG(WARN, "call func error packet_code is %d return code is %d", packet_code, return_code);
            }
          }
          else
          {
            TBSYS_LOG(ERROR, "get thread buffer error, ignore this packet");
          }
        }
      }
      return ret;//if return true packet will be deleted.
    }

    int ObUpdateServer::ups_preprocess(const int32_t version, const int32_t packet_code, common::ObDataBuffer& in_buff,
        tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff,
        const int64_t start_time, const int64_t timeout)
    {
      UNUSED(out_buff);

      int ret = OB_SUCCESS;
      if (version != MY_VERSION)
      {
        ret = OB_ERROR_FUNC_VERSION;
      }

      ObMutator *mutator_ptr = GET_TSI_MULT(ObMutator, TSI_UPS_MUTATOR_1);
      ObToken token;
      ObToken *token_ptr = NULL;

      if (NULL == mutator_ptr)
      {
        TBSYS_LOG(WARN, "GET_TSI ObMutator or ObScanner fail");
        ret = OB_ERROR;
      }

      if (OB_SUCCESS == ret)
      {
        ret = login_mgr_.check_token(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position(), token);
        if (OB_SUCCESS == ret)
        {
          token_ptr = &token;
        }
        if (OB_NOT_A_TOKEN == ret)
        {
          ret = param_.get_allow_write_without_token() ? OB_SUCCESS : OB_NO_PERMISSION;
        }
      }

      ObDataBuffer ori_in_buff = in_buff;
      if (OB_SUCCESS == ret)
      {
        ret = mutator_ptr->deserialize(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position());
        FILL_TRACE_LOG("mutator deserialize ret=%d buff_size=%d buff_pos=%d", ret, in_buff.get_capacity(), in_buff.get_position());
      }

      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "deserialize token or mutator fail ret=%d", ret);
      }
      else
      {
        ret = table_mgr_.pre_process(OB_MS_MUTATE == packet_code, *mutator_ptr, token_ptr);
        if (OB_SUCCESS == ret)
        {
          int64_t apply_timeout = timeout - (tbsys::CTimeUtil::getTime() - start_time);
          ThreadSpecificBuffer::Buffer *my_buffer = my_thread_buffer_.get_buffer();
          if (apply_timeout <= 0)
          {
            TBSYS_LOG(WARN, "response timeout, timeout=%ld, start_time=%ld, now=%ld",
                timeout, start_time, tbsys::CTimeUtil::getTime());
            ret = OB_RESPONSE_TIME_OUT;
          }
          else if (NULL == my_buffer)
          {
            TBSYS_LOG(ERROR, "alloc thread buffer fail");
            ret = OB_MEM_OVERFLOW;
          }
          else
          {
            ObDataBuffer mutator_buff(my_buffer->current(), my_buffer->remain());
            if (OB_SUCCESS != (ret = mutator_ptr->serialize(mutator_buff.get_data(), mutator_buff.get_capacity(),
                                                            mutator_buff.get_position())))
            {
              TBSYS_LOG(WARN, "failed to serialize mutator, ret=%d", ret);
            }
            else if (OB_SUCCESS != (ret = submit_async_task_((PacketCode)OB_MS_MUTATE, write_thread_queue_, write_task_queue_size_,
                                                            version, mutator_buff, conn, channel_id, apply_timeout)))
            {
              TBSYS_LOG(WARN, "failed to add apply task, ret=%d", ret);
            }
          }
        }
      }

      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "failed to pre process, ret=%d src=%s", ret, NULL == conn ? NULL : inet_ntoa_r(conn->getPeerId()));
        int res_code = (OB_MS_MUTATE == packet_code) ? OB_MS_MUTATE_RESPONSE : OB_WRITE_RES;
        int tmp_ret = response_result_(ret, res_code, MY_VERSION, conn, channel_id);
        if (OB_SUCCESS != tmp_ret)
        {
          TBSYS_LOG(WARN, "failed to send response, tmp_ret=%d", tmp_ret);
          ret = OB_ERROR;
        }
        // TODO (rizhao.ych) modify stat info
      }
      FILL_TRACE_LOG("preprocess ret=%d src=%s", ret, NULL == conn ? NULL : inet_ntoa_r(conn->getPeerId()));
      PRINT_TRACE_LOG();

      return ret;
    }

    bool ObUpdateServer::handleBatchPacketQueue(const int64_t batch_num, tbnet::Packet** packets, void *args)
    {
      UNUSED(args);
      enum __trans_status__
      {
        TRANS_NOT_START = 0,
        TRANS_STARTED = 1,
      };
      bool ret = true;
      int return_code = OB_SUCCESS;
      int64_t trans_status = TRANS_NOT_START;
      int64_t first_trans_idx = 0;
      UpsTableMgrTransHandle handle;
      ObPacket packet_repl;
      ObPacket* ob_packet = &packet_repl;
      ScannerArray *scanner_array = GET_TSI_MULT(ScannerArray, TSI_UPS_SCANNER_ARRAY_1);
      // 判断是否是master_master依然有风险 master slave -> slave master
      bool is_master_master = ObUpsRoleMgr::MASTER == role_mgr_.get_role() && ObiRole::MASTER == obi_role_.get_role();
      if (is_master_master && !is_lease_valid())
      {
        return_code = OB_NOT_MASTER;
        TBSYS_LOG(WARN, "is master_master But lease is invalid");
      }
      if (NULL == scanner_array)
      {
        TBSYS_LOG(WARN, "get tsi scanner_array fail");
        return_code = OB_ERROR;
      }
      for (int64_t i = 0; OB_SUCCESS == return_code && i < batch_num; ++i)
      {
        ObPacket* packet_orig = reinterpret_cast<ObPacket*>(packets[i]);
        packet_repl = *packet_orig;
        int packet_code = ob_packet->get_packet_code();
        int version = ob_packet->get_api_version();
        return_code = ob_packet->deserialize();
        uint32_t channel_id = ob_packet->getChannelId();//tbnet need this
        //TBSYS_LOG(DEBUG, "packet i=%ld batch_num=%ld %s", i, batch_num, ob_packet->print_self());
        if (OB_SUCCESS != return_code)
        {
          TBSYS_LOG(ERROR, "packet deserialize error packet code is %d", packet_code);
        }
        else
        {
          int64_t packet_timewait = (0 == ob_packet->get_source_timeout()) ?
            param_.get_packet_max_timewait() : ob_packet->get_source_timeout();
          ObDataBuffer* in_buf = ob_packet->get_buffer();
          in_buf->get_limit() = in_buf->get_position() + ob_packet->get_data_length();
          if ((ob_packet->get_receive_ts() + packet_timewait)< tbsys::CTimeUtil::getTime())
          {
            INC_STAT_INFO(UPS_STAT_PACKET_LONG_WAIT_COUNT, 1);
            TBSYS_LOG(WARN, "packet wait too long time, receive_time=%ld cur_time=%ld packet_max_timewait=%ld packet_code=%d "
                "read_task_queue_size=%zu write_task_queue_size=%zu lease_task_queue_size=%zu log_task_queue_size=%zu",
                ob_packet->get_receive_ts(), tbsys::CTimeUtil::getTime(), packet_timewait, packet_code,
                read_thread_queue_.size(), write_thread_queue_.size(), lease_thread_queue_.size(), log_thread_queue_.size());
            return_code = OB_RESPONSE_TIME_OUT;
          }
          else if (in_buf == NULL)
          {
            TBSYS_LOG(ERROR, "in_buff is NUll should not reach this");
            return_code = OB_ERROR;
          }
          else
          {
            tbnet::Connection* conn = ob_packet->get_connection();
            ThreadSpecificBuffer::Buffer* my_buffer = my_thread_buffer_.get_buffer();
            if (my_buffer == NULL)
            {
              TBSYS_LOG(ERROR, "get thread buffer error, ignore this packet");
              return_code = OB_ERROR;
            }
            else
            {
              my_buffer->reset();
              ObDataBuffer thread_buff(my_buffer->current(), my_buffer->remain());
              TBSYS_LOG(DEBUG, "handle packet, packe code is %d", packet_code);
              if (!is_master_master)
              {
                if (OB_SEND_LOG == packet_code)
                {
                  return_code = ups_slave_write_log(version, *in_buf, conn, channel_id, thread_buff);
                }
                else if (OB_SLAVE_REG == packet_code)
                {
                  return_code = ups_slave_register(version, *in_buf, conn, channel_id, thread_buff);
                }
                else
                {
                  return_code = return_not_master(version, conn, channel_id, packet_code);
                }
              }
              else
              {
                switch (packet_code)
                {
                  case OB_FREEZE_MEM_TABLE:
                  case OB_UPS_MINOR_FREEZE_MEMTABLE:
                  case OB_UPS_MINOR_LOAD_BYPASS:
                  case OB_UPS_MAJOR_LOAD_BYPASS:
                  case OB_UPS_ASYNC_MAJOR_FREEZE_MEMTABLE:
                  case OB_SLAVE_REG:
                  case OB_UPS_CLEAR_ACTIVE_MEMTABLE:
                  case OB_SWITCH_SCHEMA:
                  case OB_UPS_FORCE_FETCH_SCHEMA:
                  case OB_UPS_SWITCH_COMMIT_LOG:
                  case OB_UPS_ASYNC_CHECK_CUR_VERSION:
                  case OB_FAKE_WRITE_FOR_KEEP_ALIVE:
                    if (TRANS_STARTED == trans_status)
                    {
                      return_code = ups_end_transaction(packets, *scanner_array, first_trans_idx, i-1, handle, OB_SUCCESS);
                      trans_status = TRANS_NOT_START;
                      if (OB_SUCCESS != return_code)
                      {
                        TBSYS_LOG(WARN, "failed to end transaction, err=%d", return_code);
                        break;
                      }
                    }
                    break;
                  case OB_SEND_LOG:
                  case OB_WRITE:
                  case OB_INTERNAL_WRITE:
                  case OB_MS_MUTATE:
                    break;
                  default:
                    TBSYS_LOG(WARN, "unexpected packet_code %d", packet_code);
                    return_code = OB_ERR_UNEXPECTED;
                    break;
                }
                if (OB_SUCCESS == return_code)
                {
                  switch (packet_code)
                  {
                    case OB_FREEZE_MEM_TABLE:
                    case OB_UPS_MINOR_FREEZE_MEMTABLE:
                    case OB_UPS_MINOR_LOAD_BYPASS:
                    case OB_UPS_MAJOR_LOAD_BYPASS:
                    case OB_UPS_ASYNC_MAJOR_FREEZE_MEMTABLE:
                      return_code = ups_freeze_memtable(version, packet_orig, thread_buff, packet_code);
                      break;
                    case OB_SWITCH_SCHEMA:
                      return_code = ups_switch_schema(version, packet_orig, *in_buf);
                      break;
                    case OB_UPS_FORCE_FETCH_SCHEMA:
                      return_code = ups_force_fetch_schema(version, conn, channel_id);
                      break;
                    case OB_UPS_SWITCH_COMMIT_LOG:
                      return_code = ups_switch_commit_log(version, conn, channel_id, thread_buff);
                      break;
                    case OB_SLAVE_REG:
                      return_code = ups_slave_register(version, *in_buf, conn, channel_id, thread_buff);
                      break;
                    case OB_UPS_CLEAR_ACTIVE_MEMTABLE:
                      return_code = ups_clear_active_memtable(version, conn, channel_id);
                      break;
                    case OB_UPS_ASYNC_CHECK_CUR_VERSION:
                      return_code = ups_check_cur_version();
                      break;
                    case OB_FAKE_WRITE_FOR_KEEP_ALIVE:
                      return_code = ups_handle_fake_write_for_keep_alive();
                      break;
                    case OB_WRITE:
                    case OB_INTERNAL_WRITE:
                    case OB_MS_MUTATE:
                      if (TRANS_NOT_START == trans_status)
                      {
                        return_code = ups_start_transaction(WRITE_TRANSACTION, handle);
                        if (OB_SUCCESS != return_code)
                        {
                          TBSYS_LOG(ERROR, "failed to start transaction, err=%d", return_code);
                        }
                        else
                        {
                          trans_status = TRANS_STARTED;
                          first_trans_idx = i;
                        }
                      }

                      if (OB_SUCCESS == return_code)
                      {
                        return_code = ups_apply(OB_MS_MUTATE == packet_code, handle, *in_buf, (*scanner_array)[i]);
                        if (OB_EAGAIN == return_code)
                        {
                          if (first_trans_idx < i)
                          {
                            TBSYS_LOG(INFO, "exceeds memory limit, should retry");
                          }
                          else
                          {
                            TBSYS_LOG(WARN, "mutator too large more than log_buffer, cannot apply");
                            return_code = OB_BUF_NOT_ENOUGH;
                          }
                        }
                        else if (OB_SUCCESS != return_code)
                        {
                          if (OB_COND_CHECK_FAIL != return_code)
                          {
                            TBSYS_LOG(WARN, "failed to apply mutation, err=%d", return_code);
                          }
                        }
                        FILL_TRACE_LOG("ups_apply src=%s ret=%d batch_num=%ld cur_trans_idx=%ld last_trans_idx=%ld",
                            inet_ntoa_r(conn->getPeerId()), return_code, batch_num, first_trans_idx, i);
                      }

                      if (OB_EAGAIN == return_code)
                      {
                        return_code = ups_end_transaction(packets, *scanner_array, first_trans_idx, i-1, handle, OB_SUCCESS);
                        --i; // re-execute the last mutation
                        trans_status = TRANS_NOT_START;
                        if (OB_SUCCESS != return_code)
                        {
                          TBSYS_LOG(WARN, "failed to end transaction, err=%d", return_code);
                          break;
                        }
                      }
                      else if (OB_SUCCESS != return_code)
                      {
                        return_code = ups_end_transaction(packets, *scanner_array, first_trans_idx, i, handle, return_code);
                        trans_status = TRANS_NOT_START;
                        if (OB_SUCCESS != return_code)
                        {
                          TBSYS_LOG(WARN, "failed to end transaction, err=%d", return_code);
                          break;
                        }
                      }
                      else if (i == batch_num - 1)
                      {
                        return_code = ups_end_transaction(packets, *scanner_array, first_trans_idx, i, handle, OB_SUCCESS);
                        trans_status = TRANS_NOT_START;
                        if (OB_SUCCESS != return_code)
                        {
                          TBSYS_LOG(WARN, "failed to end transaction, err=%d", return_code);
                          break;
                        }
                      }
                      break;

                    case OB_SEND_LOG:
                      return_code = ups_slave_write_log(version, *in_buf, conn, channel_id, thread_buff);
                      break;
                    default:
                      TBSYS_LOG(WARN, "unexpected packet_code %d", packet_code);
                      return_code = OB_ERR_UNEXPECTED;
                      break;
                  }
                }
              }

              if (OB_SUCCESS != return_code)
              {
                TBSYS_LOG(WARN, "call func error packet_code is %d return code is %d", packet_code, return_code);
              }
            }
          }
        }
        // do something after every loop
        if (OB_SUCCESS != return_code)
        {
          if (TRANS_STARTED == trans_status)
          {
            return_code = ups_end_transaction(packets, *scanner_array, first_trans_idx, i, handle, return_code);
            trans_status = TRANS_NOT_START;
            if (OB_SUCCESS != return_code)
            {
              TBSYS_LOG(WARN, "failed to end transaction, err=%d", return_code);
              return_code = OB_SUCCESS;
            }
          }
          else
          {
            return_code = OB_SUCCESS;
          }
        }
      }
      packet_repl.get_buffer()->reset();

      if (ObiRole::MASTER == obi_role_.get_role()
          && ObUpsRoleMgr::MASTER == role_mgr_.get_role()
          && ObUpsRoleMgr::ACTIVE == role_mgr_.get_state()
          && is_lease_valid())
      {
        TableMgr::FreezeType freeze_type = TableMgr::AUTO_TRIG;
        uint64_t frozen_version = 0;
        bool report_version_changed = false;
        if (OB_SUCCESS == table_mgr_.freeze_memtable(freeze_type, frozen_version, report_version_changed))
        {
          if (report_version_changed)
          {
            submit_report_freeze();
          }
          submit_handle_frozen();
        }
      }

      return ret;//if return true packet will be deleted.
    }

    int ObUpdateServer::ups_handle_fake_write_for_keep_alive()
    {
      int err = OB_SUCCESS;
      if (!(ObiRole::MASTER == obi_role_.get_role()
            && ObUpsRoleMgr::MASTER == role_mgr_.get_role()
            && ObUpsRoleMgr::ACTIVE == role_mgr_.get_state()
            && is_lease_valid()))
      {
        err = OB_NOT_MASTER;
        TBSYS_LOG(WARN, "not master:is_lease_valid=%s", STR_BOOL(is_lease_valid()));
      }
      else if (OB_SUCCESS != (err = log_mgr_.write_keep_alive_log()))
      {
        TBSYS_LOG(ERROR, "log_mgr.write_keep_alive_log()=>%d", err);
      }
      else if (OB_SUCCESS != (err = log_mgr_.flush_log()))
      {
        TBSYS_LOG(ERROR, "log_mgr.flush_log()=>%d", err);
      }
      if (OB_SUCCESS != err)
      {
        role_mgr_.set_state(ObUpsRoleMgr::FATAL);
        TBSYS_LOG(ERROR, "write_keep_alive_log fail");
      }
      return err;
    }

    int ObUpdateServer::return_not_master(const int32_t version, tbnet::Connection * conn,
        const uint32_t channel_id, const int32_t packet_code)
    {
      int return_code = OB_SUCCESS;
      UNUSED(version);
      switch (packet_code)
      {
        case OB_FREEZE_MEM_TABLE:
        case OB_UPS_MINOR_FREEZE_MEMTABLE:
        case OB_UPS_ASYNC_MAJOR_FREEZE_MEMTABLE:
        case OB_SWITCH_SCHEMA:
          TBSYS_LOG(INFO, "no longer master. refuse the order");
          break;
        case OB_UPS_FORCE_FETCH_SCHEMA:
          return_code = response_result_(OB_NOT_MASTER, OB_UPS_FORCE_FETCH_SCHEMA_RESPONSE, MY_VERSION, conn, channel_id);
          break;
        case OB_UPS_SWITCH_COMMIT_LOG:
          return_code = response_result_(OB_NOT_MASTER, OB_UPS_SWITCH_COMMIT_LOG_RESPONSE, MY_VERSION, conn, channel_id);
          break;
        case OB_SLAVE_REG:
          return_code = response_result_(OB_NOT_MASTER, OB_SLAVE_REG_RES, MY_VERSION, conn, channel_id);
          break;
        case OB_UPS_CLEAR_ACTIVE_MEMTABLE:
          return_code = response_result_(OB_NOT_MASTER, OB_UPS_CLEAR_ACTIVE_MEMTABLE_RESPONSE, MY_VERSION, conn, channel_id);
          break;
        case OB_UPS_ASYNC_CHECK_CUR_VERSION:
          TBSYS_LOG(INFO, "not master now, need not check cur version");
          break;
        case OB_WRITE:
        case OB_INTERNAL_WRITE:
        case OB_MS_MUTATE:
          return_code = response_result_(OB_NOT_MASTER, OB_WRITE_RES, MY_VERSION, conn, channel_id);
          break;
        default:
          TBSYS_LOG(ERROR, "UNEXPECT packet %d, ignore this", packet_code);
          return_code = OB_ERROR;
          break;
      }
      return return_code;
    }

    common::ThreadSpecificBuffer::Buffer* ObUpdateServer::get_rpc_buffer() const
    {
      return rpc_buffer_.get_buffer();
    }

    ObUpsRpcStub& ObUpdateServer::get_ups_rpc_stub()
      //MockUpsRpcStub& ObUpdateServer::get_ups_rpc_stub()
    {
      return ups_rpc_stub_;
    }

    int ObUpdateServer::set_obi_role(const int32_t version, common::ObDataBuffer& in_buff,
        tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      UNUSED(out_buff);

      int err = OB_SUCCESS;

      if (version != MY_VERSION)
      {
        err = OB_ERROR_FUNC_VERSION;
      }

      ObiRole obi_role;
      err = obi_role.deserialize(in_buff.get_data(), in_buff.get_limit(), in_buff.get_position());
      if (OB_SUCCESS != err)
      {
        TBSYS_LOG(ERROR, "ObiRole deserialize error, err=%d", err);
      }
      else
      {
        if (ObiRole::MASTER == obi_role.get_role())
        {
          if (ObiRole::MASTER == obi_role_.get_role())
          {
            TBSYS_LOG(INFO, "ObiRole is already MASTER");
          }
          else if (ObiRole::SLAVE == obi_role_.get_role())
          {
            obi_role_.set_role(ObiRole::MASTER);
            TBSYS_LOG(INFO, "ObiRole is set to MASTER");
          }
          else if (ObiRole::INIT == obi_role_.get_role())
          {
            obi_role_.set_role(ObiRole::MASTER);
            TBSYS_LOG(INFO, "ObiRole is set to MASTER");
          }
        }
        else if (ObiRole::SLAVE == obi_role.get_role())
        {
          if (ObiRole::MASTER == obi_role_.get_role())
          {
            obi_role_.set_role(ObiRole::SLAVE);
            TBSYS_LOG(INFO, "ObiRole is set to SLAVE");
          }
          else if (ObiRole::SLAVE == obi_role_.get_role())
          {
            TBSYS_LOG(INFO, "ObiRole is already SLAVE");
          }
          else if (ObiRole::INIT == obi_role_.get_role())
          {
            obi_role_.set_role(ObiRole::SLAVE);
            TBSYS_LOG(INFO, "ObiRole is set to SLAVE");
          }
        }
        else
        {
          TBSYS_LOG(ERROR, "Unknown ObiRole: %d", obi_role.get_role());
          err = OB_ERROR;
        }
      }

      // send response to MASTER before writing to disk
      err = response_result_(err, OB_SET_OBI_ROLE_RESPONSE, MY_VERSION, conn, channel_id);

      return err;
    }

    bool ObUpdateServer::is_lease_valid()
    {
      bool ret = true;
      int64_t lease = lease_expire_time_us_;
      int64_t lease_time_us = tbsys::CTimeUtil::getTime();
      TBSYS_LOG(DEBUG, "lease =%ld, cur_time = %ld, lease-cur_time=%ld", lease, lease_time_us, lease-lease_time_us);
      if (lease_time_us  + lease_timeout_in_advance_ > lease)
      {
        TBSYS_LOG(DEBUG, "lease timeout. lease=%ld, cur_time =%ld", lease, lease_time_us);
        ret = false;
      }
      return ret;
    }

    /*int ObUpdateServer::slave_set_fetch_param(const int32_t version, common::ObDataBuffer& in_buff,
        tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      int err = OB_SUCCESS;
      ObResultCode result_msg;
      result_msg.result_code_ = OB_SUCCESS;
      if (version != MY_VERSION)
      {
        TBSYS_LOG(WARN, "version not equal. version=%d, MY_VERSION=%d", version, MY_VERSION);
        result_msg.result_code_ = OB_ERROR_FUNC_VERSION;
      }
      ObUpsFetchParam fetch_param;
      if (OB_SUCCESS == err && result_msg.result_code_ == OB_SUCCESS)
      {
        err = fetch_param.deserialize(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position());
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN, "fail to deserialize fetch param. err=%d", err);
        }
      }
      if (OB_SUCCESS == err && result_msg.result_code_ == OB_SUCCESS)
      {
        result_msg.result_code_ = set_fetch_thread(fetch_param);
        if (OB_SUCCESS != result_msg.result_code_)
        {
          TBSYS_LOG(WARN, "fail to set_fetch_thread, err = %d", err);
        }
      }
      if (OB_SUCCESS == err)
      {
        err = result_msg.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position());
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN, "fail to serialize result_msg,err=%d", err);
        }
      }
      if (OB_SUCCESS == err)
      {
        err = send_response(OB_SEND_FETCH_PARAM_RES, MY_VERSION, out_buff, conn, channel_id);
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN, "fail to send response. err=%d", err);
        }
      }
      return err;
    }
*/
    int ObUpdateServer::ups_fetch_log_for_slave(const int32_t version, common::ObDataBuffer& in_buff,
          tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff, ObPacket* packet)
    {
      int err = OB_SUCCESS;
      ObFetchLogReq req;
      ObFetchedLog result;
      if (version != MY_VERSION)
      {
        err = OB_ERROR_FUNC_VERSION;
      }
      else if (OB_SUCCESS != (err = req.deserialize(in_buff.get_data(), in_buff.get_limit(), in_buff.get_position())))
      {
        TBSYS_LOG(ERROR, "req.deserialize(buf=%p[%ld], pos=%ld)=>%d",
                  in_buff.get_data(), in_buff.get_limit(), in_buff.get_position(), err);
      }
      else if (OB_SUCCESS != (err = log_mgr_.get_log_for_slave_fetch(req, result))
               && OB_DATA_NOT_SERVE != err)
      {
        TBSYS_LOG(ERROR, "log_mgr_.fetch_log_for_slave(req, result)=>%d", err);
      }
      else if (OB_DATA_NOT_SERVE == err)
      {
        err = OB_SUCCESS;
      }
      if (packet->get_receive_ts() + (packet->get_source_timeout()?: param_.get_packet_max_timewait())
          < tbsys::CTimeUtil::getTime())
      {
        err = OB_RESPONSE_TIME_OUT;
        TBSYS_LOG(ERROR, "get_log_for_slave_fetch() too slow[receive_ts[%ld] + timeout[%ld] < curTime[%ld]]",
                  packet->get_receive_ts(), packet->get_source_timeout()?: param_.get_packet_max_timewait(),
                  tbsys::CTimeUtil::getTime());
      }
      else if (OB_SUCCESS != (err = response_data_(err, result, OB_FETCH_LOG_RESPONSE, MY_VERSION, conn, channel_id, out_buff)))
      {
        TBSYS_LOG(ERROR, "response_data()=>%d", err);
      }
      return err;
    }

    int ObUpdateServer::ups_fill_log_cursor_for_slave(const int32_t version, common::ObDataBuffer& in_buff,
                                                      tbnet::Connection* conn, const uint32_t channel_id,
                                                      common::ObDataBuffer& out_buff)
    {
      int err = OB_SUCCESS;
      ObLogCursor cursor;
      if (version != MY_VERSION)
      {
        err = OB_ERROR_FUNC_VERSION;
      }
      else if (OB_SUCCESS != (err = cursor.deserialize(in_buff.get_data(), in_buff.get_limit(), in_buff.get_position())))
      {
        TBSYS_LOG(ERROR, "cursor.deserialize(buf=%p[%ld], pos=%ld)=>%d",
                  in_buff.get_data(), in_buff.get_limit(), in_buff.get_position(), err);
      }
      else if (OB_SUCCESS != (err = log_mgr_.fill_log_cursor(cursor)))
      {
        TBSYS_LOG(ERROR, "log_mgr_.fill_log_cursor(cursor)=>%d", err);
      }
      else if (OB_SUCCESS !=
          (err = response_data_(err, cursor, OB_FILL_LOG_CURSOR_RESPONSE, MY_VERSION, conn, channel_id, out_buff)))
      {
        TBSYS_LOG(ERROR, "response_data()=>%d", err);
      }
      return err;
    }

    int ObUpdateServer::ups_get_clog_status(const int32_t version, common::ObDataBuffer& in_buff,
                                                      tbnet::Connection* conn, const uint32_t channel_id,
                                                      common::ObDataBuffer& out_buff)
    {
      int err = OB_SUCCESS;
      ObLogCursor replayed_cursor;
      uint64_t frozen_version = 0;
      int64_t max_log_id_replayable = 0;
      ObUpsCLogStatus stat;
      UNUSED(in_buff);

      if (version != MY_VERSION)
      {
        err = OB_ERROR_FUNC_VERSION;
      }
      else if (OB_SUCCESS != (err = log_mgr_.get_replayed_cursor(replayed_cursor)))
      {
        TBSYS_LOG(ERROR, "log_mgr.get_replayed_cursor()=>%d", err);
      }
      else if (OB_SUCCESS != (err = table_mgr_.get_last_frozen_memtable_version(frozen_version)))
      {
        TBSYS_LOG(ERROR, "table_mgr.get_last_frozen_memtable_version()=>%d", err);
      }
      else if (OB_SUCCESS != (err = log_mgr_.get_max_log_seq_replayable(max_log_id_replayable)))
      {
        TBSYS_LOG(ERROR, "log_mgr.get_max_log_id_replayable()=>%d", err);
      }
      else
      {
        stat.obi_slave_stat_ = obi_slave_stat_;
        stat.slave_sync_type_ = slave_type_;
        stat.obi_role_ = obi_role_;
        stat.role_mgr_ = role_mgr_;
        stat.rs_ = root_server_;
        stat.self_ = self_addr_;
        stat.ups_master_ = ups_master_;
        stat.inst_ups_master_ = ups_inst_master_;
        stat.lsync_ = lsync_server_;
        stat.last_frozen_version_ = frozen_version;
        stat.replay_switch_ = log_replay_thread_.get_switch();
        stat.replayed_cursor_ = replayed_cursor;
        stat.max_log_id_replayable_ = max_log_id_replayable;
        stat.master_log_id_ = log_mgr_.get_master_log_seq();
      }
      if (OB_SUCCESS !=
          (err = response_data_(err, stat, OB_GET_CLOG_STATUS_RESPONSE, MY_VERSION, conn, channel_id, out_buff)))
      {
        TBSYS_LOG(ERROR, "response_data()=>%d", err);
      }
      return err;
    }

    int ObUpdateServer::ups_slave_write_log(const int32_t version, common::ObDataBuffer& in_buff,
        tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      UNUSED(out_buff);

      int err = OB_SUCCESS;
      int response_err = OB_SUCCESS;

      if (version != MY_VERSION)
      {
        err = OB_ERROR_FUNC_VERSION;
      }

      // int64_t in_buff_begin = in_buff.get_position();
      // bool switch_log_flag = false;
      // uint64_t log_id;

      if (OB_SUCCESS != err)
      {}
      else if (OB_SUCCESS != (err = (log_mgr_.slave_receive_log(in_buff.get_data() + in_buff.get_position(),
                                                                in_buff.get_limit() - in_buff.get_position()))))
      {
        TBSYS_LOG(ERROR, "slave_receive_log()=>%d", err);
      }
      if (OB_SUCCESS != (response_err = response_result_(err, OB_SEND_LOG_RES, MY_VERSION, conn, channel_id)))
      {
        err = response_err;
        TBSYS_LOG(ERROR, "response_result_()=>%d", err);
      }

      return err;
    }

    int ObUpdateServer::ups_change_log_level(const int32_t version, common::ObDataBuffer& in_buff,
                                         tbnet::Connection* conn, const uint32_t channel_id)
    {
      int ret = OB_SUCCESS;
      UNUSED(version);
      int32_t log_level = -1;
      if (OB_SUCCESS != (ret = serialization::decode_vi32(in_buff.get_data(),
                                                          in_buff.get_capacity(),
                                                          in_buff.get_position(),
                                                          &log_level)))
      {
        TBSYS_LOG(WARN, "deserialize error, err=%d", ret);
      }
      else
      {
        if (TBSYS_LOG_LEVEL_ERROR <= log_level
            && TBSYS_LOG_LEVEL_DEBUG >= log_level)
        {
          TBSYS_LOG(INFO, "change log level. From: %d, To: %d.", TBSYS_LOGGER._level, log_level);
          TBSYS_LOGGER._level = log_level;
        }
        else
        {
          TBSYS_LOG(WARN, "invalid log level, level=%d", log_level);
          ret = OB_INVALID_ARGUMENT;
        }
        if (OB_SUCCESS == ret)
          ret = response_result_(ret, OB_CHANGE_LOG_LEVEL_RESPONSE, MY_VERSION, conn, channel_id);
      }
      return ret;
    }

    int ObUpdateServer::ups_stop_server(const int32_t version, common::ObDataBuffer& in_buff,
                                         tbnet::Connection* conn, const uint32_t channel_id)
    {
      int ret = OB_SUCCESS;
      UNUSED(version);

      int64_t server_id = root_server_.get_ipv4_server_id();
      int64_t peer_id = conn->getPeerId();
      if (server_id != peer_id)
      {
        TBSYS_LOG(WARN, "*stop server* WARNNING coz packet from unrecongnized address "
                  "which is [%ld], should be [%ld] as rootserver.", peer_id, server_id);
        // comment follow line not to strict packet from rs.
        // rc.result_code_ = OB_ERROR;
      }

      int32_t restart = 0;
      if (OB_SUCCESS != (ret = serialization::decode_i32(in_buff.get_data(),
                                                         in_buff.get_capacity(),
                                                         in_buff.get_position(),
                                                         &restart)))
      {
        TBSYS_LOG(WARN, "deserialize error, err=%d", ret);
      }
      else
      {
        ret = response_result_(ret, OB_SET_OBI_ROLE_RESPONSE, MY_VERSION, conn, channel_id);
        if (restart != 0) {
          TBSYS_LOG(WARN, "set restart server flag, ready to restart server!");
          BaseMain::set_restart_flag();
        }
        stop();
      }
      return ret;
    }

    int ObUpdateServer::ob_login(const int32_t version, common::ObDataBuffer& in_buff,
        tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      int ret = OB_SUCCESS;
      int proc_ret = OB_SUCCESS;

      if (version != MY_VERSION)
      {
        ret = OB_ERROR_FUNC_VERSION;
      }

      ObLoginInfo login_info;
      ret = login_info.deserialize(in_buff.get_data(), in_buff.get_limit(), in_buff.get_position());
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "deserialize login info fail ret=%d src=%s", ret, NULL == conn ? NULL : inet_ntoa_r(conn->getPeerId()));
      }
      else
      {
        ObToken token;
        ret = login_mgr_.handle_login(login_info, token);
        if (OB_SUCCESS == ret)
        {
          proc_ret = response_data_(ret, token, OB_LOGIN_RES, MY_VERSION, conn, channel_id, out_buff);
        }
        else
        {
          proc_ret = response_result_(ret, OB_LOGIN_RES, MY_VERSION, conn, channel_id);
        }
      }
      TBSYS_LOG(INFO, "ob_login ret=%d src=%s", ret, NULL == conn ? NULL : inet_ntoa_r(conn->getPeerId()));

      return proc_ret;
    }

    int ObUpdateServer::ups_set_sync_limit(const int32_t version, common::ObDataBuffer& in_buff,
        tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      UNUSED(out_buff);
      int ret = OB_SUCCESS;

      if (version != MY_VERSION)
      {
        ret = OB_ERROR_FUNC_VERSION;
      }

      int64_t new_limit = 0;
      ret = serialization::decode_vi64(in_buff.get_data(), in_buff.get_limit(), in_buff.get_position(), &new_limit);
      if (OB_SUCCESS == ret)
      {
        //fetch_thread_.set_limit_rate(new_limit);
        //TBSYS_LOG(INFO, "update sync limit=%ld", fetch_thread_.get_limit_rate());
      }

      ret = response_result_(ret, OB_SET_SYNC_LIMIT_RESPONSE, MY_VERSION, conn, channel_id);

      return ret;
    }

    int ObUpdateServer::ups_ping(const int32_t version, tbnet::Connection* conn, const uint32_t channel_id)
    {
      int ret = OB_SUCCESS;

      if (version != MY_VERSION)
      {
        ret = OB_ERROR_FUNC_VERSION;
      }

      ret = response_result_(ret, OB_PING_RESPONSE, MY_VERSION, conn, channel_id);

      return ret;
    }

    int ObUpdateServer::ups_get_clog_master(const int32_t version, tbnet::Connection* conn,
                                            const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      int err = OB_SUCCESS;
      common::ObServer* master = NULL;
      if (MY_VERSION != version)
      {
        err = OB_ERROR_FUNC_VERSION;
        TBSYS_LOG(ERROR, "MY_VERSION[%d] != version[%d]", MY_VERSION, version);
      }
      else if (ObiRole::MASTER == obi_role_.get_role())
      {
        if (ObUpsRoleMgr::MASTER == role_mgr_.get_role())
        {
          master = &self_addr_;
        }
        else if (ObUpsRoleMgr::SLAVE == role_mgr_.get_role())
        {
          master = &ups_master_;
        }
        else
        {
          err = OB_ERROR;
          TBSYS_LOG(ERROR, "ob_role != MASTER && obi_role != SLAVE");
        }
      }
      else if (ObiRole::SLAVE == obi_role_.get_role())
      {
        if (ObUpsRoleMgr::MASTER == role_mgr_.get_role())
        {
          if (STANDALONE_SLAVE == obi_slave_stat_)
          {
            master = &lsync_server_;
          }
          else if (FOLLOWED_SLAVE == obi_slave_stat_)
          {
            master = &ups_inst_master_;
          }
        }
        else if (ObUpsRoleMgr::SLAVE == role_mgr_.get_role())
        {
          if (STANDALONE_SLAVE == obi_slave_stat_)
          {
              //master = &lsync_server_;
            err = OB_NOT_SUPPORTED;
            TBSYS_LOG(ERROR, "slave slave ups not allowed to connect lsyncserver");
          }
          else if (FOLLOWED_SLAVE == obi_slave_stat_)
          {
            master = &ups_master_;
          }
        }
        else
        {
          err = OB_ERROR;
          TBSYS_LOG(ERROR, "ob_role != MASTER && obi_role != SLAVE");
        }
      }
      else
      {
        err = OB_ERROR;
        TBSYS_LOG(ERROR, "obi_role != MASTER && obi_role != SLAVE");
      }

      if (OB_SUCCESS != err)
      {}
      else if (NULL == master)
      {
        err = OB_ERR_UNEXPECTED;
        TBSYS_LOG(ERROR, "NULL == master");
      }
      else if(OB_SUCCESS != (err = response_data_(err, *master, OB_GET_CLOG_MASTER_RESPONSE, MY_VERSION, conn, channel_id, out_buff)))
      {
        TBSYS_LOG(ERROR, "response_data()=>%d", err);
      }

      return err;
    }

    int ObUpdateServer::ups_get_clog_cursor(const int32_t version, tbnet::Connection* conn,
                                            const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      int err = OB_SUCCESS;
      ObLogCursor log_cursor;
      if (MY_VERSION != version)
      {
        err = OB_ERROR_FUNC_VERSION;
        TBSYS_LOG(ERROR, "MY_VERSION[%d] != version[%d]", MY_VERSION, version);
      }
      else if (OB_SUCCESS != (err = log_mgr_.get_replayed_cursor(log_cursor)))
      {
        TBSYS_LOG(ERROR, "log_mgr.get_replayed_cursor()=>%d", err);
      }

      if(OB_SUCCESS != (err = response_data_(err, log_cursor, OB_GET_CLOG_CURSOR_RESPONSE, MY_VERSION, conn, channel_id, out_buff)))
      {
        TBSYS_LOG(ERROR, "response_data()=>%d", err);
      }

      return err;
    }

    int ObUpdateServer::ups_get_log_sync_delay_stat(const int32_t version, tbnet::Connection* conn,
                                            const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      int err = OB_SUCCESS;
      if (MY_VERSION != version)
      {
        err = OB_ERROR_FUNC_VERSION;
        TBSYS_LOG(ERROR, "MY_VERSION[%d] != version[%d]", MY_VERSION, version);
      }
      else if(OB_SUCCESS != (err = response_data_(err, log_mgr_.get_delay_stat(), OB_GET_LOG_SYNC_DELAY_STAT_RESPONSE, MY_VERSION, conn, channel_id, out_buff)))
      {
       TBSYS_LOG(ERROR, "response_data()=>%d", err);
      }
      else if(OB_SUCCESS != (err = log_mgr_.get_delay_stat().reset_max_delay()))
      {
       TBSYS_LOG(ERROR, "reset_max_delay()=>%d", err);
      }
      return err;
    }

    int ObUpdateServer::ups_get(const int32_t version, common::ObDataBuffer& in_buff,
        tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff,
        const int64_t start_time, const int64_t timeout, const int32_t priority)
    {
      int ret = OB_SUCCESS;
      ObGetParam get_param_stack;
      ObScanner scanner_stack;
      ObGetParam *get_param_ptr = GET_TSI_MULT(ObGetParam, TSI_UPS_GET_PARAM_1);
      ObScanner *scanner_ptr = GET_TSI_MULT(ObScanner, TSI_UPS_SCANNER_1);
      ObGetParam &get_param = (NULL == get_param_ptr) ? get_param_stack : *get_param_ptr;
      ObScanner &scanner = (NULL == scanner_ptr) ? scanner_stack : *scanner_ptr;

      if (version != MY_VERSION)
      {
        ret = OB_ERROR_FUNC_VERSION;
      }

      //CLEAR_TRACE_LOG();
      if (OB_SUCCESS == ret)
      {
        ret = get_param.deserialize(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position());
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "deserialize get param error, ret=%d", ret);
        }
        FILL_TRACE_LOG("get param deserialize ret=%d", ret);
      }

      if (OB_SUCCESS == ret)
      {
        if (get_param.get_is_read_consistency())
        {
          if ( !(ObiRole::MASTER == obi_role_.get_role() && ObUpsRoleMgr::MASTER == role_mgr_.get_role()) )
          {
            TBSYS_LOG(DEBUG, "The Get Request require consistency, ObiRole:%s RoleMgr:%s",
                obi_role_.get_role_str(), role_mgr_.get_role_str());
            ret = OB_NOT_MASTER;
          }
        }
      }

      if (OB_SUCCESS == ret)
      {
        thread_read_prepare();
        scanner.reset();
        ret = table_mgr_.get(get_param, scanner, start_time, timeout);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "failed to get, err=%d", ret);
        }
        FILL_TRACE_LOG("get from table mgr ret=%d", ret);
      }

      ret = response_data_(ret, scanner, OB_GET_RESPONSE, MY_VERSION, conn, channel_id, out_buff, &priority);
      INC_STAT_INFO(UPS_STAT_GET_COUNT, 1);
      INC_STAT_INFO(UPS_STAT_GET_TIMEU, GET_TRACE_TIMEU());
      FILL_TRACE_LOG("response scanner ret=%d", ret);
      PRINT_TRACE_LOG();

      thread_read_complete();

      return ret;
    }

    template <class T>
    int ObUpdateServer::response_data_(int32_t ret_code, const T &data,
                                          int32_t cmd_type, int32_t func_version,
                                          tbnet::Connection* conn, const uint32_t channel_id,
                                          common::ObDataBuffer& out_buff, const int32_t* priority)
    {
      int ret = OB_SUCCESS;
      common::ObResultCode result_msg;
      result_msg.result_code_ = ret_code;
      ret = result_msg.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position());
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "serialize result msg error, ret=%d", ret);
        ret = OB_ERROR;
      }
      else
      {
        common::ObDataBuffer tmp_buffer = out_buff;
        ret = ups_serialize(data, out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position());
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "serialize data error, ret=%d", ret);
          ret = OB_ERROR;
        }
        else
        {
          ret = send_response(cmd_type, func_version, out_buff, conn, channel_id);
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "failed to send scan response, ret=%d", ret);
            ret = OB_ERROR;
          }
          if (OB_SUCCESS == ret && NULL != priority && PriorityPacketQueueThread::LOW_PRIV == *priority)
          {
            low_priv_speed_control_(out_buff.get_position());
          }
        }
      }
      return ret;
    }

    int ObUpdateServer::response_fetch_param_(int32_t ret_code, const ObUpsFetchParam& fetch_param,
        const int64_t log_id, int32_t cmd_type, int32_t func_version,
        tbnet::Connection* conn, const uint32_t channel_id,
        common::ObDataBuffer& out_buff)
    {
      int ret = OB_SUCCESS;
      common::ObResultCode result_msg;
      result_msg.result_code_ = ret_code;
      ret = result_msg.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position());
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "serialize result msg error, ret=%d", ret);
        ret = OB_ERROR;
      }
      else
      {
        ret = fetch_param.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position());
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "serialize fetch param error, ret=%d", ret);
          ret = OB_ERROR;
        }
        else
        {
          if (OB_SUCCESS == (ret = serialization::encode_i64(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position(), log_id)))
          {
            ret = send_response(cmd_type, func_version, out_buff, conn, channel_id);
            if (OB_SUCCESS != ret)
            {
              TBSYS_LOG(WARN, "failed to send scan response, ret=%d", ret);
              ret = OB_ERROR;
            }
          }
          else
          {
            TBSYS_LOG(WARN, "fail to encode max log id . err=%d", ret);
          }
        }

      }
      return ret;
    }

    int ObUpdateServer::response_lease_(int32_t ret_code, const ObLease& lease,
        int32_t cmd_type, int32_t func_version,
        tbnet::Connection* conn, const uint32_t channel_id,
        common::ObDataBuffer& out_buff)
    {
      int ret = OB_SUCCESS;
      common::ObResultCode result_msg;
      result_msg.result_code_ = ret_code;
      ret = result_msg.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position());
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "serialize result msg error, ret=%d", ret);
        ret = OB_ERROR;
      }
      else
      {
        ret = lease.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position());
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "serialize lease error, ret=%d", ret);
          ret = OB_ERROR;
        }
        else
        {
          ret = send_response(cmd_type, func_version, out_buff, conn, channel_id);
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "failed to send scan response, ret=%d", ret);
            ret = OB_ERROR;
          }
        }
      }
      return ret;
    }

    int ObUpdateServer::ups_scan(const int32_t version, common::ObDataBuffer& in_buff,
        tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff,
        const int64_t start_time, const int64_t timeout, const int32_t priority)
    {
      int err = OB_SUCCESS;
      ObScanParam scan_param_stack;
      ObScanner scanner_stack;
      ObScanParam *scan_param_ptr = GET_TSI_MULT(ObScanParam, TSI_UPS_SCAN_PARAM_1);
      ObScanner *scanner_ptr = GET_TSI_MULT(ObScanner, TSI_UPS_SCANNER_1);
      ObScanParam &scan_param = (NULL == scan_param_ptr) ? scan_param_stack : *scan_param_ptr;
      ObScanner &scanner = (NULL == scanner_ptr) ? scanner_stack : *scanner_ptr;
      common::ObResultCode result_msg;

      if (version != MY_VERSION)
      {
        err = OB_ERROR_FUNC_VERSION;
      }

      //CLEAR_TRACE_LOG();
      if (OB_SUCCESS == err)
      {
        err = scan_param.deserialize(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position());
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN, "deserialize scan param error, err=%d", err);
        }
        FILL_TRACE_LOG("scan param deserialize ret=%d", err);
      }

      if (OB_SUCCESS == err)
      {
        if (scan_param.get_is_read_consistency())
        {
          if ( !(ObiRole::MASTER == obi_role_.get_role() && ObUpsRoleMgr::MASTER == role_mgr_.get_role()) )
          {
            TBSYS_LOG(INFO, "The Scan Request require consistency, ObiRole:%s RoleMgr:%s",
                obi_role_.get_role_str(), role_mgr_.get_role_str());
            err = OB_NOT_MASTER;
          }
        }
      }

      if (OB_SUCCESS == err)
      {
        thread_read_prepare();
        scanner.reset();
        err = table_mgr_.scan(scan_param, scanner, start_time, timeout);
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN, "failed to scan, err=%d", err);
        }
        FILL_TRACE_LOG("scan from table mgr ret=%d", err);
      }

      err = response_data_(err, scanner, OB_SCAN_RESPONSE, MY_VERSION, conn, channel_id, out_buff, &priority);
      INC_STAT_INFO(UPS_STAT_SCAN_COUNT, 1);
      INC_STAT_INFO(UPS_STAT_SCAN_TIMEU, GET_TRACE_TIMEU());
      FILL_TRACE_LOG("response scanner ret=%d", err);
      PRINT_TRACE_LOG();

      thread_read_complete();

      return err;
    }

    int ObUpdateServer::ups_get_bloomfilter(const int32_t version, common::ObDataBuffer& in_buff,
        tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      int ret = OB_SUCCESS;
      int64_t frozen_version = 0;
      TableBloomFilter table_bf;
      if (version != MY_VERSION)
      {
        ret = OB_ERROR_FUNC_VERSION;
      }
      else if (OB_SUCCESS != (ret = serialization::decode_vi64(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position(), &frozen_version)))
      {
        TBSYS_LOG(WARN, "decode cur version fail ret=%d", ret);
      }
      else
      {
        ret = table_mgr_.get_frozen_bloomfilter(frozen_version, table_bf);
      }
      ret = response_data_(ret, table_bf, OB_UPS_GET_BLOOM_FILTER_RESPONSE, MY_VERSION, conn, channel_id, out_buff);
      return ret;
    }

    int ObUpdateServer::ups_switch_skey()
    {
      int ret = OB_SUCCESS;
      int proc_ret = OB_SUCCESS;
      ObMutator mutator;
      if (OB_SUCCESS != (ret = skey_table_cache_.update_cur_skey(mutator)))
      {
        TBSYS_LOG(WARN, "build switch skey mutator fail ret=%d", ret);
      }
      else if (OB_SUCCESS != (ret = ups_rpc_stub_.send_mutator_apply(ups_master_, mutator, RPC_TIMEOUT)))
      {
        TBSYS_LOG(WARN, "send switch skey mutator apply fail ret=%d", ret);
      }
      else
      {
        TBSYS_LOG(INFO, "send switch skey mutator apply succ");
      }
      return proc_ret;
    }

    int ObUpdateServer::ups_freeze_memtable(const int32_t version, ObPacket *packet_orig, common::ObDataBuffer& out_buff, const int pcode)
    {
      int ret = OB_SUCCESS;
      if (version != MY_VERSION)
      {
        ret = OB_ERROR_FUNC_VERSION;
      }
      TableMgr::FreezeType freeze_type = TableMgr::AUTO_TRIG;
      if (OB_UPS_MINOR_FREEZE_MEMTABLE == pcode)
      {
        freeze_type = TableMgr::FORCE_MINOR;
      }
      else if (OB_FREEZE_MEM_TABLE == pcode
          || OB_UPS_ASYNC_MAJOR_FREEZE_MEMTABLE == pcode)
      {
        freeze_type = TableMgr::FORCE_MAJOR;
      }
      else if (OB_UPS_MINOR_LOAD_BYPASS == pcode)
      {
        freeze_type = TableMgr::MINOR_LOAD_BYPASS;
      }
      else if (OB_UPS_MAJOR_LOAD_BYPASS == pcode)
      {
        freeze_type = TableMgr::MAJOR_LOAD_BYPASS;
      }
      uint64_t frozen_version = 0;
      bool report_version_changed = false;
      if (!(ObiRole::MASTER == obi_role_.get_role()
            && ObUpsRoleMgr::MASTER == role_mgr_.get_role()
            && ObUpsRoleMgr::ACTIVE == role_mgr_.get_state()
            && is_lease_valid()))
      {
        ret = OB_NOT_MASTER;
        TBSYS_LOG(WARN, "not master:is_lease_valie=%s", STR_BOOL(is_lease_valid()));
      }
      else if (OB_SUCCESS != (ret = table_mgr_.freeze_memtable(freeze_type, frozen_version, report_version_changed, packet_orig)))
      {
        TBSYS_LOG(WARN, "freeze memtable fail ret=%d", ret);
      }
      else
      {
        if (report_version_changed
            && OB_UPS_MINOR_LOAD_BYPASS != pcode
            && OB_UPS_MAJOR_LOAD_BYPASS != pcode) // 旁路加载freeze不能立即汇报 需要等加载完后再汇报
        {
          submit_report_freeze();
        }
        submit_handle_frozen();
      }
      TBSYS_LOG(INFO, "handle freeze_memtable pcode=%d freeze_type=%d ret=%d", pcode, freeze_type, ret);
      if ((OB_UPS_ASYNC_MAJOR_FREEZE_MEMTABLE != pcode
            && OB_UPS_MINOR_LOAD_BYPASS != pcode
            && OB_UPS_MAJOR_LOAD_BYPASS != pcode)
          || (OB_UPS_MINOR_LOAD_BYPASS == pcode && OB_SUCCESS != ret)
          || (OB_UPS_MAJOR_LOAD_BYPASS == pcode && OB_SUCCESS != ret))
      {
        // 本地异步任务不需要应答
        ret = response_data_(ret, frozen_version, pcode + 1, MY_VERSION,
            packet_orig->get_connection(), packet_orig->getChannelId(), out_buff);
      }
      return ret;
    }

    int ObUpdateServer::ups_store_memtable(const int32_t version, common::ObDataBuffer &in_buf,
        tbnet::Connection* conn, const uint32_t channel_id)
    {
      int ret = OB_SUCCESS;
      if (version != MY_VERSION)
      {
        ret = OB_ERROR_FUNC_VERSION;
      }
      int64_t store_all = 0;
      if (OB_SUCCESS != (ret = serialization::decode_vi64(in_buf.get_data(), in_buf.get_capacity(), in_buf.get_position(), &store_all)))
      {
        TBSYS_LOG(WARN, "decode store_all flag fail ret=%d", ret);
      }
      else
      {
        TBSYS_LOG(INFO, "store memtable store_all=%ld", store_all);
      }
      response_result_(ret, OB_UPS_STORE_MEM_TABLE_RESPONSE, MY_VERSION, conn, channel_id);
      if (OB_SUCCESS == ret)
      {
        table_mgr_.store_memtable(0 != store_all);
      }
      return ret;
    }

    int ObUpdateServer::ups_handle_frozen()
    {
      int ret = OB_SUCCESS;
      table_mgr_.update_merged_version(ups_rpc_stub_, root_server_, param_.get_resp_root_timeout_us());
      bool force = false;
      table_mgr_.erase_sstable(force);
      bool store_all = false;
      table_mgr_.store_memtable(store_all);
      table_mgr_.log_table_info();
      return ret;
    }

    int ObUpdateServer::check_keep_alive_()
    {
      int err = OB_SUCCESS;
      if (ObUpsRoleMgr::FATAL == role_mgr_.get_state())
      {
        TBSYS_LOG(DEBUG, "enter fatal state");
      }
      else if (ObiRole::SLAVE == obi_role_.get_role()
          || ObUpsRoleMgr::SLAVE == role_mgr_.get_role())
      {
        if (ObUpsRoleMgr::MASTER == role_mgr_.get_role()
            && STANDALONE_SLAVE == obi_slave_stat_)
        {
          TBSYS_LOG(DEBUG, "STANDALONE slave_master, need not check keep alive.");
        }
        else
        {
          ObServer null_server;
          int64_t cur_time_us = tbsys::CTimeUtil::getTime();
          int64_t last_keep_alive_time = log_mgr_.get_last_receive_log_time();
          TBSYS_LOG(DEBUG, "slave check keep_alive, cur_time=%ld, last_keep_alive_time=%ld", 
                    cur_time_us, last_keep_alive_time);
          if (last_keep_alive_time + keep_alive_valid_interval_ < cur_time_us)
          {
            if (last_keep_alive_time != 0)
            {
              TBSYS_LOG(WARN, "keep_alive msg timeout, last_time = %ld, cur_time = %ld, duration_time = %ld", last_keep_alive_time,  cur_time_us, keep_alive_valid_interval_);
              if (ObUpsRoleMgr::ACTIVE == role_mgr_.get_state())
              {
                TBSYS_LOG(WARN, "slave_ups can't connect with master_ups. set state to REPLAYING_LOG");
                role_mgr_.set_state(ObUpsRoleMgr::REPLAYING_LOG);
              }
            }
            else
            {
              TBSYS_LOG(DEBUG, "ups neet register to master_ups.");
            }
            //register to master_ups
            if (ObUpsRoleMgr::MASTER == role_mgr_.get_role())
            {
              err = ups_rpc_stub_.get_inst_master_ups(root_server_, ups_inst_master_, DEFAULT_NETWORK_TIMEOUT);
              if (OB_SUCCESS != err)
              {
                TBSYS_LOG(WARN, "fail to get inst_master_ups. err=%d", err);
              }
              else
              {
                TBSYS_LOG(INFO, "get master_master_ups addr = %s", ups_inst_master_.to_cstring());
                if (ups_inst_master_ == self_addr_)
                {}
                else
                {
                  err = register_to_master_ups(ups_inst_master_);
                  if (OB_SUCCESS != err)
                  {
                    TBSYS_LOG(WARN, "fail to register to inst_master_ups. err=%d, inst_master_ups addr=%s", err, ups_inst_master_.to_cstring());
                  }
                  else
                  {
                    TBSYS_LOG(INFO, "register to inst_master_ups succ. inst_master_ups=%s", ups_inst_master_.to_cstring());
                  }
                }
              }
            }
            else
            {
              if (null_server == ups_master_)
              {}
              else if (self_addr_ == ups_master_)
              {}
              else
              {
                err = register_to_master_ups(ups_master_);
                if (OB_SUCCESS != err)
                {
                  TBSYS_LOG(WARN, "fail to register to master_ups. err=%d", err);
                }
                else
                {
                  TBSYS_LOG(INFO, "register to master_ups succ. master_ups=%s", ups_master_.to_cstring());
                }
              }
            }
          }
        }
      }
      else
      {
        //do nothing;
      }
      return err;
    }

    int ObUpdateServer::grant_keep_alive_()
    {
      int err = OB_SUCCESS;
      int64_t log_seq_id = 0;
      if (ObUpsRoleMgr::FATAL == role_mgr_.get_state())
      {
        TBSYS_LOG(DEBUG, "enter FATAL state.");
      }
      else
      {
        if (ObiRole::MASTER != obi_role_.get_role()
            || ObUpsRoleMgr::MASTER != role_mgr_.get_role()
            || ObUpsRoleMgr::ACTIVE != role_mgr_.get_state())
        {
          TBSYS_LOG(DEBUG, "ups not master.obi_role=%s, role=%s, state=%s", obi_role_.get_role_str(), role_mgr_.get_role_str(), role_mgr_.get_state_str());
        }
        else if (!is_lease_valid())
        {
          TBSYS_LOG(DEBUG, "lease is invalid");
        }
        else if (log_mgr_.get_last_flush_log_time() + GRANT_KEEP_ALIVE_PERIOD > tbsys::CTimeUtil::getTime())
        {
          TBSYS_LOG(DEBUG, "log_mgr.last_flush_log_time=%ld, no need write NOP again",
                    log_mgr_.get_last_flush_log_time());
        }
        else if (OB_SUCCESS != (err = submit_fake_write_for_keep_alive()))
        {
          TBSYS_LOG(WARN, "submit_fake_write()=>%d", err);
        }

        if (OB_SUCCESS != err)
        {}
        else if (OB_SUCCESS != (err = log_mgr_.get_max_log_seq_replayable(log_seq_id)))
        {
          TBSYS_LOG(WARN, "fail to get max log seq. err = %d", err);
        }
        else if (OB_SUCCESS != (err = register_to_rootserver(log_seq_id)))
        {
          TBSYS_LOG(WARN, "fail to send keep_alive to rs. err=%d", err);
        }
      }
      return err;
    }

    int ObUpdateServer::register_to_rootserver(const uint64_t log_seq_id)
    {
      int err = OB_SUCCESS;
      ObMsgUpsRegister msg_register;
      set_register_msg(log_seq_id, msg_register);
      int64_t renew_reserved_us = 0;
      err = ups_rpc_stub_.ups_register(root_server_, msg_register, renew_reserved_us, DEFAULT_NETWORK_TIMEOUT);
      if (OB_SUCCESS != err && OB_ALREADY_REGISTERED != err)
      {
        TBSYS_LOG(WARN, "fail to register to rootserver. err = %d", err);
      }
      else
      {
        if (0 == ups_renew_reserved_us_)
        {
          ups_renew_reserved_us_ = renew_reserved_us;
        }
        err = OB_SUCCESS;
      }
      return err;
    }

    void ObUpdateServer::set_register_msg(const uint64_t log_seq_id, ObMsgUpsRegister &msg_register)
    {
      msg_register.log_seq_num_ = log_seq_id;
      msg_register.inner_port_ = static_cast<int32_t>(param_.get_ups_inner_port());
      msg_register.addr_.set_ipv4_addr(self_addr_.get_ipv4(), self_addr_.get_port());
      if (ObUpsRoleMgr::MASTER == role_mgr_.get_role())
      {
        msg_register.lease_ = lease_expire_time_us_;
      }
      else
      {
         msg_register.lease_ = 0;
      }
    }

    int ObUpdateServer::check_lease_()
    {
      int err = OB_SUCCESS;
      int64_t cur_time_us = tbsys::CTimeUtil::getTime();
      if (ObUpsRoleMgr::FATAL == role_mgr_.get_state())
      {
        TBSYS_LOG(DEBUG, "enter fatal state");
      }
      else if (lease_expire_time_us_ < cur_time_us)
      {
        if (0 != lease_expire_time_us_)
        {
          TBSYS_LOG(ERROR, "lease timeout, need reregister to rootserver. lease=%ld, cur_time=%ld",
              lease_expire_time_us_, cur_time_us);

          if (ObUpsRoleMgr::MASTER == role_mgr_.get_role()
              && ObiRole::MASTER == obi_role_.get_role())
          {
            err = master_switch_to_slave(false, true);
            if (OB_SUCCESS == err)
            {
              TBSYS_LOG(WARN, "master_master_ups lease timeout, change to master_slave");
            }
            else
            {
              TBSYS_LOG(ERROR, "master_master_ups lease timeout, change to master_slave failed!");
            }
          }
          else if (ObUpsRoleMgr::MASTER == role_mgr_.get_role())
          {
            err = slave_change_role(false, true);
            if (OB_SUCCESS != err)
            {
              TBSYS_LOG(WARN, "ups lease timetout, change role failed!");
            }
          }
        }
        int64_t log_id;
        log_mgr_.get_max_log_seq_replayable(log_id);
        err = register_to_rootserver(log_id);
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN, "fail to register to rootserver. err=%d", err);
        }
      }
      else if (lease_expire_time_us_ - cur_time_us < ups_renew_reserved_us_)
      {
        TBSYS_LOG(WARN, "lease will be timeout, retry to send renew lease to rootserver. \
            lease_time:%ld, cur_time:%ld, remain lease[%ld] should big than %ld",
            lease_expire_time_us_, cur_time_us, lease_expire_time_us_ - cur_time_us, ups_renew_reserved_us_);
        ObMsgUpsHeartbeatResp hb_res;
        set_heartbeat_res(hb_res);
        err = ups_rpc_stub_.renew_lease(root_server_, hb_res);
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN, "fail to send renew_lease to rootserver. err=%d", err);
        }
      }
      return err;
    }
    void ObUpdateServer::set_heartbeat_res(ObMsgUpsHeartbeatResp &hb_res)
    {
      hb_res.addr_.set_ipv4_addr(self_addr_.get_ipv4(), self_addr_.get_port());
      bool sync = false;
      if (ObUpsRoleMgr::ACTIVE == role_mgr_.get_state())
      {
        sync = true;
      }
      hb_res.status_ = (true == sync) ? ObMsgUpsHeartbeatResp::SYNC : ObMsgUpsHeartbeatResp::NOTSYNC;
      hb_res.obi_role_.set_role(obi_role_.get_role());
    }

    int ObUpdateServer::submit_check_keep_alive()
    {
      return submit_async_task_(OB_UPS_ASYNC_CHECK_KEEP_ALIVE, read_thread_queue_, read_task_queue_size_);
    }

    int ObUpdateServer::submit_lease_task()
    {
      return submit_async_task_(OB_UPS_ASYNC_CHECK_LEASE, lease_thread_queue_, lease_task_queue_size_);
    }

    int ObUpdateServer::submit_grant_keep_alive()
    {
      return submit_async_task_(OB_UPS_ASYNC_GRANT_KEEP_ALIVE, read_thread_queue_, read_task_queue_size_);
    }

    int ObUpdateServer::submit_fake_write_for_keep_alive()
    {
      return submit_async_task_(OB_FAKE_WRITE_FOR_KEEP_ALIVE, write_thread_queue_, write_task_queue_size_);
    }

    int ObUpdateServer::submit_handle_frozen()
    {
      return submit_async_task_(OB_UPS_ASYNC_HANDLE_FROZEN, store_thread_, store_thread_queue_size_);
    }

    int ObUpdateServer::submit_report_freeze()
    {
      return submit_async_task_(OB_UPS_ASYNC_REPORT_FREEZE, read_thread_queue_, read_task_queue_size_);
    }

    int ObUpdateServer::submit_replay_commit_log()
    {
      TBSYS_LOG(INFO, "submit replay commit log task");
      return submit_async_task_(OB_UPS_ASYNC_REPLAY_LOG, read_thread_queue_, read_task_queue_size_);
    }

    int ObUpdateServer::submit_prefetch_remote_log(ObPrefetchLogTaskSubmitter::Task& task)
    {
      ObDataBuffer in_buf;
      in_buf.set_data((char*)&task, sizeof(task));
      in_buf.get_position() = sizeof(task);
      return submit_async_task_(OB_PREFETCH_LOG, read_thread_queue_, read_task_queue_size_, &in_buf);
    }

    int ObUpdateServer::submit_major_freeze()
    {
      int err = OB_SUCCESS;
      if (ObUpsRoleMgr::MASTER == role_mgr_.get_role()
          && ObiRole::MASTER == obi_role_.get_role())
      {
        err = submit_async_task_(OB_UPS_ASYNC_MAJOR_FREEZE_MEMTABLE, write_thread_queue_, write_task_queue_size_);
      }
      else
      {
      }
      return err;
    }

    int ObUpdateServer::submit_switch_skey()
    {
      int err = OB_SUCCESS;
      if (ObUpsRoleMgr::MASTER == role_mgr_.get_role()
          && ObiRole::MASTER == obi_role_.get_role())
      {
        err = submit_async_task_(OB_UPS_ASYNC_SWITCH_SKEY, read_thread_queue_, read_task_queue_size_);
      }
      else
      {
      }
      return err;
    }

    int ObUpdateServer::submit_force_drop()
    {
      return submit_async_task_(OB_UPS_ASYNC_FORCE_DROP_MEMTABLE, read_thread_queue_, read_task_queue_size_);
    }

    int ObUpdateServer::submit_load_bypass(const common::ObPacket *packet)
    {
      return submit_async_task_(OB_UPS_ASYNC_LOAD_BYPASS, read_thread_queue_, read_task_queue_size_, NULL, packet);
    }

    int ObUpdateServer::submit_check_cur_version()
    {
      return submit_async_task_(OB_UPS_ASYNC_CHECK_CUR_VERSION, write_thread_queue_, write_task_queue_size_, NULL, NULL);
    }

    int ObUpdateServer::submit_immediately_drop()
    {
      int ret = OB_SUCCESS;
      submit_delay_drop();
      warm_up_duty_.finish_immediately();
      return ret;
    }

    int ObUpdateServer::submit_delay_drop()
    {
      int ret = OB_SUCCESS;
      if (warm_up_duty_.drop_start())
      {
        schedule_warm_up_duty();
      }
      else
      {
        TBSYS_LOG(INFO, "there is still a warm up duty running, will not schedule another");
      }
      return ret;
    }

    void ObUpdateServer::schedule_warm_up_duty()
    {
      int ret = OB_SUCCESS;
      int64_t warm_up_step_interval = warm_up_duty_.get_warm_up_step_interval();
      bool repeat = false;
      if (OB_SUCCESS != (ret = timer_.schedule(warm_up_duty_, warm_up_step_interval, repeat)))
      {
        TBSYS_LOG(WARN, "schedule warm_up_duty fail ret=%d, will force drop", ret);
        submit_force_drop();
      }
      else
      {
        TBSYS_LOG(INFO, "warm up scheduled interval=%ld", warm_up_step_interval);
      }
    }

    template <class Queue>
    int ObUpdateServer::submit_async_task_(const PacketCode pcode, Queue& qthread, int32_t task_queue_size,
        const int32_t version, common::ObDataBuffer& in_buff, tbnet::Connection* conn,
        const uint32_t channel_id, const int64_t timeout)
    {
      int ret = OB_SUCCESS;
      ObPacket *ob_packet = NULL;
      if (NULL == (ob_packet = dynamic_cast<ObPacket*>(packet_factory_.createPacket(pcode))))
      {
        TBSYS_LOG(WARN, "create packet fail");
        ret = OB_ERROR;
      }
      else
      {
        ob_packet->set_packet_code(pcode);
        ob_packet->set_api_version(version);
        ob_packet->set_connection(conn);
        ob_packet->setChannelId(channel_id);
        ob_packet->set_target_id(OB_SELF_FLAG);
        ob_packet->set_receive_ts(tbsys::CTimeUtil::getTime());
        ob_packet->set_source_timeout(timeout);
        ob_packet->set_data(in_buff);
        if (OB_SUCCESS != (ret = ob_packet->serialize()))
        {
          TBSYS_LOG(WARN, "ob_packet serialize fail ret=%d", ret);
        }
        else if (!qthread.push(ob_packet, task_queue_size, false))
        {
          TBSYS_LOG(WARN, "submit async task to thread queue fail task_queue_size=%d, pcode=%d", task_queue_size, pcode);
          ret = OB_ERROR;
        }
        else
        {
          TBSYS_LOG(DEBUG, "submit async task succ pcode=%d", pcode);
        }
        if (OB_SUCCESS != ret)
        {
          packet_factory_.destroyPacket(ob_packet);
          ob_packet = NULL;
        }
      }
      return ret;
    }

    template <class Queue>
    int ObUpdateServer::submit_async_task_(const PacketCode pcode, Queue &qthread, int32_t &task_queue_size, const ObDataBuffer *data_buffer,
                                          const common::ObPacket *packet)
    {
      int ret = OB_SUCCESS;
      ObPacket *ob_packet = NULL;
      if (NULL == (ob_packet = dynamic_cast<ObPacket*>(packet_factory_.createPacket(pcode))))
      {
        TBSYS_LOG(WARN, "create packet fail");
        ret = OB_ERROR;
      }
      else
      {
        if (NULL != packet)
        {
          ob_packet->set_api_version(packet->get_api_version());
          ob_packet->set_connection(packet->get_connection());
          ob_packet->setChannelId(packet->getChannelId());
          ob_packet->set_source_timeout(packet->get_source_timeout());
        }

        ob_packet->set_api_version(MY_VERSION);
        ob_packet->set_packet_code(pcode);
        ob_packet->set_target_id(OB_SELF_FLAG);
        ob_packet->set_receive_ts(tbsys::CTimeUtil::getTime());
        ob_packet->set_source_timeout(INT32_MAX);
        if (NULL != data_buffer)
        {
          ob_packet->set_data(*data_buffer);
        }
        if (OB_SUCCESS != (ret = ob_packet->serialize()))
        {
          TBSYS_LOG(WARN, "ob_packet serialize fail ret=%d", ret);
        }
        else if (!qthread.push(ob_packet, task_queue_size, false))
        {
          TBSYS_LOG(WARN, "submit async task to thread queue fail task_queue_size=%d, pcode=%d", 
              task_queue_size, pcode);
          ret = OB_ERROR;
        }
        else
        {
          //TBSYS_LOG(INFO, "submit async task succ pcode=%d", pcode);
        }
        if (OB_SUCCESS != ret)
        {
          packet_factory_.destroyPacket(ob_packet);
          ob_packet = NULL;
        }
      }
      return ret;
    }

    int ObUpdateServer::ups_switch_schema(const int32_t version, ObPacket *packet_orig, common::ObDataBuffer &in_buf)
    {
      int ret = OB_SUCCESS;
      CommonSchemaManagerWrapper new_schema;

      if (!(ObiRole::MASTER == obi_role_.get_role()
            && ObUpsRoleMgr::MASTER == role_mgr_.get_role()
            && ObUpsRoleMgr::ACTIVE == role_mgr_.get_state()
            && is_lease_valid()))
      {
        ret = OB_NOT_MASTER;
        TBSYS_LOG(WARN, "not master:is_lease_valid=%s", STR_BOOL(is_lease_valid()));
      }

      if (version != MY_VERSION)
      {
        TBSYS_LOG(ERROR, "Version do not match, MY_VERSION=%d version= %d",
            MY_VERSION, version);
        ret = OB_ERROR_FUNC_VERSION;
      }

      if (OB_SUCCESS == ret)
      {
        ret = new_schema.deserialize(in_buf.get_data(), in_buf.get_capacity(), in_buf.get_position());
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "deserialize schema from packet error, ret=%d buf=%p pos=%ld cap=%ld", ret, in_buf.get_data(), in_buf.get_position(), in_buf.get_capacity());
        }
      }

      if (OB_SUCCESS == ret)
      {
        TBSYS_LOG(INFO, "switch schemas");
        ret = table_mgr_.switch_schemas(new_schema);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "set_schemas failed, ret=%d", ret);
        }
      }

      if (OB_SUCCESS == ret)
      {
        TBSYS_LOG(INFO, "switch schema succ");
      }
      else
      {
        TBSYS_LOG(ERROR, "switch schema err, ret=%d schema_version=%ld", ret, new_schema.get_version());
        hex_dump(in_buf.get_data(), static_cast<int32_t>(in_buf.get_capacity()), false, TBSYS_LOG_LEVEL_ERROR);
      }

      ret = response_result_(ret, OB_SWITCH_SCHEMA_RESPONSE, MY_VERSION,
          packet_orig->get_connection(), packet_orig->getChannelId());
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "response_result_ err, ret=%d version=%d conn=%p channel_id=%u",
            ret, MY_VERSION, packet_orig->get_connection(), packet_orig->getChannelId());
      }
      return ret;
    }

    int ObUpdateServer::ups_create_memtable_index()
    {
      int ret = OB_SUCCESS;

      // memtable建索引 重复调用直接返回OB_SUCCESS
      ret = table_mgr_.create_index();
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "create index fail ret=%d", ret);
      }

      // 反序列化出timestamp
      uint64_t new_version = 0;
      if (OB_SUCCESS == ret)
      {
        ret = table_mgr_.get_active_memtable_version(new_version);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "decode new version fail ret=%d", ret);
        }
      }

      // 发送返回消息给rootserver
      int64_t retry_times = param_.get_resp_root_times();
      int64_t timeout = param_.get_resp_root_timeout_us();
      ret = RPC_CALL_WITH_RETRY(send_freeze_memtable_resp, retry_times, timeout, root_server_, ups_master_, new_version);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "send freeze memtable resp fail ret_code=%d schema_version=%ld", ret, new_version);
      }
      return ret;
    }

    int ObUpdateServer::ups_drop_memtable(const int32_t version, tbnet::Connection* conn, const uint32_t channel_id)
    {
      int ret = OB_SUCCESS;
      if (version != MY_VERSION)
      {
        ret = OB_ERROR_FUNC_VERSION;
      }
      bool force = true;
      table_mgr_.drop_memtable(force);
      ret = response_result_(ret, OB_DROP_OLD_TABLETS_RESPONSE, MY_VERSION, conn, channel_id);
      return ret;
    }

    int ObUpdateServer::ups_delay_drop_memtable(const int32_t version, tbnet::Connection* conn, const uint32_t channel_id)
    {
      int ret = OB_SUCCESS;
      if (version != MY_VERSION)
      {
        ret = OB_ERROR_FUNC_VERSION;
      }
      submit_delay_drop();
      ret = response_result_(ret, OB_UPS_DELAY_DROP_MEMTABLE_RESPONSE, MY_VERSION, conn, channel_id);
      return ret;
    }

    int ObUpdateServer::ups_immediately_drop_memtable(const int32_t version, tbnet::Connection* conn, const uint32_t channel_id)
    {
      int ret = OB_SUCCESS;
      if (version != MY_VERSION)
      {
        ret = OB_ERROR_FUNC_VERSION;
      }
      submit_immediately_drop();
      ret = response_result_(ret, OB_UPS_IMMEDIATELY_DROP_MEMTABLE_RESPONSE, MY_VERSION, conn, channel_id);
      return ret;
    }

    int ObUpdateServer::ups_drop_memtable()
    {
      bool force = true;
      table_mgr_.drop_memtable(force);
      warm_up_duty_.drop_end();
      force = false;
      table_mgr_.erase_sstable(force);
      return OB_SUCCESS;
    }

    int ObUpdateServer::ups_load_bypass(const int32_t version,
        tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff, const int pcode)
    {
      int ret = OB_SUCCESS;
      if (version != MY_VERSION)
      {
        ret = OB_ERROR_FUNC_VERSION;
      }
      int64_t loaded_num = 0;
      if (OB_SUCCESS == ret)
      {
        ret = table_mgr_.load_sstable_bypass(sstable_mgr_, loaded_num);
        submit_report_freeze();
        submit_check_cur_version();
        table_mgr_.log_table_info();
      }
      if (NULL != conn)
      {
        ret = response_data_(ret, loaded_num, pcode + 1, MY_VERSION, conn, channel_id, out_buff);
      }
      return ret;
    }

    int ObUpdateServer::ups_check_cur_version()
    {
      return table_mgr_.check_cur_version();
    }

    int ObUpdateServer::ups_erase_sstable(const int32_t version, tbnet::Connection* conn, const uint32_t channel_id)
    {
      int ret = OB_SUCCESS;
      if (version != MY_VERSION)
      {
        ret = OB_ERROR_FUNC_VERSION;
      }
      bool force = true;
      table_mgr_.erase_sstable(force);
      ret = response_result_(ret, OB_DROP_OLD_TABLETS_RESPONSE, MY_VERSION, conn, channel_id);
      return ret;
    }

    int ObUpdateServer::ups_load_new_store(const int32_t version, tbnet::Connection* conn, const uint32_t channel_id)
    {
      int ret = OB_SUCCESS;
      if (version != MY_VERSION)
      {
        ret = OB_ERROR_FUNC_VERSION;
      }
      ret = sstable_mgr_.load_new() ? OB_SUCCESS : OB_ERROR;
      ret = response_result_(ret, OB_UPS_LOAD_NEW_STORE_RESPONSE, MY_VERSION, conn, channel_id);
      return ret;
    }

    int ObUpdateServer::ups_reload_all_store(const int32_t version, tbnet::Connection* conn, const uint32_t channel_id)
    {
      int ret = OB_SUCCESS;
      if (version != MY_VERSION)
      {
        ret = OB_ERROR_FUNC_VERSION;
      }
      sstable_mgr_.reload_all();
      ret = response_result_(ret, OB_UPS_RELOAD_ALL_STORE_RESPONSE, MY_VERSION, conn, channel_id);
      return ret;
    }

    int ObUpdateServer::ups_rs_get_max_log_seq(const int32_t version, tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer &out_buff)
    {
      int err = OB_SUCCESS;
      if (MY_VERSION != version)
      {
        err = OB_ERROR_FUNC_VERSION;
      }
      int64_t log_seq = 0;
      if (OB_SUCCESS != (err = log_mgr_.get_max_log_seq_replayable(log_seq)))
      {
        TBSYS_LOG(ERROR, "log_mgr.get_max_log_seq_replayable(log_seq)=>%d", err);
      }

      if (OB_SUCCESS != (err = response_data_(err, log_seq, OB_RS_GET_MAX_LOG_SEQ_RESPONSE, MY_VERSION, conn, channel_id, out_buff)))
      {
        TBSYS_LOG(WARN, "fail to send response, err = %d", err);
      }
      return err;
    }
    int ObUpdateServer::slave_ups_receive_keep_alive(const int32_t version, tbnet::Connection* conn, const uint32_t channel_id)
    {
      int ret = OB_SUCCESS;
      UNUSED(conn);
      UNUSED(channel_id);
      if (MY_VERSION != version)
      {
        ret = OB_ERROR_FUNC_VERSION;
      }
      else
      {
        ret = OB_NOT_SUPPORTED;
        TBSYS_LOG(WARN, "ups_receive_keep_alive(): NOT NEED anymore");
      }
      return ret;
    }

    int ObUpdateServer::ups_clear_fatal_status(const int32_t version, tbnet::Connection* conn, const uint32_t channel_id)
    {
      int ret = OB_SUCCESS;
      if (MY_VERSION != version)
      {
        ret = OB_ERROR_FUNC_VERSION;
        TBSYS_LOG(ERROR, "MY_VERSION[%d] != version[%d]", MY_VERSION, version);
      }
      else
      {
        if (ObUpsRoleMgr::FATAL == role_mgr_.get_state())
        {
          role_mgr_.set_state(ObUpsRoleMgr::ACTIVE);
          TBSYS_LOG(INFO, "clear ups FATAL status succ.");
        }
        ret = response_result_(ret, OB_UPS_CLEAR_FATAL_STATUS_RESPONSE, MY_VERSION, conn, channel_id);
      }
      return ret;
    }
    int ObUpdateServer::ups_froce_report_frozen_version(const int32_t version, tbnet::Connection* conn, const uint32_t channel_id)
    {
      int ret = OB_SUCCESS;
      if (version != MY_VERSION)
      {
        ret = OB_ERROR_FUNC_VERSION;
      }
      submit_report_freeze();
      ret = response_result_(ret, OB_UPS_FORCE_REPORT_FROZEN_VERSION_RESPONSE, MY_VERSION, conn, channel_id);
      return ret;
    }

    int ObUpdateServer::ups_reload_store(const int32_t version, common::ObDataBuffer& in_buf,
        tbnet::Connection* conn, const uint32_t channel_id)
    {
      int ret = OB_SUCCESS;
      if (version != MY_VERSION)
      {
        ret = OB_ERROR_FUNC_VERSION;
      }
      StoreMgr::Handle store_handle = StoreMgr::INVALID_HANDLE;
      if (OB_SUCCESS != (ret = serialization::decode_vi64(in_buf.get_data(), in_buf.get_capacity(), in_buf.get_position(), (int64_t*)&store_handle)))
      {
        TBSYS_LOG(WARN, "decode store_handle fail ret=%d", ret);
      }
      else
      {
        TBSYS_LOG(INFO, "reload store handle=%lu", store_handle);
        sstable_mgr_.reload(store_handle);
      }
      response_result_(ret, OB_UPS_RELOAD_STORE_RESPONSE, MY_VERSION, conn, channel_id);
      return ret;
    }

    int ObUpdateServer::ups_umount_store(const int32_t version, common::ObDataBuffer& in_buff,
        tbnet::Connection* conn, const uint32_t channel_id)
    {
      int ret = OB_SUCCESS;
      if (version != MY_VERSION)
      {
        ret = OB_ERROR_FUNC_VERSION;
      }
      ObString umount_dir;
      if (OB_SUCCESS == ret)
      {
        ret = umount_dir.deserialize(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position());
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "deserialize umount dir error, ret=%d", ret);
        }
      }
      if (OB_SUCCESS == ret)
      {
        TBSYS_LOG(INFO, "umount store=[%s]", umount_dir.ptr());
        sstable_mgr_.umount_store(umount_dir.ptr());
        sstable_mgr_.check_broken();
      }
      ret = response_result_(ret, OB_UPS_UMOUNT_STORE_RESPONSE, MY_VERSION, conn, channel_id);
      return ret;
    }

    int ObUpdateServer::ups_slave_register(const int32_t version, common::ObDataBuffer& in_buff,
        tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      int err = OB_SUCCESS;

      if (version != MY_VERSION)
      {
        err = OB_ERROR_FUNC_VERSION;
      }

      uint64_t new_log_file_id = 0;
      // deserialize ups_slave
      ObSlaveInfo slave_info;
      err = slave_info.deserialize(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position());
      if (OB_SUCCESS != err)
      {
        TBSYS_LOG(WARN, "deserialize ObSlaveInfo failed, err=%d", err);
      }

      if (OB_SUCCESS == err)
      {
        TBSYS_LOG(INFO, "start log mgr add slave, obi_role=%s", obi_role_.get_role_str());
        err = log_mgr_.add_slave(slave_info.self, new_log_file_id, ObiRole::MASTER == obi_role_.get_role());
      if (OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN, "ObUpsLogMgr add_slave error, err=%d", err);
        }

        char addr_buf[ADDR_BUF_LEN];
        slave_info.self.to_string(addr_buf, sizeof(addr_buf));
        addr_buf[ADDR_BUF_LEN - 1] = '\0';
        TBSYS_LOG(INFO, "add slave, slave_addr=%s, err=%d", addr_buf, err);
      }

     // if (OB_SUCCESS == err)
     // {
     //   TBSYS_LOG(INFO, "lease = %d", param_.get_lease_on());
     //   if (LEASE_ON == param_.get_lease_on())
     //   {
     //     ObLease lease;
     //     TBSYS_LOG(INFO, "extend slave lease");
     //     err = slave_mgr_.extend_lease(slave_info.self, lease);
     //     if (OB_SUCCESS != err)
     //     {
     //       TBSYS_LOG(WARN, "failed to extend lease, err=%d", err);
     //     }
     //   }
     // }

      // reply ups slave with related info
      if (OB_SUCCESS == err)
      {
        ObUpsFetchParam fetch_param;
        fetch_param.fetch_log_ = true;
        fetch_param.fetch_ckpt_ = false;
        fetch_param.min_log_id_ = log_mgr_.get_replay_point();
        fetch_param.max_log_id_ = new_log_file_id - 1;
        err = sstable_mgr_.fill_fetch_param(slave_info.min_sstable_id,
            slave_info.max_sstable_id, param_.get_slave_sync_sstable_num(), fetch_param);
        int64_t log_id;
        log_mgr_.get_max_log_seq_replayable(log_id);
        TBSYS_LOG(INFO, "receive slave register, max_log_id=%ld", log_id);

        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN, "ObSSTableMgr fill_fetch_param error, err=%d", err);
        }
        else
        {
          err = response_fetch_param_(err, fetch_param, log_id, OB_SLAVE_REG_RES, MY_VERSION,
              conn, channel_id, out_buff);
          if (OB_SUCCESS != err)
          {
            TBSYS_LOG(WARN, "failed to response fetch param, err=%d", err);
          }
        }
      }

      return err;
    }

    int ObUpdateServer::ups_slave_quit(const int32_t version, common::ObDataBuffer& in_buff,
        tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      int err = OB_SUCCESS;

      UNUSED(out_buff);

      if (version != MY_VERSION)
      {
        err = OB_ERROR_FUNC_VERSION;
      }

      // deserialize ups_slave
      ObServer ups_slave;
      err = ups_slave.deserialize(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position());
      if (OB_SUCCESS != err)
      {
        TBSYS_LOG(WARN, "deserialize ups_slave failed, err=%d", err);
      }

      if (OB_SUCCESS == err)
      {
        err = slave_mgr_.delete_server(ups_slave);
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN, "ObSlaveMgr delete_slave error, err=%d", err);
        }

        char addr_buf[ADDR_BUF_LEN];
        ups_slave.to_string(addr_buf, sizeof(addr_buf));
        addr_buf[ADDR_BUF_LEN - 1] = '\0';
        TBSYS_LOG(INFO, "slave quit, slave_addr=%s, err=%d", addr_buf, err);
      }

      // reply ups slave
      if (OB_SUCCESS == err)
      {
        err = response_result_(err, OB_SLAVE_QUIT_RES, MY_VERSION, conn, channel_id);
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN, "failed to response slave quit, err=%d", err);
        }
      }

      return err;
    }

    int ObUpdateServer::ups_apply(const bool using_id, UpsTableMgrTransHandle& handle, common::ObDataBuffer& in_buff, ObScanner *scanner)
    {
      int ret = OB_SUCCESS;
      ObUpsMutator ups_mutator_stack;
      ObUpsMutator *ups_mutator_ptr = GET_TSI_MULT(ObUpsMutator, TSI_UPS_UPS_MUTATOR_1);
      ObUpsMutator &ups_mutator = (NULL == ups_mutator_ptr) ? ups_mutator_stack : *ups_mutator_ptr;
      ret = ups_mutator.get_mutator().deserialize(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position());
      FILL_TRACE_LOG("mutator deserialize ret=%d", ret);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "deserialize mutator fail ret=%d", ret);
      }
      else if (NULL == scanner)
      {
        TBSYS_LOG(WARN, "scanner null pointer");
        ret = OB_ERROR;
      }
      else
      {
        ret = table_mgr_.apply(using_id, handle, ups_mutator, scanner);
      }
      INC_STAT_INFO(UPS_STAT_APPLY_COUNT, 1);
      INC_STAT_INFO(UPS_STAT_APPLY_TIMEU, GET_TRACE_TIMEU());
      FILL_TRACE_LOG("ret=%d", ret);
      PRINT_TRACE_LOG();
      if (OB_SUCCESS != ret)
      {
        INC_STAT_INFO(UPS_STAT_APPLY_FAIL_COUNT, 1);
      }
      return ret;
    }

    int ObUpdateServer::ups_start_transaction(const MemTableTransType type, UpsTableMgrTransHandle& handle)
    {
      int ret = OB_SUCCESS;
      start_trans_timestamp_ = tbsys::CTimeUtil::getTime();
      ret = table_mgr_.start_transaction(type, handle);
      CLEAR_TRACE_LOG();
      FILL_TRACE_LOG("ret=%d", ret);
      return ret;
    }

    int ObUpdateServer::response_result_(int32_t ret_code, int32_t cmd_type, int32_t func_version,
        tbnet::Connection* conn, const uint32_t channel_id)
    {
      int ret = OB_SUCCESS;
      common::ObResultCode result_msg;
      ThreadSpecificBuffer::Buffer *my_buffer = my_thread_buffer_.get_buffer();
      if (NULL == my_buffer)
      {
        TBSYS_LOG(ERROR, "alloc thread buffer fail");
        ret = OB_MEM_OVERFLOW;
      }
      else
      {
        ObDataBuffer out_buff(my_buffer->current(), my_buffer->remain());
        result_msg.result_code_ = ret_code;
        ret = result_msg.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position());
        if (OB_SUCCESS == ret)
        {
          ret = send_response(cmd_type, func_version, out_buff, conn, channel_id);
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "send response fail ret=%d conn=%p channel_id=%u result_msg=%d cmd_type=%d func_version=%d",
                ret, conn, channel_id, ret_code, cmd_type, func_version);
          }
        }
        else
        {
          TBSYS_LOG(WARN, "send response fail ret=%d conn=%p channel_id=%u result_msg=%d cmd_type=%d func_version=%d",
              ret, conn, channel_id, ret_code, cmd_type, func_version);
        }
      }
      return ret;
    }

    int ObUpdateServer::ups_end_transaction(tbnet::Packet** packets, ScannerArray &scanner_array, const int64_t start_idx,
        const int64_t last_idx, UpsTableMgrTransHandle& handle, int32_t last_err_code)
    {
      bool rollback = false;
      int proc_ret = table_mgr_.end_transaction(handle, rollback);
      int resp_ret = (OB_SUCCESS == proc_ret) ? proc_ret : OB_RESPONSE_TIME_OUT;
      ObPacket **ob_packets = reinterpret_cast<ObPacket**>(packets);
      ThreadSpecificBuffer::Buffer *my_buffer = my_thread_buffer_.get_buffer();

      if (NULL == ob_packets
          || NULL == my_buffer)
      {
        TBSYS_LOG(WARN, "ob_packet or my_buffer null pointer start_idx=%ld last_idx=%ld", start_idx, last_idx);
      }
      else
      {
        int tmp_ret = OB_SUCCESS;
        for (int64_t i = start_idx; i < last_idx; i++)
        {
          if (NULL == ob_packets[i])
          {
            TBSYS_LOG(WARN, "ob_packet[%ld] null pointer, start_idx=%ld last_idx=%ld", i, start_idx, last_idx);
          }
          else if (NULL != scanner_array[i]
                  && 0 != scanner_array[i]->get_size())
          {
            ObDataBuffer out_buff(my_buffer->current(), my_buffer->remain());
            tmp_ret = response_data_(resp_ret, *(scanner_array[i]), OB_WRITE_RES, MY_VERSION,
                                      ob_packets[i]->get_connection(), ob_packets[i]->getChannelId(), out_buff);
            TBSYS_LOG(DEBUG, "response result index=%ld send_ret=%d resp_ret=%d proc_ret=%d", i, tmp_ret, resp_ret, proc_ret);
          }
          else
          {
            tmp_ret = response_result_(resp_ret, OB_WRITE_RES, MY_VERSION,
                ob_packets[i]->get_connection(), ob_packets[i]->getChannelId());
            TBSYS_LOG(DEBUG, "response result index=%ld send_ret=%d resp_ret=%d proc_ret=%d", i, tmp_ret, resp_ret, proc_ret);
          }
        }

        if (NULL == ob_packets[last_idx])
        {
          TBSYS_LOG(WARN, "last ob_packet[%ld] null pointer, start_idx=%ld", last_idx, start_idx);
        }
        else if (NULL != scanner_array[last_idx]
                && 0 != scanner_array[last_idx]->get_size())
        {
          ObDataBuffer out_buff(my_buffer->current(), my_buffer->remain());
          tmp_ret = response_data_(resp_ret, *(scanner_array[last_idx]), OB_WRITE_RES, MY_VERSION,
                                    ob_packets[last_idx]->get_connection(), ob_packets[last_idx]->getChannelId(), out_buff);
          TBSYS_LOG(DEBUG, "response result index=%ld send_ret=%d resp_ret=%d proc_ret=%d", last_idx, tmp_ret, resp_ret, proc_ret);
        }
        else
        {
          resp_ret = (OB_SUCCESS == last_err_code) ? resp_ret : last_err_code;
          // 最后一个如果成功则返回提交的结果 如果失败则返回apply的结果
          tmp_ret = response_result_(resp_ret, OB_WRITE_RES, MY_VERSION,
              ob_packets[last_idx]->get_connection(), ob_packets[last_idx]->getChannelId());
          TBSYS_LOG(DEBUG, "response result index=%ld send_ret=%d resp_ret=%d proc_ret=%d", last_idx, tmp_ret, resp_ret, proc_ret);
        }
      }

      int64_t trans_proc_time = tbsys::CTimeUtil::getTime() - start_trans_timestamp_;
      static __thread int64_t counter = 0;
      counter++;
      if (trans_proc_time > param_.get_trans_proc_time_warn_us())
      {
        TBSYS_LOG(WARN, "transaction process time is too long, process_time=%ld cur_time=%ld response_num=%ld "
            "last_log_network_elapse=%ld last_log_disk_elapse=%ld trans_counter=%ld "
            "read_task_queue_size=%zu write_task_queue_size=%zu lease_task_queue_size=%zu log_task_queue_size=%zu",
            trans_proc_time, tbsys::CTimeUtil::getTime(), last_idx - start_idx + 1,
            log_mgr_.get_last_net_elapse(), log_mgr_.get_last_disk_elapse(), counter,
            read_thread_queue_.size(), write_thread_queue_.size(), lease_thread_queue_.size(), log_thread_queue_.size());
        counter = 0;
      }

      INC_STAT_INFO(UPS_STAT_BATCH_COUNT, 1);
      INC_STAT_INFO(UPS_STAT_BATCH_TIMEU, GET_TRACE_TIMEU());
      FILL_TRACE_LOG("resp_ret=%d proc_ret=%d", resp_ret, proc_ret);
      PRINT_TRACE_LOG();
      return proc_ret;
    }

    void ObUpdateServer::set_log_sync_delay_stat_param()
    {
      TBSYS_LOG(INFO, "set_log_sync_delay_stat_param()");
      log_mgr_.get_delay_stat().set_delay_warn_time_us(param_.get_log_sync_delay_warn_time_threshold_us());
      log_mgr_.get_delay_stat().set_report_interval_us(param_.get_log_sync_delay_warn_report_interval_us());
      log_mgr_.get_delay_stat().set_max_n_lagged_log_allowed(param_.get_max_n_lagged_log_allowed());
    }

    void ObUpdateServer::set_log_replay_thread_param()
    {
      TBSYS_LOG(INFO, "set_log_replay_thread_param()");
      log_replay_thread_.set_replay_wait_time_us(param_.get_replay_wait_time_us());
      log_replay_thread_.set_fetch_log_wait_time_us(param_.get_fetch_log_wait_time_us());
    }

    int ObUpdateServer::ups_reload_conf(const int32_t version, common::ObDataBuffer& in_buff,
        tbnet::Connection* conn, const uint32_t channel_id)
    {
      int ret = OB_SUCCESS;
      ObString conf_file;

      if (version != MY_VERSION)
      {
        ret = OB_ERROR_FUNC_VERSION;
      }
      else
      {
        ret = conf_file.deserialize(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position());
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "deserialize conf file error, ret=%d", ret);
        }
      }

      if (OB_SUCCESS == ret)
      {
        ret = param_.reload_from_config(conf_file);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "failed to reload config, ret=%d", ret);
        }
        else
        {
          set_log_replay_thread_param();
          set_log_sync_delay_stat_param();
          ob_set_memory_size_limit(param_.get_total_memory_limit());
          MemTableAttr memtable_attr;
          if (OB_SUCCESS == table_mgr_.get_memtable_attr(memtable_attr))
          {
            memtable_attr.total_memlimit = param_.get_table_memory_limit();
            table_mgr_.set_memtable_attr(memtable_attr);
          }
          if (param_.get_low_priv_cur_percent() >= 0)
          {
            read_thread_queue_.set_low_priv_cur_percent(param_.get_low_priv_cur_percent());
          }
        }
      }
      ret = response_result_(ret, OB_UPS_RELOAD_CONF_RESPONSE, MY_VERSION, conn, channel_id);

      return ret;
    }


    int ObUpdateServer::slave_report_quit()
    {
      int err = OB_SUCCESS;
      if (ObUpsRoleMgr::MASTER == role_mgr_.get_role()
          && ObiRole::MASTER == obi_role_.get_role())
      {
        //do nothing
      }
      else if (ObUpsRoleMgr::SLAVE == role_mgr_.get_role())
      {
        if (OB_SUCCESS != (err = ups_rpc_stub_.slave_quit(ups_master_, get_self(), DEFAULT_SLAVE_QUIT_TIMEOUT)))
        {
          TBSYS_LOG(WARN, "fail to send slave quit to master. err = %d, master = %s", err, ups_master_.to_cstring());
        }
      }
      else
      {
        if (OB_SUCCESS != (err = ups_rpc_stub_.slave_quit(ups_inst_master_, get_self(), DEFAULT_SLAVE_QUIT_TIMEOUT)))
        {
          TBSYS_LOG(WARN, "fail to send slave quit to master. err = %d, master = %s", err, ups_inst_master_.to_cstring());
        }
      }

      return err;
    }
    int ObUpdateServer::ups_update_lease(const common::ObMsgUpsHeartbeat &hb)
    {
      int err = OB_SUCCESS;
      int64_t cur_time_us = tbsys::CTimeUtil::getTime();
      TBSYS_LOG(DEBUG, "receive hb, master_addr_=%s, self_lease=%ld, obi_role=%s, cur_time=%ld, lease_time=%ld",
          hb.ups_master_.to_cstring(), hb.self_lease_, hb.obi_role_.get_role_str(), cur_time_us, hb.self_lease_ - cur_time_us);

      if (hb.self_lease_ == OB_MAX_UPS_LEASE_DURATION_US)
      {
        lease_expire_time_us_ = hb.self_lease_;
        TBSYS_LOG(INFO, "rootserver down, receive the overlength lease. lease_time=%ld", OB_MAX_UPS_LEASE_DURATION_US);
      }
      else if (hb.self_lease_ >= lease_expire_time_us_)
      {
        lease_expire_time_us_ = hb.self_lease_;
      }
      else if (OB_MAX_UPS_LEASE_DURATION_US == lease_expire_time_us_)
      {
        TBSYS_LOG(INFO, "maybe rootserver restarted, receive normal lease.lease=%ld", hb.self_lease_);
        lease_expire_time_us_ = hb.self_lease_;
      }
      else
      {
        err = OB_ERROR;
        TBSYS_LOG(WARN, "lease should not rollback, self_lease=%ld, new_lease=%ld",
            lease_expire_time_us_, hb.self_lease_);
      }

      bool is_role_change = false;
      bool is_obi_change = false;

      if (OB_SUCCESS == err)
      {
        if ((ObiRole::INIT != hb.obi_role_.get_role())
            && obi_role_.get_role() != hb.obi_role_.get_role())
        {
          TBSYS_LOG(INFO, "UPS obi_role change. obi_role=%s", hb.obi_role_.get_role_str());
          is_obi_change = true;
        }
        if (!(hb.ups_master_ == ups_master_))
        {
          TBSYS_LOG(INFO, "master_ups addr has been change. old_master=%s", ups_master_.to_cstring());
          TBSYS_LOG(INFO, "new master addr =%s", hb.ups_master_.to_cstring());
          ups_master_.set_ipv4_addr(hb.ups_master_.get_ipv4(), hb.ups_master_.get_port());
        }

        if (ObUpsRoleMgr::MASTER == role_mgr_.get_role() && !(hb.ups_master_ == self_addr_))
        {
          TBSYS_LOG(INFO, "UPS role change, master ups need change to slave. new_master=%s",
              hb.ups_master_.to_cstring());
          is_role_change = true;
        }
        else if (ObUpsRoleMgr::SLAVE == role_mgr_.get_role() && (hb.ups_master_ == self_addr_))
        {
          TBSYS_LOG(INFO, "UPS role change, slave ups need change to master.");
          is_role_change = true;
        }
      }

      if (OB_SUCCESS == err)
     {
       if (is_obi_change && is_role_change)
       {
         if (ObiRole::MASTER == obi_role_.get_role()
             && ObUpsRoleMgr::MASTER == role_mgr_.get_role())
         {
           TBSYS_LOG(INFO, "switch happen. master_master ====> slave_slave");
           err = master_switch_to_slave(true, true);
         }
         else if (ObiRole::SLAVE == obi_role_.get_role()
             && ObUpsRoleMgr::SLAVE == role_mgr_.get_role())
         {
           TBSYS_LOG(INFO, "switch happen. slave_slave ===> master_master");
           err = switch_to_master_master();
         }
         else
         {
           TBSYS_LOG(INFO, "switch happen.");
           err = slave_change_role(true, true);
           if (OB_SUCCESS != err)
           {
             TBSYS_LOG(INFO, "change obi_role and role_mgr_ fail.");
           }
         }
       }
       else if (true == is_obi_change && false == is_role_change)
       {
         if (ObUpsRoleMgr::MASTER == role_mgr_.get_role())
         {
           if (ObiRole::MASTER == obi_role_.get_role())
           {
             TBSYS_LOG(INFO, "switch happen. master_master ====> slave_master");
             err = master_switch_to_slave(true, false);
           }
           else
           {
             TBSYS_LOG(INFO, "switch happen. slave_master ===> master_master");
             err = switch_to_master_master();
           }
         }
         else
         {
           TBSYS_LOG(INFO, "switch happen.");
           err = slave_change_role(true, false);
           if (OB_SUCCESS != err)
           {
             TBSYS_LOG(INFO, "change obi_role and role_mgr_ fail.");
           }
         }
       }
       else if (false == is_obi_change && true == is_role_change)
       {
         if (ObiRole::MASTER == obi_role_.get_role())
         {
           if (ObUpsRoleMgr::MASTER == role_mgr_.get_role())
           {
             TBSYS_LOG(INFO, "switch happen. master_master ===> master_slave");
             err = master_switch_to_slave(false, true);
           }
           else
           {
             TBSYS_LOG(INFO, "switch happen. master_slave ===> master_master");
             err = switch_to_master_master();
           }
         }
         else
         {
           err = slave_change_role(false, true);
         }
       }
     }
      return err;
    }

    int ObUpdateServer::ups_revoke_lease(const ObMsgRevokeLease &revoke_info)
    {
      int err = OB_SUCCESS;
      if (revoke_info.lease_ == lease_expire_time_us_ && self_addr_ == revoke_info.ups_master_)
      {
        TBSYS_LOG(INFO, "revoke info check succ. translate to slave");
        ups_master_.reset();
        if (ObiRole::MASTER == obi_role_.get_role())
        {
          TBSYS_LOG(INFO, "switch happen. master_master ====> master_slave");
          err = master_switch_to_slave(false, true);
        }
        else
        {
          TBSYS_LOG(INFO, "switch happen. slave_master ====> slave_slave");
          err = slave_change_role(false, true);
        }
      }
      else
      {
        TBSYS_LOG(INFO, "revoke info check false, refuse to translate to slave");
        err = OB_ERROR;
      }
      return err;
    }

    int ObUpdateServer::ups_rs_revoke_lease(const int32_t version, common::ObDataBuffer& in_buff,
        tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      int err = OB_SUCCESS;
      common::ObResultCode result_msg;
      result_msg.result_code_ = OB_SUCCESS;
      if (MY_VERSION != version)
      {
        TBSYS_LOG(WARN, "version not equal. version=%d, MY_VERSION=%d", version, MY_VERSION);
        result_msg.result_code_ = OB_ERROR_FUNC_VERSION;
      }
      ObMsgRevokeLease revoke_info;
      if (OB_SUCCESS == err && OB_SUCCESS == result_msg.result_code_)
      {
        err = revoke_info.deserialize(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position());
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN, "failed to serialize hb_info, err=%d", err);
        }
      }
      //根据
      if (OB_SUCCESS == err && OB_SUCCESS == result_msg.result_code_)
      {
        result_msg.result_code_ = ups_revoke_lease(revoke_info);
        if (OB_SUCCESS != result_msg.result_code_)
        {
          TBSYS_LOG(WARN, "fail to revoke lease, err = %d", result_msg.result_code_);
        }
      }
      if (OB_SUCCESS == err)
      {
        result_msg.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position());
        if(OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN, "fail to serialize, err=%d", err);
        }
      }

      if (OB_SUCCESS == err)
      {
        err = send_response(OB_RS_UPS_REVOKE_LEASE_RESPONSE, MY_VERSION, out_buff, conn, channel_id);
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN, "fail to send response. err = %d", err);
        }
      }

      return err;
    }

    bool ObUpdateServer::get_service_state()
    {
      bool is_provide_service = false;
      //从内部表中取出一致性级别

      if (ObConsistencyType::STRONG_CONSISTENCY == consistency_type_.get_consistency_type()
          && ObiRole::MASTER == obi_role_.get_role()
          && ObUpsRoleMgr::MASTER == role_mgr_.get_role()
          && ObUpsRoleMgr::ACTIVE == role_mgr_.get_state())
      {
        is_provide_service = true;
      }
      else if (ObConsistencyType::NORMAL_CONSISTENCY == consistency_type_.get_consistency_type()
          && ObUpsRoleMgr::ACTIVE == role_mgr_.get_state())
      {
        is_provide_service = true;
      }
      else if (ObConsistencyType::WEAK_CONSISTENCY == consistency_type_.get_consistency_type())
      {
        is_provide_service = true;
      }
      return is_provide_service;
    }

    //add :rs ups hb
    int ObUpdateServer::ups_rs_lease(const int32_t version, common::ObDataBuffer& in_buff,
        tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      int err = OB_SUCCESS;
      UNUSED(channel_id);
      UNUSED(conn);

      ObMsgUpsHeartbeat hb;
      if (hb.MY_VERSION != version)
      {
        TBSYS_LOG(WARN, "version not equal. version=%d, MY_VERSION=%d", version, hb.MY_VERSION);
        err = OB_ERROR_FUNC_VERSION;
      }
      if (OB_SUCCESS == err)
      {
        err = hb.deserialize(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position());
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN, "failed to deserialize hb_info, err=%d", err);
        }
      }
      //根据心跳内容，进行处理
      if (OB_SUCCESS == err)
      {
        err = ups_update_lease(hb);
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN, "fail to update lease. err=%d", err);
        }
      }

      if (OB_ERROR_FUNC_VERSION != err)
      {
        ObMsgUpsHeartbeatResp hb_res;
        set_heartbeat_res(hb_res);
        err = hb_res.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position());
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN, "fail to serialize hb_res");
        }
        if (OB_SUCCESS == err)
        {
          err = client_manager_.post_request(root_server_, OB_RS_UPS_HEARTBEAT_RESPONSE, hb_res.MY_VERSION, out_buff);
          if (OB_SUCCESS != err)
          {
            TBSYS_LOG(WARN, "fail to send response. err = %d", err);
          }
        }
      }
      return err;
    }

    /*  int ObUpdateServer::ups_change_vip(const int32_t version, common::ObDataBuffer& in_buff,
        tbnet::Connection* conn, const uint32_t channel_id)
        {
        int ret = OB_SUCCESS;

        if (version != MY_VERSION)
        {
        ret = OB_ERROR_FUNC_VERSION;
        }

        int32_t new_vip = 0;
        if (OB_SUCCESS == ret)
        {
        ret = serialization::decode_i32(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position(), &new_vip);
        if (OB_SUCCESS != ret)
        {
        TBSYS_LOG(WARN, "failed to decode vip, ret=%d", ret);
        }
        }

        if (OB_SUCCESS == ret)
        {
        check_thread_.reset_vip(new_vip);
        slave_mgr_.reset_vip(new_vip);
        }

        ret = response_result_(ret, OB_UPS_CHANGE_VIP_RESPONSE, MY_VERSION, conn, channel_id);

        return ret;
        }
     */
    int ObUpdateServer::ups_dump_text_memtable(const int32_t version, common::ObDataBuffer& in_buff,
        tbnet::Connection* conn, const uint32_t channel_id)
    {
      int ret = OB_SUCCESS;
      if (version != MY_VERSION)
      {
        ret = OB_ERROR_FUNC_VERSION;
      }
      ObString dump_dir;
      if (OB_SUCCESS == ret)
      {
        ret = dump_dir.deserialize(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position());
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "deserialize dump dir error, ret=%d", ret);
        }
        else
        {
          TBSYS_LOG(INFO, "dumping memtables to dir=[%.*s]", dump_dir.length(), dump_dir.ptr());
        }
      }
      response_result_(ret, OB_UPS_DUMP_TEXT_MEMTABLE_RESPONSE, MY_VERSION, conn, channel_id);
      if (OB_SUCCESS == ret)
      {
        table_mgr_.dump_memtable(dump_dir);
      }
      return ret;
    }

    int ObUpdateServer::ups_dump_text_schemas(const int32_t version,
        tbnet::Connection* conn, const uint32_t channel_id)
    {
      int ret = OB_SUCCESS;
      if (version != MY_VERSION)
      {
        ret = OB_ERROR_FUNC_VERSION;
      }
      if (OB_SUCCESS == ret)
      {
        table_mgr_.dump_schemas();
      }

      if (OB_SUCCESS == ret)
      {
        ob_print_mod_memory_usage();
      }

      ret = response_result_(ret, OB_UPS_DUMP_TEXT_SCHEMAS_RESPONSE, MY_VERSION, conn, channel_id);
      return ret;
    }

    int ObUpdateServer::ups_force_fetch_schema(const int32_t version,
        tbnet::Connection* conn, const uint32_t channel_id)
    {
      int ret = OB_SUCCESS;
      if (version != MY_VERSION)
      {
        ret = OB_ERROR_FUNC_VERSION;
      }
      if (OB_SUCCESS == ret)
      {
        bool write_log = true;
        ret = update_schema(false, write_log);
      }
      ret = response_result_(ret, OB_UPS_FORCE_FETCH_SCHEMA_RESPONSE, MY_VERSION, conn, channel_id);
      return ret;
    }

    int ObUpdateServer::ups_memory_watch(const int32_t version,
        tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      int ret = OB_SUCCESS;
      if (version != MY_VERSION)
      {
        ret = OB_ERROR_FUNC_VERSION;
      }
      UpsMemoryInfo memory_info;
      if (OB_SUCCESS == ret)
      {
        memory_info.total_size = ob_get_memory_size_handled();
        memory_info.cur_limit_size = ob_get_memory_size_limit();
        table_mgr_.get_memtable_memory_info(memory_info.table_mem_info);
        table_mgr_.log_table_info();
        ob_print_mod_memory_usage();
      }
      ret = response_data_(ret, memory_info, OB_UPS_MEMORY_WATCH_RESPONSE, MY_VERSION,
          conn, channel_id, out_buff);
      return ret;
    }

    int ObUpdateServer::ups_memory_limit_set(const int32_t version, common::ObDataBuffer& in_buff,
        tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      int ret = OB_SUCCESS;
      if (version != MY_VERSION)
      {
        ret = OB_ERROR_FUNC_VERSION;
      }
      UpsMemoryInfo memory_info;
      if (OB_SUCCESS == ret)
      {
        ret = memory_info.deserialize(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position());
        if (OB_SUCCESS == ret)
        {
          ob_set_memory_size_limit(memory_info.cur_limit_size);
          MemTableAttr memtable_attr;
          if (OB_SUCCESS == table_mgr_.get_memtable_attr(memtable_attr))
          {
            memtable_attr.total_memlimit = memory_info.table_mem_info.memtable_limit;
            table_mgr_.set_memtable_attr(memtable_attr);
          }
        }
        memory_info.total_size = ob_get_memory_size_handled();
        memory_info.cur_limit_size = ob_get_memory_size_limit();
        table_mgr_.get_memtable_memory_info(memory_info.table_mem_info);
        table_mgr_.log_table_info();
      }
      ret = response_data_(ret, memory_info, OB_UPS_MEMORY_LIMIT_SET_RESPONSE, MY_VERSION,
          conn, channel_id, out_buff);
      return ret;
    }

    int ObUpdateServer::ups_priv_queue_conf_set(const int32_t version, common::ObDataBuffer& in_buff,
        tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      int ret = OB_SUCCESS;
      if (version != MY_VERSION)
      {
        ret = OB_ERROR_FUNC_VERSION;
      }

      UpsPrivQueueConf priv_queue_conf;
      if (OB_SUCCESS == ret)
      {
        ret = priv_queue_conf.deserialize(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position());
        if (OB_SUCCESS == ret)
        {
          param_.set_priv_queue_conf(priv_queue_conf);
          if (param_.get_low_priv_cur_percent() >= 0)
          {
            read_thread_queue_.set_low_priv_cur_percent(param_.get_low_priv_cur_percent());
          }
        }
        priv_queue_conf.low_priv_network_lower_limit = param_.get_low_priv_network_lower_limit();
        priv_queue_conf.low_priv_network_upper_limit = param_.get_low_priv_network_upper_limit();
        priv_queue_conf.low_priv_adjust_flag = param_.get_low_priv_adjust_flag();
        priv_queue_conf.low_priv_cur_percent = param_.get_low_priv_cur_percent();
      }

      ret = response_data_(ret, priv_queue_conf, OB_UPS_PRIV_QUEUE_CONF_SET_RESPONSE, MY_VERSION,
          conn, channel_id, out_buff);
      return ret;
    }

    int ObUpdateServer::ups_clear_active_memtable(const int32_t version,
        tbnet::Connection* conn, const uint32_t channel_id)
    {
      int ret = OB_SUCCESS;
      if (!(ObiRole::MASTER == obi_role_.get_role()
            && ObUpsRoleMgr::MASTER == role_mgr_.get_role()
            && ObUpsRoleMgr::ACTIVE == role_mgr_.get_state()
            && is_lease_valid()))
      {
        ret = OB_NOT_MASTER;
        TBSYS_LOG(WARN, "not master:is_lease_valid=%s", STR_BOOL(is_lease_valid()));
      }
      if (version != MY_VERSION)
      {
        ret = OB_ERROR_FUNC_VERSION;
      }
      if (OB_SUCCESS == ret)
      {
        ret = table_mgr_.clear_active_memtable();
      }
      ret = response_result_(ret, OB_UPS_CLEAR_ACTIVE_MEMTABLE_RESPONSE, MY_VERSION, conn, channel_id);
      return ret;
    }

    int ObUpdateServer::ups_switch_commit_log(const int32_t version,
        tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      int ret = OB_SUCCESS;
      if (!(ObiRole::MASTER == obi_role_.get_role()
            && ObUpsRoleMgr::MASTER == role_mgr_.get_role()
            && ObUpsRoleMgr::ACTIVE == role_mgr_.get_state()
            && is_lease_valid()))
      {
        ret = OB_NOT_MASTER;
        TBSYS_LOG(WARN, "not master:is_lease_valid=%s", STR_BOOL(is_lease_valid()));
      }
      if (version != MY_VERSION)
      {
        ret = OB_ERROR_FUNC_VERSION;
      }
      uint64_t new_log_file_id = 0;
      int proc_ret = OB_SUCCESS;
      if (OB_SUCCESS == ret)
      {
        proc_ret = log_mgr_.switch_log_file(new_log_file_id);
        TBSYS_LOG(INFO, "switch log file id ret=%d new_log_file_id=%lu", ret, new_log_file_id);
      }
      ret = response_data_(proc_ret, new_log_file_id, OB_UPS_SWITCH_COMMIT_LOG_RESPONSE, MY_VERSION,
          conn, channel_id, out_buff);
      return ret;
    }
    int ObUpdateServer::ups_get_slave_info(const int32_t version,
        tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      int ret = OB_SUCCESS;
      common::ObResultCode result;
      result.result_code_ = OB_SUCCESS;
      if (version != MY_VERSION)
      {
        result.result_code_ = OB_ERROR_FUNC_VERSION;
      }

      if (OB_SUCCESS == ret)
      {
        ret = result.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position());
      }
      if (OB_SUCCESS == ret && OB_SUCCESS == result.result_code_)
      {
        slave_mgr_.print(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position());
      }
      if (OB_SUCCESS == ret)
      {
        ret = send_response(OB_UPS_GET_SLAVE_INFO_RESPONSE, MY_VERSION, out_buff, conn, channel_id);
      }
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "get ups slave info fail .err=%d", ret);
      }
      return ret;
    }
    //*/

    int ObUpdateServer::ups_get_last_frozen_version(const int32_t version,
        tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      int ret = OB_SUCCESS;
      if (version != MY_VERSION)
      {
        ret = OB_ERROR_FUNC_VERSION;
      }
      uint64_t last_frozen_memtable_version = 0;
      if (OB_SUCCESS == ret)
      {
        ret = table_mgr_.get_last_frozen_memtable_version(last_frozen_memtable_version);
        TBSYS_LOG(INFO, "ups get last frozen version[%ld]", last_frozen_memtable_version);
      }
      ret = response_data_(ret, last_frozen_memtable_version, OB_UPS_GET_LAST_FROZEN_VERSION_RESPONSE, MY_VERSION,
          conn, channel_id, out_buff);
      TBSYS_LOG(INFO, "rs get last frozeon version, version=%lu ret=%d",
                last_frozen_memtable_version, ret);
      return ret;
    }

    int ObUpdateServer::ups_get_table_time_stamp(const int32_t version, common::ObDataBuffer& in_buff,
        tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      int ret = OB_SUCCESS;
      if (version != MY_VERSION)
      {
        ret = OB_ERROR_FUNC_VERSION;
      }
      int proc_ret = OB_SUCCESS;
      uint64_t major_version = 0;
      int64_t time_stamp = 0;
      if (OB_SUCCESS != (proc_ret = serialization::decode_vi64(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position(), (int64_t*)&major_version)))
      {
        TBSYS_LOG(WARN, "decode major_version fail ret=%d", ret);
      }
      else
      {
        proc_ret = table_mgr_.get_table_time_stamp(major_version, time_stamp);
      }
      TBSYS_LOG(INFO, "get_table_time_stamp ret=%d major_version=%lu time_stamp=%ld src=%s",
                proc_ret, major_version, time_stamp, NULL == conn ? NULL : inet_ntoa_r(conn->getPeerId()));
      ret = response_data_(proc_ret, time_stamp, OB_UPS_GET_TABLE_TIME_STAMP_RESPONSE, MY_VERSION,
          conn, channel_id, out_buff);
      return ret;
    }

    int ObUpdateServer::ups_enable_memtable_checksum(const int32_t version, tbnet::Connection* conn, const uint32_t channel_id)
    {
      int ret = OB_SUCCESS;
      if (version != MY_VERSION)
      {
        ret = OB_ERROR_FUNC_VERSION;
      }
      if (OB_SUCCESS == ret)
      {
        table_mgr_.set_replay_checksum_flag(true);
      }
      ret = response_result_(ret, OB_UPS_ENABLE_MEMTABLE_CHECKSUM_RESPONSE, MY_VERSION, conn, channel_id);
      return ret;
    }

    int ObUpdateServer::ups_disable_memtable_checksum(const int32_t version, tbnet::Connection* conn, const uint32_t channel_id)
    {
      int ret = OB_SUCCESS;
      if (version != MY_VERSION)
      {
        ret = OB_ERROR_FUNC_VERSION;
      }
      if (OB_SUCCESS == ret)
      {
        table_mgr_.set_replay_checksum_flag(false);
      }
      ret = response_result_(ret, OB_UPS_DISABLE_MEMTABLE_CHECKSUM_RESPONSE, MY_VERSION, conn, channel_id);
      return ret;
    }

    int ObUpdateServer::ups_fetch_stat_info(const int32_t version,
        tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      int ret = OB_SUCCESS;
      if (version != MY_VERSION)
      {
        ret = OB_ERROR_FUNC_VERSION;
      }
      if (OB_SUCCESS == ret)
      {
        SET_STAT_INFO(UPS_STAT_MEMORY_TOTAL, ob_get_memory_size_handled());
        SET_STAT_INFO(UPS_STAT_MEMORY_LIMIT, ob_get_memory_size_limit());
        table_mgr_.update_memtable_stat_info();
      }
      ret = response_data_(ret, stat_mgr_, OB_FETCH_STATS_RESPONSE, MY_VERSION,
          conn, channel_id, out_buff);
      return ret;
    }

    int ObUpdateServer::ups_get_schema(const int32_t version, common::ObDataBuffer& in_buff,
        tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      int ret = OB_SUCCESS;
      if (version != MY_VERSION)
      {
        ret = OB_ERROR_FUNC_VERSION;
      }
      int proc_ret = OB_SUCCESS;
      uint64_t major_version = 0;
      CommonSchemaManagerWrapper sm;
      if (OB_SUCCESS != (proc_ret = serialization::decode_vi64(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position(), (int64_t*)&major_version)))
      {
        TBSYS_LOG(WARN, "decode major_version fail ret=%d", ret);
      }
      else
      {
        proc_ret = table_mgr_.get_schema(major_version, sm);
      }
      TBSYS_LOG(INFO, "get_schema ret=%d major_version=%lu src=%s",
                proc_ret, major_version, NULL == conn ? NULL : inet_ntoa_r(conn->getPeerId()));
      ret = response_data_(proc_ret, sm, OB_FETCH_SCHEMA_RESPONSE, MY_VERSION,
          conn, channel_id, out_buff);
      return ret;
    }

    int ObUpdateServer::ups_get_sstable_range_list(const int32_t version, common::ObDataBuffer& in_buff,
        tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      int ret = OB_SUCCESS;
      if (version != MY_VERSION)
      {
        ret = OB_ERROR_FUNC_VERSION;
      }
      int proc_ret = OB_SUCCESS;
      uint64_t major_version = 0;
      uint64_t table_id = 0;
      TabletInfoList ti_list;
      if (OB_SUCCESS != (proc_ret = serialization::decode_vi64(in_buff.get_data(), in_buff.get_capacity(),
                        in_buff.get_position(), (int64_t*)&major_version)))
      {
        TBSYS_LOG(WARN, "decode major_version fail ret=%d", ret);
      }
      else if (OB_SUCCESS != (proc_ret = serialization::decode_vi64(in_buff.get_data(), in_buff.get_capacity(),
                              in_buff.get_position(), (int64_t*)&table_id)))
      {
        TBSYS_LOG(WARN, "decode major_version fail ret=%d", ret);
      }
      else
      {
        proc_ret = table_mgr_.get_sstable_range_list(major_version, table_id, ti_list);
      }
      TBSYS_LOG(INFO, "get_sstable_range_list ret=%d major_version=%lu table_id=%lu src=%s",
                proc_ret, major_version, table_id, NULL == conn ? NULL : inet_ntoa_r(conn->getPeerId()));
      ret = response_data_(proc_ret, ti_list.inst, OB_RS_FETCH_SPLIT_RANGE_RESPONSE, MY_VERSION,
          conn, channel_id, out_buff);
      return ret;
    }

    int ObUpdateServer::low_priv_speed_control_(const int64_t scanner_size)
    {
      int ret = OB_SUCCESS;
      static volatile int64_t s_stat_times = 0;
      static volatile int64_t s_stat_size = 0;
      static volatile int64_t s_last_stat_time_ms = tbsys::CTimeUtil::getTime() / 1000L;
      static volatile int32_t flag = 0;

      if (scanner_size < 0)
      {
        TBSYS_LOG(WARN, "invalid param, scanner_size=%ld", scanner_size);
        ret = OB_ERROR;
      }
      else
      {
        atomic_inc((volatile uint64_t*) &s_stat_times);
        atomic_add((volatile uint64_t*) &s_stat_size, scanner_size);

        if (s_stat_times >= SEG_STAT_TIMES || s_stat_size >= SEG_STAT_SIZE * 1024L * 1024L)
        {
          if (atomic_compare_exchange((volatile uint32_t*) &flag, 1, 0) == 0)
          {
            // only one thread is allowed to adjust network limit
            int64_t cur_time_ms = tbsys::CTimeUtil::getTime() / 1000L;

            TBSYS_LOG(DEBUG, "stat_size=%ld cur_time_ms=%ld last_stat_time_ms=%ld", s_stat_size,
                cur_time_ms, s_last_stat_time_ms);

            int64_t adjust_flag = param_.get_low_priv_adjust_flag();

            if (1 == adjust_flag) // auto adjust low priv percent
            {
              int64_t lower_limit = param_.get_low_priv_network_lower_limit() * 1024L * 1024L;
              int64_t upper_limit = param_.get_low_priv_network_upper_limit() * 1024L * 1024L;

              int64_t low_priv_percent = read_thread_queue_.get_low_priv_cur_percent();

              if (s_stat_size * 1000L < lower_limit * (cur_time_ms - s_last_stat_time_ms))
              {
                if (low_priv_percent < PriorityPacketQueueThread::LOW_PRIV_MAX_PERCENT)
                {
                  ++low_priv_percent;
                  read_thread_queue_.set_low_priv_cur_percent(low_priv_percent);

                  TBSYS_LOG(INFO, "network lower limit, lower_limit=%ld, low_priv_percent=%ld",
                      lower_limit, low_priv_percent);
                }
              }
              else if (s_stat_size * 1000L > upper_limit * (cur_time_ms - s_last_stat_time_ms))
              {
                if (low_priv_percent > PriorityPacketQueueThread::LOW_PRIV_MIN_PERCENT)
                {
                  --low_priv_percent;
                  read_thread_queue_.set_low_priv_cur_percent(low_priv_percent);

                  TBSYS_LOG(INFO, "network upper limit, upper_limit=%ld, low_priv_percent=%ld",
                      upper_limit, low_priv_percent);
                }
              }
            }

            // reset stat_times, stat_size and last_stat_time
            s_stat_times = 0;
            s_stat_size = 0;
            s_last_stat_time_ms = cur_time_ms;

            flag = 0;
          }
        }
      }
      return ret;
    }
    void ObUpdateServer::RefreshLsyncAddrDuty::runTimerTask()
    {
      int err = OB_SUCCESS;
      ObUpdateServerMain *ups_main = ObUpdateServerMain::get_instance();
      if (NULL == ups_main)
      {
        TBSYS_LOG(WARN, "get ups_main fail");
      }
      else
      {
        ObUpdateServer &ups = ups_main->get_update_server();
        if (OB_SUCCESS != (err = ups.refresh_lsync_server_addr()))
        {
          //TBSYS_LOG(WARN, "ups.refresh_lsync_server_addr()=>%d", err);
        }
      }
    }

    int ObUpdateServer::refresh_lsync_server_addr()
    {
      int err = OB_SUCCESS;
      ObServer master;
      if (true)
      {}
      else if (OB_SUCCESS != (err = ups_rpc_stub_.get_inst_master_ups(root_server_, master,
                                                                 param_.get_refresh_lsync_addr_timeout())))
      {
        if (OB_RESPONSE_TIME_OUT != err)
        {
          TBSYS_LOG(ERROR, "ups_rpc_stub.get_ups_instance_master()=>%d", err);
        }
      }
      else
      {
        master.set_port(param_.get_lsync_port());
        set_lsync_server(master.get_ipv4(), master.get_port());
        //fetch_lsync_.set_lsync_server(get_lsync_server());
      }
      return err;
    }

    const ObServer& ObUpdateServer::get_ups_log_master()
    {
      return ObUpsRoleMgr::MASTER == role_mgr_.get_role()? ups_inst_master_: ups_master_;
    }
    ////////////////////////////////////////////////////////////////////////////////////////////////////

    UpsWarmUpDuty::UpsWarmUpDuty() : duty_start_time_(0),
                                     cur_warm_up_percent_(0),
                                     duty_waiting_(0)
    {
    }

    UpsWarmUpDuty::~UpsWarmUpDuty()
    {
    }

    bool UpsWarmUpDuty::drop_start()
    {
      bool bret = false;
      if (0 == atomic_compare_exchange(&duty_waiting_, 1, 0))
      {
        bret = true;
      }
      else
      {
        if (0 != duty_start_time_
            && tbsys::CTimeUtil::getTime() > (MAX_DUTY_IDLE_TIME + duty_start_time_))
        {
          TBSYS_LOG(WARN, "duty has run too long and will be rescheduled, duty_start_time=%ld",
                    duty_start_time_);
          bret = true;
        }
      }
      if (bret)
      {
        duty_start_time_ = tbsys::CTimeUtil::getTime();
        cur_warm_up_percent_ = 0;
        TBSYS_LOG(INFO, "warm up start duty_start_time=%ld", duty_start_time_);
      }
      return bret;
    }

    void UpsWarmUpDuty::drop_end()
    {
      atomic_exchange(&duty_waiting_, 0);
      duty_start_time_ = 0;
      cur_warm_up_percent_ = 0;
    }

    void UpsWarmUpDuty::finish_immediately()
    {
      duty_start_time_ = 0;
    }

    void UpsWarmUpDuty::runTimerTask()
    {
      int64_t warm_up_time = get_warm_up_time();
      if (tbsys::CTimeUtil::getTime() > (duty_start_time_ + warm_up_time))
      {
        submit_force_drop();
        TBSYS_LOG(INFO, "warm up finished, will drop memtable, cur_warm_up_percent=%ld cur_time=%ld duty_start_time=%ld warm_time=%ld",
                  cur_warm_up_percent_, tbsys::CTimeUtil::getTime(), duty_start_time_, warm_up_time);
      }
      else
      {
        if (CacheWarmUpConf::STOP_PERCENT > cur_warm_up_percent_)
        {
          cur_warm_up_percent_++;
          TBSYS_LOG(INFO, "warm up percent update to %ld", cur_warm_up_percent_);
          set_warm_up_percent(cur_warm_up_percent_);
        }
        schedule_warm_up_duty();
      }
    }

    int64_t UpsWarmUpDuty::get_warm_up_time()
    {
      int64_t ret = get_warm_up_conf().warm_up_time_s * 1000000;
      int64_t active_mem_limit = get_active_mem_limit();
      int64_t oldest_memtable_size = get_oldest_memtable_size();
      if (0 != active_mem_limit
          && 0 != oldest_memtable_size)
      {
        double warm_up_time = ceil(static_cast<double>(ret) * static_cast<double>(oldest_memtable_size) / static_cast<double>(active_mem_limit));
        ret = std::min(ret, static_cast<int64_t>(warm_up_time));
      }
      return ret;
    }

    int64_t UpsWarmUpDuty::get_warm_up_step_interval()
    {
      int64_t ret = get_warm_up_conf().warm_up_step_interval_us;
      double warm_up_step_interval = ceil(static_cast<double>(get_warm_up_time()) / (CacheWarmUpConf::STOP_PERCENT / CacheWarmUpConf::STEP_PERCENT));
      ret = std::min(ret, static_cast<int64_t>(warm_up_step_interval));
      return ret;
    }

    int ObUpsLogServerGetter::init(ObUpdateServer* ups)
    {
      int err = OB_SUCCESS;
      if (NULL != ups_)
      {
        err = OB_INIT_TWICE;
      }
      else if (NULL == ups)
      {
        err = OB_INVALID_ARGUMENT;
      }
      else
      {
        ups_ = ups;
      }
      return err;
    }

    int64_t ObUpsLogServerGetter::get_type() const
    {
      int64_t server_type = ANY_SERVER;
      if (NULL == ups_)
      {}
      else if (STANDALONE_SLAVE == ups_->get_obi_slave_stat()
          && ObiRole::SLAVE == ups_->get_obi_role().get_role()
          && ObUpsRoleMgr::MASTER == ups_->get_role_mgr().get_role())
      {
        server_type = LSYNC_SERVER;
      }
      else
      {
        server_type = FETCH_SERVER;
      }
      return server_type;
    }

    int ObUpsLogServerGetter::get_server(ObServer& server) const
    {
      int err = OB_SUCCESS;
      ObServer null_server;
      if (NULL == ups_)
      {
        err = OB_NOT_INIT;
      }
      else if (LSYNC_SERVER == get_type())
      {
        server = ups_->get_lsync_server();
      }
      else if (FETCH_SERVER == get_type())
      {
        server = ups_->get_ups_log_master();
      }
      else
      {
        server = null_server;
        TBSYS_LOG(WARN, "get_server(): unknown slave type");
      }
      return err;
    }

    int ObPrefetchLogTaskSubmitter::init(int64_t prefetch_timeout, ObUpdateServer* ups)
    {
      int err = OB_SUCCESS;
      if (NULL != ups_)
      {
        err = OB_INIT_TWICE;
      }
      else if (NULL == ups)
      {
        err = OB_INVALID_ARGUMENT;
      }
      else
      {
        prefetch_timeout_ = prefetch_timeout;
        ups_ = ups;
      }
      return err;
    }

    int ObPrefetchLogTaskSubmitter::done(Task& task)
    {
      int err = OB_SUCCESS;
      int64_t now_us = tbsys::CTimeUtil::getTime();
      if (task.launch_time_ < last_launch_time_)
      {
        TBSYS_LOG(WARN, "task finished[lauch_time=%ld], but timeout, now_us=%ld, last_launch_time[%ld]", task.launch_time_, now_us, last_launch_time_);
      }
      else
      {
        last_done_time_ = now_us;
      }
      return err;
    }

    int ObPrefetchLogTaskSubmitter::submit_task(void* arg)
    {
      UNUSED(arg);
      int err = OB_SUCCESS;
      int64_t now_us = tbsys::CTimeUtil::getTime();
      Task task;
      if (NULL == ups_)
      {
        err = OB_NOT_INIT;
      }
      else if (last_launch_time_ > last_done_time_ && last_launch_time_ + prefetch_timeout_ > now_us)
      {}
      else
      {
        task.launch_time_ = now_us;
        if (last_launch_time_ + prefetch_timeout_ <= now_us)
        {
          TBSYS_LOG(WARN, "last task timeout, last_launch_time_[%ld] timeout[%ld]", last_launch_time_, prefetch_timeout_);
        }
        if (OB_SUCCESS != (err = ups_->submit_prefetch_remote_log(task)))
        {
          TBSYS_LOG(ERROR, "submit_prefetch_remote_log()=>%d", err);
        }
        else
        {
          last_launch_time_ = now_us;
        }
      }
      return err;
    }
  }
}
