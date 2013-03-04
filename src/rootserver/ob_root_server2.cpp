/*===============================================================
 *   (C) 2007-2010 Taobao Inc.
 *
 *
 *   Version: 0.1 2010-09-26
 *
 *   Authors:
 *          daoan(daoan@taobao.com)
 *          maoqi(maoqi@taobao.com)
 *
 *
 ================================================================*/
#include <new>
#include <string.h>
#include <cmath>
#include <tbsys.h>

#include "common/ob_schema.h"
#include "common/ob_range.h"
#include "common/ob_scanner.h"
#include "common/ob_define.h"
#include "common/ob_action_flag.h"
#include "common/ob_atomic.h"
#include "common/utility.h"
#include "rootserver/ob_root_server2.h"
#include "rootserver/ob_root_worker.h"
#include "rootserver/ob_root_stat_key.h"
#include "rootserver/ob_root_util.h"

using oceanbase::common::databuff_printf;

namespace oceanbase
{
  namespace rootserver
  {
  const int WAIT_SECONDS = 1;
  const int RETURN_BACH_COUNT = 8;
  const int MAX_RETURN_BACH_ROW_COUNT = 1000;
  const int MIN_BALANCE_TOLERANCE = 1;

  const char* ROOT_1_PORT =   "1_port";
  const char* ROOT_1_MS_PORT =   "1_ms_port";
  const char* ROOT_1_IPV6_1 = "1_ipv6_1";
  const char* ROOT_1_IPV6_2 = "1_ipv6_2";
  const char* ROOT_1_IPV6_3 = "1_ipv6_3";
  const char* ROOT_1_IPV6_4 = "1_ipv6_4";
  const char* ROOT_1_IPV4   = "1_ipv4";
  const char* ROOT_1_TABLET_VERSION= "1_tablet_version";

  const char* ROOT_2_PORT =   "2_port";
  const char* ROOT_2_MS_PORT =   "2_ms_port";
  const char* ROOT_2_IPV6_1 = "2_ipv6_1";
  const char* ROOT_2_IPV6_2 = "2_ipv6_2";
  const char* ROOT_2_IPV6_3 = "2_ipv6_3";
  const char* ROOT_2_IPV6_4 = "2_ipv6_4";
  const char* ROOT_2_IPV4   = "2_ipv4";
  const char* ROOT_2_TABLET_VERSION= "2_tablet_version";

  const char* ROOT_3_PORT =   "3_port";
  const char* ROOT_3_MS_PORT =   "3_ms_port";
  const char* ROOT_3_IPV6_1 = "3_ipv6_1";
  const char* ROOT_3_IPV6_2 = "3_ipv6_2";
  const char* ROOT_3_IPV6_3 = "3_ipv6_3";
  const char* ROOT_3_IPV6_4 = "3_ipv6_4";
  const char* ROOT_3_IPV4   = "3_ipv4";
  const char* ROOT_3_TABLET_VERSION= "3_tablet_version";

  const char* ROOT_OCCUPY_SIZE ="occupy_size";
  const char* ROOT_RECORD_COUNT ="record_count";

  char max_row_key[oceanbase::common::OB_MAX_ROW_KEY_LENGTH];

  const int NO_REPORTING = 0;
  const int START_REPORTING = 1;
  const int WAIT_REPORT = 3;

  const int HB_RETRY_FACTOR = 100 * 1000;
}
}

namespace oceanbase
{
  namespace rootserver
  {
    const char* ObRootServer2::ROOT_TABLE_EXT = "rtable";
    const char* ObRootServer2::CHUNKSERVER_LIST_EXT = "clist";

    using namespace common;
    ObRootServer2::ObRootServer2(ObRootConfig &config)
      :config_(config), schema_manager_(NULL),
      root_table_(NULL), tablet_manager_(NULL),
      root_table_for_build_(NULL), tablet_manager_for_build_(NULL),
      have_inited_(false),
      server_status_(STATUS_INIT), build_sync_flag_(BUILD_SYNC_FLAG_NONE),
      first_cs_had_registed_(false), receive_stop_(false),
      last_frozen_mem_version_(-1), last_frozen_time_(0),
      next_select_cs_index_(0),
      latest_config_version_(-1),
      ups_manager_(NULL), ups_heartbeat_thread_(NULL), ups_check_thread_(NULL),
      balancer_(NULL), balancer_thread_(NULL), restart_server_(NULL), is_daily_merge_tablet_error_(false),
      root_table_modifier_(this), merge_checker_(this), heart_beat_checker_(this)
    {
      worker_ = NULL;
      log_worker_ = NULL;
      time_stamp_changing_ = -1;
      frozen_mem_version_ = -1;
      time(&start_time_);
    }

    ObRootServer2::~ObRootServer2()
    {
      if (schema_manager_)
      {
        delete schema_manager_;
        schema_manager_ = NULL;
      }
      if (root_table_)
      {
        delete root_table_;
        root_table_ = NULL;
      }
      if (root_table_for_build_)
      {
        delete root_table_for_build_;
        root_table_for_build_ = NULL;
      }
      if (tablet_manager_)
      {
        delete tablet_manager_;
        tablet_manager_ = NULL;
      }
      if (tablet_manager_for_build_)
      {
        delete tablet_manager_for_build_;
        tablet_manager_for_build_ = NULL;
      }
      if (NULL != ups_manager_)
      {
        delete ups_manager_;
        ups_manager_ = NULL;
      }
      if (NULL != ups_heartbeat_thread_)
      {
        delete ups_heartbeat_thread_;
        ups_heartbeat_thread_ = NULL;
      }
      if (NULL != ups_check_thread_)
      {
        delete ups_check_thread_;
        ups_check_thread_ = NULL;
      }
      if (NULL != balancer_thread_)
      {
        delete balancer_thread_;
        balancer_thread_ = NULL;
      }
      if (NULL != balancer_)
      {
        delete balancer_;
        balancer_ = NULL;
      }
      if (NULL != restart_server_)
      {
        delete restart_server_;
        restart_server_ = NULL;
      }
      have_inited_ = false;
    }

    bool ObRootServer2::init(const int64_t now, OBRootWorker* worker)
    {
      bool res = false;
      if (NULL == worker)
      {
        TBSYS_LOG(ERROR, "worker=NULL");
      }
      else if (have_inited_)
      {
        TBSYS_LOG(ERROR, "already inited");
      }
      else
      {
        worker_ = worker;
        log_worker_ = worker_->get_log_manager()->get_log_worker();
        obi_role_.set_role(ObiRole::INIT); // init as init instance
        res = true;

        schema_manager_ = new(std::nothrow)ObSchemaManagerV2(now);
        TBSYS_LOG(INFO, "init schema_version=%ld", now);
        if (schema_manager_ == NULL)
        {
          TBSYS_LOG(ERROR, "new ObSchemaManagerV2() error");
          res = false;
        }
        else
        {
          // init schema manager
          tbsys::CConfig config;
          if (!schema_manager_->parse_from_file(config_.flag_schema_filename_.get(), config))
          {
            TBSYS_LOG(ERROR, "parse schema error chema file is %s ", config_.flag_schema_filename_.get());
            res = false;
          }
          else
          {
            TBSYS_LOG(INFO, "load schema from file, file=%s", config_.flag_schema_filename_.get());
          }
        }

        tablet_manager_ = new(std::nothrow)ObTabletInfoManager();
        if (tablet_manager_ == NULL)
        {
          TBSYS_LOG(ERROR, "new ObTabletInfoManager error");
          res = false;
        }
        else
        {
          root_table_ = new(std::nothrow)ObRootTable2(tablet_manager_);
          if (root_table_ == NULL)
          {
            TBSYS_LOG(ERROR, "new ObRootTable2 error");
            res = false;
          }
          else
          {
            root_table_->set_replica_num(config_.flag_tablet_replicas_num_.get());
            TBSYS_LOG(INFO, "new root table created, root_table_=%p", root_table_);
          }
        }

        client_helper_.init(worker_->get_client_manager(),worker_->get_thread_buffer(),
            worker_->get_rs_master(), worker_->get_network_timeout());

        if (res)
        {
          ups_manager_ = new(std::nothrow) ObUpsManager(worker_->get_rpc_stub(), config_, obi_role_);
          if (NULL == ups_manager_)
          {
            TBSYS_LOG(ERROR, "no memory");
            res = false;
          }
          else
          {
            ups_manager_->set_ups_config(config_.flag_read_master_master_ups_percent_.get(),
                config_.flag_read_slave_master_ups_percent_.get());
          }
        }
        if (res)
        {
          ups_heartbeat_thread_ = new(std::nothrow) ObUpsHeartbeatRunnable(*ups_manager_);
          if (NULL == ups_heartbeat_thread_)
          {
            TBSYS_LOG(ERROR, "no memory");
            res = false;
          }
        }
        if (res)
        {
          ups_check_thread_ = new(std::nothrow) ObUpsCheckRunnable(*ups_manager_);
          if (NULL == ups_check_thread_)
          {
            TBSYS_LOG(ERROR, "no memory");
            res = false;
          }
        }
        if (res)
        {
          restart_server_ = new(std::nothrow) ObRestartServer();
          if (NULL == restart_server_)
          {
            TBSYS_LOG(ERROR, "no memory");
            res = false;
          }
          else
          {
            restart_server_->set_root_config(&config_);
            restart_server_->set_server_manager(&server_manager_);
            TBSYS_LOG(INFO, "root_table_ address 0x%p", root_table_);
            restart_server_->set_root_table2(root_table_);
            restart_server_->set_root_rpc_stub(&worker_->get_rpc_stub());
            restart_server_->set_root_log_worker(log_worker_);
            restart_server_->set_root_table_build_mutex(&root_table_build_mutex_);
            restart_server_->set_server_manager_rwlock(&server_manager_rwlock_);
            restart_server_->set_root_table_rwlock(&root_table_rwlock_);
          }
        }
        if (res)
        {
          balancer_ = new(std::nothrow) ObRootBalancer();
          if (NULL == balancer_)
          {
            TBSYS_LOG(ERROR, "no memory");
            res = false;
          }
          else
          {
            balancer_->set_config(&config_);
            balancer_->set_root_table(root_table_);
            balancer_->set_server_manager(&server_manager_);
            balancer_->set_schema_manager(schema_manager_);
            balancer_->set_root_table_lock(&root_table_rwlock_);
            balancer_->set_server_manager_lock(&server_manager_rwlock_);
            balancer_->set_schema_manager_lock(&schema_manager_rwlock_);
            balancer_->set_root_table_build_mutex(&root_table_build_mutex_);
            balancer_->set_rpc_stub(&worker_->get_rpc_stub());
            balancer_->set_stat_manager(&worker_->get_stat_manager());
            balancer_->set_log_worker(log_worker_);
            balancer_->set_role_mgr(worker_->get_role_manager());
            balancer_->set_restart_server(restart_server_);
          }
        }
        if (res)
        {
          balancer_thread_ = new(std::nothrow) ObRootBalancerRunnable(config_, *balancer_, *worker_->get_role_manager());
          if (NULL == balancer_thread_)
          {
            TBSYS_LOG(ERROR, "no memory");
            res = false;
          }
        }
        have_inited_ = res;
      }
      return res;
    }

    int ObRootServer2::after_reload_config(bool did_write_log)
    {
      int ret = OB_SUCCESS;
      if (0 > config_.flag_safe_copy_count_in_init_.get()
          || OB_SAFE_COPY_COUNT < config_.flag_safe_copy_count_in_init_.get())
      {
        TBSYS_LOG(ERROR, "invalid safe_copy_count_in_init=%d",
            config_.flag_safe_copy_count_in_init_.get());
        ret = OB_INVALID_ARGUMENT;
      }
      else if (0 >= config_.flag_tablet_replicas_num_.get()
          || OB_SAFE_COPY_COUNT < config_.flag_tablet_replicas_num_.get())
      {
        config_.flag_tablet_replicas_num_.set(OB_SAFE_COPY_COUNT);
        TBSYS_LOG(ERROR, "invalid tablet_replicas_num=%d",
            config_.flag_tablet_replicas_num_.get());
        ret = OB_INVALID_ARGUMENT;
      }
      else if (MIN_BALANCE_TOLERANCE > config_.flag_balance_tolerance_count_.get())
      {
        TBSYS_LOG(ERROR, "invalid flag, balance_tolerence=%ld",
            config_.flag_balance_tolerance_count_.get());
        ret = OB_INVALID_ARGUMENT;
      }
      else if (1 > config_.flag_balance_worker_idle_sleep_seconds_.get())
      {
        TBSYS_LOG(ERROR, "invalid flag, balance_worker_idle_sleep_seconds=%ld",
            config_.flag_balance_worker_idle_sleep_seconds_.get());
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        // ok
      }

      if (OB_SUCCESS == ret
          && !my_addr_.set_ipv4_addr(config_.flag_my_vip_.get(), config_.flag_my_port_.get()))
      {
        TBSYS_LOG(ERROR, "invalid ip=%s port=%d",
            config_.flag_my_vip_.get(), config_.flag_my_port_.get());
        ret = OB_INVALID_ARGUMENT;
      }
      if (OB_SUCCESS == ret && NULL != ups_manager_)
      {
          ups_manager_->set_ups_config(config_.flag_read_master_master_ups_percent_.get(),
              config_.flag_read_slave_master_ups_percent_.get());
      }
      if (OB_SUCCESS == ret)
      {
        int32_t loaded_count = config_.client_config_.obi_list_.obi_count_;
        if (0 == loaded_count)
        {
          // no ob instances config, set myself as the only instance
          config_.client_config_.obi_list_.conf_array_[0].set_rs_addr(my_addr_);
          config_.client_config_.obi_list_.obi_count_ = 1;
        }
        else
        {
          bool found_myself = false;
          for (int i = 0; i < config_.client_config_.obi_list_.obi_count_; ++i)
          {
            if (config_.client_config_.obi_list_.conf_array_[i].get_rs_addr() == my_addr_)
            {
              found_myself = true;
              break;
            }
          }
          if (!found_myself)
          {
            TBSYS_LOG(ERROR, "this RS's address must be in the list of ob instances");
            config_.client_config_.obi_list_.print();
            ret = OB_INVALID_ARGUMENT;
          }
        }
      }
      if (OB_SUCCESS == ret)
      {
        if (NULL != schema_manager_)
        {
          strcpy(config_.client_config_.app_name_, schema_manager_->get_app_name());
        }
      }
      if (OB_SUCCESS == ret && did_write_log && is_master())
      {
        TBSYS_LOG(INFO, "write log client config");
        ret = log_worker_->set_client_config(config_.client_config_);
      }
      TBSYS_LOG(INFO, "after reload config, ret=%d", ret);
      return ret;
    }

    void ObRootServer2::start_threads()
    {
      root_table_modifier_.start();
      balancer_thread_->start();
      heart_beat_checker_.start();
      ups_heartbeat_thread_->start();
      ups_check_thread_->start();
      TBSYS_LOG(INFO, "start threads");
    }

    void ObRootServer2::start_merge_check()
    {
      merge_checker_.start();
      TBSYS_LOG(INFO, "start merge check");
    }

    void ObRootServer2::stop_threads()
    {
      receive_stop_ = true;
      heart_beat_checker_.stop();
      heart_beat_checker_.wait();
      TBSYS_LOG(INFO, "cs heartbeat thread stopped");
      if (NULL != balancer_thread_)
      {
        balancer_thread_->stop();
        balancer_thread_->wakeup();
        balancer_thread_->wait();
        TBSYS_LOG(INFO, "balance worker thread stopped");
      }
      root_table_modifier_.stop();
      root_table_modifier_.wait();
      TBSYS_LOG(INFO, "table modifier thread stopped");
      merge_checker_.stop();
      merge_checker_.wait();
      TBSYS_LOG(INFO, "merge checker thread stopped");
      if (NULL != ups_heartbeat_thread_)
      {
        ups_heartbeat_thread_->stop();
        ups_heartbeat_thread_->wait();
        TBSYS_LOG(INFO, "ups heartbeat thread stopped");
      }
      if (NULL != ups_check_thread_)
      {
        ups_check_thread_->stop();
        ups_check_thread_->wait();
        TBSYS_LOG(INFO, "ups check thread stopped");
      }
    }

    bool ObRootServer2::get_schema(common::ObSchemaManagerV2& out_schema) const
    {
      bool res = false;
      tbsys::CRLockGuard guard(schema_manager_rwlock_);

      if (schema_manager_ != NULL)
      {
        out_schema = *schema_manager_;
        res = true;
      }

      return res;
    }

    void ObRootServer2::set_daily_merge_tablet_error()
    {
      is_daily_merge_tablet_error_ = true;
    }
    void ObRootServer2::clean_daily_merge_tablet_error()
    {
      is_daily_merge_tablet_error_ = false;
    }
    bool ObRootServer2::is_daily_merge_tablet_error()const
    {
      return is_daily_merge_tablet_error_;
    }
    int64_t ObRootServer2::get_schema_version() const
    {
      int64_t t1 = 0;
      tbsys::CRLockGuard guard(schema_manager_rwlock_);

      if (schema_manager_ != NULL)
      {
        t1 = schema_manager_->get_version();
      }

      return t1;
    }

    int64_t ObRootServer2::get_config_version() const
    {
      return latest_config_version_;
    }

    void ObRootServer2::set_config_version(const int64_t version)
    {
      latest_config_version_ = version;
    }
int ObRootServer2::get_max_tablet_version(int64_t &version) const
{
  int err = OB_SUCCESS;
  version = -1;
  if (NULL != root_table_for_build_)
  {
    err = OB_ERROR;
    TBSYS_LOG(WARN, "root server in initialize state.");
  }
  else if (NULL != root_table_)
  {
    tbsys::CRLockGuard guard(root_table_rwlock_);
    version = root_table_->get_max_tablet_version();
    TBSYS_LOG(INFO, "root table max tablet version =%ld", version);
  }
  return err;
}
    void ObRootServer2::print_alive_server() const
    {
      TBSYS_LOG(INFO, "start dump server info");
      //tbsys::CRLockGuard guard(server_manager_rwlock_); // do not need this lock
      ObChunkServerManager::const_iterator it = server_manager_.begin();
      int32_t index = 0;
      for (; it != server_manager_.end(); ++it)
      {
        it->dump(index++);
      }
      TBSYS_LOG(INFO, "dump server info complete");
      return;
    }
    int ObRootServer2::get_cs_info(ObChunkServerManager* out_server_manager) const
    {
      int ret = OB_SUCCESS;
      tbsys::CRLockGuard guard(server_manager_rwlock_);
      if (out_server_manager != NULL)
      {
        *out_server_manager = server_manager_;
      }
      else
      {
        ret = OB_ERROR;
      }
      return ret;
    }

    void ObRootServer2::reset_hb_time()
    {
      TBSYS_LOG(INFO, "reset heartbeat time");
      tbsys::CRLockGuard guard(server_manager_rwlock_);
      ObChunkServerManager::iterator it = server_manager_.begin();
      ObServer server;
      for(; it != server_manager_.end(); ++it)
      {
        server = it->server_;
        if (it->status_ != ObServerStatus::STATUS_DEAD
            && it->port_cs_ > 0)
        {
          server.set_port(it->port_cs_);
          this->receive_hb(server, OB_CHUNKSERVER);
        }
        if (it->ms_status_ != ObServerStatus::STATUS_DEAD
            && it->port_ms_ > 0)
        {
          server.set_port(it->port_ms_);
          this->receive_hb(server, OB_MERGESERVER);
        }
      }
    }



    void ObRootServer2::dump_root_table() const
    {
      tbsys::CRLockGuard guard(root_table_rwlock_);
      TBSYS_LOG(INFO, "dump root table");
      if (root_table_ != NULL)
      {
        root_table_->dump();
      }
    }
    int ObRootServer2::try_create_new_table(int64_t frozen_version, const uint64_t table_id)
    {
      int err = OB_SUCCESS;
      bool table_not_exist_in_rt = false;
      TBSYS_LOG(INFO, "create new table start: table_id=%lu", table_id);
      if (NULL == root_table_)
      {
        TBSYS_LOG(WARN, "root table is not init; wait another second.");
      }
      else
      {
        {
          tbsys::CRLockGuard guard(root_table_rwlock_);
          if(!root_table_->table_is_exist(table_id))
          {
            TBSYS_LOG(INFO, "table not exist, table_id=%lu", table_id);
            table_not_exist_in_rt = true;
          }
        }
        bool is_exist_in_cs = true;
        if (table_not_exist_in_rt)
        {
          //check cs number
          if (DEFAULT_SAFE_CS_NUMBER >= get_alive_cs_number())
          {
            //check table not exist in chunkserver
            if (OB_SUCCESS != (err = table_exist_in_cs(table_id, is_exist_in_cs)))
            {
              TBSYS_LOG(WARN, "fail to check table. table_id=%lu, err=%d", table_id, err);
            }
          }
          else
          {
            TBSYS_LOG(ERROR, "cs number is too litter. alive_cs_number=%ld", get_alive_cs_number());
          }
        }
        if (!is_exist_in_cs)
        {
          ObTabletInfoList tablets;
          int64_t rt_version = 0;
          if (OB_SUCCESS != (err = split_table_range(frozen_version, table_id, tablets)))
          {
            TBSYS_LOG(WARN, "fail to get tablet info for table[%lu], err=%d", table_id, err);
          }
          else if (OB_SUCCESS != (err = check_tablets_legality(tablets)))
          {
            TBSYS_LOG(WARN, "get wrong tablets. err=%d", err);
          }
          else if (OB_SUCCESS != (err = get_max_tablet_version(rt_version)))
          {
            TBSYS_LOG(WARN, "fail get max tablet version. err=%d", err);
          }
          else if (OB_SUCCESS != (err = create_tablet_with_range(rt_version, tablets)))
          {
            TBSYS_LOG(ERROR, "fail to create tablet. rt_version=%ld, err=%d", rt_version, err);
          }
          else
          {
            TBSYS_LOG(INFO, "create tablet for table[%lu] success.", table_id);
          }
        }
      }
      return err;
    }

int64_t ObRootServer2::get_alive_cs_number()
{
  int64_t number = 0;
  ObChunkServerManager::iterator it = server_manager_.begin();
  for (; it != server_manager_.end(); ++it)
  {
    if (it->port_cs_ != 0
        && it->status_ != ObServerStatus::STATUS_DEAD)
    {
      number++;
    }
  }
  return number;
}
int ObRootServer2::check_tablets_legality(const common::ObTabletInfoList &tablets)
{
  int err = OB_SUCCESS;
  common::ObTabletInfo *p_tablet_info = NULL;
  if (0 == tablets.get_tablet_size())
  {
    TBSYS_LOG(WARN, "tablets has zero tablet_info. tablets size = %ld", tablets.get_tablet_size());
    err = OB_INVALID_ARGUMENT;
  }
  static char buff[OB_MAX_ROW_KEY_LENGTH * 2];
  if (OB_SUCCESS == err)
  {
    p_tablet_info = tablets.tablet_list.at(0);
    if (NULL == p_tablet_info)
    {
      TBSYS_LOG(WARN, "p_tablet_info should not be NULL");
      err = OB_INVALID_ARGUMENT;
    }
    else if (!p_tablet_info->range_.border_flag_.is_min_value())
    {
      p_tablet_info->range_.to_string(buff, OB_MAX_ROW_KEY_LENGTH * 2);
      err = OB_INVALID_ARGUMENT;
      TBSYS_LOG(WARN, "range not start correctly. first tablet start_key=%s", buff);
    }
  }
  if (OB_SUCCESS == err)
  {
    p_tablet_info = tablets.tablet_list.at(tablets.get_tablet_size() - 1);
    if (NULL == p_tablet_info)
    {
      TBSYS_LOG(WARN, "p_tablet_info should not be NULL");
      err = OB_INVALID_ARGUMENT;
    }
    else if (!p_tablet_info->range_.border_flag_.is_max_value())
    {
      p_tablet_info->range_.to_string(buff, OB_MAX_ROW_KEY_LENGTH * 2);
      err = OB_INVALID_ARGUMENT;
      TBSYS_LOG(WARN, "range not end correctly. last tablet range=%s", buff);
    }
  }
  if (OB_SUCCESS == err)
  {
    for (int64_t i = 1; OB_SUCCESS == err && i < tablets.get_tablet_size(); i++)
    {
      p_tablet_info = tablets.tablet_list.at(i);
      if (NULL == p_tablet_info)
      {
        err = OB_INVALID_ARGUMENT;
        TBSYS_LOG(WARN, "p_tablet_info should not be NULL");
      }
      else if (!p_tablet_info->range_.border_flag_.is_left_open_right_closed())
      {
        err = OB_INVALID_ARGUMENT;
        TBSYS_LOG(WARN, "range not legal, should be left open right closed. range=%s", buff);
      }
      else if (p_tablet_info->range_.start_key_ == p_tablet_info->range_.end_key_)
      {
        err = OB_INVALID_ARGUMENT;
        TBSYS_LOG(WARN, "range not legal, start_key = end_key; range=%s", buff);
      }
      else if (0 != p_tablet_info->range_.start_key_.compare(tablets.tablet_list.at(i - 1)->range_.end_key_))
      {
        p_tablet_info->range_.to_string(buff, OB_MAX_ROW_KEY_LENGTH * 2);
        static char end_key[OB_MAX_ROW_KEY_LENGTH * 2];
        tablets.tablet_list.at(i - 1)->range_.to_string(end_key, OB_MAX_ROW_KEY_LENGTH * 2);
        TBSYS_LOG(WARN, "range not continuous. %ldth tablet range=%s, %ldth tablet range=%s", i-1, end_key, i, buff);
        err = OB_INVALID_ARGUMENT;
      }
    }
  }
  return err;
}
    int ObRootServer2::try_create_new_table(int64_t frozen_version)
    {
      int ret = OB_SUCCESS;
      int err = OB_SUCCESS;
      uint64_t table_id = OB_INVALID_ID;
      if (NULL == root_table_)
      {
        TBSYS_LOG(WARN, "root table is not init; wait another second.");
      }
      else
      {
        for (const ObTableSchema* it=schema_manager_->table_begin();
            it != schema_manager_->table_end(); ++it)
        {
          if (it->get_table_id() != OB_INVALID_ID)
          {
            {
              tbsys::CRLockGuard guard(root_table_rwlock_);
              if(!root_table_->table_is_exist(it->get_table_id()))
              {
                TBSYS_LOG(INFO, "table not exist, table_id=%lu", it->get_table_id());
                table_id = it->get_table_id();
              }
            }
            if (OB_INVALID_ID != table_id)
            {
              err = try_create_new_table(frozen_version, table_id);
              if (OB_SUCCESS != err)
              {
                TBSYS_LOG(ERROR, "fail to create table.table_id=%ld", table_id);
                ret = err;
              }
            }
          }
        }
      }
      return ret;
    }

 int ObRootServer2::split_table_range(const int64_t frozen_version, const uint64_t table_id,
     common::ObTabletInfoList &tablets)
 {
   int err = OB_SUCCESS;
   //step1: get master ups
   ObServer master_ups;
   if (OB_SUCCESS == err)
   {
     if (OB_SUCCESS != (err = get_master_ups(master_ups, false)))
     {
       TBSYS_LOG(WARN, "fail to get master ups addr. err=%d", err);
     }
   }
   //setp2: get tablet_info form ups ,retry
   int64_t rt_version = 0;
   if (OB_SUCCESS == err)
   {
     if (OB_SUCCESS != (err = get_max_tablet_version(rt_version)))
     {
       TBSYS_LOG(WARN, "fail to get max tablet version.err=%d", err);
     }
   }
   int64_t fetch_version = rt_version;
   if (1 == fetch_version)
   {
     fetch_version = 2;
   }
   if (OB_SUCCESS == err)
   {
     while (fetch_version <= frozen_version)
     {
       common::ObTabletInfoList tmp_tablets;
       if (OB_SUCCESS != (err = worker_->get_rpc_stub().get_split_range(master_ups,
               config_.flag_network_timeout_us_.get(), table_id,
               fetch_version, tmp_tablets)))
       {
         TBSYS_LOG(WARN, "fail to get split range from ups. ups=%s, table_id=%ld, fetch_version=%ld, forzen_version=%ld",
             master_ups.to_cstring(), table_id, fetch_version, frozen_version);
         if (OB_UPS_INVALID_MAJOR_VERSION == err)
         {
           err = OB_SUCCESS;
           TBSYS_LOG(INFO, "fetch tablets retruen invalid_major_version, version=%ld, try next version.", fetch_version);
         }
         else
         {
           TBSYS_LOG(WARN, "get split_range has some trouble, abort it. err=%d", err);
           break;
         }
       }
       else
       {
         if ((1 == tmp_tablets.get_tablet_size())
             && (0 == tmp_tablets.tablet_list.at(0)->row_count_))
         {
           tablets = tmp_tablets;
           if (fetch_version == frozen_version)
           {
             TBSYS_LOG(INFO, "reach the bigger fetch_version, equal to frozen_version[%ld]", frozen_version);
             break;
           }
           else
           {
             TBSYS_LOG(INFO, "fetch only one empty tablet. fetch_version=%ld, frozen_version=%ld, row_count=%ld, try the next version",
                 fetch_version, frozen_version, tmp_tablets.tablet_list.at(0)->row_count_);
           }
         }
         else
         {
           tablets = tmp_tablets;
           break;
         }
       }
       sleep(WAIT_SECONDS);
       fetch_version ++;
     }
     if (OB_SUCCESS != err && fetch_version > frozen_version)
     {
       err = OB_ERROR;
       TBSYS_LOG(ERROR, "retry all version and failed. check it.");
     }
     else if (OB_SUCCESS == err)
     {
       TBSYS_LOG(INFO, "fetch tablet_list succ. fetch_version=%ld, tablet_list size=%ld",
           fetch_version, tablets.get_tablet_size());
       ObTabletInfo *tablet_info = NULL;
       for (int64_t i = 0; i < tablets.get_tablet_size(); i++)
       {
         tablet_info = tablets.tablet_list.at(i);
         static char row_key_dump_buff[OB_MAX_ROW_KEY_LENGTH * 2];
         tablet_info->range_.to_string(row_key_dump_buff, OB_MAX_ROW_KEY_LENGTH * 2);
         TBSYS_LOG(INFO, "%ldth tablet:range=%s", i, row_key_dump_buff);
       }
     }
   }
   return err;
 }

 int ObRootServer2::create_tablet_with_range(const int64_t frozen_version, const ObTabletInfoList& tablets)
 {
   int ret = OB_SUCCESS;
   int64_t index = tablets.tablet_list.get_array_index();
   ObTabletInfo* p_table_info = NULL;
   ObRootTable2* root_table_for_split = new(std::nothrow) ObRootTable2(NULL);
   if (NULL == root_table_for_split)
   {
     TBSYS_LOG(WARN, "new ObRootTable2 fail.");
     ret = OB_ALLOCATE_MEMORY_FAILED;
   }
   if (OB_SUCCESS == ret)
   {
     tbsys::CRLockGuard guard(root_table_rwlock_);
     *root_table_for_split = *root_table_;
   }

   int32_t **server_index = NULL;
   int32_t *create_count = NULL;
   if (OB_SUCCESS == ret)
   {
     create_count = new (std::nothrow)int32_t[index];
     server_index = new (std::nothrow)int32_t*[index];
     if (NULL == server_index || NULL == create_count)
     {
       ret = OB_ALLOCATE_MEMORY_FAILED;
     }
     else
     {
       for (int32_t i = 0; i < index; i++)
       {
         server_index[i] = new(std::nothrow) int32_t[OB_SAFE_COPY_COUNT];
         if (NULL == server_index[i])
         {
           ret = OB_ALLOCATE_MEMORY_FAILED;
           break;
         }
       }
     }
   }
   for (int64_t i = 0; OB_SUCCESS == ret && i < index; i ++)
   {
     p_table_info = tablets.tablet_list.at(i);
     if (p_table_info != NULL)
     {
       static char row_key_dump_buff[OB_MAX_ROW_KEY_LENGTH * 2];
       p_table_info->range_.to_string(row_key_dump_buff, OB_MAX_ROW_KEY_LENGTH * 2);

       TBSYS_LOG(INFO, "tablet_index=%ld, range=%s", i, row_key_dump_buff);
       if (!p_table_info->range_.border_flag_.is_left_open_right_closed())
       {
         TBSYS_LOG(WARN, "illegal tablet, range=%s", row_key_dump_buff);
       }
       else
       {
         //通知cs建立tablet
         if (OB_SUCCESS != (ret =
               create_empty_tablet_with_range(frozen_version, root_table_for_split,
                 *p_table_info, create_count[i], server_index[i])))
         {
           TBSYS_LOG(WARN, "fail to create tablet, ret =%d, range=%s", ret, row_key_dump_buff);
         }
         else
         {
           TBSYS_LOG(INFO, "create tablet succ. range=%s", row_key_dump_buff);
         }
       }
     }//end p_table_info != NULL
   }//end for
   if (OB_SUCCESS == ret && root_table_for_split != NULL)
   {
     TBSYS_LOG(INFO, "create tablet success.");
    if (is_master())
    {
      int ret2 = log_worker_->batch_add_new_tablet(tablets, server_index, create_count, frozen_version);
      if (OB_SUCCESS != ret2)
      {
        TBSYS_LOG(WARN, "failed to log add_new_tablet, err=%d", ret2);
      }
    }
     switch_root_table(root_table_for_split, NULL);
     root_table_for_split = NULL;
   }
   else
   {
     if (NULL != root_table_for_split)
     {
       delete root_table_for_split;
       root_table_for_split = NULL;
     }
   }
   for (int64_t i = 0; i < index; i++)
   {
     if (NULL != server_index[i])
     {
       delete [] server_index[i];
     }
   }
   if (NULL != server_index)
   {
     delete [] server_index;
   }
   if (NULL != create_count)
   {
     delete [] create_count;
   }
   return ret;
 }

 int ObRootServer2::create_empty_tablet_with_range(const int64_t frozen_version,
     ObRootTable2 *root_table, const common::ObTabletInfo &tablet,
     int32_t& created_count, int* t_server_index)
 {
   int ret = OB_SUCCESS;
   int32_t server_index[OB_SAFE_COPY_COUNT];
   if (NULL == root_table)
   {
     TBSYS_LOG(WARN, "invalid argument. root_table_for_split is NULL;");
     ret = OB_INVALID_ARGUMENT;
   }

   for (int i = 0; i < OB_SAFE_COPY_COUNT; i++)
   {
     server_index[i] = OB_INVALID_INDEX;
     t_server_index[i] = OB_INVALID_INDEX;
   }

   int32_t result_num = -1;
   if (OB_SUCCESS == ret)
   {
     get_available_servers_for_new_table(server_index, config_.flag_tablet_replicas_num_.get(), result_num);
     if (0 >= result_num)
     {
       TBSYS_LOG(WARN, "no cs seleted");
       ret = OB_NO_CS_SELECTED;
     }
   }

   created_count = 0;
   if (OB_SUCCESS == ret)
   {
     TBSYS_LOG(INFO, "cs selected for create new table, num=%d", result_num);
     for (int32_t i = 0; i < result_num; i++)
     {
       if (server_index[i] != OB_INVALID_INDEX)
       {
         ObServerStatus* server_status = server_manager_.get_server_status(server_index[i]);
         if (server_status != NULL)
         {
           common::ObServer server = server_status->server_;
           server.set_port(server_status->port_cs_);
           int err = worker_->get_rpc_stub().create_tablet(server, tablet.range_, frozen_version,
                 config_.flag_network_timeout_us_.get());
           if (OB_SUCCESS != err && OB_ENTRY_EXIST != err)
           {
             TBSYS_LOG(WARN, "fail to create tablet replica, server=%d", server_index[i]);
           }
           else
           {
             err = OB_SUCCESS;
             t_server_index[created_count] = server_index[i];
             created_count++;
             TBSYS_LOG(INFO, "create tablet replica, table_id=%lu server=%d version=%ld",
                 tablet.range_.table_id_, server_index[i], frozen_version);
           }
         }
         else
         {
           server_index[i] = OB_INVALID_INDEX;
         }
       }
     }
     if (created_count > 0)
     {
       ret = root_table->create_table(tablet, t_server_index, created_count, frozen_version);
       if (OB_SUCCESS != ret)
       {
         TBSYS_LOG(WARN, "fail to create table.err=%d", ret);
       }
     }
     else
     {
       TBSYS_LOG(WARN, "no tablet created");
       ret = OB_NO_TABLETS_CREATED;
     }
   }
   return ret;
 }
    int ObRootServer2::check_tablet_version(const int64_t tablet_version, bool &is_merged) const
    {
      int err = OB_SUCCESS;
      tbsys::CRLockGuard guard(root_table_rwlock_);
      TBSYS_LOG(INFO, "check tablet version[required_version=%ld]", tablet_version);
      if (NULL != root_table_for_build_)
      {
        err = OB_RS_STATUS_INIT;
        TBSYS_LOG(WARN, "aready in report process. check unlegal");
      }
      else if (NULL != root_table_)
      {
        if (root_table_->is_empty())
        {
          is_merged = false;
          TBSYS_LOG(WARN, "root_table is empty, try it later");
        }
        else
        {
          err = root_table_->check_tablet_version_merged(tablet_version, is_merged);
        }
      }
      else
      {
        err = OB_ERROR;
        TBSYS_LOG(WARN, "fail to check tablet_version_merged. root_table_ = null");
      }
      return err;
    }

    int ObRootServer2::request_cs_report_tablet()
    {
      int ret = OB_SUCCESS;
      ObChunkServerManager::iterator it = server_manager_.begin();
      for (; it != server_manager_.end(); ++it)
      {
        if (it->status_ != ObServerStatus::STATUS_DEAD)
        {
          if (OB_SUCCESS != (ret = worker_->get_rpc_stub().request_report_tablet(it->server_)))
          {
            TBSYS_LOG(WARN, "fail to request cs to report. cs_addr=%s", it->server_.to_cstring());
          }
        }
      }
      return ret;
    }

    int ObRootServer2::dump_cs_tablet_info(const common::ObServer cs, int64_t &tablet_num) const
    {
      int err = OB_SUCCESS;
      tbsys::CRLockGuard guard(root_table_rwlock_);
      TBSYS_LOG(INFO, "dump cs[%s]'s tablet info", cs.to_cstring());
      int server_index = get_server_index(cs);
      if (server_index == OB_INVALID_INDEX)
      {
        TBSYS_LOG(WARN, "can not find server's info, server=%s", cs.to_cstring());
        err = OB_ENTRY_NOT_EXIST;
      }
      else if (root_table_ != NULL)
      {
        root_table_->dump_cs_tablet_info(server_index, tablet_num);
      }
      return err;
    }

    void ObRootServer2::dump_unusual_tablets() const
    {
      int32_t num = 0;
      tbsys::CRLockGuard guard(root_table_rwlock_);
      TBSYS_LOG(INFO, "dump unusual tablets");
      if (root_table_ != NULL)
      {
        root_table_->dump_unusual_tablets(last_frozen_mem_version_, config_.flag_tablet_replicas_num_.get(), num);
      }
    }

    void ObRootServer2::dump_migrate_info() const
    {
      balancer_->dump_migrate_info();
    }

    int ObRootServer2::get_deleted_tables(const common::ObSchemaManagerV2 &old_schema,
                                          const common::ObSchemaManagerV2 &new_schema,
                                          common::ObArray<uint64_t> &deleted_tables)
    {
      int ret = OB_SUCCESS;
      deleted_tables.clear();
      const ObTableSchema* it = old_schema.table_begin();
      const ObTableSchema* it2 = NULL;
      for (; it != old_schema.table_end(); ++it)
      {
        if (NULL == (it2 = new_schema.get_table_schema(it->get_table_id())))
        {
          if (OB_SUCCESS != (ret = deleted_tables.push_back(it->get_table_id())))
          {
            TBSYS_LOG(WARN, "failed to push array, err=%d", ret);
            break;
          }
          else
          {
            TBSYS_LOG(INFO, "table deleted, table_id=%lu", it->get_table_id());
          }
        }
      }
      return ret;
    }

    int64_t ObRootServer2::get_last_frozen_version() const
    {
      return last_frozen_mem_version_;
    }

    /*
     * 从本地读取新schema, 判断兼容性
     */
    int ObRootServer2::switch_schema(int64_t time_stamp, common::ObArray<uint64_t> &deleted_tables)
    {
      TBSYS_LOG(INFO, "switch_schema time_stamp is %ld", time_stamp);
      int ret = OB_SUCCESS;
      static const int64_t WARNING_SCHEMA_SIZE = 1024*512*3; // 1.5MB
      int64_t serialize_size = 0;
      tbsys::CConfig config;
      common::ObSchemaManagerV2* new_schema_manager = NULL;
      new_schema_manager = new(std::nothrow)ObSchemaManagerV2(time_stamp);

      if (new_schema_manager == NULL)
      {
        TBSYS_LOG(ERROR, "new ObSchemaManagerV2() error");
        ret = OB_ERROR;
      }
      else if (! new_schema_manager->parse_from_file(config_.flag_schema_filename_.get(), config))
      {
        TBSYS_LOG(ERROR, "parse schema error chema file is %s ", config_.flag_schema_filename_.get());
        ret = OB_ERROR;
      }
      else if (OB_MAX_PACKET_LENGTH <= (serialize_size = new_schema_manager->get_serialize_size()))
      {
        TBSYS_LOG(ERROR, "serialized schema manager is too large, size=%ld", serialize_size);
        ret = OB_BUF_NOT_ENOUGH;
      }
      else
      {
        if (WARNING_SCHEMA_SIZE < serialize_size)
        {
          TBSYS_LOG(WARN, "serialized size of schema manager is larger than %ld", WARNING_SCHEMA_SIZE);
          ret = OB_ERROR;
        }
        tbsys::CWLockGuard guard(schema_manager_rwlock_);
        if (! schema_manager_->is_compatible(*new_schema_manager))
        {
          TBSYS_LOG(ERROR, "new schema can not compitable with the old one"
                    " schema file name is  %s ", config_.flag_schema_filename_.get());
          ret = OB_CS_SCHEMA_INCOMPATIBLE;
        }
        else if (OB_SUCCESS != (ret = get_deleted_tables(*schema_manager_, *new_schema_manager, deleted_tables)))
        {
          TBSYS_LOG(WARN, "failed to get delete tables, err=%d", ret);
        }
        else
        {
          common::ObSchemaManagerV2* old_schema_manager = NULL;
          old_schema_manager = schema_manager_;
          switch_schema_manager(new_schema_manager);
          new_schema_manager = NULL;

          if (old_schema_manager != NULL)
          {
            delete old_schema_manager;
            old_schema_manager = NULL;
          }
        }
      }

      if (OB_SUCCESS != ret && NULL != new_schema_manager)
      {
        delete new_schema_manager;
        new_schema_manager = NULL;
      }

      return ret;
    }

    void ObRootServer2::switch_schema_manager(common::ObSchemaManagerV2 *schema_manager)
    {
      OB_ASSERT(schema_manager);
      OB_ASSERT(schema_manager != schema_manager_);
      schema_manager_ = schema_manager;
      TBSYS_LOG(INFO, "new schema manager, addr=%p", schema_manager_);
      balancer_->set_schema_manager(schema_manager_);
    }

    void ObRootServer2::switch_root_table(ObRootTable2 *rt, ObTabletInfoManager *ti)
    {
      OB_ASSERT(rt);
      OB_ASSERT(rt != root_table_);
      tbsys::CWLockGuard guard(root_table_rwlock_);
      if (NULL != root_table_)
      {
        delete root_table_;
        root_table_ = NULL;
      }
      root_table_ = rt;
      TBSYS_LOG(INFO, "new root table, addr=%p", root_table_);
      balancer_->set_root_table(root_table_);
      restart_server_->set_root_table2(root_table_);

      if (NULL != ti && ti != tablet_manager_)
      {
        if (NULL != tablet_manager_)
        {
          delete tablet_manager_;
          tablet_manager_ = NULL;
        }
        tablet_manager_ = ti;
      }
    }

    int ObRootServer2::create_root_table_for_build()
    {
      int ret = OB_SUCCESS;
      if (tablet_manager_for_build_ != NULL)
      {
        delete tablet_manager_for_build_;
        tablet_manager_for_build_ = NULL;
      }
      if (root_table_for_build_ != NULL)
      {
        delete root_table_for_build_;
        root_table_for_build_ = NULL;
      }
      //root_table_for_query we need create new table
      tablet_manager_for_build_ = new(std::nothrow)ObTabletInfoManager;
      if (tablet_manager_for_build_ == NULL)
      {
        TBSYS_LOG(ERROR, "new ObTabletInfoManager error");
        ret = OB_ALLOCATE_MEMORY_FAILED;
      }
      root_table_for_build_ = new(std::nothrow)ObRootTable2(tablet_manager_for_build_);
      if (NULL == root_table_for_build_)
      {
        TBSYS_LOG(ERROR, "new root table for build error");
        ret = OB_ALLOCATE_MEMORY_FAILED;
      }
      TBSYS_LOG(INFO, "new root_table_for_build=%p", root_table_for_build_);
      return ret;
    }

    int ObRootServer2::init_root_table_by_report()
    {
      TBSYS_LOG(INFO, "[NOTICE] init_root_table_by_report begin");
      int  res = OB_SUCCESS;

      create_root_table_for_build();
      if (root_table_for_build_ == NULL)
      {
        TBSYS_LOG(ERROR, "new ObRootTable2 error");
        res = OB_ERROR;
      }
      {
        tbsys::CThreadGuard guard(&status_mutex_);
        server_status_ = STATUS_CHANGING;
        TBSYS_LOG(INFO, "server_status_ = %d", server_status_);
      }
      //wail until have cs registed
      while (OB_SUCCESS == res && !first_cs_had_registed_ && !receive_stop_)
      {
        TBSYS_LOG(INFO, "wait for the first cs regist");
        sleep(1);
      }
      bool finish_init = false;
      int round = 0;
      while(OB_SUCCESS == res && !finish_init && !receive_stop_)
      {
        TBSYS_LOG(INFO, "clock click, round=%d", ++round);

        ObTabletInfoManager* tablet_manager_tmp = NULL;
        ObRootTable2* root_table_tmp = NULL;
        if (OB_SUCCESS == res)
        {
          tablet_manager_tmp = new(std::nothrow)ObTabletInfoManager;
          if (tablet_manager_tmp == NULL)
          {
            TBSYS_LOG(ERROR, "new ObTabletInfoManager error");
            res = OB_ERROR;
          }
          root_table_tmp = new(std::nothrow)ObRootTable2(tablet_manager_tmp);
          if (root_table_tmp == NULL)
          {
            TBSYS_LOG(ERROR, "new ObRootTable2 error");
            res = OB_ERROR;
          }
        }
        if (OB_SUCCESS == res)
        {
          tbsys::CThreadGuard guard_build(&root_table_build_mutex_);
          TBSYS_LOG(INFO, "table_for_build is empty?%c", root_table_for_build_->is_empty()?'Y':'N');

          root_table_for_build_->sort();
          delete_list_.reset();
          bool check_ok = false;
          check_ok = (OB_SUCCESS == root_table_for_build_->shrink_to(root_table_tmp, delete_list_));
          if (check_ok)
          {
            root_table_tmp->sort();
          }
          else
          {
            TBSYS_LOG(ERROR, "shrink table error");
          }
          if (0 < delete_list_.get_tablet_size())
          {
            if (is_master() || worker_->get_role_manager()->get_role() == ObRoleMgr::STANDALONE)
            {
              ObRootUtil::delete_tablets(worker_->get_rpc_stub(), server_manager_, delete_list_, config_.flag_network_timeout_us_.get());
            }
          }
          if (check_ok)
          {
            check_ok = root_table_tmp->check_lost_range();
            if (!check_ok)
            {
              TBSYS_LOG(WARN, "check root table failed we will wait for another %d seconds",
                        config_.flag_safe_wait_init_duration_seconds_.get());
            }
          }
          if (check_ok)
          {
            check_ok = root_table_tmp->check_tablet_copy_count(config_.flag_safe_copy_count_in_init_.get());
            if (!check_ok)
            {
              TBSYS_LOG(WARN, "check root table copy_count fail we will wait for another %d seconds",
                config_.flag_safe_wait_init_duration_seconds_.get());
            }
          }
          if (check_ok)
          {
            //if all server is reported and
            //we have no tablets, we create it

            bool all_server_is_reported = true;
            int32_t server_count = 0;
            {
              tbsys::CRLockGuard guard(server_manager_rwlock_);
              ObChunkServerManager::iterator it = server_manager_.begin();
              for (; it != server_manager_.end(); ++it)
              {
                if(it->port_cs_ != 0 && (it->status_ == ObServerStatus::STATUS_REPORTING ||
                      it->status_ == ObServerStatus::STATUS_DEAD))
                {
                  all_server_is_reported = false;
                  break;
                }
                ++server_count;
              }
            }
            TBSYS_LOG(INFO, "all_cs_reported=%c cs_num=%d", all_server_is_reported ? 'Y' : 'N', server_count);
            if (server_count > 0
                && all_server_is_reported
                && root_table_tmp->is_empty()
                && config_.flag_create_table_in_init_.get() != 0
                && is_master())
            {
              TBSYS_LOG(INFO,"chunkservers have no data,create new table");
              if (OB_SUCCESS != create_new_table(schema_manager_, root_table_for_build_))
              {
                TBSYS_LOG(WARN, "failed to create new tables");
              }
            }

            //check we have we have enougth table
            tbsys::CRLockGuard guard(schema_manager_rwlock_);
            for (const ObTableSchema* it = schema_manager_->table_begin(); it != schema_manager_->table_end(); ++it)
            {
              if (it->get_table_id() != OB_INVALID_ID && !it->is_pure_update_table())
              {
                if(!root_table_tmp->table_is_exist(it->get_table_id()))
                {
                  TBSYS_LOG(WARN, "table_id = %lu has not been created are you sure about this?", it->get_table_id() );
                  check_ok = false;
                  break;
                }
              }
            }
          }
          if (check_ok)
          {
            delete root_table_for_build_;
            root_table_for_build_ = NULL;
            delete tablet_manager_for_build_;
            tablet_manager_for_build_ = NULL;
            switch_root_table(root_table_tmp, tablet_manager_tmp);
            root_table_tmp = NULL;
            tablet_manager_tmp = NULL;
            finish_init = true;
          }
        }
        if (tablet_manager_tmp != NULL)
        {
          delete tablet_manager_tmp;
          tablet_manager_tmp = NULL;
        }
        if (root_table_tmp != NULL)
        {
          delete root_table_tmp;
          root_table_tmp = NULL;
        }
        if (OB_SUCCESS == res && !finish_init && !receive_stop_)
        {
          TBSYS_LOG(INFO, "sleep for %d seconds", config_.flag_safe_wait_init_duration_seconds_.get());
          for (int i = 0; i < config_.flag_safe_wait_init_duration_seconds_.get() && !receive_stop_; ++i)
          {
            sleep(1);
          }
        }
      }//end while

      if (root_table_for_build_ != NULL)
      {
        delete root_table_for_build_;
        root_table_for_build_ = NULL;
      }
      if (tablet_manager_for_build_ != NULL)
      {
        delete tablet_manager_for_build_;
        tablet_manager_for_build_ = NULL;
      }
      {
        tbsys::CWLockGuard guard(server_manager_rwlock_);
        ObChunkServerManager::iterator it = server_manager_.begin();
        for (; it != server_manager_.end(); ++it)
        {
          if(it->status_ == ObServerStatus::STATUS_REPORTED || it->status_ == ObServerStatus::STATUS_REPORTING)
          {
            it->status_ = ObServerStatus::STATUS_SERVING;
          }
        }
      }

      build_sync_flag_ = BUILD_SYNC_INIT_OK;
      {
        tbsys::CThreadGuard guard(&(status_mutex_));
        if (server_status_ == STATUS_CHANGING)
        {
          server_status_ = STATUS_SLEEP;
          TBSYS_LOG(INFO, "server_status_ = %d", server_status_);
        }
      }

      if (res == OB_SUCCESS && finish_init && is_master())
      {
        TBSYS_LOG(INFO, "before do check point, res: %d", res);
        tbsys::CRLockGuard rt_guard(root_table_rwlock_);
        tbsys::CRLockGuard cs_guard(server_manager_rwlock_);
        tbsys::CThreadGuard st_guard(&status_mutex_);
        tbsys::CThreadGuard log_guard(worker_->get_log_manager()->get_log_sync_mutex());
        int ret = worker_->get_log_manager()->do_check_point();
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "do check point after init root table failed, err: %d", ret);
        }
        else
        {
          TBSYS_LOG(INFO, "do check point after init root table done");
        }
      }
      TBSYS_LOG(INFO, "[NOTICE] init_root_table_by_report finished, res=%d", res);
      return res;
    }

     int ObRootServer2::replay_remove_replica(const common::ObTabletReportInfo &replica)
     {
       int ret = OB_SUCCESS;
       ObRootTable2::const_iterator start_it;
       ObRootTable2::const_iterator end_it;
       tbsys::CThreadGuard mutex_gard(&root_table_build_mutex_);
       tbsys::CWLockGuard guard(root_table_rwlock_);
       int find_ret = root_table_->find_range(replica.tablet_info_.range_, start_it, end_it);
       if (OB_SUCCESS == find_ret && start_it == end_it)
       {
         for (int i = 0; i < OB_SAFE_COPY_COUNT; ++i)
         {
           if (OB_INVALID_INDEX != start_it->server_info_indexes_[i]
               && start_it->server_info_indexes_[i] == replica.tablet_location_.chunkserver_.get_port())
           {
             start_it->server_info_indexes_[i] = OB_INVALID_INDEX;
             break;
           }
         }
       }
       return ret;
     }

    /*
     * chunk serve和merege server注册
     * @param out status 0 do not start report 1 start report
     */
    int ObRootServer2::regist_server(const ObServer& server, bool is_merge, int32_t& status, int64_t time_stamp)
    {
      int ret = OB_NOT_INIT;
      if (server_status_ != STATUS_INIT ) // not in init
      {
        ret = OB_SUCCESS;
        if (TBSYS_LOGGER._level >= TBSYS_LOG_LEVEL_INFO)
        {
          char server_char[OB_IP_STR_BUFF];
          server.to_string(server_char, OB_IP_STR_BUFF);
          TBSYS_LOG(INFO, "regist server %s is_merge %d", server_char, is_merge);
        }
        if (is_master())
        {
          if (time_stamp < 0)
            time_stamp = tbsys::CTimeUtil::getTime();

          if (is_merge)
          {
            ret = log_worker_->regist_ms(server, time_stamp);
          }
          else
          {
            ret = log_worker_->regist_cs(server, time_stamp);
          }
        }

        if (ret == OB_SUCCESS)
        {
          {
            time_stamp = tbsys::CTimeUtil::getMonotonicTime();
            int rc = 0;
            tbsys::CWLockGuard guard(server_manager_rwlock_);
            rc = server_manager_.receive_hb(server, time_stamp, is_merge, true);
          }
          first_cs_had_registed_ = true;
          //now we always want cs report its tablet
          status = START_REPORTING;
          if (!is_merge && START_REPORTING == status)
          {
            tbsys::CThreadGuard mutex_guard(&root_table_build_mutex_); //this for only one thread modify root_table
            tbsys::CWLockGuard root_table_guard(root_table_rwlock_);
            tbsys::CWLockGuard server_info_guard(server_manager_rwlock_);
            ObChunkServerManager::iterator it;
            it = server_manager_.find_by_ip(server);
            if (it != server_manager_.end())
            {
              {
                //remove this server's tablet
                if (root_table_ != NULL)
                {
                  root_table_->server_off_line(static_cast<int32_t>(it - server_manager_.begin()), 0);
                }
              }
              if (it->status_ == ObServerStatus::STATUS_SERVING || it->status_ == ObServerStatus::STATUS_WAITING_REPORT)
              {
                // chunk server will start report
                it->status_ = ObServerStatus::STATUS_REPORTING;
              }
            }
          }

        }
        TBSYS_LOG(INFO, "regist ret %d", ret);
      }

      if(OB_SUCCESS == ret)
      {
        if(NULL != balancer_thread_)
        {
          balancer_thread_->wakeup();
        }
        else
        {
          TBSYS_LOG(WARN, "balancer_thread_ is null");
        }
      }
      return ret;
    }

    /*
     * chunk server更新自己的磁盘情况信息
     */
    int ObRootServer2::update_capacity_info(const common::ObServer& server, const int64_t capacity, const int64_t used)
    {
      int ret = OB_SUCCESS;
      if (is_master())
      {
        ret = log_worker_->report_cs_load(server, capacity, used);
      }

      if (ret == OB_SUCCESS)
      {
        ObServerDiskInfo disk_info;
        disk_info.set_capacity(capacity);
        disk_info.set_used(used);
        tbsys::CWLockGuard guard(server_manager_rwlock_);
        ret = server_manager_.update_disk_info(server, disk_info);
      }

      if (TBSYS_LOGGER._level >= TBSYS_LOG_LEVEL_INFO)
      {
        char server_str[OB_IP_STR_BUFF];
        server.to_string(server_str, OB_IP_STR_BUFF);
        TBSYS_LOG(INFO, "server %s update capacity info capacity is %ld, used is %ld ret is %d", server_str, capacity, used, ret);
      }

      return ret;
    }

    /*
     * 迁移完成操作
     */
    int ObRootServer2::migrate_over(const ObRange& range, const common::ObServer& src_server, const common::ObServer& dest_server, const bool keep_src, const int64_t tablet_version)
{
  int ret = OB_SUCCESS;
  if (TBSYS_LOGGER._level >= TBSYS_LOG_LEVEL_INFO)
  {
    static char row_key_dump_buff[OB_MAX_ROW_KEY_LENGTH * 2];
    range.to_string(row_key_dump_buff, OB_MAX_ROW_KEY_LENGTH * 2);
    char f_server[OB_IP_STR_BUFF];
    char t_server[OB_IP_STR_BUFF];
    src_server.to_string(f_server, OB_IP_STR_BUFF);
    dest_server.to_string(t_server, OB_IP_STR_BUFF);
    TBSYS_LOG(INFO, "migrate_over received, src_cs=%s dest_cs=%s keep_src=%d range=%s",
        f_server, t_server, keep_src, row_key_dump_buff);
  }

  int src_server_index = get_server_index(src_server);
  int dest_server_index = get_server_index(dest_server);

  ObRootTable2::const_iterator start_it;
  ObRootTable2::const_iterator end_it;
  common::ObTabletInfo *tablet_info = NULL;
  {
    tbsys::CThreadGuard mutex_gard(&root_table_build_mutex_);
    tbsys::CWLockGuard guard(root_table_rwlock_);
    int find_ret = root_table_->find_range(range, start_it, end_it);
    if (OB_SUCCESS == find_ret && start_it == end_it)
    {
      tablet_info = root_table_->get_tablet_info(start_it);
      if (range.equal(tablet_info->range_))
      {
        if (keep_src)
        {
          if (OB_INVALID_INDEX == dest_server_index)
          {
            // dest cs is down
            TBSYS_LOG(WARN, "can not find cs, src=%d dest=%d", src_server_index, dest_server_index);
            ret = OB_ENTRY_NOT_EXIST;
          }
          else
          {
            // add replica
            ret = root_table_->modify(start_it, dest_server_index, tablet_version);
          }
        }
        else
        {
          // dest_server_index and src_server_index may be INVALID
          ret = root_table_->replace(start_it, src_server_index, dest_server_index, tablet_version);
        }
        ObServerStatus* src_status = server_manager_.get_server_status(src_server_index);
        ObServerStatus* dest_status = server_manager_.get_server_status(dest_server_index);
        const common::ObTabletInfo* tablet_info = NULL;
        tablet_info = ((const ObRootTable2*)root_table_)->get_tablet_info(start_it);
        if (src_status != NULL && dest_status != NULL && tablet_info != NULL)
        {
          if (!keep_src)
          {
            if (OB_INVALID_INDEX != src_server_index)
            {
              src_status->disk_info_.set_used(src_status->disk_info_.get_used() - tablet_info->occupy_size_);
            }
          }
          if (OB_INVALID_INDEX != dest_server_index)
          {
            dest_status->disk_info_.set_used(dest_status->disk_info_.get_used() + tablet_info->occupy_size_);
          }
        }
        if (is_master())
        {
          ret = log_worker_->cs_migrate_done(range, src_server, dest_server, keep_src, tablet_version);
        }
      }
      else
      {
        static char row_key_dump_buff[OB_MAX_ROW_KEY_LENGTH * 2];
        range.to_string(row_key_dump_buff, OB_MAX_ROW_KEY_LENGTH * 2);
        static char rt_range[OB_MAX_ROW_KEY_LENGTH * 2];
        tablet_info->range_.to_string(rt_range, OB_MAX_ROW_KEY_LENGTH * 2);
        ret = OB_ERROR;
        TBSYS_LOG(WARN, "can not find the right range ignore this, migrate range=%s, roottable range=%s",
            row_key_dump_buff, rt_range);
      }
    }
    else
    {
      TBSYS_LOG(WARN, "fail to find range in roottable. find_ret=%d, start_it=%p, end_it=%p",
          find_ret, start_it, end_it);
      ret = OB_ERROR;
    }
  } //end lock
  if (OB_SUCCESS == ret)
  {
    if (is_master() || worker_->get_role_manager()->get_role() == ObRoleMgr::STANDALONE)
    {
      balancer_->nb_trigger_next_migrate(const_cast<ObRootTable2::iterator>(start_it),
          tablet_info, src_server_index, dest_server_index, keep_src);
    }
  }
  return ret;
}

    int ObRootServer2::make_out_cell(ObCellInfo& out_cell, ObRootTable2::const_iterator first,
        ObRootTable2::const_iterator last, ObScanner& scanner, const int32_t max_row_count, const int32_t max_key_len) const
    {
      static ObString s_root_1_port(static_cast<int32_t>(strlen(ROOT_1_PORT)), static_cast<int32_t>(strlen(ROOT_1_PORT)), (char*)ROOT_1_PORT);
      static ObString s_root_1_ms_port(static_cast<int32_t>(strlen(ROOT_1_MS_PORT)), static_cast<int32_t>(strlen(ROOT_1_MS_PORT)), (char*)ROOT_1_MS_PORT);
      static ObString s_root_1_ipv6_1(static_cast<int32_t>(strlen(ROOT_1_IPV6_1)), static_cast<int32_t>(strlen(ROOT_1_IPV6_1)), (char*)ROOT_1_IPV6_1);
      static ObString s_root_1_ipv6_2(static_cast<int32_t>(strlen(ROOT_1_IPV6_2)), static_cast<int32_t>(strlen(ROOT_1_IPV6_2)), (char*)ROOT_1_IPV6_2);
      static ObString s_root_1_ipv6_3(static_cast<int32_t>(strlen(ROOT_1_IPV6_3)), static_cast<int32_t>(strlen(ROOT_1_IPV6_3)), (char*)ROOT_1_IPV6_3);
      static ObString s_root_1_ipv6_4(static_cast<int32_t>(strlen(ROOT_1_IPV6_4)), static_cast<int32_t>(strlen(ROOT_1_IPV6_4)), (char*)ROOT_1_IPV6_4);
      static ObString s_root_1_ipv4(  static_cast<int32_t>(strlen(ROOT_1_IPV4)), static_cast<int32_t>(strlen(ROOT_1_IPV4)), (char*)ROOT_1_IPV4);
      static ObString s_root_1_tablet_version(static_cast<int32_t>(strlen(ROOT_1_TABLET_VERSION)), static_cast<int32_t>(strlen(ROOT_1_TABLET_VERSION)), (char*)ROOT_1_TABLET_VERSION);

      static ObString s_root_2_port(   static_cast<int32_t>(strlen(ROOT_2_PORT)),    static_cast<int32_t>(strlen(ROOT_2_PORT)),    (char*)ROOT_2_PORT);
      static ObString s_root_2_ms_port(static_cast<int32_t>(strlen(ROOT_2_MS_PORT)), static_cast<int32_t>(strlen(ROOT_2_MS_PORT)), (char*)ROOT_2_MS_PORT);
      static ObString s_root_2_ipv6_1( static_cast<int32_t>(strlen(ROOT_2_IPV6_1)),  static_cast<int32_t>(strlen(ROOT_2_IPV6_1)),  (char*)ROOT_2_IPV6_1);
      static ObString s_root_2_ipv6_2( static_cast<int32_t>(strlen(ROOT_2_IPV6_2)),  static_cast<int32_t>(strlen(ROOT_2_IPV6_2)),  (char*)ROOT_2_IPV6_2);
      static ObString s_root_2_ipv6_3( static_cast<int32_t>(strlen(ROOT_2_IPV6_3)),  static_cast<int32_t>(strlen(ROOT_2_IPV6_3)),  (char*)ROOT_2_IPV6_3);
      static ObString s_root_2_ipv6_4( static_cast<int32_t>(strlen(ROOT_2_IPV6_4)),  static_cast<int32_t>(strlen(ROOT_2_IPV6_4)),  (char*)ROOT_2_IPV6_4);
      static ObString s_root_2_ipv4(   static_cast<int32_t>(strlen(ROOT_2_IPV4)),    static_cast<int32_t>(strlen(ROOT_2_IPV4)),    (char*)ROOT_2_IPV4);
      static ObString s_root_2_tablet_version(static_cast<int32_t>(strlen(ROOT_2_TABLET_VERSION)), static_cast<int32_t>(strlen(ROOT_2_TABLET_VERSION)), (char*)ROOT_2_TABLET_VERSION);

      static ObString s_root_3_port(   static_cast<int32_t>(strlen(ROOT_3_PORT)),    static_cast<int32_t>(strlen(ROOT_3_PORT)),    (char*)ROOT_3_PORT);
      static ObString s_root_3_ms_port(static_cast<int32_t>(strlen(ROOT_3_MS_PORT)), static_cast<int32_t>(strlen(ROOT_3_MS_PORT)), (char*)ROOT_3_MS_PORT);
      static ObString s_root_3_ipv6_1( static_cast<int32_t>(strlen(ROOT_3_IPV6_1)),  static_cast<int32_t>(strlen(ROOT_3_IPV6_1)),  (char*)ROOT_3_IPV6_1);
      static ObString s_root_3_ipv6_2( static_cast<int32_t>(strlen(ROOT_3_IPV6_2)),  static_cast<int32_t>(strlen(ROOT_3_IPV6_2)),  (char*)ROOT_3_IPV6_2);
      static ObString s_root_3_ipv6_3( static_cast<int32_t>(strlen(ROOT_3_IPV6_3)),  static_cast<int32_t>(strlen(ROOT_3_IPV6_3)),  (char*)ROOT_3_IPV6_3);
      static ObString s_root_3_ipv6_4( static_cast<int32_t>(strlen(ROOT_3_IPV6_4)),  static_cast<int32_t>(strlen(ROOT_3_IPV6_4)),  (char*)ROOT_3_IPV6_4);
      static ObString s_root_3_ipv4(   static_cast<int32_t>(strlen(ROOT_3_IPV4)),    static_cast<int32_t>(strlen(ROOT_3_IPV4)),    (char*)ROOT_3_IPV4);
      static ObString s_root_3_tablet_version(static_cast<int32_t>(strlen(ROOT_3_TABLET_VERSION)), static_cast<int32_t>(strlen(ROOT_3_TABLET_VERSION)), (char*)ROOT_3_TABLET_VERSION);

      static ObString s_root_occupy_size (static_cast<int32_t>(strlen(ROOT_OCCUPY_SIZE)), static_cast<int32_t>(strlen(ROOT_OCCUPY_SIZE)), (char*)ROOT_OCCUPY_SIZE);
      static ObString s_root_record_count(static_cast<int32_t>(strlen(ROOT_RECORD_COUNT)), static_cast<int32_t>(strlen(ROOT_RECORD_COUNT)), (char*)ROOT_RECORD_COUNT);
      static char c = 0;

      int ret = OB_SUCCESS;
      if (c == 0)
      {
        memset(max_row_key, 0xff, OB_MAX_ROW_KEY_LENGTH);
        c = 1;
      }

      const common::ObTabletInfo* tablet_info = NULL;
      int count = 0;
      ObRootTable2::const_iterator it = first;
      for (; it <= last; it++)
      {
        if (count > max_row_count) break;
        tablet_info = ((const ObRootTable2*)root_table_)->get_tablet_info(it);
        if (tablet_info == NULL)
        {
          TBSYS_LOG(ERROR, "you should not reach this bugs");
          break;
        }
        out_cell.row_key_ = tablet_info->range_.end_key_;
        char buf[1024];
        tablet_info->range_.to_string(buf,1024);
        TBSYS_LOG(DEBUG,"add range %s",buf);
        if (tablet_info->range_.border_flag_.is_max_value())
        {
          TBSYS_LOG(DEBUG,"this tablet is the last tablet,table_id is %lu,max_key_len is %d",tablet_info->range_.table_id_,max_key_len);
          out_cell.row_key_.assign(max_row_key, max_key_len);
        }
        TBSYS_LOG(DEBUG, "add a row key to out cell, length = %d", out_cell.row_key_.length());
        count++;
        //start one row
        out_cell.column_name_ = s_root_occupy_size;
        out_cell.value_.set_int(tablet_info->occupy_size_);
        if (OB_SUCCESS != (ret = scanner.add_cell(out_cell)))
        {
          break;
        }

        out_cell.column_name_ = s_root_record_count;
        out_cell.value_.set_int(tablet_info->row_count_);
        if (OB_SUCCESS != (ret = scanner.add_cell(out_cell)))
        {
          break;
        }

        const ObServerStatus* server_status = NULL;
        if (it->server_info_indexes_[0] != OB_INVALID_INDEX &&
            (server_status = server_manager_.get_server_status(it->server_info_indexes_[0])) != NULL)
        {
          out_cell.column_name_ = s_root_1_port;
          out_cell.value_.set_int(server_status->port_cs_);

          if (OB_SUCCESS != (ret = scanner.add_cell(out_cell)))
          {
            break;
          }
          if (server_status->server_.get_version() != ObServer::IPV4)
          {
            ret = OB_NOT_SUPPORTED;
            break;
          }
          out_cell.column_name_ = s_root_1_ms_port;
          out_cell.value_.set_int(server_status->port_ms_);
          if (OB_SUCCESS != (ret = scanner.add_cell(out_cell)))
          {
            break;
          }

          out_cell.column_name_ = s_root_1_ipv4;
          out_cell.value_.set_int(server_status->server_.get_ipv4());
          if (OB_SUCCESS != (ret = scanner.add_cell(out_cell)))
          {
            break;
          }

          out_cell.column_name_ = s_root_1_tablet_version;
          out_cell.value_.set_int(it->tablet_version_[0]);
          if (OB_SUCCESS != (ret = scanner.add_cell(out_cell)))
          {
            break;
          }
        }
        if (it->server_info_indexes_[1] != OB_INVALID_INDEX &&
            (server_status = server_manager_.get_server_status(it->server_info_indexes_[1])) != NULL)
        {
          out_cell.column_name_ = s_root_2_port;
          out_cell.value_.set_int(server_status->port_cs_);
          if (OB_SUCCESS != (ret = scanner.add_cell(out_cell)))
          {
            break;
          }
          if (server_status->server_.get_version() != ObServer::IPV4)
          {
            ret = OB_NOT_SUPPORTED;
            break;
          }
          out_cell.column_name_ = s_root_2_ms_port;
          out_cell.value_.set_int(server_status->port_ms_);
          if (OB_SUCCESS != (ret = scanner.add_cell(out_cell)))
          {
            break;
          }

          out_cell.column_name_ = s_root_2_ipv4;
          out_cell.value_.set_int(server_status->server_.get_ipv4());
          if (OB_SUCCESS != (ret = scanner.add_cell(out_cell)))
          {
            break;
          }

          out_cell.column_name_ = s_root_2_tablet_version;
          out_cell.value_.set_int(it->tablet_version_[1]);
          if (OB_SUCCESS != (ret = scanner.add_cell(out_cell)))
          {
            break;
          }
        }
        if (it->server_info_indexes_[2] != OB_INVALID_INDEX &&
            (server_status = server_manager_.get_server_status(it->server_info_indexes_[2])) != NULL)
        {
          out_cell.column_name_ = s_root_3_port;
          out_cell.value_.set_int(server_status->port_cs_);
          if (OB_SUCCESS != (ret = scanner.add_cell(out_cell)))
          {
            break;
          }
          if (server_status->server_.get_version() != ObServer::IPV4)
          {
            ret = OB_NOT_SUPPORTED;
            break;
          }
          out_cell.column_name_ = s_root_3_ms_port;
          out_cell.value_.set_int(server_status->port_ms_);
          if (OB_SUCCESS != (ret = scanner.add_cell(out_cell)))
          {
            break;
          }

          out_cell.column_name_ = s_root_3_ipv4;
          out_cell.value_.set_int(server_status->server_.get_ipv4());
          if (OB_SUCCESS != (ret = scanner.add_cell(out_cell)))
          {
            break;
          }

          out_cell.column_name_ = s_root_3_tablet_version;
          out_cell.value_.set_int(it->tablet_version_[2]);
          if (OB_SUCCESS != (ret = scanner.add_cell(out_cell)))
          {
            break;
          }
        }

      } // end for each tablet
      return ret;
    }

    int ObRootServer2::find_root_table_key(const common::ObGetParam& get_param, common::ObScanner& scanner) const
    {
      int ret = OB_SUCCESS;
      const ObCellInfo* cell = NULL;

      if (NULL == (cell = get_param[0]))
      {
        TBSYS_LOG(WARN, "invalid get_param, cell_size=%ld", get_param.get_cell_size());
        ret = OB_INVALID_ARGUMENT;
      }
      else if (get_param.get_is_read_consistency() && obi_role_.get_role() != ObiRole::MASTER)
      {
        TBSYS_LOG(WARN, "we are not a master instance");
        ret = OB_NOT_MASTER;
      }
      else
      {
        int8_t rt_type = 0;
        UNUSED(rt_type); // for now we ignore this; OP_RT_TABLE_TYPE or OP_RT_TABLE_INDEX_TYPE

        if (cell->table_id_ != 0 && cell->table_id_ != OB_INVALID_ID)
        {
          char table_name_buff[OB_MAX_TABLE_NAME_LENGTH];
          ObString table_name(OB_MAX_TABLE_NAME_LENGTH, 0, table_name_buff);
          int32_t max_key_len = 0;
          if (OB_SUCCESS != (ret = get_table_info(cell->table_id_, table_name, max_key_len)))
          {
            TBSYS_LOG(WARN, "failed to get table name, err=%d table_id=%lu", ret, cell->table_id_);
          }
          else if (OB_SUCCESS != (ret = find_root_table_key(cell->table_id_, table_name, max_key_len, cell->row_key_, scanner)))
          {
            TBSYS_LOG(WARN, "failed to get tablet, err=%d table_id=%lu", ret, cell->table_id_);
          }
        }
        else
        {
          int32_t max_key_len = 0;
          uint64_t table_id = get_table_info(cell->table_name_, max_key_len);
          if (OB_INVALID_ID == table_id)
          {
            TBSYS_LOG(WARN, "failed to get table id, err=%d", ret);
          }
          else if (OB_SUCCESS != (ret = find_root_table_key(table_id, cell->table_name_, max_key_len, cell->row_key_, scanner)))
          {
            TBSYS_LOG(WARN, "failed to get tablet, err=%d table_id=%lu", ret, cell->table_id_);
          }
        }
      }
      return ret;
    }

    int ObRootServer2::find_root_table_key(const uint64_t table_id, const ObString& table_name, const int32_t max_key_len, const common::ObString& key, ObScanner& scanner) const
    {
      int ret = OB_SUCCESS;
      if (table_id == OB_INVALID_ID || 0 == table_id)
      {
        ret = OB_INVALID_ARGUMENT;
      }
      if (OB_SUCCESS == ret)
      {
        ObCellInfo out_cell;
        out_cell.table_name_ = table_name;
        tbsys::CRLockGuard guard(root_table_rwlock_);
        if (root_table_ == NULL)
        {
          ret = OB_NOT_INIT;
        }
        else
        {
          ObRootTable2::const_iterator first;
          ObRootTable2::const_iterator last;
          ObRootTable2::const_iterator ptr;
          ret = root_table_->find_key(table_id, key, RETURN_BACH_COUNT, first, last, ptr);
          TBSYS_LOG(DEBUG, "first %p last %p ptr %p", first, last, ptr);
          if (ret == OB_SUCCESS)
          {
            if (first == ptr)
            {
              // make a fake startkey
              out_cell.value_.set_ext(ObActionFlag::OP_ROW_DOES_NOT_EXIST);
              ret = scanner.add_cell(out_cell);
              out_cell.value_.reset();
            }
            if (OB_SUCCESS == ret)
            {
              ret = make_out_cell(out_cell, first, last, scanner, RETURN_BACH_COUNT, max_key_len);
            }
          }
          else if (config_.flag_is_import_.get() && OB_ERROR_OUT_OF_RANGE == ret)
          {
            TBSYS_LOG(WARN, "import application cann't privide service while importing");
            ret = OB_IMPORT_NOT_IN_SERVER;
          }
        }
      }
      return ret;
    }

    int ObRootServer2::find_root_table_range(const common::ObScanParam& scan_param, ObScanner& scanner) const
    {

      int ret = OB_SUCCESS;
      const ObString &table_name = scan_param.get_table_name();
      const ObRange &key_range = *scan_param.get_range();
      int32_t max_key_len = 0;
      uint64_t table_id = get_table_info(table_name, max_key_len);

      if (0 == table_id || OB_INVALID_ID == table_id)
      {
        TBSYS_LOG(WARN,"table name are invaild:%s,%d",table_name.ptr(),table_name.length());
        ret = OB_INVALID_ARGUMENT;
      }
      else if (scan_param.get_is_read_consistency() && obi_role_.get_role() != ObiRole::MASTER)
      {
        TBSYS_LOG(INFO, "we are not a master instance");
        ret = OB_NOT_MASTER;
      }
      else
      {
        ObCellInfo out_cell;
        out_cell.table_name_ = table_name;
        tbsys::CRLockGuard guard(root_table_rwlock_);
        if (root_table_ == NULL)
        {
          ret = OB_NOT_INIT;
          TBSYS_LOG(WARN,"scan request in initialize phase");
        }
        else
        {
          ObRootTable2::const_iterator first;
          ObRootTable2::const_iterator last;
          ObRange search_range = key_range;
          search_range.table_id_ = table_id;
          ret = root_table_->find_range(search_range, first, last);
          if (ret != OB_SUCCESS)
          {
            TBSYS_LOG(WARN,"cann't find this range,ret[%d]",ret);
          }
          else
          {
            if ((ret = make_out_cell(out_cell, first, last, scanner,
                    MAX_RETURN_BACH_ROW_COUNT, max_key_len)) != OB_SUCCESS)
            {
              TBSYS_LOG(WARN,"make out cell failed,ret[%d]",ret);
            }
          }
        }
      }
      return ret;
    }

    /*
     * CS汇报tablet结束
     *
     */
    int ObRootServer2::waiting_job_done(const common::ObServer& server, const int64_t frozen_mem_version)
    {
      int ret = OB_ENTRY_NOT_EXIST;
      UNUSED(frozen_mem_version);
      ObChunkServerManager::iterator it;
      tbsys::CWLockGuard guard(server_manager_rwlock_);
      it = server_manager_.find_by_ip(server);
      if (it != server_manager_.end())
      {
        char f_server[OB_IP_STR_BUFF];
        server.to_string(f_server, OB_IP_STR_BUFF);
        TBSYS_LOG(INFO, "cs waiting_job_done, status=%d mem_version=%ld", it->status_, frozen_mem_version_);
        if (is_master())
        {
          log_worker_->cs_merge_over(server, frozen_mem_version);
        }
        if (it->status_ == ObServerStatus::STATUS_REPORTING)
        {
          it->status_ = ObServerStatus::STATUS_REPORTED;
          if (frozen_mem_version_ == -1) // not in merge process
          {
            it->status_ = ObServerStatus::STATUS_SERVING;
          }
        }
        ret = OB_SUCCESS;
      }
      return ret;
    }
    int ObRootServer2::get_server_index(const common::ObServer& server) const
    {
      int ret = OB_INVALID_INDEX;
      ObChunkServerManager::const_iterator it;
      tbsys::CRLockGuard guard(server_manager_rwlock_);
      it = server_manager_.find_by_ip(server);
      if (it != server_manager_.end())
      {
        if (ObServerStatus::STATUS_DEAD != it->status_)
        {
          ret = static_cast<int32_t>(it - server_manager_.begin());
        }
      }
      return ret;
    }
    int ObRootServer2::report_tablets(const ObServer& server, const ObTabletReportInfoList& tablets, const int64_t frozen_mem_version)
    {
      int return_code = OB_SUCCESS;
      char t_server[OB_IP_STR_BUFF];
      server.to_string(t_server, OB_IP_STR_BUFF);
      int server_index = get_server_index(server);
      if (server_index == OB_INVALID_INDEX)
      {
        TBSYS_LOG(WARN, "can not find server's info, server=%s", t_server);
        return_code = OB_ENTRY_NOT_EXIST;
      }
      else
      {
        TBSYS_LOG_US(INFO, "[NOTICE] report tablets, server=%d ip=%s count=%ld version=%ld",
            server_index, t_server, tablets.tablet_list_.get_array_index(), frozen_mem_version);
        if (is_master())
        {
          log_worker_->report_tablets(server, tablets, frozen_mem_version);
        }
        return_code = got_reported(tablets, server_index, frozen_mem_version);
        TBSYS_LOG_US(INFO, "got_reported over");
      }
      return return_code;
    }
    /*
     * 收到汇报消息后调用
     */
    int ObRootServer2::got_reported(const ObTabletReportInfoList& tablets, const int server_index, const int64_t frozen_mem_version)
    {
      int ret = OB_SUCCESS;
      int64_t index = tablets.tablet_list_.get_array_index();
      ObTabletReportInfo* p_table_info = NULL;
      tbsys::CThreadGuard guard(&root_table_build_mutex_);
      if (NULL != root_table_for_build_ )
      {
        TBSYS_LOG(INFO, "will add tablet info to root_table_for_build");
        for(int64_t i = 0; i < index; ++i)
        {
          p_table_info = tablets.tablet_list_.at(i);
          if (p_table_info != NULL)
          {
            ret = got_reported_for_build(p_table_info->tablet_info_, server_index,
                p_table_info->tablet_location_.tablet_version_);
            if (ret != OB_SUCCESS)
            {
              TBSYS_LOG(WARN, "report_tablets error code is %d", ret);
              break;
            }
          }
        }
      }
      else
      {
        TBSYS_LOG(INFO, "will add tablet info to root_table_for_query");
        delete_list_.reset();
        got_reported_for_query_table(tablets, server_index, frozen_mem_version);
        if (0 < delete_list_.get_tablet_size())
        {
          if (is_master() || worker_->get_role_manager()->get_role() == ObRoleMgr::STANDALONE)
          {
            //ObRootUtil::delete_tablets(worker_->get_rpc_stub(), server_manager_, delete_list_, config_.flag_network_timeout_us_.get());
            TBSYS_LOG(INFO, "need to delete tablet. tablet_num=%ld", delete_list_.get_tablet_size());
            worker_->submit_delete_tablets_task(delete_list_);
          }
        }
      }
      return ret;
    }
    /*
     * 处理汇报消息, 直接写到当前的root table中
     * 如果发现汇报消息中有对当前root table的tablet的分裂或者合并
     * 要调用采用写拷贝机制的处理函数
     */
    int ObRootServer2::got_reported_for_query_table(const ObTabletReportInfoList& tablets,
        const int32_t server_index, const int64_t frozen_mem_version)
    {
      UNUSED(frozen_mem_version);
      int ret = OB_SUCCESS;
      int64_t have_done_index = 0;
      bool need_split = false;
      bool need_add = false;
      ObServerStatus* new_server_status = server_manager_.get_server_status(server_index);
      if (new_server_status == NULL)
      {
        TBSYS_LOG(ERROR, "can not find server");
      }
      else
      {
        ObTabletReportInfo* p_table_info = NULL;
        ObRootTable2::const_iterator first;
        ObRootTable2::const_iterator last;
        int64_t index = tablets.tablet_list_.get_array_index();
        int find_ret = OB_SUCCESS;
        common::ObTabletInfo* tablet_info = NULL;
        int range_pos_type = ObRootTable2::POS_TYPE_ERROR;

        tbsys::CRLockGuard guard(root_table_rwlock_);
        for(have_done_index = 0; have_done_index < index; ++have_done_index)
        {
          p_table_info = tablets.tablet_list_.at(have_done_index);
          if (p_table_info != NULL)
          {
            //TODO(maoqi) check the table of this tablet is exist in schema
            //if (NULL == schema_manager_->get_table_schema(p_table_info->tablet_info_.range_.table_id_))
            //{
            //  continue;
            //}
            if (!p_table_info->tablet_info_.range_.border_flag_.is_left_open_right_closed())
            {
              static char row_key_dump_buff[OB_MAX_ROW_KEY_LENGTH * 2];
              p_table_info->tablet_info_.range_.to_string(row_key_dump_buff, OB_MAX_ROW_KEY_LENGTH * 2);
              TBSYS_LOG(WARN, "cs reported illegal tablet, server=%d range=%s", server_index, row_key_dump_buff);
            }
            tablet_info = NULL;
            find_ret = root_table_->find_range(p_table_info->tablet_info_.range_, first, last);
            TBSYS_LOG(DEBUG, "root_table_for_query_->find_range ret = %d", find_ret);
            if (OB_SUCCESS == find_ret)
            {
              tablet_info = root_table_->get_tablet_info(first);
            }
            range_pos_type = ObRootTable2::POS_TYPE_ERROR;
            if (NULL != tablet_info)
            {
              range_pos_type = root_table_->get_range_pos_type(p_table_info->tablet_info_.range_, first, last);
            }
            TBSYS_LOG(DEBUG, " range_pos_type = %d", range_pos_type);
            if (range_pos_type == ObRootTable2::POS_TYPE_SPLIT_RANGE)
            {
              need_split = true;  //will create a new table to deal with the left
              break;
            }
            //else if (range_pos_type == ObRootTable2::POS_TYPE_ADD_RANGE)
            //{
            //  need_add = true;
            //  break;
            //}
            if (NULL != tablet_info &&
                (range_pos_type == ObRootTable2::POS_TYPE_SAME_RANGE || range_pos_type == ObRootTable2::POS_TYPE_MERGE_RANGE)
               )
            {
              if (OB_SUCCESS != write_new_info_to_root_table(p_table_info->tablet_info_,
                    p_table_info->tablet_location_.tablet_version_, server_index, first, last, root_table_))
              {
                TBSYS_LOG(ERROR, "write new tablet error");
                set_daily_merge_tablet_error();
                static char row_key_dump_buff[OB_MAX_ROW_KEY_LENGTH * 2];
                p_table_info->tablet_info_.range_.to_string(row_key_dump_buff, OB_MAX_ROW_KEY_LENGTH * 2);
                TBSYS_LOG(INFO, "%s", row_key_dump_buff);
                //p_table_info->tablet_info_.range_.hex_dump(TBSYS_LOG_LEVEL_ERROR);
              }
            }
            else
            {
              TBSYS_LOG(INFO, "can not found range ignore this");
              static char row_key_dump_buff[OB_MAX_ROW_KEY_LENGTH * 2];
              p_table_info->tablet_info_.range_.to_string(row_key_dump_buff, OB_MAX_ROW_KEY_LENGTH * 2);
              TBSYS_LOG(INFO, "%s", row_key_dump_buff);
              //p_table_info->tablet_info_.range_.hex_dump(TBSYS_LOG_LEVEL_INFO);
            }
          }
        }
      } //end else, release lock
      if (need_split || need_add)
      {
        ret = got_reported_with_copy(tablets, server_index, have_done_index);
      }
      return ret;
    }
    /*
     * 写拷贝机制的,处理汇报消息
     */
    int ObRootServer2::got_reported_with_copy(const ObTabletReportInfoList& tablets,
        const int32_t server_index, const int64_t have_done_index)
    {
      int ret = OB_SUCCESS;
      common::ObTabletInfo* tablet_info = NULL;
      ObTabletReportInfo* p_table_info = NULL;
      ObServerStatus* new_server_status = server_manager_.get_server_status(server_index);
      ObRootTable2::const_iterator first;
      ObRootTable2::const_iterator last;
      TBSYS_LOG(DEBUG, "root table write on copy");
      if (new_server_status == NULL)
      {
        TBSYS_LOG(ERROR, "can not find server");
        ret = OB_ERROR;
      }
      else
      {
        ObRootTable2* root_table_for_split = new ObRootTable2(NULL);
        if (root_table_for_split == NULL)
        {
          TBSYS_LOG(ERROR, "new ObRootTable2 error");
          ret = OB_ERROR;
        }
        else
        {
          *root_table_for_split = *root_table_;
          int range_pos_type = ObRootTable2::POS_TYPE_ERROR;
          for (int64_t index = have_done_index; OB_SUCCESS == ret && index < tablets.tablet_list_.get_array_index(); index++)
          {
            p_table_info = tablets.tablet_list_.at(index);
            if (p_table_info != NULL)
            {
              tablet_info = NULL;
              int find_ret = root_table_for_split->find_range(p_table_info->tablet_info_.range_, first, last);
              if (OB_SUCCESS == find_ret)
              {
                tablet_info = root_table_for_split->get_tablet_info(first);
              }
              range_pos_type = ObRootTable2::POS_TYPE_ERROR;
              if (NULL != tablet_info)
              {
                range_pos_type = root_table_for_split->get_range_pos_type(p_table_info->tablet_info_.range_, first, last);
              }
              if (NULL != tablet_info  )
              {
                if (range_pos_type == ObRootTable2::POS_TYPE_SAME_RANGE || range_pos_type == ObRootTable2::POS_TYPE_MERGE_RANGE)
                {
                  if (OB_SUCCESS != write_new_info_to_root_table(p_table_info->tablet_info_,
                        p_table_info->tablet_location_.tablet_version_, server_index, first, last, root_table_for_split))
                  {
                    TBSYS_LOG(ERROR, "write new tablet error");
                    set_daily_merge_tablet_error();
                    static char row_key_dump_buff[OB_MAX_ROW_KEY_LENGTH * 2];
                    p_table_info->tablet_info_.range_.to_string(row_key_dump_buff, OB_MAX_ROW_KEY_LENGTH * 2);
                    TBSYS_LOG(INFO, "%s", row_key_dump_buff);
                    //p_table_info->tablet_info_.range_.hex_dump(TBSYS_LOG_LEVEL_ERROR);
                  }
                }
                else if (range_pos_type == ObRootTable2::POS_TYPE_SPLIT_RANGE)
                {
                  if (ObRootTable2::get_max_tablet_version(first) >= p_table_info->tablet_location_.tablet_version_)
                  {
                    TBSYS_LOG(ERROR, "same version different range error !! version %ld",
                        p_table_info->tablet_location_.tablet_version_);
                    set_daily_merge_tablet_error();
                    static char row_key_dump_buff[OB_MAX_ROW_KEY_LENGTH * 2];
                    p_table_info->tablet_info_.range_.to_string(row_key_dump_buff, OB_MAX_ROW_KEY_LENGTH * 2);
                    TBSYS_LOG(INFO, "%s", row_key_dump_buff);
                    //p_table_info->tablet_info_.range_.hex_dump(TBSYS_LOG_LEVEL_ERROR);
                  }
                  else
                  {
                    ret = root_table_for_split->split_range(p_table_info->tablet_info_, first,
                        p_table_info->tablet_location_.tablet_version_, server_index);
                    if (OB_SUCCESS != ret)
                    {
                      TBSYS_LOG(ERROR, "split range error, ret = %d", ret);
                      set_daily_merge_tablet_error();
                      static char row_key_dump_buff[OB_MAX_ROW_KEY_LENGTH * 2];
                      p_table_info->tablet_info_.range_.to_string(row_key_dump_buff, OB_MAX_ROW_KEY_LENGTH * 2);
                      TBSYS_LOG(INFO, "%s", row_key_dump_buff);
                      //p_table_info->tablet_info_.range_.hex_dump(TBSYS_LOG_LEVEL_ERROR);
                    }
                  }
                }
                //else if (range_pos_type == ObRootTable2::POS_TYPE_ADD_RANGE)
                //{
                //  ret = root_table_for_split->add_range(p_table_info->tablet_info_, first,
                //      p_table_info->tablet_location_.tablet_version_, server_index);
                //}
                else
                {
                  TBSYS_LOG(INFO, "error range be ignored range_pos_type =%d",range_pos_type );
                  static char row_key_dump_buff[OB_MAX_ROW_KEY_LENGTH * 2];
                  p_table_info->tablet_info_.range_.to_string(row_key_dump_buff, OB_MAX_ROW_KEY_LENGTH * 2);
                  TBSYS_LOG(INFO, "%s", row_key_dump_buff);
                  //p_table_info->tablet_info_.range_.hex_dump(TBSYS_LOG_LEVEL_INFO);
                }
              }
            }
          }
        }

        if (OB_SUCCESS == ret && root_table_for_split != NULL)
        {
          switch_root_table(root_table_for_split, NULL);
          root_table_for_split = NULL;
        }
        else
        {
          if (root_table_for_split != NULL)
          {
            delete root_table_for_split;
            root_table_for_split = NULL;
          }
        }
      }
      return ret;
    }

    /*
     * 在一个tabelt的各份拷贝中, 寻找合适的备份替换掉
     */
    int ObRootServer2::write_new_info_to_root_table(
        const ObTabletInfo& tablet_info, const int64_t tablet_version, const int32_t server_index,
        ObRootTable2::const_iterator& first, ObRootTable2::const_iterator& last, ObRootTable2 *p_root_table)
    {
      int ret = OB_SUCCESS;
      int32_t found_it_index = OB_INVALID_INDEX;
      int64_t max_tablet_version = 0;
      ObServerStatus* server_status = NULL;
      ObServerStatus* new_server_status = server_manager_.get_server_status(server_index);
      if (new_server_status == NULL)
      {
        TBSYS_LOG(ERROR, "can not find server");
        ret = OB_ERROR;
      }
      else
      {
        for (ObRootTable2::const_iterator it = first; it <= last; it++)
        {
          ObTabletInfo *p_tablet_write = p_root_table->get_tablet_info(it);
          ObTabletCrcHistoryHelper *crc_helper = p_root_table->get_crc_helper(it);
          if (crc_helper == NULL)
          {
            TBSYS_LOG(ERROR, "%s", "get src helper error should not reach this bugs!!");
            ret = OB_ERROR;
            break;
          }
          max_tablet_version = ObRootTable2::get_max_tablet_version(it);
          if (tablet_version >= max_tablet_version)
          {
            if (first != last)
            {
              TBSYS_LOG(ERROR, "we should not have merge tablet max tabelt is %ld this one is %ld",
                  max_tablet_version, tablet_version);
              ret = OB_ERROR;
              break;
            }
          }
          if (first == last)
          {
            //check crc sum
            ret = crc_helper->check_and_update(tablet_version, tablet_info.crc_sum_);
            if (ret != OB_SUCCESS)
            {
              TBSYS_LOG(ERROR, "check crc sum error crc is %lu tablet is", tablet_info.crc_sum_);
              static char row_key_dump_buff[OB_MAX_ROW_KEY_LENGTH * 2];
              tablet_info.range_.to_string(row_key_dump_buff, OB_MAX_ROW_KEY_LENGTH * 2);
              TBSYS_LOG(INFO, "%s", row_key_dump_buff);
              it->dump();
              //tablet_info.range_.hex_dump(TBSYS_LOG_LEVEL_ERROR);
              break;
            }
          }
          //try to over write dead server or old server;
          ObTabletReportInfo to_delete;
          to_delete.tablet_location_.chunkserver_.set_port(-1);
          found_it_index = ObRootTable2::find_suitable_pos(it, server_index, tablet_version, &to_delete);
          if (found_it_index == OB_INVALID_INDEX)
          {
            //find the serve who have max free disk
            for (int32_t i = 0; i < OB_SAFE_COPY_COUNT; i++)
            {
              server_status = server_manager_.get_server_status(it->server_info_indexes_[i]);
              if (server_status != NULL &&
                  new_server_status->disk_info_.get_percent() > 0 &&
                  server_status->disk_info_.get_percent() > new_server_status->disk_info_.get_percent())
              {
                found_it_index = i;
              }
            }
            if (OB_INVALID_INDEX != found_it_index)
            {
              to_delete.tablet_info_ = tablet_info;
              to_delete.tablet_location_.tablet_version_ = it->tablet_version_[found_it_index];
              to_delete.tablet_location_.chunkserver_.set_port(it->server_info_indexes_[found_it_index]);
              if (OB_SUCCESS != delete_list_.add_tablet(to_delete))
              {
                TBSYS_LOG(WARN, "failed to add into delete list");
              }
            }
          }
          else
          {
            TBSYS_LOG(DEBUG, "find it idx=%d", found_it_index);
            if (-1 != to_delete.tablet_location_.chunkserver_.get_port())
            {
              to_delete.tablet_info_ = tablet_info;
              if (OB_SUCCESS != delete_list_.add_tablet(to_delete))
              {
                TBSYS_LOG(WARN, "failed to add into delete list");
              }
            }
          }
          if (found_it_index != OB_INVALID_INDEX)
          {
            TBSYS_LOG(DEBUG, "write a tablet to root_table found_it_index = %d server_index =%d tablet_version = %ld",
                found_it_index, server_index, tablet_version);

            //tablet_info.range_.hex_dump(TBSYS_LOG_LEVEL_DEBUG);
            static char row_key_dump_buff[OB_MAX_ROW_KEY_LENGTH * 2];
            tablet_info.range_.to_string(row_key_dump_buff, OB_MAX_ROW_KEY_LENGTH * 2);
            TBSYS_LOG(DEBUG, "%s", row_key_dump_buff);
            //over write
            atomic_exchange((uint32_t*) &(it->server_info_indexes_[found_it_index]), server_index);
            atomic_exchange((uint64_t*) &(it->tablet_version_[found_it_index]), tablet_version);
            if (p_tablet_write != NULL)
            {
              atomic_exchange((uint64_t*) &(p_tablet_write->row_count_), tablet_info.row_count_);
              atomic_exchange((uint64_t*) &(p_tablet_write->occupy_size_), tablet_info.occupy_size_);
            }
          }
        }
      }
      return ret;
    }


    /**
     * @brief check the version of tablets
     *
     * @return
     */
    bool ObRootServer2::all_tablet_is_the_last_frozen_version() const
    {
      ObRootTable2::iterator it;
      bool ret = true;
      int32_t new_copy = 0;

      {
        tbsys::CRLockGuard guard(root_table_rwlock_);
        for (it = root_table_->begin(); it < root_table_->sorted_end(); ++it)
        {
          new_copy = 0;

          for (int32_t i = 0; i < OB_SAFE_COPY_COUNT; i++)
          {
            if (OB_INVALID_INDEX == it->server_info_indexes_[i]) //the server is down
            {
            }
            else if (it->tablet_version_[i] >= last_frozen_mem_version_)
            {
              ++new_copy;
            }
          }

          if (new_copy < config_.flag_safe_copy_count_in_init_.get())
          {
            ret = false;
            break;
          }
        } //end all tablet
      } //lock

      return ret;
    }

    bool ObRootServer2::all_tablet_is_the_last_frozen_version_really() const
    {
      bool ret = true;
      ObRootTable2::iterator it;
      tbsys::CRLockGuard guard(root_table_rwlock_);
      for (it = root_table_->begin(); it < root_table_->sorted_end() && ret; ++it)
      {
        for (int32_t i = 0; i < OB_SAFE_COPY_COUNT; i++)
        {
          if (OB_INVALID_INDEX != it->server_info_indexes_[i])
          {
            if (it->tablet_version_[i] != last_frozen_mem_version_)
            {
              ret = false;
              break;
            }
          }
        }
      }
      return ret;
    }

    /*
     * 系统初始化的时候, 处理汇报消息,
     * 信息放到root table for build 结构中
     */
    int ObRootServer2::got_reported_for_build(const ObTabletInfo& tablet, const int32_t server_index, const int64_t version)
    {
      int ret = OB_SUCCESS;
      if (root_table_for_build_ == NULL)
      {
        TBSYS_LOG(ERROR, "no root_table_for_build_ ignore this");
        ret = OB_ERROR;
      }
      else
      {
        static char row_key_dump_buff[OB_MAX_ROW_KEY_LENGTH * 2];
        tablet.range_.to_string(row_key_dump_buff, OB_MAX_ROW_KEY_LENGTH * 2);
        if (!tablet.range_.border_flag_.is_left_open_right_closed())
        {
          TBSYS_LOG(WARN, "cs reported illegal tablet, server=%d crc=%lu range=%s", server_index, tablet.crc_sum_, row_key_dump_buff);
        }
        TBSYS_LOG(DEBUG, "add a tablet, server=%d crc=%lu version=%ld range=%s", server_index, tablet.crc_sum_, version, row_key_dump_buff);
        //TODO(maoqi) check the table of this tablet is exist in schema
        //if (schema_manager_->get_table_schema(tablet.range_.table_id_) != NULL)
        //{
        ret = root_table_for_build_->add(tablet, server_index, version);
        //}
      }
      return ret;
    }

    // the array size of server_index should be larger than expected_num
    void ObRootServer2::get_available_servers_for_new_table(int* server_index,
        int32_t expected_num, int32_t &results_num)
{
  //tbsys::CThreadGuard guard(&server_manager_mutex_);
  results_num = 0;
  int64_t mnow = tbsys::CTimeUtil::getMonotonicTime();
  if (next_select_cs_index_ >= server_manager_.get_array_length())
  {
    next_select_cs_index_ = 0;
  }
  ObChunkServerManager::iterator it = server_manager_.begin() + next_select_cs_index_;
  for (; it != server_manager_.end() && results_num < expected_num; ++it)
  {
    if (it->status_ != ObServerStatus::STATUS_DEAD
        && mnow > it->register_time_ + config_.flag_cs_probation_period_seconds_.get() * 1000000L)
    {
      int32_t cs_index = static_cast<int32_t>(it - server_manager_.begin());
      server_index[results_num] = cs_index;
      results_num++;
    }
  }
  if (results_num < expected_num)
  {
    it = server_manager_.begin();
    for(; it != server_manager_.begin() + next_select_cs_index_ && results_num < expected_num; ++it)
    {
      if (it->status_ != ObServerStatus::STATUS_DEAD
          && mnow > it->register_time_ + config_.flag_cs_probation_period_seconds_.get() * 1000000L)
      {
        int32_t cs_index = static_cast<int32_t>(it - server_manager_.begin());
        server_index[results_num] = cs_index;
        results_num++;
      }
    }
  }
  next_select_cs_index_ = it - server_manager_.begin() + 1;
}

    int ObRootServer2::use_new_schema()
    {
      int ret = OB_SUCCESS;
      int64_t schema_timestamp = tbsys::CTimeUtil::getTime();
      common::ObArray<uint64_t> deleted_tables;
      if (!is_master())
      {
        TBSYS_LOG(WARN, "cannot switch schema as a slave");
        ret = OB_NOT_MASTER;
      }
      else if (OB_SUCCESS != (ret = switch_schema(schema_timestamp, deleted_tables)))
      {
        TBSYS_LOG(ERROR, "failed to load and switch new schema, err=%d", ret);
      }
      else if (OB_SUCCESS != (ret = log_worker_->sync_schema(schema_timestamp)))
      {
        TBSYS_LOG(ERROR, "sync schema error, err=%d", ret);
      }
      else
      {
        tbsys::CThreadGuard guard(&root_table_build_mutex_);
        if (!config_.flag_is_import_.get())
        {
          if (OB_SUCCESS != (ret = create_new_table(schema_manager_, NULL)))
          {
            TBSYS_LOG(ERROR, "failed to create new table, err=%d", ret);
          }
        }
        if (OB_SUCCESS == ret)
        {
          if (0 < deleted_tables.count()
              && OB_SUCCESS != (ret = delete_tablets_from_root_table(deleted_tables)))
          {
            TBSYS_LOG(ERROR, "failed to delete tablets, err=%d", ret);
          }
          else if (0 < deleted_tables.count()
              && OB_SUCCESS != (ret = log_worker_->remove_table(deleted_tables)))
          {
            TBSYS_LOG(ERROR, "failed to write commit log, err=%d", ret);
          }
          else
          {
            TBSYS_LOG(INFO, "switch schema succ, ts=%ld", schema_timestamp);
          }
        }
      }

      if (OB_SUCCESS == ret && obi_role_.get_role() == ObiRole::MASTER)
      {
        int ret2 = worker_->get_rpc_stub().switch_schema(get_update_server_info(false), *schema_manager_,
            config_.flag_network_timeout_us_.get());
        if (OB_SUCCESS != ret2)
        {
            TBSYS_LOG(ERROR, "ups_switch_schema error, ret=%d schema_manager_=%p",
                ret2, schema_manager_);
        }
      }
      return ret;
    }

int ObRootServer2::slave_batch_create_new_table(const common::ObTabletInfoList& tablets,
    int32_t** t_server_index, int32_t* replicas_num, const int64_t mem_version)
{
  int ret = OB_SUCCESS;
  int64_t index = tablets.get_tablet_size();
  common::ObTabletInfo *p_table_info = NULL;
  if (NULL != root_table_for_build_)
  {
    tbsys::CThreadGuard guard_build(&root_table_build_mutex_);
    TBSYS_LOG(INFO, "replay log create_new_table and we are initializing");
    for (int64_t i = 0; i < index; i++)
    {
      p_table_info = tablets.tablet_list.at(i);
      ret = root_table_for_build_->create_table(*p_table_info, t_server_index[i], replicas_num[i], mem_version);
    }
  }
  else
  {
    ObRootTable2* root_table_for_create = NULL;
    root_table_for_create = new(std::nothrow) ObRootTable2(NULL);
    if (root_table_for_create == NULL)
    {
      TBSYS_LOG(ERROR, "new ObRootTable2 error");
      ret = OB_ERROR;
    }
    if (NULL != root_table_for_create)
    {
      *root_table_for_create = *root_table_;
    }
    for (int64_t i = 0 ; i < index && OB_SUCCESS == ret; i++)
    {
      p_table_info = tablets.tablet_list.at(i);
      ret = root_table_for_create->create_table(*p_table_info, t_server_index[i], replicas_num[i], mem_version);
    }
    if (OB_SUCCESS == ret)
    {
      switch_root_table(root_table_for_create, NULL);
      root_table_for_create = NULL;
    }
    if (root_table_for_create != NULL)
    {
      delete root_table_for_create;
    }
  }
  return ret;
}

    int ObRootServer2::slave_create_new_table(const common::ObTabletInfo& tablet, const int32_t* t_server_index, const int32_t replicas_num, const int64_t mem_version)
    {
      int ret = OB_SUCCESS;
      if (NULL != root_table_for_build_)
      {
        tbsys::CThreadGuard guard_build(&root_table_build_mutex_);
        TBSYS_LOG(INFO, "replay log create_new_table and we are initializing");
        ret = root_table_for_build_->create_table(tablet, t_server_index, replicas_num, mem_version);
      }
      else
      {
        ObRootTable2* root_table_for_create = NULL;
        root_table_for_create = new(std::nothrow) ObRootTable2(NULL);
        if (root_table_for_create == NULL)
        {
          TBSYS_LOG(ERROR, "new ObRootTable2 error");
          ret = OB_ERROR;
        }
        if (NULL != root_table_for_create)
        {
          *root_table_for_create = *root_table_;
        }
        if (OB_SUCCESS == ret)
        {
          root_table_for_create->create_table(tablet, t_server_index, replicas_num, mem_version);
          switch_root_table(root_table_for_create, NULL);
          root_table_for_create = NULL;
        }
        if (root_table_for_create != NULL)
        {
          delete root_table_for_create;
        }
      }
      return ret;
    }

    // @pre root_table_build_mutex_ locked
    // @note Do not remove tablet entries in tablet_manager_ for now.
    int ObRootServer2::delete_tablets_from_root_table(const common::ObArray<uint64_t> &deleted_tables)
    {
      OB_ASSERT(0 < deleted_tables.count());
      int ret = OB_SUCCESS;
      common::ObArray<int32_t> index_map;
      common::ObArray<common::ObString> free_keys;
      ObRootTable2* rt1 = new(std::nothrow) ObRootTable2(NULL);
      if (NULL == rt1)
      {
        TBSYS_LOG(ERROR, "no memory");
        ret = OB_ALLOCATE_MEMORY_FAILED;
      }
      else
      {
        // copy
        {
          tbsys::CRLockGuard guard(root_table_rwlock_);
          *rt1 = *root_table_;
        }
        if (OB_SUCCESS != (ret = rt1->delete_tables(deleted_tables)))
        {
          TBSYS_LOG(INFO, "failed to delete tablets, err=%d", ret);
        }
        else
        {
          switch_root_table(rt1, NULL);
        }
      }
      return ret;
    }

      int ObRootServer2::create_new_table(common::ObSchemaManagerV2* schema, ObRootTable2* root_table)
    {
      int ret = OB_SUCCESS;

      ObTabletInfo tablet;
      bool version_request = false;
      tablet.range_.border_flag_.set_inclusive_end();
      tablet.range_.border_flag_.set_min_value();
      tablet.range_.border_flag_.set_max_value();
      ObServerStatus* server_status = NULL;
      ObServer server;
      int created_count = 0;
      int32_t server_index[OB_SAFE_COPY_COUNT];
      int32_t t_server_index[OB_SAFE_COPY_COUNT];
      ObRootTable2* root_table_for_create = NULL;
      if (NULL == root_table)
      {
        version_request = true;
        tbsys::CRLockGuard guard(root_table_rwlock_);
        if (root_table_ != NULL)
        {
          for (const ObTableSchema* it=schema->table_begin(); it != schema->table_end(); ++it)
          {
            if (it->get_table_id() != OB_INVALID_ID && !it->is_pure_update_table())
            {
              if(!root_table_->table_is_exist(it->get_table_id()))
              {
                // there is some new table
                root_table_for_create = new(std::nothrow) ObRootTable2(NULL);
                if (root_table_for_create == NULL)
                {
                  ret = OB_ALLOCATE_MEMORY_FAILED;
                  TBSYS_LOG(ERROR, "new ObRootTable2 error");
                }
                break;
              }
            }
          }
          if (NULL != root_table_for_create)
          {
            // copy the whole table
            *root_table_for_create = *root_table_;
          }
        }
      }
      else
      {
        TBSYS_LOG(INFO, "create tables when init");
        root_table_for_create = root_table;
      }
      if (root_table_for_create != NULL)
      {
        int64_t mem_frozen_version = 0;
        int retry_time = 0;
        while (OB_SUCCESS != (ret = worker_->get_rpc_stub().get_last_frozen_version(
                                get_update_server_info(false), config_.flag_network_timeout_us_.get(), mem_frozen_version)))
        {
          TBSYS_LOG(WARN, "failed to get last frozen version, err=%d ups=%s",
                    ret, get_update_server_info(false).to_cstring());
          retry_time++;
          if (retry_time >= 3) break;
          sleep(WAIT_SECONDS);
        }
        if (OB_SUCCESS == ret && 0 <= mem_frozen_version)
        {
          TBSYS_LOG(INFO, "frozen_version=%ld", mem_frozen_version);
          for (const ObTableSchema* it=schema->table_begin(); it != schema->table_end(); ++it)
          {
            if (it->get_table_id() != OB_INVALID_ID && !it->is_pure_update_table())
            {
              if(!root_table_for_create->table_is_exist(it->get_table_id()))
              {
                TBSYS_LOG(INFO, "table not exist, table_id=%lu", it->get_table_id());
                tablet.range_.table_id_ = it->get_table_id();
                for (int i = 0; i < OB_SAFE_COPY_COUNT; ++i)
                {
                  server_index[i] = OB_INVALID_INDEX;
                  t_server_index[i] = OB_INVALID_INDEX;
                }
                int32_t results_num = -1;
                get_available_servers_for_new_table(server_index, config_.flag_tablet_replicas_num_.get(), results_num);
                if (0 >= results_num)
                {
                  TBSYS_LOG(WARN, "no cs seleted");
                  ret = OB_NO_CS_SELECTED;
                  break;
                }
                else
                {
                  TBSYS_LOG(INFO, "cs selected for create new table, num=%d", results_num);
                  created_count = 0;
                  for (int i = 0; i < results_num; ++i)
                  {
                    if (server_index[i] != OB_INVALID_INDEX)
                    {
                      server_status = server_manager_.get_server_status(server_index[i]);
                      if (server_status != NULL)
                      {
                        server = server_status->server_;
                        server.set_port(server_status->port_cs_);
                        if (OB_SUCCESS == worker_->get_rpc_stub().create_tablet(server, tablet.range_, mem_frozen_version, config_.flag_network_timeout_us_.get()))
                        {
                          t_server_index[created_count] = server_index[i];
                          created_count++;
                          TBSYS_LOG(INFO, "create tablet replica, table_id=%lu server=%d version=%ld",
                                    tablet.range_.table_id_, server_index[i], mem_frozen_version);
                        }
                      }
                      else
                      {
                        server_index[i] = OB_INVALID_INDEX;
                      }
                    }
                  } // end for
                  if (created_count > 0)
                  {
                    ret = root_table_for_create->create_table(tablet, t_server_index, created_count, mem_frozen_version);
                    if (is_master())
                    {
                      int ret2 = log_worker_->add_new_tablet(created_count, tablet, t_server_index, mem_frozen_version);
                      if (OB_SUCCESS != ret2)
                      {
                        TBSYS_LOG(WARN, "failed to log add_new_tablet, err=%d", ret2);
                      }
                    }
                  }
                  else
                  {
                    TBSYS_LOG(WARN, "no tablet created");
                    ret = OB_NO_TABLETS_CREATED;
                    break;
                  }
                }
              }
              else
              {
                TBSYS_LOG(INFO, "table already exist, table_id=%lu", it->get_table_id());
              }
            }
          } // end for
        }
        else
        {
          TBSYS_LOG(ERROR, "get ups_get_last_frozen_memtable_version error, err=%d frozen_version=%ld", ret, mem_frozen_version);
          ret = OB_UPS_NOT_EXIST;
        }
        if (NULL == root_table)
        {
          switch_root_table(root_table_for_create, NULL);
          root_table_for_create = NULL;
        }
      }
      return ret;
    }
int ObRootServer2::get_master_ups(ObServer &ups_addr, bool use_inner_port)
{
  int ret = OB_ENTRY_NOT_EXIST;
  ObUps ups_master;
  if (NULL != ups_manager_)
  {
    if (OB_SUCCESS != (ret = ups_manager_->get_ups_master(ups_master)))
    {
      TBSYS_LOG(WARN, "not master ups exist");
    }
    else
    {
      ups_addr = ups_master.addr_;
      if (use_inner_port)
      {
        ups_addr.set_port(ups_master.inner_port_);
      }
    }
  }
  else
  {
    TBSYS_LOG(WARN, "ups_manager is NULL, check it.");
  }
  return ret;
}

common::ObServer ObRootServer2::get_update_server_info(bool use_inner_port)
{
  ObServer server;
  ObUps ups_master;
  if (NULL != ups_manager_)
  {
    ups_manager_->get_ups_master(ups_master);
    server = ups_master.addr_;
    if (use_inner_port)
    {
      server.set_port(ups_master.inner_port_);
    }
  }
  return server;
}


    int64_t ObRootServer2::get_merge_delay_interval() const
    {
      return config_.flag_cs_merge_delay_interval_us_.get() * server_manager_.get_array_length();
    }

    uint64_t ObRootServer2::get_table_info(const common::ObString& table_name, int32_t& max_row_key_length) const
    {
      uint64_t table_id = OB_INVALID_ID;
      max_row_key_length = 0;
      tbsys::CRLockGuard guard(schema_manager_rwlock_);
      if (schema_manager_ != NULL)
      {
        const ObTableSchema* table_schema = schema_manager_->get_table_schema(table_name);
        if (table_schema != NULL)
        {
          table_id = table_schema->get_table_id();
          max_row_key_length = table_schema->get_rowkey_max_length();
        }
      }
      return table_id;
    }

    int ObRootServer2::get_table_info(const uint64_t table_id, common::ObString& table_name, int32_t& max_row_key_length) const
    {
      int ret = OB_ERROR;
      max_row_key_length = 0;
      tbsys::CRLockGuard guard(schema_manager_rwlock_);
      if (schema_manager_ != NULL)
      {
        if (table_id > 0 && table_id != OB_INVALID_ID)
        {
          const ObTableSchema* table_schema = schema_manager_->get_table_schema(table_id);
          if (table_schema != NULL)
          {
            max_row_key_length = table_schema->get_rowkey_max_length();
            int table_name_len = static_cast<int32_t>(strlen(table_schema->get_table_name()));
            if (table_name_len == table_name.write(table_schema->get_table_name(), table_name_len))
            {
              ret = OB_SUCCESS;
            }
          }
        }
      }
      return ret;
    }

    int ObRootServer2::do_check_point(const uint64_t ckpt_id)
    {
      int ret = OB_SUCCESS;

      const char* log_dir = worker_->get_log_manager()->get_log_dir_path();
      char filename[OB_MAX_FILE_NAME_LENGTH];

      int err = 0;
      err = snprintf(filename, OB_MAX_FILE_NAME_LENGTH, "%s/%lu.%s", log_dir, ckpt_id, ROOT_TABLE_EXT);
      if (err < 0 || err >= OB_MAX_FILE_NAME_LENGTH)
      {
        TBSYS_LOG(ERROR, "generate root table file name [%s] failed, error: %s", filename, strerror(errno));
        ret = OB_ERROR;
      }

      if (ret == OB_SUCCESS)
      {
        ret = root_table_->write_to_file(filename);
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "write root table to file [%s] failed, err=%d", filename, ret);
        }
      }

      err = snprintf(filename, OB_MAX_FILE_NAME_LENGTH, "%s/%lu.%s", log_dir, ckpt_id, CHUNKSERVER_LIST_EXT);
      if (err < 0 || err >= OB_MAX_FILE_NAME_LENGTH)
      {
        TBSYS_LOG(ERROR, "generate chunk server list file name [%s] failed, error: %s", filename, strerror(errno));
        ret = OB_ERROR;
      }

      if (ret == OB_SUCCESS)
      {
        ret = server_manager_.write_to_file(filename);
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "write chunkserver list to file [%s] failed, err=%d", filename, ret);
        }
      }

      return ret;
    }

    void ObRootServer2::dump_schema_manager()
    {
      int ret = OB_SUCCESS;

      ObServer ups_server = get_update_server_info(false);

      if(OB_SUCCESS == ret)
      {
        ret = common::dump_schema_manager(*schema_manager_, &client_helper_, ups_server);
        if(OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "dump schema manager to ob table fail:ret[%d]", ret);
        }
      }
    }

    int ObRootServer2::recover_from_check_point(const int server_status, const uint64_t ckpt_id)
    {
      int ret = OB_SUCCESS;

      server_status_ = server_status;
      TBSYS_LOG(INFO, "server status recover from check point is %d", server_status_);

      const char* log_dir = worker_->get_log_manager()->get_log_dir_path();
      char filename[OB_MAX_FILE_NAME_LENGTH];

      int err = 0;
      err = snprintf(filename, OB_MAX_FILE_NAME_LENGTH, "%s/%lu.%s", log_dir, ckpt_id, ROOT_TABLE_EXT);
      if (err < 0 || err >= OB_MAX_FILE_NAME_LENGTH)
      {
        TBSYS_LOG(ERROR, "generate root table file name [%s] failed, error: %s", filename, strerror(errno));
        ret = OB_ERROR;
      }

      if (ret == OB_SUCCESS)
      {
        ret = root_table_->read_from_file(filename);
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "recover root table from file [%s] failed, err=%d", filename, ret);
        }
        else
        {
          TBSYS_LOG(INFO, "recover root table, size=%ld", root_table_->end() - root_table_->begin());
        }
      }

      err = snprintf(filename, OB_MAX_FILE_NAME_LENGTH, "%s/%lu.%s", log_dir, ckpt_id, CHUNKSERVER_LIST_EXT);
      if (err < 0 || err >= OB_MAX_FILE_NAME_LENGTH)
      {
        TBSYS_LOG(ERROR, "generate chunk server list file name [%s] failed, error: %s", filename, strerror(errno));
        ret = OB_ERROR;
      }

      if (ret == OB_SUCCESS)
      {
        int32_t cs_num = 0;
        int32_t ms_num = 0;

        ret = server_manager_.read_from_file(filename, cs_num, ms_num);
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "recover chunkserver list from file [%s] failed, err=%d", filename, ret);
        }
        else
        {
          TBSYS_LOG(INFO, "recover server list, cs_num=%d ms_num=%d", cs_num, ms_num);
        }
        if (0 < cs_num)
        {
          first_cs_had_registed_ = true;
        }
      }

      TBSYS_LOG(INFO, "recover finished with ret: %d, ckpt_id: %ld", ret, ckpt_id);

      return ret;
    }

int ObRootServer2::receive_new_frozen_version(const int64_t rt_version, const int64_t frozen_version,
    const int64_t last_frozen_time, bool did_replay)
{
  int ret = OB_SUCCESS;
  UNUSED(rt_version);
  if (config_.flag_is_import_.get())
  {
    if (OB_SUCCESS != (ret = try_create_new_table(frozen_version)))
    {
      TBSYS_LOG(WARN, "fail to create new table. ");
    }
  }
  if (OB_SUCCESS == ret)
  {
    if (OB_SUCCESS != (ret = report_frozen_memtable(frozen_version, last_frozen_time, did_replay)))
    {
      TBSYS_LOG(WARN, "fail to deal with forzen_memtable report. err=%d", ret);
      ret = OB_SUCCESS;
    }
  }
  else
  {
    TBSYS_LOG(ERROR, "fail to create empty tablet. err =%d", ret);
  }
  return ret;
}
    int ObRootServer2::report_frozen_memtable(const int64_t frozen_version, const int64_t last_frozen_time, bool did_replay)
    {
      int ret = OB_SUCCESS;

      tbsys::CThreadGuard mutex_guard(&frozen_version_mutex_);
      if ( frozen_version < 0 || frozen_version <= last_frozen_mem_version_)
      {
        TBSYS_LOG(WARN, "invalid froze_version, version=%ld last_frozen_version=%ld",
            frozen_version, last_frozen_mem_version_);
        ret = OB_ERROR;
      }
      else
      {
        if (!did_replay && is_master() && (!all_tablet_is_the_last_frozen_version()) )
        {
          TBSYS_LOG(WARN,"merge is too slow, last_version=%ld curr_version=%ld",
              last_frozen_mem_version_, frozen_version); //just warn
        }
      }

      if (OB_SUCCESS == ret)
      {
        last_frozen_mem_version_ = frozen_version;
        last_frozen_time_ = did_replay ? last_frozen_time : tbsys::CTimeUtil::getMonotonicTime();
        TBSYS_LOG(INFO, "frozen_version=%ld last_frozen_time=%ld did_replay=%d",
            last_frozen_mem_version_, last_frozen_time_, did_replay);
      }

      if (OB_SUCCESS == ret)
      {
        if (is_master() && !did_replay)
        {
          log_worker_->sync_us_frozen_version(last_frozen_mem_version_, last_frozen_time_);
        }
      }
      if (OB_SUCCESS != ret && did_replay && last_frozen_mem_version_ >= frozen_version)
      {
        TBSYS_LOG(ERROR, "RS meet a unlegal forzen_version[%ld] while replay. last_frozen_verson=%ld, ignore the err",
            frozen_version, last_frozen_mem_version_);
        ret = OB_SUCCESS;
      }
      return ret;
    }

    int ObRootServer2::get_server_status() const
    {
      return server_status_;
    }
    int64_t ObRootServer2::get_time_stamp_changing() const
    {
      return time_stamp_changing_;
    }
    int64_t ObRootServer2::get_lease() const
    {
      return config_.flag_cs_lease_duration_us_.get();
    }

    void ObRootServer2::wait_init_finished()
    {
      static const unsigned long sleep_us = 200*1000;
      while(true)
      {
        {
          tbsys::CThreadGuard guard(&(status_mutex_));
          if (STATUS_INIT != server_status_)
          {
            break;
          }
        }
        usleep(sleep_us);           // sleep 200ms
      }
      TBSYS_LOG(INFO, "rootserver2 init finished");
    }

    const ObiRole& ObRootServer2::get_obi_role() const
    {
      return obi_role_;
    }

    int ObRootServer2::set_obi_role(const ObiRole& role)
    {
      int ret = OB_SUCCESS;
      if (NULL == ups_manager_)
      {
        TBSYS_LOG(ERROR, "not init");
        ret = OB_NOT_INIT;
      }
      else if (ObiRole::INIT == role.get_role())
      {
        TBSYS_LOG(WARN, "role cannot be INIT");
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        obi_role_.set_role(role.get_role());
        TBSYS_LOG(INFO, "set obi role, role=%s", role.get_role_str());
        if (ObRoleMgr::MASTER == worker_->get_role_manager()->get_role())
        {
          if (OB_SUCCESS != (ret = worker_->send_obi_role(obi_role_)))
          {
            TBSYS_LOG(WARN, "fail to set slave_rs's obi role err=%d", ret);
          }
          else if (OB_SUCCESS != (ret = ups_manager_->send_obi_role()))
          {
            TBSYS_LOG(WARN, "failed to set updateservers' obi role, err=%d", ret);
          }
        }
      }
      return ret;
    }

int ObRootServer2::get_obi_config(common::ObiConfig& obi_config) const
{
  int ret = OB_ENTRY_NOT_EXIST;
  for (int i = 0; i < config_.client_config_.obi_list_.obi_count_; ++i)
  {
    if (config_.client_config_.obi_list_.conf_array_[i].get_rs_addr() == my_addr_)
    {
      obi_config = config_.client_config_.obi_list_.conf_array_[i];
      ret = OB_SUCCESS;
      break;
    }
  }
  return ret;
}
int ObRootServer2::get_master_ups_config(int32_t &master_master_ups_read_percent, int32_t &slave_master_ups_read_percent) const
{
  int ret = OB_SUCCESS;
  if (ups_manager_ != NULL)
  {
    ups_manager_->get_master_ups_config(master_master_ups_read_percent, slave_master_ups_read_percent);
  }
  return ret;
}

int ObRootServer2::set_obi_config(const common::ObiConfig& conf)
{
  return set_obi_config(my_addr_, conf);
}

int ObRootServer2::set_obi_config(const common::ObServer &rs_addr, const common::ObiConfig& conf)
{
  int ret = OB_SUCCESS;
  if (0 > conf.get_read_percentage() || 100 < conf.get_read_percentage())
  {
    TBSYS_LOG(WARN, "invalid param, read_percentage=%d", conf.get_read_percentage());
    ret = OB_INVALID_ARGUMENT;
  }
  else
  {
    ret = OB_ENTRY_NOT_EXIST;
    for (int i = 0; i < config_.client_config_.obi_list_.obi_count_; ++i)
    {
      if (config_.client_config_.obi_list_.conf_array_[i].get_rs_addr() == rs_addr)
      {
        config_.client_config_.obi_list_.conf_array_[i].set_read_percentage(conf.get_read_percentage());
        config_.client_config_.obi_list_.print();
        if (is_master())
        {
          if (OB_SUCCESS != (ret = log_worker_->set_client_config(config_.client_config_)))
          {
            TBSYS_LOG(ERROR, "write log error, err=%d", ret);
          }
        }
        ret = OB_SUCCESS;
        break;
      }
    }
  }
  return ret;
}

void ObRootServer2::cancel_restart_all_cs()
{
  tbsys::CWLockGuard guard(server_manager_rwlock_);
  server_manager_.cancel_restart_all_cs();
}

void ObRootServer2::restart_all_cs()
{
  tbsys::CWLockGuard guard(server_manager_rwlock_);
  server_manager_.restart_all_cs();
  if(NULL != balancer_thread_)
  {
    balancer_thread_->wakeup();
  }
  else
  {
    TBSYS_LOG(WARN, "balancer_thread_ is null");
  }
}

int ObRootServer2::shutdown_cs(const common::ObArray<common::ObServer> &servers, enum ShutdownOperation op)
{
  int ret = OB_SUCCESS;
  tbsys::CWLockGuard guard(server_manager_rwlock_);
  ret = server_manager_.shutdown_cs(servers, op);
  if(RESTART == op)
  {
    if(NULL != balancer_thread_)
    {
      balancer_thread_->wakeup();
    }
    else
    {
      TBSYS_LOG(WARN, "balancer_thread_ is null");
    }
  }
  return ret;
}

int ObRootServer2::cancel_shutdown_cs(const common::ObArray<common::ObServer> &servers, enum ShutdownOperation op)
{
  int ret = OB_SUCCESS;
  tbsys::CWLockGuard guard(server_manager_rwlock_);
  ret = server_manager_.cancel_shutdown_cs(servers, op);
  return ret;
}

void ObRootServer2::do_stat_common(char *buf, const int64_t buf_len, int64_t& pos)
{
  do_stat_start_time(buf, buf_len, pos);
  do_stat_local_time(buf, buf_len, pos);
  databuff_printf(buf, buf_len, pos, "prog_version: %s(%s)\n", PACKAGE_STRING, RELEASEID);
  databuff_printf(buf, buf_len, pos, "pid: %d\n", getpid());
  databuff_printf(buf, buf_len, pos, "obi_role: %s\n", obi_role_.get_role_str());
}

void ObRootServer2::do_stat_start_time(char *buf, const int64_t buf_len, int64_t& pos)
{
  databuff_printf(buf, buf_len, pos, "start_time: %s", ctime(&start_time_));
}

void ObRootServer2::do_stat_local_time(char *buf, const int64_t buf_len, int64_t& pos)
{
  time_t now = time(NULL);
  databuff_printf(buf, buf_len, pos, "local_time: %s", ctime(&now));
}

void ObRootServer2::do_stat_schema_version(char* buf, const int64_t buf_len, int64_t &pos)
{
  tbsys::CRLockGuard guard(schema_manager_rwlock_);
  int64_t schema_version = schema_manager_->get_version();
  tbutil::Time schema_time = tbutil::Time::microSeconds(schema_version);
  struct timeval schema_tv(schema_time);
  struct tm stm;
  localtime_r(&schema_tv.tv_sec, &stm);
  char time_buf[32];
  strftime(time_buf, sizeof(time_buf), "%F %H:%M:%S", &stm);
  databuff_printf(buf, buf_len, pos, "schema_version: %lu(%s)", schema_version, time_buf);
}

void ObRootServer2::do_stat_frozen_time(char* buf, const int64_t buf_len, int64_t &pos)
{
  tbutil::Time frozen_time = tbutil::Time::microSeconds(last_frozen_time_);
  struct timeval frozen_tv(frozen_time);
  struct tm stm;
  localtime_r(&frozen_tv.tv_sec, &stm);
  char time_buf[32];
  strftime(time_buf, sizeof(time_buf), "%F %H:%M:%S", &stm);
  databuff_printf(buf, buf_len, pos, "frozen_time: %lu(%s)", last_frozen_time_, time_buf);
}

void ObRootServer2::do_stat_mem(char* buf, const int64_t buf_len, int64_t &pos)
{
  struct mallinfo minfo = mallinfo();
  databuff_printf(buf, buf_len, pos, "mem: arena=%d ordblks=%d hblkhd=%d uordblks=%d fordblks=%d keepcost=%d",
                 minfo.arena, minfo.ordblks, minfo.hblkhd, minfo.uordblks, minfo.fordblks, minfo.keepcost);
}

void ObRootServer2::do_stat_table_num(char* buf, const int64_t buf_len, int64_t &pos)
{
  int64_t num = -1;
  tbsys::CRLockGuard guard(schema_manager_rwlock_);
  if (NULL != schema_manager_)
  {
    num = schema_manager_->table_end() - schema_manager_->table_begin();
  }
  databuff_printf(buf, buf_len, pos, "table_num: %ld", num);
}

void ObRootServer2::do_stat_tablet_num(char* buf, const int64_t buf_len, int64_t &pos)
{
  int64_t num = -1;
  tbsys::CRLockGuard guard(root_table_rwlock_);
  if (NULL != tablet_manager_)
  {
    num = tablet_manager_->end() - tablet_manager_->begin();
  }
  databuff_printf(buf, buf_len, pos, "tablet_num: %ld", num);
}

int ObRootServer2::table_exist_in_cs(const uint64_t table_id, bool &is_exist)
{
  int ret = OB_SUCCESS;
  is_exist = false;
  ObChunkServerManager::iterator it = server_manager_.begin();
  for (; it != server_manager_.end(); ++it)
  {
    char server_str[OB_IP_STR_BUFF];
    it->server_.to_string(server_str, OB_IP_STR_BUFF);
    if (it->port_cs_ != 0
        && it->status_ != ObServerStatus::STATUS_DEAD)
    {
      if (OB_SUCCESS != (ret = worker_->get_rpc_stub().table_exist_in_cs(it->server_, config_.flag_network_timeout_us_.get(),
              table_id, is_exist)))
      {
        TBSYS_LOG(WARN, "fail to check table info in cs[%s], ret=%d", server_str, ret);
      }
      else
      {
        if (true == is_exist)
        {
          TBSYS_LOG(INFO, "table already exist in chunkserver. table_id=%lu, cs_addr=%s", table_id, server_str);
          break;
        }
      }
    }
  }
  return ret;
}
void ObRootServer2::do_stat_cs(char* buf, const int64_t buf_len, int64_t &pos)
{
  char server_str[OB_IP_STR_BUFF];
  ObServer tmp_server;
  databuff_printf(buf, buf_len, pos, "chunkservers: ");
  ObChunkServerManager::iterator it = server_manager_.begin();
  for (; it != server_manager_.end(); ++it)
  {
    if (it->port_cs_ != 0
        && it->status_ != ObServerStatus::STATUS_DEAD)
    {
      tmp_server = it->server_;
      tmp_server.set_port(it->port_cs_);
      tmp_server.to_string(server_str, OB_IP_STR_BUFF);
      databuff_printf(buf, buf_len, pos, "%s ", server_str);
    }
  }
}

void ObRootServer2::do_stat_cs_num(char* buf, const int64_t buf_len, int64_t &pos)
{
  int num = 0;
  ObChunkServerManager::iterator it = server_manager_.begin();
  for (; it != server_manager_.end(); ++it)
  {
    if (it->port_cs_ != 0
        && it->status_ != ObServerStatus::STATUS_DEAD)
    {
      num++;
    }
  }
  databuff_printf(buf, buf_len, pos, "cs_num: %d", num);
}

void ObRootServer2::do_stat_ms(char* buf, const int64_t buf_len, int64_t &pos)
{
  char server_str[OB_IP_STR_BUFF];
  ObServer tmp_server;
  databuff_printf(buf, buf_len, pos, "mergeservers: ");
  ObChunkServerManager::iterator it = server_manager_.begin();
  for (; it != server_manager_.end(); ++it)
  {
    if (it->port_ms_ != 0
        && it->ms_status_ != ObServerStatus::STATUS_DEAD)
    {
      tmp_server = it->server_;
      tmp_server.set_port(it->port_ms_);
      tmp_server.to_string(server_str, OB_IP_STR_BUFF);
      databuff_printf(buf, buf_len, pos, "%s ", server_str);
    }
  }
}

void ObRootServer2::do_stat_ms_num(char* buf, const int64_t buf_len, int64_t &pos)
{
  int num = 0;
  ObChunkServerManager::iterator it = server_manager_.begin();
  for (; it != server_manager_.end(); ++it)
  {
    if (it->port_ms_ != 0
        && it->status_ != ObServerStatus::STATUS_DEAD)
    {
      num++;
    }
  }
  databuff_printf(buf, buf_len, pos, "cs_num: %d", num);
}
void ObRootServer2::do_stat_all_server(char* buf, const int64_t buf_len, int64_t &pos)
{
  if (NULL != ups_manager_)
  {
    ups_manager_->print(buf, buf_len, pos);
  }
  databuff_printf(buf, buf_len, pos, "\n");
  do_stat_cs(buf, buf_len, pos);
  databuff_printf(buf, buf_len, pos, "\n");
  do_stat_ms(buf, buf_len, pos);
}

void ObRootServer2::do_stat_ups(char* buf, const int64_t buf_len, int64_t &pos)
{
  if (NULL != ups_manager_)
  {
    ups_manager_->print(buf, buf_len, pos);
  }
}

void ObRootServer2::do_stat_client_config(char* buf, const int64_t buf_len, int64_t &pos)
{
  databuff_printf(buf, buf_len, pos, "client_config:\n");
  config_.client_config_.print(buf, buf_len, pos);
}

int64_t ObRootServer2::get_stat_value(const int32_t index)
{
  int64_t ret = 0;
  ObStat *stat = NULL;
  if (OB_SUCCESS == worker_->get_stat_manager().get_stat(ObRootStatManager::ROOT_TABLE_ID, stat)
      && NULL != stat)
  {
    ret = stat->get_value(index);
  }
  return ret;
}

void ObRootServer2::do_stat_merge(char* buf, const int64_t buf_len, int64_t &pos)
{
  if (last_frozen_time_ == 0)
  {
    databuff_printf(buf, buf_len, pos, "merge: DONE");
  }
  else
  {
    if (all_tablet_is_the_last_frozen_version_really())
    {
      databuff_printf(buf, buf_len, pos, "merge: DONE");
    }
    else
    {
      int64_t now = tbsys::CTimeUtil::getMonotonicTime();
      int64_t max_merge_duration_us = config_.flag_max_merge_duration_seconds_.get()*1000*1000;
      if (now > last_frozen_time_ + max_merge_duration_us)
      {
        databuff_printf(buf, buf_len, pos, "merge: TIMEOUT");
      }
      else
      {
        databuff_printf(buf, buf_len, pos, "merge: DOING");
      }
    }
  }
}

void ObRootServer2::do_stat_unusual_tablets_num(char* buf, const int64_t buf_len, int64_t &pos)
{
  int32_t num = 0;
  tbsys::CRLockGuard guard(root_table_rwlock_);
  if (root_table_ != NULL)
  {
    root_table_->dump_unusual_tablets(last_frozen_mem_version_, config_.flag_tablet_replicas_num_.get(), num);
  }
  databuff_printf(buf, buf_len, pos, "unusual_tablets_num: %d", num);
}

const char* ObRootServer2::str_rs_status(int status) const
{
  const char* ret = "unknown";
  switch(status)
  {
    case STATUS_INIT:
      ret = "INIT";
      break;
    case STATUS_CHANGING:
      ret = "REPORTING";
      break;
    case STATUS_SLEEP:
      ret = "INITED";
      break;
    default:
      break;
  }
  return ret;
}

int ObRootServer2::do_stat(int stat_key, char *buf, const int64_t buf_len, int64_t& pos)
{
  int ret = OB_SUCCESS;
  switch(stat_key)
  {
    case OB_RS_STAT_COMMON:
      do_stat_common(buf, buf_len, pos);
      break;
    case OB_RS_STAT_START_TIME:
      do_stat_start_time(buf, buf_len, pos);
      break;
    case OB_RS_STAT_LOCAL_TIME:
      do_stat_local_time(buf, buf_len, pos);
      break;
    case OB_RS_STAT_PROGRAM_VERSION:
      databuff_printf(buf, buf_len, pos, "prog_version: %s(%s)", PACKAGE_STRING, RELEASEID);
      break;
    case OB_RS_STAT_PID:
      databuff_printf(buf, buf_len, pos, "pid: %d", getpid());
      break;
    case OB_RS_STAT_MEM:
      do_stat_mem(buf, buf_len, pos);
      break;
    case OB_RS_STAT_RS_STATUS:
      databuff_printf(buf, buf_len, pos, "rs_status: %s", str_rs_status(server_status_));
      break;
    case OB_RS_STAT_FROZEN_VERSION:
      databuff_printf(buf, buf_len, pos, "frozen_version: %ld", last_frozen_mem_version_);
      break;
    case OB_RS_STAT_SCHEMA_VERSION:
      do_stat_schema_version(buf, buf_len, pos);
      break;
    case OB_RS_STAT_LOG_SEQUENCE:
      databuff_printf(buf, buf_len, pos, "log_seq: %lu", log_worker_->get_cur_log_seq());
      break;
    case OB_RS_STAT_LOG_FILE_ID:
      databuff_printf(buf, buf_len, pos, "log_file_id: %lu", log_worker_->get_cur_log_file_id());
      break;
    case OB_RS_STAT_TABLE_NUM:
      do_stat_table_num(buf, buf_len, pos);
      break;
    case OB_RS_STAT_TABLET_NUM:
      do_stat_tablet_num(buf, buf_len, pos);
      break;
    case OB_RS_STAT_CS:
      do_stat_cs(buf, buf_len, pos);
      break;
    case OB_RS_STAT_MS:
      do_stat_ms(buf, buf_len, pos);
      break;
    case OB_RS_STAT_UPS:
      do_stat_ups(buf, buf_len, pos);
      break;
    case OB_RS_STAT_ALL_SERVER:
      do_stat_all_server(buf, buf_len, pos);
      break;
    case OB_RS_STAT_FROZEN_TIME:
      do_stat_frozen_time(buf, buf_len, pos);
      break;
    case OB_RS_STAT_CLIENT_CONF:
      do_stat_client_config(buf, buf_len, pos);
      break;
    case OB_RS_STAT_SSTABLE_DIST:
      balancer_->nb_print_balance_infos(buf, buf_len, pos);
      break;
    case OB_RS_STAT_OPS_GET:
      databuff_printf(buf, buf_len, pos, "ops_get: %ld", get_stat_value(ObRootStatManager::INDEX_SUCCESS_GET_COUNT));
      break;
    case OB_RS_STAT_OPS_SCAN:
      databuff_printf(buf, buf_len, pos, "ops_scan: %ld", get_stat_value(ObRootStatManager::INDEX_SUCCESS_SCAN_COUNT));
      break;
    case OB_RS_STAT_FAIL_GET_COUNT:
      databuff_printf(buf, buf_len, pos, "fail_get_count: %ld", get_stat_value(ObRootStatManager::INDEX_FAIL_GET_COUNT));
      break;
    case OB_RS_STAT_FAIL_SCAN_COUNT:
      databuff_printf(buf, buf_len, pos, "fail_scan_count: %ld", get_stat_value(ObRootStatManager::INDEX_FAIL_SCAN_COUNT));
      break;
    case OB_RS_STAT_GET_OBI_ROLE_COUNT:
      databuff_printf(buf, buf_len, pos, "get_obi_role_count: %ld", get_stat_value(ObRootStatManager::INDEX_GET_OBI_ROLE_COUNT));
      break;
    case OB_RS_STAT_MIGRATE_COUNT:
      databuff_printf(buf, buf_len, pos, "migrate_count: %ld", get_stat_value(ObRootStatManager::INDEX_MIGRATE_COUNT));
      break;
    case OB_RS_STAT_COPY_COUNT:
      databuff_printf(buf, buf_len, pos, "copy_count: %ld", get_stat_value(ObRootStatManager::INDEX_COPY_COUNT));
      break;
    case OB_RS_STAT_CS_NUM:
      do_stat_cs_num(buf, buf_len, pos);
      break;
    case OB_RS_STAT_MS_NUM:
      do_stat_ms_num(buf, buf_len, pos);
      break;
    case OB_RS_STAT_MERGE:
      do_stat_merge(buf, buf_len, pos);
      break;
    case OB_RS_STAT_UNUSUAL_TABLETS_NUM:
      do_stat_unusual_tablets_num(buf, buf_len, pos);
      break;
    case OB_RS_STAT_SHUTDOWN_CS:
      balancer_->nb_print_shutting_down_progress(buf, buf_len, pos);
      break;
    case OB_RS_STAT_REPLICAS_NUM:
    default:
      databuff_printf(buf, buf_len, pos, "unknown or not implemented yet, stat_key=%d", stat_key);
      break;
  }
  return ret;
}

int ObRootServer2::make_checkpointing()
{
  tbsys::CRLockGuard rt_guard(root_table_rwlock_);
  tbsys::CRLockGuard cs_guard(server_manager_rwlock_);
  tbsys::CThreadGuard st_guard(&status_mutex_);
  tbsys::CThreadGuard log_guard(worker_->get_log_manager()->get_log_sync_mutex());
  int ret = worker_->get_log_manager()->do_check_point();
  if (ret != OB_SUCCESS)
  {
    TBSYS_LOG(ERROR, "failed to make checkpointing, err=%d", ret);
  }
  else
  {
    TBSYS_LOG(INFO, "made checkpointing");
  }
  return ret;
}

int ObRootServer2::register_ups(const common::ObServer &addr, int32_t inner_port, int64_t log_seq_num, int64_t lease)
{
  int ret = OB_SUCCESS;
  if (NULL == ups_manager_)
  {
    TBSYS_LOG(ERROR, "ups_manager is NULL");
    ret = OB_NOT_INIT;
  }
  else
  {
    ret = ups_manager_->register_ups(addr, inner_port, log_seq_num, lease);
  }
  return ret;
}

int ObRootServer2::receive_ups_heartbeat_resp(const common::ObServer &addr, ObUpsStatus stat,
                                              const common::ObiRole &obi_role)
{
  int ret = OB_SUCCESS;
  if (NULL == ups_manager_)
  {
    TBSYS_LOG(ERROR, "ups_manager is NULL");
    ret = OB_NOT_INIT;
  }
  else
  {
    ret = ups_manager_->renew_lease(addr, stat, obi_role);
  }
  return ret;
}

int ObRootServer2::ups_slave_failure(const common::ObServer &addr, const common::ObServer &slave_addr)
{
  int ret = OB_SUCCESS;
  if (NULL == ups_manager_)
  {
    TBSYS_LOG(ERROR, "ups_manager is NULL");
    ret = OB_NOT_INIT;
  }
  else
  {
    ret = ups_manager_->slave_failure(addr, slave_addr);
  }
  return ret;
}

const common::ObClientConfig& ObRootServer2::get_client_config() const
{
  return config_.client_config_;
}
int ObRootServer2::set_ups_config(int32_t read_master_master_ups_percentage, int32_t read_slave_master_ups_percentage)
{
  int ret = OB_SUCCESS;
  if (NULL == ups_manager_)
  {
    TBSYS_LOG(ERROR, "ups_manager is NULL");
    ret = OB_NOT_INIT;
  }
  else
  {
    ret = ups_manager_->set_ups_config(read_master_master_ups_percentage, read_slave_master_ups_percentage);
  }
  return ret;
}
int ObRootServer2::set_ups_config(const common::ObServer &ups, int32_t ms_read_percentage, int32_t cs_read_percentage)
{
  int ret = OB_SUCCESS;
  if (NULL == ups_manager_)
  {
    TBSYS_LOG(ERROR, "ups_manager is NULL");
    ret = OB_NOT_INIT;
  }
  else
  {
    ret = ups_manager_->set_ups_config(ups, ms_read_percentage, cs_read_percentage);
  }
  return ret;
}

int ObRootServer2::change_ups_master(const common::ObServer &ups, bool did_force)
{
  int ret = OB_SUCCESS;
  if (NULL == ups_manager_)
  {
    TBSYS_LOG(ERROR, "ups_manager is NULL");
    ret = OB_NOT_INIT;
  }
  else
  {
    ret = ups_manager_->set_ups_master(ups, did_force);
  }
  return ret;
}

int ObRootServer2::get_ups_list(common::ObUpsList &ups_list)
{
  int ret = OB_SUCCESS;
  if (NULL == ups_manager_)
  {
    TBSYS_LOG(ERROR, "ups_manager is NULL");
    ret = OB_NOT_INIT;
  }
  else
  {
    ret = ups_manager_->get_ups_list(ups_list);
  }
  return ret;
}

int ObRootServer2::set_client_config(const common::ObClientConfig &client_conf)
{
  int ret = OB_SUCCESS;
  config_.client_config_ = client_conf;
  config_.client_config_.print();
  return ret;
}

int ObRootServer2::serialize_cs_list(char* buf, const int64_t buf_len, int64_t& pos) const
{
  return server_manager_.serialize_cs_list(buf, buf_len, pos);
}

int ObRootServer2::serialize_ms_list(char* buf, const int64_t buf_len, int64_t& pos) const
{
  return server_manager_.serialize_ms_list(buf, buf_len, pos);
}

int ObRootServer2::serialize_proxy_list(char* buf, const int64_t buf_len, int64_t& pos) const
{
  int ret = OB_SUCCESS;

  const ObServerStatus* it = NULL;
  int ms_num = 0;
  for (it = server_manager_.begin(); it != server_manager_.end(); ++it)
  {
    if (ObServerStatus::STATUS_DEAD != it->ms_status_)
    {
      ms_num++;
    }
  }
  ret = serialization::encode_vi32(buf, buf_len, pos, ms_num);
  if (OB_SUCCESS != ret)
  {
    TBSYS_LOG(WARN, "serialize error");
  }
  else
  {
    int i = 0;
    for (it = server_manager_.begin();
        it != server_manager_.end() && i < ms_num; ++it)
    {
      if (ObServerStatus::STATUS_DEAD != it->ms_status_)
      {
        ObServer addr = it->server_;
        addr.set_port(config_.flag_obconnector_port_.get());
        ret = addr.serialize(buf, buf_len, pos);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "ObServer serialize error, ret=%d", ret);
          break;
        }
        else
        {
          int64_t reserved = 0;
          ret = serialization::encode_vi64(buf, buf_len, pos, reserved);
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "encode_vi64 error, ret=%d", ret);
            break;
          }
          else
          {
            ++i;
          }
        }
      }
    }
  }
  return ret;
}

int ObRootServer2::grant_eternal_ups_lease()
{
  int ret = OB_SUCCESS;
  if (NULL == ups_heartbeat_thread_
      || NULL == ups_check_thread_)
  {
    TBSYS_LOG(ERROR, "ups_heartbeat_thread is NULL");
    ret = OB_NOT_INIT;
  }
  else
  {
    ups_check_thread_->stop();
    ups_heartbeat_thread_->stop();
  }
  return ret;
}

int ObRootServer2::cs_import_tablets(const uint64_t table_id, const int64_t tablet_version)
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_ID == table_id)
  {
    TBSYS_LOG(WARN, "invalid table id");
    ret = OB_INVALID_ARGUMENT;
  }
  else if (0 > tablet_version)
  {
    TBSYS_LOG(WARN, "invalid table version, version=%ld", tablet_version);
    ret = OB_INVALID_ARGUMENT;
  }
  else
  {
    int64_t version = tablet_version;
    if (0 == version)
    {
      version = last_frozen_mem_version_;
    }
    int32_t sent_num = 0;
    ObServer cs;
    ObChunkServerManager::iterator it = server_manager_.begin();
    for (; it != server_manager_.end(); ++it)
    {
      if (it->status_ != ObServerStatus::STATUS_DEAD)
      {
        cs = it->server_;
        cs.set_port(it->port_cs_);
        // CS收到消息后立即返回成功，然后再完成实际的导入
        if (OB_SUCCESS != (ret = worker_->get_rpc_stub().import_tablets(cs, table_id, version, config_.flag_network_timeout_us_.get())))
        {
          TBSYS_LOG(WARN, "failed to send msg to cs, err=%d", ret);
          break;
        }
        sent_num++;
      }
    }
    if (OB_SUCCESS == ret && 0 == sent_num)
    {
      TBSYS_LOG(WARN, "no chunkserver");
      ret = OB_DATA_NOT_SERVE;
    }
    if (OB_SUCCESS == ret)
    {
      TBSYS_LOG(INFO, "send msg to import tablets, table_id=%lu version=%ld cs_num=%d",
                table_id, version, sent_num);
    }
    else
    {
      TBSYS_LOG(WARN, "failed to import tablets, err=%d table_id=%lu version=%ld cs_num=%d",
                ret, table_id, version, sent_num);
    }
  }
  return ret;
}

////////////////////////////////////////////////////////////////
    ObRootServer2::rootTableModifier::rootTableModifier(ObRootServer2* root_server):root_server_(root_server)
    {
    }

    void ObRootServer2::rootTableModifier::run(tbsys::CThread *thread, void *arg)
    {
      UNUSED(thread);
      UNUSED(arg);
      TBSYS_LOG(INFO, "[NOTICE] root table modifier thread start");
      bool init_process = false;
      {
        tbsys::CThreadGuard guard(&(root_server_->status_mutex_));
        if (root_server_->server_status_ == STATUS_INIT)
        {
          init_process  = true;  //execute init process only the first time the system start up;
        }
      }

      if (init_process)
      {
        TBSYS_LOG(INFO, "init process");
        if ((OB_SUCCESS != root_server_->init_root_table_by_report()) ||
            (root_server_->build_sync_flag_ != BUILD_SYNC_INIT_OK))
        {
          TBSYS_LOG(ERROR, "system init error");
          exit(0);
        }
      }
      else
      {
        root_server_->build_sync_flag_ = BUILD_SYNC_INIT_OK;
        TBSYS_LOG(INFO, "don't need init process, server_status_=%d", root_server_->server_status_);
      }
      TBSYS_LOG(INFO, "[NOTICE] start service now");
    }

    ObRootServer2::MergeChecker::MergeChecker(ObRootServer2* root_server) : root_server_(root_server)
    {
    }

    void ObRootServer2::MergeChecker::run(tbsys::CThread *thread, void *arg)
    {
      UNUSED(thread);
      UNUSED(arg);
      TBSYS_LOG(INFO, "[NOTICE] merge checker thread start");
      bool did_get_last_frozen_version = true;
      while (!_stop)
      {
        if (BUILD_SYNC_INIT_OK == root_server_->build_sync_flag_)
        {
          int64_t rt_version = 0;
          if (did_get_last_frozen_version)
          {
            //get last frozen mem table version from updateserver
            int64_t frozen_version = -1;
            int err = OB_SUCCESS;
            root_server_->get_max_tablet_version(rt_version);
            ObServer ups = root_server_->get_update_server_info(false);
            if (OB_SUCCESS != (err = root_server_->worker_->get_rpc_stub().get_last_frozen_version(
                    ups, root_server_->config_.flag_network_timeout_us_.get(), frozen_version)))
            {
              TBSYS_LOG(WARN, "failed to get frozen version, err=%d ups=%s", err, ups.to_cstring());
            }
            else if (0 >= frozen_version)
            {
              TBSYS_LOG(WARN, "invalid frozen version=%ld ups=%s", frozen_version, ups.to_cstring());
            }
            else
            {
              TBSYS_LOG(INFO, "got last frozen version, ups=%s version=%ld", ups.to_cstring(), frozen_version);
              if (OB_SUCCESS != (err = root_server_->receive_new_frozen_version(rt_version, frozen_version, 0, false)))
              {
                TBSYS_LOG(WARN, "fail to deal with new frozen_version");
              }
              else
              {
                did_get_last_frozen_version = false;
              }
            }
          }
          if (root_server_->is_master() && root_server_->is_daily_merge_tablet_error())
          {
            TBSYS_LOG(ERROR, "merge process alreay have some error, check it");
          }
          int64_t now = tbsys::CTimeUtil::getMonotonicTime();
          int64_t max_merge_duration_us = root_server_->config_.flag_max_merge_duration_seconds_.get()*1000*1000;
          if (root_server_->is_master() && (root_server_->last_frozen_time_ > 0) )
          {
            if (((max_merge_duration_us + root_server_->last_frozen_time_) < now) &&
                !root_server_->all_tablet_is_the_last_frozen_version() )
            {
              TBSYS_LOG(ERROR,"merge is too slow,start at:%ld,now:%ld,max_merge_duration_:%ld",root_server_->last_frozen_time_,
                  now,max_merge_duration_us);
            }
            else if (root_server_->all_tablet_is_the_last_frozen_version())
            {
              TBSYS_LOG(INFO,"build new root table ok"); //for qa
              root_server_->last_frozen_time_ = 0;
              // checkpointing after done merge
              root_server_->make_checkpointing();
            }
          }
        }
        sleep(1);
      }
      TBSYS_LOG(INFO, "[NOTICE] merge checker thread exit");
    }

    ObRootServer2::heartbeatChecker::heartbeatChecker(ObRootServer2* root_server)
      :root_server_(root_server)
    {
    }

    bool ObRootServer2::is_master() const
    {
      return worker_->get_role_manager()->is_master();
    }

    int ObRootServer2::receive_hb(const common::ObServer& server,ObRole role)
    {
      int64_t  now = tbsys::CTimeUtil::getMonotonicTime();
      int err = OB_SUCCESS;
      err = server_manager_.receive_hb(server,now,role == OB_MERGESERVER ? true : false);
      if (2 == err && (role != OB_MERGESERVER))
      {
        TBSYS_LOG(INFO, "receive relive cs's heartbeat. need add commit log andforce to report tablet");
        if (OB_SUCCESS != (err = log_worker_->regist_cs(server, now)))
        {
          TBSYS_LOG(WARN, "fail to write log for regist_cs. err=%d", err);
        }
        else
        {
          err = worker_->get_rpc_stub().request_report_tablet(server);
          if (OB_SUCCESS != err)
          {
            TBSYS_LOG(ERROR, "fail to force cs to report tablet info. server=%s, err=%d",
                server.to_cstring(), err);
          }
        }
      }
      return err;
    }

    void ObRootServer2::heartbeatChecker::run(tbsys::CThread *thread, void *arg)
    {
      UNUSED(thread);
      UNUSED(arg);
      ObServer tmp_server;
      int64_t now = 0;
      int64_t preview_rotate_time = 0;
      bool need_report = false;
      bool need_balance = false;
      static const int64_t CHECK_INTERVAL_US = 100000LL; // 100ms
      TBSYS_LOG(INFO, "[NOTICE] heart beat checker thread start");
      while (!_stop)
      {
        need_report = false;
        need_balance = false;
        now = tbsys::CTimeUtil::getTime();
        if ((now > preview_rotate_time + 10 *1000 * 1000) &&
            ((now/(1000 * 1000)  - timezone) % (24 * 3600)  == 0))
        {
          preview_rotate_time = now;
          TBSYS_LOG(INFO, "rotateLog");
          TBSYS_LOGGER.rotateLog(NULL, NULL);
        }
        int64_t monotonic_now = tbsys::CTimeUtil::getMonotonicTime();
        //server_need_hb.init(will_heart_beat, ObChunkServerManager::MAX_SERVER_COUNT);
        if (root_server_->is_master())
        {
          ObChunkServerManager::iterator it = root_server_->server_manager_.begin();
          char server_str[OB_IP_STR_BUFF];
          for (; it != root_server_->server_manager_.end(); ++it)
          {
            if (it->status_ != ObServerStatus::STATUS_DEAD)
            {
              if (it->is_alive(monotonic_now, root_server_->config_.flag_cs_lease_duration_us_.get()))
              {
                if (monotonic_now - it->last_hb_time_ >= (root_server_->config_.flag_cs_lease_duration_us_.get() / 2 + it->hb_retry_times_ * HB_RETRY_FACTOR))
                {
                  it->hb_retry_times_ ++;
                  tmp_server = it->server_;
                  tmp_server.set_port(it->port_cs_);
                  //need hb
                  //server_need_hb.push_back(tmp_server);
                  if (TBSYS_LOGGER._level >= TBSYS_LOG_LEVEL_DEBUG)
                  {
                    tmp_server.to_string(server_str, OB_IP_STR_BUFF);
                    //TBSYS_LOG(DEBUG, "send hb to %s", server_str);
                  }
                  if (root_server_->worker_->get_rpc_stub().heartbeat_to_cs(tmp_server, root_server_->config_.flag_cs_lease_duration_us_.get(),
                        root_server_->last_frozen_mem_version_, root_server_->get_schema_version()) == OB_SUCCESS)
                  {
                    //do nothing
                  }
                }
              }
              else
              {
                TBSYS_LOG(ERROR, "server is down,monotonic_now:%ld,lease_duration:%ld",monotonic_now,root_server_->config_.flag_cs_lease_duration_us_.get());
                root_server_->server_manager_.set_server_down(it);
                root_server_->log_worker_->server_is_down(it->server_, now);

                tbsys::CThreadGuard mutex_guard(&(root_server_->root_table_build_mutex_)); //this for only one thread modify root_table
                tbsys::CWLockGuard guard(root_server_->root_table_rwlock_);
                if (root_server_->root_table_ != NULL)
                {
                  root_server_->root_table_->server_off_line(static_cast<int32_t>(it - root_server_->server_manager_.begin()), now);
                  // some cs is down, signal the balance worker
                  root_server_->balancer_thread_->wakeup();
                }
                else
                {
                  TBSYS_LOG(ERROR, "root_table_for_query_ = NULL, server_index=%ld", it - root_server_->server_manager_.begin());
                }
             }
            }

            if (it->ms_status_ != ObServerStatus::STATUS_DEAD && it->port_ms_ != 0)
            {
              if (it->is_ms_alive(monotonic_now,root_server_->config_.flag_cs_lease_duration_us_.get()) )
              {
                if (monotonic_now - it->last_hb_time_ms_ >
                    (root_server_->config_.flag_cs_lease_duration_us_.get() / 2))
                {
                    //hb to ms
                    tmp_server = it->server_;
                    tmp_server.set_port(it->port_ms_);
                    root_server_->worker_->get_rpc_stub().heartbeat_to_ms(tmp_server, root_server_->config_.flag_cs_lease_duration_us_.get(),
                                                                          root_server_->get_schema_version(), root_server_->get_obi_role(),
                                                                          root_server_->get_config_version());
                }
              }
              else
              {
                root_server_->server_manager_.set_server_down_ms(it);
              }
            }
          } //end for
        } //end if master
        //async heart beat
        usleep(CHECK_INTERVAL_US);
      }
      TBSYS_LOG(INFO, "[NOTICE] heart beat checker thread exit");
    }

  }
}

