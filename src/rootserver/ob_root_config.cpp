#include "ob_root_config.h"
#include "common/ob_tsi_factory.h"

using namespace oceanbase::rootserver;
using namespace oceanbase::common;

ObRootConfig::ObRootConfig()
{
  memset(config_filename_, 0, sizeof(config_filename_));
  add_flags();
}

ObRootConfig::~ObRootConfig()
{
}

int ObRootConfig::load_flags(tbsys::CConfig &cconf)
{
  int ret = OB_SUCCESS;
  ret = ObConfig::load_flags(cconf);

  if (OB_SUCCESS == ret)
  {
    ret = load_client_config(cconf);
  }
  return ret;
}


int ObRootConfig::add_flags()
{
  int ret = OB_SUCCESS;
  ADD_FLAG(flag_schema_filename_, SECTION_STR_SCHEMA, "file_name", "");
  ADD_FLAG(flag_cs_lease_duration_us_, SECTION_STR_CS, "lease", 10000000LL);
  ADD_FLAG(flag_migrate_wait_seconds_, SECTION_STR_RS, "migrate_wait_seconds", 60);
  ADD_FLAG(flag_safe_lost_one_duration_seconds_, SECTION_STR_CS,
                                  "safe_lost_one_duration", 3600);
  ADD_FLAG(flag_safe_wait_init_duration_seconds_, SECTION_STR_CS,
                                           "wait_init_duration", 60);
  ADD_FLAG(flag_safe_copy_count_in_init_, SECTION_STR_RS,
                                   "__safe_copy_count_in_init", 2);
  ADD_FLAG(flag_create_table_in_init_, SECTION_STR_RS, "__create_table_in_init", 0);
  ADD_FLAG(flag_tablet_replicas_num_, SECTION_STR_RS, "tablet_replicas_num", 3);
  ADD_FLAG(flag_thread_count_, SECTION_STR_RS, "thread_count", 20);
  ADD_FLAG(flag_read_task_queue_size_, SECTION_STR_RS, "read_queue_size", 500);
  ADD_FLAG(flag_write_task_queue_size_, SECTION_STR_RS, "write_queue_size", 50);
  ADD_FLAG(flag_log_task_queue_size_, SECTION_STR_RS, "log_queue_size", 50);
  ADD_FLAG(flag_network_timeout_us_, SECTION_STR_RS,
                           "network_timeout", 100LL*1000/*100ms*/);
  ADD_FLAG(flag_lease_on_, SECTION_STR_RS, "lease_on", 1);
  ADD_FLAG(flag_lease_interval_us_, SECTION_STR_RS,
                             "lease_interval_us", 8LL*1000*1000);
  ADD_FLAG(flag_lease_reserved_time_us_, SECTION_STR_RS,
                                  "lease_reserved_time_us", 5LL*1000*1000);
  ADD_FLAG(flag_ups_renew_reserved_us_, SECTION_STR_UPS,
	   "ups_renew_reserved_us", 7770000LL/*7.77s*/);
  ADD_FLAG(flag_slave_register_timeout_us_, SECTION_STR_RS,
                                    "register_timeout_us", 3LL*1000*1000);
  ADD_FLAG(flag_log_sync_timeout_us_, SECTION_STR_RS,
                               "log_sync_timeout_us", 500LL*1000);
  ADD_FLAG(flag_my_vip_, SECTION_STR_RS, "vip", "");
  ADD_FLAG(flag_my_port_, SECTION_STR_RS, "port", 0);
  ADD_FLAG(flag_dev_name_, SECTION_STR_RS, "dev_name", "");
  ADD_FLAG(flag_vip_check_period_us_, SECTION_STR_RS,
                               "vip_check_period_us", 500LL*1000);
  ADD_FLAG(flag_log_replay_wait_time_us_, SECTION_STR_RS,
                                   "replay_wait_time_us", 100LL*1000);
  ADD_FLAG(flag_log_sync_limit_kb_, SECTION_STR_RS,
                             "log_sync_limit_kb", 40*1024);
  ADD_FLAG(flag_cs_merge_delay_interval_us_, SECTION_STR_RS,
                                      "cs_command_interval_us", 60LL*1000*1000);
  ADD_FLAG(flag_max_merge_duration_seconds_, SECTION_STR_CS,
                                      "max_merge_duration", 7200LL);
  ADD_FLAG(flag_cs_probation_period_seconds_, SECTION_STR_CS,
                                       "cs_probation_period", 10);
  ADD_FLAG(flag_basic_util_ratio_tolerance_, SECTION_STR_CS,
                                      "__basic_util_ratio_tolerance", 10);
  ADD_FLAG(flag_ups_lease_us_, SECTION_STR_UPS,
                        "lease_us", 2000000LL/*2s*/);
  ADD_FLAG(flag_ups_lease_reserved_us_, SECTION_STR_UPS,
                                 "lease_reserved_us", 1000000LL/*1s*/);
  ADD_FLAG(flag_ups_waiting_register_duration_us_, SECTION_STR_UPS,
                                            "waiting_reigster_duration_us", 15000000LL/*15s*/);
  ADD_FLAG(flag_expected_request_process_us_, SECTION_STR_RS, "expected_process_us", 10000LL/*10ms*/);
  ADD_FLAG(flag_balance_worker_idle_sleep_seconds_, SECTION_STR_RS,
                                        "balance_worker_idle_sleep_sec", 30);
  ADD_FLAG(flag_balance_tolerance_count_, SECTION_STR_CS, "balance_tolerance", 10);
  ADD_FLAG(flag_balance_timeout_us_delta_, SECTION_STR_CS, "balance_timeout_us_delta", 10000000LL/*10s*/);
  ADD_FLAG(flag_balance_max_timeout_seconds_, SECTION_STR_CS, "max_batch_migrate_timeout", 300/*5min*/);
  ADD_FLAG(flag_balance_max_concurrent_migrate_num_, SECTION_STR_CS,
                                              "max_concurrent_migrate_per_cs", 2);
  ADD_FLAG(flag_balance_max_migrate_out_per_cs_, SECTION_STR_CS,
                                          "max_batch_migrate_out_per_cs", 20);
  ADD_FLAG(flag_enable_balance_, SECTION_STR_RS, "__enable_balance", 1);
  ADD_FLAG(flag_is_import_, SECTION_STR_RS, "is_import", 0);
  ADD_FLAG(flag_read_master_master_ups_percent_, SECTION_STR_UPS, "read_master_master_ups_percent", -1);
  ADD_FLAG(flag_read_slave_master_ups_percent_, SECTION_STR_UPS, "read_slave_master_ups_percent", -1);
  ADD_FLAG(flag_enable_rereplication_, SECTION_STR_RS, "__enable_rereplication", 1);
  ADD_FLAG(flag_tablet_migrate_disabling_period_us_, SECTION_STR_CS, "tablet_migrate_disabling_period_us", 60000000LL/*1min*/);
  ADD_FLAG(flag_obconnector_port_, SECTION_STR_OBCONNECTOR, "port", 5433);
  return ret;
}

void ObRootConfig::print() const
{
  ObConfig::print();
  client_config_.print();
}

int ObRootConfig::load_client_config(tbsys::CConfig &config)
{
  int ret = OB_SUCCESS;
  static const int MY_BUF_SIZE = 64;
  char conf_key[MY_BUF_SIZE];
  client_config_.BNL_alpha_ = config.getInt(SECTION_STR_CLIENT, "BNL_alpha",
                                            ObClientConfig::DEFAULT_BNL_ALPHA);
  client_config_.BNL_alpha_denominator_ = config.getInt(SECTION_STR_CLIENT, "BNL_alpha_denominator",
                                                        ObClientConfig::DEFAULT_BNL_ALPHA_DENOMINATOR);
  client_config_.BNL_threshold_ = config.getInt(SECTION_STR_CLIENT, "BNL_threshold",
                                                ObClientConfig::DEFAULT_BNL_THRESHOLD);
  client_config_.BNL_threshold_denominator_ = config.getInt(SECTION_STR_CLIENT, "BNL_threshold_denominator",
                                                            ObClientConfig::DEFAULT_BNL_THRESHOLD_DENOMINATOR);
  client_config_.obi_list_.obi_count_ = config.getInt(SECTION_STR_OBI, "obi_count", 0);
  TBSYS_LOG(INFO, "loading obi count=%d", client_config_.obi_list_.obi_count_);
  int32_t loaded_count = 0;
  for (int32_t i = 0; OB_SUCCESS == ret && i < client_config_.obi_list_.obi_count_ && i < client_config_.obi_list_.MAX_OBI_COUNT; ++i)
  {
    snprintf(conf_key, MY_BUF_SIZE, "obi%d_rs_vip", i);
    const char* ip =  config.getString(SECTION_STR_OBI, conf_key, NULL);
    snprintf(conf_key, MY_BUF_SIZE, "obi%d_rs_port", i);
    int port = config.getInt(SECTION_STR_OBI, conf_key, 0);
    snprintf(conf_key, MY_BUF_SIZE, "obi%d_read_percentage", i);
    int read_percentage = config.getInt(SECTION_STR_OBI, conf_key, -1);
    snprintf(conf_key, MY_BUF_SIZE, "obi%d_random_ms", i);
    int flag_random_ms = config.getInt(SECTION_STR_OBI, conf_key, 0);
    ObServer rs_addr;
    if (!rs_addr.set_ipv4_addr(ip, port))
    {
      TBSYS_LOG(ERROR, "invalid ups addr, addr=%s port=%d", ip, port);
      ret = OB_INVALID_ARGUMENT;
    }
    else if (0 > read_percentage || 100 < read_percentage)
    {
      TBSYS_LOG(ERROR, "invalid obi read_percentage=%d", read_percentage);
      ret = OB_INVALID_ARGUMENT;
    }
    else
    {
      client_config_.obi_list_.conf_array_[i].set_rs_addr(rs_addr);
      client_config_.obi_list_.conf_array_[i].set_read_percentage(read_percentage);
      if (0 != flag_random_ms)
      {
        client_config_.obi_list_.conf_array_[i].set_flag_rand_ms();
      }
      else
      {
        client_config_.obi_list_.conf_array_[i].unset_flag_rand_ms();
      }
      loaded_count++;
    }
  } // end for
  client_config_.obi_list_.obi_count_ = loaded_count;
  return ret;
}

