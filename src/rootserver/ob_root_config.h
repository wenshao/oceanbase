/*
 * Copyright (C) 2007-2012 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * Description here
 *
 * Version: $Id$
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *     - some work details here
 */
#ifndef _OB_ROOT_CONFIG_H
#define _OB_ROOT_CONFIG_H 1

#include "common/ob_config.h"
#include "common/ob_client_config.h"

namespace oceanbase
{
  namespace rootserver
  {
    using common::ObFlag;

        
    class ObRootConfig : public common::ObConfig
    {
      public:
        // flags
        // naming: flag_xxx_
        // naming about time: flag_xxx_us_, flag_xxx_seconds_ 
        ObFlag<const char*> flag_schema_filename_;
        ObFlag<int64_t> flag_cs_lease_duration_us_;
        ObFlag<int> flag_migrate_wait_seconds_;
        ObFlag<int> flag_safe_lost_one_duration_seconds_;
        ObFlag<int> flag_safe_wait_init_duration_seconds_;
        ObFlag<int> flag_safe_copy_count_in_init_;
        ObFlag<int> flag_create_table_in_init_;
        ObFlag<int> flag_tablet_replicas_num_;   
        ObFlag<int> flag_thread_count_;
        ObFlag<int> flag_read_task_queue_size_;
        ObFlag<int> flag_write_task_queue_size_;
        ObFlag<int> flag_log_task_queue_size_;
        ObFlag<int64_t> flag_network_timeout_us_;
        ObFlag<int> flag_lease_on_;
        ObFlag<int64_t> flag_lease_interval_us_;
        ObFlag<int64_t> flag_lease_reserved_time_us_;
        ObFlag<int64_t> flag_slave_register_timeout_us_;
        ObFlag<int64_t> flag_log_sync_timeout_us_;
        ObFlag<const char*> flag_my_vip_;
        ObFlag<int> flag_my_port_;
        ObFlag<const char*> flag_dev_name_;
        ObFlag<int64_t> flag_vip_check_period_us_;
        ObFlag<int64_t> flag_log_replay_wait_time_us_;
        ObFlag<int64_t> flag_log_sync_limit_kb_;
        ObFlag<int64_t> flag_cs_merge_delay_interval_us_;
        ObFlag<int64_t> flag_max_merge_duration_seconds_;
        ObFlag<int> flag_cs_probation_period_seconds_;
        ObFlag<int> flag_basic_util_ratio_tolerance_;
        ObFlag<int64_t> flag_ups_lease_us_;
        ObFlag<int64_t> flag_ups_lease_reserved_us_;
        ObFlag<int64_t> flag_ups_renew_reserved_us_;
        ObFlag<int64_t> flag_ups_waiting_register_duration_us_;
        ObFlag<int64_t> flag_expected_request_process_us_;
        // balance related flags        
        ObFlag<int64_t> flag_balance_worker_idle_sleep_seconds_;
        ObFlag<int64_t> flag_balance_tolerance_count_;
        ObFlag<int64_t> flag_balance_timeout_us_delta_;
        ObFlag<int64_t> flag_balance_max_timeout_seconds_;
        ObFlag<int> flag_balance_max_concurrent_migrate_num_;
        ObFlag<int> flag_balance_max_migrate_out_per_cs_;
        ObFlag<int> flag_enable_balance_;
        ObFlag<int> flag_enable_rereplication_;
        ObFlag<int64_t> flag_tablet_migrate_disabling_period_us_;
        ObFlag<int> flag_obconnector_port_;
        ObFlag<int> flag_is_import_;
        ObFlag<int> flag_read_master_master_ups_percent_;
        ObFlag<int> flag_read_slave_master_ups_percent_;
        common::ObClientConfig client_config_;
      public:
        ObRootConfig();
        virtual ~ObRootConfig();
        void print() const;
      private:
        int add_flags();
        int load_flags(tbsys::CConfig &cconf);
        int load_client_config(tbsys::CConfig &config);
        // disallow copy
        ObRootConfig(const ObRootConfig &other);
        ObRootConfig& operator=(const ObRootConfig &other);
    };
  } // end namespace rootserver
} // end namespace oceanbase
#endif /* _OB_ROOT_CONFIG_H */

