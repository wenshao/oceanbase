/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: ob_update_server_param.h,v 0.1 2010/09/30 15:49:00 chuanhui Exp $
 *
 * Authors:
 *   chuanhui <rizhao.ych@taobao.com>
 *     - some work details if you want
 *
 */
#ifndef __OCEANBASE_CHUNKSERVER_OB_UPDATE_SERVER_PARAM_H__
#define __OCEANBASE_CHUNKSERVER_OB_UPDATE_SERVER_PARAM_H__

#include "tbsys.h"
#include "common/ob_define.h"
#include "common/ob_string.h"
#include "ob_ups_utils.h"
#include "ob_slave_sync_type.h"
namespace oceanbase
{
  namespace updateserver
  {
    class ObUpdateServerParam
    {
      public:
        const static int64_t TIMEOUT_DELTA_US = 10*1000;
        ObUpdateServerParam();
        ~ObUpdateServerParam();

      public:
        int load_from_config();
        int reload_from_config(const common::ObString& conf_file);

      public:
        inline int32_t get_ups_port() const
        {
          return ups_port_;
        }

        inline int32_t get_lsync_port() const
        {
          return lsync_port_;
        }

        inline int64_t get_read_thread_count() const
        {
          return read_thread_count_;
        }

        inline int64_t get_store_thread_count() const
        {
          return store_thread_count_;
        }

        inline const char* get_dev_name() const
        {
          return dev_name_;
        }

        inline int64_t get_read_task_queue_size() const
        {
          return read_task_queue_size_;
        }

        inline int64_t get_write_task_queue_size() const
        {
          return write_task_queue_size_;
        }

        inline int64_t get_log_task_queue_size() const
        {
          return log_task_queue_size_;
        }

        inline int64_t get_lease_task_queue_size() const
        {
          return lease_task_queue_size_;
        }

        inline int64_t get_store_thread_queue_size() const
        {
          return store_thread_queue_size_;
        }

        inline const char* get_log_dir_path() const
        {
          return log_dir_path_;
        }

        inline int64_t get_log_size_mb() const
        {
          return log_size_mb_;
        }

        inline const char* get_ups_vip() const
        {
          return ups_vip_;
        }

        inline const char* get_inst_master_rs_ip() const
        {
          return inst_master_rs_ip_;
        }

        inline const int32_t get_inst_master_rs_port() const
        {
          return inst_master_rs_port_;
        }

        inline const char* get_lsync_ip() const
        {
          return lsync_ip_;
        }

        inline int64_t get_log_sync_type() const
        {
          return log_sync_type_;
        }

        inline ObSlaveSyncType::ObSlaveType get_slave_type() const
        {
          ObSlaveSyncType::ObSlaveType slave_type = (slave_type_ == 1 ? ObSlaveSyncType::REAL_TIME_SLAVE : ObSlaveSyncType::NON_REAL_TIME_SLAVE);
          return slave_type;
        }

        inline int64_t get_pre_process() const
        {
          return pre_process_;
        }

        inline int64_t get_log_sync_limit_kb() const
        {
          return log_sync_limit_kb_;
        }

        inline int64_t get_log_sync_retry_times() const
        {
          return log_sync_retry_times_;
        }

        inline const char* get_app_name() const
        {
          return app_name_;
        }

        inline int64_t get_write_thread_batch_num() const
        {
          return write_thread_batch_num_;
        }

        inline const char* get_root_server_ip() const
        {
          return rs_vip_;
        }

        inline int32_t get_root_server_port() const
        {
          return rs_port_;
        }

        inline int64_t get_fetch_schema_times() const
        {
          return fetch_schema_times_;
        }

        inline int64_t get_fetch_schema_timeout_us() const
        {
          return fetch_schema_timeout_us_;
        }

        inline int64_t get_resp_root_times() const
        {
          return resp_root_times_;
        }

        inline int64_t get_resp_root_timeout_us() const
        {
          return resp_root_timeout_us_;
        }

        inline int64_t get_state_check_period_us() const
        {
          return state_check_period_us_;
        }

        //inline int64_t get_lease_interval_us() const
        //{
        //  return lease_interval_us_;
        //}

        //inline int64_t get_lease_reserved_time_us() const
        //{
        //  return lease_reserved_time_us_;
        //}

        inline int64_t get_log_sync_timeout_us() const
        {
          return log_sync_timeout_us_;
        }

        inline int64_t get_register_times() const
        {
          return register_times_;
        }

        inline int64_t get_register_timeout_us() const
        {
          return register_timeout_us_;
        }

        inline int64_t get_replay_wait_time_us() const
        {
          return replay_wait_time_us_;
        }

        inline int64_t get_fetch_log_wait_time_us() const
        {
          return fetch_log_wait_time_us_;
        }

        inline int64_t get_log_sync_delay_warn_time_threshold_us() const
        {
          return log_sync_delay_warn_time_threshold_us_;
        }
        
        inline int64_t get_log_sync_delay_warn_report_interval_us() const
        {
          return log_sync_delay_warn_report_interval_us_;
        }

        inline int64_t get_max_n_lagged_log_allowed() const
        {
          return max_n_lagged_log_allowed_;
        }

        inline const char* get_standalone_schema() const
        {
          return standalone_schema_;
        }

        inline int64_t get_test_schema_version() const
        {
          return test_schema_version_;
        }

        inline int64_t get_total_memory_limit() const
        {
          return total_memory_limit_;
        }

        inline int64_t get_table_memory_limit() const
        {
          return table_memory_limit_;
        }

        //inline int64_t get_lease_on() const
        //{
        //  return lease_on_;
        //}

        inline int64_t get_slave_fail_wait_lease_on() const
        {
          return slave_fail_wait_lease_on_;
        }

        inline int64_t get_packet_max_timewait() const
        {
          return packet_max_timewait_;
        }

        inline int64_t get_max_thread_memblock_num() const
        {
          return max_thread_memblock_num_;
        }

        inline const char* get_log_fetch_option() const
        {
          return fetch_opt_;
        }

        inline const int64_t get_trans_proc_time_warn_us() const
        {
          return trans_proc_time_warn_us_;
        }

        int64_t get_ups_inner_port() const
        {
          return ups_inner_port_;
        }

        inline const char *get_store_root() const
        {
          return store_root_;
        }

        inline const char *get_raid_regex() const
        {
          return raid_regex_;
        }

        inline const char *get_dir_regex() const
        {
          return dir_regex_;
        }

        inline const int64_t get_blockcache_size_mb() const
        {
          return blockcache_size_mb_;
        }

        inline const int64_t get_blockindex_cache_size_mb() const
        {
          return blockindex_cache_size_mb_;
        }

        inline const int64_t get_active_mem_limit_gb() const
        {
          return active_mem_limit_gb_;
        }

        inline const int64_t get_minor_num_limit() const
        {
          return minor_num_limit_;
        }

        inline const int64_t get_sstable_time_limit_s() const
        {
          return sstable_time_limit_s_;
        }

        inline const int64_t get_min_major_freeze_interval_s() const
        {
          return min_major_freeze_interval_s_;
        }

        inline const int get_replay_checksum_flag() const
        {
          return replay_checksum_flag_;
        }

        inline const int get_allow_write_without_token() const
        {
          return allow_write_without_token_;
        }

        inline const int64_t get_slave_sync_sstable_num() const
        {
          return slave_sync_sstable_num_;
        }

        inline const char *get_sstable_compressor_name() const
        {
          return sstable_compressor_name_;
        }

        inline const int64_t get_sstable_block_size() const
        {
          return sstable_block_size_;
        }

        inline const struct tm &get_major_freeze_duty_time() const
        {
          return major_freeze_duty_time_;
        }

        inline const int64_t get_lsync_fetch_timeout_us() const
        {
          return lsync_fetch_timeout_us_;
        }

        inline const int64_t get_refresh_lsync_addr_interval_us() const
        {
          return refresh_lsync_addr_interval_us_;
        }

        inline const int64_t get_refresh_lsync_addr_timeout() const
        {
          return refresh_lsync_addr_interval_us_ - TIMEOUT_DELTA_US;
        }

        inline const int64_t get_max_row_cell_num() const
        {
          return max_row_cell_num_;
        }

        inline const int64_t get_table_available_warn_size() const
        {
          return table_available_warn_size_;
        }

        inline const int64_t get_table_available_error_size() const
        {
          return table_available_error_size_;
        }

        inline const int64_t get_keep_alive_timeout() const
        {
          return ups_keep_alive_timeout_us_;
        }

        inline const int64_t get_lease_timeout_in_advance() const
        {
          return lease_timeout_in_advance_;
        }

        inline const CacheWarmUpConf &get_warm_up_conf() const
        {
          return warm_up_conf_;
        };

        inline int get_using_memtable_bloomfilter() const
        {
          return using_memtable_bloomfilter_;
        };

        inline int get_sstable_dio_writing() const
        {
          return sstable_dio_writing_;
        };
      public:
        int64_t get_low_priv_network_lower_limit() const
        {
          return low_priv_network_lower_limit_;
        }

        int64_t get_low_priv_network_upper_limit() const
        {
          return low_priv_network_upper_limit_;
        }

        int64_t get_low_priv_cur_percent() const
        {
          return low_priv_cur_percent_;
        }

        int64_t get_low_priv_adjust_flag() const
        {
          return low_priv_adjust_flag_;
        }

      public:
        // for test
        void set_update_server_port(const int32_t update_server_port)
        {
          ups_port_ = update_server_port;
        }

        void set_read_thread_count(const int64_t read_thread_count)
        {
          read_thread_count_ = read_thread_count;
        }

        void set_priv_queue_conf(const UpsPrivQueueConf& priv_queue_conf)
        {
          if (priv_queue_conf.low_priv_network_lower_limit != 0)
          {
            low_priv_network_lower_limit_ = priv_queue_conf.low_priv_network_lower_limit;
          }

          if (priv_queue_conf.low_priv_network_upper_limit != 0)
          {
            low_priv_network_upper_limit_ = priv_queue_conf.low_priv_network_upper_limit;
          }

          if (priv_queue_conf.low_priv_adjust_flag != 0)
          {
            low_priv_adjust_flag_ = priv_queue_conf.low_priv_adjust_flag;
          }

          if (priv_queue_conf.low_priv_cur_percent != 0)
          {
            low_priv_cur_percent_ = priv_queue_conf.low_priv_cur_percent;
          }
        }
      private:
        int load_master_conf();
        int load_slave_conf();
        int load_standalone_conf();
        int load_string(char* dest, const int32_t size,
            const char* section, const char* name, bool not_null, tbsys::CConfig *config = NULL);

      private:
        static const int64_t DEFAULT_READ_THREAD_COUNT = 14;
        static const int64_t DEFAULT_STORE_THREAD_COUNT = 1;
        static const int64_t DEFAULT_TASK_READ_QUEUE_SIZE = 1000;
        static const int64_t DEFAULT_TASK_WRITE_QUEUE_SIZE = 1000;
        static const int64_t DEFAULT_TASK_LOG_QUEUE_SIZE = 100;
        static const int64_t DEFAULT_TASK_LEASE_QUEUE_SIZE = 100;
        static const int64_t DEFAULT_STORE_THREAD_QUEUE_SIZE = 100;
        static const int64_t DEFAULT_LOG_SIZE_MB = 64; // 64MB
        static const int64_t DEFAULT_WRITE_GROUP_COMMIT_NUM = 1024;
        static const int64_t DEFAULT_FETCH_SCHEMA_TIMES = 10;
        static const int64_t DEFAULT_FETCH_SCHEMA_TIMEOUT_US = 3L * 1000 * 1000; // 3s
        static const int64_t DEFAULT_RESP_ROOT_TIMES = 20;
        static const int64_t DEFAULT_RESP_ROOT_TIMEOUT_US = 1L * 1000 * 1000; // 1s
        static const int64_t DEFAULT_STATE_CHECK_PERIOD_US = 500L * 1000; // 500ms
        static const int64_t DEFAULT_REGISTER_TIMES = 10;
        static const int64_t DEFAULT_REGISTER_TIMEOUT_US = 3L * 1000 * 1000; // 3s
        static const int64_t DEFAULT_REPLAY_WAIT_TIME_US = 100L * 1000; // 100ms
        static const int64_t DEFAULT_FETCH_LOG_WAIT_TIME_US = 500L * 1000; // 500ms
        static const int64_t DEFAULT_LOG_SYNC_DELAY_WARN_TIME_THRESHOLD_US = 500L * 1000;
        static const int64_t DEFAULT_LOG_SYNC_DELAY_WARN_REPORT_INTERVAL_US = 10 * 1000L * 1000;
        static const int64_t DEFAULT_MAX_N_LAGGED_LOG_ALLOWED =  10000;
        static const int64_t OB_MAX_DEV_NAME_SIZE = 64;
        static const int64_t OB_MAX_IP_SIZE = 64;
        static const int64_t OB_MAX_ROLE_SIZE = 64;
        static const int64_t DEFAULT_LOG_SYNC_TYPE = 0;
        static const int64_t DEFAULT_PRE_PROCESS = 0;
        static const int64_t DEFAULT_LOG_SYNC_LIMIT_KB = 1024 * 40;
        static const int64_t DEFAULT_LOG_SYNC_RETRY_TIMES = 2;
        static const int64_t DEFAULT_TOTAL_MEMORY_LIMIT = 25; // 25G
        static const int64_t DEFAULT_TABLE_MEMORY_LIMIT = 10; // 10G
        static const int64_t GB_UNIT = 1024L * 1024L * 1024L; // GB
        static const int64_t MB_UNIT = 1024L * 1024L; // MB
        //static const int64_t DEFAULT_LEASE_INTERVAL_US = 8000000;
        //static const int64_t DEFAULT_LEASE_RESERVED_TIME_US = 5000000;
        //static const int64_t DEFAULT_LEASE_ON = 1;
        static const int64_t DEFAULT_SLAVE_FAIL_WAIT_LEASE_ON = 0;
        static const int64_t DEFAULT_LOG_SYNC_TIMEOUT_US = 500 * 1000;
        static const int64_t DEFAULT_PACKET_MAX_TIMEWAIT = 10 * 1000 * 1000;
        static const int64_t DEFAULT_LSYNC_FETCH_TIMEOUT_US = 5 * 1000 * 1000;
        static const int64_t DEFAULT_REFRESH_LSYNC_ADDR_INTERVAL_US = 60 * 1000 * 1000;
        static const int64_t DEFAULT_MAX_THREAD_MEMBLOCK_NUM = 10;
        static const int64_t DEFAULT_TRANS_PROC_TIME_WARN_US = 1000000;
        static const int64_t DEFAULT_BLOCKCACHE_SIZE_MB = 1024;  // 1GB
        static const int64_t DEFAULT_BLOCKINDEX_CACHE_SIZE_MB = 100; // 100MB
      public:
        static const int64_t MIN_ACTIVE_MEM_LIMIT = GB_UNIT * 2L;
        static const int64_t DEFAULT_ACTIVE_MEM_LIMIT_GB = 10; // 10GB
        static const int64_t DEFAULT_MINOR_NUM_LIMIT = 255;
        static const int64_t DEFAULT_FROZEN_MEM_LIMIT_GB = 20; // 20GB
        static const int64_t DEFAULT_SSTABLE_TIME_LIMIT_S = 86400;  // 1day
        static const int64_t DEFAULT_SLAVE_SYNC_SSTABLE_NUM = 1;
        static const int64_t DEFAULT_SSTABLE_BLOCK_SIZE = 4096;
        static const int64_t DEFAULT_MIN_MAJOR_FREEZE_INTERVAL_S = 10 * 60 * 60; // 10 hour

        static const int64_t DEFAULT_LOW_PRIV_NETWORK_LOWER_LIMIT = 30; // 30M
        static const int64_t DEFAULT_LOW_PRIV_NETWORK_UPPER_LIMIT = 80; // 80M
        static const int64_t DEFAULT_LOW_PRIV_CUR_PERCENT = 10;
        static const int64_t DEFAULT_LOW_PRIV_ADJUST_FLAG = 1;
        static const int DEFAULT_REPLAY_CHECKSUM_FLAG = 1;  // 默认做
        static const int DEFAULT_ALLOW_WRITE_WITHOUT_TOKEN = 1; // 允许
        static const int64_t DEFAULT_MAX_ROW_CELL_NUM = 128;
        static const int64_t DEFAULT_TABLE_AVAILABLE_WARN_SIZE_GB = 10; // 10g
        static const int64_t DEFAULT_TABLE_AVAILABLE_ERROR_SIZE_GB = 5; // 5g
        static const int64_t DEFAULT_TABLE_AVAILABLE_WARN_SIZE = DEFAULT_TABLE_AVAILABLE_WARN_SIZE_GB * GB_UNIT;
        static const int64_t DEFAULT_TABLE_AVAILABLE_ERROR_SIZE = DEFAULT_TABLE_AVAILABLE_ERROR_SIZE_GB * GB_UNIT;
        static const int DEFAULT_USING_MEMTABLE_BLOOMFILTER = 0; // 默认不用
        static const int DEFAULT_SSTABLE_DIO_WRITING = 1; //默认使用dio方式写sstable
        static const int64_t DEFAULT_KEEP_ALIVE_TIMEOUT_US = 5 * 1000 * 1000;
        static const int64_t DEFAULT_LEASE_TIMEOUT_IN_ADVANCE_US = 500 * 1000;
        static const int DEFAULT_USING_STATIC_CM_COLUMN_ID = 0;
        static const int DEFAULT_USING_HASH_INDEX = 0;

      private:
        int32_t ups_port_;
        int64_t read_thread_count_;
        int64_t store_thread_count_;
        char dev_name_[OB_MAX_DEV_NAME_SIZE];
        int64_t read_task_queue_size_;
        int64_t write_task_queue_size_;
        int64_t log_task_queue_size_;
        int64_t lease_task_queue_size_;
        int64_t store_thread_queue_size_;
        char log_dir_path_[common::OB_MAX_FILE_NAME_LENGTH];
        int64_t log_size_mb_;
        char app_name_[common::OB_MAX_APP_NAME_LENGTH];
        int64_t write_thread_batch_num_;
        char rs_vip_[OB_MAX_IP_SIZE];
        int32_t rs_port_;
        int64_t fetch_schema_times_;
        int64_t fetch_schema_timeout_us_;
        int64_t resp_root_times_;
        int64_t resp_root_timeout_us_;
        int64_t register_times_;
        int64_t register_timeout_us_;
        char standalone_schema_[common::OB_MAX_FILE_NAME_LENGTH];
        int64_t test_schema_version_;
        char ups_vip_[OB_MAX_IP_SIZE];
        char lsync_ip_[OB_MAX_IP_SIZE];
        int32_t lsync_port_;
        char inst_master_rs_ip_[OB_MAX_IP_SIZE];
        int32_t inst_master_rs_port_;
        int64_t replay_wait_time_us_;
        int64_t fetch_log_wait_time_us_;
        int64_t log_sync_delay_warn_time_threshold_us_;
        int64_t log_sync_delay_warn_report_interval_us_;
        int64_t max_n_lagged_log_allowed_;
        int64_t state_check_period_us_;
        int64_t log_sync_type_;
        int64_t pre_process_;
        int64_t log_sync_limit_kb_;
        int64_t log_sync_retry_times_;
        int64_t total_memory_limit_;
        int64_t table_memory_limit_;
        //int64_t lease_interval_us_;
        //int64_t lease_reserved_time_us_;
        //int64_t lease_on_;
        int64_t slave_fail_wait_lease_on_;
        int64_t log_sync_timeout_us_;
        int64_t packet_max_timewait_;
        int64_t max_thread_memblock_num_;
        int64_t trans_proc_time_warn_us_;

        int64_t ups_inner_port_;
        int64_t low_priv_network_lower_limit_;
        int64_t low_priv_network_upper_limit_;
        int64_t low_priv_adjust_flag_;
        int64_t low_priv_cur_percent_;

        char store_root_[common::OB_MAX_FILE_NAME_LENGTH];
        char raid_regex_[common::OB_MAX_FILE_NAME_LENGTH];
        char dir_regex_[common::OB_MAX_FILE_NAME_LENGTH];
        int64_t blockcache_size_mb_;
        int64_t blockindex_cache_size_mb_;
        int64_t slave_sync_sstable_num_;

        int64_t active_mem_limit_gb_;
        int64_t minor_num_limit_;
        int64_t sstable_time_limit_s_;
        char sstable_compressor_name_[common::OB_MAX_FILE_NAME_LENGTH];
        int64_t sstable_block_size_;
        struct tm major_freeze_duty_time_;
        int64_t min_major_freeze_interval_s_;
        int replay_checksum_flag_;
        int allow_write_without_token_;

        char fetch_opt_[common::OB_MAX_FETCH_CMD_LENGTH];

        int64_t lsync_fetch_timeout_us_;
        int64_t refresh_lsync_addr_interval_us_;
        int64_t max_row_cell_num_;
        int64_t table_available_warn_size_;
        int64_t table_available_error_size_;

        CacheWarmUpConf warm_up_conf_;
        int using_memtable_bloomfilter_;
        int sstable_dio_writing_;

        int64_t ups_keep_alive_timeout_us_;
        int64_t lease_timeout_in_advance_;
        int64_t slave_type_;

        int using_static_cm_column_id_;
        int using_hash_index_;
    };
  }
}

#endif //__OB_UPDATE_SERVER_PARAM_H__

