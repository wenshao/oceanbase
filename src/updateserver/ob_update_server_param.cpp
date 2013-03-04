/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: ob_update_server_param.cpp,v 0.1 2010/09/30 15:58:11 chuanhui Exp $
 *
 * Authors:
 *   chuanhui <rizhao.ych@taobao.com>
 *     - some work details if you want
 *
 */

#include "common/compress/ob_compressor.h"
#include "common/ob_thread_mempool.h"
#include "ob_update_server_param.h"
#include "ob_update_server_main.h"
#include "ob_ups_utils.h"

namespace
{
  const char* OBUPS_UPS_SECTION  = "update_server";
  const char* OBUPS_VIP = "vip";
  const char* OBUPS_PORT = "port";
  const char* OBINST_MASTER_RS_IP = "instance_master_rs_ip";
  const char* OBINST_MASTER_RS_PORT = "instance_master_rs_port";
  const char* OBUPS_LSYNC_IP = "lsync_ip";
  const char* OBUPS_LSYNC_PORT = "lsync_port";
  const char* OBUPS_DEVNAME = "dev_name";
  const char* OBUPS_APP_NAME = "app_name";
  const char* OBUPS_LOG_DIR_PATH = "log_dir_path";
  const char* OBUPS_LOG_SIZE_MB = "log_size_mb";
  const char* OBUPS_READ_THREAD_COUNT = "read_thread_count";
  const char* OBUPS_STORE_THREAD_COUNT = "store_thread_count";
  const char* OBUPS_READ_TASK_QUEUE_SIZE = "read_task_queue_size";
  const char* OBUPS_WRITE_TASK_QUEUE_SIZE = "write_task_queue_size";
  const char* OBUPS_WRITE_GROUP_COMMIT_NUM = "write_group_commit_num";
  const char* OBUPS_LOG_TASK_QUEUE_SIZE = "log_task_queue_size";
  const char* OBUPS_LEASE_TASK_QUEUE_SIZE = "lease_task_queue_size";
  const char* OBUPS_STORE_THREAD_QUEUE_SIZE = "store_thread_queue_size";
  const char* OBUPS_LOG_SYNC_TYPE = "log_sync_type";
  //const char* OBUPS_PRE_PROCESS = "pre_process";
  const char* OBUPS_FETCH_SCHEMA_TIMES = "fetch_schema_times";
  const char* OBUPS_FETCH_SCHEMA_TIMEOUT_US = "fetch_schema_timeout_us";
  const char* OBUPS_RESP_ROOT_TIMES = "resp_root_times";
  const char* OBUPS_RESP_ROOT_TIMEOUT_US = "resp_root_timeout_us";
  const char* OBUPS_STATE_CHECK_PERIOD_US = "state_check_period_us";
  const char* OBUPS_REGISTER_TIMES = "register_times";
  const char* OBUPS_REGISTER_TIMEOUT_US = "register_timeout_us";
  const char* OBUPS_REPLAY_WAIT_TIME_US = "replay_wait_time_us";
  const char* OBUPS_FETCH_LOG_WAIT_TIME_US = "fetch_log_wait_time_us";
  const char* OBUPS_LOG_SYNC_DELAY_WARN_TIME_THRESHOLD_US = "log_sync_delay_warn_time_threshold_us";
  const char* OBUPS_LOG_SYNC_DELAY_WARN_REPORT_INTERVAL_US = "log_sync_delay_warn_time_report_interval_us";
  const char* OBUPS_MAX_N_LAGGED_LOG_ALLOWED = "max_n_lagged_log_allowed";
  const char* OBUPS_LOG_SYNC_LIMIT_KB = "log_sync_limit_kb";
  const char* OBUPS_LOG_SYNC_RETRY_TIMES = "log_sync_retry_times";
  const char* OBUPS_TOTAL_MEMORY_LIMIT = "total_memory_limit";
  const char* OBUPS_TABLE_MEMORY_LIMIT = "table_memory_limit";
  //const char* OBUPS_LEASE_INTERVAL_US = "lease_interval_us";
  //const char* OBUPS_LEASE_RESERVED_TIME_US = "lease_reserved_time_us";
  //const char* OBUPS_LEASE_ON = "lease_on";
  const char* OBUPS_SLAVE_FAIL_WAIT_LEASE_ON = "slave_fail_wait_lease_on";
  const char* OBUPS_LOG_SYNC_TIMEOUT_US = "log_sync_timeout_us";
  const char* OBUPS_PACKET_MAX_TIMEWAIT = "packet_max_timewait";
  const char* OBUPS_LSYNC_FETCH_TIMEOUT_US = "lsync_fetch_timeout_us";
  const char* OBUPS_REFRESH_LSYNC_ADDR_INTERVAL_US = "refresh_lsync_addr_interval_us";
  const char* OBUPS_MAX_THREAD_MEMBLOCK_NUM = "max_thread_memblock_num";
  const char* OBUPS_LOG_FETCH_OPTION = "log_fetch_option";
  const char* OBUPS_TRANS_PROC_TIME_WARN_US = "transaction_process_time_warning_us";
  const char* OBUPS_STORE_ROOT = "store_root";
  const char* OBUPS_RAID_REGEX = "raid_regex";
  const char* OBUPS_DIR_REGEX = "dir_regex";
  const char* OBUPS_BLOCKCACHE_SIZE_MB = "blockcache_size_mb";
  const char* OBUPS_BLOCKINDEX_CACHE_SIZE_MB = "blockindex_cache_size_mb";
  const char* OBUPS_ACTIVE_MEM_LIMIT_GB = "active_mem_limit_gb";
  const char* OBUPS_MINOR_NUM_LIMIT = "minor_num_limit";
  const char* OBUPS_FROZEN_MEM_LIMIT_GB = "frozen_mem_limit_gb";
  const char* OBUPS_SSTABLE_TIME_LIMIT_S = "sstable_time_limit_s";
  const char* OBUPS_SLAVE_SYNC_SSTABLE_NUM = "slave_sync_sstable_num";
  const char* OBUPS_SSTABLE_COMPRESSOR_NAME = "sstable_compressor_name";
  const char* OBUPS_SSTABLE_BLOCK_SIZE = "sstable_block_size";
  const char* OBUPS_MAJOR_FREEZE_DUTY_TIME = "major_freeze_duty_time";
  const char* OBUPS_MAJOR_FREEZE_DUTY_TIME_FROMAT = "%H:%M";
  const char* OBUPS_MIN_MAJOR_FREEZE_INTERVAL_S = "min_major_freeze_interval_s";
  const char* OBUPS_REPLAY_CHECKSUM_FLAG = "replay_checksum_flag";
  const char* OBUPS_ALLOW_WRITE_WITHOUT_TOKEN = "allow_write_without_token";
  const char *OBUPS_MAX_ROW_CELL_NUM = "max_row_cell_num";
  const char *OBUPS_TABLE_AVAILABLE_WARN_SIZE_GB = "table_available_warn_size_gb";
  const char *OBUPS_TABLE_AVAILABLE_ERROR_SIZE_GB = "table_available_error_size_gb";
  const char *OBUPS_WARM_UP_TIME_S= "warm_up_time_s";
  const char *OBUPS_KEEP_ALIVE_TIMEOUT_US = "keep_alive_timeout_us";
  const char *OBUPS_LEASE_TIMEOUT_IN_ADVANCE_US = "lease_timeout_in_advance_us";
  const char *OBUPS_SLAVE_TYPE = "slave_type";
  const char *OBUPS_USING_MEMTABLE_BLOOMFILTER = "using_memtable_bloomfilter";
  const char *OBUPS_SSTABLE_DIO_WRITING = "sstable_dio_writing";
  const char *OBUPS_USING_STATIC_CM_COLUMN_ID = "using_static_cm_column_id";
  const char *OBUPS_USING_HASH_INDEX = "using_hash_index";

  const char* OBUPS_RS_SECTION  = "root_server";

  const char* OBUPS_STANDALONE_SECTION = "standalone";
  const char* OBUPS_STANDALONE_SCHEMA = "test_schema";
  const char* OBUPS_STANDALONE_SCHEMA_VERSION = "test_schema_version";

  const char* DEFAULT_LOG_FETCH_OPTION = "-e \"ssh -oStrictHostKeyChecking=no\" -avz --inplace";

  const char* OBUPS_UPS_INNER_PORT = "ups_inner_port";
  const char* OBUPS_LOW_PRIV_NETWORK_LOWER_LIMIT = "low_priv_network_lower_limit";
  const char* OBUPS_LOW_PRIV_NETWORK_UPPER_LIMIT = "low_priv_network_upper_limit";
  const char* OBUPS_LOW_PRIV_ADJUST_FLAG = "low_priv_adjust_flag";
  const char* OBUPS_LOW_PRIV_CUR_PERCENT = "low_priv_cur_percent";
}

namespace oceanbase
{
  namespace updateserver
  {
    using namespace oceanbase::common;

    ObUpdateServerParam::ObUpdateServerParam()
    {
      memset(this, 0x00, sizeof(*this));
      memset(&major_freeze_duty_time_, -1, sizeof(major_freeze_duty_time_));
      //memset(ups_inst_master_ip_, 0x00, sizeof(ups_inst_master_ip_));
      memset(inst_master_rs_ip_, 0x00, sizeof(inst_master_rs_ip_));
    }

    ObUpdateServerParam::~ObUpdateServerParam()
    {
    }

    int ObUpdateServerParam::load_from_config()
    {
      int err = OB_SUCCESS;

      ups_port_ = TBSYS_CONFIG.getInt(OBUPS_UPS_SECTION, OBUPS_PORT, 0);
      if (ups_port_ <= 0)
      {
        TBSYS_LOG(ERROR, "update server port (%d) cannot <= 0.", ups_port_);
        err = OB_ERROR;
      }

      if (OB_SUCCESS == err)
      {
        err = load_string(dev_name_, OB_MAX_DEV_NAME_SIZE, OBUPS_UPS_SECTION, OBUPS_DEVNAME, true);
      }

      if (OB_SUCCESS == err)
      {
        err = load_string(app_name_, OB_MAX_APP_NAME_LENGTH,
            OBUPS_UPS_SECTION, OBUPS_APP_NAME, true);
      }

      if (OB_SUCCESS == err)
      {
        err = load_string(log_dir_path_, OB_MAX_FILE_NAME_LENGTH, OBUPS_UPS_SECTION,
            OBUPS_LOG_DIR_PATH, true);
      }

      if (OB_SUCCESS == err)
      {
        log_size_mb_ = TBSYS_CONFIG.getInt(OBUPS_UPS_SECTION, OBUPS_LOG_SIZE_MB,
            DEFAULT_LOG_SIZE_MB);
      }

      if (OB_SUCCESS == err)
      {
        err = load_string(rs_vip_, OB_MAX_IP_SIZE, OBUPS_RS_SECTION, OBUPS_VIP, true);
      }

      if (OB_SUCCESS == err)
      {
        rs_port_ = TBSYS_CONFIG.getInt(OBUPS_RS_SECTION, OBUPS_PORT, 0);
      }

      if (OB_SUCCESS == err)
      {
        fetch_schema_times_ = TBSYS_CONFIG.getInt(OBUPS_UPS_SECTION,
            OBUPS_FETCH_SCHEMA_TIMES, DEFAULT_FETCH_SCHEMA_TIMES);
      }

      if (OB_SUCCESS == err)
      {
        fetch_schema_timeout_us_ = TBSYS_CONFIG.getInt(OBUPS_UPS_SECTION,
            OBUPS_FETCH_SCHEMA_TIMEOUT_US, DEFAULT_FETCH_SCHEMA_TIMEOUT_US);
      }

      if (OB_SUCCESS == err)
      {
        resp_root_times_ = TBSYS_CONFIG.getInt(OBUPS_UPS_SECTION,
            OBUPS_RESP_ROOT_TIMES, DEFAULT_RESP_ROOT_TIMES);
      }

      if (OB_SUCCESS == err)
      {
        resp_root_timeout_us_ = TBSYS_CONFIG.getInt(OBUPS_UPS_SECTION,
            OBUPS_RESP_ROOT_TIMEOUT_US, DEFAULT_RESP_ROOT_TIMEOUT_US);
      }

      if (OB_SUCCESS == err)
      {
        state_check_period_us_ = TBSYS_CONFIG.getInt(OBUPS_UPS_SECTION,
            OBUPS_STATE_CHECK_PERIOD_US, DEFAULT_STATE_CHECK_PERIOD_US);
      }

     /* if (OB_SUCCESS == err)
      {
        err = load_string(ups_vip_, OB_MAX_IP_SIZE, OBUPS_UPS_SECTION,
            OBUPS_VIP, true);
      }*/

     /* if (OB_SUCCESS == err)
      {
        err = load_string(ups_inst_master_ip_, OB_MAX_IP_SIZE, OBUPS_UPS_SECTION,
            OBUPS_INST_MASTER_IP, false);
      }

      if (OB_SUCCESS == err)
      {
        ups_inst_master_port_ = TBSYS_CONFIG.getInt(OBUPS_UPS_SECTION, OBUPS_INST_MASTER_PORT, 0);
      }
      */
      if (OB_SUCCESS == err)
      {
        err = load_string(inst_master_rs_ip_, OB_MAX_IP_SIZE, OBUPS_UPS_SECTION, OBINST_MASTER_RS_IP, false);
      }

      if (OB_SUCCESS == err)
      {
        inst_master_rs_port_ = TBSYS_CONFIG.getInt(OBUPS_UPS_SECTION, OBINST_MASTER_RS_PORT, 0);
      }

      if (OB_SUCCESS == err)
      {
        err = load_string(lsync_ip_, OB_MAX_IP_SIZE, OBUPS_UPS_SECTION,
            OBUPS_LSYNC_IP, false);
      }

      if (OB_SUCCESS == err)
      {
        lsync_port_ = TBSYS_CONFIG.getInt(OBUPS_UPS_SECTION, OBUPS_LSYNC_PORT, 0);
      }

      if (OB_SUCCESS == err)
      {
        log_sync_type_ = TBSYS_CONFIG.getInt(OBUPS_UPS_SECTION,
            OBUPS_LOG_SYNC_TYPE, DEFAULT_LOG_SYNC_TYPE);
      }

      //if (OB_SUCCESS == err)
      //{
      //  pre_process_ = TBSYS_CONFIG.getInt(OBUPS_UPS_SECTION,
      //      OBUPS_PRE_PROCESS, DEFAULT_PRE_PROCESS);
      //}
      pre_process_ = 1;

      if (OB_SUCCESS == err)
      {
        log_sync_limit_kb_ = TBSYS_CONFIG.getInt(OBUPS_UPS_SECTION,
            OBUPS_LOG_SYNC_LIMIT_KB, DEFAULT_LOG_SYNC_LIMIT_KB);
      }

      if (OB_SUCCESS == err)
      {
        log_sync_retry_times_ = TBSYS_CONFIG.getInt(OBUPS_UPS_SECTION,
            OBUPS_LOG_SYNC_RETRY_TIMES, DEFAULT_LOG_SYNC_RETRY_TIMES);
      }

      if (OB_SUCCESS == err)
      {
        total_memory_limit_ = TBSYS_CONFIG.getInt(OBUPS_UPS_SECTION,
            OBUPS_TOTAL_MEMORY_LIMIT, DEFAULT_TOTAL_MEMORY_LIMIT);
        total_memory_limit_ *= GB_UNIT;
      }

      if (OB_SUCCESS == err)
      {
        table_memory_limit_ = TBSYS_CONFIG.getInt(OBUPS_UPS_SECTION,
            OBUPS_TABLE_MEMORY_LIMIT, DEFAULT_TABLE_MEMORY_LIMIT);
        table_memory_limit_ *= GB_UNIT;
      }

      if (OB_SUCCESS == err)
      {
        read_thread_count_ = TBSYS_CONFIG.getInt(OBUPS_UPS_SECTION, OBUPS_READ_THREAD_COUNT,
            DEFAULT_READ_THREAD_COUNT);
        if (read_thread_count_ <= 0)
        {
          TBSYS_LOG(ERROR, "read thread count (%ld) cannot <= 0.", read_thread_count_);
          err = OB_ERROR;
        }
      }

      if (OB_SUCCESS == err)
      {
        store_thread_count_ = TBSYS_CONFIG.getInt(OBUPS_UPS_SECTION, OBUPS_STORE_THREAD_COUNT,
            DEFAULT_STORE_THREAD_COUNT);
        if (store_thread_count_ <= 0)
        {
          TBSYS_LOG(ERROR, "store thread count (%ld) cannot <= 0.", store_thread_count_);
          err = OB_ERROR;
        }
      }

      if (OB_SUCCESS == err)
      {
        read_task_queue_size_ = TBSYS_CONFIG.getInt(OBUPS_UPS_SECTION, OBUPS_READ_TASK_QUEUE_SIZE,
            DEFAULT_TASK_READ_QUEUE_SIZE);
      }

      if (OB_SUCCESS == err)
      {
        write_task_queue_size_ = TBSYS_CONFIG.getInt(OBUPS_UPS_SECTION, OBUPS_WRITE_TASK_QUEUE_SIZE,
            DEFAULT_TASK_WRITE_QUEUE_SIZE);
      }

      if (OB_SUCCESS == err)
      {
        write_thread_batch_num_ = TBSYS_CONFIG.getInt(OBUPS_UPS_SECTION,
            OBUPS_WRITE_GROUP_COMMIT_NUM, DEFAULT_WRITE_GROUP_COMMIT_NUM);
      }

      if (OB_SUCCESS == err)
      {
        log_task_queue_size_ = TBSYS_CONFIG.getInt(OBUPS_UPS_SECTION, OBUPS_LOG_TASK_QUEUE_SIZE,
            DEFAULT_TASK_LOG_QUEUE_SIZE);
      }

      if (OB_SUCCESS == err)
      {
        lease_task_queue_size_ = TBSYS_CONFIG.getInt(OBUPS_UPS_SECTION, OBUPS_LEASE_TASK_QUEUE_SIZE,
            DEFAULT_TASK_LEASE_QUEUE_SIZE);
      }

      if (OB_SUCCESS == err)
      {
        store_thread_queue_size_ = TBSYS_CONFIG.getInt(OBUPS_UPS_SECTION, OBUPS_STORE_THREAD_QUEUE_SIZE,
            DEFAULT_STORE_THREAD_QUEUE_SIZE);
      }

      if (OB_SUCCESS == err)
      {
        register_times_ = TBSYS_CONFIG.getInt(OBUPS_UPS_SECTION, OBUPS_REGISTER_TIMES,
            DEFAULT_REGISTER_TIMES);
      }

      if (OB_SUCCESS == err)
      {
        register_timeout_us_ = TBSYS_CONFIG.getInt(OBUPS_UPS_SECTION, OBUPS_REGISTER_TIMEOUT_US,
            DEFAULT_REGISTER_TIMEOUT_US);
      }

      if (OB_SUCCESS == err)
      {
        replay_wait_time_us_ = TBSYS_CONFIG.getInt(OBUPS_UPS_SECTION, OBUPS_REPLAY_WAIT_TIME_US,
            DEFAULT_REPLAY_WAIT_TIME_US);
      }

      if (OB_SUCCESS == err)
      {
        fetch_log_wait_time_us_ = TBSYS_CONFIG.getInt(OBUPS_UPS_SECTION, OBUPS_FETCH_LOG_WAIT_TIME_US,
            DEFAULT_FETCH_LOG_WAIT_TIME_US);
      }

      if (OB_SUCCESS == err)
      {
        log_sync_delay_warn_time_threshold_us_ = TBSYS_CONFIG.getInt(OBUPS_UPS_SECTION,
                                                                     OBUPS_LOG_SYNC_DELAY_WARN_TIME_THRESHOLD_US,
                                                                     DEFAULT_LOG_SYNC_DELAY_WARN_TIME_THRESHOLD_US);
      }
      
      if (OB_SUCCESS == err)
      {
        log_sync_delay_warn_report_interval_us_ = TBSYS_CONFIG.getInt(OBUPS_UPS_SECTION,
                                                                     OBUPS_LOG_SYNC_DELAY_WARN_REPORT_INTERVAL_US,
                                                                     DEFAULT_LOG_SYNC_DELAY_WARN_REPORT_INTERVAL_US);
      }

      if (OB_SUCCESS == err)
      {
        max_n_lagged_log_allowed_ = TBSYS_CONFIG.getInt(OBUPS_UPS_SECTION,
                                                                     OBUPS_MAX_N_LAGGED_LOG_ALLOWED,
                                                                     DEFAULT_MAX_N_LAGGED_LOG_ALLOWED);
      }

      //if (OB_SUCCESS == err)
      //{
      //  lease_interval_us_ = TBSYS_CONFIG.getInt(OBUPS_UPS_SECTION, OBUPS_LEASE_INTERVAL_US,
      //      DEFAULT_LEASE_INTERVAL_US);
      //}

      //if (OB_SUCCESS == err)
      //{
      //  lease_reserved_time_us_ = TBSYS_CONFIG.getInt(OBUPS_UPS_SECTION, OBUPS_LEASE_RESERVED_TIME_US,
      //      DEFAULT_LEASE_RESERVED_TIME_US);
      //}

      if (OB_SUCCESS == err)
      {
        log_sync_timeout_us_ = TBSYS_CONFIG.getInt(OBUPS_UPS_SECTION, OBUPS_LOG_SYNC_TIMEOUT_US,
            DEFAULT_LOG_SYNC_TIMEOUT_US);
      }

      if (OB_SUCCESS == err)
      {
        packet_max_timewait_ = TBSYS_CONFIG.getInt(OBUPS_UPS_SECTION, OBUPS_PACKET_MAX_TIMEWAIT,
            DEFAULT_PACKET_MAX_TIMEWAIT);
      }

      if (OB_SUCCESS == err)
      {
        lsync_fetch_timeout_us_ = TBSYS_CONFIG.getInt(OBUPS_UPS_SECTION, OBUPS_LSYNC_FETCH_TIMEOUT_US,
            DEFAULT_LSYNC_FETCH_TIMEOUT_US);
      }

      if (OB_SUCCESS == err)
      {
        refresh_lsync_addr_interval_us_ = TBSYS_CONFIG.getInt(OBUPS_UPS_SECTION, OBUPS_REFRESH_LSYNC_ADDR_INTERVAL_US,
            DEFAULT_REFRESH_LSYNC_ADDR_INTERVAL_US);
      }

     // if (OB_SUCCESS == err)
     // {
     //   lease_on_ = TBSYS_CONFIG.getInt(OBUPS_UPS_SECTION, OBUPS_LEASE_ON,
     //       DEFAULT_LEASE_ON);
     // }

      if (OB_SUCCESS == err)
      {
        slave_fail_wait_lease_on_ = TBSYS_CONFIG.getInt(OBUPS_UPS_SECTION,
            OBUPS_SLAVE_FAIL_WAIT_LEASE_ON, DEFAULT_SLAVE_FAIL_WAIT_LEASE_ON);
      }

      if (OB_SUCCESS == err)
      {
        max_thread_memblock_num_ = TBSYS_CONFIG.getInt(OBUPS_UPS_SECTION, OBUPS_MAX_THREAD_MEMBLOCK_NUM,
            DEFAULT_MAX_THREAD_MEMBLOCK_NUM);
        thread_mempool_set_max_free_num(static_cast<int32_t>(max_thread_memblock_num_));
      }

      if (OB_SUCCESS == err)
      {
        trans_proc_time_warn_us_ = TBSYS_CONFIG.getInt(OBUPS_UPS_SECTION, OBUPS_TRANS_PROC_TIME_WARN_US,
            DEFAULT_TRANS_PROC_TIME_WARN_US);
      }

      if (OB_SUCCESS == err)
      {
        err = load_string(store_root_, OB_MAX_FILE_NAME_LENGTH, OBUPS_UPS_SECTION, OBUPS_STORE_ROOT, true);
      }

      if (OB_SUCCESS == err)
      {
        err = load_string(raid_regex_, OB_MAX_FILE_NAME_LENGTH, OBUPS_UPS_SECTION, OBUPS_RAID_REGEX, true);
      }

      if (OB_SUCCESS == err)
      {
        err = load_string(dir_regex_, OB_MAX_FILE_NAME_LENGTH, OBUPS_UPS_SECTION, OBUPS_DIR_REGEX, true);
      }

      if (OB_SUCCESS == err)
      {
        err = load_string(sstable_compressor_name_, OB_MAX_FILE_NAME_LENGTH, OBUPS_UPS_SECTION, OBUPS_SSTABLE_COMPRESSOR_NAME, true);
        if (OB_SUCCESS == err)
        {
          ObCompressor *compressor = create_compressor(sstable_compressor_name_);
          if (NULL == compressor)
          {
            TBSYS_LOG(ERROR, "cannot load compressor library name=[%s]", sstable_compressor_name_);
            err = OB_ERROR;
          }
          else
          {
            destroy_compressor(compressor);
          }
        }
      }

      if (OB_SUCCESS == err)
      {
        blockcache_size_mb_ = TBSYS_CONFIG.getInt(OBUPS_UPS_SECTION, OBUPS_BLOCKCACHE_SIZE_MB,
            DEFAULT_BLOCKCACHE_SIZE_MB);
      }

      if (OB_SUCCESS == err)
      {
        blockindex_cache_size_mb_ = TBSYS_CONFIG.getInt(OBUPS_UPS_SECTION, OBUPS_BLOCKINDEX_CACHE_SIZE_MB,
            DEFAULT_BLOCKINDEX_CACHE_SIZE_MB);
      }

      if (OB_SUCCESS == err)
      {
        active_mem_limit_gb_ = TBSYS_CONFIG.getInt(OBUPS_UPS_SECTION, OBUPS_ACTIVE_MEM_LIMIT_GB,
            DEFAULT_ACTIVE_MEM_LIMIT_GB);
      }

      if (OB_SUCCESS == err)
      {
        minor_num_limit_ = TBSYS_CONFIG.getInt(OBUPS_UPS_SECTION, OBUPS_MINOR_NUM_LIMIT,
            DEFAULT_MINOR_NUM_LIMIT);
      }

      if (OB_SUCCESS == err)
      {
        TBSYS_LOG(INFO, "will not parse %s", OBUPS_FROZEN_MEM_LIMIT_GB);
      }

      if (OB_SUCCESS == err)
      {
        sstable_time_limit_s_ = TBSYS_CONFIG.getInt(OBUPS_UPS_SECTION, OBUPS_SSTABLE_TIME_LIMIT_S,
            DEFAULT_SSTABLE_TIME_LIMIT_S);
      }

      if (OB_SUCCESS == err)
      {
        min_major_freeze_interval_s_ = TBSYS_CONFIG.getInt(OBUPS_UPS_SECTION, OBUPS_MIN_MAJOR_FREEZE_INTERVAL_S,
            DEFAULT_MIN_MAJOR_FREEZE_INTERVAL_S);
      }

      if (OB_SUCCESS == err)
      {
        replay_checksum_flag_ = TBSYS_CONFIG.getInt(OBUPS_UPS_SECTION, OBUPS_REPLAY_CHECKSUM_FLAG,
            DEFAULT_REPLAY_CHECKSUM_FLAG);
      }
      
      if (OB_SUCCESS == err)
      {
        allow_write_without_token_ = TBSYS_CONFIG.getInt(OBUPS_UPS_SECTION, OBUPS_ALLOW_WRITE_WITHOUT_TOKEN,
            DEFAULT_ALLOW_WRITE_WITHOUT_TOKEN);
      }

      if (OB_SUCCESS == err)
      {
        slave_sync_sstable_num_ = TBSYS_CONFIG.getInt(OBUPS_UPS_SECTION, OBUPS_SLAVE_SYNC_SSTABLE_NUM,
            DEFAULT_SLAVE_SYNC_SSTABLE_NUM);
      }

      if (OB_SUCCESS == err)
      {
        sstable_block_size_ = TBSYS_CONFIG.getInt(OBUPS_UPS_SECTION, OBUPS_SSTABLE_BLOCK_SIZE,
            DEFAULT_SSTABLE_BLOCK_SIZE);
      }

      if (OB_SUCCESS == err)
      {
        const char *str = TBSYS_CONFIG.getString(OBUPS_UPS_SECTION, OBUPS_MAJOR_FREEZE_DUTY_TIME);
        if (NULL != str)
        {
          strptime(str, OBUPS_MAJOR_FREEZE_DUTY_TIME_FROMAT, &major_freeze_duty_time_);
        }
      }

      if (OB_SUCCESS == err)
      {
        max_row_cell_num_ = TBSYS_CONFIG.getInt(OBUPS_UPS_SECTION, OBUPS_MAX_ROW_CELL_NUM,
            DEFAULT_MAX_ROW_CELL_NUM);
      }

      if (OB_SUCCESS == err)
      {
        table_available_warn_size_ = TBSYS_CONFIG.getInt(OBUPS_UPS_SECTION, OBUPS_TABLE_AVAILABLE_WARN_SIZE_GB,
            DEFAULT_TABLE_AVAILABLE_WARN_SIZE_GB);
        table_available_warn_size_ *= GB_UNIT;
      }

      if (OB_SUCCESS == err)
      {
        table_available_error_size_ = TBSYS_CONFIG.getInt(OBUPS_UPS_SECTION, OBUPS_TABLE_AVAILABLE_ERROR_SIZE_GB,
            DEFAULT_TABLE_AVAILABLE_ERROR_SIZE_GB);
        table_available_error_size_ *= GB_UNIT;
      }

      if (OB_SUCCESS == err)
      {
        ups_keep_alive_timeout_us_ = TBSYS_CONFIG.getInt(OBUPS_UPS_SECTION, OBUPS_KEEP_ALIVE_TIMEOUT_US, DEFAULT_KEEP_ALIVE_TIMEOUT_US);
      }

      if (OB_SUCCESS == err)
      {
        lease_timeout_in_advance_ = TBSYS_CONFIG.getInt(OBUPS_UPS_SECTION, OBUPS_LEASE_TIMEOUT_IN_ADVANCE_US, DEFAULT_LEASE_TIMEOUT_IN_ADVANCE_US);
      }

      if (OB_SUCCESS == err)
      {
        slave_type_ = TBSYS_CONFIG.getInt(OBUPS_UPS_SECTION, OBUPS_SLAVE_TYPE, 1);
      }

      if (OB_SUCCESS == err)
      {
        warm_up_conf_.warm_up_time_s = TBSYS_CONFIG.getInt(OBUPS_UPS_SECTION, OBUPS_WARM_UP_TIME_S,
            CacheWarmUpConf::DEFAULT_WARM_UP_TIME_S);
      }

      if (OB_SUCCESS == err)
      {
        err = load_string(fetch_opt_, sizeof(fetch_opt_),
            OBUPS_UPS_SECTION, OBUPS_LOG_FETCH_OPTION, false);
        if (OB_SUCCESS == err)
        {
          if ('\0' == fetch_opt_[0])
          {
            strcpy(fetch_opt_, DEFAULT_LOG_FETCH_OPTION);
          }
        }
      }

      if (OB_SUCCESS == err)
      {
        ups_inner_port_ = TBSYS_CONFIG.getInt(OBUPS_UPS_SECTION, OBUPS_UPS_INNER_PORT, 0);
        if (ups_inner_port_ <= 0)
        {
          TBSYS_LOG(ERROR, "update server inner port (%ld) cannot <= 0.", ups_inner_port_);
          err = OB_ERROR;
        }
      }

      if (OB_SUCCESS == err)
      {
        low_priv_network_lower_limit_ = TBSYS_CONFIG.getInt(OBUPS_UPS_SECTION, OBUPS_LOW_PRIV_NETWORK_LOWER_LIMIT,
            DEFAULT_LOW_PRIV_NETWORK_LOWER_LIMIT);
        low_priv_network_upper_limit_ = TBSYS_CONFIG.getInt(OBUPS_UPS_SECTION, OBUPS_LOW_PRIV_NETWORK_UPPER_LIMIT,
            DEFAULT_LOW_PRIV_NETWORK_UPPER_LIMIT);
        low_priv_cur_percent_ = TBSYS_CONFIG.getInt(OBUPS_UPS_SECTION, OBUPS_LOW_PRIV_CUR_PERCENT,
            DEFAULT_LOW_PRIV_CUR_PERCENT);
        low_priv_adjust_flag_ = TBSYS_CONFIG.getInt(OBUPS_UPS_SECTION, OBUPS_LOW_PRIV_ADJUST_FLAG,
            DEFAULT_LOW_PRIV_ADJUST_FLAG);
      }

      if (OB_SUCCESS == err)
      {
        using_memtable_bloomfilter_ = TBSYS_CONFIG.getInt(OBUPS_UPS_SECTION, OBUPS_USING_MEMTABLE_BLOOMFILTER,
            DEFAULT_USING_MEMTABLE_BLOOMFILTER);
      }

      if (OB_SUCCESS == err)
      {
        sstable_dio_writing_ = TBSYS_CONFIG.getInt(OBUPS_UPS_SECTION, OBUPS_SSTABLE_DIO_WRITING,
            DEFAULT_SSTABLE_DIO_WRITING);
      }

      if (OB_SUCCESS == err)
      {
        using_static_cm_column_id_ = TBSYS_CONFIG.getInt(OBUPS_UPS_SECTION, OBUPS_USING_STATIC_CM_COLUMN_ID,
            DEFAULT_USING_STATIC_CM_COLUMN_ID);
        g_conf.using_static_cm_column_id = (0 != using_static_cm_column_id_);
        TBSYS_LOG(INFO, "g_conf.using_static_cm_column_id set to %s", STR_BOOL(g_conf.using_static_cm_column_id));
      }

      if (OB_SUCCESS == err)
      {
        using_hash_index_ = TBSYS_CONFIG.getInt(OBUPS_UPS_SECTION, OBUPS_USING_HASH_INDEX,
            DEFAULT_USING_HASH_INDEX);
        g_conf.using_hash_index = (0 != using_hash_index_);
        TBSYS_LOG(INFO, "g_conf.using_hash_index set to %s", STR_BOOL(g_conf.using_hash_index));
      }

      if (OB_SUCCESS == err)
      {
        int64_t mem_total = (blockcache_size_mb_ + blockindex_cache_size_mb_) * MB_UNIT + table_memory_limit_;
        if (mem_total >= total_memory_limit_)
        {
          TBSYS_LOG(WARN, "blockcache_size + blockindex_cache + table_memory_limit %ld cannot larger than total_memory_limit=%ld",
                    mem_total, total_memory_limit_);
          err = OB_INVALID_ARGUMENT;
        }
      }

      if (OB_SUCCESS == err)
      {
        if (MIN_ACTIVE_MEM_LIMIT > (active_mem_limit_gb_ * GB_UNIT))
        {
          TBSYS_LOG(WARN, "active_mem_limit=%ld cannot less than %0.2f",
                    active_mem_limit_gb_, MIN_ACTIVE_MEM_LIMIT * 1.0 / GB_UNIT);
          err = OB_INVALID_ARGUMENT;
        }
        if (0 != using_hash_index_
            && (active_mem_limit_gb_ * GB_UNIT) >= table_memory_limit_)
        {
          TBSYS_LOG(WARN, "active_mem_limit=%ld cannot larger than table_memory_limit=%ld",
                    active_mem_limit_gb_, table_memory_limit_ / GB_UNIT);
          err = OB_INVALID_ARGUMENT;
        }
      }

      if (OB_SUCCESS == err)
      {
        if (!warm_up_conf_.check())
        {
          err = OB_INVALID_ARGUMENT;
        }
      }

      return err;
    }

    int ObUpdateServerParam::reload_from_config(const ObString& conf_file)
    {
      int err = OB_SUCCESS;

      char config_file_str[OB_MAX_FILE_NAME_LENGTH];
      int64_t length = conf_file.length();
      strncpy(config_file_str, conf_file.ptr(), length);
      config_file_str[length] = '\0';

      tbsys::CConfig config;

      if(config.load(config_file_str))
      {
        fprintf(stderr, "load file %s error\n", config_file_str);
        err = OB_ERROR;
      }

      if (OB_SUCCESS == err)
      {
        int old_value = using_hash_index_;
        using_hash_index_ = config.getInt(OBUPS_UPS_SECTION, OBUPS_USING_HASH_INDEX,
            DEFAULT_USING_HASH_INDEX);
        TBSYS_LOG(INFO, "uing_hash_index switch from %d to %d", old_value, using_hash_index_);
        g_conf.using_hash_index = (0 != using_hash_index_);
        TBSYS_LOG(INFO, "g_conf.using_hash_index set to %s", STR_BOOL(g_conf.using_hash_index));
      }

      if (OB_SUCCESS == err)
      {
        int64_t tmp_active_mem_limit = config.getInt(OBUPS_UPS_SECTION, OBUPS_ACTIVE_MEM_LIMIT_GB, DEFAULT_ACTIVE_MEM_LIMIT_GB);
        int64_t tmp_total_memory_limit = config.getInt(OBUPS_UPS_SECTION, OBUPS_TOTAL_MEMORY_LIMIT, DEFAULT_TOTAL_MEMORY_LIMIT);
        tmp_total_memory_limit *= GB_UNIT;
        int64_t tmp_table_memory_limit = config.getInt(OBUPS_UPS_SECTION, OBUPS_TABLE_MEMORY_LIMIT, DEFAULT_TABLE_MEMORY_LIMIT);
        tmp_table_memory_limit *= GB_UNIT;

        int64_t mem_total = (blockcache_size_mb_ + blockindex_cache_size_mb_) * MB_UNIT + tmp_table_memory_limit;

        if (mem_total >= tmp_total_memory_limit)
        {
          TBSYS_LOG(WARN, "blockcache_size + blockindex_cache + table_memory_limit %ld cannot larger than total_memory_limit=%ld",
                    mem_total, tmp_total_memory_limit);
          err = OB_INVALID_ARGUMENT;
        }

        if (0 != using_hash_index_
            && MIN_ACTIVE_MEM_LIMIT > (tmp_active_mem_limit * GB_UNIT))
        {
          TBSYS_LOG(WARN, "active_mem_limit=%ld cannot less than %0.2f",
                    tmp_active_mem_limit, MIN_ACTIVE_MEM_LIMIT * 1.0 / GB_UNIT);
          err = OB_INVALID_ARGUMENT;
        }

        if ((tmp_active_mem_limit * GB_UNIT) >= tmp_table_memory_limit)
        {
          TBSYS_LOG(WARN, "active_mem_limit=%ld cannot larger than table_memory_limit=%ld",
                    tmp_active_mem_limit, tmp_table_memory_limit / GB_UNIT);
          err = OB_INVALID_ARGUMENT;
        }

        TBSYS_LOG(INFO, "will not parse %s", OBUPS_FROZEN_MEM_LIMIT_GB);
        if (OB_SUCCESS == err)
        {
          TBSYS_LOG(INFO, "active_mem_limit switch from %ld to %ld, "
                    "total_memory_limit switch from %ld to %ld, "
                    "table_memory_limit switch from %ld to %ld",
                    active_mem_limit_gb_, tmp_active_mem_limit,
                    total_memory_limit_ / GB_UNIT, tmp_total_memory_limit / GB_UNIT,
                    table_memory_limit_ / GB_UNIT, tmp_table_memory_limit / GB_UNIT);
          active_mem_limit_gb_ = tmp_active_mem_limit;
          total_memory_limit_ = tmp_total_memory_limit;
          table_memory_limit_ = tmp_table_memory_limit;

          ob_set_memory_size_limit(total_memory_limit_);
          ObUpdateServerMain *ups_main = ObUpdateServerMain::get_instance();
          if (NULL == ups_main)
          {
            TBSYS_LOG(WARN, "get updateserver main fail");
          }
          else
          {
            MemTableAttr memtable_attr;
            if (OB_SUCCESS == ups_main->get_update_server().get_table_mgr().get_memtable_attr(memtable_attr))
            {
              memtable_attr.total_memlimit = table_memory_limit_;
              ups_main->get_update_server().get_table_mgr().set_memtable_attr(memtable_attr);
            }
          }
        }
      }

      if (OB_SUCCESS == err)
      {
        int64_t old_value = minor_num_limit_;
        minor_num_limit_ = config.getInt(OBUPS_UPS_SECTION, OBUPS_MINOR_NUM_LIMIT,
            DEFAULT_MINOR_NUM_LIMIT);
        TBSYS_LOG(INFO, "minor_num_limit switch from %ld to %ld", old_value, minor_num_limit_);
      }

      if (OB_SUCCESS == err)
      {
        int64_t old_value = sstable_time_limit_s_;
        sstable_time_limit_s_ = config.getInt(OBUPS_UPS_SECTION, OBUPS_SSTABLE_TIME_LIMIT_S,
            DEFAULT_SSTABLE_TIME_LIMIT_S);
        TBSYS_LOG(INFO, "sstable_time_limit switch from %ld to %ld", old_value, sstable_time_limit_s_);
      }

      if (OB_SUCCESS == err)
      {
        int64_t old_value = min_major_freeze_interval_s_;
        min_major_freeze_interval_s_ = config.getInt(OBUPS_UPS_SECTION, OBUPS_MIN_MAJOR_FREEZE_INTERVAL_S,
            DEFAULT_MIN_MAJOR_FREEZE_INTERVAL_S);
        TBSYS_LOG(INFO, "min_major_freeze_interval switch from %ld to %ld", old_value, min_major_freeze_interval_s_);
      }

      if (OB_SUCCESS == err)
      {
        int64_t old_value = trans_proc_time_warn_us_;
        trans_proc_time_warn_us_ = config.getInt(OBUPS_UPS_SECTION, OBUPS_TRANS_PROC_TIME_WARN_US,
            DEFAULT_TRANS_PROC_TIME_WARN_US);
        TBSYS_LOG(INFO, "trans_proc_time_warn_us switch from %ld to %ld", old_value, trans_proc_time_warn_us_);
      }

      if (OB_SUCCESS == err)
      {
        int old_value = replay_checksum_flag_;
        replay_checksum_flag_ = config.getInt(OBUPS_UPS_SECTION, OBUPS_REPLAY_CHECKSUM_FLAG,
            DEFAULT_REPLAY_CHECKSUM_FLAG);
        ObUpdateServerMain *ups_main = ObUpdateServerMain::get_instance();
        if (NULL == ups_main)
        {
          TBSYS_LOG(WARN, "get updateserver main fail");
        }
        else
        {
          ups_main->get_update_server().get_table_mgr().set_replay_checksum_flag(replay_checksum_flag_);
        }
        TBSYS_LOG(INFO, "replay_checksum_flag switch from %d to %d", old_value, replay_checksum_flag_);
      }

      if (OB_SUCCESS == err)
      {
        err = load_string(sstable_compressor_name_, OB_MAX_FILE_NAME_LENGTH, OBUPS_UPS_SECTION,
                          OBUPS_SSTABLE_COMPRESSOR_NAME, true, &config);
        if (OB_SUCCESS == err)
        {
          ObCompressor *compressor = create_compressor(sstable_compressor_name_);
          if (NULL == compressor)
          {
            TBSYS_LOG(ERROR, "cannot load compressor library name=[%s]", sstable_compressor_name_);
            err = OB_ERROR;
          }
          else
          {
            TBSYS_LOG(INFO, "sstable_compressor_name switch to [%s]", sstable_compressor_name_);
            destroy_compressor(compressor);
          }
        }
      }

      if (OB_SUCCESS == err)
      {
        int64_t old_value = sstable_block_size_;
        sstable_block_size_ = config.getInt(OBUPS_UPS_SECTION, OBUPS_SSTABLE_BLOCK_SIZE,
            DEFAULT_SSTABLE_BLOCK_SIZE);
        TBSYS_LOG(INFO, "sstable_block_size switch from %ld to %ld", old_value, sstable_block_size_);
      }

      if (OB_SUCCESS == err)
      {
        int old_value_hour = major_freeze_duty_time_.tm_hour;
        int old_value_min = major_freeze_duty_time_.tm_min;
        const char *str = config.getString(OBUPS_UPS_SECTION, OBUPS_MAJOR_FREEZE_DUTY_TIME);
        if (NULL != str)
        {
          if (NULL == strptime(str, OBUPS_MAJOR_FREEZE_DUTY_TIME_FROMAT, &major_freeze_duty_time_))
          {
            memset(&major_freeze_duty_time_, -1, sizeof(major_freeze_duty_time_));
          }
        }
        TBSYS_LOG(INFO, "major_freeze_duty_time switch from [%d:%d] to [%d:%d]",
                  old_value_hour, old_value_min, major_freeze_duty_time_.tm_hour, major_freeze_duty_time_.tm_min);
      }

      if (OB_SUCCESS == err)
      {
        int64_t old_value = max_row_cell_num_;
        max_row_cell_num_ = config.getInt(OBUPS_UPS_SECTION, OBUPS_MAX_ROW_CELL_NUM,
            DEFAULT_MAX_ROW_CELL_NUM);
        TBSYS_LOG(INFO, "max_row_cell_num switch from %ld to %ld", old_value, max_row_cell_num_);
      }

      if (OB_SUCCESS == err)
      {
        int64_t tmp_table_available_warn_size_gb = config.getInt(OBUPS_UPS_SECTION, OBUPS_TABLE_AVAILABLE_WARN_SIZE_GB,
            DEFAULT_TABLE_AVAILABLE_WARN_SIZE_GB);
        tmp_table_available_warn_size_gb *= GB_UNIT;
        TBSYS_LOG(INFO, "table_available_warn_size switch from %ld to %ld",
                  table_available_warn_size_ / GB_UNIT, tmp_table_available_warn_size_gb / GB_UNIT);
        table_available_warn_size_ = tmp_table_available_warn_size_gb;
      }

      if (OB_SUCCESS == err)
      {
        int64_t tmp_table_available_error_size_gb = config.getInt(OBUPS_UPS_SECTION, OBUPS_TABLE_AVAILABLE_ERROR_SIZE_GB,
            DEFAULT_TABLE_AVAILABLE_ERROR_SIZE_GB);
        tmp_table_available_error_size_gb *= GB_UNIT;
        TBSYS_LOG(INFO, "table_available_error_size switch from %ld to %ld",
                  table_available_error_size_ / GB_UNIT, tmp_table_available_error_size_gb / GB_UNIT);
        table_available_error_size_ = tmp_table_available_error_size_gb;
      }

      CacheWarmUpConf tmp_warm_up_conf;
      if (OB_SUCCESS == err)
      {
        tmp_warm_up_conf.warm_up_time_s = config.getInt(OBUPS_UPS_SECTION, OBUPS_WARM_UP_TIME_S,
            CacheWarmUpConf::DEFAULT_WARM_UP_TIME_S);
      }

      if (OB_SUCCESS == err)
      {
        char tmp_lsync_ip[OB_MAX_IP_SIZE];
        err = load_string(tmp_lsync_ip, OB_MAX_IP_SIZE, OBUPS_UPS_SECTION,
            OBUPS_LSYNC_IP, false, &config);
        TBSYS_LOG(INFO, "lsync server ip addr switch from [%s] to [%s]", lsync_ip_, tmp_lsync_ip);
        snprintf(lsync_ip_, OB_MAX_IP_SIZE, "%s", tmp_lsync_ip);
      }

      if (OB_SUCCESS == err)
      {
        int tmp_lsync_port = config.getInt(OBUPS_UPS_SECTION, OBUPS_LSYNC_PORT, 0);
        TBSYS_LOG(INFO, "lsync server port addr switch from [%d] to [%d]", lsync_port_, tmp_lsync_port);
        lsync_port_ = tmp_lsync_port;
      }

      ObUpdateServerMain *ups_main = ObUpdateServerMain::get_instance();
      if (NULL == ups_main)
      {
        TBSYS_LOG(WARN, "get updateserver main fail");
      }
      else
      {
        ups_main->get_update_server().set_lsync_server(lsync_ip_, lsync_port_);
      }

      if (OB_SUCCESS == err)
      {
        if (!tmp_warm_up_conf.check())
        {
          err = OB_INVALID_ARGUMENT;
        }
        if (OB_SUCCESS == err)
        {
          TBSYS_LOG(INFO, "warm_up_time_s switch from %ld to %ld",
                    warm_up_conf_.warm_up_time_s, tmp_warm_up_conf.warm_up_time_s);
          TBSYS_LOG(INFO, "warm_up_step_interval_us switch from %ld to %ld",
                    warm_up_conf_.warm_up_step_interval_us, tmp_warm_up_conf.warm_up_step_interval_us);
          warm_up_conf_ = tmp_warm_up_conf;
        }
      }

      if (OB_SUCCESS == err)
      {
        int64_t ori_lower_limit = low_priv_network_lower_limit_;
        int64_t ori_upper_limit = low_priv_network_upper_limit_;
        int64_t ori_adjust_flag = low_priv_adjust_flag_;
        int64_t ori_cur_percent = low_priv_cur_percent_;

        low_priv_network_lower_limit_ = config.getInt(OBUPS_UPS_SECTION, OBUPS_LOW_PRIV_NETWORK_LOWER_LIMIT,
            static_cast<int32_t>(ori_lower_limit));
        low_priv_network_upper_limit_ = config.getInt(OBUPS_UPS_SECTION, OBUPS_LOW_PRIV_NETWORK_UPPER_LIMIT,
            static_cast<int32_t>(ori_upper_limit));
        low_priv_adjust_flag_ = config.getInt(OBUPS_UPS_SECTION, OBUPS_LOW_PRIV_ADJUST_FLAG,
            static_cast<int32_t>(ori_adjust_flag));
        low_priv_cur_percent_ = config.getInt(OBUPS_UPS_SECTION, OBUPS_LOW_PRIV_CUR_PERCENT,
            static_cast<int32_t>(ori_cur_percent));

        TBSYS_LOG(INFO, "low_priv_network_lower_limit switch from [%ld] to [%ld]",
            ori_lower_limit, low_priv_network_lower_limit_);
        TBSYS_LOG(INFO, "low_priv_network_upper_limit switch from [%ld] to [%ld]",
            ori_upper_limit, low_priv_network_upper_limit_);
        TBSYS_LOG(INFO, "low_priv_adjust_flag switch from [%ld] to [%ld]",
            ori_adjust_flag, low_priv_adjust_flag_);
        TBSYS_LOG(INFO, "low_priv_cur_percent switch from [%ld] to [%ld]",
            ori_cur_percent, low_priv_cur_percent_);
      }

      if (OB_SUCCESS == err)
      {
        int old_value = using_memtable_bloomfilter_;
        using_memtable_bloomfilter_ = config.getInt(OBUPS_UPS_SECTION, OBUPS_USING_MEMTABLE_BLOOMFILTER,
            DEFAULT_USING_MEMTABLE_BLOOMFILTER);
        TBSYS_LOG(INFO, "using_memtable_bloomfilter switch from %d to %d", old_value, using_memtable_bloomfilter_);
      }

      if (OB_SUCCESS == err)
      {
        int old_value = sstable_dio_writing_;
        sstable_dio_writing_ = config.getInt(OBUPS_UPS_SECTION, OBUPS_SSTABLE_DIO_WRITING,
            DEFAULT_SSTABLE_DIO_WRITING);
        TBSYS_LOG(INFO, "sstable_dio_writing switch from %d to %d", old_value, sstable_dio_writing_);
      }

      if (OB_SUCCESS == err)
      {
        int old_value = using_static_cm_column_id_;
        using_static_cm_column_id_ = config.getInt(OBUPS_UPS_SECTION, OBUPS_USING_STATIC_CM_COLUMN_ID,
            DEFAULT_USING_STATIC_CM_COLUMN_ID);
        TBSYS_LOG(INFO, "uing_static_cm_column_id switch from %d to %d", old_value, using_static_cm_column_id_);
        g_conf.using_static_cm_column_id = (0 != using_static_cm_column_id_);
        TBSYS_LOG(INFO, "g_conf.using_static_cm_column_id set to %s", STR_BOOL(g_conf.using_static_cm_column_id));
      }

      if (OB_SUCCESS == err)
      {
        int64_t old_value = replay_wait_time_us_;
        replay_wait_time_us_ = config.getInt(OBUPS_UPS_SECTION, OBUPS_REPLAY_WAIT_TIME_US,
            DEFAULT_REPLAY_WAIT_TIME_US);
        TBSYS_LOG(INFO, "replay_wait_time_us_ switch from %ld to %ld", old_value, replay_wait_time_us_);
      }

      if (OB_SUCCESS == err)
      {
        int64_t old_value = fetch_log_wait_time_us_;
        fetch_log_wait_time_us_ = config.getInt(OBUPS_UPS_SECTION, OBUPS_FETCH_LOG_WAIT_TIME_US,
            DEFAULT_FETCH_LOG_WAIT_TIME_US);
        TBSYS_LOG(INFO, "fetch_log_wait_time_us_ switch from %ld to %ld", old_value, fetch_log_wait_time_us_);
      }

      if (OB_SUCCESS == err)
      {
        int64_t old_value = log_sync_delay_warn_time_threshold_us_;
        log_sync_delay_warn_time_threshold_us_ = config.getInt(OBUPS_UPS_SECTION,
                                        OBUPS_LOG_SYNC_DELAY_WARN_TIME_THRESHOLD_US,
                                        DEFAULT_LOG_SYNC_DELAY_WARN_TIME_THRESHOLD_US);
        TBSYS_LOG(INFO, "log_sync_delay_warn_time_threshold_us_ switch from %ld to %ld", old_value, log_sync_delay_warn_time_threshold_us_);
      }
      
      if (OB_SUCCESS == err)
      {
        int64_t old_value = log_sync_delay_warn_report_interval_us_;
        log_sync_delay_warn_report_interval_us_ = config.getInt(OBUPS_UPS_SECTION,
                                                                     OBUPS_LOG_SYNC_DELAY_WARN_REPORT_INTERVAL_US,
                                                                     DEFAULT_LOG_SYNC_DELAY_WARN_REPORT_INTERVAL_US);
        TBSYS_LOG(INFO, "log_sync_delay_warn_report_interval_us_ switch from %ld to %ld", old_value, log_sync_delay_warn_report_interval_us_);

      }

      if (OB_SUCCESS == err)
      {
        int64_t old_value = max_n_lagged_log_allowed_;
        max_n_lagged_log_allowed_ = config.getInt(OBUPS_UPS_SECTION,
                                                                     OBUPS_MAX_N_LAGGED_LOG_ALLOWED,
                                                                     DEFAULT_MAX_N_LAGGED_LOG_ALLOWED);
        TBSYS_LOG(INFO, "max_n_lagged_log_allowed_ switch from %ld to %ld", old_value, max_n_lagged_log_allowed_);

      }

      return err;
    }

   int ObUpdateServerParam::load_standalone_conf()
    {
      int err = OB_SUCCESS;

      read_thread_count_ = TBSYS_CONFIG.getInt(OBUPS_STANDALONE_SECTION, OBUPS_READ_THREAD_COUNT,
          DEFAULT_READ_THREAD_COUNT);
      if (read_thread_count_ <= 0)
      {
        TBSYS_LOG(ERROR, "read thread count (%ld) cannot <= 0.", read_thread_count_);
        err = OB_ERROR;
      }

      if (OB_SUCCESS == err)
      {
        read_task_queue_size_ = TBSYS_CONFIG.getInt(OBUPS_STANDALONE_SECTION, OBUPS_READ_TASK_QUEUE_SIZE,
            DEFAULT_TASK_READ_QUEUE_SIZE);
      }

      if (OB_SUCCESS == err)
      {
        write_task_queue_size_ = TBSYS_CONFIG.getInt(OBUPS_STANDALONE_SECTION, OBUPS_WRITE_TASK_QUEUE_SIZE,
            DEFAULT_TASK_WRITE_QUEUE_SIZE);
      }

      if (OB_SUCCESS == err)
      {
        write_thread_batch_num_ = TBSYS_CONFIG.getInt(OBUPS_STANDALONE_SECTION,
            OBUPS_WRITE_GROUP_COMMIT_NUM, DEFAULT_WRITE_GROUP_COMMIT_NUM);
      }

      if (OB_SUCCESS == err)
      {
        err = load_string(standalone_schema_, sizeof(standalone_schema_),
            OBUPS_STANDALONE_SECTION, OBUPS_STANDALONE_SCHEMA, false);
      }

      if (OB_SUCCESS == err)
      {
        test_schema_version_ = TBSYS_CONFIG.getInt(OBUPS_STANDALONE_SECTION,
            OBUPS_STANDALONE_SCHEMA_VERSION, 0);
        if (test_schema_version_ <= 0)
        {
          TBSYS_LOG(WARN, "invalid schema version, test_schema_version=%ld",
              test_schema_version_);
          err = OB_ERROR;
        }
      }
      return err;
    }

    int ObUpdateServerParam::load_string(char* dest, const int32_t size,
        const char* section, const char* name, bool not_null, tbsys::CConfig *config)
    {
      int ret = OB_SUCCESS;
      if (NULL == dest || 0 >= size || NULL == section || NULL == name)
      {
        ret = OB_ERROR;
      }

      dest[0] = '\0';
      const char* value = NULL;
      if (OB_SUCCESS == ret)
      {
        if (NULL == config)
        {
          value = TBSYS_CONFIG.getString(section, name);
        }
        else
        {
          value = config->getString(section, name);
        }
        if (not_null && (NULL == value || 0 >= strlen(value)))
        {
          TBSYS_LOG(ERROR, "%s.%s has not been set.", section, name);
          ret = OB_ERROR;
        }
      }

      if (OB_SUCCESS == ret && NULL != value)
      {
        if ((int32_t)strlen(value) >= size)
        {
          TBSYS_LOG(ERROR, "%s.%s too long, length (%ld) > %d",
              section, name, strlen(value), size);
          ret = OB_SIZE_OVERFLOW;
        }
        else
        {
          strncpy(dest, value, strlen(value) + 1);
        }
      }

      return ret;
    }
  }
}

