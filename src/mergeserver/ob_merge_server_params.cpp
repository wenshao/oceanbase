#include "common/ob_define.h"
#include "ob_merge_server_params.h"
#include "config.h"
#include "common/ob_malloc.h"
#include "common/utility.h"
#include <unistd.h>
#include <errno.h>

using namespace oceanbase::common;

namespace
{
  const char * OBMS_MS_SECTION = "merge_server";
  const char * OBMS_RS_SECTION = "root_server";
  const char * OBMS_PORT = "port";
  const char * OBMS_VIP = "vip";
  const char * OBMS_DEVNAME = "dev_name";
  const char * OBMS_RETRY_TIMES = "retry_times";
  const char * OBMS_TASK_QUEUE_SIZE = "task_queue_size";
  const char * OBMS_TASK_THREAD_COUNT = "task_thread_count";
  const char * OBMS_TASK_LEFT_TIME = "task_left_time_us";
  const char * OBMS_LOG_INTERVAL_COUNT = "log_interval_count";
  const char * OBMS_NETWORK_TIMEOUT = "network_timeout_us";
  const char * OBMS_LEASE_INTERVAL = "lease_interval_us";
  const char * OBMS_MONITOR_INTERVAL = "monitor_interval_us";
  const char * OBMS_UPSLIST_INTERVAL = "upslist_interval_us";
  const char * OBMS_UPSVERION_TIMEOT = "ups_version_timeout_us";
  const char * OBMS_BLACKLIST_TIMEOUT = "blacklist_timeout_us";
  const char * OBMS_BLACKLIST_FAIL_COUNT = "blacklist_fail_count";
  const char * OBMS_LOCATION_CACHE_SIZE = "location_cache_size_mb";
  const char * OBMS_LOCATION_CACHE_TIMEOUT = "location_cache_timeout_us";
  const char * OBMS_INTERMEDIATE_BUFFER_SIZE = "intermediate_buffer_size_mbyte";
  const char * OBMS_MAX_REQ_PROCESS_TIME = "max_req_process_time_us";
  const char * OBMS_MEMORY_SIZE_LIMIT_PERCENT = "memory_size_limit_percent";
  const char * OBMS_MAX_CS_TIMEOUT_PERCENT = "max_cs_timeout_percent";
  const char * OBMS_SUPPORT_SESSION_NEXT = "support_session_next";
  const char * OBMS_ALLOW_RETURN_UNCOMPLETE_RESULT = "allow_return_uncomplete_result";
  const char * OBMS_MAX_PARELLEL_COUNT = "max_parellel_count";
  const char * OBMS_MAX_GET_ROWS_PER_SUBREQ = "max_get_rows_per_subreq";
  const char * OBMS_SLOW_QUERY_THRESHOLD = "slow_query_threshold";
  const char * OBMS_QUERY_CACHE_SIZE = "query_cache_size_mbyte";
  const char * OBMS_MIN_DROP_ERROR_COUNT = "min_drop_err_count";
  const char * OBMS_MAX_ACCESS_LOCK_COUNT = "max_access_lock_count";
  const char * OBMS_RESERVE_GET_PARAM_COUNT = "reserve_get_param_count";
  const char * OBMS_GET_REQUEST_FACTOR = "cs_get_request_factor";
  const char * OBMS_SCAN_REQUEST_FACTOR = "cs_scan_request_factor";
  const int64_t OB_MS_MAX_MEMORY_SIZE_LIMIT = 80;
  const int64_t OB_MS_MIN_MEMORY_SIZE_LIMIT = 10;
}

namespace oceanbase
{
  namespace mergeserver
  {
    tbsys::CThreadMutex ObMergeServerParams::cache_file_lock_;

    ObMergeServerParams::ObMergeServerParams()
    {
      memset(this, 0, sizeof(ObMergeServerParams));
    }

    int ObMergeServerParams::reload_from_config(const char * config_file)
    {
      tbsys::CThreadGuard guard(&cache_file_lock_);
      int ret = OB_SUCCESS;
      ObMergeServerParams new_param;
      tbsys::CConfig conf;
      if (conf.load(config_file))
      {
        TBSYS_LOG(WARN, "check load conf failed:file[%s]", config_file);
        ret = OB_ERROR;
      }
      else
      {
        ret = new_param.load_from_config(conf);
        if (ret != OB_SUCCESS)
        {
          new_param.dump_config();
          TBSYS_LOG(ERROR, "check reload config failed:ret[%d]", ret);
        }
      }

      if (OB_SUCCESS == ret)
      {
        task_queue_size_ = new_param.task_queue_size_;
        task_thread_count_ = new_param.task_thread_count_;
        log_interval_count_ = new_param.log_interval_count_;
        task_left_time_ = new_param.task_left_time_;
        retry_times_ = new_param.retry_times_;
        network_time_out_ = new_param.network_time_out_;
        frozen_version_timeout_ = new_param.frozen_version_timeout_;
        ups_fail_count_ = new_param.ups_fail_count_;
        fetch_ups_interval_ = new_param.fetch_ups_interval_;
        monitor_interval_ = new_param.monitor_interval_;
        check_lease_interval_ = new_param.check_lease_interval_;
        ups_blacklist_timeout_ = new_param.ups_blacklist_timeout_;
        location_cache_size_ = new_param.location_cache_size_;
        location_cache_timeout_ = new_param.location_cache_timeout_;
        intermediate_buffer_size_ = new_param.intermediate_buffer_size_;
        memory_size_limit_ = new_param.memory_size_limit_;
        max_parellel_count_ = new_param.max_parellel_count_;
        max_get_rows_per_subreq_ = new_param.max_get_rows_per_subreq_;
        max_req_process_time_ = new_param.max_req_process_time_;
        support_session_next_ = new_param.support_session_next_;
        allow_return_uncomplete_result_ = new_param.allow_return_uncomplete_result_;
        slow_query_threshold_ = new_param.slow_query_threshold_;
        max_cs_timeout_percent_ = new_param.max_cs_timeout_percent_;
        min_drop_error_count_ = new_param.min_drop_error_count_;
        //max_access_lock_count_ = new_param.max_access_lock_count_;
        reserve_get_param_count_ = new_param.reserve_get_param_count_;
        get_request_factor_ = new_param.get_request_factor_;
        scan_request_factor_ = new_param.scan_request_factor_;
      }
      return ret;
    }

    void ObMergeServerParams::copy_to(ObMergeServerParams &new_param) const
    {
      memcpy(&new_param, this, sizeof(ObMergeServerParams));
    }

    int ObMergeServerParams::load_from_config(tbsys::CConfig & conf)
    {
      int ret = OB_SUCCESS;

      ret = load_string(conf, root_server_ip_, OB_MAX_IP_SIZE, OBMS_RS_SECTION, OBMS_VIP);
      if (ret == OB_SUCCESS)
      {
        ret = load_string(conf, dev_name_, OB_MAX_IP_SIZE, OBMS_MS_SECTION, OBMS_DEVNAME);
      }

      if (ret == OB_SUCCESS)
      {
        server_listen_port_ = conf.getInt(OBMS_MS_SECTION, OBMS_PORT, 0);
        if (server_listen_port_ <= 0)
        {
          TBSYS_LOG(ERROR, "mergeserver listen port must > 0, got (%d)", server_listen_port_);
          ret = OB_INVALID_ARGUMENT;
        }
      }

      if (ret == OB_SUCCESS)
      {
        root_server_port_ = conf.getInt(OBMS_RS_SECTION, OBMS_PORT, 0);
        if (root_server_port_ <= 0)
        {
          TBSYS_LOG(ERROR, "root server's port must > 0, got (%d)", root_server_port_);
          ret = OB_INVALID_ARGUMENT;
        }
      }

      if (ret == OB_SUCCESS)
      {
        task_queue_size_ = conf.getInt(OBMS_MS_SECTION, OBMS_TASK_QUEUE_SIZE, 1000);
        if (task_queue_size_ <= 0)
        {
          TBSYS_LOG(ERROR, "task queue size must > 0, got (%d)", task_queue_size_);
          ret = OB_INVALID_ARGUMENT;
        }
      }

      if (ret == OB_SUCCESS)
      {
        task_thread_count_ = conf.getInt(OBMS_MS_SECTION, OBMS_TASK_THREAD_COUNT, 20);
        if (task_thread_count_ <= 0)
        {
          TBSYS_LOG(ERROR, "task thread count must > 0, got (%d)", task_thread_count_);
          ret = OB_INVALID_ARGUMENT;
        }
      }

      if (ret == OB_SUCCESS)
      {
        task_left_time_ = conf.getInt(OBMS_MS_SECTION, OBMS_TASK_LEFT_TIME, 100 * 1000);
        if (task_left_time_ < 0)
        {
          TBSYS_LOG(ERROR, "task left time must >= 0, got (%ld)", task_left_time_);
          ret = OB_INVALID_ARGUMENT;
        }
        // log interval count
        log_interval_count_ = conf.getInt(OBMS_MS_SECTION, OBMS_LOG_INTERVAL_COUNT, 100);
      }

      if (ret == OB_SUCCESS)
      {
        network_time_out_ = conf.getInt(OBMS_MS_SECTION, OBMS_NETWORK_TIMEOUT, 2000000);
        if (network_time_out_ <= 0)
        {
          TBSYS_LOG(ERROR, "network timeout must > 0, got (%ld)", network_time_out_);
          ret = OB_INVALID_ARGUMENT;
        }
      }

      if (ret == OB_SUCCESS)
      {
        ups_blacklist_timeout_ = conf.getInt(OBMS_MS_SECTION, OBMS_BLACKLIST_TIMEOUT, 60 * 1000 * 1000L);
        if (ups_blacklist_timeout_ <= 0)
        {
          TBSYS_LOG(ERROR, "ups in blacklist timeout must > 0, got (%ld)", ups_blacklist_timeout_);
          ret = OB_INVALID_ARGUMENT;
        }
      }

      if (ret == OB_SUCCESS)
      {
        ups_fail_count_ = conf.getInt(OBMS_MS_SECTION, OBMS_BLACKLIST_FAIL_COUNT, 100);
        if (ups_fail_count_ <= 0)
        {
          TBSYS_LOG(ERROR, "ups fail count into blacklist must > 0, got (%ld)", ups_fail_count_);
          ret = OB_INVALID_ARGUMENT;
        }
      }

      if (ret == OB_SUCCESS)
      {
        fetch_ups_interval_ = conf.getInt(OBMS_MS_SECTION, OBMS_UPSLIST_INTERVAL, 60 * 1000 * 1000L);
        if (fetch_ups_interval_<= 0)
        {
          TBSYS_LOG(ERROR, "fetch ups list interval time must > 0, got (%ld)", fetch_ups_interval_);
          ret = OB_INVALID_ARGUMENT;
        }
      }

      if (ret == OB_SUCCESS)
      {
        frozen_version_timeout_ = conf.getInt(OBMS_MS_SECTION, OBMS_UPSVERION_TIMEOT, 600 * 1000 * 1000L);
        if (frozen_version_timeout_ <= 0)
        {
          TBSYS_LOG(ERROR, "ups frozen version cache tiemout must > 0, got (%ld)", frozen_version_timeout_);
          ret = OB_INVALID_ARGUMENT;
        }
      }

      if (ret == OB_SUCCESS)
      {
        check_lease_interval_ = conf.getInt(OBMS_MS_SECTION, OBMS_LEASE_INTERVAL, 6 * 1000 * 1000L);
        if (check_lease_interval_ <= 0)
        {
          TBSYS_LOG(ERROR, "check lease interval time must > 0, got (%ld)", check_lease_interval_);
          ret = OB_INVALID_ARGUMENT;
        }
      }

      if (ret == OB_SUCCESS)
      {
        monitor_interval_ = conf.getInt(OBMS_MS_SECTION, OBMS_MONITOR_INTERVAL, 600 * 1000 * 1000L);
        if (monitor_interval_ <= 0)
        {
          TBSYS_LOG(ERROR, "monitor interval time must > 0, got (%ld)", monitor_interval_);
          ret = OB_INVALID_ARGUMENT;
        }
      }

      if (ret == OB_SUCCESS)
      {
        retry_times_ = conf.getInt(OBMS_MS_SECTION, OBMS_RETRY_TIMES, 3);
        if (retry_times_ < 0)
        {
          TBSYS_LOG(ERROR, "retry_times must >= 0, got (%d)", retry_times_);
          ret = OB_INVALID_ARGUMENT;
        }
      }

      if (OB_SUCCESS == ret)
      {
        location_cache_timeout_ = conf.getInt(OBMS_MS_SECTION, OBMS_LOCATION_CACHE_TIMEOUT, 1000 * 1000 * 600L);
        if (location_cache_timeout_ <= 0)
        {
          TBSYS_LOG(ERROR, "tablet location cache timeout must > 0, got (%ld)", location_cache_timeout_);
          ret = OB_INVALID_ARGUMENT;
        }
      }

      if (OB_SUCCESS == ret)
      {
        location_cache_size_ = conf.getInt(OBMS_MS_SECTION, OBMS_LOCATION_CACHE_SIZE);
        if (location_cache_size_ <= 0)
        {
          TBSYS_LOG(ERROR, "tablet location cache size should great than 0 got (%d)",
            location_cache_size_);
          ret = OB_INVALID_ARGUMENT;
        }
        else
        {
          location_cache_size_ *= 1024*1024;
        }
      }

      if (ret == OB_SUCCESS)
      {
        intermediate_buffer_size_ = conf.getInt(OBMS_MS_SECTION, OBMS_INTERMEDIATE_BUFFER_SIZE, 24);
        if (intermediate_buffer_size_ <= 0)
        {
          TBSYS_LOG(ERROR, "intermediate_buffer_size_ must > 0, got (%ld)", intermediate_buffer_size_);
          ret = OB_INVALID_ARGUMENT;
        }
        else
        {
          intermediate_buffer_size_ *= 1024*1024;
        }
      }

      if (OB_SUCCESS == ret)
      {
        max_parellel_count_ = conf.getInt(OBMS_MS_SECTION, OBMS_MAX_PARELLEL_COUNT, 16);
        if (max_parellel_count_ <= 0)
        {
          TBSYS_LOG(ERROR, "max_parellel_count_ must > 0, got (%ld)", max_parellel_count_);
          ret = OB_INVALID_ARGUMENT;
        }
      }
      if (OB_SUCCESS == ret)
      {
        max_get_rows_per_subreq_ = conf.getInt(OBMS_MS_SECTION, OBMS_MAX_GET_ROWS_PER_SUBREQ, 20);
        if (max_get_rows_per_subreq_ < 0)
        {
          TBSYS_LOG(ERROR, "max_get_rows_per_subreq_ must > 0, got (%ld)", max_get_rows_per_subreq_);
          ret = OB_INVALID_ARGUMENT;
        }
      }
      if (OB_SUCCESS == ret)
      {
        slow_query_threshold_= conf.getInt(OBMS_MS_SECTION, OBMS_SLOW_QUERY_THRESHOLD, 100000);
        if (slow_query_threshold_ <= 0)
        {
          TBSYS_LOG(ERROR, "slow_query_threshold must > 0, got (%ld)", slow_query_threshold_);
          ret = OB_INVALID_ARGUMENT;
        }
      }

      if (OB_SUCCESS == ret)
      {
        max_req_process_time_ = conf.getInt(OBMS_MS_SECTION, OBMS_MAX_REQ_PROCESS_TIME, 1500000);
        if (max_req_process_time_ <= 0)
        {
          TBSYS_LOG(ERROR, "max_req_process_time_ must > 0, got (%ld)", max_req_process_time_);
          ret = OB_INVALID_ARGUMENT;
        }
      }
      if (OB_SUCCESS == ret)
      {
        int64_t percent =  conf.getInt(OBMS_MS_SECTION, OBMS_MAX_CS_TIMEOUT_PERCENT, 70);
        if (percent <= 0 || percent > 100)
        {
          TBSYS_LOG(ERROR, "max_cs_time_percent must be in  (0, 100], but here , it is %ld", percent);
          ret = OB_INVALID_ARGUMENT;
        }
        else
        {
          max_cs_timeout_percent_ = (static_cast<double>(percent) / 100.0);
        }
      }

      if (ret == OB_SUCCESS)
      {
        support_session_next_ = conf.getInt(OBMS_MS_SECTION, OBMS_SUPPORT_SESSION_NEXT, 0);
        if ((support_session_next_!= 0) && (support_session_next_ != 1))
        {
          TBSYS_LOG(ERROR, "supoort session next must 0 or 1 it's [%d]", support_session_next_);
          ret = OB_INVALID_ARGUMENT;
        }
      }

      if (ret == OB_SUCCESS)
      {
        allow_return_uncomplete_result_ = conf.getInt(OBMS_MS_SECTION, OBMS_ALLOW_RETURN_UNCOMPLETE_RESULT, 0);
        if ((allow_return_uncomplete_result_ != 0) && (allow_return_uncomplete_result_ != 1))
        {
          TBSYS_LOG(ERROR, "allow_return_uncomplete_result must 0 or 1 it's [%d]", allow_return_uncomplete_result_);
          ret = OB_INVALID_ARGUMENT;
        }
      }

      if (ret == OB_SUCCESS)
      {
        memory_size_limit_ = conf.getInt(OBMS_MS_SECTION, OBMS_MEMORY_SIZE_LIMIT_PERCENT, 15);
        if (memory_size_limit_ < OB_MS_MIN_MEMORY_SIZE_LIMIT
          || OB_MS_MAX_MEMORY_SIZE_LIMIT < memory_size_limit_)
        {
          TBSYS_LOG(ERROR, "memory_size_limit_percent between [%ld,%ld], got (%ld)", OB_MS_MIN_MEMORY_SIZE_LIMIT,
            OB_MS_MAX_MEMORY_SIZE_LIMIT, memory_size_limit_);
          ret = OB_INVALID_ARGUMENT;
        }
        else
        {
          memory_size_limit_ = memory_size_limit_*sysconf(_SC_PHYS_PAGES)*sysconf(_SC_PAGE_SIZE)/100;
          ob_set_memory_size_limit(memory_size_limit_);
          TBSYS_LOG(DEBUG, "set memory size limit [limit:%ld]", memory_size_limit_);
        }
      }

      if (ret == OB_SUCCESS)
      {
        query_cache_size_ = conf.getInt(OBMS_MS_SECTION, OBMS_QUERY_CACHE_SIZE, 0);
        if (query_cache_size_ < 0)
        {
          TBSYS_LOG(ERROR, "query_cache_size_ must >= 0, got (%ld)", query_cache_size_);
          ret = OB_INVALID_ARGUMENT;
        }
        else
        {
          query_cache_size_ *= 1024*1024;
        }
      }

      if (ret == OB_SUCCESS)
      {
        min_drop_error_count_ = conf.getInt(OBMS_MS_SECTION, OBMS_MIN_DROP_ERROR_COUNT, 1);
        if (min_drop_error_count_ <= 0)
        {
          TBSYS_LOG(ERROR, "min drop packet count for error log must > 0, got (%ld)", min_drop_error_count_);
          ret = OB_INVALID_ARGUMENT;
        }
      }

      if (ret == OB_SUCCESS)
      {
        max_access_lock_count_ = conf.getInt(OBMS_MS_SECTION, OBMS_MAX_ACCESS_LOCK_COUNT, 10);
        if (max_access_lock_count_ <= 0)
        {
          TBSYS_LOG(ERROR, "max access root server lock count must > 0, got (%d)", max_access_lock_count_);
          ret = OB_INVALID_ARGUMENT;
        }
      }

      if (ret == OB_SUCCESS)
      {
        reserve_get_param_count_ = conf.getInt(OBMS_MS_SECTION, OBMS_RESERVE_GET_PARAM_COUNT, 5);
        if (reserve_get_param_count_ < 0)
        {
          TBSYS_LOG(ERROR, "reserve get param count must >= 0, got (%d)", reserve_get_param_count_);
          ret = OB_INVALID_ARGUMENT;
        }
      }

      if (ret == OB_SUCCESS)
      {
        // default not open
        get_request_factor_ = conf.getInt(OBMS_MS_SECTION, OBMS_GET_REQUEST_FACTOR, 1);
        scan_request_factor_ = conf.getInt(OBMS_MS_SECTION, OBMS_SCAN_REQUEST_FACTOR, 2);
      }
      return ret;
    }

    int ObMergeServerParams::dump_config_to_file(const char * config_file)
    {
      tbsys::CThreadGuard guard(&cache_file_lock_);
      int ret = OB_SUCCESS;
      int fd = 0;
      static const int64_t buf_len = 512;
      int64_t pos = 0;
      char buf[buf_len];
      ssize_t size = 0;
      fd = open(config_file, O_WRONLY | O_CREAT | O_TRUNC, S_IRUSR  | S_IWUSR | S_IRGRP);
      if (0 > fd)
      {
        TBSYS_LOG(WARN, "fail to create temp config file %s. msg=%s", config_file, strerror(errno));
        ret = OB_ERROR;
      }
      else
      {
#define WRITE_SECTION_CONF_TO_FILE(section_name) \
        if (OB_SUCCESS == ret) \
        { \
          pos = 0; \
          databuff_printf(buf, buf_len, pos, "[%s]\n", section_name); \
          OB_ASSERT(pos < buf_len); \
          size = write(fd, buf, pos); \
          if (size != pos) \
          { \
            TBSYS_LOG(WARN, "fail to write data to file. buf=%.*s", static_cast<int32_t>(buf_len), buf); \
            ret = OB_ERROR; \
          }    \
        }


#define WRITE_KV_CONF_TO_FILE(format, key, value) \
        if (OB_SUCCESS == ret) \
        { \
          pos = 0; \
          databuff_printf(buf, buf_len, pos, format, key, value); \
          OB_ASSERT(pos < buf_len); \
          size = write(fd, buf, pos); \
          if (size != pos) \
          { \
            TBSYS_LOG(WARN, "fail to write data to file. buf=%.*s", static_cast<int32_t>(buf_len), buf); \
            ret = OB_ERROR; \
          }    \
        }

        // rootserver section
        WRITE_SECTION_CONF_TO_FILE(OBMS_RS_SECTION);
        WRITE_KV_CONF_TO_FILE("%s=%s\n", OBMS_VIP, root_server_ip_);
        WRITE_KV_CONF_TO_FILE("%s=%d\n", OBMS_PORT, root_server_port_);
        // mergeserver section
        WRITE_SECTION_CONF_TO_FILE(OBMS_MS_SECTION);
        WRITE_KV_CONF_TO_FILE("%s=%d\n", OBMS_PORT, server_listen_port_);
        WRITE_KV_CONF_TO_FILE("%s=%s\n", OBMS_DEVNAME, dev_name_);
        WRITE_KV_CONF_TO_FILE("%s=%d\n", OBMS_RETRY_TIMES, retry_times_);
        WRITE_KV_CONF_TO_FILE("%s=%d\n", OBMS_TASK_QUEUE_SIZE, task_queue_size_);
        WRITE_KV_CONF_TO_FILE("%s=%d\n", OBMS_TASK_THREAD_COUNT, task_thread_count_);
        WRITE_KV_CONF_TO_FILE("%s=%ld\n", OBMS_TASK_LEFT_TIME, task_left_time_);
        WRITE_KV_CONF_TO_FILE("%s=%ld\n", OBMS_LOG_INTERVAL_COUNT, log_interval_count_);
        WRITE_KV_CONF_TO_FILE("%s=%ld\n", OBMS_NETWORK_TIMEOUT, network_time_out_);
        WRITE_KV_CONF_TO_FILE("%s=%ld\n", OBMS_LEASE_INTERVAL, check_lease_interval_);
        WRITE_KV_CONF_TO_FILE("%s=%ld\n", OBMS_MONITOR_INTERVAL, monitor_interval_);
        WRITE_KV_CONF_TO_FILE("%s=%ld\n", OBMS_UPSLIST_INTERVAL, fetch_ups_interval_);
        WRITE_KV_CONF_TO_FILE("%s=%ld\n", OBMS_UPSVERION_TIMEOT, frozen_version_timeout_);
        WRITE_KV_CONF_TO_FILE("%s=%ld\n", OBMS_BLACKLIST_TIMEOUT, ups_blacklist_timeout_);
        WRITE_KV_CONF_TO_FILE("%s=%ld\n", OBMS_BLACKLIST_FAIL_COUNT, ups_fail_count_);
        int64_t location_cache_size = location_cache_size_/ (1024*1024);
        WRITE_KV_CONF_TO_FILE("%s=%ld\n", OBMS_LOCATION_CACHE_SIZE, location_cache_size);
        WRITE_KV_CONF_TO_FILE("%s=%ld\n", OBMS_LOCATION_CACHE_TIMEOUT, location_cache_timeout_);
        int64_t intermediate_buffer_size = intermediate_buffer_size_ / (1024*1024);
        WRITE_KV_CONF_TO_FILE("%s=%ld\n", OBMS_INTERMEDIATE_BUFFER_SIZE, intermediate_buffer_size);
        WRITE_KV_CONF_TO_FILE("%s=%ld\n", OBMS_MAX_REQ_PROCESS_TIME, max_req_process_time_);
        int32_t max_cs_timeout_percent = (int32_t)(max_cs_timeout_percent_ * 100);
        WRITE_KV_CONF_TO_FILE("%s=%d\n", OBMS_MAX_CS_TIMEOUT_PERCENT, max_cs_timeout_percent);
        int64_t memory_size_limit = memory_size_limit_ * 100 / (sysconf(_SC_PHYS_PAGES)*sysconf(_SC_PAGE_SIZE));
        WRITE_KV_CONF_TO_FILE("%s=%ld\n", OBMS_MEMORY_SIZE_LIMIT_PERCENT, memory_size_limit);
        WRITE_KV_CONF_TO_FILE("%s=%d\n", OBMS_ALLOW_RETURN_UNCOMPLETE_RESULT, allow_return_uncomplete_result_);
        WRITE_KV_CONF_TO_FILE("%s=%ld\n", OBMS_MAX_PARELLEL_COUNT, max_parellel_count_);
        WRITE_KV_CONF_TO_FILE("%s=%ld\n", OBMS_MAX_GET_ROWS_PER_SUBREQ, max_get_rows_per_subreq_);
        WRITE_KV_CONF_TO_FILE("%s=%ld\n", OBMS_SLOW_QUERY_THRESHOLD, slow_query_threshold_);
        WRITE_KV_CONF_TO_FILE("%s=%d\n", OBMS_GET_REQUEST_FACTOR, get_request_factor_);
        WRITE_KV_CONF_TO_FILE("%s=%d\n", OBMS_SCAN_REQUEST_FACTOR, scan_request_factor_);
        int64_t query_cache_size = query_cache_size_ / (1024*1024);
        WRITE_KV_CONF_TO_FILE("%s=%ld\n", OBMS_QUERY_CACHE_SIZE, query_cache_size);
#undef WRITE_SECTION_CONF_TO_FILE
#undef WRITE_KV_CONF_TO_FILE
        if (fd >= 0)
        {
          int err = close(fd);
          if (err < 0)
          {
            TBSYS_LOG(WARN, "fail to close file %d", fd);
          }
        }
      }
      return ret;
    }

    int ObMergeServerParams::update_item(const ObObj &item_name, const ObObj &item_value)
    {
      int ret = OB_SUCCESS;
      static const int max_name_len = 256;
      char name[max_name_len];
      int64_t value = 0;
      ObString str;

      if (OB_SUCCESS != (ret = item_name.get_varchar(str)))
      {
        TBSYS_LOG(WARN, "fail to get item name value. ret=%d", ret);
      }
      else if (str.length() < max_name_len)
      {
        strncpy(name, str.ptr(), str.length());
        name[str.length()] = '\0';
      }
      else
      {
        TBSYS_LOG(WARN, "item name too long. are you sure about this? item_name len=%d, max=%d", str.length(), max_name_len);
      }

      if ((OB_SUCCESS == ret) && (OB_SUCCESS != (ret = item_value.get_int(value))))
      {
        TBSYS_LOG(WARN, "fail to get value. expect int type");
      }

      if (OB_SUCCESS == ret)
      {
        if (0 == strcmp(name, OBMS_RETRY_TIMES))
        {
          retry_times_ = static_cast<int32_t>(value);
        }
        else if (0 == strcmp(name,  OBMS_TASK_QUEUE_SIZE))
        {
          task_queue_size_ = static_cast<int32_t>(value);
        }
        else if (0 == strcmp(name,  OBMS_TASK_THREAD_COUNT))
        {
          task_thread_count_ = static_cast<int32_t>(value);
        }
        else if (0 == strcmp(name,  OBMS_TASK_LEFT_TIME))
        {
          task_left_time_ = value;
        }
        else if (0 == strcmp(name,  OBMS_LOG_INTERVAL_COUNT))
        {
          log_interval_count_ = value;
        }
        else if (0 == strcmp(name,  OBMS_NETWORK_TIMEOUT))
        {
          network_time_out_ = value;
        }
        else if (0 == strcmp(name,  OBMS_LEASE_INTERVAL))
        {
          check_lease_interval_ = value;
        }
        else if (0 == strcmp(name,  OBMS_MONITOR_INTERVAL))
        {
          monitor_interval_ = value;
        }
        else if (0 == strcmp(name,  OBMS_UPSLIST_INTERVAL))
        {
          fetch_ups_interval_ = value;
        }
        else if (0 == strcmp(name,  OBMS_UPSVERION_TIMEOT))
        {
          frozen_version_timeout_ = value;
        }
        else if (0 == strcmp(name,  OBMS_BLACKLIST_TIMEOUT))
        {
          ups_blacklist_timeout_ = value;
        }
        else if (0 == strcmp(name,  OBMS_BLACKLIST_FAIL_COUNT))
        {
          ups_fail_count_ = value;
        }
        else if (0 == strcmp(name,  OBMS_LOCATION_CACHE_SIZE))
        {
          location_cache_size_ = static_cast<int32_t>(value) * 1024 * 1024L;
        }
        else if (0 == strcmp(name,  OBMS_LOCATION_CACHE_TIMEOUT))
        {
          location_cache_timeout_ = value;
        }
        else if (0 == strcmp(name,  OBMS_INTERMEDIATE_BUFFER_SIZE))
        {
          intermediate_buffer_size_  = value * (1024*1024);
        }
        else if (0 == strcmp(name,  OBMS_MAX_REQ_PROCESS_TIME))
        {
          max_req_process_time_ = value;
        }
        else if (0 == strcmp(name,  OBMS_MEMORY_SIZE_LIMIT_PERCENT))
        {
          memory_size_limit_ = value * (sysconf(_SC_PHYS_PAGES)*sysconf(_SC_PAGE_SIZE)) / 100;
        }
        else if (0 == strcmp(name, OBMS_MAX_CS_TIMEOUT_PERCENT))
        {
          max_cs_timeout_percent_ = static_cast<double>(value) / 100.0f;
        }
        else if (0 == strcmp(name,  OBMS_ALLOW_RETURN_UNCOMPLETE_RESULT))
        {
          allow_return_uncomplete_result_ = static_cast<int32_t>(value);
        }
        else if (0 == strcmp(name,  OBMS_MAX_PARELLEL_COUNT))
        {
          max_parellel_count_ = value;
        }
        else if (0 == strcmp(name,  OBMS_MAX_GET_ROWS_PER_SUBREQ))
        {
          max_get_rows_per_subreq_ = value;
        }
        else if (0 == strcmp(name,  OBMS_GET_REQUEST_FACTOR))
        {
          get_request_factor_ = value;
        }
        else if (0 == strcmp(name,  OBMS_SCAN_REQUEST_FACTOR))
        {
          scan_request_factor_ = value;
        }
        else if (0 == strcmp(name,  OBMS_SLOW_QUERY_THRESHOLD))
        {
          slow_query_threshold_ = value;
        }
        else
        {
          TBSYS_LOG(ERROR, "unexpected config item name. name=%s", name);
          ret = OB_ERROR;
        }
      }
      return ret;
    }

    int ObMergeServerParams::load_string(tbsys::CConfig & conf, char* dest, const int32_t size,
      const char* section, const char* name, bool require)
    {
      int ret = OB_SUCCESS;
      if (NULL == dest || 0 >= size || NULL == section || NULL == name)
      {
        ret = OB_ERROR;
      }

      const char* value = NULL;
      if (OB_SUCCESS == ret)
      {
        value = conf.getString(section, name);
        if (require && (NULL == value || 0 >= strlen(value)))
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
          strncpy(dest, value, strlen(value));
        }
      }

      return ret;
    }

    void ObMergeServerParams::dump_config()
    {
      TBSYS_LOG(INFO, "%s", "dump mergeserver config:");
      TBSYS_LOG(INFO, "root server ip => %s", root_server_ip_);
      TBSYS_LOG(INFO, "root server port => %d", root_server_port_);
      TBSYS_LOG(INFO, "listen port => %d", server_listen_port_);
      TBSYS_LOG(INFO, "device name => %s", dev_name_);
      TBSYS_LOG(INFO, "task queue size => %d", task_queue_size_);
      TBSYS_LOG(INFO, "task min left time => %ld", task_left_time_);
      TBSYS_LOG(INFO, "task thread count => %d", task_thread_count_);
      TBSYS_LOG(INFO, "log interval count => %ld", log_interval_count_);
      TBSYS_LOG(INFO, "network time out => %ld", network_time_out_);
      TBSYS_LOG(INFO, "timeout retry times => %d", retry_times_);
      TBSYS_LOG(INFO, "check lease time interval => %ld", check_lease_interval_);
      TBSYS_LOG(INFO, "monitor time interval => %ld", monitor_interval_);
      TBSYS_LOG(INFO, "fetch ups list time interval => %ld", fetch_ups_interval_);
      TBSYS_LOG(INFO, "ups frozen version timeout => %ld", frozen_version_timeout_);
      TBSYS_LOG(INFO, "ups fail count into blacklist => %ld", ups_fail_count_);
      TBSYS_LOG(INFO, "ups blacklist time out => %ld", ups_blacklist_timeout_);
      TBSYS_LOG(INFO, "tablet location cache size in Mbyte => %d", location_cache_size_);
      TBSYS_LOG(INFO, "tablet location cache time out in ms => %ld", location_cache_timeout_);
      TBSYS_LOG(INFO, "rootserver address => %s:%d", root_server_ip_, root_server_port_);
      TBSYS_LOG(INFO, "memory size limit => %ld", memory_size_limit_);
      TBSYS_LOG(INFO, "max parellel count => %ld", max_parellel_count_);
      TBSYS_LOG(INFO, "max get rows per sub request => %ld", max_get_rows_per_subreq_);
      TBSYS_LOG(INFO, "max request process time_ => %ld", max_req_process_time_);
      TBSYS_LOG(INFO, "support session next => %d", support_session_next_);
      TBSYS_LOG(INFO, "allow return uncomplete result => %d", allow_return_uncomplete_result_);
      TBSYS_LOG(INFO, "slow query threshold => %ld", slow_query_threshold_);
      TBSYS_LOG(INFO, "max cs process timeout percent => %lf", max_cs_timeout_percent_);
      TBSYS_LOG(INFO, "query cache size => %ld", query_cache_size_);
      TBSYS_LOG(INFO, "min drop packet error log count => %ld", min_drop_error_count_);
      TBSYS_LOG(INFO, "max access lock count => %d", max_access_lock_count_);
      TBSYS_LOG(INFO, "reserve get param count => %d", reserve_get_param_count_);
      TBSYS_LOG(INFO, "cs load balance get factor => %d", get_request_factor_);
      TBSYS_LOG(INFO, "cs load balance scan factor => %d", scan_request_factor_);
    }


  } /* mergeserver */
} /* oceanbase */
