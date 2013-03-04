#ifndef OCEANBASE_MERGESERVER_MERGESERVER_PARAMS_H_
#define OCEANBASE_MERGESERVER_MERGESERVER_PARAMS_H_
#include <stdint.h>
#include "tbsys.h"
#include "config.h"
#include "common/ob_object.h"

namespace oceanbase
{
  namespace mergeserver
  {
    class ObMergeServerParams
    {
      public:
        static const int32_t OB_MAX_IP_SIZE = 64;
        ObMergeServerParams();

      public:
        int load_from_config(tbsys::CConfig & conf);
        int reload_from_config(const char * config_file);
        void copy_to(ObMergeServerParams &new_param) const;
        int update_item(const common::ObObj &item_name, const common::ObObj &item_value);
        inline const char* get_config_cache_file_name() const { return "mergeserver.conf.bin"; }

        inline const char* get_device_name() const { return dev_name_; }
        inline const char* get_root_server_ip() const { return root_server_ip_; }
        inline int32_t get_root_server_port() const { return root_server_port_; }
        inline int32_t get_listen_port() const { return server_listen_port_; }
        inline int32_t get_task_queue_size() const { return task_queue_size_; }
        inline int32_t get_task_thread_size() const { return task_thread_count_; }
        inline int32_t get_task_left_time() const { return static_cast<int32_t>(task_left_time_); }
        inline int32_t get_retry_times() const { return retry_times_; }
        inline int64_t get_log_interval_count() const { return log_interval_count_; }
        inline int64_t get_network_timeout() const { return network_time_out_; }
        inline int64_t get_lease_check_interval() const { return check_lease_interval_; }
        inline int64_t get_monitor_interval() const { return monitor_interval_; }
        inline int64_t get_fetch_ups_interval() const { return fetch_ups_interval_; }
        inline int64_t get_frozen_version_timeout() const { return frozen_version_timeout_; }
        inline int64_t get_ups_fail_count() const { return ups_fail_count_; }
        inline int64_t get_ups_blacklist_timeout() const { return ups_blacklist_timeout_; }
        inline int32_t get_tablet_location_cache_size() const { return location_cache_size_;}
        inline int64_t get_tablet_location_cache_timeout() const { return location_cache_timeout_;}
        inline int64_t get_intermediate_buffer_size() const { return intermediate_buffer_size_; }
        inline int64_t get_max_parellel_count() const { return max_parellel_count_; }
        inline int64_t get_max_get_rows_per_subreq() const { return max_get_rows_per_subreq_; }
        inline int64_t get_max_memory_size_limit() const { return memory_size_limit_;}
        inline int64_t get_max_req_process_time() const { return max_req_process_time_;}
        inline double get_max_timeout_percent() const { return max_cs_timeout_percent_;}
        inline int64_t get_slow_query_threshold() const { return slow_query_threshold_; }
        inline bool get_support_session_next() const { return support_session_next_; }
        inline bool allow_return_uncomplete_result() const { return allow_return_uncomplete_result_; }
        inline int64_t get_query_cache_size() const { return query_cache_size_; }
        inline int64_t get_min_drop_count() const { return min_drop_error_count_; }
        inline int32_t get_max_access_lock_count() const { return max_access_lock_count_; }
        inline int32_t get_reserve_get_param_count() const { return reserve_get_param_count_; }
        inline int32_t get_get_request_factor() const { return get_request_factor_; }
        inline int32_t get_scan_request_factor() const { return scan_request_factor_; }
        //
        void dump_config();
        int dump_config_to_file(const char * config_file);
      private:
        int load_string(tbsys::CConfig & conf, char* dest, const int32_t size,
            const char* section, const char* name, bool require=true);

      private:
        char dev_name_[OB_MAX_IP_SIZE];
        char root_server_ip_[OB_MAX_IP_SIZE];
        int32_t root_server_port_;
        int32_t server_listen_port_;
        int64_t task_left_time_;
        int32_t task_queue_size_;
        int32_t task_thread_count_;
        int64_t log_interval_count_;
        int32_t retry_times_;
        int64_t network_time_out_;
        int64_t frozen_version_timeout_;
        int64_t ups_fail_count_;
        int64_t ups_blacklist_timeout_;
        int64_t fetch_ups_interval_;
        int64_t monitor_interval_;
        int64_t check_lease_interval_;
        int32_t location_cache_size_;
        int64_t location_cache_timeout_;
        int64_t intermediate_buffer_size_;
        int64_t memory_size_limit_;
        int64_t max_parellel_count_;
        int64_t max_get_rows_per_subreq_;
        int64_t max_req_process_time_;
        double max_cs_timeout_percent_;
        int32_t support_session_next_;
        int32_t allow_return_uncomplete_result_;
        int64_t slow_query_threshold_;
        int64_t query_cache_size_;
        int64_t min_drop_error_count_;
        int32_t max_access_lock_count_;
        int32_t reserve_get_param_count_;
        int32_t get_request_factor_;
        int32_t scan_request_factor_;
        // if new config item added, don't forget add it into dump_config
        static tbsys::CThreadMutex cache_file_lock_;
    };
  } /* mergeserver */
} /* oceanbase */

#endif /* end of include guard: OCEANBASE_MERGESERVER_MERGESERVER_PARAMS_H_ */
