/*===============================================================
*   (C) 2007-2010 Taobao Inc.
*   
*   
*   Version: 0.1 2010-09-19
*   
*   Authors:
*          ruohai(ruohai@taobao.com)
*   
*
================================================================*/
#ifndef OCEANBASE_ROOT_SERVER_LOG_WORKER_H
#define OCEANBASE_ROOT_SERVER_LOG_WORKER_H
#include "common/ob_server.h"
#include "common/ob_range.h"
#include "common/ob_tablet_info.h"
#include "common/ob_log_entry.h"
#include "rootserver/ob_chunk_server_manager.h"
#include "common/ob_ups_info.h"
#include "common/ob_client_config.h"
namespace oceanbase
{
  namespace rootserver
  {
    class ObRootLogManager;
    class ObRootServer2;
    class ObRootLogWorker
    {
      public:
        ObRootLogWorker();

      public:
        void set_root_server(ObRootServer2* root_server);
        void set_log_manager(ObRootLogManager* log_manager);
        int apply(common::LogCommand cmd, const char* log_data, const int64_t& data_len);

      uint64_t get_cur_log_file_id();
      uint64_t get_cur_log_seq();

      public:
        int sync_schema(const int64_t timestamp);
        int regist_cs(const common::ObServer& server, const int64_t timestamp);
        int regist_ms(const common::ObServer& server, const int64_t timestamp);
        int server_is_down(const common::ObServer& server, const int64_t timestamp);
        int report_cs_load(const common::ObServer& server, const int64_t capacity, const int64_t used);
        int cs_migrate_done(const common::ObRange& range, const common::ObServer& src_server, const common::ObServer& dest_server, const bool keep_src, const int64_t tablet_version);
        int report_tablets(const common::ObServer& server, const common::ObTabletReportInfoList& tablets, const int64_t timestamp);
        int remove_replica(const common::ObTabletReportInfo &replica);
        int remove_table(const common::ObArray<uint64_t> &deleted_tables);

        int add_new_tablet(const int count, const common::ObTabletInfo tablet, const int* server_indexs, const int64_t mem_vwesion);

        int batch_add_new_tablet(const common::ObTabletInfoList& tablets,
            int** server_indexs, int* count, const int64_t mem_version);
        int cs_merge_over(const common::ObServer& server, const int64_t timestamp);

        int sync_us_frozen_version(const int64_t frozen_version, const int64_t last_frozen_time);
        int set_ups_list(const common::ObUpsList &ups_list);
        int set_client_config(const common::ObClientConfig &client_conf);
      private:
        int log_server(const common::LogCommand cmd, const common::ObServer& server);
        int log_server_with_ts(const common::LogCommand cmd, const common::ObServer& server, const int64_t timestamp);
        int flush_log(const common::LogCommand cmd, const char* log_buffer, const int64_t& serialize_size);

      public:
        int do_check_point(const char* log_data, const int64_t& log_length);
        int do_schema_sync(const char* log_data, const int64_t& log_length);
        int do_cs_regist(const char* log_data, const int64_t& log_length);
        int do_ms_regist(const char* log_data, const int64_t& log_length);
        int do_server_down(const char* log_data, const int64_t& log_length);
        int do_cs_load_report(const char* log_data, const int64_t& log_length);
        int do_cs_migrate_done(const char* log_data, const int64_t& log_length);

        int do_report_tablets(const char* log_data, const int64_t& log_length);
        int do_remove_replica(const char* log_data, const int64_t& log_length);
        int do_remove_table(const char* log_data, const int64_t& log_length);
        
        int do_add_new_tablet(const char* log_data, const int64_t& log_length);
        int do_batch_add_new_tablet(const char* log_data, const int64_t& log_length);
        int do_create_table_done();

        int do_begin_balance();
        int do_balance_done();

        int do_cs_merge_over(const char* log_data, const int64_t& log_length);
        int do_sync_frozen_version(const char* log_data, const int64_t& log_length);
        int do_sync_frozen_version_and_time(const char* log_data, const int64_t& log_length);
        int do_set_ups_list(const char* log_data, const int64_t& log_length);
        int do_set_client_config(const char* log_data, const int64_t& log_length);

        void exit();

      private:

      private:
        ObRootServer2* root_server_;
        ObRootLogManager* log_manager_;
    };
  } /* rootserver */
} /* oceanbase */

#endif /* end of include guard: OCEANBASE_ROOT_SERVER_LOG_WORKER_H */
