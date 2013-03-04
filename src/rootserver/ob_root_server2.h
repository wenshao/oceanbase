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
#ifndef OCEANBASE_ROOTSERVER_OB_ROOT_SERVER2_H_
#define OCEANBASE_ROOTSERVER_OB_ROOT_SERVER2_H_
#include <tbsys.h>

#include "common/ob_define.h"
#include "common/ob_server.h"
#include "common/ob_string.h"
#include "common/ob_scan_param.h"
#include "common/ob_get_param.h"
#include "common/ob_obi_role.h"
#include "common/ob_obi_config.h"
#include "common/ob_ups_info.h"
#include "common/ob_schema_table.h"
#include "common/ob_client_config.h"
#include "common/ob_array.h"
#include "common/ob_tablet_info.h"
#include "rootserver/ob_chunk_server_manager.h"
#include "rootserver/ob_root_table2.h"
#include "rootserver/ob_root_log_worker.h"
#include "rootserver/ob_ups_manager.h"
#include "rootserver/ob_root_config.h"
#include "rootserver/ob_ups_heartbeat_runnable.h"
#include "rootserver/ob_ups_check_runnable.h"
#include "rootserver/ob_root_balancer.h"
#include "rootserver/ob_root_balancer_runnable.h"

class ObBalanceTest;
class ObBalanceTest_test_n_to_2_Test;
class ObBalanceTest_test_timeout_Test;
class ObBalanceTest_test_rereplication_Test;
class ObBalanceTest_test_n_to_2_with_faulty_dest_cs_Test;
class ObDeleteReplicasTest_delete_in_init_Test;
class ObDeleteReplicasTest_delete_when_rereplication_Test;
class ObDeleteReplicasTest_delete_when_report_Test;
class ObBalanceTest_test_shutdown_servers_Test;
namespace oceanbase 
{ 
  namespace common
  {
    class ObSchemaManagerV2;
    class ObRange;
    class ObTabletInfo;
    class ObTabletLocation;
    class ObScanner;
    class ObCellInfo;
    class ObTabletReportInfoList;
  }
  namespace rootserver 
  {
    class ObRootTable2;
    class ObRootServerTester;
    class OBRootWorker;

    class ObRootServer2
    {
      public:
        // status values for server_status_
        static const int STATUS_INIT         = 0;
        static const int STATUS_CHANGING     = 3;
        static const int STATUS_SLEEP        = 6;
      
        static const int64_t DEFAULT_SAFE_CS_NUMBER = 2;
        static const char* ROOT_TABLE_EXT;
        static const char* CHUNKSERVER_LIST_EXT;
        enum 
        {
          BUILD_SYNC_FLAG_NONE = 0,
          BUILD_SYNC_INIT_OK = 1,
          BUILD_SYNC_FLAG_CAN_ACCEPT_NEW_TABLE = 3,
        };

        ObRootServer2(ObRootConfig& config);
        virtual ~ObRootServer2();

        bool init(const int64_t now, OBRootWorker* worker);
        void start_threads();
        void stop_threads();
        void start_merge_check();
        int after_reload_config(bool did_write_log);
        /*
         * 从本地读取新schema, 判断兼容性
         */
        int switch_schema(const int64_t time_stamp, common::ObArray<uint64_t> &deleted_tables);
        /*
         * 切换过程中, update server冻结内存表 或者chunk server 进行merge等耗时操作完成
         * 发送消息调用此函数
         */
        int waiting_job_done(const common::ObServer& server, const int64_t frozen_mem_version);
        /*
         * chunk serve和merege server注册
         * @param out status 0 do not start report 1 start report
         */
        int regist_server(const common::ObServer& server, bool is_merge_server, int32_t& status, int64_t time_stamp = -1);
        /*
         * chunk server更新自己的磁盘情况信息
         */
        int update_capacity_info(const common::ObServer& server, const int64_t capacity, const int64_t used);
        /*
         * 迁移完成操作
         */
        virtual int migrate_over(const common::ObRange& range, const common::ObServer& src_server, 
            const common::ObServer& dest_server, const bool keep_src, const int64_t tablet_version);
        /*
         * 要求chunk server 主动进行汇报
         */
        int request_cs_report_tablet();
        bool get_schema(common::ObSchemaManagerV2& out_schema) const;
        int64_t get_schema_version() const;
int get_max_tablet_version(int64_t &version) const;
        int64_t get_config_version() const;
        int64_t get_alive_cs_number();
        void set_config_version(const int64_t version);
        
        int find_root_table_key(const uint64_t table_id, const common::ObString& table_name, const int32_t max_key_len, 
            const common::ObString& key, common::ObScanner& scanner) const;

        int find_root_table_key(const common::ObGetParam& get_param, common::ObScanner& scanner) const;
        int find_root_table_range(const common::ObScanParam& scan_param, common::ObScanner& scanner) const;

        virtual int report_tablets(const common::ObServer& server, const common::ObTabletReportInfoList& tablets, const int64_t time_stamp);
        int replay_remove_replica(const common::ObTabletReportInfo &replica);
        int receive_hb(const common::ObServer& server,common::ObRole role);
        common::ObServer get_update_server_info(bool use_inner_port);
        int get_master_ups(common::ObServer &ups_addr, bool use_inner_port);
        int64_t get_merge_delay_interval() const;
        int table_exist_in_cs(const uint64_t table_id, bool &is_exist);
        int split_table_range(const int64_t frozen_version, const uint64_t table_id,
            common::ObTabletInfoList &tablets);
        int create_tablet(const common::ObTabletInfoList& tablets);
        int create_tablet_with_range(const int64_t frozen_version, 
            const common::ObTabletInfoList& tablets);
        int create_empty_tablet_with_range(const int64_t frozen_version, 
            ObRootTable2 *root_table, const common::ObTabletInfo &tablet,
            int32_t& created_count, int* t_server_index);
        uint64_t get_table_info(const common::ObString& table_name, int32_t& max_row_key_length) const;
        int get_table_info(const uint64_t table_id, common::ObString& table_name, int32_t& max_row_key_length) const;

        int get_server_status() const;
        int64_t get_time_stamp_changing() const;
        int64_t get_lease() const;
        int get_server_index(const common::ObServer& server) const;

        int get_cs_info(ObChunkServerManager* out_server_manager) const;

        void clean_daily_merge_tablet_error();
        void set_daily_merge_tablet_error();
        bool is_daily_merge_tablet_error() const;
        void print_alive_server() const;
        bool is_master() const;
        void dump_root_table() const;
        int dump_cs_tablet_info(const common::ObServer cs, int64_t &tablet_num) const;
        void dump_unusual_tablets() const;
        int check_tablet_version(const int64_t tablet_version, bool &is_merged) const;
        int use_new_schema();
        int do_check_point(const uint64_t ckpt_id); // dump current root table and chunkserver list into file

        int recover_from_check_point(const int server_status, const uint64_t ckpt_id); // recover root table and chunkserver list from file
       int try_create_new_table(int64_t frozen_version, const uint64_t table_id); 
       int try_create_new_table(int64_t fetch_version); 
       int check_tablets_legality(const common::ObTabletInfoList &tablets); 
       int receive_new_frozen_version(int64_t rt_version, const int64_t frozen_version,
            const int64_t last_frozen_time, bool did_replay);
        int report_frozen_memtable(const int64_t frozen_version, const int64_t last_frozen_time,bool did_replay);
        // 用于slave启动过程中的同步
        void wait_init_finished();
        const common::ObiRole& get_obi_role() const;
        int set_obi_role(const common::ObiRole& role);
        int get_obi_config(common::ObiConfig& obi_config) const;
        int get_master_ups_config(int32_t &master_master_ups_read_percent, int32_t &slave_master_ups_read_percent) const;
        int set_obi_config(const common::ObiConfig& conf);
        int set_obi_config(const common::ObServer &rs_addr, const common::ObiConfig& conf);
        const common::ObUpsList &get_ups_list() const;
        int set_ups_list(const common::ObUpsList &ups_list);
        const common::ObClientConfig& get_client_config() const;
        int set_client_config(const common::ObClientConfig &client_conf);
        int do_stat(int stat_key, char *buf, const int64_t buf_len, int64_t& pos);
        int register_ups(const common::ObServer &addr, int32_t inner_port, int64_t log_seq_num, int64_t lease);
        int receive_ups_heartbeat_resp(const common::ObServer &addr, ObUpsStatus stat,
                                       const common::ObiRole &obi_role);
        int ups_slave_failure(const common::ObServer &addr, const common::ObServer &slave_addr);
        int get_ups_list(common::ObUpsList &ups_list);
        int set_ups_config(const common::ObServer &ups, int32_t ms_read_percentage, int32_t cs_read_percentage);
        int set_ups_config(int32_t read_master_master_ups_percentage, int32_t read_slave_master_ups_percentage);
        int change_ups_master(const common::ObServer &ups, bool did_force);
        int serialize_cs_list(char* buf, const int64_t buf_len, int64_t& pos) const;
        int serialize_ms_list(char* buf, const int64_t buf_len, int64_t& pos) const;
        int serialize_proxy_list(char* buf, const int64_t buf_len, int64_t& pos) const;
        int grant_eternal_ups_lease();
        int cs_import_tablets(const uint64_t table_id, const int64_t tablet_version);
        void dump_schema_manager();
        void dump_migrate_info() const; // for monitor
        int shutdown_cs(const common::ObArray<common::ObServer> &servers, enum ShutdownOperation op);
        void restart_all_cs();
        void cancel_restart_all_cs();
        int cancel_shutdown_cs(const common::ObArray<common::ObServer> &servers, enum ShutdownOperation op);
        void reset_hb_time();
        int64_t get_last_frozen_version() const;
        
        friend class ObRootServerTester;
        friend class ObRootLogWorker;
        friend class ::ObBalanceTest;
        friend class ::ObBalanceTest_test_n_to_2_Test;
        friend class ::ObBalanceTest_test_timeout_Test;
        friend class ::ObBalanceTest_test_rereplication_Test;
        friend class ::ObBalanceTest_test_n_to_2_with_faulty_dest_cs_Test;
        friend class ::ObDeleteReplicasTest_delete_in_init_Test;
        friend class ::ObDeleteReplicasTest_delete_when_rereplication_Test;
        friend class ::ObDeleteReplicasTest_delete_when_report_Test;
        friend class ::ObBalanceTest_test_shutdown_servers_Test;
      public:
        /*
         * 收到汇报消息后调用
         */
        int got_reported(const common::ObTabletReportInfoList& tablets, const int server_index, const int64_t frozen_mem_version);
      private:
        /*
         * 系统初始化的时候, 处理汇报消息, 
         * 信息放到root table for build 结构中
         */
        int got_reported_for_build(const common::ObTabletInfo& tablet, 
                                   const int32_t server_index, const int64_t version);
        /*
         * 处理汇报消息, 直接写到当前的root table中
         * 如果发现汇报消息中有对当前root table的tablet的分裂或者合并
         * 要调用采用写拷贝机制的处理函数
         */
        int got_reported_for_query_table(const common::ObTabletReportInfoList& tablets, 
                                         const int32_t server_index, const int64_t frozen_mem_version);
        /*
         * 写拷贝机制的,处理汇报消息
         */
        int got_reported_with_copy(const common::ObTabletReportInfoList& tablets, 
                                   const int32_t server_index, const int64_t have_done_index);

        int create_new_table(common::ObSchemaManagerV2* schema, ObRootTable2* root_table);
        int slave_create_new_table(const common::ObTabletInfo& tablet, const int32_t* t_server_index, const int32_t replicas_num, const int64_t mem_version);
        int slave_batch_create_new_table(const common::ObTabletInfoList& tablets,
            int32_t** t_server_index,
            int32_t* replicas_num,
            const int64_t mem_version);
        void get_available_servers_for_new_table(int* server_index,
            int32_t expected_num, int32_t &results_num);
        int get_deleted_tables(const common::ObSchemaManagerV2 &old_schema, 
                               const common::ObSchemaManagerV2 &new_schema, 
                               common::ObArray<uint64_t> &deleted_tables);
        int delete_tablets_from_root_table(const common::ObArray<uint64_t> &deleted_tables);
        /*
         * 生成查询的输出cell
         */
        int make_out_cell(common::ObCellInfo& out_cell, ObRootTable2::const_iterator start, 
                          ObRootTable2::const_iterator end, common::ObScanner& scanner, const int32_t max_row_count,
                          const int32_t max_key_len) const;

        const char* str_rs_status(int status) const;
        
        // stat related functions
        void do_stat_start_time(char *buf, const int64_t buf_len, int64_t& pos);
        void do_stat_local_time(char *buf, const int64_t buf_len, int64_t& pos);
        void do_stat_common(char *buf, const int64_t buf_len, int64_t& pos);
        void do_stat_schema_version(char* buf, const int64_t buf_len, int64_t &pos);
        void do_stat_mem(char* buf, const int64_t buf_len, int64_t &pos);
        void do_stat_table_num(char* buf, const int64_t buf_len, int64_t &pos);
        void do_stat_tablet_num(char* buf, const int64_t buf_len, int64_t &pos);
        void do_stat_cs(char* buf, const int64_t buf_len, int64_t &pos);
        void do_stat_ms(char* buf, const int64_t buf_len, int64_t &pos);
        void do_stat_ups(char* buf, const int64_t buf_len, int64_t &pos);
        void do_stat_all_server(char* buf, const int64_t buf_len, int64_t &pos);
        void do_stat_frozen_time(char* buf, const int64_t buf_len, int64_t &pos);
        void do_stat_client_config(char* buf, const int64_t buf_len, int64_t &pos);
        int64_t get_stat_value(const int32_t index);
        void do_stat_cs_num(char* buf, const int64_t buf_len, int64_t &pos);
        void do_stat_ms_num(char* buf, const int64_t buf_len, int64_t &pos);
        void do_stat_merge(char* buf, const int64_t buf_len, int64_t &pos);
        void do_stat_unusual_tablets_num(char* buf, const int64_t buf_len, int64_t &pos);
        int make_checkpointing();
        void switch_root_table(ObRootTable2 *rt, ObTabletInfoManager *ti);
        void switch_schema_manager(common::ObSchemaManagerV2 *schema_manager);
        int init_root_table_by_report();
        /*
         * 在一个tabelt的各份拷贝中, 寻找合适的备份替换掉
         */
        int write_new_info_to_root_table(
          const common::ObTabletInfo& tablet_info, const int64_t tablet_version, const int32_t server_index, 
          ObRootTable2::const_iterator& first, ObRootTable2::const_iterator& last, ObRootTable2 *p_root_table);

        bool all_tablet_is_the_last_frozen_version() const;
        bool all_tablet_is_the_last_frozen_version_really() const;
        int load_client_config(tbsys::CConfig &config);        
        int create_root_table_for_build();
        DISALLOW_COPY_AND_ASSIGN(ObRootServer2);

      private:
        common::ObClientHelper client_helper_;
        ObRootConfig &config_;
        ObChunkServerManager server_manager_;
        mutable tbsys::CRWLock server_manager_rwlock_;

        common::ObSchemaManagerV2* schema_manager_; 
        mutable tbsys::CRWLock schema_manager_rwlock_;

        int64_t time_stamp_changing_;
        int64_t frozen_mem_version_;

        mutable tbsys::CThreadMutex root_table_build_mutex_; //any time only one thread can modify root_table
        //ObRootTable one for query 
          //another for receive reporting and build new one
        ObRootTable2* root_table_; 
        ObTabletInfoManager* tablet_manager_;
        mutable tbsys::CRWLock root_table_rwlock_; //every query root table should rlock this

        ObRootTable2* root_table_for_build_; 
        ObTabletInfoManager* tablet_manager_for_build_;
        common::ObTabletReportInfoList delete_list_;

        bool have_inited_;
        int server_status_;
        mutable tbsys::CThreadMutex status_mutex_; 
        mutable int build_sync_flag_;
        bool first_cs_had_registed_;
        volatile bool receive_stop_;

        mutable tbsys::CThreadMutex frozen_version_mutex_;
        int64_t last_frozen_mem_version_;
        int64_t last_frozen_time_;
        int64_t next_select_cs_index_;

        OBRootWorker* worker_;//who does the net job
        ObRootLogWorker* log_worker_;

        common::ObiRole obi_role_;        // my role as oceanbase instance
        common::ObServer my_addr_;
       
        time_t start_time_;
        int64_t latest_config_version_; // update when all_sys_param changed
        // ups related        
        ObUpsManager *ups_manager_;
        ObUpsHeartbeatRunnable *ups_heartbeat_thread_;
        ObUpsCheckRunnable *ups_check_thread_;
        // balance related
        ObRootBalancer *balancer_;
        ObRootBalancerRunnable *balancer_thread_;
        ObRestartServer *restart_server_;

        //for merge_error msg
        bool is_daily_merge_tablet_error_;

        // flag constants
        static const int MIN_BALANCE_TOLERANCE = 1;
      protected:
        class rootTableModifier : public tbsys::CDefaultRunnable
        {
          public:
            explicit rootTableModifier(ObRootServer2* root_server);
            void run(tbsys::CThread *thread, void *arg);
          private:
            ObRootServer2* root_server_;
        };//report, switch schema and balance job
        
        class MergeChecker : public tbsys::CDefaultRunnable
        {
        public:
          explicit MergeChecker(ObRootServer2* root_server);
          void run(tbsys::CThread *thread, void *arg);
        private:
          ObRootServer2* root_server_;
        };

        rootTableModifier root_table_modifier_;
        MergeChecker merge_checker_;

        class heartbeatChecker : public tbsys::CDefaultRunnable
      {
        public:
          explicit heartbeatChecker(ObRootServer2* root_server);
          void run(tbsys::CThread *thread, void *arg);
        private:
          ObRootServer2* root_server_;
      };
        heartbeatChecker heart_beat_checker_;
    };
  }
}

#endif
