/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 * 
 * Version: $Id$
 *
 * ob_root_balancer.h
 * rebalance and re-replication functions
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#ifndef _OB_ROOT_BALANCER_H
#define _OB_ROOT_BALANCER_H 1
#include "common/ob_schema.h"
#include "common/ob_tablet_info.h"
#include "ob_root_config.h"
#include "ob_root_table2.h"
#include "ob_chunk_server_manager.h"
#include "ob_root_rpc_stub.h"
#include "ob_root_stat.h"
#include "ob_root_log_worker.h"
#include "ob_restart_server.h"
#include "tbsys.h"

namespace oceanbase
{
  namespace rootserver
  {
    enum RereplicationAction
    {
      RA_NOP = 0,
      RA_COPY = 1,
      RA_DELETE = 2
    };

    class ObRootBalancer
    {
      public:
        ObRootBalancer();
        virtual ~ObRootBalancer();
        void set_config(ObRootConfig *config);
        void set_root_table(ObRootTable2 *root_table);
        void set_root_table_lock(tbsys::CRWLock *root_table_rwlock);
        void set_server_manager(ObChunkServerManager *server_manager);
        void set_server_manager_lock(tbsys::CRWLock *server_manager_rwlock);
        void set_schema_manager(common::ObSchemaManagerV2 *schema_manager);
        void set_schema_manager_lock(tbsys::CRWLock *schema_manager_rwlock);
        void set_root_table_build_mutex(tbsys::CThreadMutex *root_table_build_mutex);
        void set_rpc_stub(ObRootRpcStub *rpc_stub);
        void set_stat_manager(ObRootStatManager *stat_manager);
        void set_log_worker(ObRootLogWorker* log_worker);
        void set_restart_server(ObRestartServer* restart_server);
        void set_role_mgr(common::ObRoleMgr *role_mgr);
        
        void do_balance(bool &did_migrating);

        int nb_restart_servers();
        int nb_trigger_next_migrate(ObRootTable2::iterator it, const common::ObTabletInfo* tablet,
                                    int32_t src_cs_idx, int32_t dest_cs_idx, bool keep_src);
        // monitor functions        
        void nb_print_balance_infos(char *buf, const int64_t buf_len, int64_t& pos); // for monitor
        void dump_migrate_info() const; // for monitor
        void nb_print_shutting_down_progress(char *buf, const int64_t buf_len, int64_t& pos); // for monitor

        // testing functions        
        bool nb_is_all_tables_balanced(const common::ObServer &except_cs); // only for testing
        bool nb_is_all_tablets_replicated(int32_t expected_replicas_num);    // only for testing
        void nb_find_can_restart_server(int32_t expected_replicas_num);
        int nb_calculate_sstable_count(const uint64_t table_id, int64_t &avg_size, int64_t &avg_count, 
                                       int32_t &cs_num, int32_t &migrate_out_per_cs, int32_t &shutdown_count); // public only for testing
        bool nb_did_cs_have_no_tablets(const common::ObServer &cs) const;
      private:
        //check wether shutdown_cs is migrate clean
        void check_shutdown_process();
        void check_components();
        void do_new_balance();
        int nb_balance_by_table(const uint64_t table_id, bool &scan_next_table);
        int do_rereplication_by_table(const uint64_t table_id, bool &scan_next_table);
        int nb_find_dest_cs(ObRootTable2::const_iterator meta, int64_t low_bound, int32_t cs_num, 
                            int32_t &dest_cs_idx, ObChunkServerManager::iterator &dest_it);
        uint64_t nb_get_next_table_id(int32_t table_count, int32_t seq = -1);
        int32_t nb_get_table_count();
        int send_msg_migrate(const common::ObServer &src, const common::ObServer &dest, const common::ObRange& range, bool keep_src);
        int nb_start_batch_migrate();
        int nb_check_migrate_timeout();
        bool nb_is_in_batch_migrating();
        void nb_batch_migrate_done();
        bool nb_is_curr_table_balanced(int64_t avg_sstable_count, const common::ObServer &except_cs) const;
        bool nb_is_curr_table_balanced(int64_t avg_sstable_count) const;        
        void nb_print_balance_info() const;
        void nb_print_migrate_infos() const;
        // @return 0 do not copy, 1 copy immediately, -1 delayed copy
        int need_copy(int32_t available_num, int32_t lost_num);
        int nb_add_copy(ObRootTable2::const_iterator it, const common::ObTabletInfo* tablet, int64_t low_bound, int32_t cs_num);
        int nb_del_copy(ObRootTable2::const_iterator it, const common::ObTabletInfo* tablet);
        int nb_select_copy_src(ObRootTable2::const_iterator it, 
                               int32_t &src_cs_idx, ObChunkServerManager::iterator &src_it);
        int nb_check_rereplication(ObRootTable2::const_iterator it, RereplicationAction &act);
        int nb_check_add_migrate(ObRootTable2::const_iterator it, const common::ObTabletInfo* tablet, int64_t avg_count, 
                                 int32_t cs_num, int32_t migrate_out_per_cs);
         
        bool nb_is_all_tables_balanced(); // only for testing
         
        void nb_print_balance_info(char *buf, const int64_t buf_len, int64_t& pos) const; // for monitor
        // disallow copy
        ObRootBalancer(const ObRootBalancer &other);
        ObRootBalancer& operator=(const ObRootBalancer &other);
      private:
        // constants        
        static const int32_t MAX_MAX_MIGRATE_OUT_PER_CS = 60;
        // data members
        ObRootConfig *config_;
        ObRootTable2 *root_table_;
        ObChunkServerManager *server_manager_;
        common::ObSchemaManagerV2 *schema_manager_;
        tbsys::CRWLock *root_table_rwlock_;
        tbsys::CRWLock *server_manager_rwlock_;
        tbsys::CRWLock *schema_manager_rwlock_;
        tbsys::CThreadMutex *root_table_build_mutex_;
        ObRootRpcStub *rpc_stub_;
        ObRootStatManager *stat_manager_;
        ObRootLogWorker* log_worker_;
        common::ObRoleMgr *role_mgr_;
        ObRestartServer* restart_server_;
        
        common::ObTabletReportInfoList delete_list_;
        int64_t balance_start_time_us_;
        int64_t balance_timeout_us_;
        int64_t balance_last_migrate_succ_time_;
        int32_t balance_next_table_seq_;
        int32_t balance_batch_migrate_count_;
        int32_t balance_batch_migrate_done_num_;
        int32_t balance_select_dest_start_pos_;
        int32_t balance_batch_copy_count_; // for monitor purpose
    };

    inline void ObRootBalancer::set_config(ObRootConfig *config)
    {
      config_ = config;
    }
    inline void ObRootBalancer::set_root_table(ObRootTable2 *root_table)
    {
      root_table_ = root_table;
    }
    inline void ObRootBalancer::set_root_table_lock(tbsys::CRWLock *root_table_rwlock)
    {
      root_table_rwlock_ = root_table_rwlock;
    }
    inline void ObRootBalancer::set_server_manager(ObChunkServerManager *server_manager)
    {
      server_manager_ = server_manager;
    }
    inline void ObRootBalancer::set_server_manager_lock(tbsys::CRWLock *server_manager_rwlock)
    {
      server_manager_rwlock_ = server_manager_rwlock;
    }
    inline void ObRootBalancer::set_schema_manager(common::ObSchemaManagerV2 *schema_manager)
    {
      schema_manager_ = schema_manager;
    }
    inline void ObRootBalancer::set_schema_manager_lock(tbsys::CRWLock *schema_manager_rwlock)
    {
      schema_manager_rwlock_ = schema_manager_rwlock;
    }
    inline void ObRootBalancer::set_root_table_build_mutex(tbsys::CThreadMutex *root_table_build_mutex)
    {
      root_table_build_mutex_ = root_table_build_mutex;
    }
    inline void ObRootBalancer::set_rpc_stub(ObRootRpcStub *rpc_stub)
    {
      rpc_stub_ = rpc_stub;
    }
    inline void ObRootBalancer::set_stat_manager(ObRootStatManager *stat_manager)
    {
      stat_manager_ = stat_manager;
    }
    inline void ObRootBalancer::set_log_worker(ObRootLogWorker* log_worker)
    {
      log_worker_ = log_worker;
    }
    inline void ObRootBalancer::set_restart_server(ObRestartServer* restart_server)
    {
      restart_server_ = restart_server;
    }
    inline void ObRootBalancer::set_role_mgr(common::ObRoleMgr *role_mgr)
    {
      role_mgr_ = role_mgr;
    }
  } // end namespace rootserver
} // end namespace oceanbase

#endif /* _OB_ROOT_BALANCER_H */

