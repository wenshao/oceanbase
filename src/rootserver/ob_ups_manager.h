/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 * 
 * Version: $Id$
 *
 * ob_ups_manager.h
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#ifndef _OB_UPS_MANAGER_H
#define _OB_UPS_MANAGER_H 1
#include "common/ob_server.h"
#include "common/ob_ups_info.h"
#include "common/ob_ups_info.h"
#include "rootserver/ob_root_rpc_stub.h"
#include <tbsys.h>

// forward declarations for unit test classes
class ObUpsManagerTest_test_basic_Test;
class ObUpsManagerTest_test_register_lease_Test;
class ObUpsManagerTest_test_register_lease2_Test;
class ObUpsManagerTest_test_offline_Test;
class ObUpsManagerTest_test_read_percent_Test;
class ObUpsManagerTest_test_read_percent2_Test;
namespace oceanbase
{
  namespace rootserver
  {
    enum ObUpsStatus
    {
      UPS_STAT_OFFLINE = 0,     ///< the ups is offline
      UPS_STAT_NOTSYNC = 1,     ///< slave & not sync
      UPS_STAT_SYNC = 2,        ///< slave & sync
      UPS_STAT_MASTER = 3,      ///< the master
    };
    struct ObUps
    {
      common::ObServer addr_;
      int32_t inner_port_;
      ObUpsStatus stat_;
      common::ObiRole obi_role_;
      int64_t log_seq_num_;
      int64_t lease_;
      int32_t ms_read_percentage_;
      int32_t cs_read_percentage_;
      bool did_renew_received_;
      ObUps()
        :inner_port_(0), stat_(UPS_STAT_OFFLINE), 
         log_seq_num_(0), lease_(0), 
         ms_read_percentage_(0), cs_read_percentage_(0),
         did_renew_received_(false)
      {
      }
      void reset();
      void convert_to(common::ObUpsInfo &ups_info) const;
    };
    class ObUpsManager
    {
      public:
        ObUpsManager(ObRootRpcStub &rpc_stub, ObRootConfig &config, const common::ObiRole &obi_role);
        virtual ~ObUpsManager();

        int get_ups_master(ObUps &ups_master);
        void reset_ups_read_percent();
        void print(char* buf, const int64_t buf_len, int64_t &pos);
        int get_ups_list(common::ObUpsList &ups_list);
        int register_ups(const common::ObServer &addr, int32_t inner_port, int64_t log_seq_num, int64_t lease);
        int renew_lease(const common::ObServer &addr, ObUpsStatus stat, const common::ObiRole &obi_role);
        int slave_failure(const common::ObServer &addr, const common::ObServer &slave_addr);
        int set_ups_master(const common::ObServer &master, bool did_force);
        int set_ups_config(const common::ObServer &addr, int32_t ms_read_percentage, int32_t cs_read_percentage);
        int set_ups_config(int32_t read_master_master_ups_percentage, int32_t read_slave_master_ups_percentage);
        void get_master_ups_config(int32_t &master_master_ups_read_percent, int32_t &slave_master_ups_read_precent) const;
        int send_obi_role();

        int grant_lease(bool did_force = false);     // for heartbeat thread
        int grant_eternal_lease();     // for heartbeat thread
        int check_lease();    // for check thread
        int check_ups_master_exist(); // for check thread
      private:
        int find_ups_index(const common::ObServer &addr) const;
        bool did_ups_exist(const common::ObServer &addr) const;
        bool is_ups_master(const common::ObServer &addr) const;
        bool has_master() const;
        bool is_master_lease_valid() const;
        bool need_grant(int64_t now, const ObUps &ups) const;
        int send_granting_msg(const common::ObServer &addr,
                                 const common::ObServer& master, int64_t lease);
        int select_new_ups_master();
        int select_ups_master_with_highest_lsn();
        void update_ups_lsn();
        bool is_ups_with_highest_lsn(int ups_idx);
        int revoke_master_lease(int64_t &waiting_lease_us);
        int send_revoking_msg(const common::ObServer &addr, int64_t lease, const common::ObServer& master);
        const char* ups_stat_to_cstr(ObUpsStatus stat) const;
        void check_all_ups_offline();
        bool is_idx_valid(int ups_idx) const;
        // disallow copy
        ObUpsManager(const ObUpsManager &other);
        ObUpsManager& operator=(const ObUpsManager &other);
        // for unit test
        int32_t get_ups_count() const;
        int32_t get_active_ups_count() const;
        friend class ::ObUpsManagerTest_test_basic_Test;
        friend class ::ObUpsManagerTest_test_register_lease_Test;
        friend class ::ObUpsManagerTest_test_register_lease2_Test;
        friend class ::ObUpsManagerTest_test_offline_Test;
        friend class ::ObUpsManagerTest_test_read_percent_Test;
        friend class ::ObUpsManagerTest_test_read_percent2_Test;
      private:
        ObRootConfig *config_;
        static const int32_t MAX_UPS_COUNT = 17;
        static const int64_t MAX_CLOCK_SKEW_US = 200000LL; // 200ms
        // data members
        ObRootRpcStub &rpc_stub_;
        const common::ObiRole &obi_role_;
        tbsys::CThreadMutex ups_array_mutex_;
        ObUps ups_array_[MAX_UPS_COUNT];
        int32_t ups_master_idx_;
        int64_t waiting_ups_finish_time_;
        int32_t master_master_ups_read_percentage_;
        int32_t slave_master_ups_read_percentage_;
        bool is_flow_control_by_ip_;
    };
  } // end namespace rootserver
} // end namespace oceanbase

#endif /* _OB_UPS_MANAGER_H */

