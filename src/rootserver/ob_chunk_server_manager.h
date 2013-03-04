/*===============================================================
*   (C) 2007-2010 Taobao Inc.
*   
*   
*   Version: 0.1 2010-09-19
*   
*   Authors:
*          daoan(daoan@taobao.com)
*   
*
================================================================*/
#ifndef OCEANBASE_ROOTSERVER_OB_CHUNK_SERVER_MANAGER_H_
#define OCEANBASE_ROOTSERVER_OB_CHUNK_SERVER_MANAGER_H_
#include "common/ob_array_helper.h"
#include "common/ob_server.h"
#include "rootserver/ob_server_balance_info.h"
#include "rootserver/ob_migrate_info.h"
#include "common/ob_array.h"
namespace oceanbase 
{ 
  namespace rootserver 
  {
    // shutdown具体执行的操作:shutdown, restart
    enum ShutdownOperation
    {
      SHUTDOWN = 0,
      RESTART,
    };

    struct ObBalanceInfo
    {      
      /// total size of all sstables for one particular table in this CS
      int64_t table_sstable_total_size_;
      /// total count of all sstables for one particular table in this CS
      int64_t table_sstable_count_;
      /// the count of currently migrate-in tablets
      int32_t curr_migrate_in_num_;
      /// the count of currently migrate-out tablets
      int32_t curr_migrate_out_num_;
      ObCsMigrateTo migrate_to_;
      
      ObBalanceInfo();
      ~ObBalanceInfo();
      void reset();
      void reset_for_table();
    };
    
    const int64_t CHUNK_LEASE_DURATION = 1000000;         
    const int16_t CHUNK_SERVER_MAGIC = static_cast<int16_t>(0xCDFF);
    /* schema 切换过程与 Sever的状态
     * 切换开始前, 所有的机器处于 SERVING 状态.
     * root server向所有活着的节点发送start new schema 命令,
     * 确认节点收到命令后, 状态转成 STATUS_REPORTING.
     * 所有节点汇报自己的信息, 汇报完成后, 节点发送汇报完成命令
     * 收到汇报完成命令, 更改该节点状态到 STATUS_REPORTED
     * 所有的活节点都从STATUS_REPORTING 到 STATUS_REPORTED 后
     * 发送切换命令. 确认收到切换命令的节点, 状态变成 STATUS_SERVING.
     * 所有节点切换后, 且root table
     * 过程中注意死去的节点以及后加入的节点
     *
     * schema switch will change every server's status;
     * when this begins, every server has a statu,valued as STATUS_SERVING
     * then root server will get new schema and starts this swiching by
     * sending commmond prepair_star_new_schema to every server, who is alive.
     * when the server gets this command, and give his echo to root server, root 
     * server will changes the chunk server's status to STATUS_REPORTING.
     * so the chunk server can report his tablets to root server. when the chunk server
     * finish reporting, it will let the root server knows. root server will change chunk server's 
     * status to STATUS_REPORTED. when every alive chunk server has a status with STATUS_REPORTED.
     * root server can repacles his old root_table with the new one. then
     * the root server will give them the command star_new_schema, and change their status to 
     * STATUS_SERVING.
     *
     * we must deal with the chunk server died during this process or the new chunk server joined 
     * in this process.
     *
     *
     */
    struct ObServerStatus
    {
      enum {
        STATUS_DEAD = 0,
        STATUS_WAITING_REPORT,
        STATUS_SERVING ,
        STATUS_REPORTING,
        STATUS_REPORTED,
        STATUS_SHUTDOWN,         // will be shut down
      };
      ObServerStatus();
      NEED_SERIALIZE_AND_DESERIALIZE;
      void set_hb_time(int64_t hb_t);
      void set_hb_time_ms(int64_t hb_t);
      bool is_alive(int64_t now, int64_t lease) const;
      bool is_ms_alive(int64_t now, int64_t lease) const;
      void dump(const int32_t index) const;
      const char* get_cs_stat_str() const;
      
      common::ObServer server_;
      volatile int64_t last_hb_time_;
      volatile int64_t last_hb_time_ms_;  //the last hb time of mergeserver,for compatible,we don't serialize this field
      int32_t ms_status_;
      int32_t status_;

      int32_t port_cs_; //chunk server port
      int32_t port_ms_; //merger server port

      int32_t hb_retry_times_;        //no need serialize

      ObServerDiskInfo disk_info_; //chunk server disk info
      int64_t register_time_;       // no need serialize
      //used in the new rebalance algorithm, don't serialize
      ObBalanceInfo balance_info_;
      bool wait_restart_;
      bool can_restart_; //all the tablet in this chunkserver has safe copy num replicas, means that this server can be restarted;

    };
    class ObChunkServerManager
    {
      public:
        enum {
          MAX_SERVER_COUNT = 1000,
        };

        typedef ObServerStatus* iterator;
        typedef const ObServerStatus* const_iterator;

        ObChunkServerManager();
        virtual ~ObChunkServerManager();

        iterator begin();
        const_iterator begin() const;
        iterator end();
        const_iterator end() const;

        iterator find_by_ip(const common::ObServer& server);
        const_iterator find_by_ip(const common::ObServer& server) const;

        /*
         * root server will call this when a server regist to root or echo heart beat
         * @return 1 new serve 2 relive server 0 heartbt
         */
        int receive_hb(const common::ObServer& server, int64_t time_stamp, bool is_merge_server = false, bool is_regist = false);
        int update_disk_info(const common::ObServer& server, const ObServerDiskInfo& disk_info);
        int get_array_length() const;
        ObServerStatus* get_server_status(const int32_t index);
        const ObServerStatus* get_server_status(const int32_t index) const;
        common::ObServer get_cs(const int32_t index) const;
        int get_server_index(const common::ObServer &server, int32_t &index) const;
        
        void set_server_down(iterator& it);
        void set_server_down_ms(iterator& it);
        void reset_balance_info(int32_t max_migrate_out_per_cs);
        void reset_balance_info_for_table(int32_t &cs_num, int32_t &shutdown_num);
        bool is_migrate_infos_full() const;
        int add_migrate_info(ObServerStatus& cs, const common::ObRange &range, int32_t dest_cs_idx);
        int add_copy_info(ObServerStatus& cs, const common::ObRange &range, int32_t dest_cs_idx);
        int32_t get_max_migrate_num() const;

        int shutdown_cs(const common::ObArray<common::ObServer> &servers, enum ShutdownOperation op);
        void restart_all_cs();
        void cancel_restart_all_cs();
        int cancel_shutdown_cs(const common::ObArray<common::ObServer> &servers, enum ShutdownOperation op);
        bool has_shutting_down_server() const;
        
        NEED_SERIALIZE_AND_DESERIALIZE;
        ObChunkServerManager& operator= (const ObChunkServerManager& other);
        int serialize_cs(const ObServerStatus *it, char* buf, const int64_t buf_len, int64_t& pos) const;
        int serialize_ms(const ObServerStatus *it, char* buf, const int64_t buf_len, int64_t& pos) const;
        int serialize_cs_list(char* buf, const int64_t buf_len, int64_t& pos) const;
        int serialize_ms_list(char* buf, const int64_t buf_len, int64_t& pos) const;
      public:
        int write_to_file(const char* filename);
        int read_from_file(const char* filename, int32_t &cs_num, int32_t &ms_num);
      private:
        ObServerStatus data_holder_[MAX_SERVER_COUNT];
        common::ObArrayHelper<ObServerStatus> servers_;
        ObMigrateInfos migrate_infos_;
    };
  }
}
#endif

