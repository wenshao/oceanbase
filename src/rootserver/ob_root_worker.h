/*===============================================================
*   (C) 2007-2010 Taobao Inc.
*
*
*   Version: 0.1 2010-09-26
*
*   Authors:
*          daoan(daoan@taobao.com)
*
*
================================================================*/
#ifndef OCEANBASE_ROOTSERVER_ROOT_WORKER_H_
#define OCEANBASE_ROOTSERVER_ROOT_WORKER_H_
#include "common/ob_define.h"
#include "common/ob_base_server.h"
#include "common/thread_buffer.h"
#include "common/ob_client_manager.h"
#include "common/ob_fetch_runnable.h"
#include "common/ob_role_mgr.h"
#include "common/ob_slave_mgr.h"
#include "common/ob_check_runnable.h"
#include "common/ob_packet_queue_thread.h"
#include "common/ob_packet_factory.h"
#include "rootserver/ob_root_server2.h"
#include "rootserver/ob_root_rpc_stub.h"
#include "rootserver/ob_root_log_replay.h"
#include "rootserver/ob_root_log_manager.h"
#include "rootserver/ob_root_stat.h"
#include "rootserver/ob_root_fetch_thread.h"
#include "rootserver/ob_root_config.h"

namespace oceanbase
{
  namespace common
  {
    class ObDataBuffer;
    class ObServer;
    class ObScanner;
    class ObTabletReportInfoList;
    class ObGetParam;
    class ObScanParam;
    class ObRange;
  }
  namespace rootserver
  {
    class OBRootWorker :public common::ObBaseServer, public tbnet::IPacketQueueHandler
    {
    public:
      OBRootWorker();
      virtual ~OBRootWorker();

      tbnet::IPacketHandler::HPRetCode handlePacket(tbnet::Connection *connection, tbnet::Packet *packet);
      bool handleBatchPacket(tbnet::Connection *connection, tbnet::PacketQueue &packetQueue);
      bool handlePacketQueue(tbnet::Packet *packet, void *args);

      int initialize();
      int start_service();
      void wait_for_queue();
      void destroy();

      bool start_merge();
      int set_config_file_name(const char* conf_file_name);
      int submit_delete_tablets_task(const common::ObTabletReportInfoList& delete_list);
      ObRootLogManager* get_log_manager();
      common::ObRoleMgr* get_role_manager();
      common::ThreadSpecificBuffer::Buffer* get_rpc_buffer() const;
      virtual ObRootRpcStub& get_rpc_stub();

      ObRootStatManager& get_stat_manager();

      int send_obi_role(common::ObiRole obi_role);
      common::ObClientManager* get_client_manager();
      int64_t get_network_timeout();
      common::ObServer get_rs_master();
      common::ThreadSpecificBuffer* get_thread_buffer();
    private:
      template <class Queue>
        int submit_async_task_(const common::PacketCode pcode, Queue &qthread, int32_t task_queue_size,
            const common::ObDataBuffer *data_buffer = NULL,
            const common::ObPacket *packet = NULL);
      template <class Queue>
        int submit_async_task_(const common::PacketCode pcode, Queue &qthread, int32_t task_queue_size,
            const int32_t version, common::ObDataBuffer& in_buff, tbnet::Connection* conn,
            const uint32_t channel_id, const int64_t timeout);

      // notice that in_buff can not be const.
      int rt_get_update_server_info(const int32_t version, common::ObDataBuffer& in_buff,
          tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff,
          bool use_inner_port = false);

      int rt_get_merge_delay_interval(const int32_t version, common::ObDataBuffer& in_buff,
          tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff);

      int rt_get(const int32_t version, common::ObDataBuffer& in_buff,
          tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff);

      int rt_scan(const int32_t version, common::ObDataBuffer& in_buff,
          tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff);

      int rt_fetch_schema(const int32_t version, common::ObDataBuffer& in_buff,
          tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff);

      int rt_fetch_schema_version(const int32_t version, common::ObDataBuffer& in_buff,
          tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff);

      int rt_report_tablets(const int32_t version, common::ObDataBuffer& in_buff,
          tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff);

      int rt_waiting_job_done(const int32_t version, common::ObDataBuffer& in_buff,
          tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff);

      int rt_register(const int32_t version, common::ObDataBuffer& in_buff,
          tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff);

      int rt_migrate_over(const int32_t version, common::ObDataBuffer& in_buff,
          tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff);

      int rt_report_capacity_info(const int32_t version, common::ObDataBuffer& in_buff,
          tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff);

      int rt_heartbeat(const int32_t version, common::ObDataBuffer& in_buff,
          tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff);

      int rt_dump_cs_info(const int32_t version, common::ObDataBuffer& in_buff,
          tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff);

      int rt_check_tablet_merged(const int32_t version, common::ObDataBuffer& in_buff,
          tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff);
      int rt_split_tablet(const int32_t version, common::ObDataBuffer& in_buff,
          tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff);

      int rt_fetch_stats(const int32_t version, common::ObDataBuffer& in_buff,
          tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff);

      int rt_ping(const int32_t version, common::ObDataBuffer& in_buff, tbnet::Connection* conn,
          const uint32_t channel_id, common::ObDataBuffer& out_buff);

      int rt_slave_quit(const int32_t version, common::ObDataBuffer& in_buff,
          tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff);

      int rt_update_server_report_freeze(const int32_t version, common::ObDataBuffer& in_buff,
          tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff);

      int rt_slave_register(const int32_t version, common::ObDataBuffer& in_buff,
          tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff);
      int rt_renew_lease(const int32_t version, common::ObDataBuffer& in_buff,
          tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff);
      int rt_grant_lease(const int32_t version, common::ObDataBuffer& in_buff,
          tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff);
      int rt_get_obi_role(const int32_t version, common::ObDataBuffer& in_buff,
          tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff);
      int rt_set_obi_role_to_slave(const int32_t version, common::ObDataBuffer& in_buff,
          tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff);
      int rt_set_obi_role(const int32_t version, common::ObDataBuffer& in_buff,
          tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff);
      int rt_get_last_frozen_version(const int32_t version, common::ObDataBuffer& in_buff,
          tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff);
      int rt_force_cs_to_report(const int32_t version, common::ObDataBuffer& in_buff,
          tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff);
      int rt_admin(const int32_t version, common::ObDataBuffer& in_buff,
          tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff);
      int rt_change_log_level(const int32_t version, common::ObDataBuffer& in_buff,
          tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff);
      int rt_stat(const int32_t version, common::ObDataBuffer& in_buff,
          tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff);

      int rs_dump_cs_tablet_info(const int32_t version, common::ObDataBuffer& in_buff,
          tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff);
      int rt_get_obi_config(const int32_t version, common::ObDataBuffer& in_buff,
          tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff);
      int rt_get_master_ups_config(const int32_t version, common::ObDataBuffer& in_buff,
          tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff);
      int rt_set_obi_config(const int32_t version, common::ObDataBuffer& in_buff,
          tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff);
      int rt_ups_heartbeat_resp(const int32_t version, common::ObDataBuffer& in_buff,
          tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff);
      int rt_ups_register(const int32_t version, common::ObDataBuffer& in_buff,
          tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff);
      int rt_ups_slave_failure(const int32_t version, common::ObDataBuffer& in_buff,
          tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff);
      int rt_get_ups(const int32_t version, common::ObDataBuffer& in_buff,
          tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff);
      int rt_set_ups_config(const int32_t version, common::ObDataBuffer& in_buff,
          tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff);
      int rt_set_master_ups_config(const int32_t version, common::ObDataBuffer& in_buff,
          tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff);
      int rt_change_ups_master(const int32_t version, common::ObDataBuffer& in_buff,
          tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff);
      int rt_get_client_config(const int32_t version, common::ObDataBuffer& in_buff,
          tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff);
      int rt_get_cs_list(const int32_t version, common::ObDataBuffer& in_buff,
          tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff);
      int rt_get_ms_list(const int32_t version, common::ObDataBuffer& in_buff,
          tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff);
      int rt_get_proxy_list(const int32_t version, common::ObDataBuffer& in_buff,
          tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff);
      int rt_cs_import_tablets(const int32_t version, common::ObDataBuffer& in_buff,
          tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff);
      int rt_shutdown_cs(const int32_t version, common::ObDataBuffer& in_buff,
          tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff);
      int rt_restart_cs(const int32_t version, common::ObDataBuffer& in_buff,
          tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff);
      int rt_cs_delete_tablets(const int32_t version, common::ObDataBuffer& in_buff,
          tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buff);
      int rt_delete_tablets(const int32_t version, common::ObDataBuffer& in_buff,
          tbnet::Connection* conn, const uint32_t channel_id,
          common::ObDataBuffer& out_buff);
      int start_as_master();
      int start_as_slave();

    private:
      int do_stat(int stat_key, char *buf, const int64_t buf_len, int64_t& pos);
      int do_admin_with_return(int admin_cmd);
      int do_admin_without_return(int admin_cmd);
      int slave_register_(common::ObFetchParam& fetch_param);
      int rt_slave_write_log(const int32_t version, common::ObDataBuffer& in_buffer,
          tbnet::Connection* conn, const uint32_t channel_id, common::ObDataBuffer& out_buffer);
      int get_obi_role_from_master();
    protected:
      ObRootConfig config_;
      common::ObPacketFactory packet_factory_;
      bool is_registered_;
      char config_file_name_[common::OB_MAX_FILE_NAME_LENGTH];
      ObRootServer2 root_server_;
      common::ObPacketQueueThread read_thread_queue_;
      common::ObPacketQueueThread write_thread_queue_;
      common::ObPacketQueueThread log_thread_queue_;
      common::ThreadSpecificBuffer my_thread_buffer;
      common::ObClientManager client_manager;

      common::ObServer rt_master_;
      common::ObServer self_addr_;
      common::ObRoleMgr role_mgr_;
      common::ObSlaveMgr slave_mgr_;
      common::ObCheckRunnable check_thread_;
      ObRootFetchThread fetch_thread_;

      ObRootRpcStub rt_rpc_stub_;
      ObRootLogReplay log_replay_thread_;
      ObRootLogManager log_manager_;
      ObRootStatManager stat_manager_;
    };

    inline ObRootRpcStub& OBRootWorker::get_rpc_stub()
    {
      return rt_rpc_stub_;
    }

    inline ObRootStatManager& OBRootWorker::get_stat_manager()
    {
      return stat_manager_;
    }

  }
}

#endif

