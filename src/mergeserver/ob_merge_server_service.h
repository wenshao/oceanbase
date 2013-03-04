#ifndef OCEANBASE_MERGESERVER_SERVICE_H_
#define OCEANBASE_MERGESERVER_SERVICE_H_

#include "tbnet.h"
#include "common/ob_define.h"
#include "common/data_buffer.h"
#include "common/ob_obi_role.h"
#include "common/thread_buffer.h"
#include "common/ob_session_mgr.h"
#include "common/ob_nb_accessor.h"
#include "ob_ms_schema_task.h"
#include "ob_ms_config_task.h"
#include "ob_ms_monitor_task.h"
#include "ob_ms_lease_task.h"
#include "ob_ms_ups_task.h"
#include "ob_ms_version_proxy.h"
#include "ob_merge_join_agent_imp.h"
#include "ob_query_cache.h"

namespace oceanbase
{
  namespace mergeserver
  {
    class ObMergeServer;
    class ObMergerRpcProxy;
    class ObMergerRootRpcProxy;
    class ObMergerRpcStub;
    class ObMergerAsyncRpcStub;
    class ObMergerSchemaProxy;
    class ObMergerSchemaManager;
    class ObMergerConfigProxy;
    class ObMergerConfigManager;
    class ObMergerServiceMonitor;
    class ObMergerTabletLocationCache;
    class ObMergerLocationCacheProxy;
    static const int32_t RESPONSE_PACKET_BUFFER_SIZE = 1024*1024*2; //2MB
    static const int64_t RETRY_INTERVAL_TIME = 1000 * 1000; // usleep 1 s
    class ObMergeServerService
    {
    public:
      ObMergeServerService();
      ~ObMergeServerService();

    public:
      int initialize(ObMergeServer* merge_server);
      int start();
      int destroy();
    public:
      /// extend lease valid time = sys.cur_timestamp + delay
      void extend_lease(const int64_t delay);

      /// check lease expired
      bool check_lease(void) const;

      /// register to root server
      int register_root_server(void);

      int do_request(
                    const int64_t receive_time,
                    const int32_t packet_code,
                    const int32_t version,
                    const int32_t channel_id,
                    tbnet::Connection* connection,
                    common::ObDataBuffer& in_buffer,
                    common::ObDataBuffer& out_buffer,
                    const int64_t timeout_us);

      //int get_agent(ObMergeJoinAgent *&agent);
      void handle_failed_request(const int64_t timeout, const int32_t packet_code);
    private:
      // lease init 20s
      static const int64_t DEFAULT_LEASE_TIME = 20 * 1000 * 1000L;

      // warning: fetch schema interval can not be too long
      // because of the heartbeat handle will block tbnet thread
      static const int64_t FETCH_SCHEMA_INTERVAL = 30 * 1000;

      // list sessions
      int ms_list_sessions(
                 const int32_t version,
                 const int32_t channel_id,
                 tbnet::Connection* connection,
                 common::ObDataBuffer& in_buffer,
                 common::ObDataBuffer& out_buffer);
      // list sessions
      int ms_kill_session(
                 const int32_t version,
                 const int32_t channel_id,
                 tbnet::Connection* connection,
                 common::ObDataBuffer& in_buffer,
                 common::ObDataBuffer& out_buffer);
      // heartbeat
      int ms_heartbeat(
                 const int64_t receive_time,
                 const int32_t version,
                 const int32_t channel_id,
                 tbnet::Connection* connection,
                 common::ObDataBuffer& in_buffer,
                 common::ObDataBuffer& out_buffer,
                 const int64_t timeout_us);
      // clear cache
      int ms_clear(
                 const int64_t receive_time,
                 const int32_t version,
                 const int32_t channel_id,
                 tbnet::Connection* connection,
                 common::ObDataBuffer& in_buffer,
                 common::ObDataBuffer& out_buffer,
                 const int64_t timeout_us);
      // monitor stat
      int ms_stat(
                 const int64_t receive_time,
                 const int32_t version,
                 const int32_t channel_id,
                 tbnet::Connection* connection,
                 common::ObDataBuffer& in_buffer,
                 common::ObDataBuffer& out_buffer,
                 const int64_t timeout_us);
      // reload conf
      int ms_reload(
                 const int64_t receive_time,
                 const int32_t version,
                 const int32_t channel_id,
                 tbnet::Connection* connection,
                 common::ObDataBuffer& in_buffer,
                 common::ObDataBuffer& out_buffer,
                 const int64_t timeout_us);
      // get query
      int ms_get(
                 const int64_t receive_time,
                 const int32_t version,
                 const int32_t channel_id,
                 tbnet::Connection* connection,
                 common::ObDataBuffer& in_buffer,
                 common::ObDataBuffer& out_buffer,
                 const int64_t timeout_us);
      // scan query
      int ms_scan(
                 const int64_t receive_time,
                 const int32_t version,
                 const int32_t channel_id,
                 tbnet::Connection* connection,
                 common::ObDataBuffer& in_buffer,
                 common::ObDataBuffer& out_buffer,
                 const int64_t timeout_us);
      int do_timeouted_req(
                          const int32_t version,
                          const int32_t channel_id,
                          tbnet::Connection* connection,
                          common::ObDataBuffer& in_buffer,
                          common::ObDataBuffer& out_buffer);

      // mutate update
      int ms_mutate(
                 const int64_t receive_time,
                 const int32_t version,
                 const int32_t channel_id,
                 tbnet::Connection* connection,
                 common::ObDataBuffer& in_buffer,
                 common::ObDataBuffer& out_buffer,
                 const int64_t timeout_us);

      // change log level
      int ms_change_log_level(
                 const int64_t receive_time,
                 const int32_t version,
                 const int32_t channel_id,
                 tbnet::Connection* connection,
                 common::ObDataBuffer& in_buffer,
                 common::ObDataBuffer& out_buffer,
                 const int64_t timeout_us);
    private:
      // init server properties
      int init_ms_properties_();
      friend class ObMergerConfigTask;
      // reload config
      int reload_config(const char * config_file);
      // check read master role
      bool check_instance_role(const bool read_master) const;

    private:
      DISALLOW_COPY_AND_ASSIGN(ObMergeServerService);
      ObMergeServer* merge_server_;
      bool inited_;
      // is registered or not
      volatile bool registered_;
      // lease timeout time
      int64_t lease_expired_time_;
      // instance role type
      common::ObiRole instance_role_;

    private:
      static const int64_t REQUEST_EVENT_QUEUE_SIZE = 8192;
      static const uint64_t MAX_ACCESS_RS_POINT = 10;
      //
      ObMergerRpcProxy  *rpc_proxy_;
      ObMergerRpcStub   *rpc_stub_;
      ObMergerAsyncRpcStub   *async_rpc_;
      ObMergerSchemaManager *schema_mgr_;
      ObMergerConfigManager *config_mgr_;
      ObMergerSchemaProxy *schema_proxy_;
      ObMergerConfigProxy *config_proxy_;
      oceanbase::common::ObNbAccessor *nb_accessor_;
      ObMergerRootRpcProxy * root_rpc_;
      ObMergerUpsTask fetch_ups_task_;
      ObMergerSchemaTask fetch_schema_task_;
      ObMergerConfigTask fetch_config_task_;
      ObMergerLeaseTask check_lease_task_;
      ObMergerMonitorTask monitor_task_;
      ObMergerVersionProxy *data_version_proxy_;
      ObMergerTabletLocationCache *location_cache_;
      ObMergerLocationCacheProxy *cache_proxy_;
      ObMergerServiceMonitor *service_monitor_;
      oceanbase::common::ObSessionManager session_mgr_;
      ObQueryCache* query_cache_;
    };

    inline void ObMergeServerService::extend_lease(const int64_t delay)
    {
      lease_expired_time_ = tbsys::CTimeUtil::getTime() + delay;
    }

    inline bool ObMergeServerService::check_lease(void) const
    {
      return tbsys::CTimeUtil::getTime() <= lease_expired_time_;
    }
  } /* mergeserver */
} /* oceanbase */

#endif /* end of include guard: OCEANBASE_MERGESERVER_SERVICE_H_ */
