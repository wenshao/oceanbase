#include <sys/syscall.h>
#include <unistd.h>
#include "ob_merge_server_service.h"
#include "ob_merge_server.h"
#include "common/ob_define.h"
#include "common/ob_packet.h"
#include "common/ob_read_common_data.h"
#include "common/ob_mod_define.h"
#include "common/ob_mutator.h"
#include "common/ob_result.h"
#include "common/ob_action_flag.h"
#include "common/ob_trace_log.h"
#include "common/ob_tsi_factory.h"
#include "common/ob_nb_accessor.h"
#include "ob_ms_rpc_proxy.h"
#include "ob_ms_rpc_stub.h"
#include "ob_ms_async_rpc.h"
#include "ob_rs_rpc_proxy.h"
#include "ob_ms_get_event_parellel.h"
#include "ob_ms_prefetch_data.h"
#include "ob_mutator_param_decoder.h"
#include "ob_ms_service_monitor.h"
#include "ob_ms_schema_manager.h"
#include "ob_ms_schema_proxy.h"
#include "ob_ms_config_manager.h"
#include "ob_ms_config_proxy.h"
#include "ob_ms_scanner_encoder.h"
#include "ob_ms_tablet_location.h"
#include "ob_ms_tablet_location_proxy.h"
#include "ob_ms_service_monitor.h"
#include "ob_read_param_decoder.h"
#include "ob_ms_tsi.h"
#include "ob_ms_scan_param.h"
#include "ob_ms_scan_event.h"
#include "ob_ms_counter_infos.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::mergeserver;

namespace oceanbase
{
  namespace mergeserver
  {
    static const int32_t MAX_ERROR_MSG_LEN = 256;
    ObMergeServerService::ObMergeServerService()
    {
      merge_server_ = NULL;
      inited_ = false;
      registered_ = false;

      rpc_proxy_ = NULL;
      rpc_stub_ = NULL;
      async_rpc_ = NULL;

      schema_mgr_ = NULL;
      schema_proxy_ = NULL;
      data_version_proxy_ = NULL;
      root_rpc_ = NULL;

      location_cache_ = NULL;
      cache_proxy_ = NULL;
      service_monitor_ = NULL;

      config_proxy_ = NULL;
      nb_accessor_ = NULL;
      query_cache_ = NULL;

      lease_expired_time_ = tbsys::CTimeUtil::getTime() + DEFAULT_LEASE_TIME;
    }

    ObMergeServerService::~ObMergeServerService()
    {
      destroy();
    }

    int ObMergeServerService::start()
    {
      return init_ms_properties_();
    }

    int ObMergeServerService::initialize(ObMergeServer* merge_server)
    {
      int rc = OB_SUCCESS;
      if (inited_)
      {
        rc = OB_INIT_TWICE;
      }
      else
      {
        merge_server_ = merge_server;
      }
      return rc;
    }

    int ObMergeServerService::register_root_server()
    {
      int err = OB_SUCCESS;
      registered_ = false;
      while (!merge_server_->is_stoped())
      {
        err = rpc_stub_->register_server(merge_server_->get_params().get_network_timeout(),
          merge_server_->get_root_server(),
          merge_server_->get_self(), true);
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN,"fail to register merge server to root server [root_ip:%d,root_port:%d]",
            merge_server_->get_root_server().get_ipv4(),
            merge_server_->get_root_server().get_port());
          usleep(RETRY_INTERVAL_TIME);
        }
        else
        {
          registered_ = true;
          lease_expired_time_ = tbsys::CTimeUtil::getTime() + DEFAULT_LEASE_TIME;
          break;
        }
      }
      return err;
    }

    // for reload config
    int ObMergeServerService::reload_config(const char * config_file)
    {
      // reload server frame work related config items
      int ret = OB_SUCCESS;
      if ((NULL == config_file) || (NULL == merge_server_ )
        || (NULL == data_version_proxy_) || (NULL == rpc_proxy_))
      {
        TBSYS_LOG(ERROR, "check merge server failed:config[%s], server[%p], rpc[%p], version[%p]",
          config_file, merge_server_, rpc_proxy_, data_version_proxy_);
        ret = OB_ERROR;
      }
      else
      {
        merge_server_->reload_config(config_file);
      }

      if (OB_SUCCESS == ret)
      {
        ret = rpc_proxy_->set_rpc_param(merge_server_->get_params().get_retry_times(),
          merge_server_->get_params().get_network_timeout());
      }

      if (OB_SUCCESS == ret)
      {
        ret = rpc_proxy_->set_blacklist_param(merge_server_->get_params().get_ups_blacklist_timeout(),
          merge_server_->get_params().get_ups_fail_count());
      }

      if (OB_SUCCESS == ret)
      {
        location_cache_->set_timeout(merge_server_->get_params().get_tablet_location_cache_timeout());
        ret = location_cache_->set_mem_size(merge_server_->get_params().get_tablet_location_cache_size());
      }

      if (OB_SUCCESS == ret)
      {
        data_version_proxy_->set_timeout(merge_server_->get_params().get_frozen_version_timeout());
        ObScanMergeJoinAgentImp::set_return_uncomplete_result(
          merge_server_->get_params().allow_return_uncomplete_result());
      }

      if (OB_SUCCESS == ret)
      {
        merge_server_->get_timer().destroy();
        ret = merge_server_->get_timer().init();
      }

      if (OB_SUCCESS == ret)
      {
        ret = merge_server_->get_timer().schedule(check_lease_task_,
          merge_server_->get_params().get_lease_check_interval(), true);
      }

      if (OB_SUCCESS == ret)
      {
        ret = monitor_task_.init(merge_server_, merge_server_->get_params().get_min_drop_count());
        monitor_task_.set_cache(cache_proxy_);
      }

      if (OB_SUCCESS == ret)
      {
        ObChunkServerTaskDispatcher::get_instance()->set_factor(merge_server_->get_params().get_get_request_factor(),
            merge_server_->get_params().get_scan_request_factor());
      }

      if (OB_SUCCESS == ret)
      {
        ret = merge_server_->get_timer().schedule(monitor_task_,
          merge_server_->get_params().get_monitor_interval(), true);
      }

      if (OB_SUCCESS == ret)
      {
        fetch_config_task_.init(rpc_stub_, config_proxy_,
            merge_server_->get_self(),
            merge_server_->get_params().get_config_cache_file_name(),
            merge_server_->get_params().get_network_timeout());
      }
      return ret;
    }

    // check instance role is right for read master
    bool ObMergeServerService::check_instance_role(const bool read_master) const
    {

      bool result = true;
      if ((true == read_master) && (instance_role_.get_role() != ObiRole::MASTER))
      {
        result = false;
      }
      return result;
    }



    int ObMergeServerService::init_ms_properties_()
    {
      int err  = OB_SUCCESS;
      ObSchemaManagerV2 *newest_schema_mgr = NULL;
      if (OB_SUCCESS == err)
      {
        newest_schema_mgr = new(std::nothrow)ObSchemaManagerV2;
        if (NULL == newest_schema_mgr)
        {
          err = OB_ALLOCATE_MEMORY_FAILED;
        }
      }

      if (OB_SUCCESS == err)
      {
        rpc_stub_ = new(std::nothrow)ObMergerRpcStub();
        if (NULL == rpc_stub_)
        {
          err = OB_ALLOCATE_MEMORY_FAILED;
        }
        else
        {
          err = rpc_stub_->init(merge_server_->get_rpc_buffer(),
            &(merge_server_->get_client_manager()));
          if (OB_SUCCESS == err)
          {
            err = register_root_server();
            if (err != OB_SUCCESS)
            {
              TBSYS_LOG(WARN, "server register to root failed:ret[%d]", err);
            }
          }
        }
      }

      if (OB_SUCCESS == err)
      {
        async_rpc_ = new(std::nothrow)ObMergerAsyncRpcStub();
        if (NULL == async_rpc_)
        {
          err = OB_ALLOCATE_MEMORY_FAILED;
        }
        else
        {
          err = async_rpc_->init(merge_server_->get_rpc_buffer(),
            &(merge_server_->get_client_manager()));
          if (err != OB_SUCCESS)
          {
            TBSYS_LOG(WARN, "init async rpc failed:ret[%d]", err);
          }
        }
      }

      if (OB_SUCCESS == err)
      {
        root_rpc_ = new(std::nothrow)ObMergerRootRpcProxy(merge_server_->get_params().get_retry_times(),
          merge_server_->get_params().get_network_timeout(), merge_server_->get_root_server());
        if (NULL == root_rpc_)
        {
          err = OB_ALLOCATE_MEMORY_FAILED;
        }
        else
        {
          err = root_rpc_->init(rpc_stub_);
          if (err != OB_SUCCESS)
          {
            TBSYS_LOG(WARN, "root rpc proxy init failed:ret[%d]", err);
          }
        }
      }

      if (OB_SUCCESS == err)
      {
        schema_mgr_ = new(std::nothrow)ObMergerSchemaManager;
        if (NULL == schema_mgr_)
        {
          err = OB_ALLOCATE_MEMORY_FAILED;
        }
        else
        {
          for (int32_t i = 0; i <= merge_server_->get_params().get_retry_times(); ++i)
          {
            err = rpc_stub_->fetch_schema(merge_server_->get_params().get_network_timeout(),
              merge_server_->get_root_server(),
              0, *newest_schema_mgr);
            if (OB_SUCCESS == err)
            {
              schema_mgr_->init(*newest_schema_mgr);
              break;
            }
            usleep(RETRY_INTERVAL_TIME);
          }
        }
      }

      if (OB_SUCCESS == err)
      {
        schema_proxy_ = new(std::nothrow)ObMergerSchemaProxy(root_rpc_, schema_mgr_);
        if (NULL == schema_proxy_)
        {
          err = OB_ALLOCATE_MEMORY_FAILED;
        }
      }

      if (OB_SUCCESS == err)
      {
        config_mgr_ = new(std::nothrow)ObMergerConfigManager;
        if (NULL == config_mgr_)
        {
          err = OB_ALLOCATE_MEMORY_FAILED;
        }
        else
        {
          config_mgr_->init(&merge_server_->get_params());
        }
      }

      if (OB_SUCCESS == err)
      {
        nb_accessor_ = new(std::nothrow)ObNbAccessor();
        if (NULL == nb_accessor_)
        {
          err = OB_ALLOCATE_MEMORY_FAILED;
        }
      }

      if (OB_SUCCESS == err)
      {
        config_proxy_ = new(std::nothrow)ObMergerConfigProxy(nb_accessor_, config_mgr_);
        if (NULL == config_proxy_)
        {
          err = OB_ALLOCATE_MEMORY_FAILED;
        }
        else
        {
          fetch_config_task_.init(rpc_stub_, config_proxy_, merge_server_->get_self(),
              merge_server_->get_params().get_config_cache_file_name(),
              merge_server_->get_params().get_network_timeout());
        }
      }

      if (OB_SUCCESS == err)
      {
        rpc_proxy_ = new(std::nothrow)ObMergerRpcProxy(merge_server_->get_params().get_retry_times(),
          merge_server_->get_params().get_network_timeout(), merge_server_->get_root_server(),
          merge_server_->get_self());
        if (NULL == rpc_proxy_)
        {
          err = OB_ALLOCATE_MEMORY_FAILED;
        }
      }

      if (OB_SUCCESS == err)
      {
        data_version_proxy_ = new(std::nothrow)ObMergerVersionProxy(
          merge_server_->get_params().get_frozen_version_timeout());
        if (NULL == data_version_proxy_)
        {
          err = OB_ALLOCATE_MEMORY_FAILED;
        }
        else
        {
          err = data_version_proxy_->init(rpc_proxy_);
        }
      }
      if (OB_SUCCESS == err)
      {
        location_cache_ = new(std::nothrow)ObMergerTabletLocationCache;
        if (NULL == location_cache_)
        {
          err = OB_ALLOCATE_MEMORY_FAILED;
        }
        else
        {
          err = location_cache_->init(merge_server_->get_params().get_tablet_location_cache_size(),
            1024, merge_server_->get_params().get_tablet_location_cache_timeout());
        }
      }

      if (OB_SUCCESS == err)
      {
        service_monitor_ = new(std::nothrow)ObMergerServiceMonitor(0);
        if (NULL == service_monitor_)
        {
          err = OB_ALLOCATE_MEMORY_FAILED;
        }
      }

      if (OB_SUCCESS == err)
      {
        cache_proxy_ = new(std::nothrow)ObMergerLocationCacheProxy(merge_server_->get_self(),
          root_rpc_, location_cache_, service_monitor_);
        if (NULL == cache_proxy_)
        {
          err = OB_ALLOCATE_MEMORY_FAILED;
        }
        else
        {
          err = cache_proxy_->init(merge_server_->get_params().get_max_access_lock_count());
        }
      }

      if (OB_SUCCESS == err)
      {
        if (merge_server_->get_params().get_query_cache_size() > 0)
        {
          query_cache_ = new(std::nothrow)ObQueryCache();
          if (NULL == query_cache_)
          {
            err = OB_ALLOCATE_MEMORY_FAILED;
          }
          else
          {
            err = query_cache_->init(merge_server_->get_params().get_query_cache_size());
            if (OB_SUCCESS != err)
            {
              TBSYS_LOG(WARN, "initialize query cache failed:ret[%d]", err);
            }
          }
        }
      }

      if (OB_SUCCESS == err)
      {
        err = rpc_proxy_->init(rpc_stub_, schema_mgr_, location_cache_, service_monitor_);
      }
      /// set update server black list param
      if (OB_SUCCESS == err)
      {
        err = rpc_proxy_->set_blacklist_param(merge_server_->get_params().get_ups_blacklist_timeout(),
            merge_server_->get_params().get_ups_fail_count());
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN, "set update server black list param failed:ret[%d]", err);
        }
      }

      if (OB_SUCCESS == err)
      {
        ObServer update_server;
        err = rpc_proxy_->get_master_ups(true, update_server);
      }

      if (OB_SUCCESS == err)
      {
        err = merge_server_->get_timer().init();
        if (err != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "timer init failed:ret[%d]", err);
        }
      }

      // lease check timer task
      if (OB_SUCCESS == err)
      {
        check_lease_task_.init(this);
        err = merge_server_->get_timer().schedule(check_lease_task_,
          merge_server_->get_params().get_lease_check_interval(), true);
        if (OB_SUCCESS == err)
        {
          TBSYS_LOG(INFO, "%s", "lease check timer schedule succ");
        }
        else
        {
          TBSYS_LOG(ERROR, "lease check timer schedule failed:ret[%d]", err);
        }
      }

      // cs balance factor
      if (OB_SUCCESS == err)
      {
        ObChunkServerTaskDispatcher::get_instance()->set_factor(merge_server_->get_params().get_get_request_factor(),
            merge_server_->get_params().get_scan_request_factor());
      }

      // monitor timer task
      if (OB_SUCCESS == err)
      {
        err = monitor_task_.init(merge_server_, merge_server_->get_params().get_min_drop_count());
        if (OB_SUCCESS == err)
        {
          monitor_task_.set_cache(cache_proxy_);
          err = merge_server_->get_timer().schedule(monitor_task_,
            merge_server_->get_params().get_monitor_interval(), true);
          if (OB_SUCCESS == err)
          {
            TBSYS_LOG(INFO, "%s", "monitor timer schedule succ");
          }
          else
          {
            TBSYS_LOG(ERROR, "monitor timer schedule failed:ret[%d]", err);
          }
        }
      }

      if (OB_SUCCESS != err)
      {
        if (rpc_proxy_)
        {
          delete rpc_proxy_;
          rpc_proxy_ = NULL;
        }

        if (rpc_stub_)
        {
          delete rpc_stub_;
          rpc_stub_ = NULL;
        }

        if (data_version_proxy_)
        {
          delete data_version_proxy_;
          data_version_proxy_ = NULL;
        }

        if (schema_mgr_)
        {
          delete schema_mgr_;
          schema_mgr_ = NULL;
        }

        if (schema_proxy_)
        {
          delete schema_proxy_;
          schema_proxy_ = NULL;
        }

        if (config_mgr_)
        {
          delete config_mgr_;
          config_mgr_ = NULL;
        }

        if (config_proxy_)
        {
          delete config_proxy_;
          config_proxy_ = NULL;
        }


        if (location_cache_)
        {
          delete location_cache_;
          location_cache_ = NULL;
        }

        if (cache_proxy_)
        {
          delete cache_proxy_;
          cache_proxy_ = NULL;
        }

        if (query_cache_)
        {
          delete query_cache_;
          query_cache_ = NULL;
        }
      }

      if (newest_schema_mgr)
      {
        delete newest_schema_mgr;
        newest_schema_mgr = NULL;
      }

      if (OB_SUCCESS == err)
      {
        inited_ = true;
      }
      return err;
    }

    int ObMergeServerService::destroy()
    {
      int rc = OB_SUCCESS;
      if (inited_)
      {
        inited_ = false;
        merge_server_->get_timer().destroy();
        merge_server_ = NULL;
        delete rpc_proxy_;
        rpc_proxy_ = NULL;
        delete rpc_stub_;
        rpc_stub_ = NULL;
        delete schema_mgr_;
        schema_mgr_ = NULL;
        delete schema_proxy_;
        schema_proxy_ = NULL;
        delete config_mgr_;
        config_mgr_ = NULL;
        delete config_proxy_;
        config_proxy_ = NULL;
        delete location_cache_;
        location_cache_ = NULL;
        delete cache_proxy_;
        cache_proxy_ = NULL;
        delete service_monitor_;
        service_monitor_ = NULL;
        delete data_version_proxy_;
        data_version_proxy_ = NULL;
        if (NULL != query_cache_)
        {
          delete query_cache_;
          query_cache_ = NULL;
        }
      }
      else
      {
        rc = OB_NOT_INIT;
      }
      return rc;
    }

    void ObMergeServerService::handle_failed_request(const int64_t timeout, const int32_t packet_code)
    {
      if (!inited_)  //|| !registered_)
      {
        TBSYS_LOG(WARN, "%s", "merge server has not inited or registered");
      }
      else
      {
        // no need deserialize the packet to get the table id
        switch (packet_code)
        {
        case OB_SCAN_REQUEST:
          service_monitor_->inc(0, ObMergerServiceMonitor::FAIL_SCAN_OP_COUNT);
          service_monitor_->inc(0, ObMergerServiceMonitor::FAIL_SCAN_OP_TIME, timeout);
          break;
        case OB_GET_REQUEST:
          service_monitor_->inc(0, ObMergerServiceMonitor::FAIL_GET_OP_COUNT);
          service_monitor_->inc(0, ObMergerServiceMonitor::FAIL_GET_OP_TIME, timeout);
          break;
        default:
          TBSYS_LOG(WARN, "handle overflow or timeout packet not include statistic info:packet[%d]", packet_code);
        }
      }
    }

    int ObMergeServerService::do_request(
      const int64_t receive_time,
      const int32_t packet_code,
      const int32_t version,
      const int32_t channel_id,
      tbnet::Connection* connection,
      common::ObDataBuffer& in_buffer,
      common::ObDataBuffer& out_buffer,
      const int64_t timeout_us)
    {
      static const int32_t DROP_REQUEST_REF_INTERVAL = 5;
      int rc = OB_SUCCESS;
      if (!inited_)  //|| !registered_)
      {
        TBSYS_LOG(WARN, "%s", "merge server has not inited or registered");
        rc = OB_NOT_INIT;
      }

      if (rc == OB_SUCCESS)
      {
        UNUSED(timeout_us);
        switch (packet_code)
        {
        case OB_LIST_SESSIONS_REQUEST:
          rc = ms_list_sessions(version, channel_id, connection, in_buffer, out_buffer);
          break;
        case OB_KILL_SESSION_REQUEST:
          rc = ms_kill_session(version, channel_id, connection, in_buffer, out_buffer);
          break;
        case OB_REQUIRE_HEARTBEAT:
          rc = ms_heartbeat(receive_time, version, channel_id, connection, in_buffer, out_buffer, timeout_us);
          break;
        case OB_SCAN_REQUEST:
          if (timeout_us >= ms_get_counter_set().get_avg_time_used(ObMergerCounterIds::C_CLIENT_SCAN,
            DROP_REQUEST_REF_INTERVAL))
          {
            rc = ms_scan(receive_time, version, channel_id, connection, in_buffer, out_buffer, timeout_us);
          }
          else
          {
            TBSYS_LOG(INFO, "server is busy, drop scan request [channel_id:%d,ref_interval_us:%ld,timeout_us:%ld,"
                "avg_time_used:%ld]", channel_id, DROP_REQUEST_REF_INTERVAL*ms_get_counter_set().get_static_interval(
                ObMergerCounterIds::C_CLIENT_SCAN), timeout_us, ms_get_counter_set().get_avg_time_used(
                ObMergerCounterIds::C_CLIENT_SCAN, DROP_REQUEST_REF_INTERVAL));
            rc = do_timeouted_req(version, channel_id, connection, in_buffer, out_buffer);
          }
          break;
        case OB_GET_REQUEST:
          if (timeout_us >= ms_get_counter_set().get_avg_time_used(ObMergerCounterIds::C_CLIENT_GET,
            DROP_REQUEST_REF_INTERVAL))
          {
            rc = ms_get(receive_time, version, channel_id, connection, in_buffer, out_buffer, timeout_us);
          }
          else
          {
            TBSYS_LOG(INFO, "server is busy, drop get request [channel_id:%d,ref_interval_us:%ld,timeout_us:%ld,"
                "avg_time_used:%ld]", channel_id, DROP_REQUEST_REF_INTERVAL*ms_get_counter_set().get_static_interval(
                ObMergerCounterIds::C_CLIENT_GET), timeout_us, ms_get_counter_set().get_avg_time_used(
                ObMergerCounterIds::C_CLIENT_GET, DROP_REQUEST_REF_INTERVAL));
            rc = do_timeouted_req(version, channel_id, connection, in_buffer, out_buffer);
          }
          break;
        case OB_WRITE:
        case OB_MS_MUTATE:
          rc = ms_mutate(receive_time, version, channel_id, connection, in_buffer, out_buffer, timeout_us);
          break;
        case OB_FETCH_STATS:
          rc = ms_stat(receive_time, version, channel_id, connection, in_buffer, out_buffer, timeout_us);
          ms_get_counter_set().inc(ObMergerCounterIds::C_FETCH_STATUS);
          break;
        case OB_CLEAR_REQUEST:
          rc = ms_clear(receive_time, version, channel_id, connection, in_buffer, out_buffer, timeout_us);
          ms_get_counter_set().inc(ObMergerCounterIds::C_CLEAR_REQUEST);
          break;
        case OB_UPS_RELOAD_CONF:
          rc = ms_reload(receive_time, version, channel_id, connection, in_buffer, out_buffer, timeout_us);
          break;
        case OB_CHANGE_LOG_LEVEL:
          rc = ms_change_log_level(receive_time, version, channel_id, connection, in_buffer, out_buffer, timeout_us);
          break;
        default:
          TBSYS_LOG(WARN, "check packet type failed:type[%d]", packet_code);
          rc = OB_ERROR;
        }
      }
      return rc;
    }

    int ObMergeServerService::do_timeouted_req(
      const int32_t version,
      const int32_t channel_id,
      tbnet::Connection* connection,
      common::ObDataBuffer& in_buffer,
      common::ObDataBuffer& out_buffer)
    {
      int64_t start_time = tbsys::CTimeUtil::getTime();
      UNUSED(in_buffer);
      UNUSED(version);
      ObResultCode rc;
      const int RESPONSE_VERSION = 1;
      rc.result_code_ = OB_RESPONSE_TIME_OUT;

      int32_t err = OB_SUCCESS;
      int32_t send_err  = OB_SUCCESS;
      err = rc.serialize(out_buffer.get_data(),out_buffer.get_capacity(), out_buffer.get_position());
      if (OB_SUCCESS == err)
      {
        send_err = merge_server_->send_response(OB_CLEAR_RESPONSE, RESPONSE_VERSION,
          out_buffer, connection, channel_id);
      }
      ms_get_counter_set().inc(ObMergerCounterIds::C_CLIENT_DROP, tbsys::CTimeUtil::getTime() - start_time);
      return send_err;
    }

    int ObMergeServerService::ms_list_sessions(
      const int32_t version,
      const int32_t channel_id,
      tbnet::Connection* connection,
      common::ObDataBuffer& in_buffer,
      common::ObDataBuffer& out_buffer)
    {
      const int32_t MS_LIST_SESSIONS_VERSION = 1;
      ObResultCode rc;
      int32_t &err = rc.result_code_;
      UNUSED(in_buffer);
      UNUSED(version);
      UNUSED(channel_id);
      UNUSED(connection);
      const int64_t buf_size = 16*1024;
      char *buf = (char*)ob_malloc(buf_size);
      ObObj result;
      int64_t pos = 0;
      ObString str;
      result.set_varchar(str);
      if (NULL == buf)
      {
        TBSYS_LOG(WARN,"fail to allocate memory for session infos");
        err = OB_ALLOCATE_MEMORY_FAILED;
      }
      if (OB_SUCCESS == err)
      {
        session_mgr_.get_sessions(buf,buf_size,pos);
        str.assign(buf,static_cast<int32_t>(pos));
        result.set_varchar(str);
      }
      int32_t send_err  = OB_SUCCESS;
      if (OB_SUCCESS != (err = rc.serialize(out_buffer.get_data(),out_buffer.get_capacity(), out_buffer.get_position())))
      {
        TBSYS_LOG(WARN,"fail to serialize result code [err:%d]", err);
      }
      if ((OB_SUCCESS == err)
        && (OB_SUCCESS != (err = result.serialize(out_buffer.get_data(),out_buffer.get_capacity(), out_buffer.get_position()))))
      {
        TBSYS_LOG(WARN,"fail to serialize result msg [err:%d]", err);
      }
      if ((OB_SUCCESS == err)
        && (OB_SUCCESS != (send_err =  merge_server_->send_response(OB_LIST_SESSIONS_RESPONSE,
        MS_LIST_SESSIONS_VERSION, out_buffer, connection, channel_id))))
      {
        TBSYS_LOG(WARN,"fail to send list sessions response [send_err:%d]", send_err);
      }
      ob_free(buf);
      return send_err;
    }

    int ObMergeServerService::ms_kill_session(
      const int32_t version,
      const int32_t channel_id,
      tbnet::Connection* connection,
      common::ObDataBuffer& in_buffer,
      common::ObDataBuffer& out_buffer)
    {
      const int32_t MS_KILL_SESSION_VERSION = 1;
      ObResultCode rc;
      int32_t &err = rc.result_code_;
      UNUSED(out_buffer);
      UNUSED(version);
      UNUSED(channel_id);
      UNUSED(connection);
      int64_t session_id = 0;
      ObObj obj;
      if (OB_SUCCESS != (err = obj.deserialize(in_buffer.get_data(), in_buffer.get_capacity(), in_buffer.get_position())))
      {
        TBSYS_LOG(WARN,"fail to get session id from request [err:%d]", err);
      }
      if ((OB_SUCCESS == err) && (OB_SUCCESS != (err = obj.get_int(session_id))))
      {
        TBSYS_LOG(WARN,"fail to get session id from reqeust [err:%d]", err);
      }
      if ((OB_SUCCESS == err) && (session_id <= 0))
      {
        TBSYS_LOG(WARN,"invalid aguemnt [session_id:%ld]", session_id);
      }
      if (OB_SUCCESS == err)
      {
        session_mgr_.kill_session(static_cast<uint64_t>(session_id));
      }
      int32_t send_err  = OB_SUCCESS;
      err = rc.serialize(out_buffer.get_data(),out_buffer.get_capacity(), out_buffer.get_position());
      if (OB_SUCCESS == err)
      {
        TBSYS_LOG(INFO, "kill session [session_id:%ld]", session_id);
        send_err = merge_server_->send_response(OB_KILL_SESSION_RESPONSE, MS_KILL_SESSION_VERSION,
          out_buffer, connection, channel_id);
      }
      return send_err;
    }

    int ObMergeServerService::ms_heartbeat(
      const int64_t start_time,
      const int32_t version,
      const int32_t channel_id,
      tbnet::Connection* connection,
      common::ObDataBuffer& in_buffer,
      common::ObDataBuffer& out_buffer,
      const int64_t timeout_us)
    {
      static const int32_t MS_HEARTBEAT_LEASE_SCHEMA_VERSION = 3;
      static const int32_t MS_HEARTBEAT_LEASE_SCHEMA_CONFIG_VERSION = 4;

      FILL_TRACE_LOG("step 1. start process heartbeat");
      ObResultCode rc;
      int32_t &err = rc.result_code_;
      UNUSED(channel_id);
      UNUSED(start_time);
      UNUSED(connection);
      UNUSED(out_buffer);
      UNUSED(timeout_us);

      if (version >= MS_HEARTBEAT_LEASE_SCHEMA_VERSION)
      {
        int64_t lease_duration = 0;
        err = serialization::decode_vi64(in_buffer.get_data(),
            in_buffer.get_capacity(), in_buffer.get_position(), &lease_duration);
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(ERROR, "parse heartbeat input lease_duration param failed:ret[%d]", err);
        }
        else
        {
          if (lease_duration <= 0)
          {
            err = OB_ERROR;
            TBSYS_LOG(ERROR, "check lease duration failed:duration[%ld]", lease_duration);
          }
        }

        int64_t local_version = schema_mgr_->get_latest_version();
        int64_t schema_version = 0;
        if (OB_SUCCESS == err)
        {
          err = serialization::decode_vi64(in_buffer.get_data(),in_buffer.get_capacity(),
              in_buffer.get_position(), &schema_version);
          if (OB_SUCCESS != err)
          {
            TBSYS_LOG(ERROR, "parse heartbeat schema version failed:ret[%d]", err);
          }
          else if (local_version > schema_version)
          {
            err = OB_ERROR;
            TBSYS_LOG(ERROR, "check schema local version gt than new version:"
                "local[%ld], new[%ld]", local_version, schema_version);
          }
        }

        FILL_TRACE_LOG("step 2. decode heartbeat:lease[%ld], local[%ld], version[%ld]",
            lease_duration, local_version, schema_version);

        if (OB_SUCCESS == err)
        {
          err = root_rpc_->async_heartbeat(merge_server_->get_self());
          if (err != OB_SUCCESS)
          {
            TBSYS_LOG(ERROR, "heartbeat to root server failed:ret[%d]", err);
          }
          else
          {
            extend_lease(lease_duration);
            TBSYS_LOG(DEBUG, "%s", "heartbeat to root server succ");
          }
        }

        // fetch new schema in a temp timer task
        if ((OB_SUCCESS == err) && (local_version < schema_version) && (schema_version > 0))
        {
          fetch_schema_task_.init(rpc_proxy_, schema_mgr_);
          fetch_schema_task_.set_version(local_version, schema_version);
          srand(static_cast<int32_t>(tbsys::CTimeUtil::getTime()));
          err = merge_server_->get_timer().schedule(fetch_schema_task_,
              random() % FETCH_SCHEMA_INTERVAL, false);
          if (err != OB_SUCCESS)
          {
            TBSYS_LOG(ERROR, "schedule fetch new schema task failed:version[%ld], ret[%d]",
                schema_version, err);
          }
        }

        // Role
        common::ObiRole role;
        if (OB_SUCCESS == err)
        {
          if (OB_SUCCESS != (err = role.deserialize(in_buffer.get_data(), in_buffer.get_capacity(), in_buffer.get_position())))
          {
            TBSYS_LOG(WARN, "parse heartbeat input role param failed:ret[%d]", err);
          }
        }
      }

      if (version >= MS_HEARTBEAT_LEASE_SCHEMA_CONFIG_VERSION)
      {
        // update config version
        // when all_sys_param table updated, rootserver will notify us in this heartbeat
        int64_t local_config_version = config_mgr_->get_config_version();
        int64_t config_version = 0;
        err = serialization::decode_vi64(in_buffer.get_data(),
            in_buffer.get_capacity(), in_buffer.get_position(), &config_version);
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(ERROR, "parse heartbeat input config_version param failed:ret[%d]", err);
        }
        else
        {
          if (local_config_version < config_version)
          {
            // fetch config in a temp timer task
            fetch_config_task_.set_version(config_version);
            srand(static_cast<int32_t>(tbsys::CTimeUtil::getTime()));
            err = merge_server_->get_timer().schedule(fetch_config_task_,
                random() % FETCH_SCHEMA_INTERVAL, false);
            if (err != OB_SUCCESS)
            {
              TBSYS_LOG(ERROR, "schedule fetch new config task failed:version[%ld], ret[%d]",
                  config_version, err);
            }
          }
        }
      }

      FILL_TRACE_LOG("step 3. process heartbeat finish:ret[%d]", err);
      CLEAR_TRACE_LOG();
      return err;
    }

    int ObMergeServerService::ms_clear(
      const int64_t start_time,
      const int32_t version,
      const int32_t channel_id,
      tbnet::Connection* connection,
      common::ObDataBuffer& in_buffer,
      common::ObDataBuffer& out_buffer,
      const int64_t timeout_us)
    {
      FILL_TRACE_LOG("step 1. start clear tablet location cache");
      const int32_t MS_CLEAR_VERSION = 1;
      UNUSED(start_time);
      UNUSED(in_buffer);
      UNUSED(version);
      UNUSED(timeout_us);
      ObResultCode rc;
      int32_t &err = rc.result_code_;
      if (NULL != location_cache_)
      {
        err = location_cache_->clear();
        TBSYS_LOG(INFO, "clear tablet location cache:ret[%d]", err);
      }
      else
      {
        err = OB_ERROR;
      }

      int32_t send_err  = OB_SUCCESS;
      err = rc.serialize(out_buffer.get_data(),out_buffer.get_capacity(), out_buffer.get_position());
      if (OB_SUCCESS == err)
      {
        send_err = merge_server_->send_response(OB_CLEAR_RESPONSE, MS_CLEAR_VERSION,
          out_buffer, connection, channel_id);
      }

      FILL_TRACE_LOG("step 2. process clear cache finish:ret[%d]", err);
      PRINT_TRACE_LOG();
      CLEAR_TRACE_LOG();
      return send_err;
    }

    int ObMergeServerService::ms_reload(
      const int64_t start_time,
      const int32_t version,
      const int32_t channel_id,
      tbnet::Connection* connection,
      common::ObDataBuffer& in_buffer,
      common::ObDataBuffer& out_buffer,
      const int64_t timeout_us)
    {
      FILL_TRACE_LOG("step 1. start reload config file");
      const int32_t MS_RELOAD_VERSION = 1;
      UNUSED(start_time);
      UNUSED(in_buffer);
      UNUSED(version);
      UNUSED(timeout_us);
      ObResultCode rc;
      int32_t &err = rc.result_code_;
      ObString conf_file;
      char config_file_str[OB_MAX_FILE_NAME_LENGTH];
      err = conf_file.deserialize(in_buffer.get_data(), in_buffer.get_capacity(), in_buffer.get_position());
      if (OB_SUCCESS != err)
      {
        TBSYS_LOG(ERROR, "deserialize conf file failed:ret[%d]", err);
      }
      else
      {
        int64_t length = conf_file.length();
        strncpy(config_file_str, conf_file.ptr(), length);
        config_file_str[length] = '\0';
      }

      if (OB_SUCCESS == err)
      {
        err = reload_config(config_file_str);
        TBSYS_LOG(INFO, "reload config file:file[%s], ret[%d]", config_file_str, err);
        int32_t send_err  = OB_SUCCESS;
        err = rc.serialize(out_buffer.get_data(),out_buffer.get_capacity(), out_buffer.get_position());
        if (OB_SUCCESS == err)
        {
          send_err = merge_server_->send_response(OB_UPS_RELOAD_CONF_RESPONSE, MS_RELOAD_VERSION,
            out_buffer, connection, channel_id);
        }
      }

      FILL_TRACE_LOG("step 2. reload config finish:ret[%d]", err);
      PRINT_TRACE_LOG();
      CLEAR_TRACE_LOG();
      return err;
    }

    int ObMergeServerService::ms_stat(
      const int64_t start_time,
      const int32_t version,
      const int32_t channel_id,
      tbnet::Connection* connection,
      common::ObDataBuffer& in_buffer,
      common::ObDataBuffer& out_buffer,
      const int64_t timeout_us)
    {
      FILL_TRACE_LOG("step 1. start monitor stat");
      const int32_t MS_MONITOR_VERSION = 1;
      ObResultCode rc;
      int32_t &err = rc.result_code_;
      int32_t send_err  = OB_SUCCESS;
      UNUSED(in_buffer);
      UNUSED(start_time);
      UNUSED(version);
      UNUSED(timeout_us);
      err = rc.serialize(out_buffer.get_data(),out_buffer.get_capacity(), out_buffer.get_position());
      if (OB_SUCCESS == err)
      {
        if (NULL != service_monitor_)
        {
          err = service_monitor_->serialize(out_buffer.get_data(),out_buffer.get_capacity(),
            out_buffer.get_position());
        }
        else
        {
          err = OB_NO_MONITOR_DATA;
        }
      }

      if (OB_SUCCESS == err)
      {
        send_err = merge_server_->send_response(OB_FETCH_STATS_RESPONSE, MS_MONITOR_VERSION,
          out_buffer, connection, channel_id);
      }

      FILL_TRACE_LOG("step 2. process monitor stat finish:ret[%d]", err);
      CLEAR_TRACE_LOG();
      return send_err;
    }

    int ObMergeServerService::ms_get(
      const int64_t start_time,
      const int32_t version,
      const int32_t channel_id,
      tbnet::Connection* connection,
      common::ObDataBuffer& in_buffer,
      common::ObDataBuffer& out_buffer,
      const int64_t timeout_us)
    {
      int64_t req_timeout_us = timeout_us;
      if ((0 >= timeout_us) || (timeout_us > merge_server_->get_params().get_max_req_process_time()))
      {
        req_timeout_us = merge_server_->get_params().get_max_req_process_time();
        TBSYS_LOG(DEBUG, "reset timeoutus %ld", req_timeout_us);
      }
      int64_t my_start = tbsys::CTimeUtil::getTime();
      int32_t request_cid = channel_id;
      const int32_t MS_GET_VERSION = 1;
      ObResultCode rc;
      char err_msg[MAX_ERROR_MSG_LEN]="";
      rc.message_.assign(err_msg, sizeof(err_msg));
      int32_t &err = rc.result_code_;
      int32_t &err_code = rc.result_code_;
      int32_t send_err  = OB_SUCCESS;
      err = OB_SUCCESS;
      ObGetParam *get_param_with_name = GET_TSI_MULT(ObGetParam,  TSI_MS_GET_PARAM_WITH_NAME_1);
      ObGetParam *org_get_param = GET_TSI_MULT(ObGetParam, TSI_MS_ORG_GET_PARAM_1);
      ObGetParam *decoded_get_param = GET_TSI_MULT(ObGetParam, TSI_MS_DECODED_GET_PARAM_1);
      ObReadParam &decoded_read_param = *decoded_get_param;
      const ObSchemaManagerV2 *schema_mgr = NULL;
      ObScanner *result_scanner = GET_TSI_MULT(ObScanner, TSI_MS_SCANNER_1);
      ObMergerParellelGetEvent * get_event = GET_TSI_ARGS(ObMergerParellelGetEvent, TSI_MS_GET_EVENT_1,
          cache_proxy_, async_rpc_);
      int64_t timeout_time = start_time + req_timeout_us;
      if (OB_SUCCESS == err && MS_GET_VERSION != version)
      {
        err = OB_ERROR_FUNC_VERSION;
      }
      if (OB_SUCCESS == err &&
        (NULL == org_get_param || NULL == decoded_get_param
            || NULL == result_scanner || NULL == get_param_with_name))
      {
        TBSYS_LOG(WARN,"fail to allocate memory for request");
        err = OB_ALLOCATE_MEMORY_FAILED;
      }
      else if (OB_SUCCESS == err)
      {
        if ((!get_event->inited())
          && ( OB_SUCCESS != (err = get_event->init(REQUEST_EVENT_QUEUE_SIZE,
              ObModIds::OB_MS_REQUEST_EVENT, &session_mgr_))))
        {
          TBSYS_LOG(WARN,"fail to init ObMergerGetEvent [err:%d]", err);
        }
      }
      /// decode request
      if (OB_SUCCESS == err)
      {
        FILL_TRACE_LOG("step 1. start serve ms_get [request_event_id:%lu]", get_event->get_request_id());
        err = org_get_param->deserialize(in_buffer.get_data(), in_buffer.get_capacity(),
          in_buffer.get_position());
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN,"fail to parse ObGetParam [err:%d]",err);
        }
        else
        {
          decoded_read_param = *dynamic_cast<ObReadParam*>(org_get_param);
        }
      }
      if (0 >= org_get_param->get_cell_size())
      {
        TBSYS_LOG(WARN,"get param cell size error, [org_get_param:%ld]", org_get_param->get_cell_size());
        err = OB_INVALID_ARGUMENT;
      }
      /// get local newest schema
      if (OB_SUCCESS == err)
      {
        err = schema_proxy_->get_schema(ObMergerSchemaProxy::LOCAL_NEWEST, &schema_mgr);
        if ((OB_SUCCESS != err) || (NULL == schema_mgr))
        {
          TBSYS_LOG(WARN, "%s", "fail to get the latest schema, unexpected error");
          err = OB_ERR_UNEXPECTED;
        }
      }

      bool is_cache_hit = false;
      uint64_t session_id = 0;
      ObCellInfo* first_cell = NULL;
      ObString first_table_name;
      bool got_all_result = false;
      int64_t net_session_id = 0;
      int64_t packet_cnt = 0;
      ObPacketQueueThread &queue_thread = merge_server_->get_default_task_queue_thread();
      ObPacket* next_request = NULL;

      if (OB_SUCCESS == err)
      {
        first_cell = org_get_param->operator [](0);
        if (NULL != first_cell)
        {
          first_table_name = first_cell->table_name_;
        }
      }

      if ((OB_SUCCESS == err) && NULL != query_cache_
          && org_get_param->get_is_result_cached()
          && schema_mgr->get_table_query_cache_expire_time(first_table_name) > 0)
      {
        ObString result_key;
        result_key.assign_ptr(in_buffer.get_data(), static_cast<int32_t>(in_buffer.get_position()));
        if (OB_SUCCESS == query_cache_->get(result_key, out_buffer))
        {
          is_cache_hit = true;
        }
      }

      if (OB_SUCCESS == err && is_cache_hit)
      {
        // query cache hit
        if ((OB_SUCCESS == send_err) &&
          (OB_SUCCESS != (send_err = merge_server_->send_response(OB_GET_RESPONSE, MS_GET_VERSION,out_buffer,
          connection,request_cid, 0))))
        {
          TBSYS_LOG(WARN,"fail to send response to client [err:%d]", send_err);
        }
        if (OB_SUCCESS == send_err)
        {
          packet_cnt ++;
        }
        FILL_TRACE_LOG("step 2. query cache hit, send_response:send_err[%d], code[%d], packet_cnt[%ld]",
          send_err, err, packet_cnt);
      }
      else
      {
        //query cache miss
        if (OB_SUCCESS == err)
        {
          err = ob_decode_get_param(*org_get_param,*schema_mgr,*decoded_get_param, *get_param_with_name, &rc);
          if (OB_SUCCESS != err)
          {
            TBSYS_LOG(WARN,"fail to decode get param [err:%d]", err);
          }
        }
        if ((OB_SUCCESS == err)
          && (OB_SUCCESS != (err = session_mgr_.session_begin(*decoded_get_param,
          connection->getPeerId(),session_id, syscall(SYS_gettid), static_cast<pid_t>(pthread_self())))))
        {
          TBSYS_LOG(WARN,"fail to create session in ObSessionManager [err:%d]", err);
        }
        if (OB_SUCCESS == err)
        {
          get_event->set_session_id(static_cast<uint32_t>(session_id));
        }

        FILL_TRACE_LOG("step 2. finish parse the schema for ms_get:err[%d],get_cell_size[%ld]", err,
          decoded_get_param->get_cell_size());

        if (OB_SUCCESS == err)
        {
          get_event->set_timeout_percent(merge_server_->get_params().get_max_timeout_percent());
          err = get_event->set_request_param(*decoded_get_param, req_timeout_us,
            merge_server_->get_params().get_max_parellel_count(),
            merge_server_->get_params().get_max_get_rows_per_subreq(),
            merge_server_->get_params().get_reserve_get_param_count());
          if (OB_SUCCESS != err)
          {
            TBSYS_LOG(ERROR, "get request set param failed:err[%d]", err);
          }
          else
          {
            // timeout should be packet timeout
            err = get_event->wait(timeout_time - tbsys::CTimeUtil::getTime());
            if (err != OB_SUCCESS)
            {
              TBSYS_LOG(WARN, "wait get request failed:err[%d]", err);
            }
          }
        }
        FILL_TRACE_LOG("step 3. wait response succ");

        /// prepare result
        if ((OB_SUCCESS == err)  && (OB_SUCCESS != (err = get_event->fill_result(*result_scanner,
          *get_param_with_name,got_all_result))))
        {
          TBSYS_LOG(WARN,"fail to faill result [err:%d]", err);
        }
        bool session_next = merge_server_->get_params().get_support_session_next();
        if ((OB_SUCCESS == err) && (!got_all_result) && session_next)
        {
          net_session_id = queue_thread.generate_session_id();
          FILL_TRACE_LOG("step 3.x generator session id:session[%ld]", net_session_id);
        }
        do
        {
          if ((OB_SUCCESS == err) && session_next && (!got_all_result) && (packet_cnt > 0))
          {
            result_scanner->reset();
            if ((OB_SUCCESS == err)  && (OB_SUCCESS != (err = get_event->fill_result(*result_scanner,
              *get_param_with_name,got_all_result))))
            {
              TBSYS_LOG(WARN,"fail to fill result [err:%d]", err);
            }
          }
          FILL_TRACE_LOG("step 4.x. finish fill next result scanner:err[%d]", send_err);
          out_buffer.get_position() = 0;
          /// always send error code
          if ((OB_SUCCESS != (send_err = rc.serialize(out_buffer.get_data(),out_buffer.get_capacity(),
            out_buffer.get_position()))))
          {
            TBSYS_LOG(WARN,"fail to serialize result code [err:%d]", send_err);
          }
          if ((OB_SUCCESS == err) && (OB_SUCCESS == send_err)
            && (OB_SUCCESS !=(send_err = result_scanner->serialize(out_buffer.get_data(),
                out_buffer.get_capacity(), out_buffer.get_position()))))
          {
            TBSYS_LOG(WARN,"fail to serialize result scanner [err:%d]", send_err);
          }
          FILL_TRACE_LOG("step 4.x. finish serialize scanner:err[%d]", send_err);
          if ((OB_SUCCESS == err) && session_next && !got_all_result
            && (OB_SUCCESS != queue_thread.prepare_for_next_request(net_session_id)))
          {
            TBSYS_LOG(WARN,"fail to prepare network session [err:%d]", err);
          }
          FILL_TRACE_LOG("step 4.x. prepare for next request:send_err[%d], code[%d], packet_cnt[%ld]",
              send_err, err_code, packet_cnt);
          if ((OB_SUCCESS == err) && (OB_SUCCESS == send_err)
              && got_all_result && 0 == packet_cnt && NULL != query_cache_
              && schema_mgr->get_table_query_cache_expire_time(first_table_name) > 0)
          {
            ObString result_key;
            ObQueryCacheValue result_value;
            result_key.assign_ptr(in_buffer.get_data(), static_cast<int32_t>(in_buffer.get_capacity()));
            result_value.expire_time_ =
              schema_mgr->get_table_query_cache_expire_time(first_table_name)
              + tbsys::CTimeUtil::getTime();
            result_value.size_ = out_buffer.get_position();
            result_value.buf_ = out_buffer.get_data();
            err = query_cache_->put(result_key, result_value);
            if (OB_SUCCESS != err)
            {
              TBSYS_LOG(WARN, "failed to put get result into query cache:err[%d]", err);
              err = OB_SUCCESS;
            }
          }
          if ((OB_SUCCESS == send_err) &&
            (OB_SUCCESS != (send_err = merge_server_->send_response(OB_GET_RESPONSE, MS_GET_VERSION,out_buffer,
            connection,request_cid, (got_all_result?0:net_session_id)))))
          {
            TBSYS_LOG(WARN,"fail to send response to client [err:%d]", send_err);
          }
          if (OB_SUCCESS == send_err)
          {
            packet_cnt ++;
          }
          FILL_TRACE_LOG("step 5.x. send_response:send_err[%d], code[%d], packet_cnt[%ld]",
              send_err, err_code, packet_cnt);
          if ((OB_SUCCESS == err) && session_next && (!got_all_result) && (OB_SUCCESS == send_err))
          {
            err = queue_thread.wait_for_next_request(net_session_id,next_request,
                timeout_time - tbsys::CTimeUtil::getTime());
            if (OB_SUCCESS != err)
            {
              TBSYS_LOG(WARN,"fail to wait next request from session [err:%d,net_session_id:%ld]",
                  err, net_session_id);
            }
            else
            {
              request_cid = next_request->getChannelId();
            }
            FILL_TRACE_LOG("step 5.x. wait next request succ:code[%d], packet_cnt[%ld]", err_code, packet_cnt);
          }
        } while (session_next  && (!got_all_result) && (OB_SUCCESS == send_err) && (OB_SUCCESS == err));
      }

      int64_t end_time = tbsys::CTimeUtil::getTime();
      if (OB_SUCCESS == err)
      {
        ms_get_counter_set().inc(ObMergerCounterIds::C_CLIENT_GET, end_time - start_time);
        ms_get_counter_set().inc(get_counter_id_for_proc_time(end_time - start_time));
        TBSYS_LOG(DEBUG, "get send reponse:ret_code[%d]", err_code);
        if (end_time - my_start > merge_server_->get_params().get_slow_query_threshold())
        {
          const char *session_info = NULL;
          if (NULL == (session_info = session_mgr_.get_session(session_id)))
          {
            TBSYS_LOG(WARN,"get empty session info");
          }
          else
          {
            TBSYS_LOG(WARN,"slow query:%s; used_time:%ld; total_time:%ld, result_size:%ld",
              session_info, end_time - my_start, end_time - start_time, result_scanner->get_size());
          }
        }
      }

      // check process timeout
      if (((err_code != OB_SUCCESS) || (end_time > start_time + req_timeout_us)) && org_get_param != NULL)
      {
        if (OB_SUCCESS == err_code)
        {
          err_code = OB_PROCESS_TIMEOUT;
        }
        TBSYS_LOG(WARN, "check process get task timeout:client[%s], cell[%ld],"
          "start[%ld], end[%ld], timeout[%ld], used[%ld]", inet_ntoa_r(connection->getPeerId()),
          org_get_param->get_cell_size(), start_time, end_time, req_timeout_us, end_time - start_time);
      }
      FILL_TRACE_LOG("step 5. send reponse scanner for ms_get:send_err[%d], ms_used[%ld], total_used[%ld], code[%d]",
            send_err, end_time - my_start, end_time - start_time, err_code);

      /// inc monitor counter
      if (NULL != service_monitor_)
      {
        int64_t table_id = 0;
        if ((*decoded_get_param)[0] != NULL)
        {
          table_id = (*decoded_get_param)[0]->table_id_;
        }
        if (OB_SUCCESS == err_code)
        {
          service_monitor_->inc(table_id, ObMergerServiceMonitor::SUCC_GET_OP_COUNT);
          service_monitor_->inc(table_id, ObMergerServiceMonitor::SUCC_GET_OP_TIME, end_time - start_time);
        }
        else
        {
          service_monitor_->inc(table_id, ObMergerServiceMonitor::FAIL_GET_OP_COUNT);
          service_monitor_->inc(table_id, ObMergerServiceMonitor::FAIL_GET_OP_TIME, end_time - start_time);
        }
      }

      if (NULL != schema_mgr)
      {
        schema_proxy_->release_schema(schema_mgr);
      }
      PRINT_TRACE_LOG();
      CLEAR_TRACE_LOG();
      if (NULL != get_event)
      {
        org_get_param->reset();
        get_param_with_name->reset();
        decoded_get_param->reset();
        result_scanner->reset();
        get_event->reset();
      }
      if (session_id > 0)
      {
        session_mgr_.session_end(session_id);
      }
      if (net_session_id > 0)
      {
        queue_thread.destroy_session(net_session_id);
      }
      return send_err;
    }


    int ObMergeServerService::ms_mutate(
      const int64_t start_time,
      const int32_t version,
      const int32_t channel_id,
      tbnet::Connection* connection,
      common::ObDataBuffer& in_buffer,
      common::ObDataBuffer& out_buffer,
      const int64_t timeout_us)
    {
      FILL_TRACE_LOG("step 1. start serve ms_mutate [peer:%s]", inet_ntoa_r(connection->getPeerId()));
      int64_t req_timeout_us = timeout_us;
      if ((0 >= timeout_us) || (timeout_us > merge_server_->get_params().get_max_req_process_time()))
      {
        req_timeout_us = merge_server_->get_params().get_max_req_process_time();
        TBSYS_LOG(DEBUG, "reset timeoutus %ld", req_timeout_us);
      }
      const int32_t MS_MUTATE_VERSION = 1;
      int32_t send_err  = OB_SUCCESS;
      ObResultCode rc;
      int32_t &err = rc.result_code_;
      err = OB_SUCCESS;
      const ObSchemaManagerV2 *schema_mgr = NULL;
      ObGetParam *org_get_param = GET_TSI_MULT(ObGetParam, TSI_MS_ORG_GET_PARAM_1);
      ObGetParam *decoded_get_param = GET_TSI_MULT(ObGetParam, TSI_MS_DECODED_GET_PARAM_1);
      ObMutator *org_mutator_param = GET_TSI_MULT(ObMutator, TSI_MS_ORG_MUTATOR_1);
      ObMutator *decoded_mutator_param = GET_TSI_MULT(ObMutator, TSI_MS_DECODED_MUTATOR_1);
      ObScanner *ups_result = GET_TSI_MULT(ObScanner, TSI_MS_UPS_SCANNER_1);
      ObScanner *result_scanner = GET_TSI_MULT(ObScanner, TSI_MS_SCANNER_1);
      ObGetMergeJoinAgentImp *agent = GET_TSI_MULT(ObGetMergeJoinAgentImp, ORG_PARAM_ID);
      if (OB_SUCCESS == err && MS_MUTATE_VERSION != version)
      {
        err = OB_ERROR_FUNC_VERSION;
      }
      if (OB_SUCCESS == err &&
        (NULL == org_get_param || NULL == decoded_get_param || NULL == org_mutator_param
        || NULL == decoded_mutator_param || NULL == ups_result || NULL == result_scanner || NULL == agent))
      {
        TBSYS_LOG(WARN,"fail to allocate memory for request");
        err = OB_ALLOCATE_MEMORY_FAILED;
      }
      else
      {
        org_mutator_param->reset();
        decoded_mutator_param->reset();
        org_get_param->reset();
        decoded_get_param->reset();
        ups_result->clear();
        result_scanner->clear();
      }
      /// decode request
      if (OB_SUCCESS == err)
      {
        err = org_mutator_param->deserialize(in_buffer.get_data(), in_buffer.get_capacity(),
          in_buffer.get_position());
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN,"fail to parse ObMutator [err:%d]",err);
        }
      }
      /// get local newest schema
      if (OB_SUCCESS == err)
      {
        err = schema_proxy_->get_schema(ObMergerRpcProxy::LOCAL_NEWEST, &schema_mgr);
        if ((OB_SUCCESS != err) || (NULL == schema_mgr))
        {
          TBSYS_LOG(WARN, "%s", "fail to get the latest schema, unexpected error");
          err = OB_ERR_UNEXPECTED;
        }
      }
      /// decode and check mutator param
      if (OB_SUCCESS == err)
      {
        err = ObMutatorParamDecoder::decode(*org_mutator_param, *schema_mgr, *decoded_mutator_param,
          *org_get_param, *decoded_get_param);
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN,"fail to decode mutator param [err:%d]", err);
        }
      }
      FILL_TRACE_LOG("step 2. finish parse the schema for ms_mutate:err[%d]", err);
      TBSYS_LOG(DEBUG, "need prefetch data:prefetch_cell[%ld], return_cell[%ld]",
        decoded_get_param->get_cell_size(), org_get_param->get_cell_size());
      /// do request for get static data
      if ((OB_SUCCESS == err) && (decoded_get_param->get_cell_size() > 0))
      {
        ObMergerPrefetchData prefetcher(merge_server_->get_params().get_intermediate_buffer_size(),
          *rpc_proxy_, *data_version_proxy_, *schema_mgr);
        err = prefetcher.prefetch(req_timeout_us, *agent, *decoded_get_param, *decoded_mutator_param);
      }
      FILL_TRACE_LOG("step 3. finish get prefetch data for mutate:prefetch[%ld], return[%ld], err[%d]",
        decoded_get_param->get_cell_size(), org_get_param->get_cell_size(), err);
      int64_t end_time = tbsys::CTimeUtil::getTime();
      if (end_time > start_time + req_timeout_us)
      {
        err = OB_PROCESS_TIMEOUT;
        TBSYS_LOG(WARN, "check process mutate task timeout:start[%ld], end[%ld], timeout[%ld], used[%ld]",
          start_time, end_time, req_timeout_us, end_time - start_time);
      }
      /// send ups request
      if (OB_SUCCESS == err)
      {
        err = rpc_proxy_->ups_mutate(*decoded_mutator_param, org_get_param->get_cell_size() > 0, *ups_result);
      }
      FILL_TRACE_LOG("step 4. ups mutate finished:err[%d]", err);
      /// encode id to names
      if ((OB_SUCCESS == err) && (org_get_param->get_cell_size() > 0))
      {
        err = ObMergerScannerEncoder::encode(*org_get_param, *ups_result, *result_scanner);
      }
      FILL_TRACE_LOG("step 5. convert mutate scanner id to name finished:err[%d]", err);
      int err_code = rc.result_code_;
      /// always send error code
      err = rc.serialize(out_buffer.get_data(),out_buffer.get_capacity(), out_buffer.get_position());
      if (OB_SUCCESS == err && !result_scanner->is_empty())
      {
        err = result_scanner->serialize(out_buffer.get_data(),out_buffer.get_capacity(),
          out_buffer.get_position());
      }
      FILL_TRACE_LOG("step 6. finish serialize the scanner result for ms_scan:err[%d]", err);
      if (OB_SUCCESS == err)
      {
        send_err = merge_server_->send_response(OB_MS_MUTATE_RESPONSE, MS_MUTATE_VERSION,out_buffer,
          connection,channel_id);
      }
      // check process timeout
      end_time = tbsys::CTimeUtil::getTime();
      if (end_time > start_time + req_timeout_us)
      {
        err_code = OB_PROCESS_TIMEOUT;
        TBSYS_LOG(WARN, "check process mutate task timeout:start[%ld], end[%ld], timeout[%ld], used[%ld]",
          start_time, end_time, req_timeout_us, end_time - start_time);
      }
      FILL_TRACE_LOG("step 7. at last send scanner reponse for ms_scan:send_err[%d], code[%d]",
        send_err, err_code);
      if (NULL != schema_mgr)
      {
        rpc_proxy_->release_schema(schema_mgr);
      }
      if (NULL != agent)
      {
        agent->clear();
      }
      PRINT_TRACE_LOG();
      CLEAR_TRACE_LOG();
      return send_err;
    }

    int ObMergeServerService::ms_scan(
      const int64_t start_time,
      const int32_t version,
      const int32_t channel_id,
      tbnet::Connection* connection,
      common::ObDataBuffer& in_buffer,
      common::ObDataBuffer& out_buffer,
      const int64_t timeout_us)
    {
      int64_t req_timeout_us = timeout_us;
      if ((0 >= timeout_us) || (timeout_us > merge_server_->get_params().get_max_req_process_time()))
      {
        req_timeout_us = merge_server_->get_params().get_max_req_process_time();
        TBSYS_LOG(DEBUG, "reset timeoutus %ld", req_timeout_us);
      }
      int64_t my_start = tbsys::CTimeUtil::getTime();
      int32_t request_cid = channel_id;
      const int32_t MS_SCAN_VERSION = 1;
      ObResultCode rc;
      char err_msg[MAX_ERROR_MSG_LEN]="";
      rc.message_.assign(err_msg, sizeof(err_msg));
      int32_t &err = rc.result_code_;
      int32_t send_err  = OB_SUCCESS;
      err = OB_SUCCESS;
      ObScanParam *org_scan_param = GET_TSI_MULT(ObScanParam, TSI_MS_ORG_SCAN_PARAM_1);
      ObScanParam *decoded_scan_param = GET_TSI_MULT(ObScanParam, TSI_MS_DECODED_SCAN_PARAM_1);
      ObReadParam & decoded_read_param = *decoded_scan_param;
      ObMergerScanParam *ms_scan_param = GET_TSI_MULT(ObMergerScanParam, TSI_MS_MS_SCAN_PARAM_1);
      ObScanner *result_scanner = GET_TSI_MULT(ObScanner, TSI_MS_SCANNER_1);
      ObMergerScanEvent *scan_event = GET_TSI_ARGS(ObMergerScanEvent, TSI_MS_SCAN_EVENT_1, cache_proxy_, async_rpc_);
      const ObSchemaManagerV2 *schema_mgr = NULL;
      int64_t timeout_time = start_time + req_timeout_us;
      if (OB_SUCCESS == err && MS_SCAN_VERSION != version)
      {
        err = OB_ERROR_FUNC_VERSION;
      }
      if (OB_SUCCESS == err &&
          ((NULL == org_scan_param)
           || (NULL == decoded_scan_param)
           || (NULL == result_scanner)
           || (NULL == ms_scan_param)
           || (NULL == scan_event)))
      {
        TBSYS_LOG(WARN,"fail to allocate memory for request");
        err = OB_ALLOCATE_MEMORY_FAILED;
      }
      else if (OB_SUCCESS == err)
      {
        if ((!scan_event->inited())
            && ( OB_SUCCESS != (err = scan_event->init(REQUEST_EVENT_QUEUE_SIZE,
                  ObModIds::OB_MS_REQUEST_EVENT, &session_mgr_))))
        {
          TBSYS_LOG(WARN,"fail to init ObMergerScanEvent [err:%d]", err);
        }
      }

      /// decode request
      if (OB_SUCCESS == err)
      {
        FILL_TRACE_LOG("step 1. start serve ms_scan [request_event_id:%lu]", scan_event->get_request_id());
        err = org_scan_param->deserialize(in_buffer.get_data(), in_buffer.get_capacity(),
            in_buffer.get_position());
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN,"fail to deserialize ObScanParam [err:%d]",err);
          snprintf(rc.message_.ptr(), rc.message_.length(), "fail to deserialize ObScanParam [err:%d]", err);
        }
        else
        {
          decoded_read_param = *dynamic_cast<ObReadParam*>(org_scan_param);
        }
      }

      /// get local newest schema
      if (OB_SUCCESS == err)
      {
        err = schema_proxy_->get_schema(ObMergerSchemaProxy::LOCAL_NEWEST, &schema_mgr);
        if ((OB_SUCCESS != err) || (NULL == schema_mgr))
        {
          TBSYS_LOG(WARN, "%s", "fail to get the latest schema, unexpected error");
          err = OB_ERR_UNEXPECTED;
        }
      }

      bool is_cache_hit = false;
      uint64_t session_id = 0;
      bool got_all_result = false;
      bool session_next = merge_server_->get_params().get_support_session_next();
      int64_t net_session_id = 0;
      int64_t packet_cnt = 0;
      ObPacketQueueThread &queue_thread = merge_server_->get_default_task_queue_thread();
      ObPacket* next_request = NULL;
      int & err_code = rc.result_code_;

      if ((OB_SUCCESS == err) && NULL != query_cache_
          && org_scan_param->get_is_result_cached()
          && schema_mgr->get_table_query_cache_expire_time(org_scan_param->get_table_name()) > 0)
      {
        ObString result_key;
        result_key.assign_ptr(in_buffer.get_data(), static_cast<int32_t>(in_buffer.get_position()));
        if (OB_SUCCESS == query_cache_->get(result_key, out_buffer))
        {
          is_cache_hit = true;
        }
      }

      if (OB_SUCCESS == err && is_cache_hit)
      {
        // query cache hit
        if ((OB_SUCCESS != (send_err = merge_server_->send_response(OB_SCAN_RESPONSE,
                  MS_SCAN_VERSION,out_buffer,connection,request_cid, 0))))
        {
          TBSYS_LOG(WARN,"fail to send response to client [err:%d]", send_err);
        }
        if (OB_SUCCESS == send_err)
        {
          packet_cnt ++;
        }
        FILL_TRACE_LOG("step 2. query cache hit, send_response:send_err[%d], code[%d], packet_cnt[%ld]",
            send_err, err, packet_cnt);
      }
      else
      {
        //query cache miss
        /// decode and check scan param
        if (OB_SUCCESS == err)
        {
          err = ob_decode_scan_param(*org_scan_param, *schema_mgr, *decoded_scan_param, &rc);
          if (OB_SUCCESS != err)
          {
            TBSYS_LOG(WARN,"fail to decode scan param [err:%d]", err);
          }
        }

        if ((OB_SUCCESS == err)
            && (OB_SUCCESS != (err = session_mgr_.session_begin(*org_scan_param,
                  connection->getPeerId(),session_id, syscall(SYS_gettid), static_cast<pid_t>(pthread_self())))))
        {
          TBSYS_LOG(WARN,"fail to create session in SessionManager [err:%d]", err);
        }
        if (OB_SUCCESS == err)
        {
          scan_event->set_session_id(static_cast<uint32_t>(session_id));
        }

        if ((OB_SUCCESS == err) && (OB_SUCCESS != (err = ms_scan_param->set_param(*decoded_scan_param))))
        {
          TBSYS_LOG(WARN,"fail to set ObMergerScanParam [err:%d]", err);
        }
        FILL_TRACE_LOG("step 2. finish parse the schema for ms_scan:err[%d]", err);

        /// do request
        if (OB_SUCCESS == err)
        {
          scan_event->set_timeout_percent(merge_server_->get_params().get_max_timeout_percent());
          const ObRange *range = ms_scan_param->get_ms_param()->get_range();
          if (range->is_whole_range())
          {
            err = scan_event->set_request_param(*ms_scan_param,
                merge_server_->get_params().get_intermediate_buffer_size(), req_timeout_us, 1);
          }
          else
          {
            err = scan_event->set_request_param(*ms_scan_param,
                merge_server_->get_params().get_intermediate_buffer_size(),
                req_timeout_us, merge_server_->get_params().get_max_parellel_count());
          }
          // ITER_END for import tablet not server
          if (err != OB_SUCCESS)
          {
            TBSYS_LOG(ERROR, "scan request set param failed:err[%d]", err);
          }
        }

        /// process result
        if ((OB_SUCCESS == err) && (OB_SUCCESS != (err = scan_event->wait(timeout_time - tbsys::CTimeUtil::getTime()))))
        {
          TBSYS_LOG(WARN,"wait scan request failed:[err:%d]", err);
        }

        if (OB_SUCCESS == err)
        {
          FILL_TRACE_LOG("mem_size_used[%ld]", scan_event->get_mem_size_used());
        }

        /// prepare result
        if ((OB_SUCCESS == err) && (OB_SUCCESS != (err = scan_event->fill_result(*result_scanner,*org_scan_param,
                  got_all_result))))
        {
          TBSYS_LOG(WARN,"fail to faill result [err:%d]", err);
        }
        if ((OB_SUCCESS == err) && session_next && (!got_all_result))
        {
          net_session_id = queue_thread.generate_session_id();
        }

        do
        {
          if ((OB_SUCCESS == err) && session_next && (!got_all_result) && (packet_cnt > 0))
          {
            result_scanner->reset();
            if ((OB_SUCCESS == err) && (OB_SUCCESS != (err = scan_event->fill_result(*result_scanner,*org_scan_param,
                      got_all_result))))
            {
              TBSYS_LOG(WARN, "fail to fill result [err:%d]", err);
            }
          }
          if ((OB_SUCCESS == err) && session_next && !got_all_result && (OB_SUCCESS !=
                queue_thread.prepare_for_next_request(net_session_id)))
          {
            TBSYS_LOG(WARN, "fail to prepare network session [err:%d]", err);
          }
          out_buffer.get_position() = 0;
          /// always send error code
          if ((OB_SUCCESS != (send_err = rc.serialize(out_buffer.get_data(), out_buffer.get_capacity(),
                    out_buffer.get_position()))))
          {
            TBSYS_LOG(WARN,"fail to serialize result code [err:%d]", send_err);
          }
          if ((OB_SUCCESS == err) && (OB_SUCCESS == send_err)
              && (OB_SUCCESS !=(send_err = result_scanner->serialize(out_buffer.get_data(),
                  out_buffer.get_capacity(), out_buffer.get_position()))))
          {
            TBSYS_LOG(WARN,"fail to serialize result scanner [err:%d]", send_err);
          }
          if ((OB_SUCCESS == err) && (OB_SUCCESS == send_err)
              && got_all_result && 0 == packet_cnt && NULL != query_cache_
              && schema_mgr->get_table_query_cache_expire_time(org_scan_param->get_table_name()) > 0)
          {
            ObString result_key;
            ObQueryCacheValue result_value;
            result_key.assign_ptr(in_buffer.get_data(),  static_cast<int32_t>(in_buffer.get_capacity()));
            result_value.expire_time_ =
              schema_mgr->get_table_query_cache_expire_time(org_scan_param->get_table_name())
              + tbsys::CTimeUtil::getTime();
            result_value.size_ = out_buffer.get_position();
            result_value.buf_ = out_buffer.get_data();
            err = query_cache_->put(result_key, result_value);
            if (OB_SUCCESS != err)
            {
              TBSYS_LOG(WARN, "failed to put scan result into query cache:err[%d]", err);
              err = OB_SUCCESS;
            }
          }
          FILL_TRACE_LOG("step 4.x. finish serialize scanner:err[%d]", send_err);
          if ((OB_SUCCESS == send_err) &&
              (OB_SUCCESS != (send_err = merge_server_->send_response(OB_SCAN_RESPONSE, MS_SCAN_VERSION,out_buffer,
                  connection,request_cid, (got_all_result?0:net_session_id)))))
          {
            TBSYS_LOG(WARN,"fail to send response to client [err:%d]", send_err);
          }
          if (OB_SUCCESS == send_err)
          {
            packet_cnt ++;
          }
          FILL_TRACE_LOG("step 5.x. send_response:send_err[%d], code[%d], packet_cnt[%ld]",
              send_err, err_code, packet_cnt);
          if ((OB_SUCCESS == err) && session_next && (!got_all_result) && (OB_SUCCESS == send_err))
          {
            err = queue_thread.wait_for_next_request(net_session_id,next_request,
                timeout_time - tbsys::CTimeUtil::getTime());
            if (OB_SUCCESS != err)
            {
              TBSYS_LOG(WARN,"fail to wait next request from session [err:%d,net_session_id:%ld]",
                  err, net_session_id);
            }
            else
            {
              request_cid = next_request->getChannelId();
            }
          }
        } while (session_next && (!got_all_result) && (OB_SUCCESS == send_err) && (OB_SUCCESS == err));
      }

      int64_t end_time = tbsys::CTimeUtil::getTime();
      if (OB_SUCCESS == err)
      {
        ms_get_counter_set().inc(ObMergerCounterIds::C_CLIENT_SCAN, end_time - start_time);
        ms_get_counter_set().inc(get_counter_id_for_proc_time(end_time - start_time));
        if (end_time - my_start > merge_server_->get_params().get_slow_query_threshold())
        {
          const char *session_info = NULL;
          if (NULL == (session_info = session_mgr_.get_session(session_id)))
          {
            TBSYS_LOG(WARN,"get empty session info");
          }
          else
          {
            TBSYS_LOG(WARN,"slow query:%s; used_time:%ld; total_time:%ld result_size:%ld",
                session_info, end_time - my_start, end_time - start_time, result_scanner->get_size());
          }
        }
      }

      // check process timeout
      if ((err_code != OB_SUCCESS) || (end_time > start_time + req_timeout_us))
      {
        if (OB_SUCCESS == err_code)
        {
          err_code = OB_PROCESS_TIMEOUT;
        }
        TBSYS_LOG(WARN, "check process scan task timeout or failed:client[%s], start[%ld], end[%ld], "
            "timeout[%ld], used[%ld]", inet_ntoa_r(connection->getPeerId()),
            start_time, end_time, req_timeout_us, end_time - start_time);
      }
      FILL_TRACE_LOG("step 5. send scanner reponse for ms_scan:send_err[%d], ms_used[%ld], "
          "total_used[%ld], errcode[%d]", send_err, end_time - my_start, end_time - start_time, err_code);

      /// inc monitor counter
      if (NULL != service_monitor_ )
      {
        uint64_t table_id = decoded_scan_param->get_table_id();
        if (OB_SUCCESS == err_code)
        {
          service_monitor_->inc(table_id, ObMergerServiceMonitor::SUCC_SCAN_OP_COUNT);
          service_monitor_->inc(table_id, ObMergerServiceMonitor::SUCC_SCAN_OP_TIME, end_time - start_time);
          if (NULL != scan_event && !scan_event->get_is_result_precision())
          {
            service_monitor_->inc(table_id, ObMergerServiceMonitor::SUCC_SCAN_NOT_PRECISION_OP_COUNT);
          }
        }
        else
        {
          service_monitor_->inc(table_id, ObMergerServiceMonitor::FAIL_SCAN_OP_COUNT);
          service_monitor_->inc(table_id, ObMergerServiceMonitor::FAIL_SCAN_OP_TIME, end_time - start_time);
        }
      }
      if (NULL != schema_mgr)
      {
        schema_proxy_->release_schema(schema_mgr);
      }
      PRINT_TRACE_LOG();
      CLEAR_TRACE_LOG();
      if (NULL != scan_event)
      {
        org_scan_param->reset();
        decoded_scan_param->reset();
        result_scanner->reset();
        ms_scan_param->reset();
        scan_event->reset();
      }
      ObMSSchemaDecoderAssis *schema_assis = GET_TSI_MULT(ObMSSchemaDecoderAssis, TSI_MS_SCHEMA_DECODER_ASSIS_1);
      if (NULL != schema_assis)
      {
        schema_assis->clear();
      }
      if (session_id > 0)
      {
        session_mgr_.session_end(session_id);
      }
      if (net_session_id > 0)
      {
        queue_thread.destroy_session(net_session_id);
      }
      return send_err;
    }

    int ObMergeServerService::ms_change_log_level(
      const int64_t receive_time,
      const int32_t version,
      const int32_t channel_id,
      tbnet::Connection* connection,
      common::ObDataBuffer& in_buffer,
      common::ObDataBuffer& out_buffer,
      const int64_t timeout_us)
    {
      int ret = OB_SUCCESS;
      static const int MY_VERSION = 1;
      UNUSED(receive_time);
      UNUSED(version);
      UNUSED(timeout_us);
      common::ObResultCode result;
      result.result_code_ = OB_SUCCESS;
      int32_t log_level = -1;
      if (OB_SUCCESS != (ret = serialization::decode_vi32(in_buffer.get_data(), in_buffer.get_capacity(),
          in_buffer.get_position(), &log_level)))
      {
        TBSYS_LOG(WARN, "deserialize error, err=%d", ret);
      }
      else
      {
        if ((TBSYS_LOG_LEVEL_ERROR <= log_level) && (TBSYS_LOG_LEVEL_DEBUG >= log_level))
        {
          TBSYS_LOG(INFO, "change log level. From: %d, To: %d", TBSYS_LOGGER._level, log_level);
          TBSYS_LOGGER._level = log_level;
        }
        else
        {
          TBSYS_LOG(WARN, "invalid log level, level=%d", log_level);
          result.result_code_ = OB_INVALID_ARGUMENT;
        }
        if (OB_SUCCESS != (ret = result.serialize(out_buffer.get_data(), out_buffer.get_capacity(),
            out_buffer.get_position())))
        {
          TBSYS_LOG(WARN, "serialize error, err=%d", ret);
        }
        else
        {
          ret = merge_server_->send_response(OB_RS_ADMIN_RESPONSE, MY_VERSION, out_buffer, connection, channel_id);
        }
      }
      return ret;
    }
  } /* mergeserver */
} /* oceanbase */
