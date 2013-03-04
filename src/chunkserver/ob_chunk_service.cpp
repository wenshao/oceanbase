/*
 *  (C) 2007-2010 Taobao Inc.
 *  
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License version 2 as
 *  published by the Free Software Foundation.
 *
 *         ????.cpp is for what ...
 *
 *  Version: $Id: ipvsadm.c,v 1.27 2005/12/10 16:00:07 wensong Exp $
 *
 *  Authors:
 *     Author Name <email address>
 *        - some work details if you want
 */

#include "ob_chunk_service.h"
#include "ob_chunk_server.h"
#include "common/ob_atomic.h"
#include "common/utility.h"
#include "common/ob_define.h"
#include "common/data_buffer.h"
#include "common/ob_packet.h"
#include "common/ob_read_common_data.h"
#include "common/ob_scanner.h"
#include "common/ob_result.h"
#include "common/file_directory_utils.h"
#include "common/ob_trace_log.h"
#include "sstable/ob_disk_path.h"
#include "sstable/ob_aio_buffer_mgr.h"
#include "ob_tablet.h"
#include "ob_chunk_server_main.h"
#include "ob_query_service.h"
#include "compactsstable/ob_compactsstable_mem.h"

using namespace oceanbase::common;
using namespace oceanbase::sstable;
using namespace oceanbase::compactsstable;

namespace oceanbase 
{ 
  namespace chunkserver 
  {
    ObChunkService::ObChunkService()
    : chunk_server_(NULL), inited_(false), 
      service_started_(false), in_register_process_(false), 
      service_expired_time_(0), merge_delay_interval_(1),
      migrate_task_count_(0), lease_checker_(this), merge_task_(this),
      fetch_ups_task_(this), query_service_buffer_(sizeof(ObQueryService))
    {
    }

    ObChunkService::~ObChunkService()
    {
      destroy();
    }

    /**
     * use ObChunkService after initialized.
     */
    int ObChunkService::initialize(ObChunkServer* chunk_server)
    {
      int rc = OB_SUCCESS;

      if (inited_) 
      {
        rc = OB_INIT_TWICE;
      }
      else if (NULL == chunk_server)
      {
        rc = OB_INVALID_ARGUMENT;
      }
      else
      {
        chunk_server_ = chunk_server;
        inited_ = true;
      }

      if (OB_SUCCESS == rc)
      {
        rc = timer_.init();
      }

      inited_ = (OB_SUCCESS == rc);
      return rc;
    }

    /*
     * stop service, before chunkserver stop working thread.
     */
    int ObChunkService::destroy()
    {
      int rc = OB_SUCCESS;
      if (inited_)
      {
        inited_ = false;
        timer_.destroy();
        service_started_ = false;
        in_register_process_ = false;
        chunk_server_ = NULL;
      }
      else
      {
        rc = OB_NOT_INIT;
      }

      return rc;
    }

    /**
     * ChunkServer must fetch schema from RootServer first.
     * then provide service.
     */
    int ObChunkService::start()
    {
      int rc = OB_SUCCESS;
      if (!inited_)
      {
        rc = OB_NOT_INIT;
      }
      else
      {
        rc = chunk_server_->init_merge_join_rpc();
        if (rc != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "init merge join rpc failed.");
        }
      }

      if (OB_SUCCESS == rc)
      {
        rc = load_tablets();
        if (OB_SUCCESS != rc) 
        {
          TBSYS_LOG(ERROR, "load local tablets error, rc=%d", rc);
        }
      }

      if (OB_SUCCESS == rc) 
      {
        rc = register_self();
      }

      if (OB_SUCCESS == rc) 
      {
        rc = timer_.schedule(lease_checker_, 
            chunk_server_->get_param().get_lease_check_interval(), false);
      }

      if (OB_SUCCESS == rc)
      {
        get_merge_delay_interval();
      }

      if (OB_SUCCESS == rc)
      {
        //for the sake of simple,just update the stat per second
        rc = timer_.schedule(stat_updater_,1000000,true);
      }

      if (OB_SUCCESS == rc)
      {
        rc = timer_.schedule(fetch_ups_task_, 
          chunk_server_->get_param().get_fetch_ups_interval(), false);
      }

      if (OB_SUCCESS == rc)
      {
        ObTabletManager & tablet_manager = chunk_server_->get_tablet_manager();
        rc = tablet_manager.start_merge_thread();
        if (rc != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR,"start merge thread failed.");
        }

        int64_t compactsstable_cache_size = chunk_server_->get_param().get_compactsstable_cache_size();
        if ((OB_SUCCESS == rc) && (compactsstable_cache_size > 0))
        {
          if ((rc = tablet_manager.start_cache_thread()) != OB_SUCCESS)
          {
            TBSYS_LOG(ERROR,"start cache thread failed");
          }
        }
      }

      return rc;
    }

    int ObChunkService::load_tablets()
    {
      int rc = OB_SUCCESS;
      if (!inited_)
      {
        rc = OB_NOT_INIT;
      }

      ObTabletManager & tablet_manager = chunk_server_->get_tablet_manager();
      // load tablets;
      if (OB_SUCCESS == rc)
      {
        int32_t size = 0;
        const int32_t *disk_no_array = tablet_manager.get_disk_manager().get_disk_no_array(size);
        if (disk_no_array != NULL && size > 0)
        {
          rc = tablet_manager.load_tablets(disk_no_array, size);
        }
        else
        {
          rc = OB_ERROR;
          TBSYS_LOG(ERROR, "get disk no array failed.");
        }
      }

      return rc;
    }

    int ObChunkService::register_self_busy_wait(int32_t &status)
    {
      int rc = OB_SUCCESS;
      status = 0;
      ObRootServerRpcStub & rs_rpc_stub = chunk_server_->get_rs_rpc_stub();
      while (inited_)
      {
        rc = rs_rpc_stub.register_server(chunk_server_->get_self(), false, status);
        if (OB_SUCCESS == rc) break;
        if (OB_RESPONSE_TIME_OUT != rc && OB_NOT_INIT != rc) 
        {
          TBSYS_LOG(ERROR, "register self to rootserver failed, rc=%d", rc);
          break;
        }
        usleep(static_cast<useconds_t>(chunk_server_->get_param().get_network_time_out()));
      }
      return rc;
    }

    int ObChunkService::report_tablets_busy_wait()
    {
      int rc = OB_SUCCESS;
      ObTabletManager & tablet_manager = chunk_server_->get_tablet_manager();
      while (inited_ && (service_started_ || (in_register_process_ && !service_started_)))
      {
        rc = tablet_manager.report_tablets();
        if (OB_SUCCESS == rc) break;
        if (OB_RESPONSE_TIME_OUT != rc) 
        {
          TBSYS_LOG(ERROR, "report tablets to rootserver failed, rc=%d", rc);
          break;
        }
        usleep(static_cast<useconds_t>(chunk_server_->get_param().get_network_time_out()));
      }
      return rc;
    }

    int ObChunkService::fetch_schema_busy_wait(ObSchemaManagerV2 *schema)
    {
      int rc = OB_SUCCESS;
      if (NULL == schema) 
      {
        TBSYS_LOG(ERROR,"invalid argument,sceham is null");
        rc = OB_INVALID_ARGUMENT;
      }
      else
      {
        while (inited_)
        {
          rc = chunk_server_->get_rs_rpc_stub().fetch_schema(0, *schema);
          if (OB_SUCCESS == rc) break;
          if (OB_RESPONSE_TIME_OUT != rc) 
          {
            TBSYS_LOG(ERROR, "report tablets to rootserver failed, rc=%d", rc);
            break;
          }
          usleep(static_cast<useconds_t>(chunk_server_->get_param().get_network_time_out()));
        }
      }
      return rc;
    }

    int ObChunkService::register_self()
    {
      int rc = OB_SUCCESS;
      if (!inited_)
      {
        TBSYS_LOG(ERROR, "service not initialized, cannot register_self.");
        rc = OB_NOT_INIT;
      }

      if (in_register_process_)
      {
        TBSYS_LOG(ERROR, "another thread is registering.");
        rc = OB_ERROR;
      }
      else
      {
        in_register_process_ = true;
      }

      ObTabletManager & tablet_manager = chunk_server_->get_tablet_manager();
      //const ObChunkServerParam & param = chunk_server_->get_param();
      //ObRootServerRpcStub & rs_rpc_stub = chunk_server_->get_rs_rpc_stub();
      int32_t status = 0;
      // register self to rootserver until success.
      if (OB_SUCCESS == rc)
      {
        rc = register_self_busy_wait(status);
      }

      if (OB_SUCCESS == rc)
      {
        // TODO init lease 10s for first startup.
        service_expired_time_ = tbsys::CTimeUtil::getTime() + 10000000;
        int64_t current_data_version = tablet_manager.get_serving_data_version();
        if (0 == status)
        {
          TBSYS_LOG(INFO, "system startup on first time, wait rootserver start new schema,"
              "current data version=%ld", current_data_version);
          // start chunkserver on very first time, do nothing, wait rootserver
          // launch the start_new_schema process.
          //service_started_ = true;
        }
        else
        {
          TBSYS_LOG(INFO, "chunk service start, current data version: %ld", current_data_version);
          rc = report_tablets_busy_wait();
          if (OB_SUCCESS == rc)
          {
            tablet_manager.report_capacity_info();
            service_started_ = true;
          }
        }
      }

      in_register_process_ = false;

      return rc;
    }

    /*
     * after initialize() & start(), then can handle the request.
     */
    int ObChunkService::do_request(
        const int64_t receive_time,
        const int32_t packet_code,
        const int32_t version,
        const int32_t channel_id,
        tbnet::Connection* connection,
        common::ObDataBuffer& in_buffer, 
        common::ObDataBuffer& out_buffer,
        const int64_t timeout_time)
    {
      int rc = OB_SUCCESS;
      if (!inited_)
      {
        TBSYS_LOG(ERROR, "service not initialized, cannot accept any message.");
        rc = OB_NOT_INIT;
      }

      if (OB_SUCCESS == rc)
      {
        if (!service_started_ 
            && packet_code != OB_START_MERGE 
            && packet_code != OB_REQUIRE_HEARTBEAT)
        {
          TBSYS_LOG(ERROR, "service not started, only accept "
              "start schema message or heatbeat from rootserver.");
          rc = OB_CS_SERVICE_NOT_STARTED;
        }
      }
      if (OB_SUCCESS == rc)
      {
        //check lease valid.
        if (!is_valid_lease()  
            && (!in_register_process_) 
            && packet_code != OB_REQUIRE_HEARTBEAT)
        {
          // TODO re-register self??
          TBSYS_LOG(WARN, "lease expired, wait timer schedule re-register self to rootserver.");
        }
      }

      if (OB_SUCCESS != rc)
      {
        TBSYS_LOG(ERROR, "call func error packet_code is %d return code is %d",
            packet_code, rc);
        common::ObResultCode result;
        result.result_code_ = rc;
        // send response.
        int serialize_ret = result.serialize(out_buffer.get_data(), 
            out_buffer.get_capacity(), out_buffer.get_position());
        if (OB_SUCCESS != serialize_ret)
        {
          TBSYS_LOG(ERROR, "serialize result code object failed.");
        }
        else
        {  
          chunk_server_->send_response(
              packet_code + 1, version, 
              out_buffer, connection, channel_id);
        }
      }
      else
      {
        switch(packet_code)
        {
          case OB_GET_REQUEST:
            rc = cs_get(receive_time, version, channel_id, connection, in_buffer, out_buffer, timeout_time);
            break;
          case OB_SCAN_REQUEST:
            rc = cs_scan(receive_time, version, channel_id, connection, in_buffer, out_buffer, timeout_time);
            break;
          case OB_BATCH_GET_REQUEST:
            rc = cs_batch_get(version, channel_id, connection, in_buffer, out_buffer);
            break;
          case OB_DROP_OLD_TABLETS: 
            rc = cs_drop_old_tablets(version, channel_id, connection, in_buffer, out_buffer);
            break;
          case OB_REQUIRE_HEARTBEAT:
            rc = cs_heart_beat(version, channel_id, connection, in_buffer, out_buffer);
            break;
          case OB_CS_MIGRATE: 
            rc = cs_migrate_tablet(version, channel_id, connection, in_buffer, out_buffer);
            break;
          case OB_MIGRATE_OVER: 
            rc = cs_load_tablet(version, channel_id, connection, in_buffer, out_buffer);
            break;
          case OB_CS_DELETE_TABLETS: 
            rc = cs_delete_tablets(version, channel_id, connection, in_buffer, out_buffer);
            break;
          case OB_CS_CREATE_TABLE:
            rc = cs_create_tablet(version, channel_id, connection, in_buffer, out_buffer);
            break;
          case OB_CS_GET_MIGRATE_DEST_LOC:
            rc = cs_get_migrate_dest_loc(version, channel_id, connection, in_buffer, out_buffer);
            break;
          case OB_CS_DUMP_TABLET_IMAGE:
            rc = cs_dump_tablet_image(version, channel_id, connection, in_buffer, out_buffer);
            break;
          case OB_FETCH_STATS:
            rc = cs_fetch_stats(version, channel_id, connection, in_buffer, out_buffer);
            break;
          case OB_CS_START_GC:
            rc = cs_start_gc(version, channel_id, connection, in_buffer, out_buffer);
            break;
          case OB_UPS_RELOAD_CONF:
            rc = cs_reload_conf(version, channel_id, connection, in_buffer, out_buffer);
            break;
          case OB_CS_SHOW_PARAM:
            rc = cs_show_param(version, channel_id, connection, in_buffer, out_buffer);
            break;
          case OB_STOP_SERVER:
            rc = cs_stop_server(version, channel_id, connection, in_buffer, out_buffer);
            break;
          case OB_CHANGE_LOG_LEVEL:
            rc = cs_change_log_level(version, channel_id, connection, in_buffer, out_buffer);
            break;
          case OB_RS_REQUEST_REPORT_TABLET:
            rc = cs_force_to_report_tablet(version, channel_id, connection, in_buffer, out_buffer);
            break;
          case OB_CS_CHECK_TABLET:
            rc = cs_check_tablet(version, channel_id, connection, in_buffer, out_buffer);
            break;
          case OB_CS_MERGE_TABLETS: 
            rc = cs_merge_tablets(version, channel_id, connection, in_buffer, out_buffer);
            break;
          case OB_CS_SYNC_ALL_IMAGES: 
            rc = cs_sync_all_images(version, channel_id, connection, in_buffer, out_buffer);
            break;
          default:
            rc = OB_ERROR;
            break;
        }
      }
      return rc;
    }

    int ObChunkService::cs_batch_get(
        const int32_t version,
        const int32_t channel_id,
        tbnet::Connection* connection, 
        common::ObDataBuffer& in_buffer, 
        common::ObDataBuffer& out_buffer)
    {
      // TODO  not implement yet.
      UNUSED(version);
      UNUSED(channel_id);
      UNUSED(connection);
      UNUSED(in_buffer);
      UNUSED(out_buffer);
      return OB_SUCCESS;
    }

    int ObChunkService::get_query_service(ObQueryService *&service)
    {
      int ret = OB_SUCCESS;
      static __thread ObQueryService *thread_service = NULL; 
      service = NULL;

      if (NULL == thread_service)
      {
        ThreadSpecificBuffer::Buffer *service_buffer = query_service_buffer_.get_buffer();
        if (NULL == service_buffer)
        {
          TBSYS_LOG(ERROR, "fail to get thread specific buffer for merge join service");
          ret = OB_ALLOCATE_MEMORY_FAILED;
        }
        if (OB_SUCCESS == ret)
        {
          service_buffer->reset();
          if (NULL == service_buffer->current() 
              || service_buffer->remain() < static_cast<int32_t>(sizeof(ObQueryService)))
          {
            TBSYS_LOG(WARN, "logic error, thread buffer is null");
          }
          else
          {
            service = new(service_buffer->current()) ObQueryService(*chunk_server_);
            thread_service = service;
          }
        }
      }
      else
      {
        service = thread_service;
      }

      return ret;
    }

    int ObChunkService::cs_get(
        const int64_t start_time,
        const int32_t version,
        const int32_t channel_id,
        tbnet::Connection* connection, 
        common::ObDataBuffer& in_buffer, 
        common::ObDataBuffer& out_buffer,
        const int64_t timeout_time)
    {
      const int32_t CS_GET_VERSION = 1;
      uint64_t table_id = OB_INVALID_ID;
      common::ObResultCode rc;
      rc.result_code_ = OB_SUCCESS;
      ObQueryService* query_service = NULL;
      ObScanner* scanner = GET_TSI_MULT(ObScanner, TSI_CS_SCANNER_1);
      ObGetParam *get_param_ptr = GET_TSI_MULT(ObGetParam, TSI_CS_GET_PARAM_1);
      int64_t session_id = 0;
      int32_t response_cid = channel_id;
      int64_t packet_cnt = 0;
      int64_t ups_data_version = 0;
      bool is_fullfilled = true;
      int64_t fullfilled_num = 0;
      ObPacket* next_request = NULL;
      ObPacketQueueThread& queue_thread = 
        chunk_server_->get_default_task_queue_thread();

      FILL_TRACE_LOG("start cs_get");

      if (version != CS_GET_VERSION)
      {
        rc.result_code_ = OB_ERROR_FUNC_VERSION;
      }
      else if (NULL == scanner || NULL == get_param_ptr)
      {
        TBSYS_LOG(ERROR, "failed to get thread local get_param or scanner, "
            "scanner=%p, get_param_ptr=%p", scanner, get_param_ptr);
        rc.result_code_ = OB_ALLOCATE_MEMORY_FAILED;
      }
      else
      {
        rc.result_code_ = get_query_service(query_service);
      }

      if (OB_SUCCESS == rc.result_code_)
      {
        scanner->reset();
        rc.result_code_ = get_param_ptr->deserialize(
            in_buffer.get_data(), in_buffer.get_capacity(), 
            in_buffer.get_position());

        if (OB_SUCCESS != rc.result_code_)
        {
          TBSYS_LOG(ERROR, "parse cs_get input param error.");
        }
      }

      if (OB_SUCCESS == rc.result_code_)
      {
        if (get_param_ptr->get_cell_size() <= 0)
        {
          TBSYS_LOG(WARN, "invalid param, cell_size=%ld",get_param_ptr->get_cell_size());
          rc.result_code_ = OB_INVALID_ARGUMENT;
        }
        else if (NULL == get_param_ptr->get_row_index() || get_param_ptr->get_row_size() <= 0)
        {
          TBSYS_LOG(WARN, "invalid get param, row_index=%p, row_size=%ld",
              get_param_ptr->get_row_index(), get_param_ptr->get_row_size());
          rc.result_code_ = OB_INVALID_ARGUMENT;
        }
        else
        {
          // FIXME: the count is not very accurate,we just inc the get count of the first table
          table_id = (*get_param_ptr)[0]->table_id_;
          OB_CHUNK_STAT(inc,table_id, ObChunkServerStatManager::INDEX_GET_COUNT);
          OB_CHUNK_STAT(inc,ObChunkServerStatManager::META_TABLE_ID,ObChunkServerStatManager::INDEX_META_REQUEST_COUNT);
          rc.result_code_ = query_service->get(*get_param_ptr, *scanner, timeout_time);
        }
      }

      FILL_TRACE_LOG("finish get, ret=%d,", rc.result_code_);

      if (OB_SUCCESS == rc.result_code_)
      {
        scanner->get_is_req_fullfilled(is_fullfilled, fullfilled_num);
        if (!is_fullfilled)
        {
          session_id = queue_thread.generate_session_id();
        }
      }

      do
      {
        if (OB_SUCCESS == rc.result_code_ && !is_fullfilled)
        {
          rc.result_code_ = queue_thread.prepare_for_next_request(session_id);
        }

        // send response. return result code anyway.
        out_buffer.get_position() = 0;
        int serialize_ret = rc.serialize(out_buffer.get_data(), 
            out_buffer.get_capacity(), out_buffer.get_position());
        if (OB_SUCCESS != serialize_ret)
        {
          TBSYS_LOG(ERROR, "serialize result code object failed.");
          break;
        }
  
        // if get return success, we can return the scanner.
        if (OB_SUCCESS == rc.result_code_ && OB_SUCCESS == serialize_ret)
        {
          serialize_ret = scanner->serialize(out_buffer.get_data(), 
              out_buffer.get_capacity(), out_buffer.get_position());
          if (OB_SUCCESS != serialize_ret)
          {
            TBSYS_LOG(ERROR, "serialize ObScanner failed.");
            break;
          }
          ups_data_version = scanner->get_data_version();
        }

        if (OB_SUCCESS == serialize_ret)
        {
          OB_CHUNK_STAT(inc,table_id,ObChunkServerStatManager::INDEX_GET_BYTES,out_buffer.get_position());
          chunk_server_->send_response(
              OB_GET_RESPONSE, CS_GET_VERSION, 
              out_buffer, connection, response_cid, session_id);
          packet_cnt++;
        }

        if (OB_SUCCESS == rc.result_code_ && !is_fullfilled)
        {
          scanner->reset();
          rc.result_code_ = queue_thread.wait_for_next_request(
            session_id, next_request, timeout_time - tbsys::CTimeUtil::getTime());
          if (OB_SUCCESS != rc.result_code_)
          {
            TBSYS_LOG(WARN, "failed to wait for next reques timeout, ret=%d", 
              rc.result_code_);
            break;
          }
          else 
          {
            response_cid = next_request->getChannelId();
            rc.result_code_ = query_service->fill_get_data(*get_param_ptr, *scanner);
            scanner->get_is_req_fullfilled(is_fullfilled, fullfilled_num);
          }
        }
        else 
        {
          //error happen or fullfilled
          break;
        }
      } while (true);

      int64_t consume_time = tbsys::CTimeUtil::getTime() - start_time;
      OB_CHUNK_STAT(inc,table_id,ObChunkServerStatManager::INDEX_GET_TIME, consume_time);

      if (NULL != get_param_ptr 
          && consume_time >= chunk_server_->get_param().get_slow_query_warn_time())
      {
        TBSYS_LOG(WARN, "slow get: table_id:%lu, "
            "row size=%ld, peer=%s, rc=%d, scanner size=%ld,  consume=%ld", 
            table_id,  get_param_ptr->get_row_size(),
            tbsys::CNetUtil::addrToString(connection->getPeerId()).c_str(), 
            rc.result_code_, scanner->get_size(), consume_time);
      }

      FILL_TRACE_LOG("send get response, packet_cnt=%ld, session_id=%ld, ret=%d", 
        packet_cnt, session_id, rc.result_code_);
      PRINT_TRACE_LOG();
      CLEAR_TRACE_LOG();

      bool release_table = true;
      ObTabletManager & tablet_manager = chunk_server_->get_tablet_manager();      
      ObTabletManager::ObGetThreadContext*& get_context = tablet_manager.get_cur_thread_get_contex();
      if (get_context != NULL)
      {
        int64_t tablet_count = get_context->tablets_count_;
        for (int64_t i=0; i< tablet_count; ++i)
        {
          if ((get_context->tablets_[i] != NULL) &&
              (check_update_data(*get_context->tablets_[i],ups_data_version,release_table) != OB_SUCCESS))
          {
            TBSYS_LOG(WARN,"Problem of check frozen data of this tablet"); //just warn
          }
          else if (!release_table)
          {
            get_context->tablets_[i] = NULL; //do not release
          }
          else
          {
            //have new frozen table
          }
        }
      }

      if (session_id > 0)
      {
        queue_thread.destroy_session(session_id);
      }
      query_service->end_get();

      chunk_server_->get_tablet_manager().end_get();

      return rc.result_code_;
    }

    int ObChunkService::reset_internal_status(bool release_table /*=true*/)
    {
      int ret = OB_SUCCESS;

      /**
       * if this function fails, it means that some critical problems 
       * happen, don't kill chunk server here, just output some error
       * info, then we can restart this chunk server manualy. 
       */
      ret = chunk_server_->get_tablet_manager().end_scan(release_table);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "failed to end scan to release resources");
      }

      ret = reset_query_thread_local_buffer();
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "failed to reset query thread local buffer");
      }

      ret = wait_aio_buffer();
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "failed to wait aio buffer free");
      }

      return ret;
    }

    int ObChunkService::cs_scan(
        const int64_t start_time,
        const int32_t version,
        const int32_t channel_id,
        tbnet::Connection* connection, 
        common::ObDataBuffer& in_buffer, 
        common::ObDataBuffer& out_buffer,
        const int64_t timeout_time)
    {
      const int32_t CS_SCAN_VERSION = 1;
      common::ObResultCode rc;
      rc.result_code_ = OB_SUCCESS;
      uint64_t  table_id = OB_INVALID_ID; //for stat
      ObQueryService* query_service = NULL;
      ObScanner* scanner = GET_TSI_MULT(ObScanner, TSI_CS_SCANNER_1);
      common::ObScanParam *scan_param_ptr = GET_TSI_MULT(ObScanParam, TSI_CS_SCAN_PARAM_1); 
      int64_t session_id = 0;
      int32_t response_cid = channel_id;
      int64_t packet_cnt = 0;
      bool is_last_packet = false;
      bool is_fullfilled = true;
      int64_t fullfilled_num = 0;
      int64_t ups_data_version = 0;
      ObPacket* next_request = NULL;
      ObPacketQueueThread& queue_thread = 
        chunk_server_->get_default_task_queue_thread();
      char sql[1024] = "";
      int64_t pos = 0;

      FILL_TRACE_LOG("start cs_scan");

      if (version != CS_SCAN_VERSION)
      {
        rc.result_code_ = OB_ERROR_FUNC_VERSION;
      }
      else if (NULL == scanner || NULL == scan_param_ptr)
      {
        TBSYS_LOG(ERROR, "failed to get thread local scan_param or scanner, "
            "scanner=%p, scan_param_ptr=%p", scanner, scan_param_ptr);
        rc.result_code_ = OB_ALLOCATE_MEMORY_FAILED;
      }
      else
      {
        rc.result_code_ = get_query_service(query_service);
      }

      if (OB_SUCCESS == rc.result_code_)
      {
        scanner->reset();
        rc.result_code_ = scan_param_ptr->deserialize(
            in_buffer.get_data(), in_buffer.get_capacity(), 
            in_buffer.get_position());
        if (OB_SUCCESS != rc.result_code_)
        {
          TBSYS_LOG(ERROR, "parse cs_scan input scan param error.");
        }
        else
        {
          // force every client scan request use preread mode.
          scan_param_ptr->set_read_mode(ObScanParam::PREREAD);
        }
      }

      if (OB_SUCCESS == rc.result_code_)
      {
        table_id = scan_param_ptr->get_table_id();
        OB_CHUNK_STAT(inc,table_id,ObChunkServerStatManager::INDEX_SCAN_COUNT);
        OB_CHUNK_STAT(inc,ObChunkServerStatManager::META_TABLE_ID,ObChunkServerStatManager::INDEX_META_REQUEST_COUNT);
        rc.result_code_ = query_service->scan(*scan_param_ptr, *scanner, timeout_time);
        if (OB_ITER_END == rc.result_code_)
        {
          is_last_packet = true;
          rc.result_code_ = OB_SUCCESS;
        }
      }

      FILL_TRACE_LOG("finish scan, sql=[%s], version_range=%s, "
                     "scan_size=%ld, scan_direction=%d, ret=%d,", 
        (NULL != scan_param_ptr) ? (scan_param_ptr->to_str(sql, sizeof(sql), pos),sql) : sql,
        (NULL != scan_param_ptr) ? range2str(scan_param_ptr->get_version_range()) : "", 
        (NULL != scan_param_ptr) ? (GET_SCAN_SIZE(scan_param_ptr->get_scan_size())) : 0, 
        (NULL != scan_param_ptr) ? scan_param_ptr->get_scan_direction() : 0,
        rc.result_code_);

      if (OB_SUCCESS == rc.result_code_)
      {
        scanner->get_is_req_fullfilled(is_fullfilled, fullfilled_num);
        if (!is_fullfilled && !is_last_packet)
        {
          session_id = queue_thread.generate_session_id();
        }
      }

      do
      {
        if (OB_SUCCESS == rc.result_code_ && !is_fullfilled && !is_last_packet)
        {
          rc.result_code_ = queue_thread.prepare_for_next_request(session_id);
        }
        // send response.
        out_buffer.get_position() = 0;
        int serialize_ret = rc.serialize(out_buffer.get_data(), 
            out_buffer.get_capacity(), out_buffer.get_position());
        if (OB_SUCCESS != serialize_ret)
        {
          TBSYS_LOG(ERROR, "serialize result code object failed.");
          break ;
        }
  
        // if scan return success , we can return scanner.
        if (OB_SUCCESS == rc.result_code_ && OB_SUCCESS == serialize_ret)
        {
          serialize_ret = scanner->serialize(out_buffer.get_data(), 
              out_buffer.get_capacity(), out_buffer.get_position());
          ups_data_version = scanner->get_data_version();
          if (OB_SUCCESS != serialize_ret)
          {
            TBSYS_LOG(ERROR, "serialize ObScanner failed.");
            break;
          }
        }
 
        if (OB_SUCCESS == serialize_ret)
        {
          OB_CHUNK_STAT(inc,table_id,
              ObChunkServerStatManager::INDEX_SCAN_BYTES,out_buffer.get_position());
          chunk_server_->send_response(
              is_last_packet ? OB_SESSION_END : OB_SCAN_RESPONSE, CS_SCAN_VERSION, 
              out_buffer, connection, response_cid, session_id);
          packet_cnt++;
        }

        if (OB_SUCCESS == rc.result_code_ && !is_fullfilled && !is_last_packet)
        {
          scanner->reset();
          rc.result_code_ = queue_thread.wait_for_next_request(
            session_id, next_request, timeout_time - tbsys::CTimeUtil::getTime());
          if (OB_NET_SESSION_END == rc.result_code_)
          {
            //merge server end this session
            rc.result_code_ = OB_SUCCESS;
            break;
          }
          else if (OB_SUCCESS != rc.result_code_)
          {
            TBSYS_LOG(WARN, "failed to wait for next reques timeout, ret=%d", 
              rc.result_code_);
            break;
          }
          else 
          {
            response_cid = next_request->getChannelId();
            rc.result_code_ = query_service->fill_scan_data(*scanner);
            if (OB_ITER_END == rc.result_code_)
            {
              /**
               * the last packet is not always with true fullfilled flag, 
               * maybe there is not enough memory to query the normal scan 
               * request, we just return part of result, user scan the next 
               * data if necessary. it's order to be compatible with 0.2 
               * version. 
               */
              is_last_packet = true;
              rc.result_code_ = OB_SUCCESS;
            }
            scanner->get_is_req_fullfilled(is_fullfilled, fullfilled_num);
          }
        }
        else 
        {
          //error happen or fullfilled or sent last packet
          break;
        }
      } while (true);

      int64_t consume_time = tbsys::CTimeUtil::getTime() - start_time;
      OB_CHUNK_STAT(inc,table_id,ObChunkServerStatManager::INDEX_SCAN_TIME, consume_time);

      //if ups have frozen mem table,get it
      bool release_tablet = true;

      if (OB_SUCCESS == rc.result_code_)
      {
        ObTabletManager& tablet_manager = chunk_server_->get_tablet_manager();        
        ObTablet*& tablet = tablet_manager.get_cur_thread_scan_tablet();
        if (tablet != NULL && check_update_data(*tablet,ups_data_version,release_tablet) != OB_SUCCESS)
        {
          TBSYS_LOG(WARN,"check update data failed"); //just warn
        }
      }

      if (NULL != scan_param_ptr 
          && consume_time >= chunk_server_->get_param().get_slow_query_warn_time())
      {
        pos = 0;
        scan_param_ptr->to_str(sql, sizeof(sql), pos);
        TBSYS_LOG(WARN,
            "slow scan:[%s], version_range=%s, scan size=%ld, scan_direction=%d, "
            "read mode=%d, peer=%s, rc=%d, scanner size=%ld, row_num=%ld, "
            "cell_num=%ld, io_stat: %s, consume=%ld",
            sql, range2str(scan_param_ptr->get_version_range()),  
            GET_SCAN_SIZE(scan_param_ptr->get_scan_size()), 
            scan_param_ptr->get_scan_direction(),
            scan_param_ptr->get_read_mode(),
            tbsys::CNetUtil::addrToString(connection->getPeerId()).c_str(),
            rc.result_code_, scanner->get_size(), 
            scanner->get_row_num(), scanner->get_cell_num(), 
            get_io_stat_str(), consume_time);
      }

      FILL_TRACE_LOG("send response, packet_cnt=%ld, session_id=%ld, read mode=%d, "
                     "io stat: %s, ret=%d", 
        packet_cnt, session_id, (NULL != scan_param_ptr) ? scan_param_ptr->get_read_mode() : 1,
        get_io_stat_str(), rc.result_code_);
      PRINT_TRACE_LOG();
      CLEAR_TRACE_LOG();

      //reset initernal status for next scan operator
      if (session_id > 0)
      {
        queue_thread.destroy_session(session_id);
      }
      query_service->end_scan();
      reset_internal_status(release_tablet);

      return rc.result_code_;
    }

    int ObChunkService::cs_drop_old_tablets(
        const int32_t version,
        const int32_t channel_id,
        tbnet::Connection* connection, 
        common::ObDataBuffer& in_buffer, 
        common::ObDataBuffer& out_buffer)
    {
      const int32_t CS_DROP_OLD_TABLES_VERSION = 1;
      common::ObResultCode rc;
      rc.result_code_ = OB_SUCCESS;
      //char msg_buff[24]; 
      //rc.message_.assign(msg_buff, 24);
      if (version != CS_DROP_OLD_TABLES_VERSION)
      {
        rc.result_code_ = OB_ERROR_FUNC_VERSION;
      }

      int64_t memtable_frozen_version = 0;

      if (OB_SUCCESS == rc.result_code_)
      {
        rc.result_code_ = common::serialization::decode_vi64(
            in_buffer.get_data(), in_buffer.get_capacity(), 
            in_buffer.get_position(), &memtable_frozen_version);
        if (OB_SUCCESS != rc.result_code_)
        {
          TBSYS_LOG(ERROR, "parse drop_old_tablets input memtable_frozen_version param error.");
        }
      }

      TBSYS_LOG(INFO, "drop_old_tablets: memtable_frozen_version:%ld", memtable_frozen_version);

      int serialize_ret = rc.serialize(out_buffer.get_data(), 
          out_buffer.get_capacity(), out_buffer.get_position());
      if (serialize_ret != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR, "rc.serialize error");
      }

      if (OB_SUCCESS == serialize_ret)
      {
        chunk_server_->send_response(
            OB_DROP_OLD_TABLETS_RESPONSE, 
            CS_DROP_OLD_TABLES_VERSION, 
            out_buffer, connection, channel_id);
      }


      // call tablet_manager_ drop tablets.
      //ObTabletManager & tablet_manager = chunk_server_->get_tablet_manager();
      //rc.result_code_ = tablet_manager.drop_tablets(memtable_frozen_version);

      return rc.result_code_;
    }

    /*
     * int cs_heart_beat(const int64_t lease_duration);
     */
    int ObChunkService::cs_heart_beat(
        const int32_t version,
        const int32_t channel_id,
        tbnet::Connection* connection, 
        common::ObDataBuffer& in_buffer, 
        common::ObDataBuffer& out_buffer)
    {
      const int32_t CS_HEART_BEAT_VERSION = 2;
      common::ObResultCode rc;
      rc.result_code_ = OB_SUCCESS;
      //char msg_buff[24]; 
      //rc.message_.assign(msg_buff, 24);
      UNUSED(channel_id);
      UNUSED(connection);
      UNUSED(out_buffer);
      //if (version != CS_HEART_BEAT_VERSION)
      //{
      //  rc.result_code_ = OB_ERROR_FUNC_VERSION;
      //}

      // send heartbeat request to root_server first
      if (OB_SUCCESS == rc.result_code_)
      {
        ObRootServerRpcStub & rs_rpc_stub = chunk_server_->get_rs_rpc_stub();
        rc.result_code_ = rs_rpc_stub.async_heartbeat(chunk_server_->get_self());
        if (OB_SUCCESS != rc.result_code_)
        {
          TBSYS_LOG(WARN, "failed to async_heartbeat, ret=%d", rc.result_code_);
        }
      }

      // ignore the returned code of async_heartbeat
      int64_t lease_duration = 0;
      rc.result_code_ = common::serialization::decode_vi64(
          in_buffer.get_data(), in_buffer.get_capacity(), 
          in_buffer.get_position(), &lease_duration);
      if (OB_SUCCESS != rc.result_code_)
      {
        TBSYS_LOG(ERROR, "parse cs_heart_beat input lease_duration param error.");
      }
      else
      {
        service_expired_time_ = tbsys::CTimeUtil::getTime() + lease_duration; 
        TBSYS_LOG(DEBUG, "cs_heart_beat: lease_duration=%ld", lease_duration);
      }

      TBSYS_LOG(DEBUG,"cs_heart_beat,version:%d,CS_HEART_BEAT_VERSION:%d",version,CS_HEART_BEAT_VERSION);

      if (version >= CS_HEART_BEAT_VERSION)
      {
        int64_t frozen_version = 0;
        if (OB_SUCCESS == rc.result_code_)
        {
          rc.result_code_ = common::serialization::decode_vi64(
              in_buffer.get_data(),in_buffer.get_capacity(),
              in_buffer.get_position(),&frozen_version);
          if (OB_SUCCESS != rc.result_code_)
          {
            TBSYS_LOG(ERROR, "parse cs_heart_beat input frozen_version param error.");
          }
          else
          {
            TBSYS_LOG(DEBUG, "cs_heart_beat: frozen_version=%ld", frozen_version);
          }
        }

        if (OB_SUCCESS == rc.result_code_ && service_started_)
        {
          int64_t wait_time = 0;
          ObTabletManager &tablet_manager = chunk_server_->get_tablet_manager();
          if ( frozen_version > chunk_server_->get_tablet_manager().get_serving_data_version() )
          {
            if (frozen_version > merge_task_.get_last_frozen_version())
            {
              TBSYS_LOG(INFO,"pending a new frozen version need merge:%ld,last:%ld",
                  frozen_version, merge_task_.get_last_frozen_version() );
              merge_task_.set_frozen_version(frozen_version);
            }
            if (!merge_task_.is_scheduled() 
                && tablet_manager.get_chunk_merge().can_launch_next_round(frozen_version))
            {
              srand(static_cast<int32_t>(tbsys::CTimeUtil::getTime()));
              if (merge_delay_interval_ > 0) 
              {
                wait_time = random() % merge_delay_interval_;
              }
              // wait one more minute for ensure slave updateservers sync frozen version.
              wait_time += chunk_server_->get_param().get_merge_delay_for_lsync();
              TBSYS_LOG(INFO, "launch a new merge process after wait %ld us.", wait_time);
              timer_.schedule(merge_task_, wait_time, false);  //async
              merge_task_.set_scheduled();
            }
          }
        }
      }

      if (version > CS_HEART_BEAT_VERSION)
      {
        int64_t local_version = 0;
        int64_t schema_version = 0;
        if (OB_SUCCESS == rc.result_code_)
        {
          rc.result_code_ = serialization::decode_vi64(in_buffer.get_data(),
                                                       in_buffer.get_capacity(),
                                                       in_buffer.get_position(), 
                                                       &schema_version);
          if (OB_SUCCESS != rc.result_code_)
          {
            TBSYS_LOG(ERROR, "parse heartbeat schema version failed:ret[%d]", 
                      rc.result_code_);
          }
        }
  
        // fetch new schema in a temp timer task
        if ((OB_SUCCESS == rc.result_code_) && service_started_)
        {
          local_version = chunk_server_->get_schema_manager()->get_latest_version();
          if (local_version > schema_version)
          {
            rc.result_code_ = OB_ERROR;
            TBSYS_LOG(ERROR, "check schema local version gt than new version:"
                "local[%ld], new[%ld]", local_version, schema_version);
          }
          else if (!fetch_schema_task_.is_scheduled() 
                   && local_version < schema_version)
          {
            fetch_schema_task_.init(chunk_server_->get_rpc_proxy(), 
                                    chunk_server_->get_schema_manager());
            fetch_schema_task_.set_version(local_version, schema_version);
            srand(static_cast<int32_t>(tbsys::CTimeUtil::getTime()));
            timer_.schedule(fetch_schema_task_, 
                            random() % FETCH_SCHEMA_INTERVAL, false);
            fetch_schema_task_.set_scheduled();
          }
        }
      }

      /*
      int serialize_ret = rc.serialize(out_buffer.get_data(), 
          out_buffer.get_capacity(), out_buffer.get_position());
      if (serialize_ret != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR, "rc.serialize error");
      }

      if (OB_SUCCESS == serialize_ret)
      {
        chunk_server_->send_response(
            OB_HEARTBEAT_RESPONSE, 
            CS_HEART_BEAT_VERSION, 
            out_buffer, connection, channel_id);
      }
      */
      return rc.result_code_;
    }

    int ObChunkService::cs_create_tablet(
        const int32_t version,
        const int32_t channel_id,
        tbnet::Connection* connection, 
        common::ObDataBuffer& in_buffer, 
        common::ObDataBuffer& out_buffer)
    {
      const int32_t CS_CREATE_TABLE_VERSION = 1;
      common::ObResultCode rc;
      rc.result_code_ = OB_SUCCESS;
      if (version != CS_CREATE_TABLE_VERSION)
      {
        rc.result_code_ = OB_ERROR_FUNC_VERSION;
      }


      ObRange range;
      if (OB_SUCCESS == rc.result_code_)
      {
        rc.result_code_ = range.deserialize(
            in_buffer.get_data(), in_buffer.get_capacity(), 
            in_buffer.get_position());
        if (OB_SUCCESS != rc.result_code_)
        {
          TBSYS_LOG(ERROR, "parse cs_create_tablet input range param error.");
        }
      }

      char range_buf[OB_RANGE_STR_BUFSIZ];
      range.to_string(range_buf, sizeof(range_buf));
      TBSYS_LOG(INFO, "cs_create_tablet, dump input range below:%s", range_buf);

      // get last frozen memtable version for update
      int64_t last_frozen_memtable_version = 0;
      if (OB_SUCCESS == rc.result_code_)
      {
        rc.result_code_ = serialization::decode_vi64(
            in_buffer.get_data(), in_buffer.get_capacity(),
            in_buffer.get_position(), &last_frozen_memtable_version);
        /*
        ObServer update_server;
        rc.result_code_ = chunk_server_->get_rs_rpc_stub().get_update_server(update_server);
        ObRootServerRpcStub update_stub;
        if (OB_SUCCESS == rc.result_code_)
        {
          rc.result_code_ = update_stub.init(update_server, &chunk_server_->get_client_manager());
        }
        if (OB_SUCCESS == rc.result_code_)
        {
          rc.result_code_ = update_stub.get_last_frozen_memtable_version(last_frozen_memtable_version);
        }
        */
      }

      if (OB_SUCCESS == rc.result_code_)
      {
        TBSYS_LOG(DEBUG, "create tablet, last_frozen_memtable_version=%ld", 
            last_frozen_memtable_version);
        rc.result_code_ = chunk_server_->get_tablet_manager().create_tablet(
            range, last_frozen_memtable_version);
      }

      int serialize_ret = rc.serialize(out_buffer.get_data(), 
          out_buffer.get_capacity(), out_buffer.get_position());
      if (serialize_ret != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR, "cs_create_tablet rc.serialize error");
      }

      if (OB_SUCCESS == serialize_ret)
      {
        chunk_server_->send_response(
            OB_CS_CREATE_TABLE_RESPONSE,
            CS_CREATE_TABLE_VERSION, 
            out_buffer, connection, channel_id);
      }


      // call tablet_manager_ drop tablets.

      return rc.result_code_;
    }


    int ObChunkService::cs_load_tablet(
        const int32_t version,
        const int32_t channel_id,
        tbnet::Connection* connection, 
        common::ObDataBuffer& in_buffer, 
        common::ObDataBuffer& out_buffer)
    {
      const int32_t CS_LOAD_TABLET_VERSION = 1;
      common::ObResultCode rc;
      rc.result_code_ = OB_SUCCESS;
      if (version != CS_LOAD_TABLET_VERSION)
      {
        rc.result_code_ = OB_ERROR_FUNC_VERSION;
      }
      else if (chunk_server_->get_param().get_merge_migrate_concurrency() == 0
          && (!chunk_server_->get_tablet_manager().get_chunk_merge().is_merge_stoped()))
      {
        TBSYS_LOG(WARN, "merge running, cannot migrate in.");
        rc.result_code_ = OB_CS_EAGAIN;
      }

      ObRange range;
      int64_t num_file = 0;
      //deserialize ObRange
      if (OB_SUCCESS == rc.result_code_)
      {
        rc.result_code_ = range.deserialize(in_buffer.get_data(), in_buffer.get_capacity(), 
            in_buffer.get_position());
        if (OB_SUCCESS != rc.result_code_)
        {
          TBSYS_LOG(ERROR, "parse cs_load_tablet range param error.");
        }
        else
        {
          char range_buf[OB_RANGE_STR_BUFSIZ];
          range.to_string(range_buf, OB_RANGE_STR_BUFSIZ);
          TBSYS_LOG(DEBUG,"cs_load_tablet dump range <%s>", range_buf);
        }
      }

      int32_t dest_disk_no = 0;
      if (OB_SUCCESS == rc.result_code_)
      {
        rc.result_code_ = serialization::decode_vi32(in_buffer.get_data(), in_buffer.get_capacity(), 
            in_buffer.get_position(), &dest_disk_no);
        if (OB_SUCCESS != rc.result_code_)
        {
          TBSYS_LOG(ERROR, "parse dest_disk_no range param error.");
        }
        else
        {
          TBSYS_LOG(INFO, "cs_load_tablet dest_disk_no=%d ", dest_disk_no);
        }
      }

      // deserialize tablet_version;
      int64_t tablet_version = 0;
      if (OB_SUCCESS == rc.result_code_)
      {
        rc.result_code_ = serialization::decode_vi64(in_buffer.get_data(), 
            in_buffer.get_capacity(), in_buffer.get_position(), &tablet_version);
        if (OB_SUCCESS != rc.result_code_)
        {
          TBSYS_LOG(ERROR, "parse cs_load_tablet tablet_version param error.");
        }
        else
        {
          TBSYS_LOG(INFO,"cs_load_tablet tablet_version = %ld ", tablet_version);
        }
      }

      uint64_t crc_sum = 0;
      if (OB_SUCCESS == rc.result_code_)
      {
        rc.result_code_ = serialization::decode_vi64(in_buffer.get_data(), 
            in_buffer.get_capacity(), in_buffer.get_position(), (int64_t*)(&crc_sum));
        if (OB_SUCCESS != rc.result_code_)
        {
          TBSYS_LOG(ERROR, "parse cs_load_tablet crc_sum param error.");
        }
        else
        {
          TBSYS_LOG(INFO,"cs_load_tablet crc_sum = %lu ", crc_sum);
        }
      }

      if (OB_SUCCESS == rc.result_code_)
      {
        rc.result_code_ = common::serialization::decode_vi64(in_buffer.get_data(), 
            in_buffer.get_capacity(), in_buffer.get_position(),&num_file);
        if (OB_SUCCESS != rc.result_code_)
        {
          TBSYS_LOG(ERROR, "parse cs_load_tablet number of sstable  param error.");
        }
        else
        {
          TBSYS_LOG(INFO,"cs_load_tablet num_file = %ld ", num_file);
        }
      }

      ObTabletManager & tablet_manager = chunk_server_->get_tablet_manager();
      if (OB_SUCCESS == rc.result_code_ && num_file > 0)
      {
        char (*path)[OB_MAX_FILE_NAME_LENGTH];
        char * path_buf = static_cast<char*>(ob_malloc(num_file*OB_MAX_FILE_NAME_LENGTH));
        if ( NULL == path_buf )
        {
          TBSYS_LOG(ERROR, "failed to allocate memory for path array.");
          rc.result_code_ = OB_ALLOCATE_MEMORY_FAILED;
        }
        else
        {
          path = new(path_buf)char[num_file][OB_MAX_FILE_NAME_LENGTH];
        }

        int64_t len = 0;
        if (OB_SUCCESS == rc.result_code_)
        {
          for( int64_t idx =0; idx < num_file; idx++)
          {
            if(NULL == common::serialization::decode_vstr(in_buffer.get_data(), 
                  in_buffer.get_capacity(), in_buffer.get_position(),
                  path[idx], OB_MAX_FILE_NAME_LENGTH, &len))
            {
              rc.result_code_ = OB_ERROR;
              TBSYS_LOG(ERROR, "parse cs_load_tablet dest_path param error.");
              break;
            }
            else
            {
              TBSYS_LOG(INFO, "parse cs_load_tablet dest_path [%ld] = %s", idx, path[idx]);
            }
          }
        }

        if (OB_SUCCESS == rc.result_code_)
        {       
          rc.result_code_ = tablet_manager.dest_load_tablet(range, path, num_file, tablet_version, dest_disk_no, crc_sum);
          if (OB_SUCCESS != rc.result_code_ && OB_CS_MIGRATE_IN_EXIST != rc.result_code_)
          {
            TBSYS_LOG(WARN, "ObTabletManager::dest_load_tablet error, rc=%d", rc.result_code_);
          }
        }

        if ( NULL != path_buf )
        {
          ob_free(path_buf);
        }      
      }
      else if (OB_SUCCESS == rc.result_code_ && num_file == 0)
      {
        rc.result_code_ = tablet_manager.dest_load_tablet(range, NULL, 0, tablet_version, dest_disk_no, crc_sum);
        if (OB_SUCCESS != rc.result_code_)
        {
          TBSYS_LOG(WARN, "ObTabletManager::dest_load_tablet error, rc=%d", rc.result_code_);
        }
      }

      //send response to src chunkserver
      int serialize_ret = rc.serialize(out_buffer.get_data(), 
          out_buffer.get_capacity(), out_buffer.get_position());
      if (serialize_ret != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR, "rc.serialize error");
      }

      if (OB_SUCCESS == serialize_ret)
      {
        chunk_server_->send_response(
            OB_CS_MIGRATE_RESPONSE, 
            CS_LOAD_TABLET_VERSION, 
            out_buffer, connection, channel_id);
      }
      return rc.result_code_;
    }

    int ObChunkService::cs_delete_tablets(
        const int32_t version,
        const int32_t channel_id,
        tbnet::Connection* connection, 
        common::ObDataBuffer& in_buffer, 
        common::ObDataBuffer& out_buffer)
    {
      const int32_t CS_DELETE_TABLETS_VERSION = 1;
      common::ObResultCode rc;
      rc.result_code_ = OB_SUCCESS;
      ObTabletReportInfoList *delete_tablet_list = NULL;
      bool is_force = false;

      if (version != CS_DELETE_TABLETS_VERSION
          && version != CS_DELETE_TABLETS_VERSION + 1)
      {
        rc.result_code_ = OB_ERROR_FUNC_VERSION;
      }
      else if (!chunk_server_->get_tablet_manager().get_chunk_merge().is_merge_stoped())
      {
        TBSYS_LOG(WARN, "merge running, cannot remove tablets.");
        rc.result_code_ = OB_CS_EAGAIN;
      }
      else if (NULL == (delete_tablet_list = GET_TSI_MULT(ObTabletReportInfoList, TSI_CS_TABLET_REPORT_INFO_LIST_1))) 
      {
        TBSYS_LOG(ERROR, "cannot get ObTabletReportInfoList object.");
        rc.result_code_ = OB_ALLOCATE_MEMORY_FAILED;
      }
      else
      {
        delete_tablet_list->reset();
      }

      if (OB_SUCCESS == rc.result_code_)
      {
        rc.result_code_ = delete_tablet_list->deserialize(in_buffer.get_data(), 
            in_buffer.get_capacity(), in_buffer.get_position());
        if (OB_SUCCESS != rc.result_code_)
        {
          TBSYS_LOG(ERROR, "parse cs_delete_tablets tablet info list param error.");
        }
      }

      if (version > CS_DELETE_TABLETS_VERSION)
      {
        if (OB_SUCCESS == rc.result_code_)
        {
          rc.result_code_ = common::serialization::decode_bool(
              in_buffer.get_data(),in_buffer.get_capacity(),
              in_buffer.get_position(),&is_force);
          if (OB_SUCCESS != rc.result_code_)
          {
            TBSYS_LOG(ERROR, "parse cs_delete_tablets input is_force param error.");
          }
          else
          {
            TBSYS_LOG(DEBUG, "cs_heart_beat: is_force=%d", is_force);
          }
        }
      }

      if (OB_SUCCESS == rc.result_code_)
      {
        ObTabletManager & tablet_manager = chunk_server_->get_tablet_manager();
        ObMultiVersionTabletImage & tablet_image = tablet_manager.get_serving_tablet_image();

        char range_buf[OB_RANGE_STR_BUFSIZ];
        int64_t size = delete_tablet_list->get_tablet_size();
        int64_t version = 0;
        const ObTabletReportInfo* const tablet_info_array = delete_tablet_list->get_tablet();
        ObTablet *src_tablet = NULL;
        int32_t disk_no = -1;

        for (int64_t i = 0; i < size ;  ++i)
        {
          version = tablet_info_array[i].tablet_location_.tablet_version_;
          tablet_info_array[i].tablet_info_.range_.to_string(range_buf, OB_RANGE_STR_BUFSIZ);
          rc.result_code_ = tablet_image.acquire_tablet(
              tablet_info_array[i].tablet_info_.range_,
              ObMultiVersionTabletImage::SCAN_FORWARD, version, src_tablet);

          if (OB_SUCCESS == rc.result_code_ && NULL != src_tablet 
              && src_tablet->get_data_version() == version)
          {
            TBSYS_LOG(INFO, "delete tablet , version=%ld,disk=%d, range=<%s>",
                src_tablet->get_data_version(), src_tablet->get_disk_no(), range_buf);
            src_tablet->set_merged(); 
            src_tablet->set_removed();
            if (is_force)
            {
              tablet_image.release_tablet(src_tablet);
              src_tablet = NULL;
              rc.result_code_ = tablet_image.remove_tablet(
                tablet_info_array[i].tablet_info_.range_, version, disk_no);
              if (OB_SUCCESS == rc.result_code_)
              {
                tablet_image.write(version, disk_no);
              }
            }
            else
            {
              tablet_image.write(src_tablet->get_data_version(), src_tablet->get_disk_no());
            }
          }
          else
          {
            TBSYS_LOG(INFO, "cannot find tablet , version=%ld,range=<%s>",
                version,  range_buf);
          }

          if (NULL != src_tablet)
          {
            tablet_image.release_tablet(src_tablet);
          }
        }

      }

      //send response to src chunkserver
      int serialize_ret = rc.serialize(out_buffer.get_data(), 
          out_buffer.get_capacity(), out_buffer.get_position());
      if (serialize_ret != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR, "rc.serialize error");
      }

      if (OB_SUCCESS == serialize_ret)
      {
        chunk_server_->send_response(
            OB_CS_DELETE_TABLETS_RESPONSE, 
            CS_DELETE_TABLETS_VERSION, 
            out_buffer, connection, channel_id);
      }
      return rc.result_code_;
    }

    int ObChunkService::cs_merge_tablets(
        const int32_t version,
        const int32_t channel_id,
        tbnet::Connection* connection, 
        common::ObDataBuffer& in_buffer, 
        common::ObDataBuffer& out_buffer)
    {
      const int32_t CS_MERGE_TABLETS_VERSION = 1;
      common::ObResultCode rc;
      rc.result_code_ = OB_SUCCESS;
      ObTabletReportInfoList *merge_tablet_list = NULL;

      if (version != CS_MERGE_TABLETS_VERSION)
      {
        rc.result_code_ = OB_ERROR_FUNC_VERSION;
      }
      else if (!chunk_server_->get_tablet_manager().get_chunk_merge().is_merge_stoped())
      {
        TBSYS_LOG(WARN, "merge running, cannot merge tablets.");
        rc.result_code_ = OB_CS_EAGAIN;
      }
      else if (NULL == (merge_tablet_list = GET_TSI_MULT(ObTabletReportInfoList, TSI_CS_TABLET_REPORT_INFO_LIST_1))) 
      {
        TBSYS_LOG(ERROR, "cannot get ObTabletReportInfoList object.");
        rc.result_code_ = OB_ALLOCATE_MEMORY_FAILED;
      }
      else
      {
        merge_tablet_list->reset();
      }

      if (OB_SUCCESS == rc.result_code_)
      {
        rc.result_code_ = merge_tablet_list->deserialize(in_buffer.get_data(), 
            in_buffer.get_capacity(), in_buffer.get_position());
        if (OB_SUCCESS != rc.result_code_)
        {
          TBSYS_LOG(ERROR, "parse cs_merge_tablets tablet info list param error.");
        }
      }

      //response to root server first
      int serialize_ret = rc.serialize(out_buffer.get_data(), 
          out_buffer.get_capacity(), out_buffer.get_position());
      if (serialize_ret != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR, "merge_tablets rc.serialize error");
      }

      if (OB_SUCCESS == serialize_ret)
      {
        chunk_server_->send_response(
            OB_CS_MERGE_TABLETS_RESPONSE, 
            CS_MERGE_TABLETS_VERSION, 
            out_buffer, connection, channel_id);
      }

      if (OB_SUCCESS == rc.result_code_)
      {
        rc.result_code_ = chunk_server_->get_tablet_manager().merge_multi_tablets(*merge_tablet_list);
      }

      ObRootServerRpcStub & rs_rpc_stub = chunk_server_->get_rs_rpc_stub();
      bool is_merge_succ = (OB_SUCCESS == rc.result_code_);
      //report merge tablets info to root server
      rc.result_code_= rs_rpc_stub.merge_tablets_over(*merge_tablet_list, 
          is_merge_succ);
      if (OB_SUCCESS != rc.result_code_)
      {
        TBSYS_LOG(WARN, "report merge tablets over error, is_merge_succ=%d", 
            is_merge_succ);
      }
      else if (is_merge_succ)
      {
        char range_buf[OB_RANGE_STR_BUFSIZ];
        merge_tablet_list->get_tablet()[0].tablet_info_.range_.to_string(
          range_buf, sizeof(range_buf));
        TBSYS_LOG(INFO, "merge tablets <%s> over and success, seq_num=%ld",
            range_buf, merge_tablet_list->get_tablet()[0].tablet_location_.tablet_seq_);
      }

      return rc.result_code_;
    }


    int ObChunkService::cs_migrate_tablet(
        const int32_t version,
        const int32_t channel_id,
        tbnet::Connection* connection, 
        common::ObDataBuffer& in_buffer, 
        common::ObDataBuffer& out_buffer)
    {
      const int32_t CS_MIGRATE_TABLET_VERSION = 1;
      common::ObResultCode rc;
      rc.result_code_ = OB_SUCCESS;
      int32_t is_inc_migrate_task_count = 0; 

      if (version != CS_MIGRATE_TABLET_VERSION)
      {
        rc.result_code_ = OB_ERROR_FUNC_VERSION;
      }


      ObRange range;
      ObServer dest_server;
      bool keep_src = false;

      //deserialize ObRange
      if (OB_SUCCESS == rc.result_code_)
      {
        rc.result_code_ = range.deserialize(in_buffer.get_data(), in_buffer.get_capacity(), 
            in_buffer.get_position());
        if (OB_SUCCESS != rc.result_code_)
        {
          TBSYS_LOG(ERROR, "parse cs_migrate_tablet range param error.");
        }
      }

      //deserialize destination chunkserver
      if (OB_SUCCESS == rc.result_code_)
      {
        rc.result_code_ = dest_server.deserialize(in_buffer.get_data(), in_buffer.get_capacity(), 
            in_buffer.get_position());
        if (OB_SUCCESS != rc.result_code_)
        {
          TBSYS_LOG(ERROR, "parse cs_migrate_tablet dest_server param error.");
        }
      }

      //deserialize migrate type(copy or move)
      if (OB_SUCCESS == rc.result_code_)
      {
        rc.result_code_ = common::serialization::decode_bool(in_buffer.get_data(), in_buffer.get_capacity(), 
            in_buffer.get_position(),&keep_src);
        if (OB_SUCCESS != rc.result_code_)
        {
          TBSYS_LOG(ERROR, "parse cs_migrate_tablet keep_src param error.");
        }
      }

      char range_buf[OB_RANGE_STR_BUFSIZ];
      if (OB_SUCCESS == rc.result_code_)
      {
        char ip_addr_string[OB_IP_STR_BUFF];
        range.to_string(range_buf, OB_RANGE_STR_BUFSIZ);
        dest_server.to_string(ip_addr_string, OB_IP_STR_BUFF);
        TBSYS_LOG(INFO, "begin migrate_tablet %s, dest_server=%s, keep_src=%d", 
            range_buf, ip_addr_string, keep_src);
      }

      if (chunk_server_->get_param().get_merge_migrate_concurrency() == 0
          && (!chunk_server_->get_tablet_manager().get_chunk_merge().is_merge_stoped()))
      {
        TBSYS_LOG(WARN, "merge running, cannot migrate.");
        rc.result_code_ = OB_CS_EAGAIN;
      }

      int64_t max_migrate_task_count = chunk_server_->get_param().get_max_migrate_task_count();
      if (OB_SUCCESS == rc.result_code_ )
      {
        uint32_t old_migrate_task_count = migrate_task_count_;
        while(old_migrate_task_count < max_migrate_task_count)
        {
          uint32_t tmp = atomic_compare_exchange(&migrate_task_count_, old_migrate_task_count+1, old_migrate_task_count);
          if (tmp == old_migrate_task_count)
          {
            is_inc_migrate_task_count = 1;
            break;
          }
          old_migrate_task_count = migrate_task_count_;
        }
        if (0 == is_inc_migrate_task_count)
        {
          TBSYS_LOG(WARN, "current migrate task count = %u, exceeded max = %ld",
              old_migrate_task_count, max_migrate_task_count);
          rc.result_code_ = OB_CS_EAGAIN;
        }
      }


      //response to root server first
      int serialize_ret = rc.serialize(out_buffer.get_data(), 
          out_buffer.get_capacity(), out_buffer.get_position());
      if (serialize_ret != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR, "migrate_tablet rc.serialize error");
      }

      if (OB_SUCCESS == serialize_ret)
      {
        chunk_server_->send_response(
            OB_MIGRATE_OVER_RESPONSE, 
            CS_MIGRATE_TABLET_VERSION, 
            out_buffer, connection, channel_id);
      }

      ObRootServerRpcStub cs_rpc_stub; 

      if (OB_SUCCESS == rc.result_code_)
      {
        rc.result_code_ = cs_rpc_stub.init(dest_server, &(chunk_server_->get_client_manager()));
        if (OB_SUCCESS != rc.result_code_)
        {
          TBSYS_LOG(ERROR, "migrate_tablet init cs_rpc_stub error.");
        }
      }

      ObTabletManager & tablet_manager = chunk_server_->get_tablet_manager();
      char (*dest_path)[OB_MAX_FILE_NAME_LENGTH] = NULL;
      char (*src_path)[OB_MAX_FILE_NAME_LENGTH] = NULL;
      char *dest_path_buf = static_cast<char*>(ob_malloc(ObTablet::MAX_SSTABLE_PER_TABLET*OB_MAX_FILE_NAME_LENGTH));
      char *src_path_buf  = static_cast<char*>(ob_malloc(ObTablet::MAX_SSTABLE_PER_TABLET*OB_MAX_FILE_NAME_LENGTH));
      if ( NULL == src_path_buf || NULL == dest_path_buf)
      {
        TBSYS_LOG(ERROR, "migrate_tablet failed to allocate memory for path array.");
        rc.result_code_ = OB_ALLOCATE_MEMORY_FAILED;
      }
      else
      {
        src_path = new(src_path_buf)char[ObTablet::MAX_SSTABLE_PER_TABLET][OB_MAX_FILE_NAME_LENGTH];
        dest_path = new(dest_path_buf)char[ObTablet::MAX_SSTABLE_PER_TABLET][OB_MAX_FILE_NAME_LENGTH];
      }
      
      int64_t num_file = 0;
      int64_t tablet_version = 0;
      int32_t dest_disk_no = 0;
      uint64_t crc_sum = 0;

      ObMultiVersionTabletImage & tablet_image = tablet_manager.get_serving_tablet_image();        
      if (OB_SUCCESS == rc.result_code_)
      {
        rc.result_code_ = tablet_manager.migrate_tablet(range, 
            dest_server, src_path, dest_path, num_file, tablet_version, dest_disk_no, crc_sum);
        if (OB_SUCCESS != rc.result_code_)
        {
          TBSYS_LOG(WARN, "ObTabletManager::migrate_tablet <%s> error, rc.result_code_=%d", 
              range_buf, rc.result_code_);
        }
      }


      //send request to load sstable
      if (OB_SUCCESS == rc.result_code_)
      {
        rc.result_code_= cs_rpc_stub.dest_load_tablet(dest_server,
            range, dest_disk_no, tablet_version, crc_sum, num_file, dest_path);
        if (OB_CS_MIGRATE_IN_EXIST == rc.result_code_)
        {
          TBSYS_LOG(INFO, "dest server already hold this tablet, consider it done.");
          rc.result_code_ = OB_SUCCESS;
        }
        if (OB_SUCCESS != rc.result_code_)
        {
          TBSYS_LOG(WARN, "dest server load tablet <%s> error ,rc.code=%d", 
              range_buf, rc.result_code_);
        }
      }

      /**
       * it's better to decrease the migrate task counter before 
       * calling migrate_over(), because rootserver keeps the same 
       * migrate task counter of each chunkserver, if migrate_over() 
       * returned, rootserver has decreased the migrate task counter 
       * and send a new migrate task to this chunkserver immediately, 
       * if this chunkserver hasn't decreased the migrate task counter
       * at this time, the chunkserver will return -1010 error. if 
       * rootserver doesn't send migrate message successfully, it 
       * doesn't increase the migrate task counter, so it sends 
       * migrate message continiously and it receives a lot of error 
       * -1010. 
       */
      if (0 != is_inc_migrate_task_count)
      {
        atomic_dec(&migrate_task_count_);
      }

      ObRootServerRpcStub & rs_rpc_stub = chunk_server_->get_rs_rpc_stub();
      //report migrate info to root server
      if (OB_SUCCESS == rc.result_code_)
      {
        rc.result_code_= rs_rpc_stub.migrate_over(range, 
            chunk_server_->get_self(), dest_server, keep_src, tablet_version);
        if (OB_SUCCESS != rc.result_code_)
        {
          TBSYS_LOG(WARN, "report migrate tablet <%s> over error, rc.code=%d", 
              range_buf, rc.result_code_);
        }
        else
        {
          TBSYS_LOG(INFO,"report migrate tablet <%s> over to rootserver success.",
              range_buf);
        }
      }

      if( OB_SUCCESS == rc.result_code_ && false == keep_src)
      {
        // migrate/move , set local tablet merged, 
        // will be discarded in next time merge process .
        ObTablet *src_tablet = NULL;
        rc.result_code_ = tablet_image.acquire_tablet(range,
            ObMultiVersionTabletImage::SCAN_FORWARD, 0, src_tablet);
        if (OB_SUCCESS == rc.result_code_ && NULL != src_tablet)
        {
          char range_buf[OB_RANGE_STR_BUFSIZ];
          src_tablet->get_range().to_string(range_buf, sizeof(range_buf));
          TBSYS_LOG(INFO, "src tablet set merged, version=%ld,disk=%d, range:%s",
              src_tablet->get_data_version(), src_tablet->get_disk_no(), range_buf);
          src_tablet->set_merged(); 
          src_tablet->set_removed();
          tablet_image.write(
              src_tablet->get_data_version(), src_tablet->get_disk_no());
        }

        if (NULL != src_tablet)
        {
          tablet_image.release_tablet(src_tablet);
        }

      }

      if ( NULL != src_path_buf )
      {
        ob_free(src_path_buf);
      }
      if ( NULL != dest_path_buf )
      {
        ob_free(dest_path_buf);
      }     
      TBSYS_LOG(INFO, "migrate_tablet finish rc.code=%d",rc.result_code_);
      
      return rc.result_code_;
    }

    int ObChunkService::cs_get_migrate_dest_loc(
        const int32_t version,
        const int32_t channel_id,
        tbnet::Connection* connection, 
        common::ObDataBuffer& in_buffer, 
        common::ObDataBuffer& out_buffer)
    {
      const int32_t CS_GET_MIGRATE_DEST_LOC_VERSION = 1;
      common::ObResultCode rc;
      rc.result_code_ = OB_SUCCESS;

      if (version != CS_GET_MIGRATE_DEST_LOC_VERSION)
      {
        rc.result_code_ = OB_ERROR_FUNC_VERSION;
      }
      else if (chunk_server_->get_param().get_merge_migrate_concurrency() == 0
          && (!chunk_server_->get_tablet_manager().get_chunk_merge().is_merge_stoped()))
      {
        TBSYS_LOG(WARN, "merge running, cannot migrate in.");
        rc.result_code_ = OB_CS_EAGAIN;
      }


      int64_t occupy_size = 0;
      //deserialize occupy_size
      if (OB_SUCCESS == rc.result_code_)
      {
        rc.result_code_ = serialization::decode_vi64(in_buffer.get_data(), 
            in_buffer.get_capacity(), in_buffer.get_position(), &occupy_size);
        if (OB_SUCCESS != rc.result_code_)
        {
          TBSYS_LOG(ERROR, "parse cs_get_migrate_dest_loc occupy_size param error.");
        }
        else
        {
          TBSYS_LOG(INFO, "cs_get_migrate_dest_loc occupy_size =%ld", occupy_size);
        }
      }

      int32_t disk_no = 0;
      char dest_directory[OB_MAX_FILE_NAME_LENGTH];
      
      ObTabletManager & tablet_manager = chunk_server_->get_tablet_manager();
      disk_no = tablet_manager.get_disk_manager().get_disk_for_migrate();
      if (disk_no <= 0) 
      {
        TBSYS_LOG(ERROR, "get wrong disk no =%d", disk_no);
        rc.result_code_ = OB_ERROR;
      }
      else
      {
        rc.result_code_ = get_sstable_directory(disk_no, dest_directory, OB_MAX_FILE_NAME_LENGTH);
      }

      //response to root server first
      int serialize_ret = rc.serialize(out_buffer.get_data(), 
          out_buffer.get_capacity(), out_buffer.get_position());
      if (serialize_ret != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR, "rc.serialize error");
      }

      // ifreturn success , we can return disk_no & dest_directory.
      if (OB_SUCCESS == rc.result_code_ && OB_SUCCESS == serialize_ret)
      {
        serialize_ret = serialization::encode_vi32(out_buffer.get_data(), 
            out_buffer.get_capacity(), out_buffer.get_position(), disk_no);
        if (OB_SUCCESS != serialize_ret)
        {
          TBSYS_LOG(ERROR, "serialize disk_no failed.");
        }
      }

      if (OB_SUCCESS == rc.result_code_ && OB_SUCCESS == serialize_ret)
      {
        ObString dest_string(OB_MAX_FILE_NAME_LENGTH, 
            static_cast<int32_t>(strlen(dest_directory)), dest_directory);
        serialize_ret = dest_string.serialize(out_buffer.get_data(), 
            out_buffer.get_capacity(), out_buffer.get_position());
        if (OB_SUCCESS != serialize_ret)
        {
          TBSYS_LOG(ERROR, "serialize dest_directory failed.");
        }
      }

      if (OB_SUCCESS == serialize_ret)
      {
        chunk_server_->send_response(
            OB_CS_GET_MIGRATE_DEST_LOC_RESPONSE, 
            CS_GET_MIGRATE_DEST_LOC_VERSION, 
            out_buffer, connection, channel_id);
      }

      return rc.result_code_;
    }

    int ObChunkService::cs_dump_tablet_image(
        const int32_t version,
        const int32_t channel_id,
        tbnet::Connection* connection, 
        common::ObDataBuffer& in_buffer, 
        common::ObDataBuffer& out_buffer)
    {
      const int32_t CS_DUMP_TABLET_IMAGE_VERSION = 1;
      common::ObResultCode rc;
      rc.result_code_ = OB_SUCCESS;

      int32_t index = 0;
      int32_t disk_no = 0;

      char *dump_buf = NULL;
      const int64_t dump_size = OB_MAX_PACKET_LENGTH - 1024;

      int64_t pos = 0;
      ObTabletImage * tablet_image = NULL;

      if (version != CS_DUMP_TABLET_IMAGE_VERSION)
      {
        rc.result_code_ = OB_ERROR_FUNC_VERSION;
      }
      else if (OB_SUCCESS != ( rc.result_code_ = 
            serialization::decode_vi32(in_buffer.get_data(), 
            in_buffer.get_capacity(), in_buffer.get_position(), &index)))
      {
        TBSYS_LOG(WARN, "parse cs_dump_tablet_image index param error.");
      }
      else if (OB_SUCCESS != ( rc.result_code_ = 
            serialization::decode_vi32(in_buffer.get_data(), 
            in_buffer.get_capacity(), in_buffer.get_position(), &disk_no)))
      {
        TBSYS_LOG(WARN, "parse cs_dump_tablet_image disk_no param error.");
      }
      else if (disk_no <= 0)
      {
        TBSYS_LOG(WARN, "cs_dump_tablet_image input param error, "
            "disk_no=%d", disk_no);
        rc.result_code_ = OB_INVALID_ARGUMENT;
      }
      else if (NULL == (dump_buf = static_cast<char*>(ob_malloc(dump_size))))
      {
        rc.result_code_ = OB_ALLOCATE_MEMORY_FAILED;
        TBSYS_LOG(ERROR, "allocate memory for serialization failed.");
      }
      else if (OB_SUCCESS != (rc.result_code_ = 
            chunk_server_->get_tablet_manager().get_serving_tablet_image().
            serialize(index, disk_no, dump_buf, dump_size, pos)))
      {
        TBSYS_LOG(WARN, "serialize tablet image failed. disk_no=%d", disk_no);
      }

      //response to root server first
      int serialize_ret = rc.serialize(out_buffer.get_data(), 
          out_buffer.get_capacity(), out_buffer.get_position());
      if (serialize_ret != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR, "rc.serialize error");
      }

      // ifreturn success , we can return dump_buf
      if (OB_SUCCESS == rc.result_code_ && OB_SUCCESS == serialize_ret)
      {
        ObString return_dump_obj(static_cast<int32_t>(pos), static_cast<int32_t>(pos), dump_buf);
        serialize_ret = return_dump_obj.serialize(out_buffer.get_data(), 
            out_buffer.get_capacity(), out_buffer.get_position());
        if (OB_SUCCESS != serialize_ret)
        {
          TBSYS_LOG(ERROR, "serialize return_dump_obj failed.");
        }
      }

      if (OB_SUCCESS == serialize_ret)
      {
        chunk_server_->send_response(
            OB_CS_DUMP_TABLET_IMAGE_RESPONSE, 
            CS_DUMP_TABLET_IMAGE_VERSION, 
            out_buffer, connection, channel_id);
      }

      if (NULL != dump_buf) 
      {
        ob_free(dump_buf);
        dump_buf = NULL;
      }
      if (NULL != tablet_image) 
      { 
        delete(tablet_image); 
        tablet_image = NULL; 
      }

      return rc.result_code_;
    }

    int ObChunkService::cs_fetch_stats(
        const int32_t version,
        const int32_t channel_id,
        tbnet::Connection* connection, 
        common::ObDataBuffer& in_buffer, 
        common::ObDataBuffer& out_buffer)
    {
      UNUSED(in_buffer);
      const int32_t CS_FETCH_STATS_VERSION = 1;
      int ret = OB_SUCCESS;

      common::ObResultCode rc;
      rc.result_code_ = OB_SUCCESS;

      if (version != CS_FETCH_STATS_VERSION)
      {
        rc.result_code_ = OB_ERROR_FUNC_VERSION;
      }

      ret = rc.serialize(out_buffer.get_data(),out_buffer.get_capacity(), out_buffer.get_position());
      if (ret != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR, "rc.serialize error");
      }
      
      // ifreturn success , we can return dump_buf
      if (OB_SUCCESS == rc.result_code_ && OB_SUCCESS == ret)
      {
        ret = chunk_server_->get_stat_manager().serialize(
            out_buffer.get_data(),out_buffer.get_capacity(),out_buffer.get_position());
      }

      if (OB_SUCCESS == ret)
      {
        chunk_server_->send_response(
            OB_FETCH_STATS_RESPONSE,
            CS_FETCH_STATS_VERSION,
            out_buffer, connection, channel_id);
      }
      return ret;
    }

    int ObChunkService::cs_start_gc(
        const int32_t version,
        const int32_t channel_id,
        tbnet::Connection* connection, 
        common::ObDataBuffer& in_buffer, 
        common::ObDataBuffer& out_buffer)
    {
      const int32_t CS_START_GC_VERSION = 1;
      int ret = OB_SUCCESS;
      int64_t recycle_version = 0;

      common::ObResultCode rc;
      rc.result_code_ = OB_SUCCESS;

      if (version != CS_START_GC_VERSION)
      {
        rc.result_code_ = OB_ERROR_FUNC_VERSION;
      }

      if (OB_SUCCESS == ret && OB_SUCCESS == rc.result_code_ )
      {
        rc.result_code_ = serialization::decode_vi64(in_buffer.get_data(), in_buffer.get_capacity(), 
            in_buffer.get_position(), &recycle_version);
      }

      ret = rc.serialize(out_buffer.get_data(),out_buffer.get_capacity(), out_buffer.get_position());
      if (ret != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR, "rc.serialize error");
      }
      
      if (OB_SUCCESS == ret)
      {
        chunk_server_->send_response(
            OB_RESULT,
            CS_START_GC_VERSION,
            out_buffer, connection, channel_id);
      }
      chunk_server_->get_tablet_manager().start_gc(recycle_version);
      return ret;
    }

    int ObChunkService::cs_check_tablet(
        const int32_t version,
        const int32_t channel_id,
        tbnet::Connection* connection, 
        common::ObDataBuffer& in_buffer, 
        common::ObDataBuffer& out_buffer)
    {
      const int32_t DEFAULT_VERSION = 1;
      int ret = OB_SUCCESS;
      int64_t table_id = 0;
      ObRange range;
      ObTablet* tablet = NULL;

      common::ObResultCode rc;
      rc.result_code_ = OB_SUCCESS;

      if (version != DEFAULT_VERSION)
      {
        rc.result_code_ = OB_ERROR_FUNC_VERSION;
      }
      //if (OB_SUCCESS == ret && OB_SUCCESS == rc.result_code_ )
      else if (OB_SUCCESS != 
          (rc.result_code_ = serialization::decode_vi64(in_buffer.get_data(), 
             in_buffer.get_capacity(), in_buffer.get_position(), &table_id)))
      {
        TBSYS_LOG(WARN, "deserialize table id error. pos=%ld, cap=%ld",
            in_buffer.get_position(), in_buffer.get_capacity());
        rc.result_code_ = OB_INVALID_ARGUMENT;
      }
      else 
      {
        range.table_id_ = table_id;
        range.border_flag_.set_min_value();
        range.border_flag_.set_max_value();
        rc.result_code_ = chunk_server_->get_tablet_manager().get_serving_tablet_image().acquire_tablet(
            range, ObMultiVersionTabletImage::SCAN_FORWARD, 0, tablet);
        if (NULL != tablet) 
        {
          chunk_server_->get_tablet_manager().get_serving_tablet_image().release_tablet(tablet);
        }
      }

      if (OB_SUCCESS != (ret = rc.serialize(out_buffer.get_data(),
              out_buffer.get_capacity(), out_buffer.get_position())))
      {
        TBSYS_LOG(ERROR, "rc.serialize error");
      }
      else
      {
        chunk_server_->send_response(
            OB_CS_CHECK_TABLET_RESPONSE,
            DEFAULT_VERSION,
            out_buffer, connection, channel_id);
      }
      return ret;
    }

    int ObChunkService::cs_reload_conf(
        const int32_t version,
        const int32_t channel_id,
        tbnet::Connection* connection, 
        common::ObDataBuffer& in_buffer, 
        common::ObDataBuffer& out_buffer)
    {
      int ret = OB_SUCCESS;
      const int32_t CS_RELOAD_CONF_VERSION = 1;

      ObString conf_file;
      char config_file_str[OB_MAX_FILE_NAME_LENGTH];
      common::ObResultCode rc;
      rc.result_code_ = OB_SUCCESS;

      if (version != CS_RELOAD_CONF_VERSION)
      {
        rc.result_code_ = OB_ERROR_FUNC_VERSION;
      }
      else
      {
        if ( OB_SUCCESS != (rc.result_code_ = 
              conf_file.deserialize(in_buffer.get_data(), 
                in_buffer.get_capacity(), in_buffer.get_position())))
        {
          TBSYS_LOG(WARN, "deserialize conf file error, ret=%d", ret);
        }
        else
        {
          int64_t length = conf_file.length();
          strncpy(config_file_str, conf_file.ptr(), length);
          config_file_str[length] = '\0';
          TBSYS_LOG(INFO, "reload conf from file %s", config_file_str);
        }
      }

      if (OB_SUCCESS == rc.result_code_)
      {
        if (OB_SUCCESS != (rc.result_code_ = 
              chunk_server_->get_param().reload_from_config(config_file_str)))
        {
          TBSYS_LOG(WARN, "failed to reload config, ret=%d", ret);
        }
        else
        {
          chunk_server_->get_tablet_manager().get_chunk_merge().set_config_param();
          chunk_server_->set_default_queue_size(static_cast<int32_t>(chunk_server_->get_param().get_task_queue_size()));
          chunk_server_->set_min_left_time(chunk_server_->get_param().get_task_left_time());
          ObMergerRpcProxy* rpc_proxy = chunk_server_->get_rpc_proxy();
          if (NULL != rpc_proxy)
          {
            ret = rpc_proxy->set_rpc_param(chunk_server_->get_param().get_retry_times(),
              chunk_server_->get_param().get_network_time_out());
            if (OB_SUCCESS == ret)
            {
              ret = rpc_proxy->set_blacklist_param(
                chunk_server_->get_param().get_ups_blacklist_timeout(),
                chunk_server_->get_param().get_ups_fail_count());
              if (OB_SUCCESS != ret)
              {
                TBSYS_LOG(WARN, "set update server black list param failed:ret=%d", ret);
              }
            } 
          }
          else 
          {
            TBSYS_LOG(WARN, "get rpc proxy from chunkserver failed");
            ret = OB_ERROR;
          }
        }
      }

      ret = rc.serialize(out_buffer.get_data(),out_buffer.get_capacity(), out_buffer.get_position());
      if (ret != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR, "rc.serialize error");
      }
      else
      {
        chunk_server_->send_response(
            OB_UPS_RELOAD_CONF_RESPONSE,
            CS_RELOAD_CONF_VERSION,
            out_buffer, connection, channel_id);
      }

      return ret;
    }

    int ObChunkService::cs_show_param(
        const int32_t version,
        const int32_t channel_id,
        tbnet::Connection* connection, 
        common::ObDataBuffer& in_buffer, 
        common::ObDataBuffer& out_buffer)
    {
      const int32_t CS_SHOW_PARAM_VERSION = 1;
      int ret = OB_SUCCESS;
      UNUSED(in_buffer);

      common::ObResultCode rc;
      rc.result_code_ = OB_SUCCESS;

      if (version != CS_SHOW_PARAM_VERSION)
      {
        rc.result_code_ = OB_ERROR_FUNC_VERSION;
      }

      ret = rc.serialize(out_buffer.get_data(),out_buffer.get_capacity(), out_buffer.get_position());
      if (ret != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR, "rc.serialize error");
      }
      
      if (OB_SUCCESS == ret)
      {
        chunk_server_->send_response(
            OB_RESULT,
            CS_SHOW_PARAM_VERSION,
            out_buffer, connection, channel_id);
      }
      chunk_server_->get_param().show_param();
      ob_print_mod_memory_usage();
      return ret;
    }

    int ObChunkService::cs_stop_server(
      const int32_t version,
      const int32_t channel_id,
      tbnet::Connection* connection, 
      common::ObDataBuffer& in_buffer, 
      common::ObDataBuffer& out_buffer)
    {
      UNUSED(in_buffer);
      common::ObResultCode rc;
      rc.result_code_ = OB_SUCCESS;

      //int64_t server_id = chunk_server_->get_root_server().get_ipv4_server_id();
      int64_t peer_id = connection->getPeerId();

      /*
      if (server_id != peer_id)
      {
        TBSYS_LOG(WARN, "*stop server* WARNNING coz packet from unrecongnized address "
                  "which is [%lld], should be [%lld] as rootserver.", peer_id, server_id);
        // comment follow line not to strict packet from rs.
        // rc.result_code_ = OB_ERROR;
      }
      */
      
      int32_t restart = 0;
      rc.result_code_ = serialization::decode_i32(in_buffer.get_data(), in_buffer.get_capacity(), 
                                                  in_buffer.get_position(), &restart);

      //int64_t pos = 0;
      //rc.result_code_ = serialization::decode_i32(in_buffer.get_data(), in_buffer.get_position(), pos, &restart);

      if (restart != 0)
      {
        BaseMain::set_restart_flag();
        TBSYS_LOG(INFO, "receive *restart server* packet from: [%ld]", peer_id);
      }
      else
      {
        TBSYS_LOG(INFO, "receive *stop server* packet from: [%ld]", peer_id);
      }
      
      int serialize_ret = rc.serialize(out_buffer.get_data(), 
          out_buffer.get_capacity(), out_buffer.get_position());
      if (serialize_ret != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR, "rc.serialize error");
      }

      if (OB_SUCCESS == serialize_ret)
      {
        chunk_server_->send_response(
          OB_STOP_SERVER_RESPONSE,
          version,
          out_buffer, connection, channel_id);
      }

      if (OB_SUCCESS == rc.result_code_)
      {
        
        chunk_server_->stop();
      }
      return rc.result_code_;
    }

    int ObChunkService::cs_force_to_report_tablet(
        const int32_t version,
        const int32_t channel_id,
        tbnet::Connection* connection,
        common::ObDataBuffer& in_buffer,
        common::ObDataBuffer& out_buffer)
    {
      int ret = OB_SUCCESS;
      UNUSED(channel_id);
      UNUSED(connection);
      UNUSED(in_buffer);
      UNUSED(out_buffer);
      static const int MY_VERSION = 1;
      static volatile uint64_t report_in_process = 0;
      TBSYS_LOG(INFO, "cs receive force_report_tablet to rs. maybe have some network trouble");
      if (MY_VERSION != version)
      {
        ret = OB_ERROR_FUNC_VERSION;
        TBSYS_LOG(WARN, "force to report tablet verion not equal. my_version=%d, receive_version=%d", MY_VERSION, version);
      }
      ObTabletManager & tablet_manager = chunk_server_->get_tablet_manager();
      if (OB_SUCCESS == ret)
      {
        if (in_register_process_ || tablet_manager.get_chunk_merge().is_pending_in_upgrade()
            || 0 != atomic_compare_exchange(&report_in_process, 1, 0))
        {
          TBSYS_LOG(WARN, "someone else is reporting. give up this process");
        }
        else
        {
          ret = report_tablets_busy_wait();
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "fail to report tablets. err = %d", ret);
          }
          else
          {
            tablet_manager.report_capacity_info();
          }
          atomic_exchange(&report_in_process, 0);
        }
      }
      return ret;
    }

    int ObChunkService::cs_change_log_level(
      const int32_t version,
      const int32_t channel_id,
      tbnet::Connection* connection,
      common::ObDataBuffer& in_buffer, 
      common::ObDataBuffer& out_buffer)
    {
      int ret = OB_SUCCESS;
      static const int MY_VERSION = 1;
      UNUSED(version);
      common::ObResultCode result;
      result.result_code_ = OB_SUCCESS;
      int32_t log_level = -1;
      if (OB_SUCCESS != (ret = serialization::decode_vi32(in_buffer.get_data(),
                                                          in_buffer.get_capacity(),
                                                          in_buffer.get_position(),
                                                          &log_level)))
      {
        TBSYS_LOG(WARN, "deserialize error, err=%d", ret);
      }
      else
      {
        if (TBSYS_LOG_LEVEL_ERROR <= log_level
            && TBSYS_LOG_LEVEL_DEBUG >= log_level)
        {
          TBSYS_LOG(INFO, "change log level. From: %d, To: %d", TBSYS_LOGGER._level, log_level);
          TBSYS_LOGGER._level = log_level;
        }
        else
        {
          TBSYS_LOG(WARN, "invalid log level, level=%d", log_level);
          result.result_code_ = OB_INVALID_ARGUMENT;
        }
        if (OB_SUCCESS != (ret = result.serialize(out_buffer.get_data(),
                                                  out_buffer.get_capacity(),
                                                  out_buffer.get_position())))
        {
          TBSYS_LOG(WARN, "serialize error, err=%d", ret);
        }
        else
        {
          ret = chunk_server_->send_response(OB_RS_ADMIN_RESPONSE, MY_VERSION, out_buffer, connection, channel_id);
        }
      }
      return ret;
    }

    int ObChunkService::cs_sync_all_images(
        const int32_t version,
        const int32_t channel_id,
        tbnet::Connection* connection, 
        common::ObDataBuffer& in_buffer, 
        common::ObDataBuffer& out_buffer)
    {
      const int32_t CS_SYNC_ALL_IMAGES_VERSION = 1;
      int ret = OB_SUCCESS;
      UNUSED(in_buffer);

      ObTabletManager& tablet_manager = chunk_server_->get_tablet_manager();
      common::ObResultCode rc;
      rc.result_code_ = OB_SUCCESS;

      TBSYS_LOG(INFO, "chunkserver start sync all tablet images");
      if (version != CS_SYNC_ALL_IMAGES_VERSION)
      {
        rc.result_code_ = OB_ERROR_FUNC_VERSION;
      }

      ret = rc.serialize(out_buffer.get_data(),out_buffer.get_capacity(), out_buffer.get_position());
      if (ret != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR, "rc.serialize error");
      }
      
      if (OB_SUCCESS == ret)
      {
        chunk_server_->send_response(
            OB_RESULT,
            CS_SYNC_ALL_IMAGES_VERSION,
            out_buffer, connection, channel_id);
      }

      if (inited_ && service_started_ 
          && !tablet_manager.get_chunk_merge().is_pending_in_upgrade())
      {
        rc.result_code_ = tablet_manager.sync_all_tablet_images();
      }
      else
      {
        TBSYS_LOG(WARN, "can't sync tablet images now, please try again later");
        rc.result_code_ = OB_CS_EAGAIN;
      }
      TBSYS_LOG(INFO, "finish sync all tablet images, ret=%d", rc.result_code_);

      return ret;
    }

    bool ObChunkService::is_valid_lease()
    {
      int64_t current_time = tbsys::CTimeUtil::getTime();
      return current_time < service_expired_time_;
    }
    
    bool ObChunkService::have_frozen_table(const ObTablet& tablet,const int64_t ups_data_version) const
    {
      bool ret = false;
      int64_t                     cs_data_version = tablet.get_cache_data_version();
      int64_t                     cs_major        = ObVersion::get_major(cs_data_version);
      int64_t                     ups_major       = ObVersion::get_major(ups_data_version);      
      int64_t                     ups_minor       = ObVersion::get_minor(ups_data_version);
      int64_t                     cs_minor        = ObVersion::get_minor(cs_data_version);      
      bool                        is_final_minor  = ObVersion::is_final_minor(cs_data_version);

      //这里有两种情况，一种是cs还没有compactsstable,此时ups不可能还有cs_major版本的数据，
      //需要判断ups的minor大于OB_UPS_START_MINOR,比如cs的版本为1且没有compactsstable,那么ups版本为
      //2,2的时候才表明有新的minor freeze
      //另外一种情况是cs已经有了一部份compactsstable,cs_major和ups_major相等，如果minor的版本
      //相差大于1则有新的冻结表，比如cs的版本为(2，1)，ups版本为(2,3)。如果ups_major大于cs_major，
      //且cs_major版本下的所有数据已经拉取完成(is_final_minor为true),说明有新的大版本产生
      //目前限制cs最多只能拥有一个大版本的数据。

      TBSYS_LOG(DEBUG,"cs_version:%ld:%ld,ups_version:%ld:%ld,is_final_minor:%s",
                cs_major,cs_minor,ups_major,ups_minor,is_final_minor ? "Yes" : "No");

      if (cs_major <= 0)
      {
        ret = false; //initialize phase
      }
      else if (0 == cs_minor)
      {
        //cs have no compactsstable and ups major version equal to cs_major + 1,
        //then if usp_minor > 1,there is a frozen table        
        if ( (ups_major > (cs_major + 1)) ||
             ((ups_major == (cs_major + 1)) && (ups_minor > OB_UPS_START_MINOR_VERSION)))
        {
          ret = true;
        }
      }
      else
      {
        //there is some compactsstable already
        if ((cs_major == ups_major) && ((ups_minor - cs_minor) > 1))
        {
          ret = true;
        }
        else if (ups_major > cs_major)
        {
          if (is_final_minor)
          {
            TBSYS_LOG(DEBUG,"there is some new frozen table,but we can hold only one major version");
          }
          else
          {
            ret = true; 
          }
        }
        else
        {
          //there is no new frozen table
        }
      }

      return ret;
    }

    int ObChunkService::check_update_data(ObTablet& tablet,int64_t ups_data_version,bool& release_tablet)
    {
      int     ret = OB_SUCCESS;
      int64_t compactsstable_cache_size = chunk_server_->get_param().get_compactsstable_cache_size();
      int64_t usage_size = ob_get_mod_memory_usage(ObModIds::OB_COMPACTSSTABLE_WRITER);
      const ObSchemaManagerV2* schema_manager = chunk_server_->get_schema_manager()->get_schema(0);
      release_tablet = true;

      if (NULL == schema_manager)
      {
        ret = OB_ERROR;
      }
      else if ((compactsstable_cache_size > usage_size) &&
          !chunk_server_->get_tablet_manager().get_cache_thread().is_full())
      {
        ObTabletManager& tablet_manager = chunk_server_->get_tablet_manager();
        const ObRange& range = tablet.get_range();

        if (have_frozen_table(tablet,ups_data_version) &&
            (!merge_task_.is_scheduled() && tablet_manager.get_chunk_merge().is_merge_stoped()))
        {
          if (!schema_manager->is_join_table(range.table_id_))
          {
            if (tablet.compare_and_set_compactsstable_loading())
            {
              if ( (tablet.get_compactsstable_num() > OB_MAX_COMPACTSSTABLE_NUM) ||
                   ((ret = tablet_manager.get_cache_thread().push(&tablet,ups_data_version)) != OB_SUCCESS))
              {
                TBSYS_LOG(WARN,"put %p to cache thread failed,ret=%d,compactsstable num:%d",
                          &tablet,ret,tablet.get_compactsstable_num());
                tablet.clear_compactsstable_flag();
              }
              else
              {
                //do not release this tablet
                release_tablet = false;
                TBSYS_LOG(INFO,"push [%p,%s] to cache thread,ups_data_version:%ld",
                          &tablet,scan_range2str(tablet.get_range()),ups_data_version);
              }
            }
          }
          else //join table
          {
            ret = check_update_data_join_table(range.table_id_,ups_data_version);
          }
        }
      }

      if (NULL != schema_manager)
      {
        chunk_server_->get_schema_manager()->release_schema(schema_manager->get_version());        
      }
      return ret;
    }

    int ObChunkService::check_update_data_join_table(const uint64_t table_id,const int64_t ups_data_version)
    {
      int ret                                   = OB_SUCCESS;
      ObTabletManager& tablet_manager           = chunk_server_->get_tablet_manager();
      ObJoinCompactSSTable& join_compactsstable = tablet_manager.get_join_compactsstable();
      ObTablet* tablet                          = join_compactsstable.get_join_tablet(table_id);
      
      if (OB_INVALID_ID == table_id)
      {
        ret = OB_INVALID_ARGUMENT;
      }

      if ((OB_SUCCESS == ret) && (NULL == tablet))
      {
        if (join_compactsstable.compare_and_set_load_flag())
        {
           if((ret = join_compactsstable.add_join_table(table_id)) != OB_SUCCESS)
           {
             TBSYS_LOG(WARN,"add join table failed,ret=%d,tablet_id=%lu",ret,table_id);
           }
           join_compactsstable.clear_load_flag();
        }
        tablet = join_compactsstable.get_join_tablet(table_id);
      }

      if ((OB_SUCCESS == ret) && (tablet != NULL))
      {
        if (tablet->compare_and_set_compactsstable_loading())
        {
          if ( (tablet->get_compactsstable_num() > OB_MAX_COMPACTSSTABLE_NUM) ||
               ((ret = tablet_manager.get_cache_thread().push(tablet,ups_data_version)) != OB_SUCCESS))
          {
            TBSYS_LOG(WARN,"put [%p,%s] to cache thread failed,ret=%d,compactsstable num:%d, ret:%d",
                      &tablet,scan_range2str(tablet->get_range()),ret,tablet->get_compactsstable_num(), ret);
            tablet->clear_compactsstable_flag();
          }
          else
          {
            TBSYS_LOG(INFO,"push [%p,%s] to cache thread,ups_data_version:[%ld,%ld,is_final_minor=%s] %p %ld",
                      tablet,scan_range2str(tablet->get_range()),
                      ObVersion::get_major(ups_data_version), ObVersion::get_minor(ups_data_version),
                      ObVersion::is_final_minor(ups_data_version) ? "true" : "false",
                      &join_compactsstable, table_id);
          }        
        }
      }
      return ret;
    }

    void ObChunkService::get_merge_delay_interval()
    {
      int64_t interval = 0;
      int rc = OB_SUCCESS;
      ObRootServerRpcStub & rs_rpc_stub = chunk_server_->get_rs_rpc_stub();
      rc = rs_rpc_stub.get_merge_delay_interval(interval);
      if (OB_SUCCESS == rc) 
      {
        merge_delay_interval_ = interval;
      }
      else
      {
        merge_delay_interval_ = chunk_server_->get_param().get_merge_delay_interval();
      }
      TBSYS_LOG(INFO,"merge_delay_interval_ is %ld",merge_delay_interval_);
    }

    void ObChunkService::LeaseChecker::runTimerTask()
    {
      if (NULL != service_ && service_->inited_)
      {
        if (!service_->is_valid_lease() && !service_->in_register_process_ )
        {
          TBSYS_LOG(INFO, "lease expired, re-register to root_server");
          service_->register_self();
        }

        // memory usage stats
        ObTabletManager& manager = service_->chunk_server_->get_tablet_manager();
        OB_CHUNK_STAT(set_value, ObChunkServerStatManager::META_TABLE_ID, 
            ObChunkServerStatManager::INDEX_MU_DEFAULT, 
            ob_get_mod_memory_usage(ObModIds::OB_MOD_DEFAULT));
        OB_CHUNK_STAT(set_value, ObChunkServerStatManager::META_TABLE_ID, 
            ObChunkServerStatManager::INDEX_MU_NETWORK, 
            ob_get_mod_memory_usage(ObModIds::OB_COMMON_NETWORK));
        OB_CHUNK_STAT(set_value, ObChunkServerStatManager::META_TABLE_ID, 
            ObChunkServerStatManager::INDEX_MU_THREAD_BUFFER, 
            ob_get_mod_memory_usage(ObModIds::OB_THREAD_BUFFER));
        OB_CHUNK_STAT(set_value, ObChunkServerStatManager::META_TABLE_ID, 
            ObChunkServerStatManager::INDEX_MU_TABLET, 
            ob_get_mod_memory_usage(ObModIds::OB_CS_TABLET_IMAGE));
        OB_CHUNK_STAT(set_value, ObChunkServerStatManager::META_TABLE_ID, 
            ObChunkServerStatManager::INDEX_MU_BI_CACHE, 
            manager.get_serving_block_index_cache().get_cache_mem_size());
        OB_CHUNK_STAT(set_value, ObChunkServerStatManager::META_TABLE_ID, 
            ObChunkServerStatManager::INDEX_MU_BLOCK_CACHE, 
            manager.get_serving_block_cache().size());
        OB_CHUNK_STAT(set_value, ObChunkServerStatManager::META_TABLE_ID, 
            ObChunkServerStatManager::INDEX_MU_BI_CACHE_UNSERVING, 
            manager.get_unserving_block_index_cache().get_cache_mem_size());
        OB_CHUNK_STAT(set_value, ObChunkServerStatManager::META_TABLE_ID, 
            ObChunkServerStatManager::INDEX_MU_BLOCK_CACHE_UNSERVING, 
            manager.get_unserving_block_cache().size());
        if (manager.get_join_cache().is_inited())
        {
          OB_CHUNK_STAT(set_value, ObChunkServerStatManager::META_TABLE_ID, 
              ObChunkServerStatManager::INDEX_MU_JOIN_CACHE, 
              manager.get_join_cache().get_cache_mem_size());
        }

        // reschedule
        service_->timer_.schedule(*this, 
            service_->chunk_server_->get_param().get_lease_check_interval(), false);
      }
    }

    void ObChunkService::StatUpdater::runTimerTask()
    {
      ObStat *stat = NULL;
      THE_CHUNK_SERVER.get_stat_manager().get_stat(ObChunkServerStatManager::META_TABLE_ID,stat);
      if (NULL == stat)
      {
        TBSYS_LOG(DEBUG,"get stat failed");
      }
      else
      {
        int64_t request_count = stat->get_value(ObChunkServerStatManager::INDEX_META_REQUEST_COUNT);
        current_request_count_ = request_count - pre_request_count_;
        stat->set_value(ObChunkServerStatManager::INDEX_META_REQUEST_COUNT_PER_SECOND,current_request_count_);
        pre_request_count_ = request_count;
      }
    }

    void ObChunkService::MergeTask::runTimerTask()
    {
      int err = OB_SUCCESS;
      ObTabletManager & tablet_manager = service_->chunk_server_->get_tablet_manager();
      unset_scheduled();

      if (frozen_version_ <= 0)
      {
        //initialize phase or slave rs switch to master but get frozen version failed
      }
      else if (frozen_version_  < tablet_manager.get_serving_tablet_image().get_newest_version())
      {
        TBSYS_LOG(ERROR,"mem frozen version (%ld) < newest local tablet version (%ld),exit",frozen_version_,
            tablet_manager.get_serving_tablet_image().get_newest_version());
        kill(getpid(),2);
      }
      else if (tablet_manager.get_chunk_merge().can_launch_next_round(frozen_version_))
      {
        err = tablet_manager.merge_tablets(frozen_version_);
        if (err != OB_SUCCESS)
        {
          frozen_version_ = 0; //failed,wait for the next schedule
        }
        service_->get_merge_delay_interval();
      }
      else
      {
        tablet_manager.get_chunk_merge().set_newest_frozen_version(frozen_version_);
      }
    }

    int ObChunkService::fetch_update_server_list()
    {
      int err = OB_SUCCESS;

      if (NULL == chunk_server_->get_rpc_proxy())
      {
        TBSYS_LOG(ERROR, "rpc_proxy_ is NULL");
      }
      else
      {
        int32_t count = 0;
        err = chunk_server_->get_rpc_proxy()->fetch_update_server_list(count);
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN, "fetch update server list failed:ret[%d]", err);
        }
        else
        {
          TBSYS_LOG(DEBUG, "fetch update server list succ:count[%d]", count);
        }
      }

      return err;
    }

    void ObChunkService::FetchUpsTask::runTimerTask()
    {
      service_->fetch_update_server_list();
      // reschedule fetch updateserver list task with new interval.
      service_->timer_.schedule(*this, 
        service_->chunk_server_->get_param().get_fetch_ups_interval(), false);
    }


  } // end namespace chunkserver
} // end namespace oceanbase



