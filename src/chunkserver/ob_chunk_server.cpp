/*
 *   (C) 2007-2010 Taobao Inc.
 *   
 *   Version: 0.1
 *           
 *   Authors:
 *      qushan <qushan@taobao.com>
 *        - some work details if you want
 *               
 */

#include <stdint.h>
#include <tblog.h>
#include "common/ob_trace_log.h"
#include "sstable/ob_sstable_stat.h"
#include "ob_chunk_server.h"
#include "ob_chunk_server_main.h"

using namespace oceanbase::common;

namespace oceanbase 
{ 
#ifndef NO_STAT
  namespace sstable
  {
    using namespace oceanbase::chunkserver;

    void set_stat(const uint64_t table_id, const int32_t index, const int64_t value)
    {
      switch (index)
      {
      case INDEX_BLOCK_INDEX_CACHE_HIT:
        OB_CHUNK_STAT(set_value, table_id, ObChunkServerStatManager::INDEX_BLOCK_INDEX_CACHE_HIT, value);
        break;
      case INDEX_BLOCK_INDEX_CACHE_MISS:
        OB_CHUNK_STAT(set_value, table_id, ObChunkServerStatManager::INDEX_BLOCK_INDEX_CACHE_MISS, value);
        break;
      case INDEX_BLOCK_CACHE_HIT:
        OB_CHUNK_STAT(set_value, table_id, ObChunkServerStatManager::INDEX_BLOCK_CACHE_HIT, value);
        break;
      case INDEX_BLOCK_CACHE_MISS:
        OB_CHUNK_STAT(set_value, table_id, ObChunkServerStatManager::INDEX_BLOCK_CACHE_MISS, value);
        break;
      case INDEX_DISK_IO_NUM:
        OB_CHUNK_STAT(set_value, table_id, ObChunkServerStatManager::INDEX_DISK_IO_NUM, value);
        break;
      case INDEX_DISK_IO_BYTES:
        OB_CHUNK_STAT(set_value, table_id, ObChunkServerStatManager::INDEX_DISK_IO_BYTES, value);
        break;
      case INDEX_SSTABLE_ROW_CACHE_HIT:
        OB_CHUNK_STAT(set_value, table_id, ObChunkServerStatManager::INDEX_SSTABLE_ROW_CACHE_HIT, value);
        break;
      case INDEX_SSTABLE_ROW_CACHE_MISS:
        OB_CHUNK_STAT(set_value, table_id, ObChunkServerStatManager::INDEX_SSTABLE_ROW_CACHE_MISS, value);
        break;
      default:
        break;
      }
    }

    void inc_stat(const uint64_t table_id, const int32_t index, const int64_t inc_value)
    {
      switch (index)
      {
      case INDEX_BLOCK_INDEX_CACHE_HIT:
        OB_CHUNK_STAT(inc, table_id, ObChunkServerStatManager::INDEX_BLOCK_INDEX_CACHE_HIT, inc_value);
        break;
      case INDEX_BLOCK_INDEX_CACHE_MISS:
        OB_CHUNK_STAT(inc, table_id, ObChunkServerStatManager::INDEX_BLOCK_INDEX_CACHE_MISS, inc_value);
        break;
      case INDEX_BLOCK_CACHE_HIT:
        OB_CHUNK_STAT(inc, table_id, ObChunkServerStatManager::INDEX_BLOCK_CACHE_HIT, inc_value);
        break;
      case INDEX_BLOCK_CACHE_MISS:
        OB_CHUNK_STAT(inc, table_id, ObChunkServerStatManager::INDEX_BLOCK_CACHE_MISS, inc_value);
        break;
      case INDEX_DISK_IO_NUM:
        OB_CHUNK_STAT(inc, table_id, ObChunkServerStatManager::INDEX_DISK_IO_NUM, inc_value);
        break;
      case INDEX_DISK_IO_BYTES:
        OB_CHUNK_STAT(inc, table_id, ObChunkServerStatManager::INDEX_DISK_IO_BYTES, inc_value);
        break;
      default:
        break;
      }
    }
  }
#endif

  namespace chunkserver 
  {

    ObChunkServer::ObChunkServer()
      : response_buffer_(RESPONSE_PACKET_BUFFER_SIZE),
      rpc_buffer_(RPC_BUFFER_SIZE)
    {
    }

    ObChunkServer::~ObChunkServer()
    {
    }

    common::ThreadSpecificBuffer::Buffer* ObChunkServer::get_rpc_buffer() const
    {
      return rpc_buffer_.get_buffer();
    }

    common::ThreadSpecificBuffer::Buffer* ObChunkServer::get_response_buffer() const
    {
      return response_buffer_.get_buffer();
    }
        
    const common::ThreadSpecificBuffer* ObChunkServer::get_thread_specific_rpc_buffer() const
    {
      return &rpc_buffer_;
    }

    const common::ObServer& ObChunkServer::get_self() const
    {
      return self_;
    }

    const common::ObServer& ObChunkServer::get_root_server() const
    {
      return param_.get_root_server();
    }

    const common::ObClientManager& ObChunkServer::get_client_manager() const
    {
      return client_manager_;
    }

    const ObChunkServerParam & ObChunkServer::get_param() const 
    {
      return param_;
    }

    ObChunkServerParam & ObChunkServer::get_param() 
    {
      return param_;
    }

    ObChunkServerStatManager & ObChunkServer::get_stat_manager()
    {
      return stat_;
    }

    const ObTabletManager & ObChunkServer::get_tablet_manager() const 
    {
      return tablet_manager_;
    }

    ObTabletManager & ObChunkServer::get_tablet_manager() 
    {
      return tablet_manager_;
    }

    ObRootServerRpcStub & ObChunkServer::get_rs_rpc_stub()
    {
      return rs_rpc_stub_;
    }

    ObMergerRpcStub & ObChunkServer::get_rpc_stub()
    {
      return rpc_stub_;
    }

    ObMergerRpcProxy* ObChunkServer::get_rpc_proxy()
    {
      return rpc_proxy_;
    }

    ObMergerSchemaManager* ObChunkServer::get_schema_manager()
    {
      return schema_mgr_;
    }

    int ObChunkServer::set_self(const char* dev_name, const int32_t port)
    {
      int ret = OB_SUCCESS;
      int32_t ip = tbsys::CNetUtil::getLocalAddr(dev_name);
      if (0 == ip)
      {
        TBSYS_LOG(ERROR, "cannot get valid local addr on dev:%s.", dev_name);
        ret = OB_ERROR;
      }
      if (OB_SUCCESS == ret)
      {
        bool res = self_.set_ipv4_addr(ip, port);
        if (!res)
        {
          TBSYS_LOG(ERROR, "chunk server dev:%s, port:%d is invalid.", 
              dev_name, port);
          ret = OB_ERROR;
        }
      }
      return ret;
    }

    int ObChunkServer::init_merge_join_rpc()
    {
      int ret = OB_SUCCESS;
      ObSchemaManagerV2 *newest_schema_mgr = NULL;
      int64_t retry_times = 0;
      int64_t timeout = 0;

      if (OB_SUCCESS == ret)
      {
        ret = rpc_stub_.init(&rpc_buffer_, &client_manager_);
      }

      if (OB_SUCCESS == ret)
      {
        newest_schema_mgr = new(std::nothrow)ObSchemaManagerV2;
        if (NULL == newest_schema_mgr)
        {
          ret = OB_ALLOCATE_MEMORY_FAILED;
        }
      }

      if (OB_SUCCESS == ret)
      {
        schema_mgr_ = new(std::nothrow)ObMergerSchemaManager;
        if (NULL == schema_mgr_)
        {
          ret = OB_ALLOCATE_MEMORY_FAILED;
        }
        else
        {
          retry_times = param_.get_retry_times();
          timeout = param_.get_network_time_out();
          for (int64_t i = 0; i <= retry_times; ++i)
          {
            ret = rpc_stub_.fetch_schema(timeout, get_root_server(), 
                                          0, *newest_schema_mgr);
            if (OB_SUCCESS == ret)
            {
              ret = schema_mgr_->init(*newest_schema_mgr);
              break;
            }
            usleep(RETRY_INTERVAL_TIME);
          }
        }
      }

      if (OB_SUCCESS == ret)
      {
        rpc_proxy_ = new(std::nothrow)ObMergerRpcProxy(
          retry_times, timeout, get_root_server());
        if (NULL == rpc_proxy_)
        {
          ret = OB_ALLOCATE_MEMORY_FAILED;
        }
      }

      if (OB_SUCCESS == ret)
      {
        ret = rpc_proxy_->init(&rpc_stub_, schema_mgr_);
      }

      // set update server black list param
      if (OB_SUCCESS == ret)
      {
        ret = rpc_proxy_->set_blacklist_param(param_.get_ups_blacklist_timeout(),
            param_.get_ups_fail_count());
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "set update server black list param failed:ret=%d", ret);
        }
      } 

      if (OB_SUCCESS == ret)
      {
        ObServer update_server;
        ret = rpc_proxy_->get_update_server(true, update_server);
      }

      if (OB_SUCCESS == ret)
      {
        int32_t count = 0;
        for (int32_t i = 0; i <= param_.get_retry_times(); ++i)
        {
          ret = rpc_proxy_->fetch_update_server_list(count);
          if (OB_SUCCESS == ret)
          {
            TBSYS_LOG(INFO, "fetch update server list succ:count=%d", count);
            break;
          }
          usleep(RETRY_INTERVAL_TIME);
        }
      }

      if (OB_SUCCESS != ret)
      {
        if (NULL != rpc_proxy_)
        {
          delete rpc_proxy_;
          rpc_proxy_ = NULL;
        }
        
        if (NULL != schema_mgr_)
        {
          delete schema_mgr_;
          schema_mgr_ = NULL;
        }
      }
      
      if (NULL != newest_schema_mgr)
      {
        delete newest_schema_mgr;
        newest_schema_mgr = NULL;
      }

      return ret;
    }

    int ObChunkServer::initialize()
    {
      int ret = OB_SUCCESS;
      // do not handle batch packet.
      // process packet one by one.
      set_batch_process(false);

      // read configure item value from configure file.
      // this step is the very first thing.
      ret = param_.load_from_config();

      // set listen port
      if (OB_SUCCESS == ret) 
      {
        param_.show_param();
        ret = set_listen_port(param_.get_chunk_server_port());
      }

      if (OB_SUCCESS == ret)
      {
        ret = set_dev_name(param_.get_dev_name());
        if (OB_SUCCESS == ret)
        {
          ret = set_self(param_.get_dev_name(), 
              param_.get_chunk_server_port());
        }
      }

      // task queue and work thread count
      if (OB_SUCCESS == ret)
      {
        ret = set_default_queue_size(static_cast<int32_t>(param_.get_task_queue_size()));
      }

      if (OB_SUCCESS == ret)
      {
        ret = set_thread_count(param_.get_task_thread_count());
      }

      if (OB_SUCCESS == ret)
      {
        ret = set_min_left_time(param_.get_task_left_time());
      }

      // set packet factory object.
      if (OB_SUCCESS == ret)
      {
        ret = set_packet_factory(&packet_factory_);
      }

      // initialize client_manager_ for server remote procedure call.
      if (OB_SUCCESS == ret)
      {
        ret = client_manager_.initialize(get_transport(), get_packet_streamer());
      }

      if (OB_SUCCESS == ret)
      {
        ret = rs_rpc_stub_.init( param_.get_root_server(), &client_manager_);
      }

      if (OB_SUCCESS == ret)
      {
        ret = tablet_manager_.init(&param_);
      }

      // server initialize, including start transport, 
      // listen port, accept socket data from client
      if (OB_SUCCESS == ret)
      {
        ret = ObSingleServer::initialize();
      }

      if (OB_SUCCESS == ret)
      {
        ret = service_.initialize(this);
      }

      return ret;
    }

    int ObChunkServer::start_service()
    {
      TBSYS_LOG(INFO, "start service...");
      // finally, start service, handle the request from client.
      return service_.start();
    }

    void ObChunkServer::wait_for_queue()
    {
      ObSingleServer::wait_for_queue();
    }

    void ObChunkServer::destroy()
    {
      ObSingleServer::destroy();
      tablet_manager_.destroy();
      service_.destroy();
      //TODO maybe need more destroy
    }

    tbnet::IPacketHandler::HPRetCode ObChunkServer::handlePacket(
        tbnet::Connection *connection, tbnet::Packet *packet)
    {
      tbnet::IPacketHandler::HPRetCode rc = tbnet::IPacketHandler::FREE_CHANNEL;
      if (NULL == packet || !packet->isRegularPacket())
      {
        TBSYS_LOG(WARN, "packet is illegal, discard.");
      }
      else 
      {
        ObPacket* ob_packet = (ObPacket*) packet;
        ob_packet->set_connection(connection);

        // handle heartbeat packet directly (in tbnet event loop thread)
        // generally, heartbeat service nerver be blocked and must be
        // response immediately, donot put into work thread pool.
        if (ob_packet->get_packet_code() == OB_REQUIRE_HEARTBEAT)
        {
          ObSingleServer::handle_request(ob_packet);
        }
        else
        {
          rc = ObSingleServer::handlePacket(connection, packet);
        }
      }
      return rc;
    }

    int64_t ObChunkServer::get_process_timeout_time(
      const int64_t receive_time, const int64_t network_timeout)
    {
      int64_t timeout_time  = 0;
      int64_t timeout       = network_timeout;

      if (network_timeout <= 0)
      {
        timeout = param_.get_network_time_out();
      }

      timeout_time = receive_time + timeout;

      return timeout_time;
    }

    int ObChunkServer::do_request(ObPacket* base_packet)
    {
      int ret = OB_SUCCESS;
      ObPacket* ob_packet = base_packet;
      int32_t packet_code = ob_packet->get_packet_code();
      int32_t version = ob_packet->get_api_version();
      int32_t channel_id = ob_packet->getChannelId();
      int64_t receive_time = ob_packet->get_receive_ts();
      int64_t network_timeout = ob_packet->get_source_timeout();
      
      if (OB_GET_REQUEST == packet_code || OB_SCAN_REQUEST == packet_code)
      {
        FILL_TRACE_LOG("request from peer=%s, wait_time_in_queue=%ld, packet_code=%d",
          tbsys::CNetUtil::addrToString(ob_packet->get_connection()->getPeerId()).c_str(), 
          tbsys::CTimeUtil::getTime() - receive_time, packet_code);
      }

      ret = ob_packet->deserialize();
      if (OB_SUCCESS == ret) 
      {
        int64_t timeout_time = get_process_timeout_time(receive_time, network_timeout);
        ObDataBuffer* in_buffer = ob_packet->get_buffer(); 
        if (NULL == in_buffer)
        {
          TBSYS_LOG(ERROR, "in_buffer is NUll should not reach this");
        }
        else
        {
          tbnet::Connection* connection = ob_packet->get_connection();
          ThreadSpecificBuffer::Buffer* thread_buffer = 
            response_buffer_.get_buffer();
          if (NULL != thread_buffer)
          {
            thread_buffer->reset();
            ObDataBuffer out_buffer(thread_buffer->current(), thread_buffer->remain());
            //TODO read thread stuff multi thread 
            TBSYS_LOG(DEBUG, "handle packet, packe code is %d, packet:%p", 
                packet_code, ob_packet);
            ret = service_.do_request(receive_time, packet_code, 
                version, channel_id, connection, *in_buffer, out_buffer, timeout_time);
          }
          else
          {
            TBSYS_LOG(ERROR, "get thread buffer error, ignore this packet");
          }
        }
      }

      return ret;
    }
  } // end namespace chunkserver
} // end namespace oceanbase

