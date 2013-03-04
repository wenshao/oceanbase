#include "mock_update_server.h"
#include "common/ob_result.h"
#include "common/ob_define.h"
#include "common/ob_scanner.h"
#include "common/ob_tablet_info.h"

using namespace oceanbase::common;
using namespace oceanbase::rootserver;

int MockUpdateServer::initialize()
{
  bool res = true;
  tbsys::CConfig config;
  if (res) 
  {
    if (EXIT_FAILURE == config.load("./mock.conf"))
    {
      TBSYS_LOG(ERROR, "load config file ./mock.conf error");
      res = false;
    }
  }

  int port  = 3000;
  const char * vip = NULL;
  int rport = 2000;
  if (res)
  {
    port = config.getInt("update", "port", 3000);
  }

  if (res)
  {
    vip = config.getString("root", "vip", NULL);
    rport = config.getInt("root", "port", 2000);
  }
  const char* dev_name = NULL;
  dev_name = config.getString("update", "dev_name", NULL);
  set_listen_port(port);
  set_self(dev_name, port);
  root_server_.set_ipv4_addr(vip, rport);
  return MockServer::initialize();
}

int MockUpdateServer::do_request(ObPacket* base_packet)
{
  int ret = OB_SUCCESS;
  ObPacket* ob_packet = base_packet;
  int32_t packet_code = ob_packet->get_packet_code();
  ret = ob_packet->deserialize();
  if (OB_SUCCESS == ret)
  {
    switch (packet_code)
    {
    case OB_FREEZE_MEM_TABLE:
      {
        ret = handle_mock_freeze(ob_packet);
        break;
      }
    case OB_DROP_OLD_TABLETS:
      {
        ret = handle_drop_tablets(ob_packet);
        break;
      }
    default:
      {
        ret = OB_ERROR;
        TBSYS_LOG(ERROR, "wrong packet code");
        break;
      }
    }
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "check handle failed:ret[%d]", ret);
    }
  }
  return ret;
}


int MockUpdateServer::handle_mock_freeze(ObPacket *ob_packet)
{
  static int version = 1;
  TBSYS_LOG(INFO, "get freeze comman");
  int ret = OB_SUCCESS;
  ObDataBuffer* data = ob_packet->get_buffer();
  int32_t channel_id = ob_packet->getChannelId();
  tbnet::Connection* connection = ob_packet->get_connection();
  if (NULL == data)
  {
    ret = OB_ERROR;
  }
  ThreadSpecificBuffer::Buffer* thread_buffer = response_packet_buffer_.get_buffer();
  if (NULL == thread_buffer)
  {
    ret = OB_ERROR;
  }
  else
  {
    thread_buffer->reset();
    ObDataBuffer out_buffer(thread_buffer->current(), thread_buffer->remain());

    ObResultCode result_msg;
    result_msg.result_code_ = ret;
    ret = result_msg.serialize(out_buffer.get_data(), out_buffer.get_capacity(), out_buffer.get_position());

    if (OB_SUCCESS == ret)
    {
      version++;
      ret = common::serialization::encode_vi64(out_buffer.get_data(), out_buffer.get_capacity(), out_buffer.get_position(), version);
    }
    if (OB_SUCCESS == ret)
    {
      ret = send_response(OB_FREEZE_MEM_TABLE_RESPONSE, 1, out_buffer, connection, channel_id);
    }
    sleep(1);
    {    
      thread_buffer->reset();
      ObDataBuffer thread_buff(thread_buffer->current(),thread_buffer->remain());
      ObServer server = self_;
      ret = server.serialize(thread_buff.get_data(), 
          thread_buff.get_capacity(), thread_buff.get_position());
      ret = common::serialization::encode_vi64(thread_buff.get_data(), thread_buff.get_capacity(), thread_buff.get_position(), version);
      if (OB_SUCCESS == ret) 
      {    
        ret = client_manager_.send_request(root_server_, OB_WAITING_JOB_DONE, 1, 50000, thread_buff);
      }    
      ObDataBuffer out_buffer(thread_buff.get_data(), thread_buff.get_position());
      if (ret == OB_SUCCESS) 
      {    
        common::ObResultCode result_msg;
        ret = result_msg.deserialize(out_buffer.get_data(), out_buffer.get_capacity(), out_buffer.get_position());
        if (ret == OB_SUCCESS)
        {    
          ret = result_msg.result_code_;
          if (ret != OB_SUCCESS) 
          {    
            TBSYS_LOG(INFO, "rpc return error code is %d msg is %s", result_msg.result_code_, result_msg.message_.ptr());
          }    
        }    
      }    
      thread_buffer->reset();
    }
  }
  TBSYS_LOG(INFO, "handle freeze result:ret[%d]", ret);
  return ret;
}
int MockUpdateServer::handle_drop_tablets(ObPacket *ob_packet)
{
  int ret = OB_SUCCESS;
  ObDataBuffer* data = ob_packet->get_buffer();
  int32_t channel_id = ob_packet->getChannelId();
  tbnet::Connection* connection = ob_packet->get_connection();
  if (NULL == data)
  {
    ret = OB_ERROR;
  }
  ThreadSpecificBuffer::Buffer* thread_buffer = response_packet_buffer_.get_buffer();
  if (NULL == thread_buffer)
  {
    ret = OB_ERROR;
  }
  else
  {
    thread_buffer->reset();
    ObDataBuffer out_buffer(thread_buffer->current(), thread_buffer->remain());

    ObResultCode result_msg;
    result_msg.result_code_ = ret;
    ret = result_msg.serialize(out_buffer.get_data(), out_buffer.get_capacity(), out_buffer.get_position());

    if (OB_SUCCESS == ret)
    {
      ret = send_response(OB_DROP_OLD_TABLETS_RESPONSE, 1, out_buffer, connection, channel_id);
    }
  }
  TBSYS_LOG(INFO, "handle drop_tablets result:ret[%d]", ret);
  return ret;
}







