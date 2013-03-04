#include "tbnet.h"
#include "common/ob_define.h"
#include "common/ob_packet.h"
#include "common/ob_result.h"
#include "common/ob_read_common_data.h"
#include "ob_merge_server_main.h"
#include "ob_ms_request_event.h"
#include "ob_ms_rpc_event.h"
#include "ob_ms_async_rpc.h"
#include "ob_ms_counter_infos.h"

using namespace tbnet;
using namespace oceanbase::common;
using namespace oceanbase::mergeserver;

ObMergerRpcEvent::ObMergerRpcEvent()
{
  client_request_id_ = OB_INVALID_ID;
  client_request_ = NULL;
  timeout_us_ = 0;
}

ObMergerRpcEvent::~ObMergerRpcEvent()
{
  reset();
}

void ObMergerRpcEvent::reset(void)
{
  // print debug info
  ObCommonRpcEvent::reset();
  client_request_id_ = OB_INVALID_ID;
  client_request_ = NULL;
}

uint64_t ObMergerRpcEvent::get_client_id(void) const
{
  return client_request_id_;
}

const ObMergerRequestEvent * ObMergerRpcEvent::get_client_request(void) const
{
  return client_request_;
}

int ObMergerRpcEvent::init(const uint64_t client_id, ObMergerRequestEvent * request)
{
  int ret = OB_SUCCESS;
  if ((OB_INVALID_ID == client_id) || (NULL == request))
  {
    ret = OB_INPUT_PARAM_ERROR;
    TBSYS_LOG(WARN, "check input failed:client[%lu], event[%lu], request[%p]",
        client_id, get_event_id(), request);
  }
  else
  {
    client_request_id_ = client_id;
    client_request_ = request;
    TBSYS_LOG(DEBUG, "init rpc event succ:client[%lu], event[%lu], request[%p]",
        client_id, get_event_id(), request);
  }
  return ret;
}

int ObMergerRpcEvent::parse_packet(tbnet::Packet * packet, void * args)
{
  int ret = OB_SUCCESS;
  if (NULL == packet)
  {
    ret = OB_INPUT_PARAM_ERROR;
    TBSYS_LOG(WARN, "check packet is NULL:client[%lu], request[%lu], event[%lu]",
        client_request_id_, client_request_->get_request_id(), get_event_id()); 
  }
  else if (packet->isRegularPacket())
  {
    ret = deserialize_packet(*dynamic_cast<ObPacket *>(packet), ObCommonRpcEvent::get_result());
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "deserialize packet failed:client[%lu], request[%lu], "
          "event[%lu], ret[%d]", client_request_id_, client_request_->get_request_id(),
          ObCommonRpcEvent::get_event_id(), ret);
    }
  }
  else// if (NULL != args)
  {
    UNUSED(args);
    tbnet::ControlPacket *ctrl_packet = static_cast<tbnet::ControlPacket*>(packet);
    if (NULL == ctrl_packet)
    {
      ret = OB_ERROR;
      TBSYS_LOG(ERROR, "not regular packet discard anyway:client[%lu], request[%lu], "
          "event[%lu], code[%d]", client_request_id_, client_request_->get_request_id(),
          ObCommonRpcEvent::get_event_id(), packet->getPCode());
    }
    else if (tbnet::ControlPacket::CMD_TIMEOUT_PACKET == ctrl_packet->getCommand())
    {
      ret = OB_RESPONSE_TIME_OUT;
      TBSYS_LOG(WARN, "timeout packet:client[%lu], request[%lu], event[%lu], command[%d]",
          client_request_id_, client_request_->get_request_id(), 
          ObCommonRpcEvent::get_event_id(), ctrl_packet->getCommand());
    }
    else
    {
      ret = OB_CONN_ERROR;
      TBSYS_LOG(WARN, "bad or disconnect packet:client[%lu], request[%lu], event[%lu], "
          "command[%d]", client_request_id_, client_request_->get_request_id(),
          ObCommonRpcEvent::get_event_id(), ctrl_packet->getCommand());
    }
  }
  return ret;
}

tbnet::IPacketHandler::HPRetCode ObMergerRpcEvent::handlePacket(tbnet::Packet * packet, void * args)
{
  int ret = OB_SUCCESS;
  if (false == check_inner_stat())
  {
    ret = OB_ERROR;
    TBSYS_LOG(WARN, "check inner stat failed");
  }
  else if (ObMergeServerMain::get_instance()->get_merge_server().is_stoped())
  {
    TBSYS_LOG(WARN, "server stoped, cannot handle anything.");
    ret = OB_ERROR;
  }
  else
  {
    this->end();
    switch (get_req_type())
    {
      case ObMergerRpcEvent::GET_RPC:
        ms_get_counter_set().inc(ObMergerCounterIds::C_CS_GET, get_time_used());
        break;
      case ObMergerRpcEvent::SCAN_RPC:
        ms_get_counter_set().inc(ObMergerCounterIds::C_CS_SCAN, get_time_used());
        break;
      default:
        TBSYS_LOG(ERROR, "unknown rpc type [event:%p,client:%lu, request:%lu, "
            "event_id:%lu,req_type:%d]", this, client_request_id_, 
            client_request_->get_request_id(), get_event_id(), get_req_type());
    }
    /// parse the packet for get result code and result scanner
    ret = parse_packet(packet, args);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "parse the packet failed:client[%lu], request[%lu], event[%lu], ptr[%p]",
          client_request_id_, client_request_->get_request_id(), get_event_id(), this);
      /// set result code, maybe timeout packet, connection errors.
      ObCommonRpcEvent::set_result_code(ret);
    }

    char ip_addr[ObMergerAsyncRpcStub::MAX_SERVER_LEN];
    get_server().to_string(ip_addr,sizeof(ip_addr));
    ObPacket* obpacket = dynamic_cast<ObPacket*>(packet);
    if (NULL != obpacket)
    {
      TBSYS_LOG(DEBUG, "handle packet eventid[%lu], time_used[%ld], server[%s], "
          "result code=%d, packet code=%d, session_id=%ld", 
          get_event_id(), get_time_used(), ip_addr, get_result_code(), 
          obpacket->get_packet_code(), obpacket->get_session_id());
    }

    if (client_request_ != NULL)
    {
      /// no matter parse succ or failed push to finish queue
      /// not check the event valid only push to the finish queue
      ret = client_request_->signal(*this);
    }
  }
  return tbnet::IPacketHandler::FREE_CHANNEL;
}

int ObMergerRpcEvent::deserialize_packet(ObPacket & packet, ObScanner & result)
{
  int64_t data_length = packet.get_data_length();
  ObDataBuffer * data_buff = NULL;
  int ret = packet.deserialize();
  if (ret != OB_SUCCESS)
  {
    TBSYS_LOG(WARN, "deserialize the packet failed:ret[%d]", ret);
  }
  else
  {
    data_buff = packet.get_buffer();
    if (NULL == data_buff)
    {
      ret = OB_INNER_STAT_ERROR;
      TBSYS_LOG(WARN, "check packet data buff failed:buff[%p]", data_buff);
    }
    else
    {
      // packet header
      data_length += data_buff->get_position();
    }
    if (packet.get_packet_code() == OB_SESSION_END)
    {
      /// when session end, set session id to 0
      set_session_end();
    }
    else
    {
      set_session_id(packet.get_session_id());
    }
  }
  ObResultCode code;
  if (OB_SUCCESS == ret)
  {
    ret = code.deserialize(data_buff->get_data(), data_length, data_buff->get_position()); 
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "deserialize result failed:pos[%ld], ret[%d]",
          data_buff->get_position(), ret);
    }
    else
    {
      ObCommonRpcEvent::set_result_code(code.result_code_);
    }
  }
  result.reset();
  if ((OB_SUCCESS == ret) && (OB_SUCCESS == code.result_code_))
  {
    ret = result.deserialize(data_buff->get_data(), data_length, data_buff->get_position());
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "deserialize scanner failed:pos[%ld], ret[%d]",
          data_buff->get_position(), ret);
    }
  }
  return ret;
}

void ObMergerRpcEvent::print_info(FILE * file) const
{
  if (NULL != file)
  {
    ObCommonRpcEvent::print_info(file);
    if (NULL == client_request_)
    {
      fprintf(file, "merger rpc event::clinet[%lu], request[%p]\n",
          client_request_id_, client_request_);
    }
    else
    {
      fprintf(file, "merger rpc event:client[%lu], request[%lu], ptr[%p]\n", 
          client_request_id_, client_request_->get_request_id(), client_request_);
    }
    fflush(file);
  }
}

