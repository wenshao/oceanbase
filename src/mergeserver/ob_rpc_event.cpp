#include "tblog.h"
#include "common/ob_atomic.h"
#include "common/ob_scanner.h"
#include "ob_rpc_event.h"

using namespace oceanbase::common;
using namespace oceanbase::mergeserver;

uint64_t ObCommonRpcEvent::id_allocator_ = 0;

ObCommonRpcEvent::ObCommonRpcEvent()
{
  reset();
  event_id_ = atomic_inc(reinterpret_cast<volatile uint64_t*>(&id_allocator_));
}

ObCommonRpcEvent::~ObCommonRpcEvent()
{
  reset();
}

void ObCommonRpcEvent::reset(void)
{
  result_.reset();
  event_id_ = OB_INVALID_ID;
  result_code_ = OB_INVALID_ERROR;
  session_id_ = INVALID_SESSION_ID;
}

uint64_t ObCommonRpcEvent::get_event_id(void) const
{
  return event_id_;
}

void ObCommonRpcEvent::set_server(const ObServer & server)
{
  server_ = server;
}

const ObServer & ObCommonRpcEvent::get_server(void) const
{
  return server_;
}

int32_t ObCommonRpcEvent::get_result_code(void) const
{
  return result_code_;
}

void ObCommonRpcEvent::set_result_code(const int32_t code)
{
  result_code_ = code;
}

ObScanner & ObCommonRpcEvent::get_result(void)
{
  return result_;
}

ObScanner & ObCommonRpcEvent::get_result(int32_t & result_code)
{
  result_code = result_code_;
  return result_;
}

tbnet::IPacketHandler::HPRetCode ObCommonRpcEvent::handlePacket(tbnet::Packet * packet, void *args)
{
  UNUSED(packet);
  UNUSED(args);
  TBSYS_LOG(ERROR, "unexpected path not implenmented in common rpc event");
  return tbnet::IPacketHandler::FREE_CHANNEL;
}

void ObCommonRpcEvent::print_info(FILE * file) const
{
  if (NULL != file)
  {
    const static uint32_t MAX_SERVER_LEN = 128; 
    char server_addr[MAX_SERVER_LEN] = "";
    server_.to_string(server_addr, sizeof(server_addr));
    fprintf(file, "common rpc event:allocator[%lu]\n", ObCommonRpcEvent::id_allocator_);
    fprintf(file, "common rpc event:event[%lu]\n", event_id_);
    fprintf(file, "common rpc event:server[%s]\n", server_addr);
    fprintf(file, "common rpc event:code[%d]\n", result_code_);
    fprintf(file, "common rpc event:size[%ld]\n", result_.get_size());
    fprintf(file, "common rpc event:version[%ld]\n", result_.get_data_version());
    fflush(file);
  }
}

void ObCommonRpcEvent::start()
{
  start_time_us_ = tbsys::CTimeUtil::getTime();
}

void ObCommonRpcEvent::end()
{
  end_time_us_ = tbsys::CTimeUtil::getTime();
}

int64_t ObCommonRpcEvent::get_time_used()const
{
  return end_time_us_ - start_time_us_;
}

