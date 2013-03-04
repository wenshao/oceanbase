
#include "common/ob_role_mgr.h"
#include "common/ob_slave_mgr.h"
#include "common/ob_packet_factory.h"

using namespace oceanbase::common;

class SlaveMgr4Test
{
public:
  SlaveMgr4Test()
  {
    streamer.setPacketFactory(&factory);
    client_mgr.initialize(&transport, &streamer);
    rpc_stub.init(&client_mgr);
    slave_mgr_.init(&role_mgr_, 1, &rpc_stub, 1000000, 15000000, 12000000);
    transport.start();
  }

  ~SlaveMgr4Test()
  {
    transport.stop();
    transport.wait();
  }

  ObSlaveMgr* get_slave_mgr()
  {
    return &slave_mgr_;
  }

private:
  ObSlaveMgr slave_mgr_;
  ObRoleMgr role_mgr_;

  ObClientManager client_mgr;
  ObCommonRpcStub rpc_stub;
  ObPacketFactory factory;
  tbnet::Transport transport;
  tbnet::DefaultPacketStreamer streamer;
};
