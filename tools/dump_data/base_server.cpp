#include "base_server.h"

using namespace oceanbase::common;
using namespace oceanbase::tools;

int BaseServer::initialize()
{
  int ret = set_packet_factory(&factory_);
  if (OB_SUCCESS == ret)
  {
    ret = client_manager_.initialize(get_transport(), get_packet_streamer());
    if (OB_SUCCESS == ret)
    {
      ret = ObSingleServer::initialize();
      if (ret != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR, "check single server init failed:ret[%d]", ret);
      }
    }
    else
    {
      TBSYS_LOG(ERROR, "check client manager init failed:ret[%d]", ret);
    }
  }
  return ret;
}

