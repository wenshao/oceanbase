/*===============================================================
*   (C) 2007-2010 Taobao Inc.
*   
*   
*   Version: 0.1 2010-11-18
*   
*   Authors:
*          daoan(daoan@taobao.com)
*   
*
================================================================*/
#include "common/ob_packet_factory.h"
#include "common/ob_client_manager.h"
#include "common/ob_server.h"
#include "common/ob_string.h"
#include "common/ob_scanner.h"
#include "common/ob_result.h"
#include "common/utility.h"
#include "common/ob_malloc.h"
#include "rootserver/ob_root_stat.h"
using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::rootserver;

ObClientManager client;
ObServer root_server;
const char *str[4] = {"ok_get","ok_scan","err_get","err_scan"};
int main(int argc, char** argv)
{
  if (argc != 3)
  {
    printf("%s root_ip root_port \n",argv[0]);
    return 0;
  }
  ob_init_memory_pool();
  tbnet::Transport transport_;
  common::ObPacketFactory packet_factory_;
  tbnet::DefaultPacketStreamer streamer_;
  streamer_.setPacketFactory(&packet_factory_);
  transport_.start();
  client.initialize(&transport_, &streamer_);
  root_server.set_ipv4_addr(argv[1], atoi(argv[2]));
  char* p_data = (char*)malloc(1024 * 1024 *2);
  ObDataBuffer data_buff(p_data, 1024 * 1024 *2);
  int ret = OB_SUCCESS;
  if (OB_SUCCESS == ret)
  {
    ret = client.send_request(root_server, OB_FETCH_STATS, 1,
        100000, data_buff);
  }
  int64_t pos = 0;

  ObResultCode result_code;
  ObStatManager stat(0);
  if (OB_SUCCESS == ret) {
    ret = result_code.deserialize(data_buff.get_data(), data_buff.get_position(), pos);
  }
  if (OB_SUCCESS != ret)
  {
    TBSYS_LOG(ERROR, "deserialize result failed:pos[%ld], ret[%d]", pos, ret);
  }
  else
  {
    ret = result_code.result_code_;
  }
  if (OB_SUCCESS == ret)
  {
    ret = stat.deserialize(data_buff.get_data(), data_buff.get_position(), pos);
  }
  if (OB_SUCCESS == ret)
  {
    ObStatManager::const_iterator it = stat.begin();
    for (; it != stat.end(); ++it)
    {
      for (int i = 0; i < 4; i++)
      {
        TBSYS_LOG(INFO, "%s %ld", str[i], it->get_value(i));
      }
    }
  }
  else
  {
    TBSYS_LOG(INFO, "read err %d", ret);
  }
  return 0;
}
