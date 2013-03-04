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
#include "common/ob_crc64.h"
#include "common/utility.h"
#include "rootserver/ob_chunk_server_manager.h"
using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::rootserver;

ObClientManager client;
ObServer root_server;
int main(int argc, char** argv)
{
  if (argc != 3)
  {
    printf("%s root_ip root_port \n",argv[0]);
    return 0;
  }
  ob_init_memory_pool();
  ob_init_crc64_table(OB_DEFAULT_CRC64_POLYNOM);
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
    ret = client.send_request(root_server, OB_DUMP_CS_INFO, 1,
        100000, data_buff);
  }
  int64_t pos = 0;

  ObResultCode result_code;
  ObChunkServerManager obcsm;
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
    ret = obcsm.deserialize(data_buff.get_data(), data_buff.get_position(), pos);
  }
  if (OB_SUCCESS == ret)
  {
    int32_t index = 0;
    ObChunkServerManager::const_iterator it = obcsm.begin();
    for (; it != obcsm.end(); ++it)
    {
      it->dump(index++);
    }
  }
  else
  {
    TBSYS_LOG(INFO, "read err %d", ret);
  }
  return 0;
}
