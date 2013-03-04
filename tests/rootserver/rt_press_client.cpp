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
#include "common/ob_get_param.h"
#include "common/ob_scan_param.h"
#include "common/ob_server.h"
#include "common/ob_string.h"
#include "common/ob_scanner.h"
#include "common/ob_result.h"
using namespace oceanbase;
using namespace oceanbase::common;

ObClientManager client;
ObServer root_server;
int scan_root(ObString& row_key, ObScanner & scanner, ObDataBuffer& data_buff)
{
  ObCellInfo cell;
  cell.table_id_ = 1001;
  cell.column_id_ = 0;
  cell.row_key_ = row_key;
  data_buff.get_position() = 0;

  ObGetParam get_param;
  int ret = get_param.add_cell(cell);
  if (ret != OB_SUCCESS)
  {
    TBSYS_LOG(ERROR, "add cell to get param failed:ret[%d]", ret);
  }
  else
  {
    ret = get_param.serialize(data_buff.get_data(), data_buff.get_capacity(),
        data_buff.get_position());
    if (OB_SUCCESS == ret)
    {
      ret = client.send_request(root_server, OB_GET_REQUEST, 1,
          100000, data_buff);
    }
    int64_t pos = 0;

    ObResultCode result_code;
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
      scanner.clear();
      ret = scanner.deserialize(data_buff.get_data(), data_buff.get_position(), pos);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "deserialize scanner from buff failed:pos[%ld], ret[%d]", pos, ret);
      }
    }

    if (ret != OB_SUCCESS)
    {   
      TBSYS_LOG(ERROR, "scan root server for get chunk server location failed:");
    }
    else
    {
      //TBSYS_LOG(DEBUG, "scan root server for get chunk server location succ:");
    }
  }
  return ret;
}
int main(int argc, char** argv)
{
  if (argc != 3)
  {
    printf("%s root_ip root_port\n",argv[0]);
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

  ObString row_key;
  char buff[50];
  ObScanner scanner;
  int ret;
  row_key.assign_buffer(buff, 50);
  row_key.write("00",2);
  uint64_t count = 0;
  while(1)
  {
    while (OB_SUCCESS != scan_root(row_key, scanner, data_buff))
    {
      sleep(1);
    }
    count++;
    if (count >= 1000) 
    {
      TBSYS_LOG_US(INFO, "finish %ld scan", count);
      count = 0;
    }
    ObScannerIterator iter = scanner.begin();
    ObCellInfo * cell = NULL;
    bool row_change;
    while (iter != scanner.end())
    {
      row_key.assign_buffer(buff, 50);
      ret = iter.get_cell(&cell, &row_change);
      row_key.write(cell->row_key_.ptr(), cell->row_key_.length());
      if (buff[0] == -1) break;

      if ((cell->column_name_.compare("1_port") == 0)
          || (cell->column_name_.compare("2_port") == 0)
          || (cell->column_name_.compare("3_port") == 0))
      {
        int64_t port = 0;
        ret= cell->value_.get_int(port);
        bool is_ok = false;
        for (int j = 0; j < 3; j++)
        {
          char tmp[4];
          tmp[0] = buff[10 + j*3 + 0];
          tmp[1] = buff[10 + j*3 + 1];
          tmp[2] = buff[10 + j*3 + 2];
          tmp[3] = 0;
          //printf("%d %ld %d\n", atoi(tmp), port, port == atoi(tmp));
          if (atoi(tmp) == port - port/100 * 100) 
          {
            //printf("is ok\n");
            is_ok = true;
          }
        }
        if (!is_ok)
        {
          TBSYS_LOG(ERROR,"some thing wrong\n");
          TBSYS_LOG(ERROR,"rowkey = %.19s port =%ld\n", buff, port);
        }
      }
      iter++;
    }
    if (buff[0] == -1) 
    {
      row_key.assign_buffer(buff, 50);
      row_key.write("00",2);
      sleep(1);
    }
  }

  return 0;
}
