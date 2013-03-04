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
#include "common/ob_crc64.h"
using namespace oceanbase;
using namespace oceanbase::common;

ObClientManager client;
ObServer root_server;
int BUFF_SIZE = 150;
int scan_root(ObString& row_key, ObScanner & scanner, ObDataBuffer& data_buff, int table_id)
{
  ObCellInfo cell;
  cell.table_id_ = table_id;
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
        TBSYS_LOG(ERROR, "deserialize scanner from buff failed:pos[%lld], ret[%d]", pos, ret);
      }
    }

    if (ret != OB_SUCCESS)
    {   
      TBSYS_LOG(ERROR, "scan root server for get chunk server location failed:%d", ret);
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
  int table_id = 0;
  if (argc != 4)
  {
    printf("%s root_ip root_port table_id \n",argv[0]);
    return 0;
  }
  TBSYS_LOGGER.setLogLevel("info");
  table_id = atoi(argv[3]);
  int max_row_key_length = 50; 
  if (max_row_key_length > 50) 
  {
    printf("for now we only can deal with max_row_key_length < 50 ");
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

  ObString row_key;
  char buff[BUFF_SIZE];
  char end_key[BUFF_SIZE];
  char start_str[BUFF_SIZE * 2];
  char end_str[BUFF_SIZE * 2];
  memset(end_key, -1, BUFF_SIZE);
  memset(start_str, 0, BUFF_SIZE);
  memset(end_str, 0, BUFF_SIZE);
  ObScanner scanner;
  sprintf(buff, "%c", 0);
  row_key.assign(buff, 1);
  uint64_t count = 0;
  ObCellInfo * cell = NULL;
  while(1)
  {
    int ret = 0;
    while (OB_SUCCESS != (ret = scan_root(row_key, scanner, data_buff, table_id)))
    {
      if (OB_ERROR_OUT_OF_RANGE == ret)
      {
        return 0;
      }
      sleep(1);
    }
    ObScannerIterator iter = scanner.begin();
    bool row_change;
    bool first = true;
    bool skip_this = true;
    while (iter != scanner.end())
    {
      ret = iter.get_cell(&cell, &row_change);
      if (row_change) 
      {
        count++;
        if (first) 
        {
          first = false;
          //skip the first line. 
          skip_this = true;
        }
        else
        {
          skip_this = false;
        }
        if (!skip_this) {
          memcpy(start_str, end_str, BUFF_SIZE*2);
          hex_to_str(cell->row_key_.ptr(), cell->row_key_.length(), end_str, BUFF_SIZE*2);
          end_str[cell->row_key_.length() *2 +1] = 0;
          TBSYS_LOG(INFO, "(%s,%s]", start_str, end_str);
          //common::hex_dump(cell->row_key_.ptr(), cell->row_key_.length(),true, TBSYS_LOG_LEVEL_INFO);
        }
        if (cell->row_key_.length() >= BUFF_SIZE) 
        {
          TBSYS_LOG(ERROR, "rowkey len too large %d", cell->row_key_.length());
          exit(0);
        }
        row_key.assign_buffer(buff, BUFF_SIZE);
        row_key.write(cell->row_key_.ptr(), cell->row_key_.length());
        if (cell->row_key_.length() > 0)
          buff[cell->row_key_.length() -1]++;
      }
      if (!skip_this)
      {
        int64_t value =0;
        cell->value_.get_int(value);
        if (*(cell->column_name_.ptr() + cell->column_name_.length() -1) == '4')
        {
          TBSYS_LOG(INFO, "column:%.*s value:%d.%d.%d.%d", cell->column_name_.length(), cell->column_name_.ptr(), 
              value & 0xFF, (value >> 8) & 0xFF, (value >> 16) & 0xFF, (value >> 24) & 0xFF);
        }
        else
        {
        TBSYS_LOG(INFO, "column:%.*s value:%ld", cell->column_name_.length(), cell->column_name_.ptr(), value);
        }
      }
      //TBSYS_LOG(INFO, "row key length %d\n", row_key.length());
      iter++;
    }
      if (cell->row_key_.length() > 0 && memcmp(cell->row_key_.ptr(), end_key, cell->row_key_.length()) == 0 ) break;
  }


  return 0;
}
