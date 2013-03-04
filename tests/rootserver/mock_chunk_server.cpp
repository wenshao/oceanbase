#include "mock_chunk_server.h"
#include "common/ob_result.h"
#include "common/ob_define.h"
#include "common/ob_scanner.h"
#include "common/ob_tablet_info.h"

using namespace oceanbase::common;
using namespace oceanbase::rootserver;
  MockChunkServer::MockChunkServer()
:control_thread_(this)
{
}
MockChunkServer::~MockChunkServer()
{
  control_thread_.stop();
  control_thread_.wait();
}
int MockChunkServer::set_args(int total, int number)
{
  total_ = total;
  number_ = number;
  return 0;
}
int MockChunkServer::regist_self()
{    
  self_.reset_ipv4_10(number_+1);
  ThreadSpecificBuffer::Buffer* thread_buffer = response_packet_buffer_.get_buffer();
  thread_buffer->reset();
  ObDataBuffer thread_buff(thread_buffer->current(),thread_buffer->remain());
  ObServer server = self_;
  int ret = server.serialize(thread_buff.get_data(), 
      thread_buff.get_capacity(), thread_buff.get_position());
  ret = common::serialization::encode_bool(thread_buff.get_data(), thread_buff.get_capacity(), thread_buff.get_position(), false);
  char str[60];
  root_server_.to_string(str,60);
  if (OB_SUCCESS == ret) 
  {    
    do {
      TBSYS_LOG(INFO, "root server is %s", str);
      ret = client_manager_.send_request(root_server_, OB_SERVER_REGISTER, 1, 50000, thread_buff);
      if (OB_SUCCESS != ret) sleep(1);
    }while (OB_SUCCESS != ret);
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
  return 0;
}
int MockChunkServer::initialize()
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
    port = config.getInt("chunk", "port", 3000);
  }
  port += number_;

  if (res)
  {
    vip = config.getString("root", "vip", NULL);
    rport = config.getInt("root", "port", 2000);
  }
  const char* dev_name = NULL;
  dev_name = config.getString("chunk", "dev_name", NULL);
  seed_ = config.getInt("chunk", "seed", 19885);
  root_table_size_ = config.getInt("chunk", "root_table_size", 1000);

  set_listen_port(port);
  set_self(dev_name, port);
  TBSYS_LOG(INFO, "root server vip =%s port = %d",vip, rport);
  root_server_.set_ipv4_addr(vip, rport);
  control_thread_.start();
  return MockServer::initialize();

}

int MockChunkServer::do_request(ObPacket* base_packet)
{
  int ret = OB_SUCCESS;
  ObPacket* ob_packet = base_packet;
  int32_t packet_code = ob_packet->get_packet_code();
  ret = ob_packet->deserialize();
  if (OB_SUCCESS == ret)
  {
    switch (packet_code)
    {
      case OB_REQUIRE_HEARTBEAT:
        ret = handle_hb(ob_packet);
        break;
      case OB_START_MERGE:
        {
        tbsys::CThreadGuard guard(&mutex_);
        ret = handle_start_merge(ob_packet);
        report_tablets();
        }
        break;
      case OB_DROP_OLD_TABLETS:
        ret = handle_drop(ob_packet);
        break;
      default:
        ret = OB_ERROR;
        TBSYS_LOG(ERROR, "wrong packet code");
        break;
    }
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "check handle failed:ret[%d]", ret);
    }
  }
  return ret;
}
  MockChunkServer::controlThread::controlThread(MockChunkServer* server)
:server_(server)
{
}
void MockChunkServer::controlThread::run(tbsys::CThread *thread, void *arg)
{
  UNUSED(thread);
  UNUSED(arg);
  sleep(1);
  server_->regist_self();
  server_->version_ = 1;
  server_->generate_root_table();
  //////////////////
  //server_->version_ = 1;
  //server_->split_table();
  //server_->version_ = 2;
  //server_->split_table();
  //server_->version_ = 3;
  //server_->split_table();
  /////////////////////////////
  server_->report_tablets();
  while (!_stop)
  {
    sleep(1);
  }
}


int MockChunkServer::handle_hb(ObPacket *ob_packet)
{
  int ret = OB_SUCCESS;
  ObDataBuffer* data = ob_packet->get_buffer();
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
    ObServer client_server = self_;
    ret = client_server.serialize(out_buffer.get_data(), out_buffer.get_capacity(), out_buffer.get_position());
    ret = client_manager_.post_request(root_server_, OB_HEARTBEAT, 1 , out_buffer);
  }

  TBSYS_LOG_US(INFO, "handle hb result:ret[%d]", ret);
  return ret;
}
int MockChunkServer::handle_drop(ObPacket *ob_packet)
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
      ret = send_response(OB_HEARTBEAT_RESPONSE, 1, out_buffer, connection, channel_id);
    }
  }
  TBSYS_LOG(INFO, "handle drop result:ret[%d]", ret);
  return ret;
}
int MockChunkServer::handle_start_merge(ObPacket *ob_packet)
{
  int ret = OB_SUCCESS;
  ObDataBuffer* data = ob_packet->get_buffer();
  int32_t channel_id = ob_packet->getChannelId();
  tbnet::Connection* connection = ob_packet->get_connection();
  int64_t tmp_version = 0;
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
    ret = common::serialization::decode_vi64(data->get_data(), data->get_capacity(), data->get_position(), &tmp_version);
    thread_buffer->reset();
    ObDataBuffer out_buffer(thread_buffer->current(), thread_buffer->remain());

    ObResultCode result_msg;
    result_msg.result_code_ = ret;
    ret = result_msg.serialize(out_buffer.get_data(), out_buffer.get_capacity(), out_buffer.get_position());

    if (OB_SUCCESS == ret)
    {
      ret = send_response(OB_HEARTBEAT_RESPONSE, 1, out_buffer, connection, channel_id);
    }
  }
  TBSYS_LOG(INFO, "handle start merge result:ret[%d]", ret);
  if (OB_SUCCESS == ret && tmp_version > version_) 
  {
    version_ = tmp_version;
    split_table();
  }
  return ret;
}
int MockChunkServer::split_table()
{
  TBSYS_LOG(INFO, "----- version %ld ----", version_);
  srandom(static_cast<int32_t>(version_));
  map<string,int>::iterator it = root_table_.begin();
  map<string, int> splited_table;
  char prev_key[11];
  char curr_key[11];
  char split_key[20];
  memcpy(prev_key, it->first.c_str(),10);
  prev_key[10]=0;
  it++;
  for (; it != root_table_.end(); it++)
  {
    if (random() % 100 < 2)
    {
      memcpy(curr_key, it->first.c_str(),10);
      curr_key[10]=0;
      int prev = atoi(prev_key);
      int curr = atoi(curr_key);
      int step = (curr - prev) / 4;
      if (step <= 0) continue;
      int new_key1 = prev + step;
      int new_key2 = new_key1 + step;
      int new_key3 = new_key2 + step;
      //TBSYS_LOG(INFO, "pre =%d cur=%d new_key1=%d it=%.19s",prev, curr, new_key1, it->first.c_str());
      sprintf(split_key, "%010d%.9s", new_key1, it->first.c_str()+10);
      splited_table[split_key] = it->second;
      sprintf(split_key, "%010d%.9s", new_key2, it->first.c_str()+10);
      splited_table[split_key] = it->second;
      sprintf(split_key, "%010d%.9s", new_key3, it->first.c_str()+10);
      splited_table[split_key] = it->second;
    }
    memcpy(prev_key, it->first.c_str(),10);
    prev_key[10]=0;
  }
  for (it = splited_table.begin(); it != splited_table.end(); it++)
  {
    root_table_[it->first]=it->second;
  }
  TBSYS_LOG(INFO, "-----------------------------\n");
  for (map<string,int>::iterator it = root_table_.begin(); it != root_table_.end(); it++)
  {
    TBSYS_LOG(INFO,"%s %d\n", it->first.c_str(), it->second);
  }
  return 0;
}
int MockChunkServer::generate_root_table()
{
  root_table_.clear();
  srandom(static_cast<int32_t>(seed_));
  char rowkey_str[20];
  rowkey_str[19] = 0;
  int tail_number[3];
  for (int i = 0; i < root_table_size_; i++)
  {
    int status = 0;
    for (int j = 0; j < 3; j++)
    {
      tail_number[j] = static_cast<int32_t>(random() % total_);
      for (int k = 0; k < j; k++) 
      {
        if (tail_number[k] == tail_number[j]) {
          j--;
          continue;
        }
      }
      if (tail_number[j] == number_) 
      {
        status = 1;
      }
    }
    sprintf(rowkey_str, "%010d%03d%03d%03d",i * 100,tail_number[0], tail_number[1], tail_number[2]);
    root_table_[rowkey_str] = status;
  }

  //for (map<string,int>::iterator it = root_table_.begin(); it != root_table_.end(); it++)
  //{
  //  TBSYS_LOG(INFO,"%s %d\n", it->first.c_str(), it->second);
  //}
  return 0;
}

int MockChunkServer::report_tablets()
{ 
  int ret = OB_SUCCESS;
  map<string,int>::iterator it = root_table_.begin();
  ObTabletReportInfo ti;
  ti.tablet_location_.tablet_version_ = version_;
  ti.tablet_location_.chunkserver_ = self_;
  ti.tablet_info_.range_.table_id_ = 1001;
  ti.tablet_info_.range_.border_flag_.unset_inclusive_start();
  ti.tablet_info_.range_.border_flag_.set_inclusive_end();
  ti.tablet_info_.range_.border_flag_.unset_min_value();
  ti.tablet_info_.range_.border_flag_.unset_max_value();
  ObString pre_key;
  bool last_is_mine = false;
  while (it != root_table_.end())
  {
    ObTabletReportInfoList report_list;
    for (int i =0; i < OB_MAX_TABLET_LIST_NUMBER -1 && it != root_table_.end(); )
    {
      last_is_mine = false;
      if (it->second == 1) 
      {
        last_is_mine = true;
        if (it == root_table_.begin())
        {
          ti.tablet_info_.range_.border_flag_.set_min_value();
        }
        else
        {
          ti.tablet_info_.range_.border_flag_.unset_min_value();
        }
        ti.tablet_info_.range_.start_key_ = pre_key ;
        ti.tablet_info_.range_.end_key_.assign_ptr((char*)it->first.c_str(), static_cast<int32_t>(it->first.length()));
        report_list.add_tablet(ti);
        i++;
      }
      pre_key.assign_ptr((char*)it->first.c_str(), static_cast<int32_t>(it->first.length()));
      it++;
    }
    if (it == root_table_.end() && last_is_mine < 3) 
    {
        ti.tablet_info_.range_.start_key_ = pre_key ;
        ti.tablet_info_.range_.border_flag_.set_max_value();
        report_list.add_tablet(ti);
    }
    do {
      ret = report_tablets(report_list, version_, it != root_table_.end());
      if (ret != OB_SUCCESS) sleep(1);
    }while (ret != OB_SUCCESS);
  }
  return 0;
}

int MockChunkServer::report_tablets(const ObTabletReportInfoList& tablets, int64_t time_stamp, bool has_more)
{
  int ret = OB_SUCCESS;
  ThreadSpecificBuffer::Buffer* thread_buffer = response_packet_buffer_.get_buffer();
  thread_buffer->reset();
  ObDataBuffer data_buff(thread_buffer->current(),thread_buffer->remain());

  const int64_t report_timeout = 10000000;
  ObServer client_server = self_;

  // serialize ObServer(chunkserver) + ObTabletReportInfoList + int64_t(timestamp)
  // 1. serialize ObServer(client_server)
  if (OB_SUCCESS == ret)
  {
    ret = client_server.serialize(data_buff.get_data(), data_buff.get_capacity(),
        data_buff.get_position());
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "serialize ObServer(chunkserver) failed: ret[%d].", ret);
    }
  }

  // 2. serialize report Tablets
  if (OB_SUCCESS == ret)
  {
    // report tablets information.
    for (int64_t i = 0; i < tablets.get_tablet_size(); ++i)
    {
      const common::ObTabletReportInfo& info = tablets.get_tablet()[i];
      TBSYS_LOG(DEBUG, "report begin dump tablets:i:%ld, row count:%ld", 
          i, info.tablet_info_.row_count_);
      info.tablet_info_.range_.hex_dump();
      TBSYS_LOG(DEBUG, "tablet occupy size:%ld, row count:%ld", 
          info.tablet_info_.occupy_size_, info.tablet_info_.row_count_);
    }

    ret = tablets.serialize(data_buff.get_data(), 
        data_buff.get_capacity(), data_buff.get_position());
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "serialize TabletInfoList failed: ret[%d].", ret);
    }
  }

  // 3. serialize time_stamp
  if (OB_SUCCESS == ret)
  {
    TBSYS_LOG(DEBUG, "report_tablets, timestamp:%ld count = %ld", time_stamp, tablets.tablet_list_.get_array_index());
    ret = serialization::encode_vi64(data_buff.get_data(), 
        data_buff.get_capacity(), data_buff.get_position(), time_stamp);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "encode_vi64 time_stamp failed: ret[%d].", ret);
    }
  }

  if (OB_SUCCESS == ret)
  {
    ret = client_manager_.send_request(root_server_, OB_REPORT_TABLETS,
        1 , report_timeout, data_buff);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "send_request(ReportTablets) to RootServer failed: ret[%d].", ret);
    }
  }

  int64_t return_start_pos = 0;
  if (OB_SUCCESS == ret)
  {
    ObResultCode report_result;
    ret = report_result.deserialize(data_buff.get_data(), 
        data_buff.get_position(), return_start_pos);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "deserialize ObResultCode(report tablet result) failed: ret[%d].", ret);
    }
    else if (OB_SUCCESS != report_result.result_code_)
    {
      TBSYS_LOG(ERROR, "report Tablet failed, the RootServer returned error: ret[%d].", 
          report_result.result_code_);
      ret = report_result.result_code_;
    }
  }

  if (OB_SUCCESS == ret)
  {
    if (!has_more)  // at the end of report process
    {
      TBSYS_LOG(INFO, "report tablets over, send OB_WAITING_JOB_DONE message.");
      // reset param buffer, for new remote procedure call process(OB_WAITING_JOB_DONE)
      data_buff.get_position() = 0;
      // serialize input param (const ObServer& client, const int64_t time_stamp)
      ret = client_server.serialize(data_buff.get_data(), 
          data_buff.get_capacity(), data_buff.get_position());
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "serialize ObServer(chunkserver) failed: ret[%d].", ret);
      }

      if (OB_SUCCESS == ret)
      {
        ret = serialization::encode_vi64(data_buff.get_data(), 
            data_buff.get_capacity(), data_buff.get_position(), time_stamp);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "encode_vi64 time_stamp failed: ret[%d].", ret);
        }
      }

      if (OB_SUCCESS == ret)
      {
        ret = client_manager_.send_request(root_server_, OB_WAITING_JOB_DONE,
            1 , report_timeout, data_buff);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "ObClientManager::send_request(schema_changed) to "
              "RootServer failed: ret[%d].", ret);
        }
      }

    }
  }

  return ret;

}


