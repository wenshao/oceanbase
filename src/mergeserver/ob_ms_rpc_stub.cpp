#include "ob_ms_rpc_stub.h"
#include "common/ob_client_manager.h"
#include "common/ob_server.h"
#include "common/ob_result.h"
#include "common/ob_operate_result.h"
#include "common/thread_buffer.h"
#include "common/ob_schema.h"
#include "common/ob_tablet_info.h"
#include "common/ob_read_common_data.h"
#include "common/ob_mutator.h"
#include "common/ob_scanner.h"
#include "common/ob_trace_log.h"
#include "common/utility.h"
#include "ob_merge_server.h"
#include "ob_merge_server_main.h"
#include "ob_ms_tablet_location_item.h"

using namespace oceanbase::common;
using namespace oceanbase::mergeserver;


ObMergerRpcStub::ObMergerRpcStub()
{
  init_ = false;
  rpc_buffer_ = NULL;
  rpc_frame_ = NULL;
}

ObMergerRpcStub::~ObMergerRpcStub()
{
}

int ObMergerRpcStub::init(const ThreadSpecificBuffer * rpc_buffer, 
    const ObClientManager * rpc_frame) 
{
  int ret = OB_SUCCESS;
  if (init_ || (NULL == rpc_buffer) || (NULL == rpc_frame))
  {
    TBSYS_LOG(ERROR, "already inited or check input failed:inited[%s], "
        "rpc_buffer[%p], rpc_frame[%p]", (init_? "ture": "false"), rpc_buffer, rpc_frame); 
    ret = OB_INPUT_PARAM_ERROR; 
  }
  else
  {
    rpc_buffer_ = rpc_buffer;
    rpc_frame_ = rpc_frame;
    init_ = true;
  }
  return ret;
}

int ObMergerRpcStub::get_rpc_buffer(ObDataBuffer & data_buffer) const
{
  int ret = OB_SUCCESS;
  if (!check_inner_stat())
  {
    TBSYS_LOG(ERROR, "%s", "check inner stat failed");
    ret = OB_INNER_STAT_ERROR;
  }
  else
  {
    common::ThreadSpecificBuffer::Buffer* rpc_buffer = rpc_buffer_->get_buffer();
    if (NULL == rpc_buffer)
    {
      TBSYS_LOG(ERROR, "get thread rpc buff failed:buffer[%p].", rpc_buffer);
      ret = OB_INNER_STAT_ERROR;
    }
    else
    {
      rpc_buffer->reset();
      data_buffer.set_data(rpc_buffer->current(), rpc_buffer->remain());
    }
  }
  return ret;
}


int ObMergerRpcStub::find_server(const int64_t timeout, const ObServer & root_server,
    ObServer & update_server) const
{
  int ret = OB_SUCCESS;
  ObDataBuffer data_buff;
  ret = get_rpc_buffer(data_buff);
  // step 1. send get update server info request
  if (OB_SUCCESS == ret)
  {
    ret = rpc_frame_->send_request(root_server, OB_GET_UPDATE_SERVER_INFO, DEFAULT_VERSION, 
        timeout, data_buff);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "send request to root server for find update server failed:ret[%d]", ret);
    }
  }

  // step 2. deserialize restult code
  int64_t pos = 0;
  if (OB_SUCCESS == ret)
  { 
    ObResultCode result_code;
    ret = result_code.deserialize(data_buff.get_data(), data_buff.get_position(), pos);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "deserialize result_code failed:pos[%ld], ret[%d]", pos, ret);
    }
    else
    {
      ret = result_code.result_code_;
    }
  }

  // step 3. deserialize update server addr
  if (OB_SUCCESS == ret)
  {
    ret = update_server.deserialize(data_buff.get_data(), data_buff.get_position(), pos);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "deserialize server failed:pos[%ld], ret[%d]", pos, ret);
    }
    else
    {
      TBSYS_LOG(DEBUG, "find update server succ:addr[%d], port[%d]", update_server.get_ipv4(),
          update_server.get_port());
    }
  }
  return ret;
}

int ObMergerRpcStub::fetch_server_list(const int64_t timeout, const ObServer & root_server,
    ObUpsList & server_list) const
{
  int ret = OB_SUCCESS;
  ObDataBuffer data_buff;
  ret = get_rpc_buffer(data_buff);
  // step 1. send get update server list info request
  if (OB_SUCCESS == ret)
  {
    ret = rpc_frame_->send_request(root_server, OB_GET_UPS, DEFAULT_VERSION,
        timeout, data_buff);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "send request to root server for find update server failed:ret[%d]", ret);
    }
  }

  // step 2. deserialize restult code
  int64_t pos = 0;
  if (OB_SUCCESS == ret)
  {
    ObResultCode result_code;
    ret = result_code.deserialize(data_buff.get_data(), data_buff.get_position(), pos);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "deserialize result_code failed:pos[%ld], ret[%d]", pos, ret);
    }
    else
    {
      ret = result_code.result_code_;
    }
  }

  // step 3. deserialize update server addr
  if (OB_SUCCESS == ret)
  {
    ret = server_list.deserialize(data_buff.get_data(), data_buff.get_position(), pos);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "deserialize server list failed:pos[%ld], ret[%d]", pos, ret);
    }
    else
    {
      TBSYS_LOG(DEBUG, "fetch update server list succ:count[%d]", server_list.ups_count_);
    }
  }
  return ret;
}

// heartbeat rpc through register server interface
int ObMergerRpcStub::heartbeat_server(const int64_t timeout, const ObServer & root_server,
    const ObServer & merge_server, const ObRole server_role) const
{
  int ret = OB_SUCCESS;
  UNUSED(timeout);
  ObDataBuffer data_buff;
  ret = get_rpc_buffer(data_buff);
  // step 1. serialize merge server to data_buff
  if (OB_SUCCESS == ret)
  {
    ret = merge_server.serialize(data_buff.get_data(), data_buff.get_capacity(),
        data_buff.get_position());
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "serialize merge server addr failed:ret[%d]", ret);
    }
  }
  // step 2. serialize is merge server to data_buff
  if (OB_SUCCESS == ret)
  {
    ret = serialization::encode_vi32(data_buff.get_data(), 
        data_buff.get_capacity(), data_buff.get_position(), server_role);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "serialize is merge server statu failed:ret[%d]", ret);
    }
  }
  // step 3. send request for server heartbeat 
  if (OB_SUCCESS == ret)
  {
    ret = rpc_frame_->post_request(root_server, OB_HEARTBEAT, NEW_VERSION, data_buff);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "post request to root server for heartbeat failed:ret[%d]", ret);
    }
  }
  return ret;
}


int ObMergerRpcStub::register_server(const int64_t timeout, const ObServer & root_server,
    const ObServer & merge_server, const bool is_merger) const
{
  int ret = OB_SUCCESS;
  ObDataBuffer data_buff;
  ret = get_rpc_buffer(data_buff);
  // step 1. serialize merge server to data_buff
  if (OB_SUCCESS == ret)
  {
    ret = merge_server.serialize(data_buff.get_data(), data_buff.get_capacity(),
        data_buff.get_position());
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "serialize merge server addr failed:ret[%d]", ret);
    }
  }
  // step 2. serialize is merge server to data_buff
  if (OB_SUCCESS == ret)
  {
    ret = serialization::encode_bool(data_buff.get_data(), 
        data_buff.get_capacity(), data_buff.get_position(), is_merger);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "serialize is merge server statu failed:ret[%d]", ret);
    }
  }
  // step 3. send request for server register 
  if (OB_SUCCESS == ret)
  {
    ret = rpc_frame_->send_request(root_server, OB_SERVER_REGISTER, DEFAULT_VERSION, 
        timeout, data_buff);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "send request to root server for register server failed:ret[%d]", ret);
    }
  }
  // step 4. deserialize the response code
  int64_t pos = 0;
  if (OB_SUCCESS == ret)
  {
    ObResultCode result_code;
    ret = result_code.deserialize(data_buff.get_data(), data_buff.get_position(), pos);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "deserialize result_code failed:pos[%ld], ret[%d]", pos, ret);
    }
    else
    {
      ret = result_code.result_code_;
    }
  }
  return ret;
}


// fetch schema current version
int ObMergerRpcStub::fetch_schema_version(const int64_t timeout, const common::ObServer & root_server, 
    int64_t & timestamp) const
{
  int ret = OB_SUCCESS;
  ObDataBuffer data_buff;
  ret = get_rpc_buffer(data_buff);
  // step 1. send request for fetch schema newest version
  if (OB_SUCCESS == ret)
  {
    ret = rpc_frame_->send_request(root_server, OB_FETCH_SCHEMA_VERSION, DEFAULT_VERSION,
        timeout, data_buff);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "send request to root server for fetch schema version failed:ret[%d]", ret);
    }
  }
  // step 2. deserialize the response code
  int64_t pos = 0;
  if (OB_SUCCESS == ret)
  {
    ObResultCode result_code;
    ret = result_code.deserialize(data_buff.get_data(), data_buff.get_position(), pos);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "deserialize result_code failed:pos[%ld], ret[%d]", pos, ret);
    }
    else
    {
      ret = result_code.result_code_;
    }
  }
  
  // step 3. deserialize the version
  if (OB_SUCCESS == ret)
  {
    ret = serialization::decode_vi64(data_buff.get_data(), data_buff.get_position(), pos, &timestamp);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "deserialize schema version from buff failed:"
          "version[%ld], pos[%ld], ret[%d]", timestamp, pos, ret);
    }
    else
    {
      TBSYS_LOG(DEBUG, "fetch schema version succ:version[%ld]", timestamp);
    }
  }
  return ret;
}

int ObMergerRpcStub::fetch_schema(const int64_t timeout, const ObServer & root_server, 
    const int64_t version, ObSchemaManagerV2 & schema) const
{
  int ret = OB_SUCCESS;
  ObDataBuffer data_buff;
  ret = get_rpc_buffer(data_buff);
  // step 1. serialize timestamp to data_buff
  if (OB_SUCCESS == ret)
  {
    ret = serialization::encode_vi64(data_buff.get_data(), 
        data_buff.get_capacity(), data_buff.get_position(), version);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "serialize timestamp failed:version[%ld], ret[%d]", 
          version, ret);
    }
  }
  // step 2. send request for fetch new schema
  if (OB_SUCCESS == ret)
  {
    ret = rpc_frame_->send_request(root_server, OB_FETCH_SCHEMA, DEFAULT_VERSION,
        timeout, data_buff);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "send request to root server for fetch schema failed:"
          "version[%ld], ret[%d]", version, ret);
    }
  }
  // step 3. deserialize the response code
  int64_t pos = 0;
  if (OB_SUCCESS == ret)
  {
    ObResultCode result_code;
    ret = result_code.deserialize(data_buff.get_data(), data_buff.get_position(), pos);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "deserialize result_code failed:pos[%ld], ret[%d]", pos, ret);
    }
    else
    {
      ret = result_code.result_code_;
    }
  }
  // step 4. deserialize the table schema
  if (OB_SUCCESS == ret)
  {
    schema.set_drop_column_group(true);
    ret = schema.deserialize(data_buff.get_data(), data_buff.get_position(), pos);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "deserialize schema from buff failed:"
          "version[%ld], pos[%ld], ret[%d]", version, pos, ret);
    }
    else
    {
      TBSYS_LOG(DEBUG, "fetch schema succ:version[%ld]", schema.get_version());
      //schema.print_info();
    }
  }
  return ret;
}


int ObMergerRpcStub::get(const int64_t timeout, const ObServer & server,
    const ObGetParam & get_param, ObScanner & scanner) const
{
  int ret = OB_SUCCESS;
  ObDataBuffer data_buff;
  ret = get_rpc_buffer(data_buff);
  // step 1. serialize ObGetParam to the data_buff
  if (OB_SUCCESS == ret)
  {
    ret = get_param.serialize(data_buff.get_data(), data_buff.get_capacity(), 
        data_buff.get_position());
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "serialize get_param failed:ret[%d]", ret);
    }
  }
  // step 2. send request for get data 
  if (OB_SUCCESS == ret)
  {
    ret = rpc_frame_->send_request(server, OB_GET_REQUEST, DEFAULT_VERSION,
        timeout, data_buff);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "send get request to server failed:ret[%d]", ret);
    }
  }
  // step 3. deserialize the response result
  int64_t pos = 0;
  if (OB_SUCCESS == ret)
  {
    ObResultCode result_code;
    ret = result_code.deserialize(data_buff.get_data(), data_buff.get_position(), pos);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "deserialize result failed:pos[%ld], ret[%d]", pos, ret);
    }
    else
    {
      ret = result_code.result_code_;
    }
  }
  // step 4. deserialize the scanner 
  if (OB_SUCCESS == ret)
  {
    scanner.clear();
    ret = scanner.deserialize(data_buff.get_data(), data_buff.get_position(), pos);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "deserialize scanner from buff failed:pos[%ld], ret[%d]", pos, ret);
    }
  }
  const int32_t MAX_SERVER_ADDR_SIZE = 128;
  char server_addr[MAX_SERVER_ADDR_SIZE];
  bool is_fullfilled = false;
  int64_t fullfilled_item_num = 0;
  int64_t res_size = pos;
  scanner.get_is_req_fullfilled(is_fullfilled,fullfilled_item_num);
  server.to_string(server_addr, MAX_SERVER_ADDR_SIZE);
  // write debug log
  if (OB_SUCCESS == ret)
  {
    TBSYS_LOG(DEBUG, "get data succ from server:addr[%s]", server_addr);
  }
  else
  {
    TBSYS_LOG(WARN, "get data failed from server:addr[%s], ret[%d]", server_addr, ret);
  }
	FILL_TRACE_LOG("step 3.* finish server get:addr[%s], err[%d] fullfill[%d] item_num[%ld] res_size[%ld]",
      server_addr, ret, is_fullfilled, fullfilled_item_num, res_size);
  return ret;
}

int ObMergerRpcStub::mutate(const int64_t timeout, const ObServer & server,
    const ObMutator & mutate_param, const bool has_data, ObScanner & scanner) const
{
  int ret = OB_SUCCESS;
  ObDataBuffer data_buff;
  ret = get_rpc_buffer(data_buff);
  // step 1. serialize ObScanParam to the data_buff
  if (OB_SUCCESS == ret)
  {
    ret = mutate_param.serialize(data_buff.get_data(), data_buff.get_capacity(),
        data_buff.get_position());
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "serialize mutate param failed:ret[%d]", ret);
    }
  }
  // step 2. send request for scan data
  if (OB_SUCCESS == ret)
  {
    ret = rpc_frame_->send_request(server, OB_MS_MUTATE, DEFAULT_VERSION,
        timeout, data_buff);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "send mutate request to server failed:ret[%d]", ret);
    }
  }
  // step 3. deserialize the response result
  int64_t pos = 0;
  if (OB_SUCCESS == ret)
  {
    ObResultCode result_code;
    ret = result_code.deserialize(data_buff.get_data(), data_buff.get_position(), pos);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "deserialize result failed:pos[%ld], ret[%d]", pos, ret);
    }
    else
    {
      ret = result_code.result_code_;
    }
  }

  // step 4. deserialize the scanner
  if ((OB_SUCCESS == ret) && (true == has_data))
  {
    ret = scanner.deserialize(data_buff.get_data(), data_buff.get_position(), pos);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "deserialize scanner from buff failed:pos[%ld], ret[%d]", pos, ret);
    }
  }

  const int32_t MAX_SERVER_ADDR_SIZE = 128;
  char server_addr[MAX_SERVER_ADDR_SIZE];
  server.to_string(server_addr, MAX_SERVER_ADDR_SIZE);
  bool is_fullfilled = false;
  int64_t fullfilled_item_num = 0;
  int64_t res_size = pos;
  scanner.get_is_req_fullfilled(is_fullfilled, fullfilled_item_num);
  if (OB_SUCCESS == ret)
  {
    TBSYS_LOG(DEBUG, "mutate succ to server:addr[%s]", server_addr);
  }
  else
  {
    TBSYS_LOG(WARN, "mutate failed from server:addr[%s], ret[%d]", server_addr, ret);
  }
	FILL_TRACE_LOG("step 3.* finish server mutate:addr[%s], err[%d] fullfill[%d] item_num[%ld] res_size[%ld]",
      server_addr, ret, is_fullfilled, fullfilled_item_num, res_size);
  return ret;
}

int ObMergerRpcStub::scan(const int64_t timeout, const ObServer & server,
    const ObScanParam & scan_param, ObScanner & scanner) const
{
  int ret = OB_SUCCESS;
  ObDataBuffer data_buff;
  ret = get_rpc_buffer(data_buff);
  // step 1. serialize ObScanParam to the data_buff
  if (OB_SUCCESS == ret)
  {
    ret = scan_param.serialize(data_buff.get_data(), data_buff.get_capacity(), 
        data_buff.get_position());
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "serialize scan param failed:ret[%d]", ret);
    }
  }
  // step 2. send request for scan data 
  if (OB_SUCCESS == ret)
  {
    ret = rpc_frame_->send_request(server, OB_SCAN_REQUEST, DEFAULT_VERSION, 
        timeout, data_buff);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "send scan request to server failed:ret[%d]", ret);
    }
  }
  // step 3. deserialize the response result
  int64_t pos = 0;
  if (OB_SUCCESS == ret)
  {
    ObResultCode result_code;
    ret = result_code.deserialize(data_buff.get_data(), data_buff.get_position(), pos);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "deserialize result failed:pos[%ld], ret[%d]", pos, ret);
    }
    else
    {
      ret = result_code.result_code_;
    }
  }
  
  // step 4. deserialize the scanner 
  if (OB_SUCCESS == ret)
  {
    scanner.clear();
    ret = scanner.deserialize(data_buff.get_data(), data_buff.get_position(), pos);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "deserialize scanner from buff failed:pos[%ld], ret[%d]", pos, ret);
    }
  }
  
  // write debug log
  const int32_t MAX_SERVER_ADDR_SIZE = 128;
  char server_addr[MAX_SERVER_ADDR_SIZE];
  server.to_string(server_addr, MAX_SERVER_ADDR_SIZE);
  bool is_fullfilled = false;
  int64_t fullfilled_item_num = 0;
  int64_t res_size = pos;
  scanner.get_is_req_fullfilled(is_fullfilled,fullfilled_item_num);
  if (OB_SUCCESS == ret)
  {
    TBSYS_LOG(DEBUG, "scan data succ from server:addr[%s]", server_addr);
  }
  else
  {
    TBSYS_LOG(WARN, "scan data failed from server:addr[%s], ret[%d]", server_addr, ret);
  }
	FILL_TRACE_LOG("step 3.* finish server scan:addr[%s], err[%d] fullfill[%d] item_num[%ld] res_size[%ld]", 
                 server_addr, ret, is_fullfilled, fullfilled_item_num, res_size);
  return ret;
}

// server get 
int ObMergerRpcStub::get(const int64_t timeout, ObMergerTabletLocationList & list,
    const ObGetParam & get_param, ObMergerTabletLocation & succ_addr, 
    ObScanner & scanner, bool & update_list) const
{
  int ret = OB_SUCCESS; 
  if (0 == list.size())
  {
    TBSYS_LOG(WARN, "%s", "check list size is zero");
    ret = OB_DATA_NOT_SERVE;
  }
  else
  {
    // set all invlaid item to valid status
    if (list.get_valid_count() < 1)
    {
      list.set_item_valid(tbsys::CTimeUtil::getTime());
    }

    ret = OB_CHUNK_SERVER_ERROR;
    for (int32_t i = 0; i < list.size(); ++i)
    {
      if (list[i].err_times_ >= ObMergerTabletLocation::MAX_ERR_TIMES)
      {
        TBSYS_LOG(DEBUG, "check server err times too much:times[%ld]", list[i].err_times_);
        continue;
      }
      scanner.clear();
      ret = get(timeout, list[i].server_.chunkserver_, get_param, scanner);
      if (OB_CS_TABLET_NOT_EXIST == ret)
      {
        TBSYS_LOG(WARN, "check chunk server position failed:pos[%d], count[%ld], ret[%d]",
            i, list.size(), ret);
        list[i].err_times_ = ObMergerTabletLocation::MAX_ERR_TIMES;
        update_list = true;
        continue;
      }
      else if (ret != OB_SUCCESS)
      {
        TBSYS_LOG(WARN, "get from chunk server failed:pos[%d], count[%ld], ret[%d]",
            i, list.size(), ret);
        ++list[i].err_times_;
        update_list = true;
        continue;
      }
      else
      {
        TBSYS_LOG(DEBUG, "get from chunk server succ:pos[%d], count[%ld]", i, list.size());
        if (list[i].err_times_ != 0)
        {
          list[i].err_times_ = 0;
          update_list = true;
        }
        succ_addr = list[i];
        break;
      }
    }
  }
  return ret;
}


// server scan 
int ObMergerRpcStub::scan(const int64_t timeout, ObMergerTabletLocationList & list,
    const ObScanParam & scan_param, ObMergerTabletLocation & succ_addr, 
    ObScanner & scanner, bool & update_list) const
{
  int ret = OB_SUCCESS; 
  if (0 == list.size())
  {
    TBSYS_LOG(WARN, "%s", "check list size is zero");
    ret = OB_DATA_NOT_SERVE;
  }
  else
  {
    // set all invlaid item to valid status
    if (list.get_valid_count() < 1)
    {
      list.set_item_valid(tbsys::CTimeUtil::getTime());
    }

    ret = OB_CHUNK_SERVER_ERROR;
    for (int32_t i = 0; i < list.size(); ++i)
    {
      if (list[i].err_times_ >= ObMergerTabletLocation::MAX_ERR_TIMES)
      {
        TBSYS_LOG(DEBUG, "check server err times too much:times[%ld]", list[i].err_times_);
        continue;
      }
      scanner.clear();
      ret = scan(timeout, list[i].server_.chunkserver_, scan_param, scanner);
      if (OB_CS_TABLET_NOT_EXIST == ret)
      {
        TBSYS_LOG(WARN, "check chunk server position failed:pos[%d], count[%ld], ret[%d]",
            i, list.size(), ret);
        list[i].err_times_ = ObMergerTabletLocation::MAX_ERR_TIMES;
        update_list = true;
        continue;
      }
      else if (ret != OB_SUCCESS)
      {
        TBSYS_LOG(WARN, "scan from chunk server failed:pos[%d], count[%ld], ret[%d]",
            i, list.size(), ret);
        ++list[i].err_times_;
        update_list = true;
        continue;
      }
      else
      {
        TBSYS_LOG(DEBUG, "scan from chunk server succ:pos[%d], count[%ld]", i, list.size());
        if (list[i].err_times_ != 0)
        {
          list[i].err_times_ = 0;
          update_list = true;
        }
        succ_addr = list[i];
        break;
      }
    }
  }
  return ret;
}

int ObMergerRpcStub::fetch_frozen_version(const int64_t timeout, const ObServer & server,
    int64_t & version) const
{
  int ret = OB_SUCCESS;
  ObDataBuffer data_buff;
  ret = get_rpc_buffer(data_buff);
  if (OB_SUCCESS == ret)
  {
    ret = rpc_frame_->send_request(server, OB_UPS_GET_LAST_FROZEN_VERSION,
        DEFAULT_VERSION, timeout, data_buff);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "send get last frozen version request to server failed:ret[%d]", ret);
    }
  }

  int64_t pos = 0;
  if (OB_SUCCESS == ret)
  {
    ObResultCode result_code;
    ret = result_code.deserialize(data_buff.get_data(), data_buff.get_position(), pos);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "deserialize result failed:pos[%ld], ret[%d]", pos, ret);
    }
    else
    {
      ret = result_code.result_code_;
    }
  }

  // step 4. deserialize the scanner
  if (OB_SUCCESS == ret)
  {
    ret = serialization::decode_vi64(data_buff.get_data(), data_buff.get_position(), pos, &version);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "deserialize scanner from buff failed:pos[%ld], ret[%d]", pos, ret);
    }
  }
  return ret;
}

int ObMergerRpcStub::fetch_tablet_location(const int64_t timeout, const ObServer & root_server,
    const uint64_t root_table_id, const uint64_t table_id, const ObString & row_key,
    ObScanner & scanner) const
{
  ObCellInfo cell;
  // cell info not root table id
  UNUSED(root_table_id);
  cell.table_id_ = table_id;
  cell.column_id_ = 0;
  cell.row_key_ = row_key;
  ObGetParam get_param(cell);
  get_param.set_is_result_cached(false);
  get_param.set_is_read_consistency(false);
  int ret = get(timeout, root_server, get_param, scanner);
  if (ret != OB_SUCCESS)
  {
    TBSYS_LOG(WARN, "scan root server for get chunk server location failed:"
        "table_id[%lu], ret[%d]", table_id, ret);
    hex_dump(row_key.ptr(), row_key.length(), true);
  }
  else
  {
    TBSYS_LOG(DEBUG, "scan root server for get chunk server location succ:"
        "table_id[%lu]", table_id);
  }
  return ret;
}

int ObMergerRpcStub::reload_self_config(const int64_t timeout, const ObServer & merge_server, const char *filename) const
{
  int ret = OB_SUCCESS;
  UNUSED(timeout);
  ObDataBuffer data_buff;
  ObString file_str;
  ret = get_rpc_buffer(data_buff);
  // step 1. serialize filename to data_buff
  if (OB_SUCCESS == ret)
  {
    file_str.assign(const_cast<char*>(filename), static_cast<int32_t>(strlen(filename)));
    ret = file_str.serialize(data_buff.get_data(), data_buff.get_capacity(),
                data_buff.get_position());
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "serialize merge server addr failed:ret[%d]", ret);
    }
  }
  // step 2. send request for server conf reload
  if (OB_SUCCESS == ret)
  {
    ret = rpc_frame_->send_request(merge_server, OB_UPS_RELOAD_CONF, DEFAULT_VERSION, timeout, data_buff);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "post request to root server for heartbeat failed:ret[%d]", ret);
    }
  }
  // step 3. deserialize restult code
  int64_t pos = 0;
  if (OB_SUCCESS == ret)
  {
    ObResultCode result_code;
    ret = result_code.deserialize(data_buff.get_data(), data_buff.get_position(), pos);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "deserialize result_code failed:pos[%ld], ret[%d]", pos, ret);
    }
    else
    {
      ret = result_code.result_code_;
    }
  }
  return ret;
}


