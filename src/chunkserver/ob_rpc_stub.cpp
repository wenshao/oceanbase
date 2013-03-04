/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or 
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_rpc_stub.cpp for rpc among chunk server, update server and
 * root server. 
 *
 * Authors:
 *   xielun <xielun.szd@taobao.com>
 *   huating <huating.zmq@taobao.com>
 *
 */
#include "ob_rpc_stub.h"
#include "common/ob_client_manager.h"
#include "common/ob_server.h"
#include "common/ob_result.h"
#include "common/ob_operate_result.h"
#include "common/thread_buffer.h"
#include "common/ob_schema.h"
#include "common/ob_tablet_info.h"
#include "common/ob_read_common_data.h"
#include "common/ob_scanner.h"
#include "common/ob_trace_log.h"
#include "common/utility.h"

namespace oceanbase
{
  namespace chunkserver
  {
    using namespace oceanbase::common;
        
    ObMergerRpcStub::ObMergerRpcStub()
    {
      init_ = false;
      rpc_buffer_ = NULL;
      rpc_frame_ = NULL;
    }
    
    ObMergerRpcStub::~ObMergerRpcStub()
    {
    }
    
    int ObMergerRpcStub::init(
        const ThreadSpecificBuffer * rpc_buffer, 
        const ObClientManager * rpc_frame) 
    {
      int ret = OB_SUCCESS;
      if (init_ || (NULL == rpc_buffer) || (NULL == rpc_frame))
      {
        TBSYS_LOG(WARN, "already inited or check input failed:inited[%s], "
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
        TBSYS_LOG(WARN, "check inner stat failed");
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
    
    int ObMergerRpcStub::fetch_update_server(
        const int64_t timeout, const ObServer & root_server,
        ObServer & update_server, bool for_merge) const
    {
      int ret = OB_SUCCESS;
      ObDataBuffer data_buff;
      ret = get_rpc_buffer(data_buff);
      // step 1. send get update server info request
      if (OB_SUCCESS == ret)
      {
        ret = rpc_frame_->send_request(root_server, 
          for_merge ? OB_GET_UPDATE_SERVER_INFO_FOR_MERGE : OB_GET_UPDATE_SERVER_INFO, 
          DEFAULT_VERSION, timeout, data_buff);
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
          TBSYS_LOG(WARN, "deserialize result_code failed:pos[%ld], ret[%d]", pos, ret);
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
          TBSYS_LOG(WARN, "deserialize server failed:pos[%ld], ret[%d]", pos, ret);
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


    int ObMergerRpcStub::fetch_frozen_time(
        const int64_t timeout, ObServer & update_server, 
        int64_t frozen_version, int64_t& frozen_time) const
    {
      int ret = OB_SUCCESS;
      ObDataBuffer data_buff;
      ret = get_rpc_buffer(data_buff);
      // step 1. encode frozen version and send request
      if (OB_SUCCESS == ret)
      {
        ret = serialization::encode_vi64(data_buff.get_data(), data_buff.get_capacity(),
            data_buff.get_position(), frozen_version);
        if(OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "encode frozen_version faied: frozen_version=%ld", frozen_version);
        }
        else
        {
          ret = rpc_frame_->send_request(update_server, OB_UPS_GET_TABLE_TIME_STAMP,
              DEFAULT_VERSION, timeout, data_buff);
        }
    
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "send request to update server for get timestamp failed:ret[%d]", ret);
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
        ret = serialization::decode_vi64(data_buff.get_data(),
            data_buff.get_position(), pos, &frozen_time);
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "deserialize frozen_time failed:ret[%d]", ret);
        }
      }
      return ret;
    }

    // fetch schema current version
    int ObMergerRpcStub::fetch_schema_version(
        const int64_t timeout, const common::ObServer & root_server, 
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
          TBSYS_LOG(WARN, "deserialize result_code failed:pos[%ld], ret[%d]", pos, ret);
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
          TBSYS_LOG(WARN, "deserialize schema version from buff failed:"
              "version[%ld], pos[%ld], ret[%d]", timestamp, pos, ret);
        }
        else
        {
          TBSYS_LOG(DEBUG, "fetch schema version succ:version[%ld]", timestamp);
        }
      }
      return ret;
    }
    
    int ObMergerRpcStub::fetch_schema(
        const int64_t timeout, const ObServer & root_server, 
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
          TBSYS_LOG(WARN, "serialize timestamp failed:version[%ld], ret[%d]", 
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
          TBSYS_LOG(WARN, "send request to root server[%s] for fetch schema failed:"
                    "version[%ld], ret[%d]", root_server.to_cstring(), version, ret);
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
          TBSYS_LOG(WARN, "deserialize result_code failed:pos[%ld], ret[%d]", pos, ret);
        }
        else
        {
          ret = result_code.result_code_;
        }
      }
      // step 4. deserialize the table schema
      if (OB_SUCCESS == ret)
      {
        ret = schema.deserialize(data_buff.get_data(), data_buff.get_position(), pos);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "deserialize schema from buff failed:"
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
    
    int ObMergerRpcStub::get(
        const int64_t timeout, const ObServer & server,
        const ObGetParam & get_param, ObScanner & scanner) const
    {
      int ret = OB_SUCCESS;
      ObDataBuffer data_buff;
      int64_t start_time = tbsys::CTimeUtil::getTime();
      ret = get_rpc_buffer(data_buff);
      // step 1. serialize ObGetParam to the data_buff
      if (OB_SUCCESS == ret)
      {
        ret = get_param.serialize(data_buff.get_data(), data_buff.get_capacity(), 
            data_buff.get_position());
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "serialize get_param failed:ret[%d]", ret);
        }
      }
      // step 2. send request for get data 
      if (OB_SUCCESS == ret)
      {
        ret = rpc_frame_->send_request(server, OB_GET_REQUEST, DEFAULT_VERSION,
            timeout, data_buff);
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(WARN, "send get request to server failed:ret[%d], timeout=%ld", 
            ret, timeout);
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
          TBSYS_LOG(WARN, "deserialize result failed:pos[%ld], ret[%d]", pos, ret);
        }
        else
        {
          ret = result_code.result_code_;
        }
      }
      // step 4. deserialize the scanner 
      if (OB_SUCCESS == ret)
      {
        ret = scanner.deserialize(data_buff.get_data(), data_buff.get_position(), pos);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "deserialize scanner from buff failed:pos[%ld], ret[%d]", pos, ret);
        }
      }
      const int32_t MAX_SERVER_ADDR_SIZE = 128;
      char server_addr[MAX_SERVER_ADDR_SIZE];
      bool is_fullfilled = false;
      int64_t fullfilled_item_num = 0;
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

      int64_t consume_time = tbsys::CTimeUtil::getTime() - start_time;
      if ((double)consume_time > (double)timeout * 0.8)
      {
        TBSYS_LOG(WARN, "slow ups get, ups_addr=%s, timeout=%ld, consume=%ld", 
            server.to_cstring(), timeout, consume_time);
      }
      return ret;
    }
    
    int ObMergerRpcStub::scan(
        const int64_t timeout, const ObServer & server, 
        const ObScanParam & scan_param, ObScanner & scanner) const
    {
      int ret = OB_SUCCESS;
      ObDataBuffer data_buff;
      int64_t start_time = tbsys::CTimeUtil::getTime();
      ret = get_rpc_buffer(data_buff);
      // step 1. serialize ObScanParam to the data_buff
      if (OB_SUCCESS == ret)
      {
        ret = scan_param.serialize(data_buff.get_data(), data_buff.get_capacity(), 
            data_buff.get_position());
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "serialize scan param failed:ret[%d]", ret);
        }
      }
      // step 2. send request for scan data 
      if (OB_SUCCESS == ret)
      {
        ret = rpc_frame_->send_request(server, OB_SCAN_REQUEST, DEFAULT_VERSION, 
            timeout, data_buff);
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(WARN, "send scan request to server failed:ret[%d], timeout=%ld", 
            ret, timeout);
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
          TBSYS_LOG(WARN, "deserialize result failed:pos[%ld], ret[%d]", pos, ret);
        }
        else
        {
          ret = result_code.result_code_;
        }
      }
      
      // step 4. deserialize the scanner 
      if (OB_SUCCESS == ret)
      {
        ret = scanner.deserialize(data_buff.get_data(), data_buff.get_position(), pos);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "deserialize scanner from buff failed:pos[%ld], ret[%d]", pos, ret);
        }
      }
      
      // write debug log
      const int32_t MAX_SERVER_ADDR_SIZE = 128;
      char server_addr[MAX_SERVER_ADDR_SIZE];
      server.to_string(server_addr, MAX_SERVER_ADDR_SIZE);
      bool is_fullfilled = false;
      int64_t fullfilled_item_num = 0;
      scanner.get_is_req_fullfilled(is_fullfilled,fullfilled_item_num);
      if (OB_SUCCESS == ret)
      {
        TBSYS_LOG(DEBUG, "scan data succ from server:addr[%s]", server_addr);
      }
      else
      {
        TBSYS_LOG(WARN, "scan data failed from server:addr[%s], "
                        "version_range=%s, ret[%d]", 
          server_addr, range2str(scan_param.get_version_range()), ret);
      }

      int64_t consume_time = tbsys::CTimeUtil::getTime() - start_time;
      if ((double)consume_time > (double)timeout * 0.8)
      {
        TBSYS_LOG(WARN, "slow ups scan, ups_addr=%s, timeout=%ld, consume=%ld", 
            server.to_cstring(), timeout, consume_time);
      }
      return ret;
    }
  } // end namespace chunkserver
} // end namespace oceanbase
