/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_server_rpc.cpp for define API of merge server, 
 * chunk server, update server, root server rpc. 
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */
#include "ob_server_rpc.h"
#include "common/ob_result.h"
#include "ob_base_client.h"

namespace oceanbase 
{ 
  namespace client 
  {
    using namespace common;
    using namespace common::serialization;

    ObServerRpc::ObServerRpc()
    : frame_buffer_(FRAME_BUFFER_SIZE), rpc_frame_(NULL)
    {

    }
    
    ObServerRpc::~ObServerRpc()
    {
    
    }
    
    int ObServerRpc::init(const ObClientManager* rpc_frame)
    {
      int ret = OB_SUCCESS;

      if (NULL != rpc_frame_ || NULL == rpc_frame)
      {
        TBSYS_LOG(WARN, "already inited or check input failed, "
                        "rpc_frame_=%p, rpc_frame=%p",
                  rpc_frame_, rpc_frame);
        ret = OB_ERROR; 
      }
      else
      {
        rpc_frame_ = rpc_frame;
      }

      return ret;
    }
    
    int ObServerRpc::get_frame_buffer(ObDataBuffer& data_buffer) const
    {
      int ret = OB_SUCCESS;
      ThreadSpecificBuffer::Buffer* rpc_buffer = NULL;

      if (NULL == rpc_frame_)
      {
        TBSYS_LOG(WARN, "server rpc doesn't init.");
        ret = OB_ERROR;
      }
      
      if (OB_SUCCESS == ret)
      {
        rpc_buffer = frame_buffer_.get_buffer();
        if (NULL == rpc_buffer)
        {
          TBSYS_LOG(WARN, "get thread rpc buff failed,buffer=%p.", rpc_buffer);
          ret = OB_ERROR;
        }
        else
        {
          rpc_buffer->reset();
          data_buffer.set_data(rpc_buffer->current(), rpc_buffer->remain());
        }
      }

      return ret;
    }

    int ObServerRpc::fetch_schema(const ObServer& root_server,
                                  const int64_t timestap, 
                                  ObSchemaManagerV2& schema,
                                  const int64_t timeout)
    {
      int ret     = OB_SUCCESS;
      int64_t pos = 0;
      ObResultCode result_code;
      ObDataBuffer data_buff;

      if (root_server.get_ipv4() == 0 || root_server.get_port() == 0
          || timestap < 0 || timeout <= 0)
      {
        TBSYS_LOG(WARN, "invalid param, ip=%d, port=%d, timestap=%ld, "
                        "timeout=%ld",
                  root_server.get_ipv4(), root_server.get_port(),
                  timestap, timeout);
        ret = OB_ERROR;
      }
      else if (NULL == rpc_frame_)
      {
        TBSYS_LOG(WARN, "server rpc doesn't init.");
        ret = OB_ERROR;
      }

      if (OB_SUCCESS == ret)
      {
        ret = get_frame_buffer(data_buff);
      }
    
      // step 1. serialize timestap to data_buff
      if (OB_SUCCESS == ret)
      {
        ret = encode_vi64(data_buff.get_data(), 
                          data_buff.get_capacity(), 
                          data_buff.get_position(), timestap);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "serialize timestap failed, timestap=%ld, ret=%d.", 
                    timestap, ret);
        }
      }
    
      // step 2. send request for fetch new schema
      if (OB_SUCCESS == ret)
      {
        ret = rpc_frame_->send_request(root_server, 
                                       OB_FETCH_SCHEMA, DEFAULT_VERSION, 
                                       timeout, data_buff);
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(WARN, "send request to root server for fetch schema failed, "
                          "timestap=%ld, ret=%d.", timestap, ret);
        }
      }
    
      // step 3. deserialize the response code
      if (OB_SUCCESS == ret)
      {
        ret = result_code.deserialize(data_buff.get_data(), 
                                      data_buff.get_position(), pos);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "deserialize result_code failed, pos=%ld, ret=%d.", 
                    pos, ret);
        }
        else
        {
          ret = result_code.result_code_;
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "get response from root server failed, "
                            "timeout=%ld, ret=%d.", timeout, ret);
          }
        }
      }

      // step 4. deserialize the table schema
      if (OB_SUCCESS == ret)
      {
        ret = schema.deserialize(data_buff.get_data(), 
                                 data_buff.get_position(), pos);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "deserialize schema from buffer failed, "
                          "timestap=%ld, pos=%ld, ret=%d", 
                    timestap, pos, ret);
        }
      }
    
      return ret;
    }

    int ObServerRpc::fetch_update_server(const ObServer& root_server,
                                         ObServer& update_server,
                                         const int64_t timeout)
    {
      int ret     = OB_SUCCESS;
      int64_t pos = 0;
      ObResultCode result_code;
      ObDataBuffer data_buff;

      if (root_server.get_ipv4() == 0 || root_server.get_port() == 0
          || timeout <= 0)
      {
        TBSYS_LOG(WARN, "invalid param, ip=%d, port=%d, timeout=%ld",
                  root_server.get_ipv4(), root_server.get_port(),
                  timeout);
        ret = OB_ERROR;
      }
      else if (NULL == rpc_frame_)
      {
        TBSYS_LOG(WARN, "server rpc doesn't init.");
        ret = OB_ERROR;
      }

      if (OB_SUCCESS == ret)
      {
        ret = get_frame_buffer(data_buff);
      }

      // step 1. send get update server info request
      if (OB_SUCCESS == ret)
      {
        ret = rpc_frame_->send_request(root_server, 
                                       OB_GET_UPDATE_SERVER_INFO, 
                                       DEFAULT_VERSION,
                                       timeout, data_buff);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "send request to root server for "
                           "get update server failed, ret=%d", ret);
        }
      }
    
      // step 2. deserialize restult code
      if (OB_SUCCESS == ret)
      {
        ret = result_code.deserialize(data_buff.get_data(), 
                                      data_buff.get_position(), pos);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "deserialize result_code failed, pos=%ld, ret=%d", 
                    pos, ret);
        }
        else
        {
          ret = result_code.result_code_;
          if (OB_SUCCESS != ret && OB_DATA_NOT_SERVE != ret)
          {
            TBSYS_LOG(WARN, "get response from root server failed, "
                            "timeout=%ld, ret=%d.", timeout, ret);
          }
        }
      }
    
      // step 3. deserialize update server addr
      if (OB_SUCCESS == ret)
      {
        ret = update_server.deserialize(data_buff.get_data(), 
                                        data_buff.get_position(), pos);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "deserialize update server failed, pos=%ld, ret=%d", 
                    pos, ret);
        }
      }

      return ret;
    }
    
    int ObServerRpc::scan(const ObServer& remote_server,
                          const ObScanParam& scan_param, 
                          ObScanner& scanner,
                          const int64_t timeout)
    {
      int ret     = OB_SUCCESS;
      int64_t pos = 0;
      ObResultCode result_code;
      ObDataBuffer data_buff;

      if (remote_server.get_ipv4() == 0 || remote_server.get_port() == 0
          || timeout <= 0)
      {
        TBSYS_LOG(WARN, "invalid param, ip=%d, port=%d, timeout=%ld",
                  remote_server.get_ipv4(), remote_server.get_port(),
                  timeout);
        ret = OB_ERROR;
      }
      else if (scan_param.get_scan_size() <= 0)
      {
        TBSYS_LOG(WARN, "invalid param, scan_size=%ld", 
                  scan_param.get_scan_size());
        ret = OB_ERROR;
      }
      else if (NULL == rpc_frame_)
      {
        TBSYS_LOG(WARN, "server rpc doesn't init.");
        ret = OB_ERROR;
      }

      if (OB_SUCCESS == ret)
      {
        ret = get_frame_buffer(data_buff);
      }

      // step 1. serialize scan param to data_buff
      if (OB_SUCCESS == ret)
      {
        ret = scan_param.serialize(data_buff.get_data(), 
                                   data_buff.get_capacity(), 
                                   data_buff.get_position());
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "serialize scan_param failed,ret=%d.", ret);
        }
      }
    
      // step 2. send request for scan
      if (OB_SUCCESS == ret)
      {
        ret = rpc_frame_->send_request(remote_server, 
                                       OB_SCAN_REQUEST, DEFAULT_VERSION, 
                                       timeout, data_buff);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "send request to remote server for scan failed, "
                          "ret=%d.", ret);
        }
      }
    
      // step 3. deserialize the response code
      if (OB_SUCCESS == ret)
      {
        ret = result_code.deserialize(data_buff.get_data(), 
                                      data_buff.get_position(), pos);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "deserialize result_code failed, pos=%ld, ret=%d.", 
                    pos, ret);
        }
        else
        {
          ret = result_code.result_code_;
          if (OB_SUCCESS != ret && OB_DATA_NOT_SERVE != ret)
          {
            TBSYS_LOG(WARN, "get response from remote server failed, "
                            "timeout=%ld, ret=%d.", timeout, ret);
          }
        }
      }

      // step 4. deserialize the scannner
      if (OB_SUCCESS == ret)
      {
        ret = scanner.deserialize(data_buff.get_data(), 
                                  data_buff.get_position(), pos);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "deserialize scanner from buff failed, "
                          "pos=%ld, ret=%d.", pos, ret);
        }
      }
    
      return ret;
    }
    
    int ObServerRpc::get(const ObServer& remote_server,
                         const ObGetParam& get_param, 
                         ObScanner& scanner,
                         const int64_t timeout)
    {
      int ret     = OB_SUCCESS;
      int64_t pos = 0;
      ObResultCode result_code;
      ObDataBuffer data_buff;

      if (remote_server.get_ipv4() == 0 || remote_server.get_port() == 0
          || timeout <= 0)
      {
        TBSYS_LOG(WARN, "invalid param, ip=%d, port=%d, timeout=%ld",
                  remote_server.get_ipv4(), remote_server.get_port(),
                  timeout);
        ret = OB_ERROR;
      }
      else if (get_param.get_cell_size() <= 0 || get_param.get_row_size() <= 0)
      {
        TBSYS_LOG(WARN, "invalid param, cell_size=%ld, row_size=%ld", 
                  get_param.get_cell_size(), get_param.get_row_size());
        ret = OB_ERROR;
      }
      else if (NULL == rpc_frame_)
      {
        TBSYS_LOG(WARN, "server rpc doesn't init.");
        ret = OB_ERROR;
      }

      if (OB_SUCCESS == ret)
      {
        ret = get_frame_buffer(data_buff);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "get frame buffer failed, ret=%d.", ret);
        }
      }
      
      // step 1. serialize get param to data_buff
      if (OB_SUCCESS == ret)
      {
        ret = get_param.serialize(data_buff.get_data(), 
                                  data_buff.get_capacity(), 
                                  data_buff.get_position());
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "serialize get_param failed, ret=%d.", ret);
        }
      }
    
      // step 2. send request for get
      if (OB_SUCCESS == ret)
      {
        ret = rpc_frame_->send_request(remote_server, 
                                       OB_GET_REQUEST, DEFAULT_VERSION, 
                                       timeout, data_buff);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "send request to remote server for get failed, "
                          "ret=%d.", ret);
        }
      }
    
      // step 3. deserialize the response code
      if (OB_SUCCESS == ret)
      {
        ret = result_code.deserialize(data_buff.get_data(), 
                                      data_buff.get_position(), pos);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "deserialize result_code failed,pos=%ld, ret=%d.", 
                    pos, ret);
        }
        else
        {
          ret = result_code.result_code_;
          if (OB_SUCCESS != ret && OB_DATA_NOT_SERVE != ret)
          {
            TBSYS_LOG(WARN, "get response from remote server failed, "
                            "timeout=%ld, ret=%d.", timeout, ret);
          }
        }
      }

      // step 4. deserialize the scanner
      if (OB_SUCCESS == ret)
      {
        ret = scanner.deserialize(data_buff.get_data(), 
                                  data_buff.get_position(), pos);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "deserialize scanner from buff failed, "
                          "pos=%ld, ret=%d.", pos, ret);
        }
      }
    
      return ret;
    }

    int ObServerRpc::ups_apply(const ObServer& update_server,
                               const ObMutator &mutator, 
                               const int64_t timeout)
    {
      int ret     = OB_SUCCESS;
      int64_t pos = 0;
      ObResultCode result_code;
      ObDataBuffer data_buff;

      if (update_server.get_ipv4() == 0 || update_server.get_port() == 0
          || timeout <= 0)
      {
        TBSYS_LOG(WARN, "invalid param, ip=%d, port=%d, timeout=%ld",
                  update_server.get_ipv4(), update_server.get_port(),
                  timeout);
        ret = OB_ERROR;
      }
      else if (NULL == rpc_frame_)
      {
        TBSYS_LOG(WARN, "server rpc doesn't init.");
        ret = OB_ERROR;
      }

      if (NULL == rpc_frame_)
      {
        TBSYS_LOG(WARN, "server rpc doesn't init.");
        ret = OB_ERROR;
      }

      if (OB_SUCCESS == ret)
      {
        ret = get_frame_buffer(data_buff);
      }

      // step 1. serialize mutator to data_buff
      if (OB_SUCCESS == ret)
      {
        ret = mutator.serialize(data_buff.get_data(), 
                                data_buff.get_capacity(), 
                                data_buff.get_position());
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "serialize get_param failed, ret=%d.", ret);
        }
      }

      // step 2. send request for apply
      if (OB_SUCCESS == ret)
      {
        ret = rpc_frame_->send_request(update_server, 
                                       OB_WRITE, DEFAULT_VERSION, 
                                       timeout, data_buff);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "send request to update server for apply failed, "
                          "timeout=%ld, ret=%d.", timeout, ret);
        }
      }

      // step 3. deserialize the response code
      if (OB_SUCCESS == ret)
      {
        ret = result_code.deserialize(data_buff.get_data(), 
                                      data_buff.get_position(), pos);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "deserialize result_code failed, pos=%ld, ret=%d.", 
                    pos, ret);
        }
        else
        {
          ret = result_code.result_code_;
        }
      }

      return ret;
    }
  } // end namespace client
} // end namespace oceanbase
