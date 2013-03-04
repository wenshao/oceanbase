#include "rootserver/ob_root_worker.h"
#include "rootserver/ob_root_rpc_stub.h"
#include "common/ob_schema.h"
#include "common/ob_define.h"
#include "common/ob_rs_ups_message.h"

using namespace oceanbase::rootserver;
using namespace oceanbase::common;

ObRootRpcStub::ObRootRpcStub()
  :thread_buffer_(NULL)
{      
}

ObRootRpcStub::~ObRootRpcStub()
{
}

int ObRootRpcStub::init(const ObClientManager *client_mgr, common::ThreadSpecificBuffer* tsbuffer)
{
  OB_ASSERT(NULL != client_mgr);
  OB_ASSERT(NULL != tsbuffer);
  ObCommonRpcStub::init(client_mgr);
  thread_buffer_ = tsbuffer;
  return OB_SUCCESS;
}

    int ObRootRpcStub::slave_register(const common::ObServer& master, const common::ObServer& slave_addr, common::ObFetchParam& fetch_param, const int64_t timeout)
    {
      int err = OB_SUCCESS;
      ObDataBuffer data_buff;

      if (NULL == client_mgr_)
      {
        TBSYS_LOG(WARN, "invalid status, client_mgr_[%p]", client_mgr_);
        err = OB_ERROR;
      }
      else
      {
        err = get_thread_buffer_(data_buff);
      }

      // step 1. serialize slave addr
      if (OB_SUCCESS == err)
      {
        err = slave_addr.serialize(data_buff.get_data(), data_buff.get_capacity(),
            data_buff.get_position());
      }

      // step 2. send request to register
      if (OB_SUCCESS == err)
      {
        err = client_mgr_->send_request(master, 
            OB_SLAVE_REG, DEFAULT_VERSION, timeout, data_buff);
        if (err != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "send request to register failed"
              "err[%d].", err);
        }
      }

      // step 3. deserialize the response code
      int64_t pos = 0;
      if (OB_SUCCESS == err)
      {
        ObResultCode result_code;
        err = result_code.deserialize(data_buff.get_data(), data_buff.get_position(), pos);
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(ERROR, "deserialize result_code failed:pos[%ld], err[%d].", pos, err);
        }
        else
        {
          err = result_code.result_code_;
        }
      }

      // step 3. deserialize fetch param
      if (OB_SUCCESS == err)
      {
        err = fetch_param.deserialize(data_buff.get_data(), data_buff.get_position(), pos);
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN, "deserialize fetch param failed, err[%d]", err);
        }
      }

      return err;
    }

int ObRootRpcStub::get_thread_buffer_(common::ObDataBuffer& data_buffer)
{
  int ret = OB_SUCCESS;
  if (NULL == thread_buffer_)
  {
    TBSYS_LOG(ERROR, "thread_buffer_ = NULL");
    ret = OB_ERROR;
  }
  else
  {
    common::ThreadSpecificBuffer::Buffer* buff = thread_buffer_->get_buffer();
    if (NULL == buff)
    {
      TBSYS_LOG(ERROR, "thread_buffer_ = NULL");
      ret = OB_ERROR;
    }
    else
    {
      buff->reset();
      data_buffer.set_data(buff->current(), buff->remain());
    }
  }
  return ret;
}

int ObRootRpcStub::set_obi_role(const common::ObServer& ups, const common::ObiRole& role, const int64_t timeout_us)
{
  int ret = OB_SUCCESS;
  ObDataBuffer msgbuf;
  
  if (NULL == client_mgr_)
  {
    TBSYS_LOG(ERROR, "client_mgr_=NULL");
    ret = OB_ERROR;
  }
  else if (OB_SUCCESS != (ret = get_thread_buffer_(msgbuf)))
  {
    TBSYS_LOG(ERROR, "failed to get thread buffer, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = role.serialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
  {
    TBSYS_LOG(WARN, "failed to serialize role, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = client_mgr_->send_request(ups, OB_SET_OBI_ROLE, DEFAULT_VERSION, timeout_us, msgbuf)))
  {
    TBSYS_LOG(WARN, "failed to send request, err=%d", ret);
  }
  else
  {
    ObResultCode result_code;
    int64_t pos = 0;
    if (OB_SUCCESS != (ret = result_code.deserialize(msgbuf.get_data(), msgbuf.get_position(), pos)))
    {
      TBSYS_LOG(ERROR, "failed to deserialize response, err=%d", ret);
    }
    else if (OB_SUCCESS != result_code.result_code_)
    {
      TBSYS_LOG(WARN, "failed to set obi role, err=%d", result_code.result_code_);
      ret = result_code.result_code_;
    }
  }
  return ret;
}

int ObRootRpcStub::switch_schema(const common::ObServer& ups, const common::ObSchemaManagerV2& schema_manager, const int64_t timeout_us)
{
  int ret = OB_SUCCESS;
  ObDataBuffer msgbuf;

  if (NULL == client_mgr_)
  {
    TBSYS_LOG(ERROR, "client_mgr_=NULL");
    ret = OB_ERROR;
  }
  else if (OB_SUCCESS != (ret = get_thread_buffer_(msgbuf)))
  {
    TBSYS_LOG(ERROR, "failed to get thread buffer, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = schema_manager.serialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
  {
    TBSYS_LOG(ERROR, "failed to serialize schema, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = client_mgr_->send_request(ups, OB_SWITCH_SCHEMA, DEFAULT_VERSION, timeout_us, msgbuf)))
  {
    TBSYS_LOG(WARN, "failed to send request, err=%d", ret);
  }
  else
  {
    ObResultCode result;
    int64_t pos = 0;
    if (OB_SUCCESS != (ret = result.deserialize(msgbuf.get_data(), msgbuf.get_position(), pos)))
    {
      TBSYS_LOG(ERROR, "failed to deserialize response, err=%d", ret);
    }
    else if (OB_SUCCESS != result.result_code_)
    {
      TBSYS_LOG(WARN, "failed to switch schema, err=%d", result.result_code_);
      ret = result.result_code_;
    }
    else
    {
      char server_buf[OB_IP_STR_BUFF];
      ups.to_string(server_buf, OB_IP_STR_BUFF);
      TBSYS_LOG(INFO, "send up_switch_schema, ups=%s schema_version=%ld", server_buf, schema_manager.get_version());
    }
  }
  return ret;
}

int ObRootRpcStub::migrate_tablet(const common::ObServer& src_cs, const common::ObServer& dest_cs, const common::ObRange& range, bool keey_src, const int64_t timeout_us)
{
  int ret = OB_SUCCESS;
  ObDataBuffer msgbuf;

  if (NULL == client_mgr_)
  {
    TBSYS_LOG(ERROR, "client_mgr_=NULL");
    ret = OB_ERROR;
  }
  else if (OB_SUCCESS != (ret = get_thread_buffer_(msgbuf)))
  {
    TBSYS_LOG(ERROR, "failed to get thread buffer, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = range.serialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
  {
    TBSYS_LOG(ERROR, "failed to serialize rage, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = dest_cs.serialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
  {
    TBSYS_LOG(ERROR, "failed to serialize dest_cs, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = common::serialization::encode_bool(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), keey_src)))
  {
    TBSYS_LOG(ERROR, "failed to serialize keey_src, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = client_mgr_->send_request(src_cs, OB_CS_MIGRATE, DEFAULT_VERSION, timeout_us, msgbuf)))
  {
    TBSYS_LOG(WARN, "failed to send request, err=%d", ret);
  }
  else
  {
    ObResultCode result;
    int64_t pos = 0;
    if (OB_SUCCESS != (ret = result.deserialize(msgbuf.get_data(), msgbuf.get_position(), pos)))
    {
      TBSYS_LOG(ERROR, "failed to deserialize response, err=%d", ret);
    }
    else if (OB_SUCCESS != result.result_code_)
    {
      TBSYS_LOG(WARN, "failed to migrate tablet, err=%d", result.result_code_);
      ret = result.result_code_;
    }
    else
    {
    }
  }
  return ret;
}

int ObRootRpcStub::create_tablet(const common::ObServer& cs, const common::ObRange& range, const int64_t mem_version, const int64_t timeout_us)
{
  int ret = OB_SUCCESS;
  ObDataBuffer msgbuf;
  static char buff[OB_MAX_PACKET_LENGTH];
  msgbuf.set_data(buff, OB_MAX_PACKET_LENGTH);
  if (NULL == client_mgr_)
  {
    TBSYS_LOG(ERROR, "client_mgr_=NULL");
    ret = OB_ERROR;
  }
  else if (OB_SUCCESS != (ret = range.serialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
  {
    TBSYS_LOG(ERROR, "failed to serialize range, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = common::serialization::encode_vi64(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), mem_version)))
  {
    TBSYS_LOG(ERROR, "failed to serialize key_src, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = client_mgr_->send_request(cs, OB_CS_CREATE_TABLE, DEFAULT_VERSION, timeout_us, msgbuf)))
  {
    TBSYS_LOG(WARN, "failed to send request, err=%d", ret);
  }
  else
  {
    ObResultCode result;
    int64_t pos = 0;
    static char range_buff[OB_MAX_ROW_KEY_LENGTH * 2];
    if (OB_SUCCESS != (ret = result.deserialize(msgbuf.get_data(), msgbuf.get_position(), pos)))
    {
      TBSYS_LOG(ERROR, "failed to deserialize response, err=%d", ret);
    }
    else if (OB_SUCCESS != result.result_code_)
    {
      range.to_string(range_buff, OB_MAX_ROW_KEY_LENGTH * 2);
      TBSYS_LOG(WARN, "failed to create tablet, err=%d, cs=%s, range=%s", result.result_code_, cs.to_cstring(), range_buff);
      ret = result.result_code_;
    }
    else
    {
    }
  }
  return ret;
}

int ObRootRpcStub::delete_tablets(const common::ObServer& cs, const common::ObTabletReportInfoList &tablets, const int64_t timeout_us)
{
  int ret = OB_SUCCESS;
  ObDataBuffer msgbuf;

  if (NULL == client_mgr_)
  {
    TBSYS_LOG(ERROR, "client_mgr_=NULL");
    ret = OB_ERROR;
  }
  else if (OB_SUCCESS != (ret = get_thread_buffer_(msgbuf)))
  {
    TBSYS_LOG(ERROR, "failed to get thread buffer, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = tablets.serialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
  {
    TBSYS_LOG(ERROR, "failed to serializ, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = client_mgr_->send_request(cs, OB_CS_DELETE_TABLETS, DEFAULT_VERSION, timeout_us, msgbuf)))
  {
    TBSYS_LOG(WARN, "failed to send request, err=%d", ret);
  }
  else
  {
    ObResultCode result;
    int64_t pos = 0;
    if (OB_SUCCESS != (ret = result.deserialize(msgbuf.get_data(), msgbuf.get_position(), pos)))
    {
      TBSYS_LOG(ERROR, "failed to deserialize response, err=%d", ret);
    }
    else if (OB_SUCCESS != result.result_code_)
    {
      TBSYS_LOG(WARN, "failed to delete tablets, err=%d", result.result_code_);
      ret = result.result_code_;
    }
  }
  return ret;
}
    
    int ObRootRpcStub::import_tablets(const common::ObServer& cs, const uint64_t table_id, const int64_t version, const int64_t timeout_us)
    {
      int ret = OB_SUCCESS;
      ObDataBuffer msgbuf;
      
      if (NULL == client_mgr_)
      {
        TBSYS_LOG(ERROR, "client_mgr_=NULL");
        ret = OB_ERROR;
      }
      else if (OB_SUCCESS != (ret = get_thread_buffer_(msgbuf)))
      {
        TBSYS_LOG(ERROR, "failed to get thread buffer, err=%d", ret);
      }
      else if (OB_SUCCESS != (ret = common::serialization::encode_vi64(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), table_id)))
      {
        TBSYS_LOG(ERROR, "failed to serialize keey_src, err=%d", ret);
      }
      else if (OB_SUCCESS != (ret = common::serialization::encode_vi64(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), version)))
      {
        TBSYS_LOG(ERROR, "failed to serialize keey_src, err=%d", ret);
      }
      else if (OB_SUCCESS != (ret = client_mgr_->send_request(cs, OB_CS_IMPORT_TABLETS, DEFAULT_VERSION, timeout_us, msgbuf)))
      {
        TBSYS_LOG(WARN, "failed to send request, err=%d", ret);
      }
      else
      {
        ObResultCode result;
        int64_t pos = 0;
        if (OB_SUCCESS != (ret = result.deserialize(msgbuf.get_data(), msgbuf.get_position(), pos)))
        {
          TBSYS_LOG(ERROR, "failed to deserialize response, err=%d", ret);
        }
        else if (OB_SUCCESS != result.result_code_)
        {
          TBSYS_LOG(WARN, "failed to create tablet, err=%d", result.result_code_);
          ret = result.result_code_;
        }
        else
        {
        }
      }
      return ret;
    }
    
int ObRootRpcStub::get_last_frozen_version(const common::ObServer& ups, const int64_t timeout_us, int64_t &frozen_version)
{
  int ret = OB_SUCCESS;
  ObDataBuffer msgbuf;
  frozen_version = -1;

  if (NULL == client_mgr_)
  {
    TBSYS_LOG(ERROR, "client_mgr_=NULL");
    ret = OB_ERROR;
  }
  else if (OB_SUCCESS != (ret = get_thread_buffer_(msgbuf)))
  {
    TBSYS_LOG(ERROR, "failed to get thread buffer, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = client_mgr_->send_request(ups, OB_UPS_GET_LAST_FROZEN_VERSION, DEFAULT_VERSION, timeout_us, msgbuf)))
  {
    TBSYS_LOG(WARN, "failed to send request, err=%d", ret);
  }
  else
  {
    ObResultCode result;
    int64_t pos = 0;
    if (OB_SUCCESS != (ret = result.deserialize(msgbuf.get_data(), msgbuf.get_position(), pos)))
    {
      TBSYS_LOG(ERROR, "failed to deserialize response, err=%d", ret);
    }
    else if (OB_SUCCESS != result.result_code_)
    {
      TBSYS_LOG(WARN, "failed to create tablet, err=%d", result.result_code_);
      ret = result.result_code_;
    }
    else if (OB_SUCCESS != (ret = serialization::decode_vi64(msgbuf.get_data(), msgbuf.get_position(), pos, &frozen_version)))
    {
      TBSYS_LOG(WARN, "failed to deserialize frozen version ,err=%d", ret);
      frozen_version = -1;
    }
    else
    {
      TBSYS_LOG(INFO, "last_frozen_version=%ld", frozen_version);
    }
  }
  return ret;
}

    int ObRootRpcStub::get_obi_role(const common::ObServer& master, const int64_t timeout_us, common::ObiRole &obi_role)
    {
      int ret = OB_SUCCESS;
      ObDataBuffer msgbuf;

      if (NULL == client_mgr_)
      {
        TBSYS_LOG(ERROR, "client_mgr_=NULL");
        ret = OB_ERROR;
      }
      else if (OB_SUCCESS != (ret = get_thread_buffer_(msgbuf)))
      {
        TBSYS_LOG(ERROR, "failed to get thread buffer, err=%d", ret);
      }
      else if (OB_SUCCESS != (ret = client_mgr_->send_request(master, OB_GET_OBI_ROLE, DEFAULT_VERSION, timeout_us, msgbuf)))
      {
        TBSYS_LOG(WARN, "failed to send request, err=%d", ret);
      }
      else
      {
        ObResultCode result;
        int64_t pos = 0;
        if (OB_SUCCESS != (ret = result.deserialize(msgbuf.get_data(), msgbuf.get_position(), pos)))
        {
          TBSYS_LOG(ERROR, "failed to deserialize response, err=%d", ret);
        }
        else if (OB_SUCCESS != result.result_code_)
        {
          TBSYS_LOG(WARN, "failed to get obi_role, err=%d", result.result_code_);
          ret = result.result_code_;
        }
        else if (OB_SUCCESS != (ret = obi_role.deserialize(msgbuf.get_data(), msgbuf.get_position(), pos)))
        {
          TBSYS_LOG(WARN, "failed to deserialize frozen version ,err=%d", ret);
        }
        else
        {
          TBSYS_LOG(INFO, "get obi_role from master, obi_role=%s", obi_role.get_role_str());
        }
      }
      return ret;      
    }
    
int ObRootRpcStub::heartbeat_to_cs(
    const common::ObServer& cs, 
    const int64_t lease_time, 
    const int64_t frozen_mem_version, 
    const int64_t schema_version,
    const int64_t config_version)
{
  int ret = OB_SUCCESS;
  static const int MY_VERSION = 3;
  ObDataBuffer msgbuf;

  UNUSED(config_version);

  if (NULL == client_mgr_)
  {
    TBSYS_LOG(ERROR, "client_mgr_=NULL");
    ret = OB_ERROR;
  }
  else if (OB_SUCCESS != (ret = get_thread_buffer_(msgbuf)))
  {
    TBSYS_LOG(ERROR, "failed to get thread buffer, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = common::serialization::encode_vi64(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), lease_time)))
  {
    TBSYS_LOG(ERROR, "failed to serialize, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = common::serialization::encode_vi64(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), frozen_mem_version)))
  {
    TBSYS_LOG(ERROR, "failed to serialize, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = common::serialization::encode_vi64(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), schema_version)))
  {
    TBSYS_LOG(ERROR, "failed to serialize, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = client_mgr_->post_request(cs, OB_REQUIRE_HEARTBEAT, MY_VERSION, msgbuf)))
  {
    TBSYS_LOG(WARN, "failed to send request, err=%d", ret);
  }
  else
  {
    // success
  }
  return ret;
}

int ObRootRpcStub::heartbeat_to_ms(
    const common::ObServer& ms, 
    const int64_t lease_time, 
    const int64_t schema_version, 
    const common::ObiRole &role,
    const int64_t config_version)
{
  int ret = OB_SUCCESS;
  ObDataBuffer msgbuf;
  /*
   * VERSION UPDATE LOG:
   *  - 2012/7/20 xiaochu.yh: add config_version, update MY_VERSION from 3 to 4
   */
  static const int MY_VERSION = 4;

  if (NULL == client_mgr_)
  {
    TBSYS_LOG(ERROR, "client_mgr_=NULL");
    ret = OB_ERROR;
  }
  else if (OB_SUCCESS != (ret = get_thread_buffer_(msgbuf)))
  {
    TBSYS_LOG(ERROR, "failed to get thread buffer, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = common::serialization::encode_vi64(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), lease_time)))
  {
    TBSYS_LOG(ERROR, "failed to serialize, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = common::serialization::encode_vi64(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), schema_version)))
  {
    TBSYS_LOG(ERROR, "failed to serialize, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = role.serialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
  {
    TBSYS_LOG(ERROR, "failed to serialize, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = common::serialization::encode_vi64(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), config_version)))
  {
    TBSYS_LOG(ERROR, "failed to serialize, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = client_mgr_->post_request(ms, OB_REQUIRE_HEARTBEAT, MY_VERSION, msgbuf)))
  {
    TBSYS_LOG(WARN, "failed to send request, err=%d", ret);
  }
  else
  {
    // success
  }
  return ret;
}

int ObRootRpcStub::grant_lease_to_ups(const common::ObServer& ups,
                                      const common::ObServer& master, const int64_t lease,
                                      const ObiRole &obi_role,
                                      const int64_t config_version)
{
  int ret = OB_SUCCESS;
  ObDataBuffer msgbuf;
  ObMsgUpsHeartbeat msg;
  msg.ups_master_ = master;
  msg.self_lease_ = lease;
  msg.obi_role_ = obi_role;
  
  UNUSED(config_version);

  if (NULL == client_mgr_)
  {
    TBSYS_LOG(ERROR, "client_mgr_=NULL");
    ret = OB_ERROR;
  }
  else if (OB_SUCCESS != (ret = get_thread_buffer_(msgbuf)))
  {
    TBSYS_LOG(ERROR, "failed to get thread buffer, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = msg.serialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
  {
    TBSYS_LOG(ERROR, "failed to serialize msg, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = client_mgr_->post_request(ups, OB_RS_UPS_HEARTBEAT, msg.MY_VERSION, msgbuf)))
  {
    TBSYS_LOG(WARN, "failed to send request, err=%d", ret);
  }
  else
  {
    // success
  }
  return ret;
}

int ObRootRpcStub::request_report_tablet(const common::ObServer& chunkserver)
{
  int ret = OB_SUCCESS;
  ObDataBuffer msgbuf;
  if (NULL == client_mgr_)
  {
    TBSYS_LOG(ERROR, "client_mgr_=NULL");
    ret = OB_ERROR;
  }
  if (OB_SUCCESS == ret)
  {
    ret = get_thread_buffer_(msgbuf);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "fail to get thread buffer. err=%d", ret);
    }
  }
  if (OB_SUCCESS == ret)
  {
    ret = client_mgr_->post_request(chunkserver, OB_RS_REQUEST_REPORT_TABLET, DEFAULT_VERSION, msgbuf);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "fail to post request to chunkserver. err=%d, chunkserver_addr=%s", ret, chunkserver.to_cstring());
    }
  }
  return ret;
}
    int ObRootRpcStub::revoke_ups_lease(const common::ObServer& ups, const int64_t lease, const common::ObServer& master, const int64_t timeout_us)
    {
      int ret = OB_SUCCESS;
      ObDataBuffer msgbuf;
      ObMsgRevokeLease msg;
      msg.lease_ = lease;
      msg.ups_master_ = master;
      
      if (NULL == client_mgr_)
      {
        TBSYS_LOG(ERROR, "client_mgr_=NULL");
        ret = OB_ERROR;
      }
      else if (OB_SUCCESS != (ret = get_thread_buffer_(msgbuf)))
      {
        TBSYS_LOG(ERROR, "failed to get thread buffer, err=%d", ret);
      }
      else if (OB_SUCCESS != (ret = msg.serialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
      {
        TBSYS_LOG(ERROR, "failed to serialize, err=%d", ret);
      }
      else if (OB_SUCCESS != (ret = client_mgr_->send_request(ups, OB_RS_UPS_REVOKE_LEASE, msg.MY_VERSION, timeout_us, msgbuf)))
      {
        TBSYS_LOG(WARN, "failed to send request, err=%d", ret);
      }
      else
      {
        // success
        ObResultCode result;
        int64_t pos = 0;
        if (OB_SUCCESS != (ret = result.deserialize(msgbuf.get_data(), msgbuf.get_position(), pos)))
        {
          TBSYS_LOG(ERROR, "failed to deserialize response, err=%d", ret);
        }
        else if (OB_SUCCESS != result.result_code_)
        {
          TBSYS_LOG(WARN, "failed to revoke lease, err=%d", result.result_code_);
          ret = result.result_code_;
        }
        else
        {
        }
      }
      return ret;
    }

int ObRootRpcStub::get_ups_max_log_seq(const common::ObServer& ups, uint64_t &max_log_seq, const int64_t timeout_us)
{
  int ret = OB_SUCCESS;
  ObDataBuffer msgbuf;      
  if (NULL == client_mgr_)
  {
    TBSYS_LOG(ERROR, "client_mgr_=NULL");
    ret = OB_ERROR;
  }
  else if (OB_SUCCESS != (ret = get_thread_buffer_(msgbuf)))
  {
    TBSYS_LOG(ERROR, "failed to get thread buffer, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = client_mgr_->send_request(ups, OB_RS_GET_MAX_LOG_SEQ, DEFAULT_VERSION, timeout_us, msgbuf)))
  {
    TBSYS_LOG(WARN, "failed to send request, err=%d", ret);
  }
  else
  {
    // success
    ObResultCode result;
    int64_t pos = 0;
    if (OB_SUCCESS != (ret = result.deserialize(msgbuf.get_data(), msgbuf.get_position(), pos)))
    {
      TBSYS_LOG(ERROR, "failed to deserialize response, err=%d", ret);
    }
    else if (OB_SUCCESS != result.result_code_)
    {
      TBSYS_LOG(WARN, "failed to revoke lease, err=%d", result.result_code_);
      ret = result.result_code_;
    }
    else if (OB_SUCCESS != (ret = serialization::decode_vi64(msgbuf.get_data(), msgbuf.get_position(), 
                                                             pos, (int64_t*)&max_log_seq)))
    {
      TBSYS_LOG(WARN, "failed to deserialize, err=%d", ret);
    }
    else
    {
      TBSYS_LOG(INFO, "get ups max log seq, ups=%s seq=%lu", ups.to_cstring(), max_log_seq);
    }
  }
  return ret;

}

int ObRootRpcStub::shutdown_cs(const common::ObServer& cs, bool is_restart, const int64_t timeout_us)
{
  int ret = OB_SUCCESS;
  ObDataBuffer msgbuf;      
  if (NULL == client_mgr_)
  {
    TBSYS_LOG(ERROR, "client_mgr_=NULL");
    ret = OB_ERROR;
  }
  else if (OB_SUCCESS != (ret = get_thread_buffer_(msgbuf)))
  {
    TBSYS_LOG(ERROR, "failed to get thread buffer, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::encode_i32(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), is_restart ? 1 : 0)))
  {
    TBSYS_LOG(ERROR, "encode is_restart fail:ret[%d], is_restart[%d]", ret, is_restart ? 1 : 0);
  }
  else if (OB_SUCCESS != (ret = client_mgr_->send_request(cs, OB_STOP_SERVER, DEFAULT_VERSION, timeout_us, msgbuf)))
  {
    TBSYS_LOG(WARN, "failed to send request, err=%d", ret);
  }
  else
  {
    // success
    ObResultCode result;
    int64_t pos = 0;
    if (OB_SUCCESS != (ret = result.deserialize(msgbuf.get_data(), msgbuf.get_position(), pos)))
    {
      TBSYS_LOG(ERROR, "failed to deserialize response, err=%d", ret);
    }
    else if (OB_SUCCESS != result.result_code_)
    {
      TBSYS_LOG(WARN, "failed to restart, err=%d server=%s", result.result_code_, cs.to_cstring());
      ret = result.result_code_;
    }
  }
  return ret;
}
int ObRootRpcStub::get_split_range(const common::ObServer& ups, const int64_t timeout_us,
    const uint64_t table_id, const int64_t forzen_version, ObTabletInfoList &tablets)
{
  int ret = OB_SUCCESS;
  ObDataBuffer msgbuf;
  if (NULL == client_mgr_)
  {
    TBSYS_LOG(ERROR, "client_mgr_=NULL");
    ret = OB_ERROR;
  }
  if (OB_SUCCESS == ret)
  {
    if (OB_SUCCESS != (ret = get_thread_buffer_(msgbuf)))
    {
      TBSYS_LOG(ERROR, "failed to get thread buffer, err=%d", ret);
    }
  }
  if (OB_SUCCESS == ret)
  {
    if (OB_SUCCESS != (ret = serialization::encode_vi64(msgbuf.get_data(), msgbuf.get_capacity(),
            msgbuf.get_position(), forzen_version)))
    {
      TBSYS_LOG(WARN, "fail to encode forzen_version. forzen_version=%ld, ret=%d", forzen_version, ret);
    }
  }
  if (OB_SUCCESS == ret)
  {
    if (OB_SUCCESS != (ret = serialization::encode_vi64(msgbuf.get_data(), msgbuf.get_capacity(),
            msgbuf.get_position(), table_id)))
    {
      TBSYS_LOG(WARN, "fail to encode table_id. table_id=%lu, ret=%d", table_id, ret);
    }
  }
  if (OB_SUCCESS == ret)
  {
    if (OB_SUCCESS != (ret = client_mgr_->send_request(ups, OB_RS_FETCH_SPLIT_RANGE, DEFAULT_VERSION, timeout_us, msgbuf)))
    {
      TBSYS_LOG(WARN, "failed to send request, err=%d", ret);
    }
  }
  ObResultCode result;
  int64_t pos = 0;
  if (OB_SUCCESS == ret)
  {
    if (OB_SUCCESS != (ret = result.deserialize(msgbuf.get_data(), msgbuf.get_position(), pos)))
    {
      TBSYS_LOG(ERROR, "failed to deserialize response, err=%d", ret);
    }
    else if (OB_SUCCESS != result.result_code_)
    {
      TBSYS_LOG(WARN, "failed to fetch split range, err=%d", result.result_code_);
      ret = result.result_code_;
    }
  }
  if (OB_SUCCESS == ret)
  {
    if (OB_SUCCESS != (ret = tablets.deserialize(msgbuf.get_data(), msgbuf.get_position(), pos)))
    {
      TBSYS_LOG(WARN, "failed to deserialize tablets, err=%d", ret);
    }
  }
  if (OB_SUCCESS == ret)
  {
    TBSYS_LOG(INFO, "fetch split range from ups succ.");
  }
  else
  {
    TBSYS_LOG(WARN, "fetch split range from ups fail, ups_addr=%s, version=%ld", ups.to_cstring(), forzen_version);
  }
  return ret;
}
int ObRootRpcStub::table_exist_in_cs(const ObServer &cs, const int64_t timeout_us,
    const uint64_t table_id, bool &is_exist_in_cs)
{
  int ret = OB_SUCCESS;
  ObDataBuffer msgbuf;
  if (NULL == client_mgr_)
  {
    TBSYS_LOG(ERROR, "client_mgr_=NULL");
    ret = OB_ERROR;
  }
  if (OB_SUCCESS == ret)
  {
    if (OB_SUCCESS != (ret = get_thread_buffer_(msgbuf)))
    {
      TBSYS_LOG(ERROR, "failed to get thread buffer, err=%d", ret);
    }
  }
  if (OB_SUCCESS == ret)
  {
    if (OB_SUCCESS != (ret = serialization::encode_vi64(msgbuf.get_data(), msgbuf.get_capacity(),
            msgbuf.get_position(), table_id)))
    {
      TBSYS_LOG(WARN, "fail to encode table_id, table_id=%ld, ret=%d", table_id, ret);
    }
  }
  if (OB_SUCCESS == ret)
  {
    if (OB_SUCCESS != (ret = client_mgr_->send_request(cs, OB_CS_CHECK_TABLET, DEFAULT_VERSION, timeout_us, msgbuf)))
    {
      TBSYS_LOG(WARN, "failed to send request, err=%d", ret);
    }
  }
  ObResultCode result;
  int64_t pos = 0;
  if (OB_SUCCESS == ret)
  {
    if (OB_SUCCESS != (ret = result.deserialize(msgbuf.get_data(), msgbuf.get_position(), pos)))
    {
      TBSYS_LOG(ERROR, "failed to deserialize response, err=%d", ret);
    }
  }
  if (OB_SUCCESS == ret)
  {
    if (OB_CS_TABLET_NOT_EXIST == result.result_code_)
    {
      ret = OB_SUCCESS;
      is_exist_in_cs = false;
    }
    else if (OB_SUCCESS == result.result_code_)
    {
      is_exist_in_cs = true;
    }
    else
    {
      ret = result.result_code_;
      TBSYS_LOG(WARN, "fail to check cs tablet. table_id=%lu, cs_addr=%s, err=%d",
          table_id, cs.to_cstring(), ret);
    }
  }
  return ret;
}

