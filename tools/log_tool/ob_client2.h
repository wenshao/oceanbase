#include "common/ob_define.h"
#include "common/ob_result.h"
#include "common/utility.h"
#include "common/ob_packet_factory.h"
#include "common/ob_client_manager.h"

using namespace oceanbase::common;

struct Dummy
{
  int serialize(char* buf, int64_t len, int64_t& pos) const
  {
    UNUSED(buf); UNUSED(len); UNUSED(pos);
    return OB_SUCCESS;
  }
  int deserialize(char* buf, int64_t len, int64_t& pos)
  {
    UNUSED(buf); UNUSED(len); UNUSED(pos);
    return OB_SUCCESS;
  }
};

template <class T>
int uni_serialize(const T &data, char *buf, const int64_t data_len, int64_t& pos)
{
  return data.serialize(buf, data_len, pos);
};

template <class T>
int uni_deserialize(T &data, char *buf, const int64_t data_len, int64_t& pos)
{
  return data.deserialize(buf, data_len, pos);
};

template <>
int uni_serialize<uint64_t>(const uint64_t &data, char *buf, const int64_t data_len, int64_t& pos)
{
  return serialization::encode_vi64(buf, data_len, pos, (int64_t)data);
};

template <>
int uni_serialize<int64_t>(const int64_t &data, char *buf, const int64_t data_len, int64_t& pos)
{
  return serialization::encode_vi64(buf, data_len, pos, data);
};

template <>
int uni_deserialize<uint64_t>(uint64_t &data, char *buf, const int64_t data_len, int64_t& pos)
{
  return serialization::decode_vi64(buf, data_len, pos, (int64_t*)&data);
};

template <>
int uni_deserialize<int64_t>(int64_t &data, char *buf, const int64_t data_len, int64_t& pos)
{
  return serialization::decode_vi64(buf, data_len, pos, &data);
};

ObDataBuffer& __get_thread_buffer(ThreadSpecificBuffer& thread_buf, ObDataBuffer& buf)
{
  ThreadSpecificBuffer::Buffer* my_buffer = thread_buf.get_buffer();
  my_buffer->reset();
  buf.set_data(my_buffer->current(), my_buffer->remain());
  return buf;
}

template <class Input, class Output>
int send_request(ObClientManager* client_mgr, const ObServer& server,
                 const int32_t version, const int pcode, const Input &param, Output &result,
                 ObDataBuffer& buf, const int64_t timeout)
{
  int err = OB_SUCCESS;
  int64_t pos = 0;
  ObResultCode result_code;

  if (OB_SUCCESS != (err = uni_serialize(param, buf.get_data(), buf.get_capacity(), buf.get_position())))
  {
    TBSYS_LOG(ERROR, "serialize()=>%d", err);
  }
  else if (OB_SUCCESS != (err = client_mgr->send_request(server, pcode, version, timeout, buf)))
  {
    TBSYS_LOG(WARN, "failed to send request, ret=%d", err);
  }
  else if (OB_SUCCESS != (err = result_code.deserialize(buf.get_data(), buf.get_position(), pos)))
  {
    TBSYS_LOG(ERROR, "deserialize result_code failed:pos[%ld], ret[%d]", pos, err);
  }
  else if (OB_SUCCESS != (err = result_code.result_code_))
  {
    TBSYS_LOG(ERROR, "result_code.result_code = %d", err);
  }
  else if (OB_SUCCESS != (err = uni_deserialize(result, buf.get_data(), buf.get_position(), pos)))
  {
    TBSYS_LOG(ERROR, "deserialize result data failed:pos[%ld], ret[%d]", pos, err);
  }
  return err;
}

const int64_t DEFAULT_VERSION = 1;
const int64_t DEFAULT_TIMEOUT = 5 * 1000*1000;
class BaseClient
{
  public:
    const static int64_t MAX_N_TRANSPORT = 256;
    BaseClient(): n_transport_(0) {}
    virtual ~BaseClient(){}
  public:
    virtual int initialize(int64_t n_transport = 1) {
      int err = OB_SUCCESS;
      streamer_.setPacketFactory(&factory_);
      TBSYS_LOG(INFO, "client[n_transport=%ld]", n_transport);
      if (0 >= n_transport)
      {
        err = OB_INVALID_ARGUMENT;
      }
      for(int64_t i = 0; i < n_transport; i++)
      {
        if (OB_SUCCESS != (err = client_[i].initialize(&transport_[i], &streamer_)))
        {
          TBSYS_LOG(ERROR, "client_mgr.initialize()=>%d", err);
        }
        else if (!transport_[i].start())
        {
          err = OB_ERROR;
          TBSYS_LOG(ERROR, "transport.start()=>%d", err);
        }
      }
      if (OB_SUCCESS == err)
      {
        n_transport_ = n_transport;
      }
      return err;
    }

    virtual int destroy() {
      for(int64_t i = 0; i < n_transport_; i++)
      {
        transport_[i].stop();
      }
      return wait();
    }

    virtual int wait(){
      int err = OB_SUCCESS;
      for(int64_t i = 0; OB_SUCCESS == err && i < n_transport_; i++)
      {
        err = transport_[i].wait();
      }
      return err;
    }

    // ObClientManager * get_rpc() {
    //   return &client_;
    // }

    template <class Input, class Output>
    int send_request(const ObServer& server, const int pcode,
                     const Input &param, Output &result, int64_t id = 0,
                     const int32_t version=DEFAULT_VERSION, const int64_t timeout=DEFAULT_TIMEOUT) {
      ObDataBuffer buf;
      return ::send_request(&client_[id % n_transport_], server, version, pcode, param, result, __get_thread_buffer(rpc_buffer_, buf), timeout);
    }

    template <class Input, class Output>
    int send_request(const char* server_spec, const int pcode,
                     const Input &param, Output &result, int64_t id = 0,
                     const int32_t version=DEFAULT_VERSION, const int64_t timeout=DEFAULT_TIMEOUT) {
      int err = OB_SUCCESS;
      ObDataBuffer buf;
      ObServer server;
      if (OB_SUCCESS != (err = to_server(server, server_spec)))
      {
        TBSYS_LOG(ERROR, "invalid server(%s)", server_spec);
      }
      else if (OB_SUCCESS != (err = ::send_request(&client_[id % n_transport_], server, version, pcode, param, result, __get_thread_buffer(rpc_buffer_, buf), timeout)))
      {
      }
      return err;
    }
  protected:
    int64_t n_transport_;
    tbnet::DefaultPacketStreamer streamer_;
    ObPacketFactory factory_;
    tbnet::Transport transport_[MAX_N_TRANSPORT];
    ObClientManager client_[MAX_N_TRANSPORT];
    ThreadSpecificBuffer rpc_buffer_;
};
Dummy _dummy_;
