#include "ob_packet.h"
#include "ob_thread_mempool.h"

namespace oceanbase
{
  namespace common
  {
    ObVarMemPool ObPacket::out_mem_pool_(OB_MAX_PACKET_LENGTH);

    ObPacket::ObPacket() : no_free_(false), api_version_(0), timeout_(0), data_length_(0),  
    priority_(NORMAL_PRI), target_id_(0), receive_ts_(0), session_id_(0), connection_(NULL), alloc_inner_mem_(false)
    {
    }

    ObPacket::~ObPacket()
    {
      if (alloc_inner_mem_)
      {
        out_mem_pool_.free(inner_buffer_.get_data());
      }
    }

    void ObPacket::free()
    {
      if (!no_free_)
      {
        delete this;
      }
    }

    void ObPacket::set_no_free()
    {
      no_free_ = true;
    }

    int32_t ObPacket::get_packet_code()
    {
      // tbnet::Packet method
      return getPCode();
    }

    void ObPacket::set_packet_code(const int32_t packet_code)
    {
      // tbnet::Packet method
      setPCode(packet_code);
    }

    int32_t ObPacket::get_target_id() const
    {
      return target_id_;
    }

    void ObPacket::set_target_id(const int32_t target_id)
    {
      target_id_ = target_id;
    }

    int64_t ObPacket::get_session_id() const
    {
      return session_id_;
    }

    void ObPacket::set_session_id(const int64_t session_id)
    {
      session_id_ = session_id;
    }

    int32_t ObPacket::get_api_version() const
    {
      return api_version_;
    }

    void ObPacket::set_api_version(const int32_t api_version)
    {
      api_version_ = api_version;
    }

    void ObPacket::set_data(const ObDataBuffer& buffer)
    {
      buffer_ = buffer;
    }

    ObDataBuffer* ObPacket::get_buffer()
    {
      return &buffer_;
    }

    tbnet::Connection* ObPacket::get_connection() const
    {
      return connection_;
    }

    void ObPacket::set_connection(tbnet::Connection* connection)
    {
      connection_ = connection;
    }

    int ObPacket::serialize()
    {
      int ret = do_check_sum();

      if (ret == OB_SUCCESS && target_id_ != OB_SELF_FLAG)
      {
        int64_t buf_size = header_.get_serialize_size();
        buf_size += buffer_.get_position();
        char* buff = (char*)out_mem_pool_.malloc(buf_size);
        if (buff == NULL)
        {
          ret = OB_MEM_OVERFLOW;
          TBSYS_LOG(ERROR, "alloc memory from out packet pool failed, buf size: %ld, errno: %d", buf_size, errno);
        }
        else
        {
          inner_buffer_.set_data(buff, buf_size);
          alloc_inner_mem_ = true;
        }
      }

      if (ret == OB_SUCCESS)
      {
        ret = header_.serialize(inner_buffer_.get_data(), inner_buffer_.get_capacity(), inner_buffer_.get_position());
      }

      if (ret == OB_SUCCESS)
      {
        if (inner_buffer_.get_remain() >= buffer_.get_position())
        {
          int64_t& current_position = inner_buffer_.get_position();
          memcpy(inner_buffer_.get_data() + inner_buffer_.get_position(), buffer_.get_data(), buffer_.get_position());
          current_position += buffer_.get_position();
        }
        else
        {
          ret = OB_ERROR;
        }
      }

      return ret;
    }

    int ObPacket::deserialize()
    {
      int ret = OB_SUCCESS;

      buffer_.set_data(inner_buffer_.get_data(), inner_buffer_.get_capacity());

      ret = header_.deserialize(buffer_.get_data(), buffer_.get_capacity(), buffer_.get_position());

      if (ret == OB_SUCCESS)
      {
        ret = do_sum_check();
      }

      // buffer_'s position now point to the user data
      return ret;
    }

    int ObPacket::do_check_sum()
    {
      int ret = OB_SUCCESS;

      header_.set_magic_num(OB_PACKET_CHECKSUM_MAGIC);
      header_.header_length_ = static_cast<int16_t>(header_.get_serialize_size());
      header_.version_ = 0;
      header_.reserved_ = 0;

      header_.data_length_ = static_cast<int32_t>(buffer_.get_position());
      header_.data_zlength_ = header_.data_length_; // not compressed

      header_.data_checksum_ = common::ob_crc64(buffer_.get_data(), buffer_.get_position());
      header_.set_header_checksum();

      return ret;
    }

    int ObPacket::do_sum_check()
    {
      int ret = OB_SUCCESS;

      ret = header_.check_magic_num(OB_PACKET_CHECKSUM_MAGIC);

      if (ret == OB_SUCCESS)
      {
        ret = header_.check_header_checksum();
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(WARN, "packet header check failed");
        }
      }
      else
      {
        TBSYS_LOG(WARN, "packet magic number check failed");
      }

      if (ret == OB_SUCCESS)
      {
        char* body_start = buffer_.get_data() + buffer_.get_position();
        int64_t body_length = header_.data_length_;

        ret = header_.check_check_sum(body_start, body_length);
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(WARN, "packet body checksum invalid");
        }
      }

      return ret;
    }

    bool ObPacket::encode(tbnet::DataBuffer* output)
    {
      output->writeInt32(api_version_);
      output->writeInt64(session_id_);
      output->writeInt32(timeout_);

      const char* data = inner_buffer_.get_data();
      int size = static_cast<int>(inner_buffer_.get_position());
      output->writeBytes(data, size);
      return true;
    }

    bool ObPacket::decode(tbnet::DataBuffer* input, tbnet::PacketHeader* header)
    {
      bool rc = false;

      int length = header->_dataLen;
      api_version_ = input->readInt32();
      length -= 4;
      session_id_ = input->readInt64();
      length -= 8;
      timeout_ = input->readInt32();
      length -= 4;

      // real data length
      data_length_ = static_cast<int32_t>(length - header_.get_serialize_size());

      // inner_buffer_ is set when this packet is created
      // see ObPacketFactory.createPacket
      if (inner_buffer_.get_remain() >= length)
      {
        rc = input->readBytes(inner_buffer_.get_data(), length);
        if (rc)
        {
          inner_buffer_.get_position() = length;
        }
      }
      else
      {
        TBSYS_LOG(ERROR, "innert buffer is not enough, need: %d, real: %ld", length, inner_buffer_.get_remain());
      }

      receive_ts_ = tbsys::CTimeUtil::getTime();

      return rc;
    }

    int32_t ObPacket::get_data_length() const
    {
      return data_length_;
    }

    void ObPacket::set_receive_ts(const int64_t receive_ts)
    {
      receive_ts_ = receive_ts;
    }

    int64_t ObPacket::get_receive_ts() const
    {
      return receive_ts_;
    }

    void ObPacket::set_packet_priority(const int32_t priority)
    {
      if (priority == NORMAL_PRI || priority == LOW_PRI)
      {
        priority_ = priority;
      }
      else
      {
        TBSYS_LOG(WARN, "invalid packet priority: %d", priority);
      }
    }

    int32_t ObPacket::get_packet_priority() const
    {
      return priority_;
    }

    void ObPacket::set_source_timeout(const int64_t& timeout)
    {
      timeout_ = static_cast<int32_t>(timeout);
    }
    int64_t ObPacket::get_source_timeout() const
    {
      return timeout_;
    }

    void ObPacket::set_packet_buffer_offset(const int64_t buffer_offset)
    {
      buffer_offset_ = buffer_offset;
    }

    ObDataBuffer* ObPacket::get_packet_buffer()
    {
      return &inner_buffer_;
    }

    void ObPacket::set_packet_buffer(char* buffer, const int64_t buffer_length)
    {
      inner_buffer_.set_data(buffer, buffer_length);
    }
  } /* common */
} /* oceanbase */
