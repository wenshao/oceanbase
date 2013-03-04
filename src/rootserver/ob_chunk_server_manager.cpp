/*===============================================================
*   (C) 2007-2010 Taobao Inc.
*
*
*   Version: 0.1 2010-09-26
*
*   Authors:
*          daoan(daoan@taobao.com)
*
*
================================================================*/
#include <tblog.h>

#include "rootserver/ob_chunk_server_manager.h"
#include "common/file_utils.h"
#include "common/ob_malloc.h"
#include "common/ob_record_header.h"

namespace oceanbase
{
  namespace rootserver
  {
    using namespace common;
    ObBalanceInfo::ObBalanceInfo()
      :table_sstable_total_size_(0),
       table_sstable_count_(0),
       curr_migrate_in_num_(0),
       curr_migrate_out_num_(0)
    {
    }

    ObBalanceInfo::~ObBalanceInfo()
    {
      reset();
    }

    void ObBalanceInfo::reset()
    {
      reset_for_table();
      curr_migrate_in_num_ = 0;
      curr_migrate_out_num_ = 0;
      migrate_to_.reset();
    }

    void ObBalanceInfo::reset_for_table()
    {
      table_sstable_count_ = 0;
      table_sstable_total_size_ = 0;
    }

    ////////////////////////////////////////////////////////////////
    ObServerStatus::ObServerStatus()
      :last_hb_time_(0),last_hb_time_ms_(0),ms_status_(STATUS_DEAD), status_(STATUS_DEAD), port_cs_(0), port_ms_(0),
       hb_retry_times_(0), register_time_(0), wait_restart_(false), can_restart_(false)
    {
    }
    void ObServerStatus::set_hb_time(int64_t hb_t)
    {
      last_hb_time_ = hb_t;
      hb_retry_times_ = 0;
    }

    void ObServerStatus::set_hb_time_ms(int64_t hb_t)
    {
      last_hb_time_ms_ = hb_t;
      //TBSYS_LOG(DEBUG,"last_hb_time_ms_:%ld",last_hb_time_ms_);
    }

    bool ObServerStatus::is_alive(int64_t now, int64_t lease) const
    {
      return now - last_hb_time_ < lease;
    }

    bool ObServerStatus::is_ms_alive(int64_t now, int64_t lease) const
    {
      // TBSYS_LOG(DEBUG,"now:%ld,last_hb_time_ms_:%ld,lease:%ld",now,last_hb_time_ms_,lease);
      return now - last_hb_time_ms_ < lease;
    }

    const char* ObServerStatus::get_cs_stat_str() const
    {
      const char* ret = "ERR";
      switch(status_)
      {
        case STATUS_DEAD:
          ret = "DEAD";
          break;
        case STATUS_WAITING_REPORT:
          ret = "WAIT";
          break;
        case STATUS_SERVING:
          ret = "SERV";
          break;
        case STATUS_REPORTING:
          ret = "REPORT";
          break;
        case STATUS_REPORTED:
          ret = "REPORTED";
          break;
        case STATUS_SHUTDOWN:
          ret = "SHUTDOWN";
          break;
        default:
          break;
      }
      return ret;
    }

    void ObServerStatus::dump(const int32_t index) const
    {
      char ip_str[OB_IP_STR_BUFF];
      server_.to_string(ip_str,OB_IP_STR_BUFF);
      TBSYS_LOG(INFO, "index = %d server %s  status %d ms_status %d last_hb %ld port_cs %d port_ms %d hb_ms=%ld register=%ld",
                index, ip_str, status_, ms_status_,last_hb_time_, port_cs_, port_ms_, last_hb_time_ms_, register_time_);
      disk_info_.dump();
    }

    DEFINE_SERIALIZE(ObServerStatus)
    {
      int ret = 0;
      int64_t tmp_pos = pos;
      if (OB_SUCCESS == ret)
      {
        ret = serialization::encode_vi64(buf, buf_len, tmp_pos, last_hb_time_);
      }
      if (OB_SUCCESS == ret)
      {
        int64_t tmp_status = ms_status_;
        tmp_status <<= 32;
        tmp_status |= status_;

        ret = serialization::encode_vi64(buf, buf_len, tmp_pos, tmp_status);
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::encode_vi32(buf, buf_len, tmp_pos, port_cs_);
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::encode_vi32(buf, buf_len, tmp_pos, port_ms_);
      }
      if (OB_SUCCESS == ret)
      {
        ret = server_.serialize(buf, buf_len, tmp_pos);
      }
      if (OB_SUCCESS == ret)
      {
        ret = disk_info_.serialize(buf, buf_len, tmp_pos);
      }
      if (OB_SUCCESS == ret)
      {
        pos = tmp_pos;
      }
      return ret;
    }
    DEFINE_DESERIALIZE(ObServerStatus)
    {
      int ret = OB_SUCCESS;
      int64_t tmp_pos = pos;
      if (OB_SUCCESS == ret)
      {
        ret = serialization::decode_vi64(buf, data_len, tmp_pos, const_cast<int64_t*>(&last_hb_time_));
      }
      if (OB_SUCCESS == ret)
      {
        int64_t tmp_status = 0;
        ret = serialization::decode_vi64(buf, data_len, tmp_pos, &tmp_status);
        if (OB_SUCCESS == ret)
        {
          status_ = static_cast<int32_t>(tmp_status & 0xffffffff);
          ms_status_ = static_cast<int32_t>(tmp_status >> 32);
        }
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::decode_vi32(buf, data_len, tmp_pos, &port_cs_);
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::decode_vi32(buf, data_len, tmp_pos, &port_ms_);
      }
      if (OB_SUCCESS == ret)
      {
        ret = server_.deserialize(buf, data_len, tmp_pos);
      }
      if (OB_SUCCESS == ret)
      {
        ret = disk_info_.deserialize(buf, data_len, tmp_pos);
      }

      if (OB_SUCCESS == ret)
      {
        pos = tmp_pos;
      }
      return ret;
    }
    DEFINE_GET_SERIALIZE_SIZE(ObServerStatus)
    {
      int64_t len = 0;
      len += serialization::encoded_length_vi64(last_hb_time_);
      int64_t tmp_status = ms_status_;
      tmp_status <<= 32;
      tmp_status |= status_;
      len += serialization::encoded_length_vi64(tmp_status);
      len += serialization::encoded_length_vi32(port_cs_);
      len += serialization::encoded_length_vi32(port_ms_);
      len += server_.get_serialize_size();
      len += disk_info_.get_serialize_size();
      return len;
    }
    ObChunkServerManager::ObChunkServerManager()
    {
      servers_.init(MAX_SERVER_COUNT, data_holder_);
    }
    ObChunkServerManager::~ObChunkServerManager()
    {
    }
    ObChunkServerManager::iterator ObChunkServerManager::begin()
    {
      return servers_.get_base_address();
    }
    ObChunkServerManager::const_iterator ObChunkServerManager::begin() const
    {
      return servers_.get_base_address();
    }
    ObChunkServerManager::iterator ObChunkServerManager::end()
    {
      return servers_.get_base_address() +
        servers_.get_array_index();
    }
    ObChunkServerManager::const_iterator ObChunkServerManager::end() const
    {
      return servers_.get_base_address() +
        servers_.get_array_index();
    }
    ObChunkServerManager::iterator ObChunkServerManager::find_by_ip(const ObServer& server)
    {
      iterator res = end();
      for (int64_t i = 0; i < servers_.get_array_index(); i++)
      {
        if (servers_.at(i) == NULL)
        {
          TBSYS_LOG(ERROR, "never reach this, bugs");
        }
        else
        {
          //assert(servers_.at(i) != NULL);
          if (!server.compare_by_ip((servers_.at(i))->server_) && !(servers_.at(i))->server_.compare_by_ip(server))
          {
            res = servers_.at(i);
            break;
          }
        }
      }
      return res;
    }
    ObChunkServerManager::const_iterator ObChunkServerManager::find_by_ip(const ObServer& server) const
    {
      //TBSYS_LOG(DEBUG, "serid = %lu", server.get_ipv4_server_id());
      const_iterator res = end();
      for (int64_t i = 0; i < servers_.get_array_index(); i++)
      {
        if (servers_.at(i) == NULL)
        {
          TBSYS_LOG(ERROR, "never reach this, bugs");
        }
        else
        {
          //assert(servers_.at(i) != NULL);
          if (!server.compare_by_ip((servers_.at(i))->server_) && !(servers_.at(i))->server_.compare_by_ip(server))
          {
            res = servers_.at(i);
            break;
          }
        }
      }
      return res;
    }
    /*
     * root server will call this when a server regist to root or echo heart beat
     * @return 1 new serve 2 relive server 0 heartbt
     */
    int ObChunkServerManager::receive_hb(const ObServer& server, int64_t time_stamp, bool is_merge_server, bool is_regist)
    {
      int res = 0;
      iterator it = find_by_ip(server);
      if (it != end() )
      {
        if (!is_merge_server)
        {
          TBSYS_LOG(DEBUG,"receive hb from chunkserver, ts=%ld ms=%d reg=%d",
                    time_stamp, is_merge_server, is_regist);
          it->set_hb_time(time_stamp);
          if(it->status_ == ObServerStatus::STATUS_DEAD || is_regist)
          {
            TBSYS_LOG(INFO, "receive relive cs heartbeat, or new cs register. server=%s", server.to_cstring());
            it->register_time_ = time_stamp;
            it->status_ = ObServerStatus::STATUS_WAITING_REPORT;
            it->server_.set_port(server.get_port());
            if (it->port_cs_ == server.get_port() )
            {
              res = 2;
            }
            else
            {
              it->port_cs_ = server.get_port();
              res = 1;
            }
          }
        }
        else
        {
          TBSYS_LOG(DEBUG,"receive hb from mergeserver, ts=%ld ms=%d reg=%d",
                    time_stamp, is_merge_server, is_regist);
          it->port_ms_ = server.get_port();
          it->ms_status_ = ObServerStatus::STATUS_SERVING;
          it->set_hb_time_ms(time_stamp);
        }
      }
      else
      {
        // new server entry
        res = 1;
        ObServerStatus tmp_server_status;
        tmp_server_status.server_ = server;
        if (is_merge_server)
        {
          TBSYS_LOG(DEBUG,"receive hb from mergeserver, ts=%ld ms=%d reg=%d",
                    time_stamp, is_merge_server, is_regist);
          tmp_server_status.port_ms_ = server.get_port();
          tmp_server_status.ms_status_ = ObServerStatus::STATUS_SERVING;
          tmp_server_status.set_hb_time_ms(time_stamp);
        }
        else
        {
          TBSYS_LOG(DEBUG,"receive hb from chunkserver, ts=%ld ms=%d reg=%d",
                    time_stamp, is_merge_server, is_regist);
          tmp_server_status.port_cs_ = server.get_port();
          tmp_server_status.set_hb_time(time_stamp);
          tmp_server_status.status_ = ObServerStatus::STATUS_WAITING_REPORT;
          tmp_server_status.register_time_ = time_stamp;
        }
        res = servers_.push_back(tmp_server_status);
      }
      return res;
    }
    int ObChunkServerManager::update_disk_info(const common::ObServer& server, const ObServerDiskInfo& disk_info)
    {
      int ret = OB_SUCCESS;
      iterator it = find_by_ip(server);
      if (it != end() )
      {
        it->disk_info_ = disk_info;
      }
      else
      {
        char msg[30];
        server.to_string(msg,30);
        TBSYS_LOG(ERROR, " not find info about server %s", msg);
        ret = OB_ERROR;
      }
      return ret;
    }
    int ObChunkServerManager::get_array_length() const
    {
      return static_cast<int32_t>(servers_.get_array_index());
    }
    ObServerStatus* ObChunkServerManager::get_server_status(const int32_t index)
    {
      ObServerStatus* ret = NULL;
      if (index < servers_.get_array_index())
      {
        ret = data_holder_ + index;
      }
      else
      {
        TBSYS_LOG(ERROR, "never should reach this, idx=%d", index);
      }
      return ret;
    }
    const ObServerStatus* ObChunkServerManager::get_server_status(const int32_t index) const
    {
      const ObServerStatus* ret = NULL;
      if (index < servers_.get_array_index())
      {
        ret = data_holder_ + index;
      }
      else
      {
        TBSYS_LOG(ERROR, "never should reach this");
      }
      return ret;
    }

    int ObChunkServerManager::get_server_index(const common::ObServer &server, int32_t &index) const
    {
      index = -1;
      int ret = OB_ENTRY_NOT_EXIST;
      const_iterator it = find_by_ip(server);
      if (end() != it)
      {
        index = static_cast<int32_t>(it - begin());
      }
      return ret;
    }

    common::ObServer ObChunkServerManager::get_cs(const int32_t index) const
    {
      ObServer server;
      const ObServerStatus* st = get_server_status(index);
      if (st != NULL)
      {
        server = st->server_;
        server.set_port(st->port_cs_);
      }
      return server;
    }

    void ObChunkServerManager::reset_balance_info(int32_t max_migrate_out_per_cs)
    {
      int32_t cs_num = 0;
      ObChunkServerManager::iterator it;
      for (it = begin(); end() != it; ++it)
      {
        it->balance_info_.reset();
        if (ObServerStatus::STATUS_DEAD != it->status_)
        {
          cs_num++;
        }
      }
      migrate_infos_.reset(cs_num, max_migrate_out_per_cs);
    }

    void ObChunkServerManager::reset_balance_info_for_table(int32_t &cs_num, int32_t &shutdown_num)
    {
      cs_num = 0;
      shutdown_num = 0;
      ObChunkServerManager::iterator it;
      for (it = begin(); end() != it; ++it)
      {
        if (ObServerStatus::STATUS_DEAD != it->status_)
        {
          it->balance_info_.reset_for_table();
          cs_num++;
          if (ObServerStatus::STATUS_SHUTDOWN == it->status_)
          {
            shutdown_num++;
          }
        }
      }
    }

    bool ObChunkServerManager::is_migrate_infos_full() const
    {
      return migrate_infos_.is_full();
    }

    int ObChunkServerManager::add_migrate_info(ObServerStatus& cs, const common::ObRange &range, int32_t dest_cs_idx)
    {
      return cs.balance_info_.migrate_to_.add_migrate_info(range, dest_cs_idx, migrate_infos_);
    }

    int ObChunkServerManager::add_copy_info(ObServerStatus& cs, const common::ObRange &range, int32_t dest_cs_idx)
    {
      return cs.balance_info_.migrate_to_.add_copy_info(range, dest_cs_idx, migrate_infos_);
    }

    int32_t ObChunkServerManager::get_max_migrate_num() const
    {
      return migrate_infos_.get_size();
    }

    void ObChunkServerManager::set_server_down(iterator& it)
    {
      if (it >= begin() && it < end())
      {
        if (TBSYS_LOGGER._level >= TBSYS_LOG_LEVEL_INFO)
        {
          char server_str[OB_IP_STR_BUFF];
          ObServer tmp_server = it->server_;
          tmp_server.set_port(it->port_cs_);
          tmp_server.to_string(server_str, OB_IP_STR_BUFF);
          TBSYS_LOG(INFO, "chunkserver %s is down", server_str);
        }
        it->status_ = ObServerStatus::STATUS_DEAD;
        it->balance_info_.reset();
      }
      return ;
    }

    void ObChunkServerManager::set_server_down_ms(iterator& it)
    {
      if (it >= begin() && it < end())
      {
        if (TBSYS_LOGGER._level >= TBSYS_LOG_LEVEL_INFO)
        {
          char server_str[OB_IP_STR_BUFF];
          ObServer tmp_server = it->server_;
          tmp_server.set_port(it->port_ms_);
          tmp_server.to_string(server_str, OB_IP_STR_BUFF);
          TBSYS_LOG(INFO, "mergeserver %s is down", server_str);
        }
        it->ms_status_ = ObServerStatus::STATUS_DEAD;
      }
      return ;
    }

    void ObChunkServerManager::cancel_restart_all_cs()
    {
      for(iterator it = begin(); it != end(); ++it)
      {
        it->wait_restart_ = false;
      }
    }

    void ObChunkServerManager::restart_all_cs()
    {
      for(iterator it = begin(); it != end(); ++it)
      {
        it->wait_restart_ = true;
      }
    }

    int ObChunkServerManager::shutdown_cs(const common::ObArray<common::ObServer> &servers, enum ShutdownOperation op)
    {
      int ret = OB_SUCCESS;
      ObServer server;
      ObChunkServerManager::iterator it;
      for (int i = 0; i < servers.count(); ++i)
      {
        if (OB_SUCCESS != (ret = servers.at(i, server)))
        {
          TBSYS_LOG(ERROR, "fatal error=%d", ret);
          break;
        }
        else
        {
          if (end() == (it = this->find_by_ip(server)))
          {
            TBSYS_LOG(WARN, "server not exist, addr=%s", server.to_cstring());
            ret = OB_ENTRY_NOT_EXIST;
            break;
          }
          else if (ObServerStatus::STATUS_SHUTDOWN == it->status_)
          {
            TBSYS_LOG(INFO, "already shutdown, addr=%s", it->server_.to_cstring());
            continue;
          }
          else if (ObServerStatus::STATUS_SERVING != it->status_)
          {
            TBSYS_LOG(WARN, "server not serving, addr=%s stat=%d", server.to_cstring(), it->status_);
            ret = OB_ENTRY_NOT_EXIST;
            break;
          }
          else
          {
            if(SHUTDOWN == op)
            {
              TBSYS_LOG(INFO, "shutdown server=%s", it->server_.to_cstring());
              it->status_ = ObServerStatus::STATUS_SHUTDOWN;
            }
            else if(RESTART == op)
            {
              TBSYS_LOG(INFO, "restart server=%s", it->server_.to_cstring());
              it->wait_restart_ = true;
            }
            else
            {
              ret = OB_NOT_SUPPORTED;
              TBSYS_LOG(WARN, "not supported shutdown operation[%d]", op);
            }
          }
        }
      } // end for
      return ret;
    }

    int ObChunkServerManager::cancel_shutdown_cs(const common::ObArray<common::ObServer> &servers, enum ShutdownOperation op)
    {
      int ret = OB_SUCCESS;
      ObServer server;
      ObChunkServerManager::iterator it;
      for (int i = 0; i < servers.count(); ++i)
      {
        if (OB_SUCCESS != (ret = servers.at(i, server)))
        {
          TBSYS_LOG(ERROR, "fatal error=%d", ret);
          break;
        }
        else
        {
          if (end() == (it = this->find_by_ip(server)))
          {
            TBSYS_LOG(WARN, "server not exist, addr=%s", server.to_cstring());
            ret = OB_ENTRY_NOT_EXIST;
            break;
          }
          else if (ObServerStatus::STATUS_SHUTDOWN == it->status_)
          {
            switch(op)
            {
            case SHUTDOWN:
              TBSYS_LOG(INFO, "cancel shutting down server=%s", it->server_.to_cstring());
              it->status_ = ObServerStatus::STATUS_SERVING;
              break;
            case RESTART:
            default:
              TBSYS_LOG(INFO, "cancel restart server=%s", it->server_.to_cstring());
              it->wait_restart_ = false;
              break;
            }
          }
          else
          {
            TBSYS_LOG(INFO, "not shutting down, addr=%s", it->server_.to_cstring());
          }
        }
      } // end for
      return ret;
    }

    bool ObChunkServerManager::has_shutting_down_server() const
    {
      bool ret = false;
      for (const_iterator it = begin(); end() != it; ++it)
      {
        if (ObServerStatus::STATUS_SHUTDOWN == it->status_)
        {
          ret = true;
          break;
        }
      }
      return ret;
    }

    ObChunkServerManager& ObChunkServerManager::operator= (const ObChunkServerManager& other)
    {
      if (this != &other)
      {
        memcpy(data_holder_, other.data_holder_, sizeof(other.data_holder_));
        servers_ = other.servers_;
      }
      return *this;
    }

    int ObChunkServerManager::write_to_file(const char* filename)
    {
      int ret = OB_SUCCESS;
      if (filename == NULL)
      {
        ret = OB_INVALID_ARGUMENT;
        TBSYS_LOG(INFO, "file name can not be NULL");
      }

      if (ret == OB_SUCCESS)
      {
        int64_t total_size = 0;
        int64_t total_count = servers_.get_array_index();

        total_size += serialization::encoded_length_vi64(total_count);
        for(int64_t i=0; i<total_count; ++i)
        {
          total_size += data_holder_[i].get_serialize_size();
        }

        char* data_buffer = static_cast<char*>(ob_malloc(total_size));
        if (data_buffer == NULL)
        {
          ret = OB_ERROR;
          TBSYS_LOG(ERROR, "allocate memory failed");
        }

        if (ret == OB_SUCCESS)
        {
          common::ObDataBuffer buffer(data_buffer, total_size);
          ret = serialization::encode_vi64(buffer.get_data(), buffer.get_capacity(), buffer.get_position(), total_count);

          if (ret == OB_SUCCESS)
          {
            for(int64_t i=0; i<total_count; ++i)
            {
              ret = data_holder_[i].serialize(buffer.get_data(), buffer.get_capacity(), buffer.get_position());
              if (ret != OB_SUCCESS) break;
            }
          }
        }

        int64_t header_length = sizeof(common::ObRecordHeader);
        int64_t pos = 0;
        char header_buffer[header_length];
        if (ret == OB_SUCCESS)
        {
          common::ObRecordHeader header;

          header.set_magic_num(CHUNK_SERVER_MAGIC);
          header.header_length_ = static_cast<int16_t>(header_length);
          header.version_ = 0;
          header.reserved_ = 0;

          header.data_length_ = static_cast<int32_t>(total_size);
          header.data_zlength_ = static_cast<int32_t>(total_size);

          header.data_checksum_ = common::ob_crc64(data_buffer, total_size);
          header.set_header_checksum();

          ret = header.serialize(header_buffer, header_length, pos);
          if (ret != OB_SUCCESS)
          {
            TBSYS_LOG(ERROR, "serialization file header failed");
          }
        }

        if (ret == OB_SUCCESS)
        {
          common::FileUtils fu;
          int32_t rc = fu.open(filename, O_CREAT | O_WRONLY | O_TRUNC, 0644);
          if (rc < 0)
          {
            ret = OB_ERROR;
            TBSYS_LOG(ERROR, "create file [%s] failed", filename);
          }

          if (ret == OB_SUCCESS)
          {
            int64_t wl = fu.write(header_buffer, header_length);
            if (wl != header_length)
            {
              ret = OB_ERROR;
              TBSYS_LOG(ERROR, "write header into [%s] failed", filename);
            }
          }

          if (ret == OB_SUCCESS)
          {
            int64_t wl = fu.write(data_buffer, total_size);
            if (wl != total_size)
            {
              ret = OB_ERROR;
              TBSYS_LOG(ERROR, "write data info [%s] failed", filename);
            }
            else
            {
              TBSYS_LOG(DEBUG, "chunkserver list has been write into [%s]", filename);
            }
          }

          fu.close();

        }
        if (data_buffer != NULL)
        {
          ob_free(data_buffer);
          data_buffer = NULL;
        }
      }

      return ret;
    }

    int ObChunkServerManager::read_from_file(const char* filename, int32_t &cs_num, int32_t &ms_num)
    {
      int ret = OB_SUCCESS;
      cs_num = 0;
      ms_num = 0;

      if (filename == NULL)
      {
        ret = OB_INVALID_ARGUMENT;
        TBSYS_LOG(INFO, "filename can not be NULL");
      }

      common::FileUtils fu;
      if (ret == OB_SUCCESS)
      {
        int32_t rc = fu.open(filename, O_RDONLY);
        if (rc < 0)
        {
          ret = OB_ERROR;
          TBSYS_LOG(ERROR, "open file [%s] failed", filename);
        }
      }

      int64_t header_length = sizeof(common::ObRecordHeader);
      common::ObRecordHeader header;
      if (ret == OB_SUCCESS)
      {
        char header_buffer[header_length];
        int64_t rl = fu.read(header_buffer, header_length);
        if (rl != header_length)
        {
          ret = OB_ERROR;
          TBSYS_LOG(ERROR, "read header from [%s] failed", filename);
        }

        if (ret == OB_SUCCESS)
        {
          int64_t pos = 0;
          ret = header.deserialize(header_buffer, header_length, pos);
        }
        if (ret == OB_SUCCESS)
        {
          ret = header.check_header_checksum();
        }
      }

      char* data_buffer = NULL;
      int64_t size = 0;
      if (ret == OB_SUCCESS)
      {
        size = header.data_length_;
        data_buffer = static_cast<char*>(ob_malloc(size));
        if (data_buffer == NULL)
        {
          ret = OB_ERROR;
          TBSYS_LOG(ERROR, "allocate memory failed");
        }
      }

      if (ret == OB_SUCCESS)
      {
        int64_t rl = fu.read(data_buffer, size);
        if (rl != size)
        {
          ret = OB_ERROR;
          TBSYS_LOG(ERROR, "read data from file [%s] failed", filename);
        }
      }

      if (ret == OB_SUCCESS)
      {
        int cr = common::ObRecordHeader::check_record(header, data_buffer, size, CHUNK_SERVER_MAGIC);
        if (cr != OB_SUCCESS)
        {
          ret = OB_DESERIALIZE_ERROR;
          TBSYS_LOG(ERROR, "data check failed");
        }
      }

      int64_t count = 0;
      common::ObDataBuffer buffer(data_buffer, size);
      if (ret == OB_SUCCESS)
      {
        ret = serialization::decode_vi64(buffer.get_data(), buffer.get_capacity(), buffer.get_position(), &count);
      }

      if (ret == OB_SUCCESS)
      {
        ObServerStatus server;
        servers_.init(MAX_SERVER_COUNT, data_holder_);

        for(int64_t i=0; i<count; ++i)
        {
          ret = server.deserialize(buffer.get_data(), buffer.get_capacity(), buffer.get_position());
          if (ret == OB_SUCCESS)
          {
            if (ObServerStatus::STATUS_DEAD != server.status_
                && 0 < server.port_cs_)
            {
              cs_num++;
            }
            if (ObServerStatus::STATUS_DEAD != server.ms_status_
                && 0 < server.port_ms_)
            {
              ms_num++;
            }
            servers_.push_back(server);
          }
          else
          {
            TBSYS_LOG(ERROR, "record deserialize failed");
            break;
          }
        }
      }

      fu.close();
      if (data_buffer != NULL)
      {
        ob_free(data_buffer);
        data_buffer = NULL;
      }

      return ret;
    }

    DEFINE_SERIALIZE(ObChunkServerManager)
    {
      int ret = OB_SUCCESS;
      int64_t tmp_pos = pos;
      int64_t size = servers_.get_array_index();
      ret = serialization::encode_vi64(buf, buf_len, tmp_pos, size);
      if (OB_SUCCESS == ret)
      {

        for (int64_t i = 0; i < size; ++i)
        {
          ret = data_holder_[i].serialize(buf, buf_len, tmp_pos);
          if (ret != OB_SUCCESS)
            break;
        }
      }
      if (OB_SUCCESS == ret)
      {
        pos = tmp_pos;
      }
      return ret;
    }
    DEFINE_DESERIALIZE(ObChunkServerManager)
    {
      int ret = OB_ERROR;
      int64_t size = 0;
      ret = serialization::decode_vi64(buf, data_len, pos, &size);
      if (ret == OB_SUCCESS )
      {
        for (int64_t i = 0; i < size; ++i)
        {
          ObServerStatus server_status;
          ret = server_status.deserialize(buf, data_len, pos);
          if (ret != OB_SUCCESS)
          {
            break;
          }
          servers_.push_back(server_status);
        }
      }
      return ret;
    }
    DEFINE_GET_SERIALIZE_SIZE(ObChunkServerManager)
    {
      int64_t total_size = 0;
      int64_t size = servers_.get_array_index();
      total_size += serialization::encoded_length_vi64(size);
      for (int64_t i = 0; i < size; ++i)
      {
        total_size += data_holder_[i].get_serialize_size();
      }

      return total_size;
    }

    int ObChunkServerManager::serialize_cs(const ObServerStatus *it, char* buf, const int64_t buf_len, int64_t& pos) const
    {
      int ret = OB_SUCCESS;
      ObServer addr = it->server_;
      addr.set_port(it->port_cs_);
      int64_t reserve = 0;
      if (OB_SUCCESS != (ret = addr.serialize(buf, buf_len, pos)))
      {
        TBSYS_LOG(WARN, "serialize error");
      }
      else if (OB_SUCCESS != (ret = serialization::encode_vi64(buf, buf_len, pos, reserve)))
      {
        TBSYS_LOG(WARN, "serialize error");
      }
      return ret;
    }

    int ObChunkServerManager::serialize_ms(const ObServerStatus *it, char* buf, const int64_t buf_len, int64_t& pos) const
    {
      int ret = OB_SUCCESS;
      ObServer addr = it->server_;
      addr.set_port(it->port_ms_);
      int64_t reserve = 0;
      if (OB_SUCCESS != (ret = addr.serialize(buf, buf_len, pos)))
      {
        TBSYS_LOG(WARN, "serialize error");
      }
      else if (OB_SUCCESS != (ret = serialization::encode_vi64(buf, buf_len, pos, reserve)))
      {
        TBSYS_LOG(WARN, "serialize error");
      }
      return ret;
    }

    int ObChunkServerManager::serialize_cs_list(char* buf, const int64_t buf_len, int64_t& pos) const
    {
      int ret = OB_SUCCESS;
      int32_t cs_num = 0;
      const ObServerStatus* it = NULL;
      for (it = begin(); it != end(); ++it)
      {
        if (ObServerStatus::STATUS_DEAD != it->status_)
        {
          cs_num++;
        }
      }
      if (OB_SUCCESS != (ret = serialization::encode_vi32(buf, buf_len, pos, cs_num)))
      {
        TBSYS_LOG(WARN, "serialize error");
      }
      else
      {
        int i = 0;
        for (it = begin(); it != end() && i < cs_num; ++it)
        {
          if (ObServerStatus::STATUS_DEAD != it->status_)
          {
            if (OB_SUCCESS != (ret = serialize_cs(it, buf, buf_len, pos)))
            {
              break;
            }
            else
            {
              ++i;
            }
          }
        }
      }
      return ret;
    }

    int ObChunkServerManager::serialize_ms_list(char* buf, const int64_t buf_len, int64_t& pos) const
    {
      int ret = OB_SUCCESS;
      int32_t ms_num = 0;
      const ObServerStatus* it = NULL;
      for (it = begin(); it != end(); ++it)
      {
        if (ObServerStatus::STATUS_DEAD != it->ms_status_)
        {
          ms_num++;
        }
      }
      if (OB_SUCCESS != (ret = serialization::encode_vi32(buf, buf_len, pos, ms_num)))
      {
        TBSYS_LOG(WARN, "serialize error");
      }
      else
      {
	// @bug
        int i = 0;
        for (it = begin(); it != end() && i < ms_num; ++it)
        {
          if (ObServerStatus::STATUS_DEAD != it->ms_status_)
          {
            if (OB_SUCCESS != (ret = serialize_ms(it, buf, buf_len, pos)))
            {
              break;
            }
            else
            {
              ++i;
            }
          }
        }
      }
      return ret;
    }

  }
}
