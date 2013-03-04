#include "ob_base_server.h"

namespace oceanbase
{
  namespace common
  {
    ObBaseServer::ObBaseServer() : stoped_(false), batch_(false), port_(0), packet_factory_(NULL)
    {
    }

    ObBaseServer::~ObBaseServer()
    {
    }

    int ObBaseServer::initialize() { return OB_SUCCESS; }
    int ObBaseServer::start_service() {return OB_SUCCESS; }

    void ObBaseServer::wait_for_queue() {}

    void ObBaseServer::wait()
    {
      transport_.wait();
    }

    void ObBaseServer::destroy() {}

    int ObBaseServer::start(bool need_wait)
    {
      int rc = OB_SUCCESS;
      rc = initialize();

      if (rc != OB_SUCCESS)
      {
        TBSYS_LOG(WARN, "initialize failed, ret = %d", rc);
      } else
      {
        if (packet_factory_ == NULL)
        {
          rc = OB_INVALID_ARGUMENT;
          TBSYS_LOG(WARN, "packet factory can not be NULL, server not started");
        } else 
        {
          uint32_t local_ip = tbsys::CNetUtil::getLocalAddr(dev_name_);
          server_id_ = tbsys::CNetUtil::ipToAddr(local_ip, port_);

          streamer_.setPacketFactory(packet_factory_);

          setBatchPushPacket(batch_); // IServerAdapter's method

          transport_.start();

          char spec[32];
          snprintf(spec, 32, "tcp::%d", port_);
          spec[31] = '\0';

          if (transport_.listen(spec, &streamer_, this) == NULL) {
            TBSYS_LOG(ERROR, "listen on port %d failed", port_);
            rc = OB_SERVER_LISTEN_ERROR;
          } else
          {
            TBSYS_LOG(INFO, "listened on port %d", port_);
          }

          if (rc == OB_SUCCESS)
          {
            rc = start_service();
          }

          if (rc == OB_SUCCESS)
          {
            //wait_for_queue();
            if (need_wait)
            {
              transport_.wait();
            }
          }
        }
      }

      if (need_wait)
      {
        stop();
      }

      return rc;
    }

    void ObBaseServer::stop()
    {
      if (!stoped_)
      {
        stoped_ = true;
        destroy();
        transport_.stop();
        transport_.wait();
        TBSYS_LOG(INFO, "server stoped.");
      }
    }

    void ObBaseServer::set_batch_process(const bool batch)
    {
      batch_ = batch;
      TBSYS_LOG(INFO, "batch process mode %s", batch ? "enabled" : "disabled");
    }

    int ObBaseServer::set_packet_factory(tbnet::IPacketFactory *packet_factory)
    {
      int ret = OB_SUCCESS;

      if (packet_factory == NULL)
      {
        ret = OB_INVALID_ARGUMENT;
        TBSYS_LOG(WARN, "packet factory can not be NULL");
      } else
      {
        packet_factory_ = packet_factory;
      }

      return ret;
    }

    int ObBaseServer::set_dev_name(const char* dev_name)
    {
      int rc = OB_INVALID_ARGUMENT;

      if (dev_name != NULL) {
        TBSYS_LOG(INFO, "set interface name to [%s]", dev_name);
        strncpy(dev_name_, dev_name, DEV_NAME_LENGTH);
        int end = sizeof(dev_name_) - 1;
        dev_name_[end] = '\0';
        rc = OB_SUCCESS;
      } else
      {
        TBSYS_LOG(WARN, "interface name is NULL, will use default interface.");
      }

      return rc;
    }

    int ObBaseServer::set_listen_port(const int port)
    {
      int rc = OB_INVALID_ARGUMENT;

      if (port > 0)
      {
        port_ = port;
        rc = OB_SUCCESS;

        if (port_ < 1024)
        {
          TBSYS_LOG(WARN, "listen port less than 1024--[%d], make sure this is what you want.", port_);
        } else
        {
          TBSYS_LOG(INFO, "listen port set to [%d]", port_);
        }
      } else
      {
        TBSYS_LOG(WARN, "listen post should be positive, you provide: [%d]", port);
      }

      return rc;
    }

    uint64_t ObBaseServer::get_server_id() const
    {
      return server_id_;
    }

    tbnet::Transport* ObBaseServer::get_transport() 
    {
      return &transport_;
    }

    tbnet::IPacketStreamer* ObBaseServer::get_packet_streamer() 
    {
      return &streamer_;
    }

    int ObBaseServer::send_response(const int32_t pcode, const int32_t version, const ObDataBuffer& buffer, tbnet::Connection* connection, const int32_t channel_id, const int64_t session_id)
    {
      int rc = OB_SUCCESS;

      if (connection == NULL)
      {
        rc = OB_ERROR;
        TBSYS_LOG(WARN, "connection is NULL");
      }

      ObPacket* packet = new(std::nothrow) ObPacket();
      if (packet == NULL)
      {
        rc = OB_ALLOCATE_MEMORY_FAILED;
        TBSYS_LOG(ERROR, "create packet failed");
      }
      else
      {
        packet->set_packet_code(pcode);
        packet->setChannelId(channel_id);
        packet->set_api_version(version);
        packet->set_data(buffer);
        packet->set_session_id(session_id);
      }

      if (rc == OB_SUCCESS)
      {
        rc = packet->serialize();
        if (rc != OB_SUCCESS)
          TBSYS_LOG(WARN, "packet serialize error, error: %d", rc);
      }

      if (rc == OB_SUCCESS)
      {
        if (!connection->postPacket(packet))
        {
          uint64_t peer_id = connection->getPeerId();
          TBSYS_LOG(WARN, "send packet to [%s] failed", tbsys::CNetUtil::addrToString(peer_id).c_str());
          rc = OB_ERROR;
        }
      }

      if (rc != OB_SUCCESS)
      {
        if (NULL != packet)
        {
          packet->free();
          packet = NULL;
        }
      }

      return rc;
    }

  } /* common */
} /* oceanbase */
