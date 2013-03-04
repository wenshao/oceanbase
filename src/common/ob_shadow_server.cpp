#include "ob_shadow_server.h"

namespace oceanbase
{
  namespace common
  {
    ObShadowServer::ObShadowServer(ObBaseServer* master)
    {
      priority_ = NORMAL_PRI;
      if (master != NULL)
      {
        master_ = master;
      }
    }

    ObShadowServer::~ObShadowServer()
    {
      // does nothing
    }

    void ObShadowServer::set_priority(const int32_t priority)
    {
      if (priority == NORMAL_PRI || priority == LOW_PRI)
      {
        priority_ = priority;
      }
    }

    tbnet::IPacketHandler::HPRetCode ObShadowServer::handlePacket(tbnet::Connection *connection, tbnet::Packet *packet)
    {
      tbnet::IPacketHandler::HPRetCode rc = tbnet::IPacketHandler::FREE_CHANNEL;
      if (!packet->isRegularPacket())
      {
        TBSYS_LOG(WARN, "control packet, packet code: %d", ((tbnet::ControlPacket*)packet)->getCommand());
      }
      else
      {
        ObPacket* req = (ObPacket*) packet;
        req->set_packet_priority(priority_);
        if (master_ != NULL)
        {
          rc = master_->handlePacket(connection, req);
        }
        else
        {
          TBSYS_LOG(ERROR, "shadow server's master is NULL");
          packet->free();
          rc = tbnet::IPacketHandler::KEEP_CHANNEL;
        }
      }

      return rc;
    }

    bool ObShadowServer::handleBatchPacket(tbnet::Connection *connection, tbnet::PacketQueue &packetQueue)
    {
      bool ret = true;
      tbnet::PacketQueue temp_queue;
      tbnet::Packet *packet= packetQueue.getPacketList();
      while (packet != NULL)
      {
        ObPacket* req = (ObPacket*) packet;
        req->set_packet_priority(priority_);
        if (master_ == NULL)
        {
          TBSYS_LOG(ERROR, "shadow server's master is NULL");
          packet = packet->getNext();
          req->free();
        }
        else
        {
          temp_queue.push(packet);
          packet = packet->getNext();
        }
      }

      if (temp_queue.size() > 0)
      {
        ret = master_->handleBatchPacket(connection, temp_queue);
      }

      return ret;
    }

  } /* common */
} /* oceanbase */
