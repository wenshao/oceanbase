#include "ob_single_server.h"

namespace oceanbase
{
  namespace common
  {
    ObSingleServer::ObSingleServer() : thread_count_(0), task_queue_size_(100), min_left_time_(0), drop_packet_count_(0)
    {
    }

    ObSingleServer::~ObSingleServer()
    {
    }

    int ObSingleServer::initialize()
    {
      if (thread_count_ > 0)
      {
        default_task_queue_thread_.setThreadParameter(thread_count_, this, NULL);
        default_task_queue_thread_.start();
      }

      return OB_SUCCESS;
    }

    void ObSingleServer::wait_for_queue()
    {
      if (thread_count_ > 0)
        default_task_queue_thread_.wait();
    }

    void ObSingleServer::destroy()
    {
      if (thread_count_ > 0) {
        default_task_queue_thread_.stop();
        wait_for_queue();
      }
    }

    int ObSingleServer::set_min_left_time(const int64_t left_time)
    {
      int ret = OB_SUCCESS;
      if (left_time < 0)
      {
        ret = OB_ERROR;
        TBSYS_LOG(WARN, "left time should positive, you provide: %ld", left_time);
      }
      else
      {
        min_left_time_ = left_time;
      }
      return ret;
    }

    int ObSingleServer::set_thread_count(const int thread_count)
    {
      int ret = OB_SUCCESS;

      if (thread_count < 0)
      {
        ret = OB_ERROR;
        TBSYS_LOG(WARN, "thread count should positive, you provide: %d", thread_count);
      } else
      {
        thread_count_ = thread_count;
      }

      return ret;
    }

    int ObSingleServer::set_default_queue_size(const int task_queue_size)
    {
      int ret = OB_SUCCESS;

      if (task_queue_size <= 0)
      {
        ret = OB_INVALID_ARGUMENT;
        TBSYS_LOG(WARN, "default queue size should be positive, you provide %d", task_queue_size);
      } else
      {
        task_queue_size_ = task_queue_size;
      }

      return ret;
    }

    uint64_t ObSingleServer::get_drop_packet_count(void) const
    {
      return drop_packet_count_;
    }

    size_t ObSingleServer::get_current_queue_size(void) const
    {
      return default_task_queue_thread_.size();
    }

    tbnet::IPacketHandler::HPRetCode ObSingleServer::handlePacket(tbnet::Connection* connection, tbnet::Packet* packet)
    {
      tbnet::IPacketHandler::HPRetCode rc = tbnet::IPacketHandler::FREE_CHANNEL;
      if (!packet->isRegularPacket())
      {
        TBSYS_LOG(WARN, "control packet, packet code: %d", ((tbnet::ControlPacket*)packet)->getCommand());
      } else
      {
        ObPacket* req = (ObPacket*) packet;
        req->set_connection(connection);

        if (thread_count_ == 0)
        {
          handle_request(req);
        } else
        {
          bool ps = default_task_queue_thread_.push(req, task_queue_size_, false);
          if (!ps)
          {
            if (!handle_overflow_packet(req))
            {
              TBSYS_LOG(WARN, "overflow packet dropped, packet code: %d", req->getPCode());
            }
            rc = tbnet::IPacketHandler::KEEP_CHANNEL;
          }
        }
      }

      return rc;
    }

    bool ObSingleServer::handleBatchPacket(tbnet::Connection *connection, tbnet::PacketQueue &packetQueue)
    {
      ObPacketQueue temp_queue;

      ObPacket *packet= (ObPacket*)packetQueue.getPacketList();

      while (packet != NULL)
      {
        packet->set_connection(connection);

        if (thread_count_ == 0)
        {
          handle_request(packet);
        }
        else
        {
          temp_queue.push(packet); // the task queue will free this packet
        }

        packet = (ObPacket*)packet->getNext(); // handle as much as we can
      }

      if (temp_queue.size() > 0)
      {
        default_task_queue_thread_.pushQueue(temp_queue, task_queue_size_); // if queue is full, this will block
      }

      return true;
    }

    bool ObSingleServer::handlePacketQueue(tbnet::Packet *packet, void *args)
    {
      UNUSED(args);
      ObPacket* req = (ObPacket*) packet;
      int64_t source_timeout = req->get_source_timeout();

      bool ret = false;
      if (source_timeout > 0)
      {
        int64_t receive_time = req->get_receive_ts();
        int64_t current_ts = tbsys::CTimeUtil::getTime();
        if ((current_ts - receive_time) + min_left_time_ > source_timeout)
        {
          atomic_inc(&drop_packet_count_);
          TBSYS_LOG(WARN, "packet block time: %ld(us), plus min left time: %ld(us) "
              "exceed timeout: %ld(us), current queue size:%ld, packet id: %d, dropped",
              (current_ts - receive_time), min_left_time_, source_timeout, default_task_queue_thread_.size(),
              packet->getPCode());
          ret = true;
        }
      }

      if (!ret)
      {
        handle_request(req);
      }
      else
      {
        handle_timeout_packet(req);
      }
      return ret;
    }

    void ObSingleServer::handle_request(ObPacket *request)
    {
      if (request == NULL)
      {
        TBSYS_LOG(WARN, "handle a NULL packet");
      }
      else
      {
        int ret = do_request(request);
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(WARN, "request process failed, pcode: %d, ret: %d", request->get_packet_code(), ret);
        }
      }
    }

    bool ObSingleServer::handle_overflow_packet(ObPacket* base_packet)
    {
      UNUSED(base_packet);
      return false;
    }

    void ObSingleServer::handle_timeout_packet(ObPacket* base_packet)
    {
      UNUSED(base_packet);
    }

  } /* common */
} /* oceanbase */
