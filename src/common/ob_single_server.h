#ifndef OCEANBASE_COMMON_SINGLE_SERVER_H_
#define OCEANBASE_COMMON_SINGLE_SERVER_H_

#include <tbsys.h>
#include <tbnet.h>
#include <string.h>
#include "ob_define.h"
#include "ob_base_server.h"
#include "ob_packet.h"
#include "ob_packet_queue_thread.h"

namespace oceanbase
{
  namespace common
  {
    class ObSingleServer : public ObBaseServer, public tbnet::IPacketQueueHandler
    {
      public:
        ObSingleServer();
        virtual ~ObSingleServer();

        int initialize();
        void wait_for_queue();
        void destroy();

        /** set worker thread count, default is 0 */
        int set_thread_count(const int thread_count);

        /** set the min left time for checking the task should be timeout */
        int set_min_left_time(const int64_t left_time);

        /** set the queue size of the default task queue */
        int set_default_queue_size(const int task_queue_size);

        /** get current dropped packet count */
        uint64_t get_drop_packet_count(void) const;

        /** get the current queue size for sys monitor */
        virtual size_t get_current_queue_size(void) const;

        tbnet::IPacketHandler::HPRetCode handlePacket(tbnet::Connection *connection, tbnet::Packet *packet);

        /** handle batch packets */
        virtual bool handleBatchPacket(tbnet::Connection *connection, tbnet::PacketQueue &packetQueue);

        /** packet queue handler */
        bool handlePacketQueue(tbnet::Packet *packet, void *args);

        virtual int do_request(ObPacket* base_packet) = 0;

        /** handle packet which can not push into the queue since the queue is full
          * return true is the packet is handled, WARNING: you should free the packet
          * return false, ObSingleServer will free the packet
          */
        virtual bool handle_overflow_packet(ObPacket* base_packet);

        //handle packet which check timeout in the queue
        virtual void handle_timeout_packet(ObPacket* base_packet);

        ObPacketQueueThread &get_default_task_queue_thread() { return default_task_queue_thread_; }

      protected:
        void handle_request(ObPacket* request);
      private:
        int thread_count_;
        int task_queue_size_;
        int64_t min_left_time_;
        uint64_t drop_packet_count_;
        ObPacketQueueThread default_task_queue_thread_;
    };
  } /* common */
} /* oceanbase */
#endif /* end of include guard: OCEANBASE_COMMON_SINGLE_SERVER_H_ */
