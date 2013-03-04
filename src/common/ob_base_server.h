#ifndef OCEANBASE_COMMON_BASE_SERVER_H_
#define OCEANBASE_COMMON_BASE_SERVER_H_

#include <tbsys.h>
#include <tbnet.h>
#include <string.h>
#include "ob_define.h"
#include "ob_packet.h"
#include "data_buffer.h"

namespace oceanbase
{
  namespace common
  {
    class ObBaseServer : public tbnet::IServerAdapter
    {
      public:
        static const int DEV_NAME_LENGTH = 16;

        ObBaseServer();
        virtual ~ObBaseServer();

        /** called before start server */
        virtual int initialize();

        /** wait for packet thread queue */
        virtual void wait_for_queue();

        /** wait for transport stop */
        virtual void wait();

        /** called when server is stop, before transport is stop */
        virtual void destroy();

        /** WARNING: not thread safe, caller should make sure there is only one thread doing this */
        virtual int start(bool need_wait = true);

        virtual int start_service();

        /** WARNING: not thread safe, caller should make sure there is only one thread doing this */
        virtual void stop();

        /** whether enable batch process mode */
        void set_batch_process(const bool batch);

        /** set the packet factory, this is used to create packet according to packet code */
        int set_packet_factory(tbnet::IPacketFactory *packet_factory); // we can not use const here, since tbnet's interface is not const

        /** set the device name of the interface */
        int set_dev_name(const char* dev_name);

        /** set the port this server should listen on */
        int set_listen_port(const int port);

        /** handle single packet */
        virtual tbnet::IPacketHandler::HPRetCode handlePacket(tbnet::Connection *connection, tbnet::Packet *packet) = 0;

        /** handle batch packets */
        virtual bool handleBatchPacket(tbnet::Connection *connection, tbnet::PacketQueue &packetQueue) = 0;

        uint64_t get_server_id() const;

        tbnet::Transport* get_transport();
        tbnet::IPacketStreamer* get_packet_streamer();

        int send_response(const int32_t pcode, const int32_t version, const ObDataBuffer& buffer, tbnet::Connection* connection, const int32_t channel_id, const int64_t session_id = 0);

      protected:
        volatile bool stoped_;
        int thread_count_;
        bool batch_;
        char dev_name_[DEV_NAME_LENGTH];
        int port_;
        uint64_t server_id_;
        tbnet::IPacketFactory* packet_factory_;
        tbnet::DefaultPacketStreamer streamer_;
        tbnet::Transport transport_;
    };
  } /* common */
} /* oceanbase */
#endif /* end of include guard: OCEANBASE_COMMON_BASE_SERVER_H_ */
