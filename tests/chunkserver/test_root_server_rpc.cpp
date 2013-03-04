
#include <gtest/gtest.h>

#include "ob_root_server_rpc.h"

#include "common/ob_client_manager.h"
#include "common/thread_buffer.h"
#include "common/ob_tablet_info.h"
#include "common/ob_packet_factory.h"
#include "common/ob_base_server.h"
#include "common/ob_single_server.h"
#include "common/ob_malloc.h"
#include "common/ob_result.h"

#include "tbnet.h"
#include "tbsys.h"

using namespace oceanbase::common;
using namespace oceanbase::chunkserver;

namespace oceanbase
{
  namespace tests
  {
    namespace chunkserver
    {
      class TestRootServerRpcStub: public ::testing::Test
      {
      public:
        static const int MOCK_SERVER_LISTEN_PORT = 33248;
      public:
        virtual void SetUp()
        {

        }

        virtual void TearDown()
        {

        }

        class MockServer : public ObSingleServer
        {
          public:
            MockServer()
            {
            }
            virtual ~MockServer()
            {
            }
            virtual int initialize()
            {
              set_batch_process(false);
              set_listen_port(MOCK_SERVER_LISTEN_PORT);
              set_dev_name("bond0");
              set_packet_factory(&factory_);
              set_default_queue_size(100);
              set_thread_count(1);
              set_packet_factory(&factory_);
              client_manager_.initialize(get_transport(), get_packet_streamer());
              ObSingleServer::initialize();

              return OB_SUCCESS;
            }

            virtual int do_request(ObPacket* base_packet)
            {
              int ret = OB_SUCCESS;
              ObPacket* ob_packet = base_packet;
              int32_t packet_code = ob_packet->get_packet_code();
              int32_t version = ob_packet->get_api_version();
              int32_t channel_id = ob_packet->getChannelId();
              ret = ob_packet->deserialize();

              TBSYS_LOG(INFO, "recv packet with packet_code[%d] version[%d] channel_id[%d]",
                  packet_code, version, channel_id);

              if (OB_SUCCESS == ret)
              {
                switch (packet_code)
                {
                  case OB_HEARTBEAT:
                    handle_heartbeat(ob_packet);
                    break;
                  case OB_REPORT_TABLETS:
                    handle_report_tablets(ob_packet);
                    break;
                  case OB_WAITING_JOB_DONE:
                    handle_schema_changed(ob_packet);
                    break;
                };
              }

              return ret;
            }

            int handle_heartbeat(ObPacket* packet)
            {
              UNUSED(packet);
              return OB_SUCCESS;
            }

            int handle_report_tablets(ObPacket* ob_packet)
            {
              int ret = OB_SUCCESS;

              ObDataBuffer* data = ob_packet->get_buffer();
              if (NULL == data)
              {
                TBSYS_LOG(ERROR, "data is NUll should not reach this");
                ret = OB_ERROR;
              }
              else
              {
                ObResultCode result_msg;
                ObServer server;
                ObTabletReportInfoList tablet_list;
                int64_t time_stamp = 0;

                if (OB_SUCCESS == ret)
                {
                  ret = server.deserialize(data->get_data(), data->get_capacity(), data->get_position());
                  if (ret != OB_SUCCESS)
                  {
                    TBSYS_LOG(ERROR, "server.deserialize error");
                  }
                }
                if (OB_SUCCESS == ret)
                {
                  ret = tablet_list.deserialize(data->get_data(), data->get_capacity(), data->get_position());
                  if (ret != OB_SUCCESS)
                  {
                    TBSYS_LOG(ERROR, "tablet_list.deserialize error");
                  }
                  TBSYS_LOG(INFO, "tablets size: %ld", tablet_list.get_tablet_size());
                  EXPECT_EQ(tablet_list.get_tablet_size(), 1);
                }
                if (OB_SUCCESS == ret)
                {
                  ret = serialization::decode_vi64(data->get_data(), data->get_capacity(), data->get_position(), &time_stamp);
                  if (ret != OB_SUCCESS)
                  {
                    TBSYS_LOG(ERROR, "time_stamp.deserialize error");
                  }
                  TBSYS_LOG(INFO, "timestamp: %ld", time_stamp);
                  EXPECT_LT(labs((int64_t)time(NULL) - time_stamp), 10);
                }


                tbnet::Connection* connection = ob_packet->get_connection();
                ThreadSpecificBuffer::Buffer* thread_buffer =
                  response_packet_buffer_.get_buffer();
                if (NULL != thread_buffer)
                {
                  thread_buffer->reset();
                  ObDataBuffer out_buffer(thread_buffer->current(), thread_buffer->remain());

                  result_msg.result_code_ = ret;
                  result_msg.serialize(out_buffer.get_data(), out_buffer.get_capacity(),
                      out_buffer.get_position());

                  TBSYS_LOG(DEBUG, "handle tablets report packet");

                  int32_t version = 1;
                  int32_t channel_id = ob_packet->getChannelId();
                  ret = send_response(OB_REPORT_TABLETS_RESPONSE, version, out_buffer, connection, channel_id);
                }
                else
                {
                  TBSYS_LOG(ERROR, "get thread buffer error, ignore this packet");
                  ret = OB_ERROR;
                }
              }

              return ret;
            }

            int handle_schema_changed(ObPacket* ob_packet)
            {
              int ret = OB_SUCCESS;

              ObDataBuffer* data_buffer = ob_packet->get_buffer();
              if (NULL == data_buffer)
              {
                TBSYS_LOG(ERROR, "data_buffer is NUll should not reach this");
                ret = OB_ERROR;
              }
              else
              {
                tbnet::Connection* connection = ob_packet->get_connection();
                ThreadSpecificBuffer::Buffer* thread_buffer =
                  response_packet_buffer_.get_buffer();
                if (NULL != thread_buffer)
                {
                  thread_buffer->reset();
                  ObDataBuffer out_buffer(thread_buffer->current(), thread_buffer->remain());

                  ObResultCode response;
                  response.result_code_ = OB_SUCCESS;
                  response.serialize(out_buffer.get_data(), out_buffer.get_capacity(),
                      out_buffer.get_position());

                  TBSYS_LOG(DEBUG, "handle schema changed");

                  int32_t version = 1;
                  int32_t channel_id = ob_packet->getChannelId();
                  ret = send_response(OB_WAITING_JOB_DONE_RESPONSE, version, out_buffer, connection, channel_id);
                  TBSYS_LOG(DEBUG, "handle schema changed");
                }
                else
                {
                  TBSYS_LOG(ERROR, "get thread buffer error, ignore this packet");
                  ret = OB_ERROR;
                }
              }

              return ret;
            }

          private:
            ObPacketFactory factory_;
            ObClientManager  client_manager_;
            ThreadSpecificBuffer response_packet_buffer_;
        };

        class MockServerRunner : public tbsys::Runnable
        {
          public:
            virtual void run(tbsys::CThread *thread, void *arg)
            {
              UNUSED(thread);
              UNUSED(arg);
              MockServer mock_server;
              mock_server.start();
            }
        };
      };

      TEST_F(TestRootServerRpcStub, test_report_tablets)
      {
        ob_init_memory_pool();

        const char* dst_addr = "localhost";

        ObServer dst_host;
        dst_host.set_ipv4_addr(dst_addr, MOCK_SERVER_LISTEN_PORT);

        ObRootServerRpcStub rs_rpc;

        ObClientManager client_manager;
        ObPacketFactory factory;
        tbnet::Transport transport;
        tbnet::DefaultPacketStreamer streamer;

        // tablets to be reported
        ObTabletReportInfoList tablets;
        ObTabletReportInfo tablet1;
        EXPECT_EQ(OB_SUCCESS, tablets.add_tablet(tablet1));

        streamer.setPacketFactory(&factory);
        EXPECT_EQ(OB_SUCCESS, client_manager.initialize(&transport, &streamer));
        transport.start();
        EXPECT_EQ(OB_SUCCESS, rs_rpc.init(dst_host, &client_manager));

        tbsys::CThread test_root_server_thread;
        MockServerRunner test_root_server;
        test_root_server_thread.start(&test_root_server, NULL);

        sleep(1);
        EXPECT_EQ(OB_SUCCESS, rs_rpc.report_tablets(tablets, time(0), false));

        transport.stop();
        transport.wait();
      }

    }
  }
}

int main(int argc, char** argv)
{
  TBSYS_LOGGER.setLogLevel("ERROR");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

