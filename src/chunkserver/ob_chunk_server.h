/*
 *   (C) 2007-2010 Taobao Inc.
 *   
 *   Version: 0.1 $date
 *           
 *   Authors:
 *      qushan <qushan@taobao.com>
 *               
 */
#ifndef OCEANBASE_CHUNKSERVER_CHUNKSERVER_H_
#define OCEANBASE_CHUNKSERVER_CHUNKSERVER_H_

#include <pthread.h>
#include "common/ob_single_server.h"
#include "common/ob_packet_factory.h"
#include "common/thread_buffer.h"
#include "common/ob_client_manager.h"
#include "ob_chunk_service.h"
#include "ob_chunk_server_param.h"
#include "ob_tablet_manager.h"
#include "ob_root_server_rpc.h"
#include "ob_chunk_server_stat.h"
#include "ob_rpc_stub.h"
#include "ob_rpc_proxy.h"
#include "ob_schema_manager.h"

namespace oceanbase 
{ 
  namespace chunkserver 
  {
    class ObChunkServer : public common::ObSingleServer
    {
      public:
        static const int32_t RESPONSE_PACKET_BUFFER_SIZE = 1024*1024*2; //2MB
        static const int32_t RPC_BUFFER_SIZE = 1024*1024*2; //2MB
        static const int64_t RETRY_INTERVAL_TIME = 1000 * 1000; // usleep 1 s
      public:
        ObChunkServer();
        ~ObChunkServer();
      public:
        /** called before start server */
        virtual int initialize();
        /** called after start transport and listen port*/
        virtual int start_service();
        virtual void wait_for_queue();
        virtual void destroy();

        virtual tbnet::IPacketHandler::HPRetCode handlePacket(
            tbnet::Connection *connection, tbnet::Packet *packet);

        virtual int do_request(common::ObPacket* base_packet);
      public:
        common::ThreadSpecificBuffer::Buffer* get_rpc_buffer() const;
        common::ThreadSpecificBuffer::Buffer* get_response_buffer() const;

        const common::ThreadSpecificBuffer* get_thread_specific_rpc_buffer() const;

        const common::ObClientManager& get_client_manager() const;
        const common::ObServer& get_self() const;
        const common::ObServer& get_root_server() const;

        const ObChunkServerParam & get_param() const ;
        ObChunkServerParam & get_param() ;
        ObChunkServerStatManager & get_stat_manager();
        const ObTabletManager & get_tablet_manager() const ;
        ObTabletManager & get_tablet_manager() ;
        ObRootServerRpcStub & get_rs_rpc_stub();
        ObMergerRpcStub & get_rpc_stub();
        ObMergerRpcProxy* get_rpc_proxy();
        ObMergerSchemaManager* get_schema_manager();
        int init_merge_join_rpc();

      private:
        DISALLOW_COPY_AND_ASSIGN(ObChunkServer);
        int set_self(const char* dev_name, const int32_t port);
        int64_t get_process_timeout_time(const int64_t receive_time,
                                         const int64_t network_timeout);
      private:
        // request service handler
        ObChunkService service_;
        ObTabletManager tablet_manager_;
        ObChunkServerParam param_;
        ObChunkServerStatManager stat_;

        // network objects.
        common::ObPacketFactory packet_factory_;
        common::ObClientManager  client_manager_;
        ObRootServerRpcStub rs_rpc_stub_;

        ObMergerRpcStub rpc_stub_;
        ObMergerRpcProxy* rpc_proxy_;
        ObMergerSchemaManager *schema_mgr_;

        // thread specific buffers
        common::ThreadSpecificBuffer response_buffer_;
        common::ThreadSpecificBuffer rpc_buffer_;

        // server information
        common::ObServer self_;
    };

#ifndef NO_STAT
#define OB_CHUNK_STAT(op,args...) \
    ObChunkServerMain::get_instance()->get_chunk_server().get_stat_manager().op(args)
#else
#define OB_CHUNK_STAT(op,args...)
#endif

  } // end namespace chunkserver
} // end namespace oceanbase


#endif //OCEANBASE_CHUNKSERVER_CHUNKSERVER_H_

