#ifndef OCEANBASE_ROOT_RPC_STUB_H_
#define OCEANBASE_ROOT_RPC_STUB_H_

#include "common/ob_fetch_runnable.h"
#include "common/ob_common_rpc_stub.h"
#include "common/ob_server.h"
#include "common/ob_schema.h"
#include "common/ob_range.h"
#include "common/ob_tablet_info.h"
#include "common/ob_tablet_info.h"
#include "ob_chunk_server_manager.h"
namespace oceanbase
{
  namespace rootserver
  {
    class OBRootWorker;
    class ObRootRpcStub : public common::ObCommonRpcStub
    {
      public:
        ObRootRpcStub();
        virtual ~ObRootRpcStub();
        int init(const common::ObClientManager *client_mgr, common::ThreadSpecificBuffer* tsbuffer);
        // synchronous rpc messages
        virtual int slave_register(const common::ObServer& master, const common::ObServer& slave_addr, common::ObFetchParam& fetch_param, const int64_t timeout);
        virtual int set_obi_role(const common::ObServer& ups, const common::ObiRole& role, const int64_t timeout_us);
        virtual int switch_schema(const common::ObServer& ups, const common::ObSchemaManagerV2& schema_manager, const int64_t timeout_us);
        virtual int migrate_tablet(const common::ObServer& src_cs, const common::ObServer& dest_cs, const common::ObRange& range, bool keep_src, const int64_t timeout_us);
        virtual int create_tablet(const common::ObServer& cs, const common::ObRange& range, const int64_t mem_version, const int64_t timeout_us);
        virtual int delete_tablets(const common::ObServer& cs, const common::ObTabletReportInfoList &tablets, const int64_t timeout_us);
        virtual int get_last_frozen_version(const common::ObServer& ups, const int64_t timeout_us, int64_t &frozen_version);
        virtual int get_obi_role(const common::ObServer& master, const int64_t timeout_us, common::ObiRole &obi_role);
        virtual int revoke_ups_lease(const common::ObServer &ups, const int64_t lease, const common::ObServer& master, const int64_t timeout_us);
        virtual int import_tablets(const common::ObServer& cs, const uint64_t table_id, const int64_t version, const int64_t timeout_us);
        virtual int get_ups_max_log_seq(const common::ObServer& ups, uint64_t &max_log_seq, const int64_t timeout_us);
        virtual int shutdown_cs(const common::ObServer& cs, bool is_restart, const int64_t timeout_us);

        virtual int get_split_range(const common::ObServer& ups, const int64_t timeout_us,
             const uint64_t table_id, const int64_t frozen_version, common::ObTabletInfoList &tablets);
        virtual int table_exist_in_cs(const common::ObServer &cs, const int64_t timeout_us,
            const uint64_t table_id, bool &is_exist_in_cs);
        // asynchronous rpc messages
        int heartbeat_to_cs(const common::ObServer& cs, 
            const int64_t lease_time,
            const int64_t frozen_mem_version,
            const int64_t schema_version,
            const int64_t config_version = 0);
        int heartbeat_to_ms(const common::ObServer& ms, 
            const int64_t lease_time,
            const int64_t schema_version,
            const common::ObiRole &role,
            const int64_t config_version);
        virtual int grant_lease_to_ups(const common::ObServer& ups,
            const common::ObServer& master,
            const int64_t lease,
            const common::ObiRole &obi_role,
            const int64_t config_version = 0);

        virtual int request_report_tablet(const common::ObServer& chunkserver);
      private:
        int get_thread_buffer_(common::ObDataBuffer& data_buffer);
      private:
        static const int32_t DEFAULT_VERSION = 1;
        common::ThreadSpecificBuffer *thread_buffer_;
    };
  } /* rootserver */
} /* oceanbase */

#endif /* end of include guard: OCEANBASE_ROOT_RPC_STUB_H_ */
