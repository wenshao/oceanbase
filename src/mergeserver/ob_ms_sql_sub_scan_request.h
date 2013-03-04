#ifndef _OB_MERGER_SQL_SUB_SCAN_REQUEST_H_
#define _OB_MERGER_SQL_SUB_SCAN_REQUEST_H_

#include "common/ob_thread_objpool.h"
#include "common/ob_malloc.h"
#include "chunkserver/ob_chunk_server.h"
#include "common/ob_range.h"
#include "common/ob_string_buf.h"
#include "common/ob_server.h"
#include "common/ob_scanner.h"
#include "common/ob_get_param.h"
#include "common/ob_scan_param.h"
#include "ob_ms_sql_rpc_event.h"
#include "ob_ms_sql_request_event.h"
#include "ob_chunk_server.h"
#include "ob_chunk_server_task_dispatcher.h"
#include "ob_ms_schema_manager.h"
#include <algorithm>

using namespace oceanbase::common;

namespace oceanbase
{
  namespace mergeserver
  {

    class ObMsSqlRpcEvent;

    class ObMsSqlSubScanRequest
    {
    public:

      ObMsSqlSubScanRequest();

      ~ObMsSqlSubScanRequest();

      int init(ObScanParam *scan_param, 
        ObRange & query_range, 
        const int64_t limit_offset, 
        const int64_t limit_count, 
        const ObChunkServer cs_replicas[], 
        const int32_t replica_count, 
        const bool scan_full_tablet, 
        ObStringBuf *buffer_pool);

      void reset();


      int select_cs(ObChunkServer & selected_server);


      /// add a rpc event to this sub request
      int add_event(ObMsSqlRpcEvent *rpc_event, 
        ObMsSqlRequestEvent *client_request,
        ObChunkServer & selected_server);

      /// check if agent_event belong to this, and if agent_event is the first finished backup task
      /// if agent_event belong to this, set belong_to_this to true, and increment finished_backup_task_count_
      int agent_event_finish(ObMsSqlRpcEvent * agent_event, bool &belong_to_this, bool &is_first); 

      /// called to check if this tablet has been scanned over or tablet has been divided
      /// if tablet has scanned over and tablet has not been divided, return OB_ITER_END
      /// int get_next_scan_param(ObScanParam & next_param);
      /// int get_next_scan_range(ObRange & next_range);

      const ObRange &get_query_range() const;  
      bool scan_full_tablet() const;        

      /// check if this sub request finished, if finished 
      bool finish() const;

      /// get the result of current sub request
      ObMsSqlRpcEvent * get_result();

      /// get statistics params
      inline int32_t triggered_backup_task_count() const;
      inline int32_t finished_backup_task_count() const;
      inline int32_t total_replica_count() const;
      inline int32_t tried_replica_count() const;
      inline int32_t last_tried_replica_idx() const;        

      /// result interface
      inline ObNewScanner *get_scanner() const;
      inline ObScanParam * get_scan_param() const;
      inline const int64_t get_limit_offset() const;

      inline int reset_cs_replicas(const int32_t replica_cnt, const ObChunkServer *cs_vec);

      inline ObServer get_session_server()const;
      inline int64_t get_session_id()const;

      inline void get_cs_replicas(ObChunkServer * replica_buf, int64_t & replica_count)
      {
        if ((NULL != replica_buf) && (replica_count > 0))
        {
          memcpy(replica_buf, cs_replicas_, sizeof(ObChunkServer)*std::min<int64_t>(replica_count, total_replica_count_));
        }
        else
        {
          replica_count = 0;
        }
      }

    private:
      int add_event_id(uint64_t event_id);
      int check_event_id(uint64_t event_id, bool &exist);

    private:
      static const int32_t MAX_BACKUP_TASK_NUM = 16;
      ///  does this sub request scan a full tablet
      bool              scan_full_tablet_;
      uint64_t          backup_agent_tasks_[MAX_BACKUP_TASK_NUM];
      int32_t           triggered_backup_task_count_;
      int32_t           finished_backup_task_count_;

      ObChunkServer     cs_replicas_[ObMergerTabletLocationList::MAX_REPLICA_COUNT];
      int32_t           total_replica_count_;
      int32_t           tried_replica_count_;
      int32_t           last_tried_replica_idx_;
      /// does init() called
      bool              inited_;
      int32_t           event_id_pos_;
      int64_t           limit_offset_;
      int64_t           limit_count_;

      /// ThreadAllocator<ObMsSqlRpcEvent, MutilObjAllocator<ObMsSqlRpcEvent, 65536> > allocator_;
      ObRange           query_range_;
      ObMsSqlRpcEvent      *result_;
      ObScanParam       *scan_param_;
      ObStringBuf       *buffer_pool_;

      /// session related, there should be only on session associated with a sub req, because backups tasks 
      /// can't use session stream, backup tasks must use totally new scan request
      int64_t           session_id_;
      ObServer          session_server_;
      uint64_t          session_rpc_event_id_;
    };

    inline int ObMsSqlSubScanRequest::reset_cs_replicas(const int32_t replica_cnt, const ObChunkServer *cs_vec)
    {
      int err = oceanbase::common::OB_SUCCESS;
      if ((replica_cnt <= 0) || (NULL == cs_vec))
      {
        err = oceanbase::common::OB_INVALID_ARGUMENT;
      }
      if (oceanbase::common::OB_SUCCESS == err)
      {
        total_replica_count_ = 0;
        tried_replica_count_ = 0;
        last_tried_replica_idx_ = -1;
        for (int32_t i = 0; (i < replica_cnt) && (i < ObMergerTabletLocationList::MAX_REPLICA_COUNT); i++)
        {
          cs_replicas_[i] = cs_vec[i];
          cs_replicas_[i].status_ = oceanbase::mergeserver::ObChunkServer::UNREQUESTED;
          total_replica_count_ ++;
        }
      }
      return err;
    }

    inline const int64_t ObMsSqlSubScanRequest::get_limit_offset() const
    {
      return limit_offset_;
    }

    inline ObNewScanner *ObMsSqlSubScanRequest::get_scanner() const
    {
      ObNewScanner *ret = NULL;
      if (NULL != result_)
      {
        ret = &(result_->get_result());
      }
      return ret;
    }

    inline const ObRange &ObMsSqlSubScanRequest::get_query_range() const
    {
      return query_range_;
    }


    inline ObScanParam *ObMsSqlSubScanRequest::get_scan_param() const
    {
      int err = OB_SUCCESS;

      if (NULL != scan_param_)
      {

        if ((OB_SUCCESS == err) && (OB_SUCCESS != (err = scan_param_->set_range(query_range_))))
        {
          TBSYS_LOG(WARN, "fail to set scan_param [err=%d]", err);
        }
        if ((OB_SUCCESS == err) && 
          (OB_SUCCESS != (err = scan_param_->set_limit_info(limit_offset_, limit_count_))))
        {
          TBSYS_LOG(WARN, "fail to set scan limit [limit_offset_=%ld][err=%d]", limit_offset_, err);
        }
      }
      else
      {
        TBSYS_LOG(WARN, "null pointer error. [scan_param_=%p]", scan_param_);
      }
      return scan_param_;
    }


    inline int32_t ObMsSqlSubScanRequest::triggered_backup_task_count() const
    {
      return triggered_backup_task_count_;
    }


    inline int32_t ObMsSqlSubScanRequest::finished_backup_task_count() const
    {
      return finished_backup_task_count_;
    }


    inline int32_t ObMsSqlSubScanRequest::total_replica_count() const
    {
      return total_replica_count_;
    }


    inline int32_t ObMsSqlSubScanRequest::tried_replica_count() const
    {
      return tried_replica_count_;
    }


    inline int32_t ObMsSqlSubScanRequest::last_tried_replica_idx() const
    {
      return last_tried_replica_idx_;
    }

    inline bool ObMsSqlSubScanRequest::scan_full_tablet() const
    {
      return scan_full_tablet_;
    }

    /// check if this sub request finished, if finished 
    inline bool ObMsSqlSubScanRequest::finish() const
    {
      return(result_ != NULL);
    }

    /// get the result of current sub request
    inline ObMsSqlRpcEvent * ObMsSqlSubScanRequest::get_result()
    {
      return result_;
    }


    inline ObServer ObMsSqlSubScanRequest::get_session_server()const
    {
      return session_server_;
    }
    inline int64_t ObMsSqlSubScanRequest::get_session_id()const
    {
      return session_id_;
    }
  } /* EOF mergeserver ns */
} /* EOF oceanbase ns */

#endif
