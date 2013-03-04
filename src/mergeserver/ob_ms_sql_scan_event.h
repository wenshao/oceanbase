/*
 * (C) 2007-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: ob_ms_sql_scan_event.h,v 0.1 2011/09/28 14:28:10 xiaochu Exp $
 *
 * Authors:
 *   xiaochu <xiaochu.yh@taobao.com>
 *     - some work details if you want
 *
 */



#ifndef OB_MS_SQL_SCAN_EVENT_H_
#define OB_MS_SQL_SCAN_EVENT_H_

#include "ob_ms_sql_request_event.h"
#include "ob_ms_sql_sub_scan_request.h"
#include "ob_ms_sql_operator.h"
#include "ob_ms_scan_param.h"
#include "ob_ms_tablet_iterator.h"

namespace oceanbase
{
  namespace mergeserver
  {
    class ObMergerLocationCacheProxy;
    class ObMsSqlRpcEvent;
    class ObMergerAsyncRpcStub;

    class ObMsSqlScanEvent:public ObMsSqlRequestEvent
    {
    public:
      ObMsSqlScanEvent(ObMergerLocationCacheProxy * cache_proxy, ObMergerAsyncRpcStub * async_rpc);
      virtual ~ObMsSqlScanEvent();

      void reset();

      /// called by working thread when receive a request from client
      /// these two functions will trigger rpc event which will non-blocking rpc access cs
      int set_request_param(ObMergerScanParam &scan_param, const int64_t timeout_us,
        const int64_t max_parellel_count = DEFAULT_MAX_PARELLEL_COUNT, 
        const int64_t max_tablet_count_perq = DEFAULT_MAX_TABLET_COUNT_PERQ);
      int do_request(const int64_t max_parellel_count, ObTabletLocationRangeIterator &iter, 
        const int64_t timeout_us, 
        const int64_t limit_offset = 0);

      int get_session_next(const int64_t sub_req_idx, const ObMsSqlRpcEvent &prev_rpc_event, oceanbase::common::ObRange &query_range,
        const int64_t timeout_us,  const int64_t limit_offset = 0);


      inline const int32_t get_total_sub_request_count() const;
      inline const int32_t get_finished_sub_request_count() const;

      /// callback by working thread when one of the rpc event finish
      virtual int process_result(const int64_t timeout_us, ObMsSqlRpcEvent *rpc_event, bool& finish);
      int retry(const int64_t sub_req_idx, ObMsSqlRpcEvent *rpc_event, int64_t timeout_us);

      int get_next_row(oceanbase::common::ObRow &row);

      int64_t get_mem_size_used()const
      {
        return(merger_operator_.get_mem_size_used() + cs_result_mem_size_used_);
      }
      int64_t get_whole_result_row_count()const
      {
        return merger_operator_.get_whole_result_row_count();
      }

      int fill_result(oceanbase::common::ObNewScanner & scanner, oceanbase::common::ObScanParam &org_param, 
        bool &got_all_result);

    private:
      ObMsSqlSubScanRequest * alloc_sub_scan_request();
      int find_sub_scan_request(ObMsSqlRpcEvent * agent_event, bool &belong_to_this, bool &is_first, 
        int64_t &idx);
      int check_if_need_more_req(const int64_t sub_req_idx,   const int64_t timeout_us, ObMsSqlRpcEvent &prev_rpc_event);
      int prepare_get_cell();

      int send_rpc_event(ObMsSqlSubScanRequest * sub_req, const int64_t timeout_us, uint64_t * triggered_rpc_event_id = NULL);

      bool check_if_location_cache_valid_(const oceanbase::common::ObNewScanner & scanner, const oceanbase::common::ObScanParam & scan_param);

    private:
      void end_sessions_();
      int32_t               total_sub_request_count_;
      int32_t               finished_sub_request_count_;
      ObMsSqlSubScanRequest    sub_requests_[MAX_SUBREQUEST_NUM];
      ObMsSqlOperator  merger_operator_;
      ObScanParam * scan_param_;

      int64_t       cs_result_mem_size_used_;

      int64_t       max_parellel_count_;
      int64_t       max_tablet_count_perq_;
      ObTabletLocationRangeIterator org_req_range_iter_;

      int64_t sharding_limit_count_;

      static const int64_t MAX_ROW_COLUMN_COUNT = oceanbase::common::OB_MAX_COLUMN_NUMBER * 4;
      int64_t cur_row_cell_cnt_;
      oceanbase::common::ObCellInfo row_cells_[MAX_ROW_COLUMN_COUNT];
    };

    inline const int32_t ObMsSqlScanEvent::get_total_sub_request_count() const
    {
      return total_sub_request_count_;
    }

    inline const int32_t ObMsSqlScanEvent::get_finished_sub_request_count() const
    {
      return finished_sub_request_count_;
    }

  }
}

#endif

