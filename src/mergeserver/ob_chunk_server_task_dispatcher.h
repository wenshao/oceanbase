/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * ob_cell_operator.h is for ObCellInfo operation
 *
 * Version: $id: ob_cell_operator.h,v 0.1 9/25/2010 3:48p wushi Exp $
 *
 * Authors:
 *   jianming <jianming.cjq@taobao.com>
 *     - some work details if you want
 *
 */
#ifndef OCEANBASE_MERGESERVER_OB_CHUNK_SERVER_TASK_DISPATCHOR_H_
#define OCEANBASE_MERGESERVER_OB_CHUNK_SERVER_TASK_DISPATCHOR_H_

#include "common/ob_server.h"
#include "common/ob_range.h"
#include "common/ob_common_param.h"
#include "common/ob_string.h"
#include "ob_ms_tablet_location_proxy.h"
#include "ob_ms_tablet_location_item.h"
#include "ob_ms_server_counter.h"
#include "ob_chunk_server.h"
#include "ob_merge_server_main.h"

namespace oceanbase
{
  namespace mergeserver
  {
    class ObChunkServerTaskDispatcher
    {
    private:
      static ObChunkServerTaskDispatcher task_dispacher_;

    public:
      static ObChunkServerTaskDispatcher * get_instance();

    public:
      void set_factor(const int32_t get, const int32_t scan);
      /// replicas_in_out available servers for current request
      /// last_query_idx_in last query used which chunkserver
      /// tablet_in this request will access which tablet
      /// return < 0 on error; >= 0 indicate to use which cs for current query
      int select_cs(ObChunkServer * replicas_in_out, const int32_t replica_count_in,
        const int32_t last_query_idx_in, const common::ObRange & tablet_in);

      int select_cs(ObChunkServer * replicas_in_out, const int32_t replica_count_in,
        const int32_t last_query_idx_in, const common::ObCellInfo & get_cell_in);

      int32_t select_cs(ObMergerTabletLocationList & list);
    private:
      ObChunkServerTaskDispatcher();
      virtual ~ObChunkServerTaskDispatcher();

    public:
      void set_local_ip(int32_t local_ip) { local_ip_ = local_ip; }

    private:
      int select_cs(const int64_t factor, ObChunkServer * replicas_in_out, const int32_t replica_count_in,
        const int32_t last_query_idx_in, const common::ObString & start_row_key);

      int select_cs(const bool open, ObChunkServer * replicas_in_out,
        const int32_t replica_count_in, ObMergerServerCounter * counter);

    private:
      bool using_new_balance_;
      int32_t local_ip_;
      int32_t get_factor_;
      int32_t scan_factor_;
    };
  }
}

#endif /* OCEANBASE_MERGESERVER_OB_CHUNK_SERVER_TASK_DISPATCHOR_H_ */

