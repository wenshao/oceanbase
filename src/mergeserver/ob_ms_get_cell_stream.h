/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: ob_ms_get_cell_stream.h,v 0.1 2010/09/17 14:01:30 zhidong Exp $
 *
 * Authors:
 *   chuanhui <xielun.szd@taobao.com>
 *     - some work details if you want
 *
 */

#ifndef OB_MERGER_GET_CELL_STREAM_H_
#define OB_MERGER_GET_CELL_STREAM_H_

#include "ob_join_cache.h"
#include "ob_ms_cell_stream.h"
#include "ob_row_cell_vec.h"
#include "common/ob_get_param.h"
#include "common/ob_read_common_data.h"

namespace oceanbase
{
  namespace mergeserver
  {
    // this is one of ObMSGetCellStream subclasses, it can provide cell stream through get
    // operation from chunkserver or update server according get param.
    // it encapsulates many rpc calls when one server not serving all the required data or the
    // packet is too large, if u set cache, the cell stream will cache data in one row
    class ObMSGetCellStream : public ObMSCellStream
    {
    public:
      // construct
      ObMSGetCellStream(ObMergerRpcProxy * rpc_proxy) : ObMSCellStream(rpc_proxy)
      {
        is_finish_ = false;
        is_cached_ = false;
        cache_ = NULL;
        reset_inner_stat();
      }

      virtual ~ObMSGetCellStream()
      {
      }

    public:
      // next cell
      int next_cell(void);

      // get cell
      int get(const common::ObReadParam &read_param,  ObMSGetCellArray & get_cells, 
              const ObMergerTabletLocation &cs_addr);
    public:
      // using row cache
      void set_cache(ObJoinCache & cache);

      // get row cache
      int get_cache_row(const common::ObCellInfo & key, ObRowCellVec *& result);

    private:
      // check inner stat
      bool check_inner_stat(void) const;
      // reset inner stat
      void reset_inner_stat(void);

      // get next cell
      int get_next_cell(void);

      // get cell data from server through rpc call
      int get_new_cell_data(void);

      // reencape the new get param from [start to end]
      // param  @start start index of get param cells
      //        @end finish index of get param cells
      //        @param the new param
      int encape_get_param(const int64_t start, const int64_t end, common::ObGetParam & param);

      // check whether get operation is all finished
      // param  @finish is finish status
      int check_finish_all_get(bool & finish);

      // add the temp row data to cache and clear result,
      // no matter add cahce succ or failed, just clear it
      int add_cache_clear_result(void);

      // add the row to cache
      //        @result cache value is obscaner
      int add_row_cache(const ObRowCellVec &row);

      // add cell to cache according to whether the current cell's row key is different from
      // previous cell, if force is true, add the cell anyway, then clear the temp row result
      int add_cell_cache(const bool force = true);

      // modify the get param for next get rpc call
      // param  @param the newest get param
      // int modify_get_param(common::ObGetParam & param);

    private:
      bool is_finish_;                // all get data is finished
      bool is_cached_;                // set cached status
      ObRowCellVec row_result_;       // one row temp result for row_data_cache
      bool cur_row_cache_result_valid_; /// is the contents of current cache row valid
      common::ObGetParam param_;      // get param for new rpc call
      int64_t item_index_;            // next get param cell for new rpc call
      ObMSGetCellArray get_cells_;    // get operation cells
      common::ObReadParam read_param_;// read param for get param
      ObJoinCache * cache_;           // set cache for row cell
    };

    // chek inner stat
    inline bool ObMSGetCellStream::check_inner_stat(void) const
    {
      return(ObMSCellStream::check_inner_stat() 
          && ((is_cached_ && NULL != cache_) || (!is_cached_ && (NULL == cache_))));
    }

    // reset inner stat
    inline void ObMSGetCellStream::reset_inner_stat(void)
    {
      ObMSCellStream::reset_inner_stat();
      item_index_ = 0;
      is_finish_ = false;
      cur_row_cache_result_valid_ = true;
    }
  }
}



#endif //OB_MERGER_GET_CELL_STRRAM_H_
