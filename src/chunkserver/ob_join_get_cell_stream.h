/*
* (C) 2007-2011 TaoBao Inc.
*
* This program is free software; you can redistribute it and/or modify
* it under the terms of the GNU General Public License version 2 as
* published by the Free Software Foundation.
*
* ob_join_get_cell_stream.h is for what ...
*
* Version: $id$
*
* Authors:
*   MaoQi maoqi@taobao.com
*
*/
#ifndef OB_JOIN_GET_CELL_STREAM_H
#define OB_JOIN_GET_CELL_STREAM_H

#include "ob_join_cache.h"
#include "ob_cell_stream.h"
#include "ob_row_cell_vec.h"
#include "common/ob_read_common_data.h"
#include "ob_tablet.h"
#include "ob_get_cell_stream.h"

namespace oceanbase
{
  namespace chunkserver
  {
    /**
     * from ObGetCellStream
     */
    class ObJoinGetCellStream : public ObCellStream
    {
    public:
      ObJoinGetCellStream(ObMergerRpcProxy * rpc_proxy,
        const common::ObServerType server_type = common::MERGE_SERVER,
        const int64_t time_out = 0) 
        : ObCellStream(rpc_proxy, server_type, time_out),
          get_cell_stream_(rpc_proxy,server_type,time_out)
      {
        is_finish_ = false;
        is_cached_ = false;
        cache_ = NULL;
        reset_inner_stat();
      }

      virtual ~ObJoinGetCellStream()
      {
      }

    public:
      // next cell
      virtual int next_cell(void);

      // get cell
      virtual int get(const common::ObReadParam &read_param, ObCellArrayHelper & get_cells);

    public:
      // using row cache
      virtual void set_cache(ObJoinCache & cache);

      // get row cache
      virtual int get_cache_row(const common::ObCellInfo & key, ObRowCellVec *& result);

      /**
       * get the current get data version, this function must 
       * be called after next_cell() 
       * 
       * @return int64_t return data version
       */
      virtual int64_t get_data_version() const;

    private:
      // check inner stat
      bool check_inner_stat(void) const;

      // reset inner stat
      void reset_inner_stat(void);

      // get next cell
      int get_next_cell(void);

      // get cell data from server through rpc call
      int get_new_cell_data(void);

      // check whether get operation is all finished
      // param  @finish is finish status
      int check_finish_all_get(bool & finish);

      // add the temp row data to cache and clear result,
      // no matter add cahce succ or failed, just clear it
      int add_cache_clear_result(void);

      // add the row to cache
      //        @result cache value is obscaner
      int add_row_cache(const ObRowCellVec &row);

      // add cell to cache according to whether the current 
      // cell's row key is different from previous cell, if 
      // force is true, add the cell anyway, then clear the 
      // temp row result
      int add_cell_cache(const bool force = true);

    private:
      int get_compact_scanner(const common::ObGetParam& get_param,common::ObScanner& compact_scanner);
      int get_compact_row(chunkserver::ObTablet& tablet,common::ObString& rowkey,common::ObScanner& compact_scanner);
      int fill_compact_data(common::ObIterator& iterator,common::ObScanner& scanner);      

    private:
      DISALLOW_COPY_AND_ASSIGN(ObJoinGetCellStream);
      void output(common::ObScanner& result);

      bool is_finish_;                  // all get data is finished
      bool is_cached_;                  // set cached status
      ObRowCellVec row_result_;         // one row temp result for row_data_cache
      bool cur_row_cache_result_valid_; // is the contents of current cache row valid
      common::ObGetParam param_;        // get param for new rpc call
      ObGetCellStream get_cell_stream_; // get data from ups
      common::ObScanner compact_result_;// data from compactsstable
      int64_t item_index_;              // next get param cell for new rpc call
      ObCellArrayHelper get_cells_;     // get operation cells
      common::ObReadParam read_param_;  // read param for get param
      ObJoinCache * cache_;             // set cache for row cell
    };

    // chek inner stat
    inline bool ObJoinGetCellStream::check_inner_stat(void) const
    {
      return(ObCellStream::check_inner_stat() 
             && ((is_cached_ && NULL != cache_) || (!is_cached_ && (NULL == cache_))));
    }

    // reset inner stat
    inline void ObJoinGetCellStream::reset_inner_stat(void)
    {
      ObCellStream::reset_inner_stat();
      item_index_ = 0;
      is_finish_ = false;
      cur_row_cache_result_valid_ = true;
    }
  }
}

#endif
