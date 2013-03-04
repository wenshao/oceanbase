/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * ob_merge_join_agent.h is for what ...
 *
 * Version: $id: ob_merge_join_agent.h,v 0.1 9/17/2010 11:50a wushi Exp $
 *
 * Authors:
 *   wushi <wushi.ly@taobao.com>
 *     - some work details if you want
 *
 */
#ifndef OCEANBASE_MERGESERVER_OB_MERGE_JOIN_AGENT_IMP_H_ 
#define OCEANBASE_MERGESERVER_OB_MERGE_JOIN_AGENT_IMP_H_
#include <vector>
#include "ob_cell_stream.h"
#include "ob_ms_get_cell_stream.h"
#include "ob_ms_scan_cell_stream.h"
#include "ob_ms_rpc_proxy.h"
#include "ob_groupby_operator.h"
#include "common/ob_iterator.h"
#include "common/ob_vector.h"
#include "common/ob_schema.h"
#include "common/ob_string.h"
#include "common/ob_merger.h"
#include "common/ob_scanner.h"
#include "common/ob_cell_array.h"
#include "common/ob_define.h"
#include "common/hash/ob_hashmap.h"
#include "common/ob_simple_right_join_cell.h"
namespace oceanbase
{
  namespace mergeserver
  {
    /// @class  do real jobs like merge, join; but it only represent stage result
    ///   ObMergeJoinAgent will merge stage result into final result
    /// @author wushi(wushi.ly@taobao.com)  (9/17/2010)
    class ObMergeJoinOperator : public common::ObCellArray
    {
      public:
        ObMergeJoinOperator();
        ~ObMergeJoinOperator();
      public:
        void init(ObMergerRpcProxy &proxy)
        {
          rpc_proxy_ = &proxy;
        }
        /// @fn set request parameter
        /// @param max_memory_size if the intermediate results take memory size more than this 
        ///   stop process, and continue when user call this function next time
        int set_request_param(const int64_t timeout_us, const common::ObScanParam &scan_param,
            ObMSScanCellStream &ups_stream, ObMSGetCellStream &ups_join_stream, 
            const common::ObSchemaManagerV2 &schema_mgr, const int64_t max_memory_size);
        int set_request_param(const int64_t timeout_us, const common::ObGetParam &get_param,
            ObMSGetCellStream &ups_stream, ObMSGetCellStream &ups_join_stream,
            const common::ObSchemaManagerV2 &schema_mgr, const int64_t max_memory_size);
        /// @fn clear all result stored
        void clear();
        /// @fn reset intermediate result
        void prepare_next_round();
        /// @fn check if result has been finised yet
        bool is_request_finished() const;
        /// @fn next cell
        int next_cell();
        // @fn change join value type and value
        int ob_change_join_value_type(ObObj & obj);
        int ob_get_join_value(common::ObObj & res_out, const common::ObObj & left_value_in,
            const common::ObObj & right_value_in);
      private:
        const common::ObSchemaManagerV2 *schema_mgr_;
        ObMSScanCellStream *      ups_scan_stream_;
        ObMSGetCellStream *       ups_get_stream_;
        ObMSGetCellStream *       ups_join_stream_;
        int64_t                   max_memory_size_;
        bool                      request_finished_;
        bool                      cs_request_finished_;
        static char               ugly_fake_get_param_rowkey_;
        common::ObGetParam        fake_get_param_;
        const common::ObScanParam *scan_param_;
        const common::ObGetParam  *get_param_;
        /// @property when get, req_param_ == get_param_; when scan req_param_ == & fake_get_param_
        const common::ObGetParam  *req_param_;

      private:
        int64_t start_time_us_;
        int64_t timeout_us_;

      private:
        bool                       merger_iterator_moved_;
        ObMergerTabletLocation     cur_cs_addr_;
        common::ObStringBuf        cur_row_key_buf_;
        common::ObMerger           merger_;
        /// properties for get request
        common::ObGetParam         cur_get_param_;
        int64_t                    got_cell_num_;
        /// properties for scan request
        common::ObScanParam        cur_scan_param_;
        common::ObMemBuffer        cs_scan_buffer_; 
        common::ObMemBuffer        ups_scan_buffer_; 
        common::ObScanner          cur_cs_result_;
        ObMergerRpcProxy*          rpc_proxy_;
        /// properties for join
        bool                       is_scan_query_;
        bool                       join_only_one_table_;
        bool                       param_contain_duplicated_columns_;
        int64_t                    left_join_column_count_;
        int32_t                    column_id_idx_map_[common::OB_MAX_VALID_COLUMN_ID];
        int32_t                    left_column_idx_map_[common::OB_MAX_VALID_COLUMN_ID];
        const common::ObColumnSchemaV2::ObJoinInfo * column_id_join_map_[common::OB_MAX_VALID_COLUMN_ID];
        ObInnerCellInfo            ugly_random_apply_cell_info_;
        int64_t                    cur_rpc_offset_beg_;
        common::ObReadParam        cur_join_read_param_;
        bool                       is_need_query_ups_;
        bool                       is_need_join_;
	      common::ObVector<ObSimpleRightJoinCell> join_cell_vec_;
        common::ObVector<int64_t>  join_row_width_vec_;
        common::ObVector<int64_t>  join_offset_vec_;

      private:
        int do_merge_join(); 
        int merge();  
        int merge_next_row(int64_t &cur_row_beg, int64_t &cur_row_end);
        int apply_whole_row(const common::ObCellInfo &cell, const int64_t row_beg);
        int prepare_join_param_();
        int join();
        void initialize();
        int get_next_rpc_result();
        int get_next_get_rpc_result();
        int get_next_scan_rpc_result();
        // join one cell
        int join_apply_one_cell(const common::ObCellInfo & cell, const int64_t offset);
        // join all the column cells
        template<typename IteratorT>
          int join_apply_all_cell(const common::ObCellInfo & cell,
              IteratorT & dst_off_beg, IteratorT & dest_off_end);
        // new join
        template<typename IteratorT>
          int join_apply_directly(const common::ObCellInfo & cell, const int64_t cell_count,
              IteratorT & dst_off_beg, IteratorT & dst_off_end);
        // old join
        template<typename IteratorT>
          int join_apply(const common::ObCellInfo & cell,
              IteratorT & dst_off_beg, IteratorT & dst_off_end);
        // get join info
        const common::ObColumnSchemaV2::ObJoinInfo * get_join_info(
            const uint64_t table_id, const uint64_t column_id);
        // move the next row 
        void move_to_next_row(const common::ObGetParam & row_spec_arr,
            const int64_t row_spec_arr_size, int64_t &cur_row_beg,
            int64_t &cur_row_end);
      private:
        common::ObCellInfo join_apply_cell_adjusted_;
    };

    /// @class  just encapsulate ObMergeJoinOperator
    class ObGetMergeJoinAgentImp//:public common::ObIterator
    {
      public:
        ~ObGetMergeJoinAgentImp();
      public:
        void init(ObMergerRpcProxy &proxy)
        {
          merge_join_operator_.init(proxy);
        }
        int get_cell(common::ObCellInfo * *cell);
        int get_cell(common::ObCellInfo * *cell, bool *is_row_changed);
        int next_cell();
        int set_request_param(const int64_t timeout_us, const common::ObGetParam &get_param, 
            ObMSGetCellStream &ups_stream, 
            ObMSGetCellStream &ups_join_stream, 
            const common::ObSchemaManagerV2 &schema_mgr,
            const int64_t max_memory_size); 
        void clear();
        bool is_request_fullfilled();
      private:
        common::ObCellInfo ugly_cell_for_seq_access_;
        ObMergeJoinOperator merge_join_operator_;
    };

    /// @class encapsulate ObMergeJoinOperator and implement order by, limit, 
    ///   and multiple times scan intermediate result
    class ObScanMergeJoinAgentImp//:public common::ObIterator
    {
      public:
        ObScanMergeJoinAgentImp();
        ~ObScanMergeJoinAgentImp();
      public:
        void init(ObMergerRpcProxy &proxy)
        {
          merge_join_operator_.init(proxy);
        }
        int get_cell(common::ObCellInfo * *cell);
        int get_cell(common::ObCellInfo * *cell, bool *is_row_changed);
        int next_cell();
        int set_request_param(const int64_t timeout_us, const common::ObScanParam &scan_param, 
            ObMSScanCellStream &ups_stream, 
            ObMSGetCellStream &ups_join_stream, 
            const common::ObSchemaManagerV2 &schema_mgr,
            const int64_t max_memory_size); 
        void clear();
        bool is_request_fullfilled();

        static void set_return_uncomplete_result(bool return_uncomplete_result)
        {
          return_uncomplete_result_  = return_uncomplete_result;
        }
      private:
        static bool return_uncomplete_result_;
        int filter_org_row_(const common::ObCellArray &cells, const int64_t row_beg,
            const int64_t row_end, const common::ObSimpleFilter & filter,
            bool &result);
        int jump_limit_offset_(common::ObCellArray & cells, const int64_t jump_cell_num);
        int prepare_final_result_();
        int prepare_final_result_process_intermediate_result_();
      private:
        const common::ObScanParam *     param_;
        ObMergeJoinOperator             merge_join_operator_;
        ObGroupByOperator               groupby_operator_;
        common::ObCellArray *           pfinal_result_;
        int64_t                         max_avail_mem_size_;
        int64_t                         limit_offset_;
        int64_t                         limit_count_;
        int64_t                         max_avail_cell_num_;
        common::ObCellInfo              ugly_cell_for_seq_access_;
        common::ObCellArray::OrderDesc  orderby_desc_[common::OB_MAX_COLUMN_NUMBER];
    };

    inline int ObMergeJoinOperator::next_cell()
    {
      int err = common::OB_SUCCESS;
      err = ObCellArray::next_cell();
      while (common::OB_ITER_END == err && !request_finished_)
      {
        if (!merger_iterator_moved_)
        {
          ObCellArray::clear();
          err = get_next_rpc_result();
          if (common::OB_SUCCESS == err)
          {
            err = ObCellArray::next_cell();
          }
        }
        else
        {
          ObCellArray::clear();
          err = do_merge_join();
          TBSYS_LOG(DEBUG, "%s", "merge left cells");
          if (common::OB_SUCCESS == err)
          {
            err = ObCellArray::next_cell();
          }
        }
      }
      return err;
    }
    
    inline int ObGetMergeJoinAgentImp::get_cell(common::ObCellInfo** cell)
    {
      return get_cell(cell, NULL);
    }
    inline int ObGetMergeJoinAgentImp::get_cell(common::ObCellInfo** cell,
        bool *is_row_changed_)
    {
      common::ObInnerCellInfo * inner_cell = NULL;
      int ret = merge_join_operator_.get_cell(&inner_cell, is_row_changed_);
      if (OB_SUCCESS == ret)
      {
        ugly_cell_for_seq_access_.table_id_ = inner_cell->table_id_;
        ugly_cell_for_seq_access_.row_key_ = inner_cell->row_key_;
        ugly_cell_for_seq_access_.column_id_ = inner_cell->column_id_;
        ugly_cell_for_seq_access_.value_ = inner_cell->value_;
        *cell = &ugly_cell_for_seq_access_;
      }
      return ret;
    }
    inline int ObGetMergeJoinAgentImp::next_cell()
    {
      return merge_join_operator_.next_cell();
    }
    inline void ObGetMergeJoinAgentImp::clear()
    {
      merge_join_operator_.clear();
    }
    inline bool ObGetMergeJoinAgentImp::is_request_fullfilled()
    {
      return merge_join_operator_.is_request_finished();
    }
    //
    inline bool ObScanMergeJoinAgentImp::is_request_fullfilled()
    {
      return merge_join_operator_.is_request_finished();
    }
    inline void ObScanMergeJoinAgentImp::clear()
    {
      merge_join_operator_.clear();
      groupby_operator_.clear();
      pfinal_result_ = NULL;
      max_avail_mem_size_ = -1;
      param_ = NULL;
      limit_count_ = 0;
      limit_offset_ = 0;
      max_avail_cell_num_ = -1;
    }
    inline int ObScanMergeJoinAgentImp::next_cell()
    {
      int err = common::OB_SUCCESS;
      if (NULL == pfinal_result_)
      {
        err = prepare_final_result_();
      }
      if (common::OB_SUCCESS == err)
      {
        err = pfinal_result_->next_cell();
        if (common::OB_SUCCESS == err && max_avail_cell_num_ >= 0)
        {
          if (max_avail_cell_num_ == 0)
          {
            err = common::OB_ITER_END;
          }
          else
          {
            --max_avail_cell_num_;
          }
        }
      }
      return err;
    }
    inline int ObScanMergeJoinAgentImp::get_cell(common::ObCellInfo** cell)
    {
      return get_cell(cell, NULL);
    }
    inline int ObScanMergeJoinAgentImp::get_cell(common::ObCellInfo** cell, bool *is_row_changed)
    {
      int err = common::OB_SUCCESS;
      if (NULL != pfinal_result_)
      {
        common::ObInnerCellInfo * inner_cell = NULL;
        err = pfinal_result_->get_cell(&inner_cell, is_row_changed);
        if (OB_SUCCESS == err)
        {
          ugly_cell_for_seq_access_.table_id_ = inner_cell->table_id_;
          ugly_cell_for_seq_access_.row_key_ = inner_cell->row_key_;
          ugly_cell_for_seq_access_.column_id_ = inner_cell->column_id_;
          ugly_cell_for_seq_access_.value_ = inner_cell->value_;
          *cell = &ugly_cell_for_seq_access_;
        }
      }
      else
      {
        *cell = NULL;
        err = common::OB_INVALID_ARGUMENT;
      }
      return err;
    }

    inline int ObMergeJoinOperator::ob_change_join_value_type(ObObj & obj)
    {
      int err = OB_SUCCESS;
      int64_t value = -1;
      if (obj.get_type() == ObCreateTimeType)
      {
        err = obj.get_createtime(value);
        if (OB_SUCCESS == err)
        {
          obj.set_precise_datetime(value);
        }
      }
      else if (obj.get_type() == ObModifyTimeType)
      {
        err = obj.get_modifytime(value);
        if (OB_SUCCESS == err)
        {
          obj.set_precise_datetime(value);
        }
      }
      return err;
    }

    inline int ObMergeJoinOperator::ob_get_join_value(ObObj &res_out, const ObObj & left_value_in,
        const ObObj &right_value_in)
    {
      int err = OB_SUCCESS;
      if ((ObNullType == left_value_in.get_type()) 
          || ((ObCreateTimeType != right_value_in.get_type())
            && (ObModifyTimeType != right_value_in.get_type())
            )
         )
      {
        res_out = right_value_in;
        err = ob_change_join_value_type(res_out);
      }
      else if (ObCreateTimeType == right_value_in.get_type())
      {
        ObCreateTime right_time;
        ObPreciseDateTime left_time;
        ObPreciseDateTime res_time;
        err = right_value_in.get_createtime(right_time);
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN,"fail to get create time from right value [err:%d]", err);
        }
        if (OB_SUCCESS == err)
        {
          err = left_value_in.get_precise_datetime(left_time);
          if (OB_SUCCESS != err)
          {
            TBSYS_LOG(WARN,"fail to get precise time from left value [err:%d]", err);
          }
        }
        if (OB_SUCCESS == err)
        {
          res_time = std::min<int64_t>(right_time, left_time);
          res_out.set_precise_datetime(res_time);
        }
      }
      else if (ObModifyTimeType == right_value_in.get_type())
      {
        ObModifyTime right_time;
        ObPreciseDateTime left_time;
        ObPreciseDateTime res_time;
        err = right_value_in.get_modifytime(right_time);
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN,"fail to get modify time from right value [err:%d]", err);
        }
        if (OB_SUCCESS == err)
        {
          err = left_value_in.get_precise_datetime(left_time);
          if (OB_SUCCESS != err)
          {
            TBSYS_LOG(WARN,"fail to get precise time from left value [err:%d]", err);
          }
        }
        if (OB_SUCCESS == err)
        {
          res_time = std::max<int64_t>(right_time, left_time);
          res_out.set_precise_datetime(res_time);
        }
      }
      return err;
    }

    inline const ObColumnSchemaV2::ObJoinInfo * ObMergeJoinOperator::get_join_info(
        const uint64_t table_id, const uint64_t column_id)
    {
      const ObColumnSchemaV2::ObJoinInfo * join_info = NULL;
      if (true == is_scan_query_)
      {
        join_info = column_id_join_map_[column_id];
      }
      else
      {
        const ObColumnSchemaV2 * column_schema = schema_mgr_->get_column_schema(table_id, column_id);
        if (NULL != column_schema)
        {
          join_info = column_schema->get_join_info();
        }
      }
      return join_info;
    }
  }
}

#endif /* MERGESERVER_OB_MERGE_JOIN_AGENT_IMP_H_ */
