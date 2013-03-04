/**
 *  (C) 2010-2011 Taobao Inc.
 *  
 *  This program is free software; you can redistribute it
 *  and/or modify it under the terms of the GNU General Public
 *  License version 2 as published by the Free Software
 *  Foundation.
 *
 *  ob_sstable_scan_param.h is for what ...
 *
 *  Authors:
 *     qushan < qushan@taobao.com >
 *        
 */



#ifndef OCEANBASE_SSTABLE_OB_SSTABLE_SCAN_PARAM_H_
#define OCEANBASE_SSTABLE_OB_SSTABLE_SCAN_PARAM_H_

#include "common/ob_scan_param.h"
#include "common/ob_array_helper.h"
#include "common/ob_read_common_data.h"

namespace oceanbase
{
  namespace sstable
  {
    class ObSSTableScanParam : public common::ObReadParam
    {
      public:
        ObSSTableScanParam();
        //ObSSTableScanParam(const common::ObScanParam &param);
        //ObSSTableScanParam & operator=(const common::ObScanParam &param);
        ~ObSSTableScanParam();
        inline const common::ObRange & get_range() const { return range_; }
        common::ObRange & get_range() { return range_; }
        inline uint64_t get_table_id() const { return range_.table_id_; }
        inline const uint64_t* const get_column_id() const { return column_ids_; }

        inline int64_t get_column_id_size() const 
        { return column_id_list_.get_array_index(); }

        inline const common::ObArrayHelper<uint64_t> & get_column_id_list() const 
        { return column_id_list_; }

        //inline void add_column_id(uint64_t column_id) 
        //{ column_id_list_.push_back(column_id); }
        //
        inline bool is_reverse_scan() const
        {
          return scan_direction_ == common::ObScanParam::BACKWARD;
        }
        inline bool is_sync_read() const
        {
          return read_mode_ == common::ObScanParam::SYNCREAD;
        }
        inline int16_t get_read_mode() const
        {
          return read_mode_;
        }
        inline void set_not_exit_col_ret_nop(bool not_exit_col_ret_nop)
        {
          not_exit_col_ret_nop_ = not_exit_col_ret_nop;
        }
        inline bool is_not_exit_col_ret_nop() const
        {
          return not_exit_col_ret_nop_;
        }

        int assign(const common::ObScanParam & param);
        void reset();
        bool is_valid() const;

      private:
        common::ObRange range_;
        int16_t scan_direction_;
        int16_t read_mode_;
        bool not_exit_col_ret_nop_;
        uint64_t column_ids_[common::OB_MAX_COLUMN_NUMBER];
        common::ObArrayHelper<uint64_t> column_id_list_;
    };
  }
}


#endif // OCEANBASE_SSTABLE_OB_SSTABLE_SCAN_PARAM_H_
