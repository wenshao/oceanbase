/**
 *  (C) 2010-2011 Taobao Inc.
 *  
 *  This program is free software; you can redistribute it
 *  and/or modify it under the terms of the GNU General Public
 *  License version 2 as published by the Free Software
 *  Foundation.
 *
 *  ob_sstable_block_scanner.h is for what ...
 *
 *  Authors:
 *     qushan <qushan@taobao.com>
 *        
 */
#ifndef OCEANBASE_SSTABLE_OB_SSTABLE_BLOCK_SCANNER_V2_H_
#define OCEANBASE_SSTABLE_OB_SSTABLE_BLOCK_SCANNER_V2_H_

#include <string>
#include <tblog.h>
#include "common/ob_define.h"
#include "common/ob_string.h"
#include "common/ob_common_param.h"
#include "ob_sstable_block_reader.h"

namespace oceanbase
{
  namespace sstable
  {
    class ObScanColumnIndexes;
    class ObSSTableBlockScanner
    {
      public:
        struct BlockData
        {
          char* internal_buffer_;
          int64_t internal_bufsiz_;
          const char* data_buffer_;
          int64_t data_bufsiz_;
          int64_t store_style_;

          BlockData() 
            :  internal_buffer_(NULL), internal_bufsiz_(0), 
            data_buffer_(NULL),  data_bufsiz_(0), store_style_(0) 
          {
          }

          BlockData(char* ib, const int64_t ibsz, 
              const char* db, const int64_t dbsz, const int64_t store_style)
            : internal_buffer_(ib), internal_bufsiz_(ibsz), 
            data_buffer_(db),  data_bufsiz_(dbsz), store_style_(store_style)
          {
          }

          inline bool available() const 
          {
            return NULL != internal_buffer_ && 0 < internal_bufsiz_ 
              && NULL != data_buffer_ &&  0 < data_bufsiz_ && 0 < store_style_;
          }
        };
      public:
        ObSSTableBlockScanner(const ObScanColumnIndexes& column_indexes);
        ~ObSSTableBlockScanner();

        /**
         * step to next cell of sstable block.
         * @return 
         *  OB_SUCCESS on success and still has more cells.
         *  OB_ITER_END on success and reach end of block.
         *  otherwise failed.
         */
        int next_cell();

        /**
         * get current cell of sstable block .
         * you need call next_cell first.
         * @param [out] cell_info point to current cell, user need 
         * copy %cell_info 's content to its own buffer. once call
         * next_cell again, current cell_info will be overwrited.
         */
        inline int get_cell(oceanbase::common::ObCellInfo** cell)
        {
          return get_cell(cell, NULL);
        }

        inline int get_cell(oceanbase::common::ObCellInfo** cell, bool* is_row_changed)
        {
          int ret = common::OB_SUCCESS;
          if (NULL == cell)
          {
            TBSYS_LOG(ERROR, "invalid argument, cell=%p", cell);
            ret = common::OB_INVALID_ARGUMENT;
          }
          else if (common::OB_SUCCESS != initialize_status_)
          {
            TBSYS_LOG(ERROR, "initialize status error=%ld", initialize_status_);
            ret = static_cast<int32_t>(initialize_status_);
          }
          else
          {
            *cell = &current_cell_info_;
            if (NULL != is_row_changed) 
            {
              *is_row_changed = is_row_changed_;
            }
          }
          return ret;
        }

        /**
         * @param [in] range scan range(start key, end key, border flag).
         * @param [in] block_data_buf sstable block data buffer.
         * @param [in] block_data_len size of %block_data_buf
         * @param [out] need_looking_forward scan reach the end
         * @param not_exit_col_ret_nop [in] whether return nop if column
         *                             doesn't exit  
         * @return OB_SUCCESS on success, otherwise failed.
         */
        int set_scan_param(const oceanbase::common::ObRange& range, 
            const bool is_reverse_scan, const BlockData& block_data, 
            bool &need_looking_forward, bool not_exit_col_ret_nop = false);

      private:
        typedef ObSSTableBlockReader::const_iterator const_iterator;
        typedef ObSSTableBlockReader::iterator iterator;
      private:
        int load_current_row(const_iterator row_index);
        int store_sparse_column(const int64_t column_index);
        int store_current_cell(const int64_t column_index);
        int store_and_advance_column();
        int get_current_column_index(const int64_t cursor, 
            uint64_t& column_id, int64_t& column_index) const;

        void next_row();
        bool start_of_block();
        bool end_of_block();

        int initialize(const bool is_reverse_scan, const int64_t store_style, 
          const bool not_exit_col_ret_nop);

        int locate_start_pos(const common::ObRange& range,
            const_iterator& start_iterator, bool& need_looking_forward);
        int locate_end_pos(const common::ObRange& range,
            const_iterator& last_iterator, bool& need_looking_forward);

      private:
        DISALLOW_COPY_AND_ASSIGN(ObSSTableBlockScanner);

      private:
        int64_t initialize_status_;
        int64_t sstable_data_store_style_;
        bool    is_reverse_scan_;
        bool    is_row_changed_;
        bool    handled_del_row_;
        bool    not_exit_col_ret_nop_;

        int64_t column_cursor_;
        int64_t current_column_count_;
        const ObScanColumnIndexes& query_column_indexes_;

        const_iterator row_cursor_;
        const_iterator row_start_index_;
        const_iterator row_last_index_;
        
        common::ObCellInfo current_cell_info_;
        common::ObObj current_ids_[common::OB_MAX_COLUMN_NUMBER];
        common::ObObj current_columns_[common::OB_MAX_COLUMN_NUMBER];

        ObSSTableBlockReader reader_;
    };
  }//end namespace sstable
}//end namespace oceanbase

#endif //OCEANBASE_SSTABLE_OB_SSTABLE_BLOCK_SCANNER_V2_H_
