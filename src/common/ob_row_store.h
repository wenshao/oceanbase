/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_row_store.h
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#ifndef _OB_ROW_STORE_H
#define _OB_ROW_STORE_H 1
#include <stdint.h>
#include <utility>
#include "ob_row.h"
#include "ob_row_iterator.h"
#include "common/ob_array.h"
#include "common/ob_string.h"
#include "ob_ups_row.h"
#include "ob_ups_row_util.h"

namespace oceanbase
{
  namespace common
  {
    class ObRowStore
    {
      private:
        struct BlockInfo;
        class Iterator;
      public:
        struct StoredRow
        {
          int32_t compact_row_size_;
          int32_t reserved_cells_count_;
          common::ObObj reserved_cells_[0];
          // ... compact_row
          const common::ObString get_compact_row() const;
        };
      public:
        ObRowStore();
        ~ObRowStore();
        int add_reserved_column(uint64_t tid, uint64_t cid);
        void clear();
        /**
         * add row into the store
         *
         * @param row [in]
         * @param sort_row [out] stored row
         *
         * @return error code
         */
        int add_row(const ObRow &row, const StoredRow *&stored_row);
        /**
         * add row into the store
         *
         * @param row [in]
         * @param cur_size_counter [out] total mem used by the store
         *
         * @return error code
         */
        int add_row(const ObRow &row, int64_t &cur_size_counter);

        int add_row(const ObString &rowkey, const ObRow &row, const StoredRow *&stored_row);

        int add_ups_row(const ObUpsRow &row, const StoredRow *&stored_row);
        int add_ups_row(const ObUpsRow &row, int64_t &cur_size_counter);

        int add_ups_row(const ObString &rowkey, const ObUpsRow &row, const StoredRow *&stored_row);

        bool is_empty() const;
        int64_t get_used_mem_size() const;

        int get_next_row(ObRow &row, common::ObString *compact_row = NULL);
        int get_next_row(ObString *rowkey, ObRow &row, common::ObString *compact_row = NULL);
        int get_next_ups_row(ObUpsRow &row, common::ObString *compact_row = NULL);
        int get_next_ups_row(ObString *rowkey, ObUpsRow &row, common::ObString *compact_row = NULL);
        void reset_iterator();

        NEED_SERIALIZE_AND_DESERIALIZE;
      private:
        static const int64_t BLOCK_SIZE = 2*1024*1024L; // 2MB
      private:
        int next_iter_pos(BlockInfo *&iter_block, int64_t &iter_pos);
        int new_block();
        int64_t get_reserved_cells_size(const int64_t reserved_columns_count) const;
        int64_t get_compact_row_min_size(const int64_t row_columns_count) const;
        int add_row(const ObString *rowkey, const ObRow &row, const StoredRow *&stored_row, int64_t &cur_size_counter);

        // @return OB_SIZE_OVERFLOW if buffer not enough
        int append_row(const ObString *rowkey, const ObRow &row, BlockInfo &block, StoredRow &stored_row);
      private:
        common::ObArray<std::pair<uint64_t, uint64_t> > reserved_columns_;
        BlockInfo *block_list_head_;
        BlockInfo *block_list_tail_;
        int64_t block_count_;
        int64_t cur_size_counter_;
        bool got_first_next_;
        int64_t cur_iter_pos_;
        BlockInfo *cur_iter_block_;
    };

    inline int64_t ObRowStore::get_reserved_cells_size(const int64_t reserved_columns_count) const
    {
      return sizeof(StoredRow) + (sizeof(common::ObObj) * reserved_columns_count);
    }

    inline int64_t ObRowStore::get_compact_row_min_size(const int64_t row_columns_count) const
    {
      // 4 ==  SUM( len(TypeAttr) = 1, len(int8) = 1, len(column id) = 2 )
      // 8 is a padding value/magic number, try to avoid a useless deserialization when reaching the end of a block
      return 4 * row_columns_count + 8;
    }

    inline const common::ObString ObRowStore::StoredRow::get_compact_row() const
    {
      common::ObString ret;
      ret.assign_ptr(reinterpret_cast<char*>(const_cast<common::ObObj*>(&reserved_cells_[reserved_cells_count_])), compact_row_size_);
      return ret;
    }

    inline bool ObRowStore::is_empty() const
    {
      return block_count_ == 0;
    }
  } // end namespace common
} // end namespace oceanbase

#endif /* _OB_ROW_STORE_H */
