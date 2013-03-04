/**
 *  (C) 2010-2011 Taobao Inc.
 *  
 *  This program is free software; you can redistribute it
 *  and/or modify it under the terms of the GNU General Public
 *  License version 2 as published by the Free Software
 *  Foundation.
 *
 *  ob_sstable_block_reader.h is for what ...
 *
 *  Authors:
 *     qushan <qushan@taobao.com>
 *        
 */
#ifndef OCEANBASE_SSTABLE_SSTABLE_BLOCK_READER_H_
#define OCEANBASE_SSTABLE_SSTABLE_BLOCK_READER_H_

#include "common/ob_define.h"
#include "common/ob_string.h"
#include "common/ob_object.h"
#include "ob_sstable_block_builder.h"
#include "ob_sstable_row_cache.h"

namespace oceanbase 
{ 
  namespace sstable 
  {

    class ObSSTableBlockReader
    {
      public:
        ObSSTableBlockReader();
        ~ObSSTableBlockReader();
      public:
        struct IndexEntryType
        {
          int32_t offset_;
          int32_t size_;
        };
        typedef IndexEntryType* iterator;
        typedef const IndexEntryType* const_iterator;
        typedef int RowFormat;
      public:
        class Compare
        {
          public:
            Compare(const ObSSTableBlockReader& reader) : reader_(reader) {}
            bool operator()(const IndexEntryType& index, const common::ObString& key);
          private:
            const ObSSTableBlockReader& reader_;
        };
      public:
        int deserialize(
            char* internal_buffer, const int64_t internal_bufsiz,
            const char* payload_ptr, const int64_t payload_len, int64_t& pos);
        /**
         * get rowkey of sstable row
         * @param index row iterator
         * @param [out] key rowkey at %index
         */
        inline int get_row_key(const_iterator index, common::ObString& key) const
        {
          return deserialize_sstable_rowkey(data_begin_ + index->offset_, index->size_, key);
        }

        /**
         * get whole row content of sstable row, includes rowkey and all columns value
         * @param format sstable row format @see RowFormat
         * @param index row iterator
         * @param [out] key rowkey at %index
         * @param [out] ids SPARSE format stored column id array.
         * @param [out] values columns value object array.
         * @param [in,out] column_count columns count.
         */
        int get_row(const RowFormat format, const_iterator index, 
            common::ObString& key, common::ObObj* ids, 
            common::ObObj* values, int64_t& column_count) const;
        int get_row_columns(const RowFormat format, const_iterator index, 
            common::ObObj* ids, common::ObObj* objs, int64_t& column_count) const;

        int reset();

        const_iterator lower_bound(const common::ObString& key);
        const_iterator find(const common::ObString& key);
        inline const_iterator begin() const { return index_begin_; }
        inline const_iterator end() const { return index_end_; }
        inline int get_row_count() const { return header_.row_count_; }

        //WARNING: this function must be called after deserialize()
        int get_cache_row_value(const_iterator index, 
          ObSSTableRowCacheValue& row_value) const;

      private:
        static int deserialize_sstable_rowkey(const char* buf, 
          const int64_t data_len, common::ObString& key);
        int get_row_columns(const RowFormat format, 
            const char* row_start, const char* row_end, const int64_t rowkey_len, 
            common::ObObj* ids, common::ObObj* objs, int64_t& column_count) const;

      private:
          /** 
           * index_begin_ , index_end_
           * point to block internal index array
           */
          const_iterator index_begin_;
          const_iterator index_end_;
          /**
           * point to row data array
           * every row represents as:
           *   -----------------------------------------------------------------------------------------
           *   ITEM NAME | rowkey length | rowkey byte stream | ObObj vals[0] | vals[column_count-1] |
           *   -----------------------------------------------------------------------------------------
           *   ITEM SIZE | 2bytes        | rowkey length      | length of all values is index[row_index].size
           *   -----------------------------------------------------------------------------------------
           */
          const char* data_begin_;
          const char* data_end_;
          ObSSTableBlockHeader header_;
    };

  } // end namespace sstable
} // end namespace oceanbase


#endif //OCEANBASE_SSTABLE_SSTABLE_BLOCK_READER_H_

