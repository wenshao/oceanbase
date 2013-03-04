/**
 *  (C) 2010-2011 Taobao Inc.
 *  
 *  This program is free software; you can redistribute it
 *  and/or modify it under the terms of the GNU General Public
 *  License version 2 as published by the Free Software
 *  Foundation.
 *
 *  ob_sstable_block_reader.cpp is for what ...
 *
 *  Authors:
 *     qushan <qushan@taobao.com>
 *        
 */
#include "ob_sstable_block_reader.h"
#include "common/serialization.h"
#include "common/ob_malloc.h"
#include "common/ob_tsi_factory.h"
#include "common/page_arena.h"
#include "ob_sstable_trailer.h" 

using namespace oceanbase::common;
using namespace oceanbase::common::serialization;

namespace oceanbase 
{ 
  namespace sstable 
  {
    ObSSTableBlockReader::ObSSTableBlockReader()
      : index_begin_(NULL), index_end_(NULL),
        data_begin_(NULL), data_end_(NULL)
    {
      memset(&header_, 0, sizeof(header_));
    }

    ObSSTableBlockReader::~ObSSTableBlockReader()
    {
      reset();
    }

    int ObSSTableBlockReader::reset()
    {
      index_begin_ = NULL;
      index_end_ = NULL;
      data_begin_ = NULL;
      data_end_ = NULL;
      return OB_SUCCESS;
    }

    int ObSSTableBlockReader::deserialize_sstable_rowkey(const char* buf, 
      const int64_t data_len, ObString& key)
    {
      int ret = OB_SUCCESS;
      int64_t pos = 0;
      int16_t key_length = 0;
      int64_t rowkey_stream_len = data_len - sizeof(int16_t);

      if ((NULL == buf) || (data_len <= 0))
      {
        TBSYS_LOG(ERROR, "invalid argument, data_len=%ld", data_len);
        ret = OB_INVALID_ARGUMENT;
      }
      else if (OB_SUCCESS != (ret = 
            serialization::decode_i16(buf, data_len, pos, &key_length)))
      {
        TBSYS_LOG(ERROR, "decode key length error, "
            "data_len=%ld, pos=%ld, key_length=%d",
            data_len, pos, key_length);
      }
      else if ((key_length <= 0) || (key_length > rowkey_stream_len))
      {
        TBSYS_LOG(ERROR, "error parsing rowkey, key_length(%d) <= 0 "
            "or key_length(%d) > data_len(%ld)\n", 
            key_length, key_length, rowkey_stream_len);
        ret = OB_SIZE_OVERFLOW;
      }
      else
      {
        key.assign_ptr(const_cast<char*>(buf + sizeof(int16_t)), key_length); 
      }
      return ret;
    }

    int ObSSTableBlockReader::deserialize( 
        char* internal_buffer, const int64_t internal_bufsiz,
        const char* data_buffer, const int64_t data_bufsiz, int64_t& pos)
    {
      int ret = OB_SUCCESS; 
      const char* base_ptr = data_buffer + pos;

      if ((NULL == data_buffer) || (data_bufsiz <= 0))
      {
        TBSYS_LOG(ERROR, "invalid argument, databuf=%p,bufsiz=%ld", 
            data_buffer, data_bufsiz);
        ret = OB_INVALID_ARGUMENT;
      }
      else if (OB_SUCCESS != (ret = header_.deserialize(data_buffer, data_bufsiz, pos))) 
      {
        TBSYS_LOG(ERROR, "deserialize header error, databuf=%p,bufsiz=%ld", 
            data_buffer, data_bufsiz);
      }
      else
      {
        data_begin_  = base_ptr;
        data_end_    = (base_ptr + header_.row_index_array_offset_);

        int64_t index_entry_length = sizeof(IndexEntryType) * (header_.row_count_ + 1);
        char* internal_buffer_ptr = internal_buffer;

        // if fixed length buffer not enough, reallocate memory for use;
        // in case some block size larger than normal block.
        if (index_entry_length > internal_bufsiz)
        {
          TBSYS_LOG(WARN, "block need size = %ld > input internal bufsiz=%ld, realloc", 
              index_entry_length, internal_bufsiz);
          internal_buffer_ptr = GET_TSI_MULT(common::ModuleArena, TSI_SSTABLE_MODULE_ARENA_1)->alloc_aligned(index_entry_length);
        }

        if (NULL != internal_buffer_ptr)
        {
          iterator index_entry_ptr = reinterpret_cast<iterator>(internal_buffer_ptr);
          index_begin_ = index_entry_ptr;
          index_end_ = index_begin_ + header_.row_count_ + 1;

          // okay, start parse index entry(row start offset relative to payload buffer)
          pos +=  header_.row_index_array_offset_ - sizeof(header_);
          int32_t offset = 0;
          iterator index = index_entry_ptr;
          for (int i = 0; i < (header_.row_count_ + 1) && OB_SUCCESS == ret; ++i)
          {
            ret = decode_i32(data_buffer, data_bufsiz, pos, &offset); 
            if ((OB_SUCCESS == ret) && ((index_begin_ + i) < index_end_))
            {
              index[i].offset_ = offset;
              if (i > 0)
              {
                index[i - 1].size_ = offset - index[i - 1].offset_;
              }
            }
            else
            {
              TBSYS_LOG(ERROR, "analysis of the index data failed,"
                  "i=%d, ret=%d, row count=%d\n", i, ret, header_.row_count_);
              ret = OB_DESERIALIZE_ERROR;
            }
          } 


          --index_end_; // last index entry not used.

        }
        else
        {
          TBSYS_LOG(ERROR, "block internal buffer allocate error,sz=%ld", index_entry_length);
          ret = OB_ALLOCATE_MEMORY_FAILED;
        }
      }
      return ret;
    }

    inline bool ObSSTableBlockReader::Compare::operator()(
        const IndexEntryType& index, const ObString& key)
    {
      ObString row_key;
      reader_.get_row_key(&index, row_key);
      return row_key.compare(key) < 0;
    }

    ObSSTableBlockReader::const_iterator ObSSTableBlockReader::lower_bound(const ObString& key)
    {
      return std::lower_bound(index_begin_, index_end_, key, Compare(*this));
    }

    ObSSTableBlockReader::const_iterator ObSSTableBlockReader::find(const ObString& key)
    {
      ObSSTableBlockReader::const_iterator iter = end();
      ObString row_key;

      iter = std::lower_bound(index_begin_, index_end_, key, Compare(*this));
      if (end() != iter)
      {
        if (OB_SUCCESS != get_row_key(iter, row_key))
        {
          iter = end();
        }
        else
        {
          if (row_key != key)
          {
            iter = end();
          }
        }
      }

      return iter;
    }

    int ObSSTableBlockReader::get_row(
        const RowFormat format, const_iterator index, 
        common::ObString& key, common::ObObj* ids, 
        common::ObObj* values, int64_t& column_count) const
    {
      const char* row_start = data_begin_ + index->offset_;
      const char* row_end = row_start + index->size_;
      int ret = OB_SUCCESS;

      if (OB_SUCCESS != (ret = get_row_key(index, key)))
      {
        TBSYS_LOG(ERROR, "get row key error, ret=%d, format=%d, "
                         "index->offset_=%d, index->size_=%d", 
            ret, format, index->offset_, index->size_);
      }
      else
      {
        ret = get_row_columns(format, row_start, row_end, 
            key.length(), ids, values, column_count);
      }
      return ret;
    }

    int ObSSTableBlockReader::get_row_columns(
        const RowFormat format, const_iterator index, 
        common::ObObj* ids, common::ObObj* values, int64_t& column_count) const
    {
      const char* row_start = data_begin_ + index->offset_;
      const char* row_end = row_start + index->size_;

      int64_t pos = 0;
      int16_t key_length = 0;
      int ret = OB_SUCCESS;

      if (OB_SUCCESS != (ret = serialization::decode_i16(
              row_start, index->size_, pos, &key_length)))
      {
        TBSYS_LOG(ERROR, "decode key length error, ret=%d, "
            "pos=%ld, format=%d, index->offset_=%d, index->size_=%d", 
            ret, pos, format, index->offset_, index->size_);
      }
      else
      {
        ret = get_row_columns(format, row_start, row_end, key_length, ids, values, column_count);
      }

      return ret;
    }

    inline int ObSSTableBlockReader::get_row_columns(const RowFormat format, 
        const char* row_start, const char* row_end, const int64_t rowkey_len, 
        common::ObObj* ids, common::ObObj* values, int64_t& column_count) const
    {
      int ret = OB_SUCCESS;
      int64_t column_index = 0;
      int64_t column_size = column_count; //avoid CPU L2 cache miss
      int64_t size = row_end - row_start;

      if (NULL == row_start || NULL == row_end || rowkey_len <= 0 
          || NULL == values || column_size <= 0)
      {
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        // skip (length of rowkey) to ObObj array.
        int64_t pos = sizeof(int16_t) + rowkey_len;

        while (row_start + pos < row_end)
        {
          if (column_index >= column_size) 
          {
            ret = OB_SIZE_OVERFLOW;
            break;
          }

          if (format == OB_SSTABLE_STORE_SPARSE)
          {
            ret = (ids + column_index)->deserialize(row_start, size, pos);
            if (OB_SUCCESS != ret || ids[column_index].get_type() != ObIntType)
            {
              TBSYS_LOG(ERROR, "deserialize id object error, ret=%d, "
                  "column_index=%ld, row_start=%p, pos=%ld, size=%ld",
                  ret, column_index, row_start, pos, size);
              ret = OB_DESERIALIZE_ERROR;
              break;
            }
          }

          if (OB_SUCCESS != (ret = 
                (values + column_index)->deserialize(row_start, size, pos)) )
          {
            TBSYS_LOG(ERROR, "deserialize column value object error, ret=%d, "
                "column_index=%ld, row_start=%p, pos=%ld, size=%ld",
                ret, column_index, row_start, pos, size);
            break;
          }
          else
          { 
            ++column_index; 
          }
        }
        if (OB_SUCCESS == ret) column_count = column_index;
      }

      return ret;
    }

    int ObSSTableBlockReader::get_cache_row_value(const_iterator index, 
      ObSSTableRowCacheValue& row_value) const
    {
      int ret = OB_SUCCESS;
      int64_t pos = 0;
      int16_t key_length = 0;
      int64_t seri_key_len = 0;

      if (NULL == index || NULL == data_begin_)
      {
        TBSYS_LOG(WARN, "invalid param, row_index=%p, row_begin=%p",
          index, data_begin_);
        ret = OB_INVALID_ARGUMENT;
      }
      else if (OB_SUCCESS != (ret = serialization::decode_i16(
              data_begin_ + index->offset_, index->size_, pos, &key_length)))
      {
        TBSYS_LOG(ERROR, "decode key length error, ret=%d, "
            "pos=%ld, index->offset_=%d, index->size_=%d", 
            ret, pos, index->offset_, index->size_);
      }
      else
      {
        seri_key_len = sizeof(int16_t) + key_length;
        if (seri_key_len <= index->size_)
        {
          row_value.buf_ = const_cast<char*>(data_begin_ + index->offset_ + seri_key_len);
          row_value.size_ = index->size_ - seri_key_len;
        }
        else 
        {
          TBSYS_LOG(WARN, "after deserialize, the key length is greater than row size, "
                          "serialized_key_len=%ld, row_size=%ld",
            seri_key_len, row_value.size_);
          ret = OB_ERROR;
        }
      }

      return ret;
    }
  } // end namespace sstable
} // end namespace oceanbase
