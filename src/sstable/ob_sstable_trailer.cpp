/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_sstable_trailer.cpp for define sstable trailer. 
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */
#include <tbsys.h>
#include "ob_sstable_trailer.h"

namespace oceanbase
{
  namespace sstable
  {
    using namespace common;
    using namespace common::serialization;

    ObSSTableTrailer::ObSSTableTrailer() 
    {
      memset(this, 0, sizeof(ObSSTableTrailer));
#ifdef COMPATIBLE
      key_buf_size_ = DEFAULT_KEY_BUF_SIZE;
      own_key_buf_ = false;
#endif
    }

    ObSSTableTrailer::~ObSSTableTrailer()
    {
#ifdef COMPATIBLE
      if (own_key_buf_ && NULL != key_buf_)
      {
        ob_free(key_buf_);
        key_buf_ = NULL;
      }
#endif
    }

    DEFINE_SERIALIZE(ObTrailerOffset)
    {
      int ret                 = OB_SUCCESS;
      int64_t serialize_size  = get_serialize_size();

      if (NULL == buf || serialize_size + pos > buf_len)
      {
        TBSYS_LOG(WARN, "invalid param, buf=%p, buf_len=%ld, pos=%ld,"
                        "serialize_size=%ld", 
                  buf, buf_len, pos, serialize_size);
        ret = OB_ERROR;
      }

      if ((OB_SUCCESS == ret)
          && (OB_SUCCESS == encode_i64(buf, buf_len, pos, trailer_record_offset_)))
      {
        ret = OB_SUCCESS;
      }
      else
      {
        TBSYS_LOG(WARN, "failed to serialzie trailer offset, buf=%p, "
                        "buf_len=%ld, pos=%ld", buf, buf_len, pos);
        ret = OB_ERROR;
      }

      return ret;
    }

    DEFINE_DESERIALIZE(ObTrailerOffset)
    {
      int ret                 = OB_SUCCESS;
      int64_t serialize_size  = get_serialize_size();

      if (NULL == buf || serialize_size + pos > data_len)
      {
        TBSYS_LOG(WARN, "invalid param, buf=%p, data_len=%ld, pos=%ld,"
                        "serialize_size=%ld", 
                  buf, data_len, pos, serialize_size);
        ret = OB_ERROR;
      }

      if ((OB_SUCCESS == ret)
          && (OB_SUCCESS == decode_i64(buf, data_len, pos, &trailer_record_offset_)))
      {
        ret = OB_SUCCESS;
      }
      else
      {
        TBSYS_LOG(WARN, "failed to deserialzie trailer offset, buf=%p, "
                        "buf_len=%ld, pos=%ld", buf, data_len, pos);
        ret = OB_ERROR;
      }

      return ret;
    }

    DEFINE_GET_SERIALIZE_SIZE(ObTrailerOffset)
    {
      return encoded_length_i64(trailer_record_offset_);
    }

#ifdef COMPATIBLE
    DEFINE_SERIALIZE(ObTableTrailerInfo)
    {
      int ret                 = OB_SUCCESS;
      int64_t serialize_size  = get_serialize_size();

      if (NULL == buf || (serialize_size + pos > buf_len))
      {
        TBSYS_LOG(WARN, "invalid param, buf=%p, buf_len=%ld, pos=%ld,"
                        "serialize_size=%ld", 
                  buf, buf_len, pos, serialize_size);
        ret = OB_ERROR;
      }

      if ((OB_SUCCESS == ret)
          && (OB_SUCCESS == (ret = encode_i64(buf, buf_len, pos, table_id_)))
          && (OB_SUCCESS == (ret = encode_i16(buf, buf_len, pos, column_count_)))
          && (OB_SUCCESS == (ret = encode_i16(buf, buf_len, pos, start_row_key_length_)))
          && (OB_SUCCESS == (ret = encode_i16(buf, buf_len, pos, end_row_key_length_)))
          && (OB_SUCCESS == (ret = encode_i16(buf, buf_len, pos, reversed_))))
      {
        //do nothing here
      }
      else
      {
        TBSYS_LOG(WARN, "failed to serialzie trailer table info, buf=%p, "
                        "buf_len=%ld, pos=%ld", buf, buf_len, pos);
      }

      return ret;
    }

    DEFINE_DESERIALIZE(ObTableTrailerInfo)
    {
      int ret                 = OB_SUCCESS;
      int64_t serialize_size  = get_serialize_size();

      if (NULL == buf || serialize_size + pos > data_len)
      {
        TBSYS_LOG(WARN, "invalid param, buf=%p, data_len=%ld, pos=%ld,"
                        "serialize_size=%ld", 
                  buf, data_len, pos, serialize_size);
        ret = OB_ERROR;
      }

      if ((OB_SUCCESS == ret)
          && (OB_SUCCESS == (ret = decode_i64(buf, data_len, pos, 
                                              reinterpret_cast<int64_t*>(&table_id_))))
          && (OB_SUCCESS == (ret = decode_i16(buf, data_len, pos, &column_count_)))
          && (OB_SUCCESS == (ret = decode_i16(buf, data_len, pos, &start_row_key_length_)))
          && (OB_SUCCESS == (ret = decode_i16(buf, data_len, pos, &end_row_key_length_)))
          && (OB_SUCCESS == (ret = decode_i16(buf, data_len, pos, &reversed_))))
      {
        //do nothing here
      }
      else
      {
        TBSYS_LOG(WARN, "failed to deserialzie trailer table info, buf=%p, "
                        "buf_len=%ld, pos=%ld", buf, data_len, pos);
      }

      return ret;
    }

    DEFINE_GET_SERIALIZE_SIZE(ObTableTrailerInfo)
    {
      return(encoded_length_i64(table_id_) 
             + encoded_length_i16(column_count_)
             + encoded_length_i16(start_row_key_length_)
             + encoded_length_i16(end_row_key_length_) 
             + encoded_length_i16(reversed_));
    }

    const int64_t ObSSTableTrailer::get_key_stream_record_size() const
    {
      return key_stream_record_size_;
    }

    int ObSSTableTrailer::set_key_stream_record_size(const int64_t size)
    {
      int ret = OB_SUCCESS;

      if (size > 0 )
      {
        key_stream_record_size_ = size;
      }
      else
      {
        TBSYS_LOG(WARN, "invalid param, size=%ld", size);
        ret = OB_ERROR;
      }

      return ret;
    }

    const int64_t ObSSTableTrailer::get_key_stream_record_offset() const
    {
      return key_stream_record_offset_;
    }
         
    int ObSSTableTrailer::set_key_stream_record_offset(const int64_t offset)
    {
      int ret = OB_SUCCESS;

      if (offset > 0 )
      {
        key_stream_record_offset_ = offset;
      }
      else
      {
        TBSYS_LOG(WARN, "invalid param, offset=%ld", offset);
        ret = OB_ERROR;
      }

      return ret;
    }

    const uint64_t ObSSTableTrailer::get_table_id(const int64_t index) const
    {
      UNUSED(index);
      return table_info_.table_id_;
    }
         
    const int64_t ObSSTableTrailer::get_column_count(const int64_t index) const
    {
      UNUSED(index);
      return table_info_.column_count_;
    }         
 
    const int32_t ObSSTableTrailer::get_table_count() const
    {
      return table_count_;
    }

    int ObSSTableTrailer::set_table_count(const int32_t count)
    {
      int ret = OB_SUCCESS;

      if (1 == count)
      {
        table_count_ = count;
      }
      else
      {
        TBSYS_LOG(WARN, "invalid param, table_count=%d", count);
        ret = OB_ERROR;
      }

      return ret;
    }

    int ObSSTableTrailer::ensure_key_buf_space(const int64_t size)
    {
      int ret             = OB_SUCCESS;
      char *new_buf       = NULL;
      int64_t reamin_size = key_buf_size_ - key_data_size_;
      int64_t key_buf_len = 0;

      if (size <= 0)
      {
        ret = OB_ERROR;
      }
      else if (NULL == key_buf_ || (NULL != key_buf_ && size > reamin_size))
      {
        key_buf_len = size > reamin_size 
                      ? (key_buf_size_ * 2) : key_buf_size_;
        if (key_buf_len - key_data_size_ < size)
        {
          key_buf_len = key_data_size_ + size * 2;
        }
        new_buf = static_cast<char*>(ob_malloc(key_buf_len));
        if (NULL == new_buf)
        {
          TBSYS_LOG(ERROR, "Problem allocating memory for key buffer");
          ret = OB_ERROR;
        }
        else
        {
          memset(new_buf, 0, key_buf_len);
          if (NULL != key_buf_)
          {
            memcpy(new_buf, key_buf_, key_data_size_);
            if (own_key_buf_)
            {
              ob_free(key_buf_);
            }
            key_buf_ = NULL;
          }
          key_buf_size_ = key_buf_len;
          key_buf_ = new_buf;
          own_key_buf_ = true;
        }
      }

      return ret;
    }

    const int64_t ObSSTableTrailer::find_table_id(const uint64_t table_id) const
    {
      int64_t ret = -1;
      if (table_id == table_info_.table_id_)
      {
        ret = 0;
      }
      return ret;
    }
     
    const ObString ObSSTableTrailer::get_start_key(const uint64_t table_id) const
    {
      ObString start_key;
      int64_t table_index = -1;

      if (table_id == OB_INVALID_ID || table_id == 0)
      {
        TBSYS_LOG(WARN, "invalid param, table_id=%lu, table_count=%d, table_info=%p", 
                  table_id, table_count_, &table_info_); 
      }
      else
      {
        table_index = find_table_id(table_id);
        if (table_index == 0 && table_index < table_count_)
        {
          start_key.assign(key_buf_ + table_info_.start_row_key_offset_, 
                           table_info_.start_row_key_length_);
        }
        else
        {
          TBSYS_LOG(WARN, "can't find table id in trailer, table_id=%lu, "
                          "table_count=%d, table_info=%p", 
                    table_id, table_count_, &table_info_); 
        }
      }

      return start_key;  
    }

    const ObString ObSSTableTrailer::get_end_key(const uint64_t table_id) const
    {
      ObString end_key;
      int64_t table_index = -1;

      if (table_id == OB_INVALID_ID || table_id == 0)
      {
        TBSYS_LOG(WARN, "invalid param, table_id=%lu, table_count=%d, table_info=%p", 
                  table_id, table_count_, &table_info_); 
      }
      else
      {
        table_index = find_table_id(table_id);
        if (table_index == 0 && table_index < table_count_)
        {
          end_key.assign(key_buf_ + table_info_.end_row_key_offset_, 
                         table_info_.end_row_key_length_);
        }
        else
        {
          TBSYS_LOG(WARN, "can't find table id in trailer, table_id=%lu, "
                          "table_count=%d, table_info=%p", 
                    table_id, table_count_, &table_info_); 
        }
      }

      return end_key; 
    }

    int ObSSTableTrailer::set_key_buf(ObString& key_buf)
    {
      int ret         = OB_SUCCESS;
      int64_t len     = key_buf.length();
      const char* ptr = key_buf.ptr();

      if (NULL == ptr || len == 0)
      {
        TBSYS_LOG(WARN, "invalid parameter, key bufer length=%ld, ptr=%p",
                  len, ptr);
        ret = OB_ERROR;
      }
      else
      {
        if (own_key_buf_ && NULL != key_buf_)
        {
          ob_free(key_buf_);
          key_buf_ = NULL;
        }
        key_buf_ = const_cast<char*>(ptr);
        key_buf_size_ = len;
        key_data_size_ = len;
        own_key_buf_ = false;
      }

      return ret;
    }

    ObString ObSSTableTrailer::get_key_buf() const 
    {
      return ObString(static_cast<int32_t>(key_data_size_), static_cast<int32_t>(key_data_size_), key_buf_);
    }
#endif

    //serialize ObSSTableTrailer only support V0.2.0
    DEFINE_SERIALIZE(ObSSTableTrailer)
    {
      int ret                 = OB_SUCCESS;
      int64_t serialize_size  = get_serialize_size();
      size_ = static_cast<int32_t>(serialize_size);

      if (NULL == buf || serialize_size + pos > buf_len)
      {
        TBSYS_LOG(WARN, "invalid param, buf=%p, buf_len=%ld, pos=%ld,"
                        "serialize_size=%ld", 
                  buf, buf_len, pos, serialize_size);
        ret = OB_ERROR;
      }
      
      //skip key stream offset && record size in version0.2.0
      if ((OB_SUCCESS == ret)
          && (OB_SUCCESS == (ret = encode_i32(buf, buf_len, pos, size_)))
          && (OB_SUCCESS == (ret = encode_i32(buf, buf_len, pos, trailer_version_)))
          && (OB_SUCCESS == (ret = encode_i64(buf, buf_len, pos, table_version_)))
          && (OB_SUCCESS == (ret = encode_i64(buf, buf_len, pos, first_block_data_offset_)))
          && (OB_SUCCESS == (ret = encode_i64(buf, buf_len, pos, block_count_)))
          && (OB_SUCCESS == (ret = encode_i64(buf, buf_len, pos, block_index_record_offset_)))
          && (OB_SUCCESS == (ret = encode_i64(buf, buf_len, pos, block_index_record_size_)))
          && (OB_SUCCESS == (ret = encode_i64(buf, buf_len, pos, bloom_filter_hash_count_)))
          && (OB_SUCCESS == (ret = encode_i64(buf, buf_len, pos, bloom_filter_record_offset_)))
          && (OB_SUCCESS == (ret = encode_i64(buf, buf_len, pos, bloom_filter_record_size_)))
          && (OB_SUCCESS == (ret = encode_i64(buf, buf_len, pos, schema_record_offset_)))
          && (OB_SUCCESS == (ret = encode_i64(buf, buf_len, pos, schema_record_size_)))
          && (OB_SUCCESS == (ret = encode_i64(buf, buf_len, pos, block_size_)))
          && (OB_SUCCESS == (ret = encode_i64(buf, buf_len, pos, row_count_)))
          && (OB_SUCCESS == (ret = encode_i64(buf, buf_len, pos, sstable_checksum_)))
          && (OB_SUCCESS == (ret = encode_i64(buf, buf_len, pos, first_table_id_)))
          && (OB_SUCCESS == (ret = encode_i64(buf, buf_len, pos, frozen_time_))))
      {
        //do nothing here
      }
      else
      {
        TBSYS_LOG(WARN, "failed to serialzie the fourth part of trailer, buf=%p, "
                  "buf_len=%ld, pos=%ld", buf, buf_len, pos);
      }
      
      if (OB_SUCCESS == ret)
      {
        for (int64_t i = 0; i < RESERVED_LEN; ++i)
        {
          if (OB_SUCCESS != (ret = encode_i64(buf, buf_len, pos, reserved64_[i])))
          {
            TBSYS_LOG(WARN, "failed to serialzie trailer reserved, buf=%p, "
                            "buf_len=%ld, pos=%ld, index=%ld", 
                      buf, buf_len, pos, i);
            break;
          }
        }

        if ((OB_SUCCESS == ret ) && (buf_len >= OB_MAX_COMPRESSOR_NAME_LENGTH + pos))
        {
          memcpy(buf + pos, compressor_name_, OB_MAX_COMPRESSOR_NAME_LENGTH);
          pos += OB_MAX_COMPRESSOR_NAME_LENGTH;
        }
        else
        {
          TBSYS_LOG(WARN, "failed to serialzie trailer comprssor name, buf=%p, "
                          "buf_len=%ld, pos=%ld", buf, buf_len, pos);
          ret = OB_ERROR;
        }

        if ((OB_SUCCESS == ret) 
            && (OB_SUCCESS == (ret = encode_i16(buf, buf_len, pos, row_value_store_style_)))
            && (OB_SUCCESS == (ret = encode_i16(buf, buf_len, pos, reserved_))))
        {
          //do nothing here
        }
        else
        {
          TBSYS_LOG(WARN, "failed to serialzie the fourth part of trailer, buf=%p, "
                          "buf_len=%ld, pos=%ld", buf, buf_len, pos);
        }
      }
      return ret;
    }

    DEFINE_DESERIALIZE(ObSSTableTrailer)
    {
      int ret            = OB_SUCCESS;
#ifdef COMPATIBLE
      int32_t key_offset = 0;
#endif
      if (NULL == buf || data_len <= 0 || pos > data_len)
      {
        TBSYS_LOG(WARN, "invalid param, buf=%p, data_len=%ld, pos=%ld", 
                  buf, data_len, pos);
        ret = OB_ERROR;
      }

      if ((OB_SUCCESS == ret)
          && (OB_SUCCESS == (ret = decode_i32(buf, data_len, pos, &size_))))
      {
        if (size_ > data_len)
        {
          TBSYS_LOG(WARN, "data length is less than expected, buf=%p, "
                          "data_len=%ld, pos=%ld, expected_len=%d", 
                    buf, data_len, pos, size_);
          ret = OB_ERROR;
        }
      }
      else
      {
        TBSYS_LOG(WARN, "failed to deserialzie trailer size, buf=%p, "
                        "buf_len=%ld, pos=%ld", buf, data_len, pos);
      }

      if ((OB_SUCCESS == ret) 
          && (OB_SUCCESS == (ret = decode_i32(buf, data_len, pos, &trailer_version_)))
          && (OB_SUCCESS == (ret = decode_i64(buf, data_len, pos, &table_version_)))
          && (OB_SUCCESS == (ret = decode_i64(buf, data_len, pos, &first_block_data_offset_)))
          && (OB_SUCCESS == (ret = decode_i64(buf, data_len, pos, &block_count_)))
          && (OB_SUCCESS == (ret = decode_i64(buf, data_len, pos, &block_index_record_offset_)))
          && (OB_SUCCESS == (ret = decode_i64(buf, data_len, pos, &block_index_record_size_)))
          && (OB_SUCCESS == (ret = decode_i64(buf, data_len, pos, &bloom_filter_hash_count_)))
          && (OB_SUCCESS == (ret = decode_i64(buf, data_len, pos, &bloom_filter_record_offset_)))
          && (OB_SUCCESS == (ret = decode_i64(buf, data_len, pos, &bloom_filter_record_size_)))
          && (OB_SUCCESS == (ret = decode_i64(buf, data_len, pos, &schema_record_offset_)))
          && (OB_SUCCESS == (ret = decode_i64(buf, data_len, pos, &schema_record_size_))))
      {
#ifdef COMPATIBLE
        if ((0 == trailer_version_)
            && (OB_SUCCESS == (ret = decode_i64(buf, data_len, pos, &key_stream_record_offset_)))
            && (OB_SUCCESS == (ret = decode_i64(buf, data_len, pos, &key_stream_record_size_))))
        {
          //do nothing here
        }
#endif
      }
      
      if ((OB_SUCCESS == ret)
          && (OB_SUCCESS == (ret = decode_i64(buf, data_len, pos, &block_size_)))
          && (OB_SUCCESS == (ret = decode_i64(buf, data_len, pos, &row_count_)))
          && (OB_SUCCESS == (ret = decode_i64(buf, data_len, pos, 
                                              reinterpret_cast<int64_t*> (&sstable_checksum_))))
          && (OB_SUCCESS == (ret = decode_i64(buf, data_len, pos, 
                                              reinterpret_cast<int64_t*> (&first_table_id_))))
          && (OB_SUCCESS == (ret = decode_i64(buf, data_len, pos, &frozen_time_))))
      {
        for (int64_t i = 0; i < RESERVED_LEN; ++i)
        {
          if (OB_SUCCESS != (ret = decode_i64(buf, data_len, pos, &reserved64_[i])))
          {
            TBSYS_LOG(WARN, "failed to deserialzie trailer reserved, buf=%p, "
                            "buf_len=%ld, pos=%ld, index=%ld", 
                      buf, data_len, pos, i);
            break;
          }
        }
        if ((OB_SUCCESS == ret ) && (data_len >= OB_MAX_COMPRESSOR_NAME_LENGTH + pos))
        {
          memcpy(compressor_name_, buf + pos, OB_MAX_COMPRESSOR_NAME_LENGTH);
          pos += OB_MAX_COMPRESSOR_NAME_LENGTH;
        }
        else
        {
          TBSYS_LOG(WARN, "failed to deserialzie trailer compressor name, buf=%p, "
                          "buf_len=%ld, pos=%ld", buf, data_len, pos);
          ret = OB_ERROR;
        }

        if ((OB_SUCCESS == ret) 
            && (OB_SUCCESS == (ret = decode_i16(buf, data_len, pos, &row_value_store_style_)))
            && (OB_SUCCESS == (ret = decode_i16(buf, data_len, pos, &reserved_))))
        {
          //do nothing here
        }
        else
        {
          TBSYS_LOG(WARN, "failed to deserialzie the fourth part of trailer, buf=%p, "
                          "buf_len=%ld, pos=%ld", buf, data_len, pos);
        }

#ifdef COMPATIBLE
        if (0 == trailer_version_)
        {
          ret = decode_i32(buf, data_len, pos, &table_count_);
          if (OB_SUCCESS == ret && 1 == table_count_)
          {
            memset(&table_info_, 0, sizeof(ObTableTrailerInfo));
            ret = table_info_.deserialize(buf, data_len, pos);
            if (OB_SUCCESS == ret)
            {
              table_info_.start_row_key_offset_ = key_offset;
              key_offset += table_info_.start_row_key_length_;
              table_info_.end_row_key_offset_ = key_offset;
              key_offset += table_info_.end_row_key_length_;
            }
            else
            {
              TBSYS_LOG(WARN, "failed to deserialzie trailer table info, buf=%p, "
                        "buf_len=%ld, pos=%ld", 
                        buf, data_len, pos);
            }
          }
        }
#endif
      }
      else
      {
        TBSYS_LOG(WARN, "failed to deserialzie first part of trailer, buf=%p, "
                        "buf_len=%ld, pos=%ld", buf, data_len, pos);
      }   

      return ret;
    }

    //when you serialize and  deserialize, you should first give TAILER_LEN 
    //length memory for destination or source
    DEFINE_GET_SERIALIZE_SIZE(ObSSTableTrailer)
    {
      int64_t total_len = 0;

      total_len += (encoded_length_i32(size_) 
                    + encoded_length_i32(trailer_version_)
                    + encoded_length_i64(table_version_)
                    + encoded_length_i64(first_block_data_offset_) 
                    + encoded_length_i64(block_count_)
                    + encoded_length_i64(block_index_record_offset_) 
                    + encoded_length_i64(block_index_record_size_)
                    + encoded_length_i64(bloom_filter_hash_count_)
                    + encoded_length_i64(bloom_filter_record_offset_) 
                    + encoded_length_i64(bloom_filter_record_size_)
                    + encoded_length_i64(schema_record_offset_) 
                    + encoded_length_i64(schema_record_size_)
                    + encoded_length_i64(block_size_) 
                    + encoded_length_i64(row_count_)
                    + encoded_length_i64(sstable_checksum_)
                    + encoded_length_i64(first_table_id_)
                    + encoded_length_i64(frozen_time_));

      for (int64_t i = 0; i < RESERVED_LEN; ++i)
      {
        total_len += encoded_length_i64(reserved64_[i]);
      }

      total_len += OB_MAX_COMPRESSOR_NAME_LENGTH;
      total_len += (encoded_length_i16(row_value_store_style_) 
                    + encoded_length_i16(reserved_));
      
      return total_len;
    }

    const int32_t ObSSTableTrailer::get_size() const
    {
      return size_;
    }
    
    const int32_t ObSSTableTrailer::get_trailer_version() const
    {
      return trailer_version_;
    }
         
    int ObSSTableTrailer::set_trailer_version(const int32_t version)
    {
      int ret = OB_SUCCESS;

      if (version >= 0 )
      {
        trailer_version_ = version;
      }
      else
      {
         ret = OB_ERROR;
      }

      return ret;
    }

    const int64_t ObSSTableTrailer::get_table_version() const
    {
      return table_version_;
    }
         
    int ObSSTableTrailer::set_table_version(const int64_t version)
    {
      int ret = OB_SUCCESS;

      if (version >= 0 )
      {
        table_version_ = version;
      }
      else
      {
        TBSYS_LOG(WARN, "invalid param, version=%ld", version);
        ret = OB_ERROR;
      }

      return ret;
    }
    
    const int64_t ObSSTableTrailer::get_block_count() const
    {
      return block_count_;
    }
         
    int ObSSTableTrailer::set_block_count(const int64_t count)
    {
      int ret = OB_SUCCESS;

      if (count > 0 )
      {
        block_count_ = count;
      }
      else
      {
        TBSYS_LOG(WARN, "invalid param, block_count=%ld", count);
        ret = OB_ERROR;
      }

      return ret;
    }
    
    const int64_t ObSSTableTrailer::get_block_index_record_offset() const
    {
      return block_index_record_offset_;
    }
         
    int ObSSTableTrailer::set_block_index_record_offset(const int64_t offset)
    {
      int ret = OB_SUCCESS;

      if (offset > 0 )
      {
        block_index_record_offset_ = offset;
      }
      else
      {
        TBSYS_LOG(WARN, "invalid param, offset=%ld", offset);
        ret = OB_ERROR;
      }

      return ret;
    }
    
    const int64_t ObSSTableTrailer::get_block_index_record_size() const
    {
      return block_index_record_size_;
    }
         
    int ObSSTableTrailer::set_block_index_record_size(const int64_t size)
    {
      int ret = OB_SUCCESS;

      if (size > 0 )
      {
        block_index_record_size_ = size;
      }
      else
      {
        TBSYS_LOG(WARN, "invalid param, size=%ld", size);
        ret = OB_ERROR;
      }

      return ret;
    }
    
    const int64_t ObSSTableTrailer::get_bloom_filter_hash_count() const
    {
      return bloom_filter_hash_count_;
    }
         
    int ObSSTableTrailer::set_bloom_filter_hash_count(const int64_t count)
    {
      int ret = OB_SUCCESS;

      if (count > 0 )
      {
        bloom_filter_hash_count_ = count;
      }
      else
      {
        TBSYS_LOG(WARN, "invalid param, hash_count=%ld", count);
        ret = OB_ERROR;
      }

      return ret;
    }
    
    const int64_t ObSSTableTrailer::get_bloom_filter_record_offset() const
    {
      return bloom_filter_record_offset_;
    }
         
    int ObSSTableTrailer::set_bloom_filter_record_offset(const int64_t offset)
    {
      int ret = OB_SUCCESS;

      if (offset > 0 )
      {
        bloom_filter_record_offset_ = offset;
      }
      else
      {
        TBSYS_LOG(WARN, "invalid param, offset=%ld", offset);
        ret = OB_ERROR;
      }

      return ret;
    }
    
    const int64_t ObSSTableTrailer::get_bloom_filter_record_size() const
    {
      return bloom_filter_record_size_;
    }
         
    int ObSSTableTrailer::set_bloom_filter_record_size(const int64_t size)
    {
      int ret = OB_SUCCESS;

      if (size > 0 )
      {
        bloom_filter_record_size_ = size;
      }
      else
      {
        TBSYS_LOG(WARN, "invalid param, size=%ld", size);
        ret = OB_ERROR;
      }

      return ret;
    }
    
    const int64_t ObSSTableTrailer::get_schema_record_offset() const
    {
      return schema_record_offset_;
    }
         
    int ObSSTableTrailer::set_schema_record_offset(const int64_t offset)
    {
      int ret = OB_SUCCESS;

      if (offset > 0 )
      {
        schema_record_offset_ = offset;
      }
      else
      {
        TBSYS_LOG(WARN, "invalid param, offset=%ld", offset);
        ret = OB_ERROR;
      }

      return ret;
    }
    
    const int64_t ObSSTableTrailer::get_schema_record_size() const
    {
      return schema_record_size_;
    }
         
    int ObSSTableTrailer::set_schema_record_size(const int64_t size)
    {
      int ret = OB_SUCCESS;

      if (size > 0 )
      {
        schema_record_size_ = size;
      }
      else
      {
        TBSYS_LOG(WARN, "invalid param, size=%ld", size);
        ret = OB_ERROR;
      }

      return ret;
    }
    
    const int64_t ObSSTableTrailer::get_block_size() const
    {
      return block_size_;
    }
         
    int ObSSTableTrailer::set_block_size(const int64_t size)
    {
      int ret = OB_SUCCESS;

      if (size > 0 )
      {
        block_size_ = size;
      }
      else
      {
        TBSYS_LOG(WARN, "invalid param, block_size=%ld", size);
        ret = OB_ERROR;
      }

      return ret;
    }
    
    const int64_t ObSSTableTrailer::get_row_count() const
    {
      return row_count_;
    }
         
    int ObSSTableTrailer::set_row_count(const int64_t count)
    {
      int ret = OB_SUCCESS;

      if (count > 0)
      {
        row_count_ = count;
      }
      else
      {
        TBSYS_LOG(WARN, "invalid param, row_count=%ld", count);
        ret = OB_ERROR;
      }

      return ret;
    }

    const uint64_t ObSSTableTrailer::get_sstable_checksum() const
    {
      return sstable_checksum_;
    }

    void ObSSTableTrailer::set_sstable_checksum(uint64_t sstable_checksum)
    {
      sstable_checksum_ = sstable_checksum;
    }
    
    const char* ObSSTableTrailer::get_compressor_name() const
    {
      return compressor_name_;
    }
    
    int ObSSTableTrailer::set_compressor_name(const char* name)
    { 
      int ret = OB_SUCCESS;
      int len = static_cast<int>(strlen(name));

      if ((NULL != name) && (len <= OB_MAX_COMPRESSOR_NAME_LENGTH))
      {
         memcpy(compressor_name_, name, strlen(name));
      }
      else 
      {
        TBSYS_LOG(WARN, "compessor name length is too big, name_length=%d,"
                        "name_buf_length=%ld", len, OB_MAX_COMPRESSOR_NAME_LENGTH);
        ret = OB_ERROR; 
      }

      return ret;
    }
    
    const int16_t ObSSTableTrailer::get_row_value_store_style() const
    {
      return row_value_store_style_;
    }
         
    int ObSSTableTrailer::set_row_value_store_style(const int16_t style)
    {
      int ret = OB_SUCCESS;

      if (style >= OB_SSTABLE_STORE_DENSE && style <= OB_SSTABLE_STORE_MIXED)
      {
        row_value_store_style_ = style;
      }
      else
      {
        TBSYS_LOG(WARN, "unknow store style, style=%d", style);
        ret = OB_ERROR;
      }

      return ret;
    }
    
    const int64_t ObSSTableTrailer::get_first_block_data_offset() const
    {
      return first_block_data_offset_;
    }
         
    int ObSSTableTrailer::set_first_block_data_offset(const int64_t offset)
    {
      int ret = OB_SUCCESS;

      if (offset >= 0 )
      {
        first_block_data_offset_ = offset;
      }
      else
      {
        TBSYS_LOG(WARN, "invalid param, offset=%ld", offset);
        ret = OB_ERROR;
      }

      return ret;
    }

    const uint64_t ObSSTableTrailer::get_first_table_id() const
    {
      uint64_t table_id = OB_INVALID_ID;

      if (SSTABLEV1 == trailer_version_)
      {
#ifdef COMPATIBLE
        table_id = get_table_id(0);
#endif
      }
      else 
      {
        table_id = first_table_id_;
      }

      return table_id;
    }

    int ObSSTableTrailer::set_first_table_id(const uint64_t table_id)
    {
      int ret = OB_SUCCESS;

      if (OB_INVALID_ID == table_id || 0 == table_id)
      {
        TBSYS_LOG(WARN, "invalid param, table_id=%ld", table_id);
        ret = OB_ERROR;
      }
      else
      {
        first_table_id_ = table_id;
      }

      return ret;
    }

    const int64_t ObSSTableTrailer::get_frozen_time() const
    {
      return frozen_time_;
    }

    int ObSSTableTrailer::set_frozen_time(const int64_t frozen_time)
    {
      int ret = OB_SUCCESS;

      if (frozen_time < 0)
      {
        TBSYS_LOG(WARN, "invalid param, frozen_time=%ld", frozen_time);
        ret = OB_ERROR;
      }
      else
      {
        frozen_time_ = frozen_time;
      }

      return ret;
    }

    void ObSSTableTrailer::reset()
    {
#ifdef COMPATIBLE
      if (own_key_buf_ && NULL != key_buf_)
      {
        ob_free(key_buf_);
        key_buf_ = NULL;
      }
      own_key_buf_ = false;
      key_buf_size_ = DEFAULT_KEY_BUF_SIZE;
#endif
      memset(this, 0, sizeof(ObSSTableTrailer));
    }

    bool ObSSTableTrailer::is_valid()
    {
      bool ret  = false;
      int64_t i = 0;

      if (size_ <= 0)
      {
        TBSYS_LOG(WARN, "size error, size_=%d", size_);
      }
      else if (trailer_version_ < 0)
      {
        TBSYS_LOG(WARN, "trailer version error, trailer_version_=%d", trailer_version_);
      }
      else if (table_version_ < 0)
      {
        TBSYS_LOG(WARN, "table version error, table_version_=%ld", table_version_);
      }
      else if (first_block_data_offset_ != 0)
      {
        TBSYS_LOG(WARN, "first block data offset error, first_block_data_offset_=%ld", 
                  first_block_data_offset_);
      }
      else if (block_count_ <= 0 )
      {
        TBSYS_LOG(WARN, "block_count_ should be greater than 0, block_count_=%ld",block_count_);
      }
      else if (block_index_record_offset_ <= 0)
      {
        TBSYS_LOG(WARN, "block_index_record_offset_ should be greater than 0 [real:%ld]", 
                  block_index_record_offset_);
      }
      else if (block_index_record_size_ <= 0)
      {
        TBSYS_LOG(WARN, "block_index_record_size_ should be greater than 0 [real:%ld]", 
                  block_index_record_size_);
      }
      else if (bloom_filter_hash_count_ < 0)
      {
        TBSYS_LOG(WARN, "bloom_filter_hash_count_ should be greater than 0 [real:%ld]", 
                  bloom_filter_hash_count_);
      }
      else if (bloom_filter_record_offset_ < 0)
      {
        TBSYS_LOG(WARN, "bloom_filter_record_offset_ should be greater than 0 [real:%ld]", 
                  bloom_filter_record_offset_);
      }
      else if (bloom_filter_record_size_ < 0)
      {
        TBSYS_LOG(WARN, "bloom_filter_record_size_ should be greater than 0 [real:%ld]", 
                  bloom_filter_record_size_);
      }
      else if (schema_record_offset_ < 0)
      {
        TBSYS_LOG(WARN, "schema_record_offset_ should be greater than 0 [real:%ld]", 
                  schema_record_offset_);
      }
      else if (schema_record_size_ < 0)
      {
        TBSYS_LOG(WARN, "schema_record_size_ should be greater than 0 [real:%ld]", 
                  schema_record_size_);
      }
      else if (block_size_ <= 0)
      {
        TBSYS_LOG(WARN, "blocksize_ should be greater than 0 [real:%ld]", 
                  block_size_);
      }
      else if (row_count_ <= 0)
      {
        TBSYS_LOG(WARN, "row_count_ should be greater than 0 [real:%ld]", 
                  row_count_);
      }
      else if (OB_INVALID_ID == first_table_id_)
      {
        TBSYS_LOG(WARN, "first_table_id_ is invalid [real:%ld]", 
                  first_table_id_);
      }
      else if (frozen_time_ < 0)
      {
        TBSYS_LOG(WARN, "frozen_time_ should be greater than or equal to 0 [real:%ld]", 
                  frozen_time_);
      }
      else if (strlen(compressor_name_) == 0)
      {
        TBSYS_LOG(WARN, "compressor_name_ is NULL, length=%lu", strlen(compressor_name_));
      }
      else if (OB_SSTABLE_STORE_DENSE != row_value_store_style_ 
               && OB_SSTABLE_STORE_SPARSE != row_value_store_style_)
      {
        TBSYS_LOG(WARN, "row_value_store_style_ error [real:%hd,exp:%hd]", 
                  row_value_store_style_, OB_SSTABLE_STORE_DENSE);
      }
      else if (reserved_ != 0)
      {
        TBSYS_LOG(WARN, "reserved_ should be 0 [real:%d]", reserved_);
      }
      else
      {
        for (i = 0; i < RESERVED_LEN; ++i)
        {
          if (reserved64_[i] != 0)
          {
            TBSYS_LOG(WARN, "reserved64_ should be 0 [real:%ld,index:%ld]", reserved64_[i], i);
            break;
          }
        }
        if (RESERVED_LEN == i)
        {
          ret = true;
        }
      }

      return ret;
    }

    void ObSSTableTrailer::dump()
    {
      TBSYS_LOG(WARN, "size_: %d \n"
                      "trailer_version_: %d \n"
                      "table_version_: %ld \n"
                      "first_block_data_offset_: %ld \n"
                      "block_count_: %ld \n"
                      "block_index_record_offset_: %ld \n"
                      "block_index_record_size_: %ld \n"
                      "bloom_filter_hash_count_: %ld \n"
                      "bloom_filter_record_offset_: %ld \n"
                      "bloom_filter_record_size_: %ld \n"
                      "schema_record_offset_: %ld \n"
                      "schema_record_size_: %ld \n"
                      "block_size_: %ld \n"
                      "row_count_: %ld \n"
                      "sstable_checksum_: %lu \n"
                      "first_table_id_: %lu \n"
                      "frozen_time_: %ld \n"
                      "compressor_name_: %s \n"
                      "row_value_store_style_: %s \n",
              size_, 
              trailer_version_, 
              table_version_,
              first_block_data_offset_, 
              block_count_,
              block_index_record_offset_, 
              block_index_record_size_,
              bloom_filter_hash_count_, 
              bloom_filter_record_offset_,
              bloom_filter_record_size_,
              schema_record_offset_, 
              schema_record_size_,
              block_size_, 
              row_count_, 
              sstable_checksum_, 
              first_table_id_,
              frozen_time_,
              compressor_name_,
              row_value_store_style_ == OB_SSTABLE_STORE_DENSE ? "dense" : "sparse");
    }
  } // end namespace sstable
} // end namespace oceanbase
