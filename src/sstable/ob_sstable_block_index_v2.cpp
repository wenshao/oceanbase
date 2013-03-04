/**
 * (C) 2010-2011 Taobao Inc.
 *  
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_sstable_block_index_v2.cpp for block block index. 
 *  
 * Authors: 
 *   duanfei <duanfei@taobao.com>
 *
 */
#include "ob_sstable_block_index_v2.h"
#include "common/ob_range.h"
#include "common/ob_record_header.h"
#include "ob_sstable_writer.h"
using namespace oceanbase::common;

namespace oceanbase
{
  namespace sstable
  {
    ObSSTableBlockIndexV2::ObSSTableBlockIndexV2(const int64_t serialize_size,
                                                 const bool deserialized)
    : deserialized_(deserialized), serialize_size_(serialize_size), 
      base_length_(0), block_index_count_(0)
    {
      base_ = reinterpret_cast<char*>(this) + sizeof(ObSSTableBlockIndexV2);
    }

    ObSSTableBlockIndexV2::~ObSSTableBlockIndexV2()
    {

    }

    DEFINE_DESERIALIZE(ObSSTableBlockIndexV2::ObSSTableBlockIndexHeader)
    {
      int32_t iret = ((NULL == buf) || (data_len <= 0) ? OB_ERROR : OB_SUCCESS);
      if (iret == OB_SUCCESS)
      {
        iret = serialization::decode_i64(buf, data_len, pos, &block_count_); 
      } 
      if (iret == OB_SUCCESS)
      {
        iret = serialization::decode_i32(buf, data_len, pos, &end_key_offset_); 
      }
      if (iret == OB_SUCCESS)
      {
        iret = serialization::decode_i32(buf, data_len, pos, &reserved32_); 
      }
      if (iret == OB_SUCCESS)
      {
        iret = serialization::decode_i64(buf, data_len, pos, &reserved64_[0]); 
      } 
      if (iret == OB_SUCCESS)
      {
        iret = serialization::decode_i64(buf, data_len, pos, &reserved64_[1]); 
      } 
      return iret;
    }

    DEFINE_DESERIALIZE(ObSSTableBlockIndexV2::ObSSTableBlockIndexElement)
    {
      int32_t iret = ((NULL == buf) || (data_len <= 0) ? OB_ERROR : OB_SUCCESS);
      if (iret == OB_SUCCESS)
      {
        iret = serialization::decode_i16(buf, data_len, pos, &reserved1_); 
      }
      if (iret == OB_SUCCESS)
      {
        iret = serialization::decode_i16(buf, data_len, pos, reinterpret_cast<int16_t*>(&column_group_id_)); 
      }
      if (iret == OB_SUCCESS)
      {
        iret = serialization::decode_i32(buf, data_len, pos, reinterpret_cast<int32_t*>(&table_id_)); 
      }
      if (iret == OB_SUCCESS)
      {
        iret = serialization::decode_i32(buf, data_len, pos, &block_record_size_); 
      }
      if (iret == OB_SUCCESS)
      {
        iret = serialization::decode_i16(buf, data_len, pos, &block_end_key_size_); 
      }
      if (iret == OB_SUCCESS)
      {
        iret = serialization::decode_i16(buf, data_len, pos, &reserved2_); 
      }
      return iret;
    }

    const char* ObSSTableBlockIndexV2::get_base() const
    {
      return base_;
    }

    char* ObSSTableBlockIndexV2::get_base()
    {
      return base_;
    }

    int ObSSTableBlockIndexV2::get_bound(Bound& bound) const
    {
      int iret = OB_SUCCESS;
      bound.base_ = get_base();
      bound.begin_ = reinterpret_cast<const_iterator>(bound.base_);
      bound.end_ = bound.begin_ + block_index_count_;
      if ( OB_UNLIKELY( 
            (NULL == bound.begin_)
            || (NULL == bound.end_)
            || (bound.begin_ > bound.end_)
            || (base_length_ <= 0)
            || (block_index_count_ <= 0)) )
      {
        iret = OB_INVALID_BLOCK_INDEX;
      }
      return iret;
    }

    ObSSTableBlockIndexV2::const_iterator ObSSTableBlockIndexV2::begin() const 
    {
      const char* base = get_base();
      const_iterator index_begin = reinterpret_cast<const_iterator>(base);
      return index_begin;
    }

    ObSSTableBlockIndexV2* ObSSTableBlockIndexV2::deserialize_copy(char* buffer) const
    {
      int status                  = OB_SUCCESS;
      int64_t pos                 = 0;
      ObSSTableBlockIndexV2* ret  = reinterpret_cast<ObSSTableBlockIndexV2*>(buffer);

      if (NULL != ret)
      {
        if (deserialized_)
        {
          ret->deserialized_ = deserialized_;
          ret->serialize_size_ = serialize_size_;
          ret->base_ = buffer + sizeof(ObSSTableBlockIndexV2);
          ret->base_length_ = base_length_;
          ret->block_index_count_ = block_index_count_;
          memcpy(ret->get_base(), get_base(), base_length_);
        }
        else
        {
          if (base_length_ > 0)
          {
            ret->base_length_ = base_length_;
          }
          else
          {
            ret->base_length_ = get_deserialize_size(get_base(), serialize_size_, pos);
          }
          if (ret->base_length_ > 0)
          {
            pos = 0;
            ret->base_ = buffer + sizeof(ObSSTableBlockIndexV2);
            status = ret->deserialize(get_base(), serialize_size_, 
                                      pos, ret->get_base(), ret->base_length_);
            if (OB_SUCCESS != status)
            {
              TBSYS_LOG(WARN, "sstable block index deserialize failed, ret=%d",
                        status);
              ret = NULL;
            }
            else 
            {
              ret->deserialized_ = true;
              ret->serialize_size_ = serialize_size_;
            }
          }
          else
          {
            TBSYS_LOG(WARN, "sstable block index get_deserialize_size failed, ret=%ld",
                      ret->base_length_);
            ret = NULL;
          }
        }
      }

      return ret;      
    }

    const int64_t ObSSTableBlockIndexV2::get_deserialize_size()
    {
      int64_t ret_size  = 0;
      int64_t pos       = 0;

      if (deserialized_ || base_length_ > 0)
      {
        ret_size = sizeof(*this) + base_length_;
      }
      else
      {
        base_length_ = get_deserialize_size(get_base(), serialize_size_, pos);
        if (base_length_ <= 0)
        {
          TBSYS_LOG(WARN, "sstable block index get_deserialize_size failed, ret=%ld",
                    base_length_);
          base_length_ = 0;
          ret_size = 0;
        }
        else
        {
          ret_size = sizeof(*this) + base_length_;
        }
      }

      return ret_size;
    }

    ObSSTableBlockIndexV2::const_iterator ObSSTableBlockIndexV2::end() const 
    {
      const char* base = get_base();
      const_iterator index_begin = reinterpret_cast<const_iterator>(base);
      const_iterator index_end = index_begin + block_index_count_;
      return index_end;
    }

    int ObSSTableBlockIndexV2::store_block_position_info(
        const_iterator find,
        const Bound& bound,
        const SearchMode mode,
        const common::ObString& end_key,
        const uint64_t table_id,
        const uint64_t column_group_id,
        ObBlockPositionInfos &pos_info) const
    {
      int iret = OB_SUCCESS;
      if (find < bound.begin_ || find >= bound.end_)
      {
        iret = OB_ERROR;
      }
      else
      {
        int64_t need_block_count = pos_info.block_count_;
        pos_info.block_count_ = 0;
        int64_t count = 0;
        if (is_looking_forward_mode(mode))
        {
          // store from find to end, asc order
          for (; find < bound.end_ && count < need_block_count; ++find)
          {
            if (!match_table_group(*find, table_id, column_group_id)) break;

            // v2 sstable contains empty block at the first, ignore.
            // if (find->block_record_size_ == 0) continue;

            pos_info.position_info_[count].offset_ = find->block_offset_;
            pos_info.position_info_[count].size_ = find->block_record_size_;
            ++count;

            // locate end postion in blocks.
            if (NULL != end_key.ptr() && end_key.compare(get_end_key(find)) <= 0)
            {
              break;
            }
          }
        }
        else
        {
          // reverse block, store as asc order.
          int64_t max_count = find - bound.begin_ + 1;
          if (max_count > need_block_count) { max_count = need_block_count; }
          const_iterator start = find - max_count + 1;


          for (; start <= find && count < max_count; ++start)
          {
            if (!match_table_group(*start, table_id, column_group_id)) 
            {
              continue;
            }

            // locate end postion in blocks.
            if (NULL != end_key.ptr() && end_key.compare(get_end_key(start)) > 0 )
            {
              continue;
            }

            pos_info.position_info_[count].offset_ = start->block_offset_;
            pos_info.position_info_[count].size_ = start->block_record_size_;
            ++count;
          }

          /*
          // store from find to bound.begin_, desc order
          for (; find >= bound.begin_ && count < need_block_count; --find)
          {
            if (!match_table_group(*find, table_id, column_group_id)) break;

            // locate end postion in blocks.
            if (NULL != end_key.ptr() && end_key.compare(get_end_key(find)) > 0 )
            {
              break;
            }

            pos_info.position_info_[count].offset_ = find->block_offset_;
            pos_info.position_info_[count].size_ = find->block_record_size_;
            ++count;
          }
          */
        }
        pos_info.block_count_ = count;

        if (pos_info.block_count_ == 0)
        {
          iret = OB_BEYOND_THE_RANGE;
        }
      }
      return iret;
    }

    int ObSSTableBlockIndexV2::check_border(
        const_iterator &find,
        const Bound& bound,
        const SearchMode mode,
        const uint64_t table_id,
        const uint64_t column_group_id) const
    {
      int iret = OB_SUCCESS;
      if (find >= bound.end_) // rigth side
      {
        if (is_looking_forward_mode(mode))
        {
          // looking key greater than all of key in this sstable.
          iret = OB_BEYOND_THE_RANGE;
        }
        else
        {
          // looking backward, and looking key(endkey) greater than 
          // all of key in this sstable, start from last key
          find = bound.end_; 
          --find;
          // if has no block? begin == end; 
          // or last block is another table or column group.
          if (find < bound.begin_
              || !match_table_group(*find, table_id, column_group_id))
          {
            iret = OB_BEYOND_THE_RANGE;
          }
        }
      }
      /*
      else if (find <= begin) // left side
      {
        // ignore,
        // target(find) <= all of entry [begin, end) 
        if (match_table_group(*find, table_id, column_group_id))
        {
          iret = OB_BEYOND_THE_RANGE;
        }
      } 
      */
      // in middle or left side
      else if (!match_table_group(*find, table_id, column_group_id)) 
      {
        if (is_looking_forward_mode(mode))
        {
          iret = OB_BEYOND_THE_RANGE;
        }
        else
        {
          // looking backward until find match table & group;
          // previous block must less than looking key;
          // so we check the previous block if can match. 
          --find;
          if (find < bound.begin_
              || !match_table_group(*find, table_id, column_group_id))
          {
            iret = OB_BEYOND_THE_RANGE;
          }
        }
      }
      return iret;
    }

    int ObSSTableBlockIndexV2::find_by_key(
        const uint64_t table_id, 
        const uint64_t column_group_id, 
        const oceanbase::common::ObString& key, 
        const SearchMode mode, 
        const Bound& bound,
        const_iterator& find) const
    {
      int iret = OB_SUCCESS;

      if (mode == OB_SEARCH_MODE_MIN_VALUE)
      {
        iret = find_start_in_group(table_id, column_group_id, find);
      }
      else if (mode == OB_SEARCH_MODE_MAX_VALUE)
      {
        iret = find_end_in_group(table_id, column_group_id, find);
      }
      else
      {
        IndexLookupKey lookup_key(table_id, column_group_id, key);
        find = std::lower_bound(bound.begin_, 
            bound.end_, lookup_key, Compare(*this));
        iret = check_border(find, bound, mode, table_id, column_group_id);

        if (OB_SUCCESS == iret)
        {
          ObString compare_key(0, find->end_key_size_, 
              const_cast<char*>(bound.base_) + find->end_key_offset_);
          switch (mode)
          {
            // compare_key >= key
            case OB_SEARCH_MODE_EQUAL:
              // only need current find block
            case OB_SEARCH_MODE_GREATER_EQUAL:
              // >= include current block and next blocks...
              break;
            case OB_SEARCH_MODE_GREATER_THAN:
              // > not include current block if compare_key == key
              // compare_key is end key of current block.
              if (key == compare_key)
              {
                ++find;
              }
              // unfortunately, we reached the last block of this sstable.
              if (find >= bound.end_
                  || (!match_table_group(*find, table_id, column_group_id)))
              {
                iret = OB_BEYOND_THE_RANGE;
              }
              break;
            case OB_SEARCH_MODE_LESS_THAN:
            case OB_SEARCH_MODE_LESS_EQUAL:
              // <, <= include current block and previous blocks...
              // because compare_key >= key, and all rowkey of previous blocks
              // definitely less than %key
              break;
            default:
              iret = OB_SEARCH_MODE_NOT_IMPLEMENT;
              break;
          }
        }
      }

      return iret;
    }


    int ObSSTableBlockIndexV2::trans_range_to_search_key(
        const common::ObRange& range,
        const bool is_reverse_scan, 
        common::ObString& search_key, 
        SearchMode& mode) const
    {
      ObBorderFlag border_flag = range.border_flag_;
      if (!is_reverse_scan)
      {
        if (border_flag.is_min_value())
        {
          mode = OB_SEARCH_MODE_MIN_VALUE;
        }
        else if (border_flag.inclusive_start())
        {
          mode = OB_SEARCH_MODE_GREATER_EQUAL;
        }
        else
        {
          mode = OB_SEARCH_MODE_GREATER_THAN;
        }
        search_key = range.start_key_;
      }
      else
      {
        if (border_flag.is_max_value())
        {
          mode = OB_SEARCH_MODE_MAX_VALUE;
        }
        else if (border_flag.inclusive_end())
        {
          mode = OB_SEARCH_MODE_LESS_EQUAL;
        }
        else
        {
          mode = OB_SEARCH_MODE_LESS_THAN;
        }
        search_key = range.end_key_;
      }
      return OB_SUCCESS;
    }

    int ObSSTableBlockIndexV2::search_batch_blocks_by_key(
        const uint64_t table_id, 
        const uint64_t column_group_id,
        const oceanbase::common::ObString& key, 
        const SearchMode mode, 
        ObBlockPositionInfos& pos_info) const
    {
      int iret = OB_SUCCESS;
      Bound bound;
      const_iterator find_it = NULL;
      common::ObString end_key(0, 0, NULL);

      if ( table_id <= 0 || (is_regular_mode(mode) && NULL == key.ptr()) )
      {
        TBSYS_LOG(ERROR, "invalid arguments, table_id=%ld, mode=%d, key=%p",
            table_id, mode, key.ptr());
        iret = OB_INVALID_ARGUMENT;
      }
      else if (OB_SUCCESS != (iret = get_bound(bound)))
      {
        TBSYS_LOG(ERROR, "get position error, iret=%d", iret);
      }
      else if (OB_SUCCESS == ( iret = 
            find_by_key(table_id, column_group_id, key, mode, bound, find_it)) )
      {
        iret = store_block_position_info(find_it, bound, 
            mode, end_key, table_id, column_group_id, pos_info);
      }

      return iret;
    }

    int ObSSTableBlockIndexV2::search_batch_blocks_by_range(
        const uint64_t table_id, 
        const uint64_t column_group_id, 
        const oceanbase::common::ObRange& range, 
        const bool is_reverse_scan, 
        ObBlockPositionInfos& pos_info) const
    {
      int iret = OB_SUCCESS;
      Bound bound;
      const_iterator find_it = NULL;

      common::ObString search_key(0, 0, NULL);
      common::ObString end_key(0, 0, NULL);
      SearchMode mode = OB_SEARCH_MODE_MIN_VALUE;

      if ( table_id <= 0 )
      {
        TBSYS_LOG(ERROR, "invalid arguments, table_id=%ld", table_id);
        iret = OB_INVALID_ARGUMENT;
      }
      else if (OB_SUCCESS != (iret = get_bound(bound)))
      {
        TBSYS_LOG(ERROR, "get position error, iret=%d", iret);
      }
      else if (OB_SUCCESS != (iret = 
            trans_range_to_search_key(range, is_reverse_scan, search_key, mode)))
      {
        TBSYS_LOG(ERROR, "trans range error,iret=%d", iret);
      }
      else if (OB_SUCCESS == ( iret = 
            find_by_key(table_id, column_group_id, search_key, mode, bound, find_it)) )
      {
        if (is_reverse_scan && (!range.border_flag_.is_min_value())) 
        {
          end_key = range.start_key_;
        }
        else if (!is_reverse_scan && (!range.border_flag_.is_max_value()))
        {
          end_key = range.end_key_;
        }
        iret = store_block_position_info(find_it, bound, 
            mode, end_key, table_id, column_group_id, pos_info);
      }

      return iret;
    }

    int ObSSTableBlockIndexV2::search_one_block_by_key(
        const uint64_t table_id, 
        const uint64_t column_group_id,
        const oceanbase::common::ObString& key, 
        const SearchMode mode, 
        ObBlockPositionInfo& pos_info) const
    {
      int iret = OB_SUCCESS;
      Bound bound;
      const_iterator find_it = NULL;

      if ( table_id <= 0 || (is_regular_mode(mode) && NULL == key.ptr()) )
      {
        iret = OB_INVALID_ARGUMENT;
      }
      else if (OB_SUCCESS != (iret = get_bound(bound)))
      {
        TBSYS_LOG(ERROR, "get position error, iret=%d", iret);
      }
      else if (OB_SUCCESS == ( iret = 
            find_by_key(table_id, column_group_id, key, mode, bound, find_it)) )
      {
        if (NULL != find_it 
            && find_it < bound.end_
            && find_it >= bound.begin_)
        {
          pos_info.offset_ = find_it->block_offset_;
          pos_info.size_ = find_it->block_record_size_;
        }
      }

      return iret;
    }

    int ObSSTableBlockIndexV2::search_batch_blocks_by_offset(
        const uint64_t table_id, 
        const uint64_t  column_group_id,
        const int64_t offset, 
        const SearchMode mode , 
        ObBlockPositionInfos& pos_info) const
    {
      int iret = OB_SUCCESS;
      Bound bound;
      common::ObString end_key(0, 0, NULL);

      if (table_id <= 0 || offset < 0) 
      {
        iret = OB_INVALID_ARGUMENT;
      }
      else if (OB_SUCCESS != (iret = get_bound(bound)))
      {
        TBSYS_LOG(ERROR, "get position error, iret=%d", iret);
      }
      else
      {
        IndexEntryType entry;
        entry.table_id_ = table_id;
        entry.column_group_id_ = column_group_id;
        entry.block_offset_ = offset;
        entry.block_record_size_ = 0;
        const_iterator find_it = std::lower_bound(bound.begin_, bound.end_, entry);
        iret = check_border(find_it, bound, mode, table_id, column_group_id);
        if (OB_SUCCESS == iret)
        {
          switch (mode)
          {
            case OB_SEARCH_MODE_GREATER_THAN:
              if (*find_it == entry)
              {
                ++find_it;
              } 
              if (find_it >= bound.end_
                  || (!match_table_group(*find_it, table_id, column_group_id)))
              {
                iret = OB_BEYOND_THE_RANGE;
              }
              break;
            case OB_SEARCH_MODE_EQUAL:
              if (*find_it == entry)
              {
                pos_info.block_count_ = 1;
              }
              else
              {
                iret = OB_BEYOND_THE_RANGE;
              }
              break;
            case OB_SEARCH_MODE_GREATER_EQUAL:
              break;
            case OB_SEARCH_MODE_LESS_THAN:
              --find_it;
            case OB_SEARCH_MODE_LESS_EQUAL:
              if (*find_it != entry)
              {
                --find_it;
              }
              if (find_it < bound.begin_ || 
                  !match_table_group(*find_it, table_id, column_group_id))
              {
                iret = OB_BEYOND_THE_RANGE;
              }
              break;
            default:
              iret = OB_SEARCH_MODE_NOT_IMPLEMENT;
              break;
          }
        }

        if (OB_SUCCESS == iret)
        {
          iret = store_block_position_info(find_it, bound, 
              mode, end_key, table_id, column_group_id, pos_info);
        }
      }
      return iret;
    }

    int64_t ObSSTableBlockIndexV2::get_deserialize_size(
        const char* buf, const int64_t data_len, int64_t& pos) const
    {
      int64_t length = 0;
      int32_t iret = ((NULL == buf) || (data_len <= 0) ? OB_ERROR : OB_SUCCESS);
      ObRecordHeader record_header;
      ObSSTableBlockIndexHeader block_index_header;

      if (iret == OB_SUCCESS)
      {
        const char* payload_ptr = NULL;
        int64_t payload_size = 0;
        iret = ObRecordHeader::check_record(buf + pos, data_len - pos, 
            ObSSTableWriter::BLOCK_INDEX_MAGIC, record_header, payload_ptr, payload_size);
        if (OB_SUCCESS == iret)
        {
          if (record_header.is_compress())
          {
            //TODO
            TBSYS_LOG(ERROR, "compressed block index stream not support now.");
            iret = OB_ERROR;
          }
          if (OB_SUCCESS == iret)
          {
            int64_t payload_pos = 0;
            iret = block_index_header.deserialize(payload_ptr, payload_size, payload_pos);
            if (OB_SUCCESS == iret)
            {
              int64_t index_entry_length = sizeof(IndexEntryType) * (block_index_header.block_count_);
              int64_t key_length = record_header.data_length_ - block_index_header.end_key_offset_;
              length = index_entry_length + key_length; 
            }
          }
        }
      } 

      return length;
    }

    int ObSSTableBlockIndexV2::deserialize(const char* buf, const int64_t data_len, int64_t& pos,
        const char* base, int64_t base_length)
    {
      int iret = OB_SUCCESS;
      if ((NULL == buf) || (data_len <= 0) || (NULL == base) || (base_length <= 0))
      {
        iret = OB_INVALID_ARGUMENT;
      }

      // ObSSTableBlockIndexV2 memory represent three parts.
      // -----------------------------------------------------
      // object members (base_length_, block_count_) 16 bytes 
      // -----------------------------------------------------
      // IndexEntryType Object 1 (32 bytes) (base pointer position)
      // IndexEntryType Object 2
      // IndexEntryType Object end..
      // -----------------------------------------------------
      // end key stream (base + index_entry_length)
      // -----------------------------------------------------
      if (reinterpret_cast<char*>(this) + sizeof(ObSSTableBlockIndexV2) != base)
      {
        iret = OB_INVALID_ARGUMENT;
      }

      ObRecordHeader record_header;
      ObSSTableBlockIndexHeader block_index_header;

      if (iret == OB_SUCCESS)
      {
        const char* payload_ptr = NULL;
        int64_t payload_size = 0;
        iret = record_header.deserialize(buf, data_len, pos);
        /* donot check record_header again, checked in get_deserialize_size()
        iret = ObRecordHeader::check_record(buf + pos, data_len - pos, 
            ObSSTableWriter::BLOCK_INDEX_MAGIC, record_header, payload_ptr, payload_size);
            */
        if (OB_SUCCESS != iret)
        {
          TBSYS_LOG(ERROR, "deserialize record header error, "
              "data_len=%ld, pos=%ld", data_len, pos);
        }
        else
        {
          if (record_header.is_compress())
          {
            //TODO
            TBSYS_LOG(ERROR, "compressed block index stream not support now.");
            iret = OB_ERROR;
          }

          if (iret == OB_SUCCESS)
          { 
            payload_ptr = buf + pos;
            payload_size = record_header.data_length_;
            int64_t payload_pos = 0;
            iret = block_index_header.deserialize(payload_ptr, payload_size, payload_pos);
            int64_t index_entry_length = 0;
            int64_t key_length = 0;

            if (OB_SUCCESS != iret)
            {
              TBSYS_LOG(ERROR, "deserialize block_index_header error, "
                  "data_len=%ld, pos=%ld", payload_size, payload_pos);
            }
            else
            {
              //base_ = base;
              base_length_ = base_length;
              block_index_count_ = block_index_header.block_count_; 

              index_entry_length = sizeof(IndexEntryType) * (block_index_count_);
              key_length = payload_size - block_index_header.end_key_offset_;

              if (index_entry_length + key_length > base_length_)
              {
                TBSYS_LOG(ERROR, "block index cache buffer size =%ld not enough (%ld),"
                    "index count = %ld, index entry length=%ld, key length=%ld,"
                    "payload_ptr=%p, pos=%ld, payload_pos=%ld, payload_size=%ld",
                    base_length_, index_entry_length + key_length,
                    block_index_count_, index_entry_length, key_length,
                    payload_ptr, pos ,payload_pos, payload_size);
                iret = OB_SIZE_OVERFLOW;
              }
            }

            if (OB_SUCCESS == iret)
            {

              ObSSTableBlockIndexElement element;
              const IndexEntryType* index_begin = 
                reinterpret_cast<const IndexEntryType*>(base);
              const IndexEntryType* index_end   = 
                reinterpret_cast<const IndexEntryType*>(base +  index_entry_length);

              // copy end key stream to base_ + index_entry_length;
              char* ptr = const_cast<char*>(base);
              memcpy(ptr + index_entry_length, payload_ptr + block_index_header.end_key_offset_, key_length);

              IndexEntryType* entry = const_cast<IndexEntryType*>(index_begin);

              int64_t current_block_offset = 0;
              int32_t current_key_offset  = 0;
              int64_t i = 0;
              for (i = 0; i < block_index_count_ && entry < index_end; ++i)
              {
                memset(&element, 0, sizeof(element));
                iret = element.deserialize(payload_ptr, payload_size, payload_pos); 
                if (iret == OB_SUCCESS)
                {
                  entry->block_offset_ = current_block_offset;
                  entry->block_record_size_ = element.block_record_size_; 
                  entry->table_id_ = element.table_id_;
                  entry->column_group_id_ = element.column_group_id_;

                  current_block_offset += element.block_record_size_; 

                  if (current_key_offset + element.block_end_key_size_ <= key_length)
                  {
                    entry->end_key_offset_ = static_cast<int32_t>(index_entry_length + current_key_offset);
                    entry->end_key_size_ = element.block_end_key_size_;
                    current_key_offset += element.block_end_key_size_;
                  }
                  else
                  {
                    TBSYS_LOG(ERROR, "key out of range, current_key_offset=%d,key size=%d, key_length=%ld",
                        current_key_offset, element.block_end_key_size_, key_length);
                    iret = OB_ERROR;
                    break;
                  }
                  ++entry;
                }
                else
                {
                  TBSYS_LOG(ERROR, "deserialize element of block index array error.");
                  iret = OB_ERROR;
                  break;
                }
              }

              if (i != block_index_count_ || entry != index_end)
              {
                TBSYS_LOG(ERROR, "traverse block index not to end, "
                    "i =%ld, block_index_count_=%ld, entry=%p, index_end=%p",
                    i, block_index_count_, entry, index_end);
                iret = OB_ERROR;
              }

            }//end iret == OB_SUCCESS*/
          }
        }
      } 
      return iret;
    }

    common::ObString ObSSTableBlockIndexV2::get_end_key(const_iterator index_entry) const
    {
      common::ObString end_key(0, index_entry->end_key_size_, 
          const_cast<char*>(get_base()) + index_entry->end_key_offset_);
      return end_key;
    }

    common::ObString ObSSTableBlockIndexV2::get_start_key(const uint64_t table_id) const
    {
      const_iterator find_it = NULL;
      ObString start_key;
      int iret = find_start_in_table(table_id, find_it);
      if (OB_SUCCESS == iret)
      {
        start_key = get_end_key(find_it);
      }
      return start_key;
    }

    common::ObString ObSSTableBlockIndexV2::get_end_key(const uint64_t table_id) const
    {
      const_iterator find_it = NULL;
      ObString end_key;
      int iret = find_end_in_table(table_id, find_it);
      if (OB_SUCCESS == iret)
      {
        end_key = get_end_key(find_it);
      }
      return end_key;
    }

    int ObSSTableBlockIndexV2::find_start_in_table(const uint64_t table_id, 
        const_iterator& find_it) const
    {
      int iret = OB_SUCCESS;

      IndexEntryType entry;
      entry.table_id_ = table_id;
      entry.column_group_id_ = 0;
      entry.block_offset_ = 0;
      entry.block_record_size_ = 0;

      if (OB_SUCCESS == (iret = find_pos_helper(entry, true, find_it)))
      {
        if (find_it->table_id_ != table_id)
        {
          iret = OB_BEYOND_THE_RANGE;
        }
      }
      return iret;
    }

    int ObSSTableBlockIndexV2::find_end_in_table(const uint64_t table_id, 
        const_iterator& find_it) const
    {
      int iret = OB_SUCCESS;

      IndexEntryType entry;
      entry.table_id_ = table_id;
      // uint64_max, greater than any column group entry
      entry.column_group_id_ = OB_INVALID_ID;
      entry.block_offset_ = 0;
      entry.block_record_size_ = 0;

      if (OB_SUCCESS == (iret = find_pos_helper(entry, false, find_it)))
      {
        if (find_it->table_id_ != table_id)
        {
          iret = OB_BEYOND_THE_RANGE;
        }
      }

      return iret;
    }

    int ObSSTableBlockIndexV2::find_start_in_group(const uint64_t table_id, 
        const uint64_t column_group_id, const_iterator& find_it) const
    {
      int iret = OB_SUCCESS;

      IndexEntryType entry;
      entry.table_id_ = table_id;
      entry.column_group_id_ = column_group_id;
      entry.block_offset_ = 0;
      entry.block_record_size_ = 0;

      if (OB_SUCCESS == (iret = find_pos_helper(entry, true, find_it)))
      {
        if (!match_table_group(*find_it, table_id, column_group_id))
        {
          iret = OB_BEYOND_THE_RANGE;
        }
      }
      return iret;
    }

    int ObSSTableBlockIndexV2::find_end_in_group(const uint64_t table_id, 
        const uint64_t column_group_id, const_iterator& find_it) const
    {
      int iret = OB_SUCCESS;

      IndexEntryType entry;
      entry.table_id_ = table_id;
      entry.column_group_id_ = column_group_id;
      entry.block_offset_ = INT64_MAX;
      entry.block_record_size_ = 0;

      if (OB_SUCCESS == (iret = find_pos_helper(entry, false, find_it)))
      {
        if (!match_table_group(*find_it, table_id, column_group_id)) 
        {
          iret = OB_BEYOND_THE_RANGE;
        }
      }
      return iret;
    }

    int ObSSTableBlockIndexV2::find_pos_helper(const IndexEntryType& entry, 
        const bool is_start, const_iterator& find_it) const
    {
      int iret = OB_SUCCESS;
      Bound bound;

      if (OB_UNLIKELY(entry.table_id_ <= 0))
      {
        TBSYS_LOG(ERROR, "internal error, table=%ld,group=%ld",
            entry.table_id_, entry.column_group_id_);
        iret = OB_INVALID_ARGUMENT;
      }
      else if ( OB_UNLIKELY(OB_SUCCESS != (iret = get_bound(bound))) )
      {
        TBSYS_LOG(ERROR, "get position error, iret=%d", iret);
      }
      else
      {
        find_it = std::lower_bound(bound.begin_, bound.end_, entry);
        if (!is_start) --find_it;
        if (find_it < bound.begin_ || find_it >= bound.end_)
        {
          iret = OB_BEYOND_THE_RANGE;
        }
      }
      return iret;
    }

  }//end namespace sstable
}//end namespace oceanbase
