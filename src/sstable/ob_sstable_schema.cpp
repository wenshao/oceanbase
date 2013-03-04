/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * ob_sstable_schema.cpp for persistent schema.
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */
#include <algorithm>
#include <tblog.h>
#include "common/serialization.h"
#include "ob_sstable_schema.h"
#include "ob_sstable_trailer.h"

namespace oceanbase
{
  namespace sstable
  {
    using namespace std;
    using namespace common;
    using namespace common::serialization;
    
    int64_t CombineIdKey::hash() const
    {
      hash::hash_func<uint64_t> hash;
      return hash(table_id_) + hash(sec_id_);
    }

    bool CombineIdKey::operator==(const CombineIdKey& key) const
    {
      bool ret = false;
      if (table_id_ == key.table_id_ && sec_id_ == key.sec_id_)
      {
        ret = true;
      }
      return ret;
    }
    
    ObSSTableSchema::ObSSTableSchema():column_def_(column_def_array_), hash_init_(false),
                                       curr_group_head_(0)
    {
      memset(&column_def_array_, 0, sizeof(common::OB_MAX_COLUMN_NUMBER));
      memset(&schema_header_, 0, sizeof(ObSSTableSchemaHeader));
    }

    ObSSTableSchema::~ObSSTableSchema()
    {
      if (NULL != column_def_ && column_def_array_ != column_def_)
      {
        ob_free(column_def_);
        column_def_ = NULL;
      }

      if (hash_init_)
      {
        table_schema_hashmap_.destroy();
        group_schema_hashmap_.destroy();
        group_index_hashmap_.destroy();
      }
    }

    //In order to compatible with v0.1.0
    //if total_column_count equals zero, use column_count instead
    const int64_t ObSSTableSchema::get_column_count() const
    {
      int64_t count = 0;
      if (0 == schema_header_.total_column_count_)
      {
        count = schema_header_.column_count_;
#ifdef COMPATIBLE
        version_ = ObSSTableTrailer::SSTABLEV1;
#endif
      }
      else
      {
        count = schema_header_.total_column_count_;
#ifdef COMPATIBLE
        version_ = ObSSTableTrailer::SSTABLEV2;
#endif
      }
      return count;
    }

    const ObSSTableSchemaColumnDef* ObSSTableSchema::get_column_def(
      const int32_t index) const
    {
      const ObSSTableSchemaColumnDef* ret = NULL;
      if (index >= 0 && index < get_column_count())
      {
        ret = &column_def_[index];
      }
      else
      {
        TBSYS_LOG(WARN, "invalid param, index=%d, column_count=%ld",
                  index, get_column_count());
      }
      
      return ret;
    }

    DEFINE_SERIALIZE(ObSSTableSchemaHeader)
    {
      int ret                 = OB_SUCCESS;
      int64_t serialize_size  = get_serialize_size();

      if((NULL == buf) || (serialize_size + pos > buf_len))
      {
        TBSYS_LOG(WARN, "invalid param, buf=%p, buf_len=%ld, pos=%ld,"
                        "serialize_size=%ld",
                  buf, buf_len, pos, serialize_size);
        ret = OB_ERROR;
      }

      if (OB_SUCCESS == ret
          && (OB_SUCCESS == (ret = encode_i16(buf, buf_len, pos, column_count_)))
          && (OB_SUCCESS == (ret = encode_i16(buf, buf_len, pos, reserved16_)))
          && (OB_SUCCESS == (ret = encode_i32(buf, buf_len, pos, total_column_count_))))
      {
        //do nothing here
      }
       else
      {
        TBSYS_LOG(WARN, "failed to serialize schema header");
      }

      return ret;
    }

    DEFINE_DESERIALIZE(ObSSTableSchemaHeader)
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

      if (OB_SUCCESS == ret
          && (OB_SUCCESS == (ret = decode_i16(buf, data_len, pos, &column_count_)))
          && (OB_SUCCESS == (ret = decode_i16(buf, data_len, pos, &reserved16_)))
          && (OB_SUCCESS == (ret = decode_i32(buf, data_len, pos, &total_column_count_))))
      {
        //do nothing here
      }
      else
      {
        TBSYS_LOG(WARN, "failed to deserialize schema header");
      }

      return ret;
    }

    DEFINE_GET_SERIALIZE_SIZE(ObSSTableSchemaHeader)
    {
      return (encoded_length_i16(column_count_)
              + encoded_length_i16(reserved16_)
              + encoded_length_i32(total_column_count_));
    }

    DEFINE_SERIALIZE(ObSSTableSchemaColumnDef)
    {
      int ret                 = OB_SUCCESS;
      int64_t serialize_size  = get_serialize_size();

      if((NULL == buf) || (serialize_size + pos > buf_len))
      {
        TBSYS_LOG(WARN, "invalid param, buf=%p, buf_len=%ld, pos=%ld,"
                        "serialize_size=%ld",
                  buf, buf_len, pos, serialize_size);
        ret = OB_ERROR;
      }

      if (OB_SUCCESS == ret
          && (OB_SUCCESS == (ret = encode_i16(buf, buf_len, pos, reserved_)))
          && (OB_SUCCESS == (ret = encode_i16(buf, buf_len, pos, column_group_id_)))
          && (OB_SUCCESS == (ret = encode_i32(buf, buf_len, pos, column_name_id_)))
          && (OB_SUCCESS == (ret = encode_i32(buf, buf_len, pos, column_value_type_)))
          && (OB_SUCCESS == (ret = encode_i32(buf, buf_len, pos, table_id_))))
      {
        //do nothing here
      }
       else
      {
        TBSYS_LOG(WARN, "failed to serialzie schema def, buf=%p, "
                        "buf_len=%ld, pos=%ld", buf, buf_len, pos);
      }

      return ret;
    }

    DEFINE_DESERIALIZE(ObSSTableSchemaColumnDef)
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

      if (OB_SUCCESS == ret
          && (OB_SUCCESS == (ret = decode_i16(buf, data_len, pos, &reserved_)))
          && (OB_SUCCESS == (ret = decode_i16(buf, data_len, pos,
                                              reinterpret_cast<int16_t*>(&column_group_id_))))
          && (OB_SUCCESS == (ret = decode_i32(buf, data_len, pos,
                                              reinterpret_cast<int32_t*>(&column_name_id_))))
          && (OB_SUCCESS == (ret = decode_i32(buf, data_len, pos, &column_value_type_)))
          && (OB_SUCCESS == (ret = decode_i32(buf, data_len, pos,
                                              reinterpret_cast<int32_t*>(&table_id_)))))
      {
        //do nothing here
      }
      else
      {
        TBSYS_LOG(WARN, "failed to serialzie schema column def, buf=%p, "
                        "buf_len=%ld, pos=%ld", buf, data_len, pos);
      }

      return ret;
    }

    DEFINE_GET_SERIALIZE_SIZE(ObSSTableSchemaColumnDef)
    {
      return (encoded_length_i16(reserved_)
              + encoded_length_i16(column_group_id_)
              + encoded_length_i32(column_name_id_)
              + encoded_length_i32(column_value_type_)
              + encoded_length_i32(table_id_));
    }

    void ObSSTableSchema::reset()
    {
      memset(&column_def_array_, 0, sizeof(common::OB_MAX_COLUMN_NUMBER));
      memset(&schema_header_, 0, sizeof(ObSSTableSchemaHeader));
      if (column_def_ != column_def_array_ && column_def_ != NULL)
      {
        ob_free(column_def_);
      }
      column_def_ = column_def_array_;
      
      if (hash_init_)
      {
        table_schema_hashmap_.clear();
        group_schema_hashmap_.clear();
        group_index_hashmap_.clear();
      }

#ifdef COMPATIBLE
      version_ = ObSSTableTrailer::SSTABLEV2;
#endif
    }

    int ObSSTableSchema::update_hashmaps(const ObSSTableSchemaColumnDef& column_def)
    {
      int ret = OB_SUCCESS;
      if (OB_SUCCESS != (ret = update_table_schema_hashmap(column_def)))
      {
        TBSYS_LOG(WARN, "update table schema hashmap failed. table_id=%u, column_group_id=%u,"
                  "column_name_id=%u", column_def.table_id_,
                  column_def.column_group_id_, column_def.column_name_id_);
      }
      else if (OB_SUCCESS != (ret = update_group_schema_hashmap(column_def)))
      {
        TBSYS_LOG(WARN, "update group schema hashmap failed. table_id=%u, column_group_id=%u,"
                  "column_name_id=%u", column_def.table_id_,
                  column_def.column_group_id_, column_def.column_name_id_);
      }
      else if (OB_SUCCESS != (ret = update_group_index_hashmap(column_def)))
      {
        TBSYS_LOG(WARN, "update group index  hashmap failed. table_id=%u, column_group_id=%u,"
                  "column_name_id=%u", column_def.table_id_,
                  column_def.column_group_id_, column_def.column_name_id_);
      }
      return ret;
    }

    int ObSSTableSchema::update_table_schema_hashmap(const ObSSTableSchemaColumnDef& column_def)
    {
      int ret = OB_SUCCESS;
      if (!hash_init_)
      {
        TBSYS_LOG(WARN, "hashmap not init yet");
        ret = OB_ERROR;
      }
      
      if (OB_SUCCESS == ret)
      {
        ValueSet table_value;
        int64_t size = get_column_count();
        const ObSSTableSchemaColumnDef * last = NULL;
        if (size > 0)
        {
          last = &column_def_[size - 1];
        }
        int ret_hash = 0;

        if (0 == size || (NULL != last && column_def.table_id_ != last->table_id_))
        {
          table_value.head_offset_ = static_cast<int16_t>(size);
          table_value.tail_offset_ = static_cast<int16_t>(size);
          table_value.size_ = 1;
          ret_hash = table_schema_hashmap_.set(column_def.table_id_, table_value);
          if (-1 == ret_hash)
          {
            TBSYS_LOG(ERROR, "Insert table schema hashmap faied"
                      "key = %u", column_def.table_id_);
            ret = OB_ERROR;
          }
        }
        else if (NULL != last && column_def.column_group_id_ != last->column_group_id_)
        {
          ret_hash = table_schema_hashmap_.get(column_def.table_id_, table_value);
          if (-1 == ret_hash || hash::HASH_NOT_EXIST == ret_hash)
          {
            TBSYS_LOG(ERROR, "Get (table_id=%u) from table schema hashmap failed", column_def.table_id_);
            ret = OB_ERROR;
          }
          else if (hash::HASH_EXIST == ret_hash)
          {
            //update table value and set table group next of column def
            ObSSTableSchemaColumnDef* def = column_def_ + table_value.tail_offset_;
            def->table_group_next_   = static_cast<int16_t>(size);
            table_value.tail_offset_ = static_cast<int16_t>(size);
            ++table_value.size_;
            ret_hash = table_schema_hashmap_.set(column_def.table_id_, table_value, 1); //overwrite
            if (-1 == ret_hash)
            {
              TBSYS_LOG(ERROR, "insert table_schema_hashmap_ failed table_id=%u", column_def.table_id_);
              ret = OB_ERROR;
            }
          }
        }
      }
      return ret;
    }

    int ObSSTableSchema::update_group_schema_hashmap(const ObSSTableSchemaColumnDef& column_def)
    {
      int ret = OB_SUCCESS;
      if (!hash_init_)
      {
        TBSYS_LOG(WARN, "hashmap not init yet");
        ret = OB_ERROR;
      }
      
      if (OB_SUCCESS == ret)
      {
        CombineIdKey group_key;
        group_key.table_id_ = column_def.table_id_;
        group_key.sec_id_   = column_def.column_group_id_;
        ValueSet group_value;
        int64_t size = get_column_count();
        int ret_hash = 0;

        if (OB_SUCCESS == ret)
        {
          ret_hash = group_schema_hashmap_.get(group_key, group_value);
          if (-1 == ret_hash)
          {
            TBSYS_LOG(ERROR, "Get key(table_id=%lu, column_group_id=%lu)"
                      "from group schema hashmap failed", group_key.table_id_, group_key.sec_id_);
            ret = OB_ERROR;
          }
          else if (hash::HASH_NOT_EXIST == ret_hash)
          {
            group_value.head_offset_ = static_cast<int16_t>(size);
            group_value.size_ = 1;
            ret_hash = group_schema_hashmap_.set(group_key, group_value);
            if (-1 == ret_hash)
            {
              TBSYS_LOG(ERROR, "insert group_schemma_hashmap_ failed "
                        "key(table_id=%lu, column_group_id=%lu", group_key.table_id_, group_key.sec_id_);
              ret = OB_ERROR;
            }
          }
          else if (hash::HASH_EXIST == ret_hash)
          {
            ++group_value.size_;
            ret_hash = group_schema_hashmap_.set(group_key, group_value, 1); //overwrite
            if (-1 == ret_hash)
            {
              TBSYS_LOG(ERROR, "insert group_schemma_hashmap_ failed "
                        "key(table_id=%lu, column_group_id=%lu", group_key.table_id_, group_key.sec_id_);
              ret = OB_ERROR;
            }
          }
        }
      }
      return ret;
    }

    int ObSSTableSchema::update_group_index_hashmap(const ObSSTableSchemaColumnDef& column_def)
    {
      int ret = OB_SUCCESS;
      if (!hash_init_)
      {
        TBSYS_LOG(WARN, "hashmap not init yet");
        ret = OB_ERROR;
      }
      
      if (OB_SUCCESS == ret)
      {
        CombineIdKey group_index_key;
        group_index_key.table_id_ = column_def.table_id_;
        group_index_key.sec_id_   = column_def.column_name_id_;
        ValueSet index_value;
        int64_t size = get_column_count();
        int ret_hash = 0;

        if (OB_SUCCESS == ret)
        {
          ret_hash = group_index_hashmap_.get(group_index_key, index_value);
          if (-1 == ret_hash)
          {
            TBSYS_LOG(ERROR, "Get key(table_id=%lu, column_name_id=%lu)"
                      "from group index hashmap failed",
                      group_index_key.table_id_, group_index_key.sec_id_);
            ret = OB_ERROR;
          }
          else if (hash::HASH_NOT_EXIST == ret_hash)
          {
            index_value.head_offset_ = static_cast<int16_t>(size);
            index_value.tail_offset_ = static_cast<int16_t>(size);
            index_value.size_ = 1;
            ret_hash = group_index_hashmap_.set(group_index_key, index_value);
            if (-1 == ret_hash)
            {
              TBSYS_LOG(ERROR, "Get key(table_id=%lu, column_name_id=%lu)"
                        "from group index hashmap failed",
                        group_index_key.table_id_, group_index_key.sec_id_);
              ret = OB_ERROR;
            }
          }
          else if (hash::HASH_EXIST == ret_hash)
          {
            ObSSTableSchemaColumnDef* def = column_def_ + index_value.tail_offset_;
            def->group_column_next_  = static_cast<int16_t>(size);
            index_value.tail_offset_ = static_cast<int16_t>(size);
            ++index_value.size_;
            ret_hash = group_index_hashmap_.set(group_index_key, index_value, 1); //overwrite
            if (-1 == ret_hash)
            {
              TBSYS_LOG(ERROR, "Get key(table_id=%lu, column_name_id=%lu)"
                        "from group index hashmap failed",
                        group_index_key.table_id_, group_index_key.sec_id_);
              ret = OB_ERROR;
            }
          }
        }
      }
      return ret;
    }
    
    int ObSSTableSchema::add_column_def(const ObSSTableSchemaColumnDef& column_def)
    {
      int ret = OB_SUCCESS;

      int32_t size = get_column_count();
      if (DEFAULT_COLUMN_DEF_SIZE <= size)
      {
        TBSYS_LOG(WARN, "Can not add any more column_def, column_count=%d", size);
        ret = OB_ERROR;
      }
      //be sure that column_def is in order of (table_id, column_group_id, column_name_id)
      ObSSTableSchemaColumnDefCompare compare;
      if ( OB_SUCCESS == ret && 0 < size && false == compare(column_def_[size - 1], column_def) )
      {
        TBSYS_LOG(WARN, "column def must be order in (table_id, group_id, column_id),"
                  "previous def is (%u,%hu,%u)\t"
                  "current def is (%u,%hu,%u)",
                  column_def_[size-1].table_id_, column_def_[size-1].column_group_id_,
                  column_def_[size-1].column_name_id_, column_def.table_id_,
                  column_def.column_group_id_, column_def.column_name_id_);
        ret = OB_ERROR;
      }
      
      //create hashmaps
      if (OB_SUCCESS == ret && !hash_init_)
      {
        ret = create_hashmaps();
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "create hashmaps failed");
        }
      }
      
      if (OB_SUCCESS == ret)
      {
        if (common::OB_MAX_COLUMN_NUMBER <= size
            && column_def_ == column_def_array_)
        {
          column_def_ = reinterpret_cast<ObSSTableSchemaColumnDef*>
            (ob_malloc(sizeof(ObSSTableSchemaColumnDef)*(DEFAULT_COLUMN_DEF_SIZE)));
          if (NULL == column_def_)
          {
            TBSYS_LOG(WARN, "failed to alloc memory column_def_=%p", column_def_);
            ret = OB_ERROR;
          }
          else
          {
            memcpy(column_def_, column_def_array_, sizeof(ObSSTableSchemaColumnDef) * size);
          }
        }

        //set curr_group_head_
        if (OB_SUCCESS == ret)
        {
          if (0 == size || column_def.table_id_ != column_def_[size - 1].table_id_
              || column_def.column_group_id_ != column_def_[size - 1].column_group_id_)
          {
            curr_group_head_ = size;
          }
        }

        //set offset in group of column def
        if (OB_SUCCESS == ret)
        {
          column_def.offset_in_group_ = static_cast<int32_t>(size - curr_group_head_);
        }
        
        if (OB_SUCCESS == ret)
        {
          ret = update_hashmaps(column_def);
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "update hashmaps failed ret=%d", ret);
          }
        }

        if (OB_SUCCESS == ret)
        {
          column_def_[schema_header_.total_column_count_++] = column_def;
        }
      }
      return ret;
    }

    int ObSSTableSchema::get_table_column_groups_id(const uint64_t table_id,
                                                    uint64_t* group_ids, int64_t & size)const
    {
      int ret = OB_SUCCESS;
      if (OB_INVALID_ID == table_id || 0 == table_id)
      {
        TBSYS_LOG(WARN, "invalid arguments table_id = %lu", table_id);
        ret = OB_ERROR;
      }
      else if (0 != get_column_count())
      {
#ifdef COMPATIBLE
        if (ObSSTableTrailer::SSTABLEV1 == version_)
        {
          if (size >= 1)
          {
            group_ids[0] = 0;
            size = 1;            
          }
          else
          {
            size = 0;
            ret = OB_SIZE_OVERFLOW;
          }
        }
        else
#endif
        {
          if (hash_init_)
          {
            ValueSet group_id_set;
            int hash_get = 0;
            hash_get = table_schema_hashmap_.get(table_id, group_id_set);
            if (-1 == hash_get)
            {
              TBSYS_LOG(ERROR, "Get table_id=%lu failed", table_id);
              ret = OB_ERROR;
            }
            else if (hash::HASH_NOT_EXIST == hash_get)
            {
              size = 0;
              ret = OB_ERROR;
            }
            else if (hash::HASH_EXIST == hash_get)
            {
              if (size < group_id_set.size_)
              {
                ret = OB_SIZE_OVERFLOW;
              }
              else
              {
                size = group_id_set.size_;
              }
              ObSSTableSchemaColumnDef* def = NULL;
              int32_t offset = 0;
              for (int index = 0; index < size; ++index)
              {
                if (0 == index)
                {
                  offset = group_id_set.head_offset_;
                }
                else
                {
                  offset = def->table_group_next_;
                }

                if (offset < 0 || offset >= get_column_count())
                {
                  TBSYS_LOG(ERROR, "offset overflosw offset=%d total column count=%ld",
                            offset, get_column_count());
                  ret = OB_ERROR;
                  break;
                }
                else
                {
                  def = column_def_ + offset;
                  group_ids[index] = def->column_group_id_;
                }
              }
            }
          }
          else
          {
            TBSYS_LOG(INFO, "hashmap not init yet");
            size = 0;
            ret = OB_ERROR;
          }
        }
      }
      return ret;
    }

    bool ObSSTableSchema::is_table_exist(const uint64_t table_id) const
    {
      int bret = false;

      if (OB_INVALID_ID == table_id || 0 == table_id)
      {
        TBSYS_LOG(WARN, "invalid arguments table_id = %lu", table_id);
      }
      else if (0 != get_column_count())
      {
        if (hash_init_)
        {
          ValueSet group_id_set;
          int hash_get = table_schema_hashmap_.get(table_id, group_id_set);
          if (-1 == hash_get)
          {
            TBSYS_LOG(ERROR, "Get table_id=%lu failed", table_id);
          }
          else if (hash::HASH_NOT_EXIST == hash_get)
          {

          }
          else if (hash::HASH_EXIST == hash_get)
          {
            bret = true;
          }
        }
        else
        {
          TBSYS_LOG(INFO, "hashmap not init yet");
        }
      }

      return bret;
    }

    const ObSSTableSchemaColumnDef* ObSSTableSchema::get_group_schema(const uint64_t table_id,
                                                                      const uint64_t group_id,
                                                                      int64_t& size) const
    {
      const ObSSTableSchemaColumnDef* ret   = NULL;
      size = 0;
      if (OB_INVALID_ID == table_id || 0 == table_id || OB_INVALID_ID == group_id)
      {
        TBSYS_LOG(WARN, "invalid argument table_id=%lu, group_id=%lu", table_id, group_id);
      }
      else if ( 0 != get_column_count() )
      {
#ifdef COMPATIBLE
        if (ObSSTableTrailer::SSTABLEV1 == version_)
        {
          if (0 == group_id)
          {
            ret = const_cast<ObSSTableSchemaColumnDef*>(column_def_);
            size = get_column_count();
          }
          else
          {
            TBSYS_LOG(WARN, "invalid argument column_group_id=%ld when schema is old version",
                      group_id);
          }
        }
        else
#endif
        {
          if (hash_init_)
          {
            CombineIdKey key;
            key.table_id_ = table_id;
            key.sec_id_   = group_id;

            ValueSet column_set;
            int hash_get = 0;
            hash_get = group_schema_hashmap_.get(key, column_set);
            if (-1 == hash_get)
            {
              TBSYS_LOG(ERROR, "Get table_id=%lu failed", table_id);
            }
            else if (hash::HASH_NOT_EXIST == hash_get)
            {
              ret = NULL;
              size = 0;
            }
            else if (hash::HASH_EXIST == hash_get)
            {
              ret  = &column_def_[column_set.head_offset_];
              size = column_set.size_;
            }
          }
          else
          {
            TBSYS_LOG(INFO, "hashmap not init yet");
          }
        }
      }
      return ret;
    }

    int ObSSTableSchema::get_column_groups_id(const uint64_t table_id, const uint64_t column_id,
                                              uint64_t *array, int64_t &size) const
    {
      int ret = OB_SUCCESS;
      if (OB_INVALID_ID == table_id || 0 == table_id
          || OB_INVALID_ID == column_id)
      {
           TBSYS_LOG(WARN, "invalid argument table_id = %lu, column_id = %lu",
                  table_id, column_id);
        ret = OB_ERROR;
      }
      
      if (OB_SUCCESS == ret)
      {
#ifdef COMPATIBLE
        if (ObSSTableTrailer::SSTABLEV1 == version_)
        {
          if (size >= 1)
          {
            array[0] = 0;
            size = 1;
          }
          else
          {
            size = 0;
            ret  = OB_SIZE_OVERFLOW;
          }
        }
        else
#endif
        {
          if (hash_init_)
          {
            CombineIdKey key;
            key.table_id_ = table_id;
            key.sec_id_   = column_id;

            ValueSet group_set;
            int hash_get = 0;
            hash_get = group_index_hashmap_.get(key, group_set);
            if (-1 == hash_get)
            {
              TBSYS_LOG(ERROR, "Get table_id=%lu failed", table_id);
              ret = OB_ERROR;
            }
            else if (hash::HASH_NOT_EXIST == hash_get)
            {
              ret = OB_ERROR;
            }
            else if (hash::HASH_EXIST == hash_get)
            {
              if (size < group_set.size_)
              {
                ret = OB_SIZE_OVERFLOW;
              }
              else
              {
                size = group_set.size_;
              }
              ObSSTableSchemaColumnDef* def = NULL;
              int32_t offset = 0;
              for (int index = 0; index < size; ++index)
              {
                if (0 == index)
                {
                  offset = group_set.head_offset_;
                }
                else
                {
                  offset = def->group_column_next_;
                }
                if (offset < 0 || offset >= get_column_count())
                {
                  TBSYS_LOG(ERROR, "offset overflosw offset=%d total column count=%ld",
                            offset, get_column_count());
                  ret = OB_ERROR;
                  break;
                }
                else
                {
                  def = column_def_ + offset;
                  array[index] = def->column_group_id_;
                }
              }
            }
          }
          else
          {
            TBSYS_LOG(INFO, "hashmap not init yet");
          }
        }
      }
      return ret;
    }

    const int64_t ObSSTableSchema::find_offset_first_column_group_schema(const uint64_t table_id,
                                                                         const uint64_t column_id,
                                                                         uint64_t & column_group_id) const
    {
      int64_t ret = -1;
      if (OB_INVALID_ID == table_id || 0 == table_id
          || OB_INVALID_ID == column_id)
      {
        TBSYS_LOG(WARN, "invalid argument table_id=%lu, column_id=%lu", table_id, column_id);
      }
      
      else if (0 < get_column_count())
      {
#ifdef COMPATIBLE
        if (ObSSTableTrailer::SSTABLEV1 == version_)
        {
          //Only one group one table in sstable schema V1
          ret = find_column_id(column_id);
          if (-1 != ret)
          {
            column_group_id = 0;
          }
        }
        else
#endif
        {
          if (hash_init_)
          {
            CombineIdKey key;
            key.table_id_ = table_id;
            key.sec_id_   = column_id;

            ValueSet group_set;
            int hash_get = 0;
            hash_get = group_index_hashmap_.get(key, group_set);
            if (-1 == hash_get)
            {
              TBSYS_LOG(ERROR, "Get table_id=%lu failed", table_id);
            }
            else if (hash::HASH_NOT_EXIST == hash_get)
            {
              
            }
            else if (hash::HASH_EXIST == hash_get)
            {
              if (0 < group_set.size_)
              {
                ObSSTableSchemaColumnDef* def = column_def_ + group_set.head_offset_;
                if (group_set.head_offset_ < 0 || 
                    group_set.head_offset_ >= get_column_count())
                {
                  TBSYS_LOG(ERROR, "offset overflosw offset=%d max_offset=%lu",
                            group_set.head_offset_, get_column_count());
                  ret = OB_ERROR;
                }
                else
                {
                  column_group_id = def->column_group_id_;
                  ret = def->offset_in_group_;
                }
              }
            }
          }
          else
          {
            TBSYS_LOG(INFO, "hashmap not init yet");
          }
        }
      }
      return ret;
    }

    const int64_t ObSSTableSchema::find_offset_column_group_schema(const uint64_t table_id,
                                                                   const uint64_t column_group_id,
                                                                   const uint64_t column_id) const
    {
      int64_t ret = -1;
      if (OB_INVALID_ID == table_id || 0 == table_id
          ||OB_INVALID_ID == column_group_id || OB_INVALID_ID == column_id)
      {
        TBSYS_LOG(WARN, "invalid argument table_id = %lu, column_group_id = %lu"
                  "column_id = %lu", table_id, column_group_id, column_id);
      }
      
      else if (0 < get_column_count())
      {
#ifdef COMPATIBLE
        if (ObSSTableTrailer::SSTABLEV1 == version_)
        {
          if (0 == column_group_id)
          {
            ret = find_column_id(column_id);
          }
          else
          {
            TBSYS_LOG(WARN, "invalid argument column_group_id=%lu", column_group_id);
          }
        }
        else
#endif
        {
          if (hash_init_)
          {
            CombineIdKey key;
            key.table_id_ = table_id;
            key.sec_id_   = column_id;

            ValueSet group_set;
            int hash_get = 0;
            hash_get = group_index_hashmap_.get(key, group_set);
            if (-1 == hash_get)
            {
              TBSYS_LOG(ERROR, "Get table_id=%lu failed", table_id);
            }
            else if (hash::HASH_NOT_EXIST == hash_get)
            {
              
            }
            else if (hash::HASH_EXIST == hash_get)
            {
              ObSSTableSchemaColumnDef* def = NULL;
              int64_t offset = 0;
              for (int32_t index = 0; index < group_set.size_; ++index)
              {
                if (0 == index)
                {
                  offset = group_set.head_offset_;
                }
                else
                {
                  offset = def->group_column_next_;
                }
                if (offset < 0 || offset >= get_column_count())
                {
                  TBSYS_LOG(ERROR, "offset overflosw offset=%ld total colum count=%ld",
                            offset, get_column_count());
                  ret = OB_ERROR;
                  break;
                }
                else
                {
                  def = column_def_ + offset;
                  if (def->column_group_id_ == column_group_id)
                  {
                    ret = def->offset_in_group_;
                    break;
                  }
                }
              }
            }
          }
          else
          {
            TBSYS_LOG(INFO, "hashmap not init yet");
          }
        }
      }
      return ret;
    }
    
    bool ObSSTableSchema::is_column_exist(const uint64_t table_id, const uint64_t column_id) const
    {
      bool ret = false;
      uint64_t group_id = 0;
      int64_t offset = find_offset_first_column_group_schema(table_id, column_id, group_id);
      if (0 <= offset)
      {
        ret = true;
      }
      return ret;
    }

    int ObSSTableSchema::create_hashmaps()
    {
      int ret = OB_SUCCESS;
      if (OB_SUCCESS != (ret = table_schema_hashmap_.create(hash::cal_next_prime(OB_MAX_TABLE_NUMBER))))
      {
        TBSYS_LOG(WARN, "create table schema hashmap failed");
      }
      else if (OB_SUCCESS != (ret = group_schema_hashmap_.create(hash::cal_next_prime(512))))
      {
        TBSYS_LOG(WARN, "creat group schema hashmap failed");
      }
      else if (OB_SUCCESS != (ret = group_index_hashmap_.create(hash::cal_next_prime(512))))
      {
        TBSYS_LOG(WARN, "creat group index hashmap failed");
      }
      else
      {
        hash_init_ = true;
      }
      return ret;
    }

#ifdef COMPATIBLE
    const int64_t ObSSTableSchema::find_column_id(const uint64_t column_id)const
    {
      int64_t ret    = -1;
      int64_t left   = 0;
      int64_t middle = 0;
      int64_t right  = schema_header_.column_count_ - 1;
      
      while (left <= right)
      {
        middle = (left + right)/2;
        if (column_def_[middle].column_name_id_ > column_id)
        {
          right = middle - 1;
        }
        else if (column_def_[middle].column_name_id_ < column_id)
        {
          left = middle + 1;
        }
        else
        {
          ret = middle;
          break;
        }
      }
      return ret;
    }
#endif
    DEFINE_SERIALIZE(ObSSTableSchema)
    {
      int ret                 = OB_SUCCESS;
      int64_t serialize_size  = get_serialize_size();

      if((NULL == buf) || (serialize_size + pos > buf_len))
      {
        TBSYS_LOG(WARN, "invalid param, buf=%p, buf_len=%ld, pos=%ld,"
                        "serialize_size=%ld",
                  buf, buf_len, pos, serialize_size);
        ret = OB_ERROR;
      }

      if (OB_SUCCESS == ret)
      {
        ret = schema_header_.serialize(buf, buf_len, pos);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "failed to serialize schema header, buf=%p, "
                          "buf_len=%ld, pos=%ld, column_count=%d",
                    buf, buf_len, pos, schema_header_.total_column_count_);
        }
      }

      if (OB_SUCCESS == ret)
      {
        for (int64_t i = 0; OB_SUCCESS == ret
             && i < schema_header_.total_column_count_; ++i)
        {
          ret = column_def_[i].serialize(buf, buf_len, pos);
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "failed to serialize schema column def, buf=%p, "
                            "buf_len=%ld, pos=%ld, column index=%ld, "
                            "column_count=%d",
                      buf, buf_len, pos, i, schema_header_.total_column_count_);
            break;
          }
        }
      }

      return ret;
    }

    DEFINE_DESERIALIZE(ObSSTableSchema)
    {
      int ret = OB_SUCCESS;
      int64_t column_count = 0;

      if (NULL == buf || data_len <= 0 || pos > data_len)
      {
        TBSYS_LOG(WARN, "invalid param, buf=%p, data_len=%ld, pos=%ld",
                  buf, data_len, pos);
        ret = OB_ERROR;
      }
      reset();
      
      if (OB_SUCCESS == ret)
      {
        ret = schema_header_.deserialize(buf, data_len, pos);
        column_count = get_column_count();
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "failed to deserialize schema header, buf=%p, "
                          "data_len=%ld, pos=%ld, column_count=%ld",
                    buf, data_len, pos, column_count);
        }
      }
      
      if (OB_SUCCESS == ret && common::OB_MAX_COLUMN_NUMBER <= column_count)
      {
        column_def_ = reinterpret_cast<ObSSTableSchemaColumnDef*>
          (ob_malloc(sizeof(ObSSTableSchemaColumnDef) * (DEFAULT_COLUMN_DEF_SIZE)));
        if (NULL == column_def_)
        {
          TBSYS_LOG(WARN, "failed to alloc memory");
          ret = OB_ERROR;
        }
      }

#ifdef COMPATIBLE      
      if (ObSSTableTrailer::SSTABLEV1 == version_)
      {
        for (int64_t i = 0; OB_SUCCESS == ret && i < column_count; ++i)
        {
          ret = column_def_[i].deserialize(buf, data_len, pos);
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "failed to deserialize schema column def, buf=%p, "
                      "data_len=%ld, pos=%ld, column index=%ld, "
                      "column_count=%ld",
                      buf, data_len, pos, i, column_count);
            break;
          }
        }
      }
      else
#endif
      {
        if (OB_SUCCESS == ret)
        {
          schema_header_.total_column_count_ = 0;
          ObSSTableSchemaColumnDef column_def;
          for (int64_t i = 0; OB_SUCCESS == ret && i < column_count; ++i)
          {
            ret = column_def.deserialize(buf, data_len, pos);
            if (OB_SUCCESS != ret)
            {
              TBSYS_LOG(WARN, "failed to deserialize schema column def, buf=%p, "
                        "data_len=%ld, pos=%ld, column index=%ld, "
                        "column_count=%ld",
                        buf, data_len, pos, i, column_count);
                break;
            }
            if (OB_SUCCESS == ret)
            {
              ret = add_column_def(column_def);
              if (OB_SUCCESS != ret)
              {
                TBSYS_LOG(WARN, "add column def failed cocolumn_def(table_id=%u, column_group_id=%u"
                          ", column_name_id=%u", column_def.table_id_,
                          column_def.column_group_id_, column_def.column_name_id_);;
              }
            }
          }
        }
      }
      return ret;
    }

    DEFINE_GET_SERIALIZE_SIZE(ObSSTableSchema)
    {
      int64_t total_size  = 0;
      int32_t column_count = get_column_count();

      total_size += schema_header_.get_serialize_size();
      total_size += column_def_[0].get_serialize_size() * column_count;

      return total_size;
    }
  } // end namespace sstable
} // end namespace oceanbase
