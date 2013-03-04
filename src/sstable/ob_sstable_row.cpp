/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_sstable_row.cpp for persistent ssatable single row. 
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */
#include "ob_sstable_row.h"

namespace oceanbase 
{ 
  namespace sstable 
  {
    using namespace common;
    using namespace common::serialization;

    ObSSTableRow::ObSSTableRow() 
    : table_id_(OB_INVALID_ID), column_group_id_(OB_INVALID_ID), row_key_buf_(NULL), 
      row_key_len_(0), row_key_buf_size_(DEFAULT_KEY_BUF_SIZE), objs_(NULL),
      objs_buf_size_(DEFAULT_OBJS_BUF_SIZE), obj_count_(0), 
      string_buf_(ObModIds::OB_SSTABLE_WRITER)
    {
    }

    ObSSTableRow::~ObSSTableRow()
    {
      if (NULL != row_key_buf_)
      {
        ob_free(row_key_buf_);
        row_key_buf_ = NULL;
      }
      if (NULL != objs_)
      {
        ob_free(objs_);
        objs_ = NULL;
      }
      string_buf_.clear();
    }

    const int64_t ObSSTableRow::get_obj_count() const
    {
      return obj_count_;
    }

    int ObSSTableRow::set_obj_count(const int64_t obj_count, 
                                    const bool sparse_format)
    {
      int ret = OB_SUCCESS;

      if (obj_count <= 0 || (!sparse_format && obj_count > OB_MAX_COLUMN_NUMBER))
      {
        TBSYS_LOG(WARN, "invalid param, obj_count=%ld, sparse_format=%d", 
                  obj_count, sparse_format);
        ret = OB_ERROR;
      }
      else
      {
        obj_count_ = static_cast<int32_t>(obj_count);
      }

      return ret;
    }

    const uint64_t ObSSTableRow::get_table_id() const
    {
      return table_id_;
    }

    int ObSSTableRow::set_table_id(const uint64_t table_id)
    {
      int ret = OB_SUCCESS;
      if (0 == table_id || OB_INVALID_ID == table_id)
      {
        TBSYS_LOG(WARN, "invalid argument table_id=%lu", table_id);
        ret = OB_ERROR;
      }
      else
      {
        table_id_ = table_id;
      }
      return ret;
    }

    const uint64_t ObSSTableRow::get_column_group_id() const
    {
      return column_group_id_;
    }

    int ObSSTableRow::set_column_group_id(const uint64_t column_group_id)
    {
      int ret = OB_SUCCESS;
      if (OB_INVALID_ID == column_group_id)
      {
        TBSYS_LOG(WARN, "invalid argument column_group_id=%lu", column_group_id);
        ret = OB_ERROR;
      }
      else
      {
        column_group_id_ = column_group_id;
      }
      return ret;
    }

    const ObString ObSSTableRow::get_row_key() const
    {
      ObString row_key;

      if (NULL != row_key_buf_ && row_key_len_ > 0)
      {
          row_key.assign(row_key_buf_, row_key_len_);
      }

      return row_key;
    }

    const ObObj* ObSSTableRow::get_obj(const int32_t index) const
    {
      const ObObj* obj = NULL;

      if (index >= 0 && index < obj_count_)
      {
        obj = &objs_[index];
      }
      else
      {
        TBSYS_LOG(WARN, "invalid param, index=%d", index);
      }

      return obj;
    }

    const ObObj* ObSSTableRow::get_row_objs(int64_t& obj_count)
    {
      obj_count = obj_count_;
      return objs_;
    }

    int ObSSTableRow::clear()
    {
      int ret = OB_SUCCESS;

      row_key_len_ = 0;
      obj_count_ = 0;
      ret = string_buf_.reset();

      return ret;
    }

    int ObSSTableRow::ensure_key_buf_space(const int64_t size)
    {
      int ret         = OB_SUCCESS;
      char* new_buf   = NULL;
      int64_t key_len = size > row_key_buf_size_ 
                        ? size : row_key_buf_size_;

      if (size <= 0)
      {
        TBSYS_LOG(WARN, "invalid key length, size=%ld", size);
        ret = OB_ERROR;
      }
      else if (NULL == row_key_buf_ 
               || (NULL != row_key_buf_ && size > row_key_buf_size_))
      {
        new_buf = static_cast<char*>(ob_malloc(key_len, ObModIds::OB_SSTABLE_WRITER));
        if (NULL == new_buf)
        {
          TBSYS_LOG(WARN, "Problem allocating memory for row key buffer");
          ret = OB_ERROR;
        }
        else
        {
          if (NULL != row_key_buf_)
          {
            ob_free(row_key_buf_);
            row_key_buf_ = NULL;
          }
          row_key_buf_size_ =  static_cast<int32_t>(key_len);
          row_key_buf_ = new_buf;
          memset(row_key_buf_, 0, row_key_buf_size_);
        }
      }

      return ret;
    }

    int ObSSTableRow::ensure_objs_buf_space(const int64_t size)
    {
      int ret               = OB_SUCCESS;
      char *new_buf         = NULL;
      int64_t reamin_size   = objs_buf_size_ - OBJ_SIZE * obj_count_;
      int64_t objs_buf_len  = 0;

      if (size <= 0)
      {
        TBSYS_LOG(WARN, "invalid obj length, size=%ld", size);
        ret = OB_ERROR;
      }
      else if (NULL == objs_ || (NULL != objs_ && size > reamin_size))
      {
        objs_buf_len = size > reamin_size 
                       ? (objs_buf_size_ * 2) : objs_buf_size_;
        if (objs_buf_len - OBJ_SIZE * obj_count_ < size)
        {
          objs_buf_len = OBJ_SIZE * obj_count_ + size * 2;
        }
        new_buf = static_cast<char*>(ob_malloc(objs_buf_len));
        if (NULL == new_buf)
        {
          TBSYS_LOG(ERROR, "Problem allocating memory for key buffer");
          ret = OB_ERROR;
        }
        else
        {
          memset(new_buf, 0, objs_buf_len);
          if (NULL != objs_)
          {
            memcpy(new_buf, objs_, OBJ_SIZE * obj_count_);
            ob_free(objs_);
            objs_ = NULL;
          }
          objs_buf_size_ =  static_cast<int32_t>(objs_buf_len);
          objs_ = reinterpret_cast<ObObj*>(new_buf);
        }
      }

      return ret;
    }

    int ObSSTableRow::set_row_key(const ObString& key)
    {
      int ret = OB_SUCCESS;
      int64_t key_len = key.length();

      if (key_len <= 0 || NULL == key.ptr())
      {
        TBSYS_LOG(WARN, "invalid row key, ken_len=%ld ptr=%p", 
                  key_len, key.ptr());
        ret = OB_ERROR;
      }
      else if (OB_SUCCESS == (ret = ensure_key_buf_space(key_len)))
      {
        memcpy(row_key_buf_, key.ptr(), key_len);
        row_key_len_ =  static_cast<int32_t>(key_len);
      }

      return ret;
    }

    int ObSSTableRow::add_obj(const ObObj& obj)
    {
      int ret = OB_SUCCESS;
      ObObj stored_obj;

      if (obj_count_ < OB_MAX_COLUMN_NUMBER)
      {
        ret = ensure_objs_buf_space(OBJ_SIZE);
        if (OB_SUCCESS == ret)
        {
          //have enough space to store this object
          ret = string_buf_.write_obj(obj, &stored_obj);
          if (OB_SUCCESS == ret)
          {
            objs_[obj_count_++] = stored_obj;
          }
        }
      }
      else
      {
        TBSYS_LOG(WARN, "can't add obj anymore, max_count=%ld, obj_count=%d", 
                  OB_MAX_COLUMN_NUMBER, obj_count_);
        ret = OB_ERROR;
      }

      return ret;
    }

    int ObSSTableRow::add_obj(const ObObj& obj, const uint64_t column_id)
    {
      int ret = OB_SUCCESS;
      ObObj stored_obj;
      ObObj id_obj;

      if (column_id == OB_INVALID_ID)
      {
        TBSYS_LOG(WARN, "invalid column id, column_id=%lu", column_id);
        ret = OB_ERROR;
      }

      if (OB_SUCCESS == ret)
      {
        id_obj.set_int(column_id);
        ret = ensure_objs_buf_space(OBJ_SIZE * 2);
        if (OB_SUCCESS == ret)
        {
          //have enough space to store this object
          ret = string_buf_.write_obj(obj, &stored_obj);
          if (OB_SUCCESS == ret)
          {
            objs_[obj_count_++] = id_obj;
            objs_[obj_count_++] = stored_obj;
          }
        }         
      }

      return ret;
    }

    int ObSSTableRow::check_schema(const ObSSTableSchema& schema) const
    {
      int ret              = OB_SUCCESS;
      int64_t column_count = 0;
      const ObSSTableSchemaColumnDef* column_def = schema.get_group_schema(table_id_, column_group_id_,
                                                                           column_count);
      ObObjType obj_type   = ObNullType;
      
      if (column_count <= 0 || obj_count_ <= 0 
          || NULL == column_def || column_count != obj_count_)
      {
        TBSYS_LOG(WARN, "invalid column count in scheam or obj count in row,"
                  "column_count=%ld, obj_count=%d, column_def=%p",
                  column_count, obj_count_, column_def);
        ret = OB_ERROR;
      }

      if (OB_SUCCESS == ret)
      {
        for (int64_t i = 0; i < obj_count_; ++i)
        {
          obj_type = objs_[i].get_type();
          if (NULL == column_def + i)
          {
            TBSYS_LOG(WARN, "problem get column def, column_def=%p", column_def);
            ret = OB_ERROR;
            break;
          }
          /**
           * ingore ObNullType, because if add new column, we will 
           * initialize the column obj with ObNullType, and the schema 
           * also store the actual column type 
           */
          else if (ObNullType != obj_type 
                   && obj_type != column_def[i].column_value_type_)
          {
            TBSYS_LOG(WARN, "obj type is inconsistent with column type,"
                            "obj_type=%d, column_type=%d", obj_type,
                      column_def[i].column_value_type_);
            ret = OB_ERROR;
            break;
          }
        }
      }

      return ret;
    }

    DEFINE_SERIALIZE(ObSSTableRow)
    {
      int ret                 = OB_SUCCESS;
      int16_t row_key_len     =  static_cast<int16_t>(row_key_len_);
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
        ret = encode_i16(buf, buf_len, pos, row_key_len);
        if (OB_SUCCESS == ret)
        {
          memcpy(buf + pos, row_key_buf_, row_key_len);
          pos += row_key_len;
          for (int64_t i = 0; ret == OB_SUCCESS && i < obj_count_; ++i)
          {
            ret = objs_[i].serialize(buf, buf_len, pos);
            if (OB_SUCCESS != ret)
            {
              TBSYS_LOG(WARN, "failed to serialzie obj, current obj index=%ld,"
                              "obj_count=%d", i, obj_count_);
            }
          }
        }
        else
        {
          TBSYS_LOG(WARN, "failed to serialize key length");
        }
      }

      return ret;
    }

    DEFINE_DESERIALIZE(ObSSTableRow)
    {
      int ret                 = OB_SUCCESS;
      int16_t key_len         = 0;
      int64_t i               = 0;

      if (NULL == buf || data_len <= 0 || pos > data_len) 
      {
        TBSYS_LOG(WARN, "invalid param, buf=%p, data_len=%ld, pos=%ld",
                  buf, data_len, pos);
        ret = OB_ERROR;
      }
  
      if (OB_SUCCESS == ret && obj_count_ > 0)
      {
        ret = decode_i16(buf, data_len, pos, &key_len);
        if (OB_SUCCESS == ret && key_len > 0
            && OB_SUCCESS == (ret = ensure_key_buf_space(key_len)))
        {
          memcpy(row_key_buf_, buf + pos, key_len);
          pos += key_len;
          row_key_len_ = key_len;
        }
        else
        {
          TBSYS_LOG(WARN, "failed to deserialize row key, key_len=%d", key_len);
        }

        for (i = 0; OB_SUCCESS == ret && i < obj_count_; ++i)
        {
          ret = ensure_objs_buf_space(OBJ_SIZE);
          if (OB_SUCCESS == ret)
          {
            ret = objs_[i].deserialize(buf, data_len, pos);
            if (OB_SUCCESS != ret)
            {
              TBSYS_LOG(WARN, "failed to deserialzie obj, current obj index=%ld,"
                              "obj_count=%d", i, obj_count_);
            }
          }
        }

        if (i != obj_count_)
        {
          TBSYS_LOG(WARN, "expect deserialize obj_count=%d, but only "
                          "get obj_count=%ld", obj_count_, i);
          ret = OB_ERROR;
        }
      }

      return ret;
    }

    DEFINE_GET_SERIALIZE_SIZE(ObSSTableRow)
    {
      int64_t total_size = 0;

      //add row key length
      total_size += 2 + row_key_len_; //key length occupies 2 bytes
      for (int64_t i = 0; i < obj_count_; ++i)
      {
        total_size += objs_[i].get_serialize_size();
      }

      return total_size;    
    }
  } // end namespace sstable
} // end namespace oceanbase
