/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_sstable_row.h for persistent ssatable single row. 
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */
#ifndef OCEANBASE_SSTABLE_OB_SSTABLE_ROW_H_
#define OCEANBASE_SSTABLE_OB_SSTABLE_ROW_H_

#include "common/ob_define.h"
#include "common/ob_string.h"
#include "common/ob_object.h"
#include "common/ob_malloc.h"
#include "common/ob_string_buf.h"
#include "ob_sstable_schema.h"

namespace oceanbase 
{
  namespace sstable 
  {
    /**
     * this class is used to serialize or deserialize one row.
     * 
     * WARNING: an ObSSTableRow instance only can do serialization
     * or deserialization, don't use one ObSSTableRow instance to do
     * both serialization and desrialization at the same time.
     */
    class ObSSTableRow
    {
      static const int64_t DEFAULT_KEY_BUF_SIZE = 64;
      static const int64_t OBJ_SIZE = sizeof(common::ObObj);
      static const int64_t DEFAULT_OBJS_BUF_SIZE 
        = OBJ_SIZE * common::OB_MAX_COLUMN_NUMBER; 

    public:
      ObSSTableRow();
      ~ObSSTableRow();

      /**
       * return how many objects(columns) in the row, maximum count is 
       * common::MAX_COLUMN_NUMBER, if no object, return 0
       * 
       * @return int64_t object count
       */
      const int64_t get_obj_count() const;

      /**
       * If user uses this class to deserialize one row, first you 
       * need set the count of objects in the row based on the count 
       * of columns in schema 
       * 
       * @param obj_count columns count in schema 
       * @param sparse_format whether row is in sparse format 
       * 
       * @return int if success,return OB_SUCCESS, else return 
       *         OB_ERROR
       */
      int set_obj_count(const int64_t obj_count, 
                        const bool sparse_format = false);

      /**
       * return table id of this row
       *
       * @return uint64_t table id of this row
       */
      const uint64_t get_table_id() const;

      /**
       * set table id of this row
       *
       * @return int if success, return OB_SUCCESS, else return OB_ERROR
       */
      int set_table_id(const uint64_t table_id);

      /**
       * return column group id of this row
       *
       * @return uint64_t column group id of this row
       */
      const uint64_t get_column_group_id() const;

      /**
       * set column group id of this row
       *
       * @return int if success, return OB_SUCCESS, else return OB_ERROR
       */
      int set_column_group_id(const uint64_t column_group_id);

      /**
       * get row key, if the row key isn't set, return a null row key.
       * 
       * @return const common::ObString& 
       */
      const common::ObString get_row_key() const;

      /**
       * get one object by index
       * 
       * @param index the index of the object in the inner object 
       *              array
       * 
       * @return const ObObj* if object exists, return it, else return 
       *         NULL.
       */
      const common::ObObj* get_obj(const int32_t index) const;

      /**
       * get objs array of row 
       * 
       * @param obj_count obj count in row
       * 
       * @return const common::ObObj* obj array in row
       */
      const common::ObObj* get_row_objs(int64_t& obj_count);

      /**
       * clear all the members 
       *  
       * @return int if success,return OB_SUCCESS, else return 
       *         OB_INVALID_ARGUMENT 
       */
      int clear();

      /**
       * set row key of the row
       * 
       * @param key row key
       * 
       * @return int if success,return OB_SUCCESS, else return 
       *         OB_ERROR
       */
      int set_row_key(const common::ObString& key);

      /**
       * add one object into the row, increase the object count. user 
       * must add the objects as the order of columns in schema 
       * 
       * @param obj the object to add
       * 
       * @return int if success,return OB_SUCCESS, else return 
       *         OB_ERROR
       */
      int add_obj(const common::ObObj& obj);

      /**
       * add one object into the row, increase the object count, it is 
       * used for sstable writer to store sparse format 
       * 
       * @param obj the object to add
       * @param column_id column id of this obj
       * 
       * @return int if success,return OB_SUCCESS, else return 
       *         OB_ERROR
       */
      int add_obj(const common::ObObj& obj, const uint64_t column_id);

      /**
       * check if the order and type of object are consistent with 
       * schema. 
       * 
       * @param schema the schema stores the order of objects and 
       *               column type information
       * 
       * @return int if success,return OB_SUCCESS, else return 
       *         OB_ERROR
       */
      int check_schema(const ObSSTableSchema& schema) const;

      NEED_SERIALIZE_AND_DESERIALIZE;

    private:
      /**
       * allocate memory and ensure the space is enough to store the 
       * row key 
       * 
       * @param size the row key size to need
       * 
       * @return int if success,return OB_SUCCESS, else return 
       *         OB_ERROR
       */
      int ensure_key_buf_space(const int64_t size);

      /**
       * allocate memory and ensure the space is enough to store the 
       * objs 
       * 
       * @param size the obj size to need
       * 
       * @return int if success,return OB_SUCCESS, else return 
       *         OB_ERROR
       */
      int ensure_objs_buf_space(const int64_t size);

    private:
      DISALLOW_COPY_AND_ASSIGN(ObSSTableRow);
      
      uint64_t table_id_;         //table id of this row
      uint64_t column_group_id_;  //group id of this row
      char* row_key_buf_;         //buffer to store row key char stream
      int32_t row_key_len_;       //length of row key
      int32_t row_key_buf_size_;  //size of row key buffer

      /* the order of object must be the same as schema */
      common::ObObj* objs_;
      int32_t objs_buf_size_;     //objs buffer size
      int32_t obj_count_;         //count of objects in the objs array
      common::ObStringBuf string_buf_;  //store the varchar of ObObj
    };
  } // namespace oceanbase::sstable
} // namespace Oceanbase

#endif // OCEANBASE_SSTABLE_OB_SSTABLE_ROW_H_
