/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_sstable_schema.h for persistent schema. 
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */
#ifndef OCEANBASE_SSTABLE_OB_SSTABLE_SCHEMA_H_
#define OCEANBASE_SSTABLE_OB_SSTABLE_SCHEMA_H_

#include "common/ob_define.h"
#include "common/ob_malloc.h"
#include "common/hash/ob_hashutils.h"
#include "common/hash/ob_hashmap.h"

namespace oceanbase 
{
  namespace sstable 
  {
    struct ObSSTableSchemaHeader 
    {
      int16_t column_count_;        //column count compatible for V0.1.0
      int16_t reserved16_;          //must be 0
      int32_t total_column_count_;  //column count of all tables

      NEED_SERIALIZE_AND_DESERIALIZE;
    };

    struct ObSSTableSchemaColumnDef
    {
      int16_t  reserved_;           //reserved,must be 0 for V0.2.0
      uint16_t column_group_id_;    //column gropr id for V0.2.0
      uint32_t column_name_id_;     //column name id
      int32_t  column_value_type_;  //type of column value ObObj
      uint32_t table_id_;           //table id  for V0.2.0
      
      //member used for query
      int16_t table_group_next_;            //next column group offset base on start of column def array
      int16_t group_column_next_;           //next column def offset which have same table_id && column_name_id 
      mutable int32_t offset_in_group_;     //offset in column group
      NEED_SERIALIZE_AND_DESERIALIZE;
    };
    
    struct CombineIdKey
    {
      int64_t hash()const;
      bool operator==(const CombineIdKey& key)const;
      uint64_t table_id_;
      uint64_t sec_id_;
    };

    struct ValueSet
    {
      int16_t head_offset_;
      int16_t tail_offset_;
      int32_t  size_;
      ValueSet()
      {
        head_offset_ = -1;
        tail_offset_ = -1;
        size_ = 0;
      }
    };
    
    struct ObSSTableSchemaColumnDefCompare
    {
      bool operator()(const ObSSTableSchemaColumnDef& lhs, const ObSSTableSchemaColumnDef& rhs)
      {
        bool ret = true;
        if ( lhs.table_id_ > rhs.table_id_ 
             || (lhs.table_id_ == rhs.table_id_ && lhs.column_group_id_ > rhs.column_group_id_)
             || (lhs.table_id_ == rhs.table_id_ && lhs.column_group_id_ == rhs.column_group_id_
                 && lhs.column_name_id_ >= rhs.column_name_id_) )
        {
          ret = false;
        }
        return ret;
      }
    };

    class ObSSTableSchema
    {
    public:
      ObSSTableSchema();
      ~ObSSTableSchema();
      
      /**
       * get column count in the sstable schema 
       * 
       * @return int64_t return column count
       */
      const int64_t get_column_count() const;

      
      /**
       * get one column def by column index
       * 
       * @param index the column def index to get
       * 
       * @return ObSSTableSchemaColumnDef* if find, return the pointer 
       *         of the column, else return NULL
       */
      const ObSSTableSchemaColumnDef* get_column_def(const int32_t index) const;
      
      /**
       * Get table schema
       *
       * @param[in]      table_id   which table schema to get
       * @param[out]     group_ids  all group id of table
       * @param[in/out]  size       group id array size
       *
       * @return int if success return OB_SUCCESS, else
       *         return OB_ERROR
       */
      int get_table_column_groups_id(const uint64_t table_id, uint64_t* group_ids, int64_t& size) const;
      
      /**
       * get group column def array  by table id && group id
       *
       * @param[in]  table_id         which table schema to get 
       * @param[in]  column_group_id  which group schema to get
       * @param[out] size             column def array size
       *
       * @return ObSSTableSchemaColumnDef* if find, return a pointer point to the first 
       *                                   column def in the array, else return NULL
       */
      const ObSSTableSchemaColumnDef* get_group_schema(const uint64_t table_id,
                                                       const uint64_t column_group_id, int64_t& size) const;

      /**
       * Find column group id if it exists in sstable schema, if find 
       * more than one column group id return the first one
       *
       * @param[in]  table  id         the column group belongs to
       * @param[in]  column id         column id to find
       * @param[out] index array       array to store column group id
       * @param[in/out] size           [in] array size [out]return size
       *
       * @return int   if find, return OB_SUCCESS, else return OB_ERROR 
       */
      int get_column_groups_id(const uint64_t table_id, const uint64_t column_id,
                               uint64_t *array, int64_t &size) const;


      /**
       * Get offset in table schema
       *
       * @param table_id    which table schema to find in
       * @param column_id   column id to find
       *
       * @return int64_t    if found, return offset in table schema, else return -1
       */
      const int64_t find_offset_first_column_group_schema(const uint64_t table_id,
                                                          const uint64_t column_id,
                                                          uint64_t & column_group_id) const;

      /**
       * Get offset in column group schema
       *
       * @param table_id          which table column group belongs to
       * @param column_group_id   which column group schema to find in
       * @param column_id         column id to find
       *
       * @return int32_t          if found, return offset in column group schema, else return -1
       */
      const int64_t find_offset_column_group_schema(const uint64_t table_id, const uint64_t column_group_id,
                const uint64_t column_id) const;
      
      /**
       * weather column exist in table
       * 
       * @param table_id   which table column belongs to
       * @param column_id  column id to find
       *
       * @return bool      exist return true, else return false 
       */
      bool is_column_exist(const uint64_t table_id, const uint64_t column_id) const;

      /**
       * weather table exist in schema
       * 
       * @param table_id table id to check
       * 
       * @return bool exist return true, else return false 
       */
      bool is_table_exist(const uint64_t table_id) const;

      /**
       * WARING This function only can be used by sstable schema V0.2.0
       * add one column define into schema, the column define must be 
       * in ascending order by column id 
       * 
       * @param column_def column define, includes column id and 
       *                   column type
       * 
       * @return int if success, return OB_SUCCESS, else return 
       *         OB_ERROR
       */
      int add_column_def(const ObSSTableSchemaColumnDef& column_def);

      /**
       * reset scheam in order to reuse it
       */
      void reset();

      NEED_SERIALIZE_AND_DESERIALIZE;
      
    private:
      /**
       * create hashmaps
       *
       * @return int if success, return OB_SUCCESS, else return
       *         OB_ERROR
       */
      int create_hashmaps();

      /**
       * update hashmaps with column def
       *
       * @param column_def 
       *
       * @return int if success, return OB_SUCCESS, else return
       *         OB_ERROR
       */
      int update_hashmaps(const ObSSTableSchemaColumnDef& column_def);
      
      /**
       * update table schema hashmap with column def
       *
       * @param column_def 
       *
       * @return int if success, return OB_SUCCESS, else return
       *         OB_ERROR
       */
      int update_table_schema_hashmap(const ObSSTableSchemaColumnDef& column_def);
      
      /**
       * update group schema hashmap with column def
       *
       * @param column_def 
       *
       * @return int if success, return OB_SUCCESS, else return
       *         OB_ERROR
       */
      int update_group_schema_hashmap(const ObSSTableSchemaColumnDef& column_def);

      /**
       * update group index hashmap with column def
       *
       * @param column_def 
       *
       * @return int if success, return OB_SUCCESS, else return
       *         OB_ERROR
       */
      int update_group_index_hashmap(const ObSSTableSchemaColumnDef& column_def);
#ifdef COMPATIBLE
      const int64_t find_column_id(const uint64_t column_id)const;
#endif
    private:
      DISALLOW_COPY_AND_ASSIGN(ObSSTableSchema);
      
    private:
      static const int64_t DEFAULT_COLUMN_DEF_SIZE = common::OB_MAX_TABLE_NUMBER * common::OB_MAX_COLUMN_NUMBER;
      static const int64_t TABLE_ID_MASK           = 0xFFFF;
      static const int64_t COLUMN_GROUP_ID_MASK    = 0xFF;
      static const int64_t COLUMN_NAME_ID_MASK     = 0xFFFF;
      
    private:
      ObSSTableSchemaColumnDef column_def_array_[common::OB_MAX_COLUMN_NUMBER];   //column def buf for single table
      ObSSTableSchemaHeader schema_header_;
      ObSSTableSchemaColumnDef *column_def_;                                      //column def pointer
      bool hash_init_;
      int64_t curr_group_head_;
      common::hash::ObHashMap<uint64_t, ValueSet,
        common::hash::NoPthreadDefendMode> table_schema_hashmap_;

      common::hash::ObHashMap<CombineIdKey, ValueSet,
        common::hash::NoPthreadDefendMode> group_schema_hashmap_;
      
      common::hash::ObHashMap<CombineIdKey, ValueSet,
        common::hash::NoPthreadDefendMode> group_index_hashmap_;
#ifdef COMPATIBLE
      mutable int32_t version_;
#endif
    };
  } // namespace oceanbase::sstable
} // namespace Oceanbase

#endif // OCEANBASE_SSTABLE_OB_SSTABLE_SCHEMA_H_
