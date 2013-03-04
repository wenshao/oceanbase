/*
 * (C) 2007-2010 TaoBao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * ob_meta_table.cpp is for what ...
 *
 * Version: $id$
 *
 * Authors:
 *   MaoQi maoqi@taobao.com
 *
 */

#include "ob_meta_table.h"
#include "ob_client_helper.h"
#include "ob_scan_param.h"
#include "ob_tsi_factory.h"
#include "ob_string.h"
#include "ob_schema.h"
#include "ob_common_param.h"
#include "ob_mutator.h"
#include "utility.h"

namespace oceanbase
{
  namespace common
  {
    const char* USER_TABLES_NAME = "user_tables";
    const char* USER_TAB_COLUMNS_NAME = "user_tab_columns";
    const char* USER_TAB_JOININFO_NAME = "user_tab_joininfo";
    const char* VERSION_TABLE_NAME = "dba_version";

    const char* CREATE_TABLE_SQL = "create table";
    const char* DROP_TABLE_SQL = "drop table";
    const char* ALTER_TABLE_SQL = "alter table";
    const char* ADD_COLUMN_SQL = "add column";
    const char* DROP_COLUMN_SQL = "drop column";

    const char* SCHEMA_VERSION_KEY = "schema";
    const char* MAX_TABLE_ID_KEY = "max_table_id";

    const uint64_t OB_TABLE_ID_INTERVAL = 1;
  
    static const ObMetaTable::TableScanHelper USER_TABLES_COLUMNS[] = 
    {
      { 2, "gmt_created" },
      { 3, "gmt_modified" },
      { 16,"version"},
      { 17,"table_id" },
      { 18,"owner" },
      { 19,"table_name" },
      { 20,"table_type" },
      { 21,"rowkey_len" },
      { 22,"rowkey_is_fixed_len" },
      { 23,"rowkey_split" },
      { 24,"compressor" },
      { 25,"max_column_id" },
    };


    enum USER_TABLES
    {
      T_VERSION              = 2,
      T_TABLE_ID             = 3,
      T_OWNER                = 4,
      T_TABLE_NAME           = 5,
      T_TABLE_TYPE           = 6,
      T_ROWKEY_LEN           = 7,
      T_ROWKEY_IS_FIXED_LEN  = 8,
      T_ROWKEY_SPLIT         = 9,
      T_COMPRESSOR           = 10,
      T_MAX_COLUMN_ID        = 11,
      T_MAX_COLUMN_INDEX     = 12,
    };

    static const ObMetaTable::TableScanHelper USER_TAB_COL_COLUMNS [] = 
    {
      { 2,"gmt_created" },
      { 3,"gmt_modified" },
      { 16,"version"},
      { 17,"table_id" },
      { 18,"column_group_id"},
      { 19,"column_id" },
      { 20,"column_name" },
      { 21,"data_type" },
      { 22,"data_length" },
      { 23,"data_precision" },
      { 24,"data_scale" },
      { 25,"nullable" },
      { 26,"rowkey_seq" },
    };

    enum USER_TAB_COLUMNS
    {
      C_VERSION              = 2,
      C_TABLE_ID             = 3,
      C_COLUMN_GROUP_ID      = 4,
      C_COLUMN_ID            = 5,
      C_COLUMN_NAME          = 6,
      C_DATA_TYPE            = 7,
      C_DATA_LENGTH          = 8,
      C_DATA_PRECISION       = 9,
      C_DATA_SCALE           = 10,
      C_NULLABLE             = 11,
      C_ROWKEY_SEQ           = 12,
      C_MAX_COLUMN_INDEX     = 13,
    };

    static const ObMetaTable::TableScanHelper USER_TAB_JOININFO_COLUMNS[] = 
    {
      { 2,"gmt_created" },
      { 3,"gmt_modified" },
      { 16,"version"},
      { 17,"left_table_id" },
      { 18,"left_column_id" },
      { 19,"join_key_column_id"},
      { 20,"right_table_id" },
      { 21,"right_column_id" },
      { 22,"join_key_seq" },
      { 23,"left_column_len" },
    };

    enum USER_TAB_JOININFO
    {
      J_VERSION              = 2,
      J_LEFT_TABLE_ID        = 3,
      J_LEFT_COLUMN_ID       = 4,
      J_JOIN_KEY_COLUMN_ID   = 5,
      J_RIGHT_TABLE_ID       = 6,
      J_RIGHT_COLUMN_ID      = 7,
      J_JOIN_KEY_SEQ         = 8,
      J_LEFT_COLUMN_LEN      = 9,
      J_MAX_COLUMN_INDEX     = 10,
    };

    static const ObMetaTable::TableScanHelper VERSION_TABLE_COLUMNS[] = 
    {
      { 2,"gmt_created" },
      { 3,"gmt_modified" },
      { 16,"name"},
      { 17,"version"},
    };

    enum VERSION_TABLE 
    {
      V_VALUE = 3,
    };


    ObMetaTable::ObMetaTable() : inited_(false),client_helper_(NULL),
                                 startkey_buffer_(common::OB_MAX_ROW_KEY_LENGTH),
                                 endkey_buffer_(common::OB_MAX_ROW_KEY_LENGTH)
    {}

    int ObMetaTable::init(ObClientHelper* client_helper) 
    {
      int ret = OB_ERROR;

      if (!inited_ && client_helper != NULL)
      {
        client_helper_ = client_helper;
        inited_ = true;
        ret = OB_SUCCESS;
      }
      return ret;
    }

    int ObMetaTable::get_schema(int64_t version,ObSchemaManagerV2& schema_manager)
    { 
      int ret = OB_SUCCESS;
      UNUSED(version);
      //1. construct a scan param ,(min,max) of user_tables;
      ObScanParam *param = GET_TSI(ObScanParam);
      ObRange range;
      param->reset();
      
      if (0 == version)
      {
        if ((version = get_schema_version()) <= 0)
        {
          ret = OB_ERROR;
          TBSYS_LOG(INFO,"get schema version failed");
        }
      }
      
      schema_manager.set_version(version);
      schema_manager.set_max_table_id(get_max_table_id());

      build_range(range,version,USER_TABLES_ROWKEY_LEN);
      range.border_flag_.unset_inclusive_start();
      range.border_flag_.set_inclusive_end();

      ObVersionRange version_range;
      version_range.border_flag_.set_min_value();
      version_range.border_flag_.set_max_value();

      param->set_version_range(version_range);
      ObString column;
      ObString table_name;
      table_name.assign_ptr(const_cast<char*>(USER_TABLES_NAME),strlen(USER_TABLES_NAME));
      param->set(OB_INVALID_ID,table_name,range);
      for(uint32_t i = 0; i < sizeof(USER_TABLES_COLUMNS) / sizeof(USER_TABLES_COLUMNS[0]); ++i)
      {
        column.assign_ptr(const_cast<char*>(USER_TABLES_COLUMNS[i].column_name),strlen(USER_TABLES_COLUMNS[i].column_name));
        param->add_column(column);
      }
      // send the scan request to a random mergeserver
      ObScanner *scanner = GET_TSI(ObScanner);
      scanner->reset();

      if ( OB_SUCCESS == ret && (ret = client_helper_->scan(*param,*scanner)) != OB_SUCCESS)
      {
        TBSYS_LOG(WARN,"scan from ms failed,ret = %d",ret);
      }
      else
      {
        TBSYS_LOG(INFO,"got %ld cells",scanner->get_size());
      }

      ObCellInfo* cell_array = reinterpret_cast<ObCellInfo*>(ob_malloc(sizeof(*cell_array) * OB_MAX_COLUMN_NUMBER));
      bool need_next_row = true;
      int32_t size = OB_MAX_COLUMN_NUMBER;

      while(OB_SUCCESS == ret && (OB_SUCCESS == (ret = get_row(*scanner,cell_array,size,need_next_row))))
      {
        ObTableSchema table_schema;
       
        ObString varchar_val;
        int64_t val = 0;

        if (size < T_MAX_COLUMN_INDEX)
        {
          //ret = OB_ERROR;
          //TBSYS_LOG(ERROR,"get data error,size : %d",size);
          break;
        }
        else 
        {
          cell_array[T_TABLE_ID].value_.get_int(val);
          table_schema.set_table_id(val);

          cell_array[T_TABLE_NAME].value_.get_varchar(varchar_val);
          table_schema.set_table_name(varchar_val);

          cell_array[T_TABLE_TYPE].value_.get_int(val);
          table_schema.set_table_type(static_cast<ObTableSchema::TableType>(val));

          cell_array[T_ROWKEY_LEN].value_.get_int(val);
          table_schema.set_rowkey_max_length(val);

          cell_array[T_ROWKEY_IS_FIXED_LEN].value_.get_int(val);
          table_schema.set_rowkey_fixed_len(val > 0 ? true : false);

          cell_array[T_ROWKEY_SPLIT].value_.get_int(val);
          table_schema.set_split_pos(val);

          cell_array[T_COMPRESSOR].value_.get_varchar(varchar_val);
          table_schema.set_compressor_name(varchar_val);

          cell_array[T_MAX_COLUMN_ID].value_.get_int(val);
          table_schema.set_max_column_id(val);
          
          schema_manager.add_table(table_schema);
        }
      }
        
      if (OB_SUCCESS == ret && 
          (ret = fill_table_columns(schema_manager)) != OB_SUCCESS)
      {
        TBSYS_LOG(WARN,"fill_table_columns failed,ret = %d",ret);
      }

      if (OB_SUCCESS == ret &&
          (ret = fill_table_joininfo(schema_manager)) != OB_SUCCESS)
      {
        TBSYS_LOG(WARN,"fill table joininfo failed,ret = %d",ret);
      }
      
      if (cell_array != NULL)
      {
        ob_free(cell_array);
        cell_array = NULL;
      }

      return ret;
    }
    
    uint64_t ObMetaTable::get_max_table_id(void)
    {
      int64_t v = -1;
      if ( get_version(MAX_TABLE_ID_KEY,v) != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR,"cann't get value according to (%s)",MAX_TABLE_ID_KEY);
      }
      return static_cast<uint64_t>(v);
    }

    int64_t ObMetaTable::get_schema_version(void)
    {
      int64_t version = -1;
      if ( get_version(SCHEMA_VERSION_KEY,version) != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR,"cann't get version");
      }
      return version;
    }

    int ObMetaTable::get_version(const char* key,int64_t& version)
    {
      ObGetParam get_param;
      ObCellInfo cell;
      ObString version_table;
      ObString rowkey;
      ObVersionRange version_range;
      int ret = OB_SUCCESS;
      ObScanner *scanner = GET_TSI(ObScanner);
      scanner->reset();
      version = -1;

      if (NULL == key)
      {
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        version_range.border_flag_.set_min_value();
        version_range.border_flag_.set_max_value();
        get_param.set_version_range(version_range);
        version_table.assign_ptr(const_cast<char*>(VERSION_TABLE_NAME),strlen(VERSION_TABLE_NAME));
        rowkey.assign_ptr(const_cast<char*>(key),strlen(key));

        cell.table_name_ = version_table;
        cell.row_key_ = rowkey;
        cell.column_name_.assign_ptr(const_cast<char *>(VERSION_TABLE_COLUMNS[V_VALUE].column_name),
            strlen(VERSION_TABLE_COLUMNS[V_VALUE].column_name));
        get_param.add_cell(cell);

        if ((ret = client_helper_->get(get_param,*scanner)) != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR,"get schema version failed: ret = %d",ret);
        }
        else
        {
          ObString value;
          ObCellInfo *cell_out = NULL;

          if ( (ret = scanner->next_cell()) != OB_SUCCESS ||
              (ret = scanner->get_cell(&cell_out) != OB_SUCCESS) ||
              NULL == cell_out )
          {
            TBSYS_LOG(ERROR,"can't get version,ret = %d");
          }
          else
          {
            if ( ((ret = cell_out->value_.get_varchar(value)) != OB_SUCCESS) ||
                ((version = strtoll(value.ptr(),NULL,10)) <= 0) )
            {
              TBSYS_LOG(ERROR,"get version failed: version = %ld",version);
              ret = OB_ERROR;
            }
            else
            {
              TBSYS_LOG(DEBUG,"get version : %ld",version);
            }
          }
        }
      }
      return ret;
    }
    
    int ObMetaTable::set_version(const char* key,int64_t version,ObMutator& mutator)
    {
      char version_buf[32];
      ObString version_table;
      int ret = OB_SUCCESS;
      ObString rowkey;
      ObString column_name;
      ObObj value;
      
      if (key != NULL)
      {
        version_table.assign_ptr(const_cast<char*>(VERSION_TABLE_NAME),strlen(VERSION_TABLE_NAME));
        rowkey.assign_ptr(const_cast<char*>(key),strlen(key));
        snprintf(version_buf,sizeof(version_buf),"%ld",version);
        value.set_varchar(ObString(0,strlen(version_buf),version_buf));
        column_name.assign_ptr(const_cast<char*>(VERSION_TABLE_COLUMNS[V_VALUE].column_name),
            strlen(VERSION_TABLE_COLUMNS[V_VALUE].column_name));
        mutator.update(version_table,rowkey,column_name,value);
      }
      else
      {
        ret = OB_INVALID_ARGUMENT;
      }
      return ret;
    }
    
    int ObMetaTable::fill_table_columns(ObSchemaManagerV2& schema_manager)
    {
      int ret = OB_SUCCESS;

      ObScanParam *param = GET_TSI(ObScanParam);
      ObRange range;
      param->reset();
      build_range(range,schema_manager.get_version(),USER_TAB_COLUMNS_ROWKEY_LEN);
      range.border_flag_.unset_inclusive_start();
      range.border_flag_.set_inclusive_end();

      ObVersionRange version_range;
      version_range.border_flag_.set_min_value();
      version_range.border_flag_.set_max_value();

      param->set_version_range(version_range);
     
      ObString table_name;
      ObString column;
      
      table_name.assign_ptr(const_cast<char*>(USER_TAB_COLUMNS_NAME),strlen(USER_TAB_COLUMNS_NAME));
      param->set(OB_INVALID_ID,table_name,range);
      for(uint32_t i = 0; i < sizeof(USER_TAB_COL_COLUMNS) / sizeof(USER_TAB_COL_COLUMNS[0]); ++i)
      {
        column.assign_ptr(const_cast<char*>(USER_TAB_COL_COLUMNS[i].column_name),
                          strlen(USER_TAB_COL_COLUMNS[i].column_name));
        param->add_column(column);
      }

      ObScanner *scanner = GET_TSI(ObScanner);
      scanner->reset();
      if ( (ret = client_helper_->scan(*param,*scanner)) != OB_SUCCESS)
      {
        TBSYS_LOG(WARN,"scan from ms failed,ret = %d",ret);
      }

      ObCellInfo* cell_array = reinterpret_cast<ObCellInfo*>(ob_malloc(sizeof(*cell_array) * OB_MAX_COLUMN_NUMBER));
      bool need_next_cell = true;
      int32_t size = OB_MAX_COLUMN_NUMBER;

      while(OB_SUCCESS == ret && 
            OB_SUCCESS == (ret = get_row(*scanner,cell_array,size,need_next_cell)))
      {
        ObColumnSchemaV2 column;
        ObString varchar_val;
        int64_t val = 0;

        if (size < C_MAX_COLUMN_INDEX)
        {
         // TBSYS_LOG(ERROR,"get data error");
         // ret = OB_ERROR;
         break;
        }
        else
        {
          cell_array[C_TABLE_ID].value_.get_int(val);
          column.set_table_id(val);

          cell_array[C_COLUMN_GROUP_ID].value_.get_int(val);
          column.set_column_group_id(val);

          cell_array[C_COLUMN_ID].value_.get_int(val);
          column.set_column_id(val);

          cell_array[C_COLUMN_NAME].value_.get_varchar(varchar_val);
          column.set_column_name(varchar_val);

          cell_array[C_DATA_TYPE].value_.get_int(val);
          column.set_column_type(static_cast<ColumnType>(val));

          cell_array[C_DATA_LENGTH].value_.get_int(val);
          column.set_column_size(val);
          column.set_maintained(true);

          if ((ret = schema_manager.add_column(column)) != OB_SUCCESS)
          {
            TBSYS_LOG(ERROR,"add column to schemamanager failed,ret = %d",ret);
          }
        }
      }
      
      if (cell_array != NULL)
      {
        ob_free(cell_array);
        cell_array = NULL;
      }

      return ret;
    }
    
    int ObMetaTable::fill_table_joininfo(ObSchemaManagerV2& schema_manager)
    {
      int ret = OB_SUCCESS;

      ObScanParam *param = GET_TSI(ObScanParam);
      ObRange range;

      param->reset();

      build_range(range,schema_manager.get_version(),USER_TAB_JOININFO_ROWKEY_LEN);
    
      range.border_flag_.unset_inclusive_start();
      range.border_flag_.set_inclusive_end();

      ObVersionRange version_range;
      version_range.border_flag_.set_min_value();
      version_range.border_flag_.set_max_value();

      ObString column;
      ObString table_name;
      table_name.assign_ptr(const_cast<char*>(USER_TAB_JOININFO_NAME),strlen(USER_TAB_JOININFO_NAME));
      param->set_version_range(version_range);
      param->set(OB_INVALID_ID,table_name,range);
      for(uint32_t i = 0; i < sizeof(USER_TAB_JOININFO_COLUMNS) / sizeof(USER_TAB_JOININFO_COLUMNS[0]); ++i)
      {
        column.assign_ptr(const_cast<char*>(USER_TAB_JOININFO_COLUMNS[i].column_name),
                          strlen(USER_TAB_JOININFO_COLUMNS[i].column_name));
        param->add_column(column);
      }

      ObScanner *scanner = GET_TSI(ObScanner);
      scanner->reset();
      if ( (ret = client_helper_->scan(*param,*scanner)) != OB_SUCCESS)
      {
        TBSYS_LOG(WARN,"scan from ms failed,ret = %d",ret);
      }

      ObCellInfo* cell_array = reinterpret_cast<ObCellInfo*>(ob_malloc(sizeof(*cell_array) * OB_MAX_COLUMN_NUMBER));
      bool need_next_cell = true;
      int32_t size = OB_MAX_COLUMN_NUMBER;

      while(OB_SUCCESS == ret && 
            OB_SUCCESS == (ret = get_row(*scanner,cell_array,size,need_next_cell)))
      {
        ObColumnSchemaV2::ObJoinInfo join_info;
        ObColumnSchemaV2* columns[OB_MAX_COLUMN_GROUP_NUMBER];
        int64_t val = 0;

        uint64_t table_id = 0;
        uint64_t column_id = 0;

        if (size < J_MAX_COLUMN_INDEX)
        {
          break;
        }
        else
        {
          TBSYS_LOG(INFO,"got %d cells for joininfo",size);
          cell_array[J_LEFT_TABLE_ID].value_.get_int(val);
          table_id = val;

          cell_array[J_LEFT_COLUMN_ID].value_.get_int(val);
          column_id = val;

          cell_array[J_RIGHT_TABLE_ID].value_.get_int(val);
          join_info.join_table_ = val;

          cell_array[J_JOIN_KEY_COLUMN_ID].value_.get_int(val);
          join_info.left_column_id_ = val;

          cell_array[J_RIGHT_COLUMN_ID].value_.get_int(val);
          join_info.correlated_column_ = val;

          cell_array[J_JOIN_KEY_SEQ].value_.get_int(val);
          join_info.start_pos_ = val;

          cell_array[J_LEFT_COLUMN_LEN].value_.get_int(val);
          join_info.end_pos_ = val;

          if ((ret = schema_manager.get_column_schema(table_id,column_id,columns,size)) != OB_SUCCESS)
          {
            TBSYS_LOG(ERROR,"add column to schemamanager failed,ret = %d",ret);
          }
          else
          {
            for(int32_t i=0; i<size; ++i)
            {
              columns[i]->set_join_info(join_info.join_table_,
                                        join_info.left_column_id_,
                                        join_info.correlated_column_,
                                        join_info.start_pos_,
                                        join_info.end_pos_);
            }
          }
        }
      }

      if (cell_array != NULL)
      {
        ob_free(cell_array);
        cell_array = NULL;
      }
      return ret;
    }

    int ObMetaTable::add_table(const ObSchemaManagerV2& schema_manager,uint64_t ingore_table /*=OB_INVALID_ID*/)
    {
      int ret = OB_SUCCESS;
      const ObTableSchema *table_begin = schema_manager.table_begin();

      TBSYS_LOG(INFO,"add schema to meta table,version = %ld",schema_manager.get_version());
      if (OB_SUCCESS == ret && 
          (ret = add_table(schema_manager.get_version(),
                           schema_manager.get_max_table_id(),
                           table_begin,schema_manager.get_table_count(),ingore_table)) != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR,"add table failed,ret = %d",ret);
      }
      
      for(table_begin = schema_manager.table_begin();
          (OB_SUCCESS == ret) && (table_begin != schema_manager.table_end()); ++table_begin)
      {
        if (ingore_table == table_begin->get_table_id() )
        {
          continue;
        }

        int32_t size = 0;
        const ObColumnSchemaV2 *col = schema_manager.get_table_schema(table_begin->get_table_id(),size);
        if (OB_SUCCESS == ret && 
            (ret = add_columns(schema_manager.get_version(),col,size)) != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR,"add table failed,ret = %d",ret);
        }
      }
      return ret;
    }

    int ObMetaTable::add_table(int64_t version,
                               uint64_t max_table_id,
                               const ObTableSchema* table_begin,int64_t table_count,uint64_t ingore_table)
    {
      int ret = OB_SUCCESS;

      ObMutator* mutator = GET_TSI(ObMutator);
      mutator->reset();
      ObString rowkey;

      TBSYS_LOG(INFO,"add %ld tables of version %ld to meta_table ",table_count,version);

      startkey_buffer_.get_buffer()->reset();
      char *rowkey_buf = startkey_buffer_.get_buffer()->current();
      int64_t rowkey_buf_len = startkey_buffer_.get_buffer()->capacity();
      int64_t pos = 0;
      ObObj value;
      ObString user_tables_name;
      ObString column_name;
      user_tables_name.assign_ptr(const_cast<char*>(USER_TABLES_NAME),strlen(USER_TABLES_NAME));
      const ObTableSchema *table = table_begin;

      //write version to version table
      set_version(SCHEMA_VERSION_KEY,version,*mutator);
      //write max table id to version table
      set_version(MAX_TABLE_ID_KEY,max_table_id,*mutator);
      
      for(; table != table_begin + table_count; ++table) 
      {
        if (table->get_table_id() == ingore_table)
        {
          continue;
        }
        TBSYS_LOG(INFO,"add table %lu",table->get_table_id());
        pos = 0;
        //the rowkey of user_tables is 
        //version(int64) + table_id(int64_t)
        serialization::encode_i64(rowkey_buf,rowkey_buf_len,pos,version);
        serialization::encode_i64(rowkey_buf,rowkey_buf_len,pos,static_cast<int64_t>(table->get_table_id()));
        rowkey.assign_ptr(rowkey_buf,pos);

        //version
        value.set_int(version);
        column_name.assign_ptr(const_cast<char*>(USER_TABLES_COLUMNS[T_VERSION].column_name),
                               strlen(USER_TABLES_COLUMNS[T_VERSION].column_name));
        mutator->update(user_tables_name,rowkey,column_name,value);
        //table_id
        value.set_int(static_cast<int64_t>(table->get_table_id()));
        column_name.assign_ptr(const_cast<char*>(USER_TABLES_COLUMNS[T_TABLE_ID].column_name),
                               strlen(USER_TABLES_COLUMNS[T_TABLE_ID].column_name));
        mutator->update(user_tables_name,rowkey,column_name,value);
        
        //table_name
        value.set_varchar(ObString(0,strlen(table->get_table_name()),const_cast<char*>(table->get_table_name())));
        column_name.assign_ptr(const_cast<char*>(USER_TABLES_COLUMNS[T_TABLE_NAME].column_name),
                               strlen(USER_TABLES_COLUMNS[T_TABLE_NAME].column_name));
        mutator->update(user_tables_name,rowkey,column_name,value);

        //table_type
        value.set_int(table->get_table_type());
        column_name.assign_ptr(const_cast<char*>(USER_TABLES_COLUMNS[T_TABLE_TYPE].column_name),
                               strlen(USER_TABLES_COLUMNS[T_TABLE_TYPE].column_name));
        mutator->update(user_tables_name,rowkey,column_name,value);
        //rowkey_len
        value.set_int(table->get_rowkey_max_length());
        column_name.assign_ptr(const_cast<char*>(USER_TABLES_COLUMNS[T_ROWKEY_LEN].column_name),
                               strlen(USER_TABLES_COLUMNS[T_ROWKEY_LEN].column_name));
        mutator->update(user_tables_name,rowkey,column_name,value);
        //rowkey_is_fixed_len
        value.set_int(table->is_row_key_fixed_len() ? 1 : 0);
        column_name.assign_ptr(const_cast<char*>(USER_TABLES_COLUMNS[T_ROWKEY_IS_FIXED_LEN].column_name),
                                 strlen(USER_TABLES_COLUMNS[T_ROWKEY_IS_FIXED_LEN].column_name));
        mutator->update(user_tables_name,rowkey,column_name,value);
        //rowkey_split
        value.set_int(table->get_split_pos());
        column_name.assign_ptr(const_cast<char*>(USER_TABLES_COLUMNS[T_ROWKEY_SPLIT].column_name),
                               strlen(USER_TABLES_COLUMNS[T_ROWKEY_SPLIT].column_name));
        mutator->update(user_tables_name,rowkey,column_name,value);
        //compressor
        value.set_varchar(ObString(0,strlen(table->get_compress_func_name()),
                                    const_cast<char*>(table->get_compress_func_name())));
        column_name.assign_ptr(const_cast<char*>(USER_TABLES_COLUMNS[T_COMPRESSOR].column_name),
                               strlen(USER_TABLES_COLUMNS[T_COMPRESSOR].column_name));
        //max_column_id
        value.set_int(table->get_max_column_id());
        column_name.assign_ptr(const_cast<char*>(USER_TABLES_COLUMNS[T_MAX_COLUMN_ID].column_name),
                               strlen(USER_TABLES_COLUMNS[T_MAX_COLUMN_ID].column_name));
        mutator->update(user_tables_name,rowkey,column_name,value);
      }

      if (OB_SUCCESS == ret &&
          (ret = client_helper_->apply(*mutator)) != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR,"write table to meta table failed,ret = %d",ret);
      }

      return ret;
    }

    int ObMetaTable::add_columns(int64_t version,const ObColumnSchemaV2* col_begin,int64_t column_count)
    {
      int ret = OB_SUCCESS;

      ObMutator* mutator = GET_TSI(ObMutator);
      mutator->reset();
      ObString col_rowkey;
      ObString join_rowkey;

      startkey_buffer_.get_buffer()->reset();
      endkey_buffer_.get_buffer()->reset();
    
      char *col_rowkey_buf = startkey_buffer_.get_buffer()->current();
      char *join_rowkey_buf = endkey_buffer_.get_buffer()->current();
     
      int64_t col_rowkey_buf_len = startkey_buffer_.get_buffer()->capacity();
      int64_t join_rowkey_buf_len = endkey_buffer_.get_buffer()->capacity();
      
      int64_t col_pos = 0;
      int64_t join_pos = 0;
   
      ObString user_tab_columns_name;
      ObString user_tab_joininfo_name;
      ObString column_name;

      user_tab_columns_name.assign_ptr(const_cast<char*>(USER_TAB_COLUMNS_NAME),strlen(USER_TAB_COLUMNS_NAME));
      user_tab_joininfo_name.assign_ptr(const_cast<char*>(USER_TAB_JOININFO_NAME),strlen(USER_TAB_JOININFO_NAME));

      ObObj value;
      const ObColumnSchemaV2* col = col_begin;

      for(; col != col_begin + column_count; ++col)
      {
        col_pos = 0;
        //the rowkey of user_tab_columns is 
        //version(int64) + table_id(int64_t) + column_group_id(int64_t) + column_id (int64_t)
        serialization::encode_i64(col_rowkey_buf,col_rowkey_buf_len,col_pos,version);
        serialization::encode_i64(col_rowkey_buf,col_rowkey_buf_len,col_pos,
                                  static_cast<int64_t>(col->get_table_id()));
        serialization::encode_i64(col_rowkey_buf,col_rowkey_buf_len,col_pos,
                                  static_cast<int64_t>(col->get_column_group_id()));
        serialization::encode_i64(col_rowkey_buf,col_rowkey_buf_len,col_pos,
                                  static_cast<int64_t>(col->get_id()));
        col_rowkey.assign_ptr(col_rowkey_buf,col_pos);

        //version
        value.set_int(version);
        column_name.assign_ptr(const_cast<char*>(USER_TAB_COL_COLUMNS[C_VERSION].column_name),
                               strlen(USER_TAB_COL_COLUMNS[C_VERSION].column_name));
        mutator->update(user_tab_columns_name,col_rowkey,column_name,value);
        //table_id
        value.set_int(static_cast<int64_t>(col->get_table_id()));
        column_name.assign_ptr(const_cast<char*>(USER_TAB_COL_COLUMNS[C_TABLE_ID].column_name),
                               strlen(USER_TAB_COL_COLUMNS[C_TABLE_ID].column_name));
        mutator->update(user_tab_columns_name,col_rowkey,column_name,value);
        //column_group_id
        value.set_int(static_cast<int64_t>(col->get_column_group_id()));
        column_name.assign_ptr(const_cast<char*>(USER_TAB_COL_COLUMNS[C_COLUMN_GROUP_ID].column_name),
                               strlen(USER_TAB_COL_COLUMNS[C_COLUMN_GROUP_ID].column_name));
        mutator->update(user_tab_columns_name,col_rowkey,column_name,value);

        //column_id 
        value.set_int(col->get_id());
        column_name.assign_ptr(const_cast<char*>(USER_TAB_COL_COLUMNS[C_COLUMN_ID].column_name),
                               strlen(USER_TAB_COL_COLUMNS[C_COLUMN_ID].column_name));
        mutator->update(user_tab_columns_name,col_rowkey,column_name,value);

        //column_name
        value.set_varchar(ObString(0,strlen(col->get_name()),const_cast<char*>(col->get_name())));
        column_name.assign_ptr(const_cast<char*>(USER_TAB_COL_COLUMNS[C_COLUMN_NAME].column_name),
                               strlen(USER_TAB_COL_COLUMNS[C_COLUMN_NAME].column_name));
        mutator->update(user_tab_columns_name,col_rowkey,column_name,value);
                
        //data_type
        value.set_int(col->get_type());
        column_name.assign_ptr(const_cast<char*>(USER_TAB_COL_COLUMNS[C_DATA_TYPE].column_name),
                               strlen(USER_TAB_COL_COLUMNS[C_DATA_TYPE].column_name));
        mutator->update(user_tab_columns_name,col_rowkey,column_name,value);
        //data_length
        value.set_int(col->get_size());
        column_name.assign_ptr(const_cast<char*>(USER_TAB_COL_COLUMNS[C_DATA_LENGTH].column_name),
                               strlen(USER_TAB_COL_COLUMNS[C_DATA_LENGTH].column_name));
        mutator->update(user_tab_columns_name,col_rowkey,column_name,value);
        //data_precision
        //data_scale
        //nullable
        //rowkey_seq
        //

        if (col->get_join_info() != NULL) //have join info
        {
          TBSYS_LOG(INFO,"write join info");
          join_pos = 0;
          //the rowkey of user_tab_joininfo is 
          //version(int64_t) + table_id(int64_t) + column_id(int64_t) + column_id(int64_t)
          serialization::encode_i64(col_rowkey_buf,join_rowkey_buf_len,join_pos,version);
          serialization::encode_i64(col_rowkey_buf,join_rowkey_buf_len,join_pos,static_cast<int64_t>(col->get_table_id()));
          serialization::encode_i64(col_rowkey_buf,join_rowkey_buf_len,join_pos,static_cast<int64_t>(col->get_id()));
          serialization::encode_i64(col_rowkey_buf,join_rowkey_buf_len,join_pos,0L);
          join_rowkey.assign_ptr(join_rowkey_buf,join_pos);

          //version
          value.set_int(version);
          column_name.assign_ptr(const_cast<char*>(USER_TAB_JOININFO_COLUMNS[J_VERSION].column_name),
              strlen(USER_TAB_JOININFO_COLUMNS[J_VERSION].column_name));
          mutator->update(user_tab_joininfo_name,join_rowkey,column_name,value);
          //table_id
          value.set_int(static_cast<int64_t>(col->get_table_id()));
          column_name.assign_ptr(const_cast<char*>(USER_TAB_JOININFO_COLUMNS[J_LEFT_TABLE_ID].column_name),
              strlen(USER_TAB_JOININFO_COLUMNS[J_LEFT_TABLE_ID].column_name));
          mutator->update(user_tab_joininfo_name,join_rowkey,column_name,value);
          //left_column_id
          value.set_int(static_cast<int64_t>(col->get_id()));
          column_name.assign_ptr(const_cast<char*>(USER_TAB_JOININFO_COLUMNS[J_LEFT_COLUMN_ID].column_name),
              strlen(USER_TAB_JOININFO_COLUMNS[J_LEFT_COLUMN_ID].column_name));
          mutator->update(user_tab_joininfo_name,join_rowkey,column_name,value);
          //join_key_column_id
          value.set_int(0);
          column_name.assign_ptr(const_cast<char*>(USER_TAB_JOININFO_COLUMNS[J_JOIN_KEY_COLUMN_ID].column_name),
              strlen(USER_TAB_JOININFO_COLUMNS[J_JOIN_KEY_COLUMN_ID].column_name));
          mutator->update(user_tab_joininfo_name,join_rowkey,column_name,value);

          //right_table_id 
          value.set_int(col->get_join_info()->join_table_);
          column_name.assign_ptr(const_cast<char*>(USER_TAB_JOININFO_COLUMNS[J_RIGHT_TABLE_ID].column_name),
              strlen(USER_TAB_JOININFO_COLUMNS[J_RIGHT_TABLE_ID].column_name));
          mutator->update(user_tab_joininfo_name,join_rowkey,column_name,value);
          //right_column_id
          value.set_int(col->get_join_info()->correlated_column_);
          column_name.assign_ptr(const_cast<char*>(USER_TAB_JOININFO_COLUMNS[J_RIGHT_COLUMN_ID].column_name),
              strlen(USER_TAB_JOININFO_COLUMNS[J_RIGHT_COLUMN_ID].column_name));
          mutator->update(user_tab_joininfo_name,join_rowkey,column_name,value);
          //join_key_seq 
          value.set_int(col->get_join_info()->start_pos_);
          column_name.assign_ptr(const_cast<char*>(USER_TAB_JOININFO_COLUMNS[J_JOIN_KEY_SEQ].column_name),
              strlen(USER_TAB_JOININFO_COLUMNS[J_JOIN_KEY_SEQ].column_name));
          mutator->update(user_tab_joininfo_name,join_rowkey,column_name,value);
          //left_column_len
          value.set_int(col->get_join_info()->end_pos_);
          column_name.assign_ptr(const_cast<char*>(USER_TAB_JOININFO_COLUMNS[J_LEFT_COLUMN_LEN].column_name),
              strlen(USER_TAB_JOININFO_COLUMNS[J_LEFT_COLUMN_LEN].column_name));
          mutator->update(user_tab_joininfo_name,join_rowkey,column_name,value);
        }
      }

      if (OB_SUCCESS == ret &&
          (ret = client_helper_->apply(*mutator)) != OB_SUCCESS)
      {
        TBSYS_LOG(WARN,"add columns failed,ret = %d",ret);
      }

      return ret;
    }



    /** 
     * @brief just support create table & drop table
     *        TODO use more professional tools to parse sql
     * 
     * @param sql
     * 
     * @return 
     */
    int ObMetaTable::apply_sql(const char* sql)
    {
      int ret = OB_SUCCESS;

      if (NULL == sql || '\0' == *sql)
      {
        TBSYS_LOG(ERROR,"sql is null");
        ret = OB_INVALID_ARGUMENT;
      }

      if (0 == strncmp(sql,CREATE_TABLE_SQL,strlen(CREATE_TABLE_SQL)))
      {
        ret = add_table(sql);
      }
      else if (0 == strncmp(sql,DROP_TABLE_SQL,strlen(DROP_TABLE_SQL)))
      {
        ret = drop_table(sql);
      }
      else if (0 == strncmp(sql,ALTER_TABLE_SQL,strlen(ALTER_TABLE_SQL)))
      {
        ret = alter_table(sql);
      }
      else
      {
        ret = OB_NOT_SUPPORTED;
      }
      return ret;
    }

    
    int ObMetaTable::add_table(const char* sql)
    {
      int ret = OB_SUCCESS;

      if (NULL == sql || '\0' == *sql)
      {
        TBSYS_LOG(ERROR,"sql is null");
        ret = OB_INVALID_ARGUMENT;
      }

      const char* table = sql;
      const char *ptr = NULL;
      char line[OB_MAX_FILE_NAME_LENGTH];
      ObTableSchema table_schema;
      ColumnInfo  columns[OB_MAX_COLUMN_NUMBER];
      int32_t column_num = 0;
      int64_t max_column_id = 0;
      ObSchemaManagerV2 *schema_manager = get_schema_for_update();
      
      if (NULL == schema_manager)
      {
        ret = OB_ERROR;
      }
      
      if (OB_SUCCESS == ret)
      {
        while((OB_SUCCESS == ret) && (ptr= strchr(table,'\n')) != NULL)
        {
          if (ptr - table <= 0)
            continue;

          strncpy(line,table,ptr - table);
          line[ptr - table] = '\0';
          //str_trim(line);
          table = ++ptr;

          if (('(' == line[0] || ')' == line[0]) && 1 == strlen(line))
            continue;
          
          if (0 == strncmp(CREATE_TABLE_SQL,line,strlen(CREATE_TABLE_SQL)))
          {
            const char* table_name = strrchr(line,' ');
            table_schema.set_table_name(++table_name);
            uint64_t table_id = get_max_table_id() + OB_TABLE_ID_INTERVAL;
            if (table_id < OB_APP_MIN_TABLE_ID)
              table_id = OB_APP_MIN_TABLE_ID;
            table_schema.set_table_id(table_id);
          }
          else 
          {
            //column 
            ret = parse_column(line,columns[column_num]);
            ++column_num;
          }
        }
      }

      if (OB_SUCCESS == ret)
      {
        int64_t rowkey_len = 0;
        int64_t split_pos = 0;

        for(int32_t i=0; i<column_num; ++i)
        {
          columns[i].column.set_column_group_id(0);
          columns[i].column.set_table_id(table_schema.get_table_id());
          columns[i].column.set_maintained(true);
          if (0 == strncmp(columns[i].column.get_name(),OB_CREATE_TIME_COLUMN_NAME,
                strlen(OB_CREATE_TIME_COLUMN_NAME)))
          {
            columns[i].column.set_column_id(OB_CREATE_TIME_COLUMN_ID);
          }
          else if (0 == strncmp(columns[i].column.get_name(),OB_MODIFY_TIME_COLUMN_NAME,
                strlen(OB_MODIFY_TIME_COLUMN_NAME)))
          {
            columns[i].column.set_column_id(OB_MODIFY_TIME_COLUMN_ID);
          }
          else
          {
            max_column_id = i + OB_APP_MIN_COLUMN_ID;
            columns[i].column.set_column_id(max_column_id);
          }

          if (columns[i].is_rowkey)
          {
            rowkey_len += columns[i].column.get_size();
          }

          if (columns[i].is_split_pos)
          {
            split_pos = rowkey_len;
          }
        }


        if (OB_SUCCESS == ret)
        {
          TBSYS_LOG(INFO,"max rowkey len :%ld,split pos:%ld",rowkey_len,split_pos);
          table_schema.set_rowkey_max_length(rowkey_len);
          table_schema.set_split_pos(split_pos);
          table_schema.set_rowkey_fixed_len(false);
          table_schema.set_max_column_id(max_column_id);

          if ((ret = schema_manager->add_table(table_schema)) != OB_SUCCESS)
          {
            TBSYS_LOG(ERROR,"add table failed");
          }
        }
      }

      if (OB_SUCCESS == ret)
      {
        for(int32_t i=0; (i<column_num)&& (OB_SUCCESS == ret); ++i)
        {
          if ((ret = schema_manager->add_column(columns[i].column)) != OB_SUCCESS)
          {
            TBSYS_LOG(ERROR,"add column failed");
          }
        }
      }

      if (OB_SUCCESS == ret)
      {
        if ((ret = add_table(*schema_manager)) != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR,"add table failed : %d",ret);
        }
      }

      if (schema_manager != NULL)
      {
        ob_free(schema_manager);
        schema_manager = NULL;
      }
      return ret;
    }
    
    int ObMetaTable::drop_table(const char* sql)
    {
      int ret = OB_SUCCESS;

      if (NULL == sql || '\0' == *sql)
      {
        TBSYS_LOG(ERROR,"sql is null");
        ret = OB_INVALID_ARGUMENT;
      }

      const char *ptr = NULL;
      char table[OB_MAX_TABLE_NAME_LENGTH];
      uint64_t ingore_table = OB_INVALID_ID;
      ObSchemaManagerV2 *schema_manager = get_schema_for_update();

      if (NULL == schema_manager)
      {
        ret = OB_ERROR;
      }

      if (OB_SUCCESS == ret)
      {
        ptr = strrchr(sql,' ');
        ++ptr;
        strncpy(table,ptr,(sql + strlen(sql) - ptr));
        ptr = strchr(table,'\n');
        if (ptr != NULL)
          table[ptr - table] = '\0';
        str_trim(table);

        const ObTableSchema *t = schema_manager->get_table_schema(table);
        if (t != NULL)
        {
          ingore_table = t->get_table_id();
        }
        TBSYS_LOG(INFO,"drop this table:(%s:%lu)",table,ingore_table);
      }

      if (OB_SUCCESS == ret)
      {
        if ((ret = add_table(*schema_manager,ingore_table)) != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR,"add table failed : %d",ret);
        }
      }

      if (schema_manager != NULL)
      {
        ob_free(schema_manager);
        schema_manager = NULL;
      }
      return ret;
    }
    
    int ObMetaTable::alter_table(const char* sql)
    {
      int ret = OB_SUCCESS;

      if (NULL == sql || '\0' == *sql)
      {
        TBSYS_LOG(ERROR,"sql is null");
        ret = OB_INVALID_ARGUMENT;
      }

      const char* ptr = NULL;
      const char* table  = sql;
      uint64_t table_id = OB_INVALID_ID;
      const char* table_name = NULL;
      ObTableSchema* table_schema = NULL;
      char line[OB_MAX_FILE_NAME_LENGTH];

      ObSchemaManagerV2 *schema_manager = get_schema_for_update();

      if (NULL == schema_manager)
      {
        ret = OB_ERROR;
      }

      while((OB_SUCCESS == ret) && (ptr= strchr(table,'\n')) != NULL)
      {
        if (ptr - table <= 0)
          continue;

        strncpy(line,table,ptr - table);
        line[ptr - table] = '\0';
        table = ++ptr;

        if (0 == strncmp(ALTER_TABLE_SQL,line,strlen(ALTER_TABLE_SQL)))
        {
          table_name = strrchr(line,' ');
          table_schema =  schema_manager->get_table_schema(++table_name);
          if (table_schema != NULL)
          {
            table_id = table_schema->get_table_id();
          }
          else
          {
            TBSYS_LOG(WARN,"can't find table (%s)",table_name);
          }
        }
        else if (table_id != OB_INVALID_ID)
        {
          if (0 == strncmp(DROP_COLUMN_SQL,line,strlen(DROP_COLUMN_SQL)))
          {
            const char* column_name = strrchr(line,' ');
            ++column_name;

            ObColumnSchemaV2* c[OB_MAX_COLUMN_GROUP_NUMBER];
            int32_t size = sizeof(c) / sizeof(c[0]);
            if ((ret = schema_manager->get_column_schema(table_schema->get_table_name(),column_name,c,size)) != OB_SUCCESS)
            {
              TBSYS_LOG(WARN,"column (%s:%s) doesn't exist",table_name,column_name);
            }
            else
            {
              for(int32_t i=0; i<size; ++i)
              {
                TBSYS_LOG(INFO,"%d:delete %s",i,c[i]->get_name());
                schema_manager->del_column(*c[i]);
              }
            }
          }
          else if (0 == strncmp(ADD_COLUMN_SQL,line,strlen(ADD_COLUMN_SQL)))
          {
            ColumnInfo c;
            if ( (ret = parse_column(line + strlen(ADD_COLUMN_SQL),c)) != OB_SUCCESS)
            {
              TBSYS_LOG(WARN,"syntax error:%s",line);
            }
            else
            {
              c.column.set_table_id(table_schema->get_table_id());
              c.column.set_column_group_id(OB_DEFAULT_COLUMN_GROUP_ID);
              c.column.set_column_id(table_schema->get_max_column_id() + 1);
              c.column.set_maintained(true);
              table_schema->set_max_column_id(table_schema->get_max_column_id() + 1);
              ret = schema_manager->add_column(c.column);
            }
          }
          else
          {
            //NOT SUPPORTED
          }
        }
      }

      if (OB_SUCCESS == ret)
      {
        if ((ret = add_table(*schema_manager)) != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR,"add table failed : %d",ret);
        }
      }
      
      if (schema_manager != NULL)
      {
        ob_free(schema_manager);
        schema_manager = NULL;
      }
      return ret;
    }
    
    //this function will change its first arguments(line)
    int ObMetaTable::parse_column(char* line,ColumnInfo& c) 
    {
      int ret = OB_SUCCESS;
      if (NULL == line)
      {
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        char column_name[OB_MAX_COLUMN_NAME_LENGTH];
        char column_type[OB_MAX_COLUMN_NAME_LENGTH];
        int32_t column_len = 0;
        line = ltrim(line);
        if ( sscanf(line,"%[^,],%[^(](%d)",column_name,column_type,&column_len) != 3)
        {
          TBSYS_LOG(ERROR,"parse column failed : syntax error:%s,%s,%d",column_name,column_type,column_len);
          ret = OB_ERROR;
        }
        else
        {
          c.column.set_column_name(str_trim(column_name));
          c.column.set_column_type(ObColumnSchemaV2::convert_str_to_column_type(str_trim(column_type)));
          c.column.set_column_size(column_len);

          char* ptr = strchr(line,',');
          line = ++ptr;
          ptr = strchr(line,',');

          if (ptr != NULL)
          {
            line = ++ptr;
            while((ptr = strtok_r(line,",\n\0",&line)) != NULL)
            {
              if (strstr(ptr,"primary key") != NULL)
              {
                TBSYS_LOG(INFO,"rowkey field:%s",ptr);
                line = ++ptr;
                c.is_rowkey = true;
                if (strstr(ptr,"(split)") != NULL)
                {
                  TBSYS_LOG(INFO,"split pos");
                  c.is_split_pos = true;
                }
              }
            }
          }
        }
      }
      return ret;
    }


    int ObMetaTable::get_row(ObScanner& scanner,ObCellInfo* cell_array,int32_t& size,bool& need_next_cell)
    {
      int ret = OB_SUCCESS;
      bool is_row_changed = false;
      int i = 0;
      ObCellInfo *cell = NULL;
      if (need_next_cell && (OB_SUCCESS == (ret = scanner.next_cell())) ) 
      {
        need_next_cell = false;
      }

      while(OB_SUCCESS == ret && i < size)
      {
        cell = NULL;
        if ((ret = scanner.get_cell(&cell,&is_row_changed)) != OB_SUCCESS)
        {
          if (OB_ITER_END != ret)
            TBSYS_LOG(ERROR,"get cell failed, ret = %d",ret);
        }
        else if (!is_row_changed || (is_row_changed && 0 == i) )
        {
          cell_array[i++] = *cell;
        }

        if (OB_SUCCESS == ret && is_row_changed && i > 1) //the first cell with row changed
        {
          break;
        }

        if (OB_SUCCESS == ret && (ret = scanner.next_cell()) != OB_SUCCESS)
        {
          if (OB_ITER_END != ret)
          {
            TBSYS_LOG(ERROR,"next cell failed,ret = %d",ret);
          }
        }
      }

      size = i;

      if (OB_ITER_END == ret)
        ret = OB_SUCCESS;
      
      return ret;
    }

    int ObMetaTable::build_range(ObRange& range,int64_t version,int64_t rowkey_len)
    {
      int ret = OB_SUCCESS;

      int64_t len = startkey_buffer_.get_buffer()->capacity();
      int64_t pos = 0;
      char *start_key_ = startkey_buffer_.get_buffer()->current();
      char *end_key_ = endkey_buffer_.get_buffer()->current();

      serialization::encode_i64(start_key_,len,pos,version);
      memset(start_key_ + pos, 0x00,rowkey_len - pos);
      range.start_key_.assign_ptr(start_key_,rowkey_len);
      
      len = startkey_buffer_.get_buffer()->capacity();
      pos = 0;

      serialization::encode_i64(end_key_,len,pos,version);
      memset(end_key_ + pos, 0xff, rowkey_len - pos);
      range.end_key_.assign_ptr(end_key_,rowkey_len);
      return ret;
    }

    //user must delete schema_manager by self
    ObSchemaManagerV2* ObMetaTable::get_schema_for_update(void)
    {
      void *schema = ob_malloc(sizeof(ObSchemaManagerV2));
      ObSchemaManagerV2 *schema_manager = NULL;

      if (NULL != schema)
      {
        schema_manager = new(schema) ObSchemaManagerV2;
        if (get_schema(0,*schema_manager) != OB_SUCCESS)
        {
          ob_free(schema_manager);
          schema_manager = NULL;
        }
        else
        {
          TBSYS_LOG(INFO,"get schema ,sort ...");
          schema_manager->sort_column();
          int64_t schema_timestamp = tbsys::CTimeUtil::getTime();
          schema_manager->set_version(schema_timestamp);
        }
      }
      else
      {
        TBSYS_LOG(ERROR,"cannot alloc memory for schema");
      }

      return schema_manager;
    }

    
  } /* common */
  
} /* oceanbase */
