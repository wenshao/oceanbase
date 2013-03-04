/*
 * (C) 2007-2011 TaoBao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * ob_meta_table.h is for what ...
 *
 * Version: $id$
 *
 * Authors:
 *   MaoQi maoqi@taobao.com
 *
 */

#ifndef OCEANBASE_COMMON_OB_META_TABLE_H_
#define OCEANBASE_COMMON_OB_META_TABLE_H_

#include "ob_define.h"
#include "ob_schema.h"
#include "thread_buffer.h"

namespace oceanbase 
{
  namespace common
  {

    const uint64_t USER_TABLES_ID = 103UL;
    const uint64_t USER_TAB_COLUMNS_ID = 104UL;
    const uint64_t USER_TAB_JOININFO_ID = 105L;

    const int64_t USER_TABLES_ROWKEY_LEN = 16;
    const int64_t USER_TAB_COLUMNS_ROWKEY_LEN = 32;
    const int64_t USER_TAB_JOININFO_ROWKEY_LEN= 32;


    class ObClientHelper;
    class ObScanner;
    class ObCellInfo;
    class ObSchemaManagerV2;
    class ObColumnSchemaV2;
    class ObTableSchema;
    class ObRange;
    class ObMutator;

    class ObMetaTable 
    {
      public:

        ObMetaTable();

        int init(ObClientHelper *client_helper);

        struct TableScanHelper 
        {
          int64_t column_id;
          const char* column_name;
        };

        /** 
         * @brief convert the content of meta table to schema
         * 
         * @param [in] version schema version if version eq 0,then get the newest version schema
         * @param [out] schema_manager
         * 
         * @return OB_SUCCESS on success, otherwise OB_ERROR
         */
        int get_schema(int64_t version,ObSchemaManagerV2& schema_manager);

        /** 
         * @brief add the tables in schema_manager to meta table
         * 
         * @param [in] schema_manager tables that will add to meta table
         * @param [in] ingore_table this table will not add to meta table
         * 
         * @return  OB_SUCCESS on success, on error,errno is returned.
         */
        int add_table(const ObSchemaManagerV2& schema_manager,uint64_t ingore_table = OB_INVALID_ID);

        /** 
         * @brief apply sql from client,just support create table & drop table
         * 
         * @param sql
         * 
         * @return  OB_SUCCESS on success
         */
        int apply_sql(const char* sql);

        /** 
         * @brief get the version of the newest schema
         * 
         * @return schema version
         */
        int64_t get_schema_version(void);

        /** 
         * @brief get the max table id that OB used
         * 
         * @return the max table id
         */
        uint64_t get_max_table_id(void);

      private:

        /** 
         * @brief create a table from sql
         * 
         * @param [in] create table sql
         * 
         * @return OB_SUCCESS on success, on error,errno is returned.
         */
        int add_table(const char *sql);

        /** 
         * @brief drop a table
         * 
         * @param sql [drop table table_name]
         * 
         * @return  OB_SUCCESS on success
         */
        int drop_table(const char *sql);
    
        int alter_table(const char* sql);
        
        struct ColumnInfo 
        {
          ColumnInfo():is_rowkey(false),is_split_pos(false) {}
          ObColumnSchemaV2 column;
          bool is_rowkey;
          bool is_split_pos;
        };
        
        /** 
         * @brief parse one column from sql
         * 
         * @param[in] line one line in create table sql
         * @param[out] c  column info
         * 
         * @return OB_SUCCESS on success
         */
        int parse_column(char* line,ColumnInfo& c);

        int fill_table_columns(ObSchemaManagerV2& schema_manager);
        
        int fill_table_joininfo(ObSchemaManagerV2& schema_manager);
       
        int add_table(int64_t version,uint64_t max_table_id,const ObTableSchema* table_begin,
                      int64_t table_count,uint64_t ingore_table = OB_INVALID_ID);
    
        int get_version(const char* key,int64_t& version);

        int set_version(const char* key,int64_t version,ObMutator& mutator);
      
        int add_columns(int64_t version,const ObColumnSchemaV2* col_begin,int64_t column_count);
     
        int get_row(ObScanner& scanner,ObCellInfo* cell_array,int32_t& size,bool& need_next_cell);
    
        int build_range(ObRange& range,int64_t version,int64_t rowkey_len);

        ObSchemaManagerV2* get_schema_for_update(void);

      private:
        bool inited_;
        ObClientHelper *client_helper_;

        common::ThreadSpecificBuffer startkey_buffer_;
        common::ThreadSpecificBuffer endkey_buffer_;
    };
    
  } /* common */
  
} /* oceanbase */
#endif
