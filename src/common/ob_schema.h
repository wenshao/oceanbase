/*===============================================================
*   (C) 2007-2010 Taobao Inc.
*
*
*   Version: 0.1 2010-09-26
*
*   Authors:
*          daoan(daoan@taobao.com)
*          maoqi(maoqi@taobao.com)
*          fangji.hcm(fangji.hcm@taobao.com)
*
*
================================================================*/
#ifndef OCEANBASE_COMMON_OB_SCHEMA_H_
#define OCEANBASE_COMMON_OB_SCHEMA_H_
#include <stdint.h>

#include <tbsys.h>

#include "ob_define.h"
#include "ob_object.h"
#include "ob_string.h"
#include "hash/ob_hashutils.h"
#include "hash/ob_hashmap.h"
#include "ob_postfix_expression.h"

#define PERM_TABLE_NAME "__perm_info"
#define USER_TABLE_NAME "__user_info"
#define SKEY_TABLE_NAME "__skey_info"
#define PERM_COL_NAME "perm_desc"
#define USER_COL_NAME "password"
#define SKEY_COL_NAME "secure_key"

#define PERM_TABLE_ID 100
#define USER_TABLE_ID 101
#define SKEY_TABLE_ID 102
#define PERM_COL_ID 4
#define USER_COL_ID 4
#define SKEY_COL_ID 4

namespace oceanbase
{
  namespace common
  {
    //these classes are so close in logical, so I put them together to make client have a easy life
    typedef ObObjType ColumnType;

    class BaseInited
    {
      public:
        BaseInited();
        virtual ~BaseInited();
        void set_flag();
        bool have_inited() const;
        VIRTUAL_NEED_SERIALIZE_AND_DESERIALIZE;
      private:
        bool inited_;
    };

    class ObJoinInfo :public BaseInited
    {
      public:
        ObJoinInfo();
        bool init(const uint64_t left_column, const int32_t start_pos, const int32_t end_pos, const uint64_t table_id_joined);
        virtual ~ObJoinInfo();

        //if you got start_pos = end_pos = -1, that means you should use the whole rowkey
        void get_rowkey_join_range(int32_t& out_start_pos, int32_t& out_end_pos) const;

        void add_correlated_column(const uint64_t left_column_id, const uint64_t right_column_id);

        uint64_t get_table_id_joined() const;

        uint64_t find_left_column_id(const uint64_t rid) const;
        uint64_t find_right_column_id(const uint64_t lid) const;
        NEED_SERIALIZE_AND_DESERIALIZE;

        //this is for test
        void print_info() const;

      private:
        uint64_t left_column_id_; //for the first version, this one always is 1, means rowkey only
        int32_t  start_pos_;
        int32_t  end_pos_;  //this means which part of left_column_ to be used make rowkey
        uint64_t table_id_joined_;

        uint64_t correlated_left_columns[OB_OLD_MAX_COLUMN_NUMBER];
        uint64_t correlated_right_columns[OB_OLD_MAX_COLUMN_NUMBER];
        // 3,4 means this table column 3 will be
        //corrected by table_id_joined_ column 4
        int32_t correlated_info_size_;
    };

    class ObOperator;
    class ObSchemaManagerV2;

    class ObColumnSchemaV2
    {
      public:

        struct ObJoinInfo
        {
          ObJoinInfo() : join_table_(OB_INVALID_ID) {}
          uint64_t join_table_;
          uint64_t left_column_id_;//for the first version, this one always is 1, means rowkey only
          uint64_t correlated_column_;  //this means which part of left_column_ to be used make rowkey
          int32_t start_pos_;
          int32_t end_pos_;
        };

        ObColumnSchemaV2();
        ~ObColumnSchemaV2() {}
        ObColumnSchemaV2& operator=(const ObColumnSchemaV2& src_schema);

        uint64_t    get_id()   const;
        const char* get_name() const;
        ColumnType  get_type() const;
        int64_t     get_size() const;
        uint64_t    get_table_id()        const;
        bool        is_maintained()       const;
        uint64_t    get_column_group_id() const;


        void set_table_id(const uint64_t id);
        void set_column_id(const uint64_t id);
        void set_column_name(const char *name);
        void set_column_name(const ObString& name);
        void set_column_type(const ColumnType type);
        void set_column_size(const int64_t size); //only used when type is varchar
        void set_column_group_id(const uint64_t id);
        void set_maintained(bool maintained);

        void set_join_info(const uint64_t join_table,const uint64_t left_column_id,
                           const uint64_t correlated_column, const int32_t start_pos,const int32_t end_pos);

        const ObJoinInfo* get_join_info() const;

        bool operator==(const ObColumnSchemaV2& r) const;

        static ColumnType convert_str_to_column_type(const char* str);

        //this is for test
        void print_info() const;

        NEED_SERIALIZE_AND_DESERIALIZE;
      private:
        friend class ObSchemaManagerV2;
      private:
        bool maintained_;

        uint64_t table_id_;
        uint64_t column_group_id_;
        uint64_t column_id_;

        int64_t size_;  //only used when type is char or varchar
        ColumnType type_;
        char name_[OB_MAX_COLUMN_NAME_LENGTH];

        //join info
        ObJoinInfo join_info_;

        //in mem
        ObColumnSchemaV2* column_group_next_;
    };

    inline static bool column_schema_compare(const ObColumnSchemaV2& lhs, const ObColumnSchemaV2& rhs)
    {
      bool ret = false;
      if ( (lhs.get_table_id() < rhs.get_table_id()) ||
           (lhs.get_table_id() == rhs.get_table_id() && lhs.get_column_group_id() < rhs.get_column_group_id()) ||
           (lhs.get_table_id() == rhs.get_table_id() &&
            (lhs.get_column_group_id() == rhs.get_column_group_id()) && lhs.get_id() < rhs.get_id()) )
      {
        ret = true;
      }
      return ret;
    }

    struct ObColumnSchemaV2Compare
    {
      bool operator()(const ObColumnSchemaV2& lhs, const ObColumnSchemaV2& rhs)
      {
        return column_schema_compare(lhs, rhs);
      }
    };


    class ObTableSchema
    {
      public:
        ObTableSchema();
        ~ObTableSchema() {}
        ObTableSchema& operator=(const ObTableSchema& src_schema);
        enum TableType
        {
          INVALID = 0,
          SSTABLE_IN_DISK,
          SSTABLE_IN_RAM,
        };

        struct ExpireInfo
        {
          uint64_t column_id_;
          int64_t duration_;
        };

        uint64_t    get_table_id()   const;
        TableType   get_table_type() const;
        const char* get_table_name() const;
        const char* get_compress_func_name() const;
        uint64_t    get_max_column_id() const;
        int         get_expire_condition(uint64_t &column_id, int64_t &duration) const;
        const char* get_expire_condition() const;
        int32_t     get_version() const;

        int32_t get_split_pos() const;
        int32_t get_rowkey_max_length() const;

        bool is_pure_update_table() const;
        bool is_use_bloomfilter()   const;
        bool is_row_key_fixed_len() const;
        bool is_merge_dynamic_data() const;
        bool is_expire_effect_immediately() const;
        int32_t get_block_size()    const;
        int64_t get_max_sstable_size() const;
        int64_t get_expire_frequency()    const;
        int64_t get_query_cache_expire_time() const;
        int64_t get_max_scan_rows_per_tablet() const;
        int64_t get_internal_ups_scan_size() const;

        uint64_t get_create_time_column_id() const;
        uint64_t get_modify_time_column_id() const;

        void set_table_id(const uint64_t id);
        void set_max_column_id(const uint64_t id);
        void set_version(const int32_t version);

        void set_table_type(TableType type);
        void set_split_pos(const int64_t split_pos);

        void set_rowkey_max_length(const int64_t len);
        void set_block_size(const int64_t block_size);
        void set_max_sstable_size(const int64_t max_sstable_size);

        void set_table_name(const char* name);
        void set_table_name(const ObString& name);
        void set_compressor_name(const char* compressor);
        void set_compressor_name(const ObString& compressor);

        void set_pure_update_table(bool is_pure);
        void set_use_bloomfilter(bool use_bloomfilter);
        void set_rowkey_fixed_len(bool fixed_len);

        void set_merge_dynamic_data(bool merge_danamic_data);
        void set_expire_effect_immediately(const int64_t expire_effect_immediately);

        void set_expire_info(ExpireInfo& expire_info);
        void set_expire_condition(const char* expire_condition);
        void set_expire_condition(const ObString& expire_condition);

        void set_expire_frequency(const int64_t expire_frequency);
        void set_query_cache_expire_time(const int64_t expire_time);
        void set_max_scan_rows_per_tablet(const int64_t max_scan_rows);
        void set_internal_ups_scan_size(const int64_t scan_size);

        void set_create_time_column(uint64_t id);
        void set_modify_time_column(uint64_t id);

        bool operator ==(const ObTableSchema& r) const;
        bool operator ==(const ObString& table_name) const;
        bool operator ==(const uint64_t table_id) const;

        NEED_SERIALIZE_AND_DESERIALIZE;
        //this is for test
        void print_info() const;
        void print(FILE* fd) const;
      private:
        static const int64_t TABLE_SCHEMA_RESERVED_NUM = 4;
        uint64_t table_id_;
        uint64_t max_column_id_;
        int64_t rowkey_split_;
        int64_t rowkey_max_length_;

        int32_t block_size_; //KB
        TableType table_type_;

        char name_[OB_MAX_TABLE_NAME_LENGTH];
        char compress_func_name_[OB_MAX_TABLE_NAME_LENGTH];
        char expire_condition_[OB_MAX_EXPIRE_CONDITION_LENGTH];
        bool is_pure_update_table_;
        bool is_use_bloomfilter_;
        bool is_row_key_fixed_len_;
        bool is_merge_dynamic_data_;
        ExpireInfo expire_info_;
        int64_t expire_frequency_;  //how many frozen version passed before do expire once
        int64_t max_sstable_size_;
        int32_t version_;
        int64_t query_cache_expire_time_;
        int64_t is_expire_effect_immediately_;
        int64_t max_scan_rows_per_tablet_;
        int64_t internal_ups_scan_size_;
        int64_t reserved_[TABLE_SCHEMA_RESERVED_NUM];

        //in mem
        uint64_t create_time_column_id_;
        uint64_t modify_time_column_id_;
    };

    class ObSchemaManagerV2
    {
      public:
        ObSchemaManagerV2();
        explicit ObSchemaManagerV2(const int64_t timestamp);
        ~ObSchemaManagerV2();
      public:
        ObSchemaManagerV2& operator=(const ObSchemaManagerV2& schema); //ugly,for hashMap
        ObSchemaManagerV2(const ObSchemaManagerV2& schema);
      public:
        const ObColumnSchemaV2* column_begin() const;
        const ObColumnSchemaV2* column_end() const;
        const char* get_app_name() const;
        int set_app_name(const char* app_name);

        int64_t get_column_count() const;
        int64_t get_table_count() const;

        uint64_t get_max_table_id() const;
        /**
         * @brief timestap is the version of schema,version_ is used for serialize syntax
         *
         * @return
         */
        int64_t get_version() const;
        void set_version(const int64_t version);
        void set_max_table_id(const uint64_t version);

        /**
         * @brife return code version not schema version
         *
         * @return int32_t
         */
        int32_t get_code_version() const;

        const ObColumnSchemaV2* get_column_schema(const int32_t index) const;

        /**
         * @brief get a column accroding to table_id/column_group_id/column_id
         *
         * @param table_id
         * @param column_group_id
         * @param column_id
         *
         * @return column or null
         */
        const ObColumnSchemaV2* get_column_schema(const uint64_t table_id,
                                                  const uint64_t column_group_id,
                                                  const uint64_t column_id) const;

        /**
         * @brief get a column accroding to table_id/column_id, if this column belongs to
         * more than one column group,then return the column in first column group
         *
         * @param table_id the id of table
         * @param column_id the column id
         * @param idx[out] the index in table of this column
         *
         * @return column or null
         */
        const ObColumnSchemaV2* get_column_schema(const uint64_t table_id,
                                                  const uint64_t column_id,
                                                  int32_t* idx = NULL) const;


        const ObColumnSchemaV2* get_column_schema(const char* table_name,
                                                  const char* column_name,
                                                  int32_t* idx = NULL) const;

        const ObColumnSchemaV2* get_column_schema(const ObString& table_name,
                                                  const ObString& column_name,
                                                  int32_t* idx = NULL) const;


        const ObColumnSchemaV2* get_table_schema(const uint64_t table_id, int32_t& size) const;

        const ObColumnSchemaV2* get_group_schema(const uint64_t table_id,
                                                 const uint64_t column_group_id,
                                                 int32_t& size) const;

        const ObTableSchema* table_begin() const;
        const ObTableSchema* table_end() const;

        const ObTableSchema* get_table_schema(const char* table_name) const;
        const ObTableSchema* get_table_schema(const ObString& table_name) const;
        const ObTableSchema* get_table_schema(const uint64_t table_id) const;
        ObTableSchema* get_table_schema(const char* table_name);
        ObTableSchema* get_table_schema(const uint64_t table_id);
        int64_t get_table_query_cache_expire_time(const ObString& table_name) const;

        uint64_t get_create_time_column_id(const uint64_t table_id) const;
        uint64_t get_modify_time_column_id(const uint64_t table_id) const;

        int get_column_index(const char *table_name,const char* column_name,int32_t index_array[],int32_t& size) const;
        int get_column_index(const uint64_t table_id, const uint64_t column_id, int32_t index_array[],int32_t& size) const;

        int get_column_schema(const uint64_t table_id, const uint64_t column_id,
                              ObColumnSchemaV2* columns[],int32_t& size) const;

        int get_column_schema(const char *table_name, const char* column_name,
                              ObColumnSchemaV2* columns[],int32_t& size) const;

        int get_column_schema(const ObString& table_name,
                              const ObString& column_name,
                              ObColumnSchemaV2* columns[],int32_t& size) const;


        int get_column_groups(uint64_t table_id,uint64_t column_groups[],int32_t& size) const;

        bool is_compatible(const ObSchemaManagerV2& schema_manager) const;

        int add_column(ObColumnSchemaV2& column);
        int add_table(ObTableSchema& table);
        void del_column(const ObColumnSchemaV2& column);

        /**
         * @brief if you don't want to use column group,set drop_group to true and call this
         *        method before deserialize
         *
         * @param drop_group true - don't use column group,otherwise not
         *
         */
        void set_drop_column_group(bool drop_group = true);

        bool is_join_table(const uint64_t table_id) const;

        int64_t get_join_table_num() const { return join_table_nums_; }

        int add_join_table(const uint64_t table_id);

      public:
        bool parse_from_file(const char* file_name, tbsys::CConfig& config);
        bool parse_one_table(const char* section_name, tbsys::CConfig& config, ObTableSchema& schema);
        bool parse_column_info(const char* section_name, tbsys::CConfig& config, ObTableSchema& schema);
        bool parse_join_info(const char* section_name, tbsys::CConfig& config, ObTableSchema& schema);
        bool parse_expire_info(const char* section_name, tbsys::CConfig& config, ObTableSchema& schema);

      public:
        void print_info() const;
        void print(FILE* fd) const;
      public:
        struct ObColumnNameKey
        {
          int64_t hash() const;
          bool operator==(const ObColumnNameKey& key) const;
          ObString table_name_;
          ObString column_name_;
        };

        struct ObColumnIdKey
        {
          int64_t hash() const;
          bool operator==(const ObColumnIdKey& key) const;
          uint64_t table_id_;
          uint64_t column_id_;
        };

        struct ObColumnInfo
        {
          ObColumnSchemaV2* head_;
          int32_t table_begin_index_;
          ObColumnInfo() : head_(NULL), table_begin_index_(-1) {}
        };

        struct ObColumnGroupHelper
        {
          uint64_t table_id_;
          uint64_t column_group_id_;
        };

        struct ObColumnGroupHelperCompare
        {
          bool operator() (const ObColumnGroupHelper& l,const ObColumnGroupHelper& r) const;
        };

      public:
        NEED_SERIALIZE_AND_DESERIALIZE;
        int sort_column();

        static const int64_t DEFAULT_MAX_COLUMNS = OB_MAX_TABLE_NUMBER * OB_MAX_COLUMN_NUMBER;

      private:
        int replace_system_variable(char* expire_condition, const int64_t buf_size) const;
        int check_expire_dependent_columns(const ObString& expr,
          const ObTableSchema& table_schema, ObExpressionParser& parser) const;
        bool check_table_expire_condition() const;

      private:
        int32_t   schema_magic_;
        int32_t   version_;
        int64_t   timestamp_;
        uint64_t  max_table_id_;
        int64_t   column_nums_;
        int64_t   table_nums_;

        char app_name_[OB_MAX_APP_NAME_LENGTH];

        ObTableSchema    table_infos_[OB_MAX_TABLE_NUMBER];
        //ObColumnSchemaV2 columns_[DEFAULT_MAX_COLUMNS];
        //ObColumnSchemaV2 columns_[OB_MAX_COLUMN_NUMBER * OB_MAX_TABLE_NUMBER];
        ObColumnSchemaV2* columns_;

        //just in mem
        bool drop_column_group_; //
        volatile bool hash_sorted_;       //after deserialize,will rebuild the hash maps
        hash::ObHashMap<ObColumnNameKey,ObColumnInfo,hash::NoPthreadDefendMode> column_hash_map_;
        hash::ObHashMap<ObColumnIdKey,ObColumnInfo,hash::NoPthreadDefendMode> id_hash_map_;

        int64_t column_group_nums_;
        ObColumnGroupHelper column_groups_[OB_MAX_COLUMN_GROUP_NUMBER * OB_MAX_TABLE_NUMBER];

        int64_t join_table_nums_;
        uint64_t join_tables_[OB_MAX_TABLE_NUMBER];
    };
  }
}
#endif
