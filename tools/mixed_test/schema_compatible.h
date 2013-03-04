#ifndef _MIXED_TEST_SCHEMA_COMPATIBLE_
#define _MIXED_TEST_SCHEMA_COMPATIBLE_
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include "common/ob_schema.h"
#include "utils.h"

namespace oceanbase
{
  namespace common
  {
    using namespace hash;
    class ObColumnSchema
    {
      public:
        ObColumnSchema() : column_schema_(NULL)
        {
        };
        ~ObColumnSchema()
        {
        };
      public:
        void init(const ObColumnSchemaV2 *column_schema)
        {
          assert(NULL == column_schema_);
          column_schema_ = column_schema;
        };
      public:
        uint64_t get_id() const
        {
          return column_schema_->get_id();
        };
        const char *get_name() const
        {
          return column_schema_->get_name();
        };
        ObObjType get_type() const
        {
          return column_schema_->get_type();
        };
        int64_t get_size() const
        {
          return column_schema_->get_size();
        };
      private:
        const ObColumnSchemaV2 *column_schema_;
    };
    
    class ObSchema
    {
      public:
        ObSchema() : table_schema_(NULL),
                     column_num_(0),
                     column_schemas_(NULL)
        {
        };
        ~ObSchema()
        {
          if (NULL != column_schemas_)
          {
            delete[] column_schemas_;
            column_schemas_ = NULL;
            column_map_.destroy();
          }
        };
      public:
        void init(const ObTableSchema *table_schema,
                  const ObColumnSchemaV2 *column_schema,
                  const int64_t column_num)
        {
          assert(NULL == table_schema_);
          table_schema_ = table_schema;
          column_num_ = column_num;
          column_schemas_ = new ObColumnSchema[column_num];
          column_map_.create(column_num);
          for (int64_t i = 0; i < column_num; i++)
          {
            column_schemas_[i].init(&(column_schema[i]));
            column_map_.set(column_schema[i].get_name(), &(column_schemas_[i]));
          }
        };
      public:
        const ObColumnSchema *find_column_info(const char *column_name) const
        {
          ObColumnSchema *ret = NULL;
          column_map_.get(column_name, ret);
          return ret;
        };
        const ObColumnSchema *find_column_info(const ObString &column_name) const
        {
          char buffer[OB_MAX_COLUMN_NAME_LENGTH] = {'\0'};
          snprintf(buffer, OB_MAX_COLUMN_NAME_LENGTH, "%.*s", column_name.length(), column_name.ptr());
          return find_column_info(buffer);
        };
        uint64_t get_table_id() const
        {
          return table_schema_->get_table_id();
        };
        const char *get_table_name() const
        {
          return table_schema_->get_table_name();
        };
        const ObColumnSchema *column_begin() const
        {
          return column_schemas_;
        };
        const ObColumnSchema *column_end() const
        {
          return column_schemas_ + column_num_;
        };
      private:
        const ObTableSchema *table_schema_;
        int64_t column_num_;
        ObColumnSchema *column_schemas_;
        ObHashMap<const char*, ObColumnSchema*> column_map_;
    };

    class ObSchemaManager
    {
      public:
        ObSchemaManager() : schema_mgr_(),
                            schemas_(NULL),
                            table_num_(0)
        {
        };
        ~ObSchemaManager()
        {
          if (NULL != schemas_)
          {
            delete[] schemas_;
            schemas_ = NULL;
          }
        };
        ObSchemaManager(const ObSchemaManager &other)
        {
          *this = other;
        };
        ObSchemaManager &operator=(const ObSchemaManager &other)
        {
          schema_mgr_ = other.schema_mgr_;
          schemas_ = other.schemas_;
          table_num_ = other.table_num_;
          ObSchemaManager &to_modify = const_cast<ObSchemaManager&>(other);
          to_modify.schemas_ = NULL;
          return *this;
        };
      public:
        const ObSchema *begin() const
        {
          init_();
          return schemas_;
        };
        const ObSchema *end() const
        {
          init_();
          return schemas_ + table_num_;
        };
        bool parse_from_file(const char* file_name, tbsys::CConfig& config)
        {
          bool bret = schema_mgr_.parse_from_file(file_name, config);
          init_();
          return bret;
        };
        void print_info() const
        {
          schema_mgr_.print_info();
        };
        ObSchemaManagerV2 &get_v2()
        {
          return schema_mgr_;
        };
      private:
        void init_() const
        {
          if (NULL == schemas_
              && 0 < schema_mgr_.get_table_count())
          {
            schemas_ = new ObSchema[schema_mgr_.get_table_count()];
            const ObTableSchema *table_schema = NULL;
            int64_t i = 0;
            for (table_schema = schema_mgr_.table_begin(); table_schema != schema_mgr_.table_end(); table_schema++)
            {
              int32_t size = 0;
              const ObColumnSchemaV2 *column_schema = schema_mgr_.get_table_schema(table_schema->get_table_id(), size);
              int64_t meta_num = 0;
              for (int32_t j = 0; j < size; j++)
              {
                uint64_t id = column_schema[j].get_id();
                if (C_TIME_COLUMN_ID == id
                    || M_TIME_COLUMN_ID == id
                    || SEED_COLUMN_ID == id
                    || ROWKEY_INFO_COLUMN_ID == id
                    || CELL_NUM_COLUMN_ID == id
                    || SUFFIX_LENGTH_COLUMN_ID == id
                    || SUFFIX_NUM_COLUMN_ID == id
                    || PREFIX_END_COLUMN_ID == id)
                {
                  meta_num++;
                }
              }
              if (META_COLUMN_NUM == meta_num)
              {
                schemas_[i++].init(table_schema, column_schema, size);
                TBSYS_LOG(INFO, "filt valid schema table_name=%s", table_schema->get_table_name());
              }
            }
            table_num_ = i;
            TBSYS_LOG(INFO, "filt valid schema num=%ld", table_num_);
          }
        };
      private:
        ObSchemaManagerV2 schema_mgr_;
        mutable ObSchema *schemas_;
        mutable int64_t table_num_;
    };
  }
}

#endif // _MIXED_TEST_SCHEMA_COMPATIBLE_

