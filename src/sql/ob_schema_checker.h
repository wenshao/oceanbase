/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_schema_checker.h
 *
 * Authors:
 *   Guibin Du <tianguan.dgb@taobao.com>
 *
 */
#ifndef OCEANBASE_SQL_OB_SCHEMA_CHECKER_H_
#define OCEANBASE_SQL_OB_SCHEMA_CHECKER_H_
#include "common/ob_schema.h"
#include "common/ob_string.h"

namespace oceanbase
{
  namespace sql
  {
    class ObSchemaChecker
    {
      public:
        ObSchemaChecker();
        explicit ObSchemaChecker(const common::ObSchemaManagerV2& schema_mgr);
        virtual ~ObSchemaChecker();

        void set_schema(const common::ObSchemaManagerV2& schema_mgr);
        bool column_exists(
            const common::ObString& table_name,
            const common::ObString& column_name) const;
        uint64_t get_column_id(
            const common::ObString& table_name, 
            const common::ObString& column_name) const;
        uint64_t get_table_id(const common::ObString& table_name) const;
        const common::ObTableSchema* get_table_schema(const char* table_name) const;
        const common::ObColumnSchemaV2* get_column_schema(
            const common::ObString& table_name, 
            const common::ObString& column_name) const;
        const common::ObColumnSchemaV2* get_table_columns(
            const uint64_t table_id,
            int32_t& size) const;
        
      private:
        // disallow copy
        ObSchemaChecker(const ObSchemaChecker &other);
        ObSchemaChecker& operator=(const ObSchemaChecker &other);
      private:
        const common::ObSchemaManagerV2 *schema_mgr_;
    };

    inline void ObSchemaChecker::set_schema(const common::ObSchemaManagerV2& schema_mgr)
    {
      schema_mgr_ = &schema_mgr;
    }
  } // end namespace sql
} // end namespace oceanbase

#endif /* OCEANBASE_SQL_OB_SCHEMA_CHECKER_H_ */

