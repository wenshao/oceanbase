/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_sql.h
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#ifndef _OB_SQL_H
#define _OB_SQL_H 1
#include "common/ob_string.h"
#include "sql/ob_result_set.h"
namespace oceanbase
{
  namespace sql
  {
    struct ObStmtPrepareResult;
    // this class is the main interface for sql module
    // @note thread-safe
    class ObSql
    {
      public:
        ObSql(){}
        ~ObSql(){}
        /**
         * execute the SQL statement directly
         *
         * @param stmt [in]
         * @param result [out]
         *
         * @return oceanbase error code defined in ob_define.h
         */
        int direct_execute(const common::ObString &stmt, ObResultSet &result);
        /**
         * prepare the SQL statement for later execution
         * @see stmt_execute()
         * @param stmt [in]
         * @param result [out]
         *
         * @return oceanbase error code defined in ob_define.h
         */
        int stmt_prepare(const common::ObString &stmt, ObStmtPrepareResult &result);
        /**
         * execute the prepared statement
         *
         * @param stmt_id [in] statement handler id returned by stmt_prepare()
         * @param params [in] parameters for binding
         * @param result [out]
         *
         * @return oceanbase error code defined in ob_define.h
         */
        int stmt_execute(const uint64_t stmt_id, const common::ObArray<common::ObObj> params, ObResultSet &result);
        /**
         * close the prepared statement
         *
         * @param stmt_id [in] statement handler id returned by stmt_prepare()
         *
         * @return oceanbase error code defined in ob_define.h
         */
        int stmt_close(const uint64_t stmt_id);
      private:
        // types and constants
      private:
        // disallow copy
        ObSql(const ObSql &other);
        ObSql& operator=(const ObSql &other);
        // function members
      private:
        // data members
    };

    /// @see ObSql::stmt_prepare()
    struct ObStmtPrepareResult
    {
      uint64_t stmt_id_;         // statement handler id
      int64_t res_columns_count_;   // number of columns in result set
      int64_t params_count_;      // number of parameters in query
      int64_t warning_count_;   // warning count
    };

  } // end namespace sql
} // end namespace oceanbase

#endif /* _OB_SQL_H */
