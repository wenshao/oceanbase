/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_result_set.h
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#ifndef _OB_RESULT_SET_H
#define _OB_RESULT_SET_H 1
#include "common/ob_row.h"
#include "common/ob_array.h"
#include "sql/ob_phy_operator.h"

namespace oceanbase
{
  namespace sql
  {
    // query result set
    class ObResultSet
    {
      public:
        struct Field
        {
          common::ObString tname_; // table name for display
          common::ObString org_tname_; // original table name
          common::ObString cname_;     // column name for display
          common::ObString org_cname_; // original column name
          common::ObObj type_;      // value type
        };
      public:
        ObResultSet();
        ~ObResultSet();
        /// open and execute the execution plan
        /// @note SHOULD be called for all statement even if there is no result rows
        int open();
        /// get the next result row
        /// @return OB_ITER_END when no more data available
        int get_next_row(const common::ObRow *&row);
        /// close the result set after get all the rows
        int close();
        /// get number of rows affected by INSERT/UPDATE/DELETE
        int64_t get_affected_rows() const;
        /// get warning count during the execution
        int64_t get_warning_count() const;
        /// get the server's error message
        const char* get_message() const;
        /// get the field columns
        const common::ObArray<Field> & get_field_columns() const;
        /// whether the result is with rows (true for SELECT statement)
        bool is_with_rows() const;
        /// add a field columns
        int add_field_column(Field & field);

        void set_message(const char* message);
        void set_execution_plan(ObPhyOperator *exec_plan);
        ObPhyOperator *get_execution_plan();
        void set_is_with_rows(bool is_with_rows);
        void set_affected_rows(const int64_t& affected_rows);
        void set_warning_count(const int64_t& warning_count);
      private:
        // types and constants
        static const int64_t MSG_SIZE = 512;
      private:
        // disallow copy
        ObResultSet(const ObResultSet &other);
        ObResultSet& operator=(const ObResultSet &other);
        // function members
      private:
        // data members
        bool is_with_rows_;
        int64_t affected_rows_; // number of rows affected by INSERT/UPDATE/DELETE
        int64_t warning_count_;
        char message_[MSG_SIZE]; // null terminated message string
        ObPhyOperator* exec_plan_;
        common::ObArray<Field> field_columns_;
    };

    inline ObResultSet::ObResultSet()
      :is_with_rows_(true), affected_rows_(0), warning_count_(0), exec_plan_(NULL)
    {
      message_[0] = '\0';
    }

    inline ObResultSet::~ObResultSet()
    {
    }

    inline int ObResultSet::open()
    {
      int ret = common::OB_SUCCESS;
      if (OB_UNLIKELY(NULL == exec_plan_))
      {
        ret = common::OB_NOT_INIT;
      }
      else
      {
        ret = exec_plan_->open();
      }
      return ret;
    }

    inline int ObResultSet::get_next_row(const common::ObRow *&row)
    {
      OB_ASSERT(exec_plan_);
      return exec_plan_->get_next_row(row);
    }

    inline int ObResultSet::close()
    {
      int ret = common::OB_SUCCESS;
      if (OB_UNLIKELY(NULL == exec_plan_))
      {
        ret = common::OB_NOT_INIT;
      }
      else
      {
        ret = exec_plan_->close();
      }
      return ret;
    }

    inline int64_t ObResultSet::get_affected_rows() const
    {
      return affected_rows_;
    }

    inline int64_t ObResultSet::get_warning_count() const
    {
      return warning_count_;
    }


    inline const char* ObResultSet::get_message() const
    {
      return message_;
    }

    inline void ObResultSet::set_message(const char* message)
    {
      snprintf(message_, MSG_SIZE, "%s", message);
    }

    inline void ObResultSet::set_execution_plan(ObPhyOperator *exec_plan)
    {
      exec_plan_ = exec_plan;
    }

    inline ObPhyOperator *ObResultSet::get_execution_plan()
    {
      return exec_plan_;
    }

    inline int ObResultSet::add_field_column(ObResultSet::Field & field)
    {
      return field_columns_.push_back(field);
    }
    
    inline const common::ObArray<ObResultSet::Field> & ObResultSet::get_field_columns() const
    {
      return field_columns_;
    }

    inline bool ObResultSet::is_with_rows() const
    {
      return is_with_rows_;
    }

    inline void ObResultSet::set_is_with_rows(bool is_with_rows)
    {
      is_with_rows_ = is_with_rows;
    }

    inline void ObResultSet::set_affected_rows(const int64_t& affected_rows)
    {
      affected_rows_ = affected_rows;
    }
    
    inline void ObResultSet::set_warning_count(const int64_t& warning_count)
    {
      warning_count_ = warning_count;
    }
        
  } // end namespace sql
} // end namespace oceanbase

#endif /* _OB_RESULT_SET_H */
