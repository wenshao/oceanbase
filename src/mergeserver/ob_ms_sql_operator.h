/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or 
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_merger_operator.cpp for 
 *
 * Authors:
 *   wushi <wushi.ly@taobao.com>
 *
 */
#ifndef MERGESERVER_OB_MS_SQL_OPERATOR_CPP_ 
#define MERGESERVER_OB_MS_SQL_OPERATOR_CPP_
#include "common/ob_new_scanner.h"
#include "common/ob_row_iterator.h"
#include "common/ob_groupby_operator.h"
#include "common/ob_return_operator.h"
#include "common/ob_compose_operator.h"
#include "common/ob_cell_array.h"
#include "ob_merger_reverse_operator.h"
#include "ob_ms_sql_sorted_operator.h"
#include "ob_merger_groupby_operator.h"
namespace oceanbase
{
  namespace mergeserver
  {
    class ObMsSqlSortedOperator;
    class ObMergerReverseOperator;
    class ObMsSqlOperator : public common::ObRowIterator
    {
    public:
      ObMsSqlOperator();
      ~ObMsSqlOperator();
      /// initialize
      int set_param(const common::ObScanParam & scan_param);
      /// add a subscanrequest's result
      int add_sharding_result(common::ObNewScanner & sharding_res, const common::ObRange & query_range, const int64_t limit_offset,
        bool &is_finish, bool &can_free_res);
      /// finish processing result, like orderby grouped result

      int get_mem_size_used()const;

      void reset();

      int64_t get_result_row_width()const ;
      int64_t get_whole_result_row_count()const;

      int64_t get_sharding_result_count() const;
      int64_t get_cur_sharding_result_idx() const;
      
      int get_next_row(common::ObRow &row);
    private:
      enum
      {
        NOT_INIT,
        USE_SORTED_OPERATOR,
        USE_REVERSE_OPERATOR,
        USE_GROUPBY_OPERATOR
      };
      int status_;
      common::ObCellArray cells_;
      const common::ObScanParam *scan_param_;
      common::ObReturnOperator  return_operator_;
      ObMsSqlSortedOperator    sorted_operator_;
      common::ObNewScanner         *last_sharding_res_;
      int64_t                   sharding_res_count_;
    };
  }
}

#endif /* MERGESERVER_OB_MS_SQL_OPERATOR_CPP_ */
