/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_merge_join.h
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#ifndef _OB_MERGE_JOIN_H
#define _OB_MERGE_JOIN_H 1

#include "ob_join.h"
#include "common/ob_row.h"
#include "common/ob_array.h"
#include "common/ob_row_store.h"

namespace oceanbase
{
  namespace sql
  {
    // 要求两个输入left_child和right_child在等值join列上排好序
    // 支持所有join类型
    class ObMergeJoin: public ObJoin
    {
      public:
        ObMergeJoin();
        virtual ~ObMergeJoin();
        virtual int open();
        virtual int close();
        virtual int set_join_type(const ObJoin::JoinType join_type);
        virtual int get_next_row(const common::ObRow *&row);
        virtual int get_row_desc(const common::ObRowDesc *&row_desc) const;
        virtual int64_t to_string(char* buf, const int64_t buf_len) const;
      private:
        int normal_get_next_row(const common::ObRow *&row);
        int inner_get_next_row(const common::ObRow *&row);
        int left_outer_get_next_row(const common::ObRow *&row);
        int right_outer_get_next_row(const common::ObRow *&row);
        int full_outer_get_next_row(const common::ObRow *&row);
        int left_semi_get_next_row(const common::ObRow *&row);
        int right_semi_get_next_row(const common::ObRow *&row);
        int left_anti_semi_get_next_row(const common::ObRow *&row);
        int right_anti_semi_get_next_row(const common::ObRow *&row);
        int compare_equijoin_cond(const ObRow& r1, const ObRow& r2, int &cmp) const;
        int left_row_compare_equijoin_cond(const ObRow& r1, const ObRow& r2, int &cmp) const;
        int curr_row_is_qualified(bool &is_qualified);
        int cons_row_desc(const ObRowDesc &rd1, const ObRowDesc &rd2);
        int join_rows(const ObRow& r1, const ObRow& r2);
        int left_join_rows(const ObRow& r1);
        int right_join_rows(const ObRow& r2);
        // disallow copy
        ObMergeJoin(const ObMergeJoin &other);
        ObMergeJoin& operator=(const ObMergeJoin &other);
      private:
        static const int64_t MAX_SINGLE_ROW_SIZE = common::OB_ROW_MAX_COLUMNS_COUNT*(common::OB_MAX_VARCHAR_LENGTH+4);
        // data members
        typedef int (ObMergeJoin::*get_next_row_func_type)(const common::ObRow *&row);
        get_next_row_func_type get_next_row_func_;
        const common::ObRow *last_left_row_;
        const common::ObRow *last_right_row_;
        common::ObRow last_join_left_row_;
        common::ObString last_join_left_row_store_;
        common::ObRowStore right_cache_;
        common::ObRow curr_cached_right_row_;
        common::ObRow curr_row_;
        common::ObRowDesc row_desc_;
        bool right_cache_is_valid_;
        bool is_right_iter_end_;
    };
  } // end namespace sql
} // end namespace oceanbase

#endif /* _OB_MERGE_JOIN_H */
