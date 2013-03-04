/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_merge_groupby.h
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#ifndef _OB_MERGE_GROUPBY_H
#define _OB_MERGE_GROUPBY_H 1
#include "ob_groupby.h"
#include "ob_aggregate_function.h"
namespace oceanbase
{
  namespace sql
  {
    class ObScalarAggregate;

    // 输入数据已经按照groupby列排序
    class ObMergeGroupBy: public ObGroupBy
    {
      public:
        ObMergeGroupBy();
        virtual ~ObMergeGroupBy();

        virtual void set_int_div_as_double(bool did);

        virtual int open();
        virtual int close();
        virtual int get_next_row(const common::ObRow *&row);
        virtual int get_row_desc(const common::ObRowDesc *&row_desc) const;
      private:
        int is_same_group(const ObRow &row1, const ObRow &row2, bool &result);
        // disallow copy
        ObMergeGroupBy(const ObMergeGroupBy &other);
        ObMergeGroupBy& operator=(const ObMergeGroupBy &other);
        // friends
        friend class ObScalarAggregate;
      private:
        // data members
        ObAggregateFunction aggr_func_;
        const ObRow *last_input_row_;
    };

    inline void ObMergeGroupBy::set_int_div_as_double(bool did)
    {
      aggr_func_.set_int_div_as_double(did);
    }

  } // end namespace sql
} // end namespace oceanbase

#endif /* _OB_MERGE_GROUPBY_H */
