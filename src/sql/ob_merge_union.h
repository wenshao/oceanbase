/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_merge_union.h
 *
 * Authors:
 *   TIAN GUAN <tianguan.dgb@taobao.com>
 *
 */
#ifndef OCEANBASE_SQL_OB_MERGE_UNION_H_
#define OCEANBASE_SQL_OB_MERGE_UNION_H_

#include "sql/ob_set_operator.h"
#include "common/ob_row.h"

namespace oceanbase
{
  namespace sql
  {
    class ObMergeUnion: public ObSetOperator
    {
      public:
        ObMergeUnion();
        virtual ~ObMergeUnion();
        /**
         * 获得下一行的引用
         * @note 在下次调用get_next或者close前，返回的row有效
         * @pre 调用open()
         * @param row [out]
         *
         * @return OB_SUCCESS或OB_ITER_END或错误码
         */
        virtual int get_next_row(const common::ObRow *&row);
        virtual int get_row_desc(const common::ObRowDesc *&row_desc) const;
        virtual int64_t to_string(char* buf, const int64_t buf_len) const;
      private:
        DISALLOW_COPY_AND_ASSIGN(ObMergeUnion);

      private:
        // 前一条已经被输出的ObRow
        common::ObRow     last_row_;
        // 不能保证last_row_中的varchar类型的指针依然有效，所以我们自己存储在last_copressive_row_中
        //CompressiveRow last_copressive_row_;
        common::ObRow *cur_first_query_row_;
        common::ObRow *cur_second_query_row_;
        // 标识第一个查询已经迭代到结束了
        bool first_query_end_;
        // 标识第二个查询已经迭代到结束了
        bool second_query_end_;
    };
  } // end namespace sql
} // end namespace oceanbase

#endif /* OCEANBASE_SQL_OB_UNION_H_ */
