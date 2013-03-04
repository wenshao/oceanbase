/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_limit.h
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#ifndef _OB_LIMIT_H
#define _OB_LIMIT_H 1
#include "ob_single_child_phy_operator.h"

namespace oceanbase
{
  namespace sql
  {
    class ObLimit: public ObSingleChildPhyOperator
    {
      public:
        ObLimit();
        virtual ~ObLimit();

        void reset();
        /// @param limit -1 means no limit
        int set_limit(const int64_t limit, const int64_t offset);
        int get_limit(int64_t &limit, int64_t &offset) const;
        virtual int open();
        virtual int close();
        virtual int get_next_row(const common::ObRow *&row);
        virtual int get_row_desc(const common::ObRowDesc *&row_desc) const;
        virtual int64_t to_string(char* buf, const int64_t buf_len) const;
        void assign(const ObLimit &other);

        NEED_SERIALIZE_AND_DESERIALIZE;
      private:
        // disallow copy
        ObLimit(const ObLimit &other);
        ObLimit& operator=(const ObLimit &other);
      private:
        // data members
        int64_t limit_;         // -1 means no limit
        int64_t offset_;
        int64_t input_count_;
        int64_t output_count_;
    };
  } // end namespace sql
} // end namespace oceanbase

#endif /* _OB_LIMIT_H */
