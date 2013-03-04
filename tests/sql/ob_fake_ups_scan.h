/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_fake_ups_scan.h
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@taobao.com>
 *
 */

#ifndef _OB_FAKE_UPS_SCAN_H
#define _OB_FAKE_UPS_SCAN_H 1

#include "sql/ob_rowkey_phy_operator.h"
#include "sql/ob_ups_scan.h"
#include "ob_ups_file_table.h"

namespace oceanbase
{
  using namespace common;

  namespace sql 
  {
    namespace test
    {
      class ObFakeUpsScan : public ObUpsScan 
      {
        public:
          ObFakeUpsScan(const char *file_name);
          virtual ~ObFakeUpsScan() {};

          virtual int set_child(int32_t child_idx, ObPhyOperator &child_operator);
          virtual int open();
          virtual int close();
          virtual int get_next_row(const ObString *&rowkey, const ObRow *&row);
          virtual int64_t to_string(char* buf, const int64_t buf_len) const;
          
        private:
          // disallow copy
          ObFakeUpsScan(const ObFakeUpsScan &other);
          ObFakeUpsScan& operator=(const ObFakeUpsScan &other);

        private:
          // data members
          ObUpsFileTable file_table_;
          ObString cur_rowkey_;
      };
    }
  }
}

#endif /* _OB_FAKE_UPS_SCAN_H */


