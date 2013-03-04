/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_fake_sstable_scan.h
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@taobao.com>
 *
 */

#ifndef _OB_FAKE_SSTABLE_SCAN_H
#define _OB_FAKE_SSTABLE_SCAN_H 1

#include "sql/ob_rowkey_phy_operator.h"
#include "ob_file_table.h"
#include "sql/ob_sstable_scan.h"

namespace oceanbase
{
  using namespace common;

  namespace sql 
  {
    namespace test
    {
      class ObFakeSstableScan : public ObSstableScan
      {
        public:
          ObFakeSstableScan(const char *file_name);
          virtual ~ObFakeSstableScan() {};

          virtual int set_child(int32_t child_idx, ObPhyOperator &child_operator);
          virtual int open();
          virtual int close();
          virtual int get_next_row(const ObString *&rowkey, const ObRow *&row);
          virtual int64_t to_string(char* buf, const int64_t buf_len) const;
          virtual int set_scan_param(const sstable::ObSSTableScanParam &param);

        private:
          // disallow copy
          ObFakeSstableScan(const ObFakeSstableScan &other);
          ObFakeSstableScan& operator=(const ObFakeSstableScan &other);

        private:
          // data members
          ObFileTable file_table_;
          ObString cur_rowkey_;
          sstable::ObSSTableScanParam scan_param_;
          ObRow curr_row_;
          ObRowDesc row_desc_;
      };
    }
  }
}

#endif /* _OB_FAKE_SSTABLE_SCAN_H */

