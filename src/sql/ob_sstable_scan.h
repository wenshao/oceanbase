/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_sstable_scan.h
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#ifndef _OB_SSTABLE_SCAN_H
#define _OB_SSTABLE_SCAN_H 1

#include "sql/ob_phy_operator.h"
#include "common/ob_string.h"
#include "sstable/ob_sstable_scan_param.h"
#include "sql/ob_rowkey_phy_operator.h"

namespace oceanbase
{
  namespace sql
  {
    // 用于CS从磁盘或缓冲区扫描一个tablet
    class ObSstableScan: public ObRowkeyPhyOperator
    {
      public:
        ObSstableScan();
        virtual ~ObSstableScan();

        virtual int open();
        virtual int close();
        virtual int get_next_row(const common::ObString *&rowkey, const common::ObRow *&row);
        virtual int set_scan_param(const sstable::ObSSTableScanParam& param);
        virtual int set_child(int32_t child_idx, ObPhyOperator &child_operator);
        virtual int64_t to_string(char* buf, const int64_t buf_len) const;

      private:
        // disallow copy
        ObSstableScan(const ObSstableScan &other);
        ObSstableScan& operator=(const ObSstableScan &other);
      private:
        // data members
        //ObNewRange range_;
        common::ObRow curr_row_;
        common::ObRowDesc row_desc_;
    };
  } // end namespace sql
} // end namespace oceanbase

#endif /* _OB_SSTABLE_SCAN_H */

