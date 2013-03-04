/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_tablet_fuse.h
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#ifndef _OB_TABLET_FUSE_H
#define _OB_TABLET_FUSE_H 1

#include "ob_rowkey_phy_operator.h"
#include "common/ob_ups_row.h"
#include "common/ob_range.h"

namespace oceanbase
{
  using namespace common;

  namespace sql
  {
    // 用于CS从合并sstable中的静态数据和UPS中的增量数据
    class ObTabletFuse: public ObPhyOperator
    {
      public:
        ObTabletFuse();
        virtual ~ObTabletFuse();

        int set_child(int32_t child_idx, ObPhyOperator &child_operator);
        int set_sstable_scan(ObRowkeyPhyOperator *sstable_scan);
        int set_incremental_scan(ObRowkeyPhyOperator *incremental_scan);

        int open();
        int close();
        int get_next_row(const ObRow *&row);
        int64_t to_string(char* buf, const int64_t buf_len) const;
        int get_row_desc(const common::ObRowDesc *&row_desc) const {row_desc=NULL;return OB_NOT_IMPLEMENT;}
      private:
        // disallow copy
        ObTabletFuse(const ObTabletFuse &other);
        ObTabletFuse& operator=(const ObTabletFuse &other);

        int compare_rowkey(const ObString &rowkey1, const ObString &rowkey2);
        bool check_inner_stat();

      private:
        // data members
        ObRowkeyPhyOperator *sstable_scan_; // 从sstable读取的静态数据
        ObRowkeyPhyOperator *incremental_scan_; // 从UPS读取的增量数据
        const ObRow *last_sstable_row_;
        const ObUpsRow *last_incr_row_;
        const ObString *sstable_rowkey_;
        const ObString *incremental_rowkey_;
        ObRow curr_row_;
    };
  } // end namespace sql
} // end namespace oceanbase

#endif /* _OB_TABLET_FUSE_H */
