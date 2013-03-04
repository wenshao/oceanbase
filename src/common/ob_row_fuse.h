/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_row_fuse.h
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@taobao.com>
 *
 */

#ifndef _OB_ROW_FUSE_H
#define _OB_ROW_FUSE_H 1

#include "common/ob_row.h"
#include "ob_ups_row.h"

namespace oceanbase
{
  namespace common
  {
    class ObRowFuse
    {
      public:
        static int assign(const ObUpsRow &incr_row, ObRow &result);
        static int fuse_row(const ObUpsRow *incr_row, const ObRow *sstable_row, ObRow *result);
    };
  }
}

#endif /* _OB_ROW_FUSE_H */

