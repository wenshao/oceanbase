/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_operator_factory.h
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@taobao.com>
 *
 */

#ifndef _OB_OPERATOR_FACTORY_H
#define _OB_OPERATOR_FACTORY_H 1

#include "sql/ob_sstable_scan.h"
#include "sql/ob_ups_scan.h"
#include "sql/ob_ups_multi_get.h"

namespace oceanbase
{
  namespace sql
  {
    class ObOperatorFactory
    {
      public:
        virtual ~ObOperatorFactory() { }

        virtual ObSstableScan *new_sstable_scan() = 0;
        virtual ObUpsScan *new_ups_scan() = 0;
        virtual ObUpsMultiGet *new_ups_multi_get() = 0;
    };
  }
}

#endif /* _OB_OPERATOR_FACTORY_H */

