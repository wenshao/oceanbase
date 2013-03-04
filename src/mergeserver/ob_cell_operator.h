/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * ob_cell_operator.h is for ObCellInfo operation
 *
 * Version: $id: ob_cell_operator.h,v 0.1 9/25/2010 3:48p wushi Exp $
 *
 * Authors:
 *   wushi <wushi.ly@taobao.com>
 *     - some work details if you want
 *
 */
#ifndef OCEANBASE_MERGESERVER_OB_CELL_OPERATOR_H_ 
#define OCEANBASE_MERGESERVER_OB_CELL_OPERATOR_H_
#include "common/ob_read_common_data.h"
#include "common/ob_object.h"
#include "common/ob_string.h"
namespace oceanbase
{
  namespace mergeserver
  {
    /// @fn apply mutation in src to dst
    int ob_cell_info_apply(common::ObCellInfo &dst, const common::ObCellInfo &src);
  }
}




#endif /* OCEANBASE_MERGESERVER_OB_CELL_OPERATOR_H_ */ 
