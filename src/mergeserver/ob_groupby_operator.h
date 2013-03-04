/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * ob_groupby_operator.h is for what ...
 *
 * Version: $id: ob_groupby_operator.h,v 0.1 3/28/2011 10:22a wushi Exp $
 *
 * Authors:
 *   wushi <wushi.ly@taobao.com>
 *     - some work details if you want
 *
 */
#ifndef MERGESERVER_OB_GROUPBY_OPERATOR_H_ 
#define MERGESERVER_OB_GROUPBY_OPERATOR_H_
#include "common/ob_cell_array.h"
#include "common/hash/ob_hashmap.h"
#include "common/ob_groupby.h"
namespace oceanbase
{
  namespace mergeserver
  {
    class ObGroupByOperator : public oceanbase::common::ObCellArray
    {
    public:
      ObGroupByOperator();
      ~ObGroupByOperator();

      void clear();

      int init(const oceanbase::common::ObGroupByParam & param, const int64_t max_avail_mem_size); 
      /// add an org row [row_beg,row_end]
      int add_row(const oceanbase::common::ObCellArray & org_cells, const int64_t row_beg, const int64_t row_end);
      int init_all_in_one_group_row();
    private:
      static const int64_t HASH_SLOT_NUM = 256L << 10;
      const oceanbase::common::ObGroupByParam *param_;
      oceanbase::common::hash::ObHashMap<oceanbase::common::ObGroupKey,int64_t,oceanbase::common::hash::NoPthreadDefendMode> group_hash_map_;
      bool inited_;
      int64_t max_avail_mem_size_;
    };
  }
}  
#endif /* MERGESERVER_OB_GROUPBY_OPERATOR_H_ */
