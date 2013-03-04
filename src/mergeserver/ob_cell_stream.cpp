/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * ob_cell_stream.cpp is for what ...
 *
 * Version: $id: ob_cell_stream.cpp,v 0.1 10/11/2010 3:51p wushi Exp $
 *
 * Authors:
 *   wushi <wushi.ly@taobao.com>
 *     - some work details if you want
 *
 */

#include "ob_cell_stream.h"
using namespace oceanbase::common;
using namespace oceanbase::mergeserver;

ObMSGetCellArray::ObMSGetCellArray():join_cell_vec_(NULL), get_param_(NULL)
{
}

ObMSGetCellArray::ObMSGetCellArray(const ObVector<ObSimpleRightJoinCell> & join_cell_vec)
:join_cell_vec_(&join_cell_vec),get_param_(NULL)
{
}


ObMSGetCellArray::ObMSGetCellArray(const ObGetParam & get_param)
:join_cell_vec_(NULL),get_param_(&get_param)
{
}

const ObCellInfo & ObMSGetCellArray::operator [](const int64_t offset) const
{
  if (NULL != get_param_)
  {
    if (offset < get_param_->get_cell_size())
    {
      fake_seq_access_cell_ = *(get_param_->operator [](offset));
    }
    else
    {
      TBSYS_LOG(ERROR, "logic error, offset out of range [offset:%ld,cell_num:%ld]",
                offset,get_param_->get_cell_size());
      fake_seq_access_cell_ = default_cell_;
    }
  }
  else if (NULL != join_cell_vec_)
  {
    fake_seq_access_cell_.column_id_ = 0;
    fake_seq_access_cell_.table_id_ = (join_cell_vec_->operator[](static_cast<int32_t>(offset))).table_id;
    fake_seq_access_cell_.row_key_ = join_cell_vec_->operator[](static_cast<int32_t>(offset)).rowkey;
  }
  else
  {
    TBSYS_LOG(ERROR, "logic error, offset out of range [offset:%ld,cell_num:%d]", offset,0);
    fake_seq_access_cell_ = default_cell_;
  }
  return fake_seq_access_cell_;
}
