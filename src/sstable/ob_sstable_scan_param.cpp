/**
 *  (C) 2010-2011 Taobao Inc.
 *  
 *  This program is free software; you can redistribute it
 *  and/or modify it under the terms of the GNU General Public
 *  License version 2 as published by the Free Software
 *  Foundation.
 *
 *  ob_sstable_scan_param.cpp is for what ...
 *
 *  Authors:
 *     qushan < qushan@taobao.com >
 *        
 */


#include "ob_sstable_scan_param.h"
using namespace oceanbase::common;
namespace oceanbase
{
  namespace sstable
  {
    ObSSTableScanParam::ObSSTableScanParam()
    {
      reset();
    }
    ObSSTableScanParam::~ObSSTableScanParam()
    {
    }

    void ObSSTableScanParam::reset()
    {
      memset(column_ids_, 0, sizeof(column_ids_));
      column_id_list_.init(OB_MAX_COLUMN_NUMBER, column_ids_);
    }

    int ObSSTableScanParam::assign(const common::ObScanParam & param)
    {
      int ret = OB_SUCCESS;


      int64_t column_id_size = param.get_column_id_size();
      if (column_id_size > OB_MAX_COLUMN_NUMBER)
      {
        ret = OB_SIZE_OVERFLOW;
      }
      else
      {
        // copy base class members
        set_is_result_cached(param.get_is_result_cached());
        set_version_range(param.get_version_range());
        scan_direction_ = param.get_scan_direction();
        read_mode_ = param.get_read_mode();
        // copy range, donot copy start_key_ & end_key_, 
        // suppose they will not change in scan process.
        range_ = *param.get_range();
        range_.table_id_ = param.get_table_id();
        const uint64_t* const column_id_begin = param.get_column_id();
        memcpy(column_ids_, column_id_begin, column_id_size * sizeof(uint64_t));
        column_id_list_.init(OB_MAX_COLUMN_NUMBER, column_ids_, column_id_size);
      }
      return ret;
    }

    bool ObSSTableScanParam::is_valid() const
    {
      int ret = true;
      if (range_.empty())
      {
        char range_buf[OB_RANGE_STR_BUFSIZ];
        range_.to_string(range_buf, OB_RANGE_STR_BUFSIZ);
        TBSYS_LOG(ERROR, "range=%s is empty, cannot find any key.", range_buf);
        ret = false;
      }
      else if (column_id_list_.get_array_index() <= 0)
      {
        TBSYS_LOG(ERROR, "query not request any columns=%ld", 
            column_id_list_.get_array_index());
        ret = false;
      }
      return ret;
    }

  }
}

