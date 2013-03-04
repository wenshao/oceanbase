/**
 * (C) 2010-2011 Taobao Inc.
 *  
 *  This program is free software; you can redistribute it
 *  and/or modify it under the terms of the GNU General Public
 *  License version 2 as published by the Free Software
 *  Foundation.
 *
 *  ob_scan_column_indexs.h is for what ...
 *
 *  Authors:
 *     qushan <qushan@taobao.com>
 *     huating <huating.zmq@taobao.com>
 *       
 */
#ifndef OCEANBASE_SSTABLE_SCAN_COLUMN_INDEXES_H_
#define OCEANBASE_SSTABLE_SCAN_COLUMN_INDEXES_H_

#include <tblog.h>
#include "common/ob_define.h"
#include "common/ob_malloc.h"

namespace oceanbase 
{ 
  namespace sstable 
  {
    class ObScanColumnIndexes
    {
      struct ObColumnInfo
      {
        int64_t column_index_;
        uint64_t column_id_;
      };

      public:
        static const int64_t NOT_EXIST_COLUMN = INT64_MAX;
        static const int64_t ROWKEY_COLUMN = INT64_MAX - 1;
        static const int64_t INVALID_COLUMN = INT64_MAX - 2;

      public:
        ObScanColumnIndexes(const int64_t column_size = common::OB_MAX_COLUMN_NUMBER)
        : column_cnt_(0), column_info_size_(column_size), column_info_(NULL)
        {

        }

        ~ObScanColumnIndexes()
        {
          if (NULL != column_info_)
          {
            common::ob_free(column_info_);
          }
        }

      public:
        inline void reset()
        {
          memset(column_info_, 0, sizeof(ObColumnInfo) * column_cnt_);
          column_cnt_ = 0;
        }

        inline int add_column_id(const int64_t index, const uint64_t column_id)
        {
          int ret = common::OB_SUCCESS;

          if (index < 0 || 0 == column_id || common::OB_INVALID_ID == column_id)
          {
            ret = common::OB_INVALID_ARGUMENT;
          }
          else if (column_cnt_ >= column_info_size_)
          {
            ret = common::OB_SIZE_OVERFLOW;
          }
          else
          {
            if (NULL == column_info_)
            {
              column_info_ = reinterpret_cast<ObColumnInfo*>(
                common::ob_malloc(sizeof(ObColumnInfo) * column_info_size_));
              if (NULL == column_info_)
              {
                TBSYS_LOG(WARN, "failed to allocate memory for column index array");
                ret = common::OB_ALLOCATE_MEMORY_FAILED;
              }
            }

            if (common::OB_SUCCESS == ret && NULL != column_info_)
            {
              column_info_[column_cnt_].column_index_ = index;
              column_info_[column_cnt_].column_id_ = column_id;
              ++column_cnt_;
            }
          }
    
          if (common::OB_SUCCESS != ret)
          {
            TBSYS_LOG(ERROR, "add column error, ret = %d, index=%ld, column_id=%lu,"
                "column_size=%ld, max_column_size=%ld", 
              ret , index, column_id, column_cnt_, column_info_size_);
          }
    
          return ret;
        }

        inline int get_column_index(const int64_t index, int64_t& column_index) const
        {
          int ret = common::OB_SUCCESS;
    
          if (index >= 0 && index < column_cnt_)
          {
            column_index = column_info_[index].column_index_;
          }
          else
          {
            column_index = INVALID_COLUMN;
            ret = common::OB_INVALID_ARGUMENT;
          }
    
          return ret;
        }

        inline int get_column_id(const int64_t index, uint64_t& column_id) const
        {
          int ret = common::OB_SUCCESS;
    
          if (index >= 0 && index < column_cnt_)
          {
            column_id = column_info_[index].column_id_;
          }
          else
          {
            column_id = common::OB_INVALID_ID;
            ret = common::OB_INVALID_ARGUMENT;
          }
    
          return ret;
        }

        inline int get_column(const int64_t index, uint64_t& column_id, 
          int64_t& column_index) const
        {
          int ret = common::OB_SUCCESS;
    
          if (index >= 0 && index < column_cnt_)
          {
            column_index = column_info_[index].column_index_;
            column_id = column_info_[index].column_id_;
          }
          else
          {
            column_id = common::OB_INVALID_ID;
            column_index = INVALID_COLUMN;
            ret = common::OB_INVALID_ARGUMENT;
          }
    
          return ret;
        }

        inline int64_t get_column_count() const 
        { 
          return column_cnt_;
        }

      private:
        int64_t column_cnt_;
        int64_t column_info_size_;
        ObColumnInfo* column_info_;
    };

  } // end namespace sstable
} // end namespace oceanbase

#endif //OCEANBASE_SSTABLE_SCAN_COLUMN_INDEXES_H_
