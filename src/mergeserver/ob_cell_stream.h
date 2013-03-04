/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * ob_cell_stream.h is for what ...
 *
 * Version: $id: ob_cell_stream.h,v 0.1 9/17/2010 11:31a wushi Exp $
 *
 * Authors:
 *   wushi <wushi.ly@taobao.com>
 *     - some work details if you want
 *
 */

#ifndef OCEANBASE_MERGESERVER_OB_CELL_STREAM_H_ 
#define OCEANBASE_MERGESERVER_OB_CELL_STREAM_H_

#include "common/ob_iterator.h"
#include "common/ob_cache.h"
#include "common/ob_cell_array.h"
#include "common/ob_read_common_data.h"
#include "ob_row_cell_vec.h"
#include "ob_ms_tablet_location_item.h"
#include "common/ob_simple_right_join_cell.h"

namespace oceanbase
{
  namespace mergeserver
  {
    class ObMSGetCellArray
    {
      public:
        ObMSGetCellArray();
        ObMSGetCellArray(const common::ObVector<common::ObSimpleRightJoinCell> &join_cell_vec);
        ObMSGetCellArray(const common::ObGetParam & get_param);
        virtual ~ObMSGetCellArray()
        {
        }

        /// @fn get cell number
        int64_t get_cell_size() const
        {
          int64_t result = 0;
          if (NULL != get_param_)
          {
            result = get_param_->get_cell_size();
          }
          else if (NULL != join_cell_vec_)
          {
            result = join_cell_vec_->size();
          }
          else
          {
            result = 0;
          }
          return result;
        }

        /// @fn get cell according to operator []
        const common::ObCellInfo &  operator[](const int64_t offset)const;

      private:
        mutable common::ObCellInfo fake_seq_access_cell_;
        const common::ObVector<common::ObSimpleRightJoinCell> *join_cell_vec_;
        const common::ObGetParam *get_param_;
        common::ObCellInfo default_cell_;
    };

    typedef enum ServerType
    {
      CHUNK_SERVER = 100,
      UPDATE_SERVER = 101,
      UNKNOW_SERVER = 102,
    } server_type;
  }
}


#endif /* MERGESERVER_OB_CELL_STREAM_H_ */
