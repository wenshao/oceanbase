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
 *   jianming <jianming.cjq@taobao.com>
 *     - some work details if you want
 *
 */

#ifndef OCEANBASE_MERGESERVER_OB_CHUNK_SERVER_H_
#define OCEANBASE_MERGESERVER_OB_CHUNK_SERVER_H_

#include "common/ob_server.h"

namespace oceanbase
{
  namespace mergeserver
  {
    struct ObChunkServer
    {
      enum RequestStatus {
        UNREQUESTED,
        REQUESTED
      };

      /// address of the server
      common::ObServer addr_;
      /// indicate if the current request has requested this server, 0 for requested, others for not
      enum RequestStatus status_;
    };
  }
}

#endif /* OCEANBASE_MERGESERVER_OB_CHUNK_SERVER_H_ */


