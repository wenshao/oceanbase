/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: ob_ms_read_balance.h,v 0.1 2011/10/24 17:31:30 zhidong Exp $
 *
 * Authors:
 *   xielun.szd <xielun.szd@taobao.com>
 *     - some work details if you want
 *
 */

#ifndef OCEANBASE_MERGER_READ_BALANCE_H_
#define OCEANBASE_MERGER_READ_BALANCE_H_

#include "common/ob_define.h"
#include "common/ob_ups_info.h"

namespace oceanbase
{
  namespace common
  {
    class ObReadParam;
  }
  
  namespace mergeserver
  {
    /// read load balance stratege utility class
    class ObMergerReadBalance
    {
    public:
      ObMergerReadBalance();
      ~ObMergerReadBalance();

    public:
      // default sum percent 100
      const static int32_t DEFAULT_PERCENT = 100;
      
      /// select a server from server list according the read flow throughput access ratio
      static int32_t select_server(const common::ObUpsList & list, const common::ObReadParam & param, 
          const common::ObServerType type = common::MERGE_SERVER);
    };
  }
}



#endif //OCEANBASE_MERGER_READ_BALANCE_H_


