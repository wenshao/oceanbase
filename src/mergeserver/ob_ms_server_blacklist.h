/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: ob_ms_server_blacklist.h,v 0.1 2011/11/13 17:31:30 zhidong Exp $
 *
 * Authors:
 *   xielun.szd <xielun.szd@taobao.com>
 *     - some work details if you want
 *
 */

#ifndef OCEANBASE_MERGER_SERVER_BLACKLIST_H_
#define OCEANBASE_MERGER_SERVER_BLACKLIST_H_

#include "common/ob_define.h"
#include "common/ob_ups_info.h"

namespace oceanbase
{
  namespace mergeserver
  {
    // not thread safe
    class ObMergerServerBlackList
    {
    public:
      ObMergerServerBlackList();
      virtual ~ObMergerServerBlackList();

    public:
      // set max fail count for into error status
      int init(const int32_t max_fail_count, const int64_t timeout,
          const common::ObServerType type, const common::ObUpsList & list);
      // check the server is in error status
      bool check(const int32_t server_index);
      // inc the server failed count
      void fail(const int32_t server_index, const common::ObServer & server);
      // get valid count
      int32_t get_valid_count(void) const;
      // reset all the status to ok
      void reset(void);
      //
      struct ServerStatus
      {
        int32_t fail_count_;
        int64_t fail_timestamp_;
        common::ObServer server_;
      };

    private:
      const static int32_t DEFAULT_COUNT = 100;
      const static int64_t DEFAULT_TIMEOUT = 60 * 1000 * 1000L;
      const static int32_t MAX_LIST_COUNT = 16;
      int32_t server_count_;
      int32_t max_fail_count_;
      int64_t black_timeout_;
      ServerStatus fail_counter_[MAX_LIST_COUNT];
    };
  }
}



#endif //OCEANBASE_MERGER_SERVER_BLACKLIST_H_

