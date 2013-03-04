/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_server_manager.h for manage servers. 
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */
#ifndef OCEANBASE_CLIENT_OB_SERVER_MANAGER_H_
#define OCEANBASE_CLIENT_OB_SERVER_MANAGER_H_

#include "common/ob_server.h"

namespace oceanbase 
{
  namespace client 
  {
    class ObServerManager
    {
    public:
      ObServerManager();
      ~ObServerManager();

      int init(const int64_t servers_count);

      const common::ObServer& get_root_server() const;
      int set_root_server(const common::ObServer& root_server);

      const common::ObServer& get_update_server() const;
      int set_update_server(const common::ObServer& update_server);

      const common::ObServer& get_random_merge_server() const;
      int add_merge_server(const common::ObServer& merge_server);

    private:
      DISALLOW_COPY_AND_ASSIGN(ObServerManager);

      int64_t servers_count_;
      int64_t cur_merge_server_idx_;
      common::ObServer* servers_;
    };
  } // namespace oceanbase::client
} // namespace Oceanbase

#endif //OCEANBASE_CLIENT_OB_SERVER_MANAGER_H_
