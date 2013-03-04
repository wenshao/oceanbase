/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_server_manager.cpp for manage servers. 
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */
#include <stdlib.h>
#include <tblog.h>
#include "common/ob_malloc.h"
#include "ob_server_manager.h"

namespace oceanbase 
{ 
  namespace client 
  {
    using namespace common;

    ObServerManager::ObServerManager() 
    : servers_count_(0), cur_merge_server_idx_(0), servers_(NULL)
    {

    }

    ObServerManager::~ObServerManager()
    {
      if (NULL != servers_)
      {
        ob_free(servers_);
        servers_ = NULL;
      }
    }

    int ObServerManager::init(const int64_t servers_count)
    {
      int ret = OB_SUCCESS;

      if (servers_count <= 2)
      {
        TBSYS_LOG(WARN, "invalid param, servers_count=%ld", servers_count);
        ret = OB_ERROR;
      }
      else if (NULL != servers_)
      {
        TBSYS_LOG(WARN, "server manager has been inited, servers_=%p", servers_);
        ret = OB_ERROR;        
      }

      if (OB_SUCCESS == ret)
      {
        servers_ = reinterpret_cast<ObServer*>
          (ob_malloc(servers_count * sizeof(ObServer)));
        if (NULL == servers_)
        {
          TBSYS_LOG(ERROR, "failed allocate for servers array.");
          ret = OB_ERROR;
        }
        else
        {
          memset(servers_, 0, servers_count * sizeof(ObServer));
          servers_count_ = servers_count;
          cur_merge_server_idx_ = 2;
        }
      }

      return ret;
    }

    const ObServer& ObServerManager::get_root_server() const
    {
      return servers_[0];
    }

    int ObServerManager::set_root_server(const ObServer& root_server)
    {
      int ret = OB_SUCCESS;

      if (root_server.get_ipv4() == 0 || root_server.get_port() == 0)
      {
        TBSYS_LOG(WARN, "invalid root server, ip=%d, port=%d",
                  root_server.get_ipv4(), root_server.get_port());
        ret = OB_ERROR;
      }

      if (OB_SUCCESS == ret)
      {
        servers_[0] = root_server;
      }

      return ret;
    }

    const ObServer& ObServerManager::get_update_server() const
    {
      return servers_[1];
    }

    int ObServerManager::set_update_server(const ObServer& update_server)
    {
      int ret = OB_SUCCESS;

      if (update_server.get_ipv4() == 0 || update_server.get_port() == 0)
      {
        TBSYS_LOG(WARN, "invalid update server, ip=%d, port=%d",
                  update_server.get_ipv4(), update_server.get_port());
        ret = OB_ERROR;
      }

      if (OB_SUCCESS == ret)
      {
        servers_[1] = update_server;
      }

      return ret;
    }

    const ObServer& ObServerManager::get_random_merge_server() const
    {
      return servers_[random() % (servers_count_ - 2) + 2];
    }

    int ObServerManager::add_merge_server(const ObServer& merge_server)
    {
      int ret = OB_SUCCESS;

      if (merge_server.get_ipv4() == 0 || merge_server.get_port() == 0)
      {
        TBSYS_LOG(WARN, "invalid merge server, ip=%d, port=%d",
                  merge_server.get_ipv4(), merge_server.get_port());
        ret = OB_ERROR;
      }
      else if (cur_merge_server_idx_ >= servers_count_)
      {
        TBSYS_LOG(WARN, "no space to store merge server any more, "
                        "cur_merge_server_idx_=%ld, servers_count_=%ld",
                  cur_merge_server_idx_, servers_count_);
        ret = OB_ERROR;
      }

      if (OB_SUCCESS == ret)
      {
        servers_[cur_merge_server_idx_++] = merge_server;
      }

      return ret;
    }
  } // end namespace client
} // end namespace oceanbase
