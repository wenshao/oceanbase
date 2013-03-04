/**
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * Authors:
 *   yanran <yanran.hfs@taobao.com>
 *     - some work details if you want
 */

#include "ob_ups_slave_mgr.h"
#include "common/ob_malloc.h"
#include "common/utility.h"
using namespace oceanbase::updateserver;

static const int64_t DEFAULT_NETWORK_TIMEOUT = 1000 * 1000;

ObUpsSlaveMgr::ObUpsSlaveMgr()
{
  is_initialized_ = false;
  slave_num_ = 0;
  rpc_stub_ = NULL;
  role_mgr_ = NULL;
}

ObUpsSlaveMgr::~ObUpsSlaveMgr()
{
  ServerNode* node = NULL;
  ObDLink* p = slave_head_.server_list_link.next();
  while (p != &slave_head_.server_list_link)
  {
    node = (ServerNode*)p;
    p = p->next();
    p->prev()->remove();
    ob_free(node);
  }
}

int ObUpsSlaveMgr::init(ObUpsRoleMgr *role_mgr,
    ObCommonRpcStub *rpc_stub, int64_t log_sync_timeout)
{
  int err = OB_SUCCESS;
  if (NULL == role_mgr || NULL == rpc_stub)
  {
    err = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "invalid argument, role_mgr_=%p, rpc_stub=%p", role_mgr, rpc_stub);
  }
  else
  {
    log_sync_timeout_ = log_sync_timeout;
    role_mgr_ = role_mgr;
    rpc_stub_ = rpc_stub;
    is_initialized_ = true;
  }
  return err;
}

int ObUpsSlaveMgr::send_data(const char* data, const int64_t length)
{
  int ret = check_inner_stat();
  ObDataBuffer send_buf;

  if (OB_SUCCESS == ret)
  {
    if (NULL == data || length < 0)
    {
      TBSYS_LOG(ERROR, "parameters are invalid[data=%p length=%ld]", data, length);
      ret = OB_INVALID_ARGUMENT;
    }
    else
    {
      send_buf.set_data(const_cast<char*>(data), length);
      send_buf.get_position() = length;
    }
  }

  slave_info_mutex_.lock();

  if (OB_SUCCESS == ret)
  {
    ServerNode* slave_node = NULL;
    ObDLink* p = slave_head_.server_list_link.next();
    while (OB_SUCCESS == ret && p != NULL && p != &slave_head_.server_list_link)
    {
      slave_node = (ServerNode*)(p);
      int err = 0;

      err = rpc_stub_->send_log(slave_node->server, send_buf, log_sync_timeout_);
      if (OB_SUCCESS != err)
      {
        if (ObUpsRoleMgr::MASTER != role_mgr_->get_role())
        {
          // 先改角色为备，然后等待当前的写事务完成，所以这种情况不是错误
          TBSYS_LOG(WARN, "no longer be master, maybe UPS are swithing to slave.");
          p = p->next();
        }
        else
        {
          char addr_buf[BUFSIZ];
          if (!slave_node->server.to_string(addr_buf, BUFSIZ))
          {
            strcpy(addr_buf, "Get Server IP failed");
          }
          TBSYS_LOG(WARN, "send_data to slave[%s] error[err=%d]", addr_buf, err);

          //add: report to rootserver
          err = rpc_stub_->ups_report_slave_failure(slave_node->server, DEFAULT_NETWORK_TIMEOUT);
          if (OB_SUCCESS != err)
          {
            TBSYS_LOG(WARN, "fail to report slave failure. err = %d", err);
          }
          ObDLink *to_del = p;
          p = p->next();
          to_del->remove();
          slave_num_ --;
          ob_free(slave_node);
        }
      }
      else
      {
        p = p->next();
      }
    } // end of loop

    if (NULL == p)
    {
      TBSYS_LOG(ERROR, "Server list encounter NULL pointer, this should not be reached");
      ret = OB_ERROR;
    }
  }

  slave_info_mutex_.unlock();
  return ret;
}

int ObUpsSlaveMgr::grant_keep_alive()
{
  int err = OB_SUCCESS;
  ServerNode* slave_node = NULL;
  ObDLink *p = slave_head_.server_list_link.next();
  timeval time_val;
  gettimeofday(&time_val, NULL);
  int64_t cur_time_us = time_val.tv_sec * 1000 * 1000 + time_val.tv_usec;
  while(p != NULL && p != &slave_head_.server_list_link)
  {
    slave_node = (ServerNode*)(p);
    TBSYS_LOG(DEBUG, "send keep alive msg to slave[%s], time=%ld", slave_node->server.to_cstring(), cur_time_us);
    err = rpc_stub_->send_keep_alive(slave_node->server);
    if (OB_SUCCESS != err)
    {
      TBSYS_LOG(WARN, "fail to send keep alive msg to slave[%s], err = %d", slave_node->server.to_cstring(), err);
    }
    p = p->next();
  }
  return err;
}

int ObUpsSlaveMgr::add_server(const ObServer &server)
{
  return ObSlaveMgr::add_server(server);
}
int ObUpsSlaveMgr::delete_server(const ObServer &server)
{
  return ObSlaveMgr::delete_server(server);
}

int ObUpsSlaveMgr::reset_slave_list()
{
  return ObSlaveMgr::reset_slave_list();
}
int ObUpsSlaveMgr::get_num() const
{
  return ObSlaveMgr::get_num();
}
int ObUpsSlaveMgr::set_send_log_point(const ObServer &server, const uint64_t send_log_point)
{
  return ObSlaveMgr::set_send_log_point(server, send_log_point);
}
void ObUpsSlaveMgr::print(char *buf, const int64_t buf_len, int64_t& pos)
{
  return ObSlaveMgr::print(buf, buf_len, pos);
}
