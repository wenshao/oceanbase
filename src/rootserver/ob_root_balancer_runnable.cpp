/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 * 
 * Version: $Id$
 *
 * ob_root_balancer_runnable.cpp
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#include "ob_root_balancer_runnable.h"
#include "tbsys.h"
using namespace oceanbase::rootserver;
using namespace oceanbase::common;

ObRootBalancerRunnable::ObRootBalancerRunnable(ObRootConfig &config, ObRootBalancer &balancer, common::ObRoleMgr &role_mgr)
  :config_(config), balancer_(balancer), role_mgr_(role_mgr)
{
}

ObRootBalancerRunnable::~ObRootBalancerRunnable()
{
}

void ObRootBalancerRunnable::wakeup()
{
  balance_worker_sleep_cond_.broadcast();
}

bool ObRootBalancerRunnable::is_master() const
{
  return role_mgr_.is_master();
}

void ObRootBalancerRunnable::run(tbsys::CThread *thread, void *arg)
{
  UNUSED(thread);
  UNUSED(arg);
  TBSYS_LOG(INFO, "[NOTICE] balance worker thread start, waiting_seconds=%d", 
            config_.flag_migrate_wait_seconds_.get());
  for (int i = 0; i < config_.flag_migrate_wait_seconds_.get() && !_stop; i++)
  {
    sleep(1);
  }

  TBSYS_LOG(INFO, "[NOTICE] balance working");
  bool did_migrating = false;
  while (!_stop)
  {
    if (is_master() || role_mgr_.get_role() == ObRoleMgr::STANDALONE)
    {
      if (config_.flag_enable_balance_.get() 
          || config_.flag_enable_rereplication_.get())
      {
        balancer_.do_balance(did_migrating);
      }
    }
    else
    {
      TBSYS_LOG(DEBUG, "not the master");
    }
    int64_t sleep_us = 0;
    if (did_migrating)
    {
      sleep_us = MIN_BALANCE_WORKER_SLEEP_US;
    }
    else
    {
      // idle
      sleep_us = config_.flag_balance_worker_idle_sleep_seconds_.get() * 1000000LL;
      TBSYS_LOG(INFO, "balance worker idle, sleep_us=%ld", sleep_us);
    }
    int sleep_ms = static_cast<int32_t>(sleep_us/1000);
    balance_worker_sleep_cond_.wait(sleep_ms);
  } // end while
  TBSYS_LOG(INFO, "[NOTICE] balance worker thread exit");
}

