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

#include "ob_ups_replay_runnable.h"

#include "ob_ups_log_mgr.h"
#include "common/utility.h"
#include "ob_update_server.h"
#include "ob_update_server_main.h"
using namespace oceanbase::common;
using namespace oceanbase::updateserver;

ObUpsReplayRunnable::ObUpsReplayRunnable()
{
  is_initialized_ = false;
  replay_wait_time_us_ = DEFAULT_REPLAY_WAIT_TIME_US;
  fetch_log_wait_time_us_ = DEFAULT_FETCH_LOG_WAIT_TIME_US;
}

ObUpsReplayRunnable::~ObUpsReplayRunnable()
{
}

int ObUpsReplayRunnable::init(ObUpsLogMgr* log_mgr, ObiRole *obi_role, ObUpsRoleMgr *role_mgr)
{
  int ret = OB_SUCCESS;
  if (is_initialized_)
  {
    TBSYS_LOG(ERROR, "ObLogReplayRunnable has been initialized"); 
    ret = OB_INIT_TWICE;
  }

  if (OB_SUCCESS == ret)
  {
    if (NULL == log_mgr || NULL == role_mgr || NULL == obi_role)
    {
      TBSYS_LOG(ERROR, "Parameter is invalid[obi_role=%p][role_mgr=%p]", obi_role, role_mgr);
    }
  }
  if (OB_SUCCESS == ret)
  {
    log_mgr_ = log_mgr;
    role_mgr_ = role_mgr;
    obi_role_ = obi_role;
    is_initialized_ = true;
  }
  return ret;
}

void ObUpsReplayRunnable::clear()
{
  if (NULL != _thread)
  {
    delete[] _thread;
    _thread = NULL;
  }
}

void ObUpsReplayRunnable::stop()
{
  _stop = true;
}

bool ObUpsReplayRunnable::wait_stop()
{
  return switch_.wait_off();
}

bool ObUpsReplayRunnable::wait_start()
{
  return switch_.wait_on();
}

void ObUpsReplayRunnable::run(tbsys::CThread* thread, void* arg)
{
  int err = OB_SUCCESS;
  UNUSED(thread);
  UNUSED(arg);

  TBSYS_LOG(INFO, "ObUpsLogReplayRunnable start to run");
  if (!is_initialized_)
  {
    TBSYS_LOG(ERROR, "ObUpsLogReplayRunnable has not been initialized");
    err = OB_NOT_INIT;
  }
  while (!_stop && (OB_SUCCESS == err || OB_NEED_RETRY == err || OB_NEED_WAIT == err))
  {
    if (switch_.check_off(log_mgr_->has_nothing_in_buf_to_replay()))
    {
      usleep(static_cast<useconds_t>(replay_wait_time_us_));
    }
    else if (OB_SUCCESS != (err = log_mgr_->replay_log())
        && OB_NEED_RETRY != err && OB_NEED_WAIT != err)
    {
      TBSYS_LOG(ERROR, "log_mgr.replay()=>%d", err);
    }
    else if (OB_NEED_RETRY == err)
    {
      usleep(static_cast<useconds_t>(replay_wait_time_us_));
    }
    else if (OB_NEED_WAIT == err)
    {
      usleep(static_cast<useconds_t>(fetch_log_wait_time_us_));
    }
  }
  TBSYS_LOG(INFO, "ObLogReplayRunnable finished[stop=%d ret=%d]", _stop, err); 
}
