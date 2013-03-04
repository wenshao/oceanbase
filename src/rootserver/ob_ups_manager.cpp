/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_ups_manager.cpp
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#include "ob_ups_manager.h"
#include "common/ob_define.h"
#include <tbsys.h>

using namespace oceanbase::rootserver;
using namespace oceanbase::common;

void ObUps::reset()
{
  addr_.reset();
  inner_port_ = 0;
  stat_ = UPS_STAT_OFFLINE;
  log_seq_num_ = 0;
  lease_ = 0;
  ms_read_percentage_ = 0;
  cs_read_percentage_ = 0;
  did_renew_received_ = false;
}

ObUpsManager::ObUpsManager(ObRootRpcStub &rpc_stub, ObRootConfig &config, const ObiRole &obi_role)
  :config_(&config), rpc_stub_(rpc_stub), obi_role_(obi_role), ups_master_idx_(-1), waiting_ups_finish_time_(0),
  master_master_ups_read_percentage_(-1), slave_master_ups_read_percentage_(-1),
  is_flow_control_by_ip_(false)
{
}

ObUpsManager::~ObUpsManager()
{
}

int ObUpsManager::find_ups_index(const ObServer &addr) const
{
  int ret = -1;
  for (int32_t i = 0; i < MAX_UPS_COUNT; ++i)
  {
    if (ups_array_[i].addr_ == addr && UPS_STAT_OFFLINE != ups_array_[i].stat_)
    {
      ret = i;
      break;
    }
  }
  return ret;
}

bool ObUpsManager::did_ups_exist(const ObServer &addr) const
{
  return -1 != find_ups_index(addr);
}

bool ObUpsManager::is_ups_master(const ObServer &addr) const
{
  bool ret = false;
  int i = find_ups_index(addr);
  if (-1 != i)
  {
    ret = (UPS_STAT_MASTER == ups_array_[i].stat_);
  }
  return ret;
}

int ObUpsManager::register_ups(const ObServer &addr, int32_t inner_port, int64_t log_seq_num, int64_t lease)
{
  int ret = OB_SUCCESS;
  tbsys::CThreadGuard guard(&ups_array_mutex_);
  if (did_ups_exist(addr))
  {
    TBSYS_LOG(DEBUG, "the ups already registered, ups=%s", addr.to_cstring());
    ret = OB_ALREADY_REGISTERED;
  }
  else
  {
    ret = OB_SIZE_OVERFLOW;
    for (int32_t i = 0; i < MAX_UPS_COUNT; ++i)
    {
      if (UPS_STAT_OFFLINE == ups_array_[i].stat_)
      {
        int64_t now = tbsys::CTimeUtil::getTime();
        if (0 == waiting_ups_finish_time_)
        {
          // first ups register, we will waiting for some time before select the master
          waiting_ups_finish_time_ = now + config_->flag_ups_waiting_register_duration_us_.get();
          TBSYS_LOG(INFO, "first ups register, waiting_finish=%ld duration=%ld",
              waiting_ups_finish_time_, config_->flag_ups_waiting_register_duration_us_.get());
        }
        ups_array_[i].addr_ = addr;
        ups_array_[i].inner_port_ = inner_port;
        ups_array_[i].log_seq_num_ = log_seq_num;
        ups_array_[i].lease_ = now + config_->flag_ups_lease_us_.get();
        ups_array_[i].did_renew_received_ = true;
        ups_array_[i].cs_read_percentage_ = 0;
        ups_array_[i].ms_read_percentage_ = 0;
        if (lease > now)        // has valid lease
        {
          if (has_master())
          {
            char buff[OB_IP_STR_BUFF];
            buff[0] = '\0';
            ups_array_[ups_master_idx_].addr_.to_string(buff, OB_IP_STR_BUFF);
            TBSYS_LOG(WARN, "ups claimed to have the master lease but we ignore, addr=%s lease=%ld master=%s",
                addr.to_cstring(), lease, buff);
            ret = OB_CONFLICT_VALUE;
          }
          else
          {
            ups_array_[i].stat_ = UPS_STAT_MASTER;
            ups_master_idx_ = i;
            TBSYS_LOG(WARN, "ups claimed to have the master lease, addr=%s lease=%ld",
                addr.to_cstring(), lease);
            // master selected
            waiting_ups_finish_time_ = -1;
            ret = OB_SUCCESS;
          }
        }
        else
        {
          ups_array_[i].stat_ = UPS_STAT_NOTSYNC;
          ret = OB_SUCCESS;
        }
        TBSYS_LOG(INFO, "ups register, addr=%s inner_port=%d lsn=%ld lease=%ld",
            addr.to_cstring(), inner_port, log_seq_num, lease);
        reset_ups_read_percent();
        break;
      }
    }
  }
  return ret;
}

int ObUpsManager::renew_lease(const common::ObServer &addr, ObUpsStatus stat, const ObiRole &obi_role)
{
  int ret = OB_SUCCESS;
  tbsys::CThreadGuard guard(&ups_array_mutex_);
  int i = -1;
  if (-1 == (i = find_ups_index(addr)))
  {
    TBSYS_LOG(WARN, "not registered ups, addr=%s", addr.to_cstring());
  }
  else
  {
    ups_array_[i].did_renew_received_ = true;
    TBSYS_LOG(DEBUG, "renew lease, addr=%s stat=%d", addr.to_cstring(), stat);
    if ((UPS_STAT_SYNC == stat || UPS_STAT_NOTSYNC == stat)
        && (UPS_STAT_SYNC == ups_array_[i].stat_ || UPS_STAT_NOTSYNC == ups_array_[i].stat_))
    {
      if (ups_array_[i].stat_ != stat)
      {
        ups_array_[i].stat_ = stat;
        TBSYS_LOG(INFO, "ups change status, ups=%s stat=%d",
            addr.to_cstring(), ups_array_[i].stat_);
        if (!is_flow_control_by_ip_)
        {
          reset_ups_read_percent();
        }
      }
    }
    if (ObiRole::INIT != obi_role.get_role())
    {
      ups_array_[i].obi_role_ = obi_role;
    }
    else
    {
      TBSYS_LOG(WARN, "ups's obi role is INIT, ups=%s", addr.to_cstring());
    }
  }
  return ret;
}

int ObUpsManager::slave_failure(const common::ObServer &addr, const common::ObServer &slave_addr)
{
  int ret = OB_SUCCESS;
  tbsys::CThreadGuard guard(&ups_array_mutex_);
  int i = -1;
  if (!is_ups_master(addr))
  {
    TBSYS_LOG(WARN, "ups not exist or not master, addr=%s", addr.to_cstring());
    ret = OB_NOT_REGISTERED;
  }
  else if (-1 == (i = find_ups_index(slave_addr)))
  {
    TBSYS_LOG(WARN, "slave ups not exist, addr=%s", slave_addr.to_cstring());
    ret = OB_ENTRY_NOT_EXIST;
  }
  else
  {
    TBSYS_LOG(INFO, "ups master reporting slave ups failure, slave=%s", slave_addr.to_cstring());
    ups_array_[i].stat_ = UPS_STAT_OFFLINE;
    reset_ups_read_percent();
  }
  return ret;
}

int ObUpsManager::send_granting_msg(const common::ObServer &addr,
                                       const common::ObServer& master, int64_t lease)
{
  int ret = OB_SUCCESS;
  char buff[OB_IP_STR_BUFF];
  buff[0] = '\0';
  ret = rpc_stub_.grant_lease_to_ups(addr, master, lease, obi_role_);
  master.to_string(buff, OB_IP_STR_BUFF);
  TBSYS_LOG(DEBUG, "send lease to ups, ups=%s master=%s self_lease=%ld",
            addr.to_cstring(), buff, lease);
  return ret;
}

bool ObUpsManager::has_master() const
{
  return MAX_UPS_COUNT > ups_master_idx_
    && 0 <= ups_master_idx_;
}

bool ObUpsManager::need_grant(int64_t now, const ObUps &ups) const
{
  bool ret = false;
  if (ups.did_renew_received_)
  {
    if (now > ups.lease_ - config_->flag_ups_lease_reserved_us_.get()
        && now < ups.lease_)
    {
      // the lease of this ups' is going to expire
      ret = true;
    }
    // else if (master_lease > ups.sent_master_lease_)
    // {
    //   // the master's lease has been extended
    //   ret = true;
    // }
  }
  return ret;
}

int ObUpsManager::grant_lease(bool did_force /*=false*/)
{
  int ret = OB_SUCCESS;
  tbsys::CThreadGuard guard(&ups_array_mutex_);
  ObServer master;
  if (has_master())
  {
    master = ups_array_[ups_master_idx_].addr_;
  }
  int64_t now = tbsys::CTimeUtil::getTime();
  for (int32_t i = 0; i < MAX_UPS_COUNT; ++i)
  {
    if (UPS_STAT_OFFLINE != ups_array_[i].stat_)
    {
      if (need_grant(now, ups_array_[i]) || did_force)
      {
        if (!did_force)
        {
          ups_array_[i].did_renew_received_ = false;
          ups_array_[i].lease_ = now + config_->flag_ups_lease_us_.get();
        }
        int ret2 = send_granting_msg(ups_array_[i].addr_, master, ups_array_[i].lease_);
        if (OB_SUCCESS != ret2)
        {
          TBSYS_LOG(WARN, "grant lease to ups error, err=%d ups=%s",
                    ret2, ups_array_[i].addr_.to_cstring());
          // don't remove the ups right now
        }
      }
      else
      {
        TBSYS_LOG(DEBUG, "did_renew_received=%c ups=%s", ups_array_[i].did_renew_received_?'Y':'N',
                  ups_array_[i].addr_.to_cstring());
      }
    }
  }
  return ret;
}

int ObUpsManager::grant_eternal_lease()
{
  int ret = OB_SUCCESS;
  tbsys::CThreadGuard guard(&ups_array_mutex_);
  ObServer master;
  if (has_master())
  {
    master = ups_array_[ups_master_idx_].addr_;
  }
  for (int32_t i = 0; i < MAX_UPS_COUNT; ++i)
  {
    if (UPS_STAT_OFFLINE != ups_array_[i].stat_)
    {
      int ret2 = send_granting_msg(ups_array_[i].addr_, master, OB_MAX_UPS_LEASE_DURATION_US);
      if (OB_SUCCESS != ret2)
      {
        TBSYS_LOG(WARN, "grant lease to ups error, err=%d ups=%s",
                  ret2, ups_array_[i].addr_.to_cstring());
      }
    }
  } // end for
  return ret;
}

int ObUpsManager::select_ups_master_with_highest_lsn()
{
  int ret = OB_ERROR;
  if (-1 != ups_master_idx_)
  {
    TBSYS_LOG(WARN, "cannot select master when there is already one");
    ret = OB_UPS_MASTER_EXISTS;
  }
  else
  {
    int64_t highest_lsn = -1;
    int master_idx = -1;
    for (int32_t i = 0; i < MAX_UPS_COUNT; ++i)
    {
      if (UPS_STAT_OFFLINE != ups_array_[i].stat_)
      {
        if (ups_array_[i].log_seq_num_ > highest_lsn)
        {
          highest_lsn = ups_array_[i].log_seq_num_;
          master_idx = i;
        }
      }
    } // end for
    if (-1 == master_idx)
    {
      TBSYS_LOG(WARN, "no master selected");
    }
    else
    {
      ups_master_idx_ = master_idx;
      ups_array_[ups_master_idx_].stat_ = UPS_STAT_MASTER;
      TBSYS_LOG(INFO, "new ups master selected, master=%s lsn=%ld",
                ups_array_[ups_master_idx_].addr_.to_cstring(),
                ups_array_[ups_master_idx_].log_seq_num_);
      reset_ups_read_percent();
      ret = OB_SUCCESS;
    }
  }
  return ret;
}
void ObUpsManager::reset_ups_read_percent()
{
  for (int i = 0; i < MAX_UPS_COUNT; i++)
  {
    ups_array_[i].ms_read_percentage_ = 0;
    ups_array_[i].cs_read_percentage_ = 0;
  }
  is_flow_control_by_ip_ = false;
  int32_t ups_count = get_active_ups_count();
  int32_t master_read_percent = 100;
  int32_t slave_read_percent = 0;
  if (ups_count < 1)
  {
    TBSYS_LOG(DEBUG, "No active UpdateServer");
  }
  else
  {
    if (ups_count == 1)
    {
      master_read_percent = 100;
      slave_read_percent = 100;
    }
    else
    {
      if (-1 == ups_master_idx_
          || (ObiRole::MASTER == obi_role_.get_role()
            && -1 == master_master_ups_read_percentage_)
          || (ObiRole::MASTER != obi_role_.get_role()
            && -1 == slave_master_ups_read_percentage_))
      {
        master_read_percent = 100 / ups_count;
        slave_read_percent = 100 / ups_count;
      }
      else if (-1 != ups_master_idx_)
      {
        if (ObiRole::MASTER == obi_role_.get_role())
        {
          master_read_percent = master_master_ups_read_percentage_;
        }
        else
        {
          master_read_percent = slave_master_ups_read_percentage_;
        }
        slave_read_percent = (100 - master_read_percent) / (ups_count - 1);
      }
    }
  }
  for (int32_t i = 0; i < MAX_UPS_COUNT; ++i)
  {
    if (UPS_STAT_OFFLINE != ups_array_[i].stat_
        && UPS_STAT_MASTER != ups_array_[i].stat_
        && UPS_STAT_NOTSYNC != ups_array_[i].stat_)
    {
      ups_array_[i].ms_read_percentage_ = slave_read_percent;
      ups_array_[i].cs_read_percentage_ = slave_read_percent;
    }
    else if (UPS_STAT_MASTER == ups_array_[i].stat_)
    {
      ups_array_[i].ms_read_percentage_ = master_read_percent;
      ups_array_[i].cs_read_percentage_ = master_read_percent;
    }
    else if (UPS_STAT_NOTSYNC == ups_array_[i].stat_)
    {
      ups_array_[i].ms_read_percentage_ = 0;
      ups_array_[i].cs_read_percentage_ = 0;
    }
  }
}
void ObUpsManager::update_ups_lsn()
{
  for (int32_t i = 0; i < MAX_UPS_COUNT; ++i)
  {
    if (UPS_STAT_OFFLINE != ups_array_[i].stat_)
    {
      uint64_t lsn = 0;
      if (OB_SUCCESS != rpc_stub_.get_ups_max_log_seq(ups_array_[i].addr_, lsn, config_->flag_network_timeout_us_.get()))
      {
        TBSYS_LOG(WARN, "failed to get ups log seq, ups=%s", ups_array_[i].addr_.to_cstring());
      }
      else
      {
        ups_array_[i].log_seq_num_ = lsn;
      }
    } // end for
  }
}

bool ObUpsManager::is_master_lease_valid() const
{
  bool ret = false;
  if (has_master())
  {
    int64_t now = tbsys::CTimeUtil::getTime();
    ret = (ups_array_[ups_master_idx_].lease_ > now);
  }
  return ret;
}

int ObUpsManager::select_new_ups_master()
{
  int ret = OB_ERROR;
  if (-1 == ups_master_idx_ && !is_master_lease_valid())
  {
    this->update_ups_lsn();
    ret = this->select_ups_master_with_highest_lsn();
  }
  return ret;
}

void ObUpsManager::check_all_ups_offline()
{
  bool all_offline = true;
  for (int32_t i = 0; i < MAX_UPS_COUNT; ++i)
  {
    if (UPS_STAT_OFFLINE != ups_array_[i].stat_)
    {
      all_offline = false;
      break;
    }
  }
  if (all_offline)
  {
    TBSYS_LOG(INFO, "all UPS offline");
    waiting_ups_finish_time_ = 0;
  }
}

int ObUpsManager::check_lease()
{
  int ret = OB_SUCCESS;
  bool did_select_new_master = false;
  for (int32_t i = 0; i < MAX_UPS_COUNT; ++i)
  {
    tbsys::CThreadGuard guard(&ups_array_mutex_);
    int64_t now = tbsys::CTimeUtil::getTime();
    if (UPS_STAT_OFFLINE != ups_array_[i].stat_)
    {
      if (now > ups_array_[i].lease_ + MAX_CLOCK_SKEW_US)
      {
        TBSYS_LOG(INFO, "ups is offline, ups=%s lease=%ld lease_duration=%ld now=%ld",
            ups_array_[i].addr_.to_cstring(),
            ups_array_[i].lease_,
            config_->flag_ups_lease_us_.get(), now);
        // ups offline
        if (ups_array_[i].stat_ == UPS_STAT_MASTER)
        {
          ups_master_idx_ = -1;
          did_select_new_master = true;
        }
        TBSYS_LOG(INFO, "some ups is offline. reset read percentage");
        reset_ups_read_percent();
        ups_array_[i].reset();
        ups_array_[i].stat_ = UPS_STAT_OFFLINE;
        check_all_ups_offline();
      }
    }
  } // end for

  if (did_select_new_master)
  {
    tbsys::CThreadGuard guard(&ups_array_mutex_);
    // select new ups master
    int ret2 = select_new_ups_master();
    if (OB_SUCCESS != ret2)
    {
      TBSYS_LOG(WARN, "no master selected");
    }
  }

  if (did_select_new_master && has_master())
  {
    // send lease immediately to notify the change of master
    this->grant_lease(true);
  }
  return ret;
}

int ObUpsManager::check_ups_master_exist()
{
  int ret = OB_SUCCESS;
  tbsys::CThreadGuard guard(&ups_array_mutex_);
  if (0 < waiting_ups_finish_time_)
  {
    int64_t now = tbsys::CTimeUtil::getTime();
    if (now > waiting_ups_finish_time_)
    {
      if (OB_SUCCESS == (ret = select_ups_master_with_highest_lsn()))
      {
        waiting_ups_finish_time_ = -1;
      }
      else if (OB_UPS_MASTER_EXISTS == ret)
      {
        waiting_ups_finish_time_ = -1;
      }
    }
  }
  else if (0 > waiting_ups_finish_time_)
  {
    // check ups master exist
    ret = select_new_ups_master();
  }
  else
  {
    // 0 == waiting_ups_finish_time_ means all ups is offline, do nothing
  }
  return ret;
}

int ObUpsManager::send_revoking_msg(const common::ObServer &addr, int64_t lease, const common::ObServer& master)
{
  int ret = OB_SUCCESS;
  ret = rpc_stub_.revoke_ups_lease(addr, lease, master, config_->flag_network_timeout_us_.get());
  return ret;
}

int ObUpsManager::revoke_master_lease(int64_t &waiting_lease_us)
{
  int ret = OB_SUCCESS;
  waiting_lease_us = 0;
  if (has_master())
  {
    if (is_master_lease_valid())
    {
      // the lease is valid now
      int64_t master_lease = ups_array_[ups_master_idx_].lease_;
      int ret2 = send_revoking_msg(ups_array_[ups_master_idx_].addr_,
                                   master_lease, ups_array_[ups_master_idx_].addr_);
      if (OB_SUCCESS != ret2)
      {
        TBSYS_LOG(WARN, "send lease revoking message to ups master error, err=%d ups=%s",
                  ret2, ups_array_[ups_master_idx_].addr_.to_cstring());
        // we should wait for the lease timeout
        int64_t now2 = tbsys::CTimeUtil::getTime();
        if (master_lease > now2)
        {
          waiting_lease_us = master_lease - now2;
          waiting_ups_finish_time_ = 0; // tell the check thread don't select new master right now
        }
      }
      else
      {
        TBSYS_LOG(INFO, "revoked lease, ups=%s", ups_array_[ups_master_idx_].addr_.to_cstring());
      }
    }
    else
    {
      TBSYS_LOG(WARN, "has master but lease is invalid");
    }
    TBSYS_LOG(INFO, "revoke lease of old master, old_master=%s",
              ups_array_[ups_master_idx_].addr_.to_cstring());
    ups_array_[ups_master_idx_].stat_ = UPS_STAT_SYNC;
    ups_master_idx_ = -1;
  }
  return ret;
}

bool ObUpsManager::is_idx_valid(int ups_idx) const
{
  return (0 <= ups_idx && ups_idx < MAX_UPS_COUNT);
}

bool ObUpsManager::is_ups_with_highest_lsn(int ups_idx)
{
  bool ret = false;
  if (is_idx_valid(ups_idx))
  {
    this->update_ups_lsn();
    int64_t highest_lsn = -1;
    for (int32_t i = 0; i < MAX_UPS_COUNT; ++i)
    {
      if (UPS_STAT_OFFLINE != ups_array_[i].stat_)
      {
        if (ups_array_[i].log_seq_num_ > highest_lsn)
        {
          highest_lsn = ups_array_[i].log_seq_num_;
        }
      }
    } // end for
    ret = (highest_lsn == ups_array_[ups_idx].log_seq_num_);
  }
  return ret;
}

int ObUpsManager::set_ups_master(const common::ObServer &master, bool did_force)
{
  int ret = OB_SUCCESS;
  int64_t waiting_lease_us = 0;
  int i = -1;
  {
    tbsys::CThreadGuard guard(&ups_array_mutex_);
    i = find_ups_index(master);
    if (!is_idx_valid(i))
    {
      TBSYS_LOG(WARN, "ups not registered, addr=%s", master.to_cstring());
      ret = OB_NOT_REGISTERED;
    }
    else if (UPS_STAT_MASTER == ups_array_[i].stat_)
    {
      TBSYS_LOG(WARN, "ups is already the master, ups=%s",
          master.to_cstring());
      ret = OB_INVALID_ARGUMENT;
    }
    else if ((UPS_STAT_SYNC != ups_array_[i].stat_ || !is_ups_with_highest_lsn(i))
        && !did_force)
    {
      TBSYS_LOG(WARN, "ups is not sync, ups=%s stat=%d lsn=%ld",
          master.to_cstring(), ups_array_[i].stat_, ups_array_[i].log_seq_num_);
      ret = OB_INVALID_ARGUMENT;
    }
    else
    {
      revoke_master_lease(waiting_lease_us);
    }
  }
  if (OB_SUCCESS == ret && 0 < waiting_lease_us)
  {
    // wait current lease until timeout, sleep without locking so that the heartbeats will continue
    TBSYS_LOG(INFO, "revoke lease failed and we should wait, usleep=%ld", waiting_lease_us);
    usleep(static_cast<useconds_t>(waiting_lease_us));
  }
  bool new_master_selected = false;
  if (OB_SUCCESS == ret && is_idx_valid(i))
  {
    tbsys::CThreadGuard guard(&ups_array_mutex_);
    // re-check status
    if (((UPS_STAT_SYNC == ups_array_[i].stat_ && is_ups_with_highest_lsn(i)) || did_force)
        && master == ups_array_[i].addr_
        && !is_master_lease_valid())
    {
      ups_array_[i].stat_ = UPS_STAT_MASTER;
      ups_master_idx_ = i;
      TBSYS_LOG(INFO, "set new ups master, master=%s force=%c",
          master.to_cstring(), did_force?'Y':'N');
      new_master_selected = true;
      waiting_ups_finish_time_ = -1;
      reset_ups_read_percent();
    }
    else
    {
      // should rarely come here
      waiting_ups_finish_time_ = -1;
      TBSYS_LOG(WARN, "the ups removed or status changed after sleeping, try again, ups=%s", master.to_cstring());
      ret = OB_CONFLICT_VALUE;
    }
  }
  if (new_master_selected)
  {
    this->grant_lease(true);
  }
  return ret;
}
int32_t ObUpsManager::get_active_ups_count() const
{
  int32_t ret = 0;
  for (int32_t i = 0; i < MAX_UPS_COUNT; ++i)
  {
    if (UPS_STAT_OFFLINE != ups_array_[i].stat_
        && UPS_STAT_NOTSYNC != ups_array_[i].stat_)
    {
      ret++;
    }
  }
  return ret;
}


int32_t ObUpsManager::get_ups_count() const
{
  int32_t ret = 0;
  for (int32_t i = 0; i < MAX_UPS_COUNT; ++i)
  {
    if (UPS_STAT_OFFLINE != ups_array_[i].stat_)
    {
      ret++;
    }
  }
  return ret;
}

int ObUpsManager::get_ups_master(ObUps &ups_master)
{
  int ret = OB_ENTRY_NOT_EXIST;
  tbsys::CThreadGuard guard(&ups_array_mutex_);
  if (has_master())
  {
    ups_master = ups_array_[ups_master_idx_];
    ret = OB_SUCCESS;
  }
  return ret;
}

const char* ObUpsManager::ups_stat_to_cstr(ObUpsStatus stat) const
{
  const char* ret = "";
  switch(stat)
  {
    case UPS_STAT_OFFLINE:
      ret = "offline";
      break;
    case UPS_STAT_MASTER:
      ret = "master";
      break;
    case UPS_STAT_SYNC:
      ret = "sync";
      break;
    case UPS_STAT_NOTSYNC:
      ret = "nsync";
      break;
    default:
      break;
  }
  return ret;
}

void ObUpsManager::print(char* buf, const int64_t buf_len, int64_t &pos)
{
  tbsys::CThreadGuard guard(&ups_array_mutex_);
  if (is_master_lease_valid())
  {
    int64_t now2 = tbsys::CTimeUtil::getTime();
    databuff_printf(buf, buf_len, pos, "lease_left=%ld|", ups_array_[ups_master_idx_].lease_ - now2);
  }
  else
  {
    databuff_printf(buf, buf_len, pos, "lease_left=null|");
  }
  for (int32_t i = 0; i < MAX_UPS_COUNT; ++i)
  {
    if (UPS_STAT_OFFLINE != ups_array_[i].stat_)
    {
      databuff_printf(buf, buf_len, pos, "%s(%d %s %d %d %lu %s),", ups_array_[i].addr_.to_cstring(),
                      ups_array_[i].inner_port_, ups_stat_to_cstr(ups_array_[i].stat_),
                      ups_array_[i].ms_read_percentage_, ups_array_[i].cs_read_percentage_,
                      ups_array_[i].log_seq_num_, ups_array_[i].obi_role_.get_role_str());
    }
  }
}
int ObUpsManager::set_ups_config(int32_t master_master_ups_read_percentage, int32_t slave_master_ups_read_percentage)
{
  int ret = OB_SUCCESS;
  if ((-1 != master_master_ups_read_percentage)
      && (0 > master_master_ups_read_percentage
        || 100 < master_master_ups_read_percentage))
  {
    TBSYS_LOG(WARN, "invalid param, master_master_ups_read_percentage=%d", master_master_ups_read_percentage);
    ret = OB_INVALID_ARGUMENT;
  }
  else if ((-1 != slave_master_ups_read_percentage)
      && (0 > slave_master_ups_read_percentage
        || 100 < slave_master_ups_read_percentage))
  {
    TBSYS_LOG(WARN, "invalid param, slave_master_ups_read_percentage=%d", slave_master_ups_read_percentage);
    ret = OB_INVALID_ARGUMENT;
  }
  else
  {
    TBSYS_LOG(INFO, "change ups config, read_master_master_ups_percentage=%d read_slave_master_ups_percentage=%d",
        master_master_ups_read_percentage, slave_master_ups_read_percentage);
    master_master_ups_read_percentage_ = master_master_ups_read_percentage;
    slave_master_ups_read_percentage_ = slave_master_ups_read_percentage;
    tbsys::CThreadGuard guard(&ups_array_mutex_);
    reset_ups_read_percent();
  }
  return ret;
}

void ObUpsManager::get_master_ups_config(int32_t &master_master_ups_read_percent, int32_t &slave_master_ups_read_percent) const
{
  master_master_ups_read_percent = master_master_ups_read_percentage_;
  slave_master_ups_read_percent = slave_master_ups_read_percentage_;
}
int ObUpsManager::set_ups_config(const common::ObServer &addr, int32_t ms_read_percentage, int32_t cs_read_percentage)
{
  int ret = OB_SUCCESS;
  if (0 > ms_read_percentage || 100 < ms_read_percentage)
  {
    TBSYS_LOG(WARN, "invalid param, ms_read_percentage=%d", ms_read_percentage);
    ret = OB_INVALID_ARGUMENT;
  }
  else if (0 > cs_read_percentage || 100 < cs_read_percentage)
  {
    TBSYS_LOG(WARN, "invalid param, cs_read_percentage=%d", cs_read_percentage);
    ret = OB_INVALID_ARGUMENT;
  }
  else
  {
    int i = -1;
    is_flow_control_by_ip_ = true;
    tbsys::CThreadGuard guard(&ups_array_mutex_);
    if (-1 == (i = find_ups_index(addr)))
    {
      TBSYS_LOG(WARN, "ups not exist, addr=%s", addr.to_cstring());
      ret = OB_ENTRY_NOT_EXIST;
    }
    else
    {
      TBSYS_LOG(INFO, "change ups config, ups=%s ms_read_percentage=%d cs_read_percentage=%d",
                addr.to_cstring(), ms_read_percentage, cs_read_percentage);
      ups_array_[i].ms_read_percentage_ = ms_read_percentage;
      ups_array_[i].cs_read_percentage_ = cs_read_percentage;
    }
  }
  return ret;
}

void ObUps::convert_to(ObUpsInfo &ups_info) const
{
  ups_info.addr_ = addr_;
  ups_info.inner_port_ = inner_port_;
  if (UPS_STAT_MASTER == stat_)
  {
    ups_info.stat_ = UPS_MASTER;
  }
  else
  {
    ups_info.stat_ = UPS_SLAVE;
  }
  ups_info.ms_read_percentage_ = static_cast<int8_t>(ms_read_percentage_);
  ups_info.cs_read_percentage_ = static_cast<int8_t>(cs_read_percentage_);
}

int ObUpsManager::get_ups_list(common::ObUpsList &ups_list)
{
  int ret = OB_SUCCESS;
  int count = 0;
  tbsys::CThreadGuard guard(&ups_array_mutex_);
  if (!is_flow_control_by_ip_)
  {
    for (int32_t i = 0; i < MAX_UPS_COUNT && count < ups_list.MAX_UPS_COUNT; ++i)
    {
      if (UPS_STAT_OFFLINE != ups_array_[i].stat_
          && UPS_STAT_NOTSYNC != ups_array_[i].stat_)
      {
        ups_array_[i].convert_to(ups_list.ups_array_[count]);
        count++;
      }
    }
  }
  else //如果按ip分配流量的话，则返回所有在线的UPS
  {
    for (int32_t i = 0; i < MAX_UPS_COUNT && count < ups_list.MAX_UPS_COUNT; ++i)
    {
      if (UPS_STAT_OFFLINE != ups_array_[i].stat_)
      {
        ups_array_[i].convert_to(ups_list.ups_array_[count]);
        count++;
      }
    }
  }
  ups_list.ups_count_ = count;
  return ret;
}

int ObUpsManager::send_obi_role()
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = this->grant_lease(true)))
  {
    TBSYS_LOG(WARN, "failed to send lease msg, err=%d", ret);
  }
  else
  {
    TBSYS_LOG(INFO, "sent lease grant to change obi role");
    ret = OB_RESPONSE_TIME_OUT;
    // wait for the master
    const int64_t sleep_us = 111000; // 111ms
    int64_t total_sleep_us = 0;
    do
    {
      {                         // scoped lock
        tbsys::CThreadGuard guard(&ups_array_mutex_);
        if (has_master())
        {
          if (obi_role_ == ups_array_[ups_master_idx_].obi_role_)
          {
            ret = OB_SUCCESS;
            TBSYS_LOG(INFO, "ups master has changed obi_role, master_ups=%s obi_role=%s",
                      ups_array_[ups_master_idx_].addr_.to_cstring(),
                      ups_array_[ups_master_idx_].obi_role_.get_role_str());
            break;
          }
        }
        else
        {
          // no master
          ret = OB_SUCCESS;
          TBSYS_LOG(INFO, "no ups master and don't wait");
          break;
        }
      }
      usleep(sleep_us);
      total_sleep_us += sleep_us;
      TBSYS_LOG(INFO, "waiting ups for changing the obi role, wait_us=%ld", total_sleep_us);
    } while (total_sleep_us < config_->flag_ups_lease_us_.get());
  }
  reset_ups_read_percent();
  return ret;
}
