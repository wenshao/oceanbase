/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_root_balancer.cpp
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#include "ob_root_balancer.h"
#include "common/utility.h"
#include "ob_root_util.h"
using namespace oceanbase::rootserver;
using namespace oceanbase::common;

ObRootBalancer::ObRootBalancer()
  :config_(NULL),
   root_table_(NULL),
   server_manager_(NULL),
   root_table_rwlock_(NULL),
   server_manager_rwlock_(NULL),
   log_worker_(NULL),
   balance_start_time_us_(0),
   balance_timeout_us_(0),
   balance_last_migrate_succ_time_(0),
   balance_next_table_seq_(0),
   balance_batch_migrate_count_(0),
   balance_batch_migrate_done_num_(0),
   balance_select_dest_start_pos_(0),
   balance_batch_copy_count_(0)
{
}

ObRootBalancer::~ObRootBalancer()
{
}

void ObRootBalancer::do_balance(bool &did_migrating)
{
  int err = OB_SUCCESS;
  check_components();
  err = restart_server_->restart_servers();
  TBSYS_LOG(DEBUG, "restart server check run");
  if(OB_SUCCESS != err)
  {
    TBSYS_LOG(WARN, "restart server fail:err[%d]", err);
  }

  if (nb_is_in_batch_migrating())
  {
    nb_check_migrate_timeout();
  }
  else
  {
    do_new_balance();
  }
  did_migrating = (0 < balance_batch_migrate_count_);
}

void ObRootBalancer::check_components()
{
  OB_ASSERT(config_);
  OB_ASSERT(root_table_);
  OB_ASSERT(server_manager_);
  OB_ASSERT(root_table_rwlock_);
  OB_ASSERT(server_manager_rwlock_);
  OB_ASSERT(schema_manager_);
  OB_ASSERT(schema_manager_rwlock_);
  OB_ASSERT(log_worker_);
  OB_ASSERT(role_mgr_);
  OB_ASSERT(restart_server_);
}

// @return 0 do not copy, 1 copy immediately, -1 delayed copy
inline int ObRootBalancer::need_copy(int32_t available_num, int32_t lost_num)
{
  OB_ASSERT(0 <= available_num);
  OB_ASSERT(0 <= lost_num);
  int ret = 0;
  if (0 == available_num || 0 == lost_num)
  {
    ret = 0;
  }
  else if (1 == lost_num)
  {
    ret = -1;
  }
  else                          // lost_num >= 2
  {
    ret = 1;
  }
  return ret;
}

int32_t ObRootBalancer::nb_get_table_count()
{
  int32_t ret = 0;
  tbsys::CRLockGuard guard(*schema_manager_rwlock_);
  if (NULL == schema_manager_)
  {
    TBSYS_LOG(ERROR, "schema_manager is NULL");
  }
  else
  {
    const ObTableSchema* it = NULL;
    for (it = schema_manager_->table_begin(); schema_manager_->table_end() != it; ++it)
    {
      ret++;
    }
  }
  return ret;
}

uint64_t ObRootBalancer::nb_get_next_table_id(int32_t table_count, int32_t seq/*=-1*/)
{
  uint64_t ret = OB_INVALID_ID;
  // for each table
  tbsys::CRLockGuard guard(*schema_manager_rwlock_);
  if (NULL == schema_manager_)
  {
    TBSYS_LOG(ERROR, "schema_manager is NULL");
  }
  else if (0 >= table_count)
  {
    // no table
  }
  else
  {
    if (0 > seq)
    {
      seq = balance_next_table_seq_;
      balance_next_table_seq_++;
    }
    int32_t idx = 0;
    const ObTableSchema* it = NULL;
    for (it = schema_manager_->table_begin(); schema_manager_->table_end() != it; ++it)
    {
      if (seq % table_count == idx)
      {
        ret = it->get_table_id();
        break;
      }
      idx++;
    }
  }
  return ret;
}

int ObRootBalancer::nb_find_dest_cs(ObRootTable2::const_iterator meta, int64_t low_bound, int32_t cs_num,
                                   int32_t &dest_cs_idx, ObChunkServerManager::iterator &dest_it)
{
  int ret = OB_ENTRY_NOT_EXIST;
  dest_cs_idx = OB_INVALID_INDEX;
  if (0 < cs_num)
  {
    int64_t mnow = tbsys::CTimeUtil::getMonotonicTime();
    ObChunkServerManager::iterator it = server_manager_->begin();
    it += balance_select_dest_start_pos_ % cs_num;
    if (it >= server_manager_->end())
    {
      it = server_manager_->begin();
    }
    ObChunkServerManager::iterator it_start_pos = it;
    int32_t cs_idx = OB_INVALID_INDEX;
    // scan from start_pos to end(), then from start() to start_pos
    while(true)
    {
      if (it->status_ != ObServerStatus::STATUS_DEAD
          && it->status_ != ObServerStatus::STATUS_SHUTDOWN
          && it->balance_info_.table_sstable_count_ < low_bound
          && mnow > (it->register_time_ + 1000LL*1000*config_->flag_cs_probation_period_seconds_.get()))
      {
        cs_idx = static_cast<int32_t>(it - server_manager_->begin());
        // this cs does't have this tablet
        if (!meta->did_cs_have(cs_idx))
        {
          dest_it = it;
          dest_cs_idx = cs_idx;
          TBSYS_LOG(DEBUG, "find dest cs, start_pos=%ld cs_idx=%d",
                    it_start_pos-server_manager_->begin(), dest_cs_idx);
          balance_select_dest_start_pos_ = dest_cs_idx + 1;
          ret = OB_SUCCESS;
          break;
        }
      }
      ++it;
      if (it == server_manager_->end())
      {
        it = server_manager_->begin();
      }
      if (it == it_start_pos)
      {
        break;
      }
    } // end while
  }
  return ret;
}

int ObRootBalancer::nb_check_rereplication(ObRootTable2::const_iterator it, RereplicationAction &act)
{
  int ret = OB_SUCCESS;
  act = RA_NOP;
  if (config_->flag_enable_rereplication_.get())
  {
    int64_t last_version = 0;
    int32_t valid_replicas_num = 0;
    int32_t lost_copy = 0;
    for (int32_t i = 0; i < OB_SAFE_COPY_COUNT; i++)
    {
      if (OB_INVALID_INDEX != it->server_info_indexes_[i])
      {
        valid_replicas_num ++;
        if (it->tablet_version_[i] > last_version)
        {
          last_version = it->tablet_version_[i];
        }
      }
    }
    int64_t tablet_replicas_num = config_->flag_tablet_replicas_num_.get();
    if (valid_replicas_num < tablet_replicas_num)
    {
      lost_copy = static_cast<int32_t>(tablet_replicas_num - valid_replicas_num);
      int did_need_copy = need_copy(valid_replicas_num, lost_copy);
      int64_t now = tbsys::CTimeUtil::getTime();
      if (1 == did_need_copy)
      {
        act = RA_COPY;
      }
      else if (-1 == did_need_copy)
      {
        if (now - it->last_dead_server_time_ > config_->flag_safe_lost_one_duration_seconds_.get()*1000000LL)
        {
          act = RA_COPY;
        }
        else
        {
          TBSYS_LOG(DEBUG, "copy delayed, now=%ld lost_replica_time=%ld duration_sec=%d",
                    now, it->last_dead_server_time_, config_->flag_safe_lost_one_duration_seconds_.get());
        }
      }
    }
    else if (valid_replicas_num > tablet_replicas_num)
    {
      act = RA_DELETE;
    }
  }
  return ret;
}

int ObRootBalancer::nb_select_copy_src(ObRootTable2::const_iterator it,
                                      int32_t &src_cs_idx, ObChunkServerManager::iterator &src_it)
{
  int ret = OB_ENTRY_NOT_EXIST;
  src_cs_idx = OB_INVALID_INDEX;
  int32_t min_count = INT32_MAX;
  // find cs with min migrate out count
  for (int i = 0; i < OB_SAFE_COPY_COUNT; ++i)
  {
    if (OB_INVALID_INDEX != it->server_info_indexes_[i])
    {
      ObServerStatus *src_cs = server_manager_->get_server_status(it->server_info_indexes_[i]);
      int32_t migrate_count = src_cs->balance_info_.migrate_to_.count();
      if (migrate_count < min_count
          && migrate_count < config_->flag_balance_max_migrate_out_per_cs_.get())
      {
        min_count = migrate_count;
        src_cs_idx = static_cast<int32_t>(src_cs - server_manager_->begin());
        src_it = src_cs;
        ret = OB_SUCCESS;
      }
    }
  }
  return ret;
}

int ObRootBalancer::nb_add_copy(ObRootTable2::const_iterator it, const ObTabletInfo* tablet, int64_t low_bound, int32_t cs_num)
{
  int ret = OB_ERROR;
  int32_t dest_cs_idx = OB_INVALID_INDEX;
  ObServerStatus *dest_it = NULL;
  if (OB_SUCCESS != nb_find_dest_cs(it, low_bound, cs_num, dest_cs_idx, dest_it)
      || OB_INVALID_INDEX == dest_cs_idx)
  {
    if (OB_SUCCESS != nb_find_dest_cs(it, INT64_MAX, cs_num, dest_cs_idx, dest_it)
        || OB_INVALID_INDEX == dest_cs_idx)
    {
      TBSYS_LOG(DEBUG, "cannot find dest cs");
    }
  }
  if (OB_INVALID_INDEX != dest_cs_idx)
  {
    int32_t src_cs_idx = OB_INVALID_INDEX;
    ObServerStatus *src_it = NULL;
    if (OB_SUCCESS != nb_select_copy_src(it, src_cs_idx, src_it)
        || OB_INVALID_INDEX == src_cs_idx)
    {
      TBSYS_LOG(DEBUG, "cannot find src cs");
    }
    else
    {
      int bm_ret = server_manager_->add_copy_info(*src_it, tablet->range_, dest_cs_idx);
      if (OB_SUCCESS == bm_ret)
      {
        // no locking
        dest_it->balance_info_.table_sstable_count_++;
        balance_batch_migrate_count_++;
        balance_batch_copy_count_++;
        ret = OB_SUCCESS;
      }
    }
  }
  return ret;
}

int ObRootBalancer::nb_check_add_migrate(ObRootTable2::const_iterator it, const ObTabletInfo* tablet, int64_t avg_count,
                                        int32_t cs_num, int32_t migrate_out_per_cs)
{
  int ret = OB_SUCCESS;
  int64_t delta_count = config_->flag_balance_tolerance_count_.get();
  for (int i = 0; i < OB_SAFE_COPY_COUNT; ++i)
  {
    int32_t cs_idx = it->server_info_indexes_[i];
    if (OB_INVALID_INDEX != cs_idx)
    {
      ObServerStatus *src_cs = server_manager_->get_server_status(cs_idx);
      if (NULL != src_cs && ObServerStatus::STATUS_DEAD != src_cs->status_)
      {
        if ((src_cs->balance_info_.table_sstable_count_ > avg_count + delta_count
             ||src_cs->status_ == ObServerStatus::STATUS_SHUTDOWN)
            && src_cs->balance_info_.migrate_to_.count() < migrate_out_per_cs)
        {
          // move out this sstable
          // find dest cs, no locking
          int32_t dest_cs_idx = OB_INVALID_INDEX;
          ObServerStatus *dest_it = NULL;
          if (OB_SUCCESS != nb_find_dest_cs(it, avg_count - delta_count, cs_num, dest_cs_idx, dest_it)
              || OB_INVALID_INDEX == dest_cs_idx)
          {
            if (src_cs->status_ == ObServerStatus::STATUS_SHUTDOWN)
            {
              if (OB_SUCCESS != nb_find_dest_cs(it, INT64_MAX, cs_num, dest_cs_idx, dest_it)
                  || OB_INVALID_INDEX == dest_cs_idx)
              {
                TBSYS_LOG(WARN, "cannot find dest cs");
              }
            }
          }
          if (OB_INVALID_INDEX != dest_cs_idx)
          {
            int bm_ret = server_manager_->add_migrate_info(*src_cs, tablet->range_, dest_cs_idx);
            if (OB_SUCCESS == bm_ret)
            {
              // no locking
              src_cs->balance_info_.table_sstable_count_--;
              dest_it->balance_info_.table_sstable_count_++;
              balance_batch_migrate_count_++;
            }
          }
          break;
        }
      }
    }
  } // end for
  return ret;
}

int ObRootBalancer::nb_del_copy(ObRootTable2::const_iterator it, const ObTabletInfo* tablet)
{
  int ret = OB_ENTRY_NOT_EXIST;
  int64_t min_version = INT64_MAX;
  int32_t delete_idx = -1;
  int32_t valid_replicas_num = 0;
  for (int32_t i = 0; i < OB_SAFE_COPY_COUNT; i++)
  {
    if (OB_INVALID_INDEX != it->server_info_indexes_[i])
    {
      valid_replicas_num ++;
      if (it->tablet_version_[i] < min_version)
      {
        min_version = it->tablet_version_[i];
        delete_idx = i;
      }
    }
  } // end for
  // remove one replica if necessary
  int64_t tablet_replicas_num = config_->flag_tablet_replicas_num_.get();
  if (valid_replicas_num > tablet_replicas_num
      && -1 != delete_idx)
  {
    ObTabletReportInfo to_delete;
    to_delete.tablet_info_ = *tablet;
    to_delete.tablet_location_.tablet_version_ = it->tablet_version_[delete_idx];
    to_delete.tablet_location_.chunkserver_.set_port(it->server_info_indexes_[delete_idx]);
    if (OB_SUCCESS != (ret = delete_list_.add_tablet(to_delete)))
    {
      TBSYS_LOG(WARN, "failed to add into delete list");
    }
    else
    {
      TBSYS_LOG(DEBUG, "delete replica, version=%ld cs_idx=%d",
                it->tablet_version_[delete_idx], delete_idx);
      it->server_info_indexes_[delete_idx] = OB_INVALID_INDEX;
      if (role_mgr_->is_master())
      {
        if (OB_SUCCESS != log_worker_->remove_replica(to_delete))
        {
          TBSYS_LOG(ERROR, "write log error");
        }
      }
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int ObRootBalancer::do_rereplication_by_table(const uint64_t table_id, bool &scan_next_table)
{
  int ret = OB_SUCCESS;
  int64_t avg_size = 0;
  int64_t avg_count = 0;
  int64_t delta_count = config_->flag_balance_tolerance_count_.get();
  int32_t cs_num = 0;
  int32_t migrate_out_per_cs = 0;
  int32_t shutdown_num = 0;
  scan_next_table = true;
  if (OB_SUCCESS != (ret = nb_calculate_sstable_count(table_id, avg_size, avg_count,
                                                      cs_num, migrate_out_per_cs, shutdown_num)))
  {
    TBSYS_LOG(WARN, "calculate table size error, err=%d", ret);
  }
  else if (0 < cs_num)
  {
    ObRootTable2::const_iterator it;
    const ObTabletInfo* tablet = NULL;
    bool table_found = false;
    RereplicationAction ract;
    // scan the root table
    tbsys::CRLockGuard guard(*root_table_rwlock_);
    for (it = root_table_->begin(); it != root_table_->sorted_end(); ++it)
    {
      tablet = root_table_->get_tablet_info(it);
      if (NULL != tablet)
      {
        if (tablet->range_.table_id_ == table_id)
        {
          if (!table_found)
          {
            table_found = true;
          }
          // check re-replication
          int add_ret = OB_ERROR;
          if (OB_SUCCESS == nb_check_rereplication(it, ract))
          {
            if (RA_COPY == ract)
            {
              add_ret = nb_add_copy(it, tablet, avg_count - delta_count, cs_num);
            }
            else if (RA_DELETE == ract)
            {
              add_ret = nb_del_copy(it, tablet);
            }
          }
          // terminate condition
          if (server_manager_->is_migrate_infos_full())
          {
            scan_next_table = false;
            break;
          }
        }
        else
        {
          if (table_found)
          {
            // another table
            break;
          }
        }
      }
    } // end for
  }
  return ret;
}
void ObRootBalancer::check_shutdown_process()
{
  ObChunkServerManager::const_iterator it;
  for (it = server_manager_->begin(); it != server_manager_->end(); ++it)
  {
    if (ObServerStatus::STATUS_SHUTDOWN == it->status_)
    {
      int32_t cs_index = static_cast<int32_t>(it - server_manager_->begin());
      int64_t count = 0;
      ObRootTable2::const_iterator root_it;
      tbsys::CRLockGuard guard(*root_table_rwlock_);
      for (root_it = root_table_->begin(); root_it != root_table_->sorted_end() && 0 == count; ++root_it)
      {
        for (int i = 0; i < OB_SAFE_COPY_COUNT && 0 == count; ++i)
        {
          if (cs_index == root_it->server_info_indexes_[i])
          {
            count ++;
          }
        }
      }
      if (0 == count)
      {
        char addr_buf[OB_IP_STR_BUFF];
        it->server_.to_string(addr_buf, OB_IP_STR_BUFF);
        TBSYS_LOG(INFO, "shutdown chunkserver[%s] is finished", addr_buf );
      }
    }
  }
}
int ObRootBalancer::nb_balance_by_table(const uint64_t table_id, bool &scan_next_table)
{
  int ret = OB_SUCCESS;
  int64_t avg_size = 0;
  int64_t avg_count = 0;
  int32_t cs_num = 0;
  int32_t migrate_out_per_cs = 0;
  int32_t shutdown_num = 0;
  scan_next_table = true;

  if (OB_SUCCESS != (ret = nb_calculate_sstable_count(table_id, avg_size, avg_count,
                                                      cs_num, migrate_out_per_cs, shutdown_num)))
  {
    TBSYS_LOG(WARN, "calculate table size error, err=%d", ret);
  }
  else if (0 < cs_num)
  {
    if (shutdown_num > 0) //exist shutdown task.check the process
    {
      check_shutdown_process();
    }
    bool is_curr_table_balanced = nb_is_curr_table_balanced(avg_count);
    if (!is_curr_table_balanced)
    {
      TBSYS_LOG(DEBUG, "balance table, table_id=%lu avg_count=%ld", table_id, avg_count);
      nb_print_balance_info();
    }

    ObRootTable2::const_iterator it;
    const ObTabletInfo* tablet = NULL;
    bool table_found = false;
    // scan the root table
    tbsys::CRLockGuard guard(*root_table_rwlock_);
    for (it = root_table_->begin(); it != root_table_->sorted_end(); ++it)
    {
      tablet = root_table_->get_tablet_info(it);
      if (NULL != tablet)
      {
        if (tablet->range_.table_id_ == table_id)
        {
          if (!table_found)
          {
            table_found = true;
          }
          // do balnace if needed
          if ((!is_curr_table_balanced || 0 < shutdown_num)
              && config_->flag_enable_balance_.get()
              && it->can_be_migrated_now(config_->flag_tablet_migrate_disabling_period_us_.get()))
          {
            nb_check_add_migrate(it, tablet, avg_count, cs_num, migrate_out_per_cs);
          }
          // terminate condition
          if (server_manager_->is_migrate_infos_full())
          {
            scan_next_table = false;
            break;
          }
        }
        else
        {
          if (table_found)
          {
            // another table
            break;
          }
        }
      } // end if tablet not NULL
    } // end for
  }
  return ret;
}

bool ObRootBalancer::nb_is_curr_table_balanced(int64_t avg_sstable_count, const ObServer& except_cs) const
{
  bool ret = true;
  int64_t delta_count = config_->flag_balance_tolerance_count_.get();
  int32_t cs_out = 0;
  int32_t cs_in = 0;
  ObChunkServerManager::const_iterator it;
  for (it = server_manager_->begin(); it != server_manager_->end(); ++it)
  {
    if (it->status_ != ObServerStatus::STATUS_DEAD)
    {
      if (except_cs == it->server_)
      {
        // do not consider except_cs
        continue;
      }
      if ((avg_sstable_count + delta_count) < it->balance_info_.table_sstable_count_)
      {
        cs_out++;
      }
      else if ((avg_sstable_count - delta_count) > it->balance_info_.table_sstable_count_)
      {
        cs_in++;
      }
      if (0 < cs_out && 0 < cs_in)
      {
        ret = false;
        break;
      }
    }
  } // end for
  return ret;
}

bool ObRootBalancer::nb_is_curr_table_balanced(int64_t avg_sstable_count) const
{
  ObServer not_exist_cs;
  return nb_is_curr_table_balanced(avg_sstable_count, not_exist_cs);
}

int ObRootBalancer::nb_calculate_sstable_count(const uint64_t table_id, int64_t &avg_size, int64_t &avg_count,
                                               int32_t &cs_count, int32_t &migrate_out_per_cs, int32_t &shutdown_count)
{
  int ret = OB_SUCCESS;
  avg_size = 0;
  avg_count = 0;
  cs_count = 0;
  shutdown_count = 0;
  int64_t total_size = 0;
  int64_t total_count = 0; // total sstable count
  {
    // prepare
    tbsys::CWLockGuard guard(*server_manager_rwlock_);
    server_manager_->reset_balance_info_for_table(cs_count, shutdown_count);
  } // end lock
  {
    // calculate sum
    ObRootTable2::const_iterator it;
    const ObTabletInfo* tablet = NULL;
    tbsys::CRLockGuard guard(*root_table_rwlock_);
    bool table_found = false;
    for (it = root_table_->begin(); it != root_table_->sorted_end(); ++it)
    {
      tablet = root_table_->get_tablet_info(it);
      if (NULL != tablet)
      {
        if (tablet->range_.table_id_ == table_id)
        {
          if (!table_found)
          {
            table_found = true;
          }
          for (int i = 0; i < OB_SAFE_COPY_COUNT; ++i)
          {
            if (OB_INVALID_INDEX != it->server_info_indexes_[i])
            {
              ObServerStatus *cs = server_manager_->get_server_status(it->server_info_indexes_[i]);
              if (NULL != cs && ObServerStatus::STATUS_DEAD != cs->status_)
              {
                cs->balance_info_.table_sstable_total_size_ += tablet->occupy_size_;
                total_size += tablet->occupy_size_;
                cs->balance_info_.table_sstable_count_++;
                total_count++;
              }
            }
          } // end for
        }
        else
        {
          if (table_found)
          {
            break;
          }
        }
      }
    } // end for
  }   // end lock
  if (0 < cs_count)
  {
    avg_size = total_size / cs_count;
    avg_count = total_count / cs_count;
    if (0 < shutdown_count && shutdown_count < cs_count)
    {
      avg_size = total_size / (cs_count - shutdown_count);
      // make sure the shutting-down servers can find dest cs
      avg_count = total_count / (cs_count - shutdown_count) + 1 + config_->flag_balance_tolerance_count_.get();
    }
    int64_t sstable_avg_size = -1;
    if (0 != total_count)
    {
      sstable_avg_size = total_size/total_count;
    }
    int32_t out_cs = 0;
    ObServerStatus *it = NULL;
    for (it = server_manager_->begin(); it != server_manager_->end(); ++it)
    {
      if (it->status_ != ObServerStatus::STATUS_DEAD)
      {
        if (it->balance_info_.table_sstable_count_ > avg_count + config_->flag_balance_tolerance_count_.get()
            || ObServerStatus::STATUS_SHUTDOWN == it->status_)
        {
          out_cs++;
        }
      }
    }
    migrate_out_per_cs = config_->flag_balance_max_migrate_out_per_cs_.get();
    if (0 < out_cs
        && out_cs < cs_count)
    {
      migrate_out_per_cs = server_manager_->get_max_migrate_num() / out_cs;
      if (MAX_MAX_MIGRATE_OUT_PER_CS < migrate_out_per_cs)
      {
        migrate_out_per_cs = MAX_MAX_MIGRATE_OUT_PER_CS;
      }
    }
    TBSYS_LOG(DEBUG, "sstable distribution, table_id=%lu total_size=%ld total_count=%ld "
              "cs_num=%d shutdown_num=%d avg_size=%ld avg_count=%ld sstable_avg_size=%ld migrate_out_per_cs=%d",
              table_id, total_size, total_count,
              cs_count, shutdown_count, avg_size, avg_count, sstable_avg_size, migrate_out_per_cs);
  }
  return ret;
}

void ObRootBalancer::nb_print_balance_info() const
{
  char addr_buf[OB_IP_STR_BUFF];
  const ObServerStatus *it = server_manager_->begin();
  for (; it != server_manager_->end(); ++it)
  {
    if (NULL != it && ObServerStatus::STATUS_DEAD != it->status_)
    {
      it->server_.to_string(addr_buf, OB_IP_STR_BUFF);
      TBSYS_LOG(DEBUG, "cs=%s sstables_count=%ld sstables_size=%ld migrate=%d",
                addr_buf, it->balance_info_.table_sstable_count_,
                it->balance_info_.table_sstable_total_size_,
                it->balance_info_.migrate_to_.count());
    }
  }
}

void ObRootBalancer::nb_print_balance_info(char *buf, const int64_t buf_len, int64_t& pos) const
{
  char addr_buf[OB_IP_STR_BUFF];
  const ObServerStatus *it = server_manager_->begin();
  for (; it != server_manager_->end(); ++it)
  {
    if (NULL != it && ObServerStatus::STATUS_DEAD != it->status_)
    {
      it->server_.to_string(addr_buf, OB_IP_STR_BUFF);
      databuff_printf(buf, buf_len, pos, "%s %ld %ld\n",
                      addr_buf, it->balance_info_.table_sstable_count_,
                      it->balance_info_.table_sstable_total_size_);
    }
  }
}

int ObRootBalancer::send_msg_migrate(const ObServer &src, const ObServer &dest, const ObRange& range, bool keep_src)
{
  int ret = OB_SUCCESS;
  ret = rpc_stub_->migrate_tablet(src, dest, range, keep_src, config_->flag_network_timeout_us_.get());
  static char row_key_dump_buff[OB_MAX_ROW_KEY_LENGTH * 2];
  char f_server[OB_IP_STR_BUFF];
  char t_server[OB_IP_STR_BUFF];
  range.to_string(row_key_dump_buff, OB_MAX_ROW_KEY_LENGTH * 2);
  src.to_string(f_server, OB_IP_STR_BUFF);
  dest.to_string(t_server, OB_IP_STR_BUFF);
  if (OB_SUCCESS == ret)
  {
    TBSYS_LOG(INFO, "migrate tablet, tablet=%s src=%s dest=%s keep_src=%c",
        row_key_dump_buff, f_server, t_server, keep_src?'Y':'N');
  }
  else
  {
    TBSYS_LOG(WARN, "failed to send migrate msg, err=%d, tablet=%s, src=%s, dest=%s,keep_src=%c",
        ret, row_key_dump_buff, f_server, t_server, keep_src?'Y':'N');
  }
  return ret;
}

int ObRootBalancer::nb_start_batch_migrate()
{
  int ret = OB_SUCCESS;
  int32_t sent_count = 0;
  int32_t batch_migrate_per_cs = config_->flag_balance_max_concurrent_migrate_num_.get();
  ObChunkServerManager::iterator it;
  for (it = server_manager_->begin(); it != server_manager_->end(); ++it)
  {
    if (it->status_ != ObServerStatus::STATUS_DEAD
        && 0 < it->balance_info_.migrate_to_.count()
        && it->balance_info_.curr_migrate_out_num_ < batch_migrate_per_cs)
    {
      // start the first K migrate
      ObMigrateInfo *minfo = it->balance_info_.migrate_to_.head();
      for (; NULL != minfo; minfo = minfo->next_)
      {
        if (OB_INVALID_INDEX != minfo->cs_idx_
            && ObMigrateInfo::STAT_INIT == minfo->stat_)
        {
          ObServerStatus* dest_cs = server_manager_->get_server_status(minfo->cs_idx_);
          if (NULL != dest_cs
              && ObServerStatus::STATUS_DEAD != dest_cs->status_
              && batch_migrate_per_cs > dest_cs->balance_info_.curr_migrate_in_num_)
          {
            if (OB_SUCCESS == send_msg_migrate(it->server_, dest_cs->server_, minfo->range_, 1 == minfo->keep_src_))
            {
              minfo->stat_ = ObMigrateInfo::STAT_SENT;
              it->balance_info_.curr_migrate_out_num_++;
              dest_cs->balance_info_.curr_migrate_in_num_++;
              sent_count++;
              if (it->balance_info_.curr_migrate_out_num_ >= batch_migrate_per_cs)
              {
                break;
              }
            }
            else
            {
              TBSYS_LOG(WARN, "error happen while send migrate msg. try it later.");
              ret = OB_ERROR;
              break;
            }
          }
        }
      } // end while each migrate candidates
    }
    if (OB_SUCCESS != ret)
    {
      break;
    }
  } // end for
  TBSYS_LOG(INFO, "batch migrate sent_num=%d done=%d total=%d",
            sent_count, balance_batch_migrate_done_num_, balance_batch_migrate_count_);
  if (0 >= sent_count)
  {
    nb_print_migrate_infos();
  }
  return ret;
}

void ObRootBalancer::nb_print_migrate_infos() const
{
  TBSYS_LOG(INFO, "print migrate infos:");
  char addr_buf1[OB_IP_STR_BUFF];
  char addr_buf2[OB_IP_STR_BUFF];
  static char range_buf[OB_MAX_ROW_KEY_LENGTH * 2];
  int32_t idx = 0;
  int32_t total_in = 0;
  int32_t total_out = 0;
  ObChunkServerManager::const_iterator it;
  for (it = server_manager_->begin(); it != server_manager_->end(); ++it)
  {
    if (it->status_ != ObServerStatus::STATUS_DEAD)
    {
      total_in += it->balance_info_.curr_migrate_in_num_;
      total_out += it->balance_info_.curr_migrate_out_num_;
      it->server_.to_string(addr_buf1, OB_IP_STR_BUFF);
      TBSYS_LOG(INFO, "balance info, cs=%s in=%d out=%d",
                addr_buf1, it->balance_info_.curr_migrate_in_num_,
                it->balance_info_.curr_migrate_out_num_);

      if(0 < it->balance_info_.migrate_to_.count())
      {
        const ObMigrateInfo *minfo = it->balance_info_.migrate_to_.head();
        for (; NULL != minfo; minfo = minfo->next_)
        {
          if (OB_INVALID_INDEX != minfo->cs_idx_
              && ObMigrateInfo::STAT_DONE != minfo->stat_)
          {
            const ObServerStatus* dest_cs = server_manager_->get_server_status(minfo->cs_idx_);
            if (NULL != dest_cs
                && ObServerStatus::STATUS_DEAD != dest_cs->status_)
            {
              dest_cs->server_.to_string(addr_buf2, OB_IP_STR_BUFF);
              minfo->range_.to_string(range_buf, OB_MAX_ROW_KEY_LENGTH*2);
              TBSYS_LOG(INFO, "migrate info, idx=%d src_cs=%s dest_cs=%s range=%s stat=%s src_out=%d dest_in=%d keep_src=%c",
                        idx, addr_buf1, addr_buf2, range_buf, minfo->get_stat_str(),
                        it->balance_info_.curr_migrate_out_num_,
                        dest_cs->balance_info_.curr_migrate_in_num_,
                        minfo->keep_src_?'Y':'N');
              idx++;
            }
          }
        } // end while each migrate candidates
      }
    }
  } // end for
  if (total_in != total_out)
  {
    TBSYS_LOG(ERROR, "BUG total_in=%d total_out=%d", total_in, total_out);
  }
}

void ObRootBalancer::dump_migrate_info() const
{
  TBSYS_LOG(INFO, "balance batch migrate infos, start_us=%ld timeout_us=%ld done=%d total=%d",
            balance_start_time_us_, balance_timeout_us_,
            balance_batch_migrate_done_num_, balance_batch_migrate_count_);
  nb_print_migrate_infos();
}

int ObRootBalancer::nb_check_migrate_timeout()
{
  int ret = OB_SUCCESS;
  int64_t mnow = tbsys::CTimeUtil::getMonotonicTime();
  if (nb_is_in_batch_migrating()
      && balance_timeout_us_ + config_->flag_balance_timeout_us_delta_.get() + balance_start_time_us_ < mnow)
  {
    TBSYS_LOG(WARN, "balance batch migrate timeout, start_us=%ld prev_timeout_us=%ld done=%d total=%d",
              balance_start_time_us_, balance_timeout_us_,
              balance_batch_migrate_done_num_, balance_batch_migrate_count_);
    int64_t elapsed_us = 0;
    if (balance_start_time_us_ < balance_last_migrate_succ_time_)
    {
      elapsed_us = balance_last_migrate_succ_time_ - balance_start_time_us_;
    }
    else
    {
      elapsed_us = mnow - balance_start_time_us_;
    }
    // better guess of timeout
    if (0 >= balance_batch_migrate_done_num_)
    {
      balance_batch_migrate_done_num_ = 1;
    }
    balance_timeout_us_ = balance_batch_migrate_count_ * elapsed_us / balance_batch_migrate_done_num_;
    int64_t balance_max_timeout_us = 1000000LL * config_->flag_balance_max_timeout_seconds_.get();
    if (0 < balance_max_timeout_us
        && balance_timeout_us_ > balance_max_timeout_us)
    {
      balance_timeout_us_ = balance_max_timeout_us;
    }
    // clear
    balance_start_time_us_ = 0;
    balance_batch_migrate_count_ = 0;
    balance_batch_copy_count_ = 0;
    balance_batch_migrate_done_num_ = 0;
    server_manager_->reset_balance_info(config_->flag_balance_max_migrate_out_per_cs_.get());
  }
  return ret;
}

bool ObRootBalancer::nb_is_in_batch_migrating()
{
  return 0 != balance_start_time_us_;
}

void ObRootBalancer::nb_batch_migrate_done()
{
  if (nb_is_in_batch_migrating())
  {
    int64_t mnow = tbsys::CTimeUtil::getMonotonicTime();
    TBSYS_LOG(INFO, "balance batch migrate done, elapsed_us=%ld prev_timeout_us=%ld done=%d",
              mnow - balance_start_time_us_, balance_timeout_us_, balance_batch_migrate_count_);

    server_manager_->reset_balance_info(config_->flag_balance_max_migrate_out_per_cs_.get());
    balance_timeout_us_ = mnow - balance_start_time_us_;
    balance_start_time_us_ = 0;
    balance_batch_migrate_count_ = 0;
    balance_batch_copy_count_ = 0;
    balance_batch_migrate_done_num_ = 0;
  }
}

int ObRootBalancer::nb_trigger_next_migrate(ObRootTable2::iterator it, const common::ObTabletInfo* tablet,
                                            int32_t src_cs_idx, int32_t dest_cs_idx, bool keep_src)
{
  int ret = OB_SUCCESS;
  tbsys::CWLockGuard guard(*server_manager_rwlock_);
  ObServerStatus *src_cs = server_manager_->get_server_status(src_cs_idx);
  ObServerStatus *dest_cs = server_manager_->get_server_status(dest_cs_idx);
  if (NULL == src_cs || NULL == dest_cs || NULL == tablet)
  {
    TBSYS_LOG(WARN, "invalid cs, src_cs_idx=%d dest_cs_id=%d", src_cs_idx, dest_cs_idx);
    ret = OB_INVALID_ARGUMENT;
  }
  else if (ObServerStatus::STATUS_DEAD == src_cs->status_)
  {
    TBSYS_LOG(WARN, "src cs is offline, cs_idx=%d", src_cs_idx);
    ret = OB_ENTRY_NOT_EXIST;
  }
  else if (!nb_is_in_batch_migrating()
           || OB_SUCCESS != (ret = src_cs->balance_info_.migrate_to_.set_migrate_done(tablet->range_, dest_cs_idx)))
  {
    TBSYS_LOG(WARN, "not a recorded migrate, src_cs=%d dest_cs=%d", src_cs_idx, dest_cs_idx);
  }
  else
  {
    it->has_been_migrated();
    src_cs->balance_info_.curr_migrate_out_num_--;
    if (ObServerStatus::STATUS_DEAD != dest_cs->status_)
    {
      dest_cs->balance_info_.curr_migrate_in_num_--;
    }
    balance_batch_migrate_done_num_++;
    balance_last_migrate_succ_time_ = tbsys::CTimeUtil::getMonotonicTime();
    if (keep_src)
    {
      stat_manager_->inc(ObRootStatManager::ROOT_TABLE_ID, ObRootStatManager::INDEX_COPY_COUNT);
    }
    else
    {
      stat_manager_->inc(ObRootStatManager::ROOT_TABLE_ID, ObRootStatManager::INDEX_MIGRATE_COUNT);
    }
    if (balance_batch_migrate_done_num_ >= balance_batch_migrate_count_)
    {
      nb_batch_migrate_done();
    }
    else
    {
      // next migrate
      ret = nb_start_batch_migrate();
    }
  }
  return ret;
}

// 负载均衡入口函数
void ObRootBalancer::do_new_balance()
{
  int ret = OB_SUCCESS;
  balance_batch_migrate_count_ = 0;
  balance_batch_copy_count_ = 0;
  balance_batch_migrate_done_num_ = 0;
  balance_last_migrate_succ_time_ = 0;
  server_manager_->reset_balance_info(config_->flag_balance_max_migrate_out_per_cs_.get());
  delete_list_.reset();
  TBSYS_LOG(DEBUG, "rereplication begin");
  int32_t table_count = nb_get_table_count();
  bool scan_next_table = true;
  for (int32_t i = 0; i < table_count && scan_next_table; ++i) // for each table
  {
    uint64_t table_id = OB_INVALID_ID;
    if (OB_INVALID_ID != (table_id = nb_get_next_table_id(table_count)))
    {
      ret = do_rereplication_by_table(table_id, scan_next_table);
      TBSYS_LOG(DEBUG, "rereplication table, table_id=%lu table_count=%d copy_count=%d delete_count=%ld",
                table_id, table_count, balance_batch_migrate_count_, delete_list_.get_tablet_size());
    }
  }
  TBSYS_LOG(DEBUG, "new balance begin");
  for (int32_t i = 0; i < table_count && scan_next_table; ++i) // for each table
  {
    uint64_t table_id = OB_INVALID_ID;
    if (OB_INVALID_ID != (table_id = nb_get_next_table_id(table_count)))
    {
      ret = nb_balance_by_table(table_id, scan_next_table);
      TBSYS_LOG(DEBUG, "balance table, table_id=%lu table_count=%d migrate_count=%d",
                table_id, table_count, balance_batch_migrate_count_);
    }
  }

  if (0 < delete_list_.get_tablet_size())
  {
    TBSYS_LOG(INFO, "will delete replicas, num=%ld", delete_list_.get_tablet_size());
    ObRootUtil::delete_tablets(*rpc_stub_, *server_manager_, delete_list_, config_->flag_network_timeout_us_.get());
  }
  if (0 < balance_batch_migrate_count_)
  {
    TBSYS_LOG(INFO, "batch migrate begin, count=%d(copy=%d) timeout=%ld",
              balance_batch_migrate_count_, balance_batch_copy_count_, balance_timeout_us_);
    tbsys::CWLockGuard guard(*server_manager_rwlock_);
    balance_start_time_us_ = tbsys::CTimeUtil::getMonotonicTime();
    ret = nb_start_batch_migrate();
  }
}

bool ObRootBalancer::nb_is_all_tables_balanced(const common::ObServer &except_cs)
{
  bool ret = true;
  int32_t table_count = nb_get_table_count();
  for (int32_t i = 0; i < table_count; ++i) // for each table
  {
    uint64_t table_id = OB_INVALID_ID;
    if (OB_INVALID_ID != (table_id = nb_get_next_table_id(table_count, i)))
    {
      int64_t avg_size = 0;
      int64_t avg_count = 0;
      int32_t cs_num = 0;
      int32_t out_per_cs = 0;
      int32_t shutdown_num = 0;
      if(OB_SUCCESS == nb_calculate_sstable_count(table_id, avg_size, avg_count, cs_num, out_per_cs, shutdown_num))
      {
        nb_print_balance_info();
        ret = nb_is_curr_table_balanced(avg_count, except_cs);
        if (!ret)
        {
          TBSYS_LOG(DEBUG, "table not balanced, id=%lu", table_id);
          break;
        }
      }
      else
      {
        ret = false;
        break;
      }
    }
  }
  return ret;
}

bool ObRootBalancer::nb_is_all_tables_balanced()
{
  ObServer not_exist_cs;
  return nb_is_all_tables_balanced(not_exist_cs);
}

void ObRootBalancer::nb_print_balance_infos(char* buf, const int64_t buf_len, int64_t &pos)
{
  int32_t table_count = nb_get_table_count();
  for (int32_t i = 0; i < table_count; ++i) // for each table
  {
    uint64_t table_id = OB_INVALID_ID;
    if (OB_INVALID_ID != (table_id = nb_get_next_table_id(table_count, i)))
    {
      int64_t avg_size = 0;
      int64_t avg_count = 0;
      int32_t cs_num = 0;
      int32_t out_per_cs = 0;
      int32_t shutdown_num = 0;
      if(OB_SUCCESS == nb_calculate_sstable_count(table_id, avg_size, avg_count, cs_num, out_per_cs, shutdown_num))
      {
        databuff_printf(buf, buf_len, pos, "table_id=%lu avg_count=%ld avg_size=%ld cs_num=%d out_per_cs=%d shutdown_num=%d\n",
                        table_id, avg_count, avg_size, cs_num, out_per_cs, shutdown_num);
        databuff_printf(buf, buf_len, pos, "cs sstables_count sstables_size\n");
        nb_print_balance_info(buf, buf_len, pos);
        databuff_printf(buf, buf_len, pos, "--------\n");
      }
    }
  }
}

bool ObRootBalancer::nb_is_all_tablets_replicated(int32_t expected_replicas_num)
{
  bool ret = true;
  ObRootTable2::const_iterator it;
  int32_t replicas_num = 0;
  tbsys::CRLockGuard guard(*root_table_rwlock_);
  for (it = root_table_->begin(); it != root_table_->sorted_end(); ++it)
  {
    replicas_num = 0;
    for (int i = 0; i < OB_SAFE_COPY_COUNT; ++i)
    {
      if (OB_INVALID_INDEX != it->server_info_indexes_[i])
      {
        replicas_num++;
      }
    }
    if (replicas_num < expected_replicas_num)
    {
      TBSYS_LOG(DEBUG, "tablet not replicated, num=%d expected=%d",
                replicas_num, expected_replicas_num);
      ret = false;
      break;
    }
  }
  return ret;
}

bool ObRootBalancer::nb_did_cs_have_no_tablets(const common::ObServer &cs) const
{
  bool ret = false;
  int32_t cs_index = -1;
  {
    tbsys::CRLockGuard guard(*server_manager_rwlock_);
    if (OB_SUCCESS != server_manager_->get_server_index(cs, cs_index))
    {
      TBSYS_LOG(WARN, "cs not exist");
      ret = true;
    }
  }
  if (!ret)
  {
    ret = true;
    ObRootTable2::const_iterator it;
    tbsys::CRLockGuard guard(*root_table_rwlock_);
    for (it = root_table_->begin(); it != root_table_->sorted_end(); ++it)
    {
      for (int i = 0; i < OB_SAFE_COPY_COUNT; ++i)
      {
        if (OB_INVALID_INDEX != it->server_info_indexes_[i]
            && cs_index == it->server_info_indexes_[i])
        {
          ret = false;
          break;
        }
      }
    } // end for
  }
  return ret;
}

namespace oceanbase
{
  namespace rootserver
  {
    namespace balancer
    {
      struct ObShutDownProgress
      {
        ObServer server_;
        int32_t server_idx_;
        int32_t tablet_count_;
        ObShutDownProgress()
          :server_idx_(-1), tablet_count_(0)
        {
        }
      };
    } // end namespace balancer
  } // end namespace rootserver
} // end namespace oceanbase

void ObRootBalancer::nb_print_shutting_down_progress(char *buf, const int64_t buf_len, int64_t& pos)
{
  int ret = OB_SUCCESS;
  ObArray<balancer::ObShutDownProgress> shutdown_servers;
  {
    // find shutting-down servers
    ObChunkServerManager::const_iterator it;
    balancer::ObShutDownProgress sdp;
    tbsys::CRLockGuard guard(*server_manager_rwlock_);
    for (it = server_manager_->begin(); it != server_manager_->end(); ++it)
    {
      if (ObServerStatus::STATUS_SHUTDOWN == it->status_)
      {
        sdp.server_ = it->server_;
        sdp.server_.set_port(it->port_cs_);
        sdp.server_idx_ = static_cast<int32_t>(it - server_manager_->begin());
        sdp.tablet_count_ = 0;
        if (OB_SUCCESS != (ret = shutdown_servers.push_back(sdp)))
        {
          TBSYS_LOG(ERROR, "array push error, err=%d", ret);
          break;
        }
      }
    }
  }
  if (OB_SUCCESS == ret)
  {
    ObRootTable2::const_iterator it;
    tbsys::CRLockGuard guard(*root_table_rwlock_);
    for (it = root_table_->begin(); it != root_table_->sorted_end(); ++it)
    {
      // for each tablet
      for (int i = 0; i < OB_SAFE_COPY_COUNT; ++i)
      {
        // for each replica
        if (OB_INVALID_INDEX != it->server_info_indexes_[i])
        {
          for (int j = 0; j < shutdown_servers.count(); ++j)
          {
            balancer::ObShutDownProgress &sdp = shutdown_servers.at(j);
            if (it->server_info_indexes_[i] == sdp.server_idx_)
            {
              sdp.tablet_count_++;
              break;
            }
          }
        }
      } // end for
    }   // end for
  }
  databuff_printf(buf, buf_len, pos, "shutting-down chunkservers: %ld\n", shutdown_servers.count());
  databuff_printf(buf, buf_len, pos, "chunkserver tablet_count\n");
  for (int j = 0; j < shutdown_servers.count(); ++j)
  {
    balancer::ObShutDownProgress &sdp = shutdown_servers.at(j);
    databuff_printf(buf, buf_len, pos, "%s %d\n", sdp.server_.to_cstring(), sdp.tablet_count_);
  }
}
