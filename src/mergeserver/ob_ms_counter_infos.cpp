/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or 
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_ms_counter_infos.cpp for 
 *
 * Authors:
 *   wushi <wushi.ly@taobao.com>
 *
 */
#include "ob_ms_counter_infos.h"
#include "pthread.h"
using namespace oceanbase;
using namespace oceanbase::mergeserver;
namespace oceanbase
{
  namespace mergeserver
  {
    ObMergerCounterInfo OB_COUNTER_SET[G_MAX_COUNTER_NUM];
  }
}

const char *oceanbase::mergeserver::ObMergerCounterInfoSet::get_counter_name(const int64_t counter_id) const
{
  static const char *unknown_counter = "unkonw_counter";
  const char *res = unknown_counter;
  if ((counter_id <= ObMergerCounterIds::C_MS_COUNTER_BEG) 
    || (counter_id >= ObMergerCounterIds::C_MS_COUNTER_END))
  {
  }
  else
  {
    res = OB_COUNTER_SET[counter_id].counter_name_;
  }
  return res;
}

int64_t oceanbase::mergeserver::ObMergerCounterInfoSet::get_counter_static_interval(const int64_t counter_id)const
{
  int64_t res = 0;
  if ((counter_id <= ObMergerCounterIds::C_MS_COUNTER_BEG) 
    || (counter_id >= ObMergerCounterIds::C_MS_COUNTER_END))
  {
  }
  else
  {
    res = OB_COUNTER_SET[counter_id].static_interval_;
  }
  return res;
}


int64_t oceanbase::mergeserver::ObMergerCounterInfoSet::get_max_counter_size()const
{
  return ObMergerCounterIds::C_MS_COUNTER_END;
}



ObMergerCounterInfoSet &oceanbase::mergeserver::ms_get_counter_infos()
{
  static ObMergerCounterInfoSet ms_counter_info_set;
  static pthread_once_t once_ctrl = PTHREAD_ONCE_INIT;
  pthread_once(&once_ctrl, init_ob_counter_set);
  return ms_counter_info_set;
}

oceanbase::common::ObCounterSet &oceanbase::mergeserver::ms_get_counter_set()
{
  static oceanbase::common::ObCounterSet ms_counter_set;
  return ms_counter_set;
}
