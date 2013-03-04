#include "common/ob_scan_param.h"
#include "common/ob_string.h"
#include "common/ob_trace_log.h"
#include "ob_rs_rpc_proxy.h"
#include "ob_ms_service_monitor.h"
#include "ob_ms_tablet_location.h"
#include "ob_ms_tablet_location_proxy.h"

using namespace oceanbase::common;
using namespace oceanbase::mergeserver;

ObMergerLocationCacheProxy::ObMergerLocationCacheProxy()
{
  cur_succ_count_ = 0;
  cur_fail_count_ = 0;
  old_succ_count_ = 0;
  old_fail_count_ = 0;
  root_rpc_ = NULL;
  cache_monitor_ = NULL;
  tablet_cache_ = NULL;
}

ObMergerLocationCacheProxy::ObMergerLocationCacheProxy(const ObServer & server,
  ObMergerRootRpcProxy * rpc, ObMergerTabletLocationCache * cache,
  ObMergerServiceMonitor * monitor /* =NULL */)
{
  // set max rowkey as 0xFF
  memset(max_rowkey_, 0xFF, sizeof(max_rowkey_));
  merge_server_ = server;
  root_rpc_ = rpc;
  tablet_cache_ = cache;
  cache_monitor_ = monitor;
}

ObMergerLocationCacheProxy::~ObMergerLocationCacheProxy()
{
}

int ObMergerLocationCacheProxy::del_cache_item(const uint64_t table_id, const ObString & search_key)
{
  int ret = OB_SUCCESS;
  if ((NULL == search_key.ptr()) || (0 == search_key.length()))
  {
    TBSYS_LOG(ERROR, "check search key failed:table[%lu], ptr[%p]", table_id, search_key.ptr());
    ret = OB_INPUT_PARAM_ERROR;
  }
  else
  {
    ret = tablet_cache_->del(table_id, search_key);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(DEBUG, "del cache item failed:table_id[%lu], ret[%d]", table_id, ret);
      hex_dump(search_key.ptr(), search_key.length(), true);
    }
    else
    {
      TBSYS_LOG(DEBUG, "del cache item succ:table_id[%lu]", table_id);
    }
  }
  return ret;
}

int ObMergerLocationCacheProxy::del_cache_item(const ObScanParam & scan_param)
{
  int ret = OB_SUCCESS;
  const ObRange * range = scan_param.get_range();
  if (NULL == range)
  {
    TBSYS_LOG(ERROR, "check scan param range failed:range[%p]", range);
    ret = OB_INPUT_PARAM_ERROR;
  }
  else
  {
    ObString search_key;
    char * temp = NULL;
    ret = get_search_key(scan_param, search_key, &temp); 
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "get search key failed:ret[%d]", ret);
      range->hex_dump();
    }
    else
    {
      ret = del_cache_item(range->table_id_, search_key);
      if (ret != OB_SUCCESS)
      {
        TBSYS_LOG(DEBUG, "del cache item failed:table_id[%lu], ret[%d]", range->table_id_, ret);
      }
    }
    reset_search_key(search_key, temp);
  }
  return ret;
}

int ObMergerLocationCacheProxy::update_cache_item(const uint64_t table_id, const ObString & rowkey, 
  const ObMergerTabletLocationList & list)
{
  int ret = OB_SUCCESS;
  if ((NULL == rowkey.ptr()) || (0 == rowkey.length()))
  {
    TBSYS_LOG(ERROR, "check rowkey failed:table_id[%lu], rowkey[%p]", table_id, rowkey.ptr());
  }
  else
  {
    ret = tablet_cache_->update(table_id, rowkey, list);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(DEBUG, "update cache item failed:table_id[%lu], ret[%d]", table_id, ret);
    }
    else
    {
      TBSYS_LOG(DEBUG, "update cache item succ:table_id[%lu]", table_id);
    }
  }
  return ret;
}

int ObMergerLocationCacheProxy::update_cache_item(const ObScanParam & scan_param, 
  const ObMergerTabletLocationList & location)
{
  int ret = OB_SUCCESS;
  const ObRange * range = scan_param.get_range();
  if (NULL == range)
  {
    TBSYS_LOG(ERROR, "check scan param range failed:range[%p]", range);
    ret = OB_INPUT_PARAM_ERROR;
  }
  else
  {
    ObString search_key;
    char * temp = NULL;
    ret = get_search_key(scan_param, search_key, &temp); 
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "get search key failed:ret[%d]", ret);
      range->hex_dump();
    }
    else
    {
      ret = update_cache_item(range->table_id_, search_key, location);
      if (ret != OB_SUCCESS)
      {
        TBSYS_LOG(DEBUG, "update cache item failed:table_id[%lu], ret[%d]", range->table_id_, ret);
      }
    }
    reset_search_key(search_key, temp);
  }
  return ret;
}

int ObMergerLocationCacheProxy::server_fail(const common::ObScanParam & scan_param,
  ObMergerTabletLocationList & list, const ObServer & server)
{
  int ret = get_tablet_location(scan_param, list);
  if (ret != OB_SUCCESS)
  {
    TBSYS_LOG(WARN, "fail to get tablet location cache:ret[%d]", ret);
  }
  else
  {
    for (int64_t i = 0; i < list.size(); i ++)
    {
      if (list[i].server_.chunkserver_ == server)
      {
        list[i].err_times_ ++;
        if (OB_SUCCESS != (ret = update_cache_item(scan_param, list)))
        {
          TBSYS_LOG(WARN, "fail to update cache item:ret[%d]", ret);
        }
        break;
      }
    }
  }
  return ret;
}

int ObMergerLocationCacheProxy::server_fail(const uint64_t table_id, const ObString & search_key,
  ObMergerTabletLocationList & list, const ObServer & server)
{
  int ret = get_tablet_location(table_id, search_key, list);
  if (ret != OB_SUCCESS)
  {
    TBSYS_LOG(WARN, "fail to get tablet location cache:ret[%d]", ret);
  }
  else
  {
    for (int64_t i = 0; i < list.size(); i ++)
    {
      if (list[i].server_.chunkserver_ == server)
      {
        list[i].err_times_ ++;
        if (OB_SUCCESS != (ret = update_cache_item(table_id, search_key, list)))
        {
          TBSYS_LOG(WARN, "fail to update cache item:ret[%d]", ret);
        }
        break;
      }
    }
  }
  return ret;
}

//
int ObMergerLocationCacheProxy::set_item_invalid(const uint64_t table_id, const ObString & rowkey,
  const ObMergerTabletLocation & addr, ObMergerTabletLocationList & list)
{
  assert(list.get_buffer() != NULL);
  int ret = tablet_cache_->get(table_id, rowkey, list);
  if (OB_SUCCESS != ret)
  {
    TBSYS_LOG(WARN, "get cache item failed:table_id[%lu], ret[%d]",
      table_id, ret);
  }
  else
  {
    ret = list.set_item_invalid(addr);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "set item invalid failed:table_id[%lu], ret[%d]",
        table_id, ret);
    }
    else
    {
      ret = tablet_cache_->update(table_id, rowkey, list);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(DEBUG, "update cache item failed:ret[%d]", ret);
      }
      else
      {
        TBSYS_LOG(DEBUG, "%s", "set item invalid succ");
      }
    }
  }
  return ret;
}

// set item server invalid
int ObMergerLocationCacheProxy::set_item_invalid(const ObScanParam & scan_param,
  const ObMergerTabletLocation & addr, ObMergerTabletLocationList & list)
{
  int ret = OB_SUCCESS;
  const ObRange * range = scan_param.get_range();
  if (NULL == range)
  {
    TBSYS_LOG(ERROR, "check scan param range failed:range[%p]", range);
    ret = OB_INPUT_PARAM_ERROR;
  }
  else
  {
    ObString search_key;
    char * temp = NULL;
    int ret = get_search_key(scan_param, search_key, &temp); 
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "get search key failed:ret[%d]", ret);
      range->hex_dump();
    }
    else
    {
      ret = set_item_invalid(range->table_id_, search_key, addr, list);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(DEBUG, "set item invalid failed:table_id[%lu], ret[%d]", range->table_id_, ret);
      }
    }
    // reset search key buffer
    reset_search_key(search_key, temp);
  }
  return ret;
}

int ObMergerLocationCacheProxy::get_search_key(const ObScanParam::Direction scan_direction,
  const common::ObRange * const range, common::ObString & search_key, char ** new_buffer)
{
  int ret = OB_SUCCESS;
  if ((NULL == range) || (NULL == new_buffer))
  {
    TBSYS_LOG(ERROR, "check scan param range or new_buffer failed:range[%p], buffer[%p]",
      range, new_buffer);
    ret = OB_INPUT_PARAM_ERROR;
  }
  else
  {
    *new_buffer = NULL;
    if (ObScanParam::FORWARD == scan_direction)
    {
      search_key = range->start_key_;
      if (!range->border_flag_.inclusive_start() || range->border_flag_.is_min_value())
      {
        uint64_t length = 0;
        if (!range->border_flag_.is_min_value())
        {
          length = range->start_key_.length();
        }
        // if min value start_key.length must equal 0
        *new_buffer = (char *)ob_malloc(length + 1, ObModIds::OB_MS_RPC);
        if (NULL == *new_buffer)
        {
          TBSYS_LOG(ERROR, "ob malloc failed:ret[%d]", ret);
          ret = OB_ALLOCATE_MEMORY_FAILED;
        }
        else
        {
          if (length > 0)
          {
            memcpy(*new_buffer, range->start_key_.ptr(), length);
          }
          // add 0 to seach key for gt than start_key
          (*new_buffer)[length] = 0;
          search_key.assign(*new_buffer, static_cast<int32_t>(length + 1)); 
        }
      }
    }
    else
    {
      if (range->border_flag_.is_max_value())
      {
        // WARN: set eight FF as max row key
        search_key.assign(max_rowkey_, sizeof(max_rowkey_));
      }
      else
      {
        search_key = range->end_key_;
      }
    }
  }
  return ret;
}

int ObMergerLocationCacheProxy::get_search_key(const ObScanParam & scan_param,
  ObString & search_key, char ** new_buffer)
{
  int ret = OB_SUCCESS;
  if ((NULL == scan_param.get_range()) || (NULL == new_buffer))
  {
    TBSYS_LOG(ERROR, "check scan param range or new_buffer failed:range[%p], buffer[%p]",
      scan_param.get_range(), new_buffer);
    ret = OB_INPUT_PARAM_ERROR;
  }

  if (OB_SUCCESS == ret && 
    OB_SUCCESS != (ret = get_search_key(scan_param.get_scan_direction(), scan_param.get_range(),
    search_key, new_buffer)))
  {
    TBSYS_LOG(WARN, "get_search_key fail");
  }
  return ret;
}

// can not use search key after reset buffer
void ObMergerLocationCacheProxy::reset_search_key(ObString & search_key, char * buffer)
{
  // delete after using
  if (NULL != buffer)
  {
    TBSYS_LOG(DEBUG, "%s", "delete the temp buff for new search key");
    if (buffer != search_key.ptr())
    {
      TBSYS_LOG(ERROR, "check temp buff not equal with search_key ptr:"
        "ptr[%p], temp[%p]", search_key.ptr(), buffer);
    }
    else
    {
      // reset search key ptr
      ob_free(buffer);
    }
  }
}

int ObMergerLocationCacheProxy::get_tablet_location(const ObScanParam::Direction scan_direction,
  const common::ObRange * range, ObMergerTabletLocationList & list)
{
  int ret = OB_SUCCESS;
  ObString search_key;
  if (NULL == range)
  {
    TBSYS_LOG(ERROR, "check scan param range failed:range[%p]", range);
    ret = OB_INPUT_PARAM_ERROR;
  }
  else
  {
    char * temp = NULL;
    ret = get_search_key(scan_direction, range, search_key, &temp);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "get search key failed:table_id[%lu], ret[%d]", range->table_id_, ret);
      range->hex_dump();
    }
    else
    {
      ret = get_tablet_location(range->table_id_, search_key, list);
      if (ret != OB_SUCCESS)
      {
        TBSYS_LOG(WARN, "get tablet location failed:table_id[%lu], ret[%d]",
          range->table_id_, ret);
        range->hex_dump();
        hex_dump(search_key.ptr(), search_key.length(), true);
      }
    }
    reset_search_key(search_key, temp);
  }
  return ret;

}

// get the first tablet location through range, according to the range
int ObMergerLocationCacheProxy::get_tablet_location(const ObScanParam & scan_param,
  ObMergerTabletLocationList & list)
{
  int ret = OB_SUCCESS;
  if (NULL == scan_param.get_range())
  {
    TBSYS_LOG(ERROR, "check scan param range failed:range[%p]", scan_param.get_range());
    ret = OB_INPUT_PARAM_ERROR;
  }
  else
  {
    ret = get_tablet_location(scan_param.get_scan_direction(), scan_param.get_range(), list);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "get tablet location fail:ret[%d]", ret);
    }
  }
  return ret;
}

int ObMergerLocationCacheProxy::renew_location_item(const uint64_t table_id,
  const ObString & row_key, ObMergerTabletLocationList & location)
{
  // Warning: other get new tablet location thread will locked by one thread
  TBSYS_LOG(DEBUG, "local tablet location cache not exist:table_id[%lu]", table_id);
  // lock and check again
  tbsys::CThreadGuard lock(lock_holder_.acquire_lock());
  TBSYS_LOG(DEBUG, "cache not exist check local cache with lock:table_id[%lu]", table_id);
  int ret = tablet_cache_->get(table_id, row_key, location);
  if (ret != OB_SUCCESS)
  {
    if (ret != OB_ENTRY_NOT_EXIST)
    {
      TBSYS_LOG(WARN, "get from cache failed:table_id[%lu], ret[%d]", table_id, ret);
    }
    atomic_inc(&cur_fail_count_);
    // step 2. scan root server and renew the cache
    ret = root_rpc_->scan_root_table(tablet_cache_, table_id, row_key, merge_server_, location);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "get tablet location failed:table_id[%lu], ret[%d]", table_id, ret);
      hex_dump(row_key.ptr(), row_key.length(), true);
    }
    else if (0 == location.size())
    {
      ret = OB_INNER_STAT_ERROR;
      TBSYS_LOG(WARN, "check scan root table failed:table_id[%lu], count[%ld]",
        table_id, location.size());
    }
  }
  else
  {
    TBSYS_LOG(DEBUG, "cache item is fetched by other thread:table_id[%lu]", table_id);
  }
  return ret;
}

int ObMergerLocationCacheProxy::update_timeout_item(const uint64_t table_id, 
  const ObString & row_key, ObMergerTabletLocationList & location)
{
  int ret = OB_SUCCESS;
  int64_t timestamp = tbsys::CTimeUtil::getTime();
  // check location cache item timeout
  if (timestamp - location.get_timestamp() > tablet_cache_->get_timeout())
  {
    // lock and check again
    TBSYS_LOG(DEBUG, "cache timeout get local cache with lock:table_id[%lu]", table_id);
    tbsys::CThreadGuard lock(lock_holder_.acquire_lock());
    ret = tablet_cache_->get(table_id, row_key, location);
    if ((ret != OB_SUCCESS) || (timestamp - location.get_timestamp()
      > tablet_cache_->get_timeout()))
    {
      if ((ret != OB_ENTRY_NOT_EXIST) && (OB_SUCCESS != ret))
      {
        TBSYS_LOG(WARN, "get from cache failed:table_id[%lu], ret[%d]", table_id, ret);
      }
      atomic_inc(&cur_fail_count_);
      ret = root_rpc_->scan_root_table(tablet_cache_, table_id, row_key, merge_server_, location);
      // if root server die renew the item cache timestamp for longer alive time
      if (ret != OB_SUCCESS)
      {
        TBSYS_LOG(WARN, "get tablet location from root failed:table_id[%lu], length[%d], ret[%d]",
            table_id, row_key.length(), ret);
        // update location list timestamp for root server failed
        location.set_timestamp(timestamp);
        ret = tablet_cache_->update(table_id, row_key, location);
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(WARN, "update cache item failed:table_id[%lu], length[%d], ret[%d]", table_id,
              row_key.length(), ret);
        }
      }
    }
    else
    {
      atomic_inc(&cur_succ_count_);
      TBSYS_LOG(DEBUG, "already update the cache item by other thread:table_id[%lu]", table_id);
    }
  }
  else
  {
    atomic_inc(&cur_succ_count_);
  }
  return ret;
}

// get tablet location through rowkey, return the cs includeing this row_key data
int ObMergerLocationCacheProxy::get_tablet_location(const uint64_t table_id,
  const ObString & row_key, ObMergerTabletLocationList & location)
{
  int ret = OB_SUCCESS;
  if (!check_inner_stat())
  {
    TBSYS_LOG(ERROR, "%s", "check inner stat failed");
    ret = OB_INNER_STAT_ERROR;
  }
  else
  {
    bool hit_cache = false;
    assert(location.get_buffer() != NULL);
    ret = tablet_cache_->get(table_id, row_key, location);
    if (ret != OB_SUCCESS)
    {
      if (ret != OB_ENTRY_NOT_EXIST)
      {
        TBSYS_LOG(WARN, "get from cache failed:table_id[%lu], ret[%d]", table_id, ret);
      }
      // scan root table insert new location item to cache
      ret = renew_location_item(table_id, row_key, location);
      if (ret != OB_SUCCESS)
      {
        TBSYS_LOG(WARN, "fetch tablet location through rpc call failed:table_id[%lu], ret[%d]",
            table_id, ret);
      }
    }
    else
    {
      // check timeout item for lazy washout
      int err = update_timeout_item(table_id, row_key, location);
      if (err != OB_SUCCESS)
      {
        TBSYS_LOG(WARN, "update timeout item failed:table_id[%lu], ret[%d]", table_id, err);
      }
      // check invalid location
      if (0 == location.get_valid_count() || 0 == location.size())
      {
        TBSYS_LOG(WARN, "check all the chunk server invalid:size[%ld]", location.size());
        err = del_cache_item(table_id, row_key);
        if (err != OB_SUCCESS)
        {
          TBSYS_LOG(WARN, "del invalid cache item failed:table_id[%lu], ret[%d]", table_id, err);
        }
        // scan root table insert new location item to cache
        ret = renew_location_item(table_id, row_key, location);
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(WARN, "fetch tablet location through rpc call failed:table_id[%lu], ret[%d]",
              table_id, ret);
        }
      }
      else
      {
        hit_cache = true;
      }
    }
    // inc hit cache monitor
    inc_cache_monitor(table_id, hit_cache);
  }
  return ret;
}

void ObMergerLocationCacheProxy::inc_cache_monitor(const uint64_t table_id, const bool hit_cache)
{
  if (cache_monitor_ != NULL)
  {
    if (false == hit_cache)
    {
      cache_monitor_->inc(table_id, ObMergerServiceMonitor::MISS_CS_CACHE_COUNT);
    }
    else
    {
      cache_monitor_->inc(table_id, ObMergerServiceMonitor::HIT_CS_CACHE_COUNT);
    }
  }
}

void ObMergerLocationCacheProxy::dump(void) const
{
  int64_t req_count = (cur_succ_count_ + cur_fail_count_) - (old_succ_count_ + old_fail_count_);
  if (req_count != 0)
  {
    int64_t hit_count = cur_succ_count_ - old_succ_count_;
    double ratio = static_cast<double>(hit_count) / static_cast<double>(req_count) * 100;
    TBSYS_LOG(INFO, "tablet location cache proxy counter:total[%ld], hit[%ld], ratio[%.2lf]",
        req_count, hit_count, ratio);
  }
  old_succ_count_ = cur_succ_count_;
  old_fail_count_ = cur_fail_count_;
}

