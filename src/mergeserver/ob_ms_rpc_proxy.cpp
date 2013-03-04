#include "ob_ms_rpc_proxy.h"
#include "ob_ms_rpc_stub.h"

#include "common/utility.h"
#include "common/ob_crc64.h"
#include "common/ob_schema.h"
#include "common/ob_scanner.h"
#include "common/ob_mutator.h"
#include "common/ob_read_common_data.h"
#include "common/ob_trace_log.h"

#include "ob_ms_schema_manager.h"
#include "ob_ms_service_monitor.h"
#include "ob_ms_tablet_location.h"
#include "ob_ms_tablet_location_item.h"

using namespace oceanbase::common;
using namespace oceanbase::mergeserver;

ObMergerRpcProxy::ObMergerRpcProxy(const ObServerType type):ups_list_lock_(tbsys::WRITE_PRIORITY)
{
  init_ = false;
  rpc_stub_ = NULL;
  monitor_ = NULL;
  rpc_retry_times_ = 0;
  rpc_timeout_ = 0;
  min_fetch_interval_ = 10 * 1000 * 1000L;
  fetch_ups_timestamp_ = 0;
  cur_finger_print_ = 0;
  fail_count_threshold_ = 100;
  black_list_timeout_ = 60 * 1000 * 1000L;
  memset(max_rowkey_, 0xFF, sizeof(max_rowkey_));
  fail_count_threshold_ = 100;
  fetch_schema_timestamp_ = 0;
  server_type_ = type;
  tablet_cache_ = NULL;
  schema_manager_ = NULL;
}

ObMergerRpcProxy::ObMergerRpcProxy(const int64_t retry_times, const int64_t timeout,
    const ObServer & root_server, const ObServer & merge_server,
    const ObServerType type):ups_list_lock_(tbsys::WRITE_PRIORITY)
{
  init_ = false;
  rpc_stub_ = NULL;
  monitor_ = NULL;
  rpc_retry_times_ = retry_times;
  rpc_timeout_ = timeout;
  root_server_ = root_server;
  merge_server_ = merge_server;
  server_type_ = type;
  min_fetch_interval_ = 10 * 1000 * 1000L;
  fetch_ups_timestamp_ = 0;
  cur_finger_print_ = 0;
  fail_count_threshold_ = 20;
  black_list_timeout_ = 100 * 1000 * 1000L;
  memset(max_rowkey_, 0xFF, sizeof(max_rowkey_));
  fail_count_threshold_ = 100;
  black_list_timeout_ = 60 * 1000 * 1000L;
  fetch_schema_timestamp_ = 0;
  tablet_cache_ = NULL;
  schema_manager_ = NULL;
}

ObMergerRpcProxy::~ObMergerRpcProxy()
{
}

bool ObMergerRpcProxy::check_inner_stat(void) const
{
  return(init_ && (NULL != rpc_stub_) && (NULL != tablet_cache_)
      && (NULL != schema_manager_));
}

void ObMergerRpcProxy::set_min_interval(const int64_t interval)
{
  min_fetch_interval_ = interval;
}

int ObMergerRpcProxy::init(ObMergerRpcStub * rpc_stub,
    ObMergerSchemaManager * schema, ObMergerTabletLocationCache * cache,
    ObMergerServiceMonitor * monitor)
{
  int ret = OB_SUCCESS;
  if ((NULL == rpc_stub) || (NULL == schema) || (NULL == cache))
  {
    TBSYS_LOG(ERROR, "check schema or tablet cache failed:"
        "rpc[%p], schema[%p], cache[%p], monitor[%p]", rpc_stub, schema, cache, monitor);
    ret = OB_INPUT_PARAM_ERROR;
  }
  else if (true == init_)
  {
    TBSYS_LOG(ERROR, "%s", "check already inited");
    ret = OB_INNER_STAT_ERROR;
  }
  else
  {
    rpc_stub_ = rpc_stub;
    schema_manager_ = schema;
    tablet_cache_ = cache;
    monitor_ = monitor;
    init_ = true;
  }
  return ret;
}

int ObMergerRpcProxy::fetch_update_server_list(int32_t & count)
{
  int ret = OB_SUCCESS;
  if (!check_inner_stat())
  {
    TBSYS_LOG(ERROR, "%s", "check inner stat failed");
    ret = OB_ERROR;
  }
  else
  {
    ObUpsList list;
    ret = rpc_stub_->fetch_server_list(rpc_timeout_, root_server_, list);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "fetch server list from root server failed:ret[%d]", ret);
    }
    else
    {
      count = list.ups_count_;
      // if has error modify the list
      modify_ups_list(list);
      // check finger print changed
      uint64_t finger_print = ob_crc64(&list, sizeof(list));
      if (finger_print != cur_finger_print_)
      {
        TBSYS_LOG(INFO, "ups list changed succ:cur[%lu], new[%lu]", cur_finger_print_, finger_print);
        list.print();
        tbsys::CWLockGuard lock(ups_list_lock_);
        find_master_ups(list, master_update_server_);
        cur_finger_print_ = finger_print;
        memcpy(&update_server_list_, &list, sizeof(update_server_list_));
        // init update server blacklist fail_count threshold, timeout
        ret = black_list_.init(static_cast<int32_t>(fail_count_threshold_), black_list_timeout_,
            server_type_, update_server_list_);
        if (ret != OB_SUCCESS)
        {
          // if failed use old blacklist info
          TBSYS_LOG(ERROR, "init black list failed use old blacklist info:ret[%d]", ret);
        }
      }
      else
      {
        TBSYS_LOG(DEBUG, "ups list not changed:finger[%lu], count[%d]", finger_print, list.ups_count_);
      }
    }
  }
  return ret;
}

void ObMergerRpcProxy::find_master_ups(const ObUpsList & list, ObServer & master)
{
  for (int64_t i = 0; i < list.ups_count_; ++i)
  {
    if (UPS_MASTER == list.ups_array_[i].stat_)
    {
      master = list.ups_array_[i].addr_;
      break;
    }
  }
}

int ObMergerRpcProxy::release_schema(const ObSchemaManagerV2 * manager)
{
  int ret = OB_SUCCESS;
  if (!check_inner_stat() || (NULL == manager))
  {
    TBSYS_LOG(ERROR, "%s", "check inner stat failed");
    ret = OB_ERROR;
  }
  else
  {
    ret = schema_manager_->release_schema(manager->get_version());
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "release scheam failed:schema[%p], timestamp[%ld]",
          manager, manager->get_version());
    }
  }
  return ret;
}

int ObMergerRpcProxy::get_schema(const int64_t timestamp, const ObSchemaManagerV2 ** manager)
{
  int ret = OB_SUCCESS;
  if (!check_inner_stat() || (NULL == manager))
  {
    TBSYS_LOG(ERROR, "%s", "check inner stat failed");
    ret = OB_ERROR;
  }
  else
  {
    switch (timestamp)
    {
    // local newest version
    case LOCAL_NEWEST:
      {
        *manager = schema_manager_->get_schema(0);
        break;
      }
    // get server new version with old timestamp
    default:
      {
        ret = get_new_schema(timestamp, manager);
      }
    }
    // check shema data
    if ((ret != OB_SUCCESS) || (NULL == *manager))
    {
      TBSYS_LOG(DEBUG, "check get schema failed:schema[%p], version[%ld], ret[%d]",
          *manager, timestamp, ret);
    }
  }
  return ret;
}


int ObMergerRpcProxy::get_new_schema(const int64_t timestamp, const ObSchemaManagerV2 ** manager)
{
  int ret = OB_SUCCESS;
  // check update timestamp LEAST_FETCH_SCHMEA_INTERVAL
  if (tbsys::CTimeUtil::getTime() - fetch_schema_timestamp_ < LEAST_FETCH_SCHEMA_INTERVAL)
  {
    TBSYS_LOG(WARN, "check last fetch schema timestamp is too nearby:version[%ld]", timestamp);
    ret = OB_OP_NOT_ALLOW;
  }
  else
  {
    int64_t new_version = 0;
    ret = rpc_stub_->fetch_schema_version(rpc_timeout_, root_server_, new_version);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "fetch schema version failed:ret[%d]", ret);
    }
    else if (new_version <= timestamp)
    {
      TBSYS_LOG(DEBUG, "check local version not older than root version:local[%ld], root[%ld]",
        timestamp, new_version);
      ret = OB_NO_NEW_SCHEMA;
    }
    else
    {
      ret = fetch_new_schema(new_version, manager);
      if (ret != OB_SUCCESS)
      {
        TBSYS_LOG(WARN, "fetch new schema failed:local[%ld], root[%ld], ret[%d]",
          timestamp, new_version, ret);
      }
    }
  }
  return ret;
}


int ObMergerRpcProxy::fetch_new_schema(const int64_t timestamp, const ObSchemaManagerV2 ** manager)
{
  int ret = OB_SUCCESS;
  if (NULL == manager)
  {
    TBSYS_LOG(WARN, "check shema manager param failed:manager[%p]", manager);
    ret = OB_INPUT_PARAM_ERROR;
  }
  else
  {
    tbsys::CThreadGuard lock(&schema_lock_);
    if (schema_manager_->get_latest_version() >= timestamp)
    {
      *manager = schema_manager_->get_schema(0);
      if (NULL == *manager)
      {
        TBSYS_LOG(WARN, "get latest but local schema failed:schema[%p], latest[%ld]", 
          *manager, schema_manager_->get_latest_version());
        ret = OB_INNER_STAT_ERROR;
      }
      else
      {
        TBSYS_LOG(DEBUG, "get new schema is fetched by other thread:schema[%p], latest[%ld]",
          *manager, (*manager)->get_version());
      }
    }
    else
    {
      char * temp = (char *)ob_malloc(sizeof(ObSchemaManagerV2),ObModIds::OB_MS_RPC);
      if (NULL == temp)
      {
        TBSYS_LOG(ERROR, "%s", "check ob malloc failed");
        ret = OB_ALLOCATE_MEMORY_FAILED;
      }
      else
      {
        ObSchemaManagerV2 * schema = new(temp) ObSchemaManagerV2;
        if (NULL == schema)
        {
          TBSYS_LOG(ERROR, "check replacement new schema failed:schema[%p]", schema);
          ret = OB_INNER_STAT_ERROR;
        }
        else
        {
          ret = rpc_stub_->fetch_schema(rpc_timeout_, root_server_, 0, *schema);
          if (ret != OB_SUCCESS)
          {
            TBSYS_LOG(WARN, "rpc fetch schema failed:version[%ld], ret[%d]", timestamp, ret);
          }
          else
          {
            fetch_schema_timestamp_ = tbsys::CTimeUtil::getTime();
            ret = schema_manager_->add_schema(*schema, manager);
            // maybe failed because of timer thread fetch and add it already
            if (OB_SUCCESS != ret)
            {
              TBSYS_LOG(WARN, "add new schema failed:version[%ld], ret[%d]", schema->get_version(), ret);
              ret = OB_SUCCESS;
              *manager = schema_manager_->get_schema(0);
              if (NULL == *manager)
              {
                TBSYS_LOG(WARN, "get latest schema failed:schema[%p], latest[%ld]", 
                    *manager, schema_manager_->get_latest_version());
                ret = OB_INNER_STAT_ERROR;
              }
            }
            else
            {
              TBSYS_LOG(DEBUG, "fetch and add new schema succ:version[%ld]", schema->get_version());
            }
          }
        }
        schema->~ObSchemaManagerV2();
        ob_free(temp);
        temp = NULL;
      }
    }
  }
  return ret;
}

int ObMergerRpcProxy::fetch_schema_version(int64_t & timestamp)
{
  int ret = OB_SUCCESS;
  if (!check_inner_stat())
  {
    TBSYS_LOG(ERROR, "%s", "check inner stat failed");
    ret = OB_INNER_STAT_ERROR;
  }
  else
  {
    ret = rpc_stub_->fetch_schema_version(rpc_timeout_, root_server_, timestamp);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "fetch schema version failed:ret[%d]", ret);
    }
    else
    {
      TBSYS_LOG(DEBUG, "fetch schema version succ:version[%ld]", timestamp);
    }
  }
  return ret;
}

int ObMergerRpcProxy::fetch_new_version(int64_t & frozen_version)
{
  int ret = OB_SUCCESS;
  if (!check_inner_stat())
  {
    TBSYS_LOG(ERROR, "%s", "check inner stat failed");
    ret = OB_INNER_STAT_ERROR;
  }
  else
  {
    ObServer update_server;
    ret = get_master_ups(false, update_server);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "get master update server failed:ret[%d]", ret);
    }
    else
    {
      ret = rpc_stub_->fetch_frozen_version(rpc_timeout_, update_server, frozen_version);
      if (ret != OB_SUCCESS)
      {
        TBSYS_LOG(WARN, "fetch frozen version failed:ret[%d]", ret);
      }
      else
      {
        TBSYS_LOG(DEBUG, "fetch frozen version succ:version[%ld]", frozen_version);
      }
    }
  }
  return ret;
}

// get the first tablet location through range, according to the range
int ObMergerRpcProxy::get_first_tablet_location(const ObScanParam & scan_param, ObString & search_key,
    char ** temp, ObMergerTabletLocationList & list)
{
  int ret = OB_SUCCESS;
  const ObRange * range = scan_param.get_range();
  if ((NULL == range) || (NULL == temp))
  {
    TBSYS_LOG(ERROR, "check scan param range or temp buffer failed:range[%p], temp[%p]", range, temp);
    ret = OB_INPUT_PARAM_ERROR;
  }
  else
  {
    ret = get_search_key(scan_param, search_key, temp);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "get search key failed:table_id[%lu], ret[%d]", range->table_id_, ret);
      range->hex_dump();
    }
    else
    {
      ret = get_tablet_location(range->table_id_, search_key, list);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "get tablet location failed:table_id[%lu], ret[%d]", 
            range->table_id_, ret);
        range->hex_dump();
        hex_dump(search_key.ptr(), search_key.length(), true);
      }
      else
      {
        TBSYS_LOG(DEBUG, "get tablet location list succ:table_id[%lu]", range->table_id_);
      }
    }
  }
  return ret;
}

// get tablet location through rowkey, return the cs includeing this row_key data
int ObMergerRpcProxy::get_tablet_location(const uint64_t table_id, const ObString & row_key,
    ObMergerTabletLocationList & location)
{
  int ret = OB_SUCCESS;
  if (!check_inner_stat())
  {
    TBSYS_LOG(ERROR, "%s", "check inner stat failed");
    ret = OB_INNER_STAT_ERROR;
  }
  else
  {
    bool miss_cache_hit = false;
    // step 1: search in the tablet location cache includeing the row_key
    ret = tablet_cache_->get(table_id, row_key, location);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(DEBUG, "local tablet location cache not exist:table_id[%lu]", table_id);
      // Warning: other get new tablet location thread will locked by one thread
      // lock and check again
      tbsys::CThreadGuard lock(&cache_lock_);
      TBSYS_LOG(DEBUG, "cache not exist get local tablet location cache with lock:table_id[%lu]", table_id);
      ret = tablet_cache_->get(table_id, row_key, location);
      if (ret != OB_SUCCESS)
      {
        miss_cache_hit = true;
        // step 2. scan root server and renew the cache
        ret = scan_root_table(table_id, row_key, location);
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "get tablet location failed:table_id[%lu], length[%d], ret[%d]",
              table_id, row_key.length(), ret);
        }
      }
    }
    else
    {
      int64_t timestamp = tbsys::CTimeUtil::getTime();
      if (timestamp - location.get_timestamp() > tablet_cache_->get_timeout())
      {
        // lock and check again
        tbsys::CThreadGuard lock(&cache_lock_);
        TBSYS_LOG(DEBUG, "cache timeout get local tablet location cache with lock:table_id[%lu]", table_id);
        ret = tablet_cache_->get(table_id, row_key, location);
        if ((ret != OB_SUCCESS) || (timestamp - location.get_timestamp()
              > tablet_cache_->get_timeout()))
        {
          ret = scan_root_table(table_id, row_key, location);
          if (ret != OB_SUCCESS)
          {
            TBSYS_LOG(ERROR, "get tablet location from root failed:table_id[%lu], length[%d], ret[%d]",
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
          else
          {
            miss_cache_hit = true;
          }
        }
        else
        {
          TBSYS_LOG(DEBUG, "already update the cache item by other thread:table_id[%lu]", table_id);
        }
      }
    }

    // cache monitor stat 
    if (NULL != monitor_)
    {
      if (miss_cache_hit)
      {
        monitor_->inc(table_id, ObMergerServiceMonitor::MISS_CS_CACHE_COUNT);
      }
      else
      {
        monitor_->inc(table_id, ObMergerServiceMonitor::HIT_CS_CACHE_COUNT);
      }
    }
  }
  return ret;
}

// waring:all return cell in a row must be same as root table's columns,
//        and the second row is this row allocated chunkserver list
int ObMergerRpcProxy::scan_root_table(const uint64_t table_id, const ObString & row_key,
    ObMergerTabletLocationList & location)
{
  int ret = OB_SUCCESS;
  ObScanner scanner;
  // root table id = 0
  ret = rpc_stub_->fetch_tablet_location(rpc_timeout_, root_server_, 0,
      table_id, row_key, scanner);
  if (ret != OB_SUCCESS)
  {
    TBSYS_LOG(WARN, "fetch tablet location failed:table_id[%lu], length[%d], ret[%d]",
        table_id, row_key.length(), ret);
  }
  else
  {
    // waring: must del at first after return success because of the new item maybe not
    // set the invalid item. for example after the tablet combine happened,
    // the old tablet split point is still exist after renew
    // so there is no chance for us to delete the old split tablet except the code below
    if (OB_SUCCESS != tablet_cache_->del(table_id, row_key))
    {
      TBSYS_LOG(DEBUG, "del the cache item failed:table_id[%lu]", table_id);
    }
  }

  if (OB_SUCCESS == ret)
  {
    ObRange range;
    range.border_flag_.unset_inclusive_start();
    range.border_flag_.set_inclusive_end();
    ObString start_key;
    ObString end_key; 

    ObServer server;
    ObCellInfo * cell = NULL;
    bool row_change = false;
    ObScannerIterator iter = scanner.begin();
    TBSYS_LOG(DEBUG, "%s", "parse scanner result for get some tablet locations");
    // all return cell in a row must be same as root table's columns
    ++iter;
    while ((iter != scanner.end()) 
        && (OB_SUCCESS == (ret = iter.get_cell(&cell, &row_change))) && !row_change)
    {
      if (NULL == cell)
      {
        ret = OB_INNER_STAT_ERROR;
        break;
      }
      start_key.assign(cell->row_key_.ptr(), cell->row_key_.length());
      ++iter;
    }
    
    if (ret == OB_SUCCESS)
    {
      int64_t ip = 0;
      int64_t port = 0;
      bool second_row = true;
      // next cell
      ObMergerTabletLocationList list;
      for (++iter; iter != scanner.end(); ++iter)
      {
        ret = iter.get_cell(&cell, &row_change);
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "get cell from scanner iterator failed:ret[%d]", ret);
          break;
        }
        else if (row_change) // && (iter != last_iter))
        {
          range.table_id_ = table_id;
          if (NULL == start_key.ptr())
          {
            range.border_flag_.set_min_value();
          }
          else
          {
            range.border_flag_.unset_min_value();
            range.start_key_ = start_key;
          }
          range.border_flag_.unset_max_value();
          range.end_key_ = end_key;
          start_key = end_key;
          end_key.assign(cell->row_key_.ptr(), cell->row_key_.length());
          list.set_timestamp(tbsys::CTimeUtil::getTime()); 
          list.sort(merge_server_);
          // the second row is this row allocated chunkserver list
          if (second_row)
          {
            if ((row_key <= range.end_key_) && (row_key > range.start_key_))
            {
              location = list;
              second_row = false;
            }
            else
            {
              ret = OB_DATA_NOT_SERVE;
              TBSYS_LOG(ERROR, "check range not include this key:ret[%d]", ret);
              hex_dump(row_key.ptr(), row_key.length());
              range.hex_dump();
              //break;
            }
          }
          // add to cache
          if (OB_SUCCESS != tablet_cache_->set(range, list))
          {
            TBSYS_LOG(ERROR, "%s", "add the range to cache failed");
          }
          list.clear();
        }
        else
        {
          end_key.assign(cell->row_key_.ptr(), cell->row_key_.length());
          if ((cell->column_name_.compare("1_port") == 0) 
              || (cell->column_name_.compare("2_port") == 0) 
              || (cell->column_name_.compare("3_port") == 0))
          {
            ret = cell->value_.get_int(port);
          }
          else if ((cell->column_name_.compare("1_ipv4") == 0)
              || (cell->column_name_.compare("2_ipv4") == 0)
              || (cell->column_name_.compare("3_ipv4") == 0))
          {
            ret = cell->value_.get_int(ip);
            if (OB_SUCCESS == ret)
            {
              if (port == 0)
              {
                TBSYS_LOG(WARN, "%s", "check port failed");
              }
              server.set_ipv4_addr(static_cast<int32_t>(ip), static_cast<int32_t>(port));
              ObTabletLocation addr(0, server);
              if (OB_SUCCESS != (ret = list.add(addr)))
              {
                TBSYS_LOG(ERROR, "add addr failed:server[%d], port[%d], ret[%d]", 
                    server.get_ipv4(), server.get_port(), ret);
                break;
              }
              else
              {
                TBSYS_LOG(DEBUG, "add addr succ:server[%d], port[%d]", 
                    server.get_ipv4(), server.get_port());
              }
              ip = port = 0;
            }
          }

          if (ret != OB_SUCCESS)
          {
            TBSYS_LOG(ERROR, "check get value failed:ret[%d]", ret);
            break;
          }
        }
      }

      // for the last row 
      if ((OB_SUCCESS == ret) && (start_key != end_key))
      {
        range.table_id_ = table_id;
        if (NULL == start_key.ptr())
        {
          range.border_flag_.set_min_value();
        }
        else
        {
          range.border_flag_.unset_min_value();
          range.start_key_ = start_key;
        }
        range.border_flag_.unset_max_value();
        range.end_key_ = end_key;
        list.set_timestamp(tbsys::CTimeUtil::getTime()); 
        list.sort(merge_server_);
        // double check add all range->locationlist to cache
        if ((row_key <= range.end_key_) && (row_key > range.start_key_))
        {
          location = list;
        }
        else if (second_row)
        {
          range.hex_dump();
          hex_dump(row_key.ptr(), row_key.length());
          ret = OB_DATA_NOT_SERVE;
          TBSYS_LOG(ERROR, "check range not include this key:ret[%d]", ret);
        }
        // add to list to cache
        if (OB_SUCCESS != tablet_cache_->set(range, list))
        {
          range.hex_dump();
          hex_dump(row_key.ptr(), row_key.length());
          TBSYS_LOG(ERROR, "%s", "add the range to cache failed");
        }
      }
    }
    else
    {
      TBSYS_LOG(ERROR, "check get first row cell failed:ret[%d]", ret);
    }
  }
  return ret;
}

int ObMergerRpcProxy::register_merger(const ObServer & merge_server)
{
  int ret = OB_SUCCESS;
  if (!check_inner_stat())
  {
    TBSYS_LOG(ERROR, "%s", "check inner stat failed");
    ret = OB_INNER_STAT_ERROR;
  }
  else
  {
    for (int64_t i = 0; i <= rpc_retry_times_; ++i)
    {
      ret = rpc_stub_->register_server(rpc_timeout_, root_server_, merge_server, true);
      if (ret != OB_SUCCESS)
      {
        TBSYS_LOG(WARN, "register merge server failed:ret[%d]", ret);
        usleep(RETRY_INTERVAL_TIME);
      }
      else
      {
        TBSYS_LOG(INFO, "%s", "register merge server succ");
        break;
      }
    }
  }
  return ret;
}

int ObMergerRpcProxy::async_heartbeat(const ObServer & merge_server)
{
  int ret = OB_SUCCESS;
  if (!check_inner_stat())
  {
    TBSYS_LOG(ERROR, "%s", "check inner stat failed");
    ret = OB_INNER_STAT_ERROR;
  }
  else
  {
    /// async send heartbeat no need retry
    ret = rpc_stub_->heartbeat_server(rpc_timeout_, root_server_, merge_server, OB_MERGESERVER);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "heartbeat with root server failed:ret[%d]", ret);
    }
  }
  return ret;
}

bool ObMergerRpcProxy::check_range_param(const ObRange & range_param)
{
  bool bret = true;
  if (((!range_param.border_flag_.is_min_value()) && (0 == range_param.start_key_.length()))
      || (!range_param.border_flag_.is_max_value() && (0 == range_param.end_key_.length())))
  {
    TBSYS_LOG(ERROR, "%s", "check range param failed");
    bret = false;
  }
  return bret;
}


bool ObMergerRpcProxy::check_scan_param(const ObScanParam & scan_param)
{
  bool bret = true;
  const ObRange * range = scan_param.get_range();
  // the min/max value length is zero
  if (NULL == range)// || (0 == range->start_key_.length()))
  {
    TBSYS_LOG(ERROR, "%s", "check scan range failed");
    bret = false;
  }
  else
  {
    bret = check_range_param(*range);
  }
  return bret;
}

int ObMergerRpcProxy::ups_mutate(const ObMutator & mutate_param, const bool has_data, ObScanner & scanner)
{
  int ret = OB_SUCCESS;
  if (!check_inner_stat())
  {
    TBSYS_LOG(ERROR, "%s", "check inner stat failed");
    ret = OB_INNER_STAT_ERROR;
  }
  else
  {
    ObServer update_server;
    for (int64_t i = 0; i < rpc_retry_times_; ++i)
    {
      // may be need update server list
      ret = get_master_ups((OB_NOT_MASTER == ret), update_server);
      if (ret != OB_SUCCESS)
      {
        TBSYS_LOG(WARN, "get master update server failed:ret[%d]", ret);
        continue;
      }
      ret = rpc_stub_->mutate(rpc_timeout_, update_server, mutate_param, has_data, scanner);
      if (OB_NOT_MASTER == ret)
      {
        TBSYS_LOG(WARN, "mutate update server check role failed:ret[%d]", ret);
        continue;
      }
      else if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "update server mutate failed:ret[%d]", ret);
        break;
      }
      else
      {
        break;
      }
    }
  }

  if (OB_SUCCESS == ret && TBSYS_LOGGER._level >= TBSYS_LOG_LEVEL_DEBUG)
  {
    TBSYS_LOG(DEBUG, "%s", "ups_mutate");
    output(scanner);
  }
  return ret;
}

// must be in one chunk server
int ObMergerRpcProxy::cs_get(const ObGetParam & get_param,
    ObMergerTabletLocation & addr, ObScanner & scanner,  ObIterator * &it_out)
{
  int ret = OB_SUCCESS;
  ObMergerTabletLocationList list;
  const ObCellInfo * cell = get_param[0];
  if (!check_inner_stat())
  {
    TBSYS_LOG(ERROR, "%s", "check inner stat failed");
    ret = OB_INNER_STAT_ERROR;
  }
  else if ((0 == get_param.get_cell_size()) || (NULL == cell))
  {
    TBSYS_LOG(ERROR, "check get param failed:size[%ld], cell[%p]", 
        get_param.get_cell_size(), cell);
    ret = OB_INPUT_PARAM_ERROR;
  }
  else
  {
    // the first cell as tablet location key:table_id + rowkey
    ret = get_tablet_location(cell->table_id_, cell->row_key_, list);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "get first cell tablet location failed:table_id[%lu], ret[%d]",
          cell->table_id_, ret);
      hex_dump(cell->row_key_.ptr(), cell->row_key_.length());
    }
  }
  
  if (OB_SUCCESS == ret)
  {
    bool update_list = false;
    for (int64_t i = 0; i <= rpc_retry_times_; ++i)
    {
      update_list = false;
      ret = rpc_stub_->get(rpc_timeout_, list, get_param, addr, scanner, update_list);
      if (ret != OB_SUCCESS)
      {
        int64_t timestamp = tbsys::CTimeUtil::getTime();
        if (timestamp - list.get_timestamp() > ObMergerTabletLocationCache::CACHE_ERROR_TIMEOUT)
        {
          TBSYS_LOG(DEBUG, "check error cache item timeout:table_id[%lu], timestamp[%ld]",
              cell->table_id_, list.get_timestamp());
          /// must remove the old item before set new items
          int err = scan_root_table(cell->table_id_, cell->row_key_, list);
          if (OB_SUCCESS != err)
          {
            TBSYS_LOG(ERROR, "get tablet location failed:table_id[%lu], length[%d], ret[%d]",
                cell->table_id_, cell->row_key_.length(), err);
            hex_dump(cell->row_key_.ptr(), cell->row_key_.length());
            // renew the item for succ access
            list.set_item_valid(timestamp);
            update_list = true;
          }
        }
      }
      
      // update list err time not using lock
      if (update_list && (list.size() != 0))
      {
        int err = update_cache_item(cell->table_id_, cell->row_key_, list);
        if (err != OB_SUCCESS)
        {
          TBSYS_LOG(DEBUG, "update cache item failed:table_id[%lu], ret[%d]", cell->table_id_, err);
        }
      }

      if (OB_SUCCESS == ret)
      {
        it_out = &scanner;
        if (NULL != monitor_)
        {
          if (addr.server_.chunkserver_.get_ipv4() == merge_server_.get_ipv4())
          {
            monitor_->inc(cell->table_id_, ObMergerServiceMonitor::LOCAL_CS_QUERY_COUNT);
          }
          else
          {
            TBSYS_LOG(DEBUG, "%s", "check serving chunkserver is not localhost");
            monitor_->inc(cell->table_id_, ObMergerServiceMonitor::REMOTE_CS_QUERY_COUNT);
          }
        }
        break;
      }
      else
      {
        usleep(RETRY_INTERVAL_TIME);
        continue;
      }
    }
  }

  if (OB_SUCCESS == ret && TBSYS_LOGGER._level >= TBSYS_LOG_LEVEL_DEBUG)
  {
    TBSYS_LOG(DEBUG, "%s", "cs_get");
    output(scanner);
  }
  return ret;
}

// only one communication with chunk server
int ObMergerRpcProxy::cs_scan(const ObScanParam & scan_param,
    ObMergerTabletLocation & addr, ObScanner & scanner, ObIterator * &it_out)
{
  int ret = OB_SUCCESS;
  char * temp_buffer= NULL;
  ObString search_key;
  ObMergerTabletLocationList list;
  const ObRange * range = scan_param.get_range();
  if (!check_inner_stat())
  {
    TBSYS_LOG(ERROR, "%s", "check inner stat failed");
    ret = OB_INNER_STAT_ERROR;
  }
  else if (!check_scan_param(scan_param))
  {
    TBSYS_LOG(ERROR, "%s", "check scan param failed");
    ret = OB_INPUT_PARAM_ERROR;
  }
  else if (NULL == range)
  {
    TBSYS_LOG(ERROR, "check scan param range failed:range[%p]", range);
    ret = OB_INPUT_PARAM_ERROR;
  }
  else
  {
    ret = get_first_tablet_location(scan_param, search_key, &temp_buffer, list);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "get first tablet location failed:table_id[%lu], ret[%d]",
          range->table_id_, ret);
      range->hex_dump();
    }
  }
  
  if (OB_SUCCESS == ret)
  {
    bool update_list = false;
    for (int64_t i = 0; i <= rpc_retry_times_; ++i)
    {
      update_list = false;
      ret = rpc_stub_->scan(rpc_timeout_, list, scan_param, addr, scanner, update_list);
      if (ret != OB_SUCCESS)
      {
        int64_t timestamp = tbsys::CTimeUtil::getTime();
        if (timestamp - list.get_timestamp() > ObMergerTabletLocationCache::CACHE_ERROR_TIMEOUT)
        {
          TBSYS_LOG(DEBUG, "check error cache item timeout:table_id[%lu], timestamp[%ld]",
              range->table_id_, list.get_timestamp());
          int err = scan_root_table(range->table_id_, search_key, list);
          if (OB_SUCCESS != err)
          {
            TBSYS_LOG(ERROR, "get tablet location failed:table_id[%lu], length[%d], ret[%d]",
                range->table_id_, search_key.length(), err);
            // set valid again for root server down
            list.set_item_valid(timestamp);
            update_list = true;
          }
        }
      }
      // update list err time not using lock
      if (update_list && (list.size() != 0))
      {
        int err = update_cache_item(range->table_id_, search_key, list);
        if (err != OB_SUCCESS)
        {
          TBSYS_LOG(DEBUG, "update cache item failed:ret[%d]", err);
        }
      }

      if (OB_SUCCESS == ret)
      {
        it_out = &scanner;
        if (NULL != monitor_)
        {
          if (addr.server_.chunkserver_.get_ipv4() == merge_server_.get_ipv4())
          {
            monitor_->inc(range->table_id_, ObMergerServiceMonitor::LOCAL_CS_QUERY_COUNT);
          }
          else
          {
            TBSYS_LOG(DEBUG, "%s", "check serving chunkserver is not localhost");
            monitor_->inc(range->table_id_, ObMergerServiceMonitor::REMOTE_CS_QUERY_COUNT);
          }
        }
        break;
      }
      else
      {
        usleep(RETRY_INTERVAL_TIME);
        continue;
      }
    }
  }
  
  /// reset temp buffer for new search key
  reset_search_key(search_key, temp_buffer);
  
  if (OB_SUCCESS == ret && TBSYS_LOGGER._level >= TBSYS_LOG_LEVEL_DEBUG)
  {
    scan_param.get_range()->to_string(max_range_, sizeof(max_range_));
    TBSYS_LOG(DEBUG, "cs_scan:table_id[%lu], range[%s], direction[%s]", scan_param.get_table_id(),
        max_range_, (scan_param.get_scan_direction() == ObScanParam::FORWARD) ? "forward": "backward");
    output(scanner);
  }
  return ret;
}


int ObMergerRpcProxy::get_search_key(const ObScanParam & scan_param, ObString & search_key, char ** new_buffer)
{
  int ret = OB_SUCCESS;
  const ObRange * range = scan_param.get_range();
  if (NULL == range)
  {
    TBSYS_LOG(ERROR, "check scan param range failed:range[%p]", range);
    ret = OB_INPUT_PARAM_ERROR;
  }
  else if (NULL == new_buffer)
  {
    TBSYS_LOG(ERROR, "check temp buff failed:temp[%p]", new_buffer);
    ret = OB_INPUT_PARAM_ERROR;
  }
  else
  {
    *new_buffer = NULL;
    if (ObScanParam::FORWARD == scan_param.get_scan_direction())
    {
      search_key = range->start_key_;
      // not include the start row, convert the row_key
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
        // TODO set eight FF as max row key
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

// delete the cache item
int ObMergerRpcProxy::del_cache_item(const ObScanParam & scan_param)
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
    // reset search key buffer
    reset_search_key(search_key, temp);
  }
  return ret;
}


int ObMergerRpcProxy::del_cache_item(const uint64_t table_id, const ObString & search_key)
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


int ObMergerRpcProxy::update_cache_item(const uint64_t table_id, const ObString & rowkey, 
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

int ObMergerRpcProxy::update_cache_item(const ObScanParam & scan_param, 
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
      ret = tablet_cache_->update(range->table_id_, search_key, location);
      if (ret != OB_SUCCESS)
      {
        TBSYS_LOG(DEBUG, "update cache item failed:table_id[%lu], ret[%d]", range->table_id_, ret);
      }
    }
    // reset search key buffer
    reset_search_key(search_key, temp);
  }
  return ret;
}

//
int ObMergerRpcProxy::set_item_invalid(const uint64_t table_id, const ObString & rowkey,
    const ObMergerTabletLocation & addr)
{
  ObMergerTabletLocationList list;
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
int ObMergerRpcProxy::set_item_invalid(const ObScanParam & scan_param,
    const ObMergerTabletLocation & addr)
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
      ret = set_item_invalid(range->table_id_, search_key, addr);
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


// can not use search key after reset buffer
void ObMergerRpcProxy::reset_search_key(ObString & search_key, char * buffer)
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

void ObMergerRpcProxy::set_master_ups(const ObServer & server)
{
  master_update_server_ = server;
}

int ObMergerRpcProxy::get_master_ups(const bool renew, ObServer & server)
{
  int ret = OB_SUCCESS;
  if (true == renew)
  {
    int64_t timestamp = tbsys::CTimeUtil::getTime();
    if (timestamp - fetch_ups_timestamp_ > min_fetch_interval_)
    {
      int32_t server_count = 0;
      tbsys::CThreadGuard lock(&update_lock_);
      if (timestamp - fetch_ups_timestamp_ > min_fetch_interval_)
      {
        TBSYS_LOG(DEBUG, "need renew the update server list");
        fetch_ups_timestamp_ = tbsys::CTimeUtil::getTime();
        // renew the udpate server list
        ret = fetch_update_server_list(server_count);
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(WARN, "fetch update server list failed:ret[%d]", ret);
        }
        else if (server_count == 0)
        {
          TBSYS_LOG(DEBUG, "new server list empty retry fetch vip server");
          // using old protocol get update server vip
          ret = rpc_stub_->find_server(rpc_timeout_, root_server_, server);
          if (ret != OB_SUCCESS)
          {
            TBSYS_LOG(WARN, "find update server vip failed:ret[%d]", ret);
          }
          else
          {
            tbsys::CWLockGuard lock(ups_list_lock_);
            master_update_server_ = server;
          }
        }
      }
      else
      {
        TBSYS_LOG(DEBUG, "fetch update server list by other thread");
      }
    }
  }
  // renew master update server addr
  tbsys::CRLockGuard lock(ups_list_lock_);
  server = master_update_server_;
  return ret;
}

void ObMergerRpcProxy::modify_ups_list(ObUpsList & list)
{
  if (0 == list.ups_count_)
  {
    // add vip update server to list
    TBSYS_LOG(DEBUG, "check ups count is zero:count[%d]", list.ups_count_);
    ObUpsInfo info;
    info.addr_ = master_update_server_;
    // set inner port to update server port
    info.inner_port_ = master_update_server_.get_port();
    info.ms_read_percentage_ = 100;
    info.cs_read_percentage_ = 100;
    list.ups_count_ = 1;
    list.ups_array_[0] = info;
    list.sum_ms_percentage_ = 100;
    list.sum_cs_percentage_ = 100;
  }
  else if (list.get_sum_percentage(server_type_) <= 0)
  {
    TBSYS_LOG(DEBUG, "reset the percentage for all servers");
    for (int32_t i = 0; i < list.ups_count_; ++i)
    {
      // reset all ms and cs to equal
      list.ups_array_[i].ms_read_percentage_ = 1;
      list.ups_array_[i].cs_read_percentage_ = 1;
    }
    // reset all ms and cs sum percentage to count
    list.sum_ms_percentage_ = list.ups_count_;
    list.sum_cs_percentage_ = list.ups_count_;
  }
}

int ObMergerRpcProxy::master_ups_get(const ObMergerTabletLocation & addr,
    const ObGetParam & get_param, ObScanner & scanner)
{
  int ret = OB_ERROR;
  ObServer update_server;
  for (int64_t i = 0; i <= rpc_retry_times_; ++i)
  {
    ret = get_master_ups((OB_NOT_MASTER == ret), update_server);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "get master update server failed:ret[%d]", ret);
      break;
    }
    ret = rpc_stub_->get(rpc_timeout_, update_server, get_param, scanner);
    if (OB_INVALID_START_VERSION == ret)
    {
      TBSYS_LOG(WARN, "check chunk server data version failed:ret[%d]", ret);
      if (NULL != monitor_)
      {
        monitor_->inc(get_param[0]->table_id_, ObMergerServiceMonitor::FAIL_CS_VERSION_COUNT);
      }
      // update the cache item to err status // addr
      int err = set_item_invalid(get_param[0]->table_id_, get_param[0]->row_key_, addr);
      if (err != OB_SUCCESS)
      {
        TBSYS_LOG(DEBUG, "set cache item invalid failed:ret[%d]", err);
      }
      break;
    }
    else if (OB_NOT_MASTER == ret)
    {
      TBSYS_LOG(WARN, "get from update server check role failed:ret[%d]", ret);
    }
    else if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "get from update server failed:ret[%d]", ret);
    }
    else
    {
      break;
    }
    usleep(static_cast<useconds_t>(RETRY_INTERVAL_TIME * (i + 1)));
  }
  return ret;
}

int ObMergerRpcProxy::slave_ups_get(const ObMergerTabletLocation & addr,
    const ObGetParam & get_param, ObScanner & scanner)
{
  int ret = OB_ERROR;
  int32_t retry_times = 0;
  tbsys::CRLockGuard lock(ups_list_lock_);
  int32_t server_count = update_server_list_.ups_count_;
  int32_t cur_index = ObMergerReadBalance::select_server(update_server_list_, get_param, server_type_);
  if (cur_index < 0)
  {
    TBSYS_LOG(WARN, "select server failed:count[%d], index[%d]", server_count, cur_index);
    ret = OB_ERROR;
  }
  else
  {
    // bring back to alive no need write lock
    if (black_list_.get_valid_count() <= 0)
    {
      TBSYS_LOG(WARN, "check all the update server not invalid:count[%d]", black_list_.get_valid_count());
      black_list_.reset();
    }

    ObServer update_server;
    for (int32_t i = cur_index; retry_times < server_count; ++i, ++retry_times)
    {
      if (false == check_server(i%server_count))
      {
        TBSYS_LOG(DEBUG, "check server failed:index[%d]", i%server_count);
        continue;
      }
      update_server = update_server_list_.ups_array_[i%server_count].get_server(server_type_);
      TBSYS_LOG(DEBUG, "select slave update server for get:index[%d], ip[%d], port[%d]",
          i, update_server.get_ipv4(), update_server.get_port());
      ret = rpc_stub_->get(rpc_timeout_, update_server, get_param, scanner);
      if (OB_INVALID_START_VERSION == ret)
      {
        TBSYS_LOG(WARN, "check chunk server data version failed:ret[%d]", ret);
        if (NULL != monitor_)
        {
          monitor_->inc(get_param[0]->table_id_, ObMergerServiceMonitor::FAIL_CS_VERSION_COUNT);
        }
        // update the cache item to err status // addr
        int err = set_item_invalid(get_param[0]->table_id_, get_param[0]->row_key_, addr);
        if (err != OB_SUCCESS)
        {
          TBSYS_LOG(DEBUG, "set cache item invalid failed:ret[%d]", err);
        }
      }
      else if (ret != OB_SUCCESS)
      {
        // inc update server fail counter for blacklist
        black_list_.fail(i%server_count, update_server);
        TBSYS_LOG(WARN, "get from update server failed:ret[%d]", ret);
      }
      else
      {
        break;
      }
    }
  }
  return ret;
}

//
int ObMergerRpcProxy::ups_get(const ObMergerTabletLocation & addr,
    const ObGetParam & get_param, ObScanner & scanner)
{
  int ret = OB_SUCCESS;
  if (!check_inner_stat())
  {
    TBSYS_LOG(ERROR, "%s", "check inner stat failed");
    ret = OB_INNER_STAT_ERROR;
  }
  else if (NULL == get_param[0])
  {
    TBSYS_LOG(ERROR, "check first cell failed:cell[%p]", get_param[0]);
    ret = OB_INPUT_PARAM_ERROR;
  }
  else if (true == get_param.get_is_read_consistency())
  {
    ret = master_ups_get(addr, get_param, scanner);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "get from master ups failed:ret[%d]", ret);
    }
  }
  else
  {
    ret = slave_ups_get(addr, get_param, scanner);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "get from slave ups failed:ret[%d]", ret);
    }
  }

  if ((OB_SUCCESS == ret) && (get_param.get_cell_size() > 0) && scanner.is_empty())
  {
    TBSYS_LOG(WARN, "%s", "update server error, response request with zero cell");
    ret = OB_ERR_UNEXPECTED;
  }

  if (OB_SUCCESS == ret && TBSYS_LOGGER._level >= TBSYS_LOG_LEVEL_DEBUG)
  {
    TBSYS_LOG(DEBUG, "%s", "ups_get");
    output(scanner);
  }
  return ret;
}

int ObMergerRpcProxy::master_ups_scan(const ObMergerTabletLocation & addr,
    const ObScanParam & scan_param, ObScanner & scanner)
{
  int ret = OB_ERROR;
  ObServer update_server;
  for (int64_t i = 0; i <= rpc_retry_times_; ++i)
  {
    ret = get_master_ups((OB_NOT_MASTER == ret), update_server);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "get master update server failed:ret[%d]", ret);
      break;
    }
    ret = rpc_stub_->scan(rpc_timeout_, update_server, scan_param, scanner);
    if (OB_INVALID_START_VERSION == ret)
    {
      TBSYS_LOG(WARN, "check chunk server data version failed:ret[%d]", ret);
      if (NULL != monitor_)
      {
        monitor_->inc(scan_param.get_table_id(), ObMergerServiceMonitor::FAIL_CS_VERSION_COUNT);
      }
      // update the cache item to err status // addr
      int err = set_item_invalid(scan_param, addr);
      if (err != OB_SUCCESS)
      {
        TBSYS_LOG(WARN, "set cache item valid failed:ret[%d]", err);
      }
      break;
    }
    else if (OB_NOT_MASTER == ret)
    {
      TBSYS_LOG(WARN, "get from update server check role failed:ret[%d]", ret);
    }
    else if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "get from update server failed:ret[%d]", ret);
    }
    else
    {
      break;
    }
    usleep(static_cast<useconds_t>(RETRY_INTERVAL_TIME * (i + 1)));
  }
  return ret;
}

int ObMergerRpcProxy::set_rpc_param(const int64_t retry_times, const int64_t timeout)
{
  int ret = OB_SUCCESS;
  if ((retry_times < 0) || (timeout <= 0))
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "check rpc timeout param failed:retry_times[%ld], timeout[%ld]",
      retry_times, timeout);
  }
  else
  {
    rpc_retry_times_ = retry_times;
    rpc_timeout_ = timeout;
  }
  return ret;
}

int ObMergerRpcProxy::set_blacklist_param(const int64_t timeout, const int64_t fail_count)
{
  int ret = OB_SUCCESS;
  if ((timeout <= 0) || (fail_count <= 0))
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "check blacklist param failed:timeout[%ld], threshold[%ld]", timeout, fail_count);
  }
  else
  {
    fail_count_threshold_ = fail_count;
    black_list_timeout_ = timeout;
  }
  return ret;
}

bool ObMergerRpcProxy::check_server(const int32_t index)
{
  bool ret = true;
  // check the read percentage and not in black list, if in list timeout make it to be alive
  if ((false == black_list_.check(index))
    || (update_server_list_.ups_array_[index].get_read_percentage(server_type_) <= 0))
  {
    ret = false;
  }
  return ret;
}

int ObMergerRpcProxy::slave_ups_scan(const ObMergerTabletLocation & addr,
    const ObScanParam & scan_param, ObScanner & scanner)
{
  int ret = OB_ERROR;
  int32_t retry_times = 0;
  tbsys::CRLockGuard lock(ups_list_lock_);
  int32_t server_count = update_server_list_.ups_count_;
  int32_t cur_index = ObMergerReadBalance::select_server(update_server_list_, scan_param, server_type_);
  if (cur_index < 0)
  {
    TBSYS_LOG(WARN, "select server failed:count[%d], index[%d]", server_count, cur_index);
    ret = OB_ERROR;
  }
  else
  {
    // bring back to alive no need write lock
    if (black_list_.get_valid_count() <= 0)
    {
      TBSYS_LOG(WARN, "check all the update server not invalid:count[%d]", black_list_.get_valid_count());
      black_list_.reset();
    }
    ObServer update_server;
    for (int32_t i = cur_index; retry_times < server_count; ++i, ++retry_times)
    {
      if (false == check_server(i%server_count))
      {
        TBSYS_LOG(DEBUG, "check server failed:index[%d]", i%server_count);
        continue;
      }
      update_server = update_server_list_.ups_array_[i%server_count].get_server(server_type_);
      TBSYS_LOG(DEBUG, "select slave update server for scan:index[%d], ip[%d], port[%d]",
          i, update_server.get_ipv4(), update_server.get_port());
      ret = rpc_stub_->scan(rpc_timeout_, update_server, scan_param, scanner);
      if (OB_INVALID_START_VERSION == ret)
      {
        TBSYS_LOG(WARN, "check chunk server data version failed:ret[%d]", ret);
        if (NULL != monitor_)
        {
          monitor_->inc(scan_param.get_table_id(), ObMergerServiceMonitor::FAIL_CS_VERSION_COUNT);
        }
        // update the cache item to err status // addr
        int err = set_item_invalid(scan_param, addr);
        if (err != OB_SUCCESS)
        {
          TBSYS_LOG(DEBUG, "set cache item invalid failed:ret[%d]", err);
        }
      }
      else if (ret != OB_SUCCESS)
      {
        // inc update server fail counter for blacklist
        black_list_.fail(i%server_count, update_server);
        TBSYS_LOG(WARN, "get from update server failed:ret[%d]", ret);
      }
      else
      {
        break;
      }
    }
  }
  return ret;
}

int ObMergerRpcProxy::ups_scan(const ObMergerTabletLocation & addr,
    const ObScanParam & scan_param, ObScanner & scanner)
{
  int ret = OB_SUCCESS;
  if (!check_inner_stat())
  {
    TBSYS_LOG(ERROR, "%s", "check inner stat failed");
    ret = OB_INNER_STAT_ERROR;
  }
  else if (!check_scan_param(scan_param))
  {
    TBSYS_LOG(ERROR, "%s", "check scan param failed");
    ret = OB_INPUT_PARAM_ERROR;
  }
  else if (true == scan_param.get_is_read_consistency())
  {
    ret = master_ups_scan(addr, scan_param, scanner);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "scan from master ups failed:ret[%d]", ret);
    }
  }
  else
  {
    ret = slave_ups_scan(addr, scan_param, scanner);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "scan from slave ups failed:ret[%d]", ret);
    }
  }

  if (OB_SUCCESS == ret && TBSYS_LOGGER._level >= TBSYS_LOG_LEVEL_DEBUG)
  {
    const ObRange * range = scan_param.get_range();
    if (NULL == range)
    {
      TBSYS_LOG(ERROR, "check scan param range failed:table_id[%lu], range[%p]",
        scan_param.get_table_id(), range);
    }
    else
    {
      range->to_string(max_range_, sizeof(max_range_));
      TBSYS_LOG(DEBUG, "ups_scan:table_id[%lu], range[%s], direction[%s]",
          scan_param.get_table_id(), max_range_,
          (scan_param.get_scan_direction() == ObScanParam::FORWARD) ? "forward": "backward");
      output(scanner);
    }
  }
  return ret;
}

void ObMergerRpcProxy::output(common::ObScanner & result)
{
  int ret = OB_SUCCESS;
  ObCellInfo *cur_cell = NULL;
  while (result.next_cell() == OB_SUCCESS)
  {
    ret = result.get_cell(&cur_cell);
    if (OB_SUCCESS == ret)
    {
      TBSYS_LOG(DEBUG, "tableid:%lu,rowkey:%.*s,column_id:%lu,ext:%ld,type:%d", 
          cur_cell->table_id_,
          cur_cell->row_key_.length(), cur_cell->row_key_.ptr(), cur_cell->column_id_,
          cur_cell->value_.get_ext(),cur_cell->value_.get_type());
      cur_cell->value_.dump();
      hex_dump(cur_cell->row_key_.ptr(), cur_cell->row_key_.length());
    }
    else
    {
      TBSYS_LOG(WARN, "get cell failed:ret[%d]", ret);
      break;
    }
  }
  result.reset_iter();
}


