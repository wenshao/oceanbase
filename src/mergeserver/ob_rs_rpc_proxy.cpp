#include "common/ob_schema.h"
#include "common/ob_scanner.h"
#include "ob_ms_tablet_location.h"
#include "ob_ms_schema_manager.h"
#include "ob_ms_rpc_stub.h"
#include "ob_rs_rpc_proxy.h"
#include "ob_ms_counter_infos.h"

using namespace oceanbase::common;
using namespace oceanbase::mergeserver;


ObMergerRootRpcProxy::ObMergerRootRpcProxy(const int64_t retry_times,
    const int64_t timeout, const ObServer & root)
{
  rpc_retry_times_ = retry_times;
  rpc_timeout_ = timeout;
  root_server_ = root;
  rpc_stub_ = NULL;
}


ObMergerRootRpcProxy::~ObMergerRootRpcProxy()
{
}


int ObMergerRootRpcProxy::init(ObMergerRpcStub * rpc_stub)
{
  int ret = OB_SUCCESS;
  if (NULL == rpc_stub)
  {
    ret = OB_INPUT_PARAM_ERROR;
    TBSYS_LOG(ERROR, "check param failed:rpc[%p]", rpc_stub);
  }
  else
  {
    rpc_stub_ = rpc_stub;
  }
  return ret;
}

int ObMergerRootRpcProxy::register_merger(const common::ObServer & merge_server)
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
  ms_get_counter_set().inc(ObMergerCounterIds::C_REGISTER_MS);
  return ret;
}

int ObMergerRootRpcProxy::async_heartbeat(const ObServer & merge_server)
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
  ms_get_counter_set().inc(ObMergerCounterIds::C_HEART_BEAT);
  return ret;
}

int ObMergerRootRpcProxy::fetch_newest_schema(ObMergerSchemaManager * schema_manager,
    const ObSchemaManagerV2 ** manager)
{
  /// fetch new schema
  int ret = OB_SUCCESS;
  ObSchemaManagerV2 * schema = NULL;
  if (!check_inner_stat())
  {
    TBSYS_LOG(ERROR, "%s", "check inner stat failed");
    ret = OB_INNER_STAT_ERROR;
  }
  char * temp = NULL;
  if(OB_SUCCESS == ret)
  {
    temp = (char *)ob_malloc(sizeof(ObSchemaManagerV2), ObModIds::OB_MS_RPC);
    if (NULL == temp)
    {
      TBSYS_LOG(ERROR, "%s", "check ob malloc failed");
      ret = OB_ALLOCATE_MEMORY_FAILED;
    }
    else
    {
      schema = new(temp) ObSchemaManagerV2;
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
          TBSYS_LOG(WARN, "rpc fetch schema failed:ret[%d]", ret);
        }
      }
    }
  }
  
  if (OB_SUCCESS == ret)
  {
    ret = schema_manager->add_schema(*schema, manager);
    // maybe failed because of timer thread fetch and add it already
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "add new schema failed:version[%ld], ret[%d]", schema->get_version(), ret);
      ret = OB_SUCCESS;
      *manager = schema_manager->get_schema(0);
      if (NULL == *manager)
      {
        TBSYS_LOG(WARN, "get latest schema failed:schema[%p], latest[%ld]", 
            *manager, schema_manager->get_latest_version());
        ret = OB_INNER_STAT_ERROR;
      }
    }
    else
    {
      TBSYS_LOG(DEBUG, "fetch and add new schema succ:version[%ld]", schema->get_version());
    }
  }
  
  if (schema != NULL)
  {
    schema->~ObSchemaManagerV2();
  }
  if (temp != NULL)
  {
    ob_free(temp);
    temp = NULL;
  }
  ms_get_counter_set().inc(ObMergerCounterIds::C_FETCH_SCHEMA);
  return ret;
}

int ObMergerRootRpcProxy::fetch_schema_version(int64_t & timestamp)
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
  ms_get_counter_set().inc(ObMergerCounterIds::C_FETCH_SCHEMA_VERSION);
  return ret;
}

// waring:all return cell in a row must be same as root table's columns,
//        and the second row is this row allocated chunkserver list
int ObMergerRootRpcProxy::scan_root_table(ObMergerTabletLocationCache * cache, 
    const uint64_t table_id, const ObString & row_key, const ObServer & addr,
    ObMergerTabletLocationList & location)
{
  assert(location.get_buffer() != NULL);
  int ret = OB_SUCCESS;
  bool find_right_tablet = false;
  ObScanner scanner;
  // root table id = 0
  ret = rpc_stub_->fetch_tablet_location(rpc_timeout_, root_server_, 0,
      table_id, row_key, scanner);
  if (ret != OB_SUCCESS)
  {
    TBSYS_LOG(WARN, "fetch tablet location failed:table_id[%lu], length[%d], ret[%d]",
        table_id, row_key.length(), ret);
    hex_dump(row_key.ptr(), row_key.length(), true);
  }
  else
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
          list.sort(addr);
          // not deep copy the range
          list.set_tablet_range(range);
          // the second row is this row allocated chunkserver list
          if (second_row)
          {
            second_row = false;
            if ((row_key <= range.end_key_) && (row_key > range.start_key_))
            {
              find_right_tablet = true;
              location = list;
              assert(location.get_buffer() != NULL);
              location.set_tablet_range(range);
            }
            else
            {
              ret = OB_DATA_NOT_SERVE;
              TBSYS_LOG(ERROR, "check range not include this key:ret[%d]", ret);
              hex_dump(row_key.ptr(), row_key.length());
              range.hex_dump();
              break;
            }
          }
          // add to cache
          if (OB_SUCCESS != cache->set(range, list))
          {
            TBSYS_LOG(WARN, "%s", "add the range to cache failed");
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
                TBSYS_LOG(WARN, "check port failed:ip[%ld], port[%ld]", ip, port);
              }
              server.set_ipv4_addr(static_cast<int32_t>(ip), static_cast<int32_t>(port));
              ObTabletLocation addr(0, server);
              if (OB_SUCCESS != (ret = list.add(addr)))
              {
                TBSYS_LOG(ERROR, "add addr failed:ip[%ld], port[%ld], ret[%d]", 
                    ip, port, ret);
                break;
              }
              else
              {
                TBSYS_LOG(DEBUG, "add addr succ:ip[%ld], port[%ld]", ip, port);
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
        // not deep copy the range
        list.set_tablet_range(range);
        list.sort(addr);
        // double check add all range->locationlist to cache
        if ((row_key <= range.end_key_) && (row_key > range.start_key_))
        {
          find_right_tablet = true;
          location = list;
          // deep copy range
          assert(location.get_buffer() != NULL);
          location.set_tablet_range(range);
        }
        else if (second_row)
        {
          range.hex_dump();
          ret = OB_DATA_NOT_SERVE;
          TBSYS_LOG(ERROR, "check range not include this key:ret[%d]", ret);
        }
        // add to list to cache
        if (OB_SUCCESS != cache->set(range, list))
        {
          range.hex_dump();
          TBSYS_LOG(WARN, "%s", "add the range to cache failed");
        }
      }
    }
    else
    {
      TBSYS_LOG(ERROR, "check get first row cell failed:ret[%d]", ret);
    }
  }

  if ((OB_SUCCESS == ret) && (0 == location.size()))
  {
    TBSYS_LOG(ERROR, "check get location size failed:table_id[%ld], find[%d], count[%ld]",
        table_id, find_right_tablet, location.size());
    hex_dump(row_key.ptr(), row_key.length());
    ret = OB_INNER_STAT_ERROR;
  }
  ms_get_counter_set().inc(ObMergerCounterIds::C_SCAN_ROOT_TABLE);
  return ret;
}


