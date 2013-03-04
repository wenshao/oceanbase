/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * ob_rpc_proxy.h for rpc among chunk server, update server and
 * root server.
 *
 * Authors:
 *   xielun <xielun.szd@taobao.com>
 *   huating <huating.zmq@taobao.com>
 *
 */
#include "ob_rpc_proxy.h"
#include "ob_rpc_stub.h"

#include "common/utility.h"
#include "common/ob_schema.h"
#include "common/ob_scanner.h"
#include "common/ob_read_common_data.h"
#include "common/ob_trace_log.h"
#include "common/ob_crc64.h"
#include "ob_schema_manager.h"

namespace oceanbase
{
  namespace chunkserver
  {
    using namespace oceanbase::common;

    ObMergerRpcProxy::ObMergerRpcProxy():ups_list_lock_(tbsys::WRITE_PRIORITY)
    {
      init_ = false;
      rpc_stub_ = NULL;
      rpc_retry_times_ = 0;
      rpc_timeout_ = 0;
      min_fetch_interval_ = 10 * 1000 * 1000L;
      fetch_ups_timestamp_ = 0;
      cur_finger_print_ = 0;
      fail_count_threshold_ = 100;
      black_list_timeout_ = 60 * 1000 * 1000L;
      fetch_schema_timestamp_ = 0;
      schema_manager_ = NULL;
    }

    ObMergerRpcProxy::ObMergerRpcProxy(
        const int64_t retry_times, const int64_t timeout,
        const ObServer & root_server)
    {
      init_ = false;
      rpc_retry_times_ = retry_times;
      rpc_timeout_ = timeout;
      root_server_ = root_server;
      min_fetch_interval_ = 10 * 1000 * 1000L;
      fetch_ups_timestamp_ = 0;
      rpc_stub_ = NULL;
      cur_finger_print_ = 0;
      fail_count_threshold_ = 100;
      black_list_timeout_ = 60 * 1000 * 1000L;
      fetch_schema_timestamp_ = 0;
      schema_manager_ = NULL;
    }

    ObMergerRpcProxy::~ObMergerRpcProxy()
    {
    }

    bool ObMergerRpcProxy::check_inner_stat(void) const
    {
      return(init_ && (NULL != rpc_stub_) && (NULL != schema_manager_));
    }

    int ObMergerRpcProxy::init(
        ObMergerRpcStub * rpc_stub, ObMergerSchemaManager * schema)
    {
      int ret = OB_SUCCESS;
      if ((NULL == rpc_stub) || (NULL == schema))
      {
        TBSYS_LOG(WARN, "check schema or tablet cache failed:"
            "rpc[%p], schema[%p]", rpc_stub, schema);
        ret = OB_INPUT_PARAM_ERROR;
      }
      else if (true == init_)
      {
        TBSYS_LOG(WARN, "check already inited");
        ret = OB_INNER_STAT_ERROR;
      }
      else
      {
        rpc_stub_ = rpc_stub;
        schema_manager_ = schema;
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
            TBSYS_LOG(INFO, "ups list changed succ:cur[%lu], new[%lu], ups_count[%d]",
                cur_finger_print_, finger_print, count);
            list.print();
            update_ups_info(list);
            tbsys::CWLockGuard lock(ups_list_lock_);
            cur_finger_print_ = finger_print;
            memcpy(&update_server_list_, &list, sizeof(update_server_list_));
            // init update server blacklist fail_count threshold, timeout
            ret = black_list_.init(static_cast<int32_t>(fail_count_threshold_), black_list_timeout_,
              MERGE_SERVER, update_server_list_);
            if (ret != OB_SUCCESS)
            {
              // if failed use old blacklist info
              TBSYS_LOG(ERROR, "init black list failed use old blacklist info:ret[%d]", ret);
            }
            else
            {
              ret = ups_black_list_for_merge_.init(static_cast<int32_t>(fail_count_threshold_), black_list_timeout_,
                CHUNK_SERVER, update_server_list_);
              if (ret != OB_SUCCESS)
              {
                // if failed use old blacklist info
                TBSYS_LOG(ERROR, "init black list failed use old blacklist info:ret[%d]", ret);
              }
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

    void ObMergerRpcProxy::update_ups_info(const ObUpsList & list)
    {
      for (int64_t i = 0; i < list.ups_count_; ++i)
      {
        if (UPS_MASTER == list.ups_array_[i].stat_)
        {
          update_server_ = list.ups_array_[i].addr_;
          inconsistency_update_server_.set_ipv4_addr(
            update_server_.get_ipv4(), list.ups_array_[i].inner_port_);
          break;
        }
      }
    }

    int ObMergerRpcProxy::release_schema(const ObSchemaManagerV2 * manager)
    {
      int ret = OB_SUCCESS;
      if (!check_inner_stat() || (NULL == manager))
      {
        TBSYS_LOG(WARN, "check inner stat failed");
        ret = OB_ERROR;
      }
      else
      {
        ret = schema_manager_->release_schema(manager->get_version());
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(WARN, "release scheam failed:schema[%p], timestamp[%ld]",
              manager, manager->get_version());
        }
      }
      return ret;
    }

    int ObMergerRpcProxy::get_schema(const int64_t timestamp,
                                     const ObSchemaManagerV2 ** manager)
    {
      int ret = OB_SUCCESS;
      if (!check_inner_stat() || (NULL == manager))
      {
        TBSYS_LOG(WARN, "check inner stat failed");
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

    int ObMergerRpcProxy::get_new_schema(const int64_t timestamp,
                                         const ObSchemaManagerV2 ** manager)
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

    int ObMergerRpcProxy::fetch_new_schema(const int64_t timestamp,
                                           const ObSchemaManagerV2 ** manager)
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
            TBSYS_LOG(ERROR, "check ob malloc failed");
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
        TBSYS_LOG(WARN, "check inner stat failed");
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

    int ObMergerRpcProxy::get_frozen_time(
        const int64_t frozen_version, int64_t& forzen_time)
    {
      int ret = OB_SUCCESS;
      if (!check_inner_stat())
      {
        TBSYS_LOG(WARN, "check inner stat failed");
        ret = OB_INNER_STAT_ERROR;
      }
      else
      {
        ObServer update_server;
        ret = get_update_server(false, update_server);
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(WARN, "get master update server failed:ret[%d]", ret);
        }
        else
        {
          ret = rpc_stub_->fetch_frozen_time(rpc_timeout_, update_server,
              frozen_version, forzen_time);
          if (ret != OB_SUCCESS)
          {
            TBSYS_LOG(WARN, "fetch frozen time failed:ret[%d]", ret);
          }
          else
          {
            TBSYS_LOG(DEBUG, "fetch frozen time succ:frozen version[%ld],"
                "frozen time[%ld]",
                frozen_version, forzen_time);
          }
        }
      }
      return ret;
    }

    int ObMergerRpcProxy::get_frozen_schema(
      const int64_t frozen_version, ObSchemaManagerV2& schema)
    {
      int ret = OB_SUCCESS;

      if (!check_inner_stat())
      {
        TBSYS_LOG(WARN, "check inner stat failed");
        ret = OB_INNER_STAT_ERROR;
      }
      else
      {
        ObServer update_server;
        ret = get_update_server(false, update_server);
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(WARN, "get master update server failed:ret[%d]", ret);
        }
        else
        {
          ret = rpc_stub_->fetch_schema(rpc_timeout_, update_server,
              frozen_version, schema);
          if (ret != OB_SUCCESS)
          {
            TBSYS_LOG(WARN, "fetch frozen schema failed:ret[%d]", ret);
          }
          else
          {
            TBSYS_LOG(DEBUG, "fetch frozen schema succ:frozen version[%ld]",
                frozen_version);
          }
        }
      }

      return ret;
    }

    int ObMergerRpcProxy::get_update_server(const bool renew, ObServer & server)
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
            TBSYS_LOG(DEBUG, "renew fetch update server list");
            fetch_ups_timestamp_ = tbsys::CTimeUtil::getTime();
            // renew the udpate server list
            ret = fetch_update_server_list(server_count);
            if (ret != OB_SUCCESS)
            {
              TBSYS_LOG(WARN, "fetch update server list failed:ret[%d]", ret);
            }
            else if (server_count == 0)
            {
              TBSYS_LOG(DEBUG, "fetch update server list empty retry fetch vip update server");
              // using old protocol get update server vip
              ret = rpc_stub_->fetch_update_server(rpc_timeout_, root_server_, server);
              if (ret != OB_SUCCESS)
              {
                TBSYS_LOG(WARN, "find update server vip failed:ret[%d]", ret);
              }
              else
              {
                tbsys::CWLockGuard lock(ups_list_lock_);
                update_server_ = server;
              }

              if (OB_SUCCESS == ret)
              {
                // using old protocol get update server vip for daily merge
                ret = rpc_stub_->fetch_update_server(rpc_timeout_, root_server_,
                    server, true);
                if (ret != OB_SUCCESS)
                {
                  TBSYS_LOG(WARN, "find update server vip for daily merge failed:ret[%d]", ret);
                }
                else
                {
                  tbsys::CWLockGuard lock(ups_list_lock_);
                  inconsistency_update_server_ = server;
                }
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
      tbsys::CWLockGuard lock(ups_list_lock_);
      server = update_server_;
      return ret;
    }

    bool ObMergerRpcProxy::check_range_param(const ObRange & range_param)
    {
      bool bret = true;
      if (((!range_param.border_flag_.is_min_value()) && (0 == range_param.start_key_.length()))
          || (!range_param.border_flag_.is_max_value() && (0 == range_param.end_key_.length())))
      {
        TBSYS_LOG(WARN, "check range param failed");
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
        TBSYS_LOG(WARN, "check scan range failed");
        bret = false;
      }
      else
      {
        bret = check_range_param(*range);
      }
      return bret;
    }

    // must be in one chunk server
    int ObMergerRpcProxy::cs_get(
        const ObGetParam & get_param,
        ObScanner & scanner,  ObIterator *it_out[],int64_t& it_size)
    {
      UNUSED(get_param);
      UNUSED(scanner);
      UNUSED(it_out);
      UNUSED(it_size);
      TBSYS_LOG(WARN, "not implement");
      return OB_ERROR;
    }

    // only one communication with chunk server
    int ObMergerRpcProxy::cs_scan(
        const ObScanParam & scan_param,
        ObScanner & scanner, ObIterator *it_out[],int64_t& it_size)
    {
      UNUSED(scan_param);
      UNUSED(scanner);
      UNUSED(it_out);
      UNUSED(it_size);
      TBSYS_LOG(WARN, "not implement");
      return OB_ERROR;
    }

    void ObMergerRpcProxy::modify_ups_list(ObUpsList & list)
    {
      if (0 == list.ups_count_)
      {
        // add vip update server to list
        TBSYS_LOG(DEBUG, "check ups count is zero:count[%d]", list.ups_count_);
        ObUpsInfo info;
        info.addr_ = update_server_;
        // set inner port to update server port
        info.inner_port_ = inconsistency_update_server_.get_port();
        info.ms_read_percentage_ = 100;
        info.cs_read_percentage_ = 100;
        list.ups_count_ = 1;
        list.ups_array_[0] = info;
        list.sum_ms_percentage_ = 100;
        list.sum_cs_percentage_ = 100;
      }
      else
      {
        TBSYS_LOG(DEBUG, "reset the percentage for all servers");
        if (list.get_sum_percentage(MERGE_SERVER) <= 0)
        {
          for (int32_t i = 0; i < list.ups_count_; ++i)
          {
            // reset all ms to equal
            list.ups_array_[i].ms_read_percentage_ = 1;
          }
          // reset all ms sum percentage to count
          list.sum_ms_percentage_ = list.ups_count_;
        }
        if (list.get_sum_percentage(CHUNK_SERVER) <= 0)
        {
          for (int32_t i = 0; i < list.ups_count_; ++i)
          {
            // reset all cs to equal
            list.ups_array_[i].cs_read_percentage_ = 1;
          }
          // reset all cs sum percentage to count
          list.sum_cs_percentage_ = list.ups_count_;
        }
      }
    }

    int ObMergerRpcProxy::master_ups_get(const ObGetParam & get_param, ObScanner & scanner,
        const int64_t time_out)
    {
      int ret = OB_ERROR;
      ObServer update_server;
      for (int64_t i = 0; i <= rpc_retry_times_; ++i)
      {
        ret = get_update_server((OB_NOT_MASTER == ret), update_server);
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(WARN, "get master update server failed:ret[%d]", ret);
          break;
        }
        ret = rpc_stub_->get((time_out > 0) ? time_out : rpc_timeout_, update_server, get_param, scanner);
        if (OB_INVALID_START_VERSION == ret)
        {
          TBSYS_LOG(WARN, "check chunk server data version failed:ret[%d]", ret);
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
          TBSYS_LOG(DEBUG, "%s", "ups get data succ");
          break;
        }
        usleep(static_cast<useconds_t>(RETRY_INTERVAL_TIME * (i + 1)));
      }
      return ret;
    }

    int ObMergerRpcProxy::slave_ups_get(const ObGetParam & get_param,
      ObScanner & scanner, const ObServerType server_type, const int64_t time_out)
    {
      int ret = OB_SUCCESS;
      int32_t retry_times = 0;
      int32_t cur_index = -1;
      int32_t max_count = 0;
      //LOCK BLOCK
      {
        tbsys::CRLockGuard lock(ups_list_lock_);
        int32_t server_count = max_count = update_server_list_.ups_count_;
        cur_index = ObReadUpsBalance::select_server(update_server_list_, server_type);
        if (cur_index < 0)
        {
          TBSYS_LOG(WARN, "select server failed:count[%d], index[%d]", server_count, cur_index);
          ret = OB_ERROR;
        }
        else
        {
          ObUpsBlackList& black_list =
            (MERGE_SERVER == server_type) ? black_list_ : ups_black_list_for_merge_;
          // bring back to alive no need write lock
          if (black_list.get_valid_count() <= 0)
          {
            TBSYS_LOG(WARN, "check all the update server not invalid:count[%d]",
                black_list.get_valid_count());
            black_list.reset();
          }
        }
      }
      if (OB_SUCCESS == ret)
      {
        ret = OB_ERROR;
        ObServer update_server;
        for (int32_t i = cur_index; retry_times < max_count; ++i, ++retry_times)
        {
          //LOCK BLOCK
          {
            tbsys::CRLockGuard lock(ups_list_lock_);
            int32_t server_count = update_server_list_.ups_count_;
            if (false == check_server(i % server_count, server_type))
            {
              TBSYS_LOG(WARN, "check update server failed:index[%d]", i%server_count);
              continue;
            }
            update_server = update_server_list_.ups_array_[i%server_count].get_server(server_type);
          }

          TBSYS_LOG(DEBUG, "select slave update server for get:index[%d], ip[%d], port[%d]",
              i, update_server.get_ipv4(), update_server.get_port());
          ret = rpc_stub_->get((time_out > 0) ? time_out : rpc_timeout_, update_server, get_param, scanner);
          if (OB_INVALID_START_VERSION == ret)
          {
            TBSYS_LOG(WARN, "check chunk server data version failed:ret[%d]", ret);
          }
          else if (ret != OB_SUCCESS)
          {
            // inc update server fail counter for blacklist
            //LOCK BLOCK
            {
              tbsys::CRLockGuard lock(ups_list_lock_);
              int32_t server_count = update_server_list_.ups_count_;
              ObUpsBlackList& black_list =
                (MERGE_SERVER == server_type) ? black_list_ : ups_black_list_for_merge_;
              black_list.fail(i%server_count, update_server);
              TBSYS_LOG(WARN, "get from update server failed:ip[%d], port[%d], ret[%d]",
                  update_server.get_ipv4(), update_server.get_port(), ret);
            }
          }
          else
          {
            TBSYS_LOG(DEBUG, "%s", "ups get data succ");
            break;
          }
        }
      }
      return ret;
    }

    //
    int ObMergerRpcProxy::ups_get(const ObGetParam & get_param,
      ObScanner & scanner, const ObServerType server_type, const int64_t time_out)
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
        ret = master_ups_get(get_param, scanner, time_out);
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "get from master ups failed:ret[%d]", ret);
        }
      }
      else
      {
        ret = slave_ups_get(get_param, scanner, server_type, time_out);
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "get from slave ups failed:ret[%d]", ret);
        }
      }

      if ((OB_SUCCESS == ret) && (get_param.get_cell_size() > 0) && scanner.is_empty())
      {
        TBSYS_LOG(WARN, "update server error, response request with zero cell");
        ret = OB_ERR_UNEXPECTED;
      }

      if (OB_SUCCESS == ret && TBSYS_LOGGER._level >= TBSYS_LOG_LEVEL_DEBUG)
      {
        TBSYS_LOG(DEBUG, "ups_get");
        output(scanner);
      }
      return ret;
    }

    int ObMergerRpcProxy::master_ups_scan(const ObScanParam & scan_param, ObScanner & scanner,
        const int64_t time_out)
    {
      int ret = OB_ERROR;
      ObServer update_server;
      for (int64_t i = 0; i <= rpc_retry_times_; ++i)
      {
        ret = get_update_server((OB_NOT_MASTER == ret), update_server);
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(WARN, "get master update server failed:ret[%d]", ret);
          break;
        }
        ret = rpc_stub_->scan((time_out > 0) ? time_out : rpc_timeout_, update_server, scan_param, scanner);
        if (OB_INVALID_START_VERSION == ret)
        {
          TBSYS_LOG(WARN, "check chunk server data version failed:ret[%d]", ret);
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
          TBSYS_LOG(DEBUG, "%s", "ups scan data succ");
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

    int ObMergerRpcProxy::set_blacklist_param(
        const int64_t timeout, const int64_t fail_count)
    {
      int ret = OB_SUCCESS;
      if ((timeout <= 0) || (fail_count <= 0))
      {
        ret = OB_INVALID_ARGUMENT;
        TBSYS_LOG(WARN, "check blacklist param failed:timeout[%ld], threshold[%ld]",
          timeout, fail_count);
      }
      else
      {
        fail_count_threshold_ = fail_count;
        black_list_timeout_ = timeout;
      }
      return ret;
    }

    bool ObMergerRpcProxy::check_server(const int32_t index, const ObServerType server_type)
    {
      bool ret = true;
      ObUpsBlackList& black_list =
        (MERGE_SERVER == server_type) ? black_list_ : ups_black_list_for_merge_;

      // check the read percentage and not in black list, if in list timeout make it to be alive
      if ((false == black_list.check(index))
        || (update_server_list_.ups_array_[index].get_read_percentage(server_type) <= 0))
      {
        ret = false;
      }

      return ret;
    }

    int ObMergerRpcProxy::slave_ups_scan(const ObScanParam & scan_param,
      ObScanner & scanner, const ObServerType server_type, const int64_t time_out)
    {
      int ret = OB_SUCCESS;
      int32_t retry_times = 0;
      int32_t cur_index = -1;
      int32_t max_count = 0;
      //LOCK BLOCK
      {
        tbsys::CRLockGuard lock(ups_list_lock_);
        int32_t server_count = max_count = update_server_list_.ups_count_;
        cur_index = ObReadUpsBalance::select_server(update_server_list_, server_type);
        if (cur_index < 0)
        {
          TBSYS_LOG(WARN, "select server failed:count[%d], index[%d]", server_count, cur_index);
          ret = OB_ERROR;
        }
        else
        {
          ObUpsBlackList& black_list =
            (MERGE_SERVER == server_type) ? black_list_ : ups_black_list_for_merge_;
          // bring back to alive no need write lock
          if (black_list.get_valid_count() <= 0)
          {
            TBSYS_LOG(WARN, "check all the update server not invalid:count[%d]",
                black_list.get_valid_count());
            black_list.reset();
          }
        }
      }

      if (OB_SUCCESS == ret)
      {
        ret = OB_ERROR;
        ObServer update_server;
        for (int32_t i = cur_index; retry_times < max_count; ++i, ++retry_times)
        {
          //LOCK BLOCK
          {
            tbsys::CRLockGuard lock(ups_list_lock_);
            int32_t server_count = update_server_list_.ups_count_;
            if (false == check_server(i % server_count, server_type))
            {
              TBSYS_LOG(WARN, "check update server failed:index[%d]", i%server_count);
              continue;
            }
            update_server = update_server_list_.ups_array_[i%server_count].get_server(server_type);
          }
          TBSYS_LOG(DEBUG, "select slave update server for scan:index[%d], ip[%d], port[%d]",
              i, update_server.get_ipv4(), update_server.get_port());
          ret = rpc_stub_->scan((time_out > 0) ? time_out : rpc_timeout_, update_server, scan_param, scanner);
          if (OB_INVALID_START_VERSION == ret)
          {
            TBSYS_LOG(WARN, "check chunk server data version failed:ret[%d]", ret);
          }
          else if (ret != OB_SUCCESS)
          {
            // inc update server fail counter for blacklist
            //LOCK BLOCK
            {
              tbsys::CRLockGuard lock(ups_list_lock_);
              int32_t server_count = update_server_list_.ups_count_;
              ObUpsBlackList& black_list =
                (MERGE_SERVER == server_type) ? black_list_ : ups_black_list_for_merge_;
              black_list.fail(i%server_count, update_server);
              TBSYS_LOG(WARN, "scan from update server failed:ip[%d], port[%d], ret[%d]",
                  update_server.get_ipv4(), update_server.get_port(), ret);
            }
          }
          else
          {
            TBSYS_LOG(DEBUG, "%s", "ups get data succ");
            break;
          }
        }
      }
      return ret;
    }

    int ObMergerRpcProxy::ups_scan(const ObScanParam & scan_param,
      ObScanner & scanner, const ObServerType server_type, const int64_t time_out )
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
        ret = master_ups_scan(scan_param, scanner, time_out);
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "scan from master ups failed:ret[%d]", ret);
        }
      }
      else
      {
        ret = slave_ups_scan(scan_param, scanner, server_type, time_out);
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
  } // end namespace chunkserver
} // end namespace oceanbase
