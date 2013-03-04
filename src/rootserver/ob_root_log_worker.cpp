/*===============================================================
*   (C) 2007-2010 Taobao Inc.
*   
*   
*   Version: 0.1 2010-09-19
*   
*   Authors:
*          ruohai(ruohai@taobao.com)
*   
*
================================================================*/
#include "common/ob_malloc.h"
#include "common/file_utils.h"
#include "rootserver/ob_root_log_worker.h"
#include "rootserver/ob_root_server2.h"
#include "rootserver/ob_root_log_manager.h"
#include "rootserver/ob_root_worker.h"
namespace 
{
  int SYNC_WAIT_US = 10;
}

namespace oceanbase
{
  using namespace common;
  namespace rootserver
  {
    ObRootLogWorker::ObRootLogWorker()
    {
    }

    void ObRootLogWorker::set_root_server(ObRootServer2* root_server)
    {
      root_server_ = root_server;
    }

    void ObRootLogWorker::set_log_manager(ObRootLogManager* log_manager)
    {
      log_manager_ = log_manager;
    }

    int ObRootLogWorker::sync_schema(const int64_t timestamp)
    {
      int ret = OB_SUCCESS;
      char* log_data = static_cast<char*>(ob_malloc(OB_MAX_PACKET_LENGTH));
      if (log_data == NULL)
      {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        TBSYS_LOG(ERROR, "allocate memory failed, size: %ld", OB_MAX_PACKET_LENGTH);
      }

      int64_t pos = 0;
      // read schema conteng from schema file
      if (ret == OB_SUCCESS)
      {
        FileUtils fu;
        int32_t rc = fu.open(root_server_->config_.flag_schema_filename_.get(), O_RDONLY);
        if (rc < 0)
        {
          ret = OB_ERROR;
          TBSYS_LOG(ERROR, "open schema file failed when sync");
        }
        else
        {
          // read schema content from file
          char* tmp_buffer = static_cast<char*>(ob_malloc(OB_MAX_PACKET_LENGTH));
          if (tmp_buffer == NULL)
          {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            TBSYS_LOG(ERROR, "allocate memory failed, size: %ld", OB_MAX_PACKET_LENGTH);
          }

          int64_t rl = 0;
          if (ret == OB_SUCCESS)
          {
            rl = fu.read(tmp_buffer, OB_MAX_PACKET_LENGTH);
            if (rl < 0)
            {
              ret = OB_ERROR;
              TBSYS_LOG(ERROR, "read from schema file failed");
            }

            if (rl == OB_MAX_PACKET_LENGTH)
            {
              TBSYS_LOG(ERROR, "schema file too large, size: %ld", OB_MAX_PACKET_LENGTH);
            }
          }

          if (ret == OB_SUCCESS)
          {
            ret = serialization::encode_vstr(log_data, OB_MAX_PACKET_LENGTH, pos, tmp_buffer, rl);
          }

          if (tmp_buffer != NULL)
          {
            ob_free(tmp_buffer);
            tmp_buffer = NULL;
          }

          fu.close();
        }
      }

      if (ret == OB_SUCCESS)
      {
        ret = serialization::encode_vi64(log_data, OB_MAX_PACKET_LENGTH, pos, timestamp);
      }

      if (ret == OB_SUCCESS)
      {
        ret = flush_log(OB_RT_SCHEMA_SYNC, log_data, pos);
      }

      if (log_data != NULL)
      {
        ob_free(log_data);
        log_data = NULL;
      }

      return ret;
    }

    int ObRootLogWorker::regist_cs(const ObServer& server, const int64_t timestamp)
    {
      return log_server_with_ts(OB_RT_CS_REGIST, server, timestamp);
    }

    int ObRootLogWorker::regist_ms(const ObServer& server, const int64_t timestamp)
    {
      return log_server_with_ts(OB_RT_MS_REGIST, server, timestamp);
    }

    int ObRootLogWorker::server_is_down(const ObServer& server, const int64_t timestamp)
    {
      return log_server_with_ts(OB_RT_SERVER_DOWN, server, timestamp);
    }

    int ObRootLogWorker::log_server_with_ts(const LogCommand cmd, const ObServer& server, const int64_t timestamp)
    {
      int ret = OB_SUCCESS;

      char* log_data = static_cast<char*>(ob_malloc(OB_MAX_PACKET_LENGTH));
      if (log_data == NULL)
      {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        TBSYS_LOG(ERROR, "allocate memory failed");
      }

      int64_t pos = 0;
      if (ret == OB_SUCCESS)
      {
        ret = server.serialize(log_data, OB_MAX_PACKET_LENGTH, pos);
      }

      if (ret == OB_SUCCESS)
      {
        ret = serialization::encode_vi64(log_data, OB_MAX_PACKET_LENGTH, pos, timestamp);
      }

      if (ret == OB_SUCCESS)
      {
        ret = flush_log(cmd, log_data, pos);
      }

      if (log_data != NULL)
      {
        ob_free(log_data);
        log_data = NULL;
      }

      return ret;
    }

    int ObRootLogWorker::report_cs_load(const ObServer& server, const int64_t capacity, const int64_t used)
    {
      int ret = OB_SUCCESS;

      char* log_data = static_cast<char*>(ob_malloc(OB_MAX_PACKET_LENGTH));
      if (log_data == NULL)
      {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        TBSYS_LOG(ERROR, "allocate memory failed");
      }

      int64_t pos = 0;
      if (ret == OB_SUCCESS)
      {
        ret = server.serialize(log_data, OB_MAX_PACKET_LENGTH, pos);
      }

      if (ret == OB_SUCCESS)
      {
        ret = serialization::encode_vi64(log_data, OB_MAX_PACKET_LENGTH, pos, capacity);
      }

      if (ret == OB_SUCCESS)
      {
        ret = serialization::encode_vi64(log_data, OB_MAX_PACKET_LENGTH, pos, used);
      }

      if (ret == OB_SUCCESS)
      {
        ret = flush_log(OB_RT_CS_LOAD_REPORT, log_data, pos);
      }

      if (log_data != NULL)
      {
        ob_free(log_data);
        log_data = NULL;
      }

      return ret;
    }

    int ObRootLogWorker::cs_migrate_done(const ObRange& range, const ObServer& src_server, const ObServer& dest_server, const bool keep_src, const int64_t tablet_version)
    {
      int ret = OB_SUCCESS;

      char* log_data = static_cast<char*>(ob_malloc(OB_MAX_PACKET_LENGTH));
      if (log_data == NULL)
      {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        TBSYS_LOG(ERROR, "allocate memory failed");
      }

      int64_t pos = 0;
      if (ret == OB_SUCCESS)
      {
        ret = range.serialize(log_data, OB_MAX_PACKET_LENGTH, pos);
      }

      if (ret == OB_SUCCESS)
      {
        ret = src_server.serialize(log_data, OB_MAX_PACKET_LENGTH, pos);
      }

      if (ret == OB_SUCCESS)
      {
        ret = dest_server.serialize(log_data, OB_MAX_PACKET_LENGTH, pos);
      }

      if (ret == OB_SUCCESS)
      {
        ret = serialization::encode_bool(log_data, OB_MAX_PACKET_LENGTH, pos, keep_src);
      }

      if (ret == OB_SUCCESS)
      {
        ret = serialization::encode_vi64(log_data, OB_MAX_PACKET_LENGTH, pos, tablet_version);
      }

      if (ret == OB_SUCCESS)
      {
        ret = flush_log(OB_RT_CS_MIGRATE_DONE, log_data, pos);
      }

      if (log_data != NULL)
      {
        ob_free(log_data);
        log_data = NULL;
      }

      return ret;
    }

    int ObRootLogWorker::report_tablets(const common::ObServer& server, const common::ObTabletReportInfoList& tablets, const int64_t timestamp)
    {
      int ret = OB_SUCCESS;

      char* log_data = NULL;
      log_data = static_cast<char*>(ob_malloc(OB_MAX_PACKET_LENGTH));
      if (log_data == NULL)
      {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        TBSYS_LOG(ERROR, "allocate memory failed");
      }

      int64_t pos = 0;
      if (ret == OB_SUCCESS)
      {
        ret = serialization::encode_vi64(log_data, OB_MAX_PACKET_LENGTH, pos, timestamp);
      }

      if (ret == OB_SUCCESS)
      {
        ret = server.serialize(log_data, OB_MAX_PACKET_LENGTH, pos);
      }

      if (ret == OB_SUCCESS)
      {
        ret = tablets.serialize(log_data, OB_MAX_PACKET_LENGTH, pos);
      }

      if (ret == OB_SUCCESS)
      {
        ret = flush_log(OB_RT_REPORT_TABLETS, log_data, pos);
      }

      if (log_data != NULL)
      {
        ob_free(log_data);
        log_data = NULL;
      }

      return ret;
    }

    int ObRootLogWorker::remove_replica(const ObTabletReportInfo &replica)
    {
      int ret = OB_SUCCESS;
      char* log_data = NULL;
      log_data = static_cast<char*>(ob_malloc(OB_MAX_PACKET_LENGTH));
      if (log_data == NULL)
      {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        TBSYS_LOG(ERROR, "allocate memory failed");
      }
      else
      {
        int64_t pos = 0;
        if (OB_SUCCESS != (ret = replica.serialize(log_data, OB_MAX_PACKET_LENGTH, pos)))
        {
          TBSYS_LOG(WARN, "failed to serialize");
        }
        else if (OB_SUCCESS != (ret = flush_log(OB_RT_REMOVE_REPLICA, log_data, pos)))
        {
          TBSYS_LOG(WARN, "failed to flush log, err=%d", ret);
        }
      }
      if (NULL != log_data)
      {
        ob_free(log_data);
        log_data = NULL;
      }
      return ret;
    }    

    int ObRootLogWorker::remove_table(const common::ObArray<uint64_t> &deleted_tables)
    {
      int ret = OB_SUCCESS;
      char* log_data = NULL;
      log_data = static_cast<char*>(ob_malloc(OB_MAX_PACKET_LENGTH));
      int64_t pos = 0;
      if (log_data == NULL)
      {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        TBSYS_LOG(ERROR, "allocate memory failed");
      }
      else if (OB_SUCCESS != (ret = serialization::encode_vi64(log_data, OB_MAX_PACKET_LENGTH, pos, deleted_tables.count())))
      {
        TBSYS_LOG(WARN, "failed to serialize");
      }
      else 
      {
        for (int32_t i = 0; i < deleted_tables.count(); ++i)
        {
          if (OB_SUCCESS != (ret = serialization::encode_vi64(log_data, OB_MAX_PACKET_LENGTH, pos, deleted_tables.at(i))))
          {
            TBSYS_LOG(WARN, "failed to serialize");
            break;
          }
        }
        if (OB_SUCCESS == ret)
        {
          if (OB_SUCCESS != (ret = flush_log(OB_RT_REMOVE_TABLE, log_data, pos)))
          {
            TBSYS_LOG(WARN, "failed to flush log, err=%d", ret);
          }
        }
      }
      if (NULL != log_data)
      {
        ob_free(log_data);
        log_data = NULL;
      }
      return ret;
    }
    
    int ObRootLogWorker::add_new_tablet(const int count, const common::ObTabletInfo tablet, const int* server_indexs, const int64_t mem_version)
    {
      int ret = OB_SUCCESS;

      char* log_data = NULL;
      log_data = static_cast<char*>(ob_malloc(OB_MAX_PACKET_LENGTH));
      if (log_data == NULL)
      {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        TBSYS_LOG(ERROR, "allocate memory failed");
      }

      int64_t pos = 0;
      if (ret == OB_SUCCESS)
      {
        ret = serialization::encode_vi32(log_data, OB_MAX_PACKET_LENGTH, pos, count);
      }

      if (ret == OB_SUCCESS)
      {
        ret = tablet.serialize(log_data, OB_MAX_PACKET_LENGTH, pos);
      }

      if (ret == OB_SUCCESS)
      {
        for (int i=0; i<count; ++i)
        {
          ret = serialization::encode_vi32(log_data, OB_MAX_PACKET_LENGTH, pos, server_indexs[i]);
          if (ret != OB_SUCCESS)
          {
            TBSYS_LOG(ERROR, "serialize failed");
            break;
          }

        }
      }

      if (ret == OB_SUCCESS)
      {
        ret = serialization::encode_vi32(log_data, OB_MAX_PACKET_LENGTH, pos, static_cast<int32_t>(mem_version));
      }

      if (ret == OB_SUCCESS)
      {
        ret = flush_log(OB_RT_ADD_NEW_TABLET, log_data, pos);
      }

      if (log_data != NULL)
      {
        ob_free(log_data);
        log_data = NULL;
      }

      return ret;
    }
int ObRootLogWorker::batch_add_new_tablet(const common::ObTabletInfoList& tablets, 
    int** server_indexs, int* count, const int64_t mem_version)
{
  int ret = OB_SUCCESS;

  char* log_data = NULL;
  log_data = static_cast<char*>(ob_malloc(OB_MAX_PACKET_LENGTH));
  if (log_data == NULL)
  {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    TBSYS_LOG(ERROR, "allocate memory failed");
  }

  int64_t pos = 0;
  int64_t index = tablets.tablet_list.get_array_index();
  ObTabletInfo *p_table_info = NULL;
  if (OB_SUCCESS == ret)
  {
    ret = serialization::encode_vi64(log_data, OB_MAX_PACKET_LENGTH, pos, index);
  }
  if (OB_SUCCESS == ret)
  {
    for (int64_t i = 0; OB_SUCCESS == ret && i < index; i++)
    {
      p_table_info = tablets.tablet_list.at(i);
      if (NULL == p_table_info)
      {
        TBSYS_LOG(WARN, "p_table_info should not be NULL");
        ret = OB_ERROR;
      }
      if (ret == OB_SUCCESS)
      {
        ret = serialization::encode_vi32(log_data, OB_MAX_PACKET_LENGTH, pos, count[i]);
      }

      if (ret == OB_SUCCESS)
      {
        ret = p_table_info->serialize(log_data, OB_MAX_PACKET_LENGTH, pos);
      }

      if (ret == OB_SUCCESS)
      {
        for (int j = 0; j < count[i]; ++j)
        {
          ret = serialization::encode_vi32(log_data, OB_MAX_PACKET_LENGTH, pos, server_indexs[i][j]);
          if (ret != OB_SUCCESS)
          {
            TBSYS_LOG(ERROR, "serialize failed");
            break;
          }
        }
      }
    }
  }

  if (ret == OB_SUCCESS)
  {
    ret = serialization::encode_vi32(log_data, OB_MAX_PACKET_LENGTH, pos, static_cast<int32_t>(mem_version));
  }

  if (ret == OB_SUCCESS)
  {
    ret = flush_log(OB_RT_BATCH_ADD_NEW_TABLET, log_data, pos);
  }

  if (log_data != NULL)
  {
    ob_free(log_data);
    log_data = NULL;
  }

  return ret;
}

    int ObRootLogWorker::cs_merge_over(const ObServer& server, const int64_t timestamp)
    {
      return log_server_with_ts(OB_RT_CS_MERGE_OVER, server, timestamp);
    }

    int ObRootLogWorker::sync_us_frozen_version(const int64_t frozen_version, const int64_t last_frozen_time)
    {
      int ret = OB_SUCCESS;

      char* log_data = static_cast<char*>(ob_malloc(OB_MAX_PACKET_LENGTH));
      if (log_data == NULL)
      {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        TBSYS_LOG(ERROR, "allocate memory failed");
      }

      int64_t pos = 0;
      if (ret == OB_SUCCESS)
      {
        ret = serialization::encode_vi64(log_data, OB_MAX_PACKET_LENGTH, pos,frozen_version);
      }

      if (ret == OB_SUCCESS)
      {
        ret = serialization::encode_vi64(log_data, OB_MAX_PACKET_LENGTH, pos,last_frozen_time);
      }

      if (ret == OB_SUCCESS)
      {
        ret = flush_log(OB_RT_SYNC_FROZEN_VERSION_AND_TIME, log_data, pos);
      }

      if (log_data != NULL)
      {
        ob_free(log_data);
        log_data = NULL;
      }
      return ret;
    }

    int ObRootLogWorker::log_server(const LogCommand cmd, const ObServer& server)
    {
      int ret = OB_SUCCESS;

      char* log_data = static_cast<char*>(ob_malloc(OB_MAX_PACKET_LENGTH));
      if (log_data == NULL)
      {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        TBSYS_LOG(ERROR, "allocate memory failed");
      }

      int64_t pos = 0;
      if (ret == OB_SUCCESS)
      {
        ret = server.serialize(log_data, OB_MAX_PACKET_LENGTH, pos);
      }

      if (ret == OB_SUCCESS)
      {
        ret = flush_log(cmd, log_data, pos);
      }

      if (log_data != NULL)
      {
        ob_free(log_data);
        log_data = NULL;
      }

      return ret;
    }

    int ObRootLogWorker::set_ups_list(const common::ObUpsList &ups_list)
    {
      int ret = OB_SUCCESS;
      int64_t pos = 0;
      char* log_data = static_cast<char*>(ob_malloc(OB_MAX_PACKET_LENGTH));

      if (NULL == log_data)
      {
        TBSYS_LOG(ERROR, "no memory");
        ret = OB_ALLOCATE_MEMORY_FAILED;
      }
      else if (OB_SUCCESS != (ret = ups_list.serialize(log_data, OB_MAX_PACKET_LENGTH, pos)))
      {
        TBSYS_LOG(ERROR, "serialize error");
      }
      else
      {
        ret = flush_log(OB_RT_SET_UPS_LIST, log_data, pos);
      }
      if (NULL != log_data)
      {
        ob_free(log_data);
        log_data = NULL;
      }
      return ret;
    }
    
    int ObRootLogWorker::set_client_config(const common::ObClientConfig &client_conf)
    {
      int ret = OB_SUCCESS;
      int64_t pos = 0;
      char* log_data = static_cast<char*>(ob_malloc(OB_MAX_PACKET_LENGTH));

      if (NULL == log_data)
      {
        TBSYS_LOG(ERROR, "no memory");
        ret = OB_ALLOCATE_MEMORY_FAILED;
      }
      else if (OB_SUCCESS != (ret = client_conf.serialize(log_data, OB_MAX_PACKET_LENGTH, pos)))
      {
        TBSYS_LOG(ERROR, "serialize error");
      }
      else
      {
        ret = flush_log(OB_RT_SET_CLIENT_CONFIG, log_data, pos);
      }
      if (NULL != log_data)
      {
        ob_free(log_data);
        log_data = NULL;
      }
      return ret;
    }
    
    int ObRootLogWorker::flush_log(const LogCommand cmd, const char* log_data, const int64_t& serialize_size)
    {
      int ret = OB_SUCCESS;

      TBSYS_LOG(DEBUG, "flush update log, cmd type: %d", cmd);

      tbsys::CThreadGuard guard(log_manager_->get_log_sync_mutex());
      ret = log_manager_->write_and_flush_log(cmd, log_data, serialize_size);

      return ret;
    }

    //////////////////////////////////////////////////
    ///// slave apply log methods
    //////////////////////////////////////////////////

    int ObRootLogWorker::apply(common::LogCommand cmd, const char* log_data, const int64_t& data_len)
    {
      int ret = OB_SUCCESS;

      TBSYS_LOG(INFO, "start replay log, cmd type: %d", cmd);
      switch (cmd)
      {
        case OB_RT_SCHEMA_SYNC:
          ret = do_schema_sync(log_data, data_len);
          break;
        case OB_RT_CS_REGIST:
          ret = do_cs_regist(log_data, data_len);
          break;
        case OB_RT_MS_REGIST:
          ret = do_ms_regist(log_data, data_len);
          break;
        case OB_RT_SERVER_DOWN:
          ret = do_server_down(log_data, data_len);
          break;
        case OB_RT_CS_LOAD_REPORT:
          ret = do_cs_load_report(log_data, data_len);
          break;
        case OB_RT_CS_MIGRATE_DONE:
          ret = do_cs_migrate_done(log_data, data_len);
          break;
        case OB_RT_REPORT_TABLETS:
          ret = do_report_tablets(log_data, data_len);
          break;
        case OB_RT_ADD_NEW_TABLET:
          ret = do_add_new_tablet(log_data, data_len);
          break;
        case OB_RT_BATCH_ADD_NEW_TABLET:
          ret = do_batch_add_new_tablet(log_data, data_len);
          break;
        case OB_RT_CREATE_TABLE_DONE:
          ret = do_create_table_done();
          break;
        case OB_RT_BEGIN_BALANCE:
          ret = do_begin_balance();
          break;
        case OB_RT_BALANCE_DONE:
          ret = do_balance_done();
          break;
        case OB_RT_CS_MERGE_OVER:
          ret = do_cs_merge_over(log_data, data_len);
          break;
        case OB_LOG_CHECKPOINT:
          ret = do_check_point(log_data, data_len);
          break;
        case OB_RT_SYNC_FROZEN_VERSION:
          ret = do_sync_frozen_version(log_data,data_len);
          break;
        case OB_RT_SYNC_FROZEN_VERSION_AND_TIME:
          ret = do_sync_frozen_version_and_time(log_data, data_len);
          break;
        case OB_RT_SET_UPS_LIST:
          ret = do_set_ups_list(log_data, data_len);
          break;
        case OB_RT_SET_CLIENT_CONFIG:
          ret = do_set_client_config(log_data, data_len);
          break;
        case OB_RT_REMOVE_REPLICA:
          ret = do_remove_replica(log_data, data_len);
          break;
        case OB_RT_REMOVE_TABLE:
          ret = do_remove_table(log_data, data_len);
          break;
        case OB_LOG_SWITCH_LOG:
          TBSYS_LOG(INFO, "apply: switch_log");
          break;
        default:
          TBSYS_LOG(WARN, "unknow log command [%d]", cmd);
          ret = OB_INVALID_ARGUMENT;
          break;
      }

      return ret;
    }

    int ObRootLogWorker::do_check_point(const char* log_data, const int64_t& log_length)
    {
      int ret = OB_SUCCESS;

      int64_t pos = 0;
      int64_t ckpt_id;

      ret = serialization::decode_i64(log_data, log_length, pos, &ckpt_id);

      if (ret == OB_SUCCESS)
      {
        if (root_server_->is_master())
        {
          TBSYS_LOG(WARN, "this is master, may have lost checkpointing, ckpt=%ld", ckpt_id);
        }
        else
        { 
          int sum = 0;
          while(root_server_->build_sync_flag_ != ObRootServer2::BUILD_SYNC_INIT_OK)
          {
            sum++;
            if (sum > 10000)
            {
              sum = 0;
              TBSYS_LOG(WARN, "too many time waiting for build_sync_flag_ %d", root_server_->build_sync_flag_);
            }
            usleep(SYNC_WAIT_US);
          }
          ret = log_manager_->do_check_point(ckpt_id);
        }
      }

      return ret;
    }

    int ObRootLogWorker::do_schema_sync(const char* log_data, const int64_t& log_length)
    {
      int ret = OB_SUCCESS;

      int64_t pos = 0;
      int64_t schema_length = 0;
      const char* schema_data = serialization::decode_vstr(log_data, log_length, pos, &schema_length);
      if (schema_data == NULL)
      {
        ret = OB_INVALID_ARGUMENT;
        TBSYS_LOG(WARN, "deserialization sync schema log failed");
      }

      int64_t schema_ts = 0;
      if (ret == OB_SUCCESS)
      {
        ret = serialization::decode_vi64(log_data, log_length, pos, &schema_ts);
      }

      if (ret == OB_SUCCESS)
      {
        FileUtils fu;
        int32_t rc = fu.open(root_server_->config_.flag_schema_filename_.get(), O_CREAT | O_WRONLY | O_TRUNC, 0644);
        if (rc < 0)
        {
          ret = OB_ERROR;
          TBSYS_LOG(ERROR, "open schema file failed when sync");
        }
        else
        {
          int64_t wl = fu.write(schema_data, schema_length);
          if (wl != schema_length)
          {
            ret = OB_ERROR;
            TBSYS_LOG(ERROR, "write schema into [%s] failed", root_server_->config_.flag_schema_filename_.get());
          }
          else
          {
            TBSYS_LOG(INFO, "generate new schema file, filename=%s ts=%ld", root_server_->config_.flag_schema_filename_.get(), schema_ts);
          }
          fu.close();
        }
      }

      if (OB_SUCCESS == ret)
      {
        common::ObArray<uint64_t> deleted_tables;
        ret = root_server_->switch_schema(schema_ts, deleted_tables);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "failed to load schema, err=%d", ret);
        }
      }
      
      return ret;
    }

    int ObRootLogWorker::do_cs_regist(const char* log_data, const int64_t& log_length)
    {
      int ret = OB_SUCCESS;

      int64_t pos = 0;
      int64_t csr_ts = 0;

      ObServer server;
      ret = server.deserialize(log_data, log_length, pos);

      if (ret == OB_SUCCESS)
      {
        ret = serialization::decode_vi64(log_data, log_length, pos, &csr_ts);
      }

      if (ret == OB_SUCCESS)
      {
        int32_t status = 0; // we don't care this
        ret = root_server_->regist_server(server, false, status, csr_ts);
      }

      return ret;
    }

    int ObRootLogWorker::do_ms_regist(const char* log_data, const int64_t& log_length)
    {
      int ret = OB_SUCCESS;

      int64_t pos = 0;
      int64_t msr_ts = 0;

      ObServer server;
      ret = server.deserialize(log_data, log_length, pos);

      if (ret == OB_SUCCESS)
      {
        ret = serialization::decode_vi64(log_data, log_length, pos, &msr_ts);
      }

      if (ret == OB_SUCCESS)
      {
        int32_t status = 0; // we don't care this
        ret = root_server_->regist_server(server, true, status, msr_ts);
      }

      return ret;
    }

    int ObRootLogWorker::do_server_down(const char* log_data, const int64_t& log_length)
    {
      int ret = OB_SUCCESS;

      int64_t pos = 0;
      int64_t sd_ts = 0;

      ObServer server;
      ret = server.deserialize(log_data, log_length, pos);

      if (ret == OB_SUCCESS)
      {
        ret = serialization::decode_vi64(log_data, log_length, pos, &sd_ts);
      }

      if (ret == OB_SUCCESS)
      {
        ObChunkServerManager::iterator it = root_server_->server_manager_.find_by_ip(server);
        if (it != NULL)
        {
          it->status_ = ObServerStatus::STATUS_DEAD;
          tbsys::CRLockGuard guard(root_server_->root_table_rwlock_);
          if (root_server_->root_table_ != NULL)
          {
            root_server_->root_table_->server_off_line(static_cast<int32_t>(it - root_server_->server_manager_.begin()), sd_ts);
            ret = OB_SUCCESS;
          }
        }
      }

      return ret;
    }

    int ObRootLogWorker::do_cs_load_report(const char* log_data, const int64_t& log_length)
    {
      int ret = OB_SUCCESS;

      ObServer server;
      int64_t pos = 0;
      int64_t capacity = 0;
      int64_t used = 0;

      ret = server.deserialize(log_data, log_length, pos);

      if (ret == OB_SUCCESS)
      {
        ret = serialization::decode_vi64(log_data, log_length, pos, &capacity);
      }

      if (ret == OB_SUCCESS)
      {
        ret = serialization::decode_vi64(log_data, log_length, pos, &used);
      }

      if (ret == OB_SUCCESS)
      {
        //ignore return value
        root_server_->update_capacity_info(server, capacity, used);
      }

      return ret;
    }

    int ObRootLogWorker::do_cs_migrate_done(const char* log_data, const int64_t& log_length)
    {
      int ret = OB_SUCCESS;

      int64_t pos = 0;
      ObRange range;
      ObServer src_server;
      ObServer dest_server;
      bool keep_src = false;
      int64_t tablet_version = 0;

      ret = range.deserialize(log_data, log_length, pos);
      if (ret == OB_SUCCESS)
      {
        ret = src_server.deserialize(log_data, log_length, pos);
      }

      if (ret == OB_SUCCESS)
      {
        ret = dest_server.deserialize(log_data, log_length, pos);
      }

      if (ret == OB_SUCCESS)
      {
        ret = serialization::decode_bool(log_data, log_length, pos, &keep_src);
      }

      if (ret == OB_SUCCESS)
      {
        ret = serialization::decode_vi64(log_data, log_length, pos, &tablet_version);
      }

      if (ret == OB_SUCCESS)
      {
        ret = root_server_->migrate_over(range, src_server, dest_server, keep_src, tablet_version);
        if (OB_ENTRY_NOT_EXIST == ret)
        {
          ret = OB_SUCCESS;
        }
      }

      return ret;
    }

    int ObRootLogWorker::do_report_tablets(const char* log_data, const int64_t& log_length)
    {
      int ret = OB_SUCCESS;

      int64_t pos = 0;
      ObServer server;
      ObTabletReportInfoList tablets;
      int64_t timestamp = 0;

      ret = serialization::decode_vi64(log_data, log_length, pos, &timestamp);

      if (ret == OB_SUCCESS)
      {
        ret = server.deserialize(log_data, log_length, pos);
      }

      if (ret == OB_SUCCESS)
      {
        ret = tablets.deserialize(log_data, log_length, pos);
      }

      if (ret == OB_SUCCESS)
      {
        root_server_->report_tablets(server, tablets, timestamp);
      }

      return ret;
    }

    int ObRootLogWorker::do_add_new_tablet(const char* log_data, const int64_t& log_length)
    {
      int ret = OB_SUCCESS;

      int64_t pos = 0;
      int64_t mem_version = 0;

      int count = 0;
      ObTabletInfo tablet;
      int server_indexs[OB_SAFE_COPY_COUNT];
      memset(server_indexs, 0, sizeof(int) * OB_SAFE_COPY_COUNT);

      ret = serialization::decode_vi32(log_data, log_length, pos, &count);

      if (ret == OB_SUCCESS)
      {
        ret = tablet.deserialize(log_data, log_length, pos);
      }

      if (ret == OB_SUCCESS && count > 0 )
      {
        for (int i = 0; i < count && i < OB_SAFE_COPY_COUNT; i++)
        {
          ret = serialization::decode_vi32(log_data, log_length, pos, server_indexs+i);
          if (ret != OB_SUCCESS)
          {
            break;
          }
        }
      }
      if (ret == OB_SUCCESS)
      {
        ret = serialization::decode_vi64(log_data, log_length, pos, &mem_version);
      }
      if (ret == OB_SUCCESS)
      {
        ret = root_server_->slave_create_new_table(tablet, server_indexs, count, mem_version);
      }

      return ret;
    }
    int ObRootLogWorker::do_batch_add_new_tablet(const char* log_data, const int64_t& log_length)
    {
      int ret = OB_SUCCESS;

      int64_t pos = 0;
      int64_t mem_version = 0;

      int64_t index = 0;
      ObTabletInfoList tablets;
      int32_t **server_index = NULL;
      int32_t * create_count = NULL;
      ret = serialization::decode_vi64(log_data, log_length, pos, &index);
      if (OB_SUCCESS == ret)
      {
        create_count = new (std::nothrow)int32_t[index];
        server_index = new (std::nothrow)int32_t*[index];
        if (NULL == server_index || NULL == create_count)
        {
          ret = OB_ALLOCATE_MEMORY_FAILED;
        }
        else
        {
          for (int32_t i = 0; i < index; i++)
          {
            server_index[i] = new(std::nothrow) int32_t[OB_SAFE_COPY_COUNT];
            if (NULL == server_index[i])
            {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              break;
            }
          }
        }
      }
      ObTabletInfo *tablet_info = new(std::nothrow) ObTabletInfo[index];
      if (NULL == tablet_info)
      {
        ret = OB_ERROR;
        TBSYS_LOG(WARN, "new tablet_info fail.");
      }
      for (int64_t i = 0; OB_SUCCESS == ret && i < index; i++)
      {
        ret = serialization::decode_vi32(log_data, log_length, pos, &create_count[i]);
        if (ret == OB_SUCCESS)
        {
          ret = tablet_info[i].deserialize(log_data, log_length, pos);
        }

        if (ret == OB_SUCCESS && create_count[i] > 0 )
        {
          for (int j = 0; j < create_count[i] && j < OB_SAFE_COPY_COUNT; j++)
          {
            ret = serialization::decode_vi32(log_data, log_length, pos, server_index[i] + j);
            if (ret != OB_SUCCESS)
            {
              break;
            }
          }
        }
        if (OB_SUCCESS == ret)
        {
          tablets.add_tablet(tablet_info[i]);
        }
      }
      if (ret == OB_SUCCESS)
      {
        ret = serialization::decode_vi64(log_data, log_length, pos, &mem_version);
      }
      if (ret == OB_SUCCESS)
      {
        ret = root_server_->slave_batch_create_new_table(tablets, server_index,
            create_count, mem_version);
      }
      for (int64_t i = 0; i < index; i++)
      {
        if (NULL != server_index[i])
        {
          delete [] server_index[i];
        }
      }
      if (NULL != server_index)
      {
        delete [] server_index;
      }
      if (NULL != create_count)
      {
        delete [] create_count;
      }
      if (OB_SUCCESS == ret)
      {
        TBSYS_LOG(INFO, "do batch add new tablet success.");
      }
      return ret;
    }

    int ObRootLogWorker::do_remove_replica(const char* log_data, const int64_t& log_length)
    {
      int ret = OB_SUCCESS;

      int64_t pos = 0;
      ObTabletReportInfo tablet;
      if (OB_SUCCESS != (ret = tablet.deserialize(log_data, log_length, pos)))
      {
        TBSYS_LOG(WARN, "deserialize error");
      }
      else if (OB_SUCCESS != (ret = root_server_->replay_remove_replica(tablet)))
      {
        TBSYS_LOG(ERROR, "replay remove replica error, err=%d", ret);
      }
      return ret;
    }

    int ObRootLogWorker::do_remove_table(const char* log_data, const int64_t& log_length)
    {
      int ret = OB_SUCCESS;
      int64_t int_val = 0;
      int64_t pos = 0;
      int64_t table_id;
      ObArray<uint64_t> deleted_tables;
      if (OB_SUCCESS != (ret = serialization::decode_vi64(log_data, log_length, pos, &int_val)))
      {
        TBSYS_LOG(WARN, "deserialize error");
      }
      else
      {
        for (int64_t i = 0; i < int_val; ++i)
        {
          if (OB_SUCCESS != (ret = serialization::decode_vi64(log_data, log_length, pos, &table_id)))
          {
            TBSYS_LOG(WARN, "deserialize error");
            break;
          }
          else if (OB_SUCCESS != (ret = deleted_tables.push_back(table_id)))
          {
            TBSYS_LOG(ERROR, "failed to push into array, err=%d", ret);
            break;
          }
        }
        if (OB_SUCCESS == ret)
        {
          if (OB_SUCCESS != (ret = root_server_->delete_tablets_from_root_table(deleted_tables)))
          {
            TBSYS_LOG(ERROR, "failed to delete tables, err=%d", ret);
          }
        }
      }
      return ret;
    }
    
    int ObRootLogWorker::do_create_table_done()
    {
      return OB_SUCCESS;
    }

    int ObRootLogWorker::do_begin_balance()
    {
      return OB_SUCCESS;
    }

    int ObRootLogWorker::do_balance_done()
    {
      return OB_SUCCESS;
    }

    int ObRootLogWorker::do_cs_merge_over(const char* log_data, const int64_t& log_length)
    {
      int ret = OB_SUCCESS;

      int64_t pos = 0;
      ObServer server;
      int64_t cs_merge_ts = 0;

      ret = server.deserialize(log_data, log_length, pos);

      if (ret == OB_SUCCESS)
      {
        ret = serialization::decode_vi64(log_data, log_length, pos, &cs_merge_ts);
      }

      if (ret == OB_SUCCESS)
      {
        //ignore return value
        root_server_->waiting_job_done(server, cs_merge_ts);
      }

      return ret;
    }

    int ObRootLogWorker::do_sync_frozen_version(const char* log_data, const int64_t& log_length)
    {
      int ret = OB_SUCCESS;

      int64_t pos = 0;
      int64_t frozen_version = 0;

      ret = serialization::decode_vi64(log_data, log_length, pos,&frozen_version);
      
      if (ret == OB_SUCCESS)
      {
        ret = root_server_->report_frozen_memtable(frozen_version, 0, true);
      }
      return ret;
    }

    int ObRootLogWorker::do_sync_frozen_version_and_time(const char* log_data, const int64_t& log_length)
    {
      int ret = OB_SUCCESS;

      int64_t pos = 0;
      int64_t frozen_version = 0;
      int64_t last_frozen_time = 0;

      ret = serialization::decode_vi64(log_data, log_length, pos,&frozen_version);

      if (ret == OB_SUCCESS)
      {
        serialization::decode_vi64(log_data, log_length, pos,&last_frozen_time);
      }

      if (ret == OB_SUCCESS)
      {
        ret = root_server_->report_frozen_memtable(frozen_version, last_frozen_time, true);
      }
      return ret;
    }

    int ObRootLogWorker::do_set_ups_list(const char* log_data, const int64_t& log_length)
    {
      int ret = OB_SUCCESS;
      int64_t pos = 0;
      ObUpsList ups_list;
      if (OB_SUCCESS != (ret = ups_list.deserialize(log_data, log_length, pos)))
      {
        TBSYS_LOG(ERROR, "deserialize error");
      }
      else
      {
        // do nothing
      }
      return ret;
    }

    int ObRootLogWorker::do_set_client_config(const char* log_data, const int64_t& log_length)
    {
      int ret = OB_SUCCESS;
      int64_t pos = 0;
      ObClientConfig client_conf;
      if (OB_SUCCESS != (ret = client_conf.deserialize(log_data, log_length, pos)))
      {
        TBSYS_LOG(ERROR, "deserialize error");
      }
      else
      {
        ret = root_server_->set_client_config(client_conf);
      }
      return ret;
    }
    
    void ObRootLogWorker::exit()
    {
      root_server_->worker_->stop();
    }


uint64_t ObRootLogWorker::get_cur_log_file_id()
{
  uint64_t ret = 0;
  if (NULL != log_manager_)
  {
    ret = log_manager_->get_cur_log_file_id();
  }
  return ret;
}

uint64_t ObRootLogWorker::get_cur_log_seq()
{
  uint64_t ret = 0;
  if (NULL != log_manager_)
  {
    ret = log_manager_->get_cur_log_seq();
  }
  return ret;
}

  } /* rootserver */
} /* oceanbase */
