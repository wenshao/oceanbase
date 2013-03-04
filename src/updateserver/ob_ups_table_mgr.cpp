/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: ups_tablet_mgr.cpp,v 0.1 2010/09/14 10:11:10 chuanhui Exp $
 *
 * Authors:
 *   chuanhui <rizhao.ych@taobao.com>
 *     - some work details if you want
 *
 */
#include "common/ob_trace_log.h"
#include "common/ob_row_compaction.h"
#include "ob_update_server_main.h"
#include "ob_ups_table_mgr.h"
#include "ob_client_wrapper.h"
#include "ob_client_wrapper_tsi.h"

namespace oceanbase
{
  namespace updateserver
  {
    using namespace oceanbase::common;
    int set_state_as_fatal()
    {
      int ret = OB_SUCCESS;
      ObUpdateServerMain *main = ObUpdateServerMain::get_instance();
      if (NULL == main)
      {
        TBSYS_LOG(ERROR, "get updateserver main null pointer");
        ret = OB_ERROR;
      }
      else
      {
        ObUpsRoleMgr& role_mgr = main->get_update_server().get_role_mgr();
        role_mgr.set_state(ObUpsRoleMgr::FATAL);
      }
      return ret;
    }

    ObUpsTableMgr :: ObUpsTableMgr(ObUpsCache& ups_cache) : log_buffer_(NULL),
                                       ups_cache_(ups_cache),
                                       check_checksum_(true),
                                       has_started_(false),
                                       last_bypass_checksum_(0)
    {
    }

    ObUpsTableMgr :: ~ObUpsTableMgr()
    {
      if (NULL != log_buffer_)
      {
        ob_free(log_buffer_);
        log_buffer_ = NULL;
      }
    }

    int ObUpsTableMgr :: init()
    {
      int err = OB_SUCCESS;

      err = table_mgr_.init();
      if (OB_SUCCESS != err)
      {
        TBSYS_LOG(WARN, "failed to init memtable list, err=%d", err);
      }
      else if (NULL == (log_buffer_ = (char*)ob_malloc(LOG_BUFFER_SIZE)))
      {
        TBSYS_LOG(WARN, "malloc log_buffer fail size=%ld", LOG_BUFFER_SIZE);
        err = OB_ERROR;
      }

      return err;
    }

    int ObUpsTableMgr::reg_table_mgr(SSTableMgr &sstable_mgr)
    {
      return sstable_mgr.reg_observer(&table_mgr_);
    }

    int ObUpsTableMgr :: get_active_memtable_version(uint64_t &version)
    {
      int ret = OB_SUCCESS;
      version = table_mgr_.get_active_version();
      if (OB_INVALID_ID == version)
      {
        ret = OB_ERROR;
      }
      return ret;
    }

    int ObUpsTableMgr :: get_last_frozen_memtable_version(uint64_t &version)
    {
      int ret = OB_SUCCESS;
      uint64_t active_version = table_mgr_.get_active_version();
      if (OB_INVALID_ID == active_version)
      {
        ret = OB_ERROR;
      }
      else
      {
        version = active_version - 1;
      }
      return ret;
    }

    int ObUpsTableMgr :: get_table_time_stamp(const uint64_t major_version, int64_t &time_stamp)
    {
      int ret = OB_SUCCESS;
      ret = table_mgr_.get_table_time_stamp(major_version, time_stamp);
      return ret;
    }

    int ObUpsTableMgr :: get_oldest_memtable_size(int64_t &size, uint64_t &major_version)
    {
      int ret = OB_SUCCESS;
      ret = table_mgr_.get_oldest_memtable_size(size, major_version);
      return ret;
    }

    int ObUpsTableMgr :: get_frozen_bloomfilter(const uint64_t version, TableBloomFilter &table_bf)
    {
      int ret = OB_SUCCESS;
      UNUSED(version);
      UNUSED(table_bf);
      TBSYS_LOG(WARN, "no get frozen bloomfilter impl now");
      return ret;
    }

    int ObUpsTableMgr :: start_transaction(const MemTableTransType type, UpsTableMgrTransHandle &handle)
    {
      int ret = OB_SUCCESS;
      TableItem *table_item = NULL;
      if (NULL == (table_item = table_mgr_.get_active_memtable()))
      {
        TBSYS_LOG(WARN, "failed to acquire active memtable");
        ret = OB_ERROR;
      }
      else
      {
        if (OB_SUCCESS == (ret = table_item->get_memtable().start_transaction(type, handle.trans_descriptor)))
        {
          handle.cur_memtable = table_item;
        }
        else
        {
          handle.cur_memtable = NULL;
          table_mgr_.revert_active_memtable(table_item);
        }
      }
      return ret;
    }

    int ObUpsTableMgr :: end_transaction(UpsTableMgrTransHandle &handle, bool rollback)
    {
      int ret = OB_SUCCESS;
      TableItem *table_item = NULL;
      if (NULL == (table_item = handle.cur_memtable))
      {
        TBSYS_LOG(WARN, "invalid param cur_memtable null pointer");
      }
      else
      {
        if (rollback)
        {
          ret = OB_ERROR;
        }
        if (OB_SUCCESS == ret)
        {
          ret = flush_commit_log_();
          FILL_TRACE_LOG("flush log ret=%d", ret);
        }
        if (OB_SUCCESS == ret)
        {
          ret = table_item->get_memtable().end_transaction(handle.trans_descriptor, rollback);
          FILL_TRACE_LOG("end transaction ret=%d", ret);
        }
        handle.cur_memtable = NULL;
        table_mgr_.revert_active_memtable(table_item);
      }
      
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "flush log or end transaction fail ret=%d, enter FATAL state", ret);
        set_state_as_fatal();
        ret = OB_RESPONSE_TIME_OUT;
      }
      return ret;
    }

    void ObUpsTableMgr :: store_memtable(const bool all)
    {
      bool need2dump = false;
      do
      {
        need2dump = table_mgr_.try_dump_memtable();
      }
      while (all && need2dump);
    }

    int ObUpsTableMgr :: freeze_memtable(const TableMgr::FreezeType freeze_type, uint64_t &frozen_version, bool &report_version_changed,
                                        const ObPacket *resp_packet)
    {
      int ret = OB_SUCCESS;
      uint64_t new_version = 0;
      uint64_t new_log_file_id = 0;
      int64_t freeze_time_stamp = 0;
      if (OB_SUCCESS == (ret = table_mgr_.try_freeze_memtable(freeze_type, new_version, frozen_version,
                                                              new_log_file_id, freeze_time_stamp, report_version_changed)))
      {
        ObUpdateServerMain *ups_main = ObUpdateServerMain::get_instance();
        ObUpsMutator ups_mutator;
        ObMutatorCellInfo mutator_cell_info;
        CurFreezeParam freeze_param;
        CommonSchemaManagerWrapper schema_manager;
        ThreadSpecificBuffer my_thread_buffer;
        ThreadSpecificBuffer::Buffer *my_buffer = my_thread_buffer.get_buffer();
        ObDataBuffer out_buff(my_buffer->current(), my_buffer->remain());

        freeze_param.param.active_version = new_version;
        freeze_param.param.frozen_version = frozen_version;
        freeze_param.param.new_log_file_id = new_log_file_id;
        freeze_param.param.time_stamp = freeze_time_stamp;
        freeze_param.param.op_flag = 0;
        if (TableMgr::MINOR_LOAD_BYPASS == freeze_type)
        {
          freeze_param.param.op_flag |= FLAG_MINOR_LOAD_BYPASS;
        }
        else if (TableMgr::MAJOR_LOAD_BYPASS == freeze_type)
        {
          freeze_param.param.op_flag |= FLAG_MAJOR_LOAD_BYPASS;
        }
        
        ups_mutator.set_freeze_memtable();
        if (NULL == ups_main)
        {
          TBSYS_LOG(ERROR, "get updateserver main null pointer");
          ret = OB_ERROR;
        }
        else if (OB_SUCCESS != (ret = freeze_param.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position())))
        {
          TBSYS_LOG(WARN, "serialize freeze_param fail ret=%d", ret);
        }
        else if (OB_SUCCESS != (ret = schema_mgr_.get_schema_mgr(schema_manager)))
        {
          TBSYS_LOG(WARN, "get schema mgr fail ret=%d", ret);
        }
        else if (OB_SUCCESS != (ret = schema_manager.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position())))
        {
          TBSYS_LOG(WARN, "serialize schema manager fail ret=%d", ret);
        }
        else
        {
          ObString str_freeze_param;
          str_freeze_param.assign_ptr(out_buff.get_data(), static_cast<int32_t>(out_buff.get_position()));
          mutator_cell_info.cell_info.value_.set_varchar(str_freeze_param);
          if (OB_SUCCESS != (ret = ups_mutator.get_mutator().add_cell(mutator_cell_info)))
          {
            TBSYS_LOG(WARN, "add cell to ups_mutator fail ret=%d", ret);
          }
          else
          {
            ret = flush_obj_to_log(OB_LOG_UPS_MUTATOR, ups_mutator);
          }
        }
        TBSYS_LOG(INFO, "write freeze_op log ret=%d new_version=%lu frozen_version=%lu new_log_file_id=%lu op_flag=%d",
                  ret, new_version, frozen_version, new_log_file_id, 0);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "enter FATAL state.");
          set_state_as_fatal();
          ret = OB_RESPONSE_TIME_OUT;
        }
        else
        {
          if (TableMgr::MINOR_LOAD_BYPASS == freeze_type
              || TableMgr::MAJOR_LOAD_BYPASS == freeze_type)
          {
            submit_load_bypass(resp_packet);
          }
        }
      }
      return ret;
    }

    void ObUpsTableMgr :: drop_memtable(const bool force)
    {
      table_mgr_.try_drop_memtable(force);
    }

    void ObUpsTableMgr :: erase_sstable(const bool force)
    {
      table_mgr_.try_erase_sstable(force);
    }

    int ObUpsTableMgr :: handle_freeze_log_(ObUpsMutator &ups_mutator, const ReplayType replay_type)
    {
      int ret = OB_SUCCESS;

      ret = ups_mutator.get_mutator().next_cell();
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "next cell for freeze ups_mutator fail ret=%d", ret);
      }
      
      ObMutatorCellInfo *mutator_ci = NULL;
      ObCellInfo *ci = NULL;
      if (OB_SUCCESS == ret)
      {
        ret = ups_mutator.get_mutator().get_cell(&mutator_ci);
        if (OB_SUCCESS != ret
            || NULL == mutator_ci)
        {
          TBSYS_LOG(WARN, "get cell from freeze ups_mutator fail ret=%d", ret);
          ret = (OB_SUCCESS == ret) ? OB_ERROR : ret;
        }
        else
        {
          ci = &(mutator_ci->cell_info);
        }
      }

      ObString str_freeze_param;
      FreezeParamHeader *header = NULL;
      if (OB_SUCCESS == ret)
      {
        ret = ci->value_.get_varchar(str_freeze_param);
        header = (FreezeParamHeader*)str_freeze_param.ptr();
        if (OB_SUCCESS != ret
            || NULL == header
            || (int64_t)sizeof(FreezeParamHeader) >= str_freeze_param.length())
        {
          TBSYS_LOG(WARN, "get freeze_param from freeze ups_mutator fail ret=%d header=%p length=%d",
                    ret, header, str_freeze_param.length());
          ret = (OB_SUCCESS == ret) ? OB_ERROR : ret;
        }
      }

      if (OB_SUCCESS == ret)
      {
        static int64_t cur_version = 0;
        if (cur_version <= header->version)
        {
          cur_version = header->version;
        }
        else
        {
          TBSYS_LOG(ERROR, "there is a old clog version=%d follow a new clog version=%ld",
                    header->version, cur_version);
          ret = OB_ERROR;
        }
      }

      ObUpdateServerMain *ups_main = NULL;
      if (OB_SUCCESS == ret)
      {
        ups_main = ObUpdateServerMain::get_instance();
        if (NULL == ups_main)
        {
          TBSYS_LOG(WARN, "get ups main fail");
          ret = OB_ERROR;
        }
      }

      if (OB_SUCCESS == ret)
      {
        bool major_version_changed = false;
        switch (header->version)
        {
          // 回放旧日志中的freeze操作
          case 1:
            {
              FreezeParamV1 *freeze_param_v1 = (FreezeParamV1*)(header->buf);
              uint64_t new_version = freeze_param_v1->active_version + 1;
              uint64_t new_log_file_id = freeze_param_v1->new_log_file_id;
              SSTableID new_sst_id;
              SSTableID frozen_sst_id;
              //new_sst_id.major_version = new_version;
              new_sst_id.id = 0;
              new_sst_id.id = (new_version << SSTableID::MINOR_VERSION_BIT);
              new_sst_id.minor_version_start = static_cast<uint16_t>(SSTableID::START_MINOR_VERSION);
              new_sst_id.minor_version_end = static_cast<uint16_t>(SSTableID::START_MINOR_VERSION);
              frozen_sst_id = new_sst_id;
              //frozen_sst_id.major_version -= 1;
              frozen_sst_id.id -= (1L << SSTableID::MINOR_VERSION_BIT);
              major_version_changed = true;
              ret = table_mgr_.replay_freeze_memtable(new_sst_id.id, frozen_sst_id.id, new_log_file_id);
              TBSYS_LOG(INFO, "replay freeze memtable using freeze_param_v1 active_version=%ld new_log_file_id=%ld op_flag=%lx ret=%d",
                        freeze_param_v1->active_version, freeze_param_v1->new_log_file_id, freeze_param_v1->op_flag, ret);
              break;
            }
          case 2:
            {
              FreezeParamV2 *freeze_param_v2 = (FreezeParamV2*)(header->buf);
              uint64_t new_version = SSTableID::trans_format_v1(freeze_param_v2->active_version);
              uint64_t frozen_version = SSTableID::trans_format_v1(freeze_param_v2->frozen_version);
              uint64_t new_log_file_id = freeze_param_v2->new_log_file_id;
              CommonSchemaManagerWrapper schema_manager;
              char *data = str_freeze_param.ptr();
              int64_t length = str_freeze_param.length();
              int64_t pos = sizeof(FreezeParamHeader) + sizeof(FreezeParamV2);
              if (OB_SUCCESS == (ret = schema_manager.deserialize(data, length, pos)))
              {
                ret = schema_mgr_.set_schema_mgr(schema_manager);
              }
              if (OB_SUCCESS == ret)
              {
                ret = table_mgr_.replay_freeze_memtable(new_version, frozen_version, new_log_file_id);
                if (OB_SUCCESS == ret)
                {
                  SSTableID sst_id_new = new_version;
                  SSTableID sst_id_frozen = frozen_version;
                  if (sst_id_new.major_version != sst_id_frozen.major_version)
                  {
                    major_version_changed = true;
                  }
                }
              }
              TBSYS_LOG(INFO, "replay freeze memtable using freeze_param_v2 active_version=%ld after_trans=%ld "
                        "new_log_file_id=%ld op_flag=%lx ret=%d",
                        freeze_param_v2->active_version, SSTableID::trans_format_v1(freeze_param_v2->active_version),
                        freeze_param_v2->new_log_file_id, freeze_param_v2->op_flag, ret);
              break;
            }
          case 3:
            {
              FreezeParamV3 *freeze_param_v3 = (FreezeParamV3*)(header->buf);
              uint64_t new_version = SSTableID::trans_format_v1(freeze_param_v3->active_version);
              uint64_t frozen_version = SSTableID::trans_format_v1(freeze_param_v3->frozen_version);
              uint64_t new_log_file_id = freeze_param_v3->new_log_file_id;
              int64_t time_stamp = freeze_param_v3->time_stamp;
              CommonSchemaManagerWrapper schema_manager;
              char *data = str_freeze_param.ptr();
              int64_t length = str_freeze_param.length();
              int64_t pos = sizeof(FreezeParamHeader) + sizeof(FreezeParamV3);
              if (OB_SUCCESS == (ret = schema_manager.deserialize(data, length, pos)))
              {
                ret = schema_mgr_.set_schema_mgr(schema_manager);
              }
              if (OB_SUCCESS == ret)
              {
                ret = table_mgr_.replay_freeze_memtable(new_version, frozen_version, new_log_file_id, time_stamp);
                if (OB_SUCCESS == ret)
                {
                  SSTableID sst_id_new = new_version;
                  SSTableID sst_id_frozen = frozen_version;
                  if (sst_id_new.major_version != sst_id_frozen.major_version)
                  {
                    major_version_changed = true;
                  }
                }
              }
              TBSYS_LOG(INFO, "replay freeze memtable using freeze_param_v3 active_version=%ld after_trans=%ld "
                        "new_log_file_id=%ld op_flag=%lx ret=%d",
                        freeze_param_v3->active_version, SSTableID::trans_format_v1(freeze_param_v3->active_version),
                        freeze_param_v3->new_log_file_id, freeze_param_v3->op_flag, ret);
              break;
            }
          case 4:
            {
              FreezeParamV4 *freeze_param_v4 = (FreezeParamV4*)(header->buf);
              uint64_t new_version = freeze_param_v4->active_version;
              uint64_t frozen_version = freeze_param_v4->frozen_version;
              uint64_t new_log_file_id = freeze_param_v4->new_log_file_id;
              int64_t time_stamp = freeze_param_v4->time_stamp;
              CommonSchemaManagerWrapper schema_manager;
              char *data = str_freeze_param.ptr();
              int64_t length = str_freeze_param.length();
              int64_t pos = sizeof(FreezeParamHeader) + sizeof(FreezeParamV4);
              if (OB_SUCCESS == (ret = schema_manager.deserialize(data, length, pos)))
              {
                ret = schema_mgr_.set_schema_mgr(schema_manager);
              }
              if (OB_SUCCESS == ret)
              {
                if (RT_LOCAL == replay_type
                    && (freeze_param_v4->op_flag & FLAG_MINOR_LOAD_BYPASS
                        || freeze_param_v4->op_flag & FLAG_MAJOR_LOAD_BYPASS))
                {
                  ret = table_mgr_.sstable_scan_finished(SSTableID::MAX_MINOR_VERSION);
                }
                else
                {
                  ret = table_mgr_.replay_freeze_memtable(new_version, frozen_version, new_log_file_id, time_stamp);
                }
                if (OB_SUCCESS == ret)
                {
                  SSTableID sst_id_new = new_version;
                  SSTableID sst_id_frozen = frozen_version;
                  if (sst_id_new.major_version != sst_id_frozen.major_version)
                  {
                    major_version_changed = true;
                  }

                  if (RT_APPLY == replay_type
                      && (freeze_param_v4->op_flag & FLAG_MINOR_LOAD_BYPASS
                          || freeze_param_v4->op_flag & FLAG_MAJOR_LOAD_BYPASS))
                  {
                    int64_t loaded_num = 0;
                    int tmp_ret = load_sstable_bypass(ups_main->get_update_server().get_sstable_mgr(), loaded_num);
                    TBSYS_LOG(INFO, "replay load bypass ret=%d loader_num=%ld", tmp_ret, loaded_num);
                  }
                }
              }
              TBSYS_LOG(INFO, "replay freeze memtable using freeze_param_v4 active_version=%ld new_log_file_id=%ld op_flag=%lx ret=%d",
                        freeze_param_v4->active_version, freeze_param_v4->new_log_file_id, freeze_param_v4->op_flag, ret);
              break;
            }
          default:
            ret = OB_ERROR;
            TBSYS_LOG(WARN, "freeze_param version error %d", header->version);
            break;
        }
        if (OB_SUCCESS == ret)
        {
          if (major_version_changed)
          {
            ups_main->get_update_server().submit_report_freeze();
          }
          ups_main->get_update_server().submit_handle_frozen();
        }
      }
      ups_mutator.get_mutator().reset_iter();
      log_table_info();
      return ret;
    }

    int ObUpsTableMgr :: replay(ObUpsMutator &ups_mutator, const ReplayType replay_type)
    {
      int ret = OB_SUCCESS;
      if (ups_mutator.is_freeze_memtable())
      {
        has_started_ = true;
        ret = handle_freeze_log_(ups_mutator, replay_type);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "failed to handle freeze log, ret=%d", ret);
        }
      }
      else if (ups_mutator.is_drop_memtable())
      {
        TBSYS_LOG(INFO, "ignore drop commit log");
      }
      else if (ups_mutator.is_first_start())
      {
        has_started_ = true;
        ObUpdateServerMain *main = ObUpdateServerMain::get_instance();
        if (NULL == main)
        {
          TBSYS_LOG(ERROR, "get updateserver main null pointer");
        }
        else
        {
          table_mgr_.sstable_scan_finished(main->get_update_server().get_param().get_minor_num_limit());
        }
        TBSYS_LOG(INFO, "handle first start flag log");
      }
      else if (ups_mutator.is_check_cur_version())
      {
        if (table_mgr_.get_cur_major_version() != ups_mutator.get_cur_major_version())
        {
          TBSYS_LOG(ERROR, "cur major version not match, table_major=%lu mutator_major=%lu table_minor=%lu mutator_minor=%lu",
                    table_mgr_.get_cur_major_version(), ups_mutator.get_cur_major_version(),
                    table_mgr_.get_cur_minor_version(), ups_mutator.get_cur_minor_version());
          ret = OB_ERROR;
        }
        else if (table_mgr_.get_cur_minor_version() != ups_mutator.get_cur_minor_version())
        {
          TBSYS_LOG(ERROR, "cur major version not match, table_major=%lu mutator_major=%lu table_minor=%lu mutator_minor=%lu",
                    table_mgr_.get_cur_major_version(), ups_mutator.get_cur_major_version(),
                    table_mgr_.get_cur_minor_version(), ups_mutator.get_cur_minor_version());
          ret = OB_ERROR;
        }
        else if (check_checksum_
                && 0 != last_bypass_checksum_
                && ups_mutator.get_last_bypass_checksum() != last_bypass_checksum_)
        {
          TBSYS_LOG(ERROR, "last bypass checksum not match, local_checksum=%lu mutator_checksum=%lu",
                    last_bypass_checksum_, ups_mutator.get_last_bypass_checksum());
          ret = OB_ERROR;
        }
        else
        {
          TBSYS_LOG(INFO, "check cur version succ, cur_major_version=%lu cur_minor_version=%lu last_bypass_checksum=%lu",
                    table_mgr_.get_cur_major_version(), table_mgr_.get_cur_minor_version(), last_bypass_checksum_);
        }
      }
      else
      {
        INC_STAT_INFO(UPS_STAT_APPLY_COUNT, 1);
        ret = set_mutator_(ups_mutator);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "failed to set mutator, ret=%d", ret);
        }
      }
      return ret;
    }

    template <typename T>
    int ObUpsTableMgr :: flush_obj_to_log(const LogCommand log_command, T &obj)
    {
      int ret = OB_SUCCESS;
      ObUpdateServerMain *ups_main = ObUpdateServerMain::get_instance();
      if (NULL == ups_main)
      {
        TBSYS_LOG(ERROR, "get updateserver main null pointer");
        ret = OB_ERROR;
      }
      else
      {
        ObUpsLogMgr& log_mgr = ups_main->get_update_server().get_log_mgr();
        int64_t serialize_size = 0;
        if (NULL == log_buffer_)
        {
          TBSYS_LOG(WARN, "log buffer malloc fail");
          ret = OB_ERROR;
        }
        else if (OB_SUCCESS != (ret = ups_serialize(obj, log_buffer_, LOG_BUFFER_SIZE, serialize_size)))
        {
          TBSYS_LOG(WARN, "obj serialilze fail log_buffer=%p log_buffer_size=%ld serialize_size=%ld ret=%d",
                    log_buffer_, LOG_BUFFER_SIZE, serialize_size, ret);
        }
        else
        {
          if (OB_SUCCESS != (ret = log_mgr.write_log(log_command, log_buffer_, serialize_size)))
          {
            TBSYS_LOG(WARN, "write log fail log_command=%d log_buffer_=%p serialize_size=%ld ret=%d",
                      log_command, log_buffer_, serialize_size, ret);
          }
          else if (OB_SUCCESS != (ret = log_mgr.flush_log()))
          {
            TBSYS_LOG(WARN, "flush log fail ret=%d", ret);
          }
          else
          {
            // do nothing
          }
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "write log fail ret=%d, enter FATAL stat", ret);
            set_state_as_fatal();
            ret = OB_RESPONSE_TIME_OUT;
          }
        }
      }
      return ret;
    }

    int ObUpsTableMgr :: write_start_log()
    {
      int ret = OB_SUCCESS;
      if (has_started_)
      {
        TBSYS_LOG(INFO, "system has started need not write start flag");
      }
      else
      {
        ObUpsMutator ups_mutator;
        ups_mutator.set_first_start();
        ret = flush_obj_to_log(OB_LOG_UPS_MUTATOR, ups_mutator);
        TBSYS_LOG(INFO, "write first start flag ret=%d", ret);
      }
      return ret;
    }

    int ObUpsTableMgr :: set_mutator_(ObUpsMutator& mutator)
    {
      int ret = OB_SUCCESS;
      TableItem *table_item = NULL;
      uint64_t trans_descriptor = 0;
      if (NULL == (table_item = table_mgr_.get_active_memtable()))
      {
        TBSYS_LOG(WARN, "failed to acquire active memtable");
        ret = OB_ERROR;
      }
      else if (OB_SUCCESS != (ret = table_item->get_memtable().start_transaction(WRITE_TRANSACTION,
              trans_descriptor, mutator.get_mutate_timestamp())))
      {
        TBSYS_LOG(WARN, "start transaction fail ret=%d", ret);
      }
      else
      {
        //FILL_TRACE_LOG("start replay one mutator");
        if (OB_SUCCESS != (ret = table_item->get_memtable().start_mutation(trans_descriptor)))
        {
          TBSYS_LOG(WARN, "start mutation fail ret=%d", ret);
        }
        else
        {
          if (OB_SUCCESS != (ret = table_item->get_memtable().set(trans_descriptor, mutator, check_checksum_)))
          {
            TBSYS_LOG(WARN, "set to memtable fail ret=%d", ret);
          }
          else
          {
            TBSYS_LOG(DEBUG, "replay mutator succ");
          }
          table_item->get_memtable().end_mutation(trans_descriptor, OB_SUCCESS != ret);
        }
        table_item->get_memtable().end_transaction(trans_descriptor, OB_SUCCESS != ret);
        //FILL_TRACE_LOG("ret=%d", ret);
        //PRINT_TRACE_LOG();
      }
      if (NULL != table_item)
      {
        table_mgr_.revert_active_memtable(table_item);
      }
      return ret;
    }

    int ObUpsTableMgr :: apply(const bool using_id, UpsTableMgrTransHandle &handle, ObUpsMutator &ups_mutator, ObScanner *scanner)
    {
      int ret = OB_SUCCESS;
      TableItem *table_item = NULL;
      if (NULL == (table_item = table_mgr_.get_active_memtable()))
      {
        TBSYS_LOG(WARN, "failed to acquire active memtable");
        ret = OB_ERROR;
      }
      else
      {
        MemTable *p_active_memtable = &(table_item->get_memtable());
        if (!using_id
            && OB_SUCCESS != trans_name2id_(ups_mutator.get_mutator()))
        {
          TBSYS_LOG(WARN, "cellinfo do not pass valid check or trans name to id fail");
          ret = OB_SCHEMA_ERROR;
        }
        else if (!using_id
                && OB_SUCCESS != trans_cond_name2id_(ups_mutator.get_mutator()))
        {
          TBSYS_LOG(WARN, "condinfo do not pass valid check or trans name to id fail");
          ret = OB_SCHEMA_ERROR;
        }
        else if (OB_SUCCESS != (ret = check_condition_(ups_mutator.get_mutator())))
        {
          if (OB_COND_CHECK_FAIL != ret)
          {
            TBSYS_LOG(WARN, "failed to check update condition ret=%d", ret);
          }
        }
        else if (OB_SUCCESS != p_active_memtable->start_mutation(handle.trans_descriptor))
        {
          TBSYS_LOG(WARN, "start mutation fail trans_descriptor=%lu", handle.trans_descriptor);
          ret = OB_ERROR;
        }
        else
        {
          bool check_checksum = false;
          scanner->reset();
          scanner->set_id_name_type(using_id ? ObScanner::ID : ObScanner::NAME);
          FILL_TRACE_LOG("prepare mutator");
          if (OB_SUCCESS != (ret = p_active_memtable->set(handle.trans_descriptor, ups_mutator, check_checksum, this, scanner)))
          {
            TBSYS_LOG(WARN, "set to memtable fail ret=%d", ret);
          }
          else
          {
#ifndef _WITH_RELEASE
            ups_mutator.get_mutator().get_prefetch_data().reset();
#endif
            log_scanner(scanner);
            FILL_TRACE_LOG("scanner info %s", print_scanner_info(scanner));
            // 注意可能返回OB_EAGAIN
            ret = fill_commit_log_(ups_mutator);
          }
          if (OB_SUCCESS == ret)
          {
            bool rollback = false;
            ret = p_active_memtable->end_mutation(handle.trans_descriptor, rollback);
          }
          else
          {
            bool rollback = true;
            p_active_memtable->end_mutation(handle.trans_descriptor, rollback);
          }
        }
        table_mgr_.revert_active_memtable(table_item);
      }
      log_memtable_memory_info();
      return ret;
    }

    void ObUpsTableMgr :: log_memtable_memory_info()
    {
      static int64_t counter = 0;
      static const int64_t mod = 100000;
      if (0 == (counter++ % mod))
      {
        log_table_info();
      }
    }

    void ObUpsTableMgr :: get_memtable_memory_info(TableMemInfo &mem_info)
    {
      TableItem *table_item = table_mgr_.get_active_memtable();
      if (NULL != table_item)
      {
        mem_info.memtable_used = table_item->get_memtable().used() + table_mgr_.get_frozen_memused();
        mem_info.memtable_total = table_item->get_memtable().total() + table_mgr_.get_frozen_memtotal();
        MemTableAttr memtable_attr;
        table_item->get_memtable().get_attr(memtable_attr);
        mem_info.memtable_limit = memtable_attr.total_memlimit;
        table_mgr_.revert_active_memtable(table_item);
      }
    }

    void ObUpsTableMgr::set_memtable_attr(const MemTableAttr &memtable_attr)
    {
      table_mgr_.set_memtable_attr(memtable_attr);
    }

    int ObUpsTableMgr::get_memtable_attr(MemTableAttr &memtable_attr)
    {
      return table_mgr_.get_memtable_attr(memtable_attr);
    }

    void ObUpsTableMgr :: update_memtable_stat_info()
    {
      TableItem *table_item = table_mgr_.get_active_memtable();
      if (NULL != table_item)
      {
        MemTableAttr memtable_attr;
        table_mgr_.get_memtable_attr(memtable_attr);
        int64_t active_limit = memtable_attr.total_memlimit;
        int64_t frozen_limit = memtable_attr.total_memlimit;
        int64_t active_total = table_item->get_memtable().total();
        int64_t frozen_total = table_mgr_.get_frozen_memtotal();
        int64_t active_used = table_item->get_memtable().used();
        int64_t frozen_used = table_mgr_.get_frozen_memused();
        int64_t active_row_count = table_item->get_memtable().size();
        int64_t frozen_row_count = table_mgr_.get_frozen_rowcount();

        SET_STAT_INFO(UPS_STAT_MEMTABLE_TOTAL, active_total + frozen_total);
        SET_STAT_INFO(UPS_STAT_MEMTABLE_USED, active_used + frozen_used);
        SET_STAT_INFO(UPS_STAT_TOTAL_LINE, active_row_count + frozen_row_count);

        SET_STAT_INFO(UPS_STAT_ACTIVE_MEMTABLE_LIMIT, active_limit);
        SET_STAT_INFO(UPS_STAT_ACTICE_MEMTABLE_TOTAL, active_total);
        SET_STAT_INFO(UPS_STAT_ACTIVE_MEMTABLE_USED, active_used);
        SET_STAT_INFO(UPS_STAT_ACTIVE_TOTAL_LINE, active_row_count);

        SET_STAT_INFO(UPS_STAT_FROZEN_MEMTABLE_LIMIT, frozen_limit);
        SET_STAT_INFO(UPS_STAT_FROZEN_MEMTABLE_TOTAL, frozen_total);
        SET_STAT_INFO(UPS_STAT_FROZEN_MEMTABLE_USED, frozen_used);
        SET_STAT_INFO(UPS_STAT_FROZEN_TOTAL_LINE, frozen_row_count);

        table_mgr_.revert_active_memtable(table_item);
      }
    }

    int ObUpsTableMgr :: set_schemas(const CommonSchemaManagerWrapper &schema_manager)
    {
      int ret = OB_SUCCESS;

      ret = schema_mgr_.set_schema_mgr(schema_manager);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "set_schemas error, ret=%d schema_version=%ld", ret, schema_manager.get_version());
      }

      return ret;
    }

    int ObUpsTableMgr :: switch_schemas(const CommonSchemaManagerWrapper &schema_manager)
    {
      int ret = OB_SUCCESS;

      int64_t serialize_size = 0;

      ret = schema_mgr_.set_schema_mgr(schema_manager);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "set_schemas error, ret=%d", ret);
      }
      else
      {
        ObUpdateServerMain *main = ObUpdateServerMain::get_instance();
        if (NULL == main)
        {
          TBSYS_LOG(ERROR, "get updateserver main null pointer");
          ret = OB_ERROR;
        }
        else
        {
          if (NULL == log_buffer_)
          {
            TBSYS_LOG(WARN, "log buffer malloc fail");
            ret = OB_ERROR;
          }
        }

        if (OB_SUCCESS == ret)
        {
          ret = schema_manager.serialize(log_buffer_, LOG_BUFFER_SIZE, serialize_size);
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "ups_mutator serialilze fail log_buffer=%p log_buffer_size=%ld serialize_size=%ld ret=%d",
                      log_buffer_, LOG_BUFFER_SIZE, serialize_size, ret);
          }
          else
          {
            ObUpsLogMgr& log_mgr = main->get_update_server().get_log_mgr();
            TBSYS_LOG(INFO, "ups table magr switch schemas.");
            ret = log_mgr.write_and_flush_log(OB_UPS_SWITCH_SCHEMA, log_buffer_, serialize_size);
            if (OB_SUCCESS != ret)
            {
              TBSYS_LOG(WARN, "write log fail log_buffer_=%p serialize_size=%ld ret=%d", log_buffer_, serialize_size, ret);
            }
          }
        }
      }

      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "schema_version=%ld", schema_manager.get_version());
      }

      return ret;
    }

    int ObUpsTableMgr :: create_index()
    {
      int ret = OB_SUCCESS;
      // need not create index
      TBSYS_LOG(WARN, "no create index impl now");
      return ret;
    }

    int ObUpsTableMgr :: get(const ObGetParam &get_param, ObScanner &scanner, const int64_t start_time, const int64_t timeout)
    {
      int ret = OB_SUCCESS;
      TableList *table_list = GET_TSI_MULT(TableList, TSI_UPS_TABLE_LIST_1);
      uint64_t max_valid_version = 0;
      bool is_final_minor = false;
      if (NULL == table_list)
      {
        TBSYS_LOG(WARN, "get tsi table_list fail");
        ret = OB_ERROR;
      }
      else if (OB_SUCCESS != (ret = table_mgr_.acquire_table(get_param.get_version_range(), max_valid_version, *table_list, is_final_minor))
          || 0 == table_list->size())
      {
        TBSYS_LOG(WARN, "acquire table fail version_range=%s", range2str(get_param.get_version_range()));
        ret = (OB_SUCCESS == ret) ? OB_INVALID_START_VERSION : ret;
      }
      else
      {
        FILL_TRACE_LOG("version=%s table_num=%ld", range2str(get_param.get_version_range()), table_list->size());
        TableList::iterator iter;
        int64_t index = 0;
        SSTableID sst_id;
        int64_t trans_start_counter = 0;
        for (iter = table_list->begin(); iter != table_list->end(); iter++, index++)
        {
          ITableEntity *table_entity = *iter;
          ITableUtils *table_utils = NULL;
          if (NULL == table_entity)
          {
            TBSYS_LOG(WARN, "invalid table_entity version_range=%s", range2str(get_param.get_version_range()));
            ret = OB_ERROR;
            break;
          }
          if (NULL == (table_utils = table_entity->get_tsi_tableutils(index)))
          {
            TBSYS_LOG(WARN, "get tsi tableutils fail index=%ld", index);
            ret = OB_TOO_MANY_SSTABLE;
            break;
          }
          if (OB_SUCCESS != (ret = table_entity->start_transaction(table_utils->get_trans_descriptor())))
          {
            TBSYS_LOG(WARN, "start transaction fail ret=%d", ret);
            break;
          }
          trans_start_counter++;
          sst_id = (NULL == table_entity) ? 0 : table_entity->get_table_item().get_sstable_id();
          FILL_TRACE_LOG("get table %s ret=%d", sst_id.log_str(), ret);
        }

        if (OB_SUCCESS == ret)
        {
          ret = get_(*table_list, get_param, scanner, start_time, timeout);
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "failed to get_ ret=%d", ret);
          }
          else
          {
            scanner.set_data_version(ObVersion::get_version(SSTableID::get_major_version(max_valid_version),
                                                            SSTableID::get_minor_version_end(max_valid_version),
                                                            is_final_minor));
          }
        }

        for (iter = table_list->begin(), index = 0; iter != table_list->end() && index < trans_start_counter; iter++, index++)
        {
          ITableEntity *table_entity = *iter;
          ITableUtils *table_utils = NULL;
          if (NULL != table_entity
              && NULL != (table_utils = table_entity->get_tsi_tableutils(index)))
          {
            table_entity->end_transaction(table_utils->get_trans_descriptor());
            table_utils->reset();
          }
        }
        table_mgr_.revert_table(*table_list);
      }
      return ret;
    }

    int ObUpsTableMgr :: scan(const ObScanParam &scan_param, ObScanner &scanner, const int64_t start_time, const int64_t timeout)
    {
      int ret = OB_SUCCESS;
      TableList *table_list = GET_TSI_MULT(TableList, TSI_UPS_TABLE_LIST_1);
      uint64_t max_valid_version = 0;
      const ObRange *scan_range = NULL;
      bool is_final_minor = false;
      if (NULL == table_list)
      {
        TBSYS_LOG(WARN, "get tsi table_list fail");
        ret = OB_ERROR;
      }
      else if (NULL == (scan_range = scan_param.get_range()))
      {
        TBSYS_LOG(WARN, "invalid scan range");
        ret = OB_ERROR;
      }
      else if (OB_SUCCESS != (ret = table_mgr_.acquire_table(scan_param.get_version_range(), max_valid_version, *table_list, is_final_minor))
              || 0 == table_list->size())
      {
        TBSYS_LOG(WARN, "acquire table fail version_range=%s", range2str(scan_param.get_version_range()));
        ret = (OB_SUCCESS == ret) ? OB_INVALID_START_VERSION : ret;
      }
      else
      {
        ColumnFilter cf;
        ColumnFilter *pcf = ColumnFilter::build_columnfilter(scan_param, &cf);
        FILL_TRACE_LOG("%s columns=%s direction=%d version=%s table_num=%ld",
                      scan_range2str(*scan_range),
                      (NULL == pcf) ? "nil" : pcf->log_str(),
                      scan_param.get_scan_direction(),
                      range2str(scan_param.get_version_range()),
                      table_list->size());

        do
        {
          ITableEntity::Guard guard(table_mgr_.get_resource_pool());
          ObMerger merger;
          ObIterator *ret_iter = &merger;
          merger.set_asc(scan_param.get_scan_direction() == ObScanParam::FORWARD);

          TableList::iterator iter;
          int64_t index = 0;
          SSTableID sst_id;
          int64_t trans_start_counter = 0;
          ITableIterator *prev_table_iter = NULL;
          for (iter = table_list->begin(); iter != table_list->end(); iter++, index++)
          {
            ITableEntity *table_entity = *iter;
            ITableUtils *table_utils = NULL;
            ITableIterator *table_iter = NULL;
            if (NULL == table_entity)
            {
              TBSYS_LOG(WARN, "invalid table_entity version_range=%s", range2str(scan_param.get_version_range()));
              ret = OB_ERROR;
              break;
            }
            if (NULL == (table_utils = table_entity->get_tsi_tableutils(index)))
            {
              TBSYS_LOG(WARN, "get tsi tableutils fail index=%ld", index);
              ret = OB_TOO_MANY_SSTABLE;
              break;
            }
            if (ITableEntity::SSTABLE == table_entity->get_table_type())
            {
              table_iter = prev_table_iter;
            }
            if (NULL == table_iter
                && NULL == (table_iter = table_entity->alloc_iterator(table_mgr_.get_resource_pool(), guard)))
            {
              TBSYS_LOG(WARN, "alloc table iterator fai index=%ld", index);
              ret = OB_MEM_OVERFLOW;
              break;
            }
            prev_table_iter = NULL;
            if (OB_SUCCESS != (ret = table_entity->start_transaction(table_utils->get_trans_descriptor())))
            {
              TBSYS_LOG(WARN, "start transaction fail ret=%d scan_range=%s", ret, scan_range2str(*scan_range));
              break;
            }
            trans_start_counter++;
            if (OB_SUCCESS != (ret = table_entity->scan(table_utils->get_trans_descriptor(), scan_param, table_iter)))
            {
              TBSYS_LOG(WARN, "table entity scan fail ret=%d scan_range=%s", ret, scan_range2str(*scan_range));
              break;
            }
            sst_id = (NULL == table_entity) ? 0 : table_entity->get_table_item().get_sstable_id();
            FILL_TRACE_LOG("scan table %s ret=%d iter=%p", sst_id.log_str(), ret, table_iter);
            if (ITableEntity::SSTABLE == table_entity->get_table_type())
            {
              if (OB_ITER_END == table_iter->next_cell())
              {
                table_iter->reset();
                prev_table_iter = table_iter;
                continue;
              }
            }
            if (1 == table_list->size())
            {
              ret_iter = table_iter;
              break;
            }
            if (OB_SUCCESS != (ret = merger.add_iterator(table_iter)))
            {
              TBSYS_LOG(WARN, "add iterator to merger fail ret=%d", ret);
              break;
            }
          }

          if (OB_SUCCESS == ret)
          {
            int64_t row_count = 0;
            ret = add_to_scanner_(*ret_iter, scanner, row_count, start_time, timeout, scan_param.get_scan_size());
            FILL_TRACE_LOG("add to scanner scanner_size=%ld row_count=%ld ret=%d", scanner.get_size(), row_count, ret);
            if (OB_SIZE_OVERFLOW == ret)
            {
              if (row_count > 0)
              {
                scanner.set_is_req_fullfilled(false, row_count);
                ret = OB_SUCCESS;
              }
              else
              {
                TBSYS_LOG(WARN, "memory is not enough to add even one row");
                ret = OB_ERROR;
              }
            }
            else if (OB_SUCCESS != ret)
            {
              TBSYS_LOG(WARN, "failed to add data from ups_merger to scanner, ret=%d", ret);
            }
            else
            {
              scanner.set_is_req_fullfilled(true, row_count);
            }
            if (OB_SUCCESS == ret)
            {
              scanner.set_data_version(ObVersion::get_version(SSTableID::get_major_version(max_valid_version),
                                                              SSTableID::get_minor_version_end(max_valid_version),
                                                              is_final_minor));

              ObRange range;
              range.table_id_ = scan_param.get_table_id();
              range.border_flag_.set_min_value();
              range.border_flag_.set_max_value();
              ret = scanner.set_range(range);
            }
          }

          for (iter = table_list->begin(), index = 0; iter != table_list->end() && index < trans_start_counter; iter++, index++)
          {
            ITableEntity *table_entity = *iter;
            ITableUtils *table_utils = NULL;
            if (NULL != table_entity
                && NULL != (table_utils = table_entity->get_tsi_tableutils(index)))
            {
              table_entity->end_transaction(table_utils->get_trans_descriptor());
              table_utils->reset();
            }
          }
        }
        while (false);
        table_mgr_.revert_table(*table_list);
      }
      return ret;
    }
    
    int ObUpsTableMgr :: rpc_get(ObGetParam &get_param, ObScanner &scanner, const int64_t timeout)
    {
      int ret = OB_SUCCESS;
      const CommonSchemaManager* common_schema_mgr = NULL;
      ObClientWrapper* client_wrapper = NULL;
      UpsSchemaMgrGuard guard;
      ObVersionRange version_range;

      client_wrapper = get_client_wrapper_();
      common_schema_mgr = schema_mgr_.get_schema_mgr(guard);
      version_range.border_flag_.set_min_value();
      version_range.border_flag_.set_max_value();
      get_param.set_version_range(version_range);

      UNUSED(timeout);
      if (NULL == common_schema_mgr
          || NULL == client_wrapper)
      {
        TBSYS_LOG(WARN, "invalid schema_mgr=%p or client_wrapper=%p", common_schema_mgr, client_wrapper);
        ret = OB_ERROR;
      }
      else
      {
        client_wrapper->clear();
        ret = client_wrapper->get(get_param, *(common_schema_mgr));
        if (OB_SUCCESS == ret)
        {
          ObCellInfo *cell_info = NULL;
          while (OB_SUCCESS == ret
                && OB_SUCCESS == (ret = client_wrapper->next_cell()))
          {
            if (OB_SUCCESS == (ret = client_wrapper->get_cell(&cell_info)))
            {
              if (NULL != cell_info)
              {
                ret = scanner.add_cell(*cell_info);
                TBSYS_LOG(DEBUG, "get from client wrapper %s", print_cellinfo(cell_info));
              }
              else
              {
                ret = OB_ERROR;
              }
            }
          }
          ret = (OB_ITER_END == ret) ? OB_SUCCESS : ret;
        }
      }
      return ret;
    }

    int ObUpsTableMgr :: get_(TableList &table_list, const ObGetParam& get_param, ObScanner& scanner,
                              const int64_t start_time, const int64_t timeout)
    {
      int err = OB_SUCCESS;

      int64_t cell_size = get_param.get_cell_size();
      const ObCellInfo* cell = NULL;

      if (cell_size <= 0)
      {
        TBSYS_LOG(WARN, "invalid param, cell_size=%ld", cell_size);
        err = OB_INVALID_ARGUMENT;
      }
      else if (NULL == (cell = get_param[0]))
      {
        TBSYS_LOG(WARN, "invalid param first cellinfo");
        err = OB_ERROR;
      }
      else
      {
        int64_t last_cell_idx = 0;
        ObString last_row_key = cell->row_key_;
        uint64_t last_table_id = cell->table_id_;
        int64_t cell_idx = 1;

        while (OB_SUCCESS == err && cell_idx < cell_size)
        {
          if (NULL == (cell = get_param[cell_idx]))
          {
            TBSYS_LOG(WARN, "cellinfo null pointer idx=%ld", cell_idx);
            err = OB_ERROR;
          }
          else if (cell->row_key_ != last_row_key ||
                  cell->table_id_ != last_table_id)
          {
            err = get_row_(table_list, last_cell_idx, cell_idx - 1, get_param, scanner, start_time, timeout);
            if (OB_SIZE_OVERFLOW == err)
            {
              TBSYS_LOG(WARN, "allocate memory failed, first_idx=%ld, last_idx=%ld",
                  last_cell_idx, cell_idx - 1);
            }
            else if (OB_SUCCESS != err)
            {
              TBSYS_LOG(WARN, "failed to get_row_, first_idx=%ld, last_idx=%ld, err=%d",
                  last_cell_idx, cell_idx - 1, err);
            }
            else
            {
              if ((start_time + timeout) < g_conf.cur_time)
              {
                if (last_cell_idx > 0)
                {
                  // at least get one row
                  TBSYS_LOG(WARN, "get or scan too long time, start_time=%ld timeout=%ld timeu=%ld row_count=%ld",
                      start_time, timeout, g_conf.cur_time - start_time, last_cell_idx);
                  err = OB_FORCE_TIME_OUT;
                }
                else
                {
                  TBSYS_LOG(ERROR, "can't get any row, start_time=%ld timeout=%ld timeu=%ld",
                      start_time, timeout, g_conf.cur_time - start_time);
                  err = OB_RESPONSE_TIME_OUT;
                }
              }

              if (OB_SUCCESS == err)
              {
                last_cell_idx = cell_idx;
                last_row_key = cell->row_key_;
                last_table_id = cell->table_id_;
              }
            }
          }

          if (OB_SUCCESS == err)
          {
            ++cell_idx;
          }
        }

        if (OB_SUCCESS == err)
        {
          err = get_row_(table_list, last_cell_idx, cell_idx - 1, get_param, scanner, start_time, timeout);
          if (OB_SIZE_OVERFLOW == err)
          {
            TBSYS_LOG(WARN, "allocate memory failed, first_idx=%ld, last_idx=%ld",
                last_cell_idx, cell_idx - 1);
          }
          else if (OB_SUCCESS != err)
          {
            TBSYS_LOG(WARN, "failed to get_row_, first_idx=%ld, last_idx=%ld, err=%d",
                last_cell_idx, cell_idx - 1, err);
          }
        }

        FILL_TRACE_LOG("table_list_size=%ld get_param_cell_size=%ld get_param_row_size=%ld last_cell_idx=%ld cell_idx=%ld scanner_size=%ld ret=%d",
                      table_list.size(), get_param.get_cell_size(), get_param.get_row_size(), last_cell_idx, cell_idx, scanner.get_size(), err);
        if (OB_SIZE_OVERFLOW == err)
        {
          // wrap error
          scanner.set_is_req_fullfilled(false, last_cell_idx);
          err = OB_SUCCESS;
        }
        else if (OB_FORCE_TIME_OUT == err)
        {
          scanner.set_is_req_fullfilled(false, cell_idx);
          err = OB_SUCCESS;
        }
        else if (OB_SUCCESS == err)
        {
          scanner.set_is_req_fullfilled(true, cell_idx);
        }
      }

      return err;
    }

    int ObUpsTableMgr :: get_row_(TableList &table_list, const int64_t first_cell_idx, const int64_t last_cell_idx,
                                  const ObGetParam& get_param, ObScanner& scanner,
                                  const int64_t start_time, const int64_t timeout)
    {
      int ret = OB_SUCCESS;
      int64_t cell_size = get_param.get_cell_size();
      const ObCellInfo* cell = NULL;
      ColumnFilter *column_filter = NULL;
      ObRowCompaction *row_compaction = GET_TSI_MULT(ObRowCompaction, TSI_UPS_ROW_COMPACTION_1);

      if (NULL == row_compaction)
      {
        TBSYS_LOG(WARN, "get tsi row_compaction fail");
        ret = OB_ERROR;
      }
      else if (first_cell_idx > last_cell_idx)
      {
        TBSYS_LOG(WARN, "invalid param, first_cell_idx=%ld, last_cell_idx=%ld",
            first_cell_idx, last_cell_idx);
        ret = OB_INVALID_ARGUMENT;
      }
      else if (cell_size <= last_cell_idx)
      {
        TBSYS_LOG(WARN, "invalid status, cell_size=%ld, last_cell_idx=%ld",
            cell_size, last_cell_idx);
        ret = OB_ERROR;
      }
      else if (NULL == (cell = get_param[first_cell_idx]))
      {
        TBSYS_LOG(WARN, "cellinfo null pointer idx=%ld", first_cell_idx);
        ret = OB_ERROR;
      }
      else if (NULL == (column_filter = ITableEntity::get_tsi_columnfilter()))
      {
        TBSYS_LOG(WARN, "get tsi columnfilter fail");
        ret = OB_ERROR;
      }
      else
      {
        ITableEntity::Guard guard(table_mgr_.get_resource_pool());
        ObMerger merger;
        ObIterator *ret_iter = &merger;

        uint64_t table_id = cell->table_id_;
        ObString row_key = cell->row_key_;
        column_filter->clear();
        for (int64_t i = first_cell_idx; i <= last_cell_idx; ++i)
        {
          if (NULL != get_param[i]) // double check
          {
            column_filter->add_column(get_param[i]->column_id_);
          }
        }

        TableList::iterator iter;
        int64_t index = 0;
        ITableIterator *prev_table_iter = NULL;
        for (iter = table_list.begin(), index = 0; iter != table_list.end(); iter++, index++)
        {
          ITableEntity *table_entity = *iter;
          ITableUtils *table_utils = NULL;
          ITableIterator *table_iter = NULL;
          if (NULL == table_entity)
          {
            TBSYS_LOG(WARN, "invalid table_entity version_range=%s", range2str(get_param.get_version_range()));
            ret = OB_ERROR;
            break;
          }
          if (NULL == (table_utils = table_entity->get_tsi_tableutils(index)))
          {
            TBSYS_LOG(WARN, "get tsi tableutils fail index=%ld", index);
            ret = OB_ERROR;
            break;
          }
          if (ITableEntity::SSTABLE == table_entity->get_table_type())
          {
            table_iter = prev_table_iter;
          }
          if (NULL == table_iter
              && NULL == (table_iter = table_entity->alloc_iterator(table_mgr_.get_resource_pool(), guard)))
          {
            TBSYS_LOG(WARN, "alloc table iterator fai index=%ld", index);
            ret = OB_MEM_OVERFLOW;
            break;
          }
          prev_table_iter = NULL;
          if (OB_SUCCESS != (ret = table_entity->get(table_utils->get_trans_descriptor(),
              table_id, row_key, column_filter, table_iter)))
          {
            TBSYS_LOG(WARN, "table entity get fail ret=%d table_id=%lu row_key=[%s] columns=[%s]",
                      ret, table_id, print_string(row_key), column_filter->log_str());
            break;
          }
          TBSYS_LOG(DEBUG, "get row row_key=[%s] row_key_ptr=%p columns=[%s] iter=%p",
                    print_string(row_key), row_key.ptr(), column_filter->log_str(), table_iter);
          if (ITableEntity::SSTABLE == table_entity->get_table_type())
          {
            if (OB_ITER_END == table_iter->next_cell())
            {
              table_iter->reset();
              prev_table_iter = table_iter;
              continue;
            }
          }
          if (1 == table_list.size())
          {
            ret_iter = table_iter;
            break;
          }
          if (OB_SUCCESS != (ret = merger.add_iterator(table_iter)))
          {
            TBSYS_LOG(WARN, "add iterator to merger fail ret=%d", ret);
            break;
          }
        }

        if (OB_SUCCESS == ret)
        {
          int64_t row_count = 0;
          ret = add_to_scanner_(*ret_iter, scanner, row_count, start_time, timeout);
          if (OB_SIZE_OVERFLOW == ret)
          {
            // return ret
          }
          else if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "failed to add data from merger to scanner, ret=%d", ret);
          }
          else
          {
            // TODO (rizhao) add successful cell num to scanner
          }
        }
      }

      TBSYS_LOG(DEBUG, "[op=GET_ROW] [first_cell_idx=%ld] [last_cell_idx=%ld] [ret=%d]", first_cell_idx, last_cell_idx, ret);
      return ret;
    }

    int ObUpsTableMgr :: add_to_scanner_(ObIterator& ups_merger, ObScanner& scanner, int64_t& row_count,
                                        const int64_t start_time, const int64_t timeout, const int64_t result_limit_size/* = UINT64_SIZE*/)
    {
      int err = OB_SUCCESS;

      ObCellInfo* cell = NULL;
      bool is_row_changed = false;
      row_count = 0;
      while (OB_SUCCESS == err && (OB_SUCCESS == (err = ups_merger.next_cell())))
      {
        err = ups_merger.get_cell(&cell, &is_row_changed);
        if (OB_SUCCESS != err || NULL == cell)
        {
          TBSYS_LOG(WARN, "failed to get cell, err=%d", err);
          err = OB_ERROR;
        }
        else
        {
          bool is_timeout = false;
          TBSYS_LOG(DEBUG, "from merger %s is_row_changed=%s", print_cellinfo(cell), STR_BOOL(is_row_changed));
          if (is_row_changed)
          {
            ++row_count;
          }
          if (is_row_changed
              && 1 < row_count
              && (start_time + timeout) < g_conf.cur_time)
          {
            TBSYS_LOG(WARN, "get or scan too long time, start_time=%ld timeout=%ld timeu=%ld row_count=%ld",
                      start_time, timeout, g_conf.cur_time - start_time, row_count - 1);
            err = OB_SIZE_OVERFLOW;
            is_timeout = true;
          }
          else
          {
            err = scanner.add_cell(*cell, false, is_row_changed);
            if (1 < row_count
                && 0 < result_limit_size
                && result_limit_size <= scanner.get_size())
            {
              err = OB_SIZE_OVERFLOW;
            }
          }
          if (OB_SUCCESS != err)
          {
            row_count-=1;
            if (1 > row_count)
            {
              TBSYS_LOG(WARN, "invalid row_count=%ld", row_count);
            }
          }
          if (OB_SIZE_OVERFLOW == err
              && !is_timeout)
          {
            TBSYS_LOG(DEBUG, "scanner memory is not enough, rollback last row");
            // rollback last row
            if (OB_SUCCESS != scanner.rollback())
            {
              TBSYS_LOG(WARN, "failed to rollback");
              err = OB_ERROR;
            }
          }
          else if (OB_SUCCESS != err)
          {
            TBSYS_LOG(WARN, "failed to add cell, err=%d", err);
          }
        }
      }

      if (OB_ITER_END == err)
      {
        // wrap error
        err = OB_SUCCESS;
      }

      return err;
    }

    int ObUpsTableMgr :: clear_active_memtable()
    {
      return table_mgr_.clear_active_memtable();
    }

    int ObUpsTableMgr::check_permission_(ObMutator &mutator, const IToken &token)
    {
      int ret = OB_SUCCESS;
      CellInfoProcessor ci_proc;
      ObUpdateServerMain *main = ObUpdateServerMain::get_instance();
      if (NULL == main)
      {
        TBSYS_LOG(ERROR, "get updateserver main null pointer");
        ret = OB_ERROR;
      }
      while (OB_SUCCESS == ret
            && OB_SUCCESS == (ret = mutator.next_cell()))
      {
        ObMutatorCellInfo *mutator_ci = NULL;
        ObCellInfo *ci = NULL;
        if (OB_SUCCESS == mutator.get_cell(&mutator_ci)
            && NULL != mutator_ci)
        {
          const CommonTableSchema *table_schema = NULL;
          UpsSchemaMgr::SchemaHandle schema_handle;
          ci = &(mutator_ci->cell_info);
          if (!ci_proc.analyse_syntax(*mutator_ci))
          {
            ret = OB_SCHEMA_ERROR;
          }
          else if (ci_proc.need_skip())
          {
            continue;
          }
          else if (OB_SUCCESS != (ret = schema_mgr_.get_schema_handle(schema_handle)))
          {
            TBSYS_LOG(WARN, "get_schema_handle fail ret=%d", ret);
          }
          else
          {
            if (NULL == (table_schema = schema_mgr_.get_table_schema(schema_handle, ci->table_name_)))
            {
              TBSYS_LOG(WARN, "get schema fail table_name=%.*s table_name_length=%d",
                        ci->table_name_.length(), ci->table_name_.ptr(), ci->table_name_.length());
              ret = OB_SCHEMA_ERROR;
            }
            else if (OB_SUCCESS != (ret = main->get_update_server().get_perm_table().check_table_writeable(token, table_schema->get_table_id())))
            {
              ObString username;
              token.get_username(username);
              TBSYS_LOG(WARN, "check write permission fail ret=%d table_id=%lu table_name=%.*s user_name=%.*s",
                        ret, table_schema->get_table_id(), ci->table_name_.length(), ci->table_name_.ptr(),
                        username.length(), username.ptr());
            }
            else
            {
              // pass check
            }
            schema_mgr_.revert_schema_handle(schema_handle);
          }
        }
        else
        {
          ret = OB_ERROR;
        }
      }
      ret = (OB_ITER_END == ret) ? OB_SUCCESS : ret;
      mutator.reset_iter();
      return ret;
    }

    int ObUpsTableMgr::trans_name2id_(ObMutator &mutator)
    {
      int ret = OB_SUCCESS;
      CellInfoProcessor ci_proc;
      while (OB_SUCCESS == ret
            && OB_SUCCESS == (ret = mutator.next_cell()))
      {
        ObMutatorCellInfo *mutator_ci = NULL;
        ObCellInfo *ci = NULL;
        if (OB_SUCCESS == mutator.get_cell(&mutator_ci)
            && NULL != mutator_ci)
        {
          const CommonTableSchema *table_schema = NULL;
          const CommonColumnSchema *column_schema = NULL;
          UpsSchemaMgr::SchemaHandle schema_handle;
          ci = &(mutator_ci->cell_info);
          if (!ci_proc.analyse_syntax(*mutator_ci))
          {
            ret = OB_SCHEMA_ERROR;
          }
          else if (ci_proc.need_skip())
          {
            continue;
          }
          else if (!ci_proc.cellinfo_check(*ci))
          {
            ret = OB_SCHEMA_ERROR;
          }
          else if (OB_SUCCESS != (ret = schema_mgr_.get_schema_handle(schema_handle)))
          {
            TBSYS_LOG(WARN, "get_schema_handle fail ret=%d", ret);
          }
          else
          {
            // 调用schema转化name2id
            if (NULL == (table_schema = schema_mgr_.get_table_schema(schema_handle, ci->table_name_)))
            {
              TBSYS_LOG(WARN, "get schema fail table_name=%.*s table_name_length=%d",
                        ci->table_name_.length(), ci->table_name_.ptr(), ci->table_name_.length());
              ret = OB_SCHEMA_ERROR;
            }
            else if (!CellInfoProcessor::cellinfo_check(*ci, *table_schema))
            {
              ret = OB_SCHEMA_ERROR;
            }
            else if (common::ObActionFlag::OP_DEL_ROW == ci_proc.get_op_type())
            {
              // do not trans column name
              ci->table_id_ = table_schema->get_table_id();
              ci->column_id_ = OB_INVALID_ID;
              ci->table_name_.assign_ptr(NULL, 0);
              ci->column_name_.assign_ptr(NULL, 0);
            }
            else if (NULL == (column_schema = schema_mgr_.get_column_schema(schema_handle, ci->table_name_, ci->column_name_)))
            {
              TBSYS_LOG(WARN, "get column schema fail table_name=%.*s table_id=%lu column_name=%.*s column_name_length=%d",
                        ci->table_name_.length(), ci->table_name_.ptr(), table_schema->get_table_id(),
                        ci->column_name_.length(), ci->column_name_.ptr(), ci->column_name_.length());
              ret = OB_SCHEMA_ERROR;
            }
            // 数据类型与合法性检查
            else if (!CellInfoProcessor::cellinfo_check(*ci, *column_schema))
            {
              ret = OB_SCHEMA_ERROR;
            }
            else
            {
              ci->table_id_ = table_schema->get_table_id();
              ci->column_id_ = column_schema->get_id();
              ci->table_name_.assign_ptr(NULL, 0);
              ci->column_name_.assign_ptr(NULL, 0);
            }
            schema_mgr_.revert_schema_handle(schema_handle);
          }
        }
        else
        {
          ret = OB_ERROR;
        }
      }
      ret = (OB_ITER_END == ret) ? OB_SUCCESS : ret;
      mutator.reset_iter();

      return ret;
    };

    int ObUpsTableMgr::trans_cond_name2id_(ObMutator& mutator)
    {
      int ret = OB_SUCCESS;

      ObUpdateCondition& update_condition = mutator.get_update_condition();
      int64_t count = update_condition.get_count();
      for (int64_t i = 0; OB_SUCCESS == ret && i < count; ++i)
      {
        const CommonTableSchema *table_schema = NULL;
        const CommonColumnSchema *column_schema = NULL;
        UpsSchemaMgr::SchemaHandle schema_handle;

        ObCondInfo* cond_info = update_condition[i];
        if (NULL == cond_info)
        {
          TBSYS_LOG(WARN, "the %ld-th cond info is NULL", i);
          ret = OB_ERROR;
        }
        else if (OB_SUCCESS != (ret = schema_mgr_.get_schema_handle(schema_handle)))
        {
          TBSYS_LOG(WARN, "get_schema_handle fail ret=%d", ret);
        }
        else
        {
          // 调用schema转化name2id
          ObCellInfo &ci = cond_info->get_cell();
          if (NULL == (table_schema = schema_mgr_.get_table_schema(schema_handle, ci.table_name_)))
          {
            TBSYS_LOG(WARN, "get schema fail table_name=%.*s table_name_length=%d",
                ci.table_name_.length(), ci.table_name_.ptr(), ci.table_name_.length());
            ret = OB_SCHEMA_ERROR;
          }
          else
          {
            if (ci.value_.get_type() == ObExtendType
                && (ObActionFlag::OP_ROW_DOES_NOT_EXIST == ci.value_.get_ext()
                  || ObActionFlag::OP_ROW_EXIST == ci.value_.get_ext()))
            {
              ci.table_id_ = table_schema->get_table_id();
              ci.table_name_.assign_ptr(NULL, 0);

              // row exist or row not exist
              ci.column_id_ = OB_MAX_COLUMN_ID;
              ci.column_name_.assign_ptr(NULL, 0);
            }
            else if (NULL == (column_schema = schema_mgr_.get_column_schema(schema_handle,
                    ci.table_name_, ci.column_name_)))
            {
              TBSYS_LOG(WARN, "get column schema fail table_name=%.*s table_id=%lu column_name=%.*s column_name_length=%d",
                  ci.table_name_.length(), ci.table_name_.ptr(), table_schema->get_table_id(),
                  ci.column_name_.length(), ci.column_name_.ptr(), ci.column_name_.length());
              ret = OB_SCHEMA_ERROR;
            }
            else
            {
              ci.table_id_ = table_schema->get_table_id();
              ci.table_name_.assign_ptr(NULL, 0);

              ci.column_id_ = column_schema->get_id();
              ci.column_name_.assign_ptr(NULL, 0);
            }
          }

          schema_mgr_.revert_schema_handle(schema_handle);
        }
      }

      return ret;
    }

    int ObUpsTableMgr::fill_commit_log_(ObUpsMutator &ups_mutator)
    {
      int ret = OB_SUCCESS;
      int64_t serialize_size = 0;
      ObUpdateServerMain *main = ObUpdateServerMain::get_instance();
      if (NULL == main)
      {
        TBSYS_LOG(ERROR, "get updateserver main null pointer");
        ret = OB_ERROR;
      }
      else if (NULL == log_buffer_)
      {
        TBSYS_LOG(WARN, "log buffer malloc fail");
        ret = OB_ERROR;
      }
      else if (OB_SUCCESS != (ret = ups_mutator.serialize(log_buffer_, LOG_BUFFER_SIZE, serialize_size)))
      {
        TBSYS_LOG(WARN, "ups_mutator serialilze fail log_buffer=%p log_buffer_size=%ld serialize_size=%ld ret=%d",
                  log_buffer_, LOG_BUFFER_SIZE, serialize_size, ret);
      }
      else
      {
        FILL_TRACE_LOG("ups_mutator serialize");
        ObUpsLogMgr& log_mgr = main->get_update_server().get_log_mgr();
        if (OB_BUF_NOT_ENOUGH == (ret = log_mgr.write_log(OB_LOG_UPS_MUTATOR, log_buffer_, serialize_size)))
        {
          TBSYS_LOG(INFO, "log buffer full");
          ret = OB_EAGAIN;
        }
        else if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "write log fail ret=%d, enter FATAL state", ret);
          set_state_as_fatal();
          ret = OB_RESPONSE_TIME_OUT;
        }
      }
      FILL_TRACE_LOG("size=%ld ret=%d", serialize_size, ret);
      return ret;
    };

    int ObUpsTableMgr::flush_commit_log_()
    {
      int ret = OB_SUCCESS;
      ObUpdateServerMain *main = ObUpdateServerMain::get_instance();
      if (NULL == main)
      {
        TBSYS_LOG(ERROR, "get updateserver main null pointer");
        ret = OB_ERROR;
      }
      else
      {
        ObUpsLogMgr& log_mgr = main->get_update_server().get_log_mgr();
        ret = log_mgr.flush_log();
      }
      // 只要不能刷新日志都要优雅退出
      //if (OB_SUCCESS != ret)
      //{
      //  TBSYS_LOG(WARN, "flush log fail ret=%d, will kill self", ret);
      //  kill(getpid(), SIGTERM);
      //}
      FILL_TRACE_LOG("ret=%d", ret);
      return ret;
    };

    ObClientWrapper* ObUpsTableMgr::get_client_wrapper_()
    {
      int ret = OB_SUCCESS;
      ObClientWrapper* client_wrapper = NULL;

      ObClientWrapperTSI* client_wrapper_tsi = GET_TSI_MULT(ObClientWrapperTSI, TSI_UPS_CLIENT_WRAPPER_TSI_1);
      if (NULL == client_wrapper_tsi)
      {
        TBSYS_LOG(WARN, "failed to get tsi instance");
        ret = OB_MEM_OVERFLOW;
      }
      else
      {
        ObUpdateServer& update_server = ObUpdateServerMain::get_instance()->get_update_server();

        ret = client_wrapper_tsi->init(RPC_RETRY_TIMES, RPC_TIMEOUT,
            update_server.get_root_server(), update_server.get_self(),
            update_server.get_table_mgr(), update_server.get_ups_cache(),
            &update_server.get_merger_stub(), update_server.get_merger_schema(), &update_server.get_tablet_cache());
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "failed to init client wrapper tsi, ret=%d", ret);
        }
        else
        {
          client_wrapper = client_wrapper_tsi->get_client_wrapper();
        }
      }

      return client_wrapper;
    }

    int ObUpsTableMgr::get_mutate_result(ObCellInfo &mutate_cell, ObIterator &active_data, ObScanner &scanner)
    {
      int ret = OB_SUCCESS;

      const CommonSchemaManager* common_schema_mgr = NULL;
      UpsSchemaMgrGuard guard;
      common_schema_mgr = schema_mgr_.get_schema_mgr(guard);

      ObClientWrapper* client_wrapper = get_client_wrapper_();
      ObGetParam *get_param_ptr = GET_TSI_MULT(ObGetParam, TSI_UPS_GET_PARAM_1);
      ObCellInfo *static_cell = NULL;

      if (NULL == client_wrapper)
      {
        TBSYS_LOG(WARN, "invalid status, not init");
        ret = OB_NOT_INIT;
      }

      if (OB_SUCCESS == ret)
      {
        if (NULL == get_param_ptr)
        {
          TBSYS_LOG(ERROR, "memory overflow");
          ret = OB_MEM_OVERFLOW;
        }
      }

      if (OB_SUCCESS == ret)
      {
        ObCellInfo cell_info;
        cell_info.table_id_ = mutate_cell.table_id_;
        cell_info.row_key_ = mutate_cell.row_key_;
        cell_info.column_id_ = mutate_cell.column_id_;

        uint64_t last_frozen_version = 0;
        get_param_ptr->reset();
        ret = get_param_ptr->add_cell(cell_info);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "failed to add_cell, ret=%d", ret);
        }
        else
        {
          ret = get_last_frozen_memtable_version(last_frozen_version);
          if (OB_SUCCESS != ret || last_frozen_version == 0)
          {
            TBSYS_LOG(WARN, "error occurs, ret=%d, last_frozen_version=%lu",
                ret, last_frozen_version);
            ret = OB_ERROR;
          }
        }


        if (OB_SUCCESS == ret)
        {
          // set version range
          ObVersionRange version_range;
          //version_range.end_version_ = last_frozen_version;
          //version_range.border_flag_.set_inclusive_end();
          version_range.border_flag_.set_min_value();
          version_range.border_flag_.set_max_value();
          get_param_ptr->set_version_range(version_range);

          client_wrapper->clear();
          ret = client_wrapper->get(*get_param_ptr, *(common_schema_mgr));
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "failed to get, ret=%d", ret);
          }
          else
          {
            ret = client_wrapper->next_cell();
            if (OB_SUCCESS != ret)
            {
              TBSYS_LOG(WARN, "failed to call next_cell, ret=%d", ret);
            }
          }

          if (OB_SUCCESS == ret)
          {
            ret = client_wrapper->get_cell(&static_cell);
            if (NULL == static_cell || OB_SUCCESS != ret)
            {
              TBSYS_LOG(WARN, "failed to call get_cell, ret=%d", ret);
              ret = OB_ERROR;
            }
          }
        }
      }

      ObCellInfo result_cell;
      if (OB_SUCCESS == ret)
      {
        result_cell.row_key_ = mutate_cell.row_key_;
        result_cell.value_ = static_cell->value_;
        if (ObScanner::ID == scanner.get_id_name_type())
        {
          result_cell.table_id_ = mutate_cell.table_id_;
          result_cell.column_id_ = mutate_cell.column_id_;
        }
        else
        {
          const CommonTableSchema *table_schema = common_schema_mgr->get_table_schema(mutate_cell.table_id_);
          const CommonColumnSchema *column_schema = common_schema_mgr->get_column_schema(mutate_cell.table_id_, mutate_cell.column_id_);
          if (NULL == table_schema
              || NULL == column_schema)
          {
            ret = OB_SCHEMA_ERROR;
            TBSYS_LOG(WARN, "get table or column schema fail table_id=%lu column_id=%lu",
                      mutate_cell.table_id_, mutate_cell.column_id_);
          }
          else
          {
            result_cell.table_name_.assign_ptr(const_cast<char*>(table_schema->get_table_name()), static_cast<int32_t>(strlen(table_schema->get_table_name())));
            result_cell.column_name_.assign_ptr(const_cast<char*>(column_schema->get_name()), static_cast<int32_t>(strlen(column_schema->get_name())));
          }
        }
      }

      if (OB_SUCCESS == ret)
      {
        ObCellInfo *ci = NULL;
        while (OB_SUCCESS == ret
              && OB_SUCCESS == (ret = active_data.next_cell()))
        {
          if (OB_SUCCESS == (ret = active_data.get_cell(&ci)))
          {
            if (NULL == ci)
            {
              ret = OB_ERROR;
            }
            else
            {
              ret = result_cell.value_.apply(ci->value_);
            }
          }
        }
        ret = (OB_ITER_END == ret) ? OB_SUCCESS : ret;
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "iterate and apply active data fail ret=%d", ret);
        }
      }

      if (OB_SUCCESS == ret)
      {
        ret = scanner.add_cell(result_cell);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "add cell to scanner fail ret=%d", ret);
        }
      }
      FILL_TRACE_LOG("ret=%d", ret); 

      return ret;
    }

    int ObUpsTableMgr::load_sstable_bypass(SSTableMgr &sstable_mgr, int64_t &loaded_num)
    {
      int ret = OB_SUCCESS;
      table_mgr_.lock_freeze();
      uint64_t major_version = table_mgr_.get_cur_major_version();
      uint64_t minor_version_start = table_mgr_.get_cur_minor_version();
      uint64_t minor_version_end = SSTableID::MAX_MINOR_VERSION - 1;
      uint64_t clog_id = table_mgr_.get_last_clog_id();
      uint64_t checksum = 0;
      ret = sstable_mgr.load_sstable_bypass(major_version, minor_version_start, minor_version_end, clog_id,
                                            table_mgr_.get_table_list2add(), checksum);
      if (OB_SUCCESS == ret)
      {
        int64_t tmp = table_mgr_.get_table_list2add().size();
        ret = table_mgr_.add_table_list();
        if (OB_SUCCESS == ret)
        {
          loaded_num = tmp;
          last_bypass_checksum_ = checksum;
        }
      }
      table_mgr_.unlock_freeze();
      return ret;
    }

    int ObUpsTableMgr::check_cur_version()
    {
      int ret = OB_SUCCESS;
      ObUpsMutator ups_mutator;
      ups_mutator.set_check_cur_version();
      ups_mutator.set_cur_major_version(table_mgr_.get_cur_major_version());
      ups_mutator.set_cur_minor_version(table_mgr_.get_cur_minor_version());
      ups_mutator.set_last_bypass_checksum(last_bypass_checksum_);
      if (OB_SUCCESS != (ret = fill_commit_log_(ups_mutator)))
      {
        TBSYS_LOG(WARN, "fill commit log fail ret=%d", ret);
      }
      else if (OB_SUCCESS != (ret = flush_commit_log_()))
      {
        TBSYS_LOG(WARN, "flush commit log fail ret=%d", ret);
      }
      else
      {
        TBSYS_LOG(INFO, "write check cur version log succ, cur_major_version=%lu cur_minor_version=%lu",
                  ups_mutator.get_cur_major_version(), ups_mutator.get_cur_minor_version());
      }
      return ret;
    }

    void ObUpsTableMgr::update_merged_version(ObUpsRpcStub &rpc_stub, const ObServer &root_server, const int64_t timeout_us)
    {
      int64_t oldest_memtable_size = 0;
      uint64_t oldest_memtable_version = SSTableID::MAX_MAJOR_VERSION;
      table_mgr_.get_oldest_memtable_size(oldest_memtable_size, oldest_memtable_version);
      uint64_t oldest_major_version = SSTableID::MAX_MAJOR_VERSION;
      table_mgr_.get_oldest_major_version(oldest_major_version);
      uint64_t last_frozen_version = 0;
      get_last_frozen_memtable_version(last_frozen_version);

      uint64_t merged_version = table_mgr_.get_merged_version();
      if (merged_version < oldest_major_version)
      {
        merged_version = oldest_major_version;
      }

      while (merged_version <= last_frozen_version)
      {
        bool merged = false;
        int tmp_ret = rpc_stub.check_table_merged(root_server, merged_version, merged, timeout_us);
        if (OB_SUCCESS != tmp_ret)
        {
          TBSYS_LOG(WARN, "rpc check_table_merged fail ret=%d", tmp_ret);
          break;
        }
        if (!merged)
        {
          TBSYS_LOG(INFO, "major_version=%lu have not merged done", merged_version);
          break;
        }
        else
        {
          table_mgr_.set_merged_version(merged_version, tbsys::CTimeUtil::getTime());
          TBSYS_LOG(INFO, "major_version=%lu have merged done, done_time=%ld", merged_version, table_mgr_.get_merged_timestamp());
          merged_version += 1;
        }
      }

      if (oldest_memtable_version <= table_mgr_.get_merged_version())
      {
        submit_immediately_drop();
      }
    }

    int ObUpsTableMgr::check_condition_(ObMutator& mutator)
    {
      int ret = OB_SUCCESS;

      //CommonSchemaManagerWrapper schema_wrapper;
      //schema_mgr_.get_schema_mgr(schema_wrapper);
      const CommonSchemaManager* common_schema_mgr = NULL;
      UpsSchemaMgrGuard guard;
      common_schema_mgr = schema_mgr_.get_schema_mgr(guard);

      ObUpdateCondition& condition = mutator.get_update_condition();
      int64_t count = condition.get_count();
      ObClientWrapper* client_wrapper = get_client_wrapper_();
      if (NULL == client_wrapper)
      {
        TBSYS_LOG(WARN, "invalid status, not init");
        ret = OB_NOT_INIT;
      }

      for (int64_t i = 0; OB_SUCCESS == ret && i < count; ++i)
      {
        const ObCondInfo* cond_info = condition[i];
        if (NULL == cond_info)
        {
          TBSYS_LOG(WARN, "the %ld-th cond info is NULL", i);
          ret = OB_ERROR;
        }
        else
        {
          uint64_t active_memtable_version = 0;
          ObGetParam *get_param_ptr = GET_TSI_MULT(ObGetParam, TSI_UPS_GET_PARAM_1);
          if (NULL == get_param_ptr)
          {
            TBSYS_LOG(ERROR, "memory overflow");
            ret = OB_MEM_OVERFLOW;
          }
          else
          {
            get_param_ptr->reset();
            ret = get_param_ptr->add_cell(cond_info->get_cell());
            if (OB_SUCCESS != ret)
            {
              TBSYS_LOG(WARN, "failed to add_cell, ret=%d", ret);
            }
            else
            {
              ret = get_active_memtable_version(active_memtable_version);
              if (OB_SUCCESS != ret || active_memtable_version == 0)
              {
                TBSYS_LOG(WARN, "error occurs, ret=%d, active_memtable_version=%lu",
                    ret, active_memtable_version);
                ret = OB_ERROR;
              }
            }
          }

          if (OB_SUCCESS == ret)
          {
            // set version range
            ObVersionRange version_range;
            //version_range.end_version_ = active_memtable_version;
            //version_range.border_flag_.set_inclusive_end();
            version_range.border_flag_.set_min_value();
            version_range.border_flag_.set_max_value();
            get_param_ptr->set_version_range(version_range);

            client_wrapper->clear();
            //ret = client_wrapper->get(*get_param_ptr, *(schema_wrapper.get_impl()));
            ret = client_wrapper->get(*get_param_ptr, *(common_schema_mgr));
            if (OB_SUCCESS != ret)
            {
              TBSYS_LOG(WARN, "failed to get, ret=%d", ret);
            }
            else
            {
              ret = client_wrapper->next_cell();
              if (OB_SUCCESS != ret)
              {
                TBSYS_LOG(WARN, "failed to call next_cell, ret=%d", ret);
              }
            }

            if (OB_SUCCESS == ret)
            {
              ObCellInfo* cell_info = NULL;
              ret = client_wrapper->get_cell(&cell_info);
              if (NULL == cell_info || OB_SUCCESS != ret)
              {
                TBSYS_LOG(WARN, "failed to call get_cell, ret=%d", ret);
                ret = OB_ERROR;
              }
              else
              {
                ret = check_cond_info_(cond_info->get_operator(), cell_info->value_, cond_info->get_cell().value_);
                if (OB_SUCCESS != ret)
                {
                  TBSYS_LOG(INFO, "check cond failed, op_type=%d, expected=%s, real=%s, row_key=[%s]",
                            cond_info->get_operator(), print_obj(cond_info->get_cell().value_),
                            print_obj(cell_info->value_), print_string(cell_info->row_key_));
                }
              }
            }
          }
        }
      }

      return ret;
    }

    int ObUpsTableMgr::prepare_ups_cache_(const uint64_t table_id, const uint64_t column_id, const ObString &row_key,
                                          const CommonSchemaManager* common_schema_mgr, ObClientWrapper* client_wrapper)
    {
      int ret = OB_SUCCESS;
      
      ObCellInfo cell_info;
      cell_info.table_id_ = table_id;
      cell_info.row_key_ = row_key;
      cell_info.column_id_ = column_id;

      uint64_t last_frozen_version = 0;
      ObGetParam *get_param_ptr = GET_TSI_MULT(ObGetParam, TSI_UPS_GET_PARAM_1);
      if (NULL == get_param_ptr)
      {
        TBSYS_LOG(ERROR, "memory overflow");
        ret = OB_MEM_OVERFLOW;
      }
      else
      {
        get_param_ptr->reset();
        ret = get_param_ptr->add_cell(cell_info);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "failed to add_cell, ret=%d", ret);
        }
        else
        {
          ret = get_last_frozen_memtable_version(last_frozen_version);
          if (OB_SUCCESS != ret || last_frozen_version == 0)
          {
            TBSYS_LOG(WARN, "error occurs, ret=%d, last_frozen_version=%lu",
                ret, last_frozen_version);
            ret = OB_ERROR;
          }
        }
      }

      if (OB_SUCCESS == ret)
      {
        // set version range
        ObVersionRange version_range;
        //version_range.end_version_ = last_frozen_version;
        //version_range.border_flag_.set_inclusive_end();
        version_range.border_flag_.set_min_value();
        version_range.border_flag_.set_max_value();
        get_param_ptr->set_version_range(version_range);

        client_wrapper->clear();
        ret = client_wrapper->get(*get_param_ptr, *(common_schema_mgr));
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "failed to get, ret=%d", ret);
        }
        else
        {
          ret = client_wrapper->next_cell();
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "failed to call next_cell, ret=%d", ret);
          }
        }

        if (OB_SUCCESS == ret)
        {
          ObCellInfo* cell_info = NULL;
          ret = client_wrapper->get_cell(&cell_info);
          if (NULL == cell_info || OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "failed to call get_cell, ret=%d", ret);
            ret = OB_ERROR;
          }
          else
          {
            ret = add_ups_cache_(last_frozen_version, *cell_info);
            if (OB_SUCCESS != ret)
            {
              TBSYS_LOG(WARN, "failed to add cell info to cache, ret=%d", ret);
            }
          }
        }
      }
      
      return ret;
    }

    int ObUpsTableMgr::pre_process(const bool using_id, ObMutator& mutator, const IToken *token)
    {
      int ret = OB_SUCCESS;

      const CommonSchemaManager* common_schema_mgr = NULL;
      UpsSchemaMgrGuard guard;
      common_schema_mgr = schema_mgr_.get_schema_mgr(guard);

      ObUpdateCondition& condition = mutator.get_update_condition();
      int64_t count = condition.get_count();
      ObClientWrapper* client_wrapper = get_client_wrapper_();

      if (NULL == client_wrapper)
      {
        TBSYS_LOG(WARN, "invalid status, not init");
        ret = OB_NOT_INIT;
      }

      if (OB_SUCCESS == ret
          && NULL != token)
      {
        ret = check_permission_(mutator, *token);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "fail to check permission ret=%d", ret);
        }
      }

      if (OB_SUCCESS == ret
          && !using_id)
      {
        ret = trans_cond_name2id_(mutator);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "failed to trans_cond_name2id, ret=%d", ret);
        }
      }

      if (OB_SUCCESS == ret
          && !using_id)
      {
        ret = trans_name2id_(mutator);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "failed to trans_name2id, ret=%d", ret);
        }
      }

      if (OB_SUCCESS == ret)
      {
        ObScanner &scanner = mutator.get_prefetch_data().get();
        ObCellInfo *ci = NULL;
        while (OB_SUCCESS == ret
              && OB_SUCCESS == (ret = scanner.next_cell()))
        {
          if (OB_SUCCESS == (ret = scanner.get_cell(&ci)))
          {
            if (NULL == ci)
            {
              ret = OB_ERROR;
            }
            else
            {
              ret = add_ups_cache_(scanner.get_data_version(), *ci);
              TBSYS_LOG(DEBUG, "ms_data %s", print_cellinfo(ci));
            }
          }
        }
        ret = (OB_ITER_END == ret) ? OB_SUCCESS : ret;
        TBSYS_LOG(DEBUG, "ms_data version=%ld", scanner.get_data_version());
      }

      for (int64_t i = 0; OB_SUCCESS == ret && i < count; ++i)
      {
        const ObCondInfo* cond_info = condition[i];
        if (NULL == cond_info)
        {
          TBSYS_LOG(WARN, "the %ld-th cond info is NULL", i);
          ret = OB_ERROR;
        }
        else
        {
          ret = prepare_ups_cache_(cond_info->get_cell().table_id_, cond_info->get_cell().column_id_, cond_info->get_cell().row_key_,
                                  common_schema_mgr, client_wrapper);
        }
      }

      if (OB_SUCCESS == ret)
      {
        ObMutatorCellInfo *mci = NULL;
        while (OB_SUCCESS == ret
              && OB_SUCCESS == (ret = mutator.next_cell()))
        {
          if (OB_SUCCESS == (ret = mutator.get_cell(&mci)))
          {
            if (NULL == mci) 
            {
              ret = OB_ERROR;
            }
            else if (common::ObExtendType != mci->cell_info.value_.get_type()
                    && (ObActionFlag::OP_RETURN_UPDATE_RESULT & mci->op_type.get_ext()))
            {
              const CommonTableSchema *table_schema = NULL;
              const CommonColumnSchema *column_schema = NULL;
              table_schema = common_schema_mgr->get_table_schema(mci->cell_info.table_id_);
              column_schema = common_schema_mgr->get_column_schema(mci->cell_info.table_id_, mci->cell_info.column_id_);
              if (NULL == table_schema
                  || NULL == column_schema)
              {
                ret = OB_ERROR;
              }
              else
              {
                ret = prepare_ups_cache_(table_schema->get_table_id(), column_schema->get_id(), mci->cell_info.row_key_,
                                        common_schema_mgr, client_wrapper);
              }
            }
          }
        }
        ret = (OB_ITER_END == ret) ? OB_SUCCESS : ret;
        mutator.reset_iter();
      }

      return ret;
    }

    int ObUpsTableMgr::add_ups_cache_(const int64_t last_frozen_version, const ObCellInfo& cell_info)
    {
      int ret = OB_SUCCESS;

      uint64_t column_id = OB_INVALID_ID;
      ObUpsCacheValue value;
      value.version = last_frozen_version;

      if (OB_MAX_COLUMN_ID == cell_info.column_id_
          || OB_INVALID_ID == cell_info.column_id_)
      {
        if (ObNullType == cell_info.value_.get_type())
        {
          value.value.set_ext(ObActionFlag::OP_ROW_EXIST);
          column_id = OB_MAX_COLUMN_ID;
        }
        else if (ObActionFlag::OP_ROW_DOES_NOT_EXIST == cell_info.value_.get_ext())
        {
          value.value.set_ext(ObActionFlag::OP_ROW_DOES_NOT_EXIST);
          column_id = OB_MAX_COLUMN_ID;
        }
        else
        {
          TBSYS_LOG(WARN, "invalid cell value, ext_val=%ld", cell_info.value_.get_ext());
          ret = OB_ERROR;
        }
      }
      else
      {
        if (ObMinType < value.value.get_type()
            && ObMaxType > value.value.get_type())
        {
          value.value = cell_info.value_;
          column_id = cell_info.column_id_;
        }
        else
        {
          TBSYS_LOG(WARN, "invalid value type, type=%d", value.value.get_type());
          ret = OB_ERROR;
        }
      }

      if (OB_SUCCESS == ret)
      {
        ret = ups_cache_.put(cell_info.table_id_, cell_info.row_key_, column_id, value);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "failed to put cell to ups cache, ret=%d", ret);
        }
      }

      return ret;
    }

    int ObUpsTableMgr::check_cond_info_(const int64_t op_type, const ObObj& cell_value,
        const ObObj& cond_value)
    {
      int ret = OB_SUCCESS;
      bool is_valid = false;

      if (ObExtendType == cond_value.get_type())
      {
        if (ObActionFlag::OP_ROW_EXIST == cond_value.get_ext())
        {
          is_valid = (ObNullType == cell_value.get_type());
        }
        else if (ObActionFlag::OP_ROW_DOES_NOT_EXIST == cond_value.get_ext())
        {
          is_valid = (ObExtendType == cell_value.get_type()
              && ObActionFlag::OP_ROW_DOES_NOT_EXIST == cell_value.get_ext());
        }
        else
        {
          TBSYS_LOG(WARN, "invalid cond, ext_val=%ld", cond_value.get_ext());
          ret = OB_ERROR;
        }
      }
      else
      {
        switch (op_type)
        {
          case EQ:
            is_valid = (cell_value == cond_value);
            break;
          case LT:
            is_valid = (cell_value < cond_value);
            break;
          case LE:
            is_valid = (cell_value <= cond_value);
            break;
          case GT:
            is_valid = (cell_value > cond_value);
            break;
          case GE:
            is_valid = (cell_value >= cond_value);
            break;
          case NE:
            is_valid = (cell_value != cond_value);
            break;

          default:
            TBSYS_LOG(WARN, "invalid op_type, op_type=%ld", op_type);
            ret = OB_ERROR;
            break;
        }
      }

      if (OB_SUCCESS == ret && !is_valid)
      {
        ret = OB_COND_CHECK_FAIL;
      }

      return ret;
    }

    void ObUpsTableMgr::dump_memtable(const ObString &dump_dir)
    {
      table_mgr_.dump_memtable2text(dump_dir);
    }
    
    void ObUpsTableMgr::dump_schemas()
    {
      schema_mgr_.dump2text();
    }

    int ObUpsTableMgr::sstable_scan_finished(const int64_t minor_num_limit)
    {
      return table_mgr_.sstable_scan_finished(minor_num_limit);
    }

    int ObUpsTableMgr::check_sstable_id()
    {
      return table_mgr_.check_sstable_id();
    }

    void ObUpsTableMgr::log_table_info()
    {
      TBSYS_LOG(INFO, "replay checksum flag=%s", STR_BOOL(check_checksum_));
      table_mgr_.log_table_info();
      ObUpdateServerMain *ups_main = ObUpdateServerMain::get_instance();
      if (NULL != ups_main)
      {
        ups_main->get_update_server().get_sstable_mgr().log_sstable_info();
      }
    }

    void ObUpsTableMgr::set_warm_up_percent(const int64_t warm_up_percent)
    {
      table_mgr_.set_warm_up_percent(warm_up_percent);
    }

    int ObUpsTableMgr::get_schema(const uint64_t major_version, CommonSchemaManagerWrapper &sm)
    {
      return table_mgr_.get_schema(major_version, sm);
    }

    int ObUpsTableMgr::get_sstable_range_list(const uint64_t major_version, const uint64_t table_id, TabletInfoList &ti_list)
    {
      return table_mgr_.get_sstable_range_list(major_version, table_id, ti_list);
    }

    bool get_key_prefix(const TEKey &te_key, TEKey &prefix_key)
    {
      bool bret = false;
      TEKey tmp_key = te_key;
      ObUpdateServerMain *main = ObUpdateServerMain::get_instance();
      if (NULL == main)
      {
        TBSYS_LOG(ERROR, "get updateserver main null pointer");
      }
      else
      {
        ObUpsTableMgr &tm = main->get_update_server().get_table_mgr();
        const CommonTableSchema *table_schema = NULL;
        UpsSchemaMgr::SchemaHandle schema_handle;
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = tm.schema_mgr_.get_schema_handle(schema_handle)))
        {
          TBSYS_LOG(WARN, "get schema handle fail ret=%d", tmp_ret);
        }
        else
        {
          if (NULL == (table_schema = tm.schema_mgr_.get_table_schema(schema_handle, te_key.table_id)))
          {
            TBSYS_LOG(ERROR, "get schema fail table_id=%lu", te_key.table_id);
          }
          else
          {
            int32_t split_pos = table_schema->get_split_pos();
            if (split_pos > tmp_key.row_key.length())
            {
              TBSYS_LOG(ERROR, "row key cannot be splited length=%d split_pos=%d",
                        tmp_key.row_key.length(), split_pos);
            }
            else
            {
              tmp_key.row_key.assign_ptr(tmp_key.row_key.ptr(), split_pos);
              prefix_key = tmp_key;
              bret = true;
            }
          }
          tm.schema_mgr_.revert_schema_handle(schema_handle);
        }
      }
      return bret;
    }
  }
}
