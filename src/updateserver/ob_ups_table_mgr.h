/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: ups_tablet_mgr.h,v 0.1 2010/09/14 10:11:15 chuanhui Exp $
 *
 * Authors:
 *   chuanhui <rizhao.ych@taobao.com>
 *     - some work details if you want
 *
 */
#ifndef __OCEANBASE_CHUNKSERVER_UPS_TABLET_MGR_H__
#define __OCEANBASE_CHUNKSERVER_UPS_TABLET_MGR_H__

#include "tbrwlock.h"
#include "common/ob_define.h"
#include "common/ob_mutator.h"
#include "common/ob_read_common_data.h"
#include "common/bloom_filter.h"
#include "common/ob_schema.h"
#include "common/ob_merger.h"
#include "ob_ups_mutator.h"
#include "ob_memtable.h"
#include "ob_schema_mgrv2.h"
#include "ob_ups_utils.h"
#include "ob_table_mgr.h"
#include "ob_ups_cache.h"
#include "common/ob_meta_cache.h"
#include "common/ob_token.h"
#include "ob_trans_mgr.h"
#include "ob_ups_log_utils.h"

namespace oceanbase
{
  namespace updateserver
  {
    // UpsTableMgr manages the active and frozen memtable of UpdateServer.
    // It also acts as an entry point for apply/replay/get/scan operation.
    class ObClientWrapper;
    class ObUpsRpcStub;
    struct UpsTableMgrTransHandle
    {
      MemTableTransDescriptor trans_descriptor;
      TableItem *cur_memtable;
      UpsTableMgrTransHandle() : trans_descriptor(0), cur_memtable(NULL)
      {
      };
    };
    const static int64_t MT_REPLAY_OP_NULL = 0x0;
    const static int64_t MT_REPLAY_OP_CREATE_INDEX = 0x0000000000000001;
    const static int64_t FLAG_MAJOR_LOAD_BYPASS = 0x0000000000000001;
    const static int64_t FLAG_MINOR_LOAD_BYPASS = 0x0000000000000002;
    class ObUpsTableMgr : public common::IRpcStub
    {
      friend class TestUpsTableMgrHelper;
      friend bool get_key_prefix(const TEKey &te_key, TEKey &prefix_key);
      const static int64_t LOG_BUFFER_SIZE = 1024 * 1024 * 2;
      struct FreezeParamHeader
      {
        int32_t version;
        int32_t reserve1;
        int64_t reserve2;
        int64_t reserve3;
        char buf[];
      };
      struct FreezeParamV1
      {
        static const int64_t Version = 1;
        uint64_t active_version;     // freeze之前的activce memtable的version
        uint64_t new_log_file_id;
        int64_t op_flag;
      };
      struct FreezeParamV2
      {
        static const int64_t Version = 2;
        uint64_t active_version;     // freeze之的activce memtable的version 包括major和minor
        uint64_t frozen_version;
        uint64_t new_log_file_id;
        int64_t op_flag;
      };
      struct FreezeParamV3
      {
        static const int64_t Version = 3;
        uint64_t active_version;     // freeze之的activce memtable的version 包括major和minor
        uint64_t frozen_version;
        uint64_t new_log_file_id;
        int64_t time_stamp;
        int64_t op_flag;
      };
      struct FreezeParamV4
      {
        static const int64_t Version = 4;
        uint64_t active_version;     // freeze之的activce memtable的version 包括major和minor
        uint64_t frozen_version;     // 新版本的active_version和frozen_version 16:16:32个字节表示
        uint64_t new_log_file_id;
        int64_t time_stamp;
        int64_t op_flag;
      };
      struct CurFreezeParam
      {
        typedef FreezeParamV4 FreezeParam;
        FreezeParamHeader header;
        FreezeParam param;
        CurFreezeParam()
        {
          memset(this, 0, sizeof(CurFreezeParam));
          header.version = FreezeParam::Version;
        };
        int serialize(char* buf, const int64_t buf_len, int64_t& pos) const
        {
          int ret = common::OB_SUCCESS;
          if ((pos + (int64_t)sizeof(*this)) > buf_len)
          {
            ret = common::OB_ERROR;
          }
          else
          {
            memcpy(buf + pos, this, sizeof(*this));
            pos += sizeof(*this);
          }
          return ret;
        };
        int deserialize(const char* buf, const int64_t buf_len, int64_t& pos)
        {
          int ret = common::OB_SUCCESS;
          if ((pos + (int64_t)sizeof(*this)) > buf_len)
          {
            ret = common::OB_ERROR;
          }
          else
          {
            memcpy(this, buf + pos, sizeof(*this));
            pos += sizeof(*this);
          }
          return ret;
        };
      };
      struct DropParamHeader
      {
        int32_t version;
        int32_t reserve1;
        int64_t reserve2;
        int64_t reserve3;
        char buf[];
      };
      struct DropParamV1
      {
        int64_t frozen_version;     // 要drop掉的frozen memtable的version
      };
      struct CurDropParam
      {
        DropParamHeader header;
        DropParamV1 param;
        CurDropParam()
        {
          memset(this, 0, sizeof(CurDropParam));
          header.version = 1;
        };
      };

      public:
        ObUpsTableMgr(ObUpsCache& ups_cache);
        ~ObUpsTableMgr();
        int init();
        int reg_table_mgr(SSTableMgr &sstable_mgr);
        inline TableMgr* get_table_mgr()
        {
          return &table_mgr_;
        }

      public:
        int start_transaction(const MemTableTransType type,
                              UpsTableMgrTransHandle &handle);
        int end_transaction(UpsTableMgrTransHandle &handle, bool rollback);
        int pre_process(const bool using_id, common::ObMutator& ups_mutator, const common::IToken *token);
        int apply(const bool using_id, UpsTableMgrTransHandle &handle, ObUpsMutator &ups_mutator, common::ObScanner *scanner);
        int replay(ObUpsMutator& ups_mutator, const ReplayType replay_type);
        int set_schemas(const CommonSchemaManagerWrapper &schema_manager);
        int switch_schemas(const CommonSchemaManagerWrapper &schema_manager);
        int get_active_memtable_version(uint64_t &version);
        int get_last_frozen_memtable_version(uint64_t &version);
        int get_table_time_stamp(const uint64_t major_version, int64_t &time_stamp);
        int get_oldest_memtable_size(int64_t &size, uint64_t &major_version);
        UpsSchemaMgr &get_schema_mgr()
        {
          return schema_mgr_;
        };
        void dump_memtable(const common::ObString &dump_dir);
        void dump_schemas();

        void set_replay_checksum_flag(const bool if_check)
        {
          TBSYS_LOG(INFO, "replay checksum flag switch from %s to %s", STR_BOOL(check_checksum_), STR_BOOL(if_check));
          check_checksum_ = if_check;
        }

        // do not impl in ups v0.2
        int create_index();
        int get_frozen_bloomfilter(const uint64_t version, common::TableBloomFilter &table_bf);

      public:
        // Gets a list of cells.
        //
        // @param [in] get_param param used to get data
        // @param [out] scanner result data of get operation.
        // @return OB_SUCCESS if success, other error code if error occurs.
        int get(const common::ObGetParam& get_param, common::ObScanner& scanner, const int64_t start_time, const int64_t timeout);
        // Scans row range.
        //
        // @param [in] scan_param param used to scan data
        // @param [out] scanner result data of scan operation
        // @return OB_SUCCESS if success, other error code if error occurs.
        int scan(const common::ObScanParam& scan_param, common::ObScanner& scanner, const int64_t start_time, const int64_t timeout);

        virtual int rpc_get(common::ObGetParam &get_param, common::ObScanner &scanner, const int64_t timeouut);

        int get_mutate_result(common::ObCellInfo &mutate_cell, common::ObIterator &active_data, common::ObScanner &scanner);

        int load_sstable_bypass(SSTableMgr &sstable_mgr, int64_t &loaded_num);
        int check_cur_version();

        void update_merged_version(ObUpsRpcStub &rpc_stub, const common::ObServer &root_server, const int64_t timeout_us);

      public:
        int freeze_memtable(const TableMgr::FreezeType freeze_type, uint64_t &frozen_version, bool &report_version_changed,
                            const common::ObPacket *resp_packet = NULL);
        void store_memtable(const bool all);
        void drop_memtable(const bool force);
        void erase_sstable(const bool force);
        void get_memtable_memory_info(TableMemInfo &mem_info);
        void log_memtable_memory_info();
        void set_memtable_attr(const MemTableAttr &memtable_attr);
        int get_memtable_attr(MemTableAttr &memtable_attr);
        void update_memtable_stat_info();
        int clear_active_memtable();
        int sstable_scan_finished(const int64_t minor_num_limit);
        int check_sstable_id();
        void log_table_info();
        template <typename T>
        int flush_obj_to_log(const common::LogCommand log_command, T &obj);
        int write_start_log();
        void set_warm_up_percent(const int64_t warm_up_percent);
        int get_schema(const uint64_t major_version, CommonSchemaManagerWrapper &sm);
        int get_sstable_range_list(const uint64_t major_version, const uint64_t table_id, TabletInfoList &ti_list);

      private:
        int set_mutator_(ObUpsMutator &mutator);
        int get_(TableList &table_list, const common::ObGetParam& get_param, common::ObScanner& scanner,
                const int64_t start_time, const int64_t timeout);

        int get_row_(TableList &table_list, const int64_t first_cell_idx, const int64_t last_cell_idx,
            const common::ObGetParam& get_param, common::ObScanner& scanner,
            const int64_t start_time, const int64_t timeout);

        int add_to_scanner_(common::ObIterator& ups_merger, common::ObScanner& scanner,
            int64_t& row_count, const int64_t start_time, const int64_t timeout, const int64_t result_limit_size = UINT64_MAX);
      private:
        ObClientWrapper* get_client_wrapper_();

        int check_permission_(common::ObMutator &mutator, const common::IToken &token);
        int trans_name2id_(common::ObMutator &mutator);
        int trans_cond_name2id_(common::ObMutator &mutator);
        int fill_commit_log_(ObUpsMutator &ups_mutator);
        int flush_commit_log_();
        int prepare_ups_cache_(const uint64_t table_id, const uint64_t column_id, const common::ObString &row_key,
                              const CommonSchemaManager* common_schema_mgr, ObClientWrapper* client_wrapper);
        int add_ups_cache_(const int64_t last_frozen_version, const common::ObCellInfo& cell_info);
        int check_condition_(common::ObMutator& mutator);
        int check_cond_info_(const int64_t op_type, const common::ObObj& cell_value,
            const common::ObObj& cond_value);
        int handle_freeze_log_(ObUpsMutator &ups_mutator, const ReplayType replay_type);

      private:
        static const int64_t RPC_RETRY_TIMES = 3;             // rpc retry times used by client wrapper
        static const int64_t RPC_TIMEOUT = 2 * 1000L * 1000L; // rpc timeout used by client wrapper

      private:
        char *log_buffer_;
        UpsSchemaMgr schema_mgr_;
        TableMgr table_mgr_;
        ObUpsCache& ups_cache_;
        bool check_checksum_;
        bool has_started_;
        uint64_t last_bypass_checksum_;
    };
  }
}

#endif //__UPS_TABLET_MGR_H__

