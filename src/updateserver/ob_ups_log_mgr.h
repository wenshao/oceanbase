/**
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * Authors:
 *   yanran <yanran.hfs@taobao.com>
 *     - some work details if you want
 */

#ifndef OCEANBASE_UPDATESERVER_OB_UPS_LOG_MGR_H_
#define OCEANBASE_UPDATESERVER_OB_UPS_LOG_MGR_H_

#include "common/ob_define.h"
#include "common/ob_log_writer.h"
#include "common/ob_mutex_task.h"
#include "common/ob_server_getter.h"
#include "ob_ups_role_mgr.h"
#include "ob_ups_table_mgr.h"
#include "ob_ups_slave_mgr.h"

#include "ob_ups_log_utils.h"
#include "ob_log_buffer.h"
#include "ob_pos_log_reader.h"
#include "ob_cached_pos_log_reader.h"
#include "ob_replay_log_src.h"
#include "ob_log_sync_delay_stat.h"
namespace oceanbase
{
  namespace tests
  {
    namespace updateserver
    {
      // forward decleration
      class TestObUpsLogMgr_test_init_Test;
    }
  }
  namespace updateserver
  {
    class ObUpsLogMgr : public common::ObLogWriter, public IObLogApplier
    {
      friend class tests::updateserver::TestObUpsLogMgr_test_init_Test;
      struct ReplayLocalLogTask : public common::ObMutexTask
      {
        ReplayLocalLogTask(): log_mgr_(NULL) {}
        virtual ~ReplayLocalLogTask() {}
        int init(ObUpsLogMgr* log_mgr)
        {
          int err = OB_SUCCESS;
          log_mgr_ = log_mgr;
          return err;
        }
        virtual int do_work(void* arg)
        {
          UNUSED(arg);
          int err = OB_SUCCESS;
          if (finished_count_ > 0)
          {} // already done.
          else if (NULL == log_mgr_)
          {
            err = OB_NOT_INIT;
          }
          else if (OB_SUCCESS != (err = log_mgr_->replay_local_log()))
          {
            TBSYS_LOG(ERROR, "replay_local_log()=>%d", err);
          }
          else
          {
            TBSYS_LOG(INFO, "replay local log succ.");
          }
          return err;
        }
        ObUpsLogMgr* log_mgr_;
      };
      public:
      static const char* UPS_LOG_REPLAY_POINT_FILE;
      static const int UINT64_MAX_LEN;
      public:
      ObUpsLogMgr();
      virtual ~ObUpsLogMgr();
      int init(const char* log_dir, const int64_t log_file_max_size,
          ObReplayLogSrc* replay_log_src, ObUpsTableMgr* table_mgr,
          ObUpsSlaveMgr *slave_mgr, ObiRole* obi_role, ObUpsRoleMgr *role_mgr, int64_t log_sync_type);

  // write_replay_point()/set_replay_point()已经再有实际的用处了
      /// @brief set new replay point
      /// this method will write replay point to replay_point_file
      int write_replay_point(uint64_t replay_point);

      void set_replay_point(uint64_t replay_point)
      {
        replay_point_ = replay_point;
        //        if (max_log_id_ < replay_point -1)
        //        {
        //          max_log_id_ = replay_point - 1;
        //        }
      }

      public:
        // 继承log_writer留下的回调接口

        // 主UPS每次生成日志时调用
      virtual int update_write_cursor(ObLogCursor& log_cursor);
        // 主备UPS每次写盘之后调用
      virtual int update_store_cursor(ObLogCursor& log_cursor);
        // 主UPS写盘之后调用
      virtual int write_log_hook(int64_t start_id, int64_t end_id, const char* log_data, const int64_t data_len);
      public:
      // 统计回放日志的时间和生成mutator的时间延迟
      ObLogSyncDelayStat& get_delay_stat()
      {
        return delay_stat_;
      }
        // 取得recent_log_cache的引用
      ObLogBuffer& get_log_buffer();
        // 不管是重放本地日志还是备机追赶主机时重放日志，都同一调用apply_log()
      virtual int apply_log(LogCommand cmd, uint64_t seq, const char* log_data, const int64_t data_len, const ReplayType replay_type);
      public: // 主要接口
        // UPS刚启动，重放本地日志任务的函数
      int replay_local_log();
        // 重放完本地日志之后，主UPS调用start_log_for_master_write()，
        //主要是初始化log_writer写日志的起始点
      int start_log_for_master_write();
        // 主UPS给备UPS推送日志后，备UPS调用slave_receive_log()收日志
        // 收到日志后放到recent_log_cache
      int slave_receive_log(const char* buf, int64_t len);
        // 备UPS向主UPS请求日志时，主UPS调用get_log_for_slave_fetch()读缓冲区或文件得到日志
      int get_log_for_slave_fetch(ObFetchLogReq& req, ObFetchedLog& result);
        // 备UPS向主UPS注册成功后，主UPS返回自己的最大日志号，备UPS调用set_master_log_id()记录下这个值
      int set_master_log_id(const int64_t master_log_id);
        // RS选主时，需要问UPS已收到的连续的最大日志号
      int get_max_log_seq_replayable(int64_t& max_log_seq) const;
        // 如果备UPS本地没有日志，需要问主UPS自己应该从那个点开始写，
        // 主UPS调用fill_log_cursor()填充日志点
      int fill_log_cursor(ObLogCursor& log_cursor);
        // 备UPS的replay线程，调用replay_log()方法不停追赶主机的日志
        // replay_log()接着本地日志开始，不仅负责回放，也负责写盘
      int replay_log();
      int get_replayed_cursor(ObLogCursor& cursor) const;
        bool is_sync_with_master() const;
        bool has_nothing_in_buf_to_replay() const;
      protected:
        // 下面几个方法都是replay_log()需要的辅助方法
      int replay_and_write_log(const int64_t start_id, const int64_t end_id, const char* buf, int64_t len);
      int write_log_as_slave(const char* buf, const int64_t len, const common::ObLogCursor& end_cursor);
      int retrieve_log_for_replay(const int64_t start_id, int64_t& end_id, char* buf, const int64_t len,
          int64_t& read_count);
        // 确保得到一个确定的日志点才能开始回放后续日志
        // 可能需要用rpc询问主UPS
      int start_log_for_replay();

      protected:
      bool is_master_master() const;
      bool is_slave_master() const;
      int set_state_as_active();
      int get_max_log_seq_in_file(int64_t& log_seq) const;
      int get_max_log_seq_in_buffer(int64_t& log_seq) const;
      public:
      int do_replay_local_log_task()
      {
        return replay_local_log_task_.run(NULL);
      }
      bool is_log_replay_started() const
      {
        return replay_local_log_task_.get_finished_count() > 0 || replay_local_log_task_.is_running();
      }
      bool is_log_replay_finished() const
      {
        return replay_local_log_task_.get_finished_count() > 0;
      }
      uint64_t get_master_log_seq() const
      {
        return master_log_id_;
      }
      void signal_stop()
      {
        stop_ = true;
      }

      inline uint64_t get_replay_point() {return replay_point_;}
      inline int64_t get_last_receive_log_time() {return last_receive_log_time_;}

      int add_slave(const common::ObServer& server, uint64_t &new_log_file_id, const bool switch_log);

      inline uint64_t get_max_log_id() {return max_log_id_;}
      void get_cur_log_point(int64_t& log_file_id, int64_t& log_seq_id, int64_t& log_offset)
      {
        log_file_id = get_cur_log_file_id();
        log_seq_id = get_cur_log_seq();
        log_offset = get_cur_log_offset();
      }
      private:
      bool is_inited() const;
      private:
      int load_replay_point_();

      inline int check_inner_stat() const
      {
        int ret = common::OB_SUCCESS;
        if (!is_initialized_)
        {
          TBSYS_LOG(ERROR, "ObUpsLogMgr has not been initialized");
          ret = common::OB_NOT_INIT;
        }
        return ret;
      }

      private:
        ObUpsTableMgr *table_mgr_;
        ObiRole *obi_role_;
        ObUpsRoleMgr *role_mgr_;
        ObLogBuffer recent_log_cache_; // 主机写日志或备机收日志时保存日志用的缓冲区
        ObPosLogReader pos_log_reader_; //从磁盘上根据日志ID读日志， 用来初始化cached_pos_log_reader_
        ObCachedPosLogReader cached_pos_log_reader_; // 根据日志ID读日志，先查看缓冲区，在查看日志文件
        ObReplayLogSrc* replay_log_src_; // 备机replay_log()时从replay_log_src_中取日志

        IObServerGetter* master_getter_; // 用来指示远程日志源的地址和类型(是lsync还是ups)
        ReplayLocalLogTask replay_local_log_task_;
        common::ObLogCursor replayed_cursor_; // 已经回放到的点，不管发生什么情况都有保持连续递增
        int64_t local_max_log_id_when_start_;
        ObLogSyncDelayStat delay_stat_;
        volatile bool stop_; // 主要用来通知回放本地日志的任务结束
        ThreadSpecificBuffer log_buffer_for_fetch_;
        ThreadSpecificBuffer log_buffer_for_replay_;
        int64_t master_log_id_; // 备机知道的主机最大日志号
        uint64_t replay_point_; // 没用到
        int64_t last_receive_log_time_;
      uint64_t max_log_id_;
      bool is_initialized_;
      bool is_log_dir_empty_;
      char replay_point_fn_[common::OB_MAX_FILE_NAME_LENGTH];
      char log_dir_[common::OB_MAX_FILE_NAME_LENGTH];
      bool is_started_;
    };
  } // end namespace updateserver
} // end namespace oceanbase

#endif // OCEANBASE_UPDATESERVER_OB_UPS_LOG_MGR_H_
