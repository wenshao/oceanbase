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

#ifndef OCEANBASE_COMMON_OB_LOG_WRITER_H_
#define OCEANBASE_COMMON_OB_LOG_WRITER_H_

#include "tblog.h"

#include "ob_define.h"
#include "data_buffer.h"
#include "ob_slave_mgr.h"
#include "ob_log_entry.h"
#include "ob_file.h"
#include "ob_log_cursor.h"

namespace oceanbase
{
  namespace tests
  {
    namespace common
    {
      // forward decleration
      class TestObLogWriter_test_init_Test;
      class TestObLogWriter_test_write_log_Test;
      class TestObLogWriter_test_write_and_flush_log_Test;
      class TestObLogWriter_test_align_Test;
      class TestObRepeatedLogReader_test_read_log_Test;
    } // end namespace common
  } // end namespace tests
  namespace common
  {
    class ObLogWriter
    {
      friend class tests::common::TestObLogWriter_test_init_Test;
      friend class tests::common::TestObLogWriter_test_write_log_Test;
      friend class tests::common::TestObLogWriter_test_write_and_flush_log_Test;
      friend class tests::common::TestObLogWriter_test_align_Test;
      friend class tests::common::TestObRepeatedLogReader_test_read_log_Test;
    public:
      static const int64_t LOG_BUFFER_SIZE = 1966080L;  // 1.875MB
      static const int64_t LOG_FILE_ALIGN_BIT = 9;
      static const int64_t LOG_FILE_ALIGN_SIZE = 1 << LOG_FILE_ALIGN_BIT;
      static const int64_t LOG_FILE_ALIGN_MASK = (-1) >> LOG_FILE_ALIGN_BIT << LOG_FILE_ALIGN_BIT;

    public:
      ObLogWriter();
      virtual ~ObLogWriter();

      /// 初始化
      /// @param [in] log_dir 日志数据目录
      /// @param [in] log_file_max_size 日志文件最大长度限制
      /// @param [in] slave_mgr ObSlaveMgr用于同步日志
      int init(const char* log_dir, const int64_t log_file_max_size, ObSlaveMgr *slave_mgr, int64_t log_sync_type);

      void reset_log();
      void close();

        virtual int update_write_cursor(ObLogCursor& log_cursor)
      {
          UNUSED(log_cursor);
          return OB_SUCCESS;
      }
        virtual int update_store_cursor(ObLogCursor& log_cursor)
      {
          UNUSED(log_cursor);
          return OB_SUCCESS;
      }
        virtual int write_log_hook(int64_t start_id, int64_t end_id, const char* log_data, const int64_t data_len)
      {
          UNUSED(start_id);
          UNUSED(end_id);
          UNUSED(log_data);
          UNUSED(data_len);
          return OB_SUCCESS;
      }
      /// @brief start to write commit log (for master)
      /// @param [in] log_file_max_id id of last log file (the maximum log file id currently)
      /// @param [in] log_max_seq seq of last log entry (the maximum seq currently)
        int start_log(const uint64_t log_file_max_id, const uint64_t log_max_seq, const int64_t offset = -1);

      /// @brief start to write commit log (for slave)
      /// @param [in] log_file_max_id id of last log file (the maximum log file id currently)
      int start_log(const uint64_t log_file_max_id);

      /// @brief 写日志
      /// write_log函数将日志存入自己的缓冲区, 缓冲区大小LOG_BUFFER_SIZE常量
      /// 首先检查日志大小是否还允许存入一整个缓冲区, 若不够则切日志
      /// 然后将日志内容写入缓冲区
      /// @param [in] log_data 日志内容
      /// @param [in] data_len 长度
      /// @retval OB_SUCCESS 成功
      /// @retval OB_BUF_NOT_ENOUGH 内部缓冲区已满
      /// @retval otherwise 失败
      int write_log(const LogCommand cmd, const char* log_data, const int64_t data_len);

      int write_keep_alive_log();
      /// @brief 将缓冲区中的日志写入磁盘
      /// flush_log首相将缓冲区中的内容同步到Slave机器
      /// 然后写入磁盘
      /// @retval OB_SUCCESS 成功
      /// @retval otherwise 失败
      int flush_log();

      /// @brief 写日志并且写盘
      /// 序列化日志并且写盘
      /// 内部缓冲区原先如有数据, 会被清空
      int write_and_flush_log(const LogCommand cmd, const char* log_data, const int64_t data_len);

      /// @brief 将缓冲区内容写入日志文件
      /// 然后将缓冲区内容刷入磁盘
      /// @retval OB_SUCCESS 成功
      /// @retval otherwise 失败
      int store_log(const char* buf, const int64_t buf_len);

      /// @brief Master切换日志文件
      /// 产生一条切换日志文件的commit log
      /// 同步到Slave机器并等待返回
      /// 关闭当前正在操作的日志文件, 日志序号+1, 打开新的日志文件
      /// @retval OB_SUCCESS 成功
      /// @retval otherwise 失败
      int switch_log_file(uint64_t &new_log_file_id);

      /// @brief Slave切换日志文件
      /// 判断日志文件序号是否同步
      /// 关闭当前正在操作的日志文件
      /// 打开序号是log_file_id的日志文件
      /// @retval OB_SUCCESS 成功
      /// @retval otherwise 失败
      int switch_to_log_file(const uint64_t log_file_id);

      /// @brief 写一条checkpoint日志并返回当前日志号
      /// 写完checkpoint日志后切日志
      int write_checkpoint_log(uint64_t &log_file_id);

      inline uint64_t get_cur_log_file_id() {return cur_log_file_id_;}

      inline void set_cur_log_seq(uint64_t cur_log_seq) {cur_log_seq_ = cur_log_seq;}
      inline uint64_t get_cur_log_seq() {return cur_log_seq_;}
      inline uint64_t get_cur_log_offset() {return cur_log_size_;}

      inline ObSlaveMgr* get_slave_mgr() {return slave_mgr_;}

      inline int64_t get_last_net_elapse() {return last_net_elapse_;}
      inline int64_t get_last_disk_elapse() {return last_disk_elapse_;}
      inline int64_t get_last_flush_log_time() {return last_flush_log_time_;}

      inline bool get_is_log_start() {return is_log_start_;}

    protected:
      /// @brief 打开文件id为log_file_id的日志文件
      /// @param [in] is_trunk whether erase all content of existing file
      int open_log_file_(const uint64_t log_file_id, bool is_trunc);

      /// 将日志内容加上日志头部, 日志序号和LogCommand, 并序列化到缓冲区
      int serialize_log_(LogCommand cmd, const char* log_data, const int64_t data_len);

      int serialize_nop_log_();

      /// 检查当前日志还够不够new_length长度的日志, 若不够则切日志
      int check_log_file_size_(const int64_t new_length);

      inline int check_inner_stat() const
      {
        int ret = OB_SUCCESS;
        if (!is_initialized_)
        {
          TBSYS_LOG(ERROR, "ObLogWriter has not been initialized");
          ret = OB_NOT_INIT;
        }
        return ret;
      }

      protected:
      ObSlaveMgr *slave_mgr_;
    private:
      int64_t log_sync_type_;
      int64_t cur_log_size_;  //当前日志文件长度
      int64_t log_file_max_size_;  //日志文件最大长度
      uint64_t next_log_file_id_;  //切日志之后的文件编号
      uint64_t cur_log_file_id_;  //当前日志文件编号
      uint64_t cur_log_seq_;  //当前日志序号
      int64_t last_flush_log_time_;
      int64_t last_net_elapse_;  //上一次写日志网络同步耗时
      int64_t last_disk_elapse_;  //上一次写日志磁盘耗时
      ObDataBuffer log_buffer_;
      ObFileAppender file_;
      char log_dir_[OB_MAX_FILE_NAME_LENGTH];  //日志目录
      char empty_log_[LOG_FILE_ALIGN_SIZE * 2];
      bool is_initialized_;
      bool dio_;
      bool allow_log_not_align_;
      bool is_log_start_;
    };
  } // end namespace common
} // end namespace oceanbase

#endif // OCEANBASE_COMMON_OB_LOG_WRITER_H_
