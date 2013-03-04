/**
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_log_generator.cpp
 *
 * Authors:
 *   yuanqi.xhf <yuanqi.xhf@taobao.com>
 *
 */
#include "common/ob_malloc.h"
#include "ob_log_generator.h"
#include "updateserver/ob_ups_log_utils.h"

using namespace oceanbase::common;
namespace oceanbase
{
  namespace updateserver
  {
    static const int64_t LOG_FILE_ALIGN_BIT = 9;
    static const int64_t LOG_FILE_ALIGN_SIZE = 1 << LOG_FILE_ALIGN_BIT;
    static const int64_t LOG_FILE_ALIGN_MASK = (-1) >> LOG_FILE_ALIGN_BIT << LOG_FILE_ALIGN_BIT;
    static int64_t aligned_ceil(int64_t x, int64_t n_bit)
    {
      return (x + (1 << n_bit) - 1) & ((~0) << n_bit);
    }

    static bool is_aligned(int64_t x, int64_t n_bit)
    {
      return ! (x & ~((~0)<<n_bit));
    }

    static int64_t calc_nop_log_len(int64_t pos)
    {
      int64_t header_size = ObLogEntry::get_header_size();
      return aligned_ceil(pos + header_size + 1, LOG_FILE_ALIGN_BIT) - pos - header_size;
    }

    ObLogGenerator::ObLogGenerator(): is_frozen_(false), log_file_max_size_(1<<24), start_cursor_(), end_cursor_(),
                                      log_buf_(NULL), log_buf_len_(0), pos_(0)
    {}

    ObLogGenerator:: ~ObLogGenerator()
    {
      if(NULL != log_buf_)
      {
        ob_free(log_buf_);
        log_buf_ = NULL;
      }
    }

    bool ObLogGenerator::is_inited() const
    {
      return NULL != log_buf_ && log_buf_len_ > 0;
    }

    int ObLogGenerator::dump_for_debug() const
    {
      int err = OB_SUCCESS;
      TBSYS_LOG(INFO, "[start_cursor[%ld], end_cursor[%ld]]", start_cursor_.log_id_, end_cursor_.log_id_);
      return err;
    }

    int ObLogGenerator::check_state() const
    {
      int err = OB_SUCCESS;
      if (!is_inited())
      {
        err = OB_NOT_INIT;
      }
      return err;
    }

    int ObLogGenerator::init(int64_t log_buf_size, int64_t log_file_max_size)
    {
      int err = OB_SUCCESS;
      if (is_inited())
      {
        err = OB_INIT_TWICE;
      }
      else if(log_buf_size <= 0 || log_file_max_size <= 0 || log_file_max_size < 2 * LOG_FILE_ALIGN_SIZE)
      {
        err = OB_INVALID_ARGUMENT;
      }
      else if (NULL == (log_buf_ = (char*)ob_malloc(log_buf_size)))
      {
        err = OB_ALLOCATE_MEMORY_FAILED;
      }
      else
      {
        log_file_max_size_ = log_file_max_size;
        log_buf_len_ = log_buf_size;
        TBSYS_LOG(INFO, "log_generator.init(log_buf_size=%ld, log_file_max_size=%ld)", log_buf_size, log_file_max_size);
      }
      return err;
    }

    int ObLogGenerator::start_log(const ObLogCursor& log_cursor)
    {
      int err = OB_SUCCESS;
      if (!log_cursor.is_valid())
      {
        err = OB_INVALID_ARGUMENT;
        TBSYS_LOG(ERROR, "log_cursor.is_valid()=>false");
      }
      else if (start_cursor_.is_valid())
      {
        err = OB_INIT_TWICE;
        TBSYS_LOG(ERROR, "cursor=[%ld,%ld] already inited.", start_cursor_.log_id_, end_cursor_.log_id_);
      }
      else
      {
        start_cursor_ = log_cursor;
        end_cursor_ = log_cursor;
        TBSYS_LOG(INFO, "ObLogGenerator::start_log(log_cursor=%s)", log_cursor.to_str());
      }
      return err;
    }

    int ObLogGenerator:: get_start_cursor(ObLogCursor& log_cursor) const
    {
      int err = OB_SUCCESS;
      if (OB_SUCCESS != (err = check_state()))
      {
        TBSYS_LOG(ERROR, "check_state()=>%d", err);
      }
      else
      {
        log_cursor = start_cursor_;
      }
      return err;
    }

    int ObLogGenerator:: get_end_cursor(ObLogCursor& log_cursor) const
    {
      int err = OB_SUCCESS;
      if (OB_SUCCESS != (err = check_state()))
      {
        TBSYS_LOG(ERROR, "check_state()=>%d", err);
      }
      else
      {
        log_cursor = end_cursor_;
      }
      return err;
    }

    int ObLogGenerator:: do_write_log(const LogCommand cmd, const char* log_data, const int64_t data_len,
                                      const int64_t reserved_len)
    {
      int err = OB_SUCCESS;
      if (OB_SUCCESS != (err = check_state()))
      {
        TBSYS_LOG(ERROR, "check_state()=>%d", err);
      }
      else if (NULL == log_data || data_len <= 0)
      {
        err = OB_INVALID_ARGUMENT;
      }
      else if (OB_SUCCESS != (err = generate_log(log_buf_, log_buf_len_ - reserved_len, pos_,
                                                 end_cursor_, cmd, log_data, data_len))
               && OB_BUF_NOT_ENOUGH != err)
      {
        TBSYS_LOG(ERROR, "generate_log(pos=%ld)=>%d", pos_, err);
      }
      return err;
    }

    int ObLogGenerator:: write_log(const LogCommand cmd, const char* log_data, const int64_t data_len)
    {
      int err = OB_SUCCESS;
      if (OB_SUCCESS != (err = check_state()))
      {
        TBSYS_LOG(ERROR, "check_state()=>%d", err);
      }
      else if (NULL == log_data || data_len <= 0)
      {
        err = OB_INVALID_ARGUMENT;
      }
      else if (is_frozen_ || end_cursor_.offset_ + data_len + 2*LOG_FILE_ALIGN_SIZE > log_file_max_size_)
      {
        err = OB_BUF_NOT_ENOUGH;
      }
      else if (OB_SUCCESS != (err = do_write_log(cmd, log_data, data_len, 2 * LOG_FILE_ALIGN_SIZE))
               && OB_BUF_NOT_ENOUGH != err)
      {
        TBSYS_LOG(ERROR, "do_write_log(cmd=%d, pos=%ld, len=%ld)=>%d", cmd, pos_, data_len, err);
      }

      return err;
    }

    int ObLogGenerator::switch_log()
    {
      int err = OB_SUCCESS;
      const int64_t buf_len = LOG_FILE_ALIGN_SIZE - ObLogEntry::get_header_size();
      char buf[buf_len];
      int64_t buf_pos = 0;
      if (OB_SUCCESS != (err = check_state()))
      {
        TBSYS_LOG(ERROR, "check_state()=>%d", err);
      }
      else if (OB_SUCCESS != (err = serialization::encode_i64(buf, buf_len, buf_pos, end_cursor_.file_id_)))
      {
        TBSYS_LOG(ERROR, "encode_i64(file_id_=%ld)=>%d", end_cursor_.file_id_, err);
      }
      else if (OB_SUCCESS != (err = do_write_log(OB_LOG_SWITCH_LOG, buf, buf_len, 0)))
      {
        TBSYS_LOG(ERROR, "write(OB_LOG_SWITCH_LOG, len=%ld)=>%d", end_cursor_.file_id_, err);
      }
      else
      {
        TBSYS_LOG(INFO, "switch_log(file_id=%ld, log_id=%ld)", end_cursor_.file_id_, end_cursor_.log_id_);
      }
      return err;
    }

    int ObLogGenerator:: check_log_file_size()
    {
      int err = OB_SUCCESS;
      if (end_cursor_.offset_ + 2*LOG_FILE_ALIGN_SIZE < log_file_max_size_)
      {}
      else if (OB_SUCCESS != (err = switch_log()))
      {
        TBSYS_LOG(ERROR, "switch_log()=>%d", err);
      }
      return err;
    }

    int ObLogGenerator:: write_nop()
    {
      int err = OB_SUCCESS;
      static char empty_log[LOG_FILE_ALIGN_SIZE * 2];
      if (is_aligned(pos_, LOG_FILE_ALIGN_BIT))
      {
        TBSYS_LOG(INFO, "The log is aligned");
      }
      else if (OB_SUCCESS != (err = do_write_log(OB_LOG_NOP, empty_log, calc_nop_log_len(pos_), 0)))
      {
        TBSYS_LOG(ERROR, "write_log(OB_LOG_NOP, len=%ld)=>%d", calc_nop_log_len(pos_), err);
      }
      return err;
    }

    int ObLogGenerator:: switch_log(int64_t& new_file_id)
    {
      int err = OB_SUCCESS;
      if (OB_SUCCESS != (err = check_state()))
      {
        TBSYS_LOG(ERROR, "check_state()=>%d", err);
      }
      else if (OB_SUCCESS != (err = write_nop()))
      {
        TBSYS_LOG(ERROR, "write_nop()=>%d", err);
      }
      else if (OB_SUCCESS != (err = switch_log()))
      {
        TBSYS_LOG(ERROR, "switch_log()=>%d", err);
      }
      else
      {
        is_frozen_ = true;
        new_file_id = end_cursor_.file_id_;
      }
      return err;
    }

    int ObLogGenerator:: get_log(ObLogCursor& start_cursor, ObLogCursor& end_cursor, char*& buf, int64_t& len)
    {
      int err = OB_SUCCESS;
      if (OB_SUCCESS != (err = check_state()))
      {
        TBSYS_LOG(ERROR, "check_state()=>%d", err);
      }
      else if (OB_SUCCESS != (err = write_nop()))
      {
        TBSYS_LOG(ERROR, "write_nop()=>%d", err);
      }
      else if (OB_SUCCESS != (err = check_log_file_size()))
      {
        TBSYS_LOG(ERROR, "check_log_file_size()=>%d", err);
      }
      else
      {
        is_frozen_ = true;
        buf = log_buf_;
        len = pos_;
        end_cursor = end_cursor_;
        start_cursor = start_cursor_;
      }
      return err;
    }

    int ObLogGenerator:: commit(const ObLogCursor& end_cursor)
    {
      int err = OB_SUCCESS;
      if (OB_SUCCESS != (err = check_state()))
      {
        TBSYS_LOG(ERROR, "check_state()=>%d", err);
      }
      else if (!end_cursor.equal(end_cursor_))
      {
        err = OB_ERR_UNEXPECTED;
        TBSYS_LOG(ERROR, "end_cursor[%ld] != end_cursor_[%ld]", end_cursor.log_id_, end_cursor_.log_id_);
      }
      else
      {
        start_cursor_ = end_cursor_;
        pos_ = 0;
        is_frozen_ = false;
      }
      return err;
    }
  } // end namespace updateserver
} // end namespace oceanbase
