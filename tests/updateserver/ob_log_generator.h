/**
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_log_generator.h
 *
 * Authors:
 *   yuanqi.xhf <yuanqi.xhf@taobao.com>
 *
 */

#ifndef __OCEANBASE_UPDATESERVER_OB_LOG_GENERATOR_H__
#define __OCEANBASE_UPDATESERVER_OB_LOG_GENERATOR_H__

#include "common/ob_log_entry.h"
#include "common/ob_log_cursor.h"

using namespace oceanbase::common;
namespace oceanbase
{
  namespace updateserver
  {
    class ObLogGenerator
    {
      public:
        ObLogGenerator();
        ~ObLogGenerator();
        int init(int64_t log_buf_size, int64_t log_file_max_size);
        int start_log(const ObLogCursor& start_cursor);
        int write_log(const LogCommand cmd, const char* log_data, const int64_t data_len);
        int get_log(ObLogCursor& start_cursor, ObLogCursor& end_cursor, char*& buf, int64_t& len);
        int commit(const ObLogCursor& end_cursor);
        int switch_log(int64_t& new_file_id);
      public:
        int get_start_cursor(ObLogCursor& log_cursor) const;
        int get_end_cursor(ObLogCursor& log_cursor) const;
        int dump_for_debug() const;
      protected:
        bool is_inited() const;
        int check_state() const;
        int do_write_log(const LogCommand cmd, const char* log_data, const int64_t data_len,
                         const int64_t reserved_len);
        int check_log_file_size();
        int switch_log();
        int write_nop();
      private:
        bool is_frozen_;
        int64_t log_file_max_size_;
        ObLogCursor start_cursor_;
        ObLogCursor end_cursor_;
        char* log_buf_;
        int64_t log_buf_len_;
        int64_t pos_;
    };
  } // end namespace updateserver
} // end namespace oceanbase
#endif // __OCEANBASE_UPDATESERVER_OB_LOG_GENERATOR_H__
