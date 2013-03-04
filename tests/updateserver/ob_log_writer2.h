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
 *   yuanqi.xhf <yuanqi.xhf.hfs@taobao.com>
 *     - some work details if you want
 */

#ifndef OCEANBASE_COMMON_OB_LOG_WRITER2_H_
#define OCEANBASE_COMMON_OB_LOG_WRITER2_H_

#include "common/ob_define.h"
#include "common/ob_log_cursor.h"
#include "common/ob_file.h"

using namespace oceanbase::common;
namespace oceanbase
{
  namespace updateserver
  {
    class ObLogWriterV2
    {
      public:
        ObLogWriterV2();
        virtual ~ObLogWriterV2();
        int init(const char* log_dir, int64_t log_sync_type);
        int reset();

        int start_log(const ObLogCursor& log_cursor);
        int write_log(const char* log_data, int64_t data_len);
        int get_cursor(ObLogCursor& log_cursor) const;
        int flush_log();
      protected:
        int check_inited() const;
        int check_state() const;
      protected:
        char* log_dir_;
        ObFileAppender file_;
        bool dio_;
        int64_t log_sync_type_;
        ObLogCursor log_cursor_;
        int64_t last_disk_elapse_;  //上一次写日志磁盘耗时
    };
    //typedef ObLogWriterV2 ObLogWriter;
  } // end namespace updateserver
} // end namespace oceanbase
;
#endif // OCEANBASE_COMMON_OB_LOG_WRITER2_H_
