////===================================================================
 //
 // ob_trace_log.cpp / hash / common / Oceanbase
 //
 // Copyright (C) 2010 Taobao.com, Inc.
 //
 // Created on 2010-09-01 by Yubai (yubai.lk@taobao.com) 
 //
 // -------------------------------------------------------------------
 //
 // Description
 //
 //
 // -------------------------------------------------------------------
 // 
 // Change Log
 //
////====================================================================

#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <pthread.h>
#include <new>
#include <algorithm>
#include "ob_trace_log.h"
#include "ob_define.h"
#include "tblog.h"
#include "tbtimeutil.h"

namespace oceanbase
{
  namespace common
  {
    const char *const TraceLog::LOG_LEVEL_ENV_KEY = "_OB_TRACE_LOG_LEVEL_";
    const char *const TraceLog::level_strs_[] = {"ERROR", "USER_ERR", "WARN", "INFO", "TRACE", "DEBUG"};
    volatile int TraceLog::log_level_ = TBSYS_LOG_LEVEL_DEBUG;
    bool TraceLog::got_env_ = false;

    TraceLog::LogBuffer &TraceLog::get_logbuffer()
    {
      static __thread LogBuffer log_buffer;
      static __thread bool inited = false;
      if (!inited)
      {
        memset(&log_buffer, 0, sizeof(log_buffer));
        inited = true;
      }
      return log_buffer;
    }

    int TraceLog::dec_log_level()
    {
      int prev_log_level = log_level_;
      int modify_log_level = log_level_ - 1;
      if (TBSYS_LOG_LEVEL_DEBUG >= modify_log_level
          && TBSYS_LOG_LEVEL_ERROR <= modify_log_level)
      {
        log_level_ = modify_log_level;
      }
      TBSYS_LOG(INFO, "dec_log_level prev_log_level=%d cur_log_level=%d", prev_log_level, log_level_);
      return log_level_;
    }

    int TraceLog::inc_log_level()
    {
      int prev_log_level = log_level_;
      int modify_log_level = log_level_ + 1;
      if (TBSYS_LOG_LEVEL_DEBUG >= modify_log_level
          && TBSYS_LOG_LEVEL_ERROR <= modify_log_level)
      {
        log_level_ = modify_log_level;
      }
      TBSYS_LOG(INFO, "inc_log_level prev_log_level=%d cur_log_level=%d", prev_log_level, log_level_);
      return log_level_;
    }

    int TraceLog::set_log_level(const char *log_level_str)
    {
      if (NULL != log_level_str)
      {
        int level_num = sizeof(level_strs_) / sizeof(const char*);
        for (int i = 0; i < level_num; ++i)
        {
          if (0 == strcasecmp(level_strs_[i], log_level_str))
          {
            log_level_ = i;
            break;
          }
        }
        got_env_ = true;
      }
      return log_level_;
    }
        
    int TraceLog::get_log_level()
    {
      if (!got_env_)
      {
        const char *log_level_str = getenv(LOG_LEVEL_ENV_KEY);
        set_log_level(log_level_str);
      }
      return log_level_;
    }

    void TraceLog::fill_log(const char *fmt, ...)
    {
      if (get_log_level() > TBSYS_LOGGER._level)
      {
        LogBuffer &log_buffer = get_logbuffer();
        if (0 == log_buffer.prev_timestamp)
        {
          log_buffer.start_timestamp = tbsys::CTimeUtil::getTime();
          log_buffer.prev_timestamp = log_buffer.start_timestamp;
        }
      }
      else
      {
        va_list args;
        va_start(args, fmt);
        LogBuffer &log_buffer = get_logbuffer();
        int64_t valid_buffer_size = LogBuffer::LOG_BUFFER_SIZE - log_buffer.cur_pos;
        if (0 < valid_buffer_size)
        {
          int64_t ret = 0;
          ret = vsnprintf(log_buffer.buffer + log_buffer.cur_pos, valid_buffer_size, fmt, args);
          log_buffer.cur_pos += ((0 < ret && valid_buffer_size > ret) ? ret : 0);
          if (-1 < ret
              && valid_buffer_size > ret)
          {
            valid_buffer_size -= ret;
            int64_t cur_time = tbsys::CTimeUtil::getTime();
            if (0 < valid_buffer_size)
            {
              if (0 != log_buffer.prev_timestamp)
              {
                ret = snprintf(log_buffer.buffer + log_buffer.cur_pos, valid_buffer_size, " timeu=%lu | ",
                              cur_time - log_buffer.prev_timestamp);
              }
              else
              {
                ret = snprintf(log_buffer.buffer + log_buffer.cur_pos, valid_buffer_size, " | ");
                log_buffer.start_timestamp = cur_time;
              }
              log_buffer.cur_pos += ((0 < ret && valid_buffer_size > ret) ? ret : 0);
            }
            log_buffer.prev_timestamp = cur_time;
          }
          log_buffer.buffer[log_buffer.cur_pos] = '\0';
        }
        va_end(args);
      }
    }

    void TraceLog::clear_log()
    {
      oceanbase::common::TraceLog::get_logbuffer().cur_pos = 0;
      oceanbase::common::TraceLog::get_logbuffer().prev_timestamp = 0;
      oceanbase::common::TraceLog::get_logbuffer().start_timestamp = 0;
    }
  }
}

