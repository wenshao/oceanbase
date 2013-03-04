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
 *   yuanqi <yuanqi.xhf@taobao.com>
 *     - some work details if you want
 */
#ifndef __OB_UPDATESERVER_OB_CACHED_POS_LOG_READER_H__
#define __OB_UPDATESERVER_OB_CACHED_POS_LOG_READER_H__
#include "common/ob_define.h"

namespace oceanbase
{
  namespace updateserver
  {
    class ObPosLogReader;
    class ObLogBuffer;
    class ObFetchLogReq;
    class ObFetchedLog;

    class ObCachedPosLogReader
    {
      public:
        ObCachedPosLogReader();
        ~ObCachedPosLogReader();
      public:
        int init(ObPosLogReader* log_reader, ObLogBuffer* log_buffer);
        int get_log(ObFetchLogReq& req, ObFetchedLog& result);
      protected:
        bool is_inited() const;
      private:
        ObPosLogReader* log_reader_;
        ObLogBuffer* log_buffer_;
    };
  }; // end namespace updateserver
}; // end namespace oceanbase

#endif /* __OB_UPDATESERVER_OB_CACHED_POS_LOG_READER_H__ */
