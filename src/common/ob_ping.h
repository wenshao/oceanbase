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

#ifndef OCEANBASE_COMMON_UPS_MON_H_
#define OCEANBASE_COMMON_UPS_MON_H_

#include "tbsys.h"
#include "tbnet.h"
#include "ob_define.h"
#include "ob_server.h"
#include "ob_client_manager.h"
#include "ob_packet_factory.h"
#include "ob_result.h"

namespace oceanbase
{
  namespace common
  {
    class ObPing
    {
    public:

      static ObPing* get_instance();

      virtual ~ObPing();
      virtual int start(const int argc, char *argv[]);
    protected:
      ObPing();
      virtual void do_signal(const int sig);
      void add_signal_catched(const int sig);
      static ObPing* instance_;
      virtual void print_usage(const char *prog_name);

      virtual int parse_cmd_line(const int argc, char *const argv[]);
      virtual int do_work();
      virtual int ping_server();
    private:
      static void sign_handler(const int sig);

    private:
      ObServer server_;
      int64_t ping_timeout_us_;
      int64_t ping_retry_times_;

    public:
      static const int64_t DEFAULT_PING_TIMEOUT_US = 1000000;
      static const int64_t DEFAULT_PING_RETRY_TIMES = 3;
      static const char* DEFAULT_LOG_LEVEL;
    }; // end class ObPing

    class BaseClient
    {
    public:
      BaseClient();
      virtual ~BaseClient();
    public:
      virtual int initialize();
      virtual int destroy();
      virtual int wait();
      
      inline ObClientManager * get_client_mgr()
      {
        return &client_;
      }

    private:
      tbnet::DefaultPacketStreamer streamer_;
      tbnet::Transport transport_;
      ObPacketFactory factory_;
      ObClientManager client_;
    }; // end class BaseClient
  } // end namespace common
} // end namespace oceanbase

#endif // OCEANBASE_COMMON_UPS_MON_H_
