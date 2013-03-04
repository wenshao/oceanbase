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

#include "ob_ping.h"

#include <signal.h>
#include <getopt.h>
#include <new>

#include "common/ob_malloc.h"

using namespace oceanbase::common;

const char* ObPing::DEFAULT_LOG_LEVEL = "WARN";
ObPing* ObPing::instance_ = NULL;

ObPing* ObPing::get_instance() {
  if (NULL == instance_)
  {
    instance_ = new (std::nothrow) ObPing;
  }
  return instance_;
} 

ObPing::~ObPing()
{
}

int ObPing::start(const int argc, char *argv[])
{
  signal(SIGPIPE, SIG_IGN);
  signal(SIGCHLD, SIG_IGN);
  signal(SIGHUP, SIG_IGN);

  int ret = OB_SUCCESS;

  ret = parse_cmd_line(argc, argv);
  if (OB_SUCCESS != ret)
  {
    print_usage(argv[0]);
  }
  else
  {
    ret = do_work();
  }

  return ret;
}

ObPing::ObPing()
{
}

void ObPing::do_signal(const int sig)
{
  UNUSED(sig);
}

void ObPing::add_signal_catched(const int sig)
{
  signal(sig, ObPing::sign_handler);
}

void ObPing::print_usage(const char *prog_name)
{
  fprintf(stderr, "\nUsage: %s -i host -p port\n"
      "    -i, --host         server host name\n"
      "    -p, --port         server port\n"
      "    -t, --timeout      ping timeout(us, default %ld)\n"
      "    -r, --retry        ping retry times(default %ld)\n"
      "    -e, --loglevel     log level(ERROR|WARN|INFO|DEBUG, default %s)\n"
      "    -h, --help         this help\n"
      "    -V, --version      version\n\n", prog_name, DEFAULT_PING_TIMEOUT_US, DEFAULT_PING_RETRY_TIMES, DEFAULT_LOG_LEVEL);
}

int ObPing::parse_cmd_line(const int argc, char *const argv[])
{
  int ret = 0;

  int opt = 0;
  const char* opt_string = "i:p:t:r:e:hV";
  struct option longopts[] = 
  {
    {"host", required_argument, NULL, 'i'},
    {"port", required_argument, NULL, 'p'},
    {"timeout", required_argument, NULL, 't'},
    {"retry", required_argument, NULL, 'r'},
    {"loglevel", required_argument, NULL, 'e'},
    {"help", no_argument, NULL, 'h'},
    {"version", no_argument, NULL, 'V'},
    {0, 0, 0, 0}
  };

  const char* host = NULL;
  int32_t port = 0;
  const char* log_level = DEFAULT_LOG_LEVEL;
  ping_timeout_us_ = DEFAULT_PING_TIMEOUT_US;
  ping_retry_times_ = DEFAULT_PING_RETRY_TIMES;

  while((opt = getopt_long(argc, argv, opt_string, longopts, NULL)) != -1) {
    switch (opt) {
      case 'i':
        host = optarg;
        break;
      case 'p':
        port = static_cast<int32_t>(atol(optarg));
        break;
      case 't':
        ping_timeout_us_ = atol(optarg);
        break;
      case 'r':
        ping_retry_times_ = atol(optarg);
        break;
      case 'e':
        log_level = optarg;
        break;
      case 'V':
        fprintf(stderr, "BUILD_TIME: %s %s\n\n", __DATE__, __TIME__);
        ret = OB_ERROR;
        break;
      case 'h':
        print_usage(argv[0]);
        ret = OB_ERROR;
        break;
    }
  }

  if (NULL == host)
  {
    fprintf(stderr, "\nArgument -i(--host) is required\n\n");
    ret = OB_ERROR;
  }
  else if (0 == port)
  {
    fprintf(stderr, "\nArgument -p(--port) is required\n\n");
    ret = OB_ERROR;
  }
  else
  {
    TBSYS_LOGGER.setLogLevel(log_level);

    server_.set_ipv4_addr(host, port);

    TBSYS_LOG(INFO, "arguments parse succ, ups=%s:%d, timeout=%ldms, log_level=%s", host, port, ping_timeout_us_, log_level);
  }

  return ret;
}

int ObPing::do_work()
{
  int ret = OB_SUCCESS;

  ret = ping_server();

  return ret;
}

int ObPing::ping_server()
{
  int ret = OB_SUCCESS;

  static const int32_t MY_VERSION = 1;
  const int buff_size = sizeof(ObPacket);
  char buff[buff_size];
  ObDataBuffer data_buff(buff, buff_size);
  BaseClient client;
  int64_t start_time = tbsys::CTimeUtil::getTime();

  ret = client.initialize();

  if (OB_SUCCESS != ret)
  {
    TBSYS_LOG(ERROR, "failed to initialize client, ret=%d", ret);
  }
  else
  {
    ObClientManager* client_mgr = client.get_client_mgr();
    for (int64_t i = 0; i < ping_retry_times_; i++)
    {
      int64_t ping_beg_time = tbsys::CTimeUtil::getTime();
      ret = client_mgr->send_request(server_, OB_PING_REQUEST, MY_VERSION, ping_timeout_us_, data_buff);
      if (OB_SUCCESS == ret)
      {
        break;
      }
      else if (i + 1 < ping_retry_times_)
      {
        int64_t ping_elsp_time = tbsys::CTimeUtil::getTime() - ping_beg_time;
        if (ping_elsp_time < ping_timeout_us_)
        {
          usleep(static_cast<useconds_t>(ping_timeout_us_ - ping_elsp_time));
        }
      }
    }

    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "failed to send request, ret=%d", ret);
    }
    else
    {
      // deserialize the response code
      int64_t pos = 0;
      ObResultCode result_code;
      ret = result_code.deserialize(data_buff.get_data(), data_buff.get_position(), pos);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "deserialize result_code failed, pos=%ld, ret=%d", pos, ret);
      }
      else
      {
        ret = result_code.result_code_;
      }
    }
  }

  char addr[BUFSIZ];
  server_.to_string(addr, BUFSIZ);
  int64_t timeu = tbsys::CTimeUtil::getTime() - start_time;
  if (OB_SUCCESS == ret)
  {
    TBSYS_LOG(INFO, "ping ups(%s) succ, timeu=%ldus", addr, timeu);
  }
  else
  {
    TBSYS_LOG(ERROR, "ping ups(%s) failed, timeu=%ldus", addr, timeu);
  }

  client.destroy();

  return ret;
}

void ObPing::sign_handler(const int sig)
{
  UNUSED(sig);
}

BaseClient::BaseClient()
{
}

BaseClient::~BaseClient()
{
}

int BaseClient::initialize()
{
  int ret = OB_SUCCESS;

  streamer_.setPacketFactory(&factory_);
  client_.initialize(&transport_, &streamer_);

  ret = transport_.start() ? OB_SUCCESS : OB_ERROR;
  return ret;
}

int BaseClient::destroy()
{
  transport_.stop();
  return transport_.wait();
}

int BaseClient::wait()
{
  return transport_.wait();
}

int main(int argc, char** argv)
{
  int ret = OB_SUCCESS;

  ob_init_memory_pool();
  ObPing *ob_ping = ObPing::get_instance();
  if (NULL == ob_ping)
  {
    fprintf(stderr, "new ObPing failed\n");
    ret = OB_ERROR;
  }
  else
  {
    ret = ob_ping->start(argc, argv);
  }

  return ret;
}

