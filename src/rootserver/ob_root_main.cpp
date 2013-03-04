/*===============================================================
*   (C) 2007-2010 Taobao Inc.
*   
*   
*   Version: 0.1 2010-09-26
*   
*   Authors:
*          daoan(daoan@taobao.com)
*   
*
================================================================*/
#include "rootserver/ob_root_main.h"
const char* svn_version();
const char* build_date();
const char* build_time();
const char* build_flags();

namespace oceanbase 
{ 
  using common::OB_SUCCESS;
  using common::OB_ERROR;
  namespace rootserver
  { 
    ObRootMain::ObRootMain()
    {
    }
    common::BaseMain* ObRootMain::get_instance()
    {
      if (instance_ == NULL)
      {
        instance_ = new (std::nothrow)ObRootMain();
      }
      return instance_;
    }

void ObRootMain::print_version()
{
  fprintf(stderr, "rootserver (%s %s)\n", PACKAGE_STRING, RELEASEID);
  fprintf(stderr, "SVN_VERSION: %s\n", svn_version());
  fprintf(stderr, "BUILD_TIME: %s %s\n", build_date(), build_time());
  fprintf(stderr, "BUILD_FLAGS: %s\n\n", build_flags());
  fprintf(stderr, "Copyright (c) 2007-2012 Taobao Inc.\n");
}

    static const int START_REPORT_SIG = 49;
    static const int START_MERGE_SIG = 50;
    static const int DUMP_ROOT_TABLE_TO_LOG = 51;
    static const int DUMP_AVAILABLE_SEVER_TO_LOG = 52;
    static const int SWITCH_SCHEMA = 53;
    static const int RELOAD_CONFIG = 54;
    static const int DO_CHECK_POINT = 55;
    static const int DROP_CURRENT_MERGE = 56;
    static const int CREATE_NEW_TABLE = 57;

    int ObRootMain::do_work()
    {
      //add signal I want to catch
      // we don't process the following signals any more, but receive them for backward compatibility
      add_signal_catched(START_REPORT_SIG);
      add_signal_catched(START_MERGE_SIG);
      add_signal_catched(DUMP_ROOT_TABLE_TO_LOG);
      add_signal_catched(DUMP_AVAILABLE_SEVER_TO_LOG);
      add_signal_catched(SWITCH_SCHEMA);
      add_signal_catched(RELOAD_CONFIG);
      add_signal_catched(DO_CHECK_POINT);
      add_signal_catched(DROP_CURRENT_MERGE);
      add_signal_catched(CREATE_NEW_TABLE);

      int ret = OB_SUCCESS;
      if (ret == OB_SUCCESS)
      {
        ret = worker.set_config_file_name(config_file_name_);
      }
      if (ret == OB_SUCCESS)
      {
        worker.set_packet_factory(&packet_factory_);
      }
      if (ret == OB_SUCCESS)
      {
        ret = worker.start();
      }
      if (OB_SUCCESS != ret)
      {
        fprintf(stderr, "failed to start rootserver, see the log for details, err=%d\n", ret);
      }
      return ret;
    }
    void ObRootMain::do_signal(const int sig)
    {
      switch(sig)
      {
        case SIGTERM:
        case SIGINT:
          TBSYS_LOG(INFO, "stop signal received");
          worker.stop();
          break;
        default:
          TBSYS_LOG(WARN, "unknown signal ignored, sig=%d", sig);
          break;
      }
    }
  }
}
