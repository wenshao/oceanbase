#include "ob_merge_server_main.h"

namespace oceanbase
{
  namespace mergeserver
  {
    ObMergeServerMain::ObMergeServerMain()
    //ObMergeServerMain::ObMergeServerMain():BaseMain(false)
    {
    }

    ObMergeServerMain * ObMergeServerMain::get_instance()
    {
      if (instance_ == NULL)
      {
        instance_ = new(std::nothrow) ObMergeServerMain();
        if (NULL == instance_)
        {
          TBSYS_LOG(ERROR, "check alloc ObMergeServerMain failed");
        }
      }
      return dynamic_cast<ObMergeServerMain *> (instance_);
    }
    
    int ObMergeServerMain::do_work()
    {
      server_.start();
      // server_.wait_for_queue();
      return common::OB_SUCCESS;
    }

    void ObMergeServerMain::print_version()
    {
      fprintf(stderr, "mergeserver (%s %s)\n", PACKAGE_STRING, RELEASEID);
      fprintf(stderr, "SVN_VERSION: %s\n", svn_version());
      fprintf(stderr, "BUILD_TIME: %s %s\n\n", build_date(), build_time());
      fprintf(stderr, "Copyright (c) 2007-2011 Taobao Inc.\n");
    }

    void ObMergeServerMain::do_signal(const int sig)
    {
      switch (sig)
      {
        case SIGTERM:
        case SIGINT:
          server_.stop();
          break;
        default:
          break;
      }
    }
  } /* mergeserver */
} /* oceanbase */
