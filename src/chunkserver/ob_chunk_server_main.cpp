/*
 *   (C) 2007-2010 Taobao Inc.
 *   
 *   Version: 0.1
 *           
 *   Authors:
 *      qushan <qushan@taobao.com>
 *        - some work details if you want
 *               
 */

#include <new>
#include "ob_chunk_server_main.h"
#include "common/ob_malloc.h"

namespace oceanbase 
{ 
  namespace chunkserver 
  {

    // ----------------------------------------------------------
    // class ObChunkServerMain 
    // ----------------------------------------------------------
    ObChunkServerMain::ObChunkServerMain()
    {
    }


    ObChunkServerMain* ObChunkServerMain::get_instance()
    {
      if (NULL == instance_)
      {
        instance_ = new (std::nothrow) ObChunkServerMain();
      }
      return dynamic_cast<ObChunkServerMain*>(instance_);
    }

    int ObChunkServerMain::do_work()
    {
      server_.start();
      // server_.wait_for_queue();
      return common::OB_SUCCESS;
    }

    void ObChunkServerMain::do_signal(const int sig)
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

    void ObChunkServerMain::print_version()
    {
      fprintf(stderr, "chunkserver (%s %s)\n", PACKAGE_STRING, RELEASEID);
      fprintf(stderr, "SVN_VERSION: %s\n", svn_version());
      fprintf(stderr, "BUILD_TIME: %s %s\n\n", build_date(), build_time());
      fprintf(stderr, "Copyright (c) 2007-2011 Taobao Inc.\n");
    }

  } // end namespace chunkserver
} // end namespace oceanbase

