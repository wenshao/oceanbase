/*
 *   (C) 2007-2010 Taobao Inc.
 *   
 *         
 *   Version: 0.1 2010-8-12
 *           
 *   Authors:
 *      qushan <qushan@taobao.com>
 *               
 */
#ifndef OCEANBASE_CHUNKSERVER_CHUNKSERVERMAIN_H_
#define OCEANBASE_CHUNKSERVER_CHUNKSERVERMAIN_H_

#include "common/base_main.h"
#include "ob_chunk_server.h"

extern const char* svn_version();
extern const char* build_date();
extern const char* build_time();

namespace oceanbase 
{ 
  namespace chunkserver
  {


    class ObChunkServerMain : public common::BaseMain
    {
      protected:
        ObChunkServerMain();
      protected:
        virtual int do_work();
        virtual void do_signal(const int sig);
      public:
        static ObChunkServerMain* get_instance();
      public:
        const ObChunkServer& get_chunk_server() const { return server_ ; }
        ObChunkServer& get_chunk_server() { return server_ ; }
      protected:
        virtual void print_version();
      private:
        ObChunkServer server_;
    };


#define THE_CHUNK_SERVER ObChunkServerMain::get_instance()->get_chunk_server()

  } // end namespace chunkserver
} // end namespace oceanbase


#endif //OCEANBASE_CHUNKSERVER_CHUNKSERVERMAIN_H_

