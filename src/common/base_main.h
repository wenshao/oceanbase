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
#ifndef OCEANBASE_ROOTSERVER_BASE_MAIN_H_
#define OCEANBASE_ROOTSERVER_BASE_MAIN_H_
#include <tbsys.h>

#include "ob_define.h"
namespace oceanbase 
{ 
  namespace common
  { 
    class BaseMain
    {
      public:
        //static BaseMain* instance() {
        //  return instance_;
        //}
        virtual ~BaseMain();
        virtual int start(const int argc, char *argv[], const char* section_name);
        virtual void destroy();

        /* restart server if and only if the *restart server flag* is
         * set. @func destroy should been executed before. */
        static  bool restart_server(int argc, char *argv[]);
        /* set *restart_server* flag, after that invoke restart_server
         * funtion to restart. otherwise restart_server function will
         * do nothing. */
        static  void set_restart_flag(bool restart = true);
      protected:
        BaseMain();
        BaseMain(const bool daemon);
        virtual void do_signal(const int sig);
        void add_signal_catched(const int sig);
        static BaseMain* instance_;
        virtual void print_usage(const char *prog_name);
        virtual void print_version();
        virtual const char* parse_cmd_line(const int argc, char *const argv[]);
        virtual int do_work() = 0;
        char config_file_name_[OB_MAX_FILE_NAME_LENGTH];
      private:
        static void sign_handler(const int sig);
        bool use_deamon_;
        static bool restart_;
    };

  }
}
#endif
