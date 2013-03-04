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
#include <getopt.h>

#include "base_main.h"
#include "ob_define.h"
#include "ob_trace_log.h"
namespace 
{
  const char* PID_FILE = "pid_file";
  const char* LOG_FILE = "log_file";
  const char* DATA_DIR = "data_dir";
  const char* LOG_LEVEL = "log_level";
  const char* TRACE_LOG_LEVEL = "trace_log_level";
  const char* MAX_LOG_FILE_SIZE = "max_log_file_size";
}
namespace oceanbase 
{
  namespace common 
  {
    BaseMain* BaseMain::instance_ = NULL;
    bool BaseMain::restart_ = false;
    BaseMain::BaseMain()
    {
      use_deamon_ = true;
      instance_ = NULL;
      config_file_name_[0] = '\0';
    }
    BaseMain::BaseMain(const bool deamon):use_deamon_(deamon)
    {
      instance_ = NULL;
      config_file_name_[0] = '\0';
    }
    BaseMain::~BaseMain()
    {
    }
    void BaseMain::destroy()
    {
      if (instance_ != NULL)
      {
        delete instance_;
        instance_ = NULL;
      }
    }
    void BaseMain::sign_handler(const int sig)
    {
      TBSYS_LOG(INFO, "receive signal sig=%d", sig);
      switch (sig) {
        case SIGTERM:
        case SIGINT:
          break;
        case 40:
          TBSYS_LOGGER.checkFile();
          break;
        case 41:
        case 42:
          if(sig == 41) {
            TBSYS_LOGGER._level++;
          }
          else {
            TBSYS_LOGGER._level--;
          }
          TBSYS_LOG(INFO, "TBSYS_LOGGER._level: %d", TBSYS_LOGGER._level);
          break;
        case 43:
        case 44:
          if (43 == sig)
          {
            INC_TRACE_LOG_LEVEL();
          }
          else
          {
            DEC_TRACE_LOG_LEVEL();
          }
          break;
      }
      if (instance_ != NULL) instance_->do_signal(sig);
    }
    void BaseMain::do_signal(const int sig)
    {
      UNUSED(sig);
      return;
    }
    void BaseMain::add_signal_catched(const int sig)
    {
      signal(sig, BaseMain::sign_handler);
    }
    const char* BaseMain::parse_cmd_line(const int argc,  char *const argv[])
    {
      int opt = 0;
      const char* opt_string = "hNVf:";
      static char conf_name[256];
      struct option longopts[] = 
      {
        {"config_file", 1, NULL, 'f'},
        {"help", 0, NULL, 'h'},
        {"version", 0, NULL, 'V'},
        {"no_deamon", 0, NULL, 'N'},
        {0, 0, 0, 0}
      };

      const char* config_file = NULL;
      while((opt = getopt_long(argc, argv, opt_string, longopts, NULL)) != -1) {
        switch (opt) {
          case 'f':
            config_file = optarg;
            break;
          case 'V':
            print_version();
            exit(1);
          case 'h':
            print_usage(argv[0]);
            exit(1);
          case 'N':
            use_deamon_ = false;
            break;
          default:
            break;
        }
      }
      if (config_file == NULL) 
      {
        snprintf(conf_name, 256, "%s.conf", argv[0]);
        config_file = conf_name;
      }
      return config_file;
    }
    void BaseMain::print_usage(const char *prog_name)
    {
      fprintf(stderr, "%s -f config_file\n"
          "    -f, --config_file  config file\n"
          "    -h, --help         this help\n"
          "    -V, --version      version\n"
          "    -N, --no_deamon    no deamon\n\n", prog_name);
    }
    void BaseMain::print_version()
    {
      fprintf(stderr, "BUILD_TIME: %s %s\n\n", __DATE__, __TIME__);
    }

    int BaseMain::start(const int argc, char *argv[], const char* section_name)
    {
      const char* config_file = parse_cmd_line(argc, argv);
      if(config_file == NULL) 
      {
        print_usage(argv[0]);
        return EXIT_FAILURE;
      }
      if (static_cast<int>(strlen(config_file)) >= OB_MAX_FILE_NAME_LENGTH)
      {
        fprintf(stderr, "file name too long %s error\n", config_file);
        return EXIT_FAILURE;
      }
      strncpy(config_file_name_, config_file, OB_MAX_FILE_NAME_LENGTH);
      config_file_name_[OB_MAX_FILE_NAME_LENGTH - 1] = '\0';

      if(TBSYS_CONFIG.load(config_file)) 
      {
        fprintf(stderr, "load file %s error\n", config_file);
        return EXIT_FAILURE;
      }

      const char* sz_pid_file =
        TBSYS_CONFIG.getString(section_name, PID_FILE, "server.pid");
      const char* sz_log_file =
        TBSYS_CONFIG.getString(section_name, LOG_FILE, "server.log");
      {
        char *p = NULL;
        char dir_path[256];
        snprintf(dir_path, 256, "%s",
            TBSYS_CONFIG.getString(section_name, DATA_DIR, "./"));
        if(!tbsys::CFileUtil::mkdirs(dir_path)) {
          fprintf(stderr, "create dir %s error\n", dir_path);
          return EXIT_FAILURE;
        }
        snprintf(dir_path, 256, "%s", sz_pid_file);
        p = strrchr(dir_path, '/');
        if(p != NULL)
        {
          *p = '\0';
        }
        if(p != NULL && !tbsys::CFileUtil::mkdirs(dir_path)) {
          fprintf(stderr, "create dir %s error\n", dir_path);
          return EXIT_FAILURE;
        }
        snprintf(dir_path, 256, "%s", sz_log_file);
        p = strrchr(dir_path, '/');
        if(p != NULL)
        {
          *p = '\0';
        }
        if(p != NULL && !tbsys::CFileUtil::mkdirs(dir_path)) {
          fprintf(stderr, "create dir %s error\n", dir_path);
          return EXIT_FAILURE;
        }
      }

      int pid = 0;
      if((pid = tbsys::CProcess::existPid(sz_pid_file))) {
        fprintf(stderr, "program has been exist: pid=%d\n", pid);
        return EXIT_FAILURE;
      }

      const char * sz_log_level =
        TBSYS_CONFIG.getString(section_name, LOG_LEVEL, "info");
      TBSYS_LOGGER.setLogLevel(sz_log_level);
      const char * trace_log_level =
        TBSYS_CONFIG.getString(section_name, TRACE_LOG_LEVEL, "debug");
      SET_TRACE_LOG_LEVEL(trace_log_level); 
      int max_file_size= TBSYS_CONFIG.getInt(section_name, MAX_LOG_FILE_SIZE, 1024);
      TBSYS_LOGGER.setMaxFileSize(max_file_size * 1024L * 1024L);
      int ret = EXIT_SUCCESS;
      bool start_ok = true;

      if (use_deamon_) 
      {
        start_ok = (tbsys::CProcess::startDaemon(sz_pid_file, sz_log_file) == 0);
      }
      if(start_ok) 
      {
        signal(SIGPIPE, SIG_IGN);
        signal(SIGHUP, SIG_IGN);
        add_signal_catched(SIGINT);
        add_signal_catched(SIGTERM);
        add_signal_catched(40);
        add_signal_catched(41);
        add_signal_catched(42);
        add_signal_catched(43);
        add_signal_catched(44);
        //signal(SIGINT, BaseMain::sign_handler);
        //signal(SIGTERM, BaseMain::sign_handler);
        //signal(40, BaseMain::sign_handler);
        //signal(41, BaseMain::sign_handler);
        //signal(42, BaseMain::sign_handler);
        ret = do_work();
        TBSYS_LOG(INFO, "exit program.");
      }

      return ret;
    }

    void BaseMain::set_restart_flag(bool restart)
    {
      restart_ = restart;
    }
    
    bool BaseMain::restart_server(int argc, char* argv[])
    {
      pid_t pid;
      bool ret;

      for (int i = 0; ret && i < argc; i++)
      {
        if (argv[i] == NULL)
        {
          TBSYS_LOG(WARN, "invalid arguments, restart has cancled");
          ret = false;
        }
      }
      
      if (ret &&restart_)
      {
        if (0 == (pid = vfork())) // child
        {
          execv(argv[0], argv);
        }
        else if (-1 == pid)
        {
          // fork fails.
          TBSYS_LOG(ERROR, "fork process error! errno = [%d]", errno);
          ret = false;
        }
        else                      // father
        {
          ret = true;
        }
      }
      return ret;
    }
    
  }
}
