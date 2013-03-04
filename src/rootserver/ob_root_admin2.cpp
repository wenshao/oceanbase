/*
 * Copyright (C) 2007-2012 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * rootserver admin tool
 *
 * Version: $Id$
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *     - some work details here
 */

#include "rootserver/ob_root_admin2.h"

#include <unistd.h>
#include <cstdio>
#include <cstdlib>
#include "common/ob_define.h"
#include "common/ob_result.h"
#include "common/serialization.h"
#include "common/ob_obi_config.h"
#include "common/ob_server.h"
#include "common/ob_schema.h"
#include "common/ob_scanner.h"
#include "common/utility.h"
#include "common/ob_get_param.h"
#include "common/ob_schema.h"
#include "common/ob_scanner.h"
#include "common/ob_ups_info.h"
#include "rootserver/ob_root_admin_cmd.h"
#include "rootserver/ob_root_stat_key.h"

using namespace oceanbase::common;
using namespace oceanbase::rootserver;

const char* svn_version();
const char* build_date();
const char* build_time();

namespace oceanbase
{
  namespace rootserver
  {
    void usage()
    {
      printf("Usage: rs_admin -r <rootserver_ip> -p <rootserver_port> -t <request_timeout_us> <command> -o <suboptions>\n");
      printf("\n\t-r <rootserver_ip>\tthe default is `127.0.0.1'\n");
      printf("\t-p <rootserver_port>\tthe default is 2500\n");
      printf("\t-t <request_timeout_us>\tthe default is 10000000(10S)\n");
      printf("\n\t<command>:\n");
      printf("\tstat -o ");
      const char** str = &OB_RS_STAT_KEYSTR[0];
      for (; NULL != *str; ++str)
      {
        printf("%s|", *str);
      }
      printf("\n");
      printf("\tdo_check_point\n");
      printf("\treload_config\n");
      printf("\tlog_move_to_debug\n");
      printf("\tlog_move_to_error\n");
      printf("\tdump_root_table\n");
      printf("\tdump_unusual_tablets\n");
      printf("\tdump_server_info\n");
      printf("\tdump_migrate_info\n");
      printf("\tswitch_schema\n");
      printf("\tforce_cs_report\n");
      printf("\tchange_log_level -o ERROR|WARN|INFO|DEBUG\n");
      printf("\tget_obi_role\n");
      printf("\tclean_daily_merge_tablet_error\n");
      printf("\tset_obi_role -o OBI_SLAVE|OBI_MASTER\n");
      printf("\tget_obi_config\n");
      printf("\tset_obi_config -o read_percentage=<read_percentage>\n");
      printf("\tset_obi_config -o rs_ip=<rs_ip>,rs_port=<rs_port>,read_percentage=<read_percentage>\n");
      printf("\tset_ups_config -o ups_ip=<ups_ip>,ups_port=<ups_port>,ms_read_percentage=<percentage>,cs_read_percentage=<percentage>\n");
      printf("\tset_master_ups_config -o master_master_ups_read_percentage=<percentage>,slave_master_ups_read_percentage=<percentage>\n");
      printf("\tget_master_ups_config\n");
      printf("\tchange_ups_master -o ups_ip=<ups_ip>,ups_port=<ups_port>[,force]\n");
      printf("\timport_tablets -o table_id=<table_id>\n");
      printf("\trefresh_schema\n");
      printf("\tprint_schema\n");
      printf("\tprint_root_table -o table_id=<table_id>\n");
      printf("\tenable_balance|disable_balance\n");
      printf("\tenable_rereplication|disable_rereplication\n");
      printf("\tshutdown -o server_list=<ip1+ip2+...+ipn>[,cancel]\n");
      printf("\trestart_cs -o server_list=<ip1+ip2+...+ipn>[,cancel]\n");
      printf("\tdump_cs_tablet_info -o cs_ip=<cs_ip>,cs_port=<cs_port>\n");
      printf("\tcheck_tablet_version -o tablet_version=<tablet_version>\n");
      printf("\tsplit_tablet -o table_id=<table_id> -o table_version=<table_version>\n");
      printf("\n\t-h\tprint this help message\n");
      printf("\t-V\tprint the version\n");
      printf("\nSee `rs_admin.log' for the detailed execution log.\n");
    }

    void version()
    {
      printf("rs_admin (%s %s)\n", PACKAGE_STRING, RELEASEID);
      printf("SVN_VERSION: %s\n", svn_version());
      printf("BUILD_TIME: %s %s\n\n", build_date(), build_time());
      printf("Copyright (c) 2007-2011 Taobao Inc.\n");
    }

    const char* Arguments::DEFAULT_RS_HOST = "127.0.0.1";

    // define all commands string and its pcode here
    Command COMMANDS[] = {
      {
        "get_obi_role", OB_GET_OBI_ROLE, do_get_obi_role
      }
      ,
        {
          "set_obi_role", OB_SET_OBI_ROLE, do_set_obi_role
        }
      ,
        {
          "do_check_point", OB_RS_ADMIN_CHECKPOINT, do_rs_admin
        }
      ,
        {
          "reload_config", OB_RS_ADMIN_RELOAD_CONFIG, do_rs_admin
        }
      ,
        {
          "log_move_to_debug", OB_RS_ADMIN_INC_LOG_LEVEL, do_rs_admin
        }
      ,
        {
          "log_move_to_error", OB_RS_ADMIN_DEC_LOG_LEVEL, do_rs_admin
        }
      ,
        {
          "dump_root_table", OB_RS_ADMIN_DUMP_ROOT_TABLE, do_rs_admin
        }
      ,
        {
          "dump_server_info", OB_RS_ADMIN_DUMP_SERVER_INFO, do_rs_admin
        }
      ,
        {
          "dump_migrate_info", OB_RS_ADMIN_DUMP_MIGRATE_INFO, do_rs_admin
        }
      ,
        {
          "switch_schema", OB_RS_ADMIN_SWITCH_SCHEMA, do_rs_admin
        }
      ,
        {
          "dump_unusual_tablets", OB_RS_ADMIN_DUMP_UNUSUAL_TABLETS, do_rs_admin
        }
      ,
        {
          "refresh_schema", OB_RS_ADMIN_REFRESH_SCHEMA, do_rs_admin
        }
      ,
        {
          "clean_daily_merge_tablet_error", OB_RS_ADMIN_CLEAN_ERROR_MSG, do_rs_admin
        }
      ,
        {
          "change_log_level", OB_CHANGE_LOG_LEVEL, do_change_log_level
        }
      ,
        {
          "stat", OB_RS_STAT, do_rs_stat
        }
      ,
        {
          "get_obi_config", OB_GET_OBI_CONFIG, do_get_obi_config
        }
      ,
        {
          "get_master_ups_config", OB_GET_MASTER_UPS_CONFIG, do_get_master_ups_config
        }
      ,
        {
          "set_obi_config", OB_SET_OBI_CONFIG, do_set_obi_config
        }
      ,
        {
          "set_ups_config", OB_SET_UPS_CONFIG, do_set_ups_config
        }
      ,
        {
          "set_master_ups_config", OB_SET_MASTER_UPS_CONFIG, do_set_master_ups_config
        }
      ,
        {
          "change_ups_master", OB_CHANGE_UPS_MASTER, do_change_ups_master
        }
      ,
        {
          "import_tablets", OB_CS_IMPORT_TABLETS, do_import_tablets
        }
      ,
        {
          "print_schema", OB_FETCH_SCHEMA, do_print_schema
        }
      ,
        {
          "print_root_table", OB_GET_REQUEST, do_print_root_table
        }
      ,
        {
          "enable_balance", OB_RS_ADMIN_ENABLE_BALANCE, do_rs_admin
        }
      ,
        {
          "disable_balance", OB_RS_ADMIN_DISABLE_BALANCE, do_rs_admin
        }
      ,
        {
          "enable_rereplication", OB_RS_ADMIN_ENABLE_REREPLICATION, do_rs_admin
        }
      ,
        {
          "disable_rereplication", OB_RS_ADMIN_DISABLE_REREPLICATION, do_rs_admin
        }
      ,
        {
          "shutdown", OB_RS_SHUTDOWN_SERVERS, do_shutdown_servers
        }
      ,
        {
          "restart_cs", OB_RS_RESTART_SERVERS, do_restart_servers
        }
      ,
        {
          "dump_cs_tablet_info" , OB_RS_DUMP_CS_TABLET_INFO, do_dump_cs_tablet_info
        }
      ,
        {
          "check_tablet_version", OB_RS_CHECK_TABLET_MERGED, do_check_tablet_version
        }
      ,
        {
          "force_cs_report", OB_RS_FORCE_CS_REPORT, do_force_cs_report
        },
        {
          "split_tablet", OB_RS_SPLIT_TABLET, do_split_tablet
        }
    };

    enum 
    {
      OPT_OBI_ROLE_MASTER = 0,
      OPT_OBI_ROLE_SLAVE = 1,
      OPT_LOG_LEVEL_ERROR = 2,
      OPT_LOG_LEVEL_WARN = 3,
      OPT_LOG_LEVEL_INFO = 4,
      OPT_LOG_LEVEL_DEBUG = 5,
      OPT_READ_PERCENTAGE = 6,
      OPT_UPS_IP = 7,
      OPT_UPS_PORT = 8,
      OPT_MS_READ_PERCENTAGE = 9,
      OPT_CS_READ_PERCENTAGE = 10,
      OPT_RS_IP = 11,
      OPT_RS_PORT = 12,
      OPT_TABLE_ID = 13,
      OPT_FORCE = 14,
      OPT_SERVER_LIST = 15,
      OPT_CANCEL = 16,
      OPT_CS_IP = 17,
      OPT_CS_PORT = 18,
      OPT_TABLET_VERSION = 19,
      OPT_TABLE_VERSION = 20,
      OPT_READ_MASTER_MASTER_UPS_PERCENTAGE = 21,
      OPT_READ_SLAVE_MASTER_UPS_PERCENTAGE = 22,
      THE_END
    };

    // 需要与上面的enum一一对应
    const char* SUB_OPTIONS[] = 
    {
      "OBI_MASTER",
      "OBI_SLAVE",
      "ERROR",
      "WARN",
      "INFO",
      "DEBUG",
      "read_percentage",            // 6
      "ups_ip",                     // 7
      "ups_port",                   // 8
      "ms_read_percentage",         // 9
      "cs_read_percentage",         // 10
      "rs_ip",                      // 11
      "rs_port",                    // 12
      "table_id",                   // 13
      "force",                      // 14
      "server_list",                // 15
      "cancel",                     // 16
      "cs_ip",                      //17
      "cs_port",                    //18
      "tablet_version",             //19
      "table_version",
      "master_master_ups_read_percentage", //21
      "slave_master_ups_read_percentage", //22
      NULL
    };

    int parse_cmd_line(int argc, char* argv[], Arguments &args)
    {
      int ret = OB_SUCCESS;
      // merge SUB_OPTIONS and OB_RS_STAT_KEYSTR
      int key_num = 0;
      const char** str = &OB_RS_STAT_KEYSTR[0];
      for (; NULL != *str; ++str)
      {
        key_num++;
      }
      int local_num = ARRAYSIZEOF(SUB_OPTIONS);
      const char** all_sub_options = new(std::nothrow) const char*[key_num+local_num];
      if (NULL == all_sub_options)
      {
        printf("no memory\n");
        ret = OB_ALLOCATE_MEMORY_FAILED;
      }
      else
      {
        memset(all_sub_options, 0, sizeof(all_sub_options));
        for (int i = 0; i < local_num; ++i)
        {
          all_sub_options[i] = SUB_OPTIONS[i];
        }
        for (int i = 0; i < key_num; ++i)
        {
          all_sub_options[i+local_num-1] = OB_RS_STAT_KEYSTR[i];
        }
        all_sub_options[local_num+key_num-1] = NULL;

        char *subopts = NULL;
        char *value = NULL;
        int ch = -1;
        int suboptidx = 0;
        while(-1 != (ch = getopt(argc, argv, "hVr:p:o:t:")))
        {
          switch(ch)
          {
            case '?':
              usage();
              exit(-1);
              break;
            case 'h':
              usage();
              exit(0);
              break;
            case 'V':
              version();
              exit(0);
              break;
            case 'r':
              args.rs_host = optarg;
              break;
            case 'p':
              args.rs_port = atoi(optarg);
              break;
            case 't':
              args.request_timeout_us = atoi(optarg);
              break;
            case 'o':
              subopts = optarg;
              while('\0' != *subopts)
              {
                switch(suboptidx = getsubopt(&subopts, (char* const*)all_sub_options, &value))
                {
                  case -1:
                    break;
                  case OPT_OBI_ROLE_MASTER:
                    args.obi_role.set_role(ObiRole::MASTER);
                    break;
                  case OPT_OBI_ROLE_SLAVE:
                    args.obi_role.set_role(ObiRole::SLAVE);
                    break;
                  case OPT_LOG_LEVEL_ERROR:
                    args.log_level = TBSYS_LOG_LEVEL_ERROR;
                    break;
                  case OPT_LOG_LEVEL_WARN:
                    args.log_level = TBSYS_LOG_LEVEL_WARN;
                    break;
                  case OPT_LOG_LEVEL_INFO:
                    args.log_level = TBSYS_LOG_LEVEL_INFO;
                    break;
                  case OPT_LOG_LEVEL_DEBUG:
                    args.log_level = TBSYS_LOG_LEVEL_DEBUG;
                    break;
                  case OPT_READ_PERCENTAGE:
                    if (NULL == value)
                    {
                      printf("read_percentage needs an argument value\n");
                      ret = OB_INVALID_ARGUMENT;
                    }
                    else 
                    {
                      args.obi_read_percentage = atoi(value);
                    }
                    break;
                  case OPT_UPS_IP:
                    if (NULL == value)
                    {
                      printf("option `ups' needs an argument value\n");
                      ret = OB_INVALID_ARGUMENT;
                    }
                    else 
                    {
                      args.ups_ip = value;
                    }
                    break;
                  case OPT_UPS_PORT:
                    if (NULL == value)
                    {
                      printf("option `port' needs an argument value\n");
                      ret = OB_INVALID_ARGUMENT;
                    }
                    else 
                    {
                      args.ups_port = atoi(value);
                    }
                    break;
                  case OPT_CS_IP:
                    if (NULL == value)
                    {
                      printf("option 'cs_ip' needs an argment value \n");
                      ret = OB_INVALID_ARGUMENT;
                    }
                    else
                    {
                      args.cs_ip = value;
                    }
                    break;
                  case OPT_CS_PORT:
                    if (NULL == value)
                    {
                      printf("option `port' needs an argument value\n");
                      ret = OB_INVALID_ARGUMENT;
                    }
                    else
                    {
                      args.cs_port = atoi(value);
                    }
                    break;
                  case OPT_TABLET_VERSION:
                    if (NULL == value)
                    {
                      printf("option 'tablet_version' needs an argument value\n");
                      ret = OB_INVALID_ARGUMENT;
                    }
                    else
                    {
                      args.tablet_version = atoi(value);
                    }
                    break;
                  case OPT_MS_READ_PERCENTAGE:
                    if (NULL == value)
                    {
                      printf("option `ms_read_percentage' needs an argument value\n");
                      ret = OB_INVALID_ARGUMENT;
                    }
                    else 
                    {
                      args.ms_read_percentage = atoi(value);
                    }
                    break;
                  case OPT_CS_READ_PERCENTAGE:
                    if (NULL == value)
                    {
                      printf("option `cs_read_percentage' needs an argument value\n");
                      ret = OB_INVALID_ARGUMENT;
                    }
                    else
                    {
                      args.cs_read_percentage = atoi(value);
                    }
                    break;
                  case OPT_READ_MASTER_MASTER_UPS_PERCENTAGE:
                    if (NULL == value)
                    {
                      printf("option `master_master_ups_read_percentage' needs an argument value\n");
                      ret = OB_INVALID_ARGUMENT;
                    }
                    else
                    {
                      char *end = NULL;
                      int32_t data = strtol(value, &end, 10);
                      if (end != value + strlen(value))
                      {
                        printf("option `master_master_ups_read_percentage' needs an valid integer value, value=%s\n", value);
                        ret = OB_INVALID_ARGUMENT;
                      }
                      else
                      {
                        args.master_master_ups_read_percentage = data;
                      }
                    }
                    break;
                  case OPT_READ_SLAVE_MASTER_UPS_PERCENTAGE:
                    if (NULL == value)
                    {
                      printf("option `slave_master_ups_read_percentage' needs an argument value\n");
                      ret = OB_INVALID_ARGUMENT;
                    }
                    else
                    {
                      char *end = NULL;
                      int32_t data = strtol(value, &end, 10);
                      if (end !=  value + strlen(value))
                      {
                        printf("option `slave_master_ups_read_percentage' needs an valid integer value, value=%s\n", value);
                        ret = OB_INVALID_ARGUMENT;
                      }
                      else
                      {
                        args.slave_master_ups_read_percentage = data;
                      }
                    }
                    break;
                  case OPT_FORCE:
                    args.force_change_ups_master = 1;
                    break;
                  case OPT_RS_IP:
                    if (NULL == value)
                    {
                      printf("option `rs_ip' needs an argument value\n");
                      ret = OB_INVALID_ARGUMENT;
                    }
                    else 
                    {
                      args.ups_ip = value;
                    }
                    break;
                  case OPT_RS_PORT:
                    if (NULL == value)
                    {
                      printf("option `rs_port' needs an argument value\n");
                      ret = OB_INVALID_ARGUMENT;
                    }
                    else 
                    {
                      args.ups_port = atoi(value);
                    }
                    break;
                  case OPT_TABLE_VERSION:
                    if (NULL == value)
                    {
                      printf("option `table_id' needs an argument value\n");
                      ret = OB_INVALID_ARGUMENT;
                    }
                    else 
                    {
                      args.table_version = atoi(value);
                    }
                    break;
                  case OPT_TABLE_ID:
                    if (NULL == value)
                    {
                      printf("option `table_id' needs an argument value\n");
                      ret = OB_INVALID_ARGUMENT;
                    }
                    else 
                    {
                      args.table_id = atoi(value);
                    }
                    break;
                  case OPT_SERVER_LIST:
                    if (NULL == value)
                    {
                      printf("option `server_list' needs an argument value\n");
                      ret = OB_INVALID_ARGUMENT;
                    }
                    else
                    {
                      args.server_list = value;
                    }
                    break;
                  case OPT_CANCEL:
                    args.flag_cancel = 1;
                    break;
                  default:
                    // stat keys
                    args.stat_key = suboptidx - local_num + 1;
                    break;
                }
              }
            default:
              break;
          }
        }
        if (OB_SUCCESS != ret)
        {
          usage();
        }
        else if (optind >= argc)
        {
          printf("no command specified\n");
          usage();
          ret = OB_INVALID_ARGUMENT;
        }
        else
        {
          char* cmd = argv[optind];
          for (uint32_t i = 0; i < ARRAYSIZEOF(COMMANDS); ++i)
          {
            if (0 == strcmp(cmd, COMMANDS[i].cmdstr))
            {
              args.command.cmdstr = cmd;
              args.command.pcode = COMMANDS[i].pcode;
              args.command.handler = COMMANDS[i].handler;
              break;
            }
          }
          if (-1 == args.command.pcode)
          {
            printf("unknown command=%s\n", cmd);
            ret = OB_INVALID_ARGUMENT;
          }
          else
          {
            args.print();
          }
        }
        if (NULL != all_sub_options)
        {
          delete [] all_sub_options;
          all_sub_options = NULL;
        }
      }
      return ret;
    }

    void Arguments::print()
    {
      TBSYS_LOG(INFO, "server_ip=%s port=%d", rs_host, rs_port);
      TBSYS_LOG(INFO, "cmd=%s pcode=%d", command.cmdstr, command.pcode);
      TBSYS_LOG(INFO, "obi_role=%d", obi_role.get_role());
      TBSYS_LOG(INFO, "log_level=%d", log_level);
      TBSYS_LOG(INFO, "stat_key=%d", stat_key);
      TBSYS_LOG(INFO, "obi_read_percentage=%d", obi_read_percentage);
      TBSYS_LOG(INFO, "ups=%s port=%d", ups_ip, ups_port);
      TBSYS_LOG(INFO, "cs=%s port=%d",cs_ip, cs_port);
      TBSYS_LOG(INFO, "ms_read_percentage=%d", ms_read_percentage);
      TBSYS_LOG(INFO, "cs_read_percentage=%d", cs_read_percentage);
      TBSYS_LOG(INFO, "force_change_ups_master=%d", force_change_ups_master);
      TBSYS_LOG(INFO, "table_id=%lu", table_id);
      TBSYS_LOG(INFO, "server_list=%s", server_list);
      TBSYS_LOG(INFO, "tablet_version=%ld", tablet_version);
    }

    int do_get_obi_role(ObBaseClient &client, Arguments &args)
    {
      int ret = OB_SUCCESS;
      printf("get_obi_role...\n");
      static const int32_t MY_VERSION = 1;
      const int buff_size = sizeof(ObPacket) + 32;
      char buff[buff_size];
      ObDataBuffer msgbuf(buff, buff_size);

      if (OB_SUCCESS != (ret = client.send_recv(OB_GET_OBI_ROLE, MY_VERSION, args.request_timeout_us, msgbuf)))
      {
        printf("failed to send request, err=%d\n", ret);
      }
      else
      {
        ObResultCode result_code;
        ObiRole role;
        msgbuf.get_position() = 0;
        if (OB_SUCCESS != (ret = result_code.deserialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
        {
          printf("failed to deserialize response, err=%d\n", ret);
        }
        else if (OB_SUCCESS != (ret = result_code.result_code_))
        {
          printf("failed to get obi role, err=%d\n", result_code.result_code_);
        }
        else if (OB_SUCCESS != (ret = role.deserialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
        {
          printf("failed to deserialized role, err=%d\n", ret);
        }
        else
        {
          printf("obi_role=%s\n", role.get_role_str());
        }
      }
      return ret;
    }

    int do_set_obi_role(ObBaseClient &client, Arguments &args)
    {
      int ret = OB_SUCCESS;
      printf("set_obi_role...role=%d\n", args.obi_role.get_role());
      static const int32_t MY_VERSION = 1;
      const int buff_size = sizeof(ObPacket) + 32;
      char buff[buff_size];
      ObDataBuffer msgbuf(buff, buff_size);
      if (OB_SUCCESS != (ret = args.obi_role.serialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
      {
        printf("failed to serialize role, err=%d\n", ret);
      }
      else if (OB_SUCCESS != (ret = client.send_recv(OB_SET_OBI_ROLE, MY_VERSION, args.request_timeout_us, msgbuf)))
      {
        printf("failed to send request, err=%d\n", ret);
      }
      else
      {
        ObResultCode result_code;
        msgbuf.get_position() = 0;
        if (OB_SUCCESS != (ret = result_code.deserialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
        {
          printf("failed to deserialize response, err=%d\n", ret);
        }
        else if (OB_SUCCESS != (ret = result_code.result_code_))
        {
          printf("failed to set obi role, err=%d\n", result_code.result_code_);
        }
        else
        {
          printf("Okay\n");
        }
      }
      return ret;
    }

    int do_rs_admin(ObBaseClient &client, Arguments &args)
    {
      int ret = OB_SUCCESS;
      printf("do_rs_admin, cmd=%d...\n", args.command.pcode);
      static const int32_t MY_VERSION = 1;
      const int buff_size = sizeof(ObPacket) + 32;
      char buff[buff_size];
      ObDataBuffer msgbuf(buff, buff_size);
      // admin消息的内容为一个int32_t类型，定义在ob_root_admin_cmd.h中
      if (OB_SUCCESS != (ret = serialization::encode_vi32(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), args.command.pcode)))
      {
        printf("failed to serialize, err=%d\n", ret);
      }
      else if (OB_SUCCESS != (ret = client.send_recv(OB_RS_ADMIN, MY_VERSION, args.request_timeout_us, msgbuf)))
      {
        printf("failed to send request, err=%d\n", ret);
      }
      else
      {
        ObResultCode result_code;
        msgbuf.get_position() = 0;
        if (OB_SUCCESS != (ret = result_code.deserialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
        {
          printf("failed to deserialize response, err=%d\n", ret);
        }
        else if (OB_SUCCESS != (ret = result_code.result_code_))
        {
          printf("failed to do_rs_admin, err=%d\n", result_code.result_code_);
        }
        else
        {
          printf("Okay\n");
        }
      }
      return ret;
    }

    int do_change_log_level(ObBaseClient &client, Arguments &args)
    {
      int ret = OB_SUCCESS;
      printf("do_change_log_level, level=%d...\n", args.log_level);
      static const int32_t MY_VERSION = 1;
      const int buff_size = sizeof(ObPacket) + 32;
      char buff[buff_size];
      ObDataBuffer msgbuf(buff, buff_size);
      if (Arguments::INVALID_LOG_LEVEL == args.log_level)
      {
        printf("invalid log level, level=%d\n", args.log_level);
      }
      // change_log_level消息的内容为一个int32_t类型log_level
      else if (OB_SUCCESS != (ret = serialization::encode_vi32(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), args.log_level)))
      {
        printf("failed to serialize, err=%d\n", ret);
      }
      else if (OB_SUCCESS != (ret = client.send_recv(OB_CHANGE_LOG_LEVEL, MY_VERSION, args.request_timeout_us, msgbuf)))
      {
        printf("failed to send request, err=%d\n", ret);
      }
      else
      {
        ObResultCode result_code;
        msgbuf.get_position() = 0;
        if (OB_SUCCESS != (ret = result_code.deserialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
        {
          printf("failed to deserialize response, err=%d\n", ret);
        }
        else if (OB_SUCCESS != (ret = result_code.result_code_))
        {
          printf("failed to change_log_level, err=%d\n", result_code.result_code_);
        }
        else
        {
          printf("Okay\n");
        }
      }
      return ret;
    }

    int do_rs_stat(ObBaseClient &client, Arguments &args)
    {
      int ret = OB_SUCCESS;
      printf("do_rs_stat, key=%d...\n", args.stat_key);
      static const int32_t MY_VERSION = 1;
      char *buff = new(std::nothrow) char[OB_MAX_PACKET_LENGTH];
      ObDataBuffer msgbuf(buff, OB_MAX_PACKET_LENGTH);
      // rs_stat消息的内容为一个int32_t类型，定义在ob_root_stat_key.h中
      if (NULL == buff)
      {
        printf("no memory\n");
      }
      else if (0 > args.stat_key)
      {
        printf("invalid stat_key=%d\n", args.stat_key);
      }
      else if (OB_SUCCESS != (ret = serialization::encode_vi32(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), args.stat_key)))
      {
        printf("failed to serialize, err=%d\n", ret);
      }
      else if (OB_SUCCESS != (ret = client.send_recv(OB_RS_STAT, MY_VERSION, args.request_timeout_us, msgbuf)))
      {
        printf("failed to send request, err=%d\n", ret);
      }
      else
      {
        ObResultCode result_code;
        msgbuf.get_position() = 0;
        if (OB_SUCCESS != (ret = result_code.deserialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
        {
          printf("failed to deserialize response, err=%d\n", ret);
        }
        else if (OB_SUCCESS != (ret = result_code.result_code_))
        {
          printf("failed to do_rs_admin, err=%d\n", result_code.result_code_);
        }
        else
        {
          printf("%s\n", msgbuf.get_data()+msgbuf.get_position());
        }
      }
      if (NULL != buff)
      {
        delete [] buff;
        buff = NULL;
      }
      return ret;
    }
    int do_get_master_ups_config(ObBaseClient &client, Arguments &args)
    {
      int ret = OB_SUCCESS;
      printf("get_master_ups_config...\n");
      static const int32_t MY_VERSION = 1;
      const int buff_size = sizeof(ObPacket) + 32;
      char buff[buff_size];
      ObDataBuffer msgbuf(buff, buff_size);

      if (OB_SUCCESS != (ret = client.send_recv(OB_GET_MASTER_UPS_CONFIG, MY_VERSION, args.request_timeout_us, msgbuf)))
      {
        printf("failed to send request, err=%d\n", ret);
      }
      else
      {
        ObResultCode result_code;
        int32_t master_master_ups_read_percent = 0;
        int32_t slave_master_ups_read_percent = 0;
        msgbuf.get_position() = 0;
        if (OB_SUCCESS != (ret = result_code.deserialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
        {
          printf("failed to deserialize response, err=%d\n", ret);
        }
        else if (OB_SUCCESS != (ret = result_code.result_code_))
        {
          printf("failed to get obi role, err=%d\n", result_code.result_code_);
        }
        else if (OB_SUCCESS != (ret = serialization::decode_vi32(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), &master_master_ups_read_percent)))
        {
          printf("failed to deserialize master_master_ups_read_percent");
        }
        else if (OB_SUCCESS != (ret = serialization::decode_vi32(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), &slave_master_ups_read_percent)))
        {
          printf("failed to deserialize slave_master_ups_read_percent");
        }
        else
        {
          printf("master_master_ups_read_percentage=%d, slave_master_ups_read_percent=%d\n",
              master_master_ups_read_percent, slave_master_ups_read_percent);
        }
      }
      return ret;
    }
    int do_get_obi_config(ObBaseClient &client, Arguments &args)
    {
      int ret = OB_SUCCESS;
      printf("get_obi_config...\n");
      static const int32_t MY_VERSION = 1;
      const int buff_size = sizeof(ObPacket) + 32;
      char buff[buff_size];
      ObDataBuffer msgbuf(buff, buff_size);

      if (OB_SUCCESS != (ret = client.send_recv(OB_GET_OBI_CONFIG, MY_VERSION, args.request_timeout_us, msgbuf)))
      {
        printf("failed to send request, err=%d\n", ret);
      }
      else
      {
        ObResultCode result_code;
        ObiConfig conf;
        msgbuf.get_position() = 0;
        if (OB_SUCCESS != (ret = result_code.deserialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
        {
          printf("failed to deserialize response, err=%d\n", ret);
        }
        else if (OB_SUCCESS != (ret = result_code.result_code_))
        {
          printf("failed to get obi role, err=%d\n", result_code.result_code_);
        }
        else if (OB_SUCCESS != (ret = conf.deserialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
        {
          printf("failed to deserialized role, err=%d\n", ret);
        }
        else
        {
          printf("read_percentage=%d\n", conf.get_read_percentage());
        }
      }
      return ret;    
    }

    int do_set_obi_config(ObBaseClient &client, Arguments &args)
    {
      int ret = OB_SUCCESS;
      printf("set_obi_config, read_percentage=%d rs_port=%d...\n", args.obi_read_percentage, args.ups_port);
      static const int32_t MY_VERSION = 2;

      const int buff_size = sizeof(ObPacket) + 32;
      char buff[buff_size];
      ObDataBuffer msgbuf(buff, buff_size);
      ObiConfig conf;
      conf.set_read_percentage(args.obi_read_percentage);
      ObServer rs_addr;
      if (0 != args.ups_port && !rs_addr.set_ipv4_addr(args.ups_ip, args.ups_port))
      {
        printf("invalid param, addr=%s port=%d\n", args.ups_ip, args.ups_port);
        usage();
        ret = OB_INVALID_ARGUMENT;
      }
      else if (0 > args.obi_read_percentage || 100 < args.obi_read_percentage)
      {
        printf("invalid param, read_percentage=%d\n", args.obi_read_percentage);
        usage();
        ret = OB_INVALID_ARGUMENT;
      }
      else if (OB_SUCCESS != (ret = conf.serialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
      {
        printf("failed to serialize obi_config, err=%d\n", ret);
      }
      else if (OB_SUCCESS != (ret = rs_addr.serialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
      {
        printf("failed to serialize obi_config, err=%d\n", ret);
      }
      else if (OB_SUCCESS != (ret = client.send_recv(OB_SET_OBI_CONFIG, MY_VERSION, args.request_timeout_us, msgbuf)))
      {
        printf("failed to send request, err=%d\n", ret);
      }
      else
      {
        ObResultCode result_code;
        msgbuf.get_position() = 0;
        if (OB_SUCCESS != (ret = result_code.deserialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
        {
          printf("failed to deserialize response, err=%d\n", ret);
        }
        else if (OB_SUCCESS != (ret = result_code.result_code_))
        {
          printf("failed to set obi config, err=%d\n", result_code.result_code_);
        }
        else
        {
          printf("Okay\n");
        }
      }
      return ret;
    }
    int do_set_master_ups_config(ObBaseClient &client, Arguments &args)
    {
      int ret = OB_SUCCESS;
      if (((-1 != args.master_master_ups_read_percentage)
          && (0 > args.master_master_ups_read_percentage
            || 100 < args.master_master_ups_read_percentage))
          || ((-1 != args.slave_master_ups_read_percentage)
          && (0 > args.slave_master_ups_read_percentage
            || 100 < args.slave_master_ups_read_percentage)))
      {
        printf("invalid param, master_master_ups_read_percentage=%d, slave_master_ups_read_percentage=%d\n",
            args.master_master_ups_read_percentage, args.slave_master_ups_read_percentage);
        usage();
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        printf("set_master_ups_config, master_master_ups_read_percentage=%d,read_salve_master_ups_percentage=%d...\n",
            args.master_master_ups_read_percentage, args.slave_master_ups_read_percentage);

        static const int32_t MY_VERSION = 1;
        const int buff_size = sizeof(ObPacket) + 128;
        char buff[buff_size];
        ObDataBuffer msgbuf(buff, buff_size);
        if (OB_SUCCESS != (ret = serialization::encode_vi32(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), args.master_master_ups_read_percentage)))
        {
          printf("failed to serialize read_percentage, err=%d\n", ret);
        }
        else if (OB_SUCCESS != (ret = serialization::encode_vi32(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), args.slave_master_ups_read_percentage)))
        {
          printf("failed to serialize read_percentage, err=%d\n", ret);
        }
        else if (OB_SUCCESS != (ret = client.send_recv(OB_SET_MASTER_UPS_CONFIG, MY_VERSION, args.request_timeout_us, msgbuf)))
        {
          printf("failed to send request, err=%d\n", ret);
        }
        else
        {
          ObResultCode result_code;
          msgbuf.get_position() = 0;
          if (OB_SUCCESS != (ret = result_code.deserialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
          {
            printf("failed to deserialize response, err=%d\n", ret);
          }
          else if (OB_SUCCESS != (ret = result_code.result_code_))
          {
            printf("failed to set master ups config, err=%d\n", result_code.result_code_);
          }
          else
          {
            printf("Okay\n");
          }
        }
      }
      return ret;
    }

    int do_set_ups_config(ObBaseClient &client, Arguments &args)
    {
      int ret = OB_SUCCESS;
      common::ObServer ups_addr;
      if (0 > args.ms_read_percentage || 100 < args.ms_read_percentage)
      {
        printf("invalid param, ms_read_percentage=%d\n", args.ms_read_percentage);
        usage();
        ret = OB_INVALID_ARGUMENT;
      }
      else if (0 > args.cs_read_percentage || 100 < args.cs_read_percentage)
      {
        printf("invalid param, cs_read_percentage=%d\n", args.cs_read_percentage);
        usage();
        ret = OB_INVALID_ARGUMENT;
      }
      else if (NULL == args.ups_ip)
      {
        printf("invalid param, ups_ip is NULL\n");
        usage();
        ret = OB_INVALID_ARGUMENT;
      }
      else if (0 >= args.ups_port)
      {
        printf("invalid param, ups_port=%d\n", args.ups_port);
        usage();
        ret = OB_INVALID_ARGUMENT;
      }
      else if (!ups_addr.set_ipv4_addr(args.ups_ip, args.ups_port))
      {
        printf("invalid param, ups_host=%s ups_port=%d\n", args.ups_ip, args.ups_port);
        usage();
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        printf("set_ups_config, ups_ip=%s ups_port=%d ms_read_percentage=%d cs_read_percentage=%d...\n", 
            args.ups_ip, args.ups_port, args.ms_read_percentage, args.cs_read_percentage);

        static const int32_t MY_VERSION = 1;
        const int buff_size = sizeof(ObPacket) + 128;
        char buff[buff_size];
        ObDataBuffer msgbuf(buff, buff_size);

        if (OB_SUCCESS != (ret = ups_addr.serialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
        {
          printf("failed to serialize ups_addr, err=%d\n", ret);
        }
        else if (OB_SUCCESS != (ret = serialization::encode_vi32(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), args.ms_read_percentage)))
        {
          printf("failed to serialize read_percentage, err=%d\n", ret);
        }
        else if (OB_SUCCESS != (ret = serialization::encode_vi32(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), args.cs_read_percentage)))
        {
          printf("failed to serialize read_percentage, err=%d\n", ret);
        }
        else if (OB_SUCCESS != (ret = client.send_recv(OB_SET_UPS_CONFIG, MY_VERSION, args.request_timeout_us, msgbuf)))
        {
          printf("failed to send request, err=%d\n", ret);
        }
        else
        {
          ObResultCode result_code;
          msgbuf.get_position() = 0;
          if (OB_SUCCESS != (ret = result_code.deserialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
          {
            printf("failed to deserialize response, err=%d\n", ret);
          }
          else if (OB_SUCCESS != (ret = result_code.result_code_))
          {
            printf("failed to set obi config, err=%d\n", result_code.result_code_);
          }
          else
          {
            printf("Okay\n");
          }
        }
      }
      return ret;
    }

    int do_dump_cs_tablet_info(ObBaseClient &client, Arguments &args)
    {
      int ret = OB_SUCCESS;
      static const int32_t MY_VERSION = 1;
      const int buff_size = sizeof(ObPacket) + 128;
      char buff[buff_size];
      ObDataBuffer msgbuf(buff, buff_size);

      common::ObServer cs_addr;
      if (NULL == args.cs_ip)
      {
        printf("invalid param, cs_ip is NULL\n");
        usage();
        ret = OB_INVALID_ARGUMENT;
      }
      else if (0 >= args.cs_port)
      {
        printf("invalid param, cs_port is %d\n", args.cs_port);
        usage();
        ret = OB_INVALID_ARGUMENT;
      }
      else if (!cs_addr.set_ipv4_addr(args.cs_ip, args.cs_port))
      {
        printf("invalid param, cs_host=%s cs_port=%d\n", args.cs_ip, args.cs_port);
        usage();
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        printf("dump chunkserver tablet info, cs_ip=%s, cs_port=%d...\n", args.cs_ip, args.cs_port);
        if (OB_SUCCESS != (ret = cs_addr.serialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
        {
          printf("failed to serialize cs_addr, err=%d", ret);
        }
        else if (OB_SUCCESS != (ret = client.send_recv(OB_RS_DUMP_CS_TABLET_INFO, MY_VERSION, args.request_timeout_us, msgbuf)))
        {
          printf("failed to send request, err = %d", ret);
        }
        else
        {
          ObResultCode result_code;
          msgbuf.get_position() = 0;
          if (OB_SUCCESS != (ret = result_code.deserialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
          {
            printf("failed to deserialize response code, err=%d", ret);
          }
          else if (OB_SUCCESS != (ret = result_code.result_code_))
          {
            printf("failed to dump cs tablet info. err=%d", ret);
          }
          else
          {
            printf("Okay\n");
            int64_t tablet_num = 0;
            if (OB_SUCCESS != (ret = serialization::decode_vi64(
                    msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), &tablet_num)))
            {
              printf("failed to decode tablet_num, err=%d", ret);
            }
            else
            {
              printf("tablet_num=%ld\n", tablet_num);
            }
          }
        }
      }
      return ret;
    }

    int do_change_ups_master(ObBaseClient &client, Arguments &args)
    {
      int ret = OB_SUCCESS;
      static const int32_t MY_VERSION = 1;
      const int buff_size = sizeof(ObPacket) + 128;
      char buff[buff_size];  
      ObDataBuffer msgbuf(buff, buff_size);

      common::ObServer ups_addr;
      if (NULL == args.ups_ip)
      {
        printf("invalid param, ups_ip is NULL\n");
        usage();
        ret = OB_INVALID_ARGUMENT;
      }
      else if (0 >= args.ups_port)
      {
        printf("invalid param, ups_port=%d\n", args.ups_port);
        usage();
        ret = OB_INVALID_ARGUMENT;
      }
      else if (!ups_addr.set_ipv4_addr(args.ups_ip, args.ups_port))
      {
        printf("invalid param, ups_host=%s ups_port=%d\n", args.ups_ip, args.ups_port);
        usage();
        ret = OB_INVALID_ARGUMENT;
      }
      else if (0 != args.force_change_ups_master && 1 != args.force_change_ups_master)
      {
        printf("invalid param, force=%d\n", args.force_change_ups_master);
        usage();
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        printf("change_ups_master, ups_ip=%s ups_port=%d force=%d...\n", args.ups_ip, args.ups_port, args.force_change_ups_master);
        if (OB_SUCCESS != (ret = ups_addr.serialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
        {
          printf("failed to serialize ups_addr, err=%d\n", ret);
        }
        else if (OB_SUCCESS != (ret = serialization::encode_vi32(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), args.force_change_ups_master)))
        {
          printf("failed to serialize int32, err=%d\n", ret);
        }
        else if (OB_SUCCESS != (ret = client.send_recv(OB_CHANGE_UPS_MASTER, MY_VERSION, args.request_timeout_us, msgbuf)))
        {
          printf("failed to send request, err=%d\n", ret);
        }
        else
        {
          ObResultCode result_code;
          msgbuf.get_position() = 0;
          if (OB_SUCCESS != (ret = result_code.deserialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
          {
            printf("failed to deserialize response, err=%d\n", ret);
          }
          else if (OB_SUCCESS != (ret = result_code.result_code_))
          {
            printf("failed to change ups master, err=%d\n", result_code.result_code_);
          }
          else
          {
            printf("Okay\n");
          }
        }
      }
      return ret;
    }

    int do_import_tablets(ObBaseClient &client, Arguments &args)
    {
      int ret = OB_SUCCESS;
      printf("import_tablets...table_id=%lu\n", args.table_id);
      static const int32_t MY_VERSION = 1;
      const int buff_size = sizeof(ObPacket) + 32;
      char buff[buff_size];
      ObDataBuffer msgbuf(buff, buff_size);
      int64_t import_version = 0;
      if (OB_INVALID_ID == args.table_id)
      {
        printf("invalid table_id\n");
        usage();
        ret = OB_INVALID_ARGUMENT;
      }
      else if (OB_SUCCESS != (ret = common::serialization::encode_vi64(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), args.table_id)))
      {
        printf("failed to serialize, err=%d\n", ret);
      }
      else if (OB_SUCCESS != (ret = common::serialization::encode_vi64(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), import_version)))
      {
        printf("failed to serialize, err=%d\n", ret);
      }
      else if (OB_SUCCESS != (ret = client.send_recv(OB_CS_IMPORT_TABLETS, MY_VERSION, args.request_timeout_us*20, msgbuf)))
      {
        printf("failed to send request, err=%d\n", ret);
      }
      else
      {
        ObResultCode result_code;
        int64_t pos = 0;
        if (OB_SUCCESS != (ret = result_code.deserialize(msgbuf.get_data(), msgbuf.get_position(), pos)))
        {
          printf("failed to deserialize response, err=%d\n", ret);
        }
        else if (OB_SUCCESS != (ret = result_code.result_code_))
        {
          printf("failed to import tablets, err=%d\n", result_code.result_code_);
        }
        else
        {
          printf("Okay\n");
        }
      }
      return ret;
    }

    int do_print_schema(ObBaseClient &client, Arguments &args)
    {
      int ret = OB_SUCCESS;
      printf("do_print_schema...\n");
      static const int32_t MY_VERSION = 1;
      char *buff = new(std::nothrow) char[OB_MAX_PACKET_LENGTH];
      ObDataBuffer msgbuf(buff, OB_MAX_PACKET_LENGTH);
      if (NULL == buff)
      {
        printf("no memory\n");
      }
      else if (OB_SUCCESS != (ret = client.send_recv(args.command.pcode, MY_VERSION, args.request_timeout_us, msgbuf)))
      {
        printf("failed to send request, err=%d\n", ret);
      }
      else
      {
        ObResultCode result_code;
        common::ObSchemaManagerV2 schema_manager;
        int64_t pos = 0;
        if (OB_SUCCESS != (ret = result_code.deserialize(msgbuf.get_data(), msgbuf.get_position(), pos)))
        {
          printf("failed to deserialize response, err=%d\n", ret);
        }
        else if (OB_SUCCESS != (ret = result_code.result_code_))
        {
          printf("failed to fetch schema, err=%d\n", result_code.result_code_);
        }
        else if (OB_SUCCESS != (ret = schema_manager.deserialize(msgbuf.get_data(), msgbuf.get_position(), pos)))
        {
          printf("failed to deserialize schema manager, err=%d\n", ret);
        }
        else
        {
          schema_manager.print(stdout);      
        }
      }
      if (NULL != buff)
      {
        delete [] buff;
        buff = NULL;
      }
      return ret;
    }

    int do_print_root_table(ObBaseClient &client, Arguments &args)
    {
      int ret = OB_SUCCESS;
      printf("do_print_root_table, table_id=%lu...\n", args.table_id);
      static const int32_t MY_VERSION = 1;
      char *buff = new(std::nothrow) char[OB_MAX_PACKET_LENGTH];
      ObDataBuffer msgbuf(buff, OB_MAX_PACKET_LENGTH);
      char keybuf[1];
      keybuf[0] = '\0';
      ObCellInfo cell;
      cell.table_id_ = args.table_id;
      cell.column_id_ = 0;
      cell.row_key_.assign(keybuf, 1);
      ObGetParam get_param;
      if (OB_INVALID_ID == args.table_id)
      {
        printf("invalid table_id\n");
        usage();
        ret = OB_INVALID_ARGUMENT;
      }
      else if (OB_SUCCESS != (ret = get_param.add_cell(cell)))
      {
        printf("failed to add cell to get_param, err=%d\n", ret);
      }
      else if (NULL == buff)
      {
        printf("no memory\n");
        ret = OB_ALLOCATE_MEMORY_FAILED;
      }
      else if (OB_SUCCESS != (ret = get_param.serialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
      {
        printf("failed to serialize get_param, err=%d\n", ret);
      }
      else if (OB_SUCCESS != (ret = client.send_recv(args.command.pcode, MY_VERSION, args.request_timeout_us, msgbuf)))
      {
        printf("failed to send request, err=%d\n", ret);
      }
      else
      {
        ObResultCode result_code;
        common::ObScanner scanner;
        int64_t pos = 0;
        if (OB_SUCCESS != (ret = result_code.deserialize(msgbuf.get_data(), msgbuf.get_position(), pos)))
        {
          printf("failed to deserialize response, err=%d\n", ret);
        }
        else if (OB_SUCCESS != (ret = result_code.result_code_))
        {
          printf("failed to get root table, err=%d\n", result_code.result_code_);
        }
        else if (OB_SUCCESS != (ret = scanner.deserialize(msgbuf.get_data(), msgbuf.get_position(), pos)))
        {
          printf("failed to deserialize scanner, err=%d\n", ret);
        }
        else
        {
          print_root_table(stdout, scanner);
        }
      }
      if (NULL != buff)
      {
        delete [] buff;
        buff = NULL;
      }  
      return ret;
    }

    static int serialize_servers(const char* server_list, ObDataBuffer &msgbuf)
    {
      int ret = OB_SUCCESS;
      char* servers = NULL;
      char* servers2 = NULL;
      if (NULL == server_list)
      {
        ret = OB_INVALID_ARGUMENT;
      }
      else if (NULL == (servers = strdup(server_list))
          || NULL == (servers2 = strdup(server_list)))
      {
        TBSYS_LOG(ERROR, "no memory");
        ret = OB_ALLOCATE_MEMORY_FAILED;
      }
      else
      {
        char *saveptr = NULL;
        const char* delim = "+";
        // count
        int32_t count = 0;
        char *server = strtok_r(servers, delim, &saveptr);
        while(NULL != server)
        {
          count++;
          server = strtok_r(NULL, delim, &saveptr);
        }
        if (0 >= count)
        {
          ret = OB_INVALID_ARGUMENT;
          TBSYS_LOG(ERROR, "no valid server, servers=%s", server_list);
        }
        else if (OB_SUCCESS != (ret = serialization::encode_vi32(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), count)))
        {
          TBSYS_LOG(ERROR, "serialize error");
        }
        else
        {
          // serialize
          ObServer addr;
          const int ignored_port = 1;
          server = strtok_r(servers2, delim, &saveptr);
          while(NULL != server)
          {
            if (!addr.set_ipv4_addr(server, ignored_port))
            {
              ret = OB_INVALID_ARGUMENT;
              TBSYS_LOG(ERROR, "invalid addr, server=%s", server);
              break;
            }
            else if (OB_SUCCESS != (ret = addr.serialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
            {
              TBSYS_LOG(ERROR, "serialize error");
              break;
            }
            else
            {
              TBSYS_LOG(INFO, "server=%s", server);
            }
            server = strtok_r(NULL, delim, &saveptr);
          }
        }
      }
      if (NULL != servers)
      {
        free(servers);
        servers = NULL;
      }
      if (NULL != servers2)
      {
        free(servers2);
        servers2 = NULL;
      }  
      return ret;
    }

    int do_restart_servers(ObBaseClient &client, Arguments &args)
    {
      int ret = OB_SUCCESS;
      printf("do_restart_servers, server_list=%s cancel=%d...\n", 
          args.server_list, args.flag_cancel);
      static const int32_t MY_VERSION = 1;
      char *buff = new(std::nothrow) char[OB_MAX_PACKET_LENGTH];
      ObDataBuffer msgbuf(buff, OB_MAX_PACKET_LENGTH);

      if (NULL == args.server_list)
      {
        printf("invalid NULL server_list\n");
        usage();
        ret = OB_INVALID_ARGUMENT;
      }
      else if (NULL == buff)
      {
        printf("no memory\n");
        ret = OB_ALLOCATE_MEMORY_FAILED;
      }

      if(OB_SUCCESS == ret)
      {
        if(0 == strcmp(args.server_list, "all"))
        {
          ret = serialization::encode_bool(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), true);
        }
        else
        {
          ret = serialization::encode_bool(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), false);
          if(OB_SUCCESS == ret)
          {
            if(OB_SUCCESS != (ret = serialize_servers(args.server_list, msgbuf)))
            {
              printf("failed to serialize get_param, err=%d\n", ret);
            }
          }
        }
      }

      if(OB_SUCCESS == ret)
      {
        if (OB_SUCCESS != (ret = serialization::encode_vi32(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), args.flag_cancel)))
        {
          TBSYS_LOG(ERROR, "serialize error");
        }
        else if (OB_SUCCESS != (ret = client.send_recv(args.command.pcode, MY_VERSION, args.request_timeout_us, 
                msgbuf)))
        {
          printf("failed to send request, err=%d\n", ret);
        }
        else
        {
          ObResultCode result_code;
          int64_t pos = 0;
          if (OB_SUCCESS != (ret = result_code.deserialize(msgbuf.get_data(), msgbuf.get_position(), pos)))
          {
            printf("failed to deserialize response, err=%d\n", ret);
          }
          else if (OB_SUCCESS != (ret = result_code.result_code_))
          {
            printf("failed to get restart servers, err=%d\n", result_code.result_code_);
          }
          else
          {
            printf("Okay\n");
          }
        }
      }
      if (NULL != buff)
      {
        delete [] buff;
        buff = NULL;
      }  
      return ret;
    }

    int do_shutdown_servers(ObBaseClient &client, Arguments &args)
    {
      int ret = OB_SUCCESS;
      printf("do_shutdown_servers, server_list=%s cancel=%d...\n", 
          args.server_list, args.flag_cancel);
      static const int32_t MY_VERSION = 1;
      char *buff = new(std::nothrow) char[OB_MAX_PACKET_LENGTH];
      ObDataBuffer msgbuf(buff, OB_MAX_PACKET_LENGTH);

      if (NULL == args.server_list)
      {
        printf("invalid NULL server_list\n");
        usage();
        ret = OB_INVALID_ARGUMENT;
      }
      else if (NULL == buff)
      {
        printf("no memory\n");
        ret = OB_ALLOCATE_MEMORY_FAILED;
      }
      else if (OB_SUCCESS != (ret = serialize_servers(args.server_list, msgbuf)))
      {
        printf("failed to serialize get_param, err=%d\n", ret);
      }
      else if (OB_SUCCESS != (ret = serialization::encode_vi32(msgbuf.get_data(), msgbuf.get_capacity(), 
              msgbuf.get_position(), args.flag_cancel)))
      {
        TBSYS_LOG(ERROR, "serialize error");
      }
      else if (OB_SUCCESS != (ret = client.send_recv(args.command.pcode, MY_VERSION, args.request_timeout_us, 
              msgbuf)))
      {
        printf("failed to send request, err=%d\n", ret);
      }
      else
      {
        ObResultCode result_code;
        int64_t pos = 0;
        if (OB_SUCCESS != (ret = result_code.deserialize(msgbuf.get_data(), msgbuf.get_position(), pos)))
        {
          printf("failed to deserialize response, err=%d\n", ret);
        }
        else if (OB_SUCCESS != (ret = result_code.result_code_))
        {
          printf("failed to get shut down servers, err=%d\n", result_code.result_code_);
        }
        else
        {
          printf("Okay\n");
        }
      }
      if (NULL != buff)
      {
        delete [] buff;
        buff = NULL;
      }  
      return ret;
    }

    int do_check_tablet_version(ObBaseClient &client, Arguments &args)
    {
      int err = OB_SUCCESS;
      static const int32_t MY_VERSION = 1;
      const int buff_size = sizeof(ObPacket) + 128;
      char buff[buff_size];
      ObDataBuffer msgbuf(buff, buff_size);

      if (0 >= args.tablet_version)
      {
        printf("invalid param. tablet_version is %ld", args.tablet_version);
        usage();
        err = OB_INVALID_ARGUMENT;
      }
      else
      {
        if (OB_SUCCESS != (err = serialization::encode_vi64(msgbuf.get_data(), msgbuf.get_capacity(),
                msgbuf.get_position(), args.tablet_version)))
        {
          printf("fail to encode tablet_version to buf. err=%d", err);
        }
        else if (OB_SUCCESS != (err = client.send_recv(OB_RS_CHECK_TABLET_MERGED, MY_VERSION, args.request_timeout_us, msgbuf)))
        {
          printf("failed to send request. err=%d", err);
        }
        else
        {
          ObResultCode result_code;
          msgbuf.get_position() = 0;
          if (OB_SUCCESS != (err = result_code.deserialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
          {
            printf("failed to deserialize response code, err=%d", err);
          }
          else if (OB_SUCCESS != (err = result_code.result_code_))
          {
            printf("failed to check tablet version. err=%d", err);
          }
          else
          {
            int64_t is_merged = 0;
            if (OB_SUCCESS != (err = serialization::decode_vi64(msgbuf.get_data(), msgbuf.get_capacity(),
                    msgbuf.get_position(), &is_merged)))
            {
              printf("failed to deserialize is_merged, err=%d", err);
            }
            else
            {
              if (0 == is_merged)
              {
                printf("OKay, check_version=%ld, already have some tablet not reach this version\n",args.tablet_version);
              }
              else
              {
                printf("OKay, check_version=%ld, all tablet reach this version\n", args.tablet_version);
              }
            }
          }
        }
      }
      return err;
    }

    int do_force_cs_report(ObBaseClient &client, Arguments &args)
    {
      int ret = OB_SUCCESS;
      printf("force_cs_report_tablet...\n");
      static const int32_t MY_VERSION = 1;
      const int buff_size = sizeof(ObPacket) + 32;
      char buff[buff_size];
      ObDataBuffer msgbuf(buff, buff_size);
      if (OB_SUCCESS != (ret = client.send_recv(OB_RS_FORCE_CS_REPORT, MY_VERSION, args.request_timeout_us, msgbuf)))
      {
        printf("fail to send request. err=%d\n", ret);
      }
      else
      {
        ObResultCode result_code;
        msgbuf.get_position() = 0;
        if (OB_SUCCESS != (ret = result_code.deserialize(msgbuf.get_data(), msgbuf.get_capacity(),
                msgbuf.get_position())))
        {
          printf("failed to deserialize response, err=%d\n", ret);
        }
        else if (OB_SUCCESS != (ret = result_code.result_code_))
        {
          printf("failed to request cs to report tablet, err=%d\n", result_code.result_code_);
        }
        else
        {
          printf("Okay\n");
        }
      }
      return ret;
    }
 int do_split_tablet(ObBaseClient &client, Arguments &args)
 {
   int err = OB_SUCCESS;
   printf("for split tablet...\n");
   static const int32_t MY_VERSION = 1;
   const int buff_size = sizeof(ObPacket) + 128;
   char buff[buff_size];
   ObDataBuffer msgbuf(buff, buff_size);

   if (OB_SUCCESS != (err = client.send_recv(OB_RS_SPLIT_TABLET, MY_VERSION, args.request_timeout_us, msgbuf)))
   {
     printf("failed to send request. err=%d", err);
   }
   else
   {
     ObResultCode result_code;
     msgbuf.get_position() = 0;
     if (OB_SUCCESS != (err = result_code.deserialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
     {
       printf("failed to deserialize response code, err=%d", err);
     }
     else if (OB_SUCCESS != (err = result_code.result_code_))
     {
       printf("failed to split tablet. err=%d", err);
     }
     else
     {
       printf("OKay");
     }
   }
   return err;
 }

  } // end namespace rootserver
}   // end namespace oceanbase
