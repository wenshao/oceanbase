/*
 * Copyright (C) 2007-2011 Taobao Inc.
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
 *   Yudi Shi <fufeng.syd@taobao.com>
 *     - some work details here
 */

#ifndef _OB_CS_ADMIN2_H_
#define _OB_CS_ADMIN2_H_

#include <string>
#include <map>
#include <iostream>
#include "common/ob_base_client.h"
#include "common/ob_obi_role.h"
#include "client_rpc.h"
#include "base_client.h"

using namespace std;
using namespace oceanbase;
using namespace oceanbase::common;
// using namespace oceanbase::chunkserver;
// using namespace oceanbase::sstable;
using namespace oceanbase::tools;


namespace oceanbase {
  namespace tools {
    enum
    {
      CMD_MIN,
      CMD_DUMP_TABLET = 1,
      CMD_DUMP_SERVER_STATS = 2,
      CMD_DUMP_CLUSTER_STATS = 3,
      CMD_MANUAL_MERGE = 4,
      CMD_MANUAL_DROP_TABLET = 5,
      CMD_MANUAL_GC = 6,
      CMD_RS_GET_TABLET_INFO = 7,
      CMD_SCAN_ROOT_TABLE = 8,
      CMD_MAX ,
    };

    enum CS_ADMIN_OPT
    {
      OPT_HOST,
      OPT_PORT,
      OPT_ACTION,

      OPT_DISK_NO,

      OPT_SERVER_TYPE,
      OPT_INTERVAL,
      OPT_RUN_COUNT,
      OPT_SHOW_HEADER_COUNT,
      OPT_TABLE_FILTER,
      OPT_INDEX_FILTER,

      OPT_MEMTABLE_FROZEN_VERSION,
      OPT_INIT_FLAG,

      OPT_RESERVE_COPY_COUNT,

      OPT_TABLE_ID,
      OPT_TABLE_NAME,
      OPT_RANGE,
    };

    enum StatsObjectType
    {
      SOT_SERVER_STATS = 1,
      SOT_CLUSTER_STATS = 2,
    };

    extern char * OPTIONS[];
    class Arguments
    {
      public:
        Arguments() {}
        ~Arguments() {}

        const string get(CS_ADMIN_OPT cao) const {
          map<string, string>::const_iterator it;
          if (args_.end() != (it = args_.find(OPTIONS[cao])))
            return it->second;
          return "";
        }
        bool set(CS_ADMIN_OPT cao, const string& v) {
          return args_.insert(pair<string,string>(OPTIONS[cao], v)).second;
        }
        bool set(const string& k, const string& v) {
          return args_.insert(pair<string,string>(k, v)).second;
        }

        struct Command
        {
          typedef int (*CmdHandler)(const Arguments &args);
          const char* cmdstr;
          int pcode;
          CmdHandler handler;
        } command;

      void print() const {
        map<string, string>::const_iterator it;
        for (it = args_.begin(); it != args_.end(); ++it)
        {
          cout << it->first << " : " << it->second << endl;
        }
        cout << endl;
      };

      void adjust_range_str() {
        const string& range = get(OPT_RANGE);
        if (range.empty())
          return;
        int pos = range.find("~");
        args_[OPTIONS[OPT_RANGE]].replace(pos, 1, ",");
      }

      private:
        map<string, string> args_;
    };

    typedef Arguments::Command Command;
    typedef int (*CmdHandler)(const Arguments &args);

    extern Command COMMANDS[];

    int do_dump_tablet_image(const Arguments &args);
    int do_dump_server_stats(const Arguments &args);
    int do_dump_cluster_stats(const Arguments &args);
    int do_manual_merge(const Arguments &args);
    int do_manual_drop_tablet(const Arguments &args);
    int do_manual_gc(const Arguments &args);
    int do_get_tablet_info(const Arguments &args);
    int do_scan_root_table(const Arguments &args);

    struct Arguments_ori
    {
      string host;
      int port;
      string action;

      // dump_tablet_image
      int disk_no;

      // dump_server_stats
      // dump_cluster_stats
      int32_t server_type;
      int32_t interval;          // default: 1;
      int32_t run_count;         // default: 0;
      int32_t show_header_count; // default: 50;
      string table_filter;
      string index_filter;

      // manual_merge
      int64_t memtable_frozen_version;
      int32_t init_flag;        // default: 0

      // manual_drop_tablet
      // int64_t memtable_frozen_version;

      // manual_gc
      int32_t reserve_copy_count; // default: 3

      // get_tablet_info
      int64_t table_id;
      const char* table_name;
      const char* range_str;

      // scan_root_tablet
      // const char* table_name;

      Command command;
      const char* rs_host;
      int rs_port;
      const char* cs_host;
      int cs_port;
      int64_t request_timeout_us;
      oceanbase::common::ObiRole obi_role;
      int log_level;
      int stat_key;
      int32_t obi_read_percentage;
      const char* ups_ip;           // ups or rs ip
      int ups_port;                 // ups or rs port
      int32_t ms_read_percentage;
      int32_t cs_read_percentage;
      Arguments_ori()
      {
        command.cmdstr = NULL;
        command.pcode = -1;
        command.handler = NULL;
/*        rs_host = DEFAULT_RS_HOST;
          rs_port = DEFAULT_RS_PORT;
          request_timeout_us = DEFAULT_REQUEST_TIMEOUT_US;
          log_level = INVALID_LOG_LEVEL;
*/
        // stat_key = OB_RS_STAT_COMMON;
        obi_read_percentage = -1;
        ups_ip = NULL;
        ups_port = 0;
        cs_read_percentage = -1;
        ms_read_percentage = -1;
      }
      void print() const;
      public:
        static const int INVALID_LOG_LEVEL = -1;
      private:
        static const char* DEFAULT_RS_HOST;
        static const int DEFAULT_RS_PORT = 2500;
        static const int64_t DEFAULT_REQUEST_TIMEOUT_US = 2000000; // 2s
    };

    class GFactory
    {
      public:
        GFactory();
        ~GFactory(); 
        int initialize(const oceanbase::common::ObServer& chunk_server);
        int start();
        int stop();
        int wait();
        static GFactory& get_instance() { return instance_; }
        inline ObClientRpcStub& get_rpc_stub() { return rpc_stub_; }
        inline BaseClient& get_base_client() { return client_; }
        inline const std::map<std::string, int> & get_cmd_map() const { return cmd_map_; }
      private:
        void init_cmd_map();
        static GFactory instance_;
        ObClientRpcStub rpc_stub_;
        BaseClient client_;
        std::map<std::string, int> cmd_map_;
    };

  } // namespace tools
} // namespace oceanbase
#endif
