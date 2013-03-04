/*
 * Copyright (C) 2007-2012 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * Description here
 *
 * Version: $Id$
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *     - some work details here
 */
#ifndef _OB_ROOT_ADMIN2_H
#define _OB_ROOT_ADMIN2_H 1

#include "common/ob_base_client.h"
#include "common/ob_obi_role.h"
#include "rootserver/ob_root_stat_key.h"
#include "common/ob_define.h"

namespace oceanbase
{
namespace rootserver
{
using oceanbase::common::ObBaseClient;

struct Arguments;
typedef int (*CmdHandler)(ObBaseClient &client, Arguments &args);

// declare all the handlers here
int do_get_obi_role(ObBaseClient &client, Arguments &args);
int do_set_obi_role(ObBaseClient &client, Arguments &args);
int do_rs_admin(ObBaseClient &client, Arguments &args);
int do_change_log_level(ObBaseClient &client, Arguments &args);
int do_rs_stat(ObBaseClient &client, Arguments &args);
int do_get_obi_config(ObBaseClient &client, Arguments &args);
int do_set_obi_config(ObBaseClient &client, Arguments &args);
int do_set_ups_config(ObBaseClient &client, Arguments &args);
int do_set_master_ups_config(ObBaseClient &client, Arguments &args);
int do_get_master_ups_config(ObBaseClient &client, Arguments &args);
int do_change_ups_master(ObBaseClient &client, Arguments &args);
int do_import_tablets(ObBaseClient &client, Arguments &args);
int do_print_schema(ObBaseClient &client, Arguments &args);
int do_print_root_table(ObBaseClient &client, Arguments &args);
int do_shutdown_servers(ObBaseClient &client, Arguments &args);
int do_restart_servers(ObBaseClient &client, Arguments &args);
int do_dump_cs_tablet_info(ObBaseClient &client, Arguments &args);
int do_check_tablet_version(ObBaseClient &client, Arguments &args);
int do_force_cs_report(ObBaseClient &client, Arguments &args);
int do_split_tablet(ObBaseClient &client, Arguments &args);
struct Command
{
  const char* cmdstr;
  int pcode;
  CmdHandler handler;
};

struct Arguments
{
  Command command;
  const char* rs_host;
  int rs_port;
  int64_t request_timeout_us;
  oceanbase::common::ObiRole obi_role;
  int log_level;
  int stat_key;
  int32_t obi_read_percentage;
  const char* ups_ip;           // ups or rs ip
  int ups_port;                 // ups or rs port
  const char* cs_ip;
  int cs_port;
  int32_t ms_read_percentage;
  int32_t cs_read_percentage;
  int32_t master_master_ups_read_percentage;
  int32_t slave_master_ups_read_percentage;
  int32_t force_change_ups_master;
  uint64_t table_id;
  int64_t table_version;
  const char* server_list;
  int32_t flag_cancel;
  int64_t tablet_version;
  Arguments()
  {
    command.cmdstr = NULL;
    command.pcode = -1;
    command.handler = NULL;
    rs_host = DEFAULT_RS_HOST;
    rs_port = DEFAULT_RS_PORT;
    request_timeout_us = DEFAULT_REQUEST_TIMEOUT_US;
    log_level = INVALID_LOG_LEVEL;
    stat_key = OB_RS_STAT_COMMON;
    obi_read_percentage = -1;
    ups_ip = NULL;
    ups_port = 0;
    cs_ip = NULL;
    cs_port = 0;
    table_version = 0;
    cs_read_percentage = -1;
    ms_read_percentage = -1;
    master_master_ups_read_percentage = -1;
    slave_master_ups_read_percentage = -1;
    force_change_ups_master = 0;
    table_id = common::OB_INVALID_ID;
    server_list = NULL;
    flag_cancel = 0;
  }
  void print();
public:
  static const int INVALID_LOG_LEVEL = -1;
private:
  static const char* DEFAULT_RS_HOST;
  static const int DEFAULT_RS_PORT = 2500;
  static const int64_t DEFAULT_REQUEST_TIMEOUT_US = 10000000; // 10s
};

void usage();
void version();
int parse_cmd_line(int argc, char* argv[], Arguments &args);

} // end namespace rootserver
} // end namespace oceanbase

#endif /* _OB_ROOT_ADMIN2_H */

