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
 *   yuanqi <yuanqi.xhf@taobao.com>
 *     - some work details if you want
 */

#include "tbsys.h"
#include "common/ob_malloc.h"
#include "updateserver/ob_ups_mutator.h"
#include "updateserver/ob_on_disk_log_locator.h"
#include "common/ob_direct_log_reader.h"
#include "common/ob_repeated_log_reader.h"
#include "common/ob_log_writer.h"
#include <stdlib.h>
#include <set>
#include "file_utils.h"

using namespace oceanbase::common;
using namespace oceanbase::updateserver;

const char* _usages = "Usages:\n"
  "\t# You can set env var 'log_level' to 'DEBUG'/'WARN'...\n"
  "\t%1$s gen_nop log_file n_logs\n"
  "\t%1$s reset_id src_log_file dest_log_file start_id\n"
  "\t%1$s locate log_dir log_id\n"
  "\t%1$s dump_by_log_reader log_dir/log_file_id\n"
  "\t%1$s dump log_file\n"
  "\t%1$s merge src_log_file dest_log_file\n"
  "\t%1$s trans2to3 src_dir/log_file_id dest_dir/log_file_id\n"
  "\t%1$s a2i src_file dest_file\n"
  "\t%1$s dumpi file_of_binary_int\n"
  "\t%1$s read_file_test file_name offset\n"
  "\t%1$s select src_log_file dest_log_file id_seq_file\n";

int dump_log_location(ObOnDiskLogLocator& log_locator, int64_t log_id)
{
  int err = OB_SUCCESS;
  ObLogLocation location;
  if (OB_SUCCESS != (err = log_locator.get_location(log_id, location)))
  {
    TBSYS_LOG(ERROR, "log_locator.get_location(%ld)=>%d", log_id, err);
  }
  else
  {
    printf("%ld -> [file_id=%ld, offset=%ld, log_id=%ld]\n", log_id, location.file_id_, location.offset_, location.log_id_);
  }
  return err;
}

int read_file_test(const char* path, int64_t offset)
{
  int err = OB_SUCCESS;
  ObString fname;
  ObFileReader file_reader;
  char buf[1<<21];
  int64_t len = sizeof(buf);
  bool dio = true;
  int64_t read_count = 0;
  if (NULL == path || 0 > offset || NULL == buf || 0 >= len)
  {
    err = OB_INVALID_ARGUMENT;
  }
  else
  {
    fname.assign_ptr((char*)path, static_cast<int32_t>(strlen(path)));
  }

  if (OB_SUCCESS != err)
  {}
  else if (OB_SUCCESS != (err = file_reader.open(fname, dio)))
  {
    TBSYS_LOG(ERROR, "file_reader.open(%s)=>%d", path, err);
  }
  else if (OB_SUCCESS != (err = file_reader.pread(buf, len, offset, read_count)))
  {
    TBSYS_LOG(ERROR, "file_reader.pread(buf=%p[%ld], offset=%ld)=>%d", buf, len, offset, err);
  }

  if (file_reader.is_opened())
  {
    file_reader.close();
  }
  TBSYS_LOG(INFO, "read file '%s', offset=%ld, readcount=%ld", path, offset, read_count);
  return err;
}

int locate(const char* log_dir, int64_t log_id)
{
  int err = OB_SUCCESS;
  ObOnDiskLogLocator log_locator;
  ObLogLocation location;
  if (NULL == log_dir || 0 >= log_id)
  {
    err = OB_INVALID_ARGUMENT;
    TBSYS_LOG(ERROR, "find_log(log_dir=%s, log_id=%ld): INVALID ARGUMENT.", log_dir, log_id);
  }
  else if (OB_SUCCESS != (err = log_locator.init(log_dir)))
  {
    TBSYS_LOG(ERROR, "log_locator.init(%s)=>%d", log_dir, err);
  }
  else if (OB_SUCCESS != (err = dump_log_location(log_locator, log_id)))
  {
    TBSYS_LOG(ERROR, "dump_log_location(%ld)=>%d", log_id, err);
  }
  return err;
}

static const char* LogCmdStr[1024];
int init_log_cmd_str()
{
  memset(LogCmdStr, 0x00, sizeof(LogCmdStr));
  LogCmdStr[OB_LOG_SWITCH_LOG]            = "SWITCH_LOG";
  LogCmdStr[OB_LOG_CHECKPOINT]            = "CHECKPOINT";
  LogCmdStr[OB_LOG_NOP]                   = "NOP";
  LogCmdStr[OB_LOG_UPS_MUTATOR]           = "UPS_MUTATOR";
  LogCmdStr[OB_UPS_SWITCH_SCHEMA]         = "UPS_SWITCH_SCHEMA";
  LogCmdStr[OB_RT_SCHEMA_SYNC]            = "OB_RT_SCHEMA_SYNC";
  LogCmdStr[OB_RT_CS_REGIST]              = "OB_RT_CS_REGIST";
  LogCmdStr[OB_RT_MS_REGIST]              = "OB_RT_MS_REGIST";
  LogCmdStr[OB_RT_SERVER_DOWN]            = "OB_RT_SERVER_DOWN";
  LogCmdStr[OB_RT_CS_LOAD_REPORT]         = "OB_RT_CS_LOAD_REPORT";
  LogCmdStr[OB_RT_CS_MIGRATE_DONE]        = "OB_RT_CS_MIGRATE_DONE";
  LogCmdStr[OB_RT_CS_START_SWITCH_ROOT_TABLE]
    = "OB_RT_CS_START_SWITCH_ROOT_TABLE";
  LogCmdStr[OB_RT_START_REPORT]           = "OB_RT_START_REPORT";
  LogCmdStr[OB_RT_REPORT_TABLETS]         = "OB_RT_REPORT_TABLETS";
  LogCmdStr[OB_RT_ADD_NEW_TABLET]         = "OB_RT_ADD_NEW_TABLET";
  LogCmdStr[OB_RT_CREATE_TABLE_DONE]      = "OB_RT_CREATE_TABLE_DONE";
  LogCmdStr[OB_RT_BEGIN_BALANCE]          = "OB_RT_BEGIN_BALANCE";
  LogCmdStr[OB_RT_BALANCE_DONE]           = "OB_RT_BALANCE_DONE";
  LogCmdStr[OB_RT_US_MEM_FRZEEING]        = "OB_RT_US_MEM_FRZEEING";
  LogCmdStr[OB_RT_US_MEM_FROZEN]          = "OB_RT_US_MEM_FROZEN";
  LogCmdStr[OB_RT_CS_START_MERGEING]      = "OB_RT_CS_START_MERGEING";
  LogCmdStr[OB_RT_CS_MERGE_OVER]          = "OB_RT_CS_MERGE_OVER";
  LogCmdStr[OB_RT_CS_UNLOAD_DONE]         = "OB_RT_CS_UNLOAD_DONE";
  LogCmdStr[OB_RT_US_UNLOAD_DONE]         = "OB_RT_US_UNLOAD_DONE";
  LogCmdStr[OB_RT_DROP_CURRENT_BUILD]     = "OB_RT_DROP_CURRENT_BUILD";
  LogCmdStr[OB_RT_DROP_LAST_CS_DURING_MERGE]
    = "OB_RT_DROP_LAST_CS_DURING_MERGE";
  LogCmdStr[OB_RT_SYNC_FROZEN_VERSION]    = "OB_RT_SYNC_FROZEN_VERSION";
  LogCmdStr[OB_RT_SET_UPS_LIST]           = "OB_RT_SET_UPS_LIST";
  LogCmdStr[OB_RT_SET_CLIENT_CONFIG]      = "OB_RT_SET_CLIENT_CONFIG";
  return 0;
}

const char* get_log_cmd_repr(const LogCommand cmd)
{
  const char* cmd_repr = NULL;
  if (cmd < 0 || cmd >= (int)ARRAYSIZEOF(LogCmdStr))
  {}
  else
  {
    cmd_repr = LogCmdStr[cmd];
  }
  if (NULL == cmd_repr)
  {
    cmd_repr = "unknown";
  }
  return cmd_repr;
}

const char* format_time(int64_t time_us)
{
  static char time_str[1024];
  const char* format = "%Y-%m-%d %H:%M:%S";
  struct tm time_struct;
  int64_t time_s = time_us / 1000000;
  if(NULL != localtime_r(&time_s, &time_struct))
  {
    strftime(time_str, sizeof(time_str), format, &time_struct);
  }
  time_str[sizeof(time_str)-1] = 0;
  return time_str;
}

bool is_same_row(ObCellInfo& cell_1, ObCellInfo& cell_2)
{
  return cell_1.table_name_ == cell_2.table_name_ && cell_1.table_id_ == cell_2.table_id_ && cell_1.row_key_ == cell_2.row_key_;
}

int mutator_size(ObMutator& src, int64_t& n_cell, int64_t& n_row)
{
  int err = OB_SUCCESS;
  ObMutatorCellInfo* last_cell = NULL;
  n_cell = 0;
  n_row = 0;
  src.reset_iter();
  while ((OB_SUCCESS == err) && (OB_SUCCESS == (err = src.next_cell())))
  {
    ObMutatorCellInfo* cell = NULL;
    if (OB_SUCCESS != (err = src.get_cell(&cell)))
    {
      TBSYS_LOG(ERROR, "mut.get_cell()=>%d", err);
    }
    else if (last_cell != NULL && is_same_row(last_cell->cell_info,cell->cell_info))
    {}
    else
    {
      n_row++;
    }
    if (OB_SUCCESS == err)
    {
      n_cell++;
      last_cell = cell;
    }
  }
  if (OB_ITER_END == err)
  {
    err = OB_SUCCESS;
  }
  return err;
}
    
int dump_mutator(ObUpsMutator& mutator, int64_t& n_cell, int64_t& n_row)
{
  int err = OB_SUCCESS;
  if (OB_SUCCESS != (err = mutator_size(mutator.get_mutator(), n_cell, n_row)))
  {
    TBSYS_LOG(ERROR, "mutator_size()=>%d", err);
  }
  else
  {
    printf("normal: %s, size %ld:%ld, MutationTime: %s, checksum %ld:%ld\n",
           STR_BOOL(mutator.is_normal_mutator()),
           n_cell, n_row,
           format_time(mutator.get_mutate_timestamp()),
           mutator.get_memtable_checksum_before_mutate(),
           mutator.get_memtable_checksum_after_mutate());
  }
  return err;
}

int dump_by_log_reader(const char* log_file)
{
  int err = OB_SUCCESS;
  ObDirectLogReader direct_reader;
  ObRepeatedLogReader repeated_reader;
  ObSingleLogReader* reader = NULL;
  LogCommand cmd = OB_LOG_UNKNOWN;
  uint64_t log_seq = 0;
  char* log_data = NULL;
  int64_t data_len = 0;
  char* p = NULL;
  const char* log_dir = NULL;
  const char* log_name = NULL;
  int64_t n_cell = 0;
  int64_t n_row = 0;
  int64_t total_cell = 0;
  int64_t total_row = 0;
  int64_t n_mutator = 0;
  bool test_read_speed = getenv("test_read_speed") != NULL;
  ObUpsMutator mutator;
  reader = (0 == strcmp(getenv("reader")? :"direct_log_reader", "direct_log_reader"))? (ObSingleLogReader*)&direct_reader: (ObSingleLogReader*)&repeated_reader;
  TBSYS_LOG(INFO, "use %s as log_reader", (reader == &direct_reader)? "direct_reader": "repeated_reader");
  if (NULL == log_file)
  {
    err = OB_INVALID_ARGUMENT;
  }
  else if (NULL == (p = strrchr(const_cast<char*> (log_file), '/')))
  {
    log_dir = ".";
    log_name = log_file;
  }
  else
  {
    log_dir = log_file;
    *p = '\0';
    log_name = p + 1;
  }

  if (OB_SUCCESS != err)
  {}
  else if (OB_SUCCESS != (err = reader->init(log_dir)))
  {
    TBSYS_LOG(ERROR, "reader.init(log_dir=%s)=>%d", log_dir, err);
  }
  else if (OB_SUCCESS != (err = reader->open(atoi(log_name))))
  {
    TBSYS_LOG(ERROR, "reader.open(log_name=%s)=>%d", log_name, err);
  }

  while(OB_SUCCESS == err)
  {
    if (OB_SUCCESS != (err = reader->read_log(cmd, log_seq, log_data, data_len)) && OB_READ_NOTHING != err)
    {
      TBSYS_LOG(ERROR, "read_log()=>%d", err);
    }
    else if (OB_READ_NOTHING == err)
    {
      if (reader == &repeated_reader)
      {
        err = OB_SUCCESS;
        TBSYS_LOG(INFO, "read to end of log, need to wait");
        usleep(1000000);
      }
    }
    else if (!test_read_speed)
    {
      int64_t pos = 0;
      fprintf(stdout, "%lu|%ld\t|%ld\t%s[%d]\n", log_seq, reader->get_last_log_offset(), data_len, get_log_cmd_repr(cmd), cmd);
      if (OB_LOG_UPS_MUTATOR != cmd)
      {}
      else if (OB_SUCCESS != (err = mutator.deserialize(log_data, data_len, pos)))
      {
        TBSYS_LOG(ERROR, "mutator.deserialize(seq=%ld)=>%d", log_seq, err);
      }
      else if (OB_SUCCESS != (err = dump_mutator(mutator, n_cell, n_row)))
      {
        TBSYS_LOG(ERROR, "dump_mutator()=>%d", err);
      }
      else
      {
        total_cell += n_cell;
        total_row += n_row;
        n_mutator++;
      }
    }
  }
  if (OB_READ_NOTHING == err)
  {
    err = OB_SUCCESS;
  }
  if (OB_SUCCESS == err)
  {
    printf("cell=%ld/%ld=%.2f, row=%ld/%ld=%.2f\n", total_cell, n_mutator, 1.0 * static_cast<double> (total_cell)/static_cast<double> (n_mutator),
		    total_row, n_mutator, 1.0 * static_cast<double>(total_row)/static_cast<double>(n_mutator));
  }
  return err;
}

int dump_log(const char* buf, const int64_t len)
{
  int err = OB_SUCCESS;
  int64_t pos = 0;
  ObLogEntry entry;
  ObUpsMutator mutator;
  bool dump_header_only = getenv("dump_header_only");
  int64_t n_cell = 0;
  int64_t n_row = 0;
  int64_t total_cell = 0;
  int64_t total_row = 0;
  int64_t n_mutator = 0;

  if (NULL == buf || 0 > len)
  {
    err = OB_INVALID_ARGUMENT;
  }
  while(OB_SUCCESS == err && pos < len)
  {
    if (OB_SUCCESS != (err = entry.deserialize(buf, len, pos)))
    {
      TBSYS_LOG(ERROR, "log_entry.deserialize()=>%d", err);
    }
    else
    {
      int64_t tmp_pos = 0;
      fprintf(stdout, "%lu|%ld\t|%ld\t%s[%d]\n", entry.seq_, pos + entry.get_log_data_len(), (int64_t)entry.get_log_data_len(), get_log_cmd_repr((LogCommand)entry.cmd_), entry.cmd_);
      if (dump_header_only)
      {}
      else if (OB_LOG_UPS_MUTATOR != entry.cmd_)
      {}
      else if (OB_SUCCESS != (err = mutator.deserialize(buf + pos, entry.get_log_data_len(), tmp_pos)))
      {
        TBSYS_LOG(ERROR, "mutator.deserialize(seq=%ld)=>%d", (int64_t)entry.seq_, err);
      }
      else if (OB_SUCCESS != (err = dump_mutator(mutator, n_cell, n_row)))
      {
        TBSYS_LOG(ERROR, "dump_mutator()=>%d", err);
      }
      if (OB_SUCCESS == err)
      {
        pos += entry.get_log_data_len();
        total_cell += n_cell;
        total_row += n_row;
        n_mutator++;
      }
    }
  }
  if (OB_SUCCESS == err && pos != len)
  {
    err = OB_ERR_UNEXPECTED;
    TBSYS_LOG(ERROR, "pos[%ld] != len[%ld]", pos, len);
  }
  if (OB_SUCCESS == err)
  {
    printf("cell=%ld/%ld=%.2f, row=%ld/%ld=%.2f\n", total_cell, n_mutator, 1.0 * static_cast<double>(total_cell)/static_cast<double>(n_mutator),
		    total_row, n_mutator, 1.0 * static_cast<double>(total_row)/static_cast<double>(n_mutator));
  }
  return err;
}

int dump(const char* log_file)
{
  int err = OB_SUCCESS;
  const char* src_buf = NULL;
  int64_t len = 0;
  TBSYS_LOG(DEBUG, "dump(src=%s)", log_file);
  if (NULL == log_file)
  {
    err = OB_INVALID_ARGUMENT;
  }
  else if (OB_SUCCESS != (err = get_file_len(log_file, len)))
  {
    TBSYS_LOG(ERROR, "get_file_len(%s)=>%d", log_file, err);
  }
  else if (OB_SUCCESS != (err = file_map_read(log_file, len, src_buf)))
  {
    TBSYS_LOG(ERROR, "file_map_read(%s)=>%d", log_file, err);
  }
  else if (OB_SUCCESS != (err = dump_log(src_buf, len)))
  {
    TBSYS_LOG(ERROR, "dump_log(buf=%p[%ld])=>%d", src_buf, len, err);
  }
  return err;
}

int mutator_add_(ObMutator& dst, ObMutator& src)
{
  int err = OB_SUCCESS;
  src.reset_iter();
  while ((OB_SUCCESS == err) && (OB_SUCCESS == (err = src.next_cell())))
  {
    ObMutatorCellInfo* cell = NULL;
    if (OB_SUCCESS != (err = src.get_cell(&cell)))
    {
      TBSYS_LOG(ERROR, "mut.get_cell()=>%d", err);
    }
    else if (OB_SUCCESS != (err = dst.add_cell(*cell)))
    {
      TBSYS_LOG(ERROR, "dst.add_cell()=>%d", err);
    }
  }
  if (OB_ITER_END == err)
  {
    err = OB_SUCCESS;
  }
  return err;
}
    
int mutator_add(ObMutator& dst, ObMutator& src, int64_t size_limit)
{
  int err = OB_SUCCESS;
  if (dst.get_serialize_size() + src.get_serialize_size() > size_limit)
  {
    err = OB_SIZE_OVERFLOW;
    TBSYS_LOG(DEBUG, "mutator_add(): size overflow");
  }
  else if (OB_SUCCESS != (err = mutator_add_(dst, src)))
  {
    TBSYS_LOG(ERROR, "mutator_add()=>%d", err);
  }
  return err;
}

inline int64_t get_align_padding_size(const int64_t x, const int64_t mask)
{
  return -x & mask;
}

int serialize_nop_log(int64_t seq, char* buf, const int64_t limit, int64_t& pos)
{
  int err = OB_SUCCESS;
  ObLogEntry entry;
  int64_t data_len = get_align_padding_size(pos + entry.get_serialize_size() + 1, ObLogWriter::LOG_FILE_ALIGN_SIZE-1) + 1;
  entry.seq_ = seq;
  entry.cmd_ = OB_LOG_NOP;
  if (NULL == buf || 0 >= limit || pos >= limit)
  {
    err = OB_INVALID_ARGUMENT;
  }
  else if (pos + entry.get_serialize_size() + data_len >= limit)
  {
    err = OB_BUF_NOT_ENOUGH;
  }
  else if (NULL == memset(buf + pos + entry.get_serialize_size(), 0, data_len))
  {
    err = OB_ERR_UNEXPECTED;
  }
  else if (OB_SUCCESS != (err = entry.fill_header(buf + pos + entry.get_serialize_size(), data_len)))
  {
    TBSYS_LOG(ERROR, "entry.fill_header()=>%d", err);
  }
  else if (OB_SUCCESS != (err = entry.serialize(buf, limit, pos)))
  {
    TBSYS_LOG(ERROR, "entry.serialize()=>%d", err);
  }
  else
  {
    pos += data_len;
  }
  return err;
}

int serialize_mutator_log(int64_t seq, char* buf, const int64_t limit, int64_t& pos, ObUpsMutator& mutator)
{
  int err = OB_SUCCESS;
  ObLogEntry entry;
  int64_t tmp_pos = pos + entry.get_serialize_size();
  entry.seq_ = seq;
  entry.cmd_ = OB_LOG_UPS_MUTATOR;
  if (NULL == buf || 0 >= limit || pos >= limit)
  {
    err = OB_INVALID_ARGUMENT;
  }
  else if (OB_SUCCESS != (err = mutator.serialize(buf, limit, tmp_pos)))
  {
    TBSYS_LOG(ERROR, "mutator.serialize()=>%d", err);
  }
  else if (OB_SUCCESS != (err = entry.fill_header(buf + pos + entry.get_serialize_size(), mutator.get_serialize_size())))
  {
    TBSYS_LOG(ERROR, "entry.fill_header()=>%d", err);
  }
  else if (OB_SUCCESS != (err = entry.serialize(buf, limit, pos)))
  {
    TBSYS_LOG(ERROR, "entry.serialize()=>%d", err);
  }
  else if (tmp_pos - pos != mutator.get_serialize_size())
  {
    err = OB_ERR_UNEXPECTED;
    TBSYS_LOG(ERROR, "mutator.get_serialize_size()[%ld] != tmp_pos[%ld] - pos[%ld]", mutator.get_serialize_size(), tmp_pos, pos);
  }
  else
  {
    pos = tmp_pos;
  }
  return err;
}

int merge_mutator(const char* src_buf, const int64_t src_len, char* dest_buf, const int64_t dest_len, int64_t& ret_len)
{
  int err = OB_SUCCESS;
  int64_t src_pos = 0;
  int64_t dest_pos = 0;
  ObLogEntry entry;
  ObUpsMutator mutator;
  ObUpsMutator agg_mut;
  int64_t seq = 1;
  const int64_t size_limit = OB_MAX_LOG_BUFFER_SIZE;
  if (NULL == src_buf || NULL == dest_buf || 0 > src_len || 0 > dest_len)
  {
    err = OB_INVALID_ARGUMENT;
  }
  while(OB_SUCCESS == err && src_pos < src_len)
  {
    int64_t tmp_pos = 0;
    if (OB_SUCCESS != (err = entry.deserialize(src_buf, src_len, src_pos)))
    {
      TBSYS_LOG(ERROR, "log_entry.deserialize()=>%d", err);
    }
    else if (OB_LOG_UPS_MUTATOR != entry.cmd_)
    {}
    else if (OB_SUCCESS != (err = mutator.deserialize(src_buf + src_pos, entry.get_log_data_len(), tmp_pos)))
    {
      TBSYS_LOG(ERROR, "mutator.deserialize(seq=%ld)=>%d", (int64_t)entry.seq_, err);
    }
    else if (!mutator.is_normal_mutator())
    {
      TBSYS_LOG(DEBUG, "mutator[%ld] is not normal mutator", entry.seq_);
    }
    else if (OB_SUCCESS != (err = mutator_add(agg_mut.get_mutator(), mutator.get_mutator(), size_limit))
             && OB_SIZE_OVERFLOW != err)
    {
      TBSYS_LOG(ERROR, "mutator_add(%ld)=>%d", entry.seq_, err);
    }
    else if (OB_SUCCESS == err)
    {
      TBSYS_LOG(DEBUG, "mutator_add(%ld):succ", entry.seq_);
    }
    else if (OB_SUCCESS != (err = serialize_mutator_log(seq++, dest_buf, dest_len, dest_pos, agg_mut)))
    {
      TBSYS_LOG(ERROR, "serialize_mutator_log()=>%d", err);
    }
    else if (OB_SUCCESS != (err = serialize_nop_log(seq++, dest_buf, dest_len, dest_pos)))
    {
      TBSYS_LOG(ERROR, "serialize_nop_log()=>%d", err);
    }
    else if (OB_SUCCESS != (err = agg_mut.get_mutator().reset()))
    {
      TBSYS_LOG(ERROR, "agg_mut.reset()=>%d", err);
    }
    else if (OB_SUCCESS != (err = mutator_add(agg_mut.get_mutator(), mutator.get_mutator(), size_limit)))
    {
      TBSYS_LOG(ERROR, "mutator_add(%ld)=>%d", entry.seq_, err);
    }
    if (OB_SUCCESS == err)
    {
      src_pos += entry.get_log_data_len();
    }
  }
  if (OB_SUCCESS == err && OB_SUCCESS != (err = serialize_mutator_log(seq++, dest_buf, dest_len, dest_pos, agg_mut)))
  {
    TBSYS_LOG(ERROR, "serialize_mutator_log()=>%d", err);
  }
  else if (OB_SUCCESS != (err = serialize_nop_log(seq++, dest_buf, dest_len, dest_pos)))
  {
    TBSYS_LOG(ERROR, "serialize_nop_log()=>%d", err);
  }
  if (OB_SUCCESS == err && src_pos != src_len)
  {
    err = OB_ERR_UNEXPECTED;
    TBSYS_LOG(ERROR, "pos[%ld] != len[%ld]", src_pos, src_len);
  }
  if (OB_SUCCESS == err)
  {
    ret_len = dest_pos;
  }
  return err;
}

int merge(const char* src_log_file, const char* dest_log_file)
{
  int err = OB_SUCCESS;
  const char* src_buf = NULL;
  char* dest_buf = NULL;
  int64_t src_len = 0;
  int64_t dest_len = 0;
  TBSYS_LOG(DEBUG, "merge(src=%s, dest=%s)", src_log_file, dest_log_file);
  if (NULL == src_log_file || NULL == dest_log_file)
  {
    err = OB_INVALID_ARGUMENT;
  }
  else if (OB_SUCCESS != (err = get_file_len(src_log_file, src_len)))
  {
    TBSYS_LOG(ERROR, "get_file_len(%s)=>%d", src_log_file, err);
  }
  else if (OB_SUCCESS != (err = file_map_read(src_log_file, src_len, src_buf)))
  {
    TBSYS_LOG(ERROR, "file_map_read(%s)=>%d", src_log_file, err);
  }
  else if (OB_SUCCESS != (err = file_map_write(dest_log_file, src_len, dest_buf)))
  {
    TBSYS_LOG(ERROR, "file_map_write(%s)=>%d", dest_log_file, err);
  }
  else if (OB_SUCCESS != (err = merge_mutator(src_buf, src_len, dest_buf, src_len, dest_len)))
  {
    TBSYS_LOG(ERROR, "merge_mutator(src_len=%ld, dest_len=%ld)=>%d", src_len, dest_len, err);
  }
  else if (OB_SUCCESS != (err = truncate(dest_log_file, dest_len)))
  {
    TBSYS_LOG(ERROR, "truncate(%s, len=%ld)=>%d", dest_log_file, dest_len, err);
  }
  return err;
}

// 每条log的seq = seq + file_id
int do_convert_log_seq_2to3(const int64_t file_id, char* buf, const int64_t len)
{
  int err = OB_SUCCESS;
  int64_t old_pos = 0;
  int64_t pos = 0;
  ObLogEntry log_entry;

  if (0 >= file_id || NULL == buf || 0 > len)
  {
    err = OB_INVALID_ARGUMENT;
  }
  while(OB_SUCCESS == err && pos < len)
  {
    old_pos = pos;
    if (OB_SUCCESS != (err = log_entry.deserialize(buf, len, pos)))
    {
      TBSYS_LOG(ERROR, "log_entry.deserialize()=>%d", err);
    }
    else
    {
      //TBSYS_LOG(DEBUG, "log_entry[seq=%ld, file_id=%ld]", log_entry.seq_, file_id);
      log_entry.set_log_seq(log_entry.seq_ + ((OB_LOG_SWITCH_LOG == log_entry.cmd_) ?file_id : file_id - 1));
    }

    if (OB_SUCCESS != err)
    {}
    else if (OB_SUCCESS != (err = log_entry.fill_header(buf + pos, log_entry.get_log_data_len())))
    {
      TBSYS_LOG(ERROR, "log_entry.fill_header()=>%d", err);
    }
    else if (OB_SUCCESS != (err = log_entry.serialize(buf, len, old_pos)))
    {
      TBSYS_LOG(ERROR, "log_entry.serialize()=>%d", err);
    }
    if (OB_SUCCESS == err)
    {
      pos += log_entry.get_log_data_len();
    }
  }
  if (OB_SUCCESS == err && pos != len)
  {
    err = OB_ERR_UNEXPECTED;
    TBSYS_LOG(ERROR, "pos[%ld] != len[%ld]", pos, len);
  }
  return err;
}

int reset_log_id(char* buf, int64_t len, const int64_t start_id)
{
  int err = OB_SUCCESS;
  int64_t real_log_id = start_id;
  int64_t old_pos = 0;
  int64_t pos = 0;
  ObLogEntry log_entry;

  if (0 >= start_id || NULL == buf || 0 > len)
  {
    err = OB_INVALID_ARGUMENT;
  }
  while(OB_SUCCESS == err && pos < len)
  {
    old_pos = pos;
    if (OB_SUCCESS != (err = log_entry.deserialize(buf, len, pos)))
    {
      TBSYS_LOG(ERROR, "log_entry.deserialize()=>%d", err);
    }
    else
    {
      TBSYS_LOG(DEBUG, "set_log_seq(%ld)", real_log_id);
      log_entry.set_log_seq(real_log_id++);
    }

    if (OB_SUCCESS != err)
    {}
    else if (OB_SUCCESS != (err = log_entry.fill_header(buf + pos, log_entry.get_log_data_len())))
    {
      TBSYS_LOG(ERROR, "log_entry.fill_header()=>%d", err);
    }
    else if (OB_SUCCESS != (err = log_entry.serialize(buf, len, old_pos)))
    {
      TBSYS_LOG(ERROR, "log_entry.serialize()=>%d", err);
    }
    if (OB_SUCCESS == err)
    {
      pos += log_entry.get_log_data_len();
    }
  }
  if (OB_SUCCESS == err && pos != len)
  {
    err = OB_ERR_UNEXPECTED;
    TBSYS_LOG(ERROR, "pos[%ld] != len[%ld]", pos, len);
  }
  return err;
}

int trans2to3(const char* src_log_file, const char* dest_log_file)
{
  int err = OB_SUCCESS;
  int64_t src_file_id = 0;
  const char* path_sep = NULL;
  const char* src_buf = NULL;
  char* dest_buf = NULL;
  int64_t len = 0;
  TBSYS_LOG(DEBUG, "trans2to3(src=%s, dest=%s)", src_log_file, dest_log_file);
  if (NULL == src_log_file || NULL == dest_log_file)
  {
    err = OB_INVALID_ARGUMENT;
  }
  else if (0 >= (src_file_id = atoll((path_sep = strrchr(src_log_file, '/'))? path_sep+1: src_log_file)))
  {
    err = OB_INVALID_ARGUMENT;
    TBSYS_LOG(ERROR, "src_log_file[%s] should end with a number:path_sep=%s, strlen(path_sep)=%ld", src_log_file, path_sep, strlen(path_sep));
  }
  else if (OB_SUCCESS != (err = get_file_len(src_log_file, len)))
  {
    TBSYS_LOG(ERROR, "get_file_len(%s)=>%d", src_log_file, err);
  }
  else if (OB_SUCCESS != (err = file_map_read(src_log_file, len, src_buf)))
  {
    TBSYS_LOG(ERROR, "file_map_read(%s)=>%d", src_log_file, err);
  }
  else if (OB_SUCCESS != (err = file_map_write(dest_log_file, len, dest_buf)))
  {
    TBSYS_LOG(ERROR, "file_map_write(%s)=>%d", dest_log_file, err);
  }
  else if (NULL == memcpy(dest_buf, src_buf, len))
  {
    err = OB_ERR_UNEXPECTED;
  }
  else if (OB_SUCCESS != (err = do_convert_log_seq_2to3(src_file_id, dest_buf, len)))
  {
    TBSYS_LOG(ERROR, "do_convert_log_seq_2to3(file_id=%ld)=>%d", src_file_id, err);
  }
  return err;
}

int fill_nop_log(char* buf, int64_t nop_size, int64_t n)
{
  int err = OB_SUCCESS;
  ObLogEntry entry;
  int64_t pos = 0;
  int64_t header_size = entry.get_serialize_size();
  if (NULL == buf || 0 >= nop_size || 0 >= n)
  {
    err = OB_INVALID_ARGUMENT;
  }
  for(int64_t i = 0; OB_SUCCESS == err && i < n; i++)
  {
    entry.set_log_seq(i + 1);
    entry.set_log_command(OB_LOG_NOP);
    memset(buf + pos, 0, nop_size);
    if (OB_SUCCESS != (err = entry.fill_header(buf + pos + header_size, nop_size - header_size)))
    {
      TBSYS_LOG(ERROR, "entry.fill_header()=>%d", err);
    }
    else if (OB_SUCCESS != (err = entry.serialize(buf, nop_size * n, pos)))
    {
      TBSYS_LOG(ERROR, "entry.serialize()=>%d", err);
    }
    else
    {
      pos += nop_size - header_size;
    }
  }
  return err;
}

int gen_nop(const char* dest_log_file, int64_t n_logs)
{
  int err = OB_SUCCESS;
  char* dest_buf = NULL;
  int64_t nop_size = OB_DIRECT_IO_ALIGN;
  TBSYS_LOG(DEBUG, "gen_nop(dest=%s, n_logs=%ld)", dest_log_file, n_logs);
  if (NULL == dest_log_file || 0 >= n_logs)
  {
    err = OB_INVALID_ARGUMENT;
  }
  else if (OB_SUCCESS != (err = file_map_write(dest_log_file, nop_size * n_logs, dest_buf)))
  {
    TBSYS_LOG(ERROR, "file_map_write(%s)=>%d", dest_log_file, err);
  }
  else if (OB_SUCCESS != (err = fill_nop_log(dest_buf, nop_size, n_logs)))
  {
    TBSYS_LOG(ERROR, "fill_nop_log(n_logs=%ld)=>%d", n_logs, err);
  }
  return err;
}

int reset_id(const char* src_log_file, const char* dest_log_file, int64_t start_id)
{
  int err = OB_SUCCESS;
  const char* src_buf = NULL;
  char* dest_buf = NULL;
  int64_t len = 0;
  TBSYS_LOG(DEBUG, "reset_id(src=%s, dest=%s, start_id=%ld)", src_log_file, dest_log_file, start_id);
  if (NULL == src_log_file || NULL == dest_log_file || 0 >= start_id)
  {
    err = OB_INVALID_ARGUMENT;
  }
  else if (OB_SUCCESS != (err = get_file_len(src_log_file, len)))
  {
    TBSYS_LOG(ERROR, "get_file_len(%s)=>%d", src_log_file, err);
  }
  else if (OB_SUCCESS != (err = file_map_read(src_log_file, len, src_buf)))
  {
    TBSYS_LOG(ERROR, "file_map_read(%s)=>%d", src_log_file, err);
  }
  else if (OB_SUCCESS != (err = file_map_write(dest_log_file, len, dest_buf)))
  {
    TBSYS_LOG(ERROR, "file_map_write(%s)=>%d", dest_log_file, err);
  }
  else if (NULL == memcpy(dest_buf, src_buf, len))
  {
    err = OB_ERR_UNEXPECTED;
  }
  else if (OB_SUCCESS != (err = reset_log_id(dest_buf, len, start_id)))
  {
    TBSYS_LOG(ERROR, "reset_log_id(start_id=%ld)=>%d", start_id, err);
  }
  return err;
}

int dumpi(const char* src_file)
{
  int err = OB_SUCCESS;
  const char* src_buf = NULL;
  int64_t len = 0;
  TBSYS_LOG(DEBUG, "dumpi(src=%s)", src_file);
  if (NULL == src_file)
  {
    err = OB_INVALID_ARGUMENT;
  }
  else if (OB_SUCCESS != (err = get_file_len(src_file, len)))
  {
    TBSYS_LOG(ERROR, "get_file_len(%s)=>%d", src_file, err);
  }
  else if (OB_SUCCESS != (err = file_map_read(src_file, len, src_buf)))
  {
    TBSYS_LOG(ERROR, "file_map_read(%s)=>%d", src_file, err);
  }
  if (0 != (len % sizeof(int64_t)))
  {
    TBSYS_LOG(WARN, "len[%ld] %% sizeof(int64_t)[%ld] != 0", len, sizeof(int64_t));
  }
  for(int64_t offset = 0; OB_SUCCESS == err && offset + (int64_t)sizeof(int64_t) <= len; offset += (int64_t)sizeof(int64_t))
  {
    printf("%ld\n", *(int64_t*)(src_buf + offset));
  }
  return err;
}

int a2i(const char* src_file, const char* dest_file)
{
  int err = OB_SUCCESS;
  const char* src_buf = NULL;
  char* dest_buf = NULL;
  int64_t* dest_int_buf = NULL;;
  const char* start = NULL;
  char* end = NULL;
  int64_t len = 0;
  int64_t int_file_len = 0;
  TBSYS_LOG(DEBUG, "atoi(src=%s, dest=%s)", src_file, dest_file);
  if (NULL == src_file || NULL == dest_file)
  {
    err = OB_INVALID_ARGUMENT;
  }
  else if (OB_SUCCESS != (err = get_file_len(src_file, len)))
  {
    TBSYS_LOG(ERROR, "get_file_len(%s)=>%d", src_file, err);
  }
  else if (OB_SUCCESS != (err = file_map_read(src_file, len, src_buf)))
  {
    TBSYS_LOG(ERROR, "file_map_read(%s)=>%d", src_file, err);
  }
  else if (OB_SUCCESS != (err = file_map_write(dest_file, len * sizeof(int64_t), dest_buf)))
  {
    TBSYS_LOG(ERROR, "file_map_write(%s)=>%d", dest_file, err);
  }
  else
  {
    dest_int_buf = (int64_t*)dest_buf;
    start = src_buf;
    end = NULL;
    while(1)
    {
      int64_t id = strtol(start, &end, 10);
      if (end == start)
      {
        break;
      }
      else
      {
        *dest_int_buf++ = id;
        start = end;
      }
    }
    int_file_len = (int64_t)dest_int_buf - (int64_t)dest_buf;
  }
  if (OB_SUCCESS != err)
  {}
  else if (0 != truncate(dest_file, int_file_len))
  {
    err = OB_IO_ERROR;
    TBSYS_LOG(ERROR, "truncate(file=%s, len=%ld): %s", dest_file, int_file_len, strerror(errno));
  }
  return err;
}

int select_log_by_id(char* dest_buf, const int64_t dest_limit, int64_t& dest_len, const char* src_buf, const int64_t src_len,
                     const int64_t* id_seq, int64_t n_id)
{
  int err = OB_SUCCESS;
  int64_t old_pos = 0;
  int64_t src_pos = 0;
  int64_t dest_pos = 0;
  ObLogEntry entry;
  int64_t size = 0;
  if (NULL == dest_buf || NULL == src_buf || NULL == id_seq || 0 >= dest_limit || 0 >= src_len || 0 >= n_id)
  {
    err = OB_INVALID_ARGUMENT;
  }
  else
  {
    std::set<int64_t> id_set(id_seq, id_seq + n_id);
    while(OB_SUCCESS == err && src_pos < src_len)
    {
      old_pos = src_pos;
      if (OB_SUCCESS != (err = entry.deserialize(src_buf, src_len, src_pos)))
      {
        TBSYS_LOG(ERROR, "log_entry.deserialize()=>%d", err);
      }
      else if (id_set.find(entry.seq_) == id_set.end())
      {
        TBSYS_LOG(DEBUG, "ignore this log[seq=%ld]", entry.seq_);
      }
      else
      {
        size = entry.get_serialize_size() + entry.get_log_data_len();
        if (dest_pos + size > dest_limit)
        {
          err = OB_BUF_NOT_ENOUGH;
          TBSYS_LOG(ERROR, "dest_buf is not enough[dest_limit=%ld, cur_log_seq=%ld]", dest_limit, entry.seq_);
        }
        else
        {
          memcpy(dest_buf + dest_pos, src_buf + old_pos, size);
          dest_pos += size;
        }
      }

      if (OB_SUCCESS == err)
      {
        src_pos += entry.get_log_data_len();
      }
    }
  }
  if (OB_SUCCESS == err && src_pos != src_len)
  {
    err = OB_ERR_UNEXPECTED;
    TBSYS_LOG(ERROR, "pos[%ld] != len[%ld]", src_pos, src_len);
  }
  if (OB_SUCCESS == err)
  {
    dest_len = dest_pos;
  }
  return err;
}

int select(const char* src_log_file, const char* dest_log_file, const char* id_seq_file)
{
  int err = OB_SUCCESS;
  int64_t src_len = 0;
  int64_t dest_len = 0;
  int64_t id_file_len = 0;
  const char* src_buf = NULL;
  char* dest_buf = NULL;
  const char* id_seq_buf = NULL;
  TBSYS_LOG(DEBUG, "select(src=%s, dest=%s, id_file=%s)", src_log_file, dest_log_file, id_seq_file);
  if (NULL == src_log_file || NULL == dest_log_file || NULL == id_seq_file)
  {
    err = OB_INVALID_ARGUMENT;
  }
  else if (OB_SUCCESS != (err = get_file_len(src_log_file, src_len)))
  {
    TBSYS_LOG(ERROR, "get_file_len(%s)=>%d", src_log_file, err);
  }
  else if (OB_SUCCESS != (err = get_file_len(id_seq_file, id_file_len)))
  {
    TBSYS_LOG(ERROR, "get_file_len(%s)=>%d", id_seq_file, err);
  }
  else if (OB_SUCCESS != (err = file_map_read(src_log_file, src_len, src_buf)))
  {
    TBSYS_LOG(ERROR, "file_map_read(%s)=>%d", src_log_file, err);
  }
  else if (OB_SUCCESS != (err = file_map_read(id_seq_file, id_file_len, id_seq_buf)))
  {
    TBSYS_LOG(ERROR, "file_map_read(%s)=>%d", src_log_file, err);
  }
  else if (OB_SUCCESS != (err = file_map_write(dest_log_file, src_len, dest_buf)))
  {
    TBSYS_LOG(ERROR, "file_map_write(%s)=>%d", dest_log_file, err);
  }
  else if (OB_SUCCESS != (err = select_log_by_id(dest_buf, src_len, dest_len, src_buf, src_len, (const int64_t*)id_seq_buf, id_file_len/sizeof(int64_t))))
  {
    TBSYS_LOG(ERROR, "select_log_by_id()=>%d", err);
  }
  if (0 != (truncate(dest_log_file, dest_len)))
  {
    TBSYS_LOG(ERROR, "truncate(file=%s, len=%ld):%s", dest_log_file, dest_len, strerror(errno));
  }
  return err;
}

#include "cmd_args_parser.h"
#define report_error(err, ...) if (OB_SUCCESS != err)TBSYS_LOG(ERROR, __VA_ARGS__);
int main(int argc, char *argv[])
{
  int err = 0;
  TBSYS_LOGGER.setLogLevel(getenv("log_level")?:"WARN");
  init_log_cmd_str();
  if (OB_SUCCESS != (err = ob_init_memory_pool()))
  {
    TBSYS_LOG(ERROR, "ob_init_memory_pool()=>%d", err);
  }
  else if (OB_NEED_RETRY != (err = CmdCall(argc, argv, read_file_test, StrArg(path), IntArg(offset)):OB_NEED_RETRY))
  {
    report_error(err, "read_file_test()=>%d", err);
  }
  else if (OB_NEED_RETRY != (err = CmdCall(argc, argv, gen_nop, StrArg(log_file), IntArg(n_logs)):OB_NEED_RETRY))
  {
    report_error(err, "gen_nop()=>%d", err);
  }
  else if (OB_NEED_RETRY != (err = CmdCall(argc, argv, reset_id, StrArg(src_log_file), StrArg(dest_log_file), IntArg(start_id)):OB_NEED_RETRY))
  {
    report_error(err, "reset()=>%d", err);
  }
  else if (OB_NEED_RETRY != (err = CmdCall(argc, argv, dump_by_log_reader, StrArg(log_file)):OB_NEED_RETRY))
  {
    report_error(err, "dump()=>%d", err);
  }
  else if (OB_NEED_RETRY != (err = CmdCall(argc, argv, dump, StrArg(log_file)):OB_NEED_RETRY))
  {
    report_error(err, "dump()=>%d", err);
  }
  else if (OB_NEED_RETRY != (err = CmdCall(argc, argv, merge, StrArg(src_log_file), StrArg(dest_log_file)):OB_NEED_RETRY))
  {
    report_error(err, "merge()=>%d", err);
  }
  else if (OB_NEED_RETRY != (err = CmdCall(argc, argv, locate, StrArg(log_dir), IntArg(log_id)):OB_NEED_RETRY))
  {
    report_error(err, "locate()=>%d", err);
  }
  else if (OB_NEED_RETRY != (err = CmdCall(argc, argv, trans2to3, StrArg(src_log_file), StrArg(dest_log_file)):OB_NEED_RETRY))
  {
    report_error(err, "trans2to3()=>%d", err);
  }
  else if (OB_NEED_RETRY != (err = CmdCall(argc, argv, a2i, StrArg(src_file), StrArg(dest_file)):OB_NEED_RETRY))
  {
    report_error(err, "a2i()=>%d", err);
  }
  else if (OB_NEED_RETRY != (err = CmdCall(argc, argv, dumpi, StrArg(src_file)):OB_NEED_RETRY))
  {
    report_error(err, "dumpi()=>%d", err);
  }
  else if (OB_NEED_RETRY != (err = CmdCall(argc, argv, select, StrArg(src_log_file), StrArg(dest_log_file), StrArg(id_seq_file)):OB_NEED_RETRY))
  {
    report_error(err, "select()=>%d", err);
  }
  else
  {
    fprintf(stderr, _usages, argv[0]);
  }
  return err;
}
