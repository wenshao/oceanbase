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

#include "stdlib.h"
#include "tbsys.h"
#include "common/ob_malloc.h"
#include "common/ob_obi_role.h"
#include "common/ob_schema.h"
#include "common/ob_client_config.h"
#include "common/ob_log_cursor.h"
#include "common/ob_scan_param.h"
#include "common/ob_mutator.h"
#include "common/ob_ups_info.h"
#include "updateserver/ob_ups_mutator.h"
#include "ob_utils.h"
#include "ob_client2.h"
#include "file_utils.h"
#include "updateserver/ob_log_sync_delay_stat.h"
#include "updateserver/ob_ups_clog_status.h"

using namespace oceanbase::common;
using namespace oceanbase::updateserver;

const int64_t MAX_BUF_SIZE = 1<<20;
const int64_t MAX_N_COLUMNS = 1<<12;

const char* _usages = "Usages:\n"
  "\t# You can set env var 'log_level' to 'DEBUG'/'WARN'...\n"
  "\t#         set env var 'n_transport' to a number\n"
  "\t%1$s desc rs_ip:rs_port table\n"
  "\t%1$s get_obi_role rs_ip:rs_port\n"
  "\t%1$s get_master_ups rs_ip:rs_port\n"
  "\t%1$s get_client_cfg rs_ip:rs_port\n"
  "\t%1$s get_ms rs_ip:rs_port\n"
  "\t%1$s get_replayed_cursor ups_ip:ups_port\n"
  "\t%1$s get_max_log_seq_replayable ups_ip:ups_port\n"
  "\t%1$s fill_log_cursor ups_ip:ups_port\n"
  "\t%1$s get_last_frozen_version ups_ip:ups_port\n"
  "\t%1$s scan rs_ip:rs_port table columns rowkey limit server\n"
  "\t# default scan args: columns='*', rowkey=[min,max], limit=10, server=ms\n"
  "\t%1$s set rs_ip:rs_port table column rowkey value server\n"
  "\t# default set args: server=ms\n"
  "\t%1$s randset rs table=any server=ups n_row=-1\n"
  "\t%1$s stress rs server=ups|ms|ip:port duration=-1 start=write_start_id end=-1 write=1 scan=1 mget=1 write_size=1 scan_size=1 mget_size=1\n"
  "\t# default set args: server=ms\n"
  "\t%1$s send_mutator rs log_file server=ups\n"
  "\t%1$s get_log_sync_delay_stat ups\n"
  "\t%1$s get_clog_status ups\n";

struct ServerList
{
  static const int64_t MAX_N_SERVERS = 1<<5;
  int32_t n_servers_;
  ObServer servers_[MAX_N_SERVERS];
  ServerList(): n_servers_(0) {}
  ~ServerList(){}
  int serialize(char* buf, int64_t len, int64_t& pos) const
  {
    UNUSED(buf); UNUSED(len); UNUSED(pos);
    return OB_NOT_SUPPORTED;
  }
  int deserialize(char* buf, int64_t len, int64_t& pos)
  {
    int err = OB_SUCCESS;
    int64_t reserved = 0;
    if (OB_SUCCESS != (err = serialization::decode_vi32(buf, len, pos, &n_servers_)))
    {
      TBSYS_LOG(ERROR, "deserialize server_num error, err=%d", err);
    }
    for(int64_t i = 0; OB_SUCCESS == err && i < n_servers_; i++)
    {
      if (OB_SUCCESS != (err = servers_[i].deserialize(buf, len, pos)))
      {
        TBSYS_LOG(ERROR, "deserialize %ldth SERVER error, ret=%d", i, err);
      }
      else if (OB_SUCCESS != (err = serialization::decode_vi64(buf, len, pos, &reserved)))
      {
        TBSYS_LOG(ERROR, "deserialize reserve field error, ret=%d", err);
      }
    }
    return err;
  }
  char* to_str(char* buf, int64_t len, int64_t& pos)
  {
    int err = OB_SUCCESS;
    char* ret_str = buf + pos;
    if (OB_SUCCESS != (err = strformat(buf, len, pos, "n_server=%d ", n_servers_)))
    {
      TBSYS_LOG(ERROR, "strformat()=>%d", err);
    }
    for(int64_t i = 0; i < n_servers_; i++)
    {
      if (OB_SUCCESS != (err = strformat(buf, len, pos, "%s ", servers_[i].to_cstring())))
      {
        TBSYS_LOG(ERROR, "strformat()=>%d", err);
      }
    }
    if (OB_SUCCESS != err)
    {
      ret_str = NULL;
    }
    return ret_str;
  }
};

class RPC;
class ClientWorker
{
  public:
    ClientWorker(): stop_(false), keep_going_on_err_(false), rpc_(NULL), schema_mgr_(NULL),
                    rs_(NULL), server_(NULL), start_(0), end_(0),
                    write_thread_count_(0), scan_thread_count_(0), mget_thread_count_(0),
                    write_req_size_(0), scan_req_size_(0), mget_req_size_(0),
                    finish_write_thread_count_(0), finish_scan_thread_count_(0), finish_mget_thread_count_(0),
                    write_seq_(0), scan_seq_(0), mget_seq_(0),
                    write_fail_count_(0), scan_fail_count_(0), mget_fail_count_(0)
  {}
    ~ClientWorker() { stop(); wait(); }
    const static int64_t MAX_N_THREADS = 1<<16;
    volatile bool stop_;
    bool keep_going_on_err_;
    RPC* rpc_;
    ObSchemaManagerV2* schema_mgr_;
    const char* rs_;
    const char* server_;
    int64_t start_;
    int64_t end_;
    int64_t write_thread_count_;
    int64_t scan_thread_count_;
    int64_t mget_thread_count_;
    int64_t write_req_size_;
    int64_t scan_req_size_;
    int64_t mget_req_size_;
    volatile int64_t finish_write_thread_count_;
    volatile int64_t finish_scan_thread_count_;
    volatile int64_t finish_mget_thread_count_;
    volatile int64_t write_seq_;
    volatile int64_t scan_seq_;
    volatile int64_t mget_seq_;
    volatile int64_t write_fail_count_;
    volatile int64_t scan_fail_count_;
    volatile int64_t mget_fail_count_;
    pthread_t threads_[MAX_N_THREADS];

    int init(RPC* rpc, const char* rs,
             const char* server, int64_t start, int64_t end,
             int64_t write_thread_count, int64_t scan_thread_count, int64_t mget_thread_count,
             int64_t write_req_size, int64_t scan_req_size, int64_t mget_req_size) {
      int err = OB_SUCCESS;
      if (NULL == rpc || NULL == rs || NULL == server || 0 >= start
          || 0 > write_thread_count || 0 > scan_thread_count || 0 > mget_thread_count
          || write_thread_count + scan_thread_count + mget_thread_count > MAX_N_THREADS)
      {
        err = OB_INVALID_ARGUMENT;
      }
      else
      {
        rpc_ = rpc;
        rs_ = rs;
        server_ = server;
        write_seq_ = start;
        start_ = start;
        end_ = end;
        write_thread_count_ = write_thread_count;
        scan_thread_count_ = scan_thread_count;
        mget_thread_count_ = mget_thread_count;
        write_req_size_ = write_req_size;
        scan_req_size_ = scan_req_size;
        mget_req_size_ = mget_req_size;
        keep_going_on_err_ = 0 == strcmp(getenv("keep_going_on_err")?: "true", "true");
      }
      return err;
    }

    bool is_inited() {
      return NULL != rpc_ && NULL != rs_ && NULL != server_ && 0 < write_seq_;
    }
    int64_t get_thread_count() {
      return write_thread_count_ + scan_thread_count_ + mget_thread_count_;
    }
    int start() {
      int err = OB_SUCCESS;
      if (!is_inited()){
        err = OB_NOT_INIT;
      }
      else if (OB_SUCCESS != (err = fetch_schema()))
      {
        TBSYS_LOG(ERROR, "fetch_schema()=>%d", err);
      }
      for(int64_t i = 0; OB_SUCCESS == err && i < get_thread_count(); i++) {
        if (0 != pthread_create(threads_ + i, NULL, (void*(*)(void*))ClientWorker::do_work, this)){
          err = OB_ERR_UNEXPECTED;
          TBSYS_LOG(ERROR, "pthread_create(%ld): %s", i, strerror(errno));
        }
      }
      return err;
    }

    int stop() {
      stop_ = true;
      return OB_SUCCESS;
    }
    int wait() {
      int64_t n_err = 0;
      int64_t err = 0;
      for(int64_t i = 0; i < get_thread_count(); i++)
      {
        pthread_join(threads_[i], (void**)&err);
        if (OB_SUCCESS != err){
          TBSYS_LOG(ERROR, "threads[%ld]=>%ld", i, err);
          n_err ++;
        }
      }
      TBSYS_LOG(INFO, "all worker finish: n_err=%ld", n_err);
      return static_cast<int>(err);
    }
    bool is_finish() {
      return finish_write_thread_count_ + finish_scan_thread_count_ + finish_mget_thread_count_ == get_thread_count();
    }
  protected:
    static int do_work(ClientWorker* self){
      int err = OB_SUCCESS;
      err = self->run();
      return err;
    }
    int get_thread_idx() {
      int64_t idx = -1;
      pthread_t self = pthread_self();
      for(int64_t i = 0; i < get_thread_count(); i++)
      {
        if (self == threads_[i])
        {
          idx = i;
          break;
        }
      }
      return static_cast<int>(idx);
    }
    int run() {
      int err = OB_SUCCESS;
      int64_t idx = get_thread_idx();
      if (idx < 0 || idx > get_thread_count())
      {
        TBSYS_LOG(ERROR, "invalid thread idx[%ld]", idx);
      }
      else if (idx < write_thread_count_)
      {
        TBSYS_LOG(INFO, "write_worker[%ld] start.", idx);
        err = write(get_thread_idx());
        __sync_fetch_and_add(&finish_write_thread_count_, 1);
        TBSYS_LOG(INFO, "write_worker[%ld] finished[%d]", idx, err);
      }
      else if (idx < write_thread_count_ + scan_thread_count_)
      {
        TBSYS_LOG(INFO, "scan_worker[%ld] start.", idx);
        err = scan(get_thread_idx());
        __sync_fetch_and_add(&finish_scan_thread_count_, 1);
        TBSYS_LOG(INFO, "scan_worker[%ld] finished[%d]", idx, err);
      }
      else
      {
        TBSYS_LOG(INFO, "mget_worker[%ld] start.", idx);
        err = mget(get_thread_idx());
        __sync_fetch_and_add(&finish_mget_thread_count_, 1);
        TBSYS_LOG(INFO, "mget_worker[%ld] finished[%d]", idx, err);
      }
      return err;
    }
    int is_ups(bool& ret, ObServer& server);
    int fetch_schema();
    int write(int64_t idx);
    int scan(int64_t idx);
    int mget(int64_t idx);
  public:
    class Monitor
    {
      public:
        const static int64_t MONITOR_INTERVAL_US = 1000000;
      public:
        Monitor(ClientWorker* worker): worker_(worker), start_time_(0),
                                       start_write_(0), start_scan_(0), start_mget_(0),
                                       last_time_(0), last_write_(0), last_scan_(0), last_mget_(0)
      {}
        ~Monitor() {}
        int report(){
          int err = OB_SUCCESS;
          int64_t last_time0 = last_time_;
          int64_t last_write0 = last_write_;
          int64_t last_scan0 = last_scan_;
          int64_t last_mget0 = last_mget_;
          last_time_ = tbsys::CTimeUtil::getTime(); 
          last_write_ = worker_->write_seq_;
          last_scan_ = worker_->scan_seq_;
          last_mget_ = worker_->mget_seq_;
          int64_t this_duration = last_time_ - last_time0;
          int64_t total_duration = last_time_ - start_time_;
          TBSYS_LOG(INFO, "worker report: write=[total=%ld:%ld][tps=%ld:%ld][thread=%ld:%ld], scan=[total=%ld:%ld][qps=%ld:%ld][thread=%ld:%ld], mget=[total=%ld:%ld][qps=%ld:%ld][thread=%ld:%ld]",
                    last_write_, worker_->write_fail_count_, 1000000 * (last_write_ - last_write0)/(this_duration + 1),
                    1000000 * (last_write_ - start_write_)/(total_duration + 1), worker_->write_thread_count_, worker_->finish_write_thread_count_,
                    last_scan_, worker_->scan_fail_count_, 1000000 * (last_scan_ - last_scan0)/(this_duration + 1),
                    1000000 * (last_scan_ - start_scan_)/total_duration, worker_->scan_thread_count_, worker_->finish_scan_thread_count_,
                    last_mget_, worker_->mget_fail_count_, 1000000 * (last_mget_ - last_mget0)/(this_duration + 1),
                    1000000 * (last_mget_ - start_mget_)/(total_duration + 1), worker_->mget_thread_count_, worker_->finish_mget_thread_count_);
          return err;
        }
        int monitor(int64_t duration) {
          int err = OB_SUCCESS;
          int64_t interval = MONITOR_INTERVAL_US;
          start_time_ = tbsys::CTimeUtil::getTime(); 
          start_write_ = worker_->write_seq_;
          start_scan_ = worker_->scan_seq_;
          start_mget_ = worker_->mget_seq_;
          while(!worker_->is_finish() && OB_SUCCESS == err && (duration < 0 || last_time_ < start_time_ + duration)){
            usleep(static_cast<useconds_t>(interval));
            err = report();
          }
          return err;
        }
      private:
        ClientWorker* worker_;
        int64_t start_time_;
        int64_t start_write_;
        int64_t start_scan_;
        int64_t start_mget_;
        int64_t last_time_;
        int64_t last_write_;
        int64_t last_scan_;
        int64_t last_mget_;
    };
    friend class Monitor;
};

int desc_tables(ObDataBuffer& buf, ObSchemaManagerV2* schema_mgr, const char* table_name, const char*& result)
{
  int err = OB_SUCCESS;
  int n_column = 0;
  ObTableSchema* table_schema = NULL;
  const ObColumnSchemaV2* columns = NULL;
  const char* tmp_result = buf.get_data() + buf.get_position();
  TBSYS_LOG(INFO, "desc_tables(table_name='%s')", table_name);
  if (NULL == schema_mgr)
  {
    err = OB_INVALID_ARGUMENT;
    TBSYS_LOG(ERROR, "schema_mgr == NULL");
  }
  else if (NULL == table_name || 0 == strcmp(table_name, "*"))
  {
    for (const ObTableSchema* it=schema_mgr->table_begin();
         OB_SUCCESS == err && it != schema_mgr->table_end(); ++it)
    {
      if (OB_SUCCESS != (err = strformat(buf, "[%ld %s] ", it->get_table_id(), it->get_table_name())))
      {
        TBSYS_LOG(ERROR, "strfomat()=>%d", err);
      }
    }
  }
  else
  {
    if (NULL == (table_schema = schema_mgr->get_table_schema(table_name)))
    {
      err = OB_SCHEMA_ERROR;
      TBSYS_LOG(ERROR, "schema_mgr->get_table_schema(table_name=%s)=>NULL", table_name);
    }
    else if (NULL == (columns = schema_mgr->get_table_schema(table_schema->get_table_id(), n_column)))
    {
      err = OB_SCHEMA_ERROR;
      TBSYS_LOG(ERROR, "schema_mgr->get_table_schema(table_name=%s)=>NULL", table_name);
    }
    for(int i = 0; OB_SUCCESS == err && i < n_column; i++)
    {
      if (OB_SUCCESS != (err = strformat(buf, "[%ld %s %s] ", columns[i].get_id(), columns[i].get_name(), obj_type_repr(columns[i].get_type()))))
      {
        TBSYS_LOG(ERROR, "strformat()=>%d", err);
      }
    }
  }
  if (OB_SUCCESS == err)
  {
    result = tmp_result;
  }
  return err;
}

int make_version_range(ObVersionRange& version_range, int64_t start_version)
{
  int err = OB_SUCCESS;
  if (start_version > 0)
  {
    version_range.border_flag_.set_inclusive_start();
    version_range.border_flag_.unset_inclusive_end();
    version_range.border_flag_.unset_min_value();
    version_range.border_flag_.set_max_value();
    version_range.start_version_ = start_version;
    version_range.end_version_ = 0;
  }
  return err;
}

int set_range(ObDataBuffer& buf, ObScanParam& scan_param, ObString& table_name, int64_t start_version, const char* start_key, const char* end_key)
{
  int err = OB_SUCCESS;
  ObRange range;
  ObVersionRange version_range;
  if (0 == strcmp(start_key, "min"))
  {
    TBSYS_LOG(INFO, "start_key is min");
    range.border_flag_.set_min_value();
  }
  else if (OB_SUCCESS != (err = hex2bin(buf, range.start_key_, start_key, strlen(start_key))))
  {
    TBSYS_LOG(ERROR, "hex2bin()=>%d", err);
  }
  if (0 == strcmp(end_key, "max"))
  {
    TBSYS_LOG(INFO, "end_key is max");
    range.border_flag_.set_max_value();
  }
  else if (OB_SUCCESS != (err = hex2bin(buf, range.end_key_, end_key, strlen(end_key))))
  {
    TBSYS_LOG(ERROR, "hex2bin()=>%d", err);
  }
  range.border_flag_.set_inclusive_start();
  range.border_flag_.set_inclusive_end();
  make_version_range(version_range, start_version);
  scan_param.set_version_range(version_range);
  scan_param.set(OB_INVALID_ID, table_name, range);
  TBSYS_LOG(DEBUG, "scan_param{table_id=%ld, table_name=%.*s}", scan_param.get_table_id(),
            scan_param.get_table_name().length(), scan_param.get_table_name().ptr());
  return err;
}

int add_columns_to_scan_param(ObDataBuffer& buf, ObScanParam& scan_param, int n_columns, char** const columns)
{
  int err = OB_SUCCESS;
  ObString str;
  for (int i = 0; OB_SUCCESS == err && i < n_columns; i++)
  {
    if (OB_SUCCESS != (err = alloc_str(buf, str, columns[i])))
    {
      TBSYS_LOG(ERROR, "alloc_str(%s)=>%d", columns[i], err);
    }
    else if (OB_SUCCESS != (err = scan_param.add_column(str)))
    {
      TBSYS_LOG(ERROR, "scan_param.add_column(%.*s)=>%d", str.length(), str.ptr(), err);
    }
  }
  return err;
}

int scan_func(ObDataBuffer& buf, ObScanParam& scan_param, int64_t start_version, const char* _table_name,
               int n_columns, char** const columns, char* start_key, char* end_key, int64_t limit)
{
  int err = OB_SUCCESS;
  ObString table_name;
  scan_param.reset();
  if (OB_SUCCESS != (err = alloc_str(buf, table_name, _table_name)))
  {
    TBSYS_LOG(ERROR, "alloc_str(%s)=>%d", _table_name, err);
  }
  else if (OB_SUCCESS != (err = set_range(buf, scan_param, table_name, start_version, start_key, end_key)))
  {
    TBSYS_LOG(ERROR, "set_range(table_name=%.*s, start_version=%ld)=>%d",
              table_name.length(), table_name.ptr(), start_version, err);
  }
  else if (((1 == n_columns && strcmp("*", columns[0])) || 1 < n_columns)
           && OB_SUCCESS != (err = add_columns_to_scan_param(buf, scan_param, n_columns, columns)))
  {
    TBSYS_LOG(ERROR, "add_columns_to_scan_param()=>%d", err);
  }
  else if (OB_SUCCESS != (err = scan_param.set_limit_info(0, limit)))
  {
    TBSYS_LOG(ERROR, "scan_param.set_limit_info(offset=%d, limit=%ld)=>%d", 0, limit, err);
  }
  return err;
}

int scan_func2(ObDataBuffer& buf, ObScanParam& scan_param, int64_t start_version,
               const char* table, const char* columns_spec, const char* rowkey_range, int64_t limit)
{
  int err = OB_SUCCESS;
  int n_columns = 0;
  char* columns[MAX_N_COLUMNS];
  char* start_key = NULL;
  char* end_key = NULL;
  TBSYS_LOG(INFO, "scan(start_version=%ld, table='%s', columns='%s', rowkey='%s')", start_version, table, columns_spec, rowkey_range);
  if (NULL == table || NULL == columns_spec || NULL == rowkey_range)
  {
    err = OB_INVALID_ARGUMENT;
    TBSYS_LOG(ERROR, "select(table=%s, columns=%s, rowkey=%s)=>%d", table, columns_spec, rowkey_range, err);
  }
  else if (OB_SUCCESS != (err = split(buf, columns_spec, ", ", ARRAYSIZEOF(columns), n_columns, columns)))
  {
    TBSYS_LOG(ERROR, "split(column_spec=%s)=>%d", columns_spec, err);
  }
  else if (OB_SUCCESS != (err = parse_range(buf, rowkey_range, start_key, end_key)))
  {
    TBSYS_LOG(ERROR, "parse_range(%s)=>%d", rowkey_range, err);
  }
  else if (OB_SUCCESS != (err = scan_func(buf, scan_param, start_version, table, n_columns, columns, start_key, end_key, limit)))
  {
    TBSYS_LOG(ERROR, "scan_table(table_schema=%s, columns=%s, rowkey=[%s,%s])=>%d", table, columns_spec, start_key, end_key, err);
  }
  return err;
}

int set_obj(ObDataBuffer& buf, ObObj& obj, ColumnType type, const char* value)
{
  int err = OB_SUCCESS;
  ObString str_value;
  switch(type)
  {
    case ObIntType:
      obj.set_int(atoll(value));
      break;
    case ObBoolType:
      obj.set_bool(atoi(value));
      break;
    case ObVarcharType:
      if (OB_SUCCESS != (err = alloc_str(buf, str_value, value)))
      {
        TBSYS_LOG(ERROR, "alloc_str(%s)=>%d", value, err);
      }
      else
      {
        obj.set_varchar(str_value);
      }
      break;
    case ObFloatType:
      obj.set_float(static_cast<float>(atof(value)));
      break;
    case ObDoubleType:
      obj.set_double(atof(value));
      break;
    case ObDateTimeType:
      obj.set_datetime(atoll(value));
      break;
    case ObPreciseDateTimeType:
      obj.set_precise_datetime(atoll(value));
      break;
    case ObCreateTimeType:
    case ObModifyTimeType:
    default:
      err = OB_NOT_SUPPORTED;
      break;
  }
  return err;
}

int mutate_func(ObDataBuffer& buf, ObMutator& mutator, ObSchemaManagerV2& schema_mgr,
                const char* _table_name, const char* _rowkey, const char* _column_name, const char* _value)
{
  int err = OB_SUCCESS;
  ObString table_name;
  ObString rowkey;
  ObString column_name;
  const ObColumnSchemaV2* column_schema = NULL;
  ObString value;
  ObObj cell_value;
  mutator.reset();
  if (OB_SUCCESS != (err = alloc_str(buf, table_name, _table_name)))
  {
    TBSYS_LOG(ERROR, "alloc_str(%s)=>%d", _table_name, err);
  }
  else if (OB_SUCCESS != (err = hex2bin(buf, rowkey, _rowkey, strlen(_rowkey))))
  {
    TBSYS_LOG(ERROR, "hex2bin()=>%d", err);
  }
  else if (OB_SUCCESS != (err = alloc_str(buf, column_name, _column_name)))
  {
    TBSYS_LOG(ERROR, "alloc_str(%s)=>%d", _column_name, err);
  }
  else if (NULL == (column_schema = schema_mgr.get_column_schema(_table_name, _column_name)))
  {
    err = OB_SCHEMA_ERROR;
    TBSYS_LOG(ERROR, "schema_mgr.get_column_schema(table=%s, column=%s): NO SCHEMA FOUND",
              _table_name, _column_name);
  }
  else if (OB_SUCCESS != (err = set_obj(buf, cell_value, column_schema->get_type(), _value)))
  {
    TBSYS_LOG(ERROR, "set_obj()=>%d", err);
  }
  else if (OB_SUCCESS != (err = mutator.update(table_name, rowkey, column_name, cell_value)))
  {
    TBSYS_LOG(ERROR, "mutator.update()=>%d", err);
  }
  return err;
}

int choose_table(ObDataBuffer& buf, ObSchemaManagerV2& schema_mgr, 
                 ObString& table_name, const char* _table_name, int64_t seed)
{
  int err = OB_SUCCESS;
  int64_t w = -1;
  int64_t max_weight = -1;
  const ObTableSchema* chosen_table = NULL;
  if (NULL == _table_name)
  {
    err = OB_INVALID_ARGUMENT;
  }
  else if (0 != strcmp(_table_name, "any"))
  {
    if (OB_SUCCESS != (err = alloc_str(buf, table_name, _table_name)))
    {
      TBSYS_LOG(ERROR, "alloc_str(%s)=>%d", _table_name, err);
    }
  }
  else
  {
    w = seed;
    for(const ObTableSchema* table_schema = schema_mgr.table_begin();
        table_schema != schema_mgr.table_end(); table_schema++)
    {
      TBSYS_LOG(DEBUG, "choose_table(cur=%s[%ld])", table_schema->get_table_name(), table_schema->get_table_id());
      if (table_schema->get_table_id() >= OB_APP_MIN_TABLE_ID
          && (w = rand2(w)) > max_weight)
      {
        max_weight = w;
        chosen_table = table_schema;
        //break; // always choose first user table, otherwise build_rand_scan_param() will not work
      }
    }
    if (NULL == chosen_table)
    {
      err = OB_ENTRY_NOT_EXIST;
      TBSYS_LOG(ERROR, "no table been chosen");
    }
    else if (OB_SUCCESS != (err = alloc_str(buf, table_name, chosen_table->get_table_name())))
    {
      TBSYS_LOG(ERROR, "alloc_str(%s)=>%d", chosen_table->get_table_name(), err);
    }
  }
  return err;
}

int choose_rowkey(ObDataBuffer& buf, ObSchemaManagerV2& schema_mgr, 
                  const ObString& table_name, ObString& rowkey, int64_t seed)
{
  int err = OB_SUCCESS;
  const ObTableSchema* table_schema = NULL;
  char _rowkey[OB_MAX_ROW_KEY_LENGTH];
  if (NULL == (table_schema = schema_mgr.get_table_schema(table_name)))
  {
    err = OB_SCHEMA_ERROR;
  }
  else if (OB_SUCCESS != (err = rand_str(_rowkey, table_schema->get_rowkey_max_length() + 1, seed)))
  {
    TBSYS_LOG(ERROR, "rand_str(len=%ld)=>%d", sizeof(_rowkey), err);
  }
  else if (OB_SUCCESS != (err = alloc_str(buf, rowkey, _rowkey)))
  {
    TBSYS_LOG(ERROR, "alloc_str(%s)=>%d", _rowkey, err);
  }
  TBSYS_LOG(DEBUG, "choose_rowkey('%*s')", rowkey.length(), rowkey.ptr());
  return err;
}

int64_t rand_choose_column(const ObColumnSchemaV2* column_schema, int64_t n_column, int64_t seed)
{
  int64_t column_id = -1;
  while(0 > column_id)
  {
    column_id = (seed = rand2(seed)) % n_column;
    if (column_schema[column_id].get_join_info())
    {
      column_id = -1;
    }
    else
    {
      switch(column_schema[column_id].get_type())
      {
        case ObIntType:
        case ObFloatType:
        case ObVarcharType:
          break;
        default:
          column_id = -1;
          break;
      }
    }
  }
  return column_id;
}

int choose_column(ObDataBuffer& buf, ObSchemaManagerV2& schema_mgr, 
                  const ObString& table_name, ObString& column_name, int64_t seed)
{
  int err = OB_SUCCESS;
  int64_t n_column = 0;
  const ObTableSchema* table_schema = NULL;
  const ObColumnSchemaV2* column_schema = NULL;
  if (NULL == (table_schema = schema_mgr.get_table_schema(table_name)))
  {
    err = OB_SCHEMA_ERROR;
  }
  else if (NULL == (column_schema = schema_mgr.get_table_schema(table_schema->get_table_id(), (int32_t&)n_column)))
  {
    err = OB_SCHEMA_ERROR;
    TBSYS_LOG(ERROR, "schema_mgr.get_table_schema(table_id=%ld)=>%d", table_schema->get_table_id(), err);
  }
  else if (OB_SUCCESS != (err = alloc_str(buf, column_name, column_schema[rand_choose_column(column_schema, n_column, seed)].get_name())))
  {
    TBSYS_LOG(ERROR, "alloc_str()=>%d", err);
  }
  TBSYS_LOG(DEBUG, "choose_column('%*s')", column_name.length(), column_name.ptr());
  return err;
}

int choose_value(ObDataBuffer& buf, ObSchemaManagerV2& schema_mgr, 
                 const ObString& table_name, ObString& column_name, ObObj& value, int64_t seed)
{
  int err = OB_SUCCESS;
  const ObColumnSchemaV2* column_schema = NULL;
  char _str[1<<4];
  ObString str;
  if (NULL == (column_schema = schema_mgr.get_column_schema(table_name, column_name)))
  {
    err = OB_SCHEMA_ERROR;
  }
  else
  {
    switch(column_schema->get_type())
    {
      case ObIntType:
        value.set_int(rand2(seed));
        break;
      case ObFloatType:
        value.set_float(static_cast<float>(1.0) * static_cast<float>(rand2(seed))/static_cast<float>(RAND_MAX));
        break;
      case ObVarcharType:
        if (OB_SUCCESS != (err = rand_str(_str, sizeof(_str), seed)))
        {
          TBSYS_LOG(ERROR, "rand_str(len=%ld)=>%d", sizeof(_str), err);
        }
        else if (OB_SUCCESS != (err = alloc_str(buf, str, _str)))
        {
          TBSYS_LOG(ERROR, "alloc_str(str=%s)=>%d", _str, err);
        }
        else
        {
          value.set_varchar(str);
        }
        break;
      default:
        break;
    }
  }
  TBSYS_LOG(DEBUG, "choose_value(table=%*s, column=%*s)", table_name.length(), table_name.ptr(),
            column_name.length(), column_name.ptr());
  return err;
}

int build_rand_mutator(ObDataBuffer& buf, ObMutator& mutator, int64_t seed, ObSchemaManagerV2& schema_mgr,
                       const char* _table_name)
{
  int err = OB_SUCCESS;
  ObString table_name;
  ObString rowkey;
  ObString column_name;
  ObObj cell_value;
  if (OB_SUCCESS != (err = choose_table(buf, schema_mgr, table_name, _table_name, seed)))
  {
    TBSYS_LOG(ERROR, "choose_table(table=%s)=>%d", _table_name, err);
  }
  else if (OB_SUCCESS != (err = choose_rowkey(buf, schema_mgr, table_name, rowkey, seed)))
  {
    TBSYS_LOG(ERROR, "choose_rowkey(table=%*s)=>%d", table_name.length(), table_name.ptr(), err);
  }
  else if (OB_SUCCESS != (err = choose_column(buf, schema_mgr, table_name, column_name, seed)))
  {
    TBSYS_LOG(ERROR, "choose_column(table=%*s)=>%d", table_name.length(), table_name.ptr(), err);
  }
  else if (OB_SUCCESS != (err = choose_value(buf, schema_mgr, table_name, column_name, cell_value, seed)))
  {
    TBSYS_LOG(ERROR, "choose_value(table=%*s, column=%*s)=>%d", table_name.length(), table_name.ptr(),
              column_name.length(), column_name.ptr(), err);
  }
  else if (OB_SUCCESS != (err = mutator.update(table_name, rowkey, column_name, cell_value)))
  {
    TBSYS_LOG(ERROR, "mutator.update()=>%d", err);
  }
  return err;
}

int build_rand_batch_mutator(ObDataBuffer& buf, ObMutator& mutator, int64_t start, int64_t end, ObSchemaManagerV2& schema_mgr,
                             const char* _table_name)
{
  int err = OB_SUCCESS;
  for(int64_t i = start; i < end; i++)
  {
    err = build_rand_mutator(buf, mutator, i, schema_mgr, _table_name);
  }
  return err;
}

int add_cell_to_get_param(ObGetParam& get_param, ObString& table_name, ObString& row_key, ObString& column_name)
{
  int err = OB_SUCCESS;
  ObCellInfo cell_info;
  cell_info.table_name_ = table_name;
  cell_info.row_key_ = row_key;
  cell_info.column_name_ = column_name;
  err = get_param.add_cell(cell_info);
  return err;
}

int build_rand_get_param(ObDataBuffer& buf, ObGetParam& get_param, int64_t seed, ObSchemaManagerV2& schema_mgr,
                       const char* _table_name)
{
  int err = OB_SUCCESS;
  ObString table_name;
  ObString rowkey;
  ObString column_name;
  if (OB_SUCCESS != (err = choose_table(buf, schema_mgr, table_name, _table_name, seed)))
  {
    TBSYS_LOG(ERROR, "choose_table(table=%s)=>%d", _table_name, err);
  }
  else if (OB_SUCCESS != (err = choose_rowkey(buf, schema_mgr, table_name, rowkey, seed)))
  {
    TBSYS_LOG(ERROR, "choose_rowkey(table=%*s)=>%d", table_name.length(), table_name.ptr(), err);
  }
  else if (OB_SUCCESS != (err = choose_column(buf, schema_mgr, table_name, column_name, seed)))
  {
    TBSYS_LOG(ERROR, "choose_column(table=%*s)=>%d", table_name.length(), table_name.ptr(), err);
  }
  else if (OB_SUCCESS != (err = add_cell_to_get_param(get_param, table_name, rowkey, column_name)))
  {
    TBSYS_LOG(ERROR, "get_param.add_cell()=>%d", err);
  }
  return err;
}

int build_rand_mget_param(ObDataBuffer& buf, ObGetParam& get_param, int64_t start, int64_t end, ObSchemaManagerV2& schema_mgr,
                          const char* _table_name, int64_t start_version)
{
  int err = OB_SUCCESS;
  ObVersionRange version_range;
  if (0 >= start_version)
  {
    TBSYS_LOG(DEBUG, "no need to set start_version");
  }
  else if (OB_SUCCESS != (err = make_version_range(version_range, start_version)))
  {
    TBSYS_LOG(ERROR, "make_version_range(%ld)=>%d", start_version, err);
  }
  else
  {
    get_param.set_version_range(version_range);
  }
  for(int64_t i = start; OB_SUCCESS == err && i < end; i++)
  {
    err = build_rand_get_param(buf, get_param, i, schema_mgr, _table_name);
  }
  if (OB_SUCCESS == err)
  {
    get_param.set_is_read_consistency(false);
  }
  return err;
}

int set_range2(ObScanParam& scan_param, ObString& table_name, int64_t start_version,
              ObString& start_key, ObString& end_key)
{
  int err = OB_SUCCESS;
  ObRange range;
  ObVersionRange version_range;
  range.start_key_ = start_key;
  range.end_key_ = end_key;
  range.border_flag_.set_inclusive_start();
  range.border_flag_.set_inclusive_end();
  make_version_range(version_range, start_version);
  scan_param.set_version_range(version_range);
  scan_param.set(OB_INVALID_ID, table_name, range);
  return err;
}

int build_rand_scan_param(ObDataBuffer& buf, ObScanParam& scan_param, int64_t start, int64_t end,
                          ObSchemaManagerV2& schema_mgr, const char* _table_name, int64_t start_version)
{
  int err = OB_SUCCESS;
  int64_t limit = 200;
  ObString table_name;
  ObString rowkey;
  ObString column_name;
  ObString start_key;
  ObString end_key;
  if (OB_SUCCESS != (err = choose_table(buf, schema_mgr, table_name, _table_name, start)))
  {
    TBSYS_LOG(ERROR, "choose_table(table=%s)=>%d", _table_name, err);
  }
  else if (OB_SUCCESS != (err = choose_rowkey(buf, schema_mgr, table_name, start_key, start)))
  {
    TBSYS_LOG(ERROR, "choose_rowkey(table=%*s)=>%d", table_name.length(), table_name.ptr(), err);
  }
  else if (OB_SUCCESS != (err = choose_rowkey(buf, schema_mgr, table_name, end_key, end)))
  {
    TBSYS_LOG(ERROR, "choose_rowkey(table=%*s)=>%d", table_name.length(), table_name.ptr(), err);
  }
  else if (OB_SUCCESS != (err = set_range2(scan_param, table_name, start_version, start_key, end_key)))
  {
    TBSYS_LOG(ERROR, "set_range(table_name=%.*s, start_version=%ld)=>%d",
              table_name.length(), table_name.ptr(), start_version, err);
  }
  else if (OB_SUCCESS != (err = scan_param.set_limit_info(0, limit)))
  {
    TBSYS_LOG(ERROR, "scan_param.set_limit_info(offset=%d, limit=%ld)=>%d", 0, limit, err);
  }
  else
  {
    scan_param.set_is_read_consistency(false);
  }
  return err;
}

int cell_info_resolve_table_name(ObSchemaManagerV2& sch_mgr, ObCellInfo& cell)
{
  int err = OB_SUCCESS;
  uint64_t table_id = cell.table_id_;
  const ObTableSchema* table_schema = NULL;
  const char* table_name = NULL;
  // `table_id == OB_INVALID_ID' is possible when cell.op_type == OB_USE_OB or cell.op_type == OB_USE_DB
  if (OB_INVALID_ID != table_id)
  {
    if (NULL == (table_schema = sch_mgr.get_table_schema(table_id)))
    {
      err = OB_SCHEMA_ERROR;
      TBSYS_LOG(WARN, "sch_mge.get_table_schema(table_id=%lu)=>NULL", table_id);
    }
    else if (NULL == (table_name = table_schema->get_table_name()))
    {
      err = OB_SCHEMA_ERROR;
      TBSYS_LOG(ERROR, "get_table_name(table_id=%lu) == NULL", table_id);
    }
    else
    {
      cell.table_name_.assign_ptr((char*)table_name, static_cast<int32_t>(strlen(table_name)));
      //cell.table_id_ = OB_INVALID_ID;
    }
  }
  return err;
}

int cell_info_resolve_column_name(ObSchemaManagerV2& sch_mgr, ObCellInfo& cell)
{
  int err = OB_SUCCESS;
  uint64_t table_id = cell.table_id_;
  uint64_t column_id = cell.column_id_;
  const ObColumnSchemaV2* column_schema = NULL;
  const char* column_name = NULL;
  // `table_id == OB_INVALID_ID' is possible when cell.op_type == OB_USE_OB or cell.op_type == OB_USE_DB
  // `column_id == OB_INVALID_ID' is possible when cell.op_type == OB_USE_OB or cell.op_type == OB_USE_DB
  //                                                        or cell.op_type == OB_DEL_ROW
  if (OB_INVALID_ID != table_id && OB_INVALID_ID != column_id)
  {
    if (NULL == (column_schema = sch_mgr.get_column_schema(table_id, column_id)))
    {
      err = OB_SCHEMA_ERROR;
      TBSYS_LOG(ERROR, "sch_mgr.get_column_schema(table_id=%lu, column_id=%lu) == NULL", table_id, column_id);
    }
    else if(NULL == (column_name = column_schema->get_name()))
    {
      err = OB_SCHEMA_ERROR;
      TBSYS_LOG(ERROR, "get_column_name(table_id=%lu, column_id=%lu) == NULL", table_id, column_id);
    }
    else
    {
      cell.column_name_.assign_ptr((char*)column_name, static_cast<int32_t>(strlen(column_name)));
      //cell.column_id_ = OB_INVALID_ID;
    }
  }
  return err;
}

static void dump_ob_mutator_cell(ObMutatorCellInfo& cell)
{
  uint64_t op = cell.op_type.get_ext();
  uint64_t table_id = cell.cell_info.table_id_;
  uint64_t column_id = cell.cell_info.column_id_;
  ObString table_name = cell.cell_info.table_name_;
  ObString column_name = cell.cell_info.column_name_;
  TBSYS_LOG(INFO, "cell{op=%lu, table=%lu[%*s], column=%lu[%*s]", op,
            table_id, table_name.length(), table_name.ptr(), column_id, column_name.length(), column_name.ptr());
}

int dump_ob_mutator(ObMutator& mut)
{
  int err = OB_SUCCESS;
  TBSYS_LOG(DEBUG, "dump_ob_mutator");
  mut.reset_iter();
  while (OB_SUCCESS == err && OB_SUCCESS == (err = mut.next_cell()))
  {
    ObMutatorCellInfo* cell = NULL;
    if (OB_SUCCESS != (err = mut.get_cell(&cell)))
    {
      TBSYS_LOG(ERROR, "mut.get_cell()=>%d", err);
    }
    else
    {
      dump_ob_mutator_cell(*cell);
    }
  }
  if (OB_ITER_END == err)
  {
    err = OB_SUCCESS;
  }
  return err;
}

int ob_mutator_resolve_name(ObSchemaManagerV2& sch_mgr, ObMutator& mut)
{
  int err = OB_SUCCESS;
  while (OB_SUCCESS == err && OB_SUCCESS == (err = mut.next_cell()))
  {
    ObMutatorCellInfo* cell = NULL;
    if (OB_SUCCESS != (err = mut.get_cell(&cell)))
    {
      TBSYS_LOG(ERROR, "mut.get_cell()=>%d", err);
    }
    else if (OB_SUCCESS != (err = cell_info_resolve_column_name(sch_mgr, cell->cell_info)))
    {
      TBSYS_LOG(ERROR, "resolve_column_name(table_id=%lu, column_id=%lu)=>%d",
                cell->cell_info.table_id_, cell->cell_info.column_id_, err);
    }
    else if (OB_SUCCESS != (err = cell_info_resolve_table_name(sch_mgr, cell->cell_info)))
    {
      TBSYS_LOG(ERROR, "resolve_table_name(table_id=%lu)=>%d", cell->cell_info.table_id_, err);
    }
  }
  if (OB_ITER_END == err)
  {
    err = OB_SUCCESS;
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

struct RPC : public BaseClient
{
  RPC(): is_init_(false) {}
  ~RPC() { if (is_init_)destroy(); }
  int initialize(int64_t n_transport=1) {
    int err = OB_SUCCESS;
    if (OB_SUCCESS == (err = BaseClient::initialize(n_transport)))
    {
      is_init_ = true;
    }
    return err;
  }
  int desc(const char* rs, const char* table)
  {
    int err = OB_SUCCESS;
    char cbuf[MAX_BUF_SIZE];
    ObDataBuffer buf(cbuf, sizeof(cbuf));
    const char* result = NULL;
    ObSchemaManagerV2 schema_mgr;
    if (OB_SUCCESS != (err = send_request(rs, OB_FETCH_SCHEMA, _dummy_, schema_mgr)))
    {
      TBSYS_LOG(ERROR, "send_request()=>%d", err);
    }
    else if (OB_SUCCESS != (err = desc_tables(buf, &schema_mgr, table, result)))
    {
      TBSYS_LOG(ERROR, "desc_table()=>%d", err);
    }
    else
    {
      printf("%s\n", result);
    }
    return err;
  }

  int get_obi_role(const char* rs)
  {
    int err = OB_SUCCESS;
    ObiRole obi_role;
    if (OB_SUCCESS != (err = send_request(rs, OB_GET_OBI_ROLE, _dummy_, obi_role)))
    {
      TBSYS_LOG(ERROR, "send_request()=>%d", err);
    }
    else
    {
      printf("obi_role=%s\n", obi_role.get_role_str());
    }
    return err;
  }

  int get_master_ups(const char* rs)
  {
    int err = OB_SUCCESS;
    ObServer ups;
    if (OB_SUCCESS != (err = send_request(rs, OB_GET_UPDATE_SERVER_INFO, _dummy_, ups)))
    {
      TBSYS_LOG(ERROR, "send_request()=>%d", err);
    }
    else
    {
      printf("%s\n", ups.to_cstring());
    }
    return err;
  }

  int get_client_cfg(const char* rs)
  {
    int err = OB_SUCCESS;
    char cbuf[MAX_BUF_SIZE];
    int64_t pos = 0;
    ObClientConfig clicfg;
    if (OB_SUCCESS != (err = send_request(rs, OB_GET_CLIENT_CONFIG, _dummy_, clicfg)))
    {
      TBSYS_LOG(ERROR, "send_request()=>%d", err);
    }
    else
    {
      clicfg.print(cbuf, sizeof(cbuf), pos);
      cbuf[sizeof(cbuf)-1] = 0;
      printf("%s\n", cbuf);
    }
    return err;
  }

  int get_ms(const char* rs)
  {
    int err = OB_SUCCESS;
    ServerList ms_list;
    int64_t pos = 0;
    char cbuf[MAX_BUF_SIZE];
    char* result = NULL;
    if (OB_SUCCESS != (err = send_request(rs, OB_GET_MS_LIST, _dummy_, ms_list)))
    {
      TBSYS_LOG(ERROR, "send_request(GET_MS_LIST)=>%d", err);
    }
    else if (NULL == (result = ms_list.to_str(cbuf, sizeof(cbuf), pos)))
    {
      TBSYS_LOG(ERROR, "ms_list.to_str()=>%d", err);
    }
    else
    {
      printf("%s\n", result);
    }
    return err;
  }

  int get_replayed_cursor(const char* ups)
  {
    int err = OB_SUCCESS;
    ObLogCursor log_cursor;
    if (OB_SUCCESS != (err = send_request(ups, OB_GET_CLOG_CURSOR, _dummy_, log_cursor)))
    {
      TBSYS_LOG(ERROR, "send_request()=>%d", err);
    }
    else
    {
      printf("%s\n", log_cursor.to_str());
    }
    return err;
  }

  int get_max_log_seq_replayable(const char* ups)
  {
    int err = OB_SUCCESS;
    int64_t log_seq = 0;
    if (OB_SUCCESS != (err = send_request(ups, OB_RS_GET_MAX_LOG_SEQ, _dummy_, log_seq)))
    {
      TBSYS_LOG(ERROR, "send_request()=>%d", err);
    }
    else
    {
      printf("%ld\n", log_seq);
    }
    return err;
  }

  int fill_log_cursor(const char* ups)
  {
    int err = OB_SUCCESS;
    ObLogCursor log_cursor;
    if (OB_SUCCESS != (err = send_request(ups, OB_FILL_LOG_CURSOR, log_cursor, log_cursor)))
    {
      TBSYS_LOG(ERROR, "send_request()=>%d", err);
    }
    else
    {
      printf("%s\n", log_cursor.to_str());
    }
    return err;
  }

  int get_last_frozen_version(const char* ups)
  {
    int err = OB_SUCCESS;
    int64_t frozen_version = 0;
    if (OB_SUCCESS != (err = send_request(ups, OB_UPS_GET_LAST_FROZEN_VERSION, _dummy_, frozen_version)))
    {
      TBSYS_LOG(ERROR, "send_request()=>%d", err);
    }
    else
    {
      printf("%ld\n", frozen_version);
    }
    return err;
  }

  int choose_server(const char* rs, const char* _server, ObServer& server)
  {
    int err = OB_SUCCESS;
    ServerList ms_list;
    if (0 != strcmp("ms", _server) && 0 != strcmp("ups", _server))
    {
      if (OB_SUCCESS != (err = to_server(server, _server)))
      {
        TBSYS_LOG(ERROR, "to_server(%s)=>%d", _server, err);
      }
    }
    else if (0 == strcmp("ms", _server))
    {
      if (OB_SUCCESS != (err = send_request(rs, OB_GET_MS_LIST, _dummy_, ms_list)))
      {
        TBSYS_LOG(ERROR, "send_request(GET_MS_LIST)=>%d", err);
      }
      else if (0 >= ms_list.n_servers_)
      {
        TBSYS_LOG(ERROR, "ms_list.n_servers[%d] <= 0", ms_list.n_servers_);
      }
      else
      {
        server = ms_list.servers_[0];
      }
    }
    else if (0 == strcmp("ups", _server))
    {
      if (OB_SUCCESS != (err = send_request(rs, OB_GET_UPDATE_SERVER_INFO, _dummy_, server)))
      {
        TBSYS_LOG(ERROR, "send_request()=>%d", err);
      }
    }
    return err;
  }

  int scan(const char* rs, const char* table, const char* columns, const char* rowkey, int64_t limit, const char* _server)
  {
    int err = OB_SUCCESS;
    char cbuf[MAX_BUF_SIZE];
    ObDataBuffer buf(cbuf, sizeof(cbuf));
    ObServer ups;
    int64_t frozen_version = 0;
    ObScanParam scan_param;
    ObServer server;
    ObScanner scanner;
    char* result = NULL;
    if (OB_SUCCESS != (err = send_request(rs, OB_GET_UPDATE_SERVER_INFO, _dummy_, ups)))
    {
      TBSYS_LOG(ERROR, "send_request(get_ups)=>%d", err);
    }
    else if (OB_SUCCESS != (err = send_request(ups, OB_UPS_GET_LAST_FROZEN_VERSION, _dummy_, frozen_version)))
    {
      TBSYS_LOG(ERROR, "send_request(get_last_frozen_version)=>%d", err);
    }
    else if (OB_SUCCESS != (err = choose_server(rs, _server, server)))
    {
      TBSYS_LOG(ERROR, "choose_server(rs=%s, server=%s)=>%d", rs, _server, err);
    }
    else if (OB_SUCCESS != (err = scan_func2(buf, scan_param, frozen_version + 1, table, columns, rowkey, limit)))
    {
      TBSYS_LOG(ERROR, "scan_func2()=>%d", err);
    }
    else if (OB_SUCCESS != (err = send_request(server, OB_SCAN_REQUEST, scan_param, scanner)))
    {
      TBSYS_LOG(ERROR, "send_request(scan)=>%d", err);
      scan_param.dump();
    }
    else if (NULL == (result = buf.get_data() + buf.get_position()))
    {}
    else if (OB_SUCCESS != (err = repr(buf, scanner)))
    {
      TBSYS_LOG(ERROR, "repr(scanner)=>%d", err);
    }
    else
    {
      printf("%s\n", result);
    }
    return err;
  }

  int set(const char* rs, const char* table, const char* column, const char* rowkey, const char* value, const char* _server)
  {
    int err = OB_SUCCESS;
    char cbuf[MAX_BUF_SIZE];
    ObDataBuffer buf(cbuf, sizeof(cbuf));
    ObSchemaManagerV2 schema_mgr;
    ObServer server;
    ObMutator mutator;
    if (OB_SUCCESS != (err = send_request(rs, OB_FETCH_SCHEMA, _dummy_, schema_mgr)))
    {
      TBSYS_LOG(ERROR, "send_request()=>%d", err);
    }
    else if (OB_SUCCESS != (err = choose_server(rs, _server, server)))
    {
      TBSYS_LOG(ERROR, "choose_server(rs=%s, server=%s)=>%d", rs, _server, err);
    }
    else if (OB_SUCCESS != (err = mutate_func(buf, mutator, schema_mgr, table, rowkey, column, value)))
    {
      TBSYS_LOG(ERROR, "mutate_func()=>%d", err);
    }
    else if (OB_SUCCESS != (err = send_request(server, OB_WRITE, mutator, _dummy_)))
    {
      TBSYS_LOG(ERROR, "send_request(server=%s, write)=>%d", server.to_cstring(), err);
    }
    else
    {
      printf("%s mutate OK!\n", server.to_cstring());
    }
    return err;
  }

  int randset(const char* rs, const char* table, const char* _server, const int64_t n_row)
  {
    int err = OB_SUCCESS;
    int send_err = OB_SUCCESS;
    char cbuf[MAX_BUF_SIZE];
    ObDataBuffer buf(cbuf, sizeof(cbuf));
    ObSchemaManagerV2 schema_mgr;
    ObServer server;
    ObMutator mutator;
    const bool keep_going_on_err = false;
    if (OB_SUCCESS != (err = send_request(rs, OB_FETCH_SCHEMA, _dummy_, schema_mgr)))
    {
      TBSYS_LOG(ERROR, "send_request()=>%d", err);
    }
    else if (OB_SUCCESS != (err = choose_server(rs, _server, server)))
    {
      TBSYS_LOG(ERROR, "choose_server(rs=%s, server=%s)=>%d", rs, _server, err);
    }
    for(int64_t i = 0; OB_SUCCESS == err && (n_row < 0 || i < n_row); i++)
    {
      buf.get_position() = 0;
      if (OB_SUCCESS != (err = build_rand_mutator(buf, mutator, i, schema_mgr, table)))
      {
        TBSYS_LOG(ERROR, "mutate_func()=>%d", err);
      }
      else if (OB_SUCCESS != (send_err = send_request(server, OB_WRITE, mutator, _dummy_)))
      {
        TBSYS_LOG(ERROR, "send_write(server=%s, iter=%ld)=>%d", server.to_cstring(), i, send_err);
        if (!keep_going_on_err)
        {
          err = send_err;
        }
      }
      else
      {
        TBSYS_LOG(INFO, "%s mutate[iter=%ld] OK!", server.to_cstring(), i);
      }
    }
    return err;
  }

  int ping_req(ObServer& server, int64_t idx, int64_t worker_id)
  {
    int err = OB_SUCCESS;
    if (OB_SUCCESS != (err = send_request(server, OB_PING_REQUEST, _dummy_, _dummy_, worker_id)))
    {
      TBSYS_LOG(ERROR, "send_ping(server=%s, iter=[%ld])=>%d", server.to_cstring(), idx, err);
    }
    else
    {
      TBSYS_LOG(DEBUG, "%s ping[%ld] OK!", server.to_cstring(), idx);
    }
    return err;
  }

  int write_req(ObServer& server, ObSchemaManagerV2& schema_mgr, int64_t start, int64_t end, int64_t worker_id)
  {
    int err = OB_SUCCESS;
    int send_err = OB_SUCCESS;
    const char* table = "any";
    char cbuf[MAX_BUF_SIZE];
    ObDataBuffer buf(cbuf, sizeof(cbuf));
    ObMutator mutator;
    if (OB_SUCCESS != (err = build_rand_batch_mutator(buf, mutator, start, end, schema_mgr, table)))
    {
      TBSYS_LOG(ERROR, "mutate_func()=>%d", err);
    }
    else if (OB_SUCCESS != (send_err = send_request(server, OB_WRITE, mutator, _dummy_, worker_id)))
    {
      err = send_err;
      TBSYS_LOG(ERROR, "send_write(server=%s, iter=[%ld,%ld])=>%d", server.to_cstring(), start, end, send_err);
    }
    else
    {
      TBSYS_LOG(DEBUG, "%s mutate[%ld, %ld] OK!", server.to_cstring(), start, end);
    }
    return err;
  }

  int mget_req(ObServer& server, ObSchemaManagerV2& schema_mgr, int64_t start, int64_t end, int64_t worker_id, int64_t start_version)
  {
    int err = OB_SUCCESS;
    const char* table = "any";
    char cbuf[MAX_BUF_SIZE];
    ObDataBuffer buf(cbuf, sizeof(cbuf));
    ObGetParam get_param;
    ObScanner scanner;
    if (OB_SUCCESS != (err = build_rand_mget_param(buf, get_param, start, end, schema_mgr, table, start_version)))
    {
      TBSYS_LOG(ERROR, "mutate_func()=>%d", err);
    }
    else if (OB_SUCCESS != (err = send_request(server, OB_GET_REQUEST, get_param, scanner, worker_id)))
    {
      TBSYS_LOG(ERROR, "send_request(server=%s, iter=[%ld, %ld])=>%d", server.to_cstring(), start, end, err);
    }
    else
    {
      TBSYS_LOG(DEBUG, "%s mget[%ld, %ld] OK!", server.to_cstring(), start, end);
    }
    return err;
  }

  int scan_req(ObServer& server, ObSchemaManagerV2& schema_mgr, int64_t start, int64_t end, int64_t worker_id, int64_t start_version)
  {
    int err = OB_SUCCESS;
    const char* table = "any";
    char cbuf[MAX_BUF_SIZE];
    ObDataBuffer buf(cbuf, sizeof(cbuf));
    ObScanParam scan_param;
    ObScanner scanner;
    if (OB_SUCCESS != (err = build_rand_scan_param(buf, scan_param, start, end, schema_mgr, table, start_version)))
    {
      TBSYS_LOG(ERROR, "mutate_func()=>%d", err);
    }
    else if (OB_SUCCESS != (err = send_request(server, OB_SCAN_REQUEST, scan_param, scanner, worker_id)))
    {
      TBSYS_LOG(ERROR, "send_request(server=%s, iter=[%ld, %ld])=>%d", server.to_cstring(), start, end, err);
    }
    else
    {
      TBSYS_LOG(DEBUG, "%s mget[%ld, %ld] OK!", server.to_cstring(), start, end);
    }
    return err;
  }

  int stress(const char* rs, const char* server,
             const int64_t duration, const int64_t start, const int64_t end,
             const int64_t write_thread, const int64_t scan_thread, const int64_t mget_thread,
             const int64_t write_size, const int64_t scan_size, const int64_t mget_size)
  {
    int err = OB_SUCCESS;
    ClientWorker worker;
    ClientWorker::Monitor monitor(&worker);
    TBSYS_LOG(INFO, "stress(rs=%s, server=%s, duration=%ld, range=[%ld,%ld), thread=%ld:%ld:%ld, req_size=%ld:%ld:%ld)",
              rs, server, duration, start, end, write_thread, scan_thread, mget_thread, write_size, scan_size, mget_size);
    if (OB_SUCCESS != (err = worker.init(this, rs, server, start, end,
                                         write_thread, scan_thread, mget_thread, write_size, scan_size, mget_size)))
    {
      TBSYS_LOG(ERROR, "worker.init()=>%d", err);
    }
    else if (OB_SUCCESS != (err = worker.start()))
    {
      TBSYS_LOG(ERROR, "worker.start()=>%d", err);
    }
    else if (OB_SUCCESS != (err = monitor.monitor(duration)))
    {
      TBSYS_LOG(ERROR, "monitor.monitor(%ld)=>%d", duration, err);
    }
    return err;
  }

  int send_mutator(const char* rs, const char* log_file, const char* _ups)
  {
    int err = OB_SUCCESS;
    int send_err = OB_SUCCESS;
    int64_t pos = 0;
    ObSchemaManagerV2 schema_mgr;
    ObServer ups;
    ObLogEntry entry;
    ObUpsMutator mutator;
    ObMutator final_mutator;
    const char* buf = NULL;
    int64_t len = 0;
    const bool use_name = true;
    const bool show_mutator = false;
    const bool keep_going_on_err = true;
    TBSYS_LOG(DEBUG, "send_mutator(ups=%s, src=%s)", _ups, log_file);
    if (NULL == _ups || NULL == log_file)
    {
      err = OB_INVALID_ARGUMENT;
    }
    else if (OB_SUCCESS != (err = send_request(rs, OB_FETCH_SCHEMA, _dummy_, schema_mgr)))
    {
      TBSYS_LOG(ERROR, "send_request(OB_FETCH_SCHEMA)=>%d", err);
    }
    else if (OB_SUCCESS != (err = choose_server(rs, _ups, ups)))
    {
      TBSYS_LOG(ERROR, "choose_server(rs=%s, _ups=%s)=>%d", rs, _ups, err);
    }
    else if (OB_SUCCESS != (err = get_file_len(log_file, len)))
    {
      TBSYS_LOG(ERROR, "get_file_len(%s)=>%d", log_file, err);
    }
    else if (OB_SUCCESS != (err = file_map_read(log_file, len, buf)))
    {
      TBSYS_LOG(ERROR, "file_map_read(%s)=>%d", log_file, err);
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
        if (OB_LOG_UPS_MUTATOR != entry.cmd_)
        {
          TBSYS_LOG(DEBUG, "ignore non mutator[seq=%ld, cmd=%d]", entry.seq_, entry.cmd_);
        }
        else if (OB_SUCCESS != (err = mutator.deserialize(buf + pos, entry.get_log_data_len(), tmp_pos)))
        {
          TBSYS_LOG(ERROR, "mutator.deserialize(seq=%ld)=>%d", (int64_t)entry.seq_, err);
        }
        else if (!mutator.is_normal_mutator())
        {
          TBSYS_LOG(DEBUG, "ignore special mutator[seq=%ld, cmd=%d]", entry.seq_, entry.cmd_);
        }
        else if (use_name && OB_SUCCESS != (err = ob_mutator_resolve_name(schema_mgr, mutator.get_mutator())))
        {
          TBSYS_LOG(ERROR, "mutator_resolve_name()=>%d", err);
        }
        else if (show_mutator && OB_SUCCESS != (err = dump_ob_mutator(mutator.get_mutator())))
        {
          TBSYS_LOG(ERROR, "dump_mutator()=>%d", err);
        }
        else if (OB_SUCCESS != (err = final_mutator.reset()))
        {
          TBSYS_LOG(ERROR, "final_mutator.reset()=>%d", err);
        }
        else if (OB_SUCCESS != (err = mutator_add(final_mutator, mutator.get_mutator(), OB_MAX_PACKET_LENGTH)))
        {
          TBSYS_LOG(ERROR, "mutator_add()=>%d", err);
        }
        else if (OB_SUCCESS != (send_err = send_request(ups, OB_WRITE, final_mutator, _dummy_)))
        {
          TBSYS_LOG(ERROR, "FAIL TO SEND MUTATOR: server=%s, seq=%ld, err=%d", ups.to_cstring(), entry.seq_, send_err);
          if (!keep_going_on_err)
          {
            err = send_err;
          }
        }
        else
        {
          TBSYS_LOG(DEBUG, "SUCCESS TO SEND MUTATOR: server=%s, seq=%ld", ups.to_cstring(), entry.seq_);
        }
        if (OB_SUCCESS == err)
        {
          pos += entry.get_log_data_len();
        }
      }
    }
    if (OB_SUCCESS == err && pos != len)
    {
      err = OB_ERR_UNEXPECTED;
      TBSYS_LOG(ERROR, "pos[%ld] != len[%ld]", pos, len);
    }
    return err;
  }

  int get_clog_status(const char* ups)
  {
    int err = OB_SUCCESS;
    char buf[MAX_BUF_SIZE];
    ObUpsCLogStatus stat;
    if (NULL == ups)
    {
      err = OB_INVALID_ARGUMENT;
    }
    else if (OB_SUCCESS != (err = send_request(ups, OB_GET_CLOG_STATUS, _dummy_, stat)))
    {
      TBSYS_LOG(ERROR, "send_request(ups=%s, OB_GET_CLOG_STATUS)=>%d", ups, err);
    }
    else if (OB_SUCCESS != (err = stat.to_str(buf, sizeof(buf))))
    {
      TBSYS_LOG(ERROR, "stat.to_str()=>%d", err);
    }
    else
    {
      printf("%s\n", buf);
    }
    return err;
  }
  int get_log_sync_delay_stat(const char* ups)
  {
    int err = OB_SUCCESS;
    ObLogSyncDelayStat delay_stat;
    if (NULL == ups)
    {
      err = OB_INVALID_ARGUMENT;
    }
    else if (OB_SUCCESS != (err = send_request(ups, OB_GET_LOG_SYNC_DELAY_STAT, _dummy_, delay_stat)))
    {
      TBSYS_LOG(ERROR, "send_request(ups=%s, OB_GET_LOG_SYNC_DELAY_STAT)=>%d", ups, err);
    }
    else
    {
      time_t tm = delay_stat.get_last_replay_time_us()/1000000;
      char* str_time = ctime(&tm);
      fprintf(stdout, "log_sync_delay: last_log_id=%ld, total_count=%ld, total_delay=%ldus, max_delay=%ldus, last_replay_time=%ldus [%s]\n",
              delay_stat.get_last_log_id(), delay_stat.get_mutator_count(), delay_stat.get_total_delay_us(),
              delay_stat.get_max_delay_us(), delay_stat.get_last_replay_time_us(),
              str_time);
    }
    return err;
  }
  bool is_init_;
};

int ClientWorker::fetch_schema() {
  int err = OB_SUCCESS;
  if (NULL == schema_mgr_ && NULL == (schema_mgr_ = new(std::nothrow)ObSchemaManagerV2()))
  {
    err = OB_ALLOCATE_MEMORY_FAILED;
  }
  else if (OB_SUCCESS != (err = rpc_->send_request(rs_, OB_FETCH_SCHEMA, _dummy_, *schema_mgr_)))
  {
    TBSYS_LOG(ERROR, "send_request(OB_FETCH_SCHEMA)=>%d", err);
  }
  return err;
}

int ClientWorker::is_ups(bool& ret, ObServer& server) {
  int err = OB_SUCCESS;
  ObUpsList ups_list;
  ret = false;
  if (OB_SUCCESS != (err = rpc_->send_request(rs_, OB_GET_UPS, _dummy_, ups_list)))
  {
    TBSYS_LOG(ERROR, "send_request(OB_GET_UPS)=>%d", err);
  }
  for(int i = 0; OB_SUCCESS == err && i < ups_list.ups_count_; i++)
  {
    if (ups_list.ups_array_[i].addr_ == server)
    {
      ret = true;
    }
  }
  return err;
}

int ClientWorker::write(int64_t idx) {
  int err = OB_SUCCESS;
  int64_t write_seq = 0;
  ObServer server;
  int64_t interval = 100000;
  const char* write_type = getenv("write_type")?: "write";
  bool is_ping = (0 == strcmp(write_type, "ping"));
  if (OB_SUCCESS != (err = rpc_->choose_server(rs_, server_, server)))
  {
    TBSYS_LOG(ERROR, "send_request(get_update_server)=>%d", err);
  }
  for(; !is_ping && !stop_ && OB_SUCCESS == err && (end_ < 0 || write_seq_ < end_);){
    write_seq = __sync_fetch_and_add(&write_seq_, write_req_size_);
    if (OB_SUCCESS != (err = rpc_->write_req(server, *schema_mgr_, write_seq, write_seq + write_req_size_, idx)))
    {
      __sync_fetch_and_add(&write_fail_count_, write_req_size_);
      TBSYS_LOG(ERROR, "write_req([%ld-%ld], id=%ld)=>%d", write_seq, write_seq + write_req_size_, idx, err);
    }
    if(keep_going_on_err_ && OB_SUCCESS != err)
    {
      err = OB_SUCCESS;
      usleep(static_cast<useconds_t>(interval));
    }
  }
  for(; is_ping && !stop_ && OB_SUCCESS == err && (end_ < 0 || write_seq_ < end_);) {
    write_seq = __sync_fetch_and_add(&write_seq_, 1);
    if (OB_SUCCESS != (err = rpc_->ping_req(server, write_seq, idx)))
    {
      __sync_fetch_and_add(&write_fail_count_, 1);
      TBSYS_LOG(ERROR, "ping_req([%ld], id=%ld)=>%d", write_seq, idx, err);
    }
    if(keep_going_on_err_ && OB_SUCCESS != err)
    {
      err = OB_SUCCESS;
      usleep(static_cast<useconds_t>(interval));
    }
  }
  return err;
}

int ClientWorker::mget(int64_t idx) {
  int err = OB_SUCCESS;
  int64_t mget_seq = 0;
  ObServer server;
  ObServer ups;
  bool server_is_ups = false;
  int64_t start_version = 0;
  int64_t interval = 100000;
  if (OB_SUCCESS != (err = rpc_->choose_server(rs_, server_, server)))
  {
    TBSYS_LOG(ERROR, "send_request(get_update_server)=>%d", err);
  }
  else if (OB_SUCCESS != (err = is_ups(server_is_ups, server)))
  {
    TBSYS_LOG(ERROR, "is_ups()=>%d", err);
  }
  else if (!server_is_ups)
  {
    TBSYS_LOG(INFO, "server[%s] is not ups, don't need to set start version", server.to_cstring());
  }
  else if (OB_SUCCESS != (err = rpc_->send_request(rs_, OB_GET_UPDATE_SERVER_INFO, _dummy_, ups)))
  {
    TBSYS_LOG(ERROR, "send_request(get_ups)=>%d", err);
  }
  else if (OB_SUCCESS != (err = rpc_->send_request(ups, OB_UPS_GET_LAST_FROZEN_VERSION, _dummy_, start_version)))
  {
    TBSYS_LOG(ERROR, "send_request(get_last_frozen_version)=>%d", err);
  }
  else
  {
    start_version++;
  }
  for(; !stop_ && OB_SUCCESS == err;){
    __sync_fetch_and_add(&mget_seq_, mget_req_size_);
    mget_seq = write_seq_ - mget_req_size_;
    if (mget_seq > 0)
    {
      mget_seq = random() % mget_seq + 1;
    }
    if (mget_seq <= 0)
    {
      TBSYS_LOG(INFO, "mget_seq[%ld] <= 0, need wait", mget_seq);
      usleep(100000);
    }
    else if (OB_SUCCESS != (err = rpc_->mget_req(server, *schema_mgr_, mget_seq, mget_seq + mget_req_size_, idx, start_version)))
    {
      __sync_fetch_and_add(&mget_fail_count_, mget_req_size_);
      TBSYS_LOG(ERROR, "mget_req([%ld-%ld], id=%ld)=>%d", mget_seq, mget_seq + mget_req_size_, idx, err);
    }
    if(keep_going_on_err_ && OB_SUCCESS != err)
    {
      err = OB_SUCCESS;
      usleep(static_cast<useconds_t>(interval));
    }
  }
  return err;
}

int ClientWorker::scan(int64_t idx) {
  int err = OB_SUCCESS;
  int64_t scan_seq = 0;
  ObServer server;
  ObServer ups;
  bool server_is_ups = false;
  int64_t start_version = 0;
  int64_t interval = 100000;
  if (OB_SUCCESS != (err = rpc_->choose_server(rs_, server_, server)))
  {
    TBSYS_LOG(ERROR, "send_request(get_update_server)=>%d", err);
  }
  else if (OB_SUCCESS != (err = is_ups(server_is_ups, server)))
  {
    TBSYS_LOG(ERROR, "is_ups()=>%d", err);
  }
  else if (!server_is_ups)
  {
    TBSYS_LOG(INFO, "server[%s] is not ups, don't need to set start version", server.to_cstring());
  }
  else if (OB_SUCCESS != (err = rpc_->send_request(rs_, OB_GET_UPDATE_SERVER_INFO, _dummy_, ups)))
  {
    TBSYS_LOG(ERROR, "send_request(get_ups)=>%d", err);
  }
  else if (OB_SUCCESS != (err = rpc_->send_request(ups, OB_UPS_GET_LAST_FROZEN_VERSION, _dummy_, start_version)))
  {
    TBSYS_LOG(ERROR, "send_request(get_last_frozen_version)=>%d", err);
  }
  else
  {
    start_version++;
  }
  for(; !stop_ && OB_SUCCESS == err;){
    __sync_fetch_and_add(&scan_seq_, scan_req_size_);
    scan_seq = write_seq_ - scan_req_size_;
    if (scan_seq > 0)
    {
      scan_seq = random() % scan_seq + 1;
    }
    if (scan_seq < 0)
    {
      TBSYS_LOG(INFO, "scan_seq[%ld] <= 0, need wait", scan_seq);
      usleep(100000);
    }
    else if (OB_SUCCESS != (err = rpc_->scan_req(server, *schema_mgr_, scan_seq, scan_seq + scan_req_size_, idx, start_version)))
    {
      __sync_fetch_and_add(&scan_fail_count_, scan_req_size_);
      TBSYS_LOG(ERROR, "scan_req([%ld-%ld], id=%ld)=>%d", scan_seq, scan_seq + scan_req_size_, idx, err);
    }
    if(keep_going_on_err_ && OB_SUCCESS != err)
    {
      err = OB_SUCCESS;
      usleep(static_cast<useconds_t>(interval));
    }
  }
  return err;
}

#define report_error(err, ...) if (OB_SUCCESS != err)TBSYS_LOG(ERROR, __VA_ARGS__);
#include "cmd_args_parser.h"
int main(int argc, char *argv[])
{
  int err = 0;
  RPC rpc;
  TBSYS_LOGGER.setLogLevel(getenv("log_level")?:"INFO");
  if (getenv("log_file"))
    TBSYS_LOGGER.setFileName(getenv("log_file"));
  if (OB_SUCCESS != (err = ob_init_memory_pool()))
  {
    TBSYS_LOG(ERROR, "ob_init_memory_pool()=>%d", err);
  }
  else if (OB_SUCCESS != (err = rpc.initialize(atoll(getenv("n_transport")?:"1"))))
  {
    TBSYS_LOG(ERROR, "rpc.initialize()=>%d", err);
  }
  else if (OB_NEED_RETRY != (err = CmdCall(argc, argv, rpc.desc, StrArg(rs), StrArg(table, "*")):OB_NEED_RETRY))
  {
    report_error(err, "desc()=>%d", err);
  }
  else if (OB_NEED_RETRY != (err = CmdCall(argc, argv, rpc.get_obi_role, StrArg(rs)):OB_NEED_RETRY))
  {
    report_error(err, "get_obi_role()=>%d", err);
  }
  else if (OB_NEED_RETRY != (err = CmdCall(argc, argv, rpc.get_master_ups, StrArg(rs)):OB_NEED_RETRY))
  {
    report_error(err, "get_master_ups()=>%d", err);
  }
  else if (OB_NEED_RETRY != (err = CmdCall(argc, argv, rpc.get_client_cfg, StrArg(rs)):OB_NEED_RETRY))
  {
    report_error(err, "get_client_cfg()=>%d", err);
  }
  else if (OB_NEED_RETRY != (err = CmdCall(argc, argv, rpc.get_replayed_cursor, StrArg(ups)):OB_NEED_RETRY))
  {
    report_error(err, "get_replayed_cursor()=>%d", err);
  }
  else if (OB_NEED_RETRY != (err = CmdCall(argc, argv, rpc.get_max_log_seq_replayable, StrArg(ups)):OB_NEED_RETRY))
  {
    report_error(err, "get_max_log_seq()=>%d", err);
  }
  else if (OB_NEED_RETRY != (err = CmdCall(argc, argv, rpc.fill_log_cursor, StrArg(ups)):OB_NEED_RETRY))
  {
    report_error(err, "fill_log_cursor()=>%d", err);
  }
  else if (OB_NEED_RETRY != (err = CmdCall(argc, argv, rpc.get_last_frozen_version, StrArg(ups)):OB_NEED_RETRY))
  {
    report_error(err, "get_last_frozen_version()=>%d", err);
  }
  else if (OB_NEED_RETRY != (err = CmdCall(argc, argv, rpc.get_ms, StrArg(rs)):OB_NEED_RETRY))
  {
    report_error(err, "get_ms()=>%d", err);
  }
  else if (OB_NEED_RETRY != (err = CmdCall(argc, argv, rpc.scan, StrArg(rs), StrArg(table),
                                           StrArg(columns, "*"), StrArg(rowkey, "[min,max]"), IntArg(limit, "10"),  StrArg(server, "ms")):OB_NEED_RETRY))
  {
    report_error(err, "scan()=>%d", err);
  }
  else if (OB_NEED_RETRY != (err = CmdCall(argc, argv, rpc.set, StrArg(rs), StrArg(table),
                                           StrArg(column), StrArg(rowkey), StrArg(value), StrArg(server, "ups")):OB_NEED_RETRY))
  {
    report_error(err, "set()=>%d", err);
  }
  else if (OB_NEED_RETRY != (err = CmdCall(argc, argv, rpc.randset, StrArg(rs), StrArg(table, "any"),
                                           StrArg(server, "ups"), IntArg(n_row, "-1")):OB_NEED_RETRY))
  {
    report_error(err, "randset()=>%d", err);
  }
  else if (OB_NEED_RETRY != (err = CmdCall(argc, argv, rpc.stress, StrArg(rs), StrArg(server, "ups"),
                                           IntArg(duration, "-1"), IntArg(start, "1"), IntArg(end, "-1"),
                                           IntArg(write, "1"), IntArg(scan, "1"), IntArg(mget, "1"),
                                           IntArg(write_size, "1"), IntArg(scan_size, "1"), IntArg(mget_size, "1")):OB_NEED_RETRY))
  {
    report_error(err, "stress()=>%d", err);
  }
  else if (OB_NEED_RETRY != (err = CmdCall(argc, argv, rpc.send_mutator, StrArg(rs), StrArg(log_file), StrArg(server, "ups")):OB_NEED_RETRY))
  {
    report_error(err, "send_mutator()=>%d", err);
  }
  else if (OB_NEED_RETRY != (err = CmdCall(argc, argv, rpc.get_log_sync_delay_stat, StrArg(ups)):OB_NEED_RETRY))
  {
    report_error(err, "get_log_sync_delay_stat()=>%d", err);
  }
  else if (OB_NEED_RETRY != (err = CmdCall(argc, argv, rpc.get_clog_status, StrArg(ups)):OB_NEED_RETRY))
  {
    report_error(err, "get_clog_status()=>%d", err);
  }
  else
  {
    fprintf(stderr, _usages, argv[0]);
    //__cmd_args_parser.dump(argc, argv);
  }
  exit(err);
  return err;
}
