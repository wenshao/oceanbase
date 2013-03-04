/*
 * =====================================================================================
 *
 *       Filename:  DbDumpConfig.cpp
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  04/14/2011 07:50:34 PM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  yushun.swh (DBA Group), yushun.swh@taobao.com
 *        Company:  taobao
 *
 * =====================================================================================
 */
#include "db_dumper_config.h"
#include "db_record_filter.h"
#include "common/utility.h"
#include "common/ob_string.h"
#include <map>

using namespace oceanbase::common;

const char *kConfigObLog="ob_log_dir";
const char *kConfigColumn= "column_info";
const char *kConfigSys = "dumper_config";

const char *kOutputDir = "output_dir";

const char *kSysParserNr = "parser_thread_nr";
const char *kSysWorkerThreadNr = "worker_thread_nr";

const char *kSysLogDir="log_dir";
const char *kSysLogLevel="log_level";

const char *kSysHost = "host";
const char *kSysPort = "port";
const char *kSysNetworkTimeout = "network_timeout";
const char *kSysAppName = "app_name";


const char *kSysMaxFileSize = "max_file_size";
const char *kSysRotateFileTime = "rotate_file_interval";
const char *kConfigTableId = "table_id";
const char *kConfigReviseColumn = "revise_column";
const char *kSysConsistency = "consistency";
const char *kSysMutiGetNr = "muti_get_nr";

//columns should be dumped in rowkey
//rowkey_column=name,start_pos,end_pos,type,endian
const char *kConfigRowkeyColumn = "rowkey_column";
//columns filter, [column name], [ column type] ,[min], [max]
const char *kConfigColumnFilter = "column_filter";

const char *kSysMonitorInterval = "monitor_interval";
const char *kSysTmpLogDir = "tmp_log_dir";
const char *kSysInitLogId = "init_log_id";
const char *kOutputFormat = "output_format";

const char *kSysHeaderDelima = "header_delima";
const char *kSysBodyDelima = "body_delima";
const char *kSysPidFile = "obdump.pid";
const char *kSysMaxNologInvertal = "max_nolog_interval";
const char *kSysInitDate = "init_date";


bool DbTableConfig::is_revise_column(std::string &column)
{
  bool ret = false;

  for(size_t i = 0;i < revise_columns_.size(); i++) {
    if (column.compare(revise_columns_[i]) == 0) {
      ret = true;
      break;
    }
  }

  return ret;
}

void DbTableConfig::parse_rowkey_item(std::vector<const char*> &vec)
{
  for(size_t i = 0;i < vec.size(); i++) {
    RowkeyItem item;

    //parse name
    const char *str = vec[i];
    const char *p = strchr(str, ',');
    if (p == NULL) {
      TBSYS_LOG(ERROR, "can't parse name");
      continue;
    }
    item.name = std::string(str, p - str);

    //parse start_pos
    str = p + 1;
    p = strchr(str, ',');
    if (p == NULL) {
      TBSYS_LOG(ERROR, "can't parse start_pos");
      continue;
    }
    item.start_pos = atol((std::string(str, p - str)).c_str());

    //parse end_pos
    str = p + 1;
    p = strchr(str, ',');
    if (p == NULL) {
      TBSYS_LOG(ERROR, "can't parse start_pos");
      continue;
    }
    item.end_pos = atol((std::string(str, p - str)).c_str());

    //parse type
    str = p + 1;
    p = strchr(str, ',');
    if (p == NULL) {
      TBSYS_LOG(ERROR, "can't parse start_pos");
      continue;
    }
    item.type = (ObObjType)atol((std::string(str, p - str)).c_str());

    rowkey_columns_.push_back(item);
  }
}

void DbTableConfig::parse_output_columns(std::string out_columns)
{
  std::string::size_type delima_pos;
  std::string::size_type base_pos = 0;
  std::string column;

  while ((delima_pos = out_columns.find(",", base_pos)) != std::string::npos) {
    column = out_columns.substr(base_pos, delima_pos - base_pos);
    output_columns_.push_back(column);
    TBSYS_LOG(DEBUG, "COLUMN: %s", column.c_str());
    base_pos = delima_pos + 1;
  }
  
  column = out_columns.substr(base_pos);
  output_columns_.push_back(column);
  TBSYS_LOG(DEBUG, "COLUMN: %s", column.c_str());
}

DbDumpConfigMgr* DbDumpConfigMgr::instance_ = NULL;

DbDumpConfigMgr* DbDumpConfigMgr::get_instance()
{
  if (instance_ == NULL)
    instance_ = new(std::nothrow) DbDumpConfigMgr();

  if (instance_ == NULL) {
    TBSYS_LOG(ERROR, "error when get instance");
  }

  return instance_;
}

int DbDumpConfigMgr::load(const char *path)
{
  int ret = TBSYS_CONFIG.load(path);
  if (ret != OB_SUCCESS) {
    TBSYS_LOG(ERROR, "Can't Load TBSYS config");
    return ret;
  }

  std::vector<std::string> sections;
  TBSYS_CONFIG.getSectionName(sections);

  std::vector<std::string>::iterator itr = sections.begin();
  while(itr != sections.end()) {
    if (itr->compare(kConfigSys)) {
      //table config
      std::vector<const char *> columns;
      columns = TBSYS_CONFIG.getStringList(itr->c_str(), kConfigColumn);
      DbTableConfig config(*itr, columns);

      std::vector<const char *> rowkey_columns;
      rowkey_columns = TBSYS_CONFIG.getStringList(itr->c_str(), kConfigRowkeyColumn);
      config.parse_rowkey_item(rowkey_columns);

      std::vector<const char*> revise_column = TBSYS_CONFIG.getStringList(itr->c_str(), kConfigReviseColumn);
      config.set_revise_columns(revise_column);

      TBSYS_LOG(INFO, "revise column size=%zu", config.get_revise_columns().size());

      const char *output_format_str = TBSYS_CONFIG.getString(itr->c_str(), std::string(kOutputFormat));
      if (output_format_str != NULL) {
        std::string str = output_format_str;
        config.parse_output_columns(str);
      }

      const char *filter_str = TBSYS_CONFIG.getString(itr->c_str(), std::string(kConfigColumnFilter));
      if (filter_str) {
        std::string str = filter_str;
        DbRowFilter *filter = create_filter_from_str(str.substr(str.find('=')));
        config.filter_ = filter;
        filter_set_.push_back(filter);
      }

      const char *table_id_str = TBSYS_CONFIG.getString(itr->c_str(), std::string(kConfigTableId));
      if (table_id_str) {
        config.table_id_ = atol(table_id_str);

        //add table 
        configs_.push_back(config);
      } else {
        TBSYS_LOG(ERROR, "table id must specified, table will be skiped");
      }
    } else {                                    /* system parameters */
      const char * parser_thread_nr = TBSYS_CONFIG.getString(kConfigSys, std::string(kSysParserNr));
      if (parser_thread_nr)
        parser_thread_nr_ = atoi(parser_thread_nr);

      const char *dir = TBSYS_CONFIG.getString(kConfigSys, std::string(kOutputDir));
      if (dir) {
        output_dir_ = dir;
      }

      const char *worker_thread_nr = TBSYS_CONFIG.getString(kConfigSys, std::string(kSysWorkerThreadNr));
      if (worker_thread_nr) {
        worker_thread_nr_ = atol(worker_thread_nr);
      }

      const char *log_dir = TBSYS_CONFIG.getString(kConfigSys, std::string(kSysLogDir));
      if (log_dir)
        log_dir_ = log_dir;
      else
        log_dir = "db_dump.log";

      const char *log_level = TBSYS_CONFIG.getString(kConfigSys, std::string(kSysLogLevel));
      if (log_level)
        log_level_ = log_level;
      else
        log_level_ = "debug";

      const char *ob_log_dir = TBSYS_CONFIG.getString(kConfigSys, std::string(kConfigObLog));
      if (log_dir)
        ob_log_dir_ = ob_log_dir;

      const char *host = TBSYS_CONFIG.getString(kConfigSys, std::string(kSysHost));
      if (host) {
        host_ = host;
      }

      const char *port_str = TBSYS_CONFIG.getString(kConfigSys, std::string(kSysPort));
      if (port_str != NULL) {
        port_ = static_cast<int16_t>(atoi(port_str));
      }

      consistency_ = (bool)TBSYS_CONFIG.getInt(kConfigSys, std::string(kSysConsistency), 1);

      const char *network_timeout = TBSYS_CONFIG.getString(kConfigSys, std::string(kSysNetworkTimeout));
      if (network_timeout != NULL ) {
        network_timeout_ = atoi(network_timeout);
      }

      const char * max_file_size = TBSYS_CONFIG.getString(kConfigSys, std::string(kSysMaxFileSize));
      if (max_file_size) {
        max_file_size_ = atol(max_file_size);
      }

      const char *rotate_file_interval = TBSYS_CONFIG.getString(kConfigSys, std::string(kSysRotateFileTime));
      if (rotate_file_interval)
        rotate_file_interval_ = atol(rotate_file_interval);


      const char *monitor_interval_str = TBSYS_CONFIG.getString(kConfigSys, std::string(kSysMonitorInterval));
      if (monitor_interval_str)
        nas_check_interval_ = atol(monitor_interval_str);

      const char *tmp_log_path_str = TBSYS_CONFIG.getString(kConfigSys, std::string(kSysTmpLogDir));
      if (tmp_log_path_str)
        tmp_log_path_ = tmp_log_path_str;

      const char *init_log_id_str = TBSYS_CONFIG.getString(kConfigSys, std::string(kSysInitLogId));
      if (init_log_id_str) {
        init_log_id_ = atol(init_log_id_str);
      } else 
        init_log_id_ = 0;

      const char *header_delima_str = TBSYS_CONFIG.getString(kConfigSys, std::string(kSysHeaderDelima));
      if (header_delima_str) {
        header_delima_ = (char)atoi(header_delima_str);
      }

      const char *body_delima_str = TBSYS_CONFIG.getString(kConfigSys, std::string(kSysBodyDelima));
      if (body_delima_str) {
        body_delima_ = (char)atoi(body_delima_str);
      }

      const char *app_name_str = TBSYS_CONFIG.getString(kConfigSys, std::string(kSysAppName));
      if (app_name_str) {
        app_name_ = app_name_str;
      }

      const char *pid_file_str = TBSYS_CONFIG.getString(kConfigSys, std::string(kSysPidFile));
      if (pid_file_str) {
        pid_file_ = pid_file_str;
      }

      const char *max_nolog_interval_str = 
        TBSYS_CONFIG.getString(kConfigSys, std::string(kSysMaxNologInvertal));
      if (max_nolog_interval_str) {
        max_nolog_interval_ = atol(max_nolog_interval_str);
      }

      init_date_ = TBSYS_CONFIG.getString(kConfigSys, std::string(kSysInitDate));

      muti_get_nr_ = TBSYS_CONFIG.getInt(kConfigSys, std::string(kSysMutiGetNr), 0);
    }
    itr++;
  }

  ret = gen_table_id_map();

  return ret;
}


int DbDumpConfigMgr::get_table_config(std::string table, DbTableConfig* &cfg)
{
  std::vector<DbTableConfig>::iterator itr = configs_.begin();
  while(itr != configs_.end()) {
    if (table == itr->table()) {
      cfg = &(*itr);
      return OB_SUCCESS;
    }
    itr++;
  }

  return OB_ERROR;
}

int DbDumpConfigMgr::gen_table_id_map()
{
  int ret = OB_SUCCESS;
  std::vector<DbTableConfig>::iterator itr = configs_.begin();

  while(itr != configs_.end()) {
    config_set_[itr->table_id()] = *itr;
    itr++;
  }
  if (configs_.size() != config_set_.size()) {
    TBSYS_LOG(ERROR, "duplicate table id found");
    ret = OB_ERROR;
  }

  return ret;
}

int DbDumpConfigMgr::get_table_config(uint64_t table_id, DbTableConfig* &cfg)
{
  int ret = OB_SUCCESS;

  std::map<uint64_t, DbTableConfig>::iterator itr = 
    config_set_.find(table_id);

  if (itr == config_set_.end()) {
    ret = OB_ERROR;
  } else {
    cfg = &(itr->second);
  }

  return ret;
}

DbDumpConfigMgr::~DbDumpConfigMgr()
{
  for(size_t i = 0;i < filter_set_.size(); i++) {
    if (filter_set_[i] != NULL)
      delete filter_set_[i];
  }
}

void DbDumpConfigMgr::destory()
{
  if (instance_ != NULL)
    delete instance_;
}
