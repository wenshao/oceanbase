/*
 *  (C) 2007-2010 Taobao Inc.
 *  
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License version 2 as
 *  published by the Free Software Foundation.
 *
 *         cs_admin.cpp is for what ...
 *
 *  Version: $Id: cs_admin.cpp 2010年12月03日 13时48分14秒 qushan Exp $
 *
 *  Authors:
 *     qushan < qushan@taobao.com >
 *        - some work details if you want
 */

#include "cs_admin.h"
#include <vector>
#include "common/ob_define.h"
#include "common/ob_malloc.h"
#include "common/ob_string.h"
#include "common/utility.h"
#include "chunkserver/ob_tablet_image.h"
#include "chunkserver/ob_fileinfo_cache.h"
#include "sstable/ob_sstable_reader.h"
#include "sstable/ob_sstable_trailer.h"
#include "ob_server_stats.h"
#include "ob_cluster_stats.h"
#include "common_func.h"


using namespace std;
using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::common::serialization;
using namespace oceanbase::chunkserver;
using namespace oceanbase::sstable;
using namespace oceanbase::tools;

static char g_config_file_name[OB_MAX_FILE_NAME_LENGTH];

//--------------------------------------------------------------------
// class GFactory
//--------------------------------------------------------------------
GFactory GFactory::instance_;

GFactory::GFactory()
{
}

GFactory::~GFactory()
{
}

void GFactory::init_cmd_map()
{
  cmd_map_["dump_tablet_image"] = CMD_DUMP_TABLET;
  cmd_map_["dump_server_stats"] = CMD_DUMP_SERVER_STATS;
  cmd_map_["dump_cluster_stats"] = CMD_DUMP_CLUSTER_STATS;
  cmd_map_["dump_app_stats"] = CMD_DUMP_APP_STATS;
  cmd_map_["manual_merge"] = CMD_MANUAL_MERGE;
  cmd_map_["manual_drop_tablet"] = CMD_MANUAL_DROP_TABLET;
  cmd_map_["manual_gc"] = CMD_MANUAL_GC;
  cmd_map_["get_tablet_info"] = CMD_RS_GET_TABLET_INFO;
  cmd_map_["scan_root_table"] = CMD_SCAN_ROOT_TABLE;
  cmd_map_["migrate_tablet"] = CMD_MIGRATE_TABLET;
  cmd_map_["check_merge_process"] = CMD_CHECK_MERGE_PROCESS;
  cmd_map_["show_param"] = CMD_SHOW_PARAM;
  cmd_map_["change_log_level"] = CMD_CHANGE_LOG_LEVEL;
  cmd_map_["stop_server"] = CMD_STOP_SERVER;
  cmd_map_["restart_server"] = CMD_RESTART_SERVER;
  cmd_map_["create_tablet"] = CMD_CREATE_TABLET;
  cmd_map_["delete_tablet"] = CMD_DELETE_TABLET;
  cmd_map_["sync_images"] = CMD_SYNC_ALL_IMAGES;
}

int GFactory::initialize(const ObServer& remote_server)
{
  common::ob_init_memory_pool();
  common::ob_init_crc64_table(OB_DEFAULT_CRC64_POLYNOM);
  init_cmd_map();

  int ret = client_.initialize();
  if (OB_SUCCESS != ret) 
  {
    TBSYS_LOG(ERROR, "initialize client object failed, ret:%d", ret);
    return ret;
  }

  ret = rpc_stub_.initialize(remote_server, &client_.get_client_manager());
  if (OB_SUCCESS != ret) 
  {
    TBSYS_LOG(ERROR, "initialize rpc stub failed, ret:%d", ret);
  }
  return ret;
}

int GFactory::start()
{
  client_.start();
  return OB_SUCCESS;
}

int GFactory::stop()
{
  client_.stop();
  return OB_SUCCESS;
}

int GFactory::wait()
{
  client_.wait();
  return OB_SUCCESS;
}


int parse_command(const char *cmdlist, vector<string> &param)
{
    int cmd = CMD_MIN;
    const int CMD_MAX_LEN = 1024;
    char key_buf[CMD_MAX_LEN];
    snprintf(key_buf, CMD_MAX_LEN, "%s", cmdlist);
    char *key = key_buf;
    char *token = NULL;
    while(*key == ' ') key++; // 去掉前空格
    token = key + strlen(key);
    while(*(token-1) == ' ' || *(token-1) == '\n' || *(token-1) == '\r') token --;
    *token = '\0';
    if (key[0] == '\0') {
        return cmd;
    }
    token = strchr(key, ' ');
    if (token != NULL) {
        *token = '\0';
    }
    const map<string, int> & cmd_map = GFactory::get_instance().get_cmd_map();
    map<string, int>::const_iterator it = cmd_map.find(key);
    if (it == cmd_map.end()) 
    {
        return CMD_MIN;    
    } 
    else 
    {
        cmd = it->second;
    }

    if (token != NULL) 
    {
        token ++;
        key = token;
    } 
    else 
    {
        key = NULL;
    }
    //分解
    param.clear();
    while((token = strsep(&key, " ")) != NULL) {
        if (token[0] == '\0') continue;
        param.push_back(token);
    }
    return cmd;
}

int get_tablet_info(const vector<string> &param)
{
  int ret = 0;
  if (param.size() <= 2) 
  {
    fprintf(stderr, "get_tablet_info: input param error\n");
    fprintf(stderr, "get_tablet_info table_id table_name range_str\n");
    return OB_ERROR;
  }

  ObRange range;

  int64_t table_id = strtoll(param[0].c_str(), NULL, 10);
  const char* table_name = param[1].c_str();
  ret  = parse_range_str(param[2].c_str(), 1, range);
  if (OB_SUCCESS != ret) return ret;

  int safe_copy_num = 3;
  ObTabletLocation location[safe_copy_num];

  ret = GFactory::get_instance().get_rpc_stub().get_tablet_info(
      table_id, table_name, range, location, safe_copy_num);
  fprintf(stderr, "return size = %d, ret= %d\n", safe_copy_num, ret);
  if (OB_SUCCESS != ret) return ret;

  char host_string[256];
  for (int i = 0; i < safe_copy_num; ++i)
  {
    location[i].chunkserver_.to_string(host_string, 256);
    fprintf(stderr, "location (%d), version = %ld , host = %s \n", 
        i, location[i].tablet_version_, host_string);
  }

  return ret;
}

void dump_multi_version_tablet_image(ObMultiVersionTabletImage & image, bool load_sstable)
{
  ObTablet *tablet  = NULL;
  uint64_t crcsum = 0;
  int tablet_index = 0;
  char range_bufstr[OB_MAX_FILE_NAME_LENGTH];

  int ret = image.begin_scan_tablets();
  while (OB_SUCCESS == ret)
  {
    ret = image.get_next_tablet(tablet);
    if (OB_SUCCESS == ret)
    {
      tablet->get_range().to_string(range_bufstr, OB_MAX_FILE_NAME_LENGTH);
      crcsum = tablet->get_checksum();
      fprintf(stderr, "tablet(%d) : %s \n", tablet_index, range_bufstr);
      fprintf(stderr, "\t info: data version = %ld , disk no = %d, "
          "row count = %ld, occupy size = %ld , crc sum = %ld , merged= %d, removed= %d\n", 
          tablet->get_data_version(), tablet->get_disk_no(), tablet->get_row_count(), 
          tablet->get_occupy_size(), crcsum, tablet->is_merged(), tablet->is_removed());
      // dump sstable content
      const ObArrayHelper<ObSSTableId> & sstable_id_array = tablet->get_sstable_id_list();
      const ObArrayHelper<ObSSTableReader*> & sstable_reader_array = tablet->get_sstable_reader_list();

      if (!load_sstable)
      {
        for (int64_t idx = 0; idx < sstable_id_array.get_array_index(); ++idx)
        {
          ObSSTableId id = *sstable_id_array.at(idx);
          fprintf(stderr, "\t sstable[%ld]:%ld\n", idx, id.sstable_file_id_);
        }
      }
      else
      {
        for (int64_t idx = 0; idx < sstable_reader_array.get_array_index(); ++idx)
        {
          ObSSTableReader *reader = *sstable_reader_array.at(idx);
          fprintf(stderr, "\t sstable[%ld]:%ld, size = %d, block count = %ld , row count = %ld\n", 
              idx, sstable_id_array.at(idx)->sstable_file_id_,
              reader->get_trailer().get_size(), 
              reader->get_trailer().get_block_count(), 
              reader->get_trailer().get_row_count() );
        }
      }

      ++tablet_index;
    }
    if (NULL != tablet) image.release_tablet(tablet);
  }
  image.end_scan_tablets();
}

int dump_tablet_image(const vector<string> &param)
{
  int ret = 0;
  if (param.size() < 1) 
  {
    fprintf(stderr, "dump_tablet_image: input param error\n");
    fprintf(stderr, "dump_tablet_image disk_no \n");
    fprintf(stderr, "dump_tablet_image disk_no_1, disk_no_2, ... \n");
    fprintf(stderr, "dump_tablet_image disk_no_min~disk_no_max \n");
    return OB_ERROR;
  }

  const string &disk_list = param[0];
  int32_t disk_no_size = 64;
  int32_t disk_no_array[disk_no_size];
  ret = parse_number_range(disk_list.c_str(), disk_no_array, disk_no_size);
  if (ret)
  {
    fprintf(stderr, "parse disk_list(%s) parameter failure\n", disk_list.c_str());
    return ret;
  }

  fprintf(stderr, "dump disk count=%d\n", disk_no_size);

  int64_t pos = 0;
  FileInfoCache cache;
  cache.init(10);
  ObMultiVersionTabletImage image_obj(cache);
  for (int i = 0; i < disk_no_size; ++i)
  {
    for (int idx = 0; idx < 2; ++idx)
    {
      ObString image_buf;
      ret = GFactory::get_instance().get_rpc_stub().cs_dump_tablet_image(idx, disk_no_array[i], image_buf);
      if (OB_SUCCESS != ret) 
      {
        //fprintf(stderr, "disk = %d has no tablets \n ", disk_no_array[i]);
      }
      else
      {
        pos = 0;
        ret = image_obj.deserialize(disk_no_array[i], image_buf.ptr(), image_buf.length(), pos);
      }
    }
  }

  dump_multi_version_tablet_image(image_obj, false);
  return ret;
}

int check_merge_process(const vector<string> &param)
{
  int ret = 0;
  if (param.size() < 1) 
  {
    fprintf(stderr, "check_merge_process: input param error\n");
    fprintf(stderr, "check_merge_process disk_no \n");
    fprintf(stderr, "check_merge_process disk_no_1, disk_no_2, ... \n");
    fprintf(stderr, "check_merge_process disk_no_min~disk_no_max \n");
    return OB_ERROR;
  }

  const string &disk_list = param[0];
  int32_t disk_no_size = 64;
  int32_t disk_no_array[disk_no_size];
  ret = parse_number_range(disk_list.c_str(), disk_no_array, disk_no_size);
  if (ret)
  {
    fprintf(stderr, "parse disk_list(%s) parameter failure\n", disk_list.c_str());
    return ret;
  }

  int64_t pos = 0;
  FileInfoCache cache;
  cache.init(10);
  ObMultiVersionTabletImage image_obj(cache);
  for (int i = 0; i < disk_no_size; ++i)
  {
    for (int idx = 0; idx < 2; ++idx)
    {
      ObString image_buf;
      ret = GFactory::get_instance().get_rpc_stub().cs_dump_tablet_image(idx, disk_no_array[i], image_buf);
      if (OB_SUCCESS != ret) 
      {
        //fprintf(stderr, "disk = %d has no tablets \n ", disk_no_array[i]);
      }
      else
      {
        pos = 0;
        ret = image_obj.deserialize(disk_no_array[i], image_buf.ptr(), image_buf.length(), pos);
      }
    }
  }

  //dump_multi_version_tablet_image(image_obj, false);

  //fprintf(stderr, "serving version=%ld\n", image_obj.get_serving_version());


  ret = image_obj.begin_scan_tablets();
  ObTablet* tablet = NULL;
  int64_t low_version = 0;
  int64_t high_version = 0;

  struct MERGE_COUNT
  {
    int32_t merged;
    int32_t not_merged;
    int32_t removed;
    int32_t count;
    MERGE_COUNT() : merged(0), not_merged(0), removed(0), count(0) {}
  };

  MERGE_COUNT low_count, high_count;
  //map<int64_t, MERGE_COUNT> tablet_version_map;
  //map<int64_t, MERGE_COUNT>::iterator find_it = tablet_version_map.end();
  while (OB_SUCCESS == ret)
  {
    ret = image_obj.get_next_tablet(tablet);
    if (OB_SUCCESS == ret && NULL != tablet)
    {
      int64_t version = tablet->get_data_version();
      if (low_version == 0) low_version = version;

      if (version == low_version)
      {
        if (tablet->is_removed())
        {
          low_count.removed++;
        }
        else if (tablet->is_merged())
        {
          low_count.merged++;
        }
        else
        {
          low_count.not_merged++;
        }
        low_count.count++;
      }
      else
      {
        high_version = version;
        if (tablet->is_removed())
        {
          high_count.removed++;
        }
        else if (tablet->is_merged())
        {
          high_count.merged++;
        }
        else
        {
          high_count.not_merged++;
        }
        high_count.count++;
      }
    }

    if (NULL != tablet) image_obj.release_tablet(tablet);
  }

  if (low_count.not_merged == 0)
  {
    if (high_count.merged != 0)
    {
      fprintf(stderr, "exception, low version %ld (%d,%d) ,high version %ld (%d,%d)\n", 
          low_version, low_count.merged, low_count.not_merged, high_version, high_count.merged, high_count.not_merged);
    }
    else
    {
      fprintf(stderr, "not in merge process, serving version= %ld, tablet count= %d\n", high_version, high_count.count);
    }
  }
  else
  {
    fprintf(stderr, "in merge process, merge version %ld to %ld, merged count= %d, not merged count = %d\n", 
        low_version, high_version, low_count.merged, low_count.not_merged);
  }

  return ret;
}

int migrate_tablet(const vector<string> &param)
{
  int ret = 0;
  if (param.size() < 5) 
  {
    fprintf(stderr, "migrate_tablet: input param error\n");
    fprintf(stderr, "migrate_tablet table range range_format dest_server keep_src\n");
    return OB_ERROR;
  }

  const int64_t table_id = atoi(param[0].c_str());
  const string &range_str = param[1];
  const int hex_format = atoi(param[2].c_str());
  const string &dest_server = param[3];
  int keep_src = atoi(param[4].c_str());

  ObServer dest;

  string ip,port;

  string::size_type pos = dest_server.find_first_of(":");
  if (pos == string::npos)
  {
    fprintf(stderr, "parse dest_server(%s) parameter failure\n", dest_server.c_str());
  }
  else
  {
    ip = dest_server.substr(0, pos);
    port = dest_server.substr(pos+1, string::npos);
    dest.set_ipv4_addr(ip.c_str(), atoi(port.c_str()));
  }

  ObRange range;
  range.table_id_ = table_id;
  ret = parse_range_str(range_str.c_str(), hex_format, range);
  if (ret)
  {
    fprintf(stderr, "parse range_str(%s) parameter failure\n", range_str.c_str());
    return ret;
  }

  char range_buffer[256];
  char server_buffer[256];

  range.to_string(range_buffer, 256);
  dest.to_string(server_buffer, 256);


  fprintf(stderr, "migrate tablet : %s to dest server : %s \n", range_buffer, server_buffer);

  ret = GFactory::get_instance().get_rpc_stub().migrate_tablet(dest, range, keep_src);
  if (OB_SUCCESS != ret) 
  {
    fprintf(stderr, "migrate range (%s) to server (%s) keep_src(%d) failed.\n", 
        range_str.c_str(), dest_server.c_str(), keep_src);
  }

  return ret;
}

int create_tablet(const vector<string> &param)
{
  int ret = 0;
  if (param.size() < 4) 
  {
    fprintf(stderr, "create_tablet: input param error\n");
    fprintf(stderr, "create_tablet table range range_format last_frozen_version\n");
    return OB_ERROR;
  }

  const int64_t table_id = atoi(param[0].c_str());
  const string &range_str = param[1];
  const int hex_format = atoi(param[2].c_str());
  const int64_t last_frozen_version = strtoll(param[3].c_str(), NULL, 10);

  ObRange range;
  range.table_id_ = table_id;
  ret = parse_range_str(range_str.c_str(), hex_format, range);
  if (ret)
  {
    fprintf(stderr, "parse range_str(%s) parameter failure\n", range_str.c_str());
    return ret;
  }

  char range_buffer[1024];
  range.to_string(range_buffer, 1024);

  fprintf(stderr, "create tablet : %s\n", range_buffer);

  ret = GFactory::get_instance().get_rpc_stub().create_tablet(range, last_frozen_version);
  if (OB_SUCCESS != ret) 
  {
    fprintf(stderr, "create range (%s) failed.\n", range_str.c_str());
  }

  return ret;
}

int delete_tablet(const vector<string> &param)
{
  int ret = 0;
  if (param.size() < 5) 
  {
    fprintf(stderr, "delete_tablet: input param error\n");
    fprintf(stderr, "delete_tablet table range range_format tablet_version is_force\n");
    return OB_ERROR;
  }

  const int64_t table_id = atoi(param[0].c_str());
  const string &range_str = param[1];
  const int hex_format = atoi(param[2].c_str());
  const int64_t tablet_version = strtoll(param[3].c_str(), NULL, 10);
  int is_force = atoi(param[4].c_str()); 

  ObRange range;
  range.table_id_ = table_id;
  ret = parse_range_str(range_str.c_str(), hex_format, range);
  if (ret)
  {
    fprintf(stderr, "parse range_str(%s) parameter failure\n", range_str.c_str());
    return ret;
  }

  char range_buffer[1024];
  range.to_string(range_buffer, 1024);
  fprintf(stderr, "delete tablet : %s\n", range_buffer);

  ObTabletReportInfo tablet_info;
  ObTabletReportInfoList info_list;
  tablet_info.tablet_info_.range_ = range;
  tablet_info.tablet_location_.tablet_version_ = tablet_version;
  ret = info_list.add_tablet(tablet_info);
  if (OB_SUCCESS != ret)
  {
    fprintf(stderr, "failed to add tablet info to delete tablet info list, range=%s\n",
      range_buffer);
  }
  else 
  {
    ret = GFactory::get_instance().get_rpc_stub().delete_tablet(info_list, is_force);
    if (OB_SUCCESS != ret) 
    {
      fprintf(stderr, "delete range (%s) failed.\n", range_str.c_str());
    }
  }

  return ret;
}

typedef std::map<ObString, map<string, int64_t> > SimpleRootTable;

int store_root_table(ObScanner & scanner, SimpleRootTable & root_table, bool & end_of_table, ObString & end_row_key)
{
  int ret = OB_SUCCESS;
  
  ObScannerIterator iter;
  int row_count = 0;
  int column_index = 0;
  bool is_row_changed = false;

  char column_strbuf[OB_MAX_ROW_KEY_LENGTH];
  map<string, int64_t> root_item;
  int64_t column_value = 0;
  ObString last_row_key;

  end_of_table = false;
  root_table.clear();

  for (iter = scanner.begin(); iter != scanner.end(); iter++)
  {
    ObCellInfo *cell_info = NULL;
    iter.get_cell(&cell_info, &is_row_changed);
    if (NULL == cell_info)
    {
      TBSYS_LOG(ERROR, "get null cell_info");
      ret = OB_ERROR;
      break;
    }

    if (is_row_changed)
    {
      column_index = 0;

      //hex_dump(cell_info->row_key_.ptr(), cell_info->row_key_.length());
      if (row_count >= 1)
      {
        ///strncpy(column_strbuf, cell_info->row_key_.ptr(), cell_info->row_key_.length());
        //string row_key_str(column_strbuf);
        //fprintf(stderr, "column_strbuf=%s\n", column_strbuf);
        //hex_dump(cell_info->row_key_.ptr(), cell_info->row_key_.length());
        root_table.insert(make_pair(last_row_key, root_item));
        root_item.clear();
      }

      ++row_count;
    }
    else
    {
      last_row_key = cell_info->row_key_;
      ++column_index;
    }



    if (cell_info->value_.get_type() == ObIntType)
    {
      cell_info->value_.get_int(column_value);
    }

    memset(column_strbuf, 0, OB_MAX_ROW_KEY_LENGTH);
    strncpy(column_strbuf, cell_info->column_name_.ptr(), cell_info->column_name_.length());
    string column_name(column_strbuf);
    root_item.insert(make_pair(column_name, column_value));
    //fprintf(stderr, "column_name=%s, column_value=%ld\n", column_name.c_str(), column_value);


    /*
    TBSYS_LOG(DEBUG, "---------------------%d----------------------\n", row_count);
    hex_dump(cell_info->row_key_.ptr(), cell_info->row_key_.length());
    TBSYS_LOG(DEBUG,"table_id:%lu, table_name:%.*s, column_id:%ld, column_name:%.*s, is_row_changed:%d\n", 
        cell_info->table_id_, cell_info->table_name_.length(), cell_info->table_name_.ptr(),
        cell_info->column_id_, cell_info->column_name_.length(), cell_info->column_name_.ptr(), is_row_changed);
    cell_info->value_.dump();
    TBSYS_LOG(DEBUG,"---------------------%d----------------------, check ret=%d\n", row_count, ret);
    */

    if (OB_SUCCESS != ret) break;
  }

  if (OB_SUCCESS == ret && last_row_key.ptr())
  {
    root_table.insert(make_pair(last_row_key, root_item));
    if (last_row_key.length() >= 1 && *(unsigned char*)(last_row_key.ptr()) == 0xff)
    {
      end_of_table = true;
    }
    end_row_key = last_row_key;
    //hex_dump(last_row_key.ptr(), last_row_key.length());
  }


  //fprintf(stderr, "row_count=%d, size=%d\n", row_count,root_table.size());
  return ret;
}

int64_t sum_total_line(const SimpleRootTable & root_table)
{
  SimpleRootTable::const_iterator it = root_table.begin();
  map<string, int64_t>::const_iterator item_it ;
  int64_t total_line = 0;

  while (it != root_table.end())
  {
    //const ObString & row_key = it->first;
    //hex_dump(row_key.ptr(), row_key.length());
    const map<string, int64_t>& item = it->second;
    //fprintf(stderr, "item.size=%d\n", item.size());
    if (item.end() != (item_it = item.find("record_count")))
    {
      //fprintf(stderr, "line=%ld\n", item_it->second);
      total_line += item_it->second;
    }
    else
    {
      fprintf(stderr, "error cannot find occupy_size\n");
      //hex_dump(row_key.ptr(), row_key.length());
    }

    ++it;
  }
  return total_line;  
}

int parse_rowkey_split(const char* split_str, RowkeySplit& rowkey_split)
{
  int ret = OB_SUCCESS;
  char* rowkey_split_desc = strdup(split_str);
  char* token = NULL;
  int cnt = 0;
  int64_t rowkey_size = 0;

  if (NULL == rowkey_split_desc)
  {
    ret = OB_ERROR;
  }

  while(OB_SUCCESS == ret && 
        (token = strsep(&rowkey_split_desc, ",")) != NULL) {
    if (token[0] == '\0') continue;

    if (cnt >= RowkeySplit::MAX_ROWKEY_SPLIT)
    {
      fprintf(stderr, "too many rowkey splits parts, only support max parts num=%ld\n",
        RowkeySplit::MAX_ROWKEY_SPLIT);
      ret = OB_ERROR;
      break;
    }
    ret = sscanf(token, "%d-%d", &rowkey_split.item_[cnt].item_type_, 
      &rowkey_split.item_[cnt].item_size_);
    if (2 != ret)
    {
      fprintf(stderr, "sscanf rowkey split failed, ret=%d\n", ret);
      ret = OB_ERROR;
      break;
    }
    else 
    {
      rowkey_size += rowkey_split.item_[cnt].item_size_;
      cnt++;
      ret = OB_SUCCESS;
    }
  }

  if (OB_SUCCESS == ret)
  {
    rowkey_split.item_size_ = cnt;
    rowkey_split.rowkey_size_ = rowkey_size;
  }

  if (NULL != rowkey_split_desc)
  {
    free(rowkey_split_desc);
  }

  return ret;
}

int build_split_rowkey(char* buf, const int64_t buf_len, 
  int64_t& pos, const ObString& rowkey, const ScanRootTableArg& arg)
{
  int ret = OB_SUCCESS;
  const RowkeySplit& split = arg.rowkey_split_;
  int64_t tmp_pos = 0;
  const char* cur_str = rowkey.ptr();
  const char* end_str = cur_str + rowkey.length();
  int64_t val64 = 0;
  int32_t val32 = 0;
  int16_t val16 = 0;
  int8_t val8 = 0;
  time_t time_val = 0;
  struct tm *tm_struct = NULL;
  int64_t varchar_len = 0;

  if (NULL == buf || buf_len <= 0 || buf_len <= pos 
      || split.item_size_ <= 0 || NULL == cur_str || rowkey.length() <= 0)
  {
    fprintf(stderr, "invalid param buf=%p, buf_len=%ld, "
                    "pos=%ld, split.item_size_=%ld, "
                    "rowkey.ptr=%p, rowkey.len=%d\n",
      buf, buf_len, pos, split.item_size_, cur_str, rowkey.length());
    ret = OB_ERROR;
  }

  if (OB_SUCCESS == ret)
  {
    for (int i = 0; i < split.item_size_ && OB_SUCCESS == ret; ++i)
    {
      if (split.rowkey_size_ == rowkey.length() 
          && cur_str + split.item_[i].item_size_> end_str)
      {
        fprintf(stderr, "rowkey part is out of boundary\n");
        ret = OB_ERROR;
        break;
      }
      else if (split.item_[i].item_size_ <= 0 
               || (split.rowkey_size_ == rowkey.length() 
                   && pos + split.item_[i].item_size_ + 1 > buf_len))
      {
        fprintf(stderr, "not enough space to store rowkey\n");
        ret = OB_ERROR;
        break;
      }
      else if (i > 0)
      {
        pos += snprintf(buf + pos, buf_len - pos, "%c", arg.key_delim_);
      }
      tmp_pos = 0;

      switch (split.item_[i].item_type_)
      {
        case VARCHAR:
          if (split.rowkey_size_ > rowkey.length())
          {
            /**
             * if there is variable char column in rowkey, the item_size_ is 
             * 0, and the variable char end with \0 
             */
            varchar_len = static_cast<int64_t>(strlen(cur_str));
            memcpy(buf + pos, cur_str, varchar_len);
            cur_str += varchar_len + 1;
            pos += varchar_len;
          }
          else
          {
            memcpy(buf + pos, cur_str, split.item_[i].item_size_);
            cur_str += split.item_[i].item_size_;
            pos += split.item_[i].item_size_;
          }
          break;

        case INT64_T:
          if (split.item_[i].item_size_ != 8)
          {
            fprintf(stderr, "INT64_T must be 8 bytes, item_size_=%d\n",
              split.item_[i].item_size_);
            ret = OB_ERROR;
          }
          else 
          {
            ret = decode_i64(cur_str, split.item_[i].item_size_, tmp_pos, &val64);
            if (OB_SUCCESS == ret)
            {
              pos += snprintf(buf + pos, buf_len - pos, "%ld", val64);
              cur_str += split.item_[i].item_size_;
            }
          }
          break;

        case INT32_T:
          if (split.item_[i].item_size_ != 4)
          {
            fprintf(stderr, "INT32_T must be 4 bytes, item_size_=%d\n",
              split.item_[i].item_size_);
            ret = OB_ERROR;
          }
          else {
            ret = decode_i32(cur_str, split.item_[i].item_size_, tmp_pos, &val32);
            if (OB_SUCCESS == ret)
            {
              pos += snprintf(buf + pos, buf_len - pos, "%d", val32);
              cur_str += split.item_[i].item_size_;
            }
          }
          break;

        case INT16_T:
          if (split.item_[i].item_size_ != 2)
          {
            fprintf(stderr, "INT16_T must be 2 bytes, item_size_=%d\n",
              split.item_[i].item_size_);
            ret = OB_ERROR;
          }
          else {
            ret = decode_i16(cur_str, split.item_[i].item_size_, tmp_pos, &val16);
            if (OB_SUCCESS == ret)
            {
              pos += snprintf(buf + pos, buf_len - pos, "%d", val16);
              cur_str += split.item_[i].item_size_;
            }
          }
          break;

        case INT8_T:
          if (split.item_[i].item_size_ != 1)
          {
            fprintf(stderr, "INT8_T must be 1 bytes, item_size_=%d\n",
              split.item_[i].item_size_);
            ret = OB_ERROR;
          }
          else {
            ret = decode_i8(cur_str, split.item_[i].item_size_, tmp_pos, &val8);
            if (OB_SUCCESS == ret)
            {
              pos += snprintf(buf + pos, buf_len - pos, "%d", val8);
              cur_str += split.item_[i].item_size_;
            }
          }
          break;

        case DATETIME_1:
        case DATETIME_2:
        case DATETIME_3:
        case DATETIME_4:
        case DATETIME_5:
          if (split.item_[i].item_size_ == 8)
          {
            ret = decode_i64(cur_str, split.item_[i].item_size_, tmp_pos, &val64);
            if (OB_SUCCESS == ret)
            {
              time_val = val64 / (1000 * 1000); //convert us to s
            }
          }
          else if (split.item_[i].item_size_ == 4)
          {
            ret = decode_i32(cur_str, split.item_[i].item_size_, tmp_pos, &val32);
            if (OB_SUCCESS == ret)
            {
              time_val = val32;
            }
          }
          else 
          {
            fprintf(stderr, "DATETIME must be 4 or 8 bytes, item_size_=%d\n",
              split.item_[i].item_size_);
            ret = OB_ERROR;
          }
          if (OB_SUCCESS == ret)
          {
            tm_struct = localtime(&time_val);
            if (OB_SUCCESS == ret)
            {
              switch (split.item_[i].item_type_)
              {
              case DATETIME_1:
                pos += snprintf(buf + pos, buf_len - pos, "%4d-%2d-%2d %2d:%2d:%2d",
                    tm_struct->tm_year, tm_struct->tm_mon, tm_struct->tm_mday, 
                    tm_struct->tm_hour, tm_struct->tm_min, tm_struct->tm_sec);
                break;
              case DATETIME_2:
                pos += snprintf(buf + pos, buf_len - pos, "%4d-%2d-%2d",
                    tm_struct->tm_year, tm_struct->tm_mon, tm_struct->tm_mday);
                break;
              case DATETIME_3:
                pos += snprintf(buf + pos, buf_len - pos, "%4d%2d%2d %2d:%2d:%2d",
                    tm_struct->tm_year, tm_struct->tm_mon, tm_struct->tm_mday, 
                    tm_struct->tm_hour, tm_struct->tm_min, tm_struct->tm_sec);
                break;
              case DATETIME_4:
                pos += snprintf(buf + pos, buf_len - pos, "%4d%2d%2d%2d%2d%2d",
                    tm_struct->tm_year, tm_struct->tm_mon, tm_struct->tm_mday, 
                    tm_struct->tm_hour, tm_struct->tm_min, tm_struct->tm_sec);
                break;
              case DATETIME_5:
                pos += snprintf(buf + pos, buf_len - pos, "%4d%2d%2d",
                    tm_struct->tm_year, tm_struct->tm_mon, tm_struct->tm_mday);
                break;
              default:
                fprintf(stderr, "upsupport datetime type, type=%d", split.item_[i].item_type_);
                ret = OB_ERROR;
                break;
              }
            }
            if (OB_SUCCESS == ret)
            {
              cur_str += split.item_[i].item_size_;
            }
          }
          break ;

        default:
          fprintf(stderr, "unsupport split type, type=%d", split.item_[i].item_type_);
          ret = OB_ERROR;
          break;
      }
    }
  }

  return ret;
}

int write_root_table_file(FILE* fp, const ScanRootTableArg& arg, 
  const SimpleRootTable & root_table, int64_t& index)
{
  int ret = OB_SUCCESS;
  SimpleRootTable::const_iterator it = root_table.begin();
  map<string, int64_t>::const_iterator item_it ;
  int64_t total_line = 0;
  static const int64_t MAX_LINE_LENGTH = 2048;
  char line[MAX_LINE_LENGTH];
  int64_t pos = 0;
  int64_t copy_size = 0;
  ObServer server;
  char ip_buf[32];

  while (OB_SUCCESS == ret && it != root_table.end())
  {
    const ObString& rowkey = it->first;
    const map<string, int64_t>& item = it->second;

    ret = build_split_rowkey(line, MAX_LINE_LENGTH, pos, rowkey, arg);
    if (OB_SUCCESS == ret)
    {
      copy_size = snprintf(line + pos, MAX_LINE_LENGTH - pos, " %lu-%06ld", arg.table_id_, index);
      if (copy_size > 0)
      {
        pos += copy_size;
        index++;
      }
      else 
      {
        fprintf(stderr, "line buffer is not enough to store table id\n");
        ret = OB_ERROR;
      }
    }

    if (OB_SUCCESS == ret)
    {
      if (rowkey.length() * 2 + 1 > MAX_LINE_LENGTH - pos)
      {
        fprintf(stderr, "line buffer is not enough to store rowkey\n");
        ret = OB_ERROR;
      }
      else 
      {
        pos += snprintf(line + pos, MAX_LINE_LENGTH - pos, "%c", arg.column_delim_);
        copy_size = hex_to_str(rowkey.ptr(), rowkey.length(), 
          line + pos, static_cast<int32_t>(MAX_LINE_LENGTH - pos));
        pos += copy_size * 2;
      }
    }

    if (OB_SUCCESS == ret)
    {
      if (item.end() != (item_it = item.find("1_ipv4")))
      {
        server.set_ipv4_addr(static_cast<int32_t>(item_it->second), 0);
        if (server.ip_to_string(ip_buf, 32))
        {
          if ((int)strlen(ip_buf) + 1 > MAX_LINE_LENGTH - pos)
          {
            fprintf(stderr, "line buffer is not enough to store first cs 1 ip\n");
            ret = OB_ERROR;
          }
          else
          {
            pos += snprintf(line + pos, MAX_LINE_LENGTH - pos, "%c", arg.column_delim_);
            memcpy(line + pos, ip_buf, strlen(ip_buf));
            pos += strlen(ip_buf);
          }
        }
      }
      else
      {
        fprintf(stderr, "error cannot find 1_ipv4\n");
      }
    }

    if (OB_SUCCESS == ret)
    {
      if (item.end() != (item_it = item.find("2_ipv4")))
      {
        server.set_ipv4_addr(static_cast<int32_t>(item_it->second), 0);
        if (server.ip_to_string(ip_buf, 32))
        {
          if ((int)strlen(ip_buf) + 1 > MAX_LINE_LENGTH - pos)
          {
            fprintf(stderr, "line buffer is not enough to store first cs 2 ip\n");
            ret = OB_ERROR;
          }
          else
          {
            pos += snprintf(line + pos, MAX_LINE_LENGTH - pos, "%c", arg.column_delim_);
            memcpy(line + pos, ip_buf, strlen(ip_buf));
            pos += strlen(ip_buf);
          }
        }
      }
      else
      {
        fprintf(stderr, "error cannot find 1_ipv4\n");
      }
    }

    if (OB_SUCCESS == ret)
    {
      if (item.end() != (item_it = item.find("3_ipv4")))
      {
        server.set_ipv4_addr(static_cast<int32_t>(item_it->second), 0);
        if (server.ip_to_string(ip_buf, 32))
        {
          if ((int)strlen(ip_buf) + 1 > MAX_LINE_LENGTH - pos)
          {
            fprintf(stderr, "line buffer is not enough to store first cs 3 ip\n");
            ret = OB_ERROR;
          }
          else
          {
            pos += snprintf(line + pos, MAX_LINE_LENGTH - pos, "%c", arg.column_delim_);
            memcpy(line + pos, ip_buf, strlen(ip_buf));
            pos += strlen(ip_buf);
          }
        }
      }
    }

    if (OB_SUCCESS == ret)
    {
      if (1 > MAX_LINE_LENGTH - pos)
      {
        fprintf(stderr, "line buffer is not enough to store \\n\n");
        ret = OB_ERROR;
      }
      else
      {
        pos += snprintf(line + pos, MAX_LINE_LENGTH - pos, "\n");
      }
    }

    if (OB_SUCCESS == ret)
    {
      ret = static_cast<int32_t>(fwrite(line, 1, pos, fp));
      if (pos != ret)
      {
        fprintf(stderr, "failed to write line to root table fiel, "
                        "pos=%ld, wrote_size=%d\n",
          pos, ret);
        ret = OB_ERROR;
      }
      else
      {
        pos = 0;
        ret = OB_SUCCESS;
      }
    }

    ++it;
  }
  return static_cast<int32_t>(total_line);
}

FILE* create_root_table_file(const ScanRootTableArg& arg)
{
  int ret = OB_SUCCESS;
  char file_name[256];
  FILE* fp = NULL;

  if (OB_SUCCESS == ret)
  {
    ret = snprintf(file_name, 256, "%s/%s-%s-%lu-range.ini", 
      arg.output_path_, arg.app_name_, arg.table_name_, arg.table_id_);
    if (ret < 0)
    {
      fprintf(stderr, "failed to build the output file name");
      ret = OB_ERROR;
    }
    else
    {
      ret = OB_SUCCESS;
    }
  }

  if (OB_SUCCESS == ret)
  {
    fp = fopen(file_name, "w+");
    if (NULL == fp)
    {
      fprintf(stderr, "failed to open file_name: %s\n", file_name);
    }
  }

  return fp;
}

int parse_scan_root_table_arg(const vector<string> &param, ScanRootTableArg& arg)
{
  int ret = OB_SUCCESS;
  arg.output_path_ = ".";
  arg.key_delim_ = 1;
  arg.column_delim_ = ' ';

  for (int i = 0; i < (int)param.size() && OB_SUCCESS == ret; ++i)
  {
    switch (i)
    {
      case 0:
        arg.table_name_ = param[i].c_str();
        break;
      case 1:
        arg.table_id_ = strtoull(param[i].c_str(), NULL, 10);
        break;
      case 2:
        ret = parse_rowkey_split(param[i].c_str(), arg.rowkey_split_);
        break;
      case 3:
        arg.app_name_ = param[i].c_str();
        break;
      case 4:
        arg.output_path_ = param[i].c_str();
        break;
      case 5:
        arg.key_delim_ = (char)atoi(param[i].c_str());
        break;
      case 6:
        arg.column_delim_ = (char)atoi(param[i].c_str());
        break;
      default:
        fprintf(stderr, "wrong param specified, i=%d", i);
        ret = OB_ERROR;
        break ;
    }
  }
  printf("table_name=%s, table_id=%lu, output_path=%s\n", 
    arg.table_name_, arg.table_id_, arg.output_path_);
  
  return ret;  
}

int scan_root_table(const vector<string> &param)
{
  int ret = 0;
  ScanRootTableArg scan_arg;
  FILE* fp = NULL;
  int64_t line_index = 0;
  int64_t param_size = param.size();

  if (param_size < 1) 
  {
    fprintf(stderr, "scan_root_table: input param error\n");
    fprintf(stderr, "scan_root_table table_name\n");
    fprintf(stderr, "scan_root_table table_name table_id rowkey_split app_name "
                    "[output_dir = .] [key_delim_assic = 1] [column_delim_assic = ' '] \n"
                    "    rowkey_split: type1-length1,type2-length2...\n"
                    "    example: 3-8,2-4 means part1 is int64_t, 8 bytes\n"
                    "    the second part is int32_t, 4 bytes\n"
                    "    (type: int8_t 0, int16_t 1, int32_t 2, int64_t 3, varchar 4, \n"
                    "     datetime_1 5 YY-MM-DD HH:MM:SS \n"
                    "     datetime_2 6 YY-MM-DD \n"
                    "     datetime_3 7 YYMMDD HH:MM:SS \n"
                    "     datetime_4 8 YYMMDDHHMMSS \n"
                    "     datetime_5 9 YYMMDD)\n");
    return OB_ERROR;
  }
  else if (param_size > 1 && param_size < 4)
  {
    fprintf(stderr, "scan_root_table table_name table_id rowkey_split app_name "
                    "[output_dir = .] [key_delim_assic = 1] [column_delim_assic = ' '] \n"
                    "    rowkey_split format: type1-length1,type2-length2...\n"
                    "    example: 3-8,2-4 means part1 is int64_t, 8 bytes\n"
                    "    the second part is int32_t, 4 bytes\n"
                    "    (type: int8_t 0, int16_t 1, int32_t 2, int64_t 3, varchar 4, \n"
                    "     datetime_1 5 YY-MM-DD HH:MM:SS \n"
                    "     datetime_2 6 YY-MM-DD \n"
                    "     datetime_3 7 YYMMDD HH:MM:SS \n"
                    "     datetime_4 8 YYMMDDHHMMSS \n"
                    "     datetime_5 9 YYMMDD)\n");
    return OB_ERROR;
  }

  const char* input_table_name = param[0].c_str();

  ObScanParam scan_param;
  ObScanner scanner;
  ObString table_name(0, static_cast<int32_t>(strlen(input_table_name)), (char*)input_table_name);

  ObRange query_range;
  query_range.border_flag_.set_min_value();
  query_range.border_flag_.set_max_value();

  SimpleRootTable root_table;

  bool end_of_table = false;
  ObString end_row_key;
  
  int64_t sum_line = 0;
  int64_t total_line = 0;

  scan_param.set(OB_INVALID_ID, table_name, query_range);
  scan_param.set_is_read_consistency(false);

  if (param_size >= 4)
  {
    ret = parse_scan_root_table_arg(param, scan_arg);
    if (OB_SUCCESS == ret)
    {
      fp = create_root_table_file(scan_arg);
      if (NULL == fp)
      {
        ret = OB_ERROR;
      }
    }
  }

  while (OB_SUCCESS == ret)
  {
    ret = GFactory::get_instance().get_rpc_stub().cs_scan(scan_param, scanner);
    if (OB_SUCCESS != ret)
    {
      fprintf(stderr, "scan root server error, ret = %d", ret);
    }
    else
    {
      store_root_table(scanner, root_table, end_of_table, end_row_key);
      total_line = sum_total_line(root_table);
      sum_line  += total_line;
      if (param_size >= 4)
      {
        ret = write_root_table_file(fp, scan_arg, root_table, line_index);
        if (OB_SUCCESS != ret)
        {
          fprintf(stderr, "failed to write root table file\n");
        }
      }
      else {
        //fprintf(stderr, "size=%d,total_line=%ld\n", (int)root_table.size(), sum_total_line(root_table));
        dump_tablet_info(scanner);
      }
    }

    //fprintf(stderr, "end_of_table=%d\n", end_of_table);
    //hex_dump(end_row_key.ptr(), end_row_key.length());

    if (end_of_table)
    {
      break;
    }
    else
    {
      scan_param.reset();
      scanner.reset();

      query_range.border_flag_.unset_min_value();
      query_range.border_flag_.set_max_value();
      query_range.start_key_ = end_row_key;
      scan_param.set(OB_INVALID_ID, table_name, query_range);
      scan_param.set_is_read_consistency(false);
    }
  }

  fprintf(stderr, "\nsum_total_line=%ld\n", sum_line);

  if (NULL != fp)
  {
    fclose(fp);
  }
  return ret;
}

int manual_merge(const vector<string> &param)
{
  int ret = 0;
  if (param.size() < 1) 
  {
    fprintf(stderr, "manual_merge: input param error\n");
    fprintf(stderr, "manual_merge memtable_frozen_version [init_flag=0]\n");
    return OB_ERROR;
  }
  int64_t memtable_frozen_version = strtoll(param[0].c_str(), NULL, 10);
  int32_t init_flag = 0; 
  if (param.size() > 1) init_flag = atoi(param[1].c_str());
  ret = GFactory::get_instance().get_rpc_stub().start_merge(memtable_frozen_version, init_flag);
  return ret;
}

int manual_drop_tablet(const vector<string> &param)
{
  int ret = 0;
  if (param.size() < 1) 
  {
    fprintf(stderr, "manual_drop_tablet: input param error\n");
    fprintf(stderr, "manual_drop_tablet memtable_frozen_version \n");
    return OB_ERROR;
  }
  int64_t memtable_frozen_version = strtoll(param[0].c_str(), NULL, 10);
  ret = GFactory::get_instance().get_rpc_stub().drop_tablets(memtable_frozen_version);
  return ret;
}

int manual_gc(const vector<string>& param)
{
  int32_t copy = 3;
  if (param.size() < 1)
  {
    fprintf(stderr, "reserve 3 copies");
  }
  else
  {
    copy = atoi(param[0].c_str());
    fprintf(stderr,"reserve %d copy(s)",copy);
  }
  int ret = GFactory::get_instance().get_rpc_stub().start_gc(copy);
  return ret;
}

enum StatsObjectType
{
  ServerStats = 1,
  ClusterStats = 2,
  AppStats = 3,
};

ObServerStats * create_stats_object(
    const StatsObjectType obj_type,
    ObClientRpcStub & rpc_stub,
    const int32_t server_type,
    const int32_t show_header,
    const char* table_filter_string,
    const char* index_filter_string,
    const char* config_file_name = NULL
    )
{
  ObServerStats* server_stats = NULL;
  int ret = 0;
  if (obj_type == ServerStats)
  {
    server_stats = new ObServerStats(rpc_stub, server_type);
  }
  else if (obj_type == ClusterStats)
  {
    server_stats = new ObClusterStats(rpc_stub, server_type);
  }
  else if (obj_type == AppStats)
  {
    server_stats = new ObAppStats(rpc_stub, server_type, config_file_name);
  }
  else
  {
    return NULL;
  }

  server_stats->set_show_header(show_header);

  int32_t table_id_size = 128;
  int32_t table_id_array[table_id_size];
  int32_t show_index_size = 128;
  int32_t show_index_array[show_index_size];
  if (NULL != table_filter_string) 
  {
    ret = parse_number_range(table_filter_string, table_id_array, table_id_size);
    if (OB_SUCCESS == ret && table_id_size > 0)
    {
      if (table_id_size > 1 || table_id_array[0] != 0)
      {
        for (int32_t i = 0 ; i < table_id_size; ++i)
        {
          server_stats->add_table_filter(table_id_array[i]);
        }
      }
    }
    else if (OB_SUCCESS != ret)
    {
      fprintf(stderr, "parse show table id(%s) failed\n", table_filter_string);
      return NULL;
    }
  }

  if (NULL != index_filter_string)
  {
    ret = parse_number_range(index_filter_string, show_index_array, show_index_size);
    if (OB_SUCCESS == ret && show_index_size > 0)
    {
      if (show_index_size > 1 || show_index_array[0] != 0)
      {
        for (int32_t i = 0 ; i < show_index_size; ++i)
        {
          server_stats->add_index_filter(show_index_array[i]);
        }
      }
    }
    else if (OB_SUCCESS != ret)
    {
      fprintf(stderr, "parse show index id(%s) failed\n", index_filter_string);
      return NULL;
    }
  }

  // special case for daily merge
  if (server_type == 5)
  {
    //printf("special case for daily merge, set table filter 0 \n");
    server_stats->clear_table_filter();
    // only get table 0
    server_stats->add_table_filter(0);
  }

  return server_stats;

}


int loop_dump(
    ObServerStats& server_stats,
    const int32_t interval,
    const int32_t run_count
    )
{
  int32_t count = 0;
  int ret = 0;
  while (true)
  {
    ret = server_stats.summary(count, interval);
    if (OB_SUCCESS != ret) 
    {
      fprintf(stderr, "summary error, ret=%d\n", ret);
      break;
    }
    ++count;
    if (run_count > 0 && count >= run_count) break;
    sleep(interval);
  }
  return ret;
}

int dump_stats_info(StatsObjectType objtype, const vector<string> &param)
{
  static const int32_t server_count = 6;
  static const char* server_name[server_count] = {"unknown", "rs", "cs", "ms", "ups", "dm"};
  int ret = 0;
  if (param.size() < 1) 
  {
    fprintf(stderr, "dump_stats_info: input param error\n");
    fprintf(stderr, "%s <server_type=rs/cs/ms/ups/dm> [interval=1] "
        "[count = 0] [show_header_count = 50] "
        "[table = 0/1001~1005/1001,1003] [show_index = 0/0~11/0,2,5]"
        "\n", objtype == ServerStats ? "dump_server_stats" : "dump_cluster_stats");
    return OB_ERROR;
  }

  const char* server_type_str = param[0].c_str();
  int32_t server_type = atoi(param[0].c_str());
  for (int32_t i = 0; i < server_count; ++i)
  {
    if (strcmp(server_type_str, server_name[i]) == 0)
    {
      server_type = i;
    }
  }

  //printf("str=%s,server_type=%d\n", server_type_str, server_type);

  int32_t interval = 1; 
  int32_t run_count = 0 ;
  int32_t show_header = 50;

  const char *table_filter_string = NULL;
  const char *index_filter_string = NULL;
  if (param.size() > 1) interval = atoi(param[1].c_str());
  if (param.size() > 2) run_count = atoi(param[2].c_str());
  if (param.size() > 3) show_header = atoi(param[3].c_str());
  if (param.size() > 4) table_filter_string = param[4].c_str();
  if (param.size() > 5) index_filter_string = param[5].c_str();

  ObClientRpcStub rpc_stub;
  ret = rpc_stub.initialize( 
      GFactory::get_instance().get_rpc_stub().get_remote_server(),    
      &GFactory::get_instance().get_base_client().get_client_manager());
  if (OB_SUCCESS != ret)
  {
    fprintf(stderr, "initialize rpc stub failed, ret = %d\n", ret);
    return ret;
  }


  ObServerStats* server_stats = create_stats_object(
      objtype,
      rpc_stub,
      server_type,
      show_header,
      table_filter_string,
      index_filter_string,
      g_config_file_name
      );

  if (NULL == server_stats) return ret;

  ret = loop_dump(
      *server_stats,
      interval,
      run_count
      );

  return ret;
}

int show_param(const vector<string> &param)
{
  int ret = 0;
  UNUSED(param);
  ret = GFactory::get_instance().get_rpc_stub().show_param();
  return ret;
}

int sync_all_tablet_images(const vector<string> &param)
{
  int ret = 0;
  UNUSED(param);
  ret = GFactory::get_instance().get_rpc_stub().sync_all_tablet_images();
  return ret;
}

int change_log_level(const vector<string> &param)
{
  int log_level = -1;
  if (param.size() != 1) {
    fprintf(stderr, "change_log_level: param error!\n");
    fprintf(stderr, "usage: \n\tchange_log_level ERROR|WARN|INFO|DEBUG\n");
    return OB_ERROR;
  }
  if (param[0].compare("ERROR") == 0)
    log_level = TBSYS_LOG_LEVEL_ERROR;
  else if (param[0].compare("WARN") == 0)
    log_level = TBSYS_LOG_LEVEL_WARN;
  else if (param[0].compare("INFO") == 0)
    log_level = TBSYS_LOG_LEVEL_INFO;
  else if (param[0].compare("DEBUG") == 0)
    log_level = TBSYS_LOG_LEVEL_DEBUG;
  else {
    fprintf(stderr, "change_log_level: param error!\n");
    fprintf(stderr, "usage: \n\tchange_log_level ERROR|WARN|INFO|DEBUG\n");
    return OB_ERROR;
  }
  return GFactory::get_instance().get_rpc_stub().change_log_level(log_level);
}

int do_work(const int cmd, const vector<string> &param)
{
  int ret = OB_SUCCESS;
  switch (cmd)
  {
    case CMD_DUMP_TABLET:
      ret = dump_tablet_image(param);
      break;
    case CMD_DUMP_SERVER_STATS:
      ret = dump_stats_info(ServerStats, param);
      break;
    case CMD_DUMP_CLUSTER_STATS:
      ret = dump_stats_info(ClusterStats, param);
      break;
    case CMD_DUMP_APP_STATS:
      ret = dump_stats_info(AppStats, param);
      break;
    case CMD_MANUAL_MERGE:
      ret = manual_merge(param);
      break;
    case CMD_MANUAL_DROP_TABLET:
      ret = manual_drop_tablet(param);
      break;
    case CMD_MANUAL_GC:
      ret = manual_gc(param);
      break;
    case CMD_RS_GET_TABLET_INFO:
      ret = get_tablet_info(param);
      break;
    case CMD_CHECK_MERGE_PROCESS:
      ret = check_merge_process(param);
      break;
    case CMD_SCAN_ROOT_TABLE:
      ret = scan_root_table(param);
      break;
    case CMD_MIGRATE_TABLET:
      ret = migrate_tablet(param);
      break;
    case CMD_CREATE_TABLET:
      ret = create_tablet(param);
      break;
    case CMD_DELETE_TABLET:
      ret = delete_tablet(param);
      break;
    case CMD_SHOW_PARAM:
      ret = show_param(param);
      break;
    case CMD_CHANGE_LOG_LEVEL:
      ret = change_log_level(param);
      break;
    case CMD_RESTART_SERVER:
      ret = GFactory::get_instance().get_rpc_stub().stop_server(true);
      break;
    case CMD_STOP_SERVER:
      ret = GFactory::get_instance().get_rpc_stub().stop_server();
      break;
    case CMD_SYNC_ALL_IMAGES:
      ret = sync_all_tablet_images(param);
      break;
    default:
      fprintf(stderr, "unsupported command : %d\n", cmd);
      ret = OB_ERROR;
      break;
  }
  return ret;
}

void usage()
{
  fprintf(stderr, "usage: ./cs_admin -s chunkserver_ip -p chunkserver_port -i \"command args\"\n"
          "command: \n\tdump_tablet_image dump_server_stats dump_cluster_stats dump_app_stats\n\t"
          "manual_merge manual_drop_tablet manual_gc get_tablet_info scan_root_table migrate_tablet\n\t"
          "check_merge_process change_log_level stop_server restart_server create_tablet delete_tablet\n\t"
          "sync_images\n\n"
          "run command for detail.\n");
  fprintf(stderr, "    command args: \n");
  fprintf(stderr, "\tdump_tablet_image disk_no \n");
  fprintf(stderr, "\tdump_tablet_image disk_no_1, disk_no_2, ... \n");
  fprintf(stderr, "\tdump_tablet_image disk_no_min~disk_no_max \n");
  fprintf(stderr, "\tdump_server_stats <server_type=rs/cs/ms/ups/dm> [interval=1] "
                  "[count = 0] [show_header_count = 50] "
                  "[table = 0/1001~1005/1001,1003] [show_index = 0/0~11/0,2,5]\n");
  fprintf(stderr, "\tdump_cluster_stats <server_type=rs/cs/ms/ups/dm> [interval=1] "
                  "[count = 0] [show_header_count = 50] "
                  "[table = 0/1001~1005/1001,1003] [show_index = 0/0~11/0,2,5]\n");
  fprintf(stderr, "\tmanual_merge memtable_frozen_version [init_flag=0]\n");
  fprintf(stderr, "\tmanual_drop_tablet memtable_frozen_version \n");
  fprintf(stderr, "\tget_tablet_info table_id table_name range_str\n");
  fprintf(stderr, "\tscan_root_table table_name\n");
  fprintf(stderr, "\tscan_root_table table_name table_id rowkey_split app_name "
                  "\t[output_dir = .] [key_delim_assic = 1] [column_delim_assic = ' '] \n"
                  "\t    rowkey_split: type1-length1,type2-length2...\n"
                  "\t    example: 3-8,2-4 means part1 is int64_t, 8 bytes\n"
                  "\t    the second part is int32_t, 4 bytes\n"
                  "\t    (type: int8_t 0, int16_t 1, int32_t 2, int64_t 3, varchar 4, \n"
                  "\t     datetime_1 5 YY-MM-DD HH:MM:SS \n"
                  "\t     datetime_2 6 YY-MM-DD \n"
                  "\t     datetime_3 7 YYMMDD HH:MM:SS \n"
                  "\t     datetime_4 8 YYMMDDHHMMSS \n"
                  "\t     datetime_5 9 YYMMDD)\n");
  fprintf(stderr, "\tmigrate_tablet table range range_format dest_server keep_src\n");
  fprintf(stderr, "\tcreate_tablet table range range_format last_frozen_version\n");
  fprintf(stderr, "\tdelete_tablet table range range_format tablet_version is_force\n"
                  "\t    range_format: 0 : plain string, 1 : hex format string 'FACB012D', 2 : int64_t number '1232'\n");
  fprintf(stderr, "\tcheck_merge_process disk_no \n");
  fprintf(stderr, "\tcheck_merge_process disk_no_1, disk_no_2, ... \n");
  fprintf(stderr, "\tcheck_merge_process disk_no_min~disk_no_max \n");
  fprintf(stderr, "\tshow_param show chunkserver param\n");
  fprintf(stderr, "\tsync_images sync all tablet images in chunkserver\n");
}

int main(const int argc, char *argv[])
{

  const char *addr = "127.0.0.1";
  const char *cmdstring = NULL;
  int32_t port = 2600;

  int ret = 0;
  int silent = 0;

  ObServer chunk_server;

  memset(g_config_file_name, OB_MAX_FILE_NAME_LENGTH, 0);


  while((ret = getopt(argc,argv,"s:p:i:f:q")) != -1)
  {
    switch(ret)
    {
      case 's':
        addr = optarg;
        break;
      case 'i':
        cmdstring = optarg;
        break;
      case 'p':
        port = atoi(optarg);
        break;
      case 'q':
        silent = 1;
        break;
      case 'f':
        strcpy(g_config_file_name, optarg);
        break;
      default:
        fprintf(stderr, "%s is not identified\n",optarg);
        usage();
        return ret;
        break;
    };
  }

  if (silent) TBSYS_LOGGER.setLogLevel("ERROR");

  ret = chunk_server.set_ipv4_addr(addr, port);
  if (!ret && strlen(g_config_file_name) <= 0)
  {
    fprintf(stderr, "chunkserver addr invalid, addr=%s, port=%d\n", addr, port);
    usage();
    return ret;
  }

  ret = GFactory::get_instance().initialize(chunk_server);
  if (OB_SUCCESS != ret)
  {
    fprintf(stderr, "initialize GFactory error, ret=%d\n", ret);
    return ret;
  }
  ret = GFactory::get_instance().start();
  if (OB_SUCCESS != ret)
  {
    fprintf(stderr, "start GFactory error, ret=%d\n", ret);
    return ret;
  }

  vector<string> param;
  int cmd = parse_command(cmdstring, param);
  if (CMD_MIN != cmd)
  {
    ret = do_work(cmd, param);
  }
  else
  {
    usage();
    return ret;
  }

  GFactory::get_instance().stop();
  GFactory::get_instance().wait();

  return ret == OB_SUCCESS ? 0 : -1;
}

