#include <gtest/gtest.h>
#include <stdlib.h>
#include <time.h>
#include <vector>
#include <string>
#include <stdlib.h>
#include <time.h>
#include <iostream>
#include <fstream>
#include <stdio.h>
#include "common/ob_cell_array.h"
#include "common/ob_define.h"
#include "common/ob_cache.h"
#include "common/ob_string.h"
#include "common/ob_action_flag.h"
#include "common/ob_groupby.h"
#include "mergeserver/ob_groupby_operator.h"
#include "../updateserver/mock_client.h"
#include "../updateserver/test_utils.h"
using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::mergeserver;
using namespace testing;
using namespace std;

void init_mock_client(const char *addr, int32_t port, MockClient &client)
{
  ObServer dst_host;
  dst_host.set_ipv4_addr(addr, port);
  client.init(dst_host);
}
static const int64_t TIMEOUT =  50000000L;
static const int32_t ms_port = 52800;
static const int32_t ups_port = 52700;
static const int64_t left_rowkey_val = 0;
static const int32_t right_rowkey_val = 0;
static const char *  server_addr = "127.0.0.1";
static const char *  left_table_name = "collect_info";
static const char *  right_table_name = "collect_item";
MockClient g_ups_client;
MockClient g_ms_client;


TEST(Merge, row_not_exist)
{
  ObStringBuf buffer;
  EXPECT_EQ(g_ups_client.clear_active_memtable(TIMEOUT),OB_SUCCESS);
  ObScanParam scan_param;
  ObRange scan_range;
  ObVersionRange version_range;
  version_range.border_flag_.set_min_value();
  version_range.border_flag_.set_max_value();
  version_range.border_flag_.set_inclusive_start();
  version_range.border_flag_.set_inclusive_end();

  ObString str;
  str.assign((char*)&left_rowkey_val, sizeof(left_rowkey_val));
  scan_range.start_key_ = str;
  scan_range.end_key_ = str;
  scan_range.border_flag_.unset_min_value();
  scan_range.border_flag_.unset_max_value();
  scan_range.border_flag_.set_inclusive_start();
  scan_range.border_flag_.set_inclusive_end();

  /// scan empty table
  str.assign(const_cast<char*>(left_table_name),strlen(left_table_name));
  ObString left_table_name_str;
  EXPECT_EQ(buffer.write_string(str,&left_table_name_str), OB_SUCCESS);
  scan_param.set(OB_INVALID_ID,left_table_name_str,scan_range);
  scan_param.set_version_range(version_range);

  str.assign(const_cast<char*>("info_user_nick"),strlen("info_user_nick"));
  ObString column_name;
  EXPECT_EQ(buffer.write_string(str,&column_name), OB_SUCCESS);
  EXPECT_EQ(scan_param.add_column(column_name),OB_SUCCESS);
  ObScanner result;
  EXPECT_EQ(g_ms_client.ups_scan(scan_param,result, TIMEOUT), 0);
  EXPECT_TRUE(result.is_empty());

  /// get empty row
  ObGetParam get_param;
  int64_t key = 0;
  str.assign((char*)&key,sizeof(key));
  ObString left_row_key;
  EXPECT_EQ(buffer.write_string(str,&left_row_key), OB_SUCCESS);
  ObCellInfo cell;
  cell.row_key_ = left_row_key;
  cell.table_name_ = left_table_name_str;
  cell.column_name_ = column_name;
  get_param.set_version_range(version_range);
  EXPECT_EQ(get_param.add_cell(cell),OB_SUCCESS);
  EXPECT_EQ(g_ms_client.ups_get(get_param,result,TIMEOUT),OB_SUCCESS);
  int64_t cell_num = 0;
  ObCellInfo *cur_cell = NULL;
  int err = OB_SUCCESS;
  while ((err = result.next_cell()) == OB_SUCCESS)
  {
    EXPECT_EQ(result.get_cell(&cur_cell),OB_SUCCESS);
    EXPECT_TRUE(cur_cell->value_.get_ext() == ObActionFlag::OP_ROW_DOES_NOT_EXIST);
    cell_num ++;
  }
  EXPECT_EQ(err, OB_ITER_END);
  EXPECT_EQ(cell_num, 1);

  /// scan exist row, not exist cell
  ObMutator mutator;
  str.assign(const_cast<char*>("info_is_shared"),strlen("info_is_shared"));
  EXPECT_EQ(buffer.write_string(str,&column_name), OB_SUCCESS);
  ObObj obj;
  obj.set_int(5);
  EXPECT_EQ(mutator.update(left_table_name_str,left_row_key,column_name,obj), OB_SUCCESS);
  EXPECT_EQ(g_ups_client.ups_apply(mutator,TIMEOUT),OB_SUCCESS);

  EXPECT_EQ(g_ms_client.ups_scan(scan_param,result, TIMEOUT), 0);
  cell_num = 0;
  while ((err = result.next_cell()) == OB_SUCCESS)
  {
    EXPECT_EQ(result.get_cell(&cur_cell),OB_SUCCESS);
    EXPECT_TRUE(cur_cell->value_.get_type() == ObNullType);
    cell_num ++;
  }
  EXPECT_EQ(err, OB_ITER_END);
  EXPECT_EQ(cell_num, 1);

  /// get exist row, not exist cell
  EXPECT_EQ(g_ms_client.ups_get(get_param,result,TIMEOUT),OB_SUCCESS);
  cell_num = 0;
  while ((err = result.next_cell()) == OB_SUCCESS)
  {
    EXPECT_EQ(result.get_cell(&cur_cell),OB_SUCCESS);
    EXPECT_TRUE(cur_cell->value_.get_type()== ObNullType);
    cell_num ++;
  }
  EXPECT_EQ(err, OB_ITER_END);
  EXPECT_EQ(cell_num, 1);

  /// delete row
  EXPECT_EQ(mutator.reset(), OB_SUCCESS);
  EXPECT_EQ(mutator.del_row(left_table_name_str,left_row_key), OB_SUCCESS);
  EXPECT_EQ(g_ups_client.ups_apply(mutator,TIMEOUT),OB_SUCCESS);

  /// scan deleted row
  EXPECT_EQ(g_ms_client.ups_scan(scan_param,result, TIMEOUT), 0);
  EXPECT_TRUE(result.is_empty());

  /// get deleted row
  EXPECT_EQ(g_ms_client.ups_get(get_param,result,TIMEOUT),OB_SUCCESS);
  cell_num = 0;
  err = OB_SUCCESS;
  while ((err = result.next_cell()) == OB_SUCCESS)
  {
    EXPECT_EQ(result.get_cell(&cur_cell),OB_SUCCESS);
    EXPECT_TRUE(cur_cell->value_.get_ext() == ObActionFlag::OP_ROW_DOES_NOT_EXIST);
    cell_num ++;
  }
  EXPECT_EQ(err, OB_ITER_END);
  EXPECT_EQ(cell_num, 1);

  /// update right table
  str.assign(const_cast<char*>(right_table_name),strlen(right_table_name));
  ObString right_table_name_str;
  EXPECT_EQ(buffer.write_string(str,&right_table_name_str), OB_SUCCESS); 
  str.assign(const_cast<char*>("item_price"),strlen("item_price"));
  EXPECT_EQ(buffer.write_string(str,&column_name), OB_SUCCESS);
  EXPECT_EQ(mutator.reset(),OB_SUCCESS);
  ObString right_row_key;
  right_row_key.assign((char*)&right_rowkey_val,sizeof(right_rowkey_val));
  EXPECT_EQ(mutator.update(right_table_name_str,right_row_key,column_name, obj),OB_SUCCESS);
  EXPECT_EQ(g_ups_client.ups_apply(mutator,TIMEOUT),OB_SUCCESS);

  /// scan delete row's join cell
  EXPECT_EQ(scan_param.add_column(column_name),OB_SUCCESS); 
  EXPECT_EQ(g_ms_client.ups_scan(scan_param,result, TIMEOUT), 0);
  EXPECT_TRUE(result.is_empty());

  /// get delete row's join cell
  cell.column_name_ = column_name;
  EXPECT_EQ(get_param.add_cell(cell), OB_SUCCESS);
  EXPECT_EQ(g_ms_client.ups_get(get_param,result,TIMEOUT),OB_SUCCESS);
  cell_num = 0;
  err = OB_SUCCESS;
  while ((err = result.next_cell()) == OB_SUCCESS)
  {
    EXPECT_EQ(result.get_cell(&cur_cell),OB_SUCCESS);
    EXPECT_TRUE(cur_cell->value_.get_ext() == ObActionFlag::OP_ROW_DOES_NOT_EXIST);
    cell_num ++;
  }
  EXPECT_EQ(err, OB_ITER_END);
  EXPECT_GT(cell_num ,0 );
}

TEST(GroupBy, all_in_one_group)
{
  EXPECT_EQ(g_ups_client.clear_active_memtable(TIMEOUT),OB_SUCCESS);
  ObScanParam scan_param;
  ObStringBuf buffer;
  ObString str;
  ObString table_name;
  ObString count_all_column_name;
  ObString column_name;
  ObString as_column_name;
  str.assign(const_cast<char*>("*"),strlen("*"));
  EXPECT_EQ(buffer.write_string(str,&count_all_column_name), OB_SUCCESS);
  str.assign(const_cast<char*>(left_table_name),strlen(left_table_name));
  EXPECT_EQ(buffer.write_string(str, &table_name), OB_SUCCESS);
  ObString min_row_key;
  str.assign(const_cast<char*>("\0"),1);
  EXPECT_EQ(buffer.write_string(str, &min_row_key), OB_SUCCESS);
  ObRange range;
  /// range.start_key_ = min_row_key;
  range.border_flag_.unset_inclusive_start();
  range.border_flag_.set_inclusive_end();
  /// range.border_flag_.unset_min_value();
  range.border_flag_.set_min_value();
  range.border_flag_.set_max_value();
  scan_param.set(OB_INVALID_ID,table_name,range);
  str.assign(const_cast<char*>("item_price"), strlen("item_price"));
  EXPECT_EQ(buffer.write_string(str,&column_name),OB_SUCCESS);

  EXPECT_EQ(scan_param.add_column(column_name),OB_SUCCESS);

  str.assign(const_cast<char*>("count"), strlen("count"));
  EXPECT_EQ(buffer.write_string(str,&as_column_name),OB_SUCCESS);
  EXPECT_EQ(scan_param.get_group_by_param().add_aggregate_column(column_name,as_column_name,COUNT), OB_SUCCESS);


  str.assign(const_cast<char*>("sum"), strlen("sum"));
  EXPECT_EQ(buffer.write_string(str,&as_column_name),OB_SUCCESS);
  EXPECT_EQ(scan_param.get_group_by_param().add_aggregate_column(column_name,as_column_name,SUM), OB_SUCCESS);

  str.assign(const_cast<char*>("max"), strlen("max"));
  EXPECT_EQ(buffer.write_string(str,&as_column_name),OB_SUCCESS);
  EXPECT_EQ(scan_param.get_group_by_param().add_aggregate_column(column_name,as_column_name,MAX), OB_SUCCESS);

  str.assign(const_cast<char*>("min"), strlen("min"));
  EXPECT_EQ(buffer.write_string(str,&as_column_name),OB_SUCCESS);
  EXPECT_EQ(scan_param.get_group_by_param().add_aggregate_column(column_name,as_column_name,MIN), OB_SUCCESS);

  ObScanner result;
  EXPECT_EQ(g_ms_client.ups_scan(scan_param,result,TIMEOUT),OB_SUCCESS);

  ObCellInfo *cur_cell = NULL;
  int64_t intval = 0;
  /// count
  EXPECT_EQ(result.next_cell(),OB_SUCCESS);
  EXPECT_EQ(result.get_cell(&cur_cell),OB_SUCCESS);
  EXPECT_EQ(cur_cell->value_.get_int(intval),OB_SUCCESS);
  EXPECT_EQ(intval, 0);

  /// count
  EXPECT_EQ(result.next_cell(),OB_SUCCESS);
  EXPECT_EQ(result.get_cell(&cur_cell),OB_SUCCESS);
  EXPECT_EQ(cur_cell->value_.get_int(intval),OB_SUCCESS);
  EXPECT_EQ(intval, 0);

  /// max
  EXPECT_EQ(result.next_cell(),OB_SUCCESS);
  EXPECT_EQ(result.get_cell(&cur_cell),OB_SUCCESS);
  EXPECT_EQ(cur_cell->value_.get_type(),ObNullType);

  /// min
  EXPECT_EQ(result.next_cell(),OB_SUCCESS);
  EXPECT_EQ(result.get_cell(&cur_cell),OB_SUCCESS);
  EXPECT_EQ(cur_cell->value_.get_type(),ObNullType);

  EXPECT_EQ(result.next_cell(),OB_ITER_END);
}

TEST(select_all, select_all_and_filter)
{
  EXPECT_EQ(g_ups_client.clear_active_memtable(TIMEOUT),OB_SUCCESS);
  ObScanParam scan_param;
  ObStringBuf buffer;
  ObString str;
  ObString table_name;
  ObString count_all_column_name;
  ObString column_name;
  ObString as_column_name;
  str.assign(const_cast<char*>("*"),strlen("*"));
  EXPECT_EQ(buffer.write_string(str,&count_all_column_name), OB_SUCCESS);
  str.assign(const_cast<char*>(left_table_name),strlen(left_table_name));
  EXPECT_EQ(buffer.write_string(str, &table_name), OB_SUCCESS);
  ObString min_row_key;
  str.assign(const_cast<char*>("\0"),1);
  EXPECT_EQ(buffer.write_string(str, &min_row_key), OB_SUCCESS);
  ObRange range;
  /// range.start_key_ = min_row_key;
  range.border_flag_.unset_inclusive_start();
  range.border_flag_.set_inclusive_end();
  /// range.border_flag_.unset_min_value();
  range.border_flag_.set_min_value();
  range.border_flag_.set_max_value();
  scan_param.set(OB_INVALID_ID,table_name,range);
  str.assign(const_cast<char*>("item_price"), strlen("item_price"));
  EXPECT_EQ(buffer.write_string(str,&column_name),OB_SUCCESS);
  ObObj cond_obj;
  cond_obj.set_int(4);
  EXPECT_EQ(scan_param.add_where_cond(column_name,GT,cond_obj), OB_SUCCESS);
  ObScanner result;
  EXPECT_EQ(g_ms_client.ups_scan(scan_param,result,TIMEOUT),OB_SUCCESS);
}

TEST(CreateTime, ModifyTime)
{
  ObString str;
  ObStringBuf buffer;
  str.assign(const_cast<char*>(left_table_name),strlen(left_table_name));
  ObString left_table_name_str;
  EXPECT_EQ(buffer.write_string(str,&left_table_name_str), OB_SUCCESS);

  str.assign(const_cast<char*>(right_table_name),strlen(right_table_name));
  ObString right_table_name_str;
  EXPECT_EQ(buffer.write_string(str,&right_table_name_str), OB_SUCCESS);

  struct left_key_t
  {
    uint32_t left_prefix_;
    uint32_t right_key_;
  };
  left_key_t left_key;
  left_key.left_prefix_ = 1;
  left_key.right_key_ = 2;

  ObString left_key_str;
  left_key_str.assign((char*)&left_key, sizeof(left_key));
  ObString right_key_str;
  right_key_str.assign((char*)&left_key.right_key_,sizeof(left_key.right_key_));

  EXPECT_EQ(g_ups_client.clear_active_memtable(TIMEOUT),OB_SUCCESS);

  ObString column_name;
  ObMutator mutator;
  ObObj obj;
  str.assign(const_cast<char*>("info_is_shared"),strlen("info_is_shared"));
  EXPECT_EQ(buffer.write_string(str,&column_name), OB_SUCCESS);
  obj.set_int(5);
  EXPECT_EQ(mutator.insert(left_table_name_str,left_key_str,column_name,obj), OB_SUCCESS);

  str.assign(const_cast<char*>("item_picurl"),strlen("item_picurl"));
  EXPECT_EQ(buffer.write_string(str,&column_name), OB_SUCCESS);
  obj.set_int(5);
  EXPECT_EQ(mutator.insert(right_table_name_str,right_key_str,column_name,obj), OB_SUCCESS);

  EXPECT_EQ(g_ups_client.ups_apply(mutator,TIMEOUT),OB_SUCCESS);

  ObScanParam scan_param;
  ObRange range;
  /// range.start_key_ = min_row_key;
  range.border_flag_.unset_inclusive_start();
  range.border_flag_.set_inclusive_end();
  /// range.border_flag_.unset_min_value();
  range.border_flag_.set_min_value();
  range.border_flag_.set_max_value();
  scan_param.set(OB_INVALID_ID,left_table_name_str,range);
  ObVersionRange version_range;
  version_range.border_flag_.set_min_value();
  version_range.border_flag_.set_max_value();
  version_range.border_flag_.set_inclusive_start();
  version_range.border_flag_.set_inclusive_end();
  scan_param.set_version_range(version_range);

  str.assign("item_create",strlen("item_create"));
  EXPECT_EQ(buffer.write_string(str,&column_name),OB_SUCCESS);
  EXPECT_EQ(scan_param.add_column(column_name),OB_SUCCESS);

  str.assign("item_modify",strlen("item_modify"));
  EXPECT_EQ(buffer.write_string(str,&column_name),OB_SUCCESS);
  EXPECT_EQ(scan_param.add_column(column_name),OB_SUCCESS);

  ObScanner result;
  ObCellInfo *cur_cell = NULL;
  EXPECT_EQ(g_ms_client.ups_scan(scan_param,result, TIMEOUT),OB_SUCCESS);
  int64_t first_create_time;
  int64_t first_modify_time;
  EXPECT_EQ(result.next_cell(), OB_SUCCESS);
  EXPECT_EQ(result.get_cell(&cur_cell),OB_SUCCESS);
  EXPECT_EQ(cur_cell->value_.get_precise_datetime(first_create_time),OB_SUCCESS);

  EXPECT_EQ(result.next_cell(), OB_SUCCESS);
  EXPECT_EQ(result.get_cell(&cur_cell),OB_SUCCESS);
  EXPECT_EQ(cur_cell->value_.get_precise_datetime(first_modify_time),OB_SUCCESS);

  EXPECT_EQ(result.next_cell(), OB_ITER_END);

  string line;
  fprintf(stderr, "do a minor freeze\n");
  getline(cin,line);
  EXPECT_EQ(g_ups_client.ups_apply(mutator,TIMEOUT),OB_SUCCESS);
  EXPECT_EQ(g_ms_client.ups_scan(scan_param,result, TIMEOUT),OB_SUCCESS);

  int64_t second_create_time;
  int64_t second_modify_time;
  EXPECT_EQ(result.next_cell(), OB_SUCCESS);
  EXPECT_EQ(result.get_cell(&cur_cell),OB_SUCCESS);
  EXPECT_EQ(cur_cell->value_.get_precise_datetime(second_create_time),OB_SUCCESS);

  EXPECT_EQ(result.next_cell(), OB_SUCCESS);
  EXPECT_EQ(result.get_cell(&cur_cell),OB_SUCCESS);
  EXPECT_EQ(cur_cell->value_.get_precise_datetime(second_modify_time),OB_SUCCESS);

  EXPECT_EQ(result.next_cell(), OB_ITER_END);

  EXPECT_LT(first_modify_time, second_modify_time);
  EXPECT_EQ(first_create_time, second_create_time);
}

int main(int argc, char **argv)
{
  if (argc > 1)
  {
    srandom(time(NULL));
    ob_init_memory_pool(64*1024);
    InitGoogleTest(&argc, argv);
    init_mock_client(server_addr,ups_port,g_ups_client);
    init_mock_client(server_addr,ms_port,g_ms_client);
    return RUN_ALL_TESTS();
  } 
  else
  {
    return 0;
  }
}
