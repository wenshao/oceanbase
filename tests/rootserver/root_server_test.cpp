#include <gtest/gtest.h>

#include "root_server_tester.h"
#include "rootserver/ob_root_server2.h"
#include "common/ob_tablet_info.h"
#include "rootserver/ob_root_meta2.h"
#include "rootserver/ob_root_table2.h"
using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::rootserver;
void print(const ObString& p)
{
  char str[1024];
  int i = 0;
  for (; i < p.length() && i < 1023; ++i)
  {
    str[i] = *(p.ptr() + i);
  }
  str[i]=0;
  TBSYS_LOG(INFO, "%s", str);
}
void create_root_table(ObRootServer2* root_server)
{
  char buf1[10][30];
  char buf2[10][30];
  ObServer server1(ObServer::IPV4, "10.10.10.1", 1001);
  ObServer server2(ObServer::IPV4, "10.10.10.2", 1001);
  ObServer server3(ObServer::IPV4, "10.10.10.3", 1001);
  ObServer server4(ObServer::IPV4, "10.10.10.4", 1001);

  ObTabletReportInfoList report_list1;
  ObTabletReportInfoList report_list2;
  ObTabletReportInfoList report_list3;
  ObTabletReportInfoList report_list4;
  ObTabletReportInfo report_info;
  ObTabletInfo info1;
  ObTabletLocation location;

  location.tablet_version_ = 1;
  info1.range_.table_id_ = 10001;
  info1.occupy_size_ = 2;

  //we will make this root table by report
  //aa1-ba1 1,3,4
  //ba1-ca1 1,2,3
  //ca1-da1 2,3,4
  //da1-ea1 1,3,4
  //ea1-fa1 1,2,3
  //fa1-    2,3,4
  //so  1: aa1-ba1 ba1-ca1 da1-ea1 ea1-fa1
  //    2: ba1-ca1 ca1-da1 ea1-fa1 fa1-
  //    3: aa1-ba1 ba1-ca1 ca1-da1 da1-ea1 ea1-fa1 fa1-
  //    4: aa1-ba1 ca1-da1 da1-ea1 fa1-

  info1.range_.border_flag_.set_inclusive_end();
  info1.range_.border_flag_.unset_inclusive_start();
  info1.range_.border_flag_.set_min_value();
  info1.range_.border_flag_.unset_max_value();
  info1.range_.start_key_.assign_buffer(buf1[0], 30);
  info1.range_.end_key_.assign_buffer(buf2[0], 30);
  info1.range_.start_key_.write("aa1", 3);
  info1.range_.end_key_.write("ba1", 3);
  location.chunkserver_ = server1;
  report_info.tablet_info_ = info1;
  report_info.tablet_location_ = location;
  report_list1.add_tablet(report_info);

  info1.range_.border_flag_.set_inclusive_end();
  info1.range_.border_flag_.unset_inclusive_start();
  info1.range_.border_flag_.unset_min_value();
  info1.range_.border_flag_.unset_max_value();
  info1.range_.start_key_.assign_buffer(buf1[1], 30);
  info1.range_.end_key_.assign_buffer(buf2[1], 30);
  info1.range_.start_key_.write("ba1", 3);
  info1.range_.end_key_.write("ca1", 3);
  location.chunkserver_ = server1;
  report_info.tablet_info_ = info1;
  report_info.tablet_location_ = location;
  report_list1.add_tablet(report_info);

  info1.range_.border_flag_.set_inclusive_end();
  info1.range_.border_flag_.unset_inclusive_start();
  info1.range_.border_flag_.unset_min_value();
  info1.range_.border_flag_.unset_max_value();
  info1.range_.start_key_.assign_buffer(buf1[2], 30);
  info1.range_.end_key_.assign_buffer(buf2[2], 30);
  info1.range_.start_key_.write("da1", 3);
  info1.range_.end_key_.write("ea1", 3);
  location.chunkserver_ = server1;
  report_info.tablet_info_ = info1;
  report_info.tablet_location_ = location;
  report_list1.add_tablet(report_info);

  info1.range_.border_flag_.set_inclusive_end();
  info1.range_.border_flag_.unset_inclusive_start();
  info1.range_.border_flag_.unset_min_value();
  info1.range_.border_flag_.unset_max_value();
  info1.range_.start_key_.assign_buffer(buf1[3], 30);
  info1.range_.end_key_.assign_buffer(buf2[3], 30);
  info1.range_.start_key_.write("ea1", 3);
  info1.range_.end_key_.write("fa1", 3);
  location.chunkserver_ = server1;
  report_info.tablet_info_ = info1;
  report_info.tablet_location_ = location;
  report_list1.add_tablet(report_info);

  root_server->report_tablets(server1, report_list1,0);
  //    2: ba1-ca1 ca1-da1 ea1-fa1 fa1-
  info1.range_.border_flag_.set_inclusive_end();
  info1.range_.border_flag_.unset_inclusive_start();
  info1.range_.border_flag_.unset_min_value();
  info1.range_.border_flag_.unset_max_value();
  info1.range_.start_key_.assign_buffer(buf1[0], 30);
  info1.range_.end_key_.assign_buffer(buf2[0], 30);
  info1.range_.start_key_.write("ba1", 3);
  info1.range_.end_key_.write("ca1", 3);
  location.chunkserver_ = server2;
  report_info.tablet_info_ = info1;
  report_info.tablet_location_ = location;
  report_list2.add_tablet(report_info);

  info1.range_.border_flag_.set_inclusive_end();
  info1.range_.border_flag_.unset_inclusive_start();
  info1.range_.border_flag_.unset_min_value();
  info1.range_.border_flag_.unset_max_value();
  info1.range_.start_key_.assign_buffer(buf1[1], 30);
  info1.range_.end_key_.assign_buffer(buf2[1], 30);
  info1.range_.start_key_.write("ca1", 3);
  info1.range_.end_key_.write("da1", 3);
  location.chunkserver_ = server2;
  report_info.tablet_info_ = info1;
  report_info.tablet_location_ = location;
  report_list2.add_tablet(report_info);

  info1.range_.border_flag_.set_inclusive_end();
  info1.range_.border_flag_.unset_inclusive_start();
  info1.range_.border_flag_.unset_min_value();
  info1.range_.border_flag_.unset_max_value();
  info1.range_.start_key_.assign_buffer(buf1[2], 30);
  info1.range_.end_key_.assign_buffer(buf2[2], 30);
  info1.range_.start_key_.write("ea1", 3);
  info1.range_.end_key_.write("fa1", 3);
  location.chunkserver_ = server2;
  report_info.tablet_info_ = info1;
  report_info.tablet_location_ = location;
  report_list2.add_tablet(report_info);

  info1.range_.border_flag_.set_inclusive_end();
  info1.range_.border_flag_.unset_inclusive_start();
  info1.range_.border_flag_.unset_min_value();
  info1.range_.border_flag_.set_max_value();
  info1.range_.start_key_.assign_buffer(buf1[3], 30);
  info1.range_.end_key_.assign_buffer(buf2[3], 30);
  info1.range_.start_key_.write("fa1", 3);
  info1.range_.end_key_.write("fa1", 3);
  location.chunkserver_ = server2;
  report_info.tablet_info_ = info1;
  report_info.tablet_location_ = location;
  report_list2.add_tablet(report_info);

  root_server->report_tablets(server2, report_list2,0);

  //    3: aa1-ba1 ba1-ca1 ca1-da1 da1-ea1 ea1-fa1 fa1-

  info1.range_.border_flag_.set_inclusive_end();
  info1.range_.border_flag_.unset_inclusive_start();
  info1.range_.border_flag_.set_min_value();
  info1.range_.border_flag_.unset_max_value();
  info1.range_.start_key_.assign_buffer(buf1[0], 30);
  info1.range_.end_key_.assign_buffer(buf2[0], 30);
  info1.range_.start_key_.write("aa1", 3);
  info1.range_.end_key_.write("ba1", 3);
  location.chunkserver_ = server3;
  report_info.tablet_info_ = info1;
  report_info.tablet_location_ = location;
  report_list3.add_tablet(report_info);

  info1.range_.border_flag_.set_inclusive_end();
  info1.range_.border_flag_.unset_inclusive_start();
  info1.range_.border_flag_.unset_min_value();
  info1.range_.border_flag_.unset_max_value();
  info1.range_.start_key_.assign_buffer(buf1[1], 30);
  info1.range_.end_key_.assign_buffer(buf2[1], 30);
  info1.range_.start_key_.write("ba1", 3);
  info1.range_.end_key_.write("ca1", 3);
  location.chunkserver_ = server3;
  report_info.tablet_info_ = info1;
  report_info.tablet_location_ = location;
  report_list3.add_tablet(report_info);

  info1.range_.border_flag_.set_inclusive_end();
  info1.range_.border_flag_.unset_inclusive_start();
  info1.range_.border_flag_.unset_min_value();
  info1.range_.border_flag_.unset_max_value();
  info1.range_.start_key_.assign_buffer(buf1[2], 30);
  info1.range_.end_key_.assign_buffer(buf2[2], 30);
  info1.range_.start_key_.write("ca1", 3);
  info1.range_.end_key_.write("da1", 3);
  location.chunkserver_ = server3;
  report_info.tablet_info_ = info1;
  report_info.tablet_location_ = location;
  report_list3.add_tablet(report_info);

  info1.range_.border_flag_.set_inclusive_end();
  info1.range_.border_flag_.unset_inclusive_start();
  info1.range_.border_flag_.unset_min_value();
  info1.range_.border_flag_.unset_max_value();
  info1.range_.start_key_.assign_buffer(buf1[3], 30);
  info1.range_.end_key_.assign_buffer(buf2[3], 30);
  info1.range_.start_key_.write("da1", 3);
  info1.range_.end_key_.write("ea1", 3);
  location.chunkserver_ = server3;
  report_info.tablet_info_ = info1;
  report_info.tablet_location_ = location;
  report_list3.add_tablet(report_info);

  info1.range_.border_flag_.set_inclusive_end();
  info1.range_.border_flag_.unset_inclusive_start();
  info1.range_.border_flag_.unset_min_value();
  info1.range_.border_flag_.unset_max_value();
  info1.range_.start_key_.assign_buffer(buf1[4], 30);
  info1.range_.end_key_.assign_buffer(buf2[4], 30);
  info1.range_.start_key_.write("ea1", 3);
  info1.range_.end_key_.write("fa1", 3);
  location.chunkserver_ = server3;
  report_info.tablet_info_ = info1;
  report_info.tablet_location_ = location;
  report_list3.add_tablet(report_info);

  info1.range_.border_flag_.set_inclusive_end();
  info1.range_.border_flag_.unset_inclusive_start();
  info1.range_.border_flag_.unset_min_value();
  info1.range_.border_flag_.set_max_value();
  info1.range_.start_key_.assign_buffer(buf1[5], 30);
  info1.range_.end_key_.assign_buffer(buf2[5], 30);
  info1.range_.start_key_.write("fa1", 3);
  info1.range_.end_key_.write("fa1", 3);
  location.chunkserver_ = server3;
  report_info.tablet_info_ = info1;
  report_info.tablet_location_ = location;
  report_list3.add_tablet(report_info);

  root_server->report_tablets(server3, report_list3,0);

  //    4: aa1-ba1 ca1-da1 da1-ea1 fa1-
  info1.range_.border_flag_.set_inclusive_end();
  info1.range_.border_flag_.unset_inclusive_start();
  info1.range_.border_flag_.set_min_value();
  info1.range_.border_flag_.unset_max_value();
  info1.range_.start_key_.assign_buffer(buf1[0], 30);
  info1.range_.end_key_.assign_buffer(buf2[0], 30);
  info1.range_.start_key_.write("aa1", 3);
  info1.range_.end_key_.write("ba1", 3);
  location.chunkserver_ = server4;
  report_info.tablet_info_ = info1;
  report_info.tablet_location_ = location;
  report_list4.add_tablet(report_info);

  info1.range_.border_flag_.set_inclusive_end();
  info1.range_.border_flag_.unset_inclusive_start();
  info1.range_.border_flag_.unset_min_value();
  info1.range_.border_flag_.unset_max_value();
  info1.range_.start_key_.assign_buffer(buf1[1], 30);
  info1.range_.end_key_.assign_buffer(buf2[1], 30);
  info1.range_.start_key_.write("ca1", 3);
  info1.range_.end_key_.write("da1", 3);
  location.chunkserver_ = server4;
  report_info.tablet_info_ = info1;
  report_info.tablet_location_ = location;
  report_list4.add_tablet(report_info);

  info1.range_.border_flag_.set_inclusive_end();
  info1.range_.border_flag_.unset_inclusive_start();
  info1.range_.border_flag_.unset_min_value();
  info1.range_.border_flag_.unset_max_value();
  info1.range_.start_key_.assign_buffer(buf1[2], 30);
  info1.range_.end_key_.assign_buffer(buf2[2], 30);
  info1.range_.start_key_.write("da1", 3);
  info1.range_.end_key_.write("ea1", 3);
  location.chunkserver_ = server4;
  report_info.tablet_info_ = info1;
  report_info.tablet_location_ = location;
  report_list4.add_tablet(report_info);

  info1.range_.border_flag_.set_inclusive_end();
  info1.range_.border_flag_.unset_inclusive_start();
  info1.range_.border_flag_.unset_min_value();
  info1.range_.border_flag_.set_max_value();
  info1.range_.start_key_.assign_buffer(buf1[3], 30);
  info1.range_.end_key_.assign_buffer(buf2[3], 30);
  info1.range_.start_key_.write("fa1", 3);
  info1.range_.end_key_.write("fa1", 3);
  location.chunkserver_ = server4;
  report_info.tablet_info_ = info1;
  report_info.tablet_location_ = location;
  report_list4.add_tablet(report_info);

  root_server->report_tablets(server4, report_list4,0);

}
//由于其他功能还不完善, 测试暂时不加
TEST(ObRootServer2Test2, regist_server)
{
  ObRootServer2* root_server = NULL;
  ObServer server(ObServer::IPV4, "10.10.10.1", 1001);
  ObRootWorkerForTest worker;
  worker.set_config_file_name("./root_server.conf");
  ASSERT_EQ(OB_SUCCESS, worker.initialize());
  //ASSERT_TRUE(root_server.init(100, &worker));
  root_server = worker.get_root_server();

  ObRootServerTester tester(root_server);
  tester.get_wait_init_time() = 2 * 1000000;
  root_server->start_threads();
  int status;
  sleep(3);
  
  int ret = root_server->regist_server(server,false, status);
  ASSERT_EQ(OB_SUCCESS, ret);
  
  ObChunkServerManager& server_manager = tester.get_server_manager();
  ObChunkServerManager::iterator it = server_manager.find_by_ip(server);
  ASSERT_TRUE(it != server_manager.end());
  //ASSERT_EQ(0, status);
  root_server->stop_threads();
}
TEST(ObRootServer2Test2, init_report)
{
  ObRootServer2* root_server;
  ObRootWorkerForTest worker;
  worker.set_config_file_name("./root_server.conf");
  ASSERT_EQ(OB_SUCCESS, worker.initialize());
  root_server = worker.get_root_server();
  //ASSERT_TRUE(root_server->init(100, &worker));
  ObRootServerTester tester(root_server);
  tester.get_wait_init_time() = 3 * 1000000;
  ObServer server(ObServer::IPV4, "10.10.10.1", 1001);
  ObServer server2(ObServer::IPV4, "10.10.10.2", 1001);
  ASSERT_TRUE(!(server == server2));
  int64_t time_stamp = 0;
  root_server->start_threads();
  int status;
  sleep(3);
  int ret = root_server->regist_server(server, false, status);
  ASSERT_EQ(ret, OB_SUCCESS);
  ret = root_server->regist_server(server2, false, status);
  ASSERT_EQ(ret, OB_SUCCESS);
  // now we have two cs 

  tester.get_lease_duration() = 100 * 1000 * 1000;
  TBSYS_LOG(INFO, "will start test");
  //tester.init_root_table_by_report();

  //while (worker.start_new_send_times < 2)
  //{
  //  TBSYS_LOG(INFO, "wait send start_new_schema now %d have sended",worker.start_new_send_times);
  //  sleep(1);
  //}
  TBSYS_LOG(INFO, "over send start_new_schema now %d have sended",worker.start_new_send_times);
  //all commond sended.
  //mimic report 
  char buf1[10][30];
  char buf2[10][30];
  //const common::ObTabletInfo& tablet, const common::ObTabletLocation& location)
  ObTabletReportInfoList report_list1;
  ObTabletReportInfoList report_list2;
  ObTabletReportInfo report_info;
  ObTabletInfo info1;
  ObTabletLocation location;
  location.tablet_version_ = 0;
  info1.range_.table_id_ = 10001;
  info1.range_.border_flag_.set_inclusive_end();
  info1.range_.border_flag_.unset_inclusive_start();
  info1.range_.border_flag_.set_min_value();
  info1.range_.border_flag_.unset_max_value();

  info1.range_.start_key_.assign_buffer(buf1[0], 30);
  info1.range_.end_key_.assign_buffer(buf2[0], 30);
  info1.range_.start_key_.write("aa1", 3);
  info1.range_.end_key_.write("ba1", 3);

  location.chunkserver_ = server;

  report_info.tablet_info_ = info1;
  report_info.tablet_location_ = location;
  report_list1.add_tablet(report_info);

  //ASSERT_EQ(0,root_server.got_reported(info1, location));  //(,"ba1"]  server

  info1.range_.border_flag_.unset_min_value();
  info1.range_.start_key_.assign_buffer(buf1[1], 30);
  info1.range_.end_key_.assign_buffer(buf2[1], 30);
  info1.range_.start_key_.write("ba1", 3);
  info1.range_.end_key_.write("ca1", 3);

  report_info.tablet_info_ = info1;
  report_info.tablet_location_ = location;
  report_list1.add_tablet(report_info);

  ret = root_server->report_tablets(server, report_list1,time_stamp);
  ASSERT_EQ(OB_SUCCESS, ret);
  
  location.chunkserver_ = server2;

  info1.range_.start_key_.assign_buffer(buf1[3], 30);
  info1.range_.end_key_.assign_buffer(buf2[3], 30);
  info1.range_.start_key_.write("da1", 3);
  info1.range_.end_key_.write("ea1", 3);

  report_info.tablet_info_ = info1;
  report_info.tablet_location_ = location;
  report_list2.add_tablet(report_info);

  info1.range_.start_key_.assign_buffer(buf1[2], 30);
  info1.range_.end_key_.assign_buffer(buf2[2], 30);
  info1.range_.start_key_.write("ca1", 3);
  info1.range_.end_key_.write("da1", 3);

  report_info.tablet_info_ = info1;
  report_info.tablet_location_ = location;
  report_list2.add_tablet(report_info);



  info1.range_.start_key_.assign_buffer(buf1[4], 30);
  info1.range_.end_key_.assign_buffer(buf2[4], 30);
  info1.range_.start_key_.write("ea1", 3);
  info1.range_.end_key_.write("fa1", 3);

  report_info.tablet_info_ = info1;
  report_info.tablet_location_ = location;
  report_list2.add_tablet(report_info);

  info1.range_.start_key_.assign_buffer(buf1[5], 30);
  info1.range_.end_key_.assign_buffer(buf2[5], 30);
  info1.range_.start_key_.write("fa1", 3);
  info1.range_.border_flag_.set_max_value();

  report_info.tablet_info_ = info1;
  report_info.tablet_location_ = location;
  report_list2.add_tablet(report_info);

  ret = root_server->report_tablets(server2, report_list2,time_stamp);
  ASSERT_EQ(OB_SUCCESS, ret);

  //mimic schema_changed
  //root_server->waiting_job_done(server,time_stamp );
  //root_server->waiting_job_done(server2,time_stamp );
  //wait unload send
  //while (worker.unload_old_table_times < 2)
  //{
  //  TBSYS_LOG(INFO, "wait send unload_old_table now %d have sended", worker.unload_old_table_times);
  //  sleep(1);
  //}
  sleep(5);
  {
    ObRootTable2 *& t_tb = tester.get_root_table_for_query();
    ObRootTable2::const_iterator start;
    ObRootTable2::const_iterator end;
    ObRootTable2::const_iterator ptr;

    ObString key;
    key.assign_buffer(buf1[0], 30);
    key.write("cb",2);
    //TBSYS_LOG(DEBUG,"key");
    //print(key);
    //t_tb->dump();

    t_tb->find_key(10001,key, 1, start, end, ptr);  // sholud hit ("ca1", "da1"] server2, so start will be the previous one
    {
      //should be ("ba1","ca1"]  server
      char buf5[30];
      ObString test;
      test.assign_buffer(buf5, 30);
      test.write("ba1",3);
      info1.range_.start_key_.assign_buffer(buf1[0], 30);
      info1.range_.end_key_.assign_buffer(buf2[0], 30);
      info1.range_.start_key_.write("ca1", 3);
      info1.range_.end_key_.write("da1", 3);
      info1.range_.border_flag_.set_inclusive_end();
      info1.range_.border_flag_.unset_inclusive_start();
      info1.range_.border_flag_.unset_max_value();
      info1.range_.border_flag_.unset_min_value();
      start++;
      const common::ObTabletInfo* tablet_info = NULL;        
      tablet_info = ((const ObRootTable2*)t_tb)->get_tablet_info(start);
      ASSERT_TRUE(tablet_info != NULL);
      tablet_info->range_.hex_dump();
      info1.range_.hex_dump();
      ASSERT_TRUE(tablet_info->range_.equal(info1.range_));

    }
  }
  root_server->stop_threads();
}
TEST(ObRootServer2Test2, update_capacity_info)
{
  ObRootServer2* root_server;
  ObRootWorkerForTest worker;
  root_server = worker.get_root_server();
  worker.set_config_file_name("./root_server.conf");
  ASSERT_EQ(OB_SUCCESS, worker.initialize());
  //ASSERT_TRUE(root_server->init(100, &worker));
  ObRootServerTester tester(root_server);
  ObServer server(ObServer::IPV4, "10.10.10.1", 1001);
  ObServer server2(ObServer::IPV4, "10.10.10.2", 1001);
  ASSERT_TRUE(!(server == server2));
  tester.get_wait_init_time() = 2 * 1000000;
  root_server->start_threads();
  int status;
  sleep(3);
  int ret = root_server->regist_server(server, false, status);
  ASSERT_EQ(OB_SUCCESS, ret);
  root_server->regist_server(server2, false, status);
  ASSERT_EQ(OB_SUCCESS, ret);
  // now we have two cs 

  tester.get_lease_duration() = 100 * 1000 * 1000;
  ASSERT_EQ(OB_SUCCESS,root_server->update_capacity_info(server, 50, 10));
  ObChunkServerManager& server_manager = tester.get_server_manager();
  ObServerStatus* it = server_manager.find_by_ip(server);
  ASSERT_TRUE(it != NULL);
  ASSERT_EQ(20, it->disk_info_.get_percent());
  root_server->stop_threads();
}
//migrate_over
TEST(ObRootServer2Test2, migrate_over)
{
  // init make a root table
  ObRootServer2* root_server;
  ObRootWorkerForTest worker;
  root_server = worker.get_root_server();
  int64_t now = tbsys::CTimeUtil::getTime();
  worker.set_config_file_name("./root_server.conf");
  ASSERT_EQ(OB_SUCCESS, worker.initialize());
  //ASSERT_TRUE(root_server->init(100, &worker));
  ObRootServerTester tester(root_server);
  ObServer server(ObServer::IPV4, "10.10.10.1", 1001);
  ObServer server2(ObServer::IPV4, "10.10.10.2", 1001);
  ObServer server3(ObServer::IPV4, "10.10.10.3", 1001);
  ObServer server4(ObServer::IPV4, "10.10.10.4", 1001);
  ASSERT_TRUE(!(server == server2));
  int64_t time_stamp = now;
  root_server->start_threads();
  
  int status;
  tester.get_wait_init_time() = 2 * 1000000;
  sleep(2);
  int ret = root_server->regist_server(server, false, status);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = root_server->regist_server(server2, false, status);
  ASSERT_EQ(OB_SUCCESS, ret);
  // now we have two cs 

  tester.get_lease_duration() = 100 * 1000 * 1000;
  TBSYS_LOG(INFO, "will start test");
  //tester.init_root_table_by_report();

  TBSYS_LOG(INFO, "over send start_new_schema now %d have sended",worker.start_new_send_times);
  time_stamp = 0;
  //all commond sended.
  //mimic report 
  char buf1[10][30];
  char buf2[10][30];
  //const common::ObTabletInfo& tablet, const common::ObTabletLocation& location)
  ObTabletReportInfoList report_list1;
  ObTabletReportInfoList report_list2;
  ObTabletReportInfo report_info;
  ObTabletInfo info1;
  ObTabletLocation location;
  location.tablet_version_ = 1;
  info1.range_.table_id_ = 10001;
  info1.range_.border_flag_.set_inclusive_end();
  info1.range_.border_flag_.unset_inclusive_start();
  info1.range_.border_flag_.set_min_value();
  info1.range_.border_flag_.unset_max_value();

  info1.range_.start_key_.assign_buffer(buf1[0], 30);
  info1.range_.end_key_.assign_buffer(buf2[0], 30);
  info1.range_.start_key_.write("aa1", 3);
  info1.range_.end_key_.write("ba1", 3);

  location.chunkserver_ = server;

  report_info.tablet_info_ = info1;
  report_info.tablet_location_ = location;
  report_list1.add_tablet(report_info);

  //ASSERT_EQ(0,root_server.got_reported(info1, location));  //(,"ba1"]  server

  info1.range_.border_flag_.unset_min_value();
  info1.range_.start_key_.assign_buffer(buf1[1], 30);
  info1.range_.end_key_.assign_buffer(buf2[1], 30);
  info1.range_.start_key_.write("ba1", 3);
  info1.range_.end_key_.write("ca1", 3);

  report_info.tablet_info_ = info1;
  report_info.tablet_location_ = location;
  report_list1.add_tablet(report_info);

  ret = root_server->report_tablets(server, report_list1,time_stamp);
  ASSERT_EQ(OB_SUCCESS, ret);
  
  location.chunkserver_ = server2;

  info1.range_.start_key_.assign_buffer(buf1[3], 30);
  info1.range_.end_key_.assign_buffer(buf2[3], 30);
  info1.range_.start_key_.write("da1", 3);
  info1.range_.end_key_.write("ea1", 3);

  report_info.tablet_info_ = info1;
  report_info.tablet_location_ = location;
  report_list2.add_tablet(report_info);

  info1.range_.start_key_.assign_buffer(buf1[2], 30);
  info1.range_.end_key_.assign_buffer(buf2[2], 30);
  info1.range_.start_key_.write("ca1", 3);
  info1.range_.end_key_.write("da1", 3);

  report_info.tablet_info_ = info1;
  report_info.tablet_location_ = location;
  report_list2.add_tablet(report_info);



  info1.range_.start_key_.assign_buffer(buf1[4], 30);
  info1.range_.end_key_.assign_buffer(buf2[4], 30);
  info1.range_.start_key_.write("ea1", 3);
  info1.range_.end_key_.write("fa1", 3);

  report_info.tablet_info_ = info1;
  report_info.tablet_location_ = location;
  report_list2.add_tablet(report_info);

  info1.range_.start_key_.assign_buffer(buf1[5], 30);
  info1.range_.end_key_.assign_buffer(buf2[5], 30);
  info1.range_.start_key_.write("fa1", 3);
  info1.range_.border_flag_.set_max_value();

  report_info.tablet_info_ = info1;
  report_info.tablet_location_ = location;
  report_list2.add_tablet(report_info);

  ret = root_server->report_tablets(server2, report_list2,time_stamp);
  ASSERT_EQ(OB_SUCCESS, ret);
  //wait unload send
  //while (worker.unload_old_table_times < 2)
  //{
  //  TBSYS_LOG(INFO, "wait send unload_old_table now %d have sended", worker.unload_old_table_times);
  //  sleep(1);
  //}
  ObRootTable2 *& t_tb = tester.get_root_table_for_query();
  ObRootTable2::const_iterator start;
  ObRootTable2::const_iterator end;
  ObRootTable2::const_iterator ptr;
  sleep(3);
  {

    ObString key;
    key.assign_buffer(buf1[0], 30);
    key.write("cb",2);
    //TBSYS_LOG(DEBUG,"key");
    //print(key);
    //t_tb->dump();

    t_tb->find_key(10001,key, 1, start, end, ptr);  // sholud hit ("ca1", "da1"] server2, so start will be the previous one
    {
      //should be ("ba1","ca1"]  server
      char buf5[30];
      ObString test;
      test.assign_buffer(buf5, 30);
      test.write("ba1",3);
      info1.range_.start_key_.assign_buffer(buf1[0], 30);
      info1.range_.end_key_.assign_buffer(buf2[0], 30);
      info1.range_.start_key_.write("ca1", 3);
      info1.range_.end_key_.write("da1", 3);
      info1.range_.border_flag_.set_inclusive_end();
      info1.range_.border_flag_.unset_inclusive_start();
      info1.range_.border_flag_.unset_max_value();
      info1.range_.border_flag_.unset_min_value();
      start++;
      const common::ObTabletInfo* tablet_info = NULL;        
      tablet_info = ((const ObRootTable2*)t_tb)->get_tablet_info(start);
      ASSERT_TRUE(tablet_info != NULL);
      tablet_info->range_.hex_dump();
      info1.range_.hex_dump();
      ASSERT_TRUE(tablet_info->range_.equal(info1.range_));

    }
  }
  root_server->regist_server(server3, false, status);
  info1.range_.table_id_ = 10001;
  info1.range_.border_flag_.set_inclusive_end();
  info1.range_.border_flag_.unset_inclusive_start();
  info1.range_.border_flag_.unset_min_value();
  info1.range_.border_flag_.unset_max_value();

  info1.range_.start_key_.assign_buffer(buf1[2], 30);
  info1.range_.end_key_.assign_buffer(buf2[2], 30);
  info1.range_.start_key_.write("ca1", 3);
  info1.range_.end_key_.write("da1", 3);

  ObChunkServerManager& server_manager = tester.get_server_manager();
  ObChunkServerManager::iterator  pos = server_manager.find_by_ip(server3);
  int server3_index = static_cast<int32_t>(pos - server_manager.begin());
  pos = server_manager.find_by_ip(server2);
  int server2_index = static_cast<int32_t>(pos - server_manager.begin());

  root_server->migrate_over(info1.range_, server2, server3, true, 1);

  t_tb->find_range(info1.range_, start, end);
  bool found_server3 =false;
  bool found_server2 =false;
  TBSYS_LOG(INFO, "check migrate result s2=%d s3=%d", server2_index, server3_index);
  ObRootTable2::const_iterator it = start;
  for (int i = 0; i < OB_SAFE_COPY_COUNT; i++)
  {
    if (server3_index == it->server_info_indexes_[i])
    {
      found_server3 = true;
    }
    if (server2_index == it->server_info_indexes_[i])
    {
      found_server2 = true;
    }
  }
  ASSERT_TRUE(found_server2);
  ASSERT_TRUE(found_server3);
  root_server->regist_server(server4, false, status);
  pos = server_manager.find_by_ip(server4);
  int server4_index = static_cast<int32_t>(pos - server_manager.begin());
  root_server->migrate_over(info1.range_, server2, server4, false, 1);
  TBSYS_LOG(INFO, "check migrate 2->4 ");

  found_server2 = false;
  found_server3 = false;
  bool found_server4 =false;
  it = start;
  for (int i = 0; i < OB_SAFE_COPY_COUNT; i++)
  {
    if (server3_index == it->server_info_indexes_[i])
    {
      found_server3 = true;
    }
    if (server2_index == it->server_info_indexes_[i])
    {
      found_server2 = true;
    }
    if (server4_index == it->server_info_indexes_[i])
    {
      found_server4 = true;
    }
  }
  ASSERT_FALSE(found_server2);
  ASSERT_TRUE(found_server3);
  ASSERT_TRUE(found_server4);
  root_server->stop_threads();
}
TEST(ObRootServer2Test2, cs_stop_start_data_keep)
{
  ObRootServer2* root_server;
  ObRootWorkerForTest worker;
  root_server = worker.get_root_server();
  worker.set_config_file_name("./root_server.conf");
  ASSERT_EQ(OB_SUCCESS, worker.initialize());
  //ASSERT_TRUE(root_server->init(100, &worker));
  ObRootServerTester tester(root_server);
  ObServer server1(ObServer::IPV4, "10.10.10.1", 1001);
  ObServer server2(ObServer::IPV4, "10.10.10.2", 1001);
  ObServer server3(ObServer::IPV4, "10.10.10.3", 1001);
  ObServer server4(ObServer::IPV4, "10.10.10.4", 1001);

  int status;
  tester.get_wait_init_time() = 2 * 1000000;
  sleep(1);
  root_server->regist_server(server1, false, status);
  root_server->regist_server(server2, false, status);
  root_server->regist_server(server3, false, status);
  root_server->regist_server(server4, false, status);
  // now we have two cs 

  tester.get_lease_duration() = 100 * 1000 * 1000;
  TBSYS_LOG(INFO, "will start test");
  //tester.init_root_table_by_report();

  //all commond sended.
  //mimic report 
  create_root_table(root_server);
  //mimic schema_changed
  sleep(2);
  ObRootTable2* rt_q = tester.get_root_table_for_query();
  rt_q->dump();
  rt_q->server_off_line(2, 20);
  rt_q->dump();
  //cs start again
  TBSYS_LOG(INFO," ------------------- server start again -------------------");
  root_server->regist_server(server3, false, status);
  char buf1[10][30];
  char buf2[10][30];
  ObTabletReportInfoList report_list1;
  ObTabletReportInfoList report_list2;
  ObTabletReportInfoList report_list3;
  ObTabletReportInfoList report_list4;
  ObTabletReportInfo report_info;
  ObTabletInfo info1;
  ObTabletLocation location;

  location.tablet_version_ = 2;
  info1.range_.table_id_ = 10001;
  //    3: aa1-ba1 ba1-ca1 ca1-da1 da1-ea1 ea1-fa1 fa1-
  info1.range_.border_flag_.set_inclusive_end();
  info1.range_.border_flag_.unset_inclusive_start();
  info1.range_.border_flag_.set_min_value();
  info1.range_.border_flag_.unset_max_value();
  info1.range_.start_key_.assign_buffer(buf1[0], 30);
  info1.range_.end_key_.assign_buffer(buf2[0], 30);
  info1.range_.start_key_.write("aa1", 3);
  info1.range_.end_key_.write("ba1", 3);
  location.chunkserver_ = server3;
  report_info.tablet_info_ = info1;
  report_info.tablet_location_ = location;
  report_list3.add_tablet(report_info);

  info1.range_.border_flag_.set_inclusive_end();
  info1.range_.border_flag_.unset_inclusive_start();
  info1.range_.border_flag_.unset_min_value();
  info1.range_.border_flag_.unset_max_value();
  info1.range_.start_key_.assign_buffer(buf1[1], 30);
  info1.range_.end_key_.assign_buffer(buf2[1], 30);
  info1.range_.start_key_.write("ba1", 3);
  info1.range_.end_key_.write("ca1", 3);
  location.chunkserver_ = server3;
  report_info.tablet_info_ = info1;
  report_info.tablet_location_ = location;
  report_list3.add_tablet(report_info);

  info1.range_.border_flag_.set_inclusive_end();
  info1.range_.border_flag_.unset_inclusive_start();
  info1.range_.border_flag_.unset_min_value();
  info1.range_.border_flag_.unset_max_value();
  info1.range_.start_key_.assign_buffer(buf1[2], 30);
  info1.range_.end_key_.assign_buffer(buf2[2], 30);
  info1.range_.start_key_.write("ca1", 3);
  info1.range_.end_key_.write("da1", 3);
  location.chunkserver_ = server3;
  report_info.tablet_info_ = info1;
  report_info.tablet_location_ = location;
  report_list3.add_tablet(report_info);

  info1.range_.border_flag_.set_inclusive_end();
  info1.range_.border_flag_.unset_inclusive_start();
  info1.range_.border_flag_.unset_min_value();
  info1.range_.border_flag_.unset_max_value();
  info1.range_.start_key_.assign_buffer(buf1[3], 30);
  info1.range_.end_key_.assign_buffer(buf2[3], 30);
  info1.range_.start_key_.write("da1", 3);
  info1.range_.end_key_.write("ea1", 3);
  location.chunkserver_ = server3;
  report_info.tablet_info_ = info1;
  report_info.tablet_location_ = location;
  report_list3.add_tablet(report_info);

  info1.range_.border_flag_.set_inclusive_end();
  info1.range_.border_flag_.unset_inclusive_start();
  info1.range_.border_flag_.unset_min_value();
  info1.range_.border_flag_.unset_max_value();
  info1.range_.start_key_.assign_buffer(buf1[4], 30);
  info1.range_.end_key_.assign_buffer(buf2[4], 30);
  info1.range_.start_key_.write("ea1", 3);
  info1.range_.end_key_.write("fa1", 3);
  location.chunkserver_ = server3;
  report_info.tablet_info_ = info1;
  report_info.tablet_location_ = location;
  report_list3.add_tablet(report_info);

  info1.range_.border_flag_.set_inclusive_end();
  info1.range_.border_flag_.unset_inclusive_start();
  info1.range_.border_flag_.unset_min_value();
  info1.range_.border_flag_.set_max_value();
  info1.range_.start_key_.assign_buffer(buf1[5], 30);
  info1.range_.end_key_.assign_buffer(buf2[5], 30);
  info1.range_.start_key_.write("fa1", 3);
  info1.range_.end_key_.write("fa1", 3);
  location.chunkserver_ = server3;
  report_info.tablet_info_ = info1;
  report_info.tablet_location_ = location;
  report_list3.add_tablet(report_info);
  root_server->report_tablets(server3, report_list3,0);
  sleep(1);
  rt_q->dump();
  root_server->stop_threads();
}

// bugfix http://bugfree.corp.taobao.com/Bug.php?BugID=113521
struct MigrateTestEnv
{
  ObRootWorkerForTest worker_;
  ObServer cs1_;
  ObServer cs2_;
  ObTabletInfo info1_;
  char buf1[10][30];
  char buf2[10][30];
  static const int64_t tablet_version_ = 2;

  MigrateTestEnv();
  void setup();
};

MigrateTestEnv::MigrateTestEnv()
  :cs1_(ObServer::IPV4, "10.10.10.1", 1001),
   cs2_(ObServer::IPV4, "10.10.10.2", 1002)
{
}

void MigrateTestEnv::setup()
{
  // 1. init
  ObRootServer2* rs = worker_.get_root_server();
  worker_.set_config_file_name("./root_server.conf");
  ASSERT_EQ(OB_SUCCESS, worker_.initialize());
  //ASSERT_TRUE(rs->init(100, &worker_));

  rs->start_threads();
  sleep(1);
  
  // 2. cs register
  int status;
  ASSERT_EQ(OB_SUCCESS, rs->regist_server(cs1_, false, status));
  ASSERT_EQ(OB_SUCCESS, rs->regist_server(cs2_, false, status));

  // 3. cs1 report tablets replicas
  ObTabletReportInfoList report_list1;
  ObTabletReportInfoList report_list2;
  ObTabletReportInfo report_info;
  ObTabletLocation location;
  location.tablet_version_ = tablet_version_;
  info1_.range_.table_id_ = 10001;
  info1_.range_.border_flag_.set_inclusive_end();
  info1_.range_.border_flag_.unset_inclusive_start();
  info1_.range_.border_flag_.set_min_value();
  info1_.range_.border_flag_.unset_max_value();

  info1_.range_.start_key_.assign_buffer(buf1[0], 30);
  info1_.range_.end_key_.assign_buffer(buf2[0], 30);
  info1_.range_.start_key_.write("aa1", 3);
  info1_.range_.end_key_.write("ba1", 3);

  location.chunkserver_ = cs1_;

  report_info.tablet_info_ = info1_;
  report_info.tablet_location_ = location;
  report_list1.add_tablet(report_info);

  info1_.range_.border_flag_.unset_min_value();
  info1_.range_.border_flag_.set_max_value();
  info1_.range_.start_key_.assign_buffer(buf1[1], 30);
  info1_.range_.end_key_.assign_buffer(buf2[1], 30);
  info1_.range_.start_key_.write("ba1", 3);
  info1_.range_.end_key_.write("ca1", 3);

  report_info.tablet_info_ = info1_;
  report_info.tablet_location_ = location;
  report_list1.add_tablet(report_info);

  int64_t now = tbsys::CTimeUtil::getTime();
  ASSERT_EQ(OB_SUCCESS, rs->report_tablets(cs1_, report_list1, now));
  ASSERT_EQ(OB_SUCCESS, rs->report_tablets(cs2_, report_list2, now));

  // wait init finish
  sleep(5);
}

TEST(ObRootServer2Test2, migrate_over2_1)
{
  MigrateTestEnv env;
  env.setup();
  
  ObRootServer2* rs = env.worker_.get_root_server();
  ObRootServerTester tester(rs);
  ObChunkServerManager& csmgr = tester.get_server_manager();
  ObRootTable2* roottable = tester.get_root_table_for_query();

  /// case 1
  // 4. target cs2 down  
  ObChunkServerManager::iterator it = csmgr.find_by_ip(env.cs2_);
  ASSERT_TRUE(csmgr.end() != it);
  csmgr.set_server_down(it);
  int64_t now = tbsys::CTimeUtil::getTime();
  roottable->server_off_line(static_cast<int32_t>(it - csmgr.begin()), now);
  
  // 5. report migrate over 
  rs->migrate_over(env.info1_.range_, env.cs1_, env.cs2_, true, env.tablet_version_);

  // 6. verify
  ObRootTable2::const_iterator it1, it2;
  ASSERT_EQ(OB_SUCCESS, roottable->find_range(env.info1_.range_, it1, it2));
  ASSERT_EQ(it1, it2);
  ASSERT_EQ(0, it1->server_info_indexes_[0]);
  ASSERT_EQ(OB_INVALID_INDEX, it1->server_info_indexes_[1]);
  ASSERT_EQ(OB_INVALID_INDEX, it1->server_info_indexes_[2]);
  rs->stop_threads();
}

TEST(ObRootServer2Test2, migrate_over2_2)
{
  MigrateTestEnv env;
  env.setup();
  
  ObRootServer2* rs = env.worker_.get_root_server();
  ObRootServerTester tester(rs);
  ObChunkServerManager& csmgr = tester.get_server_manager();
  ObRootTable2* roottable = tester.get_root_table_for_query();

  /// case 2
  // 4. target cs2 down  
  ObChunkServerManager::iterator it = csmgr.find_by_ip(env.cs2_);
  ASSERT_TRUE(csmgr.end() != it);
  csmgr.set_server_down(it);
  int64_t now = tbsys::CTimeUtil::getTime();
  roottable->server_off_line(static_cast<int32_t>(it - csmgr.begin()), now);
  
  // 5. report migrate over 
  rs->migrate_over(env.info1_.range_, env.cs1_, env.cs2_, false, env.tablet_version_);
  ObRootTable2::const_iterator it1, it2;
  ASSERT_EQ(OB_SUCCESS, roottable->find_range(env.info1_.range_, it1, it2));
  ASSERT_EQ(it1, it2);  
  ASSERT_EQ(OB_INVALID_INDEX, it1->server_info_indexes_[0]);
  ASSERT_EQ(OB_INVALID_INDEX, it1->server_info_indexes_[1]);
  ASSERT_EQ(OB_INVALID_INDEX, it1->server_info_indexes_[2]);
  rs->stop_threads();
}

TEST(ObRootServer2Test2, migrate_over2_3)
{
  MigrateTestEnv env;
  env.setup();
  
  ObRootServer2* rs = env.worker_.get_root_server();
  ObRootServerTester tester(rs);
  ObChunkServerManager& csmgr = tester.get_server_manager();
  ObRootTable2* roottable = tester.get_root_table_for_query();

  /// case 3
  // 4. source cs1 down  
  ObChunkServerManager::iterator it = csmgr.find_by_ip(env.cs1_);
  ASSERT_TRUE(csmgr.end() != it);
  csmgr.set_server_down(it);
  int64_t now = tbsys::CTimeUtil::getTime();
  roottable->server_off_line(static_cast<int32_t>(it - csmgr.begin()), now);
  
  // 5. report migrate over 
  rs->migrate_over(env.info1_.range_, env.cs1_, env.cs2_, true, env.tablet_version_);
  ObRootTable2::const_iterator it1, it2;
  ASSERT_EQ(OB_SUCCESS, roottable->find_range(env.info1_.range_, it1, it2));
  ASSERT_EQ(it1, it2);  
  ASSERT_EQ(1, it1->server_info_indexes_[0]);
  ASSERT_EQ(OB_INVALID_INDEX, it1->server_info_indexes_[1]);
  ASSERT_EQ(OB_INVALID_INDEX, it1->server_info_indexes_[2]);
  rs->stop_threads();
}

TEST(ObRootServer2Test2, migrate_over2_4)
{
  MigrateTestEnv env;
  env.setup();
  
  ObRootServer2* rs = env.worker_.get_root_server();
  ObRootServerTester tester(rs);
  ObChunkServerManager& csmgr = tester.get_server_manager();
  ObRootTable2* roottable = tester.get_root_table_for_query();

  /// case 3
  // 4. source cs1 down  
  ObChunkServerManager::iterator it = csmgr.find_by_ip(env.cs1_);
  ASSERT_TRUE(csmgr.end() != it);
  csmgr.set_server_down(it);
  int64_t now = tbsys::CTimeUtil::getTime();
  roottable->server_off_line(static_cast<int32_t>(it - csmgr.begin()), now);
  
  // 5. report migrate over 
  rs->migrate_over(env.info1_.range_, env.cs1_, env.cs2_, false, env.tablet_version_);
  ObRootTable2::const_iterator it1, it2;
  ASSERT_EQ(OB_SUCCESS, roottable->find_range(env.info1_.range_, it1, it2));
  ASSERT_EQ(it1, it2);  
  ASSERT_EQ(1, it1->server_info_indexes_[0]);
  ASSERT_EQ(OB_INVALID_INDEX, it1->server_info_indexes_[1]);
  ASSERT_EQ(OB_INVALID_INDEX, it1->server_info_indexes_[2]);
  rs->stop_threads();
}

int main(int argc, char **argv)
{
  ob_init_memory_pool();
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
