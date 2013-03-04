/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 * 
 * Version: $Id$
 *
 * ob_delete_replicas_test.cpp
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#include "rootserver/ob_root_server2.h"
#include "common/ob_tablet_info.h"
#include "rootserver/ob_root_meta2.h"
#include "rootserver/ob_root_table2.h"
#include "rootserver/ob_root_worker.h"
#include "root_server_tester.h"
#include "rootserver/ob_root_rpc_stub.h"
#include "mock_root_rpc_stub.h"
#include <gtest/gtest.h>
#include <cassert>

using namespace oceanbase::common;
using namespace oceanbase::rootserver;
using namespace oceanbase;
using namespace oceanbase::rootserver::testing;
using ::testing::_;
using ::testing::Eq;
using ::testing::AtLeast;

class TestRootWorker: public ObRootWorkerForTest
{
  public:
    virtual ObRootRpcStub& get_rpc_stub() { return rpc_;}    
    MockObRootRpcStub rpc_;
};

class ObDeleteReplicasTest: public ::testing::Test
{
  public:
    virtual void SetUp();
    virtual void TearDown();
    void register_cs(int32_t cs_num);
    void heartbeat_cs(int32_t cs_num);
    ObServer &get_addr(int32_t idx);
  public:
    TestRootWorker worker_;
    ObRootServer2 *server_;
  protected:
    ObServer addr_;
};

void ObDeleteReplicasTest::SetUp()
{
  server_ = worker_.get_root_server();
  worker_.set_config_file_name("ob_delete_replicas_test.conf");
  ASSERT_EQ(OB_SUCCESS, worker_.initialize());
  worker_.get_role_manager()->set_role(ObRoleMgr::STANDALONE); // for testing

  server_->start_threads();
  sleep(5);
}

void ObDeleteReplicasTest::TearDown()
{
  server_->stop_threads();
}

ObServer &ObDeleteReplicasTest::get_addr(int32_t idx)
{
  char buff[128];
  snprintf(buff, 128, "10.10.10.%d", idx);
  int port = 26666;
  addr_.set_ipv4_addr(buff, port);
  return addr_;
}

void ObDeleteReplicasTest::register_cs(int32_t cs_num)
{
  for (int i = 0; i < cs_num; ++i)
  {
    int32_t status = 0;
    ASSERT_EQ(OB_SUCCESS, server_->regist_server(get_addr(i), false, status));
    TBSYS_LOG(INFO, "register cs, id=%d status=%d", i, status);
  }
}

void ObDeleteReplicasTest::heartbeat_cs(int32_t cs_num)
{
  for (int i = 0; i < cs_num; ++i)
  {
    ASSERT_EQ(OB_SUCCESS, server_->receive_hb(get_addr(i), OB_CHUNKSERVER));
  }
}


TEST_F(ObDeleteReplicasTest, delete_in_init)
{
  register_cs(4);
  char* startkey = (char*)"A";
  char* endkey = (char*)"Z";
  ObTabletReportInfo tablet;
  tablet.tablet_info_.range_.border_flag_.set_min_value();
  tablet.tablet_info_.range_.border_flag_.set_max_value();
  tablet.tablet_info_.range_.table_id_ = 10001;
  tablet.tablet_info_.range_.start_key_.assign_ptr(startkey, static_cast<int32_t>(strlen(startkey)));
  tablet.tablet_info_.range_.end_key_.assign_ptr(endkey, static_cast<int32_t>(strlen(endkey)));
  tablet.tablet_info_.row_count_ = 100;
  tablet.tablet_info_.occupy_size_ = 1024;
  tablet.tablet_info_.crc_sum_ = 3456;
  tablet.tablet_location_.tablet_version_ = 1;
  ObTabletReportInfoList tablet_list;
  for (int i = 0; i < 4; ++i)
  {
    tablet_list.reset();
    tablet.tablet_location_.chunkserver_ = get_addr(i);
    ASSERT_EQ(OB_SUCCESS, tablet_list.add_tablet(tablet));
    ASSERT_EQ(OB_SUCCESS, server_->report_tablets(get_addr(i), tablet_list, 1));
    if (3 == i)
    {
      EXPECT_CALL(worker_.rpc_, delete_tablets(get_addr(i),Eq(tablet_list),_))
        .Times(AtLeast(1));
    }
  }
  for (int i = 0; i < 8; ++i)
  {
    heartbeat_cs(4);
    sleep(1);
  }
}

TEST_F(ObDeleteReplicasTest, delete_when_rereplication)
{
  const int CS_NUM = 3;
  register_cs(CS_NUM);
  char* startkey = (char*)"A";
  char* endkey = (char*)"Z";
  ObTabletReportInfo tablet;
  tablet.tablet_info_.range_.border_flag_.set_min_value();
  tablet.tablet_info_.range_.border_flag_.set_max_value();
  tablet.tablet_info_.range_.table_id_ = 10001;
  tablet.tablet_info_.range_.start_key_.assign_ptr(startkey, static_cast<int32_t>(strlen(startkey)));
  tablet.tablet_info_.range_.end_key_.assign_ptr(endkey, static_cast<int32_t>(strlen(endkey)));
  tablet.tablet_info_.row_count_ = 100;
  tablet.tablet_info_.occupy_size_ = 1024;
  tablet.tablet_info_.crc_sum_ = 3456;
  tablet.tablet_location_.tablet_version_ = 1;
  ObTabletReportInfoList tablet_list;
  for (int i = 0; i < CS_NUM; ++i)
  {
    tablet_list.reset();
    tablet.tablet_location_.chunkserver_ = get_addr(i);
    ASSERT_EQ(OB_SUCCESS, tablet_list.add_tablet(tablet));
    ASSERT_EQ(OB_SUCCESS, server_->report_tablets(get_addr(i), tablet_list, 1));
  }
  for (int i = 0; i < 8; ++i)
  {
    heartbeat_cs(CS_NUM);
    sleep(1);
  }
  // case 2
  server_->config_.flag_tablet_replicas_num_.set(2);

  tablet.tablet_location_.chunkserver_ = get_addr(0);
  tablet_list.reset();
  ASSERT_EQ(OB_SUCCESS, tablet_list.add_tablet(tablet));
  EXPECT_CALL(worker_.rpc_, delete_tablets(get_addr(0),Eq(tablet_list),_))
    .Times(AtLeast(1));

  //worker_.get_mock_rpc_stub().set_delete_tablet(tablet);
  for (int i = 0; i < 70; ++i)  // wait balance worker
  {
    heartbeat_cs(CS_NUM);
    sleep(1);
  }
}

//TEST_F(ObDeleteReplicasTest, delete_when_report)
//{
//  register_cs(4);
//  char* startkey = (char*)"A";
//  char* endkey = (char*)"Z";
//  ObTabletReportInfo tablet;
//  tablet.tablet_info_.range_.border_flag_.set_min_value();
//  tablet.tablet_info_.range_.border_flag_.set_max_value();
//  tablet.tablet_info_.range_.table_id_ = 10001;
//  tablet.tablet_info_.range_.start_key_.assign_ptr(startkey, static_cast<int32_t>(strlen(startkey)));
//  tablet.tablet_info_.range_.end_key_.assign_ptr(endkey, static_cast<int32_t>(strlen(endkey)));
//  tablet.tablet_info_.row_count_ = 100;
//  tablet.tablet_info_.occupy_size_ = 1024;
//  tablet.tablet_info_.crc_sum_ = 3456;
//  tablet.tablet_location_.tablet_version_ = 1;
//  ObTabletReportInfoList tablet_list;
//  for (int i = 0; i < 3; ++i)
//  {
//    tablet_list.reset();
//    tablet.tablet_location_.chunkserver_ = get_addr(i);
//    ASSERT_EQ(OB_SUCCESS, tablet_list.add_tablet(tablet));
//    ASSERT_EQ(OB_SUCCESS, server_->report_tablets(get_addr(i), tablet_list, 1));
//  }
//  for (int i = 0; i < 8; ++i)
//  {
//    heartbeat_cs(4);
//    sleep(1);
//  }
//  tablet.tablet_location_.chunkserver_ = get_addr(0);
//  tablet_list.reset();
//  ASSERT_EQ(OB_SUCCESS, tablet_list.add_tablet(tablet));
//  EXPECT_CALL(worker_.rpc_, delete_tablets(get_addr(0),Eq(tablet_list),_))
//    .Times(AtLeast(1));
//
//  // new cs
//  tablet.tablet_location_.chunkserver_ = get_addr(3);
//  tablet.tablet_location_.tablet_version_ = 2;
//  tablet_list.reset();
//  ASSERT_EQ(OB_SUCCESS, tablet_list.add_tablet(tablet));
//  ASSERT_EQ(OB_SUCCESS, server_->report_tablets(get_addr(3), tablet_list, 1));
//  for (int i = 0; i < 8; ++i)
//  {
//    heartbeat_cs(4);
//    sleep(1);
//  }
//}
//
int main(int argc, char **argv)
{
  ob_init_memory_pool();
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
