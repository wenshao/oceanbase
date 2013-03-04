#include <gtest/gtest.h>
#include "wait_object.h"
#include "common/ob_packet.h"

using namespace oceanbase;
using namespace oceanbase::common;

TEST(WaitObject, base_test)
{
  WaitObjectManager wo_manager;

  // create a new wait object
  WaitObject* wo = wo_manager.create_wait_object();
  ASSERT_TRUE(wo != NULL);
  ASSERT_TRUE(wo->get_id() > 0);

  int32_t packet_code = 101;
  // wake up it
  ObPacket* result = new ObPacket();
  result->set_packet_code(packet_code);

  bool ret = wo_manager.wakeup_wait_object(wo->get_id(), result);
  ASSERT_TRUE(ret);

  // check response
  ObPacket* res = (ObPacket*)wo->get_response();

  ASSERT_TRUE(res != NULL);
  ASSERT_EQ(result->get_packet_code(), res->get_packet_code());

  // destory it
  wo_manager.destroy_wait_object(wo);
}

TEST(WaitObject, multi_test)
{
  WaitObjectManager wo_manager;

  WaitObject* wo = wo_manager.create_wait_object();
  ASSERT_TRUE(wo != NULL);
  ASSERT_TRUE(wo->get_id() > 0);

  int32_t pc1 = 101;
  ObPacket* res1 = new ObPacket();
  res1->set_packet_code(pc1);

  int32_t pc2 = 102;
  ObPacket* res2 = new ObPacket();
  res2->set_packet_code(pc2);

  bool ret = wo_manager.wakeup_wait_object(wo->get_id(), res1);
  ASSERT_TRUE(ret);
  
  ret = wo_manager.wakeup_wait_object(wo->get_id(), res2);
  ASSERT_TRUE(ret);

  // got control packet
  ret = wo_manager.wakeup_wait_object(wo->get_id(), &tbnet::ControlPacket::TimeoutPacket);
  ASSERT_TRUE(ret);

  ObPacket* res3 = NULL;
  ret = wo_manager.wakeup_wait_object(wo->get_id(), res3);
  ASSERT_TRUE(ret);

  ret = wo->wait(0);
  ASSERT_TRUE(ret);

  ObPacket* ret1 = (ObPacket*)wo->get_response();
  ASSERT_TRUE(ret1 != NULL);
  ASSERT_TRUE(ret1->get_packet_code() == pc1 || ret1->get_packet_code() == pc2);

  ret1 = (ObPacket*)wo->get_response();
  ASSERT_TRUE(ret1 != NULL);
  ASSERT_TRUE(ret1->get_packet_code() == pc1 || ret1->get_packet_code() == pc2);

  wo_manager.destroy_wait_object(wo);
}

TEST(WaitObject, WaitObjectNotExistTest)
{
  WaitObjectManager wo_manager;

  int64_t not_exist_wo_id = 100;
  bool ret = wo_manager.wakeup_wait_object(not_exist_wo_id, &tbnet::ControlPacket::TimeoutPacket);
  ASSERT_TRUE(ret);

  ObPacket* p1 = new ObPacket();
  ret = wo_manager.wakeup_wait_object(not_exist_wo_id, p1);
  ASSERT_TRUE(ret);
}

TEST(WaitObject, TimeOutTest)
{
  WaitObjectManager wo_manager;

  WaitObject* wo = wo_manager.create_wait_object();
  ASSERT_TRUE(wo != NULL);
  ASSERT_TRUE(wo->get_id() > 0);

  bool ret = wo->wait(1000000);
  ASSERT_FALSE(ret);

  wo_manager.destroy_wait_object(wo);
}

int main(int argc, char **argv)
{
  ob_init_memory_pool();
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
