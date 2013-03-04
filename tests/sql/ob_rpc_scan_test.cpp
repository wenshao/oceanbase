#include <iostream>
#include <sstream>
#include <algorithm>
#include <tblog.h>
#include <gtest/gtest.h>

#include "common/ob_schema.h"
#include "common/ob_malloc.h"
#include "common/ob_scanner.h"
#include "common/ob_tablet_info.h"
#include  "mergeserver/ob_ms_schema_manager.h"
#include "mergeserver/ob_rs_rpc_proxy.h"
#include "mergeserver/ob_ms_rpc_proxy.h"
#include "mergeserver/ob_ms_tablet_location.h"
#include "mergeserver/ob_ms_tablet_location_proxy.h"
#include "mergeserver/ob_ms_rpc_stub.h"
#include "mergeserver/ob_ms_scan_event.h"
#include "mergeserver/ob_ms_async_rpc.h"

#include "mock_server.h"
#include "mock_root_server.h"
#include "mock_update_server.h"
#include "mock_chunk_server.h"

#include "sql/ob_rpc_scan.h"
#include "common/ob_row.h"
using namespace std;
using namespace oceanbase::common;
using namespace oceanbase::mergeserver;
using namespace oceanbase::sql;
using namespace oceanbase::sql::test;

const uint64_t timeout = 10000000;
const char * addr = "127.0.0.1";
/*
namespace oceanbase
{
  namespace sql
  {
    namespace test
    {
*/

class ExprGen{
  public:
    static ObSqlExpression & create_expr_by_id(uint64_t id)
    {
      oceanbase::sql::ObExpression expr;
      ExprItem item_a;
      item_a.type_ = ExprItem::T_REF_COLUMN;
      item_a.value_.cell_.tid = 123;
      item_a.value_.cell_.cid = id;
      expr.add_expr_item(item_a);
      sql_expr.set_expression(expr);
      sql_expr.set_tid_cid(123, id);
      return sql_expr;
    }

    static ObSqlExpression sql_expr;
    static ObStringBuf buf;
};
ObSqlExpression  ExprGen::sql_expr;
ObStringBuf ExprGen::buf;


int main(int argc, char **argv)
{
  ob_init_memory_pool();
  TBSYS_LOGGER.setLogLevel("DEBUG");
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}

class TestObRpcScan: public ::testing::Test
{
  public:
    virtual void SetUp()
    {
    }

    virtual void TearDown()
    {
    }
};
TEST_F(TestObRpcScan, test_single_table_scan)
{
  /// 1. setup transport
  /// 2. setup servers (root, ups, CSes)
  /// 3. setup scan request (location list, scan range)

  TBSYS_LOG(INFO, "( 1 )");
  tbnet::Transport * transport = new tbnet::Transport();
  tbnet::DefaultPacketStreamer * streamer = new tbnet::DefaultPacketStreamer();
  ObPacketFactory * factory = new ObPacketFactory();
  streamer->setPacketFactory(factory);
  ObClientManager client_manager;
  client_manager.initialize(transport, streamer);
  transport->start();



  TBSYS_LOG(INFO, "( 2 )");
  ObServer update_server;
  ObServer root_server;
  ObServer merge_server;
  update_server.set_ipv4_addr(addr, MockUpdateServer::UPDATE_SERVER_PORT);
  root_server.set_ipv4_addr(addr, MockRootServer::ROOT_SERVER_PORT);    
  ObMergerRpcProxy proxy(3, timeout, root_server, merge_server);

  ObMergerRpcStub stub;
  ThreadSpecificBuffer buffer;
  stub.init(&buffer, &client_manager);
  ObMergerRootRpcProxy rpc(0, timeout, root_server);
  EXPECT_TRUE(OB_SUCCESS == rpc.init(&stub));

  merge_server.set_ipv4_addr(addr, 10256);
  ObMergerTabletLocationCache * location = new ObMergerTabletLocationCache;
  location->init(50000 * 5, 1000, 100000);

  // init tablet cache 
  char temp[256] = ""; 
  char temp_end[256] = "";
  ObServer server;
  const uint64_t START_ROW = 100L;
  const uint64_t MAX_ROW = 200L; 
  ObMergerTabletLocationList list;
  for (uint64_t i = START_ROW; i <= MAX_ROW - 100; i += 100)
  {
    server.set_ipv4_addr(addr, MockChunkServer::CHUNK_SERVER_PORT);
    ObTabletLocation addr(i, server);

    EXPECT_TRUE(OB_SUCCESS == list.add(addr));
    //EXPECT_TRUE(OB_SUCCESS == list.add(addr));
    ///EXPECT_TRUE(OB_SUCCESS == list.add(addr));

    snprintf(temp, 100, "row_%lu", i);
    snprintf(temp_end, 100, "row_%lu", i + 100);
    ObString start_key(100, static_cast<int32_t>(strlen(temp)), temp);
    ObString end_key(100, static_cast<int32_t>(strlen(temp_end)), temp_end);

    ObRange range;
    range.table_id_ = 123;
    range.start_key_ = start_key;
    range.end_key_ = end_key;
    range.border_flag_.unset_inclusive_start();
    range.border_flag_.set_inclusive_end();
    list.set_tablet_range(range);
    list.set_timestamp(tbsys::CTimeUtil::getTime());
    EXPECT_TRUE(OB_SUCCESS == location->set(range, list));
  }

  TBSYS_LOG(INFO, "( 3 )");

  // start root server
  MockRootServer root;
  tbsys::CThread root_server_thread;
  MockServerRunner test_root_server(root);
  root_server_thread.start(&test_root_server, NULL);

  // start chunk server
  MockChunkServer chunk;
  tbsys::CThread chunk_server_thread;
  MockServerRunner test_chunk_server(chunk);
  chunk_server_thread.start(&test_chunk_server, NULL);
  sleep(2);

  /// (4) do it!
  ObMergerAsyncRpcStub async;
  async.init(&buffer, &client_manager);
  ObMergerLocationCacheProxy location_proxy(root_server, &rpc, location);
  ObRpcScan scan;
  scan.set_table(123);
  scan.add_output_column(ExprGen::create_expr_by_id(1));

  ASSERT_TRUE(OB_SUCCESS == scan.init(&location_proxy, &async));
  ASSERT_TRUE(OB_SUCCESS == scan.open());

  const ObRow * cur_row = NULL;
  while(1)
  {
    int ret = OB_SUCCESS;

    TBSYS_LOG(DEBUG, "scannnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnning - begin");
    if (OB_ITER_END == (ret = scan.get_next_row(cur_row)))
    {
      TBSYS_LOG(INFO, "scan.get_next_row ret iter_end");
      break;
    }
    else if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "get next row failed. ret=%d", ret);
      break;
    }
    TBSYS_LOG(DEBUG, "scannnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnning - end");
    /*TBSYS_LOG(DEBUG, "[id:%lu][key:%.*s][obj:dumped bellow]", cur_row->table_id_, cur_row->row_key_.length(), 
      cur_row->row_key_.ptr());
      cur_row->value_.dump();
      */
    if (NULL != cur_row)
    {
      cur_row->dump();
    }
    else
    {
      TBSYS_LOG(WARN, "no current row");
    }
    TBSYS_LOG(INFO, "( ====================while(1)===================)");
  }
  TBSYS_LOG(DEBUG, "scannnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnning - terminatet");
  //sleep(10);

  scan.close();

  TBSYS_LOG(INFO, "( request sent )");
  transport->stop();
  test_chunk_server.~MockServerRunner();
  chunk_server_thread.join();
  test_root_server.~MockServerRunner();
  root_server_thread.join();
}
/*
   };
   };
   };
   */
