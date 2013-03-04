#include <iostream>
#include <sstream>
#include <algorithm>
#include <tblog.h>
#include <gtest/gtest.h>

#include "common/ob_schema.h"
#include "common/ob_malloc.h"
#include "ob_ms_cache_table.h"

using namespace std;
using namespace oceanbase::common;
using namespace oceanbase::mergeserver;

int main(int argc, char **argv)
{
  ob_init_memory_pool();
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}

class TestCacheTable: public ::testing::Test
{
  public:
    virtual void SetUp()
    {
    }

    virtual void TearDown()
    {
    }
};

TEST_F(TestCacheTable, test_compare)
{
  char temp[100];
  char temp_end[100];
  ObRange range;
  sprintf(temp, "%d", 1001);
  sprintf(temp_end, "%d", 100100);
  ObString start_key(100, static_cast<int32_t>(strlen(temp)), temp);
  ObString end_key(100, static_cast<int32_t>(strlen(temp_end)), temp_end);
  range.start_key_ = start_key;
  range.end_key_ = end_key;
  range.table_id_ = 100;

  ObCBtreeTable<int, int>::MapKey key;
  ObString rowkey;
  // bad case bug
  char * cmp_key = (char*)"100";
  rowkey.assign(cmp_key, static_cast<int32_t>(strlen(cmp_key)));
  ObBorderFlag row_key_flag;
  int ret = key.compare_range_with_key(100, rowkey, row_key_flag, ObCBtreeTable<int, int>::OB_SEARCH_MODE_EQUAL, range);
  // bug fix EXPECT_TRUE(ret == 0);
  EXPECT_TRUE(ret < 0);
  
  // good case 
  cmp_key = (char*)"10010";
  rowkey.assign(cmp_key, static_cast<int32_t>(strlen(cmp_key)));
  ret = key.compare_range_with_key(100, rowkey, row_key_flag, ObCBtreeTable<int, int>::OB_SEARCH_MODE_EQUAL, range);
  EXPECT_TRUE(ret == 0);

  cmp_key = (char*)"101";
  rowkey.assign(cmp_key, static_cast<int32_t>(strlen(cmp_key)));
  ret = key.compare_range_with_key(100, rowkey, row_key_flag, ObCBtreeTable<int, int>::OB_SEARCH_MODE_EQUAL, range);
  EXPECT_TRUE(ret > 0);
  
  cmp_key = (char*)"1001002";
  rowkey.assign(cmp_key, static_cast<int32_t>(strlen(cmp_key)));
  ret = key.compare_range_with_key(100, rowkey, row_key_flag, ObCBtreeTable<int, int>::OB_SEARCH_MODE_EQUAL, range);
  EXPECT_TRUE(ret > 0);
  
  // bad case bug
  cmp_key = (char*)"10010002";
  rowkey.assign(cmp_key, static_cast<int32_t>(strlen(cmp_key)));
  ret = key.compare_range_with_key(100, rowkey, row_key_flag, ObCBtreeTable<int, int>::OB_SEARCH_MODE_EQUAL, range);
  // buf fix EXPECT_TRUE(ret == 0);
  EXPECT_TRUE(ret > 0);
}


