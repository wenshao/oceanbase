#include <tblog.h>
#include <gtest/gtest.h>

#include "common/ob_range.h"
#include "common/ob_scan_param.h"
#include "ob_read_param_modifier.h"

using namespace oceanbase::common;
using namespace oceanbase::mergeserver;

int main(int argc, char **argv)
{
  ob_init_memory_pool();
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}

class TestReadParamModifier: public ::testing::Test
{
  public:
    virtual void SetUp()
    {
    }
    
    static bool inner_check_finish(const ObScanParam & param, const ObRange & result_range)
    {
      bool ret = false;
      if (0 == param.get_scan_direction())
      {
        if (result_range.border_flag_.is_max_value()
            || ((!param.get_range()->border_flag_.is_max_value())
              && (result_range.end_key_ >= param.get_range()->end_key_)))
        {
          ret = true;
        }
      }
      else
      {
        if (result_range.border_flag_.is_min_value()
            || ((!param.get_range()->border_flag_.is_min_value())
              && ((param.get_range()->border_flag_.inclusive_start() 
                  && result_range.start_key_ < param.get_range()->start_key_)
                || (!param.get_range()->border_flag_.inclusive_start() 
                  && result_range.start_key_ <= param.get_range()->start_key_))
              )
           )
        {
          ret = true;
        }
      }
      return ret;
    }

    virtual void TearDown()
    {
    }
};


#define BLOCK_FUNC() if (true) \


// request fullfilled
TEST_F(TestReadParamModifier, test_check_finish)
{
  ObScanParam param;
  param.set_scan_direction(ObScanParam::FORWARD);
  char *key1 = (char*)"cdef";
  ObString sk1(0, 4, key1);
  uint64_t table_id = 110;
  ObString table_name(0, 4, key1);
  ObRange result;
  result.start_key_ = sk1;
  result.end_key_ = sk1;
  result.border_flag_.unset_inclusive_start();
  result.border_flag_.set_inclusive_end();

  // normal sequence
  BLOCK_FUNC()
  {
    // lt
    ObRange request;
    char *key2 = (char*)"adef";
    ObString sk2(0, 4, key2);
    request.end_key_ = sk2;

    // inclusive
    request.border_flag_.set_inclusive_end();
    param.set(table_id, table_name, request);
    EXPECT_TRUE(is_finish_scan(param.get_scan_direction(), *param.get_range(), result) == true);
    EXPECT_TRUE(TestReadParamModifier::inner_check_finish(param, result) == true);

    // not inclusive
    request.border_flag_.unset_inclusive_end();
    param.set(table_id, table_name, request);
    EXPECT_TRUE(is_finish_scan(param.get_scan_direction(), *param.get_range(), result) == true);
    EXPECT_TRUE(TestReadParamModifier::inner_check_finish(param, result) == true);
  }

  // normal sequence
  BLOCK_FUNC()
  {
    // eq
    ObRange request;
    char *key2 = (char*)"cdef";
    ObString sk2(0, 4, key2);
    request.end_key_ = sk2;

    // inclusive
    request.border_flag_.set_inclusive_end();
    param.set(table_id, table_name, request);
    EXPECT_TRUE(is_finish_scan(param.get_scan_direction(), *param.get_range(), result) == true);
    EXPECT_TRUE(TestReadParamModifier::inner_check_finish(param, result) == true);

    // not inclusive
    request.border_flag_.unset_inclusive_end();
    param.set(table_id, table_name, request);
    EXPECT_TRUE(is_finish_scan(param.get_scan_direction(), *param.get_range(), result) == true);
    EXPECT_TRUE(TestReadParamModifier::inner_check_finish(param, result) == true);
  }
  
  // normal sequence
  BLOCK_FUNC()
  {
    // gt 
    ObRange request;
    char *key2 = (char*)"ddef";
    ObString sk2(0, 4, key2);
    request.end_key_ = sk2;

    // inclusive
    request.border_flag_.set_inclusive_end();
    param.set(table_id, table_name, request);
    EXPECT_TRUE(is_finish_scan(param.get_scan_direction(), *param.get_range(), result) == false);
    EXPECT_TRUE(TestReadParamModifier::inner_check_finish(param, result) == false);

    // not inclusive
    request.border_flag_.unset_inclusive_end();
    param.set(table_id, table_name, request);
    EXPECT_TRUE(is_finish_scan(param.get_scan_direction(), *param.get_range(), result) == false);
    EXPECT_TRUE(TestReadParamModifier::inner_check_finish(param, result) == false);
  }
  
  // invert sequence
  param.set_scan_direction(ObScanParam::BACKWARD);
  BLOCK_FUNC()
  {
    // lt
    ObRange request;
    char *key2 = (char*)"adef";
    ObString sk2(0, 4, key2);
    request.start_key_= sk2;

    // inclusive
    request.border_flag_.set_inclusive_start();
    param.set(table_id, table_name, request);
    EXPECT_TRUE(is_finish_scan(param.get_scan_direction(), *param.get_range(), result) == false);
    EXPECT_TRUE(TestReadParamModifier::inner_check_finish(param, result) == false);

    // not inclusive
    request.border_flag_.unset_inclusive_start();
    param.set(table_id, table_name, request);
    EXPECT_TRUE(is_finish_scan(param.get_scan_direction(), *param.get_range(), result) == false);
    EXPECT_TRUE(TestReadParamModifier::inner_check_finish(param, result) == false);
  }

  // invert sequence
  BLOCK_FUNC()
  {
    // eq
    ObRange request;
    char *key2 = (char*)"cdef";
    ObString sk2(0, 4, key2);
    request.start_key_= sk2;

    // inclusive
    request.border_flag_.set_inclusive_start();
    param.set(table_id, table_name, request);
    EXPECT_TRUE(is_finish_scan(param.get_scan_direction(), *param.get_range(), result) == false);
    EXPECT_TRUE(TestReadParamModifier::inner_check_finish(param, result) == false);

    // not inclusive
    request.border_flag_.unset_inclusive_start();
    param.set(table_id, table_name, request);
    EXPECT_TRUE(is_finish_scan(param.get_scan_direction(), *param.get_range(), result) == true);
    EXPECT_TRUE(TestReadParamModifier::inner_check_finish(param, result) == true);
  }
  
  // invert sequence
  BLOCK_FUNC()
  {
    // gt 
    ObRange request;
    char *key2 = (char*)"ddef";
    ObString sk2(0, 4, key2);
    request.start_key_= sk2;

    // inclusive
    request.border_flag_.set_inclusive_start();
    param.set(table_id, table_name, request);
    EXPECT_TRUE(is_finish_scan(param.get_scan_direction(), *param.get_range(), result) == true);
    EXPECT_TRUE(TestReadParamModifier::inner_check_finish(param, result) == true);

    // not inclusive
    request.border_flag_.unset_inclusive_start();
    param.set(table_id, table_name, request);
    EXPECT_TRUE(is_finish_scan(param.get_scan_direction(), *param.get_range(), result) == true);
    EXPECT_TRUE(TestReadParamModifier::inner_check_finish(param, result) == true);
  }
}


