#include "ob_string.h"
#include "gtest/gtest.h"
#include "common/ob_malloc.h"

using namespace oceanbase::common;

int main(int argc, char **argv)
{
  ob_init_memory_pool();
  testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}

TEST(TestObString, deserialize)
{
  char buffer[1024] = "";
  char * temp = "abcd";
  ObString input_string;
  input_string.assign(temp, strlen(temp));
  int64_t pos = 0;
  int ret = input_string.serialize(buffer, sizeof(buffer), pos);
  EXPECT_TRUE(OB_SUCCESS == ret);

  ObString output_string;
  int64_t len = 0;
  ret = output_string.deserialize(buffer, pos, len);
  EXPECT_TRUE(OB_SUCCESS == ret);
  EXPECT_TRUE(len == pos);

  char * temp_two = "abcde";
  input_string.assign(temp_two, strlen(temp_two));
  pos = 0;
  ret = input_string.serialize(buffer, sizeof(buffer), pos);
  EXPECT_TRUE(OB_SUCCESS == ret);
  len = 0;
  ret = output_string.deserialize(buffer, pos, len);
  EXPECT_TRUE(OB_SUCCESS == ret);
  EXPECT_TRUE(len == pos);
  //
  output_string.assign(temp, strlen(temp));
  len = 0;
  ret = output_string.deserialize(buffer, pos, len);
  EXPECT_TRUE(OB_SUCCESS != ret);
}

