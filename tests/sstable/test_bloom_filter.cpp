/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * test_bloom_filter.cpp for test bloom filter 
 *
 * Authors: 
 *   fanggang< fanggang@taobao.com>
 *   huating <huating.zmq@taobao.com>
 *
 */

#include <gtest/gtest.h>
#include <stdint.h>
#include <string.h>
#include <iostream>
#include "common/bloom_filter.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace std;

//generate the key string
char* random_str(uint len)
{
  ::srandom(1);
  char *str = new char[len];
  long rand = 0;
  uint offset = 0;

  while(offset < len)
  {
    rand = ::random() % 256;
    str[offset] = (char)rand;
    offset ++;
  }
  return str;
}

TEST(BLOOM_FILTER, test)
{
  //element count is about 32k  = 256m/ 0.3k
  uint32_t num = 1024 * 32;
  float prob = static_cast<float>(0.1);
  uint key_len = 17;
  BloomFilter bfilter;
  uint i = 0;
  char *array[num];
  int ret = bfilter.init(num, prob);
  EXPECT_TRUE(OB_SUCCESS == ret);
  while(i < num) {
    char *key_str = random_str(key_len);
    array[i] = key_str;
    bfilter.insert(key_str, key_len);
    i++;
  }
  
  i = 0;
  bool is_contain = false;
  while(i < num)
  {
   is_contain = bfilter.may_contain(array[i], key_len);
   EXPECT_EQ(true, is_contain);
   delete [] array[i];
   i++;
  }
  
  //test not contain key
  char* out_key = new char[key_len];
  memset(out_key, 0, key_len);
  is_contain = bfilter.may_contain(out_key, key_len);
  EXPECT_EQ(is_contain, false);
  memset(out_key, 1, key_len);
  is_contain = bfilter.may_contain(out_key, key_len);
  EXPECT_EQ(is_contain, false);
  delete [] out_key;
}

int main(int argc, char **argv)
{
  TBSYS_LOGGER.setLogLevel("INFO");
  ob_init_memory_pool();
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
