/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * test_sstable_trailer.cpp for test trailer of sstable 
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
#include "ob_string.h"
#include "sstable/ob_sstable_trailer.h"
#include "ob_sstable_trailerV1.h"
#include "common/file_utils.h"
#include "common/file_directory_utils.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::sstable;
using namespace std;

namespace oceanbase
{
  namespace tests
  {
    namespace sstable 
    {
      static const char * trailer_file_name = "trailer_for_test";
      class TestObSSTableTrailer: public ::testing::Test
      {
      public:
        virtual void SetUp()
        {
      
        }
      
        virtual void TearDown()
        {
      
        }
      };
      
      TEST_F(TestObSSTableTrailer, SetAndGetV2)
      {
        ObSSTableTrailer trailer;
        trailer.set_trailer_version(0x200);
        trailer.set_table_version(1);
        trailer.set_first_block_data_offset(0);
        trailer.set_row_value_store_style(1);
        trailer.set_block_count(1023);
        trailer.set_block_index_record_offset(2047);
        trailer.set_block_index_record_size(1024);
        trailer.set_bloom_filter_hash_count(8);
        trailer.set_bloom_filter_record_offset(2047 + 1024);
        trailer.set_bloom_filter_record_size(511);
        trailer.set_schema_record_offset(2047+ 1024 + 511);
        trailer.set_schema_record_size(1023);
        trailer.set_block_size(64 * 1024);
        trailer.set_row_count(777);
        trailer.set_sstable_checksum(123456789);
        trailer.set_first_table_id(1001);
        trailer.set_frozen_time(123456789);
        const char *compressor_name = "lzo1x_1_11_compress";
        trailer.set_compressor_name(compressor_name);
#ifdef COMPATIBLE
        trailer.set_table_count(1);
#endif
        ObTrailerOffset trailer_offset;
        trailer_offset.trailer_record_offset_= 256 * 1024 - 1023;
  
        int64_t offset_len = trailer_offset.get_serialize_size();
        int64_t trailer_len = trailer.get_serialize_size();
        int64_t len = offset_len + trailer_len;
        char *buf = new char[len]; 
        int64_t pos = 0;
        trailer.serialize(buf, len, pos);
        trailer_offset.serialize(buf, len, pos);
        EXPECT_EQ(pos, len);
        pos = trailer_len;
        trailer_offset.trailer_record_offset_ = 0;
        trailer_offset.deserialize(buf, len, pos);
        EXPECT_EQ(trailer_offset.trailer_record_offset_, 256 * 1024 - 1023);
        
        trailer.reset();
        pos = 0;
        trailer.deserialize(buf, len, pos);
        EXPECT_EQ(0x200, trailer.get_trailer_version());
        EXPECT_EQ(1, trailer.get_table_version());
        EXPECT_EQ(0, trailer.get_first_block_data_offset());
        EXPECT_EQ(1, trailer.get_row_value_store_style());
        EXPECT_EQ(1023, trailer.get_block_count());
        EXPECT_EQ(2047, trailer.get_block_index_record_offset());
        EXPECT_EQ(1024, trailer.get_block_index_record_size());
        EXPECT_EQ(8, trailer.get_bloom_filter_hash_count());
        EXPECT_EQ(2047 + 1024, trailer.get_bloom_filter_record_offset());
        EXPECT_EQ(511, trailer.get_bloom_filter_record_size());
        EXPECT_EQ(2047+ 1024 + 511, trailer.get_schema_record_offset());
        EXPECT_EQ(1023, trailer.get_schema_record_size());
#ifdef COMPATIBLE
        EXPECT_EQ(0, trailer.get_key_stream_record_offset());
        EXPECT_EQ(0, trailer.get_key_stream_record_size());
#endif
        EXPECT_EQ(64 * 1024, trailer.get_block_size());
        EXPECT_EQ(777, trailer.get_row_count());
        EXPECT_EQ(123456789, (int64_t)trailer.get_sstable_checksum());
        EXPECT_EQ(1001, (int64_t)trailer.get_first_table_id());
        EXPECT_EQ(123456789, trailer.get_frozen_time());
        int ret = memcmp(compressor_name, trailer.get_compressor_name(), strlen(compressor_name));
        EXPECT_EQ(0, ret);

        delete [] buf;
      }

      TEST_F(TestObSSTableTrailer, write_tariler_to_disk)
      {
        ObSSTableTrailer trailer;
        FileUtils filesys;
        trailer.set_trailer_version(0x200);
        trailer.set_table_version(1);
        trailer.set_first_block_data_offset(0);
        trailer.set_row_value_store_style(1);
        trailer.set_block_count(1023);
        trailer.set_block_index_record_offset(2047);
        trailer.set_block_index_record_size(1024);
        trailer.set_bloom_filter_hash_count(8);
        trailer.set_bloom_filter_record_offset(2047 + 1024);
        trailer.set_bloom_filter_record_size(511);
        trailer.set_schema_record_offset(2047+ 1024 + 511);
        trailer.set_schema_record_size(1023);
        trailer.set_block_size(64 * 1024);
        trailer.set_row_count(777);
        trailer.set_sstable_checksum(123456789);
        trailer.set_first_table_id(1001);
        trailer.set_frozen_time(123456789);
        const char *compressor_name = "lzo1x_1_11_compress";
        trailer.set_compressor_name(compressor_name);

        ObTrailerOffset trailer_offset;
        trailer_offset.trailer_record_offset_= 256 * 1024 - 1023;
  
        int64_t offset_len = trailer_offset.get_serialize_size();
        int64_t trailer_len = trailer.get_serialize_size();

        int64_t buf_size = offset_len + trailer_len;
        int64_t pos = 0;
        char *serialize_buf = reinterpret_cast<char*>(malloc(buf_size));
        EXPECT_TRUE(NULL != serialize_buf);
        trailer.serialize(serialize_buf, buf_size, pos);
        trailer_offset.serialize(serialize_buf, buf_size, pos);
        filesys.open(trailer_file_name, O_WRONLY | O_CREAT | O_TRUNC, 0644);
        int64_t write_size;
        write_size = filesys.write(serialize_buf, buf_size);
        EXPECT_EQ(write_size, buf_size);
        free(serialize_buf);
        serialize_buf = NULL;
      }

#ifdef  COMPATIBLE
      //test compatible between version1 and version2
      TEST_F(TestObSSTableTrailer, Compatible)
      {
        ObSSTableTrailerV1 trailer;
        ObSSTableTrailer trailer2;
        
        trailer.set_trailer_version(0);
        trailer.set_table_version(1);
        trailer.set_first_block_data_offset(0);
        trailer.set_row_value_store_style(1);
        trailer.set_block_count(1023);
        trailer.set_block_index_record_offset(2047);
        trailer.set_block_index_record_size(1024);
        trailer.set_bloom_filter_hash_count(8);
        trailer.set_bloom_filter_record_offset(2047 + 1024);
        trailer.set_bloom_filter_record_size(511);
        trailer.set_schema_record_offset(2047+ 1024 + 511);
        trailer.set_schema_record_size(1023);
        trailer.set_key_stream_record_offset(2047+ 1024 + 511 + 1023);
        trailer.set_key_stream_record_size(1024);
        trailer.set_block_size(64 * 1024);
        trailer.set_row_count(777);
        trailer.set_sstable_checksum(123456789);
        const char *compressor_name = "lzo1x_1_11_compress";
        trailer.set_compressor_name(compressor_name);
        trailer.set_table_count(1);
        int err = trailer.init_table_info(1);
        EXPECT_EQ(0, err);
        trailer.set_table_id(0, 1);
        char *start_key_value=(char*)"0101010101011";
        char *end_key_value=(char*)"1101010101011";
        oceanbase::common::ObString start_key(13, 13, start_key_value);
        oceanbase::common::ObString end_key(13, 13, end_key_value);
        trailer.set_start_key(0, start_key);
        trailer.set_end_key(0, end_key);
        ObTrailerOffset trailer_offset;
        trailer_offset.trailer_record_offset_= 256 * 1024 - 1023;
  
        int64_t offset_len = trailer_offset.get_serialize_size();
        int64_t trailer_len = trailer.get_serialize_size();
        int64_t len = offset_len + trailer_len;
        char *buf = new char[len]; 
        int64_t pos = 0;
        trailer.serialize(buf, len, pos);
        trailer_offset.serialize(buf, len, pos);
        
        EXPECT_EQ(pos, len);
        pos = trailer_len;
        trailer_offset.trailer_record_offset_ = 0;
        trailer_offset.deserialize(buf, len, pos);
        EXPECT_EQ(trailer_offset.trailer_record_offset_, 256 * 1024 - 1023);
       
        pos = 0;
        trailer2.deserialize(buf, len, pos);
        delete [] buf;
        EXPECT_EQ(0, trailer2.get_trailer_version());
        EXPECT_EQ(1, trailer2.get_table_version());
        EXPECT_EQ(0, trailer2.get_first_block_data_offset());
        EXPECT_EQ(1, trailer2.get_row_value_store_style());
        EXPECT_EQ(1023, trailer2.get_block_count());
        EXPECT_EQ(2047, trailer2.get_block_index_record_offset());
        EXPECT_EQ(1024, trailer2.get_block_index_record_size());
        EXPECT_EQ(8, trailer2.get_bloom_filter_hash_count());
        EXPECT_EQ(2047 + 1024, trailer2.get_bloom_filter_record_offset());
        EXPECT_EQ(511, trailer2.get_bloom_filter_record_size());
        EXPECT_EQ(2047+ 1024 + 511, trailer2.get_schema_record_offset());
        EXPECT_EQ(1023, trailer2.get_schema_record_size());
        EXPECT_EQ(2047+ 1024 + 511 + 1023, trailer2.get_key_stream_record_offset());
        EXPECT_EQ(1024, trailer2.get_key_stream_record_size());
        EXPECT_EQ(64 * 1024, trailer2.get_block_size());
        EXPECT_EQ(777, trailer2.get_row_count());
        EXPECT_EQ(1, trailer2.get_table_count());
        EXPECT_EQ(123456789, (int64_t)trailer2.get_sstable_checksum());
        int ret = memcmp(compressor_name, trailer2.get_compressor_name(), strlen(compressor_name));
        EXPECT_EQ(0, ret);
        
        //set trailer version 0x200
        trailer2.set_trailer_version(0x200);
        trailer_offset.trailer_record_offset_= 256 * 1024 - 1023;
  
        offset_len = trailer_offset.get_serialize_size();
        trailer_len = trailer2.get_serialize_size();
        len = offset_len + trailer_len;
        buf = new char[len]; 
        pos = 0;
        trailer2.serialize(buf, len, pos);
        trailer_offset.serialize(buf, len, pos);
        oceanbase::common::FileUtils filesys;
        int32_t fd = -1;
        fd = filesys.open("tmptrailer", O_WRONLY | O_CREAT | O_TRUNC, 0644);
        int64_t write_size = 0;
        write_size = filesys.write(buf, len);
        EXPECT_EQ(write_size, len);
        filesys.close();
        EXPECT_EQ(pos, len);
        pos = trailer_len;

        pos = 0;
        trailer2.reset();
        //deserialize trailer version 0x200
        trailer2.deserialize(buf, len, pos);
        delete [] buf;

        //check data in trailer2
        EXPECT_EQ(0x200, trailer2.get_trailer_version());
        EXPECT_EQ(1, trailer2.get_table_version());
        EXPECT_EQ(0, trailer2.get_first_block_data_offset());
        EXPECT_EQ(1, trailer2.get_row_value_store_style());
        EXPECT_EQ(1023, trailer2.get_block_count());
        EXPECT_EQ(2047, trailer2.get_block_index_record_offset());
        EXPECT_EQ(1024, trailer2.get_block_index_record_size());
        EXPECT_EQ(8, trailer2.get_bloom_filter_hash_count());
        EXPECT_EQ(2047 + 1024, trailer2.get_bloom_filter_record_offset());
        EXPECT_EQ(511, trailer2.get_bloom_filter_record_size());
        EXPECT_EQ(2047+ 1024 + 511, trailer2.get_schema_record_offset());
        EXPECT_EQ(1023, trailer2.get_schema_record_size());
        EXPECT_EQ(0, trailer2.get_key_stream_record_offset());
        EXPECT_EQ(0, trailer2.get_key_stream_record_size());
        EXPECT_EQ(64 * 1024, trailer2.get_block_size());
        EXPECT_EQ(777, trailer2.get_row_count());
        EXPECT_EQ(0, trailer2.get_table_count());
        EXPECT_EQ(123456789, (int64_t)trailer2.get_sstable_checksum());
        ret = memcmp(compressor_name, trailer2.get_compressor_name(), strlen(compressor_name));
        EXPECT_EQ(0, ret);
      }
#else
      //test compatible between version2 && version2 compiled without -DCOMPATIBLE
      TEST_F(TestObSSTableTrailer, Compatible)
      {
        ObTrailerOffset trailer_offset;
        ObSSTableTrailer trailer;
        FileUtils filesys;
        const char *compressor_name = "lzo1x_1_11_compress";
        int64_t file_len = FileDirectoryUtils::get_size(trailer_file_name);
        char *file_buf = reinterpret_cast<char*>(malloc(file_len));
        EXPECT_TRUE(NULL != file_buf);
        int64_t read_size = 0;
        read_size = filesys.read(file_buf, file_len);
        EXPECT_EQ(read_size, file_len);
            
        int64_t pos = 0;
        pos = trailer.get_serialize_size();
        trailer_offset.trailer_record_offset_ = 0;
        trailer_offset.deserialize(file_buf, file_len, pos);
        EXPECT_EQ(trailer_offset.trailer_record_offset_, 256 * 1024 - 1023);
        
        pos = 0;
        trailer.deserialize(file_buf, file_len, pos);
        EXPECT_EQ(0x200, trailer.get_trailer_version());
        EXPECT_EQ(1, trailer.get_table_version());
        EXPECT_EQ(0, trailer.get_first_block_data_offset());
        EXPECT_EQ(1, trailer.get_row_value_store_style());
        EXPECT_EQ(1023, trailer.get_block_count());
        EXPECT_EQ(2047, trailer.get_block_index_record_offset());
        EXPECT_EQ(1024, trailer.get_block_index_record_size());
        EXPECT_EQ(8, trailer.get_bloom_filter_hash_count());
        EXPECT_EQ(2047 + 1024, trailer.get_bloom_filter_record_offset());
        EXPECT_EQ(511, trailer.get_bloom_filter_record_size());
        EXPECT_EQ(2047+ 1024 + 511, trailer.get_schema_record_offset());
        EXPECT_EQ(1023, trailer.get_schema_record_size());
        EXPECT_EQ(64 * 1024, trailer.get_block_size());
        EXPECT_EQ(777, trailer.get_row_count());
        EXPECT_EQ(123456789, (int64_t)trailer.get_sstable_checksum());
        EXPECT_EQ(1001, (int64_t)trailer.get_first_table_id());
        EXPECT_EQ(123456789, trailer.get_frozen_time());
        int ret = memcmp(compressor_name, trailer.get_compressor_name(), strlen(compressor_name));
        EXPECT_EQ(0, ret);
        free(file_buf);
        file_buf = NULL;
      }
#endif
    }//end namespace sstable
  }//end namespace tests
}//end namespace oceanbase

int main(int argc, char** argv)
{
  ob_init_memory_pool();
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
