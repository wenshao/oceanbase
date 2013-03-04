/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * ob_sstable_checker.h is for checking validity of a given 
 * sstable file 
 *
 * Authors:
 *   wushi <wushi.ly@taobao.com>
 *   huating <huating.zmq@taobao.com>  
 *
 */
#ifndef OCEANBASE_CHUNKSERVER_OB_SSTABLE_CHECKER_H_ 
#define OCEANBASE_CHUNKSERVER_OB_SSTABLE_CHECKER_H_
#include "sstable/ob_sstable_trailer.h"
#include "common/ob_malloc.h"
#include "common/bloom_filter.h"
#include "common/ob_define.h"
#include "common/compress/ob_compressor.h"
#define DEF_DESERIALIZE_AND_CHECK_MEMBER(T) \
  static T * deserialize_and_check(char *data_buf, const int64_t size, int64_t &pos)
namespace oceanbase
{
  namespace chunkserver 
  {
    /// @struct  ObSCRecordHeader sstable中record头，非压缩存储，磁盘上存放网络字节序
    struct ObSCRecordHeader 
    {
      static ObSCRecordHeader * deserialize_and_check(char *data_buf, 
                                                      const int64_t size,
                                                      int64_t &pos,
                                                      const int16_t magic);
      /// @property magic number
      int16_t magic_;          
      /// @property length of header
      int16_t header_length_; 
      /// @property version of this struct
      int16_t version_;       
      /// @property header checksum   (parity checking)
      int16_t header_checksum_;
      int64_t reserved_;  
      /// @property size of orignal data
      int32_t data_length_;   
      /// @property size of compressed data, if not compressed data_zlength_ == data_length_
      int32_t data_zlength_;  
      /// @property crc checksum of record data, excluding the ObRecordHeader
      int64_t data_checksum_; 
      /// @property pointer to payload data
      char payload_[0];
    };

    /// @struct  ObSCSSTableTrailer sstable文件trailer
    struct ObSCSSTableTrailer 
    {
      DEF_DESERIALIZE_AND_CHECK_MEMBER(ObSCSSTableTrailer);
      /// @property size of ObSSTableTrailer
      int32_t size_;      
      /// @property version of this struct
      int32_t version_;
      /// @property version of table  
      int64_t table_version_; 
      /// @property offset of first data block
      int64_t first_block_data_offset_;
      /// @property count of block in sstable
      int64_t block_count_;       
      /// @property offset of block index in sstable file
      int64_t block_index_record_offset_; 
      /// @property size of block index
      int64_t block_index_record_size_;   
      /// @property hash count of bloom filter
      int64_t bloom_filter_hash_count_; 
      /// @property offset of bloom filter in sstable file 
      int64_t bloom_filter_record_offset_;  
      /// @property size of bloom filter record
      int64_t bloom_filter_record_size_;  
      /// @property offset of shcema record in sstable file
      int64_t schema_record_offset_;    
      /// @property size of schema record
      int64_t schema_record_size_;  
      /// @property size of block, disk block
      int64_t blocksize_;
      /// @property number of record stored in sstable file
      int64_t row_count_; 
      /// @property checksum of sstable file
      uint64_t sstable_checksum_;
      uint64_t first_table_id_;
      int64_t frozen_time_;
      int64_t reserved64_[oceanbase::sstable::ObSSTableTrailer::RESERVED_LEN]; 
      /// @property compressed method name
      char compressor_name_[oceanbase::common::OB_MAX_COMPRESSOR_NAME_LENGTH]; 
      /// @property format of row 稠密(dense)，稀疏(sparse)或者混合(mixed)格式
      int16_t row_value_store_style_; 
      int16_t reserved_;  
    };

    /// @struct  ObSCTrailerOffset trailer offset in sstable file
    struct ObSCTrailerOffset
    {
      DEF_DESERIALIZE_AND_CHECK_MEMBER(ObSCTrailerOffset);
      /// @property offset of sstable trailer
      int64_t trailer_offset_;
    };

    /// @struct  ObSCTableSchemaColumnDef schema column info
    struct ObSCTableSchemaColumnDef
    {
      /// @property id of column
      int16_t  reserved_;           //reserved,must be 0 for V0.2.0
      uint16_t column_group_id_;    //column gropr id for V0.2.0
      uint32_t column_name_id_;     //column name id
      /// @property column value type
      int32_t  column_value_type_; 
      uint32_t table_id_;

      static int64_t get_serialize_size()
      {
        return (sizeof(uint64_t) + sizeof(int32_t) * 2);
      }

      bool is_less(const ObSCTableSchemaColumnDef& rhs)
      {
        bool ret = true;
        if ( table_id_ > rhs.table_id_ 
             || (table_id_ == rhs.table_id_ && column_group_id_ > rhs.column_group_id_)
             || (table_id_ == rhs.table_id_ && column_group_id_ == rhs.column_group_id_
                 && column_name_id_ >= rhs.column_name_id_) )
        {
          ret = false;
        }
        return ret;
      }
    };
    /// @struct  ObSCTableSchemaHeader schema header
    struct ObSCTableSchemaHeader
    {
      static int deserialize_and_check(char *data_buf, ObSCTableSchemaHeader& schema,
                                       const int64_t size, int64_t &pos);
      static int64_t get_serialize_size()
      {
        return (sizeof(int16_t) * 4);
      }
      /// @property count of columns
      int16_t column_count_;        //column count compatible for V0.1.0
      int16_t reserved16_;          //must be 0
      int32_t total_column_count_;  //column count of all tables
 
      ObSCTableSchemaColumnDef column_defs_[common::OB_MAX_COLUMN_NUMBER * common::OB_MAX_TABLE_NUMBER];
    };



    /// @struct  block info
    struct ObSCBlockInfo
    {
      /// @property table id
      int16_t  reserved16_;          //must be 0 V0.2.0
      uint16_t column_group_id_;     //column group id       
      uint32_t table_id_;            //table id

      /// @property size of block record
      int32_t  block_record_size_;     
      /// @property size of maximum key of block
      int16_t  block_end_key_size_;   
      int16_t reserved_;         
    };

    /// @struct  block index header
    struct ObSCSSTableBlockIndexHeader
    {
      DEF_DESERIALIZE_AND_CHECK_MEMBER(ObSCSSTableBlockIndexHeader);
      /// @property block count
      int64_t sstable_block_count_;
      /// @property offset of end key string arrays, according to block index self
      int32_t end_key_char_stream_offset_;
      int32_t reserved32_;
      int64_t reserved64_[2]; 
      ObSCBlockInfo   block_info_array_[0];
    };

    /// @struct  block header
    struct ObSCSSTableBlockHeader
    {
      ObSCSSTableBlockHeader* deserialize_and_check(char *data_buf,
                                                    const int64_t size,
                                                    int64_t &pos,
                                                    common::ObMemBuffer& mem_buf,
                                                    const char* compressor_name);
      /// @property 行索引数组相对PayLoad起始点的偏移值
      int32_t row_index_array_offset_; 
      /// @property 该block中行数
      int32_t row_count_;     
      int64_t reserved_;    
    };

    /// @class  check validity of an existing sstable file
    /// @author wushi(wushi.ly@taobao.com)  (8/26/2010)
    class ObSCSSTableChecker
    {
    public:
      /// @fn constructor 
      ObSCSSTableChecker();
      /// @fn destructor
      ~ObSCSSTableChecker();
      /// @fn check if sstable format is correct
      int check(const char *sstable_fname);
    private:
      /// @fn check record offset and size
      int check_offset_size(const int64_t sstable_file_size);
      /// @fn check record rowkey order
      int check_rowkey_order(const int sstable_fd);
      /// @fn check a single row
      int check_row(const char * row, const int64_t row_len, const char *&key, int64_t &key_len);
      /// @property estimate item number in each block
      static const int64_t BLOOM_FILTER_ITEMS_ESTIMATE  = 1024 * 32 * 4;
      /// @property 2M byte memory to read block
      static const int64_t BLOCK_SIZE=2*1024*1024;
      ObSCTrailerOffset trailer_offset_;
      ObSCSSTableTrailer *trailer_;
      oceanbase::common::ObMemBuffer trailer_buffer_;

      ObSCTableSchemaHeader schema_;
      oceanbase::common::ObMemBuffer schema_buffer_;
      oceanbase::common::ObMemBuffer key_stream_buffer_;
      ObSCRecordHeader       *bloom_filter_record_;
      oceanbase::common::ObMemBuffer bloom_filter_buffer_;
      oceanbase::common::BloomFilter bloom_filter_readed_;
      oceanbase::common::BloomFilter bloom_filter_generated_;
      ObSCSSTableBlockIndexHeader   *block_index_;
      oceanbase::common::ObMemBuffer block_index_buffer_;
      oceanbase::common::ObMemBuffer block_buffer_;
      oceanbase::common::ObMemBuffer decompress_block_buffer_;
    };
  }
}
#endif /* CHUNKSERVER_OB_SSTABLE_CHECKER_H_ */
