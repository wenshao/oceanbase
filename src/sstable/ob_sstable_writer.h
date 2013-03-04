/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_sstable_writer.h for persistent ssatable. 
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */
#ifndef OCEANBASE_SSTABLE_OB_SSTABLE_WRITER_H_
#define OCEANBASE_SSTABLE_OB_SSTABLE_WRITER_H_

#include "common/ob_define.h"
#include "common/ob_string.h"
#include "common/ob_file.h"
#include "common/bloom_filter.h"
#include "common/ob_record_header.h"
#include "common/compress/ob_compressor.h"

#include "ob_sstable_schema.h"
#include "ob_sstable_row.h"
#include "ob_sstable_trailer.h"
#include "ob_sstable_block_index_builder.h"
#include "ob_sstable_block_builder.h"


namespace oceanbase 
{
  namespace sstable 
  {
    /**
     * the class is used to write a sstable, the format of sstable
     * is below:
     *              ----------------------------------
     *              |          Block-0               |
     *              ----------------------------------
     *              |          Block-1               |
     *              ----------------------------------
     *              |          Block-2               |
     *              ----------------------------------
     *              |          Block-3               |
     *              ----------------------------------
     *              |          .......               |
     *              ----------------------------------
     *              |         Block index            |
     *              ----------------------------------
     *              |        Bloom filter            |
     *              ----------------------------------
     *              |       ObTableSchem             |
     *              ----------------------------------
     *              |      ObSstableTrailer          |
     *              ----------------------------------
     *              |   ObSSTableTrailerOffset       |
     *              ----------------------------------
     *  
     * WARNING: 1. be careful, the class is not thread safe. 
     *          2. if error happen, please call function
     *          close_sstable(), the function will close the file
     *          handler.
     *          3. if you call open_sstable, please call
     *          close_sstable whatever happens.
     */
    class ObSSTableWriter
    {
    public:
      static const int64_t DEFAULT_KEY_BUF_SIZE;
      static const int64_t DEFAULT_COMPRESS_BUF_SIZE;
      static const int64_t DEFAULT_SERIALIZE_BUF_SIZE;
      static const int64_t DEFAULT_WRITE_BUFFER_SIZE;
      static const int16_t DATA_BLOCK_MAGIC;
      static const int16_t BLOCK_INDEX_MAGIC;
      static const int16_t BLOOM_FILTER_MAGIC;
      static const int16_t SCHEMA_MAGIC;
      static const int16_t KEY_STREAM_MAGIC;
      static const int16_t TRAILER_MAGIC;

    public:
      ObSSTableWriter();
      ~ObSSTableWriter();

      /**
       * create sstable with one or more tables, this function is used 
       * to create the trasfered sstable or normal sstable
       * 
       * @param schema sstable schema
       * @param path sstable file path
       * @param compressor_name compressor name
       * @param table_version table version, all tables use the same 
       *                      one version
       * @param store_type store type, dense or spare, default use 
       *                   sparse
       * @param block_size block size in sstable file
       * @param element_count  element count of bloom filter
       * 
       * @return int if succes, return OB_SUCCESS, else return 
       *         OB_ERROR or OB_IO_ERROR
       */
      int create_sstable(const ObSSTableSchema& schema,
                         const common::ObString& path,
                         const common::ObString& compressor_name,
                         const int64_t table_version,
                         const int store_type = OB_SSTABLE_STORE_DENSE,
                         const int64_t block_size = ObSSTableBlockBuilder::SSTABLE_BLOCK_SIZE,
                         const int64_t element_count = 0);

      /**
       * create sstable with one or more tables, this function is used 
       * to create the trasfered sstable or normal sstable
       * 
       * @param schema sstable schema
       * @param path sstable file full path
       * @param trailer_param parameters of trailer
       * @param element_count element count of bloom filter
       * 
       * @return int if succes, return OB_SUCCESS, else return 
       *         OB_ERROR or OB_IO_ERROR
       */
      int create_sstable(const ObSSTableSchema& schema,
                         const common::ObString& path,
                         const ObTrailerParam& trailer_param,
                         const int64_t element_count = 0);

      /**
       * Inserts a sorted row into the sstable. This function only
       * accept sorted row. 
       * 
       * @param row one row data, include rowkey and ObObjs[]
       * @param approx_space_usage [out] approximate disk usage of 
       *                   sstable after adding this row, it always
       *                   returns the current disk usage even through
       *                   error happens;
       * 
       * @return int if success return OB_SUCCESS, else return 
       *         OB_ERROR
       */
      int append_row(const ObSSTableRow& row, int64_t& approx_space_usage);

 
      /**
       * Finalizes the creation of a sstable store, by writing block 
       * index, bloom fileter, schema, key stream and trailer. at the 
       * end, reset the writer to reuse it. 
       *  
       * @param trailer_offset offset of trailer in the sstable file, 
       *
       * @return int if success return OB_SUCCESS, else return 
       *         OB_ERROR
       */
      int close_sstable(int64_t& trailer_offset);

      /**
       * close sstable, it also return the sstable file size
       * 
       * @param trailer_offset [out] the trailer offset to return
       * @param sstable_size [out] the sstable file size to return
       * 
       * @return int if success return OB_SUCCESS, else return 
       *         OB_ERROR
       */
      int close_sstable(int64_t& trailer_offset, int64_t& sstable_size);

      void set_file_sys(common::ObIFileAppender *file_sys);
      
      void set_dio(const bool dio)
      {
        dio_ = dio;
      }

      const ObSSTableTrailer& get_trailer() const
      {
        return trailer_;
      }
    private:
      /**
       * reset sstable writer to reuse it
       */
      void reset();
      
      /**
       * copy sstable schema
       *
       * @param schema  schema to copy
       *
       * @return int if success return OB_SUCCESS, else return
       *         OB_ERROR
       */
      int copy_schema(const ObSSTableSchema& schema);

      bool is_invalid_row_key(const uint64_t table_id, 
                              const uint64_t column_group_id,
                              const common::ObString row_key);

      bool need_switch_block(const uint64_t table_id, 
                             const uint64_t column_group_id, 
                             const int64_t row_size);

      int store_trailer_fields(const uint64_t first_table_id, 
                               const common::ObString& compressor_name,
                               const int64_t table_version, 
                               const int store_type, 
                               const int64_t block_size);

      int update_bloom_filter(const uint64_t column_group_id,
                              const uint64_t table_id,
                              const common::ObString& key); 

      /**
       * check whether write sstable with dense foramt 
       * 
       * @return bool if true, return true, else return false
       */
      bool is_dense_format();

      /**
       * check whether the row count of column group is consitent 
       * 
       * @return int if success return OB_SUCCESS, else return 
       *         OB_ERROR
       */
      int check_row_count();

      int write_record_header(const int16_t magic, const char* comp_data, 
                              const int64_t comp_size, const int64_t uncomp_size, 
                              int64_t& writed_len);

      /**
       * compress and write one record, it will call the compressor 
       * here, 
       * 
       * @param input input buffer which store the block data
       * @param input_len length of input buffer
       * @param magic magic of record
       * @param writed_len how much data is written 
       * @param need_compress if need compress to store 
       * 
       * @return int if success return OB_SUCCESS, else return 
       *         OB_ERROR
       */
      int compress_and_write(const char* input, const int64_t input_len,
                             const int16_t magic, int64_t& writed_len,
                             const bool need_compress = true); 

      /**
       * write current block into sstable  
       * 
       * @return int if success return OB_SUCCESS, else return 
       */   
      int write_current_block();

      /**
       * write block index data  
       * 
       * @return int if success return OB_SUCCESS, else return 
       *         OB_ERROR
       */
      int write_block_index();

      /**
       * write bloom filter 
       * 
       * @return int if success return OB_SUCCESS, else return 
       *         OB_ERROR
       */
      int write_bloom_filter();

      /**
       * write sstable schema 
       * 
       * @return int if success return OB_SUCCESS, else return 
       *         OB_ERROR
       */
      int write_schema();

      /**
       * writer trailer 
       * 
       * @return int if success return OB_SUCCESS, else return 
       *         OB_ERROR
       */
      int write_trailer();

    private:
      static const int64_t MAX_SSTABLE_NAME_SIZE = 1024;  //1k
      static const int64_t MAX_BLOCK_INDEX_SIZE = INT32_MAX; //2G - 1

    private:
      DISALLOW_COPY_AND_ASSIGN(ObSSTableWriter);

      bool inited_;                              //whether sstable writer is inited
      bool first_row_;                           //whether first row in sstable
      bool add_row_count_;                       //whether add row count
      bool dio_;                                 //whether write sstable using dio
      
      common::ObFileAppender default_filesys_;   //file system
      common::ObIFileAppender* filesys_;          //actual file system
      char filename_[MAX_SSTABLE_NAME_SIZE];     //full file name of sstable
      uint64_t table_id_;                        //current table id of sstable
      uint64_t column_group_id_;                 //current column group id

      common::ObString cur_key_;                 //current key
      common::ObMemBuf cur_key_buf_;             //current key buffer
      common::ObMemBuf bf_key_buf_;              //bloom filter key buf
                                                
      int64_t offset_;                           //current offset of sstable
      int64_t prev_offset_;                      //previous offset of sstable
      int64_t row_count_;                        //row count
      int64_t prev_column_group_row_count_;      //row count of previous column group
      int64_t column_group_row_count_;           //row count of current writting column group
      ObCompressor* compressor_;                 //compressor to use
      int64_t uncompressed_blocksize_;           //uncompressed block size
      common::ObMemBuf compress_buf_;            //compress buffer
      common::ObMemBuf serialize_buf_;           //serrialize buffer

      ObSSTableSchema schema_;                   //schema of tables
      ObSSTableBlockBuilder block_builder_;      //row data block builder
      ObSSTableBlockIndexBuilder index_builder_; //index block builder
      bool enable_bloom_filter_;                 //if enable bloom filter  
      common::BloomFilter bloom_filter_;         //bloom filter
      uint64_t sstable_checksum_;                //checksum of sstable
      ObSSTableTrailer trailer_;                 //sstable trailer
      int64_t frozen_time_;                      //frozen time
    };
  } // namespace oceanbase::sstable
} // namespace Oceanbase

#endif // OCEANBASE_SSTABLE_OB_SSTABLE_WRITER_H_
