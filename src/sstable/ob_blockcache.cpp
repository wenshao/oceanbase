/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_blockcache.cpp for block cache. 
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */
#include "common/ob_file.h"
#include "common/ob_record_header.h"
#include "ob_blockcache.h"
#include "ob_sstable_stat.h"
#include "ob_sstable_block_index_v2.h"
#include "ob_sstable_writer.h"

namespace oceanbase
{
  namespace sstable
  {
    using namespace common;

    ObBlockCache::ObBlockCache()
    : inited_(false), fileinfo_cache_(NULL)
    {

    }

    ObBlockCache::ObBlockCache(IFileInfoMgr& fileinfo_cache) 
    : inited_(false), fileinfo_cache_(&fileinfo_cache)
    {
    }

    ObBlockCache::~ObBlockCache()
    {
    }

    int ObBlockCache::init(const ObBlockCacheConf& conf)
    {
      int ret = OB_SUCCESS;

      if (inited_)
      {
        TBSYS_LOG(INFO, "have inited");
      }
      else if (OB_SUCCESS != kv_cache_.init(conf.block_cache_memsize_mb * 1024 * 1024))
      {
        TBSYS_LOG(WARN, "init kv cache fail");
        ret = OB_ERROR;
      }
      else
      {
        inited_ = true;
        TBSYS_LOG_US(DEBUG, "init blockcache succ block_cache_memsize_mb=%ld, "
                            "ficache_max_num=%ld",
                     conf.block_cache_memsize_mb, conf.ficache_max_num);
      }

      return ret;
    }

    int ObBlockCache::destroy()
    {
      int ret = OB_SUCCESS;

      if (inited_)
      {
        if (OB_SUCCESS != kv_cache_.destroy())
        {
          TBSYS_LOG(WARN, "destroy cache fail");
          ret = OB_ERROR;
        }
        else
        {
          inited_ = false;
          fileinfo_cache_ = NULL;
        }
      }

      return ret;
    }

    int ObBlockCache::clear()
    {
      int ret = OB_SUCCESS;

      if (!inited_)
      {
        TBSYS_LOG(WARN, "have not inited");
        ret = OB_ERROR;
      }
      else if (OB_SUCCESS != kv_cache_.clear())
      {
        TBSYS_LOG(WARN, "clear cache fail");
        ret = OB_ERROR;
      }
      else
      {
        // do nothing
      }

      return ret;
    }

    int ObBlockCache::read_record(IFileInfoMgr& fileinfo_cache, 
                                  const uint64_t sstable_id, 
                                  const int64_t offset, 
                                  const int64_t size, 
                                  const char*& out_buffer)
    {
      int ret                 = OB_SUCCESS;
      ObFileBuffer* file_buf  = GET_TSI_MULT(ObFileBuffer, TSI_SSTABLE_FILE_BUFFER_1);
      out_buffer = NULL;

      if (NULL == file_buf)
      {
        TBSYS_LOG(WARN, "get thread file read buffer failed, file_buf=NULL");
        ret = OB_ERROR;
      }
      else
      {
        ret = ObFileReader::read_record(fileinfo_cache, sstable_id, offset, 
                                        size, *file_buf);
        if (OB_SUCCESS == ret)
        {
          out_buffer = file_buf->get_buffer() + file_buf->get_base_pos();
        }
      }

      return ret;
    }

    int32_t ObBlockCache::get_block(const uint64_t sstable_id,
                                    const int64_t offset,
                                    const int64_t nbyte,
                                    ObBufferHandle& buffer_handle,
                                    const uint64_t table_id,
                                    const bool check_crc)
    {
      int32_t ret         = -1;
      int status          = OB_SUCCESS;
      const char* buffer  = NULL;
      ObDataIndexKey data_index;
      BlockCacheValue input_value;
      BlockCacheValue output_value;

      if (!inited_ || NULL == fileinfo_cache_)
      {
        TBSYS_LOG(WARN, "have not inited, fileinfo_cache_=%p", fileinfo_cache_);
      }
      else if (OB_INVALID_ID == sstable_id || offset < 0 || nbyte <= 0
               || OB_INVALID_ID == table_id || 0 == table_id)
      {
        TBSYS_LOG(WARN, "invalid param sstable_id=%lu, offset=%ld, nbyte=%ld, "
                        "table_id=%lu", 
                  sstable_id, offset, nbyte, table_id);
      }
      else
      {
        data_index.sstable_id = sstable_id;
        data_index.offset = offset;
        data_index.size = nbyte;

        if (OB_SUCCESS == kv_cache_.get(data_index, output_value, buffer_handle.handle_, false))
        {
          buffer_handle.block_cache_ = this;
          buffer_handle.buffer_ = output_value.buffer;
          ret = static_cast<int>(nbyte);
          INC_STAT(table_id, INDEX_BLOCK_CACHE_HIT, 1);
        }
        else
        {
          INC_STAT(table_id, INDEX_BLOCK_CACHE_MISS, 1);
          status = read_record(*fileinfo_cache_, sstable_id, offset, nbyte, buffer);
          if (OB_SUCCESS == status && NULL != buffer)
          {
            INC_STAT(table_id, INDEX_DISK_IO_NUM, 1); 
            INC_STAT(table_id, INDEX_DISK_IO_BYTES, nbyte); 
            input_value.nbyte = nbyte;
            input_value.buffer = const_cast<char*>(buffer);

            if (check_crc)
            {
              status = ObRecordHeader::check_record(input_value.buffer, 
                input_value.nbyte, ObSSTableWriter::DATA_BLOCK_MAGIC);
              if (OB_SUCCESS != status)
              {
                TBSYS_LOG(WARN, "failed to check block record, sstable_id=%lu "
                                "offset=%ld nbyte=%ld",
                          sstable_id, offset, nbyte);              
              }
            }
            
            if (OB_SUCCESS == status)
            {
              //put and fetch block from block cache
              status = kv_cache_.put_and_fetch(data_index, input_value, output_value, 
                                               buffer_handle.handle_, false, false);
              if (OB_SUCCESS == status)
              {
                buffer_handle.block_cache_ = this;
                buffer_handle.buffer_ = output_value.buffer;
                ret = static_cast<int>(nbyte);
              }
              else
              {
                TBSYS_LOG(WARN, "failed to get block from block cached after put "
                                "block into block cache, sstable_id=%lu offset=%ld nbyte=%ld",
                          sstable_id, offset, nbyte);
              }
            }
          }
          else
          {
            TBSYS_LOG(WARN, "read block fail sstable_id=%lu offset=%ld nbyte=%ld",
                      sstable_id, offset, nbyte);
          }
        }
      }

      return ret;
    }

    int32_t ObBlockCache::get_block_readahead(
        const uint64_t sstable_id,
        const uint64_t table_id,
        const ObBlockPositionInfos &block_infos,
        const int64_t cursor, 
        const bool is_reverse, 
        ObBufferHandle &buffer_handle,
        const bool check_crc)
    {
      UNUSED(table_id);
      int ret = -1;
      int status = OB_SUCCESS;

      const char* buffer = NULL;
      ObDataIndexKey data_index;
      BlockCacheValue input_value;
      BlockCacheValue output_value;

      if (cursor < 0 || cursor >= block_infos.block_count_)
      {
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        // read current block in cache;
        const ObBlockPositionInfo & current_block = block_infos.position_info_[cursor];
        data_index.sstable_id = sstable_id;
        data_index.offset = current_block.offset_;
        data_index.size = current_block.size_;

        if (OB_SUCCESS == kv_cache_.get(data_index, output_value, buffer_handle.handle_, true))
        {
          // found in cache, continue search next block;
          buffer_handle.block_cache_ = this;
          buffer_handle.buffer_ = output_value.buffer;
          ret = static_cast<int>(data_index.size);
          INC_STAT(table_id, INDEX_BLOCK_CACHE_HIT, 1);
        }
        else
        {
          int64_t start_cursor = cursor;
          int64_t end_cursor = cursor;
          int64_t readahead_size = 0;
          int64_t inner_offset = 0;
          int64_t readahead_offset = 0;
          int64_t pos = 0;
          int64_t next_offset = block_infos.position_info_[cursor].offset_;

          // now calculate read ahead offset and size,
          // need attention reverse scan from down to top.
          if (!is_reverse)
          {
            for (pos = cursor; pos < block_infos.block_count_ && readahead_size < MAX_READ_AHEAD_SIZE; ++pos)
            {
              readahead_size += block_infos.position_info_[pos].size_;
              if (next_offset != block_infos.position_info_[pos].offset_)
              {
                TBSYS_LOG(ERROR, "asc: block not continious, current =%ld, acc =%ld, pos=%ld",
                    block_infos.position_info_[pos].offset_, next_offset, pos);
                status = OB_ERROR;
                break;
              }
              else
              {
                next_offset += block_infos.position_info_[pos].size_;
              }
            }
            end_cursor = pos - 1;
          }
          else
          {
            for (pos = cursor; pos >= 0 && readahead_size < MAX_READ_AHEAD_SIZE ; --pos)
            {
              readahead_size += block_infos.position_info_[pos].size_;
              if (next_offset != block_infos.position_info_[pos].offset_)
              {
                TBSYS_LOG(ERROR, "dsc: block not continious, current =%ld, acc =%ld, pos=%ld",
                    block_infos.position_info_[pos].offset_, next_offset, pos);
                status = OB_ERROR;
                break;
              }
              else if (pos > 0)
              {
                next_offset -= block_infos.position_info_[pos-1].size_;
              }
            }
            start_cursor = pos + 1;
          }


          if (OB_SUCCESS == status)
          {
            readahead_offset = block_infos.position_info_[start_cursor].offset_;
            status = read_record(*fileinfo_cache_, sstable_id, 
                readahead_offset, readahead_size, buffer);

            INC_STAT(table_id, INDEX_BLOCK_CACHE_MISS, 1);
            INC_STAT(table_id, INDEX_DISK_IO_NUM, 1); 
            INC_STAT(table_id, INDEX_DISK_IO_BYTES, readahead_size);
            
            ObIOStat stat;
            stat.total_read_size_ = readahead_size;
            stat.total_read_times_ = 1;
            stat.total_read_blocks_ = end_cursor - start_cursor + 1;
            add_io_stat(stat);
          }

          if (OB_SUCCESS == status && NULL != buffer)
          {
            // put all blocks into cache.
            for (int64_t i = start_cursor; i <= end_cursor; ++i)
            {
              data_index.sstable_id = sstable_id;
              data_index.offset = block_infos.position_info_[i].offset_;
              data_index.size = block_infos.position_info_[i].size_;

              input_value.nbyte = block_infos.position_info_[i].size_;
              input_value.buffer = const_cast<char*>(buffer) + inner_offset;

              if (check_crc)
              {
                status = ObRecordHeader::check_record(input_value.buffer, 
                  input_value.nbyte, ObSSTableWriter::DATA_BLOCK_MAGIC);
                if (OB_SUCCESS != status)
                {
                  TBSYS_LOG(WARN, "failed to check block record, sstable_id=%lu "
                                  "offset=%ld nbyte=%ld",
                            sstable_id, data_index.offset, data_index.size);   
                  break;           
                }
              }

              if (cursor == i)
              {
                status = kv_cache_.put_and_fetch(data_index, input_value, 
                    output_value, buffer_handle.handle_, false, false);
                if (OB_SUCCESS == status)
                {
                  buffer_handle.block_cache_ = this;
                  buffer_handle.buffer_ = output_value.buffer;
                  ret = static_cast<int>(data_index.size);
                }
                else
                {
                  TBSYS_LOG(WARN, "failed to get block from block cached after put "
                      "block into block cache, sstable_id=%lu offset=%ld nbyte=%ld",
                      sstable_id, data_index.offset, data_index.size);
                  break;
                }
              }
              else
              {
                kv_cache_.put(data_index, input_value, false);
              }
              inner_offset += block_infos.position_info_[i].size_; 
            }
          }
        }
      }

      return ret;
    }

    int32_t ObBlockCache::get_cache_block(const uint64_t sstable_id,
                                          const int64_t offset,
                                          const int64_t nbyte,
                                          ObBufferHandle& buffer_handle)
    {
      int32_t ret = -1;
      ObDataIndexKey data_index;
      BlockCacheValue value;

      if (!inited_ || NULL == fileinfo_cache_)
      {
        TBSYS_LOG(WARN, "have not inited, fileinfo_cache_=%p", fileinfo_cache_);
      }
      else if (OB_INVALID_ID == sstable_id || offset < 0 || nbyte <= 0)
      {
        TBSYS_LOG(WARN, "invalid param sstable_id=%lu, offset=%ld, nbyte=%ld",
                  sstable_id, offset, nbyte);
      }
      else
      {
        data_index.sstable_id = sstable_id;
        data_index.offset = offset;
        data_index.size = nbyte;

        if (OB_SUCCESS == kv_cache_.get(data_index, value, buffer_handle.handle_, true))
        {
          buffer_handle.block_cache_ = this;
          buffer_handle.buffer_ = value.buffer;
          ret = static_cast<int32_t>(nbyte);
        }
      }

      return ret;
    }


    const int64_t ObBlockCache::size() const
    {
      return kv_cache_.size();
    }

    ObAIOBufferMgr* ObBlockCache::get_aio_buf_mgr(const uint64_t sstable_id, 
                                                  const uint64_t table_id, 
                                                  const uint64_t column_group_id,
                                                  const bool free_mgr)
    {
      ObAIOBufferMgr* aio_buf_mgr = NULL;
      ObThreadAIOBufferMgrArray* aio_buf_mgr_array = GET_TSI_MULT(ObThreadAIOBufferMgrArray, TSI_SSTABLE_THREAD_AIO_BUFFER_MGR_ARRAY_1);

      if (NULL != aio_buf_mgr_array)
      {
        aio_buf_mgr = aio_buf_mgr_array->get_aio_buf_mgr(sstable_id, table_id, 
                                                         column_group_id, free_mgr);
      }

      return aio_buf_mgr;
    }

    int ObBlockCache::advise(const uint64_t sstable_id, 
                             const ObBlockPositionInfos& block_infos, 
                             const uint64_t table_id,
                             const uint64_t column_group_id,
                             const bool copy2cache,
                             const bool reverse_scan)
    {
      int ret                     = OB_SUCCESS;
      ObAIOBufferMgr* aio_buf_mgr = NULL;
  
      if (!inited_ || NULL == fileinfo_cache_)
      {
        TBSYS_LOG(WARN, "have not inited, fileinfo_cache_=%p", fileinfo_cache_);
        ret = OB_ERROR;
      }
      else if (OB_INVALID_ID == sstable_id || OB_INVALID_ID == table_id 
               || 0 == table_id || OB_INVALID_ID == column_group_id)
      {
        TBSYS_LOG(WARN, "invalid parameter, sstable_id=%lu, table_id=%lu, "
                        "column_group_id=%lu",
                  sstable_id, table_id, column_group_id);
        ret = OB_ERROR;
      }
      else if (NULL == (aio_buf_mgr = get_aio_buf_mgr(sstable_id, table_id, 
                                                      column_group_id, true)))
      {
        TBSYS_LOG(WARN, "get thread local aio buffer manager failed");
        ret = OB_ERROR;
      }
      else
      {
        ret = aio_buf_mgr->advise(*this, sstable_id, block_infos, copy2cache, reverse_scan);
      }
  
      return ret;
    }

    int32_t ObBlockCache::get_block_aio(const uint64_t sstable_id,
                                        const int64_t offset,
                                        const int64_t nbyte,
                                        ObBufferHandle& buffer_handle,
                                        const int64_t timeout_us,
                                        const uint64_t table_id,
                                        const uint64_t column_group_id,
                                        const bool check_crc)
    {
      int status                  = OB_SUCCESS;
      int32_t ret_size            = -1;
      ObAIOBufferMgr* aio_buf_mgr = NULL;
      char* buffer                = NULL;
      bool from_cache             = false;
      
      if (!inited_ || NULL == fileinfo_cache_)
      {
        TBSYS_LOG(WARN, "have not inited, fileinfo_cache_=%p", fileinfo_cache_);
      }
      else if (OB_INVALID_ID == table_id || 0 == table_id
               || OB_INVALID_ID == column_group_id)
      {
        TBSYS_LOG(WARN, "invalid parameter, table_id=%lu, column_group_id=%lu",
                  table_id, column_group_id);
      }
      else if (NULL == (aio_buf_mgr = get_aio_buf_mgr(sstable_id, table_id, column_group_id)))
      {
        TBSYS_LOG(WARN, "get thread local aio buffer manager failed");
      }
      else
      {
        status = aio_buf_mgr->get_block(*this, sstable_id, offset, nbyte, 
                                        timeout_us, buffer, from_cache, check_crc);
        if (OB_SUCCESS == status && NULL != buffer)
        {
          //build buffer handle
          ObBufferHandle handle_tmp(buffer);
          buffer_handle = handle_tmp;
          ret_size = static_cast<int32_t>(nbyte);
          if (from_cache)
          {
            INC_STAT(table_id, INDEX_BLOCK_CACHE_HIT, 1);
          }
          else
          {
            INC_STAT(table_id, INDEX_BLOCK_CACHE_MISS, 1);
            INC_STAT(table_id, INDEX_DISK_IO_NUM, 1); 
            INC_STAT(table_id, INDEX_DISK_IO_BYTES, nbyte); 
          }
        }
        else
        {
          TBSYS_LOG(WARN, "get block aio failed");
        }
      }
  
      return ret_size;
    }

    int ObBlockCache::get_next_block(ObDataIndexKey &data_index, 
                                     ObBufferHandle &buffer_handle)
    {
      int ret = OB_EAGAIN;
      BlockCacheValue value;

      if (!inited_ || NULL == fileinfo_cache_)
      {
        TBSYS_LOG(WARN, "have not inited, fileinfo_cache_=%p", fileinfo_cache_);
        ret = OB_ERROR;
      }
      else
      {
        while (OB_EAGAIN == ret)
        {
          ret = kv_cache_.get_next(data_index, value, buffer_handle.handle_);
          if (OB_SUCCESS == ret)
          {
            buffer_handle.block_cache_ = this;
            buffer_handle.buffer_ = value.buffer;
          }
          else if (OB_ITER_END == ret)
          {
            //complete traversal, do nothing
          }
          else
          {
            ret = OB_EAGAIN;
          }
        } // while
      }

      return ret;
    }
  }
}
