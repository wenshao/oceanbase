/*
* (C) 2007-2011 TaoBao Inc.
*
* This program is free software; you can redistribute it and/or modify
* it under the terms of the GNU General Public License version 2 as
* published by the Free Software Foundation.
*
* ob_join_get_cell_stream.cpp is for what ...
*
* Version: $id$
*
* Authors:
*   MaoQi maoqi@taobao.com
*
*/
#include "ob_join_get_cell_stream.h"
#include "ob_read_param_modifier.h"
#include "ob_chunk_server_main.h"
#include "ob_compactsstable_cache.h"
#include "common/utility.h"

namespace oceanbase
{
  namespace chunkserver
  {
    using namespace oceanbase::common;
    using namespace oceanbase::compactsstable;

    int ObJoinGetCellStream::next_cell()
    {
      int ret = OB_SUCCESS;
      if (!check_inner_stat())
      {
        TBSYS_LOG(ERROR, "check inner stat failed");
        ret = OB_INNER_STAT_ERROR;
      }
      else
      {
        if (get_cells_.get_cell_size() == 0)
        {
          TBSYS_LOG(DEBUG, "check get param cells count is 0");
          ret = OB_ITER_END;
        }
        else
        {
          ret = get_next_cell();
        }
    
        if (OB_ITER_END == ret)
        {
          TBSYS_LOG(DEBUG, "get the next cell return finish");
        }
        else if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "check get next cell failed:ret[%d]", ret);
        }
      }
      return ret;
    }

    int ObJoinGetCellStream::get(const ObReadParam &read_param, ObCellArrayHelper & get_cells)
    {
      int ret = OB_SUCCESS;
      reset_inner_stat();
      get_cells_ = get_cells;
      read_param_ = read_param;
      if (NULL != cache_)
      {
        ret = row_result_.init();
      }
      return ret;
    }
    
    void ObJoinGetCellStream::set_cache(ObJoinCache &cache)
    {
      is_cached_ = true;
      cache_ = &cache;
    }
    
    int ObJoinGetCellStream::get_next_cell(void)
    {
      int add_cache_err = OB_SUCCESS;
      int ret = cur_result_.next_cell();
      if (OB_ITER_END == ret)
      {
        // must add cell firstly before scanner reset
        if (is_cached_)
        {
          add_cache_err = add_cell_cache(true);
          if (add_cache_err != OB_SUCCESS)
          {
            TBSYS_LOG(ERROR, "add cell to cache failed:err[%d]", add_cache_err);
          }
        }
        // new rpc call get cell data
        ret = get_new_cell_data();
        if (OB_ITER_END == ret)
        {
          TBSYS_LOG(DEBUG, "get cell data all finish");
        }
        else if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "get cell data failed:ret[%d]", ret);
        }
      }
      // add cell to cache
      if (OB_SUCCESS == ret)
      {
        if (is_cached_)
        {
          // all finishi if OB_ITER_END == ret
          add_cache_err = add_cell_cache(false);
          if (add_cache_err != OB_SUCCESS)
          {
            TBSYS_LOG(ERROR, "add cell to cache failed:err[%d]", add_cache_err);
          }
        }
      }
      return ret;
    }
    
    int ObJoinGetCellStream::add_cell_cache(const bool force)
    {
      int ret = OB_SUCCESS;
      if (!force)
      {
        ObCellInfo * cell = NULL;
        bool row_change = false;
        ret = ObCellStream::get_cell(&cell, &row_change);
        if ((OB_SUCCESS != ret) || (NULL == cell))
        {
          TBSYS_LOG(ERROR, "get cell failed:ret[%d]", ret);
          ret = OB_ERROR;
        }
        else
        {
          if (row_change)
          {
            ret = add_cache_clear_result();
            if (ret != OB_SUCCESS)
            {
              TBSYS_LOG(ERROR, "add and clear result to cache failed:ret[%d]", ret);
            }
          }
          // add current cell to scanner temp result
          if (OB_SUCCESS == ret && cur_row_cache_result_valid_)
          {
            ret = row_result_.add_cell(*cell);
            if (ret != OB_SUCCESS)
            {
              cur_row_cache_result_valid_ = false;
              TBSYS_LOG(ERROR, "add cell to scanner failed:ret[%d]", ret);
            }
          }
        }
      }
      else
      {
        ret = add_cache_clear_result();
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "add cache and clear result failed:ret[%d]", ret);
        }
      }
      return ret;
    }
    
    // must clear the row scanner result
    int ObJoinGetCellStream::add_cache_clear_result()
    {
      int ret = OB_SUCCESS;
      // check data
      if (!row_result_.is_empty() && cur_row_cache_result_valid_)
      {
        if (OB_SUCCESS == ret)
        {
          ret = add_row_cache(row_result_);
          if (ret != OB_SUCCESS)
          {
            TBSYS_LOG(ERROR, "add row cache failed:ret[%d]", ret);
          }
        }
      }
      // clear
      if (NULL != cache_)
      {
        row_result_.init();
        cur_row_cache_result_valid_ = true;
      }
      return ret;
    }

    // new rpc call for get the new data
    int ObJoinGetCellStream::get_new_cell_data(void)
    {
      int ret = OB_SUCCESS;
      bool is_need_query_ups = false;
      ObMerger merger;
      // step 1. check finish get
      bool finish = false;
      ret = check_finish_all_get(finish);
      if (ret != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR, "check finish get failed:ret[%d]", ret);
      }
      else if (true == finish)
      {
        TBSYS_LOG(DEBUG, "check cell data is already finished");
        ret = OB_ITER_END;
      }
    
      // step 2. construct get param
      if (OB_SUCCESS == ret)
      {
        ret = get_next_param(read_param_, get_cells_, item_index_, &param_);
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "modify get param failed:ret[%d]", ret);
        }
      }

      //setp 3. get from compact sstable
      if (OB_SUCCESS == ret)
      {
        compact_result_.reset();
        if ((ret = get_compact_scanner(param_,compact_result_)) != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR,"get data from compact sstable failed,ret=%d",ret);
        }
      }

      //setp 4. modify get_param,get data from ups
      if (OB_SUCCESS == ret)
      {
        if (!compact_result_.is_empty())
        {
          ret = get_ups_param(param_, compact_result_);
        }

        if (OB_SUCCESS == ret)
        {
          is_need_query_ups = true;
          ObCellArrayHelper cur_get_cells(param_);          
          ret = get_cell_stream_.get(param_,cur_get_cells);
        }
        
        if (OB_ITER_END == ret)
        {
          ret = OB_SUCCESS;
          is_need_query_ups = false;
          get_cell_stream_.reset();
        }
      }

      if (OB_SUCCESS == ret)
      {
        if (compact_result_.is_empty() && !is_need_query_ups)
        {
          TBSYS_LOG(WARN,"in join stream but do not need join");
          ret = OB_ERR_UNEXPECTED;
        }
      }

      //setp 5. merge result
      //TODO (maoqi) ,maybe do not need ObMerger
      if (OB_SUCCESS == ret)
      {
        merger.reset();
        if ( (!compact_result_.is_empty()) && ((ret = merger.add_iterator(&compact_result_)) != OB_SUCCESS))
        {
          TBSYS_LOG(WARN,"add compactsstable iterator to merger failed,ret=%d",ret);
        }
        else if (is_need_query_ups && ((ret = merger.add_iterator(&get_cell_stream_)) != OB_SUCCESS))
        {
          TBSYS_LOG(WARN,"add ups stream to merger failed,ret=%d",ret);
        }
      }

      if (OB_SUCCESS == ret)
      {
        cur_result_.reset();
        ret = fill_compact_data(merger,cur_result_);
        if (OB_SIZE_OVERFLOW == ret)
        {
          ret = OB_SUCCESS;
          cur_result_.rollback();
          cur_result_.set_is_req_fullfilled(false,cur_result_.get_row_num());
        }
        else if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN,"merge result failed,ret=%d",ret);
        }
        else
        {
          cur_result_.set_is_req_fullfilled(true,cur_result_.get_row_num());          
        }

        //set fullfilled & version
        if (OB_SUCCESS == ret)
        {
          if (is_need_query_ups)
          {
            cur_result_.set_data_version(get_cell_stream_.get_data_version());
          }
          else
          {
            cur_result_.set_data_version(compact_result_.get_data_version());
          }
        }
      }

      if (OB_SUCCESS == ret)
      {
        ret = cur_result_.next_cell();
      }
      return ret;
    }
    
    // check all get op finish
    int ObJoinGetCellStream::check_finish_all_get(bool & finish)
    {
      int ret = OB_SUCCESS;
      int64_t item_count = 0;
      finish = false;
      if (is_finish_)
      {
        finish = is_finish_;
      }
      else
      {
        bool is_fullfill = false;
        ret = ObCellStream::cur_result_.get_is_req_fullfilled(is_fullfill, item_count);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "get scanner full filled status failed:ret[%d]", ret);
        }
        else if (item_count < 0)
        {
          TBSYS_LOG(ERROR, "check item count failed:item_count[%ld], item_index[%ld]", 
                    item_count, item_index_);
          ret = OB_ITEM_COUNT_ERROR;
        }
        else
        {
          item_index_ += item_count;
          if (is_fullfill)
          {
            if (item_index_ < get_cells_.get_cell_size())
            {
              TBSYS_LOG(DEBUG, "continue");
            }
            else if (item_index_ == get_cells_.get_cell_size())
            {
              is_finish_ = true;
              finish = is_finish_;
            }
            else
            {
              ret = OB_ITEM_COUNT_ERROR;
              TBSYS_LOG(ERROR, "check index error:item_count[%ld], item_index[%ld], "
                               "get_count[%ld]",
                        item_count, item_index_, get_cells_.get_cell_size());
            }
          }
          else if (item_index_ == get_cells_.get_cell_size())
          {
            is_finish_ = true;
            finish = is_finish_;
            TBSYS_LOG(WARN, "check item index succ but fulifill failed:"
                      "item_count[%ld], item_index[%ld], get_count[%ld]",
                      item_count, item_index_, get_cells_.get_cell_size());
          }
        }
      }
      return ret;
    }
    
    int ObJoinGetCellStream::add_row_cache(const ObRowCellVec &row)
    {
      int ret = OB_SUCCESS;
      uint64_t table_id = row.get_table_id();
      ObString row_key  = row.get_row_key();
      ObString cache_key;
      if ((NULL == row_key.ptr()) || (0 == row_key.length()))
      {
        ret = OB_INPUT_PARAM_ERROR;
        TBSYS_LOG(ERROR, "check row key failed:table_id[%lu]", table_id);
        hex_dump(row_key.ptr(), row_key.length(), true);
      }
    
      if (OB_SUCCESS == ret)
      {
        int64_t get_param_end_version = read_param_.get_version_range().end_version_;
        if (!read_param_.get_version_range().border_flag_.inclusive_end())
        {
          get_param_end_version--;
        }
        ObJoinCacheKey cache_key(static_cast<int32_t>(get_param_end_version), table_id, row_key);
        ret = cache_->put_row(cache_key, row);
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "put row to cache failed:ret[%d]", ret);
        }
      }
      return ret;
    }
    
    int ObJoinGetCellStream::get_cache_row(const ObCellInfo & key, ObRowCellVec *& result)
    {
      int ret = OB_SUCCESS;
      uint64_t table_id = key.table_id_;
      ObString row_key = key.row_key_;
      if (!is_cached_)
      {
        ret = OB_ENTRY_NOT_EXIST;
      }
      else if (!check_inner_stat())
      {
        TBSYS_LOG(ERROR, "check inner stat failed");
        ret = OB_INNER_STAT_ERROR;
      }
      else if ((NULL == row_key.ptr()) || (0 == row_key.length()))
      {
        TBSYS_LOG(ERROR, "check row key failed:table_id[%lu]", table_id);
        ret = OB_INPUT_PARAM_ERROR;
      }
      else if (key.row_key_.length() > OB_MAX_ROW_KEY_LENGTH)
      {
        TBSYS_LOG(WARN, "rowkey length too big [length:%d, OB_MAX_ROW_KEY_LENGTH:%ld]", 
                  key.row_key_.length(), OB_MAX_ROW_KEY_LENGTH);
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        if (OB_SUCCESS == ret)
        {
          int64_t get_param_end_version = read_param_.get_version_range().end_version_;
          if (!read_param_.get_version_range().border_flag_.inclusive_end())
          {
            get_param_end_version--;
          }
          ObJoinCacheKey cache_key(static_cast<int32_t>(get_param_end_version), table_id, row_key); 
          ret = row_result_.init();
          if (OB_SUCCESS == ret)
          {
            // get the row cell vec according to the key from join cache
            ret = cache_->get_row(cache_key, row_result_);
            if (OB_SUCCESS != ret)
            {
              result = NULL;
              TBSYS_LOG(DEBUG, "find this row from cache failed:table_id[%lu], ret[%d]", table_id, ret); 
              hex_dump(row_key.ptr(), row_key.length(), true);
            }
            else
            {
              result = &row_result_;
              TBSYS_LOG(DEBUG, "find row from cache succ");
              hex_dump(row_key.ptr(), row_key.length(), true);
            }
          }
          else
          {
            TBSYS_LOG(WARN, "row data init failed:ret[%d]", ret);
          }
        }
      }
      return ret;
    }
    
    int ObJoinGetCellStream::get_compact_scanner(const ObGetParam& get_param,ObScanner& compact_scanner)
    {
      const ObGetParam::ObRowIndex* row_index           = get_param.get_row_index();
      int64_t  row_num                                  = get_param.get_row_size();
      ObTablet* tablet                                  = NULL;
      uint64_t table_id                                 = OB_INVALID_ID;
      int ret                                           = OB_SUCCESS;
      bool is_fullfilled                                = true;
      ObString rowkey;

      if (NULL == row_index)
      {
        ret = OB_ERROR;
      }
      else
      {
        table_id = get_param[row_index[0].offset_]->table_id_;
        tablet   = THE_CHUNK_SERVER.get_tablet_manager().get_join_compactsstable().get_join_tablet(table_id);

        if (NULL != tablet)
        {
          for(int64_t i=0; (i < row_num) && (OB_SUCCESS == ret); ++i)
          {
            rowkey = get_param[row_index[i].offset_]->row_key_;
            if (table_id != get_param[row_index[i].offset_]->table_id_)
            {
              //we can only handle one join table once
              break;
            }
            else
            {
              ret = get_compact_row(*tablet,rowkey,compact_scanner);
              if (OB_SIZE_OVERFLOW == ret)
              {
                //scanner is full
                compact_scanner.rollback();
                is_fullfilled = false;
                ret = OB_SUCCESS;
                break;
              }
              else if (ret != OB_SUCCESS)
              {
                TBSYS_LOG(WARN,"get data from compactsstable failed,ret=%d",ret);
              }
            }
          }

          if (OB_SUCCESS == ret)
          {
            compact_scanner.set_data_version(tablet->get_cache_data_version());
            compact_scanner.set_is_req_fullfilled(is_fullfilled,compact_scanner.get_row_num());
          }
        }
      }
      return ret;
    }

    int ObJoinGetCellStream::get_compact_row(ObTablet& tablet,ObString& rowkey,ObScanner& compact_scanner)
    {
      int32_t compactsstable_num        = tablet.get_compactsstable_num();
      ObCompactSSTableMemNode* mem_node = tablet.get_compactsstable_list();
      ObCompactMemIteratorArray *its    = GET_TSI_MULT(ObCompactMemIteratorArray,TSI_CS_COMPACTSSTABLE_ITERATOR_2);
      ObCompactSSTableMem* mem          = NULL;
      int ret                           = OB_SUCCESS;
      int32_t m                         = 0;
      uint64_t table_id                 = OB_INVALID_ID;
      bool add_row_not_exist            = false;
      ObMerger merger;

      for(m=0; (OB_SUCCESS == ret) &&
            (mem_node != NULL) && (m < compactsstable_num); )
      {
        mem = &mem_node->mem_;
        if (NULL == mem)
        {
          TBSYS_LOG(WARN,"unexpect error,compactsstable is null");
          ret = OB_ERROR;
        }
        else
        {
          if (OB_INVALID_ID == table_id)
          {
            table_id = mem->get_table_id();
          }
          if (!mem->is_row_exist(rowkey))
          {
            //row not exists,do nothing            
            TBSYS_LOG(DEBUG,"row not exist,%s",print_string(rowkey));
            add_row_not_exist = true;
          }
          else if ((ret = its->iters_[m].init(mem)) != OB_SUCCESS)
          {
            TBSYS_LOG(WARN,"init iterator failed,ret=%d",ret);
          }
          else if ((ret = its->iters_[m].set_get_param(rowkey,NULL)) != OB_SUCCESS)
          {
            TBSYS_LOG(WARN,"set get param failed,ret=%d",ret);
          }
          else if ((ret = merger.add_iterator(&(its->iters_[m]))) != OB_SUCCESS)
          {
            TBSYS_LOG(WARN,"add iterator to merger failed,ret=%d",ret);
          }          
          else
          {
            ++m;
            //success
            //todo set data version
          }
        }
        mem_node = mem_node->next_;
      }

      if ((m > 0) && (OB_SUCCESS == ret))
      {
        ret = fill_compact_data(merger,compact_scanner);
      }
      else if ((OB_SUCCESS == ret) && add_row_not_exist)
      {
        //not exist
        ObCellInfo cell;
        cell.table_id_ = table_id;
        cell.column_id_ = OB_INVALID_ID;
        cell.value_.set_ext(ObActionFlag::OP_ROW_DOES_NOT_EXIST);
        cell.row_key_ = rowkey;
        ret = compact_scanner.add_cell(cell,false,true);
      }
      return ret;
    }
    
    int ObJoinGetCellStream::fill_compact_data(ObIterator& iterator,ObScanner& scanner)
    {
      int ret = OB_SUCCESS;

      ObCellInfo* cell = NULL;
      bool is_row_changed = false;

      while (OB_SUCCESS == (ret = iterator.next_cell()))
      {
        ret = iterator.get_cell(&cell,&is_row_changed);
        if (OB_SUCCESS != ret || NULL == cell)
        {
          TBSYS_LOG(WARN, "failed to get cell, cell=%p, err=%d", cell, ret);
        }
        else
        {
          ret = scanner.add_cell(*cell, false, is_row_changed);
          if (OB_SIZE_OVERFLOW == ret)
          {
            break;
          }
          else if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "failed to add cell to scanner, ret=%d", ret);
          }
        }

        if (OB_SUCCESS != ret)
        {
          break;
        }
      }

      if (OB_ITER_END == ret)
      {
        ret = OB_SUCCESS;
      }
      else if (OB_SIZE_OVERFLOW == ret)
      {
        //full
      }
      else if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "retor occurs while iterating, ret=%d", ret);
      }
      return ret;
    }

    int64_t ObJoinGetCellStream::get_data_version() const
    {
      return cur_result_.get_data_version();
    }

    void ObJoinGetCellStream::output(common::ObScanner & result)
    {
      int ret = OB_SUCCESS;
      ObCellInfo *cur_cell = NULL;
      bool is_row_changed = false;
      while (result.next_cell() == OB_SUCCESS)
      {
        ret = result.get_cell(&cur_cell,&is_row_changed);
        if (OB_SUCCESS == ret)
        {
          TBSYS_LOG(INFO,"%s,row_changed:%s",print_cellinfo(cur_cell),STR_BOOL(is_row_changed));
        }
        else
        {
          TBSYS_LOG(WARN, "get cell failed:ret[%d]", ret);
          break;
        }
      }
      result.reset_iter();
    }    
  } // end namespace chunkserver
} // end namespace oceanbase
