/*===============================================================
 *   (C) 2007-2010 Taobao Inc.
 *   
 *   
 *   Version: 0.1 2010-10-21
 *   
 *   Authors:
 *          daoan(daoan@taobao.com)
 *   
 *
 ================================================================*/
#include <algorithm>
#include <stdlib.h>
#include "rootserver/ob_root_table2.h"
#include "rootserver/ob_chunk_server_manager.h"
#include "common/ob_record_header.h"
#include "common/file_utils.h"
#include "common/ob_atomic.h"
namespace 
{
  const int SPLIT_TYPE_ERROR = -1;
  const int SPLIT_TYPE_TOP_HALF = 1;
  const int SPLIT_TYPE_BOTTOM_HALF = 2;
  const int SPLIT_TYPE_MIDDLE_HALF = 3;
}
namespace oceanbase 
{ 
  using namespace common;
  namespace rootserver 
  {
    ObRootTable2::ObRootTable2(ObTabletInfoManager* tim):tablet_info_manager_(tim), sorted_count_(0)
    {
      meta_table_.init(ObTabletInfoManager::MAX_TABLET_COUNT, data_holder_);
    }
    ObRootTable2::~ObRootTable2()
    {
    }

    int ObRootTable2::find_range(const common::ObRange& range, 
        const_iterator& first, 
        const_iterator& last) const
    {
      int ret = OB_ERROR;
      if (NULL != tablet_info_manager_) 
      {
        ret = OB_SUCCESS;
        //
        // find start_key in whole range scope
        common::ObRange start_range;
        start_range.table_id_  = range.table_id_;
        if (!range.border_flag_.is_min_value())
        {
          start_range.end_key_ = range.start_key_;
        }

        const_iterator find_start_pos = lower_bound(start_range);
        if (NULL == find_start_pos || find_start_pos == sorted_end())
        {
          TBSYS_LOG(DEBUG, "find start pos error find_start_pos = %p sorted_end = %p", find_start_pos, sorted_end());
          ret = OB_ERROR_OUT_OF_RANGE;
        }
        const common::ObTabletInfo* tablet_info = NULL;
        common::ObString end_key;
        if (OB_SUCCESS == ret)
        {
          first = find_start_pos;
          tablet_info = get_tablet_info(first);
          if (NULL == tablet_info)
          {
            TBSYS_LOG(DEBUG, "%s", "get_tablet_info");
            ret = OB_ERROR_OUT_OF_RANGE;
          }
        }
        if (OB_SUCCESS == ret)
        {
          // right on one of range's end boundary?
          if (!range.border_flag_.is_min_value() && 
              !tablet_info->range_.border_flag_.is_max_value() &&
              range.start_key_.compare(tablet_info->range_.end_key_) == 0)
          {
            if (!range.border_flag_.inclusive_start())
            {
              first++;
              if (first >= sorted_end())
              {
                TBSYS_LOG(DEBUG, "first[%p] >= sorted_end[%p] ", first, sorted_end());
                ret = OB_ERROR_OUT_OF_RANGE;
              }
              else 
              {
                tablet_info = get_tablet_info(first);
                if (NULL == tablet_info)
                {
                  TBSYS_LOG(DEBUG, "%s", "get_tablet_info");
                  ret = OB_ERROR_OUT_OF_RANGE;
                }
              }
            }
          }
        }

        //will find the last range
        if (OB_SUCCESS == ret)
        {
          last = lower_bound(range);
          if (NULL == last || last == sorted_end())
          {
            TBSYS_LOG(DEBUG, "last[%p] == sorted_end[%p] ", last, sorted_end());
            ret = OB_ERROR_OUT_OF_RANGE;
          }
        }
        if (OB_SUCCESS == ret && first > last)
        {
          TBSYS_LOG(DEBUG, "first[%p] > last[%p] ", first, last);
          ret = OB_ERROR_OUT_OF_RANGE;
        }
      }
      return ret;
    }


    int ObRootTable2::find_key(const uint64_t table_id, 
        const common::ObString& key,
        int32_t adjacent_offset,
        const_iterator& first, 
        const_iterator& last,
        const_iterator& ptr
        ) const
    {
      int ret = OB_ERROR;
      if (NULL != tablet_info_manager_) 
      {
        ret = OB_SUCCESS;
        if (adjacent_offset < 0) adjacent_offset = 0;

        // find start_key in whole range scope
        common::ObRange find_range;
        find_range.table_id_ = table_id;
        find_range.end_key_ = key;

        const_iterator find_pos = lower_bound(find_range);
        if (find_pos == NULL || find_pos == sorted_end())
        {
          ret = OB_ERROR_OUT_OF_RANGE;
        }

        const common::ObTabletInfo* tablet_info = NULL;

        if (OB_SUCCESS == ret)
        {
          tablet_info = get_tablet_info(find_pos);
          if (NULL == tablet_info)
          {
            ret = OB_ERROR_OUT_OF_RANGE;
          }
        }
        //if (OB_SUCCESS == ret)
        //{
        //  ptr = find_pos;
        //  // right on one of range's end boundary?
        //  common::ObString ptr_end_key = found_range.end_key_;
        //  if (key.compare(ptr_end_key) == 0)
        //  {
        //    common::ObBorderFlag ptr_border_flag = found_range.border_flag_;

        //    if (!(ptr_border_flag.inclusive_end()) )
        //    {
        //      TBSYS_LOG(ERROR, "every range should be set inclusive_end, so should not reach this");
        //      ret = OB_ERROR_OUT_OF_RANGE;
        //    }
        //  }
        //}
        if (OB_SUCCESS == ret) 
        {
          ptr = find_pos;
          int64_t max_left_offset = ptr - begin();
          int64_t max_right_offset = sorted_end() - ptr - 1;
          if (max_left_offset > 1) 
            max_left_offset = 1;
          if (max_right_offset > adjacent_offset)
            max_right_offset = adjacent_offset;

          // return pointers of tablets.
          first = ptr - max_left_offset;
          const common::ObTabletInfo* tablet_info = NULL;

          if (OB_SUCCESS == ret)
          {
            tablet_info = get_tablet_info(first);
            if (NULL == tablet_info || tablet_info->range_.table_id_ != table_id)
            {
              first = ptr; //previes is not the same table
            }
          }
          if (OB_SUCCESS == ret)
          {
            tablet_info = get_tablet_info(first);
            ObRange start_range;
            start_range.table_id_ = table_id;
            start_range.start_key_ = key;
            if (NULL == tablet_info || tablet_info->range_.compare_with_startkey(start_range) > 0)
            {
              TBSYS_LOG(WARN, "find range not exist.find_pos.start_key_=%s, key=%s",
                  tablet_info->range_.start_key_.ptr(), key.ptr());
              ret = OB_ERROR_OUT_OF_RANGE;
            }
          }
          if (OB_SUCCESS == ret)
          {
            last = ptr + max_right_offset;
            while (last >= first) 
            {
              tablet_info = get_tablet_info(last);
              if (NULL != tablet_info && tablet_info->range_.table_id_ == table_id)
              {
                break;
              }
              last--;
            }
          }
          if (last < first)
          {
            TBSYS_LOG(WARN, "table not exist in root_table. table_id=%ld", table_id);
            ret = OB_ERROR_OUT_OF_RANGE;
          }
        }
      }
      return ret;
    }

    bool ObRootTable2::table_is_exist(const uint64_t table_id) const
    {
      //common::ObString key;
      const_iterator first;
      const_iterator last;
      //const_iterator ptr;
      common::ObRange range;
      range.table_id_ = table_id;
      range.border_flag_.set_min_value();
      range.border_flag_.set_max_value();
      const common::ObTabletInfo *tablet_info = NULL;
      // if table is exist, we should find at lest one range with this table id
      bool ret = false;
      //if (OB_SUCCESS == find_key(table_id, key, 1, first, last, ptr))
      if (OB_SUCCESS == find_range(range, first, last))
      {
        if (first == last)
        {
          tablet_info = get_tablet_info(first);
          if (tablet_info != NULL && tablet_info->range_.table_id_ == table_id)
          {
            ret = true;
          }
        }
        else
        {
          ret = true;
        }
      }
      return ret;
    }

    void ObRootTable2::server_off_line(const int32_t server_index, const int64_t time_stamp)
    {
      int64_t count = 0;
      for (int32_t i = 0; i < (int32_t)meta_table_.get_array_index(); ++i)
      {
        for (int32_t j = 0; j < OB_SAFE_COPY_COUNT; j++)
        {
          if (data_holder_[i].server_info_indexes_[j] == server_index)
          {
            atomic_exchange((uint32_t*) &(data_holder_[i].server_info_indexes_[j]), (uint32_t)OB_INVALID_INDEX);
            if (time_stamp > 0 ) atomic_exchange((uint64_t*) &(data_holder_[i].last_dead_server_time_), time_stamp);
            count++;
          }
        }
      }
      TBSYS_LOG(INFO, "delete replicas, server_index=%d count=%ld", server_index, count);
      return;
    }

    void ObRootTable2::get_cs_version(const int64_t index, int64_t &version)
    {
      if (tablet_info_manager_ != NULL)
      {
        for (int32_t i = 0; i < meta_table_.get_array_index(); i++)
        {
          for (int32_t j = 0; j < OB_SAFE_COPY_COUNT; j++)
          {
            if (index == data_holder_[i].server_info_indexes_[j]) //the server is down
            {
              version = data_holder_[i].tablet_version_[j];
              break;
            }
          }
        }
      }
    }
    void ObRootTable2::dump() const
    {
      TBSYS_LOG(INFO, "meta_table_.size():%d", (int32_t)meta_table_.get_array_index());
      if (tablet_info_manager_ != NULL)
      {
        for (int32_t i = 0; i < meta_table_.get_array_index(); i++)
        {
          tablet_info_manager_->hex_dump(data_holder_[i].tablet_info_index_, TBSYS_LOG_LEVEL_INFO);
          data_holder_[i].dump();
        }
      }
      TBSYS_LOG(INFO, "dump meta_table_ complete");
      return;
    }

    void ObRootTable2::dump_cs_tablet_info(const int server_index, int64_t &tablet_num)const
    {
      if (NULL != tablet_info_manager_)
      {
        for (int32_t i = 0; i < meta_table_.get_array_index(); i++)
        {
          data_holder_[i].dump(server_index, tablet_num);
        }
      }
    }

    void ObRootTable2::dump_unusual_tablets(int64_t current_version, int32_t replicas_num, int32_t &num) const
    {
      num = 0;
      TBSYS_LOG(INFO, "meta_table_.size():%d", (int32_t)meta_table_.get_array_index());
      TBSYS_LOG(INFO, "current_version=%ld replicas_num=%d", current_version, replicas_num);
      if (tablet_info_manager_ != NULL)
      {
        for (int32_t i = 0; i < meta_table_.get_array_index(); i++)
        {
          int32_t new_num = 0;
          for (int32_t j = 0; j < OB_SAFE_COPY_COUNT; j++)
          {
            if (OB_INVALID_INDEX == data_holder_[i].server_info_indexes_[j]) //the server is down
            {
            }
            else if (data_holder_[i].tablet_version_[j] == current_version)
            {
              ++new_num;
            }
          }
          if (new_num < replicas_num)
          {
            // is an unusual tablet
            num++;
            tablet_info_manager_->hex_dump(data_holder_[i].tablet_info_index_, TBSYS_LOG_LEVEL_INFO);
            data_holder_[i].dump();
          }
        }
      }
      TBSYS_LOG(INFO, "dump meta_table_ complete, num=%d", num);
      return;
    }

    int ObRootTable2::check_tablet_version_merged(const int64_t tablet_version, bool &is_merged) const
    {
      int err = OB_SUCCESS;
      int32_t count = 0;
      is_merged = true;
      if (NULL == tablet_info_manager_)
      {
        TBSYS_LOG(WARN, "tablet_info_manager is null");
        is_merged = false;
        err = OB_ERROR;
      }
      else
      {
        for (int32_t i = 0; i < meta_table_.get_array_index(); i++)
        {
          int32_t succ_count = 0;
          int32_t fail_count = 0;
          for(int32_t j =0; j < OB_SAFE_COPY_COUNT; j++) 
          {
            //all online cs should be merged
            if (data_holder_[i].server_info_indexes_[j] != OB_INVALID_INDEX)
            {
              if (data_holder_[i].tablet_version_[j] < tablet_version)
              {
                TBSYS_LOG(DEBUG, "server[%d]'s tablet version[%ld] less than required[%ld]",
                    data_holder_[i].server_info_indexes_[j], data_holder_[i].tablet_version_[j],
                    tablet_version);
                is_merged = false;
                fail_count ++;
              }
              else
              {
                succ_count ++;
              }
            }
          }
          if (succ_count >= replica_num_)
          {
          }
          else
          {
            is_merged = false;
            count += fail_count;
          }
        }
      }
      TBSYS_LOG(INFO, "check tablet version. aready have %d tablet not merged to version[%ld]", count, tablet_version);
      return err;
    }
    const common::ObTabletInfo* ObRootTable2::get_tablet_info(const const_iterator& it) const
    {
      int32_t tablet_index = 0;
      const common::ObTabletInfo* tablet_info = NULL;
      if (NULL != it && it < end() && it >= begin() && NULL != tablet_info_manager_)
      {
        tablet_index = it->tablet_info_index_;
        tablet_info = tablet_info_manager_->get_tablet_info(tablet_index);
        if (NULL == tablet_info)
        {
          TBSYS_LOG(ERROR, "you should not reach here");
        }
      }
      return tablet_info;
    }
    common::ObTabletInfo* ObRootTable2::get_tablet_info(const const_iterator& it)
    {
      int32_t tablet_index = 0;
      common::ObTabletInfo* tablet_info = NULL;
      if (NULL != it && it < end() && it >= begin() && NULL != tablet_info_manager_)
      {
        tablet_index = it->tablet_info_index_;
        tablet_info = tablet_info_manager_->get_tablet_info(tablet_index);
        if (NULL == tablet_info)
        {
          TBSYS_LOG(ERROR, "you should not reach here");
        }
      }
      return tablet_info;
    }


    bool ObRootTable2::move_back(const int32_t from_index_inclusive, const int32_t move_step)
    {
      bool ret = false;
      int32_t data_count = static_cast<int32_t>(meta_table_.get_array_index());
      if ((move_step + data_count < ObTabletInfoManager::MAX_TABLET_COUNT) && from_index_inclusive <= data_count)
      {
        ret = true;
        memmove(&(data_holder_[from_index_inclusive + move_step]), //dest
            &(data_holder_[from_index_inclusive]), //src
            (data_count - from_index_inclusive) * sizeof(ObRootMeta2) ); //size
        meta_table_.init(ObTabletInfoManager::MAX_TABLET_COUNT, data_holder_, data_count + move_step);
        sorted_count_ += move_step;
      }
      if (!ret)
      {
        TBSYS_LOG(ERROR, "too much data, not enought sapce");
      }
      return ret;

    }
    int64_t ObRootTable2::get_max_tablet_version()
    {
      int64_t max_tablet_version = 0;
      int64_t tmp_version = 0;
      if (NULL != tablet_info_manager_)
      {
        for (const_iterator it = begin(); it != end(); it++)
        {
          tmp_version = get_max_tablet_version(it);
          if (tmp_version > max_tablet_version)
          {
            max_tablet_version = tmp_version;
          }

        }
      }
      return max_tablet_version;
    }
    int64_t ObRootTable2::get_max_tablet_version(const const_iterator& it)
    {
      int64_t max_tablet_version = 0;
      for (int32_t i = 0 ; i < OB_SAFE_COPY_COUNT; i++)
      {
        if (it->tablet_version_[i] > max_tablet_version)
        {
          max_tablet_version = it->tablet_version_[i];
        }
      }
      return max_tablet_version;
    }
    int ObRootTable2::modify(const const_iterator& it, const int32_t dest_server_index, const int64_t tablet_version)
    {
      int32_t found_it_index = OB_INVALID_INDEX;
      found_it_index = find_suitable_pos(it, dest_server_index, tablet_version);
      if (found_it_index != OB_INVALID_INDEX)
      {
        it->server_info_indexes_[found_it_index] = dest_server_index;
        it->tablet_version_[found_it_index] = tablet_version;
      }
      return OB_SUCCESS;
    }

    int ObRootTable2::replace(const const_iterator& it, 
        const int32_t src_server_index, const int32_t dest_server_index, 
        const int64_t tablet_version)
    {
      int ret = OB_SUCCESS;
      if (OB_INVALID_INDEX != src_server_index)
      {
        for (int32_t i = 0; i < OB_SAFE_COPY_COUNT; i++) 
        {
          if (it->server_info_indexes_[i] == src_server_index)
          {
            it->server_info_indexes_[i] = OB_INVALID_INDEX;
            break;
          }
        }
      }
      if (OB_INVALID_INDEX != dest_server_index)
      {
        ret = modify(it, dest_server_index, tablet_version);
      }
      return ret;
    }

    ObRootTable2::const_iterator ObRootTable2::lower_bound(const common::ObRange& range) const
    {
      const_iterator out = NULL; 
      if (sorted_count_ > 0 && tablet_info_manager_ != NULL )       
      {        
        int32_t len = sorted_count_;
        int32_t half = 0;
        int32_t first = 0;
        int32_t middle = 0;        
        int32_t tablet_index = 0;
        int32_t com = 0;        
        const common::ObTabletInfo* tablet_info = NULL;
        while (len > 0) 
        {
          half = len >> 1;
          middle = first + half;
          //get tablet info
          tablet_index = data_holder_[middle].tablet_info_index_;
          tablet_info = tablet_info_manager_->get_tablet_info(tablet_index);

          if (tablet_info != NULL)
          {
            com = tablet_info->range_.compare_with_endkey(range);
          }
          else
          {
            com = -1 ;
          }

          if (com < 0)
          {    
            first = middle;
            ++first;
            len = len - half - 1; 
          }    
          else 
          {
            len = half;
          }
        }
        out = data_holder_ + first;
      }
      return out;
    }
    ObRootTable2::iterator ObRootTable2::lower_bound(const common::ObRange& range)
    {
      iterator out = NULL; 
      if (sorted_count_ > 0 && tablet_info_manager_ != NULL )       
      {        
        int32_t len = sorted_count_;
        int32_t half = 0;
        int32_t first = 0;
        int32_t middle = 0;        
        int32_t tablet_index = 0;
        int32_t com = 0;        
        common::ObTabletInfo* tablet_info = NULL;
        while (len > 0) 
        {
          half = len >> 1;
          middle = first + half;
          //get tablet info
          tablet_index = data_holder_[middle].tablet_info_index_;
          tablet_info = tablet_info_manager_->get_tablet_info(tablet_index);

          if (tablet_info != NULL)
          {
            com = tablet_info->range_.compare_with_endkey(range);
          }
          else
          {
            com = -1 ;
          }

          if (com < 0)
          {    
            first = middle;
            ++first;
            len = len - half - 1; 
          }    
          else 
          {
            len = half;
          }
        }
        out = data_holder_ + first;
      }
      return out;
    }
    //only used in root server's init process
    int ObRootTable2::add(const common::ObTabletInfo& tablet, const int32_t server_index, const int64_t tablet_version)
    {
      int ret = OB_ERROR;
      if (tablet_info_manager_ != NULL)
      {
        ret = OB_SUCCESS;
        int32_t tablet_index = OB_INVALID_INDEX;
        ret = tablet_info_manager_->add_tablet_info(tablet, tablet_index);
        ObTabletCrcHistoryHelper *crc_helper = NULL;
        if (OB_SUCCESS == ret)
        {
          crc_helper = tablet_info_manager_->get_crc_helper(tablet_index);
          if (crc_helper != NULL)
          {
            crc_helper->reset();
            crc_helper->check_and_update(tablet_version, tablet.crc_sum_);
          }
          ObRootMeta2 meta;
          meta.tablet_info_index_ = tablet_index;
          meta.server_info_indexes_[0] = server_index;
          meta.tablet_version_[0] = tablet_version;
          meta.last_dead_server_time_ = 0;
          if (!meta_table_.push_back(meta))
          {
            TBSYS_LOG(ERROR, "add new tablet error have no enough space");
            ret = OB_ERROR;
          }
        }
      }
      return ret;
    }
int ObRootTable2::create_table(const common::ObTabletInfo& tablet, const int32_t* server_indexes, const int32_t replicas_num, const int64_t tablet_version)
    {
      int ret = OB_ERROR;
      int32_t insert_pos = -1;
      if (tablet_info_manager_ != NULL)
      {
        ret = OB_SUCCESS;
        const_iterator first;
        const_iterator last;
        ret = find_range(tablet.range_, first, last);
        if (OB_SUCCESS == ret && first == last)
        {
          insert_pos = static_cast<int32_t>(first-begin());
        }
        if (OB_ERROR_OUT_OF_RANGE == ret && -1 == insert_pos)
        {
          ret =OB_SUCCESS;
          insert_pos = static_cast<int32_t>(end() - begin());
        }
        if (OB_SUCCESS == ret)
        {
          TBSYS_LOG(DEBUG, "insert pos = %d", insert_pos);
          if(!move_back(insert_pos, 1))
          {
            TBSYS_LOG(ERROR, "too many tablet!!!");
            ret = OB_ERROR;
          }
        }
        int32_t tablet_index = OB_INVALID_INDEX;
        if (OB_SUCCESS == ret)
        {
          ret = tablet_info_manager_->add_tablet_info(tablet, tablet_index);
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(ERROR, "too many tablet info!!!");
          }
          else
          {
            ObTabletCrcHistoryHelper *crc_helper = NULL;
            crc_helper = tablet_info_manager_->get_crc_helper(tablet_index);
            if (crc_helper != NULL)
            {
              crc_helper->reset();
              crc_helper->check_and_update(tablet_version, tablet.crc_sum_);
            }
          }
        }
        ObRootMeta2 meta;
        if (OB_SUCCESS == ret)
        {
          meta.tablet_info_index_ = tablet_index;
          meta.last_dead_server_time_ = 0;
          for (int32_t i = 0; i < replicas_num; i++)
          {
            meta.server_info_indexes_[i] = server_indexes[i];
            meta.tablet_version_[i] = tablet_version;
          }
        }
        if (OB_SUCCESS == ret)
        {
          data_holder_[insert_pos] = meta;
        }
      }
      return ret;
    }
    bool ObRootTable2::add_lost_range() 
    {
      bool ret = true;
      ObRange last_range;
      last_range.table_id_ = OB_INVALID_ID;
      last_range.border_flag_.set_max_value();
      last_range.border_flag_.set_min_value();
      const ObTabletInfo* tablet_info = NULL;
      ObTabletInfo will_add;
      int32_t i = 0;
      while (i < (int32_t)meta_table_.get_array_index())
      {
        tablet_info = get_tablet_info(begin() + i);
        if (tablet_info == NULL)
        {
          TBSYS_LOG(ERROR, "invalid tablet info index is %d", i);
          ret = false;
          break;
        }
        will_add.range_ = tablet_info->range_;
        if (tablet_info->range_.table_id_ != OB_INVALID_ID)
        {
          if (tablet_info->range_.table_id_ != last_range.table_id_ )
          {
            if (!last_range.border_flag_.is_max_value())
            {
              TBSYS_LOG(ERROR, "table %lu not end correctly", last_range.table_id_);
              will_add.range_ = last_range;
              will_add.range_.start_key_ = last_range.end_key_;
              will_add.range_.border_flag_.unset_inclusive_start();
              will_add.range_.border_flag_.set_inclusive_end();
              will_add.range_.border_flag_.unset_min_value();
              will_add.range_.border_flag_.set_max_value();
              if (OB_SUCCESS != add_range(will_add, begin() + i, FIRST_TABLET_VERSION, OB_INVALID_INDEX))
              {
                ret = false;
                break;
              }
            }
            if (!tablet_info->range_.border_flag_.is_min_value())
            {
              TBSYS_LOG(ERROR, "table %lu not start correctly", tablet_info->range_.table_id_);
            }
          }
          else
          {
            if (tablet_info->range_.start_key_.compare(last_range.end_key_) != 0)
            {
              TBSYS_LOG(ERROR, "table %lu have a hole last range is ", last_range.table_id_);
              //last_range.hex_dump(TBSYS_LOG_LEVEL_INFO);
              static char row_key_dump_buff[OB_MAX_ROW_KEY_LENGTH * 2];
              last_range.to_string(row_key_dump_buff, OB_MAX_ROW_KEY_LENGTH * 2);
              TBSYS_LOG(INFO, "%s", row_key_dump_buff);
              TBSYS_LOG(INFO, "now is table %lu  range is ", tablet_info->range_.table_id_);
              //tablet_info->range_.hex_dump(TBSYS_LOG_LEVEL_INFO);
              tablet_info->range_.to_string(row_key_dump_buff, OB_MAX_ROW_KEY_LENGTH * 2);
              TBSYS_LOG(INFO, "%s", row_key_dump_buff);
              will_add.range_ = last_range;
              will_add.range_.start_key_ = last_range.end_key_;
              will_add.range_.end_key_ = tablet_info->range_.start_key_;
              will_add.range_.border_flag_.unset_inclusive_start();
              will_add.range_.border_flag_.set_inclusive_end();
              will_add.range_.border_flag_.unset_min_value();
              will_add.range_.border_flag_.unset_max_value();
              if (OB_SUCCESS != add_range(will_add, begin() + i, FIRST_TABLET_VERSION, OB_INVALID_INDEX))
              {
                ret = false;
                break;
              }
            }
          }
        }
        last_range = will_add.range_;
        ++i;
      }
      if (ret && meta_table_.get_array_index() > 0)
      {
        if (!last_range.border_flag_.is_max_value())
        {
          TBSYS_LOG(ERROR, "table %lu not end correctly", last_range.table_id_);
          will_add.range_ = last_range;
          will_add.range_.start_key_ = last_range.end_key_;
          will_add.range_.border_flag_.unset_inclusive_start();
          will_add.range_.border_flag_.set_inclusive_end();
          will_add.range_.border_flag_.unset_min_value();
          will_add.range_.border_flag_.set_max_value();
          if (OB_SUCCESS != add_range(will_add, end() , FIRST_TABLET_VERSION, OB_INVALID_INDEX))
          {
            ret = false;
          }
        }
      }
      return ret;
    }
    bool ObRootTable2::check_lost_range() 
    {
      bool ret = true;
      ObRange last_range;
      last_range.table_id_ = OB_INVALID_ID;
      last_range.border_flag_.set_max_value();
      last_range.border_flag_.set_min_value();
      const ObTabletInfo* tablet_info = NULL;
      int32_t i = 0;
      while (i < (int32_t)meta_table_.get_array_index())
      {
        tablet_info = get_tablet_info(begin() + i);
        if (tablet_info == NULL)
        {
          TBSYS_LOG(ERROR, "invalid tablet info index is %d", i);
          ret = false;
          break;
        }
        if (tablet_info->range_.table_id_ != OB_INVALID_ID)
        {
          if (tablet_info->range_.table_id_ != last_range.table_id_ )
          {
            if (!last_range.border_flag_.is_max_value())
            {
              TBSYS_LOG(WARN, "table %lu not end correctly", last_range.table_id_);
              ret = false;
              //break;
            }
            if (!tablet_info->range_.border_flag_.is_min_value())
            {
              TBSYS_LOG(WARN, "table %lu not start correctly", tablet_info->range_.table_id_);
              ret = false;
              //break;
            }
          }
          else
          {
            if (tablet_info->range_.start_key_.compare(last_range.end_key_) != 0)
            {
              static char row_key_dump_buff[OB_MAX_ROW_KEY_LENGTH * 2];
              last_range.to_string(row_key_dump_buff, OB_MAX_ROW_KEY_LENGTH * 2);
              TBSYS_LOG(WARN, "table not complete, table_id=%lu last_range=%s", 
                        last_range.table_id_, row_key_dump_buff);
              tablet_info->range_.to_string(row_key_dump_buff, OB_MAX_ROW_KEY_LENGTH * 2);
              TBSYS_LOG(WARN, "cur_table_id=%lu  curr_range=%s", 
                        tablet_info->range_.table_id_, row_key_dump_buff);
              ret = false;
              //break;
            }
          }
        }
        last_range = tablet_info->range_;
        ++i;
      }
      if (ret && meta_table_.get_array_index() > 0)
      {
        if (!last_range.border_flag_.is_max_value())
        {
          TBSYS_LOG(WARN, "table %lu not end correctly", last_range.table_id_);
          ret = false;
        }
      }
      return ret;
    }
    void ObRootTable2::sort()
    {
      if (tablet_info_manager_ != NULL)
      {
        ObRootMeta2CompareHelper sort_helper(tablet_info_manager_);
        std::sort(begin(), end(), sort_helper);
        sorted_count_ = static_cast<int32_t>(end() - begin());
      }
      return;
    }
    /*
     * root table第一次构造的时候使用
     * 整理合并相同的tablet, 生成一份新的root table
     */
    int ObRootTable2::shrink_to(ObRootTable2* shrunk_table, ObTabletReportInfoList &delete_list) 
    {
      int ret = OB_ERROR;
      static char row_key_dump_buff[OB_MAX_ROW_KEY_LENGTH * 2];
      if (shrunk_table != NULL && 
          shrunk_table->tablet_info_manager_ != NULL &&
          end() == sorted_end())
      {
        ret = OB_SUCCESS;
        ObRange last_range;
        last_range.table_id_ = OB_INVALID_ID;
        ObTabletInfo* tablet_info = NULL;
        ObString end_key;
        ObTabletInfo new_tablet_info;
        int32_t last_range_index = OB_INVALID_INDEX;
        int32_t last_tablet_index = OB_INVALID_INDEX;
        ObRootMeta2 root_meta;
        const_iterator it = begin();
        const_iterator it2 = begin();
        int cmp_ret = 0;
        ObTabletReportInfo to_delete;
        while (OB_SUCCESS == ret && it < end() )
        {
          tablet_info = get_tablet_info(it);
          if (NULL == tablet_info)
          {
            TBSYS_LOG(ERROR, "should never reach this bug occurs");
            ret = OB_ERROR;
            break;
          }
          last_range = tablet_info->range_;
          ret = shrunk_table->tablet_info_manager_->add_tablet_info(*tablet_info, last_range_index);
          ObTabletCrcHistoryHelper *last_tablet_crc_helper = NULL;
          if (OB_SUCCESS == ret)
          {
            last_tablet_crc_helper = shrunk_table->tablet_info_manager_->get_crc_helper(last_range_index);
            if (last_tablet_crc_helper != NULL)
            {
              last_tablet_crc_helper->reset();
              last_tablet_crc_helper->check_and_update(it->tablet_version_[0], tablet_info->crc_sum_);
            }
          }
          if (OB_SUCCESS == ret) 
          {
            root_meta = *it;
            root_meta.tablet_info_index_ = last_range_index;
            last_tablet_index = static_cast<int32_t>(shrunk_table->meta_table_.get_array_index());
            if (!shrunk_table->meta_table_.push_back(root_meta))
            {
              TBSYS_LOG(ERROR, "too much tablet");
              ret = OB_ERROR;
              break;
            }
          }
          //merge the same range
          while(tablet_info->range_.equal(last_range))
          {
            tablet_info->range_.to_string(row_key_dump_buff, OB_MAX_ROW_KEY_LENGTH * 2);
            TBSYS_LOG(DEBUG, "shrink idx=%ld range=%s", it-begin(), row_key_dump_buff);

            if (OB_SUCCESS != last_tablet_crc_helper->check_and_update(it->tablet_version_[0], tablet_info->crc_sum_))
            {
              TBSYS_LOG(ERROR, "crc check error,  crc=%lu last_crc=%lu last_cs=%d", 
                        tablet_info->crc_sum_, it->tablet_version_[0], it->server_info_indexes_[0]);
              TBSYS_LOG(ERROR, "error tablet is");
              last_range.to_string(row_key_dump_buff, OB_MAX_ROW_KEY_LENGTH * 2);
              TBSYS_LOG(ERROR, "%s", row_key_dump_buff);
              TBSYS_LOG(ERROR, "tabletinfo in shrink table is");

              shrunk_table->data_holder_[last_tablet_index].dump();
              //last_range.hex_dump(TBSYS_LOG_LEVEL_ERROR);
            }
            else
            {
              to_delete.tablet_location_.chunkserver_.set_port(0);
              merge_one_tablet(shrunk_table, last_tablet_index, it, to_delete);
              if (0 != to_delete.tablet_location_.chunkserver_.get_port())
              {
                if (OB_SUCCESS != delete_list.add_tablet(to_delete))
                {
                  TBSYS_LOG(WARN, "failed to add delete tablet");
                }
              }
            }
            it++;
            if (it >= end())
            {
              break;
            }
            tablet_info = get_tablet_info(it);
            if (NULL == tablet_info)
            {
              TBSYS_LOG(ERROR, "should never reach this bug occurs");
              ret = OB_ERROR;
              break;
            }
          } // end while same range
          //split next OB_SAFE_COPY_COUNT * 2 is enough
          if (OB_SUCCESS == ret)
          {
            for (it2 = it; it2 < it + OB_SAFE_COPY_COUNT * 2 && it2 < end() && OB_SUCCESS == ret; it2++)
            {
              tablet_info = get_tablet_info(it2);
              if (NULL == tablet_info)
              {
                TBSYS_LOG(ERROR, "should never reach this bug occurs");
                ret = OB_ERROR;
                break;
              }
              //compare start key;
              cmp_ret = last_range.compare_with_startkey(tablet_info->range_);
              if (cmp_ret > 0)
              {
                last_range.to_string(row_key_dump_buff, OB_MAX_ROW_KEY_LENGTH * 2);
                TBSYS_LOG(ERROR, "lost some tablet, last_range=%s", row_key_dump_buff);
                tablet_info->range_.to_string(row_key_dump_buff, OB_MAX_ROW_KEY_LENGTH * 2);
                TBSYS_LOG(ERROR, "this_range=%s", row_key_dump_buff);
                ret = OB_ERROR;
                break;
              }
              if (cmp_ret < 0)
              {
                //no need split
                continue;
              }
              // same start key, compare end key
              cmp_ret = last_range.compare_with_endkey(tablet_info->range_);
              if (cmp_ret > 0)
              {
                TBSYS_LOG(ERROR, "you have not sorted it? bugs!!");
                ret = OB_ERROR;
              }
              if (cmp_ret == 0)
              {
                TBSYS_LOG(ERROR, "we should skip this , bugs!!");
                last_range.to_string(row_key_dump_buff, OB_MAX_ROW_KEY_LENGTH * 2);
                TBSYS_LOG(ERROR, "last_range=%s", row_key_dump_buff);
                tablet_info->range_.to_string(row_key_dump_buff, OB_MAX_ROW_KEY_LENGTH * 2);
                TBSYS_LOG(ERROR, "this_range=%s", row_key_dump_buff);

                //no need split
                continue;
              }
              //split it2;
              TBSYS_LOG(ERROR, "in this version we sould not reach this init data have some error");
              last_range.to_string(row_key_dump_buff, OB_MAX_ROW_KEY_LENGTH * 2);
              TBSYS_LOG(ERROR, "last_range=%s", row_key_dump_buff);
              tablet_info->range_.to_string(row_key_dump_buff, OB_MAX_ROW_KEY_LENGTH * 2);
              TBSYS_LOG(ERROR, "this_range=%s", row_key_dump_buff);
              ret = OB_ERROR;
              break;
              merge_one_tablet(shrunk_table, last_tablet_index, it2, to_delete);
              tablet_info->range_.start_key_ = last_range.end_key_;
              tablet_info->range_.border_flag_.unset_min_value();
            } //end for next OB_SAFE_COPY_COUNT * 2
          }
        } //end while
      }
      return ret;
    }

    int32_t ObRootTable2::find_suitable_pos(const const_iterator& it, const int32_t server_index, const int64_t tablet_version, common::ObTabletReportInfo *to_delete/*=NULL*/)
    {
      int32_t found_it_index = OB_INVALID_INDEX;
      for (int32_t i = 0; i < OB_SAFE_COPY_COUNT; i++)
      {
        if (it->server_info_indexes_[i] == server_index)
        {
          found_it_index = i;
          break;
        }
      }
      if (OB_INVALID_INDEX == found_it_index)
      {
        for (int32_t i = 0; i < OB_SAFE_COPY_COUNT; i++)
        {
          if (it->server_info_indexes_[i] == OB_INVALID_INDEX)
          {
            found_it_index = i;
            break;
          }
        }
      }
      if (OB_INVALID_INDEX == found_it_index)
      {
        int64_t min_tablet_version = tablet_version;
        for (int32_t i = 0; i < OB_SAFE_COPY_COUNT; i++)
        {
          if (it->tablet_version_[i] < min_tablet_version)
          {
            min_tablet_version = it->tablet_version_[i];
            found_it_index = i;
          }
        }
        if (OB_INVALID_INDEX != found_it_index && NULL != to_delete)
        {
          to_delete->tablet_location_.tablet_version_ = it->tablet_version_[found_it_index];
          to_delete->tablet_location_.chunkserver_.set_port(it->server_info_indexes_[found_it_index]);
        }
      }
      return found_it_index;
    }

    int ObRootTable2::check_tablet_version(const int64_t tablet_version, int safe_copy_count) const
    {
      int ret = OB_SUCCESS;
      int tablet_ok = 0;
      const common::ObTabletInfo* tablet_info = NULL;
      for (int32_t i = 0; i < meta_table_.get_array_index(); i++)
      {
        tablet_ok = 0;
        for (int32_t j = 0; j < OB_SAFE_COPY_COUNT; j++)
        {
          if (data_holder_[i].tablet_version_[j] >= tablet_version)
          {
            ++tablet_ok;
          }
        }
        if (tablet_ok < safe_copy_count)
        {
          tablet_info = get_tablet_info(begin() + i);
          if (tablet_info != NULL)
          {
            TBSYS_LOG(WARN, "tablet version error now version is %ld, tablet is", tablet_version);
            static char row_key_dump_buff[OB_MAX_ROW_KEY_LENGTH * 2];
            tablet_info->range_.to_string(row_key_dump_buff, OB_MAX_ROW_KEY_LENGTH * 2);
            TBSYS_LOG(INFO, "%s", row_key_dump_buff);
            //tablet_info->range_.hex_dump(TBSYS_LOG_LEVEL_INFO);
            for (int32_t j = 0; j < OB_SAFE_COPY_COUNT; j++)
            {
              TBSYS_LOG(INFO, "root table's tablet version is %ld", data_holder_[i].tablet_version_[j]);
            }
            ret = OB_ERROR;
          }
        }
      }
      return ret;
    }

    ObTabletCrcHistoryHelper* ObRootTable2::get_crc_helper(const const_iterator& it)
    {
      ObTabletCrcHistoryHelper *ret = NULL;
      if (NULL != it && it < end() && it >= begin() && NULL != tablet_info_manager_)
      {
        int32_t tablet_index = it->tablet_info_index_;
        ret = tablet_info_manager_->get_crc_helper(tablet_index);
      }
      return ret;
    }

    bool ObRootTable2::check_tablet_copy_count(const int32_t copy_count) const
    {
      bool ret = true;
      int tablet_ok = 0;
      const common::ObTabletInfo* tablet_info = NULL;
      for (int32_t i = 0; i < meta_table_.get_array_index(); i++)
      {
        tablet_ok = 0;
        for (int32_t j = 0; j < OB_SAFE_COPY_COUNT; j++)
        {
          if (data_holder_[i].server_info_indexes_[j] != OB_INVALID_INDEX)
          {
            ++tablet_ok;
          }
        }
        if (tablet_ok < copy_count)
        {
          tablet_info = get_tablet_info(begin() + i);
          if (tablet_info != NULL)
          {
            TBSYS_LOG(WARN, "tablet copy count less than %d", copy_count);
            static char row_key_dump_buff[OB_MAX_ROW_KEY_LENGTH * 2];
            tablet_info->range_.to_string(row_key_dump_buff, OB_MAX_ROW_KEY_LENGTH * 2);
            TBSYS_LOG(INFO, "%s", row_key_dump_buff);
            //tablet_info->range_.hex_dump(TBSYS_LOG_LEVEL_INFO);
          }
          ret = false;
        }
      }
      return ret;
    }


    void ObRootTable2::merge_one_tablet(ObRootTable2* shrunk_table, int32_t last_tablet_index, const_iterator it, ObTabletReportInfo &to_delete)
    {
      to_delete.tablet_location_.chunkserver_.set_port(0);
      int32_t i = 0;
      for (i = 0; i < OB_SAFE_COPY_COUNT; ++i)
      {
        if (shrunk_table->data_holder_[last_tablet_index].server_info_indexes_[i] == OB_INVALID_INDEX ||
            shrunk_table->data_holder_[last_tablet_index].server_info_indexes_[i] == it->server_info_indexes_[0])
        {
          shrunk_table->data_holder_[last_tablet_index].server_info_indexes_[i] = it->server_info_indexes_[0];
          shrunk_table->data_holder_[last_tablet_index].tablet_version_[i] = it->tablet_version_[0];
          break;
        }
      }
      if (OB_SAFE_COPY_COUNT == i)
      {
        const ObTabletInfo *tablet_info = shrunk_table->get_tablet_info(&shrunk_table->data_holder_[last_tablet_index]);
        for (i = 0; i < OB_SAFE_COPY_COUNT; ++i)
        {
          if (shrunk_table->data_holder_[last_tablet_index].tablet_version_[i] < it->tablet_version_[0])
          {
            to_delete.tablet_location_.tablet_version_ = shrunk_table->data_holder_[last_tablet_index].tablet_version_[i];
            // reinterprete port_ to present server_index
            to_delete.tablet_location_.chunkserver_.set_port(shrunk_table->data_holder_[last_tablet_index].server_info_indexes_[i]);
            
            shrunk_table->data_holder_[last_tablet_index].server_info_indexes_[i] = it->server_info_indexes_[0];
            shrunk_table->data_holder_[last_tablet_index].tablet_version_[i] = it->tablet_version_[0];
            break;
          }
        }
        if (OB_SAFE_COPY_COUNT == i)
        {
          to_delete.tablet_location_.tablet_version_ = it->tablet_version_[0];
          to_delete.tablet_location_.chunkserver_.set_port(it->server_info_indexes_[0]);
        }
        TBSYS_LOG(DEBUG, "delete replica, cs_idx=%d", to_delete.tablet_location_.chunkserver_.get_port());
        to_delete.tablet_info_ = *tablet_info;
      }
      return;
    }

    /*
     * 得到range会对root table产生的影响
     * 一个range可能导致root table分裂 合并 无影响等
     */
    int ObRootTable2::get_range_pos_type(const common::ObRange& range, const const_iterator& first, const const_iterator& last) const
    {
      int ret = POS_TYPE_ERROR;
      const common::ObTabletInfo* tablet_info = NULL;
      tablet_info = this->get_tablet_info(first);
      if (tablet_info == NULL)
      {
        TBSYS_LOG(ERROR, "you should not reach this bugs");
      }
      else
      {
        if (range.compare_with_startkey(tablet_info->range_) < 0 )
        {

          if (range.table_id_ < tablet_info->range_.table_id_)
          {
            ret = POS_TYPE_ADD_RANGE;
          }
          if (POS_TYPE_ERROR == ret)
          {
            if (!range.border_flag_.is_max_value() && range.end_key_ <= tablet_info->range_.start_key_) 
            {
              ret = POS_TYPE_ADD_RANGE;
            }
          }
          if (POS_TYPE_ADD_RANGE == ret)
          {
            //range is less than first, we should check if range is large than prev
            if (first != begin())
            {
              const const_iterator prev = first - 1;
              const common::ObTabletInfo* tablet_info = NULL;
              tablet_info = this->get_tablet_info(prev);
              if (tablet_info == NULL)
              {
                TBSYS_LOG(ERROR, "you should not reach this bugs");
              }
              else
              {
                if (range.table_id_ == tablet_info->range_.table_id_)
                {
                  if (tablet_info->range_.border_flag_.is_max_value() || 
                      range.border_flag_.is_min_value() ||
                      range.start_key_ < tablet_info->range_.end_key_)
                  {
                    ret = POS_TYPE_ERROR;
                  }
                }
              }
            }
          }
        }
        else if (first == last)
        {
          //range in one tablet
          if (range.equal(tablet_info->range_))
          {
            ret = POS_TYPE_SAME_RANGE;
          }
          else
          {
            ret = POS_TYPE_SPLIT_RANGE;
          }
        }
        else
        {
          if (range.compare_with_startkey(tablet_info->range_) == 0)
          {
            tablet_info = this->get_tablet_info(last);
            if (tablet_info == NULL)
            {
              TBSYS_LOG(ERROR, "you should not reach this bugs");
            }
            else if (range.compare_with_endkey(tablet_info->range_) == 0)
            {
              ret = POS_TYPE_MERGE_RANGE;
            }
          }
        }
        if (ret == POS_TYPE_ERROR)
        {
          TBSYS_LOG(ERROR, "error range found");
          TBSYS_LOG(INFO, "tablet rang is :");
          //tablet_info->range_.hex_dump(TBSYS_LOG_LEVEL_INFO);
          static char row_key_dump_buff[OB_MAX_ROW_KEY_LENGTH * 2];
          tablet_info->range_.to_string(row_key_dump_buff, OB_MAX_ROW_KEY_LENGTH * 2);
          TBSYS_LOG(INFO, "%s", row_key_dump_buff);
          TBSYS_LOG(INFO, "reported rang is :");
          range.to_string(row_key_dump_buff, OB_MAX_ROW_KEY_LENGTH * 2);
          TBSYS_LOG(INFO, "%s", row_key_dump_buff);
          //range.hex_dump(TBSYS_LOG_LEVEL_INFO);
        }
      }

      return ret;
    }
    int ObRootTable2::split_range(const common::ObTabletInfo& reported_tablet_info, const const_iterator& pos, 
        const int64_t tablet_version, const int32_t server_index)
    {
      int ret = OB_ERROR;
      ObRange range = reported_tablet_info.range_;
      ObTabletInfo will_add_table;
      will_add_table = reported_tablet_info;
      ObTabletCrcHistoryHelper* crc_helper = NULL;
      if (get_range_pos_type(range, pos, pos) == POS_TYPE_SPLIT_RANGE)
      {
        ret = OB_SUCCESS;
        common::ObTabletInfo* tablet_info = NULL;
        tablet_info = get_tablet_info(pos);
        if (tablet_info == NULL || tablet_info_manager_ == NULL)
        {
          TBSYS_LOG(ERROR, "you should not reach this bugs!!");
          ret = OB_ERROR;
        }
        else
        {
          int split_type = SPLIT_TYPE_ERROR;
          int cmp =range.compare_with_startkey(tablet_info->range_);
          if (cmp == 0) 
          {
            //new range is top half of root table range
            //will split to two ranges
            split_type = SPLIT_TYPE_TOP_HALF;
          }
          else if (cmp > 0)
          {
            cmp = range.compare_with_endkey(tablet_info->range_);
            if (cmp == 0)
            {
              //this range is bottom half of root table range
              //will split to two ranges;
              split_type = SPLIT_TYPE_BOTTOM_HALF;
            }
            else if (cmp < 0 )
            {
              //this range is middle half of root table range
              //will split to three ranges;
              split_type = SPLIT_TYPE_MIDDLE_HALF;
            }
          }
          int32_t from_pos_inclusive = static_cast<int32_t>(pos - begin());
          if (split_type == SPLIT_TYPE_TOP_HALF || split_type == SPLIT_TYPE_BOTTOM_HALF)
          {
            if (from_pos_inclusive <  meta_table_.get_array_index() )
            {
              if (!move_back(from_pos_inclusive, 1))
              {
                TBSYS_LOG(ERROR, "move back error");
                ret = OB_ERROR;
              }
            }
            else
            {
              TBSYS_LOG(ERROR, "why you reach this bugss!!");
              ret = OB_ERROR;
            }
            if (OB_SUCCESS == ret)
            {
              ObRange splited_range_top = tablet_info->range_;
              ObRange splited_range_bottom = tablet_info->range_;
              if (split_type == SPLIT_TYPE_TOP_HALF)
              {
                splited_range_top.end_key_ = range.end_key_;
              }
              else
              {
                splited_range_top.end_key_ = range.start_key_;
              }
              splited_range_top.border_flag_.unset_max_value();
              // set inclusive end anyway
              splited_range_top.border_flag_.set_inclusive_end();
              splited_range_bottom.border_flag_.set_inclusive_end();
              int32_t out_index_top = OB_INVALID_INDEX;
              int32_t out_index_bottom = OB_INVALID_INDEX;
              //not clone start key clone end key
              will_add_table.range_ = splited_range_top;

              ret = tablet_info_manager_->add_tablet_info(will_add_table, out_index_top, false, true);
              crc_helper = tablet_info_manager_->get_crc_helper(out_index_top);
              if (crc_helper != NULL)
              {
                crc_helper->reset();
                if (split_type == SPLIT_TYPE_TOP_HALF)
                {
                  crc_helper->check_and_update(tablet_version, reported_tablet_info.crc_sum_);
                }
              }

              const ObTabletInfo* tablet_info_added = NULL;
              if (OB_SUCCESS == ret)
              {
                tablet_info_added = tablet_info_manager_->get_tablet_info(out_index_top);
                splited_range_bottom.start_key_ = tablet_info_added->range_.end_key_;//so we do not need alloc mem for key
              }
              splited_range_bottom.border_flag_.unset_min_value();
              if (OB_SUCCESS == ret)
              {
                will_add_table.range_ = splited_range_bottom;
                ret = tablet_info_manager_->add_tablet_info(will_add_table, out_index_bottom, false, false);
                crc_helper = tablet_info_manager_->get_crc_helper(out_index_bottom);
                if (crc_helper != NULL)
                {
                  crc_helper->reset();
                  if (split_type != SPLIT_TYPE_TOP_HALF)
                  {
                    crc_helper->check_and_update(tablet_version, reported_tablet_info.crc_sum_);
                  }
                }
              }
              if (OB_SUCCESS == ret)
              {
                data_holder_[from_pos_inclusive].tablet_info_index_ = out_index_top;
                data_holder_[from_pos_inclusive + 1].tablet_info_index_ = out_index_bottom;
                int32_t new_range_index = 0;
                if (split_type == SPLIT_TYPE_TOP_HALF) 
                {
                  new_range_index = from_pos_inclusive;
                }
                else
                {
                  new_range_index = from_pos_inclusive + 1;
                }
                int32_t found_index = find_suitable_pos(begin() + new_range_index, server_index, tablet_version);
                if (OB_INVALID_INDEX != found_index)
                {
                  data_holder_[new_range_index].server_info_indexes_[found_index] = server_index;
                  data_holder_[new_range_index].tablet_version_[found_index] = tablet_version;
                }
              }
            }
          }
          else if (split_type == SPLIT_TYPE_MIDDLE_HALF)
          {
            //will split to three range
            if (from_pos_inclusive <  meta_table_.get_array_index() )
            {
              if (!move_back(from_pos_inclusive, 2))
              {
                TBSYS_LOG(ERROR, "too many tablet");
                ret = OB_ERROR;
              }
            }
            else
            {
              TBSYS_LOG(ERROR, "why we got this bugss!");
              ret = OB_ERROR;
            }
            if (OB_SUCCESS == ret)
            {
              data_holder_[from_pos_inclusive+1] = data_holder_[from_pos_inclusive];
              data_holder_[from_pos_inclusive+2] = data_holder_[from_pos_inclusive];

              const ObTabletInfo* tablet_info_added = NULL;
              ObRange splited_range_middle = range; 
              ObRange splited_range_bottom = tablet_info->range_;
              ObRange splited_range_top = tablet_info->range_;
              int32_t out_index_top = OB_INVALID_INDEX;
              int32_t out_index_middle = OB_INVALID_INDEX;
              int32_t out_index_bottom = OB_INVALID_INDEX;
              // set inclusive end anyway
              splited_range_top.border_flag_.set_inclusive_end();
              splited_range_bottom.border_flag_.set_inclusive_end();
              splited_range_middle.border_flag_.set_inclusive_end();
              
              if (OB_SUCCESS == ret)
              {
                //clone end key, not clone start key
                will_add_table.range_ = splited_range_middle;
                will_add_table.crc_sum_ = reported_tablet_info.crc_sum_; //no use any more
                ret = tablet_info_manager_->add_tablet_info(will_add_table, out_index_middle,true,true );
                crc_helper = tablet_info_manager_->get_crc_helper(out_index_middle);
                if (crc_helper != NULL)
                {
                  crc_helper->reset();
                  crc_helper->check_and_update(tablet_version, reported_tablet_info.crc_sum_);
                }
              }
              if (OB_SUCCESS == ret)
              {
                tablet_info_added = tablet_info_manager_->get_tablet_info(out_index_middle);
                splited_range_top.end_key_ = tablet_info_added->range_.start_key_;
                splited_range_top.border_flag_.unset_max_value();
                will_add_table.range_ = splited_range_top;
                will_add_table.crc_sum_ = 0; //no use any more
                ret = tablet_info_manager_->add_tablet_info(will_add_table, out_index_top, false, false);
                crc_helper = tablet_info_manager_->get_crc_helper(out_index_top);
                if (crc_helper != NULL)
                {
                  crc_helper->reset();
                }
              }
              if (OB_SUCCESS == ret)
              {
                splited_range_bottom.start_key_ = tablet_info_added->range_.end_key_;
                splited_range_top.border_flag_.unset_min_value();
                will_add_table.range_ = splited_range_bottom;
                will_add_table.crc_sum_ = 0; //no use any more
                ret = tablet_info_manager_->add_tablet_info(will_add_table, out_index_bottom, false, false);
                crc_helper = tablet_info_manager_->get_crc_helper(out_index_bottom);
                if (crc_helper != NULL)
                {
                  crc_helper->reset();
                }
              }
              if (OB_SUCCESS == ret)
              {
                data_holder_[from_pos_inclusive].tablet_info_index_ = out_index_top;
                data_holder_[from_pos_inclusive + 1].tablet_info_index_ = out_index_middle;
                data_holder_[from_pos_inclusive + 2].tablet_info_index_ = out_index_bottom;
                int32_t new_range_index = from_pos_inclusive + 1;
                int32_t found_index = find_suitable_pos(begin() + from_pos_inclusive + 1, server_index, tablet_version);
                if (OB_INVALID_INDEX != found_index)
                {
                  data_holder_[new_range_index].server_info_indexes_[found_index] = server_index;
                  data_holder_[new_range_index].tablet_version_[found_index] = tablet_version;
                }
              }
            }
          }
          else
          {
            TBSYS_LOG(ERROR, "you should not reach this bugs!!");
            ret = OB_ERROR;
          }

        }
      }
      return ret;
    }
    int ObRootTable2::add_range(const common::ObTabletInfo& reported_tablet_info, const const_iterator& pos, 
        const int64_t tablet_version, const int32_t server_index)
    {
      int ret = OB_ERROR;
      ObRange range = reported_tablet_info.range_;
      ObTabletInfo will_add_table;
      will_add_table = reported_tablet_info;
      if (pos == end() || get_range_pos_type(range, pos, pos) == POS_TYPE_ADD_RANGE)
      {
        ret = OB_SUCCESS;
        int32_t from_pos_inclusive = static_cast<int32_t>(pos - begin());
        if (!move_back(from_pos_inclusive, 1))
        {
          ret = OB_ERROR;
        }
        if (OB_SUCCESS == ret)
        {
          int32_t out_index = OB_INVALID_INDEX;
          ret = tablet_info_manager_->add_tablet_info(will_add_table, out_index, true, true);
          if (OB_SUCCESS == ret)
          {
            ObTabletCrcHistoryHelper *crc_helper = NULL;
            crc_helper = tablet_info_manager_->get_crc_helper(out_index);
            if (crc_helper != NULL)
            {
              crc_helper->reset();
              crc_helper->check_and_update(tablet_version, will_add_table.crc_sum_);
            }
          }
          if (OB_SUCCESS == ret)
          {
            ObRootMeta2 meta;
            meta.tablet_info_index_ = out_index;
            meta.server_info_indexes_[0] = server_index;
            meta.tablet_version_[0] = tablet_version;
            meta.last_dead_server_time_ = 0;
            data_holder_[from_pos_inclusive] = meta;
          }
        }

      }
      return ret;
    }

    int ObRootTable2::write_to_file(const char* filename)
    {
      int ret = OB_SUCCESS;
      if (filename == NULL)
      {
        ret = OB_INVALID_ARGUMENT;
        TBSYS_LOG(INFO, "file name can not be NULL");
      }

      if (tablet_info_manager_ == NULL)
      {
        TBSYS_LOG(ERROR, "tablet_info_manager should not be NULL bugs!!");
        ret = OB_ERROR;
      }
      if (ret == OB_SUCCESS)
      {
        int64_t total_size = 0;
        int64_t total_count = meta_table_.get_array_index();
        // cal total size
        total_size += tablet_info_manager_->get_serialize_size();
        total_size += serialization::encoded_length_vi64(total_count);
        total_size += serialization::encoded_length_vi32(sorted_count_);
        for(int64_t i=0; i<total_count; ++i)
        {
          total_size += data_holder_[i].get_serialize_size();
        }

        // allocate memory
        char* data_buffer = static_cast<char*>(ob_malloc(total_size));
        if (data_buffer == NULL)
        {
          ret = OB_ERROR;
          TBSYS_LOG(ERROR, "allocate memory failed.");
        }

        // serialization
        if (ret == OB_SUCCESS)
        {
          common::ObDataBuffer buffer(data_buffer, total_size);
          ret = tablet_info_manager_->serialize(buffer.get_data(), buffer.get_capacity(), buffer.get_position());
          if (ret == OB_SUCCESS)
          {
            ret = serialization::encode_vi64(buffer.get_data(), buffer.get_capacity(), buffer.get_position(), total_count);
          }
          if (ret == OB_SUCCESS)
          {
            ret = serialization::encode_vi32(buffer.get_data(), buffer.get_capacity(), buffer.get_position(), sorted_count_);
          }
          if (ret == OB_SUCCESS)
          {
            for(int64_t i=0; i<total_count; ++i)
            {
              ret = data_holder_[i].serialize(buffer.get_data(), buffer.get_capacity(), buffer.get_position());
              if (ret != OB_SUCCESS) break;
            }
          }
        }

        // header
        int64_t header_length = sizeof(common::ObRecordHeader);
        int64_t pos = 0;
        char header_buffer[header_length];
        if (ret == OB_SUCCESS)
        {
          common::ObRecordHeader header;

          header.set_magic_num(ROOT_TABLE_MAGIC);
          header.header_length_ = static_cast<int16_t>(header_length);
          header.version_ = 0;
          header.reserved_ = 0;

          header.data_length_ = static_cast<int32_t>(total_size);
          header.data_zlength_ = static_cast<int32_t>(total_size);

          header.data_checksum_ = common::ob_crc64(data_buffer, total_size);
          header.set_header_checksum();

          ret = header.serialize(header_buffer, header_length, pos);
          if (ret != OB_SUCCESS)
          {
            TBSYS_LOG(ERROR, "serialization file header failed");
          }
        }

        // write into file
        if (ret == OB_SUCCESS)
        {
          // open file
          common::FileUtils fu;
          int32_t rc = fu.open(filename, O_CREAT | O_WRONLY | O_TRUNC, 0644);
          if (rc < 0)
          {
            ret = OB_ERROR;
            TBSYS_LOG(ERROR, "create file [%s] failed", filename);
          }

          if (ret == OB_SUCCESS)
          {
            int64_t wl = fu.write(header_buffer, header_length);
            if (wl != header_length)
            {
              ret = OB_ERROR;
              TBSYS_LOG(ERROR, "write header into [%s] failed", filename);
            }
          }

          if (ret == OB_SUCCESS)
          {
            int64_t wl = fu.write(data_buffer, total_size);
            if (wl != total_size)
            {
              ret = OB_ERROR;
              TBSYS_LOG(ERROR, "write data into [%s] failed", filename);
            }
            else
            {
              TBSYS_LOG(DEBUG, "root table has been write into [%s]", filename);
            }
          }

          fu.close();
        }
        if (data_buffer != NULL)
        {
          ob_free(data_buffer);
          data_buffer = NULL;
        }
      }

      return ret;
    }
    int ObRootTable2::read_from_file(const char* filename)
    {
      int ret = OB_SUCCESS;
      if (filename == NULL)
      {
        ret = OB_INVALID_ARGUMENT;
        TBSYS_LOG(INFO, "filename can not be NULL");
      }
      if (tablet_info_manager_ == NULL)
      {
        TBSYS_LOG(ERROR, "tablet_info_manager should not be NULL bugs!!");
        ret = OB_ERROR;
      }

      common::FileUtils fu;
      if (ret == OB_SUCCESS)
      {
        int32_t rc = fu.open(filename, O_RDONLY);
        if (rc < 0)
        {
          ret = OB_ERROR;
          TBSYS_LOG(ERROR, "open file [%s] failed", filename);
        }
      }

      TBSYS_LOG(DEBUG, "read root table from [%s]", filename);
      int64_t header_length = sizeof(common::ObRecordHeader);
      common::ObRecordHeader header;
      if (ret == OB_SUCCESS)
      {
        char header_buffer[header_length];
        int64_t rl = fu.read(header_buffer, header_length);
        if (rl != header_length)
        {
          ret = OB_ERROR;
          TBSYS_LOG(ERROR, "read header from [%s] failed", filename);
        }

        if (ret == OB_SUCCESS)
        {
          int64_t pos = 0;
          ret = header.deserialize(header_buffer, header_length, pos);
        }

        if (ret == OB_SUCCESS)
        {
          ret = header.check_header_checksum();
        }
      }
      TBSYS_LOG(DEBUG, "header read success ret: %d", ret);

      char* data_buffer = NULL;
      int64_t size = 0;
      if (ret == OB_SUCCESS)
      {
        size = header.data_length_;
        data_buffer = static_cast<char*>(ob_malloc(size));
        if (data_buffer == NULL)
        {
          ret = OB_ERROR;
          TBSYS_LOG(ERROR, "allocate memory failed");
        }
      }

      if (ret == OB_SUCCESS)
      {
        int64_t rl = fu.read(data_buffer, size);
        if (rl != size)
        {
          ret = OB_ERROR;
          TBSYS_LOG(ERROR, "read data from file [%s] failed", filename);
        }
      }

      if (ret == OB_SUCCESS)
      {
        int cr = common::ObRecordHeader::check_record(header, data_buffer, size, ROOT_TABLE_MAGIC);
        if (cr != OB_SUCCESS)
        {
          ret = OB_DESERIALIZE_ERROR;
          TBSYS_LOG(ERROR, "data check failed, rc: %d", cr);
        }
      }

      int64_t count = 0;
      common::ObDataBuffer buffer(data_buffer, size);
      if (ret == OB_SUCCESS)
      {
        ret = tablet_info_manager_->deserialize(buffer.get_data(), buffer.get_capacity(), buffer.get_position());
      }
      if (ret == OB_SUCCESS)
      {
        ret = serialization::decode_vi64(buffer.get_data(), buffer.get_capacity(), buffer.get_position(), &count);
      }
      if (ret == OB_SUCCESS)
      {
        ret = serialization::decode_vi32(buffer.get_data(), buffer.get_capacity(), buffer.get_position(), &sorted_count_);
      }

      if (ret == OB_SUCCESS)
      {
        ObRootMeta2 record;
        for (int64_t i=0; i<count; ++i)
        {
          data_holder_[i].deserialize(buffer.get_data(), buffer.get_capacity(), buffer.get_position());
          if (ret != OB_SUCCESS)
          {
            TBSYS_LOG(ERROR, "record deserialize failed");
            break;
          }
        }
        meta_table_.init(ObTabletInfoManager::MAX_TABLET_COUNT, data_holder_, count);

      }

      fu.close();
      if (data_buffer != NULL)
      {
        ob_free(data_buffer);
        data_buffer = NULL;
      }

      return ret;
    }
    void ObRootTable2::dump_as_hex(FILE* stream) const
    {
      fprintf(stream, "root_table_size %ld sorted_count_ %d\n", meta_table_.get_array_index(), sorted_count_);
      ObRootTable2::const_iterator it = this->begin();
      for (; it != this->end(); it++)
      {
        it->dump_as_hex(stream);
      }
    }
    void ObRootTable2::read_from_hex(FILE* stream)
    {
      int64_t size = 0;
      fscanf(stream, "root_table_size %ld sorted_count_ %d", &size, &sorted_count_);
      fscanf(stream, "\n");
      for (int64_t i = 0 ; i < size; i++)
      {
        data_holder_[i].read_from_hex(stream);
      }
      meta_table_.init(ObTabletInfoManager::MAX_TABLET_COUNT, data_holder_, size);
    }

    int ObRootTable2::shrink()
    {
      int ret = OB_SUCCESS;
      int32_t new_count = 0;
      iterator it_invalid = NULL;
      for (iterator it = begin(); sorted_end() != it; ++it)
      {
        if (OB_INVALID_INDEX == it->tablet_info_index_)
        {
          if (NULL == it_invalid)
          {
            it_invalid = it;
          }
        }
        else
        {
          new_count++;
          if (NULL != it_invalid)
          {
            *it_invalid = *it;
            it_invalid++;
          }
        }
      } // end for
      sorted_count_ = new_count;
      meta_table_.init(ObTabletInfoManager::MAX_TABLET_COUNT, data_holder_, sorted_count_);
      TBSYS_LOG(INFO, "tablet num after shrink, num=%d", sorted_count_);
      return ret;
    }

    int ObRootTable2::delete_tables(const common::ObArray<uint64_t> &deleted_tables)
    {
      int ret = OB_SUCCESS;
      char buf[OB_MAX_ROW_KEY_LENGTH * 2];
      // mark deleted
      ObRootTable2::iterator it;
      ObTabletInfo* tablet = NULL;
      for (it = this->begin(); it != this->sorted_end(); ++it)
      {
        tablet = this->get_tablet_info(it);
        OB_ASSERT(NULL != tablet);
        for (int j = 0; j < deleted_tables.count(); ++j)
        {
          // for each deleted table
          if (tablet->range_.table_id_ == deleted_tables.at(j))
          {
            it->tablet_info_index_ = OB_INVALID_INDEX;
            tablet->range_.to_string(buf, OB_MAX_ROW_KEY_LENGTH * 2);
            TBSYS_LOG(INFO, "delete tablets from roottable, range=%s", buf);
            break;
          }
        }
      } // end for
      if (OB_SUCCESS == ret
          && OB_SUCCESS != (ret = this->shrink()))
      {
        TBSYS_LOG(WARN, "failed to shrink table, err=%d", ret);
      }
      return ret;
    }
  }
}
