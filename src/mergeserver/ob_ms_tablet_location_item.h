/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: ob_ms_tablet_location_item.h,v 0.1 2010/09/26 14:01:30 zhidong Exp $
 *
 * Authors:
 *   chuanhui <xielun.szd@taobao.com>
 *     - some work details if you want
 *
 */

#ifndef OB_MERGER_TABLET_LOCATION_ITEM_H_
#define OB_MERGER_TABLET_LOCATION_ITEM_H_

#include "common/ob_string_buf.h"
#include "common/ob_tablet_info.h"

using namespace oceanbase::common;

namespace oceanbase
{
  namespace mergeserver
  {
    /// server and access err times
    struct ObMergerTabletLocation
    {
      static const int64_t MAX_ERR_TIMES = 30;
      int64_t err_times_;
      common::ObTabletLocation server_;
      ObMergerTabletLocation() : err_times_(0) {}
    };
    
    /// tablet location item info
    class ObMergerTabletLocationList 
    {
    public:
      ObMergerTabletLocationList();
      virtual ~ObMergerTabletLocationList();

    public:
      static const int32_t MAX_REPLICA_COUNT = 3;
      /// operator = the static content not contain the range buffer
      ObMergerTabletLocationList & operator = (const ObMergerTabletLocationList & other);

      /// add a tablet location to item list
      int add(const common::ObTabletLocation & location);

      /// del the index pos TabletLocation
      int del(const int64_t index, ObMergerTabletLocation & location);

      /// set item invalid status
      int set_item_invalid(const ObMergerTabletLocation & location);

      /// set item valid status
      void set_item_valid(const int64_t timestamp);

      /// get valid item count
      int64_t get_valid_count(void) const;

      /// operator random access
      ObMergerTabletLocation & operator[] (const uint64_t index);

      /// sort the server list
      int sort(const common::ObServer & server);

      /// current tablet locastion server count
      int64_t size(void) const;

      /// clear all items
      void clear(void);

      /// reset all items
      void reset(void);

      /// get modify timestamp
      int64_t get_timestamp(void) const;
      /// set timestamp
      void set_timestamp(const int64_t timestamp);

      /// dump all info
      void print_info(void) const;

      /// set tablet range if contain the buffer deep copy it
      int set_tablet_range(const common::ObRange & tablet_range);
      const common::ObRange & get_tablet_range() const;

      void set_buffer(common::ObStringBuf & buf);
      const common::ObStringBuf * get_buffer(void) const;
      /// serailize or deserialization
      NEED_SERIALIZE_AND_DESERIALIZE;

    private:
      int64_t cur_count_;
      int64_t timestamp_;
      common::ObRange tablet_range_;
      common::ObStringBuf *tablet_range_rowkey_buf_;
      ObMergerTabletLocation locations_[MAX_REPLICA_COUNT];
    };

    inline void ObMergerTabletLocationList::set_buffer(common::ObStringBuf & buf)
    {
      tablet_range_rowkey_buf_ = &buf;
    }

    inline const common::ObStringBuf * ObMergerTabletLocationList::get_buffer(void) const
    {
      return tablet_range_rowkey_buf_;
    }

    inline int ObMergerTabletLocationList::set_tablet_range(const common::ObRange & tablet_range)
    {
      int ret = OB_SUCCESS;

      if(&tablet_range != &(tablet_range_))
      {
        this->tablet_range_ = tablet_range;
      }

      if(NULL != tablet_range_rowkey_buf_)
      {
        ret = tablet_range_rowkey_buf_->write_string(tablet_range.start_key_, &(tablet_range_.start_key_));
        if(OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "write tablet range start key fail:ret[%d]", ret);
        }
      }

      if((OB_SUCCESS == ret) && (NULL != tablet_range_rowkey_buf_))
      {
        ret = tablet_range_rowkey_buf_->write_string(tablet_range.end_key_, &(tablet_range_.end_key_));
        if(OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "write tablet range end key fail:ret[%d]", ret);
        }
      }
      return ret;
    }

    inline const common::ObRange & ObMergerTabletLocationList::get_tablet_range() const
    {
      return tablet_range_;
    }
    
    inline int64_t ObMergerTabletLocationList::size(void) const
    {
      return cur_count_;
    }

    // not reset the rowkey_buffer
    inline void ObMergerTabletLocationList::reset(void)
    {
      timestamp_ = 0;
      cur_count_ = 0;
      tablet_range_.start_key_.reset();
      tablet_range_.end_key_.reset();
    }

    inline void ObMergerTabletLocationList::clear(void)
    {
      timestamp_ = 0;
      cur_count_ = 0;
      tablet_range_rowkey_buf_ = NULL;
    }

    inline int64_t ObMergerTabletLocationList::get_timestamp(void) const
    {
      return timestamp_;
    }

    inline void ObMergerTabletLocationList::set_timestamp(const int64_t timestamp)
    {
      timestamp_ = timestamp;
    }

    inline ObMergerTabletLocation & ObMergerTabletLocationList::operator[] (const uint64_t index)
    {
      return locations_[index];
    }
  }
}



#endif // OB_MERGER_TABLET_LOCATION_ITEM_H_


