
/*
 *   (C) 2007-2010 Taobao Inc.
 *   
 *         
 *   Version: 0.1
 *           
 *   Authors:
 *      qushan <qushan@taobao.com>
 *        - base data structure, maybe modify in future
 *               
 */
#ifndef OCEANBASE_COMMON_OB_TABLET_INFO_H_
#define OCEANBASE_COMMON_OB_TABLET_INFO_H_

#include <tblog.h>
#include "ob_define.h"
#include "ob_range.h"
#include "ob_server.h"
#include "ob_array_helper.h"
#include "page_arena.h"

namespace oceanbase 
{ 
  namespace common
  {
    struct ObTabletLocation 
    {
      int64_t tablet_version_;
      int64_t tablet_seq_;
      ObServer chunkserver_;

      ObTabletLocation() :tablet_version_(0), tablet_seq_(0) { } 
      ObTabletLocation(int64_t tablet_version, const ObServer & server)
        :tablet_version_(tablet_version), tablet_seq_(0),
         chunkserver_(server) { }

      ObTabletLocation(int64_t tablet_version, int64_t tablet_seq, 
        const ObServer & server)
        :tablet_version_(tablet_version), tablet_seq_(tablet_seq),
         chunkserver_(server) { }

      static void dump(const ObTabletLocation & location);

      NEED_SERIALIZE_AND_DESERIALIZE;
    };
    
    struct ObTabletInfo 
    {
      ObRange range_;
      int64_t row_count_;
      int64_t occupy_size_;
      uint64_t crc_sum_;

      ObTabletInfo() 
        : range_(), row_count_(0), occupy_size_(0), crc_sum_(0) {}
      ObTabletInfo(const ObRange& r, const int32_t rc, const int32_t sz, const uint64_t crc_sum = 0)
        : range_(r), row_count_(rc), occupy_size_(sz), crc_sum_(crc_sum) {}

      inline bool equal(const ObTabletInfo& tablet) const 
      {
        return range_.equal(tablet.range_);
      }
      int deep_copy(common::CharArena &allocator, const ObTabletInfo &other, bool new_start_key, bool new_end_key);
      NEED_SERIALIZE_AND_DESERIALIZE;
    };

    struct ObTabletReportInfo
    {
      ObTabletInfo tablet_info_;
      ObTabletLocation tablet_location_;

      bool operator== (const ObTabletReportInfo &other) const;
      NEED_SERIALIZE_AND_DESERIALIZE;
    };

    struct ObTabletReportInfoList
    {
      ObTabletReportInfo tablets_[OB_MAX_TABLET_LIST_NUMBER];
      ObArrayHelper<ObTabletReportInfo> tablet_list_;

      ObTabletReportInfoList()
      {
        reset();
      }

      void reset()
      {
        tablet_list_.init(OB_MAX_TABLET_LIST_NUMBER, tablets_);
      }

      inline int add_tablet(const ObTabletReportInfo& tablet)
      {
        int ret = OB_SUCCESS;

        if (!tablet_list_.push_back(tablet))
          ret = OB_ARRAY_OUT_OF_RANGE;

        return ret;
      }

      inline int rollback(ObTabletReportInfoList& rollback_list, const int64_t rollback_count)
      {
        int ret = OB_SUCCESS;
        int64_t tablet_size = get_tablet_size();

        if (rollback_count > 0 && rollback_count <= tablet_size && rollback_count <= OB_MAX_TABLET_LIST_NUMBER)
        {
          for (int64_t i = 0; i < rollback_count && OB_SUCCESS == ret; i++)
          {
            ret = rollback_list.add_tablet(tablets_[tablet_size - rollback_count + i]);
          }
          if (OB_SUCCESS == ret)
          {
            for (int64_t i = 0; i < rollback_count; i++)
            {
              tablet_list_.pop();
            }
          }
        }
        else
        {
          TBSYS_LOG(WARN, "invalid rollback count, rollback_count=%ld, tablet_size=%ld", 
              rollback_count, tablet_size);
          ret = OB_ERROR;
        }

        return ret;
      }

      inline int64_t get_tablet_size() const { return tablet_list_.get_array_index(); }
      inline const ObTabletReportInfo* const get_tablet() const { return tablets_; }
      bool operator== (const ObTabletReportInfoList &other) const;
      NEED_SERIALIZE_AND_DESERIALIZE;
    };

    struct ObTabletInfoList
    {
      ObTabletInfo tablets[OB_MAX_TABLET_LIST_NUMBER];
      ObArrayHelper<ObTabletInfo> tablet_list;

      ObTabletInfoList()
      {
        reset();
      }

      void reset()
      {
        tablet_list.init(OB_MAX_TABLET_LIST_NUMBER, tablets);
      }

      inline int add_tablet(const ObTabletInfo& tablet)
      {
        int ret = OB_SUCCESS;

        if (!tablet_list.push_back(tablet))
          ret = OB_ARRAY_OUT_OF_RANGE;

        return ret;
      }

      inline int64_t get_tablet_size() const { return tablet_list.get_array_index(); }
      inline const ObTabletInfo* const get_tablet() const { return tablets; }

      NEED_SERIALIZE_AND_DESERIALIZE;
    };

  } // end namespace common
} // end namespace oceanbase

#endif //OCEANBASE_ROOTSERVER_COMMONDATA_H_

