/*
 *   (C) 2007-2010 Taobao Inc.
 *   
 *         
 *   Version: 0.1 
 *           
 *   Authors:
 *      daoan <daoan@taobao.com>
 *               
 */
#ifndef OCEANBASE_ROOTSERVER_OB_ROOTMETA2_H_
#define OCEANBASE_ROOTSERVER_OB_ROOTMETA2_H_
#include <stdio.h>

#include "common/page_arena.h"
#include "common/ob_tablet_info.h"
#include "common/ob_define.h"

namespace oceanbase 
{ 
  namespace rootserver 
  {
    class ObTabletInfoManager;
    struct ObRootMeta2 
    {
      int32_t tablet_info_index_;
      //index of chunk server manager. so we can find out the server' info by this
      mutable int32_t server_info_indexes_[common::OB_SAFE_COPY_COUNT];  
      mutable int64_t tablet_version_[common::OB_SAFE_COPY_COUNT];  
      mutable int64_t last_dead_server_time_;
      mutable int64_t last_migrate_time_; // don't serialize

      ObRootMeta2();
      void dump() const;
      void dump(const int server_index, int64_t &tablet_num) const;
      void dump_as_hex(FILE* stream) const;
      void read_from_hex(FILE* stream);
      bool did_cs_have(const int32_t cs_idx) const;
      bool can_be_migrated_now(int64_t disabling_period_us) const;
      void has_been_migrated();
      NEED_SERIALIZE_AND_DESERIALIZE;
    };
    
    inline bool ObRootMeta2::did_cs_have(const int32_t cs_idx) const
    {
      bool ret = false;
      for (int i = 0; i < common::OB_SAFE_COPY_COUNT; ++i)
      {
        if (cs_idx == server_info_indexes_[i])
        {
          ret = true;
          break;
        }
      }
      return ret;
    }
    
    class ObRootMeta2CompareHelper
    {
      public:
        explicit ObRootMeta2CompareHelper(ObTabletInfoManager* otim);
        int compare(const int32_t r1, const int32_t r2) const;
        bool operator () (const ObRootMeta2& r1, const ObRootMeta2& r2) const;
      private:
        ObTabletInfoManager* tablet_info_manager_;
    };


  } // end namespace rootserver
} // end namespace oceanbase


#endif 
