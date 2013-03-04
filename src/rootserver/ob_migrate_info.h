/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 * 
 * Version: $Id$
 *
 * ob_migrate_info.h
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#ifndef _OB_MIGRATE_INFO_H
#define _OB_MIGRATE_INFO_H 1
#include "common/ob_range.h"
#include "common/page_arena.h"
namespace oceanbase
{
  namespace rootserver
  {
    struct ObMigrateInfo
    {
      // types
      typedef int8_t Status;
      static const int8_t STAT_INIT = 0;
      static const int8_t STAT_SENT = 1;
      static const int8_t STAT_DONE = 2;
      // members
      common::ObRange range_;
      int32_t cs_idx_;
      Status stat_;
      int8_t keep_src_;
      int16_t reserve_;
      ObMigrateInfo* next_;
      // methods
      ObMigrateInfo();
      ~ObMigrateInfo();
      void reset(common::CharArena &allocator);
      bool is_init() const;
      const char* get_stat_str() const;
    };

    class ObMigrateInfos
    {
      public:
        ObMigrateInfos();
        ~ObMigrateInfos();
        void reset(int32_t cs_num, int32_t max_migrate_out_per_cs);
        ObMigrateInfo *alloc_info();
        common::CharArena& get_allocator();
        bool is_full() const;
        int32_t get_size() const;
      private:
        common::CharArena key_allocator_;
        ObMigrateInfo *infos_;
        int32_t size_;
        int32_t alloc_idx_;
    };
    inline int32_t ObMigrateInfos::get_size() const
    {
      return size_;
    }
    // static const int32_t MAX_MIGRATE_OUT_PER_CS = 20;
    class ObCsMigrateTo
    {
      public:
        ObCsMigrateTo();
        ~ObCsMigrateTo();
        
        ObMigrateInfo *head();
        ObMigrateInfo *tail();
        const ObMigrateInfo *head() const;
        const ObMigrateInfo *tail() const;
        int32_t count() const;
        int add_migrate_info(const common::ObRange &range, int32_t dest_cs_idx, ObMigrateInfos &infos);
        int add_copy_info(const common::ObRange &range, int32_t dest_cs_idx, ObMigrateInfos &infos);
        int set_migrate_done(const common::ObRange &range, int32_t dest_cs_idx);
        void reset();
      private:
        int add_migrate_info(const common::ObRange &range, int32_t dest_cs_idx, int8_t keep_src, ObMigrateInfos &infos);
        int clone_string(common::CharArena &allocator, const common::ObString &in, common::ObString &out);
      private:
        ObMigrateInfo *info_head_;
        ObMigrateInfo *info_tail_;
        int32_t count_;         // the length of infos_
        int32_t reserve_;        
    };
    
  } // end namespace rootserver
} // end namespace oceanbase
    


#endif /* _OB_MIGRATE_INFO_H */

