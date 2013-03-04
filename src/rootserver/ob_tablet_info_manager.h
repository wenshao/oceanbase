/*===============================================================
*   (C) 2007-2010 Taobao Inc.
*   
*   
*   Version: 0.1 2010-10-20
*   
*   Authors:
*          daoan(daoan@taobao.com)
*   
*
================================================================*/
#ifndef OCEANBASE_ROOTSERVER_OB_TABLET_INFO_MANAGER_H_
#define OCEANBASE_ROOTSERVER_OB_TABLET_INFO_MANAGER_H_
#include "common/ob_define.h"
#include "common/ob_tablet_info.h"
#include "common/page_arena.h"
#include "common/ob_array.h"

namespace oceanbase 
{ 
  namespace rootserver 
  {
    class ObTabletInfoManager;
    class ObTabletCrcHistoryHelper
    {
      public:
        enum 
        {
          MAX_KEEP_HIS_COUNT = 3,
        };
        ObTabletCrcHistoryHelper();
        void reset();
        void rest_all_crc_sum();
        int check_and_update(const int64_t version, const uint64_t crc_sum);
        void reset_crc_sum(const int64_t version);
        NEED_SERIALIZE_AND_DESERIALIZE;
        friend class ObTabletInfoManager;
      private:
        void get_min_max_version(int64_t &min_version, int64_t &max_version) const;
        void update_crc_sum(const int64_t version, const int64_t new_version, const uint64_t crc_sum);
      private:
        int64_t version_[MAX_KEEP_HIS_COUNT];
        uint64_t crc_sum_[MAX_KEEP_HIS_COUNT];
    };
    class ObTabletInfoManager
    {
      public:
        ObTabletInfoManager();
        void set_allocator(common::CharArena *allocator);

        int add_tablet_info(const common::ObTabletInfo& tablet_info, int32_t& out_index, 
            bool clone_start_key = true, bool clone_end_key = true);

        int32_t get_index(common::ObTabletInfo* data_pointer) const;

        const common::ObTabletInfo* get_tablet_info(const int32_t index) const;
        common::ObTabletInfo* get_tablet_info(const int32_t index);
        const common::ObTabletInfo* begin() const;
        common::ObTabletInfo* begin();
        const common::ObTabletInfo* end() const;
        common::ObTabletInfo* end();
        int32_t get_array_size() const;

        ObTabletCrcHistoryHelper* get_crc_helper(const int32_t index);
        const ObTabletCrcHistoryHelper* get_crc_helper(const int32_t index) const;

        void hex_dump(const int32_t index, const int32_t log_level = TBSYS_LOG_LEVEL_DEBUG) const;
        void dump_as_hex(FILE* stream) const;
        void read_from_hex(FILE* stream);

        NEED_SERIALIZE_AND_DESERIALIZE;
      public:
        static const int64_t MAX_TABLET_COUNT = 1024 * 1024 * 10;
      private:
        DISALLOW_COPY_AND_ASSIGN(ObTabletInfoManager);        
      private:
        common::ObTabletInfo data_holder_[MAX_TABLET_COUNT];
        ObTabletCrcHistoryHelper crc_helper_[MAX_TABLET_COUNT];
        common::ObArrayHelper<common::ObTabletInfo> tablet_infos_;
        common::CharArena allocator_;
    };

  }
}

#endif
