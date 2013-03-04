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
#ifndef OCEANBASE_ROOTSERVER_OB_ROOTTABLE2_H_
#define OCEANBASE_ROOTSERVER_OB_ROOTTABLE2_H_
#include "rootserver/ob_root_meta2.h"
#include "rootserver/ob_tablet_info_manager.h"
#include "common/ob_tablet_info.h"
namespace oceanbase 
{ 
  namespace rootserver 
  {
    class ObRootServer2;
    class ObRootTable2
    {
      public:
        static const int16_t ROOT_TABLE_MAGIC = static_cast<int16_t>(0xABCD);
        typedef ObRootMeta2* iterator;
        typedef const ObRootMeta2*  const_iterator;
        enum 
        {
          POS_TYPE_ADD_RANGE = 0,
          POS_TYPE_SAME_RANGE = 1,
          POS_TYPE_SPLIT_RANGE = 2,
          POS_TYPE_MERGE_RANGE = 3,
          POS_TYPE_ERROR = 5,

        };
        enum
        {
          FIRST_TABLET_VERSION = 1,
        };
      public:
        friend class ObRootServer2;
        explicit ObRootTable2(ObTabletInfoManager* tim);
        virtual ~ObRootTable2();
        
        inline iterator begin() { return &(data_holder_[0]); }
        inline iterator end()  { return begin() + meta_table_.get_array_index(); }
        inline iterator sorted_end()  { return begin() + sorted_count_; }

        inline const_iterator begin() const { return &(data_holder_[0]); }
        inline const_iterator end()  const { return begin() + meta_table_.get_array_index(); }
        inline const_iterator sorted_end() const { return begin() + sorted_count_; }
        inline bool is_empty() const { return begin() == end(); }
        inline void set_replica_num(int32_t replica_num) { replica_num_ = replica_num; }

        void get_cs_version(const int64_t index, int64_t &version);
        int find_range(const common::ObRange& range, 
            const_iterator& first, 
            const_iterator& last) const;

        int find_key(const uint64_t table_id, 
            const common::ObString& key,
            int32_t adjacent_offset,
            const_iterator& first, 
            const_iterator& last,
            const_iterator& ptr
            ) const;

        bool table_is_exist(const uint64_t table_id) const;

        void server_off_line(const int32_t server_index, const int64_t time_stamp);

        void dump() const;
        void dump_cs_tablet_info(const int server_index, int64_t &tablet_num)const;
        void dump_unusual_tablets(int64_t current_version, int32_t replicas_num, int32_t &num) const;
     
        int check_tablet_version_merged(const int64_t tablet_version, bool &is_merged) const;
        const common::ObTabletInfo* get_tablet_info(const const_iterator& it) const;
        common::ObTabletInfo* get_tablet_info(const const_iterator& it);

        static int64_t get_max_tablet_version(const const_iterator& it);
        int64_t get_max_tablet_version();
        int modify(const const_iterator& it, const int32_t dest_server_index, const int64_t tablet_version);
        int replace(const const_iterator& it, const int32_t src_server_index, const int32_t dest_server_index, const int64_t tablet_version);
        /*
         * 得到range会对root table产生的影响
         * 一个range可能导致root table分裂 合并 无影响等
         */
        int get_range_pos_type(const common::ObRange& range, const const_iterator& first, const const_iterator& last) const;
        int split_range(const common::ObTabletInfo& tablet_info, const const_iterator& pos, const int64_t tablet_version, const int32_t server_index);
        int add_range(const common::ObTabletInfo& tablet_info, const const_iterator& pos, const int64_t tablet_version, const int32_t server_index);

        int add(const common::ObTabletInfo& tablet, const int32_t server_index, const int64_t tablet_version);
        int create_table(const common::ObTabletInfo& tablet, const int32_t* server_indexes, const int32_t replicas_num, const int64_t tablet_version);
        bool add_lost_range();
        bool check_lost_range();
        bool check_tablet_copy_count(const int32_t copy_count) const;
        void sort();
        
        /*
         * root table第一次构造的时候使用
         * 整理合并相同的tablet, 生成一份新的root table
         */
        int shrink_to(ObRootTable2* shrunk_table, common::ObTabletReportInfoList &delete_list);
        static int32_t find_suitable_pos(const const_iterator& it, const int32_t server_index, const int64_t tablet_version, common::ObTabletReportInfo *to_delete = NULL);
        int check_tablet_version(const int64_t tablet_version, int safe_copy_count) const;
        //void remove_old_tablet();
        ObTabletCrcHistoryHelper* get_crc_helper(const const_iterator& it);
        int delete_tables(const common::ObArray<uint64_t> &deleted_tables);
      public:
        int write_to_file(const char* filename);
        int read_from_file(const char* filename);
        void dump_as_hex(FILE* stream) const;
        void read_from_hex(FILE* stream);

      private:
      	const_iterator lower_bound(const common::ObRange& range) const;
      	iterator lower_bound(const common::ObRange& range);
        void merge_one_tablet(ObRootTable2* shrunk_table, const int32_t last_tablet_index, const_iterator it, common::ObTabletReportInfo &to_delete);
        bool move_back(const int32_t from_index_inclusive, const int32_t move_step);
        // @pre is sorted
        int shrink();
      private:
        ObRootMeta2 data_holder_[ObTabletInfoManager::MAX_TABLET_COUNT];
        common::ObArrayHelper<ObRootMeta2> meta_table_;
        ObTabletInfoManager* tablet_info_manager_;
        int32_t sorted_count_;
        int32_t replica_num_;
    };
  }
}
#endif
