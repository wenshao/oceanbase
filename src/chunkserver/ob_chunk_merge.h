/*
 * (C) 2007-2010 TaoBao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * ob_chunk_merge.h is for what ...
 *
 * Version: $id$
 *
 * Authors:
 *   MaoQi maoqi@taobao.com
 *
 */
#ifndef OB_CHUNKSERVER_OB_CHUNK_MERGE_H_
#define OB_CHUNKSERVER_OB_CHUNK_MERGE_H_
#include <tbsys.h>
#include <Mutex.h>
#include <Monitor.h>
#include "common/ob_define.h"
#include "common/ob_schema.h"
#include "common/ob_vector.h"
#include "common/thread_buffer.h"
#include "sstable/ob_sstable_writer.h"
#include "sstable/ob_sstable_row.h"
#include "ob_tablet_manager.h"
#include "ob_file_recycle.h"
#include "ob_import_sstable.h"

#include "common/ob_read_common_data.h"
#include "common/ob_object.h"
#include "ob_cell_stream.h"
#include "ob_query_agent.h"
#include "ob_merge_reader.h"
#include "ob_get_cell_stream_wrapper.h"
#include "ob_chunk_server_merger_proxy.h"
#include "ob_tablet_merge_filter.h"
#include "ob_get_scan_proxy.h"

namespace oceanbase
{
  namespace chunkserver
  {
    class ObChunkMerge 
    {
      public:
        ObChunkMerge();
        ~ObChunkMerge() {}

        int init(ObTabletManager *manager);
        void destroy();

        /**
         * the new frozen_version come,
         * we will wake up all merge thread to start merge
         * 
         */
        int schedule(const int64_t frozen_version);

        inline const bool is_pending_in_upgrade() const
        {
          return pending_in_upgrade_;
        }

        inline bool is_merge_stoped() const
        {
          return 0 == active_thread_num_;
        }

        inline const ObImportSSTable& get_import_sstable() const
        {
          return import_sstable_;
        }

        inline void set_newest_frozen_version(const int64_t frozen_version)
        {
          if (frozen_version > newest_frozen_version_)
            newest_frozen_version_ = frozen_version;
        }

        void set_config_param();

        bool can_launch_next_round(const int64_t frozen_version);

        int create_merge_threads(const int32_t max_merge_thread);
     
      private:
        static void* run(void *arg);
        void merge_tablets();
        int get_tablets(ObTablet* &tablet);

        bool have_new_version_in_othercs(const ObTablet* tablet);
        int delete_tablet_on_rootserver(const ObTablet* tablet);
        
        int start_round(const int64_t frozen_version);
        int finish_round(const int64_t frozen_version);
        int fetch_frozen_time_busy_wait(const int64_t frozen_version, int64_t &frozen_time);
        int fetch_frozen_schema_busy_wait(
          const int64_t frozen_version, common::ObSchemaManagerV2& schema);

      public:
        struct RowKeySwap 
        {
          RowKeySwap(): last_end_key_buf(NULL),start_key_buf(NULL),
          last_end_key_buf_len(0),last_end_key_len(0),
          start_key_buf_len(0),start_key_len(0) {}

          char *last_end_key_buf;
          char *start_key_buf;

          int32_t last_end_key_buf_len;
          int32_t last_end_key_len;

          int32_t start_key_buf_len;
          int32_t start_key_len;
        };

        bool check_load();
      private:
        friend class ObTabletMerger;
        const static int32_t MAX_MERGE_THREAD = 32;
        const static int32_t TABLET_COUNT_PER_MERGE = 1024;
        const static uint32_t MAX_MERGE_PER_DISK = 2;
      private:
        volatile bool inited_;
        pthread_cond_t cond_;
        pthread_mutex_t mutex_;

        pthread_t tid_[MAX_MERGE_THREAD];

        ObTablet *tablet_array_[TABLET_COUNT_PER_MERGE];
        int32_t tablets_num_;
        int32_t tablet_index_;
        int32_t thread_num_;

        volatile int32_t tablets_have_got_;
        volatile int64_t active_thread_num_;

        volatile int64_t frozen_version_; //frozen_version_ in merge
        int64_t frozen_timestamp_; //current frozen_timestamp_ in merge;
        volatile int64_t newest_frozen_version_; //the last frozen version in updateserver

        volatile int64_t merge_start_time_;    //this version start time
        volatile int64_t merge_last_end_time_;    //this version merged complete time

        ObTabletManager *tablet_manager_;

        common::ThreadSpecificBuffer last_end_key_buffer_;
        common::ThreadSpecificBuffer cur_row_key_buffer_;
        
        volatile uint32_t pending_merge_[common::OB_MAX_DISK_NUMBER]; //use atomic op

        volatile bool round_start_;
        volatile bool round_end_;
        volatile bool pending_in_upgrade_;

        int64_t merge_load_high_;
        int64_t request_count_high_;
        int64_t merge_adjust_ratio_;
        int64_t merge_load_adjust_;
        int64_t merge_pause_row_count_;
        int64_t merge_pause_sleep_time_;
        int64_t merge_highload_sleep_time_;
        int64_t min_merge_thread_num_;

        common::ObSchemaManagerV2 last_schema_;
        common::ObSchemaManagerV2 current_schema_;

        ObImportSSTable import_sstable_;
    }; 
    

    class ObTabletMerger
    {
      public:
        ObTabletMerger(ObChunkMerge& chunk_merge,ObTabletManager& manager);
        ~ObTabletMerger() {}

        int init(ObChunkMerge::RowKeySwap *swap);
        int merge_or_import(ObTablet *tablet,int64_t frozen_version);

      private:
        void reset();

        DISALLOW_COPY_AND_ASSIGN(ObTabletMerger);

        enum RowStatus
        {
          ROW_START = 0,
          ROW_GROWING = 1,
          ROW_END = 2
        };

        bool is_table_need_join(const uint64_t table_id);
        int fill_sstable_schema(const common::ObTableSchema& common_schema,
          sstable::ObSSTableSchema& sstable_schema);
        int update_range_start_key();
        int update_range_end_key();
        int create_new_sstable();
        int create_hard_link_sstable(int64_t& sstable_size);
        int finish_sstable(const bool is_tablet_unchanged, ObTablet*& new_tablet);
        int update_meta();
        int report_tablets(ObTablet* tablet_list[],int32_t tablet_size);
        bool maybe_change_sstable() const;

        int fill_scan_param(const uint64_t column_group_id);
        int wait_aio_buffer() const;
        int reset_local_proxy() const;
        int merge_column_group(
            const int64_t column_group_idx,
            const uint64_t column_group_id,
            int64_t& split_row_pos,
            const int64_t max_sstable_size, 
            const bool is_need_join,
            bool& is_tablet_splited,
            bool& is_tablet_unchanged
            );

        int save_current_row(const bool current_row_expired);
        int check_row_count_in_column_group();
        void reset_for_next_column_group();
        int merge(ObTablet *tablet,int64_t frozen_version);
        int import(ObTablet *tablet,int64_t frozen_version);

      private:
        ObChunkMerge&            chunk_merge_;
        ObTabletManager&         manager_;

        ObChunkMerge::RowKeySwap*   row_key_swap_;

        common::ObInnerCellInfo*     cell_;
        ObTablet*       old_tablet_;
        const common::ObTableSchema* new_table_schema_;

        sstable::ObSSTableRow    row_;
        sstable::ObSSTableSchema sstable_schema_;
        sstable::ObSSTableWriter writer_;
        common::ObRange new_range_;
        sstable::ObSSTableId     sstable_id_;

        int64_t max_sstable_size_;
        int64_t frozen_version_;
        int64_t current_sstable_size_;
        int64_t row_num_;
        int64_t pre_column_group_row_num_;

        char             path_[common::OB_MAX_FILE_NAME_LENGTH];
        common::ObString path_string_;
        common::ObString compressor_string_;

        ObGetScanProxy           cs_proxy_;
        common::ObScanParam      scan_param_;

        ObGetCellStreamWrapper    ms_wrapper_;
        ObQueryAgent          merge_join_agent_;
        common::ObVector<ObTablet *> tablet_array_;
        ObTabletMergerFilter tablet_merge_filter_;
    };
  } /* chunkserver */
} /* oceanbase */
#endif
