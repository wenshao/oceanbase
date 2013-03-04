/*
 *  (C) 2007-2010 Taobao Inc.
 *  
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License version 2 as
 *  published by the Free Software Foundation.
 *
 *         ????.cpp is for what ...
 *
 *  Version: $Id: ipvsadm.c,v 1.27 2005/12/10 16:00:07 wensong Exp $
 *
 *  Authors:
 *     qushan <qushan@taobao.com>
 *        - some work details if you want
 */
#ifndef OCEANBASE_CHUNKSERVER_TABLET_IMAGE_H_
#define OCEANBASE_CHUNKSERVER_TABLET_IMAGE_H_

#include "atomic.h"
#include "common/page_arena.h"
#include "common/ob_range.h"
#include "common/ob_vector.h"
#include "common/ob_spin_rwlock.h"
#include "common/ob_file.h"
#include "common/ob_atomic.h"
#include "sstable/ob_disk_path.h"
#include "sstable/ob_sstable_reader.h"
#include "ob_tablet.h"

namespace oceanbase 
{ 
  namespace chunkserver 
  {
    struct ObTabletMetaHeader
    {
      static const int16_t TABLET_META_MAGIC = 0x6D69; // "mi"
      int64_t tablet_count_;     
      int64_t data_version_;    
      int32_t row_key_stream_offset_;
      int32_t row_key_stream_size_;
      int32_t tablet_extend_info_offset_;
      int32_t tablet_extend_info_size_;

      NEED_SERIALIZE_AND_DESERIALIZE;
    };


    class ObTabletBuilder
    {
      public:
        ObTabletBuilder();
        virtual ~ObTabletBuilder();
        inline int64_t get_data_size() const { return data_size_; }
        inline int64_t get_buf_size() const { return buf_size_; }
        inline const char* get_buf() const { return buf_; }
        inline void reset() { data_size_ = 0; }
      protected:
        static const int64_t DEFAULT_BUILDER_BUF_SIZE = 2 * 1024 * 1024; //2M
        int ensure_space(const int64_t size);
        char* buf_;
        int64_t buf_size_;
        int64_t data_size_;
    };

    class ObTabletRowkeyBuilder : public ObTabletBuilder
    {
      public:
        ObTabletRowkeyBuilder();
        ~ObTabletRowkeyBuilder();
        int append_range(const common::ObRange& range);
    };

    class ObTabletExtendBuilder : public ObTabletBuilder
    {
      public:
        ObTabletExtendBuilder();
        ~ObTabletExtendBuilder();
        int append_tablet(const ObTabletExtendInfo& info);
    };

    struct ObThreadMetaWriter
    {
      void reset()
      {
        builder_.reset();
        extend_.reset();
        meta_data_size_ = 0;
      }

      common::ObFileAppender appender_;
      ObTabletRowkeyBuilder builder_;
      ObTabletExtendBuilder extend_;
      common::ObMemBuf meta_buf_;
      int64_t meta_data_size_;
    };

    class ObMultiVersionTabletImage;
    class ObTabletImage
    {
      friend class ObMultiVersionTabletImage;
      static const int32_t INVALID_ITER_INDEX = -1;
      public:
        ObTabletImage();
        ~ObTabletImage();

        friend class ObTablet;

      public:
        int destroy();
        int dump(const bool dump_sstable = false) const;

        int acquire_tablet(const common::ObRange& range, const int32_t scan_direction, ObTablet* &tablet) const;
        int release_tablet(ObTablet* tablet) const;
        int acquire_sstable(const sstable::ObSSTableId& sstable_id, sstable::ObSSTableReader* &reader) const;
        int release_sstable(sstable::ObSSTableReader* reader) const;
        int include_sstable(const sstable::ObSSTableId& sstable_id) const;

        int remove_tablet(const common::ObRange& range, int32_t &disk_no);

        /*
         * scan traverses over all tablets,
         * begin_scan_tablets, get_next_tablet will add
         * reference count of tablet, so need call release_tablet
         * when %tablet not used.
         */
        int begin_scan_tablets();
        int get_next_tablet(ObTablet* &tablet);
        int end_scan_tablets();

        inline int32_t get_ref_count() const { return atomic_read((atomic_t*)&ref_count_); }
        inline int64_t get_max_sstable_file_seq() const { return max_sstable_file_seq_; }
        inline int64_t get_data_version() const { return data_version_; }
        inline void set_data_version(int64_t version) { data_version_ = version; }

        int add_tablet(ObTablet* tablet);
        int alloc_tablet_object(const common::ObRange& range, ObTablet* &tablet);
        void set_fileinfo_cache(common::IFileInfoMgr& fileinfo_cache);

        inline void incr_merged_tablet_count() { common::atomic_inc(&merged_tablet_count_); }
        inline void decr_merged_tablet_count() { common::atomic_dec(&merged_tablet_count_); }
        inline int64_t get_merged_tablet_count() const { return merged_tablet_count_; }

      public:
        /**
         * all read methods are not support mutithread
         * read method used only for load tablets when system initialize.
         */
        int read(const int32_t* disk_no_array, const int32_t size, const bool load_sstable = false);
        int write(const int32_t* disk_no_array, const int32_t size);
        int read(const char* idx_path, const int32_t disk_no, const bool load_sstable = false);
        int write(const char* idx_path, const int32_t disk_no);
        /**
         * read/write from/to index file at specific disk.
         */
        int write(const int32_t disk_no);
        int read(const int32_t disk_no, const bool load_sstable = false);

        /**
         * seperate the write() function to two functions, 
         * prepare_write_meta() prepare the meta in memory, and it can 
         * run in critical section, write_meta() write the meta to 
         * disk by direct io
         */
        int prepare_write_meta(const int32_t disk_no);
        int write_meta(const char* idx_path, const int32_t disk_no);

      public:
        int serialize(const int32_t disk_no, char* buf, const int64_t buf_len, int64_t &pos);
        int deserialize(const int32_t disk_no, const char* buf, const int64_t buf_len, int64_t &pos);
        int64_t get_max_serialize(const int32_t disk_no) const;
        int32_t get_tablets_num() const;
      private:
        int serialize(const int32_t disk_no, 
            char* buf, const int64_t buf_len, int64_t& pos, 
            ObTabletRowkeyBuilder& builder, ObTabletExtendBuilder& extend);
        int deserialize(const bool load_sstable, const int32_t disk_no, 
            const char* buf, const int64_t data_len, int64_t& pos);

      private:
        int find_sstable(const sstable::ObSSTableId& sstable_id, sstable::ObSSTableReader* &reader) const;
        int find_tablet(const common::ObRange& range, const int32_t scan_direction, ObTablet* &tablet) const;
        int find_tablet(const uint64_t table_id, const common::ObBorderFlag &border_flag, 
            const common::ObString& key, const int32_t search_mode, ObTablet* &tablet) const;
      private:
        sstable::ObSSTableReader* alloc_sstable_object();
        ObTablet* alloc_tablet_object();
        int reset();
        inline int acquire() { return atomic_inc_return((atomic_t*) &ref_count_); }
        inline int release() { return atomic_dec_return((atomic_t*) &ref_count_); }

      private:
        static const int64_t DEFAULT_TABLET_NUM = 128 * 1024L;
        typedef common::hash::ObHashMap<sstable::ObSSTableId, ObTablet*, 
          common::hash::NoPthreadDefendMode> HashMap;

      private:
        common::ObSortedVector<ObTablet*> tablet_list_;
        common::ObVector<sstable::ObSSTableReader*> sstable_list_;
        HashMap sstable_to_tablet_map_;
        bool hash_map_inited_;
        // all tablets has same data_version_ for now.
        // will be discard on next version.
        int64_t data_version_; 
        int64_t max_sstable_file_seq_;
        volatile int32_t ref_count_;
        volatile int32_t cur_iter_idx_;
        volatile uint64_t merged_tablet_count_;

        common::ModulePageAllocator mod_;
        common::ModuleArena allocator_;

        common::IFileInfoMgr* fileinfo_cache_;
        mutable tbsys::CThreadMutex alloc_sstable_mutex_;
    };

    class ObMultiVersionTabletImage 
    {
      public:
        static const int32_t MAX_RESERVE_VERSION_COUNT = 2;
        enum ScanDirection
        {
          SCAN_FORWARD = 0,
          SCAN_BACKWARD = 1,
        };

        enum ScanPosition
        {
          FROM_SERVICE_INDEX = 0,
          FROM_NEWEST_INDEX = 1,
        };

      public:
        explicit ObMultiVersionTabletImage(common::IFileInfoMgr& fileinfo_cache);
        ~ObMultiVersionTabletImage();
      public:
        /**
         *search tablet includes the %range in tablet image
         *and add the tablet's reference count.
         *@param range search range
         *@param scan_direction normally SCAN_FORWARD
         *@param version if > 0, then search the tablet which 
         *version less or equal this version, otherwise search
         *the tablet which version is newest.
         *@param [out] tablet search result.
         */
        int acquire_tablet(const common::ObRange &range, 
            const ScanDirection scan_direction, 
            const int64_t version, 
            ObTablet* &tablet) const;
        int acquire_tablet_all_version(
            const common::ObRange &range, 
            const ScanDirection scan_direction, 
            const ScanPosition from_index,
            const int64_t version, 
            ObTablet* &tablet) const;

        /**
         * release tablet, subtract ref count.
         */
        int release_tablet(ObTablet* tablet) const;

        /**
         * get sstable reader object from tablet image.
         * this function could run slower than acquire_tablet
         * cause need traverse every tablet to find which one
         * include sstable.
         */
        int acquire_sstable(
            const sstable::ObSSTableId& sstable_id, 
            const int64_t version, 
            sstable::ObSSTableReader* &reader) const;
        int acquire_sstable_all_version(
            const sstable::ObSSTableId& sstable_id, 
            const ScanPosition from_index,
            const int64_t version, 
            sstable::ObSSTableReader* &reader) const;

        int release_sstable(const int64_t version,
            sstable::ObSSTableReader* reader) const;

        /**
         * check sstable if alive in current images.
         * which could be recycled when not exist.
         */
        int include_sstable(const sstable::ObSSTableId& sstable_id) const;

        /**
         * add new tablet to image
         * for create tablet.
         */
        int add_tablet(ObTablet *tablet, 
            const bool load_sstable = false, 
            const bool for_create = false);

        /**
         *version of tablet upgrade, when tablet compacted with update data in ups.
         *replace oldest tablet entry in image with new_tablet.
         *@param new_tablet new version tablet entry.
         */
        int upgrade_tablet(ObTablet *old_tablet, ObTablet *new_tablet, 
            const bool load_sstable = false);

        /**
         *same as above , but tablet splited in compaction. old tablet splits to 
         *several of new tablets.
         *@param old_tablet 
         *@param new_tablets  new split tablet array.
         *@param split_size size of new_tablets.
         *@param load_sstable load the sstable file of tablets.
         */
        int upgrade_tablet(ObTablet *old_tablet, 
            ObTablet *new_tablets[], const int32_t split_size, 
            const bool load_sstable = false);

        int upgrade_service();

        int drop_compactsstable();

        /**
         * remove specific version of tablet 
         * @param range of tablet
         * @param version of tablet
         * @param [out] disk no of meta index file.
         */
        int remove_tablet(const common::ObRange& range, 
            const int64_t version, int32_t &disk_no);

        /**
         * check if can launch a new merge process with version
         */
        int prepare_for_merge(const int64_t version);

        /**
         * fetch some tablets who has version less than %version for merge.
         * @param version 
         * @param [in,out] fetch size of tablets
         * @param [out] tablets for merge
         */
        int get_tablets_for_merge(const int64_t current_frozen_version, 
            int32_t &size, ObTablet *tablets[]) const;

        /**
         * check if remains some tablets which has data version = %version
         * need merge(upgrade to newer version)
         */
        bool has_tablets_for_merge(const int64_t version) const;

        /**
         * give up merge some tablets which has data version = %version
         * set them merged.
         */
        int discard_tablets_not_merged(const int64_t version);

        /**
         * allocate new tablet object, with range and version.
         * @param range of tablet, rowkey 's memory will be reallocated.
         * @param version of tablet.
         * @param [out] tablet object pointer.
         */
        int alloc_tablet_object(const common::ObRange& range, 
            const int64_t version, ObTablet* &tablet);

        /**
         * write tablet image to disk index file
         * one file per version, disk
         */
        int write(const int64_t version, const int32_t disk_no);
        int write(const char* idx_path, const int64_t version, const int32_t disk_no);
        int sync_all_images(const int32_t* disk_no_array, const int32_t size);

        /**
         * read index file to tablet image object.
         * one file per version, disk, only for test.
         */
        int read(const int64_t version, const int32_t disk_no, 
            const bool load_sstable = false);
        int read(const char* idx_path, const int64_t version, 
            const int32_t disk_no, const bool load_sstable = false);
        int load_tablets(const int32_t* disk_no_array, const int32_t size, 
            const bool load_sstable = false);

        /*
         * scan traverses over all tablets,
         * begin_scan_tablets, get_next_tablet will add
         * reference count of tablet, so need call release_tablet
         * when %tablet not used.
         */
        int begin_scan_tablets();
        int get_next_tablet(ObTablet* &tablet);
        int end_scan_tablets();

        int64_t get_max_sstable_file_seq() const;

        int64_t get_eldest_version() const;
        int64_t get_newest_version() const;
        int64_t get_serving_version() const;

        void dump(const bool dump_sstable = true) const;

        int serialize(const int32_t index, const int32_t disk_no, char* buf, const int64_t buf_len, int64_t &pos);
        int deserialize(const int32_t disk_no, const char* buf, const int64_t buf_len, int64_t &pos);

        common::IFileInfoMgr& get_fileinfo_cache() const { return fileinfo_cache_; }
        const char* print_tablet_image_stat() const;

      private:
        int64_t get_eldest_index() const;
        int alloc_tablet_image(const int64_t version);
        
        inline int64_t get_index(const int64_t version) const
        {
          return version % MAX_RESERVE_VERSION_COUNT;
        }
        inline ObTabletImage &get_image(const int64_t version) 
        {
          return *image_tracker_[version % MAX_RESERVE_VERSION_COUNT];
        }
        inline const ObTabletImage &get_image(const int64_t version) const
        {
          return *image_tracker_[version % MAX_RESERVE_VERSION_COUNT];
        }
        inline bool has_tablet(const int64_t index) const
        {
          return NULL != image_tracker_[index] 
            && image_tracker_[index]->data_version_ > 0
            && image_tracker_[index]->tablet_list_.size() > 0;
        }
        inline bool has_version_tablet(const int64_t version) const
        {
          return has_tablet(version % MAX_RESERVE_VERSION_COUNT);
        }

        inline bool has_match_version_tablet(const int64_t version) const
        {
          return has_version_tablet(version) 
            && get_image(version).data_version_ == version;
        }


        int64_t initialize_service_index();

        // check tablet image object for prepare to store tablets.
        int prepare_tablet_image(const int64_t version, const bool destroy_exist);
        // check tablet image object if can destroy.
        int tablets_all_merged(const int64_t index) const;
        // destroy tablet image object in image_tracker_[index].
        int destroy(const int64_t index);
        // destroy all tablet image objects.
        int destroy();


      private:
        struct Iterator
        {
          static const int64_t INVALID_INDEX = -1;
          int64_t cur_vi_;
          int64_t cur_ti_;
          int64_t start_vi_;
          int64_t end_vi_;
          const ObMultiVersionTabletImage& image_;
          Iterator(const ObMultiVersionTabletImage& image) 
            : image_(image)
          {
            reset();
          }
          inline void reset() 
          { 
            cur_vi_ = INVALID_INDEX; 
            cur_ti_ = INVALID_INDEX; 
            start_vi_ = INVALID_INDEX;
            end_vi_ = INVALID_INDEX;
          }
          inline bool is_stop() const
          {
            return start_vi_ == INVALID_INDEX;
          }
          inline bool is_end() const
          {
            return cur_vi_ == end_vi_ && (!image_.has_tablet(cur_vi_) 
                || cur_ti_ >= image_.image_tracker_[cur_vi_]->tablet_list_.size());
          }
          inline bool is_last_tablet() const
          {
            return image_.has_tablet(cur_vi_) 
              && cur_ti_ == image_.image_tracker_[cur_vi_]->tablet_list_.size() - 1;
          }
          inline ObTablet* get_tablet() const
          {
            ObTablet* tablet = NULL;
            if (image_.has_tablet(cur_vi_))
            {
              tablet = image_.image_tracker_[cur_vi_]->tablet_list_.at(static_cast<int32_t>(cur_ti_));
              image_.image_tracker_[cur_vi_]->acquire();
            }
            return tablet;
          }
          int start();
          int next();
        };
      private:
        int64_t newest_index_;
        int64_t service_index_;
        ObTabletImage *image_tracker_[MAX_RESERVE_VERSION_COUNT];
        Iterator iterator_;
        common::IFileInfoMgr& fileinfo_cache_;
        mutable tbsys::CRWLock lock_;
    };

  } // end namespace chunkserver
} // end namespace oceanbase


#endif //OCEANBASE_CHUNKSERVER_TABLET_IMAGE_H_

