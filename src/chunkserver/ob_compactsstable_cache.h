/*
* (C) 2007-2011 TaoBao Inc.
*
* This program is free software; you can redistribute it and/or modify
* it under the terms of the GNU General Public License version 2 as
* published by the Free Software Foundation.
*
* ob_compactsstable_cache.h is for what ...
*
* Version: $id$
*
* Authors:
*   MaoQi maoqi@taobao.com
*
*/
#ifndef OB_COMPACTSSTABLE_CACHE_H
#define OB_COMPACTSSTABLE_CACHE_H

#include "tbsys.h"
#include "common/ob_define.h"
#include "common/ob_scan_param.h"
#include "compactsstable/ob_compactsstable_mem.h"
#include "ob_tablet.h"
#include "ob_scan_cell_stream.h"

namespace oceanbase
{
  namespace chunkserver
  {
    class ObTabletManager;

    /** 
     * get frozen data of a tablet
     * 
     */
    class ObCompacSSTableMemGetter
    {
    public:
      ObCompacSSTableMemGetter(ObMergerRpcProxy& proxy);
      ~ObCompacSSTableMemGetter();

      /** 
       * get forzen data of *tablet*
       * 
       * @param tablet the tablet
       * @param data_version  the version of ups active mem tablet
       * 
       * @return 0 on success,otherwise on failure
       */
      int get(ObTablet* tablet,int64_t data_version);

      void reset();
      
    private:
      int fill_scan_param(ObTablet& tablet,int64_t data_version);
      void fill_version_range(const ObTablet& tablet,const int64_t data_version,
                              common::ObVersionRange& version_range) const;
      
      /** 
       * convert scanner to CompactSSTableMem
       * 
       * @param cell_strem the scanner
       * @param sstable
       * 
       * @return 0 on success,otherwise on failure
       */
      int fill_compact_sstable(ObScanCellStream& cell_strem,compactsstable::ObCompactSSTableMem& sstable);
      int save_row(uint64_t table_id,compactsstable::ObCompactSSTableMem& sstable);
      void cacl_compactcache_stat(ObTablet& tablet, compactsstable::ObCompactSSTableMemNode& cache_node);

    private:
      struct compactcache_table_stat
      {
        int64_t tablet_num;
        int64_t row_num;
        int64_t data_size;
        int64_t usage_size;
      };
      static const int64_t MAX_TABLE_ID = 65536;

      ObMergerRpcProxy&                 rpc_proxy_;
      common::ObScanParam               scan_param_;
      ObScanCellStream                  cell_stream_;
      compactsstable::ObCompactRow      row_;
      int64_t                           row_cell_num_;
      compactcache_table_stat           table_stat_[MAX_TABLE_ID];
    };

    class ObCompactSSTableMemThread : public tbsys::CDefaultRunnable
    {
    public:
      ObCompactSSTableMemThread();
      ~ObCompactSSTableMemThread();

      int init(ObTabletManager* tablet_manager);
      void destroy();

      int push(ObTablet* tablet,int64_t data_version);
      /*virtual*/ void run(tbsys::CThread *thread, void *arg);

      bool is_full() const { return full_; }
      void clear_full_flag();

    private:
      static const int32_t MAX_TABLETS_NUM = 1024;
      static const int64_t MAX_THREAD_COUNT = 16;
      ObTablet* pop();
    private:
      tbsys::CThreadCond       mutex_;
      ObTabletManager*         manager_;
      bool                     inited_;
      bool                     full_;
      ObTablet*                tablets_[MAX_TABLETS_NUM];
      int32_t                  read_idx_;
      int32_t                  write_idx_;
      int32_t                  tablets_num_;
      int64_t                  data_version_;
    };

    class ObJoinCompactSSTable
    {
    public:
      ObJoinCompactSSTable();
      ~ObJoinCompactSSTable();
      static const uint64_t OB_MAX_JOIN_TABLE_NUM = 16;

      bool compare_and_set_load_flag();
      void clear_load_flag();
      int add_join_table(const uint64_t table_id);
      ObTablet* get_join_tablet(const uint64_t table_id);

      void clear();
    private:
      ObTablet tablet_[OB_MAX_JOIN_TABLE_NUM];
      volatile uint64_t join_table_nums_;
      volatile uint64_t join_table_loading_;
    };
    
  } //end namespace chunkserver
} //end namespace oceanbase

#endif
