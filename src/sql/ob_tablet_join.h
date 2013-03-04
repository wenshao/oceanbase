/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_tablet_join.h
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#ifndef _OB_TABLET_JOIN_H
#define _OB_TABLET_JOIN_H 1

#include "common/ob_define.h"
#include "common/ob_array.h"
#include "common/ob_cache.h"
#include "ob_phy_operator.h"
#include "common/ob_ups_row.h"
#include "common/ob_kv_storecache.h"
#include "common/ob_get_param.h"
#include "common/ob_row_store.h"
#include "ob_ups_multi_get.h"
#include "common/thread_buffer.h"

namespace oceanbase
{
  using namespace common;

  namespace sql
  {
    namespace test
    {
      class ObTabletJoinTest_fetch_ups_row_Test;
      class ObTabletJoinTest_compose_get_param_Test;
      class ObTabletJoinTest_gen_ups_row_desc_Test;
      class ObTabletJoinTest_get_right_table_rowkey_Test;
      class ObTabletJoinTest_fetch_fused_row_Test;

      class ObTabletScanTest_create_plan_Test;
      class ObTabletScanTest_create_plan2_Test;
    }

    class ObTabletJoin: public ObPhyOperator
    {
      public:
        struct JoinInfo
        {
          uint64_t right_table_id_;
          uint64_t left_column_id_;
          uint64_t right_column_id_;

          JoinInfo();
        };

        struct TableJoinInfo
        {
          uint64_t left_table_id_; //左表table_id

          /* join条件信息，左表的column对应于右表的rowkey */
          ObArray<JoinInfo> join_condition_;

          /* 指示左表哪些列是从右表取过来的 */
          ObArray<JoinInfo> join_column_;

          public:
            TableJoinInfo();
        };


      public:
        ObTabletJoin();
        virtual ~ObTabletJoin();

        int set_child(int32_t child_idx, ObPhyOperator &child_operator);

        int open();
        int close();
        int get_next_row(const ObRow *&row);
        int64_t to_string(char* buf, const int64_t buf_len) const;
        int get_row_desc(const common::ObRowDesc *&row_desc) const {row_desc=NULL;return OB_NOT_IMPLEMENT;}
        int add_column_id(uint64_t column_id);

        inline void set_batch_count(const int64_t batch_count);
        inline void set_table_join_info(const TableJoinInfo &table_join_info);
        inline void set_ups_multi_get(ObUpsMultiGet *ups_multi_get);

        static const int64_t KVCACHE_SIZE = 2*1024L*1024L*100L; //200M
        static const int64_t KVCACHE_ITEM_SIZE = 128;
        static const int64_t KVCACHE_BLOCK_SIZE = 1024L*1024L; //1M

        friend class test::ObTabletScanTest_create_plan_Test;
        friend class test::ObTabletScanTest_create_plan2_Test;
        friend class test::ObTabletJoinTest_fetch_ups_row_Test;
        friend class test::ObTabletJoinTest_compose_get_param_Test;
        friend class test::ObTabletJoinTest_gen_ups_row_desc_Test;
        friend class test::ObTabletJoinTest_get_right_table_rowkey_Test;
        friend class test::ObTabletJoinTest_fetch_fused_row_Test;

      private:
        // disallow copy
        ObTabletJoin(const ObTabletJoin &other);
        ObTabletJoin& operator=(const ObTabletJoin &other);

        int get_right_table_rowkey(const ObRow &row, uint64_t &right_table_id, ObString &rowkey) const;
        int compose_get_param(uint64_t table_id, const ObString &rowkey, ObGetParam &get_param);
        bool check_inner_stat();
        int fetch_fused_row(ObGetParam *get_param);
        int fetch_ups_row(ObGetParam *get_param);
        int gen_ups_row_desc();

      private:
        // data members
        TableJoinInfo table_join_info_;
        int64_t batch_count_;
        ObPhyOperator *fused_scan_;
        common::KeyValueCache<ObString, ObString, KVCACHE_ITEM_SIZE, KVCACHE_BLOCK_SIZE> ups_row_cache_;
        ObRowStore fused_row_store_;
        ThreadSpecificBuffer thread_buffer_;
        ObUpsMultiGet *ups_multi_get_;
        ObRowDesc curr_row_desc_;
        ObRowDesc ups_row_desc_;
        ObRowDesc ups_row_desc_for_join_;
        ObRow curr_row_;
    };

    void ObTabletJoin::set_ups_multi_get(ObUpsMultiGet *ups_multi_get)
    {
      this->ups_multi_get_ = ups_multi_get;
    }

    void ObTabletJoin::set_table_join_info(const TableJoinInfo &table_join_info)
    {
      table_join_info_ = table_join_info;
    }

    void ObTabletJoin::set_batch_count(const int64_t batch_count)
    {
      batch_count_ = batch_count;
    }

  } // end namespace sql
} // end namespace oceanbase

#endif /* _OB_TABLET_JOIN_H */
