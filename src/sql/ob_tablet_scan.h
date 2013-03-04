/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_tablet_scan.h
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#ifndef _OB_TABLET_SCAN_H
#define _OB_TABLET_SCAN_H 1

#include "sql/ob_phy_operator.h"
#include "sql/ob_sstable_scan.h"
#include "sql/ob_ups_scan.h"
#include "sql/ob_tablet_fuse.h"
#include "sql/ob_tablet_join.h"
#include "sql/ob_sql_expression.h"
#include "sql/ob_operator_factory.h"
#include "sql/ob_project.h"
#include "sql/ob_filter.h"
#include "sql/ob_limit.h"
#include "common/ob_schema.h"
#include <set>

namespace oceanbase
{
  namespace sql
  {
    namespace test
    {
      class ObTabletScanTest_create_plan_Test;
      class ObTabletScanTest_create_plan2_Test;
    }

    // 用于CS从磁盘扫描一个tablet，合并、join动态数据，并执行计算过滤等
    // @code
    // int cs_handle_scan(...)
    // {
    //   ObTabletScan tablet_scan_op;
    //   ScanResult results;
    //   设置tablet_scan的参数;
    //   tablet_scan_op.open();
    //   const ObRow *row = NULL;
    //   for(tablet_scan_op.get_next_row(row))
    //   {
    //     results.output(row);
    //   }
    //   tablet_scan_op.close();
    //   send_response(results);
    // }
    class ObTabletScan: public ObPhyOperator
    {
      friend class test::ObTabletScanTest_create_plan_Test;
      friend class test::ObTabletScanTest_create_plan2_Test;

      public:
        ObTabletScan();
        virtual ~ObTabletScan();

        int open();
        int close();
        int get_next_row(const ObRow *&row);
        int get_row_desc(const common::ObRowDesc *&row_desc) const {row_desc=NULL;return OB_NOT_IMPLEMENT;}
        /// 设置要扫描的tablet
        void set_range(const ObRange &scan_range);

        /// 设置批量做join的个数，需要做join时才生效
        void set_join_batch_count(int64_t join_batch_count)
        {
          join_batch_count_ = join_batch_count;
        }

        /**
         * 添加一个需输出的column
         *
         * @note 只有通过复合列结算新生成的列才需要new_column_id
         * @param expr [in] 需输出的列（这个列可能是个复合列的结果）
         *
         * @return OB_SUCCESS或错误码
         */
        int add_output_column(const ObSqlExpression& expr);

        /**
         * 添加一个filter
         *
         * @param expr [in] 过滤表达式
         *
         * @return OB_SUCCESS或错误码
         */
        int add_filter(const ObSqlExpression& expr);
        /**
         * 指定limit/offset
         *
         * @param limit [in]
         * @param offset [in]
         *
         * @return OB_SUCCESS或错误码
         */
        int set_limit(const int64_t limit, const int64_t offset);

        int set_child(int32_t child_idx, ObPhyOperator &child_operator);
        int64_t to_string(char* buf, const int64_t buf_len) const;

        int set_schema_manager(ObSchemaManagerV2 *schema_mgr);
        int set_operator_factory(ObOperatorFactory *op_factory);
        int set_ups_rpc_stub(ObUpsRpcStub *ups_rpc_stub);

      private:
        // disallow copy
        ObTabletScan(const ObTabletScan &other);
        ObTabletScan& operator=(const ObTabletScan &other);

        int create_plan();
        virtual int get_join_cond(ObTabletJoin::TableJoinInfo &table_join_info) = 0;
        bool check_inner_stat();

      private:
        // data members
        ObRange scan_range_;
        ObArray<ObSqlExpression> out_columns_;
        int64_t join_batch_count_;
        ObPhyOperator *op_root_;
        ObSstableScan *op_sstable_scan_;
        ObUpsScan *op_ups_scan_;
        ObUpsMultiGet *op_ups_multi_get_;
        ObTabletFuse *op_tablet_fuse_;
        ObTabletJoin *op_tablet_join_;
        ObSchemaManagerV2 *schema_mgr_;
        ObOperatorFactory *op_factory_;
        ObUpsRpcStub *ups_rpc_stub_;
        ObProject project_;
        ObFilter filter_;
        ObLimit limit_;
        bool has_project_;
        bool has_filter_;
        bool has_limit_;
    };
  } // end namespace sql
} // end namespace oceanbase

#endif /* _OB_TABLET_SCAN_H */
