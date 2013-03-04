/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_rpc_scan.h
 *
 * Authors:
 *   Yu Huang <xiaochu.yh@taobao.com>
 *
 */
#ifndef _OB_RPC_SCAN_H
#define _OB_RPC_SCAN_H 1
#include "ob_phy_operator.h"
#include "ob_sql_expression.h"
#include "common/ob_row.h"
#include "common/ob_scan_param.h"
#include "mergeserver/ob_ms_sql_scan_event.h"
#include "mergeserver/ob_ms_scan_param.h"
namespace oceanbase
{
  namespace sql
  {
    //class oceanbase::mergeserver::ObMergerLocationCacheProxy;
    //class oceanbase::mergeserver::ObMergerAsyncRpcStub;
    // 用于MS进行全表扫描
    class ObRpcScan : public ObPhyOperator
    {
      public:
        ObRpcScan();
        virtual ~ObRpcScan();

        int set_child(int32_t child_idx, ObPhyOperator &child_operator)
        {
          UNUSED(child_idx);
          UNUSED(child_operator);
          return OB_ERROR;
        }
        int init(mergeserver::ObMergerLocationCacheProxy * cache_proxy, mergeserver::ObMergerAsyncRpcStub * async_rpc);
        virtual int open();
        virtual int close();
        virtual int get_next_row(const common::ObRow *&row);
        virtual int get_row_desc(const common::ObRowDesc *&row_desc) const;
        /**
         * 添加一个需输出的column
         *
         * @note 只有通过复合列结算新生成的列才需要new_column_id
         * @param expr [in] 需输出的列（这个列可能是个复合列的结果）
         *
         * @return OB_SUCCESS或错误码
         *
         * NOTE: 如果传入的expr是一个条件表达式，本函数将不做检查，需要调用者保证
         */
        int add_output_column(const ObSqlExpression& expr);

        /**
         * 设置table_id
         * @param table_id [in] 被访问表的id
         *
         * @return OB_SUCCESS或错误码
         */
        int set_table(const uint64_t table_id);

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

        int set_scan_range(const ObRange &range)
        {
          UNUSED(range);
          return OB_ERROR;
        }

        int64_t to_string(char* buf, const int64_t buf_len) const;
      private:
        // disallow copy
        ObRpcScan(const ObRpcScan &other);
        ObRpcScan& operator=(const ObRpcScan &other);

        // member method
        ObScanParam *create_scan_param();
        int create_scan_param(mergeserver::ObMergerScanParam &scan_param);
        int get_next_compact_row(const common::ObRow*& row);
      private:
        static const int64_t REQUEST_EVENT_QUEUE_SIZE = 8192;
        // 等待结果返回的超时时间
        int64_t timeout_us_;
        mergeserver::ObMsSqlScanEvent *sql_scan_event_;
        mergeserver::ObMergerScanParam merger_scan_param_;
        common::ObScanParam scan_param_;
        common::ObRow cur_row_;
        common::ObRowDesc cur_row_desc_;
        uint64_t table_id_;
    };
  } // end namespace sql
} // end namespace oceanbase

#endif /* _OB_TABLE_RPC_SCAN_H */
