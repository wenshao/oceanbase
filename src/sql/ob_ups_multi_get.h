/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_ups_multi_get.h
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#ifndef _OB_UPS_MULTI_GET_H
#define _OB_UPS_MULTI_GET_H 1

#include "common/ob_define.h"
#include "common/ob_get_param.h"
#include "common/ob_ups_rpc_stub.h"
#include "ob_rowkey_phy_operator.h"
#include "common/ob_row.h"

namespace oceanbase
{
  using namespace common;
  namespace sql
  {
    // 用于CS从UPS获取多行数据
    class ObUpsMultiGet: public ObRowkeyPhyOperator
    {
      public:
        ObUpsMultiGet();
        virtual ~ObUpsMultiGet();

        virtual int set_child(int32_t child_idx, ObPhyOperator &child_operator);
        virtual int open();
        virtual int close();
        virtual int get_next_row(const ObString *&rowkey, const ObRow *&row);
        virtual void set_row_desc(const ObRowDesc &row_desc);
        virtual int64_t to_string(char* buf, const int64_t buf_len) const;
        virtual int get_row_desc(const common::ObRowDesc *&row_desc) const {row_desc=NULL;return OB_NOT_IMPLEMENT;}

        inline int set_rpc_stub(ObUpsRpcStub *rpc_stub);
        inline int set_network_timeout(int64_t network_timeout);
        virtual void reset();

        /**
         * 设置MultiGet的参数
         */
        inline void set_get_param(const ObGetParam &get_param);
      private:
        // disallow copy
        ObUpsMultiGet(const ObUpsMultiGet &other);
        ObUpsMultiGet& operator=(const ObUpsMultiGet &other);

      protected:
        int next_get_param();
        bool check_inner_stat();

      protected:
        // data members
        const ObGetParam *get_param_;
        ObGetParam cur_get_param_;
        ObNewScanner cur_new_scanner_;
        ObUpsRpcStub *rpc_stub_;
        ObUpsRow cur_ups_row_;
        ObString cur_rowkey_;
        ObServer ups_server;
        int64_t got_cell_count_;
        const ObRowDesc *row_desc_;
        int64_t network_timeout_;
    };

    int ObUpsMultiGet::set_network_timeout(int64_t network_timeout)
    {
      int ret = OB_SUCCESS;
      if(network_timeout <= 0)
      {
        ret = OB_INVALID_ARGUMENT;
        TBSYS_LOG(WARN, "network_timeout should be positive[%ld]", network_timeout);
      }
      else
      {
        network_timeout_ = network_timeout;
      }
      return ret;
    }

    int ObUpsMultiGet::set_rpc_stub(ObUpsRpcStub *rpc_stub)
    {
      int ret = OB_SUCCESS;
      if(NULL == rpc_stub)
      {
        ret = OB_INVALID_ARGUMENT;
        TBSYS_LOG(WARN, "rpc_stub is null");
      }
      else
      {
        rpc_stub_ = rpc_stub;
      }
      return ret;
    }

    void ObUpsMultiGet::set_get_param(const ObGetParam &get_param)
    {
      get_param_ = &get_param;
    }
  } // end namespace sql
} // end namespace oceanbase

#endif /* _OB_UPS_MULTI_GET_H */
