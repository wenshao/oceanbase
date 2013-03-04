/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 * 
 * Version: $Id$
 *
 * ob_nb_accessor.h
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@taobao.com>
 *
 */

#ifndef _OB_NB_ACCESSOR_H
#define _OB_NB_ACCESSOR_H

#include "common/ob_common_param.h"
#include "common/ob_malloc.h"
#include "common/ob_scanner.h"
#include "common/ob_array.h"
#include "common/ob_scan_param.h"
#include "common/ob_get_param.h"
#include "common/ob_tsi_factory.h"
#include "common/ob_client_proxy.h"
#include "common/ob_object.h"
#include "common/ob_mod_define.h"
#include "common/hash/ob_hashmap.h"
#include "common/ob_simple_condition.h"
#include "common/ob_easy_array.h"
#include "common/ob_string.h"
#include "common/ob_mutator_helper.h"

#define COLUMN_NAME_MAP_BUCKET_NUM 100

class MyIterator;

namespace oceanbase
{
  namespace common
  {
    typedef EasyArray<const char*> SC;

    class ScanConds : public EasyArray<ObSimpleCond>
    {
    public:
      ScanConds();
      ScanConds(const char* column_name, const ObLogicOperator& cond_op, int64_t value);
      ScanConds(const char* column_name, const ObLogicOperator& cond_op, ObString& value);

      ScanConds& operator()(const char* column_name, const ObLogicOperator& cond_op, int64_t value);
      ScanConds& operator()(const char* column_name, const ObLogicOperator& cond_op, ObString& value);
    };
    
    class TableRow;
    class QueryRes;

    class ObNbAccessor
    {
    public:
      ObNbAccessor();
      virtual ~ObNbAccessor();

      int init(ObClientProxy* client_proxy);
      int insert(const char* table_name, const ObString& rowkey, const KV& kv);
      int update(const char* table_name, const ObString& rowkey, const KV& kv);
      int delete_row(const char* table_name, const ObString& rowkey);

      virtual int scan(QueryRes*& res, const char* table_name, const ObRange& range, const SC& select_columns, const ScanConds& scan_conds);
      virtual int scan(QueryRes*& res, const char* table_name, const ObRange& range, const SC& select_columns);
      virtual int get(QueryRes*& res, const char* table_name, const ObString& rowkey, const SC& select_columns);
      void release_query_res(QueryRes* res);

  public:
      struct BatchHandle
      {
        ObMutator* mutator_;
        ObMutatorHelper mutator_helper_;
        bool modified;

        BatchHandle() : mutator_(NULL), modified(false)
        {
        }
      };

      int batch_begin(BatchHandle& handle);
      int batch_update(BatchHandle& handle, const char* table_name, const ObString& rowkey, const KV& kv);
      int batch_insert(BatchHandle& handle, const char* table_name, const ObString& rowkey, const KV& kv);
      int batch_end(BatchHandle& handle);
      
    private:
      bool check_inner_stat();

      ObClientProxy* client_proxy_;
    };

    //用于scan和get操作的结果，表示表单中的一行数据
    class TableRow
    {
    public:
      TableRow();
      virtual ~TableRow();

      int init(hash::ObHashMap<const char*, int64_t>* cell_map, ObCellInfo* cells, int64_t cell_count);

      virtual ObCellInfo* get_cell_info(int64_t index) const;
      //通过列名获取当前行的cell
      virtual ObCellInfo* get_cell_info(const char* column_name) const;

      int set_cell(ObCellInfo* cell, int64_t index);
      int64_t count() const;

    private:
      bool check_inner_stat() const;

      hash::ObHashMap<const char*,int64_t>* cell_map_;
      ObCellInfo* cells_;
      int64_t cell_count_;
    };

    //scan和get操作的返回的对象，包含了一个或者多个TableRow
    class QueryRes
    {
    public:
      QueryRes();
      virtual ~QueryRes();

      virtual int get_row(TableRow** table_row);
      virtual int next_row();

      TableRow* get_only_one_row(); //取出第一个TableRow

      int init(const SC& sc);

      inline ObScanner* get_scanner()
      {
        return &scanner_;
      }

      friend class ::MyIterator;
      
    private:
      TableRow cur_row_;
      ObScanner scanner_;
      ObScannerIterator scanner_iter_;
      hash::ObHashMap<const char*,int64_t> cell_map_; //保存列名到列序号的对应关系
    };
  }
}

#endif /* _OB_NB_ACCESSOR_H */


