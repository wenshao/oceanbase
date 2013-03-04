////===================================================================
 //
 // ob_cellinfo_processor.h / hash / common / Oceanbase
 //
 // Copyright (C) 2010 Taobao.com, Inc.
 //
 // Created on 2010-09-27 by Yubai (yubai.lk@taobao.com) 
 //
 // -------------------------------------------------------------------
 //
 // Description
 //
 // ObCellInfo的预处理器
 // 由MemTable内部使用
 //
 // -------------------------------------------------------------------
 // 
 // Change Log
 //
////====================================================================

#ifndef  OCEANBASE_UPDATESERVER_CELLINFO_PROCESSOR_H_
#define  OCEANBASE_UPDATESERVER_CELLINFO_PROCESSOR_H_
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <pthread.h>
#include <new>
#include <algorithm>
#include "tbtimeutil.h"
#include "common/ob_define.h"
#include "common/murmur_hash.h"
#include "common/ob_read_common_data.h"
#include "common/ob_object.h"
#include "common/ob_action_flag.h"
#include "ob_ups_utils.h"

namespace oceanbase
{
  namespace updateserver
  {
    class CellInfoProcessor
    {
      public:
        inline CellInfoProcessor() : is_db_sem_(false), need_skip_(false), need_return_(false), cur_op_type_(0)
        {
        };
        inline ~CellInfoProcessor()
        {
        };
      public:
        // 返回语法是否合法
        inline bool analyse_syntax(common::ObMutatorCellInfo &mutator_cell_info)
        {
          bool bret = true;
          const common::ObCellInfo &cell_info = mutator_cell_info.cell_info;
          const common::ObObj &value = cell_info.value_;
          const common::ObObj &op_type = mutator_cell_info.op_type;
          if (common::ObExtendType == value.get_type())
          {
            switch (value.get_ext())
            {
              case common::ObActionFlag::OP_USE_DB_SEM:
                need_skip_ = true;
                is_db_sem_ = true;
                break;
              case common::ObActionFlag::OP_USE_OB_SEM:
                need_skip_ = true;
                is_db_sem_ = false;
                break;
              case common::ObActionFlag::OP_DEL_ROW:
                need_skip_ = false;
                cur_op_type_ = common::ObActionFlag::OP_DEL_ROW;
                break;
              default:
                TBSYS_LOG(WARN, "value invalid extend info %ld %s", value.get_ext(), print_cellinfo(&cell_info));
                bret = false;
                break;
            }
          }
          else if (common::ObExtendType == op_type.get_type()
                  && value.is_valid_type())
          {
            // op_type仅表示update和insert两种操作
            switch (op_type.get_ext() & common::ObActionFlag::OP_ACTION_FLAG_LOW_MASK)
            {
              case common::ObActionFlag::OP_UPDATE:
                need_skip_ = false;
                cur_op_type_ = common::ObActionFlag::OP_UPDATE;
                need_return_ = common::ObActionFlag::OP_RETURN_UPDATE_RESULT & op_type.get_ext() ? true : false;
                //mutator_cell_info.op_type.set_ext(common::ObActionFlag::OP_UPDATE);
                break;
              case common::ObActionFlag::OP_INSERT:
                need_skip_ = false;
                cur_op_type_ = common::ObActionFlag::OP_INSERT;
                need_return_ = common::ObActionFlag::OP_RETURN_UPDATE_RESULT & op_type.get_ext() ? true : false;
                //mutator_cell_info.op_type.set_ext(common::ObActionFlag::OP_UPDATE);
                break;
              default:
                TBSYS_LOG(WARN, "op_type invalid extend info %ld %s", op_type.get_ext(), print_cellinfo(&cell_info));
                bret = false;
                break;
            }
          }
          else
          {
            TBSYS_LOG(WARN, "no extend info in op_type or value op_type=%d value_type=%d %s",
                      op_type.get_type(), value.get_type(), print_cellinfo(&cell_info));
            bret = false;
          }

          return bret;
        };

        // 基本的表明 列名 rowkey检查
        inline bool cellinfo_check(const common::ObCellInfo &cell_info)
        {
          bool bret = true;
          if (NULL == cell_info.row_key_.ptr() || 0 == cell_info.row_key_.length())
          {
            TBSYS_LOG(WARN, "invalid rowkey ptr=%p length=%d",
                      cell_info.row_key_.ptr(), cell_info.row_key_.length());
            bret = false;
          }
          else if (NULL == cell_info.table_name_.ptr() || 0 == cell_info.table_name_.length())
          {
            TBSYS_LOG(WARN, "invalid table name ptr=%p length=%d",
                      cell_info.table_name_.ptr(), cell_info.table_name_.length());
            bret = false;
          }
          else if (common::ObActionFlag::OP_DEL_ROW != cur_op_type_
                  && (NULL == cell_info.column_name_.ptr() || 0 == cell_info.column_name_.length()))
          {
            TBSYS_LOG(WARN, "invalid column name ptr=%p length=%d op_type=%ld",
                      cell_info.column_name_.ptr(), cell_info.column_name_.length(), cur_op_type_);
            bret = false;
          }
          else
          {
            // do nothing
          }

          if (!bret)
          {
            TBSYS_LOG(WARN, "invalid cell_info %s", print_cellinfo(&cell_info));
          }
          return bret;
        }

        // rowkey合法性与schema的匹配检查
        inline static bool cellinfo_check(const common::ObCellInfo &cell_info,
                                          const CommonTableSchema &table_schema)
        {
          bool bret = true;
          if (cell_info.row_key_.length() > table_schema.get_rowkey_max_length()
              || cell_info.row_key_.length() < table_schema.get_split_pos())
          {
            TBSYS_LOG(WARN, "length=%d not match schema, schema_max=%d schema_split_pos=%d",
                      cell_info.row_key_.length(), table_schema.get_rowkey_max_length(), table_schema.get_split_pos());
            bret = false;
          }
          else if (table_schema.is_row_key_fixed_len()
                  && cell_info.row_key_.length() != table_schema.get_rowkey_max_length())
          {
            TBSYS_LOG(WARN, "length=%d not match schema, schema_max=%d must fixed len",
                      cell_info.row_key_.length(), table_schema.get_rowkey_max_length());
            bret = false;
          }
          else
          {
            // do nothing
          }

          if (!bret)
          {
            TBSYS_LOG(WARN, "invalid cell_info %s", print_cellinfo(&cell_info));
          }
          return bret;
        }

        // column信息合法性与schema的匹配检查
        inline static bool cellinfo_check(const common::ObCellInfo &cell_info,
                                          const CommonColumnSchema &column_schema)
        {
          bool bret = true;
          const common::ObObj &value = cell_info.value_;

          // 数据类型与schema匹配检查
          if (column_schema.get_type() != value.get_type()
              && common::ObNullType != value.get_type())
          {
            TBSYS_LOG(WARN, "schema_type=%d value_type=%d not match", column_schema.get_type(), value.get_type());
            bret = false;
          }
          else if (NULL != column_schema.get_join_info())
          {
            TBSYS_LOG(WARN, "column_id=%lu only depend by join table, cannot update", column_schema.get_id());
            bret = false;
          }
          else
          {
            if (common::ObVarcharType == value.get_type())
            {
              common::ObString tmp;
              if (common::OB_SUCCESS != value.get_varchar(tmp)
                  || tmp.length() > column_schema.get_size())
              {
                TBSYS_LOG(WARN, "invalid varchar schema_max_length=%ld ptr=%p length=%d",
                          column_schema.get_size(), tmp.ptr(), tmp.length());
                bret = false;
              }
            }
          }

          // 数据类型合法性检查
          if (bret)
          {
            switch (column_schema.get_type())
            {
              case common::ObCreateTimeType:
              case common::ObModifyTimeType:
                TBSYS_LOG(WARN, "cannot update value_type=%d", column_schema.get_type());
                bret = false;
                break;
              case common::ObSeqType:
                TBSYS_LOG(WARN, "not support seq_type now");
                bret = false;
                break;
              default:
                break;
            }
          }

          if (!bret)
          {
            TBSYS_LOG(WARN, "invalid cell_info %s", print_cellinfo(&cell_info));
          }
          return bret;
        }

        // 当前是否执行db语义
        // true 表示执行db语义
        // false 表示执行ob语义
        inline bool is_db_sem() const
        {
          return is_db_sem_;
        };

        inline int64_t get_op_type() const
        {
          return cur_op_type_;
        };

        inline int64_t need_skip() const
        {
          return need_skip_;
        };

        inline bool need_return() const
        {
          return need_return_;
        };

      private:
        bool is_db_sem_;
        bool need_skip_;
        bool need_return_;
        int64_t cur_op_type_;
    };
  }
}

#endif //OCEANBASE_UPDATESERVER_CELLINFO_PREPROCESSOR_H_

