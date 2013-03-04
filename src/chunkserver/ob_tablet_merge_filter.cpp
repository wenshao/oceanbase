/**
 * (C) 2010-2012 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_tablet_merge_filter.cpp for expire row of tablet when do 
 * daily merge. 
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */
#include <time.h>
#include <tblog.h>
#include "common/utility.h"
#include "ob_tablet_merge_filter.h"

namespace oceanbase
{
  namespace chunkserver
  {
    using namespace common;
    using namespace common::hash;
    using namespace sstable;

    const char* ObTabletMergerFilter::EXPIRE_CONDITION_CNAME_PREFIX = "__EXPIRE_COND_";

    ObTabletMergerFilter::ObTabletMergerFilter()
    : inited_(false), hash_map_inited_(false), 
      expire_row_filter_(DEFAULT_SSTABLE_ROW_COUNT, &expire_filter_allocator_), 
      schema_(NULL), table_id_(OB_INVALID_ID), frozen_version_(0), frozen_time_(0),
      column_group_num_(0), extra_column_cnt_(0), need_filter_(false)
    {

    }

    ObTabletMergerFilter::~ObTabletMergerFilter()
    {
      if (hash_map_inited_)
      {
        cname_to_idx_map_.destroy();
      }
      expire_row_filter_.destroy();
    }

    int ObTabletMergerFilter::init(const ObSchemaManagerV2& schema, 
      ObTablet* tablet, const int64_t frozen_version, const int64_t frozen_time)
    {
      int ret           = OB_SUCCESS;
      uint64_t table_id = OB_INVALID_ID;

      if (NULL == tablet || frozen_version <= 1 || frozen_time < 0)
      {
        TBSYS_LOG(WARN, "invalid param, tablet=%p, frozen_version=%ld, "
                        "frozen_time=%ld", 
          tablet, frozen_version, frozen_time);
        ret = OB_ERROR;
      }
      else if (OB_INVALID_ID == (table_id = tablet->get_range().table_id_))
      {
        TBSYS_LOG(WARN, "invalid table_id=%lu", table_id);
        ret = OB_ERROR;
      }
      else if (!hash_map_inited_)
      {
        ret = cname_to_idx_map_.create(HASH_MAP_BUCKET_NUM);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "failed to create hash map for select column "
                          "name to column index map ret=%d", ret);
        }
        else
        {
          hash_map_inited_ = true;
        }
      }

      if (OB_SUCCESS == ret)
      {
        reset();
        schema_ = &schema;
        table_id_ = table_id;
        frozen_version_ = frozen_version;
        frozen_time_ = frozen_time;
        need_filter_ = need_filter(frozen_version, tablet->get_last_do_expire_version());

        uint64_t column_group_ids[OB_MAX_COLUMN_GROUP_NUMBER];
        int32_t column_group_num = 
          sizeof(column_group_ids) / sizeof(column_group_ids[0]);
        if (OB_SUCCESS != (ret = schema.get_column_groups(table_id_,
                column_group_ids, column_group_num))
            || column_group_num <= 0)
        {
          TBSYS_LOG(WARN, "table=%lu get column groups error, column_group_num=%d", 
            table_id_, column_group_num);
        }
        else 
        {
          column_group_num_ = column_group_num;
        }
      }

      if (OB_SUCCESS == ret)
      {
        inited_ = true;
      }

      return ret;
    }

    void ObTabletMergerFilter::reset()
    {
      inited_ = false;
      cname_to_idx_map_.clear();
      expire_filter_.reset();
      expire_row_filter_.clear();
      schema_ = NULL;
      table_id_ = OB_INVALID_ID;
      frozen_version_ = 0;
      frozen_time_ = 0;
      column_group_num_ = 0;
      extra_column_cnt_ = 0;
      need_filter_ = false;
    }

    void ObTabletMergerFilter::clear_expire_bitmap()
    {
      expire_row_filter_.clear();
    }

    bool ObTabletMergerFilter::need_filter(const int64_t frozen_version, 
      const int64_t last_do_expire_version)
    {
      bool ret            = false;
      uint64_t column_id  = 0;
      int64_t duration    = 0;
      const ObTableSchema *table_schema = NULL;
      const char* expire_condition      = NULL;

      if (frozen_version <= 1 || last_do_expire_version < 0)
      {
        TBSYS_LOG(WARN, "invalid param, frozen_version=%ld, last_do_expire_version=%ld", 
          frozen_version, last_do_expire_version);
      }
      else if (NULL == (table_schema = schema_->get_table_schema(table_id_)))
      {
        TBSYS_LOG(ERROR, "table=%lu not exist.", table_id_);
      }
      else if (frozen_version >= table_schema->get_expire_frequency() + last_do_expire_version)
      {
        if (OB_SUCCESS == table_schema->get_expire_condition(column_id, duration))
        {
          ret = true;
        }

        if (NULL != (expire_condition = table_schema->get_expire_condition())
                 && expire_condition[0] != '\0')
        {
          ret = true;
        }
        TBSYS_LOG(INFO, "expire info, frozen_version=%ld, expire_frequency=%ld, "
                        "last_do_expire_version=%ld, table_id=%lu, "
                        "expire column id=%ld, expire_duration=%ld, "
                        "expire_condition=%s, need_filter=%d", 
          frozen_version, table_schema->get_expire_frequency(), 
          last_do_expire_version, table_id_, column_id, duration, 
          (NULL == expire_condition) ? "" : expire_condition, ret);
      }

      return ret;
    }

    int ObTabletMergerFilter::get_expire_column_info(const ObColumnSchemaV2*& column_schema,
      int64_t& duration) const
    {
      int ret                             = OB_SUCCESS;
      const ObTableSchema* table_schema   = NULL; 
      const ObColumnSchemaV2* col_schema  = NULL;
      uint64_t column_id                  = 0;

      column_schema = NULL;
      duration = 0;

      if (!inited_)
      {
        TBSYS_LOG(WARN, "tablet merge filter doesn't initialize");
        ret = OB_ERROR;
      }
      else if (NULL == (table_schema = schema_->get_table_schema(table_id_)))
      {
        TBSYS_LOG(ERROR, "table = %lu not exist.", table_id_);
        ret = OB_ERROR;
      }
      else if (OB_SUCCESS != ( ret = 
            table_schema->get_expire_condition(column_id, duration)))
      {
        TBSYS_LOG(INFO, "table=%lu do not contain old expire condition", table_id_);
        ret = OB_SUCCESS;
      }
      else if (NULL == (col_schema = schema_->get_column_schema(table_id_, column_id)))
      {
        TBSYS_LOG(WARN, "define expire table=%lu, column id= %lu, not exist in table schema", 
            table_id_, column_id);
        ret = OB_ERROR;
      }
      else if ( ObDateTimeType != col_schema->get_type()
          && ObPreciseDateTimeType != col_schema->get_type()
          && ObCreateTimeType !=  col_schema->get_type()
          && ObModifyTimeType !=  col_schema->get_type())
      {
        TBSYS_LOG(WARN, "define expire column type=%d, not a date time type", 
            col_schema->get_type());
        ret = OB_ERROR;
      }
      else
      {
        column_schema = col_schema;
        duration *= 1000000L; // trans to microseconds;
      }

      return ret;
    }

    int ObTabletMergerFilter::build_expire_column_filter(ObScanParam& scan_param)
    {
      int ret                   = OB_SUCCESS;
      uint64_t column_id        = 0;
      int64_t duration          = 0;
      int hash_err              = 0;
      int64_t expire_column_idx = -1;
      int64_t column_size       = 0;
      const ObColumnSchemaV2* column_schema = NULL;
      ObString column_name;
      ObObj cond_obj;

      column_size = scan_param.get_column_id_size();
      if (column_size <= 0)
      {
        TBSYS_LOG(WARN, "invalid scan_param, no column in scan_param to scan, "
                        "column_size=%ld",
          column_size);
        ret = OB_ERROR;
      }
      else 
      {
        ret = get_expire_column_info(column_schema, duration);
      }

      if (OB_SUCCESS == ret && NULL != column_schema && duration > 0)
      {
        column_id = column_schema->get_id();
        column_name.assign_ptr(const_cast<char*>(column_schema->get_name()), 
          static_cast<int32_t>(strlen(column_schema->get_name())));

        if (HASH_NOT_EXIST == (hash_err = 
            cname_to_idx_map_.get(column_name, expire_column_idx)))
        {
          // expire column isn't in scan param, add the expire column to scan
          ret = add_extra_basic_column(scan_param, column_name, expire_column_idx);
        }
        else if (HASH_EXIST != hash_err)
        {
          TBSYS_LOG(WARN, "failed to find column_name=%s in hash map, hash_err=%d",
            column_schema->get_name(), hash_err);
          ret = OB_ERROR;
        }

        if (OB_SUCCESS == ret)
        {
          if (expire_column_idx < 0)
          {
            TBSYS_LOG(WARN, "expire column isn't in scan param, "
                            "expire_column_idx=%ld, column_size=%ld",
              expire_column_idx, scan_param.get_column_id_size());
            ret = OB_ERROR;
          }
          else
          {
            switch (column_schema->get_type())
            {
              case ObDateTimeType:
                cond_obj.set_datetime(frozen_time_ - duration);
                break;
              case ObPreciseDateTimeType:
                cond_obj.set_precise_datetime(frozen_time_ - duration);
                break;
              case ObCreateTimeType:
                cond_obj.set_createtime(frozen_time_ - duration);
                break;
              case ObModifyTimeType:
                cond_obj.set_modifytime(frozen_time_ - duration);
                break;
              default:
                TBSYS_LOG(WARN, "invalid expire column type=%d", 
                  column_schema->get_type());
                ret = OB_ERROR;
                break;
            }
            
            if (OB_SUCCESS == ret)
            {
              ret = expire_filter_.add_cond(expire_column_idx, LT, cond_obj);
            }
          }
        }
      }

      return ret;
    }

    int ObTabletMergerFilter::replace_system_variable(char* expire_condition, 
      const int64_t buf_size)
    {
      int ret = OB_SUCCESS;
      struct tm* tm = NULL;
      time_t frozen_second = 0;
      char replace_str_buf[32];

      if (NULL == expire_condition || buf_size <= 0)
      {
        TBSYS_LOG(WARN, "invalid param, expire_condition=%p, buf_size=%ld",
          expire_condition, buf_size);
        ret = OB_ERROR;
      }
      else 
      {
        frozen_second = static_cast<time_t>(frozen_time_ / 1000 / 1000);
        tm = localtime(&frozen_second);
        if (NULL == tm)
        {
          TBSYS_LOG(WARN, "failed to transfer frozen time to tm, "
                          "frozen_time_=%ld, frozen_second=%ld",
            frozen_time_, frozen_second);
          ret = OB_ERROR;
        }
        else 
        {
          strftime(replace_str_buf, sizeof(replace_str_buf), 
            "#%Y-%m-%d %H:%M:%S#", tm);
          ret = replace_str(expire_condition, buf_size, SYS_DATE, replace_str_buf);
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "failed to replace $SYS_DATE in expire condition");
          }
        }
      }

      return ret;
    }

    int ObTabletMergerFilter::infix_to_postfix(const ObString& expr,
      ObScanParam& scan_param, ObObj* objs, const int64_t obj_count)
    {
      int ret               = OB_SUCCESS;
      int hash_ret          = 0;
      int i                 = 0;
      int64_t type          = 0;
      int64_t val_index     = 0;
      int64_t postfix_size  = 0;
      ObString key_col_name;
      ObArrayHelper<ObObj> expr_array;

      if (NULL == objs || obj_count <= 0)
      {
        TBSYS_LOG(WARN, "invalid param, objs=%p, obj_count=%ld", 
          objs, obj_count);
        ret = OB_ERROR;
      }
      else
      {
        expr_array.init(obj_count, objs);
        ret = post_expression_parser_.parse(expr, expr_array);
        TBSYS_LOG(DEBUG, "parse done. expr_array len=%ld",
           expr_array.get_array_index());
        if (OB_SUCCESS == ret)
        {
          postfix_size = expr_array.get_array_index();
        }
        else
        {
          TBSYS_LOG(WARN, "parse infix expression to postfix expression "
                          "error, ret=%d", ret);
        }
      }

      if (OB_SUCCESS == ret)
      {
        // 基本算法如下：
        //  1. 遍历expr_array数组，将其中类型为COLUMN_IDX的Obj读出
        //  2. 将obj中表示列名的值key_col_name读出，到hashmap中查找对应index
        //      scan_param.some_hash_map(key_col_name, val_index)
        //  3. 如果找到，则将obj的内容填充为val_index
        //     如果找不到，则报错返回

        i = 0;
        while(i < postfix_size - 1)
        {
          if (OB_SUCCESS != expr_array.at(i)->get_int(type))
          {
            TBSYS_LOG(WARN, "unexpected data type. int expected, but actual type is %d", 
                expr_array.at(i)->get_type());
            ret = OB_ERR_UNEXPECTED;
            break;
          }
          else
          {
            if (ObExpression::COLUMN_IDX == type)
            {
              if (OB_SUCCESS != expr_array.at(i+1)->get_varchar(key_col_name))  
              {
                TBSYS_LOG(WARN, "unexpected data type. varchar expected, "
                                "but actual type is %d", 
                    expr_array.at(i+1)->get_type());
                ret = OB_ERR_UNEXPECTED;
                break;
              }
              else
              {
                hash_ret = cname_to_idx_map_.get(key_col_name, val_index);
                if (HASH_EXIST != hash_ret)
                {
                  ret = add_extra_basic_column(scan_param, key_col_name, val_index);
                  if (OB_SUCCESS != ret || val_index < 0)
                  {
                    TBSYS_LOG(WARN, "failed add extra column into scan param, "
                                    "column_name=%.*s, table_id=%lu, val_index=%ld", 
                      key_col_name.length(), key_col_name.ptr(), table_id_, val_index);
                    break;
                  }
                }
                if (OB_SUCCESS == ret)
                {
                  /* decode. quite simple, isn't it. */
                  expr_array.at(i+1)->set_int(val_index);
                  TBSYS_LOG(DEBUG, "succ decode.  key(%.*s) -> %ld", 
                      key_col_name.length(), key_col_name.ptr(), val_index);
                }
              }
            }/* only column name needs to decode. other type is ignored */
            i += 2; // skip <type, data> (2 objects as an element)
          }
        }
      }

      return ret;
    }

    int ObTabletMergerFilter::add_extra_basic_column(ObScanParam& scan_param, 
      const ObString& column_name, int64_t& column_index)
    {
      int ret             = OB_SUCCESS;
      int hash_err        = 0;
      int64_t column_size = 0;
      const ObTableSchema* table_schema     = NULL; 
      const ObColumnSchemaV2* column_schema = NULL;
      ObString table_name;

      column_index = -1;

      if (NULL == column_name.ptr() || column_name.length() <= 0)
      {
        TBSYS_LOG(WARN, "invalid param, column_name_ptr=%p, column_name_len=%d",
          column_name.ptr(), column_name.length());
        ret = OB_ERROR;
      }
      else if (NULL == (table_schema = schema_->get_table_schema(table_id_)))
      {
        TBSYS_LOG(ERROR, "table=%lu not exist", table_id_);
        ret = OB_ERROR;
      }
      else 
      {
        table_name.assign_ptr(const_cast<char*>(table_schema->get_table_name()), 
          static_cast<int32_t>(strlen(table_schema->get_table_name())));
        column_schema = schema_->get_column_schema(table_name, column_name);
        if (NULL == column_schema)
        {
          TBSYS_LOG(WARN, "failed to get column schema, table_name=%s, "
                          "column_name=%.*s",
            table_schema->get_table_name(), column_name.length(), column_name.ptr());
          ret = OB_ERROR;
        }
      }

      if (OB_SUCCESS == ret)
      {
        column_size = scan_param.get_column_id_size();
        if (HASH_INSERT_SUCC != (hash_err = cname_to_idx_map_.set(
          column_name, column_size)))
        {
          TBSYS_LOG(WARN, "fail to add expire column <column_name, idx> pair "
                          "hash_err=%d, column_name=%.*s, expire_column_idx=%ld", 
            hash_err, column_name.length(), column_name.ptr(), column_size);
          ret = OB_ERROR;
        }
        else if (OB_SUCCESS != (ret = scan_param.add_column(column_schema->get_id())))
        {
          TBSYS_LOG(WARN, "failed to add expire column into scan_param, "
                          "column_id=%lu, ret=%d",
            column_schema->get_id(), ret);
        }
        else 
        {
          column_index = column_size;
          extra_column_cnt_ += 1;
        }
      }

      return ret;
    }

    int ObTabletMergerFilter::add_extra_composite_column(ObScanParam& scan_param,
      const ObObj* post_expr)
    {
      int ret = OB_SUCCESS;
      int hash_ret = 0;
      int64_t column_size = 0;
      int64_t comp_cname_len = 0;
      char comp_cname[MAX_COMPOSITE_CNAME_LEN];
      ObString comp_cname_str;
      ObObj true_obj;

      if (NULL == post_expr)
      {
        TBSYS_LOG(WARN, "invalid param, post_expr=%p", post_expr);
        ret = OB_ERROR;
      }
      else 
      {
        column_size = scan_param.get_column_id_size();
        comp_cname_len = snprintf(comp_cname, sizeof(comp_cname),
          "%s%ld", EXPIRE_CONDITION_CNAME_PREFIX, column_size);
        comp_cname_str.assign_ptr(comp_cname, static_cast<int32_t>(comp_cname_len));
        if (HASH_INSERT_SUCC != (hash_ret = cname_to_idx_map_.set(
          comp_cname_str, column_size)))
        {
          TBSYS_LOG(WARN, "fail to add expire composite column <column_name, idx> pair "
                          "hash_err=%d, column_name=%s, expire_column_idx=%ld", 
            hash_ret, comp_cname, column_size);
          ret = OB_ERROR;
        }
        else if (OB_SUCCESS != (ret = scan_param.add_column(post_expr)))
        {
          TBSYS_LOG(WARN, "failed to add expire composite column into scan_param, "
                          "column_name=%s, ret=%d",
            comp_cname, ret);
        }
        else
        {
          true_obj.set_bool(true);
          ret = expire_filter_.add_cond(scan_param.get_column_id_size(), 
            EQ, true_obj);
          if (OB_SUCCESS == ret)
          {
            extra_column_cnt_ += 1;
          }
        }
      }

      return ret;
    }

    int ObTabletMergerFilter::build_expire_condition_filter(ObScanParam& scan_param)
    {
      int ret                           = OB_SUCCESS;
      const ObTableSchema *table_schema = NULL;
      const char* expire_condition      = NULL;
      char infix_condition_expr[OB_MAX_EXPIRE_CONDITION_LENGTH];
      ObObj post_expr[OB_MAX_COMPOSITE_SYMBOL_COUNT];
      ObString cond_expr;
      ObObj true_obj;

      if (NULL == (table_schema = schema_->get_table_schema(table_id_)))
      {
        TBSYS_LOG(ERROR, "table=%lu not exist", table_id_);
        ret = OB_ERROR;
      }
      else if (NULL != (expire_condition = table_schema->get_expire_condition())
               && expire_condition[0] != '\0')
      {
        if (static_cast<int64_t>(strlen(expire_condition)) >= OB_MAX_EXPIRE_CONDITION_LENGTH)
        {
          TBSYS_LOG(WARN, "expire condition too large, expire_condition_len=%zu, "
                          "max_condition_len=%ld",
            strlen(expire_condition), OB_MAX_EXPIRE_CONDITION_LENGTH);
          ret = OB_ERROR;
        }
        else
        {
          strcpy(infix_condition_expr, expire_condition);
          ret = replace_system_variable(infix_condition_expr, 
            OB_MAX_EXPIRE_CONDITION_LENGTH);
          if (OB_SUCCESS == ret)
          {
            cond_expr.assign_ptr(infix_condition_expr, 
              static_cast<int32_t>(strlen(infix_condition_expr)));
            ret = infix_to_postfix(cond_expr, scan_param, post_expr, 
              OB_MAX_COMPOSITE_SYMBOL_COUNT);
            if (OB_SUCCESS != ret)
            {
              TBSYS_LOG(WARN, "failed to transfer infix expression to postfix "
                              "expression, infix_expr=%s", 
                infix_condition_expr);
            }
            else 
            {
              TBSYS_LOG(INFO, "success to transfer infix expression to postfix "
                              "expression, infix_expr=%s", 
                infix_condition_expr);
            }
          }

          if (OB_SUCCESS == ret)
          {
            ret = add_extra_composite_column(scan_param, post_expr);
          }
        }
      }

      return ret;
    }

    int ObTabletMergerFilter::fill_cname_to_idx_map(const uint64_t column_group_id)
    {
      int ret                     = OB_SUCCESS;
      int hash_err                = 0;
      int32_t size                = 0;
      const ObColumnSchemaV2* col = NULL;
      ObString column_name;

      col = schema_->get_group_schema(table_id_, column_group_id, size);
      if (NULL == col || size <= 0)
      {
        TBSYS_LOG(WARN, "cann't find this column group=%lu", column_group_id);
        ret = OB_ERROR;
      }
      else
      {
        for(int64_t i = 0; i < size; ++i)
        {
          column_name.assign_ptr(const_cast<char*>((col + i)->get_name()), 
            static_cast<int32_t>(strlen((col + i)->get_name())));
          if (HASH_INSERT_SUCC != (hash_err = cname_to_idx_map_.set(column_name, i)))
          {
            TBSYS_LOG(WARN, "fail to add column <column_name, idx> pair hash_err=%d", 
              hash_err);
            ret = OB_ERROR;
            break;
          }
        }
      }

      return ret;
    }

    int ObTabletMergerFilter::adjust_scan_param(const int64_t column_group_idx, 
      const uint64_t column_group_id, ObScanParam& scan_param)
    {
      int ret = OB_SUCCESS;

      if (!inited_)
      {
        TBSYS_LOG(WARN, "tablet merge filter doesn't initialize");
        ret = OB_ERROR;
      } 
      else if (column_group_idx < 0 || column_group_idx >= column_group_num_
               || OB_INVALID_ID == column_group_id)
      {
        TBSYS_LOG(WARN, "invalid param, column_group_idx=%ld, column_group_num_=%ld, "
                        "column_group_id=%lu", 
          column_group_idx, column_group_num_, column_group_id);
        ret = OB_ERROR;
      }
      else if (need_filter_ && 0 == column_group_idx)
      {
        extra_column_cnt_ = 0;
        cname_to_idx_map_.clear();
        expire_filter_.reset();
        ret = fill_cname_to_idx_map(column_group_id);
        if (OB_SUCCESS == ret 
            && cname_to_idx_map_.size() != scan_param.get_column_id_size())
        {
          TBSYS_LOG(WARN, "column count in scan param is unexpected, "
                          "column_count_in_param=%ld, scan_column_count=%ld", 
            scan_param.get_column_id_size(), cname_to_idx_map_.size());
          ret = OB_ERROR;
        }
        if (OB_SUCCESS == ret)
        {
          ret = build_expire_column_filter(scan_param);
        }
        if (OB_SUCCESS == ret)
        {
          ret = build_expire_condition_filter(scan_param);
        }
      }

      return ret;
    }

    int ObTabletMergerFilter::check_and_trim_row(const int64_t column_group_idx, 
      const int64_t row_num, ObSSTableRow& sstable_row, bool& is_expired)
    {
      int ret           = OB_SUCCESS;
      const ObObj* objs = NULL;
      int64_t obj_count = 0;
      is_expired        = false;

      if (!inited_)
      {
        TBSYS_LOG(WARN, "tablet merge filter doesn't initialize");
        ret = OB_ERROR;
      }
      else if (column_group_idx < 0 || column_group_idx >= column_group_num_
               || row_num < 0)
      {
        TBSYS_LOG(WARN, "invalid param, column_group_idx=%ld, column_group_num_=%ld, "
                        "row_num=%ld", 
          column_group_idx, column_group_num_, row_num);
        ret = OB_ERROR;
      }
      else if (need_filter_ && 0 == column_group_idx)
      {
        objs = sstable_row.get_row_objs(obj_count);
        if (obj_count != cname_to_idx_map_.size())
        {
          TBSYS_LOG(WARN, "column count in sstable row is unexpected, "
                          "column_count_in_row=%ld, scan_column_count=%ld", 
            obj_count, cname_to_idx_map_.size());
          ret = OB_ERROR;
        }
        else 
        {
          /**
           * because check() default return true, if there is not 
           * condition in filter, it return true, we call 
           * default_false_check() to check whether the current is 
           * expired, if no filter, it return false. it only support 
           * 'AND', only if all the expire condition is satisfied, the 
           * function default_false_check() return true. 
           */
          ret = expire_filter_.default_false_check(objs, obj_count, is_expired);
        }
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "failed to filter expire sstable row, objs=%p, "
                          "obj_count=%ld, ret=%d", 
            objs, obj_count, ret);
        }
        else
        {
          /**
           * in order to do expire, we add some extra columns after the 
           * basic columns to scan, after do expire, we trim the extra 
           * columns here.
           */
          if (extra_column_cnt_ > 0)
          {
            ret = sstable_row.set_obj_count(obj_count - extra_column_cnt_);
          }

          if (OB_SUCCESS == ret && is_expired && column_group_num_ > 1)
          {
            ret = expire_row_filter_.set(row_num, true);
            if (OB_SUCCESS != ret)
            {
              TBSYS_LOG(WARN, "failed to set expire bitmap filter, column_group_idx=%ld, "
                              "row_num=%ld, is_expired=%d, ret=%d", 
                column_group_idx, row_num, is_expired, ret);
            }
          }
        }
      }
      else if (need_filter_ && column_group_idx > 0 && column_group_num_ > 1)
      {
        is_expired = expire_row_filter_.test(row_num);
      }

      return ret;
    }
  } // namespace oceanbase::chunkserver
} // namespace Oceanbase
