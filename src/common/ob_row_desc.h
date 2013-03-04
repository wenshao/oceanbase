/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_row_desc.h
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#ifndef _OB_ROW_DESC_H
#define _OB_ROW_DESC_H 1
#include "common/ob_define.h"

namespace oceanbase
{
  namespace common
  {
    /// 行描述
    class ObRowDesc
    {
      public:
        ObRowDesc();
        ~ObRowDesc();
        /**
         * 根据表ID和列ID获得改列在元素数组中的下标
         *
         * @param table_id 表ID
         * @param column_id 列ID
         *
         * @return 下标或者OB_INVALID_INDEX
         */
        int64_t get_idx(const uint64_t table_id, const uint64_t column_id) const;
        /**
         * 根据列下标获得表ID和列ID
         *
         * @param idx
         * @param table_id [out]
         * @param column_id [out]
         *
         * @return OB_SUCCESS或错误码
         */
        int get_tid_cid(const int64_t idx, uint64_t &table_id, uint64_t &column_id) const;

        /// 一行中列的数目
        int64_t get_column_num() const;

        /// 添加下一列的描述信息
        int add_column_desc(const uint64_t table_id, const uint64_t column_id);

        /// 重置
        void reset();

        /// 获得内部运行时遇到的散列冲撞总数，用于监控和调优
        static uint64_t get_hash_collisions_count();
      private:
        struct Desc
        {
          uint64_t table_id_;
          uint64_t column_id_;

          bool is_invalid() const;
          bool operator== (const Desc &other) const;
        };
        struct DescIndex
        {
          Desc desc_;
          int64_t idx_;
        };
        int hash_find(const uint64_t table_id, const uint64_t column_id, const DescIndex *&desc_idx) const;
        int hash_insert(const uint64_t table_id, const uint64_t column_id, const int64_t index);
        int slow_find(const uint64_t table_id, const uint64_t column_id, const DescIndex *&desc_idx) const;
        int slow_insert(const uint64_t table_id, const uint64_t column_id, const int64_t index);
      private:
        static const int64_t MAX_COLUMNS_COUNT = common::OB_ROW_MAX_COLUMNS_COUNT; // 512
        static const int64_t HASH_BACKETS_COUNT = 1543;
        static volatile uint64_t HASH_COLLISIONS_COUNT;
        // data members
        Desc cells_desc_[MAX_COLUMNS_COUNT];
        int64_t cells_desc_count_;
        DescIndex hash_backets_[HASH_BACKETS_COUNT];
        DescIndex overflow_backets_[MAX_COLUMNS_COUNT];
        int64_t overflow_backets_count_;
    };

    inline int64_t ObRowDesc::get_column_num() const
    {
      return cells_desc_count_;
    }

    inline uint64_t ObRowDesc::get_hash_collisions_count()
    {
      return HASH_COLLISIONS_COUNT;
    }

  } // end namespace common
} // end namespace oceanbase

#endif /* _OB_ROW_DESC_H */

