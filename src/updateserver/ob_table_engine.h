////===================================================================
 //
 // ob_table_engine.h / hash / common / Oceanbase
 //
 // Copyright (C) 2010, 2011 Taobao.com, Inc.
 //
 // Created on 2010-09-09 by Yubai (yubai.lk@taobao.com) 
 //
 // -------------------------------------------------------------------
 //
 // Description
 //
 //
 // -------------------------------------------------------------------
 // 
 // Change Log
 //
////====================================================================

#ifndef  OCEANBASE_UPDATESERVER_TABLE_ENGINE_H_
#define  OCEANBASE_UPDATESERVER_TABLE_ENGINE_H_
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <pthread.h>
#include <new>
#include <algorithm>
#include "common/ob_define.h"
#include "common/murmur_hash.h"
#include "common/ob_read_common_data.h"
#include "common/utility.h"
#include "common/ob_crc64.h"
#include "ob_ups_utils.h"
#include "ob_ups_compact_cell_iterator.h"

namespace oceanbase
{
  namespace updateserver
  {
    const int64_t CELL_INFO_SIZE_UNIT = 1024L;

    struct ObCellInfoNode
    {
      int64_t modify_time;
      ObCellInfoNode *next;
      char buf[0];

      std::pair<int64_t, int64_t> get_size_and_cnt() const;
    };

    class ObCellInfoNodeIterable
    {
      struct CtrlNode
      {
        uint64_t column_id;
        common::ObObj value;
        CtrlNode *next;
      };
      public:
        ObCellInfoNodeIterable();
        virtual ~ObCellInfoNodeIterable();
      public:
        int next_cell();
        int get_cell(uint64_t &column_id, common::ObObj &value);
        void reset();
      public:
        void set_cell_info_node(const ObCellInfoNode *cell_info_node);
        void set_mtime(const uint64_t column_id, const ObModifyTime value);
        void set_rne();
        void set_head();
      private:
        const ObCellInfoNode *cell_info_node_;
        bool is_iter_end_;
        ObUpsCompactCellIterator cci_;
        CtrlNode *ctrl_list_;
        CtrlNode head_node_;
        CtrlNode rne_node_;
        CtrlNode mtime_node_;
    };

    class ObCellInfoNodeIterableWithCTime : public ObCellInfoNodeIterable
    {
      public:
        ObCellInfoNodeIterableWithCTime();
        virtual ~ObCellInfoNodeIterableWithCTime();
      public:
        int next_cell();
        int get_cell(uint64_t &column_id, common::ObObj &value);
        void reset();
      public:
        void set_ctime_column_id(const uint64_t column_id);
        void set_need_nop();
      private:
        uint64_t ctime_column_id_;
        uint64_t column_id_;
        common::ObObj value_;
        bool is_iter_end_;
        bool need_nop_;
    };

    struct TEHashKey
    {
      uint32_t table_id;
      int32_t rk_length;
      const char *row_key;

      TEHashKey() : table_id(UINT32_MAX),
                    rk_length(0),
                    row_key(NULL)
      {
      };
      inline int64_t hash() const
      {
        return (common::murmurhash2(row_key, rk_length, 0) + table_id);
      };
      inline bool operator == (const TEHashKey &other) const
      {
        bool bret = false;
        if (table_id == other.table_id
            && (row_key == other.row_key
                || 0 == memcmp(row_key, other.row_key, std::min(rk_length, other.rk_length))))
        {
          bret = (rk_length == other.rk_length);
        }
        return bret;
      };
    };
    
    struct TEKey
    {
      uint64_t table_id;
      uint64_t rowkey_prefix;
      common::ObString row_key;

      TEKey() : table_id(common::OB_INVALID_ID), rowkey_prefix(0), row_key()
      {
      };
      inline int64_t hash() const
      {
        return (common::murmurhash2(row_key.ptr(), row_key.length(), 0) + table_id);
      };
      inline void checksum(common::ObBatchChecksum &bc) const
      {
        bc.fill(&table_id, sizeof(table_id));
        bc.fill(row_key.ptr(), row_key.length());
      };
      inline const char *log_str() const
      {
        static const int32_t BUFFER_SIZE = 2048;
        static __thread char BUFFER[2][BUFFER_SIZE];
        static __thread int64_t i = 0;
        if (NULL != row_key.ptr()
            && 0 != row_key.length()
            && !isprint(row_key.ptr()[0]))
        {
          char hex_buffer[BUFFER_SIZE] = {'\0'};
          common::hex_to_str(row_key.ptr(), row_key.length(), hex_buffer, BUFFER_SIZE);
          snprintf(BUFFER[i % 2], BUFFER_SIZE, "table_id=%lu rowkey_prefix=%lu row_ptr=%p row_key=[0x %s] key_length=%d",
                  table_id, rowkey_prefix, row_key.ptr(), hex_buffer, row_key.length());
        }
        else
        {
          snprintf(BUFFER[i % 2], BUFFER_SIZE, "table_id=%lu rowkey_prefix=%lu row_ptr=%p row_key=[%.*s] key_length=%d",
                  table_id, rowkey_prefix, row_key.ptr(), row_key.length(), row_key.ptr(), row_key.length());
        }
        return BUFFER[i++ % 2];
      };
      inline bool operator == (const TEKey &other) const
      {
        return (table_id == other.table_id
                && rowkey_prefix == other.rowkey_prefix
                && row_key == other.row_key);
      };
      inline bool operator != (const TEKey &other) const
      {
        return (table_id != other.table_id
                || rowkey_prefix != other.rowkey_prefix
                || row_key != other.row_key);
      };
      inline int operator - (const TEKey &other) const
      {
        int ret = 0;
        if (table_id > other.table_id)
        {
          ret = 1;
        }
        else if (table_id < other.table_id)
        {
          ret = -1;
        }
        else
        { 
          int mc_ret = memcmp(&rowkey_prefix, &(other.rowkey_prefix), sizeof(rowkey_prefix));
          if (0 < mc_ret)
          {
            ret = 1;
          }
          else if (0 > mc_ret)
          {
            ret = -1;
          }
          else if (row_key > other.row_key)
          {
            ret = 1;
          }
          else if (row_key < other.row_key)
          {
            ret = -1;
          }
          else
          {
            ret = 0;
          }
        }
        return ret;
      };
    };

    struct TEValue
    {
      int16_t cell_info_cnt;
      int16_t cell_info_size; // 单位为1K
      int16_t reserve1;
      int16_t reserve2;
      ObCellInfoNode *list_head;
      ObCellInfoNode *list_tail;

      TEValue()
      {
        reset();
      };
      inline void reset()
      {
        cell_info_cnt = 0;
        cell_info_size = 0;
        reserve1 = 0;
        reserve2 = 0;
        list_head = NULL;
        list_tail = NULL;
      };
      inline const char *log_str() const
      {
        static const int32_t BUFFER_SIZE = 2048;
        static __thread char BUFFER[2][BUFFER_SIZE];
        static __thread int64_t i = 0;
        snprintf(BUFFER[i % 2], BUFFER_SIZE, "cell_info_cnt=%hd cell_info_size=%hdKB "
                "list_head=%p list_tail=%p",
                 cell_info_cnt, cell_info_size,
                 list_head, list_tail);
        return BUFFER[i++ % 2];
      };
      inline bool is_empty() const
      {
        return (NULL == list_head);
      };
    };

    enum TETransType
    {
      INVALID_TRANSACTION = 0,
      READ_TRANSACTION = 1,
      WRITE_TRANSACTION = 2,
    };

    extern bool get_key_prefix(const TEKey &te_key, TEKey &prefix_key);
  }
}

#endif //OCEANBASE_UPDATESERVER_TABLE_ENGINE_H_

