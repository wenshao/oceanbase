////===================================================================
 //
 // ob_memtable.h updateserver / Oceanbase
 //
 // Copyright (C) 2010 Taobao.com, Inc.
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

#ifndef  OCEANBASE_UPDATESERVER_MEMTABLE_H_
#define  OCEANBASE_UPDATESERVER_MEMTABLE_H_
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <pthread.h>
#include <new>
#include <bitset>
#include <algorithm>
#include "common/ob_atomic.h"
#include "common/ob_define.h"
#include "common/ob_string_buf.h"
#include "common/ob_iterator.h"
#include "common/page_arena.h"
#include "common/ob_mutator.h"
#include "common/ob_read_common_data.h"
#include "common/ob_scanner.h"
#include "common/ob_bloomfilter.h"
#include "common/ob_cell_meta.h"
#include "common/ob_column_filter.h"
#include "ob_table_engine.h"
#include "ob_cellinfo_processor.h"
#include "ob_ups_mutator.h"
#include "ob_trans_mgr.h"
#include "ob_query_engine.h"
#include "ob_ups_compact_cell_writer.h"

namespace oceanbase
{
  namespace updateserver
  {
    class MemTable;

    typedef QueryEngine TableEngine;
    typedef QueryEngineIterator TableEngineIterator;

    typedef TETransType MemTableTransType;
    typedef uint64_t MemTableTransDescriptor;

    template <int64_t BUF_SIZE>
    class FixedSizeBuffer
    {
      public:
        char *get_buffer() {return buffer_;};
        int64_t get_size() {return BUF_SIZE;};
      private:
        char buffer_[BUF_SIZE];
    };

    class TransNodeWrapper4Merge : public ITransNode
    {
      public:
        TransNodeWrapper4Merge(const TransNode &tn) : trans_node_(tn)
        {
        };
        virtual ~TransNodeWrapper4Merge()
        {
        };
      public:
        int64_t get_trans_id() const
        {
          return trans_node_.get_min_flying_trans_id();
        };
      private:
        const TransNode &trans_node_;
    };

    class MemTableScanIter;
    class MemTableRowIterator;
    class MemTableGetIter : public common::ObIterator
    {
      friend class MemTable;
      friend class MemTableScanIter;
      friend class MemTableRowIterator;
      friend class QueryEngine;
      public:
        MemTableGetIter();
        ~MemTableGetIter();
      public:
        int next_cell();
        int get_cell(common::ObCellInfo **cell_info);
        int get_cell(common::ObCellInfo **cell_info, bool *is_row_changed);
      public:
        void reset();
      private:
        void set_(const TEKey &te_key,
                  const TEValue *te_value,
                  const common::ColumnFilter *column_filter,
                  const ITransNode *trans_node);
        const ObCellInfoNode *get_cur_node_iter_() const;
        static bool trans_end_(const common::ObObj &value, const ITransNode *trans_node);
      private:
        TEKey te_key_;
        const TEValue *te_value_;
        const common::ColumnFilter *column_filter_;

        const ITransNode *trans_node_;
        bool is_iter_end_;
        const ObCellInfoNode *node_iter_;
        ObCellInfoNodeIterableWithCTime cell_iter_;
        int64_t iter_counter_;
        common::ObCellInfo ci_;
    };

    class MemTableScanIter : public common::ObIterator
    {
      friend class MemTable;
      public:
        MemTableScanIter();
        ~MemTableScanIter();
      public:
        int next_cell();
        int get_cell(common::ObCellInfo **cell_info);
        int get_cell(common::ObCellInfo **cell_info, bool *is_row_changed);
      public:
        void reset();
      private:
        void set_(const uint64_t table_id,
                  common::ColumnFilter *column_filter,
                  const TransNode *trans_node);
        TableEngineIterator &get_te_iter_();
        static bool is_row_not_exist_(MemTableGetIter &get_iter);
      private:
        TableEngineIterator te_iter_;
        uint64_t table_id_;
        common::ColumnFilter *column_filter_;
        const TransNode *trans_node_;
        bool is_iter_end_;
        MemTableGetIter get_iter_;
    };

    class MemTableIterator : public common::ObIterator
    {
      friend class MemTable;
      public:
        MemTableIterator();
        ~MemTableIterator();
      public:
        int next_cell();
        int get_cell(common::ObCellInfo **cell_info);
        int get_cell(common::ObCellInfo **cell_info, bool *is_row_changed);
      public:
        void reset();
      private:
        MemTableScanIter &get_scan_iter_();
        MemTableGetIter &get_get_iter_();
      private:
        MemTableScanIter scan_iter_;
        MemTableGetIter get_iter_;
        common::ObIterator *iter_;
    };

    struct MemTableAttr
    {
      int64_t total_memlimit;
      //int64_t drop_page_num_once;
      //int64_t drop_sleep_interval_us;
      IExternMemTotal *extern_mem_total;
      MemTableAttr() : total_memlimit(0),
                       //drop_page_num_once(0),
                       //drop_sleep_interval_us(0),
                       extern_mem_total(NULL)
      {
      };
    };

    class ObUpsTableMgr;
    class MemTable : public ITableEngine
    {
      friend class ObCellInfoNode;
      struct RollbackInfo
      {
        TEKey key;
        TEValue *dest;
        TEValue src;
      };
      struct CommitInfo
      {
        int64_t row_counter;
      };
      static const char *MIN_STR;
      static const int64_t MAX_ROW_CELLINFO = 128;
      static const int64_t MAX_ROW_SIZE = (common::OB_MAX_PACKET_LENGTH - 512L * 1024L) / CELL_INFO_SIZE_UNIT;
      static const int64_t BLOOM_FILTER_NHASH = 1;
      static const int64_t BLOOM_FILTER_NBYTE = common::OB_MAX_PACKET_LENGTH - 1 * 1024;
      static const int64_t MAX_TRANS_NUM = 64;
      public:
        MemTable();
        ~MemTable();
      public:
        int init();
        int destroy();
      public:
        int rollback(void *data);
        int commit(void *data);
      public:
        // 插入key-value对，如果已存在则覆盖
        // @param [in] key 待插入的key
        // @param [in] value 待插入的value
        int set(const MemTableTransDescriptor td, ObUpsMutator &mutator, const bool check_checksum = false,
                ObUpsTableMgr *ups_table_mgr = NULL, common::ObScanner *scanner = NULL);
    
        // 获取指定key的value
        // @param [in] key 要查询的key
        // @param [out] value 查询返回的value
        int get(const MemTableTransDescriptor td,
                const uint64_t table_id, const common::ObString &row_key,
                MemTableIterator &iterator,
                common::ColumnFilter *column_filter = NULL);
        // 范围查询，返回一个iterator
        // @param [in] 查询范围的start key
        // @param [in] 查询范围是否包含start key本身, 0为包含, 非0为不包含; 在hash实现时必须为0
        // @param [in] 查询范围的end key
        // @param [in] 查询范围是否包含end key本身, 0为包含, 非0为不包含; 在hash实现时必须为0
        // @param [out] iter 查询结果的迭代器
        int scan(const MemTableTransDescriptor td,
                const common::ObRange &range,
                const bool reverse,
                MemTableIterator &iter,
                common::ColumnFilter *column_filter = NULL);

        int start_transaction(const TETransType trans_type, MemTableTransDescriptor &td, const int64_t trans_id = -1);

        int end_transaction(const MemTableTransDescriptor td, bool rollback = false);
        
        // 开始一次事务性的更新
        // 一次mutation结束之前不能开启新的mutation
        int start_mutation(const MemTableTransDescriptor td);

        // 结束一次mutation
        // @param[in] rollback 是否回滚
        int end_mutation(const MemTableTransDescriptor td, bool rollback);
        
        inline int64_t get_version() const
        {
          return version_;
        };
        inline void set_version(int64_t new_version)
        {
          common::atomic_exchange((uint64_t*)&version_, (uint64_t)new_version);
        };

        inline int64_t get_ref_cnt() const
        {
          return ref_cnt_;
        };
        inline int64_t inc_ref_cnt()
        {
          return common::atomic_inc((uint64_t*)&ref_cnt_);
        };
        inline int64_t dec_ref_cnt()
        {
          return common::atomic_dec((uint64_t*)&ref_cnt_);
        };

        int clear();

        inline int64_t total() const
        {
          return mem_tank_.total();
        };

        inline int64_t used() const
        {
          return mem_tank_.used();
        };

        inline void set_attr(const MemTableAttr &attr)
        {
          mem_tank_.set_total_limit(attr.total_memlimit);
          mem_tank_.set_extern_mem_total(attr.extern_mem_total);
        };

        inline void get_attr(MemTableAttr &attr)
        {
          attr.total_memlimit = mem_tank_.get_total_limit();
          attr.extern_mem_total = mem_tank_.get_extern_mem_total();
        };

        inline void log_memory_info() const
        {
          mem_tank_.log_info();
        };

        void dump2text(const common::ObString &dump_dir)
        {
          const int64_t BUFFER_SIZE = 1024;
          char buffer[BUFFER_SIZE];
          snprintf(buffer, BUFFER_SIZE, "%.*s/ups_memtable.ref_%ld.ver_%ld.pid_%d.tim_%ld",
                  dump_dir.length(), dump_dir.ptr(),
                  ref_cnt_, version_, getpid(), tbsys::CTimeUtil::getTime());
          table_engine_.dump2text(buffer);
        };

        inline int64_t size() const
        {
          return row_counter_;
        };

        inline int64_t btree_size()
        {
          return table_engine_.btree_size();
        };

        inline int64_t hash_size() const
        {
          return table_engine_.hash_size();
        };

        inline int64_t hash_bucket_using() const
        {
          return table_engine_.hash_bucket_using();
        };

        inline int64_t hash_uninit_unit_num() const
        {
          return table_engine_.hash_uninit_unit_num();
        };

        int get_bloomfilter(common::TableBloomFilter &table_bf) const;

        int scan_all(TableEngineIterator &iter);

      private:
        inline int copy_cells_(TransNode &tn,
                              TEValue &value,
                              ObUpsCompactCellWriter &ccw);
        inline int build_mtime_cell_(const int64_t mtime,
                                    const uint64_t table_id,
                                    ObUpsCompactCellWriter &ccw);
        inline int ob_sem_handler_(TransNode &tn,
                                  ObCellInfo &cell_info,
                                  TEKey &cur_key,
                                  TEValue *&cur_value,
                                  const TEKey &prev_key,
                                  TEValue *prev_value,
                                  bool is_row_changed,
                                  bool is_row_finished,
                                  ObUpsCompactCellWriter &ccw,
                                  int64_t &total_row_counter,
                                  int64_t &new_row_counter,
                                  common::ObBatchChecksum &bc);
        inline int get_cur_value_(TransNode &tn,
                                  TEKey &cur_key,
                                  const TEKey &prev_key,
                                  TEValue *prev_value,
                                  TEValue *&cur_value,
                                  bool is_row_changed,
                                  int64_t &total_row_counter,
                                  int64_t &new_row_counter,
                                  common::ObBatchChecksum &bc);
        inline int update_value_(const TransNode &tn,
                                const uint64_t table_id,
                                ObCellInfo &cell_info,
                                TEValue &value,
                                ObUpsCompactCellWriter &ccw,
                                common::ObBatchChecksum &bc);
        inline int merge_(const TransNode &tn,
                          const TEKey &te_key,
                          TEValue &te_value);

        inline static int16_t get_varchar_length_kb_(const common::ObObj &value)
        {
          int16_t ret = 0;
          if (ObVarcharType == value.get_type())
          {
            ObString vc;
            value.get_varchar(vc);
            int64_t length = vc.length();
            length = (length + CELL_INFO_SIZE_UNIT - 1) & ~(CELL_INFO_SIZE_UNIT- 1);
            length = length / CELL_INFO_SIZE_UNIT;
            ret = (int16_t)length;
          }
          return ret;
        };
        inline static bool is_row_not_exist_(const common::ObObj &value)
        {
          return (value.get_ext() == common::ObActionFlag::OP_ROW_DOES_NOT_EXIST);
        };
        inline static bool is_delete_row_(const common::ObObj &value)
        {
          return (value.get_ext() == common::ObActionFlag::OP_DEL_ROW);
        };
        inline static bool is_insert_(const int64_t op_type)
        {
          return (op_type == common::ObActionFlag::OP_INSERT);
        };
        inline static bool is_update_(const int64_t op_type)
        {
          return (op_type == common::ObActionFlag::OP_UPDATE);
        };

        inline static const common::ObString &get_start_key(const common::ObRange &range)
        {
          return range.start_key_;
        };
        inline static const common::ObString &get_end_key(const common::ObRange &range)
        {
          return range.end_key_;
        };
        inline static uint64_t get_table_id(const common::ObRange &range)
        {
          return range.table_id_;
        };
        inline static int get_start_exclude(const common::ObRange &range)
        {
          return range.border_flag_.inclusive_start() ? 0 : 1;
        };
        inline static int get_end_exclude(const common::ObRange &range)
        {
          return range.border_flag_.inclusive_end() ? 0 : 1;
        };
        inline static int get_min_key(const common::ObRange &range)
        {
          return range.border_flag_.is_min_value() ? 1 : 0;
        };
        inline static int get_max_key(const common::ObRange &range)
        {
          return range.border_flag_.is_max_value() ? 1 : 0;
        };

        void handle_checksum_error(ObUpsMutator &mutator);
      private:
        bool inited_;
        MemTank mem_tank_;
        TableEngine table_engine_;
        common::TableBloomFilter table_bf_;

        int64_t version_;
        int64_t ref_cnt_;

        int64_t checksum_before_mutate_;
        int64_t checksum_after_mutate_;

        TransMgr trans_mgr_;
        int64_t row_counter_;
    };
  }
}

#endif //OCEANBASE_UPDATESERVER_MEMTABLE_H_

