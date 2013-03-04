/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: ob_mutator.h,v 0.1 2010/09/15 11:10:37 chuanhui Exp $
 *
 * Authors:
 *   chuanhui <rizhao.ych@taobao.com>
 *     - some work details if you want
 *
 */

#ifndef __OCEANBASE_CHUNKSERVER_OB_MUTATOR_H__
#define __OCEANBASE_CHUNKSERVER_OB_MUTATOR_H__

#include "ob_read_common_data.h"
#include "ob_string_buf.h"
#include "ob_action_flag.h"
#include "page_arena.h"
#include "ob_update_condition.h"
#include "ob_prefetch_data.h"

namespace oceanbase
{
  namespace tests
  {
    namespace common
    {
      class TestMutatorHelper;
    }
  }
}

namespace oceanbase
{
  namespace common
  {
    struct ObMutatorCellInfo
    {
      // all ext type
      ObObj op_type;
      ObCellInfo cell_info;

      bool is_return(void) const
      {
        return (op_type.get_ext() & ObActionFlag::OP_RETURN_UPDATE_RESULT);
      }
    };

    // ObMutator represents a list of cell mutation.
    class ObMutator
    {
      friend class oceanbase::tests::common::TestMutatorHelper;
      public:
        enum
        {
          RETURN_NO_RESULT = 0,
          RETURN_UPDATE_RESULT = 1,
        };

        enum MUTATOR_TYPE
        {
          NORMAL_UPDATE = 10,    // normal update type
          CORRECTION_UPDATE = 11,// correction data type
        };

        enum RowChangedStat
        {
          CHANGED_UNKNOW = 0,
          NOCHANGED = 1,
          CHANGED = 2,
        };

        enum RowFinishedStat
        {
          FINISHED_UNKNOW = 0,
          NOFINISHED = 1,
          FINISHED = 2,
        };

      public:
        ObMutator();
        virtual ~ObMutator();
        int reset();
      public:
        // Uses ob&db semantic, ob semantic is used by default.
        int use_ob_sem();
        int use_db_sem();

        // check mutator type:normal or data correction
        void set_mutator_type(const MUTATOR_TYPE type);
        ObMutator::MUTATOR_TYPE get_mutator_type(void) const;

        // Adds update mutation to list
        int update(const ObString& table_name, const ObString& row_key,
            const ObString& column_name, const ObObj& value, const int return_flag = RETURN_NO_RESULT);
        int update(const uint64_t table_id, const ObString& row_key,
            const uint64_t column_id, const ObObj& value, const int return_flag = RETURN_NO_RESULT);
        // Adds insert mutation to list
        int insert(const ObString& table_name, const ObString& row_key,
            const ObString& column_name, const ObObj& value, const int return_flag = RETURN_NO_RESULT);
        int insert(const uint64_t table_id, const ObString& row_key,
            const uint64_t column_id, const ObObj& value, const int return_flag = RETURN_NO_RESULT);
        // Adds add mutation to list
        int add(const ObString& table_name, const ObString& row_key,
            const ObString& column_name, const int64_t value, const int return_flag = RETURN_NO_RESULT);
        int add_datetime(const ObString& table_name, const ObString& row_key,
            const ObString& column_name, const ObDateTime& value, const int return_flag = RETURN_NO_RESULT);
        int add_precise_datetime(const ObString& table_name, const ObString& row_key,
            const ObString& column_name, const ObPreciseDateTime& value, const int return_flag = RETURN_NO_RESULT);
        // Adds del_row mutation to list
        int del_row(const ObString& table_name, const ObString& row_key);

        int add_cell(const ObMutatorCellInfo& cell);

      public:
        // get update condition
        const ObUpdateCondition& get_update_condition(void) const;
        ObUpdateCondition& get_update_condition(void);

        /// set and get prefetch data
        const ObPrefetchData & get_prefetch_data(void) const;
        ObPrefetchData & get_prefetch_data(void);

      public:
        virtual void reset_iter();
        virtual int next_cell();
        virtual int get_cell(ObMutatorCellInfo** cell);
        virtual int get_cell(ObMutatorCellInfo** cell, bool* is_row_changed, bool* is_row_finished);

      public:
        int serialize(char* buf, const int64_t buf_len, int64_t& pos) const;
        int deserialize(const char* buf, const int64_t buf_len, int64_t& pos);
        int64_t get_serialize_size(void) const;

      private:
        // UPDATE_CONDITION_PARAM_FIELD
        int serialize_condition_param(char * buf, const int64_t buf_len, int64_t & pos) const;
        int deserialize_condition_param(const char * buf, const int64_t data_len, int64_t & pos);
        int64_t get_condition_param_serialize_size(void) const;

        // PREFETCH_PARAM_FIELD
        int serialize_prefetch_param(char * buf, const int64_t buf_len, int64_t & pos) const;
        int deserialize_prefetch_param(const char * buf, const int64_t data_len, int64_t & pos);
        int64_t get_prefetch_param_serialize_size(void) const;

        int add_cell(const ObMutatorCellInfo& cell, const RowChangedStat row_changed_stat);


      private:
        struct CellInfoNode
        {
          ObMutatorCellInfo cell;
          RowChangedStat row_changed_stat;
          RowFinishedStat row_finished_stat;
          CellInfoNode* next;
        };

        enum IdNameType
        {
          UNSURE = 0,
          USE_ID = 1,
          USE_NAME = 2,
        };

      private:
        int copy_cell_(const ObMutatorCellInfo& src_cell, ObMutatorCellInfo& dst_cell,
                      RowChangedStat row_changed_stat, int64_t& store_size);
        int add_node_(CellInfoNode* cur_node);

      private:
        int serialize_flag_(char* buf, const int64_t buf_len, int64_t& pos, const int64_t flag) const;
        int64_t get_obj_serialize_size_(const int64_t value, bool is_ext) const;
        int64_t get_obj_serialize_size_(const ObString& str) const;

      private:
        CellInfoNode* list_head_;
        CellInfoNode* list_tail_;
        PageArena<CellInfoNode> page_arena_;
        ObStringBuf str_buf_;
        ObString last_row_key_;
        ObString last_table_name_;
        uint64_t last_table_id_;
        IdNameType id_name_type_;
        int64_t  cell_store_size_;
        CellInfoNode* cur_iter_node_;
        /// update condition
        ObUpdateCondition condition_;
        /// prefetch data for temp result
        ObPrefetchData prefetch_data_;
        /// mutator type
        MUTATOR_TYPE type_;
        bool has_begin_;
    };
  }
}

#endif //__OB_MUTATOR_H__

