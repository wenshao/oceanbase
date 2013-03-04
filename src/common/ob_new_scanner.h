/**
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * Authors:
 *   yubai <yubai.lk@taobao.com>
 *     - some work details if you want
 *   yanran <yanran.hfs@taobao.com> 2010-10-27
 *     - new serialization format(ObObj composed, extented version)
 */

#ifndef OCEANBASE_COMMON_OB_NEW_SCANNER_H_
#define OCEANBASE_COMMON_OB_NEW_SCANNER_H_

#include "ob_define.h"
#include "ob_read_common_data.h"
#include "ob_row_iterator.h"
#include "ob_string_buf.h"
#include "ob_object.h"
#include "ob_row.h"
#include "ob_row_store.h"

namespace oceanbase
{
  namespace common
  {
    class ObNewScanner : public ObRowIterator
    {
      static const int64_t DEFAULT_MAX_SERIALIZE_SIZE = OB_MAX_PACKET_LENGTH - 1024;
      static const int64_t DEFAULT_MEMBLOCK_SIZE = 512 * 1024;


    public:
      enum ID_NAME_STATUS
      {
        UNKNOWN = 0,
        ID = 1,
        NAME = 2
      };
      public:
        ObNewScanner();
        virtual ~ObNewScanner();

        void reset();
        void clear();

        int64_t set_mem_size_limit(const int64_t limit);

        /// @brief add a row in the ObNewScanner
        /// @retval OB_SUCCESS successful
        /// @retval OB_SIZE_OVERFLOW memory limit is exceeded if add this row
        /// @retval otherwise error
        int add_row(const ObRow &row);

        int add_row(const ObString &rowkey, const ObRow &row);

        /// @retval OB_ITER_END iterate end
        /// @retval OB_SUCCESS go to the next cell succ
        int get_next_row(ObRow &row);

        int get_next_row(ObString &rowkey, ObRow &row);
            
        bool is_empty() const;
        /// after deserialization, get_last_row_key can retreive the last row key of this scanner
        /// @param [out] row_key the last row key
        int get_last_row_key(ObString &row_key) const;
        NEED_SERIALIZE_AND_DESERIALIZE;

      public:
        /// indicating the request was fullfiled, must be setted by cs and ups
        /// @param [in] is_fullfilled  缓冲区不足导致scanner中只包含部分结果的时候设置为false
        /// @param [in] fullfilled_item_num -# when getting, this means processed position in GetParam
        ///                                 -# when scanning, this means row number fullfilled
        int set_is_req_fullfilled(const bool &is_fullfilled, const int64_t fullfilled_item_num);
        int get_is_req_fullfilled(bool &is_fullfilled, int64_t &fullfilled_item_num) const;

        /// when response scan request, range must be setted
        /// the range is the seviced range of this tablet or (min, max) in updateserver
        /// @param [in] range
        int set_range(const ObRange &range);
        /// same as above but do not deep copy keys of %range, just set the reference.
        /// caller ensures keys of %range remains reachable until ObNewScanner serialized.
        int set_range_shallow_copy(const ObRange &range);
        int get_range(ObRange &range) const;

        inline void set_data_version(const int64_t version)
        {
          data_version_ = version;
        }

        inline int64_t get_data_version() const
        {
          return data_version_;
        }

        inline int64_t get_size() const
        {
          return cur_size_counter_;
        }

        // TODO: ?
        inline int64_t get_cell_num() const
        {
          return cell_num_;
        }

        inline int64_t get_row_num() const
        {
          return row_num_;
        }

        void dump(void) const;
        void dump_all(int log_level) const; 

        inline int64_t get_whole_result_row_num() const
        {
          return whole_result_row_num_;
        }

        inline void set_whole_result_row_num(int64_t new_whole_result_row_num)
        {
          whole_result_row_num_ = new_whole_result_row_num;
        }


        inline void set_id_name_type(const ID_NAME_STATUS type)
        {
          id_name_type_ = type;
        }

        inline ID_NAME_STATUS get_id_name_type() const
        {
          return (ID_NAME_STATUS)id_name_type_;
        }
      private:
        int deserialize_basic_(const char* buf, const int64_t data_len, int64_t& pos, ObObj &last_obj);

        int deserialize_table_(const char* buf, const int64_t data_len, int64_t& pos, ObObj &last_obj);

        static int deserialize_int_(const char* buf, const int64_t data_len, int64_t& pos,
            int64_t &value, ObObj &last_obj);

        static int deserialize_varchar_(const char* buf, const int64_t data_len, int64_t& pos,
            ObString &value, ObObj &last_obj);

        static int deserialize_int_or_varchar_(const char* buf, const int64_t data_len, int64_t& pos,
            int64_t& int_value, ObString &varchar_value, ObObj &last_obj);


      private:
        ObRowStore row_store_;
        int64_t cur_size_counter_;
        ObString cur_table_name_;
        uint64_t cur_table_id_;
        ObString cur_row_key_;
        ObString tmp_table_name_;
        uint64_t tmp_table_id_;
        ObString tmp_row_key_;
        bool has_row_key_flag_;
        int64_t mem_size_limit_;
        ObStringBuf string_buf_;
        ObRange range_;
        int64_t data_version_;
        bool has_range_;
        bool is_request_fullfilled_;
        int64_t  fullfilled_item_num_;
        int64_t id_name_type_;
        int64_t row_num_;
        int64_t cell_num_;
        int64_t whole_result_row_num_;

        ObString last_row_key_;
    };
  } /* common */
} /* oceanbase */

#endif /* end of include guard: OCEANBASE_COMMON_OB_NEW_SCANNER_H_ */
