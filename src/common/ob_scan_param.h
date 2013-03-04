#ifndef OCEANBASE_COMMON_SCAN_PARAM_H_
#define OCEANBASE_COMMON_SCAN_PARAM_H_

#include "ob_define.h"
#include "ob_array_helper.h"
#include "ob_object.h"
#include "ob_range.h"
#include "ob_simple_filter.h"
#include "ob_common_param.h"
#include "ob_groupby.h" 
#include "ob_composite_column.h"

namespace oceanbase
{
  namespace common
  {
    extern const char * SELECT_CLAUSE_WHERE_COND_AS_CNAME_PREFIX;
    class ObScanParam : public ObReadParam
    {
    public:
      enum Order
      {
        ASC = 1,
        DESC
      };

      enum Direction
      {
        FORWARD = 0,
        BACKWARD = 1,
      };

      enum ReadMode
      {
        SYNCREAD = 0,
        PREREAD = 1,
      };

      struct ScanFlag
      {
        ScanFlag() 
        : scan_direction_(FORWARD), read_mode_(PREREAD), reserved_(0)
        {
        }
        ScanFlag(const Direction dir, const ReadMode mode) 
        {
          flag_ = 0;
          flag_ |= (dir & 0xF);
          flag_ |= ((mode & 0xF) << 4);
        }
        union
        {
          int64_t flag_;
          struct
          {
            int64_t scan_direction_ : 4;
            int64_t read_mode_ : 4;
            int64_t reserved_ : 56;
          };
        };
      };

      ObScanParam();
      virtual ~ObScanParam();

      int set(const uint64_t& table_id, const ObString& table_name, const ObRange& range, bool deep_copy_args = false);

      int set_range(const ObRange& range);

      int add_column(const ObString& column_name, bool is_return = true);
      int add_column(const uint64_t& column_id, bool is_return = true);
      int add_column(const ObString & expr, const ObString & as_name, bool is_return = true);
      int add_column(const ObObj *expr, bool is_return = true);
      void clear_column(void);

      int add_where_cond(const ObString & expr, bool is_expire_cond = false);

      inline void set_scan_size(const int64_t scan_size)
      {
        scan_size_ = scan_size;
      }
      inline void set_scan_direction(const Direction scan_dir)
      {
        scan_flag_.flag_ = (scan_flag_.flag_ & (~0xF)) | (scan_dir & 0xF);
      }
      inline void set_read_mode(const ReadMode mode)
      {
        scan_flag_.flag_ = (scan_flag_.flag_ & (~0xF0)) | ((mode & 0xF) << 4);
      }
      inline void set_scan_flag(const ScanFlag flag)
      {
        scan_flag_ = flag;
      }

      inline uint64_t get_table_id() const
      {
        return table_id_;
      }
      inline const ObString get_table_name() const
      {
        return table_name_;
      }
      inline const ObRange* const get_range() const
      {
        return &range_;
      }
      inline ScanFlag get_scan_flag()const
      {
        return scan_flag_;
      }
      inline int64_t get_scan_size() const
      {
        return scan_size_;
      }
      inline Direction get_scan_direction() const
      {
        return(Direction)scan_flag_.scan_direction_;
      }
      inline ReadMode get_read_mode() const
      {
        return(ReadMode)scan_flag_.read_mode_;
      }
      inline int64_t get_column_name_size() const
      {
        return basic_column_list_.get_array_index();
      }
      inline int64_t get_column_id_size() const
      {
        return basic_column_id_list_.get_array_index();
      }
      inline const ObString* const get_column_name() const
      {
        return basic_column_names_;
      }
      inline const uint64_t* const get_column_id() const
      {
        return basic_column_ids_;
      }

      /*
      inline const ObCompositeColumn *const get_composite_columns() const
      {
        return select_comp_columns_;
      }
      */ 
      inline int64_t get_composite_columns_size()const
      {
        return select_comp_column_list_.get_array_index();
      } 
      const ObArrayHelper<ObCompositeColumn> & get_composite_columns() const
      {
        return select_comp_column_list_;
      }

      bool *is_return(const int64_t c_idx)const;


      inline int64_t get_return_info_size() const
      {
        return basic_return_info_list_.get_array_index() + comp_return_info_list_.get_array_index();
      } 

      const ObArrayHelpers<bool> & get_return_infos()const
      {
        return select_return_info_list_;
      }

      const ObGroupByParam &get_group_by_param()const;
      ObGroupByParam &get_group_by_param();

      /// set and get condition filter
      int add_where_cond(const ObString & column_name, const ObLogicOperator & cond_op, const ObObj & cond_value);
      const ObSimpleFilter & get_filter_info(void)const; 
      ObSimpleFilter & get_filter_info(void); 

      /// set and get order by information
      int add_orderby_column(const ObString & column_name, Order order = ASC);
      int add_orderby_column(const int64_t column_idx, Order order = ASC);
      int64_t get_orderby_column_size()const;
      void get_orderby_column(ObString const* & names, uint8_t  const* & orders, 
        int64_t &column_size)const;
      void get_orderby_column(int64_t const* & column_idxs, uint8_t const * & orders, 
        int64_t &column_size)const;
      inline void clear_orderby_info(void)
      {
        orderby_column_id_list_.clear();
        orderby_column_name_list_.clear();
        orderby_order_list_.clear();
      }
      void set_all_column_return();

      /// set and get limit information
      int set_limit_info(const int64_t offset, const int64_t count);
      void get_limit_info(int64_t &offset, int64_t &count) const;

      /// set and get topk precision info
      int set_topk_precision(const int64_t sharding_minimum_row_count, const double precision);
      void get_topk_precision(int64_t &sharding_minimum_row_count, double &precision) const;

      void reset(void);

      /// safe copy the array data and init the pointer to itself data
      /// warning: only include some basic info and column info 
      /// not ensure the advanced info copy safely
      int safe_copy(const ObScanParam & other);

      int  get_select_column_name(const int64_t column_idx, ObString & column_name)const;

      int64_t get_returned_column_num();

      /// get readable scan param info
      int to_str(char *buf, int64_t buf_size, int64_t &pos)const; 

      NEED_SERIALIZE_AND_DESERIALIZE;

      // dump scan param info, basic version
      void dump(void) const;
      void dump_basic_param(void) const;
      void dump_column_param(void) const;
      void dump_composite_column_param(void) const;
      void dump_filter_param(void) const;
      void dump_return_info_param(void) const;
      void dump_groupby_param(void) const;
      void dump_sort_param(void) const;
      void dump_limit_param(void) const;
      void dump_topk_param(void) const;
        
    private:
      // BASIC_PARAM_FIELD
      int serialize_basic_param(char * buf, const int64_t buf_len, int64_t & pos) const;
      int deserialize_basic_param(const char * buf, const int64_t data_len, int64_t & pos);
      int64_t get_basic_param_serialize_size(void) const;

      // COLUMN_PARAM_FIELD
      int serialize_column_param(char * buf, const int64_t buf_len, int64_t & pos) const;
      int deserialize_column_param(const char * buf, const int64_t data_len, int64_t & pos);
      int64_t get_column_param_serialize_size(void) const;

      // SELECT_CLAUSE_COMP_COLUMN_FIELD
      int serialize_composite_column_param(char * buf, const int64_t buf_len, int64_t & pos) const;
      int deserialize_composite_column_param(const char * buf, const int64_t data_len, int64_t & pos);
      int64_t get_composite_column_param_serialize_size(void) const; 

      // SELECT_CLAUSE_WHERE_FIELD
      int serialize_filter_param(char * buf, const int64_t buf_len, int64_t & pos) const;
      int deserialize_filter_param(const char * buf, const int64_t data_len, int64_t & pos);
      int64_t get_filter_param_serialize_size(void) const;

      // SELECT_CLAUSE_RETURN_INFO_FIELD
      int serialize_return_info_param(char * buf, const int64_t buf_len, int64_t & pos) const;
      int deserialize_return_info_param(const char * buf, const int64_t data_len, int64_t & pos);
      int64_t get_return_info_serialize_size(void) const;

      // GROUPBY_PARAM_FILED
      int serialize_groupby_param(char * buf, const int64_t buf_len, int64_t & pos) const;
      int deserialize_groupby_param(const char * buf, const int64_t data_len, int64_t & pos);
      int64_t get_groupby_param_serialize_size(void) const;

      // SORT_PARAM_FIELD
      int serialize_sort_param(char * buf, const int64_t buf_len, int64_t & pos) const;
      int deserialize_sort_param(const char * buf, const int64_t data_len, int64_t & pos);
      int64_t get_sort_param_serialize_size(void) const;

      // LIMIT_PARAM_FIELD
      int serialize_limit_param(char * buf, const int64_t buf_len, int64_t & pos) const;
      int deserialize_limit_param(const char * buf, const int64_t data_len, int64_t & pos);
      int64_t get_limit_param_serialize_size(void) const;  

      // topk info
      int serialize_topk_param(char * buf, const int64_t buf_len, int64_t & pos) const;
      int deserialize_topk_param(const char * buf, const int64_t data_len, int64_t & pos);
      int64_t get_topk_param_serialize_size(void) const;  





      // END_PARAM_FIELD
      int serialize_end_param(char * buf, const int64_t buf_len, int64_t & pos) const;
      int deserialize_end_param(const char * buf, const int64_t data_len, int64_t & pos);
      int64_t get_end_param_serialize_size(void) const;
      


      int malloc_composite_columns();
    private:
      ObStringBuf buffer_pool_;
      bool        deep_copy_args_;

      uint64_t table_id_;
      ObString table_name_;
      ObRange range_;
      int64_t scan_size_;
      ScanFlag scan_flag_;
      ObString basic_column_names_[OB_MAX_COLUMN_NUMBER];
      ObArrayHelper<ObString> basic_column_list_;
      /// 接口中所有add_...idx的时候，其中的idx是引用basic_column_ids_或者groupby_param_中id数组的下标
      uint64_t basic_column_ids_[OB_MAX_COLUMN_NUMBER];
      ObArrayHelper<uint64_t> basic_column_id_list_;
      bool basic_return_infos_[OB_MAX_COLUMN_NUMBER];
      ObArrayHelper<bool> basic_return_info_list_;

      /// composite columns
      oceanbase::common::ObCompositeColumn *select_comp_columns_;
      ObArrayHelper<oceanbase::common::ObCompositeColumn> select_comp_column_list_;
      bool comp_return_infos_[OB_MAX_COLUMN_NUMBER];
      ObArrayHelper<bool> comp_return_info_list_;

      ObArrayHelpers<bool> select_return_info_list_;

      int64_t limit_offset_;
      /// @property 0 means not limit
      int64_t limit_count_;
      int64_t sharding_minimum_row_count_;
      double  topk_precision_;

      /// advanced property
      // orderby and filter and groupby
      ObString orderby_column_names_[OB_MAX_COLUMN_NUMBER];
      ObArrayHelper<ObString> orderby_column_name_list_;
      int64_t orderby_column_ids_[OB_MAX_COLUMN_NUMBER];
      ObArrayHelper<int64_t> orderby_column_id_list_;
      uint8_t orderby_orders_[OB_MAX_COLUMN_NUMBER];
      ObArrayHelper<uint8_t> orderby_order_list_;
      ObSimpleFilter condition_filter_;
      ObGroupByParam group_by_param_;
    };

  } /* common */
} /* oceanbase */

#endif /* end of include guard: OCEANBASE_COMMON_SCAN_PARAM_H_ */

