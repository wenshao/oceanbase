
/*
 *   (C) 2007-2010 Taobao Inc.
 *   
 *         
 *   Version: 0.1
 *           
 *   Authors:
 *      qushan <qushan@taobao.com>
 *        - ob range class, 
 *               
 */
#ifndef OCEANBASE_COMMON_OB_RANGE_H_
#define OCEANBASE_COMMON_OB_RANGE_H_

#include <tbsys.h>
#include "ob_define.h"
#include "ob_string.h"
#include "ob_string_buf.h"

namespace oceanbase 
{ 
  namespace common
  {
    class ObBorderFlag
    {
      public:
        static const int8_t INCLUSIVE_START = 0x1;
        static const int8_t INCLUSIVE_END = 0x2;
        static const int8_t MIN_VALUE = 0x4;
        static const int8_t MAX_VALUE = 0x8;

      public:
        ObBorderFlag() : data_(0) {}
        virtual ~ObBorderFlag() {}

        inline void set_inclusive_start() { data_ |= INCLUSIVE_START; }

        inline void unset_inclusive_start() { data_ &= (~INCLUSIVE_START); }

        inline bool inclusive_start() const { return (data_ & INCLUSIVE_START) == INCLUSIVE_START; }

        inline void set_inclusive_end() { data_ |= INCLUSIVE_END; }

        inline void unset_inclusive_end() { data_ &= (~INCLUSIVE_END); }

        inline bool inclusive_end() const { return (data_ & INCLUSIVE_END) == INCLUSIVE_END; }

        inline void set_min_value() { data_ |= MIN_VALUE; }
        inline void unset_min_value() { data_ &= (~MIN_VALUE); }
        inline bool is_min_value() const { return (data_ & MIN_VALUE) == MIN_VALUE; }

        inline void set_max_value() { data_ |= MAX_VALUE; }
        inline void unset_max_value() { data_ &= (~MAX_VALUE); }
        inline bool is_max_value() const { return (data_ & MAX_VALUE) == MAX_VALUE; }

        inline void set_data(const int8_t data) { data_ = data; }
        inline int8_t get_data() const { return data_; }

        inline bool is_left_open_right_closed() const { return (!inclusive_start() && inclusive_end()) || is_max_value(); }
      private:
        int8_t data_;
    };

    struct ObVersion
    {
      ObVersion() : version_(0) {}
      ObVersion(int64_t version) : version_(version) {}
      union
      {
        int64_t version_;
        struct
        {
          int32_t major_           : 32;
          int16_t minor_           : 16;
          int16_t is_final_minor_  : 16;
        };
      };

      int64_t operator=(int64_t version)
      {
        version_ = version;
        return version_;
      }

      operator int64_t() const
      {
        return version_;
      }

      static int64_t get_version(int64_t major,int64_t minor,bool is_final_minor)
      {
        ObVersion v;
        v.major_          = static_cast<int32_t>(major);
        v.minor_          = static_cast<int16_t>(minor);
        v.is_final_minor_ = is_final_minor ? 1 : 0;
        return v.version_;
      }

      static int64_t get_major(int64_t version)
      {
        ObVersion v;
        v.version_ = version;
        return v.major_;
      }

      static int64_t get_minor(int64_t version)
      {
        ObVersion v;
        v.version_ = version;
        return v.minor_;
      }

      static bool is_final_minor(int64_t version)
      {
        ObVersion v;
        v.version_ = version;
        return v.is_final_minor_ != 0;
      }

      static int compare(int64_t l,int64_t r)
      {
        int ret = 0;
        ObVersion lv = l;
        ObVersion rv = r;

        //ignore is_final_minor
        if ((lv.major_ == rv.major_) && (lv.minor_ == rv.minor_))
        {
          ret = 0;
        }
        else if ((lv.major_ < rv.major_) ||
                 ((lv.major_ == rv.major_) && lv.minor_ < rv.minor_))
        {
          ret = -1;
        }
        else
        {
          ret = 1;
        }
        return ret;        
      }
    };

    struct ObVersionRange
    {
      ObBorderFlag border_flag_;
      ObVersion start_version_;
      ObVersion end_version_;

      // from MIN to MAX, complete set.
      inline bool is_whole_range() const 
      {
        return (border_flag_.is_min_value()) && (border_flag_.is_max_value());
      }

      int to_string(char* buf,int64_t buf_len) const
      {
        int ret        = OB_SUCCESS;
        const char* lb = NULL;
        const char* rb = NULL;
        if (buf != NULL && buf_len > 0)
        {
          if (border_flag_.is_min_value())
          {
            lb = "(MIN";
          }
          else if (border_flag_.inclusive_start())
          {
            lb = "[";
          }
          else
          {
            lb = "(";
          }
          
          if (border_flag_.is_max_value())
          {
            rb = "MAX)";
          }
          else if (border_flag_.inclusive_end())
          {
            rb = "]";
          }
          else
          {
            rb = ")";
          }

          int64_t len = 0;

          if (is_whole_range())
          {
            len = snprintf(buf,buf_len,"%s,%s",lb,rb);
          }
          else if (border_flag_.is_min_value())
          {
            len = snprintf(buf,buf_len,"%s,%d-%hd-%hd%s",lb,
                           end_version_.major_,end_version_.minor_,end_version_.is_final_minor_,rb);            
          }
          else if (border_flag_.is_max_value())
          {
            len = snprintf(buf,buf_len,"%s%d-%hd-%hd, %s",lb,
                           start_version_.major_,start_version_.minor_,start_version_.is_final_minor_,rb);
          }
          else 
          {
            len = snprintf(buf,buf_len,"%s %d-%hd-%hd,%d-%hd-%hd %s",lb,
                           start_version_.major_,start_version_.minor_,start_version_.is_final_minor_,
                           end_version_.major_,end_version_.minor_,end_version_.is_final_minor_,rb);
          }
          if (len < 0 || len > buf_len)
          {
            ret = OB_SIZE_OVERFLOW;
          }
        }
        return ret;
      }
    };

    struct ObRange 
    {
      enum SubDirection
      {
        RIGHT,
        LEFT
      };

      uint64_t table_id_;
      ObBorderFlag border_flag_;
      ObString start_key_;
      ObString end_key_;

      ObRange()
      {
        reset();
      }

      void reset()
      {
        table_id_ = OB_INVALID_ID;
        border_flag_.set_data(0);
        start_key_.assign(NULL, 0);
        end_key_.assign(NULL, 0);
      }
      
      // new compare func for tablet.range and scan_param.range
      int compare_with_endkey2(const ObRange & r) const
      {
        int cmp = 0;
        if (border_flag_.is_max_value())
        {
          if (!r.border_flag_.is_max_value())
          {
            cmp = 1;
          }
        }
        else if (r.border_flag_.is_max_value())
        {
          cmp = -1;
        }
        else
        {
          cmp = end_key_.compare(r.end_key_);
          if (0 == cmp)
          {
            if (border_flag_.inclusive_end() && !r.border_flag_.inclusive_end())
            {
              cmp = 1;
            }
            else if (!border_flag_.inclusive_end() && r.border_flag_.inclusive_end())
            {
              cmp = -1;
            }
          }
        }
        return cmp;
      }

      inline int compare_with_endkey(const ObRange & r) const 
      {
        int cmp = 0;
        if (table_id_ != r.table_id_) 
        {
          cmp = (table_id_ < r.table_id_) ? -1 : 1;
        } 
        else 
        {
          if (border_flag_.is_max_value())
          {
            // MAX_VALUE == MAX_VALUE;
            if (r.border_flag_.is_max_value())
            {
              cmp = 0;
            }
            else
            {
              cmp = 1;
            }
          }
          else
          {
            if (r.border_flag_.is_max_value())
            {
              cmp = -1;
            }
            else
            {
              cmp = end_key_.compare(r.end_key_);
            }
          }
        }
        return cmp;
      }
      
      inline int compare_with_startkey(const ObRange & r) const 
      {
        int cmp = 0;
        if (table_id_ != r.table_id_) 
        {
          cmp = (table_id_ < r.table_id_) ? -1 : 1;
        } 
        else 
        {
          if (border_flag_.is_min_value())
          {
            // MIN_VALUE == MIN_VALUE;
            if (r.border_flag_.is_min_value())
            {
              cmp = 0;
            }
            else
            {
              cmp = -1;
            }
          }
          else
          {
            if (r.border_flag_.is_min_value())
            {
              cmp = 1;
            }
            else
            {
              cmp = start_key_.compare(r.start_key_);
            }
          }
        }
        return cmp;
      }
      
      int compare_with_startkey2(const ObRange & r) const 
      {
        int cmp = 0;
        if (border_flag_.is_min_value())
        {
          if (!r.border_flag_.is_min_value())
          {
            cmp = -1;
          }
        }
        else if (r.border_flag_.is_min_value())
        {
          cmp = 1;
        }
        else
        {
          cmp = start_key_.compare(r.start_key_);
          if (0 == cmp)
          {
            if (border_flag_.inclusive_start() && !r.border_flag_.inclusive_start())
            {
              cmp = -1;
            }
            else if (!border_flag_.inclusive_start() && r.border_flag_.inclusive_start())
            {
              cmp = 1;
            }
          }
        }
        return cmp;
      }

      inline bool empty() const 
      {
        bool ret = false;
        if (border_flag_.is_min_value() || border_flag_.is_max_value())
        {
          ret = false;
        }
        else 
        {
          ret  = end_key_ < start_key_ 
            || ((end_key_ == start_key_) 
                && !((border_flag_.inclusive_end()) 
                  && border_flag_.inclusive_start()));
        }
        return ret;
      }

      // from MIN to MAX, complete set.
      inline bool is_whole_range() const 
      {
        return (border_flag_.is_min_value()) && (border_flag_.is_max_value());
      }

      bool equal(const ObRange& r) const;

      bool intersect(const ObRange& r) const;
      
      // cut out the range r from this range, this range will be changed
      // r should be included by this range
      // one of condition below should be satisfied
      // 1. the left end of r should equal to the left end of this range
      // 2. the right end of r should equal to the right end of this range
      // 
      // @param r ObRange 
      // @param string_buf string memory for changed this range rowkey
      //
      // if r equal to this range, than will return OB_EMPTY_RANGE, this range will NOT CHANGED
      int trim(const ObRange& r, ObStringBuf & string_buf);

      void dump() const;
      
      bool check(void) const;

      void hex_dump(const int32_t log_level = TBSYS_LOG_LEVEL_DEBUG) const;

      int to_string(char* buffer, const int32_t length) const;

      int64_t hash() const;

      bool operator == (const ObRange &other) const
      {
        return equal(other);
      };

      NEED_SERIALIZE_AND_DESERIALIZE;
    };

    template <typename Allocator>
      int deep_copy_range(Allocator& allocator, const ObRange &src, ObRange &dst)
      {
        int ret = OB_SUCCESS;

        ObString::obstr_size_t start_len = src.start_key_.length();
        ObString::obstr_size_t end_len = src.end_key_.length();

        char* copy_start_key_ptr = NULL;
        char* copy_end_key_ptr = NULL;

        dst.table_id_ = src.table_id_;
        dst.border_flag_ = src.border_flag_;

        if (!src.border_flag_.is_min_value() 
            && NULL == (copy_start_key_ptr = allocator.alloc(start_len)))
        {
          ret = OB_ALLOCATE_MEMORY_FAILED;
        }
        else if (!src.border_flag_.is_max_value() 
                 && NULL == (copy_end_key_ptr = allocator.alloc(end_len)))
        {
          ret = OB_ALLOCATE_MEMORY_FAILED;
        }
        else
        {
          if (!src.border_flag_.is_min_value())
          {
            memcpy(copy_start_key_ptr, src.start_key_.ptr(), start_len);
            dst.start_key_.assign_ptr(copy_start_key_ptr, start_len);
          }
          else
          {
            dst.start_key_.assign_ptr(NULL, 0);
          }
          if (!src.border_flag_.is_max_value())
          {
            memcpy(copy_end_key_ptr, src.end_key_.ptr(), end_len);
            dst.end_key_.assign_ptr(copy_end_key_ptr, end_len);
          }
          else
          {
            dst.end_key_.assign_ptr(NULL, 0);
          }
        }

        return ret;
      }


  } // end namespace common
} // end namespace oceanbase

#endif //OCEANBASE_COMMON_OB_RANGE_H_

