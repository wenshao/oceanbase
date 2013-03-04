#ifndef OCEANBASE_COMMON_OB_OBJECT_H_
#define OCEANBASE_COMMON_OB_OBJECT_H_

#include "ob_define.h"
#include "ob_string.h"
#include "tbsys.h"
#include "ob_action_flag.h"
#include "ob_number.h"
namespace oceanbase
{
  namespace tests
  {
    namespace common
    {
      class ObjTest;
    }
  }
  namespace common
  {
    enum ObObjType
    {
      ObMinType = -1,
      ObNullType,   // 空类型
      ObIntType,
      ObFloatType,              // @deprecated
      ObDoubleType,             // @deprecated
      ObDateTimeType,           // @deprecated
      ObPreciseDateTimeType,
      ObVarcharType,
      ObSeqType,
      ObCreateTimeType,
      ObModifyTimeType,
      ObExtendType,
      ObBoolType,
      ObDecimalType,            // aka numeric
      ObMaxType,
    };

    union _ObjValue
    {
      int64_t int_val;
      ObDateTime second_val;
      ObPreciseDateTime microsecond_val;
      ObCreateTime      create_time_val;
      ObModifyTime      modify_time_val;
      bool bool_val;
      float float_val;
      double double_val;
    };

    class ObBatchChecksum;
    class ObExprObj;
    class ObObj
    {
      public:
        ObObj();

        /// set add flag
        int set_add(const bool is_add = false);
        bool get_add()const;
        /*
         *   设置类型
         */
        void set_type(const ObObjType& type);
        /*
         *   设置列值
         */
        void set_int(const int64_t value,const bool is_add = false);
        void set_float(const float value,const bool is_add = false);
        void set_double(const double value,const bool is_add = false);
        void set_ext(const int64_t value);
        /*
         *   设置日期类型，如果date_time为OB_SYS_DATE，表示设置为服务器当前时间
         */
        void set_datetime(const ObDateTime& value,const bool is_add = false);
        void set_precise_datetime(const ObPreciseDateTime& value,const bool is_add = false);
        void set_varchar(const ObString& value);
        void set_seq();
        void set_modifytime(const ObModifyTime& value);
        void set_createtime(const ObCreateTime& value);
        void set_bool(const bool value);

        /*
         *   设置列值为空
         */
        void set_null();
        bool is_null() const;

        /// @fn apply mutation to this obj
        int apply(const ObObj &mutation);


        void reset();
        void reset_op_flag();
        bool is_valid_type() const;

        void dump(const int32_t log_level = TBSYS_LOG_LEVEL_DEBUG) const;
        void print_value(FILE* fd);
        //
        NEED_SERIALIZE_AND_DESERIALIZE;
        /*
         *   获取列值，用户根据已知的数据类型调用相应函数，如果类型不符则返回失败
         */
        int get_int(int64_t& value,bool& is_add) const;
        int get_int(int64_t& value) const;

        int get_float(float& value,bool& is_add) const;
        int get_float(float& value) const;

        int get_double(double& value,bool& is_add) const;
        int get_double(double& value) const;

        int get_ext(int64_t& value) const;
        int64_t get_ext() const;

        int get_datetime(ObDateTime& value,bool& is_add) const;
        int get_datetime(ObDateTime& value) const;

        int get_precise_datetime(ObPreciseDateTime& value,bool& is_add ) const;
        int get_precise_datetime(ObPreciseDateTime& value) const;

        int get_modifytime(ObModifyTime& value) const;
        int get_createtime(ObCreateTime& value) const;
        /*
         *   获取varchar类型数据，直接使用ObObj内部缓冲区
         */
        int get_varchar(ObString& value) const;
        int get_bool(bool &value) const;

        int set_decimal(const ObNumber &num, int8_t precision = 38, int8_t scale = 0, bool is_add = false);
        int get_decimal(ObNumber &num) const;
        int get_decimal(ObNumber &num, bool &is_add) const;
        /*
         *   获取数据类型
         */
        ObObjType get_type(void) const;
        bool need_deep_copy()const;
        bool is_true() const;
        /*
         *   计算obj内数据的校验和
         */
        int64_t checksum(const int64_t current) const;
        void checksum(ObBatchChecksum &bc) const;

        uint32_t murmurhash2(const uint32_t hash) const;
        int64_t hash() const;   // for ob_hashtable.h

        bool operator<(const ObObj &that_obj) const;
        bool operator>(const ObObj &that_obj) const;
        bool operator<=(const ObObj &that_obj) const;
        bool operator>=(const ObObj &that_obj) const;
        bool operator==(const ObObj &that_obj) const;
        bool operator!=(const ObObj &that_obj) const;

      private:
        friend class tests::common::ObjTest;
        friend class ObCompactCellWriter;
        friend class ObCompactCellIterator;
        friend class ObExprObj;
        bool is_datetime() const;
        bool can_compare(const ObObj & other) const;
      public:
        int get_timestamp(int64_t & timestamp) const;
      private:
        int  compare_same_type(const ObObj &other) const;
        int compare(const ObObj &other) const;
        void set_flag(bool is_add);
      private:
        static const uint8_t INVALID_OP_FLAG = 0x0;
        static const uint8_t ADD = 0x1;
        static const uint8_t META_VSCALE_MASK = 0x3F;
        static const uint8_t META_OP_FLAG_MASK = 0x3;
        static const uint8_t META_PREC_MASK = 0x7F;
        static const uint8_t META_SCALE_MASK = 0x3F;
        static const uint8_t META_NWORDS_MASK = 0x7;
        struct ObObjMeta
        {
          uint32_t type_:8;
          uint32_t dec_vscale_:6;
          uint32_t op_flag_:2;
          uint32_t dec_precision_:7;
          uint32_t dec_scale_:6;
          uint32_t dec_nwords_:3; // the actual nwords is dec_nwords_ + 1, so the range of nwords is [1, 8]
        } meta_;
      int32_t varchar_len_;
      union          // value实际内容
      {
        int64_t int_val;
        int64_t ext_val;
        float float_val;
        double double_val;
        ObDateTime time_val;
        ObPreciseDateTime precisetime_val;
        ObModifyTime modifytime_val;
        ObCreateTime createtime_val;
        const char *varchar_val;
        bool bool_val;
        uint32_t *dec_words_;
      } value_;
    };

    inline ObObj::ObObj()
    {
      meta_.type_ = ObNullType;
      meta_.op_flag_ = INVALID_OP_FLAG;
      meta_.dec_vscale_ = 0;
      meta_.dec_precision_ = 0;
      meta_.dec_nwords_ = 0;
      meta_.dec_scale_ = 0;
      varchar_len_ = 0;
      value_.int_val = 0;
    }

    inline void ObObj::reset()
    {
      meta_.type_ = ObNullType;
      meta_.op_flag_ = INVALID_OP_FLAG;
      meta_.dec_vscale_ = 0;
      meta_.dec_precision_ = 0;
      meta_.dec_nwords_ = 0;
      meta_.dec_scale_ = 0;
      varchar_len_ = 0;
      value_.int_val = 0;
    }

    inline void ObObj::set_flag(bool is_add)
    {
      uint8_t flag = is_add ? ADD : INVALID_OP_FLAG;
      meta_.op_flag_ = flag & META_OP_FLAG_MASK;
    }

    inline void ObObj::reset_op_flag()
    {
      if (ObExtendType == get_type() && ObActionFlag::OP_NOP == value_.ext_val)
      {
        meta_.type_ = ObNullType;
      }
      meta_.op_flag_ = INVALID_OP_FLAG;
    }

    inline bool ObObj::get_add()const
    {
      bool ret = false;
      switch (get_type())
      {
        case ObIntType:
        case ObDateTimeType:
        case ObPreciseDateTimeType:
          ret = (meta_.op_flag_ == ADD);
          break;
        default:
          ;/// do nothing
      }
      return ret;
    }

    inline int ObObj::set_add(const bool is_add /*=false*/)
    {
      int ret = OB_SUCCESS;
      switch (get_type())
      {
        case ObIntType:
        case ObDateTimeType:
        case ObPreciseDateTimeType:
        case ObDecimalType:
        case ObFloatType:
        case ObDoubleType:
          set_flag(is_add);
          break;
        default:
          TBSYS_LOG(ERROR, "check obj type failed:type[%d]", get_type());
          ret = OB_ERROR;
      }
      return ret;
    }

    inline void ObObj::set_type(const ObObjType& type)
    {
      meta_.type_ = static_cast<unsigned char>(type);
    }

    inline void ObObj::set_int(const int64_t value,const bool is_add /*=false*/)
    {
      set_flag(is_add);
      meta_.type_ = ObIntType;
      value_.int_val = value;
    }

    inline void ObObj::set_float(const float value,const bool is_add /*=false*/)
    {
      set_flag(is_add);
      meta_.type_ = ObFloatType;
      value_.float_val = value;
    }

    inline void ObObj::set_double(const double value,const bool is_add /*=false*/)
    {
      set_flag(is_add);
      meta_.type_ = ObDoubleType;
      value_.double_val = value;
    }

    inline void ObObj::set_datetime(const ObDateTime& value,const bool is_add /*=false*/)
    {
      set_flag(is_add);
      meta_.type_ = ObDateTimeType;
      value_.time_val= value;
    }

    inline void ObObj::set_precise_datetime(const ObPreciseDateTime& value,const bool is_add /*=false*/)
    {
      set_flag(is_add);
      meta_.type_ = ObPreciseDateTimeType;
      value_.precisetime_val = value;
    }

    inline void ObObj::set_varchar(const ObString& value)
    {
      meta_.type_ = ObVarcharType;
      meta_.op_flag_ = INVALID_OP_FLAG;
      value_.varchar_val = value.ptr();
      varchar_len_ = value.length();
    }

    inline void ObObj::set_seq()
    {
      meta_.type_ = ObSeqType;
      meta_.op_flag_ = INVALID_OP_FLAG;
    }

    inline void ObObj::set_modifytime(const ObModifyTime& value)
    {
      meta_.type_ = ObModifyTimeType;
      meta_.op_flag_ = INVALID_OP_FLAG;
      value_.modifytime_val = value;
    }

    inline void ObObj::set_createtime(const ObCreateTime& value)
    {
      meta_.type_ = ObCreateTimeType;
      meta_.op_flag_ = INVALID_OP_FLAG;
      value_.createtime_val = value;
    }

    inline void ObObj::set_bool(const bool value)
    {
      meta_.type_ = ObBoolType;
      meta_.op_flag_ = INVALID_OP_FLAG;
      value_.bool_val = value;
    }

    inline void ObObj::set_null()
    {
      meta_.type_ = ObNullType;
      meta_.op_flag_ = INVALID_OP_FLAG;
    }

    inline bool ObObj::is_null() const
    {
      return meta_.type_ == ObNullType;
    }

    inline void ObObj::set_ext(const int64_t value)
    {
      meta_.type_ = ObExtendType;
      meta_.op_flag_ = INVALID_OP_FLAG;
      value_.ext_val = value;
    }

    inline bool ObObj::is_valid_type() const
    {
      ObObjType type = get_type();
      bool ret = true;
      if (type <= ObMinType || type >= ObMaxType)
      {
        ret = false;
      }
      return ret;
    }

    inline int ObObj::get_int(int64_t& value,bool& is_add) const
    {
      int res = OB_OBJ_TYPE_ERROR;
      if (meta_.type_ == ObIntType)
      {
        is_add = (ADD == meta_.op_flag_);
        value = value_.int_val;
        res = OB_SUCCESS;
      }
      return res;
    }

    inline int ObObj::get_int(int64_t& value) const
    {
      bool add = false;
      return get_int(value,add);
    }


    inline bool ObObj::need_deep_copy()const
    {
      return (meta_.type_ == ObVarcharType);
    }

    inline int ObObj::get_datetime(ObDateTime& value,bool& is_add) const
    {
      int ret = OB_OBJ_TYPE_ERROR;
      if (ObDateTimeType == meta_.type_)
      {
        value = value_.time_val;
        is_add = (meta_.op_flag_ == ADD);
        ret = OB_SUCCESS;
      }
      return ret;
    }

    inline int ObObj::get_datetime(ObDateTime& value) const
    {
      bool add = false;
      return get_datetime(value,add);
    }

    inline int ObObj::get_precise_datetime(ObPreciseDateTime& value,bool& is_add) const
    {
      int ret = OB_OBJ_TYPE_ERROR;
      if (ObPreciseDateTimeType == meta_.type_)
      {
        value = value_.precisetime_val;
        is_add = (meta_.op_flag_ == ADD);
        ret = OB_SUCCESS;
      }
      return ret;
    }

    inline int ObObj::get_precise_datetime(ObPreciseDateTime& value) const
    {
      bool add = false;
      return get_precise_datetime(value,add);
    }

    inline int ObObj::get_varchar(ObString& value) const
    {
      int res = OB_OBJ_TYPE_ERROR;
      if (meta_.type_ == ObVarcharType)
      {
        value.assign_ptr(const_cast<char *>(value_.varchar_val), varchar_len_);
        res = OB_SUCCESS;
      }
      return res;
    }

    inline int ObObj::get_modifytime(ObModifyTime& value) const
    {
      int res = OB_OBJ_TYPE_ERROR;
      if (ObModifyTimeType == meta_.type_)
      {
        value = value_.modifytime_val;
        res = OB_SUCCESS;
      }
      return res;
    }

    inline int ObObj::get_createtime(ObCreateTime& value) const
    {
      int res = OB_OBJ_TYPE_ERROR;
      if (ObCreateTimeType == meta_.type_)
      {
        value = value_.createtime_val;
        res = OB_SUCCESS;
      }
      return res;
    }

    inline int ObObj::get_ext(int64_t& value) const
    {
      int res = OB_OBJ_TYPE_ERROR;
      if (ObExtendType == meta_.type_)
      {
        value = value_.ext_val;
        res = OB_SUCCESS;
      }
      return res;
    }

    inline int64_t ObObj::get_ext() const
    {
      int64_t res = 0;
      if (ObExtendType == meta_.type_)
      {
        res = value_.ext_val;
      }
      return res;
    }

    inline int ObObj::get_float(float& value,bool& is_add) const
    {
      int res = OB_OBJ_TYPE_ERROR;
      if (meta_.type_ == ObFloatType)
      {
        value = value_.float_val;
        is_add = (meta_.op_flag_ == ADD);
        res = OB_SUCCESS;
      }
      return res;
    }

    inline int ObObj::get_float(float& value) const
    {
      bool add = false;
      return get_float(value,add);
    }

    inline int ObObj::get_double(double& value,bool& is_add) const
    {
      int res = OB_OBJ_TYPE_ERROR;
      if (meta_.type_ == ObDoubleType)
      {
        value = value_.double_val;
        is_add = (meta_.op_flag_ == ADD);
        res = OB_SUCCESS;
      }
      return res;
    }

    inline int ObObj::get_double(double& value) const
    {
      bool add = false;
      return get_double(value,add);
    }

    inline ObObjType ObObj::get_type(void) const
    {
      return static_cast<ObObjType>(meta_.type_);
    }

    inline int ObObj::get_bool(bool &value) const
    {
      int res = OB_OBJ_TYPE_ERROR;
      if (get_type() == ObBoolType)
      {
        value = value_.bool_val;
        res = OB_SUCCESS;
      }
      return res;
    }

    inline int64_t ObObj::hash() const
    {
      return this->murmurhash2(0);
    }

  }
}

#endif //
