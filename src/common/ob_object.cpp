#include <string.h>
#include <algorithm>
#include <math.h>

#include "ob_object.h"
#include "serialization.h"
#include "tbsys.h"
#include "ob_action_flag.h"
#include "utility.h"
#include "ob_crc64.h"
#include "murmur_hash.h"

using namespace oceanbase;
using namespace oceanbase::common;

const uint8_t ObObj::INVALID_OP_FLAG;
const uint8_t ObObj::ADD;

inline bool ObObj::is_datetime() const
{
  return ((meta_.type_ == ObDateTimeType)
          || (meta_.type_ == ObPreciseDateTimeType)
          || (meta_.type_ == ObCreateTimeType)
          || (meta_.type_ == ObModifyTimeType));
}

inline bool ObObj::can_compare(const ObObj & other) const
{
  bool ret = false;
  if ((get_type() == ObNullType) || (other.get_type() == ObNullType)
      || (get_type() == other.get_type()) || (is_datetime() && other.is_datetime()))
  {
    ret = true;
  }
  return ret;
}

int ObObj::get_timestamp(int64_t & timestamp) const
{
  int ret = OB_SUCCESS;
  switch(meta_.type_)
  {
    case ObDateTimeType:
      timestamp = value_.time_val * 1000 * 1000L;
      break;
    case ObPreciseDateTimeType:
      timestamp = value_.precisetime_val;
      break;
    case ObModifyTimeType:
      timestamp = value_.modifytime_val;
      break;
    case ObCreateTimeType:
      timestamp = value_.createtime_val;
      break;
    default:
      TBSYS_LOG(ERROR, "unexpected branch");
      ret = OB_OBJ_TYPE_ERROR;
  }
  return ret;
}

bool ObObj::is_true() const
{
  bool ret = false;
  switch (get_type())
  {
    case ObBoolType:
      ret = value_.bool_val;
      break;
    case ObVarcharType:
      ret = (varchar_len_ > 0);
      break;
    case ObIntType:
      ret = (value_.int_val != 0);
      break;
    case ObDecimalType:
    {
      ObNumber dec;
      bool is_add = false;
      if (OB_SUCCESS == get_decimal(dec, is_add))
      {
        ret = !dec.is_zero();
      }
      break;
    }
    case ObFloatType:
      ret = (fabsf(value_.float_val) > FLOAT_EPSINON);
      break;
    case ObDoubleType:
      ret = (fabs(value_.double_val) > DOUBLE_EPSINON);
      break;
    case ObDateTimeType:
    case ObPreciseDateTimeType:
    case ObCreateTimeType:
    case ObModifyTimeType:
    {
      int64_t ts1 = 0;
      get_timestamp(ts1);
      ret = (0 != ts1);
      break;
    }
    default:
      break;
  }
  return ret;
}


int ObObj::get_decimal(ObNumber &num, bool &is_add) const
{
  int ret = OB_OBJ_TYPE_ERROR;
  if (ObDecimalType == meta_.type_)
  {
    ret = OB_SUCCESS;
    is_add = (ADD == meta_.op_flag_);
    int8_t nwords = static_cast<int8_t>(meta_.dec_nwords_ + 1);
    int8_t vscale = meta_.dec_vscale_;
    if (nwords <= 3)
    {
      num.from(vscale, nwords, reinterpret_cast<const uint32_t*>(&varchar_len_));
    }
    else
    {
      num.from(vscale, nwords, value_.dec_words_);
    }
  }
  return ret;
}

int ObObj::get_decimal(ObNumber &num) const
{
  bool is_add;
  return get_decimal(num, is_add);
}

int ObObj::set_decimal(const ObNumber &num, int8_t precision, int8_t scale, bool is_add /*= false*/)
{
  int ret = OB_SUCCESS;
  set_flag(is_add);
  meta_.type_ = ObDecimalType;
  meta_.dec_precision_ = static_cast<uint8_t>(precision) & META_PREC_MASK;
  meta_.dec_scale_ = static_cast<uint8_t>(scale) & META_SCALE_MASK;
  int8_t nwords = 0;
  int8_t vscale = 0;
  uint32_t words[ObNumber::MAX_NWORDS];
  ret = num.round_to(precision, scale, nwords, vscale, words);
  if (OB_SUCCESS == ret)
  {
    if (nwords <= 3)
    {
      meta_.dec_nwords_ = static_cast<uint8_t>(nwords - 1) & META_NWORDS_MASK;
      meta_.dec_vscale_ = static_cast<uint8_t>(vscale) & META_VSCALE_MASK;
      memcpy(reinterpret_cast<uint32_t*>(&varchar_len_), words, sizeof(uint32_t)*nwords);
    }
    else
    {
      //@todo, use ob_pool.h to allocate memory
      ret = OB_NOT_IMPLEMENT;
    }
  }
  return ret;
}

int ObObj::compare_same_type(const ObObj &other) const
{
  int cmp = 0;
  switch(get_type())
  {
    case ObIntType:
      if (this->value_.int_val < other.value_.int_val)
      {
        cmp = -1;
      }
      else if (this->value_.int_val == other.value_.int_val)
      {
        cmp = 0;
      }
      else
      {
        cmp = 1;
      }
      break;
    case ObDecimalType:
    {
      ObNumber n1, n2;
      get_decimal(n1);
      other.get_decimal(n2);
      cmp = n1.compare(n2);
      break;
    }
    case ObVarcharType:
    {
      ObString varchar1, varchar2;
      this->get_varchar(varchar1);
      other.get_varchar(varchar2);
      cmp = varchar1.compare(varchar2);
      break;
    }
    case ObFloatType:
    {
      bool float_eq = fabsf(value_.float_val - other.value_.float_val) < FLOAT_EPSINON;
      if (float_eq)
      {
        cmp = 0;
      }
      else if (this->value_.float_val < other.value_.float_val)
      {
        cmp = -1;
      }
      else
      {
        cmp = 1;
      }
      break;
    }
    case ObDoubleType:
    {
      bool double_eq = fabs(value_.double_val - other.value_.double_val) < DOUBLE_EPSINON;
      if (double_eq)
      {
        cmp = 0;
      }
      else if (this->value_.double_val < other.value_.double_val)
      {
        cmp = -1;
      }
      else
      {
        cmp = 1;
      }
      break;
    }
    case ObDateTimeType:
    case ObPreciseDateTimeType:
    case ObCreateTimeType:
    case ObModifyTimeType:
    {
      int64_t ts1 = 0;
      int64_t ts2 = 0;
      get_timestamp(ts1);
      other.get_timestamp(ts2);
      if (ts1 < ts2)
      {
        cmp = -1;
      }
      else if (ts1 == ts2)
      {
        cmp = 0;
      }
      else
      {
        cmp = 1;
      }
      break;
    }
    case ObBoolType:
      cmp = this->value_.bool_val - other.value_.bool_val;
      break;
    default:
      TBSYS_LOG(ERROR, "invalid type=%d", get_type());
      break;
  }
  return cmp;
}

int ObObj::compare(const ObObj &other) const
{
  int cmp = 0;
  if (!can_compare(other))
  {
    TBSYS_LOG(ERROR, "can not be compared, this_type=%d other_type=%d",
              get_type(), other.get_type());
  }
  else
  {
    ObObjType this_type = get_type();
    ObObjType other_type = other.get_type();
    if (this_type == ObNullType || other_type == ObNullType)
    {
      cmp = this_type - other_type;
    }
    else
    {
      cmp = this->compare_same_type(other);
    }
  }
  return cmp;
}

bool ObObj::operator < (const ObObj &other) const
{
  return this->compare(other) < 0;
}

bool ObObj::operator>(const ObObj &other) const
{
  return this->compare(other) > 0;
}

bool ObObj::operator>=(const ObObj &other) const
{
  return this->compare(other) >= 0;
}

bool ObObj::operator<=(const ObObj &other) const
{
  return this->compare(other) <= 0;
}

bool ObObj::operator==(const ObObj &other) const
{
  return this->compare(other) == 0;
}

bool ObObj::operator!=(const ObObj &other) const
{
  return this->compare(other) != 0;
}

int ObObj::apply(const ObObj &mutation)
{
  int err = OB_SUCCESS;
  int org_type = get_type();
  int org_ext = get_ext();
  int mut_type = mutation.get_type();
  ObCreateTime create_time = 0;
  ObModifyTime modify_time = 0;
  bool is_add = false;
  bool org_is_add = false;
  if (ObSeqType == mut_type
      || ObMinType >= mut_type
      || ObMaxType <= mut_type)
  {
    TBSYS_LOG(WARN,"unsupported type [type:%d]", mut_type);
    err = OB_INVALID_ARGUMENT;
  }
  if (OB_SUCCESS == err
      && ObExtendType != org_type
      && ObNullType != org_type
      && ObExtendType != mut_type
      && ObNullType != mut_type
      && org_type != mut_type)
  {
    TBSYS_LOG(WARN,"type not coincident [this->type:%d,mutation.type:%d]",
              org_type, mut_type);
    err = OB_INVALID_ARGUMENT;
  }
  _ObjValue value, mutation_value;
  if (OB_SUCCESS == err)
  {
    bool ext_val_can_change =  (ObActionFlag::OP_ROW_DOES_NOT_EXIST == org_ext)  ||  (ObNullType == org_type);
    bool org_is_nop = (ObActionFlag::OP_NOP == org_ext);
    switch (mut_type)
    {
      case ObNullType:
        set_null();
        break;
      case ObVarcharType:
        *this = mutation;
        break;
      case ObBoolType:
        *this = mutation;
        break;
      case ObIntType:
        if (ext_val_can_change || org_is_nop)
        {
          set_int(0);
        }
        err = get_int(value.int_val,org_is_add);
        if (OB_SUCCESS == err)
        {
          err = mutation.get_int(mutation_value.int_val,is_add);
        }
        if (OB_SUCCESS == err)
        {
          if (is_add)
          {
            value.int_val += mutation_value.int_val; // @bug value overflow
          }
          else
          {
            value.int_val = mutation_value.int_val;
          }
          set_int(value.int_val, (org_is_add || org_is_nop) && is_add);
        }
        break;
      case ObFloatType:
        if (ext_val_can_change || org_is_nop)
        {
          set_float(0);
        }
        err = get_float(value.float_val,org_is_add);
        if (OB_SUCCESS == err)
        {
          err = mutation.get_float(mutation_value.float_val, is_add);
        }
        if (OB_SUCCESS == err)
        {
          if (is_add)
          {
            value.float_val += mutation_value.float_val;
          }
          else
          {
            value.float_val = mutation_value.float_val;
          }
          set_float(value.float_val,is_add && (org_is_add || org_is_nop));
        }
        break;
      case ObDoubleType:
        if (ext_val_can_change || org_is_nop)
        {
          set_double(0);
        }
        err = get_double(value.double_val,org_is_add);
        if (OB_SUCCESS == err)
        {
          err = mutation.get_double(mutation_value.double_val,is_add);
        }
        if (OB_SUCCESS == err)
        {
          if (is_add)
          {
            value.double_val += mutation_value.double_val;
          }
          else
          {
            value.double_val = mutation_value.double_val;
          }
          set_double(value.double_val, (org_is_add || org_is_nop) && is_add);
        }
        break;
      case ObDateTimeType:
        if (ext_val_can_change || org_is_nop)
        {
          set_datetime(0);
        }
        err = get_datetime(value.second_val,org_is_add);
        if (OB_SUCCESS == err)
        {
          err = mutation.get_datetime(mutation_value.second_val,is_add);
        }
        if (OB_SUCCESS == err)
        {
          if (is_add)
          {
            value.second_val += mutation_value.second_val;
          }
          else
          {
            value.second_val = mutation_value.second_val;
          }
          set_datetime(value.second_val,is_add && (org_is_add || org_is_nop));
        }
        break;
      case ObPreciseDateTimeType:
        if (ext_val_can_change || org_is_nop)
        {
          set_precise_datetime(0);
        }
        err = get_precise_datetime(value.microsecond_val,org_is_add);
        if (OB_SUCCESS == err)
        {
          err = mutation.get_precise_datetime(mutation_value.microsecond_val,is_add);
        }
        if (OB_SUCCESS == err)
        {
          if (is_add)
          {
            value.microsecond_val += mutation_value.microsecond_val;
          }
          else
          {
            value.microsecond_val = mutation_value.microsecond_val;
          }
          set_precise_datetime(value.microsecond_val,is_add && (org_is_add || org_is_nop));
        }
        break;
      case ObExtendType:
        switch (mutation.get_ext())
        {
          case ObActionFlag::OP_DEL_ROW:
          case ObActionFlag::OP_DEL_TABLE:
            /// used for join, if right row was deleted, set the cell to null
            set_null();
            break;
          case ObActionFlag::OP_ROW_DOES_NOT_EXIST:
            /// do nothing
            break;
          case ObActionFlag::OP_NOP:
            if (org_ext == ObActionFlag::OP_ROW_DOES_NOT_EXIST
                || org_ext == ObActionFlag::OP_DEL_ROW)
            {
              set_null();
            }
            break;
          default:
            TBSYS_LOG(ERROR,"unsupported ext value [value:%ld]", mutation.get_ext());
            err = OB_INVALID_ARGUMENT;
            break;
        }

        break;
      case ObCreateTimeType:
        err = mutation.get_createtime(create_time);
        if (OB_SUCCESS == err)
        {
          if (ext_val_can_change || org_is_nop)
          {
            set_createtime(create_time);
          }
        }
        if (OB_SUCCESS == err)
        {
          err = get_createtime(value.create_time_val);
        }
        if (OB_SUCCESS == err)
        {
          err = mutation.get_createtime(mutation_value.create_time_val);
        }
        if (OB_SUCCESS == err)
        {
          set_createtime(std::min<ObCreateTime>(value.create_time_val,mutation_value.create_time_val));
        }
        break;
      case ObModifyTimeType:
        err = mutation.get_modifytime(modify_time);
        if (OB_SUCCESS == err)
        {
          if (ext_val_can_change || org_is_nop)
          {
            set_modifytime(modify_time);
          }
        }
        if (OB_SUCCESS == err)
        {
          err = get_modifytime(value.modify_time_val);
        }
        if (OB_SUCCESS == err)
        {
          err = mutation.get_modifytime(mutation_value.modify_time_val);
        }
        if (OB_SUCCESS == err)
        {
          set_modifytime(std::max<ObCreateTime>(value.modify_time_val,mutation_value.modify_time_val));
        }
        break;
      case ObDecimalType:
      {
        ObNumber num, mutation_num, res;
        if (ext_val_can_change || org_is_nop)
        {
          num.set_zero();
        }
        else
        {
          err = get_decimal(num, org_is_add);
        }
        if (OB_SUCCESS == err)
        {
          err = mutation.get_decimal(mutation_num, is_add);
        }
        if (OB_SUCCESS == err)
        {
          if (is_add)
          {
            err = num.add(mutation_num, res);
          }
          else
          {
            res = mutation_num;
          }
        }
        if (OB_SUCCESS == err)
        {
          set_decimal(res, meta_.dec_precision_, meta_.dec_scale_, (org_is_add || org_is_nop) && is_add);
        }
        break;
      }
      default:
        /* case ObSeqType: */
        TBSYS_LOG(ERROR,"unsupported type [type:%d]", mut_type);
        err = OB_INVALID_ARGUMENT;
        break;
    }
  }
  if(OB_SUCCESS != err)
  {
    TBSYS_LOG(WARN,"fail to apply [this->type:%d,this->ext:%d,"
              "mutation->type:%d,mutation->ext:%ld, err:%d]",
              org_type, org_ext, mut_type, mutation.get_ext(), err);
  }
  return err;
}

    void ObObj::dump(const int32_t log_level /*=TBSYS_LOG_LEVEL_DEBUG*/) const
    {
      int64_t int_val = 0;
      bool bool_val = false;
      bool is_add = false;
      float float_val = 0.0f;
      double double_val = 0.0f;
      ObString str_val;
      ObNumber num;
      char num_buf[ObNumber::MAX_PRINTABLE_SIZE];
      switch (get_type())
      {
        case ObNullType:
          TBSYS_LOGGER.logMessage(TBSYS_LOG_NUM_LEVEL(log_level), "[%ld] type:ObNull",pthread_self());
          break;
        case ObIntType:
          get_int(int_val,is_add);
          TBSYS_LOGGER.logMessage(TBSYS_LOG_NUM_LEVEL(log_level),
              "[%ld] type:ObInt, val:%ld,is_add:%s",pthread_self(),int_val,is_add ? "true" : "false");
          break;
        case ObVarcharType:
          get_varchar(str_val);
          TBSYS_LOGGER.logMessage(TBSYS_LOG_NUM_LEVEL(log_level),
              "[%ld] type:ObVarChar,len :%d,val:",pthread_self(),str_val.length());
          common::hex_dump(str_val.ptr(),str_val.length(),true,log_level);
          break;
        case ObFloatType:
          get_float(float_val,is_add);
          TBSYS_LOGGER.logMessage(TBSYS_LOG_NUM_LEVEL(log_level),
              "[%ld] type:ObFloat, val:%f,is_add:%s",pthread_self(),float_val,is_add ? "true" : "false");
          break;
        case ObDoubleType:
          get_double(double_val,is_add);
          TBSYS_LOGGER.logMessage(TBSYS_LOG_NUM_LEVEL(log_level),
              "[%ld] type:ObDouble, val:%f,is_add:%s",pthread_self(),double_val,is_add ? "true" : "false");
          break;
        case ObDateTimeType:
          get_datetime(int_val,is_add);
          TBSYS_LOGGER.logMessage(TBSYS_LOG_NUM_LEVEL(log_level),
              "[%ld] type:ObDateTime(seconds), val:%ld,is_add:%s",pthread_self(),int_val,is_add ? "true" : "false");
          break;
        case ObPreciseDateTimeType:
          get_precise_datetime(int_val,is_add);
          TBSYS_LOGGER.logMessage(TBSYS_LOG_NUM_LEVEL(log_level),
              "[%ld] type:ObPreciseDateTime(microseconds), val:%ld,is_add:%s",pthread_self(),int_val,is_add ? "true" : "false");
          break;
        case ObSeqType:
          //TODO
          break;
        case ObCreateTimeType:
          get_createtime(int_val);
          TBSYS_LOGGER.logMessage(TBSYS_LOG_NUM_LEVEL(log_level),
              "[%ld] type:ObCreateTime, val:%ld",pthread_self(),int_val);
          break;
        case ObModifyTimeType:
          get_modifytime(int_val);
          TBSYS_LOGGER.logMessage(TBSYS_LOG_NUM_LEVEL(log_level),
              "[%ld] type:ObModifyTime, val:%ld",pthread_self(),int_val);
          break;
        case ObBoolType:
          get_bool(bool_val);
          TBSYS_LOGGER.logMessage(TBSYS_LOG_NUM_LEVEL(log_level),
              "[%ld] type:ObBool, val:%s",pthread_self(),bool_val?"true":"false");
          break;
        case ObExtendType:
          get_ext(int_val);
          TBSYS_LOGGER.logMessage(TBSYS_LOG_NUM_LEVEL(log_level),
              "[%ld] type:ObExt, val:%ld",pthread_self(),int_val);
          break;
        case ObDecimalType:
          get_decimal(num, is_add);
          num.to_string(num_buf, ObNumber::MAX_PRINTABLE_SIZE);
          TBSYS_LOGGER.logMessage(TBSYS_LOG_NUM_LEVEL(log_level),
                                  "[%ld] type:ObDecimalType, val:%s, is_add:%s",
                                  pthread_self(), num_buf, is_add ? "true" : "false");
          break;
        default:
          TBSYS_LOGGER.logMessage(TBSYS_LOG_NUM_LEVEL(log_level)," [%ld] unexpected type (%d)",pthread_self(),get_type());
          break;
      }
    }


    void ObObj::print_value(FILE* fd)
    {
      switch (get_type())
      {
        case ObNullType:
          fprintf(fd, "nil");
          break;
        case ObIntType:
          fprintf(fd, "%ld", value_.int_val);
          break;
        case ObVarcharType:
          fprintf(fd, "%.*s", varchar_len_, value_.varchar_val);
          break;
        case ObFloatType:
          fprintf(fd, "%2f", value_.float_val);
          break;
        case ObDoubleType:
          fprintf(fd, "%2lf", value_.double_val);
          break;
        case ObDateTimeType:
          fprintf(fd, "%s", time2str(value_.time_val));
          break;
        case ObPreciseDateTimeType:
          fprintf(fd, "%s", time2str(value_.precisetime_val));
          break;
        case ObModifyTimeType:
          fprintf(fd, "%s", time2str(value_.modifytime_val));
          break;
        case ObCreateTimeType:
          fprintf(fd, "%s", time2str(value_.createtime_val));
          break;
        case ObSeqType:
          fprintf(fd, "seq");
          break;
        case ObExtendType:
          fprintf(fd, "%lde", value_.ext_val);
          break;
        case ObDecimalType:
        {
          char num_buf[ObNumber::MAX_PRINTABLE_SIZE];
          ObNumber num;
          get_decimal(num);
          num.to_string(num_buf, ObNumber::MAX_PRINTABLE_SIZE);
          fprintf(fd, "%s", num_buf);
          break;
        }
        default:
          break;
      }
    }


    DEFINE_SERIALIZE(ObObj)
    {
      ObObjType type = get_type();
      int ret = 0;
      int64_t tmp_pos = pos;
      int8_t obj_op_flag = meta_.op_flag_;

      if (OB_SUCCESS == ret)
      {
        switch (type)
        {
          case ObNullType:
            ret = serialization::encode_null(buf,buf_len,tmp_pos);
            break;
          case ObIntType:
            ret = serialization::encode_int(buf,buf_len,tmp_pos,value_.int_val,obj_op_flag == ADD);
            break;
          case ObVarcharType:
            ret = serialization::encode_str(buf,buf_len,tmp_pos,value_.varchar_val,varchar_len_);
            break;
          case ObFloatType:
            ret = serialization::encode_float_type(buf,buf_len,tmp_pos,value_.float_val,obj_op_flag == ADD);
            break;
          case ObDoubleType:
            ret = serialization::encode_double_type(buf,buf_len,tmp_pos,value_.double_val,obj_op_flag == ADD);
            break;
          case ObDateTimeType:
            ret = serialization::encode_datetime_type(buf,buf_len,tmp_pos,value_.time_val,obj_op_flag == ADD);
            break;
          case ObPreciseDateTimeType:
            ret = serialization::encode_precise_datetime_type(buf,buf_len,tmp_pos,value_.precisetime_val,obj_op_flag == ADD);
            break;
          case ObModifyTimeType:
            ret = serialization::encode_modifytime_type(buf,buf_len,tmp_pos,value_.modifytime_val);
            break;
          case ObCreateTimeType:
            ret = serialization::encode_createtime_type(buf,buf_len,tmp_pos,value_.createtime_val);
            break;
          case ObSeqType:
            //TODO
            break;
          case ObExtendType:
            ret = serialization::encode_extend_type(buf,buf_len,tmp_pos,value_.ext_val);
            break;
          case ObBoolType:
            ret = serialization::encode_bool_type(buf, buf_len, tmp_pos, value_.bool_val);
            break;
          case ObDecimalType:
            if (meta_.dec_nwords_ + 1 <= 3)
            {
              ret = serialization::encode_decimal_type(buf, buf_len, tmp_pos, obj_op_flag == ADD, meta_.dec_precision_,
                                                       meta_.dec_scale_, meta_.dec_vscale_,
                                                       static_cast<int8_t>(meta_.dec_nwords_ + 1),
                                                       reinterpret_cast<const uint32_t*>(&varchar_len_));
            }
            else
            {
              ret = serialization::encode_decimal_type(buf, buf_len, tmp_pos, obj_op_flag == ADD, meta_.dec_precision_,
                                                       meta_.dec_scale_, meta_.dec_vscale_, static_cast<int8_t>(meta_.dec_nwords_ + 1),
                                                       value_.dec_words_);
            }
            break;
          default:
            TBSYS_LOG(ERROR, "invalid obj_type=%d", type);
            ret = OB_ERR_UNEXPECTED;
            break;
        }
      }
      if (OB_SUCCESS == ret)
        pos = tmp_pos;
      return ret;
    }

    DEFINE_DESERIALIZE(ObObj)
    {
      int ret = OB_SUCCESS;
      int64_t tmp_pos = pos;
      int8_t first_byte = 0;
      bool is_add = false;

      if (OB_SUCCESS == (ret = serialization::decode_i8(buf,data_len,tmp_pos,&first_byte)))
      {
        if ( serialization::OB_EXTEND_TYPE == first_byte )  // is extend type
        {
          meta_.type_ = ObExtendType;
          ret = serialization::decode_vi64(buf,data_len,tmp_pos,&value_.ext_val);
        }
        else
        {
          int8_t type = static_cast<int8_t>((first_byte & 0xc0) >> 6);
          switch (type)
          {
            case 0:
            case 1: //int
              meta_.type_ = ObIntType;
              ret = serialization::decode_int(buf,data_len,first_byte,tmp_pos,value_.int_val,is_add);
              break;
            case 2: //str
              meta_.type_ = ObVarcharType;
              value_.varchar_val = serialization::decode_str(buf,data_len,first_byte,tmp_pos,varchar_len_);
              if (NULL == value_.varchar_val)
              {
                ret = OB_ERROR;
              }
              break;
            case 3: //other
              {
                int8_t  sub_type = static_cast<int8_t>((first_byte & 0x30) >> 4); //00 11 00 00
                switch (sub_type)
                {
                  case 0: //TODO seq & reserved
                    break;
                  case 1: //ObDatetime
                    meta_.type_ = ObDateTimeType;
                    ret = serialization::decode_datetime_type(buf,data_len,first_byte,tmp_pos,value_.time_val,is_add);
                    break;
                  case 2: //ObPreciseDateTime
                    meta_.type_ = ObPreciseDateTimeType;
                    ret = serialization::decode_precise_datetime_type(buf,data_len,first_byte,tmp_pos,value_.precisetime_val,is_add);
                    break;
                  case 3: //other
                    {
                      int8_t sub_sub_type = static_cast<int8_t>((first_byte & 0x0c) >> 2); // 00 00 11 00
                      switch (sub_sub_type)
                      {
                        case 0: //ObModifyTime
                          meta_.type_ = ObModifyTimeType;
                          ret = serialization::decode_modifytime_type(buf,data_len,first_byte,tmp_pos,value_.modifytime_val);
                          break;
                        case 1: //ObCreateTime
                          meta_.type_ = ObCreateTimeType;
                          ret = serialization::decode_createtime_type(buf,data_len,first_byte,tmp_pos,value_.createtime_val);
                          break;
                        case 2:
                          if (first_byte & 0x02) //ObDouble
                          {
                            meta_.type_ = ObDoubleType;
                            ret = serialization::decode_double_type(buf,data_len,first_byte,tmp_pos,value_.double_val,is_add);
                          }
                          else //ObFloat
                          {
                            meta_.type_ = ObFloatType;
                            ret = serialization::decode_float_type(buf,data_len,first_byte,tmp_pos,value_.float_val,is_add);
                          }
                          break;
                        case 3: //Other
                          {

                            int8_t sub_sub_sub_type = first_byte & 0x03;
                            switch (sub_sub_sub_type)
                            {
                              case 0:
                                meta_.type_ = ObNullType;
                                break;
                              case 1:
                                meta_.type_ = ObBoolType;
                                ret = serialization::decode_bool_type(buf,data_len,first_byte,tmp_pos,value_.bool_val);
                                break;
                              case 3: //obdecimaltype
                                {
                                  meta_.type_ = ObDecimalType;
                                  uint32_t words[ObNumber::MAX_NWORDS];
                                  int8_t p = 0;
                                  int8_t s = 0;
                                  int8_t vs = 0;
                                  int8_t n = 0;
                                  ret = serialization::decode_decimal_type(buf, data_len, tmp_pos, is_add, p, s, vs, n, words);
                                  if(OB_SUCCESS == ret)
                                  {
                                    meta_.dec_precision_ = static_cast<uint8_t>(p) & META_PREC_MASK;
                                    meta_.dec_scale_ = static_cast<uint8_t>(s) & META_SCALE_MASK;
                                    meta_.dec_vscale_ = static_cast<uint8_t>(vs) & META_VSCALE_MASK;
                                    meta_.dec_nwords_ = static_cast<uint8_t>(n - 1) & META_NWORDS_MASK;
                                    if (n <= 3)
                                    {
                                      memcpy(reinterpret_cast<char*>(&varchar_len_), words, n * sizeof(uint32_t));
                                    }
                                    else
                                    {
                                      //@todo
                                      ret = OB_NOT_IMPLEMENT;
                                    }
                                  }
                                  break;
                                }
                              default:
                                TBSYS_LOG(ERROR, "invalid obj_type=%d", sub_sub_sub_type);
                                ret = OB_ERR_UNEXPECTED;
                                break;
                            }
                            break;
                          }
                        default:
                          TBSYS_LOG(ERROR, "invalid obj_type=%d", sub_sub_type);
                          ret = OB_ERR_UNEXPECTED;
                          break;
                      }
                      break;
                    }
                  default:
                    TBSYS_LOG(ERROR, "invalid obj_type=%d", sub_type);
                    ret = OB_ERR_UNEXPECTED;
                    break;
                }
              }
              break;
            default:
              TBSYS_LOG(ERROR, "invalid obj_type=%d", type);
              ret = OB_ERR_UNEXPECTED;
              break;
          }
          //
          if (is_add)
          {
            meta_.op_flag_ = ADD;
          }
          else
          {
            meta_.op_flag_ = INVALID_OP_FLAG;
          }
        }
        if (OB_SUCCESS == ret)
          pos = tmp_pos;
      }
      return ret;
    }

    DEFINE_GET_SERIALIZE_SIZE(ObObj)
    {
      ObObjType type = get_type();
      int64_t len = 0;

      switch (type)
      {
        case ObNullType:
          len += serialization::encoded_length_null();
          break;
        case ObIntType:
          len += serialization::encoded_length_int(value_.int_val);
          break;
        case ObVarcharType:
          len += serialization::encoded_length_str(varchar_len_);
          break;
        case ObFloatType:
          len += serialization::encoded_length_float_type();
          break;
        case ObDoubleType:
          len += serialization::encoded_length_double_type();
          break;
        case ObDateTimeType:
          len += serialization::encoded_length_datetime(value_.time_val);
          break;
        case ObPreciseDateTimeType:
          len += serialization::encoded_length_precise_datetime(value_.precisetime_val);
          break;
        case ObModifyTimeType:
          len += serialization::encoded_length_modifytime(value_.modifytime_val);
          break;
        case ObCreateTimeType:
          len += serialization::encoded_length_createtime(value_.createtime_val);
          break;
        case ObSeqType:
          //TODO (maoqi)
          break;
        case ObExtendType:
          len += serialization::encoded_length_extend(value_.ext_val);
          break;
        case ObBoolType:
          len += serialization::encoded_length_bool_type(value_.bool_val);
          break;
        case ObDecimalType:
          if (meta_.dec_nwords_+1 <= 3)
          {
            len += serialization::encoded_length_decimal_type(static_cast<int8_t>(meta_.dec_nwords_+1), reinterpret_cast<const uint32_t*>(&varchar_len_));
          }
          else
          {
            len += serialization::encoded_length_decimal_type(static_cast<int8_t>(meta_.dec_nwords_+1), value_.dec_words_);
          }
          break;
        default:
          TBSYS_LOG(ERROR,"unexpected obj type [obj.type:%d]", type);
          break;
      }
      return len;
    }

    uint32_t ObObj::murmurhash2(const uint32_t hash) const
    {
      uint32_t result = hash;
      ObObjType type = get_type();

      result = ::murmurhash2(&meta_,sizeof(meta_),result);
      switch (type)
      {
        case ObNullType:
          break;
        case ObIntType:
          result = ::murmurhash2(&value_.int_val,sizeof(value_.int_val),result);
          break;
        case ObVarcharType:
          result = ::murmurhash2(value_.varchar_val,varchar_len_,result);
          break;
        case ObFloatType:
          result = ::murmurhash2(&value_.float_val,sizeof(value_.float_val),result);
          break;
        case ObDoubleType:
          result = ::murmurhash2(&value_.double_val,sizeof(value_.double_val),result);
          break;
        case ObDateTimeType:
          result = ::murmurhash2(&value_.time_val,sizeof(value_.time_val),result);
          break;
        case ObPreciseDateTimeType:
          result = ::murmurhash2(&value_.precisetime_val,sizeof(value_.precisetime_val),result);
          break;
        case ObModifyTimeType:
          result = ::murmurhash2(&value_.modifytime_val,sizeof(value_.modifytime_val),result);
          break;
        case ObCreateTimeType:
          result = ::murmurhash2(&value_.createtime_val,sizeof(value_.createtime_val),result);
          break;
        case ObSeqType:
          //TODO (maoqi)
          break;
        case ObExtendType:
          result = ::murmurhash2(&value_.ext_val,sizeof(value_.ext_val),result);
          break;
        case ObBoolType:
          result = ::murmurhash2(&value_.bool_val,sizeof(value_.bool_val),result);
          break;
        case ObDecimalType:
        {
          int8_t nwords = static_cast<int8_t>(meta_.dec_nwords_+1);
          if (nwords <= 3)
          {
            result = ::murmurhash2(reinterpret_cast<const uint32_t*>(&varchar_len_), static_cast<int32_t>(sizeof(uint32_t)*nwords), result);
          }
          else
          {
            result = ::murmurhash2(value_.dec_words_, static_cast<int32_t>(sizeof(uint32_t)*nwords), result);
          }
          break;
        }
        default:
          TBSYS_LOG(ERROR, "invalid obj_type=%d", type);
          result = 0;
          break;
      }
      return result;
    }

    int64_t ObObj::checksum(const int64_t current) const
    {
      int64_t ret = current;
      ObObjType type = get_type();

      ret = ob_crc64(ret, &meta_, sizeof(meta_));
      switch (type)
      {
        case ObNullType:
          break;
        case ObIntType:
          ret = ob_crc64(ret, &value_.int_val, sizeof(value_.int_val));
          break;
        case ObVarcharType:
          ret = ob_crc64(ret, value_.varchar_val, varchar_len_);
          break;
        case ObFloatType:
          ret = ob_crc64(ret, &value_.float_val, sizeof(value_.float_val));
          break;
        case ObDoubleType:
          ret = ob_crc64(ret, &value_.double_val, sizeof(value_.double_val));
          break;
        case ObDateTimeType:
          ret = ob_crc64(ret, &value_.time_val, sizeof(value_.time_val));
          break;
        case ObPreciseDateTimeType:
          ret = ob_crc64(ret, &value_.precisetime_val, sizeof(value_.precisetime_val));
          break;
        case ObModifyTimeType:
          ret = ob_crc64(ret, &value_.modifytime_val, sizeof(value_.modifytime_val));
          break;
        case ObCreateTimeType:
          ret = ob_crc64(ret, &value_.createtime_val, sizeof(value_.createtime_val));
          break;
        case ObSeqType:
          //TODO (maoqi)
          break;
        case ObExtendType:
          ret = ob_crc64(ret, &value_.ext_val, sizeof(value_.ext_val));
          break;
        case ObBoolType:
          ret = ob_crc64(ret, &value_.bool_val, sizeof(value_.bool_val));
          break;
        case ObDecimalType:
        {
          int8_t nwords = static_cast<int8_t>(meta_.dec_nwords_+1);
          if (nwords <= 3)
          {
            ret = ob_crc64(ret, reinterpret_cast<const uint32_t*>(&varchar_len_), sizeof(uint32_t)*nwords);
          }
          else
          {
            ret = ob_crc64(ret, value_.dec_words_, sizeof(uint32_t)*nwords);
          }
          break;
        }
        default:
          TBSYS_LOG(ERROR, "invalid obj_type=%d", type);
          ret = 0;
          break;
      }
      return ret;
    }

    void ObObj::checksum(ObBatchChecksum &bc) const
    {
      ObObjType type = get_type();

      bc.fill(&meta_, sizeof(meta_));
      switch (type)
      {
        case ObNullType:
          break;
        case ObIntType:
          bc.fill(&value_.int_val, sizeof(value_.int_val));
          break;
        case ObVarcharType:
          bc.fill(value_.varchar_val, varchar_len_);
          break;
        case ObDateTimeType:
          bc.fill(&value_.time_val, sizeof(value_.time_val));
          break;
        case ObPreciseDateTimeType:
          bc.fill(&value_.precisetime_val, sizeof(value_.precisetime_val));
          break;
        case ObModifyTimeType:
          bc.fill(&value_.modifytime_val, sizeof(value_.modifytime_val));
          break;
        case ObCreateTimeType:
          bc.fill(&value_.createtime_val, sizeof(value_.createtime_val));
          break;
        case ObSeqType:
          //TODO (maoqi)
          break;
        case ObExtendType:
          bc.fill(&value_.ext_val, sizeof(value_.ext_val));
          break;
        case ObBoolType:
          bc.fill(&value_.bool_val, sizeof(value_.bool_val));
          break;
        case ObDecimalType:
        {
          int8_t nwords = static_cast<int8_t>(meta_.dec_nwords_+1);
          if (nwords <= 3)
          {
            bc.fill(reinterpret_cast<const uint32_t*>(&varchar_len_), sizeof(uint32_t)*nwords);
          }
          else
          {
            bc.fill(value_.dec_words_, sizeof(uint32_t)*nwords);
          }
          break;
        }
        default:
          TBSYS_LOG(ERROR, "invalid obj_type=%d", type);
          break;
      }
    }
