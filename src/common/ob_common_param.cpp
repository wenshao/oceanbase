#include "ob_object.h"
#include "ob_action_flag.h"
#include "ob_common_param.h"

namespace oceanbase
{
  namespace common
  {
    bool ObCellInfo::operator == (const ObCellInfo & other) const
    {
      return ((table_name_ == other.table_name_) && (table_id_ == other.table_id_)
          && (row_key_ == other.row_key_) && (column_name_ == other.column_name_)
          && (column_id_ == other.column_id_) && (value_ == other.value_));
    }

    ObReadParam::ObReadParam()
    {
      reset();
    }

    void ObReadParam::reset()
    {
      is_read_master_ = 1;
      is_result_cached_ = 0;
      version_range_.start_version_ = 0;
      version_range_.end_version_ = 0;
    }

    ObReadParam::~ObReadParam()
    {
    }

    void ObReadParam::set_is_read_consistency(const bool consistency)
    {
      is_read_master_ = consistency;
    }

    bool ObReadParam::get_is_read_consistency()const
    {
      return (is_read_master_ > 0);
    }

    void ObReadParam::set_is_result_cached(const bool cached)
    {
      is_result_cached_ = cached;
    }

    bool ObReadParam::get_is_result_cached()const
    {
      return (is_result_cached_ > 0);
    }

    void ObReadParam::set_version_range(const ObVersionRange & range)
    {
      version_range_ = range;
    }

    ObVersionRange ObReadParam::get_version_range(void) const
    {
      return version_range_;
    }

    int ObReadParam::serialize_reserve_param(char * buf, const int64_t buf_len, int64_t & pos) const
    {
      ObObj obj;
      // serialize RESERVER PARAM FIELD
      obj.set_ext(ObActionFlag::RESERVE_PARAM_FIELD);
      int ret = obj.serialize(buf, buf_len, pos);
      if (ret == OB_SUCCESS)
      {
        obj.set_int(get_is_read_consistency());
        ret = obj.serialize(buf, buf_len, pos);
      }
      return ret;
    }

    int ObReadParam::deserialize_reserve_param(const char * buf, const int64_t data_len, int64_t & pos)
    {
      ObObj obj;
      int64_t int_value = 0;
      int ret = obj.deserialize(buf, data_len, pos);
      if (OB_SUCCESS == ret)
      {
        ret = obj.get_int(int_value);
        if (OB_SUCCESS == ret)
        {
          //is read master 
          set_is_read_consistency(int_value);
        }
      }
      return ret; 
    }

    int64_t ObReadParam::get_reserve_param_serialize_size(void) const
    {
      ObObj obj;
      // reserve for read master
      obj.set_ext(ObActionFlag::RESERVE_PARAM_FIELD);
      int64_t total_size = obj.get_serialize_size();
      obj.set_int(get_is_read_consistency());
      total_size += obj.get_serialize_size();
      return total_size;
    }

    DEFINE_SERIALIZE(ObReadParam)
    {
      ObObj obj;
      // is cache
      obj.set_int(ObReadParam::get_is_result_cached());
      int ret = obj.serialize(buf, buf_len, pos);
      // scan version range
      if (ret == OB_SUCCESS)
      {
        ObVersionRange version_range = ObReadParam::get_version_range();;
        obj.set_int(version_range.border_flag_.get_data());
        ret = obj.serialize(buf, buf_len, pos);
        if (ret == OB_SUCCESS)
        {
          obj.set_int(version_range.start_version_);
          ret = obj.serialize(buf, buf_len, pos);
        }
        
        if (ret == OB_SUCCESS)
        {
          obj.set_int(version_range.end_version_);
          ret = obj.serialize(buf, buf_len, pos);
        }
      }
      
      return ret;
    }

    DEFINE_DESERIALIZE(ObReadParam)
    {
      ObObj obj;
      int ret = OB_SUCCESS;
      int64_t int_value = 0;
      ret = obj.deserialize(buf, data_len, pos);
      if (OB_SUCCESS == ret)
      {
        ret = obj.get_int(int_value);
        if (OB_SUCCESS == ret)
        {
          //is cached
          set_is_result_cached(int_value);
        }
      }

      // version range
      if (OB_SUCCESS == ret)
      {
        // border flag
        ret = obj.deserialize(buf, data_len, pos);
        if (OB_SUCCESS == ret)
        {
          ret = obj.get_int(int_value);
          if (OB_SUCCESS == ret)
          {
            version_range_.border_flag_.set_data(static_cast<int8_t>(int_value));
          }
        }
      }

      // start version
      if (OB_SUCCESS == ret)
      {
        ret = obj.deserialize(buf, data_len, pos);
        if (OB_SUCCESS == ret)
        {
          ret = obj.get_int(int_value);
          if (OB_SUCCESS == ret)
          {
            version_range_.start_version_ = int_value;
          }
        }
      }

      // end version
      if (OB_SUCCESS == ret)
      {
        ret = obj.deserialize(buf, data_len, pos);
        if (OB_SUCCESS == ret)
        {
          ret = obj.get_int(int_value);
          if (OB_SUCCESS == ret)
          {
            version_range_.end_version_ = int_value;
          }
        }
      }

      return ret;
    }

    DEFINE_GET_SERIALIZE_SIZE(ObReadParam)
    {
      ObObj obj;
      // is cache
      obj.set_int(get_is_result_cached());
      int64_t total_size = obj.get_serialize_size();
      
      // scan version range
      obj.set_int(version_range_.border_flag_.get_data());
      total_size += obj.get_serialize_size();
      obj.set_int(version_range_.start_version_);
      total_size += obj.get_serialize_size();
      obj.set_int(version_range_.end_version_);
      total_size += obj.get_serialize_size();
      
      return total_size;
    }

  } /* common */
} /* oceanbase */
