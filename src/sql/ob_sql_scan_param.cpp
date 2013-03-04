#include "common/ob_action_flag.h"
#include "common/ob_malloc.h"
#include "common/utility.h"
#include "ob_sql_scan_param.h"

namespace oceanbase
{
  namespace sql 
  {
    const char * SELECT_CLAUSE_WHERE_COND_AS_CNAME_PREFIX = "__WHERE_";

    void ObSqlScanParam::reset(void)
    {
      table_id_ = OB_INVALID_ID;
      ObReadParam::reset();
      range_.table_id_ = OB_INVALID_ID;
      range_.start_key_.assign(NULL, 0);
      range_.end_key_.assign(NULL, 0);
      project_.reset();
      limit_.reset();
      filter_.reset();
      has_project_ = false;
      has_limit_ = false;
      has_filter_ = false;
      buffer_pool_.reset();
    }

    // ObSqlScanParam
    ObSqlScanParam::ObSqlScanParam() : 
      table_id_(OB_INVALID_ID), range_(), project_(), limit_(), filter_(),
      has_project_(false), has_limit_(false), has_filter_(false)
    {
      deep_copy_args_ = false;
    }

    ObSqlScanParam::~ObSqlScanParam()
    {
      reset();
    }

    int ObSqlScanParam::set_range(const ObRange& range)
    {
      int err = OB_SUCCESS;
      range_ = range;
      range_.table_id_ = table_id_;
      if (deep_copy_args_)
      {
        if ((OB_SUCCESS == err) && (OB_SUCCESS != (err = buffer_pool_.write_string(range.start_key_,&(range_.start_key_)))))
        {
          TBSYS_LOG(WARN,"fail to copy range.start_key_ to local buffer [err:%d]", err);
        }
        if ((OB_SUCCESS == err) && (OB_SUCCESS != (err = buffer_pool_.write_string(range.end_key_,&(range_.end_key_)))))
        {
          TBSYS_LOG(WARN,"fail to copy range.end_key_ to local buffer [err:%d]", err);
        }
      }
      return err;
    }

    int ObSqlScanParam::set(const uint64_t& table_id, const ObRange& range, bool deep_copy_args)
    {
      int err = OB_SUCCESS;
      table_id_ = table_id;
      deep_copy_args_ = deep_copy_args;
      if ((OB_SUCCESS == err) && (OB_SUCCESS != (err = set_range(range))))
      {
        TBSYS_LOG(WARN,"fail to set range [err:%d]", err);
      }
      return err;
    }

    int ObSqlScanParam::set_project(const ObProject &project)
    {
      int ret = OB_SUCCESS;
      if (OB_SUCCESS == ret)
      {
        project_.assign(project);
        has_project_ = true;
      }
      return ret;
    }

    int ObSqlScanParam::set_filter(const ObFilter &filter)
    {
      int ret = OB_SUCCESS;
      if (OB_SUCCESS == ret)
      {
        filter_.assign(filter);
        has_filter_ = true;
      }
      return ret;
    }

    int ObSqlScanParam::set_limit(const ObLimit &limit)
    {
      int ret = OB_SUCCESS;
      if (OB_SUCCESS == ret)
      {
        limit_.assign(limit);
        has_limit_ = true;
      }
      return ret;
    }

    // BASIC_PARAM_FIELD
    int ObSqlScanParam::serialize_basic_param(char * buf, const int64_t buf_len, int64_t & pos) const
    {
      ObObj obj;
      obj.set_ext(ObActionFlag::BASIC_PARAM_FIELD);
      int ret = obj.serialize(buf, buf_len, pos);
      // read param info
      if (OB_SUCCESS == ret)
      {
        if (OB_SUCCESS != (ret = ObReadParam::serialize(buf, buf_len, pos)))
        {
          TBSYS_LOG(WARN, "fail to serialize ObReadParam. ret=%d", ret);
        }
      }
      // table id
      if (OB_SUCCESS == ret)
      {
        if (OB_INVALID_ID != table_id_)
        {
          obj.set_int(table_id_);
          ret = obj.serialize(buf, buf_len, pos);
        }
        else
        {
          TBSYS_LOG(WARN, "Invalid table_id_. table_id_=%ld", table_id_);
          ret = OB_ERROR;
        }
      }
      // scan range
      if (OB_SUCCESS == ret)
      {
        obj.set_int(range_.border_flag_.get_data());
        ret = obj.serialize(buf, buf_len, pos);
        if (OB_SUCCESS == ret)
        {
          obj.set_varchar(range_.start_key_);
          ret = obj.serialize(buf, buf_len, pos);
        }
        if (OB_SUCCESS == ret)
        {
          obj.set_varchar(range_.end_key_);
          ret = obj.serialize(buf, buf_len, pos);
        }
      }
      return ret;
    }

    int ObSqlScanParam::deserialize_basic_param(const char * buf, const int64_t data_len, int64_t & pos)
    {
      int64_t int_value = 0;
      int ret = ObReadParam::deserialize(buf, data_len, pos);
      // table id
      ObObj obj;
      ObString str_value;
      if (OB_SUCCESS == ret)
      {
        ret = obj.deserialize(buf, data_len, pos);
        if (OB_SUCCESS == ret)
        {
          if (ObIntType == obj.get_type())
          {
            ret = obj.get_int(int_value);
            if (OB_SUCCESS == ret)
            {
              table_id_ = int_value;
              range_.table_id_ = int_value;
            }
          }
        }
      }

      // scan range
      if (OB_SUCCESS == ret)
      {
        // border flag
        if (OB_SUCCESS == ret)
        {
          ret = obj.deserialize(buf, data_len, pos);
          if (OB_SUCCESS == ret)
          {
            ret = obj.get_int(int_value);
            if (OB_SUCCESS == ret)
            {
              range_.border_flag_.set_data(static_cast<int8_t>(int_value));
            }
          }
        }

        // start key
        if (OB_SUCCESS == ret)
        {
          ret = obj.deserialize(buf, data_len, pos);
          if (OB_SUCCESS == ret)
          {
            ret = obj.get_varchar(str_value);
            if (OB_SUCCESS == ret)
            {
              range_.start_key_ = str_value;
            }
          }
        }

        // end key
        if (OB_SUCCESS == ret)
        {
          ret = obj.deserialize(buf, data_len, pos);
          if (OB_SUCCESS == ret)
          {
            ret = obj.get_varchar(str_value);
            if (OB_SUCCESS == ret)
            {
              range_.end_key_ = str_value;
            }
          }
        }
      }
      return ret;
    }

    int64_t ObSqlScanParam::get_basic_param_serialize_size(void) const
    {
      int64_t total_size = 0;
      ObObj obj;
      // BASIC_PARAM_FIELD
      obj.set_ext(ObActionFlag::BASIC_PARAM_FIELD);
      total_size += obj.get_serialize_size();

      /// READ PARAM
      total_size += ObReadParam::get_serialize_size();

      // table id
      obj.set_int(table_id_);
      total_size += obj.get_serialize_size();

      // scan range
      obj.set_int(range_.border_flag_.get_data());
      total_size += obj.get_serialize_size();
      obj.set_varchar(range_.start_key_);
      total_size += obj.get_serialize_size();
      obj.set_varchar(range_.end_key_);
      total_size += obj.get_serialize_size();

      return total_size;
    }

    int ObSqlScanParam::serialize_end_param(char * buf, const int64_t buf_len, int64_t & pos) const
    {
      ObObj obj;
      obj.set_ext(ObActionFlag::END_PARAM_FIELD);
      return obj.serialize(buf, buf_len, pos);
    }

    int ObSqlScanParam::deserialize_end_param(const char * buf, const int64_t data_len, int64_t & pos)
    {
      // no data
      UNUSED(buf);
      UNUSED(data_len);
      UNUSED(pos);
      return 0;
    }

    int64_t ObSqlScanParam::get_end_param_serialize_size(void) const
    {
      ObObj obj;
      obj.set_ext(ObActionFlag::END_PARAM_FIELD);
      return obj.get_serialize_size();
    }

    DEFINE_SERIALIZE(ObSqlScanParam)
    {
      int ret = OB_SUCCESS;
      ObObj obj;

      // RESERVE_PARAM_FIELD
      if (OB_SUCCESS == ret)
      {
        if (OB_SUCCESS != (ret = ObReadParam::serialize_reserve_param(buf, buf_len, pos)))
        {
          TBSYS_LOG(WARN, "fail to serialize reserve param. buf=%p, buf_len=%ld, pos=%ld, ret=%d", buf, buf_len, pos, ret);
        }
      }

      // BASIC_PARAM_FIELD
      if (OB_SUCCESS == ret)
      {
        if (OB_SUCCESS != (ret = serialize_basic_param(buf, buf_len, pos)))
        {
          TBSYS_LOG(WARN, "fail to serialize basic param. buf=%p, buf_len=%ld, pos=%ld, ret=%d", buf, buf_len, pos, ret);
        }
      }

      // SQL_PROJECT_PARAM_FIELD
      if (OB_SUCCESS == ret && has_project_)
      {
        obj.set_ext(ObActionFlag::SQL_PROJECT_PARAM_FIELD);
        if (OB_SUCCESS != (ret = obj.serialize(buf, buf_len, pos)))
        {
          TBSYS_LOG(WARN, "fail to serialize obj. buf=%p, buf_len=%ld, pos=%ld, ret=%d", buf, buf_len, pos, ret);
        }
        else if (OB_SUCCESS != (ret = project_.serialize(buf, buf_len, pos)))
        {
          TBSYS_LOG(WARN, "fail to serialize project param. buf=%p, buf_len=%ld, pos=%ld, ret=%d", buf, buf_len, pos, ret);
        }
      }

      // LIMIT_PARAM_FIELD
      if (OB_SUCCESS == ret && has_limit_)
      {
        obj.set_ext(ObActionFlag::SQL_LIMIT_PARAM_FIELD);
        if (OB_SUCCESS != (ret = obj.serialize(buf, buf_len, pos)))
        {
          TBSYS_LOG(WARN, "fail to serialize obj. buf=%p, buf_len=%ld, pos=%ld, ret=%d", buf, buf_len, pos, ret);
        }
        else if (OB_SUCCESS != (ret = limit_.serialize(buf, buf_len, pos)))
        {
          TBSYS_LOG(WARN, "fail to serialize limit param. buf=%p, buf_len=%ld, pos=%ld, ret=%d", buf, buf_len, pos, ret);
        }
      }

      // FILTER_PARAM_FIELD
      if (OB_SUCCESS == ret && has_filter_)
      {
        obj.set_ext(ObActionFlag::SQL_FILTER_PARAM_FIELD);
        if (OB_SUCCESS != (ret = obj.serialize(buf, buf_len, pos)))
        {
          TBSYS_LOG(WARN, "fail to serialize obj. buf=%p, buf_len=%ld, pos=%ld, ret=%d", buf, buf_len, pos, ret);
        }
        else if (OB_SUCCESS != (ret = filter_.serialize(buf, buf_len, pos)))
        {
          TBSYS_LOG(WARN, "fail to serialize filter param. buf=%p, buf_len=%ld, pos=%ld, ret=%d", buf, buf_len, pos, ret);
        }
      }

      // END_PARAM_FIELD
      if (OB_SUCCESS == ret)
      {
        if (OB_SUCCESS != (ret = serialize_end_param(buf, buf_len, pos)))
        {
          TBSYS_LOG(WARN, "fail to serialize end param. buf=%p, buf_len=%ld, pos=%ld, ret=%d", buf, buf_len, pos, ret);
        }
      }
      
      return ret;
    }

    DEFINE_DESERIALIZE(ObSqlScanParam)
    {
      // reset contents
      reset();
      ObObj obj;
      int ret = OB_SUCCESS;
      while (OB_SUCCESS == ret)
      {
        do
        {
          ret = obj.deserialize(buf, data_len, pos);
        } while (OB_SUCCESS == ret && ObExtendType != obj.get_type());

        if (OB_SUCCESS == ret && ObActionFlag::END_PARAM_FIELD != obj.get_ext())
        {
          switch (obj.get_ext())
          {
          case ObActionFlag::RESERVE_PARAM_FIELD:
            {
              ret = ObReadParam::deserialize_reserve_param(buf, data_len, pos);
              break;
            }
          case ObActionFlag::BASIC_PARAM_FIELD:
            {
              ret = deserialize_basic_param(buf, data_len, pos);
              break;
            }
          case ObActionFlag::SQL_PROJECT_PARAM_FIELD:
            {
              if (OB_SUCCESS != (ret = project_.deserialize(buf, data_len, pos)))
              {
                TBSYS_LOG(WARN, "fail to deserialize project. buf=%p, data_len=%ld, pos=%ld, ret=%d",
                    buf, data_len, pos, ret);
              }
              else
              {
                has_project_ = true;
              }
              break;
            }
          case ObActionFlag::SQL_LIMIT_PARAM_FIELD:
            {
              if (OB_SUCCESS != (ret = limit_.deserialize(buf, data_len, pos)))
              {
                TBSYS_LOG(WARN, "fail to deserialize limit. buf=%p, data_len=%ld, pos=%ld, ret=%d",
                    buf, data_len, pos, ret);
              }
              else
              {
                has_limit_ = true;
              }
              break;
            }
          case ObActionFlag::SQL_FILTER_PARAM_FIELD:
            {
              if (OB_SUCCESS != (ret = filter_.deserialize(buf, data_len, pos)))
              {
                TBSYS_LOG(WARN, "fail to deserialize filter. buf=%p, data_len=%ld, pos=%ld, ret=%d",
                    buf, data_len, pos, ret);
              }
              else
              {
                has_filter_ = true;
              }
              break;
            }
          default:
            {
              // deserialize next cell
              // ret = obj.deserialize(buf, data_len, pos);
              break;
            }
          }
        }
        else
        {
          break;
        }
      }
     return ret;
    }

    DEFINE_GET_SERIALIZE_SIZE(ObSqlScanParam)
    {
      int64_t total_size = get_basic_param_serialize_size();
      total_size += ObReadParam::get_reserve_param_serialize_size();
      if (has_project_)
      {
        total_size += project_.get_serialize_size();
      }
      if (has_limit_)
      {
        total_size += limit_.get_serialize_size();
      }
      if (has_filter_)
      {
        total_size += filter_.get_serialize_size();
      }
      total_size += get_end_param_serialize_size();
      return total_size;
    }


    void ObSqlScanParam::dump(void) const
    {
    }

  } /* sql */
} /* oceanbase */


