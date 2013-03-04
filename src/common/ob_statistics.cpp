/*===============================================================
*   (C) 2007-2010 Taobao Inc.
*   
*   
*   Version: 0.1 2010-12-06
*   
*   Authors:
*          daoan(daoan@taobao.com)
*   
*
================================================================*/
#include <tblog.h>

#include "ob_statistics.h"
#include "ob_atomic.h"
#include "serialization.h"

namespace oceanbase 
{
  namespace common 
  {
    ObStat::ObStat():table_id_(OB_INVALID_ID)
    {
      memset((void*)(values_), 0, sizeof(int64_t) * MAX_STATICS_PER_TABLE);
    }
    uint64_t ObStat::get_table_id() const
    {
      return table_id_;
    }
    int64_t ObStat::get_value(const int32_t index) const
    {
      int64_t ret = OB_SIZE_OVERFLOW;
      if (index < MAX_STATICS_PER_TABLE && index >= 0) 
      {
        ret = values_[index];
      }
      return ret;
    }
    int ObStat::set_value(const int32_t index, int64_t value)
    {
      int ret = OB_SIZE_OVERFLOW;
      if (index < MAX_STATICS_PER_TABLE && index >= 0) 
      {
        atomic_exchange(reinterpret_cast<volatile uint64_t*>(&(values_[index])), value);
        ret = OB_SUCCESS;
      }
      return ret;
    }
    int ObStat::inc(const int32_t index, const int64_t inc_value)
    {
      int ret = OB_SIZE_OVERFLOW;
      if (index < MAX_STATICS_PER_TABLE && index >= 0) 
      {
        if (inc_value == 1) 
        {
          atomic_inc(reinterpret_cast<volatile uint64_t*>(&(values_[index])));
        }
        else
        {
          atomic_add(reinterpret_cast<volatile uint64_t*>(&(values_[index])), inc_value);
        }
        ret = OB_SUCCESS;
      }
      return ret;
    }
    void ObStat::set_table_id(const uint64_t table_id)
    {
      if (table_id_ == OB_INVALID_ID)
      {
        table_id_ = table_id;
      }
      return;
    }
    DEFINE_SERIALIZE(ObStat)
    {
      int ret = OB_SUCCESS;
      int64_t tmp_pos = pos;
      ret = serialization::encode_vi64(buf, buf_len, tmp_pos, table_id_);
      if (OB_SUCCESS == ret)
      {
        for (int32_t i = 0; i < MAX_STATICS_PER_TABLE; i++)
        {
          ret = serialization::encode_vi64(buf, buf_len, tmp_pos, values_[i]);
          if (OB_SUCCESS != ret)
          {
            break;
          }

        }
      }
      if (OB_SUCCESS == ret)
      {
        pos = tmp_pos;
      }
      return ret;
    }
    DEFINE_DESERIALIZE(ObStat)
    {
      int ret = OB_SUCCESS;      
      int64_t tmp_pos = pos;
      if (OB_SUCCESS == ret)
      {
        ret = serialization::decode_vi64(buf, data_len, tmp_pos, reinterpret_cast<int64_t*>(&table_id_));
      }
      if (OB_SUCCESS == ret)
      {
        int64_t value = 0;
        for (int64_t i = 0; i < MAX_STATICS_PER_TABLE; i++)
        {
          ret = serialization::decode_vi64(buf, data_len, tmp_pos, &value);
          values_[i] = value;
          if (OB_SUCCESS != ret) 
          {
            break;
          }
        }
      }
      if (OB_SUCCESS == ret)
      {
        pos = tmp_pos;
      }
      return ret;
    }
    DEFINE_GET_SERIALIZE_SIZE(ObStat)
    {
      int64_t len = serialization::encoded_length_vi64(table_id_);
      for (int64_t i = 0; i < MAX_STATICS_PER_TABLE; i++)
      {
        len += serialization::encoded_length_vi64(values_[i]);
      }
      return len;
    }

    ObStatManager::ObStatManager(uint64_t server_type):server_type_(server_type)
    {
      table_stats_.init(OB_MAX_TABLE_NUMBER, data_holder_);
    }
    ObStatManager::ObStatManager()
    {
      reset();
    }
    ObStatManager::~ObStatManager()
    {
    }
    uint64_t ObStatManager::get_server_type() const
    {
      return server_type_;
    }
    void ObStatManager::set_server_type(const uint64_t server_type)
    {
      server_type_ = server_type;
    }
    int ObStatManager::set_value(const uint64_t table_id, const int32_t index, const int64_t value)
    {
      int ret = OB_ERROR;
      bool need_add_new = true;
      for (int32_t i = 0; table_id != OB_INVALID_ID && i < table_stats_.get_array_index(); i++)
      {
        if (data_holder_[i].get_table_id() == table_id)
        {
          need_add_new = false;
          ret = data_holder_[i].set_value(index, value);
        }
      }
      if (need_add_new && table_id != OB_INVALID_ID) 
      {
        ObStat stat;
        stat.set_table_id(table_id);
        ret = stat.set_value(index, value);
        if (OB_SUCCESS == ret)
        {
          if (!table_stats_.push_back(stat))
          {
            TBSYS_LOG(WARN, "too much tables");
            ret = OB_ERROR;
          }
        }
      }
      return ret;
    }
    int ObStatManager::inc(const uint64_t table_id, const int32_t index, const int64_t inc_value)
    {
      int ret = OB_ERROR;
      bool need_add_new = true;
      for (int32_t i = 0; table_id != OB_INVALID_ID && i < table_stats_.get_array_index(); i++)
      {
        if (data_holder_[i].get_table_id() == table_id)
        {
          need_add_new = false;
          ret = data_holder_[i].inc(index, inc_value);
        }
      }
      if (need_add_new && table_id != OB_INVALID_ID) 
      {
        ObStat stat;
        stat.set_table_id(table_id);
        ret = stat.set_value(index, inc_value);
        if (OB_SUCCESS == ret)
        {
          if (!table_stats_.push_back(stat))
          {
            TBSYS_LOG(WARN, "too much tables");
            ret = OB_ERROR;
          }
        }
      }
      return ret;
    }

    ObStatManager::const_iterator ObStatManager::begin() const
    {
      return data_holder_;
    }
    ObStatManager::const_iterator ObStatManager::end() const
    {
      return data_holder_ + table_stats_.get_array_index();
    }
    DEFINE_SERIALIZE(ObStatManager)
    {
      int ret = OB_SUCCESS;
      int64_t tmp_pos = pos;
      ret = serialization::encode_vi64(buf, buf_len, tmp_pos, server_type_);
      //we need get size first, so new table added to stat will not occus error
      int32_t size = static_cast<int32_t>(table_stats_.get_array_index()); 

      if (OB_SUCCESS == ret)
      {
        ret = serialization::encode_vi32(buf, buf_len, tmp_pos, size);
      }
      if (OB_SUCCESS == ret)
      {
        for (int32_t i = 0; i < size; i++)
        {
          ret = data_holder_[i].serialize(buf, buf_len, tmp_pos);
          if (OB_SUCCESS != ret)
          {
            break;
          }
        }
      }
      if (OB_SUCCESS == ret)
      {
        pos = tmp_pos;
      }
      return ret;
    }
    DEFINE_DESERIALIZE(ObStatManager)
    {
      int ret = OB_SUCCESS;      
      int64_t tmp_pos = pos;
      int32_t size = 0;
      if (OB_SUCCESS == ret)
      {
        ret = serialization::decode_vi64(buf, data_len, tmp_pos, reinterpret_cast<int64_t*>(&server_type_));
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::decode_vi32(buf, data_len, tmp_pos, &size);
      }
      if (OB_SUCCESS == ret)
      {
        for (int64_t i = 0; i < size; i++)
        {
          ret = data_holder_[i].deserialize(buf, data_len, tmp_pos);
          if (OB_SUCCESS != ret) 
          {
            break;
          }
        }
      }
      if (OB_SUCCESS == ret)
      {
        table_stats_.init(OB_MAX_TABLE_NUMBER, data_holder_, size);
        pos = tmp_pos;
      }
      return ret;
    }
    DEFINE_GET_SERIALIZE_SIZE(ObStatManager)
    {
      int64_t len = serialization::encoded_length_vi64(server_type_);
      len += serialization::encoded_length_vi32(static_cast<int32_t>(table_stats_.get_array_index()));
      for (int64_t i = 0; i < table_stats_.get_array_index(); i++)
      {
        len += data_holder_[i].get_serialize_size();
      }
      return len;
    }

    int ObStatManager::reset()
    {
      server_type_ = 0;
      memset(data_holder_, 0, sizeof(data_holder_));
      table_stats_.init(OB_MAX_TABLE_NUMBER, data_holder_);
      return OB_SUCCESS;
    }

    ObStatManager & ObStatManager::operator=(const ObStatManager &rhs)
    {
      if (this != &rhs)
      {
        reset();
        server_type_ = rhs.get_server_type();
        const_iterator it = rhs.begin();
        while (it != rhs.end())
        {
          table_stats_.push_back(*it);
          ++it;
        }
      }
      return *this;
    }

    int64_t ObStatManager::addop(const int64_t lv, const int64_t rv)
    {
      return lv + rv;
    }

    int64_t ObStatManager::subop(const int64_t lv, const int64_t rv)
    {
      return lv - rv;
    }

    ObStatManager & ObStatManager::add(const ObStatManager &augend)
    {
      return operate(augend, addop);
    }

    ObStatManager & ObStatManager::subtract(const ObStatManager &minuend)
    {
      return operate(minuend, subop);
    }

    ObStatManager & ObStatManager::operate(const ObStatManager &operand, OpFunc op)
    {
      if (server_type_ == operand.server_type_)
      {
        for (int32_t i = 0; i < table_stats_.get_array_index(); i++)
        {
          ObStat *stat = table_stats_.at(i);
          ObStat *operand_stat = NULL;
          int has_table = operand.get_stat(stat->get_table_id(), operand_stat);
          int64_t op_value = 0;
          // if operand has status value of same table, then apply op, otherwise keep old values.
          if (OB_SUCCESS == has_table && NULL != operand_stat)
          {
            for (int32_t index = 0; index < ObStat::MAX_STATICS_PER_TABLE; ++index)
            {
              op_value = op(stat->get_value(index) , operand_stat->get_value(index));
              stat->set_value(index, op_value);
            }
          }
        }
      }
      return *this;
    }

    int ObStatManager::get_stat(const uint64_t table_id, ObStat* &stat) const
    {
      int ret = OB_ENTRY_NOT_EXIST;
      stat = NULL;
      for (int32_t i = 0; table_id != OB_INVALID_ID && i < table_stats_.get_array_index(); i++)
      {
        if (table_stats_.at(i)->get_table_id() == table_id)
        {
          stat = table_stats_.at(i);
          ret = OB_SUCCESS;
        }
      }
      return ret;
    }

  }
}
