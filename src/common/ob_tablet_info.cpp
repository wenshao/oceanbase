/*
 *   (C) 2007-2010 Taobao Inc.
 *   
 *         
 *   Version: 0.1
 *           
 *   Authors:
 *      qushan <qushan@taobao.com>
 *        - some work details if you want
 *               
 */
#include "tbsys.h"
#include "ob_tablet_info.h"

namespace oceanbase 
{ 
  namespace common 
  {

    // --------------------------------------------------------
    // class ObTabletLocation implements
    // --------------------------------------------------------

    void ObTabletLocation::dump(const ObTabletLocation & location)
    {
      const int32_t MAX_SERVER_ADDR_SIZE = 128;
      char server_addr[MAX_SERVER_ADDR_SIZE];
      location.chunkserver_.to_string(server_addr, MAX_SERVER_ADDR_SIZE);
      TBSYS_LOG(INFO,"tablet_version :%ld, tablet_seq: %ld, location:%s\n", 
          location.tablet_version_, location.tablet_seq_, server_addr);
    }

    DEFINE_SERIALIZE(ObTabletLocation)
    {
      int ret = OB_ERROR;
      ret = serialization::encode_vi64(buf, buf_len, pos, static_cast<int64_t>(tablet_version_));

      if (OB_SUCCESS == ret)
      {
        ret = serialization::encode_vi64(buf, buf_len, pos, 
          static_cast<int64_t>(tablet_seq_));
      }

      if (ret == OB_SUCCESS)
        ret = chunkserver_.serialize(buf, buf_len, pos);

      return ret;
    }

    DEFINE_DESERIALIZE(ObTabletLocation)
    {
      int ret = OB_ERROR;
      ret = serialization::decode_vi64(buf, data_len, pos, (int64_t*)&tablet_version_);

      if (OB_SUCCESS == ret)
      {
        ret = serialization::decode_vi64(buf, data_len, pos, 
          (int64_t*)&tablet_seq_);
      }
      if (ret == OB_SUCCESS)
        ret = chunkserver_.deserialize(buf, data_len, pos);

      return ret;
    }

    DEFINE_GET_SERIALIZE_SIZE(ObTabletLocation)
    {
      int64_t total_size = 0;
      total_size += serialization::encoded_length_vi64(tablet_version_);
      total_size += serialization::encoded_length_vi64(tablet_seq_);
      total_size += chunkserver_.get_serialize_size();
      return total_size;
    }
    
    DEFINE_SERIALIZE(ObTabletInfo)
    {
      int ret = OB_ERROR;
      ret = serialization::encode_vi64(buf, buf_len, pos, row_count_);

      if (ret == OB_SUCCESS)
        ret = serialization::encode_vi64(buf, buf_len, pos, occupy_size_);

      if (ret == OB_SUCCESS)
        ret = serialization::encode_vi64(buf, buf_len, pos, crc_sum_);

      if (ret == OB_SUCCESS)
        ret = range_.serialize(buf, buf_len, pos);

      return ret;
    }

    DEFINE_DESERIALIZE(ObTabletInfo)
    {
      int ret = OB_ERROR;
      ret = serialization::decode_vi64(buf, data_len, pos, &row_count_);

      if (ret == OB_SUCCESS)
        ret = serialization::decode_vi64(buf, data_len, pos, &occupy_size_);

      if (ret == OB_SUCCESS)
        ret = serialization::decode_vi64(buf, data_len, pos, reinterpret_cast<int64_t *>(&crc_sum_));

      if (ret == OB_SUCCESS)
        ret = range_.deserialize(buf, data_len, pos);

      return ret;
    }

    DEFINE_GET_SERIALIZE_SIZE(ObTabletInfo)
    {
      int64_t total_size = 0;

      total_size += serialization::encoded_length_vi64(row_count_);
      total_size += serialization::encoded_length_vi64(occupy_size_);
      total_size += serialization::encoded_length_vi64(crc_sum_);
      total_size += range_.get_serialize_size();

      return total_size;
    }

    int ObTabletInfo::deep_copy(CharArena &allocator, const ObTabletInfo &other, bool new_start_key, bool new_end_key)
    {
      int ret = OB_SUCCESS;
      
      this->row_count_ = other.row_count_;
      this->occupy_size_ = other.occupy_size_;
      this->crc_sum_ = other.crc_sum_;
      this->range_.table_id_ = other.range_.table_id_;
      this->range_.border_flag_ = other.range_.border_flag_;


      if (new_start_key) 
      {
        common::ObString::obstr_size_t sk_len = other.range_.start_key_.length();
        char* sk = reinterpret_cast<char*>(allocator.alloc(sk_len));
        if (NULL == sk)
        {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          TBSYS_LOG(ERROR, "no memory");
        }
        else
        {
          memcpy(sk, other.range_.start_key_.ptr(), sk_len);
          this->range_.start_key_.assign(sk, sk_len);
        }
      }
      else
      {
        this->range_.start_key_ = other.range_.start_key_;
      }

      if (new_end_key)
      {
        common::ObString::obstr_size_t ek_len = other.range_.end_key_.length();
        char* ek = reinterpret_cast<char*>(allocator.alloc(ek_len));
        if (NULL == ek)
        {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          TBSYS_LOG(ERROR, "no memory");
        }
        else
        {
          memcpy(ek, other.range_.end_key_.ptr(), ek_len);
          this->range_.end_key_.assign(ek, ek_len);
        }
      }
      else
      {
        this->range_.end_key_ = other.range_.end_key_;
      }
      return ret;
    }
    
    // ObTabletReportInfo
    DEFINE_SERIALIZE(ObTabletReportInfo)
    {
      int ret = OB_SUCCESS;
      ret = tablet_info_.serialize(buf, buf_len, pos);
      if (ret == OB_SUCCESS)
        ret = tablet_location_.serialize(buf, buf_len, pos);
      return ret;
    }

    DEFINE_DESERIALIZE(ObTabletReportInfo)
    {
      int ret = OB_SUCCESS;
      ret = tablet_info_.deserialize(buf, data_len, pos);
      if (ret == OB_SUCCESS)
        ret = tablet_location_.deserialize(buf, data_len, pos);
      return ret;
    }

    DEFINE_GET_SERIALIZE_SIZE(ObTabletReportInfo)
    {
      int64_t total_size = 0;

      total_size += tablet_info_.get_serialize_size();
      total_size += tablet_location_.get_serialize_size();

      return total_size;
    }

    bool ObTabletReportInfo::operator== (const ObTabletReportInfo &other) const
    {
      return tablet_info_.equal(other.tablet_info_)
        && tablet_location_.tablet_version_ == other.tablet_location_.tablet_version_
        && tablet_location_.chunkserver_ == other.tablet_location_.chunkserver_;
    }
    
    // ObTabletReportInfoList
    DEFINE_SERIALIZE(ObTabletReportInfoList)
    {
      int ret = OB_ERROR;
      
      int64_t size = tablet_list_.get_array_index();
      ret = serialization::encode_vi64(buf, buf_len, pos, size);

      if (ret == OB_SUCCESS)
      {
        for (int64_t i = 0; i < size; ++i)
        {
          ret = tablets_[i].serialize(buf, buf_len, pos);
          if (ret != OB_SUCCESS)
            break;
        }
      }

      return ret;
    }

    DEFINE_DESERIALIZE(ObTabletReportInfoList)
    {
      int ret = OB_ERROR;

      int64_t size = 0;
      ret = serialization::decode_vi64(buf, data_len, pos, &size);

      if (ret == OB_SUCCESS && size > 0)
      {
        for (int64_t i=0; i<size; ++i)
        {
          ObTabletReportInfo tablet;
          ret = tablet.deserialize(buf, data_len, pos);
          if (ret != OB_SUCCESS)
            break;

          tablet_list_.push_back(tablet);
        }
      }

      return ret;
    }

    bool ObTabletReportInfoList::operator== (const ObTabletReportInfoList &other) const
    {
      bool ret = true;
      if (tablet_list_.get_array_index() != other.tablet_list_.get_array_index())
      {
        ret = false;
      }
      else
      {
        for (int i = 0; i < tablet_list_.get_array_index(); ++i)
        {
          if (!(tablets_[i] == other.tablets_[i]))
          {
            ret = false;
            break;
          }
        }
      }
      return ret;
    }
    
    DEFINE_GET_SERIALIZE_SIZE(ObTabletReportInfoList)
    {
      int64_t total_size = 0;
      
      int64_t size = tablet_list_.get_array_index();
      total_size += serialization::encoded_length_vi64(size);

      if (size > 0)
      {
        for (int64_t i=0; i<size; ++i)
          total_size += tablets_[i].get_serialize_size();
      }

      return total_size;
    }


    // ObTabletInfoList
    DEFINE_SERIALIZE(ObTabletInfoList)
    {
      int ret = OB_ERROR;
      
      int64_t size = tablet_list.get_array_index();
      ret = serialization::encode_vi64(buf, buf_len, pos, size);

      if (ret == OB_SUCCESS)
      {
        for (int64_t i=0; i<size; ++i)
        {
          ret = tablets[i].serialize(buf, buf_len, pos);
          if (ret != OB_SUCCESS)
            break;
        }
      }

      return ret;
    }

    DEFINE_DESERIALIZE(ObTabletInfoList)
    {
      int ret = OB_ERROR;

      int64_t size = 0;
      ret = serialization::decode_vi64(buf, data_len, pos, &size);

      if (ret == OB_SUCCESS && size > 0)
      {
        ObTabletInfo tablet;
        for (int64_t i=0; i<size; ++i)
        {
          ret = tablet.deserialize(buf, data_len, pos);
          if (ret != OB_SUCCESS)
            break;

          tablet_list.push_back(tablet);
        }
      }

      return ret;
    }

    DEFINE_GET_SERIALIZE_SIZE(ObTabletInfoList)
    {
      int64_t total_size = 0;
      
      int64_t size = tablet_list.get_array_index();
      total_size += serialization::encoded_length_vi64(size);

      if (size > 0)
      {
        for (int64_t i=0; i<size; ++i)
          total_size += tablets[i].get_serialize_size();
      }

      return total_size;
    }

  } // end namespace common
} // end namespace oceanbase

