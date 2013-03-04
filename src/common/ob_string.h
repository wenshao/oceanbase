#ifndef OCEANBASE_COMMON_OB_STRING_H_
#define OCEANBASE_COMMON_OB_STRING_H_

#include "serialization.h"
#include "ob_define.h"
#include "data_buffer.h"
#include "murmur_hash.h"
#include <algorithm>
#include <iostream>
namespace oceanbase
{
  namespace common
  {
    /**
     * ObString do not own the buffer's memory
     * @ptr_ : buffer pointer, allocated by user.
     * @buffer_size_ : buffer's capacity
     * @data_length_ : actual data length of %ptr_
     */
    class ObString
    {
      public:
        typedef int32_t obstr_size_t;
      public:
        ObString()
          : buffer_size_(0), data_length_(0), ptr_(NULL)
        {
        }
        void reset()
        {
          buffer_size_ = 0;
          data_length_ = 0;
          ptr_ = NULL;
        }
        INLINE_NEED_SERIALIZE_AND_DESERIALIZE;

        /*
        ObString(obstr_size_t size, char* ptr)
          : size_(size), length_(0), ptr_(ptr)
        {
          assert(length_ <= size_);
        }
        */

        /*
         * attach the buff start from ptr, capacity is size, data length is length
         * 用一块以ptr标识的, 大小为 size, 其中已经存在了 length 长度数据 的buf 初始化
         */

        ObString(const obstr_size_t size, const obstr_size_t length, char* ptr)
          : buffer_size_(size), data_length_(length), ptr_(ptr)
        {
          if (ptr_ == NULL)
          {
            buffer_size_ = 0;
            data_length_ = 0;
          }
        }
        /*
         * attache myself to buf, and then copy rv's data to myself.
         * 把rv中的obstring拷贝到buf中, 并且把自己和该buf建立联系
         *
         */

        inline int clone(const ObString& rv, ObDataBuffer& buf)
        {
          int ret = OB_ERROR;
          assign_buffer(buf.get_data(), static_cast<obstr_size_t>(buf.get_remain()));
          obstr_size_t writed_length = write(rv.ptr(), rv.length());
          if (writed_length == rv.length())
          {
            ret = OB_SUCCESS;
            buf.get_position() += writed_length;
          }
          return ret;
        }

        // not virtual, i'm not a base class.
        ~ObString()
        {
        }

        // ObString 's copy constructor && assignment, use default, copy every member.
        // ObString(const ObString & obstr);
        // ObString & operator=(const ObString& obstr);

        /*
         * write a stream to my buffer,
         *
         * 向自己的buf里写入一段字节流
         * 返回值是成功写入的字节数
         *
         */

        inline obstr_size_t write(const char* bytes, const obstr_size_t length)
        {
          obstr_size_t writed = 0;
          if (!bytes || length <= 0 )
          {
            ;
          }
          else
          {
            if (data_length_ + length <= buffer_size_)
            {
              memcpy(ptr_ + data_length_, bytes, length);
              data_length_ += length;
              writed = length;
            }
          }
          return writed;
        }

        /*
         * DO NOT USE THIS ANY MORE
         *
         * 请不要使用这个接口, 后期可能废弃
         */

        inline void assign(char* bytes, const obstr_size_t length)
        {
          ptr_ = bytes;
          buffer_size_ = length;
          data_length_ = length;
          if (ptr_ == NULL)
          {
            buffer_size_ = 0;
            data_length_ = 0;
          }
        }

        /*
         * attach myself to other's buff, so you can read through me, but not write
         * 把ob_string 关联到一块不属于自己的内存空间上, 该 obstring 可读,不可写
         */

        inline void assign_ptr(char* bytes, const obstr_size_t length)
        {
          ptr_ = bytes;
          buffer_size_ = 0;   //this means I do not hold the buff, just a ptr
          data_length_ = length;
          if (ptr_ == NULL)
          {
            data_length_ = 0;
          }
        }

        /*
         * attach myself to a buffer, whoes capacity is size
         * 把自己与一片buffer 关联起来
         */

        inline void assign_buffer(char* buffer, const obstr_size_t size)
        {
          ptr_ = buffer;
          buffer_size_ = size;  //this means I hold the buffer, so you can do write
          data_length_ = 0;
          if (ptr_ == NULL)
          {
            buffer_size_ = 0;
            data_length_ = 0;
          }

        }

        /*
         * the remain size of my buffer
         * 当自己hold buffer的时候, 是buffer中还没使用的长度
         * 当仅仅是保存一个指针的时候, 返回0
         */

        inline obstr_size_t remain() const
        {
          return buffer_size_ > 0 ? (buffer_size_ - data_length_): buffer_size_;
        }

        inline obstr_size_t length() const { return data_length_; }
        inline obstr_size_t size() const { return buffer_size_; }
        inline const char* ptr() const { return ptr_; }
        inline char* ptr() { return ptr_; }

        inline int64_t hash() const
        {
          int64_t hash_val = 0;

          if (NULL != ptr_ && data_length_ > 0)
          {
            hash_val = murmurhash2(ptr_, data_length_, 0);
          }

          return hash_val;
        };

        inline int32_t compare(const ObString& obstr) const
        {
          int32_t ret = 0;
          if (ptr_ == obstr.ptr_)
          {
            ret = data_length_ - obstr.data_length_;
          }
          else if ( 0 == (ret = ::memcmp(ptr_,obstr.ptr_,std::min(data_length_,obstr.data_length_)) ))
          {
            ret = data_length_ - obstr.data_length_;
          }
          return ret;
        }

        inline int32_t compare(const char* str) const
        {
          int32_t len = 0;
          if (str != NULL)
          {
            len = static_cast<int32_t>(strlen(str));
          }
          char * p = (char*)str;
          ObString rv(0, len, p);
          return compare(rv);
        }

        inline obstr_size_t shrink()
        {
          obstr_size_t rem  = remain();
          if (buffer_size_ > 0) {
            buffer_size_ = data_length_;
          }
          return rem;
        }

        inline bool operator<(const ObString& obstr) const
        {
          return compare(obstr) < 0;
        }

        inline bool operator<=(const ObString& obstr) const
        {
          return compare(obstr) <= 0;
        }

        inline bool operator>(const ObString& obstr) const
        {
          return compare(obstr) > 0;
        }

        inline bool operator>=(const ObString& obstr) const
        {
          return compare(obstr) >= 0;
        }

        inline bool operator==(const ObString& obstr) const
        {
          return compare(obstr) == 0;
        }
        inline bool operator!=(const ObString& obstr) const
        {
          return compare(obstr) != 0;
        }
        friend std::ostream & operator<<(std::ostream &os, const ObString& str); // for google test
      private:
        obstr_size_t buffer_size_;
        obstr_size_t data_length_;
        char* ptr_;
    };

    DEFINE_SERIALIZE(ObString)
    {
      int res = OB_SUCCESS;
      int64_t serialize_size = get_serialize_size();
      if (buf == NULL || serialize_size > buf_len - pos)
      {
        res = OB_ERROR;
      }

      if (res == OB_SUCCESS)
      {
        //Null ObString is allowed
        res = serialization::encode_vstr(buf, buf_len, pos,ptr_, data_length_);
      }
      return res;
    }

    DEFINE_DESERIALIZE(ObString)
    {
      int res = OB_SUCCESS;
      int64_t len = 0;
      if ( NULL == buf || (data_len - pos) < 2)  //at least need two bytes
      {
        TBSYS_LOG(WARN, "check buf failed:ptr[%p], len[%ld], pos[%ld]", buf, data_len, pos);
        res = OB_ERROR;
      }
      if (OB_SUCCESS == res)
      {
        if (0 == buffer_size_)
        {
          ptr_ = const_cast<char*>(serialization::decode_vstr(buf,data_len,pos,&len));
          if (NULL == ptr_)
          {
            TBSYS_LOG(WARN, "check decode ptr failed");
            res = OB_ERROR;
          }
        }
        else
        {
          //copy to ptr_
          int64_t str_len = serialization::decoded_length_vstr(buf,data_len,pos);
          if (str_len < 0 || buffer_size_ < str_len || (data_len - pos) < str_len)
          {
            TBSYS_LOG(WARN, "check string len failed:str_len[%ld], buf_size[%d], data_len[%ld], pos[%ld]",
                str_len, buffer_size_, data_len, pos);
            res = OB_ERROR;
          }
          else if (NULL == serialization::decode_vstr(buf,data_len,pos,ptr_,buffer_size_,&len))
          {
            TBSYS_LOG(WARN, "decode to inner buffer failed");
            res = OB_ERROR;
          }
        }
        data_length_ = static_cast<obstr_size_t>(len);
      }
      return res;
    }

    DEFINE_GET_SERIALIZE_SIZE(ObString)
    {
      return serialization::encoded_length_vstr(data_length_);
    }

    inline std::ostream & operator<<(std::ostream &os, const ObString& str) // for google test
    {
      os << "size=" << str.buffer_size_ << " len=" << str.data_length_;
      return os;
    }
  }
}
#endif
