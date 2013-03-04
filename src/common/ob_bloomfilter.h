////===================================================================
 //
 // ob_bloomfilter.h / common / Oceanbase
 //
 // Copyright (C) 2010 Taobao.com, Inc.
 //
 // Created on 2011-01-18 by Yubai (yubai.lk@taobao.com) 
 //
 // -------------------------------------------------------------------
 //
 // Description
 //
 //
 // -------------------------------------------------------------------
 // 
 // Change Log
 //
////====================================================================

#ifndef  OCEANBASE_COMMON_BLOOM_FILTER_H_
#define  OCEANBASE_COMMON_BLOOM_FILTER_H_
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <pthread.h>
#include <new>
#include <limits.h>
#include <algorithm>
#include "ob_define.h"
#include "ob_malloc.h"
#include "murmur_hash.h"

namespace oceanbase
{
  namespace common
  {
    template <class T, class HashFunc, class Alloc>
    class ObBloomFilter
    {
      struct BinData
      {
        static const int64_t CUR_VERSION = 1;
        const int64_t version;
        int64_t nhash;
        int64_t nbit;
        uint8_t bits[];
        BinData() : version(CUR_VERSION), nhash(0), nbit(0)
        {
        };
      };
      typedef ObBloomFilter<T, HashFunc, Alloc> bloom_filter_t;
      public:
        ObBloomFilter();
        ~ObBloomFilter();
        DISALLOW_COPY_AND_ASSIGN(ObBloomFilter);
      public:
        int init(const int64_t nhash, const int64_t nbit);
        void destroy();
        void clear();
        int deep_copy(const bloom_filter_t &other);
        int insert(const T &element);
        bool contain(const T &element) const;
        int serialize(char* buf, const int64_t buf_len, int64_t& pos) const;
        int deserialize(const char* buf, const int64_t data_len, int64_t& pos);
        int64_t get_serialize_size(void) const;
        static int64_t calc_nbyte(const int64_t nbit);
      private:
        mutable HashFunc hash_func_;
        Alloc alloc_;
        int64_t nhash_;
        int64_t nbit_;
        uint8_t *bits_;
    };

    class TableBloomFilter
    {
      struct DefaultAllocator
      {
        void *alloc(const int32_t nbyte) { return ob_malloc(nbyte); };
        void free(void *ptr) { ob_free(ptr); };
      };
      struct Elemnet
      {
        uint64_t table_id;
        ObString row_key;
      };
      struct HashFunc
      {
        int64_t operator () (const Elemnet &key, const int64_t hash) const
        {
          return (murmurhash2(key.row_key.ptr(), key.row_key.length(), static_cast<int32_t>(hash)) + key.table_id);
        };
      };
      public:
        TableBloomFilter() : bf_()
        {
        };
        ~TableBloomFilter()
        {
        };
        DISALLOW_COPY_AND_ASSIGN(TableBloomFilter);
      public:
        int init(const int64_t nhash, const int64_t nbyte)
        {
          return bf_.init(nhash, nbyte * CHAR_BIT);
        };
        void destroy()
        {
          bf_.destroy();
        };
        void clear()
        {
          bf_.clear();
        };
        int deep_copy(const TableBloomFilter &other)
        {
          return bf_.deep_copy(other.bf_);
        };
        int insert(const uint64_t table_id, const ObString &row_key)
        {
          Elemnet key;
          key.table_id = table_id;
          key.row_key = row_key;
          return bf_.insert(key);
        };
        bool contain(const uint64_t table_id, const ObString &row_key) const
        {
          Elemnet key;
          key.table_id = table_id;
          key.row_key = row_key;
          return bf_.contain(key);
        };
        int serialize(char* buf, const int64_t buf_len, int64_t& pos) const
        {
          return bf_.serialize(buf, buf_len, pos);
        };
        int deserialize(const char* buf, const int64_t data_len, int64_t& pos)
        {
          return bf_.deserialize(buf, data_len, pos);
        };
        int64_t get_serialize_size(void) const
        {
          return bf_.get_serialize_size();
        };
      private:
        ObBloomFilter<Elemnet, HashFunc, DefaultAllocator> bf_;
    };

    ////////////////////////////////////////////////////////////////////////////////////////////////////

    template <class T, class HashFunc, class Alloc>
    ObBloomFilter<T, HashFunc, Alloc>::ObBloomFilter() : alloc_(), nhash_(0), nbit_(0), bits_(NULL)
    {
    }

    template <class T, class HashFunc, class Alloc>
    ObBloomFilter<T, HashFunc, Alloc>::~ObBloomFilter()
    {
      destroy();
    }

    template <class T, class HashFunc, class Alloc>
    int ObBloomFilter<T, HashFunc, Alloc>::deep_copy(const bloom_filter_t &other)
    {
      int ret = OB_SUCCESS;
      if (OB_SUCCESS != (ret = init(other.nhash_, other.nbit_)))
      {
        TBSYS_LOG(WARN, "init fail ret=%d", ret);
      }
      else
      {
        memcpy(bits_, other.bits_, calc_nbyte(nbit_));
      }
      return ret;
    }

    template <class T, class HashFunc, class Alloc>
    int64_t ObBloomFilter<T, HashFunc, Alloc>::calc_nbyte(const int64_t nbit)
    {
      return (nbit / CHAR_BIT + (nbit % CHAR_BIT ? 1 : 0));
    }

    template <class T, class HashFunc, class Alloc>
    int ObBloomFilter<T, HashFunc, Alloc>::init(const int64_t nhash, const int64_t nbit)
    {
      int ret = OB_SUCCESS;
      if (0 >= nhash
          || 0 >= nbit)
      {
        ret = OB_INVALID_ARGUMENT;
        TBSYS_LOG(WARN, "invalid param nhash=%ld nbit=%ld", nhash, nbit);
      }
      else
      {
        destroy();
        const int64_t nbyte = calc_nbyte(nbit);
        bits_ = (uint8_t*)alloc_.alloc(static_cast<int32_t>(nbyte));
        if (NULL == bits_)
        {
          TBSYS_LOG(WARN, "bits_ null pointer, nbit_=%ld", nbit_);
          ret = OB_ERROR;
        }
        else
        {
          memset(bits_, 0, nbyte);
          nhash_ = nhash;
          nbit_ = nbit;
        }
      }
      return ret;
    }

    template <class T, class HashFunc, class Alloc>
    void ObBloomFilter<T, HashFunc, Alloc>::destroy()
    {
      if (NULL != bits_)
      {
        alloc_.free(bits_);
        bits_ = NULL;
        nhash_ = 0;
        nbit_ = 0;
      }
    }

    template <class T, class HashFunc, class Alloc>
    void ObBloomFilter<T, HashFunc, Alloc>::clear()
    {
      if (NULL != bits_)
      {
        memset(bits_, 0, calc_nbyte(nbit_));
      }
    }

    template <class T, class HashFunc, class Alloc>
    int ObBloomFilter<T, HashFunc, Alloc>::insert(const T &element)
    {
      int ret = OB_SUCCESS;
      if (NULL == bits_
          || 0 >= nbit_
          || 0 >= nhash_)
      {
        TBSYS_LOG(WARN, "maybe have not inited bits_=%p nbit_=%ld nhash_=%ld", bits_, nbit_, nhash_);
        ret = OB_ERROR;
      }
      else
      {
        int64_t hash = 0;
        for (int64_t i = 0; i < nhash_; ++i)
        {
          hash = hash_func_(element, hash) % nbit_;
          bits_[hash / CHAR_BIT] = static_cast<unsigned char>(bits_[hash / CHAR_BIT] | (1 << (hash % CHAR_BIT)));
        }
      }
      return ret;
    }

    template <class T, class HashFunc, class Alloc>
    bool ObBloomFilter<T, HashFunc, Alloc>::contain(const T &element) const
    {
      bool bret = true;
      if (NULL == bits_
          || 0 >= nbit_
          || 0 >= nhash_)
      {
        TBSYS_LOG(WARN, "maybe have not inited bits_=%p nbit_=%ld nhash_=%ld", bits_, nbit_, nhash_);
        bret = false;
      }
      else
      {
        uint32_t hash = 0;
        uint8_t byte_mask = 0;
        uint8_t byte = 0;
        for (int64_t i = 0; i < nhash_; ++i)
        {
          hash = static_cast<uint32_t>(hash_func_(element, hash) % nbit_);
          byte = bits_[hash / CHAR_BIT];
          byte_mask = static_cast<int8_t>(1 << (hash % CHAR_BIT));
          if (0 == (byte & byte_mask))
          {
            bret = false;
            break;
          }
        }
      }
      return bret;
    }

    template <class T, class HashFunc, class Alloc>
    int ObBloomFilter<T, HashFunc, Alloc>::serialize(char* buf, const int64_t buf_len, int64_t& pos) const
    {
      int ret = OB_SUCCESS;
      if (NULL == buf
              || (buf_len - pos) < get_serialize_size())
      {
        TBSYS_LOG(WARN, "invalid param buf=%p buf_len=%ld pos=%ld serialize_size=%ld",
                  buf, buf_len, pos, get_serialize_size());
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        if (NULL == bits_
            || 0 >= nbit_
            || 0 >= nhash_)
        {
          TBSYS_LOG(WARN, "maybe have not inited bits_=%p nbit_=%ld", bits_, nbit_);
        }
        BinData *bin_data = (BinData*)(buf + pos);
        bin_data->nhash = nhash_;
        bin_data->nbit = nbit_;
        memcpy(bin_data->bits, bits_, calc_nbyte(nbit_));
        pos += get_serialize_size();
      }
      return ret;
    }

    template <class T, class HashFunc, class Alloc>
    int ObBloomFilter<T, HashFunc, Alloc>::deserialize(const char* buf, const int64_t data_len, int64_t& pos)
    {
      int ret = OB_SUCCESS;
      if (NULL == buf
          || (data_len - pos) <= (int64_t)sizeof(BinData))
      {
        TBSYS_LOG(WARN, "invalid param buf=%p buf_len=%ld pos=%ld bin_data_size=%ld",
                  buf, data_len, pos, sizeof(BinData));
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        BinData *bin_data = (BinData*)(buf + pos);
        const int64_t nbyte = calc_nbyte(bin_data->nbit);
        if ((data_len - pos) < ((int64_t)sizeof(BinData) + nbyte))
        {
          TBSYS_LOG(WARN, "invalid param buf=%p buf_len=%ld pos=%ld total_data_size=%ld",
                    buf, data_len, pos, sizeof(BinData) + nbyte);
        }
        else
        {
          if (0 != nbyte
              && NULL == (bits_ = (uint8_t*)alloc_.alloc(static_cast<int32_t>(nbyte))))
          {
            TBSYS_LOG(WARN, "alloc bits_ fail nbyte=%ld", nbyte);
            ret = OB_ERROR;
          }
          else
          {
            memcpy(bits_, bin_data->bits, nbyte);
          }
          nhash_ = bin_data->nhash;
          nbit_ = bin_data->nbit;
          pos += get_serialize_size();
        }
      }
      return ret;
    }

    template <class T, class HashFunc, class Alloc>
    int64_t ObBloomFilter<T, HashFunc, Alloc>::get_serialize_size(void) const
    {
      return (sizeof(BinData) + calc_nbyte(nbit_));
    }
  }
}

#endif //OCEANBASE_COMMON_BLOOM_FILTER_H_

