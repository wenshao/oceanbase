/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * bloom_filter.h for bloom filter. 
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */
#ifndef OCEANBASE_COMMON_BASIC_BLOOM_FILTER_H_
#define OCEANBASE_COMMON_BASIC_BLOOM_FILTER_H_

#include <limits.h>
#include <cmath>
#include <iostream>
#include <string.h>
#include "tblog.h"
#include "murmur_hash.h"
#include "ob_define.h"
#include "ob_string.h"
#include "ob_malloc.h"

namespace oceanbase
{ 
  namespace common
  { 
   
    /**
     * A space-efficent probabilistic set for membership test, false postives
     * are possible, but false negatives are not.
     */
    static const float BLOOM_FILTER_FALSE_POSITIVE_PROB = static_cast<float>(0.1);
    
    template <class Hasher = common::MurmurHash2>
    class BasicBloomFilter
    {
      
    public:
//      BasicBloomFilter(int64_t element_count, float false_positive_prob = BLOOM_FILTER_FALSE_POSITIVE_PROB)
//      {
//        element_count_ = element_count;
//        assert(element_count > 0);
//        if (false_positive_prob >= 1.0 || false_positive_prob < 0.0)
//        { 
//          TBSYS_LOG(ERROR, "bloom filter false_positive_prob should be < 1.0 and > 0.0");
//          return;
//        }
//        //assert(0.0< false_positive_prob < 1.0);
//        false_positive_prob_ = false_positive_prob;
//        double num_hashes = -std::log(false_positive_prob_) / std::log(2);
//        num_hash_functions_ = (int64_t)num_hashes;
//        num_bits_ = (int64_t)(element_count_ * num_hashes / std::log(2));
//        assert(num_bits_ != 0);
//        num_bytes_ = (num_bits_ / CHAR_BIT) + (num_bits_ % CHAR_BIT ? 1 : 0);
//        bloom_bits_ = new uint8_t[num_bytes_];
//    
//        memset(bloom_bits_, 0, num_bytes_);
//        
//        TBSYS_LOG(DEBUG, "num funcs= %l, num bits=%l, num bytes= %l,bits per element=%f",
//             num_hash_functions_, num_bits_, num_bytes_, double(num_bits_) / element_count_);
//          
//      }
      
      int init(int64_t element_count, float false_positive_prob = BLOOM_FILTER_FALSE_POSITIVE_PROB)
      {
        int ret = common::OB_SUCCESS;
        if (element_count > 0)
        {
          element_count_ = element_count; 
        }
        else
        { 
          TBSYS_LOG(ERROR, "bloom filter element_count should be > 0 ");
          ret = common::OB_ERROR;
        }
        
        if (common::OB_SUCCESS == ret && (false_positive_prob < 1.0 || false_positive_prob > 0.0))
        { 
           false_positive_prob_ = false_positive_prob;
        }
        else
        {
          TBSYS_LOG(ERROR, "bloom filter false_positive_prob should be < 1.0 and > 0.0");
          ret = common::OB_ERROR;
        }
        if(ret == common::OB_SUCCESS)
        {
          double num_hashes = -std::log(false_positive_prob_) / std::log(2);
          num_hash_functions_ = (int64_t)num_hashes;
          num_bits_ = (int64_t)(static_cast<double>(element_count_) * static_cast<double>(num_hashes) / static_cast<double>(std::log(2)));
          //assert(num_bits_ != 0);
          int64_t temp = num_bytes_;
          num_bytes_ = (num_bits_ / CHAR_BIT) + (num_bits_ % CHAR_BIT ? 1 : 0);
          if (NULL != bloom_bits_)
          { 
            if ((temp != num_bytes_))
            {
              ob_free(bloom_bits_);
              bloom_bits_ = static_cast<uint8_t *>(ob_malloc(num_bytes_));
           }
          }
          else
          {
            bloom_bits_ = static_cast<uint8_t *>(ob_malloc(num_bytes_));
          }
          
          if (NULL == bloom_bits_) 
          {
            ret = common::OB_ERROR;
          }
          else
          {
            memset(bloom_bits_, 0, num_bytes_);
          }
          
          TBSYS_LOG(DEBUG, "num funcs= %ld, num bits=%ld, num bytes= %ld,bits per element=%f",
               num_hash_functions_, num_bits_, num_bytes_, 
               static_cast<double>(num_bits_) / static_cast<double>(element_count_));
        }
        return ret;  
      }
      
      BasicBloomFilter()
      { 
        bloom_bits_ = NULL;
        element_count_ = 0;
        num_hash_functions_ = 0;
        num_bits_ = 0;
        num_bytes_ = 0;
        false_positive_prob_ = 0.0;
      }
      
      virtual ~BasicBloomFilter()
      {
       if(bloom_bits_) 
       {
        ob_free(bloom_bits_);
       }
        bloom_bits_ = NULL;
      }
      
      //hash the key and make the bits set to 1
      void insert(const void *key, const int64_t len)
      { 
        if (!key || len < 0)
        {
         //todo: error log, finish after the log entry define
         return;
        }
        uint32_t hash = static_cast<uint32_t>(len);
    
        for (uint32_t i = 0; i < num_hash_functions_; ++i)
        {
          hash = static_cast<uint32_t>(hasher_(key, static_cast<int32_t>(len), hash) % num_bits_);
          bloom_bits_[hash / CHAR_BIT] = static_cast<unsigned char>(bloom_bits_[hash / CHAR_BIT] | (1 << (hash % CHAR_BIT)));
        }
      }
    
      void insert(const ObString &ob_string)
      {
        insert(ob_string.ptr(), ob_string.length());
      }
      
      //test if contain the key
      bool may_contain(const void *key, const int64_t len) const
      { 
        if (!key || len < 0)
        {
         //todo: error log
         return false;
        }
        uint32_t hash = static_cast<uint32_t>(len);
        uint8_t byte_mask = 0;
        uint8_t byte = 0;
    
        for (uint32_t i = 0; i < num_hash_functions_; ++i)
        {
          hash = static_cast<uint32_t>(hasher_(key, static_cast<int32_t>(len), hash) % num_bits_);
          byte = bloom_bits_[hash / CHAR_BIT];
          byte_mask = static_cast<int8_t>((1 << (hash % CHAR_BIT)));
    
          if ( (byte & byte_mask) == 0 ) {
            return false;
          }
        }
        return true;
      }
    
      bool may_contain(const ObString &ob_string) const
      {
        return may_contain(ob_string.ptr(), ob_string.length());
      }
    
      uint8_t* get_bitmap() const
      {
        return bloom_bits_;
      }
    
      int64_t get_num_bytes() const
      {
        return num_bytes_;
      }
      
      int64_t get_num_hash_functions()
      { 
        return num_hash_functions_;
      }
      
      int set_num_hash_functions(const int64_t num)
      { 
        int ret = common::OB_SUCCESS;
        if (num < 0)
        {
          ret = common::OB_ERROR;
        }
        else 
        {
          num_hash_functions_ = num;
        }
        return ret;
      }
      
      int set_bitmap(const char *bitmap, const int64_t len)
      { 
        int ret = common::OB_SUCCESS;
        if(len < 0)
        {
          ret = common::OB_ERROR;
        }
        else
        {
          if (NULL != bitmap)
          { 
            if (NULL != bloom_bits_)
            {
              ob_free(bloom_bits_);
              bloom_bits_ = NULL;
            }
            num_bytes_ = len;
            bloom_bits_ = static_cast<uint8_t *>(ob_malloc(len));
            if(NULL == bloom_bits_)
            {
              ret = common::OB_ERROR;
            } 
            else
            {
              memcpy(bloom_bits_, bitmap, len);
            }
          }
          else
          {
            ret = common::OB_ERROR;
          }
        }
        return ret;
      }
     
      //serialize the bit set,now have not implement,should first implement serialization 
      int serialize(char* buf, const int64_t buf_len, int64_t& pos) const
      { 
        
        int ret = OB_SUCCESS;
        if (num_bytes_ + pos  <=  buf_len ) 
        { 
          memcpy(buf,  bloom_bits_, num_bytes_);
          pos += num_bytes_;  
        }
        else
        {
          ret = OB_ERROR;
        }
        return ret;
      }
       
      bool reset()
      { 
        bool ret = true;
        if (NULL == bloom_bits_ || 0 >= num_bytes_)
        {
          ret = false;
        }
        else
        {
          memset(bloom_bits_, 0, num_bytes_);
        }
        return ret;
      }
      
    private:
      DISALLOW_COPY_AND_ASSIGN(BasicBloomFilter);
      Hasher     hasher_;
      int64_t    element_count_;
      int64_t    num_hash_functions_;
      int64_t    num_bits_;
      int64_t    num_bytes_;
      float      false_positive_prob_;
      uint8_t    *bloom_bits_;
    };
   
    typedef BasicBloomFilter<> BloomFilter;
   
  }
}

#endif  //OCEANBASE_COMMON_BASIC_BLOOM_FILTER_H_
