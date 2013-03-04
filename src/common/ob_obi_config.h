/*
 * Copyright (C) 2007-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * Description here
 *
 * Version: $Id$
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *     - some work details here
 */
#ifndef _OB_OBI_CONFIG_H
#define _OB_OBI_CONFIG_H 1
#include "common/ob_define.h"
#include "common/ob_server.h"
#include <stdint.h>

namespace oceanbase
{
  namespace common
  {
/**
 * configuration for an oceanbase instance
 * 
 */
    class ObiConfig
    {
      public:
        ObiConfig();
        virtual ~ObiConfig();
        int32_t get_read_percentage() const;
        void set_read_percentage(int32_t read_percentage);
        void set_flag_rand_ms();
        void unset_flag_rand_ms();
        bool is_rand_ms() const;
        NEED_SERIALIZE_AND_DESERIALIZE;
      protected:
        static const int32_t FLAG_RAND_MS = 1;
        // data members
        int32_t read_percentage_;     ///< the percentage of this instance for read(get/scan) operations 
        int32_t flag_;
        int64_t reserve2_;
    };

    inline ObiConfig::ObiConfig()
      :read_percentage_(0), flag_(0), reserve2_(0)
    {
    }

    inline ObiConfig::~ObiConfig()
    {
    }

    inline int32_t ObiConfig::get_read_percentage() const
    {
      return read_percentage_;
    }

    inline void ObiConfig::set_read_percentage(int32_t read_percentage)
    {
      read_percentage_ = read_percentage;
    }
    
    inline void ObiConfig::set_flag_rand_ms()
    {
      flag_ |= FLAG_RAND_MS;
    }

    inline void ObiConfig::unset_flag_rand_ms()
    {
      flag_ &= ~FLAG_RAND_MS;
    }

    inline bool ObiConfig::is_rand_ms() const
    {
      return (flag_ & FLAG_RAND_MS) == FLAG_RAND_MS;
    }

    class ObiConfigEx: public ObiConfig
    {
      public:
        ObiConfigEx();
        virtual ~ObiConfigEx();
        void set_rs_addr(const ObServer& rs_addr);
        const ObServer& get_rs_addr() const;
        NEED_SERIALIZE_AND_DESERIALIZE;
        void print() const;
        void print(char* buf, const int64_t buf_len, int64_t &pos) const;        
      private:
        // data members
        ObServer rs_addr_;
        int64_t reserve_;
    };

    inline ObiConfigEx::ObiConfigEx()
      :reserve_(0)
    {
    }

    inline ObiConfigEx::~ObiConfigEx()
    {
    }

    inline void ObiConfigEx::set_rs_addr(const ObServer& rs_addr)
    {
      rs_addr_ = rs_addr;
    }

    inline const ObServer& ObiConfigEx::get_rs_addr() const
    {
      return rs_addr_;
    }


    struct ObiConfigList
    {
      // data members
      static const int MAX_OBI_COUNT = 5;
      ObiConfigEx conf_array_[MAX_OBI_COUNT];
      int32_t obi_count_;
      int32_t reserve_;

      ObiConfigList();
      NEED_SERIALIZE_AND_DESERIALIZE;
      void print() const;
      void print(char* buf, const int64_t buf_len, int64_t &pos) const;
    };
    inline ObiConfigList::ObiConfigList()
      :obi_count_(0), reserve_(0)
    {
    }
  
  } // end namespace common
} // end namespace oceanbase

#endif /* _OB_OBI_CONFIG_H */

