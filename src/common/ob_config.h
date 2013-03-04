/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 * 
 * Version: $Id$
 *
 * ob_config.h
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@taobao.com>
 *
 */

#ifndef _OB_CONFIG_H
#define _OB_CONFIG_H

#include "common/ob_flag.h"
#include "common/ob_define.h"
#include "common/ob_array.h"

namespace oceanbase
{
  namespace common
  {
    class ObConfig
    {
    public:
      ObConfig();
      virtual ~ObConfig();
    public:
      int load(const char* filename);
      int reload();
      const char* get_config_filename() const;
      virtual void print() const;
          
    protected:
      virtual int load_flags(tbsys::CConfig & cconf);
      virtual int add_flags() = 0;

      template<typename T>
      inline int add_flag(ObFlag<T>& flag, ObArray<ObFlag<T>*>& flag_array_, const char* section_str, const char* flag_str, T default_value)
      {
        int ret = OB_SUCCESS;
        ret = flag.set_flag(section_str, flag_str, default_value);
        if(OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "set flag fail:ret[%d]", ret);
        }
        if(OB_SUCCESS == ret)
        {
          ret = flag_array_.push_back(&flag);
          if(OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "push back flag to flag array fail:ret[%d]", ret);
          }
        }
        return ret;
      }

      inline int add_flag(ObFlag<int>& flag, const char* section_str, const char* flag_str, int default_value)
      {
        return add_flag<int>(flag, int_flag_array_, section_str, flag_str, default_value);
      }

      inline int add_flag(ObFlag<int64_t>& flag, const char* section_str, const char* flag_str, int64_t default_value)
      {
        return add_flag<int64_t>(flag, int64_flag_array_, section_str, flag_str, default_value);
      }

      inline int add_flag(ObFlag<const char*>& flag, const char* section_str, const char* flag_str, const char* default_value)
      {
        return add_flag<const char*>(flag, const_char_flag_array_, section_str, flag_str, default_value);
      }

    protected:
      char config_filename_[common::OB_MAX_FILE_NAME_LENGTH];
      ObArray<ObFlag<int>*> int_flag_array_;
      ObArray<ObFlag<int64_t>*> int64_flag_array_;
      ObArray<ObFlag<const char*>*> const_char_flag_array_;
      static const char* SECTION_STR_RS;
      static const char* SECTION_STR_UPS;
      static const char* SECTION_STR_SCHEMA;
      static const char* SECTION_STR_CS;
      static const char* SECTION_STR_MS;
      static const char* SECTION_STR_CLIENT;
      static const char* SECTION_STR_OBI;
      static const char* SECTION_STR_OBCONNECTOR;
    };
  }
}

#define ADD_FLAG(flag, section_str, flag_str, default_value) \
  if(OB_SUCCESS == ret) \
  { \
    ret = add_flag(flag, section_str, flag_str, default_value); \
    if(OB_SUCCESS != ret) \
    { \
      TBSYS_LOG(WARN, "add flag fail:ret[%d]", ret); \
    } \
  }

#endif /* _OB_CONFIG_H */


