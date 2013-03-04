/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.2: ob_update_condition.h,v 0.1 2011/11/29 10:06:35 xielun.szd Exp $
 *
 * Authors:
 *   chuanhui <rizhao.ych@taobao.com>
 *   2011.11.29 modify by xielun.szd for protocol extension can used by api and inner servers
 *
 */

#ifndef OCEANBASE_UPDATE_CONDITION_H_
#define OCEANBASE_UPDATE_CONDITION_H_

#include "ob_define.h"
#include "ob_string.h"
#include "ob_object.h"
#include "ob_common_param.h"
#include "ob_cond_info.h"
#include "ob_vector.h"
#include "ob_string_buf.h"
#include "ob_action_flag.h"

namespace oceanbase
{
  namespace common
  {
    class ObUpdateCondition
    {
      public:
        ObUpdateCondition();
        ~ObUpdateCondition();

        void reset();

      // outer protocol
      public:
        int add_cond(const ObString& table_name, const ObString& row_key, const ObString& column_name,
            const ObLogicOperator op_type, const ObObj& value);

        int add_cond(const ObString& table_name, const ObString& row_key, const bool is_exist);
      // inner protocol
      public:
        int add_cond(const uint64_t table_id, const ObString& row_key, const uint64_t column_id,
            const ObLogicOperator op_type, const ObObj& value);

        int add_cond(const uint64_t table_id, const ObString& row_key, const bool is_exist);

        // deep copy the cond content and add into conditions_
        int add_cond(const ObCondInfo & cond_info);
      public:
        int64_t get_count(void) const
        {
          return conditions_.size();
        }
        const ObCondInfo* operator[] (const int64_t index) const;
        ObCondInfo* operator[] (const int64_t index);

        NEED_SERIALIZE_AND_DESERIALIZE;
      private:
        ObVector<ObCondInfo> conditions_;
        ObStringBuf string_buf_;
    };
  }
}

#endif // OCEANBASE_UPDATE_CONDITION_H_

