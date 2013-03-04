/*
 * (C) 2007-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * ob_infix_expression.cpp is for what ...
 *
 * Version: $id: ob_infix_expression.cpp, v 0.1 7/29/2011 14:39 xiaochu Exp $
 *
 * Authors:
 *   xiaochu <xiaochu.yh@taobao.com>
 *
 */



#ifndef OCEANBASE_COMMON_OB_INFIX_EXPRESSION_H_
#define OCEANBASE_COMMON_OB_INFIX_EXPRESSION_H_
#include "ob_string.h"
#include "ob_string_search.h"
#include "ob_expression.h"
#include "ob_array_helper.h"

namespace oceanbase
{
  namespace common
  {
    class ObCellArray;
    class ObInfixExpression
    {
      public:
        /* @param expr:未经解析的中缀字符串表达式 */
        ObInfixExpression(const ObString &expr)
        {
          set_expression(expr);
        };

        ObInfixExpression(){};

        inline int set_expression(const ObString &expr)
        {
          expr_ = expr;
          return OB_SUCCESS;
        }

        inline ObString get_expression()const
        {
          return expr_;
        }

        ~ObInfixExpression(){};

        // serialization
        NEED_SERIALIZE_AND_DESERIALIZE;
      private:
        ObString expr_;
    }; // class ObInfixExpression
 } // namespace commom
}// namespace oceanbae

#endif

