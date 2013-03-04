/*
 * (C) 2007-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * ob_postfix_expression.h is for what ...
 *
 * Version: $id: ob_postfix_expression.h, v 0.1 7/29/2011 14:39 xiaochu Exp $
 *
 * Authors:
 *   xiaochu <xiaochu.yh@taobao.com>
 *     - 后缀表达式求值，可用于复合列等需要支持复杂求值的场合
 *
 */



#ifndef OCEANBASE_SQL_OB_POSTFIX_EXPRESSION_H_
#define OCEANBASE_SQL_OB_POSTFIX_EXPRESSION_H_
#include "ob_item_type.h"
#include "common/ob_string.h"
#include "common/ob_string_buf.h"
#include "common/ob_string_search.h"
#include "common/ob_array.h"
#include "common/ob_object.h"
#include "common/ob_result.h"
#include "common/ob_row.h"
#include "common/ob_expr_obj.h"
using namespace oceanbase::common;

namespace oceanbase
{
  namespace sql
  {
    struct ExprItem
    {
      struct SqlCellInfo{
        uint64_t tid;
        uint64_t cid;
      };

      ObItemType  type_;
      common::ObObjType data_type_;
      /* for:
               * 1. INTNUM
               * 2. BOOL
               * 3. DATE_VALUE
               * 4. query reference
               * 5. column reference
               * 6. expression reference
               * 7. num of operands
               */
      union{
        bool      bool_;
        int64_t   datetime_;
        int64_t   int_;
        double    double_;
        struct SqlCellInfo cell_;  // table_id, column_id
      }value_;
      // due to compile restriction, cant put string_ into union.
      // reason: ObString default constructor has parameters
      ObString  string_;        // const varchar obj or system function name
    };

    struct ObPostExprExtraParams
    {
      bool did_int_div_as_double_;
      int32_t operand_count_;
      ObPostExprExtraParams()
        :did_int_div_as_double_(false), operand_count_(0)
      {
      }
    };
    typedef int(*op_call_func_t)(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params);

    enum ObSqlSysFunc
    {
      SYS_FUNC_LENGTH = 0,
      SYS_FUNC_NUM
    };

    class ObPostfixExpression
    {
      public:
        ObPostfixExpression();
        ~ObPostfixExpression();
        ObPostfixExpression& operator=(const ObPostfixExpression &other);
        void set_int_div_as_double(bool did);

        int add_expr_item(const ExprItem &item);
        int add_expr_item_end();

        /* 获得表达式 */
        int get_expression(const oceanbase::common::ObObj *&expr, int &expr_size) const;

        /* 将org_cell中的值代入到expr计算结果 */
        int calc(const common::ObRow &row, const ObObj *&result);

        /*
         * 判断表达式类型：是否是const, column_index, etc
         * 如果表达式类型为column_index,则返回index值
         */
        int is_const_expr(bool &is_type) const;
        int is_column_index_expr(bool &is_type) const;
        bool is_empty() const;
        bool is_equijoin_cond(ExprItem::SqlCellInfo &c1, ExprItem::SqlCellInfo &c2) const;
        // print the postfix expression
        int64_t to_string(char* buf, const int64_t buf_len) const;

        NEED_SERIALIZE_AND_DESERIALIZE;
      private:
        enum {
          BEGIN_TYPE = 0,
          VECTOR,
          COLUMN_IDX,
          CONST_OBJ,
          OP,
          END, /* postfix expression terminator */
          END_TYPE
        };
      private:
        ObPostfixExpression(const ObPostfixExpression &other);
        static inline int nop_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params);
        static inline int reserved_func(const ObExprObj &obj1, const ObExprObj &obj2, ObExprObj &result);
        /* compare function list:
         * >   gt_func
         * >=  ge_func
         * <=  le_func
         * <   lt_func
         * ==  eq_func
         * !=  neq_func
         */
        static inline int do_gt_func(const ObExprObj &obj1, const ObExprObj &obj2, ObExprObj &result);
        static inline int gt_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params);
        static inline int do_ge_func(const ObExprObj &obj1, const ObExprObj &obj2, ObExprObj &result);
        static inline int ge_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params);
        static inline int do_lt_func(const ObExprObj &obj1, const ObExprObj &obj2, ObExprObj &result);

        static inline int do_le_func(const ObExprObj &obj1, const ObExprObj &obj2, ObExprObj &result);
        static inline int le_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params);
        static inline int lt_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params);
        static inline int do_eq_func(const ObExprObj &obj1, const ObExprObj &obj2, ObExprObj &result);
        static inline int eq_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params);
        static inline int do_neq_func(const ObExprObj &obj1, const ObExprObj &obj2, ObExprObj &result);
        static inline int neq_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params);
        static inline int do_is_not_func(const ObExprObj &obj1, const ObExprObj &obj2, ObExprObj &result);
        static inline int is_not_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params);
        static inline int do_is_func(const ObExprObj &obj1, const ObExprObj &obj2, ObExprObj &result);
        static inline int is_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params);
        static inline int add_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params);
        static inline int sub_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params);
        static inline int mul_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params);
        static inline int div_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params);
        static inline int mod_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params);
        static inline int and_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params);
        static inline int or_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params);
        static inline int minus_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params);
        static inline int plus_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params);
        static inline int not_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params);
        static inline int like_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params);
        static inline int in_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params);
        static inline int not_like_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params);
        static inline int btw_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params);
        static inline int not_btw_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params);
        static inline int sys_func_length(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params);

        // 辅助函数，检查表达式是否表示const或者column index
        int check_expr_type(const int64_t type_val, bool &is_type, const int64_t stack_len) const;
        int get_sys_func(const common::ObString &sys_func, ObSqlSysFunc &func_type) const;
      private:
        static const int64_t DEF_STRING_BUF_SIZE = 64 * 1024L;
        static const int64_t OB_MAX_COMPOSITE_SYMBOL_COUNT = 1024;
        static op_call_func_t call_func[T_MAX_OP - T_MIN_OP - 1];
        static op_call_func_t SYS_FUNCS_TAB[SYS_FUNC_NUM];
        static const char* const SYS_FUNCS_NAME[SYS_FUNC_NUM];
        ObObj expr_[OB_MAX_COMPOSITE_SYMBOL_COUNT];
        // ObExpressionParser parser_;
        // TODO: 修改成指针引用，减少数据拷贝。需要另外建立一个临时值缓冲区。
        ObExprObj stack_i[OB_MAX_COMPOSITE_SYMBOL_COUNT];
        bool did_int_div_as_double_;
        int postfix_size_;
        ObObj result_;
        ObStringBuf str_buf_;
    }; // class ObPostfixExpression

    inline void ObPostfixExpression::set_int_div_as_double(bool did)
    {
      did_int_div_as_double_ = did;
    }

    inline bool ObPostfixExpression::is_empty() const
    {
      int64_t type = 0;
      return 0 == postfix_size_
        || (1 == postfix_size_
            && common::OB_SUCCESS == expr_[0].get_int(type)
            && END == type);
    }

  } // namespace commom
}// namespace oceanbae

#endif
