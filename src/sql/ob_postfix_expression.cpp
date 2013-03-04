/*
 * (C) 2007-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * ob_postfix_expression.cpp is for what ...
 *
 * Version: $id: ob_postfix_expression.cpp, v 0.1 7/29/2011 14:39 xiaochu Exp $
 *
 * Authors:
 *   xiaochu <xiaochu.yh@taobao.com>
 *     - some work details if you want
 *
 * last update:
 * 2012/5/30 change name space oceanbase::common to oceanbase::sql
 *           modify calc interface to adapt ExprItem
 *
 */


#include "ob_postfix_expression.h"
#include "common/utility.h"
#include "sql/ob_item_type_str.h"
using namespace oceanbase::sql;

namespace oceanbase
{
  namespace sql
  {
    /*     初始化数学运算操作调用表 */
    op_call_func_t ObPostfixExpression::call_func[T_MAX_OP - T_MIN_OP - 1] = {
      /*   WARNING: 下面的顺序不可以调换，
       *   需要与ExprType enum定义对应
       */
      ObPostfixExpression::minus_func, /* T_OP_NEG */
      ObPostfixExpression::plus_func, /* T_OP_POS */
      ObPostfixExpression::add_func, /* T_OP_ADD */
      ObPostfixExpression::sub_func, /* T_OP_MINUS */
      ObPostfixExpression::mul_func, /* T_OP_MUL */
      ObPostfixExpression::div_func, /* T_OP_DIV */
      ObPostfixExpression::nop_func, /* TODO: T_OP_POW */
      ObPostfixExpression::nop_func, /* TODO: T_OP_REM */
      ObPostfixExpression::mod_func, /* T_OP_MOD */
      ObPostfixExpression::eq_func,  /* T_OP_EQ */
      ObPostfixExpression::le_func,  /* T_OP_LE */
      ObPostfixExpression::lt_func,  /* T_OP_LT */
      ObPostfixExpression::ge_func,  /* T_OP_GE */
      ObPostfixExpression::gt_func,  /* T_OP_GT */
      ObPostfixExpression::neq_func, /* T_OP_NE */
      ObPostfixExpression::is_func,  /* T_OP_IS */
      ObPostfixExpression::is_not_func,/* T_OP_IS_NOT */
      ObPostfixExpression::btw_func, /* T_OP_BTW */
      ObPostfixExpression::not_btw_func, /* T_OP_NOT_BTW */
      ObPostfixExpression::like_func,/* T_OP_LIKE */
      ObPostfixExpression::not_like_func, /* T_OP_NOT_LIKE */
      ObPostfixExpression::not_func, /* T_OP_NOT */
      ObPostfixExpression::and_func, /* T_OP_AND */
      ObPostfixExpression::or_func,  /* T_OP_OR */
      ObPostfixExpression::nop_func, /* TODO:  T_OP_IN */
      ObPostfixExpression::nop_func, /* TODO:  T_OP_NOT_IN */
      ObPostfixExpression::nop_func, /* TODO:  T_OP_AGR_CASE */
      ObPostfixExpression::nop_func, /* TODO:  T_OP_CASE */
      ObPostfixExpression::nop_func, /* TODO:  T_OP_ROW */
      ObPostfixExpression::nop_func, /* TODO: T_OP_EXISTS */
      ObPostfixExpression::nop_func, /* TODO:  T_OP_CNN */
      ObPostfixExpression::nop_func, // T_FUN_SYS
    };

    // system function table
    op_call_func_t ObPostfixExpression::SYS_FUNCS_TAB[SYS_FUNC_NUM] =
    {
      ObPostfixExpression::sys_func_length, // SYS_FUNC_LENGTH
    };

    const char* const ObPostfixExpression::SYS_FUNCS_NAME[SYS_FUNC_NUM] =
    {
      "length",
    };

    ObPostfixExpression::ObPostfixExpression()
      :did_int_div_as_double_(false), postfix_size_(0), str_buf_(0, DEF_STRING_BUF_SIZE)
    {

    }

    ObPostfixExpression::~ObPostfixExpression()
    {

    }

    ObPostfixExpression& ObPostfixExpression::operator=(const ObPostfixExpression &other)
    {
      int ret = OB_SUCCESS;
      int i = 0;
      for (i = 0; i < other.postfix_size_; i++)
      {
        if (ObVarcharType == expr_[i].get_type())
        {
          if(OB_SUCCESS != (ret = str_buf_.write_obj(other.expr_[i], &expr_[i])))
          {
            TBSYS_LOG(ERROR, "fail to write object to string buffer. ret=%d", ret);
          }
        }
        else
        {
          expr_[i] = other.expr_[i];
        }
      }
      postfix_size_ = other.postfix_size_;
      return *this;
    }

    int ObPostfixExpression::get_sys_func(const common::ObString &sys_func, ObSqlSysFunc &func_type) const
    {
      int ret = OB_SUCCESS;
      if (sys_func.length() == static_cast<int64_t>(strlen(SYS_FUNCS_NAME[SYS_FUNC_LENGTH]))
          && 0 == strncasecmp(SYS_FUNCS_NAME[SYS_FUNC_LENGTH], sys_func.ptr(), sys_func.length()))
      {
        func_type = SYS_FUNC_LENGTH;
      }
      else
      {
        ret = OB_ERR_UNKNOWN_SYS_FUNC;
      }
      return ret;
    }

    int ObPostfixExpression::add_expr_item(const ExprItem &item)
    {
      int err = OB_SUCCESS;
      ObObj obj;
      ObSqlSysFunc sys_func;
      if (OB_SUCCESS == err && postfix_size_ + 3 /* max symbol count for each item  */ <= OB_MAX_COMPOSITE_SYMBOL_COUNT)
      {
        switch(item.type_)
        {
          case T_STRING:
          case T_BINARY:
            expr_[postfix_size_++].set_int(CONST_OBJ);
            obj.set_varchar(item.string_);
            if (OB_SUCCESS != (err = str_buf_.write_obj(obj, expr_ + postfix_size_)))
            {
              TBSYS_LOG(WARN, "fail to write object to string buffer. err=%d", err);
              break;
            }
            postfix_size_++;
            break;
          case T_FLOAT:
            expr_[postfix_size_++].set_int(CONST_OBJ);
            expr_[postfix_size_++].set_double(item.value_.double_);
            break;
          case T_DOUBLE:
            expr_[postfix_size_++].set_int(CONST_OBJ);
            expr_[postfix_size_++].set_double(item.value_.double_);
            break;
          case T_DECIMAL:
            expr_[postfix_size_++].set_int(CONST_OBJ);
            obj.set_varchar(item.string_);
            if (OB_SUCCESS != (err = str_buf_.write_obj(obj, expr_ + postfix_size_)))
            {
              TBSYS_LOG(WARN, "fail to write object to string buffer. err=%d", err);
              break;
            }
            postfix_size_++;
            break;
          case T_INT:
            expr_[postfix_size_++].set_int(CONST_OBJ);
            expr_[postfix_size_++].set_int(item.value_.int_);
            break;
          case T_BOOL:
            expr_[postfix_size_++].set_int(CONST_OBJ);
            expr_[postfix_size_++].set_bool(item.value_.bool_);
            break;
          case T_DATE:
            expr_[postfix_size_++].set_int(CONST_OBJ);
            expr_[postfix_size_++].set_datetime(item.value_.datetime_);
            break;
          case T_REF_COLUMN:
             expr_[postfix_size_++].set_int(COLUMN_IDX);
             expr_[postfix_size_++].set_int(item.value_.cell_.tid);
             expr_[postfix_size_++].set_int(item.value_.cell_.cid);
             break;
          //case T_REF_EXPR:
          case T_REF_QUERY:
             TBSYS_LOG(WARN, "TODO... not implement yet");
             break;
          case T_OP_EXISTS:
          case T_OP_POS:
          case T_OP_NEG:
          case T_OP_ADD:
          case T_OP_MINUS:
          case T_OP_MUL:
          case T_OP_DIV:
          case T_OP_REM:
          case T_OP_POW:
          case T_OP_MOD:
          case T_OP_LE:
          case T_OP_LT:
          case T_OP_EQ:
          case T_OP_GE:
          case T_OP_GT:
          case T_OP_NE:
          case T_OP_LIKE:
          case T_OP_NOT_LIKE:
          case T_OP_AND:
          case T_OP_OR:
          case T_OP_NOT:
          case T_OP_IS:
          case T_OP_IS_NOT:
          case T_OP_BTW:
          case T_OP_NOT_BTW:
          case T_OP_CNN:
          case T_OP_IN:
          case T_OP_NOT_IN:
          case T_OP_ARG_CASE:
          case T_OP_CASE:
            expr_[postfix_size_++].set_int(OP);
            expr_[postfix_size_++].set_int(item.type_);
            expr_[postfix_size_++].set_int(item.value_.int_); // operator count
            break;
          case T_FUN_SYS:
            expr_[postfix_size_++].set_int(OP);
            expr_[postfix_size_++].set_int(item.type_); // system function
            expr_[postfix_size_++].set_int(item.value_.int_); // operator count
            if (OB_SUCCESS != (err = get_sys_func(item.string_, sys_func)))
            {
              TBSYS_LOG(WARN, "unknown system function=%.*s", item.string_.length(), item.string_.ptr());
            }
            else
            {
              expr_[postfix_size_++].set_int(sys_func); // system function type
            }
            break;
          case T_OP_ROW:
            expr_[postfix_size_++].set_int(VECTOR);
            expr_[postfix_size_++].set_int(item.type_);
            expr_[postfix_size_++].set_int(item.value_.int_);
            break;
          case T_NULL:
            /// TODO: T_NULL op?
            TBSYS_LOG(WARN, "TODO...");
            break;
          default:
            break;
        }
      }
      else
      {
        TBSYS_LOG(WARN, "There may be no enough spaces to set expression. "
            "postfix_size_=%d, OB_MAX_COMPOSITE_SYMBOL_COUNT=%ld",
            postfix_size_, OB_MAX_COMPOSITE_SYMBOL_COUNT);
        err = OB_ERROR;
      }
      return err;
    }

    int ObPostfixExpression::add_expr_item_end()
    {
      int err = OB_SUCCESS;
      if (postfix_size_ < OB_MAX_COMPOSITE_SYMBOL_COUNT)
      {
        expr_[postfix_size_++].set_int(END);
      }
      else
      {
        err = OB_ERROR;
        TBSYS_LOG(WARN, "fail to set expression END");
      }
      return err;
    }

    int ObPostfixExpression::get_expression(const oceanbase::common::ObObj *&expr, int &expr_size) const
    {
      int ret = OB_SUCCESS;
      expr = NULL;
      expr_size = 0;
      if (postfix_size_ > 0)
      {
        expr_size = postfix_size_;
        expr = expr_;
      }
      else
      {
        ret = OB_NOT_INIT;
        TBSYS_LOG(WARN, "expression is empty. postfix_size_=%d", postfix_size_);
      }
      return ret;
    }

    int ObPostfixExpression::calc(const common::ObRow &row, const ObObj *&composite_val)
    {
      int err = OB_SUCCESS;
      int64_t type = 0;
      int64_t value = 0;
      int64_t value2 = 0;
      int64_t sys_func = 0;
      int idx = 0;
      ObExprObj result;

      int idx_i = 0;
      ObPostExprExtraParams extra_params;
      extra_params.did_int_div_as_double_ = did_int_div_as_double_;

      do{
        // TBSYS_LOG(DEBUG, "idx=%d, idx_i=%d, type=%d\n", idx, idx_i, type);
        // 获得数据类型:列id、数字、操作符、结束标记
        if (OB_SUCCESS != (err = expr_[idx++].get_int(type)))
        {
          TBSYS_LOG(WARN, "fail to get int value. unexpected! ret=%d", err);
          err = OB_ERR_UNEXPECTED;
        }
        // expr_中以END符号表示结束
        else if (END == type){
          if (idx_i != 1)
          {
            TBSYS_LOG(WARN,"calculation stack must be empty. check the code for bugs. idx_i=%d", idx_i);
            err = OB_ERROR;
          }
          else if (OB_SUCCESS != (err = stack_i[--idx_i].to(result_)))
          {
            TBSYS_LOG(WARN, "failed to convert exprobj to obj, err=%d", err);
          }
          else
          {
            composite_val = &result_;
          }
          break;
        }
        else if(type <= BEGIN_TYPE || type >= END_TYPE)
        {
          TBSYS_LOG(WARN,"unsupported operand type [type:%ld]", value);
          err = OB_INVALID_ARGUMENT;
          break;
        }

        if (idx_i < 0 || idx_i >= OB_MAX_COMPOSITE_SYMBOL_COUNT || idx > postfix_size_)
        {
          TBSYS_LOG(WARN,"calculation stack overflow [stack.index:%d] "
              "or run out of operand [operand.used:%d,operand.avaliable:%d]", idx_i, idx, postfix_size_);
          err = OB_ERR_UNEXPECTED;
          break;
        }
        if (OB_SUCCESS == err)
        {
          switch(type)
          {
            case COLUMN_IDX:
              if (OB_SUCCESS  != (err = expr_[idx++].get_int(value)))
              {
                TBSYS_LOG(WARN,"get_int error [err:%d]", err);
              }
              else if (OB_SUCCESS  != (err = expr_[idx++].get_int(value2)))
              {
                TBSYS_LOG(WARN,"get_int error [err:%d]", err);
              }
              else
              {
                const ObObj *cell = NULL;
                if (OB_SUCCESS != (err = row.get_cell((const uint64_t)value, (const uint64_t)value2, cell)))
                {
                  TBSYS_LOG(WARN, "fail to get cell from row. err=%d tid=%ld cid=%ld", err, value, value2);
                }
                else if (NULL != cell)
                {
                  stack_i[idx_i++].assign(*cell);
                }
                else
                {
                  TBSYS_LOG(WARN, "null cell");
                  err = OB_ERROR;
                }
              }
              break;
            case CONST_OBJ:
              stack_i[idx_i++].assign(expr_[idx++]);
              break;
            case OP:
              // 根据OP的类型，从堆栈中弹出1个或多个操作数，进行计算
              if (OB_SUCCESS != (err = expr_[idx++].get_int(value)))
              {
                TBSYS_LOG(WARN,"get_int error [err:%d]", err);
              }
              else if (value <= T_MIN_OP || value >= T_MAX_OP)
              {
                TBSYS_LOG(WARN,"unsupported operator type [type:%ld]", value);
                err = OB_INVALID_ARGUMENT;
              }
              else if(OB_SUCCESS != (err = expr_[idx++].get_int(value2 /*param_count*/)))
              {
                 TBSYS_LOG(WARN,"get_int error [err:%d]", err);
              }
              else
              {
                extra_params.operand_count_ = static_cast<int32_t>(value2);
                if (OB_UNLIKELY(T_FUN_SYS == value))
                {
                  if(OB_SUCCESS != (err = expr_[idx++].get_int(sys_func)))
                  {
                    TBSYS_LOG(WARN, "failed to get sys func, err=%d", err);
                  }
                  else if (0 > sys_func || sys_func >= SYS_FUNC_NUM)
                  {
                    TBSYS_LOG(WARN, "invalid sys function type=%ld", sys_func);
                    err = OB_ERR_UNEXPECTED;
                  }
                  else if (OB_SUCCESS != (err = SYS_FUNCS_TAB[sys_func](stack_i, idx_i, result, extra_params)))
                  {
                    TBSYS_LOG(WARN, "failed to call sys func, err=%d func=%ld", err, sys_func);
                  }
                  else
                  {
                    stack_i[idx_i++] = result;
                  }
                }
                else
                {
                  if (OB_SUCCESS != (err = (this->call_func[value - T_MIN_OP - 1])(stack_i, idx_i, result, extra_params)))
                  {
                    TBSYS_LOG(WARN,"call calculation function error [value:%ld, idx_i:%d, err:%d]",value, idx_i, err);
                  }
                  else
                  {
                    stack_i[idx_i++] = result;
                  }
                }
              }
              break;
            default:
              err = OB_ERR_UNEXPECTED;
              TBSYS_LOG(WARN,"unexpected [type:%ld]", type);
              break;
          }
        }
        if (OB_SUCCESS != err)
        {
          break;
        }
      }while(true);

      return err;
    }


    int ObPostfixExpression::is_const_expr(bool &is_type) const
    {
      return check_expr_type((int64_t)CONST_OBJ, is_type, 3);
    }

    int ObPostfixExpression::is_column_index_expr(bool &is_type) const
    {
      return ObPostfixExpression::check_expr_type((int64_t)COLUMN_IDX, is_type, 4);
    }

    int ObPostfixExpression::check_expr_type(const int64_t type_val, bool &is_type, const int64_t stack_size) const
    {
      int err = OB_SUCCESS;
      int64_t expr_type = -1;
      is_type = false;
      if (postfix_size_ == stack_size)
      {
        if (ObIntType == expr_[0].get_type())
        {
          if (OB_SUCCESS != (err = expr_[0].get_int(expr_type)))
          {
            TBSYS_LOG(WARN, "fail to get int value.err=%d", err);
          }
          else if (type_val == expr_type)
          {
            is_type = true;
          }
        }
      }
      return err;
    }

    inline static bool test_expr_int(const ObObj* expr, int i, int64_t expected)
    {
      int64_t val = 0;
      return ObIntType == expr[i].get_type()
        && OB_SUCCESS == expr[i].get_int(val)
        && expected == val;
    }

    bool ObPostfixExpression::is_equijoin_cond(ExprItem::SqlCellInfo &c1, ExprItem::SqlCellInfo &c2) const
    {
      bool ret = false;
      // COL_IDX|tid|cid|COL_IDX|tid|cid|OP|EQ|2|END
      if (postfix_size_ == 10)
      {
        if (test_expr_int(expr_, 0, COLUMN_IDX)
            && test_expr_int(expr_, 3, COLUMN_IDX)
            && test_expr_int(expr_, 6, OP)
            && test_expr_int(expr_, 7, T_OP_EQ)
            && test_expr_int(expr_, 8, 2))
        {
          ret = true;
          int64_t val = 0;
          if (ObIntType == expr_[1].get_type()
              && OB_SUCCESS == expr_[1].get_int(val))
          {
            c1.tid = val;
          }
          else
          {
            ret = false;
          }
          if (ObIntType == expr_[2].get_type()
              && OB_SUCCESS == expr_[2].get_int(val))
          {
            c1.cid = val;
          }
          else
          {
            ret = false;
          }
          if (ObIntType == expr_[4].get_type()
              && OB_SUCCESS == expr_[4].get_int(val))
          {
            c2.tid = val;
          }
          else
          {
            ret = false;
          }
          if (ObIntType == expr_[5].get_type()
              && OB_SUCCESS == expr_[5].get_int(val))
          {
            c2.cid = val;
          }
          else
          {
            ret = false;
          }
        }
      }
      return ret;
    }

    DEFINE_SERIALIZE(ObPostfixExpression)
    {
      int ret = OB_SUCCESS;
      int i = 0;
      ObObj obj;
      obj.set_int(postfix_size_);
      if (OB_SUCCESS != (ret = obj.serialize(buf, buf_len, pos)))
      {
        TBSYS_LOG(WARN, "fail to serialize postfix expression size. ret=%d", ret);
      }
      else
      {
        for (i = 0; i < postfix_size_; i++)
        {
          if (OB_SUCCESS != (ret = expr_[i].serialize(buf, buf_len, pos)))
          {
            TBSYS_LOG(WARN, "fail to serialize expr[%d]. ret=%d", i, ret);
            break;
          }
        }
      }
      return ret;
    }

    DEFINE_DESERIALIZE(ObPostfixExpression)
    {
      int ret = OB_SUCCESS;
      int i = 0;
      ObObj obj;
      int64_t val = 0;
      if (OB_SUCCESS != (ret = obj.deserialize(buf, data_len, pos)))
      {
        TBSYS_LOG(WARN, "fail to deserialize obj. ret=%d. buf=%p, data_len=%ld, pos=%ld",
            ret, buf, data_len, pos);
      }
      else if (ObIntType != obj.get_type())
      {
        TBSYS_LOG(WARN, "unexpected obj type. actual type:%d, expected:%d", obj.get_type(), ObIntType);
        ret = OB_ERR_UNEXPECTED;
      }
      else
      {
        if ((OB_SUCCESS != (ret = obj.get_int(val))) || (val <= 0))
        {
          TBSYS_LOG(WARN, "fail to get int value. ret=%d, postfix_size_%ld", ret, val);
        }
        else
        {
          postfix_size_ = static_cast<int32_t>(val);
          for (i = 0; i < postfix_size_; i++)
          {
            if (OB_SUCCESS != (ret = obj.deserialize(buf, data_len, pos)))
            {
              TBSYS_LOG(WARN, "fail to deserialize obj. ret=%d. buf=%p, data_len=%ld, pos=%ld",
                  ret, buf, data_len, pos);
              break;
            }
            else if (ObVarcharType == obj.get_type())
            {
              if (OB_SUCCESS != (ret = str_buf_.write_obj(obj, &expr_[i])))
              {
                TBSYS_LOG(WARN, "fail to write object to string buffer. ret=%d", ret);
              }
            }
            else
            {
              expr_[i] = obj;
            }
          }
        }
      }
      return ret;
    }

    DEFINE_GET_SERIALIZE_SIZE(ObPostfixExpression)
    {
      int64_t size = 0;
      int i = 0;
      ObObj obj;
      obj.set_int(postfix_size_);
      size += obj.get_serialize_size();
      for (i = 0; i < postfix_size_; i++)
      {
        size += expr_[i].get_serialize_size();
      }
      return size;
    }

    /************************************************************************/
    /*****************   function implementation     ************************/
    /************************************************************************/
    inline int ObPostfixExpression::nop_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params)
    {
      UNUSED(params);
      UNUSED(stack_i);
      UNUSED(idx_i);
      UNUSED(result);
      TBSYS_LOG(WARN, "function not implemented!");
      return OB_ERROR;
    }

    inline int ObPostfixExpression::reserved_func(const ObExprObj &obj1, const ObExprObj &obj2, ObExprObj &result)
    {
      int err = OB_INVALID_ARGUMENT;
      UNUSED(obj1);
      UNUSED(obj2);
      UNUSED(result);
      return err;
    }


    /* compare function list:
     * >   gt_func
     * >=  ge_func
     * <=  le_func
     * <   lt_func
     * ==  eq_func
     * !=  neq_func
     */
    inline int ObPostfixExpression::gt_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params)
    {
      int err = OB_SUCCESS;
      UNUSED(params);
      if (NULL == stack_i)
      {
        TBSYS_LOG(WARN, "stack pointer is NULL.");
        err = OB_INVALID_ARGUMENT;
      }
      else if (idx_i < 2)
      {
        TBSYS_LOG(WARN, "no enough operand in the stack. current size:%d", idx_i);
        err = OB_INVALID_ARGUMENT;
      }
      else
      {
        stack_i[idx_i-2].gt(stack_i[idx_i-1], result);
        idx_i -= 2;
      }
      return err;
    }

    inline int ObPostfixExpression::ge_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params)
    {
      int err = OB_SUCCESS;
      UNUSED(params);
      if (NULL == stack_i)
      {
        TBSYS_LOG(WARN, "stack pointer is NULL.");
        err = OB_INVALID_ARGUMENT;
      }
      else if (idx_i < 2)
      {
        TBSYS_LOG(WARN, "no enough operand in the stack. current size:%d", idx_i);
        err = OB_INVALID_ARGUMENT;
      }
      else
      {
        stack_i[idx_i-2].ge(stack_i[idx_i-1], result);
        idx_i -= 2;
      }
      return err;
    }

    inline int ObPostfixExpression::le_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params)
    {
      int err = OB_SUCCESS;
      UNUSED(params);
      if (NULL == stack_i)
      {
        TBSYS_LOG(WARN, "stack pointer is NULL.");
        err = OB_INVALID_ARGUMENT;
      }
      else if (idx_i < 2)
      {
        TBSYS_LOG(WARN, "no enough operand in the stack. current size:%d", idx_i);
        err = OB_INVALID_ARGUMENT;
      }
      else
      {
        stack_i[idx_i-2].le(stack_i[idx_i-1], result);
        idx_i -= 2;
      }
      return err;
    }

    inline int ObPostfixExpression::lt_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params)
    {
      int err = OB_SUCCESS;
      UNUSED(params);
      if (NULL == stack_i)
      {
        TBSYS_LOG(WARN, "stack pointer is NULL.");
        err = OB_INVALID_ARGUMENT;
      }
      else if (idx_i < 2)
      {
        TBSYS_LOG(WARN, "no enough operand in the stack. current size:%d", idx_i);
        err = OB_INVALID_ARGUMENT;
      }
      else
      {
        stack_i[idx_i-2].lt(stack_i[idx_i-1], result);
        idx_i -= 2;
      }
      return err;
    }

    inline int ObPostfixExpression::eq_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params)
    {
      int err = OB_SUCCESS;
      UNUSED(params);
      if (NULL == stack_i)
      {
        TBSYS_LOG(WARN, "stack pointer is NULL.");
        err = OB_INVALID_ARGUMENT;
      }
      else if (idx_i < 2)
      {
        TBSYS_LOG(WARN, "no enough operand in the stack. current size:%d", idx_i);
        err = OB_INVALID_ARGUMENT;
      }
      else
      {
        stack_i[idx_i-2].eq(stack_i[idx_i-1], result);
        idx_i -= 2;
      }
      return err;
    }

    inline int ObPostfixExpression::neq_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params)
    {
      int err = OB_SUCCESS;
      UNUSED(params);
      if (NULL == stack_i)
      {
        TBSYS_LOG(WARN, "stack pointer is NULL.");
        err = OB_INVALID_ARGUMENT;
      }
      else if (idx_i < 2)
      {
        TBSYS_LOG(WARN, "no enough operand in the stack. current size:%d", idx_i);
        err = OB_INVALID_ARGUMENT;
      }
      else
      {
        stack_i[idx_i-2].ne(stack_i[idx_i-1], result);
        idx_i -= 2;
      }
      return err;
    }

    inline int ObPostfixExpression::is_not_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params)
    {
      int err = OB_SUCCESS;
      UNUSED(params);
      if (NULL == stack_i)
      {
        TBSYS_LOG(WARN, "stack pointer is NULL.");
        err = OB_INVALID_ARGUMENT;
      }
      else if (idx_i < 2)
      {
        TBSYS_LOG(WARN, "no enough operand in the stack. current size:%d", idx_i);
        err = OB_INVALID_ARGUMENT;
      }
      else
      {
        stack_i[idx_i-2].is_not(stack_i[idx_i-1], result);
        idx_i -= 2;
      }
      return err;
    }

    inline int ObPostfixExpression::is_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params)
    {
      int err = OB_SUCCESS;
      UNUSED(params);
      if (NULL == stack_i)
      {
        TBSYS_LOG(WARN, "stack pointer is NULL.");
        err = OB_INVALID_ARGUMENT;
      }
      else if (idx_i < 2)
      {
        TBSYS_LOG(WARN, "no enough operand in the stack. current size:%d", idx_i);
        err = OB_INVALID_ARGUMENT;
      }
      else
      {
        stack_i[idx_i-2].is(stack_i[idx_i-1], result);
        idx_i -= 2;
      }
      return err;
    }


    inline int ObPostfixExpression::add_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params)
    {
      int err = OB_SUCCESS;
      UNUSED(params);
      if (NULL == stack_i)
      {
        TBSYS_LOG(WARN, "stack pointer is NULL.");
        err = OB_INVALID_ARGUMENT;
      }
      else if (idx_i < 2)
      {
        TBSYS_LOG(WARN, "no enough operand in the stack. current size:%d", idx_i);
        err = OB_INVALID_ARGUMENT;
      }
      else
      {
        stack_i[idx_i-2].add(stack_i[idx_i-1], result);
        idx_i -= 2;
      }
      return err;
    }


    inline int ObPostfixExpression::sub_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params)
    {
      int err = OB_SUCCESS;
      UNUSED(params);
      if (NULL == stack_i)
      {
        TBSYS_LOG(WARN, "stack pointer is NULL.");
        err = OB_INVALID_ARGUMENT;
      }
      else if (idx_i < 2)
      {
        TBSYS_LOG(WARN, "no enough operand in the stack. current size:%d", idx_i);
        err = OB_INVALID_ARGUMENT;
      }
      else
      {
        stack_i[idx_i-2].sub(stack_i[idx_i-1], result);
        idx_i -= 2;
      }
      return err;
    }


    inline int ObPostfixExpression::mul_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params)
    {
      int err = OB_SUCCESS;
      UNUSED(params);
      if (NULL == stack_i)
      {
        TBSYS_LOG(WARN, "stack pointer is NULL.");
        err = OB_INVALID_ARGUMENT;
      }
      else if (idx_i < 2)
      {
        TBSYS_LOG(WARN, "no enough operand in the stack. current size:%d", idx_i);
        err = OB_INVALID_ARGUMENT;
      }
      else
      {
        stack_i[idx_i-2].mul(stack_i[idx_i-1], result);
        idx_i -= 2;
      }
      return err;
    }


    inline int ObPostfixExpression::div_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params)
    {
      int err = OB_SUCCESS;
      if (NULL == stack_i)
      {
        TBSYS_LOG(WARN, "stack pointer is NULL.");
        err = OB_INVALID_ARGUMENT;
      }
      else if (idx_i < 2)
      {
        TBSYS_LOG(WARN, "no enough operand in the stack. current size:%d", idx_i);
        err = OB_INVALID_ARGUMENT;
      }
      else
      {
        stack_i[idx_i-2].div(stack_i[idx_i-1], result, params.did_int_div_as_double_);
        idx_i -= 2;
      }
      return err;
    }

    inline int ObPostfixExpression::mod_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params)
    {
      int err = OB_SUCCESS;
      UNUSED(params);
      if (NULL == stack_i)
      {
        TBSYS_LOG(WARN, "stack pointer is NULL.");
        err = OB_INVALID_ARGUMENT;
      }
      else if (idx_i < 2)
      {
        TBSYS_LOG(WARN, "no enough operand in the stack. current size:%d", idx_i);
        err = OB_INVALID_ARGUMENT;
      }
      else
      {
        stack_i[idx_i-2].mod(stack_i[idx_i-1], result);
        idx_i -= 2;
      }
      return err;
    }


    inline int ObPostfixExpression::and_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params)
    {
      int err = OB_SUCCESS;
      UNUSED(params);
      if (NULL == stack_i)
      {
        TBSYS_LOG(WARN, "stack pointer is NULL.");
        err = OB_INVALID_ARGUMENT;
      }
      else if (idx_i < 2)
      {
        TBSYS_LOG(WARN, "no enough operand in the stack. current size:%d", idx_i);
        err = OB_INVALID_ARGUMENT;
      }
      else
      {
        stack_i[idx_i-2].land(stack_i[idx_i-1], result);
        idx_i -= 2;
      }
      return err;
    }

    inline int ObPostfixExpression::or_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params)
    {
      int err = OB_SUCCESS;
      UNUSED(params);
      if (NULL == stack_i)
      {
        TBSYS_LOG(WARN, "stack pointer is NULL.");
        err = OB_INVALID_ARGUMENT;
      }
      else if (idx_i < 2)
      {
        TBSYS_LOG(WARN, "no enough operand in the stack. current size:%d", idx_i);
        err = OB_INVALID_ARGUMENT;
      }
      else
      {
        stack_i[idx_i-2].lor(stack_i[idx_i-1], result);
        idx_i -= 2;
      }
      return err;
    }

    inline int ObPostfixExpression::minus_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params)
    {
      int err = OB_SUCCESS;
      UNUSED(params);
      if (NULL == stack_i)
      {
        TBSYS_LOG(WARN, "stack pointer is NULL.");
        err = OB_INVALID_ARGUMENT;
      }
      else if (idx_i < 1)
      {
        TBSYS_LOG(WARN, "no enough operand in the stack. current size:%d", idx_i);
        err = OB_INVALID_ARGUMENT;
      }
      else
      {
        stack_i[idx_i-1].negate(result);
        idx_i -= 1;
      }
      return err;
    }


    inline int ObPostfixExpression::plus_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params)
    {
      int err = OB_SUCCESS;
      UNUSED(params);
      if (NULL == stack_i)
      {
        TBSYS_LOG(WARN, "stack pointer is NULL.");
        err = OB_INVALID_ARGUMENT;
      }
      else if (idx_i < 1)
      {
        TBSYS_LOG(WARN, "no enough operand in the stack. current size:%d", idx_i);
        err = OB_INVALID_ARGUMENT;
      }
      else
      {
        // don't touch it whatever the type is
        result = stack_i[idx_i-1];
        idx_i -= 1;
      }
      return err;
    }



    inline int ObPostfixExpression::not_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params)
    {
      int err = OB_SUCCESS;
      UNUSED(params);
      if (NULL == stack_i)
      {
        TBSYS_LOG(WARN, "stack pointer is NULL.");
        err = OB_INVALID_ARGUMENT;
      }
      else if (idx_i < 1)
      {
        TBSYS_LOG(WARN, "no enough operand in the stack. current size:%d", idx_i);
        err = OB_INVALID_ARGUMENT;
      }
      else
      {
        stack_i[idx_i-1].lnot(result);
        idx_i -= 1;
      }
      return err;
    }

    inline int ObPostfixExpression::like_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params)
    {
      int err = OB_SUCCESS;
      UNUSED(params);
      if (NULL == stack_i)
      {
        TBSYS_LOG(WARN, "stack pointer is NULL.");
        err = OB_INVALID_ARGUMENT;
      }
      else if (idx_i < 2)
      {
        TBSYS_LOG(WARN, "no enough operand in the stack. current size:%d", idx_i);
        err = OB_INVALID_ARGUMENT;
      }
      else
      {
        err = stack_i[idx_i-2].like(stack_i[idx_i-1], result);
        idx_i -= 2;
      }
      return err;
    }

    inline int ObPostfixExpression::in_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params)
    {
      // in的算法
      //
      // 例1.
      // 2 IN (3, 4) 的后缀表达式为 [栈顶] 2, 3, 4, Row(2), IN(2)
      // 那么经过本算法后，在遇到IN之前，栈内元素的顺序为
      // [栈顶] Row(2), 4, 3, 2.
      // 计算时，根据IN(2)，需要从栈中解析出两个/组操作数出来，不难解析出
      // [stack_i栈顶] Row(2), 4, 3为第一个操作数，2为第二个操作数
      //
      // 例2.
      // (1, 2) IN ((3, 4), (1, 2))的后缀表达式为
      // [栈顶] 1, 2, Row(2), 3, 4, Row(2), 1, 2, Row(2), Row(2), IN(2)
      // 遇到IN之前不断出栈，有：
      // [stack_i栈顶] Row(2), Row(2), 2, 1, Row(2), 4, 3, Row(2), 2, 1
      // 同样，计算IN时，这种结构也不难解析和计算
      //
      // Row(2), 4, 3, 2的obj数组表示方法
      // type(row) value(2) type(const) value(4) type(const) value(3) type(const) value(2)
      nop_func(stack_i, idx_i, result, params);
      return OB_SUCCESS;
    }

    int ObPostfixExpression::not_like_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params)
    {
      int err = OB_SUCCESS;
      UNUSED(params);
      if (NULL == stack_i)
      {
        TBSYS_LOG(WARN, "stack pointer is NULL.");
        err = OB_INVALID_ARGUMENT;
      }
      else if (idx_i < 2)
      {
        TBSYS_LOG(WARN, "no enough operand in the stack. current size:%d", idx_i);
        err = OB_INVALID_ARGUMENT;
      }
      else
      {
        stack_i[idx_i-2].not_like(stack_i[idx_i-1], result);
        idx_i -= 2;
      }
      return err;
    }

    int ObPostfixExpression::btw_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params)
    {
      int ret = OB_SUCCESS;
      UNUSED(params);
      if (NULL == stack_i)
      {
        TBSYS_LOG(WARN, "stack pointer is NULL.");
        ret = OB_INVALID_ARGUMENT;
      }
      else if (idx_i < 3)
      {
        TBSYS_LOG(WARN, "no enough operand in the stack. current size:%d", idx_i);
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        stack_i[idx_i-3].btw(stack_i[idx_i-2], stack_i[idx_i-1], result);
        idx_i -= 3;
      }
      return ret;
    }

    int ObPostfixExpression::not_btw_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params)
    {
      int ret = OB_SUCCESS;
      UNUSED(params);
      if (NULL == stack_i)
      {
        TBSYS_LOG(WARN, "stack pointer is NULL.");
        ret = OB_INVALID_ARGUMENT;
      }
      else if (idx_i < 3)
      {
        TBSYS_LOG(WARN, "no enough operand in the stack. current size:%d", idx_i);
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        stack_i[idx_i-3].not_btw(stack_i[idx_i-2], stack_i[idx_i-1], result);
        idx_i -= 3;
      }
      return ret;
    }

    inline int ObPostfixExpression::sys_func_length(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params)
    {
      int err = OB_SUCCESS;
      UNUSED(params);
      if (NULL == stack_i)
      {
        TBSYS_LOG(WARN, "stack pointer is NULL.");
        err = OB_INVALID_ARGUMENT;
      }
      else if (idx_i < 1)
      {
        TBSYS_LOG(WARN, "no enough operand in the stack. current size:%d", idx_i);
        err = OB_INVALID_ARGUMENT;
      }
      else
      {
        stack_i[idx_i-1].varchar_length(result);
        idx_i -= 1;
      }
      return err;
    }

    int64_t ObPostfixExpression::to_string(char* buf, const int64_t buf_len) const
    {
      int64_t pos = 0;
      int err = OB_SUCCESS;
      int idx = 0;
      int64_t type = 0;
      int64_t value = 0;
      int64_t value2 = 0;
      int64_t sys_func = 0;
      while(idx < postfix_size_ && OB_SUCCESS == err)
      {
        expr_[idx++].get_int(type);
        if (END == type)
        {
          break;
        }
        else if(type <= BEGIN_TYPE || type >= END_TYPE)
        {
          break;
        }
        switch(type)
        {
          case COLUMN_IDX:
            if (OB_SUCCESS  != (err = expr_[idx++].get_int(value)))
            {
              TBSYS_LOG(WARN,"get_int error [err:%d]", err);
            }
            else if (OB_SUCCESS  != (err = expr_[idx++].get_int(value2)))
            {
              TBSYS_LOG(WARN,"get_int error [err:%d]", err);
            }
            else
            {
              uint64_t tid = static_cast<uint64_t>(value);
              if (OB_INVALID_ID == tid)
              {
                databuff_printf(buf, buf_len, pos, "COL<NULL,%lu>|", static_cast<uint64_t>(value2));
              }
              else
              {
                databuff_printf(buf, buf_len, pos, "COL<%lu,%lu>|", tid, static_cast<uint64_t>(value2));
              }
            }
            break;
          case CONST_OBJ:
            databuff_printf(buf, buf_len, pos, "CONST|");
            //@todo pos += expr_[idx++].to_string(buf+pos, buf_len-pos);
            idx++;              // skip the const obj value
            break;
          case OP:
            // 根据OP的类型，从堆栈中弹出1个或多个操作数，进行计算
            if (OB_SUCCESS != (err = expr_[idx++].get_int(value)))
            {
              TBSYS_LOG(WARN,"get_int error [err:%d]", err);
            }
            else if (value <= T_MIN_OP || value >= T_MAX_OP)
            {
              TBSYS_LOG(WARN,"unsupported operator type [type:%ld]", value);
              err = OB_INVALID_ARGUMENT;
            }
            else if(OB_SUCCESS != (err = expr_[idx++].get_int(value2 /*param_count*/)))
            {
              TBSYS_LOG(WARN,"get_int error [err:%d]", err);
            }
            else
            {
              if (OB_UNLIKELY(T_FUN_SYS == value))
              {
                if(OB_SUCCESS != (err = expr_[idx++].get_int(sys_func)))
                {
                  TBSYS_LOG(WARN, "failed to get sys func, err=%d", err);
                }
                else if (0 > sys_func || sys_func >= SYS_FUNC_NUM)
                {
                  TBSYS_LOG(WARN, "invalid sys function type=%ld", sys_func);
                  err = OB_ERR_UNEXPECTED;
                }
                else
                {
                  databuff_printf(buf, buf_len, pos, "%s<%ld>|", SYS_FUNCS_NAME[sys_func], value2);
                }
              }
              else
              {
                databuff_printf(buf, buf_len, pos, "%s<%ld>|", ob_op_func_str(static_cast<ObItemType>(value)), value2);
              }
            }
            break;
          default:
            err = OB_ERR_UNEXPECTED;
            TBSYS_LOG(WARN,"unexpected [type:%ld]", type);
            break;
        }
      } // end while
      return pos;
    }

  } /* sql */
} /* namespace */
