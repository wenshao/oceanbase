#ifndef OCEANBASE_SQL_RAWEXPR_H_
#define OCEANBASE_SQL_RAWEXPR_H_
#include "ob_bit_set.h"
#include "ob_sql_expression.h"
#include "common/ob_define.h"
#include "common/ob_vector.h"
#include "common/ob_string.h"
#include "common/ob_string_buf.h"

namespace oceanbase
{
  namespace sql
  {
    class ObTransformer;
    class ObLogicalPlan;
    class ObPhysicalPlan;
    
    class ObRawExpr
    {
    public:
      explicit ObRawExpr(ObItemType expr_type = T_INVALID)
          :type_(expr_type)
      {
        result_type_ = ObMinType;
      }
      virtual ~ObRawExpr() {}
      //virtual void trace(FILE *stream, int indentNum = 0);
      const ObItemType get_expr_type() const { return type_; }
      const common::ObObjType & get_result_type() const { return result_type_; }
      void set_expr_type(ObItemType type) { type_ = type; }
      void set_result_type(const common::ObObjType & type) { result_type_ = type; }

      bool is_const() const;
      // Format like "C1 = 5"
      bool is_equal_filter() const;
      // Format like "C1 between 5 and 10"
      bool is_range_filter() const;
      // Only format like "T1.c1 = T2.c1", not "T1.c1 - T2.c1 = 0"
      bool is_join_cond() const;
      bool is_aggr_fun() const;
      virtual int fill_sql_expression(
          ObSqlExpression& inter_expr,
          ObTransformer *transformer = NULL,
          ObLogicalPlan *logical_plan = NULL,
          ObPhysicalPlan *physical_plan = NULL) const = 0;
      virtual void print(FILE* fp, int32_t level) const = 0;

    private:
      ObItemType  type_;
      common::ObObjType result_type_;
    };

    class ObConstRawExpr : public ObRawExpr
    {
    public:
      ObConstRawExpr()
      {
      }
      ObConstRawExpr(oceanbase::common::ObObj& val, ObItemType expr_type = T_INVALID)
          : ObRawExpr(expr_type), value_(val)
      {
      }
      virtual ~ObConstRawExpr() {}
      const oceanbase::common::ObObj& get_value() const { return value_; } 
      void set_value(const oceanbase::common::ObObj& val) { value_ = val; }
      virtual int fill_sql_expression(
          ObSqlExpression& inter_expr,
          ObTransformer *transformer = NULL,
          ObLogicalPlan *logical_plan = NULL,
          ObPhysicalPlan *physical_plan = NULL) const;
      void print(FILE* fp, int32_t level) const;
      
    private:
      oceanbase::common::ObObj value_;
    };

    class ObUnaryRefRawExpr : public ObRawExpr
    {
    public:
      ObUnaryRefRawExpr()
      {
        id_ = OB_INVALID_ID;
      }
      ObUnaryRefRawExpr(uint64_t id, ObItemType expr_type = T_INVALID)
          : ObRawExpr(expr_type), id_(id)
      {
      }
      virtual ~ObUnaryRefRawExpr() {}
      uint64_t get_ref_id() const { return id_; } 
      void set_ref_id(uint64_t id) { id_ = id; }
      virtual int fill_sql_expression(
          ObSqlExpression& inter_expr,
          ObTransformer *transformer = NULL,
          ObLogicalPlan *logical_plan = NULL,
          ObPhysicalPlan *physical_plan = NULL) const;
      void print(FILE* fp, int32_t level) const;
      int get_name(common::ObString& name) const;
      
    private:
      uint64_t id_;
    };   

    class ObBinaryRefRawExpr : public ObRawExpr
    {
    public:
      ObBinaryRefRawExpr()
      {
        first_id_ = OB_INVALID_ID;
        second_id_ = OB_INVALID_ID;
      }
      ObBinaryRefRawExpr(uint64_t first_id, uint64_t second_id, ObItemType expr_type = T_INVALID)
          : ObRawExpr(expr_type), first_id_(first_id), second_id_(second_id)
      {
      }
      virtual ~ObBinaryRefRawExpr() {}
      uint64_t get_first_ref_id() const { return first_id_; } 
      uint64_t get_second_ref_id() const { return second_id_; } 
      void set_first_ref_id(uint64_t id) { first_id_ = id; }
      void set_second_ref_id(uint64_t id) { second_id_ = id; }
      virtual int fill_sql_expression(
          ObSqlExpression& inter_expr,
          ObTransformer *transformer = NULL,
          ObLogicalPlan *logical_plan = NULL,
          ObPhysicalPlan *physical_plan = NULL) const;
      void print(FILE* fp, int32_t level) const;
      
    private:
      uint64_t first_id_;
      uint64_t second_id_;
    }; 

    class ObUnaryOpRawExpr : public ObRawExpr
    {
    public:
      ObUnaryOpRawExpr()
      {
        expr_ = NULL;
      }
      ObUnaryOpRawExpr(ObRawExpr *expr, ObItemType expr_type = T_INVALID)
          : ObRawExpr(expr_type), expr_(expr)
      {
      }
      virtual ~ObUnaryOpRawExpr() {}
      ObRawExpr* get_op_expr() const { return expr_; } 
      void set_op_expr(ObRawExpr *expr) { expr_ = expr; }
      virtual int fill_sql_expression(
          ObSqlExpression& inter_expr,
          ObTransformer *transformer = NULL,
          ObLogicalPlan *logical_plan = NULL,
          ObPhysicalPlan *physical_plan = NULL) const;
      void print(FILE* fp, int32_t level) const;
      
    private:
      ObRawExpr *expr_;
    };   

    class ObBinaryOpRawExpr : public ObRawExpr
    {
    public:
      ObBinaryOpRawExpr()
      {
      }
      ObBinaryOpRawExpr(
          ObRawExpr *first_expr, ObRawExpr *second_expr, ObItemType expr_type = T_INVALID)
          : ObRawExpr(expr_type), first_expr_(first_expr), second_expr_(second_expr)
      {
      }
      virtual ~ObBinaryOpRawExpr(){} 
      ObRawExpr* get_first_op_expr() const { return first_expr_; } 
      ObRawExpr* get_second_op_expr() const { return second_expr_; } 
      void set_first_op_expr(ObRawExpr *first_expr) { first_expr_ = first_expr; }
      void set_second_op_expr(ObRawExpr *second_expr) { second_expr_ = second_expr; }
      virtual int fill_sql_expression(
          ObSqlExpression& inter_expr,
          ObTransformer *transformer = NULL,
          ObLogicalPlan *logical_plan = NULL,
          ObPhysicalPlan *physical_plan = NULL) const;
      void print(FILE* fp, int32_t level) const;
      
    private:
      ObRawExpr *first_expr_;
      ObRawExpr *second_expr_;
    }; 
    
    class ObTripleOpRawExpr : public ObRawExpr
    {
    public:
      ObTripleOpRawExpr()
      {
        first_expr_ = NULL;
        second_expr_ = NULL;
        third_expr_ = NULL;
      }
      ObTripleOpRawExpr(
          ObRawExpr *first_expr, ObRawExpr *second_expr, 
          ObRawExpr *third_expr, ObItemType expr_type = T_INVALID)
          : ObRawExpr(expr_type), 
          first_expr_(first_expr), second_expr_(second_expr),
          third_expr_(third_expr)
      {
      }
      virtual ~ObTripleOpRawExpr(){}
      ObRawExpr* get_first_op_expr() const { return first_expr_; } 
      ObRawExpr* get_second_op_expr() const { return second_expr_; } 
      ObRawExpr* get_third_op_expr() const { return third_expr_; } 
      void set_first_op_expr(ObRawExpr *first_expr) { first_expr_ = first_expr; }
      void set_second_op_expr(ObRawExpr *second_expr) { second_expr_ = second_expr; }
      void set_third_op_expr(ObRawExpr *third_expr) { third_expr_ = third_expr; }
      virtual int fill_sql_expression(
          ObSqlExpression& inter_expr,
          ObTransformer *transformer = NULL,
          ObLogicalPlan *logical_plan = NULL,
          ObPhysicalPlan *physical_plan = NULL) const;
      void print(FILE* fp, int32_t level) const;
      
    private:
      ObRawExpr *first_expr_;
      ObRawExpr *second_expr_;
      ObRawExpr *third_expr_;
    }; 

    class ObMultiOpRawExpr : public ObRawExpr
    {
    public:
      ObMultiOpRawExpr()
      {
      }
      virtual ~ObMultiOpRawExpr(){}
      ObRawExpr* get_op_expr(int32_t index) const
      {
        ObRawExpr* expr = NULL;
        if (index >= 0 && index < exprs_.size())
          expr = exprs_.at(index); 
        return expr;
      } 
      int add_op_expr(ObRawExpr *expr) { return exprs_.push_back(expr); }
      int32_t get_expr_size() const { return exprs_.size(); }
      virtual int fill_sql_expression(
          ObSqlExpression& inter_expr,
          ObTransformer *transformer = NULL,
          ObLogicalPlan *logical_plan = NULL,
          ObPhysicalPlan *physical_plan = NULL) const;
      void print(FILE* fp, int32_t level) const;

    private:
      oceanbase::common::ObVector<ObRawExpr*> exprs_;
    }; 

    class ObCaseOpRawExpr : public ObRawExpr
    {
    public:
      ObCaseOpRawExpr()
      {
        arg_expr_ = NULL;
        default_expr_ = NULL;
      }
      virtual ~ObCaseOpRawExpr(){}
      ObRawExpr* get_arg_op_expr() const { return arg_expr_; } 
      ObRawExpr* get_default_op_expr() const { return default_expr_; } 
      ObRawExpr* get_when_op_expr(int32_t index) const
      {
        ObRawExpr* expr = NULL;
        if (index >= 0 && index < when_exprs_.size())
          expr = when_exprs_[index];
        return expr;
      } 
      ObRawExpr* get_then_op_expr(int32_t index) const
      {
        ObRawExpr* expr = NULL;
        if (index >= 0 || index < then_exprs_.size())
          expr = then_exprs_[index];
        return expr;
      } 
      void set_arg_op_expr(ObRawExpr *expr) { arg_expr_ = expr; }
      void set_default_op_expr(ObRawExpr *expr) { default_expr_ = expr; }
      int add_when_op_expr(ObRawExpr *expr) { return when_exprs_.push_back(expr); }
      int add_then_op_expr(ObRawExpr *expr) { return then_exprs_.push_back(expr); }
      int32_t get_when_expr_size() const { return when_exprs_.size(); }
      int32_t get_then_expr_size() const { return then_exprs_.size(); }
      virtual int fill_sql_expression(
          ObSqlExpression& inter_expr,
          ObTransformer *transformer = NULL,
          ObLogicalPlan *logical_plan = NULL,
          ObPhysicalPlan *physical_plan = NULL) const;
      void print(FILE* fp, int32_t level) const;
      
    private:
      ObRawExpr *arg_expr_;
      oceanbase::common::ObVector<ObRawExpr*> when_exprs_;
      oceanbase::common::ObVector<ObRawExpr*> then_exprs_;
      ObRawExpr *default_expr_;
    }; 

    class ObAggFunRawExpr : public ObRawExpr
    {
    public:
      ObAggFunRawExpr()
      {
        param_expr_ = NULL;
        set_distinct_ = false;
      }
      virtual ~ObAggFunRawExpr() {}
      ObRawExpr* get_param_expr() const { return param_expr_; }
      void set_param_expr(ObRawExpr *expr) { param_expr_ = expr; }
      bool is_set_type() const { return set_distinct_; }
      void set_param_distinct() { set_distinct_ = true; }
      virtual int fill_sql_expression(
          ObSqlExpression& inter_expr,
          ObTransformer *transformer = NULL,
          ObLogicalPlan *logical_plan = NULL,
          ObPhysicalPlan *physical_plan = NULL) const;
      void print(FILE* fp, int32_t level) const;

    private:
      // NULL means '*'
      ObRawExpr* param_expr_;
      bool     set_distinct_;
    };  

    class ObSysFunRawExpr : public ObRawExpr
    {
    public:
      ObSysFunRawExpr() {}
      virtual ~ObSysFunRawExpr() {}
      ObRawExpr* get_param_expr(int32_t index) const
      {
        ObRawExpr* expr = NULL;
        if (index >= 0 || index < exprs_.size())
          expr = exprs_[index];
        return expr;
      }
      int add_param_expr(ObRawExpr *expr) { return exprs_.push_back(expr); }
      void set_func_name(const common::ObString& name) { func_name_ = name; }
      const common::ObString& get_func_name() { return func_name_; }
      virtual int fill_sql_expression(
          ObSqlExpression& inter_expr,
          ObTransformer *transformer = NULL,
          ObLogicalPlan *logical_plan = NULL,
          ObPhysicalPlan *physical_plan = NULL) const;
      void print(FILE* fp, int32_t level) const;

    private:
      common::ObString func_name_;
      common::ObVector<ObRawExpr*> exprs_;
    };  

    class ObSqlRawExpr
    {
    public:
      ObSqlRawExpr();
      ObSqlRawExpr(
          uint64_t expr_id, 
          uint64_t table_id = oceanbase::common::OB_INVALID_ID,
          uint64_t column_id = oceanbase::common::OB_INVALID_ID, 
          ObRawExpr* expr = NULL);
      virtual ~ObSqlRawExpr() {}
      uint64_t get_expr_id() const { return expr_id_; }
      uint64_t get_column_id() const { return column_id_; }
      uint64_t get_table_id() const { return table_id_; }
      void set_expr_id(uint64_t expr_id) { expr_id_ = expr_id; }
      void set_column_id(uint64_t column_id) { column_id_ = column_id; }
      void set_table_id(uint64_t table_id) { table_id_ = table_id; }
      void set_expr(ObRawExpr* expr) { expr_ = expr; }
      void set_tables_set(const ObBitSet tables_set) { tables_set_ = tables_set; }
      void set_applied(bool is_applied) { is_apply_ = is_applied; }
      void set_contain_aggr(bool contain_aggr) { contain_aggr_ = contain_aggr; }
      bool is_apply() const { return is_apply_; }
      bool is_contain_aggr() const { return contain_aggr_; }
      ObRawExpr* get_expr() const { return expr_; }
      const ObBitSet& get_tables_set() const { return tables_set_; }
      ObBitSet& get_tables_set() { return tables_set_; }
      const common::ObObjType get_result_type() const { return expr_->get_result_type(); }
      int fill_sql_expression(
          ObSqlExpression& inter_expr,
          ObTransformer *transformer = NULL,
          ObLogicalPlan *logical_plan = NULL,
          ObPhysicalPlan *physical_plan = NULL);
      void print(FILE* fp, int32_t level, int32_t index = 0) const;

    private:
      uint64_t  expr_id_;
      uint64_t  table_id_;
      uint64_t  column_id_;
      bool      is_apply_;
      bool      contain_aggr_;
      ObBitSet  tables_set_;
      ObRawExpr*  expr_;
    };  
      
  }
   
}

#endif //OCEANBASE_SQL_RAWEXPR_H_



