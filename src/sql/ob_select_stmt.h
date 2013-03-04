#ifndef OCEANBASE_SQL_SELECTSTMT_H_
#define OCEANBASE_SQL_SELECTSTMT_H_

#include "parse_tools.h"
#include "ob_stmt.h"
#include "ob_raw_expr.h"
#include "common/ob_string.h"
#include "common/ob_string_buf.h"
#include "common/ob_array.h"
#include "common/ob_vector.h"

namespace oceanbase
{
  namespace sql
  {
    struct SelectItem
    {
      uint64_t   expr_id_;
      bool       is_real_alias_;
      common::ObString alias_name_;
      common::ObString expr_name_;
      common::ObObjType type_;
    };

    struct OrderItem
    {
      enum OrderType
      {
        ASC,
        DESC
      };

      uint64_t   expr_id_;
      OrderType  order_type_;
    };

    struct JoinedTable
    {
      enum JoinType
      {
        T_FULL,
        T_LEFT,
        T_RIGHT,
        T_INNER,
      };

      int add_table_id(uint64_t tid) { return table_ids_.push_back(tid); }
      int add_join_type(JoinType type) { return join_types_.push_back(type); }
      int add_expr_id(uint64_t eid) { return expr_ids_.push_back(eid); }
      void set_joined_tid(uint64_t tid) { joined_table_id_ = tid; }

      uint64_t   joined_table_id_;
      common::ObArray<uint64_t>  table_ids_;
      common::ObArray<uint64_t>  join_types_;
      common::ObArray<uint64_t>  expr_ids_;
    };

    struct FromItem
    {
      uint64_t   table_id_;
      // false: it is the real table id
      // true: it is the joined table id
      bool      is_joined_;
    };
  }

  namespace common
  {
    template <>
      struct ob_vector_traits<oceanbase::sql::SelectItem>
      {
        typedef oceanbase::sql::SelectItem* pointee_type;
        typedef oceanbase::sql::SelectItem value_type;
        typedef const oceanbase::sql::SelectItem const_value_type;
        typedef value_type* iterator;
        typedef const value_type* const_iterator;
        typedef int32_t difference_type;
      };

    template <>
      struct ob_vector_traits<oceanbase::sql::OrderItem>
      {
        typedef oceanbase::sql::OrderItem* pointee_type;
        typedef oceanbase::sql::OrderItem value_type;
        typedef const oceanbase::sql::OrderItem const_value_type;
        typedef value_type* iterator;
        typedef const value_type* const_iterator;
        typedef int32_t difference_type;
      };

    template <>
      struct ob_vector_traits<oceanbase::sql::FromItem>
      {
        typedef oceanbase::sql::FromItem* pointee_type;
        typedef oceanbase::sql::FromItem value_type;
        typedef const oceanbase::sql::FromItem const_value_type;
        typedef value_type* iterator;
        typedef const value_type* const_iterator;
        typedef int32_t difference_type;
      };
  }

  namespace sql
  {
    class ObSelectStmt : public ObStmt
    {
    public:
      enum SetOperator
      {
        UNION,
        INTERSECT,
        EXCEPT,
        NONE,
      };
      
      ObSelectStmt(common::ObStringBuf* name_pool);
      virtual ~ObSelectStmt();

      int32_t get_select_item_size() { return select_items_.size(); }
      int32_t get_from_item_size() { return from_items_.size(); }
      int32_t get_joined_table_size() { return joined_tables_.size(); }
      int32_t get_group_expr_size() { return group_expr_ids_.size(); }
      int32_t get_agg_fun_size() { return agg_func_ids_.size(); }
      int32_t get_having_expr_size() { return having_expr_ids_.size(); }
      int32_t get_order_item_size() { return order_items_.size(); }
      void assign_distinct() { is_distinct_ = true; }
      void assign_all() { is_distinct_ = false; }
      void assign_set_op(SetOperator op) { set_op_ = op; }
      void assign_set_distinct() { is_set_distinct_ = true; }
      void assign_set_all() { is_set_distinct_ = false; }
      void assign_left_query_id(uint64_t lid) { left_query_id_ = lid; }
      void assign_right_query_id(uint64_t rid) { right_query_id_ = rid; }
      int check_alias_name(ResultPlan& result_plan, const common::ObString& sAlias) const;
      uint64_t get_alias_expr_id(common::ObString& alias_name);
      uint64_t generate_joined_tid() { return gen_joined_tid_--; }
      uint64_t get_left_query_id() { return left_query_id_; }
      uint64_t get_right_query_id() { return right_query_id_; }
      int64_t get_limit() { return limit_; }
      int64_t get_offset() { return offset_; }
      bool is_distinct() { return is_distinct_; }
      bool is_set_distinct() { return is_set_distinct_; }
      SetOperator get_set_op() { return set_op_; }
      JoinedTable* get_joined_table(uint64_t table_id);

      const SelectItem& get_select_item(int32_t index) const
      { 
        OB_ASSERT(0 <= index && index < select_items_.size()); 
        return select_items_[index]; 
      }
      
      const FromItem& get_from_item(int32_t index) const
      {
        OB_ASSERT(0 <= index && index < from_items_.size()); 
        return from_items_[index];
      }

      const OrderItem& get_order_item(int32_t index) const
      { 
        OB_ASSERT(0 <= index && index < order_items_.size()); 
        return order_items_[index]; 
      }

      uint64_t get_group_expr_id(int32_t index)
      {
        OB_ASSERT(0 <= index && index < group_expr_ids_.size()); 
        return group_expr_ids_[index];
      }

      uint64_t get_agg_expr_id(int32_t index)
      {
        OB_ASSERT(0 <= index && index < agg_func_ids_.size()); 
        return agg_func_ids_[index];
      }

      uint64_t get_having_expr_id(int32_t index)
      {
        OB_ASSERT(0 <= index && index < having_expr_ids_.size()); 
        return having_expr_ids_[index];
      }

      common::ObVector<uint64_t>& get_having_exprs()
      {
        return having_expr_ids_;
      }

      int add_group_expr(uint64_t expr_id) 
      { 
        return group_expr_ids_.push_back(expr_id);
      }

      int add_agg_func(uint64_t expr_id) 
      { 
        return agg_func_ids_.push_back(expr_id);
      }

      int add_from_item(uint64_t tid, bool is_joined = false)
      {
        int ret = OB_SUCCESS;
        if (tid != OB_INVALID_ID)
        {
          FromItem item;
          item.table_id_ = tid;
          item.is_joined_ = is_joined;
          ret = from_items_.push_back(item);
        }
        else
        {
          ret = OB_ERR_ILLEGAL_ID;
        }
        return ret;
      }

      int add_order_item(OrderItem& order_item)
      {
        return order_items_.push_back(order_item);
      }

      int add_joined_table(JoinedTable* pJoinedTable)
      {
        return joined_tables_.push_back(pJoinedTable);
      }

      int add_having_expr(uint64_t expr_id) 
      { 
        return having_expr_ids_.push_back(expr_id);
      }

      void set_limit(int64_t limit) 
      { 
        limit_ = limit;
      }
      
      void set_offset(int64_t offset) 
      { 
        offset_ = offset;
      }

      int check_having_ident(
          ResultPlan& result_plan,
          ObString& column_name, 
          TableItem* table_item, 
          ObRawExpr*& ret_expr) const;

      int add_select_item(
          uint64_t eid, 
          bool is_real_alias, 
          const common::ObString& alias_name,
          const common::ObString& expr_name,
          const common::ObObjType& type);

      int copy_select_items(ObSelectStmt* select_stmt);
      void print(FILE* fp, int32_t level, int32_t index = 0);

    private:
      /* These fields are only used by normal select */
      bool    is_distinct_;
      common::ObVector<SelectItem>   select_items_;
      common::ObVector<FromItem>     from_items_;
      common::ObVector<JoinedTable*> joined_tables_;
      common::ObVector<uint64_t>     group_expr_ids_;
      common::ObVector<uint64_t>     having_expr_ids_;
      common::ObVector<uint64_t>     agg_func_ids_;
      
      /* These fields are only used by set select */
      SetOperator set_op_;
      bool        is_set_distinct_;
      uint64_t    left_query_id_;
      uint64_t    right_query_id_;

      /* These fields are used by both normal select and set select */
      common::ObVector<OrderItem>  order_items_;

      /* -1 means no limit */
      int64_t    limit_;
      int64_t    offset_;

      uint64_t    gen_joined_tid_;
    };
  }
}

#endif //OCEANBASE_SQL_SELECTSTMT_H_

