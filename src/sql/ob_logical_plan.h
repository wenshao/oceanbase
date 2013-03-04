#ifndef OCEANBASE_SQL_LOGICALPLAN_H_
#define OCEANBASE_SQL_LOGICALPLAN_H_
#include "parse_node.h"
#include "ob_raw_expr.h"
#include "ob_stmt.h"
#include "ob_select_stmt.h"
#include "ob_result_set.h"
#include "common/ob_string.h"
#include "common/ob_string_buf.h"
#include "common/ob_vector.h"

namespace oceanbase
{
  namespace sql
  {
    class ObLogicalPlan
    {
    public:
      explicit ObLogicalPlan(oceanbase::common::ObStringBuf* name_pool);
      virtual ~ObLogicalPlan();
      
      oceanbase::common::ObStringBuf* get_name_pool() const
      {
        return name_pool_;
      }

      ObStmt* get_query(uint64_t query_id) const;

      ObStmt* get_main_stmt()
      {
       ObStmt *stmt = NULL;
        if (stmts_.size() > 0)
          stmt = stmts_[0];
        return stmt;
      }

      ObSelectStmt* get_select_query(uint64_t query_id) const;
     
      ObSqlRawExpr* get_expr(uint64_t expr_id) const;

      int add_query(ObStmt* stmt)
      {
        int ret = common::OB_SUCCESS;
        if (!stmt || stmts_.push_back(stmt) != common::OB_SUCCESS)
          ret = common::OB_ERROR;
        return ret;
      }

      int add_expr(ObSqlRawExpr* expr)
      {
        int ret = common::OB_SUCCESS;
        if (!expr || exprs_.push_back(expr) != common::OB_SUCCESS)
          ret = common::OB_ERROR;
        return ret;
      }

      // Just a storage, only need to add raw expression
      int add_raw_expr(ObRawExpr* expr)
      {
        return raw_exprs_store_.push_back(expr);
      }

      int fill_result_set(ObResultSet& result_set);
      
      uint64_t generate_table_id()
      {
        return new_gen_tid_--;
      }

      uint64_t generate_column_id()
      {
        return new_gen_cid_--;
      }
      
      uint64_t generate_expr_id()
      {
        return new_gen_eid_++;
      }

      uint64_t generate_query_id()
      {
        return new_gen_qid_++;
      }

      void print(FILE* fp = stderr, int32_t level = 0) const;
      
    protected:
      oceanbase::common::ObStringBuf* name_pool_;
      
    private:
      oceanbase::common::ObVector<ObStmt*> stmts_;
      oceanbase::common::ObVector<ObSqlRawExpr*> exprs_;
      oceanbase::common::ObVector<ObRawExpr*> raw_exprs_store_;
      uint64_t  new_gen_tid_;
      uint64_t  new_gen_cid_;
      uint64_t  new_gen_qid_;
      uint64_t  new_gen_eid_;
    };
  }
}

#endif //OCEANBASE_SQL_LOGICALPLAN_H_

