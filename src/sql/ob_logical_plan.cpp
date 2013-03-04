#include "ob_logical_plan.h"
#include "ob_select_stmt.h"
#include "ob_delete_stmt.h"
#include "ob_insert_stmt.h"
#include "ob_update_stmt.h"
#include "parse_malloc.h"

namespace oceanbase
{
  namespace sql
  {
    using namespace oceanbase::sql;
    using namespace oceanbase::common;

    ObLogicalPlan::ObLogicalPlan(ObStringBuf* name_pool)
      : name_pool_(name_pool)
    {
      // from valid max id desc
      new_gen_tid_ = UINT64_MAX - 2;
      new_gen_cid_ = UINT64_MAX - 2;
      new_gen_qid_ = 1;
      new_gen_eid_ = 1;
    }
    
    ObLogicalPlan::~ObLogicalPlan()
    {
      for(int32_t i = 0; i < stmts_.size(); ++i)
      {
        //delete stmts_[i];
        stmts_[i]->~ObStmt();
        parse_free(stmts_[i]);
      }
      stmts_.clear();

      for(int32_t i = 0; i < exprs_.size(); ++i)
      {
        //delete exprs_[i];
        exprs_[i]->~ObSqlRawExpr();
        parse_free(exprs_[i]);
      }
      exprs_.clear();

      for(int32_t i = 0; i < raw_exprs_store_.size(); ++i)
      {
        if (raw_exprs_store_[i])
        {
          raw_exprs_store_[i]->~ObRawExpr();
          parse_free(exprs_[i]);
        }
      }
      raw_exprs_store_.clear();
    }
    
    ObStmt* ObLogicalPlan::get_query(uint64_t query_id) const
    {
      ObStmt *stmt = NULL;
      int32_t num = stmts_.size();
      for (int32_t i = 0; i < num; i++)
      {
        if (stmts_[i]->get_query_id() == query_id)
        {
          stmt = stmts_[i];
          break;
        }
      }
      return stmt;
    }

    ObSelectStmt* ObLogicalPlan::get_select_query(uint64_t query_id) const
    {
      ObSelectStmt *select_stmt = NULL;
      int32_t num = stmts_.size();
      for (int32_t i = 0; i < num; i++)
      {
        if (stmts_[i]->get_query_id() == query_id)
        {
          select_stmt = static_cast<ObSelectStmt*>(stmts_[i]);
          break;
        }
      }
      return select_stmt;
    }
    
    ObSqlRawExpr* ObLogicalPlan::get_expr(uint64_t expr_id) const
    {
      ObSqlRawExpr *expr = NULL;
      int32_t num = exprs_.size();
      for (int32_t i = 0; i < num; i++)
      {
        if (exprs_[i]->get_expr_id() == expr_id)
        {
          expr = exprs_[i];
          break;
        }
      }
      return expr;
    }

    int ObLogicalPlan::fill_result_set(ObResultSet& result_set)
    {
      int ret = OB_SUCCESS;
      
      ObResultSet::Field field;
      switch(stmts_[0]->get_stmt_type())
      {
        case ObStmt::T_SELECT:
        {
          result_set.set_is_with_rows(false);
          result_set.set_affected_rows(0);
          result_set.set_warning_count(0);
          result_set.set_message("");

          ObSelectStmt *select_stmt = static_cast<ObSelectStmt*>(stmts_[0]);
          int32_t size = select_stmt->get_select_item_size();
          for (int32_t i = 0; ret == OB_SUCCESS && i < size; i++)
          {
            const SelectItem& select_item = select_stmt->get_select_item(i);
            if (select_item.alias_name_.length() > 0)
            {
              if ((ret = alloc_str_by_obstring(select_item.alias_name_, field.cname_, name_pool_)) != OB_SUCCESS)
                break;
            }
            else
            {
              if ((ret = alloc_str_by_obstring(select_item.expr_name_, field.cname_, name_pool_)) != OB_SUCCESS)
                break;
            }

            ObSqlRawExpr* sql_expr = get_expr(select_item.expr_id_);
            if (sql_expr == NULL)
            {
              ret = OB_ERR_ILLEGAL_ID;
              break;
            }
            field.type_.set_type(sql_expr->get_result_type());
            ObRawExpr* expr = sql_expr->get_expr();
            if (select_stmt->get_set_op() != ObSelectStmt::NONE)
            {
              if ((ret = alloc_str_by_obstring(select_item.expr_name_, field.org_cname_, name_pool_)) != OB_SUCCESS)
                break;
            }
            else if (expr->get_expr_type() == T_REF_COLUMN)
            {
              ObBinaryRefRawExpr *column_expr = static_cast<ObBinaryRefRawExpr*>(expr);
              uint64_t table_id = column_expr->get_first_ref_id();
              uint64_t column_id = column_expr->get_second_ref_id();
              if (table_id != OB_INVALID_ID)
              {
                ColumnItem *column_item = select_stmt->get_column_item_by_id(table_id, column_id);
                if (column_item == NULL)
                {
                  ret = OB_ERR_ILLEGAL_ID;
                  break;
                }
                ret = alloc_str_by_obstring(column_item->column_name_, field.org_cname_, name_pool_);
                if (ret != OB_SUCCESS)
                  break;
                TableItem *table_item = select_stmt->get_table_item_by_id(table_id);
                if (table_item == NULL)
                {
                  ret = OB_ERR_ILLEGAL_ID;
                  break;
                }
                if (table_item->alias_name_.length() > 0)
                  ret = alloc_str_by_obstring(table_item->alias_name_, field.tname_, name_pool_);
                else
                  ret = alloc_str_by_obstring(table_item->table_name_, field.tname_, name_pool_);
                if (ret != OB_SUCCESS)
                  break;
                ret = alloc_str_by_obstring(table_item->table_name_, field.org_tname_, name_pool_);
                if (ret != OB_SUCCESS)
                  break;
              }
            }
            ret = result_set.add_field_column(field);
            if (ret != OB_SUCCESS)
              break;

            field.cname_.assign(NULL, 0);
            field.org_cname_.assign(NULL, 0);
            field.tname_.assign(NULL, 0);
            field.org_tname_.assign(NULL, 0);
            field.type_.set_type(ObMinType);
          }
          break;
        }
        case ObStmt::T_INSERT:
        {
          break;
        }
        case ObStmt::T_DELETE:
        {
          break;
        }
        case ObStmt::T_UPDATE:
        {
          break;
        }
        default:
          break;
      }

      return ret;
    }
    
    void ObLogicalPlan::print(FILE* fp, int32_t level) const
    {
      int32_t i;
      fprintf(fp, "<logical_plan Begin>\n");
      fprintf(fp, "    <StmtList Begin>\n");
      for (i = 0; i < stmts_.size(); i ++)
      {
        ObStmt* stmt = stmts_[i];
        stmt->print(fp, level + 2, i);
      }
      fprintf(fp, "    <StmtList End>\n");
      fprintf(fp, "    <ExprList Begin>\n");
      for (i = 0; i < exprs_.size(); i ++)
      {
        ObSqlRawExpr* sql_expr = exprs_[i];
        sql_expr->print(fp, level + 2, i);
      }
      fprintf(fp, "    <ExprList End>\n");
      fprintf(fp, "<logical_plan End>\n");
    }
  }
}

