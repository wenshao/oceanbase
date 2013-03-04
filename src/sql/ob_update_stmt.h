#ifndef OCEANBASE_SQL_UPDATESTMT_H_
#define OCEANBASE_SQL_UPDATESTMT_H_
#include "ob_stmt.h"
#include <stdio.h>
#include "common/ob_array.h"
#include "common/ob_string.h"
#include "common/ob_string_buf.h"

namespace oceanbase
{
  namespace sql
  {
    class ObUpdateStmt : public ObStmt
    {
    public:
      ObUpdateStmt(oceanbase::common::ObStringBuf* name_pool);
      virtual ~ObUpdateStmt();

      uint64_t set_update_table(uint64_t id)
      {
        if (id == oceanbase::common::OB_INVALID_ID)
          return oceanbase::common::OB_INVALID_ID;
        table_id_ = id;
        return id;
      }
      
      int add_update_column(uint64_t column_id)
      {
        int ret = common::OB_SUCCESS;
        if (column_id != oceanbase::common::OB_INVALID_ID)
          ret = update_columns_.push_back(column_id);
        return ret;
      }
      
      int add_update_expr(uint64_t expr_id)
      {
        int ret = common::OB_SUCCESS;
        if (expr_id == oceanbase::common::OB_INVALID_ID)
          ret = common::OB_ERROR;
        else
          ret = update_exprs_.push_back(expr_id);
        return ret;
      }

      void print(FILE* fp, int32_t level, int32_t index);

    private:
      uint64_t   table_id_;
      oceanbase::common::ObArray<uint64_t> update_columns_;
      oceanbase::common::ObArray<uint64_t> update_exprs_;        
    };
  }
}

#endif //OCEANBASE_SQL_UPDATESTMT_H_

