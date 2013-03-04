#ifndef OCEANBASE_SQL_INSERTSTMT_H_
#define OCEANBASE_SQL_INSERTSTMT_H_
#include "ob_stmt.h"
#include <stdio.h>
#include "common/ob_array.h"
#include "common/ob_string.h"
#include "common/ob_string_buf.h"

namespace oceanbase
{
  namespace sql
  {
    class ObInsertStmt : public ObStmt
    {
    public:
      ObInsertStmt(oceanbase::common::ObStringBuf* name_pool);
      virtual ~ObInsertStmt();

      uint64_t set_insert_table(uint64_t id)
      {
        if (id == oceanbase::common::OB_INVALID_ID)
          return oceanbase::common::OB_INVALID_ID;
        table_id_ = id;
        return id;
      }

      uint64_t set_insert_query(uint64_t id)
      {
        sub_query_id_ = id;
        return id;
      }

      int add_value_row(oceanbase::common::ObArray<uint64_t>& value_row)
      {
        return value_vectors_.push_back(value_row);
      }

      void print(FILE* fp, int32_t level, int32_t index);

    private:
      uint64_t   table_id_;
      uint64_t   sub_query_id_;
      oceanbase::common::ObArray<oceanbase::common::ObArray<uint64_t> > value_vectors_;
      
    };
  }
}

#endif //OCEANBASE_SQL_INSERTSTMT_H_

