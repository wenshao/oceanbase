#ifndef OCEANBASE_SQL_MYSCHEMA_H_
#define OCEANBASE_SQL_MYSCHEMA_H_
#include "common/ob_string.h"
//#include "common/ob_string_buf.h"
//#include "common/ob_vector.h"

namespace oceanbase
{
  namespace sql
  {
    class MySchema
    {
    public:
      static char* mallocObStrToChar(oceanbase::common::ObString& obStr);
      static uint64_t getMySchemaId(oceanbase::common::ObString& sName);
    };
  }
}

#endif //OCEANBASE_SQL_MYSCHEMA_H_

