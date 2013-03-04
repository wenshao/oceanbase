
#ifndef OCEANBASE_TEST_UTILITY_H_
#define OCEANBASE_TEST_UTILITY_H_

#include "stdint.h"
#include "string.h"
#include "errno.h"
#include "common/ob_define.h"
#include "tbsys.h"

namespace oceanbase
{
  using namespace common;
  namespace sql
  {
    namespace test
    {
      void split(char *line, const char *delimiters, char **tokens, int32_t &count);

      template<class T>
      int convert(char *str, T &value)
      {
        int ret = OB_SUCCESS;
        if(NULL == str)
        {
          ret = OB_ERROR;
          TBSYS_LOG(WARN, "str is null");
        }
        else
        {
          errno = 0;
          value = atoi(str);
          if(0 != errno)
          {
            ret = OB_ERROR;
            TBSYS_LOG(WARN, "convert [%s] to int fail", str);
          }
        }
        return ret;
      }
    }
  }
}

#endif /* OCEANBASE_TEST_UTILITY_H_ */

