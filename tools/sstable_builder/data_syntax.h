#ifndef  OCEANBASE_CHUNKSERVER_DATA_SYNTAX_H_
#define OCEANBASE_CHUNKSERVER_DATA_SYNTAX_H_

#include "common/ob_object.h"

namespace oceanbase 
{
  namespace chunkserver 
  {
    const int MAX_COLUMS = 128;
    const int MAX_LINE_LEN = 16 * 1024;

    struct data_format
    {
      common::ObObjType type;
      int32_t len;
      int32_t column_id;
      int32_t index; //index in raw data, -1 means the item is new ,no data in raw data
    };

    enum RowKeyType 
    {
      INT8 = 0,
      INT16,
      INT32,
      INT64,
      VARCHAR,
      DATETIME
    };

    struct row_key_format
    {
      RowKeyType type;
      int32_t len;
      int32_t index;  //index in raw data
      int32_t flag;
    };
  } /* chunkserver */
} /* oceanbase */
#endif /*OCEANBASE_CHUNKSERVER_DATA_SYNTAX_H_*/

