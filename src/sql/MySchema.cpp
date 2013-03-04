#include "MySchema.h"
#include "common/ob_malloc.h"
#include <assert.h>
#include <vector>

using namespace oceanbase::sql;
using namespace oceanbase::common;

// "MyTestSchema.ini" must be load above

char* MySchema::mallocObStrToChar(ObString& obStr)
{
  int32_t size = obStr.length();
  if (size == 0)
    return NULL;
  
  char* ptr = (char*)ob_malloc(size + 1);
  if (!ptr)
  {
    fprintf(stderr, "No more memorey!\n");
    exit(-1);
  }
  memcpy(ptr, obStr.ptr(), size);
  ptr[size] = '\0';

  return ptr;
}

uint64_t MySchema::getMySchemaId(ObString& sName)
{
  tbsys::CConfig& config = tbsys::CConfig::getCConfig();
  char* ptr = mallocObStrToChar(sName);
  uint64_t tableId = config.getInt(ptr, "table_id", 0);
  if (tableId == 0)
    tableId = OB_INVALID_ID;
  ob_free(ptr);

  return tableId;
}

