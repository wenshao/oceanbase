#ifndef OCEANBASE_SQL_PARSE_TOOLS_H_
#define OCEANBASE_SQL_PARSE_TOOLS_H_

#include "parse_node.h"
#include "sql_parser.tab.h"
#include "common/ob_string.h"
#include "common/ob_string_buf.h"
#include "common/ob_tsi_factory.h"

using namespace oceanbase::common;

inline int alloc_str_by_char(
    const char*  str,
    ObString&    dest_obstr,
    ObStringBuf* name_pool)
{
  int ret = OB_SUCCESS;
  if (str == NULL)
  {
    dest_obstr.assign_ptr(NULL, 0);
    return ret;
  }

  if (!name_pool)
    name_pool = GET_TSI_MULT(ObStringBuf, 1);

  ObString temp;
  temp.assign_ptr((char*)str, static_cast<int32_t>(strlen(str)));
  ret = name_pool->write_string(temp, &dest_obstr);
  return ret;
}

inline int alloc_str_by_obstring(
    const ObString&    src_obstr,
    ObString&    dest_obstr,
    ObStringBuf* name_pool)
{
  int ret = OB_SUCCESS;
  if (!name_pool)
    name_pool = GET_TSI_MULT(ObStringBuf, 1);

  ret = name_pool->write_string(src_obstr, &dest_obstr);
  return ret;
}


#endif //OCEANBASE_SQL_PARSE_TOOLS_H_
