#ifndef _MIXED_TEST_UTILS_
#define _MIXED_TEST_UTILS_
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>

#define TIMEOUT_US 10000000

#define SEED_START 1

#define DEFAULT_ROW_NUM_PER_MGET 256
#define DEFAULT_SUFFIX_NUM_PER_PREFIX 32

#define C_TIME_COLUMN_NAME "c_time"
#define C_TIME_COLUMN_ID (OB_CREATE_TIME_COLUMN_ID * 1UL)
#define M_TIME_COLUMN_NAME "m_time"
#define M_TIME_COLUMN_ID (OB_MODIFY_TIME_COLUMN_ID * 1UL)

#define SEED_COLUMN_NAME "seed_column" // per-row
#define SEED_COLUMN_ID 48UL

#define ROWKEY_INFO_COLUMN_NAME "rowkey_info" // per-prefix-stat
#define ROWKEY_INFO_COLUMN_ID 49UL
#define ROWKEY_INFO_ROWKEY "#rowkey_info@"

#define CELL_NUM_COLUMN_NAME "cell_num" // per-row
#define CELL_NUM_COLUMN_ID 50UL

#define SUFFIX_LENGTH_COLUMN_NAME "suffix_length" // per-prefix-start
#define SUFFIX_LENGTH_COLUMN_ID 51UL

#define SUFFIX_NUM_COLUMN_NAME "suffix_num" // per-prefix-start
#define SUFFIX_NUM_COLUMN_ID 52UL

#define PREFIX_END_COLUMN_NAME "prefix_end" // per-prefix-start
#define PREFIX_END_COLUMN_ID 53UL

#define META_COLUMN_NUM 8

namespace oceanbase
{
  namespace common
  {
    class ObSchema;
    class ObCellInfo;
    class ObSchemaManager;
  }
  namespace updateserver
  {
    class MemTank;
  }
}
class CellinfoBuilder;
class ClientWrapper;

extern int64_t range_rand(int64_t start, int64_t end, int64_t rand);

extern void build_string(char *buffer, int64_t len, int64_t seed);

extern oceanbase::common::ObCellInfo *copy_cell(oceanbase::updateserver::MemTank &mem_tank,
                                                const oceanbase::common::ObCellInfo *ci);

extern void trans_name2id(oceanbase::common::ObCellInfo &ci,
                          const oceanbase::common::ObSchema &schema);

extern int fetch_schema(const char *schema_addr,
                        const int64_t schema_port,
                        oceanbase::common::ObSchemaManager &schema_mgr);

extern bool get_check_row(const oceanbase::common::ObSchema &schema,
                          const oceanbase::common::ObString &row_key,
                          CellinfoBuilder &cb,
                          ClientWrapper &client,
                          const int64_t table_start_version,
                          const bool using_id);

template <typename T>
int copy(T &src, T &dst)
{
  int ret = oceanbase::common::OB_SUCCESS;
  int64_t size = src.get_serialize_size();
  char *buffer = (char*)oceanbase::common::ob_malloc(size);
  if (NULL == buffer)
  {
    ret = oceanbase::common::OB_ERROR;
  }
  else
  {
    int64_t pos = 0;
    ret = src.serialize(buffer, size, pos);
    if (oceanbase::common::OB_SUCCESS == ret)
    {
      pos = 0;
      ret = dst.deserialize(buffer, size, pos);
    }
    oceanbase::common::ob_free(buffer);
  }
  return ret;
}

char* my_str_join(int n, char** parts);
#endif // _MIXED_TEST_UTILS_
