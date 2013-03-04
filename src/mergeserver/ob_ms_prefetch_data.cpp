#include "ob_ms_prefetch_data.h"
#include "ob_ms_version_proxy.h"
#include "ob_merge_join_agent_imp.h"
#include "ob_ms_get_cell_stream.h"
#include "ob_mutator_param_decoder.h"

#include "common/utility.h"
#include "common/ob_schema.h"
#include "common/ob_mutator.h"
#include "common/ob_scanner.h"
#include "common/ob_prefetch_data.h"
#include "common/ob_read_common_data.h"

using namespace oceanbase::common;
using namespace oceanbase::mergeserver;

ObMergerPrefetchData::ObMergerPrefetchData(const int64_t limit, ObMergerRpcProxy & rpc,
    ObMergerVersionProxy & version, const ObSchemaManagerV2 & schema)
    :memory_limit_(limit), cur_version_(-1), rpc_(rpc), version_(version), schema_(schema)
{
}

ObMergerPrefetchData::~ObMergerPrefetchData()
{
}

int ObMergerPrefetchData::prefetch(const int64_t timeout, ObGetMergeJoinAgentImp & agent,
    ObGetParam & param, ObMutator & mutator)
{
  // modify prefetch param's version range
  int ret = modify_prefetch_param(param);
  if (ret != OB_SUCCESS)
  {
    TBSYS_LOG(WARN, "modify prefetch param failed:ret[%d]", ret);
  }
  else
  {
    ObMSGetCellStream ups_stream(&rpc_);
    ObMSGetCellStream ups_join_stream(&rpc_);
    ret = agent.set_request_param(timeout, param, ups_stream, ups_join_stream, schema_, memory_limit_);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "set request param failed:ret[%d]", ret);
    }
    else
    {
      // fill data for prefetch from agent
      ret = fill_prefetch_data(agent, mutator.get_prefetch_data());
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "check get cell and fetch data failed:ret[%d]", ret);
      }
      // TODO: check result fullfilled if false return error
    }
  }
  return ret;
}

int ObMergerPrefetchData::modify_prefetch_param(ObGetParam & param)
{
  ObVersionRange range;
  int ret = version_.get_version(range.end_version_.version_);
  if ((ret != OB_SUCCESS) || (range.end_version_ <= 0))
  {
    TBSYS_LOG(WARN, "get last version failed:version[%ld], ret[%d]", (int64_t)range.end_version_, ret);
  }
  else
  {
    cur_version_ = range.end_version_;
    range.border_flag_.set_min_value();
    range.border_flag_.set_inclusive_end();
    // maybe not need someday
    param.set_is_result_cached(true);
    param.set_is_read_consistency(true);
    param.set_version_range(range);
  }
  return ret;
}

int ObMergerPrefetchData::fill_prefetch_data(ObGetMergeJoinAgentImp & agent, ObPrefetchData & data)
{
  data.reset();
  int ret = OB_SUCCESS;
  data.get().set_data_version(cur_version_);
  ObCellInfo * cur_cell = NULL;
  while (OB_SUCCESS == (ret = agent.next_cell()))
  {
    ret = agent.get_cell(&cur_cell);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "get cell from agent failed:ret[%d]", ret);
      break;
    }
    else
    {
      ret = data.add_cell(*cur_cell);
      if (ret != OB_SUCCESS)
      {
        TBSYS_LOG(WARN, "add cell to prefetch data failed:ret[%d]", ret);
        break;
      }
      else if (TBSYS_LOGGER._level >= TBSYS_LOG_LEVEL_DEBUG)
      {
        TBSYS_LOG(DEBUG, "tableid:%lu, rowkey:%.*s, column_id:%lu, ext:%ld, type:%d",
            cur_cell->table_id_, cur_cell->row_key_.length(), cur_cell->row_key_.ptr(),
            cur_cell->column_id_, cur_cell->value_.get_ext(),cur_cell->value_.get_type());
        cur_cell->value_.dump();
        hex_dump(cur_cell->row_key_.ptr(), cur_cell->row_key_.length());
      }
    }
  }
  // fill all data succ
  if (OB_ITER_END == ret)
  {
    ret = OB_SUCCESS;
  }
  return ret;
}


