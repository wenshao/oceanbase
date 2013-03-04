#include "common/utility.h"
#include "ob_read_param_modifier.h"
#include "ob_ms_scan_cell_stream.h"

using namespace oceanbase::common;
using namespace oceanbase::mergeserver;

ObMSScanCellStream::ObMSScanCellStream(ObMergerRpcProxy * rpc_proxy): ObMSCellStream(rpc_proxy)
{
  finish_ = false;
  reset_inner_stat();
}

ObMSScanCellStream::~ObMSScanCellStream()
{
}

int ObMSScanCellStream::next_cell(void)
{
  int ret = OB_SUCCESS;
  if (NULL == scan_param_)
  {
    TBSYS_LOG(DEBUG, "%s", "check scan param is null");
    ret = OB_ITER_END;
  }
  else if (!check_inner_stat())
  {
    TBSYS_LOG(ERROR, "%s", "check inner stat failed");
    ret = OB_INNER_STAT_ERROR;
  }
  else
  {
    // do while until get scan data or finished or error occured
    do
    {
      ret = get_next_cell();
      if (OB_ITER_END == ret)
      {
        TBSYS_LOG(DEBUG, "%s", "scan cell finish");
        break;
      }
      else if (ret != OB_SUCCESS)
      {
        TBSYS_LOG(WARN, "scan next cell failed:ret[%d]", ret);
        break;
      }
    } while (cur_result_.is_empty() && (OB_SUCCESS == ret));
  }
  return ret;
}

int ObMSScanCellStream::scan(const ObScanParam & param, const ObMergerTabletLocation & cs_addr)
{
  int ret = OB_SUCCESS;
  const ObRange * range = param.get_range();
  if (NULL == range)
  {
    TBSYS_LOG(ERROR, "%s", "check scan param failed");
    ret = OB_INPUT_PARAM_ERROR;
  }
  else
  {
    cs_addr_ = cs_addr;
    reset_inner_stat();
    scan_param_ = &param;
  }
  return ret;
}

int ObMSScanCellStream::get_next_cell(void)
{
  int ret = OB_SUCCESS;
  if (finish_)
  {
    ret = OB_ITER_END;
    TBSYS_LOG(DEBUG, "%s", "check next already finish");
  }
  else
  {
    last_cell_ = cur_cell_;
    ret = cur_result_.next_cell();
    // need rpc call for new data
    if (OB_ITER_END == ret)
    {
      // TODO change the realization for param copy only range need modified
      ObScanParam param = *scan_param_;
      // scan the new data only by one rpc call
      ret = scan_row_data(param);
      if (OB_ITER_END == ret)
      {
        TBSYS_LOG(DEBUG, "%s", "finish the scan");
      }
      else if (ret != OB_SUCCESS)
      {
        TBSYS_LOG(WARN, "scan server data failed:ret[%d]", ret);
      }
    }
    else if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "next cell failed:ret[%d]", ret);
    }
  }
  return ret;
}

int ObMSScanCellStream::scan_row_data(ObScanParam & param)
{
  int ret = OB_SUCCESS;
  // step 1. modify the scan param for next scan rpc call
  if (!ObMSCellStream::first_rpc_)
  {
    bool is_fullfill = false;
    // check already finish scan
    ret = check_finish_scan(param, is_fullfill);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "check finish scan failed:ret[%d]", ret);
    }
    else if (finish_)
    {
      ret = OB_ITER_END;
    }
    else
    {
      // construct the next scan param
      ret = get_next_param(*scan_param_, cur_result_, &param, range_buffer_);
      if (ret != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR, "modify scan param failed:ret[%d]", ret);
      }
    }
  }

  // step 2. scan data according the new scan param
  if (OB_SUCCESS == ret)
  {
    ret = ObMSCellStream::rpc_scan_row_data(param);
    if ((ret != OB_SUCCESS) && (ret != OB_ITER_END))
    {
      TBSYS_LOG(WARN, "scan row data failed:ret[%d]", ret);
    }
  }
  return ret;
}


int ObMSScanCellStream::check_finish_scan(const ObScanParam & param, bool & is_fullfill)
{
  int ret = OB_SUCCESS;
  is_fullfill = false;
  if (!finish_)
  {
    int64_t item_count = 0;
    ret = ObMSCellStream::cur_result_.get_is_req_fullfilled(is_fullfill, item_count);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "get scanner full filled status failed:ret[%d]", ret);
    }
    else if (is_fullfill)
    {
      ObRange result_range;
      ret = ObMSCellStream::cur_result_.get_range(result_range);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "get result range failed:ret[%d]", ret);
      }
      else
      {
        finish_ = is_finish_scan(param, result_range);
      }
    }
  }
  return ret;
}


