
#include "ob_ms_cell_stream.h"
#include "common/ob_schema.h"
#include "common/ob_tablet_info.h"

using namespace oceanbase::common;
using namespace oceanbase::mergeserver;

ObMSCellStream::ObMSCellStream(ObMergerRpcProxy * rpc)
{
  reset_inner_stat();
  rpc_proxy_ = rpc;
}

ObMSCellStream::~ObMSCellStream()
{
}


int ObMSCellStream::scan(const ObScanParam &scan_param, const ObMergerTabletLocation &cs_addr)
{
  UNUSED(scan_param);
  UNUSED(cs_addr);
  TBSYS_LOG(ERROR, "%s", "not implement");
  return OB_ERROR;
}

void ObMSCellStream::reset()
{
  reset_inner_stat();
}


int ObMSCellStream::get(const ObReadParam &read_param, ObMSGetCellArray & get_cells, 
  const ObMergerTabletLocation &cs_addr)
{
  UNUSED(get_cells);
  UNUSED(cs_addr);
  UNUSED(read_param);
  TBSYS_LOG(ERROR, "%s", "not implement");
  return OB_ERROR;
}


int ObMSCellStream::check_scanner_result(const common::ObScanner & result)
{
  bool is_fullfill = false;
  int64_t item_count = 0;
  int ret = result.get_is_req_fullfilled(is_fullfill, item_count);
  if (ret != OB_SUCCESS)
  {
    TBSYS_LOG(WARN, "get request is fullfilled failed:ret[%d]", ret);
  }
  else
  {
    if ((false == is_fullfill) && result.is_empty())
    {
      TBSYS_LOG(ERROR, "check scanner result empty but not fullfill:fullfill[%d], item_count[%ld]",
          is_fullfill, item_count);
      ret = OB_ERROR;
    }
  }
  return ret;
}


int ObMSCellStream::get_cell(ObCellInfo** cell)
{
  int ret = OB_SUCCESS;
  if ((NULL == cell) && !check_inner_stat())
  {
    TBSYS_LOG(ERROR, "check input cell or inner stat failed:cell[%p]", cell);
    ret = OB_ERROR;
  }
  else
  {
    ret = cur_result_.get_cell(cell);
    if (OB_SUCCESS == ret)
    {
      cur_cell_ = *cell;
    }
    else
    {
      TBSYS_LOG(ERROR, "iterator get cell failed:ret[%d]", ret);
    }
  }
  return ret;
}


int ObMSCellStream::get_cell(ObCellInfo** cell, bool * is_row_changed)
{
  int ret = OB_SUCCESS;
  ret = cur_result_.get_cell(cell, is_row_changed);
  if (OB_SUCCESS != ret)
  {
    TBSYS_LOG(ERROR, "get cell failed:ret[%d]", ret);
  }
  else
  {
    cur_cell_ = *cell;
  }
  return ret;
}

int ObMSCellStream::rpc_scan_row_data(const ObScanParam & param)
{
  int ret = OB_SUCCESS;
  cur_result_.reset();

  ret = rpc_proxy_->ups_scan(cs_addr_, param, cur_result_);
  if (ret != OB_SUCCESS)
  {
    TBSYS_LOG(WARN, "scan server data failed:ret[%d]", ret);
  }
  else
  {
    ret = check_scanner_result(cur_result_);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "check scanner result failed:ret[%d]", ret);
    }
    else
    {
      first_rpc_ = false;
      ret = cur_result_.next_cell();
      if ((ret != OB_SUCCESS) && (ret != OB_ITER_END))
      {
        TBSYS_LOG(WARN, "get next cell failed:ret[%d]", ret);
      }
    }
  }
  return ret;
}

int ObMSCellStream::rpc_get_cell_data(const common::ObGetParam & param)
{
  int ret = OB_SUCCESS;
  cur_result_.reset();

  ret = rpc_proxy_->ups_get(cs_addr_, param, cur_result_);
  if (ret != OB_SUCCESS)
  {
    TBSYS_LOG(WARN, "get data through rpc call failed:ret[%d]", ret);
  }
  else
  {
    ret = check_scanner_result(cur_result_);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "check scanner result failed:ret[%d]", ret);
    }
    else
    {
      first_rpc_ = false;
      ret = cur_result_.next_cell();
      if (ret != OB_SUCCESS)
      {
        TBSYS_LOG(WARN, "check get ups first cell failed:ret[%d]", ret);
      }
    }
  }
  return ret;
}


