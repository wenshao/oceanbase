
#include "ob_ups_scan.h"

using namespace oceanbase;
using namespace sql;

int ObUpsScan::open()
{
  int ret = OB_SUCCESS;
  if(!check_inner_stat())
  {
    ret = OB_ERROR;
    TBSYS_LOG(WARN, "check inner stat fail");
  }
  else if(OB_SUCCESS != (ret = fetch_next(true)))
  {
    TBSYS_LOG(WARN, "fetch row fail:ret[%d]", ret);
  }
  else
  {
    cur_ups_row_.set_row_desc(row_desc_);
  }
  return ret;
}

int ObUpsScan::get_next_scan_param(const ObRange &last_range, ObScanParam &scan_param)
{
  int ret = OB_SUCCESS;

  ObRange next_range = *(scan_param.get_range());

  if(OB_SUCCESS != (ret = next_range.trim(last_range, range_str_buf_)))
  {
    TBSYS_LOG(WARN, "get next range fail:ret[%d]", ret);
  }
  else if(OB_SUCCESS != (ret = scan_param.set_range(next_range)))
  {
    TBSYS_LOG(WARN, "scan param set range fail:ret[%d]", ret);
  }

  return ret;
}

int ObUpsScan::add_column(const uint64_t &column_id)
{
  int ret = OB_SUCCESS;
  if(OB_SUCCESS != (ret = cur_scan_param_.add_column(column_id)))
  {
    TBSYS_LOG(WARN, "add column id fail:ret[%d] column_id[%lu]", ret, column_id);
  }
  else if(OB_INVALID_ID == range_.table_id_)
  {
    ret = OB_ERROR;
    TBSYS_LOG(WARN, "set range first");
  }
  else if(OB_SUCCESS != (ret = row_desc_.add_column_desc(range_.table_id_, column_id)))
  {
    TBSYS_LOG(WARN, "add column desc fail:ret[%d]", ret);
  }
  return ret;
}

int ObUpsScan::close()
{
  int ret = OB_SUCCESS;
  return ret;
}

int ObUpsScan::get_next_row(const common::ObString *&rowkey, const ObRow *&row)
{
  int ret = OB_SUCCESS;
  bool is_fullfilled = false;
  int64_t fullfilled_item_num = 0;

  if(!check_inner_stat())
  {
    ret = OB_ERROR;
    TBSYS_LOG(WARN, "check inner stat fail");
  }

  while(OB_SUCCESS == ret)
  {
    if(OB_SUCCESS != (ret = cur_new_scanner_.get_is_req_fullfilled(is_fullfilled, fullfilled_item_num)))
    {
      TBSYS_LOG(WARN, "get is req fullfilled fail:ret[%d]", ret);
    }
    else
    {
      ret = cur_new_scanner_.get_next_row(cur_rowkey_, cur_ups_row_);
      if(OB_ITER_END == ret )
      {
        if(is_fullfilled)
        {
          break;
        }
        else
        {
          if(OB_SUCCESS != (ret = fetch_next(false)))
          {
            TBSYS_LOG(WARN, "fetch row fail:ret[%d]", ret);
          }
        }
      }
      else if(OB_SUCCESS == ret)
      {
        break;
      }
      else
      {
        TBSYS_LOG(WARN, "get next row fail:ret[%d]", ret);
      }
    }
  }

  if(OB_SUCCESS == ret)
  {
    rowkey = &cur_rowkey_;
    row = &cur_ups_row_;
  }
  return ret;
}

int ObUpsScan::fetch_next(bool first_scan)
{
  int ret = OB_SUCCESS;
  ObRange last_range;

  if(!first_scan)
  { 
    if(OB_SUCCESS != (ret = cur_new_scanner_.get_range(last_range)))
    {
      TBSYS_LOG(WARN, "new scanner get range fail:ret[%d]", ret);
    }
    else if(OB_SUCCESS != (ret = get_next_scan_param(last_range, cur_scan_param_)))
    {
      TBSYS_LOG(WARN, "get scan param fail:ret[%d]", ret);
    }
  }
  
  if(OB_SUCCESS == ret)
  {
    if(OB_SUCCESS != (ret = rpc_stub_->scan(1000, ups_server, cur_scan_param_, cur_new_scanner_)))
    {
      TBSYS_LOG(WARN, "scan ups fail:ret[%d]", ret);
    }
  }

  return ret;
}

int ObUpsScan::set_range(const ObRange &range)
{
  int ret = OB_SUCCESS;
  ObString table_name; //设置一个空的table name
  range_ = range;
  if(OB_SUCCESS != (ret = cur_scan_param_.set(range_.table_id_, table_name, range_)))
  {
    TBSYS_LOG(WARN, "scan_param set range fail:ret[%d]", ret);
  }
  return ret;
}

int ObUpsScan::set_child(int32_t child_idx, ObPhyOperator &child_operator)
{
  int ret = OB_NOT_IMPLEMENT;
  UNUSED(child_idx);
  UNUSED(child_operator);
  TBSYS_LOG(WARN, "not implement");
  return ret;
}

int64_t ObUpsScan::to_string(char* buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  UNUSED(buf);
  UNUSED(buf_len);
  TBSYS_LOG(WARN, "not implement");
  return ret;
}

ObUpsScan::ObUpsScan()
  :rpc_stub_(NULL)
{
}

ObUpsScan::~ObUpsScan()
{
}

bool ObUpsScan::check_inner_stat()
{
  return NULL != rpc_stub_;
}

int ObUpsScan::set_ups_rpc_stub(ObUpsRpcStub *rpc_stub)
{
  int ret = OB_SUCCESS;
  if(NULL == rpc_stub)
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "rpc_stub is null");
  }
  else
  {
    rpc_stub_ = rpc_stub;
  }
  return ret;
}

void ObUpsScan::reset()
{
  cur_scan_param_.reset();
  row_desc_.reset();
}


