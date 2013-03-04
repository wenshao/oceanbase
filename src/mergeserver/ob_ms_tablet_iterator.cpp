#include "ob_ms_tablet_location_item.h"
#include "ob_ms_tablet_iterator.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::mergeserver;

ObTabletLocationRangeIterator::ObTabletLocationRangeIterator()
{
  init_ = false;
  is_iter_end_ = false;
}

ObTabletLocationRangeIterator::~ObTabletLocationRangeIterator()
{
}

int ObTabletLocationRangeIterator::initialize(ObMergerLocationCacheProxy * cache_proxy, 
  const ObScanParam * scan_param_in, ObStringBuf * range_rowkey_buf)
{
  int ret = OB_SUCCESS;
  if (NULL == cache_proxy)
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "cache_proxy is null");
  }

  if (OB_SUCCESS == ret && NULL == range_rowkey_buf)
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "range_rowkey_buf is null");
  }

  if (OB_SUCCESS == ret && NULL == scan_param_in)
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "scan_param_in is null");
  }

  if (OB_SUCCESS == ret)
  {
    is_iter_end_ = false;
    cache_proxy_ = cache_proxy;
    range_rowkey_buf_ = range_rowkey_buf;
    org_scan_param_ = scan_param_in;
    next_range_ = *(org_scan_param_->get_range());
  }

  if (OB_SUCCESS == ret)
  {
    ret = range_rowkey_buf_->write_string(org_scan_param_->get_range()->start_key_, &(next_range_.start_key_));
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "string buf write fail:ret[%d]", ret);
    }
  }

  if (OB_SUCCESS == ret)
  {
    ret = range_rowkey_buf_->write_string(org_scan_param_->get_range()->end_key_, &(next_range_.end_key_));
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "string buf write fail:ret[%d]", ret);
    }
  }

  if (OB_SUCCESS == ret)
  {
    init_ = true;
  }

  return ret;
}


int ObTabletLocationRangeIterator::range_intersect(const ObRange &r1, const ObRange& r2, 
  ObRange& r3, ObStringBuf& range_rowkey_buf) const
{
  int ret = OB_SUCCESS;
  if (r1.table_id_ != r2.table_id_)
  {
    ret = OB_ERROR;
    TBSYS_LOG(WARN, "table id is not equal:r1.table_id_[%ld], r2.table_id_[%ld]",
      r1.table_id_, r2.table_id_);
  }

  if (OB_SUCCESS == ret)
  {
    if (r1.empty() || r2.empty())
    {
      TBSYS_LOG(DEBUG, "range r1 or range r2 is emptry:r1 empty[%d], r2 empty[%d]",
        (int)r1.empty(), (int)r2.empty());
      ret = OB_EMPTY_RANGE;
    }
  }

  if (OB_SUCCESS == ret)
  {
    if (r1.intersect(r2)) // if r1 inersect r2 is not empty
    {
      r3.reset();

      r3.table_id_ = r1.table_id_;

      const ObRange * inner_range = NULL;

      if ( r1.compare_with_startkey2(r2) >= 0) // if r1 start key is greater than or equal to r2 start key
      {
        inner_range = &r1;
      }
      else // if r1 start key less than r2 start key
      {
        inner_range = &r2;
      }

      if (inner_range->border_flag_.is_min_value())
      {
        r3.border_flag_.set_min_value();
      }

      if (inner_range->border_flag_.inclusive_start())
      {
        r3.border_flag_.set_inclusive_start();
      }
      else
      {
        r3.border_flag_.unset_inclusive_start();
      }

      ret = range_rowkey_buf.write_string(inner_range->start_key_, &(r3.start_key_));
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "write start key fail:ret[%d]", ret);
      }

      if (OB_SUCCESS == ret)
      {
        if ( r1.compare_with_endkey2(r2) <= 0 ) // if r1 end key is less than or equal to r2 end key
        {
          inner_range = &r1;
        }
        else // if r1 end key is greater than r2 end key
        {
          inner_range = &r2;
        }

        if (inner_range->border_flag_.is_max_value())
        {
          r3.border_flag_.set_max_value();
        }

        if (inner_range->border_flag_.inclusive_end())
        {
          r3.border_flag_.set_inclusive_end();
        }
        else
        {
          r3.border_flag_.unset_inclusive_end();
        }

        ret = range_rowkey_buf.write_string(inner_range->end_key_, &(r3.end_key_));
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "write end key fail:ret[%d]", ret);
        }
      }
    }
    else
    {
      TBSYS_LOG(DEBUG, "dump range r1");
      r1.dump();
      TBSYS_LOG(DEBUG, "dump range r2");
      r2.dump();
      ret = OB_EMPTY_RANGE;
    }
  }

  return ret;
}

int ObTabletLocationRangeIterator::next(ObChunkServer * replicas_out, int32_t & replica_count_in_out,
  ObRange & tablet_range_out)
{
  int ret = OB_SUCCESS;
  if (!init_)
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(WARN, "ObTabletLocationRangeIterator not init yet");
  }

  if (OB_SUCCESS == ret && NULL == replicas_out)
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "replicas_out is null");
  }

  if (OB_SUCCESS == ret && replica_count_in_out <= 0)
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "replica_count_in_out should be positive: replica_count_in_out[%d]", replica_count_in_out);
  }

  if (OB_SUCCESS == ret && is_iter_end_)
  {
    ret = OB_ITER_END;
  }

  if (OB_SUCCESS == ret && next_range_.empty())
  {
    ret = OB_ITER_END;
    is_iter_end_ = true;
  }

  ObMergerTabletLocationList location_list;
  location_list.set_buffer(*range_rowkey_buf_);
  if (OB_SUCCESS == ret &&
    OB_SUCCESS != (ret = cache_proxy_->get_tablet_location(org_scan_param_->get_scan_direction(), &next_range_, location_list)))
  {
    TBSYS_LOG(ERROR, "get tablet location fail:ret[%d]", ret);
  }

  if (OB_SUCCESS == ret)
  {
    int64_t size = location_list.size();
    if (size > replica_count_in_out)
    {
      TBSYS_LOG(INFO, "replica count get from rootserver is great than the expectation:replica_count_in_out[%d],\
        location_list.size()[%ld]", replica_count_in_out, size);
    }
    int32_t fill_count = 0;
    for (int i = 0; (i < size) && (fill_count < replica_count_in_out); ++i)
    {
      if (location_list[i].err_times_ < ObMergerTabletLocation::MAX_ERR_TIMES)
      {
        replicas_out[fill_count].addr_ = location_list[i].server_.chunkserver_;
        replicas_out[fill_count].status_ = ObChunkServer::UNREQUESTED;
        ++fill_count;
      }
      else
      {
        const static int64_t MAX_SERVER_ADDR_LEN = 128;
        char addr[MAX_SERVER_ADDR_LEN] = "";
        location_list[i].server_.chunkserver_.to_string(addr, sizeof(addr));
        TBSYS_LOG(INFO, "check chunkserver is in blacklist:server[%s], size[%ld], err_times[%ld]",
          addr, size, location_list[i].err_times_);
      }
    }
    replica_count_in_out = fill_count;
    ret = range_intersect(location_list.get_tablet_range(), next_range_, tablet_range_out, *range_rowkey_buf_);
    if(OB_SUCCESS != ret && OB_EMPTY_RANGE != ret)
    {
      TBSYS_LOG(DEBUG, "dump location_list.get_tablet_range()");
      location_list.get_tablet_range().dump();
      TBSYS_LOG(DEBUG, "dump next_range_");
      next_range_.dump();
      TBSYS_LOG(WARN, "intersect next range and location list range fail:ret[%d]", ret);
    }
  }

  if (OB_SUCCESS == ret)
  {
    ret = next_range_.trim(tablet_range_out, *range_rowkey_buf_);
    if (OB_EMPTY_RANGE == ret)
    {
      ret = OB_SUCCESS;
      is_iter_end_ = true;
    }

    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "trim next range fail:ret[%d]", ret);
    }
  }

  /// ugly implimentation to process the last rowkey of last tablet, which is all '0xff'
  if ((OB_SUCCESS == ret) 
    &&  (tablet_range_out.compare_with_endkey2(location_list.get_tablet_range()) == 0)
    &&  (next_range_.border_flag_.is_max_value())
    &&  (tablet_range_out.end_key_.length() > 0)
    &&  (tablet_range_out.end_key_.ptr()[0] == static_cast<char>(0xff))
    &&  (tablet_range_out.end_key_.ptr()[tablet_range_out.end_key_.length() - 1] == static_cast<char>(0xff)))
  {
    bool is_max_row_key = true;
    for (int32_t i = 1; (i < tablet_range_out.end_key_.length()) && is_max_row_key ; i++)
    {
      if (tablet_range_out.end_key_.ptr()[i] != static_cast<char>(0xff))
      {
        is_max_row_key = false;
      }
    }
    if(is_max_row_key)
    {
      tablet_range_out.border_flag_.set_max_value();
      is_iter_end_ = true;
    }
  }

  /// using serialized access cs when scan backward, we can add more serializing access strategy here
  if((OB_SUCCESS == ret)
     && (org_scan_param_->get_scan_direction() == ObScanParam::BACKWARD))
  {
    tablet_range_out = next_range_;
    is_iter_end_ = true;
    TBSYS_LOG(INFO, "trigger seralizing access cs when backward scan");
  } 
  return ret;
}
