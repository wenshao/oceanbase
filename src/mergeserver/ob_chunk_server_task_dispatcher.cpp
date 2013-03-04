#include "ob_chunk_server_task_dispatcher.h"
#include "ob_ms_tsi.h"
#include "common/ob_malloc.h"
#include "common/ob_crc64.h"
#include "common/murmur_hash.h"

using namespace oceanbase;
using namespace oceanbase::mergeserver;
using namespace oceanbase::common;

ObChunkServerTaskDispatcher ObChunkServerTaskDispatcher::task_dispacher_;

ObChunkServerTaskDispatcher * ObChunkServerTaskDispatcher::get_instance()
{
  return &task_dispacher_;
}

ObChunkServerTaskDispatcher::ObChunkServerTaskDispatcher()
{
  local_ip_ = 0;
  get_factor_ = -1;
  scan_factor_ = -1;
  using_new_balance_ = false;
}

void ObChunkServerTaskDispatcher::set_factor(const int32_t get, const int32_t scan)
{
  get_factor_ = get;
  scan_factor_ = scan;
  if ((get <= 0) && (scan <= 0))
  {
    using_new_balance_ = false;
    TBSYS_LOG(INFO, "close new balance method:get[%d], scan[%d]", get, scan);
  }
  else
  {
    using_new_balance_ = true;
    TBSYS_LOG(INFO, "open new balance method:get[%d], scan[%d]", get, scan);
  }
}

ObChunkServerTaskDispatcher::~ObChunkServerTaskDispatcher()
{
}

int32_t ObChunkServerTaskDispatcher::select_cs(ObMergerTabletLocationList & list)
{
  int64_t list_size = list.size();
  int32_t ret = static_cast<int32_t>(random() % list_size);
  if (get_factor_ > 0)
  {
    ObMergerServerCounter * counter = GET_TSI_MULT(ObMergerServerCounter, SERVER_COUNTER_ID);
    if (NULL == counter)
    {
      TBSYS_LOG(WARN, "get tsi server counter failed:counter[%p]", counter);
    }
    else
    {
      int64_t min_count = (((uint64_t)1) << 63) - 1;
      int64_t cur_count = 0;
      for (int32_t i = 0; i < list_size; ++i)
      {
        if (list[i].err_times_ >= ObMergerTabletLocation::MAX_ERR_TIMES)
        {
          continue;
        }
        cur_count = counter->get(list[i].server_.chunkserver_);
        if (0 == cur_count)
        {
          ret = i;
          break;
        }
        if (cur_count < min_count)
        {
          min_count = cur_count;
          ret = i;
        }
      }
    }
    if ((ret >= 0) && (ret < list_size) && (counter != NULL))
    {
      int err = counter->inc(list[ret].server_.chunkserver_, get_factor_);
      if (err != OB_SUCCESS)
      {
        TBSYS_LOG(WARN, "inc selected cs failed:ret[%d], err[%d]", ret, err);
      }
    }
  }
  return ret;
}

int ObChunkServerTaskDispatcher::select_cs(const bool open, ObChunkServer * replicas_in_out, const int32_t replica_count_in,
    ObMergerServerCounter * counter)
{
  int ret = static_cast<int32_t>(random()%replica_count_in);
  if (open && (NULL != counter))
  {
    int64_t min_count = (((uint64_t)1) << 63) - 1;
    int64_t cur_count = 0;
    for (int32_t i = 0; i < replica_count_in; ++i)
    {
      cur_count = counter->get(replicas_in_out[i].addr_);
      if (0 == cur_count)
      {
        ret = i;
        break;
      }
      if (cur_count < min_count)
      {
        min_count = cur_count;
        ret = i;
      }
    }
  }
  return ret;
}

int ObChunkServerTaskDispatcher::select_cs(const int64_t factor, ObChunkServer * replicas_in_out,
    const int32_t replica_count_in, const int32_t last_query_idx_in, const ObString & start_row_key)
{
  int ret = OB_SUCCESS;
  UNUSED(start_row_key);
  if(OB_SUCCESS == ret && NULL == replicas_in_out)
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "parameter replicas_in_out is null");
  }

  if(OB_SUCCESS == ret && replica_count_in <= 0)
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "replica_count_in should be positive:replica_count_in[%d]", replica_count_in);
  }

  if(OB_SUCCESS == ret && last_query_idx_in >= replica_count_in)
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "last_query_idx_in should be less than replica_count_in:last_query_idx_in[%d], replica_count_in[%d]",
      last_query_idx_in, replica_count_in);
  }
  if(OB_SUCCESS == ret)
  {
    ObMergerServerCounter * counter = GET_TSI_MULT(ObMergerServerCounter, SERVER_COUNTER_ID);
    if (NULL == counter)
    {
      TBSYS_LOG(WARN, "get tsi server counter failed:counter[%p]", counter);
    }
    if(last_query_idx_in < 0) // The first time request
    {
      /// for(int32_t i=0;i<replica_count_in;i++)
      /// {
      ///   replicas_in_out[i].status_ = ObChunkServer::UNREQUESTED;
      /// }
      ///
      /// /*
      /// uint64_t crc_value = ob_crc64(start_row_key.ptr(), start_row_key.length());
      /// crc_value = ob_crc64(crc_value, &local_ip_, sizeof(local_ip_));
      /// ret = crc_value % replica_count_in;
      /// */
      /// uint64_t hash_value = 0;
      /// hash_value = murmurhash2(&local_ip_,sizeof(local_ip_),hash_value);
      /// hash_value = murmurhash2(start_row_key.ptr(), start_row_key.length(), hash_value);
      /// ret = hash_value % replica_count_in;
      /// replicas_in_out[ret].status_ = ObChunkServer::REQUESTED;
      /// ret = static_cast<int32_t>(random()%replica_count_in);
      /// select the min request counter server
      ret = select_cs(using_new_balance_, replicas_in_out, replica_count_in, counter);
      replicas_in_out[ret].status_ = ObChunkServer::REQUESTED;
    }
    else
    {
      ret = OB_ENTRY_NOT_EXIST;
      for(int i=1;i<=replica_count_in;i++)
      {
        int next = (last_query_idx_in + i) % replica_count_in;
        if(ObChunkServer::UNREQUESTED == replicas_in_out[next].status_)
        {
          ret = next;
          replicas_in_out[ret].status_ = ObChunkServer::REQUESTED;
          break;
        }
      }
      if(OB_ENTRY_NOT_EXIST == ret)
      {
        TBSYS_LOG(INFO, "There is no chunkserver which is never requested");
      }
    }
    // always open for dump output
    if (using_new_balance_ && (ret >= 0) && (ret < replica_count_in) && (counter != NULL))
    {
      int err = counter->inc(replicas_in_out[ret].addr_, factor);
      if (err != OB_SUCCESS)
      {
        TBSYS_LOG(WARN, "inc selected cs failed:ret[%d], err[%d]", ret, err);
      }
    }
  }
  return ret;

}

int ObChunkServerTaskDispatcher::select_cs(ObChunkServer * replicas_in_out, const int32_t replica_count_in,
        const int32_t last_query_idx_in, const ObRange & tablet_in)
{
  int ret = OB_SUCCESS;

  if(OB_SUCCESS == ret && NULL == replicas_in_out)
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "parameter replicas_in_out is null");
  }

  if(OB_SUCCESS == ret && replica_count_in <= 0)
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "replica_count_in should be positive:replica_count_in[%d]", replica_count_in);
  }

  if(OB_SUCCESS == ret && last_query_idx_in >= replica_count_in)
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "last_query_idx_in should be less than replica_count_in:last_query_idx_in[%d], replica_count_in[%d]",
      last_query_idx_in, replica_count_in);
  }

  if(OB_SUCCESS == ret &&
    0 > (ret = select_cs(scan_factor_, replicas_in_out, replica_count_in, last_query_idx_in, tablet_in.start_key_)))
  {
    TBSYS_LOG(WARN, "select cs fail: replica_count_in[%d], last_query_idx_in[%d] ret[%d]", replica_count_in, last_query_idx_in, ret);
  }

  return ret;
}

int ObChunkServerTaskDispatcher::select_cs(ObChunkServer * replicas_in_out, const int32_t replica_count_in,
  const int32_t last_query_idx_in, const ObCellInfo & get_cell_in)
{
  int ret = OB_SUCCESS;
  if(OB_SUCCESS == ret && NULL == replicas_in_out)
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "parameter replicas_in_out is null");
  }

  if(OB_SUCCESS == ret && replica_count_in <= 0)
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "replica_count_in should be positive:replica_count_in[%d]", replica_count_in);
  }

  if(OB_SUCCESS == ret && last_query_idx_in >= replica_count_in)
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "last_query_idx_in should be less than replica_count_in:last_query_idx_in[%d], replica_count_in[%d]",
      last_query_idx_in, replica_count_in);
  }

  if (OB_SUCCESS == ret &&
    0 > (ret = select_cs(get_factor_, replicas_in_out, replica_count_in, last_query_idx_in, get_cell_in.row_key_)))
  {
    TBSYS_LOG(WARN, "select cs fail: replica_count_in[%d], last_query_idx_in[%d] ret[%d]", replica_count_in, last_query_idx_in, ret);
  }
  return ret;
}


