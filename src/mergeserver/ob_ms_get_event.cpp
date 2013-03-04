#include "ob_ms_async_rpc.h"
#include "ob_ms_rpc_event.h"
#include "ob_ms_get_event.h"
#include "ob_read_param_modifier.h"
#include "ob_ms_tablet_location_item.h"
#include "ob_ms_tablet_location_proxy.h"
#include "common/ob_trace_log.h"

using namespace oceanbase::common;
using namespace oceanbase::mergeserver;

ObGetRequestEvent::ObGetRequestEvent(ObMergerLocationCacheProxy * proxy,
    const ObMergerAsyncRpcStub * rpc):ObMergerRequestEvent(proxy,rpc)
{
  returned_item_count_ = 0;
  first_move_iterator_ = true;
  cur_result_pos_ = 0;
  cur_scanner_ = NULL;
}

ObGetRequestEvent::~ObGetRequestEvent()
{
}

int ObGetRequestEvent::reset(void)
{
  //print_info(stdout);
  cur_result_pos_ = 0;
  cur_scanner_ = NULL;
  returned_item_count_ = 0;
  first_move_iterator_ = true;
  result_list_.clear();
  int ret = ObMergerRequestEvent::reset();
  return ret;
}

// no need lock
int ObGetRequestEvent::next_cell(void)
{
  int ret = OB_SUCCESS;
  // check all finished
  int64_t timeout = 0;
  const ObGetParam * param = dynamic_cast<const ObGetParam *>(ObMergerRequestEvent::get_request_param(timeout));
  if ((NULL == param) || (returned_item_count_ != param->get_cell_size()))
  {
    ret = OB_ERROR;
    if (NULL == param)
    {
      TBSYS_LOG(WARN, "check get param failed:request[%lu], param[%p]", get_request_id(), param);
    }
    else
    {
      TBSYS_LOG(WARN, "check the get request not finished yet:param[%p], total[%lu], cur[%ld]",
          param, param->get_cell_size(), returned_item_count_);
    }
  }
  else if (true == first_move_iterator_)
  {
    first_move_iterator_ = false;
    cur_result_pos_ = 0;
    ret = move_next();
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "move to the first scanner failed:ret[%d]", ret);
    }
  }
  else if (cur_scanner_ != NULL)
  {
    ret = cur_scanner_->next_cell();
    if ((OB_ITER_END == ret) && (cur_result_pos_ + 1 < (uint64_t)result_list_.size()))
    {
      TBSYS_LOG(DEBUG, "goto the next scanner result:index[%lu], count[%d]",
          cur_result_pos_, result_list_.size());
      ++cur_result_pos_;
      ret = move_next();
      if (ret != OB_SUCCESS)
      {
        TBSYS_LOG(WARN, "move to the first scanner failed:ret[%d]", ret);
      }
    }
  }
  else
  {
    ret = OB_ERROR;
    TBSYS_LOG(WARN, "check no result");
  }
  return ret;
}

int ObGetRequestEvent::move_next(void)
{
  int ret = OB_SUCCESS;
  const ObMergerRpcEvent * rpc = result_list_[static_cast<int32_t>(cur_result_pos_)];
  if (NULL == rpc)
  {
    ret = OB_INNER_STAT_ERROR;
    TBSYS_LOG(WARN, "check cur result failed:index[%lu], count[%d], event[%p]", 
        cur_result_pos_, result_list_.size(), rpc);
  }
  else //if (OB_SUCCESS == rpc->get_result_code())
  {
    cur_scanner_ = &(const_cast<ObMergerRpcEvent*>(rpc)->get_result());
    if (NULL == cur_scanner_)
    {
      ret = OB_INNER_STAT_ERROR;
      TBSYS_LOG(WARN, "check cur_scanner failed:index[%lu], count[%d], result[%p]",
          cur_result_pos_, result_list_.size(), cur_scanner_);
    }
    else
    {
      ret = cur_scanner_->next_cell();
      if (ret != OB_SUCCESS)
      {
        TBSYS_LOG(WARN, "check next cell failed:index[%lu], count[%d], ret[%d]",
            cur_result_pos_, result_list_.size(), ret);
        // must has cell for get
        ret = OB_ERROR;
      }
    }
  }
  return ret;
}

int ObGetRequestEvent::get_cell(ObCellInfo ** cell, bool * is_row_change)
{
  int ret = OB_SUCCESS;
  if (cur_scanner_ != NULL)
  {
    cur_scanner_->get_cell(cell, is_row_change);
  }
  else
  {
    ret = OB_ERROR;
    TBSYS_LOG(WARN, "check no result");
  }
  return ret;
}

int ObGetRequestEvent::get_cell(ObCellInfo ** cell)
{
  return get_cell(cell, NULL);
}

int ObGetRequestEvent::set_request_param(ObReadParam & param, const int64_t timeout)
{
  return set_request_param(dynamic_cast<ObGetParam &>(param), timeout);
}

int ObGetRequestEvent::set_request_param(ObGetParam & param, const int64_t timeout)
{
  ObMergerRequestEvent::set_request_param(&param, timeout);
  int ret = setup_new_request(false, param);
  if (ret != OB_SUCCESS)
  {
    TBSYS_LOG(WARN, "set param create new rpc event and send request failed:"
        "request[%lu], ret[%d]", get_request_id(), ret);
  }
  else
  {
    TBSYS_LOG(DEBUG, "set param and send request succ:request[%lu]", get_request_id());
  }
  return ret;
}

int ObGetRequestEvent::check_request_finish(ObMergerRpcEvent & event, bool & finish)
{
  finish = false;
  bool retry = false;
  int ret = OB_SUCCESS;
  ObGetParam new_param;
  int32_t result_code = OB_SUCCESS;
  ObScanner & result = event.get_result(result_code);
  int64_t timeout = 0;
  const ObGetParam * org_param = dynamic_cast<const ObGetParam *>(get_request_param(timeout));
  if (NULL == org_param)
  {
    ret = OB_INPUT_PARAM_ERROR;
    TBSYS_LOG(WARN, "check result code error:result[%d], request[%lu], event[%lu], param[%p]",
        event.get_result_code(), get_request_id(), event.get_event_id(), org_param);
  }
  else if (result_code != OB_SUCCESS)
  {
    // TODO retry next server or delete the cache item and terminate if retry too many times
    retry = true;
    result.clear();
    // set fullfill to true item count = 0 for next get new param
    result.set_is_req_fullfilled(true, 0);
    TBSYS_LOG(INFO, "check result code failed:result[%d], request[%lu], event[%lu]",
        event.get_result_code(), get_request_id(), event.get_event_id());
  }
  else
  {
    // right now no duplicated rpc event and all is in-sequence return
    // add to result list for iterator
    ret = result_list_.push_back(&event);
    if (ret != OB_SUCCESS)
    {
      result.clear();
      result.set_is_req_fullfilled(true, 0);
      TBSYS_LOG(ERROR, "push the result failed:request[%lu], event[%lu], ret[%d]",
          get_request_id(), event.get_event_id(), ret);
    }
    else
    {
      FILL_TRACE_LOG("got one result from cs finished_sub_get_count[%d]", result_list_.size());
    }
  }
  
  // update the returned item count and construct the new param if not finish all
  ret = get_next_param(*org_param, result, returned_item_count_, finish, &new_param);
  if (ret != OB_SUCCESS)
  {
    TBSYS_LOG(ERROR, "get next param failed:request[%lu], event[%lu], ret[%d]", 
        get_request_id(), event.get_event_id(), ret);
  }
  
  if ((false == finish) && (OB_SUCCESS == ret))
  {
    ret = setup_new_request(retry, new_param);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "setup new request failed:request[%lu], event[%lu], ret[%d]", 
          get_request_id(), event.get_event_id(), ret);
    }
  }
  return ret;
}

int ObGetRequestEvent::setup_new_request(const bool retry, const ObGetParam & get_param)
{
  int ret = OB_SUCCESS;
  ObMergerRpcEvent * event = NULL;
  // step 1. create new rpc event and add to the waiting queue
  if (false == check_inner_stat())
  {
    ret = OB_INNER_STAT_ERROR;
    TBSYS_LOG(WARN, "check inner stat failed");
  }
  else
  {
    ret = ObMergerRequestEvent::create(&event);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "add new rpc event failed:request[%lu], ret[%d]", get_request_id(), ret);
    }
  }
  
  // TODO check retry with other cs
  UNUSED(retry);
  ObMergerTabletLocationList list;
  // step 3. select the right cs for request
  if ((OB_SUCCESS == ret)) // && (false == retry))
  {
    ret = get_cache_proxy()->get_tablet_location(get_param[0]->table_id_, get_param[0]->row_key_, list);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "get tablet location failed:client[%lu], event[%lu], request[%lu], ret[%d]",
          event->get_client_id(), event->get_event_id(), get_request_id(), ret);
    }
  }
  
  // step 4. send reqeust for get
  if (OB_SUCCESS == ret)
  {
    // TODO access list[0]
    event->set_server(list[0].server_.chunkserver_);
    ret = get_rpc()->get(get_timeout(), list[0].server_.chunkserver_, get_param, *event);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "check async rpc stub failed:client[%lu], event[%lu], request[%lu], ret[%d]",
        event->get_client_id(), event->get_event_id(), get_request_id(), ret);
    }
    else
    {
      TBSYS_LOG(DEBUG, "send get param to server succ:client[%lu], event[%lu], request[%lu], "
          "get_cell[%ld]", event->get_client_id(), event->get_event_id(), get_request_id(),
          get_param.get_cell_size());
    }
  }
  
  /// if not send succ
  if ((event != NULL) && (ret != OB_SUCCESS))
  {
    uint64_t client_id = event->get_client_id();
    uint64_t event_id = event->get_event_id();
    int err = ObMergerRequestEvent::destroy(event);
    if (err != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "destroy the event failed when rpc send failed:client[%lu], event[%lu], "
          "request[%lu], ret[%d], err[%d]", client_id, event_id, get_request_id(), ret, err);
    }
    else
    {
      TBSYS_LOG(INFO, "destroy directly succ:client[%lu], event[%lu], request[%lu], ret[%d]",
          client_id, event_id, get_request_id(), ret);
    }
  }
  return ret;
}

/// for work thread
int ObGetRequestEvent::process_result(const int64_t timeout, ObMergerRpcEvent * rpc_result, bool & finish)
{
  int ret = OB_SUCCESS;
  if ((NULL == rpc_result) || (rpc_result->get_client_id() != get_request_id()))
  {
    ret = OB_INPUT_PARAM_ERROR;
    TBSYS_LOG(WARN, "check input param failed:result[%p]", rpc_result);
  }
  else
  {
    finish = true;
    ObMergerRequestEvent::set_timeout(timeout);
    ret = check_request_finish(*rpc_result, finish);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "check request finish failed:total[%ld], client[%lu], request[%lu], "
          "event[%lu], ret[%d]", returned_item_count_, rpc_result->get_client_id(),
          get_request_id(), rpc_result->get_event_id(), ret);
    }
    else if (true == finish)
    {
      TBSYS_LOG(INFO, "finish all succ:total[%ld], client[%lu], request[%lu], event[%lu]",
          returned_item_count_, rpc_result->get_client_id(), get_request_id(),
          rpc_result->get_event_id());
    }
    else
    {
      TBSYS_LOG(DEBUG, "not finish all request:total[%ld], client[%lu], request[%lu], event[%lu]",
          returned_item_count_, rpc_result->get_client_id(), get_request_id(), rpc_result->get_event_id());
    }
  }
  return ret;
}


