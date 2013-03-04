#ifndef OCEANBASE_MERGER_GET_REQUEST_H_
#define OCEANBASE_MERGER_GET_REQUEST_H_

#include "ob_ms_request_event.h"

namespace oceanbase
{
  namespace common
  {
    class ObCellInfo;
    class ObGetParam;
    class ObReadParam;
    class ObScanner;
  }

  namespace mergeserver
  {
    class ObMergerAsyncRpcStub;
    class ObMergerLocationCacheProxy;
    class ObGetRequestEvent:public ObMergerRequestEvent
    {
    public:
      ObGetRequestEvent(ObMergerLocationCacheProxy * proxy, const ObMergerAsyncRpcStub * rpc);
      virtual ~ObGetRequestEvent();

    // iterator
    public:
      int next_cell();
      int get_cell(common::ObCellInfo ** cell);
      int get_cell(common::ObCellInfo ** cell, bool * is_row_change);
    public:
      // set and create the all the sub request and send request 
      int set_request_param(common::ObGetParam & param, const int64_t timeout);

      /// virtual func for base class
      int set_request_param(common::ObReadParam & param, const int64_t timeout);

      // reset for reuse
      int reset(void);
    private:
      /// check inner stat
      inline bool check_inner_stat(void) const;

      /// move to next scanner for iterator new
      int move_next(void);

      /// process all the result after wakeup 
      int process_result(bool & finish);

      /// check request finish,if not send new request
      int process_result(const int64_t timeout, ObMergerRpcEvent * result, bool & finish);

      /// setup a new request for get_param
      int setup_new_request(const bool retry, const common::ObGetParam & get_param);

      /// check request is finished
      int check_request_finish(ObMergerRpcEvent & event, bool & finish);
    
    private:
      bool first_move_iterator_;
      // for iterator get cell
      uint64_t cur_result_pos_;
      common::ObScanner * cur_scanner_;

      // already returned total item count
      int64_t returned_item_count_;
      // result list for all the success events
      common::ObVector<ObMergerRpcEvent *> result_list_;
    };

    bool ObGetRequestEvent::check_inner_stat(void) const
    {
      return ((get_cache_proxy() != NULL) && (ObMergerRequestEvent::get_rpc() != NULL));
    }
  }
}


#endif //OCEANBASE_MERGER_GET_REQUEST_H_
