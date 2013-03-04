#ifndef OCEANBASE_MERGER_PREFETCH_DATA_H_
#define OCEANBASE_MERGER_PREFETCH_DATA_H_

#include "common/ob_define.h"

namespace oceanbase
{
  namespace common
  {
    class ObMutator;
    class ObGetParam;
    class ObPrefetchData;
    class ObSchemaManagerV2;
  }

  namespace mergeserver
  {
    class ObMergerRpcProxy;
    class ObGetMergeJoinAgentImp;
    class ObMergerVersionProxy;
    class ObMergerPrefetchData
    {
    public:
      ObMergerPrefetchData(const int64_t limit, ObMergerRpcProxy & rpc,
          ObMergerVersionProxy & version, const common::ObSchemaManagerV2 & schema);
      virtual ~ObMergerPrefetchData();

    public:
      // prefetch data for mutator
      int prefetch(const int64_t timeout, ObGetMergeJoinAgentImp & agent, common::ObGetParam & get_param,
          common::ObMutator & new_mutator);
    private:
      /// modify read param
      int modify_prefetch_param(common::ObGetParam & param);

      /// fill prefetch data
      int fill_prefetch_data(ObGetMergeJoinAgentImp & agent, common::ObPrefetchData & data);
    private:
      int64_t memory_limit_;
      int64_t cur_version_;
      ObMergerRpcProxy & rpc_;
      ObMergerVersionProxy & version_;
      const common::ObSchemaManagerV2 & schema_;
    };
  }
}

#endif // OCEANBASE_MERGER_PREFETCH_DATA_H_
