#include "time.h"
#include "common/ob_ups_info.h"
#include "ob_ms_read_balance.h"

using namespace oceanbase::common;
using namespace oceanbase::mergeserver;

ObMergerReadBalance::ObMergerReadBalance()
{
}

ObMergerReadBalance::~ObMergerReadBalance()
{
}

int32_t ObMergerReadBalance::select_server(const ObUpsList & list, const ObReadParam & param,
    const ObServerType type)
{
  UNUSED(param);
  int32_t ret = -1;
  int32_t sum_percent = list.get_sum_percentage(type);
  if (sum_percent > 0)
  {
    // no need using thread safe random
    int32_t cur_percent = 0;
    int32_t total_percent = 0;
    int32_t random_percent = static_cast<int32_t>(random() % sum_percent);
    for (int32_t i = 0; i < list.ups_count_; ++i)
    {
      cur_percent = list.ups_array_[i].get_read_percentage(type);
      total_percent += cur_percent;
      if ((random_percent < total_percent) && (cur_percent > 0))
      {
        ret = i;
        break;
      }
    }
  }
  return ret;
}


