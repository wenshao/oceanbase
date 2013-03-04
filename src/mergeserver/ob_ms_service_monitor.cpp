
#include "ob_ms_service_monitor.h"

using namespace oceanbase::common;
using namespace oceanbase::mergeserver;

ObMergerServiceMonitor::ObMergerServiceMonitor(const int64_t timestamp)
  :ObStatManager(SERVER_TYPE_MERGE)
{
  startup_timestamp_ = timestamp;
}

ObMergerServiceMonitor::~ObMergerServiceMonitor()
{

}

