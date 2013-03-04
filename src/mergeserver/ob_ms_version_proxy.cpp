#include "common/utility.h"
#include "common/ob_atomic.h"
#include "ob_ms_rpc_proxy.h"
#include "ob_ms_version_proxy.h"

using namespace oceanbase::common;
using namespace oceanbase::mergeserver;

ObMergerVersionProxy::ObMergerVersionProxy(const int64_t timeout)
{
  rpc_proxy_ = NULL;
  last_frozen_version_ = -1;
  last_timestamp_ = 0;
  set_timeout(timeout);
}

ObMergerVersionProxy::~ObMergerVersionProxy()
{
}

void ObMergerVersionProxy::set_timeout(const int64_t timeout)
{
  if (timeout <= 0)
  {
    timeout_ = DEFAULT_TIMEOUT;
    TBSYS_LOG(WARN, "check timeout failed set default:timeout[%ld], default[%ld]",
        timeout, timeout_);
  }
  else
  {
    timeout_ = timeout;
  }
}

int ObMergerVersionProxy::init(ObMergerRpcProxy * rpc)
{
  int ret = OB_SUCCESS;
  if (NULL == rpc)
  {
    TBSYS_LOG(WARN, "check init rpc failed:rpc[%p]", rpc);
    ret = OB_INPUT_PARAM_ERROR;
  }
  else
  {
    rpc_proxy_ = rpc;
  }
  return ret;
}

int ObMergerVersionProxy::get_version(int64_t & version)
{
  int ret = OB_SUCCESS;
  if (NULL == rpc_proxy_)
  {
    ret = OB_ERROR;
    TBSYS_LOG(WARN, "check rpc proxy failed:rpc[%p]", rpc_proxy_);
  }
  else
  {
    int64_t right_now = tbsys::CTimeUtil::getTime();
    if ((right_now - last_timestamp_) > timeout_)
    {
      tbsys::CThreadGuard lock(&lock_);
      if ((right_now - last_timestamp_) > timeout_)
      {
        ret = rpc_proxy_->fetch_new_version(version);
        // if fetch failed, return the old version
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(WARN, "check fetch frozen version failed:last_version[%ld], ret[%d]",
              last_frozen_version_, ret);
          version = last_frozen_version_;
          ret = OB_SUCCESS;
        }
        else
        {
          last_frozen_version_ = version;
        }
        // no matter succ or failed update last fetch timestamp if not the first time
        if (last_frozen_version_ != -1)
        {
          last_timestamp_= right_now;
        }
      }
      else
      {
        TBSYS_LOG(DEBUG, "version fetched by other thread:last_version[%ld]", last_frozen_version_);
        version = last_frozen_version_;
      }
    }
    else
    {
      // if not timeout, return the old version
      atomic_exchange(reinterpret_cast<uint64_t *>(&version), last_frozen_version_);
    }
  }
  return ret;
}


