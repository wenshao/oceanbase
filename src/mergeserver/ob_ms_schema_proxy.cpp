#include "ob_rs_rpc_proxy.h"
#include "ob_ms_schema_manager.h"
#include "ob_ms_schema_proxy.h"

using namespace oceanbase::common;
using namespace oceanbase::mergeserver;

ObMergerSchemaProxy::ObMergerSchemaProxy()
{
  root_rpc_ = NULL;
  schema_manager_ = NULL;
}

ObMergerSchemaProxy::ObMergerSchemaProxy(ObMergerRootRpcProxy * rpc, ObMergerSchemaManager * schema)
{
  root_rpc_ = rpc;
  fetch_schema_timestamp_ = 0;
  schema_manager_ = schema;
}

ObMergerSchemaProxy::~ObMergerSchemaProxy()
{
}

int ObMergerSchemaProxy::get_schema(const int64_t version, const ObSchemaManagerV2 ** manager)
{
  int ret = OB_SUCCESS;
  if (!check_inner_stat() || (NULL == manager))
  {
    TBSYS_LOG(ERROR, "%s", "check inner stat failed");
    ret = OB_ERROR;
  }
  else
  {
    switch (version)
    {
    // local newest version
    case LOCAL_NEWEST:
      {
        *manager = schema_manager_->get_schema(0);
        break;
      }
    // get server new version with timestamp
    default:
      {
        ret = fetch_schema(version, manager);
      }
    }
    // check shema data
    if ((ret != OB_SUCCESS) || (NULL == *manager))
    {
      TBSYS_LOG(DEBUG, "check get schema failed:schema[%p], version[%ld], ret[%d]",
          *manager, version, ret);
    }
  }
  return ret;
}


int ObMergerSchemaProxy::fetch_schema(const int64_t version, const ObSchemaManagerV2 ** manager)
{
  int ret = OB_SUCCESS;
  if (!check_inner_stat() || (NULL == manager))
  {
    TBSYS_LOG(WARN, "check inner stat or shema manager param failed:manager[%p]", manager);
    ret = OB_ERROR;
  }
  else if (tbsys::CTimeUtil::getTime() - fetch_schema_timestamp_ < LEAST_FETCH_SCHEMA_INTERVAL)
  {
    TBSYS_LOG(WARN, "check last fetch schema timestamp is too nearby:version[%ld]", version);
    ret = OB_OP_NOT_ALLOW;
  }
  else
  {
    // maybe get local version is ok
    tbsys::CThreadGuard lock(&schema_lock_);
    if (schema_manager_->get_latest_version() >= version)
    {
      *manager = schema_manager_->get_schema(0);
      if (NULL == *manager)
      {
        TBSYS_LOG(WARN, "get latest but local schema failed:schema[%p], latest[%ld]", 
          *manager, schema_manager_->get_latest_version());
        ret = OB_INNER_STAT_ERROR;
      }
      else
      {
        TBSYS_LOG(DEBUG, "get new schema is fetched by other thread:schema[%p], latest[%ld]",
          *manager, (*manager)->get_version());
      }
    }
    else
    {
      ret = root_rpc_->fetch_newest_schema(schema_manager_, manager);
      if (ret != OB_SUCCESS)
      {
        TBSYS_LOG(WARN, "fetch newest schema from root server failed:ret[%d]", ret);
      }
      else
      {
        fetch_schema_timestamp_ = tbsys::CTimeUtil::getTime();
      }
    }
  }
  return ret;
}

int ObMergerSchemaProxy::release_schema(const ObSchemaManagerV2 * manager)
{
  int ret = OB_SUCCESS;
  if (!check_inner_stat() || (NULL == manager))
  {
    TBSYS_LOG(ERROR, "%s", "check inner stat failed");
    ret = OB_ERROR;
  }
  else
  {
    ret = schema_manager_->release_schema(manager->get_version());
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "release scheam failed:schema[%p], timestamp[%ld]",
          manager, manager->get_version());
    }
  }
  return ret;
}


