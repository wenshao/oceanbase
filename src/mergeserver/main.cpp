#include "common/ob_define.h"
#include "common/base_main.h"
#include "common/ob_malloc.h"
#include "ob_merge_server_main.h"
#include "ob_ms_counter_infos.h"
#include <malloc.h>
#include <stdlib.h>
#include <time.h>

using namespace oceanbase::common;
using namespace oceanbase::mergeserver;
namespace 
{
  static const int DEFAULT_MMAP_MAX_VAL = 1024*1024*1024;
};

int main(int argc, char *argv[])
{
  int rc  = OB_SUCCESS;
  mallopt(M_MMAP_MAX, DEFAULT_MMAP_MAX_VAL);
  srandom(static_cast<uint32_t>(time(NULL)));
  mallopt(M_MMAP_MAX, DEFAULT_MMAP_MAX_VAL); 
  ob_init_memory_pool();
  if (OB_SUCCESS != (rc = ms_get_counter_set().init(ms_get_counter_infos())))
  {
    TBSYS_LOG(WARN,"fail to init counter set");
  }
  else
  {
    BaseMain* mergeServer = ObMergeServerMain::get_instance();
    if (NULL == mergeServer)
    {
      fprintf(stderr, "mergeserver start failed, instance is NULL.");
      rc = OB_ERROR;
    }
    else
    {
      rc = mergeServer->start(argc, argv, "merge_server");
    }

    if (NULL != mergeServer)
    {
      mergeServer->destroy();
    }
  }

  return rc;
}

