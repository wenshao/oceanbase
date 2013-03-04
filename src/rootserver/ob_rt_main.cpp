/*===============================================================
*   (C) 2007-2010 Taobao Inc.
*   
*   
*   Version: 0.1 2010-09-26
*   
*   Authors:
*          daoan(daoan@taobao.com)
*   
*
================================================================*/
#include <malloc.h>
#include "rootserver/ob_root_main.h"
#include "common/ob_malloc.h"
using namespace oceanbase::rootserver;
using namespace oceanbase::common;
namespace  
{ 
  static const int DEFAULT_MMAP_MAX_VAL = 1024*1024*1024; 
}; 


int main(int argc, char *argv[])
{
  mallopt(M_MMAP_MAX, DEFAULT_MMAP_MAX_VAL);  
  ob_init_memory_pool();
  BaseMain* pmain = ObRootMain::get_instance();
  if (pmain == NULL)
  {
    perror("not enought mem, exit \n");
  }
  else
  {
    pmain->start(argc, argv, "root_server");
    pmain->destroy();
  }
  return 0;
}
