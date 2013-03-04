#include <gtest/gtest.h>
#include "ob_define.h"
#include "chunkserver/ob_disk_manager.h"
#include "common/ob_malloc.h"

using namespace oceanbase;
using namespace oceanbase::chunkserver;

TEST(obDiskManager,test)
{
  ObDiskManager disk_m;
  disk_m.scan("/data",256*1024*1024);
  disk_m.dump();

  printf("total capacity:%ld\n",disk_m.get_total_capacity());
  printf("total used:%ld\n",disk_m.get_total_used());

  for(int i=0;i<30;++i)
  {
    int32_t disk_no = disk_m.get_dest_disk();
    printf("get_disk_no : %d\n",disk_no);
    disk_m.add_used_space(disk_no,256*1024*1024);
  }
  
  for(int i=0;i<20;++i)
  {
    int32_t disk_no = disk_m.get_disk_for_migrate();
    printf("get_most_free_disk: %d\n",disk_no);
  }

  int32_t size = 0;
  const int32_t *disk_no_array = disk_m.get_disk_no_array(size);
  for(int32_t i=0;i<size;++i)
  {
    printf(" DISK NO : [%d] ",disk_no_array[i]);
  }
  printf ("\n");
}

int main(int argc, char **argv)
{
  common::ob_init_memory_pool();
  testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}


