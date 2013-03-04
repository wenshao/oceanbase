#ifndef OCEANBASE_MERGESERVER_OB_MS_DEFINE_H_
#define OCEANBASE_MERGESERVER_OB_MS_DEFINE_H_

#include <stdint.h>

namespace oceanbase
{
  namespace mergeserver
  {
    /// memory used exceeds this will cause memory free, prevent one thread hold too much memory
    static const int64_t OB_MS_THREAD_MEM_CACHE_LOWER_WATER_MARK = 64*1024*1024;
    /// memory used exceeds this will enter into endless loop
    static const int64_t OB_MS_THREAD_MEM_CACHE_UPPER_WATER_MARK = 512*1024*1024;
  }
}

#endif /* OCEANBASE_MERGESERVER_OB_MS_DEFINE_H_ */
