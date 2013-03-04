/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_sstable_stat.h for statistic of sstable read and write. 
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */
#ifndef  OCEANBASE_SSTABLE_SSTABLE_STAT_H_
#define  OCEANBASE_SSTABLE_SSTABLE_STAT_H_

#ifndef NO_STAT
#define INC_STAT(table_id, stat_key, inc_value) \
  { \
    inc_stat(table_id, stat_key, inc_value); \
  }

#define SET_STAT(table_id, stat_key, inc_value) \
  { \
    set_stat(table_id, stat_key, inc_value); \
  }
#else
#define INC_STAT(table_id, stat_key, inc_value)
#define SET_STAT(table_id, stat_key, inc_value)
#endif

namespace oceanbase
{
  namespace sstable
  {
    enum
    {
      INDEX_BLOCK_INDEX_CACHE_HIT   = 0,
      INDEX_BLOCK_INDEX_CACHE_MISS  = 1,

      INDEX_BLOCK_CACHE_HIT         = 2,
      INDEX_BLOCK_CACHE_MISS        = 3,

      INDEX_DISK_IO_NUM             = 4,
      INDEX_DISK_IO_BYTES           = 5,

      INDEX_SSTABLE_ROW_CACHE_HIT   = 6,
      INDEX_SSTABLE_ROW_CACHE_MISS  = 7,
    };

#ifndef NO_STAT
    extern void set_stat(const uint64_t table_id, const int32_t index, const int64_t value);
    extern void inc_stat(const uint64_t table_id, const int32_t index, const int64_t inc_value);
#endif
  }
}

#endif
