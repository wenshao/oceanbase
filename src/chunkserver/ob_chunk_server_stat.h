/*
 * (C) 2007-2010 TaoBao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * ob_chunk_server_stat.h is for what ...
 *
 * Version: $id$
 *
 * Authors:
 *   MaoQi maoqi@taobao.com
 *
 */

#ifndef OCEANBASE_CHUNKSERVER_OB_CHUNK_SERVER_STAT_H_
#define OCEANBASE_CHUNKSERVER_OB_CHUNK_SERVER_STAT_H_

#include "common/ob_statistics.h"
namespace oceanbase
{
  namespace chunkserver
  {
    class ObChunkServerStatManager : public common::ObStatManager
    {
      public:
        ObChunkServerStatManager() : ObStatManager(common::ObStatManager::SERVER_TYPE_CHUNK) {}

        enum
        {
          INDEX_GET_COUNT       = 0,
          INDEX_SCAN_COUNT      = 1,
         
          INDEX_GET_TIME        = 2,
          INDEX_SCAN_TIME       = 3,

          INDEX_BLOCK_INDEX_CACHE_HIT   = 4,
          INDEX_BLOCK_INDEX_CACHE_MISS  = 5,

          INDEX_BLOCK_CACHE_HIT         = 6,
          INDEX_BLOCK_CACHE_MISS        = 7,

          INDEX_GET_BYTES               = 8,
          INDEX_SCAN_BYTES              = 9,

          INDEX_DISK_IO_NUM             = 10,
          INDEX_DISK_IO_BYTES           = 11,

          INDEX_SSTABLE_ROW_CACHE_HIT   = 12,
          INDEX_SSTABLE_ROW_CACHE_MISS  = 13,
        };

        enum
        {
          META_TABLE_ID = 0,
        };

        enum
        {
          INDEX_META_TABLETS_NUM        = 0,
          INDEX_META_MERGE_TABLETS_NUM  = 1,
          INDEX_META_MERGE_MERGED       = 2,

          INDEX_MU_DEFAULT = 3,
          INDEX_MU_NETWORK = 4,
          INDEX_MU_THREAD_BUFFER = 5,
          INDEX_MU_TABLET = 6,
          INDEX_MU_BI_CACHE = 7,
          INDEX_MU_BLOCK_CACHE = 8,
          INDEX_MU_BI_CACHE_UNSERVING = 9,
          INDEX_MU_BLOCK_CACHE_UNSERVING = 10,
          INDEX_MU_JOIN_CACHE = 11,
          INDEX_MU_MERGE_BUFFER = 12,
          INDEX_MU_MERGE_SPLIT_BUFFER= 13,

          INDEX_META_REQUEST_COUNT = 14,
          INDEX_META_REQUEST_COUNT_PER_SECOND = 15,
        };
    };
    
  } /* chunkserver */
} /* oceanbase */
#endif
