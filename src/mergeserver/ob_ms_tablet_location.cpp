#include "ob_ms_tablet_location.h"
#include "ob_ms_tablet_location_item.h"

using namespace oceanbase::common;
using namespace oceanbase::mergeserver;

ObMergerTabletLocationCache::ObMergerTabletLocationCache()
{
  init_ = false;
  cache_mem_size_ = 0;
  cache_timeout_ = DEFAULT_ALIVE_TIMEOUT;
}


ObMergerTabletLocationCache::~ObMergerTabletLocationCache()
{
}

int ObMergerTabletLocationCache::init(const uint64_t mem_size, const uint64_t count,
    const int64_t timeout)
{
  int ret = OB_SUCCESS;
  if (init_)
  {
    TBSYS_LOG(ERROR, "%s", "check cache already inited");
    ret = OB_INNER_STAT_ERROR;
  }
  else if (timeout <= 0)
  {
    TBSYS_LOG(ERROR, "check cache timeout failed:timeout[%ld]", timeout);
    ret = OB_INPUT_PARAM_ERROR;
  }
  else
  {
    cache_timeout_ = timeout;
    cache_mem_size_ = mem_size;
    // cache init cache max mem size and init item count
    ret = tablet_cache_.init(mem_size, timeout, static_cast<int32_t>(count));
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "cache init failed:mem[%lu], count[%lu], timeo[%ld], ret[%d]",
          mem_size, count, timeout, ret);
    }
    else
    {
      init_ = true;
    }
  }
  return ret; 
}


int ObMergerTabletLocationCache::get(const uint64_t table_id, const ObString & rowkey,
    ObMergerTabletLocationList & location)
{
  int ret = OB_SUCCESS;
  if (!init_)
  {
    TBSYS_LOG(ERROR, "%s", "check init failed");
    ret = OB_INNER_STAT_ERROR;
  }
  else if ((0 == rowkey.length()) || (NULL == rowkey.ptr()))
  {
    TBSYS_LOG(ERROR, "%s", "check rowkey length failed");
    ret = OB_INPUT_PARAM_ERROR;
  }
  else
  {
    char * temp = (char *)ob_malloc(sizeof(table_id) + rowkey.length(), ObModIds::OB_MS_LOCATION_CACHE);
    if (NULL == temp)
    {
      TBSYS_LOG(ERROR, "check ob malloc failed:size[%lu], pointer[%p]", 
          sizeof(table_id) + rowkey.length(), temp);
      ret = OB_ALLOCATE_MEMORY_FAILED;
    }
    else
    {
      ObString CacheKey;
      // encode table_id + rowkey as CacheKey
      *((uint64_t *) temp) = table_id;
      memcpy(temp + sizeof(uint64_t), rowkey.ptr(), rowkey.length());
      CacheKey.assign(temp, static_cast<int32_t>(sizeof(table_id) + rowkey.length()));
      // get the pair according to the key
      ObCachePair pair;
      ret = tablet_cache_.get(CacheKey, pair);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(DEBUG, "find tablet from cache failed:table_id[%lu], length[%d]",
            table_id, rowkey.length());
      }
      else
      {
        int64_t pos = 0;
        // TODO double check pair.key whether as equal with CacheKey
        ret = location.deserialize(pair.get_value().ptr(), pair.get_value().length(), pos);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "deserialize location failed:table_id[%lu], length[%d], "
              "ret[%d]", table_id, rowkey.length(), ret);
        }
      }
      // destory the temp buffer
      ob_free(temp);
    }
  }
  return ret;
}


int ObMergerTabletLocationCache::set(const ObRange & range, const ObMergerTabletLocationList & location)
{
  int ret = OB_SUCCESS;
  if (!init_)
  {
    TBSYS_LOG(ERROR, "%s", "check init failed");
    ret = OB_INNER_STAT_ERROR;
  }
  else
  {
    ObCachePair pair;
    // malloc the pair mem buffer
    int64_t size = range.get_serialize_size();
    ret = tablet_cache_.malloc(pair, static_cast<int32_t>(size), static_cast<int32_t>(location.get_serialize_size()));
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "check malloc failed:key_size[%lu], ret[%d]", size, ret);
    }
    
    int64_t pos = 0;
    // key serialize to the pair
    if (OB_SUCCESS == ret)
    {  
      ret = range.serialize(pair.get_key().ptr(), size, pos);
      if (ret != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR, "serialize range failed:ret[%d]", ret);
      }
    }

    // value serialize to the pair
    if (OB_SUCCESS == ret)
    {
      pos = 0;
      ret = location.serialize(pair.get_value().ptr(), location.get_serialize_size(), pos);
      if (ret != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR, "serialize locationlist failed:ret[%d]", ret);
      }
    }

    // delete the old cache item
    if (OB_SUCCESS == ret)
    {
      ret = del(range.table_id_, range.end_key_);
      if ((ret != OB_SUCCESS) && (ret != OB_ENTRY_NOT_EXIST))
      {
        TBSYS_LOG(WARN, "del the old item:table[%lu], ret[%d]", range.table_id_, ret);
      }
      else
      {
        ret = OB_SUCCESS;
      }
    }

    // add new pair and return the old pair for deletion
    if (OB_SUCCESS == ret)
    {
      ObCachePair oldpair;
      ret = tablet_cache_.add(pair, oldpair);
      if (ret != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR, "add the pair failed:ret[%d]", ret);
      }
      else
      {
        TBSYS_LOG(DEBUG, "%s", "set tablet location cache succ");
      }
    }
  }
  return ret;
}

int ObMergerTabletLocationCache::update(const uint64_t table_id, const ObString & rowkey,
    const ObMergerTabletLocationList & location)
{
  int ret = OB_SUCCESS;
  if (!init_)
  {
    TBSYS_LOG(ERROR, "%s", "check init failed");
    ret = OB_INNER_STAT_ERROR;
  }
  else if ((0 == rowkey.length()) || (NULL == rowkey.ptr()))
  {
    TBSYS_LOG(ERROR, "%s", "check rowkey length failed");
    ret = OB_INPUT_PARAM_ERROR;
  }
  else
  {
    char * temp = (char *)ob_malloc(sizeof(table_id) + rowkey.length(),ObModIds::OB_MS_LOCATION_CACHE);
    if (NULL == temp)
    {
      TBSYS_LOG(ERROR, "check ob malloc failed:size[%lu], pointer[%p]", 
          sizeof(table_id) + rowkey.length(), temp);
      ret = OB_ALLOCATE_MEMORY_FAILED;
    }
    else
    {
      ObRange range;
      ObString CacheKey;
      // encode table_id + rowkey as CacheKey
      *((uint64_t *) temp) = table_id;
      memcpy(temp + sizeof(uint64_t), rowkey.ptr(), rowkey.length());
      CacheKey.assign(temp, static_cast<int32_t>(sizeof(table_id) + rowkey.length()));
      // get the pair according to the key
      ObCachePair pair;
      ret = tablet_cache_.get(CacheKey, pair);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(DEBUG, "find tablet from cache failed:table_id[%lu], length[%d]",
            table_id, rowkey.length());
      }
      else
      {
        int64_t pos = 0;
        // TODO double check pair.key whether as equal with CacheKey
        ret = range.deserialize(pair.get_key().ptr(), pair.get_key().length(), pos);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "deserialize tablet range failed:table_id[%lu], ret[%d]",
              table_id, ret);
        }
        else
        {
          ret = set(range, location);
        }
      }
      // destory the temp buffer
      ob_free(temp);
    }
  }
  return ret;
}

int ObMergerTabletLocationCache::del(const uint64_t table_id, const ObString & rowkey)
{
  int ret = OB_SUCCESS;
  if (!init_)
  {
    TBSYS_LOG(ERROR, "%s", "check init failed");
    ret = OB_INNER_STAT_ERROR;
  }
  else
  {
    char * temp = (char *)ob_malloc(sizeof(table_id) + rowkey.length(), ObModIds::OB_MS_LOCATION_CACHE);
    if (NULL == temp)
    {
      TBSYS_LOG(ERROR, "check ob malloc failed:size[%lu], pointer[%p]", 
          sizeof(table_id) + rowkey.length(), temp);
      ret = OB_ALLOCATE_MEMORY_FAILED;
    }
    else
    {
      // construct table_id + rowkey to CacheKey as cache key
      ObString CacheKey;
      *(reinterpret_cast<uint64_t *>(temp)) = table_id;
      memcpy(temp + sizeof(uint64_t), rowkey.ptr(), rowkey.length());
      CacheKey.assign(temp, static_cast<int32_t>(sizeof(table_id) + rowkey.length()));
      ret = tablet_cache_.remove(CacheKey);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(DEBUG, "find this row tablet failed:table_id[%lu], length[%d]", 
            table_id, rowkey.length());
      }
      else
      {
        TBSYS_LOG(DEBUG, "%s", "del this location from cache succ");
      }
      // destory the temp buffer
      ob_free(temp);
    }
  }
  return ret;
}

uint64_t ObMergerTabletLocationCache::size(void) const
{
  return tablet_cache_.get_cache_item_num();
}

int ObMergerTabletLocationCache::clear(void)
{
  int ret = OB_SUCCESS;
  if (!init_)
  {
    TBSYS_LOG(ERROR, "%s", "check init failed");
    ret = OB_INNER_STAT_ERROR;
  }
  else
  {
    ret = tablet_cache_.clear();
  }
  return ret;
}

void ObMergerTabletLocationCache::dump(void) const
{
  TBSYS_LOG(INFO, "tablet location cache:cache_items[%lu], max_memory[%ld], timeout[%ld]",
      size(), cache_mem_size_, cache_timeout_);
}


