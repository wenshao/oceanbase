/*
 * (C) 2007-2012 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.3: ob_ms_config_task.h,v 0.3 2012/7/20 10:25:10 Exp $
 *
 * Authors:
 *   yuhuang <xiaochu.yh@taobao.com>
 *     - some work details if you want
 *
 */



#ifndef _OB_MERGER_CONFIG_PROXY_H_
#define _OB_MERGER_CONFIG_PROXY_H_

#include "tbsys.h"
#include "common/ob_nb_accessor.h"

namespace oceanbase
{
  namespace mergeserver
  {
    class ObMergerConfigManager;
    class ObMergerConfigProxy
    {
    public:
      //
      ObMergerConfigProxy();
      ObMergerConfigProxy(common::ObNbAccessor * rpc, ObMergerConfigManager * config);
      virtual ~ObMergerConfigProxy();
    
    public:
      // check inner stat
      bool check_inner_stat(void) const;
      
      // fetch new config according to the version
      // param  @version config version
      int fetch_config(const int64_t version);

    private:
      tbsys::CThreadMutex config_lock_;             // mutex lock for update config manager
      common::ObNbAccessor* rpc_;             // rpc proxy
      ObMergerConfigManager * config_manager_;      // merge server config cache
    };
    
    inline bool ObMergerConfigProxy::check_inner_stat(void) const
    {
      return ((rpc_ != NULL) && (config_manager_ != NULL));
    }
  }
}


#endif // _OB_MERGER_CONFIG_PROXY_H_
