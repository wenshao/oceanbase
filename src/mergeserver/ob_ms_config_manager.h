/*
 * (C) 2007-2012 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: 0.3: ob_ms_config_task.h,v 0.3 2012/7/20 10:25:10 zhidong Exp $
 *
 * Authors:
 *   yuhuang <xiaochu.yh@taobao.com>
 *     - some work details if you want
 *
 *
 */


#ifndef OB_MS_CONFIG_MANAGER_H_
#define OB_MS_CONFIG_MANAGER_H_

#include <pthread.h>
#include "tbsys.h"
#include "common/ob_range.h"
#include "common/ob_nb_accessor.h"

namespace oceanbase
{
  namespace mergeserver
  {
    class ObMergeServerParams;
    class ObMergerConfigManager
    {
    public:
      ObMergerConfigManager();
      virtual ~ObMergerConfigManager();
    
    public:
      // init the newest config
      int init(const ObMergeServerParams *param);

      // get the latest config version
      inline int64_t get_config_version(void) const;

      int update_config(common::QueryRes *res);
      inline const char* get_table_name() const;
      inline const common::ObRange &get_query_range() const;
      inline const common::SC &get_select_columns() const;
      inline const common::ScanConds &get_scan_condition() const;
    private:
      // check inner stat
      inline bool check_inner_stat(void) const;

    private:
      static const char *table_name_;
      static const char *item_name_str_;
      static const char *item_value_str_;
      bool init_;
      // last update timestamp
      int64_t config_version_;
      mutable tbsys::CThreadMutex lock_;
      const ObMergeServerParams *param_;
      common::ObRange query_range_;
      common::SC select_columns_;
      common::ScanConds scan_conds_;
    };

    inline bool ObMergerConfigManager::check_inner_stat(void) const
    {
      return (true == init_);
    }

    inline int64_t ObMergerConfigManager::get_config_version(void) const
    {
      tbsys::CThreadGuard lock(&lock_);
      return config_version_;
    }

    inline const char* ObMergerConfigManager::get_table_name() const 
    {
      return table_name_;
    }
    inline const common::ObRange &ObMergerConfigManager::get_query_range() const
    {
      return query_range_;
    }
    inline const common::SC &ObMergerConfigManager::get_select_columns() const
    {
      return select_columns_;
    }
    inline const common::ScanConds &ObMergerConfigManager::get_scan_condition() const
    {
      return scan_conds_;
    }

  };
};






#endif //OB_MS_CONFIG_MANAGER_H_
