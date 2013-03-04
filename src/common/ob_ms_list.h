

/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 * 
 * Version: $Id$
 *
 * ob_ms_list.h
 *
 * Authors:
 *   yanran <yanran.hfs@taobao.com>
 *   Junquan Chen <jianming.cjq@taobao.com>
 *
 */

#ifndef _OB_MS_LIST_H
#define _OB_MS_LIST_H

#include "common/ob_define.h"
#include "common/ob_server.h"
#include "common/ob_client_manager.h"
#include "common/ob_timer.h"
#include "common/ob_buffer.h"
#include "common/ob_thread_store.h"
#include "common/ob_atomic.h"
#include "common/ob_result.h"
// #include "rootserver/ob_chunk_server_manager.h"

namespace oceanbase
{
  namespace common
  {

    /**
     * MsList记录了所有Mergeserver的地址
     * update函数会从RS处获取新的MS列表，如果列表有变化，则更新自己的
     * get_one函数会从MS列表里选择一个返回
     * 继承了ObTimerTask用来定期更新MS列表
     */
    class MsList : public ObTimerTask
    {
      public:
        MsList();
        ~MsList();
        int init(const ObServer &rs, ObClientManager *client);
        void clear();
        int update();
        const ObServer get_one();
        virtual void runTimerTask();
      protected:
        bool list_equal_(const std::vector<ObServer> &list);
        void list_copy_(const std::vector<ObServer> &list);
      protected:
        ObServer rs_;
        std::vector<ObServer> ms_list_;
        uint64_t ms_iter_;
        uint64_t ms_iter_step_;
        ObClientManager *client_;
        buffer buff_;
        tbsys::CRWSimpleLock rwlock_;
        tbutil::Mutex update_mutex_;
        bool initialized_;
    };
  }
}

#endif /* _OB_MS_LIST_H */


