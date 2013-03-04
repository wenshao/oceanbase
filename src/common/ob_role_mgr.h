/**
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * Authors:
 *   yanran <yanran.hfs@taobao.com>
 *     - some work details if you want
 */

#ifndef OCEANBASE_COMMON_OB_ROLE_MGR_H_
#define OCEANBASE_COMMON_OB_ROLE_MGR_H_

#include "ob_atomic.h"
#include <tbsys.h>

namespace oceanbase
{
  namespace common
  {
    /// @brief ObRoleMgr管理了进程的角色和状态
    class ObRoleMgr
    {
    public:
      enum Role
      {
        MASTER = 1,
        SLAVE = 2,
        STANDALONE = 3, // used for test
      };

      /// @brief Master和Slave的状态位
      /// ERROR状态: 错误状态
      /// INIT状态: 初始化
      /// ACTIVE状态: 提供正常服务
      /// SWITCHING: Slave切换为Master过程中的状态
      /// STOP:
      /// Slave切换为Master过程的状态转换顺序是：
      ///     (SLAVE, ACTIVE) -> (MASTER, SWITCHING) -> (MASTER, ACTIVE)
      enum State
      {
        ERROR = 1,
        INIT = 2,
        NOTSYNC = 3,
        ACTIVE = 4,
        SWITCHING = 5,
        STOP = 6,
        HOLD = 7,
      };

    public:
      ObRoleMgr()
      {
        role_ = MASTER;
        state_ = INIT;
      }

      virtual ~ObRoleMgr() { }

      /// @brief 获取Role
      inline Role get_role() const {return role_;}

      /// @brief 修改Role
      inline void set_role(const Role role) 
      {
        atomic_exchange(reinterpret_cast<uint32_t*>(&role_), role);
        TBSYS_LOG(INFO, "set_role=%d state=%d", role_, state_);
      }

      /// 获取State
      inline State get_state() const {return state_;}

      /// 修改State
      inline void set_state(const State state)
      {
        atomic_exchange(reinterpret_cast<uint32_t*>(&state_), state);
        TBSYS_LOG(INFO, "set_state=%d role=%d", state_, role_);
      }

      inline const char* get_role_str() const
      {
        switch (role_)
        {
          case MASTER:       return "MASTER";
          case SLAVE:        return "SLAVE";
          case STANDALONE:   return "STANDALONE";
          default:           return "UNKNOWN";
        }
      }

      inline const char* get_state_str() const
      {
        switch (state_)
        {
          case ERROR:     return "ERROR";
          case INIT:      return "INIT";
          case ACTIVE:    return "ACTIVE";
          case SWITCHING: return "SWITCHING";
          case STOP:      return "STOP";
          case HOLD:      return "HOLD";
          default:        return "UNKNOWN";
        }
      }

      inline bool is_master() const
      {
        return (role_ == ObRoleMgr::MASTER) && (state_ == ObRoleMgr::ACTIVE);
      }
    private:
      Role role_;
      State state_;
    };
  } // end namespace common
} // end namespace oceanbase

#endif // OCEANBASE_COMMON_OB_ROLE_MGR_H_
