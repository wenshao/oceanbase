////===================================================================
 //
 // ob_spin_rwlock.h / common / Oceanbase
 //
 // Copyright (C) 2010 Taobao.com, Inc.
 //
 // Created on 2011-4-21 by Yubai (yubai.lk@taobao.com) 
 //
 // -------------------------------------------------------------------
 //
 // Description
 //
 //
 // -------------------------------------------------------------------
 // 
 // Change Log
 //
////====================================================================

#ifndef  OCEANBASE_COMMON_SPIN_RWLOCK_H_
#define  OCEANBASE_COMMON_SPIN_RWLOCK_H_
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include "tbsys.h"
#include "ob_atomic.h"
#include "ob_define.h"
#include "tbsys.h"

namespace oceanbase
{
  namespace common
  {
    class SpinRWLock
    {
      public:
        SpinRWLock() : ref_cnt_(0), wait_write_(0)
        {
        };
        ~SpinRWLock()
        {
          if (0 != ref_cnt_ || 0 != wait_write_)
          {
            TBSYS_LOG(ERROR, "invalid ref_cnt=%ld or wait_write_=%ld", ref_cnt_, wait_write_);
          }
        };
      public:
        inline bool try_rdlock()
        {
          bool bret = false;
          int64_t tmp = ref_cnt_;
          if (0 <= tmp)
          {
            int64_t nv = tmp + 1;
            if (tmp == (int64_t)atomic_compare_exchange((uint64_t*)&ref_cnt_, nv, tmp))
            {
              bret = true;
            }
          }
          return bret;
        };
        inline int rdlock()
        {
          int ret = common::OB_SUCCESS;
          int64_t tmp = 0;
          while (true)
          {
            tmp = ref_cnt_;
            if (0 > tmp || 0 < wait_write_)
            {
              // 写优先
              continue;
            }
            else
            {
              int64_t nv = tmp + 1;
              if (tmp == (int64_t)atomic_compare_exchange((uint64_t*)&ref_cnt_, nv, tmp))
              {
                break;
              }
            }
          }
          return ret;
        };
        inline int wrlock()
        {
          int ret = common::OB_SUCCESS;
          int64_t tmp = 0;
          atomic_inc((uint64_t*)&wait_write_);
          while (true)
          {
            tmp = ref_cnt_;
            if (0 != tmp)
            {
              continue;
            }
            else
            {
              int64_t nv = -1;
              if (tmp == (int64_t)atomic_compare_exchange((uint64_t*)&ref_cnt_, nv, tmp))
              {
                break;
              }
            }
          }
          atomic_dec((uint64_t*)&wait_write_);
          return ret;
        };
        inline int unlock()
        {
          int ret = common::OB_SUCCESS;
          int64_t tmp = 0;
          while (true)
          {
            tmp = ref_cnt_;
            if (0 == tmp)
            {
              TBSYS_LOG(ERROR, "need not unlock ref_cnt=%ld wait_write=%ld", ref_cnt_, wait_write_);
              break;
            }
            else if (-1 == tmp)
            {
              int64_t nv = 0;
              if (tmp == (int64_t)atomic_compare_exchange((uint64_t*)&ref_cnt_, nv, tmp))
              {
                break;
              }
            }
            else if (0 < tmp)
            {
              int64_t nv = tmp - 1;
              if (tmp == (int64_t)atomic_compare_exchange((uint64_t*)&ref_cnt_, nv, tmp))
              {
                break;
              }
            }
            else
            {
              TBSYS_LOG(ERROR, "invalid ref_cnt=%ld", ref_cnt_);
            }
          }
          return ret;
        };
      private:
        volatile int64_t ref_cnt_;
        volatile int64_t wait_write_;
    };

    class SpinRLockGuard
    {
      public: 
        SpinRLockGuard(SpinRWLock& lock) 
          : lock_(lock) 
        { 
          lock_.rdlock();
        }
        ~SpinRLockGuard()
        {
          lock_.unlock();
        }
      private:
        SpinRWLock& lock_;
    };

    class SpinWLockGuard
    {
      public: 
        SpinWLockGuard(SpinRWLock& lock) 
          : lock_(lock) 
        { 
          lock_.wrlock();
        }
        ~SpinWLockGuard()
        {
          lock_.unlock();
        }
      private:
        SpinRWLock& lock_;
    };
  }
}

#endif //OCEANBASE_COMMON_SPIN_RWLOCK_H_

