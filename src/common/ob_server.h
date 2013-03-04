
/*
 *   (C) 2007-2010 Taobao Inc.
 *   
 *   This program is free software; you can redistribute it and/or modify
 *   it under the terms of the GNU General Public License version 2 as
 *   published by the Free Software Foundation.
 *       
 *         
 *         
 *   Version: 0.1
 *           
 *   Authors:
 *      qushan <qushan@taobao.com>
 *        - base data structure, maybe modify in future
 *               
 */
#ifndef OCEANBASE_COMMON_OB_SERVER_H_
#define OCEANBASE_COMMON_OB_SERVER_H_

#include <assert.h>
#include "ob_define.h"
#include "serialization.h"

namespace oceanbase 
{ 
  namespace common
  {
    class ObServer 
    {
      public:
        static const int32_t IPV4 = 4;
        static const int32_t IPV6 = 6;

        ObServer()
          : version_(IPV4), port_(0)
        {
          this->ip.v4_ = 0;
          memset(&this->ip.v6_, 0, sizeof(ip.v6_));
        }

        ObServer(const int32_t version, const char* ip, const int32_t port)
        {
          assert(version == IPV4 || version == IPV6);
          if (version == IPV4)
            set_ipv4_addr(ip, port);
          // TODO ipv6 addr?
        }

        void reset()
        {
          port_ = 0;
          this->ip.v4_ = 0;
          memset(&this->ip.v6_, 0, sizeof(ip.v6_));
        }

        static uint32_t convert_ipv4_addr(const char *ip);

        bool to_string(char* buffer, const int32_t size) const;
        bool ip_to_string(char* buffer, const int32_t size) const;
        const char* to_cstring() const; // use this carefully, the content of the returned buffer will be modified by the next call

        bool set_ipv4_addr(const char* ip, const int32_t port);
        bool set_ipv4_addr(const int32_t ip, const int32_t port);

        int64_t get_ipv4_server_id() const;

        bool operator ==(const ObServer& rv) const;
        bool operator < (const ObServer& rv) const;
        bool compare_by_ip(const ObServer& rv) const;
        int32_t get_version() const;
        int32_t get_port() const;
        int32_t get_ipv4() const;
        void set_port(int32_t port);


        void reset_ipv4_10(int ip = 10);

        NEED_SERIALIZE_AND_DESERIALIZE;

      private:
        int32_t version_;
        int32_t port_;
        struct {
          uint32_t v4_;
          uint32_t v6_[4];
        } ip;
    };


  } // end namespace common
} // end namespace oceanbase

#endif //OCEANBASE_COMMON_OB_SERVER_H_

