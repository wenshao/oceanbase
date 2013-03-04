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
 *        - some work details if you want
 *               
 */

#include "ob_server.h"
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>

namespace oceanbase 
{ 
  namespace common 
  {

    // --------------------------------------------------------
    // class ObServer implements
    // --------------------------------------------------------
    uint32_t ObServer::convert_ipv4_addr(const char *ip)
    {
      if (NULL == ip) return 0;
      uint32_t x = inet_addr(ip);
      if (x == INADDR_NONE) 
      {
        struct hostent *hp = NULL;
        if ((hp = gethostbyname(ip)) == NULL) 
        {
          return 0;
        }
        x = ((struct in_addr *)hp->h_addr)->s_addr;
      }
      return x;
    }

    bool ObServer::to_string(char* buffer, const int32_t size) const
    {
      bool res = false;
      if (NULL != buffer && size > 0)
      {
        if (version_ == IPV4) 
        {
          // ip.v4_ is network byte order 
          if (port_ > 0) {
            snprintf(buffer, size, "%d.%d.%d.%d:%d", 
                (this->ip.v4_ & 0xFF),
                (this->ip.v4_ >> 8) & 0xFF,
                (this->ip.v4_ >> 16) & 0xFF,
                (this->ip.v4_ >> 24) & 0xFF,
                port_);
          } else {
            snprintf(buffer, size, "%d.%d.%d.%d", 
                (this->ip.v4_ & 0xFF),
                (this->ip.v4_ >> 8) & 0xFF,
                (this->ip.v4_ >> 16) & 0xFF,
                (this->ip.v4_ >> 24) & 0xFF);
          }
        }
        res = true;
      }
      return res;
    }

    bool ObServer::ip_to_string(char* buffer, const int32_t size) const
    {
      bool res = false;
      if (NULL != buffer && size > 0)
      {
        if (version_ == IPV4) 
        {
          // ip.v4_ is network byte order 
          snprintf(buffer, size, "%d.%d.%d.%d", 
              (this->ip.v4_ & 0xFF),
              (this->ip.v4_ >> 8) & 0xFF,
              (this->ip.v4_ >> 16) & 0xFF,
              (this->ip.v4_ >> 24) & 0xFF);
        }
        res = true;
      }
      return res;
    }
 
    const char* ObServer::to_cstring() const
    {
      static __thread char buff[OB_IP_STR_BUFF];
      memset(buff, 0, OB_IP_STR_BUFF);
      to_string(buff, OB_IP_STR_BUFF);
      return buff;
    }
    
    bool ObServer::set_ipv4_addr(const char* ip, const int32_t port)
    {
      bool res = true;
      if (NULL == ip || port <= 0)
      {
        res = false;
      }
      if (res) {
        version_ = IPV4;
        port_ = port;
        this->ip.v4_ = convert_ipv4_addr(ip);
      }
      return res;
    }

    bool ObServer::set_ipv4_addr(const int32_t ip, const int32_t port)
    {
      version_ = IPV4;
      this->ip.v4_ = ip;
      this->port_ = port;
      return true;
    }
    //this is only for test 
    void ObServer::reset_ipv4_10(int ip) 
    {
      this->ip.v4_ = this->ip.v4_ & 0xFFFFFF00L;
      this->ip.v4_ += ip;
    }

    int64_t ObServer::get_ipv4_server_id() const
    {
      int64_t server_id = 0;
      if (version_ == IPV4)
      {
        server_id = this->port_;
        server_id <<= 32;
        server_id |= this->ip.v4_;
      }
      return server_id;
    }

    bool ObServer::operator ==(const ObServer& rv) const
    {/*
      bool res = true;
      if (version_ != rv.version_)
      {
        res = false;
      }
      if (res)
      {
        if (version_ == IPV4)
        {
          if (ip.v4_ != rv.ip.v4_)
          {
            res = false;
          }
        }
        else if (version_ == IPV6)
        {
          if (ip.v6_[0] != rv.ip.v6_[0] ||
              ip.v6_[1] != rv.ip.v6_[1] ||
              ip.v6_[2] != rv.ip.v6_[2] ||
              ip.v6_[3] != rv.ip.v6_[3] )
          {
            res = false;
          }
        }
        else 
        {
          assert(false); //never reach this
        }
        
      }
      */
      return (!(*this < rv)) && (!(rv < *this));
    }
    bool ObServer::compare_by_ip(const ObServer& rv) const
    {
      bool res = true;
      if (version_ != rv.version_)
      {
        res = version_ < rv.version_;
      }
      else
      {
        if (version_ == IPV4)
        {
          res = ip.v4_ < rv.ip.v4_;
        }
        else if (version_ == IPV6)
        {
          res = memcmp(ip.v6_, rv.ip.v6_, sizeof(uint32_t) * 4) < 0;
        }
      }
      return res;
    }
    bool ObServer::operator < (const ObServer& rv) const
    {
      bool res = compare_by_ip(rv);
      if (!res)
      {
        res = port_ < rv.port_;
      }
      return res;
    }

    int32_t ObServer::get_version() const
    {
      return version_;
    }
    int32_t ObServer::get_port() const
    {
      return port_;
    }
    int32_t ObServer::get_ipv4() const
    {
      return ip.v4_;
    }
    void ObServer::set_port(int32_t port)
    {
      port_ = port;
    }

    DEFINE_SERIALIZE(ObServer)
    {
      int ret = OB_ERROR;
      ret = serialization::encode_vi32(buf, buf_len, pos, version_);

      if (ret == OB_SUCCESS)
        ret = serialization::encode_vi32(buf, buf_len, pos, port_);

      if (ret == OB_SUCCESS)
      {
        if (version_ == IPV4)
        {
          ret = serialization::encode_vi32(buf, buf_len, pos, ip.v4_);
        } else
        {
          // ipv6
          for (int i=0; i<4; i++)
          {
            ret = serialization::encode_vi32(buf, buf_len, pos, ip.v6_[i]);
            if (ret != OB_SUCCESS)
              break;
          }
        }
      }

      return ret;
    }

    DEFINE_DESERIALIZE(ObServer)
    {
      int ret = OB_ERROR;
      ret = serialization::decode_vi32(buf, data_len, pos, &version_);

      if (ret == OB_SUCCESS)
        ret = serialization::decode_vi32(buf, data_len, pos, &port_);

      if (ret == OB_SUCCESS)
      {
        if (version_ == IPV4)
        {
          ret = serialization::decode_vi32(buf, data_len, pos, (int32_t*)&(ip.v4_));
        } else
        {
          for (int i=0; i<4; i++)
          {
            ret = serialization::decode_vi32(buf, data_len, pos, (int32_t*)(ip.v6_ + i));
            if (ret != OB_SUCCESS)
              break;
          }
        }
      }

      return ret;
    }

    DEFINE_GET_SERIALIZE_SIZE(ObServer)
    {
      int64_t total_size = 0;
      total_size += serialization::encoded_length_vi32(version_);
      total_size += serialization::encoded_length_vi32(port_);

      if (version_ == IPV4)
      {
        total_size += serialization::encoded_length_vi32(ip.v4_);
      } else
      {
        // ipv6
        for (int i=0; i<4; i++)
        {
          total_size += serialization::encoded_length_vi32(ip.v6_[i]);
        }
      }

      return total_size;
    }

  } // end namespace common
} // end namespace oceanbase

