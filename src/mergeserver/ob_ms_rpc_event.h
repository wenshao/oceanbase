#ifndef OCEANBASE_MERGER_RPC_EVENT_H_
#define OCEANBASE_MERGER_RPC_EVENT_H_

#include "ob_rpc_event.h"

namespace oceanbase
{
  namespace common
  {
    class ObPacket;
    class ObScanner;
    class ObReadParam;
  }

  namespace mergeserver
  {
    class ObMergerRequestEvent;
    class ObMergerRpcEvent:public ObCommonRpcEvent
    {
    public:
      enum
      {
        SCAN_RPC,
        GET_RPC
      };
      ObMergerRpcEvent();
      virtual ~ObMergerRpcEvent();
    
    public:
      // reset stat for reuse
      void reset(void);
      
      /// client for request event check
      uint64_t get_client_id(void) const;
      const ObMergerRequestEvent * get_client_request(void) const;

      // set eventid and client request in the init step
      virtual int init(const uint64_t client_id, ObMergerRequestEvent * request);

      /// handle the response of read param
      virtual tbnet::IPacketHandler::HPRetCode handlePacket(tbnet::Packet * packet, void *args);
    
      /// print info for debug
      void print_info(FILE * file) const;

      int32_t get_req_type()const
      {
        return req_type_;
      }
      void set_req_type(const int32_t req_type)
      {
        req_type_ = req_type;
      }
      void set_timeout_us(const int64_t timeout_us)
      {
        timeout_us_ = timeout_us;
      }
      int64_t get_timeout_us()const
      {
        return timeout_us_;
      }
      
    private:
      // check inner stat
      inline bool check_inner_stat(void) const;
      
      // deserialize the response packet 
      int deserialize_packet(common::ObPacket & packet, common::ObScanner & result);
      
      // parse the packet
      int parse_packet(tbnet::Packet * packet, void * args);

    protected:
      int32_t req_type_;
      // the request id
      uint64_t client_request_id_;
      ObMergerRequestEvent * client_request_;
      int64_t timeout_us_;
    };
    
    bool ObMergerRpcEvent::check_inner_stat(void) const
    {
      return (client_request_ != NULL);
    }

  }
}

#endif // OCEANBASE_MERGER_RPC_EVENT_H_
