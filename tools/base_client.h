/*
 *  (C) 2007-2010 Taobao Inc.
 *  
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License version 2 as
 *  published by the Free Software Foundation.
 *
 *         base_client.h is for what ...
 *
 *  Version: $Id: base_client.h 2010年11月16日 13时28分20秒 qushan Exp $
 *
 *  Authors:
 *     qushan < qushan@taobao.com >
 *        - some work details if you want
 */


#include <tbnet.h>
#include "common/ob_packet.h"
#include "common/ob_packet_factory.h"
#include "common/ob_client_manager.h"

class BaseClient
{
public:
  int initialize();
  int start();
  int stop();
  int wait();
  inline const oceanbase::common::ObClientManager& get_client_manager() const
  { return client_; }
private:
  tbnet::DefaultPacketStreamer streamer_;
  tbnet::Transport transport_;
  oceanbase::common::ObPacketFactory factory_;
  oceanbase::common::ObClientManager client_;
};

