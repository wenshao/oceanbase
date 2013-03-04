
#include "ob_ms_list.h"

using namespace oceanbase;
using namespace common;

const char* ob_server_to_string(const oceanbase::common::ObServer& server);

MsList::MsList()
    : rs_(), ms_list_(), ms_iter_(0), client_(NULL), buff_(),
      rwlock_(), initialized_(false)
{
}

MsList::~MsList()
{
  clear();
}

int MsList::init(const ObServer &rs, ObClientManager *client)
{
  int ret = OB_SUCCESS;
  if (0 == rs.get_ipv4() || 0 == rs.get_port() || NULL == client) 
  {
    TBSYS_LOG(ERROR, "init error, arguments are invalid, rs=%s client=NULL",
            ob_server_to_string(rs));
    ret = OB_INVALID_ARGUMENT;
  }
  else
  {
    rs_ = rs;
    client_ = client;
    initialized_ = true;
    TBSYS_LOG(INFO, "MsList initialized succ, rs=%s client=%p",
            ob_server_to_string(rs_), client_);
  }

  if (OB_SUCCESS == ret)
  {
    ret = update();
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "update ms list failed");
    }
  }

  return ret;
}

void MsList::clear()
{
  rs_ = ObServer();
  ms_list_.clear();
  atomic_exchange(&ms_iter_, 0);
  initialized_ = false;
}

int MsList::update()
{
  int ret = OB_SUCCESS;
  static const int32_t MY_VERSION = 1;
  const int64_t timeout = 1000000;

  update_mutex_.lock();

  std::vector<common::ObServer> new_ms;

  ObDataBuffer data_buff(buff_.ptr(), buff_.capacity());
  ObResultCode res;

  if (!initialized_)
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(ERROR, "MsList has not been initialized, "
        "this should not be reached");
  }

  if (OB_SUCCESS == ret)
  {
    ret = client_->send_request(rs_, OB_GET_MS_LIST, MY_VERSION, timeout, data_buff);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "failed to send request, ret=%d", ret);
    }
  }

  if (OB_SUCCESS == ret)
  {
    data_buff.get_position() = 0;
    ret = res.deserialize(data_buff.get_data(), data_buff.get_capacity(), data_buff.get_position());
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "ObResultCode deserialize error, ret=%d", ret);
    }
  }

  int32_t ms_num = 0;
  if (OB_SUCCESS == ret)
  {
    ret = serialization::decode_vi32(data_buff.get_data(), data_buff.get_capacity(), data_buff.get_position(), &ms_num);
    if(OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "decode ms num fail:ret[%d]", ret);
    }
    else
    {
      TBSYS_LOG(DEBUG, "ms server number[%d]", ms_num);
    }
  }

  ObServer ms;
  for(int32_t i = 0;i<ms_num && OB_SUCCESS == ret;i++)
  {
    ret = ms.deserialize(data_buff.get_data(), data_buff.get_capacity(), data_buff.get_position());
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "deserialize ms server fail:ret[%d]", ret);
    }
    else
    {
      new_ms.push_back(ms);
    }
  }
  
  update_mutex_.unlock();

  if (OB_SUCCESS == ret)
  {
    rwlock_.wrlock();
    if (!list_equal_(new_ms))
    {
      TBSYS_LOG(INFO, "Mergeserver List is modified, get the most updated, "
          "MS num=%zu", new_ms.size());
      list_copy_(new_ms);
    }
    else
    {
      TBSYS_LOG(DEBUG, "Mergeserver List do not change, MS num=%zu",
          new_ms.size());
    }
    rwlock_.unlock();
  }

  return ret;
}

const ObServer MsList::get_one()
{
  ObServer ret;
  rwlock_.rdlock();
  if (ms_list_.size() > 0)
  {
    uint64_t i = atomic_inc(&ms_iter_);
    ret = ms_list_[i % ms_list_.size()];
  }
  rwlock_.unlock();
  return ret;
}

void MsList::runTimerTask()
{
  update();
}

bool MsList::list_equal_(const std::vector<ObServer> &list)
{
  bool ret = true;
  if (list.size() != ms_list_.size())
  {
    ret = false;
  }
  else
  {
    for (unsigned i = 0; i < ms_list_.size(); i++)
    {
      if (!(list[i] == ms_list_[i]))
      {
        ret = false;
        break;
      }
    }
  }
  return ret;
}

void MsList::list_copy_(const std::vector<ObServer> &list)
{
  ms_list_.clear();
  std::vector<ObServer>::const_iterator iter;
  for (iter = list.begin(); iter != list.end(); iter++)
  {
    ms_list_.push_back(*iter);
    TBSYS_LOG(INFO, "Add Mergeserver %s", ob_server_to_string(*iter));
  }
}

const char* ob_server_to_string(const ObServer& server)
{
  const int SIZE = 32;
  static char buf[SIZE];
  if (!server.to_string(buf, SIZE))
  {
    snprintf(buf, SIZE, "InvalidServerAddr");
  }
  return buf;
}


