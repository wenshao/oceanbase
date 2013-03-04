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
 *   ruohai <ruohai@taobao.com>
 */

#include "ob_packet_queue_thread.h"
#include "ob_atomic.h"

using namespace oceanbase::common;

static long *get_no_ptr(void)
{
  static __thread long p = 0;
  return &p;
}

static void set_thread_no(long n)
{
  long * p = get_no_ptr();
  if (NULL != p) *p = n;
}

static long get_thread_no(void)
{
  long *p = get_no_ptr();
  long no = 0;
  if (NULL != p)
  {
    no = *p;
  }
  return no;
}

ObPacketQueueThread::ObPacketQueueThread()
{
  _stop = 0;
  wait_finish_ = false;
  waiting_ = false;
  handler_ = NULL;
  args_ = NULL;
  queue_.init();
  session_id_ = 1;
  next_wait_map_.create(MAX_THREAD_COUNT);
  max_waiting_thread_count_  = 0;
  waiting_thread_count_ = 0;
  next_packet_buffer_ = NULL;
}

ObPacketQueueThread::~ObPacketQueueThread()
{
  stop();
  next_wait_map_.destroy();
  if (NULL != next_packet_buffer_)
  {
    ob_free(next_packet_buffer_);
    next_packet_buffer_ = NULL;
  }
}

void ObPacketQueueThread::setThreadParameter(int thread_count, tbnet::IPacketQueueHandler* handler, void* args)
{
  setThreadCount(thread_count);
  handler_ = handler;
  args_ = args;

  //default all of threads for wait..
  //FIXME: streaming interface may use all the work threads, we will add
  //extra packet queue to handle the control packets in the funture
  if (0 == max_waiting_thread_count_)
  {
    max_waiting_thread_count_  = thread_count; 
  }

  if (NULL == next_packet_buffer_)
  {
    next_packet_buffer_ = reinterpret_cast<char*>(ob_malloc(thread_count * MAX_PACKET_SIZE));
  }
}

void ObPacketQueueThread::stop(bool wait_finish)
{
  cond_.lock();
  _stop = true;
  wait_finish_ = wait_finish;
  cond_.broadcast();
  cond_.unlock();
}

bool ObPacketQueueThread::push(ObPacket* packet, int max_queue_len, bool block)
{
  if (_stop || _thread == NULL)
  {
    return true;
  }

  if (is_next_packet(packet))
  {
    if (!wakeup_next_thread(packet))
    {
      // free packet;
    }
    return true;
  }

  if (max_queue_len > 0 && queue_.size() >= max_queue_len)
  {
    pushcond_.lock();
    waiting_ = true;
    while (_stop == false && queue_.size() >= max_queue_len && block)
    {
      pushcond_.wait(1000);
    }
    waiting_ = false;
    if (queue_.size() >= max_queue_len && !block)
    {
      pushcond_.unlock();
      return false;
    }
    pushcond_.unlock();

    if (_stop)
    {
      return true;
    }
  }

  cond_.lock();
  queue_.push(packet);
  cond_.signal();
  cond_.unlock();
  return true;
}

void ObPacketQueueThread::pushQueue(ObPacketQueue& packet_queue, int max_queue_len)
{
  if (_stop)
  {
    return;
  }

  if (max_queue_len > 0 && queue_.size() >= max_queue_len)
  {
    pushcond_.lock();
    waiting_ = true;
    while (_stop == false && queue_.size() >= max_queue_len)
    {
      pushcond_.wait(1000);
    }
    waiting_ = false;
    pushcond_.unlock();
    if (_stop)
    {
      return;
    }
  }

  cond_.lock();
  packet_queue.move_to(&queue_);
  cond_.signal();
  cond_.unlock();
}

bool ObPacketQueueThread::is_next_packet(ObPacket* packet) const
{
  bool ret = false;
  if (NULL != packet)
  {
    int32_t pcode = packet->get_packet_code();
    int64_t session_id = packet->get_session_id();
    ret = (((pcode == OB_SESSION_NEXT_REQUEST) || (OB_SESSION_END == pcode)) && (session_id != 0));
  }
  return ret;
}

int64_t ObPacketQueueThread::generate_session_id()
{
  return atomic_inc(&session_id_);
}

int64_t ObPacketQueueThread::get_session_id(ObPacket* packet) const
{
  int64_t session_id = packet->get_session_id(); 
  return session_id;
}

ObPacket* ObPacketQueueThread::clone_next_packet(ObPacket* packet, int64_t thread_no) const
{
  char *clone_packet_ptr = NULL;
  ObPacket* clone_packet = NULL;

  ObDataBuffer* buf = packet->get_packet_buffer();
  int64_t inner_buf_size = buf->get_position();
  int64_t total_size = sizeof(ObPacket) + inner_buf_size;

  if (NULL != next_packet_buffer_ && total_size <= MAX_PACKET_SIZE)
  {
    clone_packet_ptr = next_packet_buffer_ + thread_no * MAX_PACKET_SIZE;
    memcpy(clone_packet_ptr, packet, total_size);
    clone_packet = reinterpret_cast<ObPacket*>(clone_packet_ptr);
    clone_packet->set_packet_buffer(clone_packet_ptr + sizeof(ObPacket), inner_buf_size);
  }

  return clone_packet;
}

bool ObPacketQueueThread::wakeup_next_thread(ObPacket* packet)
{
  bool ret = false;

  int64_t session_id = get_session_id(packet);

  WaitObject wait_object;
  int hash_ret = next_wait_map_.get(session_id, wait_object);
  if (hash::HASH_EXIST != hash_ret)
  {
    // no thread wait for this packet;
    ret = false;
  }
  else
  {
    next_cond_[wait_object.thread_no_].lock();
    if ((NULL != wait_object.packet_) && (OB_SESSION_END != packet->get_packet_code()))
    {
      TBSYS_LOG(ERROR, "wakeup thread no = %ld, session_id=%ld error, packet=%p",
        wait_object.thread_no_, session_id, wait_object.packet_);
      // impossible! notify early by another next packet.
      ret = false;
    }
    else
    {
      if(OB_SESSION_END == packet->get_packet_code())
      {
        wait_object.end_session();
      }
      else
      {
        // got next packet, wakeup wait thread;
        wait_object.packet_ = clone_next_packet(packet, wait_object.thread_no_);
      }
      int overwrite = 1;
      hash_ret = next_wait_map_.set(session_id, wait_object, overwrite);
      if (hash::HASH_OVERWRITE_SUCC != hash_ret)
      {
        TBSYS_LOG(WARN, "rewrite set session =%ld packet=%p error,hash_ret=%d", 
            session_id, wait_object.packet_, hash_ret);
      }
      next_cond_[wait_object.thread_no_].signal();
      ret = true;
    }
    next_cond_[wait_object.thread_no_].unlock();
  }

  return ret;
}

int ObPacketQueueThread::prepare_for_next_request(int64_t session_id)
{
  int ret = OB_SUCCESS;

  WaitObject wait_object;
  wait_object.thread_no_ = get_thread_no();
  wait_object.packet_ = NULL;

  int hash_ret = next_wait_map_.set(session_id, wait_object);

  if (-1 == hash_ret)
  {
    TBSYS_LOG(ERROR, "insert thread no = %ld, session_id=%ld error",
      wait_object.thread_no_, session_id);
    ret = OB_ERROR;
  }
  else if (hash::HASH_EXIST == hash_ret)
  {
    hash_ret = next_wait_map_.get(session_id, wait_object);
    if (hash::HASH_EXIST == hash_ret && NULL != wait_object.packet_ && (!wait_object.is_session_end()))
    {
      TBSYS_LOG(ERROR, "insert thread no = %ld, session_id=%ld exist, and packet not null and not end session = %p",
        wait_object.thread_no_, session_id, wait_object.packet_);
      // impossible , either last wait not set to NULL or next request 
      // reached before pepare..
      ret = OB_ERROR;
    }
  }

  return ret;
}

int ObPacketQueueThread::destroy_session(int64_t session_id)
{
  int ret = OB_SUCCESS;

  ret = next_wait_map_.erase(session_id);
  if (-1 == ret)
  {
    ret = OB_ERROR;
  }
  else if (hash::HASH_NOT_EXIST == ret)
  {
    TBSYS_LOG(WARN, "session_id = %ld not exist, destroy do nothing.", session_id);
    ret = OB_ERROR;
  }
  else
  {
    ret = OB_SUCCESS;
  }

  return ret;
}

int ObPacketQueueThread::wait_for_next_request(int64_t session_id, ObPacket* &next_request, const int64_t timeout)
{
  int ret = OB_SUCCESS;
  int hash_ret = 0;
  WaitObject wait_object;
  next_request = NULL; 
  uint64_t oldv = 0;

  hash_ret = next_wait_map_.get(session_id, wait_object);
  if (hash::HASH_NOT_EXIST == hash_ret)
  {
    TBSYS_LOG(WARN, "session_id =%ld not exist, prepare first.", session_id);
    ret = OB_ERR_UNEXPECTED; //NOT_PREPARED;
  }

  while (OB_SUCCESS == ret)
  {
    oldv = waiting_thread_count_;
    if (oldv > max_waiting_thread_count_)
    {
      TBSYS_LOG(INFO, "current wait thread =%ld >= max wait =%ld", 
          oldv, max_waiting_thread_count_);
      ret = OB_EAGAIN;
    }
    else if (atomic_compare_exchange(&waiting_thread_count_, oldv+1, oldv) == oldv)
    {
      next_cond_[wait_object.thread_no_].lock();
      hash_ret = next_wait_map_.get(session_id, wait_object);
      if (hash::HASH_NOT_EXIST == hash_ret)
      {
        TBSYS_LOG(WARN, "session_id =%ld not exist, prepare first.", session_id);
        ret = OB_ERR_UNEXPECTED; //NOT_PREPARED;
      }
      else if(wait_object.is_session_end())
      {
        ret = OB_NET_SESSION_END;
      }
      else if (NULL == wait_object.packet_)
      {
        int32_t timeout_ms = static_cast<int32_t>(timeout/1000);
        if (timeout_ms > 0 && next_cond_[wait_object.thread_no_].wait(timeout_ms))
        {
          hash_ret = next_wait_map_.get(session_id, wait_object);
          if((hash::HASH_EXIST == hash_ret) && wait_object.is_session_end())
          {
            ret = OB_NET_SESSION_END;
          }
          else if (hash::HASH_EXIST != hash_ret || NULL == wait_object.packet_)
          {
            TBSYS_LOG(ERROR, "cannot get packet of session = %ld.", session_id);
            ret = OB_ERROR;
          }
          else
          {
            if (wait_object.is_session_end())
            {
              ret = OB_NET_SESSION_END;
            }
            else
            {
              next_request = wait_object.packet_;
              ret = OB_SUCCESS;
            }
          }
        }
        else
        {
          next_request = NULL;
          ret = OB_RESPONSE_TIME_OUT; //WAIT_TIMEOUT;
        }
      }
      else
      {
        // check if already got the request..
        next_request = wait_object.packet_;
      }

      // clear last packet
      wait_object.packet_ = NULL;
      int overwrite = 1;
      hash_ret = next_wait_map_.set(session_id, wait_object, overwrite);
      if (hash::HASH_OVERWRITE_SUCC != hash_ret)
      {
        TBSYS_LOG(WARN, "rewrite clear session =%ld packet=%p error, hash_ret=%d", 
            session_id, next_request, hash_ret);
      }

      next_cond_[wait_object.thread_no_].unlock();
      atomic_dec(&waiting_thread_count_);
      break;
    }
  }
  if((NULL != next_request) && (OB_SESSION_END == next_request->get_packet_code()))
  {
    ret = OB_NET_SESSION_END;
  }
  return ret;
}

void ObPacketQueueThread::run(tbsys::CThread* thread, void* args)
{
  UNUSED(thread);

  long thread_no = reinterpret_cast<long>(args);
  set_thread_no(thread_no);

  ObPacket* packet = NULL;
  while (!_stop)
  {
    cond_.lock();
    while (!_stop && queue_.size() == 0)
    {
      cond_.wait();
    }
    if (_stop)
    {
      cond_.unlock();
      break;
    }

    packet = (ObPacket*)queue_.pop();
    cond_.unlock();

    if (waiting_)
    {
      pushcond_.lock();
      pushcond_.signal();
      pushcond_.unlock();
    }

    if (packet == NULL) continue;

    if (handler_)
    {
      handler_->handlePacketQueue(packet, args_);
    }
  }

  cond_.lock();
  while (queue_.size() > 0)
  {
    packet = (ObPacket*)queue_.pop();
    cond_.unlock();
    if (handler_ && wait_finish_)
    {
      handler_->handlePacketQueue(packet, args_);
    }
    cond_.lock();
  }
  cond_.unlock();
}

void ObPacketQueueThread::clear()
{
  _stop = false;
  delete[] _thread;
  _thread = NULL;
}

