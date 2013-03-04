/*===============================================================
*   (C) 2007-2010 Taobao Inc.
*   
*   
*   Version: 0.1 2010-09-26
*   
*   Authors:
*          ruohai(ruohai@taobao.com)
*   
*
================================================================*/
#include "wait_object.h"
#include "hash/ob_hashutils.h" // for HASH_EXIST

namespace oceanbase
{
  namespace common
  {
    WaitObject::WaitObject() : seq_id_(0), done_count_(0), response_buffer_(NULL), response_(NULL)
    {
    }

    WaitObject::~WaitObject()
    {
    }

    bool WaitObject::wait(const int64_t timeout_in_us)
    {
      bool ret = true;
      int64_t timeout_in_ms = timeout_in_us / 1000;
      if (ret)
      {
        cond_.lock();
        while (done_count_ == 0) {
          if (cond_.wait(static_cast<int32_t>(timeout_in_ms)) == false) {
            ret = false;
            break;
          }
        }
        cond_.unlock();
      }
      return ret;
    }

    void WaitObject::wakeup()
    {
      cond_.lock();
      done_count_++;
      cond_.signal();
      cond_.unlock();
    }

    int64_t WaitObject::get_id() const
    {
      return seq_id_;
    }

    ObPacket* WaitObject::get_response()
    {
      return response_;
    }

    // WaitObjectManager
    WaitObjectManager::WaitObjectManager() : seq_id_(1)
    {
      wait_objects_map_.create(WAIT_OBJECT_MAP_SIZE);
      thread_buffer_ = new ThreadSpecificBuffer(THREAD_BUFFER_SIZE);
    }

    WaitObjectManager::~WaitObjectManager()
    {
      // free 
      tbsys::CThreadGuard guard(&mutex_);
      hash::ObHashMap<int64_t, WaitObject*>::iterator iter;
      for (iter=wait_objects_map_.begin(); iter!=wait_objects_map_.end(); ++iter)
      {
        delete iter->second;
      }

      if (thread_buffer_ != NULL)
      {
        delete thread_buffer_;
        thread_buffer_ = NULL;
      }
    }

    WaitObject* WaitObjectManager::create_wait_object()
    {
      WaitObject* wo = new(std::nothrow) WaitObject();
      if (wo != NULL)
      {
        // create thread buffer
        ThreadSpecificBuffer::Buffer* tb = thread_buffer_->get_buffer();
        if (tb == NULL || tb->used() > 0)
        {
          TBSYS_LOG(ERROR, "get waitobject packet thread buffer failed, buffer %p, used: %d", 
              tb, NULL == tb ? 0 : tb->used());
        }
        else
        {
          char* buf = tb->current();
          wo->response_buffer_ = buf;
          if (!insert_wait_object(wo))
          {
            delete wo;
            wo = NULL;
            TBSYS_LOG(ERROR, "add waitobject into list failed");
          }
          else
          {
            tb->advance(THREAD_BUFFER_ADVANCE_SIZE);
          }
        }
      }
      return wo;
    }

    void WaitObjectManager::destroy_wait_object(WaitObject*& wait_object)
    {
      if (wait_object != NULL) {
        tbsys::CThreadGuard guard(&mutex_);
        wait_objects_map_.erase(wait_object->seq_id_);
        delete wait_object;
        wait_object = NULL;
        ThreadSpecificBuffer::Buffer* tb = thread_buffer_->get_buffer();
        if (tb != NULL)
        {
          tb->reset();
        }
      }
    }

    bool WaitObjectManager::wakeup_wait_object(const int64_t id, tbnet::Packet* response)
    {
      bool ret = true;
      tbsys::CThreadGuard guard(&mutex_);
      WaitObject* wait_object = NULL;
      if (wait_objects_map_.get(id, wait_object) != hash::HASH_EXIST)
      {
        TBSYS_LOG(INFO, "wait object not found, id: [%ld]", id);
      }
      else
      {
        if (response != NULL && response->isRegularPacket())
        {
          ObPacket* packet = dynamic_cast<ObPacket*>(response);
          if (packet != NULL)
          {
            int64_t total_size = sizeof(ObPacket) + packet->get_packet_buffer()->get_position();
            char* res_buf = wait_object->response_buffer_;
            memcpy(res_buf, packet, total_size);
            wait_object->response_ = (ObPacket*)res_buf;
            res_buf += sizeof(ObPacket);
            wait_object->response_->set_packet_buffer(res_buf, packet->get_packet_buffer()->get_position());
          }
          else
          {
            TBSYS_LOG(WARN, "receive packet is not ObPacket, pcode: %d", response->getPCode());
            ret = false;
          }
        }

        // always add the done count
        wait_object->wakeup();
      }

      return ret;
    }

    bool WaitObjectManager::insert_wait_object(WaitObject* wait_object)
    {
      bool ret = false;
      if (wait_object != NULL) {
        tbsys::CThreadGuard guard(&mutex_);
        seq_id_++;
        if (seq_id_ <= 0)
          seq_id_ = 1;
        wait_object->seq_id_ = seq_id_;
        int rc = wait_objects_map_.set(seq_id_, wait_object);
        ret = (rc != -1);
      }
      return ret;
    }
    
  } /* common */
} /* oceanbase */
