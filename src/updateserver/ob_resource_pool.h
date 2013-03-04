////===================================================================
 //
 // ob_resource_pool.h updateserver / Oceanbase
 //
 // Copyright (C) 2010 Taobao.com, Inc.
 //
 // Created on 2012-07-19 by Yubai (yubai.lk@taobao.com) 
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
#ifndef  OCEANBASE_UPDATESERVER_RESOURCE_POOL_H_
#define  OCEANBASE_UPDATESERVER_RESOURCE_POOL_H_
#include <pthread.h>
#include "common/ob_define.h"
#include "common/ob_malloc.h"
#include "common/ob_fixed_queue.h"

namespace oceanbase
{
  namespace updateserver
  {
    template <class T, int64_t LOCAL_NUM = 4, int64_t TOTAL_NUM = 256>
    class ObResourcePool
    {
      static const int64_t MAX_THREAD_NUM = 1024;
      static const int64_t WARN_INTERVAL = 60000000L; //60s
      struct Node
      {
        T data;
        Node *next;
        Node() : data(), next(NULL) {};
      };
      struct NodeArray
      {
        Node datas[LOCAL_NUM];
        int64_t index;
        NodeArray() : index(0) {};
      };
      typedef common::ObFixedQueue<Node> NodeQueue;
      typedef common::ObFixedQueue<NodeArray> ArrayQueue;
      public:
        class Guard
        {
          public:
            Guard(ObResourcePool &host) : host_(host),
                                          list_(NULL)
            {
            };
            ~Guard()
            {
              Node *iter = list_;
              while (NULL != iter)
              {
                Node *tmp = iter;
                iter = iter->next;
                tmp->data.reset();
                host_.free_node_(tmp);
              }
              NodeArray *array = host_.get_node_array_();
              if (NULL != array)
              {
                for (int64_t i = array->index - 1; i >= 0; i--)
                {
                  array->datas[i].data.reset();
                }
                array->index = 0;
              }
            };
          public:
            void add_node(Node *node)
            {
              if (NULL != node)
              {
                node->next = list_;
                list_ = node;
              }
            };
          private:
            ObResourcePool &host_;
            Node *list_;
        };
      public:
        ObResourcePool()
        {
          int ret = pthread_key_create(&thread_key_, NULL);
          if (0 != ret)
          {
            TBSYS_LOG(ERROR, "pthread_key_create fail ret=%d", ret);
          }
          array_list_.init(MAX_THREAD_NUM);
          free_list_.init(TOTAL_NUM);
        };
        ~ObResourcePool()
        {
          Node *node = NULL;
          while (common::OB_SUCCESS == free_list_.pop(node))
          {
            node->~Node();
            common::ob_free(node);
          }
          NodeArray *array = NULL;
          while (common::OB_SUCCESS == array_list_.pop(array))
          {
            free_array_(array);
          }
          pthread_key_delete(thread_key_);
        };
      public:
        T *get(Guard &guard)
        {
          T *ret = NULL;
          NodeArray *array = get_node_array_();
          if (NULL != array
              && LOCAL_NUM > array->index)
          {
            ret = &(array->datas[array->index].data);
            array->index += 1;
          }
          if (NULL == ret)
          {
            Node *node = alloc_node_();
            if (NULL != node)
            {
              ret = &(node->data);
              guard.add_node(node);
            }
          }
          return ret;
        };
      private:
        Node *alloc_node_()
        {
          Node *ret = NULL;
          if (common::OB_SUCCESS != free_list_.pop(ret)
              || NULL == ret)
          {
            void *buffer = common::ob_malloc(sizeof(Node), common::ObModIds::OB_UPS_RESOURCE_POOL_NODE);
            if (NULL != buffer)
            {
              ret = new(buffer) Node();
            }
          }
          return ret;
        };
        void free_node_(Node *ptr)
        {
          if (NULL != ptr)
          {
            int64_t cnt = 0;
            int64_t last_warn_time = 0;
            while (common::OB_SUCCESS != free_list_.push(ptr))
            {
              Node *node = NULL;
              if (common::OB_SUCCESS == free_list_.pop(node)
                  && NULL != node)
              {
                node->~Node();
                common::ob_free(node);
              }
              if (TOTAL_NUM < cnt++
                  && tbsys::CTimeUtil::getTime() > (WARN_INTERVAL + last_warn_time))
              {
                last_warn_time = tbsys::CTimeUtil::getTime();
                TBSYS_LOG(ERROR, "free node to list fail count to large %ld, free_list_size=%ld", cnt, free_list_.get_free());
              }
            }
          }
        };

        NodeArray *alloc_array_()
        {
          NodeArray *ret = NULL;
          void *buffer = common::ob_malloc(sizeof(NodeArray), common::ObModIds::OB_UPS_RESOURCE_POOL_ARRAY);
          if (NULL != buffer)
          {
            ret = new(buffer) NodeArray();
          }
          return ret;
        };

        void free_array_(NodeArray *ptr)
        {
          if (NULL != ptr)
          {
            ptr->~NodeArray();
            common::ob_free(ptr);
          }
        };

        NodeArray *get_node_array_()
        {
          NodeArray *ret = (NodeArray*)pthread_getspecific(thread_key_);
          if (NULL == ret)
          {
            if (NULL != (ret = alloc_array_()))
            {
              if (common::OB_SUCCESS != array_list_.push(ret))
              {
                free_array_(ret);
                ret = NULL;
              }
              else
              {
                pthread_setspecific(thread_key_, ret);
                TBSYS_LOG(INFO, "alloc thread specific node_array=%p", ret);
              }
            }
          }
          return ret;
        };
      private:
        pthread_key_t thread_key_;
        ArrayQueue array_list_;
        NodeQueue free_list_;
    };
  }
}

#endif //OCEANBASE_UPDATESERVER_RESOURCE_POOL_H_

