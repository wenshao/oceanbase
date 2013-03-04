#include "assert.h"
#include "stdlib.h"
#include "dlist.h"
namespace oceanbase
{
  namespace common
  {
    DLink::DLink()
    {
      prev_ = NULL;
      next_ = NULL;
    }
  
    // insert one node before this node
    void DLink::add_before(DLink *e)
    {
      add(prev_, e, this);
    }
  
    // insert one node after this node
    void DLink::add_after(DLink *e)
    {
      add(this, e, next_);
    }
  
    // remove node from list
    void DLink::unlink()
    {
      prev_->next_ = next_;
      next_->prev_ = prev_;
      prev_ = NULL;
      next_ = NULL;
    }
  
    void DLink::add(DLink *prev, DLink *e, DLink *next)
    {
      prev->next_ = e;
      e->prev_ = prev;
      next->prev_ = e;
      e->next_ = next;
    }
  
  //------------dlist define--------------
    DList::DList()
    {
      header_.next_ = &header_;
      header_.prev_ = &header_;
      size_ = 0;
    }
  
    // get the header
    DLink* DList::get_header()
    {
      return &header_;
    }
  
    // insert the node to the tail
    bool DList::add_last(DLink *e)
    {
      bool ret = true;
      if(!e)
      {
        ret = false;
      } 
      else 
      { 
        header_.add_before(e);
        size_++;
      }
      return ret;
    }
  
    // insert the node to the head
    bool DList::add_first(DLink *e)
    { 
      bool ret = true;
      if(!e)
      {
        ret = false;
      }
      else
      {
        header_.add_after(e);
        size_++;
      }
      return ret;
    }
  
    // move the node to the head
    bool DList::move_to_first(DLink *e)
    {
      bool ret = true;
      if(e == &header_ || e == NULL)
      {
        ret = false;
      } 
      else {
        e->unlink();
        size_--;
        ret = add_first(e);
      }
      return ret;
    }
  
    // move the node to the tail
    bool DList::move_to_last(DLink *e)
    {
      bool ret = true;
      if(e == &header_ || e == NULL)
      {
        ret = false;
      } 
      else 
      {
        e->unlink();
        size_--;
        ret = add_last(e);
      }
      return ret;
    }
  
   // remove the node at tail
    DLink* DList::remove_last()
    {
      return remove(header_.prev_);
    }
  
    // remove the node at head
    DLink* DList::remove_first()
    {
      return remove(header_.next_);
    }
  
    DLink* DList::remove(DLink *e)
    {
      DLink* ret = e;
      if (e == &header_ || e == NULL)
      {
        ret = NULL;
      }
      e->unlink();
      size_--;
      return ret;
    }
    
    DLink* DList::get_first()
    {
      DLink* first = header_.next_;
      if (first == &header_)
      {
        first = NULL;
      }
      return first;
    }
  }
    
}
