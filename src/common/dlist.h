#ifndef OCEANBASE_COMMON_DOUBLE_LIST_H_
#define OCEANBASE_COMMON_DOUBLE_LIST_H_
#include "ob_define.h"
namespace oceanbase
{
  namespace common
  {
   
   class DList;
   
   /** node in list ,it has no value, if you want to use it, you 
   *  should inherit it, and then set the value, and you should manage
   *  the memeory by self.
   */
   class DLink
   {
     public:
       // constructor,have no value
       DLink();
    
       // get the next node
       inline DLink* get_next() const
       {
         return next_;
       }
       
       // get the prev node
       inline DLink* get_prev() const
       {
         return prev_;
       }
      
       virtual ~DLink()
       {
       }
     protected:
       // insert one node before this node
       void add_before(DLink *e);
       
       // insert one node after this node
       void add_after(DLink *e);
       
       // remove node from list
       void unlink();
         
       void add(DLink *prev, DLink *e, DLink *next);
     
     protected:
       //DList *list_;
       DLink *prev_;// this will not delete pointer
       DLink *next_;// this will not delete pointer
     
     friend class DList;
   };
   
   //double list 
   class DList
   {
     public:
       DList();
       
       // get the header
       DLink* get_header();

       //get the first node
       DLink* get_first();
       
       // insert the node to the tail
       bool add_last(DLink *e);
       
       // insert the node to the head
       bool add_first(DLink *e);
       
       // move the node to the head
       bool move_to_first(DLink *e);
       
       // move the node to the tail
       bool move_to_last(DLink *e);
       
       // remove the node at tail
       DLink* remove_last();
       
       // remove the node at head
       DLink* remove_first();
       
       //the list is empty or not
       inline bool is_empty() const
       {
         return header_.next_ == &header_;
       }
       
       // get the list size
       inline int get_size() const
       {
         return size_;
       }
     
       DLink* remove(DLink *e);
     
     private:
       DISALLOW_COPY_AND_ASSIGN(DList);
       DLink header_;
       int  size_;
   };
  
 }
}
#endif
