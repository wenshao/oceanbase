#ifndef OB_STRING_SEARCH_H_
#define OB_STRING_SEARCH_H_

#include "ob_define.h"
#include "ob_string.h"

namespace oceanbase 
{ 
  namespace common 
  {
    class ObStringSearch
    {
    public:
      /// cal pattern print
      static uint64_t cal_print(const ObString & pattern);
      /// find pattern in text using kr algorithm
      static int64_t kr_search(const ObString & pattern, const ObString & text);
      static int64_t kr_search(const ObString & pattern, const uint64_t pattern_print, const ObString & text);
      /// find pattern in text using fast search algorithm
      static int64_t fast_search(const ObString & pattern, const ObString & text);
    
    private: 
      static uint64_t cal_print(const char * patter, const int64_t pat_len);
      static int64_t kr_search(const char * pattern, const int64_t pat_len, 
          const char* text, const int64_t text_len);
      
      static int64_t kr_search(const char * pattern, const int64_t pat_len, 
          const uint64_t pat_print, const char* text, const int64_t text_len);
      
      static int64_t fast_search(const char * pattern, const int64_t pat_len, 
          const char * text, const int64_t text_len);
    };
  }
}

#endif //OB_STRING_SEARCH_H_

