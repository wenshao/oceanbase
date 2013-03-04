/*
 * (C) 2007-2010 Taobao Inc. 
 *
 * This program is free software; you can redistribute it and/or modify 
 * it under the terms of the GNU General Public License version 2 as 
 * published by the Free Software Foundation.
 *
 * ob_string_search.cpp is for what ...
 *
 * Version: ***: ob_string_search.cpp  Thu Mar 24 10:47:24 2011 fangji.hcm Exp $
 *
 * Authors:
 *   Author fangji.hcm <fangji.hcm@taobao.com>
 *     -some work detail if you want 
 *
 */

#include "tblog.h"
#include "ob_string_search.h"
#include "ob_string.h"

namespace oceanbase
{
  namespace common
  {
    uint64_t ObStringSearch::cal_print(const ObString & pattern)
    {
      const char* pat = pattern.ptr();
      const int64_t pat_len = pattern.length();
      return cal_print(pat, pat_len);
    }

    int64_t ObStringSearch::kr_search(const ObString & pattern, const ObString & text)
    {
      const char* pat = pattern.ptr();
      const int64_t pat_len = pattern.length();
      const char* txt = text.ptr();
      const int64_t txt_len = text.length();
      return kr_search(pat, pat_len, txt, txt_len);
    }

    int64_t ObStringSearch::kr_search(const ObString & pattern, const uint64_t pattern_print, const ObString & text)
    {
      const char* pat = pattern.ptr();
      const int64_t pat_len = pattern.length();
      const char* txt = text.ptr();
      const int64_t txt_len = text.length();
      return kr_search(pat, pat_len, pattern_print, txt, txt_len);
    }

    int64_t ObStringSearch::fast_search(const ObString & pattern, const ObString & text)
    {
      const char* pat = pattern.ptr();
      const int64_t pat_len = pattern.length();
      const char* txt = text.ptr();
      const int64_t txt_len = text.length();
      return fast_search(pat, pat_len, txt, txt_len);
    }

    uint64_t ObStringSearch::cal_print(const char* pattern, const int64_t pat_len)
    {
      uint64_t pat_print = 0;
      if (NULL == pattern || 0 >= pat_len)
      {
        TBSYS_LOG(WARN, "invalid argument pattern=%p, pattern_len=%ld",
                  pattern, pat_len);
      }
      else
      {
        for(int64_t i = 0; i<pat_len; ++i)
        {
          pat_print = (pat_print << 4) + pat_print + pattern[i];
        }
      }
      return pat_print;
    }

    int64_t ObStringSearch::kr_search(const char* pattern, const int64_t pat_len, const char* text, const int64_t text_len)
    {
      int64_t ret = -1;
      uint64_t shift = 1;
      uint64_t text_print = 0;
      uint64_t pat_print = 0;
      
      if (NULL == pattern || 0 >= pat_len
          || NULL == text || 0 >= text_len)
      {
        TBSYS_LOG(WARN, "invalid argument pattern=%p, pattern_len=%ld, text=%p, text_len=%ld",
                  pattern, pat_len, text, text_len);
      }
      else
      {
        for(int64_t i = 0; i<pat_len; ++i)
        {
          text_print = (text_print << 4) + text_print + text[i];
          pat_print = (pat_print << 4) + pat_print + pattern[i];
          shift = (shift << 4) + shift;
        }
 
        int64_t pos = 0;

        while(pos <= text_len - pat_len)
        {
          if(pat_print == text_print && 0 == memcmp(pattern, text+pos, pat_len))
          {
            ret = pos;
            break;
          }
          text_print = (text_print << 4) + text_print -  text[pos]*shift + text[pos+pat_len];
          ++pos;
        }
      }
      return ret;
    }

    int64_t ObStringSearch::kr_search(const char* pattern, const int64_t pat_len, const uint64_t pat_print,
                                      const char* text,    const int64_t text_len)
    {
      int64_t ret = -1;
      uint64_t shift = 1;
      uint64_t text_print = 0;
     
      if (NULL == pattern || 0 >= pat_len
          || NULL == text || 0 >= text_len)
      {
        TBSYS_LOG(WARN, "invalid argument pattern=%p, pattern_len=%ld, text=%p, text_len=%ld",
                  pattern, pat_len, text, text_len);
      }
      else
      {
        for(int64_t i = 0; i<pat_len; ++i)
        {
          text_print = (text_print << 4) + text_print + text[i];
          shift = (shift << 4) + shift;
        }
      
        int64_t pos = 0;
      
        while(pos <= text_len - pat_len)
        {
          if(pat_print == text_print && 0 == memcmp(pattern, text+pos, pat_len))
          {
            ret = pos;
            break;
          }
          text_print = (text_print << 4) + text_print -  text[pos]*shift + text[pos+pat_len];
          ++pos;
        }
      }
      return ret;
    }

    int64_t ObStringSearch::fast_search(const char* pattern, const int64_t pat_len,
                                        const char* text,    const int64_t text_len)
    {
      long mask;
      int64_t skip = 0;
      int64_t i, j, mlast, w;

      if (NULL == pattern || 0 >= pat_len
          || NULL == text || 0 >= text_len)
      {
        TBSYS_LOG(WARN, "invalid argument pattern=%p, pattern_len=%ld, text=%p, text_len=%ld",
                  pattern, pat_len, text, text_len);
      }
      else
      {
        w = text_len - pat_len;

        if (w < 0)
          return -1;

        /* look for special cases */
        if (pat_len <= 1) {
          if (pat_len <= 0)
            return -1;
          /* use special case for 1-character strings */
          for (i = 0; i < text_len; i++)
            if (text[i] == pattern[0])
              return i;
          return -1;
        }

        mlast = pat_len - 1;

        /* create compressed boyer-moore delta 1 table */
        skip = mlast - 1;
        /* process pattern[:-1] */
        for (mask = i = 0; i < mlast; i++) {
          mask |= (1 << (pattern[i] & 0x1F));
          if (pattern[i] == pattern[mlast])
            skip = mlast - i - 1;
        }
        /* process pattern[-1] outside the loop */
        mask |= (1 << (pattern[mlast] & 0x1F));

        for (i = 0; i <= w; i++) {
          /* note: using mlast in the skip path slows things down on x86 */
          if (text[i+pat_len-1] == pattern[pat_len-1]) {
            /* candidate match */
            for (j = 0; j < mlast; j++)
              if (text[i+j] != pattern[j])
                break;
            if (j == mlast) {
              /* got a match! */
              return i;
            }
            /* miss: check if next character is part of pattern */
            if (!(mask & (1 << (text[i+pat_len] & 0x1F))))
              i = i + pat_len;
            else
              i = i + skip;
          } else {
            /* skip: check if next character is part of pattern */
            if (!(mask & (1 << (text[i+pat_len] & 0x1F))))
              i = i + pat_len;
          }
        }
      }
      return -1;
    }
  }//namespace common
}//namespace oceanbase
