/*
 *  (C) 2007-2010 Taobao Inc.
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License version 2 as
 *  published by the Free Software Foundation.
 *
 *         ????.cpp is for what ...
 *
 *  Version: $Id: ipvsadm.c,v 1.27 2005/12/10 16:00:07 wensong Exp $
 *
 *  Authors:
 *     qushan <qushan@taobao.com>
 *        - some work details if you want
 */
#ifndef OCEANBASE_COMMON_UTILITY_H_
#define OCEANBASE_COMMON_UTILITY_H_

#include <stdint.h>
#include "tbsys.h"
#include "ob_object.h"
#define STR_BOOL(b) ((b) ? "true" : "false")

#define htonll(i) \
  ( \
  (((uint64_t)i & 0x00000000000000ff) << 56) | \
  (((uint64_t)i & 0x000000000000ff00) << 40) | \
  (((uint64_t)i & 0x0000000000ff0000) << 24) | \
  (((uint64_t)i & 0x00000000ff000000) << 8) | \
  (((uint64_t)i & 0x000000ff00000000) >> 8) | \
  (((uint64_t)i & 0x0000ff0000000000) >> 24) | \
  (((uint64_t)i & 0x00ff000000000000) >> 40) | \
  (((uint64_t)i & 0xff00000000000000) >> 56)   \
  )

#define DEFAULT_TIME_FORMAT "%Y-%m-%d %H:%M:%S"

namespace oceanbase
{
  namespace common
  {
    class ObScanner;
    class ObCellInfo;
    class ObVersionRange;
    class ObRange;

    void hex_dump(const void* data, const int32_t size,
        const bool char_type = true, const int32_t log_level = TBSYS_LOG_LEVEL_DEBUG);
    int32_t parse_string_to_int_array(const char* line,
        const char del, int32_t *array, int32_t& size);
    int32_t hex_to_str(const void* in_data, const int32_t data_length, void* buff, const int32_t buff_size);
    int32_t str_to_hex(const void* in_data, const int32_t data_length, void* buff, const int32_t buff_size);
    int64_t lower_align(int64_t input, int64_t align);
    int64_t upper_align(int64_t input, int64_t align);
    bool is2n(int64_t input);
    bool all_zero(const char *buffer, const int64_t size);
    bool all_zero_small(const char *buffer, const int64_t size);
    char* str_trim(char *str);
    char* ltrim(char *str);
    char* rtrim(char *str);
    void databuff_printf(char *buf, const int64_t buf_len, int64_t& pos, const char* fmt, ...) __attribute__ ((format (printf, 4, 5)));
    const char *inet_ntoa_r(const uint64_t ipport);
    const char* strtype(ObObjType type);
    void print_rowkey(FILE *fd, ObString &rowkey);
    void print_root_table(FILE* fd, ObScanner &scanner);
    const char *time2str(const int64_t time_s, const char *format = DEFAULT_TIME_FORMAT);

    int mem_chunk_serialize(char* buf, int64_t len, int64_t& pos, const char* data, int64_t data_len);
    int mem_chunk_deserialize(const char* buf, int64_t len, int64_t& pos, char* data, int64_t data_len, int64_t& real_len);
    int64_t min(const int64_t x, const int64_t y);
    int64_t max(const int64_t x, const int64_t y);
    extern const char *print_obj(const common::ObObj &obj);
    extern const char *print_string(const common::ObString &str);
    extern const char *print_cellinfo(const common::ObCellInfo *ci, const char *ext_info = NULL);
    extern const char *range2str(const common::ObVersionRange &range);
    extern const char *scan_range2str(const common::ObRange &scan_range);
    extern bool str_isprint(const char *str, const int64_t length);
    extern int replace_str(char* src_str, const int64_t src_str_buf_size, 
      const char* match_str, const char* replace_str);
    template <typename Allocator>
      int deep_copy_ob_string(Allocator& allocator, const ObString& src, ObString& dst)
      {
        int ret = OB_SUCCESS;
        char *ptr = NULL;
        if (NULL == src.ptr() || 0 >= src.length())
        {
          dst.assign_ptr(NULL, 0);
        }
        else if (NULL == (ptr = reinterpret_cast<char*>(allocator.alloc(src.length()))))
        {
          ret = OB_ALLOCATE_MEMORY_FAILED;
        }
        else
        {
          memcpy(ptr, src.ptr(), src.length());
          dst.assign(ptr, src.length());
        }
        return ret;
      }

    template <typename Allocator>
      int deep_copy_ob_obj(Allocator& allocator, const ObObj& src, ObObj& dst)
      {
        int ret = OB_SUCCESS;
        dst = src;
        if (ObVarcharType == src.get_type())
        {
          ObString value;
          ObString new_value;
          if (OB_SUCCESS != ( ret = src.get_varchar(value)))
          {
            TBSYS_LOG(WARN, "get_varchar failed, obj=");
            src.dump();
          }
          else if (OB_SUCCESS != (ret = deep_copy_ob_string(allocator, value, new_value)))
          {
            TBSYS_LOG(WARN, "deep_copy_ob_string failed, obj=");
            src.dump();
          }
          else
          {
            dst.set_varchar(new_value);
          }
        }
        return ret;
      }

    struct SeqLockGuard
    {
      SeqLockGuard(volatile int64_t& seq): seq_(seq)
      {
        int64_t tmp_seq = 0;
        do {
          tmp_seq = seq_;
        } while((tmp_seq & 1) || !__sync_bool_compare_and_swap(&seq_, tmp_seq, tmp_seq + 1));
      }
      ~SeqLockGuard()
      {
        seq_++;
      }
      volatile int64_t& seq_;
    };
  } // end namespace common
} // end namespace oceanbase


#endif //OCEANBASE_COMMON_UTILITY_H_

