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

#include "utility.h"
#include "ob_define.h"
#include "ob_scanner.h"
namespace oceanbase
{
  namespace common
  {

    void hex_dump(const void* data, const int32_t size,
        const bool char_type /*= true*/, const int32_t log_level /*= TBSYS_LOG_LEVEL_DEBUG*/)
    {
      if (TBSYS_LOGGER._level < log_level) return;
      /* dumps size bytes of *data to stdout. Looks like:
       * [0000] 75 6E 6B 6E 6F 77 6E 20
       * 30 FF 00 00 00 00 39 00 unknown 0.....9.
       * (in a single line of course)
       */

      unsigned const char *p = (unsigned char*)data;
      unsigned char c = 0;
      int n = 0;
      char bytestr[4] = {0};
      char addrstr[10] = {0};
      char hexstr[ 16*3 + 5] = {0};
      char charstr[16*1 + 5] = {0};

      for(n = 1; n <= size; n++)
      {
        if (n%16 == 1)
        {
          /* store address for this line */
          snprintf(addrstr, sizeof(addrstr), "%.4x",
              (int)((unsigned long)p-(unsigned long)data) );
        }

        c = *p;
        if (isprint(c) == 0)
        {
          c = '.';
        }

        /* store hex str (for left side) */
        snprintf(bytestr, sizeof(bytestr), "%02X ", *p);
        strncat(hexstr, bytestr, sizeof(hexstr)-strlen(hexstr)-1);

        /* store char str (for right side) */
        snprintf(bytestr, sizeof(bytestr), "%c", c);
        strncat(charstr, bytestr, sizeof(charstr)-strlen(charstr)-1);

        if (n % 16 == 0)
        {
          /* line completed */
          if (char_type)
            TBSYS_LOGGER.logMessage(TBSYS_LOG_NUM_LEVEL(log_level), "[%ld] [%4.4s]   %-50.50s  %s\n",
                pthread_self(), addrstr, hexstr, charstr);
          else
            TBSYS_LOGGER.logMessage(TBSYS_LOG_NUM_LEVEL(log_level), "[%ld] [%4.4s]   %-50.50s\n",
                pthread_self(), addrstr, hexstr);
          hexstr[0] = 0;
          charstr[0] = 0;
        }
        else if(n % 8 == 0)
        {
          /* half line: add whitespaces */
          strncat(hexstr, "  ", sizeof(hexstr)-strlen(hexstr)-1);
          strncat(charstr, " ", sizeof(charstr)-strlen(charstr)-1);
        }
        p++; /* next byte */
      }

      if (strlen(hexstr) > 0)
      {
        /* print rest of buffer if not empty */
        if (char_type)
          TBSYS_LOGGER.logMessage(TBSYS_LOG_NUM_LEVEL(log_level), "[%ld] [%4.4s]   %-50.50s  %s\n",
              pthread_self(), addrstr, hexstr, charstr);
        else
          TBSYS_LOGGER.logMessage(TBSYS_LOG_NUM_LEVEL(log_level), "[%ld] [%4.4s]   %-50.50s\n",
              pthread_self(), addrstr, hexstr);
      }
    }

    int32_t parse_string_to_int_array(const char* line,
        const char del, int32_t *array, int32_t& size)
    {
      int ret = 0;
      const char* start = line;
      const char* p = NULL;
      char buffer[OB_MAX_ROW_KEY_LENGTH];

      if (NULL == line || NULL == array || size <= 0)
      {
        ret = OB_INVALID_ARGUMENT;
      }

      int32_t idx = 0;
      if (OB_SUCCESS == ret)
      {
        while (NULL != start)
        {
          p = strchr(start, del);
          if (NULL != p)
          {
            memset(buffer, 0, OB_MAX_ROW_KEY_LENGTH);
            strncpy(buffer, start, p - start);
            if (strlen(buffer) > 0)
            {
              if (idx >= size)
              {
                ret = OB_SIZE_OVERFLOW;
                break;
              }
              else
              {
                array[idx++] = static_cast<int32_t>(strtol(buffer, NULL, 10));
              }
            }
            start = p + 1;
          }
          else
          {
            memset(buffer, 0, OB_MAX_ROW_KEY_LENGTH);
            strcpy(buffer, start);
            if (strlen(buffer) > 0)
            {
              if (idx >= size)
              {
                ret = OB_SIZE_OVERFLOW;
                break;
              }
              else
              {
                array[idx++] = static_cast<int32_t>(strtol(buffer, NULL, 10));
              }
            }
            break;
          }
        }

        if (OB_SUCCESS == ret) size = idx;
      }
      return ret;
    }
    int32_t hex_to_str(const void* in_data, const int32_t data_length, void* buff, const int32_t buff_size)
    {
      unsigned const char *p = NULL;
      int32_t i = 0;
      if (in_data != NULL && buff != NULL && buff_size >= data_length * 2)
      {
        p = (unsigned const char *)in_data;
        for (; i < data_length; i++)
        {
          sprintf((char*)buff + i * 2, "%02X", *(p + i));
        }
      }
      return i;
    }
    int32_t str_to_hex(const void* in_data, const int32_t data_length, void* buff, const int32_t buff_size)
    {
      unsigned const char *p = NULL;
      unsigned char *o = NULL;
      unsigned char c;
      int32_t i = 0;
      if (in_data != NULL && buff != NULL && buff_size >= data_length / 2)
      {
        p = (unsigned const char *)in_data;
        o = (unsigned char *)buff;
        c = 0;
        for (i = 0; i < data_length; i++)
        {
          c = static_cast<unsigned char>(c << 4);
          if (*(p + i) > 'F' ||
              (*(p + i) < 'A' && *(p + i) > '9') ||
              *(p + i) < '0')
            break;
          if (*(p + i) >= 'A')
            c = static_cast<unsigned char>(c + (*(p + i) - 'A' + 10));
          else
            c = static_cast<unsigned char>(c + (*(p + i) - '0'));
          if (i % 2 == 1)
          {
            *(o + i / 2) = c;
            c = 0;
          }
        }
      }
      return i;
    }

    int64_t lower_align(int64_t input, int64_t align)
    {
      int64_t ret = input;
      ret = (input + align - 1) & ~(align - 1);
      ret = ret - ((ret - input + align - 1) & ~(align - 1));
      return ret;
    };

    int64_t upper_align(int64_t input, int64_t align)
    {
      int64_t ret = input;
      ret = (input + align - 1) & ~(align - 1);
      return ret;
    };

    bool is2n(int64_t input)
    {
      return (((~input + 1) & input) == input);
    };

    bool all_zero(const char *buffer, const int64_t size)
    {
      bool bret = true;
      const char *buffer_end = buffer + size;
      const char *start = (char*)upper_align((int64_t)buffer, sizeof(int64_t));
      start = std::min(start, buffer_end);
      const char *end = (char*)lower_align((int64_t)(buffer + size), sizeof(int64_t));
      end = std::max(end, buffer);

      bret = all_zero_small(buffer, start - buffer);
      if (bret)
      {
        bret = all_zero_small(end, buffer + size - end);
      }
      if (bret)
      {
        const char *iter = start;
        while (iter < end)
        {
          if (0 != *((int64_t*)iter))
          {
            bret = false;
            break;
          }
          iter += sizeof(int64_t);
        }
      }
      return bret;
    };

    bool all_zero_small(const char *buffer, const int64_t size)
    {
      bool bret = true;
      if (NULL != buffer)
      {
        for (int64_t i = 0; i < size; i++)
        {
          if (0 != *(buffer + i))
          {
            bret = false;
            break;
          }
        }
      }
      return bret;
    }

    char* str_trim(char *str)
    {
      char *p = str, *sa = str;
      while (*p) {
        if(*p!=' ')
          *str++ = *p;
        p++;
      }
      *str = 0;
      return sa;
    }

    char* ltrim(char* str)
    {
      char *p = str;
      while( p != NULL && *p != '\0' && isspace(*p))
        ++p;
      return p;
    }

    char* rtrim(char *str)
    {
      if ((str != NULL) && *str != '\0')
      {
        char *p = str + strlen(str) - 1;
        while((p > str) && isspace(*p))
          --p;
        *(p + 1) = '\0';
      }
      return str;
    }
    const char *inet_ntoa_r(const uint64_t ipport)
    {
      static const int64_t BUFFER_SIZE = 32;
      static __thread char buffers[2][BUFFER_SIZE];
      static __thread int64_t i = 0;
      char *buffer = buffers[i++ % 2];
      buffer[0] = '\0';

      uint32_t ip = (uint32_t)(ipport & 0xffffffff);
      int port = (int)((ipport >> 32 ) & 0xffff);
      unsigned char *bytes = (unsigned char *) &ip;
      if (port > 0)
      {
        snprintf(buffer, BUFFER_SIZE, "%d.%d.%d.%d:%d", bytes[0], bytes[1], bytes[2], bytes[3], port);
      }
      else
      {
        snprintf(buffer, BUFFER_SIZE, "%d.%d.%d.%d:-1", bytes[0], bytes[1], bytes[2], bytes[3]);
      }

      return buffer;
    }

    void databuff_printf(char *buf, const int64_t buf_len, int64_t& pos, const char* fmt, ...)
    {
      if (NULL != buf && 0 <= pos && pos <= buf_len)
      {
        va_list args;
        va_start(args, fmt);
        int len = vsnprintf(buf+pos, buf_len-pos, fmt, args);
        if (len < buf_len-pos)
        {
          pos += len;
        }
        else
        {
          pos = buf_len;
        }
      }
    }

    const char* strtype(ObObjType type)
    {
      const char* ret = "unknown";
      switch (type)
      {
        case ObNullType:
          ret = "null";
          break;
        case ObIntType:
          ret = "int";
          break;
        case ObVarcharType:
          ret = "varchar";
          break;
        case ObFloatType:
          ret = "float";
          break;
        case ObDoubleType:
          ret = "double";
          break;
        case ObDateTimeType:
          ret = "odatetime";
          break;
        case ObPreciseDateTimeType:
          ret = "datetime";
          break;
        case ObModifyTimeType:
          ret = "modifytime";
          break;
        case ObCreateTimeType:
          ret = "createtime";
          break;
        case ObSeqType:
          ret = "seq";
          break;
        case ObExtendType:
          ret = "ext";
          break;
        default:
          break;
      }
      return ret;
    }

    const char *time2str(const int64_t time_us, const char *format)
    {
      static const int32_t BUFFER_SIZE = 1024;
      static __thread char buffer[2][BUFFER_SIZE];
      static __thread int64_t i = 0;
      buffer[i % 2][0] = '\0';
      struct tm time_struct;
      int64_t time_s = time_us / 1000000;
      int64_t cur_second_time_us = time_us % 1000000;
      if(NULL != localtime_r(&time_s, &time_struct))
      {
        int64_t pos = strftime(buffer[i % 2], BUFFER_SIZE, format, &time_struct);
        if (pos < BUFFER_SIZE)
        {
          snprintf(&buffer[i % 2][pos], BUFFER_SIZE - pos, " %ld %ld", cur_second_time_us, time_us);
        }
      }
      return buffer[i++ % 2];
    }

    void print_rowkey(FILE *fd, ObString &rowkey)
    {
      char* pchar = rowkey.ptr();
      for (int i = 0; i < rowkey.length(); ++i, pchar++)
      {
        if (isprint(*pchar))
        {
          fprintf(fd, "%c", *pchar);
        }
        else
        {
          fprintf(fd, "\\%hhu", *pchar);
        }
      }
    }
    void print_root_table(FILE* fd, ObScanner &scanner)
    {
      ObCellInfo* cell = NULL;
      bool is_row_changed = false;
      while(OB_SUCCESS == scanner.next_cell())
      {
        if (OB_SUCCESS == scanner.get_cell(&cell, &is_row_changed))
        {
          if (is_row_changed)
          {
            fprintf(fd, "\n");
            print_rowkey(fd, cell->row_key_);
            fprintf(fd, "|");
          }
          fprintf(fd, "%.*s ", cell->column_name_.length(),  cell->column_name_.ptr());
          cell->value_.print_value(fd);
          fprintf(fd, "|");
        }
      }
      fprintf(fd, "\n");
    }

    int mem_chunk_serialize(char* buf, int64_t len, int64_t& pos, const char* data, int64_t data_len)
    {
      int err = OB_SUCCESS;
      int64_t tmp_pos = pos;
      if (NULL == buf || len <= 0 || pos < 0 || pos > len || NULL == data || 0 > data_len)
      {
        err = OB_INVALID_ARGUMENT;
      }
      else if(OB_SUCCESS != (err = serialization::encode_i64(buf, len, tmp_pos, data_len)))
      {
        TBSYS_LOG(ERROR, "encode_i64(buf=%p, len=%ld, pos=%ld, i=%ld)=>%d", buf, len, tmp_pos, data_len, err);
      }
      else if (tmp_pos + data_len > len)
      {
        err = OB_SERIALIZE_ERROR;
      }
      else
      {
        memcpy(buf + tmp_pos, data, data_len);
        tmp_pos += data_len;
        pos = tmp_pos;
      }
      return err;
    }

    int mem_chunk_deserialize(const char* buf, int64_t len, int64_t& pos, char* data, int64_t data_len, int64_t& real_len)
    {
      int err = OB_SUCCESS;
      int64_t tmp_pos = pos;
      if (NULL == buf || len <= 0 || pos < 0 || pos > len || NULL == data || data_len < 0)
      {
        err = OB_INVALID_ARGUMENT;
      }
      else if(OB_SUCCESS != (err = serialization::decode_i64(buf, len, tmp_pos, &real_len)))
      {
        TBSYS_LOG(ERROR, "decode_i64(buf=%p, len=%ld, pos=%ld, i=%ld)=>%d", buf, len, tmp_pos, real_len, err);
      }
      else if (real_len > data_len || tmp_pos + real_len > len)
      {
        err = OB_DESERIALIZE_ERROR;
      }
      else
      {
        memcpy(data, buf + tmp_pos, real_len);
        tmp_pos += real_len;
        pos = tmp_pos;
      }
      return err;
    }

    int64_t min(const int64_t x, const int64_t y)
    {
      return x > y? y: x;
    }

    int64_t max(const int64_t x, const int64_t y)
    {
      return x < y? y: x;
    }

    const char *print_string(const ObString &str)
    {
      static const int64_t BUFFER_SIZE = 16 * 1024;
      static __thread char buffer[BUFFER_SIZE] = "0x ";
      common::hex_to_str(str.ptr(), str.length(), &buffer[3], BUFFER_SIZE - 3);
      return buffer;
    }

    const char *print_cellinfo(const ObCellInfo *ci, const char *ext_info/* = NULL*/)
    {
      static const int64_t BUFFER_SIZE = 128 * 1024;
      static __thread char buffers[2][BUFFER_SIZE];
      static __thread int64_t i = 0;
      char *buffer = buffers[i++ % 2];
      buffer[0] = '\0';
      if (NULL != ci)
      {
        int32_t rowkey_len = 0;
        const char *rowkey_str = NULL;
        if (NULL != ci->row_key_.ptr()
            && 0 != ci->row_key_.length()
            && !str_isprint(ci->row_key_.ptr(), ci->row_key_.length()))
        {
          char hex_buffer[BUFFER_SIZE] = "0x ";
          common::hex_to_str(ci->row_key_.ptr(), ci->row_key_.length(), hex_buffer + strlen(hex_buffer), static_cast<int32_t>(BUFFER_SIZE - strlen(hex_buffer)));
          rowkey_str = hex_buffer;
          rowkey_len = static_cast<int32_t>(strlen(hex_buffer));
        }
        else
        {
          rowkey_str = ci->row_key_.ptr();
          rowkey_len = ci->row_key_.length();
        }
        if (NULL == ext_info)
        {
          snprintf(buffer, BUFFER_SIZE, "table_id=%lu table_name=[%.*s] table_name_ptr=%p table_name_length=%d "
                  "row_key=[%.*s] row_key_ptr=%p row_key_length=%d "
                  "column_id=%lu column_name=[%.*s] column_name_ptr=%p column_name_length=%d "
                  "%s",
                  ci->table_id_, ci->table_name_.length(), ci->table_name_.ptr(), ci->table_name_.ptr(), ci->table_name_.length(),
                  rowkey_len, rowkey_str, ci->row_key_.ptr(), ci->row_key_.length(),
                  ci->column_id_, ci->column_name_.length(), ci->column_name_.ptr(), ci->column_name_.ptr(), ci->column_name_.length(),
                  print_obj(ci->value_));
        }
        else
        {
          snprintf(buffer, BUFFER_SIZE, "%s table_id=%lu table_name=[%.*s] table_name_ptr=%p table_name_length=%d "
                  "row_key=[%.*s] row_key_ptr=%p row_key_length=%d "
                  "column_id=%lu column_name=[%.*s] column_name_ptr=%p column_name_length=%d "
                  "%s",
                  ext_info,
                  ci->table_id_, ci->table_name_.length(), ci->table_name_.ptr(), ci->table_name_.ptr(), ci->table_name_.length(),
                  rowkey_len, rowkey_str, ci->row_key_.ptr(), ci->row_key_.length(),
                  ci->column_id_, ci->column_name_.length(), ci->column_name_.ptr(), ci->column_name_.ptr(), ci->column_name_.length(),
                  print_obj(ci->value_));
        }
      }
      return buffer;
    }

    const char *range2str(const ObVersionRange &range)
    {
      static const int64_t BUFFER_SIZE = 1024;
      static __thread char buffers[2][BUFFER_SIZE];
      static __thread int64_t i = 0;
      char *buffer = buffers[i++ % 2];
      buffer[0] = '\0';
      const char *start_bound = range.border_flag_.inclusive_start() ? "[" : "(";
      const char *end_bound = range.border_flag_.inclusive_end() ? "]" : ")";
      if (range.border_flag_.is_min_value())
      {
        if (range.border_flag_.is_max_value())
        {
          snprintf(buffer, BUFFER_SIZE, "%sMIN,MAX%s", start_bound, end_bound);
        }
        else
        {
          snprintf(buffer, BUFFER_SIZE, "%sMIN,%d-%hd-%hd%s", start_bound, range.end_version_.major_,
                   range.end_version_.minor_,range.end_version_.is_final_minor_, end_bound);
        }
      }
      else
      {
        if (range.border_flag_.is_max_value())
        {
          snprintf(buffer, BUFFER_SIZE, "%s%d-%hd-%hd,MAX%s", start_bound,range.start_version_.major_,
                   range.start_version_.minor_,range.start_version_.is_final_minor_, end_bound);          
        }
        else
        {
          snprintf(buffer, BUFFER_SIZE, "%s%d-%hd-%hd,%d-%hd-%hd%s", start_bound, range.start_version_.major_,
                   range.start_version_.minor_,range.start_version_.is_final_minor_,
                   range.end_version_.major_,range.end_version_.minor_,range.start_version_.is_final_minor_,
                   end_bound);
        }
      }
      return buffer;
    }

    const char *scan_range2str(const common::ObRange &scan_range)
    {
      static const int64_t BUFFER_SIZE = 1024;
      static __thread char buffers[2][BUFFER_SIZE];
      static __thread int64_t i = 0;
      char *buffer = buffers[i++ % 2];
      buffer[0] = '\0';
      const char *start_bound = scan_range.border_flag_.inclusive_start() ? "[" : "(";
      const char *end_bound = scan_range.border_flag_.inclusive_end() ? "]" : ")";
      if (scan_range.border_flag_.is_min_value())
      {
        if (scan_range.border_flag_.is_max_value())
        {
          snprintf(buffer, BUFFER_SIZE, "table_id=%lu %sMIN,MAX%s", scan_range.table_id_, start_bound, end_bound);
        }
        else
        {
          char hex_buffer[BUFFER_SIZE] = {'\0'};
          hex_to_str(scan_range.end_key_.ptr(), scan_range.end_key_.length(), hex_buffer, BUFFER_SIZE);
          snprintf(buffer, BUFFER_SIZE, "table_id=%lu %sMIN,%s%s", scan_range.table_id_, start_bound, hex_buffer, end_bound);
        }
      }
      else
      {
        if (scan_range.border_flag_.is_max_value())
        {
          char hex_buffer[BUFFER_SIZE] = {'\0'};
          hex_to_str(scan_range.start_key_.ptr(), scan_range.start_key_.length(), hex_buffer, BUFFER_SIZE);
          snprintf(buffer, BUFFER_SIZE, "table_id=%lu %s%s,MAX%s", scan_range.table_id_, start_bound, hex_buffer, end_bound);
        }
        else
        {
          char hex_buffers[2][BUFFER_SIZE];
          hex_buffers[0][0] = '\0';
          hex_to_str(scan_range.start_key_.ptr(), scan_range.start_key_.length(), hex_buffers[0], BUFFER_SIZE);
          hex_buffers[1][0] = '\0';
          hex_to_str(scan_range.end_key_.ptr(), scan_range.end_key_.length(), hex_buffers[1], BUFFER_SIZE);
          snprintf(buffer, BUFFER_SIZE, "table_id=%lu %s%s,%s%s", scan_range.table_id_, start_bound, hex_buffers[0], hex_buffers[1], end_bound);
        }
      }
      return buffer;
    }

    const char *print_obj(const ObObj &obj)
    {
      static const int64_t BUFFER_SIZE = 128 * 1024;
      static __thread char buffers[2][BUFFER_SIZE];
      static __thread int64_t i = 0;
      char *buffer = buffers[i++ % 2];
      buffer[0] = '\0';
      switch (obj.get_type())
      {
        case ObNullType:
          snprintf(buffer, BUFFER_SIZE, "obj_type=null");
          break;
        case ObIntType:
          {
            bool is_add = false;
            int64_t tmp = 0;
            obj.get_int(tmp, is_add);
            snprintf(buffer, BUFFER_SIZE, "obj_type=int value=%ld is_add=%s", tmp, STR_BOOL(is_add));
          }
          break;
        case ObDateTimeType:
          {
            bool is_add = false;
            ObDateTime tmp = 0;
            obj.get_datetime(tmp, is_add);
            snprintf(buffer, BUFFER_SIZE, "obj_type=data_time value=%s is_add=%s", time2str(tmp), STR_BOOL(is_add));
          }
          break;
        case ObPreciseDateTimeType:
          {
            bool is_add = false;
            ObDateTime tmp = 0;
            obj.get_precise_datetime(tmp, is_add);
            snprintf(buffer, BUFFER_SIZE, "obj_type=precise_data_time value=%s is_add=%s", time2str(tmp), STR_BOOL(is_add));
          }
          break;
        case ObVarcharType:
          {
            ObString tmp;
            obj.get_varchar(tmp);
            if (NULL != tmp.ptr()
                && 0 != tmp.length()
                && !str_isprint(tmp.ptr(), tmp.length()))
            {
              char hex_buffer[BUFFER_SIZE] = {'\0'};
              common::hex_to_str(tmp.ptr(), tmp.length(), hex_buffer, BUFFER_SIZE);
              snprintf(buffer, BUFFER_SIZE, "obj_type=var_char value=[0x %s] value_ptr=%p value_length=%d",
                      hex_buffer, tmp.ptr(), tmp.length());
            }
            else
            {
              snprintf(buffer, BUFFER_SIZE, "obj_type=var_char value=[%.*s] value_ptr=%p value_length=%d",
                      tmp.length(), tmp.ptr(), tmp.ptr(), tmp.length());
            }
          }
          break;
        case ObSeqType:
          snprintf(buffer, BUFFER_SIZE, "obj_type=seq");
          break;
        case ObCreateTimeType:
          {
            ObCreateTime tmp = 0;
            obj.get_createtime(tmp);
            snprintf(buffer, BUFFER_SIZE, "obj_type=create_time value=%s", time2str(tmp));
          }
          break;
        case ObModifyTimeType:
          {
            ObModifyTime tmp = 0;
            obj.get_modifytime(tmp);
            snprintf(buffer, BUFFER_SIZE, "obj_type=modify_time value=%s", time2str(tmp));
          }
          break;
        case ObExtendType:
          {
            int64_t tmp = 0;
            obj.get_ext(tmp);
            snprintf(buffer, BUFFER_SIZE, "obj_type=extend value=%ld", tmp);
          }
          break;
        default:
          break;
      }
      return buffer;
    }

    bool str_isprint(const char *str, const int64_t length)
    {
      bool bret = false;
      if (NULL != str
          && 0 != length)
      {
        bret = true;
        for (int64_t i = 0; i < length; i++)
        {
          if (!isprint(str[i]))
          {
            bret = false;
            break;
          }
        }
      }
      return bret;
    }

    int replace_str(char* src_str, const int64_t src_str_buf_size, 
      const char* match_str, const char* replace_str)
    {
      int ret                 = OB_SUCCESS;
      int64_t str_len         = 0;
      int64_t match_str_len   = 0;
      int64_t replace_str_len = 0;
      const char* find_pos    = NULL;
      char new_str[OB_MAX_EXPIRE_CONDITION_LENGTH];

      if (NULL == src_str || src_str_buf_size <= 0 
          || NULL == match_str || NULL == replace_str)
      {
        TBSYS_LOG(WARN, "invalid param, src_str=%p, src_str_buf_size=%ld, "
                        "match_str=%p, replace_str=%p",
          src_str, src_str_buf_size, match_str, replace_str);
        ret = OB_ERROR;
      }
      else if (NULL != (find_pos = strstr(src_str, match_str)))
      {
        match_str_len = strlen(match_str);
        replace_str_len = strlen(replace_str);
        while(NULL != find_pos)
        {
          str_len = find_pos - src_str + replace_str_len 
            + strlen(find_pos + match_str_len);
          if (str_len >= OB_MAX_EXPIRE_CONDITION_LENGTH
              || str_len >= src_str_buf_size)
          {
            TBSYS_LOG(WARN, "str after replace is too large, new_size=%ld, "
                            "new_buf_size=%ld, src_str_buf_size=%ld",
              str_len, OB_MAX_EXPIRE_CONDITION_LENGTH, src_str_buf_size);
            ret = OB_ERROR;
            break;
          }
          else
          {
            memset(new_str, 0, OB_MAX_EXPIRE_CONDITION_LENGTH);
            strncpy(new_str, src_str, find_pos - src_str);
            strcat(new_str, replace_str);
            strcat(new_str, find_pos + match_str_len);
            strcpy(src_str, new_str);
          }

          find_pos = strstr(src_str, match_str);
        }
      }

      return ret;
    }
  } // end namespace common
} // end namespace oceanbase


