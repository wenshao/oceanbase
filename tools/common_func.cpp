/*
 *  (C) 2007-2010 Taobao Inc.
 *  
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License version 2 as
 *  published by the Free Software Foundation.
 *
 *         test.cpp is for what ...
 *
 *  Version: $Id: test.cpp 2010年11月17日 16时12分46秒 qushan Exp $
 *
 *  Authors:
 *     qushan < qushan@taobao.com >
 *        - some work details if you want
 */



#include <assert.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdlib.h>
#include "common_func.h"
#include "common/ob_define.h"
#include "common/ob_server.h"
#include "common/serialization.h"
#include "common/ob_malloc.h"
#include "common/utility.h"
#include "test_utils.h"

using namespace oceanbase::common;
using namespace oceanbase::common::serialization;

/* 从min和max中返回一个随机值 */

int64_t random_number(int64_t min, int64_t max)
{
  static int dev_random_fd = -1;
  char *next_random_byte;
  int bytes_to_read;
  int64_t random_value = 0;

  assert(max > min);

  if (dev_random_fd == -1)
  {
    dev_random_fd = open("/dev/urandom", O_RDONLY);
    assert(dev_random_fd != -1);
  }

  next_random_byte = (char *)&random_value;
  bytes_to_read = sizeof(random_value);

  /* 因为是从/dev/random中读取，read可能会被阻塞，一次读取可能只能得到一个字节，
   * 循环是为了让我们读取足够的字节数来填充random_value.
   */
  do
  {
    int bytes_read;
    bytes_read = static_cast<int32_t>(read(dev_random_fd, next_random_byte, bytes_to_read));
    bytes_to_read -= bytes_read;
    next_random_byte += bytes_read;
  } while(bytes_to_read > 0);

  return min + (abs(static_cast<int32_t>(random_value)) % (max - min + 1));
}


int parse_number_range(const char *number_string, 
    int32_t *number_array, int32_t &number_array_size)
{
  int ret = OB_ERROR;
  if (NULL != strstr(number_string, ","))
  {
    ret = parse_string_to_int_array(number_string, ',', number_array, number_array_size);
    if (ret) return ret;
  }
  else if (NULL != strstr(number_string, "~"))
  {
    int32_t min_max_array[2];
    min_max_array[0] = 0;
    min_max_array[1] = 0;
    int tmp_size = 2;
    ret = parse_string_to_int_array(number_string, '~', min_max_array, tmp_size);
    if (ret) return ret;
    int32_t min = min_max_array[0];
    int32_t max = min_max_array[1];
    int32_t index = 0;
    for (int i = min; i <= max; ++i)
    {
      if (index > number_array_size) return OB_SIZE_OVERFLOW;
      number_array[index++] = i;
    }
    number_array_size = index;
  }
  else
  {
    number_array[0] = static_cast<int32_t>(strtol(number_string, NULL, 10));
    number_array_size = 1;
  }
  return OB_SUCCESS;
}

int number_to_hex_buf(const char* sp, const int len, char* hex, const int64_t hexlen, int64_t &pos)
{
  const int max_len = 32;
  assert(len < max_len-1);
  char number[max_len];
  memset(number, 0, max_len);
  strncpy(number, sp, len);
  int64_t value = strtoll(number, NULL, 10);
  return encode_i64(hex, hexlen, pos, value);
}

/**
 * hex_format:
 * 0 : plain string
 * 1 : hex format string "FACB012D"
 * 2 : int64_t number "1232"
 */
int parse_range_str(const char* range_str, int hex_format, ObRange &range)
{
  int ret = OB_SUCCESS;
  if(NULL == range_str)
  {
    ret = OB_INVALID_ARGUMENT;
  }

  int len = 0;
  if(OB_SUCCESS == ret)
  {
    len = static_cast<int32_t>(strlen(range_str));
    if (len < 5)
      ret = OB_ERROR;
  }
  if(OB_SUCCESS == ret)
  {
    const char start_border = range_str[0];
    if (start_border == '[')
      range.border_flag_.set_inclusive_start();
    else if (start_border == '(')
      range.border_flag_.unset_inclusive_start();
    else
    {
      fprintf(stderr, "start char of range_str(%c) must be [ or (\n", start_border);
      ret = OB_ERROR;
    }
  }
  if(OB_SUCCESS == ret)
  {
    const char end_border = range_str[len - 1];
    if (end_border == ']')
      range.border_flag_.set_inclusive_end();
    else if (end_border == ')')
      range.border_flag_.unset_inclusive_end();
    else
    {
      fprintf(stderr, "end char of range_str(%c) must be [ or (\n", end_border);
      ret = OB_ERROR;
    }
  }
  const char * sp = NULL;
  char * del = NULL;
  char * hexkey = NULL;

  if(OB_SUCCESS == ret)
  {
    sp = range_str + 1;
    del = (char*)strstr(sp, ",");
    if (NULL == del)
    {
      fprintf(stderr, "cannot find , in range_str(%s)\n", range_str);
      ret = OB_ERROR;
    }
  }
  int start_len = 0;
  if(OB_SUCCESS == ret)
  {
    while (*sp == ' ') ++sp; // skip space
    start_len = static_cast<int32_t>(del - sp);
    if (start_len <= 0)
    {
      fprintf(stderr, "start key cannot be NULL\n");
      ret = OB_ERROR;
    }
  }
  if( OB_SUCCESS == ret)
  {
    if (strncasecmp(sp, "min", 3) == 0 )
    {
      range.border_flag_.set_min_value();
    }
    else
    {
      hexkey = (char*)ob_malloc(OB_RANGE_STR_BUFSIZ);
      if (hex_format == 0)
      {
        range.start_key_.assign_ptr((char*)sp, start_len);
      }
      else if (hex_format == 1)
      {
        int hexlen = str_to_hex(sp, start_len, hexkey, OB_RANGE_STR_BUFSIZ);
        range.start_key_.assign_ptr(hexkey, hexlen/2);
      }
      else if (hex_format == 2)
      {
        int64_t pos = 0;
        number_to_hex_buf(sp, start_len, hexkey, OB_RANGE_STR_BUFSIZ, pos);
        range.start_key_.assign_ptr(hexkey, static_cast<int32_t>(pos));
      }
    }
  }
  int end_len = 0;
  const char * ep = NULL;
  if(OB_SUCCESS == ret)
  {
    ep = del + 1;
    while (*ep == ' ') ++ep;
    end_len = static_cast<int32_t>(range_str + len - 1 - ep);
    if (end_len <= 0)
    {
      fprintf(stderr, "end key cannot be NULL\n");
      ret = OB_ERROR;
    }
  }

  if(OB_SUCCESS == ret)
  {
    if (strncasecmp(ep, "max", 3) == 0)
    {
      range.border_flag_.set_max_value();
    }
    else
    {
      hexkey = (char*)ob_malloc(OB_RANGE_STR_BUFSIZ);
      if (hex_format == 0)
      {
        range.end_key_.assign_ptr((char*)ep, end_len);
      }
      else if (hex_format == 1)
      {
        int hexlen = str_to_hex(ep, end_len, hexkey, OB_RANGE_STR_BUFSIZ);
        range.end_key_.assign_ptr(hexkey, hexlen/2);
      }
      else if (hex_format == 2)
      {
        int64_t pos = 0;
        number_to_hex_buf(ep, end_len, hexkey, OB_RANGE_STR_BUFSIZ, pos);
        range.end_key_.assign_ptr(hexkey, static_cast<int32_t>(pos));
      }
    }
  }

  return ret;
}

void dump_scanner(ObScanner &scanner)
{
  ObScannerIterator iter;
  int total_count = 0;
  int row_count = 0;
  bool is_row_changed = false;
  for (iter = scanner.begin(); iter != scanner.end(); iter++)
  {
    ObCellInfo *cell_info;
    iter.get_cell(&cell_info, &is_row_changed);
    if (is_row_changed) 
    {
      ++row_count;
      fprintf(stderr,"table_id:%lu, rowkey:\n", cell_info->table_id_);
      hex_dump(cell_info->row_key_.ptr(), cell_info->row_key_.length());
    }
    fprintf(stderr, "%s\n", common::print_cellinfo(cell_info, "CLI_GET"));
    ++total_count;
  }
  fprintf(stderr, "row_count=%d,total_count=%d\n", row_count, total_count);
}

int dump_tablet_info(ObScanner &scanner)
{
  int ret = OB_SUCCESS;
  int64_t ip = 0;
  int64_t port = 0;
  int64_t version = 0;
  int64_t occupy_size = 0;
  int64_t total_row_count = 0;

  ObServer server;
  char tmp_buf[32];
  char start_key_buf[128];
  char end_key_buf[128];
  ObString start_key;
  ObString end_key; 
  ObCellInfo * cell = NULL;
  ObScannerIterator iter; 
  bool row_change = false;
  int row_count = 0;

  iter = scanner.begin();
  for (; OB_SUCCESS == ret && iter != scanner.end() ; ++iter)
  {
    ret = iter.get_cell(&cell, &row_change);

    //fprintf(stderr, "row_change=%d, column_name=%.*s\n", row_change, 
     //   cell->column_name_.length(), cell->column_name_.ptr());
    //cell->value_.dump();

    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "get cell from scanner iterator failed:ret[%d]", ret);
      break;
    }

    if (row_change && OB_SUCCESS == ret)
    {          
      start_key = end_key;
      end_key.assign(cell->row_key_.ptr(), cell->row_key_.length());
      row_count++;
      ip = port = version = 0;
      hex_to_str(start_key.ptr(), start_key.length(), start_key_buf, 128);
      hex_to_str(cell->row_key_.ptr(), cell->row_key_.length(), end_key_buf, 128);
      fprintf(stderr, "\nrange:(%s,%s]\n", start_key.length() == 0 ? "min": start_key_buf, end_key_buf);
    }

    if (OB_SUCCESS == ret && cell != NULL)
    {
      if ((cell->column_name_.compare("1_port") == 0) 
          || (cell->column_name_.compare("2_port") == 0) 
          || (cell->column_name_.compare("3_port") == 0))
      {
        ret = cell->value_.get_int(port);
        //TBSYS_LOG(DEBUG,"port is %ld",port);
      }
      else if ((cell->column_name_.compare("1_ipv4") == 0)
          || (cell->column_name_.compare("2_ipv4") == 0)
          || (cell->column_name_.compare("3_ipv4") == 0))
      {
        ret = cell->value_.get_int(ip);
        //TBSYS_LOG(DEBUG,"ip is %ld",ip);
      }
      else if (cell->column_name_.compare("1_tablet_version") == 0 ||
          cell->column_name_.compare("2_tablet_version") == 0 ||
          cell->column_name_.compare("3_tablet_version") == 0)
      {
        ret = cell->value_.get_int(version);
        //hex_dump(cell->row_key_.ptr(),cell->row_key_.length(),false,TBSYS_LOG_LEVEL_INFO);
        //TBSYS_LOG(DEBUG,"tablet_version is %d",version);
      }
      else if (cell->column_name_.compare("occupy_size") == 0 )
      {
        ret = cell->value_.get_int(occupy_size);
        fprintf(stderr, "occupy_size=%ld\n", occupy_size);
      }
      else if (cell->column_name_.compare("record_count") == 0 )
      {
        ret = cell->value_.get_int(total_row_count);
        fprintf(stderr, "record_count=%ld\n", total_row_count);
      }

      //fprintf(stderr, "%.*s\n", cell->column_name_.length(), cell->column_name_.ptr());

      if (ret != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR, "check get value failed:ret[%d]", ret);
        break;
      }
    }

    if (port != 0 && ip != 0 && version != 0)
    {
      server.set_ipv4_addr(static_cast<int32_t>(ip), static_cast<int32_t>(port));
      server.to_string(tmp_buf,sizeof(tmp_buf));
      if (row_count > 0) fprintf(stderr, "server= %s ,version= %ld\n", tmp_buf, version);
      ip = port = version = 0;
    }
  }
  return ret;
}
