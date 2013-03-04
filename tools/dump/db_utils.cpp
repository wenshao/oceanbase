/*
 * =====================================================================================
 *
 *       Filename:  db_utils.cpp
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  06/17/2011 02:34:45 PM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  yushun.swh (), yushun.swh@taobao.com
 *        Company:  taobao
 *
 * =====================================================================================
 */
#include "db_utils.h"
#include "db_dumper_config.h"
#include "common/ob_action_flag.h"
#include "updateserver/ob_ups_utils.h"

using namespace oceanbase::common;

static char header_delima = '\002';
static char body_delima = '\001';
static const char kTabSym = '\t';

void escape_varchar(char *p, int size)
{
  for(int i = 0;i < size; i++) {
    if (p[i] == body_delima || p[i] == 0xd ||
        p[i] == 0xa || p[i] == kTabSym) {   
      p[i] = 0x20;                              /* replace [ctrl+A], [CR] by space */
    }
  }
}

int serialize_cell(ObCellInfo *cell, ObDataBuffer &buff)
{
  int64_t cap = buff.get_remain();
  int64_t len = -1;
  int type = cell->value_.get_type();
  char *data = buff.get_data();
  int64_t pos = buff.get_position();

  switch(type) {
   case ObNullType:
     //     TBSYS_LOG(INFO, "Null Type");
     len = 0;
     break;
   case ObIntType:
     int64_t val;
     if (cell->value_.get_int(val) != OB_SUCCESS) {
       TBSYS_LOG(ERROR, "get_int error");
       break;
     }
     len = snprintf(data + pos, cap, "%ld", val);
     break;
   case ObVarcharType:
     {
       ObString str;
       if (cell->value_.get_varchar(str) != OB_SUCCESS ||
           cap < str.length()) {
         TBSYS_LOG(ERROR, "get_varchar error");
         break;
       }

       memcpy(data + pos, str.ptr(), str.length());
       escape_varchar(data + pos, str.length());
       len = str.length();
     }
     break;
   case ObPreciseDateTimeType:
     {
       int64_t value;
       if (cell->value_.get_precise_datetime(value) != OB_SUCCESS) {
         TBSYS_LOG(ERROR, "get_precise_datetime error");
         break;
       }
       len = ObDateTime2MySQLDate(value, type, data + pos, static_cast<int32_t>(cap));
     }
     break;
   case ObDateTimeType:
     {
       int64_t value;
       if (cell->value_.get_datetime(value) != OB_SUCCESS) {
         TBSYS_LOG(ERROR, "get_datetime error ");
         break;
       }
       len = ObDateTime2MySQLDate(value, type, data + pos, static_cast<int32_t>(cap));
     }
     break;
   case ObModifyTimeType:
     {
       int64_t value;
       if (cell->value_.get_modifytime(value) != OB_SUCCESS) {
         TBSYS_LOG(ERROR, "get_modifytime error ");
         break;
       }
       len = ObDateTime2MySQLDate(value, type, data + pos, static_cast<int32_t>(cap));
     }
     break;
   case ObCreateTimeType:
     {
       int64_t value;
       if (cell->value_.get_createtime(value) != OB_SUCCESS) {
         TBSYS_LOG(ERROR, "get_createtime error");
         break;
       }
       len = ObDateTime2MySQLDate(value, type, data + pos, static_cast<int32_t>(cap));
     }
     break;
   case ObFloatType:
     {
       float value;
       if (cell->value_.get_float(value) != OB_SUCCESS) {
         TBSYS_LOG(ERROR, "get_float error");
         break;
       }
       len = snprintf(data + pos, cap, "%f", value);
     }
     break;
   case ObDoubleType:
     {
       double value;
       if (cell->value_.get_double(value) != OB_SUCCESS) {
         TBSYS_LOG(ERROR, "get_double error");
         break;
       }
       len = snprintf(data + pos, cap, "%f", value);
     }
     break;
   default:
     TBSYS_LOG(WARN, "Not Defined Type %d", cell->value_.get_type());
     break;
  }

  if (len >= 0 ) {
    buff.get_position() += len;
  }

  return static_cast<int32_t>(len);
}

int append_delima(ObDataBuffer &buff)
{
  if (buff.get_remain() < 1)
    return OB_ERROR;

  buff.get_data()[buff.get_position()++] = body_delima;
  return OB_SUCCESS;
}

int append_header_delima(ObDataBuffer &buff)
{
  if (buff.get_remain() < 1)
    return OB_ERROR;

  buff.get_data()[buff.get_position()++] = header_delima;
  return OB_SUCCESS;
}

int append_end_rec(ObDataBuffer &buff)
{
  char *data = buff.get_data();
  int64_t pos = buff.get_position();
  int64_t cap = buff.get_remain();

  int len = snprintf(data + pos, cap, "\n");
  if (len >=0 )
    buff.get_position() += len;

  return len;
}

//return value--string length
int ObDateTime2MySQLDate(int64_t ob_time, int time_type, char *outp, int size)
{
  int ret = OB_SUCCESS;
  int len = 0;
  time_t time_sec;

  switch (time_type) {
   case ObModifyTimeType:
   case ObCreateTimeType:
   case ObPreciseDateTimeType:
     time_sec = ob_time / 1000L / 1000L;
     break;
   case ObDateTimeType:
     time_sec = ob_time;
     break;
   default:
     TBSYS_LOG(ERROR, "unrecognized time format, type=%d", time_type);
     ret = OB_ERROR;
  }

  if (ret == OB_SUCCESS) {
    struct tm time_tm;
    localtime_r(&time_sec, &time_tm);

    len = static_cast<int32_t>(strftime(outp, size, "%Y-%m-%d %H:%M:%S", &time_tm));
  }

  return len;
}

const char *get_op_string(int action)
{
  const char *res = NULL;

  if (action == ObActionFlag::OP_UPDATE) {
    res = "UPDATE";
  } else if (action == ObActionFlag::OP_INSERT) {
    res = "INSERT";
  } else if (action == ObActionFlag::OP_DEL_ROW) {
    res = "DELETE";
  }

  return res;
}

int db_utils_init()
{
  //  header_delima = DUMP_CONFIG->get_header_delima();
  //  body_delima = DUMP_CONFIG->get_body_delima();
  return 0;
}

void encode_int32(char* buf, uint32_t value)
{
#if __BYTE_ORDER == __LITTLE_ENDIAN
  memcpy(buf, &value, sizeof(value));
#else
  buf[0] = value & 0xff;
  buf[1] = (value >> 8) & 0xff;
  buf[2] = (value >> 16) & 0xff;
  buf[3] = (value >> 24) & 0xff;
#endif
}

int32_t decode_int32(const char *buf)
{
  uint32_t value;
#if __BYTE_ORDER == __LITTLE_ENDIAN
  memcpy(&value, buf, sizeof(value));
#else
  value = ((static_cast<uint32_t>(ptr[0]))
           | (static_cast<uint32_t>(ptr[1]) << 8)
           | (static_cast<uint32_t>(ptr[2]) << 16)
           | (static_cast<uint32_t>(ptr[3]) << 24));
#endif

  return value;
}

int transform_date_to_time(const char *str, int len, ObDateTime &t)
{
  int err = OB_SUCCESS;
  struct tm time;
  time_t tmp_time = 0;
  char back = str[len];
  const_cast<char*>(str)[len] = '\0';
  if (NULL != str && *str != '\0')
  {
    if (strchr(str, '-') != NULL)
    {
      if (strchr(str, ':') != NULL)
      {
        if ((sscanf(str,"%4d-%2d-%2d %2d:%2d:%2d",&time.tm_year,
                &time.tm_mon,&time.tm_mday,&time.tm_hour,
                &time.tm_min,&time.tm_sec)) != 6)
        {
          err = OB_ERROR;
        }
      }
      else
      {
        if ((sscanf(str,"%4d-%2d-%2d",&time.tm_year,&time.tm_mon,
                &time.tm_mday)) != 3)
        {
          err = OB_ERROR;
        }
        time.tm_hour = 0;
        time.tm_min = 0;
        time.tm_sec = 0;
      }
    }
    else
    {
      if (strchr(str, ':') != NULL)
      {
        if ((sscanf(str,"%4d%2d%2d %2d:%2d:%2d",&time.tm_year,
                &time.tm_mon,&time.tm_mday,&time.tm_hour,
                &time.tm_min,&time.tm_sec)) != 6)
        {
          err = OB_ERROR;
        }
      }
      else if (strlen(str) > 8)
      {
        if ((sscanf(str,"%4d%2d%2d%2d%2d%2d",&time.tm_year,
                &time.tm_mon,&time.tm_mday,&time.tm_hour,
                &time.tm_min,&time.tm_sec)) != 6)
        {
          err = OB_ERROR;
        }
      }
      else
      {
        if ((sscanf(str,"%4d%2d%2d",&time.tm_year,&time.tm_mon,
                &time.tm_mday)) != 3)
        {
          err = OB_ERROR;
        }
        time.tm_hour = 0;
        time.tm_min = 0;
        time.tm_sec = 0;
      }
    }
    if (OB_SUCCESS != err)
    {
      TBSYS_LOG(ERROR,"sscanf failed : [%s] ",str);
      //t = atol(str);
    }
    else
    {
      time.tm_year -= 1900;
      time.tm_mon -= 1;
      time.tm_isdst = -1;

      if ((tmp_time = mktime(&time)) != -1)
      {
        t = tmp_time;
      }
    }
  }
  const_cast<char*>(str)[len] = back;
  return err;
}

void dump_scanner(ObScanner &scanner)
{
  ObScannerIterator iter;
  bool is_row_changed = false;

  for (iter = scanner.begin(); iter != scanner.end(); iter++)
  {
    ObCellInfo *cell_info;
    iter.get_cell(&cell_info, &is_row_changed);
    if (is_row_changed) 
    {
      TBSYS_LOG(INFO, "table_id:%lu, rowkey:\n", cell_info->table_id_);
      //      hex_dump(cell_info->row_key_.ptr(), cell_info->row_key_.length());
    }
    TBSYS_LOG(INFO, "%s", print_cellinfo(cell_info, "DUMP-SCANNER"));
  }
}

